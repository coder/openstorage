package githttp

// Per-push verify-sig work bounds (issue #59). One cheap push used to fan
// out into unbounded signature-verification work: every ref repeated its
// own history walk plus one `git verify-commit` subprocess per commit, and
// atomic pushes evaluated everything once in pre-receive and again in every
// update hook. Refs created at existing tips need no pack, so the pack
// quota never bounded any of it.
//
// Two coordinated bounds close this:
//
//  1. A per-push verdict cache: commit verdicts and whole (old,new) range
//     verdicts are recorded in a state file the serve process creates per
//     receive-pack request (EnvVerifyState, the EnvAtomic env-handoff
//     pattern — hooks are separate processes re-executing this binary, and
//     the pre-receive policy gate plus every update hook run sequentially
//     within one push, so an append-only file needs no locking).
//  2. A per-push verified-commit budget (EnvVerifySigBudget, resolved from
//     CODESTORAGE_VERIFY_SIG_BUDGET by the serve process) that fails CLOSED:
//     a ref needing more verifier spawns than the remaining budget is
//     rejected before any of them run.
//
// Deliberately NOT among the bounds: trimming creation walks by other refs
// (`rev-list --not --all`). The branch-protection contract compares against
// the protected ref's own prior tip only — a commit already on an unpoliced
// ref must still be verified the first time a protected ref references it
// (see introducedCommits in hook.go).

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"codestorage/internal/ref"
)

// EnvVerifyState carries the per-push verify-sig state file path from the
// serve process into the hooks. The file is created empty by the server
// before receive-pack starts and removed when the request ends; hooks
// append verdict lines as they verify. Absent (a serve binary predating
// issue #59, or a bare hook invocation) the hooks fall back to an
// in-process cache — correctness is unchanged, only cross-process reuse is
// lost.
const EnvVerifyState = "CODESTORAGE_GIT_VERIFY_STATE"

// EnvVerifySigBudget carries the resolved per-push verified-commit budget
// (issue #59) from the serve process into the hooks. Set only when the
// budget is enabled (> 0); malformed values fail closed like EnvPackQuota.
const EnvVerifySigBudget = "CODESTORAGE_GIT_VERIFY_SIG_BUDGET"

// EnvVerifySpawnLog is a test-mode-only knob (EnvCASDrop pattern): when set,
// every actual `git verify-commit` spawn appends its sha to the named file,
// so tests can assert exact spawn counts across the pre-receive gate and
// every update hook of one push.
const EnvVerifySpawnLog = "CODESTORAGE_TEST_VERIFY_SPAWN_LOG"

// State directives. The state is split across THREE files so that the cost
// a hook pays scales with the work its ref actually causes, not with the
// push's accumulated state (Codex rounds 5–8 and 11):
//
//   - <path>:         ok/deny commit verdicts, append-only, one line per
//     verifier spawn (bounded by the spawn budget). Loaded lazily — only
//     when a ref misses the range cache and must walk.
//   - <path>.ranges:  okrange lines for ranges that actually verified
//     commits (bounded by the spawn budget, and 1 line in the common
//     N-refs-at-one-tip shape). Loaded on every call: an exact-range hit
//     returns without touching the verdict file or spawning rev-list.
//   - <path>.walked:  the cumulative examined-commit counter, fixed-size,
//     atomically replaced. Folding it into an append-only file would grow
//     it once per walk; rewriting verdicts to update it would reserialize
//     O(budget) lines per walk.
const (
	verdictOK    = "ok"
	verdictDeny  = "deny"
	verdictRange = "okrange"
)

// verifyWalkBudgetMultiplier derives the per-push traversal bound from the
// verification budget: a push may examine at most multiplier × budget
// commits across all its history walks, cached verdicts included. Without
// it, cached-OK commits cost nothing, so refs at distinct ancestors of an
// already-verified chain could re-walk the whole history once per ref with
// zero verifier spawns (Codex round 3) — quota-invisible CPU amplification.
// Examining a commit is orders of magnitude cheaper than verifying one
// (one rev-list output line vs one subprocess), hence the generous
// multiplier: the default budget of 10000 allows a million examined
// commits per push. Compiled constant like the repack fold budgets;
// budget 0 disables the walk bound too.
const verifyWalkBudgetMultiplier = 100

// verifyState is one push's verdict cache. Loaded fresh per verifySig call
// (hooks are sequential); appends go straight to the files so the next
// hook process sees them.
type verifyState struct {
	path   string // "" = in-memory only (no serve-provided state file)
	ok     map[string]bool
	deny   map[string]string
	ranges map[string]bool
	spent  int
	walked int64
	// verdictsLoaded records whether ok/deny/spent were read from the
	// verdict file; range-cache hits never need them (see loadVerdicts).
	verdictsLoaded bool
	// rangeLines/verdictLines count state lines parsed by this load, the
	// basis of the per-evaluation fee charged to the walk allowance.
	rangeLines   int64
	verdictLines int64
}

// verifyStateFeeDivisor converts state lines loaded into walk-allowance
// units: one examined-commit-equivalent per 100 lines, so an evaluation
// that reloads a budget-sized state file (≤ ~10k lines at the default
// budget) is charged like a ~100-commit walk. Every evaluation additionally
// pays a flat unit. This is the generic bound Codex rounds 5–12 converged
// on: whatever per-push state a ref's evaluation forces the hook to parse
// is charged against the same allowance that bounds history walks, so no
// sequence of cheap cache-hitting or cache-missing refs can multiply
// bounded per-ref costs indefinitely for one request-rate admission.
const verifyStateFeeDivisor = 100

// chargeWalk charges n walk-allowance units (fail-closed against maxWalk,
// 0 = unbounded) and persists the new total for the push's later hook
// processes. The allowance error deliberately reuses the history-walk
// wording: state-load fees are denominated in examined-commit equivalents.
func (st *verifyState) chargeWalk(n int64, maxWalk int64, refName string) error {
	assertf(n >= 0 && maxWalk >= 0, "chargeWalk(%d, %d)", n, maxWalk)
	if n == 0 {
		return nil
	}
	if maxWalk > 0 && st.walked+n > maxWalk {
		return fmt.Errorf("verify-sig: push exceeds the per-push history-walk allowance: %s pushed the total past %d examined commits (%d× CODESTORAGE_VERIFY_SIG_BUDGET; raise the budget or split the push)",
			refName, maxWalk, verifyWalkBudgetMultiplier)
	}
	if err := st.markWalked(n); err != nil {
		return fmt.Errorf("verify-sig: %w", err)
	}
	return nil
}

func rangeKey(old, new string) string { return old + " " + new }

// openVerifyState loads the cheap per-push state: the range cache and the
// walk counter, both bounded and small. The (potentially budget-sized)
// verdict file is NOT read here — call loadVerdicts before walking — so a
// push fanning one verified range out to many refs costs each ref a
// constant-size load, not a rescan of every verdict (Codex round 11). The
// paths come from our own serve process, never the client; anything
// set-but-unreadable or malformed means misconfiguration or tampering and
// fails closed (the returned error denies the push). An unset env falls
// back to a fresh in-memory state.
func openVerifyState() (*verifyState, error) {
	st := &verifyState{
		ok:     make(map[string]bool),
		deny:   make(map[string]string),
		ranges: make(map[string]bool),
	}
	path := os.Getenv(EnvVerifyState)
	if path == "" {
		st.verdictsLoaded = true
		return st, nil
	}
	st.path = path
	// The range sidecar is created on first markRangeOK; absent means no
	// range has been verified yet.
	if err := readLineFile(st.rangesPath(), true, st.applyRangeLine); err != nil {
		return nil, err
	}
	// The cumulative examined-commit counter: absent means no walk has
	// been charged yet.
	raw, err := os.ReadFile(st.walkedCounterPath())
	switch {
	case err == nil:
		n, perr := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if perr != nil || n < 0 {
			return nil, fmt.Errorf("malformed walked counter %q", strings.TrimSpace(string(raw)))
		}
		st.walked = n
	case os.IsNotExist(err):
		// no walks charged yet
	default:
		return nil, fmt.Errorf("read walked counter: %w", err)
	}
	return st, nil
}

// loadVerdicts reads the ok/deny verdict file (created empty by the serve
// process, so unlike the sidecars it must exist). Idempotent; required
// before consulting st.ok/st.deny/st.spent.
func (st *verifyState) loadVerdicts() error {
	if st.verdictsLoaded {
		return nil
	}
	if err := readLineFile(st.path, false, st.applyVerdictLine); err != nil {
		return err
	}
	st.verdictsLoaded = true
	return nil
}

// readLineFile feeds every non-empty line of path to apply, failing closed
// on unreadable files (unless missingOK admits a not-yet-created sidecar).
func readLineFile(path string, missingOK bool, apply func(string) error) error {
	f, err := os.Open(path)
	if err != nil {
		if missingOK && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open verify state: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		if err := apply(line); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read verify state: %w", err)
	}
	return nil
}

// applyVerdictLine folds one verdict-file line into the in-memory maps.
// Only this binary writes these files, so a malformed line is a bug or
// tampering — but hooks must never panic mid-push, so it surfaces as a
// denial error.
func (st *verifyState) applyVerdictLine(line string) error {
	directive, rest, _ := strings.Cut(line, " ")
	switch directive {
	case verdictOK:
		if !ref.ValidSHA(rest) {
			return fmt.Errorf("malformed verify state line %q", line)
		}
		st.ok[rest] = true
		st.spent++
	case verdictDeny:
		sha, reason, found := strings.Cut(rest, " ")
		if !found || !ref.ValidSHA(sha) || reason == "" {
			return fmt.Errorf("malformed verify state line %q", line)
		}
		st.deny[sha] = reason
		st.spent++
	default:
		return fmt.Errorf("malformed verify state line %q", line)
	}
	st.verdictLines++
	return nil
}

// applyRangeLine folds one range-sidecar line into the range cache.
func (st *verifyState) applyRangeLine(line string) error {
	directive, rest, _ := strings.Cut(line, " ")
	old, new, found := strings.Cut(rest, " ")
	if directive != verdictRange || !found || !ref.ValidSHA(old) || !ref.ValidSHA(new) {
		return fmt.Errorf("malformed verify range line %q", line)
	}
	st.ranges[rangeKey(old, new)] = true
	st.rangeLines++
	return nil
}

// appendTo records one directive line durably for the following hook
// processes of the same push. An append failure fails closed at the caller
// (a verdict may not silently evaporate: losing an ok costs a re-verify,
// but losing a deny would let a later ref re-spawn for a known-bad commit).
func appendTo(path, line string, create bool) error {
	assertf(!strings.Contains(line, "\n"), "verify state line %q contains newline", line)
	flags := os.O_WRONLY | os.O_APPEND
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return fmt.Errorf("append verify state: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(line + "\n"); err != nil {
		return fmt.Errorf("append verify state: %w", err)
	}
	return nil
}

func (st *verifyState) markCommitOK(sha string) error {
	line := verdictOK + " " + sha
	if err := st.applyVerdictLine(line); err != nil {
		return err
	}
	if st.path == "" {
		return nil
	}
	return appendTo(st.path, line, false)
}

func (st *verifyState) markCommitDenied(sha, reason string) error {
	// Reasons are single-line by construction (condenseGitReason joins with
	// "; "); normalize defensively so the line-based format cannot break.
	reason = strings.Join(strings.Fields(reason), " ")
	if reason == "" {
		reason = "signature verification failed"
	}
	line := verdictDeny + " " + sha + " " + reason
	if err := st.applyVerdictLine(line); err != nil {
		return err
	}
	if st.path == "" {
		return nil
	}
	return appendTo(st.path, line, false)
}

func (st *verifyState) markRangeOK(old, new string) error {
	line := verdictRange + " " + rangeKey(old, new)
	if err := st.applyRangeLine(line); err != nil {
		return err
	}
	if st.path == "" {
		return nil
	}
	return appendTo(st.rangesPath(), line, true)
}

// verifyWalkedCounterSuffix and verifyRangesSuffix name the sibling files
// next to the verdict state file; the server removes all three when the
// request ends.
const (
	verifyWalkedCounterSuffix = ".walked"
	verifyRangesSuffix        = ".ranges"
)

// rangesPath is the sidecar carrying okrange lines. Separate from the
// verdict file so exact-range hits load only this bounded, usually tiny
// file (Codex round 11).
func (st *verifyState) rangesPath() string {
	assertf(st.path != "", "rangesPath without a state file")
	return st.path + verifyRangesSuffix
}

// walkedCounterPath is the sibling fixed-size file carrying the push's
// cumulative examined-commit counter. Kept out of the verdict state file so
// updating it costs O(1) — never a reserialization of the budget-bounded
// verdict lines (Codex round 8).
func (st *verifyState) walkedCounterPath() string {
	assertf(st.path != "", "walkedCounterPath without a state file")
	return st.path + verifyWalkedCounterSuffix
}

// markWalked folds n examined commits into the cumulative counter and
// persists it by atomically replacing the fixed-size counter file (write
// temp + rename; hooks run strictly sequentially within one push, so there
// is no concurrent reader to tear). A zero count records nothing.
func (st *verifyState) markWalked(n int64) error {
	assertf(n >= 0, "markWalked(%d)", n)
	if n == 0 {
		return nil
	}
	st.walked += n
	if st.path == "" {
		return nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(st.path), ".codestorage-verify-walked-*")
	if err != nil {
		return fmt.Errorf("write walked counter: %w", err)
	}
	if _, err := tmp.WriteString(strconv.FormatInt(st.walked, 10) + "\n"); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("write walked counter: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("write walked counter: %w", err)
	}
	if err := os.Rename(tmp.Name(), st.walkedCounterPath()); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("write walked counter: %w", err)
	}
	return nil
}

// walkBudget derives the traversal bound from the verification budget,
// saturating instead of overflowing on absurd budgets. 0 = unbounded.
func walkBudget(budget int) int64 {
	assertf(budget >= 0, "walkBudget(%d)", budget)
	if budget == 0 {
		return 0
	}
	if int64(budget) > (1<<62)/verifyWalkBudgetMultiplier {
		return 1 << 62
	}
	return int64(budget) * verifyWalkBudgetMultiplier
}

// verifySigBudget resolves the per-push verified-commit budget from the
// hook env. 0 (or unset) means unlimited; the env is set by our own server,
// so nonsense fails closed like a malformed EnvPackQuota.
func verifySigBudget() (int, error) {
	raw := os.Getenv(EnvVerifySigBudget)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("malformed %s=%q", EnvVerifySigBudget, raw)
	}
	return v, nil
}

// logVerifySpawn appends sha to the test-mode spawn log (see
// EnvVerifySpawnLog). Best-effort by design: a test knob must never affect
// push outcomes.
func logVerifySpawn(sha string) {
	if !testMode() {
		return
	}
	path := os.Getenv(EnvVerifySpawnLog)
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(sha + "\n")
}
