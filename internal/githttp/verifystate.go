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

// State file directives. Every verifier spawn appends exactly one ok/deny
// line, so spent (the budget charge) is their count; okrange lines record
// whole-range successes for cross-ref (old,new) dedup and are persisted
// only for ranges that actually verified commits. The cumulative
// examined-commit counter lives in a SEPARATE fixed-size sibling file
// (walkedCounterPath): folding it into this file would either grow it once
// per walk or force a full rewrite per walk — both let cheap cached walks
// amplify into O(refs × budget) state processing (Codex rounds 5–8).
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
// (the file is small and hooks are sequential); appends go straight to the
// file so the next hook process sees them.
type verifyState struct {
	path   string // "" = in-memory only (no serve-provided state file)
	ok     map[string]bool
	deny   map[string]string
	ranges map[string]bool
	spent  int
	walked int64
}

func rangeKey(old, new string) string { return old + " " + new }

// openVerifyState loads the per-push state. The path comes from our own
// serve process, never the client; a set-but-unreadable or malformed state
// file therefore means misconfiguration or tampering and fails closed (the
// returned error denies the push). An unset env falls back to a fresh
// in-memory state.
func openVerifyState() (*verifyState, error) {
	st := &verifyState{
		ok:     make(map[string]bool),
		deny:   make(map[string]string),
		ranges: make(map[string]bool),
	}
	path := os.Getenv(EnvVerifyState)
	if path == "" {
		return st, nil
	}
	st.path = path
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open verify state: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		if err := st.applyLine(line); err != nil {
			return nil, err
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read verify state: %w", err)
	}
	// The cumulative examined-commit counter lives in the sibling
	// fixed-size file; absent means no walk has been charged yet. Our own
	// binary writes it, so anything unreadable or malformed fails closed.
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

// applyLine folds one state-file line into the in-memory maps. Only this
// binary writes the file, so a malformed line is a bug or tampering — but
// hooks must never panic mid-push, so it surfaces as a denial error.
func (st *verifyState) applyLine(line string) error {
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
	case verdictRange:
		old, new, found := strings.Cut(rest, " ")
		if !found || !ref.ValidSHA(old) || !ref.ValidSHA(new) {
			return fmt.Errorf("malformed verify state line %q", line)
		}
		st.ranges[rangeKey(old, new)] = true
	default:
		return fmt.Errorf("malformed verify state line %q", line)
	}
	return nil
}

// appendLine records one directive both in memory and — when a state file
// is wired — durably for the following hook processes of the same push. An
// append failure fails closed at the caller (the verdict may not silently
// evaporate: losing an ok costs a re-verify, but losing a deny would let a
// later ref re-spawn for a known-bad commit).
func (st *verifyState) appendLine(line string) error {
	assertf(!strings.Contains(line, "\n"), "verify state line %q contains newline", line)
	if err := st.applyLine(line); err != nil {
		return err
	}
	if st.path == "" {
		return nil
	}
	f, err := os.OpenFile(st.path, os.O_WRONLY|os.O_APPEND, 0)
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
	return st.appendLine(verdictOK + " " + sha)
}

func (st *verifyState) markCommitDenied(sha, reason string) error {
	// Reasons are single-line by construction (condenseGitReason joins with
	// "; "); normalize defensively so the line-based format cannot break.
	reason = strings.Join(strings.Fields(reason), " ")
	if reason == "" {
		reason = "signature verification failed"
	}
	return st.appendLine(verdictDeny + " " + sha + " " + reason)
}

func (st *verifyState) markRangeOK(old, new string) error {
	return st.appendLine(verdictRange + " " + rangeKey(old, new))
}

// verifyWalkedCounterSuffix names the sibling counter file next to the
// verdict state file; the server removes both when the request ends.
const verifyWalkedCounterSuffix = ".walked"

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
