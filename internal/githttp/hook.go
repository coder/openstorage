package githttp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

// Env names used to route request context from the HTTP handler into the
// git hooks (which re-exec this binary). CODESTORAGE_TEST_* fault-injection
// knobs are honored only when CODESTORAGE_TEST_MODE=1.
const (
	EnvRepo       = "CODESTORAGE_GIT_REPO"
	EnvRefsPolicy = "CODESTORAGE_GIT_REFS_POLICY"
	EnvOps        = "CODESTORAGE_GIT_OPS"
	// EnvOrg names the org whose signing keys verify-sig loads. Shared with
	// the serve config on purpose: the server sets it explicitly for hooks.
	EnvOrg = "CODESTORAGE_ORG"
	// EnvAtomic marks the push `--atomic` (issue #28). githooks(5) exposes
	// no atomic indicator, so the HTTP layer sniffs the wire capability
	// (atomic.go) and sets this; pre-receive then evaluates the policy
	// across ALL commands and denies before publication when any command
	// is denied — git would abort every ref on the first update-hook
	// denial anyway.
	EnvAtomic    = "CODESTORAGE_GIT_ATOMIC"
	EnvTestMode  = "CODESTORAGE_TEST_MODE"
	EnvCrash     = "CODESTORAGE_TEST_CRASH"
	EnvHookDelay = "CODESTORAGE_TEST_HOOK_DELAY_MS"
	// EnvCASDrop/EnvCASDropHard hold a marker-file path; the first update
	// hook to create the marker lets its CAS statement commit, then reports
	// a synthetic connection-drop error (issue #2's ambiguous outcome).
	// The hard variant also fails the recovery recheck read, as a truly
	// dead connection would. Later invocations pass through untouched.
	EnvCASDrop     = "CODESTORAGE_TEST_CAS_DROP"
	EnvCASDropHard = "CODESTORAGE_TEST_CAS_DROP_HARD"
	// CrashHeader lets tests request an EnvCrash injection per push; the
	// server refuses it outside test mode.
	CrashHeader = "X-Codestorage-Test-Crash"
)

// PG/S3 connection env for hooks, inherited from the serve process.
// Canonical CODESTORAGE_* names first; the bare legacy names are still read
// so a serve process exporting only those keeps working hooks.
var (
	envHookPGURL   = []string{"CODESTORAGE_PG_URL", "PG_URL"}
	envHookBlobURL = []string{"CODESTORAGE_S3_URL", "BLOB_URL"}
)

func testMode() bool { return os.Getenv(EnvTestMode) == "1" }

// WrapHookRefStore decorates the ref store the update hook opens for its
// CAS. Hooks run in a fresh `codestorage hook` subprocess (git execs the
// installed hook script), so the serve process's wired store cannot reach
// them; instead the cmd wiring seam — which runs in the hook subprocess
// too, it is the same binary — replaces this variable at startup with the
// events.RefStore decorator so git pushes emit push events (docs/06)
// exactly like API writes (that decorator wraps ref.IdempotentCAS itself).
// The eventless default keeps hooks working before/without eventing wiring,
// wrapping just the CAS-recovery decorator so ambiguous outcomes still
// reconcile.
var WrapHookRefStore = func(ctx context.Context, st ref.RefStore) ref.RefStore {
	return ref.IdempotentCAS{RefStore: st}
}

func mustEnv(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	panic(fmt.Sprintf("githttp assertion failed: hook: required env %s is empty", names[0]))
}

// RunHook dispatches `codestorage hook <pre-receive|update> [args...]`,
// invoked by git via the scripts EnsureCacheRepo installs. Arguments and
// stdin come from outside this process, so malformed input must produce an
// error (stderr message + non-zero exit denies the push), never a panic.
func RunHook(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("hook: missing hook name argument")
	}
	switch args[0] {
	case "pre-receive":
		return preReceive(ctx)
	case "update":
		return updateHook(ctx, args[1:])
	default:
		return fmt.Errorf("unknown hook %q", args[0])
	}
}

type refCommand struct{ old, new, ref string }

// preReceive uploads the quarantined pack(s) to S3 FIRST, then registers
// them in PG in one transaction. Crash windows (validated by tests):
//   - die after S3 upload, before PG commit  => orphaned S3 object only
//   - die after PG commit, before ref CAS    => registered-but-unreferenced pack
//
// Refs are never touched here, so a broken ref is impossible.
func preReceive(ctx context.Context) error {
	repo := mustEnv(EnvRepo)
	// git sets GIT_QUARANTINE_PATH only when the client transfers objects;
	// delete-only pushes invoke pre-receive without it.
	quarantine := os.Getenv("GIT_QUARANTINE_PATH")

	// Stdin is external input (git relays it from the client connection):
	// malformed lines deny the push with an error, never a panic.
	var commands []refCommand
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 3 {
			return fmt.Errorf("pre-receive: malformed stdin line %q", line)
		}
		cmd := refCommand{old: parts[0], new: parts[1], ref: parts[2]}
		if !ref.ValidSHA(cmd.old) || !ref.ValidSHA(cmd.new) {
			return fmt.Errorf("pre-receive: malformed sha in stdin line %q", line)
		}
		commands = append(commands, cmd)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("pre-receive: reading stdin: %w", err)
	}
	if len(commands) == 0 {
		return errors.New("pre-receive: git invoked hook with no commands")
	}
	atomicPush := os.Getenv(EnvAtomic) == "1"

	var packFiles []string
	if quarantine != "" {
		var err error
		packFiles, err = filepath.Glob(filepath.Join(quarantine, "pack", "pack-*.pack"))
		if err != nil {
			return err
		}
	}
	if len(packFiles) == 0 {
		// No pack means git received no new objects: a delete-only push or
		// updates targeting objects the server already has (e.g. a branch
		// created from an existing commit). Whether git sends an empty pack
		// in the latter case is version/client dependent, so both paths must
		// work. receive-pack verified connectivity before invoking us, but
		// the SHAs come from client stdin: a violation denies the push with
		// an error instead of crashing receive-pack.
		for _, c := range commands {
			if c.new != ref.ZeroSHA && !objectExists(c.new) {
				return fmt.Errorf("pre-receive: no quarantined pack and unknown object %s for %s", c.new, c.ref)
			}
		}
		// No pack means nothing can leak, but an atomic push still
		// evaluates policy as a unit: git aborts every ref on the first
		// update-hook denial anyway, and denying here keeps an allowed
		// command's already-committed PG CAS (e.g. a delete that ran
		// before the denied one) from durably surviving a push git
		// reports as wholly failed — the issue #28 durable-CAS twist.
		if atomicPush {
			for _, c := range commands {
				if err := enforceRefPolicy(ctx, repo, c.ref, c.old, c.new); err != nil {
					return atomicDenied(err)
				}
			}
		}
		return nil
	}

	// Publication policy gate (issue #21, Codex security rounds): a pack
	// is published only when some policy-ALLOWED command actually needs
	// its objects. Otherwise (every command denied, or the allowed
	// commands all target already-stored objects — the round-7 decoy: a
	// denied big update bundled with an allowed create at a stored SHA)
	// the pack could only become a registered, unreachable,
	// quota-consuming blob, so the whole push is refused before any
	// upload. The update hook stays the per-ref policy authority.
	if err := preReceivePolicyGate(ctx, repo, commands, atomicPush); err != nil {
		return err
	}

	bl, err := blob.Open(ctx, mustEnv(envHookBlobURL...))
	if err != nil {
		return err
	}
	defer bl.Close()

	// Resolve quarantined pack names to their content-addressed storage
	// keys and byte sizes up front (the quota gate and the registration
	// ledger below need both). Quarantine contents are produced by git, but
	// fail closed on a surprising name instead of crashing receive-pack.
	var packs []ref.Pack
	for _, pf := range packFiles {
		base := filepath.Base(pf) // pack-<hash>.pack
		id := strings.TrimSuffix(strings.TrimPrefix(base, "pack-"), ".pack")
		if id == "" || id == base {
			return fmt.Errorf("pre-receive: unexpected pack file name %q", base)
		}
		fi, err := os.Stat(pf)
		if err != nil {
			return fmt.Errorf("pre-receive: stat quarantined pack: %w", err)
		}
		if fi.Size() <= 0 {
			return fmt.Errorf("pre-receive: quarantined pack %q is empty", base)
		}
		packs = append(packs, ref.Pack{ID: id, Key: blob.PackKey(repo, id), Size: fi.Size()})
	}

	// Content-addressed dedup (Codex rounds 4 + 10): identical pack keys
	// are re-sent by crash-window retries and ref re-creations. An already
	// published key contributes zero quota `incoming` AND is not
	// re-uploaded — on a versioned S3 bucket every overwrite would retain
	// a billed noncurrent version invisible to usage listings (unbounded
	// hidden growth), and the bytes are immutable so skipping loses
	// nothing. A sweeper deleting the key between this check and
	// registration is healed by blob.EnsurePublished below, exactly as for
	// a fresh upload.
	//
	// The Stat/upload pair is not atomic: concurrent identical first-time
	// pushes can each see the key absent and both upload. Byte-identical
	// last-writer-wins is semantically harmless (content-addressed keys),
	// and the cost is bounded — at most concurrency-many noncurrent
	// versions per unique pack on versioned buckets, reclaimable by a
	// standard noncurrent-version lifecycle rule. gocloud's portable blob
	// API has no conditional create. Issue #29's reservation ledger
	// deliberately does NOT close this window either: an exclusive live
	// same-key reservation would wedge crash-window retries for the full
	// reservation TTL (the wedge class issue #21's reviews eliminated), so
	// same-key reservations refresh instead — the quota counts the bytes
	// once and the LWW overwrite stays bounded as described above (see
	// ref/ledger.go).
	published := make([]bool, len(packs))
	for i := range packs {
		if _, err := bl.Stat(ctx, packs[i].Key); err == nil {
			published[i] = true
		} else if !errors.Is(err, blob.ErrNotFound) {
			return fmt.Errorf("pre-receive: stat %s: %w", packs[i].Key, err)
		}
	}

	// The ref store opens before the quota gate: the durable reservation
	// (issue #29) lives there, and RegisterPacks below needs it anyway. An
	// unreachable store now fails the push before any upload — strictly
	// better than failing after.
	st, err := ref.Open(ctx, mustEnv(envHookPGURL...))
	if err != nil {
		return err
	}
	defer st.Close()

	// Per-repo pack-bytes quota (issue #21, quota.go), enforced before any
	// upload: pre-receive runs BEFORE the update hook's ref policy, so
	// without this gate even a rejected push durably grows packs/. Denying
	// here leaves the quarantine for git to discard — nothing is published,
	// registered, or reserved. The error message reaches the client via the
	// sideband channel; the serve process logs quota pressure at the HTTP
	// layer.
	//
	// incoming counts only unpublished packs: double-counting bytes the
	// usage listing already covers could reject the very retry that
	// completes an interrupted publication, and an all-published push adds
	// nothing and passes regardless of usage (the zero-growth rule).
	//
	// Admission is a durable reservation in the ref store (issue #29,
	// ref/ledger.go), not a bare check: the blob listing feeds it only as
	// the usage FLOOR, while in-flight reservations close the
	// concurrent-push overshoot the listing alone admitted. The reservation
	// is consumed atomically by RegisterPacks below; a crash anywhere
	// between here and there leaves an expiring row the sweeper prunes.
	var reserved []string
	var reserveOwner string
	if quotaRaw := os.Getenv(EnvPackQuota); quotaRaw != "" {
		quota, err := strconv.ParseInt(quotaRaw, 10, 64)
		if err != nil || quota <= 0 {
			// The env is set by our own server (never by the client), but
			// fail closed on nonsense instead of waving the push through.
			return fmt.Errorf("pre-receive: malformed %s=%q", EnvPackQuota, quotaRaw)
		}
		ttl, err := reservationTTL()
		if err != nil {
			return fmt.Errorf("pre-receive: %w", err)
		}
		var unpublished []ref.Pack
		for i := range packs {
			if !published[i] {
				unpublished = append(unpublished, packs[i])
			}
		}
		if len(unpublished) > 0 {
			floor, err := packUsageBytes(ctx, bl, repo)
			if err != nil {
				return fmt.Errorf("pre-receive: %w", err)
			}
			reserveOwner = ref.NewReservationOwner()
			dec, err := st.ReservePackBytes(ctx, blob.QuotaRepoGroup(repo), unpublished, reserveOwner, floor, quota, ttl)
			if err != nil {
				return fmt.Errorf("pre-receive: reserve pack bytes: %w", err)
			}
			if !dec.OK {
				var incoming int64
				for _, p := range unpublished {
					incoming += p.Size
				}
				return fmt.Errorf("pre-receive: repository pack quota exceeded: push adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight pushes), quota is %d bytes",
					incoming, dec.UsedBytes, dec.ReservedBytes, quota)
			}
			for _, p := range unpublished {
				reserved = append(reserved, p.Key)
			}
			// Test knob: crash after the reservation committed, before any
			// upload — the reservation deliberately leaks (issue #29's
			// reserve → crash → sweep recovery window).
			if testMode() && os.Getenv(EnvCrash) == "after-reserve" {
				fmt.Fprintln(os.Stderr, "codestorage: crashing after reservation, before upload (test knob)")
				os.Exit(42)
			}
		}
	}

	for i, pf := range packFiles {
		if published[i] {
			continue
		}
		if err := bl.UploadFile(ctx, packs[i].Key, pf); err != nil {
			// Best-effort release: the push is failing, so free the
			// admission headroom now instead of waiting out the TTL. A
			// failed release only leaves an expiring row (owner-scoped, so
			// it can never free a reservation a retry re-acquired).
			if len(reserved) > 0 {
				_ = st.ReleasePackReservations(ctx, reserved, reserveOwner)
			}
			return fmt.Errorf("pre-receive: upload %s: %w", packs[i].Key, err)
		}
	}

	// Test knob: crash between S3 upload and PG commit. The reservation
	// deliberately leaks here — TTL expiry plus the sweeper reclaim it.
	if testMode() && os.Getenv(EnvCrash) == "after-s3" {
		fmt.Fprintln(os.Stderr, "codestorage: crashing after S3 upload, before PG commit (test knob)")
		os.Exit(42)
	}
	// Writer half of the orphan-sweep coordination (issue #5): a sweeper
	// may have deleted a just-uploaded key right before the registration
	// committed; re-upload from the local pack file if so. Verification
	// runs even when RegisterPacks fails — an ambiguous error (commit
	// executed, response lost) may have registered the packs, and a
	// committed row must never point at missing bytes. Extra verify work
	// on a genuinely failed registration is harmless: the keys were just
	// uploaded, so each check is one Stat.
	regErr := st.RegisterPacks(ctx, repo, packs)
	if regErr != nil {
		regErr = fmt.Errorf("pre-receive: register packs: %w", regErr)
	}
	for i, p := range packs {
		if err := blob.EnsurePublished(ctx, bl, p.Key, packFiles[i]); err != nil {
			return errors.Join(regErr, fmt.Errorf("pre-receive: verify %s: %w", p.Key, err))
		}
	}
	return regErr
}

// preReceivePolicyGate refuses pack publication unless it is justified:
// either the push is wholly policy-allowed (vestigial packs from creates at
// existing SHAs stay accepted, matching the no-pack path), or at least one
// policy-ALLOWED command needs quarantined objects (its target is not in
// the main object store yet). A push containing any denied command whose
// allowed commands all target already-stored objects is refused whole —
// its pack could only serve the denied refs (round-7 decoy) — with a
// message telling the client to push the allowed updates separately. It
// reuses enforceRefPolicy verbatim — the quarantine is visible to
// pre-receive via git's alternates, so is-ancestor and verify-sig checks
// work here exactly as in the update hook; verify-sig-policied winners are
// verified twice, an accepted redundancy for failing closed.
//
// atomicPush (issue #28, EnvAtomic) tightens the gate to the whole
// command set: under `git push --atomic` receive-pack aborts EVERY ref
// update on the first update-hook denial, so a mixed push's pack can only
// end up registered yet unreachable — permanently, because registered
// packs are not sweeper orphans and repack folds carry every registered
// object forward (internal/jobs/repack.go). Denying the whole push before
// publication changes no ref outcome git would have produced and stores
// nothing. It also closes the durable-CAS twist: without it, an allowed
// command whose update hook ran before the denial had already committed
// its PG CAS, durably surviving a push git reports as wholly failed.
//
// Known residuals, all quota-counted (self-limiting):
//   - an atomic push that passes this gate can still abort on a
//     non-policy update failure (concurrent CAS conflict, or a keyring
//     change between the pre-receive and update evaluations) after the
//     pack is registered. A client retry re-sends the identical
//     content-addressed pack, which dedups and becomes reachable on
//     success; an abandoned retry leaves quota-counted junk. Closing
//     this needs reachability accounting — issue #31 territory.
//   - junk riders: a mixed push whose allowed command genuinely needs the
//     pack still publishes ALL of the pack's objects, including ones only
//     denied commands reference. Filtering by reachability from allowed
//     winners would require re-packing at pre-receive; and the marginal
//     power it removes is nil — any holder of the same repo-scoped
//     git:write token can burn the repo's quota with policy-ALLOWED
//     garbage (push a large blob to a permitted branch, delete the
//     branch). The quota bounds per-repo growth; it cannot police
//     intra-repo goodwill among authorized writers. Retained-byte
//     reclamation is issue #31's accounting work.
func preReceivePolicyGate(ctx context.Context, repo string, commands []refCommand, atomicPush bool) error {
	assertf(len(commands) > 0, "preReceivePolicyGate: no commands")
	var firstErr error
	needsPack := false
	for _, c := range commands {
		if err := enforceRefPolicy(ctx, repo, c.ref, c.old, c.new); err != nil {
			if atomicPush {
				return atomicDenied(err)
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if c.new != ref.ZeroSHA && !objectInMainStore(c.new) {
			needsPack = true
		}
	}
	if firstErr == nil || needsPack {
		return nil
	}
	return fmt.Errorf("pre-receive: the quarantined pack is needed only by policy-denied ref updates, refusing pack publication (push the allowed updates separately): %w", firstErr)
}

// atomicDenied is the whole-push denial for an atomic push carrying a
// policy-denied command. The message reaches the client over the sideband;
// it names the remedy because the same refspecs may be fine without
// --atomic (per-ref semantics let the allowed updates land).
func atomicDenied(err error) error {
	return fmt.Errorf("pre-receive: atomic push carries a policy-denied update, denying the whole push before publication (drop --atomic or the denied refspec to push the allowed updates): %w", err)
}

// updateHook enforces the JWT ref policy (first-match-wins), then performs
// the PG CAS. Exit != 0 makes git report a per-ref `ng` with our stderr
// message; git only updates the local cache ref for winners.
func updateHook(ctx context.Context, args []string) error {
	// Hook args are handed over by git per ref update; validate instead of
	// asserting so a malformed invocation denies the update, never panics
	// (CASRef below asserts well-formed SHAs).
	if len(args) != 3 {
		return fmt.Errorf("update: want 3 args (ref old new), got %d", len(args))
	}
	refName, old, new := args[0], args[1], args[2]
	if refName == "" {
		return errors.New("update: empty ref name")
	}
	if !ref.ValidSHA(old) || !ref.ValidSHA(new) {
		return fmt.Errorf("update: malformed sha for %s", refName)
	}
	repo := mustEnv(EnvRepo)

	if err := enforceRefPolicy(ctx, repo, refName, old, new); err != nil {
		return err
	}

	// Test knob: widen the CAS race window deterministically.
	if testMode() {
		if ms := os.Getenv(EnvHookDelay); ms != "" {
			n, err := strconv.Atoi(ms)
			assertf(err == nil && n >= 0, "bad %s=%q", EnvHookDelay, ms)
			time.Sleep(time.Duration(n) * time.Millisecond)
		}
	}

	st, err := ref.Open(ctx, mustEnv(envHookPGURL...))
	if err != nil {
		return err
	}
	defer st.Close()
	var inner ref.RefStore = st
	if testMode() {
		inner = injectCASDrop(inner)
	}
	store := WrapHookRefStore(ctx, inner)
	assertf(store != nil, "WrapHookRefStore returned nil")
	won, err := store.CASRef(ctx, repo, refName, old, new)
	if err != nil {
		return err
	}
	if !won {
		return fmt.Errorf("CAS conflict: %s moved concurrently (expected %.8s)", refName, old)
	}
	return nil
}

// errCASDrop is the synthetic post-commit connection-drop error the
// EnvCASDrop knobs inject.
var errCASDrop = errors.New("injected post-commit connection drop")

// casDropStore simulates a DB connection dying after the CAS statement
// commits: CASRef delegates (the write lands), then reports errCASDrop.
// hard additionally fails GetRef, so the ref.IdempotentCAS recovery recheck
// cannot run either — as with a truly dead connection.
type casDropStore struct {
	ref.RefStore
	hard bool
}

func (s casDropStore) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	if _, err := s.RefStore.CASRef(ctx, repo, name, old, new); err != nil {
		return false, err
	}
	return false, errCASDrop
}

func (s casDropStore) GetRef(ctx context.Context, repo, name string) (string, error) {
	if s.hard {
		return "", errCASDrop
	}
	return s.RefStore.GetRef(ctx, repo, name)
}

// injectCASDrop consumes the EnvCASDrop/EnvCASDropHard marker knobs (test
// mode only): the invocation that creates the marker file gets the faulty
// store; every other invocation passes st through untouched.
func injectCASDrop(st ref.RefStore) ref.RefStore {
	marker, hard := os.Getenv(EnvCASDrop), false
	if marker == "" {
		marker, hard = os.Getenv(EnvCASDropHard), true
	}
	if marker == "" {
		return st
	}
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return st // marker exists: knob already consumed
	}
	_ = f.Close()
	return casDropStore{RefStore: st, hard: hard}
}

// enforceRefPolicy evaluates the token's ref policies (docs/02, docs/upstream
// branch-protection.md) for one ref update. The deprecated repo-wide `ops`
// claim is folded in as a fallback exactly once, here at parse time.
func enforceRefPolicy(ctx context.Context, repo, refName, old, new string) error {
	policyJSON := os.Getenv(EnvRefsPolicy)
	opsJSON := os.Getenv(EnvOps)
	if policyJSON == "" && opsJSON == "" {
		return nil
	}
	var policies []auth.RefPolicy
	if policyJSON != "" {
		var err error
		policies, err = auth.ParseRefPolicies(policyJSON)
		if err != nil {
			return fmt.Errorf("update: %w", err)
		}
	}
	if opsJSON != "" {
		var ops []string
		if err := json.Unmarshal([]byte(opsJSON), &ops); err != nil {
			return fmt.Errorf("update: parse ops claim %q: %w", opsJSON, err)
		}
		policies = auth.FoldOps(policies, ops)
	}

	// Ephemeral namespace quirk (branch-protection.md): a push through the
	// `+ephemeral` namespace matches policies against the rewritten name
	// `refs/namespaces/ephemeral/<refname>`, so persistent-branch rules do
	// not police ephemeral copies unless the token uses `*` or an explicit
	// namespaced pattern. `+import` and persistent pushes match verbatim.
	matchRef := refName
	if strings.HasSuffix(repo, "+ephemeral") {
		matchRef = auth.EphemeralRefName(refName)
	}

	rule, ok := auth.MatchPolicy(policies, matchRef)
	if !ok {
		return nil // unpoliced ref: allowed
	}
	for _, op := range rule.Ops {
		switch op {
		case auth.OpNoPush:
			return fmt.Errorf("policy no-push denies update to %s", refName)
		case auth.OpNoForcePush:
			// Rejects any rewind of an existing ref: non-fast-forward
			// updates AND deletions. docs/02: ref policies are the
			// only branch-protection mechanism, so allowing deletion
			// would make the policy trivially bypassable
			// (delete + re-push = force push). Safe to check here:
			// quarantined objects were migrated into the main store
			// after pre-receive, before update hooks run.
			if old != ref.ZeroSHA {
				if new == ref.ZeroSHA {
					return fmt.Errorf("policy no-force-push denies deletion of %s", refName)
				}
				if !isAncestor(old, new) {
					return fmt.Errorf("policy no-force-push denies non-fast-forward update to %s", refName)
				}
			}
		case auth.OpVerifySig:
			if err := verifySig(ctx, refName, old, new); err != nil {
				return err
			}
		default:
			// Unknown policy ops fail closed.
			return fmt.Errorf("unknown policy op %q for %s", op, refName)
		}
	}
	return nil
}

// verifySig rejects the update unless every commit it introduces carries a
// valid OpenPGP or SSH signature from one of the org's registered signing
// keys (docs/02 verify-sig, branch-protection.md "Commit signing
// verifications"). "Introduced" = reachable from new but not from this
// ref's own pre-push tip; branch creation therefore checks everything and
// deletion checks nothing. The keyring is read fresh from PG on every push;
// within one push, verdicts are cached and shared between the pre-receive
// policy gate and the update hooks (verifystate.go), so the pre-receive
// verdict is authoritative for the whole push and N refs at one tip cost
// each commit exactly one verifier spawn (issue #59).
func verifySig(ctx context.Context, refName, old, new string) error {
	if new == ref.ZeroSHA {
		return nil // deletion introduces no commits
	}
	state, err := openVerifyState()
	if err != nil {
		return fmt.Errorf("verify-sig: %w", err) // fail closed
	}
	// Per-push verified-commit budget (issue #59), resolved BEFORE any
	// cache checks: it also prices the state loads below.
	budget, err := verifySigBudget()
	if err != nil {
		return fmt.Errorf("verify-sig: %w", err) // fail closed
	}
	maxWalk := walkBudget(budget)
	// Every evaluation pays a fee — one flat unit plus its share of the
	// range-sidecar lines just parsed — against the walk allowance (Codex
	// round 12): cache-hitting refs cost almost nothing individually, but
	// the accumulated fee fails a push closed once its ref count times its
	// state size stops being cheap. Cheaper than the hook process spawn
	// each ref already costs until the state is thousands of lines.
	if err := state.chargeWalk(1+state.rangeLines/verifyStateFeeDivisor, maxWalk, refName); err != nil {
		return err
	}
	// Cross-ref range dedup (issue #59): the verdict for an (old,new) pair
	// is ref-independent, so N refs pushed to the same tip cost one walk.
	if state.ranges[rangeKey(old, new)] {
		return nil
	}
	// Commit verdicts load only past the range cache (Codex round 11): a
	// range-hit ref above never pays for the push's accumulated verdicts.
	// The load is fee-charged like the sidecar one.
	if err := state.loadVerdicts(); err != nil {
		return fmt.Errorf("verify-sig: %w", err) // fail closed
	}
	if err := state.chargeWalk(state.verdictLines/verifyStateFeeDivisor, maxWalk, refName); err != nil {
		return err
	}
	pending, _, err := introducedPending(ctx, state, refName, old, new, budget)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		// Nothing new to verify — no keyring access needed, mirroring the
		// deletion rule. Deliberately records NO range verdict: okrange
		// lines are persisted only when commits were actually verified
		// (below), so the state file stays bounded by the spawn budget no
		// matter how many distinct zero-introduced or fully-cached ranges
		// one push carries (Codex rounds 4 + 5 — per-ref free records
		// would grow the file once per ref and make every later hook
		// rescan it). Recomputing a cached range costs one rev-list whose
		// examined commits charge the walk allowance, keeping the total
		// bounded.
		return nil
	}

	org := mustEnv(EnvOrg)
	ms, err := meta.Open(ctx, mustEnv(envHookPGURL...))
	if err != nil {
		return fmt.Errorf("verify-sig: open metadata store: %w", err)
	}
	defer ms.Close()
	keys, err := ms.SigningKeysForOrg(ctx, org)
	if err != nil {
		return fmt.Errorf("verify-sig: load signing keys for org %q: %w", org, err)
	}
	if len(keys) == 0 {
		// Cache the denial on the first introduced commit — the first line
		// any identical-range walk emits — so later refs at the same tip
		// abort after one rev-list line instead of repeating the walk
		// (Codex round 3: without this, a keyless org turned N same-tip
		// creations into N full-history walks). The message still names
		// the tip, matching the documented log-line shape.
		reason := fmt.Sprintf("no signing keys registered for org %q", org)
		if err := state.markCommitDenied(pending[0], reason); err != nil {
			return fmt.Errorf("verify-sig: %w", err)
		}
		return verifySigDenied(refName, new, reason)
	}

	v, cleanup, err := newSigVerifier(keys)
	if err != nil {
		return fmt.Errorf("verify-sig: %w", err)
	}
	defer cleanup()

	for _, sha := range pending {
		reason := v.verifyCommit(ctx, sha)
		if reason != "" {
			if err := state.markCommitDenied(sha, reason); err != nil {
				return fmt.Errorf("verify-sig: %w", err)
			}
			return verifySigDenied(refName, sha, reason)
		}
		if err := state.markCommitOK(sha); err != nil {
			return fmt.Errorf("verify-sig: %w", err)
		}
	}
	if err := state.markRangeOK(old, new); err != nil {
		return fmt.Errorf("verify-sig: %w", err)
	}
	return nil
}

// verifySigDenied builds the canonical denial error. Its message is the
// documented log line; the hook runner prints it to stderr, which
// receive-pack relays to both the client and the server log.
func verifySigDenied(refName, sha, reason string) error {
	return fmt.Errorf("verify-sig denied %s at %s: %s", refName, sha, reason)
}

// introducedPending walks the commits reachable from new but not from old
// (branch creation: old is ZeroSHA, so everything reachable from new) and
// returns the ones still needing a verifier spawn — dropping cached-OK
// commits, failing on a cached denial, and aborting the walk the moment the
// uncached count exceeds the remaining budget. Runs in the bare repo git
// put us in (cwd, GIT_DIR set by receive-pack).
//
// The walk is deliberately NOT bounded by other refs (`--not --all` and
// friends): the upstream branch-protection contract is explicit that the
// comparison is against the protected ref's own prior tip, never against
// other refs — a commit that already exists elsewhere is still checked the
// first time it is brought onto the protected ref, so unsigned commits
// cannot be laundered through an unpoliced ref and then referenced by a
// protected creation (branch-protection.md "Commit signing verifications";
// Codex round 1 on issue #59's original direction). Cost is bounded by the
// per-push verdict cache and the budget instead — and the budget bounds the
// walk ITSELF, two ways (Codex rounds 2 and 3): rev-list output is consumed
// incrementally with the subprocess killed at abort, uncached commits are
// bounded by the remaining spawn budget, and EVERY examined commit — cached
// verdicts included — charges the push's cumulative walk allowance
// (verifyWalkBudgetMultiplier × budget, persisted across hook processes),
// so refs at distinct ancestors of an already-verified chain cannot re-walk
// the history once per ref for free. Budget 0 means unlimited by explicit
// operator choice.
func introducedPending(ctx context.Context, state *verifyState, refName, old, new string, budget int) (pending []string, examined int64, retErr error) {
	assertf(state != nil && budget >= 0 && state.verdictsLoaded, "introducedPending: state=%v budget=%d", state, budget)
	if !ref.ValidSHA(new) || new == ref.ZeroSHA {
		return nil, 0, fmt.Errorf("verify-sig: introducedPending: bad new sha %q", new)
	}
	args := []string{"rev-list", new}
	if old != ref.ZeroSHA {
		args = append(args, "^"+old)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, 0, fmt.Errorf("verify-sig: rev-list stdout: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, 0, fmt.Errorf("verify-sig: start git %s: %w", strings.Join(args, " "), err)
	}
	// Reap the subprocess on every early return; cancel() has already
	// killed it then, so the Wait error is expected and irrelevant.
	abort := func() { cancel(); _ = cmd.Wait() }

	// Persist the examined-commit count on every exit path — denials and
	// aborts included, because a non-atomic push continues with its other
	// refs. Zero-examined walks persist nothing (see verifySig — the state
	// file must not grow once per zero-introduced ref). A persist failure
	// must not let the walk allowance silently reset, so it fails the call
	// closed unless it is already failing.
	defer func() {
		if err := state.markWalked(examined); err != nil && retErr == nil {
			pending, retErr = nil, fmt.Errorf("verify-sig: %w", err)
		}
	}()
	maxWalk := walkBudget(budget)

	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		// Subprocess output: fail closed on surprises, never panic mid-push.
		if !ref.ValidSHA(line) {
			abort()
			return nil, examined, fmt.Errorf("verify-sig: rev-list emitted %q", line)
		}
		examined++
		if maxWalk > 0 && state.walked+examined > maxWalk {
			abort()
			return nil, examined, fmt.Errorf("verify-sig: push exceeds the per-push history-walk allowance: %s pushed the total past %d examined commits (%d× CODESTORAGE_VERIFY_SIG_BUDGET; raise the budget or split the push)",
				refName, maxWalk, verifyWalkBudgetMultiplier)
		}
		if state.ok[line] {
			continue
		}
		// Cached denial: an earlier ref already spawned the verifier for
		// this commit and failed — short-circuit without another spawn.
		if reason, denied := state.deny[line]; denied {
			abort()
			return nil, examined, verifySigDenied(refName, line, reason)
		}
		pending = append(pending, line)
		if budget > 0 && state.spent+len(pending) > budget {
			abort()
			return nil, examined, fmt.Errorf("verify-sig: push exceeds the per-push verification budget: %s introduces more than %d unverified commits with %d of %d verifications already used (raise CODESTORAGE_VERIFY_SIG_BUDGET or push fewer new commits)",
				refName, budget-state.spent, state.spent, budget)
		}
	}
	if err := sc.Err(); err != nil {
		abort()
		return nil, examined, fmt.Errorf("verify-sig: reading rev-list: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, examined, fmt.Errorf("verify-sig: git %s: %w", strings.Join(args, " "), err)
	}
	return pending, examined, nil
}

// sigVerifier delegates signature checking to git's own machinery
// (`git verify-commit`), which dispatches SSH vs OpenPGP by signature type:
// SSH keys go into a temporary allowed-signers file, PGP keys into a
// temporary GNUPGHOME keyring. Battle-tested verification, no bespoke
// crypto (phase-2 ADR).
type sigVerifier struct {
	allowedSigners string // path to the temp allowed_signers file
	gnupgHome      string // temp GNUPGHOME with imported PGP keys; "" when unused
}

// newSigVerifier materializes the org keyring for git. Registered keys are
// SSH public key lines or ASCII-armored PGP public key blocks (meta.Store
// contract); anything else fails closed. A missing gpg binary downgrades
// PGP support only: SSH verification still works and PGP-signed commits
// then fail with a clear reason.
func newSigVerifier(keys []meta.SigningKey) (*sigVerifier, func(), error) {
	dir, err := os.MkdirTemp("", "codestorage-verify-sig-*")
	if err != nil {
		return nil, func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }

	var signers strings.Builder
	var pgpKeys []string
	for _, k := range keys {
		text := strings.TrimSpace(k.KeyText)
		switch {
		case strings.Contains(text, "BEGIN PGP PUBLIC KEY BLOCK"):
			pgpKeys = append(pgpKeys, text)
		case looksLikeSSHPublicKey(text):
			// Any signer principal, git namespace only (git signs with
			// namespace "git"; tag/push signatures use it too).
			fmt.Fprintf(&signers, "* namespaces=\"git\" %s\n", text)
		default:
			cleanup()
			return nil, func() {}, fmt.Errorf("registered signing key %d is neither an SSH public key line nor an armored PGP block", k.ID)
		}
	}

	v := &sigVerifier{allowedSigners: filepath.Join(dir, "allowed_signers")}
	if err := os.WriteFile(v.allowedSigners, []byte(signers.String()), 0o600); err != nil {
		cleanup()
		return nil, func() {}, err
	}

	if len(pgpKeys) > 0 {
		home := filepath.Join(dir, "gnupg")
		if err := os.Mkdir(home, 0o700); err != nil {
			cleanup()
			return nil, func() {}, err
		}
		for _, k := range pgpKeys {
			cmd := exec.Command("gpg", "--batch", "--quiet", "--import")
			cmd.Env = append(os.Environ(), "GNUPGHOME="+home)
			cmd.Stdin = strings.NewReader(k)
			if out, err := cmd.CombinedOutput(); err != nil {
				if errors.Is(err, exec.ErrNotFound) {
					// Documented risk: hosts without gpg keep SSH
					// verification; PGP-signed commits fail closed below
					// with git's own missing-gpg reason.
					fmt.Fprintln(os.Stderr, "verify-sig: gpg binary not found; registered PGP keys are unusable on this host")
					break
				}
				cleanup()
				return nil, func() {}, fmt.Errorf("import registered PGP key: %v: %s", err, strings.TrimSpace(string(out)))
			}
		}
		v.gnupgHome = home
	}
	return v, cleanup, nil
}

// looksLikeSSHPublicKey is a defensive shape check for one authorized-keys
// style line ("<type> <base64> [comment]"). Embedded CR/LF would corrupt the
// allowed_signers file, so any multi-line text fails the check.
func looksLikeSSHPublicKey(line string) bool {
	if strings.ContainsAny(line, "\r\n") {
		return false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return false
	}
	for _, prefix := range []string{"ssh-", "sk-ssh-", "ecdsa-sha2-", "sk-ecdsa-"} {
		if strings.HasPrefix(fields[0], prefix) {
			return true
		}
	}
	return false
}

// verifyCommit returns "" when sha carries a valid signature from the
// materialized keyring, else a short human-readable reason. Delegates to
// `git verify-commit`, which picks gpg or ssh-keygen by signature type.
func (v *sigVerifier) verifyCommit(ctx context.Context, sha string) (reason string) {
	if !ref.ValidSHA(sha) {
		// Callers feed rev-list output; a malformed sha fails closed as a
		// denial reason instead of crashing receive-pack.
		return fmt.Sprintf("malformed commit sha %q", sha)
	}
	logVerifySpawn(sha)
	cmd := exec.CommandContext(ctx, "git",
		"-c", "gpg.ssh.allowedSignersFile="+v.allowedSigners,
		"verify-commit", sha)
	cmd.Env = os.Environ()
	if v.gnupgHome != "" {
		cmd.Env = append(cmd.Env, "GNUPGHOME="+v.gnupgHome)
	}
	out, err := cmd.CombinedOutput()
	if err == nil {
		return ""
	}
	msg := condenseGitReason(out)
	if msg == "" {
		// git verify-commit is silent for unsigned commits.
		msg = "commit is not signed"
	}
	return msg
}

// condenseGitReason squeezes git/gpg/ssh-keygen diagnostics into one short
// line for the per-ref rejection message.
func condenseGitReason(out []byte) string {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var kept []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		kept = append(kept, l)
	}
	msg := strings.Join(kept, "; ")
	const max = 300
	if len(msg) > max {
		msg = msg[:max] + "..."
	}
	return msg
}

// objectExists reports whether sha names an object in the repo git put us
// in (during pre-receive the quarantine is visible via git's alternates).
func objectExists(sha string) bool {
	err := exec.Command("git", "cat-file", "-e", sha).Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return false
	}
	// Not an exit status (git missing, spawn failure): a bug, fail fast.
	panic(fmt.Sprintf("githttp assertion failed: cat-file -e %s: %v", sha, err))
}

// objectInMainStore reports whether sha is resolvable WITHOUT the
// quarantine. git runs pre-receive with GIT_OBJECT_DIRECTORY pointed at the
// quarantine and the main store as an alternate (githooks(5)); stripping
// those variables makes cat-file consult only $GIT_DIR/objects, so a true
// result means the object is already stored and needs nothing from the
// incoming pack.
func objectInMainStore(sha string) bool {
	cmd := exec.Command("git", "cat-file", "-e", sha)
	for _, kv := range os.Environ() {
		switch {
		case strings.HasPrefix(kv, "GIT_QUARANTINE_PATH="),
			strings.HasPrefix(kv, "GIT_OBJECT_DIRECTORY="),
			strings.HasPrefix(kv, "GIT_ALTERNATE_OBJECT_DIRECTORIES="):
			// dropped: quarantine must stay invisible to this check
		default:
			cmd.Env = append(cmd.Env, kv)
		}
	}
	err := cmd.Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return false
	}
	// Not an exit status (git missing, spawn failure): a bug, fail fast.
	panic(fmt.Sprintf("githttp assertion failed: main-store cat-file -e %s: %v", sha, err))
}

// isAncestor reports whether old is an ancestor of new in the repo git put
// us in (hooks run with cwd = bare repo root, GIT_DIR set by git).
func isAncestor(old, new string) bool {
	cmd := exec.Command("git", "merge-base", "--is-ancestor", old, new)
	err := cmd.Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false
	}
	// Anything else (missing objects, bad SHA) is a bug: fail fast.
	panic(fmt.Sprintf("githttp assertion failed: merge-base --is-ancestor %s %s: %v", old, new, err))
}
