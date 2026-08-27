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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
)

// Env names used to route request context from the HTTP handler into the
// git hooks (which re-exec this binary). CODESTORAGE_TEST_* fault-injection
// knobs are honored only when CODESTORAGE_TEST_MODE=1.
const (
	EnvRepo = "CODESTORAGE_GIT_REPO"
	// EnvSyncClaim carries the loopback sync push's claim-holder token
	// (auth.Claims.SyncClaim, issue #86): the update hook refuses the ref
	// CAS when the repo's sync claim is no longer held by this holder,
	// fencing a stale run's in-flight force push out of the ref store.
	EnvSyncClaim  = "CODESTORAGE_SYNC_CLAIM"
	EnvRefsPolicy = "CODESTORAGE_GIT_REFS_POLICY"
	EnvOps        = "CODESTORAGE_GIT_OPS"
	// EnvScopes carries the pushing token's scope list (JSON array) into
	// the hook subprocess. The update hook itself never consults it; the
	// hook-side forward_on_push poke does (issue #90): auto-forwarding
	// exports every local head/tag upstream, so a write-only token's push
	// must not trigger it (the same git:read gate the explicit
	// push-upstream route enforces).
	EnvScopes = "CODESTORAGE_GIT_SCOPES"
	// EnvOrg names the org whose signing keys verify-sig loads. Shared with
	// the serve config on purpose: the server sets it explicitly for hooks.
	EnvOrg = "CODESTORAGE_ORG"
	// EnvPushHold carries the absolute path of this push's hold file
	// (issue #170, hydrate.go push-hold machinery). pre-receive reserves
	// every ref of the push in it before any update-hook CAS can run;
	// concurrent full syncs skip held refs so they cannot race
	// receive-pack's own local update of a ref it just committed, and
	// D/F-conflicting concurrent pushes serialize on the reservation.
	// Minted per request by the server and stripped from inherited env
	// like the other per-push inputs.
	EnvPushHold = "CODESTORAGE_GIT_PUSH_HOLD"
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
	// EnvPushDrainMS shrinks the post-copy drain read deadline
	// (pushDrainTimeout) so the e2e lane can prove hook phases outlive it
	// (issue #141's receive-pack sibling); ignored outside test mode.
	EnvPushDrainMS = "CODESTORAGE_TEST_PUSH_DRAIN_MS"
	// EnvCASDrop/EnvCASDropHard hold a marker-file path; the first update
	// hook to create the marker lets its CAS statement commit, then reports
	// a synthetic connection-drop error (issue #2's ambiguous outcome).
	// The hard variant also fails the recovery recheck read, as a truly
	// dead connection would. Later invocations pass through untouched.
	EnvCASDrop     = "CODESTORAGE_TEST_CAS_DROP"
	EnvCASDropHard = "CODESTORAGE_TEST_CAS_DROP_HARD"
	// EnvUpdateBarrier holds a handshake-file path prefix; the first update
	// hook to create <path>.ready pauses AFTER its authority CAS commits
	// and before the hook exits — i.e. before receive-pack performs its own
	// local ref update — until the test creates <path>.release (issue #170:
	// this is exactly the window a concurrent hydrate can full-sync the
	// just-committed ref into the shared cache repo). Later invocations
	// pass through untouched.
	EnvUpdateBarrier = "CODESTORAGE_TEST_UPDATE_BARRIER"
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
// events.RefStore decorator so git pushes emit push events (docs/reference/webhooks.md)
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

	// Full-policy verdicts are evaluated AT MOST once per command per push
	// — memoized per index so every pre-receive consumer (the atomic
	// whole-push gate, the reservation filter's signature-gated
	// evaluations, and the pack publication gate) shares ONE evaluation
	// (Codex rounds 4+6 P2, round-16 P2 on #173: each evaluation charges
	// the shared verify-sig walk budget a per-evaluation fee, so
	// re-evaluating the same command in two pre-receive gates double-fees
	// the push). The no-pack non-atomic path still evaluates nothing
	// beyond its signature-gated commands.
	verdicts := make([]error, len(commands))
	verdictDone := make([]bool, len(commands))
	evalOne := func(i int) error {
		if !verdictDone[i] {
			c := commands[i]
			verdicts[i] = enforceRefPolicy(ctx, repo, c.ref, c.old, c.new)
			verdictDone[i] = true
		}
		return verdicts[i]
	}
	evalVerdicts := func() {
		for i := range commands {
			_ = evalOne(i)
		}
	}
	// Atomic pushes fail whole on any denial (issue #28 semantics), and
	// doing so BEFORE the hold reservation means a restricted token's
	// denied command can never hold a namespace it cannot write — atomic
	// or not (Codex round-4 security P2 on #173). Unupdatable NAMES are
	// whole-push denials too (Codex round-6 P1): git runs earlier update
	// hooks — whose authority CASes commit — before rejecting the funny
	// name and aborting the local atomic transaction, so letting one
	// through recreates the authority-accepted/client-rejected wedge.
	if atomicPush {
		// Evaluated one command at a time, name check first, STOPPING at
		// the first denial (Codex round-10 P2): a denied atomic push's
		// outcome is already fixed, and each further evaluation can cost
		// subprocess work (no-force-push runs git merge-base, verify-sig
		// walks and verifies) — eager whole-slice evaluation let one
		// doomed request scale that by its command count. A complete loop
		// leaves verdicts fully populated (all nil) for the later gates.
		for i, c := range commands {
			if !pushRefUpdatable(c.ref) {
				return atomicDenied(fmt.Errorf("ref name %s is not updatable here (namespace root, git-invalid or unmaterializable name, or deeper than %d segments)", c.ref, maxPushRefDepth))
			}
			if err := evalOne(i); err != nil {
				return atomicDenied(err)
			}
		}
		// The push's OWN command set must be representable in one git ref
		// transaction (Codex round-3 P1 on #173) — an ATOMIC-only gate:
		// under --atomic, every update hook's authority CAS commits before
		// git discovers the local D/F collision and aborts the whole
		// transaction, so the denial must come first. Non-atomic pushes
		// keep git's per-ref semantics instead (Codex round-7 P2: a
		// denied parent never survives, so whole-push denial would
		// reject its representable allowed child) — there the update
		// hook's authority-side probe (denyAuthorityDFConflict) declines
		// exactly the loser at commit time, whatever the verdicts did.
		if err := atomicPushDFCheck(commands); err != nil {
			return atomicDenied(err)
		}
	}
	// Push holds (issue #170): reserve the refs this push may commit
	// BEFORE any publication or update-hook CAS, in one atomic
	// check+append per push (reservePushHolds) — concurrent full syncs
	// skip held refs so they cannot race receive-pack's own local ref
	// updates, and a D/F-conflicting concurrent push is briefly waited out
	// or cleanly denied HERE, pre-publication, instead of committing an
	// authority update whose local ref transaction is doomed. Only
	// policy-allowed commands are reserved (Codex round-3 security P2): a
	// denied command never commits, and reserving it would let a
	// restricted token hold namespaces it cannot write for the push
	// window. For atomic pushes every verdict is nil by this point, so
	// the filter naturally reserves the full set. The server removes the
	// hold file when receive-pack exits; the refresher keeps it fresh
	// through the unbounded publication work below, and each update hook
	// chains its own refresher for its runtime.
	if holdPath := os.Getenv(EnvPushHold); holdPath != "" {
		names := make([]string, 0, len(commands))
		var unreserved []string
		eligible := make([]refCommand, 0, len(commands))
		// Non-atomic pushes resolve reservations budget-free, with only
		// signature-gated commands paying a real per-command evaluation
		// — see reservationVerdicts.
		var classes []reservationClass
		var sigVerified []bool
		if !atomicPush {
			classes, sigVerified = reservationVerdicts(repo, commands, evalOne)
		}
		// Verified-verdict markers (Codex round-16 P2): each
		// signature-gated command whose REAL evaluation just passed gets
		// one marker keyed over (ref, old, new), so its update hook
		// consumes the verdict with a single stat instead of charging the
		// per-evaluation verify fee again — with a small
		// CODESTORAGE_VERIFY_SIG_BUDGET, a same-tip fanout of N gated
		// refs costs ~N+1 fee units here, and hooks re-charging another N
		// pushed a fully compliant push past the walk allowance. Inputs
		// are identical (same push, same policy env, same shas), so the
		// hook-side verdict could only repeat this one. Marker count is
		// bounded by the commands that actually PASSED verification.
		for i, ok := range sigVerified {
			if !ok {
				continue
			}
			if err := os.MkdirAll(holdPath+pushHoldVerifiedSuffix, 0o755); err != nil {
				return fmt.Errorf("pre-receive: record verified verdicts: %w", err)
			}
			c := commands[i]
			if err := os.WriteFile(pushHoldVerifiedMarker(holdPath, c.ref, c.old, c.new), nil, 0o600); err != nil {
				return fmt.Errorf("pre-receive: record verified verdict for %s: %w", c.ref, err)
			}
		}
		for i, c := range commands {
			// Reserve only names this server could ever commit (Codex
			// round-5 security P2): git hands namespace roots like the
			// literal refs/heads to pre-receive yet can never complete
			// their local update, and reserving one would hold a D/F
			// ancestor of the whole namespace. The update hook denies
			// such names fail-closed, so skipping them here loses no
			// protection. Atomic pushes reach here all-allowed (the gate
			// above denied whole on any failure), so their verdicts vouch
			// for the full set.
			denied := false
			if !atomicPush {
				denied = classes[i] == reservationDenied
			}
			if !pushRefUpdatable(c.ref) || denied {
				unreserved = append(unreserved, c.ref)
				continue
			}
			eligible = append(eligible, c)
		}
		// Creations whose strict D/F relative is ANOTHER command of this
		// same push are not bulk-reserved (Codex round-9 P1): a
		// non-atomic push creating both refs/heads/a and refs/heads/a/b
		// would otherwise let both hooks skip the commit-time probe —
		// both CASes commit before git's local per-ref update rejects
		// one, leaving the authority unrepresentable. Diverted to the
		// sidecar, each such creation re-probes the authority AT COMMIT
		// TIME, sequenced by git after every earlier sibling's CAS, so
		// exactly the losing relative is declined per-ref (and the
		// delete-then-create swap still lands, deletion first). Checked
		// against ALL command names, not just eligible ones: a command
		// policy-denied here can still be allowed by its own hook's later
		// evaluation (the round-5 divergence) and commit from the sidecar.
		allNames := make([]string, 0, len(commands))
		for _, c := range commands {
			allNames = append(allNames, c.ref)
		}
		cmdSet := newPushHoldSet(allNames)
		var reservedCreations []string
		for _, c := range eligible {
			if c.old == ref.ZeroSHA && c.new != ref.ZeroSHA && cmdSet.conflicts(c.ref) {
				unreserved = append(unreserved, c.ref)
				continue
			}
			names = append(names, c.ref)
			if c.old == ref.ZeroSHA && c.new != ref.ZeroSHA {
				reservedCreations = append(reservedCreations, c.ref)
			}
		}
		// Pre-reservation batch probe (Codex round-7 no-squat property,
		// restored with the round-13/14 push-proportional batch seam):
		// creations already doomed by the CURRENT authority never reserve
		// at all — they divert straight to the sidecar (or deny the whole
		// atomic push), so a crafted set of doomed parents holds nothing
		// while it is probed. The post-reservation probe below stays the
		// airtight racer catch; this one is the cheap common-case filter.
		var probeStore ref.Store
		if len(reservedCreations) > 0 || atomicPush {
			var err error
			probeStore, err = ref.Open(ctx, mustEnv(envHookPGURL...))
			if err != nil {
				return fmt.Errorf("pre-receive: open ref store for reservation probe: %w", err)
			}
			defer probeStore.Close()
		}
		if len(reservedCreations) > 0 {
			doomed, err := authorityDoomedCreations(ctx, probeStore, repo, reservedCreations)
			if err != nil {
				return fmt.Errorf("pre-receive: authority D/F probe: %w", err)
			}
			if len(doomed) > 0 {
				if atomicPush {
					for _, c := range commands {
						if derr, ok := doomed[c.ref]; ok {
							return atomicDenied(derr)
						}
					}
				}
				keptNames := names[:0]
				for _, name := range names {
					if _, ok := doomed[name]; ok {
						unreserved = append(unreserved, name)
						continue
					}
					keptNames = append(keptNames, name)
				}
				names = keptNames
				keptCreations := reservedCreations[:0]
				for _, name := range reservedCreations {
					if _, ok := doomed[name]; !ok {
						keptCreations = append(keptCreations, name)
					}
				}
				reservedCreations = keptCreations
			}
		}
		// Early sidecar-cap check — see the full rationale at the second
		// check below.
		if len(unreserved) > maxPushUnreserved {
			return fmt.Errorf("pre-receive: push carries %d commands that cannot be bulk-reserved (policy-denied, undeliverable, or D/F-conflicted names); the limit is %d — drop those commands or split the push", len(unreserved), maxPushUnreserved)
		}
		if err := reservePushHolds(ctx, holdPath, names); err != nil {
			return fmt.Errorf("pre-receive: reserve push holds: %w", err)
		}
		// The refresher starts the moment the reservation exists (Codex
		// round-12 P1): the post-reservation probe below is sequential
		// store I/O that can outlast pushHoldTTL on a huge creation push,
		// and an EXPIRED hold is ignored by concurrent reservers — a
		// conflicting push could commit against a name the probe already
		// cleared, after which a late-started refresher would revive our
		// hold and the bulk-reserved hook would skip its commit-time
		// guard. The synchronous first touch also refreshes the age the
		// file accrued between the server's mint and this reservation.
		// (Markers created below start fresh and are re-covered on the
		// next tick; touchPushHold tolerates the sidecar not existing.)
		defer startPushHoldRefresh(holdPath)()
		// Atomic base check (Codex round-14 P1 on #173): every command's
		// exact authority value must still match the client's declared old
		// before ANY update hook can commit — under --atomic a
		// mid-sequence CAS loss cannot retract the authority updates
		// earlier hooks already committed, so git's abort of the local
		// transaction would report the whole push failed while part of it
		// stands in the authority. The check is sound HERE and only here:
		// the reservation above now excludes same-name peer holds (covers
		// semantics), so a held name's authority value is frozen against
		// same-node writers from reservation until release — a stale base
		// (say, a peer that committed while hydration deliberately left
		// the git cache old) is caught before any CAS instead of
		// mid-sequence. One push-proportional batch (refValuesProber);
		// fail closed on store trouble. Cross-node racers remain the
		// documented store-level TOCTOU residual. Non-atomic pushes keep
		// git's per-ref semantics: an individual stale CAS loses alone.
		if atomicPush {
			refNames := make([]string, 0, len(commands))
			for _, c := range commands {
				refNames = append(refNames, c.ref)
			}
			values, err := authorityRefValues(ctx, probeStore, repo, refNames)
			if err != nil {
				return fmt.Errorf("pre-receive: atomic base check: %w", err)
			}
			for _, c := range commands {
				switch got, exists := values[c.ref]; {
				case c.old == ref.ZeroSHA && exists:
					return atomicDenied(fmt.Errorf("%s already exists (created concurrently); fetch first", c.ref))
				case c.old != ref.ZeroSHA && !exists:
					return atomicDenied(fmt.Errorf("%s no longer exists (deleted concurrently); fetch first", c.ref))
				case c.old != ref.ZeroSHA && got != c.old:
					return atomicDenied(fmt.Errorf("%s moved concurrently (expected %.8s); fetch first", c.ref, c.old))
				}
			}
		}
		// Authority D/F probe for every bulk-reserved creation, run
		// strictly AFTER the reservation landed — the only airtight probe
		// point on this node (Codex rounds 7-11 lineage; round-11 P1
		// closed the last ordering hole): a same-node conflicting creation
		// can only CAS while holding its own reservation, and that
		// reservation must have been RELEASED before ours landed (else we
		// would still be waiting on it) — so a probe after our reservation
		// observes every conflicting commit, including a racer that
		// reserved, committed, and released entirely inside our
		// probe-to-reservation window, which any pre-reservation probe
		// (with or without waited-name tracking) misses. Doomed late
		// losers: atomic pushes are denied WHOLE before any hook CAS
		// (Codex round-8 P1); non-atomic ones are RELEASED from the hold
		// file and diverted to the sidecar, so a doomed name squats on its
		// existing D/F relatives' namespace only for the probe's
		// round-trip, not the push window (the round-7 no-squat property),
		// and its own hook's commit-time probe declines it. Creations
		// only: updates and deletions cannot introduce a D/F pair. One
		// push-proportional batch per probe (the dfConflictProber seam —
		// rounds 8-14). Fail closed on store trouble. Cross-node racers
		// remain the documented store-level TOCTOU residual. A
		// delete-then-create D/F swap in ONE push still works: the
		// creation is sidecar-diverted by the intra-push check above and
		// self-reserves at hook time, after the deletion's CAS.
		if len(reservedCreations) > 0 {
			doomed, err := authorityDoomedCreations(ctx, probeStore, repo, reservedCreations)
			if err != nil {
				return fmt.Errorf("pre-receive: post-reservation authority D/F probe: %w", err)
			}
			if len(doomed) > 0 {
				if atomicPush {
					for _, c := range commands {
						if derr, ok := doomed[c.ref]; ok {
							return atomicDenied(derr)
						}
					}
				}
				release := make([]string, 0, len(doomed))
				for name := range doomed {
					unreserved = append(unreserved, name)
					release = append(release, name)
				}
				if err := removePushHolds(holdPath, release); err != nil {
					return fmt.Errorf("pre-receive: release doomed reservations: %w", err)
				}
			}
		}
		// The sidecar is bounded (Codex round-9 P2): unreserved membership
		// is attacker-chosen — a crafted push can carry hundreds of
		// thousands of tiny undeliverable commands under the byte cap, and
		// each marker below costs one inode on the shared cache volume
		// (plus crash-recovery sweep work). Real pushes carry at most a
		// handful of denied or undeliverable commands, so a generous cap
		// denies only crafted traffic; bulk mirror pushes are unaffected —
		// their reservable refs share the single hold file. Checked twice:
		// once above, BEFORE reservation, so a push that is already over
		// the cap on pre-computable grounds (policy-denied, unupdatable,
		// intra-push D/F) never reserves or probes at all (Codex round-12
		// security P2), and once here for the probe's late additions.
		if len(unreserved) > maxPushUnreserved {
			return fmt.Errorf("pre-receive: push carries %d commands that cannot be bulk-reserved (policy-denied, undeliverable, or D/F-conflicted names); the limit is %d — drop those commands or split the push", len(unreserved), maxPushUnreserved)
		}
		// The (typically empty) remainder becomes one marker file per
		// name in the sidecar directory, so an update hook's membership
		// check is a single stat — absent means bulk-reserved, present
		// means self-reserve before the CAS. Per-ref markers, not a
		// listing file: allowed hooks re-reading a crafted push's large
		// denied list would be O(allowed × denied) (Codex rounds 6+7
		// P2s). Excluded from hold readers; the server removes it with
		// the hold file, the TTL sweep covers leaks.
		if len(unreserved) > 0 {
			if err := os.MkdirAll(holdPath+pushHoldUnreservedSuffix, 0o755); err != nil {
				return fmt.Errorf("pre-receive: record unreserved refs: %w", err)
			}
			for _, name := range unreserved {
				if err := os.WriteFile(pushHoldUnreservedMarker(holdPath, name), nil, 0o600); err != nil {
					return fmt.Errorf("pre-receive: record unreserved ref: %w", err)
				}
			}
		}
	}

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
		// No pack means nothing can leak. The atomic whole-push policy
		// denial (issue #28's durable-CAS twist) already happened above,
		// before the hold reservation, from the shared verdicts.
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
	evalVerdicts()
	if err := preReceivePolicyGate(commands, verdicts, atomicPush); err != nil {
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

	// An unpublished pack whose key is already REGISTERED is a RESTORE,
	// not fresh growth (Codex security round 70): its bytes are in the
	// usage counter, floors scanned during the absence are deflated by
	// them, and uploading it through the fresh path would re-materialize
	// the bytes with no distinct cover (settlement consumes the admission
	// reservation without crediting or bumping — RowsAffected 0, the
	// zero-credit no-op). Such packs skip the admission charge (the
	// zero-growth rule: their bytes are already counted) and the fresh
	// upload loop; the post-registration EnsurePublished pass Stats them
	// missing and restores them through the covered, generation-bumping
	// seam (initial cover → renewed upload → invalidation handoff).
	restores := make([]bool, len(packs))
	for i := range packs {
		if published[i] {
			continue
		}
		has, err := st.HasPack(ctx, repo, packs[i].Key)
		if err != nil {
			return fmt.Errorf("pre-receive: registry probe %s: %w", packs[i].Key, err)
		}
		restores[i] = has
	}

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
	var reservedGens map[string]string
	var quota int64 // 0 = quota disabled (restore covers are skipped too)
	var ttl time.Duration
	if quotaRaw := os.Getenv(EnvPackQuota); quotaRaw != "" {
		q, err := strconv.ParseInt(quotaRaw, 10, 64)
		if err != nil || q <= 0 {
			// The env is set by our own server (never by the client), but
			// fail closed on nonsense instead of waving the push through.
			return fmt.Errorf("pre-receive: malformed %s=%q", EnvPackQuota, quotaRaw)
		}
		quota = q
		ttl, err = reservationTTL()
		if err != nil {
			return fmt.Errorf("pre-receive: %w", err)
		}
		var unpublished []ref.Pack
		for i := range packs {
			if !published[i] && !restores[i] {
				unpublished = append(unpublished, packs[i])
			}
		}
		if len(unpublished) > 0 {
			floor, floorGen, err := hookUsageFloor(ctx, bl, st, repo)
			if err != nil {
				return fmt.Errorf("pre-receive: %w", err)
			}
			var dec ref.ReserveDecision
			for attempt := 0; ; attempt++ {
				// Deadline-bounded by the handoff margin (Codex round 42) —
				// see the LFS PUT twin: the floor's orphan cover must
				// outlive the admission snapshot.
				rsvCtx, rsvCancel := context.WithTimeout(ctx, UsageFloorHandoffMargin)
				dec, err = st.ReservePackBytes(rsvCtx, blob.QuotaRepoGroup(repo), unpublished, ref.NewReservationOwner(), floor, floorGen, quota, ttl)
				rsvCancel()
				if err != nil {
					return fmt.Errorf("pre-receive: reserve pack bytes: %w", err)
				}
				if !dec.FloorStale {
					break
				}
				// The ledger's transactional generation recheck rejected
				// the floor (Codex round 60): a restore, adoption, or
				// retained-bytes debit moved it between the floor's own
				// check and the reserve. Self-list a fresh floor (with
				// its generation) and retry once; a second rejection
				// fails the push closed — the client retries.
				if attempt >= 1 {
					return fmt.Errorf("pre-receive: usage floor went stale during admission twice, retry")
				}
				floor, floorGen, err = hookSelfListFloor(ctx, bl, st, repo)
				if err != nil {
					return fmt.Errorf("pre-receive: %w", err)
				}
			}
			if dec.KeyCapExceeded {
				// Transient row-cap denial, not a quota breach (Codex
				// round 43): rows expire with the TTL, so tell the
				// client to RETRY instead of reporting a quota failure
				// whose arithmetic may even show the push fits.
				return fmt.Errorf("pre-receive: too many in-flight uploads reserved for this repository, retry later")
			}
			if dec.RenewalDegraded {
				// Fail-closed while quota accounting is degraded (Codex
				// round 26 on PR #132, security): transient, retry.
				return fmt.Errorf("pre-receive: storage quota accounting temporarily degraded, retry later")
			}
			if !dec.OK {
				return fmt.Errorf("pre-receive: repository pack quota exceeded: push adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight pushes), quota is %d bytes",
					ref.IncomingBytes(unpublished), dec.UsedBytes, dec.ReservedBytes, quota)
			}
			reservedGens = dec.Gens
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
		if published[i] || restores[i] {
			continue // restores go through the covered seam below
		}
		if err := bl.UploadFile(ctx, packs[i].Key, pf); err != nil {
			// PROVEN-clean failure: retire this push's attempts (Codex
			// round 30; replacing round 22's settle-or-TTL-only rule).
			// The rows are shared with concurrent identical pushes via
			// refresh, so the retire is per-attempt refcounted and
			// gen-bound (round 33: a stale retire never touches a
			// replacement row), and each key retires only on EVIDENCE its
			// blob is absent — the failed upload itself may have
			// committed with its response lost, and keys uploaded by
			// earlier iterations are durable orphans whose rows must keep
			// covering them (round 33). Ambiguity and retire failures
			// only leave what the TTL always collected.
			if len(reservedGens) > 0 {
				proven := make(map[string]string, len(reservedGens))
				for k, g := range reservedGens {
					if _, serr := bl.Stat(ctx, k); errors.Is(serr, blob.ErrNotFound) {
						proven[k] = g
					}
				}
				if len(proven) > 0 {
					if rerr := st.RetirePackAttempts(ctx, blob.QuotaRepoGroup(repo), proven); rerr != nil {
						fmt.Fprintf(os.Stderr, "codestorage: retire reservations: %v\n", rerr)
					}
				}
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
	// Packs whose blobs pre-existed this push (crash-window retries) are
	// ADOPTIONS: cached floors may already contain their bytes, so the
	// registration bumps the shared floor generation for them (Codex
	// round 53).
	//
	for i := range packs {
		packs[i].Adopted = published[i]
	}
	regErr := st.RegisterPacks(ctx, repo, packs)
	if regErr != nil {
		regErr = fmt.Errorf("pre-receive: register packs: %w", regErr)
	}
	// A restore here re-materializes counter-accounted bytes (Codex round
	// 58, the pack twin of the LFS zero-growth repair): floors scanned
	// while a registered pack's blob was absent hide equally sized
	// rowless legacy bytes. Each restore is covered by a reservation
	// BEFORE its upload (stale-floor admissions see the returning bytes),
	// and any restore bumps the shared floor generation afterward so
	// future serves re-scan.
	// The floor-generation bump is the restore's invalidation HANDOFF
	// (Codex rounds 68–70): it runs inside the seam, under a live cover,
	// detached from the hook's fate, retried until it commits or cover
	// continuity is lost.
	bump := func(bctx context.Context) error {
		return st.BumpLFSAdoptionGen(bctx, BaseRepo(repo))
	}
	for i, p := range packs {
		var cover func(context.Context, int64) error
		if quota > 0 {
			cover = func(cctx context.Context, size int64) error {
				return ref.ReserveRestoreCover(cctx, st, blob.QuotaRepoGroup(repo), p.Key, size, ttl)
			}
		}
		if _, err := blob.EnsurePublished(ctx, bl, p.Key, packFiles[i], cover, bump); err != nil {
			return errors.Join(regErr, fmt.Errorf("pre-receive: verify %s: %w", p.Key, err))
		}
	}
	return regErr
}

// maxPushRefDepth caps the path depth of refs this server will commit.
// Git imposes no limit, but no legitimate layout nests remotely this deep,
// and depth is attacker-chosen amplification: the authority D/F probe
// costs one point-read per ancestor segment (Codex round-5 security P2 on
// #173 demonstrated a 3KB ref name with 1,502 segments = 1,502 store reads
// per tiny push command). Deny-with-message keeps the bound explicit.
const maxPushRefDepth = 32

// maxPushRefSegmentBytes and maxPushRefNameBytes bound what the git files
// backend can MATERIALIZE (Codex round-16 P1 on #173): git's own
// check-ref-format accepts a 256-byte path component, but creating the
// loose ref's ".lock" file then fails ENAMETOOLONG (NAME_MAX 255) only
// AFTER the update hook's authority CAS committed — one non-atomic push
// leaves an authority-only ref that every later hydration also fails to
// write, wedging the repo. 200 leaves headroom under NAME_MAX for the
// lock suffix; 900 keeps the whole loose-ref path comfortably inside
// PATH_MAX for any sane cache root. Stricter than git's format rules by
// design: names past these bounds are unrepresentable in the cache, so
// denying them loses nothing (the parity oracle only requires accepted
// names to be git-valid).
const (
	maxPushRefSegmentBytes = 200
	maxPushRefNameBytes    = 900
)

// maxPushUnreserved caps how many of one push's commands may land in the
// unreserved sidecar (one marker inode each — see preReceive). Membership
// is attacker-chosen: policy-denied, unupdatable, or D/F-conflicted names
// cost the client nothing but bound server-side inode and sweep work, so
// the cap fails crafted pushes closed while staying far above any real
// push's denied-command count. Bulk-RESERVABLE refs (mirror pushes) are
// not counted — they share the single hold file.
const maxPushUnreserved = 1024

// pushRefUpdatable reports whether receive-pack could ever commit name on
// this server: a properly namespaced ref (refs/<ns>/<name>...) of bounded
// depth that git itself considers well-formed. Namespace ROOTS (e.g. the
// literal refs/heads) matter for security (Codex round-5 security P2): git
// hands them to pre-receive but can never complete their local update in a
// cache repo (the namespace directory always exists), so reserving one
// would let a restricted token hold a D/F ancestor of EVERY ref in that
// namespace for the push window. Used to filter reservations and,
// fail-closed, to deny such names in the update hook — where the depth cap
// also bounds the authority D/F probe's per-ref work.
//
// The format rules mirror git's check_refname_format EXACTLY (Codex
// round-9 P1): the atomic whole-push gate leans on this predicate being at
// least as strict as receive-pack's own funny-refname check, which runs
// per-command AFTER earlier commands' update hooks — and their authority
// CASes — already committed. A name this predicate accepts but git later
// rejects aborts the local atomic transaction with the authority moved.
// (Stricter-than-git would instead deny representable pushes, so parity is
// verified against `git check-ref-format` in the unit tests.)
func pushRefUpdatable(name string) bool {
	rest, ok := strings.CutPrefix(name, "refs/")
	if !ok || !strings.Contains(rest, "/") {
		return false
	}
	if len(name) > maxPushRefNameBytes { // filesystem PATH_MAX headroom
		return false
	}
	if strings.HasSuffix(name, ".") { // git: a refname cannot end with a dot
		return false
	}
	segs := strings.Split(name, "/")
	if len(segs) > maxPushRefDepth {
		return false
	}
	for _, seg := range segs {
		if len(seg) > maxPushRefSegmentBytes { // filesystem NAME_MAX headroom
			return false
		}
		if !gitRefSegmentValid(seg) {
			return false
		}
	}
	return true
}

// dfConflictProber is the batched authority D/F probe seam (Codex
// round-13 P2/security P2 on #173): probe work must be proportional to the
// PUSH and indexed on the store, never repo-scaled — the probe runs while
// the push's namespace holds block conflicting same-node writers. Every
// production ref store implements it (PG: two round trips of indexed
// seeks; SQLite: one call of local indexed seeks; s3wal: the single
// manifest read every access pays anyway), compile-asserted so a store
// change cannot silently regress pushes to the per-name fallback.
type dfConflictProber interface {
	DFConflicts(ctx context.Context, repo string, names []string) (map[string]string, error)
}

var (
	_ dfConflictProber = (*ref.PG)(nil)
	_ dfConflictProber = (*ref.SQLite)(nil)
	_ dfConflictProber = (*ref.S3WAL)(nil)
)

// refValuesProber is the batched authority exact-value probe seam for the
// atomic-push base check (Codex round-14 P1 on #173) — the same
// push-proportional contract as dfConflictProber. Every production ref
// store implements it (PG/SQLite: indexed membership reads; s3wal: the
// single manifest read every access pays anyway), compile-asserted so a
// store change cannot silently regress atomic pushes to per-name reads.
type refValuesProber interface {
	RefValues(ctx context.Context, repo string, names []string) (map[string]string, error)
}

var (
	_ refValuesProber = (*ref.PG)(nil)
	_ refValuesProber = (*ref.SQLite)(nil)
	_ refValuesProber = (*ref.S3WAL)(nil)
)

// authorityRefValues returns the authority's current sha for each of the
// named refs, omitting absent ones. Production stores answer through the
// batched refValuesProber seam; stores without it (test fakes, foreign
// decorators) fall back to per-name GetRef point-reads — acceptable there
// because atomic pushes bound names by the client's command list. Store
// errors fail the whole probe; callers fail closed.
func authorityRefValues(ctx context.Context, st ref.RefStore, repo string, names []string) (map[string]string, error) {
	if prober, ok := st.(refValuesProber); ok {
		return prober.RefValues(ctx, repo, names)
	}
	out := make(map[string]string, len(names))
	for _, name := range names {
		switch sha, err := st.GetRef(ctx, repo, name); {
		case err == nil:
			out[name] = sha
		case errors.Is(err, ref.ErrNotFound):
			// absent: omitted
		default:
			return nil, fmt.Errorf("authority value probe for %s: %w", name, err)
		}
	}
	return out, nil
}

// gitRefSegmentValid mirrors git's check_refname_component (refs.c) for one
// slash-separated segment: no empty segments (this also rejects leading,
// trailing, and doubled slashes), no segment starting with "." or ending
// with ".lock", no ".." or "@{" sequences, and none of git's forbidden
// bytes — ASCII controls, DEL, space, "~^:?*[\". A trailing "." is legal
// for a NON-final segment ("refs/heads/foo./bar" is git-valid); the final
// segment's trailing dot is the whole-name rule checked by the caller.
func gitRefSegmentValid(seg string) bool {
	if seg == "" || seg[0] == '.' || strings.HasSuffix(seg, ".lock") {
		return false
	}
	var last byte
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		switch {
		case c < 0x20 || c == 0x7f || c == ' ' || c == '~' || c == '^' ||
			c == ':' || c == '?' || c == '*' || c == '[' || c == '\\':
			return false
		case c == '.' && last == '.':
			return false
		case c == '{' && last == '@':
			return false
		}
		last = c
	}
	return true
}

// atomicPushDFCheck denies an ATOMIC push whose command set cannot land in
// one git ref transaction (Codex round-3 P1 on #173): under --atomic every
// update hook's authority CAS commits before git discovers the local D/F
// collision and aborts the whole transaction, leaving the authority with
// refs the client was told failed. Any strict-prefix pair is fatal unless
// both sides are deletions (the end state has neither; git's files backend
// cannot even reorder a delete+create D/F swap inside one transaction —
// see syncRefsStreamed's D/F ordering note). Non-atomic pushes are NOT
// checked: git's per-ref semantics apply, and the update hook's authority
// probe declines exactly the losing ref at commit time (Codex round-7 P2:
// a whole-push denial here would reject an allowed child whose denied
// parent never survives).
func atomicPushDFCheck(commands []refCommand) error {
	isDelete := make(map[string]bool, len(commands))
	names := make([]string, 0, len(commands))
	for _, c := range commands {
		isDelete[c.ref] = c.new == ref.ZeroSHA
		names = append(names, c.ref)
	}
	set := newPushHoldSet(names)
	for _, c := range commands {
		// Strict directory ancestors of c.ref within the command set;
		// checking ancestors alone visits every conflicting pair once.
		for i := 0; i < len(c.ref); i++ {
			if c.ref[i] != '/' || !set.contains(c.ref[:i]) {
				continue
			}
			ancName := c.ref[:i]
			if isDelete[ancName] && isDelete[c.ref] {
				continue
			}
			return fmt.Errorf("refs %s and %s conflict (directory/file) in one atomic transaction; drop --atomic or split the push", ancName, c.ref)
		}
	}
	return nil
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
func preReceivePolicyGate(commands []refCommand, verdicts []error, atomicPush bool) error {
	assertf(len(commands) > 0, "preReceivePolicyGate: no commands")
	assertf(len(verdicts) == len(commands), "preReceivePolicyGate: %d verdicts for %d commands", len(verdicts), len(commands))
	var firstErr error
	needsPack := false
	for i, c := range commands {
		// Verdicts are the caller's single shared policy evaluation
		// (Codex round-4 P2 on #173: re-evaluating here double-charged
		// the push's verify-sig walk budget).
		if err := verdicts[i]; err != nil {
			if atomicPush {
				// Defensive: preReceive already denied atomic pushes
				// carrying any denial before reserving holds.
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
func updateHook(ctx context.Context, args []string) (retErr error) {
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
	// Names this server will never commit are denied outright: namespace
	// roots cannot complete their local update anyway, and the depth cap
	// bounds the authority D/F probe below to maxPushRefDepth point-reads
	// per created ref — ref depth is attacker-chosen amplification against
	// the shared store otherwise (Codex round-5 security P2 on #173).
	if !pushRefUpdatable(refName) {
		return fmt.Errorf("update: ref name %s is not updatable here (namespace root, git-invalid name, or deeper than %d segments)", refName, maxPushRefDepth)
	}
	repo := mustEnv(EnvRepo)

	// Push hold, refresher half (issue #170): keep the shared hold file
	// fresh across this hook's FULL runtime — pre-CAS policy/fence work
	// and unbounded post-CAS decorator work alike, because in an atomic
	// push an EARLIER hook's committed CAS relies on later hooks keeping
	// the file fresh until git's final local transaction (Codex round-2
	// P2 on #173).
	holdPath := os.Getenv(EnvPushHold)
	if holdPath != "" {
		defer startPushHoldRefresh(holdPath)()
	}

	// Verified-verdict marker (Codex round-16 P2): pre-receive already ran
	// the FULL policy evaluation for this exact (ref, old, new) — same
	// push, same policy env — and it passed; re-evaluating here could
	// only repeat the verdict while charging the shared verify-sig walk
	// budget another per-evaluation fee, failing large compliant same-tip
	// fanouts under small budgets. Any stat trouble falls back closed to
	// a real evaluation.
	sigVerified := false
	if holdPath != "" {
		if _, err := os.Stat(pushHoldVerifiedMarker(holdPath, refName, old, new)); err == nil {
			sigVerified = true
		}
	}
	if !sigVerified {
		if err := enforceRefPolicy(ctx, repo, refName, old, new); err != nil {
			return err
		}
	}

	// Push hold, reservation half: ensure THIS ref is held before its CAS
	// can commit, independent of pre-receive's verdicts (Codex round-5 P2
	// on #173: a command transiently policy-denied in pre-receive — and
	// therefore not bulk-reserved — can still be allowed by the check
	// above and would otherwise commit unheld, reopening the hydrate
	// race). Placed strictly AFTER this hook's own policy check so a
	// genuinely denied ref still cannot hold anything (the round-3
	// namespace-squatting property).
	//
	// O(1) in the common case (Codex round-6 P2: a per-ref peer scan goes
	// quadratic across concurrent high-fanout pushes): pre-receive
	// bulk-reserved every updatable, reservation-allowed ref before any
	// hook ran and recorded the remainder in the sidecar — no sidecar
	// means this ref is already held, and only sidecar-listed refs (the
	// rare divergence case) pay the full atomic check+reserve.
	bulkReserved := false
	if holdPath != "" {
		switch _, err := os.Stat(pushHoldUnreservedMarker(holdPath, refName)); {
		case errors.Is(err, os.ErrNotExist):
			// Bulk-reserved (no sidecar, or this ref not marked); verify
			// the reservation file actually exists (fail closed, one
			// stat).
			if _, err := os.Stat(holdPath); err != nil {
				return fmt.Errorf("update: push hold reservation missing (pre-receive owns it): %w", err)
			}
			bulkReserved = true
		case err != nil:
			return fmt.Errorf("update: check unreserved marker: %w", err)
		default:
			if err := reservePushHolds(ctx, holdPath, []string{refName}); err != nil {
				return fmt.Errorf("update: reserve push hold: %w", err)
			}
			// Release the self-reservation on any denial below (Codex
			// round-16 P2): a sidecar-listed creation denied by the
			// commit-time D/F probe (or fence, or CAS loss) commits
			// nothing, yet its name would otherwise stay held until the
			// whole receive-pack exits — a doomed parent bundled into a
			// large push squats its existing relatives' namespace for the
			// push's full duration, timing out their legitimate updates.
			// Best-effort: a failed release leaves the pre-fix
			// until-exit hold, and the denial itself still stands. On
			// success the reservation is kept through the CAS, the local
			// update, and receive-pack exit, exactly like bulk holds.
			defer func() {
				if retErr != nil {
					_ = removePushHolds(holdPath, []string{refName})
				}
			}()
		}
	}
	// Sync claim fence (issue #86): a loopback sync push carries its
	// run's claim-holder token. The precheck refuses obviously-stale
	// pushes with a clear message (and fails closed on meta trouble);
	// the fenced CAS context below closes the residual check-to-update
	// race by re-evaluating the holder ATOMICALLY inside the CAS
	// statement itself — a steal committing after the precheck makes the
	// CAS report won=false instead of publishing the stale ref.
	if holder := os.Getenv(EnvSyncClaim); holder != "" {
		if err := enforceSyncClaimFence(ctx, repo, holder); err != nil {
			return err
		}
		ctx = ref.WithSyncClaimFence(ctx, repo, holder)
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
	// Authority-side directory/file guard (Codex round-4 P1 on #173, and
	// the pre-existing divergence class beneath it): the CAS below is
	// keyed by the exact ref name, so nothing in the store itself stops
	// refs/heads/a and refs/heads/a/b from coexisting in the authority —
	// a state no git ref store can materialize, which wedges every future
	// hydration of the repo. Pre-receive's checks are advisory (policy
	// verdicts there cannot perfectly predict this hook's), so the commit
	// point itself verifies against the AUTHORITY's current refs: a
	// CREATION whose strict D/F relative exists is declined per-ref,
	// exactly like a policy denial.
	//
	// Creations only (Codex round-5 P1: on s3wal every probe read is a
	// manifest fetch, so update-heavy fanout pushes must not pay it):
	// updates and deletions never change the authority's NAME SET, so if
	// the no-strict-D/F-pair invariant held before them it holds after —
	// only a creation adds a name and needs the probe. Probing costs the
	// created ref O(path depth) point-reads; its own CAS chain already
	// pays manifest round-trips of the same order on s3wal.
	//
	// Skipped for bulk-reserved refs (Codex round-8 security P2): those
	// already passed pre-receive's batch authority probe, the hold has
	// serialized same-node D/F writers ever since, and re-probing here
	// would re-pay per-creation manifest reads on s3wal. Sidecar refs
	// (pre-receive could not vouch) and hookless invocations keep the
	// commit-time probe.
	//
	// Probed on the raw store: the test fault decorators below must keep
	// scoping to the CAS itself. TOCTOU residual: conflicting creations
	// racing from different NODES (or API ref writes) can still cross
	// inside the probe-to-CAS window — for bulk-reserved refs that window
	// is pre-receive-probe-to-CAS; closing it needs store-level
	// transactional enforcement and is tracked outside this fix.
	if old == ref.ZeroSHA && new != ref.ZeroSHA && !bulkReserved {
		if err := denyAuthorityDFConflict(ctx, st, repo, refName); err != nil {
			return err
		}
	}
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
	// Test seam (issue #170): pause between the committed authority CAS and
	// receive-pack's local ref update so the e2e lane can deterministically
	// interleave a concurrent hydrate into that window.
	if testMode() {
		if p := os.Getenv(EnvUpdateBarrier); p != "" {
			pauseAtUpdateBarrier(p)
		}
	}
	return nil
}

// pauseAtUpdateBarrier implements the EnvUpdateBarrier handshake: the first
// invocation to create <p>.ready owns the pause and polls for <p>.release;
// later invocations pass through (O_EXCL one-shot, like injectCASDrop). The
// wait is bounded so a broken test wedges nothing.
func pauseAtUpdateBarrier(p string) {
	f, err := os.OpenFile(p+".ready", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return // barrier already consumed
	}
	_ = f.Close()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(p + ".release"); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// authorityDoomedCreations reports which of the created names are doomed
// by a strict D/F relative existing in the authority. Production stores
// answer through the batched dfConflictProber seam — push-proportional,
// indexed work in a fixed number of store calls (Codex rounds 8-13
// lineage: per-name manifest fetches, repo-wide paged scans, and
// per-creation seek storms were each rejected in turn; the batch probe is
// the terminal shape). Stores without the seam (test fakes, foreign
// decorators) fall back to deduped ancestor point-reads plus a
// decorated-order seek-merge of single-row ListRefsAfter probes.
//
// Decorated order in the fallback is load-bearing (compare
// s3walEachBaseCoverOrder): bytes like '.' sort below '/', so RAW-sorted
// creations ("refs/heads/a" before "refs/heads/a.b") have NON-monotonic
// range starts, and a skip window carried across them would silently skip
// a real conflict.
//
// Store errors fail the whole probe; callers fail closed.
func authorityDoomedCreations(ctx context.Context, st ref.RefStore, repo string, names []string) (map[string]error, error) {
	doomed := map[string]error{}
	if len(names) == 0 {
		return doomed, nil
	}
	deny := func(created, existing string) {
		if _, dup := doomed[created]; !dup {
			doomed[created] = fmt.Errorf("%s conflicts with existing ref %s (directory/file); delete %s first", created, existing, existing)
		}
	}
	if prober, ok := st.(dfConflictProber); ok {
		conflicts, err := prober.DFConflicts(ctx, repo, names)
		if err != nil {
			return nil, fmt.Errorf("directory/file batch probe: %w", err)
		}
		for created, existing := range conflicts {
			deny(created, existing)
		}
		return doomed, nil
	}
	// Ancestor arm, memoized across the set.
	ancestor := map[string]bool{} // strict prefix -> exists in authority
	for _, name := range names {
		for i := 0; i < len(name); i++ {
			if name[i] != '/' {
				continue
			}
			anc := name[:i]
			exists, probed := ancestor[anc]
			if !probed {
				switch _, err := st.GetRef(ctx, repo, anc); {
				case err == nil:
					exists = true
				case errors.Is(err, ref.ErrNotFound):
					exists = false
				default:
					return nil, fmt.Errorf("directory/file probe for %s: %w", name, err)
				}
				ancestor[anc] = exists
			}
			if exists {
				deny(name, anc)
				break
			}
		}
	}
	// Descendant arm: at most one single-row probe per creation, walked in
	// decorated order so lo only moves forward.
	sorted := append([]string(nil), names...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i]+"/" < sorted[j]+"/" })
	lastKey := "" // when haveProbe: the authority's first key at or after the last probed lo
	haveProbe := false
	for _, n := range sorted {
		lo := n + "/"
		if haveProbe {
			if strings.HasPrefix(lastKey, lo) {
				deny(n, lastKey) // the witness key itself is a descendant
				continue
			}
			// Any descendant of n lives in [n+"/", n+"0") ('/'+1 == '0'),
			// and the last probe proved [prev lo, lastKey) empty; lo only
			// grows in decorated order, so lastKey beyond the range means
			// no descendants without a fresh probe.
			if lastKey >= n+"0" {
				continue
			}
		}
		page, err := st.ListRefsAfter(ctx, repo, lo, 1)
		if err != nil {
			return nil, fmt.Errorf("directory/file probe for %s: %w", n, err)
		}
		if len(page) == 0 {
			break // nothing at or after lo; later creations sit even higher
		}
		lastKey, haveProbe = page[0].Name, true
		if strings.HasPrefix(lastKey, lo) {
			deny(n, lastKey)
		}
	}
	return doomed, nil
}

// denyAuthorityDFConflict declines a non-deletion update of name when the
// authority currently contains a strict directory/file relative of it — a
// strict ancestor (one GetRef point-read per path segment) or any
// descendant (one single-row ListRefsAfter page at name+"/", the smallest
// key any descendant can exceed). Store errors fail CLOSED: an unprobeable
// authority is not proof of representability, and the denial is retryable.
func denyAuthorityDFConflict(ctx context.Context, st ref.RefStore, repo, name string) error {
	for i := 0; i < len(name); i++ {
		if name[i] != '/' {
			continue
		}
		anc := name[:i]
		switch _, err := st.GetRef(ctx, repo, anc); {
		case err == nil:
			return fmt.Errorf("update: %s conflicts with existing ref %s (directory/file); delete %s first", name, anc, anc)
		case !errors.Is(err, ref.ErrNotFound):
			return fmt.Errorf("update: directory/file probe for %s: %w", name, err)
		}
	}
	page, err := st.ListRefsAfter(ctx, repo, name+"/", 1)
	if err != nil {
		return fmt.Errorf("update: directory/file probe for %s: %w", name, err)
	}
	if len(page) > 0 && strings.HasPrefix(page[0].Name, name+"/") {
		return fmt.Errorf("update: %s conflicts with existing ref %s (directory/file); delete %s first", name, page[0].Name, page[0].Name)
	}
	return nil
}

// enforceSyncClaimFence refuses a loopback sync push whose run no longer
// holds the repo's sync claim (issue #86). The token TTL already fences
// NEW requests out past the lease, but an in-flight receive-pack that
// authenticated before a steal could still commit its stale force push and
// regress the thief's newer refs — this check runs per ref update,
// immediately before the CAS, shrinking that window from an unbounded
// in-flight request to the gap between this read and the CAS commit. It
// fails CLOSED (no meta access ⇒ no fence ⇒ no update): a spuriously
// refused sync push is retried by the pending-request machinery, while an
// unfenced stale push is unrecoverable.
func enforceSyncClaimFence(ctx context.Context, repo, holder string) error {
	ms, err := meta.Open(ctx, mustEnv(envHookPGURL...))
	if err != nil {
		return fmt.Errorf("sync claim fence: open meta store: %w", err)
	}
	defer ms.Close()
	up, err := ms.GetRepoUpstream(ctx, repo)
	if err != nil {
		return fmt.Errorf("sync claim fence: load upstream: %w", err)
	}
	if up.SyncClaimHolder != holder {
		return fmt.Errorf("sync claim fence: claim now held by %q, refusing the stale sync push", up.SyncClaimHolder)
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

// enforceRefPolicy evaluates the token's ref policies (docs/reference/authentication.md, docs/upstream
// branch-protection.md) for one ref update. The deprecated repo-wide `ops`
// claim is folded in as a fallback exactly once, here at parse time.
func enforceRefPolicy(ctx context.Context, repo, refName, old, new string) error {
	return evalRefPolicy(ctx, repo, refName, old, new, true)
}

// reservationClass is the budget-free reservation filter's verdict about
// one command (Codex rounds 6, 12, 13 on #173): allowed and denied are
// final without touching the shared verify budget; needs-verify means the
// command is signature-gated, so vouching for its reservation requires the
// push's REAL verdicts (pre-receive evaluates them once in that case —
// see preReceive — because flatly diverting signature-gated commands
// breaks legitimate large signed fanout pushes at the sidecar cap, while
// flatly allowing them lets unsigned pushers squat D/F namespaces).
type reservationClass int

const (
	reservationAllowed reservationClass = iota
	reservationDenied
	reservationNeedsVerify
)

// errReservationNeedsSig is evalRefPolicy's budget-free sentinel for
// signature-gated non-deletion commands: not a denial, but not a verdict
// the reservation filter may trust without real verification.
var errReservationNeedsSig = errors.New("signature-gated command needs real verification")

func classifyReservation(repo, refName, old, new string) reservationClass {
	switch err := evalRefPolicy(context.Background(), repo, refName, old, new, false); {
	case err == nil:
		return reservationAllowed
	case errors.Is(err, errReservationNeedsSig):
		return reservationNeedsVerify
	default:
		return reservationDenied
	}
}

// reservationVerdicts resolves each NON-ATOMIC command to a final allowed
// or denied reservation class. Every command is classified budget-free
// first; only the signature-GATED remainder gets a real policy evaluation.
// Gated commands cannot be vouched for budget-free (round-12 security P2:
// assuming them allowed lets unsigned pushers squat D/F namespaces), but
// flatly diverting them breaks legitimate large SIGNED fanout pushes at
// the sidecar cap (round-13 P2) — so exactly they pay verification here:
// valid signed refs bulk-reserve, invalid ones divert.
// STRICTLY per-gated-command, never the push's full verdicts (Codex
// round-15 P2): a mixed push carrying one gated ref plus thousands of
// distinct no-force-push pairs must not launch one git merge-base per
// ungated pair here — rounds 4+6 kept the no-pack path subprocess-free
// for everything the budget-free classifier can already answer, and the
// ungated commands' own update hooks enforce the real ancestry checks.
// Real evaluations go through the caller's per-index memo (evalOne), so
// the pack-path publication gate never re-fees them, and the returned
// verified flags mark gated commands whose evaluation PASSED — the caller
// persists those as verdict markers the update hooks consume without
// charging the verify budget again (Codex round-16 P2).
func reservationVerdicts(repo string, commands []refCommand, evalOne func(int) error) (classes []reservationClass, verified []bool) {
	classes = make([]reservationClass, len(commands))
	verified = make([]bool, len(commands))
	for i, c := range commands {
		classes[i] = classifyReservation(repo, c.ref, c.old, c.new)
	}
	for i := range commands {
		if classes[i] != reservationNeedsVerify {
			continue
		}
		if evalOne(i) != nil {
			classes[i] = reservationDenied
		} else {
			classes[i] = reservationAllowed
			verified[i] = true
		}
	}
	return classes, verified
}

// evalRefPolicy is the shared evaluator. fullEval=false is the budget-free
// mode for the reservation classifier: it replaces signature verification
// with a fail-unreservable verdict for non-deletion commands (callers must
// not charge the shared verify-sig state and walk budget) and skips the
// no-force-push ancestry subprocess (treating the update as reservable —
// the ref's own update hook re-runs the full check before its CAS).
// Classification must never launch subprocesses: its cost is
// attacker-chosen via the push's command list (Codex round-14 security
// P2).
func evalRefPolicy(ctx context.Context, repo, refName, old, new string, fullEval bool) error {
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
			// updates AND deletions. docs/reference/authentication.md: ref policies are the
			// only branch-protection mechanism, so allowing deletion
			// would make the policy trivially bypassable
			// (delete + re-push = force push). Safe to check here:
			// quarantined objects were migrated into the main store
			// after pre-receive, before update hooks run.
			if old != ref.ZeroSHA {
				if new == ref.ZeroSHA {
					return fmt.Errorf("policy no-force-push denies deletion of %s", refName)
				}
				// Budget-free callers (the reservation classifier) skip
				// the ancestry subprocess and treat the update as
				// reservable (Codex round-14 security P2: a no-pack push
				// advancing thousands of refs from DISTINCT old commits
				// would otherwise launch one git merge-base per pair in
				// pre-receive — the memo only dedupes identical pairs —
				// on top of the one each update hook process pays
				// anyway). Reserving a doomed non-fast-forward update
				// squats nothing: its name already exists in the
				// authority, which blocks D/F relatives regardless, and
				// any git:write pusher could hold the same name with a
				// valid update. The commit is still policed — the ref's
				// own update hook re-runs this check with fullEval
				// before its CAS.
				if fullEval && !isAncestor(old, new) {
					return fmt.Errorf("policy no-force-push denies non-fast-forward update to %s", refName)
				}
			}
		case auth.OpVerifySig:
			if fullEval {
				if err := verifySig(ctx, refName, old, new); err != nil {
					return err
				}
			} else if new != ref.ZeroSHA {
				// Budget-free callers (the reservation filter) cannot
				// vouch for a signature-gated creation or update without
				// real verification (Codex round-12 security P2: treating
				// it as allowed would let a pusher WITHOUT a valid
				// signing key bulk-reserve the name — a D/F namespace
				// squat for the push window). The sentinel routes the
				// caller to the push's REAL verdicts. Deletions introduce
				// no commits, always pass verify-sig, and stay allowed.
				return fmt.Errorf("%s: %w", refName, errReservationNeedsSig)
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
// keys (docs/reference/authentication.md verify-sig, branch-protection.md "Commit signing
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
		// Keyring construction is deterministic on the registered key
		// material, so its failure holds for the whole push: cache it on
		// the first introduced commit like the empty-keyring denial, or a
		// registered-but-unimportable PGP key would re-spawn gpg once per
		// ref at the same tip without charging any budget (Codex round
		// 15).
		reason := fmt.Sprintf("signing keyring unusable: %v", err)
		if cacheErr := state.markCommitDenied(pending[0], reason); cacheErr != nil {
			return fmt.Errorf("verify-sig: %w", cacheErr)
		}
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
			return nil, examined, state.exhaust(fmt.Sprintf("verify-sig: push exceeds the per-push history-walk allowance: %s pushed the total past %d examined commits (%d× CODESTORAGE_VERIFY_SIG_BUDGET; raise the budget or split the push)",
				refName, maxWalk, verifyWalkBudgetMultiplier))
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

// ancestryMemo caches isAncestor verdicts per (old, new) pair for this
// process's lifetime (Codex round-11 security P2): pre-receive's policy
// passes evaluate whole command lists, and a crafted non-atomic push
// advancing thousands of refs between the SAME two commits under a
// no-force-push policy would otherwise launch one git merge-base
// subprocess per command — identical pairs now cost one. Never stale:
// ancestry between two fixed SHAs is a property of the immutable,
// content-addressed object graph, so no concurrent ref movement can
// change a cached verdict. Update hooks are one process per ref and
// evaluate a single pair, so they neither need nor abuse the memo.
var (
	ancestryMemoMu sync.Mutex
	ancestryMemo   = map[[2]string]bool{}
)

// isAncestor reports whether old is an ancestor of new in the repo git put
// us in (hooks run with cwd = bare repo root, GIT_DIR set by git).
func isAncestor(old, new string) bool {
	key := [2]string{old, new}
	ancestryMemoMu.Lock()
	got, ok := ancestryMemo[key]
	ancestryMemoMu.Unlock()
	if ok {
		return got
	}
	got = isAncestorUncached(old, new)
	ancestryMemoMu.Lock()
	ancestryMemo[key] = got
	ancestryMemoMu.Unlock()
	return got
}

func isAncestorUncached(old, new string) bool {
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
