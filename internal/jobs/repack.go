// repack.go implements the insert-triggered background repack (issue #1):
// per-repo pack rows otherwise grow monotonically with every push and API
// write (cache repos pin gc.auto=0 / receive.unpackLimit=0), inflating
// hydration cost and blob-request amplification without bound. The repack
// job folds a repo's registered packs into one consolidated pack:
//
//  1. snapshot the registered pack rows (the fold input),
//  2. select the fold input under per-fold object/byte budgets — sizes and
//     exact object counts come from per-candidate point reads, so selection
//     is decided BEFORE anything is downloaded (repack runs automatically
//     on writes, so one fold's work must be bounded even against
//     adversarial pack shapes; see selectFoldInput),
//  3. materialize only the selected packs into the cache repo,
//  4. write ONE self-contained pack holding the union of the selected
//     packs' objects,
//  5. upload the new pack blob (publish-before-flip: bytes must be durable
//     before any row references them),
//  6. install the fold with ref.RefStore.ReplacePacks — one transaction that
//     deletes exactly the selected rows and inserts the new one.
//
// A lost install (another node folded first) stops cleanly; there is no
// eager retry — the next write past the threshold re-triggers. NOTHING is
// ever deleted from blob storage on this path: superseded pack blobs and a
// lost fold's own upload alike become unregistered, age past the orphan
// sweeper's threshold, and are collected by its crash-safe quarantine
// protocol (issue #5) — synchronous deletion raced concurrent identical
// folds on content-addressed keys and could not be made crash-safe without
// duplicating that protocol. Concurrent hydrators that raced the swap
// re-list and retry via the bounded loop in githttp.HydratePacks (issue #6).
//
// Fold contents — why the object UNION and not `git pack-objects --all`
// (reachable objects): reachability at fold time is NOT a safe upper bound
// for what the deleted rows must keep serving. A push registers its pack in
// pre-receive and CASes refs in the update hook moments later — a fold
// snapshotting between the two would drop the (momentarily unreachable)
// pushed objects, then the ref CAS lands and points refs at objects no
// registered pack contains: cold-hydrate corruption no swap-time CAS can
// prevent. Packing the union of the snapshotted packs' objects instead
// preserves the invariant that the registered object set never shrinks, so
// any interleaving of registrations and ref CASes stays consistent. The
// cost is that unreachable objects (crashed pushes, deleted branches) are
// carried forward; reclaiming them safely needs a reachability grace
// protocol and is out of scope here.
package jobs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// MaterializeFunc materializes EXACTLY the given registered packs into
// repo's cache repo and returns its path. It is injected — wiring composes
// githttp.EnsureCacheRepo + githttp.MaterializePack over the serve cache
// dir — so this package never imports githttp, whose own tests import this
// package's sweeper. The fold selects its input under byte/object budgets
// BEFORE materializing, so a full-repo hydrate here would defeat them; the
// implementation must honor ctx (the fold's wall-clock budget) in lock
// waits, downloads, and git subprocesses, and must propagate
// blob.ErrNotFound (a selected pack's blob vanishing means a concurrent
// fold already replaced the snapshotted rows).
type MaterializeFunc func(ctx context.Context, repo string, packs []ref.Pack) (repoPath string, err error)

// runGitStdin executes git with args and the given stdin, returning stdout
// only (stderr is captured separately and surfaced in the error text, like
// githttp.runGit: warnings must never contaminate parsed stdout). The
// subprocess is killed when ctx expires — the fold's runtime budget must
// bound pack-objects, whose work scales with logical (inflated) content,
// not with the compressed input sizes the byte budget sees.
func runGitStdin(ctx context.Context, dir string, stdin io.Reader, args ...string) (string, error) {
	assertf(len(args) > 0, "runGitStdin: no args")
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Stdin = stdin
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w: %s",
			strings.Join(args, " "), normalizeCtxErr(ctx, err), strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// normalizeCtxErr folds the context's own error into a subprocess failure
// caused by cancellation: CommandContext KILLS the child on expiry, and the
// raw *exec.ExitError ("signal: killed") does not match
// errors.Is(err, context.DeadlineExceeded) — but callers classify
// deterministic fold-budget exhaustion by exactly that identity (the river
// worker cancels terminally instead of multiplying the budget by its retry
// count).
func normalizeCtxErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return fmt.Errorf("%w: %w", cerr, err)
	}
	return err
}

// Repacker folds one repo's registered packs into a consolidated pack.
// Construct with NewRepacker; Repack is safe to run concurrently with
// pushes, API writes, hydrators, sweeps, and other repackers (ReplacePacks
// is the only mutation and it is a CAS).
type Repacker struct {
	store       ref.RefStore
	blobs       blob.BlobStore
	materialize MaterializeFunc
	logger      *slog.Logger

	// FollowUpThreshold, when >= 2, is the registered-pack count at which
	// a just-finished fold is followed by another one (queue backends
	// consult NeedsFollowUp): packs registered after the fold's row
	// snapshot survive it, and a burst could otherwise finish above the
	// trigger threshold with nothing scheduled until the next write.
	// Wiring sets it to the trigger threshold; 0 disables follow-ups.
	FollowUpThreshold int

	// MaxFoldObjects/MaxFoldBytes bound ONE fold's input (0 = default):
	// repack runs automatically on writes, and a git:write caller must not
	// be able to make the service enumerate, recompress, and upload an
	// unbounded object set on every threshold crossing. Packs are selected
	// oldest-first under the cumulative budgets (see selectFoldInput);
	// oversized leftovers stay registered as-is, so a huge repo converges
	// to O(size/budget) consolidated rows instead of one — bounded work
	// per fold beats one unbounded fold.
	MaxFoldObjects int
	MaxFoldBytes   int64
	// MaxFoldDuration bounds ONE fold's wall-clock runtime (0 = default).
	// The input budgets above measure compressed pack bytes, which
	// undercount CPU: a few-hundred-KiB pack can inflate to hundreds of
	// MiB of logical content that pack-objects re-delta-compresses. The
	// runtime budget caps the actual work directly — the whole fold runs
	// under a deadline and every git subprocess it spawns is killed on
	// expiry. A timed-out fold is crash-equivalent at every point by
	// design (at worst one unregistered uploaded blob, sweeper class).
	MaxFoldDuration time.Duration

	// BeforeSwap is a test seam: when set, it runs after the consolidated
	// pack's blob upload and before ReplacePacks — the fold-install race
	// window — receiving the pending pack (its content-addressed blob key
	// in particular; a listing diff cannot identify it when an identical
	// orphan already exists). Tests and the DST harness use it to
	// interleave competing folds deterministically. Never set in
	// production wiring.
	BeforeSwap func(ctx context.Context, pending ref.Pack) error
}

// FollowUpDue reports repo's registered pack count and whether it has
// reached FollowUpThreshold — the recheck queue backends use to schedule
// one more fold for registrations that landed mid-fold. The count lets the
// river worker distinguish growth from a wedged plateau (a repo stuck
// at/above the threshold with nothing pairable must not chain no-op
// successors forever). Read failures propagate: inside the river worker's
// completion transaction a silently-false answer would commit the job
// away without a successor, stranding coalesced triggers — the caller
// decides whether to snooze (river) or log and defer (local).
func (r *Repacker) FollowUpDue(ctx context.Context, repo string) (count int, due bool, err error) {
	assertf(repo != "", "FollowUpDue: empty repo")
	if r.FollowUpThreshold < 2 {
		return 0, false, nil
	}
	packs, err := r.store.ListPacks(ctx, repo)
	if err != nil {
		return 0, false, fmt.Errorf("follow-up recheck: %w", err)
	}
	return len(packs), len(packs) >= r.FollowUpThreshold, nil
}

// NewRepacker builds a Repacker over the raw ref/blob stores and the
// injected materialization seam (normally githttp cache-repo helpers
// closed over the serve cache dir, so the fold packs in the same cache the
// git surface uses and a successful fold leaves this node warm).
func NewRepacker(store ref.RefStore, blobs blob.BlobStore, materialize MaterializeFunc, logger *slog.Logger) *Repacker {
	assertf(store != nil, "NewRepacker: nil ref store")
	assertf(blobs != nil, "NewRepacker: nil blob store")
	assertf(materialize != nil, "NewRepacker: nil materialize func")
	assertf(logger != nil, "NewRepacker: nil logger")
	return &Repacker{store: store, blobs: blobs, materialize: materialize, logger: logger}
}

// Default per-fold budgets (see the Repacker fields): one million objects
// bounds the in-memory union at tens of MB, 2 GiB of input pack bytes
// bounds one fold's download/upload traffic, and five minutes of wall-clock
// runtime bounds its CPU regardless of how the inputs compress.
const (
	defaultMaxFoldObjects  = 1 << 20
	defaultMaxFoldBytes    = 2 << 30
	defaultMaxFoldDuration = 5 * time.Minute
)

// repackVerifyReserve is the fold-budget slice that must remain before the
// swap may start: the post-swap publication verification (one Stat, rarely
// a repair re-upload) runs under the same fold deadline, and a swap whose
// verify leg gets zero time would leave the committed row unverified.
const repackVerifyReserve = 30 * time.Second

// Repack runs one fold attempt for repo. folded reports whether a fold WAS
// OR MAY HAVE BEEN installed — true on a won swap even when its post-swap
// verification fails, and true on an ambiguous swap error (committed with
// the response lost): queue backends key their follow-up threshold recheck
// on folded, and packs registered after the fold's snapshot must not
// strand when the swap actually landed. A no-op (fewer than two registered
// packs, or budgets selecting fewer than two) and a cleanly LOST race both
// return (false, nil) — a loss proves a concurrent fold won, and in every
// deployment shape that winner runs the same follow-up recheck.
func (r *Repacker) Repack(ctx context.Context, repo string) (folded bool, err error) {
	assertf(repo != "", "Repack: empty repo")
	// Runtime budget: the fold's deadline covers everything below —
	// hydration, enumeration, pack-objects (killed via CommandContext on
	// expiry), upload, and swap. Aborting anywhere is crash-equivalent.
	ctx, cancel := context.WithTimeout(ctx, r.durationBudget())
	defer cancel()

	// Snapshot the fold input. Packs registered after this listing are
	// simply not part of the fold and survive the swap untouched.
	old, err := r.store.ListPacks(ctx, repo)
	if err != nil {
		return false, fmt.Errorf("repack %s: list packs: %w", repo, err)
	}
	if len(old) < 2 {
		return false, nil // nothing to fold
	}

	// Joint two-budget selection (bytes and exact object counts from
	// per-candidate point reads before any download; see selectFoldInput).
	repoPath, fold, err := r.selectFoldInput(ctx, repo, old)
	if errors.Is(err, blob.ErrNotFound) {
		// A candidate's blob vanished between the listing and its
		// download: a concurrent fold already replaced the snapshotted
		// rows, so this fold's input is stale — ReplacePacks would lose
		// anyway. Stop cleanly; nothing uploaded.
		r.logger.Info("repack: lost fold race before packing", "repo", repo, "err", err)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repack %s: %w", repo, err)
	}
	if len(fold) < 2 {
		r.logger.Info("repack: budgets select nothing to fold", "repo", repo, "packs", len(old),
			"byte_budget", r.byteBudget(), "object_budget", r.objectBudget())
		return false, nil
	}
	old = fold

	objs, err := packObjectUnion(ctx, repoPath, old)
	if err != nil {
		return false, fmt.Errorf("repack %s: %w", repo, err)
	}

	id, packPath, err := writeConsolidatedPack(ctx, repoPath, objs)
	if err != nil {
		return false, fmt.Errorf("repack %s: %w", repo, err)
	}
	fi, err := os.Stat(packPath)
	if err != nil {
		return false, fmt.Errorf("repack %s: stat consolidated pack: %w", repo, err)
	}
	assertf(fi.Size() > 0, "repack %s: empty consolidated pack %s", repo, packPath)
	// Size feeds ReplacePacks' usage-counter credit (issue #31): the
	// consolidated blob is NEW prefix bytes on top of the lingering
	// superseded ones, so an at-quota repo shows the fold's cost until the
	// sweeper reclaims the retained blobs.
	newPack := ref.Pack{ID: id, Key: blob.PackKey(repo, id), Size: fi.Size()}

	// Publish-before-flip: the blob must be durable before ReplacePacks can
	// commit a row referencing it. A crash after this upload strands only
	// an unregistered blob, which the orphan sweeper collects.
	if err := r.blobs.UploadFile(ctx, newPack.Key, packPath); err != nil {
		return false, fmt.Errorf("repack %s: upload %s: %w", repo, newPack.Key, err)
	}

	if r.BeforeSwap != nil {
		if err := r.BeforeSwap(ctx, newPack); err != nil {
			return false, fmt.Errorf("repack %s: before-swap hook: %w", repo, err)
		}
	}

	// The swap commits a row that MUST then be verified against the
	// sweeper race (below), and that verification runs under this same
	// fold deadline — the budget bounds the fold end to end. Refuse to
	// start the swap without enough budget left for the verify leg:
	// aborting here is crash-equivalent (one unregistered uploaded blob,
	// sweeper class), while a swap whose verification gets zero time
	// would leave the committed row unverified until the sweeper's heal.
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < repackVerifyReserve {
		return false, fmt.Errorf("repack %s: fold budget exhausted before swap (%v left < %v reserve): %w",
			repo, time.Until(deadline).Round(time.Millisecond), repackVerifyReserve, context.DeadlineExceeded)
	}

	won, err := r.store.ReplacePacks(ctx, repo, old, newPack)
	if err != nil {
		// Ambiguous by construction (the transaction may have committed
		// with the response lost), so the uploaded blob must NOT be
		// deleted here. RESOLVE the ambiguity by probing a DISTINGUISHING
		// old row — one whose id differs from newPack.ID, which always
		// exists (len(old) >= 2, and the content-derived newPack.ID can
		// coincide with at most one input). newPack.Key itself proves
		// nothing: in an identity fold it is registered BEFORE the swap.
		// A committed swap deletes the probe row; a rollback keeps it:
		//
		//   - probe still registered: the swap rolled back — report
		//     folded=false with the error, so a PERSISTENT write failure
		//     stays inside river's retry budget instead of driving
		//     unbounded fold+upload loops through follow-up scheduling.
		//     (A committed swap racing an identical re-push that
		//     re-registers the probe id would misread as rollback — the
		//     cost is one skipped follow-up recheck, recovered by the
		//     next write; never a loop, never corruption.)
		//   - probe gone: a fold committed (ours, or a concurrent one
		//     that folded the row away) — folded=true, and verify the
		//     possibly-referenced blob like every registration path;
		//   - probe unreadable: stay conservative (folded=true + verify).
		probe := old[0]
		if probe.ID == newPack.ID {
			probe = old[1]
		}
		assertf(probe.ID != newPack.ID, "ReplacePacks input %v deduplicates to one id", old)
		// The resolution runs on a DETACHED bounded context: the most
		// common ambiguity cause is the fold deadline expiring during the
		// swap, and a probe on that same expired context could never
		// resolve to rollback — the conservative folded=true would then
		// send a deterministic deadline failure down the follow-up path
		// (local rerun, river successor) instead of river's terminal
		// deadline cancel. The bound keeps a wedged store from pinning
		// the worker (the trigger-check timeout, same scale as one point
		// read).
		probeCtx, cancelProbe := context.WithTimeout(context.WithoutCancel(ctx), repackTriggerTimeout)
		defer cancelProbe()
		has, herr := r.store.HasPack(probeCtx, repo, probe.Key)
		if herr == nil && has {
			return false, fmt.Errorf("repack %s: replace packs (rolled back): %w", repo, err)
		}
		verr := verifyPublished(probeCtx, r.blobs, newPack.Key, packPath)
		return true, errors.Join(fmt.Errorf("repack %s: replace packs: %w", repo, err), verr)
	}
	if !won {
		// Cleanly lost fold: the uploaded blob is NEVER deleted here — it
		// is left for the aged orphan sweeper, whose safeDelete protocol
		// (age threshold, registration handshake, quarantine copy, heal)
		// is the only crash-safe deletion path. Deleting synchronously
		// raced concurrent identical folds on the content-addressed key: a
		// winner installing the same pack between our HasPack read and the
		// delete — with a crash before any restore — would leave its live
		// row permanently referencing missing bytes, unrecoverable because
		// this path (unlike the sweeper) kept no quarantine copy. The cost
		// of deferring is one consolidated blob lingering for the sweep
		// age threshold. When the key IS already registered (a concurrent
		// identical fold won), the blob is live: verify its publication
		// like the won path below.
		if has, herr := r.store.HasPack(ctx, repo, newPack.Key); herr != nil {
			r.logger.Warn("repack: lost fold; registration check failed (blob left for the sweeper)",
				"repo", repo, "key", newPack.Key, "err", herr)
		} else if has {
			if verr := verifyPublished(ctx, r.blobs, newPack.Key, packPath); verr != nil {
				return false, fmt.Errorf("repack %s: lost fold: %w", repo, verr)
			}
		}
		r.logger.Info("repack: lost fold-install race (blob left for the sweeper)",
			"repo", repo, "folded", len(old), "key", newPack.Key)
		return false, nil
	}
	// Writer half of the orphan-sweep coordination (issue #5), exactly like
	// the paths that follow RegisterPacks: a delete-enabled sweeper may
	// have reaped this content-addressed key between the upload above and
	// the swap commit (an identical aged orphan from an earlier crashed
	// fold makes the key look old), with its post-delete registration
	// recheck racing the transaction. Re-verify — and re-upload from the
	// local pack file if needed — now that the row is committed. The swap
	// COMMITTED regardless of this leg's outcome, so folded reports true
	// even on error: queue backends must still run their follow-up check
	// (coalesced mid-fold triggers would otherwise strand), and the
	// sweeper's quarantine heal backstops the bytes.
	if verr := verifyPublished(ctx, r.blobs, newPack.Key, packPath); verr != nil {
		return true, fmt.Errorf("repack %s: fold committed, verification failed: %w", repo, verr)
	}
	r.logger.Info("repack: folded packs", "repo", repo, "folded", len(old), "pack", id)
	return true, nil
}

// verifyPublished re-checks (and if needed restores) the consolidated
// pack's blob after a swap that may have committed a row referencing it.
// Unlike blob.EnsurePublished — whose detached, size-scaled timeout exists
// because request-path registrations must survive their CLIENT hanging up —
// this runs under the caller's fold deadline verbatim: repack has no client
// to outlive, and the fold budget must bound the job end to end (a 2 GiB
// pack's detached repair window would otherwise retain a river worker ~40
// minutes past the advertised budget). The swap-side reserve
// (repackVerifyReserve) guarantees this leg starts with budget in hand; if
// the store stalls it out anyway, the sweeper's quarantine heal is the
// durable backstop for a row whose bytes a racing sweep removed.
func verifyPublished(ctx context.Context, bl blob.BlobStore, key, packPath string) error {
	_, err := bl.Stat(ctx, key)
	if errors.Is(err, blob.ErrNotFound) {
		if uerr := bl.UploadFile(ctx, key, packPath); uerr != nil {
			return fmt.Errorf("verify %s: repair upload: %w", key, uerr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify %s: %w", key, err)
	}
	return nil
}

func (r *Repacker) objectBudget() int {
	if r.MaxFoldObjects > 0 {
		return r.MaxFoldObjects
	}
	return defaultMaxFoldObjects
}

func (r *Repacker) byteBudget() int64 {
	if r.MaxFoldBytes > 0 {
		return r.MaxFoldBytes
	}
	return defaultMaxFoldBytes
}

func (r *Repacker) durationBudget() time.Duration {
	if r.MaxFoldDuration > 0 {
		return r.MaxFoldDuration
	}
	return defaultMaxFoldDuration
}

// foldCandidate is one selection candidate: blob bytes from a point Stat
// and the exact object count from the pack blob's 12-byte header (another
// point read — never a prefix listing, which grows with the repo's whole
// write history under the list-only sweeper). Both dimensions are known
// for EVERY candidate before anything is downloaded. Cumulative counts
// overestimate the union (shared objects count twice) — conservative in
// the safe direction.
type foldCandidate struct {
	pack    ref.Pack
	bytes   int64
	objects int64
}

// selectFoldInput picks the fold's input under BOTH budgets jointly, and
// COMPLETELY: byte sizes (point Stats) and exact object counts (12-byte
// pack-header point reads) are known for every candidate up front, so if
// any two candidates fit both budgets together, a fold is selected — the
// guarantee that killed rounds 21–23's wedge class, where selection had
// to materialize packs in bounded batches just to learn object counts
// and adversarially cheap unpairable packs could starve every batch in
// front of a feasible pair, deterministically, on every trigger. Only
// the SELECTED fold is ever downloaded, so one fold's I/O is bounded by
// one byte budget plus two point reads per registered row.
//
// Skipped packs stay registered untouched: any subset of whole packs
// folds into a self-contained pack (only reachability trimming was
// unsafe), so bounding per-fold work never risks correctness, and
// repeated folds converge oversized repos toward O(size/budget)
// consolidated rows. Rows whose blob point reads report not-found are
// skipped (concurrent fold or repair in flight), as are rows that can
// never pair (individually over either budget). Selection order:
// oldest-first joint greedy (the steady-state path, preferring the
// oldest accumulation), then the exact pair search with oldest-first
// extension. A nil fold is therefore a PROVEN no-op — no two remaining
// packs fit both budgets together — not a missed one.
func (r *Repacker) selectFoldInput(ctx context.Context, repo string, rows []ref.Pack) (repoPath string, fold []ref.Pack, err error) {
	byteBudget, objBudget := r.byteBudget(), int64(r.objectBudget())
	var cands []*foldCandidate
	for _, p := range rows {
		size, err := r.blobs.Stat(ctx, p.Key)
		if errors.Is(err, blob.ErrNotFound) {
			continue // folded away or repair in flight: not a candidate
		}
		if err != nil {
			return "", nil, fmt.Errorf("stat pack blob %s: %w", p.Key, err)
		}
		if size > byteBudget {
			continue // can never pair: skip without reading further
		}
		objects, err := packHeaderObjectCount(ctx, r.blobs, p.Key)
		if errors.Is(err, blob.ErrNotFound) {
			continue // vanished between the point reads: same skip
		}
		if err != nil {
			return "", nil, err
		}
		if objects > objBudget {
			continue // can never pair on the object dimension either
		}
		cands = append(cands, &foldCandidate{pack: p, bytes: size, objects: objects})
	}
	if len(cands) < 2 {
		return "", nil, nil
	}

	sel := jointGreedy(cands, byteBudget, objBudget)
	if sel == nil {
		sel = pairAndExtend(cands, byteBudget, objBudget)
	}
	if sel == nil {
		return "", nil, nil
	}
	repoPath, err = r.materializeFold(ctx, repo, sel)
	if err != nil {
		return "", nil, err
	}
	fold = make([]ref.Pack, len(sel))
	for i, c := range sel {
		fold[i] = c.pack
		// The candidate's point-Stat size feeds ReplacePacks' retained-byte
		// ledger (issue #31): superseded blobs linger under packs/ until a
		// sweep reclaims exactly these bytes from the usage counter.
		fold[i].Size = c.bytes
	}
	return repoPath, fold, nil
}

// jointGreedy accumulates order's candidates under both budgets; a fold
// needs at least two packs, else nil.
func jointGreedy(order []*foldCandidate, byteBudget, objBudget int64) []*foldCandidate {
	var sel []*foldCandidate
	var b, o int64
	for _, c := range order {
		if b+c.bytes > byteBudget || o+c.objects > objBudget {
			continue
		}
		sel = append(sel, c)
		b += c.bytes
		o += c.objects
	}
	if len(sel) >= 2 {
		return sel
	}
	return nil
}

// pairAndExtend is the completeness backstop when the oldest-first greedy
// pairs nothing (its single pick can block every partner): if ANY two
// candidates fit both budgets together, it finds such a pair in
// O(n log n) and tops it up oldest-first. Sorted by bytes ascending, each
// candidate j checks the minimal-object candidate among the strictly
// earlier, byte-compatible prefix (prefix minima; strictly earlier also
// rules out self-pairing) — the best possible partner: if even that one
// busts the object budget, no partner for j exists at all. Any feasible
// pair (a, b) with bytes(a) <= bytes(b) is found no later than j = b, so
// nil proves NO feasible pair — and hence no feasible fold — exists.
func pairAndExtend(cands []*foldCandidate, byteBudget, objBudget int64) []*foldCandidate {
	byBytes := append([]*foldCandidate(nil), cands...)
	sort.SliceStable(byBytes, func(i, j int) bool { return byBytes[i].bytes < byBytes[j].bytes })
	// prefixMin[k]: index in byBytes[0..k] of the minimal object count.
	prefixMin := make([]int, len(byBytes))
	for k := 1; k < len(byBytes); k++ {
		prefixMin[k] = prefixMin[k-1]
		if byBytes[k].objects < byBytes[prefixMin[k-1]].objects {
			prefixMin[k] = k
		}
	}
	for j := 1; j < len(byBytes); j++ {
		// Largest i < j with bytes[i] + bytes[j] <= byteBudget: ascending
		// order makes this a binary search over [0, j).
		limit := byteBudget - byBytes[j].bytes
		i := sort.Search(j, func(k int) bool { return byBytes[k].bytes > limit }) - 1
		if i < 0 {
			continue
		}
		m := prefixMin[i]
		if byBytes[m].objects+byBytes[j].objects > objBudget {
			continue
		}
		// Seed pair found: extend oldest-first (candidate order).
		sel := []*foldCandidate{byBytes[m], byBytes[j]}
		b, o := byBytes[m].bytes+byBytes[j].bytes, byBytes[m].objects+byBytes[j].objects
		for _, c := range cands {
			if c == byBytes[m] || c == byBytes[j] || b+c.bytes > byteBudget || o+c.objects > objBudget {
				continue
			}
			sel = append(sel, c)
			b += c.bytes
			o += c.objects
		}
		return sel
	}
	return nil
}

// packHeaderLen is the fixed git pack header: "PACK" magic, version,
// big-endian uint32 object count.
const packHeaderLen = 12

// packHeaderObjectCount reads a pack's exact object count from its header
// via one ranged blob read. The header is trustworthy for every REGISTERED
// pack: git-push packs passed index-pack in quarantine (it reads exactly
// the header's count of objects and then verifies the trailer checksum — a
// lying header cannot index), and API-write/fold packs are produced by
// this server's own pack-objects. A malformed header is therefore a
// corrupt blob and fails the fold loudly; a missing blob propagates
// blob.ErrNotFound for the caller's concurrent-fold skip.
func packHeaderObjectCount(ctx context.Context, bl blob.BlobStore, key string) (int64, error) {
	hdr, err := bl.ReadRange(ctx, key, 0, packHeaderLen)
	if err != nil {
		return 0, err
	}
	if !bytes.Equal(hdr[:4], []byte("PACK")) {
		return 0, fmt.Errorf("pack blob %s: bad magic %q", key, hdr[:4])
	}
	if v := binary.BigEndian.Uint32(hdr[4:8]); v != 2 && v != 3 {
		return 0, fmt.Errorf("pack blob %s: unsupported pack version %d", key, v)
	}
	return int64(binary.BigEndian.Uint32(hdr[8:12])), nil
}

// materializeFold materializes the selected packs (via the injected seam —
// the fold's ONLY pack downloads) and cross-checks each header count that
// steered selection against the authoritative idx fanout the seam just
// wrote. A mismatch is impossible for a registered pack (see
// packHeaderObjectCount); detecting one means blob corruption, and the
// fold fails loudly rather than pack a corrupt input.
func (r *Repacker) materializeFold(ctx context.Context, repo string, sel []*foldCandidate) (string, error) {
	packs := make([]ref.Pack, len(sel))
	for i, c := range sel {
		packs[i] = c.pack
	}
	repoPath, err := r.materialize(ctx, repo, packs)
	if err != nil {
		return "", fmt.Errorf("materialize: %w", err)
	}
	for _, c := range sel {
		count, err := packObjectCount(filepath.Join(repoPath, "objects", "pack", "pack-"+c.pack.ID+".idx"))
		if err != nil {
			return "", fmt.Errorf("pack %s: %w", c.pack.ID, err)
		}
		if int64(count) != c.objects {
			return "", fmt.Errorf("pack %s: header claims %d objects, idx has %d (corrupt blob)",
				c.pack.ID, c.objects, count)
		}
	}
	return repoPath, nil
}

// packObjectCount reads a pack's exact object count from its version-2 idx
// fanout table (the 256th entry counts all objects) — constant work
// regardless of pack size. index-pack has written v2 for every pack this
// server materializes; an unexpected header fails the fold loudly.
func packObjectCount(idxPath string) (int, error) {
	f, err := os.Open(idxPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	var hdr [8]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return 0, fmt.Errorf("read idx header: %w", err)
	}
	if !bytes.Equal(hdr[:4], []byte{0xff, 0x74, 0x4f, 0x63}) || binary.BigEndian.Uint32(hdr[4:]) != 2 {
		return 0, fmt.Errorf("unsupported pack idx format (want v2): header %x", hdr)
	}
	var last [4]byte
	if _, err := f.ReadAt(last[:], 8+255*4); err != nil {
		return 0, fmt.Errorf("read idx fanout: %w", err)
	}
	return int(binary.BigEndian.Uint32(last[:])), nil
}

// RepackTrigger runs the post-write threshold check (issue #1): callers
// invoke MaybeEnqueue after a write that registered packs — the git surface
// after each receive-pack, the API after each RegisterPacks — and one
// unique-by-repo repack job is enqueued when the registered pack count
// reached the threshold. Construct with NewRepackTrigger; a nil trigger at
// the call sites means repacking is disabled.
type RepackTrigger struct {
	store     ref.RefStore
	threshold int
	enqueue   func(ctx context.Context, repo string) error
	logger    *slog.Logger
	timeout   time.Duration // bounds one MaybeEnqueue run (tests may lower)
}

// repackTriggerTimeout bounds one trigger check. Callers detach the trigger
// from request cancellation (the enqueue must survive the client hanging up
// right after its write), so this bound is what keeps a stalled store from
// pinning a write handler — the check is one pack-count read plus at most
// one job insert, normally milliseconds.
const repackTriggerTimeout = 10 * time.Second

// NewRepackTrigger builds the trigger. threshold is the registered-pack
// count at which a fold is scheduled; enqueue is normally the jobs queue's
// EnqueueRepack.
func NewRepackTrigger(store ref.RefStore, threshold int, enqueue func(ctx context.Context, repo string) error, logger *slog.Logger) *RepackTrigger {
	assertf(store != nil, "NewRepackTrigger: nil ref store")
	assertf(threshold >= 2, "NewRepackTrigger: threshold %d below 2 (folding fewer than two packs is a no-op)", threshold)
	assertf(enqueue != nil, "NewRepackTrigger: nil enqueue")
	assertf(logger != nil, "NewRepackTrigger: nil logger")
	return &RepackTrigger{store: store, threshold: threshold, enqueue: enqueue, logger: logger, timeout: repackTriggerTimeout}
}

// MaybeEnqueue schedules one repack when repo's registered pack count
// reached the threshold. Failures are logged, never propagated: the write
// that prompted the check already succeeded, and the count only keeps
// growing until a fold lands, so the next write retries the trigger.
// Execution is time-bounded regardless of the caller's context — the
// trigger runs synchronously inside write handlers (before the ref CAS in
// the API commit pipeline) on a cancel-detached context, and a stalled
// store must not pin them. (ListPacks materializes the row set —
// acceptable here because the trigger is what keeps that set near one row
// in steady state, and the full-hydrate path already lists it on every
// cache rebuild.)
func (t *RepackTrigger) MaybeEnqueue(ctx context.Context, repo string) {
	assertf(repo != "", "MaybeEnqueue: empty repo")
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	packs, err := t.store.ListPacks(ctx, repo)
	if err != nil {
		t.logger.Warn("repack trigger: list packs failed", "repo", repo, "err", err)
		return
	}
	if len(packs) < t.threshold {
		return
	}
	if err := t.enqueue(ctx, repo); err != nil {
		t.logger.Warn("repack trigger: enqueue failed", "repo", repo, "err", err)
		return
	}
	t.logger.Info("repack trigger: scheduled fold", "repo", repo, "packs", len(packs), "threshold", t.threshold)
}

// packObjectUnion enumerates the union of the selected packs' objects from
// their .idx files in the cache repo, sorted ascending (a deterministic
// pack-objects input). show-index output is STREAMED line by line — never
// buffered whole — so the fold's memory is bounded by the union itself,
// which the selection budgets have already capped. The materialize seam
// ran on exactly these packs, so a missing idx here is local-filesystem
// trouble, not a race.
func packObjectUnion(ctx context.Context, repoPath string, packs []ref.Pack) ([]string, error) {
	seen := make(map[string]bool)
	for _, p := range packs {
		idx := filepath.Join(repoPath, "objects", "pack", "pack-"+p.ID+".idx")
		f, err := os.Open(idx)
		if err != nil {
			return nil, fmt.Errorf("selected pack %s not materialized: %w", p.ID, err)
		}
		err = streamShowIndex(ctx, repoPath, f, func(sha string) { seen[sha] = true })
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("show-index %s: %w", p.ID, err)
		}
	}
	objs := make([]string, 0, len(seen))
	for sha := range seen {
		objs = append(objs, sha)
	}
	sort.Strings(objs)
	return objs, nil
}

// streamShowIndex runs `git show-index` over one idx (fed via stdin) and
// calls fn per object id, streaming stdout so memory never scales with the
// pack's object count.
func streamShowIndex(ctx context.Context, repoPath string, idx io.Reader, fn func(sha string)) error {
	cmd := exec.CommandContext(ctx, "git", "show-index")
	cmd.Dir = repoPath
	cmd.Stdin = idx
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	sc := bufio.NewScanner(stdout)
	var scanErr error
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue // empty pack (delete-only push): zero objects
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || !ref.ValidSHA(fields[1]) {
			scanErr = fmt.Errorf("unexpected show-index line %q", line)
			break
		}
		fn(fields[1])
	}
	if scanErr == nil {
		scanErr = sc.Err()
	}
	// Drain remaining output so Wait never blocks on a full pipe after an
	// early scan abort, then surface the first failure.
	_, _ = io.Copy(io.Discard, stdout)
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git show-index: %w: %s", normalizeCtxErr(ctx, err), strings.TrimSpace(stderr.String()))
	}
	return scanErr
}

// writeConsolidatedPack writes one self-contained pack holding exactly objs
// into the cache repo's own pack dir (pack-objects writes .pack/.idx/.rev
// under a temp name and renames, so concurrent readers never see torn
// files, and this node's cache is warm immediately). Single-threaded so the
// pack bytes — and therefore the content-derived pack id — are a
// deterministic function of the object set. Returns the pack id (the
// content hash git names the pack with) and the .pack path.
func writeConsolidatedPack(ctx context.Context, repoPath string, objs []string) (id, packPath string, err error) {
	prefix := filepath.Join(repoPath, "objects", "pack", "pack")
	stdin := strings.NewReader(strings.Join(objs, "\n") + "\n")
	if len(objs) == 0 {
		// pack-objects still emits a valid empty pack for empty stdin;
		// keep the reader empty rather than sending a blank line.
		stdin = strings.NewReader("")
	}
	out, err := runGitStdin(ctx, repoPath, stdin, "pack-objects", "--threads=1", "-q", prefix)
	if err != nil {
		return "", "", fmt.Errorf("pack-objects: %w", err)
	}
	id = strings.TrimSpace(out)
	assertf(id != "", "pack-objects emitted no pack hash")
	packPath = prefix + "-" + id + ".pack"
	if _, err := os.Stat(packPath); err != nil {
		return "", "", fmt.Errorf("pack-objects output missing: %w", err)
	}
	return id, packPath, nil
}
