// repack.go implements the insert-triggered background repack (issue #1):
// per-repo pack rows otherwise grow monotonically with every push and API
// write (cache repos pin gc.auto=0 / receive.unpackLimit=0), inflating
// hydration cost and blob-request amplification without bound. The repack
// job folds a repo's registered packs into one consolidated pack:
//
//  1. snapshot the registered pack rows (the fold input),
//  2. hydrate the cache repo (materializes at least every snapshotted pack),
//  3. select the fold input under per-fold object/byte budgets (repack runs
//     automatically on writes, so one fold's work must be bounded even
//     against adversarial pack shapes; see selectFoldInput),
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

// HydrateFunc materializes repo's cache repo and returns its path plus a
// tolerable ref-sync error (githttp.Hydrate's contract). It is injected —
// wiring closes githttp.Hydrate over the serve cache dir — so this package
// never imports githttp, whose own tests import this package's sweeper.
type HydrateFunc func(ctx context.Context, repo string) (repoPath string, refsErr error, err error)

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
			strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// Repacker folds one repo's registered packs into a consolidated pack.
// Construct with NewRepacker; Repack is safe to run concurrently with
// pushes, API writes, hydrators, sweeps, and other repackers (ReplacePacks
// is the only mutation and it is a CAS).
type Repacker struct {
	store   ref.RefStore
	blobs   blob.BlobStore
	hydrate HydrateFunc
	logger  *slog.Logger

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
	// window. Tests and the DST harness use it to interleave a competing
	// fold deterministically. Never set in production wiring.
	BeforeSwap func(ctx context.Context) error
}

// NeedsFollowUp reports whether repo's registered pack count has reached
// FollowUpThreshold — the post-fold recheck queue backends use to schedule
// one more fold for registrations that landed mid-fold. Read failures
// report false (the next write's trigger retries).
func (r *Repacker) NeedsFollowUp(ctx context.Context, repo string) bool {
	assertf(repo != "", "NeedsFollowUp: empty repo")
	if r.FollowUpThreshold < 2 {
		return false
	}
	packs, err := r.store.ListPacks(ctx, repo)
	if err != nil {
		r.logger.Warn("repack follow-up recheck failed", "repo", repo, "err", err)
		return false
	}
	return len(packs) >= r.FollowUpThreshold
}

// NewRepacker builds a Repacker over the raw ref/blob stores and the
// injected hydrate seam (normally githttp.Hydrate closed over the serve
// cache dir, so the fold hydrates and packs in the same cache the git
// surface uses and a successful fold leaves this node warm).
func NewRepacker(store ref.RefStore, blobs blob.BlobStore, hydrate HydrateFunc, logger *slog.Logger) *Repacker {
	assertf(store != nil, "NewRepacker: nil ref store")
	assertf(blobs != nil, "NewRepacker: nil blob store")
	assertf(hydrate != nil, "NewRepacker: nil hydrate func")
	assertf(logger != nil, "NewRepacker: nil logger")
	return &Repacker{store: store, blobs: blobs, hydrate: hydrate, logger: logger}
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

// Repack runs one fold attempt for repo, returning whether a fold was
// installed. A no-op (fewer than two registered packs, or budgets selecting
// fewer than two) and a cleanly lost race both return (false, nil); only
// infrastructure failures propagate (the river job records them, and the
// next trigger retries).
func (r *Repacker) Repack(ctx context.Context, repo string) (folded bool, err error) {
	assertf(repo != "", "Repack: empty repo")
	// Runtime budget: the fold's deadline covers everything below —
	// hydration, enumeration, pack-objects (killed via CommandContext on
	// expiry), upload, and swap. Aborting anywhere is crash-equivalent.
	ctx, cancel := context.WithTimeout(ctx, r.durationBudget())
	defer cancel()

	// Snapshot BEFORE hydrating: Hydrate materializes at least every pack
	// listed here, so the union enumeration below can never miss a
	// snapshotted pack's objects. Packs registered after this listing are
	// simply not part of the fold and survive the swap untouched.
	old, err := r.store.ListPacks(ctx, repo)
	if err != nil {
		return false, fmt.Errorf("repack %s: list packs: %w", repo, err)
	}
	if len(old) < 2 {
		return false, nil // nothing to fold
	}

	repoPath, refsErr, err := r.hydrate(ctx, repo)
	if err != nil {
		return false, fmt.Errorf("repack %s: hydrate: %w", repo, err)
	}
	if refsErr != nil {
		// Tolerable: the fold packs the snapshotted packs' OBJECTS, which
		// HydratePacks fully materialized; ref sync only affects the local
		// ref view, which the fold does not read.
		r.logger.Warn("repack: tolerated ref-sync failure", "repo", repo, "err", refsErr)
	}

	lostBeforePacking := func(err error) bool {
		if !errors.Is(err, errFoldInputGone) {
			return false
		}
		// A snapshotted pack has no local idx even after Hydrate: a
		// concurrent fold replaced the row set between our snapshot and
		// Hydrate's own listing, so hydration (correctly) never
		// materialized it into this cold cache. Our input is stale —
		// ReplacePacks would lose anyway. Stop cleanly; nothing uploaded.
		r.logger.Info("repack: lost fold race before packing", "repo", repo, "err", err)
		return true
	}

	fold, err := selectFoldInput(repoPath, old, r.objectBudget(), r.byteBudget())
	if lostBeforePacking(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repack %s: %w", repo, err)
	}
	if len(fold) < 2 {
		r.logger.Info("repack: budgets select nothing to fold",
			"repo", repo, "packs", len(old), "object_budget", r.objectBudget(), "byte_budget", r.byteBudget())
		return false, nil
	}
	old = fold

	objs, err := packObjectUnion(ctx, repoPath, old)
	if lostBeforePacking(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repack %s: %w", repo, err)
	}

	id, packPath, err := writeConsolidatedPack(ctx, repoPath, objs)
	if err != nil {
		return false, fmt.Errorf("repack %s: %w", repo, err)
	}
	newPack := ref.Pack{ID: id, Key: blob.PackKey(repo, id)}

	// Publish-before-flip: the blob must be durable before ReplacePacks can
	// commit a row referencing it. A crash after this upload strands only
	// an unregistered blob, which the orphan sweeper collects.
	if err := r.blobs.UploadFile(ctx, newPack.Key, packPath); err != nil {
		return false, fmt.Errorf("repack %s: upload %s: %w", repo, newPack.Key, err)
	}

	if r.BeforeSwap != nil {
		if err := r.BeforeSwap(ctx); err != nil {
			return false, fmt.Errorf("repack %s: before-swap hook: %w", repo, err)
		}
	}

	won, err := r.store.ReplacePacks(ctx, repo, old, newPack)
	if err != nil {
		// Ambiguous by construction (the transaction may have committed
		// with the response lost), so the uploaded blob must NOT be
		// deleted here — and, like every registration path, the possibly
		// committed row must be verified against the sweeper race below:
		// a committed row must never point at missing bytes.
		verr := verifyPublished(ctx, r.blobs, newPack.Key, packPath)
		return false, errors.Join(fmt.Errorf("repack %s: replace packs: %w", repo, err), verr)
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
	// local pack file if needed — now that the row is committed.
	if verr := verifyPublished(ctx, r.blobs, newPack.Key, packPath); verr != nil {
		return false, fmt.Errorf("repack %s: %w", repo, verr)
	}
	r.logger.Info("repack: folded packs", "repo", repo, "folded", len(old), "pack", id)
	return true, nil
}

// verifyPublished re-checks (and if needed restores) the consolidated
// pack's blob after a swap that may have committed a row referencing it.
func verifyPublished(ctx context.Context, bl blob.BlobStore, key, packPath string) error {
	if err := blob.EnsurePublished(ctx, bl, key, packPath); err != nil {
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

// selectFoldInput picks the fold's input greedily, oldest-first (ListPacks
// order), under cumulative object and byte budgets: each pack's exact
// object count comes from its idx fanout table and its byte size from the
// materialized .pack file. Skipped packs stay registered untouched — any
// subset of whole packs folds into a self-contained pack (only
// reachability trimming was unsafe), so bounding per-fold work never risks
// correctness, and repeated folds converge oversized repos to
// O(size/budget) consolidated rows. Cumulative counts overestimate the
// union (shared objects count twice) — conservative in the safe direction.
func selectFoldInput(repoPath string, packs []ref.Pack, maxObjects int, maxBytes int64) ([]ref.Pack, error) {
	assertf(maxObjects > 0 && maxBytes > 0, "selectFoldInput: non-positive budgets %d/%d", maxObjects, maxBytes)
	var selected []ref.Pack
	var objects int
	var size int64
	for _, p := range packs {
		base := filepath.Join(repoPath, "objects", "pack", "pack-"+p.ID)
		count, err := packObjectCount(base + ".idx")
		if err != nil {
			return nil, fmt.Errorf("pack %s: %w: %w", p.ID, errFoldInputGone, err)
		}
		fi, err := os.Stat(base + ".pack")
		if err != nil {
			return nil, fmt.Errorf("pack %s: %w: %w", p.ID, errFoldInputGone, err)
		}
		if objects+count > maxObjects || size+fi.Size() > maxBytes {
			continue // over budget: leave this pack registered as-is
		}
		selected = append(selected, p)
		objects += count
		size += fi.Size()
	}
	return selected, nil
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

// errFoldInputGone reports (wrapped, match with errors.Is) that a
// snapshotted pack is not materialized in the cache even after Hydrate:
// a concurrent fold replaced the rows between the snapshot and Hydrate's
// own listing, so this fold's input is stale and it has already lost.
var errFoldInputGone = errors.New("jobs: snapshotted pack not materialized (folded away concurrently)")

// packObjectUnion enumerates the union of the snapshotted packs' objects
// from their .idx files in the hydrated cache repo, sorted ascending (a
// deterministic pack-objects input). show-index output is STREAMED line by
// line — never buffered whole — so the fold's memory is bounded by the
// union itself, which selectFoldInput has already capped. Hydrate ran after
// the snapshot, so a snapshotted pack can be missing locally only when a
// concurrent fold removed its row before Hydrate listed — reported as
// errFoldInputGone.
func packObjectUnion(ctx context.Context, repoPath string, packs []ref.Pack) ([]string, error) {
	seen := make(map[string]bool)
	for _, p := range packs {
		idx := filepath.Join(repoPath, "objects", "pack", "pack-"+p.ID+".idx")
		f, err := os.Open(idx)
		if err != nil {
			return nil, fmt.Errorf("pack %s: %w: %w", p.ID, errFoldInputGone, err)
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
		return fmt.Errorf("git show-index: %w: %s", err, strings.TrimSpace(stderr.String()))
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
