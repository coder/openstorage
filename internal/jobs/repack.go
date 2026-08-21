// repack.go implements the insert-triggered background repack (issue #1):
// per-repo pack rows otherwise grow monotonically with every push and API
// write (cache repos pin gc.auto=0 / receive.unpackLimit=0), inflating
// hydration cost and blob-request amplification without bound. The repack
// job folds a repo's registered packs into one consolidated pack:
//
//  1. snapshot the registered pack rows (the fold input),
//  2. hydrate the cache repo (materializes at least every snapshotted pack),
//  3. write ONE self-contained pack holding the union of the snapshotted
//     packs' objects,
//  4. upload the new pack blob (publish-before-flip: bytes must be durable
//     before any row references them),
//  5. install the fold with ref.RefStore.ReplacePacks — one transaction that
//     deletes exactly the snapshotted rows and inserts the new one.
//
// A lost install (another node folded first) deletes its own uploaded blob
// and stops; there is no eager retry — the next write past the threshold
// re-triggers. Superseded pack BLOBS are never deleted here: they become
// unregistered, age past the orphan sweeper's threshold, and are collected
// by it (issue #5). Concurrent hydrators that raced the swap re-list and
// retry via the bounded loop in githttp.HydratePacks (issue #6).
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

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
// githttp.runGit: warnings must never contaminate parsed stdout).
func runGitStdin(dir string, stdin io.Reader, args ...string) (string, error) {
	assertf(len(args) > 0, "runGitStdin: no args")
	cmd := exec.Command("git", args...)
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

	// BeforeSwap is a test seam: when set, it runs after the consolidated
	// pack's blob upload and before ReplacePacks — the fold-install race
	// window. Tests and the DST harness use it to interleave a competing
	// fold deterministically. Never set in production wiring.
	BeforeSwap func(ctx context.Context) error
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

// Repack runs one fold attempt for repo. A no-op (fewer than two registered
// packs) and a cleanly lost race both return nil; only infrastructure
// failures propagate (the river job records them, and the next trigger
// retries).
func (r *Repacker) Repack(ctx context.Context, repo string) error {
	assertf(repo != "", "Repack: empty repo")

	// Snapshot BEFORE hydrating: Hydrate materializes at least every pack
	// listed here, so the union enumeration below can never miss a
	// snapshotted pack's objects. Packs registered after this listing are
	// simply not part of the fold and survive the swap untouched.
	old, err := r.store.ListPacks(ctx, repo)
	if err != nil {
		return fmt.Errorf("repack %s: list packs: %w", repo, err)
	}
	if len(old) < 2 {
		return nil // nothing to fold
	}

	repoPath, refsErr, err := r.hydrate(ctx, repo)
	if err != nil {
		return fmt.Errorf("repack %s: hydrate: %w", repo, err)
	}
	if refsErr != nil {
		// Tolerable: the fold packs the snapshotted packs' OBJECTS, which
		// HydratePacks fully materialized; ref sync only affects the local
		// ref view, which the fold does not read.
		r.logger.Warn("repack: tolerated ref-sync failure", "repo", repo, "err", refsErr)
	}

	objs, err := packObjectUnion(repoPath, old)
	if err != nil {
		return fmt.Errorf("repack %s: %w", repo, err)
	}

	id, packPath, err := writeConsolidatedPack(repoPath, objs)
	if err != nil {
		return fmt.Errorf("repack %s: %w", repo, err)
	}
	newPack := ref.Pack{ID: id, Key: blob.PackKey(repo, id)}

	// Publish-before-flip: the blob must be durable before ReplacePacks can
	// commit a row referencing it. A crash after this upload strands only
	// an unregistered blob, which the orphan sweeper collects.
	if err := r.blobs.UploadFile(ctx, newPack.Key, packPath); err != nil {
		return fmt.Errorf("repack %s: upload %s: %w", repo, newPack.Key, err)
	}

	if r.BeforeSwap != nil {
		if err := r.BeforeSwap(ctx); err != nil {
			return fmt.Errorf("repack %s: before-swap hook: %w", repo, err)
		}
	}

	won, err := r.store.ReplacePacks(ctx, repo, old, newPack)
	if err != nil {
		// Ambiguous by construction (the transaction may have committed
		// with the response lost), so the uploaded blob must NOT be
		// deleted here: if the swap landed, rows reference it; if not, it
		// ages into the sweeper's orphan diff like any crashed publish.
		return fmt.Errorf("repack %s: replace packs: %w", repo, err)
	}
	if !won {
		// Cleanly lost fold: delete our own orphan — unless an identical
		// pack id is registered (identity fold or a concurrent identical
		// fold won), in which case the blob is live and must stay.
		if has, herr := r.store.HasPack(ctx, repo, newPack.Key); herr != nil {
			r.logger.Warn("repack: lost fold; skipping blob cleanup (HasPack failed, sweeper will collect)",
				"repo", repo, "key", newPack.Key, "err", herr)
		} else if !has {
			if derr := r.blobs.Delete(ctx, newPack.Key); derr != nil && !errors.Is(derr, blob.ErrNotFound) {
				r.logger.Warn("repack: lost fold; blob cleanup failed (sweeper will collect)",
					"repo", repo, "key", newPack.Key, "err", derr)
			}
		}
		r.logger.Info("repack: lost fold-install race", "repo", repo, "folded", len(old))
		return nil
	}
	r.logger.Info("repack: folded packs", "repo", repo, "folded", len(old), "pack", id)
	return nil
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
}

// NewRepackTrigger builds the trigger. threshold is the registered-pack
// count at which a fold is scheduled; enqueue is normally the jobs queue's
// EnqueueRepack.
func NewRepackTrigger(store ref.RefStore, threshold int, enqueue func(ctx context.Context, repo string) error, logger *slog.Logger) *RepackTrigger {
	assertf(store != nil, "NewRepackTrigger: nil ref store")
	assertf(threshold >= 2, "NewRepackTrigger: threshold %d below 2 (folding fewer than two packs is a no-op)", threshold)
	assertf(enqueue != nil, "NewRepackTrigger: nil enqueue")
	assertf(logger != nil, "NewRepackTrigger: nil logger")
	return &RepackTrigger{store: store, threshold: threshold, enqueue: enqueue, logger: logger}
}

// MaybeEnqueue schedules one repack when repo's registered pack count
// reached the threshold. Failures are logged, never propagated: the write
// that prompted the check already succeeded, and the count only keeps
// growing until a fold lands, so the next write retries the trigger.
// (ListPacks materializes the row set — acceptable here because the trigger
// is what keeps that set near one row in steady state, and the full-hydrate
// path already lists it on every cache rebuild.)
func (t *RepackTrigger) MaybeEnqueue(ctx context.Context, repo string) {
	assertf(repo != "", "MaybeEnqueue: empty repo")
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

// packObjectUnion enumerates the union of the snapshotted packs' objects
// from their .idx files in the hydrated cache repo, sorted ascending (a
// deterministic pack-objects input). Every snapshotted pack must be
// materialized locally — Hydrate ran after the snapshot — so a missing idx
// is an invariant violation, not a race.
func packObjectUnion(repoPath string, packs []ref.Pack) ([]string, error) {
	seen := make(map[string]bool)
	for _, p := range packs {
		idx := filepath.Join(repoPath, "objects", "pack", "pack-"+p.ID+".idx")
		f, err := os.Open(idx)
		if err != nil {
			return nil, fmt.Errorf("snapshotted pack %s not materialized: %w", p.ID, err)
		}
		out, err := runGitStdin(repoPath, f, "show-index")
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("show-index %s: %w", p.ID, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			if line == "" {
				continue // empty pack (delete-only push): zero objects
			}
			fields := strings.Fields(line)
			if len(fields) < 2 || !ref.ValidSHA(fields[1]) {
				return nil, fmt.Errorf("show-index %s: unexpected line %q", p.ID, line)
			}
			seen[fields[1]] = true
		}
	}
	objs := make([]string, 0, len(seen))
	for sha := range seen {
		objs = append(objs, sha)
	}
	sort.Strings(objs)
	return objs, nil
}

// writeConsolidatedPack writes one self-contained pack holding exactly objs
// into the cache repo's own pack dir (pack-objects writes .pack/.idx/.rev
// under a temp name and renames, so concurrent readers never see torn
// files, and this node's cache is warm immediately). Single-threaded so the
// pack bytes — and therefore the content-derived pack id — are a
// deterministic function of the object set. Returns the pack id (the
// content hash git names the pack with) and the .pack path.
func writeConsolidatedPack(repoPath string, objs []string) (id, packPath string, err error) {
	prefix := filepath.Join(repoPath, "objects", "pack", "pack")
	stdin := strings.NewReader(strings.Join(objs, "\n") + "\n")
	if len(objs) == 0 {
		// pack-objects still emits a valid empty pack for empty stdin;
		// keep the reader empty rather than sending a blank line.
		stdin = strings.NewReader("")
	}
	out, err := runGitStdin(repoPath, stdin, "pack-objects", "--threads=1", "-q", prefix)
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
