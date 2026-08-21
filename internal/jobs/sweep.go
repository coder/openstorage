// sweep.go implements the orphan-pack sweeper (issue #5): the
// publish-before-CAS ordering means a crash between pack blob upload and
// RegisterPacks strands the blob forever — committed refs never point at it,
// so it is invisible except as storage cost. The sweeper diffs the blob
// store's packs/ listing against the authoritative pack registry and
// reports (and, only when explicitly enabled, deletes) unregistered blobs
// older than an age threshold. The threshold is the only concurrency guard
// needed: in-flight pushes register their packs within a request timeout,
// far below any sane threshold.
//
// Repos are discovered from the blob listing itself (the repo segment of
// each packs/<repo>/... key) rather than from repo metadata: namespaced
// repos (`<repo>+import`/`<repo>+ephemeral`) keep fully separate pack
// spaces registered under their full name, and packs of deleted repos must
// still be sweepable — both fall out of keying the authoritative lookup by
// the listed segment.
//
// Forward-compat (issue #1): a background repack will later deregister
// superseded packs and rely on this same age-threshold diff to collect
// their blobs after a grace period.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// PackLister is the slice of ref.RefStore the sweeper needs: the
// authoritative registered-pack set per repo.
type PackLister interface {
	ListPacks(ctx context.Context, repo string) ([]ref.Pack, error)
}

// SweepConfig controls one sweeper's classification and disposal.
type SweepConfig struct {
	// Threshold is the minimum age (since blob upload) before an
	// unregistered pack blob counts as an orphan candidate. It must
	// comfortably exceed the longest possible push/import duration so
	// in-flight uploads are never classified.
	Threshold time.Duration
	// Delete reaps candidates instead of only reporting them. Off by
	// default everywhere: enable only after list-only reports have been
	// boring for a while (issue #5 ships list-only first).
	Delete bool
}

// OrphanPack is one orphan candidate found by a sweep.
type OrphanPack struct {
	Key  string
	Size int64
	Age  time.Duration
}

// SweepReport summarizes one sweep run.
type SweepReport struct {
	ReposScanned   int
	Live           int // blobs present in the pack registry
	Fresh          int // unregistered but younger than the threshold
	Orphans        []OrphanPack
	OrphanBytes    int64
	Deleted        int
	BytesReclaimed int64
}

// Sweeper finds (and optionally deletes) orphan pack blobs. Construct with
// NewSweeper; Sweep is safe to run concurrently with pushes and repacks
// (the age threshold is the guard) and is idempotent.
type Sweeper struct {
	blobs  blob.BlobStore
	packs  PackLister
	cfg    SweepConfig
	logger *slog.Logger
	now    func() time.Time // injectable clock for tests
}

// NewSweeper builds a Sweeper over the raw blob store and pack registry.
func NewSweeper(blobs blob.BlobStore, packs PackLister, cfg SweepConfig, logger *slog.Logger) *Sweeper {
	assertf(blobs != nil, "NewSweeper: nil blob store")
	assertf(packs != nil, "NewSweeper: nil pack lister")
	assertf(cfg.Threshold > 0, "NewSweeper: non-positive threshold %v", cfg.Threshold)
	assertf(logger != nil, "NewSweeper: nil logger")
	return &Sweeper{blobs: blobs, packs: packs, cfg: cfg, logger: logger, now: time.Now}
}

// Sweep lists every blob under packs/, diffs each repo group against the
// registered pack set, and reports orphan candidates (deleting them only
// when SweepConfig.Delete is set). Per-key delete failures do not stop the
// sweep; they are joined into the returned error so the job backend records
// the failure while the report stays complete.
//
// Deletes never trust the initial listing snapshot: a retried push can
// overwrite an orphaned key (pack keys are content-addressed, so the retry
// reproduces the same key) after the snapshot was taken, and deleting from
// stale state would destroy the fresh upload right before its RegisterPacks
// commits. Each delete therefore revalidates the candidate first — a fresh
// registry lookup, then a fresh ModTime read — so the age threshold is
// evaluated at delete time, not list time. The residual window is the gap
// between the revalidation reads and the delete call (milliseconds, versus
// an upload that must age past the threshold to matter); closing it fully
// needs conditional deletes or upload leases, which no portable blob
// backend offers (see the PR discussion on issue #5).
func (s *Sweeper) Sweep(ctx context.Context) (SweepReport, error) {
	start := s.now()
	objs, err := s.blobs.List(ctx, blob.PacksRoot)
	if err != nil {
		return SweepReport{}, fmt.Errorf("sweep: list %s: %w", blob.PacksRoot, err)
	}

	// Group by the key's repo segment (packs/<repo>/<file>). Keys not
	// matching that shape were not written through blob.PackKey — leave
	// foreign objects alone, loudly.
	byRepo := make(map[string][]blob.ListedObject)
	for _, obj := range objs {
		rest := strings.TrimPrefix(obj.Key, blob.PacksRoot)
		repo, file, ok := strings.Cut(rest, "/")
		if !ok || repo == "" || file == "" {
			s.logger.Warn("orphan pack sweep: skipping foreign key under packs/", "key", obj.Key)
			continue
		}
		byRepo[repo] = append(byRepo[repo], obj)
	}
	repos := make([]string, 0, len(byRepo))
	for repo := range byRepo {
		repos = append(repos, repo)
	}
	sort.Strings(repos) // deterministic logs and reports

	var report SweepReport
	var deleteErrs []error
	for _, repo := range repos {
		packs, err := s.packs.ListPacks(ctx, repo)
		if err != nil {
			return report, fmt.Errorf("sweep: list packs for %s: %w", repo, err)
		}
		live := make(map[string]bool, len(packs))
		for _, p := range packs {
			live[p.Key] = true
		}
		for _, obj := range byRepo[repo] {
			if live[obj.Key] {
				report.Live++
				continue
			}
			if obj.ModTime.IsZero() {
				// A backend that reports no mod time makes age
				// unknowable; never treat unknowable as expired.
				s.logger.Warn("orphan pack sweep: skipping key without mod time", "key", obj.Key)
				continue
			}
			age := s.now().Sub(obj.ModTime)
			if age < s.cfg.Threshold {
				report.Fresh++
				continue
			}
			report.Orphans = append(report.Orphans, OrphanPack{Key: obj.Key, Size: obj.Size, Age: age})
			report.OrphanBytes += obj.Size
			s.logger.Info("orphan pack candidate",
				"key", obj.Key, "size_bytes", obj.Size, "age", age.Round(time.Second),
				"delete_enabled", s.cfg.Delete)
			if !s.cfg.Delete {
				continue
			}
			stillOrphan, err := s.revalidateOrphan(ctx, repo, obj.Key)
			if err != nil {
				deleteErrs = append(deleteErrs, err)
				continue
			}
			if !stillOrphan {
				continue
			}
			if err := s.blobs.Delete(ctx, obj.Key); err != nil && !errors.Is(err, blob.ErrNotFound) {
				deleteErrs = append(deleteErrs, err)
				continue
			}
			report.Deleted++
			report.BytesReclaimed += obj.Size
		}
	}
	report.ReposScanned = len(repos)

	s.logger.Info("orphan pack sweep finished",
		"repos", report.ReposScanned, "live", report.Live, "fresh", report.Fresh,
		"orphans", len(report.Orphans), "orphan_bytes", report.OrphanBytes,
		"deleted", report.Deleted, "bytes_reclaimed", report.BytesReclaimed,
		"delete_enabled", s.cfg.Delete, "threshold", s.cfg.Threshold,
		"duration", s.now().Sub(start).Round(time.Millisecond))
	return report, errors.Join(deleteErrs...)
}

// revalidateOrphan re-checks one delete candidate against live state,
// discarding the sweep's initial snapshot: first a fresh registry lookup
// (a pack registered mid-sweep is live), then a fresh ModTime read of the
// exact key (an overwrite by a retried push resets the object's age; a key
// that vanished needs no delete). Registration strictly follows upload, so
// checking the registry before the mod time means any registration the
// registry read misses still refreshes the ModTime read after it.
func (s *Sweeper) revalidateOrphan(ctx context.Context, repo, key string) (bool, error) {
	packs, err := s.packs.ListPacks(ctx, repo)
	if err != nil {
		return false, fmt.Errorf("sweep: revalidate %s: list packs: %w", key, err)
	}
	for _, p := range packs {
		if p.Key == key {
			s.logger.Info("orphan pack sweep: skipping delete, pack registered mid-sweep", "key", key)
			return false, nil
		}
	}
	objs, err := s.blobs.List(ctx, key)
	if err != nil {
		return false, fmt.Errorf("sweep: revalidate %s: list key: %w", key, err)
	}
	for _, obj := range objs {
		if obj.Key != key {
			continue // sibling key extending the prefix
		}
		if obj.ModTime.IsZero() || s.now().Sub(obj.ModTime) < s.cfg.Threshold {
			s.logger.Info("orphan pack sweep: skipping delete, object refreshed mid-sweep", "key", key)
			return false, nil
		}
		return true, nil
	}
	return false, nil // gone already: nothing to delete
}

// SweepSchedule configures the periodic orphan-pack sweep on a queue
// backend: a River periodic job in postgres mode, a background ticker in
// local mode. Nil disables sweeping (hook subprocesses, tests).
type SweepSchedule struct {
	Sweeper  *Sweeper
	Interval time.Duration
}

// StartSweepTicker runs sweeper.Sweep every interval on a background
// goroutine — local mode's analogue of the River periodic job, with the
// same run-on-start behavior. The returned stop function cancels the loop
// and waits for an in-flight sweep to finish.
func StartSweepTicker(sweeper *Sweeper, interval time.Duration) (stop func()) {
	assertf(sweeper != nil, "StartSweepTicker: nil sweeper")
	assertf(interval > 0, "StartSweepTicker: non-positive interval %v", interval)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if _, err := sweeper.Sweep(ctx); err != nil && ctx.Err() == nil {
				sweeper.logger.Error("orphan pack sweep failed", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
