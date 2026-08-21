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
	ReposScanned int
	Live         int // blobs present in the pack registry
	Fresh        int // unregistered but younger than the threshold
	Orphans      []OrphanPack
	OrphanBytes  int64
	// Deleted counts orphans reaped this run. Their bytes move to a
	// quarantine backup rather than disappearing, so reaping frees nothing
	// by itself; BytesReclaimed counts only quarantine backups purged this
	// run — the moment storage consumption actually decreases.
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
// reproduces the same key — and therefore identical bytes) at any point
// during the sweep, and no portable blob backend offers conditional
// deletes. Four layers make deletion safe anyway:
//
//  1. revalidateOrphan re-reads the registry and the object's ModTime just
//     before acting, sparing candidates registered or refreshed mid-sweep.
//  2. safeDelete copies the bytes to a quarantine key before deleting, then
//     re-reads the registry AFTER the delete: a registration that landed
//     mid-delete is repaired on the spot by re-uploading the identical
//     bytes (content-addressed keys make the restore always correct).
//  3. healQuarantine, at the start of EVERY sweep (list-only included, so a
//     rollback to list-only still repairs a delete-enabled crash), restores
//     any quarantined pack whose key is registered but missing (covering a
//     sweeper crash mid-handshake); in delete mode it also purges
//     quarantine backups once they age past the threshold.
//  4. Writers verify after registering (blob.EnsurePublished at every
//     RegisterPacks call site): a registration that lands after
//     safeDelete's recheck necessarily verifies after the delete, sees the
//     missing key, and re-uploads it on the spot — so the recheck and the
//     writer verify jointly cover every interleaving with no timing
//     assumptions (see EnsurePublished's ordering proof).
//
// Any residual exposure (e.g. a reader racing the restore itself) is
// transient, not data loss — hydration's bounded missing-pack retry
// absorbs it — and the next sweep's heal pass remains the crash backstop.
func (s *Sweeper) Sweep(ctx context.Context) (SweepReport, error) {
	start := s.now()
	// Heal before classifying so a pack restored from quarantine is listed
	// (and counted live) by the very sweep that healed it. Healing runs in
	// EVERY mode, list-only included: turning the delete flag off is the
	// operator's rollback move after a scare, and a registered pack whose
	// only copy sits in quarantine (delete-enabled crash mid-handshake)
	// must still be restored then. Only purging stays delete-gated.
	var report SweepReport
	var deleteErrs []error
	if err := s.healQuarantine(ctx, &report); err != nil {
		deleteErrs = append(deleteErrs, err)
	}
	objs, err := s.blobs.List(ctx, blob.PacksRoot)
	if err != nil {
		return report, fmt.Errorf("sweep: list %s: %w", blob.PacksRoot, err)
	}

	// Group by the key's repo segment (packs/<repo>/<file>). Keys not
	// matching the exact blob.PackKey shape (packs/<repo>/pack-<id>.pack,
	// no deeper nesting) were not written by this system — leave foreign
	// objects alone, loudly.
	byRepo := make(map[string][]blob.ListedObject)
	for _, obj := range objs {
		repo, ok := splitPackKey(obj.Key)
		if !ok {
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
			reaped, err := s.safeDelete(ctx, repo, obj.Key)
			if err != nil {
				deleteErrs = append(deleteErrs, err)
				continue
			}
			if !reaped {
				continue
			}
			report.Deleted++
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

// quarantinePrefix roots the sweeper's pre-delete backups: safeDelete
// copies packs/<repo>/pack-<id>.pack to sweep/quarantine/packs/... before
// deleting, and healQuarantine restores or purges the copies. Deliberately
// outside blob.PacksRoot so quarantined bytes never re-enter classification.
const quarantinePrefix = "sweep/quarantine/"

// splitPackKey validates that key has the exact shape blob.PackKey
// produces — packs/<repo>/pack-<id>.pack with a non-empty id and no deeper
// nesting — and returns the repo segment. Anything else (manifests, nested
// directories, foreign objects) is not ours to classify or delete.
func splitPackKey(key string) (repo string, ok bool) {
	rest, found := strings.CutPrefix(key, blob.PacksRoot)
	if !found {
		return "", false
	}
	repo, file, found := strings.Cut(rest, "/")
	if !found || repo == "" || strings.Contains(file, "/") {
		return "", false
	}
	id, isPack := strings.CutPrefix(file, "pack-")
	id, isPack2 := strings.CutSuffix(id, ".pack")
	if !isPack || !isPack2 || id == "" {
		return "", false
	}
	return repo, true
}

// safeDelete reaps one revalidated orphan candidate without ever being able
// to lose registered bytes: it copies the object to a quarantine key
// (server-side — never staged through local disk, so pack size and temp-dir
// capacity are irrelevant), deletes, then re-reads the registry — a
// registration that landed mid-delete is repaired immediately by copying
// the identical bytes back (pack keys are content-addressed, so the restore
// is always correct). The quarantine backup is kept after a successful
// reap; healQuarantine purges it once it ages past the threshold, and until
// then it covers a sweeper crash anywhere in this sequence. Returns whether
// the candidate ended up reaped.
func (s *Sweeper) safeDelete(ctx context.Context, repo, key string) (bool, error) {
	if err := s.blobs.Copy(ctx, quarantinePrefix+key, key); err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return false, nil // gone already: nothing to delete
		}
		return false, fmt.Errorf("sweep: quarantine %s: %w", key, err)
	}
	if err := s.blobs.Delete(ctx, key); err != nil && !errors.Is(err, blob.ErrNotFound) {
		// The delete outcome is unknown — the request may have executed
		// with its response lost. Restore from quarantine unconditionally:
		// identical bytes, so a no-op if the delete never happened and a
		// repair if it did. Either way a registration racing this delete
		// finds its bytes present. The candidate stays unreaped (its next
		// classification restarts from the restore's fresh ModTime); if
		// even the restore fails, the retained backup plus the heal pass
		// remain the backstop.
		deleteErr := fmt.Errorf("sweep: delete %s: %w", key, err)
		if rerr := s.blobs.Copy(ctx, key, quarantinePrefix+key); rerr != nil {
			return false, errors.Join(deleteErr,
				fmt.Errorf("sweep: restore after ambiguous delete %s: %w", key, rerr))
		}
		return false, deleteErr
	}
	// Post-delete registry recheck: whichever of {our delete, a retried
	// push's RegisterPacks} lands last sees the other's state, so a
	// registration this read observes gets its bytes restored here, and
	// one it misses is healed from quarantine next sweep.
	packs, err := s.packs.ListPacks(ctx, repo)
	if err != nil {
		// Deleted but unverifiable: a registration that landed before the
		// delete (its EnsurePublished saw the key present) would be
		// stranded until a heal. Restore from quarantine before returning,
		// exactly like the ambiguous-delete path — a no-op if the key was
		// truly orphaned, a repair if it was registered.
		recheckErr := fmt.Errorf("sweep: post-delete recheck %s: %w", key, err)
		if rerr := s.blobs.Copy(ctx, key, quarantinePrefix+key); rerr != nil {
			return false, errors.Join(recheckErr,
				fmt.Errorf("sweep: restore after failed recheck %s: %w", key, rerr))
		}
		return false, recheckErr
	}
	for _, p := range packs {
		if p.Key != key {
			continue
		}
		if err := s.blobs.Copy(ctx, key, quarantinePrefix+key); err != nil {
			return false, fmt.Errorf("sweep: restore %s registered mid-delete: %w", key, err)
		}
		s.logger.Warn("orphan pack sweep: restored pack registered mid-delete", "key", key)
		if err := s.blobs.Delete(ctx, quarantinePrefix+key); err != nil && !errors.Is(err, blob.ErrNotFound) {
			s.logger.Warn("orphan pack sweep: purge quarantine after restore", "key", key, "err", err)
		}
		return false, nil
	}
	return true, nil
}

// healQuarantine finishes interrupted or lost-race safeDelete handshakes
// from quarantine backups: a backup whose original key is registered gets
// restored when the original is missing (identical bytes) and its backup
// purged. The restorative pass runs in every sweep mode — a list-only
// rollback must still repair a delete-enabled crash — while destructive
// purging is delete-gated: an unregistered backup is purged only when
// deletes are enabled AND it aged past the threshold, so an in-flight
// sweep's backup is never touched. Purged backup bytes are credited to
// report.BytesReclaimed — the point where storage consumption actually
// drops (reaping only moves bytes into quarantine). Failures are joined;
// each entry is independent.
func (s *Sweeper) healQuarantine(ctx context.Context, report *SweepReport) error {
	entries, err := s.blobs.List(ctx, quarantinePrefix)
	if err != nil {
		return fmt.Errorf("sweep: list quarantine: %w", err)
	}
	var errs []error
	for _, q := range entries {
		orig := strings.TrimPrefix(q.Key, quarantinePrefix)
		repo, ok := splitPackKey(orig)
		if !ok {
			s.logger.Warn("orphan pack sweep: skipping foreign quarantine key", "key", q.Key)
			continue
		}
		packs, err := s.packs.ListPacks(ctx, repo)
		if err != nil {
			errs = append(errs, fmt.Errorf("sweep: heal %s: list packs: %w", orig, err))
			continue
		}
		registered := false
		for _, p := range packs {
			if p.Key == orig {
				registered = true
				break
			}
		}
		reclaims := false // restore-then-purge is net zero; only an aged-orphan purge frees bytes
		if registered {
			if _, err := s.blobs.Stat(ctx, orig); errors.Is(err, blob.ErrNotFound) {
				if err := s.blobs.Copy(ctx, orig, q.Key); err != nil {
					errs = append(errs, fmt.Errorf("sweep: heal %s: restore: %w", orig, err))
					continue
				}
				s.logger.Warn("orphan pack sweep: healed registered pack from quarantine", "key", orig)
			} else if err != nil {
				errs = append(errs, fmt.Errorf("sweep: heal %s: stat: %w", orig, err))
				continue
			}
		} else if !s.cfg.Delete || q.ModTime.IsZero() || s.now().Sub(q.ModTime) < s.cfg.Threshold {
			// Unregistered: purge only in delete mode once aged past the
			// threshold (an in-flight sweep's backup is always fresh).
			continue
		} else {
			reclaims = true
		}
		if err := s.blobs.Delete(ctx, q.Key); err != nil && !errors.Is(err, blob.ErrNotFound) {
			errs = append(errs, fmt.Errorf("sweep: purge quarantine %s: %w", q.Key, err))
			continue
		}
		if reclaims {
			report.BytesReclaimed += q.Size
		}
	}
	return errors.Join(errs...)
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

// Heal runs one standalone restorative quarantine pass. Serve calls it at
// startup when periodic sweeping is disabled (CODESTORAGE_SWEEP_INTERVAL=0):
// a delete-enabled process that crashed mid-handshake may have left a
// registered pack's only copy in quarantine, and disabling the schedule
// must not strand it.
func (s *Sweeper) Heal(ctx context.Context) error {
	var report SweepReport
	err := s.healQuarantine(ctx, &report)
	if report.BytesReclaimed > 0 {
		s.logger.Info("standalone quarantine heal reclaimed bytes", "bytes", report.BytesReclaimed)
	}
	return err
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
