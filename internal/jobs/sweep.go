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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"codestorage/internal/blob"
)

// PackRegistry is the slice of ref.RefStore the sweeper needs: point
// membership in the authoritative registered-pack set. Point reads, not
// ListPacks: a repo's registered-pack count is attacker-growable
// (pre-receive registers packs before ref policy), so the sweeper must
// never materialize a repo's full pack set.
type PackRegistry interface {
	HasPack(ctx context.Context, repo, key string) (bool, error)
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
	Live  int // blobs present in the pack registry
	Fresh int // unregistered but younger than the threshold
	// Orphans is a bounded sample (maxOrphanSample) of the candidates:
	// an orphan backlog after an outage can be arbitrarily large, and the
	// report must not scale worker memory with it. OrphanCount and
	// OrphanBytes stay exact; every candidate is also logged as it is
	// classified.
	Orphans     []OrphanPack
	OrphanCount int
	OrphanBytes int64
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
	packs  PackRegistry
	cfg    SweepConfig
	logger *slog.Logger
	now    func() time.Time // injectable clock for tests
}

// NewSweeper builds a Sweeper over the raw blob store and pack registry.
func NewSweeper(blobs blob.BlobStore, packs PackRegistry, cfg SweepConfig, logger *slog.Logger) *Sweeper {
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
	var deleteErrs boundedErrs
	if err := s.healQuarantine(ctx, &report, s.cfg.Delete); err != nil {
		deleteErrs.add(err)
	}
	// Stream packs/ page-wise and classify each object with a point
	// registry read (PackRegistry.HasPack): both the blob population and a
	// repo's registered-pack count are attacker-growable (pre-receive
	// uploads and registers packs before ref policy), so the sweep must
	// never materialize a prefix listing or a repo's pack set. Memory is
	// bounded by one page, the report's bounded orphan sample
	// (maxOrphanSample; counts and bytes stay exact), and the bounded
	// error sample — deliberately NO per-repo state: a repo-creation
	// principal can grow the distinct-repo population without bound, and
	// classification is per-object anyway. Keys not matching the exact
	// blob.PackKey shape (packs/<repo>/pack-<id>.pack, no deeper nesting)
	// were not written by this system — leave foreign objects alone,
	// loudly.
	err := s.blobs.ListPages(ctx, blob.PacksRoot, sweepListPageSize, func(page []blob.ListedObject) error {
		for _, obj := range page {
			repo, ok := splitPackKey(obj.Key)
			if !ok {
				s.logger.Warn("orphan pack sweep: skipping foreign key under packs/", "key", obj.Key)
				continue
			}
			if err := s.classify(ctx, repo, obj, &report, &deleteErrs); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return report, fmt.Errorf("sweep: list %s: %w", blob.PacksRoot, err)
	}

	s.logger.Info("orphan pack sweep finished",
		"live", report.Live, "fresh", report.Fresh,
		"orphans", report.OrphanCount, "orphan_bytes", report.OrphanBytes,
		"deleted", report.Deleted, "bytes_reclaimed", report.BytesReclaimed,
		"delete_enabled", s.cfg.Delete, "threshold", s.cfg.Threshold,
		"duration", s.now().Sub(start).Round(time.Millisecond))
	return report, deleteErrs.join()
}

// sweepListPageSize bounds how many listed objects a sweep holds per page.
// A var so tests can force multi-page scans with few objects.
var sweepListPageSize = 1000

// maxSweepErrs bounds how many per-entry errors a sweep retains: an outage
// can fail every entry of an attacker-growable population, and error
// accumulation must not scale memory with it. A var so tests can exercise
// the cap cheaply.
var maxSweepErrs = 20

// boundedErrs retains the first maxSweepErrs errors plus an exact count of
// the overflow it elided.
type boundedErrs struct {
	errs   []error
	elided int
}

func (b *boundedErrs) add(err error) {
	if len(b.errs) < maxSweepErrs {
		b.errs = append(b.errs, err)
		return
	}
	b.elided++
}

func (b *boundedErrs) join() error {
	if b.elided > 0 {
		return errors.Join(append(b.errs, fmt.Errorf("sweep: %d additional errors elided", b.elided))...)
	}
	return errors.Join(b.errs...)
}

// maxOrphanSample bounds SweepReport.Orphans (see its doc). A var so tests
// can exercise the cap without minting thousands of objects.
var maxOrphanSample = 100

// sweepRecoveryTimeout bounds safeDelete's post-delete recovery phase
// (registry recheck + quarantine restore) when it runs detached from a
// canceled sweep, and separately its delete phase. Restores are
// server-side copies, so this generously covers even multipart copies of
// the largest supported packs.
const sweepRecoveryTimeout = 15 * time.Minute

// quarantinePurgeFloor is the minimum age before a quarantine backup may
// be purged, independent of the configured classification threshold: a
// backup is safe to purge only once its handshake is provably dead, and
// safeDelete bounds a handshake's lifetime at 2×sweepRecoveryTimeout from
// the backup's write. 2× that bound doubles the margin.
const quarantinePurgeFloor = 4 * sweepRecoveryTimeout

// quarantinePurgeAge is the age gate for purging quarantine backups: the
// configured threshold, floored at quarantinePurgeFloor (the threshold is
// an orphan-classification knob an operator may tune far below any safe
// purge age).
func (s *Sweeper) quarantinePurgeAge() time.Duration {
	return max(s.cfg.Threshold, quarantinePurgeFloor)
}

// classify handles one listed pack blob: a point registry read, then the
// age-threshold diff, then (delete mode only) the revalidate + quarantine
// handshake. Registry errors abort the sweep (they would misclassify the
// object); disposal errors accumulate into deleteErrs.
func (s *Sweeper) classify(ctx context.Context, repo string, obj blob.ListedObject, report *SweepReport, deleteErrs *boundedErrs) error {
	registered, err := s.packs.HasPack(ctx, repo, obj.Key)
	if err != nil {
		return fmt.Errorf("has pack %s: %w", obj.Key, err)
	}
	if registered {
		report.Live++
		return nil
	}
	if obj.ModTime.IsZero() {
		// A backend that reports no mod time makes age unknowable; never
		// treat unknowable as expired.
		s.logger.Warn("orphan pack sweep: skipping key without mod time", "key", obj.Key)
		return nil
	}
	age := s.now().Sub(obj.ModTime)
	if age < s.cfg.Threshold {
		report.Fresh++
		return nil
	}
	if len(report.Orphans) < maxOrphanSample {
		report.Orphans = append(report.Orphans, OrphanPack{Key: obj.Key, Size: obj.Size, Age: age})
	}
	report.OrphanCount++
	report.OrphanBytes += obj.Size
	s.logger.Info("orphan pack candidate",
		"key", obj.Key, "size_bytes", obj.Size, "age", age.Round(time.Second),
		"delete_enabled", s.cfg.Delete)
	if !s.cfg.Delete {
		return nil
	}
	stillOrphan, err := s.revalidateOrphan(ctx, repo, obj.Key)
	if err != nil {
		deleteErrs.add(err)
		return nil
	}
	if !stillOrphan {
		return nil
	}
	reaped, err := s.safeDelete(ctx, repo, obj.Key)
	if err != nil {
		deleteErrs.add(err)
		return nil
	}
	if reaped {
		report.Deleted++
	}
	return nil
}

// quarantinePrefix roots the sweeper's pre-delete backups: safeDelete
// copies packs/<repo>/pack-<id>.pack to a unique key under it before
// deleting, and healQuarantine restores or purges the copies. Deliberately
// outside blob.PacksRoot so quarantined bytes never re-enter classification.
const quarantinePrefix = "sweep/quarantine/"

// quarantineKey mints the WRITE-ONCE backup key for one safeDelete
// handshake: sweep/quarantine/<nonce>/packs/<repo>/pack-<id>.pack. The
// random nonce is the coordination between overlapping delete-mode sweeps
// (Codex round 14): no handshake can ever overwrite another's backup, so an
// aged entry is proof its handshake is dead and purging it can never
// destroy a live handshake's restore source. (The pre-nonce layout wrote
// all handshakes of one pack to the same shared key, where purging by any
// age read — however fresh — raced a concurrent overwrite.)
func quarantineKey(key string) string {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		panic(fmt.Sprintf("sweep: read nonce entropy: %v", err)) // crypto/rand never fails on supported platforms
	}
	return quarantinePrefix + hex.EncodeToString(nonce[:]) + "/" + key
}

// parseQuarantineKey recovers the original pack key from a nonce-layout
// quarantine backup key (sweep/quarantine/<nonce>/packs/...). This is the
// only layout any released sweeper has ever written; anything else under
// the prefix — including entries whose first segment is not exactly the
// 16-hex-char nonce quarantineKey mints — is foreign: never a restore
// source, never purged, skipped loudly by the caller.
func parseQuarantineKey(qkey string) (orig string, ok bool) {
	rest, found := strings.CutPrefix(qkey, quarantinePrefix)
	if !found {
		return "", false
	}
	nonce, orig, found := strings.Cut(rest, "/")
	if !found || !isQuarantineNonce(nonce) {
		return "", false
	}
	return orig, true
}

// isQuarantineNonce reports whether s is exactly the hex.EncodeToString
// form of quarantineKey's 8 random bytes: 16 lowercase hex characters.
func isQuarantineNonce(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

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
// reap; healQuarantine purges it once it ages past quarantinePurgeAge, and
// until then it covers a sweeper crash anywhere in this sequence. Returns
// whether the candidate ended up reaped.
//
// The handshake's total lifetime is bounded BY CONSTRUCTION: the delete
// phase runs under a sweepRecoveryTimeout deadline and the recovery phase
// under another, so from the moment the backup is written (its ModTime) to
// the last possible restore from it, at most 2×sweepRecoveryTimeout
// elapses. quarantinePurgeFloor is derived from that bound — the age gate
// is a proof, not a heuristic.
func (s *Sweeper) safeDelete(ctx context.Context, repo, key string) (bool, error) {
	qKey := quarantineKey(key)
	if err := s.blobs.Copy(ctx, qKey, key); err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return false, nil // gone already: nothing to delete
		}
		return false, fmt.Errorf("sweep: quarantine %s: %w", key, err)
	}
	// The delete phase is deadline-bounded so the handshake lifetime bound
	// above holds even against a wedged store or a sweep context without a
	// deadline of its own.
	hctx, hcancel := context.WithTimeout(ctx, sweepRecoveryTimeout)
	defer hcancel()
	// Everything after the delete request is recovery: it must be able to
	// finish even when the sweep itself is being canceled (server shutdown,
	// job backend cancellation racing the delete's response), or a canceled
	// sweep could end a handshake with the bytes missing and the recheck
	// unread. Recovery therefore runs on a bounded context detached from
	// the sweep's cancellation (Codex round 16).
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sweepRecoveryTimeout)
	defer cancel()
	if err := s.blobs.Delete(hctx, key); err != nil && !errors.Is(err, blob.ErrNotFound) {
		// The delete outcome is unknown — the request may have executed
		// with its response lost. Restore from quarantine unconditionally:
		// identical bytes, so a no-op if the delete never happened and a
		// repair if it did. Either way a registration racing this delete
		// finds its bytes present. The candidate stays unreaped (its next
		// classification restarts from the restore's fresh ModTime); if
		// even the restore fails, the retained backup plus the heal pass
		// remain the backstop.
		deleteErr := fmt.Errorf("sweep: delete %s: %w", key, err)
		if rerr := s.blobs.Copy(rctx, key, qKey); rerr != nil {
			return false, errors.Join(deleteErr,
				fmt.Errorf("sweep: restore after ambiguous delete %s: %w", key, rerr))
		}
		return false, deleteErr
	}
	// Post-delete registry recheck: whichever of {our delete, a retried
	// push's RegisterPacks} lands last sees the other's state, so a
	// registration this read observes gets its bytes restored here, and
	// one it misses is healed from quarantine next sweep.
	registered, err := s.packs.HasPack(rctx, repo, key)
	if err != nil {
		// Deleted but unverifiable: a registration that landed before the
		// delete (its EnsurePublished saw the key present) would be
		// stranded until a heal. Restore from quarantine before returning,
		// exactly like the ambiguous-delete path — a no-op if the key was
		// truly orphaned, a repair if it was registered.
		recheckErr := fmt.Errorf("sweep: post-delete recheck %s: %w", key, err)
		if rerr := s.blobs.Copy(rctx, key, qKey); rerr != nil {
			return false, errors.Join(recheckErr,
				fmt.Errorf("sweep: restore after failed recheck %s: %w", key, rerr))
		}
		return false, recheckErr
	}
	if registered {
		if err := s.blobs.Copy(rctx, key, qKey); err != nil {
			return false, fmt.Errorf("sweep: restore %s registered mid-delete: %w", key, err)
		}
		// The backup is deliberately retained until the aged purge in
		// healQuarantineEntry collects it; until then it covers a sweeper
		// crash anywhere after this handshake.
		s.logger.Warn("orphan pack sweep: restored pack registered mid-delete", "key", key)
		return false, nil
	}
	return true, nil
}

// healQuarantine finishes interrupted or lost-race safeDelete handshakes
// from quarantine backups: a backup whose original key is registered gets
// restored when the original is missing (identical bytes). The restorative
// pass runs in every sweep mode — a list-only
// rollback must still repair a delete-enabled crash — while destructive
// purging is gated on the caller's purge flag (the sweep passes its Delete
// config; the standalone Heal forces it off): an unregistered backup is
// purged only when purge is set AND it aged past the threshold, so an
// in-flight sweep's backup is never touched. Purged backup bytes are
// credited to report.BytesReclaimed — the point where storage consumption
// actually drops (reaping only moves bytes into quarantine). Failures are
// joined; each entry is independent.
func (s *Sweeper) healQuarantine(ctx context.Context, report *SweepReport, purge bool) error {
	// Streamed like the packs/ scan: quarantine grows with reaped orphans,
	// which an attacker can mint, so this prefix must not be materialized
	// whole either — and neither may its errors: an outage can fail every
	// entry, so retention is bounded (boundedErrs) while the elided count
	// stays exact. Entries are independent, so per-page handling is exact.
	var errs boundedErrs
	listErr := s.blobs.ListPages(ctx, quarantinePrefix, sweepListPageSize, func(page []blob.ListedObject) error {
		for _, q := range page {
			if err := s.healQuarantineEntry(ctx, q, report, purge); err != nil {
				errs.add(err)
			}
		}
		return nil
	})
	if listErr != nil {
		errs.add(fmt.Errorf("sweep: list quarantine: %w", listErr))
	}
	return errs.join()
}

// healQuarantineEntry disposes of one quarantine backup per the
// healQuarantine contract.
func (s *Sweeper) healQuarantineEntry(ctx context.Context, q blob.ListedObject, report *SweepReport, purge bool) error {
	orig, ok := parseQuarantineKey(q.Key)
	var repo string
	if ok {
		repo, ok = splitPackKey(orig)
	}
	if !ok {
		s.logger.Warn("orphan pack sweep: skipping foreign quarantine key", "key", q.Key)
		return nil
	}
	registered, err := s.packs.HasPack(ctx, repo, orig)
	if err != nil {
		return fmt.Errorf("sweep: heal %s: has pack: %w", orig, err)
	}
	if registered {
		if _, err := s.blobs.Stat(ctx, orig); errors.Is(err, blob.ErrNotFound) {
			switch err := s.blobs.Copy(ctx, orig, q.Key); {
			case errors.Is(err, blob.ErrNotFound):
				// The entry vanished between listing and restore: a
				// concurrent replica healed this same entry and purged it —
				// its restore strictly precedes its purge, so the original
				// must exist now. Verify rather than assume.
				if _, serr := s.blobs.Stat(ctx, orig); serr != nil {
					return errors.Join(
						fmt.Errorf("sweep: heal %s: restore source vanished: %w", orig, err),
						fmt.Errorf("sweep: heal %s: re-stat: %w", orig, serr))
				}
				return nil // healed by a peer; the peer also owns the purge
			case err != nil:
				return fmt.Errorf("sweep: heal %s: restore: %w", orig, err)
			}
			s.logger.Warn("orphan pack sweep: healed registered pack from quarantine", "key", orig)
		} else if err != nil {
			return fmt.Errorf("sweep: heal %s: stat: %w", orig, err)
		}
	}
	// Purge strictly by age: quarantine entries are write-once (see
	// quarantineKey), so an aged listing ModTime is immutable proof the
	// entry's handshake is dead and no live handshake can be relying on it
	// — PROVIDED the age gate exceeds the handshake's constructive
	// lifetime bound (see safeDelete). The configured threshold governs
	// orphan classification and may be tuned arbitrarily low, so the purge
	// age enforces its own floor rather than trusting it.
	if !purge || q.ModTime.IsZero() || s.now().Sub(q.ModTime) < s.quarantinePurgeAge() {
		return nil
	}
	switch err := s.blobs.Delete(ctx, q.Key); {
	case errors.Is(err, blob.ErrNotFound):
		// A concurrent sweep purged this entry first: it reclaimed the
		// bytes and credited them; crediting here too would double-count.
		return nil
	case err != nil:
		return fmt.Errorf("sweep: purge quarantine %s: %w", q.Key, err)
	}
	if !registered {
		// A registered entry's purge is net zero (its original is live);
		// only orphan-backup purges actually reduce consumption.
		report.BytesReclaimed += q.Size
	}
	return nil
}

// revalidateOrphan re-checks one delete candidate against live state,
// discarding the sweep's initial snapshot: first a fresh registry lookup
// (a pack registered mid-sweep is live), then a fresh ModTime read of the
// exact key (an overwrite by a retried push resets the object's age; a key
// that vanished needs no delete). Registration strictly follows upload, so
// checking the registry before the mod time means any registration the
// registry read misses still refreshes the ModTime read after it.
func (s *Sweeper) revalidateOrphan(ctx context.Context, repo, key string) (bool, error) {
	registered, err := s.packs.HasPack(ctx, repo, key)
	if err != nil {
		return false, fmt.Errorf("sweep: revalidate %s: has pack: %w", key, err)
	}
	if registered {
		s.logger.Info("orphan pack sweep: skipping delete, pack registered mid-sweep", "key", key)
		return false, nil
	}
	mod, found, err := s.freshModTime(ctx, key)
	if err != nil {
		return false, fmt.Errorf("sweep: revalidate %s: list key: %w", key, err)
	}
	if !found {
		return false, nil // gone already: nothing to delete
	}
	if mod.IsZero() || s.now().Sub(mod) < s.cfg.Threshold {
		s.logger.Info("orphan pack sweep: skipping delete, object refreshed mid-sweep", "key", key)
		return false, nil
	}
	return true, nil
}

// freshModTime re-reads the exact key's current ModTime, discarding any
// listing-snapshot value. found is false when the key no longer exists;
// sibling keys merely extending the prefix do not count.
func (s *Sweeper) freshModTime(ctx context.Context, key string) (mod time.Time, found bool, err error) {
	objs, err := s.blobs.List(ctx, key)
	if err != nil {
		return time.Time{}, false, err
	}
	for _, obj := range objs {
		if obj.Key == key {
			return obj.ModTime, true, nil
		}
	}
	return time.Time{}, false, nil
}

// Heal runs one standalone, strictly restorative quarantine pass. Serve
// calls it at startup when periodic sweeping is disabled
// (CODESTORAGE_SWEEP_INTERVAL=0): a delete-enabled process that crashed
// mid-handshake may have left a registered pack's only copy in quarantine,
// and disabling the schedule must not strand it. Purging is forced off
// regardless of SweepConfig.Delete — interval zero documents "no sweeping",
// so an emergency stop must never destroy anything.
func (s *Sweeper) Heal(ctx context.Context) error {
	var report SweepReport
	return s.healQuarantine(ctx, &report, false)
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
