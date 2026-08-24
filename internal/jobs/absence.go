package jobs

// Registry-vs-storage absence reconciliation (issue #101): a bounded,
// resumable background job that pages registered rows — lfs_objects and
// packs — and point-checks their bytes still physically exist in blob
// storage. While a registered blob's bytes are absent (storage-layer loss;
// every software deletion path quarantines first), the uncounted quota
// floor max(0, listing − counter) is deflated by the absent size, hiding
// equally sized rowless legacy bytes from admission; a write settled during
// that window durably exceeds quota once the blob is later restored. The
// restore-time covers + generation bumps (PR #61 rounds 54–66) protect
// admissions from restore onward only — this job closes the window BEFORE
// any restore, to one reconciliation period, by charging truly absent
// registered bytes as TTL'd restore covers (ref.ReserveRestoreCover, the
// same reservation row the restore paths renew) and bumping the shared
// floor generation.
//
// The scan direction is the sweeper's inverse: the sweeper walks storage
// listings and probes the registry; this walks registry rows and Stats
// storage. Both cursors are the tables' primary keys, in-memory per
// process like the LFS reclaimer's (issue #121 tracks fleet-persistent
// cursors for multi-replica River fleets; a lost cursor only delays
// convergence — covers re-charge on the next full rotation).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

// AbsenceRegistry is the ref-store surface the absence reconciler drives.
// Both concrete backends (ref.PG, ref.SQLite) satisfy it.
type AbsenceRegistry interface {
	// ListFleetLFSObjects pages lfs_objects rows fleet-wide, keyset on
	// the (repo, key) primary key ascending.
	ListFleetLFSObjects(ctx context.Context, afterRepo, afterKey string, limit int) ([]ref.AbsenceLFSRow, error)
	// ListFleetPacks pages packs rows fleet-wide, keyset on the
	// (repo, id) primary key ascending.
	ListFleetPacks(ctx context.Context, afterRepo, afterID string, limit int) ([]ref.AbsencePackRow, error)
	// IsRepoPurged probes the repository purge fence (issue #52): fenced
	// repos' rows are on their way out and are never covered.
	IsRepoPurged(ctx context.Context, base string) (bool, error)
	// BumpLFSAdoptionGen invalidates cached usage floors after a cover
	// made absent bytes visible to admission.
	BumpLFSAdoptionGen(ctx context.Context, repo string) error
	// ReservePackBytes is the ref.RestoreCoverStore seam
	// ref.ReserveRestoreCover charges covers through.
	ReservePackBytes(ctx context.Context, group []string, packs []ref.Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ref.ReserveDecision, error)
}

// BlobStatter is the single blob-store probe the reconciler needs.
type BlobStatter interface {
	Stat(ctx context.Context, key string) (int64, error)
}

// AbsenceReport summarizes one reconciliation pass.
type AbsenceReport struct {
	LFSRowsExamined  int64 `json:"lfs_rows_examined"`
	PackRowsExamined int64 `json:"pack_rows_examined"`
	// LFSRowsAbsent counts rows whose bytes are missing under BOTH ADR-41
	// spellings — genuine storage loss.
	LFSRowsAbsent int64 `json:"lfs_rows_absent"`
	// LFSRowsTwinOnly counts rows absent under their registered spelling
	// but present under the canonical/legacy twin: physically listed (no
	// floor deflation), healed by download-time restore (PR #104).
	LFSRowsTwinOnly int64 `json:"lfs_rows_twin_only"`
	PackRowsAbsent  int64 `json:"pack_rows_absent"`
	// PackRowsUnsized counts absent pack rows registered before the
	// packs.bytes migration: the loss is reported but uncoverable until a
	// repack fold rewrites the row with its size.
	PackRowsUnsized   int64 `json:"pack_rows_unsized"`
	CoversCharged     int64 `json:"covers_charged"`
	BytesCovered      int64 `json:"bytes_covered"`
	CoverErrors       int64 `json:"cover_errors"`
	StatErrors        int64 `json:"stat_errors"`
	RowsSkippedPurged int64 `json:"rows_skipped_purged"`
	QuotaEnforced     bool  `json:"quota_enforced"`
}

const (
	// absencePageSize bounds one registry page.
	absencePageSize = 1_000
	// DefaultAbsenceReconcileBudget bounds the work units one pass may
	// charge (CODESTORAGE_ABSENCE_RECONCILE_BUDGET overrides): one unit
	// per row examined plus one per extra twin-spelling probe. Sized for
	// point Stats — remote HEAD requests, not row reads — so a default
	// pass stays minutes, not hours.
	DefaultAbsenceReconcileBudget = int64(100_000)
	// absencePassTimeout bounds one whole pass (the reclaimer's pattern):
	// work units meter requests, but a degraded blob store can stretch
	// each one — time, not just work, must bound the pass.
	absencePassTimeout = time.Hour
)

// AbsenceReconciler runs the reconciliation scan. Construct with
// NewAbsenceReconciler; Run executes one bounded pass per schedule tick.
type AbsenceReconciler struct {
	registry AbsenceRegistry
	blobs    BlobStatter
	// quotaEnforced gates cover charging and generation bumps: with quota
	// admission disabled nothing consults floors or reservations (the
	// sweep restore pattern, Codex round 73) — detection still reports.
	quotaEnforced bool
	budget        int64
	// coverTTL is the reservation TTL admission actually consults
	// (PackReservationTTL): a cover is visible to an admission only while
	// its created_at is within the ADMITTING call's TTL, so charging any
	// other value would not change visibility. Continuously absent bytes
	// stay visible because every pass re-charges the same-key row
	// (ReservePackBytes refreshes created_at on conflict).
	coverTTL    time.Duration
	logger      *slog.Logger
	passTimeout time.Duration // whole-pass cap, shrinkable in tests
	// Flat resume cursors, in-memory per process (issue #121 tracks
	// fleet persistence). Empty = start of the keyspace.
	lfsAfterRepo, lfsAfterKey  string
	packAfterRepo, packAfterID string
}

// NewAbsenceReconciler wires the reconciliation job.
func NewAbsenceReconciler(registry AbsenceRegistry, blobs BlobStatter, quotaEnforced bool, budget int64, coverTTL time.Duration, logger *slog.Logger) *AbsenceReconciler {
	assertf(registry != nil, "NewAbsenceReconciler: nil registry")
	assertf(blobs != nil, "NewAbsenceReconciler: nil blob statter")
	assertf(budget > 0, "NewAbsenceReconciler: non-positive budget %d", budget)
	assertf(coverTTL > 0, "NewAbsenceReconciler: non-positive cover TTL %v", coverTTL)
	assertf(logger != nil, "NewAbsenceReconciler: nil logger")
	return &AbsenceReconciler{
		registry:      registry,
		blobs:         blobs,
		quotaEnforced: quotaEnforced,
		budget:        budget,
		coverTTL:      coverTTL,
		logger:        logger,
		passTimeout:   absencePassTimeout,
	}
}

// absencePassState memoizes per-pass verdicts so an attacker-scaled row
// population cannot turn one base repo's condition into budget-many
// probe/reserve transactions.
type absencePassState struct {
	purged      map[string]bool // memoized purge-fence probes
	probeFailed map[string]bool // probe errored: reported once, fence deferred to the reserve
	coverSkip   map[string]bool // one cover denial skips the base for the rest of the pass
}

func newAbsencePassState() *absencePassState {
	return &absencePassState{
		purged:      make(map[string]bool),
		probeFailed: make(map[string]bool),
		coverSkip:   make(map[string]bool),
	}
}

// Run executes one bounded reconciliation pass: page lfs_objects rows from
// the LFS cursor, then packs rows from the pack cursor, point-checking
// each row's bytes and covering genuine losses. The budget is split so the
// pack registry can never starve behind an LFS registry larger than a
// whole budget (both tables are attacker-growable): LFS gets the first
// half, packs the second half plus any LFS leftover.
func (r *AbsenceReconciler) Run(ctx context.Context) (AbsenceReport, error) {
	report := AbsenceReport{QuotaEnforced: r.quotaEnforced}
	assertf(r.passTimeout > 0, "Run: non-positive pass timeout %v", r.passTimeout)
	ctx, cancelPass := context.WithTimeout(ctx, r.passTimeout)
	defer cancelPass()
	var errs boundedErrs
	st := newAbsencePassState()
	lfsBudget := r.budget - r.budget/2
	charged := r.lfsPhase(ctx, lfsBudget, &report, st, &errs)
	r.packPhase(ctx, r.budget-charged, &report, st, &errs)
	r.logger.Info("absence reconcile pass finished",
		"lfs_rows_examined", report.LFSRowsExamined,
		"pack_rows_examined", report.PackRowsExamined,
		"lfs_rows_absent", report.LFSRowsAbsent,
		"lfs_rows_twin_only", report.LFSRowsTwinOnly,
		"pack_rows_absent", report.PackRowsAbsent,
		"pack_rows_unsized", report.PackRowsUnsized,
		"covers_charged", report.CoversCharged,
		"bytes_covered", report.BytesCovered,
		"cover_errors", report.CoverErrors,
		"stat_errors", report.StatErrors,
		"rows_skipped_purged", report.RowsSkippedPurged,
		"quota_enforced", report.QuotaEnforced)
	return report, errs.join()
}

// cursorAfter reports whether (repoB, keyB) sorts strictly after
// (repoA, keyA) — the keyset order both list methods page in.
func cursorAfter(repoA, keyA, repoB, keyB string) bool {
	if repoA != repoB {
		return repoB > repoA
	}
	return keyB > keyA
}

// lfsPhase pages lfs_objects rows from the resume cursor, wrapping once at
// the end of the keyspace (the reclaimer's rotation), and settles each row
// within the phase budget. Returns the work charged.
func (r *AbsenceReconciler) lfsPhase(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	afterRepo, afterKey := r.lfsAfterRepo, r.lfsAfterKey
	startRepo, startKey := afterRepo, afterKey
	startedAtZero := afterRepo == "" && afterKey == ""
	wrapped := startedAtZero
	defer func() { r.lfsAfterRepo, r.lfsAfterKey = afterRepo, afterKey }()
	for charged < budget {
		if ctx.Err() != nil {
			errs.add(ctx.Err())
			return charged
		}
		limit := r.pageLimit(budget - charged)
		rows, err := r.registry.ListFleetLFSObjects(ctx, afterRepo, afterKey, limit)
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: list lfs objects: %w", err))
			return charged
		}
		if len(rows) == 0 {
			if wrapped {
				// The whole keyspace was offered this pass: the next
				// pass starts a fresh rotation.
				afterRepo, afterKey = "", ""
				return charged
			}
			afterRepo, afterKey, wrapped = "", "", true
			continue
		}
		for _, row := range rows {
			if wrapped && !startedAtZero && cursorAfter(startRepo, startKey, row.Repo, row.Key) {
				// Wrapped past this pass's own starting point: every
				// remaining row was already offered above.
				afterRepo, afterKey = "", ""
				return charged
			}
			if ctx.Err() != nil {
				errs.add(ctx.Err())
				return charged
			}
			if charged >= budget {
				return charged // the cursor rests on the previous row
			}
			charged += r.settleLFSRow(ctx, row, report, st, errs)
			afterRepo, afterKey = row.Repo, row.Key
		}
	}
	return charged
}

// packPhase mirrors lfsPhase over packs rows.
func (r *AbsenceReconciler) packPhase(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	afterRepo, afterID := r.packAfterRepo, r.packAfterID
	startRepo, startID := afterRepo, afterID
	startedAtZero := afterRepo == "" && afterID == ""
	wrapped := startedAtZero
	defer func() { r.packAfterRepo, r.packAfterID = afterRepo, afterID }()
	var charged int64
	for charged < budget {
		if ctx.Err() != nil {
			errs.add(ctx.Err())
			return
		}
		limit := r.pageLimit(budget - charged)
		rows, err := r.registry.ListFleetPacks(ctx, afterRepo, afterID, limit)
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: list packs: %w", err))
			return
		}
		if len(rows) == 0 {
			if wrapped {
				afterRepo, afterID = "", ""
				return
			}
			afterRepo, afterID, wrapped = "", "", true
			continue
		}
		for _, row := range rows {
			if wrapped && !startedAtZero && cursorAfter(startRepo, startID, row.Repo, row.ID) {
				afterRepo, afterID = "", ""
				return
			}
			if ctx.Err() != nil {
				errs.add(ctx.Err())
				return
			}
			if charged >= budget {
				return
			}
			charged += r.settlePackRow(ctx, row, report, st, errs)
			afterRepo, afterID = row.Repo, row.ID
		}
	}
}

func (r *AbsenceReconciler) pageLimit(remaining int64) int {
	limit := absencePageSize
	if remaining < int64(limit) {
		limit = int(remaining)
	}
	assertf(limit > 0, "pageLimit: non-positive limit from remaining %d", remaining)
	return limit
}

// settleLFSRow point-checks one lfs_objects row and covers a genuine loss.
// Returns the work charged: one unit for the row plus one per extra
// twin-spelling probe.
func (r *AbsenceReconciler) settleLFSRow(ctx context.Context, row ref.AbsenceLFSRow, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	charged = 1
	report.LFSRowsExamined++
	assertf(row.Repo != "" && row.Key != "" && row.Bytes >= 0, "settleLFSRow: bad row %+v", row)
	base := baseOf(row.Repo)
	if r.repoPurged(ctx, base, st, errs) {
		report.RowsSkippedPurged++
		return charged
	}
	if row.Bytes == 0 {
		// A zero-size registered object deflates no floor (zero-size
		// objects are uncharged) and covers must be positive: skip
		// without a probe.
		return charged
	}
	if _, err := r.blobs.Stat(ctx, row.Key); err == nil {
		return charged // physically present under the registered spelling
	} else if !errors.Is(err, blob.ErrNotFound) {
		report.StatErrors++
		errs.add(fmt.Errorf("absence reconcile: stat %s: %w", row.Key, err))
		return charged
	}
	// The registered spelling is absent — classify the ADR-41 twin before
	// flagging: a canonical-missing object may be legacy-present (or the
	// reverse for a pre-migration legacy row). The twin's bytes are still
	// physically listed, so the floor is NOT deflated, and download-time
	// restore (PR #104) heals the spelling through the digest-verified
	// adopt pipeline — report, never cover.
	coverKey := row.Key
	if repoID, oid, ok := splitLFSKey(row.Key); ok {
		canonical, legacy := blob.LFSKey(repoID, oid), blob.LFSLegacyKey(repoID, oid)
		// Cover under the canonical spelling regardless of the row's: it
		// is the reservation row the restore paths renew, so a restore
		// racing this pass refreshes the same key instead of double
		// charging.
		coverKey = canonical
		twin := legacy
		if row.Key == legacy {
			twin = canonical
		}
		charged++
		if _, err := r.blobs.Stat(ctx, twin); err == nil {
			report.LFSRowsTwinOnly++
			r.logger.Warn("absence reconcile: registered lfs object present only under its twin spelling",
				"repo", row.Repo, "key", row.Key, "twin", twin, "bytes", row.Bytes)
			return charged
		} else if !errors.Is(err, blob.ErrNotFound) {
			report.StatErrors++
			errs.add(fmt.Errorf("absence reconcile: stat twin %s: %w", twin, err))
			return charged
		}
	}
	report.LFSRowsAbsent++
	r.logger.Error("absence reconcile: registered lfs object bytes are missing from blob storage",
		"repo", row.Repo, "key", row.Key, "bytes", row.Bytes)
	r.cover(ctx, base, coverKey, row.Bytes, report, st, errs)
	return charged
}

// settlePackRow point-checks one packs row and covers a genuine loss.
func (r *AbsenceReconciler) settlePackRow(ctx context.Context, row ref.AbsencePackRow, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	charged = 1
	report.PackRowsExamined++
	assertf(row.Repo != "" && row.ID != "" && row.Key != "" && row.Bytes >= 0, "settlePackRow: bad row %+v", row)
	base := baseOf(row.Repo)
	if r.repoPurged(ctx, base, st, errs) {
		report.RowsSkippedPurged++
		return charged
	}
	if _, err := r.blobs.Stat(ctx, row.Key); err == nil {
		return charged
	} else if !errors.Is(err, blob.ErrNotFound) {
		report.StatErrors++
		errs.add(fmt.Errorf("absence reconcile: stat %s: %w", row.Key, err))
		return charged
	}
	report.PackRowsAbsent++
	// Committed refs must never point at unavailable packs — a missing
	// registered pack is an integrity loss beyond the quota deflation.
	r.logger.Error("absence reconcile: registered pack bytes are missing from blob storage — committed refs may reference this pack",
		"repo", row.Repo, "key", row.Key, "bytes", row.Bytes)
	if row.Bytes == 0 {
		// Pre-#101 row: size unknown, nothing chargeable — the loss stays
		// reported until a repack fold rewrites the row with its size.
		report.PackRowsUnsized++
		return charged
	}
	r.cover(ctx, base, row.Key, row.Bytes, report, st, errs)
	return charged
}

// repoPurged memoizes the purge-fence probe per base repo. A probe error
// defers the fence to ReserveRestoreCover's own transactional check (the
// reserve fails closed with ErrRepoPurged), reported once per base.
func (r *AbsenceReconciler) repoPurged(ctx context.Context, base string, st *absencePassState, errs *boundedErrs) bool {
	if v, ok := st.purged[base]; ok {
		return v
	}
	if st.probeFailed[base] {
		return false
	}
	purged, err := r.registry.IsRepoPurged(ctx, base)
	if err != nil {
		st.probeFailed[base] = true
		errs.add(fmt.Errorf("absence reconcile: purge probe %s: %w", base, err))
		return false
	}
	st.purged[base] = purged
	return purged
}

// cover charges size absent bytes as a TTL'd restore cover under key and
// bumps the shared floor generation. Cover FIRST, then bump (the round
// 57/58 order): an admission already holding a floor scanned during the
// absence sees the cover in active_reservations, while the bump only stops
// FUTURE serves of cached deflated floors.
func (r *AbsenceReconciler) cover(ctx context.Context, base, key string, size int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	assertf(size > 0, "cover: non-positive size %d for %s", size, key)
	if !r.quotaEnforced {
		return // nothing consults floors or reservations (Codex round 73)
	}
	if st.coverSkip[base] {
		report.CoverErrors++
		return
	}
	if err := ref.ReserveRestoreCover(ctx, r.registry, blob.QuotaRepoGroup(base), key, size, r.coverTTL); err != nil {
		if errors.Is(err, ref.ErrRepoPurged) {
			// Raced a purge fence minted after the probe: the rows are on
			// their way out; treat like the probe verdict.
			st.purged[base] = true
			return
		}
		// One denial (e.g. the active-reservation row cap) means further
		// covers for this base would fail the same way this pass: skip
		// them — the next pass retries — instead of minting budget-many
		// failed transactions.
		st.coverSkip[base] = true
		report.CoverErrors++
		errs.add(fmt.Errorf("absence reconcile: cover %s: %w", key, err))
		return
	}
	report.CoversCharged++
	report.BytesCovered += size
	if err := r.registry.BumpLFSAdoptionGen(ctx, base); err != nil {
		// The cover row still guards every admission that re-lists; the
		// next pass re-charges and re-bumps.
		errs.add(fmt.Errorf("absence reconcile: bump adoption gen %s: %w", base, err))
	}
}

// AbsenceReconcileSchedule pairs the reconciler with its cadence — its own
// schedule, independent of the sweeper's (absence detection is useful even
// where blob sweeping stays disabled).
type AbsenceReconcileSchedule struct {
	Reconciler *AbsenceReconciler
	Interval   time.Duration
}

// StartAbsenceReconcileTicker runs the schedule every Interval on a
// background goroutine — local mode's analogue of the River periodic job,
// with the same run-on-start behavior. The returned stop function cancels
// the loop and waits for an in-flight run.
func StartAbsenceReconcileTicker(schedule *AbsenceReconcileSchedule) (stop func()) {
	assertf(schedule != nil && schedule.Reconciler != nil, "StartAbsenceReconcileTicker: nil schedule or reconciler")
	assertf(schedule.Interval > 0, "StartAbsenceReconcileTicker: non-positive interval %v", schedule.Interval)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(schedule.Interval)
		defer ticker.Stop()
		for {
			if _, err := schedule.Reconciler.Run(ctx); err != nil && ctx.Err() == nil {
				schedule.Reconciler.logger.Error("scheduled absence reconcile failed", "err", err)
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
