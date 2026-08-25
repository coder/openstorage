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
	// HasPack and HasLFSObject are the sweeper's point probes, reused by
	// the known-absent refresh to drop tracked rows that were retired,
	// folded away, or purged since their discovery.
	HasPack(ctx context.Context, repo, key string) (bool, error)
	HasLFSObject(ctx context.Context, repo, key string) (bool, error)
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
	// AbsentTracked is the known-absent refresh set's size at pass end:
	// discovered losses the round-robin refresh renews across passes.
	AbsentTracked int64 `json:"absent_tracked"`
	// AbsentRefreshed counts tracked losses the refresh re-settled this
	// pass; when it stays below AbsentTracked, renewal spans multiple
	// passes (raise the budget if that period approaches the TTL).
	AbsentRefreshed int64 `json:"absent_refreshed"`
	// AbsentUntracked counts absent rows seen while the refresh set was
	// at its memory cap — covered on discovery, refreshed only by
	// rotation revisits.
	AbsentUntracked int64 `json:"absent_untracked"`
	QuotaEnforced   bool  `json:"quota_enforced"`
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
	// MinAbsenceReconcileBudget is the configuration floor (Codex round
	// 1, P2): one row can cost up to three units (registration probe +
	// stat + twin probe), so a degenerate budget cannot guarantee the
	// refresh/rotation split makes progress on both registries. The
	// floor keeps every configurable budget far above single-row
	// overdraw territory.
	MinAbsenceReconcileBudget = int64(100)
	// absencePassTimeout bounds one whole pass (the reclaimer's pattern):
	// work units meter requests, but a degraded blob store can stretch
	// each one — time, not just work, must bound the pass. The
	// constructor tightens it to a quarter of the cover TTL (Codex round
	// 3, security): tracked covers renew at pass STARTS, and River
	// coalesces periodic ticks that fire while a pass runs, so the gap
	// between one row's renewals is bounded by pass duration + interval
	// + the next refresh phase — with the pass capped at TTL/4 and the
	// interval validated at ≤ TTL/2 (config), that gap stays within the
	// TTL and a long scan can never let live covers lapse. A timed-out
	// pass parks its cursors and resumes, exactly like a budget-limited
	// one.
	absencePassTimeout = time.Hour
	// absentTrackedCap bounds the tracked-loss population as a MEMORY
	// bound only (Codex rounds 2+3): losses are storage-incident-scaled,
	// never attacker-mintable (every software deletion path quarantines
	// first), so the cap exists to keep a catastrophic incident from
	// growing the worker heap without bound — not to match the per-pass
	// refresh capacity. One tracked row's worst-case refresh cost is 3
	// units (registration probe + stat + twin probe), so tracked losses
	// beyond one pass's refresh half-budget still renew deterministically
	// via the round-robin ring, once per ceil(3×tracked/(budget/2))
	// passes.
	absentTrackedCap = 100_000
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
	// knownAbsent tracks rows a prior pass proved truly absent, keyed by
	// the row's blob key (Codex round 1, P1): covers are TTL'd, and on a
	// registry larger than one pass's budget the rotation returns to a
	// row less often than its cover survives — the visibility window
	// would silently reopen between visits. Every pass re-settles this
	// set FIRST, independent of the rotation. In-memory per process like
	// the cursors: a restart loses it and the rotation rediscovers —
	// convergence delay, never corruption (issue #125 tracks fleet
	// persistence with #121).
	knownAbsent map[string]*absentRow
	// refreshRing is knownAbsent's round-robin schedule (Codex round 3,
	// P1): a set larger than one pass's refresh capacity must not leave
	// stragglers to unbounded map-iteration luck — the persistent ring
	// cursor guarantees every tracked loss renews at least once per
	// ceil(3×tracked / (budget/2)) passes (at most twice that under
	// swap-remove churn). Entries hold their ring index for O(1)
	// removal.
	refreshRing []*absentRow
	refreshPos  int
	// passes counts completed Run invocations to alternate phase order
	// (Codex round 1, P2): a twin probe can overdraw the LFS allocation
	// by one unit, and at degenerate budgets a fixed order would hand
	// the pack phase zero units forever.
	passes int64
}

// absentRow is one tracked truly-absent registry row: everything the
// refresh needs to re-verify registration and re-settle it, plus its
// position in the round-robin ring.
type absentRow struct {
	repo  string
	id    string // packs keyset component; empty for LFS rows
	key   string // the row's blob key
	bytes int64
	pack  bool
	idx   int // position in refreshRing, maintained by track/untrack
}

// NewAbsenceReconciler wires the reconciliation job.
func NewAbsenceReconciler(registry AbsenceRegistry, blobs BlobStatter, quotaEnforced bool, budget int64, coverTTL time.Duration, logger *slog.Logger) *AbsenceReconciler {
	assertf(registry != nil, "NewAbsenceReconciler: nil registry")
	assertf(blobs != nil, "NewAbsenceReconciler: nil blob statter")
	assertf(budget > 0, "NewAbsenceReconciler: non-positive budget %d", budget)
	assertf(coverTTL > 0, "NewAbsenceReconciler: non-positive cover TTL %v", coverTTL)
	assertf(logger != nil, "NewAbsenceReconciler: nil logger")
	passTimeout := absencePassTimeout
	if quarter := coverTTL / 4; quarter < passTimeout {
		passTimeout = quarter // see absencePassTimeout: renewal gap ≤ TTL
	}
	return &AbsenceReconciler{
		registry:      registry,
		blobs:         blobs,
		quotaEnforced: quotaEnforced,
		budget:        budget,
		coverTTL:      coverTTL,
		logger:        logger,
		passTimeout:   passTimeout,
		knownAbsent:   make(map[string]*absentRow),
	}
}

// absencePassState memoizes per-pass verdicts so an attacker-scaled row
// population cannot turn one base repo's condition into budget-many
// probe/reserve transactions.
type absencePassState struct {
	purged      map[string]bool // memoized purge-fence probes
	probeFailed map[string]bool // probe errored: reported once, fence deferred to the reserve
	coverSkip   map[string]bool // one cover denial skips the base for the rest of the pass
	settled     map[string]bool // rows settled this pass: the rotation skips refresh-visited rows
}

func newAbsencePassState() *absencePassState {
	return &absencePassState{
		purged:      make(map[string]bool),
		probeFailed: make(map[string]bool),
		coverSkip:   make(map[string]bool),
		settled:     make(map[string]bool),
	}
}

// Run executes one bounded reconciliation pass. Order (Codex round 1):
// the known-absent set is re-settled FIRST — keeping already-discovered
// losses covered is the correctness core and must not wait on the
// rotation — then the two registry rotations split the remaining budget.
// The leading rotation alternates between passes: a twin probe can
// overdraw the leader's allocation by one unit, and at degenerate budgets
// a fixed order would starve the trailing table forever; alternation
// guarantees each table leads every other pass.
func (r *AbsenceReconciler) Run(ctx context.Context) (AbsenceReport, error) {
	report := AbsenceReport{QuotaEnforced: r.quotaEnforced}
	assertf(r.passTimeout > 0, "Run: non-positive pass timeout %v", r.passTimeout)
	ctx, cancelPass := context.WithTimeout(ctx, r.passTimeout)
	defer cancelPass()
	var errs boundedErrs
	st := newAbsencePassState()
	// The refresh gets at most half the budget so a large tracked set
	// cannot starve discovery entirely; while 3×tracked stays within that
	// half every tracked cover refreshes every pass (map order randomizes
	// which entries wait when it does not).
	remaining := r.budget - r.refreshKnownAbsent(ctx, r.budget-r.budget/2, &report, st, &errs)
	if remaining < 0 {
		remaining = 0
	}
	half := remaining - remaining/2
	if r.passes%2 == 0 {
		charged := r.lfsPhase(ctx, half, &report, st, &errs)
		r.packPhase(ctx, remaining-charged, &report, st, &errs)
	} else {
		charged := r.packPhase(ctx, half, &report, st, &errs)
		r.lfsPhase(ctx, remaining-charged, &report, st, &errs)
	}
	r.passes++
	report.AbsentTracked = int64(len(r.knownAbsent))
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
		"absent_tracked", report.AbsentTracked,
		"absent_refreshed", report.AbsentRefreshed,
		"absent_untracked", report.AbsentUntracked,
		"quota_enforced", report.QuotaEnforced)
	return report, errs.join()
}

// refreshKnownAbsent re-settles tracked truly-absent rows within budget,
// in round-robin ring order resuming at the persistent cursor: verify the
// registry row still exists (retired, folded-away, and purge-completed
// rows drop out), then re-settle it exactly like a rotation visit —
// re-charging the cover while the bytes stay absent, and dropping the row
// the moment it heals. Entries the budget cannot reach this pass are
// exactly the ones the cursor starts from next pass.
func (r *AbsenceReconciler) refreshKnownAbsent(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	// Visit at most the ring's population at entry: untracks shrink the
	// ring mid-loop, and without the visit bound a key that keeps
	// re-settling could loop past every other entry.
	for visits := len(r.refreshRing); visits > 0 && len(r.refreshRing) > 0; visits-- {
		if ctx.Err() != nil {
			errs.add(ctx.Err())
			return charged
		}
		if charged >= budget {
			return charged
		}
		if r.refreshPos >= len(r.refreshRing) {
			r.refreshPos = 0
		}
		row := r.refreshRing[r.refreshPos]
		charged++ // the registration probe
		var registered bool
		var err error
		if row.pack {
			registered, err = r.registry.HasPack(ctx, row.repo, row.key)
		} else {
			registered, err = r.registry.HasLFSObject(ctx, row.repo, row.key)
		}
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: refresh registration probe %s: %w", row.key, err))
			r.refreshPos++ // still tracked; retried on the next rotation
			continue
		}
		if !registered {
			r.untrackAbsent(row.key)
			continue // the swap-removed replacement now sits at refreshPos
		}
		report.AbsentRefreshed++
		before := len(r.refreshRing)
		if row.pack {
			charged += r.settlePackRow(ctx, ref.AbsencePackRow{Repo: row.repo, ID: row.id, Key: row.key, Bytes: row.bytes}, report, st, errs)
		} else {
			charged += r.settleLFSRow(ctx, ref.AbsenceLFSRow{Repo: row.repo, Key: row.key, Bytes: row.bytes}, report, st, errs)
		}
		if len(r.refreshRing) == before {
			r.refreshPos++ // entry survived (still absent): advance past it
		}
	}
	return charged
}

// trackAbsent records a truly-absent row for round-robin cover refresh.
// Only coverable rows are tracked: with quota unenforced or an unknown
// size there is no cover to keep alive, and the rotation's detection
// logging already covers reporting.
func (r *AbsenceReconciler) trackAbsent(row absentRow, report *AbsenceReport) {
	if !r.quotaEnforced {
		return
	}
	if _, ok := r.knownAbsent[row.key]; ok {
		return
	}
	if len(r.knownAbsent) >= absentTrackedCap {
		report.AbsentUntracked++
		r.logger.Warn("absence reconcile: known-absent set is at its memory cap; this loss refreshes only on rotation revisits",
			"key", row.key, "cap", absentTrackedCap)
		return
	}
	row.idx = len(r.refreshRing)
	tracked := &row
	r.refreshRing = append(r.refreshRing, tracked)
	r.knownAbsent[row.key] = tracked
}

// untrackAbsent drops a tracked row (healed, twin-present, retired, or
// purged) with an O(1) ring swap-remove; the moved entry inherits the
// vacated index. assertf-checked consistency: the map and ring must agree.
func (r *AbsenceReconciler) untrackAbsent(key string) {
	row, ok := r.knownAbsent[key]
	if !ok {
		return
	}
	assertf(row.idx >= 0 && row.idx < len(r.refreshRing) && r.refreshRing[row.idx] == row,
		"untrackAbsent(%q): ring index %d inconsistent", key, row.idx)
	last := len(r.refreshRing) - 1
	moved := r.refreshRing[last]
	r.refreshRing[row.idx] = moved
	moved.idx = row.idx
	r.refreshRing[last] = nil
	r.refreshRing = r.refreshRing[:last]
	delete(r.knownAbsent, key)
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
func (r *AbsenceReconciler) packPhase(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	afterRepo, afterID := r.packAfterRepo, r.packAfterID
	startRepo, startID := afterRepo, afterID
	startedAtZero := afterRepo == "" && afterID == ""
	wrapped := startedAtZero
	defer func() { r.packAfterRepo, r.packAfterID = afterRepo, afterID }()
	for charged < budget {
		if ctx.Err() != nil {
			errs.add(ctx.Err())
			return charged
		}
		limit := r.pageLimit(budget - charged)
		rows, err := r.registry.ListFleetPacks(ctx, afterRepo, afterID, limit)
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: list packs: %w", err))
			return charged
		}
		if len(rows) == 0 {
			if wrapped {
				afterRepo, afterID = "", ""
				return charged
			}
			afterRepo, afterID, wrapped = "", "", true
			continue
		}
		for _, row := range rows {
			if wrapped && !startedAtZero && cursorAfter(startRepo, startID, row.Repo, row.ID) {
				afterRepo, afterID = "", ""
				return charged
			}
			if ctx.Err() != nil {
				errs.add(ctx.Err())
				return charged
			}
			if charged >= budget {
				return charged
			}
			charged += r.settlePackRow(ctx, row, report, st, errs)
			afterRepo, afterID = row.Repo, row.ID
		}
	}
	return charged
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
	assertf(row.Repo != "" && row.Key != "" && row.Bytes >= 0, "settleLFSRow: bad row %+v", row)
	if st.settled[row.Key] {
		return charged // the refresh already settled this row this pass
	}
	st.settled[row.Key] = true
	report.LFSRowsExamined++
	base := baseOf(row.Repo)
	if r.repoPurged(ctx, base, st, errs) {
		report.RowsSkippedPurged++
		r.untrackAbsent(row.Key)
		return charged
	}
	if row.Bytes == 0 {
		// A zero-size registered object deflates no floor (zero-size
		// objects are uncharged) and covers must be positive: skip
		// without a probe.
		return charged
	}
	if _, err := r.blobs.Stat(ctx, row.Key); err == nil {
		r.untrackAbsent(row.Key)
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
			r.untrackAbsent(row.Key)
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
	r.trackAbsent(absentRow{repo: row.Repo, key: row.Key, bytes: row.Bytes}, report)
	r.cover(ctx, base, coverKey, row.Bytes, report, st, errs)
	return charged
}

// settlePackRow point-checks one packs row and covers a genuine loss.
func (r *AbsenceReconciler) settlePackRow(ctx context.Context, row ref.AbsencePackRow, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	charged = 1
	assertf(row.Repo != "" && row.ID != "" && row.Key != "" && row.Bytes >= 0, "settlePackRow: bad row %+v", row)
	if st.settled[row.Key] {
		return charged // the refresh already settled this row this pass
	}
	st.settled[row.Key] = true
	report.PackRowsExamined++
	base := baseOf(row.Repo)
	if r.repoPurged(ctx, base, st, errs) {
		report.RowsSkippedPurged++
		r.untrackAbsent(row.Key)
		return charged
	}
	if _, err := r.blobs.Stat(ctx, row.Key); err == nil {
		r.untrackAbsent(row.Key)
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
	r.trackAbsent(absentRow{repo: row.Repo, id: row.ID, key: row.Key, bytes: row.Bytes, pack: true}, report)
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
