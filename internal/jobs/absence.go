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
	// LFSRowsTruncated counts rows physically present (under either
	// spelling) but SHORTER than their registered size — a partial loss
	// whose deficit deflates the floor exactly like full absence (Codex
	// round 4, security).
	LFSRowsTruncated int64 `json:"lfs_rows_truncated"`
	// LFSRowsTwinOnly counts rows absent under their registered spelling
	// but fully present under the canonical/legacy twin: physically
	// listed (no floor deflation), healed by download-time restore (PR
	// #104).
	LFSRowsTwinOnly   int64 `json:"lfs_rows_twin_only"`
	PackRowsAbsent    int64 `json:"pack_rows_absent"`
	PackRowsTruncated int64 `json:"pack_rows_truncated"`
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
	// at its memory or base-capacity cap — covered on discovery,
	// refreshed only by rotation revisits.
	AbsentUntracked int64 `json:"absent_untracked"`
	// AbsentDropped counts tracked rows dropped because their base's
	// aggregate cover could not be renewed this pass (pass deadline or
	// ledger failure): tracking must never outlive its renewal
	// guarantee. The rotation rediscovers and re-covers them.
	AbsentDropped int64 `json:"absent_dropped"`
	QuotaEnforced bool  `json:"quota_enforced"`
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
	// refreshRing is knownAbsent's round-robin re-verification schedule
	// (Codex rounds 3+4): re-settling a tracked row only SHRINKS covers
	// (healed, retired, folded-away, purged rows drop out of their base
	// aggregate), so a set larger than one pass's re-verification
	// capacity merely delays shrinking — never a lapse. The persistent
	// ring cursor still bounds every row's revisit deterministically
	// (once per ceil(3×tracked/(budget/2)) passes) instead of leaving
	// stragglers to map-iteration luck. Entries hold their ring index
	// for O(1) removal.
	refreshRing []*absentRow
	refreshPos  int
	// absentBases aggregates tracked lost bytes per BASE repository into
	// ONE cover reservation row each (Codex round 4): per-row covers
	// scale renewal work with the loss population and collide with the
	// per-base active-reservation-row cap (10k rows), so a big enough
	// incident either outruns the refresh budget's renewal cycle or gets
	// denied outright. The aggregate makes renewal per-REPOSITORY — one
	// work unit and one reservation row per affected base per pass —
	// which the refresh half-budget covers for any realistic incident,
	// and which the row cap cannot bind (one row per base).
	absentBases map[string]*absentBase
	baseRing    []*absentBase
	basePos     int
	// passes counts completed Run invocations to alternate phase order
	// (Codex round 1, P2): a twin probe can overdraw the LFS allocation
	// by one unit, and at degenerate budgets a fixed order would hand
	// the pack phase zero units forever.
	passes int64
}

// absentBase is one base repository's tracked-loss aggregate: the single
// cover reservation row that keeps ALL of its tracked lost bytes visible
// to admission.
type absentBase struct {
	base  string
	bytes int64
	rows  int64
	idx   int // position in baseRing, maintained by track/untrack
	// coverKey is the aggregate's reservation-key incarnation, re-minted
	// after bytes SHRINK (Codex round 5): ReservePackBytes' same-key
	// upsert keeps GREATEST(old, new) and refreshes the TTL, so renewing
	// a smaller aggregate under the old key would pin healed bytes
	// reserved forever. A fresh key charges the exact total and the old
	// row expires by TTL — at most one TTL of over-cover per shrink, the
	// same deliberate tail every cover has. The incarnation is
	// crypto-random, NEVER a resettable counter (Codex round 6): a
	// counter restarts at zero when the aggregate empties and re-forms
	// (or across process restarts) while the stale GREATEST-pinned row
	// is still alive, silently re-adopting it. Rotation is COALESCED to
	// at most once per base per pass, applied lazily at the next charge
	// (Codex round 8): rotating eagerly on every heal interleaved with
	// re-charges could mint row-cap many incarnations in one pass —
	// shrunk marks the pending rotation instead.
	coverKey string
	shrunk   bool
}

// absenceCoverKey mints a fresh synthetic per-base aggregate key (used
// with ref.RepairCoverSuffix). No real blob key can collide: pack keys
// live under "packs/", LFS keys under "lfs/", and the suffix keeps the
// row in the repair-cover class settlement never consumes — the TTL
// collects it after its incarnation is abandoned.
func absenceCoverKey(base string) string {
	return "absence-lost/" + base + "/" + ref.NewReservationOwner()
}

// absentRow is one tracked truly-absent-or-short registry row: everything
// the refresh needs to re-verify registration and re-settle it, plus its
// position in the round-robin ring.
type absentRow struct {
	repo string
	id   string // packs keyset component; empty for LFS rows
	key  string // the row's blob key
	// bytes is the row's COVERED DEFICIT — its contribution to the base
	// aggregate: the full registered size for absence, the shortfall for
	// truncation.
	bytes int64
	// registered is the row's authoritative registry size (Codex round
	// 5, security): re-verification must compare a fresh Stat against
	// THIS, never against the tracked deficit — a truncated row that
	// partially heals would otherwise compare physical bytes to the old
	// shortfall and untrack while bytes are still missing.
	registered int64
	pack       bool
	idx        int // position in refreshRing, maintained by track/untrack
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
		absentBases:   make(map[string]*absentBase),
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
	// baseCovered memoizes the aggregate (gen, bytes) charged per base
	// this pass: renewal and every discovery re-charge would otherwise
	// mint one upsert per absent row. A gen rotation or byte growth
	// re-charges.
	baseCovered map[string]coveredAggregate
	// overflowKeys/overflowBytes aggregate UNTRACKED losses (beyond the
	// base capacity or memory cap) into one pass-scoped cover row per
	// base (Codex round 7, security): per-row one-shot covers would
	// re-collide with the per-base reservation-row cap on a huge
	// single-base loss. Pass-scoped keys are GREATEST-safe — a pass's
	// running total only grows — and each expires by TTL, bounding the
	// row population at TTL/interval rows per overflow base.
	overflowKeys  map[string]string
	overflowBytes map[string]int64
	// rotated coalesces shrink-driven key rotations to one fresh
	// incarnation per base per pass (Codex round 8).
	rotated map[string]bool
}

// coveredAggregate is one pass's per-base charge memo.
type coveredAggregate struct {
	key   string
	bytes int64
}

func newAbsencePassState() *absencePassState {
	return &absencePassState{
		purged:        make(map[string]bool),
		probeFailed:   make(map[string]bool),
		coverSkip:     make(map[string]bool),
		settled:       make(map[string]bool),
		baseCovered:   make(map[string]coveredAggregate),
		overflowKeys:  make(map[string]string),
		overflowBytes: make(map[string]int64),
		rotated:       make(map[string]bool),
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
		"lfs_rows_truncated", report.LFSRowsTruncated,
		"lfs_rows_twin_only", report.LFSRowsTwinOnly,
		"pack_rows_absent", report.PackRowsAbsent,
		"pack_rows_truncated", report.PackRowsTruncated,
		"pack_rows_unsized", report.PackRowsUnsized,
		"covers_charged", report.CoversCharged,
		"bytes_covered", report.BytesCovered,
		"cover_errors", report.CoverErrors,
		"stat_errors", report.StatErrors,
		"rows_skipped_purged", report.RowsSkippedPurged,
		"absent_tracked", report.AbsentTracked,
		"absent_refreshed", report.AbsentRefreshed,
		"absent_untracked", report.AbsentUntracked,
		"absent_dropped", report.AbsentDropped,
		"quota_enforced", report.QuotaEnforced)
	return report, errs.join()
}

// refreshKnownAbsent keeps every tracked loss visible within budget. Two
// sub-phases, safety-critical first:
//
//  1. RENEWAL: re-charge each affected base's aggregate cover row — one
//     work unit per REPOSITORY, round-robin — so covers renew every pass
//     for any incident up to half-budget many affected bases, regardless
//     of how many rows are lost (Codex round 4).
//  2. RE-VERIFICATION: re-settle tracked rows round-robin (registration
//     probe + stats) so healed, retired, folded-away, and purged rows
//     drop out of their aggregates. This only SHRINKS covers, so budget
//     exhaustion here delays convergence, never visibility.
func (r *AbsenceReconciler) refreshKnownAbsent(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	renewed := make(map[string]bool, len(r.baseRing))
	complete := true
	for visits := len(r.baseRing); visits > 0 && len(r.baseRing) > 0; visits-- {
		if ctx.Err() != nil {
			errs.add(ctx.Err())
			complete = false
			break
		}
		if charged >= budget {
			complete = false // unreachable while baseCapacity ≤ budget/2, kept as a guard
			break
		}
		if r.basePos >= len(r.baseRing) {
			r.basePos = 0
		}
		base := r.baseRing[r.basePos].base
		charged++
		r.coverBase(ctx, base, report, st, errs)
		if !st.coverSkip[base] {
			renewed[base] = true
		}
		r.basePos++
	}
	// Tracked ⇒ renewed every pass, by construction (Codex round 6): a
	// renewal the pass deadline or a ledger failure aborted must not
	// leave silently-stale tracking whose covers lapse before the next
	// tick. Unrenewed bases are dropped — their aggregate rows expire by
	// TTL exactly as they would have anyway, the rotation rediscovers
	// and re-covers the losses, and the drop is loud and counted.
	if !complete || len(renewed) < len(r.baseRing) {
		var dropKeys []string
		for key, row := range r.knownAbsent {
			if !renewed[baseOf(row.repo)] {
				dropKeys = append(dropKeys, key)
			}
		}
		for _, key := range dropKeys {
			r.untrackAbsent(key)
		}
		if len(dropKeys) > 0 {
			report.AbsentDropped += int64(len(dropKeys))
			r.logger.Warn("absence reconcile: dropped tracked losses whose aggregate covers could not be renewed this pass; the rotation will rediscover them",
				"rows_dropped", len(dropKeys), "renewal_complete", complete)
		}
	}
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
			charged += r.settlePackRow(ctx, ref.AbsencePackRow{Repo: row.repo, ID: row.id, Key: row.key, Bytes: row.registered}, report, st, errs)
		} else {
			charged += r.settleLFSRow(ctx, ref.AbsenceLFSRow{Repo: row.repo, Key: row.key, Bytes: row.registered}, report, st, errs)
		}
		if len(r.refreshRing) == before {
			r.refreshPos++ // entry survived (still absent): advance past it
		}
	}
	return charged
}

// trackAbsent records a truly-absent row for aggregate cover renewal and
// round-robin re-verification, and reports whether the row is tracked
// (its bytes live in its base's aggregate). Only coverable rows are
// tracked: with quota unenforced or an unknown size there is no cover to
// keep alive, and the rotation's detection logging already covers
// reporting.
func (r *AbsenceReconciler) trackAbsent(row absentRow, report *AbsenceReport) bool {
	if !r.quotaEnforced {
		return false
	}
	if existing, ok := r.knownAbsent[row.key]; ok {
		if existing.bytes == row.bytes {
			return true // already aggregated; the caller renews the base cover
		}
		// The row's deficit CHANGED — e.g. a partial loss became total
		// (Codex round 9, security): keeping the old entry would renew
		// the stale smaller amount forever. Replace it so the base
		// aggregate stays exact.
		r.untrackAbsent(row.key)
	}
	if len(r.knownAbsent) >= absentTrackedCap {
		report.AbsentUntracked++
		r.logger.Warn("absence reconcile: known-absent set is at its memory cap; this loss gets a one-shot cover and refreshes only on rotation revisits",
			"key", row.key, "cap", absentTrackedCap)
		return false
	}
	base := baseOf(row.repo)
	agg, ok := r.absentBases[base]
	if !ok {
		// One aggregate = one renewal work unit per pass, so the tracked
		// BASE population must stay within the renewal phase's budget
		// allocation (Codex round 5): beyond it, some aggregate would
		// wait multiple passes and its cover could lapse. A new base past
		// that capacity gets one-shot covers instead — reported, renewed
		// on rotation revisits — while rows joining EXISTING aggregates
		// add no renewal work and always track.
		if int64(len(r.absentBases)) >= r.baseCapacity() {
			report.AbsentUntracked++
			r.logger.Warn("absence reconcile: tracked losses span more repositories than the per-pass renewal capacity; this repository's losses get one-shot covers — raise CODESTORAGE_ABSENCE_RECONCILE_BUDGET",
				"repo", base, "capacity", r.baseCapacity())
			return false
		}
		agg = &absentBase{base: base, idx: len(r.baseRing), coverKey: absenceCoverKey(base)}
		r.baseRing = append(r.baseRing, agg)
		r.absentBases[base] = agg
	}
	row.idx = len(r.refreshRing)
	tracked := &row
	r.refreshRing = append(r.refreshRing, tracked)
	r.knownAbsent[row.key] = tracked
	agg.bytes += row.bytes
	agg.rows++
	return true
}

// baseCapacity is the maximum tracked-base population whose aggregates
// provably renew every pass: HALF the refresh allocation (a quarter of
// the whole budget), so renewal at full capacity still leaves the other
// half of the refresh phase for registry revalidation (Codex round 6) —
// without it, a capacity-full renewal loop would consume the entire
// refresh allocation and retired/folded rows would never untrack, their
// aggregates renewing forever.
func (r *AbsenceReconciler) baseCapacity() int64 {
	return (r.budget - r.budget/2) / 2
}

// untrackAbsent drops a tracked row (healed, twin-present, retired, or
// purged) with an O(1) ring swap-remove; the moved entry inherits the
// vacated index. The row's bytes leave its base aggregate; the last row
// of a base drops the aggregate entirely (its cover row expires by TTL).
// assertf-checked consistency: the maps and rings must agree.
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

	base := baseOf(row.repo)
	agg, ok := r.absentBases[base]
	assertf(ok && agg.rows > 0 && agg.bytes >= row.bytes,
		"untrackAbsent(%q): base aggregate %s inconsistent (%+v)", key, base, agg)
	agg.bytes -= row.bytes
	agg.rows--
	// Bytes shrank: mark the pending key rotation (see
	// absentBase.coverKey) — applied lazily, coalesced to once per pass,
	// at the next charge.
	agg.shrunk = true
	if agg.rows == 0 {
		assertf(agg.bytes == 0, "untrackAbsent(%q): empty base %s holds %d bytes", key, base, agg.bytes)
		assertf(agg.idx >= 0 && agg.idx < len(r.baseRing) && r.baseRing[agg.idx] == agg,
			"untrackAbsent(%q): base ring index %d inconsistent", key, agg.idx)
		lastB := len(r.baseRing) - 1
		movedB := r.baseRing[lastB]
		r.baseRing[agg.idx] = movedB
		movedB.idx = agg.idx
		r.baseRing[lastB] = nil
		r.baseRing = r.baseRing[:lastB]
		delete(r.absentBases, base)
	}
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
	if size, err := r.blobs.Stat(ctx, row.Key); err == nil {
		if size >= row.Bytes {
			r.untrackAbsent(row.Key)
			return charged // fully present under the registered spelling
		}
		// Present but SHORT (Codex round 4, security): a truncated blob —
		// e.g. a still-valid v0.1 presigned PUT overwriting a legacy key
		// with fewer bytes — deflates the floor by the deficit exactly
		// like partial absence. Cover the deficit; a full twin could
		// compensate, but probing it here would trade a rare short-lived
		// over-cover for an extra request on a corruption path.
		report.LFSRowsTruncated++
		r.logger.Error("absence reconcile: registered lfs object is shorter than its registered size",
			"repo", row.Repo, "key", row.Key, "registered", row.Bytes, "physical", size)
		r.coverDeficit(ctx, base, absentRow{repo: row.Repo, key: row.Key, bytes: row.Bytes - size, registered: row.Bytes}, report, st, errs)
		return charged
	} else if !errors.Is(err, blob.ErrNotFound) {
		report.StatErrors++
		errs.add(fmt.Errorf("absence reconcile: stat %s: %w", row.Key, err))
		return charged
	}
	// The registered spelling is absent — classify the ADR-41 twin before
	// flagging: a canonical-missing object may be legacy-present (or the
	// reverse for a pre-migration legacy row). A FULL twin's bytes are
	// physically listed, so the floor is NOT deflated, and download-time
	// restore (PR #104) heals the spelling through the digest-verified
	// adopt pipeline — report, never cover. A SHORT twin compensates only
	// its physical size; the remainder is covered like truncation.
	if repoID, oid, ok := splitLFSKey(row.Key); ok {
		canonical, legacy := blob.LFSKey(repoID, oid), blob.LFSLegacyKey(repoID, oid)
		twin := legacy
		if row.Key == legacy {
			twin = canonical
		}
		charged++
		if size, err := r.blobs.Stat(ctx, twin); err == nil {
			if size >= row.Bytes {
				report.LFSRowsTwinOnly++
				r.untrackAbsent(row.Key)
				r.logger.Warn("absence reconcile: registered lfs object present only under its twin spelling",
					"repo", row.Repo, "key", row.Key, "twin", twin, "bytes", row.Bytes)
				return charged
			}
			report.LFSRowsTruncated++
			r.logger.Error("absence reconcile: registered lfs object survives only as a short twin",
				"repo", row.Repo, "key", row.Key, "twin", twin, "registered", row.Bytes, "physical", size)
			r.coverDeficit(ctx, base, absentRow{repo: row.Repo, key: row.Key, bytes: row.Bytes - size, registered: row.Bytes}, report, st, errs)
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
	if r.trackAbsent(absentRow{repo: row.Repo, key: row.Key, bytes: row.Bytes, registered: row.Bytes}, report) {
		r.coverBase(ctx, base, report, st, errs)
	} else {
		r.coverOverflow(ctx, base, row.Bytes, report, st, errs)
	}
	return charged
}

// coverDeficit tracks and covers a PARTIAL loss (truncated or short-twin
// bytes): the tracked entry's bytes are the deficit, re-derived on every
// re-verification — trackAbsent replaces an existing entry whose deficit
// changed so the base aggregate stays exact.
func (r *AbsenceReconciler) coverDeficit(ctx context.Context, base string, row absentRow, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	assertf(row.bytes > 0, "coverDeficit: non-positive deficit %d for %s", row.bytes, row.key)
	if r.trackAbsent(row, report) {
		r.coverBase(ctx, base, report, st, errs)
	} else {
		r.coverOverflow(ctx, base, row.bytes, report, st, errs)
	}
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
	if size, err := r.blobs.Stat(ctx, row.Key); err == nil {
		if row.Bytes == 0 || size >= row.Bytes {
			r.untrackAbsent(row.Key)
			return charged // fully present (or pre-#101 row: size unknown)
		}
		report.PackRowsTruncated++
		r.logger.Error("absence reconcile: registered pack is shorter than its registered size — committed refs may reference this pack",
			"repo", row.Repo, "key", row.Key, "registered", row.Bytes, "physical", size)
		r.coverDeficit(ctx, base, absentRow{repo: row.Repo, id: row.ID, key: row.Key, bytes: row.Bytes - size, registered: row.Bytes, pack: true}, report, st, errs)
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
	if r.trackAbsent(absentRow{repo: row.Repo, id: row.ID, key: row.Key, bytes: row.Bytes, registered: row.Bytes, pack: true}, report) {
		r.coverBase(ctx, base, report, st, errs)
	} else {
		r.coverOverflow(ctx, base, row.Bytes, report, st, errs)
	}
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

// coverBase charges base's tracked-loss AGGREGATE as one TTL'd cover
// reservation row and bumps the shared floor generation. Cover FIRST,
// then bump (the round 57/58 order): an admission already holding a floor
// scanned during the absence sees the cover in active_reservations, while
// the bump only stops FUTURE serves of cached deflated floors. Charged at
// most once per base per pass unless the aggregate grew since (discovery
// mid-pass): the same-key upsert makes re-charges idempotent, but
// skipping them keeps report counters and SQL round-trips honest.
func (r *AbsenceReconciler) coverBase(ctx context.Context, base string, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	if !r.quotaEnforced {
		return // nothing consults floors or reservations (Codex round 73)
	}
	agg, ok := r.absentBases[base]
	if !ok {
		return // nothing tracked for this base (untracked rows use coverRow)
	}
	assertf(agg.bytes > 0 && agg.rows > 0, "coverBase(%s): empty aggregate %+v", base, agg)
	if agg.shrunk {
		// Apply the pending shrink rotation, coalesced to at most one
		// fresh incarnation per base per pass (Codex round 8): heals
		// interleaved with re-charges could otherwise mint row-cap many
		// keys in one pass. A shrink after this pass's rotation stays
		// PENDING (Codex round 9: clearing it here would strand the
		// smaller total on the GREATEST-pinned intermediate key forever)
		// and rotates on the next pass — one pass of over-cover at this
		// pass's GREATEST, then exact.
		if !st.rotated[base] {
			agg.coverKey = absenceCoverKey(base)
			st.rotated[base] = true
			agg.shrunk = false
		}
	}
	if prev, done := st.baseCovered[base]; done && prev.key == agg.coverKey && agg.bytes <= prev.bytes {
		return
	}
	r.chargeCover(ctx, base, agg.coverKey, agg.bytes, report, st, errs)
	st.baseCovered[base] = coveredAggregate{key: agg.coverKey, bytes: agg.bytes}
}

// coverOverflow charges one UNTRACKED loss (beyond the base capacity or
// memory cap) into its base's pass-scoped overflow aggregate: one bounded
// reservation row per base per pass regardless of how many rows the base
// lost (Codex round 7, security — per-row one-shot covers re-collided
// with the reservation-row cap). It expires by TTL and renews only when
// the rotation revisits the rows — the documented overflow residual.
func (r *AbsenceReconciler) coverOverflow(ctx context.Context, base string, size int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	if !r.quotaEnforced {
		return
	}
	assertf(size > 0, "coverOverflow: non-positive size %d for %s", size, base)
	key, ok := st.overflowKeys[base]
	if !ok {
		key = absenceCoverKey(base)
		st.overflowKeys[base] = key
	}
	st.overflowBytes[base] += size
	r.chargeCover(ctx, base, key, st.overflowBytes[base], report, st, errs)
}

// chargeCover reserves size bytes under key's repair-cover spelling and
// bumps the floor generation, with per-base error memoization.
func (r *AbsenceReconciler) chargeCover(ctx context.Context, base, key string, size int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
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
