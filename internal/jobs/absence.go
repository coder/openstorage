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
// storage.
//
// FLEET PERSISTENCE (issue #125): River may run each pass on any replica,
// so no correctness-bearing state lives in process memory. Two durable
// sources carry everything across worker turnover:
//
//   - The rotation cursors (with per-class traversal bookkeeping) persist
//     as job_scan_state rows, so any replica resumes the one fleet-shared
//     scan instead of restarting its own.
//   - The TTL'd cover reservations THEMSELVES are the tracking state for
//     discovered losses. Every pass re-derives its renewal obligations by
//     LISTING the live absence-cover rows (ref.ListAbsenceCovers) and
//     re-charging them — no in-memory known-absent set to lose.
//
// Each base repository holds at most ONE live aggregate cover per registry
// class, under a deterministic incarnation key
// absence-lost/<base>/<class>-e<n> (zero-padded so byte order is numeric
// order). The lifecycle keeps steady-state cover EXACT (1×):
//
//   - GROW: each traversal recomputes the base's running deficit total
//     from scratch; whenever the running total exceeds the live row's
//     bytes, the SAME key is re-charged with the new total (the ledger
//     upsert keeps GREATEST, and a fresh total is never "live + delta", so
//     crash replays can only transiently over-charge, never compound).
//     Fresh incidents mint their first incarnation immediately, so a new
//     loss is admission-visible at discovery.
//   - SHRINK/RELEASE: when a traversal observed a base's ENTIRE row range
//     within one pass with no stat/charge errors, its running total is
//     exact. A smaller total mints incarnation n+1 charged exactly and
//     deletes the old row (mint first — never a coverage gap); a zero
//     total deletes the row outright. Deletes are safe because the
//     reconciler is the only writer under the prefix and River's unique
//     jobs serialize passes fleet-wide. Ranges that span pass boundaries
//     skip evaluation (their in-memory running total is partial) and shed
//     healed bytes on a later traversal; a base whose row count exceeds
//     one pass's phase allocation sheds only after a budget raise — the
//     documented sizing residual.
//   - RENEW: the renewal phase re-charges each (base, class)'s
//     highest-incarnation live row from the durable listing — one unit per
//     row, from ANY replica — deleting superseded lower incarnations left
//     by crashes. Legacy nonce-keyed covers from pre-#125 workers renew
//     until BOTH classes complete one clean traversal (by then every
//     still-absent loss lives in incarnation keys), then are released.
//
// Deterministic keys are load-bearing (Codex round 6 on PR #123 banned
// resettable-counter keys): incarnation numbers restart at 1 only after
// the previous row was DELETED, so a stale GREATEST-pinned row can never
// be silently re-adopted.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
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
	// ListAbsenceCovers pages the live absence-cover reservation rows —
	// the durable tracking state the renewal phase re-derives its
	// obligations from every pass (issue #125).
	ListAbsenceCovers(ctx context.Context, afterRaw string, limit int) ([]ref.AbsenceCoverRow, error)
	// AbsenceCoverHead point-reads the highest live incarnation under a
	// synthetic-key prefix — the charge path's fallback when the renewal
	// listing did not cover the base this pass.
	AbsenceCoverHead(ctx context.Context, prefix string) (ref.AbsenceCoverRow, bool, error)
	// DeleteAbsenceCover releases one cover row (healed loss, superseded
	// incarnation, or retired legacy spelling).
	DeleteAbsenceCover(ctx context.Context, key string) error
	// IsRepoPurged probes the repository purge fence (issue #52): fenced
	// repos' rows are on their way out and are never covered.
	IsRepoPurged(ctx context.Context, base string) (bool, error)
	// BumpLFSAdoptionGen invalidates cached usage floors after a cover
	// made absent bytes visible to admission.
	BumpLFSAdoptionGen(ctx context.Context, repo string) error
	// ReservePackBytes is the ref.RestoreCoverStore seam
	// ref.ReserveRestoreCover charges covers through.
	ReservePackBytes(ctx context.Context, group []string, packs []ref.Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ref.ReserveDecision, error)
	// ScanStateStore persists the rotation cursors and traversal
	// bookkeeping (issue #125).
	ref.ScanStateStore
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
	// CoversRenewed counts live cover rows the renewal phase re-charged
	// this pass (fresh TTL, same key and bytes) — the fleet-derived
	// replacement for the per-process known-absent refresh (issue #125).
	CoversRenewed int64 `json:"covers_renewed"`
	// CoversRotated counts shrink re-mints: a base's exact re-derived
	// total came in below its live row, so a fresh incarnation charged
	// the exact total and the stale row was deleted.
	CoversRotated int64 `json:"covers_rotated"`
	// CoversReleased counts cover rows DELETED: fully healed bases and
	// superseded incarnations left by crashes.
	CoversReleased int64 `json:"covers_released"`
	// CoversDecaying counts listed legacy (pre-#125 nonce-keyed) covers
	// left to expire by TTL once both registries completed a clean
	// traversal: never deleted — a not-yet-upgraded replica may still
	// depend on them mid-roll.
	CoversDecaying int64 `json:"covers_decaying"`
	// RenewalIncomplete reports the renewal listing was cut short by the
	// pass deadline (never by budget): un-renewed rows resume first on
	// the durable cursor next pass. Investigate the blob store or ledger
	// if persistent — covers lapse if renewals cannot land within the
	// TTL.
	RenewalIncomplete bool `json:"renewal_incomplete"`
	// RenewalOverBudget reports renewal charged more than its soft
	// allocation (half the budget): every live cover still renewed —
	// renewal is the correctness core and never lapses on budget — but
	// discovery was starved this pass. Raise the budget above twice the
	// live cover population.
	RenewalOverBudget bool `json:"renewal_over_budget"`
	// LFSTraversals/PacksTraversals are the classes' completed rotation
	// counts (durable bookkeeping, useful for tracking convergence).
	LFSTraversals  int64 `json:"lfs_traversals"`
	PackTraversals int64 `json:"pack_traversals"`
	QuotaEnforced  bool  `json:"quota_enforced"`
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
	// renewal/rotation split makes progress on both registries. The
	// floor keeps every configurable budget far above single-row
	// overdraw territory.
	MinAbsenceReconcileBudget = int64(100)
	// absencePassTimeout bounds one whole pass (the reclaimer's pattern):
	// work units meter requests, but a degraded blob store can stretch
	// each one — time, not just work, must bound the pass. The
	// constructor tightens it to a quarter of the cover TTL (Codex round
	// 3, security): live covers renew at pass STARTS (the renewal phase
	// runs first), and River coalesces periodic ticks that fire while a
	// pass runs, so the gap between one cover's renewals is bounded by
	// pass duration + interval + the next renewal phase — with the pass
	// capped at TTL/4 and the interval validated at ≤ TTL/2 (config),
	// that gap stays within the TTL and a long scan can never let live
	// covers lapse. A timed-out pass parks its cursors and resumes,
	// exactly like a budget-limited one.
	absencePassTimeout = time.Hour
	// Cover-key classes: each registry rotation maintains its own
	// incarnation keys, because the two traversals complete
	// independently.
	absenceClassLFS   = "lfs"
	absenceClassPacks = "packs"
)

// AbsenceReconciler runs the reconciliation scan. Construct with
// NewAbsenceReconciler; Run executes one bounded pass per schedule tick.
// All cross-pass state is durable (job_scan_state cursors + the cover rows
// themselves); the struct carries only configuration and per-pass scratch.
type AbsenceReconciler struct {
	registry AbsenceRegistry
	blobs    BlobStatter
	// quotaEnforced gates cover charging, renewal, and generation bumps:
	// with quota admission disabled nothing consults floors or
	// reservations (the sweep restore pattern, Codex round 73) —
	// detection still reports.
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

	// Per-pass scratch, loaded from durable state at Run start and parked
	// at Run end.
	lfs, packs absenceCursorState
	// renewCursor round-robins the cover-renewal listing across passes
	// when it outruns its budget half; passes alternates the leading
	// rotation (Codex round 1, P2: a twin probe can overdraw the leader's
	// allocation by one unit, and at degenerate budgets a fixed order
	// would starve the trailing table forever). Both persist in the
	// renewal scan-state row.
	renewCursor string
	passes      int64
	// liveCovers is this pass's harvest of the renewal listing: the
	// highest live incarnation per (class, base). It spares the rotation
	// a point lookup per touched base; renewalComplete records whether
	// the harvest saw the WHOLE listing (when it did, a missing entry
	// proves no live cover exists).
	liveCovers      map[string]map[string]liveCover
	renewalComplete bool
	// lfsRange/packsRange track the base range each rotation is
	// streaming through, in memory only: shrink/release verdicts are
	// evaluated exclusively for ranges observed START to END within one
	// pass, so partial in-memory totals can never under-shrink a cover.
	lfsRange, packsRange rangeTracker
}

// liveCover is one harvested live aggregate incarnation.
type liveCover struct {
	key   string // synthetic cover key
	n     int64  // incarnation number
	bytes int64
}

// absenceCursorState is one registry rotation's durable bookkeeping.
type absenceCursorState struct {
	AfterRepo string `json:"after_repo"`
	// AfterKey is the second keyset component (lfs_objects.key or
	// packs.id).
	AfterKey string `json:"after_key"`
	// Traversals counts completed origin-to-origin rotations.
	Traversals int64 `json:"traversals"`
	// Dirty records that the CURRENT traversal hit a stat or cover error
	// somewhere; a dirty traversal cannot flip HasClean.
	Dirty bool `json:"dirty"`
	// HasClean records that some traversal completed with no errors
	// since the scan state was minted — the legacy-cover retirement
	// gate: after both classes have one clean traversal, every
	// still-absent loss provably lives in incarnation keys.
	HasClean bool `json:"has_clean"`
}

// rangeTracker follows one rotation's current base range within a pass.
type rangeTracker struct {
	class string
	base  string // "" = no pending range
	// started: the transition INTO base was observed this pass, so the
	// running total spans the whole range once the range ends this pass.
	started bool
	// dirty: a row of this range failed to settle or charge — the
	// running total may under-count, so shrink/release must not fire.
	dirty   bool
	running int64
}

// absenceIncarnationKey builds the deterministic aggregate cover key for
// one (base, class) incarnation. The zero-padded number makes byte order
// numeric order, so "highest live incarnation" is a suffix-ordered lookup.
func absenceIncarnationKey(base, class string, n int64) string {
	assertf(base != "" && (class == absenceClassLFS || class == absenceClassPacks) && n >= 1,
		"absenceIncarnationKey(%q, %q, %d)", base, class, n)
	return absenceIncarnationPrefix(base, class) + fmt.Sprintf("%019d", n)
}

// absenceIncarnationPrefix is the shared prefix of one (base, class)'s
// incarnation keys.
func absenceIncarnationPrefix(base, class string) string {
	return ref.AbsenceCoverPrefix + base + "/" + class + "-e"
}

// parseAbsenceCoverKey classifies a listed synthetic cover key:
// class/incarnation for this design's keys; legacy=true for any other
// segment spelling under the prefix (pre-#125 nonce keys).
func parseAbsenceCoverKey(key string) (base, class string, n int64, legacy, ok bool) {
	rest, found := strings.CutPrefix(key, ref.AbsenceCoverPrefix)
	if !found {
		return "", "", 0, false, false
	}
	i := strings.LastIndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return "", "", 0, false, false
	}
	base, seg := rest[:i], rest[i+1:]
	for _, c := range []string{absenceClassLFS, absenceClassPacks} {
		if digits, found := strings.CutPrefix(seg, c+"-e"); found && len(digits) == 19 {
			v, err := strconv.ParseInt(digits, 10, 64)
			if err == nil && v >= 1 {
				return base, c, v, false, true
			}
		}
	}
	return base, "", 0, true, true // legacy or foreign segment: renew-then-retire class
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

// loadState restores the durable cursors and renewal bookkeeping and
// resets the per-pass scratch. A missing or undecodable row degrades to a
// fresh scan — cursors are bookkeeping, never authority.
func (r *AbsenceReconciler) loadState(ctx context.Context, errs *boundedErrs) {
	r.lfs, r.packs = absenceCursorState{}, absenceCursorState{}
	r.renewCursor, r.passes = "", 0
	r.liveCovers = map[string]map[string]liveCover{
		absenceClassLFS:   make(map[string]liveCover),
		absenceClassPacks: make(map[string]liveCover),
	}
	r.renewalComplete = false
	r.lfsRange = rangeTracker{class: absenceClassLFS}
	r.packsRange = rangeTracker{class: absenceClassPacks}
	for _, c := range []struct {
		scope string
		into  *absenceCursorState
	}{{ref.ScanScopeAbsenceLFS, &r.lfs}, {ref.ScanScopeAbsencePacks, &r.packs}} {
		st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobAbsence, c.scope)
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: read %s cursor: %w", c.scope, err))
			continue
		}
		if !ok {
			continue
		}
		if err := json.Unmarshal([]byte(st.Payload), c.into); err != nil {
			// Undecodable bookkeeping (never written by this binary):
			// fresh scan, loudly.
			*c.into = absenceCursorState{}
			r.logger.Warn("absence reconcile: discarding undecodable cursor state", "scope", c.scope, "err", err)
		}
	}
	if st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewal); err != nil {
		errs.add(fmt.Errorf("absence reconcile: read renewal cursor: %w", err))
	} else if ok {
		r.renewCursor, r.passes = st.Payload, st.Generation
	}
}

// saveState parks the durable cursors on a bounded DETACHED context: the
// pass regularly ends by burning its own deadline, and losing the park
// replays this pass's segment (bounded transient over-cover: replayed
// totals are recomputed from scratch, never compounded).
func (r *AbsenceReconciler) saveState(ctx context.Context, errs *boundedErrs) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scanStateWriteTimeout)
	defer cancel()
	for _, c := range []struct {
		scope string
		from  *absenceCursorState
	}{{ref.ScanScopeAbsenceLFS, &r.lfs}, {ref.ScanScopeAbsencePacks, &r.packs}} {
		payload, err := json.Marshal(c.from)
		assertf(err == nil, "absence reconcile: marshal %s cursor: %v", c.scope, err)
		if err := r.registry.PutJobScanState(sctx, ref.ScanJobAbsence, c.scope, string(payload), 0); err != nil {
			errs.add(fmt.Errorf("absence reconcile: park %s cursor: %w", c.scope, err))
		}
	}
	if err := r.registry.PutJobScanState(sctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewal, r.renewCursor, r.passes); err != nil {
		errs.add(fmt.Errorf("absence reconcile: park renewal cursor: %w", err))
	}
}

// Run executes one bounded reconciliation pass. Order (Codex round 1 on
// PR #123): cover RENEWAL first — keeping already-discovered losses
// covered is the correctness core and must not wait on the rotation —
// then the two registry rotations split the remaining budget, leading
// class alternating between passes.
func (r *AbsenceReconciler) Run(ctx context.Context) (AbsenceReport, error) {
	report := AbsenceReport{QuotaEnforced: r.quotaEnforced}
	assertf(r.passTimeout > 0, "Run: non-positive pass timeout %v", r.passTimeout)
	ctx, cancelPass := context.WithTimeout(ctx, r.passTimeout)
	defer cancelPass()
	var errs boundedErrs
	st := newAbsencePassState()
	r.loadState(ctx, &errs)
	// The renewal gets at most half the budget so a large cover
	// population cannot starve discovery entirely; while the live cover
	// rows fit within that half every cover refreshes every pass, and
	// beyond it the round-robin cursor bounds every row's renewal period
	// (RenewalIncomplete flags the backlog).
	remaining := r.budget - r.renewCovers(ctx, r.budget-r.budget/2, &report, st, &errs)
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
	r.saveState(ctx, &errs)
	report.LFSTraversals, report.PackTraversals = r.lfs.Traversals, r.packs.Traversals
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
		"covers_renewed", report.CoversRenewed,
		"covers_rotated", report.CoversRotated,
		"covers_released", report.CoversReleased,
		"covers_decaying", report.CoversDecaying,
		"renewal_incomplete", report.RenewalIncomplete,
		"renewal_over_budget", report.RenewalOverBudget,
		"lfs_traversals", report.LFSTraversals,
		"pack_traversals", report.PackTraversals,
		"quota_enforced", report.QuotaEnforced)
	return report, errs.join()
}

// renewCovers re-charges each (base, class)'s highest-incarnation live
// cover row — one listing page plus one reserve per row, all derived from
// durable state, so ANY replica keeps every aggregate alive (issue #125).
// Superseded lower incarnations (crash leftovers) are deleted; legacy
// nonce-keyed covers from pre-#125 workers renew until both classes have
// one clean traversal, then decay by TTL (never deleted: during a rolling
// upgrade an old replica may still mint and depend on them — Codex round
// 1, P1). Renewal is the correctness core (a lapsed cover silently
// reopens the admission window), so it always offers the FULL listing:
// budget only sets the soft allocation, and overrunning it — flagged
// loudly — starves the discovery rotation for the pass rather than
// letting live covers lapse (Codex round 1, P1: a round-robin backlog
// could exceed the TTL between one row's renewals). Only a context abort
// leaves the listing incomplete, parked on the durable cursor. Returns
// the work charged: one unit per listed row.
func (r *AbsenceReconciler) renewCovers(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	if !r.quotaEnforced {
		r.renewalComplete = true // nothing consults reservations (Codex round 73)
		return 0
	}
	after := r.renewCursor
	startRaw := after
	startedAtZero := after == ""
	wrapped := startedAtZero
	defer func() { r.renewCursor = after }()
	renewLegacy := !(r.lfs.HasClean && r.packs.HasClean)
	// held delays each epoch-keyed row one step: a following row with the
	// same (base, class) supersedes it (higher incarnation, zero-padded
	// byte order), so the held row is a crash leftover to delete.
	var held *ref.AbsenceCoverRow
	flushHeld := func(supersededBy *ref.AbsenceCoverRow) {
		if held == nil {
			return
		}
		base, class, n, _, ok := parseAbsenceCoverKey(held.Key)
		assertf(ok, "renewCovers: held unparseable key %q", held.Key)
		if supersededBy != nil {
			b2, c2, _, legacy2, ok2 := parseAbsenceCoverKey(supersededBy.Key)
			if ok2 && !legacy2 && b2 == base && c2 == class {
				// A higher incarnation exists: this row is a crash
				// leftover — release it.
				if err := r.registry.DeleteAbsenceCover(ctx, held.Key); err != nil {
					errs.add(fmt.Errorf("absence reconcile: release superseded cover %s: %w", held.Key, err))
				} else {
					report.CoversReleased++
				}
				held = nil
				return
			}
		}
		if r.chargeCover(ctx, held.Base, held.Key, held.Bytes, report, st, errs) {
			report.CoversRenewed++
		}
		if prev, seen := r.liveCovers[class][base]; !seen || n > prev.n {
			r.liveCovers[class][base] = liveCover{key: held.Key, n: n, bytes: held.Bytes}
		}
		held = nil
	}
	overrun := func() {
		if charged > budget && !report.RenewalOverBudget {
			report.RenewalOverBudget = true
			r.logger.Warn("absence reconcile: cover renewal overran its budget allocation; discovery is starved this pass — raise CODESTORAGE_ABSENCE_RECONCILE_BUDGET above twice the live cover population",
				"allocation", budget, "charged", charged)
		}
	}
	for {
		if ctx.Err() != nil {
			// The ONLY incomplete-renewal path: parked on the durable
			// cursor, resumed (and completed) by the next pass's renewal.
			errs.add(ctx.Err())
			flushHeld(nil)
			report.RenewalIncomplete = true
			return charged
		}
		rows, err := r.registry.ListAbsenceCovers(ctx, after, absencePageSize)
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: list covers: %w", err))
			flushHeld(nil)
			report.RenewalIncomplete = true
			return charged
		}
		if len(rows) == 0 {
			flushHeld(nil)
			if wrapped {
				after = ""
				r.renewalComplete = startedAtZero
				overrun()
				return charged
			}
			after, wrapped = "", true
			continue
		}
		for i := range rows {
			row := &rows[i]
			if wrapped && !startedAtZero && row.RawKey > startRaw {
				flushHeld(nil)
				after = ""
				r.renewalComplete = true // full listing offered this pass
				overrun()
				return charged
			}
			if ctx.Err() != nil {
				errs.add(ctx.Err())
				flushHeld(nil)
				report.RenewalIncomplete = true
				return charged
			}
			charged++
			after = row.RawKey
			_, _, _, legacy, ok := parseAbsenceCoverKey(row.Key)
			if !ok {
				continue // unparseable spelling under the prefix: not ours
			}
			if legacy {
				flushHeld(row)
				if renewLegacy {
					if r.chargeCover(ctx, row.Base, row.Key, row.Bytes, report, st, errs) {
						report.CoversRenewed++
					}
					continue
				}
				// Both classes completed a clean traversal: every
				// still-absent loss lives in incarnation keys, so the
				// pre-#125 spelling stops renewing and expires by TTL.
				// Never deleted: a not-yet-upgraded replica may still be
				// minting and depending on nonce-keyed covers mid-roll,
				// and deleting one before the rotation rediscovers its
				// rows would uncover them (Codex round 1, P1).
				report.CoversDecaying++
				continue
			}
			flushHeld(row)
			held = row
		}
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

// rotationPhase pages one registry from its durable resume cursor and
// settles each row within the phase budget, offering the keyspace at most
// once per pass. An origin crossing completes the traversal (tick); the
// scan then continues into the next traversal until it reaches this pass's
// starting point. Base-range boundaries drive the cover lifecycle: see the
// header comment. list returns one keyset page as (repo, cursorKey) pairs
// with a settle callback per row — the two registries differ only there.
func (r *AbsenceReconciler) rotationPhase(ctx context.Context, cur *absenceCursorState, tr *rangeTracker, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs,
	list func(ctx context.Context, afterRepo, afterKey string, limit int) (repos, keys []string, settle []func() int64, err error)) (charged int64) {
	startRepo, startKey := cur.AfterRepo, cur.AfterKey
	startedAtZero := startRepo == "" && startKey == ""
	wrapped := startedAtZero
	// The pending range (if any) began before this pass: never evaluate
	// it — its in-memory running total would be partial. Resuming
	// mid-keyspace therefore pre-seeds the tracker with the CURSOR's own
	// base, unstarted (Codex round 1, P1): without it the first resumed
	// row would read as a range transition and the suffix's total would
	// masquerade as the whole range's, shrinking covers that still
	// protect rows in the skipped prefix.
	*tr = rangeTracker{class: tr.class}
	if !startedAtZero {
		tr.base = baseOf(startRepo)
	}
	defer func() { tr.abandon() }()
	for charged < budget {
		if ctx.Err() != nil {
			errs.add(ctx.Err())
			return charged
		}
		repos, keys, settle, err := list(ctx, cur.AfterRepo, cur.AfterKey, r.pageLimit(budget-charged))
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: list %s registry: %w", tr.class, err))
			return charged
		}
		if len(repos) == 0 {
			// Origin crossing: the traversal is complete. Every pending
			// range ends AT the keyspace end.
			r.endRange(ctx, tr, report, st, errs)
			r.tickTraversal(cur, tr.class, report)
			cur.AfterRepo, cur.AfterKey = "", ""
			if wrapped {
				return charged // the whole keyspace was offered this pass
			}
			wrapped = true
			continue
		}
		for i := range repos {
			if wrapped && !startedAtZero && cursorAfter(startRepo, startKey, repos[i], keys[i]) {
				// Wrapped past this pass's own starting point: every
				// remaining row was already offered above. The cursor
				// PARKS on the last settled row (never resets); the
				// pending range is mid-flight in the PREVIOUS
				// traversal's tail and must not be evaluated.
				return charged
			}
			if ctx.Err() != nil {
				errs.add(ctx.Err())
				return charged
			}
			if charged >= budget {
				return charged // the cursor rests on the previous row
			}
			// Base-range boundary: rows are keyset-ordered by repo, and
			// a base's namespace members (base, base+suffix) sort
			// contiguously, so a base change ends the previous range.
			if b := baseOf(repos[i]); b != tr.base {
				r.endRange(ctx, tr, report, st, errs)
				tr.base, tr.started, tr.dirty, tr.running = b, true, false, 0
			}
			charged += settle[i]()
			cur.AfterRepo, cur.AfterKey = repos[i], keys[i]
		}
	}
	return charged
}

// lfsPhase adapts the lfs_objects registry into rotationPhase.
func (r *AbsenceReconciler) lfsPhase(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) int64 {
	return r.rotationPhase(ctx, &r.lfs, &r.lfsRange, budget, report, st, errs,
		func(ctx context.Context, afterRepo, afterKey string, limit int) ([]string, []string, []func() int64, error) {
			rows, err := r.registry.ListFleetLFSObjects(ctx, afterRepo, afterKey, limit)
			if err != nil {
				return nil, nil, nil, err
			}
			repos := make([]string, len(rows))
			keys := make([]string, len(rows))
			settle := make([]func() int64, len(rows))
			for i := range rows {
				row := rows[i]
				repos[i], keys[i] = row.Repo, row.Key
				settle[i] = func() int64 { return r.settleLFSRow(ctx, row, &r.lfsRange, report, st, errs) }
			}
			return repos, keys, settle, nil
		})
}

// packPhase adapts the packs registry into rotationPhase.
func (r *AbsenceReconciler) packPhase(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) int64 {
	return r.rotationPhase(ctx, &r.packs, &r.packsRange, budget, report, st, errs,
		func(ctx context.Context, afterRepo, afterID string, limit int) ([]string, []string, []func() int64, error) {
			rows, err := r.registry.ListFleetPacks(ctx, afterRepo, afterID, limit)
			if err != nil {
				return nil, nil, nil, err
			}
			repos := make([]string, len(rows))
			keys := make([]string, len(rows))
			settle := make([]func() int64, len(rows))
			for i := range rows {
				row := rows[i]
				repos[i], keys[i] = row.Repo, row.ID
				settle[i] = func() int64 { return r.settlePackRow(ctx, row, &r.packsRange, report, st, errs) }
			}
			return repos, keys, settle, nil
		})
}

// tickTraversal records an origin crossing for one registry class.
func (r *AbsenceReconciler) tickTraversal(cur *absenceCursorState, class string, report *AbsenceReport) {
	clean := !cur.Dirty
	if clean {
		cur.HasClean = true
	}
	r.logger.Info("absence reconcile: rotation traversal completed",
		"class", class, "traversal", cur.Traversals, "clean", clean)
	cur.Traversals++
	cur.Dirty = false
	if class == absenceClassLFS {
		report.LFSTraversals = cur.Traversals
	} else {
		report.PackTraversals = cur.Traversals
	}
}

// abandon discards the pending range without evaluation (pass boundary:
// the running total is partial). Grow charges already made remain valid —
// they only ever raise a cover toward the true total.
func (tr *rangeTracker) abandon() {
	tr.base, tr.started, tr.dirty, tr.running = "", false, false, 0
}

// endRange completes the pending base range: when the WHOLE range was
// observed within this pass with no errors, the running total is exact and
// the cover lifecycle may shrink or release the base's aggregate.
func (r *AbsenceReconciler) endRange(ctx context.Context, tr *rangeTracker, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	base, class := tr.base, tr.class
	started, dirty, running := tr.started, tr.dirty, tr.running
	tr.abandon()
	if base == "" || !started || !r.quotaEnforced {
		return
	}
	if dirty || st.purged[base] || st.coverSkip[base] {
		return // the total may under-count: never shrink on it
	}
	head, ok := r.resolveHead(ctx, base, class, st, errs)
	if !ok || head.key == "" {
		return // no live cover (or unresolved): nothing to shrink
	}
	switch {
	case running == 0:
		// Fully healed (or rows retired/folded away): release the row.
		if err := r.registry.DeleteAbsenceCover(ctx, head.key); err != nil {
			errs.add(fmt.Errorf("absence reconcile: release healed cover %s: %w", head.key, err))
			return
		}
		report.CoversReleased++
		delete(r.liveCovers[class], base)
	case running < head.bytes:
		// Shrink: mint the next incarnation at the exact total FIRST
		// (never a coverage gap), then delete the stale GREATEST-pinned
		// row. A crash between the two leaves both; the next renewal
		// deletes the superseded one.
		next := liveCover{key: absenceIncarnationKey(base, class, head.n+1), n: head.n + 1, bytes: running}
		if !r.chargeCover(ctx, base, next.key, running, report, st, errs) {
			return // retried next traversal; the old row keeps covering
		}
		if err := r.registry.DeleteAbsenceCover(ctx, head.key); err != nil {
			errs.add(fmt.Errorf("absence reconcile: release shrunk cover %s: %w", head.key, err))
		} else {
			report.CoversReleased++
		}
		report.CoversRotated++
		r.liveCovers[class][base] = next
	}
}

// resolveHead finds base's live incarnation for class: the renewal harvest
// when it saw the whole listing this pass, a point lookup otherwise.
// ok=false means the lookup failed (reported); key=="" means none exists.
func (r *AbsenceReconciler) resolveHead(ctx context.Context, base, class string, st *absencePassState, errs *boundedErrs) (liveCover, bool) {
	if head, seen := r.liveCovers[class][base]; seen {
		return head, true
	}
	if r.renewalComplete {
		return liveCover{}, true // the full harvest proves no live cover
	}
	row, found, err := r.registry.AbsenceCoverHead(ctx, absenceIncarnationPrefix(base, class))
	if err != nil {
		errs.add(fmt.Errorf("absence reconcile: cover head %s/%s: %w", base, class, err))
		return liveCover{}, false
	}
	if !found {
		r.liveCovers[class][base] = liveCover{}
		return liveCover{}, true
	}
	_, c2, n, legacy, ok := parseAbsenceCoverKey(row.Key)
	if !ok || legacy || c2 != class {
		return liveCover{}, false // foreign spelling where ours belongs: leave it alone
	}
	head := liveCover{key: row.Key, n: n, bytes: row.Bytes}
	r.liveCovers[class][base] = head
	return head, true
}

// chargeDeficit folds one freshly settled deficit into the pending range's
// running total and grows the base's live cover whenever the recomputed
// total exceeds it — a fresh incident mints its first incarnation
// immediately, so a new loss is admission-visible at discovery. The charge
// is always the traversal's own running total (never "live + delta"), so
// crash replays recompute rather than compound.
func (r *AbsenceReconciler) chargeDeficit(ctx context.Context, tr *rangeTracker, deficit int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	assertf(deficit > 0, "chargeDeficit: non-positive deficit %d for %s", deficit, tr.base)
	tr.running += deficit
	if !r.quotaEnforced {
		return // detection still reported; nothing consults reservations
	}
	head, ok := r.resolveHead(ctx, tr.base, tr.class, st, errs)
	if !ok {
		tr.dirty = true // unaccounted deficit: never shrink on this range
		return
	}
	if tr.running <= head.bytes {
		return // still within the live cover: renewal keeps it fresh
	}
	if head.key == "" {
		head = liveCover{key: absenceIncarnationKey(tr.base, tr.class, 1), n: 1}
	}
	if !r.chargeCover(ctx, tr.base, head.key, tr.running, report, st, errs) {
		tr.dirty = true
		return
	}
	head.bytes = tr.running
	r.liveCovers[tr.class][tr.base] = head
}

func (r *AbsenceReconciler) pageLimit(remaining int64) int {
	limit := absencePageSize
	if remaining < int64(limit) {
		limit = int(remaining)
	}
	assertf(limit > 0, "pageLimit: non-positive limit from remaining %d", remaining)
	return limit
}

// settleLFSRow point-checks one lfs_objects row and folds a genuine loss
// into the pending range. Returns the work charged: one unit for the row
// plus one per extra twin-spelling probe.
func (r *AbsenceReconciler) settleLFSRow(ctx context.Context, row ref.AbsenceLFSRow, tr *rangeTracker, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	charged = 1
	assertf(row.Repo != "" && row.Key != "" && row.Bytes >= 0, "settleLFSRow: bad row %+v", row)
	report.LFSRowsExamined++
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
	// The deficit is registered bytes minus EVERY physical copy across the
	// ADR-41 spellings (Codex rounds 4+12 on PR #123): both copies are in
	// the usage listing, so any surviving bytes — full or short,
	// registered spelling or twin — compensate the floor and must not be
	// re-covered.
	physical := int64(0)
	if size, err := r.blobs.Stat(ctx, row.Key); err == nil {
		if size >= row.Bytes {
			return charged // fully present under the registered spelling
		}
		physical = size
	} else if !errors.Is(err, blob.ErrNotFound) {
		report.StatErrors++
		// An unverifiable row means the traversal's totals may
		// under-count: the range must not shrink and the traversal is
		// not clean.
		tr.dirty, r.cursorFor(tr.class).Dirty = true, true
		errs.add(fmt.Errorf("absence reconcile: stat %s: %w", row.Key, err))
		return charged
	}
	twinPhysical := int64(0)
	if repoID, oid, ok := splitLFSKey(row.Key); ok {
		canonical, legacy := blob.LFSKey(repoID, oid), blob.LFSLegacyKey(repoID, oid)
		twin := legacy
		if row.Key == legacy {
			twin = canonical
		}
		charged++
		if size, err := r.blobs.Stat(ctx, twin); err == nil {
			twinPhysical = size
		} else if !errors.Is(err, blob.ErrNotFound) {
			report.StatErrors++
			tr.dirty, r.cursorFor(tr.class).Dirty = true, true
			errs.add(fmt.Errorf("absence reconcile: stat twin %s: %w", twin, err))
			return charged
		}
	}
	deficit := row.Bytes - physical - twinPhysical
	switch {
	case deficit <= 0:
		// The surviving copies compensate the whole registered size:
		// physically listed, no floor deflation, nothing to charge. The
		// CLASSIFICATION still depends on whether a complete copy exists
		// (Codex round 13 on PR #123): only a full twin is restorable via
		// download-time restore (PR #104) — split short copies are an
		// integrity loss even though quota accounting is whole.
		if twinPhysical >= row.Bytes {
			report.LFSRowsTwinOnly++
			r.logger.Warn("absence reconcile: registered lfs object compensated by its full twin spelling",
				"repo", row.Repo, "key", row.Key, "registered", row.Bytes, "physical", physical, "twin_physical", twinPhysical)
		} else {
			report.LFSRowsTruncated++
			r.logger.Error("absence reconcile: no complete copy of registered lfs object survives, although listed bytes cover its size",
				"repo", row.Repo, "key", row.Key, "registered", row.Bytes, "physical", physical, "twin_physical", twinPhysical)
		}
		return charged
	case physical > 0 || twinPhysical > 0:
		report.LFSRowsTruncated++
		r.logger.Error("absence reconcile: registered lfs object is shorter than its registered size across all spellings",
			"repo", row.Repo, "key", row.Key, "registered", row.Bytes, "physical", physical, "twin_physical", twinPhysical)
		r.chargeDeficit(ctx, tr, deficit, report, st, errs)
		return charged
	}
	report.LFSRowsAbsent++
	r.logger.Error("absence reconcile: registered lfs object bytes are missing from blob storage",
		"repo", row.Repo, "key", row.Key, "bytes", row.Bytes)
	r.chargeDeficit(ctx, tr, row.Bytes, report, st, errs)
	return charged
}

// settlePackRow point-checks one packs row and folds a genuine loss into
// the pending range.
func (r *AbsenceReconciler) settlePackRow(ctx context.Context, row ref.AbsencePackRow, tr *rangeTracker, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	charged = 1
	assertf(row.Repo != "" && row.ID != "" && row.Key != "" && row.Bytes >= 0, "settlePackRow: bad row %+v", row)
	report.PackRowsExamined++
	base := baseOf(row.Repo)
	if r.repoPurged(ctx, base, st, errs) {
		report.RowsSkippedPurged++
		return charged
	}
	if size, err := r.blobs.Stat(ctx, row.Key); err == nil {
		if row.Bytes == 0 || size >= row.Bytes {
			return charged // fully present (or pre-#101 row: size unknown)
		}
		report.PackRowsTruncated++
		r.logger.Error("absence reconcile: registered pack is shorter than its registered size — committed refs may reference this pack",
			"repo", row.Repo, "key", row.Key, "registered", row.Bytes, "physical", size)
		r.chargeDeficit(ctx, tr, row.Bytes-size, report, st, errs)
		return charged
	} else if !errors.Is(err, blob.ErrNotFound) {
		report.StatErrors++
		tr.dirty, r.cursorFor(tr.class).Dirty = true, true
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
	r.chargeDeficit(ctx, tr, row.Bytes, report, st, errs)
	return charged
}

// cursorFor maps a class to its durable cursor state.
func (r *AbsenceReconciler) cursorFor(class string) *absenceCursorState {
	if class == absenceClassPacks {
		return &r.packs
	}
	return &r.lfs
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

// chargeCover reserves size bytes under key's repair-cover spelling and
// bumps the floor generation, with per-base error memoization. Cover
// FIRST, then bump (the round 57/58 order): an admission already holding
// a floor scanned during the absence sees the cover in
// active_reservations, while the bump only stops FUTURE serves of cached
// deflated floors. Returns whether the cover row was charged.
func (r *AbsenceReconciler) chargeCover(ctx context.Context, base, key string, size int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) bool {
	if st.coverSkip[base] {
		report.CoverErrors++
		return false
	}
	if st.purged[base] {
		return false // fenced mid-pass: the rows are on their way out
	}
	if err := ref.ReserveRestoreCover(ctx, r.registry, blob.QuotaRepoGroup(base), key, size, r.coverTTL); err != nil {
		if errors.Is(err, ref.ErrRepoPurged) {
			// Raced a purge fence minted after the probe: the rows are on
			// their way out; treat like the probe verdict.
			st.purged[base] = true
			return false
		}
		// One denial (e.g. the active-reservation row cap) means further
		// covers for this base would fail the same way this pass: skip
		// them — the next pass retries — instead of minting budget-many
		// failed transactions.
		st.coverSkip[base] = true
		report.CoverErrors++
		errs.add(fmt.Errorf("absence reconcile: cover %s: %w", key, err))
		return false
	}
	report.CoversCharged++
	report.BytesCovered += size
	if err := r.registry.BumpLFSAdoptionGen(ctx, base); err != nil {
		// The cover row still guards every admission that re-lists; the
		// next pass re-charges and re-bumps.
		errs.add(fmt.Errorf("absence reconcile: bump adoption gen %s: %w", base, err))
	}
	return true
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
