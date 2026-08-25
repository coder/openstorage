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
//     with no stat/charge errors, its running total is exact. A smaller
//     total mints incarnation n+1 charged exactly and deletes the old row
//     (mint first — never a coverage gap); a zero total deletes the row
//     outright. Deletes are safe because the reconciler is the only
//     writer under the prefix and River's unique jobs serialize passes
//     fleet-wide. The pending range PARKS WITH the durable cursor (base,
//     started flag, dirty flag, running total — Codex round 2), so a
//     range spanning any number of budget-limited passes accumulates and
//     evaluates exactly; crash replays re-derive the same totals from the
//     parked cursor (idempotent, never compounding).
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
	"sort"
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
	// RenewAbsenceCovers re-stamps the TTL of every LIVE deterministic
	// cover row AND every un-folded legacy nonce row in DB-clock
	// statements — the pass-start bulk touch that makes cover protection
	// independent of the keyset listing completing within the TTL (Codex
	// rounds 9+15). Strict TTL: expired rows stay dead; nonce rows
	// marked folded decay.
	RenewAbsenceCovers(ctx context.Context, ttl time.Duration) (int64, error)
	// MarkAbsenceCoverFolded removes one legacy nonce row from the bulk
	// touch once its total is provably folded into both incarnations
	// (Codex round 15); an old replica's re-charge un-marks it.
	MarkAbsenceCoverFolded(ctx context.Context, rawKey string) error
	// ListAbsenceCovers pages the live absence-cover reservation rows —
	// the durable tracking state the renewal phase re-derives its
	// obligations from every pass (issue #125). ttl is the liveness
	// window admission consults: expired rows are excluded (renewing or
	// migrating one would resurrect a stale reservation, Codex round 6).
	ListAbsenceCovers(ctx context.Context, afterRaw string, limit int, ttl time.Duration) ([]ref.AbsenceCoverRow, error)
	// AbsenceCoverHead point-reads the highest LIVE incarnation under a
	// synthetic-key prefix — the charge path's fallback when the renewal
	// listing did not cover the base this pass. Same ttl semantics.
	AbsenceCoverHead(ctx context.Context, prefix string, ttl time.Duration) (ref.AbsenceCoverRow, bool, error)
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
	// CoversTouched counts live deterministic cover rows whose TTL the
	// pass-start bulk touch re-stamped (Codex round 9): protection never
	// depends on the keyset listing completing within the TTL, so a
	// multi-pass listing round robin can delay lifecycle work
	// (fold/shrink/release) but cannot lapse a live cover.
	CoversTouched int64 `json:"covers_touched"`
	// RenewalIncomplete reports the renewal listing was cut short by the
	// pass deadline (never by budget): un-renewed rows resume first on
	// the durable cursor next pass. Investigate the blob store or ledger
	// if persistent — covers lapse if renewals cannot land within the
	// TTL.
	RenewalIncomplete bool `json:"renewal_incomplete"`
	// RenewalOverBudget reports renewal charged more than its soft
	// allocation (half the budget): every live cover still renewed —
	// renewal is the correctness core and never lapses on budget — while
	// discovery keeps its guaranteed half-budget allowance (Codex round
	// 14: a zero-allowance clamp would be an absorbing state under the
	// bulk touch). Raise the budget above twice the live cover
	// population.
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
	// migratedLegacy memoizes the byte WATERMARK this PROCESS already
	// folded per nonce-keyed cover. Purely an optimization: every fresh
	// worker re-folds each live nonce once (GREATEST-safe over-cover the
	// rotation right-sizes within one clean range), but re-folding every
	// pass would inflate and re-rotate the incarnation endlessly while
	// the nonce decays. A watermark, not a boolean (Codex round 7, P1):
	// an old replica can re-grow the same nonce mid-roll AFTER this
	// process folded it, and a key-only memo would strand the growth in
	// a decaying row the incarnations never absorbed. Process-lifetime,
	// deliberately not per-pass — correctness never depends on it.
	migratedLegacy map[string]int64
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
	// since the scan state was minted (observability; legacy-cover
	// handling migrates rather than gating on it — see renewCovers).
	HasClean bool `json:"has_clean"`
	// The pending base range parks WITH the cursor (Codex round 2 on PR
	// #132, P1): a base whose rows span multiple phase allocations must
	// carry its partial deficit total across passes, or each resumed
	// chunk would restart at zero and the grow charge (always the
	// recomputed total, never additive) could never reach the true sum.
	// Parking the whole tracker keeps multi-pass ranges EXACT: the
	// range-end evaluation only needs an error-free observed
	// start-to-end range, regardless of how many passes it spanned.
	RangeBase    string `json:"range_base,omitempty"`
	RangeStarted bool   `json:"range_started,omitempty"`
	RangeDirty   bool   `json:"range_dirty,omitempty"`
	RangeRunning int64  `json:"range_running,omitempty"`
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
		registry:       registry,
		blobs:          blobs,
		quotaEnforced:  quotaEnforced,
		budget:         budget,
		coverTTL:       coverTTL,
		logger:         logger,
		passTimeout:    passTimeout,
		migratedLegacy: make(map[string]int64),
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
	// The renewal is ALLOCATED half the budget but always offers the full
	// listing (a bounded round-robin would lapse covers); discovery gets
	// a GUARANTEED floor of the other half regardless of the overrun
	// (Codex round 14, security): with the bulk touch keeping every live
	// cover fresh, a cover population at or beyond the budget would
	// otherwise clamp the rotation allowance to zero on every pass — an
	// absorbing state where further losses stay uncovered indefinitely.
	// The pass wall clock (TTL/4) bounds total work either way;
	// RenewalOverBudget flags the sizing problem loudly.
	remaining := r.budget - r.renewCovers(ctx, r.budget-r.budget/2, &report, st, &errs)
	if floor := r.budget / 2; remaining < floor {
		remaining = floor
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
		"covers_touched", report.CoversTouched,
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
// Superseded lower incarnations (crash leftovers) are deleted. Legacy
// nonce-keyed covers from pre-#125 workers are MIGRATED on sight (Codex
// round 4, P1): their bytes fold into the base's deterministic LFS-class
// incarnation via the ledger's GREATEST merge — grow-only, and the
// rotation right-sizes the merged row within one clean range observation
// — after which the nonce row decays by TTL. Migration needs no rollout
// fence: an old replica can mint nonce covers at any point of a mixed
// roll, and every new-replica pass immediately re-protects them under a
// key its own renewals keep alive (deleting or merely aging-out nonce
// rows could lapse them while the rotation is still far away). Renewal
// is the correctness core (a lapsed cover silently reopens the admission
// window), so it always offers the FULL listing: budget only sets the
// soft allocation, and overrunning it — flagged loudly — starves the
// discovery rotation for the pass rather than letting live covers lapse
// (Codex round 1, P1). Only a context abort leaves the listing
// incomplete, parked STRICTLY BEFORE any row whose renewal did not
// happen (Codex round 4, P1: parking past an unrenewed held row would
// skip it until the next full wrap). Returns the work charged: one unit
// per listed row.
func (r *AbsenceReconciler) renewCovers(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) (charged int64) {
	if !r.quotaEnforced {
		r.renewalComplete = true // nothing consults reservations (Codex round 73)
		return 0
	}
	// Bulk touch FIRST (Codex round 9): one DB-clock statement re-stamps
	// every live deterministic cover's TTL, so protection never depends
	// on the keyset listing below completing within the TTL — a large
	// population or degraded ledger latency can turn that listing into a
	// multi-pass round robin, and a row aging past the TTL mid-cycle
	// would be excluded as expired and silently never renewed. The
	// listing remains the precise mechanism for everything else: legacy
	// folds, superseded-row cleanup, byte re-pinning, and the liveCovers
	// harvest. On failure the cursor is untouched and the pass carries
	// the renewal-incomplete flag.
	touched, err := r.registry.RenewAbsenceCovers(ctx, r.coverTTL)
	report.CoversTouched = touched
	if err != nil {
		errs.add(fmt.Errorf("absence reconcile: bulk-renew covers: %w", err))
		report.RenewalIncomplete = true
		return 0
	}
	after := r.renewCursor
	startRaw := after
	startedAtZero := after == ""
	wrapped := startedAtZero
	defer func() { r.renewCursor = after }()
	// held delays each epoch-keyed row one step: a following row with the
	// same (base, class) supersedes it (higher incarnation, zero-padded
	// byte order), so the held row is a crash leftover to delete.
	// heldPrev remembers the cursor position strictly BEFORE the held
	// row: abort paths park there, so a renewal the dead context skipped
	// is re-offered first thing next pass (Codex round 4, P1).
	var held *ref.AbsenceCoverRow
	heldPrev := after
	abort := func(err error) {
		if held != nil {
			after = heldPrev // the held row was never renewed: re-offer it
			held = nil
		}
		errs.add(err)
		report.RenewalIncomplete = true
	}
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
				// leftover (rotate minted the successor but the delete —
				// or its floor invalidation — never landed) — release
				// it, under the same proven bump-before-delete as every
				// compensation-shrinking delete (Codex round 12). On
				// bump failure, fall through to the normal renewal: the
				// leftover stays alive and protective until a bump
				// lands.
				if err := r.registry.BumpLFSAdoptionGen(ctx, base); err != nil {
					errs.add(fmt.Errorf("absence reconcile: invalidate floors before releasing superseded cover %s: %w", held.Key, err))
				} else {
					if err := r.registry.DeleteAbsenceCover(ctx, held.Key); err != nil {
						errs.add(fmt.Errorf("absence reconcile: release superseded cover %s: %w", held.Key, err))
					} else {
						report.CoversReleased++
					}
					held = nil
					return
				}
			}
		}
		if r.chargeCover(ctx, held.Base, held.Key, held.Bytes, true, report, st, errs) {
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
			r.logger.Warn("absence reconcile: cover renewal overran its budget allocation (discovery keeps its guaranteed half-budget allowance) — raise CODESTORAGE_ABSENCE_RECONCILE_BUDGET above twice the live cover population",
				"allocation", budget, "charged", charged)
		}
	}
	for {
		if ctx.Err() != nil {
			// The ONLY incomplete-renewal paths: parked on the durable
			// cursor — before the held row, whose renewal did not
			// happen — and resumed first thing next pass.
			abort(ctx.Err())
			return charged
		}
		rows, err := r.registry.ListAbsenceCovers(ctx, after, absencePageSize, r.coverTTL)
		if err != nil {
			abort(fmt.Errorf("absence reconcile: list covers: %w", err))
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
				abort(ctx.Err())
				return charged
			}
			charged++
			prevAfter := after
			after = row.RawKey
			_, _, _, legacy, ok := parseAbsenceCoverKey(row.Key)
			if !ok {
				continue // unparseable spelling under the prefix: not ours
			}
			if legacy {
				flushHeld(row)
				if !r.migrateLegacyCover(ctx, row, report, st, errs) {
					// Neither folded nor renewed (Codex round 8, P1):
					// the nonce's TTL keeps ticking while one or both
					// incarnations may be unfunded. Park strictly
					// BEFORE it — the held-row abort semantics — so
					// the next pass re-offers it first instead of
					// waiting a full listing wrap.
					after = prevAfter
					abort(fmt.Errorf("absence reconcile: legacy cover %s neither folded nor renewed", row.Key))
					return charged
				}
				continue
			}
			flushHeld(row)
			held, heldPrev = row, prevAfter
		}
	}
}

// migrateLegacyCover folds one pre-#125 nonce-keyed cover into its base's
// deterministic incarnations (Codex round 4, P1): the ledger's GREATEST
// merge grows each incarnation to at least the legacy total — same-universe
// totals, so max is the right merge. A pre-#125 cover aggregates BOTH
// classes' deficits with no class split (Codex round 5, P1), so the total
// folds into BOTH class incarnations: each class's own next clean range
// right-sizes (or row-less-releases) its copy, and until then the transient
// over-cover only tightens admission. The nonce row then decays by TTL,
// un-renewed — but only once both folds are proven durable; if either
// fails, the legacy row is renewed as-is so nothing lapses, and the next
// pass retries (grow-only merges make the retry idempotent). Reports
// whether the row's protection is secured for this pass (folded, renewed,
// or the base is purge-fenced); false means NEITHER fold nor renewal
// landed (Codex round 8, P1) and the caller must park the renewal cursor
// before the row.
func (r *AbsenceReconciler) migrateLegacyCover(ctx context.Context, row *ref.AbsenceCoverRow, report *AbsenceReport, st *absencePassState, errs *boundedErrs) bool {
	if folded, seen := r.migratedLegacy[row.RawKey]; seen && row.Bytes <= folded && r.foldedHeadsLive(ctx, row.Base, st, errs) {
		// Already folded up to this total by this process: decay on. A
		// LARGER listed total re-folds (Codex round 7, P1): an old
		// replica can re-grow the same nonce mid-roll after the first
		// fold, and that growth must reach the incarnations before the
		// nonce decays. And the shortcut holds only while BOTH
		// deterministic heads still exist in the ledger (Codex round 13,
		// P1): the lifecycle can have released them after a heal — by
		// ANY worker, so a process-local memo invalidation could not
		// cover it — and an old replica re-charging the same nonce at an
		// equal total is a NEW incident the released incarnations no
		// longer compensate. Right-sized (smaller) heads still shortcut:
		// a clean range's exact re-derivation supersedes the nonce's
		// stale aggregate, and re-grown deficits follow normal discovery
		// latency like any fresh loss. Re-mark on every visit (idempotent
		// UPDATE): an old replica's re-charge overwrote owner, and an
		// un-marked row would be bulk-touched forever instead of
		// decaying once the fleet finishes rolling.
		if err := r.registry.MarkAbsenceCoverFolded(ctx, row.RawKey); err != nil {
			errs.add(fmt.Errorf("absence reconcile: re-mark legacy cover folded %s: %w", row.Key, err))
		}
		report.CoversDecaying++
		return true
	}
	migrated := true
	for _, class := range []string{absenceClassLFS, absenceClassPacks} {
		if !r.foldIntoIncarnation(ctx, row.Base, class, row.Bytes, false, report, st, errs) {
			migrated = false
			break
		}
	}
	if migrated {
		// Mark the nonce folded so the bulk touch stops renewing it and
		// TTL decay begins (Codex round 15). Memoize only after the mark
		// proves durable: an unmarked nonce keeps being bulk-touched, so
		// on mark failure the next pass re-folds (grow-only, idempotent)
		// and retries the mark — protection is intact either way.
		if err := r.registry.MarkAbsenceCoverFolded(ctx, row.RawKey); err != nil {
			errs.add(fmt.Errorf("absence reconcile: mark legacy cover folded %s: %w", row.Key, err))
			return true
		}
		r.migratedLegacy[row.RawKey] = row.Bytes
		report.CoversDecaying++
		return true
	}
	// Migration could not be proven durable (head lookup or charge
	// failed, or the base is purge-fenced — where nothing must be
	// re-minted): renew the nonce row as-is so nothing lapses, except
	// for purged bases, whose rows are on their way out anyway. The
	// renewal is a same-key re-charge of a listed row, so it bypasses
	// the pass-level coverSkip the failed fold may have just set
	// (Codex round 7, P1): the ledger excludes the incoming key from
	// its row-cap count, so this can succeed where a mint was denied.
	if st.purged[row.Base] {
		return true
	}
	if r.chargeCover(ctx, row.Base, row.Key, row.Bytes, true, report, st, errs) {
		report.CoversRenewed++
		return true
	}
	return false
}

// foldedHeadsLive reports whether BOTH deterministic class incarnations
// exist in the ledger right now — the DB-authoritative validity check for
// the legacy-fold memo shortcut (Codex round 13). A failed lookup reads as
// not-live: the caller re-folds (grow-only, idempotent) or falls to the
// nonce renewal, both fail-safe. A base with zero rows in one class
// re-folds each pass while a live nonce keeps being re-charged (the gap
// proof keeps releasing the row-less twin) — bounded by the nonce TTL,
// rollout-only, and strictly conservative.
func (r *AbsenceReconciler) foldedHeadsLive(ctx context.Context, base string, st *absencePassState, errs *boundedErrs) bool {
	for _, class := range []string{absenceClassLFS, absenceClassPacks} {
		if head, ok := r.resolveHead(ctx, base, class, st, errs); !ok || head.key == "" {
			return false
		}
	}
	return true
}

// foldIntoIncarnation grows one class's live incarnation to at least bytes,
// minting incarnation 1 when the class has none. Reports whether the grown
// total is proven durable in the ledger. bypassSkip forces the charge past
// the pass-level coverSkip memo even for a mint (the bounded endRange
// retry); same-key grows of an existing head always bypass.
func (r *AbsenceReconciler) foldIntoIncarnation(ctx context.Context, base, class string, bytes int64, bypassSkip bool, report *AbsenceReport, st *absencePassState, errs *boundedErrs) bool {
	assertf(bytes >= 0, "foldIntoIncarnation: negative total %d for %s/%s", bytes, base, class)
	head, ok := r.resolveHead(ctx, base, class, st, errs)
	if !ok {
		return false
	}
	target := head
	if target.key == "" {
		target = liveCover{key: absenceIncarnationKey(base, class, 1), n: 1}
	}
	if !r.chargeCover(ctx, base, target.key, bytes, head.key != "" || bypassSkip, report, st, errs) {
		return false
	}
	if bytes > target.bytes {
		target.bytes = bytes
	}
	r.liveCovers[class][base] = target
	return true
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
	coverBases := r.sortedCoverBases(tr.class)
	// Restore the pending range parked with the cursor (Codex rounds
	// 1+2, P1): resuming mid-range must neither read the first resumed
	// row as a range transition (the suffix's total would masquerade as
	// the whole range's and shrink covers protecting the skipped prefix)
	// nor restart the running total at zero (each resumed chunk would
	// under-accumulate and the cover could never grow to the true sum).
	// The parked tracker carries base, started, dirty, and the running
	// total, so ranges spanning any number of passes stay exact.
	*tr = rangeTracker{class: tr.class, base: cur.RangeBase, started: cur.RangeStarted, dirty: cur.RangeDirty, running: cur.RangeRunning}
	if tr.base == "" && !startedAtZero {
		// Legacy parked cursor without range state (rows written before
		// this fix): treat the pending range as unstarted, never exact.
		tr.base = baseOf(startRepo)
	}
	assertf(startedAtZero == (tr.base == "" && !tr.started), "rotationPhase(%s): cursor/range state disagree (%q vs %+v)", tr.class, startRepo, tr)
	// Park the tracker back into the durable state on every exit; the
	// end-of-pass saveState persists it.
	defer func() {
		cur.RangeBase, cur.RangeStarted, cur.RangeDirty, cur.RangeRunning = tr.base, tr.started, tr.dirty, tr.running
		tr.abandon()
	}()
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
			// range ends AT the keyspace end, and the empty page proves
			// no base after it holds any rows in this class.
			prevBase := tr.base
			r.endRange(ctx, tr, report, st, errs)
			r.releaseZeroRowCovers(ctx, tr.class, coverBases, prevBase, "", report, errs)
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
			// contiguously, so a base change ends the previous range —
			// and the keyset adjacency proves every base BETWEEN the two
			// holds no rows in this class at all.
			if b := baseOf(repos[i]); b != tr.base {
				prevBase := tr.base
				r.endRange(ctx, tr, report, st, errs)
				r.releaseZeroRowCovers(ctx, tr.class, coverBases, prevBase, b, report, errs)
				tr.base, tr.started, tr.dirty, tr.running = b, true, false, 0
			}
			charged += settle[i]()
			cur.AfterRepo, cur.AfterKey = repos[i], keys[i]
		}
	}
	return charged
}

// sortedCoverBases snapshots one class's harvested cover bases in key
// order — the rotation's O(log C) window index for releaseZeroRowCovers
// (Codex round 14, P1: scanning the whole harvest map at every base
// boundary made a pass O(C×R)). Covers folded in AFTER the snapshot are
// only ever for bases the rotation is currently processing (which have
// rows), so missing one merely defers its release to a later pass.
func (r *AbsenceReconciler) sortedCoverBases(class string) []string {
	bases := make([]string, 0, len(r.liveCovers[class]))
	for base := range r.liveCovers[class] {
		bases = append(bases, base)
	}
	sort.Strings(bases)
	return bases
}

// releaseZeroRowCovers releases this class's covers for bases the rotation
// just proved ROW-LESS: the keyset stream moved from base lo directly to
// base hi (""=keyspace edge), so any live cover base strictly between them
// has zero registered rows in this class — nothing left to protect, and no
// range evaluation would ever visit it (a base emptied by retirement or
// folds would otherwise pin its cover forever). The proof is the page
// adjacency itself, so it holds regardless of traversal or harvest
// completeness; covers this pass's harvest missed are released on a later
// pass. Deletion is single-writer-safe like every cover delete. bases is
// the phase's sorted harvest snapshot: only the (lo, hi) window is
// examined (binary search), so a traversal's total release work is
// O(R log C + C), not O(C×R); already-released entries skip via the map
// membership check.
func (r *AbsenceReconciler) releaseZeroRowCovers(ctx context.Context, class string, bases []string, lo, hi string, report *AbsenceReport, errs *boundedErrs) {
	if !r.quotaEnforced {
		return
	}
	for i := sort.SearchStrings(bases, lo); i < len(bases); i++ {
		base := bases[i]
		if base <= lo && lo != "" {
			continue // the boundary base itself: it has rows
		}
		if hi != "" && base >= hi {
			return
		}
		cover, live := r.liveCovers[class][base]
		if !live || cover.key == "" {
			continue
		}
		// Same warm-floor invalidation before deletion as endRange's
		// healed release: never delete a compensating reservation while
		// a deflated floor may still validate.
		if err := r.registry.BumpLFSAdoptionGen(ctx, base); err != nil {
			errs.add(fmt.Errorf("absence reconcile: invalidate floors before releasing cover %s: %w", cover.key, err))
			continue
		}
		if err := r.registry.DeleteAbsenceCover(ctx, cover.key); err != nil {
			errs.add(fmt.Errorf("absence reconcile: release row-less cover %s: %w", cover.key, err))
			continue
		}
		report.CoversReleased++
		delete(r.liveCovers[class], base)
	}
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
	if dirty {
		// The total may under-count: never shrink on it. But it can
		// still be UNCHARGED (Codex round 8, P1): a charge failure on
		// the range's last deficits leaves running accumulated but not
		// covered, and the next retry would otherwise wait a whole
		// class traversal. One bounded grow-only retry per range —
		// forced past the coverSkip memo — closes the transient case;
		// an under-counted running only grows the cover less than the
		// truth, never shrinks it.
		if running > 0 && !st.purged[base] {
			r.foldIntoIncarnation(ctx, base, class, running, true, report, st, errs)
		}
		return
	}
	if st.purged[base] || st.coverSkip[base] {
		return // the total may under-count: never shrink on it
	}
	head, ok := r.resolveHead(ctx, base, class, st, errs)
	if !ok || head.key == "" {
		return // no live cover (or unresolved): nothing to shrink
	}
	switch {
	case running == 0:
		// Fully healed (or rows retired/folded away): release the row —
		// but invalidate warm floors FIRST (Codex security round on
		// PR #132): a usage floor measured while the bytes were absent
		// can still be cached, and the cover is what compensated for its
		// deflation. Application restores bump the generation themselves,
		// but backend-side recovery (bytes reappearing out of band)
		// bypasses them — deleting the compensation while the deflated
		// floor still validates would let admission overshoot quota. If
		// the bump fails, retain the cover; the next clean range retries.
		if err := r.registry.BumpLFSAdoptionGen(ctx, base); err != nil {
			errs.add(fmt.Errorf("absence reconcile: invalidate floors before releasing cover %s: %w", head.key, err))
			return
		}
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
		if !r.chargeCover(ctx, base, next.key, running, false, report, st, errs) {
			return // retried next traversal; the old row keeps covering
		}
		// Deleting the larger row SHRINKS compensation by the healed
		// delta, so it needs a PROVEN floor invalidation first (Codex
		// round 12): chargeCover's own bump is best-effort — sufficient
		// for grows, where a stale floor is only over-compensated — but
		// a cached floor measured during the larger absence stays
		// deflated by bytes the smaller cover no longer compensates. On
		// bump failure both rows stay; the next renewal's superseded
		// cleanup retries under the same proven-bump rule.
		if err := r.registry.BumpLFSAdoptionGen(ctx, base); err != nil {
			errs.add(fmt.Errorf("absence reconcile: invalidate floors before rotating cover %s: %w", head.key, err))
			r.liveCovers[class][base] = next
			return
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
	row, found, err := r.registry.AbsenceCoverHead(ctx, absenceIncarnationPrefix(base, class), r.coverTTL)
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
		// Unaccounted deficit: never shrink on this range, and the
		// traversal is not clean (Codex round 4, security P2: a cover
		// failure must dirty the durable class cursor like a stat error,
		// or the traversal-completed bookkeeping over-reports health).
		tr.dirty, r.cursorFor(tr.class).Dirty = true, true
		return
	}
	if tr.running <= head.bytes {
		return // still within the live cover: renewal keeps it fresh
	}
	sameKey := head.key != ""
	if head.key == "" {
		// No LIVE incarnation (Codex round 6: expired rows are filtered
		// as dead). Minting can collide with a lingering expired row of
		// the same key: the upsert's GREATEST then transiently
		// resurrects its stale total — bounded like a crash replay (the
		// clean-range rotate right-sizes it, and the ledger's
		// expired-row cleanup removes lingering rows).
		head = liveCover{key: absenceIncarnationKey(tr.base, tr.class, 1), n: 1}
	}
	if !r.chargeCover(ctx, tr.base, head.key, tr.running, sameKey, report, st, errs) {
		tr.dirty, r.cursorFor(tr.class).Dirty = true, true
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
// deflated floors. Returns whether the cover row was charged. bypassSkip
// exempts this charge from the pass-level coverSkip memo, for exactly two
// bounded justifications: (a) a same-key re-charge of a row KNOWN to
// exist — renewal, fallback renewal, grow of a resolved head — succeeds
// where the denial that set the memo was a fresh mint hitting the
// active-row cap, because the ledger excludes the incoming key from that
// count (Codex round 7, P1); (b) the once-per-range endRange retry of an
// uncharged dirty total (Codex round 8, P1) — one forced attempt, not the
// budget-many the memo exists to prevent. Denials still SET the memo in
// every mode.
func (r *AbsenceReconciler) chargeCover(ctx context.Context, base, key string, size int64, bypassSkip bool, report *AbsenceReport, st *absencePassState, errs *boundedErrs) bool {
	if st.coverSkip[base] && !bypassSkip {
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
