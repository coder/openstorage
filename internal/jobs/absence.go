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
	// CASAbsenceCoverOwner rewrites one legacy nonce row's owner guarded
	// on the listed (owner, bytes) — the seam the folded sentinel goes
	// through (Codex rounds 15+18+19): a CAS miss proves a concurrent
	// re-charge, which must win.
	CASAbsenceCoverOwner(ctx context.Context, rawKey, expectedOwner string, expectedBytes int64, newOwner string) (bool, error)
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
	// LFSAdoptionGen reads one repo's shared adoption generation — the
	// mutation fence resumed ranges verify across park gaps (Codex
	// round 20 on PR #132): summed over the base group, any bump while
	// the cursor was parked taints the pending range's observation.
	LFSAdoptionGen(ctx context.Context, repo string) (int64, error)
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
	// left to expire by TTL once both classes proved their base within
	// one pass (foldRedundantLegacy): never deleted — a not-yet-upgraded
	// replica may still depend on them mid-roll.
	CoversDecaying int64 `json:"covers_decaying"`
	// RowsVerified counts registry rows re-observed by verifyAndJudge —
	// the fresh storage-visible evidence behind every verdict whose
	// trigger spanned a pass boundary (Codex round 21).
	RowsVerified int64 `json:"rows_verified"`
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
	// cursorStaleAge bounds how old a parked rotation cursor may be
	// before loadState resets it to origin (Codex round 17, security):
	// covers renew at pass START and cursors park at pass END, so a
	// cursor older than coverTTL-passTimeout means some cover MAY have
	// expired since the last renewal — the loss it covered is uncovered
	// and must not wait for the resumed traversal to wrap. A false reset
	// only costs a fresh traversal; a false resume is the bug, so the
	// bound stays strictly conservative (no floor; SQLite's ±1s age
	// over-read only resets earlier). Shrinkable in tests.
	cursorStaleAge time.Duration

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
	// legacyRows is this pass's harvest of live pre-#125 nonce covers
	// still awaiting retirement (Codex round 19): the renewal listing
	// records them as-listed, and the pass end folds exactly those whose
	// base holds BOTH class proofs (cleanBases). legacyBases indexes
	// their bases for the endRange verification relevance gate.
	legacyRows  []ref.AbsenceCoverRow
	legacyBases map[string]bool
	// cleanBases[class][base] records that class holds SAME-PASS
	// storage-visible evidence for base: a fence-continuous
	// start-to-end range observation with no stat/charge errors —
	// single-pass only (Codex rounds 19-20) — a row-less keyset gap,
	// or a fresh verifyAndJudge re-observation completed THIS pass.
	// Nothing persisted or resumed lands here directly: evidence older
	// than this pass spans a park gap where out-of-band byte loss has
	// no DB event, so no fence can vouch for it (Codex round 24).
	// Retirement needs both classes in one map, same pass.
	cleanBases map[string]map[string]bool
	// proofGens[base][class] is the surviving persisted candidacy set
	// loaded (and fence-validated) this pass. Entries are SCHEDULING
	// HINTS only (Codex round 24): a hint nominates its class for
	// commitment-time re-verification at fold, and is never itself
	// evidence — verifyAndJudge deletes it via markCleanBase when the
	// fresh re-observation succeeds. Also the retention source for
	// saveState when an incomplete renewal listing leaves legacyRows
	// non-authoritative.
	proofGens map[string]map[string]absenceProofEntry
	// verifyPool bounds the pass's total fresh-verification work (one
	// budget's worth, shared by every verifyAndJudge): verification is
	// per-verdict full-base work, so without a pool a pass completing
	// many small ranges could multiply its row budget. Fold-phase
	// verification may overshoot it by at most one base's combined
	// class rows (run-to-completion, Codex rounds 23+24): an exhausted
	// pool blocks further base starts but never abandons a started
	// fold candidate mid-verification.
	verifyPool int64
	// foldAttempted dedupes fold-time re-stats per (base, class) across
	// the pass's two foldRedundantLegacy invocations (round 25,
	// security): a class re-statted dirty by the pre-rotation fold must
	// not be re-statted again by the post-rotation one.
	foldAttempted map[string]bool
	// fenceArmed mirrors the durable admission-fence marker (Codex
	// round 26, security; see ref.ScanScopeAbsenceAdmissionFence):
	// while armed, finite quota-gated admissions fail closed because
	// TTL'd covers may have lapsed un-renewed. fenceLFS/fencePacks are
	// the marker's PERSISTED traversal snapshot at arming: the fence
	// clears only after a healthy error-free pass advances BOTH class
	// traversals past it — one complete recovery rotation re-covering
	// every registered-but-missing byte the lapse may have uncovered.
	fenceArmed           bool
	fenceLFS, fencePacks int64
	// touchFailed records that this pass's bulk cover renewal FAILED:
	// the rotation cursors must not be re-stamped by saveState (Codex
	// round 19, P1) — a cursor row's DB-clock freshness is the loadState
	// proof that the covers behind it were renewed within passTimeout of
	// the stamp, and a touch-failed pass would refresh the proof without
	// refreshing the covers. Skipped saves leave the previous rows
	// aging, so repeated failures trip the stale-age reset.
	touchFailed bool
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
	// retirement gates on per-base fence-continuous proofs, not this
	// flag — see foldRedundantLegacy).
	HasClean bool `json:"has_clean"`
	// The pending base range parks WITH the cursor (Codex round 2 on PR
	// #132, P1): a base whose rows span multiple phase allocations must
	// carry its partial deficit total across passes, or each resumed
	// chunk would restart at zero and the grow charge (always the
	// recomputed total, never additive) could never reach the true sum.
	// Parking the whole tracker keeps multi-pass ranges EXACT for GROW
	// charging; shrink, release, and legacy-retirement proofs
	// additionally require FENCE CONTINUITY (Codex rounds 19-20):
	// RangeGen snapshots the base group's summed adoption generation at
	// park time — after every bump the parking pass itself made — and
	// loadState re-reads it at the next pass start, before this job
	// makes any. The job is fleet-unique, so a mismatch proves a
	// FOREIGN mutation (restore, upload, adoption, delete) landed while
	// the cursor was parked, and the range is TAINTED: still exact for
	// grow charging, never a shrink/release/fold proof. A verified
	// chain of segments is as trustworthy as a single-pass observation
	// — without it, a base larger than one pass's class allowance could
	// NEVER prove itself and its legacy covers would be bulk-renewed
	// forever (round 20, P1). RangeGenOK distinguishes a real snapshot
	// from a park whose fence read failed (conservatively tainted).
	RangeBase    string `json:"range_base,omitempty"`
	RangeStarted bool   `json:"range_started,omitempty"`
	RangeDirty   bool   `json:"range_dirty,omitempty"`
	RangeRunning int64  `json:"range_running,omitempty"`
	RangeGen     int64  `json:"range_gen,omitempty"`
	RangeGenOK   bool   `json:"range_gen_ok,omitempty"`
	RangeTainted bool   `json:"range_tainted,omitempty"`
	// Quota records whether quota enforcement was ON when the cursor
	// parked (Codex round 17, security): quota-off passes advance the
	// cursors but skip all cover maintenance, so a cursor parked under
	// quota-off must not be resumed once enforcement turns on — losses
	// its traversal already passed were never covered and would wait a
	// full wrap. loadState resets such cursors to origin. Pre-upgrade
	// payloads lack the field (reads false): one fresh traversal after
	// the roll, conservative.
	Quota bool `json:"quota,omitempty"`
}

// rangeTracker follows one rotation's current base range within a pass.
type rangeTracker struct {
	class string
	base  string // "" = no pending range
	// started: the transition INTO base was observed, so the running
	// total spans the whole range once the range ends.
	started bool
	// dirty: a row of this range failed to settle or charge — the
	// running total may under-count, so shrink/release must not fire.
	dirty   bool
	running int64
	// resumed: the range was restored from a parked cursor, so segments
	// of its observation predate this pass (Codex rounds 19-21): the
	// accumulated total stays EXACT for grow charging, but verdicts
	// (markCleanBase, shrink, release) never come from it directly —
	// completion triggers verifyAndJudge, whose FRESH re-observation is
	// the verdict authority (out-of-band byte loss in a park gap is
	// invisible to any DB fence).
	resumed bool
	// tainted: the range's observation lost fence continuity across a
	// park gap (Codex rounds 19-20): a foreign mutation bumped the base
	// group's adoption generation while the cursor was parked, or the
	// fence could not be read/written. Still exact for grow charging,
	// but not even a verification trigger — the group demonstrably
	// changed, so the next traversal re-observes from scratch.
	tainted bool
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
	staleAge := coverTTL - passTimeout
	assertf(staleAge > 0, "NewAbsenceReconciler: non-positive cursor stale bound %v", staleAge)
	return &AbsenceReconciler{
		registry:       registry,
		blobs:          blobs,
		quotaEnforced:  quotaEnforced,
		budget:         budget,
		coverTTL:       coverTTL,
		logger:         logger,
		passTimeout:    passTimeout,
		cursorStaleAge: staleAge,
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
	r.legacyRows = nil
	r.legacyBases = make(map[string]bool)
	r.cleanBases = map[string]map[string]bool{
		absenceClassLFS:   make(map[string]bool),
		absenceClassPacks: make(map[string]bool),
	}
	r.proofGens = make(map[string]map[string]absenceProofEntry)
	r.verifyPool = r.budget
	r.foldAttempted = make(map[string]bool)
	r.touchFailed = false
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
			continue
		}
		if !r.quotaEnforced {
			continue // nothing consults covers: any resume point serves detection
		}
		// A resumed cursor must prove the covers it left behind are still
		// standing (Codex round 17, security): a fleet pause longer than
		// the cover TTL expires covers — the pass-start bulk touch
		// rightly refuses to resurrect them — so the losses they
		// compensated are uncovered, and resuming mid-keyspace would
		// leave them undiscovered until the traversal wraps while
		// admission spends the reopened headroom. Both staleness (the
		// cursor row's DB-clock age outliving the renewal guarantee) and
		// a quota-off parking (cover maintenance was skipped entirely)
		// reset the rotation to origin: strictly a fairness cost.
		reason := ""
		if !c.into.Quota {
			reason = "cursor parked while quota enforcement was off"
		} else if age, live, err := r.registry.JobScanStateAge(ctx, ref.ScanJobAbsence, c.scope); err != nil {
			errs.add(fmt.Errorf("absence reconcile: read %s cursor age: %w", c.scope, err))
			reason = "cursor age unreadable"
		} else if !live || age >= r.cursorStaleAge {
			reason = "cursor outlived the cover-renewal guarantee"
		}
		if reason != "" {
			*c.into = absenceCursorState{}
			r.logger.Warn("absence reconcile: restarting rotation from origin", "scope", c.scope, "reason", reason)
			continue
		}
		// Fence continuity for the parked range (Codex round 20, P1):
		// this runs BEFORE the pass makes any generation bump of its
		// own, and the park snapshot ran AFTER the previous pass's last
		// — the job is fleet-unique, so the window between them is
		// exactly the parked gap, and a mismatch proves a FOREIGN
		// mutation. A verified resume keeps the range proof-eligible:
		// a base larger than one pass's class allowance can only ever
		// complete resumed, and a blanket denial would bulk-renew its
		// legacy covers forever. Taint is sticky until the range ends.
		if c.into.RangeStarted && !c.into.RangeTainted {
			reason := ""
			if !c.into.RangeGenOK {
				reason = "range parked without a fence snapshot"
			} else if gen, err := r.rangeFence(ctx, c.into.RangeBase); err != nil {
				errs.add(fmt.Errorf("absence reconcile: read %s range fence: %w", c.scope, err))
				reason = "range fence unreadable"
			} else if gen != c.into.RangeGen {
				reason = "base group mutated while parked"
			}
			if reason != "" {
				c.into.RangeTainted = true
				r.logger.Info("absence reconcile: pending range tainted", "scope", c.scope, "base", c.into.RangeBase, "reason", reason)
			}
		}
	}
	if st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewal); err != nil {
		errs.add(fmt.Errorf("absence reconcile: read renewal cursor: %w", err))
	} else if ok {
		r.renewCursor, r.passes = st.Payload, st.Generation
	}
	// Mirror the durable admission-fence marker (Codex round 26,
	// security): its payload is the traversal snapshot at arming. An
	// unreadable or undecodable snapshot re-anchors at the CURRENT
	// persisted counters — requiring a fresh full rotation from here,
	// strictly the conservative direction.
	r.fenceArmed, r.fenceLFS, r.fencePacks = false, 0, 0
	if st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil {
		errs.add(fmt.Errorf("absence reconcile: read admission fence: %w", err))
	} else if ok {
		r.fenceArmed = true
		r.fenceLFS, r.fencePacks = r.lfs.Traversals, r.packs.Traversals
		var snap map[string]int64
		if err := json.Unmarshal([]byte(st.Payload), &snap); err == nil {
			r.fenceLFS, r.fencePacks = snap["lfs"], snap["packs"]
		}
	}
	r.loadProofs(ctx, errs)
}

// loadProofs restores persisted per-class fold candidacies (Codex rounds
// 21+24): a class's fenced completion for a base waits durably until the
// counterpart class completes — their rotations traverse independent
// populations and can stay out of phase indefinitely, so requiring both
// to land in one pass would leave both-class-large bases permanently
// unprovable. Each entry re-validates against the CURRENT group
// generation at pass start, before this pass makes any bump of its own:
// the persisted gen was snapshotted at a previous pass's end, so a
// mismatch proves a foreign mutation landed in between and the candidacy
// is dropped (re-proven by a later traversal). Surviving entries are
// scheduling hints ONLY — they never seed cleanBases, because the park
// gap they span is invisible to any DB fence (out-of-band byte loss has
// no DB event, and an old worker's nonce recharge lists as ordinary
// liveness): fold re-stats every hinted class fresh before retiring
// anything.
func (r *AbsenceReconciler) loadProofs(ctx context.Context, errs *boundedErrs) {
	if !r.quotaEnforced {
		return
	}
	st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceProofs)
	if err != nil {
		errs.add(fmt.Errorf("absence reconcile: read proof state: %w", err))
		return
	}
	if !ok || st.Payload == "" {
		return
	}
	var stored map[string]map[string]absenceProofEntry
	if err := json.Unmarshal([]byte(st.Payload), &stored); err != nil {
		r.logger.Warn("absence reconcile: discarding undecodable proof state", "err", err)
		return
	}
	for base, classes := range stored {
		if base == "" {
			continue
		}
		gen, err := r.rangeFence(ctx, base)
		if err != nil {
			// Unverifiable: dropped, re-proven by a later traversal.
			errs.add(fmt.Errorf("absence reconcile: validate proof fence for %s: %w", base, err))
			continue
		}
		for class, entry := range classes {
			if class != absenceClassLFS && class != absenceClassPacks {
				continue
			}
			if entry.Gen != gen {
				r.logger.Info("absence reconcile: persisted proof invalidated",
					"base", base, "class", class, "reason", "base group mutated since the proof")
				continue
			}
			if r.proofGens[base] == nil {
				r.proofGens[base] = make(map[string]absenceProofEntry)
			}
			r.proofGens[base][class] = absenceProofEntry{Gen: entry.Gen}
		}
	}
}

// absenceProofEntry is one persisted (base, class) fold candidacy: the
// group generation the class's evidence was fenced at. It schedules a
// commitment-time fresh re-verification and is never itself evidence
// (Codex round 24) — the fence bounds retention (a mutated group evicts
// the hint at load), not trust.
type absenceProofEntry struct {
	Gen int64 `json:"gen"`
}

// saveState parks the durable cursors on a bounded DETACHED context: the
// pass regularly ends by burning its own deadline, and losing the park
// replays this pass's segment (bounded transient over-cover: replayed
// totals are recomputed from scratch, never compounded).
func (r *AbsenceReconciler) saveState(ctx context.Context, errs *boundedErrs) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scanStateWriteTimeout)
	defer cancel()
	if r.touchFailed && r.quotaEnforced {
		// The bulk cover renewal failed this pass (Codex round 19, P1):
		// re-stamping the advanced rotation cursors would refresh the
		// loadState freshness proof ("the covers behind this cursor were
		// renewed within passTimeout of its stamp") without refreshing
		// the covers. Leave the previous rows aging instead — a repeat
		// failure trips the stale-age reset, and this pass's segment is
		// merely re-examined (idempotent same-key charges).
		r.logger.Warn("absence reconcile: bulk renewal failed; rotation cursors not re-stamped")
	} else {
		for _, c := range []struct {
			scope string
			from  *absenceCursorState
		}{{ref.ScanScopeAbsenceLFS, &r.lfs}, {ref.ScanScopeAbsencePacks, &r.packs}} {
			c.from.Quota = r.quotaEnforced // see absenceCursorState.Quota
			// Snapshot the fence WITH the park (Codex round 20): read
			// after every generation bump this pass made, so only a
			// foreign mutation inside the parked gap can move it. An
			// unreadable fence parks without one — the resume taints,
			// strictly a deferral.
			c.from.RangeGen, c.from.RangeGenOK = 0, false
			if c.from.RangeStarted && !c.from.RangeTainted && r.quotaEnforced {
				if gen, err := r.rangeFence(sctx, c.from.RangeBase); err != nil {
					errs.add(fmt.Errorf("absence reconcile: snapshot %s range fence: %w", c.scope, err))
				} else {
					c.from.RangeGen, c.from.RangeGenOK = gen, true
				}
			}
			payload, err := json.Marshal(c.from)
			assertf(err == nil, "absence reconcile: marshal %s cursor: %v", c.scope, err)
			if err := r.registry.PutJobScanState(sctx, ref.ScanJobAbsence, c.scope, string(payload), 0); err != nil {
				errs.add(fmt.Errorf("absence reconcile: park %s cursor: %w", c.scope, err))
			}
		}
	}
	if err := r.registry.PutJobScanState(sctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewal, r.renewCursor, r.passes); err != nil {
		errs.add(fmt.Errorf("absence reconcile: park renewal cursor: %w", err))
	}
	r.saveProofs(sctx, errs)
}

// absenceProofCap bounds the persisted half-proof payload: entries exist
// only for bases with live legacy nonces awaiting retirement, a
// population that shrinks to zero as folds proceed. Overflow drops the
// byte-largest bases (loudly) — strictly a retirement deferral.
const absenceProofCap = 512

// saveProofs persists per-base fold candidacies for the next pass (Codex
// rounds 21+24): every base with an un-folded legacy nonce recorded this
// pass carries forward each class holding either FRESH same-pass
// evidence or a surviving seeded hint. EVERY carried class is
// re-snapshotted under a fence read here, after every bump this pass
// made, so the next load-time validation covers exactly the parked gap —
// seeded hints included (Codex round 25): the pass's own renewal
// re-charge bumps the group generation every pass for bases holding live
// deterministic covers (chargeCover pairs each charge with a generation
// bump), so retaining a seeded entry's original fence would self-expire
// the candidacy after one carry and out-of-phase completions more than
// one pass apart could never converge. Re-snapshotting is sound because
// a candidacy is scheduling-only (round 24): commitment evidence always
// comes from the fold-time re-stat, never the fence. Candidacies only
// schedule commitment-time re-verification — fold re-stats every hinted
// class before retiring anything. Bases whose fold succeeded still
// persist (the entry evicts next pass with the listing); a CAS miss
// means a re-charge won and the re-stat repeats. Bases with no live
// nonce need no candidacy. When the renewal listing did not complete,
// this pass's legacyRows are not authoritative — previously persisted
// bases are retained rather than dropped blind.
func (r *AbsenceReconciler) saveProofs(sctx context.Context, errs *boundedErrs) {
	if !r.quotaEnforced {
		return
	}
	bases := make(map[string]bool, len(r.legacyRows))
	for i := range r.legacyRows {
		bases[r.legacyRows[i].Base] = true
	}
	if !r.renewalComplete {
		for base := range r.proofGens {
			bases[base] = true
		}
	}
	keep := make(map[string]map[string]absenceProofEntry)
	for base := range bases {
		carried := make([]string, 0, 2)
		for _, class := range []string{absenceClassLFS, absenceClassPacks} {
			if _, seeded := r.proofGens[base][class]; seeded || r.cleanBases[class][base] {
				carried = append(carried, class)
			}
		}
		if len(carried) == 0 {
			continue // nothing to carry
		}
		if len(keep) >= absenceProofCap {
			r.logger.Warn("absence reconcile: candidacy cap exceeded; excess entries dropped",
				"cap", absenceProofCap)
			break
		}
		gen, err := r.rangeFence(sctx, base)
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: snapshot proof fence for %s: %w", base, err))
			continue
		}
		entry := make(map[string]absenceProofEntry, len(carried))
		for _, class := range carried {
			entry[class] = absenceProofEntry{Gen: gen}
		}
		keep[base] = entry
	}
	payload := ""
	if len(keep) > 0 {
		b, err := json.Marshal(keep)
		assertf(err == nil, "absence reconcile: marshal proof state: %v", err)
		payload = string(b)
	}
	if err := r.registry.PutJobScanState(sctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceProofs, payload, 0); err != nil {
		errs.add(fmt.Errorf("absence reconcile: park proof state: %w", err))
	}
}

// armAdmissionFence writes the durable admission-fence marker when a
// bulk-renewal failure can no longer be proven harmless (Codex round 26,
// security): if the renewal-health watermark is absent or older than the
// cover-lapse safety bound (cursorStaleAge = coverTTL - passTimeout),
// live covers may lapse un-renewed before the next successful pass, and
// strict-TTL admission queries would silently under-count them —
// reopening the exact quota headroom the covers exist to close. The
// marker fails finite quota-gated admissions closed (see
// ReserveDecision.RenewalDegraded) until settleAdmissionFence proves a
// complete recovery rotation. Its payload snapshots the PERSISTED class
// traversal counters — frozen during failing passes because saveState
// suppresses cursor stamps — so wraps completed while renewal was still
// failing can never count as recovery. Writes run on a bounded detached
// context: the failure being reacted to may already have burned the pass
// deadline. An unreadable watermark age arms too — the failure evidence
// at hand outranks an unprovable freshness claim.
func (r *AbsenceReconciler) armAdmissionFence(ctx context.Context, errs *boundedErrs) {
	if !r.quotaEnforced {
		return
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scanStateWriteTimeout)
	defer cancel()
	age, ok, err := r.registry.JobScanStateAge(sctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth)
	if err != nil {
		errs.add(fmt.Errorf("absence reconcile: read renewal health: %w", err))
	} else if ok && age <= r.cursorStaleAge {
		return // renewed recently: no cover can lapse before the next pass
	}
	snap, mErr := json.Marshal(map[string]int64{"lfs": r.lfs.Traversals, "packs": r.packs.Traversals})
	assertf(mErr == nil, "absence reconcile: marshal fence snapshot: %v", mErr)
	if err := r.registry.PutJobScanState(sctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence, string(snap), 0); err != nil {
		errs.add(fmt.Errorf("absence reconcile: arm admission fence: %w", err))
		return
	}
	r.fenceArmed, r.fenceLFS, r.fencePacks = true, r.lfs.Traversals, r.packs.Traversals
	r.logger.Warn("absence reconcile: cover renewal degraded past the lapse safety bound — finite quota admissions fenced until a complete recovery rotation",
		"lfs_traversals", r.fenceLFS, "packs_traversals", r.fencePacks)
}

// settleAdmissionFence clears the admission fence after one healthy,
// error-free pass has advanced BOTH persisted class traversal counters
// past the marker's arming snapshot: a full origin-to-origin rotation of
// each registry re-covered every registered-but-missing byte the lapse
// may have uncovered (dirty ranges charge deficits as they go), so
// strict-TTL admission arithmetic is trustworthy again. Runs after
// saveState so the compared counters are the durably stamped ones.
func (r *AbsenceReconciler) settleAdmissionFence(ctx context.Context, errs *boundedErrs) {
	if !r.fenceArmed || !r.quotaEnforced || r.touchFailed || !errs.ok() {
		return
	}
	if r.lfs.Traversals <= r.fenceLFS || r.packs.Traversals <= r.fencePacks {
		return // recovery rotation not yet complete: stay fail-closed
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scanStateWriteTimeout)
	defer cancel()
	if err := r.registry.DeleteJobScanState(sctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil {
		errs.add(fmt.Errorf("absence reconcile: clear admission fence: %w", err))
		return
	}
	r.fenceArmed = false
	r.logger.Info("absence reconcile: recovery rotation complete — admission fence cleared")
}

// rangeFence reads the mutation fence for one base's pending range: the
// base group's summed shared adoption generation (the same group every
// cover charge pins). Every DB-visible mutation that could change a
// base's registered rows or their compensation — restores, uploads,
// adoptions, retirements, cover lifecycle — bumps a member's generation,
// so an unchanged sum across a park gap proves the group was quiescent
// while no segment was watching (Codex round 20 on PR #132).
// Out-of-band byte loss has no DB event and is invisible to ANY fence;
// it carries the same accepted residual as a loss striking mid-pass
// behind an in-pass cursor: rediscovered and re-covered by the next
// traversal.
func (r *AbsenceReconciler) rangeFence(ctx context.Context, base string) (int64, error) {
	assertf(base != "", "rangeFence: empty base")
	var sum int64
	for _, repo := range blob.QuotaRepoGroup(base) {
		gen, err := r.registry.LFSAdoptionGen(ctx, repo)
		if err != nil {
			return 0, err
		}
		assertf(gen >= 0, "rangeFence: negative generation %d for %s", gen, repo)
		sum += gen
	}
	return sum, nil
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
	// Hint-driven candidacies fold FIRST (Codex round 25, security):
	// the fold-time re-stat is pool-unbounded but wall-clock-bounded
	// and non-resumable — same-pass evidence only (round 24), so
	// durable partial verification progress is not an option — and
	// running it after the rotations would let them consume the pass
	// deadline every pass, starving a large base's re-stat forever.
	// Same-pass fresh completions fold in the second invocation below.
	r.foldRedundantLegacy(ctx, &report, st, &errs)
	half := remaining - remaining/2
	if r.passes%2 == 0 {
		charged := r.lfsPhase(ctx, half, &report, st, &errs)
		r.packPhase(ctx, remaining-charged, &report, st, &errs)
	} else {
		charged := r.packPhase(ctx, half, &report, st, &errs)
		r.lfsPhase(ctx, remaining-charged, &report, st, &errs)
	}
	// Retire legacy nonce covers whose base both rotations' proofs
	// completed THIS pass (Codex rounds 19-20) — after the phases, so
	// every proof is in. (Second invocation: the pass-level attempted
	// memo keeps the two from re-statting the same class twice.)
	r.foldRedundantLegacy(ctx, &report, st, &errs)
	r.passes++
	r.saveState(ctx, &errs)
	r.settleAdmissionFence(ctx, &errs)
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
		"rows_verified", report.RowsVerified,
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
// nonce-keyed covers from pre-#125 workers are RECORDED for retirement
// (recordLegacyCover, Codex rounds 4+19): each nonce row is itself the
// cover for whatever it aggregates, kept alive by the pass-start bulk
// touch until both rotations prove it redundant (foldRedundantLegacy)
// — an old replica can mint or re-charge nonces at
// any point of a mixed roll, and deleting or aging them out early could
// lapse them while the rotation is still far away. Renewal
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
		r.touchFailed = true // saveState must not re-stamp rotation cursors
		r.armAdmissionFence(ctx, errs)
		return 0
	}
	if r.quotaEnforced {
		// Stamp the DB-clock renewal-health watermark: the durable
		// proof that every live cover was re-TTL'd just now (Codex
		// round 26, security). A stamp failure leaves the watermark
		// aging, which only moves the fleet TOWARD the fail-closed
		// admission fence — the conservative direction.
		if err := r.registry.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth, "", 0); err != nil {
			errs.add(fmt.Errorf("absence reconcile: stamp renewal health: %w", err))
		}
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
				r.recordLegacyCover(row, report)
				continue
			}
			flushHeld(row)
			held, heldPrev = row, prevAfter
		}
	}
}

// recordLegacyCover notes one live pre-#125 nonce-keyed cover for this
// pass (Codex rounds 4, 17, 18, 19). The nonce row ITSELF is its cover:
// pre-#125 workers minted one random nonce per replica/pass aggregating
// BOTH classes' deficits, several live nonces can compensate disjoint
// deficits admission sums, and old replicas keep re-charging them mid-roll
// — so no bytes are ever transferred into the deterministic incarnations
// (rounds 17-18 tried GREATEST- and additive-transfer variants; every
// transfer scheme re-interpreted an old worker's periodic same-key TTL
// refresh, which overwrites owner, as new deficit and re-added it, round
// 19 P1). Instead the row stays live — the pass-start bulk touch renews
// every un-folded nonce (round 15) — until foldRedundantLegacy proves it
// redundant. Already-folded rows (owner sentinel intact, so no re-charge
// arrived since the proof) are left to decay by TTL.
func (r *AbsenceReconciler) recordLegacyCover(row *ref.AbsenceCoverRow, report *AbsenceReport) {
	if row.Folded {
		report.CoversDecaying++ // no re-charge since the fold: decaying
		return
	}
	r.legacyRows = append(r.legacyRows, *row)
	r.legacyBases[row.Base] = true
}

// foldRedundantLegacy retires this pass's recorded legacy nonce covers
// whose base BOTH rotations' proofs completed THIS PASS (Codex rounds
// 19-20): a clean fence-continuous start-to-end range observation
// recomputes the base's true current deficit from the registry itself and
// charges it into the deterministic head — ground truth that subsumes
// whatever the nonce once aggregated — and a row-less keyset gap proves a
// zero deficit the same way. Only then is the nonce redundant and safe to
// let decay. The fold is a CAS on the LISTED (owner, bytes): every
// old-replica re-charge overwrites owner and GREATEST-grows bytes, so a
// CAS miss proves a newer reliance arrived after the listing — it wins,
// the row stays live (bulk-touched), and a later pass re-proves.
// Both classes' evidence must be storage-visible from THIS pass (round
// 24: a persisted or resumed total spans a park gap no DB fence can
// vouch for) — a class completed in an earlier pass persists only a fold
// CANDIDACY (round 21, P1; see loadProofs) that is re-statted fresh here
// before anything retires, run-to-completion per started base so bases
// whose combined class rows outgrow the verification pool still converge
// (round 23, P1). Run invokes the fold twice per pass: once before the
// rotations so hint-driven re-stats get first claim on the pass wall
// clock (round 25, security), and once after so same-pass completions
// retire without an extra pass; r.foldAttempted dedupes across the two.
func (r *AbsenceReconciler) foldRedundantLegacy(ctx context.Context, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	attempted := r.foldAttempted
	started := false
	for i := range r.legacyRows {
		row := &r.legacyRows[i]
		// Candidacy: each class needs SAME-PASS storage-visible
		// evidence (cleanBases) or a persisted scheduling hint. A hint
		// is never evidence (Codex round 24): the park gap it spans is
		// invisible to any DB fence — out-of-band byte loss has no DB
		// event, and an old worker's nonce recharge before this pass's
		// listing lists as ordinary liveness the fold CAS would match.
		need := make([]string, 0, 2)
		eligible := true
		for _, class := range []string{absenceClassLFS, absenceClassPacks} {
			if r.cleanBases[class][row.Base] {
				continue
			}
			if _, hint := r.proofGens[row.Base][class]; !hint {
				eligible = false
				break
			}
			if !attempted[row.Base+"\x00"+class] {
				need = append(need, class)
			}
		}
		if !eligible {
			continue // no complete candidacy yet: stays live, retried later
		}
		if st.purged[row.Base] {
			continue // the purge pipeline owns the base's rows
		}
		// Commitment-time evidence: every hinted class is re-statted
		// NOW, all-or-nothing per base and run-to-completion once
		// started — an exhausted pool blocks further base STARTS but
		// never abandons a started candidate, so each pass completes
		// at least its first candidate even when the base's combined
		// class rows outgrow the pool (Codex round 23) and rotation-
		// triggered verification cannot starve the fold path. Success
		// lands in cleanBases (markCleanBase); a dirty or failed
		// re-stat leaves the nonce live for a later pass.
		if len(need) > 0 {
			if started && r.verifyPool <= 0 {
				continue // pool exhausted: deferred, candidacy persists
			}
			started = true
			for _, class := range need {
				attempted[row.Base+"\x00"+class] = true
				r.verifyAndJudge(ctx, row.Base, class, true, report, st, errs)
			}
		}
		if !r.cleanBases[absenceClassLFS][row.Base] || !r.cleanBases[absenceClassPacks][row.Base] {
			continue // re-stat dirty, failed, or deferred: stays live
		}
		cas, err := r.registry.CASAbsenceCoverOwner(ctx, row.RawKey, row.Owner, row.Bytes, ref.AbsenceFoldedOwner)
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: mark legacy cover folded %s: %w", row.Key, err))
			continue
		}
		if cas {
			report.CoversDecaying++
		}
	}
}

// markCleanBase records a completed FRESH clean proof for one
// (class, base) — see cleanBases. A fresh proof supersedes a persisted
// half-proof for the same pair: the trigger entry is dropped so fold
// consumers know the class needs no re-verification this pass.
func (r *AbsenceReconciler) markCleanBase(class, base string) {
	if base == "" {
		return
	}
	r.cleanBases[class][base] = true
	if m := r.proofGens[base]; m != nil {
		delete(m, class)
		if len(m) == 0 {
			delete(r.proofGens, base)
		}
	}
}

// foldIntoIncarnation grows one class's live incarnation to at least
// bytes (the ledger's GREATEST merge — an ABSOLUTE total, so replays are
// idempotent), minting incarnation 1 when the class has none. Reports
// whether the grown total is proven durable in the ledger. bypassSkip
// forces the charge past the pass-level coverSkip memo even for a mint
// (the bounded endRange retry); same-key grows of an existing head always
// bypass.
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
	legacyBases := r.sortedLegacyBases()
	// Restore the pending range parked with the cursor (Codex rounds
	// 1+2, P1): resuming mid-range must neither read the first resumed
	// row as a range transition (the suffix's total would masquerade as
	// the whole range's and shrink covers protecting the skipped prefix)
	// nor restart the running total at zero (each resumed chunk would
	// under-accumulate and the cover could never grow to the true sum).
	// The parked tracker carries base, started, dirty, and the running
	// total, so ranges spanning any number of passes stay exact — for
	// GROW charging unconditionally, and for shrink/release/fold proofs
	// exactly when loadState verified fence continuity across every park
	// gap (Codex rounds 19-20; see absenceCursorState.RangeGen).
	*tr = rangeTracker{class: tr.class, base: cur.RangeBase, started: cur.RangeStarted, dirty: cur.RangeDirty, running: cur.RangeRunning, resumed: cur.RangeStarted, tainted: cur.RangeTainted}
	if tr.base == "" && !startedAtZero {
		// Legacy parked cursor without range state (rows written before
		// this fix): treat the pending range as unstarted, never exact.
		tr.base = baseOf(startRepo)
	}
	assertf(startedAtZero == (tr.base == "" && !tr.started), "rotationPhase(%s): cursor/range state disagree (%q vs %+v)", tr.class, startRepo, tr)
	// Park the tracker back into the durable state on every exit; the
	// end-of-pass saveState persists it (and snapshots the fence).
	defer func() {
		cur.RangeBase, cur.RangeStarted, cur.RangeDirty, cur.RangeRunning, cur.RangeTainted = tr.base, tr.started, tr.dirty, tr.running, tr.tainted
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
			r.releaseZeroRowCovers(ctx, tr.class, coverBases, legacyBases, prevBase, "", report, errs)
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
				r.releaseZeroRowCovers(ctx, tr.class, coverBases, legacyBases, prevBase, b, report, errs)
				tr.base, tr.started, tr.dirty, tr.running, tr.resumed, tr.tainted = b, true, false, 0, false, false
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

// sortedLegacyBases snapshots the bases of this pass's recorded legacy
// nonce covers in key order — the gap window's index for same-pass
// row-less clean proofs (Codex round 19). Renewal runs before the
// rotations, so the snapshot is complete by phase start; rows the listing
// missed are simply proven on a later pass.
func (r *AbsenceReconciler) sortedLegacyBases() []string {
	seen := make(map[string]bool, len(r.legacyRows))
	bases := make([]string, 0, len(r.legacyRows))
	for i := range r.legacyRows {
		if base := r.legacyRows[i].Base; !seen[base] {
			seen[base] = true
			bases = append(bases, base)
		}
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
// adjacency itself — a DB-fresh query answered THIS pass, so it also
// counts as this class's same-pass clean proof (deficit zero) for legacy
// bases inside the gap (Codex round 19). Covers this pass's harvest
// missed are released on a later pass. Deletion is single-writer-safe
// like every cover delete. bases is the phase's sorted harvest snapshot
// and legacyBases the recorded nonce bases: only the (lo, hi) window is
// examined (binary search), so a traversal's total release work is
// O(R log C + C), not O(C×R); already-released entries skip via the map
// membership check.
func (r *AbsenceReconciler) releaseZeroRowCovers(ctx context.Context, class string, bases, legacyBases []string, lo, hi string, report *AbsenceReport, errs *boundedErrs) {
	if !r.quotaEnforced {
		return
	}
	for i := sort.SearchStrings(legacyBases, lo); i < len(legacyBases); i++ {
		base := legacyBases[i]
		if base <= lo && lo != "" {
			continue
		}
		if hi != "" && base >= hi {
			break
		}
		r.markCleanBase(class, base) // zero rows in this class: deficit 0
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

// classPage lists one keyset page of a registry class as (repo, cursorKey)
// pairs plus a settle callback per row bound to tr — the shared adapter
// under both the rotation phases and verifyAndJudge.
func (r *AbsenceReconciler) classPage(ctx context.Context, class string, tr *rangeTracker, afterRepo, afterKey string, limit int, report *AbsenceReport, st *absencePassState, errs *boundedErrs) ([]string, []string, []func() int64, error) {
	if class == absenceClassLFS {
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
			settle[i] = func() int64 { return r.settleLFSRow(ctx, row, tr, report, st, errs) }
		}
		return repos, keys, settle, nil
	}
	rows, err := r.registry.ListFleetPacks(ctx, afterRepo, afterKey, limit)
	if err != nil {
		return nil, nil, nil, err
	}
	repos := make([]string, len(rows))
	keys := make([]string, len(rows))
	settle := make([]func() int64, len(rows))
	for i := range rows {
		row := rows[i]
		repos[i], keys[i] = row.Repo, row.ID
		settle[i] = func() int64 { return r.settlePackRow(ctx, row, tr, report, st, errs) }
	}
	return repos, keys, settle, nil
}

// lfsPhase adapts the lfs_objects registry into rotationPhase.
func (r *AbsenceReconciler) lfsPhase(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) int64 {
	return r.rotationPhase(ctx, &r.lfs, &r.lfsRange, budget, report, st, errs,
		func(ctx context.Context, afterRepo, afterKey string, limit int) ([]string, []string, []func() int64, error) {
			return r.classPage(ctx, absenceClassLFS, &r.lfsRange, afterRepo, afterKey, limit, report, st, errs)
		})
}

// packPhase adapts the packs registry into rotationPhase.
func (r *AbsenceReconciler) packPhase(ctx context.Context, budget int64, report *AbsenceReport, st *absencePassState, errs *boundedErrs) int64 {
	return r.rotationPhase(ctx, &r.packs, &r.packsRange, budget, report, st, errs,
		func(ctx context.Context, afterRepo, afterID string, limit int) ([]string, []string, []func() int64, error) {
			return r.classPage(ctx, absenceClassPacks, &r.packsRange, afterRepo, afterID, limit, report, st, errs)
		})
}

// verifyAndJudge re-observes one (base, class) FRESH — a bounded scoped
// enumeration of the class's current registry rows with a storage stat
// per row, exactly the evidence a single-pass range observation yields —
// and applies the range verdicts (markCleanBase, shrink, release) from
// that observation alone (Codex round 21, security). Resumed completions
// and persisted fold candidacies are only TRIGGERS: their accumulated
// totals span park gaps where out-of-band byte loss is invisible to any
// DB fence, so compensation is never relaxed from them directly. The
// work draws on the shared per-pass verification pool (one budget's
// worth). Without runToCompletion (rotation-triggered), a class whose
// base outgrows the pool defers its verdicts LOUDLY — the operator sizes
// CODESTORAGE_ABSENCE_RECONCILE_BUDGET so the largest base's combined
// class rows fit — and its covers stay protectively oversized meanwhile,
// strictly the conservative direction. With runToCompletion (fold
// commitment, Codex rounds 23+24), the verification still charges the
// pool but never aborts on exhaustion: the caller bounds the overshoot
// to one base by gating STARTS on a non-exhausted pool.
func (r *AbsenceReconciler) verifyAndJudge(ctx context.Context, base, class string, runToCompletion bool, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	assertf(base != "" && (class == absenceClassLFS || class == absenceClassPacks),
		"verifyAndJudge(%q, %q)", base, class)
	vtr := rangeTracker{class: class, base: base, started: true}
	// Keyset start strictly before the base group's first row: members
	// are base plus suffixed namespaces, all sorting at or after
	// (base, "").
	afterRepo, afterKey := base, ""
	for {
		if ctx.Err() != nil {
			r.warnVerifyDeadline(runToCompletion, base, class)
			errs.add(ctx.Err())
			return
		}
		remaining := int64(absencePageSize)
		if !runToCompletion {
			if r.verifyPool <= 0 {
				r.logger.Warn("absence reconcile: verification pool exhausted — verdict deferred; raise CODESTORAGE_ABSENCE_RECONCILE_BUDGET above the largest base's combined class rows if persistent",
					"base", base, "class", class)
				return
			}
			remaining = r.verifyPool
		}
		repos, keys, settle, err := r.classPage(ctx, class, &vtr, afterRepo, afterKey, r.pageLimit(remaining), report, st, errs)
		if err != nil {
			errs.add(fmt.Errorf("absence reconcile: verify %s/%s: %w", base, class, err))
			return
		}
		if len(repos) == 0 {
			break // keyspace end: the whole base range was re-observed
		}
		done := false
		for i := range repos {
			if baseOf(repos[i]) != base {
				done = true
				break
			}
			if ctx.Err() != nil {
				r.warnVerifyDeadline(runToCompletion, base, class)
				errs.add(ctx.Err())
				return
			}
			if !runToCompletion && r.verifyPool <= 0 {
				r.logger.Warn("absence reconcile: verification pool exhausted — verdict deferred; raise CODESTORAGE_ABSENCE_RECONCILE_BUDGET above the largest base's combined class rows if persistent",
					"base", base, "class", class)
				return
			}
			charged := settle[i]()
			r.verifyPool -= charged
			report.RowsVerified += charged
			afterRepo, afterKey = repos[i], keys[i]
		}
		if done {
			break
		}
	}
	// The fresh observation is a complete, error-free-or-dirty range in
	// its own right: the normal fresh-range verdict path judges it.
	r.endRange(ctx, &vtr, report, st, errs)
}

// warnVerifyDeadline reports a fold-commitment re-stat killed by the
// pass deadline (Codex round 25, security): the candidacy survives and
// the next pass's PRE-rotation fold retries it with first claim on the
// wall clock, but a base whose single-class re-stat outgrows one whole
// pass can never produce the same-pass evidence retirement requires —
// the operator must raise the pass wall clock (a quarter of the cover
// TTL, capped at an hour; see NewAbsenceReconciler). Covers stay
// protectively oversized meanwhile.
func (r *AbsenceReconciler) warnVerifyDeadline(runToCompletion bool, base, class string) {
	if !runToCompletion {
		return
	}
	r.logger.Warn("absence reconcile: fold verification hit the pass deadline — retried with wall-clock priority next pass; raise CODESTORAGE_PACK_RESERVATION_TTL (the pass wall clock is a quarter of it, capped at an hour) so one pass can re-stat the largest base if persistent",
		"base", base, "class", class)
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
	tr.base, tr.started, tr.dirty, tr.running, tr.resumed, tr.tainted = "", false, false, 0, false, false
}

// endRange completes the pending base range: when the WHOLE range was
// observed within this pass with no errors, the running total is exact and
// the cover lifecycle may shrink or release the base's aggregate.
func (r *AbsenceReconciler) endRange(ctx context.Context, tr *rangeTracker, report *AbsenceReport, st *absencePassState, errs *boundedErrs) {
	base, class := tr.base, tr.class
	started, dirty, running, resumed, tainted := tr.started, tr.dirty, tr.running, tr.resumed, tr.tainted
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
	if tainted {
		// The observation lost fence continuity across a park gap
		// (Codex rounds 19-20): a foreign mutation changed the base
		// group while no segment was watching — exact for the grow
		// charges already made, but not even a verification trigger.
		// The next traversal re-observes from scratch.
		return
	}
	if st.purged[base] || st.coverSkip[base] {
		return // the total may under-count: never shrink on it
	}
	if resumed {
		// A fence-continuous multi-pass completion (Codex rounds
		// 20-21): the DB fence proves the group's ROWS did not change
		// across the park gaps, but out-of-band byte loss has no DB
		// event, so the accumulated total must not relax compensation
		// directly. Completion is the TRIGGER; the verdict authority is
		// a bounded fresh re-observation of the whole base range —
		// spent only when something consumes the verdict (a live head
		// that could shrink, or a legacy nonce awaiting retirement):
		// healthy bases completing resumed ranges are common and must
		// not drain the verification pool.
		head, ok := r.resolveHead(ctx, base, class, st, errs)
		if (ok && head.key != "") || r.legacyBases[base] {
			r.verifyAndJudge(ctx, base, class, false, report, st, errs)
		}
		return
	}
	// A clean single-pass start-to-end observation: the base's true
	// current deficit was recomputed from the registry and storage just
	// now and is charged (chargeDeficit grows the head incrementally; a
	// charge failure dirties the range) — this class's ground-truth
	// proof for legacy retirement.
	r.markCleanBase(class, base)
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
