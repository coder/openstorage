package jobs

// Unreferenced-LFS-object reclamation (issue #73): a bounded, resumable
// background job that walks each LFS-holding repository's reachable pointer
// files, diffs the referenced OIDs against the registered lfs_objects rows,
// and retires rows that stayed unreferenced for a full grace window —
// unwedging the per-repo object-count cap a junk uploader exhausted. Row
// retirement is the ONLY thing this job does: the now-rowless blobs (both
// key spellings) become ordinary orphans the existing sweeper reaps under
// its own delete gates (ADR-41), and #104's download-time restore is
// registered-row-gated, so a retired row is not restorable through
// download paths.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/coder/open-storage/internal/blob"
	"github.com/coder/open-storage/internal/lfsscan"
	"github.com/coder/open-storage/internal/ref"
)

// LFSReclaimRegistry is the ref-store surface the reclaimer drives — carried
// by both concrete backends (like RepoPurgeRegistry), deliberately not on
// ref.RefStore: only this job may mint retirement verdicts.
type LFSReclaimRegistry interface {
	// LFSReposWithObjects pages base repos holding registered LFS objects
	// (keyset on repo, ascending) — the scan's work queue.
	LFSReposWithObjects(ctx context.Context, after string, limit int) ([]string, error)
	// ListRepoLFSObjects pages repo's registered rows (keyset on key).
	ListRepoLFSObjects(ctx context.Context, repo, after string, limit int) ([]ref.LFSObjectRow, error)
	// MarkLFSObjectsUnreferenced stamps unreferenced_since=now() on
	// unmarked rows; an existing mark keeps its original clock.
	MarkLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error)
	// ClearLFSObjectsUnreferenced removes marks from re-referenced rows.
	ClearLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error)
	// RetireUnreferencedLFSObject is the guarded verdict — see the ref
	// store contract (generation fence, purge fence, in-transaction grace
	// recheck, reap-watermark freshness, deferred refund bucketing).
	RetireUnreferencedLFSObject(ctx context.Context, repo, key string, pinnedGens map[string]int64, grace, reapMaxAge time.Duration) (bool, error)
	// SettleLFSRefunds releases a base's pending retirement refunds
	// covered by a completed destructive sweep (Codex round 33 on PR
	// #132) — see the ref store contract.
	SettleLFSRefunds(ctx context.Context, repo string) (ref.SettledLFSRefunds, error)
	// ScanStateStore persists the rotation cursor, the per-repo
	// row-settlement cursors, and serves the sweeper's destructive-reap
	// watermark (issues #120/#121): River may run each pass on any
	// replica, so scan progress must not live in process memory.
	ref.ScanStateStore
	// IsRepoPurged skips purge-fenced repos (their rows die with the purge).
	IsRepoPurged(ctx context.Context, base string) (bool, error)
	// GetGeneration pins each namespace-group member's ref generation
	// before hydration; retirement re-checks it.
	GetGeneration(ctx context.Context, repo string) (int64, error)
	// CountRefs gates hydration — a member with no refs has nothing
	// reachable and needs no cache — and prices ref fanout WITHOUT
	// materializing the refs (Codex security round 7): the reclaimer
	// never needs the ref values, and a counting query keeps a
	// million-ref repo from ballooning the worker's heap before the
	// budget can reject it. The count stops at limit (Codex round 12) so
	// even the index walk stays budget-bounded.
	CountRefs(ctx context.Context, repo string, limit int64) (int64, error)
}

// HydrateFunc materializes one namespace repo's git cache from the stores
// (gitread.Materialize in production: hydrate packs, sync refs from the DB
// authority) and returns its bare-repo path.
type HydrateFunc func(ctx context.Context, repo, defaultBranch string) (repoPath string, err error)

// LFSWalkFunc computes the reachable pointer OIDs of the cache repo at
// repoPath (lfsscan.ReachableLFSOIDs in production). It must return an
// error wrapping lfsscan.ErrLFSScanBudget when the walk exceeds budget.
type LFSWalkFunc func(ctx context.Context, repoPath string, budget int64) (oids map[string]struct{}, walked int64, err error)

// DefaultBranchFunc resolves a base repo's default branch from the metadata
// store; ok=false skips the repo (deleted or foreign — never an error, and
// never a retirement).
type DefaultBranchFunc func(ctx context.Context, base string) (branch string, ok bool, err error)

// LFSReclaimReport is one run's outcome, logged as structured fields (the
// dogfooding/observability record, mirroring SweepReport).
type LFSReclaimReport struct {
	ReposScanned    int   `json:"repos_scanned"`
	ReposSkipped    int   `json:"repos_skipped"`
	ReposOverBudget int   `json:"repos_over_budget"`
	ObjectsWalked   int64 `json:"objects_walked"`
	RowsExamined    int64 `json:"rows_examined"`
	RowsReferenced  int64 `json:"rows_referenced"`
	RowsMarked      int64 `json:"rows_marked"`
	RowsCleared     int64 `json:"rows_cleared"`
	RowsInGrace     int64 `json:"rows_in_grace"`
	// RowsEligible counts grace-expired marked rows the run would retire;
	// with deletes disabled (dry run) nothing else happens to them.
	RowsEligible int64 `json:"rows_eligible"`
	RowsRetired  int64 `json:"rows_retired"`
	// RowsSpared counts guarded verdicts that declined at commit time
	// (generation drift, concurrent touch/re-registration, purge fence).
	RowsSpared   int64 `json:"rows_spared"`
	BytesRetired int64 `json:"bytes_retired"`
	DryRun       bool  `json:"dry_run"`
	// ReapWatermarkStale reports the pass ran report-only because the
	// sweeper's last completed destructive LFS pass is older than the
	// grace window (or has never happened) — refunds gate on OBSERVED
	// reap completion, not configured cadence (issue #120).
	ReapWatermarkStale bool `json:"reap_watermark_stale"`
	// RowsDeferredReapStale counts grace-expired eligible rows whose
	// retirement was deferred by the stale watermark; they retire on a
	// later pass once destructive sweeps demonstrably complete again.
	RowsDeferredReapStale int64 `json:"rows_deferred_reap_stale"`
	// RowsDeferredLegacyUnarmed counts grace-expired eligible
	// LEGACY-SPELLED rows deferred because legacy reaping is not armed
	// end-to-end — this replica's OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY is
	// off, or the fresh watermark does not certify a legacy-armed sweep
	// (Codex round 17 on PR #132). Their blobs would never be physically
	// reaped, so refunding their slots would grow storage past the cap.
	RowsDeferredLegacyUnarmed int64 `json:"rows_deferred_legacy_unarmed"`
	// RefundRowsSettled/RefundBytesSettled count deferred retirement
	// refunds released this pass: a destructive sweep with a strictly
	// greater epoch completed since the rows were retired, proving their
	// orphaned blobs were visible to a full clean reap (Codex round 33
	// on PR #132).
	RefundRowsSettled  int64 `json:"refund_rows_settled"`
	RefundBytesSettled int64 `json:"refund_bytes_settled"`
	// RefundBucketsPending counts buckets still awaiting a covering
	// completion at their settlement attempts this pass.
	RefundBucketsPending int `json:"refund_buckets_pending"`
}

const (
	// lfsReclaimRepoBatch bounds one work-queue page.
	lfsReclaimRepoBatch = 100
	// lfsReclaimRowPage bounds one lfs_objects page (and therefore one
	// mark/clear statement's key list).
	lfsReclaimRowPage = 1_000
	// DefaultLFSReclaimObjectBudget bounds the git objects one run may
	// list across all reachability walks (OPENSTORAGE_LFS_GC_OBJECT_BUDGET
	// overrides).
	DefaultLFSReclaimObjectBudget = int64(1_000_000)
	// DefaultLFSReclaimGrace is how long a row must stay continuously
	// unreferenced before retirement (OPENSTORAGE_LFS_GC_GRACE overrides).
	DefaultLFSReclaimGrace = 24 * time.Hour
	// lfsReclaimHydrateTimeout bounds one namespace member's cache
	// hydration (Codex round 7): on a cold replica, Materialize downloads
	// and indexes every registered pack before the budgeted walk can even
	// start, and a writer can fill the pack quota with a few huge packs.
	// A fixed deadline — the repack budgets' compiled-in pattern — defers
	// the repo with an error instead of letting one hydration monopolize
	// the pass. Retried next rotation; a warm cache hydrates in seconds.
	lfsReclaimHydrateTimeout = 10 * time.Minute
	// lfsReclaimWalkTimeout bounds one namespace member's reachability
	// walk (Codex security round 18): the work-unit budget meters objects
	// only as cat-file reports them, but rev-list must INFLATE a tree
	// before its children can stream — a single highly compressible giant
	// tree burns its whole expansion before cancellation can land. Tree
	// size is already bounded by the push-decode caps, so the residual is
	// bounded CPU per pass; the wall-clock deadline hard-caps it like the
	// hydration deadline, deferring the repo with an error.
	lfsReclaimWalkTimeout = 10 * time.Minute
	// lfsReclaimPassTimeout bounds one whole reclamation pass (Codex
	// round 19): the per-member deadlines reset for every repository, and
	// a deadline-expired walk charges almost no budget — many wedged
	// repositories could otherwise hold one pass for ten minutes each.
	// The pass cap ends the run with an error; the rotation cursor
	// resumes strictly after the last attempted repository, so successive
	// bounded passes still make fair progress across the fleet.
	lfsReclaimPassTimeout = time.Hour
	// scanStateWriteTimeout bounds the detached end-of-pass cursor park:
	// pass contexts regularly end by expiring, and the park must not
	// inherit that death, nor hang unbounded.
	scanStateWriteTimeout = 10 * time.Second
	// reapWatermarkMinFreshness floors the observed-coupling staleness
	// bound (issue #120). Production graces are ≥ 1h (the boot interlock
	// floors them at the quarantine retention), so the bound is simply
	// the grace there; the floor only matters for sub-minute test graces,
	// where SQLite's whole-second watermark stamps would otherwise demote
	// a just-completed sweep spuriously.
	reapWatermarkMinFreshness = time.Minute
)

// reapMaxAge is the watermark staleness bound refunds gate on: refunds
// flow only while a destructive LFS pass completed within roughly one
// grace window (see reapWatermarkMinFreshness for the floor).
func (r *LFSReclaimer) reapMaxAge() time.Duration {
	if r.grace > reapWatermarkMinFreshness {
		return r.grace
	}
	return reapWatermarkMinFreshness
}

// LFSReclaimer runs the reclamation scan. Construct with NewLFSReclaimer;
// Run executes one bounded pass on the schedule's cadence.
type LFSReclaimer struct {
	registry      LFSReclaimRegistry
	hydrate       HydrateFunc
	walk          LFSWalkFunc
	defaultBranch DefaultBranchFunc
	grace         time.Duration
	objectBudget  int64
	deleteEnabled bool
	// legacyDelete mirrors the fleet's OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY
	// arming (Codex round 17 on PR #132): legacy-spelled rows may only be
	// retired while THIS replica's config arms legacy reaping AND the
	// fresh watermark certifies a legacy-armed sweep — a fresh legacy
	// watermark from before the flag was switched off must not keep legacy
	// refunds flowing for its freshness window.
	legacyDelete   bool
	logger         *slog.Logger
	now            func() time.Time // injectable clock for tests
	hydrateTimeout time.Duration    // per-member cap, shrinkable in tests
	walkTimeout    time.Duration    // per-member walk cap, shrinkable in tests
	passTimeout    time.Duration    // whole-pass cap, shrinkable in tests
	// reapStale is the pass-local advisory demotion (issue #120): set at
	// pass start when the sweeper's destructive-reap watermark is stale,
	// so settlement skips retire transactions that would only refuse.
	// The authoritative gate is RetireUnreferencedLFSObject's own
	// in-transaction watermark recheck.
	reapStale bool
	// legacyReapArmed is the pass-local advisory twin for legacy-spelled
	// rows: the watermark's payload said the certified sweep had the
	// legacy delete class armed. Config (legacyDelete) is process-static
	// and checked directly at each row; the DB-racing payload half is
	// re-checked inside the retire transaction.
	legacyReapArmed bool
	// Scan progress — the rotation cursor (last base repo a run
	// attempted, resumed strictly after; the purger's rotation, issue
	// #52) and the per-repo row-settlement cursors (Codex round 8:
	// resume past a non-retiring row prefix, or the tail starves) — is
	// FLEET-PERSISTENT via the registry's job_scan_state rows (issue
	// #121): River may run each pass on any replica, and in-process
	// cursors made a repo whose settlement spans multiple budget-limited
	// passes re-examine its prefix forever under worker turnover. Row
	// cursors carry the pinned-generation fence and are invalidated by
	// any generation bump (fresh scan, never a resume across a fence).
}

// NewLFSReclaimer wires the reclamation job. legacyDelete mirrors the
// sweeper's OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY arming — see the field.
func NewLFSReclaimer(registry LFSReclaimRegistry, hydrate HydrateFunc, walk LFSWalkFunc, defaultBranch DefaultBranchFunc, grace time.Duration, objectBudget int64, deleteEnabled, legacyDelete bool, logger *slog.Logger) *LFSReclaimer {
	assertf(registry != nil, "NewLFSReclaimer: nil registry")
	assertf(hydrate != nil, "NewLFSReclaimer: nil hydrate seam")
	assertf(walk != nil, "NewLFSReclaimer: nil walk seam")
	assertf(defaultBranch != nil, "NewLFSReclaimer: nil default-branch seam")
	assertf(grace > 0, "NewLFSReclaimer: non-positive grace %v", grace)
	assertf(objectBudget > 0, "NewLFSReclaimer: non-positive object budget %d", objectBudget)
	assertf(logger != nil, "NewLFSReclaimer: nil logger")
	return &LFSReclaimer{
		registry:       registry,
		hydrate:        hydrate,
		walk:           walk,
		defaultBranch:  defaultBranch,
		grace:          grace,
		objectBudget:   objectBudget,
		deleteEnabled:  deleteEnabled,
		legacyDelete:   legacyDelete,
		logger:         logger,
		now:            time.Now,
		hydrateTimeout: lfsReclaimHydrateTimeout,
		walkTimeout:    lfsReclaimWalkTimeout,
		passTimeout:    lfsReclaimPassTimeout,
	}
}

// lfsRefundScopePage bounds one pass's vanished-base refund listing.
const lfsRefundScopePage = 512

// refundSettleTimeout bounds the detached end-of-pass vanished-refund
// settlement (Codex round 34): generous for one scope page of per-base
// point transactions, small next to the pass timeout, and independent of
// the pass deadline the main rotation may already have burned. An
// unfinished settle only defers those refunds to the next pass.
const refundSettleTimeout = time.Minute

// settleRefunds settles one base's matured refund buckets into the
// report. A settlement failure only defers refunds — logged, never fatal
// to the pass.
func (r *LFSReclaimer) settleRefunds(ctx context.Context, base string, report *LFSReclaimReport) {
	settled, err := r.registry.SettleLFSRefunds(ctx, base)
	if err != nil {
		r.logger.Warn("lfs reclaim: settling refund buckets failed; refunds deferred", "repo", base, "err", err)
		return
	}
	report.RefundRowsSettled += settled.Rows
	report.RefundBytesSettled += settled.Bytes
	report.RefundBucketsPending += settled.Pending
}

// settleVanishedRefunds pages the refund bucket keyspace behind a durable
// rotation cursor and settles bases the repo rotation did not visit this
// pass (Codex round 33): a base whose last row retired vanishes from
// LFSReposWithObjects while its pending refunds still hold quota.
func (r *LFSReclaimer) settleVanishedRefunds(ctx context.Context, scanned map[string]bool, report *LFSReclaimReport, errs *boundedErrs) {
	after := ""
	if st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRefundRotation); err != nil {
		errs.add(fmt.Errorf("lfs reclaim: read refund rotation cursor: %w", err))
		return
	} else if ok {
		after = st.Payload
	}
	scopes, err := r.registry.ListJobScanStateScopes(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRefundRoot, after, lfsRefundScopePage)
	if err != nil {
		errs.add(fmt.Errorf("lfs reclaim: list refund buckets: %w", err))
		return
	}
	next := "" // a short page drained the tail: wrap to origin
	if len(scopes) == lfsRefundScopePage {
		next = scopes[len(scopes)-1]
	}
	done := make(map[string]bool)
	for _, scope := range scopes {
		rest := strings.TrimPrefix(scope, ref.ScanScopeReclaimRefundRoot)
		// The epoch suffix is the LAST "/e<digits>" segment: base names
		// are opaque and may themselves contain "/e".
		idx := strings.LastIndex(rest, "/e")
		if idx <= 0 {
			r.logger.Warn("lfs reclaim: skipping unparseable refund bucket scope", "scope", scope)
			continue
		}
		base := rest[:idx]
		if scanned[base] || done[base] {
			continue // already settled by this pass's repo visit
		}
		done[base] = true
		r.settleRefunds(ctx, base, report)
	}
	parkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scanStateWriteTimeout)
	defer cancel()
	if err := r.registry.PutJobScanState(parkCtx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRefundRotation, next, 0); err != nil {
		errs.add(fmt.Errorf("lfs reclaim: park refund rotation cursor: %w", err))
	}
}

// gensFence folds pinned namespace-group generations into the row-cursor
// invalidation fence. Generations only ever increment (schema triggers), so
// the SUM is strictly monotone under any member's bump: equality proves no
// member's refs moved since the cursor was parked, and any drift restarts
// settlement from the top of the keyspace.
func gensFence(gens map[string]int64) int64 {
	assertf(len(gens) > 0, "gensFence: no pinned generations")
	var sum int64
	for _, gen := range gens {
		assertf(gen >= 0, "gensFence: negative generation %d", gen)
		sum += gen
	}
	return sum
}

// Run executes one bounded reclamation pass: page LFS-holding repos from
// the rotating cursor, scan each within the remaining object budget, and
// mark/clear/retire its rows. Per-repo failures defer that repo to a later
// run without stopping the pass; they are joined into the returned error
// so the job backend records them.
func (r *LFSReclaimer) Run(ctx context.Context) (LFSReclaimReport, error) {
	report := LFSReclaimReport{DryRun: !r.deleteEnabled}
	// Whole-pass wall-clock cap (Codex round 19): per-member deadlines
	// reset per repository and deadline-expired walks charge almost no
	// budget, so time — not just work units — must bound the pass.
	assertf(r.passTimeout > 0, "Run: non-positive pass timeout %v", r.passTimeout)
	ctx, cancelPass := context.WithTimeout(ctx, r.passTimeout)
	defer cancelPass()
	budget := r.objectBudget
	var errs boundedErrs
	// Observed reap coupling, advisory half (issue #120): with deletes
	// enabled, demote the pass to report-only when the sweeper's last
	// completed destructive LFS pass is older than the grace window (or
	// missing — a fresh deployment reports until its first clean
	// destructive sweep). The demotion only skips retire transactions
	// that would refuse anyway; the authoritative, race-free gate is the
	// retire transaction's own watermark recheck on the database clock.
	r.reapStale, r.legacyReapArmed = false, false
	if r.deleteEnabled {
		// Freshness is measured entirely on the DATABASE clock (Codex
		// round 10, P2): the watermark's updated_at is DB-stamped, so
		// comparing it against this replica's time.Now would let a
		// clock skewed ahead by more than the bound permanently demote
		// every pass here while the transactional gate (both ends DB
		// clock) would proceed.
		age, ok, err := r.registry.JobScanStateAge(ctx, ref.ScanJobSweep, ref.ScanScopeLFSDestructiveReap)
		switch {
		case err != nil:
			// Fail toward report-only: the in-transaction gate would
			// refuse refunds on the same broken table anyway.
			errs.add(fmt.Errorf("lfs reclaim: read reap watermark: %w", err))
			r.reapStale = true
		case !ok || age > r.reapMaxAge():
			r.reapStale = true
		}
		if r.reapStale {
			report.ReapWatermarkStale = true
			r.logger.Warn("lfs reclaim: no destructive LFS sweep completed within the grace window; this pass is report-only and slot refunds are deferred",
				"bound", r.reapMaxAge())
		} else if r.legacyDelete {
			// Legacy-spelled rows additionally need the certified
			// sweep to have reaped legacy keys (Codex round 17): the
			// payload half is advisory here — read failure defers
			// conservatively — and re-checked inside the retire
			// transaction.
			if st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobSweep, ref.ScanScopeLFSDestructiveReap); err != nil {
				errs.add(fmt.Errorf("lfs reclaim: read reap watermark payload: %w", err))
			} else if ok {
				r.legacyReapArmed = ref.DecodeReapWatermark(st.Payload).Legacy
			}
		}
	}
	// Resume the fleet-persistent rotation (issue #121); a lost or
	// unreadable cursor restarts the rotation from the top, which only
	// costs fairness, never correctness.
	cursor := ""
	if st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRotation); err != nil {
		errs.add(fmt.Errorf("lfs reclaim: read rotation cursor: %w", err))
	} else if ok {
		cursor = st.Payload
	}
	wrapped := cursor == ""
	scanned := make(map[string]bool)
loop:
	for budget > 0 {
		repos, err := r.registry.LFSReposWithObjects(ctx, cursor, lfsReclaimRepoBatch)
		if err != nil {
			errs.add(fmt.Errorf("lfs reclaim: list repos: %w", err))
			break
		}
		if len(repos) == 0 {
			if wrapped {
				break // the whole eligible set was offered this run
			}
			// Rotation wrapped past the end: resume from the first repo.
			cursor, wrapped = "", true
			continue
		}
		for _, base := range repos {
			if ctx.Err() != nil {
				errs.add(ctx.Err())
				break loop
			}
			if budget <= 0 {
				break loop
			}
			if scanned[base] {
				break loop // wrapped around to an already-scanned repo
			}
			scanned[base] = true
			hadFullBudget := budget == r.objectBudget
			prevCursor := cursor
			cursor = base // advance the rotation before attempting
			charged, err := r.reclaimRepo(ctx, base, budget, &report)
			if charged == 0 {
				// Every attempted repository costs at least one unit
				// (Codex round 5): soft-deleted repos keep their LFS rows
				// but lose their metadata, and an unbounded population of
				// such zero-work skips — several queries each — would
				// otherwise never drain the budget.
				charged = 1
			}
			budget -= charged
			switch {
			case errors.Is(err, lfsscan.ErrLFSScanBudget):
				if hadFullBudget {
					// The repo alone exceeds a full run budget: count and
					// skip it (rotation moved past it) and say so — only
					// a raised budget can ever scan it.
					report.ReposOverBudget++
					r.logger.Warn("lfs reclaim: repository exceeds the full run budget; raise OPENSTORAGE_LFS_GC_OBJECT_BUDGET to scan and settle it in one pass",
						"repo", base, "budget", r.objectBudget)
					continue
				}
				// Ran out of THIS run's remainder — not the repo's fault:
				// hold the cursor so the next run retries it first with a
				// full budget.
				cursor = prevCursor
				break loop
			case err != nil:
				errs.add(fmt.Errorf("lfs reclaim %s: %w", base, err))
			}
		}
	}
	// Settle refund buckets of bases that vanished from the work queue
	// (Codex round 33): a base whose LAST row retired never reappears in
	// LFSReposWithObjects, so its pending refunds would otherwise wait
	// forever. One bounded keyset page of bucket scopes per pass, with a
	// durable rotation cursor of its own; settled buckets vanish, so the
	// population self-drains. Runs on a bounded DETACHED context (Codex
	// round 34): the main rotation regularly burns the whole pass
	// deadline, this is the ONLY settlement path for vanished bases, and
	// inheriting the expired pass context would defer their refunds
	// indefinitely on every deadline-bound deployment.
	settleCtx, cancelSettle := context.WithTimeout(context.WithoutCancel(ctx), refundSettleTimeout)
	defer cancelSettle()
	r.settleVanishedRefunds(settleCtx, scanned, &report, &errs)
	// Park the rotation durably for whichever replica runs the next pass
	// (issue #121), on a bounded DETACHED context: the pass regularly
	// ends by burning its own deadline, and losing the park would replay
	// this pass's whole rotation segment. Best-effort beyond that.
	parkCtx, cancelPark := context.WithTimeout(context.WithoutCancel(ctx), scanStateWriteTimeout)
	defer cancelPark()
	if err := r.registry.PutJobScanState(parkCtx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRotation, cursor, 0); err != nil {
		errs.add(fmt.Errorf("lfs reclaim: park rotation cursor: %w", err))
	}
	r.logger.Info("lfs reclaim pass finished",
		"repos_scanned", report.ReposScanned,
		"repos_skipped", report.ReposSkipped,
		"repos_over_budget", report.ReposOverBudget,
		"objects_walked", report.ObjectsWalked,
		"rows_examined", report.RowsExamined,
		"rows_referenced", report.RowsReferenced,
		"rows_marked", report.RowsMarked,
		"rows_cleared", report.RowsCleared,
		"rows_in_grace", report.RowsInGrace,
		"rows_eligible", report.RowsEligible,
		"rows_retired", report.RowsRetired,
		"rows_spared", report.RowsSpared,
		"bytes_retired", report.BytesRetired,
		"dry_run", report.DryRun,
		"reap_watermark_stale", report.ReapWatermarkStale,
		"rows_deferred_reap_stale", report.RowsDeferredReapStale,
		"rows_deferred_legacy_unarmed", report.RowsDeferredLegacyUnarmed,
		"refund_rows_settled", report.RefundRowsSettled,
		"refund_bytes_settled", report.RefundBytesSettled,
		"refund_buckets_pending", report.RefundBucketsPending)
	return report, errs.join()
}

// reclaimRepo scans one base repo's namespace group and settles its rows.
// It returns the work charged against the run budget — objects the walks
// listed plus registry rows settlement examined (Codex round 4: settlement
// transactions are attacker-scaled too; a ref-less cap-filled repo walks
// nothing yet holds MaxLFSObjectsPerRepo rows) — and an error for a failed
// or exhausted scan. A walk error means NO row was touched: marking on an
// incomplete reachable set would start grace clocks on referenced objects,
// and retiring on one would delete live data. Settlement exhaustion leaves
// durable partial progress, which is safe: each row's verdict comes from
// the already-complete reachable set, and the next run recomputes anyway.
func (r *LFSReclaimer) reclaimRepo(ctx context.Context, base string, budget int64, report *LFSReclaimReport) (int64, error) {
	if purged, err := r.registry.IsRepoPurged(ctx, base); err != nil {
		return 0, err
	} else if purged {
		report.ReposSkipped++ // the purge pipeline owns fenced repos' rows
		// Drop any parked settlement cursor with the repo: the purge
		// pipeline drops the registry rows the cursor pointed into, and
		// scan-state rows must not outlive their repo (idempotent).
		if err := r.registry.DeleteJobScanState(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRepo(base)); err != nil {
			return 0, err
		}
		return 0, nil
	}
	// Settle refund buckets a completed destructive sweep now covers
	// (Codex round 33): visited bases settle here; bases that vanished
	// from the work queue (every row retired) settle via the refund
	// rotation in Run.
	r.settleRefunds(ctx, base, report)
	branch, ok, err := r.defaultBranch(ctx, base)
	if err != nil {
		return 0, err
	}
	if !ok {
		report.ReposSkipped++ // no metadata (deleted mid-scan): never retire
		return 0, nil
	}
	// Pin every namespace-group member's generation BEFORE hydration
	// (ADR-35: the group shares one object space, so a pointer anywhere in
	// it references the base-keyed object). The caches then reflect at
	// least the pinned refs; retirement demands exactly the pinned
	// generation, so refs that moved later — or hydrated newer — fail the
	// verdict safe.
	group := blob.QuotaRepoGroup(base)
	gens := make(map[string]int64, len(group))
	for _, member := range group {
		gen, err := r.registry.GetGeneration(ctx, member)
		if err != nil {
			return 0, err
		}
		gens[member] = gen
	}
	referenced := make(map[string]struct{})
	var walked int64
	for _, member := range group {
		// Count at most one past the remaining budget (Codex round 12):
		// the count itself walks the ref index, so an uncapped COUNT on
		// a fanout repo would do budget-scale index work every pass. A
		// saturated count still pushes walked past the budget, so the
		// guard below fires with the same over-budget verdict. The +1 is
		// a SATURATING add (Codex round 17): at budget MaxInt64 the
		// naive sentinel overflowed negative and clamped the cap to 1,
		// letting hydration materialize an arbitrarily large ref set
		// charged as a single unit.
		countCap := budget - walked // ≥ 1: every over-budget path above returned
		assertf(countCap >= 1, "lfs reclaim: non-positive ref count cap %d (budget %d, walked %d)", countCap, budget, walked)
		if countCap < math.MaxInt64 {
			countCap++
		}
		refCount, err := r.registry.CountRefs(ctx, member, countCap)
		if err != nil {
			return walked, err
		}
		// Every ref charges one work unit (Codex round 6): ref fanout is
		// attacker-scaled too — millions of same-tip refs cost registry
		// work, cache synchronization, and rev-list ref enumeration
		// while adding no unique objects for the walk to charge.
		walked += refCount
		if refCount == 0 {
			continue // nothing reachable, no cache needed
		}
		if walked >= budget {
			// The remainder is gone — consumed by earlier members or by
			// this member's own ref fanout — and the walker (rightly)
			// asserts a positive budget (Codex round 3). Surface the
			// verdict a mid-walk abort would have produced — the caller
			// defers or skips the repo — without hydrating a member
			// whose walk could not even start.
			return walked, fmt.Errorf("walk %s: %w (budget %d)", member, lfsscan.ErrLFSScanBudget, budget)
		}
		repoPath, err := r.hydrateBounded(ctx, member, branch)
		if err != nil {
			return walked, fmt.Errorf("hydrate %s: %w", member, err)
		}
		oids, w, err := r.walkBounded(ctx, repoPath, budget-walked)
		walked += w
		if err != nil {
			return walked, fmt.Errorf("walk %s: %w", member, err)
		}
		for oid := range oids {
			referenced[oid] = struct{}{}
		}
	}
	report.ReposScanned++
	report.ObjectsWalked += walked
	settled, err := r.settleRepoRows(ctx, base, gens, referenced, budget-walked, report)
	return walked + settled, err
}

// walkBounded runs the walk seam under the fixed per-member deadline
// (Codex security round 18): the budget cancels the walk only as cat-file
// meters objects, but rev-list's own tree inflation happens before those
// lines stream, so a giant compressible tree burns unmetered CPU first.
// The deadline hard-caps a member's walk wall-clock like hydration; expiry
// fails the repo (deferred, retried next rotation, skipped loudly if
// persistent).
func (r *LFSReclaimer) walkBounded(ctx context.Context, repoPath string, budget int64) (map[string]struct{}, int64, error) {
	assertf(r.walkTimeout > 0, "walkBounded: non-positive timeout %v", r.walkTimeout)
	wctx, cancel := context.WithTimeout(ctx, r.walkTimeout)
	defer cancel()
	return r.walk(wctx, repoPath, budget)
}

// hydrateBounded runs the hydrate seam under the fixed per-member deadline
// so one cold, pack-stuffed repository cannot monopolize a pass with
// pre-walk download/index work the budget never sees; the timeout fails the
// repo like any hydration error (deferred, retried next rotation).
func (r *LFSReclaimer) hydrateBounded(ctx context.Context, member, branch string) (string, error) {
	assertf(r.hydrateTimeout > 0, "hydrateBounded: non-positive timeout %v", r.hydrateTimeout)
	hctx, cancel := context.WithTimeout(ctx, r.hydrateTimeout)
	defer cancel()
	return r.hydrate(hctx, member, branch)
}

// settleRepoRows diffs base's registered rows against the referenced OID
// set: clears marks on referenced rows, marks unreferenced ones, and
// retires (or, dry-run, counts) rows whose mark outlived the grace window.
// Every examined row charges one unit of allowance — the same budget the
// walks draw from — and exhaustion aborts with the budget verdict so the
// caller defers or skips the repo like an over-budget walk. Progress is
// durable two ways: settled rows stay settled, and the per-repo row cursor
// resumes the next attempt after the last examined key (Codex round 8) —
// without it, a non-retiring prefix of referenced, in-grace, or dry-run
// rows would be re-paged forever and the tail never examined. The cursor
// is a fleet-persistent job_scan_state row (issue #121) fenced by the
// pinned generations: any member's ref movement invalidates it, restarting
// settlement from the top of the keyspace — never a resume across a fence.
// Resumption is safe regardless because every verdict below derives from
// THIS pass's freshly computed reachable set; the cursor only skips a
// prefix whose rows were settled against an equally valid earlier set.
func (r *LFSReclaimer) settleRepoRows(ctx context.Context, base string, gens map[string]int64, referenced map[string]struct{}, allowance int64, report *LFSReclaimReport) (int64, error) {
	assertf(allowance >= 0, "settleRepoRows: negative allowance %d", allowance)
	var errs boundedErrs
	var charged int64
	after := ""
	fence := gensFence(gens)
	cursorScope := ref.ScanScopeReclaimRepo(base)
	switch st, ok, err := r.registry.GetJobScanState(ctx, ref.ScanJobLFSReclaim, cursorScope); {
	case err != nil:
		return 0, err
	case ok && st.Generation == fence:
		after = st.Payload // resume past the settled prefix
	case ok:
		// Refs moved since the cursor was parked: the fence invalidates
		// it — fresh scan. Deleting is cosmetic (a park or completion
		// below rewrites it) but keeps stale rows from lingering when
		// the repo never parks again.
		if err := r.registry.DeleteJobScanState(ctx, ref.ScanJobLFSReclaim, cursorScope); err != nil {
			return 0, err
		}
	}
	for {
		page := lfsReclaimRowPage
		if remaining := allowance - charged; remaining < int64(page) {
			page = int(remaining)
		}
		if page == 0 {
			// The allowance is spent, but the settlement may in fact be
			// complete. One 1-row probe (listed, never processed) tells
			// which; only genuinely remaining rows defer the repo.
			probe, err := r.registry.ListRepoLFSObjects(ctx, base, after, 1)
			if err != nil {
				return charged, err
			}
			if len(probe) == 0 {
				break
			}
			// Park the cursor durably: the next attempt — on ANY
			// replica — resumes here (issue #121). The park is
			// purge-FENCED (Codex round 26): a purge committing
			// between the probe above and this write already deleted
			// the repo's scan state, and an unfenced upsert would
			// recreate a row nothing ever removes.
			if err := r.registry.ParkRepoJobScanState(ctx, ref.ScanJobLFSReclaim, cursorScope, after, fence, base); err != nil {
				return charged, err
			}
			errs.add(fmt.Errorf("settle %s: %w (budget)", base, lfsscan.ErrLFSScanBudget))
			return charged, errs.join()
		}
		rows, err := r.registry.ListRepoLFSObjects(ctx, base, after, page)
		if err != nil {
			return charged, err
		}
		if len(rows) == 0 {
			break
		}
		charged += int64(len(rows))
		after = rows[len(rows)-1].Key
		var toMark, toClear []string
		for _, row := range rows {
			report.RowsExamined++
			rowRepo, oid, ok := splitLFSKey(row.Key)
			if !ok || rowRepo != base {
				// Malformed registry row: never a retirement candidate.
				r.logger.Warn("lfs reclaim: skipping unparseable registry row", "repo", base, "key", row.Key)
				continue
			}
			if _, isRef := referenced[oid]; isRef {
				report.RowsReferenced++
				if !row.UnreferencedSince.IsZero() {
					toClear = append(toClear, row.Key)
				}
				continue
			}
			if row.UnreferencedSince.IsZero() {
				toMark = append(toMark, row.Key)
				continue
			}
			if r.now().Sub(row.UnreferencedSince) < r.grace {
				report.RowsInGrace++
				continue
			}
			report.RowsEligible++
			if !r.deleteEnabled {
				continue
			}
			if r.reapStale {
				// Advisory demotion (issue #120): the retire transaction
				// would refuse with the same verdict; skip the round
				// trip and report the deferral distinctly.
				report.RowsDeferredReapStale++
				continue
			}
			if !r.legacyReapArmed && !strings.Contains(row.Key, "/o2/") {
				// Legacy-spelled row without end-to-end legacy arming
				// (Codex round 17): its blob would never be physically
				// reaped, so the slot refund is unbacked. Config half
				// is process-static; the watermark half is re-checked
				// inside the retire transaction.
				report.RowsDeferredLegacyUnarmed++
				continue
			}
			retired, err := r.registry.RetireUnreferencedLFSObject(ctx, base, row.Key, gens, r.grace, r.reapMaxAge())
			switch {
			case errors.Is(err, ref.ErrReapWatermarkStale):
				// The in-transaction gate fired (the pass outlived the
				// watermark's freshness): deferred, not an error.
				report.RowsDeferredReapStale++
			case errors.Is(err, ref.ErrLegacyReapUnarmed):
				// The in-transaction payload recheck fired (the
				// watermark rolled to an unarmed sweep mid-pass).
				report.RowsDeferredLegacyUnarmed++
			case err != nil:
				errs.add(err)
			case retired:
				report.RowsRetired++
				report.BytesRetired += row.Bytes
			default:
				report.RowsSpared++
			}
		}
		if len(toClear) > 0 {
			n, err := r.registry.ClearLFSObjectsUnreferenced(ctx, base, toClear)
			if err != nil {
				return charged, err
			}
			report.RowsCleared += n
		}
		if len(toMark) > 0 {
			n, err := r.registry.MarkLFSObjectsUnreferenced(ctx, base, toMark)
			if err != nil {
				return charged, err
			}
			report.RowsMarked += n
		}
	}
	// End of the keyspace: the rotation is complete (a cursor-resumed pass
	// covered the tail; the front was covered when the cursor was minted).
	if err := r.registry.DeleteJobScanState(ctx, ref.ScanJobLFSReclaim, cursorScope); err != nil {
		return charged, err
	}
	return charged, errs.join()
}

// LFSReclaimSchedule pairs the reclaimer with its cadence — its own
// schedule, independent of the sweeper's (count reclamation is useful even
// where blob sweeping stays disabled).
type LFSReclaimSchedule struct {
	Reclaimer *LFSReclaimer
	Interval  time.Duration
}

// StartLFSReclaimTicker runs the schedule every Interval on a background
// goroutine — local mode's analogue of the River periodic job, with the
// same run-on-start behavior. The returned stop function cancels the loop
// and waits for an in-flight run.
func StartLFSReclaimTicker(schedule *LFSReclaimSchedule) (stop func()) {
	assertf(schedule != nil && schedule.Reclaimer != nil, "StartLFSReclaimTicker: nil schedule or reclaimer")
	assertf(schedule.Interval > 0, "StartLFSReclaimTicker: non-positive interval %v", schedule.Interval)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(schedule.Interval)
		defer ticker.Stop()
		for {
			if _, err := schedule.Reclaimer.Run(ctx); err != nil && ctx.Err() == nil {
				schedule.Reclaimer.logger.Error("scheduled lfs reclaim failed", "err", err)
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
