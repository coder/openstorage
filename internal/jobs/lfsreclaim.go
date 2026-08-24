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
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/lfsscan"
	"github.com/coder/code-storage/internal/ref"
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
	// recheck, count/usage refund).
	RetireUnreferencedLFSObject(ctx context.Context, repo, key string, pinnedGens map[string]int64, grace time.Duration) (bool, error)
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
	// budget can reject it.
	CountRefs(ctx context.Context, repo string) (int64, error)
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
}

const (
	// lfsReclaimRepoBatch bounds one work-queue page.
	lfsReclaimRepoBatch = 100
	// lfsReclaimRowPage bounds one lfs_objects page (and therefore one
	// mark/clear statement's key list).
	lfsReclaimRowPage = 1_000
	// DefaultLFSReclaimObjectBudget bounds the git objects one run may
	// list across all reachability walks (CODESTORAGE_LFS_GC_OBJECT_BUDGET
	// overrides).
	DefaultLFSReclaimObjectBudget = int64(1_000_000)
	// DefaultLFSReclaimGrace is how long a row must stay continuously
	// unreferenced before retirement (CODESTORAGE_LFS_GC_GRACE overrides).
	DefaultLFSReclaimGrace = 24 * time.Hour
	// lfsReclaimHydrateTimeout bounds one namespace member's cache
	// hydration (Codex round 7): on a cold replica, Materialize downloads
	// and indexes every registered pack before the budgeted walk can even
	// start, and a writer can fill the pack quota with a few huge packs.
	// A fixed deadline — the repack budgets' compiled-in pattern — defers
	// the repo with an error instead of letting one hydration monopolize
	// the pass. Retried next rotation; a warm cache hydrates in seconds.
	lfsReclaimHydrateTimeout = 10 * time.Minute
)

// LFSReclaimer runs the reclamation scan. Construct with NewLFSReclaimer;
// Run executes one bounded pass on the schedule's cadence.
type LFSReclaimer struct {
	registry       LFSReclaimRegistry
	hydrate        HydrateFunc
	walk           LFSWalkFunc
	defaultBranch  DefaultBranchFunc
	grace          time.Duration
	objectBudget   int64
	deleteEnabled  bool
	logger         *slog.Logger
	now            func() time.Time // injectable clock for tests
	hydrateTimeout time.Duration    // per-member cap, shrinkable in tests
	// cursor is the last base repo a run attempted (successful or not):
	// each run resumes strictly after it and wraps on exhaustion, so a
	// failing or over-budget repo cannot monopolize every run (the
	// purger's rotation, issue #52). In-memory per process.
	cursor string
}

// NewLFSReclaimer wires the reclamation job.
func NewLFSReclaimer(registry LFSReclaimRegistry, hydrate HydrateFunc, walk LFSWalkFunc, defaultBranch DefaultBranchFunc, grace time.Duration, objectBudget int64, deleteEnabled bool, logger *slog.Logger) *LFSReclaimer {
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
		logger:         logger,
		now:            time.Now,
		hydrateTimeout: lfsReclaimHydrateTimeout,
	}
}

// Run executes one bounded reclamation pass: page LFS-holding repos from
// the rotating cursor, scan each within the remaining object budget, and
// mark/clear/retire its rows. Per-repo failures defer that repo to a later
// run without stopping the pass; they are joined into the returned error
// so the job backend records them.
func (r *LFSReclaimer) Run(ctx context.Context) (LFSReclaimReport, error) {
	report := LFSReclaimReport{DryRun: !r.deleteEnabled}
	budget := r.objectBudget
	var errs boundedErrs
	cursor := r.cursor
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
					r.logger.Warn("lfs reclaim: repository exceeds the full run budget; raise CODESTORAGE_LFS_GC_OBJECT_BUDGET to scan and settle it in one pass",
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
	r.cursor = cursor
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
		"dry_run", report.DryRun)
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
		return 0, nil
	}
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
		refCount, err := r.registry.CountRefs(ctx, member)
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
		oids, w, err := r.walk(ctx, repoPath, budget-walked)
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
// caller defers or skips the repo like an over-budget walk. Rows already
// settled stay settled; retirement's per-row transactions shrink the key
// range's front, so successive passes over an oversized repo converge.
func (r *LFSReclaimer) settleRepoRows(ctx context.Context, base string, gens map[string]int64, referenced map[string]struct{}, allowance int64, report *LFSReclaimReport) (int64, error) {
	assertf(allowance >= 0, "settleRepoRows: negative allowance %d", allowance)
	var errs boundedErrs
	var charged int64
	after := ""
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
			retired, err := r.registry.RetireUnreferencedLFSObject(ctx, base, row.Key, gens, r.grace)
			switch {
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
