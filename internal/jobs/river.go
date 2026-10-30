package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/sync"
)

// deliveryArgs is one durable webhook-delivery job. The payload is the
// exact JSON body frozen at enqueue time (docs/reference/webhooks.md: only the signature is
// recomputed per attempt).
type deliveryArgs struct {
	WebhookID string          `json:"webhook_id"`
	Event     string          `json:"event"`
	Payload   json.RawMessage `json:"payload"`
}

func (deliveryArgs) Kind() string { return "webhook_delivery" }

func (deliveryArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: MaxDeliveryAttempts}
}

// syncArgs is one durable upstream-sync job, unique per repo across the
// active states so pushes cannot pile up duplicate runs (River drops the
// duplicate insert); completed runs are excluded so later pushes can
// re-sync. The run itself owns retries/outcomes (sync.Worker), so a job
// executes at most once.
//
// Uniqueness including the running state (river requires running in
// ByState) swallows pull-upstream requests that arrive while a run is in
// flight — but that run's upstream fetch may already be behind the
// request's causally-later upstream state (issue #79). The producer
// therefore marks the request durable first (RequestSync), and the worker
// completes its job transactionally with a gated successor insert — see
// syncWorker.Work for the gate and the swallowed-request coverage argument.
type syncArgs struct {
	Org    string `json:"org"`
	RepoID string `json:"repo_id" river:"unique"`
	Ref    string `json:"ref,omitempty"`
}

func (syncArgs) Kind() string { return "upstream_sync" }

// syncRescueAttempts exists for exactly one consumer: river's job rescuer.
// Work never returns a plain error (run outcomes are handled in-run and
// completion failures snooze, which does not consume attempts), so on a
// healthy worker a job still executes at most once. Only a job orphaned by
// a CRASHED worker process is rescued and re-run — and that retry is
// load-bearing (issue #86): a pull-upstream accepted just before the crash
// was deduplicated against the orphaned job's running row, so the rescued
// re-run — whose claim attempt steals the dead holder's expired lease — is
// that request's only follow-up. With a single attempt the rescuer could
// only discard the orphan, stranding the accepted 202 until an unrelated
// later trigger.
const syncRescueAttempts = 3

func (syncArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: syncRescueAttempts,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}

// pushArgs is one durable outbound push-upstream job (issue #90), unique
// per repo across the active states exactly like syncArgs. Force does NOT
// ride the args — uniqueness is keyed by repo, so a forced insert can be
// deduplicated against a queued plain job; the durable push_pending level
// (UpstreamStore.RequestPush, consumed by BeginPush) carries it instead.
// Retry counts consecutive transient-failure retries: it selects the
// successor's backoff and is deliberately excluded from uniqueness.
type pushArgs struct {
	Org    string `json:"org"`
	RepoID string `json:"repo_id" river:"unique"`
	Retry  int    `json:"retry,omitempty"`
}

func (pushArgs) Kind() string { return "upstream_push" }

func (pushArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		// Rescue semantics mirror syncArgs: a healthy worker completes
		// every job exactly once; only crash-orphaned jobs are re-run,
		// and that re-run is the marked request's only follow-up.
		MaxAttempts: syncRescueAttempts,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}

// sweepArgs is one orphan-pack sweep run. No payload: the sweep always
// covers the whole packs/ space. Unique across the active states so the
// periodic schedulers of concurrently running replicas cannot pile up
// overlapping sweeps; MaxAttempts 1 because the next periodic tick is the
// retry.
type sweepArgs struct{}

func (sweepArgs) Kind() string { return "orphan_pack_sweep" }

func (sweepArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}

// lfsReclaimArgs is one scheduled unreferenced-LFS-object reclamation pass
// (issue #73), unique across the active states like sweepArgs so
// concurrently booting replicas cannot pile up overlapping scans;
// MaxAttempts 1 because the next periodic tick is the retry.
type lfsReclaimArgs struct{}

func (lfsReclaimArgs) Kind() string { return "lfs_reclaim" }

func (lfsReclaimArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}

type lfsReclaimWorker struct {
	river.WorkerDefaults[lfsReclaimArgs]
	schedule *LFSReclaimSchedule
}

func (w *lfsReclaimWorker) Work(ctx context.Context, _ *river.Job[lfsReclaimArgs]) error {
	_, err := w.schedule.Reclaimer.Run(ctx)
	return err
}

// absenceQueue is the reserved single-worker River queue absence
// reconciliation runs on (Codex round 11, security): cover renewal is
// TTL-coupled, and the default queue's eight workers are shared with
// tenant-scalable work — webhook deliveries fan out per push to
// attacker-controlled endpoints that can hold each attempt for its full
// timeout, delaying a default-queue renewal past the cover TTL. A
// reserved queue bounds renewal latency by the job's own runtime (the
// TTL/4 pass cap), not by tenant traffic. The sweep/reclaim jobs stay on
// the default queue deliberately: starving them delays convergence, never
// a safety property.
const absenceQueue = "absence_reconcile"

// absenceReconcileArgs is one scheduled registry-vs-storage absence
// reconciliation pass (issue #101), unique across the active states like
// sweepArgs so concurrently booting replicas cannot pile up overlapping
// scans; MaxAttempts 1 because the next periodic tick is the retry.
type absenceReconcileArgs struct{}

func (absenceReconcileArgs) Kind() string { return "absence_reconcile" }

func (absenceReconcileArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue:       absenceQueue,
		MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}

type absenceReconcileWorker struct {
	river.WorkerDefaults[absenceReconcileArgs]
	schedule *AbsenceReconcileSchedule
}

func (w *absenceReconcileWorker) Work(ctx context.Context, _ *river.Job[absenceReconcileArgs]) error {
	_, err := w.schedule.Reconciler.Run(ctx)
	return err
}

// repackArgs is one durable background-repack job, unique per repo across
// the active states exactly like syncArgs: post-write triggers on every
// node can fire concurrently, and River coalesces them into one job.
// MaxAttempts 1 — a lost fold-install race must not retry eagerly (the
// next insert past the threshold is the retry), and a failed fold is
// harmless to defer the same way.
//
// Uniqueness including the running state (river requires running in
// ByState) swallows triggers that fire while a fold runs — but a running
// fold works a row snapshot from its start, so packs registered mid-fold
// survive it and a burst could otherwise finish above the threshold with
// nothing scheduled until the next write. The worker therefore completes
// its job transactionally (river.JobCompleteTx) and inserts a gated
// successor in the SAME transaction — see repackWorker.Work for the gate
// and the swallowed-trigger coverage argument.
type repackArgs struct {
	Repo string `json:"repo" river:"unique"`
}

func (repackArgs) Kind() string { return "repack" }

// repackMaxAttempts bounds river's backoff retries of a FAILING fold
// (transient materialization/blob/git errors). More than one attempt is
// load-bearing: triggers arriving while the job is active are deduplicated
// against it, so the retryable job itself is the only follow-up they have —
// a single-attempt job dying on a blip stranded them until the next write.
// Lost races and no-ops return nil (never retried), and a job exhausting
// all attempts means a persistent infrastructure failure, where the next
// write's trigger chain is the recovery path anyway.
const repackMaxAttempts = 4

func (repackArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: repackMaxAttempts,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}

// repackFollowUpDelay schedules a due follow-up fold slightly in the
// future: long enough to batch a burst's remaining registrations, short
// enough to keep pack counts near the threshold under sustained traffic.
const repackFollowUpDelay = 5 * time.Second

// repackWorker runs one fold attempt; the Repacker owns race handling and
// reporting. pool is the river client's own pgx pool (set by NewRiver
// before the client starts); the transactional completion below needs it.
type repackWorker struct {
	river.WorkerDefaults[repackArgs]
	repacker *Repacker
	pool     *pgxpool.Pool
}

// Work folds once, then closes the trigger-uniqueness window by completing
// the job and inserting a GATED successor in one transaction. The gate —
// due && (folded || count changed while this job ran) — makes chains both
// complete and terminating:
//
//   - budget-sliced backlogs progress: each link folds (folded=true) and
//     chains while the count stays at/above the threshold;
//   - wedged repos terminate: a no-op link over an unchanged count
//     schedules nothing, so a repo stuck above the threshold with no
//     pairable pair does not busy-chain (and disabling the threshold
//     stops everything);
//   - a caller drip-feeding single packs cannot force per-pack
//     recompression: after a fold drops the count below the threshold,
//     due=false ends the chain until the trigger threshold is crossed
//     again.
//
// Swallowed-trigger coverage: a trigger whose insert was deduped BEFORE
// the completion transaction updated this job's row implies its
// registration committed earlier still, so the post-completion count read
// (ordered strictly after JobCompleteTx inside the transaction window)
// observes it — count changed ⇒ successor. A trigger insert arriving
// DURING the transaction blocks on the unique index entry this row is
// vacating and proceeds after commit: it either conflicts with the
// just-inserted successor (which covers it) or, when the gate scheduled
// nothing, inserts its own job. No ordering strands a registration.
func (w *repackWorker) Work(ctx context.Context, job *river.Job[repackArgs]) error {
	// The pre-count baseline detects growth across the fold; a failed read
	// degrades to preKnown=false, which the gate treats conservatively
	// (schedule when due — an extra no-op successor terminates by the
	// plateau rule).
	preCount, _, preErr := w.repacker.FollowUpDue(ctx, job.Args.Repo)
	preKnown := preErr == nil
	folded, err := w.repacker.Repack(ctx, job.Args.Repo)
	if !folded {
		if err != nil {
			// Fold-budget exhaustion is DETERMINISTIC: the same input
			// times out on every attempt, so retrying would multiply the
			// per-fold budget by MaxAttempts (a git:write caller
			// registering pathological packs could hold a shared worker
			// for the product). Cancel terminally — the budget already
			// chose bounded-work-over-completeness for such inputs, and
			// the next threshold crossing starts a fresh, equally bounded
			// job. Everything else is a transient pre-swap failure:
			// return it so river RETRIES with backoff (repackMaxAttempts)
			// — the job must stay active, because triggers deduplicated
			// against it have no other follow-up.
			if errors.Is(err, context.DeadlineExceeded) {
				return river.JobCancel(err)
			}
			return err
		}
		// Clean no-op or lost race: complete normally, but still through
		// the gated path — registrations may have raced this job.
	} else if err != nil {
		// The fold committed (or may have) but verification failed
		// transiently: the row swap is durable and the sweeper's
		// quarantine heal backstops the bytes. Failing this MaxAttempts-1
		// job would skip the successor and strand coalesced mid-fold
		// triggers — log and continue to the atomic completion below.
		w.repacker.logger.Warn("repack: fold committed but verification failed (sweeper heal is the backstop)",
			"repo", job.Args.Repo, "err", err)
	}
	assertf(w.pool != nil, "repackWorker: pool not wired")
	// Failures anywhere in the completion transaction SNOOZE the job:
	// returning nil would let river complete it without the gate ever
	// running. The snoozed re-run costs one idempotent no-op fold and
	// retries the completion; if the commit failure was ambiguous (it
	// actually landed, successor included), the resurrected job no-ops
	// and completes normally.
	completionFailed := func(stage string, err error) error {
		w.repacker.logger.Warn("repack: atomic completion failed; snoozing to retry it",
			"repo", job.Args.Repo, "stage", stage, "err", err)
		return river.JobSnooze(repackFollowUpDelay)
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return completionFailed("begin", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return completionFailed("complete", err)
	}
	// The count read runs strictly AFTER JobCompleteTx (see the coverage
	// argument above) on the repacker's own store connection: it sees
	// every registration whose trigger was already deduped against this
	// job's active row. A failed read must NOT commit the completion —
	// treating it as due=false would strand exactly those registrations —
	// so it snoozes like any other completion-transaction failure.
	postCount, due, postErr := w.repacker.FollowUpDue(ctx, job.Args.Repo)
	if postErr != nil {
		return completionFailed("follow-up recheck", postErr)
	}
	if due && (folded || !preKnown || postCount != preCount) {
		if _, err := river.ClientFromContext[pgx.Tx](ctx).InsertTx(ctx, tx, repackArgs{Repo: job.Args.Repo},
			&river.InsertOpts{ScheduledAt: time.Now().Add(repackFollowUpDelay)}); err != nil {
			return completionFailed("insert successor", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return completionFailed("commit", err)
	}
	return nil
}

// sweepWorker runs one scheduled tick — the orphan-pack sweep plus the
// optional repo-retention purge pass (issue #52); the Sweeper and
// RepoPurger own all reporting.
type sweepWorker struct {
	river.WorkerDefaults[sweepArgs]
	schedule *SweepSchedule
}

func (w *sweepWorker) Work(ctx context.Context, _ *river.Job[sweepArgs]) error {
	return w.schedule.runOnce(ctx)
}

// deliveryWorker performs one signed POST per job attempt; River owns the
// backoff/attempt accounting (re-signing falls out of Deliverer.Attempt
// reloading the webhook and signing with time.Now on every attempt).
type deliveryWorker struct {
	river.WorkerDefaults[deliveryArgs]
	deliverer *events.Deliverer
}

func (w *deliveryWorker) Work(ctx context.Context, job *river.Job[deliveryArgs]) error {
	err := w.deliverer.Attempt(ctx, events.Delivery{
		WebhookID: job.Args.WebhookID,
		Event:     job.Args.Event,
		Payload:   job.Args.Payload,
	})
	if errors.Is(err, events.ErrWebhookGone) {
		return river.JobCancel(err) // webhook deleted: terminal, never retried
	}
	return err
}

// syncFollowUpSnooze delays the retry of a failed sync completion
// transaction (mirrors repackFollowUpDelay for the analogous repack gate).
const syncFollowUpSnooze = 5 * time.Second

// legacyClaimSnooze paces re-runs against a lapsed LEGACY claim, which may
// be deliberately non-stealable (meta.EnvSyncStealLegacy unset): the
// durable job must stay alive for the moment the opt-in lands or the old
// replica finishes, but not spin the queue and store every few seconds.
const legacyClaimSnooze = 5 * time.Minute

// syncSnoozeDelay computes the uncovered-pending-request snooze: the base
// completion-retry delay normally, until just past the observed claim
// expiry when a claim is held, at legacy pace for lapsed legacy claims,
// capped for clock-skew paranoia (the delay only defers the retry, never
// correctness — each re-run recomputes from the row).
func syncSnoozeDelay(up sync.Upstream) time.Duration {
	delay := syncFollowUpSnooze
	if !up.Syncing {
		return delay
	}
	if until := time.Until(up.SyncClaimExpiresAt) + time.Second; until > delay {
		delay = until
	}
	if up.SyncClaimHolder == sync.LegacyClaimHolder && delay < legacyClaimSnooze {
		delay = legacyClaimSnooze
	}
	if delay > 30*time.Minute {
		delay = 30 * time.Minute
	}
	return delay
}

// syncWorker executes one sync run. Run's error return is for logging only
// (documented failure outcomes already emitted repo.sync.failed), so the
// job always completes — through the gated transactional completion below,
// which closes the uniqueness window for pull-upstream requests (issue #79).
type syncWorker struct {
	river.WorkerDefaults[syncArgs]
	run       SyncRunner
	upstreams sync.UpstreamStore
	pool      *pgxpool.Pool
	logger    *slog.Logger
	runs      context.Context // see withShutdown
}

// Work runs one sync, then completes the job and inserts a GATED successor
// in one transaction, exactly like repackWorker.Work: syncArgs uniqueness
// must include the running state (river requires it), so a pull-upstream
// insert while this job runs is deduplicated against it even though this
// run's upstream fetch may already have happened — the request's only
// follow-up is the successor inserted here. The producer marks the request
// durable first (UpstreamStore.RequestSync sets SyncPending; the claim in
// BeginSync consumes it), so the gate is a durable, re-checkable condition.
//
// Swallowed-request coverage (repackWorker's argument, adapted): a
// pull-upstream insert deduplicated BEFORE the completion transaction
// vacates this job's unique-index entry implies its RequestSync committed
// earlier still, so the post-completion pending read (ordered strictly
// after JobCompleteTx) observes it — pending ⇒ successor. An insert
// arriving DURING the transaction blocks on the entry this row is vacating
// and proceeds after commit: it either deduplicates against the
// just-inserted successor (whose claim-and-fetch are still in the future,
// so it covers the request) or, when the gate scheduled nothing, inserts
// its own job. No ordering strands a request.
//
// The RunCount half of the gate keeps successor chains bounded: a job that
// never claimed (skip on ErrSyncInProgress) leaves the counters untouched
// and schedules nothing — a skip implies the claim holder was live after
// this job's pre-read, so the holder's own gated completion covers
// concurrent requests. A run that FAILED before claiming is different: a
// pending marker with no counted run and no claim holder has no coverage
// at all, so completing would strand the accepted request — that case
// SNOOZES the job to retry the whole run (which then claims and consumes
// the marker), mirroring the transient-failure retry the completion path
// already uses.
func (w *syncWorker) Work(ctx context.Context, job *river.Job[syncArgs]) error {
	// Pre-run baseline: RunCount changing across this job proves a claimed
	// run completed while it was active. A failed read degrades to
	// preKnown=false, which the gate treats conservatively (schedule when
	// pending — the successor's claim consumes the marker, so a stale flag
	// cannot chain runs).
	preRuns, preKnown := 0, false
	switch up, err := w.upstreams.GetUpstream(ctx, job.Args.RepoID); {
	case err == nil:
		preRuns, preKnown = up.RunCount, true
	case !errors.Is(err, sync.ErrNotFound):
		w.logger.Warn("sync: pre-run upstream read failed",
			"repo", job.Args.RepoID, "err", err)
	}

	rctx, done := withShutdown(ctx, w.runs)
	runErr := w.run(rctx, sync.Job{Org: job.Args.Org, RepoID: job.Args.RepoID, Ref: job.Args.Ref})
	done()
	if runErr != nil {
		w.logger.Warn("sync run finished with error",
			"org", job.Args.Org, "repo", job.Args.RepoID, "err", runErr)
	}
	if errors.Is(runErr, sync.ErrShutdown) {
		return shutdownSnooze(ctx, w.upstreams, job.Args.RepoID, syncSnoozeDelay, w.logger)
	}

	assertf(w.pool != nil, "syncWorker: pool not wired")
	// Failures anywhere in the completion transaction SNOOZE the job:
	// returning nil would let river complete it without the gate ever
	// running, stranding requests deduplicated against this job. The
	// snoozed re-run costs one idempotent extra sync attempt (its
	// BeginSync claim, or skip, is harmless) and retries the completion.
	completionFailed := func(stage string, err error) error {
		w.logger.Warn("sync: atomic completion failed; snoozing to retry it",
			"repo", job.Args.RepoID, "stage", stage, "err", err)
		return river.JobSnooze(syncFollowUpSnooze)
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return completionFailed("begin", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return completionFailed("complete", err)
	}
	// The pending read runs strictly AFTER JobCompleteTx (see the coverage
	// argument above) on the upstream store's own connection. A failed
	// read must NOT commit the completion — treating it as not-pending
	// would strand exactly the requests deduplicated against this job —
	// so it snoozes like any other completion-transaction failure.
	switch up, err := w.upstreams.GetUpstream(ctx, job.Args.RepoID); {
	case errors.Is(err, sync.ErrNotFound):
		// Detached mid-flight: nothing to follow up.
	case err != nil:
		return completionFailed("pending recheck", err)
	case up.SyncPending && preKnown && up.RunCount == preRuns && (runErr != nil || up.Syncing):
		// No run was counted and a request is pending — completing would
		// strand the accepted 202: no successor exists and no counted
		// run's gate covers it. Two shapes land here:
		//   - the run FAILED before counting (transient lookup/store
		//     error, or a claimed run that could not finish through
		//     finishSync's detached retries);
		//   - the run SKIPPED because another holder's unexpired lease
		//     refused the steal (up.Syncing) — if that holder is dead
		//     (crashed process, issue #86), nothing will ever report the
		//     marker.
		// Snooze retries the whole run: a live holder's finisher consumes
		// the marker (the re-run then completes as a no-op), and a dead
		// holder's lease is stealable once expired — so wake just past
		// the observed expiry when a claim is held. (Returning without
		// commit rolls the completion back.)
		delay := syncSnoozeDelay(up)
		w.logger.Warn("sync: uncovered pending request; snoozing to retry it",
			"repo", job.Args.RepoID, "delay", delay, "claimHeld", up.Syncing, "err", runErr)
		return river.JobSnooze(delay)
	case up.SyncPending && (!preKnown || up.RunCount != preRuns):
		if _, err := river.ClientFromContext[pgx.Tx](ctx).InsertTx(ctx, tx,
			syncArgs{Org: job.Args.Org, RepoID: job.Args.RepoID}, nil); err != nil {
			return completionFailed("insert successor", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return completionFailed("commit", err)
	}
	return nil
}

// pushSnoozeDelay is syncSnoozeDelay for the outbound lane: wake just past
// the observed push-claim expiry when one is held (no legacy pacing — push
// claims are never legacy).
func pushSnoozeDelay(up sync.Upstream) time.Duration {
	delay := syncFollowUpSnooze
	if !up.PushSyncing {
		return delay
	}
	if until := time.Until(up.PushClaimExpiresAt) + time.Second; until > delay {
		delay = until
	}
	if delay > 30*time.Minute {
		delay = 30 * time.Minute
	}
	return delay
}

// pushWorker executes one outbound push-upstream run (issue #90). It is
// syncWorker's mirror on the push_* lane: the run owns outcomes and events,
// and completion is transactional with a gated successor insert — the
// swallowed-request coverage argument in syncWorker.Work applies verbatim
// with RequestPush/BeginPush/push_pending substituted. One addition: a run
// that failed TRANSIENTLY (upstream unreachable) re-marked its own pending
// level (sync.Worker.RunPush), so the gate schedules its successor with the
// worker-defined exponential backoff and an incremented retry counter
// instead of immediately, bounding retry pressure during upstream outages.
type pushWorker struct {
	river.WorkerDefaults[pushArgs]
	run       SyncRunner
	upstreams sync.UpstreamStore
	pool      *pgxpool.Pool
	logger    *slog.Logger
	runs      context.Context // see withShutdown
}

func (w *pushWorker) Work(ctx context.Context, job *river.Job[pushArgs]) error {
	// Pre-run baseline — see syncWorker.Work: PushRunCount changing across
	// this job proves a claimed run completed while it was active.
	preRuns, preKnown := 0, false
	switch up, err := w.upstreams.GetUpstream(ctx, job.Args.RepoID); {
	case err == nil:
		preRuns, preKnown = up.PushRunCount, true
	case !errors.Is(err, sync.ErrNotFound):
		w.logger.Warn("push: pre-run upstream read failed",
			"repo", job.Args.RepoID, "err", err)
	}

	rctx, done := withShutdown(ctx, w.runs)
	runErr := w.run(rctx, sync.Job{Org: job.Args.Org, RepoID: job.Args.RepoID, Retry: job.Args.Retry})
	done()
	if runErr != nil {
		w.logger.Warn("push run finished with error",
			"org", job.Args.Org, "repo", job.Args.RepoID, "err", runErr)
	}
	if errors.Is(runErr, sync.ErrShutdown) {
		return shutdownSnooze(ctx, w.upstreams, job.Args.RepoID, pushSnoozeDelay, w.logger)
	}

	assertf(w.pool != nil, "pushWorker: pool not wired")
	completionFailed := func(stage string, err error) error {
		w.logger.Warn("push: atomic completion failed; snoozing to retry it",
			"repo", job.Args.RepoID, "stage", stage, "err", err)
		return river.JobSnooze(syncFollowUpSnooze)
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return completionFailed("begin", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return completionFailed("complete", err)
	}
	switch up, err := w.upstreams.GetUpstream(ctx, job.Args.RepoID); {
	case errors.Is(err, sync.ErrNotFound):
		// Detached mid-flight: nothing to follow up.
	case err != nil:
		return completionFailed("pending recheck", err)
	case up.PushPending != sync.PushPendingNone && preKnown && up.PushRunCount == preRuns && (runErr != nil || up.PushSyncing):
		// Uncovered pending request (no counted run) — see the matching
		// syncWorker case: completing would strand the accepted 202.
		delay := pushSnoozeDelay(up)
		w.logger.Warn("push: uncovered pending request; snoozing to retry it",
			"repo", job.Args.RepoID, "delay", delay, "claimHeld", up.PushSyncing, "err", runErr)
		return river.JobSnooze(delay)
	default:
		if up.PushPending != sync.PushPendingNone && (!preKnown || up.PushRunCount != preRuns) {
			successor := pushArgs{Org: job.Args.Org, RepoID: job.Args.RepoID}
			var opts *river.InsertOpts
			if runErr != nil && sync.TransientPushFailure(runErr) && job.Args.Retry < sync.MaxPushTransientRetries {
				// The failed run re-marked its own retry (RunPush):
				// schedule the successor on the backoff cadence. Fresh
				// requests that coalesced into the marker ride along —
				// the upstream is unreachable for them too.
				successor.Retry = job.Args.Retry + 1
				opts = &river.InsertOpts{ScheduledAt: time.Now().Add(sync.PushRetryDelay(successor.Retry))}
			}
			if _, err := river.ClientFromContext[pgx.Tx](ctx).InsertTx(ctx, tx, successor, opts); err != nil {
				return completionFailed("insert successor", err)
			}
		}
		// Stranded mirror-back rescue (Codex round 6): RunPush marks the
		// mirror-back durable (RequestSync) BEFORE its direct enqueue,
		// but on pg that enqueue is a plain insert with no boot-recovery
		// scan behind it — if it fails transiently while no pull is in
		// flight, the sync_pending marker has no job left to consume it,
		// and a pull that fetched the pre-forward upstream could stand
		// divergent forever. Covering the marker here makes mirror-back
		// scheduling part of this push job's durable completion: the
		// insert deduplicates against any live sync job (unique by
		// repo), whose own completion gate re-checks the marker.
		if up.SyncPending {
			if _, err := river.ClientFromContext[pgx.Tx](ctx).InsertTx(ctx, tx,
				syncArgs{Org: job.Args.Org, RepoID: job.Args.RepoID}, nil); err != nil {
				return completionFailed("insert mirror-back", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return completionFailed("commit", err)
	}
	return nil
}

// River is the durable Postgres queue (plan Decision 1). The serve process
// runs it in full mode (migrations + workers); hook subprocesses run
// insert-only clients against the same database, which is how git-surface
// events reach the server's workers across process boundaries.
type River struct {
	client        *river.Client[pgx.Tx]
	pool          *pgxpool.Pool
	started       bool
	repackEnabled bool
	// cancelRuns cancels every sync and push run with sync.ErrShutdown
	// (DrainSyncRuns, issue #343); the workers derive each run's context
	// from runs.
	cancelRuns context.CancelCauseFunc
}

var (
	_ Queue                   = (*River)(nil)
	_ sync.FirstSyncScheduler = (*River)(nil)
)

// River's default job timeout is 1 minute, shorter than every worker budget (issue #632).
const riverJobTimeout = 2 * time.Hour

// NewRiver opens a dedicated pool on pgURL, applies the river migrations
// (serve owns DDL, mirroring meta/ref InitSchema), and starts workers that
// share the delivery/sync code with the local queue. upstreams is the
// durable upstream-row store the sync worker's successor gate reads
// (syncWorker.Work, issue #79). A non-nil sweep schedules the periodic
// orphan-pack sweep (run once on start, then every Interval); nil disables
// it (hook subprocesses, tests without storage). A non-nil repack registers
// the background-repack worker; nil rejects EnqueueRepack (repacking
// disabled or not wired, e.g. insert-only hook clients that must never
// fold).
func NewRiver(ctx context.Context, pgURL string, deliverer *events.Deliverer, runSync, runPush SyncRunner, upstreams sync.UpstreamStore, sweep *SweepSchedule, lfsReclaim *LFSReclaimSchedule, absence *AbsenceReconcileSchedule, repack *Repacker, logger *slog.Logger) (*River, error) {
	assertf(deliverer != nil, "NewRiver: nil deliverer")
	assertf(runSync != nil, "NewRiver: nil sync runner")
	assertf(runPush != nil, "NewRiver: nil push runner")
	assertf(upstreams != nil, "NewRiver: nil upstream store")
	assertf(sweep == nil || (sweep.Sweeper != nil && sweep.Interval > 0),
		"NewRiver: sweep schedule missing sweeper or interval: %+v", sweep)
	assertf(logger != nil, "NewRiver: nil logger")
	assertf(riverJobTimeout > max(lfsReclaimPassTimeout, absencePassTimeout)+sweepRecoveryTimeout,
		"NewRiver: job timeout %v does not cover the longest pass cap plus its tail", riverJobTimeout)
	rescueAfter := riverJobTimeout + 2*sweepRecoveryTimeout

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("jobs: open pgx pool: %w", err)
	}
	driver := riverpgxv5.New(pool)

	migrator, err := rivermigrate.New(driver, nil)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: river migrator: %w", err)
	}
	// Serialize migrations across concurrently booting replicas, mirroring the
	// advisory locks in meta/ref InitSchema: rivermigrate has no locking of its
	// own, and two fresh boots race on creating river's tables (SQLSTATE 23505).
	// Session-level lock on a dedicated conn because Migrate manages its own
	// transactions internally.
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: acquire migration lock conn: %w", err)
	}
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock(0x5249564552)`); err != nil { // "RIVER"
		lockConn.Release()
		pool.Close()
		return nil, fmt.Errorf("jobs: river migration lock: %w", err)
	}
	_, migrateErr := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	// The unlock must survive ctx cancellation (the very case where Migrate
	// just failed): releasing a conn that still holds the session lock back
	// into the pool would poison it — the next boot's pg_advisory_lock on
	// that pooled conn blocks forever. Unlock on a cancel-free deadline, and
	// if even that fails, close the conn so Release destroys it instead of
	// pooling it (the server drops the advisory lock with the session).
	unlockCtx, cancelUnlock := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	if _, err := lockConn.Exec(unlockCtx, `SELECT pg_advisory_unlock(0x5249564552)`); err != nil {
		logger.Warn("river migration unlock failed; destroying lock connection", "err", err)
		_ = lockConn.Conn().Close(unlockCtx)
	}
	cancelUnlock()
	lockConn.Release()
	if migrateErr != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: river migrations: %w", migrateErr)
	}

	runs, cancelRuns := context.WithCancelCause(context.Background())
	workers := river.NewWorkers()
	river.AddWorker(workers, &deliveryWorker{deliverer: deliverer})
	river.AddWorker(workers, &syncWorker{run: runSync, upstreams: upstreams, pool: pool, logger: logger, runs: runs})
	river.AddWorker(workers, &pushWorker{run: runPush, upstreams: upstreams, pool: pool, logger: logger, runs: runs})
	if repack != nil {
		river.AddWorker(workers, &repackWorker{repacker: repack, pool: pool})
	}
	var periodic []*river.PeriodicJob
	if sweep != nil {
		river.AddWorker(workers, &sweepWorker{schedule: sweep})
		periodic = append(periodic, river.NewPeriodicJob(
			river.PeriodicInterval(sweep.Interval),
			func() (river.JobArgs, *river.InsertOpts) { return sweepArgs{}, nil },
			// RunOnStart: every replica boot inserts one sweep; the
			// unique opts coalesce concurrent inserts, and sweeps are
			// idempotent and read-only by default, so the extra runs on
			// rolling restarts are harmless.
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
	if lfsReclaim != nil {
		river.AddWorker(workers, &lfsReclaimWorker{schedule: lfsReclaim})
		periodic = append(periodic, river.NewPeriodicJob(
			river.PeriodicInterval(lfsReclaim.Interval),
			func() (river.JobArgs, *river.InsertOpts) { return lfsReclaimArgs{}, nil },
			// RunOnStart like the sweep: unique opts coalesce concurrent
			// inserts, marking is idempotent, and retirement needs a
			// grace-expired mark — extra runs on rolling restarts are
			// harmless.
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
	if absence != nil {
		river.AddWorker(workers, &absenceReconcileWorker{schedule: absence})
		periodic = append(periodic, river.NewPeriodicJob(
			river.PeriodicInterval(absence.Interval),
			func() (river.JobArgs, *river.InsertOpts) { return absenceReconcileArgs{}, nil },
			// RunOnStart like the sweep: unique opts coalesce concurrent
			// inserts, and covers are same-key refreshes — extra runs on
			// rolling restarts are harmless.
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
	queues := map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 8}}
	if absence != nil {
		// Reserved queue — see absenceQueue: cover renewal is TTL-coupled
		// and must not wait behind tenant-scalable default-queue work.
		queues[absenceQueue] = river.QueueConfig{MaxWorkers: 1}
	}
	client, err := river.NewClient(driver, &river.Config{
		Queues:               queues,
		Workers:              workers,
		PeriodicJobs:         periodic,
		Logger:               logger,
		JobTimeout:           riverJobTimeout,
		RescueStuckJobsAfter: rescueAfter,
	})
	if err != nil {
		cancelRuns(nil)
		pool.Close()
		return nil, fmt.Errorf("jobs: river client: %w", err)
	}
	// Not ctx: serve's context ends at the termination signal, and River
	// cancels every running job when its start context ends. Shutdown goes
	// through DrainSyncRuns and Close instead (issue #343).
	if err := client.Start(context.WithoutCancel(ctx)); err != nil {
		cancelRuns(nil)
		pool.Close()
		return nil, fmt.Errorf("jobs: start river client: %w", err)
	}
	return &River{client: client, pool: pool, started: true, repackEnabled: repack != nil, cancelRuns: cancelRuns}, nil
}

// NewRiverInsertOnly connects an insert-only client (no migrations — hooks
// never run DDL — and no workers): the hook subprocess enqueues into the
// serve process's durable queue.
func NewRiverInsertOnly(ctx context.Context, pgURL string) (*River, error) {
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("jobs: open pgx pool: %w", err)
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: river insert-only client: %w", err)
	}
	return &River{client: client, pool: pool}, nil
}

// TracksCreateState: River runs only on PostgreSQL.
func (q *River) TracksCreateState() bool { return true }

// CompleteSyncCreate inserts the first-sync job and marks the create
// complete in one transaction on the shared database (issue #388).
func (q *River) CompleteSyncCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
	var out meta.CreateOutcome
	err := pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
		var err error
		out, err = meta.PGCompleteRepoCreate(ctx, tx, org, repoID, func(ctx context.Context) error {
			// The durable request (as SQLite records it) also fails a
			// detached repo, so the create never completes without a link.
			if err := meta.PGRequestSync(ctx, tx, repoID); err != nil {
				return err
			}
			_, err := q.client.InsertTx(ctx, tx, syncArgs{Org: org, RepoID: repoID}, nil)
			return err
		})
		return err
	})
	return out, err
}

func (q *River) EnqueueWebhookDelivery(ctx context.Context, d events.Delivery) error {
	assertf(json.Valid(d.Payload), "EnqueueWebhookDelivery: payload is not JSON")
	_, err := q.client.Insert(ctx, deliveryArgs{
		WebhookID: d.WebhookID,
		Event:     d.Event,
		Payload:   json.RawMessage(d.Payload),
	}, nil)
	return err
}

func (q *River) EnqueueSync(ctx context.Context, job sync.Job) error {
	_, err := q.client.Insert(ctx, syncArgs{Org: job.Org, RepoID: job.RepoID, Ref: job.Ref}, nil)
	return err
}

// EnqueuePush inserts one unique-by-repo outbound push job (issue #90).
// Insert works on insert-only hook clients too — workers are only needed
// to execute, not to enqueue.
func (q *River) EnqueuePush(ctx context.Context, job sync.Job) error {
	_, err := q.client.Insert(ctx, pushArgs{Org: job.Org, RepoID: job.RepoID, Retry: job.Retry}, nil)
	return err
}

// EnqueueRepack inserts one unique-by-repo repack job. Refused when this
// client has no repack worker registered: a durable job nobody works would
// sit queued forever AND its uniqueness would swallow every later insert.
func (q *River) EnqueueRepack(ctx context.Context, repo string) error {
	assertf(repo != "", "EnqueueRepack: empty repo")
	if !q.repackEnabled {
		return errors.New("jobs: repack worker not registered on this queue")
	}
	_, err := q.client.Insert(ctx, repackArgs{Repo: repo}, nil)
	return err
}

// DrainSyncRuns is serve's shutdown drain (issue #343), run while the
// listener still takes the runs' loopback pushes. River stops fetching
// jobs and waits up to wait for running ones. Then sync and push runs are
// cancelled with sync.ErrShutdown, so they release their claims and
// re-mark their requests without counting a run, and River keeps waiting
// until ctx ends; whatever still runs then (other job kinds) is cancelled
// as a plain stop. The worker keeps a cancelled run's job (shutdownSnooze),
// so the job, its Retry, and any request deduplicated against it pass to
// the next process or another replica. Close still runs afterwards.
func (q *River) DrainSyncRuns(ctx context.Context, wait time.Duration) {
	assertf(wait >= 0, "DrainSyncRuns: negative wait %v", wait)
	if !q.started {
		return
	}
	wctx, cancel := context.WithTimeout(ctx, wait)
	err := q.client.Stop(wctx) // soft: no new fetches, waits for running jobs
	cancel()
	if err == nil {
		return // every job finished while the listener served
	}
	q.cancelRuns(sync.ErrShutdown)
	// Soft again: cancelled runs restore and snooze their jobs first.
	if err := q.client.Stop(ctx); err != nil {
		_ = q.client.StopAndCancel(ctx) // cancels the rest; ctx is done
	}
}

// shutdownSnoozeFallback is shutdownSnooze's delay when it cannot read the
// row: long enough for any claim the run left behind to expire (claim
// leases stay below the snooze cap).
const shutdownSnoozeFallback = 30 * time.Minute

// shutdownSnooze keeps the job of a run that DrainSyncRuns cancelled before
// it committed an outcome: sync.Worker then returns an error wrapping
// sync.ErrShutdown, whatever its restore did (issue #343 review). A run
// that already counted its outcome returns a plain error and goes through
// the gate, so it is not rerun. A
// restored run left its request pending and no claim; the snooze then
// matches the gate's own uncovered-pending snooze. A failed restore left
// the claim held and the request consumed; the gate would complete the
// job (no pending mark), and the request would be lost once the claim
// expired. The delay therefore waits past any claim on the fresh row, and
// the next run steals it and does the work. No successor is inserted: this
// job stays the only one for the lane and repo, and its next run's claim
// covers every request marked before it.
func shutdownSnooze(ctx context.Context, upstreams sync.UpstreamStore, repoID string,
	delayFor func(sync.Upstream) time.Duration, logger *slog.Logger) error {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	delay := shutdownSnoozeFallback
	switch up, err := upstreams.GetUpstream(rctx, repoID); {
	case err == nil:
		delay = delayFor(up)
	case errors.Is(err, sync.ErrNotFound):
		return nil // detached: nothing to keep
	default:
		logger.Warn("shutdown: upstream read failed; snoozing past any claim", "repo", repoID, "err", err)
	}
	logger.Info("shutdown: keeping the cancelled run's job for the next process", "repo", repoID, "delay", delay)
	return river.JobSnooze(delay)
}

// withShutdown returns a sync or push run's context: cancelled with
// sync.ErrShutdown by DrainSyncRuns, and with the job context's cause and
// deadline otherwise. It derives from runs, not from the job context, so
// DrainSyncRuns's cancellation lands synchronously (context.AfterFunc runs
// its callback on another goroutine): a job-context cancellation right
// after it cannot win with a plain cause and make the run count as failed.
func withShutdown(ctx, runs context.Context) (context.Context, func()) {
	assertf(runs != nil, "withShutdown: nil runs context")
	rctx, cancel := context.WithCancelCause(runs)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	release := func() {
		stop()
		cancel(nil)
	}
	if d, ok := ctx.Deadline(); ok {
		dctx, dcancel := context.WithDeadline(rctx, d)
		return dctx, func() {
			dcancel()
			release()
		}
	}
	return rctx, release
}

// closeTimeout bounds Close on an exit that skipped DrainSyncRuns or ended it
// early (issue #636). It covers a cancelled run's restore and its snooze read
// (2 seconds each).
const closeTimeout = 5 * time.Second

// Close cancels the jobs still running, waits up to closeTimeout for them to
// record their state, and closes the pool. Sync and push runs are cancelled
// with sync.ErrShutdown and keep their jobs (shutdownSnooze). River makes the
// other cancelled jobs available again. A job still running after the wait
// keeps its row running until River rescues it (RescueStuckJobsAfter), as
// after a crash. The pool then stays open, since closing it waits for every
// connection that job holds.
func (q *River) Close() {
	if q.started {
		select {
		case <-q.client.Stopped(): // DrainSyncRuns finished every job
		default:
			q.cancelRuns(sync.ErrShutdown)
			ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			err := q.client.StopAndCancel(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
	q.pool.Close()
}
