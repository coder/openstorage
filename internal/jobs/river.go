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

	"github.com/coder/code-storage/internal/events"
	"github.com/coder/code-storage/internal/sync"
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

func (syncArgs) InsertOpts() river.InsertOpts {
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

// sweepWorker runs one orphan-pack sweep; the Sweeper owns all reporting.
type sweepWorker struct {
	river.WorkerDefaults[sweepArgs]
	sweeper *Sweeper
}

func (w *sweepWorker) Work(ctx context.Context, _ *river.Job[sweepArgs]) error {
	_, err := w.sweeper.Sweep(ctx)
	return err
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

	runErr := w.run(ctx, sync.Job{Org: job.Args.Org, RepoID: job.Args.RepoID, Ref: job.Args.Ref})
	if runErr != nil {
		w.logger.Warn("sync run finished with error",
			"org", job.Args.Org, "repo", job.Args.RepoID, "err", runErr)
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
	case up.SyncPending && runErr != nil && preKnown && up.RunCount == preRuns && !up.Syncing:
		// The run failed BEFORE claiming (post-claim failures count the
		// run via FinishSync) and no claim holder is live to cover the
		// pending request — completing would strand the accepted 202.
		// Snooze retries the whole run, whose claim then consumes the
		// marker. (Returning without commit rolls the completion back.)
		w.logger.Warn("sync: run failed before claiming with a pending request; snoozing to retry it",
			"repo", job.Args.RepoID, "err", runErr)
		return river.JobSnooze(syncFollowUpSnooze)
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

// River is the durable Postgres queue (plan Decision 1). The serve process
// runs it in full mode (migrations + workers); hook subprocesses run
// insert-only clients against the same database, which is how git-surface
// events reach the server's workers across process boundaries.
type River struct {
	client        *river.Client[pgx.Tx]
	pool          *pgxpool.Pool
	started       bool
	repackEnabled bool
}

var _ Queue = (*River)(nil)

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
func NewRiver(ctx context.Context, pgURL string, deliverer *events.Deliverer, runSync SyncRunner, upstreams sync.UpstreamStore, sweep *SweepSchedule, repack *Repacker, logger *slog.Logger) (*River, error) {
	assertf(deliverer != nil, "NewRiver: nil deliverer")
	assertf(runSync != nil, "NewRiver: nil sync runner")
	assertf(upstreams != nil, "NewRiver: nil upstream store")
	assertf(sweep == nil || (sweep.Sweeper != nil && sweep.Interval > 0),
		"NewRiver: sweep schedule missing sweeper or interval: %+v", sweep)
	assertf(logger != nil, "NewRiver: nil logger")

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

	workers := river.NewWorkers()
	river.AddWorker(workers, &deliveryWorker{deliverer: deliverer})
	river.AddWorker(workers, &syncWorker{run: runSync, upstreams: upstreams, pool: pool, logger: logger})
	if repack != nil {
		river.AddWorker(workers, &repackWorker{repacker: repack, pool: pool})
	}
	var periodic []*river.PeriodicJob
	if sweep != nil {
		river.AddWorker(workers, &sweepWorker{sweeper: sweep.Sweeper})
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
	client, err := river.NewClient(driver, &river.Config{
		Queues:       map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 8}},
		Workers:      workers,
		PeriodicJobs: periodic,
		Logger:       logger,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: river client: %w", err)
	}
	if err := client.Start(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: start river client: %w", err)
	}
	return &River{client: client, pool: pool, started: true, repackEnabled: repack != nil}, nil
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

// Close stops the workers (waiting for in-flight jobs) and closes the pool.
// Unfinished durable jobs remain in Postgres and resume on next start.
func (q *River) Close() {
	if q.started {
		_ = q.client.Stop(context.Background())
	}
	q.pool.Close()
}
