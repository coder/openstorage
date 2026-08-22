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

	"codestorage/internal/events"
	"codestorage/internal/sync"
)

// deliveryArgs is one durable webhook-delivery job. The payload is the
// exact JSON body frozen at enqueue time (docs/06: only the signature is
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
// its job TRANSACTIONALLY (river.JobCompleteTx) and only then rechecks the
// threshold, inserting a durable successor when more folding is due: every
// registration commits either before that post-completion recheck (the
// successor covers it) or after it (its own trigger inserts successfully,
// the running row being gone) — no ordering strands a swallowed trigger.
type repackArgs struct {
	Repo string `json:"repo" river:"unique"`
}

func (repackArgs) Kind() string { return "repack" }

func (repackArgs) InsertOpts() river.InsertOpts {
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

// Work folds once, then closes the trigger-uniqueness window ATOMICALLY:
// one transaction completes the job (JobCompleteTx) and inserts an
// unconditional successor — the completion removes this row from the
// unique-active-states index, so the successor's insert in the same
// transaction succeeds, and either both are durable or neither is. Every
// registration whose trigger was coalesced against this running job is
// therefore covered: the successor exists the instant the completion
// commits, with no fallible check-and-insert window in between (a crash,
// canceled context, or transient store failure there previously left no
// active job to recover). The successor simply re-runs Repack: it folds
// whatever is foldable and no-ops below two packs (or when budgets select
// no pair), completing WITHOUT a successor of its own — chains terminate.
// Lost races and no-ops schedule nothing: a loss proves a concurrent fold
// won, and that winner's own completion carries the successor.
func (w *repackWorker) Work(ctx context.Context, job *river.Job[repackArgs]) error {
	folded, err := w.repacker.Repack(ctx, job.Args.Repo)
	if !folded {
		return err // real failures and clean no-ops/lost races (err == nil)
	}
	if err != nil {
		// The fold committed (or may have) but verification failed
		// transiently: the row swap is durable and the sweeper's
		// quarantine heal backstops the bytes. Failing this
		// MaxAttempts-1 job would skip the successor and strand
		// coalesced mid-fold triggers — log and continue to the atomic
		// completion below.
		w.repacker.logger.Warn("repack: fold committed but verification failed (sweeper heal is the backstop)",
			"repo", job.Args.Repo, "err", err)
	}
	assertf(w.pool != nil, "repackWorker: pool not wired")
	// Failures anywhere in the completion transaction SNOOZE the job:
	// returning nil would let river complete it without the successor
	// ever existing. The snoozed re-run costs one idempotent no-op fold
	// and retries the completion; if the commit failure was ambiguous
	// (it actually landed, successor included), the resurrected job
	// no-ops and completes normally.
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
	if _, err := river.ClientFromContext[pgx.Tx](ctx).InsertTx(ctx, tx, repackArgs{Repo: job.Args.Repo},
		&river.InsertOpts{ScheduledAt: time.Now().Add(repackFollowUpDelay)}); err != nil {
		return completionFailed("insert successor", err)
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

// syncWorker executes one sync run. Run's error return is for logging only
// (documented failure outcomes already emitted repo.sync.failed), so the
// job always completes.
type syncWorker struct {
	river.WorkerDefaults[syncArgs]
	run    SyncRunner
	logger *slog.Logger
}

func (w *syncWorker) Work(ctx context.Context, job *river.Job[syncArgs]) error {
	if err := w.run(ctx, sync.Job{Org: job.Args.Org, RepoID: job.Args.RepoID, Ref: job.Args.Ref}); err != nil {
		w.logger.Warn("sync run finished with error",
			"org", job.Args.Org, "repo", job.Args.RepoID, "err", err)
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
// share the delivery/sync code with the local queue. A non-nil sweep
// schedules the periodic orphan-pack sweep (run once on start, then every
// Interval); nil disables it (hook subprocesses, tests without storage).
// A non-nil repack registers the background-repack worker; nil rejects
// EnqueueRepack (repacking disabled or not wired, e.g. insert-only hook
// clients that must never fold).
func NewRiver(ctx context.Context, pgURL string, deliverer *events.Deliverer, runSync SyncRunner, sweep *SweepSchedule, repack *Repacker, logger *slog.Logger) (*River, error) {
	assertf(deliverer != nil, "NewRiver: nil deliverer")
	assertf(runSync != nil, "NewRiver: nil sync runner")
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
	river.AddWorker(workers, &syncWorker{run: runSync, logger: logger})
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
