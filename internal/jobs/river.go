package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

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
	client  *river.Client[pgx.Tx]
	pool    *pgxpool.Pool
	started bool
}

var _ Queue = (*River)(nil)

// NewRiver opens a dedicated pool on pgURL, applies the river migrations
// (serve owns DDL, mirroring meta/ref InitSchema), and starts workers that
// share the delivery/sync code with the local queue.
func NewRiver(ctx context.Context, pgURL string, deliverer *events.Deliverer, runSync SyncRunner, logger *slog.Logger) (*River, error) {
	assertf(deliverer != nil, "NewRiver: nil deliverer")
	assertf(runSync != nil, "NewRiver: nil sync runner")
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
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: river migrations: %w", err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &deliveryWorker{deliverer: deliverer})
	river.AddWorker(workers, &syncWorker{run: runSync, logger: logger})
	client, err := river.NewClient(driver, &river.Config{
		Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 8}},
		Workers: workers,
		Logger:  logger,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: river client: %w", err)
	}
	if err := client.Start(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: start river client: %w", err)
	}
	return &River{client: client, pool: pool, started: true}, nil
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

// Close stops the workers (waiting for in-flight jobs) and closes the pool.
// Unfinished durable jobs remain in Postgres and resume on next start.
func (q *River) Close() {
	if q.started {
		_ = q.client.Stop(context.Background())
	}
	q.pool.Close()
}
