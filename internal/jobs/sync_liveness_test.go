package jobs

// Issue #79 liveness on the River lane: syncArgs uniqueness spans the
// running state (river requires it), so a pull-upstream enqueue while a
// sync job runs is silently deduplicated against it — even though that
// run's upstream fetch may already have happened. The producer marks the
// request durable first (RequestSync); the sync worker's gated
// transactional completion must then schedule the successor run.

import (
	"context"
	"errors"
	"log/slog"
	stdsync "sync"
	"sync/atomic"
	"testing"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/code-storage/internal/events"
	"github.com/coder/code-storage/internal/sync"
)

func TestRiverSyncRequestDuringRunningJobSchedulesSuccessor(t *testing.T) {
	if testing.Short() {
		t.Skip("river successor test needs docker")
	}
	ctx := context.Background()
	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	defer func() { _ = pgC.Terminate(ctx) }()
	pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_live", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatalf("PutUpstream: %v", err)
	}

	// The runner mimics sync.Worker.Run's claim lifecycle (invariant 11)
	// without git transfers: claim or skip, hold the FIRST claimed run
	// until released, finish.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once stdsync.Once
	var claimedRuns atomic.Int32
	runner := func(ctx context.Context, job sync.Job) error {
		if _, err := store.BeginSync(ctx, job.RepoID); err != nil {
			if errors.Is(err, sync.ErrSyncInProgress) {
				return nil // overlapping run: skip, like the real worker
			}
			return err
		}
		claimedRuns.Add(1)
		once.Do(func() {
			close(entered)
			<-release
		})
		_, err := store.FinishSync(ctx, job.RepoID, true)
		return err
	}

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, runner, store, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_live"}); err != nil {
		t.Fatalf("enqueue first sync: %v", err)
	}
	<-entered // job 1 claimed and is now held mid-run

	// The pull-upstream 202 path: the request is marked durable, then the
	// enqueue deduplicates against job 1's running row and is dropped.
	if err := store.RequestSync(ctx, "repo_live"); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_live"}); err != nil {
		t.Fatalf("enqueue during running job: %v", err)
	}
	close(release) // job 1 finishes; its gated completion owns the follow-up

	// Liveness: the marked request must be observed by a successor run.
	waitFor(t, "successor sync run", func() bool { return claimedRuns.Load() >= 2 })
	up, err := store.GetUpstream(ctx, "repo_live")
	if err != nil {
		t.Fatalf("GetUpstream: %v", err)
	}
	if up.SyncPending {
		t.Errorf("SyncPending still set after the successor run: %+v", up)
	}
}
