package jobs

import (
	"context"
	"log/slog"
	stdsync "sync"
	"sync/atomic"
	"testing"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/sync"
)

// TestRiverCompleteSyncCreate: the first-sync job and the complete mark
// commit together (issue #388): concurrent completions insert one job.
// The meta tests cover complete and deleted rows (no schedule step).
func TestRiverCompleteSyncCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("needs docker")
	}
	ctx := context.Background()
	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine", tcpostgres.WithPassword("postgres"), tcpostgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })
	pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	ms, err := meta.OpenPG(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, "acme", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatal(err)
	}
	// NewRiver runs River's migrations; the insert-only client then queues
	// jobs that no worker takes, so they stay countable.
	noop := func(context.Context, sync.Job) error { return nil }
	full, err := NewRiver(ctx, pgURL, &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)},
		noop, noop, sync.NewMemStore(), nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	full.Close()
	q, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)
	jobs := func(repoID string) int {
		t.Helper()
		var n int
		if err := q.pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE kind = 'upstream_sync' AND args->>'repo_id' = $1`, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	inproc := NewLocal(&events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}, noop, noop, nil, slog.New(slog.DiscardHandler))
	t.Cleanup(inproc.Close)
	if !q.TracksCreateState() || inproc.TracksCreateState() {
		t.Fatal("River tracks create state; the in-process queue (S3-WAL, #335) does not")
	}
	r, err := ms.CreateLinkedRepo(ctx, "acme", "race", "main",
		meta.RepoUpstream{Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var completed atomic.Int32 // 8 concurrent completions: one wins
	var wg stdsync.WaitGroup
	for range 8 {
		wg.Go(func() {
			out, err := q.CompleteSyncCreate(ctx, "acme", r.RepoID)
			if err != nil || (out != meta.CreateCompleted && out != meta.CreateAlreadyComplete) {
				t.Errorf("CompleteSyncCreate = %v, %v", out, err)
			} else if out == meta.CreateCompleted {
				completed.Add(1)
			}
		})
	}
	wg.Wait()
	if got := completed.Load(); got != 1 {
		t.Fatalf("%d completions won, want 1", got)
	}
	if got := jobs(r.RepoID); got != 1 {
		t.Fatalf("first-sync jobs = %d, want 1", got)
	}
	if got, err := ms.GetRepoByID(ctx, "acme", r.RepoID); err != nil || got.CreateState != meta.CreateStateComplete {
		t.Fatalf("repo = %+v, %v; want complete", got, err)
	}

}
