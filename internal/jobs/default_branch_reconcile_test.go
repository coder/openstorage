package jobs

import (
	"context"
	"fmt"
	"log/slog"
	stdsync "sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/sync"
	"github.com/coder/openstorage/internal/testutil/testimages"
)

// reconcileDB boots PostgreSQL with the meta schema, plus a pool for the
// direct row edits that stand in for an older worker's runs.
func reconcileDB(t *testing.T) (context.Context, string, *meta.PG, *pgxpool.Pool) {
	t.Helper()
	if testing.Short() {
		t.Skip("needs docker")
	}
	ctx := context.Background()
	pgC, err := tcpostgres.Run(ctx, testimages.Postgres, tcpostgres.WithPassword("postgres"), tcpostgres.BasicWaitStrategies())
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
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return ctx, pgURL, ms, pool
}

// flaggedRepo creates a flagged GitHub sync repo whose upstream row shows
// the given success count and evidence, as an older worker's runs leave it.
func flaggedRepo(t *testing.T, ctx context.Context, ms *meta.PG, pool *pgxpool.Pool, name string, success, evidence int) string {
	t.Helper()
	r, err := ms.CreateLinkedRepo(ctx, "acme", name, "main",
		meta.RepoUpstream{Provider: "github", Owner: "o", Name: name, UpstreamHost: "github.com", DefaultBranchOmitted: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := setUpstreamColumns(ctx, pool, r.RepoID, success, evidence); err != nil {
		t.Fatal(err)
	}
	return r.RepoID
}

func setUpstreamColumns(ctx context.Context, pool *pgxpool.Pool, repoID string, success, evidence int) error {
	_, err := pool.Exec(ctx, `UPDATE repo_upstreams SET success_count = $2, default_branch_checked_success = $3, sync_pending = false WHERE repo_id = $1`, repoID, success, evidence)
	return err
}

// TestReconcileDefaultBranches (issue #609, case 1): one pass requests one
// sync for a flagged repo that an older worker synced, and the request and
// its job commit together. A repo with a request pending, evidence at its
// success count or no success gets none, and the batch is bounded.
func TestReconcileDefaultBranches(t *testing.T) {
	ctx, pgURL, ms, pool := reconcileDB(t)
	noop := func(context.Context, sync.Job) error { return nil }
	full, err := NewRiver(ctx, pgURL, &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)},
		noop, noop, sync.NewMemStore(), nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	full.Close() // migrations only: the insert-only client leaves jobs countable
	q, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Close)

	jobs := func(repoID string) int {
		t.Helper()
		var n int
		if err := q.pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE kind = 'upstream_sync' AND args->>'repo_id' = $1 AND args->>'org' = 'acme'`, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	pending := func(repoID string) bool {
		t.Helper()
		up, err := ms.GetRepoUpstream(ctx, repoID)
		if err != nil {
			t.Fatal(err)
		}
		return up.SyncPending
	}
	pass := func() int {
		t.Helper()
		n, err := reconcileDefaultBranches(ctx, q.pool, q.client)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	oldWorker := flaggedRepo(t, ctx, ms, pool, "old-worker", 1, 0)
	checked := flaggedRepo(t, ctx, ms, pool, "checked", 1, 1)
	fresh := flaggedRepo(t, ctx, ms, pool, "fresh", 0, 0)
	if n := pass(); n != 1 || jobs(oldWorker) != 1 || !pending(oldWorker) || jobs(checked)+jobs(fresh) != 0 {
		t.Fatalf("first pass requested %d (jobs %d, pending %v); want one sync for the older worker's repo only", n, jobs(oldWorker), pending(oldWorker))
	}
	if n := pass(); n != 0 || jobs(oldWorker) != 1 {
		t.Fatalf("second pass with the request pending requested %d, jobs %d; want none", n, jobs(oldWorker))
	}

	// A worker consumes the request. With no evidence the repo is asked for
	// again, and the job insert deduplicates against the one still queued.
	if err := setUpstreamColumns(ctx, pool, oldWorker, 1, 0); err != nil {
		t.Fatal(err)
	}
	if n := pass(); n != 1 || jobs(oldWorker) != 1 {
		t.Fatalf("pass after the request was consumed requested %d, jobs %d; want one request and one unique job", n, jobs(oldWorker))
	}
	// A worker with the rule leaves evidence: the requests stop.
	if err := setUpstreamColumns(ctx, pool, oldWorker, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := ms.RecordDefaultBranchChecked(ctx, oldWorker, 1); err != nil {
		t.Fatal(err)
	}
	if n := pass(); n != 0 {
		t.Fatalf("pass after the evidence requested %d, want none", n)
	}

	// The batch is bounded: the rest waits for the next pass.
	for i := range defaultBranchReconcileBatch + 1 {
		flaggedRepo(t, ctx, ms, pool, fmt.Sprintf("bulk-%03d", i), 1, 0)
	}
	if n := pass(); n != defaultBranchReconcileBatch {
		t.Fatalf("bulk pass requested %d, want the batch of %d", n, defaultBranchReconcileBatch)
	}
	if n := pass(); n != 1 {
		t.Fatalf("second bulk pass requested %d, want the 1 left over", n)
	}
}

// TestReconcileDefaultBranchesJobRuns: a River whose upstream store keeps the
// evidence runs the reconcile pass on its own queue at start, and the sync it
// requests reaches the sync runner.
func TestReconcileDefaultBranchesJobRuns(t *testing.T) {
	ctx, pgURL, ms, pool := reconcileDB(t)
	repoID := flaggedRepo(t, ctx, ms, pool, "wired", 1, 0)
	var mu stdsync.Mutex
	var ran []string
	runner := func(_ context.Context, job sync.Job) error {
		mu.Lock()
		defer mu.Unlock()
		ran = append(ran, job.Org+"/"+job.RepoID)
		return nil
	}
	noop := func(context.Context, sync.Job) error { return nil }
	_, ups := sync.MetaStores(ms)
	q, err := NewRiver(ctx, pgURL, &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)},
		runner, noop, ups, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	t.Cleanup(q.Close)
	deadline := time.Now().Add(90 * time.Second)
	for {
		mu.Lock()
		got := append([]string(nil), ran...)
		mu.Unlock()
		if len(got) > 0 {
			if got[0] != "acme/"+repoID {
				t.Fatalf("sync runner got %v, want acme/%s", got, repoID)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the reconcile job did not request a sync for the older worker's repo")
		}
		time.Sleep(100 * time.Millisecond)
	}
	var queue string
	if err := q.pool.QueryRow(ctx, `SELECT queue FROM river_job WHERE kind = 'default_branch_reconcile' LIMIT 1`).Scan(&queue); err != nil || queue != defaultBranchQueue {
		t.Fatalf("reconcile job queue = %q (%v), want %q", queue, err, defaultBranchQueue)
	}
}
