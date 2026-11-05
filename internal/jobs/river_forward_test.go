package jobs

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/sync"
)

// TestRiverRecordForwardTxWaitsForJobBeforeUpstreamRow: while a push job's
// completion transaction holds the job row, RecordForwardTx waits for it
// without holding the upstream row. That completion reads the upstream row
// through another pool, whose connections a statement waiting on that row
// could all hold.
func TestRiverRecordForwardTxWaitsForJobBeforeUpstreamRow(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	ms, err := meta.Open(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	priv, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.MarshalPublicKeyPEM(priv.Public())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, "org", []string{string(pub)}); err != nil {
		t.Fatal(err)
	}
	repo, err := ms.CreateRepo(ctx, "org", "https://example.com/lock-order.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: repo.RepoID, Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test", ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	// Migrate River, then insert without workers, so the job stays put.
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	noRun := func(context.Context, sync.Job) error { return nil }
	full, err := NewRiver(ctx, pgURL, deliverer, noRun, noRun, sync.NewMemStore(), nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	full.Close()
	q, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	job := sync.Job{Org: "org", RepoID: repo.RepoID}
	if err := q.EnqueuePush(ctx, job); err != nil {
		t.Fatal(err)
	}

	connect := func() *pgx.Conn {
		t.Helper()
		c, err := pgx.Connect(ctx, pgURL)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c
	}
	completion, err := connect().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = completion.Rollback(ctx) }()
	if _, err := completion.Exec(ctx,
		`SELECT id FROM river_job WHERE kind = 'upstream_push' AND args->>'repo_id' = $1 FOR UPDATE`, repo.RepoID); err != nil {
		t.Fatal(err)
	}

	recorded := make(chan error, 1)
	go func() {
		recorded <- pgx.BeginFunc(ctx, connect(), func(tx pgx.Tx) error {
			ok, err := q.RecordForwardTx(ctx, tx, job)
			if err == nil && !ok {
				err = errors.New("RecordForwardTx recorded nothing")
			}
			return err
		})
	}()
	probe := connect()
	var waiting bool
	for deadline := time.Now().Add(30 * time.Second); !waiting; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("RecordForwardTx never waited for the held job row")
		}
		if err := probe.QueryRow(ctx,
			`SELECT count(*) > 0 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%river_job%'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := probe.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `SELECT 1 FROM repo_upstreams WHERE repo_id = $1 FOR UPDATE NOWAIT`, repo.RepoID)
	_ = tx.Rollback(ctx)
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "55P03" {
		t.Fatal("RecordForwardTx holds the upstream row while it waits for the job row")
	} else if err != nil {
		t.Fatal(err)
	}
	if err := completion.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-recorded; err != nil {
		t.Fatalf("RecordForwardTx after the completion: %v", err)
	}
	up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
	if err != nil || up.PushPending != sync.PushPendingPush {
		t.Fatalf("forward intent not recorded: %+v, %v", up, err)
	}
}
