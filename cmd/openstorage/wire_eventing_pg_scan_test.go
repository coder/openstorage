package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/testutil/testimages"
)

// TestServeRequeuesLanesWithoutJob: on PostgreSQL, serve's lane scan gives
// a push or pull lane whose durable work lost its River job (River
// discarded it, or its row was deleted) a new job. Without the scan, the
// request waits for an unrelated later trigger.
func TestServeRequeuesLanesWithoutJob(t *testing.T) {
	if testing.Short() {
		t.Skip("needs docker")
	}
	ctx := t.Context()
	pgC, err := tcpostgres.Run(ctx, testimages.Postgres,
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })
	dbURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	ms, err := meta.Open(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	key, pub, err := newLoopbackKey()
	if err != nil {
		t.Fatal(err)
	}
	const org = "org"
	if _, err := ms.UpsertOrgWithKeys(ctx, org, []string{pub}); err != nil {
		t.Fatal(err)
	}
	linked := func(name string) meta.Repo {
		t.Helper()
		repo, err := ms.CreateRepo(ctx, org, "https://example.com/"+name+".git", "main")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{
			RepoID: repo.RepoID, Provider: "github", Owner: "up", Name: name,
			UpstreamHost: "https://example.invalid",
		}); err != nil {
			t.Fatal(err)
		}
		return repo
	}
	// Durable requests with no job: as if River had discarded their jobs.
	pushRepo, pullRepo := linked("push"), linked("pull")
	if err := ms.RequestPush(ctx, pushRepo.RepoID, false); err != nil {
		t.Fatal(err)
	}
	if err := ms.RequestSync(ctx, pullRepo.RepoID); err != nil {
		t.Fatal(err)
	}

	_, _, cleanup, err := buildServeEventing(ctx, ms, dbURL, org, "http://127.0.0.1:1", t.TempDir(), key,
		nil, nil, nil, nil, false, syncEgressSettings{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	jobs := func(kind, repoID string) int {
		t.Helper()
		var n int
		if err := conn.QueryRow(ctx,
			`SELECT count(*) FROM river_job WHERE kind = $1 AND args->>'repo_id' = $2`, kind, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// The first pass runs pendingSyncRecoveryDelay after the build.
	deadline := time.Now().Add(pendingSyncRecoveryDelay + time.Minute)
	for jobs("upstream_push", pushRepo.RepoID) == 0 || jobs("upstream_sync", pullRepo.RepoID) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("stranded lanes got no job: %d push jobs, %d sync jobs",
				jobs("upstream_push", pushRepo.RepoID), jobs("upstream_sync", pullRepo.RepoID))
		}
		time.Sleep(100 * time.Millisecond)
	}
}
