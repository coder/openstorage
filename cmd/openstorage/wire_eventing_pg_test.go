package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	upsync "github.com/coder/openstorage/internal/sync"
	"github.com/coder/openstorage/internal/testutil/pgfault"
	"github.com/coder/openstorage/internal/testutil/testimages"
)

// diedAfterCAS stands for a serve process that dies right after its ref
// CAS commits: the swap runs, and the caller never learns that it won.
// The error is definitive (SafeToRetry), so ref.IdempotentCAS does not
// recover the win and the decorator's sync poke never runs.
type diedAfterCAS struct{ ref.RefStore }

type diedAfterCASError struct{}

func (diedAfterCASError) Error() string     { return "process died after the CAS" }
func (diedAfterCASError) SafeToRetry() bool { return true }

func (s diedAfterCAS) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	if won, err := s.RefStore.CASRef(ctx, repo, name, old, new); err != nil || !won {
		return false, err
	}
	return false, diedAfterCASError{}
}

func (s diedAfterCAS) CASRefGen(ctx context.Context, repo, name, old, new string) (bool, int64, error) {
	if won, _, err := s.RefStore.CASRefGen(ctx, repo, name, old, new); err != nil || !won {
		return false, 0, err
	}
	return false, 0, diedAfterCASError{}
}

// TestServeRecordsForwardIntentWithTheCAS: on PostgreSQL a REST write
// through the serve decorator records a forward_on_push repository's
// forward intent in the CAS transaction. A process that dies right after
// the CAS commits leaves the push job behind; a caller that may not
// forward leaves nothing.
func TestServeRecordsForwardIntentWithTheCAS(t *testing.T) {
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
	rs, err := ref.OpenPG(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rs.Close)
	if err := rs.InitSchema(ctx); err != nil {
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
	logger := slog.New(slog.DiscardHandler)
	_, decorate, cleanup, err := buildServeEventing(ctx, ms, dbURL, org, "http://127.0.0.1:1", t.TempDir(), key,
		nil, nil, nil, nil, false, syncEgressSettings{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	store := decorate(diedAfterCAS{rs})

	pool, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	pushJobs := func(repoID string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM river_job WHERE kind = 'upstream_push' AND args->>'repo_id' = $1`, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	forwardRepo := func(t *testing.T, name string) meta.Repo {
		t.Helper()
		repo, err := ms.CreateRepo(ctx, org, "https://example.com/"+name+".git", "main")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{
			RepoID: repo.RepoID, Provider: "github", Owner: "up", Name: "mirror",
			UpstreamHost: "https://example.invalid", ForwardOnPush: true,
		}); err != nil {
			t.Fatal(err)
		}
		return repo
	}
	claims := func(repo meta.Repo, scopes ...string) context.Context {
		return auth.WithClaims(ctx, &auth.Claims{Iss: org, Sub: auth.SubjectLiteral, Repo: repo.URL, Scopes: scopes})
	}
	const sha = "1111111111111111111111111111111111111111"

	for _, tc := range []struct {
		name     string
		scopes   []string
		wantJobs bool
	}{
		{"may_forward", []string{auth.ScopeGitRead, auth.ScopeGitWrite}, true},
		{"write_only", []string{auth.ScopeGitWrite}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := forwardRepo(t, tc.name)
			if won, err := store.CASRef(claims(repo, tc.scopes...), repo.RepoID, "refs/heads/main", ref.ZeroSHA, sha); won || err == nil {
				t.Fatalf("CASRef: won=%v err=%v, want the injected death", won, err)
			}
			if got, err := rs.GetRef(ctx, repo.RepoID, "refs/heads/main"); err != nil || got != sha {
				t.Fatalf("authority main = %q (err %v), want the committed %s", got, err, sha)
			}
			up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
			if err != nil {
				t.Fatal(err)
			}
			// The node's worker may already have claimed the job.
			if n := pushJobs(repo.RepoID); tc.wantJobs && n != 1 {
				t.Fatalf("intent lost with the process: %d push jobs, upstream %+v", n, up)
			} else if !tc.wantJobs && (n != 0 || up.PushPending != 0) {
				t.Fatalf("a write-only caller recorded forward intent: %d push jobs, upstream %+v", n, up)
			}
		})
	}

	// Ephemeral writes (a ref in the ephemeral namespace, or the repo's
	// +ephemeral key) are never forwarded, so their CAS records nothing.
	for _, tc := range []struct{ name, key, ref string }{
		{"ephemeral_ref", "", auth.EphemeralNamespacePrefix + "refs/heads/scratch"},
		{"ephemeral_repo_key", "+ephemeral", "refs/heads/main"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := forwardRepo(t, strings.ReplaceAll(tc.name, "_", "-"))
			if won, err := store.CASRef(claims(repo, auth.ScopeGitRead, auth.ScopeGitWrite), repo.RepoID+tc.key, tc.ref, ref.ZeroSHA, sha); won || err == nil {
				t.Fatalf("CASRef: won=%v err=%v, want the injected death", won, err)
			}
			if got, err := rs.GetRef(ctx, repo.RepoID+tc.key, tc.ref); err != nil || got != sha {
				t.Fatalf("authority %s = %q (err %v), want the committed %s", tc.ref, got, err, sha)
			}
			up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
			if err != nil {
				t.Fatal(err)
			}
			if n := pushJobs(repo.RepoID); n != 0 || up.PushPending != 0 {
				t.Fatalf("an ephemeral write recorded forward intent: %d push jobs, upstream %+v", n, up)
			}
		})
	}

	// A run that claims the new job before the decorator's sync poke runs
	// has consumed the intent and snapshots the new ref. The poke must not
	// mark the level again: that would schedule a second forward run.
	t.Run("claimed_before_poke", func(t *testing.T) {
		repo := forwardRepo(t, "claimed-before-poke")
		st := decorate(claimAfterCAS{RefStore: rs, ms: ms})
		if won, err := st.CASRef(claims(repo, auth.ScopeGitRead, auth.ScopeGitWrite), repo.RepoID, "refs/heads/main", ref.ZeroSHA, sha); !won || err != nil {
			t.Fatalf("CASRef: won=%v err=%v", won, err)
		}
		up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
		if err != nil {
			t.Fatal(err)
		}
		if up.PushPending != 0 || up.PushRunCount == 0 && !up.PushSyncing {
			t.Fatalf("the poke re-marked a consumed forward: %+v", up)
		}
	})

	// A caller that may not forward already wrote the same value, so this
	// CAS loses and ref.IdempotentCAS reports a win it did not commit. Its
	// transaction recorded nothing, so the poke must forward.
	t.Run("recovered_twin", func(t *testing.T) {
		repo := forwardRepo(t, "recovered-twin")
		const base = "2222222222222222222222222222222222222222"
		for _, step := range [][2]string{{ref.ZeroSHA, base}, {base, sha}} {
			if won, err := rs.CASRef(ctx, repo.RepoID, "refs/heads/main", step[0], step[1]); !won || err != nil {
				t.Fatalf("seed %s: won=%v err=%v", step[1], won, err)
			}
		}
		if won, err := decorate(rs).CASRef(claims(repo, auth.ScopeGitRead, auth.ScopeGitWrite), repo.RepoID, "refs/heads/main", base, sha); !won || err != nil {
			t.Fatalf("CASRef: won=%v err=%v, want a recovered win", won, err)
		}
		if n := pushJobs(repo.RepoID); n != 1 {
			t.Fatalf("recovered win scheduled %d push jobs, want 1", n)
		}
	})
}

// claimAfterCAS claims the push lane right after the CAS commits, as a
// River run that takes the new job at once does. The node's own worker
// may get there first; either way a run has consumed the intent.
type claimAfterCAS struct {
	ref.RefStore
	ms meta.Backend
}

func (s claimAfterCAS) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	won, err := s.RefStore.CASRef(ctx, repo, name, old, new)
	if err != nil || !won {
		return won, err
	}
	if _, _, _, _, err := s.ms.TryBeginPush(ctx, repo, "claimed-before-poke", time.Hour); err != nil {
		return false, fmt.Errorf("claim after the CAS: %w", err)
	}
	return true, nil
}

// TestHookRecordsNoForwardForSyncLoopbackPush: on PostgreSQL the update
// hook of a sync run's own loopback push (githttp.EnvSyncClaim set) records
// no forward intent in its CAS. Forwarding the mirror's update back to the
// upstream would chain an outbound run onto every inbound one. An ordinary
// push from a caller that may forward records the intent.
func TestHookRecordsNoForwardForSyncLoopbackPush(t *testing.T) {
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
	rs, err := ref.OpenPG(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rs.Close)
	if err := rs.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	key, pub, err := newLoopbackKey()
	if err != nil {
		t.Fatal(err)
	}
	const org = "hook-org" // hookEventingEnv's org
	if _, err := ms.UpsertOrgWithKeys(ctx, org, []string{pub}); err != nil {
		t.Fatal(err)
	}
	// Serve owns the River migrations. Close it again: no worker may take
	// the jobs this test counts.
	_, _, serveCleanup, err := buildServeEventing(ctx, ms, dbURL, org, "http://127.0.0.1:1", t.TempDir(), key,
		nil, nil, nil, nil, false, syncEgressSettings{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	serveCleanup()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	for _, tc := range []struct {
		name, holder string
		want         int // push jobs and push_pending after the CAS
	}{
		{"ordinary_push", "", 1},
		{"sync_loopback_push", "sync-holder", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, err := ms.CreateRepo(ctx, org, "https://example.com/"+strings.ReplaceAll(tc.name, "_", "-")+".git", "main")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{
				RepoID: repo.RepoID, Provider: "github", Owner: "up", Name: "mirror",
				UpstreamHost: "https://example.invalid", ForwardOnPush: true,
			}); err != nil {
				t.Fatal(err)
			}
			hookEventingEnv(t, dbURL)
			t.Setenv(githttp.EnvScopes, `["git:read","git:write"]`)
			t.Setenv(githttp.EnvSyncClaim, tc.holder)
			cleanup := wireHookEventing(ctx, slog.New(slog.DiscardHandler))
			defer cleanup()
			cctx := ctx
			if tc.holder != "" {
				// The loopback push's CAS is fenced by the run's sync claim.
				if _, claimed, _, err := ms.TryBeginSync(ctx, repo.RepoID, tc.holder, time.Hour); err != nil || !claimed {
					t.Fatalf("claim the sync lane: claimed=%v err=%v", claimed, err)
				}
				cctx = ref.WithSyncClaimFence(ctx, repo.RepoID, tc.holder)
			}
			store, err := githttp.WrapHookRefStore(ctx, rs)
			if err != nil {
				t.Fatal(err)
			}
			if won, err := store.CASRef(cctx, repo.RepoID, "refs/heads/main", ref.ZeroSHA, strings.Repeat("a", 40)); err != nil || !won {
				t.Fatalf("CASRef: won=%v err=%v", won, err)
			}
			var jobs int
			if err := conn.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE kind = 'upstream_push' AND args->>'repo_id' = $1`,
				repo.RepoID).Scan(&jobs); err != nil {
				t.Fatal(err)
			}
			up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
			if err != nil {
				t.Fatal(err)
			}
			if jobs != tc.want || up.PushPending != tc.want {
				t.Fatalf("push jobs %d, push_pending %d; want %d and %d", jobs, up.PushPending, tc.want, tc.want)
			}
		})
	}
}

// TestServeSkipsPokeAfterLostCommitReply: the CAS of a forward_on_push
// repository commits its forward intent, the COMMIT reply is lost, and a
// run claims the new job before the CAS learns the outcome. The CAS must
// report the win that its transaction committed, and the decorator's sync
// poke must see that the transaction recorded the forward and must not mark
// the level again: that would schedule a second forward run.
func TestServeSkipsPokeAfterLostCommitReply(t *testing.T) {
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
	proxy, proxied := pgfault.Start(t, dbURL)
	rs, err := ref.OpenPG(ctx, proxied) // the ref store's connections run through the proxy
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rs.Close)
	if err := rs.InitSchema(ctx); err != nil {
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
	_, decorate, cleanup, err := buildServeEventing(ctx, ms, dbURL, org, "http://127.0.0.1:1", t.TempDir(), key,
		nil, nil, nil, nil, false, syncEgressSettings{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	repo, err := ms.CreateRepo(ctx, org, "https://example.com/lost-commit-reply.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: repo.RepoID, Provider: "github", Owner: "up", Name: "mirror",
		UpstreamHost: "https://example.invalid", ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	// A run takes the new job between the commit and the recovery.
	claimed := make(chan error, 1)
	proxy.Arm(pgfault.DropReply, func() {
		_, ok, _, _, err := ms.TryBeginPush(context.Background(), repo.RepoID, "run-1", time.Hour)
		if err == nil && !ok {
			err = fmt.Errorf("the push lane was not claimable")
		}
		claimed <- err
	})

	cctx := auth.WithClaims(ctx, &auth.Claims{Iss: org, Sub: auth.SubjectLiteral, Repo: repo.URL,
		Scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
	const sha = "1111111111111111111111111111111111111111"
	if won, err := decorate(rs).CASRef(cctx, repo.RepoID, "refs/heads/main", ref.ZeroSHA, sha); err != nil || !won {
		t.Fatalf("CASRef = %v, %v; want a win recovered after the lost COMMIT reply", won, err)
	}
	select {
	case err := <-claimed:
		if err != nil {
			t.Fatalf("claim after the commit: %v", err)
		}
	default:
		t.Fatal("the COMMIT reply was never dropped")
	}
	up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending != 0 || !up.PushSyncing {
		t.Fatalf("the poke re-marked a forward the committed CAS recorded: %+v", up)
	}
}

// TestHookEventingRiverFailureRefusesAndCleansUp: on PostgreSQL, when the
// hook's River client cannot be built after its metadata store opened, the
// hook refuses an update that could record forward intent, and it closes
// the metadata store's connections.
func TestHookEventingRiverFailureRefusesAndCleansUp(t *testing.T) {
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
	const app = "hook_river_failure"
	logs := hookEventingEnv(t, dbURL+"&application_name="+app)
	original := newHookRiver
	t.Cleanup(func() { newHookRiver = original })
	newHookRiver = func(context.Context, string) (*jobs.River, error) {
		return nil, errors.New("river client unavailable")
	}
	cleanup := wireHookEventing(ctx, slog.New(githttp.NewHookLogHandler(logs, nil)))
	defer cleanup()
	if !strings.Contains(logs.String(), "hook eventing unavailable: river insert-only client; refusing only updates that could record forward intent") {
		t.Fatalf("missing refusal log: %s", logs)
	}
	rs := hookRefStore(t)
	wrapped, err := githttp.WrapHookRefStore(ctx, rs)
	if err != nil {
		t.Fatal(err)
	}
	const sha = "1111111111111111111111111111111111111111"
	if won, _, err := wrapped.CASRefGen(ctx, "repo-1", "refs/heads/main", ref.ZeroSHA, sha); won || err == nil ||
		!strings.HasSuffix(err.Error(), "refusing the ref update: hook eventing unavailable (river insert-only client); push again later") {
		t.Fatalf("CAS = (%v, %v); want the update refused before its CAS", won, err)
	}
	// The metadata store opened (meta.Open pings, and the pool keeps that
	// connection); the failed wiring must have closed it.
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var n int
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`, app).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 || time.Now().After(deadline) {
			break
		}
	}
	if n != 0 {
		t.Fatalf("%d metadata store connections still open after the failed wiring", n)
	}
}

// The hook's River client writes River's lines only to serve's hook-errors
// file: git relays the hook's stdout and stderr to the pusher, and River's
// insert-notification failure carries the database error as a string.
func TestHookRiverLogsOnlyToHookErrors(t *testing.T) {
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
	key, _, err := newLoopbackKey()
	if err != nil {
		t.Fatal(err)
	}
	// serve's wiring migrates River.
	_, _, cleanup, err := buildServeEventing(ctx, ms, dbURL, "org", "http://127.0.0.1:1", t.TempDir(), key,
		nil, nil, nil, nil, false, syncEgressSettings{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	cleanup()

	// A pg_notify that fails with a canary, ahead of pg_catalog's on the
	// search path, as a database whose notify fails would.
	schema := "canary_" + strings.ToLower(rand.Text()[:8])
	canary := "canary-host-" + strings.ToLower(rand.Text()[:8]) + ".invalid"
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{
		`CREATE SCHEMA ` + schema,
		`CREATE FUNCTION ` + schema + `.pg_notify(text, text) RETURNS void LANGUAGE plpgsql AS
			$$ BEGIN RAISE EXCEPTION 'could not reach ` + canary + `'; END $$`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema+",pg_catalog,public")
	u.RawQuery = q.Encode()

	hookEventingEnv(t, dbURL)
	out, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	prevOut, prevErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = out, out
	river, err := newHookRiver(ctx, u.String())
	if err == nil {
		err = river.EnqueueSync(ctx, upsync.Job{Org: "org", RepoID: "repo-1"})
		river.Close()
	}
	os.Stdout, os.Stderr = prevOut, prevErr
	if err == nil {
		t.Fatal("EnqueueSync succeeded with a failing pg_notify; want the canary error")
	}
	b, rerr := os.ReadFile(out.Name())
	if rerr != nil {
		t.Fatal(rerr)
	}
	if n := strings.Count(string(b), canary); n != 0 {
		t.Fatalf("stdout and stderr name the canary %d times: %s", n, b)
	}
	if !strings.Contains(hookErrorRecords(t), canary) {
		t.Fatalf("the hook-errors file lacks River's notify failure: %s", hookErrorRecords(t))
	}
}

// runHook makes the hook logger the default before it wires eventing, so a
// line that the wiring's callees log through the default logger (River,
// the store packages) keeps its error value off stderr too.
func TestRunHookSetsTheDefaultLoggerBeforeWiring(t *testing.T) {
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
	hookEventingEnv(t, dbURL)
	canary := "canary-host-" + strings.ToLower(rand.Text()[:8]) + ".invalid"
	original := newHookRiver
	t.Cleanup(func() { newHookRiver = original })
	newHookRiver = func(context.Context, string) (*jobs.River, error) {
		slog.Warn("river client failed through the default logger", "err", errors.New("dial "+canary))
		return nil, errors.New("river client unavailable")
	}
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	var stderr bytes.Buffer
	if err := runHook(ctx, []string{"no-such-hook"}, &stderr); err == nil {
		t.Fatal("runHook ran an unknown hook")
	}
	if !strings.Contains(stderr.String(), "river client failed through the default logger") || strings.Contains(stderr.String(), canary) {
		t.Fatalf("stderr = %q; want the default logger's line without the canary", stderr.String())
	}
	if !strings.Contains(hookErrorRecords(t), canary) {
		t.Fatalf("the hook-errors file lacks the default logger's line: %s", hookErrorRecords(t))
	}
}
