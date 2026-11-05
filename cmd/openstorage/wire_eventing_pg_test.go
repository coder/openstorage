package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
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
