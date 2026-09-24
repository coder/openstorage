package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
	upsync "github.com/coder/openstorage/internal/sync"
)

// cancelAfterCAS reproduces a client disappearing AFTER the authority commit.
// The real SQLite CAS decides the write; cancellation must not skip the marker.
type cancelAfterCAS struct {
	ref.RefStore
	cancel context.CancelFunc
}

func (s cancelAfterCAS) CASRef(ctx context.Context, repo, name, old, next string) (bool, error) {
	won, err := s.RefStore.CASRef(ctx, repo, name, old, next)
	s.cancel()
	return won, err
}

func TestLocalHookIntentAuthorizationAndFailures(t *testing.T) {
	const rw = `["git:read","git:write"]`
	for _, tc := range []struct {
		name, scopes, refs, ops, claim            string
		forward, syncing, disconnect, storeClosed bool
		wantPull, wantPush                        bool
	}{
		{name: "forward", scopes: rw, forward: true, wantPush: true},
		{name: "write_only", scopes: `["git:write"]`, forward: true},
		{name: "no_scopes", forward: true},
		{name: "ref_policy", scopes: rw, refs: `[["refs/heads/other",["no-push"]]]`, forward: true},
		{name: "ops", scopes: rw, ops: `["no-force-push"]`, forward: true},
		{name: "malformed_refs", scopes: rw, refs: `{bad`, forward: true},
		{name: "malformed_ops", scopes: rw, ops: `{bad`, forward: true},
		{name: "malformed_scopes", scopes: `{bad`, forward: true},
		{name: "loopback_forward", scopes: rw, claim: "holder", forward: true},
		{name: "loopback_pull", scopes: rw, claim: "holder"},
		{name: "pull", scopes: rw, wantPull: true},
		{name: "pull_write_only", scopes: `["git:write"]`, wantPull: true},
		{name: "pull_ref_policy", scopes: rw, refs: `[["refs/heads/other",["no-push"]]]`},
		{name: "pull_ops", scopes: rw, ops: `["no-force-push"]`},
		{name: "pull_unknown_ops", scopes: rw, ops: `["unknown"]`},
		{name: "pull_empty_ops", scopes: rw, refs: `[["*",[]]]`, wantPull: true},
		{name: "pull_malformed_refs", scopes: rw, refs: `{bad`},
		{name: "pull_malformed_ops", scopes: rw, ops: `{bad`},
		{name: "pull_in_flight", scopes: rw, syncing: true},
		{name: "disconnect_after_commit", scopes: rw, forward: true, disconnect: true, wantPush: true},
		{name: "metadata_unavailable_after_commit", scopes: rw, forward: true, storeClosed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			dbURL := "sqlite://" + filepath.Join(t.TempDir(), "hook.db")
			ms, err := meta.Open(ctx, dbURL)
			if err != nil {
				t.Fatal(err)
			}
			defer ms.Close()
			if err := ms.InitSchema(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := ms.UpsertOrgWithKeys(ctx, "hook-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
				t.Fatal(err)
			}
			repo, err := ms.CreateRepo(ctx, "hook-org", "https://example.com/hook.git", "main")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{RepoID: repo.RepoID, ForwardOnPush: tc.forward}); err != nil {
				t.Fatal(err)
			}
			if tc.syncing {
				if _, ok, _, err := ms.TryBeginSync(ctx, repo.RepoID, "holder", time.Minute); err != nil || !ok {
					t.Fatalf("claim: %v %v", ok, err)
				}
			}
			rs, err := ref.Open(ctx, dbURL)
			if err != nil {
				t.Fatal(err)
			}
			defer rs.Close()
			if err := rs.InitSchema(ctx); err != nil {
				t.Fatal(err)
			}
			t.Setenv(envOrg, "hook-org")
			t.Setenv("OPENSTORAGE_PG_URL", dbURL)
			t.Setenv(envSecretsKey, "")
			t.Setenv(githttp.EnvScopes, tc.scopes)
			t.Setenv(githttp.EnvRefsPolicy, tc.refs)
			t.Setenv(githttp.EnvOps, tc.ops)
			t.Setenv(githttp.EnvSyncClaim, tc.claim)
			original := githttp.WrapHookRefStore
			defer func() { githttp.WrapHookRefStore = original }()
			var logs bytes.Buffer
			cleanup := sync.OnceFunc(wireHookEventing(ctx, slog.New(slog.NewTextHandler(&logs, nil))))
			defer cleanup()
			if tc.storeClosed {
				cleanup()
			}
			var inner ref.RefStore = rs
			pushCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.disconnect {
				inner = cancelAfterCAS{RefStore: rs, cancel: cancel}
			}
			wrapped := githttp.WrapHookRefStore(ctx, inner)
			sha := strings.Repeat("a", 40)
			won, err := wrapped.CASRef(pushCtx, repo.RepoID, "refs/heads/main", ref.ZeroSHA, sha)
			if err != nil || !won {
				t.Fatalf("accepted CAS: %v %v", won, err)
			}
			up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
			if err != nil {
				t.Fatal(err)
			}
			if up.SyncPending != tc.wantPull || (up.PushPending > 0) != tc.wantPush {
				t.Fatalf("pending pull=%v push=%d; want pull=%v push=%v", up.SyncPending, up.PushPending, tc.wantPull, tc.wantPush)
			}
			if tc.storeClosed && !strings.Contains(logs.String(), "sync poke lost") {
				t.Fatalf("missing scheduling failure log: %s", logs.String())
			}
		})
	}
}

type localPendingQuery func(context.Context, string, string, int) ([]meta.PendingSyncWork, error)

func (q localPendingQuery) ListPendingSyncWork(ctx context.Context, org, after string, limit int) ([]meta.PendingSyncWork, error) {
	return q(ctx, org, after, limit)
}

func TestLocalPendingConsumerPaginationAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var scans atomic.Int32
		started := make(chan string, 64)
		run := func(_ context.Context, j upsync.Job) error { started <- j.RepoID; return nil }
		q := jobs.NewLocal(&events.Deliverer{}, run, run, nil, slog.Default())
		defer q.Close()
		query := localPendingQuery(func(ctx context.Context, org, after string, limit int) ([]meta.PendingSyncWork, error) {
			if org != "org" || limit != localPendingPageSize {
				t.Errorf("org=%q limit=%d", org, limit)
			}
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > localPendingTimeout {
				t.Error("scan missing deadline")
			}
			switch scans.Add(1) {
			case 1:
				if after != "" {
					t.Errorf("first cursor=%q", after)
				}
				return []meta.PendingSyncWork{{RepoID: "a", Pull: true}, {RepoID: "b", Push: true}}, nil
			case 2:
				if after != "b" {
					t.Errorf("next cursor=%q", after)
				}
				return nil, fmt.Errorf("temporary scan failure")
			case 3:
				if after != "b" {
					t.Errorf("error advanced cursor=%q", after)
				}
				return nil, nil
			default:
				if after != "" {
					t.Errorf("traversal did not wrap=%q", after)
				}
				return nil, nil
			}
		})
		stop := startLocalPendingSync(t.Context(), query, "org", q, slog.Default())
		synctest.Wait()
		if scans.Load() != 0 {
			t.Fatal("scan before listener delay")
		}
		time.Sleep(pendingSyncRecoveryDelay)
		synctest.Wait()
		if len(started) != 2 {
			t.Fatalf("started=%d", len(started))
		}
		for range 3 {
			time.Sleep(localPendingInterval)
			synctest.Wait()
		}
		stop()
		count := scans.Load()
		if count != 4 {
			t.Fatalf("scans=%d, want four bounded passes", count)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if scans.Load() != count {
			t.Fatal("scan resurrected after shutdown")
		}
	})
}

func TestLocalPendingConsumerJoinsCanceledScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := func(context.Context, upsync.Job) error { t.Error("work after cancellation"); return nil }
		q := jobs.NewLocal(&events.Deliverer{}, run, run, nil, slog.Default())
		defer q.Close()
		entered := make(chan struct{})
		exited := make(chan struct{})
		query := localPendingQuery(func(ctx context.Context, _, _ string, _ int) ([]meta.PendingSyncWork, error) {
			close(entered)
			<-ctx.Done()
			close(exited)
			return nil, ctx.Err()
		})
		stop := startLocalPendingSync(t.Context(), query, "org", q, slog.Default())
		<-entered
		stop()
		select {
		case <-exited:
		default:
			t.Fatal("stop did not join scan")
		}
	})
}

func TestLocalHookMarkBeforeEnqueueFailure(t *testing.T) {
	st := &forwardPokeStore{up: meta.RepoUpstream{RepoID: "r1", ForwardOnPush: true}}
	want := errors.New("queue unavailable")
	poke := enqueueSyncOrForward(st,
		func(context.Context, upsync.Job) error { t.Fatal("denied fallback"); return nil },
		func(context.Context, upsync.Job) error {
			if len(st.marks) != 1 {
				t.Fatal("enqueue ran before mark")
			}
			return want
		}, "org", func(context.Context) bool { return false })
	if err := poke(t.Context(), "r1"); !errors.Is(err, want) {
		t.Fatalf("enqueue error=%v", err)
	}
}

func TestLocalBootRepairPreservesOrphanForce(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		t.Run(backend, func(t *testing.T) {
			var ms meta.Backend
			if backend == "sqlite" {
				s, err := meta.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "boot.db"))
				if err != nil {
					t.Fatal(err)
				}
				ms = s
			} else {
				ms = meta.NewS3WAL(s3wal.NewMemCAS())
			}
			defer ms.Close()
			ctx := t.Context()
			if err := ms.InitSchema(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := ms.UpsertOrgWithKeys(ctx, "boot", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
				t.Fatal(err)
			}
			var ids []string
			for i := range 2 {
				r, err := ms.CreateRepo(ctx, "boot", fmt.Sprintf("https://example.com/%d.git", i), "main")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{RepoID: r.RepoID}); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.RepoID)
			}
			if err := ms.RequestPush(ctx, ids[0], true); err != nil {
				t.Fatal(err)
			}
			if _, ok, force, _, err := ms.TryBeginPush(ctx, ids[0], "old-process", time.Hour); err != nil || !ok || !force {
				t.Fatalf("orphan claim: ok=%v force=%v err=%v", ok, force, err)
			}
			recoverLocalPushClaims(ctx, ms, slog.Default())
			up, err := ms.GetRepoUpstream(ctx, ids[0])
			if err != nil || up.PushPending != 1 || !up.PushClaimForce || !up.PushSyncing {
				t.Fatalf("boot lost orphan/force: %+v %v", up, err)
			}
			// A new claim created AFTER boot must remain unmarked through live
			// scans. Boot's list includes it, but the live query must not.
			if _, ok, _, _, err := ms.TryBeginPush(ctx, ids[1], "live-process", time.Hour); err != nil || !ok {
				t.Fatalf("live claim: %v %v", ok, err)
			}
			for range 3 {
				page, err := ms.(meta.LocalPendingStore).ListPendingSyncWork(ctx, "boot", "", 16)
				if err != nil || len(page) != 1 || page[0].RepoID != ids[0] || !page[0].Push {
					t.Fatalf("live scan manufactured work: %+v %v", page, err)
				}
			}
		})
	}
}

func TestLocalHookFailedMarkNeverEnqueues(t *testing.T) {
	want := errors.New("metadata write unavailable")
	st := &forwardPokeStore{up: meta.RepoUpstream{RepoID: "r1", ForwardOnPush: true}, markErr: want}
	never := func(context.Context, upsync.Job) error { t.Fatal("failed mark enqueued work"); return nil }
	poke := enqueueSyncOrForward(st, never, never, "org", func(context.Context) bool { return false })
	if err := poke(t.Context(), "r1"); !errors.Is(err, want) {
		t.Fatalf("mark error=%v", err)
	}
}
