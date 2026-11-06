package main

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
	upsync "github.com/coder/openstorage/internal/sync"
)

// SQLite and S3WAL serves get the durable local schedule (issue #266,
// ADR-51): the worker claims through the row, and the queue is the
// scheduler, not the in-process queue with boot recovery and retry timers.
func TestBuildLocalSyncQueueUsesDurableSchedule(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		ms, err := meta.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "meta.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ms.Close)
		if err := ms.InitSchema(t.Context()); err != nil {
			t.Fatal(err)
		}
		assertDurableLocalQueue(t, ms)
	})
	t.Run("s3wal", func(t *testing.T) {
		ms := meta.NewS3WAL(s3wal.NewMemCAS())
		t.Cleanup(ms.Close)
		assertDurableLocalQueue(t, ms)
	})
}

func assertDurableLocalQueue(t *testing.T, ms meta.Store) {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	worker := &upsync.Worker{}
	deliverer := &events.Deliverer{Webhooks: events.MetaWebhookStore{Store: ms}, Logger: logger}
	queue, cleanup, err := buildLocalSyncQueue(ms, "org", t.TempDir(), worker, deliverer, nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if worker.Local == nil || worker.WaitPublication == nil {
		t.Fatal("worker has no local schedule or publication wait")
	}
	if q, ok := queue.(*jobs.Local); !ok || !q.TracksCreateState() {
		t.Fatalf("queue is %T, want *jobs.Local over the local schedule (it tracks create state)", queue)
	}
	if worker.Requeue != nil || worker.RequeuePush != nil {
		t.Fatal("worker still wires process-local requeue")
	}
	// The API's explicit triggers reach the schedule through these
	// optional extensions: pull-upstream offers without re-marking, and
	// push-upstream records with RequestPushNow.
	if _, ok := queue.(interface {
		OfferSync(ctx context.Context, job upsync.Job) error
	}); !ok {
		t.Fatalf("queue %T cannot offer an already recorded pull", queue)
	}
	_, ups := upsync.MetaStores(ms)
	if _, ok := ups.(interface {
		RequestPushNow(ctx context.Context, repoID string, force bool) error
	}); !ok {
		t.Fatalf("upstream store %T has no RequestPushNow", ups)
	}
}

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

func (s cancelAfterCAS) CASRefGen(ctx context.Context, repo, name, old, next string) (bool, int64, error) {
	won, gen, err := s.RefStore.CASRefGen(ctx, repo, name, old, next)
	s.cancel()
	return won, gen, err
}

// observeAtCAS calls observe the moment the authority CAS commits: a hook
// that serve's shutdown kills right there leaves the row observe reads.
type observeAtCAS struct {
	ref.RefStore
	observe func()
}

func (s observeAtCAS) CASRefGen(ctx context.Context, repo, name, old, next string) (bool, int64, error) {
	won, gen, err := s.RefStore.CASRefGen(ctx, repo, name, old, next)
	if won && err == nil {
		s.observe()
	}
	return won, gen, err
}

// TestLocalHookIntentAuthorizationAndFailures (carried over from #271):
// on SQLite an accepted git-surface ref write records durable intent for
// the serve sweep, behind the same gates as the API. Forwarding needs
// git:read and no ref-policy operations; pulling needs no ref-policy
// operations; a sync run's own loopback push (claim env) records nothing;
// a live pull claim suppresses pull marking (C10, shared with PG).
//
// The forward intent is in the row before the authority CAS commits
// (verify-lane-schedule finding 1): a hook killed right after the CAS, or
// one whose metadata store fails, never leaves an accepted write that no
// run forwards. A failed intent write refuses the update instead.
func TestLocalHookIntentAuthorizationAndFailures(t *testing.T) {
	const rw = `["git:read","git:write"]`
	for _, tc := range []struct {
		name, scopes, refs, ops, claim            string
		forward, syncing, disconnect, storeClosed bool
		pullEndsAtCAS                             bool
		wantPull, wantPush, wantRefused           bool
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
		// The pull claimed when the intent step read the row ends before the
		// poke: it cannot have seen this write, so the poke must mark.
		{name: "pull_ends_during_update", scopes: rw, syncing: true, pullEndsAtCAS: true, wantPull: true},
		{name: "disconnect_after_commit", scopes: rw, forward: true, disconnect: true, wantPush: true},
		{name: "metadata_unavailable", scopes: rw, forward: true, storeClosed: true, wantRefused: true},
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
			cleanup := sync.OnceFunc(wireHookEventing(ctx, slog.New(slog.DiscardHandler)))
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
			pushAtCAS := -1
			inner = observeAtCAS{RefStore: inner, observe: func() {
				up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
				if err != nil {
					t.Errorf("read the row at the CAS: %v", err)
				}
				pushAtCAS = up.PushPending
				if tc.pullEndsAtCAS {
					if _, err := ms.FinishSync(ctx, repo.RepoID, "holder", true); err != nil {
						t.Errorf("end the pull: %v", err)
					}
				}
			}}
			wrapped, err := githttp.WrapHookRefStore(ctx, inner)
			if err != nil {
				t.Fatal(err)
			}
			sha := strings.Repeat("a", 40)
			won, _, err := wrapped.CASRefGen(pushCtx, repo.RepoID, "refs/heads/main", ref.ZeroSHA, sha)
			if tc.wantRefused {
				got, gerr := rs.GetRef(ctx, repo.RepoID, "refs/heads/main")
				if err == nil || won || !errors.Is(gerr, ref.ErrNotFound) {
					t.Fatalf("CAS = (won %v, err %v), authority main = (%q, %v); want the update refused before the CAS: an accepted write without its forward intent is never forwarded", won, err, got, gerr)
				}
				return
			}
			if err != nil || !won {
				t.Fatalf("accepted CAS: %v %v", won, err)
			}
			if tc.wantPush && pushAtCAS <= 0 {
				t.Fatalf("push_pending = %d when the authority CAS committed; a hook killed right there loses the forward", pushAtCAS)
			}
			up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
			if err != nil {
				t.Fatal(err)
			}
			if up.SyncPending != tc.wantPull || (up.PushPending > 0) != tc.wantPush {
				t.Fatalf("pending pull=%v push=%d; want pull=%v push=%v", up.SyncPending, up.PushPending, tc.wantPull, tc.wantPush)
			}
		})
	}
}
