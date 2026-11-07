package api

// Issue #388: a sync create whose first-sync schedule fails stays pending
// (503, every row kept), and only an authorized pull-upstream completes it.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

// pendingSyncScheduler is a FirstSyncScheduler over the test store with
// the real schedulers' contract (#392, PGRequestSync): a detached repo
// fails with meta.ErrNotFound and stays pending, and a won completion
// records the request and schedules one run. internal/jobs and
// internal/sync test the atomicity. hook (nil: none) runs first; its
// error fails the schedule.
type pendingSyncScheduler struct {
	upsync.Queue
	repos  meta.Store
	es     meta.EventingStore
	tracks bool

	mu                   stdsync.Mutex
	hook                 func(repoID string) error
	calls, won, enqueued int
}

func (q *pendingSyncScheduler) TracksCreateState() bool { return q.tracks }

func (q *pendingSyncScheduler) CompleteSyncCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
	if !q.tracks {
		panic("CompleteSyncCreate called on a store that tracks no create state")
	}
	q.mu.Lock()
	q.calls++
	hook := q.hook
	q.mu.Unlock()
	if hook != nil {
		if err := hook(repoID); err != nil {
			return 0, err
		}
	}
	if _, err := q.es.GetRepoUpstream(ctx, repoID); err != nil {
		return 0, err
	}
	out, err := q.repos.CompleteRepoCreate(ctx, org, repoID)
	if err != nil || out != meta.CreateCompleted {
		return out, err
	}
	if err := q.es.RequestSync(ctx, repoID); err != nil {
		return 0, err
	}
	q.mu.Lock()
	q.won++
	q.mu.Unlock()
	return out, q.EnqueueSync(ctx, upsync.Job{Org: org, RepoID: repoID})
}

func (q *pendingSyncScheduler) EnqueueSync(ctx context.Context, job upsync.Job) error {
	q.mu.Lock()
	q.enqueued++
	q.mu.Unlock()
	return q.Queue.EnqueueSync(ctx, job)
}

func (q *pendingSyncScheduler) setHook(h func(string) error) {
	q.mu.Lock()
	q.hook = h
	q.mu.Unlock()
}

// counts returns (CompleteSyncCreate calls, won completions, enqueues).
func (q *pendingSyncScheduler) counts() (int, int, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.calls, q.won, q.enqueued
}

// pendingSyncStack serves the sync create and pull routes over meta-backed
// upstream rows, with a real sync worker behind the scheduler and a
// JWT-gated CGI loopback that holds each repo's refs.
type pendingSyncStack struct {
	e        *testEnv
	org      string
	ss       *syncServer
	q        *pendingSyncScheduler
	srv      *httptest.Server
	events   chan upsync.Event
	loopRoot string
}

func newPendingSyncStack(t *testing.T, e *testEnv, tracks bool) *pendingSyncStack {
	t.Helper()
	es := e.store.(meta.EventingStore)
	creds, ups := upsync.MetaStores(es)
	verifier := auth.NewVerifier(e.store)
	// The worker's log names the cause of a repo.sync.failed event, which
	// carries only the documented error string.
	logger := testLogger(t)
	s := &pendingSyncStack{e: e, org: e.newOrg(t), events: make(chan upsync.Event, 64), loopRoot: t.TempDir()}
	loop := usyncCGIServer(t, s.loopRoot, func(r *http.Request) bool {
		user, pass, ok := r.BasicAuth()
		claims, err := verifier.Verify(r.Context(), pass)
		return ok && user == "t" && err == nil && claims.Iss == s.org && claims.HasScope(auth.ScopeGitWrite)
	}, true)
	worker := &upsync.Worker{
		Upstreams: ups, Credentials: creds, Events: &usyncSink{ch: s.events},
		Repos: func(ctx context.Context, org, repoID string) (upsync.RepoInfo, error) {
			repo, err := e.store.GetRepoByID(ctx, org, repoID)
			if errors.Is(err, meta.ErrNotFound) {
				return upsync.RepoInfo{}, upsync.ErrNotFound
			}
			return upsync.RepoInfo{RepoID: repo.RepoID, URL: repo.URL}, err
		},
		Loopback: upsync.Loopback{BaseURL: loop.URL,
			Token: func(_ context.Context, org string, repo upsync.RepoInfo, _ time.Duration, _ string) (string, error) {
				return e.mint(t, tokenSpec{iss: org, repo: repo.RepoID, scopes: []string{auth.ScopeGitWrite}}), nil
			},
		},
		Logger: logger, Timeout: time.Minute, PushEgressAllowPrivate: true, PullEgressAllowPrivate: true,
	}
	inproc := upsync.NewInProcessQueue(worker.Run, worker.RunPush, logger)
	t.Cleanup(inproc.Close)
	s.q = &pendingSyncScheduler{Queue: inproc, repos: e.store, es: es, tracks: tracks}
	s.ss = newSyncServer(e.store, creds, ups, s.q, testHTTPBaseURL, events.TargetGuard{AllowPrivate: true}, discardLogger())
	if (s.ss.firstSync != nil) != tracks {
		t.Fatalf("syncServer.firstSync set = %v, want %v", s.ss.firstSync != nil, tracks)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/repos",
		requireAuth(verifier, s.org, false, auth.ScopeRepoWrite)(http.HandlerFunc(s.ss.handleCreateRepo)))
	registerSyncRoutes(mux, verifier, s.org, s.ss, nil)
	registerAliasRoutes(mux, &server{store: e.store, logger: discardLogger()}, s.ss, verifier, s.org, nil)
	s.srv = httptest.NewServer(withProblemInstance(mux))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *pendingSyncStack) token(t *testing.T, name, refs string, scopes ...string) string {
	t.Helper()
	return s.e.mint(t, tokenSpec{iss: s.org, repo: name, scopes: scopes, refs: refs})
}

// do returns the status, headers and, for an error, the envelope message.
func (s *pendingSyncStack) do(t *testing.T, path, tok, body string) (int, http.Header, string) {
	t.Helper()
	resp, raw := p5fixReq(t, s.srv, http.MethodPost, path, tok, []byte(body))
	if resp.StatusCode >= 400 {
		return resp.StatusCode, resp.Header, requireEnvelope(t, resp, raw)
	}
	return resp.StatusCode, resp.Header, string(raw)
}

func (s *pendingSyncStack) create(t *testing.T, name, body string) (int, http.Header, string) {
	t.Helper()
	return s.do(t, "/api/v1/repos", s.token(t, name, "", auth.ScopeRepoWrite), body)
}

func (s *pendingSyncStack) pull(t *testing.T, tok string) (int, string) {
	t.Helper()
	code, _, msg := s.do(t, "/api/v1/repos/pull-upstream", tok, "{}")
	return code, msg
}

const pendingSyncBody = `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"upstream.invalid"}}`

// createPending makes a pending sync row: its first-sync schedule fails.
func (s *pendingSyncStack) createPending(t *testing.T, name, body string) meta.Repo {
	t.Helper()
	s.q.setHook(func(string) error { return errors.New("injected schedule failure") })
	code, hdr, msg := s.create(t, name, body)
	s.q.setHook(nil)
	if code != http.StatusServiceUnavailable || hdr.Get("Retry-After") != pendingCreateRetryAfter {
		t.Fatalf("create with a failed schedule = %d (Retry-After %q) %q, want 503 + Retry-After", code, hdr.Get("Retry-After"), msg)
	}
	r, err := s.e.store.GetRepoByURL(context.Background(), s.org, name)
	if err != nil || r.CreateState != meta.CreateStatePending {
		t.Fatalf("repo after a failed schedule = %+v, %v; want live and pending", r, err)
	}
	return r
}

// expect asserts the scheduler counts and the create state.
func (s *pendingSyncStack) expect(t *testing.T, step, name string, calls, won, enqueued int, state meta.CreateState) {
	t.Helper()
	if c, w, q := s.q.counts(); c != calls || w != won || q != enqueued {
		t.Fatalf("%s: scheduler calls %d, won %d, enqueues %d; want %d, %d, %d", step, c, w, q, calls, won, enqueued)
	}
	if r, err := s.e.store.GetRepoByURL(context.Background(), s.org, name); err != nil || r.CreateState != state {
		t.Fatalf("%s: repo = %+v, %v; want create_state %d", step, r, err, state)
	}
}

func TestPendingSyncCreate(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		t.Run("a delete before completion answers 404", func(t *testing.T) {
			s := newPendingSyncStack(t, e, true)
			s.q.setHook(func(string) error {
				_, err := s.e.store.SoftDeleteRepo(context.Background(), s.org, "gone")
				return err
			})
			if code, _, msg := s.create(t, "gone", pendingSyncBody); code != http.StatusNotFound {
				t.Fatalf("create deleted before completion = %d %q, want 404", code, msg)
			}
			if _, won, enq := s.q.counts(); won != 0 || enq != 0 {
				t.Fatalf("won %d, enqueues %d; want none", won, enq)
			}
		})

		t.Run("a detach before completion stays pending", func(t *testing.T) {
			// #388 desk decision (question 2, option A): like a crashed
			// fork, the row stays pending until an authorized DELETE.
			s := newPendingSyncStack(t, e, true)
			s.q.setHook(func(repoID string) error { return s.q.es.DeleteRepoUpstream(context.Background(), repoID) })
			code, hdr, msg := s.create(t, "det", pendingSyncBody)
			if code != http.StatusServiceUnavailable || hdr.Get("Retry-After") != "" || !strings.Contains(msg, "DELETE /repos/delete") {
				t.Fatalf("create detached before completion = %d (Retry-After %q) %q, want 503 naming DELETE, no Retry-After", code, hdr.Get("Retry-After"), msg)
			}
			s.q.setHook(nil)
			if code, msg := s.pull(t, s.token(t, "det", "", auth.ScopeGitWrite)); code != http.StatusBadRequest {
				t.Fatalf("pull-upstream on a detached pending row = %d %q, want 400", code, msg)
			}
			// Like a crashed fork: a plain create may retry (503), and a
			// sync create finds no link (409).
			for b, want := range map[string]int{"": http.StatusServiceUnavailable, pendingSyncBody: http.StatusConflict} {
				if code, _, _ := s.create(t, "det", b); code != want {
					t.Fatalf("create %q of a detached pending row = %d, want %d", b, code, want)
				}
			}
			s.expect(t, "detached", "det", 2, 0, 0, meta.CreateStatePending)
		})

		t.Run("an untracked store keeps the legacy create", func(t *testing.T) {
			// A queue that tracks no create state: the scheduler panics if
			// anything calls it.
			s := newPendingSyncStack(t, e, false)
			for i, want := range []int{http.StatusCreated, http.StatusConflict} {
				if code, _, msg := s.create(t, "legacy", pendingSyncBody); code != want {
					t.Fatalf("untracked sync create %d = %d %q, want %d", i, code, msg, want)
				}
			}
			if code, msg := s.pull(t, s.token(t, "legacy", "", auth.ScopeGitWrite)); code != http.StatusAccepted {
				t.Fatalf("pull-upstream on a legacy row = %d %q, want 202", code, msg)
			}
			s.expect(t, "untracked", "legacy", 0, 0, 2, meta.CreateStateLegacy)
			// A pending fork row (S3-WAL stores create state since ADR-51
			// v2): a plain create may retry, and a sync create is 409, as on
			// a tracked store.
			if _, err := e.store.CreateRepoWithState(context.Background(), s.org, "forking", "main", meta.CreateStatePending); err != nil {
				t.Fatal(err)
			}
			for b, want := range map[string]int{"": http.StatusServiceUnavailable, pendingSyncBody: http.StatusConflict} {
				if code, _, msg := s.create(t, "forking", b); code != want {
					t.Fatalf("untracked create %q of a pending fork row = %d %q, want %d", b, code, msg, want)
				}
			}
		})
	})
}

// TestPendingSyncPolicyRestricted is the #393 round-3 P1 (T1 in the #388
// plan): refs written into a pending repo survive every policy-restricted
// attempt to finish it, and no attempt schedules anything. An unpoliced
// git:write pull-upstream then finishes it, and its mirror overwrites
// main, so the assertions can see a mirror.
func TestPendingSyncPolicyRestricted(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newPendingSyncStack(t, e, true)
		upRoot := t.TempDir()
		upstream := usyncCGIServer(t, upRoot, func(r *http.Request) bool {
			user, pass, ok := r.BasicAuth()
			return ok && basicAuthEquals(user, pass, "up-secret", "")
		}, false)
		upHead := usyncSeedUpstream(t, upRoot, "own", "proj")
		const name = "pending-repo"
		body, err := json.Marshal(map[string]any{"base_repo": map[string]any{"provider": "gitea", "owner": "own",
			"name": "proj", "upstream_host": upstream.URL, "auth": map[string]string{"token": "up-secret"}}})
		if err != nil {
			t.Fatal(err)
		}
		repo := s.createPending(t, name, string(body))
		if _, err := s.ss.creds.CredentialForRepo(context.Background(), repo.RepoID); err != nil {
			t.Fatalf("credential row after a failed schedule: %v", err)
		}

		// A client writes main and tag v1 into the pending repo. The
		// loopback holds this stack's refs, and the stack wires no
		// ref-write pull poke (#421).
		bare := filepath.Join(s.loopRoot, repo.RepoID+".git")
		usyncGit(t, s.loopRoot, "init", "--bare", "-b", "main", bare)
		usyncGit(t, s.loopRoot, "-C", bare, "config", "http.receivepack", "true")
		work := t.TempDir()
		usyncGit(t, work, "init", "-b", "main")
		if err := os.WriteFile(filepath.Join(work, "local.txt"), []byte("written while pending\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		usyncGit(t, work, "add", ".")
		usyncGit(t, work, "commit", "-m", "local")
		usyncGit(t, work, "tag", "v1")
		usyncGit(t, work, "push", "file://"+bare, "main", "v1")
		localHead := usyncGit(t, work, "rev-parse", "HEAD")

		unchanged := func(step string) {
			t.Helper()
			for _, ref := range []string{"refs/heads/main", "refs/tags/v1"} {
				if got := usyncGit(t, bare, "rev-parse", ref+"^{commit}"); got != localHead {
					t.Fatalf("%s: %s = %s, want the written %s", step, ref, got, localHead)
				}
			}
			s.expect(t, step, name, 1, 0, 0, meta.CreateStatePending) // 1 call: the failed create
			if up, err := s.q.es.GetRepoUpstream(context.Background(), repo.RepoID); err != nil || up.SyncPending || up.Syncing {
				t.Fatalf("%s: upstream = %+v, %v; want no sync request", step, up, err)
			}
			select {
			case ev := <-s.events:
				t.Fatalf("%s: unexpected sync event %+v", step, ev)
			default:
			}
		}
		for _, op := range []string{"no-force-push", "verify-sig", "no-push"} {
			refs := `[["main", ["` + op + `"]]]`
			tok := s.token(t, name, refs, auth.ScopeRepoWrite, auth.ScopeGitWrite)
			for _, path := range []string{"/api/v1/repos/pull-upstream", "/api/repos/" + name + "/upstream/pull", "/api/v1/repos"} {
				if code, _, msg := s.do(t, path, tok, string(body)); code != http.StatusForbidden {
					t.Fatalf("%s %s = %d %q, want 403", op, path, code, msg)
				}
				unchanged(op + " " + path)
			}
			// The helper's own gate, whatever its caller checked.
			claims := &auth.Claims{Iss: s.org, Repo: name, Scopes: []string{auth.ScopeGitWrite}, Refs: json.RawMessage(refs)}
			if denial, _, err := s.ss.finishPendingSync(context.Background(), claims, repo); denial == "" || err != nil {
				t.Fatalf("%s finishPendingSync = %q, %v; want a denial", op, denial, err)
			}
			unchanged(op + " finishPendingSync")
		}
		// repo:write alone cannot finish it (403), and other forms are 409.
		for b, want := range map[string]int{string(body): http.StatusForbidden, pendingSyncBody: http.StatusConflict, "": http.StatusConflict} {
			if code, _, msg := s.create(t, name, b); code != want {
				t.Fatalf("repeat create %q of a pending sync row = %d %q, want %d", b, code, msg, want)
			}
		}
		unchanged("repeat creates")
		repoWriteOnly := &auth.Claims{Iss: s.org, Repo: name, Scopes: []string{auth.ScopeRepoWrite}}
		if denial, _, err := s.ss.finishPendingSync(context.Background(), repoWriteOnly, repo); denial == "" || err != nil {
			t.Fatalf("repo:write-only finishPendingSync = %q, %v; want a denial", denial, err)
		}
		unchanged("repo:write-only finishPendingSync")

		// Positive control: an unpoliced git:write token finishes it.
		if code, msg := s.pull(t, s.token(t, name, "", auth.ScopeGitWrite)); code != http.StatusAccepted {
			t.Fatalf("unpoliced pull-upstream = %d %q, want 202", code, msg)
		}
		for _, want := range []string{upsync.EventSyncStarted, upsync.EventSyncSucceeded} {
			select {
			case ev := <-s.events:
				if ev.Type != want {
					t.Fatalf("event = %+v, want %s", ev, want)
				}
			case <-time.After(60 * time.Second):
				t.Fatalf("timed out waiting for %s", want)
			}
		}
		if got := usyncGit(t, bare, "rev-parse", "refs/heads/main"); got != upHead {
			t.Fatalf("main after the first sync = %s, want the upstream %s", got, upHead)
		}
		s.expect(t, "unpoliced pull-upstream", name, 2, 1, 1, meta.CreateStateComplete)
	})
}

// testLogger is a logger whose records go to t.Log, so a failed test
// shows them. A record that arrives after the cleanups before it have run
// is dropped: t.Log panics once the test has completed. Call it before the
// code that logs registers its own cleanups.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	w := &testLogWriter{t: t}
	t.Cleanup(w.stop)
	return slog.New(slog.NewTextHandler(w, nil))
}

type testLogWriter struct {
	mu   stdsync.Mutex
	t    *testing.T
	done bool
}

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.done {
		w.t.Log(strings.TrimSuffix(string(p), "\n"))
	}
	return len(p), nil
}

func (w *testLogWriter) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.done = true
}
