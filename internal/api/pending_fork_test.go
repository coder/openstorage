package api

// Issue #388, PR 3: a fork or copy inserts a pending row and completes it
// after its won ref write. While it is pending, plain and copy creates of
// the name answer 503, never success for a repo the fork may still delete.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
)

// completeHookStore runs hook in place of CompleteRepoCreate; hook calls
// the real store itself when it wants to.
type completeHookStore struct {
	meta.Store
	hook func(ctx context.Context, org, repoID string) (meta.CreateOutcome, error)
}

func (s completeHookStore) CompleteRepoCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
	return s.hook(ctx, org, repoID)
}

// gatedBlobStore pauses the first pack upload until release closes, then
// fails it or passes it through.
type gatedBlobStore struct {
	blob.BlobStore
	entered, release chan struct{}
	once             *stdsync.Once
	fail             bool
}

func (b gatedBlobStore) UploadFile(ctx context.Context, key, path string) error {
	first := false
	b.once.Do(func() { first = true })
	if first {
		close(b.entered)
		<-b.release
		if b.fail {
			return errors.New("injected pack upload failure")
		}
	}
	return b.BlobStore.UploadFile(ctx, key, path)
}

// pendingForkStack is a pendingSyncStack whose POST /repos also takes the
// fork dispatcher, as api.New wires it, over a source repo in another org.
type pendingForkStack struct {
	*pendingSyncStack
	fork, copy string      // request bodies
	src        wrefFixture // the source repo, in another org
}

func newPendingForkStack(t *testing.T, e *testEnv, blobs blob.BlobStore, store meta.Store) *pendingForkStack {
	t.Helper()
	s := &pendingForkStack{pendingSyncStack: newPendingSyncStack(t, e, true)}
	fs := &server{store: store, httpBaseURL: testHTTPBaseURL, logger: discardLogger(),
		refStore: e.refStore, blobStore: blobs, cacheDir: e.cacheDir}
	verifier := auth.NewVerifier(e.store)
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/repos", requireAuth(verifier, s.org, false, auth.ScopeRepoWrite)(
		fs.forkAwareCreateRepo(verifier, s.ss.handleCreateRepo)))
	s.srv = httptest.NewServer(withProblemInstance(mux))
	t.Cleanup(s.srv.Close)
	src := wrefSeedFixture(t, e)
	s.src = src
	tok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeGitRead}})
	s.fork = `{"base_repo":{"provider":"code","operation":"fork","auth":{"token":"` + tok + `"}}}`
	s.copy = `{"base_repo":{"name":"` + src.url + `","auth":{"token":"` + tok + `"}}}`
	return s
}

// answers asserts each create form's status for name. A sync create
// uses a git:write token, so it could finish a pending sync create.
func (s *pendingForkStack) answers(t *testing.T, step, name string, plain, copyForm, fork, sync int) {
	t.Helper()
	for _, c := range []struct {
		form, body string
		want       int
	}{{"plain", "", plain}, {"copy", s.copy, copyForm}, {"fork", s.fork, fork}} {
		code, hdr, msg := s.create(t, name, c.body)
		if code != c.want || (code == http.StatusServiceUnavailable &&
			(hdr.Get("Retry-After") != pendingCreateRetryAfter || !strings.Contains(msg, "DELETE /repos/delete"))) {
			t.Fatalf("%s: %s create = %d (Retry-After %q) %q, want %d", step, c.form, code, hdr.Get("Retry-After"), msg, c.want)
		}
	}
	if code, msg := s.retry(t, name, pendingSyncBody); code != sync {
		t.Fatalf("%s: sync create = %d %q, want %d", step, code, msg, sync)
	}
}

func (s *pendingForkStack) state(t *testing.T, name string) (meta.CreateState, error) {
	t.Helper()
	r, err := s.e.store.GetRepoByURL(context.Background(), s.org, name)
	return r.CreateState, err
}

func TestPendingFork(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		t.Run("a fork and a copy complete after their ref write", func(t *testing.T) {
			s := newPendingForkStack(t, e, e.blobStore, e.store)
			for name, body := range map[string]string{"forked": s.fork, "copied": s.copy} {
				if code, _, msg := s.create(t, name, body); code != http.StatusCreated {
					t.Fatalf("%s = %d %q, want 201", name, code, msg)
				}
				if st, err := s.state(t, name); err != nil || st != meta.CreateStateComplete {
					t.Fatalf("%s create_state = %d, %v; want complete", name, st, err)
				}
				s.answers(t, name+" complete", name, 200, 200, 200, 409)
				if _, err := e.store.SoftDeleteRepo(context.Background(), s.org, name); err != nil {
					t.Fatal(err)
				}
				s.answers(t, name+" deleted", name, 404, 404, 404, 404)
			}
		})

		t.Run("a delete before completion answers 404", func(t *testing.T) {
			s := newPendingForkStack(t, e, e.blobStore, completeHookStore{Store: e.store,
				hook: func(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
					if _, err := e.store.SoftDeleteRepo(ctx, org, "gone"); err != nil {
						return 0, err
					}
					return e.store.CompleteRepoCreate(ctx, org, repoID)
				}})
			if code, _, msg := s.create(t, "gone", s.fork); code != http.StatusNotFound {
				t.Fatalf("fork deleted before completion = %d %q, want 404", code, msg)
			}
		})

		t.Run("a completion that committed before its error succeeds", func(t *testing.T) {
			// The first attempt commits, then fails (a cancellation after
			// the commit); the retry sees complete. Also S3-WAL's
			// ambiguous-PUT recovery path.
			var mu stdsync.Mutex
			first := true
			s := newPendingForkStack(t, e, e.blobStore, completeHookStore{Store: e.store,
				hook: func(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
					out, err := e.store.CompleteRepoCreate(ctx, org, repoID)
					mu.Lock()
					defer mu.Unlock()
					if first && err == nil {
						first = false
						return 0, errors.New("injected error after commit")
					}
					return out, err
				}})
			if code, _, msg := s.create(t, "committed", s.copy); code != http.StatusCreated {
				t.Fatalf("copy whose completion committed before its error = %d %q, want 201", code, msg)
			}
			if st, err := s.state(t, "committed"); err != nil || st != meta.CreateStateComplete {
				t.Fatalf("create_state = %d, %v; want complete", st, err)
			}
		})

		t.Run("a failed completion is retried once, then the name stays pending", func(t *testing.T) {
			var mu stdsync.Mutex
			failures := 0
			s := newPendingForkStack(t, e, e.blobStore, completeHookStore{Store: e.store,
				hook: func(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
					mu.Lock()
					defer mu.Unlock()
					if failures > 0 {
						failures--
						return 0, errors.New("injected completion failure")
					}
					return e.store.CompleteRepoCreate(ctx, org, repoID)
				}})
			failures = 1
			if code, _, msg := s.create(t, "once", s.fork); code != http.StatusCreated {
				t.Fatalf("fork with one failed completion = %d %q, want 201", code, msg)
			}
			failures = 2
			if code, _, msg := s.create(t, "stuck", s.copy); code != http.StatusInternalServerError {
				t.Fatalf("copy with two failed completions = %d %q, want 500", code, msg)
			}
			if st, err := s.state(t, "stuck"); err != nil || st != meta.CreateStatePending {
				t.Fatalf("create_state = %d, %v; want pending", st, err)
			}
			s.answers(t, "stuck", "stuck", 503, 503, 503, 409)
			if _, err := e.store.SoftDeleteRepo(context.Background(), s.org, "stuck"); err != nil {
				t.Fatal(err)
			}
			s.answers(t, "stuck deleted", "stuck", 404, 404, 404, 404)
		})

		// T4.3 in the #388 plan: creates of every form while a fork
		// publishes its pack, then the fork fails or succeeds.
		for _, fail := range []bool{true, false} {
			t.Run(map[bool]string{true: "creates during a fork that fails", false: "creates during a fork that succeeds"}[fail], func(t *testing.T) {
				g := gatedBlobStore{BlobStore: e.blobStore, entered: make(chan struct{}), release: make(chan struct{}), once: &stdsync.Once{}, fail: fail}
				s := newPendingForkStack(t, e, g, e.store)
				// A failed assertion must not leave the fork paused, or
				// the server's Close waits for it forever.
				release := stdsync.OnceFunc(func() { close(g.release) })
				t.Cleanup(release)
				first := make(chan int, 1)
				go func() {
					code, _, _ := s.create(t, "racing", s.fork)
					first <- code
				}()
				select {
				case <-g.entered:
				case <-time.After(60 * time.Second):
					t.Fatal("the fork never reached its pack upload")
				}
				s.answers(t, "while publishing", "racing", 503, 503, 503, 409)
				release()
				code := <-first
				if fail {
					if code != http.StatusInternalServerError {
						t.Fatalf("failing fork = %d, want 500", code)
					}
					s.answers(t, "after the failure", "racing", 404, 404, 404, 404)
					return
				}
				if code != http.StatusCreated {
					t.Fatalf("fork = %d, want 201", code)
				}
				s.answers(t, "after the fork", "racing", 200, 200, 200, 409)
			})
		}
	})
}
