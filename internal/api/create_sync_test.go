package api

// Issue #388: a sync create is pending until its first sync is scheduled.
// A repeat create of the same upstream answers 200 and repairs a pending
// create with the stored configuration; a complete repo schedules nothing.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	stdsync "sync"
	"sync/atomic"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

// storeFirstSync is a FirstSyncScheduler over the test store: it records
// one job per won completion (CompleteRepoCreate is atomic on PostgreSQL
// and SQLite; PR #392 tests the real queues). hook runs before each call.
type storeFirstSync struct {
	recordingSyncQueue
	store  meta.Store
	tracks bool
	hook   func() error
}

func (q *storeFirstSync) TracksCreateState() bool { return q.tracks }

func (q *storeFirstSync) CompleteSyncCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
	if !q.tracks {
		panic("CompleteSyncCreate called on a store that tracks no create state")
	}
	if q.hook != nil {
		if err := q.hook(); err != nil {
			return 0, err
		}
	}
	out, err := q.store.CompleteRepoCreate(ctx, org, repoID)
	if err == nil && out == meta.CreateCompleted {
		_ = q.EnqueueSync(ctx, upsync.Job{Org: org, RepoID: repoID})
	}
	return out, err
}

func (q *storeFirstSync) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}

type syncCreateStack struct {
	e   *testEnv
	org string
	srv *httptest.Server
	q   *storeFirstSync
}

func newSyncCreateStack(t *testing.T, e *testEnv, tracks bool) *syncCreateStack {
	t.Helper()
	s := &syncCreateStack{e: e, org: e.newOrg(t), q: &storeFirstSync{store: e.store, tracks: tracks}}
	creds, ups := upsync.MetaStores(e.store.(meta.EventingStore))
	ss := newSyncServer(e.store, creds, ups, s.q, testHTTPBaseURL, events.TargetGuard{AllowPrivate: true}, discardLogger())
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/repos",
		requireAuth(auth.NewVerifier(e.store), s.org, false, auth.ScopeRepoWrite)(http.HandlerFunc(ss.handleCreateRepo)))
	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)
	return s
}

func (s *syncCreateStack) post(t *testing.T, name, body string) (int, http.Header, map[string]any) {
	t.Helper()
	tok := s.e.mint(t, tokenSpec{iss: s.org, repo: name, scopes: []string{auth.ScopeRepoWrite}})
	resp, raw := p5fixReq(t, s.srv, http.MethodPost, "/api/v1/repos", tok, []byte(body))
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Errorf("decode %q: %v", raw, err)
	}
	return resp.StatusCode, resp.Header, out
}

func (s *syncCreateStack) state(t *testing.T, name string) meta.CreateState {
	t.Helper()
	r, err := s.e.store.GetRepoByURL(context.Background(), s.org, name)
	if err != nil {
		t.Fatalf("GetRepoByURL(%s): %v", name, err)
	}
	return r.CreateState
}

const syncBody = `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"127.0.0.1"}}`

func syncRefBody(ref string) string {
	return `{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"127.0.0.1","ref":"` + ref + `"}}`
}

func TestSyncCreateRepeat(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		t.Run("concurrent identical creates schedule one first sync", func(t *testing.T) {
			s := newSyncCreateStack(t, e, true)
			codes := map[int]int{}
			var mu stdsync.Mutex
			var wg stdsync.WaitGroup
			for range 8 {
				wg.Go(func() {
					code, _, _ := s.post(t, "conc", syncBody)
					mu.Lock()
					codes[code]++
					mu.Unlock()
				})
			}
			wg.Wait()
			if codes[http.StatusCreated] != 1 || codes[http.StatusOK] != 7 {
				t.Fatalf("statuses = %v, want one 201 and seven 200", codes)
			}
			if n := s.q.count(); n != 1 || s.state(t, "conc") != meta.CreateStateComplete {
				t.Fatalf("jobs = %d, state %d; want one job and a complete repo", n, s.state(t, "conc"))
			}
		})

		t.Run("failed schedule keeps the repo; a retry with another ref repairs it", func(t *testing.T) {
			s := newSyncCreateStack(t, e, true)
			var failed atomic.Bool
			s.q.hook = func() error {
				if failed.CompareAndSwap(false, true) {
					return errors.New("injected queue failure")
				}
				return nil
			}
			code, hdr, _ := s.post(t, "rep", syncRefBody("a"))
			if code != http.StatusServiceUnavailable || hdr.Get("Retry-After") == "" || s.q.count() != 0 {
				t.Fatalf("first create = %d (Retry-After %q), %d jobs; want 503 with Retry-After, no job", code, hdr.Get("Retry-After"), s.q.count())
			}
			if s.state(t, "rep") != meta.CreateStatePending {
				t.Fatal("the failed create must keep a pending repo")
			}
			code, _, out := s.post(t, "rep", syncRefBody("b"))
			if code != http.StatusOK || out["message"] != "Repository 'rep' already exists" {
				t.Fatalf("retry = %d %v, want 200 already exists", code, out)
			}
			if n := s.q.count(); n != 1 || s.q.last(t).Ref != "" {
				t.Fatalf("jobs = %d (last %+v), want one job without the retry's ref", n, s.q.last(t))
			}
			// The first sync is queued: repeats answer 200 and schedule nothing.
			for _, body := range []string{syncRefBody("a"), syncBody} {
				if code, _, _ := s.post(t, "rep", body); code != http.StatusOK || s.q.count() != 1 {
					t.Fatalf("repeat = %d, %d jobs; want 200 and still one job", code, s.q.count())
				}
			}
		})

		t.Run("a create failing after insert never gives a false 200 (#388 race)", func(t *testing.T) {
			s := newSyncCreateStack(t, e, true)
			entered, release := make(chan struct{}), make(chan struct{})
			var first atomic.Bool
			s.q.hook = func() error {
				if first.CompareAndSwap(false, true) {
					close(entered)
					<-release
					return errors.New("injected queue failure")
				}
				return nil
			}
			done := make(chan int)
			go func() { code, _, _ := s.post(t, "race", syncBody); done <- code }()
			<-entered
			code, _, _ := s.post(t, "race", syncBody)
			close(release)
			if a := <-done; a != http.StatusServiceUnavailable || code != http.StatusOK {
				t.Fatalf("paused create = %d, concurrent create = %d; want 503 and 200", a, code)
			}
			if s.state(t, "race") != meta.CreateStateComplete || s.q.count() != 1 {
				t.Fatalf("repo state %d, %d jobs; want a live complete repo with one job", s.state(t, "race"), s.q.count())
			}
		})

		t.Run("a delete before completion answers 404, not 201", func(t *testing.T) {
			s := newSyncCreateStack(t, e, true)
			s.q.hook = func() error {
				s.q.hook = nil
				_, err := s.e.store.SoftDeleteRepo(context.Background(), s.org, "del")
				return err
			}
			if code, _, out := s.post(t, "del", syncBody); code != http.StatusNotFound || s.q.count() != 0 {
				t.Fatalf("create = %d %v, %d jobs; want 404 and no job", code, out, s.q.count())
			}
		})

		t.Run("incompatible names stay 409", func(t *testing.T) {
			s := newSyncCreateStack(t, e, true)
			if code, _, _ := s.post(t, "plain", `{}`); code != http.StatusCreated {
				t.Fatalf("plain create = %d", code)
			}
			if code, _, _ := s.post(t, "sync", syncBody); code != http.StatusCreated {
				t.Fatalf("sync create = %d", code)
			}
			for _, c := range []struct{ name, body string }{
				{"plain", syncBody}, // no upstream on the existing repo
				{"sync", `{"upstream":{"provider":"gitea","owner":"o","name":"other","host":"127.0.0.1"}}`},
				{"sync", `{}`}, // a plain create of a synced repo: 200 only after #388's last PR
			} {
				if code, _, _ := s.post(t, c.name, c.body); code != http.StatusConflict {
					t.Fatalf("%s with %s = %d, want 409", c.name, c.body, code)
				}
			}
		})

		t.Run("a store without create state keeps the legacy create and 409", func(t *testing.T) {
			s := newSyncCreateStack(t, e, false) // S3-WAL's queue (#335)
			if code, _, _ := s.post(t, "s3", syncBody); code != http.StatusCreated || s.q.count() != 1 {
				t.Fatalf("create = %d, %d jobs; want 201 and one job", code, s.q.count())
			}
			if code, _, _ := s.post(t, "s3", syncBody); code != http.StatusConflict || s.state(t, "s3") != meta.CreateStateLegacy {
				t.Fatalf("repeat = %d, state %d; want 409 and a legacy row", code, s.state(t, "s3"))
			}
		})
	})
}
