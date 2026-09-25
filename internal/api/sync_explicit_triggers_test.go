package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	stdsync "sync"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	upsync "github.com/coder/openstorage/internal/sync"
)

// pushNowMemStore is a MemStore that records explicit push requests.
type pushNowMemStore struct {
	*upsync.MemStore
	mu  stdsync.Mutex
	now int
}

func (m *pushNowMemStore) RequestPushNow(ctx context.Context, repoID string, force bool) error {
	m.mu.Lock()
	m.now++
	m.mu.Unlock()
	return m.MemStore.RequestPush(ctx, repoID, force)
}

// offeringSyncQueue records pull offers separately from enqueues.
type offeringSyncQueue struct {
	recordingSyncQueue
	offers []upsync.Job
}

func (q *offeringSyncQueue) OfferSync(_ context.Context, job upsync.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.offers = append(q.offers, job)
	return nil
}

// TestExplicitSyncTriggersUseScheduleExtensions: on a durable local
// schedule (issue #266), pull-upstream offers its already recorded request
// instead of recording it again, and push-upstream records with
// RequestPushNow so only the explicit trigger expedites a waiting retry.
func TestExplicitSyncTriggersUseScheduleExtensions(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		ups := &pushNowMemStore{MemStore: upsync.NewMemStore()}
		queue := &offeringSyncQueue{}
		ss := newSyncServer(e.store, ups, ups, queue, testHTTPBaseURL, events.TargetGuard{AllowPrivate: true}, discardLogger())
		mux := http.NewServeMux()
		verifier := auth.NewVerifier(e.store)
		mux.Handle("POST /api/v1/repos",
			requireAuth(verifier, org, false, auth.ScopeRepoWrite)(http.HandlerFunc(ss.handleCreateRepo)))
		registerSyncRoutes(mux, verifier, org, ss, nil)
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		repoURL := "team/explicit-" + randomHex(t, 4)
		writeTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
		gitTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitWrite, auth.ScopeGitRead}})
		body := []byte(`{"base_repo":{"provider":"github","owner":"acme","name":"demo","operation":"sync"}}`)
		if resp, b := p5fixReq(t, srv, http.MethodPost, "/api/v1/repos", writeTok, body); resp.StatusCode != http.StatusOK {
			t.Fatalf("create status = %d (body %s)", resp.StatusCode, b)
		}
		if resp, b := p5fixReq(t, srv, http.MethodPost, "/api/v1/repos/pull-upstream", gitTok, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("pull-upstream status = %d (body %s)", resp.StatusCode, b)
		}
		if resp, b := p5fixReq(t, srv, http.MethodPost, "/api/v1/repos/push-upstream", gitTok, []byte(`{}`)); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("push-upstream status = %d (body %s)", resp.StatusCode, b)
		}
		queue.mu.Lock()
		enqueued, offered, pushes := len(queue.jobs), len(queue.offers), len(queue.pushJobs)
		queue.mu.Unlock()
		if enqueued != 1 || offered != 1 || pushes != 1 {
			t.Fatalf("enqueues %d (want 1: the initial sync), pull offers %d (want 1), push enqueues %d (want 1)",
				enqueued, offered, pushes)
		}
		ups.mu.Lock()
		now := ups.now
		ups.mu.Unlock()
		if now != 1 {
			t.Fatalf("push-upstream RequestPushNow calls = %d, want 1", now)
		}
	})
}
