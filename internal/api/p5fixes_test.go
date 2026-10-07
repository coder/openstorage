package api

// Regression tests for the docs/archive/phases/14-phase5-implementation-notes.md "Deferred gate findings" burned down in
// phase 6: base_repo in GET /repo(s) responses (#2), handleCreateRepo
// post-create row cleanup (#5), and pull-upstream empty-ref defaulting
// (#8). #1/#3/#4/#7 are covered next to their code (providerhost_test.go,
// refstore_test.go, credentials_test.go, eventing_finishsync_test.go).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	stdsync "sync"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

func TestGetRepoIncludesBaseRepo(t *testing.T) { runBackends(t, testGetRepoIncludesBaseRepo) }

func testGetRepoIncludesBaseRepo(t *testing.T, e *testEnv) {
	ctx := context.Background()
	org := e.newOrg(t)
	linkedURL := "team/linked-" + randomHex(t, 4)
	plainURL := "team/plain-" + randomHex(t, 4)
	linked := createRepo(t, e, org, linkedURL, nil, http.StatusCreated)
	createRepo(t, e, org, plainURL, nil, http.StatusCreated)
	if _, err := e.store.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: linked.RepoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	wantBase := &baseRepoEntry{Provider: "github", Owner: "acme", Name: "demo"}

	// GET /repo on the linked repo carries base_repo {provider,owner,name}.
	readTok := e.mint(t, tokenSpec{iss: org, repo: linkedURL, scopes: []string{auth.ScopeGitRead}})
	resp, body := e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repo status = %d (body %s)", resp.StatusCode, body)
	}
	var got repoEntry
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.BaseRepo == nil || *got.BaseRepo != *wantBase {
		t.Errorf("base_repo = %+v, want %+v", got.BaseRepo, wantBase)
	}

	// A repo without an upstream omits the key entirely (omitempty).
	plainTok := e.mint(t, tokenSpec{iss: org, repo: plainURL, scopes: []string{auth.ScopeGitRead}})
	resp, body = e.doReq(t, http.MethodGet, "/api/v1/repo", plainTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repo (plain) status = %d (body %s)", resp.StatusCode, body)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	if _, present := raw["base_repo"]; present {
		t.Errorf("unlinked repo body carries base_repo: %s", body)
	}

	// GET /repos mirrors the same per-entry shape.
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
	resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos", orgTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repos status = %d (body %s)", resp.StatusCode, body)
	}
	var list listReposResponse
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Repos) != 2 {
		t.Fatalf("list = %+v, want the two created repos", list.Repos)
	}
	for _, entry := range list.Repos {
		switch entry.URL {
		case linkedURL:
			if entry.BaseRepo == nil || *entry.BaseRepo != *wantBase {
				t.Errorf("list base_repo for %s = %+v, want %+v", entry.URL, entry.BaseRepo, wantBase)
			}
		case plainURL:
			if entry.BaseRepo != nil {
				t.Errorf("list base_repo for %s = %+v, want nil", entry.URL, entry.BaseRepo)
			}
		default:
			t.Errorf("unexpected list entry %+v", entry)
		}
	}
}

// failingSyncQueue rejects every enqueue — the post-CreateRepo failure
// injection for the cleanup test.
type failingSyncQueue struct{}

func (failingSyncQueue) EnqueueSync(context.Context, upsync.Job) error {
	return errors.New("queue down")
}

func (failingSyncQueue) EnqueuePush(context.Context, upsync.Job) error {
	return errors.New("queue down")
}

// recordingSyncQueue captures enqueued jobs.
type recordingSyncQueue struct {
	mu       stdsync.Mutex
	jobs     []upsync.Job
	pushJobs []upsync.Job
}

func (q *recordingSyncQueue) EnqueueSync(_ context.Context, job upsync.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, job)
	return nil
}

func (q *recordingSyncQueue) EnqueuePush(_ context.Context, job upsync.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pushJobs = append(q.pushJobs, job)
	return nil
}

func (q *recordingSyncQueue) last(t *testing.T) upsync.Job {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		t.Fatal("no sync jobs recorded")
	}
	return q.jobs[len(q.jobs)-1]
}

// newP5FixStack builds a minimal sync-handler server (same route shapes as
// newUsyncStack) around an injectable queue.
func newP5FixStack(t *testing.T, e *testEnv, queue upsync.Queue) (string, *httptest.Server) {
	t.Helper()
	org := e.newOrg(t)
	verifier := auth.NewVerifier(e.store)
	mem := upsync.NewMemStore()
	ss := newSyncServer(e.store, mem, mem, queue, testHTTPBaseURL, events.TargetGuard{AllowPrivate: true}, discardLogger())
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/repos",
		requireAuth(verifier, org, false, auth.ScopeRepoWrite)(http.HandlerFunc(ss.handleCreateRepo)))
	registerSyncRoutes(mux, verifier, org, ss, nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return org, srv
}

func p5fixReq(t *testing.T, srv *httptest.Server, method, path, token string, body []byte) (*http.Response, []byte) {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	return resp, buf.Bytes()
}

func TestCreateRepoAbandonsRowOnPostCreateFailure(t *testing.T) {
	runBackends(t, testCreateRepoAbandonsRowOnPostCreateFailure)
}

func testCreateRepoAbandonsRowOnPostCreateFailure(t *testing.T, e *testEnv) {
	org, srv := newP5FixStack(t, e, failingSyncQueue{})
	repoURL := "team/abandoned-" + randomHex(t, 4)
	tok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})

	body := []byte(`{"base_repo":{"provider":"github","owner":"acme","name":"demo","operation":"sync"}}`)
	resp, respBody := p5fixReq(t, srv, http.MethodPost, "/api/v1/repos", tok, body)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("create with failing queue status = %d (body %s), want 500", resp.StatusCode, respBody)
	}

	// docs/archive/phases/14-phase5-implementation-notes.md deferred #5: the fresh repo row must not linger live behind
	// the 500 — createAbandon soft-deletes it like forkAbandon.
	if _, err := e.store.GetRepoByURL(context.Background(), org, repoURL); !errors.Is(err, meta.ErrNotFound) {
		t.Errorf("GetRepoByURL after failed create err = %v, want ErrNotFound (soft-deleted)", err)
	}
}

func TestPullUpstreamDefaultsRefToDefaultBranch(t *testing.T) {
	runBackends(t, testPullUpstreamDefaultsRefToDefaultBranch)
}

func testPullUpstreamDefaultsRefToDefaultBranch(t *testing.T, e *testEnv) {
	queue := &recordingSyncQueue{}
	org, srv := newP5FixStack(t, e, queue)
	repoURL := "team/refdefault-" + randomHex(t, 4)
	writeTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
	gitTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitWrite}})

	body := []byte(`{"default_branch":"trunk","base_repo":{"provider":"github","owner":"acme","name":"demo","operation":"sync"}}`)
	resp, respBody := p5fixReq(t, srv, http.MethodPost, "/api/v1/repos", writeTok, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d (body %s)", resp.StatusCode, respBody)
	}

	// docs/archive/phases/14-phase5-implementation-notes.md deferred #8: an omitted ref defaults to the repo's default
	// branch instead of enqueueing an empty one.
	resp, respBody = p5fixReq(t, srv, http.MethodPost, "/api/v1/repos/pull-upstream", gitTok, []byte(`{}`))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("pull-upstream status = %d (body %s), want 202", resp.StatusCode, respBody)
	}
	if job := queue.last(t); job.Ref != "trunk" {
		t.Errorf("defaulted job ref = %q, want %q", job.Ref, "trunk")
	}

	// An explicit ref still passes through untouched.
	resp, respBody = p5fixReq(t, srv, http.MethodPost, "/api/v1/repos/pull-upstream", gitTok, []byte(`{"ref":"feature-x"}`))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("pull-upstream explicit ref status = %d (body %s), want 202", resp.StatusCode, respBody)
	}
	if job := queue.last(t); job.Ref != "feature-x" {
		t.Errorf("explicit job ref = %q, want %q", job.Ref, "feature-x")
	}
}

// TestCreateAbandonCleansSiblingRows seeds the durable rows a base_repo
// create produces (repo + credential + upstream) and asserts createAbandon
// removes all three, not just the repo row (docs/archive/phases/15-phase6-implementation-notes.md deferred #2).
func TestCreateAbandonCleansSiblingRows(t *testing.T) {
	runBackends(t, testCreateAbandonCleansSiblingRows)
}

func testCreateAbandonCleansSiblingRows(t *testing.T, e *testEnv) {
	ctx := context.Background()
	org := e.newOrg(t)
	repoURL := "team/abandon-rows-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(ctx, org, repoURL, "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}
	creds, ups := upsync.MetaStores(e.store)
	cred, err := creds.CreateCredential(ctx, upsync.Credential{RepoID: repo.RepoID, Password: "tok"})
	if err != nil {
		t.Fatalf("CreateCredential: %v", err)
	}
	if _, err := ups.PutUpstream(ctx, upsync.Upstream{
		RepoID: repo.RepoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("PutUpstream: %v", err)
	}

	ss := newSyncServer(e.store, creds, ups, envSyncQueue{}, testHTTPBaseURL, events.TargetGuard{AllowPrivate: true}, discardLogger())
	ss.createAbandon(ctx, org, repoURL, repo.RepoID)

	if _, err := e.store.GetRepoByURL(ctx, org, repoURL); !errors.Is(err, meta.ErrNotFound) {
		t.Errorf("repo row after createAbandon: err = %v, want ErrNotFound (soft-deleted)", err)
	}
	if _, err := creds.GetCredential(ctx, cred.ID); !errors.Is(err, upsync.ErrNotFound) {
		t.Errorf("credential row after createAbandon: err = %v, want ErrNotFound", err)
	}
	if _, err := ups.GetUpstream(ctx, repo.RepoID); !errors.Is(err, upsync.ErrNotFound) {
		t.Errorf("upstream row after createAbandon: err = %v, want ErrNotFound", err)
	}

	// Best-effort semantics: a second run over already-missing rows only
	// logs, never fails or panics.
	ss.createAbandon(ctx, org, repoURL, repo.RepoID)
}
