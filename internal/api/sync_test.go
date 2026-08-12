package api

// Integration tests for the phase-5 upstream-sync surface (handlers_sync.go
// + internal/sync): base_repo sync creation, pull-upstream, detach, and the
// docs/06 repo.sync.* lifecycle with the exact documented error strings.
//
// Topology per stack: the API mux registers ONLY this slice's routes
// (api.New is frozen and does not call the register funcs yet — see the
// integration seam note in handlers_credentials.go). The upstream is a real
// `git http-backend` CGI server behind a basic-auth gate; the loopback push
// target is a second CGI server whose gate verifies the worker's minted JWT
// with the production auth.Verifier (an in-process githttp server cannot
// accept pushes inside `go test`: its hooks re-exec os.Executable(), which
// is the test binary — the e2e suite covers that path with the real binary).

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/meta"
	upsync "codestorage/internal/sync"
)

// usyncGit runs one git command hermetically (host config masked, fixed
// identity), failing the test on error.
func usyncGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
		"GIT_TERMINAL_PROMPT=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// usyncBackendPath locates git-http-backend next to the git installation.
func usyncBackendPath(t *testing.T) string {
	t.Helper()
	execPath := usyncGit(t, ".", "--exec-path")
	backend := filepath.Join(execPath, "git-http-backend")
	if _, err := os.Stat(backend); err != nil {
		t.Fatalf("git-http-backend not found at %s: %v", backend, err)
	}
	return backend
}

// usyncCGIServer serves root over git http-backend. gate returns false to
// reject the request with 401. autoInit creates missing bare repos on
// demand (the loopback target repo is created by the sync worker's push).
func usyncCGIServer(t *testing.T, root string, gate func(r *http.Request) bool, autoInit bool) *httptest.Server {
	t.Helper()
	backend := usyncBackendPath(t)
	h := &cgi.Handler{
		Path: backend,
		Env: []string{
			"GIT_PROJECT_ROOT=" + root,
			"GIT_HTTP_EXPORT_ALL=1",
			"GIT_CONFIG_GLOBAL=/dev/null",
			"GIT_CONFIG_SYSTEM=/dev/null",
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !gate(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="usync"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		if autoInit {
			elem, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
			if strings.HasSuffix(elem, ".git") {
				bare := filepath.Join(root, elem)
				if _, err := os.Stat(filepath.Join(bare, "HEAD")); err != nil {
					usyncGit(t, root, "init", "--bare", "-b", "main", bare)
					usyncGit(t, root, "-C", bare, "config", "http.receivepack", "true")
				}
			}
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// usyncSeedUpstream creates root/{owner}/{name}.git with one commit on main
// and tag v1, returning the head SHA.
func usyncSeedUpstream(t *testing.T, root, owner, name string) string {
	t.Helper()
	bare := filepath.Join(root, owner, name+".git")
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatal(err)
	}
	usyncGit(t, root, "init", "--bare", "-b", "main", bare)
	work := t.TempDir()
	usyncGit(t, work, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(work, "f.txt"), []byte("v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	usyncGit(t, work, "add", ".")
	usyncGit(t, work, "commit", "-m", "c1")
	usyncGit(t, work, "tag", "v1")
	usyncGit(t, work, "push", "file://"+bare, "main", "v1")
	return usyncGit(t, work, "rev-parse", "HEAD")
}

// usyncAddUpstreamCommit appends one commit to the seeded upstream.
func usyncAddUpstreamCommit(t *testing.T, root, owner, name string) string {
	t.Helper()
	bare := filepath.Join(root, owner, name+".git")
	work := t.TempDir()
	usyncGit(t, work, "clone", "file://"+bare, ".")
	if err := os.WriteFile(filepath.Join(work, "g.txt"), []byte("v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	usyncGit(t, work, "add", ".")
	usyncGit(t, work, "commit", "-m", "c2")
	usyncGit(t, work, "push", "origin", "main")
	return usyncGit(t, work, "rev-parse", "HEAD")
}

// usyncSink forwards parsed events onto a channel.
type usyncSink struct {
	ch chan upsync.Event
}

func (s *usyncSink) Emit(_ context.Context, _, eventType string, payload []byte) {
	var e upsync.Event
	if err := json.Unmarshal(payload, &e); err != nil {
		panic("usyncSink: bad payload: " + err.Error())
	}
	if e.Type != eventType {
		panic("usyncSink: payload type " + e.Type + " != header " + eventType)
	}
	s.ch <- e
}

// usyncStack is one isolated sync test universe: an org, an API server with
// only this slice's routes, a MemStore, an in-process queue, and a
// JWT-gated CGI loopback.
type usyncStack struct {
	e        *testEnv
	org      string
	verifier auth.Verifier
	mem      *upsync.MemStore
	events   chan upsync.Event
	api      *httptest.Server
	loopRoot string
}

func newUsyncStack(t *testing.T, e *testEnv) *usyncStack {
	t.Helper()
	s := &usyncStack{
		e:        e,
		org:      e.newOrg(t),
		verifier: auth.NewVerifier(e.store),
		mem:      upsync.NewMemStore(),
		events:   make(chan upsync.Event, 64),
		loopRoot: t.TempDir(),
	}

	// Loopback: basic auth user "t" + verified git:write JWT for this org
	// (docs/05) — the same checks githttp performs before receive-pack.
	loop := usyncCGIServer(t, s.loopRoot, func(r *http.Request) bool {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "t" {
			return false
		}
		claims, err := s.verifier.Verify(r.Context(), pass)
		return err == nil && claims.Iss == s.org && claims.HasScope(auth.ScopeGitWrite)
	}, true)

	worker := &upsync.Worker{
		Upstreams:   s.mem,
		Credentials: s.mem,
		Repos: func(ctx context.Context, org, repoID string) (upsync.RepoInfo, error) {
			repo, err := e.store.GetRepoByID(ctx, org, repoID)
			if errors.Is(err, meta.ErrNotFound) {
				return upsync.RepoInfo{}, upsync.ErrNotFound
			}
			if err != nil {
				return upsync.RepoInfo{}, err
			}
			return upsync.RepoInfo{RepoID: repo.RepoID, URL: repo.URL}, nil
		},
		Events: &usyncSink{ch: s.events},
		Loopback: upsync.Loopback{
			BaseURL: loop.URL,
			Token: func(_ context.Context, org string, repo upsync.RepoInfo) (string, error) {
				return e.mint(t, tokenSpec{iss: org, repo: repo.RepoID, scopes: []string{auth.ScopeGitWrite}}), nil
			},
		},
		Logger:  discardLogger(),
		Timeout: time.Minute,
	}
	queue := upsync.NewInProcessQueue(worker.Run, discardLogger())
	t.Cleanup(queue.Close)

	ss := newSyncServer(e.store, s.mem, s.mem, queue, testHTTPBaseURL, discardLogger())
	mux := http.NewServeMux()
	// The frozen api.New will swap its POST /repos route to this handler;
	// the test mux registers the same shape.
	mux.Handle("POST /api/v1/repos",
		requireAuth(s.verifier, false, auth.ScopeRepoWrite)(http.HandlerFunc(ss.handleCreateRepo)))
	registerCredentialRoutes(mux, s.verifier, ss, nil)
	registerSyncRoutes(mux, s.verifier, ss, nil)
	s.api = httptest.NewServer(mux)
	t.Cleanup(s.api.Close)
	return s
}

// req performs one request against the stack's API server.
func (s *usyncStack) req(t *testing.T, method, path, token string, body []byte) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, s.api.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.api.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, respBody
}

// waitEvent blocks for the next sync event and asserts its type.
func (s *usyncStack) waitEvent(t *testing.T, wantType string) upsync.Event {
	t.Helper()
	select {
	case e := <-s.events:
		if e.Type != wantType {
			t.Fatalf("next event = %+v, want type %q", e, wantType)
		}
		return e
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out waiting for %s event", wantType)
		return upsync.Event{}
	}
}

// assertNoEvent asserts no sync event arrives within the grace window.
func (s *usyncStack) assertNoEvent(t *testing.T) {
	t.Helper()
	select {
	case e := <-s.events:
		t.Fatalf("unexpected event: %+v", e)
	case <-time.After(500 * time.Millisecond):
	}
}

// createRepo POSTs /api/v1/repos through this slice's handler and returns
// the repo_id. body nil means an empty create.
func (s *usyncStack) createRepo(t *testing.T, repoURL string, body []byte) string {
	t.Helper()
	token := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
	resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos", token, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create repo: status %d: %s", resp.StatusCode, respBody)
	}
	var out struct {
		RepoID string `json:"repo_id"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil || out.RepoID == "" {
		t.Fatalf("create repo response %q: %v", respBody, err)
	}
	return out.RepoID
}

// loopbackSHA resolves a ref in the loopback bare repo ("" when absent).
func (s *usyncStack) loopbackSHA(t *testing.T, repoID, ref string) string {
	t.Helper()
	bare := filepath.Join(s.loopRoot, repoID+".git")
	cmd := exec.Command("git", "-C", bare, "rev-parse", "--verify", ref)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func basicAuthEquals(user, pass, wantUser, wantPass string) bool {
	return subtle.ConstantTimeCompare([]byte(user), []byte(wantUser)) == 1 &&
		subtle.ConstantTimeCompare([]byte(pass), []byte(wantPass)) == 1
}

// TestUpstreamSyncLifecycle drives the full docs/06 story: base_repo create
// with auth.token -> first sync (started/succeeded, run_count 0->1,
// is_first_sync) -> pull-upstream 202 + second sync -> broken credential ->
// failed with "authentication failed" -> idempotent detach.
func TestUpstreamSyncLifecycle(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		upRoot := t.TempDir()
		const upstreamPass = "up-secret"
		upstream := usyncCGIServer(t, upRoot, func(r *http.Request) bool {
			user, pass, ok := r.BasicAuth()
			return ok && basicAuthEquals(user, pass, "git", upstreamPass)
		}, false)
		headSHA := usyncSeedUpstream(t, upRoot, "own", "proj")

		repoURL := "team/mirror-" + randomHex(t, 4)
		body, err := json.Marshal(map[string]any{
			"base_repo": map[string]any{
				"provider":      "gitea",
				"owner":         "own",
				"name":          "proj",
				"upstream_host": upstream.URL,
				"auth":          map[string]string{"token": upstreamPass},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		repoID := s.createRepo(t, repoURL, body)

		// First sync: enqueued by the create handler (invariant 11).
		started := s.waitEvent(t, upsync.EventSyncStarted)
		if started.RunCount != 0 || !started.IsFirstSync || started.StartedAt == "" {
			t.Fatalf("bad started event: %+v", started)
		}
		if started.Repository.ID != repoID || started.Repository.URL != repoURL {
			t.Fatalf("bad started repository: %+v", started.Repository)
		}
		done := s.waitEvent(t, upsync.EventSyncSucceeded)
		if done.RunCount != 1 || !done.IsFirstSync || done.CompletedAt == "" || done.Error != "" {
			t.Fatalf("bad succeeded event: %+v", done)
		}
		if got := s.loopbackSHA(t, repoID, "refs/heads/main"); got != headSHA {
			t.Fatalf("loopback main = %q, want %q", got, headSHA)
		}
		if got := s.loopbackSHA(t, repoID, "refs/tags/v1"); got != headSHA {
			t.Fatalf("loopback tag v1 = %q, want %q", got, headSHA)
		}

		// pull-upstream: exact 202 body, then a second lifecycle run.
		newHead := usyncAddUpstreamCommit(t, upRoot, "own", "proj")
		writeToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeGitWrite}})
		resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos/pull-upstream", writeToken, []byte(`{"ref":"main"}`))
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("pull-upstream status %d: %s", resp.StatusCode, respBody)
		}
		var accepted map[string]string
		if err := json.Unmarshal(respBody, &accepted); err != nil {
			t.Fatal(err)
		}
		if accepted["message"] != "Repository sync initiated successfully" || len(accepted) != 1 {
			t.Fatalf("202 body = %s", respBody)
		}
		second := s.waitEvent(t, upsync.EventSyncStarted)
		if second.RunCount != 1 || second.IsFirstSync {
			t.Fatalf("bad second started event: %+v", second)
		}
		secondDone := s.waitEvent(t, upsync.EventSyncSucceeded)
		if secondDone.RunCount != 2 || secondDone.IsFirstSync {
			t.Fatalf("bad second succeeded event: %+v", secondDone)
		}
		if got := s.loopbackSHA(t, repoID, "refs/heads/main"); got != newHead {
			t.Fatalf("loopback main after pull = %q, want %q", got, newHead)
		}

		// A token carrying any restrictive ref policy must not trigger a
		// sync: the loopback push runs with an unpoliced internal token,
		// so pull-upstream would launder policed ref updates.
		policedToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL,
			scopes: []string{auth.ScopeGitWrite}, refs: `[["main", ["no-push"]]]`})
		resp, respBody = s.req(t, http.MethodPost, "/api/v1/repos/pull-upstream", policedToken, nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("policed pull-upstream status %d: %s", resp.StatusCode, respBody)
		}
		var denied map[string]string
		if err := json.Unmarshal(respBody, &denied); err != nil || denied["error"] != `ref policy op "no-push" denies pull-upstream` {
			t.Fatalf("policed pull-upstream body = %s (err %v)", respBody, err)
		}
		s.assertNoEvent(t)

		// Break the credential (org-scoped PUT), pull again: the exact
		// documented failure string must surface.
		cred, err := s.mem.CredentialForRepo(context.Background(), repoID)
		if err != nil {
			t.Fatalf("stored credential missing: %v", err)
		}
		orgToken := s.e.mint(t, tokenSpec{iss: s.org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		putBody := []byte(`{"id":"` + cred.ID + `","password":"wrong-password"}`)
		resp, respBody = s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", orgToken, putBody)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("update credential status %d: %s", resp.StatusCode, respBody)
		}
		resp, _ = s.req(t, http.MethodPost, "/api/v1/repos/pull-upstream", writeToken, nil)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("pull-upstream (broken cred) status %d", resp.StatusCode)
		}
		s.waitEvent(t, upsync.EventSyncStarted)
		failed := s.waitEvent(t, upsync.EventSyncFailed)
		if failed.Error != "authentication failed" {
			t.Fatalf("failed.error = %q, want %q", failed.Error, "authentication failed")
		}
		if failed.RunCount != 3 || failed.CompletedAt == "" {
			t.Fatalf("bad failed event: %+v", failed)
		}

		// Idempotent detach, then pull-upstream refuses.
		repoToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
		for range 2 {
			resp, respBody = s.req(t, http.MethodDelete, "/api/v1/repos/base", repoToken, nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("detach status %d: %s", resp.StatusCode, respBody)
			}
			var detached map[string]string
			if err := json.Unmarshal(respBody, &detached); err != nil {
				t.Fatal(err)
			}
			if detached["message"] != "repository detached" {
				t.Fatalf("detach body = %s", respBody)
			}
		}
		resp, respBody = s.req(t, http.MethodPost, "/api/v1/repos/pull-upstream", writeToken, nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("pull-upstream after detach status %d: %s", resp.StatusCode, respBody)
		}
		var envelope map[string]string
		if err := json.Unmarshal(respBody, &envelope); err != nil || envelope["error"] == "" {
			t.Fatalf("pull-upstream after detach body = %s (err %v)", respBody, err)
		}
		s.assertNoEvent(t)
	})
}

// TestUpstreamSyncFailureStrings covers the documented failure strings the
// lifecycle test does not: unreachable upstream host.
func TestUpstreamSyncFailureStrings(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		repoURL := "team/unreachable-" + randomHex(t, 4)
		body := []byte(`{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"http://127.0.0.1:1"}}`)
		s.createRepo(t, repoURL, body)
		s.waitEvent(t, upsync.EventSyncStarted)
		failed := s.waitEvent(t, upsync.EventSyncFailed)
		if failed.Error != "upstream repository unreachable" {
			t.Fatalf("failed.error = %q, want %q", failed.Error, "upstream repository unreachable")
		}
	})
}

// TestCreateRepoBaseRepoValidation checks the 400 envelopes for rejected
// base_repo objects; no repo row may be created on rejection.
func TestCreateRepoBaseRepoValidation(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		cases := []struct {
			name     string
			baseRepo string
			wantMsg  string
		}{
			{"unknown provider", `{"provider":"svn","owner":"o","name":"n"}`, `unsupported base_repo provider "svn"`},
			{"fork not supported", `{"provider":"code","owner":"o","name":"n","operation":"fork"}`, `base_repo operation "fork" not supported`},
			{"code requires fork", `{"provider":"code","owner":"o","name":"n"}`, `base_repo provider "code" requires operation "fork"`},
			{"bad operation", `{"provider":"github","owner":"o","name":"n","operation":"mirror"}`, `invalid base_repo operation "mirror"`},
			{"gitea needs host", `{"provider":"gitea","owner":"o","name":"n"}`, `base_repo upstream_host is required for provider "gitea"`},
			{"missing owner", `{"provider":"github","name":"n"}`, "base_repo owner and name are required"},
			// upstream_host becomes a git fetch URL: only http(s) bases or
			// bare hostname[:port] forms may reach the sync worker.
			{"file upstream_host", `{"provider":"gitea","owner":"o","name":"n","upstream_host":"file:///srv/git"}`, `invalid base_repo upstream_host "file:///srv/git"`},
			{"ssh upstream_host", `{"provider":"gitea","owner":"o","name":"n","upstream_host":"ssh://git@host/x"}`, `invalid base_repo upstream_host "ssh://git@host/x"`},
			{"ext transport upstream_host", `{"provider":"gitea","owner":"o","name":"n","upstream_host":"ext::sh -c id"}`, `invalid base_repo upstream_host "ext::sh -c id"`},
			{"userinfo upstream_host", `{"provider":"github","owner":"o","name":"n","upstream_host":"https://u:p@github.com"}`, `invalid base_repo upstream_host "https://u:p@github.com"`},
			{"path in bare upstream_host", `{"provider":"gitea","owner":"o","name":"n","upstream_host":"host.example/../../etc"}`, `invalid base_repo upstream_host "host.example/../../etc"`},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				repoURL := "team/invalid-" + randomHex(t, 4)
				token := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
				resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos", token, []byte(`{"base_repo":`+c.baseRepo+`}`))
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status %d: %s", resp.StatusCode, respBody)
				}
				var envelope map[string]string
				if err := json.Unmarshal(respBody, &envelope); err != nil {
					t.Fatal(err)
				}
				if envelope["error"] != c.wantMsg {
					t.Fatalf("error = %q, want %q", envelope["error"], c.wantMsg)
				}
				// Rejection must not leave a repo row behind.
				if _, err := e.store.GetRepoByURL(context.Background(), s.org, repoURL); !errors.Is(err, meta.ErrNotFound) {
					t.Fatalf("repo %q exists after rejected create (err %v)", repoURL, err)
				}
			})
		}
	})
}

// TestPullUpstreamAndDetachWithoutBase: both endpoints on a repo that never
// had an upstream, plus the 404 for a missing repo.
func TestPullUpstreamAndDetachWithoutBase(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		repoURL := "team/plain-" + randomHex(t, 4)
		s.createRepo(t, repoURL, nil)
		s.assertNoEvent(t) // no base_repo -> no sync scheduled

		writeToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeGitWrite}})
		resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos/pull-upstream", writeToken, nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("pull-upstream status %d: %s", resp.StatusCode, respBody)
		}
		var envelope map[string]string
		if err := json.Unmarshal(respBody, &envelope); err != nil || envelope["error"] == "" {
			t.Fatalf("pull-upstream body = %s (err %v)", respBody, err)
		}

		// Detach without an upstream is still an idempotent 200.
		repoToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
		resp, _ = s.req(t, http.MethodDelete, "/api/v1/repos/base", repoToken, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("detach status %d", resp.StatusCode)
		}

		// Unknown repo -> 404 on both.
		ghostToken := s.e.mint(t, tokenSpec{iss: s.org, repo: "team/ghost", scopes: []string{auth.ScopeGitWrite}})
		resp, _ = s.req(t, http.MethodPost, "/api/v1/repos/pull-upstream", ghostToken, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("pull-upstream ghost status %d", resp.StatusCode)
		}
		ghostRepoToken := s.e.mint(t, tokenSpec{iss: s.org, repo: "team/ghost", scopes: []string{auth.ScopeRepoWrite}})
		resp, _ = s.req(t, http.MethodDelete, "/api/v1/repos/base", ghostRepoToken, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("detach ghost status %d", resp.StatusCode)
		}
	})
}
