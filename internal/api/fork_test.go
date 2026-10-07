package api

// Contract integration tests for synchronous repo forking (phase-5
// workstream E): POST /api/v1/repos with base_repo {provider: "code",
// operation: "fork"}, fanned out over the pg and sqlite lanes via
// runBackends.
//
// api.New is frozen (route wiring is the integrator's job — see the seam
// note in handlers_fork.go), so each lane builds a dedicated httptest
// server through the SAME production middleware stack: requireAuth +
// forkAwareCreateRepo with the frozen (*server).handleCreateRepo as the
// fallback. The write-ref routes are mounted alongside for the
// independence checks; reads go through the lane's regular server (e.srv)
// as cross-surface assertions. Source fixtures reuse wrefSeedFixture
// (main=c2, feature=c1, annotated tag, ephemeral scratch).

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/meta"
)

// newForkServer mounts POST /api/v1/repos through the production auth
// middleware with forkAwareCreateRepo (fallback = the frozen create
// handler), plus the write-ref routes used by the independence tests.
func newForkServer(t *testing.T, e *testEnv) *httptest.Server {
	t.Helper()
	s := &server{
		store:       e.store,
		httpBaseURL: testHTTPBaseURL,
		logger:      discardLogger(),
		refStore:    e.refStore,
		blobStore:   e.blobStore,
		cacheDir:    e.cacheDir,
	}
	verifier := auth.NewVerifier(e.store)
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/repos",
		requireAuth(verifier, "", false, auth.ScopeRepoWrite)(s.forkAwareCreateRepo(verifier, s.handleCreateRepo)))
	s.registerWriteRefRoutes(mux, verifier, "", nil)
	srv := httptest.NewServer(recoverPanics(s.logger)(mux))
	t.Cleanup(srv.Close)
	return srv
}

// forkCaller is one destination identity: a fresh org (or an existing one)
// plus a unique new-repo url and its repo:write JWT.
type forkCaller struct {
	org   string
	url   string
	token string
}

func newForkCaller(t *testing.T, e *testEnv, org string) forkCaller {
	t.Helper()
	if org == "" {
		org = e.newOrg(t)
	}
	url := "https://example.com/" + org + "/fork-" + randomHex(t, 4)
	return forkCaller{
		org:   org,
		url:   url,
		token: e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeRepoWrite}}),
	}
}

// forkDo sends one POST /api/v1/repos and returns the response plus the
// decoded generic body.
func forkDo(t *testing.T, srv *httptest.Server, token string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/repos", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /api/v1/repos: %v", err)
	}
	defer resp.Body.Close()
	decoded := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return resp, decoded
}

// forkBody builds the fork request body. extra augments base_repo.
func forkBody(srcToken string, extra map[string]any) map[string]any {
	base := map[string]any{
		"provider":  "code",
		"operation": "fork",
		"auth":      map[string]any{"token": srcToken},
	}
	maps.Copy(base, extra)
	return map[string]any{"base_repo": base}
}

// forkRequireCreated asserts the create response shape and returns the new
// repo_id.
func forkRequireCreated(t *testing.T, resp *http.Response, body map[string]any) string {
	t.Helper()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, body %v, want 201", resp.StatusCode, body)
	}
	repoID, _ := body["repo_id"].(string)
	if repoID == "" || body["message"] != repoCreatedMsg(body["repo_name"].(string)) {
		t.Fatalf("create body = %v", body)
	}
	if body["http_url"] != testHTTPBaseURL+"/"+repoID+".git" {
		t.Fatalf("http_url = %v, want %s", body["http_url"], testHTTPBaseURL+"/"+repoID+".git")
	}
	return repoID
}

// forkHasObject reports whether the materialized repo cache contains sha.
func forkHasObject(t *testing.T, e *testEnv, repoID, sha string) bool {
	t.Helper()
	repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, repoID, "main")
	if err != nil {
		t.Fatalf("materialize %s: %v", repoID, err)
	}
	_, err = wrefGit(context.Background(), repoPath, nil, nil, "cat-file", "-e", sha)
	return err == nil
}

// forkRequireNoRepoRow asserts no live repo row exists for (org, url) — the
// failure-matrix invariant that a refused fork leaves nothing behind.
func forkRequireNoRepoRow(t *testing.T, e *testEnv, org, url string) {
	t.Helper()
	_, err := e.store.GetRepoByURL(context.Background(), org, url)
	if !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("GetRepoByURL(%s, %s) = %v, want ErrNotFound", org, url, err)
	}
}

func TestForkRepo(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newForkServer(t, e)
		src := wrefSeedFixture(t, e)
		srcReadTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeGitRead}})

		t.Run("fork at default branch", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, forkBody(srcReadTok, nil))
			forkID := forkRequireCreated(t, resp, body)

			refs := wrefRefs(t, e, forkID)
			if len(refs) != 1 || refs["refs/heads/main"] != src.c2 {
				t.Fatalf("fork refs = %v, want exactly refs/heads/main=%s", refs, src.c2)
			}
			// Cross-surface: the read API serves the forked content.
			readTok := e.mint(t, tokenSpec{iss: caller.org, repo: caller.url, scopes: []string{auth.ScopeGitRead}})
			rresp, rbody := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=a.txt&ref=main", readTok, nil, "")
			if rresp.StatusCode != http.StatusOK || string(rbody) != "v2\n" {
				t.Fatalf("fork a.txt = %d %q", rresp.StatusCode, rbody)
			}
			// The repo row honors the inherited default branch name.
			row, err := e.store.GetRepoByURL(context.Background(), caller.org, caller.url)
			if err != nil || row.DefaultBranch != "main" || row.RepoID != forkID {
				t.Fatalf("repo row = %+v err %v", row, err)
			}
		})

		t.Run("fork at ref", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, forkBody(srcReadTok, map[string]any{"ref": "feature"}))
			forkID := forkRequireCreated(t, resp, body)
			if refs := wrefRefs(t, e, forkID); refs["refs/heads/main"] != src.c1 {
				t.Fatalf("fork refs = %v, want refs/heads/main=%s", refs, src.c1)
			}
		})

		t.Run("fork at sha", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, forkBody(srcReadTok, map[string]any{"sha": src.c1}))
			forkID := forkRequireCreated(t, resp, body)
			if refs := wrefRefs(t, e, forkID); refs["refs/heads/main"] != src.c1 {
				t.Fatalf("fork refs = %v, want refs/heads/main=%s", refs, src.c1)
			}
		})

		t.Run("sha overrides ref and bounds the object set", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token,
				forkBody(srcReadTok, map[string]any{"ref": "main", "sha": src.c1}))
			forkID := forkRequireCreated(t, resp, body)
			if refs := wrefRefs(t, e, forkID); refs["refs/heads/main"] != src.c1 {
				t.Fatalf("fork refs = %v, want refs/heads/main=%s (sha wins)", refs, src.c1)
			}
			// The fork's object set is the c1 closure: c2 must be absent.
			if !forkHasObject(t, e, forkID, src.c1) {
				t.Fatal("fork is missing the fork-point commit")
			}
			if forkHasObject(t, e, forkID, src.c2) {
				t.Fatal("fork leaked objects beyond the fork point (c2 present)")
			}
		})

		t.Run("explicit default_branch", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			req := forkBody(srcReadTok, nil)
			req["default_branch"] = "trunk"
			resp, body := forkDo(t, srv, caller.token, req)
			forkID := forkRequireCreated(t, resp, body)
			refs := wrefRefs(t, e, forkID)
			if len(refs) != 1 || refs["refs/heads/trunk"] != src.c2 {
				t.Fatalf("fork refs = %v, want exactly refs/heads/trunk=%s", refs, src.c2)
			}
			row, err := e.store.GetRepoByURL(context.Background(), caller.org, caller.url)
			if err != nil || row.DefaultBranch != "trunk" {
				t.Fatalf("repo row = %+v err %v", row, err)
			}
		})

		// Issue #293: the optional body repo_name may only confirm the
		// destination's JWT repo claim (caller.url contains slashes).
		t.Run("repo_name equal to claim", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			req := forkBody(srcReadTok, nil)
			req["repo_name"] = caller.url
			resp, body := forkDo(t, srv, caller.token, req)
			forkID := forkRequireCreated(t, resp, body)
			if body["repo_name"] != caller.url {
				t.Fatalf("repo_name = %v, want %s", body["repo_name"], caller.url)
			}
			row, err := e.store.GetRepoByURL(context.Background(), caller.org, caller.url)
			if err != nil || row.RepoID != forkID {
				t.Fatalf("repo row = %+v err %v, want repo_id %s", row, err, forkID)
			}
		})

		t.Run("repo_name mismatch creates nothing", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			other := caller.url + "-other"
			for _, name := range []string{other, src.url, ""} {
				req := forkBody(srcReadTok, nil)
				req["repo_name"] = name
				resp, body := forkDo(t, srv, caller.token, req)
				if resp.StatusCode != http.StatusBadRequest || body["error"] != "repo_name must equal the JWT repo claim" {
					t.Fatalf("repo_name %q: status = %d body %v, want 400 name mismatch", name, resp.StatusCode, body)
				}
			}
			forkRequireNoRepoRow(t, e, caller.org, caller.url)
			forkRequireNoRepoRow(t, e, caller.org, other)
			// The fork source named in the body is untouched.
			if refs := wrefRefs(t, e, src.repoID); refs["refs/heads/main"] != src.c2 {
				t.Fatalf("source refs = %v, want refs/heads/main=%s", refs, src.c2)
			}
		})

		t.Run("repo_name mismatch on the frozen fallback", func(t *testing.T) {
			// No base_repo: the dispatcher replays to the frozen creator,
			// which enforces the same rule.
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, map[string]any{"repo_name": caller.url + "x"})
			if resp.StatusCode != http.StatusBadRequest || body["error"] != "repo_name must equal the JWT repo claim" {
				t.Fatalf("status = %d body %v, want 400 name mismatch", resp.StatusCode, body)
			}
			forkRequireNoRepoRow(t, e, caller.org, caller.url)
			resp, body = forkDo(t, srv, caller.token, map[string]any{"repo_name": caller.url})
			if resp.StatusCode != http.StatusCreated || body["repo_name"] != caller.url {
				t.Fatalf("equal repo_name status = %d body %v, want 201", resp.StatusCode, body)
			}
		})

		t.Run("cross-org fork", func(t *testing.T) {
			// The caller org differs from the source org; authorization to
			// read the source comes solely from the embedded source JWT.
			caller := newForkCaller(t, e, e.newOrg(t))
			if caller.org == src.org {
				t.Fatal("test bug: caller org must differ from source org")
			}
			resp, body := forkDo(t, srv, caller.token, forkBody(srcReadTok, nil))
			forkID := forkRequireCreated(t, resp, body)
			row, err := e.store.GetRepoByURL(context.Background(), caller.org, caller.url)
			if err != nil || row.RepoID != forkID {
				t.Fatalf("repo row in caller org = %+v err %v", row, err)
			}
			if refs := wrefRefs(t, e, forkID); refs["refs/heads/main"] != src.c2 {
				t.Fatalf("fork refs = %v", refs)
			}
		})

		t.Run("fork is bidirectionally independent", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, forkBody(srcReadTok, nil))
			forkID := forkRequireCreated(t, resp, body)

			// Ref-leak check: ONLY the default branch exists — the source's
			// feature branch, tags, and ephemeral refs must not leak.
			refs := wrefRefs(t, e, forkID)
			if len(refs) != 1 || refs["refs/heads/main"] != src.c2 {
				t.Fatalf("fork refs = %v, want exactly refs/heads/main", refs)
			}
			srcBefore := wrefRefs(t, e, src.repoID)

			// Write to the fork (API branch create): the source must not move.
			forkTok := e.mint(t, tokenSpec{iss: caller.org, repo: caller.url,
				scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
			wresp, wbody := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", forkTok,
				map[string]any{"target_branch": "fork-only", "base_branch": "main"})
			if wresp.StatusCode != http.StatusOK {
				t.Fatalf("branch create on fork: status %d body %v", wresp.StatusCode, wbody)
			}
			if got := wrefRefs(t, e, src.repoID); len(got) != len(srcBefore) {
				t.Fatalf("source refs changed after fork write: %v -> %v", srcBefore, got)
			}
			if _, leaked := wrefRefs(t, e, src.repoID)["refs/heads/fork-only"]; leaked {
				t.Fatal("fork write leaked into the source")
			}

			// Write to the source: the fork must not move.
			forkBefore := wrefRefs(t, e, forkID)
			wresp, wbody = wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", src.token,
				map[string]any{"target_branch": "src-only", "base_branch": "main"})
			if wresp.StatusCode != http.StatusOK {
				t.Fatalf("branch create on source: status %d body %v", wresp.StatusCode, wbody)
			}
			forkAfter := wrefRefs(t, e, forkID)
			if len(forkAfter) != len(forkBefore) {
				t.Fatalf("fork refs changed after source write: %v -> %v", forkBefore, forkAfter)
			}
			if _, leaked := forkAfter["refs/heads/src-only"]; leaked {
				t.Fatal("source write leaked into the fork")
			}
		})

		t.Run("plain create delegates to the fallback", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, map[string]any{"default_branch": "dev"})
			repoID := forkRequireCreated(t, resp, body)
			row, err := e.store.GetRepoByURL(context.Background(), caller.org, caller.url)
			if err != nil || row.RepoID != repoID || row.DefaultBranch != "dev" {
				t.Fatalf("repo row = %+v err %v", row, err)
			}
			// A plain create has no refs at all (unborn default branch).
			if refs := wrefRefs(t, e, repoID); len(refs) != 0 {
				t.Fatalf("plain create refs = %v, want none", refs)
			}
			// Empty body delegates too.
			caller2 := newForkCaller(t, e, caller.org)
			resp, body = forkDo(t, srv, caller2.token, nil)
			forkRequireCreated(t, resp, body)
		})

		t.Run("non-code base_repo delegates to the fallback", func(t *testing.T) {
			// Both the sync operation and a non-code fork provider replay to
			// the fallback verbatim — here the frozen creator, which rejects
			// every base_repo with its documented 400.
			for name, base := range map[string]map[string]any{
				"github sync": {"provider": "github", "owner": "o", "name": "n"},
				"github fork": {"provider": "github", "owner": "o", "name": "n", "operation": "fork"},
			} {
				caller := newForkCaller(t, e, "")
				resp, body := forkDo(t, srv, caller.token, map[string]any{"base_repo": base})
				if resp.StatusCode != http.StatusBadRequest || body["error"] != "base_repo not supported" {
					t.Errorf("%s: status %d body %v, want the fallback's 400", name, resp.StatusCode, body)
				}
				forkRequireNoRepoRow(t, e, caller.org, caller.url)
			}
		})

		t.Run("fork onto an existing url is 409", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, forkBody(srcReadTok, nil))
			forkRequireCreated(t, resp, body)
			resp, body = forkDo(t, srv, caller.token, forkBody(srcReadTok, nil))
			if resp.StatusCode != http.StatusConflict || body["error"] == "" {
				t.Fatalf("second fork: status %d body %v, want 409 envelope", resp.StatusCode, body)
			}
		})

		t.Run("failure matrix leaves no repo row", func(t *testing.T) {
			expiredTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url,
				scopes: []string{auth.ScopeGitRead}, expIn: -time.Hour})
			writeOnlyTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url,
				scopes: []string{auth.ScopeGitWrite}})
			orgTok := e.mint(t, tokenSpec{iss: src.org, repo: auth.RepoClaimOrg,
				scopes: []string{auth.ScopeGitRead}})
			unknownSrcTok := e.mint(t, tokenSpec{iss: src.org,
				repo: "https://example.com/" + src.org + "/no-such-repo", scopes: []string{auth.ScopeGitRead}})

			cases := []struct {
				name     string
				body     map[string]any
				wantCode int
			}{
				{"missing auth", map[string]any{"base_repo": map[string]any{
					"provider": "code", "operation": "fork"}}, http.StatusBadRequest},
				{"empty token", forkBody("", nil), http.StatusBadRequest},
				{"invalid token", forkBody("not-a-jwt", nil), http.StatusForbidden},
				{"expired token", forkBody(expiredTok, nil), http.StatusForbidden},
				{"wrong-scope token", forkBody(writeOnlyTok, nil), http.StatusForbidden},
				{"org-scoped token", forkBody(orgTok, nil), http.StatusForbidden},
				{"unknown source", forkBody(unknownSrcTok, nil), http.StatusNotFound},
				{"bad ref", forkBody(srcReadTok, map[string]any{"ref": "no-such-branch"}), http.StatusBadRequest},
				{"unknown sha", forkBody(srcReadTok, map[string]any{
					"sha": strings.Repeat("beef1234", 5)}), http.StatusBadRequest},
				{"malformed sha", forkBody(srcReadTok, map[string]any{"sha": "zzzz"}), http.StatusBadRequest},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					caller := newForkCaller(t, e, "")
					resp, body := forkDo(t, srv, caller.token, tc.body)
					if resp.StatusCode != tc.wantCode {
						t.Fatalf("status = %d body %v, want %d", resp.StatusCode, body, tc.wantCode)
					}
					if msg, _ := body["error"].(string); msg == "" {
						t.Fatalf("body %v is not an error envelope", body)
					}
					forkRequireNoRepoRow(t, e, caller.org, caller.url)
				})
			}

			t.Run("invalid default_branch", func(t *testing.T) {
				caller := newForkCaller(t, e, "")
				req := forkBody(srcReadTok, nil)
				req["default_branch"] = "a..b"
				resp, body := forkDo(t, srv, caller.token, req)
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status = %d body %v, want 400", resp.StatusCode, body)
				}
				forkRequireNoRepoRow(t, e, caller.org, caller.url)
			})
		})

		t.Run("caller ref policy denies the initial ref", func(t *testing.T) {
			org := e.newOrg(t)
			url := "https://example.com/" + org + "/fork-" + randomHex(t, 4)
			polTok := e.mint(t, tokenSpec{iss: org, repo: url,
				scopes: []string{auth.ScopeRepoWrite},
				refs:   `[["refs/heads/*", ["no-push"]]]`})
			resp, body := forkDo(t, srv, polTok, forkBody(srcReadTok, nil))
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d body %v, want 403", resp.StatusCode, body)
			}
			forkRequireNoRepoRow(t, e, org, url)
		})

		t.Run("missing token is 401", func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/repos",
				strings.NewReader(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
			}
		})
	})
}

// TestForkRelativeCacheDir: --cache-dir may be relative, and then
// gitread.Materialize returns a relative repo path that git also uses as
// its working directory. Paths handed to git through the environment must
// not be resolved against that working directory (PR #362 review).
func TestForkRelativeCacheDir(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		src := wrefSeedFixture(t, e)
		srcReadTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeGitRead}})
		caller := newForkCaller(t, e, "")
		// Hydrate the source cache first, as a git push does in production:
		// the fork itself must then only pack from it.
		if _, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, src.repoID, "main"); err != nil {
			t.Fatalf("materialize source: %v", err)
		}

		t.Chdir(filepath.Dir(e.cacheDir))
		s := &server{
			store:       e.store,
			httpBaseURL: testHTTPBaseURL,
			logger:      discardLogger(),
			refStore:    e.refStore,
			blobStore:   e.blobStore,
			cacheDir:    filepath.Base(e.cacheDir),
		}
		verifier := auth.NewVerifier(e.store)
		mux := http.NewServeMux()
		mux.Handle("POST /api/v1/repos",
			requireAuth(verifier, "", false, auth.ScopeRepoWrite)(s.forkAwareCreateRepo(verifier, s.handleCreateRepo)))
		srv := httptest.NewServer(recoverPanics(s.logger)(mux))
		t.Cleanup(srv.Close)

		resp, body := forkDo(t, srv, caller.token, forkBody(srcReadTok, nil))
		forkID := forkRequireCreated(t, resp, body)
		if refs := wrefRefs(t, e, forkID); refs["refs/heads/main"] != src.c2 {
			t.Fatalf("fork refs = %v, want refs/heads/main=%s", refs, src.c2)
		}
	})
}

// TestForkQuotaDoesNotTombstoneName (issue #21, Codex round 13): a fork
// whose closure exceeds the quota is denied BEFORE CreateRepo — a
// post-create denial would soft-delete the row and permanently burn the
// requested name (soft-deleted URLs stay reserved by design). The same
// name must fork successfully once the quota allows it.
func TestForkQuotaDoesNotTombstoneName(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		quotaSrv := func(quota int64) *httptest.Server {
			s := &server{
				store:          e.store,
				httpBaseURL:    testHTTPBaseURL,
				logger:         discardLogger(),
				refStore:       e.refStore,
				blobStore:      e.blobStore,
				cacheDir:       e.cacheDir,
				packQuotaBytes: quota,
			}
			verifier := auth.NewVerifier(e.store)
			mux := http.NewServeMux()
			mux.Handle("POST /api/v1/repos",
				requireAuth(verifier, "", false, auth.ScopeRepoWrite)(s.forkAwareCreateRepo(verifier, s.handleCreateRepo)))
			srv := httptest.NewServer(recoverPanics(s.logger)(mux))
			t.Cleanup(srv.Close)
			return srv
		}
		src := wrefSeedFixture(t, e)
		srcReadTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeGitRead}})
		caller := newForkCaller(t, e, "")

		// 1-byte quota: denied 403 naming the quota, and — critically —
		// no repo row exists for the URL afterward (name not burned).
		resp, body := forkDo(t, quotaSrv(1), caller.token, forkBody(srcReadTok, nil))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("over-quota fork = %d body %v", resp.StatusCode, body)
		}
		if msg, _ := body["error"].(string); !strings.Contains(msg, "quota") {
			t.Fatalf("error = %v, want quota denial", body)
		}
		if _, err := e.store.GetRepoByURL(context.Background(), caller.org, caller.url); !errors.Is(err, meta.ErrNotFound) {
			t.Fatalf("denied fork left a repo row (err %v): name tombstoned", err)
		}

		// The SAME name forks fine under a generous quota: no tombstone.
		resp, body = forkDo(t, quotaSrv(1<<20), caller.token, forkBody(srcReadTok, nil))
		forkID := forkRequireCreated(t, resp, body)
		if refs := wrefRefs(t, e, forkID); refs["refs/heads/main"] != src.c2 {
			t.Fatalf("fork refs = %v, want refs/heads/main=%s", refs, src.c2)
		}
	})
}
