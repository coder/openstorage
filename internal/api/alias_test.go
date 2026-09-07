package api

// Integration tests for the named-repo route aliases /api/repos/{repo_name}
// (alias.go): table-driven byte-identity against the v1 twins (sampled
// matrix per docs/archive/phases/07-implementation-roadmap.md p6), URL-encoded name handling (the fixture repo URL
// contains slashes, so every alias path here exercises %2F segments),
// alias-by-repo_id, claim/path mismatch → 404 in the route's envelope
// flavor, and auth wiring (same flavor + scopes as v1).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/open-storage/internal/auth"
)

// aliasRequireIdentical asserts one alias request and its v1 twin produce a
// byte-identical response (status, Content-Type, body). Callers must pass
// side-effect-free requests: both are actually executed.
func aliasRequireIdentical(t *testing.T, e *testEnv, method, aliasPath, v1Path, token string, body []byte) {
	t.Helper()
	aliasResp, aliasBody := e.doReq(t, method, aliasPath, token, body, "")
	v1Resp, v1Body := e.doReq(t, method, v1Path, token, body, "")
	if aliasResp.StatusCode != v1Resp.StatusCode {
		t.Fatalf("%s %s = %d, v1 %s = %d (alias body %q, v1 body %q)",
			method, aliasPath, aliasResp.StatusCode, v1Path, v1Resp.StatusCode, aliasBody, v1Body)
	}
	if act, vct := aliasResp.Header.Get("Content-Type"), v1Resp.Header.Get("Content-Type"); act != vct {
		t.Errorf("%s %s Content-Type = %q, v1 = %q", method, aliasPath, act, vct)
	}
	if !bytes.Equal(aliasBody, v1Body) {
		t.Errorf("%s %s body != v1 body\nalias: %q\nv1:    %q", method, aliasPath, aliasBody, v1Body)
	}
}

func TestAliasV1Equivalence(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)
		if !strings.Contains(fx.repo.URL, "/") {
			t.Fatalf("fixture repo URL %q has no slash; the URL-encoding case needs one", fx.repo.URL)
		}
		esc := url.PathEscape(fx.repo.URL)
		writeToken := e.mint(t, tokenSpec{iss: org, repo: fx.repo.URL,
			scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})

		cases := []struct {
			name   string
			method string
			// suffix is appended (with any query) to both path styles.
			suffix string
			v1Path string
			token  string
			body   []byte
		}{
			{"repo", http.MethodGet, "", "/api/v1/repo", fx.token, nil},
			{"branches", http.MethodGet, "/branches", "/api/v1/repos/branches", fx.token, nil},
			{"branch", http.MethodGet, "/branch?name=main", "/api/v1/repos/branch?name=main", fx.token, nil},
			{"branch missing", http.MethodGet, "/branch?name=nope", "/api/v1/repos/branch?name=nope", fx.token, nil},
			{"commits", http.MethodGet, "/commits?limit=2", "/api/v1/repos/commits?limit=2", fx.token, nil},
			{"files", http.MethodGet, "/files?ref=main&recursive=true", "/api/v1/repos/files?ref=main&recursive=true", fx.token, nil},
			{"file", http.MethodGet, "/file?path=README.md", "/api/v1/repos/file?path=README.md", fx.token, nil},
			{"tags", http.MethodGet, "/tags", "/api/v1/repos/tags", fx.token, nil},
			{"tag", http.MethodGet, "/tag?name=v1-annot", "/api/v1/repos/tag?name=v1-annot", fx.token, nil},
			{"tag missing", http.MethodGet, "/tag?name=nope", "/api/v1/repos/tag?name=nope", fx.token, nil},
			// Write route: {} deterministically 400s in the shared handler,
			// so alias and v1 bodies must match without mutating state.
			{"branches/create", http.MethodPost, "/branches/create", "/api/v1/repos/branches/create", writeToken, []byte(`{}`)},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				aliasRequireIdentical(t, e, tc.method, "/api/repos/"+esc+tc.suffix, tc.v1Path, tc.token, tc.body)
			})
		}

		// Alias by repo_id in the path (meta.ResolveRepoKey ByID branch).
		t.Run("by-repo-id", func(t *testing.T) {
			aliasRequireIdentical(t, e, http.MethodGet,
				"/api/repos/"+url.PathEscape(fx.repo.RepoID), "/api/v1/repo", fx.token, nil)
		})

		// Org-level alias (PUT git-credentials) with an org token: the
		// alias only checks the path name resolves, then dispatches.
		t.Run("org-level-git-credentials", func(t *testing.T) {
			orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
			aliasRequireIdentical(t, e, http.MethodPut,
				"/api/repos/"+esc+"/git-credentials", "/api/v1/repos/git-credentials", orgTok, []byte(`{}`))
		})
	})
}

// TestAliasWriteThrough proves a mutating request through the alias reaches
// the real v1 handler with its body intact: a tag created via the alias is
// visible through the v1 listing.
func TestAliasWriteThrough(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)
		esc := url.PathEscape(fx.repo.URL)
		writeToken := e.mint(t, tokenSpec{iss: org, repo: fx.repo.URL, scopes: []string{auth.ScopeGitWrite}})

		tagName := "alias-tag-" + randomHex(t, 4)
		reqBody := fmt.Sprintf(`{"name": %q, "target": %q}`, tagName, fx.c1)
		resp, body := e.doReq(t, http.MethodPost, "/api/repos/"+esc+"/tags", writeToken, []byte(reqBody), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST alias tags = %d (%s), want 200", resp.StatusCode, body)
		}
		var created struct {
			Name string `json:"name"`
			SHA  string `json:"sha"`
		}
		if err := json.Unmarshal(body, &created); err != nil {
			t.Fatalf("unmarshal %q: %v", body, err)
		}
		if created.Name != tagName || created.SHA != fx.c1 {
			t.Fatalf("created tag = %+v, want name %q sha %q", created, tagName, fx.c1)
		}

		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/tags", fx.token, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET v1 tags = %d (%s), want 200", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), tagName) {
			t.Fatalf("v1 tag listing %q does not contain alias-created tag %q", body, tagName)
		}
	})
}

// TestAliasDeleteRepo proves DELETE /api/repos/{repo_name} re-dispatches to
// the v1 delete handler and answers the upstream-required 200 shape
// {repo_name, repo_id (deprecated upstream), message} — byte-identical to
// the v1 route by construction (issue #147). The v1 route's own shape is
// asserted in testCRUDHappyPath.
func TestAliasDeleteRepo(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "alias/delete-" + randomHex(t, 4)
		created := createRepo(t, e, org, repoURL, nil, http.StatusOK)
		writeTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})

		resp, body := e.doReq(t, http.MethodDelete, "/api/repos/"+url.PathEscape(repoURL), writeTok, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("DELETE alias = %d (%s), want 200", resp.StatusCode, body)
		}
		requireDeleteRepoShape(t, body, created.RepoID, repoURL)
	})
}

// TestAliasNotFound covers the alias-layer 404s: unknown path name, path
// repo != claim repo (read and write envelope flavors), and a claim that no
// longer resolves. Mismatches answer 404 — not 403 — so a foreign token
// cannot probe which names exist.
func TestAliasNotFound(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		ctx := context.Background()
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)
		esc := url.PathEscape(fx.repo.URL)

		otherURL := "https://example.com/" + org + "/other-" + randomHex(t, 4)
		if _, err := e.store.CreateRepo(ctx, org, otherURL, "main"); err != nil {
			t.Fatalf("create other repo: %v", err)
		}
		otherRead := e.mint(t, tokenSpec{iss: org, repo: otherURL, scopes: []string{auth.ScopeGitRead}})
		otherWrite := e.mint(t, tokenSpec{iss: org, repo: otherURL, scopes: []string{auth.ScopeGitWrite}})

		// Unknown path name with a valid token -> 404 error envelope.
		resp, body := e.doReq(t, http.MethodGet, "/api/repos/no-such-repo", fx.token, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("unknown name = %d (%s), want 404", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// Read route, claim repo != path repo -> 404 error envelope.
		resp, body = e.doReq(t, http.MethodGet, "/api/repos/"+esc, otherRead, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("read mismatch = %d (%s), want 404", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// Write route, claim repo != path repo -> 404 result envelope.
		resp, body = e.doReq(t, http.MethodPost, "/api/repos/"+esc+"/tags", otherWrite,
			[]byte(`{"name": "x", "target": "main"}`), "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("write mismatch = %d (%s), want 404", resp.StatusCode, body)
		}
		requireAuthResultEnvelope(t, body, "not_found")

		// Claim repo that never existed -> 404 (claim-side resolution).
		ghost := e.mint(t, tokenSpec{iss: org, repo: "https://example.com/" + org + "/ghost",
			scopes: []string{auth.ScopeGitRead}})
		resp, body = e.doReq(t, http.MethodGet, "/api/repos/"+esc, ghost, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("ghost claim = %d (%s), want 404", resp.StatusCode, body)
		}

		// An UNENCODED slashy name spans several path segments and must not
		// match the single-segment {repo_name} wildcard.
		resp, body = e.doReq(t, http.MethodGet, "/api/repos/"+fx.repo.URL, fx.token, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("unencoded name = %d (%s), want 404", resp.StatusCode, body)
		}
	})
}

// TestAliasAuthWiring asserts aliases carry the same auth flavor and scopes
// as their v1 twins: 401/403 shapes per route flavor, org tokens rejected on
// repo-scoped aliases, repo tokens rejected on the org-level alias.
func TestAliasAuthWiring(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)
		esc := url.PathEscape(fx.repo.URL)
		readToken := fx.token
		writeToken := e.mint(t, tokenSpec{iss: org, repo: fx.repo.URL, scopes: []string{auth.ScopeGitWrite}})
		orgToken := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg,
			scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite, auth.ScopeRepoWrite}})

		// Read-flavor aliases: {"error"} envelopes.
		for _, readPath := range []string{
			"/api/repos/" + esc + "/branches",
			"/api/repos/" + esc + "/branch?name=main",
			"/api/repos/" + esc + "/tag?name=v1-annot",
		} {
			resp, body := e.doReq(t, http.MethodGet, readPath, "", nil, "")
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("%s no token = %d, want 401", readPath, resp.StatusCode)
			}
			requireEnvelope(t, resp, body)
			resp, _ = e.doReq(t, http.MethodGet, readPath, writeToken, nil, "")
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s wrong scope = %d, want 403", readPath, resp.StatusCode)
			}
			resp, _ = e.doReq(t, http.MethodGet, readPath, orgToken, nil, "")
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s org token = %d, want 403", readPath, resp.StatusCode)
			}
		}

		// Write-flavor alias: docs/explanation/streaming-commit-protocols.md result envelopes (ADR-26).
		writePath := "/api/repos/" + esc + "/tags"
		resp, body := e.doReq(t, http.MethodPost, writePath, "", nil, "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("write alias no token = %d, want 401", resp.StatusCode)
		}
		if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
			t.Errorf("write alias 401 WWW-Authenticate = %q, want Bearer", got)
		}
		requireAuthResultEnvelope(t, body, "unauthorized")
		resp, body = e.doReq(t, http.MethodPost, writePath, readToken, nil, "")
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("write alias wrong scope = %d, want 403", resp.StatusCode)
		}
		requireAuthResultEnvelope(t, body, "forbidden")

		// Org-level alias rejects repo-scoped tokens.
		repoWriteToken := e.mint(t, tokenSpec{iss: org, repo: fx.repo.URL, scopes: []string{auth.ScopeRepoWrite}})
		resp, body = e.doReq(t, http.MethodPut, "/api/repos/"+esc+"/git-credentials", repoWriteToken, []byte(`{}`), "")
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("org-level alias repo token = %d (%s), want 403", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)
	})
}
