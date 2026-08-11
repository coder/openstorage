package api

// Scaffolding tests for the p3 git:read surface: every route is registered
// under requireAuth with scope git:read and currently answers 501 (endpoint
// slices replace the stubs and these expectations with real contract tests).

import (
	"net/http"
	"testing"

	"codestorage/internal/auth"
)

// readRoutes is the full docs/07 p3 route set (method + path).
var readRoutes = []struct{ method, path string }{
	{http.MethodGet, "/api/v1/repos/file"},
	{http.MethodHead, "/api/v1/repos/file"}, // GET patterns match HEAD too
	{http.MethodGet, "/api/v1/repos/files"},
	{http.MethodGet, "/api/v1/repos/files/metadata"},
	{http.MethodGet, "/api/v1/repos/branches"},
	{http.MethodGet, "/api/v1/repos/branches/diff"},
	{http.MethodGet, "/api/v1/repos/commits"},
	{http.MethodGet, "/api/v1/repos/commit"},
	{http.MethodGet, "/api/v1/repos/diff"},
	{http.MethodGet, "/api/v1/repos/tags"},
	{http.MethodGet, "/api/v1/repos/blame"},
	{http.MethodPost, "/api/v1/repos/grep"},
	{http.MethodPost, "/api/v1/repos/archive"},
}

func TestReadRoutesAuthWiring(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/org/read-" + randomHex(t, 4)

		readToken := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
		writeOnlyToken := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitWrite}})
		orgToken := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeGitRead}})

		for _, route := range readRoutes {
			// No token -> 401 with the error envelope + WWW-Authenticate.
			resp, body := e.doReq(t, route.method, route.path, "", nil, "")
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("%s %s without token = %d, want 401", route.method, route.path, resp.StatusCode)
			}
			if route.method != http.MethodHead {
				requireEnvelope(t, resp, body)
			}

			// Wrong scope -> 403.
			resp, _ = e.doReq(t, route.method, route.path, writeOnlyToken, nil, "")
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s %s with git:write-only token = %d, want 403", route.method, route.path, resp.StatusCode)
			}

			// Org-scoped token on a repo route -> 403.
			resp, _ = e.doReq(t, route.method, route.path, orgToken, nil, "")
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s %s with org token = %d, want 403", route.method, route.path, resp.StatusCode)
			}

			// Proper git:read token reaches the (stub) handler -> 501.
			resp, _ = e.doReq(t, route.method, route.path, readToken, nil, "")
			if resp.StatusCode != http.StatusNotImplemented {
				t.Errorf("%s %s with read token = %d, want 501 stub", route.method, route.path, resp.StatusCode)
			}
		}
	})
}
