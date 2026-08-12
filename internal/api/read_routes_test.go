package api

// Auth-wiring tests for the p3 git:read surface: every route is registered
// under requireAuth with scope git:read. Handler behavior is covered by the
// contract tests in read_core_test.go / read_query_test.go.

import (
	"net/http"
	"testing"

	"codestorage/internal/auth"
)

// readRoutes is the full docs/07 p3 route set plus the notes read routes
// (method + path).
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
	{http.MethodGet, "/api/v1/repos/notes"},
	{http.MethodGet, "/api/v1/repos/notes/refs"},
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

			// Proper git:read token passes auth and reaches the handler
			// (the 401-without-token check above already proves the route is
			// registered; an unregistered path would 404 before auth).
			resp, _ = e.doReq(t, route.method, route.path, readToken, nil, "")
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				t.Errorf("%s %s with read token = %d, want auth to pass", route.method, route.path, resp.StatusCode)
			}
		}
	})
}
