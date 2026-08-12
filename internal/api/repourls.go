// GET /api/repo-urls/{repo_id} (docs/03, openapi get_repo_url_by_id — note:
// NO /v1 prefix). Foundation wiring for the phase-6 repo-urls slice
// (workstream C): route + auth + rate-limit hook are frozen here; the slice
// fills in the handler body.
package api

import (
	"net/http"

	"codestorage/internal/auth"
)

// registerRepoURLRoutes wires the repo-urls surface: org-level org:read
// token → 200 {"repo_id": "...", "url": "..."}; unknown/deleted repo → 404
// {"error"} envelope; repo-scoped token → 403 (org-level auth).
func registerRepoURLRoutes(mux *http.ServeMux, s *server, verifier auth.Verifier, rl *rateLimiter) {
	assertf(mux != nil && s != nil && verifier != nil, "registerRepoURLRoutes: nil dependency")
	mux.Handle("GET /api/repo-urls/{repo_id}",
		rl.limit(routeRead)(requireAuth(verifier, true, auth.ScopeOrgRead)(http.HandlerFunc(s.handleGetRepoURL))))
}

// handleGetRepoURL answers GET /api/repo-urls/{repo_id}. Foundation stub:
// workstream C replaces this body with the GetRepoByID(claims.Iss,
// PathValue("repo_id")) lookup.
func (s *server) handleGetRepoURL(w http.ResponseWriter, _ *http.Request) {
	writeErr(w, http.StatusNotImplemented, "repo-urls not implemented yet")
}
