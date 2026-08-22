// GET /api/repo-urls/{repo_id} (docs/03, openapi get_repo_url_by_id — note:
// NO /v1 prefix): resolve an internal repository ID to the repository URL
// clients put in the JWT `repo` claim. Org-level org:read auth; the response
// is exactly {"repo_id", "repo_name", "url"} (openapi: additionalProperties
// false; url is the deprecated mirror of repo_name).
package api

import (
	"errors"
	"net/http"

	"codestorage/internal/auth"
	"codestorage/internal/meta"
)

// registerRepoURLRoutes wires the repo-urls surface: org-level org:read
// token → 200 {"repo_id", "repo_name", "url"}; unknown/deleted repo → 404
// {"error"} envelope; repo-scoped token → 403 (org-level auth).
func registerRepoURLRoutes(mux *http.ServeMux, s *server, verifier auth.Verifier, org string, rl *rateLimiter) {
	assertf(mux != nil && s != nil && verifier != nil, "registerRepoURLRoutes: nil dependency")
	mux.Handle("GET /api/repo-urls/{repo_id}",
		rl.limit(routeRead)(requireAuth(verifier, org, true, auth.ScopeOrgRead)(http.HandlerFunc(s.handleGetRepoURL))))
}

// repoURLResponse is the get_repo_url_by_id 200 shape (openapi.yaml).
type repoURLResponse struct {
	RepoID   string `json:"repo_id"`
	RepoName string `json:"repo_name"`
	URL      string `json:"url"` // deprecated: mirrors repo_name
}

// handleGetRepoURL answers GET /api/repo-urls/{repo_id}: GetRepoByID scoped
// to the token's org (soft-deleted rows are invisible to the live-repo
// query, so deleted and unknown IDs both 404).
func (s *server) handleGetRepoURL(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	repoID := r.PathValue("repo_id")
	// A {repo_id} wildcard never matches an empty segment.
	assertf(repoID != "", "handleGetRepoURL: empty repo_id path value")

	repo, err := s.store.GetRepoByID(r.Context(), claims.Iss, repoID)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case err != nil:
		s.internalErr(w, r, "resolve repo url", err)
		return
	}
	assertf(repo.RepoID == repoID, "GetRepoByID returned mismatched repo %q for id %q", repo.RepoID, repoID)
	writeJSON(w, http.StatusOK, repoURLResponse{RepoID: repo.RepoID, RepoName: repo.URL, URL: repo.URL})
}
