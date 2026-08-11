package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/meta"
)

// server holds handler dependencies; one instance backs all routes.
type server struct {
	store       meta.Store
	httpBaseURL string // no trailing slash (normalized in New)
}

// maxBodyBytes caps request bodies; the only JSON body we accept
// ({default_branch}) is tiny.
const maxBodyBytes = 1 << 20

// mustClaims returns the verified claims injected by requireAuth. Handlers
// are only reachable through the auth middleware, so absence is a bug.
func mustClaims(r *http.Request) *auth.Claims {
	claims, ok := auth.ClaimsFromContext(r.Context())
	assertf(ok, "handler reached without verified claims (route not wrapped in requireAuth?)")
	return claims
}

// branchComponentRe matches one conservative git ref component: leading
// alphanumeric, then alphanumerics plus . _ - (docs/03: invalid input -> 400).
var branchComponentRe = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]*\z`)

// validBranchName reports whether name is an acceptable default branch:
// slash-separated conservative ref components, no "..", no ".lock" suffix.
func validBranchName(name string) bool {
	if name == "" || len(name) > 255 || strings.Contains(name, "..") {
		return false
	}
	for _, comp := range strings.Split(name, "/") {
		if !branchComponentRe.MatchString(comp) || strings.HasSuffix(comp, ".lock") {
			return false
		}
	}
	return true
}

type createRepoRequest struct {
	DefaultBranch string          `json:"default_branch"`
	BaseRepo      json.RawMessage `json:"base_repo"`
}

type createRepoResponse struct {
	RepoID  string `json:"repo_id"`
	HTTPURL string `json:"http_url"`
	Message string `json:"message"`
}

// handleCreateRepo implements POST /api/v1/repos (scope repo:write). The new
// repo's url is the JWT `repo` claim; the optional body carries
// {default_branch?, base_repo?} (docs/03).
func (s *server) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	url := claims.Repo
	// requireAuth(orgLevel=false) rejects empty/"org" repo claims with 403.
	assertf(url != "" && url != auth.RepoClaimOrg, "create: repo claim %q passed requireAuth", url)

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	var req createRepoRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
	}
	// Fork/upstream sync is a later phase; silently dropping base_repo would
	// lie to clients, so reject it outright.
	if len(req.BaseRepo) > 0 && string(req.BaseRepo) != "null" {
		writeErr(w, http.StatusBadRequest, "base_repo not supported")
		return
	}
	branch := req.DefaultBranch
	if branch == "" {
		// openapi.yaml: optional query override, used only when the body
		// does not already specify a default branch.
		branch = r.URL.Query().Get("default_branch")
	}
	if branch == "" {
		branch = "main"
	}
	if !validBranchName(branch) {
		writeErr(w, http.StatusBadRequest, "invalid default_branch")
		return
	}

	repo, err := s.store.CreateRepo(r.Context(), claims.Iss, url, branch)
	switch {
	case errors.Is(err, meta.ErrAlreadyExists):
		writeErr(w, http.StatusConflict, "repository already exists")
		return
	case errors.Is(err, meta.ErrNotFound):
		// Unreachable after successful auth (the org must exist to have
		// verification keys), but map it defensively.
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "create repository: "+err.Error())
		return
	}
	assertf(repo.RepoID != "", "store returned repo with empty RepoID")
	writeJSON(w, http.StatusOK, createRepoResponse{
		RepoID:  repo.RepoID,
		HTTPURL: s.httpBaseURL + "/" + repo.RepoID + ".git",
		Message: "repository created",
	})
}

// handleGetRepo implements GET /api/v1/repo (scope git:read). The target
// repo is the JWT `repo` claim; 404 when absent or soft-deleted. The
// response schema (openapi.yaml) requires repo_id, url, default_branch,
// created_at — repoEntry carries exactly those.
func (s *server) handleGetRepo(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	repo, err := s.store.GetRepoByURL(r.Context(), claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "get repository: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, repoEntry{
		RepoID:        repo.RepoID,
		URL:           repo.URL,
		DefaultBranch: repo.DefaultBranch,
		CreatedAt:     repo.CreatedAt.UTC().Format(time.RFC3339),
	})
}

type repoEntry struct {
	RepoID        string `json:"repo_id"`
	URL           string `json:"url"`
	DefaultBranch string `json:"default_branch"`
	CreatedAt     string `json:"created_at"`
}

type listReposResponse struct {
	Repos      []repoEntry `json:"repos"`
	NextCursor string      `json:"next_cursor"`
	HasMore    bool        `json:"has_more"`
}

const (
	defaultListLimit = 20
	maxListLimit     = 100
)

// handleListRepos implements GET /api/v1/repos (scope org:read, repo claim
// "org"): keyset pagination via opaque cursor, `q` substring filter on url.
func (s *server) handleListRepos(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	query := r.URL.Query()

	limit := defaultListLimit
	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxListLimit {
			writeErr(w, http.StatusBadRequest, "invalid limit (want 1..100)")
			return
		}
		limit = n
	}

	res, err := s.store.ListRepos(r.Context(), claims.Iss, meta.ListParams{
		Q:      strings.TrimSpace(query.Get("q")),
		Cursor: query.Get("cursor"),
		Limit:  limit,
	})
	switch {
	case errors.Is(err, meta.ErrInvalidCursor):
		writeErr(w, http.StatusBadRequest, "invalid cursor")
		return
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "list repositories: "+err.Error())
		return
	}
	assertf(res.Repos != nil, "store returned nil Repos slice")

	entries := make([]repoEntry, 0, len(res.Repos))
	for _, repo := range res.Repos {
		entries = append(entries, repoEntry{
			RepoID:        repo.RepoID,
			URL:           repo.URL,
			DefaultBranch: repo.DefaultBranch,
			CreatedAt:     repo.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, listReposResponse{
		Repos:      entries,
		NextCursor: res.NextCursor,
		HasMore:    res.HasMore,
	})
}

type deleteRepoResponse struct {
	RepoID  string `json:"repo_id"`
	Message string `json:"message"`
}

// handleDeleteRepo implements DELETE /api/v1/repos/delete (scope
// repo:write): soft delete of the JWT `repo` claim target. 404 when the
// repo never existed, 409 when already soft-deleted (docs/01, docs/03).
func (s *server) handleDeleteRepo(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	repo, err := s.store.SoftDeleteRepo(r.Context(), claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case errors.Is(err, meta.ErrAlreadyDeleted):
		writeErr(w, http.StatusConflict, "repository already deleted")
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "delete repository: "+err.Error())
		return
	}
	assertf(repo.RepoID != "", "store returned deleted repo with empty RepoID")
	writeJSON(w, http.StatusOK, deleteRepoResponse{
		RepoID:  repo.RepoID,
		Message: "repository deleted",
	})
}
