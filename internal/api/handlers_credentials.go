package api

// git-credentials CRUD (docs/03 "Git credentials", phase-5 workstream C):
//
//	POST   /api/v1/repos/git-credentials  repo:write (repo=repoId)  {repo_id, password, username?} -> {id}
//	PUT    /api/v1/repos/git-credentials  repo:write (repo="org")   {id, password, username?}      -> {id, created_at}; 404
//	DELETE /api/v1/repos/git-credentials  repo:write (repo="org")   {id}                           -> 204; 404
//
// INTEGRATION SEAM (frozen-contract constraint): api.New and the server
// struct are owned by the foundation slice, so these handlers live on the
// sibling syncServer type. Wiring is two calls inside api.New once the
// durable stores land:
//
//	ss := newSyncServer(cfg.Store, credStore, upstreamStore, syncQueue, logger)
//	registerCredentialRoutes(mux, cfg.Verifier, ss)
//	registerSyncRoutes(mux, cfg.Verifier, ss)         // handlers_sync.go
//	route("POST /api/v1/repos", false, ss.handleCreateRepo, auth.ScopeRepoWrite) // replaces s.handleCreateRepo
//
// Until then the routes are exercised by credentials_test.go/sync_test.go,
// which build their own mux.

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/meta"
	upsync "codestorage/internal/sync"
)

// syncServer holds the phase-5 credential/upstream-sync handler
// dependencies — a sibling of `server` because that struct is a frozen
// contract for this slice.
type syncServer struct {
	store       meta.Store
	creds       upsync.CredentialStore
	ups         upsync.UpstreamStore
	queue       upsync.Queue
	httpBaseURL string       // no trailing slash; POST /repos response http_url
	logger      *slog.Logger // never nil
}

func newSyncServer(store meta.Store, creds upsync.CredentialStore, ups upsync.UpstreamStore, queue upsync.Queue, httpBaseURL string, logger *slog.Logger) *syncServer {
	assertf(store != nil, "newSyncServer: nil store")
	assertf(creds != nil, "newSyncServer: nil credential store")
	assertf(ups != nil, "newSyncServer: nil upstream store")
	assertf(queue != nil, "newSyncServer: nil queue")
	assertf(httpBaseURL != "", "newSyncServer: empty httpBaseURL")
	assertf(logger != nil, "newSyncServer: nil logger")
	return &syncServer{
		store:       store,
		creds:       creds,
		ups:         ups,
		queue:       queue,
		httpBaseURL: httpBaseURL,
		logger:      logger,
	}
}

// internalErr mirrors (*server).internalErr: log the real error, answer the
// generic envelope (docs/03: clients branch on status codes only).
func (s *syncServer) internalErr(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(err != nil, "internalErr called with nil error")
	assertf(op != "", "internalErr called with empty op")
	s.logger.Error("internal error", "op", op, "method", r.Method, "path", r.URL.Path, "err", err)
	writeErr(w, http.StatusInternalServerError, internalErrorMsg)
}

// registerCredentialRoutes registers the git-credentials CRUD routes.
// POST is repo-scoped (repo=repoId); PUT/DELETE are org-scoped (docs/03).
func registerCredentialRoutes(mux *http.ServeMux, verifier auth.Verifier, s *syncServer, rl *rateLimiter) {
	assertf(mux != nil, "registerCredentialRoutes: nil mux")
	assertf(verifier != nil, "registerCredentialRoutes: nil verifier")
	assertf(s != nil, "registerCredentialRoutes: nil syncServer")
	mux.Handle("POST /api/v1/repos/git-credentials",
		rl.limit(routeRead)(requireAuth(verifier, false, auth.ScopeRepoWrite)(http.HandlerFunc(s.handleCreateCredential))))
	mux.Handle("PUT /api/v1/repos/git-credentials",
		rl.limit(routeRead)(requireAuth(verifier, true, auth.ScopeRepoWrite)(http.HandlerFunc(s.handleUpdateCredential))))
	mux.Handle("DELETE /api/v1/repos/git-credentials",
		rl.limit(routeRead)(requireAuth(verifier, true, auth.ScopeRepoWrite)(http.HandlerFunc(s.handleDeleteCredential))))
}

// gcredDecodeBody reads and unmarshals a bounded JSON body. false = a 400
// envelope was already written.
func gcredDecodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return false
	}
	if len(body) == 0 {
		writeErr(w, http.StatusBadRequest, "request body is required")
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

type gcredCreateRequest struct {
	RepoID   string `json:"repo_id"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// handleCreateCredential implements POST /api/v1/repos/git-credentials:
// store one HTTPS credential for the repo's generic upstream. The JWT repo
// claim must target the same repo as body repo_id (docs/03: repo=repoId).
func (s *syncServer) handleCreateCredential(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	var req gcredCreateRequest
	if !gcredDecodeBody(w, r, &req) {
		return
	}
	if req.RepoID == "" {
		writeErr(w, http.StatusBadRequest, "repo_id is required")
		return
	}
	if req.Password == "" {
		writeErr(w, http.StatusBadRequest, "password is required")
		return
	}
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, req.RepoID)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case err != nil:
		s.internalErr(w, r, "resolve credential repo", err)
		return
	}
	// The claim may carry either the repo url or the repo_id (docs/02);
	// both must resolve to the same repo the body targets.
	if claims.Repo != repo.RepoID && claims.Repo != repo.URL {
		writeErr(w, http.StatusForbidden, "jwt repo claim does not match repo_id")
		return
	}
	cred, err := s.creds.CreateCredential(r.Context(), upsync.Credential{
		RepoID:   repo.RepoID,
		Username: req.Username,
		Password: req.Password,
	})
	switch {
	case errors.Is(err, upsync.ErrCredentialExists):
		writeErr(w, http.StatusConflict, "credential already exists")
		return
	case err != nil:
		s.internalErr(w, r, "create credential", err)
		return
	}
	assertf(cred.ID != "", "credential store returned empty ID")
	writeJSON(w, http.StatusOK, map[string]string{"id": cred.ID})
}

type gcredUpdateRequest struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// gcredForOrg loads the credential and enforces org ownership: an ID owned
// by another org (or pointing at a soft-deleted repo) answers 404, exactly
// like a missing one — no cross-org existence oracle.
func (s *syncServer) gcredForOrg(w http.ResponseWriter, r *http.Request, org, id string) (upsync.Credential, bool) {
	cred, err := s.creds.GetCredential(r.Context(), id)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		writeErr(w, http.StatusNotFound, "credential not found")
		return upsync.Credential{}, false
	case err != nil:
		s.internalErr(w, r, "load credential", err)
		return upsync.Credential{}, false
	}
	_, err = s.store.GetRepoByID(r.Context(), org, cred.RepoID)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "credential not found")
		return upsync.Credential{}, false
	case err != nil:
		s.internalErr(w, r, "resolve credential owner", err)
		return upsync.Credential{}, false
	}
	return cred, true
}

// handleUpdateCredential implements PUT /api/v1/repos/git-credentials
// (org-scoped): replace username/password -> {id, created_at}; 404.
func (s *syncServer) handleUpdateCredential(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	var req gcredUpdateRequest
	if !gcredDecodeBody(w, r, &req) {
		return
	}
	if req.ID == "" {
		writeErr(w, http.StatusBadRequest, "id is required")
		return
	}
	if req.Password == "" {
		writeErr(w, http.StatusBadRequest, "password is required")
		return
	}
	if _, ok := s.gcredForOrg(w, r, claims.Iss, req.ID); !ok {
		return
	}
	cred, err := s.creds.UpdateCredential(r.Context(), req.ID, req.Username, req.Password)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		// Deleted between the ownership check and the update.
		writeErr(w, http.StatusNotFound, "credential not found")
		return
	case err != nil:
		s.internalErr(w, r, "update credential", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"id":         cred.ID,
		"created_at": cred.CreatedAt.UTC().Format(time.RFC3339),
	})
}

type gcredDeleteRequest struct {
	ID string `json:"id"`
}

// handleDeleteCredential implements DELETE /api/v1/repos/git-credentials
// (org-scoped): {id} -> 204; 404.
func (s *syncServer) handleDeleteCredential(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	var req gcredDeleteRequest
	if !gcredDecodeBody(w, r, &req) {
		return
	}
	if req.ID == "" {
		writeErr(w, http.StatusBadRequest, "id is required")
		return
	}
	if _, ok := s.gcredForOrg(w, r, claims.Iss, req.ID); !ok {
		return
	}
	err := s.creds.DeleteCredential(r.Context(), req.ID)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		writeErr(w, http.StatusNotFound, "credential not found")
		return
	case err != nil:
		s.internalErr(w, r, "delete credential", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
