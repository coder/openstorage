package api

// git-credentials CRUD (docs/reference/http-api.md "Git credentials", phase-5 workstream C):
//
//	POST   /api/v1/repos/git-credentials  repo:write (repo=repoId)  {repo_id?, password, username?} -> {id}
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

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

// syncServer holds the phase-5 credential/upstream-sync handler
// dependencies — a sibling of `server` because that struct is a frozen
// contract for this slice.
type syncServer struct {
	store       meta.Store
	creds       upsync.CredentialStore
	ups         upsync.UpstreamStore
	queue       upsync.Queue
	httpBaseURL string // no trailing slash; POST /repos response http_url
	// guard runs the registration-time egress courtesy check on base_repo
	// upstream_host (issue #139), configured with the effective pull-lane
	// policy; the sync worker's dial-time vetting proxy stays authoritative.
	guard  events.TargetGuard
	logger *slog.Logger // never nil

	// firstSync is the queue's scheduler when it tracks create state
	// (issue #388); nil on S3-WAL (#335), whose sync creates stay legacy.
	firstSync upsync.FirstSyncScheduler
}

func newSyncServer(store meta.Store, creds upsync.CredentialStore, ups upsync.UpstreamStore, queue upsync.Queue, httpBaseURL string, guard events.TargetGuard, logger *slog.Logger) *syncServer {
	assertf(store != nil, "newSyncServer: nil store")
	assertf(creds != nil, "newSyncServer: nil credential store")
	assertf(ups != nil, "newSyncServer: nil upstream store")
	assertf(queue != nil, "newSyncServer: nil queue")
	assertf(httpBaseURL != "", "newSyncServer: empty httpBaseURL")
	assertf(logger != nil, "newSyncServer: nil logger")
	firstSync, _ := queue.(upsync.FirstSyncScheduler)
	if firstSync != nil && !firstSync.TracksCreateState() {
		firstSync = nil
	}
	return &syncServer{
		firstSync:   firstSync,
		store:       store,
		creds:       creds,
		ups:         ups,
		queue:       queue,
		httpBaseURL: httpBaseURL,
		guard:       guard,
		logger:      logger,
	}
}

// internalErr mirrors (*server).internalErr: log the real error, answer the
// generic envelope (docs/reference/http-api.md: clients branch on status codes only).
func (s *syncServer) internalErr(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(err != nil, "internalErr called with nil error")
	assertf(op != "", "internalErr called with empty op")
	s.logger.Error("internal error", "op", op, "method", r.Method, "path", r.URL.Path, "err", err)
	writeErr(w, http.StatusInternalServerError, internalErrorMsg)
}

// registerCredentialRoutes registers the git-credentials CRUD routes.
// POST is repo-scoped (repo=repoId); PUT/DELETE are org-scoped (docs/reference/http-api.md).
func registerCredentialRoutes(mux *http.ServeMux, verifier auth.Verifier, org string, s *syncServer, rl *rateLimiter) {
	assertf(mux != nil, "registerCredentialRoutes: nil mux")
	assertf(verifier != nil, "registerCredentialRoutes: nil verifier")
	assertf(s != nil, "registerCredentialRoutes: nil syncServer")
	mux.Handle("POST /api/v1/repos/git-credentials",
		rl.limit(routeRead)(requireAuth(verifier, org, false, auth.ScopeRepoWrite)(http.HandlerFunc(s.handleCreateCredential))))
	mux.Handle("PUT /api/v1/repos/git-credentials",
		rl.limit(routeRead)(requireAuth(verifier, org, true, auth.ScopeRepoWrite)(http.HandlerFunc(s.handleUpdateCredential))))
	mux.Handle("DELETE /api/v1/repos/git-credentials",
		rl.limit(routeRead)(requireAuth(verifier, org, true, auth.ScopeRepoWrite)(http.HandlerFunc(s.handleDeleteCredential))))
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
// claim must target the same repo as body repo_id (docs/reference/http-api.md: repo=repoId).
// The alias POST /api/repos/{repo_name}/git-credentials (openapi.yaml)
// re-dispatches here with a body that carries NO repo_id, so an absent
// repo_id falls back to the token's repo claim — requireAuth(orgLevel=false)
// guarantees it is non-empty and non-"org", and aliasHandler has already
// matched it against the path name. Success is 201 {id} (openapi.yaml;
// docs/archive/phases/14-phase5-implementation-notes.md deferred #4).
func (s *syncServer) handleCreateCredential(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	var req gcredCreateRequest
	if !gcredDecodeBody(w, r, &req) {
		return
	}
	if req.Password == "" {
		writeErr(w, http.StatusBadRequest, "password is required")
		return
	}
	repoKey := req.RepoID
	if repoKey == "" {
		repoKey = claims.Repo
	}
	assertf(repoKey != "" && repoKey != auth.RepoClaimOrg, "handleCreateCredential: unusable repo key %q", repoKey)
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, repoKey)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case err != nil:
		s.internalErr(w, r, "resolve credential repo", err)
		return
	}
	// The claim may carry either the repo url or the repo_id (docs/reference/authentication.md);
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
	case errors.Is(err, meta.ErrReservedSecret):
		writeErr(w, http.StatusBadRequest, "invalid password: "+meta.ErrReservedSecret.Error())
		return
	case err != nil:
		s.internalErr(w, r, "create credential", err)
		return
	}
	assertf(cred.ID != "", "credential store returned empty ID")
	writeJSON(w, http.StatusCreated, map[string]string{"id": cred.ID})
}

type gcredUpdateRequest struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// gcredForOrg loads the credential's password-free metadata and enforces
// org ownership: an ID owned by another org (or pointing at a soft-deleted
// repo) answers 404, exactly like a missing one — no cross-org existence
// oracle. The returned Credential carries an empty Password on purpose:
// update/delete prechecks must not decrypt the stored value, or a row under
// a prematurely retired key could never be replaced or removed (issue #54).
func (s *syncServer) gcredForOrg(w http.ResponseWriter, r *http.Request, org, id string) (upsync.Credential, bool) {
	cred, err := s.creds.CredentialMeta(r.Context(), id)
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
	case errors.Is(err, meta.ErrReservedSecret):
		writeErr(w, http.StatusBadRequest, "invalid password: "+meta.ErrReservedSecret.Error())
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

// Path-id git-credentials routes (openapi.yaml, docs/archive/phases/15-phase6-implementation-notes.md deferred #1):
//
//	PUT    /api/repos/{repo_name}/git-credentials/{git_credential_id}  repo:write (per repo)  {username?, password} -> {id, created_at}
//	DELETE /api/repos/{repo_name}/git-credentials/{git_credential_id}  repo:write (per repo)  -> 204
//
// Registered by registerAliasRoutes (alias.go) as direct routes — there is
// no v1 twin to re-dispatch to. Auth is requireAuthWrite (per-repo
// repo:write, ADR-26 result envelopes), so every failure here carries the
// write flavor; the success bodies match the v1 PUT/DELETE exactly.

// internalErrResult mirrors internalErr with the docs/explanation/streaming-commit-protocols.md result envelope
// (the path-id credential routes are write-flavored, ADR-26).
func (s *syncServer) internalErrResult(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(s != nil && s.logger != nil, "internalErrResult: nil syncServer deps")
	assertf(err != nil && op != "", "internalErrResult: nil error or empty op")
	s.logger.Error("internal error", "op", op, "method", r.Method, "path", r.URL.Path, "err", err)
	aliasFail(w, routeWrite, http.StatusInternalServerError, internalErrorMsg)
}

// gcredByPath resolves the {repo_name} and {git_credential_id} path values:
// the name must resolve in the token's org AND match the token's repo claim
// (aliasHandler's 404-on-mismatch rule, so foreign tokens cannot probe name
// existence), and the credential must exist and be attached to that repo
// (404 otherwise — no cross-repo existence oracle). false = a failure
// envelope was already written.
func (s *syncServer) gcredByPath(w http.ResponseWriter, r *http.Request) (upsync.Credential, bool) {
	assertf(s != nil && s.store != nil && s.creds != nil, "gcredByPath: nil syncServer deps")
	name := r.PathValue("repo_name")
	credID := r.PathValue("git_credential_id")
	// Path wildcards never match an empty segment.
	assertf(name != "" && credID != "", "gcredByPath: empty path values")
	claims := mustClaims(r)

	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, name)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		aliasFail(w, routeWrite, http.StatusNotFound, "repository not found")
		return upsync.Credential{}, false
	case err != nil:
		s.internalErrResult(w, r, "resolve credential repo name", err)
		return upsync.Credential{}, false
	}

	// requireAuthWrite(orgLevel=false) guarantees a non-empty, non-"org"
	// repo claim; it may carry the repo url or the repo_id (docs/reference/authentication.md).
	claimRepo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		aliasFail(w, routeWrite, http.StatusNotFound, "repository not found")
		return upsync.Credential{}, false
	case err != nil:
		s.internalErrResult(w, r, "resolve credential repo claim", err)
		return upsync.Credential{}, false
	}
	if claimRepo.RepoID != repo.RepoID {
		aliasFail(w, routeWrite, http.StatusNotFound, "repository not found")
		return upsync.Credential{}, false
	}

	// Password-free metadata on purpose, mirroring gcredForOrg: the alias
	// PUT/DELETE recovery paths must work on rows whose envelope can no
	// longer be decrypted (issue #54).
	cred, err := s.creds.CredentialMeta(r.Context(), credID)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		aliasFail(w, routeWrite, http.StatusNotFound, "credential not found")
		return upsync.Credential{}, false
	case err != nil:
		s.internalErrResult(w, r, "load credential", err)
		return upsync.Credential{}, false
	}
	if cred.RepoID != repo.RepoID {
		// Another repo's credential answers exactly like a missing one.
		aliasFail(w, routeWrite, http.StatusNotFound, "credential not found")
		return upsync.Credential{}, false
	}
	return cred, true
}

type gcredPathUpdateRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// handleUpdateCredentialByPath implements
// PUT /api/repos/{repo_name}/git-credentials/{git_credential_id}: rotate
// username/password -> 200 {id, created_at}, the exact v1 PUT body shape.
// Only `password` is required (openapi.yaml); an omitted username clears
// the stored one, as on the v1 PUT.
func (s *syncServer) handleUpdateCredentialByPath(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.gcredByPath(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		aliasFail(w, routeWrite, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	if len(body) == 0 {
		aliasFail(w, routeWrite, http.StatusBadRequest, "request body is required")
		return
	}
	var req gcredPathUpdateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		aliasFail(w, routeWrite, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Password == "" {
		aliasFail(w, routeWrite, http.StatusBadRequest, "password is required")
		return
	}
	updated, err := s.creds.UpdateCredential(r.Context(), cred.ID, req.Username, req.Password)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		// Deleted between the path resolution and the update.
		aliasFail(w, routeWrite, http.StatusNotFound, "credential not found")
		return
	case errors.Is(err, meta.ErrReservedSecret):
		aliasFail(w, routeWrite, http.StatusBadRequest, "invalid password: "+meta.ErrReservedSecret.Error())
		return
	case err != nil:
		s.internalErrResult(w, r, "update credential", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"id":         updated.ID,
		"created_at": updated.CreatedAt.UTC().Format(time.RFC3339),
	})
}

// handleDeleteCredentialByPath implements
// DELETE /api/repos/{repo_name}/git-credentials/{git_credential_id}: no
// body -> 204 empty, exactly like the v1 DELETE.
func (s *syncServer) handleDeleteCredentialByPath(w http.ResponseWriter, r *http.Request) {
	cred, ok := s.gcredByPath(w, r)
	if !ok {
		return
	}
	err := s.creds.DeleteCredential(r.Context(), cred.ID)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		// Deleted between the path resolution and this delete.
		aliasFail(w, routeWrite, http.StatusNotFound, "credential not found")
		return
	case err != nil:
		s.internalErrResult(w, r, "delete credential", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
