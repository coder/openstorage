package api

// Upstream-sync HTTP surface (docs/03 "Upstream sync", phase-5 workstream D):
//
//	POST   /api/v1/repos                repo:write  base_repo {operation: "sync"} -> mirror link + first sync
//	POST   /api/v1/repos/pull-upstream  git:write   {ref?, after?} -> 202 {"message": "Repository sync initiated successfully"}
//	DELETE /api/v1/repos/base           repo:write  idempotent detach -> 200 {"message": "repository detached"}
//
// api.New calls registerSyncRoutes and wires (*syncServer).handleCreateRepo
// — which extends the frozen (*server).handleCreateRepo flow with base_repo
// support — as the POST /repos fallback of the fork dispatcher
// (handlers_fork.go, workstream E).
//
// base_repo {operation: "fork"} for code/code.storage is dispatched to the
// fork handler before reaching this creator; the 400 below covers fork
// requests for providers the dispatcher does not own.

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"

	"codestorage/internal/auth"
	"codestorage/internal/meta"
	upsync "codestorage/internal/sync"
)

// registerSyncRoutes registers pull-upstream + upstream detach. Scopes per
// docs/02: pull-upstream is a ref mutation (git:write); detach is repo
// lifecycle (repo:write). Both are repo-scoped.
func registerSyncRoutes(mux *http.ServeMux, verifier auth.Verifier, s *syncServer, rl *rateLimiter) {
	assertf(mux != nil, "registerSyncRoutes: nil mux")
	assertf(verifier != nil, "registerSyncRoutes: nil verifier")
	assertf(s != nil, "registerSyncRoutes: nil syncServer")
	mux.Handle("POST /api/v1/repos/pull-upstream",
		rl.limit(routeRead)(requireAuth(verifier, false, auth.ScopeGitWrite)(http.HandlerFunc(s.handlePullUpstream))))
	mux.Handle("DELETE /api/v1/repos/base",
		rl.limit(routeRead)(requireAuth(verifier, false, auth.ScopeRepoWrite)(http.HandlerFunc(s.handleDetachBase))))
}

// usyncBaseRepo is the POST /repos base_repo object (docs/03).
type usyncBaseRepo struct {
	Provider      string             `json:"provider"`
	Owner         string             `json:"owner"`
	Name          string             `json:"name"`
	Operation     string             `json:"operation"`
	Ref           string             `json:"ref"`
	SHA           string             `json:"sha"`
	DefaultBranch string             `json:"default_branch"`
	UpstreamHost  string             `json:"upstream_host"`
	Auth          *usyncBaseRepoAuth `json:"auth"`
}

type usyncBaseRepoAuth struct {
	Token    string `json:"token"`
	AuthType string `json:"auth_type"`
}

// validateSyncBaseRepo checks a base_repo object for the sync operation and
// resolves the effective upstream host. errMsg != "" means 400 with that
// envelope message.
func validateSyncBaseRepo(spec *usyncBaseRepo) (host, errMsg string) {
	assertf(spec != nil, "validateSyncBaseRepo: nil spec")
	switch spec.Operation {
	case "", "sync":
		// mirror sync — handled here
	case "fork":
		// Workstream E (separate slice) owns forking.
		return "", `base_repo operation "fork" not supported`
	default:
		return "", "invalid base_repo operation " + strconv.Quote(spec.Operation)
	}
	if spec.Provider == "code" || spec.Provider == "code.storage" {
		return "", "base_repo provider " + strconv.Quote(spec.Provider) + ` requires operation "fork"`
	}
	defaultHost, known := upsync.ProviderHost(spec.Provider)
	if !known {
		return "", "unsupported base_repo provider " + strconv.Quote(spec.Provider)
	}
	if spec.Owner == "" || spec.Name == "" {
		return "", "base_repo owner and name are required"
	}
	host = spec.UpstreamHost
	if host != "" && !validUpstreamHost(host) {
		return "", "invalid base_repo upstream_host " + strconv.Quote(spec.UpstreamHost)
	}
	if host == "" {
		host = defaultHost
	}
	if host == "" {
		return "", "base_repo upstream_host is required for provider " + strconv.Quote(spec.Provider)
	}
	return host, ""
}

// validUpstreamHost accepts the two documented upstream_host shapes — a
// bare hostname[:port] or an http(s):// base URL — and nothing else.
// upsync.CloneURL uses the value verbatim as the base of a git fetch URL,
// so file://, ssh://, transport helpers (ext::), embedded credentials, and
// query/fragment noise must be rejected here, where the attacker-controlled
// value enters (the worker additionally pins GIT_ALLOW_PROTOCOL).
func validUpstreamHost(host string) bool {
	assertf(host != "", "validUpstreamHost: empty host")
	if !strings.Contains(host, "://") {
		// Bare host[:port]: must round-trip as exactly the authority
		// component (no path, userinfo, or control characters).
		u, err := neturl.Parse("https://" + host)
		return err == nil && u.Host == host && u.User == nil &&
			u.Path == "" && u.RawQuery == "" && u.Fragment == ""
	}
	u, err := neturl.Parse(host)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	// A path is allowed (sub-path-hosted forges); credentials belong in
	// the credential store, never in the URL.
	return u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

// handleCreateRepo is the base_repo-aware POST /api/v1/repos (scope
// repo:write). It mirrors the frozen (*server).handleCreateRepo flow —
// claims, fail-closed ref policies, body/branch validation, store error
// mapping — and additionally accepts base_repo sync links: store an
// optional credential from auth.token, insert the repo_upstreams row, and
// enqueue the first sync (docs/01 invariant 11).
func (s *syncServer) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	url := claims.Repo
	// requireAuth(orgLevel=false) rejects empty/"org" repo claims with 403.
	assertf(url != "" && url != auth.RepoClaimOrg, "create: repo claim %q passed requireAuth", url)

	// docs/02: ref policies apply to every HTTP write endpoint; malformed
	// policy claims fail closed exactly like the frozen create handler.
	if _, err := refPolicies(claims); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}

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

	var spec *usyncBaseRepo
	upstreamHost := ""
	if len(req.BaseRepo) > 0 && string(req.BaseRepo) != "null" {
		spec = &usyncBaseRepo{}
		if err := json.Unmarshal(req.BaseRepo, spec); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid base_repo: "+err.Error())
			return
		}
		var errMsg string
		if upstreamHost, errMsg = validateSyncBaseRepo(spec); errMsg != "" {
			writeErr(w, http.StatusBadRequest, errMsg)
			return
		}
	}

	branch := req.DefaultBranch
	if branch == "" && spec != nil {
		// base_repo.default_branch names the upstream branch to track.
		branch = spec.DefaultBranch
	}
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
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case err != nil:
		s.internalErr(w, r, "create repository", err)
		return
	}
	assertf(repo.RepoID != "", "store returned repo with empty RepoID")

	if spec != nil {
		// Order matters: the credential must exist before the first sync
		// run can start (the queue spawns the worker immediately).
		if spec.Auth != nil && spec.Auth.Token != "" {
			if _, err := s.creds.CreateCredential(r.Context(), upsync.Credential{
				RepoID:   repo.RepoID,
				Password: spec.Auth.Token,
			}); err != nil {
				s.internalErr(w, r, "store base_repo credential", err)
				return
			}
		}
		if _, err := s.ups.PutUpstream(r.Context(), upsync.Upstream{
			RepoID:       repo.RepoID,
			Provider:     spec.Provider,
			Owner:        spec.Owner,
			Name:         spec.Name,
			UpstreamHost: upstreamHost,
		}); err != nil {
			s.internalErr(w, r, "store base_repo upstream", err)
			return
		}
		if err := s.queue.EnqueueSync(r.Context(), upsync.Job{
			Org:    claims.Iss,
			RepoID: repo.RepoID,
			Ref:    spec.Ref,
		}); err != nil {
			s.internalErr(w, r, "enqueue first sync", err)
			return
		}
	}

	writeJSON(w, http.StatusOK, createRepoResponse{
		RepoID:  repo.RepoID,
		HTTPURL: s.httpBaseURL + "/" + repo.RepoID + ".git",
		Message: "repository created",
	})
}

type usyncPullUpstreamRequest struct {
	Ref string `json:"ref"`
	// After is a reserved opaque token for future incremental syncs
	// (openapi); accepted and ignored.
	After string `json:"after"`
}

// pullUpstreamAcceptedMsg is the exact documented 202 body message.
const pullUpstreamAcceptedMsg = "Repository sync initiated successfully"

// policyDeniesPullUpstream reports whether any policy rule carries an op
// that denies triggering a sync. The worker mirror-pushes refs/heads/* and
// refs/tags/* through a loopback token that carries NO ref policies, with
// upstream state winning (force refspecs) and commits this server cannot
// signature-verify — so a policed caller must not launder ref updates
// through it. Every op fails closed: no-push denies all updates,
// no-force-push denies the potential non-fast-forwards, verify-sig cannot
// be satisfied, and unknown ops fail closed (same coarseness as
// policyDeniesRepoDelete: the pattern is not consulted because the sync
// may touch any synced ref).
func policyDeniesPullUpstream(policies []auth.RefPolicy) (string, bool) {
	for _, rule := range policies {
		if len(rule.Ops) > 0 {
			return rule.Ops[0], true
		}
	}
	return "", false
}

// handlePullUpstream implements POST /api/v1/repos/pull-upstream (scope
// git:write): enqueue a sync for the JWT repo claim target. 400 when the
// repo has no upstream base_repo (docs/03: "only for repos with base_repo").
func (s *syncServer) handlePullUpstream(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	// docs/02: ref policies apply to every HTTP write endpoint. The
	// loopback push runs with an unpoliced internal token, so the caller's
	// policies are enforced here, before the sync is enqueued; malformed
	// policy claims fail closed.
	policies, err := refPolicies(claims)
	if err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	if op, denied := policyDeniesPullUpstream(policies); denied {
		writeErr(w, http.StatusForbidden, "ref policy op "+strconv.Quote(op)+" denies pull-upstream")
		return
	}
	var req usyncPullUpstreamRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
	}
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case err != nil:
		s.internalErr(w, r, "resolve pull-upstream repo", err)
		return
	}
	_, err = s.ups.GetUpstream(r.Context(), repo.RepoID)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		writeErr(w, http.StatusBadRequest, "repository has no upstream base_repo")
		return
	case err != nil:
		s.internalErr(w, r, "load upstream", err)
		return
	}
	if err := s.queue.EnqueueSync(r.Context(), upsync.Job{
		Org:    claims.Iss,
		RepoID: repo.RepoID,
		Ref:    req.Ref,
	}); err != nil {
		s.internalErr(w, r, "enqueue sync", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"message": pullUpstreamAcceptedMsg})
}

// handleDetachBase implements DELETE /api/v1/repos/base (scope repo:write):
// idempotent upstream detach. In-flight or queued syncs observe the missing
// row and end silently — the documented cancellation semantics (docs/06).
func (s *syncServer) handleDetachBase(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	// Malformed ref-policy claims fail closed on write endpoints (docs/02).
	if _, err := refPolicies(claims); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case err != nil:
		s.internalErr(w, r, "resolve detach repo", err)
		return
	}
	err = s.ups.DeleteUpstream(r.Context(), repo.RepoID)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		// Already detached: idempotent success (openapi).
	case err != nil:
		s.internalErr(w, r, "detach upstream", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "repository detached"})
}
