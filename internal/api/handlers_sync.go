package api

// Upstream-sync HTTP surface (docs/reference/http-api.md "Upstream sync", phase-5 workstream D):
//
//	POST   /api/v1/repos                repo:write  upstream {provider, owner, name, access?, host?}
//	                                                or base_repo {operation: "sync"} -> mirror link + first sync
//	POST   /api/v1/repos/pull-upstream  git:write   {ref?, after?} -> 202 {"message": "Repository sync initiated successfully"}
//	DELETE /api/v1/repos/base           repo:write  idempotent detach -> 200 {"message": "repository detached"}
//	                                                (DELETE .../upstream without an upstream: "repository already detached")
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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

// registerSyncRoutes registers pull-upstream + upstream detach. Scopes per
// docs/reference/authentication.md: pull-upstream is a ref mutation (git:write); detach is repo
// lifecycle (repo:write). Both are repo-scoped.
func registerSyncRoutes(mux *http.ServeMux, verifier auth.Verifier, org string, s *syncServer, rl *rateLimiter) {
	assertf(mux != nil, "registerSyncRoutes: nil mux")
	assertf(verifier != nil, "registerSyncRoutes: nil verifier")
	assertf(s != nil, "registerSyncRoutes: nil syncServer")
	mux.Handle("POST /api/v1/repos/pull-upstream",
		deprecated(rl.limit()(requireAuth(verifier, org, false, auth.ScopeGitWrite)(http.HandlerFunc(s.handlePullUpstream)))))
	// push-upstream requires git:read ON TOP of git:write (Codex security
	// round on #90): the forward exports every local head and tag to the
	// upstream, so a write-only token could otherwise exfiltrate
	// repository content it cannot clone — the worker self-mints its
	// loopback read token, so the caller's own read authority is the only
	// gate. requireAuth demands ALL listed scopes.
	mux.Handle("POST /api/v1/repos/push-upstream",
		rl.limit()(requireAuth(verifier, org, false, auth.ScopeGitWrite, auth.ScopeGitRead)(http.HandlerFunc(s.handlePushUpstream))))
	mux.Handle("DELETE /api/v1/repos/base",
		deprecated(rl.limit()(requireAuth(verifier, org, false, auth.ScopeRepoWrite)(http.HandlerFunc(s.handleDetachBase)))))
}

// deprecationHeader is the RFC 9745 Deprecation value (a structured-field
// date: "@" + Unix seconds) of the deprecated pull-upstream and base routes:
// 2026-10-07T00:00:00Z, the date of upstream's "Upstream Paths" changelog
// entry. Upstream documents the header but not its value (issue #456).
const deprecationHeader = "@1791331200"

// deprecated sets the Deprecation header on every response of a deprecated
// route. It wraps the route's whole chain (limiter, auth, handler), so 429s,
// 401/403s and handler errors carry it too. A request that the alias layer
// re-dispatched passes through unmarked: the alias route decides, so the
// replacements (.../upstream/pull, DELETE .../upstream), which share these
// v1 handlers, carry no header.
func deprecated(next http.Handler) http.Handler {
	assertf(next != nil, "deprecated: nil handler")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isAliasDispatch(r.Context()) {
			w.Header().Set("Deprecation", deprecationHeader)
		}
		next.ServeHTTP(w, r)
	})
}

// usyncBaseRepo is the POST /repos base_repo object (docs/reference/http-api.md).
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
	// ForwardPushes opts the repo into forwarding won local pushes to the
	// upstream (issue #90; our extension — upstream forwards implicitly in
	// GitHub App mode). Default off: forwarding stays explicit-trigger via
	// POST /repos/push-upstream.
	ForwardPushes bool `json:"forward_pushes"`
}

type usyncBaseRepoAuth struct {
	Token    string `json:"token"`
	AuthType string `json:"auth_type"`
}

// validateSyncBaseRepo checks a base_repo object for the sync operation,
// resolves the effective upstream host, and adds a problem to errs for every
// invalid base_repo field, in the order of the docs. base_repo.operation decides
// which of the other fields apply: when it is not sync, it is the only
// problem. A field that another field's check needs (the provider for a
// default host) is not checked when that field has a problem. branchErr is
// the problem of base_repo.default_branch, or nil; it stands where the
// field does. The host is meaningful when errs holds no problem of the host.
func validateSyncBaseRepo(spec *usyncBaseRepo, branchErr error, errs *bodyErrors) (host string) {
	assertf(spec != nil, "validateSyncBaseRepo: nil spec")
	switch spec.Operation {
	case "", "sync":
		// mirror sync — handled here
	case "fork":
		// Workstream E (separate slice) owns forking.
		errs.add(newFieldError("base_repo.operation", "is not supported", `base_repo operation "fork" not supported`))
		return ""
	default:
		errs.add(newFieldError("base_repo.operation", "must be sync or fork", "invalid base_repo operation "+strconv.Quote(spec.Operation)))
		return ""
	}
	if spec.Provider == "code" || spec.Provider == "code.storage" {
		errs.add(newFieldError("base_repo.operation", "must be fork for this provider",
			"base_repo provider "+strconv.Quote(spec.Provider)+` requires operation "fork"`))
		return ""
	}
	defaultHost, known := upsync.ProviderHost(spec.Provider)
	if !known {
		errs.add(newFieldError("base_repo.provider", "is not supported", "unsupported base_repo provider "+strconv.Quote(spec.Provider)))
	}
	// Owner, name and token are stored, and PostgreSQL refuses a NUL in a
	// text value (the token reaches it encrypted only when a secrets key is
	// set). Every backend refuses such a value alike.
	addFields(errs, "base_repo owner and name are required",
		[][2]string{{"base_repo.owner", spec.Owner}, {"base_repo.name", spec.Name}}, textField)
	token := ""
	if spec.Auth != nil {
		token = spec.Auth.Token
	}
	errs.add(invalidTextFields([2]string{"base_repo.auth.token", token}))
	errs.add(branchErr)
	host = spec.UpstreamHost
	switch {
	case host != "" && !validUpstreamHost(host):
		errs.add(newFieldError("base_repo.upstream_host", "is not a valid host", "invalid base_repo upstream_host "+strconv.Quote(spec.UpstreamHost)))
	case host == "" && known:
		if host = defaultHost; host == "" {
			errs.add(newFieldError("base_repo.upstream_host", "is required for this provider",
				"base_repo upstream_host is required for provider "+strconv.Quote(spec.Provider)))
		}
	}
	return host
}

// addFields adds the problem of each of fields (dotted wire path -> value),
// in order. The empty fields are named by one error that holds msg, as
// requiredFields does. When another field with a problem of its own lies
// between two empty ones, that error also holds this problem, so that its
// items keep the order of the fields. Each other field gets the problem that
// check returns for it, or none.
func addFields(errs *bodyErrors, msg string, fields [][2]string, check func(name, value string) error) {
	first, last := -1, -1
	for i, f := range fields {
		if f[1] == "" {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		first, last = len(fields), len(fields)-1
	}
	for _, f := range fields[:first] {
		errs.add(check(f[0], f[1]))
	}
	if first <= last {
		var (
			params []invalidParam
			msgs   = []string{msg}
		)
		for _, f := range fields[first : last+1] {
			if f[1] == "" {
				params = append(params, invalidParam{Name: f[0], Reason: "is required"})
			} else if problem := check(f[0], f[1]); problem != nil {
				params = append(params, invalidParamsFor(problem)...)
				msgs = append(msgs, problem.Error())
			}
		}
		errs.add(newFieldErrors(strings.Join(msgs, "; "), params...))
	}
	for _, f := range fields[last+1:] {
		errs.add(check(f[0], f[1]))
	}
}

// textField is the check of a field that holds stored text.
func textField(name, value string) error {
	return invalidTextFields([2]string{name, value})
}

// upstreamConfig is upstream's UpstreamConfig (POST /repos `upstream`,
// issue #359): the Git Sync form that replaces base_repo's deprecated sync
// fields.
type upstreamConfig struct {
	Provider string `json:"provider"`
	Owner    string `json:"owner"`
	Name     string `json:"name"`
	Access   string `json:"access"`
	Host     string `json:"host"`
}

// upstreamAppAccessMsg rejects GitHub App access (upstream's default for
// provider github): open.storage has no GitHub App integration, and
// silently syncing without it would change what the link reads.
const upstreamAppAccessMsg = `upstream access "app" (the default for github) is not supported: open.storage has no GitHub App integration; set access "public"`

// parseUpstreamConfig decodes an UpstreamConfig strictly. A non-nil err
// means 400 with its message, naming the invalid upstream field where it
// can.
func parseUpstreamConfig(raw json.RawMessage) (cfg upstreamConfig, err error) {
	assertf(jsonPresent(raw), "parseUpstreamConfig: absent upstream")
	dec := json.NewDecoder(bytes.NewReader(raw))
	// additionalProperties: false upstream; an ignored field (for example
	// a branch) would silently change what the link syncs.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return upstreamConfig{}, fieldErrorUnder("upstream", "invalid upstream: "+err.Error(), err)
	}
	return cfg, nil
}

// validate adds a problem to errs for every invalid field of cfg, in the
// order of the docs, and resolves the upstream host: the host is meaningful
// when errs holds no problem of the host. The provider decides what the
// access and host fields mean, so a problem with the provider leaves them
// unchecked.
func (cfg upstreamConfig) validate(errs *bodyErrors) (host string) {
	// ProviderHost knows exactly upstream's UpstreamConfig provider enum;
	// code/code.storage are not in it (a copy uses base_repo).
	defaultHost, known := upsync.ProviderHost(cfg.Provider)
	// Owner and name are stored (PostgreSQL refuses a NUL byte).
	addFields(errs, "upstream provider, owner, and name are required",
		[][2]string{{"upstream.provider", cfg.Provider}, {"upstream.owner", cfg.Owner}, {"upstream.name", cfg.Name}},
		func(name, value string) error {
			if name == "upstream.provider" && !known {
				return newFieldError(name, "is not supported", "unsupported upstream provider "+strconv.Quote(cfg.Provider))
			}
			return textField(name, value)
		})
	switch {
	case cfg.Access != "" && cfg.Access != "app" && cfg.Access != "public":
		errs.add(newFieldError("upstream.access", "must be app or public", "invalid upstream access "+strconv.Quote(cfg.Access)))
	case known && cfg.Provider != "github" && cfg.Access != "":
		errs.add(newFieldError("upstream.access", "applies to provider github only", "upstream access applies to provider github only"))
	case known && cfg.Provider == "github" && cfg.Access != "public":
		errs.add(newFieldError("upstream.access", "must be public", upstreamAppAccessMsg))
	}
	host = cfg.Host
	switch {
	case host != "" && !validUpstreamHost(host):
		errs.add(newFieldError("upstream.host", "is not a valid host", "invalid upstream host "+strconv.Quote(cfg.Host)))
	case host == "" && known:
		if host = defaultHost; host == "" {
			errs.add(newFieldError("upstream.host", "is required for this provider", "upstream host is required for provider "+strconv.Quote(cfg.Provider)))
		}
	}
	return host
}

// validUpstreamHost accepts the two documented upstream_host shapes — a
// bare hostname[:port] or an http(s):// base URL — and nothing else. Both
// need a hostname: upstreamHostname asserts one, so a host such as ":80"
// must be refused here and not reach it.
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
		return err == nil && u.Host == host && u.Hostname() != "" && u.User == nil &&
			u.Path == "" && u.RawQuery == "" && u.Fragment == ""
	}
	u, err := neturl.Parse(host)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	// A path is allowed (sub-path-hosted forges); credentials belong in
	// the credential store, never in the URL.
	return u.Hostname() != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

// upstreamHostname extracts the hostname the sync fetch will dial from a
// validUpstreamHost-validated value (bare host[:port] or http(s):// base
// URL) for the registration-time egress courtesy check (issue #139).
func upstreamHostname(host string) string {
	assertf(host != "", "upstreamHostname: empty host")
	raw := host
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := neturl.Parse(raw)
	assertf(err == nil && u.Hostname() != "", "upstreamHostname: unvalidated host %q", host)
	return u.Hostname()
}

// handleCreateRepo is the base_repo-aware POST /api/v1/repos (scope
// repo:write). It mirrors the frozen (*server).handleCreateRepo flow —
// claims, fail-closed ref policies, body/branch validation, store error
// mapping — and additionally accepts base_repo sync links: store an
// optional credential from auth.token, insert the repo_upstreams row, and
// enqueue the first sync (docs/explanation/domain-model.md invariant 11).
func (s *syncServer) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	assertf(s != nil && s.store != nil && s.creds != nil && s.ups != nil && s.queue != nil,
		"handleCreateRepo: nil syncServer deps")
	claims := mustClaims(r)
	url := claims.Repo
	// requireAuth(orgLevel=false) rejects empty/"org" repo claims with 403.
	assertf(url != "" && url != auth.RepoClaimOrg, "create: repo claim %q passed requireAuth", url)

	// docs/reference/authentication.md: ref policies apply to every HTTP write endpoint; malformed
	// policy claims fail closed exactly like the frozen create handler.
	policies, err := refPolicies(claims)
	if err != nil {
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
		if err := unmarshalBody(body, &req); err != nil {
			writeInvalid(w, "invalid JSON body: "+err.Error(), err)
			return
		}
	}
	if !req.repoNameMatches(url) {
		writeProblem(w, http.StatusBadRequest, codeRepoNameMismatch, repoNameMismatchMsg)
		return
	}

	// Upstream documents no error for both forms; setting both is
	// ambiguous, so it is a 400 (issue #359). Neither form is checked
	// then, as it is unknown which one applies.
	both := jsonPresent(req.BaseRepo) && jsonPresent(req.Upstream)
	var (
		spec *usyncBaseRepo
		cfg  upstreamConfig
		// hostField names the host in messages; hostPath is its wire path.
		hostField, hostPath string
	)
	switch {
	case both:
	case jsonPresent(req.Upstream):
		if cfg, err = parseUpstreamConfig(req.Upstream); err != nil {
			writeInvalid(w, err.Error(), err)
			return
		}
		spec = &usyncBaseRepo{Provider: cfg.Provider, Owner: cfg.Owner, Name: cfg.Name}
		hostField, hostPath = "upstream host", "upstream.host"
	case jsonPresent(req.BaseRepo):
		spec = &usyncBaseRepo{}
		if err := unmarshalBody(req.BaseRepo, spec); err != nil {
			fe := fieldErrorUnder("base_repo", "invalid base_repo: "+err.Error(), err)
			writeInvalid(w, fe.Error(), fe)
			return
		}
		hostField, hostPath = "base_repo upstream_host", "base_repo.upstream_host"
	}

	// Every problem of the body that needs no state, in the order of the
	// request body in the docs, so that the 400 names every invalid field
	// (bodyErrors). The default branch of the body is the first field.
	topBranch, baseBranch := bodyBranchProblem(&req, spec)
	var (
		errs         bodyErrors
		upstreamHost string
	)
	errs.add(topBranch)
	switch {
	case both:
		errs.add(errors.New("base_repo and upstream cannot both be set"))
	case jsonPresent(req.Upstream):
		upstreamHost = cfg.validate(&errs)
	case jsonPresent(req.BaseRepo):
		upstreamHost = validateSyncBaseRepo(spec, baseBranch, &errs)
	}
	if writeBodyErrors(w, &errs, topBranch, baseBranch) {
		return
	}
	if spec != nil {
		assertf(upstreamHost != "" && hostField != "", "create: validated sync spec without host")
		// Issue #394: the first sync mirror-pushes through the unpoliced
		// loopback token exactly like pull-upstream, so the same refusal
		// applies here, before the repo row, the credential and the first
		// sync exist.
		if op, denied := policyDeniesPullUpstream(policies); denied {
			writeErr(w, http.StatusForbidden, "ref policy op "+strconv.Quote(op)+" denies sync repository create")
			return
		}
	}

	// A bad default_branch of the body that is the only problem answers here:
	// after the ref policy, and before the egress check, which may look up a
	// name and so comes after every check of the body. The query parameter is
	// no body field: it is checked after the egress check, as before.
	if err := cmp.Or(topBranch, baseBranch); err != nil {
		writeInvalid(w, err.Error(), err)
		return
	}
	if spec != nil {
		// Registration-time egress courtesy check (issue #139), mirroring
		// webhook CheckRegistration: fast feedback for upstream hosts the
		// pull lane's dial-time vetting proxy would deny anyway, before any
		// rows exist. Fail-open on resolution errors — acceptance is not a
		// reachability promise; the worker's guard is authoritative.
		if err := s.guard.CheckHost(r.Context(), upstreamHostname(upstreamHost)); err != nil {
			writeInvalidField(w, hostPath, "is denied by the sync egress policy", hostField+" denied by sync egress policy: "+err.Error())
			return
		}
	}

	branch, branchPath := req.DefaultBranch, "default_branch"
	if branch == "" && spec != nil {
		// base_repo.default_branch names the upstream branch to track.
		branch, branchPath = spec.DefaultBranch, "base_repo.default_branch"
	}
	if branch == "" {
		// openapi.yaml: optional query override, used only when the body
		// does not already specify a default branch.
		branch, branchPath = r.URL.Query().Get("default_branch"), ""
	}
	// Upstream: an omitted default branch on a sync from GitHub takes the
	// GitHub repository's default. The create makes no network call, so
	// the repo starts on "main" and a sync adopts the upstream HEAD later
	// (issue #599, recorded in the upstream row).
	omitted := branch == "" && spec != nil && spec.Provider == "github"
	if branch == "" {
		branch = meta.OmittedDefaultBranch
	}
	// The body's default branch is valid here (checked above), so only the
	// query parameter can answer.
	if err := defaultBranchProblem(branch, branchPath); err != nil {
		writeInvalid(w, err.Error(), err)
		return
	}

	var repo meta.Repo
	var link meta.RepoUpstream
	tracked := spec != nil && s.firstSync != nil
	switch {
	case spec == nil:
		// A plain create finishes at insert (issue #388).
		repo, err = s.store.CreateRepoWithState(r.Context(), claims.Iss, url, branch, meta.CreateStateComplete)
	case tracked:
		// Repo, upstream and credential in one write, pending until the
		// first sync is scheduled below (issue #388).
		var cred *meta.GitCredential
		if spec.Auth != nil && spec.Auth.Token != "" {
			cred = &meta.GitCredential{Password: spec.Auth.Token}
		}
		link = meta.RepoUpstream{Provider: spec.Provider, Owner: spec.Owner, Name: spec.Name,
			UpstreamHost: upstreamHost, ForwardOnPush: spec.ForwardPushes, DefaultBranchOmitted: omitted}
		repo, err = s.linked().CreateLinkedRepo(r.Context(), claims.Iss, url, branch, link, cred)
	default:
		// The queue tracks no create state: a legacy row.
		repo, err = s.store.CreateRepo(r.Context(), claims.Iss, url, branch)
	}
	switch {
	case errors.Is(err, meta.ErrAlreadyExists) && tracked:
		s.answerTakenSyncName(w, r, claims, url, link)
		return
	case errors.Is(err, meta.ErrAlreadyExists):
		answerTakenName(w, r, s.store, s.internalErr, s.httpBaseURL, claims.Iss, url, spec != nil)
		return
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case err != nil:
		s.internalErr(w, r, "create repository", err)
		return
	}
	assertf(repo.RepoID != "", "store returned repo with empty RepoID")

	if tracked && !s.completeOwnSyncCreate(w, r, claims.Iss, url, repo.RepoID) {
		return
	}
	if spec != nil && !tracked {
		// Order matters: the credential must exist before the first sync
		// run can start (the queue spawns the worker immediately). Each
		// post-create failure abandons the fresh repo row (docs/archive/phases/14-phase5-implementation-notes.md
		// deferred #5): without the soft-delete a half-linked live repo
		// would remain behind a 500.
		if spec.Auth != nil && spec.Auth.Token != "" {
			if _, err := s.creds.CreateCredential(r.Context(), upsync.Credential{
				RepoID:   repo.RepoID,
				Password: spec.Auth.Token,
			}); err != nil {
				s.createAbandon(r.Context(), claims.Iss, url, repo.RepoID)
				s.internalErr(w, r, "store base_repo credential", err)
				return
			}
		}
		if _, err := s.ups.PutUpstream(r.Context(), upsync.Upstream{
			RepoID:        repo.RepoID,
			Provider:      spec.Provider,
			Owner:         spec.Owner,
			Name:          spec.Name,
			UpstreamHost:  upstreamHost,
			ForwardOnPush: spec.ForwardPushes,
		}); err != nil {
			s.createAbandon(r.Context(), claims.Iss, url, repo.RepoID)
			s.internalErr(w, r, "store base_repo upstream", err)
			return
		}
		if err := s.queue.EnqueueSync(r.Context(), upsync.Job{
			Org:    claims.Iss,
			RepoID: repo.RepoID,
			Ref:    spec.Ref,
		}); err != nil {
			s.createAbandon(r.Context(), claims.Iss, url, repo.RepoID)
			s.internalErr(w, r, "enqueue first sync", err)
			return
		}
	}

	writeJSON(w, http.StatusCreated, createRepoResponse{
		RepoID:   repo.RepoID,
		RepoName: repo.URL,
		HTTPURL:  s.httpBaseURL + "/" + repo.RepoID + ".git",
		Message:  repoCreatedMsg(repo.URL),
	})
}

// linked is the store's atomic linked create. Every store that tracks
// create state (PostgreSQL, SQLite) is an EventingStore.
func (s *syncServer) linked() meta.EventingStore {
	es, ok := s.store.(meta.EventingStore)
	assertf(ok, "sync create: store %T tracks create state but is not an EventingStore", s.store)
	return es
}

// completeOwnSyncCreate schedules the first sync of the pending repo this
// request just inserted, and marks the create complete (issue #388). It
// reports whether the caller may answer 201; otherwise it wrote the
// error. The request already passed #395's ref-policy refusal, before the
// insert.
func (s *syncServer) completeOwnSyncCreate(w http.ResponseWriter, r *http.Request, org, url, repoID string) bool {
	assertf(s.firstSync != nil && org != "" && url != "" && repoID != "", "completeOwnSyncCreate: missing inputs")
	out, err := s.firstSync.CompleteSyncCreate(r.Context(), org, repoID)
	return s.syncCompletionOK(w, r, org, url, repoID, out, err)
}

// syncCompletionOK reports whether a sync create's completion succeeded
// (completed, or a concurrent completion won); otherwise it writes the
// answer. A scheduling failure keeps every row and answers 503: a retry or
// pull-upstream (finishPendingSync) completes the create later. An
// upstream link removed first leaves the create pending for good (#388
// desk decision): 503 without Retry-After, naming the only way out. A
// delete that landed first answers 404, never success.
func (s *syncServer) syncCompletionOK(w http.ResponseWriter, r *http.Request, org, url, repoID string, out meta.CreateOutcome, err error) bool {
	switch {
	case errors.Is(err, meta.ErrNotFound):
		s.logger.Warn("upstream link removed before the first sync was scheduled; the repository stays pending", "org", org, "repo_id", repoID, "err", err)
		writeErr(w, http.StatusServiceUnavailable,
			"the repository exists, but its upstream link was removed before its first sync was scheduled; DELETE /repos/delete clears the name")
		return false
	case err != nil:
		s.logger.Error("schedule first sync; the repository stays pending", "org", org, "repo_id", repoID, "err", err)
		w.Header().Set("Retry-After", pendingCreateRetryAfter)
		writeErr(w, http.StatusServiceUnavailable,
			"the repository exists, but its first sync could not be scheduled; retry the create with a git:write token, or POST /repos/pull-upstream")
		return false
	case out == meta.CreateDeleted:
		writeErr(w, http.StatusNotFound, repoDeletedMsg(url))
		return false
	}
	assertf(out == meta.CreateCompleted || out == meta.CreateAlreadyComplete, "CompleteSyncCreate outcome %d", out)
	return true
}

// answerTakenSyncName answers a sync create of a taken name on a store that
// tracks create state (issue #388). Only a repo with the same stored
// upstream (provider, host, owner, name) and the same forward_pushes is
// compatible: a 200 must not hide that the repo forwards pushes when the
// caller asked it not to. The request's ref, default_branch and auth are
// ignored. A complete repo answers 200 and schedules nothing, so repo:write
// alone never triggers a sync. A pending one is finished only through
// finishPendingSync, with its stored link. Legacy and deleted rows keep
// answerTakenName's answers.
func (s *syncServer) answerTakenSyncName(w http.ResponseWriter, r *http.Request, claims *auth.Claims, url string, want meta.RepoUpstream) {
	assertf(claims != nil && url != "" && want.Provider != "", "answerTakenSyncName: missing inputs")
	org := claims.Iss
	repo, err := s.store.GetRepoByURL(r.Context(), org, url)
	if err != nil || repo.CreateState == meta.CreateStateLegacy {
		answerTakenName(w, r, s.store, s.internalErr, s.httpBaseURL, org, url, true) // 404 deleted, 409 legacy, or 500
		return
	}
	up, err := s.ups.GetUpstream(r.Context(), repo.RepoID)
	switch {
	case errors.Is(err, upsync.ErrNotFound): // a plain repo, a fork, or a detached sync
		writeErr(w, http.StatusConflict, "repository already exists")
		return
	case err != nil:
		s.internalErr(w, r, "get existing upstream", err)
		return
	case up.Provider != want.Provider || up.UpstreamHost != want.UpstreamHost || up.Owner != want.Owner || up.Name != want.Name:
		writeErr(w, http.StatusConflict, "repository already exists with a different upstream")
		return
	case up.ForwardOnPush != want.ForwardOnPush:
		writeErr(w, http.StatusConflict, "repository already exists with a different forward_pushes setting")
		return
	}
	if repo.CreateState == meta.CreateStatePending {
		denial, out, err := s.finishPendingSync(r.Context(), claims, repo)
		if denial != "" {
			writeErr(w, http.StatusForbidden, denial)
			return
		}
		if !s.syncCompletionOK(w, r, org, url, repo.RepoID, out, err) {
			return
		}
	}
	writeRepoExists(w, s.httpBaseURL, repo)
}

// finishPendingSync completes a pending sync create for a request other
// than the one that inserted it (issue #388). It is the only such path,
// and it enforces the authority of pull-upstream itself, whatever its
// caller checked: the first sync mirror-pushes every head and tag through
// the unpoliced loopback with force refspecs, so it can overwrite refs that
// clients pushed into the pending repo. A token with any ref-policy op, or
// without git:write, gets a non-empty denial and nothing is scheduled.
// Otherwise it returns CompleteSyncCreate's result, which uses the STORED
// upstream and credential; the job carries no ref.
func (s *syncServer) finishPendingSync(ctx context.Context, claims *auth.Claims, repo meta.Repo) (denial string, out meta.CreateOutcome, err error) {
	assertf(claims != nil && repo.RepoID != "", "finishPendingSync: nil claims or empty repo")
	assertf(s.firstSync != nil && repo.CreateState == meta.CreateStatePending,
		"finishPendingSync: repo %s create_state %d, scheduler %v", repo.RepoID, repo.CreateState, s.firstSync != nil)
	policies, err := refPolicies(claims)
	if err != nil {
		return err.Error(), 0, nil
	}
	if op, denied := policyDeniesPullUpstream(policies); denied {
		return "ref policy op " + strconv.Quote(op) + " denies completing a pending sync create", 0, nil
	}
	if !claims.HasScope(auth.ScopeGitWrite) {
		return "completing a pending sync create needs scope " + auth.ScopeGitWrite + "; POST /repos/pull-upstream with a git:write token finishes it", 0, nil
	}
	out, err = s.firstSync.CompleteSyncCreate(ctx, claims.Iss, repo.RepoID)
	return "", out, err
}

// createAbandon undoes the rows a failed post-CreateRepo step left behind,
// mirroring (*server).forkAbandon: the failure may BE the request context's
// cancellation, so detach from it (bounded) instead of orphaning the rows
// with a context.Canceled cleanup. Besides soft-deleting the repo row it
// best-effort deletes the upstream and credential rows created alongside it
// (docs/archive/phases/15-phase6-implementation-notes.md deferred #2); each step logs its own failure and not-found is
// fine (the step that failed determines which rows exist).
func (s *syncServer) createAbandon(ctx context.Context, org, url, repoID string) {
	assertf(s != nil && s.store != nil && s.creds != nil && s.ups != nil && s.logger != nil,
		"createAbandon: nil syncServer deps")
	assertf(org != "" && url != "" && repoID != "", "createAbandon: empty org/url/repoID")
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := s.ups.DeleteUpstream(ctx, repoID); err != nil && !errors.Is(err, upsync.ErrNotFound) {
		s.logger.Error("abandon failed create: delete upstream", "org", org, "repo_id", repoID, "err", err)
	}
	switch cred, err := s.creds.CredentialForRepo(ctx, repoID); {
	case errors.Is(err, upsync.ErrNotFound):
		// No credential was stored (e.g. token-less base_repo).
	case err != nil:
		s.logger.Error("abandon failed create: load credential", "org", org, "repo_id", repoID, "err", err)
	default:
		if err := s.creds.DeleteCredential(ctx, cred.ID); err != nil && !errors.Is(err, upsync.ErrNotFound) {
			s.logger.Error("abandon failed create: delete credential", "org", org, "repo_id", repoID, "err", err)
		}
	}
	if _, err := s.store.SoftDeleteRepo(ctx, org, url); err != nil {
		s.logger.Error("abandon failed create", "org", org, "url", url, "err", err)
	}
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
//
// A nil/empty policies slice is valid input by design (an unpoliced token);
// the tuples themselves come from parsed JWT claims (user input), so there
// is deliberately no entry assert on their contents.
func policyDeniesPullUpstream(policies []auth.RefPolicy) (string, bool) {
	for _, rule := range policies {
		if len(rule.Ops) > 0 {
			return rule.Ops[0], true
		}
	}
	return "", false
}

// strictPullBodyKey marks a request re-dispatched from POST
// /api/repos/{repo_name}/upstream/pull (alias.go); only the alias layer
// sets it.
type strictPullBodyKey struct{}

func withStrictPullBody(ctx context.Context) context.Context {
	return context.WithValue(ctx, strictPullBodyKey{}, true)
}

func isStrictPullBody(ctx context.Context) bool {
	v, _ := ctx.Value(strictPullBodyKey{}).(bool)
	return v
}

// handlePullUpstream implements POST /api/v1/repos/pull-upstream (scope
// git:write): enqueue a sync for the JWT repo claim target. 400 when the
// repo has no upstream base_repo (docs/reference/http-api.md: "only for repos with base_repo").
func (s *syncServer) handlePullUpstream(w http.ResponseWriter, r *http.Request) {
	assertf(s != nil && s.store != nil && s.ups != nil && s.queue != nil,
		"handlePullUpstream: nil syncServer deps")
	claims := mustClaims(r)
	// docs/reference/authentication.md: ref policies apply to every HTTP write endpoint. The
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
		decode := unmarshalBody
		if isStrictPullBody(r.Context()) {
			// POST .../upstream/pull (issue #378): ReposUpstreamPullRequest
			// is additionalProperties false, so a typo such as "refs" must
			// not silently sync the default branch. The deprecated route
			// keeps its lenient decode for existing callers.
			decode = strictJSON
		}
		if err := decode(body, &req); err != nil {
			writeInvalid(w, "invalid JSON body: "+err.Error(), err)
			return
		}
	}
	// The ref rides in the sync job's arguments, which PostgreSQL's River
	// queue stores as jsonb, and jsonb cannot hold a NUL (SQLSTATE 22P05).
	// Every backend refuses such a ref alike.
	if writeInvalidText(w, [2]string{"ref", req.Ref}) {
		return
	}
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeRepoNotFound(w)
		return
	case err != nil:
		s.internalErr(w, r, "resolve pull-upstream repo", err)
		return
	}
	if s.firstSync != nil && repo.CreateState == meta.CreateStatePending {
		// A sync create whose first sync was never scheduled (issue #388).
		// A pending fork has no upstream: ErrNotFound, as below.
		denial, out, err := s.finishPendingSync(r.Context(), claims, repo)
		switch {
		case denial != "":
			writeErr(w, http.StatusForbidden, denial)
			return
		case errors.Is(err, meta.ErrNotFound):
			writeErr(w, http.StatusBadRequest, "repository has no upstream base_repo")
			return
		case err != nil:
			s.internalErr(w, r, "complete pending sync create", err)
			return
		case out == meta.CreateDeleted:
			writeErr(w, http.StatusNotFound, "repository not found")
			return
		case out == meta.CreateCompleted:
			// CompleteSyncCreate recorded the request and scheduled the run.
			writeJSON(w, http.StatusAccepted, map[string]string{"message": pullUpstreamAcceptedMsg})
			return
		}
		// AlreadyComplete: a concurrent completion won; sync as usual.
		assertf(out == meta.CreateAlreadyComplete, "CompleteSyncCreate outcome %d", out)
	}
	// RequestSync doubles as the has-upstream precheck AND makes the 202
	// promise durable (issue #79): the pending marker is set BEFORE the
	// enqueue, so a request that coalesces with a run that already claimed
	// (whose upstream fetch may predate this request) survives to that
	// run's FinishSync, and the finisher schedules the follow-up run.
	err = s.ups.RequestSync(r.Context(), repo.RepoID)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		writeErr(w, http.StatusBadRequest, "repository has no upstream base_repo")
		return
	case err != nil:
		s.internalErr(w, r, "request sync", err)
		return
	}
	syncRef := req.Ref
	if syncRef == "" {
		// docs/archive/phases/14-phase5-implementation-notes.md deferred #8: an omitted ref targets the repo's default
		// branch instead of flowing empty into the sync job.
		syncRef = repo.DefaultBranch
		assertf(syncRef != "", "repo %s has empty default branch", repo.RepoID)
	}
	// The request is recorded above; a queue with a durable schedule
	// (SQLite, ADR-50) must only offer the run, or it could re-mark a
	// request that a run already claimed.
	enqueue := s.queue.EnqueueSync
	if o, ok := s.queue.(syncOfferer); ok {
		enqueue = o.OfferSync
	}
	if err := enqueue(r.Context(), upsync.Job{
		Org:    claims.Iss,
		RepoID: repo.RepoID,
		Ref:    syncRef,
	}); err != nil {
		s.internalErr(w, r, "enqueue sync", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"message": pullUpstreamAcceptedMsg})
}

type usyncPushUpstreamRequest struct {
	// Force opts into force refspecs (+refs/...): without it a
	// non-fast-forward ref is rejected per ref by the upstream and the
	// run fails with rejected_refs detail (issue #90).
	Force bool `json:"force"`
}

// pushUpstreamAcceptedMsg mirrors pullUpstreamAcceptedMsg for the outbound
// direction (our extension: upstream has no explicit push trigger).
const pushUpstreamAcceptedMsg = "Repository push initiated successfully"

// handlePushUpstream implements POST /api/v1/repos/push-upstream (scope
// git:write, issue #90): mark + enqueue an outbound push of refs/heads/* and
// refs/tags/* to the linked upstream. 400 when the repo has no upstream
// base_repo. Ref policies fail closed exactly like pull-upstream — the
// outbound run publishes EVERY local head and tag with the stored upstream
// credential, so a caller restricted on any ref must not be able to cause
// refs to be published upstream (policyDeniesPullUpstream's op coarseness
// applies unchanged: the run may touch any synced ref).
func (s *syncServer) handlePushUpstream(w http.ResponseWriter, r *http.Request) {
	assertf(s != nil && s.store != nil && s.ups != nil && s.queue != nil,
		"handlePushUpstream: nil syncServer deps")
	claims := mustClaims(r)
	policies, err := refPolicies(claims)
	if err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	if op, denied := policyDeniesPullUpstream(policies); denied {
		writeErr(w, http.StatusForbidden, "ref policy op "+strconv.Quote(op)+" denies push-upstream")
		return
	}
	var req usyncPushUpstreamRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeInvalid(w, "invalid JSON body: "+err.Error(), err)
			return
		}
	}
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeRepoNotFound(w)
		return
	case err != nil:
		s.internalErr(w, r, "resolve push-upstream repo", err)
		return
	}
	// RequestPush doubles as the has-upstream precheck AND makes the 202
	// promise durable, mirroring pull-upstream's RequestSync-before-enqueue
	// protocol (issue #79): a request that coalesces with a run that
	// already claimed survives to that run's FinishPush. Force rides the
	// durable marker, not the job — queue deduplication is keyed by repo.
	// A store with due times (the local schedule, ADR-50) records
	// it with RequestPushNow: an explicit request makes a waiting retry
	// due now and keeps its retry position. Implicit forward requests use
	// RequestPush and join the backoff.
	if pn, ok := s.ups.(pushNowRequester); ok {
		err = pn.RequestPushNow(r.Context(), repo.RepoID, req.Force)
	} else {
		err = s.ups.RequestPush(r.Context(), repo.RepoID, req.Force)
	}
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		writeErr(w, http.StatusBadRequest, "repository has no upstream base_repo")
		return
	case err != nil:
		s.internalErr(w, r, "request push", err)
		return
	}
	if err := s.queue.EnqueuePush(r.Context(), upsync.Job{
		Org:    claims.Iss,
		RepoID: repo.RepoID,
	}); err != nil {
		s.internalErr(w, r, "enqueue push", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"message": pushUpstreamAcceptedMsg})
}

// syncOfferer is the optional queue extension for pull runs whose request
// the caller already recorded (jobs.Local; SQLite schedules without a
// second write).
type syncOfferer interface {
	OfferSync(ctx context.Context, job upsync.Job) error
}

// pushNowRequester is the optional UpstreamStore extension for explicit
// push requests (the meta adapter; RequestPush on stores without due times).
type pushNowRequester interface {
	RequestPushNow(ctx context.Context, repoID string, force bool) error
}

// handleDetachBase implements DELETE /api/v1/repos/base (scope repo:write):
// idempotent upstream detach. In-flight or queued syncs observe the missing
// row and end silently — the documented cancellation semantics (docs/reference/webhooks.md).
func (s *syncServer) handleDetachBase(w http.ResponseWriter, r *http.Request) {
	assertf(s != nil && s.store != nil && s.ups != nil, "handleDetachBase: nil syncServer deps")
	claims := mustClaims(r)
	// Malformed ref-policy claims fail closed on write endpoints (docs/reference/authentication.md).
	if _, err := refPolicies(claims); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeRepoNotFound(w)
		return
	case err != nil:
		s.internalErr(w, r, "resolve detach repo", err)
		return
	}
	msg := "repository detached"
	err = s.ups.DeleteUpstream(r.Context(), repo.RepoID)
	switch {
	case errors.Is(err, upsync.ErrNotFound):
		// Already detached: idempotent success (openapi). Only the
		// path-style route names it (changelog 2026-10-07, issue #459):
		// the deprecated base routes keep their old text.
		if isUpstreamDetach(r.Context()) {
			msg = "repository already detached"
		}
	case err != nil:
		s.internalErr(w, r, "detach upstream", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": msg})
}

// upstreamDetachKey marks a request re-dispatched from DELETE
// /api/repos/{repo_name}/upstream (alias.go); only the alias layer sets it.
type upstreamDetachKey struct{}

func withUpstreamDetach(ctx context.Context) context.Context {
	return context.WithValue(ctx, upstreamDetachKey{}, true)
}

func isUpstreamDetach(ctx context.Context) bool {
	v, _ := ctx.Value(upstreamDetachKey{}).(bool)
	return v
}
