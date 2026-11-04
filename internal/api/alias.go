// Named-repo route aliases /api/repos/{repo_name}/... (docs/README,
// openapi.yaml): a thin routing layer over the unchanged /api/v1 handlers,
// identical bodies by construction. Contract (frozen by the phase-6
// foundation):
//
//   - One alias per repo-scoped v1 route, registered with the SAME
//     scopes as its v1 twin. Org-scoped routes (GET/POST
//     /api/v1/repos list+create, webhooks) get NO alias.
//   - Handler shape: decode r.PathValue("repo_name") (Go 1.22 mux matches
//     the escaped path, so "pierre%2Fexample" stays one segment) →
//     meta.ResolveRepoKey for both the path name and the token's repo claim
//     → unknown name or RepoID mismatch ⇒ 404 `repository_not_found`
//     → rewrite a cloned
//     *http.Request to the v1 path (query preserved) and re-dispatch through
//     mux. Double JWT verification is accepted.
//   - Every alias is registered through rl.limit() BEFORE auth,
//     exactly like the v1 routes. The re-dispatch runs the v1 chain again,
//     so the rewritten request carries the rateLimitExempt context marker:
//     the v1 twin's limiter passes it through and each aliased request is
//     charged exactly one token (unlike the accepted double JWT
//     verification, double-charging would halve the effective alias rate).
package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// aliasRoute maps METHOD /api/repos/{repo_name}<suffix> onto METHOD <v1Path>
// with the v1 route's exact org-level flag and scopes.
type aliasRoute struct {
	method string
	// suffix is the path after {repo_name} ("" aliases the repo root).
	suffix string
	v1Path string
	// orgLevel mirrors the v1 route's requireAuth org-level flag (only the
	// PUT/DELETE git-credentials routes). Org tokens carry repo claim
	// "org", so these skip the path-vs-claim repo check.
	orgLevel bool
	scopes   []string
	// mark, when set, tags the re-dispatched request's context so the
	// shared v1 handler can apply a path-style operation's own contract
	// (POST .../notes/append, issue #378). Clients cannot set it.
	mark func(context.Context) context.Context
	// deprecated sets the Deprecation header on every response of the
	// alias (issue #456). The v1 twin cannot decide it: its handler also
	// serves the non-deprecated path-style replacements.
	deprecated bool
}

// aliasRoutes mirrors every repo-scoped v1 route registered in api.go /
// handlers_write_*.go / handlers_credentials.go / handlers_sync.go. The
// org-level flag and scopes are copied verbatim from the v1 registration
// sites; keep the two tables in lockstep.
func aliasRoutes() []aliasRoute {
	alias := func(method, suffix, v1Path string, scopes ...string) aliasRoute {
		return aliasRoute{method: method, suffix: suffix, v1Path: v1Path, scopes: scopes}
	}
	deprecatedRoute := func(ar aliasRoute) aliasRoute {
		ar.deprecated = true
		return ar
	}
	return []aliasRoute{
		// Repo root: GET/PATCH /api/v1/repo and DELETE /api/v1/repos/delete.
		alias(http.MethodGet, "", "/api/v1/repo", auth.ScopeGitRead),
		alias(http.MethodPatch, "", "/api/v1/repo", auth.ScopeRepoWrite),
		alias(http.MethodDelete, "", "/api/v1/repos/delete", auth.ScopeRepoWrite),

		// git:read surface (api.go route table).
		alias(http.MethodGet, "/file", "/api/v1/repos/file", auth.ScopeGitRead),
		alias(http.MethodGet, "/files", "/api/v1/repos/files", auth.ScopeGitRead),
		alias(http.MethodGet, "/files/metadata", "/api/v1/repos/files/metadata", auth.ScopeGitRead),
		alias(http.MethodGet, "/branches", "/api/v1/repos/branches", auth.ScopeGitRead),
		alias(http.MethodGet, "/branch", "/api/v1/repos/branch", auth.ScopeGitRead),
		alias(http.MethodGet, "/branches/diff", "/api/v1/repos/branches/diff", auth.ScopeGitRead),
		alias(http.MethodGet, "/commits", "/api/v1/repos/commits", auth.ScopeGitRead),
		alias(http.MethodGet, "/commit", "/api/v1/repos/commit", auth.ScopeGitRead),
		alias(http.MethodGet, "/diff", "/api/v1/repos/diff", auth.ScopeGitRead),
		alias(http.MethodGet, "/tags", "/api/v1/repos/tags", auth.ScopeGitRead),
		alias(http.MethodGet, "/tag", "/api/v1/repos/tag", auth.ScopeGitRead),
		alias(http.MethodGet, "/blame", "/api/v1/repos/blame", auth.ScopeGitRead),
		alias(http.MethodPost, "/grep", "/api/v1/repos/grep", auth.ScopeGitRead),
		alias(http.MethodPost, "/archive", "/api/v1/repos/archive", auth.ScopeGitRead),
		alias(http.MethodGet, "/notes", "/api/v1/repos/notes", auth.ScopeGitRead),
		alias(http.MethodGet, "/notes/refs", "/api/v1/repos/notes/refs", auth.ScopeGitRead),
		alias(http.MethodGet, "/merge/preview", "/api/v1/repos/merge/preview", auth.ScopeGitRead),

		// git:write surface.
		alias(http.MethodPost, "/commit-pack", "/api/v1/repos/commit-pack", auth.ScopeGitWrite),
		alias(http.MethodPost, "/diff-commit", "/api/v1/repos/diff-commit", auth.ScopeGitWrite),
		alias(http.MethodPost, "/restore-commit", "/api/v1/repos/restore-commit", auth.ScopeGitWrite),
		alias(http.MethodPost, "/branches/create", "/api/v1/repos/branches/create", auth.ScopeGitWrite),
		alias(http.MethodDelete, "/branches", "/api/v1/repos/branches", auth.ScopeGitWrite),
		alias(http.MethodPost, "/tags", "/api/v1/repos/tags", auth.ScopeGitWrite),
		alias(http.MethodDelete, "/tags", "/api/v1/repos/tags", auth.ScopeGitRead, auth.ScopeGitWrite),
		alias(http.MethodPost, "/notes", "/api/v1/repos/notes", auth.ScopeGitWrite),
		alias(http.MethodDelete, "/notes", "/api/v1/repos/notes", auth.ScopeGitWrite),
		// Path-style append (spec 2026-10-05, issue #378): no v1 twin, so it
		// re-dispatches to the notes write route marked as an append.
		{method: http.MethodPost, suffix: "/notes/append", v1Path: "/api/v1/repos/notes",
			scopes: []string{auth.ScopeGitWrite}, mark: withNoteAppend},
		alias(http.MethodPost, "/merge", "/api/v1/repos/merge", auth.ScopeGitWrite),

		// Sync + credentials.
		deprecatedRoute(alias(http.MethodPost, "/pull-upstream", "/api/v1/repos/pull-upstream", auth.ScopeGitWrite)),
		alias(http.MethodPost, "/push-upstream", "/api/v1/repos/push-upstream", auth.ScopeGitWrite, auth.ScopeGitRead),
		deprecatedRoute(alias(http.MethodDelete, "/base", "/api/v1/repos/base", auth.ScopeRepoWrite)),
		// Path-style replacements of pull-upstream and base (spec
		// 2026-10-05, issue #378): same handlers, bodies and scopes.
		{method: http.MethodPost, suffix: "/upstream/pull", v1Path: "/api/v1/repos/pull-upstream",
			scopes: []string{auth.ScopeGitWrite}, mark: withStrictPullBody},
		{method: http.MethodDelete, suffix: "/upstream", v1Path: "/api/v1/repos/base",
			scopes: []string{auth.ScopeRepoWrite}, mark: withUpstreamDetach},
		alias(http.MethodPost, "/git-credentials", "/api/v1/repos/git-credentials", auth.ScopeRepoWrite),
		{method: http.MethodPut, suffix: "/git-credentials", v1Path: "/api/v1/repos/git-credentials",
			orgLevel: true, scopes: []string{auth.ScopeRepoWrite}},
		{method: http.MethodDelete, suffix: "/git-credentials", v1Path: "/api/v1/repos/git-credentials",
			orgLevel: true, scopes: []string{auth.ScopeRepoWrite}},
	}
}

// registerAliasRoutes wires the alias surface onto mux, which is both the
// registration target and the re-dispatch target for the rewritten v1
// requests. ss backs the path-id git-credentials routes registered below.
func registerAliasRoutes(mux *http.ServeMux, s *server, ss *syncServer, verifier auth.Verifier, org string, rl *rateLimiter) {
	assertf(mux != nil && s != nil && ss != nil && verifier != nil, "registerAliasRoutes: nil dependency")
	seen := make(map[string]bool)
	for _, ar := range aliasRoutes() {
		assertf(ar.method != "", "alias route %q: empty method", ar.suffix)
		assertf(ar.suffix == "" || strings.HasPrefix(ar.suffix, "/"), "alias suffix %q must be empty or start with /", ar.suffix)
		assertf(strings.HasPrefix(ar.v1Path, "/api/v1/"), "alias %s%s: v1 path %q outside /api/v1/", ar.method, ar.suffix, ar.v1Path)
		assertf(len(ar.scopes) > 0, "alias %s %s: no scopes", ar.method, ar.suffix)

		pattern := ar.method + " /api/repos/{repo_name}" + ar.suffix
		assertf(!seen[pattern], "duplicate alias pattern %q", pattern)
		seen[pattern] = true

		h := rl.limit()(requireAuth(verifier, org, ar.orgLevel, ar.scopes...)(s.aliasHandler(mux, ar)))
		if ar.deprecated {
			h = deprecated(h)
		}
		mux.Handle(pattern, h)
	}

	// Path-id git-credentials routes (openapi.yaml; docs/archive/phases/15-phase6-implementation-notes.md deferred #1):
	// direct routes with NO v1 twin to re-dispatch to — the handlers own
	// the alias-layer repo_name resolution themselves
	// (handlers_credentials.go). Auth is per-repo repo:write behind
	// rl.limit().
	for _, route := range []struct {
		method  string
		handler http.HandlerFunc
	}{
		{http.MethodPut, ss.handleUpdateCredentialByPath},
		{http.MethodDelete, ss.handleDeleteCredentialByPath},
	} {
		mux.Handle(route.method+" /api/repos/{repo_name}/git-credentials/{git_credential_id}",
			rl.limit()(requireAuth(verifier, org, false, auth.ScopeRepoWrite)(route.handler)))
	}
}

// aliasHandler resolves the path repo name, enforces the claim/path repo
// match (404 on mismatch so foreign tokens cannot probe name existence),
// then re-dispatches the request to the v1 twin through mux.
func (s *server) aliasHandler(mux *http.ServeMux, ar aliasRoute) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("repo_name")
		// A {repo_name} wildcard never matches an empty segment.
		assertf(name != "", "aliasHandler: empty repo_name path value")
		claims := mustClaims(r)

		repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, name)
		switch {
		case errors.Is(err, meta.ErrNotFound):
			writeRepoNotFound(w)
			return
		case err != nil:
			s.internalErr(w, r, "resolve alias repo name", err)
			return
		}

		if !ar.orgLevel {
			// requireAuth guarantees a non-empty, non-"org" repo claim here.
			claimRepo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
			switch {
			case errors.Is(err, meta.ErrNotFound):
				writeRepoNotFound(w)
				return
			case err != nil:
				s.internalErr(w, r, "resolve alias repo claim", err)
				return
			}
			if claimRepo.RepoID != repo.RepoID {
				writeRepoNotFound(w)
				return
			}
		}

		// Re-dispatch the rewritten request through the mux so the v1 route
		// (its auth + handler) serves it unchanged. The body is untouched
		// above, so the v1 handler reads it from the start. The exempt
		// marker stops the v1 limiter from charging a second token — this
		// request already paid at the alias registration.
		ctx := withAliasDispatch(rateLimitExempt(r.Context()))
		if ar.mark != nil {
			ctx = ar.mark(ctx)
		}
		r2 := r.Clone(ctx)
		r2.URL.Path = ar.v1Path
		r2.URL.RawPath = "" // v1 paths need no escaping; Path is canonical
		r2.RequestURI = r2.URL.RequestURI()
		mux.ServeHTTP(w, r2)
	}
}

// aliasDispatchKey marks a request that aliasHandler re-dispatched to its
// v1 twin; only aliasHandler sets it.
type aliasDispatchKey struct{}

func withAliasDispatch(ctx context.Context) context.Context {
	return context.WithValue(ctx, aliasDispatchKey{}, true)
}

func isAliasDispatch(ctx context.Context) bool {
	v, _ := ctx.Value(aliasDispatchKey{}).(bool)
	return v
}
