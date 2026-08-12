// Named-repo route aliases /api/repos/{repo_name}/... (docs/README,
// openapi.yaml): a thin routing layer over the unchanged /api/v1 handlers,
// identical bodies by construction. Foundation wiring for the phase-6 alias
// slice (workstream B); the contract below is frozen:
//
//   - One alias per repo-scoped v1 route, registered with the SAME auth
//     flavor and scopes as its v1 twin. Org-scoped routes (GET/POST
//     /api/v1/repos list+create, webhooks) get NO alias.
//   - Handler shape: decode r.PathValue("repo_name") (Go 1.22 mux matches
//     the escaped path, so "pierre%2Fexample" stays one segment) →
//     meta.ResolveRepoKey for both the path name and the token's repo claim
//     → unknown name or RepoID mismatch ⇒ 404 (error envelope on read
//     routes, result envelope on write routes) → rewrite a cloned
//     *http.Request to the v1 path (query preserved) and re-dispatch through
//     mux. Double JWT verification is accepted.
//   - Every alias is registered through rl.limit(flavor) BEFORE auth,
//     exactly like the v1 routes.
package api

import (
	"net/http"

	"codestorage/internal/auth"
)

// registerAliasRoutes wires the alias surface onto mux, which is both the
// registration target and the re-dispatch target for the rewritten v1
// requests. Foundation hook: workstream B registers the {method,
// aliasSuffix → v1Path} table here.
func registerAliasRoutes(mux *http.ServeMux, s *server, verifier auth.Verifier, rl *rateLimiter) {
	assertf(mux != nil && s != nil && verifier != nil, "registerAliasRoutes: nil dependency")
	_ = rl // applied per alias route once the table lands
}
