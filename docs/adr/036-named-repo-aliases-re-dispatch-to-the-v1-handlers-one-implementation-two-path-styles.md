# ADR-36: Named-repo aliases re-dispatch to the v1 handlers — one implementation, two path styles

> Extracted verbatim from [`15-phase6-implementation-notes.md`](../archive/phases/15-phase6-implementation-notes.md)
> (“Phase-6 Implementation Notes & ADRs (LFS, named-repo aliases, repo-urls, rate limiting)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

The openapi catalog defines the named-repo style (`/api/repos/{repo_name}/...`) in
parallel to the v1 style the SDKs use. Decision: **aliases are a thin routing layer over
the unchanged v1 handlers — the alias handler rewrites the request to the v1 path and
re-dispatches it through the mux**, giving identical bodies by construction. Duplicating
32 repo-scoped routes' handler logic would be 32 opportunities for the two styles to
drift; a re-dispatch cannot drift.

- Each alias registers with its v1 twin's exact auth flavor, org-level flag, and scopes;
  org-scoped routes (repo list/create, webhooks) get no alias — there is no single repo
  to name in the path.
- The handler URL-decodes `{repo_name}` (Go 1.22's mux matches the escaped path, so
  `pierre%2Fexample` stays one segment), resolves both the path name and the token's
  `repo` claim through `meta.ResolveRepoKey`, and answers 404 on unknown names or RepoID
  mismatch — shaped as the route's envelope flavor (`{"error"}` on reads, docs/04 result
  envelope on writes).
- Accepted costs: the re-dispatched request runs the v1 middleware chain again (double
  JWT verification), and rate limiting must not double-charge — the rewritten request
  carries a `rateLimitExempt` context marker so an aliased request costs exactly one
  token (ADR-37). Double verification is cheap; a halved effective alias rate would be a
  behavioral difference between the two styles.
