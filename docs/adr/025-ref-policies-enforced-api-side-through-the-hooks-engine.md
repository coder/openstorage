# ADR-25: Ref policies enforced API-side through the hook's engine

> Extracted verbatim from [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md)
> (“Phase-4 Implementation Notes & ADRs (write API + p3 read burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

The receive-hook policy core (`auth.ParseRefPolicies`/`FoldOps`/`MatchPolicy` + the
enforce loop) is shared with the write handlers, called with full ref names
(`refs/heads/…`, `refs/tags/…`, `refs/notes/…`): `no-push` blocks all writes/deletes,
`no-force-push` blocks non-fast-forward updates and deletes (is-ancestor callback),
`verify-sig` rejects unsigned server-created commits — parity with the push path
(invariant 5). Default-branch deletion is refused in the persistent namespace
(invariant 4) and exempted for `+ephemeral` (invariant 7).
