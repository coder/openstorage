# ADR-35: LFS objects are keyed by base RepoID — namespaces share one object space

> Extracted verbatim from [`15-phase6-implementation-notes.md`](../archive/phases/15-phase6-implementation-notes.md)
> (“Phase-6 Implementation Notes & ADRs (LFS, named-repo aliases, repo-urls, rate limiting)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

`blob.LFSKey(repoID, oid) = lfs/<repoID>/<oid>`. Decision: **the `+ephemeral` /
`+import` namespaces map onto the base repo's LFS object space** — a batch request on
`/{repo_id}+ephemeral.git` resolves keys under the base RepoID.

This is the deliberate inverse of ADR-3 (separate ref stores per namespace), and the
reason is the same distinction that drove ADR-3: refs are mutable *positions* that must
not leak across namespaces, while LFS objects are immutable *content* — a pointer file
in any namespace addresses exactly the same bytes. Sharing the space means an ephemeral
branch can carry LFS objects and promotion needs no object copying, exactly like pack
objects. The key stays **repo-scoped** rather than global, though: RepoID remains the
only storage key (ADR-21), so two repos' GC lifetimes never couple, and a batch `Stat`
probe on one repo cannot be used as a cross-repo existence oracle for content known only
by hash.
