# ADR-21: Repos are keyed by `repo_id` everywhere; URL path elements resolve via meta

> Extracted verbatim from [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md)
> (“Phase-4 Implementation Notes & ADRs (write API + p3 read burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.
>
> **Amended by [#804](https://github.com/coder/openstorage/issues/804):** the `+import`
> remote no longer has its own storage key. It is push-only, and an import push writes the
> base repository's refs ([import remote](../explanation/git-protocol-and-namespaces.md#import-remote)).
> Data that 0.20.0 and earlier stored under `{repo}+import` stays in storage.

The p3 gate surfaced a repo-key split: `internal/githttp` keyed packs/refs/hook env by the
git URL path element verbatim while `internal/api` keyed by `RepoID`, so a push to
`/{name}.git` was accepted but invisible to every read endpoint. Decision: **`RepoID` (plus
the `+ephemeral`/`+import` namespace suffix) is the only storage key.** githttp resolves the
path element (after stripping the namespace suffix) through the metadata store — try
`GetRepoByID`, then `GetRepoByURL` (`meta.ResolveRepoKey`, shared with the API's claim
resolution) — and canonicalizes before any pack/ref/hook-env use.
Path elements that resolve to no repo record are a **404 on both surfaces**: repos must be
created via `POST /api/v1/repos` first (docs/05 addresses git URLs by `repo_id`; auth
already accepted both spellings).
