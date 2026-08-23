# ADR-31: Forking is synchronous — a one-shot deterministic clone inside POST /repos

> Extracted verbatim from [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md)
> (“Phase-5 Implementation Notes & ADRs (eventing, upstream sync, credentials, fork)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

`base_repo: {provider: "code"|"code.storage", operation: "fork", auth: {token}}` forks an
existing repo on this server (docs/01 invariant 12). Decision: **the fork completes
synchronously inside the `POST /repos` request** (`handlers_fork.go`), unlike provider
sync, which is async (202 + background worker).

Rationale. A fork's source lives in the *same* ref/blob stores — there is no network
fetch, so the work is bounded by packing the fork-point closure from the already
materialized source cache. The client contract also demands it: `POST /repos` answers
200 with a `repo_id`/`http_url` the SDK immediately clones or reads, so an async fork
would need either a lying response or a polling surface that docs/03 does not define.
Finally, the local-mode queue loses jobs on restart (ADR-27): an async fork could leave a
permanently empty shell repo, while the synchronous flow is atomic-or-abandoned —
everything up to and including packing happens *before* the repo row is created, and any
failure after the row exists soft-deletes it (`forkAbandon` — which detaches from the
possibly-canceled request context via `context.WithoutCancel` plus a bounded timeout, so
a client disconnect mid-failure still cleans up), so no orphaned live repo
remains. A lost race or crash can strand at most an unreferenced immutable pack, the same
debris every write pipeline may leave.

Mechanics (all in `handlers_fork.go`):

- **Dispatch.** `forkAwareCreateRepo(verifier, fallback)` reads the bounded body once and
  probes `base_repo`; only `operation: "fork"` with provider `code`/`code.storage` is
  handled here. Everything else — no `base_repo`, sync operations, non-code fork
  providers, unparseable bodies — replays the body verbatim to the fallback creator, so
  the sync-aware creator (handlers_sync.go) keeps owning its own validation. The fallback
  parameter is the integration seam; the exact `api.New` registration line is in the file
  header.
- **Source authorization.** The embedded `base_repo.auth.token` is verified with the same
  production `auth.Verifier` and must be repo-scoped with `git:read` on the source
  (missing token → 400; unverifiable/expired/wrong-scope/org-scoped → 403). The source
  org comes from that token's `iss`, so cross-org forks need nothing beyond a valid
  source-read JWT.
- **Fork point.** `sha` overrides `ref`; default is the source's default branch, resolved
  in the materialized source cache (`gitread.ResolveRef` — branch/tag/full-or-short sha,
  peeled to a commit). Unresolvable fork points are invalid input (400).
- **Independence.** The fork-point reachability closure is packed with no negatives and
  published under the **new** RepoID's storage keys (ADR-21: RepoID is the only storage
  key — no pack sharing, no copy-on-write coupling of two repos' GC lifetimes), then the
  fork's default branch (request `default_branch` honored; source's name inherited
  otherwise) is CAS-created from the zero SHA. Exactly one ref exists afterwards: no
  source branches, tags, or ephemeral refs leak. **No `repo_upstreams` row is written** —
  a fork is one-shot and never tracks its source; the caller's ref policies are enforced
  on the initial ref like any other HTTP write.
