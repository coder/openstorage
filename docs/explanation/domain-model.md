# Domain Model & Backend Invariants

**TL;DR:** the entities the code.storage API exposes and the invariants a compatible
backend must uphold. An organization owns repositories with refs in three namespaces; ref
updates use compare-and-swap; several response details are load-bearing for clients.

## Entity model

```
Organization (org)                     ← tenant; from subdomain + JWT `iss`
 └── Repository
      ├── repo_id: string             ← internal ID (e.g. "repo_7f2b3d9" / UUID)
      ├── url: string                 ← client-facing URL path; may contain slashes ("team/project"); the value used in the JWT `repo` claim
      ├── default_branch: string      ← defaults to "main"
      ├── created_at: ISO-8601 string
      ├── upstream? / base_repo?      ← optional upstream link {provider, owner, name, ...}
      ├── git_credentials?            ← stored HTTPS credential for generic upstreams
      ├── Refs
      │    ├── refs/heads/*           ← persistent branches
      │    ├── refs/tags/*            ← lightweight tags (annotated tags readable, not creatable via API)
      │    ├── refs/notes/*           ← git notes (default refs/notes/commits)
      │    └── ephemeral namespace    ← isolated ref space, exposed as {repo_id}+ephemeral.git
      ├── Commits (sha, parent_shas[], author, committer, date, message, signature?, payload?)
      └── Trees/Blobs (mode: 100644 | 100755 | 120000 | 160000 | 040000)
```

- **Multi-tenancy is host-based:** `api.{org}.code.storage` (API) and `{org}.code.storage`
  (git storage). The JWT `iss` claim must match the host's org.
- **Repositories have two identities:** an internal `repo_id` and a `url` path. The JWT
  `repo` claim carries the `url`. `GET /api/repo-urls/{repo_id}` resolves internal ID →
  url (org:read scope, no per-repo claim needed).
- **There is no user model.** Authentication is entirely customer-signed JWTs (see the
  [auth spec](../reference/authentication.md)); authorization is claims-based. The
  backend never stores accounts.

## Ref namespaces

| Namespace | Git remote | Notes |
|---|---|---|
| persistent | `{repo_id}.git` | normal branches/tags/notes; triggers webhooks + upstream sync |
| ephemeral  | `{repo_id}+ephemeral.git` | disposable branches; never mirrored/synced upstream; default-branch protection does not apply; promotable to persistent |
| import     | `{repo_id}+import.git` | bulk ingestion; pushed objects go (per official docs) to immediate cold archival |

The API addresses ephemeral refs with boolean flags, not ref names: `ephemeral`,
`ephemeral_base`, `base_is_ephemeral`, `target_is_ephemeral`, `source_is_ephemeral`. To
promote an ephemeral branch, call `POST /repos/branches/create` with
`base_is_ephemeral: true, target_is_ephemeral: false`.

## Invariants a backend MUST uphold

Derived from SDK behavior and the real consumer (`just-code-storage`):

1. **Optimistic concurrency (CAS) everywhere.**
   - `commit-pack`, `diff-commit`, `restore-commit`: `expected_head_sha` vs current branch tip.
   - `merge`: `expected_target_sha` vs target branch tip.
   - notes write/delete: `expected_ref_sha` vs notes ref tip.
   - On mismatch: fail with HTTP 409, code `ref_guard_failed`, `guard` and `current_sha`.
     The commit writes keep their `result` object
     `{target_branch, branch, old_sha, new_sha: "", success: false, status: "conflict"}`
     with `commit: null`, and the notes writes keep
     the note envelope with `result.status` `conflict`.
     Clients (e.g. `just-code-storage` push) map this to git's "failed to push some refs"
     UX, so the shape is load-bearing.
2. **Root-commit signaling.** For a first commit on an unborn branch, `result.old_sha` is
   the all-zeros SHA (`0{40}`); the consumer detects root commits via `/^0+$/`.
3. **Unborn branches.** A new repo has an unborn default branch. `GET
   /repos/files/metadata` (and file reads) on an unborn branch MUST return 404; the
   consumer falls back to an empty baseline for `git status`.
4. **Default branch protection.** `DELETE /repos/branches` MUST refuse to delete the
   repository's default branch (persistent namespace only).
5. **Ref policy enforcement.** The JWT `refs` claim is an ordered list of `[pattern, ops]`
   rules; **first match wins**. Ops: `no-push` (reject all writes), `no-force-push`
   (reject non-fast-forward), `verify-sig` (reject unsigned/unverified commits). Enforced
   on both the git receive-pack path and the HTTP write endpoints. Short names normalize
   to `refs/heads/<name>`.
6. **Read-only preview.** `GET /repos/merge/preview` must never mutate refs or persist
   objects (conflict rendering may use request-local scratch storage). A conflicted
   preview returns HTTP 200 with `status: "conflicted"`; a conflicted *actual* merge
   returns HTTP 409.
7. **Ephemeral isolation.** Ephemeral refs never trigger upstream sync/mirroring or
   webhooks intended for persistent refs. Deleting ephemeral branches skips
   default-branch protection.
8. **Deletion is soft-first.** `DELETE /repos/delete` soft-deletes, then cleans up
   hot/cold storage asynchronously; repeat deletion returns 409 ("already deleted").
   - Here the async cleanup is the retention purge
     ([`OPENSTORAGE_REPO_RETENTION`](../reference/configuration.md#openstorage_repo_retention)
     / [`openstorage purge-repo`](../reference/cli.md#openstorage-purge-repo)). It removes
     the repository's storage permanently, with no restore.
   - The name burn persists: URL-scoped tokens may outlive any retention, so the
     `(org, url)` pair is never released.
9. **Commit ordering.** `GET /repos/commits` returns history newest-first by committer date,
   cursor-paginated.
10. **Tag semantics.** `POST /repos/tags` creates lightweight tags only; `target` must
    resolve to a commit. List/delete must dereference annotated tags correctly (the
    listed `sha` is the *dereferenced commit* SHA).
11. **Upstream sync.** Repos created with `upstream` (or the deprecated `base_repo` sync
    form) mirror an external provider (`github`, `gitlab`, `bitbucket`, `gitea`,
    `forgejo`, `codeberg`, `sr.ht`; self-hosted instances via `upstream.host` or
    `upstream_host`, credentials via git-credentials).
    - Non-ephemeral ref mutations on synced repos schedule background sync, unless the
      writing token carries a ref-policy op (see
      [Ref policies fail closed](../reference/http-api.md#upstream-sync)).
      `pull-upstream` returns 202 (async); `DELETE /repos/base` detaches idempotently.
    - The sync lifecycle emits `repo.sync.started/succeeded/failed` webhooks.
    - At most one sync run per repo is in flight. A `pull-upstream` accepted during a run
      is never dropped: it coalesces into one follow-up run scheduled when the in-flight
      run finishes. So a 202 always means a sync run will observe upstream as of the
      request or later.
    - The in-flight claim is leased (holder token + expiry derived from the run budget).
      If the holder dies mid-run, the claim expires and the next run steals it
      atomically, with no operator intervention. Holder-token guards refuse a stolen
      run's late writes. A crash can delay syncing but never permanently stop it.
12. **Forking.** `base_repo: {provider: "code", operation: "fork", auth: {token: <jwt>}}`
    forks an existing code.storage repo (optionally at `ref`/`sha`) into an independent
    repo. The fork-source JWT carries `git:read` on the source repo. Upstream's copy form,
    `base_repo: {name, ref?, auth: {token}}` without `provider`, runs the same fork;
    `name` must name the repo the token grants.

## Consumer-assumed response semantics (load-bearing details)

- **Diffs:** endpoints return per-file pre-formatted unified diff text in `files[].raw`;
  consumers concatenate these directly (`git show`, `git diff`).
  `gitApplyCompatible=true` must produce `git apply`-able patches.
- **File reads:** `GET /repos/file` responses must include `X-Blob-Sha` and
  `X-Last-Commit-Sha` headers, a strong `ETag` (the quoted blob SHA), `Last-Modified`,
  `Accept-Ranges: bytes`, and support `Range` (206), `If-None-Match` (304),
  `If-(Un)Modified-Since`, `If-Match` (412), `If-Range` (416 on bad ranges).
- **Commit lookup:** `GET /repos/commit?sha=` resolves branch names, short SHAs, and full
  SHAs; the consumer's `rev-parse` builds on this.
- **Blame:** line numbers are 1-indexed and map 1:1 to the file content at the blamed
  ref.
- **Grep:** results may be asynchronously indexed; the vendor's own integration test polls
  after a commit until matches appear. An open-source backend MAY grep synchronously:
  strictly fresher than the reference implementation, which is safe for clients.

## Known gaps (not derivable from public sources)

- Signing-key registration for `verify-sig` (no public endpoint; likely control-plane).
- Exact cursor token encoding (opaque to clients; any stable encoding works).
- Grep/archive 503 "warming from cold storage" behavior implies hot/cold tiering; an
  open-source backend can serve everything hot and never emit these.
