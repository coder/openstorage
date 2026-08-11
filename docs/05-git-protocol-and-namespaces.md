# Git Smart HTTP, Namespaces & LFS

Besides the JSON API on `api.{org}.code.storage`, the backend must serve **standard git
smart HTTP** on the storage host `{org}.code.storage`. This is what makes plain
`git clone/push/fetch/pull` work.

## Remote URL scheme

```
https://t:<jwt>@{org}.code.storage/{repo_id}.git              # persistent namespace
https://t:<jwt>@{org}.code.storage/{repo_id}+ephemeral.git    # ephemeral namespace
https://t:<jwt>@{org}.code.storage/{repo_id}+import.git       # import namespace
```

- Basic auth: username is the fixed literal `t`, password is the JWT.
- JWT requirements are identical to the HTTP API: `repo` claim must match the repo,
  `git:read` for fetch/clone, `git:write` for push; `refs`/`ops` policies enforced during
  receive-pack per-ref.

## Endpoints to implement (git http-protocol, smart only)

```
GET  /{repo}.git/info/refs?service=git-upload-pack     # advertise refs (fetch/clone)
POST /{repo}.git/git-upload-pack                        # pack negotiation + packfile
GET  /{repo}.git/info/refs?service=git-receive-pack     # advertise refs (push)
POST /{repo}.git/git-receive-pack                        # receive pack, update refs
```

(The exact smart-protocol paths are standard git; the SDKs only construct the base remote
URL and hand it to `git`.) Serving via `git http-backend` or a git library (go-git, libgit2,
gitoxide, JGit) against the repo storage is sufficient. Dumb protocol is not required.

Requirements on the receive path:
- Enforce JWT ref policies per ref update (`no-push`, `no-force-push`, `verify-sig`),
  first-match-wins, reporting per-ref `ng` results with a policy message.
- Namespace mapping: `+ephemeral` remotes see/update only the ephemeral ref space;
  persistent remotes must not see ephemeral refs (git namespaces or separate ref stores).
- Push to persistent refs emits `push` webhooks (before/after SHAs per ref) and schedules
  upstream sync for synced repos.
- `+import` pushes accept large histories; official docs describe immediate cold archival —
  for an OSS backend, a functional equivalent is "accept the objects, keep refs" (tiering
  optional).

## Git LFS

The storage host must also expose the LFS Batch API at the standard discovery path:

```
POST https://{org}.code.storage/{repo_id}.git/info/lfs/objects/batch
```

Same JWT basic-auth remote; no separate LFS server or config. Implementing the batch API +
object upload/download (e.g. via S3-presigned or local storage) covers the documented
behavior ("no separate LFS server, no extra configuration").

## Interop invariant

Objects/refs written through the HTTP API (commit-pack, merge, tags, notes) and through git
push must land in the **same object database and ref store** — the consumer freely mixes
`git clone` with API-created commits, and integration tests poll `GET /repos/branches`
right after pushes.

## Notes on refs via git

- Notes refs (`refs/notes/*`) and tags are fetchable/pushable via standard git refspecs
  (e.g. `git fetch origin 'refs/notes/*:refs/notes/*'`), so the smart-HTTP layer must not
  filter them.
- The API's branch listing shows `created_at` per branch — the backend needs per-ref
  creation metadata beyond what bare git stores (reflog or DB column).
