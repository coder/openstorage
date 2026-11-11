# Git Smart HTTP, Namespaces & LFS

**TL;DR:** besides the JSON API on `api.{org}.code.storage`, the backend must serve
**standard git smart HTTP** on the storage host `{org}.code.storage`, so plain
`git clone/push/fetch/pull` work.

## Remote URL scheme

```
https://t:<jwt>@{org}.code.storage/{repo_id}.git              # persistent namespace
https://t:<jwt>@{org}.code.storage/{repo_id}+ephemeral.git    # ephemeral namespace
https://t:<jwt>@{org}.code.storage/{repo_id}+import.git       # import namespace
```

- Basic auth: the username is the fixed literal `t`; the password is the JWT.
- JWT requirements match the HTTP API: the `repo` claim must match the repo; `git:read`
  for fetch/clone, `git:write` for push; `refs`/`ops` policies enforced per ref during
  receive-pack.

## Endpoints to implement (git http-protocol, smart only)

```
GET  /{repo}.git/info/refs?service=git-upload-pack     # advertise refs (fetch/clone)
POST /{repo}.git/git-upload-pack                        # pack negotiation + packfile
GET  /{repo}.git/info/refs?service=git-receive-pack     # advertise refs (push)
POST /{repo}.git/git-receive-pack                        # receive pack, update refs
```

These are standard git paths; the SDKs only build the base remote URL and hand it to
`git`. Serving via `git http-backend` or a git library (go-git, libgit2,
gitoxide, JGit) against the repo storage is sufficient. The dumb protocol is not
required.

The receive path must:

- Enforce JWT ref policies per ref update (`no-push`, `no-force-push`, `verify-sig`),
  first match wins, reporting per-ref `ng` results with a policy message.
- Map namespaces: `+ephemeral` remotes see and update only the ephemeral ref space;
  persistent remotes must not see ephemeral refs (git namespaces or separate ref stores).
  open.storage gives each remote its own storage key. The HTTP API, though, stores its
  ephemeral branches under the base repository's key as
  `refs/namespaces/ephemeral/refs/heads/<name>`, so the persistent remote hides
  `refs/namespaces` and every ref under it: fetch, clone and `ls-remote` never list one, a
  fetch of one by name fails (`couldn't find remote ref`), and a push that updates or
  deletes one gets its own `ng` (`deny updating a hidden ref` or
  `deny deleting a hidden ref`). An `--atomic` push that names one lands nothing, and so
  does a push whose new objects only such a ref needs. The `+ephemeral` and `+import`
  remotes hide nothing. One exception: git still resolves a hidden ref that a fetch names
  in `--shallow-exclude`, and cuts the history at its commit, so a client with `git:read`
  can tell whether such a ref exists. The API's branch list shows the ephemeral branches to
  the same token.
- On pushes to persistent refs, emit `push` webhooks (before/after SHAs per ref) and
  schedule upstream sync for synced repos.
- Accept large histories on `+import` pushes. Official docs describe immediate cold
  archival; for an OSS backend, "accept the objects, keep refs" is a functional
  equivalent (tiering optional).

Before open.storage hid them, a push to the persistent remote could write refs under
`refs/namespaces/`. Those refs stay after an upgrade, and git can no longer list or delete
them. Delete every one you do not want before you upgrade: list them with
`git ls-remote <url> 'refs/namespaces/*'`, and delete each one with
`git push <url> --delete <ref>`. Do not count on the API to delete them later: it deletes
only ephemeral branches whose names it accepts.

## Git LFS

The storage host must also expose the LFS Batch API at the standard discovery path:

```
POST https://{org}.code.storage/{repo_id}.git/info/lfs/objects/batch
```

Same JWT basic-auth remote; no separate LFS server or config. Implementing the batch API
plus object upload/download (e.g. via S3-presigned or local
storage) covers the documented behavior ("no separate LFS server, no extra
configuration").

## Interop invariant

Objects and refs written through the HTTP API (commit-pack, merge, tags, notes) and
through git push must land in the **same object database and ref store**. The consumer
freely mixes `git clone` with API-created commits, and integration tests poll
`GET /repos/branches` right after pushes.

## Notes on refs via git

- Notes refs (`refs/notes/*`) and tags are fetchable and pushable via standard git
  refspecs (e.g. `git fetch origin 'refs/notes/*:refs/notes/*'`), so the smart-HTTP layer
  must not filter them.
- The API's branch listing shows `created_at` per branch, so the backend needs per-ref
  creation metadata beyond what bare git stores (reflog or DB column).
