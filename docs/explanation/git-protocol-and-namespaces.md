# Git Smart HTTP, Namespaces & LFS

**TL;DR:** besides the JSON API on `api.{org}.code.storage`, the backend must serve
**standard git smart HTTP** on the storage host `{org}.code.storage`, so plain
`git clone/push/fetch/pull` work.

## Remote URL scheme

```
https://t:<jwt>@{org}.code.storage/{repo_id}.git              # persistent namespace
https://t:<jwt>@{org}.code.storage/{repo_id}+ephemeral.git    # ephemeral namespace
https://t:<jwt>@{org}.code.storage/{repo_id}+import.git       # import remote (push only)
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
- On pushes to persistent refs, emit `push` webhooks (before/after SHAs per ref) and
  schedule upstream sync for synced repos.
- Accept large histories on `+import` pushes (see [Import remote](#import-remote)).

## Import remote

The `+import` remote follows upstream's
[import page](https://code.storage/docs/repos/imports):

- **Push only.** A clone, fetch or `ls-remote` through `{repo_id}+import.git` gets `403`
  with a `text/plain` body, and git prints it as
  `remote: reads are disabled for +import remotes; use the repository remote for clone/fetch`.
  Upstream names no status code: `403` is open.storage's choice. Use the normal remote to
  read. LFS batch requests on the import URL keep working in both directions.
- **An import push writes normal branches and tags.** The push goes to the repository's
  own refs, exactly like a push to `{repo_id}.git`: the same ref policies, quota, `push`
  webhooks and LFS object space. A normal clone shows every branch and tag you pushed,
  and the REST branch list shows every branch.
- **A synced repository refuses an import push.** When the repository has a sync base,
  the pre-receive hook refuses the whole push before it publishes any pack:
  `import push refused: the repository has a sync base; ...`. A push to the normal
  remote still works.
- Upstream moves an imported repository to cold storage at once. open.storage keeps it on
  the normal storage path, which is a functional equivalent.

### Upgrading from 0.20.0 or earlier

Versions up to 0.20.0 kept `+import` pushes in a separate ref space, under the storage key
`<repo_id>+import`, that only the `+import` remote could read. The new version deletes
nothing there, but no git path reads it any more, and only a 0.20.0 server can still
serve it. Copy that data to the normal remote with the current version before every
replica runs the new one:

1. Stop sending pushes to `+import` remotes. On a fleet, an import push that reaches a
   replica that still runs 0.20.0 lands under the old key again.
2. Check the pack quota. The copy stores the objects a second time under the repository's
   own key, and both copies count against the
   [pack quota](../how-to/storage-lifecycle-and-sweeping.md#pack-quotas): the old packs
   keep counting until the repository is deleted. When the repository's stored bytes plus
   the import's size pass `OPENSTORAGE_PACK_QUOTA_BYTES`, raise that limit (or set it to
   `0`) for the copy and for the pushes after it.
3. Copy every ref that you pushed through `+import` (branches, tags, notes and any other
   ref):

   ```sh
   git clone --mirror https://t:<jwt>@{org}.code.storage/{repo_id}+import.git import.git
   git -C import.git push https://t:<jwt>@{org}.code.storage/{repo_id}.git 'refs/*:refs/*'
   ```

   The push refuses a ref that already exists on the normal remote and does not
   fast-forward, such as a branch with other history or a tag with another value. Push
   such a ref under a new name, for example `refs/heads/main:refs/heads/imported/main` or
   `refs/tags/v1:refs/tags/imported/v1`.

   Do not copy into a repository that has a sync base: when the link forwards pushes, a
   copied ref reaches the connected upstream. Create a repository without a sync base and
   copy into that one instead.
4. Upgrade every replica, then send import pushes again. A replica's cache can keep a
   `<repo_id>+import.git` directory that no git request uses: 0.20.0 made it when it
   served the old key, and repack and LFS reclamation jobs for the old key can make it
   again, on any replica that runs them. Check every replica. The cache is rebuildable, so
   deleting that directory is safe.

If a replica already runs the new version and you did not copy the data, the data is
still in storage. Stop every replica of the new version, start 0.20.0 on the same stores,
and follow steps 1 to 4. Do this only when no release after 0.20.0, up to and including
the version you upgraded to, lists a change to the stored formats in its release notes.
Otherwise do not start 0.20.0 on those stores: keep the new version and leave the old
data in place.

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
