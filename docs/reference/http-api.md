# HTTP API Reference (v1, SDK-compatible surface)

The SDK-verified v1 contract, with implementation notes the official spec lacks. Full
request/response schemas, including the parallel `/api/repos/{repo_name}/...` path style,
are in [`../upstream/openapi.yaml`](../upstream/openapi.yaml).

- **Base URL:** `https://api.{org}.code.storage/api/v1`. SDKs build
  `{apiBaseUrl}/api/v{version}` with version fixed at 1.
- **Auth:** every endpoint requires `Authorization: Bearer <jwt>`
  ([auth spec](authentication.md)).
- **`Code-Storage-Agent` header** (informational): `code-storage-sdk/<ver>` (TypeScript),
  `code-storage-py-sdk/<ver>` (Python), `code-storage-go-sdk/<ver>` (Go).

## Conventions

- **Pagination** (all list endpoints): request `cursor` + `limit` query params (grep:
  `pagination: {cursor, limit}` in the body). Response: `next_cursor` (opaque string,
  sent only when `has_more` is true; grep alone sends `""` when exhausted) + `has_more`
  (bool). Documented default limit 20, max 100.
  SDK-observed server defaults differ: files 1000/max 5000, files/metadata 200/max 1000,
  notes refs 20.
- **Errors**: `{"error": "<unstable human string>"}` + status code, with RFC 9457
  problem-details fields alongside `error`: `type` (`about:blank`), `title` (the status
  text), `status`, `code`, `detail` (same string as `error`), and `instance` (the
  escaped request path the client sent, without the query string; for a
  `/api/repos/{repo_name}/...` alias, the alias path). `code` is upstream's
  generic code for the status (`bad_request`, `unauthorized`, `forbidden`, `not_found`,
  `conflict`, `unprocessable_entity`, `rate_limited`, `internal_error`,
  `service_unavailable`, …), except for these specific upstream codes:
  `repository_not_found` (404: the repository does not exist or the token cannot
  access it, including a fork's source repository), `insufficient_scope` (403: the
  token, or a fork's `base_repo.auth.token`, lacks a required scope),
  `repo_name_mismatch` (400: the create body's `repo_name` differs from the token's
  `repo` claim), `branch_not_found` (404), `ref_not_found` (404: a ref, tag, or
  revision does not resolve), `credential_not_found` (404), `credential_exists` (409),
  and `repository_deleted` (409: `DELETE /api/v1/repos/delete` on a deleted repository;
  `DELETE /api/repos/{repo_name}` answers that case with 404 `repository_not_found`,
  because its name lookup skips deleted repositories). open.storage
  emits no other specific codes. As upstream advises, treat
  an unknown code as the generic code for its status. Branch on the status code or
  `code`, not on the message.
  Upstream's 400 bodies can also carry `invalid_params` (`[{name, reason}]`, one item
  per invalid request field). The member is optional upstream, and open.storage never
  sends it: read the message in `detail`.
  The content type stays `application/json`. `status` is a JSON number, as in upstream's
  bodies: decode the body into a struct or a map of arbitrary values, not a string-only
  map (such as Go's `map[string]string`). Ref-moving write
  endpoints return the richer `result` envelope
  ([streaming protocols](../explanation/streaming-commit-protocols.md)), even on 4xx/5xx
  statuses `[400,401,403,404,408,409,412,422,429,499,500,502,503,504]`.
- **Throttling**: every `429` (the gateway [rate limiter](configuration.md#rate-limiting)
  and transient busy/backpressure answers) carries:
  - `Retry-After` (delay-seconds), which takes precedence over the `RateLimit` window;
  - the IETF `RateLimit` service-limit field, `"default";r=0;t=<retry-after-secs>`
    (current draft-ietf-httpapi-ratelimit-headers shape).

  No `RateLimit-Policy` is advertised.
- **Guard SHAs** (`expected_target_sha`, `expected_head_sha`, `expected_notes_ref_sha`,
  `expected_ref_sha`, and the deprecated restore `target_commit_sha`) take a full 40-hex
  SHA in either case.
- **Booleans in query strings** are literal `"true"` / `"false"`.
- **Repeated params**: `path` may repeat on diff endpoints; `range` may repeat on blame.
- **No client retries**: no SDK retries; every error surfaces immediately. Python
  client-side timeouts: 30s default, 60s diff/grep, 180s
  branch-create/merge/merge-preview/commit/tag/restore-commit.

## Endpoint catalog

### Organization metadata (`org:read`)

`GET /api/meta` — absolute path, no `/v1`, and no `/api/v1/meta` alias.

- **Auth:** org-scoped token (`repo: "org"`) with `org:read` for the served organization.
- **Rate limit:** the ordinary pre-auth limiter.
- **Response:** `200` with this exact shape:

```json
{"ips":{"api":[],"git":[]},"provider":{"name":"","region":""}}
```

The arrays are empty because open.storage publishes no ingress CIDRs; provider and
region are empty because it does not determine deployment location. These are the
upstream contract's "unpublished/unavailable" values.

> [!WARNING]
> Empty arrays are **not proof that ingress does not exist, nor a populated firewall
> allowlist**. Get ingress ranges from your deployment operator before configuring a
> firewall.

Implements the upstream contract as documented on September 21, 2026; this says nothing
about the endpoint's rollout status on upstream clusters.

### Repository lifecycle (`repo:write` / `org:read`)

| Method & path | Scope | Notes |
|---|---|---|
| `POST /repos` | repo:write (repo=new id) | Optional body `{repo_name?, default_branch?, upstream?, base_repo?}` (details below). → `{repo_id, repo_name, http_url, message}`. **201** = created; 409 = a live repo holds the name; 404 = a deleted repo holds the name (below). |
| `GET /repos` | org:read (repo="org") | Query `cursor, limit, q` (case-insensitive substring on repo `url`). → `{repos: [{repo_id, repo_name, url (deprecated mirror), default_branch, created_at, upstream?, base_repo?}], next_cursor, has_more}` |
| `GET /repo` | git:read (repo from JWT) | → `{repo_id, repo_name, url (deprecated mirror), default_branch, created_at, upstream?, base_repo?}`; 404 → SDK returns null |
| `PATCH /repo` | repo:write | Update the default branch; body `{default_branch}` (details below). → `{repo_id, id (deprecated mirror), repo_name, url (deprecated mirror), default_branch}` (no `created_at`, per the live spec) |
| `DELETE /repos/delete` | repo:write | Soft delete + async cleanup. → `{repo_name, repo_id (deprecated mirror), message}`; 404 not found; 409 already deleted |
| `GET /api/repo-urls/{repo_id}` | org:read | (No /v1 prefix.) Resolve internal id → `{repo_id, repo_name, url (deprecated mirror)}`; `repo_name` is the JWT `repo` claim value. An open.storage extension: upstream removed the route from its spec on 2026-10-05, and open.storage keeps serving it for existing clients |
| `DELETE /repos/base` | repo:write | Detach upstream provider; idempotent (from official docs/OpenAPI, not in SDKs). Deprecated upstream (2026-10-05) for `DELETE /api/repos/{repo_name}/upstream`, which runs the same handler; still served |
| `DELETE /api/repos/{repo_name}/upstream` | repo:write | Path style only (spec 2026-10-05). Same handler, scope and responses as `DELETE /repos/base` |

**`POST /repos` body:**

- The JWT `repo` claim always names the new repository. The optional `repo_name` (it can
  contain `/`) must equal that claim exactly, with no case folding, trimming, or path
  normalization. A mismatch, including an empty string, returns 400 and creates nothing,
  for forks too. `null` or an omitted `repo_name` changes nothing.
- Query `default_branch` (deprecated upstream) is a fallback, used only when the body omits it.
- `upstream` (Git Sync): `{provider, owner, name, access?, host?}`. It creates a sync link
  and schedules the first sync, like the deprecated `base_repo` sync form. Unknown fields
  → 400.
  - Providers: `github, gitlab, bitbucket, gitea, forgejo, codeberg, sr.ht, sourcehut`.
    Any other value → 400. `gitea` and `forgejo` need `host`.
  - `access` applies to `github` only (any other provider → 400). Only
    `access: "public"` is supported: a public mirror with no stored credential. `app`,
    upstream's default when `access` is omitted, needs a GitHub App installation, which
    open.storage does not have → 400. To sync a private GitHub repository, use the
    deprecated `base_repo` sync form with `auth.token`.
  - `host`: a bare `hostname[:port]`, or (open.storage extension) an `http(s)://` base
    URL like `base_repo.upstream_host`. It defaults to the provider's public host.
  - No credential is stored at create. For a private non-GitHub upstream, add one with
    `POST /repos/git-credentials`, then call `pull-upstream`.
  - `upstream` has no `forward_pushes`; use the deprecated `base_repo` sync form to opt in.
- Setting both `upstream` and `base_repo` → 400 (upstream says not to set both and
  documents no error).
- A sync create (`upstream`, or a `base_repo` sync) with a token whose ref policies carry
  any op → 403, and nothing is stored or scheduled: the first sync updates refs that those
  policies cannot police. A plain create with the same token still works.
- `base_repo` copy (no `provider`, no `operation`): `{name, ref?, auth: {token}}` copies a
  repository on this server once, with no link kept. It runs the fork below: `auth.token`
  needs `git:read` on the source, and `name` must name the repository that token grants
  (otherwise 403). `ref` is a branch, tag, or commit SHA; it defaults to the source's
  default branch. Without a top-level `default_branch`, the copy's default branch takes
  the source's default branch name, as a fork does. Unknown fields, and the sync-only fields
  `owner`, `upstream_host`, `forward_pushes`, and `auth.auth_type`, → 400.
- `base_repo` (deprecated sync and fork form): `{provider, owner, name, operation?: "fork", auth?: {token}|{auth_type:"public"}, ref?, sha?, default_branch?, upstream_host?, forward_pushes?}`.
- Providers: `code, github, gitlab, bitbucket, gitea, forgejo, codeberg, sr.ht`, plus
  generic with `upstream_host`.
- `forward_pushes: true` (default false; open.storage extension) schedules a push-upstream
  run after every won local push. It also inverts the sync conflict rule: local wins, and
  inbound syncs never force-rewind local refs. See [Upstream sync](#upstream-sync).

**Repeat creates** (upstream spec 2026-10-07; open.storage covers part of it, see [#388](https://github.com/coder/openstorage/issues/388)):

- A new repository → **201** `Repository '<name>' created`.
- A live repo already holds the name → **409** `repository already exists`, for every create
  form (plain, `upstream` sync, `base_repo` copy or fork), even for an identical request.
  Upstream answers 200 unless the upstream differs. open.storage cannot yet tell a finished
  repo from a create that is still in progress, so a 200 could name a repo that the failing
  create then deletes. [#388](https://github.com/coder/openstorage/issues/388) tracks the fix.
- A deleted repo keeps its name → **404**. Nothing is restored.

**`PATCH /repo` rules:**

- `default_branch` is required and non-empty: a plain branch name (qualified to
  `refs/heads/<name>` at hydrate time) or its `refs/heads/`-qualified spelling (stored
  verbatim).
- Other ref namespaces (tags/notes) → 400; the default names a branch.
- The call never creates the branch. It may be unborn; clones then see a dangling `HEAD`,
  like a fresh repo.
- CAS against the previously read value → 409 on a concurrent change.
- `no-push` ref policies covering `HEAD` or the target → 403.
- Clone `HEAD` follows on each node's next cache hydrate.

### Git credentials (generic HTTPS upstreams)

| Method & path | Scope | Body → Response |
|---|---|---|
| `POST /repos/git-credentials` | repo:write (repo=repoId) | `{repo_id, password, username?}` → `{id}`; 409 = exists |
| `PUT /repos/git-credentials` | repo:write (repo="org") | `{id, password, username?}` → `{id, created_at}`; 404 |
| `DELETE /repos/git-credentials` | repo:write (repo="org") | `{id}` → 200/204; 404 |

`username` is optional on create and update, on these routes and on the path-style
`/repos/{repo_name}/git-credentials` routes. Only `password` is required. The server
stores the username exactly as sent. An update that omits it clears the stored one.

A stored username is always sent as is. Git sends no credentials when the username is
empty, so for a credential with no username the sync worker sends the upstream's
per-provider default from upstream's spec of 2026-10-08:

| Upstream provider | Sent when no username is stored |
|---|---|
| `bitbucket` | Username `x-token-auth`, the token as the password |
| `gitlab` | Username `oauth2`, the token as the password |
| `gitea`, `forgejo`, `codeberg` | The token as the username, with an empty password (git cannot send a username alone) |
| `github` and every other provider | Username `git`, the token as the password |

The spec also notes that sr.ht takes no credential over HTTPS. A credential stored for
an sr.ht upstream still gets the `git` default.

| Host | What to send |
|---|---|
| Gitea, Forgejo, Codeberg | The token as `password`. The username can be omitted. |
| GitLab personal, project, group or OAuth token | The token as `password`. The username can be omitted when the provider is `gitlab`. Otherwise send `oauth2`. |
| GitLab deploy token | The deploy token's username. CI job tokens use `gitlab-ci-token`. |
| Bitbucket access token | The token as `password`. The username can be omitted when the provider is `bitbucket`. Otherwise send `x-token-auth`. |
| Bitbucket app password or API token | Your Bitbucket username. |
| Any other host | The username that host expects. |

The worker hands the pair to git through a credential helper that reads it from the
environment. A stored username or password that contains a carriage return, line feed
or NUL fails the sync as a configuration error.

### Files & trees (`git:read`)

| Method & path | Query/body | Response |
|---|---|---|
| `GET /repos/file` | `path` (req), `ref`, `ephemeral`, `ephemeral_base`; headers `Range`, `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, `If-Range` | Raw blob stream. Status 200/206/304/412/416. Headers: `X-Blob-Sha`, `X-Last-Commit-Sha`, `ETag`, `Last-Modified`, `Accept-Ranges`, `Content-Range`, `Content-Type`, `Content-Length` |
| `HEAD /repos/file` | same | headers only |
| `GET /repos/files` | `ref, ephemeral, path, recursive, cursor, limit` | `{paths: [..], ref, entries: [{path, type: blob\|tree\|symlink\|submodule, mode}], next_cursor, has_more}` — trees have no trailing `/`; branch on `type` |
| `GET /repos/files/metadata` | same | `{files: [{path, mode, size, last_commit_sha, type}], commits: {<sha>: {author, date, message, order}}, ref, next_cursor, has_more}`. `order` is described below the table. **404 on unborn branch** |
| `POST /repos/archive` | body `{ref?, include_globs?, exclude_globs?, max_blob_size?, archive?: {prefix}}` | tar.gz stream |
| `POST /repos/grep` | body `{query: {pattern, case_sensitive?}, ref?, ephemeral?, paths?, file_filters?: {include_globs, exclude_globs, extension_filters}, context?: {before, after}, limits?: {max_lines, max_matches_per_file}, pagination?: {cursor, limit}}` | `{query, repo: {ref, commit}, matches: [{path, lines: [{line_number, text, type: "match"\|context}]}], next_cursor, has_more}` |
| `GET /repos/blame` | `path` (req), `ref`, `ephemeral`, `range` (repeatable, `git blame -L` specs like `1,10`), `detect_moves` | `{ref, path, commit_sha, lines: [{line_number, commit_sha, original_line_number, original_path, previous_commit_sha?, author_name, author_email, author_time, committer_name, committer_email, committer_time, summary}]}` |

`order` in `files/metadata` is the commit's history-order rank relative to the resolved ref: its 0-based position in `git rev-list --topo-order <resolved commit>`. The ref's own commit is `0`, and larger values are older. A commit always ranks higher than every one of its descendants, whatever its author or committer date. Among a folder's files, the one with the smallest `order` was changed most recently in the ref's history.

`order` is an absolute position in the ref's history, not a rank within the response. Values can have gaps, and a response contains `0` only when the ref's own commit is the last commit of a listed file. Ranks agree across pages when the requests resolve to the same commit. A branch can move between requests, so pin `ref` to a commit SHA to page through one snapshot.

Ties between commits that ancestry does not order follow git's topo-order walk, which ignores dates. A commit becomes eligible once all of its children reachable from the ref are ranked, and the most recently eligible commit is ranked next. When ranking a merge makes several of its parents eligible at once, they become eligible in parent order, so the line of history behind the last of them is ranked first. The same history always yields the same ranks.

Each page runs one `git rev-list --topo-order` walk for `order`, as one more git subprocess of the request, under the [resource limits on git reads](#resource-limits-on-git-reads). If a limit is hit, the request fails with that section's status: 503 with `Retry-After` when busy, 503 when out of time, 422 for memory. A 200 response always carries the exact `order` of every commit; the field is never omitted or estimated. With a commit-graph ([ADR-47](../adr/047-cache-repos-get-split-commit-graphs-written-by-full-hydrates.md)), git can print ranks as it goes, and the walk is stopped as soon as every listed commit is ranked. Without one, git reads the whole history of the ref before it prints anything: about 70–90 ms for 20,000 commits in `BenchmarkTopoRanks`, and about 80 MB of git heap at 300,000 commits and 124 MB at 700,000 (measured on Linux), below the 256 MiB limit.

### Branches

| Method & path | Scope | Body/query → Response |
|---|---|---|
| `GET /repos/branches` | git:read | `cursor, limit, ephemeral` → `{branches: [{cursor, name, head_sha, created_at}], next_cursor, has_more}` |
| `GET /repos/branch` | git:read | `name` (req), `ephemeral?` → `{branch: {name, head_sha, created_at}}` — one branch by exact short name (no pagination fields); 404 when absent |
| `POST /repos/branches/create` | git:write | `{target_branch?, base_ref?\|base_branch?(deprecated), preferred_base?, base_is_ephemeral?, target_is_ephemeral?, force?}` → `{message, target_branch, target_is_ephemeral, commit_sha?}` |
| `DELETE /repos/branches` | git:write | Query `target_branch`, `ephemeral?`. The deprecated body `{target_branch (deprecated alias name), ephemeral?}` still works; a query parameter that is present wins over its body field, even when empty → `{target_branch, name (deprecated mirror), message, ephemeral}`. Refuses default branch (persistent): 409 for the query form, 403 for the body form |
| `GET /repos/branches/diff` | git:read | **Deprecated upstream** (both path styles); still implemented. Use `GET /repos/diff` ([migration](#migrating-from-branchesdiff)). `branch` (req), `base?` (defaults to the repo default branch), `ephemeral?`, `ephemeral_base?`, `path` (repeatable) → `{branch, base, merge_base_sha, stats: {files, additions, deletions, changes}, files: [FileDiff], filtered_files}`; `merge_base_sha` is empty when the histories are unrelated (direct `base..branch` diff) |
| `POST /repos/merge` | git:write | `{source_ref (bare branch name or refs/heads form; deprecated alias source_branch), target_branch, strategy: merge\|ff_only\|ff_prefer, source_is_ephemeral?, target_is_ephemeral?, expected_target_sha?, commit_message?, author?, committer?, allow_unrelated_histories?, squash?}` → `{result: merge_commit\|fast_forward\|no_op\|squash, commit_sha, tree_sha, source: {ref, branch (deprecated mirror), ephemeral, sha}, target: {branch, ephemeral, old_sha, new_sha}, merge_base_sha?, promoted_commits}`. Conflict → 409 (+`conflict_paths`, `merge_base_sha`) |
| `GET /repos/merge/preview` | git:read | `source_branch, target_branch, source_is_ephemeral?, target_is_ephemeral?` (deprecated aliases `source_ephemeral`/`target_ephemeral`), `include_content?` → `{status: clean\|conflicted, result, source_branch, target_branch, source_tip_sha, target_tip_sha, merge_base_sha?, conflict_paths, conflicts: [{path, result\|base\|ours\|theirs: {oid?, content?, truncated, binary}}], filtered_conflicts: [{path, reason}]}`. Read-only |

#### Migrating from `branches/diff`

Upstream deprecated `GET /api/v1/repos/branches/diff` and
`GET /api/repos/{repo_name}/branches/diff` in favor of `GET /repos/diff` (same path
styles). open.storage keeps serving `branches/diff`. To migrate, rename the parameters:

| `branches/diff` | `diff` |
|---|---|
| `branch` | `ref` |
| `base` | `base_ref` |
| `ephemeral` | `ref_is_ephemeral` |
| `ephemeral_base` | `base_is_ephemeral` |
| `path` | `path` |

With `base_ref` set, `diff` returns the same three-dot diff (`merge_base_sha`, `stats`,
`files`, `filtered_files`) as `branches/diff`. The envelope differs: `sha` and `base_sha`
(resolved commits) replace `branch` and `base` (the names you sent).

**Default base:** `branches/diff` without `base` diffs against the repo default branch.
`diff` without `base_ref` returns the diff of the one commit `ref` points to. To keep the
old behavior, read `default_branch` from `GET /repo` and send it as `base_ref`.

**Server-allocated branch names** (`POST /repos/branches/create`, both path styles):

- Omit `target_branch` to allocate a new opaque name in the requested target namespace.
  Use the returned `target_branch`, `commit_sha`, and `target_is_ephemeral`; do not
  depend on a name format.
- Each successful call creates a new branch, so **retrying after an unknown result can
  create another branch**. Keep the returned name and delete branches you no longer need.
- `force: true` requires an explicit, valid target name (otherwise `400`). Named-target
  force and ref-policy checks are unchanged.
- In open.storage, explicit empty or `null` targets are `400`, not allocation requests.
- Allocation requests reject unknown fields with `400`, so a misspelled target field
  cannot silently allocate a branch. Named-target requests keep their unknown-field
  tolerance.
- Name collisions retry with create-only CAS and never overwrite an existing ref.
  Exhausting the bounded attempts returns `409`. Policy denials and storage errors are
  not retried.

This follows the public OpenAPI/SDK contract, not demonstrated upstream cluster behavior
([contract audit](../explanation/wire-compatibility.md#server-allocated-branch-contract)).

`FileDiff` = `{path, state (raw letter; SDKs normalize A/M/D/R/C/T/U), old_path?, raw
(unified diff text), bytes, is_eof, additions, deletions}`. `filtered_files` entries omit
`raw`/counts; large files are filtered unless requested via `path`.

### Commits

| Method & path | Scope | Notes |
|---|---|---|
| `GET /repos/commits` | git:read | `ref?` (deprecated alias `branch`), `cursor?, limit?, ephemeral?, path?` → `{commits: [CommitInfo], next_cursor, has_more}`, newest first; defaults to default branch |
| `GET /repos/commit` | git:read | `ref` (req; deprecated alias `sha`; branch name, tag, short or full SHA) → `{commit: CommitInfo & {signature?, payload?}}` (signed commits: armored sig + exact signed payload bytes) |
| `GET /repos/diff` | git:read | `ref` (req; deprecated alias `sha`), `base_ref?` (deprecated alias `baseSha`), `ref_is_ephemeral?`, `base_is_ephemeral?`, `git_apply_compatible?` (deprecated alias `gitApplyCompatible`), `path` (repeatable) → `{sha, base_sha, merge_base_sha, stats, files: [FileDiff], filtered_files}`. `base_sha` is the commit `base_ref` resolved to, empty for a single-commit diff. On alias conflict the current name wins |
| `POST /repos/commit-pack` | git:write | NDJSON stream — see the [streaming protocols](../explanation/streaming-commit-protocols.md) |
| `POST /repos/diff-commit` | git:write | NDJSON stream — see the [streaming protocols](../explanation/streaming-commit-protocols.md) |
| `POST /repos/restore-commit` | git:write | JSON `{metadata: {target_branch, base_ref (branch, tag, or commit; deprecated alias target_commit_sha, 40-hex only), author, commit_message?, expected_target_sha? (deprecated alias expected_head_sha), committer?}}` → commit ack ([streaming protocols](../explanation/streaming-commit-protocols.md)). Creates a *new* commit restoring old tree (history preserved). A `refs/...` `base_ref` resolves verbatim. Any other name tries a branch, then a tag (annotated tags peel). Then `base_ref` resolves as a full or abbreviated SHA (a full SHA may be uppercase). It always resolves outside the ephemeral namespace. Extensions: `target_is_ephemeral?` (deprecated alias `ephemeral`) restores a branch in the ephemeral namespace. `base_is_ephemeral` (deprecated alias `ephemeral_base`) is accepted and ignored |
| `POST /repos/reset-commits` | git:write | In OpenAPI only (not in SDKs): NDJSON metadata stream; same restore-commit family |

`CommitInfo` = `{sha, parent_shas[], message, author_name, author_email, committer_name,
committer_email, date (RFC3339), committer_date (RFC3339)}`. `date` is the author
timestamp and `committer_date` the committer timestamp, as in upstream's spec of
2026-10-08. A rebase or cherry-pick changes only `committer_date`. Before that spec,
`date` carried the committer timestamp.

### Tags

| Method & path | Scope | Body/query |
|---|---|---|
| `GET /repos/tags` | git:read | `cursor, limit` → `{tags: [{cursor, name, sha}], next_cursor, has_more}` — lexical order; `sha` dereferenced to commit |
| `GET /repos/tag` | git:read | `name` (req) → `{tag: {name, sha}}` — one tag by exact short name, `sha` dereferenced to commit; 404 when absent |
| `POST /repos/tags` | git:write | `{name, ref (deprecated alias target)}` → `{name, sha, message}` — lightweight only; name must not start with `refs/` |
| `DELETE /repos/tags` | git:read+git:write | `{name}` → `{name, message}` — resolves stored ref for lightweight & annotated |

### Notes

| Method & path | Scope | Body/query |
|---|---|---|
| `GET /repos/notes` | git:read | `object_ref` (req; deprecated alias `sha`), `notes_ref?` (deprecated alias `ref`) → `{sha, note, ref_sha}` |
| `POST /repos/notes` | git:write | `{object_ref (deprecated alias sha), action: "add"\|"append" (deprecated upstream), note, notes_ref? (deprecated alias ref), expected_notes_ref_sha? (deprecated alias expected_ref_sha), author?}` → `{sha, notes_ref, target_ref (deprecated mirror), base_commit?, new_ref_sha, result: {success, status, message?}}`. `add` fails if note exists; `append` creates or appends |
| `POST /api/repos/{repo_name}/notes/append` | git:write | Path style only (spec 2026-10-05). `{object_ref (deprecated alias sha), note, notes_ref? (deprecated alias ref), expected_notes_ref_sha? (deprecated alias expected_ref_sha), author?}`, decoded strictly: `action`, any unknown field, or a `null` → 400. Appends, or creates the note when the object has none → **201** with the same envelope. A stale guard → 409 `conflict` and nothing is appended |
| `DELETE /repos/notes` | git:write | Query `object_ref`, `notes_ref?`, `expected_notes_ref_sha?`. The deprecated body `{object_ref (deprecated alias sha), notes_ref?, expected_notes_ref_sha?, author?}` still works; a query parameter that is present wins over its body field, even when empty → same envelope |
| `GET /repos/notes/refs` | git:read | `prefix?` (default `refs/notes/`), `cursor, limit` → `{refs: [{cursor, ref, sha}], prefix, next_cursor, has_more}`. 400 if custom notes refs disabled server-side |

- Default notes ref: `refs/notes/commits`. Bare names resolve to `refs/notes/<name>`.
- `object_ref`: a full object SHA (commit, tree, blob, or annotated tag), or a branch,
  tag, short SHA, or other revision that resolves to a commit. Unresolvable → 404.
- Responses echo the resolved object id in `sha`.
- A stale `expected_notes_ref_sha` → 412 `precondition_failed`, except `POST .../notes/append`
  and the `DELETE` query parameter form, which answer 409 `conflict` as upstream documents
  (the SDKs send the body forms and read `result.status`).

### Upstream sync

| Method & path | Scope | Notes |
|---|---|---|
| `POST /repos/pull-upstream` | git:write | Optional body `{ref?, after?}` → **202 Accepted** (async). Sync-linked repos (`upstream` or `base_repo` sync) only. Deprecated upstream (2026-10-05) for `POST /api/repos/{repo_name}/upstream/pull`; still served. Both body fields are accepted and ignored: every pull syncs all branches and tags of the upstream. Upstream deprecated the body on 2026-10-08 |
| `POST /api/repos/{repo_name}/upstream/pull` | git:write | Path style only (spec 2026-10-05). Same handler, scope, ref-policy check and responses as `POST /repos/pull-upstream`. The optional, deprecated body `{ref?, after?}` is decoded strictly: an unknown field or a `null` → 400. Its fields are ignored |
| `POST /repos/push-upstream` | git:write + git:read | `{force?}` → **202 Accepted** (async). Sync-linked repos only. open.storage extension |

**Both triggers:**

- **A 202 is never dropped.** A request that arrives during an in-flight sync run
  coalesces into one follow-up run, so some run observes upstream as of the request or
  later.
- Egress is policy-guarded at dial time; see [Egress policy](#egress-policy) below.

**`push-upstream`** (upstream forwards implicitly; see
[wire compatibility](../explanation/wire-compatibility.md)):

- Publishes local `refs/heads/*` and `refs/tags/*` to the upstream with the stored
  credential. Ref deletions are not propagated.
- Requires **both** scopes: the run exports every local head and tag, so a write-only
  token must not publish content it cannot clone.
- Non-force by default. A ref the upstream refuses (non-fast-forward) fails the run, with
  per-ref reasons in the `repo.push_upstream.failed` event's `rejected_refs`. `force: true` opts
  into `+refs`, and survives coalescing (a forced request is never downgraded).
- The push is atomic: one refused ref fails the whole batch and leaves the upstream
  untouched.
- An unreachable upstream retries with exponential backoff (1–16 min, 5 attempts).
  Auth, config, and rejection failures do not retry.
- A successful run schedules one follow-up inbound sync, so a pull that raced the forward
  cannot strand a stale local snapshot.
- Private/special destinations are denied (permanent `invalid sync configuration`
  failure) unless
  [`OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE`](configuration.md#openstorage_sync_push_allow_private)
  opts internal forges in.

**Events and cancellation:** outbound runs emit `repo.push_upstream.*` events
([webhooks](webhooks.md)); inbound runs emit `repo.sync.*`. `DELETE /repos/base` silently cancels
in-flight runs in both directions. With a SQLite or PostgreSQL database, a run that
server shutdown cancels also ends without a terminal event: its request stays pending,
and a later run (the next server process, or on PostgreSQL another replica) runs it with
a new `repo.sync.started` (outbound: `repo.push_upstream.started`).

**Ref policies fail closed:** any policy op denies pull-upstream, push-upstream, and a
sync create at `POST /repos` (403).
The same gate covers the sync that a ref write schedules. A REST write or git push from
a token that carries any ref-policy op does not trigger the inbound sync, since the sync
force-updates every head and tag from the upstream, not just the refs that token may
touch. For `forward_pushes` repos, such a write, or one from a token that lacks
git:read, does not trigger the forward, since the forward publishes every head and tag.
The write stays local. Nothing polls the upstream, so the repository converges only on
the next authorized trigger: a write, pull-upstream, or push-upstream that passes these
gates.

#### Egress policy

Per ADR-45, `upstream_host` is caller-linked data. Every http(s) sync transfer in either
direction dials through a vetting proxy that applies the webhook delivery destination
policy. That covers pull-upstream fetches (including the first sync at `POST /repos` and
freshness pokes) and push-upstream forwards.

- Governed by
  [`OPENSTORAGE_SYNC_ALLOW_PRIVATE`](configuration.md#openstorage_sync_allow_private).
  Transitional default: pulls allow private upstreams, pushes deny.
- A denied pull destination fails the run as `repository configuration error`; a denied
  push destination as `invalid sync configuration`.
- `POST /repos` with `upstream` or a `base_repo` sync also rejects, with 400, an
  `upstream.host` or `upstream_host` the pull policy denies. This registration check is a courtesy; dial time is authoritative.

> [!CAUTION]
> **Mixed-fleet prerequisite:** finish the rolling upgrade of every replica to a build
> with this feature **before creating repositories with `forward_pushes: true`**. The
> forward reroute and local-wins pull rule are replica-local code. A pre-feature replica
> that accepts a push on such a repository still schedules the legacy inbound
> force-mirror ("upstream wins"), which can rewind the just-accepted write, and schedules
> no forward. Repositories without the flag are unaffected: old and new replicas agree on
> pull-only behavior. This ordering is documentation-enforced, like the other
> [upgrade orderings](configuration.md).

## Resource limits on git reads

The `git:read` endpoints answer from git subprocesses over a cache repository. Anyone who can push shapes that repository, so every git subprocess that reads repository data for these endpoints runs under the limits below ([#313](https://github.com/coder/openstorage/issues/313)). Each limit takes effect before the work it bounds.

| Limit | Value | When it is exceeded |
|---|---|---|
| Concurrency | At most 8 requests per server process run git at once, and `file` and `archive` downloads have a separate pool of 8. One repository can hold at most 2 slots of each pool, whether its tokens name it by URL or by ID. Cache maintenance (commit-graph refreshes) can hold up to 2 of the 8 JSON slots at a time, counted in the refreshed repository's share, so JSON reads always keep at least 6. A request takes its slot at its first git subprocess, including single-object reads, and keeps it until the server has finished writing its response to the network connection. A request waits up to 1 second for a slot. | 503 with `Retry-After: 1`. git is not started. |
| Time | Every git subprocess of a request must finish within 30 seconds of the request's first one. This is wall-clock time, so the server's own work between subprocesses counts too. `archive` gets 10 minutes, because it streams at the client's pace between subprocesses. | 503 without `Retry-After`. The running git subprocess is stopped. |
| Memory (Linux) | 256 MiB of git heap per subprocess (`RLIMIT_DATA`, set before git starts); 32 MiB for single-object reads. Read-only pack mappings are shared page cache and are not counted. | 422. A git crash by `SIGSEGV`, `SIGBUS` or `SIGABRT` while running under the cap is also reported as this limit. |
| Output | 64 MiB of buffered git output per request, shared by all its subprocesses. File contents that `file`, `archive` and `diff` stream instead of buffering are not counted. | 422. git is stopped as soon as the limit is passed. |
| File size | 100 MiB per file, checked from the tree before the file is read. | 422 for `file` (GET and HEAD) and `archive`, before any response byte. |
| Delivery | Each 64 KiB of a response must reach the client within 30 seconds, and the whole response within 2 minutes of its first byte (`file` and `archive`: 10 minutes). | The connection is closed. The request's slot, memory and temporary files are released. |

The largest git heap measured on a 300,000-commit linear history is 184 MB, for `git log -1 -- <file added in the root commit>`, so such histories are served. Histories past roughly 400,000 commits can get 422 for per-file history walks and `blame` of old files. Deep `commits` pages need less memory: they still fit at 1,000,000 commits (at most 232 MiB) and are estimated to reach the cap at roughly 1.1 million. A `files/metadata` page whose per-file walks and `order` walk need more than 30 seconds in total gets 503; request smaller pages with `limit`. Commit-graphs, which full hydrates now write ([ADR-47](../adr/047-cache-repos-get-split-commit-graphs-written-by-full-hydrates.md)), make these walks cheaper when present, but the limits do not depend on them: the measurements above are without a commit-graph.

| Endpoint | git work | Cost grows with |
|---|---|---|
| all below | ref resolution (`check-ref-format`, `rev-parse`) | nothing (single object) |
| `commits` | `git log --skip=<cursor offset> -n <limit+1> [-- path]` | cursor offset; history searched for `path` |
| `commit` | `git log -n 1`, `git cat-file commit` | the commit object (single object) |
| `files` | `git ls-tree [-r]` | tree size |
| `files/metadata` | `git ls-tree -r`, then one `git log -1 -- <path>` per listed file, then one `git rev-list --topo-order` for `order` | tree size; page size × history depth; history depth |
| `file` | `git ls-tree`, `git log -1 -- <path>`, `git cat-file blob` | history depth; file size |
| `diff`, `branches/diff` | `git merge-base`, three `git diff` runs, `git ls-tree` and `git cat-file blob` per changed file | history to the merge base; number and size of changed files |
| `blame` | `git blame` | file length × history depth |
| `grep` | `git grep` (one thread) | tree size; number of matches |
| `archive` | `git ls-tree -r`, `git cat-file blob` per file | tree size |
| `tags`, `tag` | `git for-each-ref refs/tags` | number of tags |
| `notes` | `git notes show` | notes tree and note size |
| `merge/preview` | `git merge-base`, `git merge-tree`, `git ls-tree` and `git cat-file` per conflict | history to the merge base; tree size |

Other limits and gaps:

- **Platforms.** The memory limit needs `RLIMIT_DATA`, which only Linux enforces. On other server platforms, such as macOS, git runs without a memory cap, and the concurrency, time, output and file-size limits still apply. No endpoint refuses a request because the memory cap is unavailable. For deployments where untrusted users can push, run the server on Linux. See [ADR-49](../adr/049-read-side-git-children-run-under-one-per-request-budget.md).
- `archive` streams its response. An error after the response has started (for example, the time limit, or a delta-compressed file whose base object does not fit in memory) closes the connection, so the client sees a truncated, invalid gzip stream instead of a status code.
- Not covered by these limits: hydrating the cache repository before the git reads (serialized per repository), git clone and fetch over the git protocol (`upload-pack`, which has its own request-size limits), and the write endpoints.

## Status-code contract (what clients key on)

| Status | Meaning to clients |
|---|---|
| 200/201 | success (201 accepted by SDKs on create endpoints) |
| 202 | pull-upstream / push-upstream accepted |
| 206/304/412/416 | file conditional/range results (passed through) |
| 400 | invalid input; also "custom notes refs not enabled" |
| 401 / 403 | auth failed / permission or ref-policy denied |
| 404 | repo/object/branch not found; unborn-branch file metadata |
| 409 | repo exists, repo already deleted, credential exists, merge conflict, CAS conflict |
| 412 | CAS precondition failed |
| 422 | a git read exceeded a memory, output or file-size limit ([resource limits on git reads](#resource-limits-on-git-reads)); retrying the same request fails the same way |
| 429 | rate limit ("unavailable" to ref-update clients) |
| 5xx | internal/unavailable; 503 documented for cold-storage warmup on grep/archive, and for git reads when the server's git concurrency is exhausted (with `Retry-After`) or a request exceeds its git time budget (without `Retry-After`) ([resource limits on git reads](#resource-limits-on-git-reads)) |
