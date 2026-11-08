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
  `files` and `files/metadata` differ (spec 2026-10-27): with no `cursor` and no `limit`
  (`limit=0` counts as no `limit`), they return every entry with `has_more` false. With
  a `cursor` and no `limit`, they return pages of 1000 (`files`, max 5000) and 200
  (`files/metadata`, max 1000). Notes refs default to 20.
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
  `repository_deleted` (409: `DELETE /api/v1/repos/delete` on a deleted repository;
  `DELETE /api/repos/{repo_name}` answers that case with 404 `repository_not_found`,
  because its name lookup skips deleted repositories), `ref_guard_failed` (409: a
  failed ref guard, with `guard` and `current_sha`: on a merge, [details](#merge-guards),
  on a commit write, [details](#commit-write-guards), and on a notes write,
  [details](#notes)), `authentication_unavailable` (403: the key store failed while
  verifying the token; retry), and `route_not_found` (404: no API route matches the
  path; a path that has routes for other methods answers 405 `method_not_allowed` with
  `Allow`). open.storage emits no other specific codes. It does not send
  `repo_claim_missing`: a token without a `repo` claim is org-wide, so a per-repo route
  refuses it with 403 `forbidden`, as it refuses `repo: "org"`
  ([authentication](authentication.md)). As upstream advises, treat an unknown code as
  the generic code for its status. Branch on the status code or `code`, not on the
  message.
  A `400` for a request body can also carry `invalid_params` (`[{name, reason}]`, as
  upstream's `ValidationProblemDetails`): `name` is the dotted wire path of the invalid
  field, and `reason` is the rule its value breaks. open.storage sends it when a body
  field has the wrong JSON type (for example `{"name":"default_branch","reason":"must be
  a string"}`; nested fields such as `base_repo.auth.token` and array elements such as
  `paths.0` too), when a strictly decoded body has an unknown, duplicate, or `null`
  member, or a member that must be an object, and when a body field fails a check of
  its own: a required field, an allowed value, a range, or a name format (for example
  `{"name":"limits.max_lines","reason":"must be between 1 and 2000"}`). It leaves the
  member out when it cannot name the fields: an unreadable or empty body, a JSON syntax
  error, a body that is not an object, an empty member name, unknown fields on the
  other bodies, rules across fields (such as `base_repo` and `upstream` together), and
  query parameters. The message in `detail` does not change.
  These bodies have `Content-Type: application/problem+json`, the type upstream documents
  for problem details, whatever the request's `Accept` header says. Before
  [#352](https://github.com/coder/openstorage/issues/352) item 5 they were
  `application/json`: SDK `1.17.1` Go and TypeScript clients parse error JSON only for
  `application/json`, so they now show the raw body as the error message. SDK `0.1.0`
  (Go, Python, TypeScript) reads the body whatever the type. Result envelopes and success
  bodies stay `application/json`. `status` is a JSON number, as in upstream's
  bodies: decode the body into a struct or a map of arbitrary values, not a string-only
  map (such as Go's `map[string]string`). Every failure on every route uses the error
  body above, except where upstream documents another body
  ([#352](https://github.com/coder/openstorage/issues/352) item 3):
  `commit-pack`, `diff-commit` and `restore-commit` return the richer `result` envelope
  on the statuses that the
  [streaming protocols](../explanation/streaming-commit-protocols.md) list, a merge
  `409` for file conflicts returns upstream's `MergeConflict`
  ([merge](#branches)), and the `409` for a stale guard on a commit write or a notes
  write keeps its `result` envelope and adds the problem members
  ([commit writes](#commit-write-guards), [notes](#notes)). The commit-write `result` is
  upstream's `RefUpdateResult`: it has every member that the schema requires, with empty
  strings where there is no value: `target_branch`, the deprecated `branch` (the same
  value), `old_sha`, `new_sha`, `success` and `status`. A failure envelope sends
  `commit: null`, except a lost CAS on `commit-pack` and `diff-commit`. Before
  [#665](https://github.com/coder/openstorage/issues/665), `result` had no
  `target_branch`, most failures left out empty members, and failure envelopes sent no
  `commit`. A merge `409` for a failed guard (a stale `expected_target_sha`, or a source
  branch that no longer contains
  `expected_source_sha`) uses the error body above with code `ref_guard_failed`
  ([details](#merge-guards)).
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
- **Paths are literal**: a `path` param names a file or directory. Wildcards
  (`*`, `?`, `[`) and git pathspec magic (`:(...)`) match only a path of that name.
- **Path encoding**: a git tree entry name can hold any byte except NUL and `/`, but
  JSON strings hold Unicode text. A response path that is not valid UTF-8 comes back
  twice. The path field holds one U+FFFD per invalid byte, and a sibling field
  `<field>_base64` holds the raw bytes as standard base64 (RFC 4648 section 4, with
  padding). The sibling is absent when the path is valid UTF-8, so a repository with
  only valid names gets responses with no new key. Read the sibling, not the listed
  path: two names can list as the same path (`a<0xff>b` and `a<0xfe>b` both list as
  `a\ufffdb`), and the listed U+FFFD form can also be a different file whose valid
  UTF-8 name holds a real U+FFFD (`a\ufffdb`, which has no sibling). A list field
  gets a list sibling of the same length and order, present when any element is not
  valid UTF-8, and it holds the base64 of every element. The siblings are:
  - `files`: `entries[].path_base64`, and `paths_base64` next to `paths`. Both
    `paths` and `paths_base64` hold only the blob entries.
  - `files/metadata`: `files[].path_base64`.
  - `diff` and `branches/diff`: `path_base64` and `old_path_base64` in `files[]` and
    `filtered_files[]`.
  - `blame`: `path_base64`, and `original_path_base64` in `lines[]`.
  - `grep`: `path_base64` in `matches[]`.

  The merge conflict paths (`conflict_paths` in the 409 body and the merge preview
  fields) still list U+FFFD only. To read a file by the name in a sibling, send its
  raw bytes percent-encoded in the `path` query parameter, for example
  `file?path=a%FFb`. `file`, `files`, `files/metadata`, `commits`, `diff`,
  `branches/diff` and `blame` read `path` byte for byte. An `archive` (tar member
  names) or a git clone gives the raw names. A JSON request body cannot hold such a
  name yet: grep `paths` and commit-pack file paths cannot name the file, and the
  archive and grep globs match it only through a wildcard (`a?b`, `a*`), which can
  match other names too. Upstream documents no path encoding, so the siblings are an
  extension ([#587](https://github.com/coder/openstorage/issues/587)).
- **No client retries**: no SDK retries; every error surfaces immediately. Python
  client-side timeouts: 30s default, 60s diff/grep, 180s
  branch-create/merge/merge-preview/commit/tag/restore-commit.

## Endpoint catalog

### Organization metadata (`org:read`)

`GET /api/meta` — absolute path, no `/v1`, and no `/api/v1/meta` alias.

- **Auth:** org-scoped token (`repo: "org"`, or no `repo` claim) with `org:read` for the
  served organization.
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
| `POST /repos` | repo:write (repo=new id) | Optional body `{repo_name?, default_branch?, upstream?, base_repo?}` (details below). → `{repo_id, repo_name, http_url, message}`. **201** = created; 200 = the name holds a finished repo the request matches; 409 = another live repo holds the name; 404 = a deleted repo holds the name; 503 = a create of the name is not finished (below). |
| `GET /repos` | org:read (repo="org" or no repo) | Query `cursor, limit, q, prefix`. `q` is a case-insensitive substring of repo `url`, and `prefix` a case-insensitive start of it, both trimmed (empty = no filter). With `prefix` set, `q` matches only the rest of `url` after the prefix: `prefix=pierre/&q=sdk` returns `pierre/sdk-documentation`, not `pierre/example`. → `{repos: [{repo_id, repo_name, url (deprecated mirror), default_branch, created_at, upstream?, base_repo?}], next_cursor, has_more}` |
| `GET /repo` | git:read (repo from JWT) | → `{repo_id, repo_name, url (deprecated mirror), default_branch, created_at, upstream?, base_repo?}`; 404 → SDK returns null |
| `PATCH /repo` | repo:write | Update the default branch; body `{default_branch}` (details below). → `{repo_id, id (deprecated mirror), repo_name, url (deprecated mirror), default_branch}` (no `created_at`, per the live spec) |
| `DELETE /repos/delete` | repo:write | Soft delete + async cleanup. → `{repo_name, repo_id (deprecated mirror), message}`; 404 not found; 409 already deleted |
| `GET /api/repo-urls/{repo_id}` | org:read (repo="org" or no repo) | (No /v1 prefix.) Resolve internal id → `{repo_id, repo_name, url (deprecated mirror)}`; `repo_name` is the JWT `repo` claim value. An open.storage extension: upstream removed the route from its spec on 2026-10-05, and open.storage keeps serving it for existing clients |
| `DELETE /repos/base` | repo:write | Detach upstream provider; idempotent (from official docs/OpenAPI, not in SDKs). → 200 `{message: "repository detached"}`, also when the repo has no upstream. Deprecated upstream (2026-10-05) for `DELETE /api/repos/{repo_name}/upstream`, which runs the same handler; still served. Every response on both path styles, errors included, carries a [`Deprecation`](#deprecation-header) header |
| `DELETE /api/repos/{repo_name}/upstream` | repo:write | Path style only (spec 2026-10-05). Same handler and scope as `DELETE /repos/base`. → 200 `{message: "repository detached"}`. A repo without an upstream → 200 `{message: "repository already detached"}` (changelog 2026-10-07); the deprecated `base` routes keep `repository detached` in that case |

`POST /repos` and `GET /repos` are also served at the canonical paths `POST /api/repos`
and `GET /api/repos` (no `/v1`), which SDK 0.1.0 calls. They run the same handlers with
the same scopes, query parameters and responses.

**`POST /repos` body:**

- The JWT `repo` claim always names the new repository. The optional `repo_name` (it can
  contain `/`) must equal that claim exactly, with no case folding, trimming, or path
  normalization. A mismatch, including an empty string, returns 400 and creates nothing,
  for forks too. `null` or an omitted `repo_name` changes nothing.
- Query `default_branch` (deprecated upstream) is a fallback, used only when the body omits it.
- Without a `default_branch` in the body, in `base_repo` or in the query, a sync from
  `github` takes the GitHub repository's default branch
  ([#599](https://github.com/coder/openstorage/issues/599)). The create makes no network
  call, so the repository starts on `main`, and `GET /repo` shows `main`. The first
  successful sync reads the upstream `HEAD` and records its branch before it sends
  `repo.sync.succeeded`, so `GET /repo` then shows that branch. If the sync cannot use
  `HEAD` (no HEAD branch is advertised, or the fetch did not return that branch), the
  repository keeps `main`, the server logs a `WARN` line, and the next successful sync
  tries again. Later changes of the GitHub default branch do not move it, and a
  `PATCH /repo` ends the wait. Other providers use `main`. The S3-WAL backend keeps `main`
  ([deviation](../explanation/wire-compatibility.md#known-deviations-from-the-vendored-spec)).
  During a rolling upgrade from a release without this rule, two limits apply. A sync that
  an older worker runs does not adopt the branch, so the repository keeps `main` until the
  next sync by an upgraded worker. An older replica's `PATCH /repo` to `main` before that
  sync looks the same as the create's `main`, so the sync still adopts the GitHub branch.
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

**Repeat creates** (upstream spec 2026-10-07, [#388](https://github.com/coder/openstorage/issues/388)):

- A new repository → **201** `Repository '<name>' created`.
- A sync create (`upstream`, or a `base_repo` sync) of a repo with the same upstream →
  **200** `Repository '<name>' already exists`, with the existing `repo_id`. "Same" means the
  same stored provider, host, owner and name, and the same `forward_pushes`. The request's
  `ref`, `default_branch` and `auth` are ignored, and nothing is scheduled.
- A plain, copy or fork create of a finished repo without an upstream (a plain repo, a copy,
  or a fork) → **200** `Repository '<name>' already exists`, with the existing `repo_id`.
  It changes nothing: a copy or fork copies nothing, and it answers before it reads its
  source, so it succeeds after the source is deleted or its token expired. The request's
  `ref`, `sha` and `default_branch` are ignored.
- Any other create of a live name → **409**: a different upstream or `forward_pushes`, a sync
  create of a repo without an upstream, and a plain, copy or fork create of a repo with an
  upstream.
- Known gaps (409 where upstream answers 200): a repo whose create was not tracked answers
  409 to every repeat create, because open.storage cannot tell whether that create finished.
  That covers plain repos created before v0.16.0, sync repos created before v0.18.0, copies
  and forks created by v0.18.0 or earlier, every S3-WAL repo created by v0.18.0 or earlier
  (its metadata format v1 stores no create state, #335), and repos that an older server
  created during a rolling upgrade.
- A deleted repo keeps its name → **404**. Nothing is restored.
- A sync create whose first sync cannot be scheduled → **503** with `Retry-After`. The repo,
  its upstream link and its credential exist, but the create is pending. Either of these
  finishes it, with the stored link, and needs `git:write` and a token without ref-policy
  ops: a compatible repeat create (→ 200), or `POST /repos/pull-upstream` (→ 202). A
  repeat create with `repo:write` only → 403, naming `pull-upstream`.
- A copy or fork create is pending from its insert until its default branch is written.
  Meanwhile a plain, copy or fork create of the name → **503** with `Retry-After`, and a
  sync create → 409. When the fork fails, the repo is deleted and the name answers 404.
  When it succeeds, repeat creates answer as for any complete repo.
- A pending create can stay pending, and repeat creates then answer the same until
  `DELETE /repos/delete` clears the name (it then answers 404):
  - a fork or copy whose server crashed, or whose last step (marking the create complete)
    failed and answered 500: plain, copy and fork creates → 503;
  - a sync create whose upstream link was removed (`DELETE /repos/base`) before its first
    sync was scheduled: the create that finds the link gone → **503** without
    `Retry-After`. After that the repo has no link, so a plain, copy or fork create →
    503, a sync create → 409, and `pull-upstream` → 400.

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
| `PUT /repos/git-credentials` | repo:write (repo="org" or no repo) | `{id, password, username?}` → `{id, created_at}`; 404 |
| `DELETE /repos/git-credentials` | repo:write (repo="org" or no repo) | `{id}` → 200/204; 404 |

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
| `GET /repos/files` | `ref, ephemeral, path, recursive, cursor, limit` | `{paths: [..], paths_base64?: [..], ref, entries: [{path, path_base64?, type: blob\|tree\|symlink\|submodule, mode}], next_cursor, has_more}` — trees have no trailing `/`; branch on `type`. Without `recursive`, with an empty `recursive=`, or with `recursive=true`, it lists the contents of every folder under `path` and no `tree` entries. `recursive=false` lists only the direct children of `path`, folders as `tree` entries. No `cursor` and no `limit` (or `limit=0`): every entry, or 422 when the listing passes the git [output limit](#resource-limits-on-git-reads). Big trees must page: a recursive call with `limit` or `cursor` reads only the entries of its page. With a cursor it reads the folders on the cursor's path and the ones after it, each whole, and none of the folders before it, so a deep cursor costs about what the first page does plus the size of those folders and a request to git for each folder of the page: a cursor inside one folder of a million entries still reads that whole folder, but the folders before it are not read. Because they are not read, a missing or unreadable object there, or a tree nested past git's depth limit, no longer fails a page that the walk answers. A page that this does not suit is read from the start of the listing, as for a first page, and gives the same result, a failure on such an object included: trees with unusual entries, trees nested more than 128 folders deep or past a lower `core.maxTreeDepth` that git is set to, many folders without files, many small folders, or one very large folder, when the cursor is near the start of the listing (git reads those faster than the walk does), a git that fails on the way, and pages that come out empty. A tree that git stores out of order (one that `git fsck` rejects as `treeNotSorted`) can page in git's order and skip entries; git itself cannot look such entries up by path |
| `GET /repos/files/metadata` | same | `{files: [{path, path_base64?, mode, size, last_commit_sha, type}], commits: {<sha>: {author, date, message, order}}, ref, next_cursor, has_more}`. `order` is described below the table. `message` is the subject line only, as in the commits list ([Commits](#commits)). No `cursor` and no `limit` (or `limit=0`): every file. **429** when the files under `path` have more than 50,000 distinct last commits, also with `limit` or `cursor`: a retry gets the same 429 (no `Retry-After`), so send a narrower `path`. **404 on unborn branch** |
| `POST /repos/archive` | body `{ref?, include_globs?, exclude_globs?, max_blob_size?, archive?: {prefix}}` | tar.gz stream |
| `POST /repos/grep` | body `{query: {pattern, case_sensitive?}, ref?, ephemeral?, paths?, file_filters?: {include_globs, exclude_globs, extension_filters}, context?: {before, after}, limits?: {max_lines, max_matches_per_file}, pagination?: {cursor, limit}}` | `{query, repo: {ref, commit}, matches: [{path, path_base64?, lines: [{line_number, text, type: "match"\|context}]}], next_cursor, has_more}` |
| `GET /repos/blame` | `path` (req), `ref`, `ephemeral`, `range` (repeatable, `git blame -L` specs like `1,10`), `detect_moves` | `{ref, path, path_base64?, commit_sha, lines: [{line_number, commit_sha, original_line_number, original_path, original_path_base64?, previous_commit_sha?, author_name, author_email, author_time, committer_name, committer_email, committer_time, summary}]}` |

`order` in `files/metadata` is the commit's history-order rank relative to the resolved ref: its 0-based position in `git rev-list --topo-order <resolved commit>`. The ref's own commit is `0`, and larger values are older. A commit always ranks higher than every one of its descendants, whatever its author or committer date. Among a folder's files, the one with the smallest `order` was changed most recently in the ref's history.

`order` is an absolute position in the ref's history, not a rank within the response. Values can have gaps, and a response contains `0` only when the ref's own commit is the last commit of a listed file. Ranks agree across pages when the requests resolve to the same commit. A branch can move between requests, so pin `ref` to a commit SHA to page through one snapshot.

Ties between commits that ancestry does not order follow git's topo-order walk, which ignores dates. A commit becomes eligible once all of its children reachable from the ref are ranked, and the most recently eligible commit is ranked next. When ranking a merge makes several of its parents eligible at once, they become eligible in parent order, so the line of history behind the last of them is ranked first. The same history always yields the same ranks.

`order` comes from the same `git rev-list --topo-order` walk that finds the last commits ([resource limits on git reads](#resource-limits-on-git-reads)), so it costs no extra git subprocess. A 200 response always carries the exact `order` of every commit; the field is never omitted or estimated.

### Branches

| Method & path | Scope | Body/query → Response |
|---|---|---|
| `GET /repos/branches` | git:read | `cursor, limit, ephemeral` → `{branches: [{cursor, name, head_sha, created_at}], next_cursor, has_more}` |
| `GET /repos/branch` | git:read | `name` (req), `ephemeral?` → `{branch: {name, head_sha, created_at}}` — one branch by exact short name (no pagination fields); 404 when absent |
| `POST /repos/branches/create` | git:write | `{target_branch?, base_ref?\|base_branch?(deprecated), preferred_base?, base_is_ephemeral?, target_is_ephemeral?, force?}` → `{message, target_branch, target_is_ephemeral, commit_sha?}` |
| `DELETE /repos/branches` | git:write | Query `target_branch`, `ephemeral?`. The deprecated body `{target_branch (deprecated alias name), ephemeral?}` still works; a query parameter that is present wins over its body field, even when empty → `{target_branch, name (deprecated mirror), message, ephemeral}`. Refuses default branch (persistent): 409 for the query form, 403 for the body form |
| `GET /repos/branches/diff` | git:read | **Deprecated upstream** (both path styles); still implemented. Use `GET /repos/diff` ([migration](#migrating-from-branchesdiff)). `branch` (req), `base?` (defaults to the repo default branch), `ephemeral?`, `ephemeral_base?`, `path` (repeatable) → `{branch, base, merge_base_sha, stats: {files, additions, deletions, changes}, files: [FileDiff], filtered_files}`; `merge_base_sha` is empty when the histories are unrelated (direct `base..branch` diff) |
| `POST /repos/merge` | git:write | `{source_ref (bare branch name or refs/heads form; deprecated alias source_branch), target_branch, strategy: merge\|ff_only\|ff_prefer, source_is_ephemeral?, target_is_ephemeral?, expected_target_sha?, expected_source_sha?, commit_message? (no NUL byte), author?, committer?, allow_unrelated_histories?, squash?}` → `{result: merge_commit\|fast_forward\|no_op\|squash, commit_sha, tree_sha, source: {ref, branch (deprecated mirror), ephemeral, sha}, target: {branch, ephemeral, old_sha, new_sha}, merge_base_sha?, promoted_commits}`. File conflicts → 409 `MergeConflict` `{error, conflict_type: merge_conflict, conflict_paths, merge_base_sha?}`; a target that moved during the merge → 409 error body, `code: conflict` (upstream's `target_moved` reports a failed automatic retry, and open.storage does not retry); an `ff_only` merge that is not a fast-forward, and unrelated histories without `allow_unrelated_histories` → 412 error body, `code: precondition_failed`, as upstream documents. A failed `expected_target_sha` or `expected_source_sha` guard → 409 error body, `code: ref_guard_failed` ([details](#merge-guards)) |
| `GET /repos/merge/preview` | git:read | `source_branch, target_branch, source_is_ephemeral?, target_is_ephemeral?` (deprecated aliases `source_ephemeral`/`target_ephemeral`), `include_content?` → `{status: clean\|conflicted, result, source_branch, target_branch, source_tip_sha, target_tip_sha, merge_base_sha?, conflict_paths, conflicts: [{path, result\|base\|ours\|theirs: {oid?, content?, truncated, binary}}], filtered_conflicts: [{path, reason}]}`. Read-only |

#### Merge guards

A failed guard answers `409` with the error body before any merge work, and nothing
changes. The body has `code: ref_guard_failed` and two more members, as upstream's
`RefGuardProblemDetails`:

- `guard`: the request field of the guard that failed, `expected_target_sha` or
  `expected_source_sha`.
- `current_sha`: the tip of the guarded branch when the server checked the guard, or
  40 zeros when the source branch does not exist. Send it as the guard of the next
  attempt.

Before [#568](https://github.com/coder/openstorage/issues/568), the code was
`conflict`, without `guard` and `current_sha`.

`expected_target_sha` (a full 40-hex commit SHA) requires the target branch to still
point at that commit. If it does not, the guard fails (`detail` "expected_target_sha …
does not match current tip …"). v0.18.0 and earlier answered `412`
`precondition_failed` with a `result` object
([#532](https://github.com/coder/openstorage/issues/532)).

`expected_source_sha` (a full 40-hex commit SHA) makes `POST /repos/merge` merge that
commit instead of the current tip of `source_ref`. Use it to merge exactly the
`source_tip_sha` that `GET /repos/merge/preview` returned, even if the source branch
moved on since.

- The source branch must still contain the commit: the commit is the tip or one of its
  ancestors. Otherwise the guard fails (`detail` "source branch moved: the source
  branch does not contain commit …").
- A source branch that does not exist fails the guard too, with `current_sha` 40
  zeros. Without `expected_source_sha`, a missing source branch is `404`.
- `source.sha` in the response is the pinned commit.
- An explicit `null`, `""` or any value that is not a full SHA is `400`.
- `expected_target_sha` is checked first, after both branches are looked up. When both
  guards fail, the body names the target. A missing target branch is `404`.

This follows the [public OpenAPI](https://code.storage/docs/openapi.yaml) of
2026-10-16 (the field) and 2026-10-27 (`ref_guard_failed`). On 2026-10-27 the cluster
OpenAPI still served the older spec
([snapshot provenance](../explanation/wire-compatibility.md#how-drift-is-tracked)).

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

`FileDiff` = `{path, path_base64?, state (raw letter; SDKs normalize A/M/D/R/C/T/U), old_path?, old_path_base64?, raw
(unified diff text), bytes, is_eof, additions, deletions}`. `filtered_files` entries omit
`raw`/counts; large files are filtered unless requested via `path`. Every `*_base64?` key
is present only for a path that is not valid UTF-8 ([Path encoding](#conventions)).

### Commits

| Method & path | Scope | Notes |
|---|---|---|
| `GET /repos/commits` | git:read | `ref?` (deprecated alias `branch`), `cursor?, limit?, ephemeral?, path?` → `{commits: [CommitInfo], next_cursor, has_more}`, newest first; defaults to default branch. Each entry's `message` is the subject line only (below the table) |
| `GET /repos/commit` | git:read | `ref` (req; deprecated alias `sha`; branch name, tag, short or full SHA) → `{commit: CommitInfo & {signature?, payload?}}` (signed commits: armored sig + exact signed payload bytes) |
| `GET /repos/diff` | git:read | `ref` (req; deprecated alias `sha`), `base_ref?` (deprecated alias `baseSha`), `ref_is_ephemeral?`, `base_is_ephemeral?`, `git_apply_compatible?` (deprecated alias `gitApplyCompatible`), `path` (repeatable) → `{sha, base_sha, merge_base_sha, stats, files: [FileDiff], filtered_files}`. `base_sha` is the commit `base_ref` resolved to, empty for a single-commit diff. On alias conflict the current name wins |
| `POST /repos/commit-pack` | git:write | NDJSON stream — see the [streaming protocols](../explanation/streaming-commit-protocols.md) |
| `POST /repos/diff-commit` | git:write | NDJSON stream — see the [streaming protocols](../explanation/streaming-commit-protocols.md) |
| `POST /repos/restore-commit` | git:write | JSON `{metadata: {target_branch, base_ref (branch, tag, or commit; deprecated alias target_commit_sha, 40-hex only), author, commit_message? (no NUL byte), expected_target_sha? (deprecated alias expected_head_sha), committer?}}` → commit ack ([streaming protocols](../explanation/streaming-commit-protocols.md)). Creates a *new* commit restoring old tree (history preserved). A `refs/...` `base_ref` resolves verbatim. Any other name tries a branch, then a tag (annotated tags peel). Then `base_ref` resolves as a full or abbreviated SHA (a full SHA may be uppercase). It always resolves outside the ephemeral namespace. Extensions: `target_is_ephemeral?` (deprecated alias `ephemeral`) restores a branch in the ephemeral namespace. `base_is_ephemeral` (deprecated alias `ephemeral_base`) is accepted and ignored |
| `POST /repos/reset-commits` | git:write | In OpenAPI only (not in SDKs): NDJSON metadata stream; same restore-commit family |

`CommitInfo` = `{sha, parent_shas[], message, author_name, author_email, committer_name,
committer_email, date (RFC3339), committer_date (RFC3339)}`. `date` is the author
timestamp and `committer_date` the committer timestamp, as in upstream's spec of
2026-10-08. A rebase or cherry-pick changes only `committer_date`. Before that spec,
`date` carried the committer timestamp.

A push can hold a commit whose author or committer line git cannot parse, because the
server runs no fsck on a push. Reads of such a commit answer 200 with git's own
fallbacks:

- If git cannot split the line into a name and an `<email>` (for example, a NUL byte in
  the name), `author_name` and `author_email` (or the committer pair) are `""`, as git's
  `%an` and `%ae` print them. Blame sends git blame's `(unknown)` instead. The date is
  `1970-01-01T00:00:00Z`. `git log` shows neither the person nor a date for such a line.
- If git cannot read the date (no date, a date that is not a number, no time zone),
  `date` (or `committer_date`) is `1970-01-01T00:00:00Z`. `git log` shows the same epoch
  for such a line.
- A date after `9999-12-31T23:59:59Z` also gives `1970-01-01T00:00:00Z`, because RFC 3339
  cannot write it.

The `files/metadata` `commits` map and blame's `author_time` and `committer_time` follow
the same rules. Blame keeps the line's time zone offset, except an offset that RFC 3339
cannot write (24 hours or more, or 60 minutes or more) or one that moves the local date
past the year 9999. Then it sends UTC.

`GET /repos/commit` sends the full `message`, with trailing newlines removed. `GET
/repos/commits` and the `files/metadata` `commits` map send only the subject line, as in
upstream's docs spec of 2026-10-27: the first paragraph of the message as one line,
without the body and the trailers. This is git's `%s`: blank lines before the first
paragraph are skipped, a whitespace-only line ends it, its lines are joined with one
space after their trailing whitespace is removed, and an empty message gives `""`. A
continuation line keeps its leading whitespace, so `"subj\n  next"` gives `subj   next`.
Before that spec, both carried the full message.

#### Commit write guards

`commit-pack`, `diff-commit` and `restore-commit` take `expected_target_sha` (deprecated
alias `expected_head_sha`, a full 40-hex commit SHA): the target branch must still point
at that commit. When the target branch does not exist and a `commit-pack` or
`diff-commit` names `base_branch`, the guard compares with the tip of `base_branch`. When the guard fails,
no ref moves, and the answer is `409` with the
[result envelope](../explanation/streaming-commit-protocols.md#3-response-envelope-shared-also-used-by-restore-commit)
plus the problem members of the error body, as upstream's
`CommitWriteRefGuardProblemDetails` and `RestoreCommitRefGuardProblemDetails`:

- `code: ref_guard_failed` (`detail` "expected_target_sha … does not match current tip
  …").
- `guard: expected_target_sha`, also when the request sent `expected_head_sha`.
- `current_sha`: the commit the guard was checked against, that is the tip of the target
  branch, or the tip of `base_branch` for a new branch. Send it as the guard of the next
  attempt.
- `result` has every member of upstream's `RefUpdateResult`: `target_branch` and `branch`
  name the target branch, `new_sha` is empty, `success` is `false`, and `status` is
  `conflict`. `old_sha` is the tip of the target branch, so it repeats `current_sha`,
  except on a new branch, where it is 40 zeros. `commit` is `null`, as upstream's
  schema descriptions say.

The content type is `application/problem+json`, as for every body with the problem
members. A CAS race lost at publish time is a different `409`, an `application/json`
result envelope without the problem members
([streaming protocols](../explanation/streaming-commit-protocols.md#3-response-envelope-shared-also-used-by-restore-commit)).
Before [#647](https://github.com/coder/openstorage/issues/647), a failed guard answered
`412` `precondition_failed` with the result envelope only.

### Tags

| Method & path | Scope | Body/query |
|---|---|---|
| `GET /repos/tags` | git:read | `cursor, limit` → `{tags: [{cursor, name, sha}], next_cursor, has_more}` — lexical order; `sha` dereferenced to commit |
| `GET /repos/tag` | git:read | `name` (req) → `{tag: {name, sha}}` — one tag by exact short name, `sha` dereferenced to commit; 404 when absent |
| `POST /repos/tags` | git:write | `{name, ref (deprecated alias target)}` → `{name, sha, message}` — lightweight only; name must not start with `refs/` |
| `DELETE /repos/tags` | git:read+git:write | `{name}` → `{name, message}` — resolves stored ref for lightweight & annotated |
| `DELETE /api/repos/{repo_name}/tags/{tag_name}` | git:read+git:write | Path style only (the form SDK 0.1.0 calls). Same handler, scopes and responses as `DELETE /repos/tags`, with the tag name from the path (a `/` in it is sent as `%2F`). A request body is not read |

### Notes

| Method & path | Scope | Body/query |
|---|---|---|
| `GET /repos/notes` | git:read | `object_ref` (req; deprecated alias `sha`), `notes_ref?` (deprecated alias `ref`) → `{sha, note, ref_sha}` |
| `POST /repos/notes` | git:write | `{object_ref (deprecated alias sha), action: "add"\|"append" (deprecated upstream), note, notes_ref? (deprecated alias ref), expected_notes_ref_sha? (deprecated alias expected_ref_sha), author?}` → `{sha, notes_ref, target_ref (deprecated mirror), base_commit?, new_ref_sha, result: {success, status, message?}}`. `add` fails if note exists; `append` creates or appends |
| `POST /api/repos/{repo_name}/notes/append` | git:write | Path style only (spec 2026-10-05). `{object_ref (deprecated alias sha), note, notes_ref? (deprecated alias ref), expected_notes_ref_sha? (deprecated alias expected_ref_sha), author?}`, decoded strictly: `action`, any unknown field, or a `null` → 400. Appends, or creates the note when the object has none → **201** with the same envelope. A stale guard → 409 `ref_guard_failed` and nothing is appended |
| `DELETE /repos/notes` | git:write | Query `object_ref`, `notes_ref?`, `expected_notes_ref_sha?`. The deprecated body `{object_ref (deprecated alias sha), notes_ref?, expected_notes_ref_sha?, author?}` still works; a query parameter that is present wins over its body field, even when empty → same envelope |
| `GET /repos/notes/refs` | git:read | `prefix?` (default `refs/notes/`), `cursor, limit` → `{refs: [{cursor, ref, sha}], prefix, next_cursor, has_more}`. 400 if custom notes refs disabled server-side |

- Default notes ref: `refs/notes/commits`. Bare names resolve to `refs/notes/<name>`.
- `object_ref`: a full object SHA (commit, tree, blob, or annotated tag), or a branch,
  tag, short SHA, or other revision that resolves to a commit. Unresolvable → 404.
- Responses echo the resolved object id in `sha`.
- A stale `expected_notes_ref_sha` → 409 on every form: `POST .../notes`, `POST
  .../notes/append`, and `DELETE .../notes` with the guard in the query or in the
  deprecated body. Before [#647](https://github.com/coder/openstorage/issues/647), the
  `POST .../notes` and `DELETE` body forms answered 412 `precondition_failed` with the
  note envelope and no problem members.
- Every other note-write failure is the plain error body (no note envelope), as upstream
  documents. A notes ref that moves while the write runs (a lost CAS) → 409 `conflict`.
- The stale-guard 409 keeps the note envelope (`result.status` `conflict`) and adds
  the problem members of the error body: `code: ref_guard_failed`, `guard: expected_notes_ref_sha`
  (also when the request sent the deprecated `expected_ref_sha`), and `current_sha`, the
  notes ref's tip or 40 zeros when it does not exist. `new_ref_sha` repeats
  `current_sha`. Its content type is `application/problem+json`, as for every body with
  the problem members. Before
  [#568](https://github.com/coder/openstorage/issues/568), the body had no problem
  members.

### Upstream sync

| Method & path | Scope | Notes |
|---|---|---|
| `POST /repos/pull-upstream` | git:write | Optional body `{ref?, after?}` → **202 Accepted** (async). Sync-linked repos (`upstream` or `base_repo` sync) only. Deprecated upstream (2026-10-05) for `POST /api/repos/{repo_name}/upstream/pull`; still served. Every response on both path styles, errors included, carries a [`Deprecation`](#deprecation-header) header. Both body fields are accepted and ignored: every pull syncs all branches and tags of the upstream. Upstream deprecated the body on 2026-10-08 |
| `POST /api/repos/{repo_name}/upstream/pull` | git:write | Path style only (spec 2026-10-05). Same handler, scope, ref-policy check and responses as `POST /repos/pull-upstream`. The optional, deprecated body `{ref?, after?}` is decoded strictly: an unknown field or a `null` → 400. Its fields are ignored |
| `POST /repos/push-upstream` | git:write + git:read | `{force?}` → **202 Accepted** (async). Sync-linked repos only. open.storage extension |

#### Deprecation header

Every response of the four deprecated routes carries `Deprecation: @1791331200`:
`POST /api/v1/repos/pull-upstream`, `POST /api/repos/{repo_name}/pull-upstream`,
`DELETE /api/v1/repos/base` and `DELETE /api/repos/{repo_name}/base`. Error responses
carry it too: 400, 401, 403, 404, 429 and 500. The replacements
(`POST .../upstream/pull`, `DELETE .../upstream`) and `push-upstream` send no such header.

The value is an [RFC 9745](https://www.rfc-editor.org/rfc/rfc9745) date: deprecated since
2026-10-07T00:00:00Z, the date of upstream's changelog entry that announced the header.
Upstream documents no value and no `Link` or `Sunset` header, so open.storage sends no
`Link` or `Sunset` ([deviation](../explanation/wire-compatibility.md#known-deviations-from-the-vendored-spec)).

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
([webhooks](webhooks.md)); inbound runs emit `repo.sync.*`. An outbound run emits
`repo.push_upstream.started` once its loopback fetch of this server has the snapshot it
forwards, before it pushes to the upstream. A run that fails before or at that fetch
emits `repo.push_upstream.started` right before its `repo.push_upstream.failed`.
`DELETE /repos/base` silently cancels
in-flight runs in both directions. A run that
server shutdown cancels also ends without a terminal event: its request stays pending,
and a later run (the next server process, or on PostgreSQL another replica) runs it with
a new `repo.sync.started` (outbound: `repo.push_upstream.started`). An outbound run
emits no event when a write in progress (a git push, or on SQLite and S3WAL a ref write
through this API) holds a ref that the run would otherwise forward at an older value:
the request stays pending, and a new run starts a few seconds later.

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

**Ref writes that forward:** on SQLite and S3WAL, a ref write through this API that
triggers the forward (a `forward_pushes` repo and a token that passes the gates above)
holds the ref and records the forward request before it changes the ref. It waits up to
10 seconds for a git push or another such write that holds the same ref or a
directory/file relative of it (`refs/heads/a` and `refs/heads/a/b`), then fails with
**503** and `Retry-After: 1`. When the read of the repository's upstream link fails, a ref
write from a token that passes the gates above fails with a plain **500** on any
repository, forwarding or not: the write reads the link before it knows whether the
repository forwards. A write to the ephemeral namespace is the exception: it takes no
hold and reads no link. Any other failure to take the hold, or a failed write of the forward
request, also fails the write with a plain **500**. In each case the ref stays unchanged.
A write that loses its compare-and-swap (409)
has recorded its request already, so the repository gets one forward run with nothing new
to push.

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
| Output | 64 MiB of buffered git output per request, shared by all its subprocesses. File contents that `file`, `archive` and `diff` stream instead of buffering are not counted. A recursive `files` call with `limit` or `cursor` counts only the entries of its page. | 422. git is stopped as soon as the limit is passed. |
| File size | 100 MiB per file, checked from the tree before the file is read. | 422 for `file` (GET and HEAD) and `archive`, before any response byte. The body of a `file` GET and of an `archive` answer names the file (quoted, cut at 256 bytes), and `exclude_globs` leaves it out of an `archive`. |
| Symlink target | 4,095 bytes, the most that `symlink(2)` accepts on Linux. XFS and macOS accept at most 1,023. `archive` reads each target in memory to write its tar header, checks its size from the tree first, and stops reading at the limit if git prints more. | 422 for `archive`, before any response byte. The body names the symlink (quoted, cut at 256 bytes), and `exclude_globs` leaves the link out of the archive. `file` streams a symlink's target like any file, so the 100 MiB file limit applies to it. |
| Delivery | Each 64 KiB of a response must reach the client within 30 seconds, and the whole response within 2 minutes of its first byte (`file` and `archive`: 10 minutes). | The connection is closed. The request's slot, memory and temporary files are released. |

On a 300,000-commit linear history, `git log -1 -- <file added in the root commit>` needs 184 MB of git heap. The `files/metadata` history walk for files that date from the root commit served a 400,000-commit linear history in about 5 seconds with at most 192 MiB of git heap, and got 422 at 500,000 commits ([#576](https://github.com/coder/openstorage/issues/576), [#588](https://github.com/coder/openstorage/issues/588), measured 2026-10-28). Histories past roughly 400,000 commits can get 422 for per-file history walks and `blame` of old files. Deep `commits` pages need less memory: they still fit at 1,000,000 commits (at most 232 MiB) and are estimated to reach the cap at roughly 1.1 million. Every `files/metadata` call, whatever its `limit`, walks history once for all files under `path`: it lists the whole history to a temporary file (about 80 bytes per commit, at most 256 MiB, or 422), then reads diffs down to the oldest of the files' last commits. It diffs only the commits that a file still waits on, and diffs a merge against a later parent only while a file is left that differs from every earlier parent. It sends up to 16 diffs ahead along chains of single-parent commits, so a linear history does not wait on git once per commit. On kubernetes (142,643 commits, 58,571 merges, 31,590 files) a root call took about 11 seconds alone, about 15 seconds with four such calls on four repositories at once, and about 17 seconds with eight. On git.git (82,605 commits) a root call took about 3 seconds. A call whose walks need more than 30 seconds in total gets 503; send a narrower `path`. A commit with more than about 25,000 parents (1 MiB of parent IDs) makes the walk fail with the output limit's 422. Commit-graphs, which full hydrates now write ([ADR-47](../adr/047-cache-repos-get-split-commit-graphs-written-by-full-hydrates.md)), make these walks cheaper when present, but the limits do not depend on them. The `git log -1` and `commits` figures above are without a commit-graph; the `files/metadata` figures had one, except the 500,000-commit history, whose graph write timed out.

| Endpoint | git work | Cost grows with |
|---|---|---|
| all below | ref resolution (`check-ref-format`, `rev-parse`) | nothing (single object) |
| `commits` | `git log --skip=<cursor offset> -n <limit+1> [-- path]` | cursor offset; history searched for `path` |
| `commit` | `git log -n 1`, `git cat-file commit` | the commit object (single object) |
| `files` | `git ls-tree [-r]`; for a recursive call with a `cursor`, `git cat-file --batch` for the folders on the cursor's path and the ones after it, and `git cat-file --batch-check` for the sizes of the page's files, and `git config --type=int --get core.maxTreeDepth` once for a page the walk has filled, then `git ls-tree -r` again when the walk gives way | tree size; for a recursive call with `limit` or `cursor`, the entries of the page and, with a cursor, the folders on its path and the ones after it, each read whole (not the folders before it). A walk that gives way has already paid for the folders it read when the stream starts |
| `files/metadata` | `git ls-tree -r`; `git rev-list --topo-order --parents` and `git diff-tree --stdin` for the last commit of every file under `path`; `git log --no-walk` for the listed files' commits | tree size; history length; history depth to the oldest last commit |
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
- **Request values that git cannot take.** git gets paths, `paths` entries and grep patterns as arguments. exec refuses an argument over 128 KiB on Linux, arguments that together pass the system's limit (`ARG_MAX`, for example many `paths` in one grep), and any argument with a NUL byte. Such a request gets **400**, not 500.
- `archive` streams its response. An error after the response has started (for example, the time limit, or a delta-compressed file whose base object does not fit in memory) closes the connection, so the client sees a truncated, invalid gzip stream instead of a status code.
- Not covered by these limits: hydrating the cache repository before the git reads (serialized per repository), git clone and fetch over the git protocol (`upload-pack`, which has its own request-size limits), and the write endpoints.

## Status-code contract (what clients key on)

| Status | Meaning to clients |
|---|---|
| 200/201 | success (201 accepted by SDKs on create endpoints) |
| 202 | pull-upstream / push-upstream accepted |
| 206/304/412/416 | file conditional/range results (passed through) |
| 400 | invalid input; also "custom notes refs not enabled" |
| 401 | no `Authorization` header, or a scheme other than `Bearer` (`Authentication required`) |
| 403 | a token that fails verification (`Invalid or expired token`); permission or ref-policy denied; `authentication_unavailable` when the key store fails (retry) ([failure mapping](authentication.md#failure-mapping)) |
| 404 | repo/object/branch not found; unborn-branch file metadata; no API route matches the path (`route_not_found`) |
| 405 | the path has routes, but not for this method (`Allow` lists them) |
| 409 | repo exists, repo already deleted, credential exists, merge conflict, failed ref guard (`expected_target_sha`, `expected_source_sha`, `expected_notes_ref_sha`), CAS conflict, a `diff-commit` patch that does not apply to the base ([streaming protocols](../explanation/streaming-commit-protocols.md)) |
| 412 | a failed `restore-commit` `base_ref` precondition; a merge that is not a fast-forward under `ff_only`, or of unrelated histories without `allow_unrelated_histories`; a `commit-pack` or `diff-commit` that changes nothing (`no changes to commit`) or whose stream ends before an `eof` chunk ([streaming protocols](../explanation/streaming-commit-protocols.md)) |
| 413 | a `commit-pack` whose decoded file contents pass 262144000 bytes ([streaming protocols](../explanation/streaming-commit-protocols.md)) |
| 422 | a git read exceeded a memory, output, file-size or symlink-target limit ([resource limits on git reads](#resource-limits-on-git-reads)); retrying the same request fails the same way |
| 429 | rate limit, busy or backpressure (plain problem body) |
| 5xx | internal/unavailable; 503 documented for a sync create whose first sync could not be scheduled (`POST /repos`, with `Retry-After`), for cold-storage warmup on grep/archive, for git reads when the server's git concurrency is exhausted (with `Retry-After`) or a request exceeds its git time budget (without `Retry-After`) ([resource limits on git reads](#resource-limits-on-git-reads)), and, on SQLite and S3WAL, for a ref write that forwards and waited 10 seconds for a concurrent git push or ref write that holds the same ref or a directory/file relative of it (with `Retry-After`; [ref writes that forward](#upstream-sync)) |
