# HTTP API Reference (v1, SDK-compatible surface)

Base: `https://api.{org}.code.storage/api/v1` (SDKs build `{apiBaseUrl}/api/v{version}`,
version fixed at 1). All endpoints require `Authorization: Bearer <jwt>` (see the
[auth spec](authentication.md)). SDKs send a `Code-Storage-Agent` header — `code-storage-sdk/<ver>`
(TypeScript), `code-storage-py-sdk/<ver>` (Python), `code-storage-go-sdk/<ver>` (Go);
treat it as informational.

For exhaustive request/response schemas see [`../upstream/openapi.yaml`](../upstream/openapi.yaml) (official; also defines
the parallel `/api/repos/{repo_name}/...` path style). This document is the SDK-verified
contract with implementation notes the spec lacks.

## Conventions

- **Pagination** (all list endpoints): request `cursor` + `limit` query params (or
  `pagination: {cursor, limit}` in grep's body); response `next_cursor` (opaque string, null/
  empty when exhausted) + `has_more` (bool). Docs default limit 20, max 100; SDK-observed
  server defaults differ per endpoint (files 1000/max 5000, files/metadata 200/max 1000,
  notes refs 20).
- **Errors**: `{"error": "<unstable human string>"}` + status code. Write endpoints that move
  refs return the richer `result` envelope ([streaming protocols](../explanation/streaming-commit-protocols.md)) even on
  4xx/5xx statuses `[400,401,403,404,408,409,412,422,429,499,500,502,503,504]`.
- **Booleans in query strings** are literal `"true"` / `"false"`.
- **Repeated params**: `path` may repeat on diff endpoints; `range` may repeat on blame.
- **No client retries**: none of the SDKs retry; every error surfaces immediately. Timeouts
  observed client-side (Python): 30s default, 60s diff/grep, 180s branch-create/merge/
  merge-preview/commit/tag/restore-commit.

## Endpoint catalog

### Repository lifecycle (`repo:write` / `org:read`)

| Method & path | Scope | Notes |
|---|---|---|
| `POST /repos` | repo:write (repo=new id) | Body optional: `{default_branch?, base_repo?}`. Query `default_branch` is a fallback used only when the body omits it. `base_repo`: `{provider, owner, name, operation?: "fork", auth?: {token}\|{auth_type:"public"}, ref?, sha?, default_branch?, upstream_host?, forward_pushes?}`. Providers: `code, github, gitlab, bitbucket, gitea, forgejo, codeberg, sr.ht` (+generic w/ `upstream_host`). `forward_pushes: true` (default false, our extension) schedules a push-upstream run after every won local push — see "Upstream sync". → `{repo_id, repo_name, http_url, message}`. 409 = exists. |
| `GET /repos` | org:read (repo="org") | Query `cursor, limit, q` (case-insensitive substring on repo `url`). → `{repos: [{repo_id, repo_name, url (deprecated mirror), default_branch, created_at, base_repo?}], next_cursor, has_more}` |
| `GET /repo` | git:read (repo from JWT) | → `{repo_id, repo_name, url (deprecated mirror), default_branch, created_at}`; 404 → SDK returns null |
| `PATCH /repo` | repo:write | Update the default branch. Body `{default_branch}` required, non-empty; a plain branch name (qualified to `refs/heads/<name>` at hydrate time) or its `refs/heads/`-qualified spelling (stored verbatim). Other ref namespaces (tags/notes) → 400 — the default names a branch. The branch is never created by this call and may be unborn (clones then see a dangling `HEAD`, like a fresh repo). CAS against the previously read value → 409 on a concurrent change; `no-push` ref policies covering `HEAD` or the target deny with 403. → `{repo_id, id (deprecated mirror), repo_name, url (deprecated mirror), default_branch}` (no `created_at`, per the live spec). Clone `HEAD` follows on the next hydrate of each node's cache. |
| `DELETE /repos/delete` | repo:write | Soft delete + async cleanup. → `{repo_id, message}`; 404 not found; 409 already deleted |
| `GET /api/repo-urls/{repo_id}` | org:read | (No /v1 prefix; from OpenAPI.) Resolve internal id → `{repo_id, repo_name, url (deprecated mirror)}`; `repo_name` is the JWT `repo` claim value |
| `DELETE /repos/base` | repo:write | Detach upstream provider; idempotent (from official docs/OpenAPI, not in SDKs) |

### Git credentials (generic HTTPS upstreams)

| Method & path | Scope | Body → Response |
|---|---|---|
| `POST /repos/git-credentials` | repo:write (repo=repoId) | `{repo_id, password, username?}` → `{id}`; 409 = exists |
| `PUT /repos/git-credentials` | repo:write (repo="org") | `{id, password, username?}` → `{id, created_at}`; 404 |
| `DELETE /repos/git-credentials` | repo:write (repo="org") | `{id}` → 200/204; 404 |

### Files & trees (`git:read`)

| Method & path | Query/body | Response |
|---|---|---|
| `GET /repos/file` | `path` (req), `ref`, `ephemeral`, `ephemeral_base`; headers `Range`, `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, `If-Range` | Raw blob stream. Status 200/206/304/412/416. Headers: `X-Blob-Sha`, `X-Last-Commit-Sha`, `ETag`, `Last-Modified`, `Accept-Ranges`, `Content-Range`, `Content-Type`, `Content-Length` |
| `HEAD /repos/file` | same | headers only |
| `GET /repos/files` | `ref, ephemeral, path, recursive, cursor, limit` | `{paths: [..], ref, entries: [{path, type: blob\|tree\|symlink\|submodule, mode}], next_cursor, has_more}` — trees have no trailing `/`; branch on `type` |
| `GET /repos/files/metadata` | same | `{files: [{path, mode, size, last_commit_sha, type}], commits: {<sha>: {author, date, message}}, ref, next_cursor, has_more}`. **404 on unborn branch** |
| `POST /repos/archive` | body `{ref?, include_globs?, exclude_globs?, max_blob_size?, archive?: {prefix}}` | tar.gz stream |
| `POST /repos/grep` | body `{query: {pattern, case_sensitive?}, ref?, ephemeral?, paths?, file_filters?: {include_globs, exclude_globs, extension_filters}, context?: {before, after}, limits?: {max_lines, max_matches_per_file}, pagination?: {cursor, limit}}` | `{query, repo: {ref, commit}, matches: [{path, lines: [{line_number, text, type: "match"\|context}]}], next_cursor, has_more}` |
| `GET /repos/blame` | `path` (req), `ref`, `ephemeral`, `range` (repeatable, `git blame -L` specs like `1,10`), `detect_moves` | `{ref, path, commit_sha, lines: [{line_number, commit_sha, original_line_number, original_path, previous_commit_sha?, author_name, author_email, author_time, committer_name, committer_email, committer_time, summary}]}` |

### Branches

| Method & path | Scope | Body/query → Response |
|---|---|---|
| `GET /repos/branches` | git:read | `cursor, limit, ephemeral` → `{branches: [{cursor, name, head_sha, created_at}], next_cursor, has_more}` |
| `GET /repos/branch` | git:read | `name` (req), `ephemeral?` → `{branch: {name, head_sha, created_at}}` — one branch by exact short name (no pagination fields); 404 when absent |
| `POST /repos/branches/create` | git:write | `{target_branch, base_ref?\|base_branch?(deprecated), base_is_ephemeral?, target_is_ephemeral?}` → `{message, target_branch, target_is_ephemeral, commit_sha?}` |
| `DELETE /repos/branches` | git:write | `{target_branch (deprecated alias name), ephemeral?}` → `{target_branch, name (deprecated mirror), message, ephemeral}`. Refuses default branch (persistent) |
| `GET /repos/branches/diff` | git:read | `branch` (req), `base?`, `ephemeral?`, `ephemeral_base?`, `path` (repeatable) → `{branch, base, stats: {files, additions, deletions, changes}, files: [FileDiff], filtered_files}` |
| `POST /repos/merge` | git:write | `{source_ref (bare branch name or refs/heads form; deprecated alias source_branch), target_branch, strategy: merge\|ff_only\|ff_prefer, source_is_ephemeral?, target_is_ephemeral?, expected_target_sha?, commit_message?, author?, committer?, allow_unrelated_histories?, squash?}` → `{result: merge_commit\|fast_forward\|no_op\|squash, commit_sha, tree_sha, source: {ref, branch (deprecated mirror), ephemeral, sha}, target: {branch, ephemeral, old_sha, new_sha}, merge_base_sha?, promoted_commits}`. Conflict → 409 (+`conflict_paths`, `merge_base_sha`) |
| `GET /repos/merge/preview` | git:read | `source_branch, target_branch, source_is_ephemeral?, target_is_ephemeral?` (deprecated aliases `source_ephemeral`/`target_ephemeral`), `include_content?` → `{status: clean\|conflicted, result, source_branch, target_branch, source_tip_sha, target_tip_sha, merge_base_sha?, conflict_paths, conflicts: [{path, result\|base\|ours\|theirs: {oid?, content?, truncated, binary}}], filtered_conflicts: [{path, reason}]}`. Read-only |

`FileDiff` = `{path, state (raw letter; SDKs normalize A/M/D/R/C/T/U), old_path?, raw
(unified diff text), bytes, is_eof, additions, deletions}`; `filtered_files` entries omit
`raw`/counts (large files filtered unless explicitly requested via `path`).

### Commits

| Method & path | Scope | Notes |
|---|---|---|
| `GET /repos/commits` | git:read | `ref?` (deprecated alias `branch`), `cursor?, limit?, ephemeral?, path?` → `{commits: [CommitInfo], next_cursor, has_more}`, newest first; defaults to default branch |
| `GET /repos/commit` | git:read | `ref` (req; deprecated alias `sha`; branch name, tag, short or full SHA) → `{commit: CommitInfo & {signature?, payload?}}` (signed commits: armored sig + exact signed payload bytes) |
| `GET /repos/diff` | git:read | `ref` (req; deprecated alias `sha`), `base_ref?` (deprecated alias `baseSha`), `ref_is_ephemeral?`, `base_is_ephemeral?`, `git_apply_compatible?` (deprecated alias `gitApplyCompatible`), `path` (repeatable) → `{sha, stats, files: [FileDiff], filtered_files}`. On alias conflict the current name wins |
| `POST /repos/commit-pack` | git:write | NDJSON stream — see the [streaming protocols](../explanation/streaming-commit-protocols.md) |
| `POST /repos/diff-commit` | git:write | NDJSON stream — see the [streaming protocols](../explanation/streaming-commit-protocols.md) |
| `POST /repos/restore-commit` | git:write | JSON `{metadata: {target_branch, target_commit_sha, author, commit_message?, expected_head_sha?, committer?}}` → commit ack ([streaming protocols](../explanation/streaming-commit-protocols.md)). Creates a *new* commit restoring old tree (history preserved) |
| `POST /repos/reset-commits` | git:write | In OpenAPI only (not in SDKs): NDJSON metadata stream; same restore-commit family |

`CommitInfo` = `{sha, parent_shas[], message, author_name, author_email, committer_name,
committer_email, date (RFC3339)}`.

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
| `POST /repos/notes` | git:write | `{object_ref (deprecated alias sha), action: "add"\|"append", note, notes_ref? (deprecated alias ref), expected_notes_ref_sha? (deprecated alias expected_ref_sha), author?}` → `{sha, notes_ref, target_ref (deprecated mirror), base_commit?, new_ref_sha, result: {success, status, message?}}`. `add` fails if note exists; `append` creates or appends |
| `DELETE /repos/notes` | git:write | `{object_ref (deprecated alias sha), notes_ref?, expected_notes_ref_sha?, author?}` → same envelope |
| `GET /repos/notes/refs` | git:read | `prefix?` (default `refs/notes/`), `cursor, limit` → `{refs: [{cursor, ref, sha}], prefix, next_cursor, has_more}`. 400 if custom notes refs disabled server-side |

Default notes ref `refs/notes/commits`; bare names resolve to `refs/notes/<name>`.

### Upstream sync

| Method & path | Scope | Notes |
|---|---|---|
| `POST /repos/pull-upstream` | git:write | `{ref?}` → **202 Accepted** (async); only for repos with `base_repo`. A 202 is never dropped: a request that arrives while a sync run is already in flight coalesces into one follow-up run, so some run observes upstream as of the request or later |
| `POST /repos/push-upstream` | git:write | `{force?}` → **202 Accepted** (async); only for repos with `base_repo`. Our extension (upstream forwards implicitly — see [wire compatibility](../explanation/wire-compatibility.md)): publishes local `refs/heads/*` and `refs/tags/*` to the upstream with the stored credential. Non-force by default — a ref the upstream refuses (non-fast-forward) fails the run with per-ref reasons in the `repo.sync.failed` event's `rejected_refs`; `force: true` opts into `+refs`. Ref deletions are not propagated. The 202 coalescing guarantee matches pull-upstream, and `force` survives coalescing (a forced request is never downgraded). An unreachable upstream retries with exponential backoff (1–16 min, 5 attempts); auth/config/rejection failures do not retry |

Outbound runs emit the same `repo.sync.*` events with `direction: "push"` (see
[webhooks](webhooks.md)). `DELETE /repos/base` cancels in-flight runs in both
directions silently. Ref policies fail closed on both triggers: any policy op
denies pull-upstream and push-upstream alike.

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
| 429 | rate limit ("unavailable" to ref-update clients) |
| 5xx | internal/unavailable; 503 documented for cold-storage warmup on grep/archive |
