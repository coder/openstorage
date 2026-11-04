# Streaming Commit Protocols (NDJSON)

**TL;DR:** the two streaming commit endpoints (`commit-pack`, `diff-commit`) take a
custom **NDJSON** stream, not git packfiles: a metadata frame, then base64 chunk
frames, answered by a shared response envelope. `restore-commit` is plain JSON.

Both streaming endpoints follow these transport rules:

- Headers: `Content-Type: application/x-ndjson`, `Accept: application/json`.
- Clients send `duplex: half` chunked bodies. The backend MUST accept chunked request
  bodies without `Content-Length`.
- The backend MUST process lines incrementally (streaming parse, not
  buffer-then-parse): uploads can exceed memory.
- All three SDKs implement byte-identical framing. The chunk max is **4 MiB raw bytes
  before base64** (`4 * 1024 * 1024`), standard base64 alphabet. A decoded chunk over
  4 MiB gets 400 `invalid`.
- `commit-pack` limits the decoded file contents of one request to 262144000 bytes
  (250 MiB), summed over every chunk of every file. Past that, the server stops reading
  and answers 413 with `result.status: "payload_too_large"` (a value of the upstream
  `RefUpdateResult.status` enum) and the message
  `commit payload too large: exceeds total blob limit (262144000 bytes)`. No ref moves.

## 1. `POST /api/v1/repos/commit-pack` — commit from file operations

Line 1 — metadata frame:

```json
{"metadata": {
  "target_branch": "main",              // required; no "refs/" prefix allowed
  "commit_message": "msg",              // required
  "author": {"name": "A", "email": "a@x"},       // required
  "committer": {"name": "...", "email": "..."},  // optional
  "expected_head_sha": "<40-hex>",      // optional CAS guard
  "base_branch": "main",                // optional: start new branch from here
  "ephemeral": false,                   // target in ephemeral namespace
  "ephemeral_base": false,              // base in ephemeral namespace
  "files": [
    {"path": "src/a.ts", "content_id": "<uuid4>", "operation": "upsert", "mode": "100644"},
    {"path": "old.txt",  "content_id": "<uuid4>", "operation": "delete"}
  ]
}}
```

The upstream spec names three of these fields differently and marks the names above
deprecated. open.storage accepts both. If a request sends both, the current name wins
whenever it is present, even when it is empty or `false`: `"expected_target_sha": ""`
drops a legacy guard, and `"target_is_ephemeral": false` overrides `"ephemeral": true`.

| Current name | Deprecated name |
|---|---|
| `expected_target_sha` | `expected_head_sha` |
| `target_is_ephemeral` | `ephemeral` |
| `base_is_ephemeral` | `ephemeral_base` |

Lines 2..N — one or more blob chunk frames per `upsert` file, in order:

```json
{"blob_chunk": {"content_id": "<uuid4>", "data": "<base64>", "eof": false}}
{"blob_chunk": {"content_id": "<uuid4>", "data": "<base64>", "eof": true}}
```

- `content_id` ties chunks to the metadata `files` entry (UUIDv4 in practice; treat as
  an opaque unique string).
- The final chunk per file sets `eof: true`. Empty files send exactly one frame:
  `{"data": "", "eof": true}`. A stream that ends before a file's `eof` chunk, or that
  sends no chunk for an `upsert` file, gets 412 `precondition_failed`. Other malformed
  frames (bad JSON, unknown frames, chunks out of order or after `eof`, bad base64)
  get 400 `invalid`.
- Clients normalize paths (leading slashes stripped); the backend should still reject
  `..` traversal and absolute paths defensively.
- Modes: `100644`, `100755`, `120000` (symlink), `160000` (submodule).

### Server semantics

1. Resolve the base: `expected_head_sha` / current tip of `target_branch`. If the branch
   does not exist and `base_branch` is given, branch from that tip; otherwise create a
   root commit.
2. CAS check: a stale `expected_target_sha` (or the deprecated `expected_head_sha`) gets
   HTTP 409 and moves no ref. The body is the failure envelope (`result.status`
   `conflict`) plus the problem members `code: ref_guard_failed`, `guard` and
   `current_sha`, sent as `application/problem+json`
   ([details](../reference/http-api.md#commit-write-guards)).
3. Write blobs, build the tree on the base tree (upserts + deletes), create the commit,
   and atomically advance the ref (respecting JWT ref policies). If the new tree equals
   the base tree (or the empty tree on an unborn branch), the server answers 412
   `precondition_failed` with the message `no changes to commit` and moves no ref. This
   covers a request without file operations, files that match the tip, and deletes of
   paths that do not exist.
4. Emit a `push` webhook and schedule upstream sync (persistent namespace only).

## 2. `POST /api/v1/repos/diff-commit` — commit from a git patch

Line 1 is the same metadata frame **without** `files`. Lines 2..N are diff chunk frames:

```json
{"diff_chunk": {"data": "<base64 of unified diff bytes>", "eof": true}}
```

The patch is a standard git unified diff (binary patches per official docs: output of
`git diff --binary`). The server applies it to the base tree and commits. A diff that
changes nothing, such as an empty diff (one `{"data": "", "eof": true}` frame), gets 412
`precondition_failed` with the message `no changes to commit`. A stream that ends before
the `eof` chunk gets 412 too, and a stream without any `diff_chunk` frame gets 400.

When the patch cannot be used, the answer depends on why:

- The payload is not a patch that git can parse (no diff, a corrupt hunk, a binary diff
  without its data): 400 `invalid`.
- The patch names a path no tree may hold (absolute, a `.` or `..` segment, a `.git`
  component): 400 `invalid`, with the same wording as `commit-pack`.
- The patch does not apply to the base tree (context that does not match, a file that is
  missing or already exists, a binary preimage that differs, a path beyond a symlink, a
  file where a directory is): 409 `conflict` with the message `patch does not apply: …`
  and git's own error. No ref moves.

Before [#608](https://github.com/coder/openstorage/issues/608), all of these answered 422
`invalid`.

## 3. Response envelope (shared, also used by restore-commit)

Success (HTTP 200):

```json
{
  "commit": {
    "commit_sha": "<40-hex>",
    "tree_sha": "<40-hex>",
    "target_branch": "main",
    "pack_bytes": 2048,
    "blob_count": 2            // commit-pack only
  },
  "result": {
    "target_branch": "main",
    "branch": "main",           // deprecated upstream, same value as target_branch
    "old_sha": "<40-hex or 40 zeros for root commit>",
    "new_sha": "<40-hex>",
    "success": true,
    "status": "ok",
    "message": "..."
  }
}
```

`result` is upstream's `RefUpdateResult`. Every `result` object sends `target_branch`,
`branch`, `old_sha`, `new_sha`, `success` and `status`. A member with no value is an
empty string.

Failure uses the same envelope with `commit: null` (except a lost CAS, below),
`result.success = false` and
`result.status` one of `precondition_failed, conflict, not_found, invalid, timeout,
unauthorized, forbidden, unavailable, payload_too_large, internal, failed`. For example, a
frame that is not a metadata frame answers 400:

```json
{
  "commit": null,
  "result": {
    "target_branch": "",
    "branch": "",
    "old_sha": "",
    "new_sha": "",
    "success": false,
    "status": "invalid",
    "message": "first frame must be a metadata frame"
  }
}
```

SDKs map these strings (or the numeric HTTP status) to typed `RefUpdateError`s with this
table:

| HTTP | status string |
|---|---|
| 400, 422 | `invalid` |
| 401 | `unauthorized` |
| 403 | `forbidden` |
| 404 | `not_found` |
| 408, 499, 504 | `timeout` |
| 409 | `conflict`; `failed` for a lost CAS on `commit-pack` and `diff-commit` (below) |
| 412 | `precondition_failed` |
| 413 | `payload_too_large` (`commit-pack`) |
| 429, 502, 503 | `unavailable` |
| 500 | `internal` |

When another writer moves the branch after the server wrote the commit (a lost CAS),
`commit-pack` and `diff-commit` answer 409 with `result.status: "failed"`. `commit` and
`result.new_sha` name the commit that the branch did not get, and `result.old_sha` is the
tip the server built on. `restore-commit` answers a lost CAS with 409 `conflict` and
`commit: null`. A stale guard also answers 409 on all three routes, and its envelope carries
the problem members of §1 step 2.

The handler returns the envelope as the JSON body on any of the statuses
`[400,401,403,404,408,409,412,422,429,499,500,502,503,504]`. Clients parse the body on
these codes rather than treating them as opaque failures. Auth failures (401, 403), the
gateway rate limiter's 429 and the path-style alias 404 happen before the handler: they
use the plain problem body of the [HTTP API](../reference/http-api.md#conventions), as
upstream documents.

## 4. `POST /api/v1/repos/restore-commit` — plain JSON, same ack

Not streamed. The body is `{"metadata": {"target_branch", "base_ref", "author",
"commit_message"?, "expected_target_sha"?, "committer"?}}`. `base_ref` is a branch, tag,
or commit. The deprecated `target_commit_sha` (a 40-hex SHA only) still works, and
`expected_head_sha` aliases `expected_target_sha` as in §1. The server creates a **new**
commit whose tree equals the `base_ref` commit's tree, parented on the current tip. A
`base_ref` restore answers 412 `precondition_failed` when the branch already points at
`base_ref`, when the tree would not change, or when `base_ref` is not an ancestor of the
tip. The deprecated `target_commit_sha` form skips these checks. History
is preserved; it is not a force reset. The response/error envelope is as above (without
`blob_count`).
