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
  before base64** (`4 * 1024 * 1024`), standard base64 alphabet.

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
deprecated. open.storage accepts both. If a request sends both, the current name wins.
A current boolean wins whenever it is present, so `"target_is_ephemeral": false`
overrides `"ephemeral": true`.

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
  `{"data": "", "eof": true}`.
- Clients normalize paths (leading slashes stripped); the backend should still reject
  `..` traversal and absolute paths defensively.
- Modes: `100644`, `100755`, `120000` (symlink), `160000` (submodule).

### Server semantics

1. Resolve the base: `expected_head_sha` / current tip of `target_branch`. If the branch
   does not exist and `base_branch` is given, branch from that tip; otherwise create a
   root commit.
2. CAS check (`expected_head_sha` mismatch → `status: "conflict"`, HTTP 409/412).
3. Write blobs, build the tree on the base tree (upserts + deletes), create the commit,
   and atomically advance the ref (respecting JWT ref policies).
4. Emit a `push` webhook and schedule upstream sync (persistent namespace only).

## 2. `POST /api/v1/repos/diff-commit` — commit from a git patch

Line 1 is the same metadata frame **without** `files`. Lines 2..N are diff chunk frames:

```json
{"diff_chunk": {"data": "<base64 of unified diff bytes>", "eof": true}}
```

The patch is a standard git unified diff (binary patches per official docs: output of
`git diff --binary`). The server applies it to the base tree and commits. An empty diff
is one `{"data": "", "eof": true}` frame.

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
    "branch": "main",
    "old_sha": "<40-hex or 40 zeros for root commit>",
    "new_sha": "<40-hex>",
    "success": true,
    "status": "ok",
    "message": "..."
  }
}
```

Failure uses the same envelope with `result.success = false` and `result.status` one of
`precondition_failed, conflict, not_found, invalid, timeout, unauthorized, forbidden,
unavailable, internal, failed`. SDKs map these strings (or the numeric HTTP status) to
typed `RefUpdateError`s with this table:

| HTTP | status string |
|---|---|
| 400, 422 | `invalid` |
| 401 | `unauthorized` |
| 403 | `forbidden` |
| 404 | `not_found` |
| 408, 499, 504 | `timeout` |
| 409 | `conflict` |
| 412 | `precondition_failed` |
| 429, 502, 503 | `unavailable` |
| 500 | `internal` |

The envelope must be returned as the JSON body on any of the statuses
`[400,401,403,404,408,409,412,422,429,499,500,502,503,504]`. Clients parse the body on
these codes rather than treating them as opaque failures.

## 4. `POST /api/v1/repos/restore-commit` — plain JSON, same ack

Not streamed. The body is `{"metadata": {"target_branch", "base_ref", "author",
"commit_message"?, "expected_target_sha"?, "committer"?}}`. `base_ref` is a branch, tag,
or commit. The deprecated `target_commit_sha` (a 40-hex SHA only) still works, and
`expected_head_sha` aliases `expected_target_sha` as in §1. The server creates a **new**
commit whose tree equals the `base_ref` commit's tree, parented on the current tip. History
is preserved; it is not a force reset. The response/error envelope is as above (without
`blob_count`).
