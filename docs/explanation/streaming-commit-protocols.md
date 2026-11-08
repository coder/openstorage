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
  "commit_message": "msg",              // required; no NUL byte
  "author": {"name": "A", "email": "a@x"},       // required; name and email: no NUL byte, at most 131,051 bytes each
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
- The patch names a path no tree may hold (empty, absolute, a `.` or `..` segment, a `.git`
  component): 400 `invalid`, with the same wording as `commit-pack`.
- The patch gives a file a mode that no commit can hold, a directory mode (`new file mode
  040000`): 400 `invalid`, with the wording of `commit-pack`'s `invalid mode`. Git
  applies such a patch to the index without an error but writes no tree entry for it, so the
  server refuses the whole patch instead of committing without the file.
- Git refuses the patch on every base tree, although it parses: 400 `invalid` with git's
  own error. That covers a name that git's path rules reject (`.git.`, `git~1`, a
  `.gitmodules` symlink), a mode line that changes nothing git can record (`new mode
  100664`), a submodule patch without a commit or with a null one, and a patch that makes
  git abort on a failed assertion (git 2.55 does this when a header names a new file
  twice).
- The patch does not apply to the base tree (context that does not match, a file that is
  missing or already exists, a binary preimage that differs, a path beyond a symlink, a
  file where a directory is): 409 `conflict` with the message `patch does not apply: …`
  and git's own error. No ref moves.
- Git itself fails while it reads or applies a patch (a broken config, a held index lock, a
  full disk, an unreadable index): 500 with the generic `internal error` body. Git's message
  can name the server's scratch directory, so the server logs it and the client gets only
  the generic body. A message that names the scratch directory or the cached repository, or
  that has a `fatal:` line, answers 500 whatever the patch holds. No ref moves.
  Git copies parts of the patch into its messages: it prints a file name as it is, and
  `error: invalid mode` repeats the rest of the patch after the bad mode. So a patch can
  put a `fatal: …` line into the message of its own failure, with a file name that holds a
  newline or with text after an invalid mode. Then that 400 or 409 answers 500 instead,
  because the server cannot tell the forged line from git's. This is deliberate: a wrong
  status is better than a server path in a response. The server also runs git with its own
  scratch directory as `HOME` and `XDG_CONFIG_HOME`, so git does not read the server user's
  files and cannot warn about one.

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

A failure envelope (on the statuses listed at the end of this section) has the same
shape, with `commit: null` (except a lost CAS, below), `result.success = false` and
`result.status` naming the HTTP status: `invalid` (400), `conflict` or `failed` (409),
`precondition_failed` (412) or `payload_too_large` (413). For example, a frame that is not
a metadata frame answers 400:

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
tip the server built on. `restore-commit` answers a lost CAS with a 409 problem body
(code `conflict`), as below. A stale guard answers 409 on all three routes, with the
`result` envelope plus the problem members of §1 step 2.

The failure envelope is the body only on the statuses for which upstream documents it
([#352](https://github.com/coder/openstorage/issues/352) item 3):

| Route | Envelope statuses |
|---|---|
| `commit-pack` | 400, 409, 412, 413 |
| `diff-commit` | 400, 409, 412 |
| `restore-commit` | 412 |

Upstream also documents the envelope for a 502 on all three routes. These handlers
never answer 502, so the table leaves it out.

Every other failure, on these routes included, is the plain problem body of the
[HTTP API](../reference/http-api.md#conventions): auth failures (401, 403), 404 (with
`repository_not_found`, `branch_not_found` or `ref_not_found`), ref-policy and quota
403s, 422, 429, 500 and 503, and `restore-commit`'s 400 and 409 (except the stale-guard
409 above). The status strings
above still name the HTTP status in every envelope.

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
