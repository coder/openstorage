# Commit through the REST API

A user commits files to a branch without git: `POST /api/v1/repos/commit-pack` takes one NDJSON metadata frame plus base64 blob chunks and answers with the new commit. The new commit is then visible in REST reads and in a fresh git clone, and the server refuses a commit that changes nothing or that was built on a stale tip.

## Sub-features

- `cp-upsert` adds `hello.txt` on `main`. The answer is `200` with `commit.commit_sha`, and `result.old_sha` is the previous tip.
- `cp-readback-api` shows the new bytes, blob SHA, tip, and commit message through REST.
- `cp-readback-git` shows the same commit and bytes in a fresh clone, with the old tip as its parent.
- `cp-pack` stores one new pack file for the repo.
- `cp-no-change` answers `412` `precondition_failed` with `no changes to commit` for a commit that changes nothing, and leaves the tip alone.
- `cp-stale-guard` answers `409` `ref_guard_failed` for a stale `expected_target_sha`, with `current_sha` = the tip, and leaves the tip alone.
- `cp-new-branch` creates `feature` from `main` with `base_branch` and deletes a path with `operation: "delete"`.
- `cp-unborn` makes the first commit of an empty repo (`result.old_sha` is 40 zeros).

## How to get to it (user POV)

- `POST /api/v1/repos/commit-pack` with a `git:write` token, `Content-Type: application/x-ndjson`, and the frames in the body.
- The path-style alias `POST /api/repos/{repo_name}/commit-pack`.
- The SDK commit builders send the same frames. This map drives the HTTP call directly.
- The README section "Write with the API, clone with Git" shows the same request with `curl`.

## Driving it with osv.sh

Preconditions:

- The baseline from [`README.md`](README.md#baseline-preconditions) ran in this shell: `demo` has `Seed commit` on `main`, and `REPO_ID`, `SEED`, and the `rw` token exist.
- doctor passes.

- **Record the state before.** Read the tip and list the stored packs. `branch.head_sha` is `SEED`.

```sh
"$OSV" api "$RUN" cp-tip-before --token rw --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" cp-tip-before-is-seed "$SEED" "$("$OSV" json "$OSV_EVIDENCE/cp-tip-before.body" branch.head_sha)"
env -i PATH="$PATH" "$OSV_BIN" list-packs --repo "$REPO_ID" --data-dir "$OSV_WORK/data" >"$OSV_EVIDENCE/cp-packs-before.txt"
```

- **Commit a file.** Write the frames, then send them. The status is `200`, `result.status` is `ok`, and `result.old_sha` is `SEED`.

```sh
cat >"$OSV_EVIDENCE/cp-upsert.ndjson" <<'NDJSON'
{"metadata":{"target_branch":"main","commit_message":"Add greeting","author":{"name":"Demo","email":"demo@example.com"},"files":[{"path":"hello.txt","content_id":"greeting","operation":"upsert"}]}}
{"blob_chunk":{"content_id":"greeting","data":"SGVsbG8gZnJvbSB0aGUgQVBJIQo=","eof":true}}
NDJSON
"$OSV" api "$RUN" cp-upsert --token rw --expect 200 POST /api/v1/repos/commit-pack \
  -H 'Content-Type: application/x-ndjson' -H 'Accept: application/json' \
  --data-binary "@$OSV_EVIDENCE/cp-upsert.ndjson"
NEW=$("$OSV" json "$OSV_EVIDENCE/cp-upsert.body" commit.commit_sha)
"$OSV" eq "$RUN" cp-result-ok ok "$("$OSV" json "$OSV_EVIDENCE/cp-upsert.body" result.status)"
"$OSV" eq "$RUN" cp-old-sha "$SEED" "$("$OSV" json "$OSV_EVIDENCE/cp-upsert.body" result.old_sha)"
"$OSV" eq "$RUN" cp-new-sha "$NEW" "$("$OSV" json "$OSV_EVIDENCE/cp-upsert.body" result.new_sha)"
```

- **Read it back through REST.** Read the file, the branch, and the commit list. The file body is `Hello from the API!`, `X-Last-Commit-Sha` is `NEW`, `X-Blob-Sha` is the git blob SHA of the body, the branch tip is `NEW`, and the newest commit message is `Add greeting`.

```sh
"$OSV" api "$RUN" cp-file --token rw --expect 200 \
  GET /api/v1/repos/file -G --data-urlencode path=hello.txt --data-urlencode ref=main
"$OSV" eq "$RUN" cp-file-bytes 'Hello from the API!' "$(cat "$OSV_EVIDENCE/cp-file.body")"
"$OSV" eq "$RUN" cp-file-commit "$NEW" "$(sed -n 's/^X-Last-Commit-Sha: //p' "$OSV_EVIDENCE/cp-file.headers" | tr -d '\r')"
"$OSV" eq "$RUN" cp-file-blob "$("$OSV" git "$RUN" cp-hash-body -- hash-object "$OSV_EVIDENCE/cp-file.body")" \
  "$(sed -n 's/^X-Blob-Sha: //p' "$OSV_EVIDENCE/cp-file.headers" | tr -d '\r')"
"$OSV" api "$RUN" cp-tip-after --token rw --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" cp-tip-after-is-new "$NEW" "$("$OSV" json "$OSV_EVIDENCE/cp-tip-after.body" branch.head_sha)"
"$OSV" api "$RUN" cp-commits --token rw --expect 200 GET '/api/v1/repos/commits?ref=main'
"$OSV" eq "$RUN" cp-commits-newest "$NEW" "$("$OSV" json "$OSV_EVIDENCE/cp-commits.body" commits.0.sha)"
"$OSV" eq "$RUN" cp-commits-message 'Add greeting' "$("$OSV" json "$OSV_EVIDENCE/cp-commits.body" commits.0.message)"
```

- **Read it back through git.** Clone into a new directory. `HEAD` is `NEW`, `HEAD^` is `SEED`, and `hello.txt` has the same bytes as the REST body.

```sh
"$OSV" git "$RUN" cp-clone --token rw -- clone -q "$OSV_URL/demo.git" cp-clone
"$OSV" eq "$RUN" cp-clone-head "$NEW" "$("$OSV" git "$RUN" cp-clone-sha -- -C cp-clone rev-parse HEAD)"
"$OSV" eq "$RUN" cp-clone-parent "$SEED" "$("$OSV" git "$RUN" cp-clone-parent-sha -- -C cp-clone rev-parse 'HEAD^')"
"$OSV" eq "$RUN" cp-clone-bytes same \
  "$(cmp -s "$OSV_EVIDENCE/cp-file.body" "$OSV_WORK/cp-clone/hello.txt" && echo same || echo different)"
```

- **Check the stored pack.** List the packs again. There is exactly one more pack than before.

```sh
env -i PATH="$PATH" "$OSV_BIN" list-packs --repo "$REPO_ID" --data-dir "$OSV_WORK/data" >"$OSV_EVIDENCE/cp-packs-after.txt"
"$OSV" eq "$RUN" cp-one-new-pack "$(($(grep -c '\.pack' "$OSV_EVIDENCE/cp-packs-before.txt") + 1))" \
  "$(grep -c '\.pack' "$OSV_EVIDENCE/cp-packs-after.txt")"
```

- **Refuse a commit with no changes.** Send the same frames again. The status is `412`, `result.message` is `no changes to commit`, and the tip is still `NEW`.

```sh
"$OSV" api "$RUN" cp-no-change --token rw --expect 412 POST /api/v1/repos/commit-pack \
  -H 'Content-Type: application/x-ndjson' --data-binary "@$OSV_EVIDENCE/cp-upsert.ndjson"
"$OSV" eq "$RUN" cp-no-change-message 'no changes to commit' "$("$OSV" json "$OSV_EVIDENCE/cp-no-change.body" result.message)"
```

- **Refuse a stale guard.** Send a commit whose `expected_target_sha` is `SEED`. The status is `409`, `code` is `ref_guard_failed`, `current_sha` is `NEW`, `result.status` is `conflict`, and the tip is still `NEW`.

```sh
sed "s/@SEED@/$SEED/" >"$OSV_EVIDENCE/cp-stale.ndjson" <<'NDJSON'
{"metadata":{"target_branch":"main","commit_message":"Stale","author":{"name":"Demo","email":"demo@example.com"},"expected_target_sha":"@SEED@","files":[{"path":"stale.txt","content_id":"s","operation":"upsert"}]}}
{"blob_chunk":{"content_id":"s","data":"c3RhbGUK","eof":true}}
NDJSON
"$OSV" api "$RUN" cp-stale --token rw --expect 409 POST /api/v1/repos/commit-pack \
  -H 'Content-Type: application/x-ndjson' --data-binary "@$OSV_EVIDENCE/cp-stale.ndjson"
"$OSV" eq "$RUN" cp-stale-code ref_guard_failed "$("$OSV" json "$OSV_EVIDENCE/cp-stale.body" code)"
"$OSV" eq "$RUN" cp-stale-current "$NEW" "$("$OSV" json "$OSV_EVIDENCE/cp-stale.body" current_sha)"
"$OSV" eq "$RUN" cp-stale-status conflict "$("$OSV" json "$OSV_EVIDENCE/cp-stale.body" result.status)"
"$OSV" api "$RUN" cp-tip-final --token rw --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" cp-tip-unchanged "$NEW" "$("$OSV" json "$OSV_EVIDENCE/cp-tip-final.body" branch.head_sha)"
```

- **Create a branch and delete a path.** Commit to the new branch `feature` from `main` and delete `README.md`. The status is `200`, `result.old_sha` is 40 zeros, and the `feature` tree lists only `hello.txt`.

```sh
cat >"$OSV_EVIDENCE/cp-branch.ndjson" <<'NDJSON'
{"metadata":{"target_branch":"feature","base_branch":"main","commit_message":"Drop readme","author":{"name":"Demo","email":"demo@example.com"},"files":[{"path":"README.md","content_id":"d","operation":"delete"}]}}
NDJSON
"$OSV" api "$RUN" cp-branch --token rw --expect 200 POST /api/v1/repos/commit-pack \
  -H 'Content-Type: application/x-ndjson' --data-binary "@$OSV_EVIDENCE/cp-branch.ndjson"
"$OSV" eq "$RUN" cp-branch-old-sha 0000000000000000000000000000000000000000 "$("$OSV" json "$OSV_EVIDENCE/cp-branch.body" result.old_sha)"
"$OSV" api "$RUN" cp-branch-files --token rw --expect 200 GET '/api/v1/repos/files?ref=feature'
"$OSV" eq "$RUN" cp-branch-paths '["hello.txt"]' "$("$OSV" json "$OSV_EVIDENCE/cp-branch-files.body" paths)"
```

- **Commit to an empty repo.** Create `fresh` and commit to its unborn `main`. The status is `200` and `result.old_sha` is 40 zeros. A clone has `hello.txt`.

```sh
"$OSV" token "$RUN" fresh --repo fresh --scopes repo:write,git:read,git:write
"$OSV" api "$RUN" cp-fresh-create --token fresh --expect 201 POST /api/v1/repos -d '{"default_branch":"main"}'
"$OSV" api "$RUN" cp-fresh --token fresh --expect 200 POST /api/v1/repos/commit-pack \
  -H 'Content-Type: application/x-ndjson' --data-binary "@$OSV_EVIDENCE/cp-upsert.ndjson"
"$OSV" eq "$RUN" cp-fresh-root 0000000000000000000000000000000000000000 "$("$OSV" json "$OSV_EVIDENCE/cp-fresh.body" result.old_sha)"
"$OSV" git "$RUN" cp-fresh-clone --token fresh -- clone -q "$OSV_URL/fresh.git" fresh
"$OSV" eq "$RUN" cp-fresh-file 'Hello from the API!' "$(cat "$OSV_WORK/fresh/hello.txt")"
```

- **Proof.** `log.tsv` has a `PASS` row for every call above. `cp-upsert.ndjson` and `cp-upsert.body` are the request and the answer, `cp-clone-*.eq.txt` tie the REST commit to the clone, and `cp-packs-before.txt` and `cp-packs-after.txt` show the stored pack.

## Gotchas

- `data` is the base64 of the file bytes. `content_id` ties chunks to a `files` entry. An empty file sends one chunk, `{"data":"","eof":true}`. A `delete` sends no chunk.
- `target_branch` is a short name. A `refs/` prefix is refused.
- Write the frames with a quoted heredoc. An unquoted one lets the shell expand `$` inside the JSON.
- A stale `expected_target_sha` (or the deprecated `expected_head_sha`) answers HTTP `409` with `code` `ref_guard_failed`, `guard`, `current_sha` and `result.status` `conflict`. A `409` with `result.status` `failed` and no `code` means a lost race with another writer, which this recipe cannot force.
- Auth failures on commit-pack answer in the `result` envelope (`{"result":{"status":"forbidden",...}}`), not in the `{"error":...}` body of read endpoints.
- Without `committer`, the committer is the author.
- `list-packs` reads the blob store only. Run it with `env -i PATH="$PATH"` so an inherited `OPENSTORAGE_S3_URL` does not point it at another store.
