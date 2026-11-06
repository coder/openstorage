# Two replicas share one store

In fleet mode, two `serve` nodes share one PostgreSQL database and one S3 bucket, and each keeps only its own cache. A user can send any request to either node: a push or REST commit through one node is visible through the other at once, a stale guard on one node sees the other node's write, and one node keeps serving when the other stops.

## Sub-features

- `fl-clone-b` clones from node b a commit that was pushed through node a. Node b's cache never held the repo. The SHA and the file bytes match.
- `fl-read-b` shows node a's push through node b's REST reads: the branch tip and the file bytes.
- `fl-list-b` lists, through node b, the repo that node a created.
- `fl-commit-b` commits through node b with commit-pack. A fetch through node a gets the new commit, and the shared bucket holds one more pack.
- `fl-guard` answers `409` `ref_guard_failed` on node a for a commit built on the tip that node b replaced, with `current_sha` = node b's commit. The tip does not change.
- `fl-node-down` keeps node a serving clone, push, and REST after node b stops. Node b refuses connections.

## How to get to it (user POV)

- Each replica's base URL: `$OSV_URL` (node a) and `$OSV_URL_B` (node b). A load balancer would send each request to either one.
- REST: `osv.sh api --node b` sends the call to node b. Without `--node`, calls go to node a.
- Git: the remote URL picks the node, for example `$OSV_URL_B/demo.git`.
- The README section "Two replicas: Docker Compose with Postgres + SeaweedFS" shows the same setup with Docker Compose.

## Driving it with osv.sh

Preconditions:

- The run was launched with `"$OSV" launch "$RUN" --fleet`, and `eval "$("$OSV" env "$RUN")"` ran in this shell, so `OSV_URL_B` is set.
- The baseline from [`README.md`](README.md#baseline-preconditions) ran in this shell, through node a: `demo` has `Seed commit` on `main`, and `REPO_ID`, `SEED`, and the `rw` token exist.
- doctor passes, with every `:a` and `:b` check and the `containers` and `bucket` checks.

- **Clone through node b.** Clone `demo` from node b. `HEAD` is `SEED`, and `README.md` has the bytes that the seed clone pushed through node a.

```sh
"$OSV" git "$RUN" fl-clone-b --token rw -- clone -q "$OSV_URL_B/demo.git" clone-b
"$OSV" eq "$RUN" fl-clone-b-head "$SEED" "$("$OSV" git "$RUN" fl-clone-b-sha -- -C clone-b rev-parse HEAD)"
"$OSV" eq "$RUN" fl-clone-b-bytes same \
  "$(cmp -s "$OSV_WORK/seed/README.md" "$OSV_WORK/clone-b/README.md" && echo same || echo different)"
```

- **Read through node b.** Read the branch and the file through node b's REST API. The tip is `SEED` and the body is `hello from git`.

```sh
"$OSV" api "$RUN" fl-read-b-tip --node b --token rw --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" fl-read-b-tip-is-seed "$SEED" "$("$OSV" json "$OSV_EVIDENCE/fl-read-b-tip.body" branch.head_sha)"
"$OSV" api "$RUN" fl-read-b-file --node b --token rw --expect 200 \
  GET /api/v1/repos/file -G --data-urlencode path=README.md --data-urlencode ref=main
"$OSV" eq "$RUN" fl-read-b-file-bytes 'hello from git' "$(cat "$OSV_EVIDENCE/fl-read-b-file.body")"
```

- **List through node b.** List the organization's repos through node b. The list holds `demo`.

```sh
"$OSV" token "$RUN" org --repo org --scopes org:read
"$OSV" api "$RUN" fl-list-b --node b --token org --expect 200 GET /api/v1/repos
"$OSV" eq "$RUN" fl-list-b-has-demo yes \
  "$(grep -q '"demo"' "$OSV_EVIDENCE/fl-list-b.body" && echo yes || echo no)"
```

- **Commit through node b.** Record the stored packs and commit `hello.txt` to `main` through node b. The status is `200` and `result.old_sha` is `SEED`.

```sh
"$OSV" packs "$RUN" "$REPO_ID" >"$OSV_EVIDENCE/fl-packs-before.txt"
cat >"$OSV_EVIDENCE/fl-commit-b.ndjson" <<'NDJSON'
{"metadata":{"target_branch":"main","commit_message":"Add hello via node b","author":{"name":"Demo","email":"demo@example.com"},"files":[{"path":"hello.txt","content_id":"h","operation":"upsert"}]}}
{"blob_chunk":{"content_id":"h","data":"SGVsbG8gZnJvbSBub2RlIGIhCg==","eof":true}}
NDJSON
"$OSV" api "$RUN" fl-commit-b --node b --token rw --expect 200 POST /api/v1/repos/commit-pack \
  -H 'Content-Type: application/x-ndjson' --data-binary "@$OSV_EVIDENCE/fl-commit-b.ndjson"
NEW=$("$OSV" json "$OSV_EVIDENCE/fl-commit-b.body" commit.commit_sha)
"$OSV" eq "$RUN" fl-commit-b-old "$SEED" "$("$OSV" json "$OSV_EVIDENCE/fl-commit-b.body" result.old_sha)"
```

- **Refuse a stale guard on the other node.** Run this step before node a serves anything that node b wrote, so node a's cache still holds only `SEED`. Send a commit to node a whose `expected_target_sha` is `SEED`, the tip that node b replaced. The status is `409`, `code` is `ref_guard_failed`, `current_sha` is `NEW`, and node b still reads `NEW` as the tip.

```sh
sed "s/@SEED@/$SEED/" >"$OSV_EVIDENCE/fl-guard.ndjson" <<'NDJSON'
{"metadata":{"target_branch":"main","commit_message":"Stale on node a","author":{"name":"Demo","email":"demo@example.com"},"expected_target_sha":"@SEED@","files":[{"path":"stale.txt","content_id":"s","operation":"upsert"}]}}
{"blob_chunk":{"content_id":"s","data":"c3RhbGUK","eof":true}}
NDJSON
"$OSV" api "$RUN" fl-guard --token rw --expect 409 POST /api/v1/repos/commit-pack \
  -H 'Content-Type: application/x-ndjson' --data-binary "@$OSV_EVIDENCE/fl-guard.ndjson"
"$OSV" eq "$RUN" fl-guard-code ref_guard_failed "$("$OSV" json "$OSV_EVIDENCE/fl-guard.body" code)"
"$OSV" eq "$RUN" fl-guard-current "$NEW" "$("$OSV" json "$OSV_EVIDENCE/fl-guard.body" current_sha)"
"$OSV" api "$RUN" fl-guard-tip-b --node b --token rw --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" fl-guard-tip-unchanged "$NEW" "$("$OSV" json "$OSV_EVIDENCE/fl-guard-tip-b.body" branch.head_sha)"
```

- **Fetch through node a.** Fetch in the seed clone through node a, and list the packs again. The fetched `origin/main` is `NEW`, `hello.txt` has node b's bytes, and the shared bucket holds exactly one more pack.

```sh
"$OSV" git "$RUN" fl-fetch-a --token rw -- -C seed fetch -q "$OSV_URL/demo.git" main:refs/remotes/origin/main
"$OSV" eq "$RUN" fl-fetch-a-sha "$NEW" "$("$OSV" git "$RUN" fl-fetch-a-rev -- -C seed rev-parse refs/remotes/origin/main)"
"$OSV" eq "$RUN" fl-fetch-a-bytes 'Hello from node b!' \
  "$("$OSV" git "$RUN" fl-fetch-a-show -- -C seed show refs/remotes/origin/main:hello.txt)"
"$OSV" packs "$RUN" "$REPO_ID" >"$OSV_EVIDENCE/fl-packs-after.txt"
"$OSV" eq "$RUN" fl-one-new-pack "$(($(grep -c '\.pack' "$OSV_EVIDENCE/fl-packs-before.txt") + 1))" \
  "$(grep -c '\.pack' "$OSV_EVIDENCE/fl-packs-after.txt")"
```

- **Stop node b, keep using node a.** Run this step last: it ends node b for the rest of the run. Stop node b with `osv.sh stop` (SIGTERM, SIGKILL after 30 s). Node b then refuses connections (curl exit `7`). Node a still clones the repo at `NEW`, takes a push on top of it, and reports the pushed commit as the tip.

```sh
"$OSV" stop "$RUN" b
"$OSV" eq "$RUN" fl-down-b-refused curl-exit-7 \
  "$("$OSV" api "$RUN" fl-down-b-call --node b --token rw GET /api/v1/repo 2>/dev/null || true)"
"$OSV" git "$RUN" fl-down-clone-a --token rw -- clone -q "$OSV_URL/demo.git" clone-a
"$OSV" eq "$RUN" fl-down-clone-a-head "$NEW" "$("$OSV" git "$RUN" fl-down-clone-a-sha -- -C clone-a rev-parse HEAD)"
printf 'after node b stopped\n' >"$OSV_WORK/clone-a/after.txt"
"$OSV" git "$RUN" fl-down-add -- -C clone-a add after.txt
"$OSV" git "$RUN" fl-down-commit -- -C clone-a commit -q -m 'Push with node b down'
"$OSV" git "$RUN" fl-down-push --token rw -- -C clone-a push origin HEAD:refs/heads/main
"$OSV" api "$RUN" fl-down-tip --token rw --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" fl-down-tip-is-push "$("$OSV" git "$RUN" fl-down-push-sha -- -C clone-a rev-parse HEAD)" \
  "$("$OSV" json "$OSV_EVIDENCE/fl-down-tip.body" branch.head_sha)"
```

- **Proof.** `log.tsv` has a `PASS` row for every call above with an expected value. The `(node b)` rows ran on node b. `fl-clone-b-head.eq.txt` and `fl-fetch-a-sha.eq.txt` tie one node's write to the other node's read, `fl-packs-before.txt` and `fl-packs-after.txt` show the pack in the shared bucket, and `fl-guard.body` holds the cross-node refusal.

## Gotchas

- The baseline in [`README.md`](README.md#baseline-preconditions) runs through node a, because `$OSV_URL` is node a. Do not reuse a label from the baseline.
- doctor fails `process:b`, `port:b`, `healthz:b`, and `auth+db:b` after `fl-node-down`. That is the expected state. Run cleanup as usual: it skips the stopped node. `osv.sh stop` exits 1 when a process of the node is still alive.
- `fl-down-b-call` has no `--expect`, so its row in `log.tsv` reads `-`. `fl-down-b-refused` is the check. A curl exit other than `7` means node b still answers, or the address changed.
- `osv.sh packs` reads the shared bucket directly with the run's S3 credential. It is a read-only check, not a user path.
- `fl-guard` proves a cross-node read only while node a has not served node b's commit. Keep it before `fl-fetch-a`.
- `osv.sh git` takes no `--node`. The URL in the git arguments picks the node.
- Each node logs its `/api` requests on its own stderr: `serve.stderr.log` is node a, `serve-b.stderr.log` is node b. Use them to show which node served a REST call.
