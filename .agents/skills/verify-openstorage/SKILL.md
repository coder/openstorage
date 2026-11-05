---
name: verify-openstorage
description: Drive a real, disposable open.storage deployment the way a user does and keep the proof. By default one `openstorage serve` listener in local mode (SQLite + file blobs, no containers); with `--fleet`, two serve replicas on shared PostgreSQL + SeaweedFS (S3) containers. Each node serves the REST API (`/api/v1/...`) and Git smart HTTP (`/<repo>.git`); the helper builds serve from this checkout, starts it on free loopback ports, drives it with curl and real git, and tears everything down. Use it to verify REST or Git behavior end to end, to check that replicas share state, to reproduce a bug against a live server, or to collect evidence for a PR.
---

# Verify open.storage

`scripts/osv.sh` builds `openstorage` from this checkout, starts one local-mode node with an empty data directory, drives it with `curl` and real `git`, records every request and result, and keeps the evidence after teardown. With `launch --fleet` it starts two nodes on shared PostgreSQL and SeaweedFS containers instead (see [Fleet mode](#fleet-mode)). The feature recipes are in [`features/README.md`](features/README.md). Read it before you drive a feature.

The node is the real product: the same binary, the same `serve` command, and the same auth path as a self-hosted install. Nothing is mocked. The map does not cover LFS, webhooks, upstream sync, or S3WAL yet.

Requirements:

- bash on Linux or macOS, plus `go` (the `go` line in `go.mod`), `git` 2.32 or newer (older git ignores `GIT_CONFIG_GLOBAL` and reads your global config, so `launch` and `git` refuse it), `curl`, and `python3` on `PATH`. The repo's own smoke script needs the same tools. The helper is tested on Linux only. Its macOS branches (`lsof`, `ps`, no `setsid`) are untested.
- Fleet mode only: a reachable Docker daemon (`docker info` must work). The images are the digest-pinned `postgres` and `chrislusf/seaweedfs` references in the repo's `docker-compose.yml`, pulled on first use.
- Run the commands from the repository root.
- Choose a new run directory for every run, outside the checkout. `launch` refuses a directory inside the checkout (it would dirty `git status`) and a directory that is not empty. It sets the run directory to mode 700, and every command refuses a run directory (or its `work/`) that another user owns or that group or others can write, because the commands source `work/state.env`. Every command also refuses a run directory with a parent that another user owns (root is allowed) or that group or others can write without the sticky bit, because that user could swap the run directory. `/tmp` passes: its sticky bit stops other users from renaming your entries.

## Launch

```sh
OSV="$PWD/.agents/skills/verify-openstorage/scripts/osv.sh"
RUN=$(mktemp -d "${TMPDIR:-/tmp}/osv-run-XXXXXX")   # empty, outside the checkout
"$OSV" launch "$RUN"
eval "$("$OSV" env "$RUN")"   # exports OSV_RUN OSV_URL OSV_BIN OSV_WORK OSV_EVIDENCE OSV_ORG
echo "evidence: $OSV_EVIDENCE"
```

If your machine cleans `/tmp`, or you need the evidence after this session, create `RUN` under a durable directory instead, such as your agent's scratch or artifacts directory. `launch` takes any empty or absent directory outside the checkout.

`launch` does these steps and stops at the first failure:

1. Builds `go build -o $RUN/work/bin/openstorage ./cmd/openstorage` from the checkout. A cold build takes about 30 s.
2. Generates an org key pair with `openstorage keygen --out $RUN/work/keys/org`.
3. Starts `openstorage serve --listen 127.0.0.1:0 --data-dir $RUN/work/data` in its own session, with an empty environment except `PATH`, a private `HOME` and `TMPDIR`, `OPENSTORAGE_ORG=selfhost`, and `OPENSTORAGE_ORG_PUBKEYS`. An inherited `OPENSTORAGE_PG_URL` or `OPENSTORAGE_S3_URL` would silently replace the `--data-dir` stores, and your own `HOME` would give the server's git your global git config.
4. Waits up to 60 s for the line `listening on 127.0.0.1:<port>` on serve's stdout. The kernel picks the port.
5. Runs `doctor`. `launch` exits 0 only when doctor passes.

Ready means usable: doctor's authenticated `GET /api/v1/repos` answers 200. That proves the SQLite migrations ran, the org key loaded, and token verification works. `/healthz` alone only proves that a process answers.

The data directory starts empty. There is no seed data. Each feature recipe creates the repos it needs, and [`features/README.md`](features/README.md) has the shared baseline setup.

Two runs can work side by side: each run has its own port, data directory, keys, and tokens. Never drive a node that you did not launch, such as another agent's run or a developer's `serve` on port 8080.

Teardown is `"$OSV" cleanup "$RUN"` (see [Cleanup](#cleanup)).

### Fleet mode

```sh
"$OSV" launch "$RUN" --fleet
eval "$("$OSV" env "$RUN")"   # also exports OSV_URL_B
```

`--fleet` replaces step 3 above with these steps:

1. Starts a PostgreSQL container and a SeaweedFS container (`weed server -s3`). Each carries the label `dev.openstorage.osv.run=<run dir>` and publishes its port on a free `127.0.0.1` port. The PostgreSQL password and the S3 secret key are random for each run and stay in `$RUN/work/secrets/`, so another local user who reaches the ports cannot use the stores. Cleanup replaces them with `<secret:pg>` and `<secret:s3>` in the evidence. `launch` refuses a run directory that already has containers with its label.
2. Waits up to 90 s until PostgreSQL answers a TCP query, and up to 120 s until SeaweedFS stores an object in the probe bucket `osv-ready`. A fresh SeaweedFS can hold its first write for 10 to 15 s, so a passing `/healthz` alone is not ready.
3. Starts node a, waits for its address, then starts node b. Both get `OPENSTORAGE_PG_URL` and `OPENSTORAGE_S3_URL` for the shared stores (bucket `openstorage`, which node a creates) and their own `--cache-dir` (`$RUN/work/cache-a`, `$RUN/work/cache-b`). Node a runs the migrations before node b boots.

`OSV_URL` is node a and `OSV_URL_B` is node b. `api` calls go to node a unless you pass `--node b`. git picks the node from the URL in its arguments. `"$OSV" stop "$RUN" b` stops node b for a recipe that checks the other node alone. A fleet launch takes about 20 s with cached images. The recipe for cross-node behavior is [`features/replicas.md`](features/replicas.md). The other recipes run unchanged on node a.

## Doctor

```sh
"$OSV" doctor "$RUN"
```

Doctor is read-only. Run it first whenever a result looks wrong. It prints one line per check and appends the same lines to `$RUN/evidence/doctor.txt`. It exits 1 when any check fails.

| Check | PASS means |
|---|---|
| `process` | The saved PID is alive and runs `$RUN/work/bin/openstorage`. |
| `port` | That PID owns the listening socket (`ss`, else `lsof`; SKIP when neither is installed). |
| `healthz` | `GET /healthz` answers `{"status":"ok"}`. |
| `auth+db` | A fresh `org:read` token gets 200 from `GET /api/v1/repos`. |
| `build` | `openstorage version` names the checkout's `HEAD` commit. A `WARN build ... vcs.modified=true` line (read with `go version -m`) means the binary includes uncommitted changes. |
| `data-dir` | `$RUN/work/data/codestorage.db` exists. Local mode only. |

In fleet mode, doctor runs `process`, `port`, `healthz`, and `auth+db` once per node, as `process:a`, `process:b`, and so on. `auth+db` then proves that PostgreSQL holds the migrated schema. Two checks replace `data-dir`:

| Check | PASS means |
|---|---|
| `containers` | Exactly two containers carry this run's label, and both run. |
| `bucket` | A signed `HEAD` of bucket `openstorage` on the SeaweedFS port answers 200. |

A `build` FAIL means the checkout moved after launch. Clean up and launch a new run.

## Drive

`api`, `git`, and `eq` take the run directory first and a label second. The label names the evidence files, so it must match `[A-Za-z0-9._-]+` and be unique in the run across all three commands: a label already in `log.tsv` is refused. `token` takes the run directory and a token name (`[A-Za-z0-9_-]+`, no dots), and `json` takes a file and a field path.

Mint tokens with the run's org key. The flags after the name go to `openstorage mint` (`--repo`, `--scopes`, `--ttl`, `--refs`):

```sh
"$OSV" token "$RUN" create --repo demo --scopes repo:write
"$OSV" token "$RUN" rw     --repo demo --scopes git:read,git:write
"$OSV" token "$RUN" ro     --repo demo --scopes git:read
"$OSV" token "$RUN" org    --repo org  --scopes org:read
"$OSV" token "$RUN" junk   --raw not-a-jwt      # stores a literal value, for invalid-token checks
```

Tokens stay in `$RUN/work/tokens/`. The commands below take a token name, never the value. `--token none` sends no credentials.

The blocks below run in order on a fresh run and leave repo `demo` with one commit.

Call the REST API or the Git HTTP endpoints with curl:

```sh
"$OSV" api "$RUN" create-repo --token create --expect 201 \
  POST /api/v1/repos -H 'Content-Type: application/json' -d '{"default_branch":"main"}'
REPO_ID=$("$OSV" json "$OSV_EVIDENCE/create-repo.body" repo_id)   # one field of a saved body
```

- The path is appended to `$OSV_URL` and must start with `/`. Arguments after the path go to curl unchanged.
- `--node b` sends the request to node b of a fleet run. `log.tsv` and `<label>.request.txt` name the node. In local mode `--node b` is refused.
- `/api/...` paths get `Authorization: Bearer <token>`. Every other path is the Git surface and gets Basic auth with the user `t` and the token as the password.
- stdout is the HTTP status code. With `--expect <code>`, a different code prints the first 400 bytes of the body to stderr and exits 1.
- Write each request body to `$OSV_EVIDENCE/<label>.ndjson` (or `.json`) first, and send it with `--data-binary @file`. Then the body is part of the proof.

Run real git, isolated from your git config:

```sh
"$OSV" git "$RUN" clone --token rw -- clone -q "$OSV_URL/demo.git" clone
printf 'hello\n' >"$OSV_WORK/clone/hello.txt"
"$OSV" git "$RUN" add -- -C clone add hello.txt
"$OSV" git "$RUN" commit -- -C clone commit -q -m 'Add hello'
"$OSV" git "$RUN" push --token rw -- -C clone push origin HEAD:refs/heads/main
"$OSV" git "$RUN" push-ro --token ro --expect 128 -- -C clone push origin HEAD:refs/heads/x
```

- git runs in `$OSV_WORK`, so relative paths such as `clone` resolve there, never in the checkout.
- The helper removes inherited `GIT_*` variables. It sets `GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_SYSTEM=/dev/null`, and `GIT_CONFIG_NOSYSTEM=1`, pins the author and committer to `osv <osv@example.invalid>`, sets `LC_ALL=C` (English prompts and messages), turns off the credential helper and background maintenance, and answers git's credential prompt through `scripts/askpass.sh` (user `t`, the token as the password). The token never enters the remote URL.
- stdout is git's combined stdout and stderr. `--expect <exit>` (default `0`) sets the expected exit code. A different exit code returns 1.

Read the result through a second view and record the comparison:

```sh
"$OSV" api "$RUN" read-file --token rw --expect 200 \
  GET /api/v1/repos/file -G --data-urlencode path=hello.txt --data-urlencode ref=main
"$OSV" api "$RUN" tip --token rw --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" rest-vs-clone "$("$OSV" git "$RUN" clone-head -- -C clone rev-parse HEAD)" \
  "$("$OSV" json "$OSV_EVIDENCE/tip.body" branch.head_sha)"
```

`eq` takes the expected value first. It exits 1 on a mismatch, and also when the expected value is empty, so a failed lookup never passes.

Check stored packs with `"$OSV" packs "$RUN" "$REPO_ID"`. It runs `openstorage list-packs` on the data directory in local mode and on the shared bucket in fleet mode. In local mode, run other read-only CLI checks with `env -i PATH="$PATH" "$OSV_BIN" <command> --data-dir "$OSV_WORK/data"`. Fleet mode has no CLI access to the stores apart from `packs`: its store secrets never leave `$RUN/work/secrets/`, so prove fleet state through the nodes' REST and Git surfaces.

## Evidence

Evidence goes to `$RUN/evidence/` (`$OSV_EVIDENCE`). Cleanup never deletes it.

| File | Content |
|---|---|
| `log.tsv` | One row per `api`, `git`, and `eq` call: time, label, request, token name, result, expected result, and verdict (`PASS`, `FAIL`, or `-` when nothing was expected). |
| `<label>.request.txt`, `<label>.headers`, `<label>.body` | The API request (method, URL, token name and claims, curl arguments), response headers, and response body. |
| `<label>.git.txt` | The git command, its working directory, the token, the output, and the exit code. |
| `<label>.eq.txt` | Expected value, actual value, and verdict. |
| `launch.txt`, `build.txt`, `doctor.txt` | Checkout commit, tool versions, binary version, PID, address, and every doctor and cleanup result. |
| `serve.stdout.log`, `serve.stderr.log`, `state.env`, `tokens.txt` | Server logs, the address and PID, and each token's mint flags. Cleanup copies them here. In fleet mode `serve.*` is node a and `serve-b.*` is node b. |
| `container-postgres-<id>.log`, `container-seaweedfs-<id>.log` | Fleet mode: the last 5,000 lines of each container's log, saved by cleanup before it removes the container. |

Proof standards:

- Use the real user paths: `/api/v1` with a minted token, and Git smart HTTP with real `git`. Do not use `OPENSTORAGE_TEST_*` knobs, in-process test servers, or direct SQLite or blob-store edits.
- Capture the action and the resulting state through a second view. A REST write is proved by a fresh `git clone` or `git ls-remote`. A `git push` is proved by REST reads. Compare the commit SHAs with `osv.sh eq`.
- Check side effects as well as status codes: file bytes in a fresh clone, the branch tip in `GET /api/v1/repos/branch`, and new pack files from `openstorage list-packs`.
- Record each denied request with `--expect`, so a 401 or 403 is proof and not a silent failure.
- Report a failed step with its label and evidence files. Do not report an entry point as verified through a different entry point.
- serve logs `/api` requests (method, path, status) on stderr. It does not log Git smart-HTTP requests, so prove Git behavior from git's exit code and output.
- The helper does not write token values to evidence. One exception: a failed `api` call keeps curl's stderr, and with `-v` that shows the `Authorization` header. Cleanup replaces every stored token value in the evidence with `<token:name>`, and its Basic-auth form `base64("t:<token>")` with `<basic:name>`. Tokens expire after 1 hour (`mint --ttl`), so mint a new one for a long run.

## Cleanup

```sh
"$OSV" cleanup "$RUN"
```

Run cleanup after every run, also after a failed launch or a failed drive. It:

1. Checks that each saved serve PID (node b first in fleet mode, then node a) runs this run's binary. It never signals any other process: a node that exited is skipped, also when its PID now belongs to another process.
2. Records the PIDs of serve's descendants, sends SIGTERM to serve (the drain takes up to 10 s), waits up to 30 s, then sends SIGKILL to serve and the recorded descendants that are still alive.
3. Fleet mode: stages each labeled container's log under `$RUN/work/tmp`, then removes the container and its anonymous volumes with `docker rm -f -v`. It finds the containers by this run's label, never by name or image, so it touches no other container. Step 5 copies the staged logs.
4. Looks for leftovers: a recorded process or labeled container that is still alive, or (on Linux) any process whose working directory is under `$RUN/work`. When it finds one, it prints `LEFTOVER processes: <pid> ...` (a container shows as `container:<id>`), keeps `$RUN/work`, and exits 1.
5. Copies the serve logs, the staged container logs, `state.env`, and the token list into the evidence, and replaces token values (and, in fleet mode, store secrets) in every regular file under the evidence, subdirectories included. It never writes through a link. At each fixed evidence path (`serve*.log`, `state.env`, `tokens.txt`, `container-*.log`, `doctor.txt`) it refuses a symlink, a second hard link, and a file that is not a regular file. It also refuses a symlink where it reads a copy source or a token or secret file, and a symlink in place of the `evidence` directory. The redaction does not rewrite a file that has a second hard link, because the write would change the other name too. A refusal or a path it cannot read or write does not stop the rest: it copies and redacts everything else first. Then it names each path, keeps `$RUN/work`, and exits 1. Remove the link or fix the mode, then run cleanup again. It also does this when it stops early: at a leftover or on an error. `api`, `git`, `eq`, and `stop` refuse a symlink at `log.tsv` and `doctor.txt` the same way. `api`, `git`, and `eq` check `log.tsv` before they send a request or run git, and they refuse a symlink at their own `<label>` files.
6. Deletes `$RUN/work`: the binary, keys, tokens, data directory, caches, and clones.
7. Prints `no process or container from this run is alive`, the evidence path, and the PASS and FAIL counts from `log.tsv`.

After a `LEFTOVER` line, check each PID before you act. Your own shell is a common one: `cd` out of the run directory. Kill only a PID that you started, never by process name (`pkill`, `killall`), then run cleanup again. Cleanup is safe to repeat. A cleaned run directory cannot launch again, so use a new directory for the next run.

## Helpers

| Command | Purpose |
|---|---|
| `scripts/osv.sh launch <run> [--fleet]` | Build, generate keys, start serve (with `--fleet`: the containers and two nodes), wait for ready, run doctor. |
| `scripts/osv.sh doctor <run>` | Read-only health check (see [Doctor](#doctor)). |
| `scripts/osv.sh env <run>` | Print `export OSV_MODE=... OSV_RUN=... OSV_URL=... OSV_BIN=... OSV_WORK=... OSV_EVIDENCE=... OSV_ORG=...` for `eval`, plus `OSV_URL_B` in fleet mode. |
| `scripts/osv.sh token <run> <name> <mint flags>` | Mint a token with the run's org key (`--iss selfhost` is preset). A later `--key <pem>` replaces the run's key, for wrong-key checks. |
| `scripts/osv.sh token <run> <name> --raw <value>` | Store a literal token value. |
| `scripts/osv.sh api <run> <label> [--node a\|b] [--token <name>\|none] [--expect <code>] <METHOD> <path> [curl args]` | One HTTP request, recorded. |
| `scripts/osv.sh git <run> <label> [--token <name>\|none] [--expect <exit>] -- <git args>` | One hermetic git command in `$OSV_WORK`, recorded. |
| `scripts/osv.sh eq <run> <label> <expected> <actual>` | Record an equality check. Exits 1 on a mismatch or an empty expected value. |
| `scripts/osv.sh json <file> <dotted.path>` | Print one JSON field, for example `commits.0.sha`. A `*` segment maps a list: `commits.*.sha` prints a JSON list. |
| `scripts/osv.sh packs <run> <repo id>` | List the repo's stored packs (read-only), in either mode. |
| `scripts/osv.sh stop <run> a\|b` | Stop one node of the run. Exits 1 when a process of that node is still alive. |
| `scripts/osv.sh cleanup <run>` | Stop serve and remove the run's containers, delete `<run>/work`, keep `<run>/evidence`. |
| `scripts/askpass.sh` | The `GIT_ASKPASS` program that `osv.sh git` uses. Do not call it yourself. |
| `scripts/test-cleanup.sh` | Check cleanup's evidence handling (planted links, unreadable paths, container logs) on made-up run directories. It needs no build, server, or Docker. Run it after you change `osv.sh`'s cleanup or evidence code. `OSV=<path>` checks another copy of `osv.sh`. |

`scripts/osv.sh help` prints the same summary.

## Keeping the map current

When a route, flag, status code, or response field changes, update the matching file in [`features/`](features/README.md) in the same change. When you add a user-facing feature, add a feature file and list it in the index.
