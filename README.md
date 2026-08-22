# codestorage

An open-source, wire-compatible backend for [code.storage](https://code.storage), built from
the specification in [`docs/`](docs/README.md).

Current state: **phase 7 (deployment packaging + p6 burn-down) implemented**. One Go binary
(`codestorage serve`) exposes the `/api/v1` repo CRUD
API, the full `git:read` surface (files, branches, tags, commits, diff, blame, grep, notes,
archive — cursor pagination, conditional/range file streaming), the `git:write` surface
(NDJSON commit-pack / diff-commit streaming, restore-commit, merge + read-only preview,
branch / tag / notes writes — CAS and ref policies everywhere), the production git
smart-HTTP path (clone/push, ref-policy enforcement, commit-signature verification) behind
the same per-org JWT verifier, the eventing/sync surface (HMAC-signed `push` and
`repo.sync.*` webhooks, webhook + git-credentials CRUD, upstream `base_repo` mirror sync
with `pull-upstream`, synchronous forking), and the phase-6 additions: the Git LFS batch
API on the same git remote (presigned S3/MinIO transfers, server-proxied in local mode),
the named-repo path style `/api/repos/{repo_name}/...` as routing aliases,
`GET /api/repo-urls/{repo_id}`, and optional per-client-IP rate limiting. Background jobs
run through a queue seam — durable [River](https://riverqueue.com) jobs on Postgres
(tables migrated automatically at startup) or in-process in local mode — and nodes stay
stateless. Storage is selected by URL scheme: Postgres + S3 (MinIO) for multi-node
deployments — packaged as a multi-stage `Dockerfile` plus a two-replica compose profile
(quickstart below, decisions in docs/16) — or zero-dependency local mode (SQLite + plain
files via `serve --data-dir`).
Next: gate evidence for the p4 write surface (docs/13) —
see [docs/07-implementation-roadmap.md](docs/07-implementation-roadmap.md).

## Layout

| Path | Contents |
|---|---|
| `docs/` | Backend specification (wire formats, auth, protocols) + roadmap + ADRs |
| `cmd/codestorage` | Service binary: `serve`, `hook`, `keygen`, `mint`, `signing-key`, `list-packs` |
| `internal/api` | `/api/v1` HTTP handlers, JWT middleware, `{"error"}` envelope |
| `internal/auth` | JWT verification contract (per-org public keys, allowed algorithms) |
| `internal/meta` | Metadata store: orgs, org public keys, repos, webhooks, git credentials, upstream links |
| `internal/blob` | `BlobStore` contract + S3/MinIO and local-file implementations (immutable packs) |
| `internal/ref` | `RefStore` contract + Postgres and SQLite implementations (CAS ref authority) |
| `internal/gitread` | Read-side git plumbing (ls-tree, log, diff, blame, grep, archive) over the hydrated cache repo |
| `internal/githttp` | Git smart-HTTP server + LFS batch API, policy/verify-sig hooks, cache hydration, e2e suite |
| `internal/events` | Outbound webhooks: HMAC-signed delivery + the push-event `RefStore` decorator |
| `internal/sync` | Upstream mirror sync worker (fetch + loopback push), credential/upstream contracts |
| `internal/jobs` | Background queue seam: durable River jobs (Postgres) or in-process (local mode) |
| `docker-compose.yml` | Postgres + MinIO for local dogfooding, plus an opt-in two-replica `replicas` profile (`Dockerfile` image) |

## Quickstart

Requires Go, git, and Docker.

```sh
# 1. Infra: Postgres (refs + metadata) + MinIO (pack storage).
docker compose up -d --wait

# 2. Build and generate an org keypair (keys stay in /tmp, never commit them).
go build ./cmd/codestorage
./codestorage keygen --out /tmp/cs-key    # writes /tmp/cs-key.pem + /tmp/cs-key.pub.pem

# 3. Start a node: /api/v1 HTTP API + git smart-HTTP on one listener.
export CODESTORAGE_PG_URL='postgres://postgres:postgres@127.0.0.1:15432/postgres'
export CODESTORAGE_S3_URL='s3://codestorage?endpoint=http://127.0.0.1:19000&s3ForcePathStyle=true&region=us-east-1'
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
CODESTORAGE_ORG=selfhost CODESTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  ./codestorage serve --listen 127.0.0.1:8080 &
sleep 1 && curl -sf http://127.0.0.1:8080/healthz && kill -0 $!
# ^ catches a stale server squatting the port: serve exits with "address already
#   in use" behind `&`, and every later command would silently talk to the old
#   process (whose keys/repos don't match this shell's) — kill it first.

# 4. Mint JWTs with the org's private key (iss = org, repo = target repo or "org").
TOK_CREATE=$(./codestorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
TOK_READ=$(./codestorage mint   --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read)
TOK_ORG=$(./codestorage mint    --key /tmp/cs-key.pem --iss selfhost --repo org  --scopes org:read)

# 5. Repo CRUD (envelope + status codes per docs/03-http-api.md).
curl -s -X POST http://127.0.0.1:8080/api/v1/repos -H "Authorization: Bearer $TOK_CREATE" \
  -d '{"default_branch":"main"}'                                    # {"repo_id":...}
curl -s http://127.0.0.1:8080/api/v1/repo  -H "Authorization: Bearer $TOK_READ"
curl -s 'http://127.0.0.1:8080/api/v1/repos?limit=10&q=demo' -H "Authorization: Bearer $TOK_ORG"

# 6. Plain git against the same node.
TOKEN=$(./codestorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git"
cd demo && git commit --allow-empty -m init && git push origin HEAD:main

# 7. Optional: Git LFS on the same remote — no separate LFS server or config
#    (requires git-lfs; transfers are presigned MinIO URLs on this stack,
#    proxied through the server in local mode).
git lfs install --local && git lfs track '*.bin'
head -c 1M /dev/urandom > model.bin
git add . && git commit -m 'lfs object' && git push origin HEAD:main

# 8. Optional: register commit-signing keys (SSH public key line or armored PGP
#    block) for the `verify-sig` ref-policy op; hooks read them fresh per push.
./codestorage signing-key add --org selfhost --key ~/.ssh/id_ed25519.pub
./codestorage signing-key list --org selfhost

# 9. Clean up: soft delete + async storage cleanup. Note: this is last for a
#    reason — the name stays reserved (recreate → 409, clone → not found) until
#    cleanup completes, so don't delete a repo you're about to clone.
curl -s -X DELETE http://127.0.0.1:8080/api/v1/repos/delete -H "Authorization: Bearer $TOK_CREATE"
```

Host ports are env-overridable (`PGPORT`, `MINIOPORT`) — see the compose file header.

### Two-replica compose profile

Two stateless server replicas (`node-a`, `node-b`) share one Postgres+MinIO stack,
built from the repo `Dockerfile` — a push through one node is immediately visible
through the other:

```sh
# 1. Keys on the host (the same binary also mints tokens below).
go build ./cmd/codestorage
mkdir -p compose-keys && ./codestorage keygen --out ./compose-keys/cs-key

# 2. Boot backing services + both replicas (node A on :8081, node B on :8082).
docker compose --profile replicas up -d --wait --build

# 3. Mint tokens against the compose keypair.
TOK_W=$(./codestorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
TOK_GIT=$(./codestorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)

# 4. Create + push via node A; clone via node B — same HEAD SHA.
curl -s -X POST http://127.0.0.1:8081/api/v1/repos -H "Authorization: Bearer $TOK_W" \
  -d '{"default_branch":"main"}'
git clone "http://t:${TOK_GIT}@127.0.0.1:8081/demo.git" /tmp/demo-a
(cd /tmp/demo-a && git commit --allow-empty -m two-nodes && git push origin HEAD:main)
git clone "http://t:${TOK_GIT}@127.0.0.1:8082/demo.git" /tmp/demo-b

# 5. One API read per node (repo reads take git:read): v1 on A, named alias on B.
curl -s http://127.0.0.1:8081/api/v1/repo    -H "Authorization: Bearer $TOK_GIT"
curl -s http://127.0.0.1:8082/api/repos/demo -H "Authorization: Bearer $TOK_GIT"

# 6. Teardown (drops the Postgres/MinIO volumes too).
docker compose --profile replicas down -v
```

Replica host ports are env-overridable like the backing services (`NODEA_PORT`,
`NODEB_PORT`); keys are read from `./compose-keys` (override with `CS_KEYS_DIR`).

### Local mode quickstart

Zero external dependencies — no Docker, no Postgres, no MinIO. One flag derives the whole
storage stack: `sqlite://<dir>/codestorage.db` (refs + metadata), `file://<dir>/blobs`
(packs as plain files), and `<dir>/cache` (rebuildable cache).

```sh
# 1. Build and generate an org keypair (keys stay in /tmp, never commit them).
go build ./cmd/codestorage
./codestorage keygen --out /tmp/cs-key    # writes /tmp/cs-key.pem + /tmp/cs-key.pub.pem

# 2. Start a node: same /api/v1 + git smart-HTTP surface as the container stack.
CODESTORAGE_ORG=selfhost CODESTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  ./codestorage serve --listen 127.0.0.1:8080 --data-dir /tmp/cs-data &
sleep 1 && curl -sf http://127.0.0.1:8080/healthz && kill -0 $!
# ^ fails fast if the port was already taken by an older server (serve's
#   "address already in use" exit is easy to miss behind `&`).

# 3. Mint a token and create a repo.
TOK_CREATE=$(./codestorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
curl -s -X POST http://127.0.0.1:8080/api/v1/repos -H "Authorization: Bearer $TOK_CREATE" \
  -d '{"default_branch":"main"}'                                    # {"repo_id":...}

# 4. Plain git against the same node.
TOKEN=$(./codestorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git"
cd demo && git commit --allow-empty -m init && git push origin HEAD:main
```

The derivation fills only slots not set explicitly — `CODESTORAGE_PG_URL`,
`CODESTORAGE_S3_URL`, and `--cache-dir` always win. Local mode is intentionally
single-node (ADR-13 in
[docs/11-local-backends-implementation-notes.md](docs/11-local-backends-implementation-notes.md)).

### Configuration

`serve` reads `CODESTORAGE_*` environment variables; the phase-0 spike names are kept as
fallback aliases so old invocations keep working.

| Variable (canonical) | Fallback aliases | Meaning |
|---|---|---|
| `CODESTORAGE_PG_URL` | `SPIKE_PG_URL`, `PG_URL` | Database URL for refs + metadata: `postgres://` or `sqlite://` |
| `CODESTORAGE_S3_URL` | `SPIKE_S3_URL`, `BLOB_URL` | Pack storage URL: gocloud.dev `s3://` bucket or `file://` directory |
| `CODESTORAGE_DATA_DIR` | `--data-dir` flag wins | Local-mode data directory; derives `sqlite://<dir>/codestorage.db`, `file://<dir>/blobs`, and `<dir>/cache` for slots not set explicitly |
| `CODESTORAGE_LISTEN` | `--listen` / `--addr` flags win | Listen address (default `127.0.0.1:0`) |
| `CODESTORAGE_ORG` | — | **Required.** Org name (JWT `iss`) |
| `CODESTORAGE_ORG_PUBKEYS` | — | **Required.** PEM file with 1..n org public key blocks |
| `CODESTORAGE_BASE_URL` | — | External base for `http_url` in API responses (set it behind `0.0.0.0` binds) |
| `CODESTORAGE_RATE_RPS` | — | Per-client-IP rate limit for the REST **and** git smart-HTTP surfaces (separate token buckets each): sustained requests/second (429 + `Retry-After` when exceeded). Set together with `CODESTORAGE_RATE_BURST`; unset disables limiting. Client identity is the TCP `RemoteAddr` (forwarded headers are not trusted): behind a proxy that pools backend connections, all clients share one bucket — rate-limit at the proxy instead |
| `CODESTORAGE_RATE_BURST` | — | Token-bucket depth (>= 1) for the rate limit; set together with `CODESTORAGE_RATE_RPS`. The git surface raises depths below 8 to 8, and a limited client stays limited until 8 tokens have refilled, so one complete git exchange (several HTTP requests) always finishes after the advertised `Retry-After` |
| `CODESTORAGE_PACK_QUOTA_BYTES` | — | Per-repo pack-bytes quota on every pack writer: git pushes that would grow a repo's `packs/` prefix past this are refused at pre-receive before anything is stored, and REST pack writes (commit-pack, merge) are refused with a 403 result envelope (default 10 GiB; `0` disables) |

`serve` always runs single-org mode: `/api/v1` and the git smart-HTTP path authenticate
against the same org-key verifier. The phase-0 git-only mode and its `--jwt-pubkey` flag
are gone (ADR-8 in [docs/10-phase2-implementation-notes.md](docs/10-phase2-implementation-notes.md)).

## Tests

```sh
go test ./...   # needs Docker: boots real Postgres + MinIO via testcontainers
```

Zero-container lanes cover the same surfaces on the local backends: the api integration
tests fan out into `/pg` + `/sqlite` subtests, and
`CODESTORAGE_TEST_BACKEND=local go test ./internal/githttp` runs the full git e2e suite on
`sqlite://` + `file://` (see docs/11 for the lane table).
