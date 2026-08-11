# codestorage

An open-source, wire-compatible backend for [code.storage](https://code.storage), built from
the specification in [`docs/`](docs/README.md).

Current state: **phase 2 (git smart HTTP) + pluggable storage backends done**. One Go binary
(`codestorage serve`) exposes the `/api/v1` repo CRUD API and the production git smart-HTTP
path (clone/push, ref-policy enforcement, commit-signature verification) behind the same
per-org JWT verifier; nodes are stateless. Storage is selected by URL scheme: Postgres +
S3 (MinIO) for multi-node deployments, or zero-dependency local mode (SQLite + plain files
via `serve --data-dir`). Next: phase 3 (read API) — see
[docs/07-implementation-roadmap.md](docs/07-implementation-roadmap.md).

## Layout

| Path | Contents |
|---|---|
| `docs/` | Backend specification (wire formats, auth, protocols) + roadmap + ADRs |
| `cmd/codestorage` | Service binary: `serve`, `hook`, `keygen`, `mint`, `signing-key`, `list-packs` |
| `internal/api` | `/api/v1` HTTP handlers, JWT middleware, `{"error"}` envelope |
| `internal/auth` | JWT verification contract (per-org public keys, allowed algorithms) |
| `internal/meta` | Metadata store: orgs, org public keys, repos (soft delete, pagination) |
| `internal/blob` | `BlobStore` contract + S3/MinIO and local-file implementations (immutable packs) |
| `internal/ref` | `RefStore` contract + Postgres and SQLite implementations (CAS ref authority) |
| `internal/githttp` | Git smart-HTTP server + policy/verify-sig hooks, cache hydration, e2e suite |
| `docker-compose.yml` | Postgres + MinIO for local dogfooding |

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

# 4. Mint JWTs with the org's private key (iss = org, repo = target repo or "org").
TOK_CREATE=$(./codestorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
TOK_READ=$(./codestorage mint   --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read)
TOK_ORG=$(./codestorage mint    --key /tmp/cs-key.pem --iss selfhost --repo org  --scopes org:read)

# 5. Repo CRUD (envelope + status codes per docs/03-http-api.md).
curl -s -X POST http://127.0.0.1:8080/api/v1/repos -H "Authorization: Bearer $TOK_CREATE" \
  -d '{"default_branch":"main"}'                                    # {"repo_id":...}
curl -s http://127.0.0.1:8080/api/v1/repo  -H "Authorization: Bearer $TOK_READ"
curl -s 'http://127.0.0.1:8080/api/v1/repos?limit=10&q=demo' -H "Authorization: Bearer $TOK_ORG"
curl -s -X DELETE http://127.0.0.1:8080/api/v1/repos/delete -H "Authorization: Bearer $TOK_CREATE"

# 6. Plain git against the same node.
TOKEN=$(./codestorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git"
cd demo && git commit --allow-empty -m init && git push origin HEAD:main

# 7. Optional: register commit-signing keys (SSH public key line or armored PGP
#    block) for the `verify-sig` ref-policy op; hooks read them fresh per push.
./codestorage signing-key add --org selfhost --key ~/.ssh/id_ed25519.pub
./codestorage signing-key list --org selfhost
```

Host ports are env-overridable (`PGPORT`, `MINIOPORT`) — see the compose file header.

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
