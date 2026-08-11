# codestorage

An open-source, wire-compatible backend for [code.storage](https://code.storage), built from
the specification in [`docs/`](docs/README.md).

Current state: **phase 1 (skeleton + auth) done**. One Go binary (`codestorage serve`) exposes
the `/api/v1` repo CRUD API (JWT auth, single-org self-host mode) alongside the phase-0 git
smart-HTTP path, with Postgres as the ref/metadata authority and S3 (MinIO) as immutable
pack storage; nodes are stateless. Next: phase 2 (git smart HTTP) — see
[docs/07-implementation-roadmap.md](docs/07-implementation-roadmap.md).

## Layout

| Path | Contents |
|---|---|
| `docs/` | Backend specification (wire formats, auth, protocols) + roadmap + ADRs |
| `cmd/codestorage` | Service binary: `serve`, `hook`, `keygen`, `mint`, `list-packs` |
| `internal/api` | `/api/v1` HTTP handlers, JWT middleware, `{"error"}` envelope |
| `internal/auth` | JWT verification contract (per-org public keys, allowed algorithms) |
| `internal/meta` | Metadata store: orgs, org public keys, repos (soft delete, pagination) |
| `internal/blob` | `BlobStore` contract + S3/MinIO implementation (immutable packs) |
| `internal/ref` | `RefStore` contract + Postgres implementation (CAS ref authority) |
| `internal/spike` | Phase-0 git smart-HTTP node + container-based acceptance tests |
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
```

Host ports are env-overridable (`PGPORT`, `MINIOPORT`) — see the compose file header.

### Configuration

`serve` reads `CODESTORAGE_*` environment variables; the phase-0 spike names are kept as
fallback aliases so old invocations keep working.

| Variable (canonical) | Fallback aliases | Meaning |
|---|---|---|
| `CODESTORAGE_PG_URL` | `SPIKE_PG_URL`, `PG_URL` | Postgres URL (refs + metadata) |
| `CODESTORAGE_S3_URL` | `SPIKE_S3_URL`, `BLOB_URL` | gocloud.dev s3 bucket URL for packs |
| `CODESTORAGE_LISTEN` | `--listen` / `--addr` flags win | Listen address (default `127.0.0.1:0`) |
| `CODESTORAGE_ORG` | — | Org name (JWT `iss`); enables `/api/v1` |
| `CODESTORAGE_ORG_PUBKEYS` | — | PEM file with 1..n org public key blocks |

Without `CODESTORAGE_ORG`, `serve` runs the git smart-HTTP path only (spike mode,
`--jwt-pubkey` required). With it, `/api/v1` is served and the git path uses the first
ECDSA key from `CODESTORAGE_ORG_PUBKEYS` unless `--jwt-pubkey` overrides it.

## Tests

```sh
go test ./...   # needs Docker: boots real Postgres + MinIO via testcontainers
```
