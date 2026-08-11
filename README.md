# codestorage

An open-source, wire-compatible backend for [code.storage](https://code.storage), built from
the specification in [`docs/`](docs/README.md).

Current state: **phase-0 de-risk spike**. One Go binary serves git smart HTTP with Postgres
as the ref authority and S3 (MinIO) as immutable pack storage; nodes are stateless. See
[docs/08-spike-implementation-notes.md](docs/08-spike-implementation-notes.md) for the
decisions and [docs/07-implementation-roadmap.md](docs/07-implementation-roadmap.md) for
what comes next.

## Layout

| Path | Contents |
|---|---|
| `docs/` | Backend specification (wire formats, auth, protocols) + roadmap + ADRs |
| `cmd/codestorage` | Spike binary: `serve`, `hook`, `keygen`, `mint`, `list-packs` |
| `internal/spike` | Spike implementation + container-based acceptance tests |
| `docker-compose.spike.yml` | Postgres + MinIO for local dogfooding |

## Quickstart (spike)

Requires Go, git, and Docker.

```sh
# 1. Infra: Postgres (ref authority) + MinIO (pack storage).
docker compose -f docker-compose.spike.yml up -d --wait

# 2. Build, generate an org keypair, start a node.
go build ./cmd/codestorage
./codestorage keygen --out /tmp/spike-key
PG_URL='postgres://postgres:postgres@127.0.0.1:15432/postgres' \
BLOB_URL='s3://spike?endpoint=http://127.0.0.1:19000&region=us-east-1' \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
./codestorage serve --addr 127.0.0.1:8080 --cache-dir /tmp/spike-cache \
  --jwt-pubkey /tmp/spike-key.pub.pem &

# 3. Mint a JWT and use plain git against it (repos are created on first use).
TOKEN=$(./codestorage mint --key /tmp/spike-key.pem --repo demo)
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git"
cd demo && git commit --allow-empty -m init && git push origin HEAD:main
```

Host ports are env-overridable (`PGPORT`, `MINIOPORT`) — see the compose file header.

## Tests

```sh
go test ./...   # needs Docker: boots real Postgres + MinIO via testcontainers
```
