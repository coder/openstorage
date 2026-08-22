# Deploy a two-replica stack with Docker Compose

**Goal:** a running two-replica codestorage deployment — two stateless server nodes
(`node-a`, `node-b`) sharing one Postgres (ref CAS + metadata) and one MinIO (immutable
packs) — and proof that a write through one node is immediately visible through the other.

> **This is a development stack, not deployment guidance.** It uses default credentials,
> publishes the backing-service ports, and speaks plain HTTP. Three flows are not
> functional from the host: **upstream mirror sync** (`pull-upstream`) — each node's
> `CODESTORAGE_BASE_URL` is the **host-facing** URL (`http://127.0.0.1:808x`), which the
> sync worker cannot reach from inside its own container — **Git LFS transfers** — the
> batch API presigns URLs for the container-only endpoint `http://minio:9000`, which
> host-side clients cannot resolve — and **`verify-sig`-protected pushes** — the image
> ships neither `gpg` nor `ssh-keygen`, so signature verification fails closed
> ([#50](https://github.com/coder/code-storage/issues/50)). Everything else works. For a
> hardened
> deployment (TLS, trusted proxies, rolling upgrades, key rotation), follow the
> [production deployment guide](deploy-production.md) — the topology is the same, only the
> hardening differs.

Everything below runs against the repository's [`docker-compose.yml`](../../docker-compose.yml):
the default `docker compose up` boots only Postgres + MinIO (for running the server on the
host); the opt-in `replicas` profile adds the two server nodes, built from the repo
[`Dockerfile`](../../Dockerfile).

## Prerequisites

- Docker with the Compose plugin.
- Go and git on the host — the same `codestorage` binary generates keys and mints test
  tokens.
- If sibling checkouts share your Docker daemon, pick unique host ports and a unique
  compose project name (`-p`); every published port is env-overridable (`PGPORT`,
  `MINIOPORT`, `NODEA_PORT`, `NODEB_PORT` — see the compose file header).

## 1. Build the binary and generate the org keypair

Runtime keys are generated, never committed. The nodes mount only the **public** half
(read-only, via the `CS_KEYS_DIR` volume, default `./compose-keys`); the private key stays
on the host for minting:

```sh
go build ./cmd/codestorage
mkdir -p compose-keys && ./codestorage keygen --out ./compose-keys/cs-key
# writes compose-keys/cs-key.pem (private, 0600) + compose-keys/cs-key.pub.pem
```

## 2. Boot the stack

```sh
docker compose --profile replicas up -d --wait --build
```

`--wait` blocks until every healthcheck passes: Postgres (`pg_isready`), MinIO
(`/minio/health/live`), and both nodes (unauthenticated
[`GET /healthz`](deploy-production.md#health-checks)). Node A publishes on `:8081`, node B
on `:8082`; both containers listen on `0.0.0.0:8080` internally.

Both nodes boot **concurrently** against an empty database on the first run — schema setup
(metadata, ref store, and the durable [River](https://riverqueue.com) job tables) is
advisory-lock-serialized, so concurrent replica boot is safe by design. The
[production guide](deploy-production.md#concurrent-boot-schema-setup-and-migrations)
explains which locks are taken and what a stalled boot looks like.

Verify both nodes answer:

```sh
curl -s http://127.0.0.1:8081/healthz   # {"status":"ok"}
curl -s http://127.0.0.1:8082/healthz   # {"status":"ok"}
```

## 3. Prove multi-node statelessness

Mint tokens against the compose keypair, create a repo and push through node A, then read
it back through node B — no replication step in between, because nodes keep no durable
state:

```sh
TOK_W=$(./codestorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
TOK_GIT=$(./codestorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)

# Create + push via node A.
curl -s -X POST http://127.0.0.1:8081/api/v1/repos -H "Authorization: Bearer $TOK_W" \
  -d '{"default_branch":"main"}'
git clone "http://t:${TOK_GIT}@127.0.0.1:8081/demo.git" /tmp/demo-a
(cd /tmp/demo-a && git -c user.name=demo -c user.email=demo@example.com \
  commit --allow-empty -m two-nodes && git push origin HEAD:main)

# Clone via node B — same HEAD SHA as the push through A.
git clone "http://t:${TOK_GIT}@127.0.0.1:8082/demo.git" /tmp/demo-b
git -C /tmp/demo-a rev-parse HEAD && git -C /tmp/demo-b rev-parse HEAD

# One API read per node: /api/v1 on A, the named-repo alias on B.
curl -s http://127.0.0.1:8081/api/v1/repo    -H "Authorization: Bearer $TOK_GIT"
curl -s http://127.0.0.1:8082/api/repos/demo -H "Authorization: Bearer $TOK_GIT"
```

`mint` needs `--iss selfhost` because the compose nodes run with
`CODESTORAGE_ORG=selfhost` — the JWT `iss` claim must match the configured org
([auth spec](../02-authentication.md)).

## 4. Watch the nodes work

```sh
docker compose --profile replicas logs node-a node-b
```

Boot logs show each node listening; pushes appear as git smart-HTTP requests on whichever
node served them. Background maintenance (orphan sweeps, repack folds) runs as durable
River jobs on Postgres, so **either** node may execute a job triggered through the other —
see the [storage lifecycle guide](storage-lifecycle-and-sweeping.md) for the log lines to
expect.

## 5. Tear down

```sh
docker compose --profile replicas down -v   # -v also removes the anonymous data volumes
```

This stack is disposable by design: the compose file declares **no named data volumes**
for Postgres or MinIO, so `down` (with or without `-v`) discards repository state and
the next `up` starts empty. To pause instead of discard, use
`docker compose --profile replicas stop` / `start`, which keeps the containers and their
data; steady-state restarts skip schema setup.

## Variation: server on the host, backing services in Docker

For fast iteration on the server itself, boot only Postgres + MinIO and run `serve` on the
host against the published ports:

```sh
docker compose up -d --wait
CODESTORAGE_ORG=selfhost CODESTORAGE_ORG_PUBKEYS=./compose-keys/cs-key.pub.pem \
CODESTORAGE_PG_URL="postgres://postgres:postgres@127.0.0.1:${PGPORT:-15432}/postgres" \
CODESTORAGE_S3_URL="s3://codestorage?endpoint=http://127.0.0.1:${MINIOPORT:-19000}&s3ForcePathStyle=true&region=us-east-1" \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  ./codestorage serve --listen 127.0.0.1:8080
```

Every variable is documented in the [configuration reference](../reference/configuration.md).

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `up --wait` times out on a node healthcheck | The node crashed at startup — `docker compose --profile replicas logs node-a`. Startup validation is fail-fast and names the offending variable ([configuration reference](../reference/configuration.md)). A common cause: no `compose-keys/cs-key.pub.pem` (step 1 not run), so `CODESTORAGE_ORG_PUBKEYS` points at nothing. |
| Host port already in use | Override `PGPORT` / `MINIOPORT` / `NODEA_PORT` / `NODEB_PORT` and pass a unique `-p <project>` so parallel checkouts don't collide. |
| `401`/`403` from curl or git | Token minted with the wrong `--iss` (must equal `CODESTORAGE_ORG`, here `selfhost`), the wrong key, or missing scopes. Repo routes need a token whose `repo` claim matches the target repo. |
| Rotated the keypair but old tokens still verify | Each node registers the key set from its mounted file **at boot** — restart the nodes (`docker compose --profile replicas restart node-a node-b`) after changing `compose-keys/`. |
