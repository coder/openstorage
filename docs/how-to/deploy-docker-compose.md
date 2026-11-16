# Deploy a two-replica stack with Docker Compose

Run two stateless server nodes (`node-a`, `node-b`) that share one Postgres (ref CAS
and metadata) and one SeaweedFS (immutable packs, via its S3 gateway). Then prove a
write through one node is immediately visible through the other.

For a **single-node, Docker-only** try-out with persistent SQLite/file storage and
host-reachable Git LFS, use [Try with Docker only](quickstart-docker.md) instead.

> [!WARNING]
> **This is a development stack, not deployment guidance.** It uses default
> credentials, publishes backing-service ports, and speaks plain HTTP. For TLS,
> trusted proxies, rolling upgrades, and key rotation on the same topology, follow the
> [production deployment guide](deploy-production.md).

Known limits from the host:

- **Git LFS downloads fail.** The batch API presigns URLs for the container-only
  endpoint `http://seaweedfs:8333`, which the host cannot resolve. Uploads work
  because they proxy through the server ([#51](https://github.com/coder/openstorage/issues/51)).
- **Upstream mirror sync** (`pull-upstream`) works against any upstream the
  *containers* can reach, such as public forges. It fails for an upstream bound to
  the host's loopback. The sync worker pushes through the node's **own bound
  listener**, never `OPENSTORAGE_BASE_URL` ([#49](https://github.com/coder/openstorage/pull/49),
  [#60](https://github.com/coder/openstorage/issues/60)).

Everything else works. The guide uses the repository's
[`docker-compose.yml`](../../docker-compose.yml): plain `docker compose up` starts
only Postgres and SeaweedFS. The opt-in `replicas` profile adds the two nodes, built
from the repo [`Dockerfile`](../../Dockerfile).

## Prerequisites

- Docker with the Compose plugin.
- Go and git on the host. The `openstorage` binary generates keys and mints test
  tokens.
- On a shared Docker daemon: unique host ports and a unique Compose project name
  (`-p`). Override ports with `PGPORT`, `S3PORT`, `NODEA_PORT`, `NODEB_PORT` (see
  the compose file header).

## 1. Build the binary and generate the org keypair

Keys are generated, never committed. Nodes mount only the **public** half,
read-only, via the `CS_KEYS_DIR` volume (default `./compose-keys`). The private key
stays on the host for minting:

```sh
go build ./cmd/openstorage   # or install a released binary: README → Install
mkdir -p compose-keys && ./openstorage keygen --out ./compose-keys/cs-key
# writes compose-keys/cs-key.pem (private, 0600) + compose-keys/cs-key.pub.pem
```

## 2. Boot the stack

```sh
docker compose --profile replicas up -d --wait --build
```

`--wait` blocks until all healthchecks pass:

- Postgres: `pg_isready`.
- SeaweedFS: the S3 gateway's unauthenticated `/healthz`.
- Both nodes: unauthenticated [`GET /healthz`](deploy-production.md#health-checks).

Node A publishes on `:8081` and node B on `:8082`. Both listen on `0.0.0.0:8080`
inside their containers.

Postgres (`PGPORT`) and the S3 gateway (`S3PORT`) publish on `127.0.0.1` only,
because the Postgres password and the gateway's static Admin identity are public
development defaults. Reach them as `127.0.0.1`, not `localhost`, which can resolve
to `::1`. To publish one on every interface on purpose, remove the `127.0.0.1:`
prefix from its `ports:` entry in `docker-compose.yml` and change its credentials
first. The nodes reach both over the Compose network, so this does not affect them.

> [!NOTE]
> The SeaweedFS healthcheck does not prove volumes accept writes. Before storage
> validation, require `HeadBucket` plus a scratch object PUT/DELETE on the target
> bucket, also after a SeaweedFS restart. The
> [test fixtures](../reference/testing.md#seaweedfs-fixture-image) use this gate.

On first run both nodes boot **concurrently** against an empty database. This is
safe: schema setup (metadata, ref store, and [River](https://riverqueue.com) job
tables) is serialized by advisory locks. See the
[production guide](deploy-production.md#concurrent-boot-schema-setup-and-migrations)
for the locks and what a stalled boot looks like.

**Verify** both nodes answer:

```sh
curl -s http://127.0.0.1:8081/healthz   # {"status":"ok"}
curl -s http://127.0.0.1:8082/healthz   # {"status":"ok"}
```

## 3. Prove multi-node statelessness

Mint tokens, create a repo and push through node A, then read it through node B.
There is no replication step because nodes keep no durable state:

```sh
TOK_W=$(./openstorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
TOK_GIT=$(./openstorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)

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

**Verify:** both `rev-parse` lines print the same SHA.

`mint` needs `--iss selfhost` because the nodes run with `OPENSTORAGE_ORG=selfhost`.
The JWT `iss` claim must match the configured org
([auth spec](../reference/authentication.md)).

## 4. Watch the nodes work

```sh
docker compose --profile replicas logs node-a node-b
```

Boot logs show each node listening. Pushes appear as git smart-HTTP requests on the
node that served them. Background maintenance (orphan sweeps, repack folds) runs as
durable River jobs on Postgres, so **either** node may run a job triggered through
the other. The [storage lifecycle guide](storage-lifecycle-and-sweeping.md) lists the
log lines to expect.

## 5. Tear down

> [!WARNING]
> The compose file declares **no named data volumes** for Postgres or SeaweedFS.
> `down`, with or without `-v`, discards all repository state. The next `up` starts
> empty.

```sh
docker compose --profile replicas down -v   # -v also removes the anonymous data volumes
```

To pause instead, use `docker compose --profile replicas stop` / `start`. This keeps
containers and data; steady-state restarts skip schema setup.

## Variation: server on the host, backing services in Docker

For fast iteration on the server, start only Postgres and SeaweedFS and run `serve`
on the host against the published ports:

```sh
docker compose up -d --wait
OPENSTORAGE_ORG=selfhost OPENSTORAGE_ORG_PUBKEYS=./compose-keys/cs-key.pub.pem \
OPENSTORAGE_PG_URL="postgres://postgres:postgres@127.0.0.1:${PGPORT:-15432}/postgres" \
OPENSTORAGE_S3_URL="s3://codestorage?endpoint=http://127.0.0.1:${S3PORT:-18333}&s3ForcePathStyle=true&region=us-east-1" \
AWS_ACCESS_KEY_ID=codestorage AWS_SECRET_ACCESS_KEY=codestorage \
  ./openstorage serve --listen 127.0.0.1:8080
```

The bucket name and development credentials stay `codestorage` so app nodes keep
working against existing backing containers. They are compatibility settings.

Every variable is documented in the [configuration reference](../reference/configuration.md).

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `up --wait` times out on a node healthcheck | The node crashed at startup. Run `docker compose --profile replicas logs node-a`. Startup validation fails fast and names the bad variable ([configuration reference](../reference/configuration.md)). Common cause: step 1 was skipped, so `compose-keys/cs-key.pub.pem` is missing and `OPENSTORAGE_ORG_PUBKEYS` points at nothing. |
| Host port already in use | Override `PGPORT` / `S3PORT` / `NODEA_PORT` / `NODEB_PORT` and pass a unique `-p <project>`. |
| `401`/`403` from curl or git | Wrong `--iss` (must equal `OPENSTORAGE_ORG`, here `selfhost`), wrong key, or missing scopes. Repo routes need a token whose `repo` claim matches the target repo. |
| Rotated the keypair but old tokens still verify | Nodes load keys from the mounted file **at boot**. After changing `compose-keys/`, run `docker compose --profile replicas restart node-a node-b`. |
