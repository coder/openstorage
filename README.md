# open.storage

**Self-hosted, API-first Git storage.**

Create repositories, commit files, and merge branches over HTTP. Clone the same
repositories with plain `git`. Run one binary on SQLite and local files, or a fleet
of replicas on Postgres and S3. The code.storage SDKs work against it, with
[documented differences](docs/explanation/wire-compatibility.md).

[Install](#install) · [Quickstart](#quickstart) · [API reference](docs/reference/http-api.md) · [Docs](docs/README.md)

## Features

- **Repository API:** files, commits, branches, tags, diffs, blame, search, archives,
  merges, and restores over [HTTP](docs/reference/http-api.md).
- **Git remote:** clone, fetch, push, and [Git LFS](docs/explanation/git-protocol-and-namespaces.md)
  on the same repositories, with ref policies and optional signed-commit checks.
- **Integrations:** signed [webhooks](docs/reference/webhooks.md), forks, and upstream
  sync. On SQLite and S3-only storage, Git pushes do not trigger upstream sync; API
  writes and explicit sync requests do.
- **Access control:** organization-signed JWTs with repository scopes. One organization
  per deployment ([authentication](docs/reference/authentication.md)).
- **Operations:** rate limits, per-repository pack quotas, orphan sweeping, and
  repacking ([storage lifecycle](docs/how-to/storage-lifecycle-and-sweeping.md)).

## Install

Install a release binary with mise, or build from source with Go 1.27.3 or newer:

```sh
# Install a release binary:
mise use -g github:coder/openstorage@latest

# Or build the current source:
git clone https://github.com/coder/openstorage.git && cd openstorage
go install ./cmd/openstorage

openstorage version
```

- **Access:** the repository and releases currently require Coder organization access.
  See [authenticated installation](CONTRIBUTING.md#authenticated-installation), or
  [build and run](CONTRIBUTING.md#build-and-run) from a checkout.
- **Releases:** [Linux and macOS binaries](https://github.com/coder/openstorage/releases)
  for amd64 and arm64, plus `checksums.txt`.
- **Runtime dependencies:** the server needs `git`. Signed-commit ref policies also
  need `gpg` or `ssh-keygen` for the relevant signature type. The Docker image includes all three; the binary does not.

<a name="quickstarts"></a>

## Quickstart

<a name="quickstart-local-mode-60-seconds"></a>

### Try locally: SQLite + files

You need `openstorage` on your `PATH`, `git`, and `curl`. No database or object
store required.

```sh
# 1. Generate an org keypair (keys are never committed).
openstorage keygen --out /tmp/cs-key      # writes /tmp/cs-key.pem + /tmp/cs-key.pub.pem

# 2. Start a node: REST API + git smart-HTTP on one listener, storage under ~/cs-data.
OPENSTORAGE_ORG=selfhost OPENSTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  openstorage serve --listen 127.0.0.1:8080 --data-dir ~/cs-data &
sleep 1 && curl -sf http://127.0.0.1:8080/healthz && kill -0 $!
# ^ fails fast if the port was already taken by an older server (serve's
#   "address already in use" exit is easy to miss behind `&`).

# 3. Mint a token and create a repo.
TOK_CREATE=$(openstorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
curl -s -X POST http://127.0.0.1:8080/api/v1/repos -H "Authorization: Bearer $TOK_CREATE" \
  -d '{"default_branch":"main"}'                                    # {"repo_id":...}

# 4. Plain git against the same node.
TOKEN=$(openstorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git"
cd demo && git -c user.name=demo -c user.email=demo@example.com \
  commit --allow-empty -m init && git push origin HEAD:main
```

`--data-dir` derives the whole storage stack:

| Path | Holds |
|---|---|
| `sqlite://<dir>/codestorage.db` | Refs and metadata |
| `file://<dir>/blobs` | Git packs, as plain files |
| `<dir>/cache` | Rebuildable cache |

Data persists across restarts. Explicit `OPENSTORAGE_PG_URL`, `OPENSTORAGE_S3_URL`, and
`--cache-dir` settings override the derived paths. Local mode runs as a
[single node](docs/adr/013-local-mode-is-intentionally-single-node.md).

### Write with the API, clone with Git

Keep the server running. In the same shell, commit `hello.txt` through the API and
read it from a fresh clone:

```sh
curl --fail-with-body -sS http://127.0.0.1:8080/api/v1/repos/commit-pack \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/x-ndjson' -H 'Accept: application/json' \
  --data-binary @- <<'NDJSON'
{"metadata":{"target_branch":"main","commit_message":"Add greeting","author":{"name":"Demo","email":"demo@example.com"},"files":[{"path":"hello.txt","content_id":"greeting","operation":"upsert"}]}}
{"blob_chunk":{"content_id":"greeting","data":"SGVsbG8gZnJvbSB0aGUgQVBJIQo=","eof":true}}
NDJSON
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git" demo-from-api
cat demo-from-api/hello.txt  # Hello from the API!
```

The API takes file operations as [NDJSON metadata and content chunks](docs/explanation/streaming-commit-protocols.md).
SDKs handle this framing for you; with curl, you send the frames yourself.

### Other setups

| Setup | Use it to | Guide |
|---|---|---|
| Docker only | Run one local-mode server with persistent volumes and no host Go or binary. Uses the published image or a local build. | [Try with Docker only](docs/how-to/quickstart-docker.md) |
| Two replicas | See two nodes share Postgres and SeaweedFS. | [Below](#quickstart-multi-node-with-docker-compose) |
| One S3 bucket | Keep all durable state in one bucket. Experimental, single node. | [Below](#quickstart-zero-infrastructure-mode-one-bucket-experimental) |

<a name="quickstart-multi-node-with-docker-compose"></a>

<details>
<summary><b>Two replicas: Docker Compose with Postgres + SeaweedFS</b></summary>

Two stateless replicas (`node-a`, `node-b`), built from the repo `Dockerfile`, share
Postgres and SeaweedFS. A push through one node is visible through the other.

> [!WARNING]
> This is a development stack: default credentials, published backing-service ports
> (on `127.0.0.1` only), and plain HTTP. For real deployments, follow the [production guide](docs/how-to/deploy-production.md).

```sh
# 1. Generate keys on the host from this checkout (requires openstorage; see Install).
mkdir -p compose-keys && openstorage keygen --out ./compose-keys/cs-key

# 2. Boot backing services + both replicas (node A on :8081, node B on :8082).
docker compose --profile replicas up -d --wait --build

# 3. Mint tokens against the compose keypair.
TOK_W=$(openstorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
TOK_GIT=$(openstorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)

# 4. Create and push via node A, then clone via node B. Both clones have the same HEAD SHA.
curl -s -X POST http://127.0.0.1:8081/api/v1/repos -H "Authorization: Bearer $TOK_W" \
  -d '{"default_branch":"main"}'
git clone "http://t:${TOK_GIT}@127.0.0.1:8081/demo.git" /tmp/demo-a
(cd /tmp/demo-a && git -c user.name=demo -c user.email=demo@example.com \
  commit --allow-empty -m two-nodes && git push origin HEAD:main)
git clone "http://t:${TOK_GIT}@127.0.0.1:8082/demo.git" /tmp/demo-b

# 5. One API read per node (repo reads take git:read): v1 on A, named alias on B.
curl -s http://127.0.0.1:8081/api/v1/repo    -H "Authorization: Bearer $TOK_GIT"
curl -s http://127.0.0.1:8082/api/repos/demo -H "Authorization: Bearer $TOK_GIT"

# 6. Teardown (drops the Postgres/SeaweedFS volumes too).
docker compose --profile replicas down -v
```

- Override host ports and key locations with `PGPORT`, `S3PORT`, `NODEA_PORT`,
  `NODEB_PORT`, and `CS_KEYS_DIR` (see the [`docker-compose.yml`](docker-compose.yml) header).
- To run the server on the host instead, start only the backing services with
  `docker compose up -d --wait`, then point `OPENSTORAGE_PG_URL` and `OPENSTORAGE_S3_URL`
  at the published ports ([configuration reference](docs/reference/configuration.md)).

</details>

<a name="quickstart-zero-infrastructure-mode-one-bucket-experimental"></a>

<details>
<summary><b>One S3 bucket (experimental, single node)</b></summary>

The **experimental, single-node** `s3wal://` backend keeps all durable state in one
S3-compatible bucket: no Postgres, no SQLite file, no durable local disk. Refs and
metadata live in conditional-CAS objects under the bucket's `_wal/` prefix, next to
the packs.

```sh
# 1. Start an S3-compatible service for this example (SeaweedFS).
docker run -d --name cs-s3 -p 127.0.0.1:8333:8333 -v cs-s3-data:/data chrislusf/seaweedfs:4.53@sha256:ccad23e18639c8b98cee17e7100b95830a06077b56d8e5b006d6dcf8ec5e94a0 server -s3 -dir=/data -ip.bind=0.0.0.0
until curl -sf http://127.0.0.1:8333/healthz; do sleep 1; done
# The bucket must exist before serve boots. On a cold store both bucket creation and
# object writes can precede a writable volume, so retry create + a real write as one unit
# (the create tolerates already-exists; readiness is the object roundtrip).
until curl -s -X PUT http://127.0.0.1:8333/openstorage >/dev/null &&
      curl -sf -X PUT -d ready http://127.0.0.1:8333/openstorage/ready &&
      curl -sf -X DELETE http://127.0.0.1:8333/openstorage/ready; do sleep 1; done

# 2. One node, refs + metadata + packs all in the bucket (this dev SeaweedFS is unauthenticated; any creds sign).
openstorage keygen --out /tmp/cs-key
OPENSTORAGE_ORG=selfhost OPENSTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
OPENSTORAGE_PG_URL='s3wal://openstorage?endpoint=http://127.0.0.1:8333&region=us-east-1' \
OPENSTORAGE_S3_URL='s3://openstorage?endpoint=http://127.0.0.1:8333&s3ForcePathStyle=true&region=us-east-1' \
AWS_ACCESS_KEY_ID=any AWS_SECRET_ACCESS_KEY=any \
  openstorage serve --listen 127.0.0.1:8080 --cache-dir /tmp/cs-cache &
sleep 3 && curl -sf http://127.0.0.1:8080/healthz && kill -0 $!

# 3. Follow the same client flow as local mode: mint, create, clone, push.
TOK=$(openstorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes repo:write,git:read,git:write)
curl -s -X POST http://127.0.0.1:8080/api/v1/repos -H "Authorization: Bearer $TOK" \
  -d '{"default_branch":"main"}'
git clone "http://t:${TOK}@127.0.0.1:8080/demo.git" && cd demo
git -c user.name=demo -c user.email=demo@example.com commit --allow-empty -m init
git push origin HEAD:main
```

**Stopping and restarting.** Stop with plain `kill` (`SIGTERM`), then confirm the
process and all hook and maintenance writers have exited. Once stopped, you can
discard the cache; a fresh node rebuilds from the bucket.

> [!WARNING]
> Shutdown releases the instance lease on a best-effort basis, so a lease can remain
> even after a clean stop. After a hard stop or failed release:
>
> 1. Keep old writers fenced.
> 2. Wait at least the **holder's recorded TTL plus 2s** after the confirmed stop (default TTL 2m).
> 3. Follow the [shutdown and restart runbook](docs/how-to/deploy-production.md#s3wal-shutdown-and-restart), then start exactly one replacement.
>
> A corrupt or unjudgeable lease needs fenced investigation, not repeated starts.
> Never delete the lease or edit `_wal/` by hand to skip the wait or to make startup pass.

This mode is single-node. Ref updates within a repository group share one
compare-and-swap point, so concurrent pushes serialize. See [the storage engine](docs/explanation/storage-engine.md#the-zero-infrastructure-backend-s3-as-sole-authority-s3wal)
for what changes underneath.

</details>

<a name="architecture-at-a-glance"></a>

## How it works

```mermaid
flowchart TB
    clients["SDKs · git · HTTP clients<br/>(organization-signed JWTs)"]

    subgraph node["openstorage serve: replicate with shared Postgres + S3"]
        api["HTTP API<br/>/api/v1 + named-repo aliases"]
        git["Git smart-HTTP + LFS"]
        jobs["Background jobs<br/>sweep · repack · webhooks · upstream sync"]
    end

    refs[("Postgres<br/>refs + metadata")]
    packs[("S3-compatible storage<br/>immutable packs")]

    clients --> api
    clients --> git
    node --> refs
    node --> packs
```

- **Postgres** holds refs and metadata. **S3-compatible storage** holds immutable Git packs.
- Packs are published before refs advance through compare-and-swap.
- Replicas share the durable stores and keep only rebuildable local caches, so clients
  can use any replica.
- Local mode (SQLite + files) and experimental [S3-only storage](#quickstart-zero-infrastructure-mode-one-bucket-experimental)
  each support one server process.

The [storage engine](docs/explanation/storage-engine.md) page covers concurrency,
durability, and recovery.

## Compatibility with code.storage

open.storage implements the storage API of [code.storage](https://code.storage),
Pierre Computer Company's Git storage service. Point its TypeScript, Python, or Go SDK
at this backend with the SDK's API and storage base-URL settings.

- **Covered:** the storage surface, with [documented differences](docs/explanation/wire-compatibility.md#known-deviations-from-the-vendored-spec)
  in endpoint behavior, ephemeral namespaces, and upstream forwarding.
- **Out of scope:** account management, billing, a web UI, and a multi-tenant control plane.

Check the [compatibility guide](docs/explanation/wire-compatibility.md) against your integration.

## Configuration

| Setting | Purpose |
|---|---|
| `OPENSTORAGE_ORG`, `OPENSTORAGE_ORG_PUBKEYS` | Organization name and its public signing keys |
| `--data-dir` / `OPENSTORAGE_DATA_DIR` | Local mode: derive all storage from one directory |
| `OPENSTORAGE_PG_URL`, `OPENSTORAGE_S3_URL` | Select the ref/metadata store and the pack store |

Everything else: [configuration reference](docs/reference/configuration.md) (defaults,
limits, background jobs) and [CLI reference](docs/reference/cli.md) (commands and flags).

## Documentation

| I want to… | Start here |
|---|---|
| Try it | [Quickstart](#quickstart) |
| Build an integration | [HTTP API](docs/reference/http-api.md) · [Authentication](docs/reference/authentication.md) · [Streaming writes](docs/explanation/streaming-commit-protocols.md) |
| Deploy and operate | [Production deployment](docs/how-to/deploy-production.md) · [Configuration](docs/reference/configuration.md) · [Storage lifecycle](docs/how-to/storage-lifecycle-and-sweeping.md) |
| Understand the design | [Storage engine](docs/explanation/storage-engine.md) · [Compatibility](docs/explanation/wire-compatibility.md) · [Architecture decisions](docs/adr/README.md) |
| Find everything else | [Documentation index](docs/README.md) |

Before exposing a service beyond the quickstart, read the
[deployment guide](docs/how-to/deploy-production.md) and the
[storage lifecycle guide](docs/how-to/storage-lifecycle-and-sweeping.md).

<a name="contributing--testing"></a>

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers building from source, tests, and the review
bar. The [testing reference](docs/reference/testing.md) describes the local, Postgres,
and S3-only test lanes and the simulation harness.

<details>
<summary>Repository layout</summary>

| Path | Contents |
|---|---|
| `cmd/openstorage` | Service binary: `serve`, `hook`, `keygen`, `mint`, `signing-key`, `list-packs` |
| `internal/api` | `/api/v1` HTTP handlers, JWT middleware, `{"error"}` envelope |
| `internal/auth` | JWT verification (per-org public keys, allowed algorithms) |
| `internal/meta` | Metadata store: orgs, org public keys, repos, webhooks, git credentials, upstream links |
| `internal/blob` | `BlobStore` contract + S3 and local-file implementations (immutable packs) |
| `internal/ref` | `RefStore` contract + Postgres, SQLite, and s3wal implementations (CAS ref authority) |
| `internal/gitread` | Read-side git plumbing (ls-tree, log, diff, blame, grep, archive) over the hydrated cache repo |
| `internal/githttp` | Git smart-HTTP + LFS batch API, policy/verify-sig hooks, cache hydration, e2e suite |
| `internal/events` | Outbound webhooks: HMAC-signed delivery + the push-event `RefStore` decorator |
| `internal/sync` | Upstream mirror sync worker (fetch + loopback push), credential/upstream contracts |
| `internal/jobs` | Background queue: durable River jobs (Postgres) or in-process (local mode) |
| `docs/` | Documentation by [Diátaxis](https://diataxis.fr/) quadrant; start at the [index](docs/README.md) |
| `docker-compose.yml` | Postgres + SeaweedFS for local development, plus an opt-in two-replica `replicas` profile |

</details>

## System Overview

open.storage is one Go module, `github.com/coder/openstorage`. `cmd/` holds its two
binaries, and `internal/` holds the server packages. Requests enter through `internal/api`
(REST) and `internal/githttp` (Git smart HTTP, Git LFS, and the Git hooks). Both use
`internal/auth` for JWTs, `internal/meta` for metadata, `internal/ref` for refs, packs, and
quota, and `internal/blob` for pack and LFS bytes. `internal/jobs` and `internal/sync` run
the background work. [How it works](#how-it-works) shows the deployed shape.

The sections below index the per-directory contracts (Interface, Invariants, Side Effects,
Verification).

## Subdirectories & System Boundaries

- cmd: [./cmd/README.md](./cmd/README.md) - The `openstorage` service binary (`serve`,
  `hook`, and operator subcommands) and `dst`, the deterministic-simulation replay tool.
  `cmd/openstorage` is the composition root: it resolves `OPENSTORAGE_*` settings and
  wires every `internal/` package.
- internal: [./internal/README.md](./internal/README.md) - All server packages: `api`,
  `githttp`, `auth`, `meta`, `ref` (with `ref/s3wal`), `blob`, `gitread`, `gitlimit`,
  `jobs`, `sync`, `events`, `lfsscan`, `metrics`, `envconfig`, `secrets`, `s3oplog`,
  `version`, the `simtest` simulation harness, and `testutil`.
- deploy: [./deploy/README.md](./deploy/README.md) - The Terraform reference deployment
  for AWS (ALB, EC2, RDS PostgreSQL 17, S3), evaluation only. It installs a
  checksum-verified release binary with cloud-init.
- scripts: [./scripts/README.md](./scripts/README.md) - The docs link checker, the
  release archive builder, and the Compose quickstart smoke test. CI runs the link
  checker and the smoke test. `release.yml` runs the archive builder.
- workflows: [./workflows/README.md](./workflows/README.md) - `build-phase.js`, a Xum
  workflow script that an operator starts by hand. No build or CI step runs it.
- `docs/` (human documentation, start at the [index](docs/README.md)), `testdata/`, and
  `vendor/` have no contract files.

## Global Invariants

- Pack bytes are durable before a ref moves. On an API write, `wcPublish` calls
  `publishPack`, which uploads and registers the pack, and then runs the ref CAS
  [internal/api/handlers_write_commit.go:wcPublish] [internal/api/publish.go:publishPack].
  On a Git push, `preReceive`
  uploads and registers packs, and `updateHook` then runs the CAS
  [internal/githttp/hook.go:preReceive] [internal/githttp/hook.go:updateHook].
- Refs change only through compare-and-swap on the ref store. Postgres, SQLite, and s3wal
  all implement the same `RefStore` contract [internal/ref/ref.go:RefStore]
  [internal/ref/ref.go:CASRef].
- Every API and Git request carries an organization-signed JWT. A missing token gets 401.
  A token that fails verification gets 403 on the API and 401 on Git. A
  wrong org, scope, or repo claim gets 403 [internal/api/middleware.go:requireAuth]
  [internal/githttp/server.go:verifyClaims] [internal/auth/verifier.go:Verify].
- Startup fails fast on bad configuration. `OPENSTORAGE_ORG` and `OPENSTORAGE_ORG_PUBKEYS`
  are required, and an `s3wal://` store requires an `s3://` blob store
  [cmd/openstorage/config.go:resolveServeConfig]
  [cmd/openstorage/config.go:validateZeroInfraPairing]. A `CODESTORAGE_*` setting without
  its `OPENSTORAGE_*` replacement refuses startup [internal/envconfig/envconfig.go:CheckLegacy].
- An s3wal deployment serves from one process only. The instance lease refuses a second
  server, and `serve` exits when it loses the lease [internal/ref/s3wal/lease.go:AcquireInstanceLease]
  [cmd/openstorage/main.go:cmdServe].
- Outbound webhook and upstream sync traffic goes through an SSRF guard. The guard always
  denies metadata, unspecified, multicast, and broadcast addresses, and denies private
  addresses unless allowed [internal/events/guard.go:deniedReason]
  [internal/sync/egress.go:startEgressProxy].
- Git subprocesses run under shared admission slots, in tracked process groups that
  shutdown stops [internal/gitlimit/gitlimit.go:ReadSlots]
  [internal/gitlimit/children.go:StopChildren].
- The orphan sweeper never acts on a key under `_wal/` [internal/jobs/sweep.go:assertNotWAL].

## Global Side Effects

- Refs and metadata live in Postgres, SQLite, or an s3wal bucket. `serve` runs schema DDL at
  startup [internal/ref/ref.go:InitSchema] [internal/meta/pg.go:InitSchema].
- Packs and LFS objects live in an S3-compatible bucket or a local directory, under
  `packs/<repo>/` and `lfs/<repo>/` [internal/blob/blob.go:PackKey]
  [internal/blob/blob.go:LFSKey]. s3wal state lives under `_wal/` in the same bucket
  [internal/ref/s3wal/manifest.go:RepoPrefix] [internal/ref/s3wal/lease.go:LeaseKey].
- Each node keeps rebuildable bare cache repos on local disk, hydrated from the stores
  [internal/githttp/hydrate.go:Hydrate].
- The server runs `git` subprocesses, and its cache repos have hook scripts that re-run
  this binary as `openstorage hook` [internal/githttp/hydrate.go:hookScript]
  [internal/githttp/hook.go:RunHook].
- Outbound HTTP: signed webhook deliveries and upstream Git fetch and push
  [internal/events/deliver.go:Attempt] [internal/sync/worker.go:runGitTo].
- Background work runs as River jobs on Postgres or as in-process goroutines: sweeps,
  repacks, LFS reclamation, absence reconciliation, webhooks, and sync
  [internal/jobs/river.go:NewRiver] [internal/jobs/local.go:Local].
- Optional: a separate `/metrics` listener and a per-process S3 operation log
  [internal/metrics/metrics.go:NewServer] [internal/s3oplog/s3oplog.go:setup].
- CAUTION: `terraform apply` in `deploy/terraform/aws` creates billable AWS resources
  ([deploy/terraform/aws/README.md](./deploy/terraform/aws/README.md)).

## System Verification

- Run: `go test ./... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (needs
  Docker, git, and git-lfs).
- Run: `golangci-lint run`
- Run: `python3 scripts/check-doc-links.py` and
  `python3 -B -m unittest discover -s scripts -p 'test_*.py'`
- The local and s3wal test lanes are in the [testing reference](docs/reference/testing.md).
  The Terraform gates are in [deploy/terraform/aws/README.md](./deploy/terraform/aws/README.md).

## License

[Apache License 2.0](LICENSE).
