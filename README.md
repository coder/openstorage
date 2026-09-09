# open.storage

**API-first Git storage you can self-host.**

Git storage for your applications, on infrastructure you control. Build applications
and tools that create repositories, read and write files, and manage branches and
merges through an HTTP API. Ordinary Git clients can use the same repositories.

Run locally with SQLite and files, or deploy multiple server nodes backed by Postgres
and S3-compatible storage. The code.storage SDKs can connect to it, with
[documented compatibility differences](docs/explanation/wire-compatibility.md).

[Install](#install) · [Try locally](#quickstart-local-mode-60-seconds) · [API reference](docs/reference/http-api.md)

## Write with the API, clone with Git

Complete **Try locally** below, then keep the server running and use `$TOKEN` in the
same shell. Commit `hello.txt` through the API and read it from a new clone:

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

The API accepts file operations as [NDJSON metadata and content chunks](docs/explanation/streaming-commit-protocols.md).
SDKs handle the framing. With an HTTP client such as curl, send the frames directly
as shown above.

## Install

[Release downloads](https://github.com/coder/openstorage/releases) include Linux and
macOS binaries for amd64 and arm64, plus `checksums.txt`. Install a release with mise,
or build with Go 1.27.1 or newer:

```sh
# Choose one:
mise use -g github:coder/openstorage@latest
go install github.com/coder/open-storage/cmd/openstorage@latest

openstorage version
```

The repository and release downloads currently require Coder organization access.
See [authenticated installation](CONTRIBUTING.md#authenticated-installation) for the
required GitHub credentials and Go settings. To build from a checkout, see
[contributing](CONTRIBUTING.md#build-and-run). The quickstarts assume `openstorage` is
on your `PATH`.

**Runtime dependencies:** install `git` on the server. Ref policies that verify
commit signatures also require `gpg` or `ssh-keygen` for the relevant signature type.
The repository's Docker image includes all three; a standalone binary does not.

## Quickstarts

Start with **Try locally** (recommended). Expand a section for setup commands.

<a name="quickstart-local-mode-60-seconds"></a>

<details>
<summary>Try locally (recommended): SQLite + files</summary>

Run the `openstorage` binary ([install](#install)) with git installed; the commands
below also use curl. No database or object-storage service required. One flag derives
the whole storage stack:
`sqlite://<dir>/codestorage.db` (refs + metadata), `file://<dir>/blobs` (packs as plain
files), and `<dir>/cache` (rebuildable cache).

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

Explicit `OPENSTORAGE_PG_URL`, `OPENSTORAGE_S3_URL`, and `--cache-dir` settings take
precedence over paths derived from `--data-dir`. Local mode runs as a
[single node](docs/adr/013-local-mode-is-intentionally-single-node.md).
Data persists across restarts.

</details>

<a name="quickstart-multi-node-with-docker-compose"></a>

<details>
<summary>Explore multiple nodes: Docker Compose with Postgres + SeaweedFS</summary>

This Docker Compose stack runs two stateless server replicas (`node-a`, `node-b`)
built from the repo `Dockerfile`. They share Postgres and SeaweedFS, so a push through
one node is visible through the other. It is a **development stack** with default
credentials, published backing-service ports, and plain HTTP. For deployment beyond
local development, follow the [production guide](docs/how-to/deploy-production.md).

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

Override host ports and key locations with `PGPORT`, `S3PORT`, `NODEA_PORT`,
`NODEB_PORT`, and `CS_KEYS_DIR`, as listed in the
[`docker-compose.yml`](docker-compose.yml) header.
To run the server on the host against just the backing services, boot them with plain
`docker compose up -d --wait` and point `OPENSTORAGE_PG_URL` / `OPENSTORAGE_S3_URL` at the
published ports ([configuration reference](docs/reference/configuration.md)).

</details>

<a name="quickstart-zero-infrastructure-mode-one-bucket-experimental"></a>

<details>
<summary>Use one S3 bucket (experimental, single node)</summary>

The **experimental, single-node** `s3wal://` backend uses S3-only storage: one
S3-compatible bucket holds all durable state. No Postgres, SQLite file, or durable
local disk is required. Refs and metadata live in
conditional-CAS objects under the bucket's `_wal/` prefix, next to the packs:

```sh
# 1. Start an S3-compatible service for this example (SeaweedFS).
docker run -d --name cs-s3 -p 127.0.0.1:8333:8333 -v cs-s3-data:/data chrislusf/seaweedfs:4.45 server -s3 -dir=/data -ip.bind=0.0.0.0
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

Stop the process with plain `kill` to release its instance lease. You can delete the
cache directory and start a fresh node against the same bucket; it reconstructs
service from the stored data. After a hard stop (`kill -9` or machine loss), a
replacement waits for the old lease to expire (up to 2 minutes by default).
Once the old process is confirmed dead, delete the lease to skip that wait:

```sh
curl -sf -X DELETE http://127.0.0.1:8333/openstorage/_wal/instance-lease
```

This mode is single-node: ref updates within each repository group share one
compare-and-swap point, so concurrent pushes serialize there. The
[deployment guide](docs/how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node)
has the full lease and crash-recovery runbook, and
[the storage engine](docs/explanation/storage-engine.md#the-zero-infrastructure-backend-s3-as-sole-authority-s3wal)
explains what changes underneath.

</details>

## Architecture at a glance

For deployments with multiple server nodes, Postgres holds refs and metadata, and
S3-compatible storage holds immutable Git packs:

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

Pack data is published before refs advance through compare-and-swap. Server nodes
share the durable stores and keep rebuildable local caches, so clients can use any
replica. Local mode substitutes SQLite and files and supports one server process.
Experimental [S3-only storage](#quickstart-zero-infrastructure-mode-one-bucket-experimental)
puts refs, metadata, and packs in one bucket and also supports only one server process.
See the [storage engine](docs/explanation/storage-engine.md) for concurrency, durability,
and recovery details.

## Features

- **Read and write repositories:** files, commits, branches, tags, diffs, blame, search,
  archives, merges, and restores through the [HTTP API](docs/reference/http-api.md).
- **Use Git clients:** clone, fetch, push, and [Git LFS](docs/explanation/git-protocol-and-namespaces.md)
  on the same remote, with ref policies and optional signed-commit verification.
- **Connect workflows:** signed [webhooks](docs/reference/webhooks.md), repository forks,
  and upstream synchronization. On SQLite and S3-only storage, Git pushes do not
  automatically schedule upstream sync; API writes and explicit sync requests do.
- **Control access:** organization-signed JWTs with repository scopes and ref policies.
  Each deployment serves **one organization**; see [authentication](docs/reference/authentication.md).
- **Operate the storage:** rate limits, per-repository pack quotas, orphan sweeping,
  and repacking. Use the [deployment guide](docs/how-to/deploy-production.md) and
  [storage operations guide](docs/how-to/storage-lifecycle-and-sweeping.md) before
  exposing a service beyond the quickstart.

## Compatibility with code.storage

open.storage implements the storage API used by [code.storage](https://code.storage),
Pierre Computer Company's Git storage service. Its TypeScript, Python, and Go SDKs
can target this backend through their API and storage base-URL settings.

Compatibility covers the storage surface, with [documented differences](docs/explanation/wire-compatibility.md#known-deviations-from-the-vendored-spec)
in endpoint behavior, ephemeral namespaces, and upstream forwarding. Account management,
billing, a web UI, and a multi-tenant control plane are outside this project's scope.
Review the [compatibility guide](docs/explanation/wire-compatibility.md) for your integration.

## Configuration

Configure the organization and signing keys with `OPENSTORAGE_ORG` and
`OPENSTORAGE_ORG_PUBKEYS`. Use `--data-dir` / `OPENSTORAGE_DATA_DIR` for local mode,
or `OPENSTORAGE_PG_URL` and `OPENSTORAGE_S3_URL` to select backing stores.

The [configuration reference](docs/reference/configuration.md) covers defaults, limits,
and background jobs; the [CLI reference](docs/reference/cli.md) covers commands and flags.

## Documentation

| Task | Start here |
|---|---|
| Try it | [Quickstarts](#quickstarts) |
| Build an integration | [HTTP API](docs/reference/http-api.md) · [Authentication](docs/reference/authentication.md) · [Streaming writes](docs/explanation/streaming-commit-protocols.md) |
| Deploy and operate | [Deployment](docs/how-to/deploy-production.md) · [Configuration](docs/reference/configuration.md) · [Storage lifecycle](docs/how-to/storage-lifecycle-and-sweeping.md) |
| Understand the design | [Storage engine](docs/explanation/storage-engine.md) · [Compatibility](docs/explanation/wire-compatibility.md) · [Architecture decisions](docs/adr/README.md) |
| Find everything else | [Documentation index](docs/README.md) |

## Contributing & testing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building from source, test commands, and
review requirements. The [testing reference](docs/reference/testing.md) describes
local, Postgres, and S3-only test lanes and the simulation harness.

<details>
<summary>Repository layout</summary>

| Path | Contents |
|---|---|
| `docs/` | Documentation organized by [Diátaxis](https://diataxis.fr/) quadrant. Start at the [index](docs/README.md). |
| `docs/reference/` | Reference: configuration, CLI, testing, HTTP API, auth, webhooks |
| `docs/explanation/` | Explanation: storage engine, wire compatibility, domain model, protocols |
| `docs/adr/` | Standalone architecture decision records (ADR-1..46) |
| `docs/archive/` | Historical roadmap + per-phase implementation notes |
| `cmd/openstorage` | Service binary: `serve`, `hook`, `keygen`, `mint`, `signing-key`, `list-packs` |
| `internal/api` | `/api/v1` HTTP handlers, JWT middleware, `{"error"}` envelope |
| `internal/auth` | JWT verification contract (per-org public keys, allowed algorithms) |
| `internal/meta` | Metadata store: orgs, org public keys, repos, webhooks, git credentials, upstream links |
| `internal/blob` | `BlobStore` contract + S3 and local-file implementations (immutable packs) |
| `internal/ref` | `RefStore` contract + Postgres, SQLite, and s3wal implementations (CAS ref authority) |
| `internal/gitread` | Read-side git plumbing (ls-tree, log, diff, blame, grep, archive) over the hydrated cache repo |
| `internal/githttp` | Git smart-HTTP server + LFS batch API, policy/verify-sig hooks, cache hydration, e2e suite |
| `internal/events` | Outbound webhooks: HMAC-signed delivery + the push-event `RefStore` decorator |
| `internal/sync` | Upstream mirror sync worker (fetch + loopback push), credential/upstream contracts |
| `internal/jobs` | Background queue seam: durable River jobs (Postgres) or in-process (local mode) |
| `docker-compose.yml` | Postgres + SeaweedFS for local dogfooding, plus an opt-in two-replica `replicas` profile |

</details>

## License

Licensed under the [Apache License 2.0](LICENSE).
