# codestorage

An open-source, wire-compatible backend for [code.storage](https://code.storage) — Pierre
Computer Company's git-native storage service — built from the specification in
[`docs/`](docs/README.md). One Go binary serves the `/api/v1` HTTP API and the production
git smart-HTTP path (clone/push, LFS) behind customer-signed JWTs; the official SDKs drop
in via their `apiBaseUrl` / `storageBaseUrl` overrides without changing client code. Run it
as a fleet of stateless nodes on Postgres + S3/MinIO, or as a single zero-dependency
process on SQLite + plain files.

## Architecture at a glance

```mermaid
flowchart TB
    clients["SDKs · plain git · curl<br/>(customer-signed JWTs, per-org keys)"]

    subgraph node["codestorage serve — stateless, replicate freely"]
        api["REST API<br/>/api/v1 + named-repo aliases"]
        git["git smart-HTTP + LFS"]
        jobs["background jobs<br/>orphan sweep · repack · webhooks · upstream sync"]
    end

    refs[("Postgres — ref CAS + metadata<br/>(local mode: SQLite)")]
    packs[("S3 / MinIO — immutable packs<br/>(local mode: plain files)")]

    clients --> api
    clients --> git
    node --> refs
    node --> packs
```

Refs advance only through compare-and-swap on the database; pack data is published to the
blob store *before* any ref moves, so a committed ref never points at unavailable objects.
Nodes keep no durable state — a push through one replica is immediately visible through
every other.

## Install

Prebuilt binaries (`linux`/`darwin` × `amd64`/`arm64`, with `checksums.txt`) ship on the
[releases page](https://github.com/coder/code-storage/releases). The first two options put
`codestorage` on your `PATH`, which the quickstarts below assume; a source build lands at
`./codestorage` — substitute accordingly:

```sh
# Go toolchain (needs Go >= 1.26):
go install github.com/coder/code-storage/cmd/codestorage@latest

# mise (downloads the release binary; verifies checksums + GitHub artifact attestations):
mise use -g github:coder/code-storage@latest

codestorage version               # verify the install

# From source (contributors; builds ./codestorage without touching PATH):
git clone https://github.com/coder/code-storage.git && cd code-storage
go build ./cmd/codestorage && ./codestorage version
```

> [!NOTE]
> While this repository is **internal** (coder org members only), both hosted paths need
> GitHub credentials:
>
> - `go install` must fetch the module directly from GitHub instead of the public proxy,
>   using authenticated git — without `GOPRIVATE` it fails at the public checksum
>   database (`sum.golang.org … 404 Not Found`):
>
>   ```sh
>   gh auth setup-git   # or any git credential helper for github.com
>   GOPRIVATE='github.com/coder/*' go install github.com/coder/code-storage/cmd/codestorage@latest
>   ```
>
> - mise needs a GitHub token for the release download: a logged-in `gh` CLI is picked up
>   automatically (the `github.gh_cli_tokens` setting is on by default); otherwise set
>   `GITHUB_TOKEN`. On mise versions that predate the `github:` backend, the equivalent is
>   `mise use -g 'ubi:coder/code-storage[exe=codestorage]@latest'` — the `exe=` hint is
>   required because the repo name (`code-storage`) differs from the binary name
>   (`codestorage`).

**Runtime dependencies:** the server shells out to `git` (always) and to `gpg` /
`ssh-keygen` (only for `verify-sig` ref policies, which fail closed without them —
[ADR-9](docs/10-phase2-implementation-notes.md#adr-9-verify-sig-delegates-to-gits-own-verification-machinery),
[#50](https://github.com/coder/code-storage/issues/50)). The Docker image carries all
three; a bare binary brings none — install them on the host.

## Quickstart: local mode (60 seconds)

Requires only the `codestorage` binary ([install](#install)) and git — no Docker, no
Postgres, no MinIO. One flag derives the whole storage stack:
`sqlite://<dir>/codestorage.db` (refs + metadata), `file://<dir>/blobs` (packs as plain
files), and `<dir>/cache` (rebuildable cache).

```sh
# 1. Generate an org keypair (keys are never committed).
codestorage keygen --out /tmp/cs-key      # writes /tmp/cs-key.pem + /tmp/cs-key.pub.pem

# 2. Start a node: REST API + git smart-HTTP on one listener, storage under ~/cs-data.
CODESTORAGE_ORG=selfhost CODESTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  codestorage serve --listen 127.0.0.1:8080 --data-dir ~/cs-data &
sleep 1 && curl -sf http://127.0.0.1:8080/healthz && kill -0 $!
# ^ fails fast if the port was already taken by an older server (serve's
#   "address already in use" exit is easy to miss behind `&`).

# 3. Mint a token and create a repo.
TOK_CREATE=$(codestorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
curl -s -X POST http://127.0.0.1:8080/api/v1/repos -H "Authorization: Bearer $TOK_CREATE" \
  -d '{"default_branch":"main"}'                                    # {"repo_id":...}

# 4. Plain git against the same node.
TOKEN=$(codestorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git"
cd demo && git -c user.name=demo -c user.email=demo@example.com \
  commit --allow-empty -m init && git push origin HEAD:main
```

The derivation fills only slots not set explicitly — `CODESTORAGE_PG_URL`,
`CODESTORAGE_S3_URL`, and `--cache-dir` always win. Local mode is intentionally
single-node (ADR-13 in
[docs/11](docs/11-local-backends-implementation-notes.md)); the same binary and data
survive restarts, so it doubles as a tiny durable git server.

## Quickstart: multi-node with Docker Compose

Two stateless server replicas (`node-a`, `node-b`) share one Postgres + MinIO stack, built
from the repo `Dockerfile` — a push through one node is immediately visible through the
other. This is a **development stack** for demonstrating multi-node statelessness (default
credentials, published backing-service ports, plain HTTP) — not a hardened deployment
(for that, see the [production guide](docs/how-to/deploy-production.md)):

```sh
# 1. Keys on the host, from this checkout (needs the codestorage binary — see Install).
mkdir -p compose-keys && codestorage keygen --out ./compose-keys/cs-key

# 2. Boot backing services + both replicas (node A on :8081, node B on :8082).
docker compose --profile replicas up -d --wait --build

# 3. Mint tokens against the compose keypair.
TOK_W=$(codestorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes repo:write)
TOK_GIT=$(codestorage mint --key ./compose-keys/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)

# 4. Create + push via node A; clone via node B — same HEAD SHA.
curl -s -X POST http://127.0.0.1:8081/api/v1/repos -H "Authorization: Bearer $TOK_W" \
  -d '{"default_branch":"main"}'
git clone "http://t:${TOK_GIT}@127.0.0.1:8081/demo.git" /tmp/demo-a
(cd /tmp/demo-a && git -c user.name=demo -c user.email=demo@example.com \
  commit --allow-empty -m two-nodes && git push origin HEAD:main)
git clone "http://t:${TOK_GIT}@127.0.0.1:8082/demo.git" /tmp/demo-b

# 5. One API read per node (repo reads take git:read): v1 on A, named alias on B.
curl -s http://127.0.0.1:8081/api/v1/repo    -H "Authorization: Bearer $TOK_GIT"
curl -s http://127.0.0.1:8082/api/repos/demo -H "Authorization: Bearer $TOK_GIT"

# 6. Teardown (drops the Postgres/MinIO volumes too).
docker compose --profile replicas down -v
```

Host ports and key locations are env-overridable (`PGPORT`, `MINIOPORT`, `NODEA_PORT`,
`NODEB_PORT`, `CS_KEYS_DIR`) — see the [`docker-compose.yml`](docker-compose.yml) header.
To run the server on the host against just the backing services, boot them with plain
`docker compose up -d --wait` and point `CODESTORAGE_PG_URL` / `CODESTORAGE_S3_URL` at the
published ports ([configuration reference](docs/reference/configuration.md)).

## Features

- **Full read surface** (`git:read`): files, branches, tags, commits, diff, blame, grep,
  notes, archive — cursor pagination, conditional/range file streaming
  ([docs/03](docs/03-http-api.md)).
- **Full write surface** (`git:write`): NDJSON commit-pack / diff-commit streaming,
  restore-commit, merge with read-only preview, branch/tag/notes writes — compare-and-swap
  and ref policies everywhere ([docs/04](docs/04-streaming-commit-protocols.md)).
- **Production git smart-HTTP**: clone/push with ref-policy enforcement and
  commit-signature verification ([docs/05](docs/05-git-protocol-and-namespaces.md)),
  behind the same per-org JWT verifier as the API
  ([docs/02](docs/02-authentication.md)).
- **Git LFS on the same remote** — no separate LFS server: presigned S3/MinIO downloads
  (server-proxied in local mode) and verified, quota-metered server-proxied uploads
  ([docs/05](docs/05-git-protocol-and-namespaces.md)).
- **Eventing & sync**: HMAC-signed `push` / `repo.sync.*` webhooks, webhook +
  git-credential CRUD, upstream mirror sync, synchronous forking
  ([docs/06](docs/06-webhooks.md)).
- **Both API path styles**: `/api/v1` (JWT-addressed) plus the named-repo aliases
  `/api/repos/{repo_name}/...` and `GET /api/repo-urls/{repo_id}`.
- **Operations built in**: per-client-IP rate limiting, per-repo pack quotas with durable
  admission reservations, orphan-pack sweeping, and background repack — durable
  [River](https://riverqueue.com) jobs on Postgres, in-process in local mode
  ([configuration reference](docs/reference/configuration.md)).
- **Commit-signing key registry** managed from the CLI
  ([`codestorage signing-key`](docs/reference/cli.md#codestorage-signing-key)).

## Configuration

`serve` is configured through `CODESTORAGE_*` environment variables. The essentials:

| Variable | Meaning |
|---|---|
| `CODESTORAGE_ORG` | **Required.** Org name (JWT `iss` claim) |
| `CODESTORAGE_ORG_PUBKEYS` | **Required.** PEM file with the org's public keys |
| `CODESTORAGE_PG_URL` | Database URL for refs + metadata: `postgres://`/`postgresql://` or `sqlite://` |
| `CODESTORAGE_S3_URL` | Pack storage URL: `s3://` bucket or `file://` directory |
| `CODESTORAGE_DATA_DIR` | Local mode: one directory that derives the two storage URLs above (org settings are still required) |

Every variable — rate limiting, trusted proxies, pack quotas, sweep and repack tuning,
legacy aliases — with defaults and validation constraints:
**[configuration reference](docs/reference/configuration.md)**. Flags and subcommands:
**[CLI reference](docs/reference/cli.md)**.

## Documentation

| | Start here |
|---|---|
| **Tutorials** | The two quickstarts above |
| **Reference** | [Configuration](docs/reference/configuration.md) · [CLI](docs/reference/cli.md) · [HTTP API](docs/03-http-api.md) · [Auth & JWT claims](docs/02-authentication.md) · [Webhooks](docs/06-webhooks.md) |
| **Explanation** | [Wire compatibility with code.storage](docs/explanation/wire-compatibility.md) · [The storage engine](docs/explanation/storage-engine.md) · [Domain model](docs/01-domain-model.md) · [Git protocol & namespaces](docs/05-git-protocol-and-namespaces.md) · [Streaming commit protocols](docs/04-streaming-commit-protocols.md) · [Spec index & decision log](docs/README.md) |
| **How-to** | [Deploy to production (multi-replica)](docs/how-to/deploy-production.md) · [Deploy a two-replica stack with Docker Compose](docs/how-to/deploy-docker-compose.md) · [Operate the storage lifecycle: repack, quotas, sweeping](docs/how-to/storage-lifecycle-and-sweeping.md) |

## Contributing & testing

```sh
go test ./...   # needs Docker: boots real Postgres + MinIO via testcontainers
```

Zero-container lanes cover the same surfaces on the local backends: the api integration
tests fan out into `/pg` + `/sqlite` subtests, and
`CODESTORAGE_TEST_BACKEND=local go test ./internal/githttp` runs the full git e2e suite on
`sqlite://` + `file://` (see [docs/11](docs/11-local-backends-implementation-notes.md) for
the lane table).

<details>
<summary>Repository layout</summary>

| Path | Contents |
|---|---|
| `docs/` | Backend specification (wire formats, auth, protocols) + decision log |
| `docs/reference/` | Operator reference: configuration, CLI |
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
| `docker-compose.yml` | Postgres + MinIO for local dogfooding, plus an opt-in two-replica `replicas` profile |

</details>
