# open.storage documentation

open.storage is an open-source Git storage backend, wire-compatible with
[code.storage](https://code.storage) (Pierre Computer Company). These docs follow the
[Diátaxis](https://diataxis.fr/) layout: tutorials, how-to guides, reference, and
explanation.

New here? Start with the [README quickstart](../README.md#quickstart-local-mode-60-seconds).

<a name="navigation"></a>

## Tutorials

- [README quickstarts](../README.md#quickstart-local-mode-60-seconds): local mode in
  60 seconds, two replicas with Docker Compose, and one S3 bucket (experimental).

## How-to guides

**Try and deploy**

- [Try with Docker only](how-to/quickstart-docker.md)
- [Deploy a two-replica stack with Docker Compose](how-to/deploy-docker-compose.md)
- [Deploy to production](how-to/deploy-production.md)
- [Deploy the AWS EC2 reference (Terraform)](how-to/deploy-aws-terraform.md)
- [Migrate to open.storage](how-to/migrate-to-openstorage.md)

**Operate**

- [Operate the storage lifecycle: repack, quotas, sweeping](how-to/storage-lifecycle-and-sweeping.md)
- [Run the zero-infrastructure mode (s3wal, experimental)](how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node)
- [Back up and restore a stopped S3WAL node](how-to/backup-restore-s3wal.md)

**Maintain the project**

- [Cut a release](how-to/release.md)
- [Test an official-release upgrade locally](how-to/test-release-upgrade.md)
- [Retain private SeaweedFS test logs](how-to/capture-seaweedfs-test-logs.md)

## Reference

| Page | Covers |
|---|---|
| [Configuration](reference/configuration.md) | Every `OPENSTORAGE_*` variable, defaults, and validation |
| [CLI](reference/cli.md) | `openstorage` commands and flags |
| [HTTP API](reference/http-api.md) | Endpoint catalog, scopes, and the status-code contract |
| [Authentication](reference/authentication.md) | Customer-signed JWTs, claims, scopes, and ref policies |
| [Webhooks](reference/webhooks.md) | Endpoint management, delivery headers, signatures, and event payloads |
| [Testing](reference/testing.md) | Test lanes, conventions, fuzzing, and benchmarks |

## Explanation

| Page | Explains |
|---|---|
| [Wire compatibility](explanation/wire-compatibility.md) | What matches code.storage, and the known deviations |
| [The storage engine](explanation/storage-engine.md) | Immutable packs, ref compare-and-swap, garbage collection, and s3wal |
| [Domain model](explanation/domain-model.md) | Entities, ref namespaces, and the invariants a backend must uphold |
| [Git protocol and namespaces](explanation/git-protocol-and-namespaces.md) | Git smart HTTP, remote URLs, LFS, and refs over Git |
| [Streaming commit protocols](explanation/streaming-commit-protocols.md) | NDJSON commits from file operations or patches, and restore commits |

## Other directories

| Directory | Contents |
|---|---|
| [`adr/`](adr/README.md) | Architecture decision records (ADR-1 to ADR-46). Historical and individually addressable. |
| [`archive/`](archive/phases/07-implementation-roadmap.md) | The original implementation roadmap and per-phase notes (`archive/phases/08` to `16`). Historical: for current behavior, prefer reference and explanation pages. Recorded evidence paths and upstream names are unchanged. |
| `upstream/` | Verbatim copies of the vendor's OpenAPI spec and docs, kept for compatibility audits. |

Contributors: start with [CONTRIBUTING](../CONTRIBUTING.md) and the
[testing reference](reference/testing.md).

<a name="the-specification-set-sources--methodology"></a>

## How the API specification was built

Six pages specify the code.storage API surface precisely enough to build a compatible
backend: [domain model](explanation/domain-model.md),
[authentication](reference/authentication.md), [HTTP API](reference/http-api.md),
[streaming commit protocols](explanation/streaming-commit-protocols.md),
[Git protocol and namespaces](explanation/git-protocol-and-namespaces.md), and
[webhooks](reference/webhooks.md). A compatible backend drops in through the SDKs'
`apiBaseUrl` / `storageBaseUrl` overrides, with no client code changes.

Each claim was extracted independently from three SDKs and cross-checked against the
vendor's docs:

| Source | Defines |
|---|---|
| `sdk/packages/code-storage-typescript` | Wire formats, JWT claims, NDJSON streaming, webhooks |
| `sdk/packages/code-storage-python` | Same surface; confirms wire formats |
| `sdk/packages/code-storage-go` | Same surface; confirms wire formats |
| `just-code-storage` (real consumer) | Semantics clients rely on: sync, conflicts, status codes |
| `upstream/openapi.yaml` (official) | Full endpoint catalog, including paths the SDKs do not use |
| `upstream/getting-started/`, `upstream/guides/` (official) | Auth model, webhooks, LFS, and Git operations |

All three SDKs implement the same wire surface (endpoints, claims, chunk sizes,
signature scheme), which gives high confidence in the extracted spec.

<a name="the-two-api-path-styles"></a>

## Two API path styles

The official spec defines two path styles for the same operations:

| Style | Example | Repository comes from |
|---|---|---|
| **v1** (`/api/v1/...`) — used by all three SDKs | `GET /api/v1/repos/commits` | The JWT `repo` claim |
| **Named-repo** (`/api/repos/{repo_name}/...`) — newer | `GET /api/repos/{repo_name}/commits` | The path; the JWT still needs a matching `repo` claim |

Implement v1 first for SDK compatibility. The named-repo style is a thin routing layer
over the same handlers.
