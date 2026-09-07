# open.storage documentation

Documentation for open.storage, the open-source backend wire-compatible with
[code.storage](https://code.storage) (Pierre Computer Company), organized by
[Diátaxis](https://diataxis.fr/) quadrant.

## Navigation

| Quadrant | Start here |
|---|---|
| **Tutorials** | The [README quickstarts](../README.md#quickstart-local-mode-60-seconds): local mode in 60 seconds, multi-node with Docker Compose, zero-infrastructure mode on one bucket (experimental) |
| **How-to guides** | [Migrate to open.storage](how-to/migrate-to-openstorage.md) · [Deploy to production](how-to/deploy-production.md) · [Run the zero-infrastructure mode (s3wal)](how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node) · [Deploy a two-replica stack with Docker Compose](how-to/deploy-docker-compose.md) · [Operate the storage lifecycle](how-to/storage-lifecycle-and-sweeping.md) · [Cut a release](how-to/release.md) |
| **Reference** | [Configuration](reference/configuration.md) · [CLI](reference/cli.md) · [Testing](reference/testing.md) · [HTTP API](reference/http-api.md) · [Authentication & JWT claims](reference/authentication.md) · [Webhooks](reference/webhooks.md) |
| **Explanation** | [Wire compatibility](explanation/wire-compatibility.md) · [The storage engine](explanation/storage-engine.md) · [Domain model](explanation/domain-model.md) · [Git protocol & namespaces](explanation/git-protocol-and-namespaces.md) · [Streaming commit protocols](explanation/streaming-commit-protocols.md) |

Two further directories sit outside the quadrants:

- **[`adr/`](adr/README.md)** — standalone architecture decision records (ADR-1..46),
  extracted from the phase notes; historical, individually addressable.
- **[`archive/`](archive/phases/07-implementation-roadmap.md)** — the original
  [implementation roadmap](archive/phases/07-implementation-roadmap.md) and the per-phase
  implementation notes (`archive/phases/08` … `16`), preserving historical workflows
  with current project naming. Recorded evidence paths and upstream names are unchanged.
  **Historical material**: for current behavior, prefer the reference and explanation
  pages above.
- **`upstream/`** — verbatim copies of the vendor's official OpenAPI spec and doc pages,
  kept for wire-compatibility auditing.

Contributor entry points: [CONTRIBUTING](../CONTRIBUTING.md) (setup, gates, review bar)
and the [testing reference](reference/testing.md).

## The specification set: sources & methodology

Five of the pages above — [domain model](explanation/domain-model.md),
[authentication](reference/authentication.md), [HTTP API](reference/http-api.md),
[streaming commit protocols](explanation/streaming-commit-protocols.md),
[git protocol & namespaces](explanation/git-protocol-and-namespaces.md), and
[webhooks](reference/webhooks.md) — specify the API surface of code.storage (Pierre
Computer Company) precisely enough to build a wire-compatible backend from scratch. A
compatible backend can be dropped in via the SDKs' `apiBaseUrl` / `storageBaseUrl`
overrides without changing client code.

Claims were extracted independently from three parallel SDK implementations and cross-checked
against the vendor's own documentation:

| Source | What it authoritatively defines |
|---|---|
| `sdk/packages/code-storage-typescript` | Wire formats, JWT claims, NDJSON streaming, webhooks |
| `sdk/packages/code-storage-python` | Same surface, confirms wire formats (convergent) |
| `sdk/packages/code-storage-go` | Same surface, confirms wire formats (convergent) |
| `just-code-storage` (real consumer) | Semantics clients depend on (sync, conflicts, status codes) |
| `upstream/openapi.yaml` (official) | Full HTTP endpoint catalog incl. paths not used by SDKs |
| `upstream/getting-started/`, `upstream/guides/` (official) | Auth model, webhooks, LFS, git operations prose |

The three SDKs implement an **identical** wire surface (endpoints, claims, chunk sizes,
signature scheme), which gives high confidence that the extracted spec is correct.

## The two API path styles

The official OpenAPI spec defines **two parallel path styles** for the same operations:

- **v1 API (`/api/v1/...`)** — what all three SDKs use today. The target repository is *not*
  in the path; it is identified by the JWT `repo` claim (e.g. `GET /api/v1/repos/commits`).
- **Named-repo API (`/api/repos/{repo_name}/...`)** — newer style where the repo is in the
  path (e.g. `GET /api/repos/{repo_name}/commits`); JWT still needs a matching `repo` claim.

An open-source backend targeting SDK compatibility must implement the **v1 API first**;
the named-repo style is a thin routing layer over the same handlers.
