# Building an Open-Source `code.storage` Backend — Specification Documents

This document set specifies the API surface of [code.storage](https://code.storage) (Pierre
Computer Company) precisely enough to build an open-source, wire-compatible backend from
scratch. A compatible backend can be dropped in via the SDKs' `apiBaseUrl` / `storageBaseUrl`
overrides without changing client code.

## Sources & methodology

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

## Document index

1. [01-domain-model.md](01-domain-model.md) — Entities, lifecycle, invariants a backend must uphold
2. [02-authentication.md](02-authentication.md) — Customer-signed JWTs, scopes, ref policies
3. [03-http-api.md](03-http-api.md) — Endpoint catalog, conventions (pagination, errors, headers)
4. [04-streaming-commit-protocols.md](04-streaming-commit-protocols.md) — NDJSON commit-pack / diff-commit / restore-commit
5. [05-git-protocol-and-namespaces.md](05-git-protocol-and-namespaces.md) — Smart HTTP, `+ephemeral` / `+import` namespaces, LFS
6. [06-webhooks.md](06-webhooks.md) — Outbound events, HMAC signatures
7. [07-implementation-roadmap.md](07-implementation-roadmap.md) — Phased build plan + conformance testing strategy
8. [08-spike-implementation-notes.md](08-spike-implementation-notes.md) — ADRs from the phase-0 de-risk spike
9. [09-phase1-implementation-notes.md](09-phase1-implementation-notes.md) — ADRs from phase 1 (skeleton + auth)
10. [10-phase2-implementation-notes.md](10-phase2-implementation-notes.md) — ADRs from phase 2 (git smart HTTP promotion)
11. [11-local-backends-implementation-notes.md](11-local-backends-implementation-notes.md) — ADRs from the pluggable-storage interlude (local mode: SQLite + file blobs)

`upstream/` contains verbatim copies of the official OpenAPI spec and doc pages for reference.

## The two API path styles

The official OpenAPI spec defines **two parallel path styles** for the same operations:

- **v1 API (`/api/v1/...`)** — what all three SDKs use today. The target repository is *not*
  in the path; it is identified by the JWT `repo` claim (e.g. `GET /api/v1/repos/commits`).
- **Named-repo API (`/api/repos/{repo_name}/...`)** — newer style where the repo is in the
  path (e.g. `GET /api/repos/{repo_name}/commits`); JWT still needs a matching `repo` claim.

An open-source backend targeting SDK compatibility must implement the **v1 API first**;
the named-repo style is a thin routing layer over the same handlers.
