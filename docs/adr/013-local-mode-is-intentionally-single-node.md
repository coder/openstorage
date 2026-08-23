# ADR-13: Local mode is intentionally single-node

> Extracted verbatim from [`11-local-backends-implementation-notes.md`](../archive/phases/11-local-backends-implementation-notes.md)
> (“Local-Backends Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** Local mode exists so tests, CI lanes, and single-machine deployments run with
zero external dependencies — not to replace the PG+S3 production stack. HA would demand a
networked DB and shared blob storage, which is exactly the stack local mode removes.

**Decision.** Local mode is one node, one directory: a single-writer SQLite file
(WAL + `busy_timeout` + `BEGIN IMMEDIATE` via DSN pragmas, shared by the serve process and
its re-exec'd hooks on the same host) and node-local `file://` blobs (gocloud fileblob,
packs as plain files). Multi-node setups keep using `postgres://` + `s3://`; nothing stops
a node from mixing (PG + `file://`) but nothing optimizes for it either.

- **Shared vs. split DB.** Ref state (`internal/ref`) and metadata (`internal/meta`) could
  live in two SQLite files — per-store isolation, independent locking — but both stores
  serve one logical deployment, and one `codestorage.db` means one file to back up, one
  WAL to checkpoint, and one obvious artifact for "hand me your data dir" debugging. The
  tables are disjoint, so the shared file changes no SQL. Split remains possible later by
  passing different URLs; the derivation just doesn't.
- **Derivation precedence.** `--data-dir <dir>` (or `CODESTORAGE_DATA_DIR`; flag wins)
  fills only the slots not set explicitly: `sqlite://<dir>/codestorage.db`,
  `file://<dir>/blobs`, cache `<dir>/cache`. Explicit `CODESTORAGE_PG_URL` /
  `CODESTORAGE_S3_URL` (and legacy aliases) and an explicit `--cache-dir` always win; the
  dir is made absolute up front because hooks re-exec from the cache repo's working
  directory, where a relative path would silently resolve elsewhere.

**Consequences.** A test or laptop deployment is `serve --data-dir` plus org keys — no
containers, no credentials. The cache dir stays a rebuildable cache even when it lives
inside the data dir: wiping it and restarting the node loses nothing (proved by the local
e2e lane). SQLite's single-writer lock serializes concurrent pushes harder than PG row
locks do; that is acceptable at single-node scale and irrelevant to the pg lane.
