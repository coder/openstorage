# Local-Backends Implementation Notes & ADRs

Records the load-bearing decisions from the pluggable-storage phase (local mode): swapping
Postgres + S3 for SQLite + local files behind URL-scheme factories, so one binary serves
either stack. Every prior decision — PG-style CAS as the ref authority (ADR-1), blob upload
strictly before the ref flip (ADR-2), namespace mapping via separate ref stores (ADR-3),
frozen contracts + one integration seam (ADR-4), single-org serve mode (ADR-8) — carries
over unchanged; the backends underneath became selectable.

## ADR-12: URL-scheme factories with lifecycle interfaces

**Context.** `serve`, the re-exec'd hooks, and the CLI helpers all constructed concrete
stores (`ref.OpenPG`, `blob.OpenS3`, `meta.OpenPG`). Adding local backends must not fork
those call sites per backend, and hooks reconstruct stores from env URLs across a process
boundary — the selection mechanism has to survive re-exec with zero extra plumbing.

**Decision.** Each storage package exposes one factory dispatching on `url.Parse(...).Scheme`:
`ref.Open` (`postgres://`/`postgresql://` → `OpenPG`, `sqlite://` → `OpenSQLite`),
`blob.Open` (`s3://` → `OpenS3`, `file://` → `OpenFile`), `meta.Open` (same DB dispatch);
unknown schemes fail with the supported list. Consumers keep coding against the frozen
contracts (`ref.RefStore`, `meta.Store`); new lifecycle interfaces (`ref.Store`,
`meta.Backend` = contract + `InitSchema`/`Close`) exist only so the wire seam can init
schema and shut down without naming concrete types. Factories never run `InitSchema` —
hooks must not run DDL; the serve/wire seam owns schema init, exactly as before.
Two same-shape decisions ride along:

- **`Pack.S3Key` → `Pack.Key`.** The field names the blob-store object key, which is no
  longer necessarily S3. The backing SQL column stays `s3_key` on **both** backends so the
  pack SQL is byte-comparable across them and existing PG deployments need no migration.
- **`modernc.org/sqlite`** (pure Go) over `mattn/go-sqlite3` (cgo): codestorage ships as a
  cross-compiled single binary, and the hook path re-execs that binary — a cgo toolchain
  requirement per target platform would poison exactly the deployment story local mode
  exists for. The dependency is large, accepted deliberately.

**Consequences.** Call sites select a backend by URL alone; serve re-exports the resolved
URLs to the hook environment, so hook re-exec → factory dispatch works across the process
boundary with sqlite/file URLs the same way it did with postgres/s3. Every backend
implements the same contract tests; scheme typos fail fast at startup.

## ADR-13: Local mode is intentionally single-node

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

## Test surface: which lanes run zero-container

Container-based acceptance (testcontainers PG + MinIO) remains the default; every new
backend rides the same suites through a lane switch instead of a parallel test tree.

| Lane | How to run | Containers |
|---|---|---|
| api integration matrix (`/sqlite` subtests) | `go test ./internal/api` — every integration test fans out `t.Run("pg")` / `t.Run("sqlite")` via `runBackends`; the sqlite lane opens a temp-dir DB | pg lane only |
| githttp e2e, local lane | `CODESTORAGE_TEST_BACKEND=local go test ./internal/githttp` — TestMain derives `sqlite://`+`file://` URLs from a temp data dir; whole suite incl. hook re-exec, plus the local-only on-disk/stateless-cache test | none |
| githttp e2e, pg lane | `go test ./internal/githttp` (default) | PG + MinIO |
| ref/blob/meta unit lanes | `go test ./internal/ref ./internal/blob ./internal/meta` — sqlite store, fileblob, and sqlite meta tests run on temp dirs; PG-specific tests keep their lazy container | PG tests only |

Selecting only zero-container lanes (e.g. `-run '/sqlite'`, `CODESTORAGE_TEST_BACKEND=local`)
runs with `docker ps` empty — the distribution story local mode exists for, applied to CI.
