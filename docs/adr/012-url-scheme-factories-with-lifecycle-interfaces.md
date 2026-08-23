# ADR-12: URL-scheme factories with lifecycle interfaces

> Extracted verbatim from [`11-local-backends-implementation-notes.md`](../archive/phases/11-local-backends-implementation-notes.md)
> (“Local-Backends Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

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
