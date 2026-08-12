# Phase-7 Implementation Notes & ADRs (deployment packaging + p6 burn-down)

Phase 7 delivers the deployment-packaging deliverable that the p6 gate carried forward —
a multi-stage `Dockerfile` and an opt-in two-replica compose profile, with quickstarts in
the root `README.md` — plus the p6 deferred-findings burn-down. The burn-down closures
(alias path-id git-credentials routes, `createAbandon` row cleanup, rate-limiter polish,
assertf entry guards) are recorded inline against their entries in docs/15 "Open items",
which is the phase-7 burn-down record; this document covers the packaging decisions and
the one storage-layer fix the packaging work surfaced.

No new storage decisions were needed: the replicas profile is the first *packaged* form
of the spike's core claim — stateless nodes over PG as ref authority (ADR-1) and S3 as
immutable pack storage published before the ref flip (ADR-2). What concurrent from-scratch
boot did surface is that "PG is the authority" extends to *schema DDL*: every startup
migration path must be advisory-lock-serialized, and river's was not (ADR-40).

## ADR-38: Runtime image is alpine + external git; the binary is static

Decision: **multi-stage build — `golang:1.26-alpine` with `CGO_ENABLED=0 -trimpath` and
module/build cache mounts, then an `alpine:3.21` runtime stage with `git` and
`ca-certificates`, a non-root user, `ENTRYPOINT ["codestorage"]` / `CMD ["serve"]`.**

- **Alpine, not distroless/scratch.** The server has a hard runtime dependency on
  external `git` (smart-HTTP upload/receive-pack, merges, hook re-execs); a
  binary-only image cannot serve the git surface at all. Alpine is the smallest base
  that carries a packaged git.
- **`CGO_ENABLED=0` is safe and desired.** Both storage stacks are pure Go (pgx,
  gocloud, `modernc.org/sqlite`), so disabling cgo costs nothing and yields a static
  binary that is independent of the runtime image's libc.
- **`CMD ["serve"]` only defaults the subcommand**: the same image runs `keygen`,
  `mint`, `signing-key`, etc. via `docker run <img> <subcommand>` — the compose
  quickstart deliberately mints tokens with the *host* binary instead, so tokens and
  private keys never depend on a running container.

## ADR-39: Replicas are an opt-in profile of the dev compose file, symmetric by construction

Decision: **the two-node deployment lives in the existing `docker-compose.yml` as a
`replicas` profile — `node-a`/`node-b` defined via YAML anchors, differing only in host
port mapping and the `CODESTORAGE_BASE_URL` that embeds it.**

- **One compose file, not a second deployment file.** The nodes reuse the same
  `postgres`/`minio` service definitions the dev quickstart boots; a parallel file
  would drift. `profiles: ["replicas"]` keeps the default `docker compose up`
  behavior unchanged (backing services only).
- **Symmetry is load-bearing.** An earlier draft serialized `node-b` behind
  `node-a` (`depends_on: service_healthy`) to dodge a fresh-boot migration race.
  That workaround was dropped and the race fixed at the root (ADR-40): the profile
  exists to *prove* multi-node statelessness, and an asymmetric boot order would
  mask exactly the class of races it is supposed to demonstrate absent.
- **Healthcheck is a TCP probe** (`nc -z`, present in the alpine runtime): the server
  has no unauthenticated HTTP endpoint, and inventing one just for compose would widen
  the surface. A listener probe is sufficient for `up --wait`.
- **Keys stay on the host.** The org keypair is generated on the host
  (`codestorage keygen --out ./compose-keys/cs-key`) and the public key is mounted
  read-only into the nodes; `/compose-keys/` is gitignored. Runtime keys are never
  baked into the image or committed — the standing key policy from the quickstarts.

## ADR-40: River migrations run under a session-level advisory lock

Concurrent from-scratch boot of the two replicas surfaced a latent race:
`rivermigrate` runs its DDL with no locking of its own, so two fresh nodes migrating
simultaneously collide on creating river's tables (SQLSTATE 23505 duplicate key), and
the loser exits. Decision: **`jobs.NewRiver` serializes migrations under
`pg_advisory_lock(0x5249564552)` (“RIVER”), mirroring the meta/ref `InitSchema` locks —
every startup DDL path must be advisory-lock-serialized, including third-party ones.**

- **Session-level lock on a dedicated pool conn**, not `pg_advisory_xact_lock`:
  the meta/ref stores lock inside their own DDL transaction, but
  `rivermigrate.Migrate` manages its transactions internally, so there is no caller
  transaction to scope a xact lock to.
- **The unlock must survive context cancellation.** A canceled ctx is precisely the
  case where `Migrate` just failed and the lock is still held; the unlock runs on a
  `context.WithoutCancel` 10-second deadline.
- **A failed unlock destroys the connection instead of pooling it.** Releasing a conn
  that still holds the session lock back into the pool poisons it — the next boot's
  `pg_advisory_lock` on that pooled conn blocks forever. On unlock failure the
  underlying conn is closed so `Release` discards it and the server drops the
  advisory lock with the session.

## Burn-down addendum: alias git-credentials POST

Gate dogfood on the replicas stack found one more alias gap beyond the docs/15 list:
`POST /api/repos/{repo_name}/git-credentials` answered 400. The openapi body for the
named-repo form is `{username?, password}` — no `repo_id` — but the v1 handler the alias
re-dispatches to (ADR-36) required one. Fix: an absent body `repo_id` falls back to the
token's `repo` claim. This is safe by construction: `requireAuth(orgLevel=false)`
guarantees the claim is non-empty and non-`"org"`, and `aliasHandler` has already
matched it against the path name — the fallback can only name the repo the alias
resolved. When both are present, body `repo_id` and claim must still agree.

## Gate outcome and remaining deferrals

The first p7 gate returned NO-GO solely for missing dogfood evidence (the dogfood run
was interrupted after compose boot; all static gates were green at `68b1bf8`). The
dogfood-only re-gate was executed against the still-running `cs-p7pkg` stack and
**PASSED 2/2**:

- **Flow 1 (multi-node statelessness):** repo created via node A, push via node A,
  clone via node B — identical HEAD on both sides; one API read per node (v1 on A,
  named alias on B). Zero `level=ERROR` lines in either replica log.
- **Flow 2 (path-id git-credentials):** create via node A → PUT path-id via node B
  (200, body shape identical to the v1 form) → DELETE path-id (204) → 404 result
  envelopes for PUT/DELETE on the deleted id.
- A live probe confirmed the alias POST returns **201** (spec-conformant status).

Evidence: transcripts, replica logs, and flow scripts under
`~/codestorage-evidence/p7-packaging-deferrals/`.

**Remaining deferrals (non-blocking, end-of-roadmap backlog):**

1. **P2 — alias git-credentials POST body omits `created_at`** (`handlers_credentials.go`):
   openapi.yaml specifies `201 {id, created_at}`; live probe shows `201 {id}`.
2. **P2 — test-coverage gaps**: no DELETE-by-repo-id (v1 body form) or malformed-JSON
   PUT cases in `credentials_test.go`.
3. **P3 ×5** (polish): negative-RPS silently disables rate limiting vs ADR-37 wording;
   advisory-lock release on panic paths; redundant `ResolveRepoKey` lookup in the alias
   path; `internalErrResult` duplication; compose healthchecks are TCP-only.

