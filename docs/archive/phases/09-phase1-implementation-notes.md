# Phase-1 Implementation Notes & ADRs

> **Archived.** Historical implementation notes, kept verbatim as the project's build
> history; decisions here may have been superseded by later work. For current
> behavior, start at the [docs index](../../README.md); the ADRs recorded below are
> individually addressable under [`docs/adr/`](../../adr/README.md).

Records the load-bearing decisions from phase 1 (skeleton + auth: `internal/meta`,
`internal/auth`, `internal/api`, and the `cmd/codestorage` wiring). The phase-0 core
decisions — PG as ref authority (ADR-1), S3 upload before the ref flip (ADR-2), namespace
mapping via separate ref stores (ADR-3) — carry into phase 1 unchanged and stay recorded in
[08-spike-implementation-notes.md](08-spike-implementation-notes.md): `serve` mounts
`/api/v1` alongside the phase-0 git path without touching it. Acceptance is covered by
container-based tests (real Postgres via testcontainers) plus the vendor's unmodified
TypeScript SDK driven against the in-process server.

## ADR-4: Frozen contracts first, one integration seam

**Context.** Phase 1 was built as parallel slices (meta store, auth, API service, CLI
wiring) that could not see each other's code while in flight.

**Decision.** The `meta.Store` and `auth.Verifier` interfaces — including sentinel errors
mapping onto the docs/03 status contract and the docs/02 scope/claim constants — were
declared and frozen *before* implementation started. `cmd/codestorage/wire.go` is the
single integration seam: the only file referencing sibling-slice constructors. An opt-in
`codestorage_stub` build tag swapped in `wire_stub.go` so the CLI could build and test
before the slices landed; the stub was deleted once they did — it was scaffolding for the
parallel build, not a supported mode.

**Consequences.** The frozen interfaces held and the slices merged without cross-slice
edits. The one drift was on the *unfrozen* constructor surface — the API slice shipped
`api.New(api.Config{...})` where the CLI anticipated `api.NewHandler(store, verifier)` —
and was fixed in `wire.go` alone, as designed. Lesson: freeze constructor signatures along
with the interfaces.

## ADR-5: Multi-org schema, single-org serve mode

**Context.** The vendor model is multi-tenant — JWTs are customer-signed and `iss` names
the org — but a self-hosted deployment has exactly one tenant. The auth path must stay
wire-identical either way.

**Decision.** Schema and verifier are fully multi-org (orgs, per-org public keys, `iss`
resolves the key set). `serve` provisions exactly one org at startup: `CODESTORAGE_ORG`
names it and `CODESTORAGE_ORG_PUBKEYS` supplies 1..n PEM public-key blocks (idempotent
upsert). `/api/v1` is mounted only when both are set; without them the binary serves the
phase-0 git path only.

**Consequences.** No single-tenant special cases in auth or the store: verification runs
the same `iss` → key set → algorithm-allow-list path a multi-tenant deployment would.
Growing to real multi-org later is provisioning work (org CRUD), not an auth change.

## ADR-6: Soft delete via a unique constraint spanning deleted rows

**Context.** docs/01 requires deletion to be soft-first, and docs/03 maps re-creating an
existing url onto 409 — including while a soft-deleted row still holds the url.

**Decision.** `repos` keeps one named `UNIQUE(org_id, url)` that deliberately spans
soft-deleted rows, so `CreateRepo` gets its 409 (`ErrAlreadyExists`) from the constraint
for free, with no live-vs-deleted casing. Reads exclude deleted rows (`deleted_at IS
NULL`), backed by a partial keyset index on `(org_id, created_at DESC, repo_id DESC)`.

**Consequences.** A re-create can never silently resurrect or shadow a deleted repo. The
trade-off: a url stays occupied until its row is hard-deleted, so the async cleanup that
docs/01 prescribes after a soft delete is what will free names for reuse.

## Smaller notes

- **Listen before building handlers.** `api.Config.HTTPBaseURL` — the base of the
  `http_url` values `POST /repos` returns — is required and asserted at construction. The
  default listen address is `127.0.0.1:0`, so `serve` binds the listener first and derives
  the base URL from the actually bound address.
- **All verifier failures are 401.** `auth.Verifier` only reports verification failures
  (parse/signature/exp/iss); scope and repo-claim checks — the 403s of docs/02 — belong to
  the API middleware. Each half of the failure map lives in exactly one layer.
- **Cursor encoding settled.** Listing uses keyset pagination (`created_at DESC, repo_id
  DESC`) with an opaque cursor: unpadded base64url of `<created_at_unixnano>|<repo_id>`.
  Malformed cursors map onto 400. This resolves the roadmap's open item.
- **Legacy env propagation for hooks.** `serve` re-exports the resolved PG/S3 URLs under
  the spike-era env names so the re-exec'd `codestorage hook` processes keep working no
  matter which alias configured the server.
- **SDK conformance starts now, not at phase 6.** The api tests drive the vendor's
  unmodified TypeScript SDK end-to-end against the in-process server (skipping loudly when
  the SDK checkout or module proxy is unavailable) — the docs/07 conformance strategy
  applied from the first phase.

## Post-integration hardening

A review pass after the slices merged closed the phase; each item below corrects a
non-obvious assumption made during the parallel build.

- **Repo-claim checks completed in the middleware.** Per-repo routes reject tokens whose
  `repo` claim is empty or `"org"` with 403 `token is not repo-scoped`; the handler-level
  400 on create became an assert (unreachable behind the gate). Together with "all verifier
  failures are 401" above, each half of the docs/02 failure map now lives in exactly one
  layer.
- **Multi-signature JWS is input, not an invariant.** `jws.Parse` auto-detects JSON
  serializations, and a JSON-serialized JWS can contain exactly two `.` characters —
  slipping past the compact-form pre-check — so the signature count is attacker-controlled.
  Verification returns a 401-mapped error for multi-signature tokens instead of asserting
  (regression-tested for the flattened and general JSON forms).
- **`default_branch` precedence: body → query → `"main"`.** Upstream's `create_repo` takes
  an optional `?default_branch=` query parameter used only when the body omits one; the
  query value goes through the same branch-name validation as the body value.
- **`GET /repo` returns the full upstream schema.** `{repo_id, url, default_branch,
  created_at}` — the docs/03 summary table listed only the last two and was corrected
  against `upstream/openapi.yaml`.
- **Log outside recover.** Middleware order is `logRequests(recoverPanics(mux))` so a
  panicked (500-mapped) request still produces a request log line.
- **API tests run the production constructors.** The `internal/api` test env is built
  through `meta.OpenPG` + `InitSchema` + `auth.NewVerifier` — the same wiring as `wire.go`
  — replacing package-local doubles; the duplicated stub-backed test files were deleted.
