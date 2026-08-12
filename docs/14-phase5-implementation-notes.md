# Phase-5 Implementation Notes & ADRs (eventing, upstream sync, credentials, fork)

Phase 5 delivers the docs/06 outbound-webhook surface (`internal/events`), upstream mirror
sync with the loopback-push worker (`internal/sync`, `internal/api/handlers_sync.go`),
git-credential and webhook CRUD (`handlers_credentials.go`, `handlers_webhooks.go`), and
synchronous repo forking (`handlers_fork.go`) — plus the workstream-F carry-over backlog
recorded at the end. Contract sources: docs/01 invariants 11–12, docs/02, docs/03
"Upstream sync"/"Git credentials", docs/06. Route registration and process assembly are
described in "Integrator wiring" below; the cross-process queue story for git-push
events is ADR-33.

Every prior decision — PG as ref authority (ADR-1), blob upload strictly before the ref
flip (ADR-2), namespace mapping via separate ref stores (ADR-3) — still holds and is
load-bearing here: push events hang off a decorator on the ref authority (ADR-28), and
the sync worker's loopback push reuses the S3-before-ref-flip write pipeline instead of
growing a second one (ADR-30).

## ADR-27: Jobs are a queue seam — River on Postgres, in-process goroutines in local mode

Webhook deliveries and upstream syncs are background work. Decision: **the queue is a
narrow seam, not a fixed implementation.** The consumer-side contracts
(`events.Delivery` + enqueue funcs, `sync.Queue`/`sync.Job`) are owned by the eventing and
sync packages; the Postgres deployment plugs in a durable River-backed jobs package, while
local mode ships an in-process implementation (`sync.InProcessQueue`, and
`events.Deliverer.Deliver` looping attempts with backoff on a goroutine).
`events.Deliverer` is deliberately shaped for both backends: River workers call `Attempt`
once per job attempt (River owns backoff/attempt accounting and durability), the local
queue calls `Deliver` which owns the retry loop itself. Every attempt re-signs with the
current time, so retries never carry stale signatures.

Accepted trade-off: **local-mode queued work is lost on process restart.** Every queued
action is re-derivable — a lost sync re-enqueues on the next push or `pull-upstream`, and
webhook delivery is best-effort with bounded retries (docs/06) — so local mode trades
durability for zero infrastructure. `InProcessQueue` also coalesces per repo (a job queued
while one is still waiting is dropped; the waiting run observes the same upstream state),
which is exactly the semantics invariant 11 needs.

## ADR-28: Push events are emitted by a CASRef decorator, not by handlers

Every ref mutation on this server — API commits, merges, branch/tag/notes writes, git
push via the receive hook, and the sync worker's loopback pushes — funnels through
`ref.RefStore.CASRef`. Decision: **`push` events (docs/01 invariant 7) are emitted by a
`RefStore` decorator (`events.NewRefStore`) wrapping the ref authority**, not by
individual handlers. The decorator fires only on *won*, *non-ephemeral* CAS flips, builds
the payload from a `RepoSource` lookup (repo url, customer id), fans out one delivery per
registered webhook, and optionally pokes `EnqueueSync` (pushes to a repo with an upstream
schedule a sync — invariant 11). Rationale: a single choke point cannot be forgotten by
the next write endpoint; handler-side emission would already have five call sites to keep
honest. Enqueue failures are logged, never propagated — the ref flip has already
committed, and un-notified is recoverable while un-flipped is not.

## ADR-29: Webhook endpoint management is a self-defined CRUD surface

docs/06 specifies delivery (headers, HMAC signing, retries) but upstream manages webhook
*registration* in its closed dashboard/control-plane — there is no SDK surface to be
compatible with. Decision: **the OSS backend defines its own org-scoped CRUD routes**
(`POST/GET /api/v1/webhooks`, `DELETE /api/v1/webhooks/{id}`), following the
git-credentials precedent: org-wide tokens with `repo:write`. The signing secret is
returned exactly once — from `POST`, server-generated when the body omits it — and `GET`
never echoes secrets.

## ADR-30: Upstream sync = mirror fetch + loopback push through our own front door

The sync worker fetches from the upstream provider and then performs a **real `git push`
against this server's own smart-HTTP endpoint** (the loopback), authenticated with a
short-lived minted `git:write` JWT passed via `GIT_ASKPASS` (never logged). Rationale:
the receive path already implements pack upload/registration, ref policies, CAS ref
flips, and push-event emission; a direct-to-store sync writer would be a second,
divergence-prone implementation of all four. The loopback reuses them by construction.
Overlap is prevented by `BeginSync`'s atomic claim (`ErrSyncInProgress` → silent skip,
invariant 11), and the run lifecycle emits the documented `repo.sync.started/succeeded/
failed` events with the exact docs/06 error strings.

Two consequences of the loopback design are enforced explicitly. The loopback push runs
with an internally minted token that carries no ref policies, so `handlePullUpstream`
checks the *caller's* policies before enqueuing — any restrictive policy op denies
pull-upstream fail-closed rather than letting the worker bypass it. And because
`upstream_host` becomes a fetch URL, `validateSyncBaseRepo` accepts only a bare
`hostname[:port]` or an http(s) base URL, and the worker pins `GIT_ALLOW_PROTOCOL`
(default `http:https`) so `file://`/`ssh://` and friends cannot be smuggled in.

## ADR-31: Forking is synchronous — a one-shot deterministic clone inside POST /repos

`base_repo: {provider: "code"|"code.storage", operation: "fork", auth: {token}}` forks an
existing repo on this server (docs/01 invariant 12). Decision: **the fork completes
synchronously inside the `POST /repos` request** (`handlers_fork.go`), unlike provider
sync, which is async (202 + background worker).

Rationale. A fork's source lives in the *same* ref/blob stores — there is no network
fetch, so the work is bounded by packing the fork-point closure from the already
materialized source cache. The client contract also demands it: `POST /repos` answers
200 with a `repo_id`/`http_url` the SDK immediately clones or reads, so an async fork
would need either a lying response or a polling surface that docs/03 does not define.
Finally, the local-mode queue loses jobs on restart (ADR-27): an async fork could leave a
permanently empty shell repo, while the synchronous flow is atomic-or-abandoned —
everything up to and including packing happens *before* the repo row is created, and any
failure after the row exists soft-deletes it (`forkAbandon` — which detaches from the
possibly-canceled request context via `context.WithoutCancel` plus a bounded timeout, so
a client disconnect mid-failure still cleans up), so no orphaned live repo
remains. A lost race or crash can strand at most an unreferenced immutable pack, the same
debris every write pipeline may leave.

Mechanics (all in `handlers_fork.go`):

- **Dispatch.** `forkAwareCreateRepo(verifier, fallback)` reads the bounded body once and
  probes `base_repo`; only `operation: "fork"` with provider `code`/`code.storage` is
  handled here. Everything else — no `base_repo`, sync operations, non-code fork
  providers, unparseable bodies — replays the body verbatim to the fallback creator, so
  the sync-aware creator (handlers_sync.go) keeps owning its own validation. The fallback
  parameter is the integration seam; the exact `api.New` registration line is in the file
  header.
- **Source authorization.** The embedded `base_repo.auth.token` is verified with the same
  production `auth.Verifier` and must be repo-scoped with `git:read` on the source
  (missing token → 400; unverifiable/expired/wrong-scope/org-scoped → 403). The source
  org comes from that token's `iss`, so cross-org forks need nothing beyond a valid
  source-read JWT.
- **Fork point.** `sha` overrides `ref`; default is the source's default branch, resolved
  in the materialized source cache (`gitread.ResolveRef` — branch/tag/full-or-short sha,
  peeled to a commit). Unresolvable fork points are invalid input (400).
- **Independence.** The fork-point reachability closure is packed with no negatives and
  published under the **new** RepoID's storage keys (ADR-21: RepoID is the only storage
  key — no pack sharing, no copy-on-write coupling of two repos' GC lifetimes), then the
  fork's default branch (request `default_branch` honored; source's name inherited
  otherwise) is CAS-created from the zero SHA. Exactly one ref exists afterwards: no
  source branches, tags, or ephemeral refs leak. **No `repo_upstreams` row is written** —
  a fork is one-shot and never tracks its source; the caller's ref policies are enforced
  on the initial ref like any other HTTP write.

## ADR-32: Upstream credentials and webhook secrets are stored plaintext

`sync.Credential.Password` (upstream git credentials) and `events.Webhook.Secret` (HMAC
signing keys) are stored as given. Decision: **at-rest secret encryption is out of scope
for phase 5.** Both secrets are *used* in cleartext on every consumption — basic-auth
material for upstream fetches, per-delivery HMAC signing — so reversible encryption
without a KMS/keystore seam would be obfuscation, not a security boundary, and this
self-host deployment has no such seam yet. Compensating rules that ARE enforced: secrets
are never logged (worker and delivery code paths keep them out of errors and slog
fields), webhook secrets are shown exactly once at registration (ADR-29), and credential
reads are scoped to the sync worker. Revisit when a KMS/keystore seam exists.

## ADR-33: Git-push eventing crosses processes — durable River inserts on pg, synchronous deliveries in local mode

Git pushes mutate refs in the `codestorage hook` subprocess (the pre-receive/update
re-exec), not in the serve process, so ADR-28's CASRef decorator needs a queue reachable
*across processes* there. Decision: **queue construction dispatches on the metadata DB
URL scheme** (the same dispatch as `meta.Open`/`ref.Open`), with asymmetric semantics:

- **Postgres:** the hook builds an insert-only River client
  (`jobs.NewRiverInsertOnly` — no migrations; hooks never run DDL) and inserts durable
  delivery/sync jobs that the serve process's full River client works. Serve owns the
  schema: `jobs.NewRiver` runs `rivermigrate` at startup, and sync jobs are unique per
  repo across active job states (`river.UniqueOpts`), so duplicate enqueues collapse —
  the durable twin of `InProcessQueue`'s coalescing (invariant 11).
- **Local mode:** there is no shared durable queue, so the hook delivers webhooks
  *synchronously* with bounded attempts (3) before the hook exits — the subprocess IS
  the queue — and git-surface pushes do **not** schedule upstream syncs (API writes
  still do). Accepted gap, consistent with ADR-27's durability trade-off: a missed sync
  re-enqueues on the next API write or `pull-upstream`.

Failure stance is fail-open: if the hook cannot open the meta store or build the queue,
eventing is disabled for that push rather than the push denied — same reasoning as
ADR-28, un-notified is recoverable while un-pushed work is a user-visible outage.

## Integrator wiring

The phase-5 routes landed in `api.New` (api.go) as follows:

- `RegisterWebhookRoutes` — `POST/GET /api/v1/webhooks`, `DELETE /api/v1/webhooks/{id}`
  (ADR-29), backed by `events.MetaWebhookStore{Store: cfg.Store}`.
- `registerCredentialRoutes` + `registerSyncRoutes` on the `syncServer` —
  git-credentials CRUD, `POST /api/v1/repos/pull-upstream`, `DELETE /api/v1/repos/base`.
- `POST /api/v1/repos` is now `s.forkAwareCreateRepo(cfg.Verifier, ss.handleCreateRepo)`:
  code/code.storage fork specs dispatch to the synchronous fork (ADR-31), everything
  else replays to the sync-aware creator, which embeds the frozen phase-1 create flow.
- `Config.SyncQueue` (a `sync.Queue`) is a new **required** field; the serve command
  passes the jobs seam, tests may pass a recorder/no-op.

Supporting pieces outside `internal/api`:

- **Persistence:** `meta.EventingStore` (internal/meta/eventing.go) declares the
  webhook / git-credential / repo-upstream rows plus the atomic
  `TryBeginSync`/`FinishSync` claim, implemented on both Postgres and SQLite and
  embedded into `meta.Store`. The thin adapters `events.MetaWebhookStore` and
  `sync.MetaStores` map it onto the slice-owned store contracts (type and sentinel
  drift stays at the boundary; the frozen packages did not change).
- **Serve assembly** (cmd/codestorage/wire.go + wire_eventing.go):
  `buildServeEventing` picks the queue by DB scheme (ADR-27/33), builds the webhook
  deliverer and sync worker, and returns the `events.NewRefStore` decorate func that
  wraps the API's `RefStore` — so API ref writes emit push events. The hook subprocess
  gets the same decorator via `wireHookEventing` setting `githttp.WrapHookRefStore`
  (ADR-33).
- **Loopback signing key:** serve generates a per-process ES256 keypair and appends its
  public half to the org's registered keys at bootstrap (`UpsertOrgWithKeys`), so the
  sync worker's internally minted short-lived (15 min) `git:write` JWTs verify at our
  own smart-HTTP endpoint (ADR-30). The key rotates every process start and the private
  half never leaves memory.

## Workstream-F carry-over backlog (landed with this slice)

Fix-where-touched items from the phase-5 plan that landed alongside workstream E:

1. **HTTP-status→docs/04 status map centralized** — the three duplicated tables
   (`wcFail`'s map, `wmFailConflict`'s map, `wrefNoteStatusForHTTP`) are now one shared
   `statusForHTTP` helper in `internal/api/errors.go`; the write-envelope writers assert
   their status/code pairing against it, behavior unchanged.
2. **Malformed-base64 `diff_chunk` test** — the missing 400-envelope regression test for
   diff-commit's base64 rejection (write_commit_test.go).
3. **Validator entry guards** — `assertf` nil-guards on `wcValidateMetadata` and
   `wmValidateMergeRequest` (internal invariants only; request data still returns errors).
4. **`POST /repos/branches/create` gains `force` + `preferred_base`** — `force`
   CAS-recreates an existing branch, with non-fast-forward moves still subject to the
   `no-force-push` ref-policy op via a real `merge-base --is-ancestor` check;
   `preferred_base` is the fallback base when none is given or the primary base does not
   resolve (write_refs_test.go covers the force/no-force-push interaction).

## Deferred gate findings (open; non-blocking at the p5 gate)

The p5 review gate closed GO with zero P0/P1 remaining. These P2/P3 findings are
tracked here for follow-up. Suggested bundling: one small spec-conformance PR
(items 1–4, each a sub-5-line change) and one robustness PR (items 5–7).

1. **P2 — `sourcehut` provider unmapped** (`internal/sync/sync.go`): the
   OpenAPI-allowed provider enum value is rejected with 400; add
   `"sourcehut": "git.sr.ht"` to the provider→host map.
2. **P2 — `base_repo` omitted from GET responses** (`internal/api/handlers.go`):
   `GET /api/v1/repo` and `GET /api/v1/repos` never include the `base_repo`
   object (docs/03, openapi.yaml).
3. **P2 — `PushPayload.CustomerID` type** (`internal/events/refstore.go`): emits
   the internal integer org ID; should be the org-name string matching the `iss`
   claim.
4. **P3 — git-credentials create status** (`internal/api/handlers_credentials.go`):
   `POST /api/v1/repos/git-credentials` returns 200; spec says 201.
5. **P2 — `handleCreateRepo` row cleanup** (`internal/api/handlers_sync.go`):
   a failure after `CreateRepo` (e.g. credential validation, first-sync enqueue)
   orphans the repo row, so retries hit 409; mirror `forkAbandon`'s
   detached-context soft-delete.
6. **P2 — missing `assertf` entry guards** (`handlers_fork.go`,
   `handlers_sync.go`, `wire_eventing.go`): handlers/callbacks lack the
   defensive nil-dependency asserts used elsewhere.
7. **P3 — `FinishSync` race guard** (`internal/meta/pg_eventing.go`,
   `sqlite_eventing.go`): missing `AND syncing = true` predicate allows stats
   pollution if a detached run finishes after re-attach.
8. **P3 — pull-upstream empty ref not defaulted** (`internal/api/handlers_sync.go`):
   `POST /repos/pull-upstream` passes an empty `req.Ref` straight into the sync
   job instead of defaulting to the repo's default branch.
