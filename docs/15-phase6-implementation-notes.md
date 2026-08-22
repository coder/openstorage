# Phase-6 Implementation Notes & ADRs (LFS, named-repo aliases, repo-urls, rate limiting)

Phase 6 delivers the Git LFS Batch API on the smart-HTTP host
(`internal/githttp/lfs.go`), the named-repo path style `/api/repos/{repo_name}/...` as
routing aliases (`internal/api/alias.go`), `GET /api/repo-urls/{repo_id}`
(`internal/api/repourls.go`), and per-client-IP rate limiting
(`internal/api/ratelimit.go`) — plus the phase-5 deferred-findings burn-down recorded at
the end. Contract sources: docs/05 (LFS, namespaces), docs/README "The two API path
styles", docs/03 (repo-urls, 429 conventions, status mapping), `upstream/openapi.yaml`.

The spike-era decisions — PG as ref authority (ADR-1), blob upload strictly before the
ref flip (ADR-2), namespace mapping via separate ref stores (ADR-3) — still hold and
shape this phase: LFS objects are one more class of immutable content-addressed blobs
published to the same `BlobStore` before anything references them (ADR-34), and the
namespace question ADR-3 answered for *refs* gets the opposite answer for LFS *objects*
(ADR-35), for the same underlying reason.

## ADR-34: LFS lives on the storage host — presigned transfers, proxy fallback

> **Amended by [#51](https://github.com/coder/code-storage/issues/51):** presigned
> transfers are now download-only. Uploads always use the verified proxy endpoint — a
> presigned PUT binds neither content length nor digest, which left LFS outside the
> storage-quota perimeter — and every verified upload is admitted through the quota
> ledger and registered for the orphan sweep. See the
> [wire-compatibility notes](explanation/wire-compatibility.md#git-lfs-transfer-semantics).

docs/05 puts LFS on the git remote itself: git-lfs discovers
`POST /{repo}.git/info/lfs/objects/batch` relative to the clone URL. Decision: **the
smart-HTTP server implements the Batch API directly — no separate LFS server, no extra
client config** — and transfer actions are **presigned blob URLs when the backend can
sign them, else proxy endpoints back through this server**.

- **Auth.** The batch body is parsed (size-bounded, gzip-aware) *before* authentication
  because the `operation` field selects the required scope: `download` → `git:read`,
  `upload` → `git:write`, with the docs/02 401/403 failure mapping and the
  `LFS-Authenticate: Basic` discovery challenge on 401. Credentials arrive as Basic with
  username `t` (the git remote-URL form) or as a Bearer token; the shared claim check was
  split out of `authenticate()` into `verifyClaims()` so both paths use it.
- **Presigned path (S3/MinIO).** `blob.SignedURL(ctx, key, method, 15m)` presigns GET/PUT
  via gocloud. Content-Type is deliberately left unsigned: aws-sdk-v2 cannot sign it for
  presigned PUTs, and an unsigned Content-Type lets git-lfs's
  `application/octet-stream` PUT verify against MinIO SigV4. The server never touches
  object bytes — transfers go client↔S3.
- **Proxy path (local mode).** `fileblob` cannot presign and returns the
  `blob.ErrSignedURLUnsupported` sentinel; the action then points at
  `GET/PUT /{repo}.git/info/lfs/objects/{oid}` on this server, echoing the caller's
  `Authorization` header in the action headers so git-lfs re-presents the same
  credentials. Proxy uploads recompute the sha256 while streaming to a scratch file and
  **refuse to publish on mismatch** — the oid IS the content address, and publishing
  wrong bytes under it would corrupt every future download of that oid. This is ADR-2's
  ordering applied to LFS: verified content lands in the blob store atomically
  (tmp+rename), or not at all.
- **Batch semantics.** Only the `basic` transfer adapter (422 otherwise) and `sha256`
  (409 per the spec's hash_algo negotiation). Existence is a `blob.Stat` probe: download
  of a missing oid is a per-object 404 error entry; upload of a present oid returns no
  actions, which tells the client to skip the transfer. Download sizes are the
  authoritative stored sizes. Unknown `info/lfs/*` subpaths (e.g. the optional locking
  API's `locks/verify` probe) answer 404, which git-lfs treats as "not supported".

## ADR-35: LFS objects are keyed by base RepoID — namespaces share one object space

`blob.LFSKey(repoID, oid) = lfs/<repoID>/<oid>`. Decision: **the `+ephemeral` /
`+import` namespaces map onto the base repo's LFS object space** — a batch request on
`/{repo_id}+ephemeral.git` resolves keys under the base RepoID.

This is the deliberate inverse of ADR-3 (separate ref stores per namespace), and the
reason is the same distinction that drove ADR-3: refs are mutable *positions* that must
not leak across namespaces, while LFS objects are immutable *content* — a pointer file
in any namespace addresses exactly the same bytes. Sharing the space means an ephemeral
branch can carry LFS objects and promotion needs no object copying, exactly like pack
objects. The key stays **repo-scoped** rather than global, though: RepoID remains the
only storage key (ADR-21), so two repos' GC lifetimes never couple, and a batch `Stat`
probe on one repo cannot be used as a cross-repo existence oracle for content known only
by hash.

## ADR-36: Named-repo aliases re-dispatch to the v1 handlers — one implementation, two path styles

The openapi catalog defines the named-repo style (`/api/repos/{repo_name}/...`) in
parallel to the v1 style the SDKs use. Decision: **aliases are a thin routing layer over
the unchanged v1 handlers — the alias handler rewrites the request to the v1 path and
re-dispatches it through the mux**, giving identical bodies by construction. Duplicating
32 repo-scoped routes' handler logic would be 32 opportunities for the two styles to
drift; a re-dispatch cannot drift.

- Each alias registers with its v1 twin's exact auth flavor, org-level flag, and scopes;
  org-scoped routes (repo list/create, webhooks) get no alias — there is no single repo
  to name in the path.
- The handler URL-decodes `{repo_name}` (Go 1.22's mux matches the escaped path, so
  `pierre%2Fexample` stays one segment), resolves both the path name and the token's
  `repo` claim through `meta.ResolveRepoKey`, and answers 404 on unknown names or RepoID
  mismatch — shaped as the route's envelope flavor (`{"error"}` on reads, docs/04 result
  envelope on writes).
- Accepted costs: the re-dispatched request runs the v1 middleware chain again (double
  JWT verification), and rate limiting must not double-charge — the rewritten request
  carries a `rateLimitExempt` context marker so an aliased request costs exactly one
  token (ADR-37). Double verification is cheap; a halved effective alias rate would be a
  behavioral difference between the two styles.

## ADR-37: Rate limiting is a pre-auth, per-node, per-client-IP token bucket

docs/03 describes gateway-level 429s. Decision: **admission control is a hand-rolled
in-memory token bucket per client IP, applied at every REST route registration site
(v1 + aliases + repo-urls) *before* the auth middleware**, disabled unless configured.

- **Hand-rolled** with x/time/rate semantics (float tokens, monotonic refill, burst cap)
  rather than importing the module: the bucket logic is a few dozen lines, go.mod stays
  untouched, and the injectable clock makes refill deterministic in tests. Buckets live
  in a lazy map under a mutex; a periodic sweep drops stale (fully refilled) entries —
  behavior-neutral, since a dropped bucket is recreated full.
- **Key = `RemoteAddr` host.** `X-Forwarded-For` is deliberately not trusted — a
  spoofable header as the limit key would let any client mint fresh buckets at will.
  Deployments behind a trusted proxy terminate client identity there. *(Issue #30
  later added an opt-in `CODESTORAGE_TRUSTED_PROXIES` CIDR allowlist: when the peer
  is a listed proxy, the key becomes the rightmost untrusted `X-Forwarded-For` hop;
  the default stays `RemoteAddr`-only.)*
- **Per node, in memory.** Nodes are stateless (no shared limiter state to operate), and
  per-node limiting is the documented "basic" scope. Pre-auth 429s mean unauthenticated
  clients consume budget keyed by IP — accepted for this scope.
- **429 shape.** `Retry-After` (whole seconds, rounded up) plus the flavor-matched body:
  read routes answer the `{"error"}` envelope, write routes the docs/04 result envelope
  with status `"unavailable"` — `statusForHTTP` in errors.go gained the 429 row, so the
  mapping stays centralized.
- **Config is fail-fast.** `CODESTORAGE_RATE_RPS` / `CODESTORAGE_RATE_BURST` must be set
  as a pair (`RPS > 0` requires `BURST >= 1`); malformed, negative, NaN, or infinite
  values abort startup naming the variable. Unset (or explicit zero) disables limiting —
  the middleware becomes the identity.

## Integrator wiring

- **The API handler owns the whole `/api` subtree.** serve now mounts it at `/api/`
  instead of `/api/v1/` — the phase-6 routes (`/api/repos/{repo_name}/...`,
  `/api/repo-urls/{repo_id}`) were unreachable behind the old mount, falling through to
  the git smart-HTTP fallback. (This was the p6 gate's P0: the routes existed in
  `api.New` but the process never routed to them.)
- **Foundation contract** (frozen for the parallel lanes): `blob.BlobStore` gained
  `SignedURL` + `Stat` with the `ErrSignedURLUnsupported` / `ErrNotFound` sentinels and
  the `LFSKey` helper; `api.Config` gained `RateLimitConfig`, and every route
  registration site threads `rl.limit(routeRead|routeWrite)` ahead of auth (nil limiter
  = identity).
- **Env plumbing:** `resolveRateLimit` (config.go) parses the pair into
  `api.RateLimitConfig` via `serveConfig` → `buildAPIHandler`.
- **repo-urls:** `GET /api/repo-urls/{repo_id}` (note: no `/v1` prefix) is org-level
  `org:read`, answering exactly `{"repo_id", "url"}` via `GetRepoByID`; unknown,
  cross-org, and soft-deleted ids are 404.

## Workstream-F: phase-5 deferred-findings burn-down (landed with this phase)

Seven of the eight P2/P3 findings tracked in docs/14 are closed: `FinishSync` in-flight
claim predicate (pg + sqlite), `PushPayload.CustomerID` as the org-name string,
`base_repo` included in repo GET responses, the `sourcehut` provider mapping,
git-credentials create → 201, `createAbandon` cleanup of post-create failures in the
sync-flavored creator, and pull-upstream defaulting an omitted ref to the default
branch. Item 6 (assertf entry guards on the p5 handlers/callbacks) remained open
at the p6 gate and was closed in phase 7 (see Open items below).

## Open items

*Status: the p6 gate re-run closed **GO** at `1fdb11a` (LFS round-trip + alias matrix
2/2 against the compose Postgres+MinIO stack; all P0 findings resolved and
recheck-confirmed). All items below — the deployment-packaging deliverable and the five
deferred findings — were closed by the phase-7 burn-down; each entry carries its fix
location inline. This section is the phase-7 burn-down record.*

### Deliverable gap

- **Deployment packaging did not land.** The planned `Dockerfile` and a compose
  `replicas` profile (two stateless nodes on one Postgres+MinIO stack, proving
  multi-node statelessness in the quickstart) are not in the tree. Acceptance at the
  gate did not cover this deliverable, so it carries forward.
  *Closed in phase 7: multi-stage `Dockerfile` (alpine runtime carrying the required
  external `git`) + `.dockerignore` at the repo root; two-replica `replicas` profile
  (`node-a`/`node-b`, differing only in port mapping) in `docker-compose.yml`;
  "Two-replica compose profile" subsection in the root `README.md` quickstart. The
  concurrent fresh boot surfaced a latent race — `rivermigrate` runs its DDL without
  any locking, so two replicas migrating from scratch collide (SQLSTATE 23505) —
  fixed by serializing river migrations under a pg advisory lock in
  `internal/jobs/river.go`, mirroring the meta/ref `InitSchema` locks.*

### Deferred p6 gate findings (P2/P3, non-blocking)

1. **P2 — alias git-credentials path-id routes 404** (`internal/api/alias.go`):
   `PUT/DELETE /api/repos/{repo_name}/git-credentials/{git_credential_id}` are
   documented in openapi.yaml but unrouted (the alias only mirrors the v1 body-id
   form). Top-priority deferral: a documented vendor API surface currently 404s.
   *Closed in phase 7: path-id PUT/DELETE handlers in `handlers_credentials.go`
   (sharing the v1 store logic; `200 {id, created_at}` / `204`, v1-identical shapes),
   registered as direct alias routes in `alias.go`; round-trip + negative cases in
   `credentials_test.go` on both backends.*
2. **P2 — `createAbandon` leaves credential/upstream rows** (`internal/api/handlers_sync.go`):
   post-`CreateRepo` failure cleanup soft-deletes the repo row but not git-credential
   or upstream rows created alongside it.
   *Closed in phase 7: `createAbandon` now takes the RepoID and best-effort deletes the
   upstream and credential rows alongside the repo soft-delete (failures logged,
   not-found ignored); covered by a package-internal test on both backends.*
3. **P2 — `clientKey` empty-`RemoteAddr` fallback** (`internal/api/ratelimit.go`):
   an empty `RemoteAddr` still trips `admit`'s assert; fall back to a sentinel key
   (e.g. `"unknown"`) instead.
   *Closed in phase 7: `clientKey` returns the `"unknown"` sentinel for an empty
   `RemoteAddr`; case added to `TestRateLimitClientKey`.*
4. **P3 — `newRateLimiter` startup validation** (`internal/api/ratelimit.go`):
   invalid config panics via assertf rather than a clean startup error
   (accepted-by-design; optional polish).
   *Closed in phase 7: `newRateLimiter` returns `(*rateLimiter, error)` with the config
   checks as returned errors (`RPS <= 0` still disables the limiter); propagated as
   `api.New(cfg) (http.Handler, error)` through `buildAPIHandler`
   (`cmd/codestorage/wire.go`); invalid-config test asserts an error, not a panic.*
5. **P2 — assertf entry guards on p5 handlers/callbacks** (`handlers_fork.go`,
   `handlers_sync.go`, `wire_eventing.go`): carried over from docs/14 item 6.
   *Closed in phase 7: entry guards (nil receiver/deps/args only — never user input)
   added to `forkAwareCreateRepo`/`handleForkRepo`/`forkAbandon` (`handlers_fork.go`),
   `handleCreateRepo`/`createAbandon`/`policyDeniesPullUpstream`/`handlePullUpstream`/
   `handleDetachBase` (`handlers_sync.go`), and `webhookSink.Emit` (`wire_eventing.go`).*
