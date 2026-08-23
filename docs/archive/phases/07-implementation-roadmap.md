# Implementation Roadmap & Conformance Strategy

> **Archived.** Historical implementation notes, kept verbatim as the project's build
> history; decisions here may have been superseded by later work. For current
> behavior, start at the [docs index](../../README.md); the ADRs recorded below are
> individually addressable under [`docs/adr/`](../../adr/README.md).

## Architecture sketch

```
                    ┌──────────────────────────────────────────────┐
   api.{org}.host   │  API service (/api/v1, /api/repos/{name})    │
  ──────────────────▶  JWT verify → route → repo service           │
                    └──────────────┬───────────────────────────────┘
                                   │
   {org}.host       ┌──────────────▼───────────────────────────────┐
  ──────────────────▶  Git service: smart HTTP + LFS batch         │
                    │  (upload-pack / receive-pack, policy hooks)  │
                    └──────────────┬───────────────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────────────┐
                    │  Core: object DB + ref store per repo        │
                    │  (bare git repos or git library over blob    │
                    │   storage; ephemeral namespace per repo)     │
                    ├──────────────────────────────────────────────┤
                    │  Metadata DB: orgs, public keys, repos,      │
                    │  base_repo links, credentials, webhooks,     │
                    │  branch created_at, soft-delete state        │
                    ├──────────────────────────────────────────────┤
                    │  Async workers: webhook delivery, upstream   │
                    │  sync (pull/push mirror), import archival    │
                    └──────────────────────────────────────────────┘
```

Simplest viable core: **bare git repositories on disk** managed through a git library
(go-git/gitoxide/libgit2), one bare repo per repository with the ephemeral namespace stored
under a separate ref prefix (e.g. `refs/ephemeral/heads/*`) mapped in/out at both API and
git-protocol layers. Everything else (diff, blame, grep, archive, merge) can shell out to
`git` initially.

## Phases

Each phase ends with a dogfooding gate (below) before moving on.

**Phase 0 — de-risk spike: done.** `cmd/codestorage` + `internal/spike` (promoted to
`internal/githttp` in phase 2) prove the storage core end-to-end: git push → packs in S3 →
ref CAS in Postgres → clone from a second stateless node, with JWT ref-policy enforcement
and `+ephemeral`/`+import` namespace routing. This replaces the "bare git repositories on
disk" core sketched above with
**PG as ref authority + S3 as immutable pack storage + rebuildable disk caches**; decisions
and crash-ordering analysis are recorded in
[08-spike-implementation-notes.md](08-spike-implementation-notes.md).

**Phase 1 — skeleton + auth: done.** `internal/meta` (Postgres metadata store),
`internal/auth` (multi-algorithm JWT verification), and `internal/api` (repo CRUD,
middleware, `{"error"}` envelope) implement the phase-1 surface behind frozen contracts;
`codestorage serve` mounts `/api/v1` alongside the phase-0 git path in single-org
self-host mode (quickstart in the top-level README). Conformance is already SDK-driven:
the api tests run the vendor's unmodified TypeScript SDK against the in-process server,
plus a 401/403/404/409 matrix on real Postgres, wired through the production constructors
rather than test doubles. Decisions are recorded in
[09-phase1-implementation-notes.md](09-phase1-implementation-notes.md).

**Phase 2 — git smart HTTP: done.** The spike git path was promoted to `internal/githttp`
and completed as the production surface: git-path auth moved onto the production org-key
verifier (multi-algorithm, iss-validated; git-only serve mode dropped — `serve` requires
`CODESTORAGE_ORG` + `CODESTORAGE_ORG_PUBKEYS`), the ref-policy engine gained upstream glob
semantics, the deprecated `ops` fallback, the ephemeral policy quirk, and `verify-sig`
backed by an org signing-key registry (`codestorage signing-key`, keyring read fresh from
Postgres each push). `CODESTORAGE_BASE_URL` makes `http_url` routable behind 0.0.0.0
binds. Decisions are recorded in
[10-phase2-implementation-notes.md](10-phase2-implementation-notes.md). Phase 6 later
added the LFS batch API and rate limiting on this surface (see 15).

**Interlude — pluggable storage backends (local mode): done.** Before phase 3, the storage
layer went behind URL-scheme factories (`ref.Open` / `blob.Open` / `meta.Open`) with two
stacks: the production `postgres://` + `s3://` pair, and a zero-dependency local pair —
`sqlite://` (pure-Go driver, one `codestorage.db` for refs + metadata) and `file://` pack
storage — derived from a single `serve --data-dir` flag. Nodes stay stateless either way;
the container-based acceptance suites gained zero-container lanes (api `/sqlite` subtests,
`CODESTORAGE_TEST_BACKEND=local` e2e). Decisions are recorded in
[11-local-backends-implementation-notes.md](11-local-backends-implementation-notes.md).

1. **Skeleton + auth — done.** Org registry with public keys; JWT middleware (ES256/RS256/EdDSA,
   claims/scope checks); error envelope; `POST /repos`, `GET /repo`, `GET /repos`,
   `DELETE /repos/delete` on the phase-0 core + metadata DB.
2. **Git smart HTTP — done.** upload-pack/receive-pack with JWT basic auth, ref-policy
   enforcement hooks, ephemeral/import namespace routing. *Gate: real `git clone/push`
   round-trip.*
3. **Read API — done.** files, files/metadata (incl. unborn-branch 404), file stream with
   conditional/range headers, branches, tags, commits, commit, diff, branches/diff, blame,
   archive, grep (synchronous `git grep` is fine). Cursor pagination everywhere. *Gate:
   endpoint contract tests on both backend lanes, vendor Go-SDK read drive, schemathesis
   conformance against `docs/upstream/openapi.yaml`, and dual-backend live dogfood
   transcripts.* Decisions are recorded in
   [12-phase3-implementation-notes.md](12-phase3-implementation-notes.md).
4. **Write API — done.** commit-pack +
   diff-commit NDJSON streaming (incremental parse, 4 MiB chunks), restore-commit, branch
   create/delete (+default-branch protection), merge + preview (`git merge-tree
   --write-tree` in a per-request scratch odb), tags, notes (+CAS `expected_ref_sha`). All
   CAS + ref-policy checks; `result` envelope on the documented status list. Includes the
   repo-key-split fix (repos keyed by `repo_id` on both surfaces) and the p3 read-API
   burn-down. All 11 write routes are wired into `api.New`; the full test suite passes on
   both backend lanes. *Gate: closed GO — review burn-down (incl. the notes read
   endpoints and write-path fixes from 13) landed, with live dogfood transcripts on both
   backend lanes.* Decisions are recorded in
   [13-phase4-implementation-notes.md](13-phase4-implementation-notes.md).
5. **Eventing & sync — done.** `push` webhooks with HMAC signatures, emitted by a CASRef
   decorator on the ref authority; upstream `base_repo` sync workers (mirror fetch +
   loopback push through our own git endpoint, pull-upstream 202, `repo.sync.*` webhooks,
   git-credentials CRUD); webhook CRUD; synchronous forking (`provider: "code"`).
   Background work goes through a queue seam: durable River jobs on Postgres, in-process
   in local mode (local git pushes deliver synchronously and don't schedule syncs — ADR-33
   in 14). *Gate: all test lanes green on both backends (incl. River against real
   Postgres) plus a live local-mode dogfood — real `git push` → HMAC-verified `push`
   webhook, fork through `POST /repos`, upstream mirror round-trip, pull-upstream 202
   with signed `repo.sync.started/succeeded` events.* Decisions are recorded in
   [14-phase5-implementation-notes.md](14-phase5-implementation-notes.md).
6. **LFS + polish — done.** LFS batch API on the smart-HTTP
   host (presigned S3/MinIO transfers; server-proxied in local mode); named-repo path
   style (`/api/repos/{repo_name}/...`) as re-dispatching routing aliases; per-client-IP
   token-bucket rate limiting (pre-auth 429s, env-configured, off by default);
   `GET /api/repo-urls/{repo_id}`; plus the p5 deferred-findings burn-down (7 of 8
   closed, see 14). *Gate: closed GO on the re-run — the first run's P0 (batch endpoint
   unreachable behind the old `/api/v1/` mount) fixed and covered by unit + e2e tests
   incl. a real `git-lfs` round-trip on both backend lanes, plus live LFS + alias-matrix
   dogfood against the compose Postgres+MinIO stack. The multi-node deployment packaging
   (Dockerfile, compose replicas) carried forward to phase 7.* Decisions are recorded in
   [15-phase6-implementation-notes.md](15-phase6-implementation-notes.md).
7. **Deployment packaging + p6 burn-down — done.**
   Multi-stage `Dockerfile` (static build; alpine runtime carrying the required external
   `git`) and an opt-in two-replica compose `replicas` profile (symmetric
   `node-a`/`node-b` on one Postgres+MinIO stack) with README quickstarts; river
   migrations serialized under a pg advisory lock (concurrent fresh boot raced on
   river's DDL); all five deferred p6 findings closed, plus the alias git-credentials
   POST fix (burn-down record in 15). *Gate: closed GO — from-scratch `--profile
   replicas up --build` booted symmetric replicas with zero restarts and zero
   duplicate-key DDL errors; push via node A == clone via node B; one API read per node
   (v1 + named alias); path-id git-credentials round-trip across both replicas.*
   Decisions are recorded in
   [16-phase7-implementation-notes.md](16-phase7-implementation-notes.md).

## Conformance testing (the big advantage)

The vendor's SDKs are the conformance suite:

1. **Unit-level:** all three SDKs allow full base-URL override
   (`apiBaseUrl`/`storageBaseUrl`, no env vars needed). Point them at the OSS backend.
2. **End-to-end:** `sdk/packages/code-storage-typescript/tests/full-workflow.js` is a live
   integration test the vendor runs against their own backend — repo create → commit-pack →
   branches/commits polling → grep → file reads → diff-commit patch → feature branch →
   restore-commit. Make it pass unmodified.
3. **Consumer-level:** `just-code-storage`'s git command (`init/clone/add/commit/push/
   log/show/diff/grep/blame/branch/tag/merge/status`) exercises the semantic invariants
   (zero-SHA root commits, 404 unborn branches, conflict → "failed to push some refs").
   Run its examples/tests against the backend.
4. **Contract-level:** validate responses against `upstream/openapi.yaml` schemas
   (e.g. schemathesis/dredd for the read endpoints).
5. **Git-level:** plain `git` + `git-lfs` CLIs against the storage host (clone, push,
   force-push denial under `no-force-push` JWT, notes fetch, LFS round-trip).

## Suggested build order rationale

- Phase 2 before 3: once real git works, most read endpoints are thin wrappers over
  plumbing (`ls-tree`, `log`, `diff`, `blame`, `grep`, `archive`) — high surface, low risk.
- The NDJSON write path (phase 4) is the only novel protocol work; isolate it behind a
  "commit builder" service with exhaustive CAS/conflict tests.
- Defer hot/cold storage tiering, search indexing, and horizontal scaling — they are
  transparent to the API contract.

## Open items to decide early

- Repo ID/url scheme: vendor distinguishes internal `repo_id` from `url` (JWT claim). An
  OSS backend can use one identifier for both as long as `GET /api/repo-urls/{repo_id}`
  and `listRepos` stay coherent. *Settled in phase 1:* kept distinct — `repo_id` is
  server-generated, `url` comes from the JWT `repo` claim.
- Ephemeral ref storage: separate bare repo vs ref-prefix namespace (ref-prefix recommended:
  shared objects make promotion O(1)). The spike used fully separate ref stores keyed by
  the namespaced name (see ADR-3 in 08); the promotion-cost question is still open.
- Cursor encoding: opaque to clients; base64(JSON{offset|last-key}) is fine. *Settled in
  phase 1:* unpadded base64url keyset cursor (see 09).
- Webhook endpoint management API (not publicly specified — design freely). *Settled in
  phase 5:* self-defined org-scoped CRUD, secret shown once at registration (ADR-29 in 14).
