# Implementation Roadmap & Conformance Strategy

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
[10-phase2-implementation-notes.md](10-phase2-implementation-notes.md). Phases 3–6 below
are not started.

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
3. **Read API.** files, files/metadata (incl. unborn-branch 404), file stream with
   conditional/range headers, branches, tags, commits, commit, diff, branches/diff, blame,
   archive, grep (synchronous `git grep` is fine). Cursor pagination everywhere.
4. **Write API.** commit-pack + diff-commit NDJSON streaming (incremental parse, 4 MiB
   chunks), restore-commit, branch create/delete (+default-branch protection), merge +
   preview (scratch worktree or in-memory merge), tags, notes (+CAS `expected_ref_sha`).
   All CAS + ref-policy checks; `result` envelope on the documented status list.
5. **Eventing & sync.** `push` webhooks with HMAC signatures; upstream `base_repo` sync
   workers (clone/mirror from github and other providers, pull-upstream 202, sync webhooks,
   git-credentials CRUD); forking (`provider: "code"`).
6. **LFS + polish.** LFS batch API; named-repo path style (`/api/repos/{repo_name}/...`)
   as routing aliases; rate limiting; `GET /api/repo-urls/{repo_id}`.

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
- Webhook endpoint management API (not publicly specified — design freely).
