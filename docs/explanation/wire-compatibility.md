# Wire compatibility with code.storage

**TL;DR:** open.storage can stand in for [code.storage](https://code.storage) (Pierre
Computer Company) behind existing clients. The differences are one organization per
deployment, no multi-tenant control plane, and a short list of
[tracked deviations](#known-deviations-from-the-vendored-spec). This page is for platform
teams and integrators deciding without reading the source.

## The contract

The official code.storage SDKs (TypeScript, Python, Go) accept `apiBaseUrl` /
`storageBaseUrl` overrides. Point them at an open.storage deployment and client code runs
unchanged, with the same:

- endpoints and request/response shapes;
- JWT claims and scopes;
- NDJSON streaming protocols;
- webhook signatures;
- git smart-HTTP and LFS behavior.

Plain `git` and `curl` work identically. The backend was built from a
[specification](../README.md) extracted independently from all three SDKs, a real
consumer, and the vendor's own OpenAPI spec and docs. The SDKs implement an identical wire
surface, which makes "wire-compatible" testable.

The target is the surface **clients actually use**: everything the SDKs call, plus the
documented HTTP API. It is not a bug-for-bug clone of upstream internals.

## Implemented surface

Full endpoint catalogs: [HTTP API](../reference/http-api.md) (REST),
[streaming protocols](streaming-commit-protocols.md) (streaming writes),
[git protocol](git-protocol-and-namespaces.md) (git protocol, LFS, namespaces), and
[webhooks](../reference/webhooks.md). By feature group:

| Surface | Status | Notes |
|---|---|---|
| Repository lifecycle | ✅ | Create (incl. fork via `base_repo`), get, `PATCH` (default branch), list, delete, `GET /api/repo-urls/{repo_id}` |
| Read surface | ✅ | Files (conditional/range streaming), file listings + metadata, branches, tags, commits, diff, branches-diff, blame, grep, archive, notes — cursor pagination throughout |
| Single-item lookups | ✅ | `GET .../branch`, `GET .../tag` — served although the vendored spec omits them (SDK-relied surface) |
| Write surface | ✅ | NDJSON `commit-pack` and `diff-commit` streaming, `restore-commit`, branch/tag/notes writes — compare-and-swap (`expected_head_sha` etc.) and ref policies everywhere |
| Merge | ✅ | `POST .../merge` + read-only `GET .../merge/preview` |
| Both path styles | ✅ | `/api/v1/...` (repo from the JWT `repo` claim) and named-repo `/api/repos/{repo_name}/...` aliases over the same handlers |
| Deprecated spellings | ✅ | Documented upstream aliases accepted ([below](#deprecated-aliases)) |
| Git smart-HTTP | ✅ | Clone/fetch/push at `/{repo}.git` (repo ID or URL-encoded name), ref policies, signed-commit verification, atomic pushes ([below](#git-protocol-semantics)) |
| Namespaces & forks | ✅ | `+ephemeral` / `+import` namespaces. Synchronous forks copy the full fork-point closure into the new repository (independent storage and [quota](../how-to/storage-lifecycle-and-sweeping.md#pack-quotas), nothing shared with the source). Git-pushed and REST-written ephemeral state live in separate ref spaces ([deviation](#known-deviations-from-the-vendored-spec)) |
| Git LFS | ✅ | Batch API on the same remote; presigned S3 downloads with a server-proxied fallback, verified server-proxied uploads ([below](#git-lfs-transfer-semantics)) |
| Webhooks | ✅ | HMAC-signed `push` and `repo.sync.*` events, plus webhook CRUD |
| Upstream sync & credentials | ✅ | `pull-upstream`, `push-upstream` (outbound forwarding: explicit trigger plus opt-in `base_repo.forward_pushes`; trigger semantics differ from upstream's implicit forwarding, see the [deviation](#known-deviations-from-the-vendored-spec)), detach base, git-credential CRUD. Non-Postgres gap (SQLite local mode and experimental s3wal mode): a *git-surface* push does not auto-schedule sync or forwarding, because the hook subprocess cannot reach the serve process's in-process queue. API writes and explicit `pull-upstream`/`push-upstream` triggers are unaffected |
| Throttled responses | ✅ | Every HTTP-level `429` (gateway [rate limiter](../reference/configuration.md#rate-limiting) and busy/backpressure paths) carries both upstream-documented headers: `Retry-After` (delay-seconds) and the IETF `RateLimit` service-limit field (`"default";r=0;t=<retry-after-secs>`); `Retry-After` takes precedence |
| Auth model | ✅ | Customer-signed JWTs (`ES256`/`RS256`/`EdDSA`/`PS256`/`ES384`/`ES512`), scopes `git:read` / `git:write` / `repo:write` / `org:read` (exact matching — no scope implies another), `refs` policy tuples ([auth spec](../reference/authentication.md)) |

### Server-allocated branch contract

Both branch-create routes let you omit `target_branch`; the server then allocates a new
opaque name in the selected persistent or ephemeral namespace.

- `force: true` requires a named target.
- Each successful allocation is independent: retrying after an unknown result can create
  another branch. Persist the returned name; do not treat the request as idempotent.
- Existing namespace behavior, including the REST/Git ephemeral split below, is
  unchanged. See [HTTP API](../reference/http-api.md#branches).

This is an additive adoption of the [public OpenAPI](https://code.storage/docs/openapi.yaml)
and [SDK create-branch documentation](https://code.storage/docs/reference/sdk/create-branch.md).
At the 2026-09-11 refresh, the public spec SHA-256 was
`1ef19148db362d1cd201cb25eeba4462cad1e420aff8c7c2323ebd3036ad1cec`, while
[the cluster OpenAPI](https://api.pierre.code.storage/api/openapi.yaml) still matched the
previous snapshot (`e7638a43bbdc9ae0639c1d5ec65a81fea18e30c3f2ae22815d141042ab07574f`).
The specs had not converged, and these read-only audits do not show that allocation is
deployed on that cluster. No upstream identifier layout or equivalence for explicit
empty/null values is claimed; open.storage continues to reject those values.

### Deprecated aliases

Upstream renamed several parameters and body fields; SDKs in the wild still send the old
spellings. The backend accepts exactly the documented deprecated aliases, no invented
ones. On conflict:

- The **current spelling wins**, and the losing duplicate is ignored rather than
  independently validated.
- Exception: boolean *query* aliases shape-validate both spellings, so a request that
  `400`d on a malformed bool before the alias existed keeps `400`ing.
- Exception: the JWT `refs`/`ops` pair **merges** rather than conflicts (last row).

| Endpoint | Current | Deprecated alias |
|---|---|---|
| `GET /repos/commits` | `ref` | `branch` |
| `GET /repos/commit`, `GET /repos/diff` | `ref` | `sha` |
| `GET /repos/diff` | `base_ref` | `baseSha` |
| `GET /repos/diff` | `git_apply_compatible` | `gitApplyCompatible` |
| `GET`/`POST`/`DELETE /repos/notes` | `object_ref`, `notes_ref` | `sha`, `ref` |
| `POST`/`DELETE /repos/notes` | `expected_notes_ref_sha` | `expected_ref_sha` |
| `GET /repos/merge/preview` | `source_is_ephemeral`, `target_is_ephemeral` | `source_ephemeral`, `target_ephemeral` |
| `POST /repos/branches/create` | `base_ref` | `base_branch` |
| `DELETE /repos/branches` | `target_branch` | `name` |
| `POST /repos/tags` | `ref` | `target` |
| `POST /repos/merge` | `source_ref` | `source_branch` |
| `POST /repos/grep` | `ref` | `rev` |
| JWT claim | `refs` policy tuples | `ops` (deprecated repo-wide op list; merged into the `"*"` pattern's policy alongside any `refs` tuples) |

Where upstream does, responses mirror legacy names alongside current ones, so older SDK
response parsing keeps working. Examples: `url` mirrors `repo_name`; delete-repo
responses carry `repo_name` plus the deprecated `repo_id`; deleted-branch responses carry
both `target_branch` and `name`.

### Git protocol semantics

Beyond the smart-HTTP wire format, the semantics clients depend on match git:

- **Ref policies** from the JWT `refs` claim (`no-push`, `no-force-push`, `verify-sig`)
  are enforced in server-side hooks; unknown operations fail closed.
- **Atomic pushes** (`git push --atomic`): the server detects the `atomic` capability and
  evaluates the full policy set across every pushed ref *before* anything is stored. One
  policy-denied ref rejects the whole push, as in git. Without the flag, refs succeed and
  fail independently, as in git.
  - **Documented deviation:** refs advance through per-ref compare-and-swap on the ref
    authority. A non-policy failure midway through the batch (for example, a concurrent
    write landing on a later ref) can leave earlier refs committed even though git
    reports the push as failed. Policy enforcement is all-or-nothing; the
    concurrent-conflict window is not.
- **Signed pushes**: `verify-sig` validates SSH/PGP commit signatures against keys
  registered via [`openstorage signing-key`](../reference/cli.md#openstorage-signing-key).
  Verification shells out through git (`ssh-keygen` for SSH, `gpg` for PGP) and fails
  closed per ref when the binary is absent. The repository's supplied container image
  ships both; if you build a custom image and protect refs with `verify-sig`, install
  them yourself.

### Git LFS transfer semantics

The batch API speaks the standard LFS `basic` adapter, so any git-lfs client works
unchanged. The transfer hrefs are asymmetric by design:

- **Downloads** get presigned S3 URLs when the backend can sign, and a server-proxied
  fallback otherwise (e.g. local file mode). Reads are never quota-gated.
- **Uploads** always get a server-proxied href, even on S3. A presigned PUT binds neither
  content length nor checksum, so a writer could bypass the repository
  [storage quota](../how-to/storage-lifecycle-and-sweeping.md#pack-quotas) and publish
  bytes that don't match their claimed oid. The server instead:
  - verifies the sha256 content address and the actual `Content-Length` before
    publishing;
  - denies over-quota uploads with per-object `507` errors at the batch, and again at the
    transfer (closing declared-size lies);
  - registers every verified object for the
    [orphan sweep](../how-to/storage-lifecycle-and-sweeping.md#the-orphan-sweeper).

Upstream may presign uploads. The wire shape is identical either way (the `href` is
opaque to clients), so this is a hardening choice, not a compatibility deviation.

## The architectural difference: single-org

Upstream code.storage is multi-tenant; open.storage deliberately is not. One deployment
serves **one organization**, configured at boot
([`OPENSTORAGE_ORG`](../reference/configuration.md#openstorage_org) +
[`OPENSTORAGE_ORG_PUBKEYS`](../reference/configuration.md#openstorage_org_pubkeys)).

- **Clients see no difference:** tokens carry the same `iss`/`repo`/`scopes` claims.
- **Other orgs are rejected:** both surfaces reject any verified token whose `iss` is not
  the served org with `403`. Org rows retained from an earlier `OPENSTORAGE_ORG` value
  stay in the database, and each boot logs a warning naming them, but their tokens are
  rejected on both surfaces.
- **Operations change:** org onboarding and signing-key registration are deployment
  configuration (env + CLI), not a vendor control plane. Serving multiple orgs means
  multiple deployments.

## Intentionally out of scope

The multi-tenant **control plane** around the storage API is not part of the
compatibility target and is not implemented:

- Organization onboarding/management APIs, dynamic org routing.
- User and identity management: accounts, sessions, SSO/SCIM, dashboards.
- Billing, plans, usage metering APIs.
- The code.storage web UI and any UI-only endpoints.

If your integration only mints org-signed JWTs and calls the documented storage API, as
the SDKs do, none of these are observable.

## Known deviations from the vendored spec

Where open.storage intentionally differs from the upstream OpenAPI snapshot (recorded in
the [archived phase notes](../archive/phases/)):

| Upstream spec | Here | Rationale |
|---|---|---|
| `POST .../reset-commits` (NDJSON) | Not implemented; `POST .../restore-commit` (plain JSON) covers the SDK-visible restore semantics | Recorded deviation ([phase-3 notes](../archive/phases/12-phase3-implementation-notes.md)); no SDK calls `reset-commits` |
| `DELETE /api/repos/{repo_name}/tags/{tag_name}` (tag in path) | `DELETE .../tags` with the tag in the request body | Matches the SDKs' actual call shape |
| `GET`/`POST /api/repos` (bare collection routes) | `/api/v1/repos` only; named-repo aliases exist for repo-scoped routes, not org-scoped collections | The alias layer covers the styles SDKs use |
| RFC 9457 problem-details error bodies (spec) | No problem-details anywhere. Failures use one of two shapes, by route and failure class: the plain `{"error":"<msg>"}` envelope or the [streaming protocols](streaming-commit-protocols.md) `{"result":{"success":false,…}}` envelope. Do not assume one shape per route family: branch on the status code, or check both shapes. Per-route contracts are in [HTTP API](../reference/http-api.md) and [streaming protocols](streaming-commit-protocols.md) | Recorded deviations ([phase-3 notes](../archive/phases/12-phase3-implementation-notes.md), [ADR-26](../adr/026-result-envelope-statuses-499-is-not-emittable.md)): all three SDKs branch on status codes and none parses problem-details fields |
| `201` on write endpoints (spec) | `commit-pack` / `diff-commit` / `restore-commit` and `POST`/`DELETE .../notes` return `200` | Recorded deviation ([phase-3 notes](../archive/phases/12-phase3-implementation-notes.md)): the SDKs accept both; schema-validating clients should expect `200` |
| Credential-create response includes a summary (`created_at`, …) | `POST .../git-credentials` returns only `{"id": …}` | Recorded deviation ([phase-7 notes](../archive/phases/16-phase7-implementation-notes.md)); fetch the summary via the credential list if needed |
| One ephemeral namespace across git and REST | Two separate ref spaces: git push to `/{repo}+ephemeral.git` writes a distinct `+ephemeral` storage key; REST `ephemeral=true` reads/writes `refs/namespaces/ephemeral/…` under the base repository. Ephemeral state does not cross surfaces | Recorded cross-surface split ([phase-4 notes](../archive/phases/13-phase4-implementation-notes.md)): REST writes follow the REST read surface, so API-created ephemeral refs stay visible to `GET .../branches?ephemeral=true` and merge promotion |
| Implicit push forwarding: in upstream's GitHub App mode every `git push` to the default remote is proxied straight to GitHub, always on, with no REST trigger ([integrations guide](../upstream/guides/integrations.md)) | Explicit `POST /repos/push-upstream` (non-force by default, `force: true` opt-in) plus an opt-in per-repo `base_repo.forward_pushes` flag (default **off**) that schedules a forward after every won local push and mirrors back on success. A rejected non-fast-forward fails the run with per-ref reasons instead of rewriting upstream history. Ref deletions are not propagated in either direction (the inbound mirror deliberately avoids `--mirror` for the same reason). With `forward_pushes` off, a local push still triggers the inbound mirror, which force-resets local refs to upstream state ("upstream wins") | Recorded deviation ([ADR-44](../adr/044-outbound-push-forwarding-rides-a-separate-push-claim-lane.md), issue #90): implicit always-on forwarding invites silent divergence and needs GitHub App infrastructure this backend does not have. The explicit trigger + opt-in flag keep the direction observable (`repo.sync.*` events with `direction: "push"`) and non-destructive by default |

Extensions that upstream's spec does not list:

- the single-item `branch`/`tag` lookups (SDK-relied);
- webhook management endpoints (upstream documents webhooks but doesn't spec the CRUD);
- the unauthenticated `/healthz` liveness probe.

## How drift is tracked

Upstream evolves continuously, so drift is tracked in the open:

- **Vendored snapshot:** [`docs/upstream/`](../upstream) holds verbatim copies of the
  official OpenAPI spec ([`openapi.yaml`](../upstream/openapi.yaml)) and the doc pages the
  implementation was built against.
- **Recurring diff:** the live upstream spec and docs are periodically re-fetched and
  diffed against the snapshot and the implemented surface. Every new, changed, or
  deprecated upstream endpoint becomes a GitHub issue labeled
  [**`upstream-compat`**](https://github.com/coder/openstorage/issues?q=label%3Aupstream-compat).
  That label query is the live drift status.
- **Compatibility gates:** merge-gate CI runs the full e2e suites (API, git protocol,
  LFS) that pin the documented wire behavior. An opt-in SDK-compatibility suite drives
  the real vendored Go SDK against a live server (`OPENSTORAGE_SDK_DIR`, see
  `internal/api/sdkcompat_test.go`). A wire-contract regression fails a test, not a
  customer.

To evaluate a specific integration, check the [surface matrix](#implemented-surface) for
the feature groups you use, then the open
[`upstream-compat` issues](https://github.com/coder/openstorage/issues?q=label%3Aupstream-compat)
for anything upstream shipped that has not landed here yet.
