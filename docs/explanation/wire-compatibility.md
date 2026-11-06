# Wire compatibility with code.storage

**TL;DR:** open.storage can stand in for [code.storage](https://code.storage) (Pierre
Computer Company) behind existing clients. The main differences are one organization per
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
| Repository lifecycle | ✅ | Create (incl. Git Sync via `upstream`, and copy or fork via `base_repo`), get (with `upstream` for sync-linked repos), `PATCH` (default branch), list, delete, `GET /api/repo-urls/{repo_id}` (an extension, see below) |
| Read surface | ✅ | Files (conditional/range streaming), file listings + metadata, branches, tags, commits, diff, branches-diff, blame, grep, archive, notes — cursor pagination throughout. File listings without `limit` and `cursor` return every entry, and `files/metadata` answers `429` past 50,000 distinct last commits (spec 2026-10-27) |
| Single-item lookups | ✅ | `GET .../branch`, `GET .../tag` — served although the vendored spec omits them (SDK-relied surface) |
| Write surface | ✅ | NDJSON `commit-pack` and `diff-commit` streaming, `restore-commit`, branch/tag/notes writes — compare-and-swap (`expected_head_sha` etc.) and ref policies everywhere |
| Merge | ✅ | `POST .../merge` + read-only `GET .../merge/preview` |
| Both path styles | ✅ | `/api/v1/...` (repo from the JWT `repo` claim) and the canonical `/api/repos` routes over the same handlers: `GET`/`POST /api/repos` and the named-repo `/api/repos/{repo_name}/...` aliases, including `DELETE .../tags/{tag_name}` |
| Deprecated spellings | ✅ | Documented upstream aliases accepted ([below](#deprecated-aliases)) |
| Git smart-HTTP | ✅ | Clone/fetch/push at `/{repo}.git` (repo ID or URL-encoded name), ref policies, signed-commit verification, atomic pushes ([below](#git-protocol-semantics)) |
| Namespaces & forks | ✅ | `+ephemeral` / `+import` namespaces. Synchronous forks copy the full fork-point closure into the new repository (independent storage and [quota](../how-to/storage-lifecycle-and-sweeping.md#pack-quotas), nothing shared with the source). Git-pushed and REST-written ephemeral state live in separate ref spaces ([deviation](#known-deviations-from-the-vendored-spec)) |
| Git LFS | ✅ | Batch API on the same remote; presigned S3 downloads with a server-proxied fallback, verified server-proxied uploads ([below](#git-lfs-transfer-semantics)) |
| Webhooks | ✅ | HMAC-signed `push` and `repo.sync.*` events, plus webhook CRUD. Outbound push-upstream runs send the extension events `repo.push_upstream.*` |
| Upstream sync & credentials | ✅ | `pull-upstream`, `push-upstream` (outbound forwarding: explicit trigger plus opt-in `base_repo.forward_pushes`; trigger semantics differ from upstream's implicit forwarding, see the [deviation](#known-deviations-from-the-vendored-spec)), detach base, git-credential CRUD. SQLite local mode and experimental s3wal mode schedule git-surface pushes like Postgres: an authorized push records durable intent that the serve process runs once the push's refs are published |
| Throttled responses | ✅ | Every HTTP-level `429` (gateway [rate limiter](../reference/configuration.md#rate-limiting) and busy/backpressure paths) carries both upstream-documented headers: `Retry-After` (delay-seconds) and the IETF `RateLimit` service-limit field (`"default";r=0;t=<retry-after-secs>`); `Retry-After` takes precedence. The `files/metadata` `429` for more than 50,000 distinct last commits carries neither: upstream says a retry gets the same response (spec 2026-10-27) |
| Auth model | ✅ | Customer-signed JWTs (`ES256`/`RS256`/`EdDSA`/`PS256`/`ES384`/`ES512`), scopes `git:read` / `git:write` / `repo:write` / `org:read` (exact matching — no scope implies another), `refs` policy tuples, any non-empty `sub` (SDK 0.1.0 sends `pierre-sdk-go`, `pierre-sdk-python` or `pierre-sdk-typescript`), and a token without `repo` is org-wide, like `repo: "org"` ([auth spec](../reference/authentication.md)) |

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
- For every pair in the table below, the current spelling counts as sent only when it is
  non-empty. An empty current value falls back to the deprecated one.
- The commit metadata names (`expected_target_sha`, `target_is_ephemeral`,
  `base_is_ephemeral`, and restore's `base_ref`, described in
  [streaming protocols](streaming-commit-protocols.md)) win whenever they are present,
  even when empty or `false`. So do the `DELETE .../branches` and `DELETE .../notes` query
  parameters over the deprecated request body.
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
both `target_branch` and `name`; commit-write `result` objects carry both `target_branch`
and `branch`; [webhook](../reference/webhooks.md) bodies carry
`repository.repo_name` and `org` next to the deprecated `repository.url` and `customer_id`.

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
| `POST /repos` `upstream.access`: `app` (the default for `github`) reads through the organization's GitHub App installation | Only `access: "public"` is accepted; `app` or an omitted `access` with `provider: "github"` → 400. A public mirror syncs on the same triggers as a `base_repo` sync link, not only on request | No GitHub App integration. Private GitHub repositories sync through the deprecated `base_repo` form with `auth.token` |
| `POST /repos` `upstream.host`: a bare hostname | Also accepts an `http(s)://` base URL, as `base_repo.upstream_host` does | Self-hosted forges on plain HTTP or a sub-path; the same validation and egress policy apply |
| `Deprecation` header on the deprecated `pull-upstream` and `base` routes: the spec defines none. Upstream's changelog of 2026-10-07 says every response carries one but gives no value | `Deprecation: @1791331200` (RFC 9745 date, 2026-10-07T00:00:00Z) on every response of the four routes, errors included ([details](../reference/http-api.md#deprecation-header)). No `Link` or `Sunset` | Upstream's exact value is undocumented, so open.storage uses the standard form |
| `POST /repos` with both `upstream` and `base_repo`: "do not set" both, no documented error | 400 | Ambiguous request; failing loudly beats guessing which form wins |
| `POST /repos` without `default_branch`: a sync from GitHub takes the GitHub repository's default branch (spec `40e43e00`, 2026-10-28) | PostgreSQL and SQLite: the repository starts on `main`, and the first successful sync adopts the upstream `HEAD` branch before it sends `repo.sync.succeeded`. S3-WAL: the repository keeps `main` | The create makes no network call ([#599](https://github.com/coder/openstorage/issues/599)). S3-WAL metadata v2 is frozen and cannot store the "branch omitted" flag ([#603](https://github.com/coder/openstorage/issues/603), trigger: the next S3-WAL metadata format change) |
| `POST /repos` is idempotent on the name: `200` for any existing name unless its upstream differs (spec 2026-10-07) | A finished repo answers `200` to a matching repeat create: a sync create with the same upstream and `forward_pushes`, or a plain, copy or fork create of a repo without an upstream. A different upstream is `409`, as upstream documents, and so is a create form that does not match the repo (sync onto a plain repo, plain onto a synced repo). Repos created before create tracking (on S3-WAL, every repo created by v0.18.0 or earlier) answer `409` to every repeat. While a copy or fork create is still running, plain, copy and fork creates of the name are `503` with `Retry-After`. A pending sync create is finished by a matching sync create with `git:write` (`200`); other forms are `409` ([Repeat creates](../reference/http-api.md#repository-lifecycle-repowrite--orgread)). A name a deleted repo holds is `404` | A `200` names a repo only after its create finished, so it never names a repo that the failing create then deletes ([#388](https://github.com/coder/openstorage/issues/388)). A form mismatch is `409` because a `200` would hide that the request was not applied. Older repos store no create state (#335 for S3-WAL) |
| RFC 9457 problem-details error bodies (spec). Since upstream's 2026-09-24 spec update, `ProblemDetails` requires a snake_case `code` and tells clients to branch on `code`, not on `detail` | The plain `{"error":"<msg>"}` envelope carries problem-details fields next to `error`: `type` (`about:blank`), `title`, `status`, `detail` (same string as `error`), `code` ([#349](https://github.com/coder/openstorage/issues/349)), and `instance`, the request path without the query ([#352](https://github.com/coder/openstorage/issues/352)). `code` is upstream's generic per-status code, except for eleven of upstream's specific codes: `repository_not_found`, `insufficient_scope`, `repo_name_mismatch`, `branch_not_found`, `ref_not_found`, `credential_not_found`, `credential_exists`, `repository_deleted`, `authentication_unavailable` (403, key store failure) and `route_not_found` (404, with a 405 `method_not_allowed` problem body for a path that has routes for other methods) ([#352](https://github.com/coder/openstorage/issues/352)), and `ref_guard_failed` with its `guard` and `current_sha` members on every failed ref guard ([#568](https://github.com/coder/openstorage/issues/568), [#647](https://github.com/coder/openstorage/issues/647)). A 400 for a request body carries the optional `invalid_params` list (`ValidationProblemDetails`, spec 2026-10-08, [#396](https://github.com/coder/openstorage/issues/396)) when open.storage can name the invalid fields: body decode failures (wrong JSON type, and unknown, duplicate or `null` members of strictly decoded bodies) and single-field checks (required fields, allowed values, ranges, name formats). Every body with the problem members has `Content-Type: application/problem+json`, as upstream documents and serves ([#352](https://github.com/coder/openstorage/issues/352) item 5); result envelopes and success bodies stay `application/json`. Not emitted: upstream's other specific codes and `invalid_params` for failures that cannot name one field. `repo_claim_missing` is not sent because a token without a `repo` claim is org-wide ([#658](https://github.com/coder/openstorage/issues/658)): a per-repo route refuses it with 403, as it refuses `repo: "org"`. `merge_conflict` is listed upstream, but the merge 409 for file conflicts is documented as `MergeConflict`, which has no `code` member; `sync_in_progress` and `upstream_already_configured` are documented on no operation, and no open.storage answer matches them; the hosting, deployment, GitHub App, cold-storage and idempotency codes belong to features open.storage lacks. The [streaming protocols](streaming-commit-protocols.md) `{"result":{"success":false,…}}` envelope carries no problem-details fields, except the 409 body of a failed ref guard on a commit write or a notes write, which keeps the `result` object (and the note members) and adds the problem members as upstream's `CommitWriteRefGuardProblemDetails`, `RestoreCommitRefGuardProblemDetails` and `NotesWriteRefGuardProblemDetails`, sent as `application/problem+json`. Auth failures (401, 403), the gateway 429 and the path-style alias and git-credentials failures use the plain problem body on every route, as upstream documents ([#352](https://github.com/coder/openstorage/issues/352) item 3). `commit-pack`, `diff-commit` and `restore-commit` send the envelope only on the statuses for which upstream documents `CommitWriteResponse` or `ReposCommitsRestoreResponse` (400, 409, 412 and 413 on commit-pack; 400, 409 and 412 on diff-commit; 412 on restore-commit) and the plain problem body on every other failure. Merge answers file conflicts with upstream's `MergeConflict` (`error`, `conflict_type: merge_conflict`, `conflict_paths`, `merge_base_sha`) and every other failure with the plain problem body. A target that moved during the merge is a 409 `conflict` problem body: upstream sends `MergeConflict` with `target_moved` or `repo_changed` only after an automatic stale-target retry, which open.storage does not do. Note writes answer every failure with the plain problem body except the guard body above. Failures use one of these shapes, by route and failure class. Do not assume one shape per route family: branch on the status code, or check both shapes. Per-route contracts are in [HTTP API](../reference/http-api.md) and [streaming protocols](streaming-commit-protocols.md) | Recorded deviations ([phase-3 notes](../archive/phases/12-phase3-implementation-notes.md), [ADR-26](../adr/026-result-envelope-statuses-499-is-not-emittable.md)): all three SDKs branch on status codes. At the 2026-09-25 audit this held for SDK `1.17.1` (stable) and `1.18.0-beta.0` (beta): no SDK reads `code`, `detail`, or any problem-details field other than the legacy `error` string. SDK `0.1.0` (Go, Python, TypeScript, stable since 2026-11-04) reads every error body as JSON whatever its type, and the `1.18.0-beta` clients accept `application/problem+json`; the `1.17.1` Go and TypeScript clients parse error JSON only for `application/json`, so on a problem body they report the raw body text as the message (a breaking change for them, accepted because upstream sends the same type). Upstream says to treat an unknown code as the generic code for its status, so a client that follows that guidance handles every `code` open.storage sends; a client that expects one of the specific codes open.storage does not emit sees the generic one instead. `status` is a JSON number, as in upstream's bodies: a client that decoded the old `{"error"}` body into a string-only map (such as Go's `map[string]string`) must switch to a struct or `map[string]any`. At the 2026-10-08 audit, no SDK (`1.17.1`, `1.18.0-beta.1`) reads `invalid_params`; the member is optional, so a client that ignores it sees no change |
| `201` on write endpoints (spec) | `commit-pack` / `diff-commit` / `restore-commit` and `POST`/`DELETE .../notes` return `200`. The path-style `POST .../notes/append` returns `201` | Recorded deviation ([phase-3 notes](../archive/phases/12-phase3-implementation-notes.md)): the SDKs accept both; schema-validating clients should expect `200` on those legacy routes and `201` on `notes/append` |
| `restore-commit`: `412` when the branch already points at the target, the tree would not change, or the target is not an ancestor of the tip (spec) | Enforced for `base_ref`. The deprecated `target_commit_sha` form keeps restoring without these checks | The SDKs send `target_commit_sha`; adding the checks there would change what SDK callers see |
| `DELETE .../branches` naming the default branch: `409` (spec) | `409` for the `target_branch` query parameter form. `403` for the deprecated body form | The SDKs send the body form; changing its status would change what SDK callers see |
| Streaming commit chunk size (`commit-pack` `blob_chunk`, `diff-commit` `diff_chunk`): the spec limits a decoded chunk to 4 MiB but documents no status for a larger one. The spec of 2026-10-28 dropped commit-pack's older `413` for it, and diff-commit never had one | A decoded `blob_chunk` or `diff_chunk` over 4 MiB answers `400` `invalid`. The spec's `413` for more than 262144000 decoded bytes per commit-pack is implemented | Desk decision ([#597](https://github.com/coder/openstorage/issues/597)): the limit bounds the memory one chunk needs, and all three SDKs send at most 4 MiB per chunk |
| Credential-create response includes a summary (`created_at`, …) | `POST .../git-credentials` returns only `{"id": …}` | Recorded deviation ([phase-7 notes](../archive/phases/16-phase7-implementation-notes.md)); fetch the summary via the credential list if needed |
| One ephemeral namespace across git and REST | Two separate ref spaces: git push to `/{repo}+ephemeral.git` writes a distinct `+ephemeral` storage key; REST `ephemeral=true` reads/writes `refs/namespaces/ephemeral/…` under the base repository. Ephemeral state does not cross surfaces | Recorded cross-surface split ([phase-4 notes](../archive/phases/13-phase4-implementation-notes.md)): REST writes follow the REST read surface, so API-created ephemeral refs stay visible to `GET .../branches?ephemeral=true` and merge promotion |
| Implicit push forwarding: in upstream's GitHub App mode every `git push` to the default remote is proxied straight to GitHub, always on, with no REST trigger ([integrations guide](../upstream/guides/integrations.md)). Since spec `412b90c3` (2026-11-02), the REST writes (`commit-pack`, `diff-commit`, `reset-commits`, `restore-commit`, `merge`, and branch and tag create and delete) on a repository with a GitHub upstream also push to GitHub before they answer. They answer `412` when the write cannot reach GitHub or GitHub refuses it: the commit writes (`commit-pack`, `diff-commit`, `reset-commits`, `restore-commit`) with `result.status` `failed`, the others with the code `github_app_not_configured`, `github_repository_inaccessible` or `precondition_failed`. They answer `502` when GitHub fails or cannot be reached, and then the write can already be applied | Explicit `POST /repos/push-upstream` (non-force by default, `force: true` opt-in) plus an opt-in per-repo `base_repo.forward_pushes` flag (default **off**) that schedules a forward after every won local push and mirrors back on success. A rejected non-fast-forward fails the run with per-ref reasons instead of rewriting upstream history. Ref deletions are not propagated in either direction (the inbound mirror deliberately avoids `--mirror` for the same reason). With `forward_pushes` off, a local push still triggers the inbound mirror, which force-resets local refs to upstream state ("upstream wins"). REST writes change only local refs and answer without contacting GitHub. A won REST write pokes the same sync scheduler as a won push, with the same caller gates: on a `forward_pushes` repo it schedules the same forward, unless the token lacks `git:read` or carries a ref-policy op. Then the write stays local until the next authorized trigger. A scheduled forward runs in the background, and the answer does not wait for it. No REST write returns a `412` or `502` for a GitHub failure | Recorded deviation ([ADR-44](../adr/044-outbound-push-forwarding-rides-a-separate-push-claim-lane.md), issue #90): implicit always-on forwarding invites silent divergence and needs GitHub App infrastructure this backend does not have. The explicit trigger + opt-in flag keep the direction observable (`repo.push_upstream.*` events) and non-destructive by default. Upstream's REST write-through runs through its GitHub App, and open.storage rejects `access: "app"` with `400` ([#654](https://github.com/coder/openstorage/issues/654)) |
| `GET .../file` and `POST .../archive`: no documented file-size cap and no `422`; `archive` has only the caller-chosen `max_blob_size` | `file` and `archive` answer 422 for a file over 100 MiB (`archive` can skip such files with `max_blob_size`), before any response byte. The other read-side git [resource limits](../reference/http-api.md#resource-limits-on-git-reads) apply too. After an `archive` response has started, a limit closes the connection instead of returning a status | Hardening ([ADR-49](../adr/049-read-side-git-children-run-under-one-per-request-budget.md)): the limits bound server memory, temporary disk and delivery time for data that anyone who can push controls. Fetch larger files with git |
| Every `503` response can carry an optional `Retry-After` header, the minimum delay in seconds before a retry (spec 2026-10-21) | Two 503s leave `Retry-After` out: a git read that exceeds its [time budget](../reference/http-api.md#resource-limits-on-git-reads), and a sync create whose upstream link was removed before its first sync was scheduled ([pending creates](../reference/http-api.md#repository-lifecycle-repowrite--orgread)). The 503 for exhausted git concurrency and the 503s for a create that is not finished carry `Retry-After` | A retry of either request fails the same way, so a `Retry-After` would invite a useless retry. The same page needs the same git time ([ADR-49](../adr/049-read-side-git-children-run-under-one-per-request-budget.md)). The pending create stays pending until `DELETE /repos/delete` clears the name ([#388](https://github.com/coder/openstorage/issues/388)) |
| A token that fails verification (malformed, bad signature, expired, unknown issuer): upstream's live docs and servers answer `403` `forbidden` (`Invalid or expired token`) on the REST API, and `403` on the Git and LFS transports too (checked 2026-11-04). The vendored spec documents no status for it | The REST API answers `403` `forbidden` (`Invalid or expired token`), as upstream does ([#664](https://github.com/coder/openstorage/issues/664)). The Git and LFS transports answer `401` with `WWW-Authenticate: Basic`, as for a missing token ([authentication](../reference/authentication.md#failure-mapping)) | Git asks for credentials only after a `401`, and only a `401` makes it erase a stored credential. After a `403`, git fails and keeps the expired token in the credential helper, so every later command fails the same way until the user removes the token |
| Every path field and `path` parameter is a plain `type: string`. The spec says nothing about tree entry names whose bytes are not valid UTF-8 | A response path that is not valid UTF-8 still lists with one U+FFFD per invalid byte, and gets an extra sibling `<field>_base64` with the raw bytes in standard base64 (for example `path_base64`, `paths_base64`, `old_path_base64`, `original_path_base64`). The sibling is absent for valid UTF-8 paths, so those responses are unchanged. This covers `files`, `files/metadata`, the diff routes, `blame`, `grep` and the merge conflict paths (`conflict_paths_base64` and `path_base64`). JSON request bodies cannot name such a file yet. The `path` query parameter takes the raw bytes percent-encoded ([path encoding](../reference/http-api.md#conventions)) | JSON strings hold Unicode text, so such a byte has no exact JSON form. A sibling field (like GitHub's `pathRaw`) is additive: no SDK reads it, and the SDKs ignore response keys they do not know. Upstream's schemas for these responses are closed (`additionalProperties: false`), so a client that validates a response against the vendored spec as written rejects one that has a sibling. No SDK validates that way. If upstream documents another form, we carry both ([#587](https://github.com/coder/openstorage/issues/587)) |
||||||| caa296e6
| Every path field and `path` parameter is a plain `type: string`. The spec says nothing about tree entry names whose bytes are not valid UTF-8 | Each invalid byte comes back as U+FFFD in JSON, so two names can list as the same path, and a listed path can name a different file whose name holds a real U+FFFD. The `path` query parameter takes the raw bytes percent-encoded, and JSON request bodies cannot name such a file ([path encoding](../reference/http-api.md#conventions)) | JSON strings hold Unicode text, so such a byte has no exact JSON form. An extra base64 field next to each path (like GitHub's `pathRaw`) waits for the triggers in [#587](https://github.com/coder/openstorage/issues/587): a user who cannot read a path back, an upstream raw or encoded path form, or the next API change that adds a path field |

Extensions that upstream's spec does not list:

- the single-item `branch`/`tag` lookups (SDK-relied);
- webhook management endpoints (upstream documents webhooks but doesn't spec the CRUD);
- the `repo.push_upstream.*` webhook events for outbound push-upstream runs. Upstream
  has no outbound-run event, and its `repo.sync.*` schemas forbid the outbound fields, so
  every `push` and `repo.sync.*` event matches upstream's `webhooks` schemas
  ([#354](https://github.com/coder/openstorage/issues/354));
- on `POST .../restore-commit`, the `target_is_ephemeral` flag (deprecated alias
  `ephemeral`, accepted there before upstream's 2026-10-05 spec) and a fully qualified
  `refs/...` `base_ref`. Upstream's `ReposCommitsRestoreRequest` lists neither. Restore
  also accepts `base_is_ephemeral` (deprecated alias `ephemeral_base`) but ignores it
  ([HTTP API](../reference/http-api.md));
- `GET /api/repo-urls/{repo_id}`, which upstream removed from its spec on 2026-10-05.
  It resolves a repository ID only inside the token's organization (`org:read`);
- the `<field>_base64` sibling of each response path field (`path_base64`,
  `paths_base64`, `old_path_base64`, `original_path_base64`, `conflict_paths_base64`),
  present only for a path that is not valid UTF-8
  ([path encoding](../reference/http-api.md#conventions));
- the unauthenticated `/healthz` liveness probe.

## How drift is tracked

Upstream evolves continuously, so drift is tracked in the open:

- **Vendored snapshot:** [`docs/upstream/`](../upstream) holds verbatim copies of the
  official OpenAPI spec ([`openapi.yaml`](../upstream/openapi.yaml)) and the doc pages the
  implementation was built against. The current `openapi.yaml` is
  <https://code.storage/docs/openapi.yaml> as fetched on 2026-11-04 (upstream
  `Last-Modified` 2026-11-04 02:26:55 GMT), SHA-256
  `ee8092bfc51e6a94187e1c26574505aa941eb7cd6c2aaa4f8258cd0fd9d5c3ff`.
  [The cluster OpenAPI](https://api.pierre.code.storage/api/openapi.yaml) served
  `412b90c3` at 10:07 and 10:11 UTC on 2026-11-04, so the change since `412b90c3` is
  documented drift, not a proven deployment change (a spec that the cluster serves
  would not prove how the cluster behaves either). Against `40e43e00` (2026-10-28),
  the snapshot adds three things:
  - A `prefix` query parameter on `GET /repos` (snapshot `34ac6de1`, 2026-11-01): a
    case-insensitive filter on the start of `repo_name`, combined with `q`, which then
    matches only the rest of the name after the prefix. open.storage does both
    ([#651](https://github.com/coder/openstorage/issues/651)).
  - GitHub causes for `412` and a new `502` on 18 write operations (snapshot
    `412b90c3`, 2026-11-02): a write to a repository with a GitHub upstream that
    cannot reach GitHub, or that GitHub refuses or fails. The `412` answers keep their
    local causes, such as a merge without a common ancestor, and the `502` also covers
    a failed storage node or upstream Git server. open.storage does not push REST
    writes to GitHub, so it never sends the GitHub-caused answers (the
    implicit push forwarding row under
    [known deviations](#known-deviations-from-the-vendored-spec),
    [#654](https://github.com/coder/openstorage/issues/654)).
  - Wording on empty repositories (snapshot `ee8092bf`, 2026-11-04): a new
    repository has no branch until its first commit, so `GET .../branch` answers
    `404`, the branch list is empty, and `branches/create` answers `404` because its
    base does not exist. Only descriptions changed, and open.storage already behaves
    this way ([#657](https://github.com/coder/openstorage/issues/657)).
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
