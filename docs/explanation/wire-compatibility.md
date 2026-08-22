# Wire compatibility with code.storage

This page is for platform teams and integrators deciding whether this backend can stand
in for [code.storage](https://code.storage) (Pierre Computer Company) behind their
existing clients — without reading the source. It states the compatibility contract, the
implemented surface, the deliberate deviations, what is out of scope, and how drift
against the live upstream service is tracked.

## The contract

The official code.storage SDKs (TypeScript, Python, Go) accept `apiBaseUrl` /
`storageBaseUrl` overrides. Point them at a codestorage deployment and client code runs
unchanged: same endpoints, same request/response shapes, same JWT claims and scopes, same
NDJSON streaming protocols, same webhook signatures, same git smart-HTTP and LFS
behavior. Plain `git` and `curl` work identically. The backend was built from a
[specification](../README.md) extracted independently from all three SDKs, a real
consumer, and the vendor's own OpenAPI spec and docs — the SDKs implement an identical
wire surface, which is what makes "wire-compatible" testable rather than aspirational.

The compatibility target is the surface **clients actually use**: everything the SDKs
call, plus the documented HTTP API. It is not a bug-for-bug clone of upstream internals.

## Implemented surface

The full endpoint catalog with request/response shapes is
[docs/03](../03-http-api.md) (REST), [docs/04](../04-streaming-commit-protocols.md)
(streaming writes), [docs/05](../05-git-protocol-and-namespaces.md) (git protocol, LFS,
namespaces), and [docs/06](../06-webhooks.md) (webhooks). By feature group:

| Surface | Status | Notes |
|---|---|---|
| Repository lifecycle | ✅ | Create (incl. fork via `base_repo`), get, `PATCH` (default branch), list, delete, `GET /api/repo-urls/{repo_id}` |
| Read surface | ✅ | Files (conditional/range streaming), file listings + metadata, branches, tags, commits, diff, branches-diff, blame, grep, archive, notes — cursor pagination throughout |
| Single-item lookups | ✅ | `GET .../branch`, `GET .../tag` — served even though the vendored spec omits them (SDK-relied surface) |
| Write surface | ✅ | NDJSON `commit-pack` and `diff-commit` streaming, `restore-commit`, branch/tag/notes writes — compare-and-swap (`expected_head_sha` etc.) and ref policies everywhere |
| Merge | ✅ | `POST .../merge` + read-only `GET .../merge/preview` |
| Both path styles | ✅ | `/api/v1/...` (repo from the JWT `repo` claim) and named-repo `/api/repos/{repo_name}/...` aliases over the same handlers |
| Deprecated spellings | ✅ | Documented upstream aliases accepted ([below](#deprecated-aliases)) |
| Git smart-HTTP | ✅ | Clone/fetch/push at `/{repo}.git` (repo ID or URL-encoded name), ref policies, signed-commit verification, atomic pushes ([below](#git-protocol-semantics)) |
| Namespaces & forks | ✅ | `+ephemeral` / `+import` namespaces; synchronous forks — the full fork-point closure is copied into the new repository (independent storage and [quota](../how-to/storage-lifecycle-and-sweeping.md#pack-quotas), nothing shared with the source). Git-pushed vs REST-written ephemeral state live in separate ref spaces ([deviation](#known-deviations-from-the-vendored-spec)) |
| Git LFS | ✅ | Batch API on the same remote; presigned S3 transfers with a server-proxied fallback |
| Webhooks | ✅ | HMAC-signed `push` and `repo.sync.*` events, plus webhook CRUD |
| Upstream sync & credentials | ✅ | `pull-upstream`, detach base, git-credential CRUD. Local-mode (SQLite) gap: a *git-surface* push does not auto-schedule upstream sync (the hook subprocess cannot reach the in-process queue) — API writes and explicit `pull-upstream` are unaffected |
| Auth model | ✅ | Customer-signed JWTs (`ES256`/`RS256`/`EdDSA`/`PS256`/`ES384`/`ES512`), scopes `git:read` / `git:write` / `repo:write` / `org:read` (exact matching — no scope implies another), `refs` policy tuples ([docs/02](../02-authentication.md)) |

### Deprecated aliases

Upstream renamed several parameters and body fields over time; SDKs in the wild still
send the old spellings. The backend accepts exactly the documented deprecated aliases —
no invented ones. On conflict the **current spelling wins** and the losing duplicate is
ignored rather than independently validated — with two deliberate exceptions: boolean
*query* aliases shape-validate both spellings (a request that `400`d on a malformed bool
before the alias existed keeps `400`ing), and the JWT `refs`/`ops` pair **merges**
rather than conflicts — see the table's last row:

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

Responses mirror the legacy names alongside the current ones where upstream does (e.g.
`url` mirrors `repo_name`, deleted-branch responses carry both `target_branch` and
`name`), so older SDK response parsing keeps working.

### Git protocol semantics

Beyond serving the smart-HTTP wire format, the semantics clients depend on match git:

- **Ref policies** from the JWT `refs` claim (`no-push`, `no-force-push`, `verify-sig`)
  are enforced in server-side hooks; unknown operations fail closed.
- **Atomic pushes** (`git push --atomic`): the server detects the `atomic` capability
  and evaluates the full policy set across every pushed ref *before* anything is
  stored — one policy-denied ref rejects the whole push, as in git. One documented
  deviation remains: refs advance through per-ref compare-and-swap on the ref
  authority, so a non-policy failure midway through the batch (e.g. a concurrent write
  landing on a later ref) can leave the batch's earlier refs already committed even
  though git reports the push as failed. Policy enforcement is all-or-nothing; the
  concurrent-conflict window is not. Without the flag, refs succeed and fail
  independently, as in git.
- **Signed pushes**: `verify-sig` validates SSH/PGP commit signatures against keys
  registered via [`codestorage signing-key`](../reference/cli.md#codestorage-signing-key).
  Verification shells out through git — `ssh-keygen` for SSH, `gpg` for PGP — and fails
  closed per ref when the binary is absent. The repository's supplied container image
  ships neither ([#50](https://github.com/coder/code-storage/issues/50)); install them
  before protecting refs with `verify-sig`.

## The architectural difference: single-org

Upstream code.storage is a multi-tenant service; this backend deliberately is not. One
deployment serves **one organization**, configured at boot
([`CODESTORAGE_ORG`](../reference/configuration.md#codestorage_org) +
[`CODESTORAGE_ORG_PUBKEYS`](../reference/configuration.md#codestorage_org_pubkeys)).

For clients this is invisible — tokens carry the same `iss`/`repo`/`scopes` claims either
way. The git smart-HTTP surface rejects any token whose `iss` is not the served org
outright (`403`). The REST surface scopes every request to the token's issuer instead:
with a single org row in the database — the normal state — an unknown issuer simply
fails verification, but org rows retained from an earlier `CODESTORAGE_ORG` value stay
addressable by their own tokens until removed
([#47](https://github.com/coder/code-storage/issues/47) tracks aligning this with the
git surface). What changes is operations: org onboarding and signing-key
registration are your deployment configuration (env + CLI) rather than a vendor control
plane, and serving multiple orgs means multiple deployments.

## Intentionally out of scope

The multi-tenant **control plane** around the storage API is not part of the
wire-compatibility target and is not implemented:

- Organization onboarding/management APIs, dynamic org routing.
- User and identity management: accounts, sessions, SSO/SCIM, dashboards.
- Billing, plans, usage metering APIs.
- The code.storage web UI and any UI-only endpoints.

If your integration only mints org-signed JWTs and talks to the documented storage API —
which is what the SDKs do — none of these are observable.

## Known deviations from the vendored spec

Tracked deviations where this backend intentionally differs from the upstream OpenAPI
snapshot (each recorded in the [decision log](../README.md#document-index)):

| Upstream spec | Here | Rationale |
|---|---|---|
| `POST .../reset-commits` (NDJSON) | Not implemented; `POST .../restore-commit` (plain JSON) covers the SDK-visible restore semantics | Recorded deviation (docs/12) — no SDK calls `reset-commits` |
| `DELETE /api/repos/{repo_name}/tags/{tag_name}` (tag in path) | `DELETE .../tags` with the tag in the request body | Matches the SDKs' actual call shape |
| `GET`/`POST /api/repos` (bare collection routes) | `/api/v1/repos` only; named-repo aliases exist for repo-scoped routes, not org-scoped collections | Alias layer covers the styles SDKs use |
| RFC 9457 problem-details error bodies (spec) | No problem-details anywhere. Failures use one of two shapes depending on route family and failure class: the plain `{"error":"<msg>"}` envelope (all read/management routes, and most ref/tag/notes write failures such as validation, CAS conflicts, and policy denials) or the [docs/04](../04-streaming-commit-protocols.md) `{"result":{"success":false,…}}` envelope (streaming commit-route failures and write-route auth failures). Status codes are meaningful throughout — the per-route contract is in docs/03 and docs/04 | Recorded deviations (docs/12, ADR-26): all three SDKs branch on status codes and none parses problem-details fields |
| `201` on streaming write endpoints (spec) | `commit-pack` / `diff-commit` / `restore-commit` return `200` | Recorded deviation (docs/12): the SDKs accept both; schema-validating clients should expect `200` |
| One ephemeral namespace across git and REST | Two separate ref spaces: git push to `/{repo}+ephemeral.git` writes a distinct `+ephemeral` storage key, REST `ephemeral=true` reads/writes `refs/namespaces/ephemeral/…` under the base repository — ephemeral state does not cross surfaces | Recorded cross-surface split (docs/13): REST writes follow the REST read surface so API-created ephemeral refs stay visible to `GET .../branches?ephemeral=true` and merge promotion |

And extensions upstream's spec does not list: the single-item `branch`/`tag` lookups
(SDK-relied), webhook management endpoints (upstream documents webhooks but doesn't spec
the CRUD), and the unauthenticated `/healthz` liveness probe.

## How drift is tracked

Upstream evolves continuously; wire compatibility is this project's purpose, so drift is
tracked as ordinary, visible project work:

- **Vendored snapshot:** [`docs/upstream/`](../upstream/) holds verbatim copies of the
  official OpenAPI spec ([`openapi.yaml`](../upstream/openapi.yaml)) and doc pages the
  implementation was built against.
- **Recurring diff:** the live upstream spec and docs are periodically re-fetched and
  diffed against the snapshot and the implemented surface. Every new, changed, or
  deprecated upstream endpoint becomes a GitHub issue labeled
  [**`upstream-compat`**](https://github.com/coder/code-storage/issues?q=label%3Aupstream-compat) —
  that label query is the live drift status.
- **Compatibility gates:** the merge-gate CI runs the full e2e suites (API, git
  protocol, LFS) that pin the documented wire behavior, and an opt-in SDK-compatibility
  suite drives the real vendored Go SDK against a live server
  (`CODESTORAGE_SDK_DIR`, see `internal/api/sdkcompat_test.go`) — so a regression against
  the wire contract fails a test, not a customer.

To evaluate SDK compatibility for a specific integration: check the
[surface matrix](#implemented-surface) for the feature groups you use, then the open
[`upstream-compat` issues](https://github.com/coder/code-storage/issues?q=label%3Aupstream-compat)
for anything upstream shipped that has not landed here yet.
