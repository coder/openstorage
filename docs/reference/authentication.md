# Authentication: Customer-Signed JWTs

**Customers sign their own tokens.** The backend holds each organization's *public* key
and verifies signatures; it never issues tokens. There are no sessions, API keys, or
OAuth. Endpoint scopes per route: [HTTP API](http-api.md).

## Token transport

| Surface | Transport |
|---|---|
| HTTP API | `Authorization: Bearer <jwt>` |
| Git smart HTTP | Basic auth in remote URL: `https://t:<jwt>@{org}.code.storage/{repo_id}.git` (username is the literal `t`, password is the JWT) |
| Fork-source auth | `base_repo.auth.token` in create-repo body |

## Signing algorithms

Verification accepts these algorithms, keyed by the per-org public key:

| Algorithm | Notes |
|---|---|
| **ES256** | Default in all SDKs; P-256 ECDSA; PKCS#8 or SEC1 private keys client-side |
| **RS256** | Python SDK auto-selects for RSA keys |
| **EdDSA** | Python SDK auto-selects for Ed25519/Ed448 |
| **PS256, ES384, ES512** | Auto-detected by the vendor's integration-test helper; accepted for maximal compatibility |

Official docs list only **ES256 and RS256** (EC P-256/384/521 and RSA keys).

JWT header: `{"alg": "<alg>", "typ": "JWT"}`.

### Token encoding

A token is a compact JWS: three segments (header, payload, signature) separated by `.`.

- Each segment must be canonical unpadded base64url (RFC 7515): no `=` padding, no `+`
  or `/`, no spaces or line breaks, and unused trailing bits set to zero. Any other
  spelling gets **401**, even when it decodes to the same bytes.
- The rule applies to the token itself. The HTTP API trims spaces around the
  `Authorization` value; the Git Basic-auth password is checked as sent.
- Standard JWT libraries emit this form, including the ones the SDKs use (`jose`, PyJWT,
  golang-jwt). Earlier versions accepted other spellings; a hand-built token that relies
  on one must be reissued.
- Canonical encoding does not provide replay protection or make token text a unique
  credential identity. For example, an ECDSA signature has a second valid form with
  different bytes.

### Organization public-key strength

RSA organization public keys need a modulus of **at least 2048 bits**, for both RS256 and
PS256. ECDSA (P-256/384/521) and Ed25519 are unaffected.

- **Registration:** only through single-org boot configuration (`OPENSTORAGE_ORG_PUBKEYS`).
  There is no HTTP org-key registration endpoint.
- **Startup** rejects a weak RSA key, reporting the file path and required minimum.
- **Every request** checks every registered PEM block, including keys stored before the
  upgrade.
- **One weak key rejects the whole set:** even a token signed by a strong key gets
  **401**, not 500. Weak keys are never silently skipped, and there is no override.
- **Scope:** public authentication keys only. Client-side private-key parsing and
  token-minting helpers are unchanged, so a successfully minted token does not mean the
  server accepts its key.

> [!WARNING]
> **Rotate before upgrading.** On the old fleet, add a strong key, switch clients to it,
> and reissue tokens (including Git credentials). Then remove every weak key from the PEM
> files on all replicas and from registered org key sets before rolling out. Updating
> only the database is not enough: startup replaces the configured org's registered keys
> from its file. Old weak-key tokens stop working. Adding a strong key without removing
> the weak one is not enough either. See
> [fleet key rotation](../how-to/deploy-production.md#jwt-keys-across-the-fleet).

## Claim set

```json
{
  "iss": "<org name>",                     // must match the org of the request host
  "sub": "<any non-empty string>",         // caller label, e.g. "pierre-sdk-go"
  "repo": "<repo url|id>" | "org",         // target repo; "org" or omitted for org-level operations
  "scopes": ["git:read", "git:write"],     // see scope table
  "iat": 1700000000,                       // seconds
  "exp": 1700003600,                       // seconds; SDK defaults: 1h per API call,
                                           // 365d for generated git remote URLs
  "refs": [["refs/heads/main", ["no-force-push"]],   // OPTIONAL ordered ref policies
           ["refs/heads/feature/*", []]],
  "ops": ["no-force-push"]                 // OPTIONAL, deprecated repo-wide policy ops
}
```

- `repo` carries the repository **url** value (what `GET /api/repo-urls/{repo_id}` returns).
- A token with no `repo` claim is org-wide and behaves exactly like `repo: "org"`:
  org-scoped endpoints accept it, and repository endpoints and the git surface refuse it
  with `403`. A `repo` that is present but empty (`""` or `null`) is not org-wide: every
  endpoint refuses it with `403`.
- `sub` is a free-text label that the signer picks ("Agent identity (for logging)" in the
  [upstream guide](../upstream/getting-started/authentication.md)). Any non-empty string
  verifies, and an empty or missing `sub` is a `401`. It grants nothing, and open.storage
  does not log it or key anything on it. SDK 1.17.1 and `openstorage mint` send
  `@pierre/storage`. SDK 0.1.0 sends `pierre-sdk-go`, `pierre-sdk-python` or
  `pierre-sdk-typescript`, and omits `repo` for org-wide calls.
- A token verifies when: the signature is valid for the org, `exp` is in the future, `sub`
  is not empty, `repo` matches the target repo (or is `"org"` or omitted for org-scoped
  endpoints), and the required scopes are present.
- SDKs mint a fresh JWT per request when configured with a private key, and send
  pre-minted tokens verbatim. The backend must not assume token reuse or caching.

## Scopes

| Scope | Grants |
|---|---|
| `git:read` | all read endpoints (files, branches, tags, commits, diffs, blame, grep, notes read, merge preview, `GET /repo` metadata) + git clone/fetch |
| `git:write` | ref mutations (commit-pack, diff-commit, restore-commit, branch create/delete, merge, tag create, notes write/append/delete, pull-upstream and `upstream/pull`) + git push |
| `repo:write` | repo lifecycle (create/delete repo, git-credentials CRUD, unset base repo via `DELETE .../base` or `DELETE .../upstream`) |
| `org:read` | organization metadata (`GET /api/meta`, `repo: "org"` or no `repo`) and org-wide listing (`GET /repos`, `GET /api/repo-urls/{repo_id}`) |

Endpoint quirks (verified across all three SDKs):

- `DELETE /repos/tags` is requested with **both** `git:read` and `git:write`.
- `createRepo` mints `repo:write` with `repo` = the *new* repo id (client-generated UUID if
  unspecified).
- `listRepos`, `updateGitCredential`, `deleteGitCredential` use `repo: "org"`;
  `createGitCredential` uses `repo: <repoId>`. SDK 0.1.0 omits `repo` for these org-wide
  calls instead.

## Ref policies (`refs` claim) — branch protection embedded in tokens

Encoding: a JSON array of `[pattern, ops]` 2-element tuples. Evaluation:

1. Normalize shorthand in **both** the target ref and rule patterns: anything not starting
   with `refs/` (and not exactly `*` or `HEAD`) becomes `refs/heads/<value>` (`main` →
   `refs/heads/main`, `agents/*` → `refs/heads/agents/*`).
2. Scan rules in order; **first pattern match wins** (an exact ref, a lone `*`, or a single
   trailing `/*` prefix such as `refs/heads/*`; see [`openstorage mint`](cli.md#openstorage-mint)).
3. Apply that rule's ops; an empty ops list means "allow everything".

| Op | Enforcement |
|---|---|
| `no-push` | reject any update to matching refs |
| `no-force-push` | reject non-fast-forward updates (git push -f, restore-commit that rewinds, etc.) |
| `verify-sig` | reject pushes introducing commits without a valid signature from a registered key |

Ops apply to git receive-pack **and** every HTTP write endpoint. They are the only
branch-protection mechanism; there is no server-side protection config.

## Failure mapping

| Condition | Status |
|---|---|
| missing/invalid/expired token | 401 |
| valid token, insufficient scope or ref policy denies | 403 |
| `repo` claim mismatch with target | 403 (do not leak existence: 404 acceptable per vendor error-string instability) |

Error body: `{"error": "<message>"}` plus the problem-details fields described under
[HTTP API conventions](http-api.md#conventions) (`type`, `title`, `status`, a generic
`code` such as `unauthorized` or `forbidden`, and `detail`). Ref-moving write endpoints
answer with the `result` envelope instead
([streaming protocols](../explanation/streaming-commit-protocols.md)). Clients branch on
status codes (or `code` where present), not messages.
