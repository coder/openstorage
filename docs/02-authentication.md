# Authentication: Customer-Signed JWTs

The defining feature of the auth model: **the customer signs their own tokens**. The backend
holds the customer's *public* key per organization and verifies signatures; it never issues
tokens. There are no sessions, API keys, or OAuth.

## Token transport

| Surface | Transport |
|---|---|
| HTTP API | `Authorization: Bearer <jwt>` |
| Git smart HTTP | Basic auth in remote URL: `https://t:<jwt>@{org}.code.storage/{repo_id}.git` (username is the literal `t`, password is the JWT) |
| Fork-source auth | `base_repo.auth.token` in create-repo body |

## Signing algorithms

Verification must accept, keyed per-org public key:

- **ES256** (default in all SDKs; P-256 ECDSA, PKCS#8 or SEC1 private keys client-side)
- **RS256** (Python SDK auto-selects for RSA keys)
- **EdDSA** (Python SDK auto-selects for Ed25519/Ed448)
- Official docs list **ES256 and RS256** (EC P-256/384/521 and RSA keys); the vendor's own
  integration-test helper additionally auto-detects **PS256, ES384, ES512** — accept them
  for maximal compatibility

JWT header: `{"alg": "<alg>", "typ": "JWT"}`.

## Claim set

```json
{
  "iss": "<org name>",                     // must match the org of the request host
  "sub": "@pierre/storage",                // fixed literal in all three SDKs
  "repo": "<repo url|id>" | "org",         // target repo; "org" for org-level operations
  "scopes": ["git:read", "git:write"],     // see scope table
  "iat": 1700000000,                       // seconds
  "exp": 1700003600,                       // seconds; SDK defaults: 1h per API call,
                                           // 365d for generated git remote URLs
  "refs": [["refs/heads/main", ["no-force-push"]],   // OPTIONAL ordered ref policies
           ["refs/heads/feature/*", []]],
  "ops": ["no-force-push"]                 // OPTIONAL, deprecated repo-wide policy ops
}
```

Notes:
- `repo` carries the repository **url** value (what `GET /api/repo-urls/{repo_id}` returns).
- Verification requirements: valid signature for the org, `exp` in the future, `repo` claim
  matching the target repo (or `"org"` for org-scoped endpoints), and required scopes present.
- SDKs mint a fresh JWT per request when configured with a private key; pre-minted tokens are
  sent verbatim. The backend must not assume token reuse or caching either way.

## Scopes

| Scope | Grants |
|---|---|
| `git:read` | all read endpoints (files, branches, tags, commits, diffs, blame, grep, notes read, merge preview, `GET /repo` metadata) + git clone/fetch |
| `git:write` | ref mutations (commit-pack, diff-commit, restore-commit, branch create/delete, merge, tag create, notes write/delete, pull-upstream) + git push |
| `repo:write` | repo lifecycle (create/delete repo, git-credentials CRUD, unset base repo) |
| `org:read` | org-wide listing (`GET /repos`, `GET /api/repo-urls/{repo_id}`) |

Endpoint quirks (verified across all three SDKs):
- `DELETE /repos/tags` is requested with **both** `git:read` and `git:write`.
- `createRepo` mints `repo:write` with `repo` = the *new* repo id (client-generated UUID if
  unspecified).
- `listRepos`, `updateGitCredential`, `deleteGitCredential` use `repo: "org"`;
  `createGitCredential` uses `repo: <repoId>`.

## Ref policies (`refs` claim) — branch protection embedded in tokens

Encoding: JSON array of `[pattern, ops]` tuples (tuple = 2-element array). Evaluation:

1. Normalize shorthand in **both** the target ref and rule patterns: anything not starting
   with `refs/` (and not exactly `*` or `HEAD`) becomes `refs/heads/<value>` (`main` →
   `refs/heads/main`, `agents/*` → `refs/heads/agents/*`).
2. Scan rules in order; **first pattern match wins** (glob patterns, e.g. `refs/heads/*`).
3. Apply that rule's ops; an empty ops list means "allow everything".

| Op | Enforcement |
|---|---|
| `no-push` | reject any update to matching refs |
| `no-force-push` | reject non-fast-forward updates (git push -f, restore-commit that rewinds, etc.) |
| `verify-sig` | reject pushes introducing commits without a valid signature from a registered key |

These apply to git receive-pack **and** every HTTP write endpoint (they are the only
branch-protection mechanism; there is no server-side protection config).

## Failure mapping

| Condition | Status |
|---|---|
| missing/invalid/expired token | 401 |
| valid token, insufficient scope or ref policy denies | 403 |
| `repo` claim mismatch with target | 403 (do not leak existence: 404 acceptable per vendor error-string instability) |

Error body: `{"error": "<message>"}` — clients branch on status codes, not messages.
