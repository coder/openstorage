# Token auth and scopes

Every REST and Git request carries a JWT that the organization signed with its own key. The server answers `401` when the token is missing, malformed, signed by another key, or expired, and `403` when a valid token lacks the scope or names another repo. A refused write changes nothing.

## Sub-features

- `auth-missing` answers `401` without a token, on REST and on Git (with `WWW-Authenticate: Basic realm="open.storage"`).
- `auth-malformed` answers `401` `invalid token` for a value that is not a JWT.
- `auth-wrong-key` answers `401` for a token signed by a key that the server does not hold.
- `auth-expired` answers `401` for a token past its `exp`.
- `auth-scope-write` refuses writes for a `git:read` token: commit-pack `403` (result envelope), repo create `403` `insufficient_scope`, and git push `403`.
- `auth-scope-read` refuses reads for a `git:write` token without `git:read`.
- `auth-scope-org` refuses `GET /api/v1/repos` for a repo token (`403` `missing required scope org:read`).
- `auth-repo-mismatch` refuses a token for repo `other` on `demo`: Git `403` `jwt repo claim mismatch`, path-style REST `404` `repository_not_found`.
- `auth-ok` accepts the right token for the same calls (control).

## How to get to it (user POV)

- REST: `Authorization: Bearer <jwt>` on every `/api/...` call.
- Git: the token is the Basic-auth password for user `t`, from a credential prompt or the remote URL.
- Tokens come from the organization's token service. In local mode that is `openstorage mint --key <org.pem> --iss <org> --repo <repo|org> --scopes <list>`.

## Driving it with osv.sh

Preconditions:

- The baseline from [`README.md`](README.md#baseline-preconditions) ran in this shell.
- doctor passes.

- **Mint the test tokens.** Mint a read-only token, a write-only token, a token for another repo, a token from a foreign key, a short-lived token, and a literal junk value.

```sh
"$OSV" token "$RUN" ro --repo demo --scopes git:read
"$OSV" token "$RUN" wo --repo demo --scopes git:write
"$OSV" token "$RUN" other-create --repo other --scopes repo:write
"$OSV" token "$RUN" other --repo other --scopes git:read,git:write
env -i PATH="$PATH" "$OSV_BIN" keygen --out "$OSV_WORK/keys/foreign" >/dev/null
"$OSV" token "$RUN" foreign --key "$OSV_WORK/keys/foreign.pem" --repo demo --scopes git:read,git:write
"$OSV" token "$RUN" short --repo demo --scopes git:read --ttl 2s
"$OSV" token "$RUN" junk --raw not-a-jwt
"$OSV" api "$RUN" auth-other-create --token other-create --expect 201 POST /api/v1/repos -d '{}'
```

- **Control.** Read the repo with `ro`. The status is `200`.

```sh
"$OSV" api "$RUN" auth-ok-get --token ro --expect 200 GET /api/v1/repo
```

- **Missing token.** Read without credentials on REST and on Git. Both answer `401`. The Git answer carries `WWW-Authenticate: Basic realm="open.storage"`.

```sh
"$OSV" api "$RUN" auth-missing-api --token none --expect 401 GET /api/v1/repo
"$OSV" eq "$RUN" auth-missing-api-msg 'missing or malformed authorization header' "$("$OSV" json "$OSV_EVIDENCE/auth-missing-api.body" error)"
"$OSV" api "$RUN" auth-missing-git --token none --expect 401 GET '/demo.git/info/refs?service=git-upload-pack'
"$OSV" eq "$RUN" auth-missing-git-challenge 'Basic realm="open.storage"' "$(sed -n 's/^Www-Authenticate: //p' "$OSV_EVIDENCE/auth-missing-git.headers" | tr -d '\r')"
```

- **Malformed, wrong-key, and expired tokens.** Read with `junk`, `foreign`, and (after 3 s) `short`. All answer `401` with `invalid token`. git clone with `foreign` fails with `Authentication failed` (exit `128`).

```sh
"$OSV" api "$RUN" auth-junk --token junk --expect 401 GET /api/v1/repo
"$OSV" api "$RUN" auth-foreign --token foreign --expect 401 GET /api/v1/repo
"$OSV" eq "$RUN" auth-foreign-msg 'invalid token' "$("$OSV" json "$OSV_EVIDENCE/auth-foreign.body" error)"
"$OSV" git "$RUN" auth-foreign-clone --token foreign --expect 128 -- clone -q "$OSV_URL/demo.git" foreign-clone
"$OSV" eq "$RUN" auth-foreign-clone-msg yes "$(grep -q 'Authentication failed' "$OSV_EVIDENCE/auth-foreign-clone.git.txt" && echo yes || echo no)"
sleep 3
"$OSV" api "$RUN" auth-expired --token short --expect 401 GET /api/v1/repo
```

- **Write with a read-only token.** Try commit-pack, repo create, and git push with `ro`. commit-pack answers `403` with `result.status` `forbidden`. Create answers `403` with `code` `insufficient_scope`. git push exits `128` with `The requested URL returned error: 403`. The `main` tip is still `SEED`.

```sh
cat >"$OSV_EVIDENCE/auth-ro.ndjson" <<'NDJSON'
{"metadata":{"target_branch":"main","commit_message":"Denied","author":{"name":"Demo","email":"demo@example.com"},"files":[{"path":"denied.txt","content_id":"d","operation":"upsert"}]}}
{"blob_chunk":{"content_id":"d","data":"ZGVuaWVkCg==","eof":true}}
NDJSON
"$OSV" api "$RUN" auth-ro-commit --token ro --expect 403 POST /api/v1/repos/commit-pack \
  -H 'Content-Type: application/x-ndjson' --data-binary "@$OSV_EVIDENCE/auth-ro.ndjson"
"$OSV" eq "$RUN" auth-ro-commit-status forbidden "$("$OSV" json "$OSV_EVIDENCE/auth-ro-commit.body" result.status)"
"$OSV" api "$RUN" auth-ro-create --token ro --expect 403 POST /api/v1/repos -d '{}'
"$OSV" eq "$RUN" auth-ro-create-code insufficient_scope "$("$OSV" json "$OSV_EVIDENCE/auth-ro-create.body" code)"
printf 'denied\n' >"$OSV_WORK/seed/denied.txt"
"$OSV" git "$RUN" auth-ro-add -- -C seed add denied.txt
"$OSV" git "$RUN" auth-ro-commit-local -- -C seed commit -q -m 'Denied push'
"$OSV" git "$RUN" auth-ro-push --token ro --expect 128 -- -C seed push "$OSV_URL/demo.git" HEAD:refs/heads/main
"$OSV" eq "$RUN" auth-ro-push-msg yes "$(grep -q 'returned error: 403' "$OSV_EVIDENCE/auth-ro-push.git.txt" && echo yes || echo no)"
"$OSV" api "$RUN" auth-ro-tip --token ro --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" auth-ro-tip-unchanged "$SEED" "$("$OSV" json "$OSV_EVIDENCE/auth-ro-tip.body" branch.head_sha)"
```

- **Read with a write-only token.** Read the repo and the Git refs with `wo`. Both answer `403`.

```sh
"$OSV" api "$RUN" auth-wo-get --token wo --expect 403 GET /api/v1/repo
"$OSV" api "$RUN" auth-wo-git --token wo --expect 403 GET '/demo.git/info/refs?service=git-upload-pack'
```

- **Org endpoint with a repo token.** List repos with `rw`. The status is `403` and `error` is `missing required scope org:read`.

```sh
"$OSV" api "$RUN" auth-org-scope --token rw --expect 403 GET /api/v1/repos
"$OSV" eq "$RUN" auth-org-scope-msg 'missing required scope org:read' "$("$OSV" json "$OSV_EVIDENCE/auth-org-scope.body" error)"
```

- **Token for another repo.** Use `other` against `demo` on Git and on the path-style REST alias. Git answers `403` with `jwt repo claim mismatch`, and the alias answers `404` `repository_not_found`. git clone exits `128`.

```sh
"$OSV" api "$RUN" auth-mismatch-git --token other --expect 403 GET '/demo.git/info/refs?service=git-upload-pack'
"$OSV" eq "$RUN" auth-mismatch-git-msg 'jwt repo claim mismatch' "$("$OSV" json "$OSV_EVIDENCE/auth-mismatch-git.body" error)"
"$OSV" api "$RUN" auth-mismatch-alias --token other --expect 404 GET /api/repos/demo/branches
"$OSV" eq "$RUN" auth-mismatch-alias-code repository_not_found "$("$OSV" json "$OSV_EVIDENCE/auth-mismatch-alias.body" code)"
"$OSV" git "$RUN" auth-mismatch-clone --token other --expect 128 -- clone -q "$OSV_URL/demo.git" mismatch-clone
```

- **Proof.** `log.tsv` has a `PASS` row for every call above, each refusal's `.body` holds the error, and `auth-ro-tip-unchanged.eq.txt` shows that the refused push and commit changed nothing.

## Gotchas

- `--token none` on `osv.sh git` never sends credentials: git stops at `could not read Username ... terminal prompts disabled` (exit `128`) and the server sees no authenticated request. Prove the server's `401` with `osv.sh api --token none` on `/demo.git/info/refs?service=git-upload-pack`.
- git reports a `401` as `Authentication failed` and a `403` as `The requested URL returned error: 403`. Both exit `128`.
- For `/api/v1/...` the repo comes from the token, so a token for another repo reads that repo, or gets `404` `repository_not_found` when it does not exist. A mismatch is only visible on Git URLs and `/api/repos/{repo_name}/...` aliases.
- Write endpoints that move refs (commit-pack, merge, branch and tag writes) answer auth failures in the `result` envelope. Read endpoints use `{"error": ..., "code": ...}`.
- The Git surface wants Basic auth with user `t`. `osv.sh api` sends Basic on every non-`/api` path. A Bearer header there answers `401` `basic auth with username 't' required`.
- `mint` refuses a zero or negative `--ttl`. Mint a short one and wait for it to expire.
- The REST `401` messages are generic (`invalid token`). The Git surface answers with more detail, for example `jwt must be a compact JWS (header.payload.signature)`. Branch on status codes, not messages.
