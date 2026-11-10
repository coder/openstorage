# `internal/api`

## Overview
The REST API (`/api/v1/...`) and its `/api/repos/{repo_name}` alias layer. `New` validates a `Config`, registers every route on one `ServeMux`, and wraps it in panic recovery, request logging, and RFC 9457 problem-instance middleware. Handlers authenticate bearer JWTs, apply per-client-IP rate limits and git-read budgets, and call the meta store, ref store, blob store, `gitread`, and the sync queue. Write handlers build commits in a per-request scratch git directory and publish them as packs followed by a ref CAS.

## Interface
- `Config`, `New` — wiring struct and handler constructor [api.go:New]
- `RateLimitConfig`, `NewGitRateLimit` — per-client-IP token buckets for REST and the git surface [ratelimit.go:NewGitRateLimit]
- `RegisterWebhookRoutes` — `POST`/`GET /api/v1/webhooks`, `DELETE /api/v1/webhooks/{id}` [handlers_webhooks.go:RegisterWebhookRoutes]
- Org routes: `GET /api/meta`, `GET /api/v1/repos`, `GET /api/repo-urls/{repo_id}` [api.go:New] [repourls.go:registerRepoURLRoutes]
- Repo routes: `POST /api/v1/repos` (create, fork, copy, or upstream link), `GET`/`PATCH /api/v1/repo`, `DELETE /api/v1/repos/delete` [api.go:New] [handlers_fork.go:forkAwareCreateRepo]
- Read routes: `GET /api/v1/repos/file`, `/files`, `/files/metadata`, `/branches`, `/branch`, `/branches/diff`, `/commits`, `/commit`, `/diff`, `/tags`, `/tag`, `/blame`, `/notes`, `/notes/refs`; `POST /api/v1/repos/grep`, `/archive` [api.go:New]
- Write routes: `POST /api/v1/repos/commit-pack`, `/diff-commit`, `/restore-commit`, `/merge`, `GET /api/v1/repos/merge/preview` [handlers_write_commit.go:registerWriteCommitRoutes] [handlers_write_merge.go:registerWriteMergeRoutes]
- Ref routes: `POST /api/v1/repos/branches/create`, `DELETE /api/v1/repos/branches`, `POST`/`DELETE /api/v1/repos/tags`, `POST`/`DELETE /api/v1/repos/notes` [handlers_write_refs.go:registerWriteRefRoutes]
- Credential routes: `POST`/`PUT`/`DELETE /api/v1/repos/git-credentials`, `PUT`/`DELETE /api/repos/{repo_name}/git-credentials/{git_credential_id}` [handlers_credentials.go:registerCredentialRoutes] [alias.go:registerAliasRoutes]
- Sync routes: `POST /api/v1/repos/pull-upstream`, `POST /api/v1/repos/push-upstream`, `DELETE /api/v1/repos/base` [handlers_sync.go:registerSyncRoutes]
- Alias table: `aliasRoutes` maps each `/api/repos/{repo_name}` path to its v1 route [alias.go:aliasRoutes]

## Invariants
- `New` panics when a required dependency is missing and returns an error for a bad cache dir or rate-limit config [api.go:New]
- `requireAuth` returns 401 for a missing or non-Bearer `Authorization` header, 403 `forbidden` for a token that fails verification, 403 `authentication_unavailable` when the key store fails, and 403 for a wrong org, scope, or repo claim [middleware.go:requireAuth]
- `forkAwareCreateRepo` answers 400 before any create form runs when the `repo` claim is not `meta.ValidText` [handlers_fork.go:forkAwareCreateRepo]
- `recoverPanics` re-panics `http.ErrAbortHandler` and otherwise answers 500 [middleware.go:recoverPanics]
- `newProblem` asserts a 4xx/5xx status and a valid code; `internalErr` never returns raw error text [errors.go:newProblem] [errors.go:internalErr]
- `writeGitLimitErr` maps `ErrBusy`/`ErrTimeBudget` to 503 and memory, output, or blob limits to 422 [git_budget.go:writeGitLimitErr]
- `aliasHandler` answers 404 when the path repo does not resolve or differs from the claim repo, and charges one rate-limit token. The org-level `PUT`/`DELETE /api/repos/{repo_name}/git-credentials` aliases skip the claim-repo check [alias.go:aliasHandler] [alias.go:aliasRoutes]
- `clientKey` trusts `X-Forwarded-For` only from trusted proxies; a limited bucket latches until the reserve refills [ratelimit.go:clientKey]
- `handleCreateRepo` rejects a body `repo_name` that differs from the JWT repo claim [handlers.go:handleCreateRepo]
- `validBranchName` rejects names over 255 bytes, `..`, `.lock` components, and characters outside a conservative set [handlers.go:validBranchName]
- Ref policies deny `no-push`, non-fast-forward under `no-force-push`, server-created commits under `verify-sig`, and unknown ops (fail closed) [handlers_write_refs.go:wrefPolicyCheck] [handlers_write_commit.go:wcCheckRefPolicy]
- Write-commit paths reject `.git` components, absolute or dot paths, and control bytes; a stale `expected_head_sha` is 412 [handlers_write_commit.go:wcValidPath] [handlers_write_commit.go:wcPlanCommit]
- Merges need git >= 2.38 and refuse unrelated histories unless allowed; a stale target SHA is 409 [handlers_write_merge.go:wmCheckGitVersion] [handlers_write_merge.go:handleMerge]
- Fork and copy check the source token for `git:read` and the pack size against quota before creating any row [handlers_fork.go:handleForkRepo]
- `publishPack` denies over-quota packs before upload and fails closed on a second stale usage floor [publish.go:publishPack]
- `validUpstreamHost` rejects userinfo, query, fragment, and non-http(s) schemes [handlers_sync.go:validUpstreamHost]
- Text the request asks to store answers 400 on every backend when it holds a NUL byte (a JSON body decodes invalid UTF-8 to U+FFFD, so only a NUL reaches the check): a webhook `secret`, a credential `username` and `password`, the `owner`, `name` and `auth.token` of a sync link, and a `pull-upstream` `ref`. A webhook or credential ID with a NUL byte or invalid UTF-8 answers the 404 of an unknown ID [errors.go:invalidTextFields] [handlers_webhooks.go:handleCreate] [handlers_credentials.go:handleCreateCredential] [handlers_sync.go:validateSyncBaseRepo] [handlers_sync.go:parseUpstreamConfig] [handlers_sync.go:handlePullUpstream]
- `handleDeleteBranch` refuses to delete the default branch in the persistent namespace [handlers_write_refs.go:handleDeleteBranch]

## Side Effects
- Reads and writes repos, credentials, upstreams, and webhooks through the meta store [handlers.go:handleCreateRepo] [handlers_webhooks.go:handleCreate]
- Materializes cache repos through `gitread.Materialize` and runs git reads under budgets that hold shared admission slots [handlers_read_core.go:materializeReadRepo] [git_budget.go:gitReadBudget]
- Runs scratch git subprocesses (`hash-object`, `update-index`, `write-tree`, `commit-tree`, `apply`, `merge-tree`, `pack-objects`, `check-ref-format`) that write new objects only to temp dirs [handlers_write_commit.go:wcNewScratch]
- Notes writes run `git update-ref`, `git notes`, and `git rev-parse` in the materialized cache repo: new objects go to a temp object dir, but a temporary `refs/notes/wref-tmp-*` ref is written into the cache repo and deleted with `update-ref -d` on cleanup [handlers_write_refs.go:wrefRunNotesOp] [handlers_write_refs.go:wrefGit]
- `publishPack` reserves quota, uploads and verifies packs through the blob store, registers them through the ref store, and then runs the repack-threshold trigger [publish.go:publishPack] [handlers.go:triggerRepack]
- Write and ref handlers move refs with `CASRef` on the ref store. Write-commit runs it after `publishPack` succeeds [handlers_write_commit.go:wcPublish] [handlers_write_refs.go:handleDeleteBranch]
- Enqueues sync and push jobs and schedules first syncs [handlers_sync.go:handlePullUpstream]
- Checks webhook and upstream targets through the egress guard [handlers_webhooks.go:handleCreate]
- Streams files through temp files and archives as gzip tar [handlers_read_core.go:handleGetFile] [handlers_read_query.go:handleArchive]
- Logs one line per request and logs internal errors and panics [middleware.go:logRequests]

## Verification
- Run: `go test ./internal/api/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (needs Docker: the `/pg` subtests boot a Postgres testcontainer)
- Nightly fuzz lane (`.github/workflows/fuzz-nightly.yml`), one target per run, for `FuzzClientKey` and `FuzzCoreCursor`: `go test ./internal/api -run '^$' -fuzz '^FuzzClientKey$' -fuzztime 4m -parallel 8 -timeout 30m`
