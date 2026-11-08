# `internal/githttp`

## Overview
The git smart-HTTP and Git LFS server. `Server` authenticates JWTs, resolves repos, hydrates a bare cache repo from the ref store and blob store, and runs `git upload-pack` and `git receive-pack`. The same binary runs as the git hook (`RunHook`) that enforces ref policy, quota, push holds, signature verification, and the authoritative ref CAS during a push. The package also serves the LFS batch and object APIs with verified uploads, quota accounting, and legacy-key adoption.

## Interface
- `Server` (fields `Store`, `Blob`, `CacheDir`, `Verifier`, `Org`, `TestMode`, `ResolveRepo`, `MaybeRepack`, `PackQuotaBytes`, `PackReservationTTL`, `BaseURL`, `MaxPushBytes`, `VerifySigBudget`, `LFSStagingBytes`, `LFSV020WriteCompat`), `NewServer`, `(*Server).Handler` [server.go:Server]
- Routes: `GET /{repo}.git/info/refs`, `POST /{repo}.git/git-upload-pack`, `POST /{repo}.git/git-receive-pack`, prefix `/{repo}.git/info/lfs/` [server.go:Handler]
- LFS routes: `POST /{repo}.git/info/lfs/objects/batch`, `GET` and `PUT /{repo}.git/info/lfs/objects/{oid}` [lfs.go:handleLFS]
- Cache repos: `ValidRepoName`, `BaseRepo`, `RepoPath`, `EnsureCacheRepo`, `EnsureCacheRepoCtx`, `ValidateCacheDir`, `HeadTarget` [hydrate.go:EnsureCacheRepoCtx]
- Hydration: `Hydrate`, `HydratePacks`, `MaterializePack`, `ErrPackMissing` [hydrate.go:Hydrate]
- Hooks: `RunHook` (`pre-receive`, `update`, `post-receive`, `reference-transaction`), `WrapHookRefStore` [hook.go:RunHook]
- Push publication: `WaitForPushPublication`, `ErrPushPublicationUnverified` [publication.go:WaitForPushPublication]
- Quota: `EnvPackQuota`, `EnvPackReservationTTL`, `EnvUsageFloor`, `MinPackReservationTTL`, `UsageFloorHandoffMargin`, `CachedUsageFloor`, `CachedUsageFloorGen` [quota.go:CachedUsageFloor]
- `SetRateLimited` — `Retry-After` and `RateLimit` headers for 429s [ratelimited.go:SetRateLimited]
- Hook env names: `EnvRepo`, `EnvSyncClaim`, `EnvRefsPolicy`, `EnvOps`, `EnvScopes`, `EnvOrg`, `EnvPushHold`, `EnvAtomic` (`OPENSTORAGE_GIT_*`, `OPENSTORAGE_ORG`, `OPENSTORAGE_SYNC_CLAIM`), `EnvVerifyState`, `EnvVerifySigBudget`, `EnvVerifySpawnLog` [hook.go:EnvRepo] [verifystate.go:EnvVerifyState]
- Test-mode names: `EnvTestMode`, `EnvCrash`, `EnvHookDelay`, `EnvPushDrainMS`, `EnvCASDrop`, `EnvCASDropHard`, `EnvUpdateBarrier`, `EnvUpdateBarrierEach`, `EnvPreReceiveBarrier`, header `CrashHeader` (`X-Openstorage-Test-Crash`) [hook.go:EnvTestMode]
- Hook store URLs: `OPENSTORAGE_PG_URL`/`PG_URL`, `OPENSTORAGE_S3_URL`/`BLOB_URL` [hook.go:envHookPGURL]

## Invariants
- `assertf` panics on programming errors; request-dependent failures map to 4xx [server.go:assertf]
- Git auth requires basic auth with username `t`; a bad token is 401; a wrong org, an org-level or mismatched repo claim, or a missing scope is 403 [server.go:verifyClaims]
- Auth runs before repo resolution, so an unknown repo answers 404 only after auth [server.go:handleServiceRPC]
- `CrashHeader` is refused with 403 unless `TestMode` [server.go:handleServiceRPC]
- Receive-pack bodies over the push cap and gzip upload-pack bodies over 4 MiB answer 413 [server.go:handleServiceRPC] [uploadcap.go:maxUploadPackBytes]
- `sniffAtomicPush` fails closed as atomic when the peek budget is exhausted [atomic.go:sniffAtomicPush]
- `preReceive` rejects malformed stdin lines and zero commands, and releases all push holds on error [hook.go:preReceive]
- Atomic pushes are denied whole, before any hold is reserved, on a policy denial or a D/F conflict inside the pushed commands [hook.go:preReceive] [hook.go:atomicPushDFCheck]; after the holds are reserved and before any update-hook CAS, a stale old SHA or a D/F conflict with refs already in the store also denies an atomic push whole [hook.go:preReceive] [hook.go:authorityDoomedCreations]
- `preReceivePolicyGate` re-checks the policy verdicts and refuses pack publication that only denied updates need [hook.go:preReceivePolicyGate]
- `pushRefUpdatable` rejects names deeper than 32, with a segment over 200 bytes, or over 900 bytes [hook.go:pushRefUpdatable]
- `updateHook` checks policy, reservation, and the sync-claim fence before the CAS; unknown policy ops fail closed [hook.go:updateHook] [hook.go:evalRefPolicy]
- Hold reservation fails closed on unreadable peer holds and waits at most 10 s for conflicting holds [hydrate.go:reservePushHoldsCovering]
- `Hydrate` publishes the generation marker only after a full sync and re-reads the generation under the hydrate lock [hydrate.go:Hydrate]
- `hydratePacksBy` retries a missing pack blob a bounded number of times, then returns `ErrPackMissing` [hydrate.go:hydratePacksBy]
- A failed `update-ref` batch returns a `refBatchError` that names the batch's refs. `syncRefsWithCleanup` runs `cleanStaleRefLocks` after a failed sync has returned and its git children are reaped, after each of its three attempts, so one hydration cleans up to three failed batches, and nothing remembers a batch or earns an attempt [hydrate.go:syncRefsWithCleanup] [hydrate.go:refBatchError]
- `cleanStaleRefLocks` removes a ref lock only when it holds the repository's lease exclusively, and only a regular file older than one minute and newer than the lease epoch: it renames the lock into `codestorage-stale-locks/` and deletes it only when the moved file has the device, inode, modification time and size that it inspected [staleref.go:cleanStaleRefLocks] [staleref.go:quarantineRefLock]
- One cleanup run writes one log line and counts one `metrics.StaleRefLockCleanup` outcome, and a run that finds no lock older than one minute does neither and takes no lease [staleref.go:cleanStaleRefLocks]
- Cache repos pin `receive.unpackLimit=0` and `gc.auto=0`; a pinned value that survives repair fails the call [hydrate.go:EnsureCacheRepoCtx]
- `EnsureCacheRepoCtx` writes the lease epoch file for a directory that has no HEAD (a new one, or one that an interrupted creation left), before git init, and never in a repository that has HEAD. A repository without the file keeps every stale ref lock [hydrate.go:EnsureCacheRepoCtx] [staleref_epoch_test.go:TestEnsureCacheRepoWritesTheLeaseEpochOnlyBeforeHEADExists] [staleref_epoch_test.go:TestEnsureCacheRepoWritesTheEpochForADirectoryWithoutHEAD] [staleref.go:cleanStaleRefLocks]
- LFS OIDs must match `^[a-f0-9]{64}$`; batches are capped at 4 MiB and 1000 objects; only `basic` transfer and `sha256` [lfs.go:handleLFSBatch]
- LFS PUT needs a Content-Length (411), must hash to the OID (400), answers 413 when its Content-Length exceeds the per-repo staging allowance and 429 when the staging budget is exhausted, and is limited by `ref.MaxLFSObjectsPerRepo` (507) [lfs.go:lfsProxyUpload]
- Registered LFS objects are served only from the canonical key [lfs.go:lfsBatchDownload]
- `chargeWalk` fails closed through `exhaust` past the verify walk budget [verifystate.go:chargeWalk]
- `recordCommitGraphOutcome` asserts a known outcome [commitgraph.go:recordCommitGraphOutcome]

## Side Effects
- Runs `git upload-pack`, `receive-pack`, `init --bare`, `config`, `index-pack`, `update-ref --stdin`, `for-each-ref`, `symbolic-ref`, `verify-commit`, `rev-list`, `cat-file`, `merge-base`, and `commit-graph write` subprocesses [server.go:handleServiceRPC] [hydrate.go:runGitCtx]
- Creates cache repos with hook scripts that re-exec this binary [hydrate.go:hookScript]
- Downloads pack blobs into `objects/pack`; uploads packs and LFS objects; deletes and copies LFS blobs [hydrate.go:hydrateOnePack] [lfs.go:lfsProxyUpload]
- Writes push-hold files and sidecars under `codestorage.push-holds`, and marker files `codestorage.generation` and `codestorage.generation.held`; takes flocks on lock files [hydrate.go:writeGenerationMarker]
- Starts push-hold refresher, adoption-claim renewal, and repack-probe goroutines [hydrate.go:startPushHoldRefresh] [server.go:dispatchRepackProbe]
- Reserves quota, registers packs and LFS objects, and CASes refs in the ref store [hook.go:preReceive] [hook.go:updateHook]
- Spills large receive-pack output to an unlinked temp file [pushcap.go:pushGate.Write]
- Writes verify-sig state files and imports signing keys with `gpg --import` in a temp dir [verifystate.go:appendTo] [hook.go:newSigVerifier]
- Increments `metrics.CommitGraphRefresh`; logs through `log.Printf` [commitgraph.go:recordCommitGraphOutcome]
- Takes the repository's lease exclusively for the moments of a stale ref lock cleanup, which makes new git starts in that repository wait; moves and deletes ref locks under `codestorage-stale-locks/`; increments `metrics.StaleRefLockCleanup` [staleref.go:cleanStaleRefLocks]
- Test-mode crash knobs call `os.Exit(42)` [hook.go:preReceive]

## Verification
- Run: `go test ./internal/githttp/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (default `pg` lane; needs Docker for Postgres 17 + SeaweedFS through testcontainers)
- Local lane (no Docker): `OPENSTORAGE_TEST_BACKEND=local go test ./internal/githttp -count=1 -race -timeout=15m`
- s3wal lane (needs Docker): `OPENSTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp -tags=openstorage_viewcheck -count=1 -race -timeout=25m`
- Floci nightly lane (needs Docker, a Floci container, and STS session credentials; see `docs/reference/testing.md`): `AWS_REGION=us-east-1 OPENSTORAGE_TEST_S3_ENDPOINT=http://127.0.0.1:4566 go test ./internal/githttp -count=1 -race -timeout=25m`
- Fuzz nightly lane, one target per run (`FuzzSniffAtomicPush`, `FuzzLFSBatch`, `FuzzPushPublicationScan`): `go test ./internal/githttp -run '^$' -fuzz '^FuzzLFSBatch$' -fuzztime 4m -parallel 8 -timeout 30m`
- Benchmark nightly (`.github/workflows/bench-nightly.yml`, defaults `BENCHTIME=1s`, `COUNT=1`): the whole-repo sweep `OPENSTORAGE_TEST_BACKEND=local go test -p 1 ./... -run '^$' -bench . -benchmem -benchtime 1s -count 1 -timeout 50m`, then the push-path bench on the s3wal lane (needs Docker) `OPENSTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp/ -run '^$' -bench 'BenchmarkPushE2E' -benchtime 1x -count 1 -timeout 50m`
