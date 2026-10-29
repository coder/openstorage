# `internal/sync`

## Overview
Upstream git mirror sync (pull) and outbound push-forward. The package defines the consumer-side contracts (credential, upstream, event, queue stores), the `Worker` that claims a repo's lane, moves refs with `git` through a temporary bare mirror and the loopback smart-HTTP endpoint, and emits lifecycle events. It also holds the loopback egress proxy that guards git transfers, an in-memory store, adapters over `meta.EventingStore`, an in-process queue, and the durable `LocalScheduler` for SQLite and S3WAL deployments.

## Interface
- Data types: `Credential`, `Upstream` (`CloneURL`), `Job` [sync.go:Credential]; `RejectedRef`, `EventRepository`, `Event` [events.go:Event]
- `ProviderHost`, `NewCredentialID` (`ggc_` + 16 hex) [sync.go:ProviderHost]
- `PushPendingNone`, `PushPendingPush`, `PushPendingForce` — outbound pending levels 0, 1, 2 [sync.go:PushPendingNone]
- Errors: `ErrNotFound`, `ErrCredentialExists`, `ErrUpstreamExists`, `ErrSyncInProgress`, `ErrPushInProgress`, `ErrClaimLost`, `ErrShutdown` [sync.go:ErrNotFound] [local_schedule.go:ErrShutdown]
- Contracts: `CredentialStore`, `UpstreamStore`, `EventSink`, `Queue`, `DefaultBranchAdopter`, `FirstSyncScheduler`, `ShutdownRestorer`, `LocalLanes` [sync.go:Queue] [default_branch.go:DefaultBranchAdopter] [first_sync.go:FirstSyncScheduler] [metastore.go:ShutdownRestorer] [local_schedule.go:LocalLanes]
- `ValidBranchName` — acceptable default branch name check [default_branch.go:ValidBranchName]
- Event names `EventSyncStarted`, `EventSyncSucceeded`, `EventSyncFailed` (`repo.sync.*`), `EventPushUpstreamStarted`, `EventPushUpstreamSucceeded`, `EventPushUpstreamFailed` (`repo.push_upstream.*`), `DirectionPush` [events.go:EventSyncStarted]
- Failure codes `FailAuth`, `FailClone`, `FailPush`, `FailConfig`, `FailUnreachable`, `FailInternal`, `FailSession`, `FailGeneric`, `FailForward`, `FailRejected` [events.go:FailAuth]
- `Worker` (fields `Upstreams`, `Credentials`, `Repos`, `Events`, `Loopback`, `Logger`, `Timeout`, `AllowProtocol`, hooks `Requeue`, `RequeuePush`, `AfterForward`, `Local`, `WaitPublication`, `DetachPoll`, egress flags), `RepoInfo`, `RepoLookup`, `Loopback` [worker.go:Worker]
- `(*Worker).Run` — one inbound sync run end to end [worker.go:Run]
- `(*Worker).RunPush`, `MaxPushTransientRetries` (5), `PushRetryDelay`, `TransientPushFailure` — one outbound push run and its retry policy [worker_push.go:RunPush]
- `MemStore`, `NewMemStore`, `LegacyClaimHolder` — in-memory credential and upstream store [memstore.go:MemStore]
- `MetaStores` — adapters from `meta.EventingStore` to `CredentialStore` and `UpstreamStore` [metastore.go:MetaStores]
- `InProcessQueue`, `NewInProcessQueue` (`EnqueueSync`, `EnqueuePush`, `Close`) — goroutine queue with per-repo coalescing [queue.go:InProcessQueue]
- `LocalScheduler`, `NewLocalScheduler`, `MetaLocalLanes`, `LaneClaim` — durable local schedule: `EnqueueSync`, `OfferSync`, `EnqueuePush`, `SweepOnce`, `Start`, `Stop`, `Drain`, `Close`, `CompleteSyncCreate`, `TracksCreateState` [local_schedule.go:NewLocalScheduler] [first_sync.go:LocalScheduler.CompleteSyncCreate]
- Env `OPENSTORAGE_SYNC_STEAL_LEGACY` (legacy claim steal opt-in), `OPENSTORAGE_ASKPASS_USERNAME`, `OPENSTORAGE_ASKPASS_PASSWORD` (credential pair passed to the git credential helper) [memstore.go:BeginSync] [worker.go:authGitEnv]

## Invariants
- `assertf` panics on programming errors only, never on untrusted input [sync.go:assertf]
- `ValidBranchName` rejects empty names, names over 255 bytes, `..`, bad components, and `.lock` or `.` suffixes [default_branch.go:ValidBranchName]
- `Worker.assertReady` panics when a required dependency is unset; `Run` and `RunPush` assert non-empty org and repo ID [worker.go:assertReady]
- `Run` returns nil without events when the repo no longer exists [worker.go:Run]
- A transfer refuses to push when the holder changed (`errClaimStolen`), the row is gone (`errDetached`), or the claim lease is inside `claimFenceMargin` [worker.go:transfer]
- `hermeticGitEnv` strips inherited `GIT_*` variables and sets `GIT_ALLOW_PROTOCOL` [worker.go:hermeticGitEnv]; stored credentials containing CR, LF, or NUL fail with `FailConfig` [worker.go:Worker.upstreamAuth]
- Only `FailUnreachable` push failures retry, at most `MaxPushTransientRetries` times; pushes use `--atomic` [worker_push.go:RunPush]
- `proxyEnv` removes inherited proxy variables in any case; the egress proxy maps policy denials to 403 and dial failures to 502 [egress.go:proxyEnv]
- `MemStore.BeginSync` returns `ErrSyncInProgress` while an unexpired claim exists; legacy claims get a grace period; `RequestPush` never downgrades force; a stolen push claim inherits force [memstore.go:BeginPush]
- Finish calls return `ErrClaimLost` for another holder and `ErrNotFound` when no run is in flight [memstore.go:FinishSync]
- `parsePushRejections` caps output at 100 refs and 200-byte reasons [pushstatus.go:parsePushRejections]
- `InProcessQueue.enqueue` rejects jobs after `Close` and coalesces a pending job in the same lane and repo [queue.go:enqueue]
- `LocalScheduler.offer` drops offers while draining or while the lane key is in flight; sweep pull offers wait while a push runs for the same repo, and sweep offers are dropped when all `localSweepSlots` (4) are in use; explicit `EnqueueSync`, `OfferSync`, and `EnqueuePush` offers take no slot, and an explicit pull offer starts even while a push runs [local_schedule.go:LocalScheduler.offer] [local_schedule.go:localSweepSlots]
- `Event.marshal` asserts event shape rules before encoding [events.go:Event.marshal]

## Side Effects
- Creates and removes a temp dir with a bare `mirror.git`; runs `git init`, `fetch`, `for-each-ref`, `push`, `ls-remote --symref`, and `rev-parse` with process groups [worker.go:runGitTo] [default_branch.go:upstreamHead]
- Starts a loopback egress proxy on `127.0.0.1:0` per transfer, with CONNECT hijacking and reverse proxying [egress.go:startEgressProxy]
- Reads and writes upstream claims, credentials, and outcomes through the injected stores; mints loopback tokens [worker.go:Run]
- Emits sync and push lifecycle events on detached contexts [worker.go:emitDetached]
- Arms `time.AfterFunc` retry timers under `deferMu`; the push watchdog polls `GetUpstream` and cancels a detached push [worker_push.go:transferPush]
- `LocalScheduler` writes durable pull requests (`RequestSync`) and create completion (`CompleteSyncRepoCreate`), sweeps due lanes on a timer, and runs work on goroutines [local_schedule.go:LocalScheduler.EnqueueSync] [first_sync.go:LocalScheduler.CompleteSyncCreate] [local_schedule.go:LocalScheduler.SweepOnce]; the `Worker` writes run outcomes through `Local.FinishSyncOutcome` and `Local.FinishPushOutcome` [worker.go:Run] [worker_push.go:RunPush]
- `setProcGroup` kills the whole git process group with SIGKILL on cancel (unix) [procgroup_unix.go:setProcGroup]
- Logs warnings and info through the injected logger [worker.go:Run]

## Verification
- Run: `go test ./internal/sync/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
- Run (s3wal lane, needs Docker for the SeaweedFS bucket): `OPENSTORAGE_TEST_BACKEND=s3wal go test ./internal/sync -tags=openstorage_viewcheck -run 'TestLocalSchedule' -count=1 -race -timeout=10m`
