# `internal/jobs`

## Overview
Background jobs. The package defines the `Queue` contract and two implementations: `River` (durable Postgres queue with periodic jobs) and `Local` (in-process goroutines for SQLite mode). The jobs themselves are the orphan-pack sweeper with its quarantine handshake, repository purge, pack repack folds, unreferenced-LFS reclamation, and the absence reconciler that renews restore covers for missing blob bytes.

## Interface
- `Queue`, `SyncRunner`, `MaxDeliveryAttempts` (10) [jobs.go:Queue]
- `River`, `NewRiver`, `NewRiverInsertOnly` — durable Postgres queue; methods `EnqueueWebhookDelivery`, `EnqueueSync`, `EnqueuePush`, `EnqueueRepack`, `CompleteSyncCreate`, `TracksCreateState`, `DrainSyncRuns`, `Close` [river.go:NewRiver]
- River job kinds `webhook_delivery`, `upstream_sync`, `upstream_push`, `orphan_pack_sweep`, `lfs_reclaim`, `absence_reconcile`, `repack` [river.go:deliveryArgs.Kind]
- River queues: `default` plus one single-worker queue each for `orphan_pack_sweep`, `lfs_reclaim` and `absence_reconcile`, registered only when the replica's schedule for that job is on [river.go:NewRiver]
- `Local`, `NewLocal`, `NewLocalScheduled` — in-process queue with the same enqueue methods plus `OfferSync`, `DrainSyncRuns`, `Close` [local.go:Local]
- `(*Local).TracksCreateState`, `(*Local).CompleteSyncCreate` — true and delegated only when the sync queue is a `sync.FirstSyncScheduler` (the local schedule); `CompleteSyncCreate` asserts that [local.go:TracksCreateState] [local.go:CompleteSyncCreate]
- Sweeper: `Sweeper`, `NewSweeper`, `Sweeper.Sweep`, `Sweeper.Heal`, `PackRegistry`, `SweepConfig`, `SweepReport`, `OrphanPack`, `QuarantinePurgeFloor`, `SweepSchedule`, `StartSweepTicker`, `ReconcileTickInterval`, `StartReconcileTicker` [sweep.go:Sweeper]
- Purge: `RepoPurger`, `NewRepoPurger`, `PurgeExpired`, `PurgeRepo`, `RepoPurgeRegistry`, `RepoPurgeMeta`, `MinRepoPurgeAge` (1 h), `ErrRepoRecentlyDeleted`, `ErrPurgeDeferred`, `RepoPurgeReport`, `PurgeRunReport` [purge.go:RepoPurger]
- Repack: `Repacker`, `NewRepacker`, `Repack`, `FollowUpDue`, `MaterializeFunc`, `RepackTrigger`, `NewRepackTrigger`, `MaybeEnqueue` [repack.go:Repacker]
- LFS reclaim: `LFSReclaimer`, `NewLFSReclaimer`, `Run`, `LFSReclaimRegistry`, `HydrateFunc`, `LFSWalkFunc`, `DefaultBranchFunc`, `LFSReclaimReport`, `DefaultLFSReclaimObjectBudget`, `DefaultLFSReclaimGrace` (24 h), `LFSReclaimSchedule`, `StartLFSReclaimTicker` [lfsreclaim.go:LFSReclaimer]
- Absence reconcile: `AbsenceReconciler`, `NewAbsenceReconciler`, `Run`, `AbsenceRegistry`, `BlobStatter`, `AbsenceReport`, `DefaultAbsenceReconcileBudget`, `MinAbsenceReconcileBudget`, `AbsenceReconcileSchedule`, `StartAbsenceReconcileTicker` [absence.go:NewAbsenceReconciler]
- Env names in operator warnings: `OPENSTORAGE_ABSENCE_RECONCILE_BUDGET` [absence.go:renewCovers], `OPENSTORAGE_PACK_RESERVATION_TTL` [absence.go:warnVerifyDeadline], `OPENSTORAGE_LFS_GC_OBJECT_BUDGET` [lfsreclaim.go:LFSReclaimer.Run]

## Invariants
- `assertf` panics on programming errors only [jobs.go:assertf]
- After `Close`, `Local` enqueue methods return "jobs queue is closed"; at most one repack per repo runs, with at most 4 folds in parallel [local.go:EnqueueRepack]
- `EnqueueRepack` fails when repack is not configured; `EnqueueWebhookDelivery` asserts a JSON payload [river.go:EnqueueRepack]
- River sync and push workers snooze instead of completing when a completion step fails, and complete jobs in a transaction that inserts gated successors [river.go:syncWorker.Work]
- `deliveryWorker` cancels jobs whose webhook is gone [river.go:deliveryWorker.Work]
- A replica with the sweep or LFS reclaim schedule off never fetches that kind's queue; the worker stays registered for crash rescue [river.go:NewRiver]
- `LFSReclaimer.Run` admits one run at a time per reclaimer, because its advisory verdicts belong to the pass; a second run during a rolling upgrade does nothing [lfsreclaim.go:LFSReclaimer.Run]
- `assertNotWAL` panics on any key under `_wal/` before the sweeper acts on it [sweep.go:assertNotWAL]
- Registered, fresh (younger than `Threshold`), or zero-ModTime blobs are never sweep candidates; deletion needs a passing revalidation [sweep.go:classify]
- A `DeleteGuard` error downgrades a sweep to list-only; an LFS quarantine backup that fails its OID hash check is never restored [sweep.go:sweep] [sweep.go:restoreFromQuarantine]
- Purge takes base repo IDs only, refuses live repos or repos deleted less than `MinRepoPurgeAge` ago, fences before dropping rows, and returns `ErrPurgeDeferred` while rows or spared blobs remain [purge.go:purgeRepo]
- Repack needs at least two packs, refuses the swap with less than 30 s left, and never deletes a lost fold synchronously [repack.go:repack]
- LFS reclaim touches no row after a walk error and retires a row only past `grace` and unreferenced [lfsreclaim.go:reclaimRepo]
- The absence reconciler never shrinks or releases a cover on a dirty, tainted, or purged range, and deletes a cover only after the generation bump succeeds [absence.go:endRange]

## Side Effects
- River writes job rows, runs migrations under `pg_advisory_lock(0x5249564552)`, and registers periodic sweep, reclaim, and absence jobs [river.go:NewRiver]
- `Local` runs deliveries, sync, push, and repack on goroutines [local.go:EnqueueWebhookDelivery]
- Sweeper lists `packs/` and `lfs/` blobs, quarantines under `sweep/quarantine/`, deletes, restores, and writes the LFS reap watermark [sweep.go:sweep]
- Purge drops registry rows, reaps blob prefixes, and stamps the repo row purged [purge.go:purgeRepo]
- Repack runs `git pack-objects` and `git show-index` under the repository's `gitlimit` lease, uploads the fold, and calls `ReplacePacks` [repack.go:runGitStdin]
- LFS reclaim hydrates repos, walks reachable OIDs, marks, clears, and retires rows, and settles refunds [lfsreclaim.go:settleRepoRows]
- The absence reconciler stats blobs, renews and rotates cover reservations, and writes scan-state cursors and the admission fence [absence.go:renewCovers]
- Records `metrics.JobRun` for sweep and repack; logs through slog; tickers run on goroutines [sweep.go:Sweep]

## Verification
- Run: `go test ./internal/jobs/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (needs Docker: the tests boot Postgres through testcontainers)
