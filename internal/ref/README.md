# `internal/ref`

## Overview
The ref authority. `RefStore` holds a repo's refs and registered packs; refs change only through compare-and-swap (`CASRef`), and packs register only after their bytes are durable. Three backends implement it: Postgres (`PG`), SQLite (`SQLite`), and `S3WAL`, which keeps each base repo's whole state in one manifest object advanced by conditional PUT (see the `s3wal` child package). Around that core the package holds the pack-quota ledger (reservations, usage, retained bytes, adoption claims, restore covers), the LFS object registry and reclamation rows, repo purge phases, absence-cover rows, job scan state, and the `IdempotentCAS` decorator that recovers ambiguous CAS outcomes.

## Interface
- `ZeroSHA`, `ValidSHA`, `ErrNotFound`, `ErrRepoPurged`, `ErrRefNameNotUTF8` [ref.go:ZeroSHA] [purge.go:ErrRepoPurged]
- Types: `Pack`, `RefInfo`, `RefEntry`, `HydrateSnapshot` [ref.go:Pack]
- Contracts: `RefStore`, `Store` (adds `InitSchema`, `Close`), `HydrateSnapshotter`, `ScanStateStore`, `RestoreCoverStore` [ref.go:RefStore] [scanstate.go:ScanStateStore]
- `Open` — dispatch `postgres`/`postgresql`, `sqlite`, or `s3wal` URLs; never runs `InitSchema` [ref.go:Open]
- Backends: `PG`/`OpenPG`, `SQLite`/`OpenSQLite`, `S3WAL`/`NewS3WAL` (plus `S3WAL.ConditionalStore`) [ref.go:OpenPG] [sqlite.go:OpenSQLite] [s3walstore.go:NewS3WAL]
- Ref and pack methods: `ListRefs`, `ListRefsWithMeta`, `ListRefsAfter`, `GetRef`, `CASRef`, `CASRefGen`, `RegisterPacks`, `ReplacePacks`, `ListPacks`, `HasPack`, `PackRegisteredBytes`, `GetGeneration`, `HydrateSnapshot`, `DFConflicts`, `RefValues`, `CountRefs` [ref.go:CASRef] [dfconflicts.go:DFConflicts] [refvalues.go:RefValues]
- `WithSyncClaimFence` — attach a sync-claim holder fence to a CAS context [ref.go:WithSyncClaimFence]
- `WithCASTxHook` — attach a `CASTxHook` to a CAS context: on PG, `InTx` writes in the transaction of a swap that changed a row, and `Committed` runs once that transaction is known to have committed [ref.go:WithCASTxHook] [ref.go:CASTxHook]
- `IdempotentCAS` — decorator that re-reads the ref after an ambiguous or lost CAS [idempotent.go:IdempotentCAS]
- Quota ledger: `NewReservationOwner`, `ReserveNoFloorGen`, `ReserveDecision`, `ReservePackBytes`, `RetirePackAttempts`, `SweepExpiredPackReservations`, `IsRetainedPack`, `ReclaimRetainedPack`, `QuotaLedgerState`, `PackQuotaLedgerState`, `IncomingBytes`, `Admitted`, `AddSat` [ledger.go:ReservePackBytes]
- Adoption and covers: `AdoptionClaimSuffix`, `AdoptionClaimTTL`, `TryAdoptionClaim`, `RenewAdoptionClaim`, `ReleaseAdoptionClaim`, `RepairCoverSuffix`, `ReserveRestoreCover` [ledger.go:TryAdoptionClaim]
- LFS registry: `MaxLFSObjectsPerRepo`, `ErrLFSObjectCapExceeded`, `RegisterLFSObject`, `RegisterAdoptedLFSObject`, `ReconcileLegacyLFSObjects`, `GuardedLFSDelete`, `BumpLFSAdoptionGen`, `LFSAdoptionGen`, `RecordLFSMismatch`, `LFSMismatchFingerprint`, `ClearLFSMismatch`, `CountLFSObjects`, `HasLFSObject`, `TouchLFSObject`, `LFSObjectRegisteredAt` [ledger.go:RegisterLFSObject]
- LFS reclamation: `LFSObjectRow`, `LFSReposWithObjects`, `ListRepoLFSObjects`, `MarkLFSObjectsUnreferenced`, `ClearLFSObjectsUnreferenced`, `RetireUnreferencedLFSObject`, `SettleLFSRefunds` [reclaim.go:RetireUnreferencedLFSObject]
- Purge: `FenceRepoPurge`, `IsRepoPurged`, `DropRepoRefs`, `RetireRepoPacks`, `DropRepoLFSObjects`, `CountRepoRows`, `PurgeRepoLedger` [purge.go:FenceRepoPurge]
- Absence covers: `AbsenceCoverPrefix`, `AbsenceFoldedOwner`, `AbsenceCoverRow`, `AbsenceLFSRow`, `AbsencePackRow`, `ListFleetLFSObjects`, `ListFleetPacks`, `ListAbsenceCovers`, `AbsenceCoverHead`, `RenewAbsenceCovers`, `LapsedAbsenceCoverExists`, `CASAbsenceCoverOwner`, `DeleteAbsenceCover` [absence.go:ListAbsenceCovers]
- Scan state: `JobScanState`, `ReapWatermark`, `DecodeReapWatermark`, `ErrReapWatermarkStale`, `ErrLegacyReapUnarmed`, `LFSRefundBucket`, `SettledLFSRefunds`, `ScanJobLFSReclaim`, `ScanJobAbsence`, `ScanJobSweep`, `ScanScope*` builders, plus `GetJobScanState`, `PutJobScanState`, `PutJobScanStateIf`, `ErrScanStateConflict`, `DeleteJobScanState`, `ParkRepoJobScanState`, `ListJobScanStateScopes`, `JobScanStateAge`, `BeginLFSReapEpoch`, `RecordLFSReapComplete`, `ArmAbsenceAdmissionFence` [scanstate.go:JobScanState]
- S3WAL sync-claim fence: `S3WAL.SetSyncClaim`, `S3WAL.ClearSyncClaim` [s3walstore_syncclaim.go:SetSyncClaim]
- SQL tables `refs`, `packs`, `repo_generations`, `pack_reservations`, `pack_retained`, `pack_usage`, `lfs_objects`, `lfs_object_counts`, `lfs_adoption_gens`, `lfs_mismatches`, `purged_repos`, `job_scan_state` [ref.go:initSchemaTx] [sqlite.go:InitSchema]
- Child package: [`s3wal`](./s3wal/README.md) — conditional-object CAS client, instance lease, v1 manifest schema, `MemCAS` fake, and boot probe used by the `S3WAL` backend

## Invariants
- `assertf` panics with `ref assertion failed` on bad arguments [ref.go:assertf]
- `casRef` asserts valid SHAs, rejects zero-to-zero, returns `ErrRepoPurged` for a fenced repo before the swap, and reports won only when one row changed [ref.go:casRef] [sqlite.go:casRef]
- Fenced CAS requires the upstream's `sync_claim_holder` to match the fence holder [sqlite.go:casRef]
- `RegisterPacks` asserts non-empty packs and fails for purge-fenced repos; `ReplacePacks` returns won=false when the old set is not fully present [ref.go:RegisterPacks] [ref.go:ReplacePacks]
- Stored SHAs are asserted valid on every read; `ListRefsAfter` orders by byte order [ref.go:ListRefsAfter]
- `IdempotentCAS` never recovers a delete, a definitive error, or a clean create conflict; PG SQLSTATE 40003 and 08007 count as ambiguous [idempotent.go:reconcileLoss] [idempotent.go:ambiguousError]
- PG `casRef` resolves a failed COMMIT of a swap that changed a row with `txid_status`, waiting at most `commitStatusTimeout` (5 s): committed is a win, aborted is definitive, and an unknown outcome (the status query failed, or the transaction was still in progress) is ambiguous, whatever pgx's own `SafeToRetry` tag says [ref.go:casRef] [ref.go:commitStatus] [idempotent.go:commitUnknownError]
- `ReservePackBytes` denies with `FloorStale` on a floor-generation mismatch, `KeyCapExceeded` past 10,000 active rows, `RenewalDegraded` while the absence fence is armed, and on quota overflow [ledger.go:ReservePackBytes] [ledger_sqlite.go:ReservePackBytes]
- `addSat` saturates at MaxInt64, and `admitted` denies a saturated total [ledger.go:admitted]
- LFS registration rolls back above `MaxLFSObjectsPerRepo` and refuses purge-fenced repos [ledger.go:registerLFSObjectTx]
- `RetireUnreferencedLFSObject` refuses with `ErrReapWatermarkStale` or `ErrLegacyReapUnarmed`, and skips moved generations, live adoption claims, and rows inside grace [reclaim.go:RetireUnreferencedLFSObject] [s3walstore_purge.go:RetireUnreferencedLFSObject]
- Refunds settle only under a strictly newer sweep epoch [reclaim.go:SettleLFSRefunds]
- `DFConflicts` counts only strict prefix or descendant refs as conflicts [dfconflicts.go:dfConflictsFromSorted]
- S3WAL `mutate` maps exhausted contention to `ErrCASContention`, resolves ambiguous writes by writer nonce, and refuses growing manifests above the hard size cap [s3walstore.go:mutate] [s3walstore.go:checkManifestSize]
- S3WAL `InitSchema` refuses to serve without a fresh store Date witness after the probe; with a nil probe (every store built by `NewS3WAL`) it returns nil without checking [s3walstore.go:InitSchema] [s3walstore.go:NewS3WAL]
- S3WAL reservation expiry needs elapsed time plus a skew guard, and leans live when drift or Date checks fail [s3walstore_quota.go:s3walReservationLive]
- S3WAL single-writer scan rows treat a 412 lost race as dual-serve evidence; the shared fleet-global lapse marker treats it as contention and retries with jitter [s3walstore_scanstate.go:scanMutateWith] [s3walstore_scanstate.go:scanMutateShared] [s3walstore_quota.go:s3walRecordLapse]
- S3WAL scan rows stamped more than `s3walScanFutureTolerance` (2 s) ahead read as stale; a stamp inside that tolerance clamps to age 0 [s3walstore_scanstate.go:scanRowAge] [s3walstore_scanstate.go:s3walScanFutureTolerance]
- `GuardedLFSDelete` never reports a delete when its S3WAL lease was lost [s3walstore_lfs.go:GuardedLFSDelete]

## Side Effects
- `PG.InitSchema` runs DDL under advisory locks `0x5350494458`/`0x5350494B45`, installs generation triggers, and builds indexes concurrently [ref.go:InitSchema]
- `SQLite.InitSchema` creates tables and generation triggers; writes use `BEGIN IMMEDIATE` [sqlite.go:InitSchema]
- Ledger and purge paths take the per-base advisory lock and write the ledger tables listed above [ledger.go:ReservePackBytes] [purge.go:FenceRepoPurge]
- `GuardedLFSDelete` invokes the caller's delete callback inside its guard [ledger.go:GuardedLFSDelete]
- `IdempotentCAS` issues a 5 s point read on a detached context [idempotent.go:reconcileLoss]
- PG `casRef` reads `txid_current()` in each swap that changed a row, and after a failed COMMIT of such a swap polls `txid_status` on a detached context for up to 5 s [ref.go:commitStatus]
- S3WAL reads and writes manifests under `_wal/repo/` and scan rows under `_wal/scan/` through conditional GET and PUT, with per-base in-process locks and jittered retries [s3walstore.go:mutate] [s3walstore_scanstate.go:s3walScanPrefix]
- S3WAL fleet walks list manifests 512 keys per page [s3walstore_quota.go:s3walEachBase]
- S3WAL increments `metrics.S3WALManifestSizeGuard` and `metrics.S3WALContentionExhausted`, and logs size and skew warnings [s3walstore.go:countContention]
- `NewReservationOwner` reads `crypto/rand` [ledger.go:NewReservationOwner]

## Verification
- Run: `go test ./internal/ref/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
- Needs Docker: the backend fan-out's `/pg` subtests start Postgres and its `/s3wal` subtests start SeaweedFS through `s3waltest`
