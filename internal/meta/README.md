# `internal/meta`

## Overview
The metadata store: orgs, public and signing keys, repos, webhooks, git credentials, and repo upstream rows with their sync and push claim lanes. Three backends implement one contract: Postgres (`PG`), SQLite (`SQLite`), and S3WAL (`S3WAL`, all metadata in one conditional JSON object `_wal/meta.json` advanced by compare-and-swap). Secret columns are stored as plaintext by default and as `internal/secrets` envelopes when `WithSecretsCodec` sets an encrypting codec [secrets.go:WithSecretsCodec] [secrets.go:encodeSecret].

## Interface
- Contracts: `Store`, `Backend` (adds `InitSchema`, `Close`), `EventingStore`, `RunRestoreStore`, `LocalScheduleStore`, `SyncClaimSink` [meta.go:Store] [eventing.go:EventingStore]
- `Open` — dispatch a DB URL by scheme to `OpenPG`, `OpenSQLite`, or `OpenS3WAL`; never runs `InitSchema` [meta.go:Open]
- Errors: `ErrNotFound`, `ErrAlreadyExists`, `ErrAlreadyDeleted`, `ErrClaimLost`, `ErrInvalidCursor`, `ErrRepoNotDeleted`, `ErrReservedSecret`, `ErrValueNotUTF8` [meta.go:ErrNotFound]
- Types: `Org`, `Repo`, `SigningKey`, `ListParams`, `ListResult`, `Webhook`, `GitCredential`, `RepoUpstream`, `PurgeState`, `PurgeCandidate` [meta.go:Repo]
- `CreateState` (`CreateStateLegacy`, `CreateStatePending`, `CreateStateComplete`), `CreateOutcome` (`CreateCompleted`, `CreateAlreadyComplete`, `CreateDeleted`), `OmittedDefaultBranch` (`main`) [meta.go:CreateState]
- `NewEventingID`, `LegacyClaimHolder` (`!legacy-grace`), `EnvSyncStealLegacy` (`OPENSTORAGE_SYNC_STEAL_LEGACY`) [eventing.go:NewEventingID]
- Options: `Option`, `WithSecretsCodec`, `WithNowFunc`, `WithSyncClaimSink` [secrets.go:Option]
- Store methods (all backends): org and key upsert and reads, `CreateRepo`, `CreateRepoWithState`, `CompleteRepoCreate`, `GetRepoByURL`, `GetRepoByID`, `ListRepos`, `UpdateRepoDefaultBranch`, `SoftDeleteRepo`, signing-key CRUD, webhook CRUD, git credential CRUD, `SetRepoUpstream`, `GetRepoUpstream`, `DeleteRepoUpstream`, `CreateLinkedRepo`, `AdoptUpstreamDefaultBranch`, `RequestSync`, `TryBeginSync`, `FinishSync`, `RequestPush`, `TryBeginPush`, `FinishPush` [pg.go:CreateRepo] [pg_eventing.go:TryBeginSync]
- `RunRestoreStore` (`RestoreSync`, `RestorePush`) — only `*PG` implements it; SQLite and S3WAL restore through `LocalSchedule` [pg_eventing.go:RunRestoreStore]
- Purge methods on the concrete `PG`, `SQLite`, and `S3WAL` backends, not on `Store`: `PurgeableRepos`, `RepoPurgeState`, `PurgeRepoMeta` [purge.go:PurgeableRepos] [purge_sqlite.go:PurgeableRepos] [s3walmeta_purge.go:PurgeableRepos]
- `PGCompleteRepoCreate`, `PGRequestSync` — run inside a caller's Postgres transaction [pg.go:PGCompleteRepoCreate] [pg_eventing.go:PGRequestSync]
- Local schedule: `LocalSchedule`, `LocalClaim`, `LocalWork`, `PushOutcome`, `SyncOutcome`, `LaneOutcome` (`OutcomeDone`, `OutcomeRetry`, `OutcomeRestore`), `RequestPushNow`, `ClaimDuePush`, `ClaimDueSync`, `FinishPushOutcome`, `FinishSyncOutcome`, `ListLocalWork`, `CompleteSyncRepoCreate` [sqlite_schedule.go:LocalSchedule] [s3walmeta_schedule.go:s3walLocalSchedule]
- `ResolveRepoKey` — resolve a key by repo ID, then by URL, within an org [resolve.go:ResolveRepoKey]
- S3WAL: `S3WAL`, `NewS3WAL`, `OpenS3WAL`, `CheckFormat`, `FormatVersionError`, `CorruptDocumentError`, `StaleDocumentError`, `QuarantinedError`, `ConvertS3WALMeta`, `S3WALMetaConversion`, `S3WALMetaCounts`, `S3WALConvertOptions`, `S3WALPendingCreate`, `S3WALPendingCreatesError`, `ViewCheckLogEnv` (`OPENSTORAGE_TEST_VIEWCHECK_LOG`) [s3walmeta.go:S3WAL] [s3walmeta_convert.go:ConvertS3WALMeta]
- SQL tables `orgs`, `org_public_keys`, `org_signing_keys`, `repos`, `webhooks`, `git_credentials`, `repo_upstreams` [pg.go:InitSchema] [sqlite.go:InitSchema]

## Invariants
- `Open` asserts a non-empty URL and rejects unknown schemes [meta.go:Open]
- An unknown `create_state` maps to legacy, which is never treated as complete [meta.go:knownCreateState]
- Repo inserts map an `(org, url)` unique violation to `ErrAlreadyExists`, including soft-deleted rows [pg.go:pgInsertRepo] [sqlite.go:insertRepo]
- `ListRepos` asserts limit in [1, 100]; `decodeCursor` maps any malformed cursor to `ErrInvalidCursor` [pg.go:ListRepos] [cursor.go:decodeCursor]
- Postgres `ListRepos` lowers the `q` and `prefix` pattern once per query in an InitPlan, not once per row, and gives the answers of `url ILIKE` [pg.go:pgListReposQuery]
- `SoftDeleteRepo` returns `ErrAlreadyDeleted` for a deleted row and `ErrNotFound` for an absent one [pg.go:SoftDeleteRepo]
- Credentials and upstreams attach only to live repos; a second one per repo is `ErrAlreadyExists` [pg_eventing.go:createGitCredential]
- `TryBeginSync`/`TryBeginPush` refuse unexpired claims; finish and restore match only the caller's holder; a miss returns `ErrClaimLost` when a different holder owns the lane, else `ErrNotFound` (no run in flight, or upstream detached) [pg_eventing.go:FinishSync] [pg_eventing.go:classifyGuardMiss] [sqlite_eventing.go:FinishSync]
- Push pending levels only coalesce upward; a stolen push claim inherits the force mandate [pg_eventing.go:RequestPush]
- `PurgeRepoMeta` refuses live repos with `ErrRepoNotDeleted` and keeps the repos row as a name tombstone [purge.go:PurgeRepoMeta]
- `encodeSecret` rejects plaintext with the reserved prefix; `decodeSecret` fails closed on an envelope without a codec [secrets.go:encodeSecret]
- `ListWebhooks` returns an empty secret for a row that fails to decrypt; `GetWebhook` fails closed [pg_eventing.go:ListWebhooks]
- S3WAL decode rejects unknown versions (`FormatVersionError`), `seq < 1`, empty writer nonces, and malformed rows; `observeDoc` refuses seq regressions with `StaleDocumentError` [s3walmeta.go:decodeS3WALMetaDocVersion] [s3walmeta.go:observeDoc]
- S3WAL `refuse` quarantines the process on foreign-nonce and vanished-mark classes; a quarantined store returns `QuarantinedError` without store reads [s3walmeta_refuse.go:refuse]
- S3WAL `DeleteRepoUpstream` without a `SyncClaimSink` deletes the row in one mutation; with a sink it runs mark, fence clear (only when a sync holder is set), then row delete [s3walmeta_eventing.go:DeleteRepoUpstream]
- `ConvertS3WALMeta` accepts target 1 or 2 only and refuses pending creates unless `Force` [s3walmeta_convert.go:ConvertS3WALMeta]

## Side Effects
- `PG` uses a pgx pool; `InitSchema` runs DDL in one transaction under advisory lock `0x4D455441` [pg.go:InitSchema]
- `SQLite` creates the DB parent directory and uses WAL mode, a 5000 ms busy timeout, and `BEGIN IMMEDIATE` transactions [sqlite.go:sqliteDSN]
- Reads lazily rewrite secrets to the active key with a compare-and-swap [pg_eventing.go:decryptWebhook]
- Claim leases use the database clock (`now()`) on Postgres and `nowNano` on SQLite and S3WAL [pg_eventing.go:TryBeginSync]
- S3WAL reads `_wal/meta.json` with conditional GETs, writes it with CAS, retries lost races with jittered backoff, and runs an org-key refresh goroutine every 15 s [s3walmeta.go:newS3WALStore] [s3walmeta.go:mutate]
- S3WAL calls the `SyncClaimSink` to mirror sync-claim holders into the ref manifest fence [s3walmeta_eventing.go:beginSync]
- S3WAL records `metrics.S3WALMetaRefused` on refusals and `metrics.S3WALContentionExhausted` when `mutate` returns `s3wal.ErrCASContention` [s3walmeta_refuse.go:refuse] [s3walmeta.go:mutate]
- Under build tag `openstorage_viewcheck`, `init` appends failed view checks to the file in `OPENSTORAGE_TEST_VIEWCHECK_LOG` [s3walmeta_viewcheck.go:init]
- `NewEventingID` and `newRepoID` read `crypto/rand` [eventing.go:NewEventingID]

## Verification
- Run: `go test ./internal/meta/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (needs Docker: the `/pg` half of the backend fan-out boots Postgres; the tag compiles `s3walmeta_viewcheck.go`)
