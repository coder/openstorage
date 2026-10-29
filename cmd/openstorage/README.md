# openstorage

## Overview

`cmd/openstorage` is the `openstorage` binary (`package main`). It is the composition root of the service. It parses flags and `OPENSTORAGE_*` environment variables into one validated config, opens the ref, metadata, and blob stores, wires the API, Git, eventing, sync, and background-job packages from `internal/`, and serves them on one HTTP listener. It also ships operator subcommands for keys, tokens, signing keys, pack listing, repo purge, and s3wal metadata conversion.

The files split by role:

| File | Role |
|---|---|
| `main.go` | Subcommand dispatch, `serve` startup and graceful shutdown, `keygen`, `mint`, `list-packs`. |
| `config.go` | Environment variable names, defaults, and fail-fast resolvers into `serveConfig`. |
| `wire.go` | `buildAPIHandler`: metadata store, org auth, sweeps, purge, LFS reclaim, absence reconcile, repack, and `api.New`. |
| `wire_eventing.go` | Serve-side and hook-side eventing: job queue, ref-store decorator, webhook sink, loopback sync key. |
| `wire_eventing_local.go` | `buildLocalSyncQueue`: the non-Postgres job queue. |
| `activereqs.go` | In-flight request tracker, printed when the shutdown drain times out. |
| `signingkey.go`, `purgerepo.go`, `s3walmeta.go` | The `signing-key`, `purge-repo`, and `s3wal-meta` subcommands. |

## Interface

### Subcommands

`[main.go:main]` dispatches on `os.Args[1]`:

| Subcommand | Entry | Flags and arguments |
|---|---|---|
| `serve` | `[main.go:cmdServe]` | `-listen` (falls back to `-addr`, then `OPENSTORAGE_LISTEN`, then `127.0.0.1:0`), `-addr` (alias for `-listen`), `-cache-dir` (default `$TMPDIR/codestorage-cache`), `-data-dir` (falls back to `OPENSTORAGE_DATA_DIR`). |
| `hook` | `[main.go:main]` → `githttp.RunHook` | Git hook entry. `[wire_eventing.go:wireHookEventing]` wraps it so won pushes emit events. |
| `keygen` | `[main.go:cmdKeygen]` | `-out` (required prefix, writes `<out>.pem` and `<out>.pub.pem`), `-alg` (default `ES256`, one of `auth.AllowedAlgorithms`), `-if-missing` (reuse a valid existing pair). |
| `mint` | `[main.go:cmdMint]`, `[main.go:mintToken]` | `-key` (required, org private key PEM, PKCS#8 or SEC1), `-repo` (required, repo URL or `org`), `-iss` (required, org), `-scopes` (default `git:read,git:write`), `-refs` (optional refs-policy JSON), `-alg` (default: inferred from the key), `-ttl` (default `1h`). Prints the JWT. |
| `signing-key add\|list\|remove` | `[signingkey.go:cmdSigningKey]` | `-org` (required), `-data-dir`. `add` takes `-key`, the path to a file that holds an SSH public key line or an ASCII-armored PGP public key block. `[signingkey.go:runSigningKeyAdd]` reads that file. `remove` takes `-id`. |
| `list-packs` | `[main.go:cmdListPacks]` | `-repo` (required), `-data-dir`. Prints each blob key under `blob.PackPrefix(repo)` with its size, then a `total: <n> pack(s)` count line. |
| `purge-repo` | `[purgerepo.go:cmdPurgeRepo]` | One positional `<repo-id>`, `-yes` (required confirmation), `-data-dir`. |
| `s3wal-meta convert` | `[s3walmeta.go:cmdS3WALMeta]`, `[s3walmeta.go:runS3WALMetaConvert]` | `-to` (`2` upgrade, `1` downgrade), `-force` (only with `-to 1`). |
| `version`, `--version` | `[main.go:main]` | Prints `version.String()`. |

### HTTP surface of `serve`

- `[main.go:cmdServe]` mounts three mux patterns: `/api/` (the handler from `buildAPIHandler`), `/healthz` (returns `{"status":"ok"}`), and `/` (the Git smart-HTTP server behind `gitLimit`).
- `[main.go:metricsSurface]` maps each pattern to a metrics surface: `/api/` → `metrics.SurfaceAPI`, `/healthz` → `metrics.SurfaceHealth`, everything else → `metrics.SurfaceGit`.
- `[main.go:cmdServe]` serves `metrics.NewServer()` on a second listener when `OPENSTORAGE_METRICS_LISTEN` is set.
- `[main.go:cmdServe]` prints `listening on <addr>` as the first stdout line, then `metrics listening on <addr>` when metrics are on.

### Configuration (environment)

`[config.go:resolveServeConfig]` resolves everything into `[config.go:serveConfig]`. Each resolver reads through an injected lookup (`[config.go:envLookup]`), not `os.Getenv` directly.

| Group | Variables (`[config.go:const]`) | Resolver |
|---|---|---|
| Storage | `OPENSTORAGE_PG_URL` (aliases `SPIKE_PG_URL`, `PG_URL`), `OPENSTORAGE_S3_URL` (aliases `SPIKE_S3_URL`, `BLOB_URL`), `OPENSTORAGE_DATA_DIR` | `[config.go:resolveStorageURLs]` |
| Listeners and URLs | `OPENSTORAGE_LISTEN`, `OPENSTORAGE_METRICS_LISTEN`, `OPENSTORAGE_BASE_URL`, `OPENSTORAGE_TRUSTED_PROXIES` | `[config.go:normalizeBaseURL]`, `[config.go:resolveTrustedProxies]` |
| Org auth | `OPENSTORAGE_ORG`, `OPENSTORAGE_ORG_PUBKEYS` | `[config.go:resolveServeConfig]`, `[config.go:splitPEMBlocks]` |
| Limits | `OPENSTORAGE_RATE_RPS`, `OPENSTORAGE_RATE_BURST`, `OPENSTORAGE_PACK_QUOTA_BYTES` (default 10 GiB), `OPENSTORAGE_MAX_PUSH_BYTES`, `OPENSTORAGE_VERIFY_SIG_BUDGET` (default 10000), `OPENSTORAGE_PACK_RESERVATION_TTL` (default 1h), `OPENSTORAGE_LFS_STAGING_BYTES` (default 32 GiB), `OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO` | `resolveRateLimit`, `resolvePackQuota`, `resolveMaxPushBytes`, `resolveVerifySigBudget`, `resolvePackReservationTTL`, `resolveLFSStagingBytes`, `resolveLFSMaxObjectsPerRepo` |
| s3wal | `OPENSTORAGE_S3WAL_LEASE_TTL` (`0` disables the instance lease) | `[config.go:resolveS3WALLeaseTTL]` |
| Orphan sweep and retention | `OPENSTORAGE_SWEEP_INTERVAL` (default 1h, `0` disables), `OPENSTORAGE_SWEEP_AGE_THRESHOLD` (default 24h), `OPENSTORAGE_SWEEP_DELETE_ENABLED` (default false, list-only), `OPENSTORAGE_SWEEP_DELETE_SUPERSEDED` (default true), `OPENSTORAGE_SWEEP_DELETE_LFS`, `OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`, `OPENSTORAGE_REPO_RETENTION` | `[config.go:resolveSweepSettings]` |
| LFS GC | `OPENSTORAGE_LFS_GC_INTERVAL` (default `0`, off), `OPENSTORAGE_LFS_GC_GRACE`, `OPENSTORAGE_LFS_GC_OBJECT_BUDGET`, `OPENSTORAGE_LFS_GC_DELETE_ENABLED` (default false, report-only) | `[config.go:resolveLFSGCSettings]` |
| LFS v0.2.0 compatibility | `OPENSTORAGE_LFS_V020_WRITE_COMPAT` (boolean) | Parsed inline by `[config.go:resolveServeConfig]` |
| Absence reconcile | `OPENSTORAGE_ABSENCE_RECONCILE_INTERVAL` (default `0`, off), `OPENSTORAGE_ABSENCE_RECONCILE_BUDGET` | `[config.go:resolveAbsenceSettings]` |
| Repack | `OPENSTORAGE_REPACK_THRESHOLD` (default 10, `0` disables) | `[config.go:resolveRepackThreshold]` |
| Egress (SSRF guards) | `OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE`, `OPENSTORAGE_SYNC_ALLOW_PRIVATE`, `OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE` (deprecated) | `[config.go:resolveWebhookAllowPrivate]`, `[config.go:resolveSyncEgress]` |
| Secrets at rest | `OPENSTORAGE_SECRETS_KEY` (comma-separated base64 AES-256 keys, unset = plaintext), `OPENSTORAGE_SECRETS_MODE` (`""`, `encrypt`, `decrypt-only`) | `[config.go:resolveSecretsCodec]` |

With `-data-dir` and no explicit URLs, `[config.go:resolveStorageURLs]` derives `sqlite://<dir>/codestorage.db`, `file://<dir>/blobs`, and `<dir>/cache`.

Operator subcommands read some of these variables too. `[purgerepo.go:cmdPurgeRepo]` resolves the storage URLs, `OPENSTORAGE_SECRETS_KEY`, `OPENSTORAGE_SECRETS_MODE`, `OPENSTORAGE_PACK_QUOTA_BYTES`, and `OPENSTORAGE_PACK_RESERVATION_TTL`. `[signingkey.go:cmdSigningKey]` resolves the storage URLs and the two secrets variables.

The `hook` subprocess also reads per-push variables that the Git server sets (names defined in `[../../internal/githttp/hook.go:EnvSyncClaim]`). `[wire_eventing.go:hookCallerDeniedForward]` reads `OPENSTORAGE_GIT_SCOPES`. `[wire_eventing.go:hookCallerDeniedPull]` reads `OPENSTORAGE_GIT_REFS_POLICY` and `OPENSTORAGE_GIT_OPS`. `[wire_eventing.go:wireHookEventing]` reads `OPENSTORAGE_SYNC_CLAIM`, which marks a sync run's own loopback push.

### Wiring functions

- `[wire.go:buildAPIHandler]` opens the metadata store, bootstraps org auth, builds serve eventing, sweeper, repo purger, LFS reclaimer, absence reconciler, and repacker, and calls `api.New`. It returns the API handler, `meta.Store`, `auth.Verifier`, a `maybeRepack` hook, a `drainRuns` func for shutdown, and a cleanup func.
- `[wire.go:openMetaStore]` calls `meta.Open` with `meta.WithSecretsCodec` and runs `InitSchema`.
- `[wire.go:bootstrapOrgAuth]` upserts the org keys and returns a verifier over `auth.UnionKeySource(ms, org, loopbackPubPEM)` plus the per-process `[wire_eventing.go:loopbackKey]`.
- `[wire_eventing.go:buildServeEventing]` returns a `jobs.Queue` and an `events.NewRefStore` decorator. It uses `jobs.NewRiver` when `[wire_eventing.go:isPGURL]` is true (`postgres` or `postgresql` scheme), else `[wire_eventing_local.go:buildLocalSyncQueue]`.
- `[wire_eventing.go:wireHookEventing]` sets `githttp.WrapHookRefStore` inside the `hook` subprocess. On Postgres it enqueues through `jobs.NewRiverInsertOnly`. On other backends it records requests in the metadata store for the serve sweep to claim.
- `[wire_eventing.go:webhookSink]` implements the sync event sink by enqueuing one delivery per org webhook.
- `[wire_eventing.go:enqueueSyncOrForward]` is the shared `EnqueueSync` closure for serve and hook paths.
- `[activereqs.go:activeRequests]` wraps the root handler and lists in-flight requests (`describe`), oldest first.

## Invariants

### Startup and config

- `[main.go:main]` `envconfig.CheckLegacy` runs before every subcommand except `hook`, `version`, and `--version`. Old-only `CODESTORAGE_*` settings refuse startup.
- `[main.go:fatalf]` Every error that a subcommand returns prints `openstorage <cmd>: <err>` to stderr and exits `1`. An orderly signal shutdown of `serve` returns nil and exits `0`.
- `[main.go:cmdServe]`, `[main.go:cmdKeygen]`, `[main.go:cmdMint]`, `[main.go:cmdListPacks]`, `[signingkey.go:cmdSigningKey]`, `[purgerepo.go:cmdPurgeRepo]`, `[s3walmeta.go:cmdS3WALMeta]` Each subcommand parses flags with `flag.ExitOnError`. An unknown or malformed flag prints the `flag` package usage message and exits `2`, and `-h` exits `0`. `fatalf` does not run on those paths.
- `[config.go:resolveServeConfig]` A DB URL and a blob URL are required (`errDBURLRequired`, `errBlobURLRequired`). `OPENSTORAGE_ORG` and `OPENSTORAGE_ORG_PUBKEYS` must both be set.
- `[config.go:validateZeroInfraPairing]` An `s3wal://` DB URL requires an `s3://` blob URL.
- `[config.go:envFirst]` The first alias that is present wins, even when its value is empty.
- `[config.go:resolveServeConfig]` These combinations refuse startup: `OPENSTORAGE_LFS_V020_WRITE_COMPAT` with LFS sweep deletion on an active sweep; LFS GC deletion on an active sweep without `OPENSTORAGE_SWEEP_DELETE_LFS`; LFS GC grace below `max(age threshold, sweep interval, jobs.QuarantinePurgeFloor)` in that mode; an absence interval above half the pack reservation TTL.
- `[config.go:resolveRateLimit]` RPS must be finite and `>= 0`. RPS `> 0` requires burst `>= 1`. Burst without RPS is an error.
- `[config.go:resolveMaxPushBytes]` Unset derives `max(2 × pack quota, 2 MiB floor)`, saturating at `MaxInt64`. Unset with quota `0` gives no cap.
- `[config.go:resolvePackReservationTTL]` The TTL must be greater than `githttp.MinPackReservationTTL`.
- `[config.go:resolveS3WALLeaseTTL]` A textual zero disables the lease. Any other value must be `>= 1s`. Only `s3wal` DB URLs parse this variable.
- `[config.go:resolveSweepSettings]` `OPENSTORAGE_REPO_RETENTION`, when non-zero, must be `>= jobs.MinRepoPurgeAge` and needs an active sweep.
- `[config.go:resolveLFSGCSettings]` Grace and object budget must be `> 0`. Delete mode needs a non-zero interval.
- `[config.go:resolveRepackThreshold]` The threshold must be `0` or `>= 2`.
- `[config.go:resolveSyncEgress]` `OPENSTORAGE_SYNC_ALLOW_PRIVATE`, when set, governs both push and pull. A malformed value in either spelling is an error.
- `[config.go:resolveSecretsCodec]` `decrypt-only` requires a configured key.
- `[config.go:normalizeBaseURL]` The base URL must be `http` or `https` with a host and no query or fragment.
- `[config.go:assertf]` Impossible states panic with `openstorage assertion failed:`. Input errors return errors.

### Serve ordering

- `[main.go:cmdServe]` The listener binds before handlers are built, because the API needs the bound address.
- `[main.go:selfLoopbackURL]` Sync-worker loopback pushes target this process's own listener, not `OPENSTORAGE_BASE_URL`. An unspecified bind address maps to `127.0.0.1` or `::1`.
- `[main.go:cmdServe]` The s3wal instance lease is acquired only after `InitSchema`. Losing the lease heartbeat exits the process.
- `[main.go:cmdServe]` If the metrics server stops on its own, `serve` returns an error instead of running unmonitored.
- `[main.go:cmdServe]` The `/api/` subtree belongs only to the API handler, so the Git fallback at `/` cannot shadow API routes.
- `[main.go:cmdServe]` Shutdown on the first SIGINT or SIGTERM: drain sync runs (`shutdownRunWait` 5s plus `shutdownRestoreWait` 2s), then `http.Server.Shutdown` with the total `shutdownDrainTimeout` (10s) minus `shutdownGitStopWait` (2s). On timeout it prints the active requests, stops tracked Git process groups, and shuts down again. A second signal stops Git children and re-raises the signal.
- `[main.go:leaseReleaseTimeout]` The lease release on shutdown is bounded to 5s.

### Eventing and sync

- `[wire_eventing.go:enqueueSyncOrForward]` A missing upstream link (`meta.ErrNotFound`) schedules nothing. A denied forward fails closed. A pull is skipped while the upstream is syncing or when the pull is denied.
- `[wire_eventing.go:forwardDeniedFromContext]` A token without `git:read` cannot trigger a forward. Requests without claims are server-initiated and trusted.
- `[wire_eventing.go:hookCallerDeniedForward]`, `[wire_eventing.go:hookCallerDeniedPull]` Malformed scope, policy, or ops values in the hook environment count as denied.
- `[wire_eventing.go:wireHookEventing]` It is a no-op (eventless pushes still work) when `OPENSTORAGE_ORG` or `OPENSTORAGE_PG_URL`/`PG_URL` is empty, or when the secrets codec, `meta.Open`, or the River client fails. It refuses a ref update when `meta.CheckFormat` reports a format-version or corrupt-document error. A sync run's own loopback push does not re-trigger a sync.
- `[wire_eventing.go:loopbackKey.mint]` Loopback tokens are capped at `loopbackTokenTTL` (15 min). The loopback key exists only in memory and is never persisted.
- `[wire_eventing.go:hookWebhookAllowPrivate]` A malformed value keeps the SSRF guard strict.
- `[wire.go:buildAPIHandler]` Delete-mode sweeps on an S3 bucket with versioning on, or a failed versioning probe, abort startup. The delete guard re-checks versioning on every delete-mode sweep and downgrades to list-only on drift.
- `[wire.go:buildAPIHandler]` With sweeping disabled, a failed startup heal is fatal. LFS reclaim is report-only while the sweep interval is `0`.

### Operator subcommands

- `[main.go:reuseKeyPEMFiles]` `keygen -if-missing` reuses a pair only when both files exist, are regular files, have one PEM block each, have safe modes (private: no group or other bits; public: not group- or other-writable), match each other, can mint a token, and an RSA key has at least 2048 bits.
- `[purgerepo.go:cmdPurgeRepo]` Purge refuses without `-yes`, refuses a repo that is not soft-deleted, and fails closed on a versioned S3 bucket.
- `[purgerepo.go:sanitizeTerm]` Control bytes in repo URLs print as U+FFFD.
- `[s3walmeta.go:cmdS3WALMeta]` The DB URL must start with `s3wal://`. `-to` must be `1` or `2`. `-force` requires `-to 1`.
- `[signingkey.go:validateSigningKeyText]` An SSH key must use a type in `sshPublicKeyTypes`, and its base64 blob must declare the same type. A PGP block must end with the armor footer.

## Side Effects

- `[main.go:cmdServe]` Sets the package global `ref.MaxLFSObjectsPerRepo` from config before it opens any store.
- `[main.go:cmdServe]` Calls `os.Setenv` for `OPENSTORAGE_PG_URL`, `OPENSTORAGE_S3_URL`, `PG_URL`, and `BLOB_URL`, so Git hook subprocesses inherit the resolved URLs.
- `[main.go:cmdServe]` Opens TCP listeners, creates the cache directory (mode `0755`), reads the org public-key file, runs schema DDL on the ref and metadata stores, and calls `EnsureBucket` on the blob store.
- `[main.go:cmdServe]` Holds the s3wal instance lease and reports it with `metrics.SetS3WALLeaseHeld`.
- `[main.go:cmdServe]` Handles SIGINT and SIGTERM. Calls `gitlimit.StopChildren`, which sends SIGTERM then SIGKILL to tracked Git process groups.
- `[wire.go:buildAPIHandler]`, `[wire_eventing.go:buildServeEventing]` Start background work: River workers on Postgres, or local tickers for sweep, LFS reclaim, absence reconcile, and sync on other backends. The first local sweep waits `pendingSyncRecoveryDelay` (15s). `jobs.StartReconcileTicker` starts when required.
- `[wire_eventing.go:buildServeEventing]` The sync worker pushes over HTTP to this process's loopback URL with minted tokens.
- `[wire_eventing.go:webhookSink.Emit]` Enqueues webhook deliveries, which make outbound HTTP calls. The hook path delivers with `hookDeliveryAttempts` (3) on non-Postgres backends.
- `[wire.go:bootstrapOrgAuth]` Upserts the org and its public keys in the metadata store.
- `[wire.go:buildAPIHandler]`, `[wire_eventing.go:slogForHook]` Log to stderr. The hook logger writes at Warn level so the Git client shows the lines.
- `[main.go:cmdKeygen]` Writes `<out>.pem` (mode `0600`) and `<out>.pub.pem` (mode `0644`). Without `-if-missing` it truncates existing files.
- `[signingkey.go:cmdSigningKey]`, `[purgerepo.go:cmdPurgeRepo]` Run metadata schema DDL through `[wire.go:openMetaStore]`, which calls `InitSchema`. `purge-repo` also calls `InitSchema` on the ref store.
- `[signingkey.go:runSigningKeyAdd]`, `[signingkey.go:runSigningKeyRemove]` Insert or delete rows in the org signing-key registry.
- `[purgerepo.go:cmdPurgeRepo]` CAUTION: this is irreversible. It drops the repo's registry rows and quarantines its blobs through `jobs.RepoPurger`.
- `[s3walmeta.go:runS3WALMetaConvert]` Acquires the s3wal instance lease and rewrites the metadata document through `meta.ConvertS3WALMeta`.

## Verification

- Run: `go test ./cmd/openstorage/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
