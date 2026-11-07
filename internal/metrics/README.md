# `internal/metrics`

## Overview
Prometheus metrics for the server on a private registry. The package defines typed enum labels, recorder functions, fixed-surface HTTP instrumentation, the bounded `/metrics` listener, and a test helper that reads series values.

## Interface
- Label enums: `Surface` (`SurfaceAPI`, `SurfaceGit`, `SurfaceHealth`), `ContentionScope` (`ScopeRefs`, `ScopeScanState`, `ScopeMeta`), `SizeGuardResult` (`SizeGuardWarn`, `SizeGuardRefuse`), `MetaRefusal` (`MetaRefusedDecode`, `MetaRefusedStale`, `MetaRefusedVanished`, `MetaRefusedForeignNonce`, `MetaRefusedVanishedMark`; `MetaRefusal.String` returns its label value), `Job` (`JobOrphanSweep`, `JobRepack`), `Result` (`ResultSuccess`, `ResultNoop`, `ResultError`), `CommitGraphOutcome` (nine `CommitGraph*` values), `StaleRefLockOutcome` (`StaleRefLockRemoved`, `StaleRefLockBusy`, `StaleRefLockKept`) [metrics.go:Surface]
- Recorders: `S3WALContentionExhausted`, `S3WALLeaseRenewalFailed`, `SetS3WALLeaseHeld`, `S3WALManifestSizeGuard`, `S3WALMetaRefused`, `CommitGraphRefresh`, `StaleRefLockCleanup`, `JobRun` [metrics.go:JobRun]
- `InstrumentHTTP`, `InstrumentMux` — count requests by surface and status class and observe duration; `InstrumentMux` also counts mux-generated redirects [metrics.go:InstrumentMux]
- `Handler`, `NewServer` — promhttp exposition and the metrics `http.Server` with one `GET /metrics` route [metrics.go:NewServer]
- `IsUnspecifiedBind` — reports whether a listen address binds all interfaces [metrics.go:IsUnspecifiedBind]
- `Value` — sums current series values that match labels, for tests [metrics.go:Value]
- Metric names: `openstorage_http_requests_total`, `openstorage_http_request_duration_seconds`, `openstorage_s3wal_cas_contention_exhausted_total`, `openstorage_s3wal_lease_renewal_failures_total`, `openstorage_s3wal_lease_held`, `openstorage_s3wal_manifest_size_guard_total`, `openstorage_s3wal_meta_refused_total`, `openstorage_commit_graph_refresh_total`, `openstorage_stale_ref_lock_cleanup_total`, `openstorage_job_runs_total` [metrics.go:httpRequests]

## Invariants
- `name` panics on an out-of-range enum value or an empty label [metrics.go:name]
- `JobRun` panics when an orphan sweep reports a noop result [metrics.go:JobRun]
- `InstrumentHTTP` panics on a nil handler; `InstrumentMux` panics on a nil mux or classifier [metrics.go:InstrumentMux]
- `statusWriter.WriteHeader` records only the first status >= 200; `codeClass` maps status 0 to `2xx` [metrics.go:codeClass]
- Scrape bounds are fixed: `Handler` sets a 10 s timeout and 4 in-flight scrapes [metrics.go:Handler]; `NewServer` sets 8 KiB max header bytes [metrics.go:NewServer]
- `IsUnspecifiedBind` returns false for a malformed address [metrics.go:IsUnspecifiedBind]

## Side Effects
- `init` registers nine collectors on the private registry and pre-creates label series, so a clean process exposes zero values [metrics.go:init]
- Recorders mutate process-wide counters and gauges [metrics.go:S3WALContentionExhausted]
- `statusWriter.Unwrap` exposes the wrapped writer for `http.ResponseController` [metrics.go:statusWriter.Unwrap]
- `NewServer` sets 5 s header, 10 s read, 20 s write, and 60 s idle timeouts [metrics.go:NewServer]
- `Value` calls `registry.Gather` [metrics.go:Value]

## Verification
- Run: `go test ./internal/metrics/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
