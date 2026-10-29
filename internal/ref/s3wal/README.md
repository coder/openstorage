# `internal/ref/s3wal`

## Overview
The s3wal conditional-object primitive. `Client` is a typed-error compare-and-swap client over one S3 bucket (Get, Create, Advance, Delete with ETag preconditions). On top of it the package defines the instance lease that prevents two servers from serving one bucket, the v1 per-repo manifest schema with its clock and skew rules, an in-memory fault-injecting fake (`MemCAS`), and a boot-time probe that refuses to serve when a store mishandles conditional writes.

## Interface
- Sentinel errors `ErrNotExist`, `ErrNotModified`, `ErrExists`, `ErrLostRace`, `ErrNotExecuted`, `ErrCASContention`, and `AmbiguousError` (a write that may have landed) [casobj.go:ErrNotExist]
- `Object` — fetched body, ETag, and store `LastModified` [casobj.go:Object]
- `Client`, `New` — conditional client over one bucket; `New` clones the S3 client with a no-retry policy [casobj.go:New]
- `Client.Get`, `Client.GetIfChanged`, `Client.Create` (`If-None-Match: *`), `Client.Advance` (`If-Match`), `Client.Delete`, `Client.List`, `Client.Close` [casobj.go:Create] [store.go:Client.List]
- `Client.StoreDate`, `Client.DecodeSeq` — freshest HTTP Date witness, and the optional hook that arms the per-key seq stale-read assert [casobj.go:StoreDate]
- `ConditionalStore` — the store seam (Get, GetIfChanged, Create, Advance, Delete, List, StoreDate) [store.go:ConditionalStore]
- `Dial` — parses `s3wal://bucket?endpoint=&region=` into a path-style client; no network I/O [store.go:Dial]
- Env `OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER` and header `x-amz-expected-bucket-owner` — pin every request to a 12-digit bucket owner [store.go:envExpectedBucketOwner]
- `LeaseKey` (`_wal/instance-lease`), `DefaultLeaseTTL` (2 min), `LeaseConfig`, `LeaseHeldError` [lease.go:LeaseKey]
- `AcquireInstanceLease`, `InstanceLease.Heartbeat`, `InstanceLease.Release`, `InstanceLease.Holder` — create or steal the lease, renew it, release it if still owned [lease.go:AcquireInstanceLease]
- `ManifestVersion` (1), `OpsRingSize` (64), `RepoPrefix` (`_wal/repo/`) [manifest.go:ManifestVersion]
- `Manifest`, `Member`, `RefRecord`, `PackRecord`, `RetainedPack`, `Reservation`, `DeleteLock`, `LFSState`, `LFSObject`, `SyncClaim` — v1 manifest schema [manifest.go:Manifest]
- `NewManifest`, `DecodeManifest`, `Manifest.EncodeAdvance`, `Manifest.RecordOp`, `Manifest.OpApplied`, `Manifest.Member`, `Manifest.EnsureMember`, `DecodeSeq`, `NewOpID` [manifest.go:DecodeManifest]
- `ClkNext`, `Elapsed`, `SkewOK`, `DriftOK`, `DateTolerance`, `DateSkewBound`, `DateOK` — manifest clock and store-Date skew rules [manifest.go:ClkNext]
- `ManifestKey`, `ValidBaseRepoKey`, `BaseFromManifestKey` — manifest key build, alphabet check, and inverse parse [manifest.go:ManifestKey]
- `MemCAS`, `NewMemCAS`, `MemCASHooks`, `PutFault` (`FaultNone`, `FaultAmbiguousApplied`, `FaultAmbiguousDropped`, `FaultNotExecuted`, `FaultContention`) — in-memory `ConditionalStore` fake with fault hooks and an injectable clock [memcas.go:NewMemCAS]
- `Probe`, `ProbeKey` (`_wal/probe`), `ProbeError`, `ProbeRejected`, `ProbeIgnored`, `ProbeMishandled`, `ProbeInconclusive` — five-step conditional-write fidelity check and its verdict classes [probe.go:Probe]
- Child package: [`s3waltest`](./s3waltest/README.md) — test fixtures (SeaweedFS, Floci, MinIO containers, fake booting S3, payload codec) for this package's qualification lanes

## Invariants
- `New` sets `aws.NopRetryer`, so each classified attempt sends exactly one wire request [casobj.go:New]
- `putConditional` maps 412 to `ErrExists` (create) or `ErrLostRace` (advance), retries only 409 `ConditionalRequestConflict` up to `maxAttempts` then returns `ErrCASContention`, and maps 5xx to `AmbiguousError` [casobj.go:putConditional]
- `classifyTransport` maps dial errors to `ErrNotExecuted` and other transport errors to `AmbiguousError` [casobj.go:classifyTransport]
- `get` maps only 404 `NoSuchKey` to `ErrNotExist` [casobj.go:get]
- `checkSeq` panics when a served seq is below the per-key watermark [casobj.go:checkSeq]
- `AcquireInstanceLease` asserts `2*cadence <= ttl`, refuses a corrupt lease, and refuses while store-clock age is below holder TTL plus 1 s [lease.go:AcquireInstanceLease]
- `renew` fails stop on a stolen, vanished, or corrupt lease, or when `now - renewedAt >= ttl - cadence` [lease.go:renew]
- `DecodeManifest` rejects version != 1, seq < 1, an empty writer, or a zero clock; `EncodeAdvance` asserts the clock advances and the ops ring holds at most 64 entries [manifest.go:DecodeManifest]
- `assertKeysUTF8` panics on invalid UTF-8 keys; `ValidBaseRepoKey` rejects `+` and bytes <= 0x2B or >= 0x7F [manifest.go:assertKeysUTF8]
- `Client.List` asserts a non-empty prefix, a positive limit, and that each key sorts after the cursor [store.go:Client.List]
- `Dial` rejects an empty bucket and any scheme other than `s3wal`; a non-empty owner must be exactly 12 digits [store.go:Dial]
- `MemCAS` returns `ErrExists` or `ErrLostRace` before fault injection runs, and `FaultAmbiguousApplied` commits the write before it returns `AmbiguousError` [memcas.go:fault]
- `probeStepError` classifies ambiguous or not-executed outcomes as `ProbeInconclusive`, definitive 4xx as `ProbeRejected`, and the rest as `ProbeMishandled`; the probe always tries to delete its nonce key [probe.go:probeStepError]

## Side Effects
- Issues S3 GetObject, PutObject, DeleteObject, and paginated ListObjectsV2 calls [casobj.go:get] [store.go:Client.List]
- Records the HTTP Date header of every response through a deserialize middleware [casobj.go:captureStoreDate]
- Mutates `lastSeq` under `seqMu` and the Date witness under `dateMu` [casobj.go:checkSeq]
- Adds `s3oplog.APIOption` to SDK client options when enabled [casobj.go:New]
- `Dial` loads AWS config from the environment and clones an HTTP transport (64 idle connections per host) [store.go:Dial]
- `Heartbeat` runs a ticker loop that advances `LeaseKey`; a failed renew calls `metrics.S3WALLeaseRenewalFailed` [lease.go:renew]
- `Release` deletes `LeaseKey` only when the holder matches [lease.go:Release]
- `Probe` writes and deletes `_wal/probe/<nonce>` and starts two goroutines for the racing-advance step [probe.go:probeAtKey]
- Reads `crypto/rand` for nonces and op IDs [manifest.go:newHexNonce]

## Verification
- Run: `go test ./internal/ref/s3wal/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
- Needs Docker: the SeaweedFS and Floci qualification tests start containers through `s3waltest`; they skip only under `-short`
- Nightly Floci lane (`.github/workflows/floci-nightly.yml`): `go test ./internal/ref/s3wal -run 'TestQualifyFloci$' -count=1 -v -timeout=25m`
- Opt-in legacy MinIO lane (`docs/reference/testing.md`): `OPENSTORAGE_TEST_MINIO_LEGACY=1 go test ./internal/ref/s3wal -run '^TestQualifyMinIOLegacy$' -count=1 -v`
- Opt-in external endpoint lane: set `OPENSTORAGE_TEST_S3WAL_URL` (an `s3://bucket?endpoint=…&region=…` URL) plus AWS credentials to run `TestQualifyExternalEndpoint`; it skips when the variable is unset
