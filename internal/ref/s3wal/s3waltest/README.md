# `internal/ref/s3wal/s3waltest`

## Overview
Test-only fixtures for the s3wal qualification lanes. The package starts pinned SeaweedFS, Floci, and legacy MinIO containers, builds path-style S3 clients, creates and cleans buckets, and provides a fake S3 handler that fails object PUTs during a simulated boot window. It also holds the `{seq, nonce}` payload codec that s3wal CAS tests write. The package does not import `s3wal`.

## Interface
- `SeaweedFSImage`, `MinIOLegacyImage`, `FlociImage`, `SeaweedFSAccessKey`, `SeaweedFSSecretKey` — pinned image references and fixture static credentials [s3waltest.go:SeaweedFSImage]
- `EnvTestS3WALURL` — env var name `OPENSTORAGE_TEST_S3WAL_URL` [s3waltest.go:EnvTestS3WALURL]
- `StartSeaweedFSContainer`, `StartSeaweedFS` — start the pinned SeaweedFS `weed server -s3` container; the `StartSeaweedFS` wrapper terminates it on test cleanup [s3waltest.go:StartSeaweedFSContainer]
- `StartFloci` — start the pinned Floci container (S3 with non-enforcing auth) [s3waltest.go:StartFloci]
- `StartMinIOLegacy` — start a legacy MinIO container and return endpoint, user, and password [s3waltest.go:StartMinIOLegacy]
- `HTTPClient` — shared pooled HTTP client (256 idle connections) for all fixture S3 clients [s3waltest.go:HTTPClient]
- `RawS3Client`, `RawS3ClientSession` — path-style SigV4 S3 clients with static or session credentials [s3waltest.go:RawS3ClientSession]
- `EnsureBucket`, `MustEnsureBucket`, `WaitWriteReady` — create a bucket with retry and wait until it accepts writes [s3waltest.go:EnsureBucket]
- `OpenExternalS3` — open an `s3://` URL endpoint with safety checks for real S3 [s3waltest.go:OpenExternalS3]
- `DeleteAllPrefix` — list and delete every key under a prefix [s3waltest.go:DeleteAllPrefix]
- `BootingFakeS3` (`Handler`, `Snapshot`) — deterministic fake that fails the first N object PUTs with 500 `InternalError` [s3waltest.go:BootingFakeS3]
- `Payload`, `EncodePayload`, `EncodePayloadNonce`, `DecodePayload`, `DecodeSeq` — JSON `{seq, nonce, fill}` CAS body codec; `DecodeSeq` is the s3wal `Client.DecodeSeq` hook [s3waltest.go:DecodeSeq]
- `NewNonce`, `RandomHex` — crypto-random hex strings [s3waltest.go:RandomHex]

## Invariants
- `DeleteAllPrefix` fails the test when the prefix is empty, so it cannot delete a whole bucket [s3waltest.go:DeleteAllPrefix]
- `OpenExternalS3` requires scheme `s3` and a non-empty host, defaults the region to `us-east-1`, and creates buckets only when `endpoint` is set; on real S3 it runs HeadBucket instead of creating the bucket, and it calls GetBucketVersioning whether or not `endpoint` is set [s3waltest.go:OpenExternalS3]
- `OpenExternalS3` fails the test when bucket versioning is enabled or suspended [s3waltest.go:OpenExternalS3]
- `EnsureBucket` tolerates `BucketAlreadyOwnedByYou` and `BucketAlreadyExists` and stops retrying after 60 s [s3waltest.go:EnsureBucket]
- `WaitWriteReady` retries the PUT/DELETE pair as one unit for at most 60 s [s3waltest.go:WaitWriteReady]
- `DecodePayload` returns ok=false on invalid JSON or an empty nonce [s3waltest.go:DecodePayload]
- `EncodePayloadNonce` pads only when the target size exceeds the bare encoding plus the 10-byte `,"fill":""` overhead [s3waltest.go:EncodePayloadNonce]
- `BootingFakeS3.Handler` fails exactly the first `PutFailures` object PUTs and returns 400 for other methods; `Snapshot` reads counters under the mutex [s3waltest.go:BootingFakeS3.Handler]
- `StartSeaweedFSContainer` terminates the container when the host or mapped-port lookup fails [s3waltest.go:StartSeaweedFSContainer]

## Side Effects
- Starts Docker containers through testcontainers and terminates them on cleanup or through the returned func [s3waltest.go:StartSeaweedFSContainer]
- Registers `tb.Cleanup` terminate hooks [s3waltest.go:StartSeaweedFS]
- Sends S3 CreateBucket, HeadBucket, PutObject, DeleteObject, GetBucketVersioning, and paginated ListObjectsV2 requests [s3waltest.go:EnsureBucket]
- `WaitWriteReady` writes and deletes the scratch key `harness-readiness/<hex>` [s3waltest.go:WaitWriteReady]
- `OpenExternalS3` loads AWS config from the environment [s3waltest.go:OpenExternalS3]
- `RandomHex` and `NewNonce` read `crypto/rand` [s3waltest.go:RandomHex]

## Verification
- Run: `go test ./internal/ref/s3wal/s3waltest/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
