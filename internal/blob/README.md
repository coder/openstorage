# `internal/blob`

## Overview
The `BlobStore` contract for immutable pack and LFS objects, with an S3 backend (gocloud `s3blob` plus the AWS SDK v2 client) and a local-filesystem backend (gocloud `fileblob`). The package also owns the key layout for packs and LFS objects, quota usage listing, and the restore-cover logic used when bytes are re-published.

## Interface
- `BlobStore` — UploadFile, DownloadFile, DownloadFileCapped, List, ListPages, Delete, Copy, SignedURL, Stat, StatFingerprint, ReadRange, EnsureBucket, Close [blob.go:BlobStore]
- `ErrNotFound`, `ErrSignedURLUnsupported`, `ErrCapExceeded`, `ErrCoverContinuityLost` — sentinel errors [blob.go:ErrNotFound]
- `ListedObject` — Key, Size, ModTime [blob.go:ListedObject]
- `ListPagesAfter`, `PageListerAfter` — list the keys that sort strictly after a key, so a scan can resume; `S3` and `File` implement it, and any other store gets a filter over `ListPages` [blob.go:ListPagesAfter]
- `PackKey`, `PackPrefix`, `PacksRoot` — `packs/<repo>/pack-<id>.pack` and prefixes [blob.go:PackKey]
- `LFSKey`, `LFSLegacyKey`, `LFSRoot`, `LFSPrefix` — canonical `lfs/<repo>/o2/<oid>`, legacy `lfs/<repo>/<oid>`, and prefixes [blob.go:LFSKey]
- `QuotaRepoGroup`, `StorageUsageBytes` — base repo plus `+ephemeral` and `+import` variants, and their summed pack and LFS bytes [blob.go:StorageUsageBytes]
- `EnsurePublished`, `WithRenewedCover`, `CoverHandoffTimeout` (10 s) — re-upload a missing pack with restore hooks, and run an upload while renewing a restore cover [blob.go:EnsurePublished]
- `Open` — dispatch on URL scheme `s3` or `file` [blob.go:Open]
- `S3`, `OpenS3`, `(*S3).EnsureBucket`, `(*S3).VersioningEnabled`, `(*S3).SignedURL`, `(*S3).Copy`, `(*S3).Close` — S3 backend [blob.go:S3]
- `EnvExpectedBucketOwner` — env `OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`; headers `x-amz-expected-bucket-owner` and `x-amz-source-expected-bucket-owner` [blob.go:EnvExpectedBucketOwner]
- `File`, `OpenFile` and its `BlobStore` methods — filesystem backend [file.go:File]

## Invariants
- `assertSignedURLArgs` requires a non-empty key, method GET or PUT, and a positive expiry [blob.go:assertSignedURLArgs]
- `PackKey`, `LFSKey`, `LFSLegacyKey`, `LFSPrefix` assert non-empty inputs; LFS keys reject an OID containing `/` [blob.go:LFSKey]
- A non-empty expected bucket owner must be exactly 12 digits [blob.go:expectedBucketOwnerFromEnv]
- `OpenFile` rejects a non-`file` scheme, a host, or an empty path [file.go:OpenFile]
- `bucketReadRange` returns an error, not short bytes, when the object is shorter than offset+length [file.go:bucketReadRange]
- `bucketDownloadFile` writes a temp file and renames it, and returns `ErrCapExceeded` past the cap [file.go:bucketDownloadFile]
- `bucketListPages` asserts pageSize > 0 [file.go:bucketListPages]
- `ListPagesAfter` returns only keys strictly after `after`, whether the store starts there or filters: `S3` sends StartAfter (ListObjectsV2) or Marker (legacy ListObjects) only on the first page and only for a key that s3blob sends unchanged [blob.go:s3KeyIsPlain] [file.go:bucketListPagesAfter]
- `ensureBucket` accepts `BucketAlreadyOwnedByYou`/`BucketAlreadyExists` only after a HeadBucket probe when an owner is pinned [blob.go:ensureBucket]
- `EnsurePublished` runs `onRestore` before the repair upload, and an initial `onRestore` failure aborts the restore; an `onRestored` handoff failure after the upload returns `restored=true` with an error [blob.go:EnsurePublished] [blob.go:WithRenewedCover]
- `WithRenewedCover` serializes renew calls and cancels the op when the cover lapses [blob.go:WithRenewedCover]

## Side Effects
- `OpenFile` and `(*File).EnsureBucket` create directories with `MkdirAll` [file.go:OpenFile]
- Uploads, downloads (temp file plus rename), deletes, and copies mutate the bucket or local files [file.go:bucketUploadFile]
- `OpenS3` loads AWS default config; the S3 client adds the owner middleware only when an expected owner is set, and `s3oplog.APIOption` only when it reports enabled [blob.go:s3ClientOptions]
- `ensureBucket` calls CreateBucket and HeadBucket; `VersioningEnabled` calls GetBucketVersioning [blob.go:ensureBucket]
- `(*S3).Copy` stats the source, then uses CopyObject or a multipart copy that aborts on failure [blob.go:multipartCopy]
- `WithRenewedCover` starts a renewal goroutine with timers [blob.go:WithRenewedCover]

## Verification
- Run: `go test ./internal/blob/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (needs Docker: the SeaweedFS tests boot a container and fail without it; they skip only under `-short`) [seaweedfs_test.go:startSeaweedFS] [seaweedfs_test.go:TestS3_CopyMultipart]
- Floci S3 lane (endpoint tests skip unless `OPENSTORAGE_TEST_S3_URL` is set; with the URL set they also need AWS credentials, or they fail; see `docs/reference/testing.md`): `OPENSTORAGE_TEST_S3_URL='s3://floci-test?endpoint=http://127.0.0.1:4566&s3ForcePathStyle=true&region=us-east-1' go test ./internal/blob -run 'TestS3Endpoint' -count=1 -race -v` [s3endpoint_test.go:openS3Endpoint]
