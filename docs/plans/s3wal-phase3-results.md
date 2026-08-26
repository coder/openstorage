# s3wal Phase 3 results — zero-infra serve mode (issue #133)

Phase 3 ([s3wal-plan.md](s3wal-plan.md) §4) makes S3 the sole authority:
one bucket now carries refs (`_wal/repo/`), meta (`_wal/meta.json`), scan
state (`_wal/scan/`), packs (`packs/`), and LFS (`lfs/`), served by
`codestorage serve` with no PostgreSQL and no local-disk authority.

## What landed

- **S3 meta backend (§5-Q4)** — `internal/meta/s3walmeta*.go`: the full
  ~35-method `meta.Backend` over ONE `_wal/meta.json` CAS object
  (seq + writer-nonce ambiguity recovery, 304-validated process-local
  cache, secrets codec unchanged, UTF-8 write rejection, purge seams).
  `meta.Open` dispatches `s3wal://`; one URL drives ref + meta.
- **Sync-claim write-through (§2.8)** — `TryBeginSync` commits the meta
  lease, installs the holder token into the base manifest via
  `ref.S3WAL.SetSyncClaim`, then revalidates the meta claim (self-revoking
  on a raced steal, fail closed); `FinishSync` clears the fence before
  finalizing. A fenced `CASRef` checks the holder in the same object it
  CASes — strictly stronger than the SQL FOR-UPDATE argument. W10
  orderings tested, including the raced-steal fence reinstall.
- **Instance lease (§2.11)** — `_wal/instance-lease` CAS heartbeat
  (TTL 2m, cadence TTL/4): fresh-foreign refusal at startup, expired
  steal, fail-stop on heartbeat loss. Best-effort dual-serve detection;
  correctness never depends on it. `CODESTORAGE_S3WAL_LEASE_TTL=0`
  disables it (loud warning).
- **Serve wiring** — `s3wal://` ref URL requires an `s3://` blob URL
  (same bucket allowed and expected); jobs route to local tickers; hooks
  verified fence-read-only (no sink needed).
- **Sweeper `_wal/` guards** — fail-fast asserts in every sweep/heal/purge
  disposal path plus a mixed-bucket proof test.
- **Docs** — ADR-46 (renumbered from the plan's ADR-44; 44/45 taken),
  zero-infra deployment section with the honest scaling-boundaries table,
  configuration/testing/storage-lifecycle notes.

## Gate results

| Gate | Result |
|---|---|
| `CODESTORAGE_TEST_BACKEND=s3wal` githttp e2e lane, `-race` (full smart-HTTP/LFS suite + drills, one SeaweedFS bucket) | PASS (423s) |
| `TestS3WAL_RestartFromBucketOnly` (node wiped, fresh node from bucket alone, clone + fsck) | PASS |
| `TestS3WAL_DualServeRefusal` (second serve refused under live lease; starts after release/expiry) | PASS |
| `local` and `pg` e2e lanes (regression) | PASS (402s / 433s) |
| meta / ref / s3wal / jobs / simtest / cmd suites, `-race` | PASS |
| lint (golangci-lint 2.13.1) / vet / gofmt | clean |

Real-S3 leg of the recorded dogfooding session remains blocked on AWS
credentials (same blocker as Gate 1a-B) and is documented as outstanding;
the SeaweedFS leg is covered by the e2e lane and drills above.

## Reproduction

```sh
CODESTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp/ -count=1 -race -timeout=40m
go test ./internal/meta/ -run TestS3WALMeta -count=1 -race -v
go test ./internal/ref/s3wal/ -run TestLease -count=1 -race -v
```
