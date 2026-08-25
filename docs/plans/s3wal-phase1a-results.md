# s3wal Phase 1a results — store-qualification matrix & bench go/no-go (issue #133)

This document is the Phase 1a step 1–2 deliverable from
[`s3wal-plan.md`](./s3wal-plan.md) §3/§4 (named `s3wal-bench-results.md`
there): the §5-R7 store-qualification matrix plus the §3 bench tables with
the SQLite baseline. Every number below comes from a real recorded run of
the in-tree harness (`internal/ref/s3wal/qualify_test.go`,
`bench_test.go`); the exact command is stated next to each table.

**The real-S3 column is blocked: credentials pending.** No AWS credentials
exist in this environment (verified in the plan, re-verified at run time —
the `CODESTORAGE_BENCH_S3_URL` / `CODESTORAGE_TEST_S3WAL_URL` lanes skip
cleanly, visible in the transcripts). No real-S3 cell below claims a
result. Per plan §4: **Gate 1a-B is not evaluated — provisionally
continue; real-S3 gate blocked.**

## Environment

| Component | Value |
|---|---|
| Date | 2026-08-25 |
| Commit under test | `8d682b3` + harness fixes (this branch, `s3wal-refstore`) |
| Host | AMD EPYC 9454P 48-core (96 hw threads), 125 GiB RAM, Linux 6.8.0-124-generic |
| Go | go1.26.5 linux/amd64 |
| Docker | 27.5.1 (build 9f9e405) |
| testcontainers-go | v0.44.0 |
| SeaweedFS image | `chrislusf/seaweedfs:4.42` — `weed server -s3 -dir=/data -ip.bind=0.0.0.0`, S3 gateway on 8333, anonymous auth (static dummy SDK creds), versioning + object-lock never enabled (seaweedfs#8073 avoidance) |
| MinIO image | `minio/minio:RELEASE.2024-01-16T16-07-38Z` (the legacy CI pin), started via `testcontainers-go/modules/minio` exactly like `internal/blob/minio_test.go` |
| Real S3 | **blocked: credentials pending** |

Latency caveat: both containers run on the same host as the harness
(loopback networking, no WAN RTT). SeaweedFS numbers are directional
infrastructure-cost measurements, **not** a substitute for the real-S3
gate (see “Kill-threshold assessment”).

## §5-R7 store-qualification matrix

Command (one lane per store; verdicts are the `VERDICT store=… surface=…`
lines in the transcript):

```
go test ./internal/ref/s3wal/ -count=1 -v
```

Verdict vocabulary: `pass` | `rejects` (fails loudly with a wrong/coarse
error) | `ignores` (silently violates the condition — silent-corruption
class) | `mishandles` (other divergence) | `not-reached` (probe aborted at
an earlier step). SeaweedFS and the external-endpoint lane run in
**require** mode (any non-pass fails the test); the MinIO legacy pin runs
in **record** mode (its behavior is the deliverable).

| Surface | SeaweedFS 4.42 | MinIO legacy 2024-01 | real S3 |
|---|---|---|---|
| raw-roundtrip (Put/Get/Head/Delete) | pass | pass | blocked: credentials pending |
| gocloud-write-small (1 KiB) | pass | pass | blocked: credentials pending |
| gocloud-write-multipart (20 MiB above the transfer manager’s fixed 16 MiB multipart threshold, 5 MiB part size ⇒ 4 parts; see erratum, item 6) | pass — stored ETag carries the `-4` part-count suffix | pass — stored ETag carries the `-4` part-count suffix | blocked: credentials pending |
| gocloud-ifnotexist (duplicate rejected) | pass | **ignores** — accepted an IfNotExist duplicate write | blocked: credentials pending |
| conditional-create-fresh (`If-None-Match: *`) | pass | pass | blocked: credentials pending |
| conditional-create-duplicate (must 412) | pass | **ignores** — accepted a duplicate create | blocked: credentials pending |
| conditional-advance-matched (`If-Match`) | pass | pass | blocked: credentials pending |
| conditional-advance-stale (must 412) | pass | pass | blocked: credentials pending |
| get-304-matched (`If-None-Match` ⇒ 304) | pass | pass | blocked: credentials pending |
| get-304-changed (fresh body + ETag) | pass | pass | blocked: credentials pending |
| probe step 1 — create | pass | pass | blocked: credentials pending |
| probe step 2 — duplicate create 412 | pass | **ignores** — “store IGNORES If-None-Match: * — it accepted a duplicate create (silent-corruption class)” | blocked: credentials pending |
| probe step 3 — matched advance | pass | not-reached | blocked: credentials pending |
| probe step 4 — stale advance 412 | pass | not-reached | blocked: credentials pending |
| probe step 5 — racing advances, one winner | pass | not-reached | blocked: credentials pending |
| list-pagination (1,100 keys > 1000-key pages) | pass | pass | blocked: credentials pending |
| list-order (full-list lexicographic byte order) | pass | pass | blocked: credentials pending |
| list-prefix | pass | pass | blocked: credentials pending |
| list-startafter | pass | pass | blocked: credentials pending |
| member-contiguity (`abc` < `abc+ephemeral` < `abc+import` < `abc-1` < `abc-1+ephemeral` < `abc0`, base members contiguous) | pass | pass | blocked: credentials pending |
| presigned-get (redeemed via plain HTTP) | pass | pass | blocked: credentials pending |
| bucket-bootstrap / health-check (path-style + SigV4 throughout) | pass | pass | blocked: credentials pending |

**Probe refusal is proven twice:** empirically — the legacy MinIO pin
ignores `If-None-Match: *` and `Probe` refuses it at step 2 with the
ignores diagnosis above — and deterministically without Docker
(`TestProbeRefusesNonCompliantStore`, an httptest stub that ignores or
mishandles each conditional header; `TestCASTaxonomy` covers the full §2.3
classification the same way). Notably the legacy MinIO **does** honor
`If-Match` on advance — only creation guards are silently ignored, which
is exactly the corruption class a boot probe must catch.

The external-endpoint lane (`TestQualifyExternalEndpoint`) skipped
cleanly: `CODESTORAGE_TEST_S3WAL_URL` unset.

## §3 bench tables

All cells: 20-op untimed warmup, then the stated sample count; nearest-rank
percentiles; raw per-op CSVs captured via `CODESTORAGE_BENCH_CSV_DIR`
(artifact list at the end). SQLite baseline is the existing
`ref.OpenSQLite` RefStore (WAL, 5 s busy_timeout, BEGIN IMMEDIATE) on
local disk — note the durability/storage asymmetry: the SQLite “push”
writes the 8 MiB pack to a local fsynced file, not a network object PUT.

### Scenario 1 — manifest CAS advance latency

```
CODESTORAGE_BENCH_CSV_DIR=… go test ./internal/ref/s3wal/ -run "^$" \
  -bench "^(BenchmarkCASAdvance|BenchmarkSimulatedPush|BenchmarkGenerationRead)$" \
  -benchtime=220x -count=1 -v
```

| Store / body | p50 | p95 | p99 | mean | samples |
|---|---|---|---|---|---|
| SeaweedFS, 1 KB | 2.24 ms | 3.15 ms | 3.21 ms | 2.31 ms | 220 |
| SeaweedFS, 1 MB | 18.56 ms | 21.93 ms | 22.40 ms | 20.55 ms | 220 |
| SQLite (GetRef+CASRef) | 0.34 ms | 0.43 ms | 0.46 ms | 0.35 ms | 220 |
| real S3 | blocked: credentials pending | | | | |

### Scenario 2 — simulated push (manifest GET + 8 MiB pack PUT + 2 CAS advances, single writer)

Same command as scenario 1.

| Store | p50 | p99 | mean | samples |
|---|---|---|---|---|
| SeaweedFS | 92.19 ms | 110.5 ms | 89.18 ms | 220 |
| SQLite (GetRef + 8 MiB fsynced local file + 2×CASRef) | 12.23 ms | 30.33 ms | 14.81 ms | 220 |
| real S3 | blocked: credentials pending | | | |

### Scenario 3 — hot-repo contention (sustained lost-update detector)

```
CODESTORAGE_BENCH_CONTENTION_SECS=60 CODESTORAGE_BENCH_CSV_DIR=… \
  go test ./internal/ref/s3wal/ -run "^$" -bench "^BenchmarkHotRepoContention$" \
  -benchtime=1x -count=1 -v
```

One 60 s window per N (never combined with a multi-iteration benchtime).
Every window enforces the lost-update detector: strict seq monotonicity,
exactly one recorded winner per seq, final seq == initial + total wins,
final nonce == last winner’s; a window containing any ambiguous outcome
is inconclusive and fails the run (zero ambiguity in every cell here).
`adv/s` divides wins by the runner’s actual elapsed time (window start →
last worker completion), reported per cell. The latency columns are
PER-PUSH success latency — from the pusher’s previous win (or worker
start) to its next win, INCLUDING every lost race, re-read, and backoff
in between (erratum, item 7). **Zero violations in every cell.**
Re-recorded with the post-review accounting fixes (anomalies & findings
items 5 and 7).

| Store / writers | push p50 | push p99 | adv/s | wins (success samples) | elapsed | lost races | contention errors | busy retries | max starve |
|---|---|---|---|---|---|---|---|---|---|
| SeaweedFS, N=2 | 1.73 ms | 3.36 ms | 593.4 | 35,603 | 60.00 s | 31,145 | 0 | — | 2.51 s |
| SeaweedFS, N=8 | 1.38 ms | 298.7 ms | 687.8 | 41,273 | 60.00 s | 231,905 | 0 | — | 1.33 s |
| SeaweedFS, N=32 | 2.22 ms | 739.9 ms | 474.2 | 28,456 | 60.00 s | 549,203 | 0 | — | 1.97 s |
| SQLite, N=2 | 0.19 ms | 0.52 ms | 4,541 | 272,848 | 60.08 s | 36 | — | 4 | 8.94 s |
| SQLite, N=8 | 0.19 ms | 0.53 ms | 4,518 | 271,439 | 60.08 s | 192 | — | 37 | 14.14 s |
| SQLite, N=32 | 0.19 ms | 0.65 ms | 4,267 | 256,713 | 60.16 s | 959 | — | 120 | 15.01 s |
| real S3 | blocked: credentials pending | | | | | | | | |

Bounded fast variant (runs in default `go test`, Docker required):
`TestContentionLostUpdateDetectorSeaweedFS`, N=8 for 3 s at 256 B —
wins=1,810, lostRaces=3,932, contention errors=0, ambiguous=0, max
starve=4.01 s, zero detector violations.

### Scenario 4 — generation-read latency

Same command as scenario 1.

| Store / mode | p50 | p99 | samples |
|---|---|---|---|
| SeaweedFS, unconditional GET | 0.73 ms | 1.32 ms | 220 |
| SeaweedFS, If-None-Match 304 | 0.69 ms | 3.00 ms | 220 |
| SQLite, GetRef (no 304 analog — the read is the local DB lookup) | 0.010 ms | 0.017 ms | 220 |
| real S3 | blocked: credentials pending | | |

## Kill-threshold assessment (plan §3)

**Not evaluable — the §3 kill thresholds are defined against real S3 from
same-region compute; SeaweedFS-local numbers are directional only and are
NOT the Gate 1a-B verdict.** For orientation against the thresholds:

- (2) push p50 > 500 ms / p99 > 2 s: SeaweedFS-local measured 92 ms / 111 ms.
- (3) N=8 aggregate ≥ 3 adv/s and no pusher starved > 30 s: SeaweedFS-local
  measured 687.8 adv/s, max starve 1.3 s. At N=32 throughput dropped to
  474.2 adv/s — below the N=8 figure on this loopback setup, with retries
  growing with N but bounded (zero `ErrCASContention` exhaustions in 60 s;
  no livelock). Whether that constitutes “throughput collapse” must be
  judged on real S3, where the RTT-to-contention-window ratio differs
  completely.
- (4) 304-path p99 > 100 ms: SeaweedFS-local measured 3.0 ms.
- (1) semantic divergence caught by the harness: none on SeaweedFS —
  no lost update, no phantom write, exactly-once winners across ~105,000
  winning advances and ~810,000 lost races in the 60 s campaign plus every
  qualification/probe run.

**Gate 1a-A (self-hosted correctness): PASS.** SeaweedFS 4.42 passes the
full harness in require mode, all five probe steps, and the sustained
contention lost-update detector. The Phase 1a step-3 swap PR and Phase 1b
driver work are unblocked.

**Gate 1a-B (real-S3 performance): blocked — credentials pending.** Needs
a scoped test bucket + credentials from the desk; the harness lanes are
ready (`CODESTORAGE_BENCH_S3_URL`, `CODESTORAGE_TEST_S3WAL_URL`).

## Anomalies & findings

1. **MinIO legacy pin ignores `If-None-Match: *`** (raw SDK create AND
   gocloud `IfNotExist`) while honoring `If-Match` on advance — the
   silent-corruption class. The boot probe refuses it at step 2. This is
   the empirical proof of the plan’s R7 decision and of the probe’s value:
   a store can look “mostly conditional” and still silently break create
   guards.
2. **gocloud `s3blob.ErrorCode` “301” heuristic misfire (client-side
   flake).** gocloud maps any S3 operation error whose *message text*
   contains `"301"` to `NoSuchBucket`/`NotFound` (redirect detection). A
   SeaweedFS RequestID containing the substring `301` (~0.5 % of
   responses) therefore turned a correct 412 duplicate-write rejection
   into `gcerrors.NotFound`, failing one harness run. The
   `gocloud-ifnotexist` surface now classifies by wire evidence (smithy
   `PreconditionFailed`) with the gcerrors code as the primary check. No
   s3wal protocol impact — the CAS client uses the raw SDK by design —
   but it reinforces the plan’s §2.3 choice to never classify conditional
   outcomes through gocloud.
3. **SQLite under 60 s sustained hot-key hammering:** the 5 s
   `busy_timeout` can expire (definitive `SQLITE_BUSY` rejection — the CAS
   provably did not commit), and lock-holder bias starves individual
   writers for up to ~21 s at N=32 while aggregate throughput stays
   ~4,000 adv/s. The harness retries and reports these as the
   `busy-retries` metric. Honest baseline behavior of the zero-infra
   store, now visible in the table.
4. **Harness accounting bug found and fixed during the campaign:** a
   contention writer that errored mid-window discarded its recorded wins,
   making the detector report a phantom update that never happened (first
   SQLite 60 s transcript). Both runners now merge recorded transitions in
   a `defer`. Recorded here because the first (failing) transcript exists;
   the published numbers come from the fixed harness.
5. **Two more harness-accounting fixes after PR review (scenario 3
   re-recorded).** (a) A contention window containing any ambiguous
   outcome is now INCONCLUSIVE and fails the detector run: a
   possibly-landed ambiguous write has no recorded winner, so the previous
   “relaxed accounting” let the exactly-once checks hold vacuously and
   could have masked a real lost update. Ambiguity stays counted/reported
   as a metric; local-container windows are empirically ambiguity-free,
   real-S3 windows may need re-runs until an ambiguity-free window is
   observed. (b) Contention `adv/s` now divides wins by the runner’s
   actual elapsed time (window start → last worker completion) instead of
   the configured window — an advance started just before the deadline can
   retry 409s for seconds and its late success still counts in wins,
   overstating sustained throughput near the 3 adv/s kill threshold. Both
   wins and the elapsed used are reported per cell. The scenario-3 table
   and fast-variant line above were re-recorded with the fixed harness;
   on loopback the actual elapsed ≈ the configured 60 s, so the local
   numbers moved only by run-to-run variance — the fix matters on real S3.
6. **Erratum: the original multipart row never exercised multipart.** The
   surface wrote 12 MiB with `WriterOptions.BufferSize` = 5 MiB, but that
   option sets only the transfer manager’s PART size; its independent
   multipart THRESHOLD is fixed at 16 MiB (vendored
   `transfermanager.defaultMultipartUploadThreshold`, not configurable
   through gocloud). The 12 MiB body hit EOF below the threshold and was
   uploaded as a single PutObject — the row’s original “≥3 parts” claim
   was wrong and the pass verdict proved nothing about multipart. The
   surface now writes 20 MiB (> 16 MiB threshold, 5 MiB parts ⇒ exactly
   4 parts) and records store-side evidence in the verdict detail: the
   stored object’s HeadObject ETag shape. Re-recorded on both container
   stores — SeaweedFS ETag `…-4` and MinIO legacy ETag `…-4` both carry
   the multipart part-count suffix, proving 4-part assembly. Hard
   assertions remain readback size + SHA-256; the ETag shape is recorded,
   not hard-failed, since a store may legitimately use another form.
7. **Erratum: contention latency columns previously understated the
   logical push wait.** The runner timed only the final successful
   Advance call, excluding the lost races, re-reads, and backoffs a
   pusher burns between wins — CAS-attempt latency, not the plan §3
   per-push success latency. The runner now measures each logical
   attempt from the pusher’s previous win (worker start for its first)
   to its next win, and scenario 3 was re-recorded. As expected the
   tails grew sharply where contention is real: SeaweedFS N=8 p99
   2.60 ms → 298.7 ms and N=32 p99 3.88 ms → 739.9 ms (p50 barely moved
   — a pusher that wins back-to-back waits almost nothing; the SQLite
   lane is near-unchanged because its lost races are rare). Throughput
   and starvation columns are methodologically unaffected; their small
   deltas are run-to-run variance.

## Artifacts (referenced, not committed)

Transcripts (`script -q -e -c … /tmp/s3wal-phase1a-5a685e03c4/…`):
`qualify-full.typescript` (qualification, all lanes, verdict lines),
`bench-latency.typescript` (scenarios 1/2/4, 220 samples per cell),
`bench-contention.typescript` (scenario 3, 60 s per N — superseded by the
re-record below). Skip paths are
demonstrated in the same transcripts (`CODESTORAGE_TEST_S3WAL_URL` /
`CODESTORAGE_BENCH_S3_URL` unset ⇒ clean skips).

Raw per-op CSVs for scenarios 1/2/4 (8 files,
`/tmp/s3wal-phase1a-5a685e03c4/csv/`), SHA-256:

```
f3a05a8cedfe6f3d56738a0c423975d6bebee47e2cd5cc48c94419becfd779c0  BenchmarkCASAdvance__store-seaweedfs__size-1KB.csv
01a708fa4c26542aa2db3b74002003658f217c05adb3e9e9adff086a881fa171  BenchmarkCASAdvance__store-seaweedfs__size-1MB.csv
018e5adfdcfaa5781c0860d2f715df37137d6c8cb77c5135ac3fae87188b7ce1  BenchmarkCASAdvance__store-sqlite.csv
279c5a70b3f672886f9429e977b46f9a02c8a5cc5b15a25958b5f0262f99530e  BenchmarkGenerationRead__store-seaweedfs__mode-304.csv
d5533ec66349542feb21eaad1211b43ece53f45e5781e8bbdb278becd5ff9dd3  BenchmarkGenerationRead__store-seaweedfs__mode-get.csv
0d3ad418a28043bcd5f3dbefbc191d5648e5157c05300e3a361a862f46c0e5ab  BenchmarkGenerationRead__store-sqlite__mode-get.csv
6e389df1fce88574671f4ee124ed27618fea320756ef444d64fc891855449c62  BenchmarkSimulatedPush__store-seaweedfs.csv
af04703df6a86f24a3753a5ccb33ef8c7c67b0d1d907fbe1a924c5f55e2e1ab6  BenchmarkSimulatedPush__store-sqlite.csv
```

Scenario-3 re-record with the fixed accounting (anomalies & findings
items 5 and 7; supersedes the earlier item-5-only re-record): log
`/tmp/s3wal-fix-2ab6ce9508/bench-contention-r2.log`, raw per-op CSVs
(6 files, `/tmp/s3wal-fix-2ab6ce9508/csv-r2/`), SHA-256:

```
923512f5be822ba4f85706921e3411dccb7146a17a58652a46995362a5cb8689  BenchmarkHotRepoContention__store-seaweedfs__writers-2.csv
2746516edbb176fda7d9c12d5803799b55092e00db04a2a829fb3a80c73664ea  BenchmarkHotRepoContention__store-seaweedfs__writers-8.csv
94fcd4182a7f5323271903388d751af1f57bbdfc839cd429ccd6365b75547ae4  BenchmarkHotRepoContention__store-seaweedfs__writers-32.csv
bded79a6a97798b498fc164fbfc1ef1de19d636437e5af9e94c39187d2896116  BenchmarkHotRepoContention__store-sqlite__writers-2.csv
3f1baaf2f082e4a9be6c9db7b7ab16c5c96d96ca1450ef8f8fbaacc709779121  BenchmarkHotRepoContention__store-sqlite__writers-8.csv
9ec3d68aa09f467a559fd5b9661fe97b5b99af1d11c1a49237199152d9077805  BenchmarkHotRepoContention__store-sqlite__writers-32.csv
```

Method deviation noted for falsifiability: plan §3 asks for ≥3 runs on
separate days for real-S3 *headline* numbers — that applies to the blocked
Gate 1a-B campaign, not these directional SeaweedFS-local tables, which
are single-day single-run (per cell) measurements.
