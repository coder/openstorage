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
| Commit under test | `ed698828e2f3d5aa2d7c94fe8f4e089221298325` (code; the commits above it in this PR are doc-only — see anomalies item 8) |
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
| SeaweedFS, 1 KB | 2.63 ms | 3.31 ms | 3.70 ms | 2.58 ms | 220 |
| SeaweedFS, 1 MB | 31.18 ms | 36.76 ms | 39.00 ms | 29.52 ms | 220 |
| SQLite (GetRef+CASRef) | 0.34 ms | 0.48 ms | 0.64 ms | 0.37 ms | 220 |
| real S3 | blocked: credentials pending | | | | |

### Scenario 2 — simulated push (manifest GET + 8 MiB pack PUT + 2 CAS advances, single writer)

Same command as scenario 1.

| Store | p50 | p99 | mean | samples |
|---|---|---|---|---|
| SeaweedFS | 94.83 ms | 113.9 ms | 91.94 ms | 220 |
| SQLite (GetRef + 8 MiB fsynced local file + 2×CASRef) | 11.88 ms | 13.03 ms | 11.64 ms | 220 |
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
items 5, 7, and 8; the measured window is preceded by a ≥20-op untimed
concurrent warmup on a separate scratch key).

| Store / writers | push p50 | push p99 | adv/s | wins (success samples) | elapsed | lost races | contention errors | busy retries | max starve |
|---|---|---|---|---|---|---|---|---|---|
| SeaweedFS, N=2 | 1.87 ms | 3.35 ms | 553.5 | 33,212 | 60.00 s | 29,427 | 0 | — | 2.68 s |
| SeaweedFS, N=8 | 1.41 ms | 294.6 ms | 654.8 | 39,290 | 60.00 s | 220,680 | 0 | — | 1.23 s |
| SeaweedFS, N=32 | 2.20 ms | 730.2 ms | 478.5 | 28,711 | 60.00 s | 559,039 | 0 | — | 1.68 s |
| SQLite, N=2 | 0.19 ms | 0.79 ms | 4,242 | 254,595 | 60.02 s | 27 | — | 6 | 10.12 s |
| SQLite, N=8 | 0.19 ms | 0.66 ms | 4,306 | 258,792 | 60.10 s | 203 | — | 28 | 16.35 s |
| SQLite, N=32 | 0.19 ms | 0.93 ms | 3,921 | 236,054 | 60.20 s | 806 | — | 148 | 25.13 s |
| real S3 | blocked: credentials pending | | | | | | | | |

Bounded fast variant (runs in default `go test`, Docker required):
`TestContentionLostUpdateDetectorSeaweedFS`, N=8 for 3 s at 256 B —
wins=1,995, lostRaces=11,206, contention errors=0, ambiguous=0, max
starve=517 ms, zero detector violations.

### Scenario 4 — generation-read latency

Same command as scenario 1.

| Store / mode | p50 | p99 | samples |
|---|---|---|---|
| SeaweedFS, unconditional GET | 0.34 ms | 0.85 ms | 220 |
| SeaweedFS, If-None-Match 304 | 0.74 ms | 1.21 ms | 220 |
| SQLite, GetRef (no 304 analog — the read is the local DB lookup) | 0.010 ms | 0.016 ms | 220 |
| real S3 | blocked: credentials pending | | |

## Kill-threshold assessment (plan §3)

**Not evaluable — the §3 kill thresholds are defined against real S3 from
same-region compute; SeaweedFS-local numbers are directional only and are
NOT the Gate 1a-B verdict.** For orientation against the thresholds:

- (2) push p50 > 500 ms / p99 > 2 s: SeaweedFS-local measured 95 ms / 114 ms.
- (3) N=8 aggregate ≥ 3 adv/s and no pusher starved > 30 s: SeaweedFS-local
  measured 654.8 adv/s, max starve 1.2 s. At N=32 throughput dropped to
  478.5 adv/s — below the N=8 figure on this loopback setup, with retries
  growing with N but bounded (zero `ErrCASContention` exhaustions in 60 s;
  no livelock). Whether that constitutes “throughput collapse” must be
  judged on real S3, where the RTT-to-contention-window ratio differs
  completely.
- (4) 304-path p99 > 100 ms: SeaweedFS-local measured 1.2 ms.
- (1) semantic divergence caught by the harness: none on SeaweedFS —
  no lost update, no phantom write, exactly-once winners across ~101,000
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
8. **Campaign re-recorded in full at a pinned reachable commit, with the
   contention warmup actually implemented.** Two reproducibility defects
   in earlier revisions of this doc: (a) “Commit under test: `8d682b3` +
   harness fixes” became unreachable after the branch was rebased and
   never pinned the harness exactly; (b) the contention runner started
   its timer immediately after the seed create — connection setup,
   initial reads, and first advances were measured, contradicting the
   “20-op untimed warmup” methodology claim (scenarios 1/2/4 always had
   an explicit warmup loop; scenario 3 did not). The runners now warm
   the concurrent path on a separate scratch key (collectively ≥20 ops
   per cell, no accounting, detector baseline untouched), and the FULL
   campaign — qualification lanes and all four bench scenarios — was
   re-run at the single code commit stamped in the environment table,
   from which every number in this doc now comes. Earlier artifacts are
   superseded. Warmup mostly moved connection-setup noise out of the
   windows (e.g. the scenario-4 unconditional-GET p50 halved); remaining
   deltas against the previous tables are single-run variance on a
   shared host.

## Artifacts (referenced, not committed)

Every number in this doc comes from ONE campaign run at the pinned
commit in the environment table (anomalies & findings item 8); artifacts
from the original campaign and interim partial re-records are
superseded. Logs (`/tmp/s3wal-fix-2ab6ce9508/campaign-8b0c79a/`):
`qualify-full.log` (qualification, all lanes, verdict lines, fast
detector variant), `bench-latency.log` (scenarios 1/2/4, 220 samples per
cell), `bench-contention.log` (scenario 3, 60 s per N). Skip paths are
demonstrated in the same logs (`CODESTORAGE_TEST_S3WAL_URL` /
`CODESTORAGE_BENCH_S3_URL` unset ⇒ clean skips).

Raw per-op CSVs (14 files,
`/tmp/s3wal-fix-2ab6ce9508/campaign-8b0c79a/csv/`), SHA-256:

```
ff53823695a98d90f507ed3fae0ef2aaebe26902a06c9e352a96c432e6065638  BenchmarkCASAdvance__store-seaweedfs__size-1KB.csv
629ed8a081f2e6bc6a51fdab17a8ee8b699bfa157fa55975cbc067798326a221  BenchmarkCASAdvance__store-seaweedfs__size-1MB.csv
634daaae6a5ab9ea85b4b2f75e75b337d41eed6f2d20e9182a7c3e396f80f855  BenchmarkCASAdvance__store-sqlite.csv
3f76f22d5a326181e06efbb9dd7dbf0a3a61ec2175979fb7beeae647f7a66c3f  BenchmarkGenerationRead__store-seaweedfs__mode-304.csv
0616eb4588b437d219a85f735bb12211d0d4b72c2e14f9bf6ac7c2bf9303308a  BenchmarkGenerationRead__store-seaweedfs__mode-get.csv
86262399cf26d202fdb5ce91c5dc7a88a271fa2f686f98fd2f5d2bff2c3da244  BenchmarkGenerationRead__store-sqlite__mode-get.csv
fc0a1b79009cb7f0b6d3e494925419c13feedf3c4437bff0674062489c802760  BenchmarkHotRepoContention__store-seaweedfs__writers-2.csv
1a878544000c5aa904c01cb3298afc71c9a5a40271e04d86537647ed1cf7e1a0  BenchmarkHotRepoContention__store-seaweedfs__writers-8.csv
17c1045d287d6f309a5a6df73bb7279d9b9daa3f2a31e7b4316b28749875bb65  BenchmarkHotRepoContention__store-seaweedfs__writers-32.csv
94bb83d1f93f1da8ea833c84802610cd0cd49dd7ec6709c8a301c7171a32c7f0  BenchmarkHotRepoContention__store-sqlite__writers-2.csv
4f4597cdfa83cc543502277af4f6a5cef0dc612149cab3c1af9fb27f3cdc9a8a  BenchmarkHotRepoContention__store-sqlite__writers-8.csv
49efbc55430fb67be33d8de96bbc63a8fd493327866bd48b7f04af127e60535c  BenchmarkHotRepoContention__store-sqlite__writers-32.csv
f55260f084e0908438eae60c14544bb3af6c9ad80a3544cd78890ceb3ae3e37a  BenchmarkSimulatedPush__store-seaweedfs.csv
dee200c8dce24c49b469dfa1a01d4f50b34212a2113d8f7b00cc9883ff961fe9  BenchmarkSimulatedPush__store-sqlite.csv
```

Method deviation noted for falsifiability: plan §3 asks for ≥3 runs on
separate days for real-S3 *headline* numbers — that applies to the blocked
Gate 1a-B campaign, not these directional SeaweedFS-local tables, which
are single-day single-run (per cell) measurements.
