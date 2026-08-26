# s3wal benchmark results — Phase 4 publication (issue #133)

This document is the Phase 4 deliverable from
[`s3wal-plan.md`](./s3wal-plan.md) §3/§4: the §3 bench cells 1–5 re-recorded
at the Phase 3 merge commit, plus the new **full push-path e2e benchmark**
(real `git push` over loopback against a `codestorage serve` process,
`internal/githttp/push_bench_test.go`). Every number below comes from a real
recorded run of the in-tree harnesses; the exact command is stated next to
each table. Raw per-op CSVs for the fixed-op and push-path cells are checked
in under [`s3wal-bench-data/`](./s3wal-bench-data/) (175 KiB); the
contention-cell CSVs are too large to commit (~14 MiB) and are regenerable
with the commands below.

**The real-S3 column is blocked: credentials pending.** No usable AWS
credentials exist in this environment at reporting time (the SSO credentials
are expired; the `CODESTORAGE_BENCH_S3_URL` lane skips cleanly — visible as
`--- SKIP: BenchmarkHotRepoContention/store=s3/...` in the transcripts). No
real-S3 cell below claims a result. Per plan §4: **Gate 1a-B is not
evaluated — provisionally continue; real-S3 gate blocked.**

## Environment

| Component | Value |
|---|---|
| Date | 2026-08-26 (single-day campaign; see method deviations) |
| Commit under test | `7ea41d26da87ba76fd434231b1ebf69edd62d970` (Phase 3 merge) + this branch's bench harness |
| Host | AMD EPYC 9454P 48-core (96 hw threads), 125 GiB RAM, Linux 6.8.0-124-generic |
| Go | go1.27.0 linux/amd64 |
| git (client + serve cache) | 2.55.0 |
| Docker | 27.5.1, testcontainers-go v0.44.0 |
| SeaweedFS image | `chrislusf/seaweedfs:4.42` — `weed server -s3 -dir=/data -ip.bind=0.0.0.0`, S3 gateway on 8333, static-identity SigV4 auth, versioning + object-lock never enabled |
| Real S3 | **blocked: credentials pending** |

Method notes (plan §3 falsifiability):

- Every cell records ≥ 200 samples after a ≥ 20-op untimed warmup
  (`samples` column; the contention and push cells run 60 s sustained
  windows per N).
- Benchmarks run **without `-race`**: the race detector multiplies memory
  traffic and serializes synchronization, which skews latency
  distributions. Race coverage comes from the ordinary test suites
  (`go test ./internal/githttp/ -race` green on this branch).
- **Loopback caveat:** SeaweedFS and the serve node run on the same host as
  the harness (no WAN RTT). SeaweedFS numbers are directional
  infrastructure-cost measurements, **not** a substitute for the real-S3
  kill-threshold gate.
- **Single-day deviation:** plan §3 wants ≥ 3 runs on separate days for
  headline numbers. This campaign is one day's run; the numbers agree with
  the independent Phase 1a campaign (2026-08-25, see
  [`s3wal-phase1a-results.md`](./s3wal-phase1a-results.md)) where cells
  overlap, and the new nightly lane (below) produces an ongoing series.

## §3 cells 1–5 (microbench harness, `internal/ref/s3wal/bench_test.go`)

Commands:

```
# cells 1 + 4 (300 samples/cell) and cell 2 (200 samples/cell):
CODESTORAGE_BENCH_CSV_DIR=… go test ./internal/ref/s3wal/ -run '^$' \
  -bench 'BenchmarkCASAdvance|BenchmarkGenerationRead' -benchtime 300x
CODESTORAGE_BENCH_CSV_DIR=… go test ./internal/ref/s3wal/ -run '^$' \
  -bench 'BenchmarkSimulatedPush' -benchtime 200x
# cell 3 (60 s sustained per N; large CSVs — not committed):
CODESTORAGE_BENCH_CONTENTION_SECS=60 CODESTORAGE_BENCH_CSV_DIR=… \
  go test ./internal/ref/s3wal/ -run '^$' -bench 'BenchmarkHotRepoContention' -benchtime 1x -v
```

### Cell 1 — manifest CAS advance latency

| Store / body | p50 | p95 | p99 | samples |
|---|---|---|---|---|
| SeaweedFS, 1 KB | 2.21 ms | 2.42 ms | 2.82 ms | 300 |
| SeaweedFS, 1 MB | 23.66 ms | 26.41 ms | 27.58 ms | 300 |
| SQLite (CASRef analog) | 0.36 ms | 0.44 ms | 0.54 ms | 300 |
| real S3 | blocked: credentials pending | | | |

### Cell 2 — simulated push (manifest GET + 8 MiB pack PUT + 2 CAS advances)

| Store | p50 | p99 | samples |
|---|---|---|---|
| SeaweedFS | 98.14 ms | 116.6 ms | 200 |
| SQLite (+ 8 MiB fsynced local file; durability asymmetry — local disk vs S3 PUT) | 12.29 ms | 34.49 ms | 200 |
| real S3 | blocked: credentials pending | | |

### Cell 3 — hot-repo contention (60 s sustained per N; doubles as the lost-update detector)

Latency columns are per-push success latency (previous win → next win,
including every lost race, re-read, and backoff). **Zero lost-update
violations in every cell.**

| Store / writers | p50 | p99 | adv/s | wins | lost races | contention errors / busy retries | max starve |
|---|---|---|---|---|---|---|---|
| SeaweedFS, N=2 | 1.83 ms | 3.32 ms | 575.7 | 34,543 | 30,510 | 0 | 1.87 s |
| SeaweedFS, N=8 | 1.47 ms | 284.9 ms | 654.7 | 39,284 | 219,357 | 0 | 1.30 s |
| SeaweedFS, N=32 | 2.36 ms | 700.3 ms | 465.0 | 27,901 | 539,052 | 0 | 1.85 s |
| SQLite, N=2 | 0.16 ms | 0.38 ms | 5,575 | 335,081 | 27 | 3 | 8.2 s |
| SQLite, N=8 | 0.16 ms | 0.40 ms | 5,546 | 333,311 | 229 | 28 | 11.3 s |
| SQLite, N=32 | 0.16 ms | 0.48 ms | 5,337 | 320,841 | 1,061 | 116 | 15.5 s |
| real S3 | blocked: credentials pending | | | | | | |

Consistent with Phase 1a (SeaweedFS 553/655/478 adv/s then; 576/655/465
now). SQLite's higher per-writer starvation (8–15 s vs SeaweedFS's ≤ 1.9 s)
is the same fairness artifact recorded there: the file lock favors the
writer that just released it.

### Cell 4 — generation-read latency

| Store / mode | p50 | p99 | samples |
|---|---|---|---|
| SeaweedFS, unconditional GET | 0.83 ms | 1.23 ms | 300 |
| SeaweedFS, If-None-Match 304 | 0.69 ms | 1.14 ms | 300 |
| SQLite, GetRef | 0.010 ms | 0.016 ms | 300 |
| real S3 | blocked: credentials pending | | |

### Cell 5 — SQLite baseline

Included as the `sqlite` rows above: the honest "what zero-infra costs you"
comparison is roughly **6× on a CAS advance (µs → low ms), 8× on a
simulated push, ~10× on aggregate hot-repo throughput** — against a
loopback SeaweedFS; real-S3 RTT widens all of these.

## Push-path e2e benchmark (`internal/githttp/push_bench_test.go`)

Real `git push` round-trips against a `codestorage serve` node over
loopback HTTP: client git subprocess → smart HTTP → receive-pack → hook
re-exec (separate process) → ref CAS in the backend authority → pack in the
blob store. Each cell registers a fresh repo; `writers=1` is the
uncontended floor (single pusher, its own repo); `writers=2/8/32` are N
concurrent pushers on ONE repo, each on its own branch, so every push
contends on the repo group's ref authority (the s3wal manifest CAS / the
SQLite write lock) without git-level non-fast-forward rejections. Each push
carries one small commit (one ~20-byte file rewrite): **pack size 325–327
bytes** (commit + tree + blob), measured with `git pack-objects` on the
identical object set. Timed region = the `git push` subprocess only
(includes a few ms of git client startup); commit creation is untimed.
Windows are 60 s sustained per cell after a ≥ 20-push collective warmup.
Serve-side background jobs (e.g. the 10-pack repack fold) run during the
windows — deliberately: that is the production push path.

A post-window `ls-remote` lost-update detector verifies every writer's
branch equals its last successfully pushed head. **Zero violations in every
cell.**

Commands (one lane per invocation; the lane lands in the benchmark name):

```
CODESTORAGE_TEST_BACKEND=s3wal CODESTORAGE_BENCH_PUSH_SECS=60 CODESTORAGE_BENCH_CSV_DIR=… \
  go test ./internal/githttp/ -run '^$' -bench 'BenchmarkPushE2E' -benchtime 1x
CODESTORAGE_TEST_BACKEND=local CODESTORAGE_BENCH_PUSH_SECS=60 CODESTORAGE_BENCH_CSV_DIR=… \
  go test ./internal/githttp/ -run '^$' -bench 'BenchmarkPushE2E' -benchtime 1x
```

### s3wal (zero-infra: SeaweedFS bucket = ref+meta authority AND blob store)

| Writers | p50 | p95 | p99 | push/s | samples | rejected pushes | max starve |
|---|---|---|---|---|---|---|---|
| 1 (uncontended floor) | 132.2 ms | 205.9 ms | 222.9 ms | 5.3 | 318 | 0 | 0.33 s |
| 2 | 147.9 ms | 211.2 ms | 231.5 ms | 9.9 | 594 | 0 | 0.32 s |
| 8 | 284.2 ms | 531.6 ms | 766.7 ms | 22.5 | 1,360 | 0 | 1.94 s |
| 32 | 779.1 ms | 2.75 s | 5.83 s | 27.0 | 1,685 | 3 | 13.0 s |

### SQLite (local mode: `sqlite://` + `file://`)

| Writers | p50 | p95 | p99 | push/s | samples | rejected pushes | max starve |
|---|---|---|---|---|---|---|---|
| 1 (uncontended floor) | 102.4 ms | 125.8 ms | 136.5 ms | 6.5 | 390 | 0 | 0.25 s |
| 2 | 121.0 ms | 162.4 ms | 174.1 ms | 11.7 | 700 | 0 | 0.24 s |
| 8 | 237.4 ms | 391.2 ms | 416.3 ms | 27.3 | 1,641 | 0 | 0.53 s |
| 32 | 657.5 ms | 1.16 s | 1.35 s | 42.9 | 2,605 | 0 | 1.84 s |

Reading: the uncontended zero-infra push costs ~30 ms (+29%) over local
mode (132 vs 102 ms p50) — the backend is a minority of the push path,
which is dominated by git subprocesses and HTTP round-trips on both lanes.
Under hot-group contention the gap widens in the tail (N=32 p99 5.8 s vs
1.35 s) and throughput keeps scaling on SQLite (42.9 push/s) while s3wal
saturates around 27 push/s. All of this is a same-host loopback floor for
the backend cost; a real client would add its own network RTT to every
number equally.

### Retry evidence (contention surfacing to the client)

At N=32 on s3wal, 3 of 1,688 measured pushes were rejected when the hook's
manifest CAS exhausted its bounded retry budget — a clean, git-native
rejection, not a hang or a lost update:

```
remote: codestorage hook: s3wal cas ref repo_…: 16 attempts: s3wal: conditional-write contention retries exhausted
! [remote rejected] HEAD -> w11 (hook declined)
```

Separately, the N=32 warmup surfaced a **spurious rejection race** that
exists on every backend but is widest on s3wal (longer hook CAS latency): a
branch-creation push can be rejected with `cannot lock ref: reference
already exists` even though the hook already committed the ref to the
authority — a concurrent request's hydration syncs the just-committed ref
into the shared cache repo before receive-pack's own local ref update. No
data loss (the authority has the commit; a retry fast-forwards); the client
sees a spurious `[remote rejected]`. Recorded here as a finding for triage;
the bench tolerates and counts these like any rejection.

## Kill-threshold evaluation (plan §3)

Thresholds (2), (3), and (4) are **defined against real S3 from same-region
compute and cannot be evaluated in this campaign** (Gate 1a-B blocked).
SeaweedFS-loopback numbers are quoted as directional evidence only:

- **(2)** simulated-push p50 > 500 ms / p99 > 2 s *(real S3 only — not
  evaluable)*: SeaweedFS-local measured p50 98 ms / p99 117 ms.
- **(3)** N=8 aggregate < 3 adv/s or > 30 s starvation; N=32 collapse below
  N=8 or unbounded retries *(real S3 only — not evaluable)*: SeaweedFS-local
  N=8 measured 654.7 adv/s, max starve 1.3 s — orders of magnitude clear.
  At N=32 throughput dropped to 465.0 adv/s, below the N=8 figure on this
  loopback setup (same shape as Phase 1a: 478.5 vs 654.8) with retries
  growing but bounded (~19 lost races per win, zero retry-budget
  exhaustions in the microbench; 3 clean rejections in 60 s in the e2e
  bench). Whether that constitutes "throughput collapse" must be judged on
  real S3, where the RTT-to-contention-window ratio differs completely.
- **(4)** 304-path p99 > 100 ms *(real S3 only — not evaluable)*:
  SeaweedFS-local measured 1.14 ms.
- **(1)** SeaweedFS-vs-S3 semantic divergence / lost update under
  contention — the one threshold this campaign CAN evaluate on the
  self-hosted store: **none observed.** Exactly-once winners across
  101,728 winning advances and 788,919 lost races in the 60 s microbench
  cells, plus a clean ls-remote lost-update detector across all 8 e2e push
  cells (~9,300 real pushes).

**Verdict input:** nothing in the SeaweedFS or SQLite data trips any
threshold that can be measured locally; the real-S3 thresholds remain
unmeasured. Per plan §4 the gate report therefore stays: **provisionally
continue; real-S3 gate blocked.**

## Nightly bench lane

`bench-nightly.yml` already runs the full `-bench .` sweep with
`CODESTORAGE_TEST_BACKEND=local`, which now includes `BenchmarkPushE2E` on
the SQLite lane (default 5 s windows) and the s3wal microbenches (SeaweedFS
container — the runner has Docker, the sweep already depended on it). A
second step re-runs `BenchmarkPushE2E` with `CODESTORAGE_TEST_BACKEND=s3wal`
so both lanes produce a nightly series. The 60 s §3 campaign windows stay
on-demand (`workflow_dispatch` / local runs) — they are a measurement
campaign, not a nightly regression signal.

## Raw data

- Checked in ([`s3wal-bench-data/`](./s3wal-bench-data/), one CSV per cell,
  175 KiB total): cells 1/2/4 (`op,latency_ns`) and all 8 push-path cells
  (`op,writer,latency_ns`).
- Not checked in (~14 MiB): the cell-3 contention CSVs (up to ~335 k rows
  per SQLite cell). Regenerate with the cell-3 command above; per-cell
  summary statistics are in the table.
