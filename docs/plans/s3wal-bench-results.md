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
carries one small commit (one ~20-byte file rewrite): **pack size 324–327
bytes** (commit + tree + blob), measured with `git pack-objects` on the
identical object set. Timed region = the `git push` subprocess only
(includes a few ms of git client startup). Every writer's commit chain is
**pre-generated during setup** (Codex round-2: no `git commit` subprocess
runs inside the sustained window — the tables below are the re-measured
numbers after excluding client-side commit generation; a writer that
exhausts its chain ends participation early and is counted in
`exhausted-writers`, zero in every cell). The standard benchmark timer
covers only the sustained windows, and the published ns/op is overridden
to timed-window wall time per successful push (the reciprocal of push/s)
so benchstat tracks throughput; B/op and allocs/op are neutralized to
zero because under `-benchmem` they would report the harness's
whole-window client-side allocations per window iteration — meaningless
for the server push path. Every push subprocess is deadline-bounded
(window remainder + 30 s grace inside the window; 2 min for seed/warmup
pushes and observer polls) with a process-group kill, so a stalled serve
response cannot hang the bench — a killed push records as an ordinary
ambiguous failure. Windows are 60 s sustained per cell after
a ≥ 20-push collective warmup. Serve-side background jobs (e.g. the
10-pack repack fold) run during the windows — deliberately: that is the
production push path.

The bench has two mutually exclusive campaign modes (Codex round-3: a
mid-window reader is itself load — 4 polls/s against 7 push/s at N=1 is
not negligible — and each `ls-remote` poll may hydrate the shared cache
repo, the very mechanism behind issue #170, so the bench could induce the
failures it reports; a truly noninterfering observation path does not
exist, because any read through the serve node shares the hydration path
with the pushers and reading the bucket directly would bypass the
authority semantics the detector must witness):

- **Performance mode (default, `CODESTORAGE_BENCH_OBSERVE` unset):** no
  observer runs inside the window; these runs are the published
  performance tables and the nightly benchstat series. A post-window
  `ls-remote` (outside the timed window) still verifies durability: every
  writer's branch must hold one of that writer's ACCEPTED tips — a
  successful push collapses the set to its acknowledged tip; a failed push
  appends its attempted tip (the client cannot distinguish a definitive
  rejection from a transport failure — or the issue #170 race — whose ref
  update landed, so either outcome is legal). No writer is ever skipped;
  any value outside the set is a violation, and writers ending on an
  ambiguous (>1-tip) set are reported as `ambiguous-final-tips`.
- **Correctness mode (`CODESTORAGE_BENCH_OBSERVE=1`):** additionally, an
  observer goroutine polls `ls-remote` every 250 ms (`observer-polls`
  counts the rounds) and validates each observation against the
  pre-generated chains: an observed tip must be on the writer's chain,
  must not exceed the writer's attempted pushes, and must never fall below
  the rewind floor `min(maxObserved, ackedBeforePoll)` — every index that
  was observed at any earlier poll (in-flight observations are retained,
  not discarded — Codex round-4) and whose acknowledgement completed
  before the current poll started (the pre-poll snapshot, Codex round-7:
  an ack landing mid-poll may postdate the ls-remote snapshot, and using
  it would falsely reject a legal pre-ack observation under bounded-stale
  hydrate reads; the floor therefore lags acks by at most one poll —
  detection of a rewind under a just-acked tip is delayed one poll, never
  false-positive). This catches
  an acknowledged update transiently dropped and later restored by a
  descendant push, which the final check alone cannot see (Codex
  round-2). Any failed observer poll makes the run inconclusive, which
  fails the bench — a silent poll gap would undermine the verdict, and a
  loopback `ls-remote` failure is abnormal (tolerance: zero). Honest
  limit: a rewind fully contained between two polls remains undetectable.
  `TestPushObserverLegality` red-checks the legality rules synthetically,
  including the round-4 interleaving. Latency/throughput from correctness
  runs are NOT performance results (see the separate subsection below).

**Zero violations from either detector in every cell of both campaigns.**

Commands (one lane per invocation; the lane lands in the benchmark name;
add `CODESTORAGE_BENCH_OBSERVE=1` for a correctness-mode run):

```
CODESTORAGE_TEST_BACKEND=s3wal CODESTORAGE_BENCH_PUSH_SECS=60 CODESTORAGE_BENCH_CSV_DIR=… \
  go test ./internal/githttp/ -run '^$' -bench 'BenchmarkPushE2E' -benchtime 1x
CODESTORAGE_TEST_BACKEND=local CODESTORAGE_BENCH_PUSH_SECS=60 CODESTORAGE_BENCH_CSV_DIR=… \
  go test ./internal/githttp/ -run '^$' -bench 'BenchmarkPushE2E' -benchtime 1x
```

### s3wal (zero-infra: SeaweedFS bucket = ref+meta authority AND blob store) — performance mode

| Writers | p50 | p95 | p99 | push/s | samples | rejected pushes | max starve |
|---|---|---|---|---|---|---|---|
| 1 (uncontended floor) | 138.4 ms | 173.2 ms | 182.1 ms | 7.2 | 431 | 0 | 0.20 s |
| 2 | 167.3 ms | 229.3 ms | 260.8 ms | 11.9 | 714 | 0 | 0.29 s |
| 8 | 318.9 ms | 601.3 ms | 858.9 ms | 23.2 | 1,402 | 0 | 2.74 s |
| 32 | 832.5 ms | 3.22 s | 5.75 s | 26.7 | 1,679 | 1 | 12.5 s |

### SQLite (local mode: `sqlite://` + `file://`) — performance mode

| Writers | p50 | p95 | p99 | push/s | samples | rejected pushes | max starve |
|---|---|---|---|---|---|---|---|
| 1 (uncontended floor) | 112.1 ms | 140.8 ms | 150.1 ms | 8.9 | 537 | 0 | 0.16 s |
| 2 | 135.6 ms | 236.8 ms | 285.8 ms | 14.2 | 856 | 0 | 0.37 s |
| 8 | 308.5 ms | 476.6 ms | 730.2 ms | 25.5 | 1,542 | 0 | 1.02 s |
| 32 | 736.6 ms | 1.49 s | 2.82 s | 41.6 | 2,529 | 0 | 3.91 s |

Reading: the uncontended zero-infra push costs ~26 ms (+23%) over local
mode (138 vs 112 ms p50) — the backend is a minority of the push path,
which is dominated by git subprocesses and HTTP round-trips on both lanes.
Under hot-group contention the gap widens in the tail (N=32 p99 5.8 s vs
2.8 s) and throughput keeps scaling on SQLite (41.6 push/s) while s3wal
saturates around 27 push/s. All of this is a same-host loopback floor for
the backend cost; a real client would add its own network RTT to every
number equally.

### Correctness campaign (observer on — NOT performance numbers)

Re-run of the interesting contention cells with the mid-window observer
polling `ls-remote` every 250 ms; latency/throughput here carry the
observer's own load and hydration pressure and are reported only as
context for the correctness verdict:

| Cell | observer polls | observer violations | rejected pushes | ambiguous final tips | pushes |
|---|---|---|---|---|---|
| s3wal, N=8 | 241 | 0 | 0 | 0 | 1,268 |
| s3wal, N=32 | 151 | 0 | 3 | 0 | 1,689 |
| SQLite, N=32 | 237 | 0 | 0 | 0 | 2,639 |

629 polls, all legality-clean: no foreign tips, no tips beyond attempted
pushes, no rewind below the observed-and-acknowledged floor — zero
violations and zero false-positives — and zero failed polls (a failed
poll fails the run as inconclusive). (The s3wal N=32 observer completed
fewer rounds in its 60 s because polls slow under peak load.)

### Retry evidence (contention surfacing to the client)

At N=32 on s3wal, 1 of 1,680 measured performance-mode pushes (and 3 in
the correctness-mode re-run) was rejected when the hook's manifest CAS
exhausted its bounded retry budget — a clean, git-native rejection, not a
hang or a lost update (the accepted-tips detector accounts for every such
rejection):

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
  exhaustions in the microbench; 1 clean rejection in 60 s in the e2e
  bench). Whether that constitutes "throughput collapse" must be judged on
  real S3, where the RTT-to-contention-window ratio differs completely.
- **(4)** 304-path p99 > 100 ms *(real S3 only — not evaluable)*:
  SeaweedFS-local measured 1.14 ms.
- **(1)** SeaweedFS-vs-S3 semantic divergence / lost update under
  contention — the one threshold this campaign CAN evaluate on the
  self-hosted store: **none observed.** Exactly-once winners across
  101,728 winning advances and 788,919 lost races in the 60 s microbench
  cells, plus a clean ls-remote lost-update detector across all 8 e2e push
  cells (~15,300 real pushes across the performance and correctness
  campaigns, plus 629 correctness-mode observer polls all legality-clean).

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
so both lanes produce a nightly series. Nightly runs performance mode only
(`CODESTORAGE_BENCH_OBSERVE` unset): the benchstat series must never carry
the correctness observer's interference. The 60 s §3 campaign windows and
the observer-on correctness campaign stay on-demand (`workflow_dispatch` /
local runs) — they are measurement campaigns, not a nightly regression
signal.

## Raw data

- Checked in ([`s3wal-bench-data/`](./s3wal-bench-data/), one CSV per cell,
  175 KiB total): cells 1/2/4 (`op,latency_ns`) and all 8 push-path cells
  (`op,writer,latency_ns`).
- Not checked in (~14 MiB): the cell-3 contention CSVs (up to ~335 k rows
  per SQLite cell). Regenerate with the cell-3 command above; per-cell
  summary statistics are in the table.
