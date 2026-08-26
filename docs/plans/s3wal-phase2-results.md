# s3wal Phase 2 results — crash windows + DST (issue #133)

Phase 2 ([s3wal-plan.md](s3wal-plan.md) §4) subjects the Phase 1b driver
to the two verification layers the plan reserved for it: an explicit
kill/restart enumeration of the §2.5 crash windows, and the deterministic
simulation harness running the driver as a first-class authority backend
under a transport-level fault plan. This document records what runs, the
gate results, and how to reproduce them.

## 1. Crash-window matrix (§2.5 W1–W9)

`internal/ref/s3walstore_crashwindow_test.go` — table-driven, one subtest
per window; each executes its pipeline up to the kill point, reboots a
fresh driver over the same conditional store, runs the documented
recovery action, and asserts the §2.5 invariants (P publish-before-CAS,
T tombstone-before-delete, Q usage == registered + retained + LFS,
A absence-cover survival).

| Window | Kill point | Recovery proven |
|---|---|---|
| W1 | after pack blob PUT, before `RegisterPacks` | orphan unreferenced; reservation charged until TTL, then swept — the live absence cover survives the same sweep (A) |
| W2 | `RegisterPacks` CAS response lost | post-reboot re-apply is idempotent: one row, reservation consumed once, usage credited once |
| W3 | between `RegisterPacks` and `CASRef` | ref never moved (P); retry completes |
| W4 | `CASRef` update response lost | rebooted client's retry through `ref.IdempotentCAS` recovers the committed write to a win (issue #2 contract) |
| W5 | `ReplacePacks` fold response lost | fold present + old rows gone proven atomically; old bytes stay tombstone-counted (T, Q); reclaim heals idempotently after a delete/reclaim crash |
| W6 | `GuardedLFSDelete` killed before the DELETE / between DELETE and clear | stale lease fences registration for the FULL blocking window, then expires; leaked lease reaped by the sweeper strictly past that window |
| W7 | after the purge fence | every row-creating write rejects `ErrRepoPurged`; purge re-run continues |
| W8 | mid purge pipeline | every phase idempotently re-runnable to the terminal tombstone; ledger (covers included) cleared only by the terminal CAS |
| W9 | LFS register CAS response lost | re-apply idempotent: one row, one usage credit, reservation consumed once |

W10 (meta/ref cross-store windows) belongs to Phase 3's zero-infra serve
mode, per the plan. Deep per-predicate recovery coverage (57 cases)
remains in `s3walstore_recovery_test.go` (Phase 1b).

## 2. DST backend lane (plan §4 Phase 2 fault plan)

`simtest.Config.RefBackend` selects the authority store: `sqlite`
(default) or `s3wal` — every node's driver over ONE shared
`s3wal.MemCAS` (the multi-node shared-fake topology, stricter than the
single-node deployment decree: it exercises protocol races, not
deployment races). Faults inject at BOTH layers in one run:

- above the driver: the existing `SimRefStore`/`SimBlobStore` classes
  (exercising `ref.IdempotentCAS`);
- below the driver: per-node `SimCAS` transport views injecting clean
  read/list failures, never-executed and ambiguous(applied|dropped)
  conditional PUTs (exercising the driver's §2.3 mutate/recover
  machinery), plus a targeted stale first read on half the node reboots
  (a live instance treats stale reads as store-fidelity violations by
  design, so only fresh instances may observe one).

All existing oracles run unchanged on both lanes. Vacuity guards:
`TestDSTFast/s3wal` requires injected cas.* faults > 0 (and the sqlite
lane requires exactly 0); the sync/push dead-holder guards apply per
lane.

## 3. Gate results

| Gate | Result |
|---|---|
| `TestDSTFast` (seeds 1–3 × 120 steps, both lanes, `-race`) | PASS |
| `TestDSTDeterminism` (seed 7 twice per lane, `-race`) | PASS — identical trace hashes |
| s3wal sweep at the nightly budget (48 seeds × 300 steps, base 1, `-race`) | PASS (see PR for the log) |
| Crash matrix W1–W9 (`-race`) | PASS |
| Full `internal/ref` + `internal/ref/s3wal` suites (`-race`) | PASS |

Oracle mutation validation (README "Validating the oracle"): neutering
the driver's ambiguous-PUT writer-nonce recovery is detected as a
`reported won but tip=` violation at
`DST_SEED=3 DST_STEPS=200 DST_REF_BACKEND=s3wal`, step 131.

The sweep's first outing caught a real defect on its own lane: at seed
43 step 98 it reported an issue-#2 violation for an
ambiguous-but-committed CASRef whose `ref.IdempotentCAS` point-read
recheck was starved by a `cas.Get` clean fault BELOW the driver — a
starvation class the oracle's exemption (decorator-level `ref.GetRef`
faults only) predated. Driver behavior was correct (fail closed on an
unresolvable ambiguity); the oracle now recognizes transport-level
recheck starvation, with the exemption argued sound in-code: a committed
inner CAS means every driver-issued read in that window succeeded, so a
`cas.Get` fault there can only have hit the recheck.

## 4. Reproduction

```sh
# PR gate, both lanes:
go test ./internal/simtest -run 'TestDSTFast|TestDSTDeterminism' -race -count=1

# One seed on the s3wal lane, live trace:
go run ./cmd/dst -seed 3 -steps 200 -backend s3wal -v

# Nightly-budget sweep on the s3wal lane:
DST_SWEEP_SEEDS=48 DST_STEPS=300 DST_REF_BACKEND=s3wal \
  go test ./internal/simtest -count=1 -race -run 'TestDSTSweep' -v -timeout=110m

# Crash-window matrix:
go test ./internal/ref/ -run 'TestS3WALCrashWindows' -race -count=1 -v
```

The nightly workflow (`.github/workflows/dst-nightly.yml`) sweeps both
lanes as a job matrix.
