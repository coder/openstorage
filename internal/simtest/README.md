# simtest — deterministic simulation testing (DST)

## Overview

Deterministic, seeded simulation of a multi-node open.storage cluster with
fault injection, ported from waltier's SimStore + DST design (issue #3).
Everything runs in one process — zero containers — on two authority-store
lanes (`Config.RefBackend`): the shared SQLite file (default) and the
s3wal manifest-CAS driver over one shared in-memory conditional store
(`s3wal.MemCAS`), the plan §4 Phase 2 lane. The s3wal lane's multi-node
shared-fake topology is deliberately stricter than that backend's
single-node deployment decree: every PROTOCOL race between manifest
writers is simulated. The meta store (sync/push claim lanes) and fileblob
are backend-independent.

## What it exercises

A seeded scheduler serializes logically-concurrent ops against N simulated
nodes that share one SQLite ref DB, one SQLite meta DB, and one fileblob
directory (each node has its own handles and cache dir, like separate
processes on a host):

- **pushes / API writes** — deterministic commits minted with git plumbing,
  driven through the real pipeline: blob upload → `RegisterPacks` →
  `CASRef`, with the CAS flowing through the production
  `events.RefStore` → `ref.IdempotentCAS` chain;
- **ref deletes**, **node hydrations** (`githttp.Hydrate`), **mid-push
  crashes** (after upload / after register), **node crash-restarts** (warm
  and cold);
- **background repacks** (issue #1, `jobs.Repacker`): plain folds through a
  node's fault-injected stores, plus a deterministic two-folder race
  (`repack-race`) where a mid-race push forces the competing folds apart and
  the loser must delete exactly its own uploaded blob;
- **upstream-sync claim leases** (issues #86/#88, `sync-claim` /
  `sync-request`): a minimal sync actor drives TryBeginSync/FinishSync
  cycles against the real meta store (the transfer is a seeded number of
  sim steps, no real git), with seeded crash injection between claim and
  finish; node crashes kill the node's in-flight runs too. The meta store
  runs on the simulation's logical clock (`meta.WithNowFunc`), so lease
  expiry is a pure function of the seed;
- **outbound push-forward claim leases and pending markers** (issue #136,
  `push-request` / `push-claim`): the push_* lane PR #137
  added, driven the same way (RequestPush/TryBeginPush/FinishPush against
  the real meta store on the logical clock) with seeded transfer outcomes
  mirroring the worker's failure taxonomy — success, transient failure
  (the actor re-marks the durable pending level with the run's force,
  like `schedulePushRetry`), terminal non-FF rejection (no retry);
- **quota reservations** (issues #29/#31, `reserve-crash` /
  `reserve-sweep`): a `ReservePackBytes` admission whose pusher then
  crashes, which leaks the reservation, and a sweep that prunes every
  reservation through the node's fault-injected stores.

Storage calls pass through `SimRefStore`/`SimBlobStore`, which inject:

- **clean faults** — the op never executes; the error satisfies pgconn's
  `SafeToRetry`, like the real stores' acquire-stage failures;
- **ambiguous faults** — the op executes, the result is lost (the class
  behind issue #2). This holds for mutating ops and for blob downloads
  (`DownloadFile`, `DownloadFileCapped`). Ref-store reads such as `ListRefs`,
  `GetRef`, and `ListPacks`, and blob `List`, skip the inner call under an
  ambiguous fault, exactly like a clean fault;
- **transient not-found faults** — a pack download reports
  `blob.ErrNotFound` once (the vanished-pack class behind issue #6).

On the s3wal lane a second injection layer sits BELOW the driver: each
node's `SimCAS` transport view of the shared conditional store injects
clean read/list failures, never-executed and ambiguous(applied|dropped)
conditional PUTs — exercising the driver's own §2.3 mutate/recover
machinery under the same run that exercises `ref.IdempotentCAS` above it.

Stale reads are deliberately **not** in the fault plan. The s3wal driver is
written against a store with strong read-after-write consistency (plan
§1/§2.6; a documented deployment prerequisite — AWS S3 guarantees it since
December 2020), so it trusts every unconditional read; its seq watermark
only catches a read that regresses
below the process's own last observation. A stale manifest therefore
yields a silently wrong *read-only* verdict — a sweep that "finds nothing
to prune", a CAS that "lost" — with no conditional PUT to expose it, and
under such a fault the exact-state oracles are unsatisfiable by any
read-only path. PR #159's one-shot stale first read on half the node
reboots modelled exactly that store class and drove the issue-#216
violation (seed 833534285, pinned in `TestDSTRegressions`); it was retired.
The 412 lost-race recovery it was meant to trigger needs no injection: the
multi-node seed cache (issue #182) leaves every other node's cached
snapshot stale after each cross-node write, so their next seeded PUT 412s
and re-reads — measured at 34–59 real 412s per 300-step seed with the arm
gone, versus 7–69 with it (the arm added a handful of unseeded 412s that
walk the same `mutate` path).

## Invariant oracles

Checked from clean (never fault-injected) handles:

1. **(a)** every committed ref resolves through registered, downloadable
   packs: periodic + final cold hydrate on a fresh node must succeed and
   pass `git fsck --connectivity-only`;
2. **(b)** a cold node reproduces the exact ref state: hydrated
   `for-each-ref` must mirror the ref authority;
3. **(c)** no `packs` row references a missing blob (checked every step);
4. step-level CAS atomicity, the issue-#2 recovery contract
   (ambiguous-but-committed create/update with an unstarved recheck must be
   reported as a win; a clean conflict must never hide a committed
   own-write), and hydration absorbing transient not-founds (issue #6);
5. webhook soundness/completeness: every delivery matches a committed
   transition; every reported win emitted at least one delivery;
6. **pack rows only grow, modulo repack** (issue #1, checked every step):
   outside a repack a registered pack row never disappears or changes key;
   a repack's before→after transition is pinned in-op — the observable row
   set is exactly the pre-set or exactly the folded set (replacement
   atomicity), a lost race changes nothing and leaks no blob, and
   superseded blobs outlive the fold (the sweeper owns them);
7. **no sync claim outlives its lease without progress** (issues #86/#88,
   `sync_ops.go`): after any claim attempt at sim time T, no
   `repo_upstreams` row keeps `syncing=true` with an expiry ≤ T — an
   expired claim must be stolen by the very next attempt; steals only take
   expired claims (never live ones) and name the exact previous holder; a
   stolen run's late FinishSync fails (`ErrClaimLost` while the thief
   holds, `ErrNotFound` after it finished); after every mutation point
   (claim attempt, request, finish, late finish, quiescence) the row is
   mirrored field-for-field against a ground-truth model — claim state,
   pending marker, and both counters — so any unmodeled change violates at
   the next observation; and after quiescence (clock jumped past every
   lease) one final claim per repo must succeed and finish cleanly — a
   refusal is exactly the pre-#86 wedged row;
8. **sync-forward oracle over the outbound lane** (issue #136,
   `push_ops.go`), the four #136 invariants: *convergence* — refs are
   abstracted to a per-repo version counter (requests bump it, claims
   snapshot it, successful finishes cover through their snapshot); at
   quiescence every accepted request must be covered by a postdating
   successful run, terminally rejected, or still carried by the pending
   marker or an orphaned claim, and the final claim+finish converges
   covered == local; *force monotonicity* — the pending level only
   coalesces upward until a claim consumes it whole, the force
   TryBeginPush reports must equal the model mandate (marker force OR
   inherited from a dead claim's persisted `push_claim_force`), so a dead
   claim's mandate survives on the claim row alone; *lease safety* — the #88
   liveness/steal/late-finish choreography, push edition; *lane isolation*
   — one shared full-row mirror (`checkUpstreamMirror`) compares every
   sync_* AND push_* column after every mutation point in either lane, so
   a pull-lane transition touching push-lane columns (or vice versa)
   violates at the next observation.

## Entry points

| What | How |
| --- | --- |
| PR gate (fast) | `TestDSTFast`, `TestDSTDeterminism`, `TestDSTRegressions` (the pinned-seed corpus) — run in the normal `go test ./...` pipeline, one subtest per backend lane |
| Reproduce a seed | `DST_SEED=<seed> [DST_STEPS=<n>] [DST_REF_BACKEND=s3wal] go test ./internal/simtest -run 'TestDSTSeed' -v -count=1` |
| CLI repro | `go run ./cmd/dst -seed <seed> [-steps N] [-backend s3wal] [-v] [-keep]` |
| Nightly sweep | `.github/workflows/dst-nightly.yml` → `TestDSTSweep` (`DST_SWEEP_SEEDS`, `DST_SWEEP_BASE`, `DST_STEPS`, `DST_REF_BACKEND` — one job per backend lane) |

## Triaging a failing seed

1. A failing run prints the seed, the step count, and the violation. Replay
   it exactly:

   ```sh
   go run ./cmd/dst -seed <seed> -steps <steps> -v
   ```

   `-v` streams the trace: one line per op with the injected faults, the
   op's outcome, and the ground-truth tip after it. The violation names the
   step and invariant.
2. Determinism means the replay is exact — same commits, same faults, same
   outcome (the trace hash is stable). Narrow with `-steps` (runs stop at
   the first violation anyway) and inspect surviving state with `-keep`,
   which prints the run directory (SQLite DB, blob dir, node caches).
3. The trace's `faults=[...]` field attributes every injected fault to the
   op that consumed it; `ref.CASRef/ambiguous(won=true err=<nil>)` means
   the CAS committed under an ambiguous fault — the #2 recovery path.

## Validating the oracle (mutation test)

The harness must catch re-broken variants of the fixes it guards. Locally
(never commit these):

- revert #2: make `ambiguousError` in `internal/ref/idempotent.go` return
  `false` — the sweep reports an `issue-#2 class` violation;
- revert #6: make `HydratePacks` in `internal/githttp/hydrate.go` fail on
  `blob.ErrNotFound` instead of re-listing — the sweep reports an
  `issue #6 class` violation;
- revert #86: make `TryBeginSync` in `internal/meta/sqlite_eventing.go`
  refuse every `syncing=true` row regardless of expiry (change the refusal
  guard to `if prev.Syncing {`) — the sweep reports an `issue-#88 lease
  liveness` violation on the first claim attempt after a crashed holder's
  lease expires (and an `issue-#88 quiescence` violation on seeds whose
  crashes go unprobed in-run);
- break the #136 rescue semantics: make `TryBeginPush` in
  `internal/meta/sqlite_eventing.go` not inherit the dead claim's force
  (change the mandate to `force = prev.PushPending == 2`) — the sweep
  reports an `issue-#136 force monotonicity` violation on the first steal
  of a crashed forced claim;
- break #136 lane isolation: make `FinishSync` also reset a push-lane
  column (e.g. add `push_claim_force = 0` to its UPDATE) — the shared
  full-row mirror reports a push-claim-state divergence at the next
  observation after a sync finish overlapping a forced push claim;
- break #136 marker monotonicity: make `RequestPush` overwrite instead of
  coalesce (`SET push_pending = ?` instead of `MAX(push_pending, ?)`) —
  the mirror reports a pending-level divergence at the first plain
  request or rescue re-mark that lands on an unconsumed force marker;
- break the s3wal ambiguous-PUT recovery: in `mutate`
  (internal/ref/s3walstore.go) replace the `fresh.Writer == ourNonce`
  identity check with `true` (and drop its seq assert) so every ambiguous
  re-read is treated as an own-write — the s3wal lane reports a
  `reported won but tip=` violation on the first ambiguous-DROPPED
  conditional PUT under a CAS (detecting run:
  `DST_SEED=3 DST_STEPS=200 DST_REF_BACKEND=s3wal`, violation at step
  131).

The first two were validated when this harness landed, the #86 one when
the sync oracle landed, and the #136 ones when the push-lane oracle
landed; the landing PRs document the detecting seeds.

## Determinism rules

Same seed ⇒ same trace hash. Preserved by: PCG-seeded scheduler/injector
streams (draw order fixed), serialized op execution, pinned git identity
and logical-clock dates (commit SHAs are pure functions of the seed),
sequential pack ids, sorted iteration in checks, and path-free trace lines.
When extending the harness, never iterate a Go map into scheduler decisions
or trace lines, never read wall-clock time, and route every random choice
through the two seeded streams.

## Interface

- `Run` — runs one deterministic simulation; infrastructure errors return as
  `error`, invariant breaches as `Result.Violations` [scenario.go:Run]
- `Config`, `Result` — run inputs (seed, steps, nodes, repos, branches,
  `RefBackend`, `FullCheckEvery`, `Dir`, `KeepDir`, `Trace`) and outputs
  (trace hash, violations, counters) [scenario.go:Config]
- `RefBackend`, `RefBackendSQLite`, `RefBackendS3WAL` — authority-store lane
  (`"sqlite"` or `"s3wal"`) [scenario.go:RefBackend]
- `Injector`, `NewInjector` — seeded fault injector; `FailNext`, `FailNth`,
  `SetProbs`, `Count`, `Counts`, `Fired`, `FiredSince`, `Seq` [fault.go:NewInjector]
- `FaultClass` (`FaultNone`, `FaultClean`, `FaultAmbiguous`, `FaultNotFound`),
  `FaultEvent`, `Probs`, `ErrInjectedClean`, `ErrInjectedAmbiguous` [fault.go:FaultClass]
- `FaultClass.String` — the fault class name for traces [fault.go:FaultClass.String]
- Op names `OpRef*`, `OpBlob*`, `OpCASGet`, `OpCASCreate`, `OpCASAdvance`,
  `OpCASList` — the injection points [fault.go:OpRefListRefs]
- `SimRefStore`, `NewSimRefStore` — fault-injecting `ref.RefStore` decorator
  [simref.go:SimRefStore]
- `SimBlobStore`, `NewSimBlobStore` — fault-injecting `blob.BlobStore`
  decorator [simblob.go:SimBlobStore]
- `SimCAS`, `NewSimCAS` — per-node fault-injecting view of a shared
  `s3wal.MemCAS` [simcas.go:SimCAS]

## Invariants

- `Config.withDefaults` asserts positive dimensions and a known `RefBackend`
  [scenario.go:withDefaults]
- `FailNth` asserts a non-empty op, n >= 1, and a clean, ambiguous, or
  not-found class; `SetProbs` asserts non-negative probabilities that sum to
  at most 1 [fault.go:SetProbs]
- One-shot plans fire before probabilistic faults [fault.go:Injector.begin]
- A clean fault satisfies `SafeToRetry`, so `ref.IdempotentCAS` treats it as
  non-execution [fault.go:cleanFaultError]
- On a clean fault the inner call never runs; on an ambiguous fault a
  mutating inner call runs and the caller gets an error [simref.go:CASRef]
  [simblob.go:UploadFile]. Ref-store reads and blob `List` skip the inner
  call under an ambiguous fault [simref.go:refReadFault] [simblob.go:List].
  Blob `DownloadFile` and `DownloadFileCapped` still run the inner call
  [simblob.go:DownloadFile] [simblob.go:DownloadFileCapped]
- `SimCAS` returns a definitive store rejection as a real verdict and never
  injects stale reads, 409 exhaustion, or Delete faults [simcas.go:put]
- `violate` records the breach and stops the run at the first violation
  [scenario.go:violate]
- `casAttempt` flags a ref moved to anything but the target, a win that does
  not match the tip, and an ambiguous committed write not reported as a win
  [scenario.go:casAttempt]
- The quota ledger oracle requires usage == registered pack bytes + retained
  bytes after every step [quota_ops.go:checkQuotaLedger]
- After a completed full reservation sweep, every repo must hold
  `ReservedBytes == 0` [quota_ops.go:doReserveSweep]
- Repack oracles require the post-op pack set to equal the pre-set or the
  folded set [repack_ops.go:doRepack]
- Sync and push oracles check lease liveness, steal safety, late-finish
  errors, and a full-row mirror of the upstream row [sync_ops.go:checkUpstreamMirror]
  [push_ops.go:doPushClaim]

## Side Effects

- `Run` creates a temp run dir (removed unless `KeepDir`), a shared
  `meta.sqlite` file, a fileblob dir, and per-node cache dirs. The ref
  authority is a shared `refs.sqlite` file on the sqlite lane and a shared
  in-memory `s3wal.MemCAS` on the s3wal lane [scenario.go:Run]
  [cluster.go:newCluster]
- Runs hermetic `git` subprocesses (`init --bare`, `config gc.auto 0`,
  `hash-object`, `mktree`, `commit-tree`, `pack-objects`, `for-each-ref`,
  `fsck`) [gitenv.go:simGit] [gitenv.go:initScratchRepo]
- Drives the real stores: `RegisterPacks`, `CASRef`, `ReservePackBytes`,
  `TryBeginSync`, `TryBeginPush`, and `githttp.Hydrate` [scenario.go:doPush]
  [quota_ops.go:doReserveCrash] [sync_ops.go:doSyncClaim]
  [push_ops.go:doPushClaim] [oracle.go:doHydrate]
- `initSync` inserts `orgs` and `repos` rows with raw SQL [sync_ops.go:initSync]
- Cold crashes remove a node's cache dir [cluster.go:crashNode]
- `tracef` hashes every trace line and writes it to `Config.Trace` when set
  [scenario.go:tracef]

## Verification

- Run: `go test ./internal/simtest/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
- Nightly sweep (`.github/workflows/dst-nightly.yml`, one job per backend):
  `DST_SWEEP_BASE=<base> DST_SWEEP_SEEDS=48 DST_STEPS=300 DST_REF_BACKEND=<sqlite|s3wal> go test ./internal/simtest -count=1 -race -run 'TestDSTSweep' -v -timeout=110m`
  (the workflow defaults; the nightly job runs once per backend and takes `<base>` from its `base` input, else `github.run_id % 1000000000`)
- Reproduce a seed: `DST_SEED=<seed> DST_STEPS=<steps> go test ./internal/simtest -run 'TestDSTSeed' -v -count=1`
