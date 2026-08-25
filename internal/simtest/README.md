# simtest — deterministic simulation testing (DST)

Deterministic, seeded simulation of a multi-node code-storage cluster with
fault injection, ported from waltier's SimStore + DST design (issue #3).
Everything runs in one process on the SQLite + fileblob backends: zero
containers.

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
  `push-request` / `push-claim` / `push-rescue`): the push_* lane PR #137
  added, driven the same way (RequestPush/TryBeginPush/FinishPush against
  the real meta store on the logical clock) with seeded transfer outcomes
  mirroring the worker's failure taxonomy — success, transient failure
  (the actor re-marks the durable pending level with the run's force,
  like `schedulePushRetry`), terminal non-FF rejection (no retry) — plus
  a boot-recovery op mirroring local-mode rescue (`PendingPushRepoIDs` +
  plain re-mark).

Storage calls pass through `SimRefStore`/`SimBlobStore`, which inject:

- **clean faults** — the op never executes; the error satisfies pgconn's
  `SafeToRetry`, like the real stores' acquire-stage failures;
- **ambiguous faults** — the op executes, the result is lost (the class
  behind issue #2);
- **transient not-found faults** — a pack download reports
  `blob.ErrNotFound` once (the vanished-pack class behind issue #6).

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
   inherited from a dead claim's persisted `push_claim_force`), and the
   `push-rescue` op exercises the documented plain re-mark downgrade whose
   mandate must survive on the claim row alone; *lease safety* — the #88
   liveness/steal/late-finish choreography, push edition; *lane isolation*
   — one shared full-row mirror (`checkUpstreamMirror`) compares every
   sync_* AND push_* column after every mutation point in either lane, so
   a pull-lane transition touching push-lane columns (or vice versa)
   violates at the next observation.

## Entry points

| What | How |
| --- | --- |
| PR gate (fast) | `TestDSTFast`, `TestDSTDeterminism` — run in the normal `go test ./...` pipeline |
| Reproduce a seed | `DST_SEED=<seed> [DST_STEPS=<n>] go test ./internal/simtest -run 'TestDSTSeed' -v` |
| CLI repro | `go run ./cmd/dst -seed <seed> [-steps N] [-v] [-keep]` |
| Nightly sweep | `.github/workflows/dst-nightly.yml` → `TestDSTSweep` (`DST_SWEEP_SEEDS`, `DST_SWEEP_BASE`, `DST_STEPS`) |

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
  request or rescue re-mark that lands on an unconsumed force marker.

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
