# simtest — deterministic simulation testing (DST)

Deterministic, seeded simulation of a multi-node code-storage cluster with
fault injection, ported from waltier's SimStore + DST design (issue #3).
Everything runs in one process on the SQLite + fileblob backends: zero
containers.

## What it exercises

A seeded scheduler serializes logically-concurrent ops against N simulated
nodes that share one SQLite ref DB and one fileblob directory (each node has
its own handles and cache dir, like separate processes on a host):

- **pushes / API writes** — deterministic commits minted with git plumbing,
  driven through the real pipeline: blob upload → `RegisterPacks` →
  `CASRef`, with the CAS flowing through the production
  `events.RefStore` → `ref.IdempotentCAS` chain;
- **ref deletes**, **node hydrations** (`githttp.Hydrate`), **mid-push
  crashes** (after upload / after register), **node crash-restarts** (warm
  and cold).

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
   transition; every reported win emitted at least one delivery.

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
  `issue #6 class` violation.

Both were validated when this harness landed; the landing PR documents the
detecting seeds.

## Determinism rules

Same seed ⇒ same trace hash. Preserved by: PCG-seeded scheduler/injector
streams (draw order fixed), serialized op execution, pinned git identity
and logical-clock dates (commit SHAs are pure functions of the seed),
sequential pack ids, sorted iteration in checks, and path-free trace lines.
When extending the harness, never iterate a Go map into scheduler decisions
or trace lines, never read wall-clock time, and route every random choice
through the two seeded streams.
