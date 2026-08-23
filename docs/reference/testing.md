# Testing reference

How this repository is tested: the lanes CI runs, the conventions for writing
new tests, and the deterministic simulation (DST) harness. For the two-command
quick version, see [CONTRIBUTING](../../CONTRIBUTING.md#tests).

## Test lanes

One test suite, two storage stacks. Which stack a test run uses is decided in
`TestMain`, not per test:

| Lane | Command | Backends | Needs |
|---|---|---|---|
| **Default (`pg`)** | `go test ./... -count=1 -race -timeout=30m` | Postgres 17 + MinIO, booted per run via [testcontainers](https://golang.testcontainers.org/) | Docker, git, git-lfs |
| **Local** | `CODESTORAGE_TEST_BACKEND=local go test ./internal/githttp -count=1 -race -timeout=15m` | SQLite (`sqlite://`) + plain-file blobs (`file://`) | git, git-lfs — **no Docker** |
| **Backend fan-out** | part of the default lane | `internal/api`, `internal/meta`, and `internal/ref` run every integration/conformance test twice, as `/pg` and `/sqlite` subtests | Docker (for the `/pg` half) |

These are exactly the CI jobs in
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml): `test` runs the
default lane, `test-local` runs the local lane, and `lint` / `image` complete
the required set. CI asserts `git lfs version` up front so a missing LFS client
fails the job instead of silently skipping LFS coverage.

`CODESTORAGE_TEST_BACKEND` accepts `pg` (or unset) and `local`; anything else
panics in `TestMain` (`internal/githttp/e2e_test.go`). The `internal/githttp`
e2e harness builds the real `codestorage` binary, boots the chosen backends,
starts server nodes as subprocesses, and drives them with plain `git`, `curl`-level
HTTP, and a real `git lfs` client.

### Flags that are not optional

- **`-count=1` whenever an env var selects behavior.** The Go build cache does
  not key test results on environment variables read at run time, so switching
  `CODESTORAGE_TEST_BACKEND` (or any `DST_*` variable) without `-count=1` can
  replay the *other* lane's cached results and report them as fresh.
- **`-race` always.** Every CI lane runs under the race detector, including the
  nightly DST sweep. Local runs without `-race` are for fast iteration only.

## Writing tests: house conventions

### Red-first tests

Bug fixes and behavior changes land red-first:

1. Write the test that captures the desired behavior **before** the fix, run
   it, and confirm it fails — *for the expected reason*, not from a typo in the
   test.
2. Land the fix; the same test goes green with no edits to its assertions.
3. Keep the reproduction narrow: one test that pins the bug beats a broad one
   that happens to trip over it.

Reviewers may ask for the red run; for subtle fixes, note the pre-fix failure
message in the PR.

### E2E conventions (`internal/githttp`)

- **No `-count=N` on the e2e suites.** E2E tests register repositories under
  fixed names (`t-repack`, ...) against a backend that lives for the whole
  `TestMain` process, so an in-process rerun collides with the first run's
  state. Rerun by invoking `go test` again, not with `-count`.
- **Repack convergence asserts must use threshold 2.** With
  `CODESTORAGE_REPACK_THRESHOLD=t`, pack rows converge *below* `t`, not to one
  row — a fold that snapshots all but the latest push can settle at `t-1` rows
  permanently. Only `t=2` guarantees eventual convergence to exactly one pack,
  which is what makes it assertable.
- **Budget for job chains generously.** Background work (repack successors,
  sync) runs through the real queue with delays; convergence waits in the suite
  use minutes-scale deadlines because `-race` on a loaded CI runner is slow.
  Tight sleeps are how CI-only flakes are born.
- **Local-only tests skip, not fail, on the pg lane** — follow the existing
  pattern: check the lane and `t.Skipf` with the `CODESTORAGE_TEST_BACKEND=local`
  hint.

### Hermetic git

Tests that shell out to git must not inherit the developer's identity or
hooks. The house pattern (see `internal/simtest/gitenv.go`) pins
`GIT_AUTHOR_*` / `GIT_COMMITTER_*` and neutralizes config:
`GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_SYSTEM=/dev/null`,
`GIT_CONFIG_NOSYSTEM=1`. Host-level `GIT_*` variables and global hooks
otherwise leak into fixture commits and change SHAs.

### Fault injection

The e2e suites use server-side knobs (`CODESTORAGE_TEST_MODE`,
`CODESTORAGE_TEST_CRASH`, `CODESTORAGE_TEST_CAS_DROP`, ...) to force crashes at
precise pipeline stages; the server refuses them outside test mode. The full
list is in the [configuration reference](configuration.md#internal-and-test-variables).

## The DST harness

`internal/simtest` is a deterministic, seeded simulation of a multi-node
cluster — pushes, API writes, hydrations, crashes, restarts, repack races, and
injected storage faults, all in one process on the local backends, checked
against invariant oracles (ref/pack consistency, CAS atomicity, webhook
soundness). **[`internal/simtest/README.md`](../../internal/simtest/README.md)**
is the authoritative description of what it exercises, the oracles, and the
determinism rules; this section covers how to run and triage it.

| What | How |
|---|---|
| PR gate (runs inside `go test ./...`) | `TestDSTFast`, `TestDSTDeterminism` |
| Reproduce a seed (test) | `DST_SEED=<seed> [DST_STEPS=<n>] go test ./internal/simtest -run 'TestDSTSeed' -v -count=1` |
| Reproduce a seed (CLI) | `go run ./cmd/dst -seed <seed> [-steps N] [-nodes N] [-v] [-keep]` |
| Nightly sweep | [`dst-nightly.yml`](../../.github/workflows/dst-nightly.yml) → `TestDSTSweep` |

A run prints one summary line and a verdict; the trace hash is a pure function
of the seed, so the CLI and the test entry point replay identically:

```console
$ go run ./cmd/dst -seed 42
seed=42 steps=150 trace=33f26256fcdf... transitions=22 deliveries=19 faults=26
ok: no invariant violations
```

Defaults: 150 steps, 3 nodes. Exit codes: `0` clean, `1` invariant violation,
`2` infrastructure failure (a broken harness, not a found bug).

### Triaging a violation

1. **Replay exactly.** A failure (nightly log or local sweep) names the seed
   and step count: `go run ./cmd/dst -seed <seed> -steps <steps> -v`. `-v`
   streams the op-by-op trace — each line shows the op, the faults it consumed,
   its outcome, and the ground-truth tip after it; the violation names the step
   and the oracle.
2. **Inspect surviving state.** Add `-keep` to retain the run directory
   (printed as `run dir: /tmp/dst-seed<seed>-...`): the SQLite DB, the blob
   directory, and each node's cache, ready for `sqlite3` / `git fsck`.
3. **Attribute the fault.** The trace's fault annotations tie every injected
   fault to the op that consumed it — e.g.
   `ref.CASRef/ambiguous(won=true err=<nil>)` is a CAS that committed under an
   ambiguous fault, the exact class the recovery oracles guard.
4. **Pin it.** Once fixed, the seed is the regression test:
   `DST_SEED=<seed> go test ./internal/simtest -run 'TestDSTSeed' -v -count=1`
   must print the same trace hash and pass.

### The nightly sweep

[`dst-nightly.yml`](../../.github/workflows/dst-nightly.yml) runs daily (03:17
UTC): 48 seeds × 300 steps under `-race`, seed base derived from the run id so
every night covers fresh seeds while staying reproducible from the log. It can
be dispatched manually with `seeds` / `base` / `steps` inputs, which map to the
`DST_SWEEP_SEEDS` / `DST_SWEEP_BASE` / `DST_STEPS` variables of `TestDSTSweep`.
There are no artifacts — everything needed to replay a failure is in the job
log.
