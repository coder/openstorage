# dst

## Overview

`cmd/dst` is the replay command for deterministic simulation testing (DST). Give it a seed and it runs one simulation with `simtest.Run`. It prints a one-line result summary and every invariant violation that the run found. The command adds no simulation logic of its own. The scenario, the fault model, and the invariant checks live in [`internal/simtest`](../../internal/simtest/README.md).

Triage loop, from the doc comment in `main.go`:

```sh
go run ./cmd/dst -seed <seed> [-steps N] [-v] [-keep]
```

## Interface

- `[main.go:main]` Entry point. It parses flags, builds a `simtest.Config`, and calls `simtest.Run(cfg)`. It reads no positional arguments.
- Flags:

  | Flag | Type | Default | Meaning |
  |---|---|---|---|
  | `-seed` `[main.go:flag -seed]` | uint64 | `0` | Simulation seed (`DST_SEED` in the DST tests). Sets `Config.Seed`. |
  | `-steps` `[main.go:flag -steps]` | int | `0` | Scheduler steps. `0` uses the simtest default (150, `[internal/simtest/scenario.go:withDefaults]`). |
  | `-nodes` `[main.go:flag -nodes]` | int | `0` | Simulated nodes. `0` uses the simtest default (3). |
  | `-backend` `[main.go:flag -backend]` | string | `""` | Authority-store lane: `sqlite` or `s3wal` (`DST_REF_BACKEND`). Empty means `sqlite` (`simtest.RefBackendSQLite`). |
  | `-v` `[main.go:flag -v]` | bool | `false` | Sets `Config.Trace = os.Stdout`, so the trace prints while the run executes. |
  | `-keep` `[main.go:flag -keep]` | bool | `false` | Creates a run directory, sets `Config.Dir` and `Config.KeepDir`, and prints `run dir: <path>`. |

- Output on stdout `[main.go:main]`:
  - One summary line: `seed=… backend=… steps=… trace=<TraceHash> transitions=… deliveries=… faults=… cas-faults=… steals=… crash-steals=… push-steals=… push-crash-steals=… push-forced-rescues=…`.
  - `VIOLATION: <msg>` for each entry in `Result.Violations`, or `ok: no invariant violations` when there are none.
- Exit codes `[main.go:main]`: `0` clean run, `1` at least one invariant violation, `2` infrastructure failure (`simtest.Run` returned an error, or `os.MkdirTemp` failed).
- `[main.go:main]` `flag.Parse` uses the default `flag.ExitOnError` set. An unknown or malformed flag prints the usage message and exits `2`, and `-h` exits `0`.

## Invariants

- `[main.go:main]` Exit code `1` means only "invariant violation". `simtest.Run` reports invariant breaches in `Result.Violations`, not as an error `[internal/simtest/scenario.go:Run]`.
- `[main.go:main]` Exit code `2` does not mean only "infrastructure failure". A flag parse error also exits `2`. An unrecovered panic, such as a failed simtest `assertf` `[internal/simtest/fault.go:assertf]`, also exits `2` with a Go panic trace on stderr.
- `[internal/simtest/scenario.go:withDefaults]` `-backend` must be empty, `sqlite`, or `s3wal`. Any other value fails an `assertf` in simtest and the process panics. `main` does not validate the value itself.
- `[internal/simtest/scenario.go:withDefaults]` Steps and nodes must be positive after defaults. A negative `-steps` or `-nodes` panics through `assertf`.
- `[main.go:main]` The same seed, steps, nodes, and backend replay the same op trace as the DST tests (doc comment in `main.go`). Compare the printed `trace=` hash to confirm a replay.

## Side Effects

- `[main.go:main]` Writes the summary, the violation or `ok` lines, and the `run dir:` line to stdout. With `-v`, simtest also writes trace lines to stdout.
- `[main.go:main]` Writes `dst: infrastructure failure: <err>` or `dst: <err>` to stderr before `os.Exit(2)`.
- `[main.go:flag -keep]` Creates a temporary directory with `os.MkdirTemp("", "dst-seed<seed>-*")`. Nothing removes it. Delete it by hand after inspection.
- `[internal/simtest/scenario.go:Run]` Without `-keep`, simtest creates a run-scoped scratch directory and removes it after the run.

## Verification

- Run: `go test ./cmd/dst/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (this package has no test files, so this only compiles the package)
- Replay one seed (docs/reference/testing.md): `go run ./cmd/dst -seed <seed> [-steps N] [-nodes N] [-v] [-keep]`
