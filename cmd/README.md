# cmd

## Overview

`cmd` holds the two `package main` binaries of the `github.com/coder/openstorage` module. It has no source files of its own. Each binary composes packages from `internal/` and adds no domain logic beyond configuration, wiring, and operator commands.

- `openstorage` is the service binary. `serve` runs the API, Git, eventing, sync, and background-job stack on one HTTP listener. The other subcommands are offline operator tools.
- `dst` is a developer tool. It replays one deterministic simulation run by seed through `internal/simtest`.

## Subdirectories & Boundaries

- openstorage: [./openstorage/README.md](./openstorage/README.md) - Composition root. Resolves `OPENSTORAGE_*` environment variables and flags into a validated config (`config.go`), wires the stores and services (`wire*.go`), and serves `/api/`, `/healthz`, and Git smart HTTP at `/`, with an optional metrics listener. Subcommands: `serve`, `hook`, `keygen`, `mint`, `signing-key`, `list-packs`, `purge-repo`, `repair-refs`, `s3wal-meta`, `version`. A failure that reaches `[openstorage/main.go:fatalf]` exits `1`. A flag parse error exits `2`, because every subcommand flag set uses `flag.ExitOnError` (`[openstorage/main.go:cmdServe]` and the other subcommand entry points). The `hook` subcommand also exits `42` at two crash test knobs, only when `OPENSTORAGE_TEST_MODE=1` and `OPENSTORAGE_TEST_CRASH` is `after-reserve` or `after-s3` (`[../internal/githttp/hook.go:testMode]`, `[../internal/githttp/hook.go:EnvCrash]`). Verify with `go test ./cmd/openstorage/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`.
- dst: [./dst/README.md](./dst/README.md) - Deterministic simulation replay. Flags `-seed`, `-steps`, `-nodes`, `-backend` (`sqlite` or `s3wal`), `-v`, `-keep`. Exits `0` clean, `1` invariant violation, `2` infrastructure failure. Verify with `go test ./cmd/dst/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`. `cmd/dst` has no test files, so this command only compiles the package.
