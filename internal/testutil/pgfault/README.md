# `internal/testutil/pgfault`

## Overview
Test helper that relays PostgreSQL connections through a local TCP proxy and injects one fault into the next COMMIT. Tests use it to lose a COMMIT's reply after the server committed, to drop the COMMIT so the server aborts, to hold the transaction open, or to commit it late, and then check how a store resolves the outcome it never heard [pgfault.go:Proxy].

## Interface
- `Start` — listens on `127.0.0.1`, relays to the host of a database URL, and returns the proxy and the URL rewritten to go through it [pgfault.go:Start]
- `Proxy.Arm` — arms one `Fault` for the next COMMIT on any relayed connection, with an optional callback that runs once, before the proxy closes any connection [pgfault.go:Arm]
- `Proxy.Fired` — reports whether the armed fault took effect [pgfault.go:Fired]
- `Fault` values `DropReply`, `DropCommit`, `HoldCommit`, `CutAll`, `DelayCommit` [pgfault.go:Fault]
- `CommitDelay` — how long `DelayCommit` holds the COMMIT before it relays it (300 ms) [pgfault.go:CommitDelay]

## Invariants
- Only a client read that is exactly pgx's simple-protocol `commit` Query message triggers the fault, and one arm triggers at most one fault [pgfault.go:commitMsg] [pgfault.go:take]
- Every fault marks itself fired, and runs the callback, before the proxy closes any connection, so `Fired` is true once the client sees the fault. `DropReply` runs the callback after the server's reply arrives [pgfault.go:relay] [pgfault.go:fire]
- `Arm` panics on an unknown fault [pgfault.go:Arm]

## Side Effects
- Opens a TCP listener and one server connection per client connection; the test's cleanup closes all of them [pgfault.go:Start] [pgfault.go:closeAll]

## Verification
- The package has no tests of its own. The PostgreSQL tests that use it exercise it: `go test ./internal/ref/... ./cmd/openstorage/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (needs Docker)
