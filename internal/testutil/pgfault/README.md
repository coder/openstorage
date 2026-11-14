# `internal/testutil/pgfault`

## Overview
Test helper that relays PostgreSQL connections through a local TCP proxy and injects one fault into the next COMMIT, or into the reply to one named statement. Tests use it to lose a COMMIT's reply after the server committed, to drop the COMMIT so the server aborts, to hold the transaction open, to commit it late, or to lose the reply to a statement, and then check how a store resolves the outcome it never heard [pgfault.go:Proxy].

## Interface
- `Start` — listens on `127.0.0.1`, relays to the host of a database URL, and returns the proxy and the URL rewritten to go through it [pgfault.go:Start]
- `Proxy.Arm` — arms one `Fault` for the next COMMIT on any relayed connection, with an optional callback that runs once, before the proxy closes any connection [pgfault.go:Arm]
- `Proxy.Fired` — reports whether the armed fault took effect [pgfault.go:Fired]
- `Proxy.ArmStatement` — arms `DropStatementReply` for the next statement whose text contains a marker: the proxy swallows the server's reply to that statement and closes both sides. pgx sends an unprepared statement as a Parse chunk and then a Bind and Execute chunk, so the proxy waits for the chunk with the text and swallows the reply to the next one; use a connection that has not run the statement before [pgfault.go:ArmStatement]
- `Fault` values `DropReply`, `DropCommit`, `HoldCommit`, `CutAll`, `DelayCommit`, `DropStatementReply` [pgfault.go:Fault]
- `CommitDelay` — how long `DelayCommit` holds the COMMIT before it relays it (300 ms) [pgfault.go:CommitDelay]

## Invariants
- Only a client read that is exactly pgx's simple-protocol `commit` Query message triggers a COMMIT fault, and one arm triggers at most one fault [pgfault.go:commitMsg] [pgfault.go:take]
- One fault is armed at a time. A COMMIT on any relayed connection takes an armed `DropStatementReply` and drops it, so arm it only while no COMMIT is in flight; a test that arms a COMMIT fault after the statement text went by loses that COMMIT fault, and its `Fired` check fails [pgfault.go:relay]
- Every fault marks itself fired, then runs the callback, before the proxy closes any connection, so `Fired` is true once the client sees the fault, also when the callback ends the client's context. `DropReply` runs the callback after the server's reply arrives [pgfault.go:relay] [pgfault.go:fire]
- `Arm` panics on an unknown fault, `DropStatementReply` included: arm it with `ArmStatement` [pgfault.go:Arm]

## Side Effects
- Opens a TCP listener and one server connection per client connection; the test's cleanup closes all of them [pgfault.go:Start] [pgfault.go:closeAll]

## Verification
- The package has no tests of its own. The PostgreSQL tests that use it exercise it: `go test ./internal/ref/... ./cmd/openstorage/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` (needs Docker)
