# `internal/testutil/heldport`

## Overview
Test helper that keeps a test server's TCP port reserved while the server is down. A test can then close a server's listener (its dials are refused, a transient failure for the sync worker) and listen on the same address again, and no other process on the host can take the port in between and answer in its place [heldport.go:Listen].

## Interface
- `Listen` — listens on a fresh IPv4 loopback port and holds the port until the test ends [heldport.go:Listen]
- `Relisten` — listens again on an address that `Listen` holds [heldport.go:Relisten]

## Invariants
- Both listeners set `SO_REUSEPORT`, and a second socket with `SO_REUSEPORT` stays bound to the port without listening until the test's cleanup [heldport.go:listen] [heldport.go:hold]
- While no listener is open, a dial to the port is refused, and a socket without `SO_REUSEPORT` cannot bind it [heldport_test.go:TestRelistenAfterClose]
- Every setup error fails the test [heldport.go:hold]

## Side Effects
- Opens one bound socket per `Listen` call; the test's cleanup closes it [heldport.go:hold]

## Verification
- Run: `go test ./internal/testutil/heldport/... -count=1 -race`
