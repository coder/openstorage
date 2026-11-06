# `internal/testutil/heldport`

## Overview
Test helper that keeps a test server's TCP port reserved while the server is down, on Linux only. A test can then close a server's listener (its dials are refused, a transient failure for the sync worker) and listen on the same address again. In between, no other server can take the port and answer in its place, except a process of the same user that also sets `SO_REUSEPORT` and binds that exact port [heldport.go:Listen] [heldport_linux.go:hold]. Off Linux, `Listen` and `Relisten` are plain listeners and hold nothing: on macOS a bound socket that does not listen can receive the port's connections, so a held port would stop the server [heldport_other.go:hold].

## Interface
- `Listen` — listens on a fresh IPv4 loopback port; on Linux it also holds the port until the test ends [heldport.go:Listen]
- `Relisten` — listens again on an address that `Listen` returned [heldport.go:Relisten]

## Invariants
- On Linux, both listeners set `SO_REUSEPORT`, and a second socket with `SO_REUSEPORT` stays bound to the port without listening until the test's cleanup [heldport_linux.go:listenControl] [heldport_linux.go:hold]
- The held socket is close-on-exec, so no child process started after the mark inherits it [heldport_linux.go:hold] [heldport_linux_test.go:TestHeldSocketIsCloseOnExec]
- `ForkLock` is held from the socket's creation to that mark, so no child process started in between inherits it either. No test covers this race [heldport_linux.go:hold]
- On Linux, while no listener is open, a dial to the port is refused, and a socket without `SO_REUSEPORT` cannot bind it [heldport_linux_test.go:TestRelistenAfterClose]
- Off Linux, listeners set no socket option and no port is held [heldport_other.go:listenControl] [heldport_other.go:hold]
- Every setup error fails the test [heldport.go:listen] [heldport_linux.go:hold]

## Side Effects
- On Linux, opens one bound socket per `Listen` call; the test's cleanup closes it [heldport_linux.go:hold]

## Verification
- Run: `go test ./internal/testutil/heldport/... -count=1 -race` (the tests run on Linux only)
