# `internal/gitlimit`

## Overview
Shared admission and process control for git subprocesses. It provides a slot pool that bounds concurrent git reads and maintenance, a registry that starts, reaps, and stops git child process groups, and a per-child heap cap on Linux through `ulimit -d`.

## Interface
- `HeavyHeapBytes` (256 MiB), `ReadSlotCount` (8), `RepoSlotCount` (2), `ReadSlotWait` (1 s), `MaintenanceShare` (2) — sizing constants [gitlimit.go:HeavyHeapBytes]
- `ErrBusy`, `ErrMemoryLimit` — no slot in time, and git stopped by its own allocation failure [gitlimit.go:ErrBusy]
- `Slots`, `NewSlots`, `(*Slots).Acquire`, `TryAcquire`, `Release`, `InUse` — slot pool with a per-key cap [gitlimit.go:Slots]
- `RepoKey` — per-repository slot key from a canonical repo ID [gitlimit.go:RepoKey]
- `ReadSlots` — process-wide pool shared by JSON git reads and admitted maintenance [gitlimit.go:ReadSlots]
- `TryAdmitMaintenance` — non-blocking admission of one maintenance job; returns a release func [gitlimit.go:TryAdmitMaintenance]
- `ErrStopping`, `Start`, `Wait`, `Run`, `Output`, `CombinedOutput`, `Stopping`, `StopChildren` — tracked git child lifecycle [children.go:Start]
- `HeapLimitSupported`, `Command`, `HeapFailure` — heap cap support (Linux: `/bin/sh` wrapper with `ulimit -d`; other platforms: no cap) and heap-failure classification [heaplimit_linux.go:Command] [heaplimit_other.go:Command]

## Invariants
- `NewSlots` panics unless n > 0, 0 < perKey <= n, and wait >= 0 [gitlimit.go:NewSlots]
- `Release` panics unless the key holds a slot; `RepoKey` panics on an empty repo ID [gitlimit.go:Release]
- `TryAdmitMaintenance` takes the maintenance permit first, releases it if `ReadSlots` is full, and panics on a second release call [gitlimit.go:TryAdmitMaintenance]
- `Acquire` with wait 0 returns `ErrBusy` at once when no slot is free [gitlimit.go:Acquire]
- `Output` panics if the caller already set Stdout; `CombinedOutput` panics if the caller already set Stdout or Stderr [children.go:Output] [children.go:CombinedOutput]
- `registry.start` panics if `SysProcAttr` is already set, forces `Setpgid`, and refuses new children with `ErrStopping` after stop began [children.go:start]
- `registry.stop` sends SIGTERM before SIGKILL so git can clean its lock files [children.go:stop]
- `registry` signals only a group whose leader it has not reaped: `wait` leaves the registry before it reaps, and `stop` signals only groups it finds registered while it holds the lock, so a signal cannot reach a group whose ID the kernel recycled (#473) [children.go:wait] [children.go:stop]
- On Linux a leader that exits after `stop` began stays an unreaped zombie until `stop` has sent its final SIGKILL, which still ends a member the leader left behind. Other platforms reap first and `stop` no longer signals that group [children.go:wait] [exit_linux.go:awaitExit]
- The Linux wrapper keeps a stricter inherited soft limit and exits 125 if it cannot read or set the limit [heaplimit_linux.go:heapLimitWrapper]
- `HeapFailure` ignores SIGKILL and matches git diagnostics only at line start [heaplimit_linux.go:HeapFailure]
- Non-Linux `Command` asserts maxHeapBytes == 0 [heaplimit_other.go:Command]

## Side Effects
- Starts git children (through `/bin/sh` when a heap cap is set), each as its own process group [children.go:start]
- Sends SIGTERM then SIGKILL to tracked groups in `StopChildren`, polling every 10 ms until every leader has exited or the grace deadline [children.go:stop]
- Blocks one goroutine per child in `waitid(WNOWAIT)` until the leader exits, before it reaps (Linux) [exit_linux.go:awaitExit]
- Holds mutexes on slot state and on the child registry; release closes and replaces the `freed` channel to wake waiters [gitlimit.go:Release]
- `Output` and `CombinedOutput` buffer child output in memory [children.go:Output]

## Verification
- Run: `go test ./internal/gitlimit/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
