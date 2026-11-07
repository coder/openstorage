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
- `CommandLine`, `TailBuffer` — the git command line and the stderr tail that an error quotes, bounded to 4,096 bytes and the last 64 KiB [errtext.go:CommandLine]
- `HeapLimitSupported`, `Command`, `HeapFailure` — heap cap support (Linux: `/bin/sh` wrapper with `ulimit -d`; other platforms: no cap) and heap-failure classification [heaplimit_linux.go:Command] [heaplimit_other.go:Command]

## Invariants
- `CommandLine` quotes a command of up to 4,096 bytes whole and cuts a longer one at a rune boundary, with its size. `TailBuffer` keeps the last 64 KiB [errtext.go:CommandLine]
- `NewSlots` panics unless n > 0, 0 < perKey <= n, and wait >= 0 [gitlimit.go:NewSlots]
- `Release` panics unless the key holds a slot; `RepoKey` panics on an empty repo ID [gitlimit.go:Release]
- `TryAdmitMaintenance` takes the maintenance permit first, releases it if `ReadSlots` is full, and panics on a second release call [gitlimit.go:TryAdmitMaintenance]
- `Acquire` with wait 0 returns `ErrBusy` at once when no slot is free [gitlimit.go:Acquire]
- `Output` panics if the caller already set Stdout; `CombinedOutput` panics if the caller already set Stdout or Stderr [children.go:Output] [children.go:CombinedOutput]
- `registry.start` panics if `SysProcAttr` is already set, forces `Setpgid`, and refuses new children with `ErrStopping` after stop began [children.go:start]
- `registry.stop` sends SIGTERM before SIGKILL so git can clean its lock files [children.go:stop]
- `stop` signals only groups it finds registered while it holds the registry lock, and on Linux `wait` removes a group from the registry before it reaps the leader, so a signal reaches a group whose leader still owns its ID and never one whose ID the kernel recycled (#473) [children.go:wait] [children.go:stop]
- On Linux a leader that exits after `stop` began stays an unreaped zombie until `stop` has sent its final SIGKILL, which still ends a member the leader left behind. `awaitExit` waits for the exit with `waitid(WNOWAIT)`. A member left behind by a leader that exited before `stop` began is out of reach: its group left the registry at the exit, and a member that keeps the child's output pipe open keeps `Wait` blocked until it exits [children.go:wait] [exit_linux.go:awaitExit]
- Platforms other than Linux use a reap-first wait. macOS has no wait that leaves the child unreaped: Go's `os` package does not use `waitid` there because it also returns for a stopped child (golang/go#19314). FreeBSD and NetBSD have `wait6`, which this package does not use because releases build only linux and darwin. `wait` reaps first and removes the group right after. `stop` sends no signal, SIGTERM or SIGKILL, to a group that is not registered or whose leader `os` reports reaped. A leader that exits during the grace gets no final SIGKILL there, so a member it left behind is not killed. What stays open is the race that `os.Process.Signal` has on macOS (golang/go#13987): the leader is reaped between the check and the kill [children.go:wait] [children.go:signal] [exit_other.go:awaitExit]
- The Linux wrapper keeps a stricter inherited soft limit and exits 125 if it cannot read or set the limit [heaplimit_linux.go:heapLimitWrapper]
- `HeapFailure` ignores SIGKILL and matches git diagnostics only at line start [heaplimit_linux.go:HeapFailure]
- Non-Linux `Command` asserts maxHeapBytes == 0 [heaplimit_other.go:Command]

## Side Effects
- Starts git children (through `/bin/sh` when a heap cap is set), each as its own process group [children.go:start]
- Sends SIGTERM then SIGKILL to tracked groups in `StopChildren`, polling every 10 ms until every leader has exited or the grace deadline [children.go:stop]
- Blocks one goroutine per child until its leader exits: in `waitid(WNOWAIT)` before it reaps on Linux, in `Wait` elsewhere [exit_linux.go:awaitExit] [exit_other.go:awaitExit]
- Holds mutexes on slot state and on the child registry; release closes and replaces the `freed` channel to wake waiters [gitlimit.go:Release]
- `Output` and `CombinedOutput` buffer child output in memory [children.go:Output]

## Verification
- Run: `go test ./internal/gitlimit/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
