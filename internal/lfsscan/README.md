# `internal/lfsscan`

## Overview
Lenient Git LFS pointer decoder plus a bounded walk that runs three `git` subprocesses over a repo's refs to collect the sha256 OIDs that reachable pointer blobs reference. On Linux each walk subprocess gets a 2 GiB RLIMIT_DATA cap.

## Interface
- `LFSPointerMaxSize` — 1024, the prefix length decoded as a pointer [pointer.go:LFSPointerMaxSize]
- `ParseLFSPointer` — decodes bytes as an LFS pointer and returns the OID and ok [pointer.go:ParseLFSPointer]
- `ErrLFSScanBudget` — returned when the walk charges more work units than the budget [scan.go:ErrLFSScanBudget]
- `ReachableLFSOIDs` — walks all refs and returns the OID set, the units charged, and an error [scan.go:ReachableLFSOIDs]

## Invariants
- `ReachableLFSOIDs` panics if repoPath is empty or the budget is not positive [scan.go:ReachableLFSOIDs]
- `ParseLFSPointer` rejects data over `LFSPointerMaxSize` or containing NUL, needs a known version line and an `oid sha256:` line in any order, and needs a 64-char lowercase hex OID; it never panics on arbitrary input [pointer.go:ParseLFSPointer]
- Each object charges 1 + size/4096 units; exceeding the budget returns `ErrLFSScanBudget` [scan.go:ReachableLFSOIDs]
- An invalid rev-list OID, a `missing` batch-check line, a malformed size or header, or a failed subprocess fails the walk with no OID set [scan.go:ReachableLFSOIDs]
- Every git call runs with `GIT_NO_REPLACE_OBJECTS=1` (`gitlimit.NoReplaceObjects`), and rev-list runs unfiltered so every object is charged [scan.go:ReachableLFSOIDs]

## Side Effects
- Runs `git rev-list --objects --no-object-names --all`, `git cat-file --batch-check`, and `git cat-file --batch` in repoPath, each holding the repository's `gitlimit` lease until it exits [scan.go:ReachableLFSOIDs] [lease_test.go:TestReachableLFSOIDsLeaseFollowsEveryStage]
- Starts three goroutines through errgroup connected by pipes, kills started subprocesses on a later start failure, and reaps all with deferred Wait [scan.go:ReachableLFSOIDs]
- Calls `unix.Prlimit` on each subprocess PID on Linux; no-op elsewhere [memlimit_linux.go:limitGitHeap] [memlimit_other.go:limitGitHeap]

## Verification
- Run: `go test ./internal/lfsscan/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
