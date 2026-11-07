# `internal/gitread`

## Overview
Read-side git plumbing for the `git:read` API. It hydrates a cache repo, resolves refs, and runs git children under per-request budgets to serve trees, blobs, logs, blame, diff, grep, archives, tags, and notes. The package is HTTP-agnostic and returns `ErrNotFound`-wrapped errors for missing data.

## Interface
- `ErrNotFound`, `EmptyTreeSHA` [gitread.go:ErrNotFound]
- `Materialize` — hydrates the cache bare repo through `githttp.Hydrate` and returns its path [gitread.go:Materialize]
- `ResolveRef` — resolves a branch, tag, full ref, or hex spec to a commit SHA. With `ephemeral` true it tries only the ephemeral-namespace name `auth.EphemeralRefName(spec)` and the hex spec, and skips the `refs/heads/`, `refs/tags/`, and full-ref candidates [gitread.go:ResolveRef]
- Budgets: `Budget`, `NewBudget`, `WithLimits`, `WithBudget`, `BudgetFrom`, `Close`, `Limits`, `DefaultLimits`, `Slots`, `NewSlots`, `Class` (`Light`, `Heavy`), `Cmd`, `Run`, `CheckBlobSize`, `IsBudgetError` [budget.go:NewBudget]
- Limit errors: `ErrBusy`, `ErrTimeBudget`, `ErrOutputLimit`, `ErrBlobTooLarge`, `ErrMemoryLimit`, `ErrStop`; caps `HeavyHeapBytes`, `LightHeapBytes`, `DefaultOutputBytes`, `MaxBlobBytes` [budget.go:ErrBusy]
- `HeapLimitSupported` [heaplimit.go:HeapLimitSupported]
- Trees and blobs: `TreeEntry`, `EntryBlob`, `EntryTree`, `EntrySymlink`, `EntrySubmodule`, `LsTree`, `LsTreePage`, `ErrUnsortedTree`, `BlobInfo`, `StatBlob`, `CatBlobTo`, `ReadBlob` [tree.go:LsTree]
- History: `Commit`, `LogOptions`, `Log`, `CommitsBySHA`, `CommitDetail`, `GetCommit`, `LastCommits`, `ErrTooManyLastCommits` [log.go:Log] [lastcommits.go:LastCommits]
- `BlameLine`, `Blame` [blame.go:Blame]
- `FileDiff`, `DiffStats`, `DiffOptions`, `Diff` [diff.go:Diff]
- `GrepLine`, `GrepFile`, `GrepOptions`, `GrepLineMatch`, `GrepLineContext`, `Grep` [grep.go:Grep]
- `ArchiveOptions`, `PreparedArchive`, `PrepareArchive`, `(*PreparedArchive).Stream`, `Archive`, `ValidArchivePrefix`, `MatchGlob` [archive.go:PrepareArchive]
- `Tag`, `PeeledTags`, `Note` [refs.go:PeeledTags] [notes.go:Note]

## Invariants
- `assertf` panics on programming errors such as an empty repo path or a non-SHA commit argument [gitread.go:assertf]
- `ResolveRef` builds candidates only from `check-ref-format`-valid names, admits raw revisions only if they match `hexRevRe`, and returns budget errors unwrapped [gitread.go:ResolveRef]
- `IsBudgetError` errors must not be reported as not-found [budget.go:IsBudgetError]
- `(*Budget).start` asserts the budget is open and returns `ErrTimeBudget` past the deadline; `limitedBuffer.Write` kills git once output is exhausted [budget.go:Budget.start]
- `Run` never wraps `ExitError` for limit outcomes [budget.go:Run]
- `Run`'s errors quote at most 4,096 bytes of the command and the last 64 KiB of git's stderr (`gitlimit.CommandLine`, `gitlimit.TailBuffer`) [budget.go:Run]
- `childEnv` forces `LC_MESSAGES=C` and keeps `LC_CTYPE`/`LC_COLLATE` [budget.go:childEnv]
- `LsTree` passes paths as `:(literal)` pathspecs; `parseLsTreeRecord` rejects malformed records; `LsTreePage` returns `ErrUnsortedTree` on out-of-order output [tree.go:LsTreePage]
- `PrepareArchive` fails before any byte is written when a kept blob exceeds the size limit [archive.go:PrepareArchive]
- `ValidArchivePrefix` rejects NUL, backslash, a leading slash, and `.`/`..` segments [archive.go:ValidArchivePrefix]
- `mergeNumstat` and `mergePatches` error on any mismatch between the three diff passes [diff.go:mergeNumstat]
- `LastCommits` fails with `ErrTooManyLastCommits` past `maxDistinct`; spool lines over 1 MiB or spool over 256 MiB fail with `ErrOutputLimit` [lastcommits.go:spoolWriter]
- `Note` maps git exit 1 to `ErrNotFound`; `PeeledTags` skips non-commit tags [notes.go:Note]

## Side Effects
- `Materialize` writes the cache repo under `cacheDir`, reading packs from the blob store and refs from the ref store [gitread.go:Materialize]
- Runs git subprocesses through `gitlimit.Run`: rev-parse, check-ref-format, ls-tree, cat-file, log, rev-list, diff-tree, diff, blame, grep, for-each-ref, notes [budget.go:Run]
- `(*Budget).start` takes an admission slot that `Close` releases [budget.go:Close]
- `Stream` and `LastCommits` create and remove temp spool files; `LastCommits` runs a diff-tree goroutine over a pipe [lastcommits.go:LastCommits]
- `Stream` writes tar.gz output to the caller's writer [archive.go:Stream]

## Verification
- Run: `go test ./internal/gitread/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
