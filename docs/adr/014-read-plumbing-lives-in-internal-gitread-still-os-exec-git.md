# ADR-14: Read plumbing lives in `internal/gitread`, still `os/exec git`

> Extracted verbatim from [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md)
> (“Phase-3 Implementation Notes & ADRs (read API)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

All read-side plumbing (ls-tree, log, cat-file, diff, blame porcelain, grep, archive walk)
is a dedicated package of small wrappers over a repo path, each with a focused parser and
unit tests against real fixture repos built in `t.TempDir()`. API handlers stay thin:
`gitread.Materialize` runs `githttp.Hydrate` (which `SyncRefs`s the cache repo) before any
plumbing, so the ref store remains the single ref authority (ADR-1) and the bare-repo cache
is only ever a materialization. `ResolveRef` maps branch / tag / short- or full-SHA (and the
p2 ephemeral namespace) to a commit, with missing refs and unborn branches surfacing as a
typed not-found the handlers translate to the docs/03 404s (invariant 3). No go-git —
consistent with every prior phase.
