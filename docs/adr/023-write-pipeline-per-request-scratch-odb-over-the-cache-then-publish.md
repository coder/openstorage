# ADR-23: Write pipeline = per-request scratch odb over the cache, then publish

> Extracted verbatim from [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md)
> (“Phase-4 Implementation Notes & ADRs (write API + p3 read burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

Every write materializes the cache repo read-only (alternates), creates new objects in a
per-request scratch object directory (`GIT_OBJECT_DIRECTORY` +
`GIT_ALTERNATE_OBJECT_DIRECTORIES`), and publishes in the same order as the push hook:
`git pack-objects` over the new-tip reachability frontier, `blob.UploadFile(PackKey(...))`,
`RegisterPacks`, `CASRef`. A lost CAS leaves only an unreferenced immutable pack — never a
dangling ref. Reachability-based packing also publishes ephemeral→persistent promotions on
merge for free (`promoted_commits`). Merge preview (invariant 6) runs entirely in the
scratch odb and deletes it per request, so preview can never mutate stores; the merge engine
is `git merge-tree --write-tree` (requires git ≥ 2.38, asserted at first use).
