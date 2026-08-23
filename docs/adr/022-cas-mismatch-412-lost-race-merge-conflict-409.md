# ADR-22: CAS mismatch = 412, lost race / merge conflict = 409

> Extracted verbatim from [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md)
> (“Phase-4 Implementation Notes & ADRs (write API + p3 read burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

`expected_head_sha` / `expected_target_sha` / `expected_ref_sha` guard failures detected by
comparing against the current ref value return **412 `precondition_failed`**. A CAS lost at
publish time (no guard given, or the guard passed but the ref moved concurrently) and merge
conflicts return **409 `conflict`** (the merge body adds `conflict_paths` and
`merge_base_sha`). Root commits on an unborn branch report `old_sha` as 40 zeros
(invariant 2). Write successes are **200** (docs/03+04 win over openapi's 201; SDKs accept
both — docs/12 deviation 5).
