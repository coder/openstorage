# ADR-11: Ephemeral namespace policy quirk

> Extracted verbatim from [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md)
> (“Phase-2 Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** The branch-protection guide documents that ref policies written for
persistent branches do not automatically police ephemeral pushes.

**Decision.** When a push targets a `+ephemeral` repo, the hook matches policies against
`refs/namespaces/ephemeral/<refname>` instead of `<refname>`; a rule for `refs/heads/main`
therefore does not fire on an ephemeral `main` push unless the token uses `*` or an
explicit namespaced pattern. `+import` and persistent pushes match verbatim — the guide
documents the quirk for ephemeral only, so it is not extrapolated to `+import`. Pattern
syntax is tightened to the upstream contract at the same time: exact ref name, standalone
`*`, or a single trailing `*` after a `/` (prefix match); no mid-pattern globs. The
deprecated repo-wide `ops` claim is enforced as a fold-into-`*` fallback (`refs` and `ops`
are not additive), applied once at parse time.
