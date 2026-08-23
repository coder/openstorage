# ADR-28: Push events are emitted by a CASRef decorator, not by handlers

> Extracted verbatim from [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md)
> (“Phase-5 Implementation Notes & ADRs (eventing, upstream sync, credentials, fork)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

Every ref mutation on this server — API commits, merges, branch/tag/notes writes, git
push via the receive hook, and the sync worker's loopback pushes — funnels through
`ref.RefStore.CASRef`. Decision: **`push` events (docs/01 invariant 7) are emitted by a
`RefStore` decorator (`events.NewRefStore`) wrapping the ref authority**, not by
individual handlers. The decorator fires only on *won*, *non-ephemeral* CAS flips, builds
the payload from a `RepoSource` lookup (repo url, customer id), fans out one delivery per
registered webhook, and optionally pokes `EnqueueSync` (pushes to a repo with an upstream
schedule a sync — invariant 11). Rationale: a single choke point cannot be forgotten by
the next write endpoint; handler-side emission would already have five call sites to keep
honest. Enqueue failures are logged, never propagated — the ref flip has already
committed, and un-notified is recoverable while un-flipped is not.
