# ADR-1: Postgres is the ref authority

> Extracted verbatim from [`08-spike-implementation-notes.md`](../archive/phases/08-spike-implementation-notes.md)
> (“Spike Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** Multiple stateless nodes must agree on refs. On-disk bare repos cannot provide
cross-node atomicity, and replicating git's own ref store adds a consensus problem the
metadata DB already solves.

**Decision.** Refs live in a PG table `(repo, name, sha)`. Every ref update is a
compare-and-swap executed from git's `update` hook as a **single SQL statement**
(`INSERT … ON CONFLICT DO NOTHING` for create, conditional `UPDATE`/`DELETE` otherwise).
Bare repos on disk are read-through caches only: `Hydrate` rebuilds them from PG + S3, and
`SyncRefs` forces the local ref store to match PG exactly (PG wins, local strays deleted).

**Consequences.** A partial or broken ref state is impossible by construction — refs only
ever change inside those single statements. Concurrent pushes resolve to one winner with
git-native per-ref error reporting (hook exit ≠ 0 → `ng`), and git only updates the cache
ref for winners. The cost: every advertisement must sync refs from PG first, and the local
git ref store must never be trusted as truth.
