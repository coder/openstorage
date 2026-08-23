# ADR-40: River migrations run under a session-level advisory lock

> Extracted verbatim from [`16-phase7-implementation-notes.md`](../archive/phases/16-phase7-implementation-notes.md)
> (“Phase-7 Implementation Notes & ADRs (deployment packaging + p6 burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

Concurrent from-scratch boot of the two replicas surfaced a latent race:
`rivermigrate` runs its DDL with no locking of its own, so two fresh nodes migrating
simultaneously collide on creating river's tables (SQLSTATE 23505 duplicate key), and
the loser exits. Decision: **`jobs.NewRiver` serializes migrations under
`pg_advisory_lock(0x5249564552)` (“RIVER”), mirroring the meta/ref `InitSchema` locks —
every startup DDL path must be advisory-lock-serialized, including third-party ones.**

- **Session-level lock on a dedicated pool conn**, not `pg_advisory_xact_lock`:
  the meta/ref stores lock inside their own DDL transaction, but
  `rivermigrate.Migrate` manages its transactions internally, so there is no caller
  transaction to scope a xact lock to.
- **The unlock must survive context cancellation.** A canceled ctx is precisely the
  case where `Migrate` just failed and the lock is still held; the unlock runs on a
  `context.WithoutCancel` 10-second deadline.
- **A failed unlock destroys the connection instead of pooling it.** Releasing a conn
  that still holds the session lock back into the pool poisons it — the next boot's
  `pg_advisory_lock` on that pooled conn blocks forever. On unlock failure the
  underlying conn is closed so `Release` discards it and the server drops the
  advisory lock with the session.
