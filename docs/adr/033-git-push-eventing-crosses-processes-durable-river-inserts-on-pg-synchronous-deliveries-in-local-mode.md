# ADR-33: Git-push eventing crosses processes — durable River inserts on pg, synchronous deliveries in local mode

> Extracted verbatim from [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md)
> (“Phase-5 Implementation Notes & ADRs (eventing, upstream sync, credentials, fork)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

Git pushes mutate refs in the `openstorage hook` subprocess (the pre-receive/update
re-exec), not in the serve process, so ADR-28's CASRef decorator needs a queue reachable
*across processes* there. Decision: **queue construction dispatches on the metadata DB
URL scheme** (the same dispatch as `meta.Open`/`ref.Open`), with asymmetric semantics:

- **Postgres:** the hook builds an insert-only River client
  (`jobs.NewRiverInsertOnly` — no migrations; hooks never run DDL) and inserts durable
  delivery/sync jobs that the serve process's full River client works. Serve owns the
  schema: `jobs.NewRiver` runs `rivermigrate` at startup, and sync jobs are unique per
  repo across active job states (`river.UniqueOpts`), so duplicate enqueues collapse —
  the durable twin of `InProcessQueue`'s coalescing (invariant 11).
- **Local mode:** there is no shared durable queue, so the hook delivers webhooks
  *synchronously* with bounded attempts (3) before the hook exits — the subprocess IS
  the queue — and git-surface pushes do **not** schedule upstream syncs (API writes
  still do). Accepted gap, consistent with ADR-27's durability trade-off: a missed sync
  re-enqueues on the next API write or `pull-upstream`. Superseded for SQLite by
  [ADR-50](050-local-scheduling-state-is-row-owned.md): the hook records durable
  intent in the schedule row. The gap remains for S3WAL (#335).

Failure stance is fail-open: if the hook cannot open the meta store or build the queue,
eventing is disabled for that push rather than the push denied — same reasoning as
ADR-28, un-notified is recoverable while un-pushed work is a user-visible outage.

**Amendment: a hook without eventing refuses intent.** The forward intent now
rides the ref update itself: on PostgreSQL the ref CAS records it
([ADR-44](044-outbound-push-forwarding-rides-a-separate-push-claim-lane.md)), and on
the local schedule the hook records it before the CAS
([ADR-50](050-local-scheduling-state-is-row-owned.md)). An eventless push would land
with no intent and no events, and a failed step does not prove that the CAS would fail
too. So when the hook cannot build the secrets codec, open the meta store, or build
the River insert-only client, it refuses every update that would record forward
intent before its CAS, and the client can push again. Without the store the hook
cannot tell whether the repository has an upstream link, so this includes an update
to a repository without one. Ephemeral updates, a sync run's own loopback push, and
a caller whose token may not forward record no intent, and they keep the fail-open
stance. The pusher sees a fixed refusal that names only the failed step: git relays
the hook's output to the pusher, and a store error names the database host, user and
name, or the S3 endpoint, so the hook logs no store error. serve logs a store error
only when its own connection fails, so when only the hook's connection fails, neither
the pusher nor the operator sees the cause.
