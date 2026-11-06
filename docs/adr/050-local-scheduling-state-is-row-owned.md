# ADR-50: Local scheduling state is row-owned

> Decision record for issue #266 (schedule authorized Git-push work in local
> backends). Context: closed PR #271 tried to consume hook-written intent with
> a live scanner and process-local retry timers. Its review findings all came
> from one cause: "may this lane run now?" had two sources of truth, the
> durable pending marker and an in-memory timer map, written, consumed, and
> read in separate steps. The accepted design and its review record are on
> issue #266.

**The lane row owns the schedule.** For each repository lane (push or pull),
four things live only in that lane's `repo_upstreams` row:

- whether work is outstanding (the pending level);
- when it may next start (`push_due`, `sync_due`);
- its retry position (`push_attempt`);
- who is running it (the claim holder and expiry).

They change only through single-row transactions, and every transition that
ends a run is guarded by that run's claim holder. A run acts only on what its
own successful claim took from the row. Anything held in process memory
(queue entries, sweep results) is a hint: it can cause a claim attempt, but it
never admits, delays, or cancels work.

**SQLite only.** The three columns are added by the existing idempotent
upgrade slice. Their default, 0, means "first attempt, due now", so pending
work recorded before an upgrade is due at once. PostgreSQL keeps River. S3WAL
keeps its current behavior: its frozen v1 metadata format cannot take new
fields without a format change (issue #335).

**Transitions** (`meta.LocalScheduleStore`, an optional interface that only
`*meta.SQLite` implements):

| Transition | Effect |
|---|---|
| `RequestPush` (unchanged) | Raises the pending level. Due time and attempt are untouched, so a hook request during a backoff joins it, as on PostgreSQL. |
| `RequestPushNow` | `RequestPush`, plus due now. The attempt is kept. Explicit API requests use it; one made while a run holds the lane stays due now even if that run ends in `retry` or `restore`. |
| `ClaimDuePush` / `ClaimDueSync` | Claims only a lane with durable work: a due pending level and no live claim, or an expired claim. A refused claim writes nothing. The attempt stays in the row while the run executes. |
| `FinishPushOutcome` / `FinishSyncOutcome` | Ends the run and schedules what follows in the same write: `done` resets attempt and due time (a successful forward can also mark its mirror-back pull); `retry` re-marks the run's level, advances the attempt, and sets the due time; `restore` re-marks with the same attempt and counts no run. |
| `ListLocalWork` | Lists lanes whose claim guard holds now, for live repositories of the served organization. |

**Consequences.**

- A retry has no separate registration step. The write that ends a failed
  run also records the retry, so no claim can see the pending level without
  its due time and attempt.
- If an end-of-run write fails, the claim is left in place. It expires with
  its lease, and the next claim steals it with attempt and force intact.
- Retries survive restarts, because the schedule is durable.
- The retry policy itself is unchanged: only unreachable upstreams retry, one
  initial attempt plus five retries, after 1, 2, 4, 8, and 16 minutes.
- Callers must record a durable request before they enqueue local work;
  conditional claims ignore queue entries that have no durable work behind
  them.
- Shutdown keeps requests (issue #342). Before `serve` stops its listener,
  the scheduler stops starting runs and waits a bounded time for runs in
  flight, which push to that listener. It then cancels the rest and waits
  briefly for them to end. A run cancelled this way ends with `restore`,
  emits no `repo.sync.failed`, and its request runs in the next process. Correctness does not depend on the
  wait: the run is classified by its cancellation cause, which is set before
  the listener stops.

**Consumers.** On SQLite the serve process runs a sweep every second that
turns due lanes into runs; it replaces boot recovery and the process-local
retry timers. Queue enqueues only offer runs: every caller records its
request first. API `push-upstream` records with `RequestPushNow`; implicit
forward pokes and git-surface hooks use `RequestPush` and `RequestSync`.

**Git-surface hooks** (the `openstorage hook` subprocess) record intent with
one row write after the authority CAS, behind the API's gates: forwarding
needs `git:read` and no ref-policy operations, pulling needs no ref-policy
operations, a sync run's own loopback push records nothing, and a live pull
claim suppresses pull marking (as on PostgreSQL). The hook marks before
receive-pack publishes the cache ref, so a local run waits for the claimed
repository's publication (`githttp.WaitForPushPublication`) after its claim
and before its snapshot and `repo.sync.started` event. A failed wait ends
the run with `restore`, due again after a few seconds.

**Amendment: forward intent before the CAS.** A model check of this
schedule (`.agents/skills/verify-lane-schedule`) found two ways to lose a
forward with the order above. A hook that dies, or whose `RequestPush`
fails, after its authority CAS leaves an accepted push that no run
forwards. And a run that passed its publication wait could snapshot a ref
that a later push then held, while the cache still had a value older than
the authority (after a REST write, or a push that died after its CAS), and
end `done` without forwarding the newer value. Now a forwarding push's hook
records `RequestPush` before its authority CAS, after the ref's push hold is
in place. A failed write, a failed read of the repository's upstream link,
or a metadata store the hook cannot open refuses the ref update, with a
fixed message that names no store detail ([ADR-33](033-git-push-eventing-crosses-processes-durable-river-inserts-on-pg-synchronous-deliveries-in-local-mode.md)'s
amendment). Ephemeral updates, a sync run's own loopback push, and a caller
whose token may not forward record no intent, and this step does not
refuse them. The intent after the
CAS stays, and still does the pull marking. A local push run's loopback
fetch fails with HTTP 423 when its hydrate would skip a push-held ref whose
cached value is older than the authority, and the run ends with `restore`,
as after a failed publication wait.

**Rollout.** The store transitions landed first, with no callers (#336). The
SQLite worker and its sweep followed (#341), and hook marking came last,
after its consumer was qualified.
