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

**Rollout.** The store transitions land first, with no callers. The SQLite
worker and its sweep follow, and hook marking comes last, after its consumer
is qualified.
