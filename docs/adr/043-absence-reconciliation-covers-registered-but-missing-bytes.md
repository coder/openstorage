# ADR-43: Absence reconciliation covers registered-but-missing bytes

> Decision record for issue #101 (split from PR #61, security round 66).

While a registered blob's bytes are physically absent, the quota admission
floor `max(0, listing − counter)` is deflated by the absent size: the
counter includes bytes the listing lacks, hiding up to `min(absent, legacy)`
rowless legacy bytes from admission. Restore-time covers and generation
bumps (PR #61 rounds 54–66) protect every admission from the restore
*onward*; a write settled *before* the restore is unreachable by
construction at restore time. The only remaining entry into that state is
storage-layer data loss — every software deletion path quarantines first —
so the fix is a watchdog, not an admission-path change: a bounded
registry-vs-storage reconciliation that makes the loss visible to admission
within one pass.

The scan direction is deliberately the sweeper's inverse. The sweeper walks
storage listings and point-probes the registry (`HasPack`,
`HasLFSObject`); reconciliation walks registry rows (`lfs_objects`,
`packs`) and point-`HEAD`s storage. Enumeration is a flat keyset cursor
over each table's primary key — `(repo, key)` and `(repo, id)` — so every
page is one index range scan, budget-limited passes park the cursor and
resume strictly after it, and the rotation wraps at the end of the keyspace
(the reclaimer/purger discipline). Cursors are in-memory per process;
issue #121's fleet-persistent cursor work can store the two pairs as-is. A
lost cursor only delays convergence: verdicts are recomputed, never trusted
from state.

Classification precedes flagging (ADR-41): an LFS row absent under its
registered spelling probes the canonical/legacy twin first. FULLY
twin-present rows are physically listed — the floor is not deflated — and
download-time restore (PR #104) heals the spelling through the
digest-verified adopt pipeline, so they are reported
(`lfs_rows_twin_only`), never charged. Only rows absent under BOTH
spellings are full loss — and the point check compares sizes, not mere
existence (Codex round 4, security): a blob present but SHORTER than its
registered size (truncated, or surviving only as a short twin) deflates
the floor by the deficit, which is covered and tracked exactly like
partial absence.

Discovery is rotation-bounded, but refresh is not (Codex round 1): covers
are TTL'd, and on a registry larger than one pass's budget the rotation
returns to a row less often than its cover survives — so every discovered
loss joins a per-process known-absent set whose bytes AGGREGATE into one
cover reservation row per affected base repository (Codex round 4):
per-row covers would scale renewal work with the loss population and
collide with the per-base active-reservation-row cap, so a big enough
incident would either outrun the renewal cycle or be denied outright. The
refresh phase runs FIRST each pass, renewal before re-verification:
renewing every base aggregate costs one work unit per REPOSITORY, and the
tracked-base population is capped at exactly that renewal allocation
(Codex round 5) — tracked ⇒ renewed every pass, by construction. A shrink
(heal, retirement, fold, purge) rotates the aggregate's reservation key:
the ledger's same-key upsert keeps GREATEST(old, new) and refreshes the
TTL, so renewing a smaller total under the old key would pin healed bytes
reserved forever — the fresh key charges the exact total and the old row
expires within one TTL (the deliberate tail every cover has). Round-robin
re-verification (`HasPack`/`HasLFSObject` + stats) only SHRINKS covers,
so its backlog delays convergence, never visibility. Losses are
storage-incident-scaled, never attacker-mintable; beyond the base
capacity or the 100,000-row memory bound, overflow gets one-shot per-row
covers renewed on rotation revisits (`absent_untracked`). The set is in-memory like the cursors: a restart loses it and
the rotation rediscovers — convergence delay, never corruption. Renewal
timing is enforced end to end (Codex round 3, security): covers renew at
pass starts, periodic ticks that fire mid-pass coalesce (River uniqueness
includes the running state), and a pass's wall clock is capped at a
quarter of the TTL while configuration rejects intervals above half the
TTL — so the gap between one cover's renewals (pass + interval + next
refresh phase) stays within the TTL even under degraded blob stores. The rotation leader
alternates between the two registries each pass, since a twin probe can
overdraw the leader's allocation by a unit; a configuration floor
(`MinAbsenceReconcileBudget`) keeps budgets out of single-row-overdraw
territory.

Known residual (issue #125, with #121): on multi-replica PostgreSQL
fleets the River periodic job may run on any replica, and per-process
state makes refresh continuity per-replica — a cover discovered on one
replica can lapse before that replica's next turn. Never a regression
versus pre-#101 (every replica's own rotation independently rediscovers
and re-covers), and fully mitigated by sizing the budget at or above the
fleet's registered row count so every pass is a complete rotation;
fleet-persistent state belongs to the #121 bundle rather than a parallel
mechanism here.

A genuine loss is charged through the existing round-57/58 seam,
`ref.ReserveRestoreCover`, under the canonical spelling plus
`/repair-cover` — the same reservation row the restore paths renew, so a
restore racing a pass refreshes the row instead of double-charging, and a
completed restore's registration simply lets the cover expire by TTL.
Order is cover FIRST, then `BumpLFSAdoptionGen`: an admission already
holding a floor scanned during the absence sees the cover in
`active_reservations`, while the bump only stops future serves of cached
deflated floors. Charging is skipped entirely when quota enforcement is
disabled (nothing consults floors or reservations); detection still logs
and reports — a missing registered pack is an integrity loss (committed
refs may reference it) worth surfacing regardless of quota.

Charging a pack's loss requires its size, which the `packs` table never
recorded; it gains a `bytes` column (both backends, idempotent migration,
`DEFAULT 0`) written by `RegisterPacks`/`ReplacePacks`. Pre-migration rows
report as `pack_rows_unsized` — detected, uncoverable — and heal as repack
folds rewrite them.

Deliberate exclusions: purge-fenced repositories are skipped (their rows
drop and purge-aware sweeps reap their blobs; `ReserveRestoreCover`'s own
transactional fence backstops the probe), and `pack_retained` rows are out
of scope — superseded blobs are never restored, so for them the deflated
floor is the true permanent state and a re-charged cover would permanently
overcharge admission. Permanently lost but still-registered LFS rows
converge through reclamation (ADR-42) instead: retirement drops the row and
the reconciler stops covering it.

Alternatives rejected: charging covers inside request paths (unbounded
work exactly where #51's budgets forbid it); a persistent "absent" mark on
rows (state that can go stale in both directions — a cover expiring by TTL
plus per-pass re-verification is self-correcting); and folding the scan
into the sweeper (its listing walk is storage-driven and unbudgeted — the
registry-driven direction needs its own bounded cursor discipline, and the
two jobs' cadences serve different goals).
