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
(the reclaimer/purger discipline). Cursors — with per-class traversal
bookkeeping — persist fleet-wide in the ref store's `job_scan_state` rows
(issues #121/#125), so a pass scheduled on any replica resumes the one
shared scan. A lost row only delays convergence: verdicts are recomputed,
never trusted from state.

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
returns to a row less often than its cover survives — so discovered
losses AGGREGATE into one cover reservation row per affected base
repository and registry class (Codex round 4: per-row covers would scale
renewal work with the loss population and collide with the per-base
active-reservation-row cap, so a big enough incident would either outrun
the renewal cycle or be denied outright), and the cover rows THEMSELVES
are the durable tracking state (issue #125). The renewal phase runs FIRST
each pass: it LISTS the live cover rows and re-charges each — one work
unit per row, entirely database-derived, so any replica and any restart
keeps every discovered loss covered without process memory. "Live" is
admission's own liveness window (the reservation TTL, filtered in SQL):
a cover row that outlived its TTL is DEAD — admission stopped counting
it, so renewing or migrating it would resurrect a stale reservation a
healed repository already shed (Codex round 6) — and is left to the
ledger's expired-row cleanup; a still-real loss is simply rediscovered
by the rotation and charged under a fresh incarnation. Renewal never
lapses on budget: it always offers the full listing, and a population
beyond its allocation squeezes discovery for the pass, loudly
(`renewal_over_budget`) — a bounded round-robin would let a big enough
backlog outlive the cover TTL between one row's renewals. Only the pass
deadline can cut the listing short (`renewal_incomplete`, resumed first
next pass on the durable cursor).

Aggregates stay exact through deterministic INCARNATION keys
(`absence-lost/<base>/<class>-e<n>`, zero-padded): each full traversal of
a base's row range recomputes its deficit total from scratch and grows the
live incarnation in place — the charge is always the recomputed total,
never `live + delta`, so crash replays over-cover transiently instead of
compounding. When a traversal observed the WHOLE range with no stat
or charge errors, a smaller total mints incarnation n+1 charged exactly
and deletes the stale GREATEST-pinned row (mint first — never a coverage
gap), and a zero total deletes the row outright. Deletes are safe because
only this job writes under the prefix and River's unique periodic jobs
serialize passes fleet-wide; incarnation numbers restart only after the
previous row was deleted, so a stale GREATEST-pinned row can never be
silently re-adopted (the round-6 counter hazard). The pending range parks
WITH the durable cursor (base, started/dirty flags, running total), so a
range spanning any number of budget-limited passes accumulates and
evaluates exactly, and crash replays re-derive identical totals from the
parked cursor; error-dirty ranges and traversals never shrink and never
count as clean. Shrink latency is
therefore one rotation, which only delays convergence, never visibility.
Renewal timing is enforced end to end (Codex round 3, security): covers
renew at pass starts, periodic ticks that fire mid-pass coalesce (River
uniqueness includes the running state), and a pass's wall clock is capped
at a quarter of the TTL while configuration rejects intervals above half
the TTL — so the gap between one cover's renewals (pass + interval + next
renewal phase) stays within the TTL even under degraded blob stores. The
rotation leader alternates between the two registries each pass (durable
parity), since a twin probe can overdraw the leader's allocation by a
unit; a configuration floor (`MinAbsenceReconcileBudget`) keeps budgets
out of single-row-overdraw territory. Legacy nonce-keyed covers minted by
pre-#125 workers — at any point of a mixed-version roll — are MIGRATED on
first sight: a pre-#125 cover aggregates BOTH classes' deficits with no
class split, so its total folds into BOTH of the base's deterministic
class incarnations via the ledger's GREATEST merge (grow-only; each
class's own rotation right-sizes or row-less-releases its copy within one
clean range observation, and the transient over-cover until then only
tightens admission), after which the nonce row decays by TTL. Never
deleted: an old replica may still depend on its own nonce rows mid-roll.
Migration is memoized per worker process as a byte WATERMARK (not a
correctness dependency): a nonce an old replica re-grows after the first
fold re-folds at its larger total, so mid-roll growth reaches the
incarnations before the nonce decays; replays only re-inflate
transiently.

Resolved residual (issue #125, with #121): the original design kept the
known-absent set and rotation cursors in process memory, making refresh
continuity per-replica on multi-replica PostgreSQL fleets. Both now live
in durable state — cursors in `job_scan_state`, tracking in the cover
rows themselves — so any replica renews every cover and resumes the one
fleet-shared rotation. The remaining residual is narrower: a row whose
settlement stat-errors on every traversal keeps its base's ranges dirty,
so heals in that base shed only after a clean traversal (loud via
`stat_errors`, conservative direction).

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
