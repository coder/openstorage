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
each pass, in two steps (Codex round 9): a single DB-clock statement
bulk-touches every live deterministic cover's TTL — so protection never
depends on the keyset work below completing within the TTL — and then the
keyset listing re-charges each row (byte re-pinning, superseded-row
cleanup, the cover and legacy-nonce harvest). One work unit per row, entirely
database-derived, so any replica and any restart keeps every discovered
loss covered without process memory; an incomplete listing can delay
lifecycle work but cannot lapse a live cover. "Live" is
admission's own liveness window (the reservation TTL, filtered in SQL):
a cover row that outlived its TTL is DEAD — admission stopped counting
it, so renewing or migrating it would resurrect a stale reservation a
healed repository already shed (Codex round 6) — and is left to the
ledger's expired-row cleanup; a still-real loss is simply rediscovered
by the rotation and charged under a fresh incarnation. Renewal never
lapses on budget: it always offers the full listing, and a population
beyond its allocation overruns loudly (`renewal_over_budget`) — a bounded
round-robin would let a big enough backlog outlive the cover TTL between
one row's renewals — while discovery keeps a GUARANTEED half-budget
allowance regardless of the overrun (Codex round 14: the bulk touch keeps
covers live, so a zero-allowance clamp would be an absorbing state where
new losses stay uncovered forever; the pass wall clock bounds total work
either way). Only the pass
deadline or a listing failure cuts the listing short
(`renewal_incomplete`), parked strictly before the first row whose
renewal did not happen (Codex round 8) and resumed first next pass on the
durable cursor.

Aggregates stay exact through deterministic INCARNATION keys
(`absence-lost/<base>/<class>-e<n>`, zero-padded): each full traversal of
a base's row range recomputes its deficit total from scratch and grows the
live incarnation in place — the charge is always the recomputed total,
never `live + delta`, so crash replays over-cover transiently instead of
compounding. When a FENCE-CONTINUOUS observation covered the WHOLE range
with no stat or charge errors, a smaller total mints incarnation n+1
charged exactly and deletes the stale GREATEST-pinned row (mint first —
never a coverage gap), and a zero total deletes the row outright. Deletes are safe because
only this job writes under the prefix and River's unique periodic jobs
serialize passes fleet-wide; incarnation numbers restart only after the
previous row was deleted, so a stale GREATEST-pinned row can never be
silently re-adopted (the round-6 counter hazard). The pending range parks
WITH the durable cursor (base, started/dirty flags, running total), so a
range spanning any number of budget-limited passes accumulates exactly
for GROW charging, and crash replays re-derive identical totals from the
parked cursor. Verdicts (shrink, release, retirement proofs) come from
FRESH evidence only (Codex rounds 19-21): a resumed range's completion is
merely a TRIGGER — its accumulated total spans park gaps where
out-of-band byte loss is invisible to any database fence, so relaxing
compensation from it directly could spend headroom a gap loss reopened.
The trigger fires a bounded VERIFICATION: a scoped re-enumeration of the
base's class rows with a storage stat per row — exactly the evidence a
single-pass observation yields — and the verdicts use that observation
alone (a gap loss is thereby seen, covered, and only then judged).
Verification work draws on a shared per-pass pool of one budget's worth,
so verdict work cannot multiply the pass bound; a base whose class
outgrows the pool defers loudly (raise the budget above the largest
base's combined class rows) with its covers left protectively oversized —
without this path, a base larger than one pass's class allowance could
never complete unresumed, never shrink its covers, and never retire its
legacy nonces (round 20, P1). Two cheap pre-filters keep the pool for
verdicts that matter: a mutation fence (the park snapshots the base
group's summed shared adoption generation after every bump the parking
pass itself made; the next pass re-reads it at load before making any of
its own — the job is fleet-unique, so a mismatch proves a FOREIGN
mutation landed while parked and TAINTS the range, which then waits for
the next traversal), and a relevance gate (verification is spent only on
a live shrinkable head or a base with legacy nonces awaiting retirement).
Error-dirty ranges and traversals never shrink and never
count as clean — but a dirty range whose running total went UNCHARGED
(the charge itself failed) gets one bounded grow-only retry at range end,
forced past the per-pass denial memo (Codex round 8): an under-counted
total can only grow the cover less than the truth, and without the retry
a transient failure on the range's last deficits would wait a whole class
traversal while admission stays under-reserved. Shrink latency is
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
pre-#125 workers — at any point of a mixed-version roll — are RETIRED on
proof, never transferred: a pre-#125 cover aggregates BOTH classes'
deficits with no class split, pre-#125 workers minted one nonce per
replica/pass so several live nonces can compensate DISJOINT deficits that
admission sums, and old replicas keep re-charging their own rows mid-roll
— every transfer scheme tried (a GREATEST merge, then additive credits
with durable intents; Codex rounds 4-19) re-interpreted an old worker's
periodic same-key TTL refresh, which overwrites the row's owner, as new
deficit and re-added it each pass toward a wedged admission. Instead each
nonce row IS its own cover: it rides the pass-start bulk touch (Codex
round 15: a nonce minted just behind the cursor must not lapse before the
rotation ever reaches it) until BOTH classes hold FRESH proofs in one
pass — a clean single-pass start-to-end range observation, a resumed
completion's fresh verification, or a row-less keyset gap — each
recomputing the base's true deficit from the registry and storage and
charging it into the deterministic head. The two rotations traverse
independent populations, so their completions for one base can stay out
of phase indefinitely (Codex round 21): a completed class proof
therefore WAITS durably as a TRIGGER (one bounded `job_scan_state` row
keyed by base and class) under the same generation fence — re-validated
at every pass start before the pass's own bumps, dropped on any
parked-gap mutation, and re-snapshotted at every pass end because the
pass's own cover-renewal re-charge bumps the group each pass for bases
holding live deterministic covers, and a self-bump must not expire a
scheduling-only candidacy (Codex round 25) — until its counterpart
lands, and consuming it for
retirement re-verifies that class fresh at fold time (never trusted
directly, whatever its age: the parked gap it spans is invisible to any
DB fence — out-of-band loss has no DB event and an old worker's nonce
re-charge lists as ordinary liveness; Codex rounds 23-24). Fold-time
re-verification runs each started candidate base to completion past the
shared verification pool — starts gate on a non-exhausted pool, bounding
the overshoot to one base's rows — so a base whose combined class rows
outgrow one pass's pool still converges. With both fresh
proofs in hand the ground truth subsumes whatever the nonce
aggregated and the folded sentinel is CAS'd into the row's owner column,
guarded on the LISTED (owner, bytes): every re-charge overwrites owner
and GREATEST-grows bytes, so a CAS miss proves a newer reliance arrived
after the listing — it wins, the row stays live and bulk-touched, and a
later pass re-proves. Marked rows leave the touch and decay by TTL; never
deleted — an old replica may still depend on its own nonce rows mid-roll,
and its re-charge overwrites the sentinel, making the row live again
until re-proven. The owner column is the SOLE fold state — no
process-local memo: three review rounds broke memo variants the same way
(an old replica's re-charge changed what the nonce means while the memo
said "done"), and the sentinel closes the class because a marked row is
exactly one with no reliance newer than its last proof.

Resolved residual (issue #125, with #121): the original design kept the
known-absent set and rotation cursors in process memory, making refresh
continuity per-replica on multi-replica PostgreSQL fleets. Both now live
in durable state — cursors in `job_scan_state`, tracking in the cover
rows themselves — so any replica renews every cover and resumes the one
fleet-shared rotation. Durable cursors resume only while the covers they
left behind are provably standing (Codex round 17): a cursor row whose
DB-clock age outlives the renewal guarantee (cover TTL minus the pass
timeout — a fleet pause) may have covers expired behind it, and a cursor
parked while quota enforcement was off (the payload records the mode) had
no cover maintenance at all, so both reset the rotation to origin —
strictly a fairness cost — instead of letting the uncovered losses wait a
full wrap while admission spends the reopened headroom. The remaining
residual is narrower: a row whose settlement stat-errors on every
traversal keeps its base's ranges dirty, so heals in that base shed only
after a clean traversal (loud via `stat_errors`, conservative direction).

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
