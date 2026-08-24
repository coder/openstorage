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
registered spelling probes the canonical/legacy twin first. Twin-present
rows are physically listed — the floor is not deflated — and download-time
restore (PR #104) heals the spelling through the digest-verified adopt
pipeline, so they are reported (`lfs_rows_twin_only`), never charged. Only
rows absent under BOTH spellings are genuine loss.

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
