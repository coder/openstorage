# The storage engine: immutable packs, ref CAS, and garbage

This page explains *why* the storage lifecycle works the way it does — the invariants the
engine is built around, and the consequences an operator observes (orphan blobs, retained
bytes, repacks that keep unreachable objects). The corresponding runbook is the
[storage lifecycle how-to](../how-to/storage-lifecycle-and-sweeping.md); deployment
topology is in the [production guide](../how-to/deploy-production.md).

## Two stores, one invariant

All durable state splits across two systems with very different guarantees:

- **The database (Postgres; SQLite in local mode)** is the *authority*: refs, the pack
  registry, metadata, quota accounting. Refs advance only through compare-and-swap — every
  writer states the value it expects, and exactly one concurrent writer wins.
- **The blob store (S3/MinIO; plain files in local mode)** holds *immutable,
  content-addressed packs*. The bytes under a key never change; concurrent replicas
  racing the deliberate non-atomic exists-check may re-PUT the *identical* content to
  the same key — semantically harmless, though on a versioned bucket each re-PUT leaves
  a billed noncurrent version
  ([lifecycle-rule guidance](../how-to/deploy-production.md#backing-services)).

The single invariant tying them together — **publish before CAS**:

> Pack bytes are durably published to the blob store *before* any database row (pack
> registration, ref CAS) references them.

Every writer honors it: git pushes upload and register quarantined packs in the
pre-receive hook, and git's update hook only CASes refs after pre-receive succeeded; REST
writes (commit-pack, merge, notes, fork) run the same upload → register → CAS pipeline;
repack publishes its consolidated pack before swapping registry rows. After registering,
writers *verify* the blob is really readable (`blob.EnsurePublished`) — the belt to the
sweeper's suspenders ([below](#why-deletion-is-so-careful)).

The payoff is crash asymmetry. A crash between publish and commit strands a paid-for but
unreferenced blob — an **orphan**, invisible to every reader, costing only storage. The
write path can never produce the opposite — a committed ref pointing at bytes that were
never published. (With destructive sweeping enabled, rare delete/registration races can
make an already-registered pack *transiently* unavailable; the quarantine handshake and
every sweep's opening heal pass restore it, so the exposure is a bounded retry window,
never durable loss.) Orphans are cheap and reclaimable; durably dangling refs would be
data loss. The engine chooses its failure mode.

The corollary: **garbage is normal**. Crashed pushes, lost races, superseded packs all
leave blobs behind by design, and reclamation is deferred to an asynchronous, careful
[sweeper](#why-deletion-is-so-careful) instead of any synchronous delete on the write
path.

## Why repack packs the union, not reachable objects

Push-heavy repositories accumulate one small pack per push, and read-side hydration cost
grows with pack count. Background repack folds many registered packs into one
consolidated pack.

The natural design — run `git pack-objects --all` and keep reachable objects — is wrong
here, because reachability at fold time is not a safe upper bound for what registered
packs must keep serving. A push registers its pack in pre-receive and CASes its refs in
the update hook *moments later*. A fold that snapshots between the two would drop the
momentarily-unreachable pushed objects; then the ref CAS lands and points refs at objects
no registered pack contains — cold-cache corruption that no swap-time check can prevent.

So repack packs the **union of the snapshotted packs' objects**, preserving a simple
invariant: *the registered object set never shrinks*. Any interleaving of registrations
and ref CASes stays consistent. The costs are accepted deliberately:

- **Unreachable objects are carried forward** (crashed pushes, deleted branches).
  Reclaiming them safely would need a reachability grace protocol — out of scope, and why
  [deleting branches doesn't reclaim space](../how-to/storage-lifecycle-and-sweeping.md#why-is-the-bucket-growing).
- **Superseded originals linger.** The fold's swap (`ReplacePacks`) is a row-set CAS in
  one transaction: it deletes exactly the input rows (losing cleanly if a concurrent fold
  got there first), records them as *retained*, and inserts the consolidated pack. The
  old blobs are not deleted synchronously — a concurrent loser may still be mid-read on
  them, and blob-store deletes aren't transactional with the database. They stay
  quota-counted as retained bytes until the sweeper reaps them.

Folds are budgeted (bytes, objects, wall time — compiled constants) and selected from
cheap point reads (blob size + the pack header's object count) before anything is
downloaded, so a fold either provably fits its budgets or provably cannot happen. Big
repositories converge over successive folds; pack counts converge *below* the trigger
threshold, not to one — except when no pack pair fits the budgets at all, where the
no-op is proven and the count legitimately stays put.

## Why quota admission is a ledger, not a counter

A per-repository pack-bytes quota must hold across replicas and concurrent pushes: two
pushes that individually fit but together overshoot must not both pass. A plain usage
counter can't see in-flight uploads; listing the bucket on every push is slow and racy.

Admission is a durable reservation ledger in the database, serialized per base repository
(one advisory lock in Postgres, an immediate transaction in SQLite):

```
usage counter + uncounted floor + active reservations + incoming ≤ quota
```

The *uncounted floor* is the physical blob listing minus the usage counter read at that
listing's scan start, clamped at zero: the bytes the counter has never seen.

- **Reservations** claim headroom *before* upload and are consumed atomically when the
  pack registers. A crashed writer's reservation keeps gating until its TTL elapses —
  admission filters expired rows out immediately; the sweeper's later pruning is pure
  garbage collection. A *retry* of the same push refreshes the same reservation instead
  of double-charging, because packs are content-addressed.
- **The counter and the floor split the bytes between them.** The live counter covers
  everything registered — including settlements newer than the floor's scan — while the
  uncounted floor covers what the counter never saw (pre-ledger packs, pre-registry LFS
  objects, orphaned uploads). Summing the two counts every byte at least once and
  double-counts only a blob whose listing and registration straddle one scan window —
  transient, and toward *denying* at the margin, never silent overshoot. The floor is
  served from a short-lived per-repository cache shared by every admission surface
  (git pushes, REST pack writes, LFS transfers) rather than re-scanned per write — safe
  precisely because everything registered or in flight is covered by the counter and
  reservations regardless of the floor's staleness.
- **Retained bytes stay counted.** A repack fold doesn't remove the superseded blobs, so
  it doesn't free quota either — the ledger is debited only when the sweeper reaps them.
  Quota tracks the bytes under the repository's *pack prefixes*, not logical
  reachability. (Bucket-wide consumption falls a step later still: a reaped blob leaves
  an equal-sized quarantine backup outside the pack prefixes until the purge window
  passes.)
- **Namespaces roll up.** `+ephemeral` / `+import` pushes store packs under sibling
  prefixes (`packs/<repo>+ephemeral/…`), not the base prefix — but quota admission lists
  base and namespace prefixes together and charges them all against the base repository.

Repack is deliberately not admission-gated: it replaces rows rather than growing the
registered set, and gating it would deadlock exactly the at-quota repositories that need
compaction most.

Since [#51](https://github.com/coder/code-storage/issues/51) the same ledger admits Git
LFS uploads: the batch endpoint reserves each missing object's declared size before
issuing any transfer URL, the verified proxy upload re-admits the request's actual
`Content-Length` (a same-key reservation refreshes, so honest clients are charged once
while a declared-size lie re-runs the full check), and settlement — consuming the
reservation and crediting the counter — happens in one transaction with the object's
registration. The listing floor covers the `lfs/` prefix alongside the pack prefixes,
so pre-ledger LFS bytes gate admission too.

## Why deletion is so careful

The sweeper is the only component that ever destroys data, so it assumes everything else
might be racing it. Its ground truth is a diff: blobs under `packs/` without a registry
row, older than an age threshold (in-flight writers register well within it). Then four
layers make acting on that diff safe — candidates are re-validated just before acting;
deletes are quarantine-copies first; the registry is re-checked *after* each delete, with
instant restore if a registration landed mid-delete; and every sweep begins by healing any
registered-but-missing pack from quarantine. The writer-side `EnsurePublished` verify
closes the remaining interleaving: a registration that lands after the sweep's recheck
sees the missing blob and re-uploads it. This joint argument is also why destructive
sweeping has a
[fleet-wide build prerequisite](../how-to/storage-lifecycle-and-sweeping.md#before-enabling-delete-mode-fleet-prerequisite):
a writer *without* the verify reopens the one window the layers close, which is exactly
the mixed-fleet caveat.

Deletion is also two classes with different risk profiles, which is why there are
[two knobs](../how-to/storage-lifecycle-and-sweeping.md#the-orphan-sweeper):
repack-superseded blobs are *provably* garbage (durable retained records say so) and are
reaped by default, while general orphans are inferred garbage — the inference is sound,
but the class exists because something already went wrong once — so they stay list-only
until an operator opts in.

## The cache layer: hydration and generations

Replicas serve git reads from local bare-repository caches that are pure derivations of
database + blob store. Correctness across replicas comes from a per-repository
**generation counter**, bumped by database triggers on every committed ref or pack
change — not by application code, so a rolling upgrade can't miss a bump. A cache marked
with the current generation (and current default branch) is provably fresh and can answer
after one database point read; anything else triggers hydration — download missing packs,
rebuild refs — under a local file lock, with the marker written only after complete
success. Losing a cache directory is an efficiency event, never a correctness event.
