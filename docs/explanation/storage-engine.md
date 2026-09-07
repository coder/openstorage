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
- **The blob store (S3-compatible; plain files in local mode)** holds *immutable,
  content-addressed packs*. The bytes under a key never change; concurrent replicas
  racing the deliberate non-atomic exists-check may re-PUT the *identical* content to
  the same key — semantically harmless, though on a versioned bucket each re-PUT leaves
  a billed noncurrent version
  ([lifecycle-rule guidance](../how-to/deploy-production.md#backing-services)).

(An experimental third backend collapses both stores into one S3-compatible bucket —
[below](#the-zero-infrastructure-backend-s3-as-sole-authority-s3wal).)

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

Since [#51](https://github.com/coder/open-storage/issues/51) the same ledger admits Git
LFS uploads in two stages: the batch endpoint runs an **advisory** admission check
against each missing object's declared size — failing hopeless requests early with 507
before issuing any transfer URL, but holding nothing durable — and the verified proxy
upload performs the **durable** admission after the body has been staged and its sha256
verified, reserving the actual byte count immediately before publication. Settlement —
consuming the reservation and crediting the counter — happens in one transaction with
the object's registration. An upload URL therefore does not hold capacity during
transfer: two near-quota clients can both receive URLs and one may still be denied at
upload time; quota is never overshot. The listing floor covers the `lfs/` prefix
alongside the pack prefixes, so pre-ledger LFS bytes gate admission too.

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

Ref rebuilds are **bounded**: a full sync never materializes a repository's whole ref set
in memory, because ref fanout is attacker-scaled (millions of same-tip refs cost one row
each). The database side streams byte-ordered keyset pages, the cache side streams git's
own sorted ref listing, and a merge-join between them applies deletions and updates in
bounded batches — all deletions strictly before any update, so directory/file renames
among ref names can't wedge the sync. Every page is an individually consistent snapshot;
if refs change while pages stream, the mutation's generation bump outruns the marker this
sync writes, so the next read repeats the full sync and converges. That is the same
staleness window a single-snapshot rebuild always had — bounded memory costs no
additional semantics.

## The zero-infrastructure backend: S3 as sole authority (s3wal)

The **experimental** `s3wal://` backend
([ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md))
removes the database entirely: one S3-compatible bucket is the *only* durable system.
The two-store split above collapses — the bucket holds the immutable packs (`packs/`,
`lfs/`, unchanged) *and* the authority, as conditional-CAS JSON objects under the
reserved `_wal/` prefix. Deployment mechanics are in the
[production guide](../how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node);
this section explains what changes in the engine and what deliberately does not.

**One manifest object per repository group is the ref authority.** The entire state of
a base repository group `{base, +ephemeral, +import}` — refs, pack registry, quota
ledger and reservations, delete locks, LFS registry, sync-claim fence — lives in one
JSON object, `_wal/repo/<baseRepoID>`, advanced atomically with `PutObject + If-Match`
(create: `If-None-Match: *`). Every multi-row transaction the SQL backends run becomes
a single-object compare-and-swap: exactly one concurrent writer wins, losers re-read
and retry with jittered backoff. All metadata tables (repos, webhooks, credentials,
signing keys, org keys) share one further object, `_wal/meta.json`, with the same CAS
discipline and a process-local cache revalidated through `If-None-Match` 304s.

**Writes are validated by the CAS; reads are trusted.** Only the conditional PUT
proves a snapshot current: a manifest read that turned out stale surfaces as a 412
and is re-read, but a read that ends in a *negative* verdict — a sweep finding
nothing expired, a CAS whose precondition no longer holds, a generation check that
sees no change — is never followed by a write and is taken at face value. A
per-process sequence watermark crashes the node if the store ever serves a version
older than one this process already observed, but a fresh process, or a version
merely one behind, is undetectable. The design therefore makes **strong
read-after-write and list-after-write consistency a hard requirement of the
store** (see the [deployment prerequisites](../how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node))
rather than paying a validating write on every read-only verdict.

**Publish before CAS is preserved unchanged.** Pack and LFS bytes still upload through
the ordinary blob path *before* the manifest CAS registers or references them — the
crash asymmetry (orphans, never dangling refs) carries over verbatim. What changes is
only who arbitrates the ref flip: an ETag condition on one object instead of a database
row lock.

**A manifest logical clock replaces the database clock.** PG/SQLite stamp every lease
and grace window with the DB clock so node skew cannot corrupt expiry. s3wal has no
server clock, so every CAS writes `clk = max(localNow, clk_prev + 1ms)` — monotone
across restarts and backward local jumps — and all stamps use it. Verdicts that permit
destruction or steal **fail closed** (demote to report-only, log, never expire early)
behind three skew guards: when the manifest clock runs ahead of local time, when the
wall clock drifts from the process's monotonic clock, each by more than
`min(15 min, ttl/4)` — and destructive verdicts additionally cross-check the S3 HTTP
`Date` response header, the only arm that catches a clock that was wrong at boot (a
store supplying no usable `Date` witness is refused at startup). Over-retention and
false-deny are the only reachable failure directions. That guarantee covers the expiry
paths judged against the manifest and store clocks — pack reservations, delete locks,
grace windows, the instance lease. Metadata claim leases in `_wal/meta.json` (the
sync/push claims) instead use plain wall-clock TTLs, mirroring the SQL backends' lease
semantics.

**No conditional DELETE, anywhere.** S3-compatible stores do not honor `If-Match` on
DELETE uniformly — a guard that sometimes does not guard is worse than none. Deletion
safety comes from ordering instead: manifest purge tombstones are terminal and never
deleted, and physical deletes are fenced by short-TTL delete locks recorded in the
manifest via CAS plus the sweeper's quarantine-first handshake.

**Single-writer serialization, detected — not enforced.** The mode is intentionally
single-node. Correctness never depends on that (every state change is CAS-serialized
regardless), but a second writer degrades the clock guards and cache behavior, so the
`_wal/instance-lease` CAS-heartbeat turns accidental dual-serve into a *detected*
condition: serve refuses to start over a fresh foreign lease, steals expired ones, and
fail-stops on heartbeat loss. It is best-effort detection, never the correctness
mechanism.

**Manifest size is a documented boundary.** A repository group's whole registry is one
JSON object: growing writes warn past 4 MiB and are refused past 16 MiB — shrinking
writes and the purge pipeline always pass, so an oversized manifest stays cleanable.

**When to choose it — and the honest cost.** The target is small, single-tenant,
single-node deployments that want durability without operating a database: the process
dies, the disk burns, and a fresh node pointed at the bucket reconstructs full service.
The price is one CAS point per repository group: concurrent pushes to the *same*
repository serialize on one object (hot-repo contention surfaces as CAS retries and,
exhausted, as a clean push rejection), and every ref or metadata operation pays an S3
round trip. On the git data plane, distinct repositories do not contend — each group
has its own manifest. Metadata is one further CAS point shared by *everything*: every
repo, webhook, credential, org-key, and upstream-link mutation advances the single
`_wal/meta.json` (metadata write rates are low, but they serialize globally). If
hot-repo write throughput binds, local mode is the faster single-node alternative;
multi-node serving means the Postgres topology.
