# Operate the storage lifecycle: repack, quotas, sweeping

Use this runbook when the bucket keeps growing, a push is denied for quota, or you want
to let the sweeper delete data.

Design rationale lives in the [storage engine explanation](../explanation/storage-engine.md).
Every tunable lives in the [configuration reference](../reference/configuration.md#maintenance-jobs).

| Job | What it does | Default |
|---|---|---|
| [Repack](#background-repack) | Folds many small packs into one | On, at 10 packs |
| [Pack quota](#pack-quotas) | Denies writes that would exceed a repository's storage quota | 10 GiB per repository |
| [Orphan sweeper](#the-orphan-sweeper) | Lists orphan blobs, reaps repack-superseded packs, optionally deletes orphans | Hourly; list-only for orphans, superseded reap on |
| [Repository purge](#reclaiming-deleted-repositories) | Permanently removes soft-deleted repositories | Off |
| [LFS reclamation](#reclaiming-unreferenced-lfs-objects) | Retires LFS objects no reachable pointer references | Off |
| [Absence reconciliation](#reconciling-registered-but-missing-bytes) | Detects registered rows whose bytes are missing and keeps quota honest | Off |

## Why is the bucket growing?

Pack storage is append-only. Writers only add immutable, content-addressed packs, and
nothing is deleted synchronously. Every blob falls into one of these classes:

| Class | Created by | Reclaimed by |
|---|---|---|
| **Registered packs** | Every push and REST write publishes a pack and registers it | [Repack](#background-repack) folds many into one; the originals become *retained* |
| **Retained (superseded) packs** | A repack fold replaced their registry rows; the blobs linger | Sweeper's superseded reap ([`OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`](../reference/configuration.md#openstorage_sweep_delete_superseded), on by default) after the age threshold |
| **Orphan packs** | Crashed writers (pack published, registration never committed), lost repack races | Sweeper — **list-only by default**; deletion needs the [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#openstorage_sweep_delete_enabled) opt-in |
| **LFS objects** (canonical `lfs/<repo_id>/o2/<oid>`; legacy pre-adoption `lfs/<repo_id>/<oid>`, ADR-41) | Verified LFS uploads publish the object and register it | Permanent while the repository lives, unless opt-in [LFS reclamation](#reclaiming-unreferenced-lfs-objects) retires unreferenced ones for the sweeper to delete; [repository purge](#reclaiming-deleted-repositories) reclaims the rest |
| **Orphan LFS objects** | Crashed uploads, and objects uploaded before the LFS registry existed | Sweeper — **list-only by default**; deletion needs the separate [`OPENSTORAGE_SWEEP_DELETE_LFS`](../reference/configuration.md#openstorage_sweep_delete_lfs) opt-in (see [the caveat](#lfs-orphans-and-pre-registry-objects)) |
| **Quarantine copies** (`sweep/quarantine/<nonce>/…`) | Delete-mode sweeps copy every blob aside before deleting it | The sweep purges backups older than `max(age threshold, 1h)` while a destructive mode is on |

Other things in the bucket:

- **Old object versions** on versioned S3 buckets. This is why destructive sweeping
  [refuses versioned buckets](#s3-bucket-versioning).
- **The reserved `_wal/` prefix** in the experimental zero-infra mode
  ([ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md)).
  These small objects are the ref/metadata authority, not blob payloads. The sweeper
  never lists them (it lists only `packs/`, `lfs/`, and `sweep/quarantine/`) and
  fail-fast asserts that no `_wal/` key reaches its delete paths.

> [!WARNING]
> Never write to or delete anything under `_wal/` yourself.

These do **not** reclaim space:

- **Deleting branches.** Repack keeps the whole registered object set, so unreachable
  objects are carried forward by design
  ([why](../explanation/storage-engine.md#why-repack-packs-the-union-not-reachable-objects)).
- **Deleting a repository on its own.** `DELETE /repos` soft-deletes the metadata record.
  The name stays burned: recreating the same URL is rejected as already existing. The
  registry rows and blobs stay, and the sweeper still counts them live. Reclaiming them
  takes the separate [repository purge](#reclaiming-deleted-repositories).

## Inspect what a repository stores

List a repository's blobs under `packs/` straight from the blob store:

```sh
openstorage list-packs --repo repo_01k5abc...   # takes the server-generated RepoID
```

- `--repo` takes the **RepoID** (`repo_...`, returned by repo creation and
  `GET /api/v1/repo`), not the repository name.
- Namespace pushes store under sibling prefixes. List them with a suffix:
  `--repo repo_01k5abc...+ephemeral`.
- A quota denial's `stored` bytes cover the base **and** all namespace prefixes.
- The command reads `OPENSTORAGE_S3_URL`/`OPENSTORAGE_PG_URL` (or `--data-dir` in local
  mode) from the environment. See the [CLI reference](../reference/cli.md#openstorage-list-packs).

`list-packs` reports **physical** bytes: live, retained, and orphan blobs alike, without
consulting the registry. This is the same measure the quota's listing floor uses.

Fleet-wide, the [sweep report](#reading-a-sweep-report) counts live vs. orphan blobs per
run, but totals bytes for orphan candidates only. No surface reports registry-live bytes
on their own, so use blob-store metrics for fleet-wide capacity tracking.

## Reclaiming deleted repositories

Soft-delete alone reclaims nothing. Two paths run the same purge:

- **Retention window:** set
  [`OPENSTORAGE_REPO_RETENTION`](../reference/configuration.md#openstorage_repo_retention)
  (for example `720h` for 30 days; unset keeps deleted repositories forever). A purge pass
  on the [sweep](#the-orphan-sweeper) cadence reclaims every repository soft-deleted
  longer than that.
- **Operator command:**
  [`openstorage purge-repo --yes <repo-id>`](../reference/cli.md#openstorage-purge-repo)
  purges one repository now. It must have been soft-deleted at least an hour earlier.

A purge permanently removes the repository's registry and eventing rows (refs, packs, LFS
objects, credentials, upstream link) and its blobs (base packs, namespace packs, LFS
objects). Only the repository row stays, stamped as a purge tombstone.

> [!WARNING]
> A purge is **irreversible**. There is no restore.
>
> The burned `(org, url)` name is **never released**, because an unexpired token carries
> the repository URL and would otherwise authorize against a new repository at that URL.
> In s3wal mode the tombstone is the repository group's `_wal/repo/<id>` manifest with its
> terminal `purged_at` fence. Deleting that object by hand un-burns the name and reopens
> those races.

> [!IMPORTANT]
> Before the first purge, every serve replica and hook-running git node must run a
> fence-aware build
> ([fleet-upgrade prerequisite](../reference/configuration.md#openstorage_repo_retention)).

What to expect:

- **In-flight writes fail.** The purge first write-fences the repository in the ref
  store, so in-flight ref updates and pack/LFS registrations fail. Credential/upstream
  writes only attach to live repositories.
- **Late blobs are reaped later.** A fenced push may still publish its blob after the
  purge's scan. Later sweeps reap such bytes once aged, whatever the retention and delete
  settings. Re-running `purge-repo` rescans at once.
- **Interruptions are safe.** Registry rows (refs first) are tombstoned before any blob
  is deleted. A crash never leaves a ref pointing at missing bytes; the next pass or a
  `purge-repo` re-run finishes the job.
- **Storage shrinks one sweep later.** Blobs go to `sweep/quarantine/` and are freed on a
  later sweep once past the purge window, even with all `*_DELETE_*` flags off.

## Background repack

Small pushes add one pack each. When a repository's registered-pack count reaches
[`OPENSTORAGE_REPACK_THRESHOLD`](../reference/configuration.md#openstorage_repack_threshold)
(default 10), a background job folds registered packs into one consolidated pack. It is a
durable [River](https://riverqueue.com) job on Postgres (in-process in local mode); any
replica may run it.

Log lines to expect:

```
repack trigger: scheduled fold   repo=<id> packs=<n> threshold=<t>
repack: folded packs             repo=<id> folded=<n> pack=<new-pack-id>
```

What operators should know:

- **Use one threshold everywhere.** The threshold is per process (its triggers and the
  follow-ups of jobs it consumes). Set it alike on every consumer of the shared River
  queue; setting only the push node to `2` is not enough. Mixed-threshold fleets may not
  honor a producer's lower threshold.
- **Expect fewer than threshold packs, not one.** After writes stop, counts can settle at
  `threshold - 1`. Exactly one pack needs threshold `2` on every consumer and folds that
  keep succeeding within the budgets. If no 2+ packs fit the budgets, the count can stay
  at or above the threshold.
- **Folds are bounded** by compiled-in budgets: 2 GiB of pack bytes, ~1M objects,
  5 minutes (not tunable). Larger repositories fold incrementally; each successful fold
  schedules a successor while the consumer's threshold is still met.
  `repack: budgets select nothing to fold` is a no-op, not an error.
- **Usage grows before it shrinks.** Superseded originals stay in the bucket, and stay
  [quota-counted](#pack-quotas), until the sweeper reaps them.
- **Repack is exempt from the pack quota**, so an at-quota repository can still compact.
  It only replaces rows it removes.
- **`repack: lost fold race ...` is benign.** The losing replica's upload is left for the
  sweeper.

Tuning:

- Raise the threshold for bigger, rarer folds.
- Lower it (minimum 2) for latency-sensitive reads on push-heavy repos.
- `0` disables that process's triggers and follow-ups. Already-queued jobs still run.

## Pack quotas

Every repository has a storage-bytes quota
([`OPENSTORAGE_PACK_QUOTA_BYTES`](../reference/configuration.md#openstorage_pack_quota_bytes),
default 10 GiB, `0` disables). It covers packs **and LFS objects**. A repository and its
`+ephemeral` / `+import` namespaces share one quota, charged to the base repository; the
namespaces also share its LFS object space.

### What a denial looks like

Over git:

```
remote: openstorage hook: pre-receive: repository pack quota exceeded: push adds <n> bytes
to <n> bytes stored (+<n> bytes reserved by in-flight pushes), quota is <n> bytes
```

Over REST (commit-pack, diff-commit, restore-commit, merge, notes writes): `403` with
`repository pack quota exceeded: write adds ... quota is <n> bytes`. A fork whose source
closure alone exceeds the quota is denied up front with
`repository pack quota exceeded: fork closure is <n> bytes, quota is <n> bytes`.

Over Git LFS: git-lfs shows `repository storage quota exceeded: upload adds <n> bytes to
<n> bytes stored (+<n> bytes reserved by in-flight uploads), quota is <n> bytes` and the
push fails. Downloads are never quota-gated. LFS admission has two stages:

1. **Batch API (advisory).** Checks the declared sizes. An over-quota batch gets
   per-object `507` errors and no upload URLs. It reserves nothing durable.
2. **Verified upload (durable).** Re-checks the request's actual `Content-Length`, so
   under-declared sizes are caught here. The reservation is charged per object just
   before its bytes are published, then settled into the usage counter when registration
   completes. It expires after
   [`OPENSTORAGE_PACK_RESERVATION_TTL`](../reference/configuration.md#openstorage_pack_reservation_ttl)
   if registration never lands.

Nothing is held between the stages. Two near-quota clients can both get upload URLs, and
one can transfer a whole object before being rejected: wasted transfer, never quota
overshoot.

### Reading the numbers

Admission is `stored + reserved + incoming ≤ quota`:

- **stored** counts registered packs and LFS objects **plus retained (superseded)
  bytes**; after a fold, headroom returns only when the sweeper reports
  `retained_bytes_reclaimed`. It is floored by the *physical* bytes under the
  repository's prefixes, including aged **orphan** blobs. Orphan deletion is off by
  default, so abandoned uploads can cause denials that only
  [enabling delete mode](#before-enabling-delete-mode-fleet-prerequisite) resolves.
- **reserved** counts other writers' in-flight admissions. A crashed writer's reservation
  blocks only until its
  [`OPENSTORAGE_PACK_RESERVATION_TTL`](../reference/configuration.md#openstorage_pack_reservation_ttl)
  (default 1h) elapses; admission ignores expired rows at once. The sweeper's
  `reservations_expired` pruning is only garbage collection. A retry of the *same* push
  is never double-charged: the same content key refreshes the reservation.
- **incoming** is zero for content already stored (identical pack key).

A push denied after heavy writes may succeed later with no change, once the next sweep
reclaims retained bytes and crashed reservations expire. If denials persist, raise the
quota or accept that the registered history is that large. Remember: [deleting branches does not reclaim space](#why-is-the-bucket-growing).

The git surface also logs quota pressure before the authoritative pre-receive check:

```
quota: receive-pack for <repo> at/over quota (<n> pack bytes stored >= <n>): enforcement defers to pre-receive
```

## The orphan sweeper

Every [`OPENSTORAGE_SWEEP_INTERVAL`](../reference/configuration.md#openstorage_sweep_interval)
(default 1h), a sweep job:

1. heals the quarantine;
2. prunes expired quota reservations;
3. streams the `packs/` and `lfs/` listings and classifies every blob against its
   registry (pack rows for pack keys, LFS rows for LFS keys).

Blobs with no registry row older than
[`OPENSTORAGE_SWEEP_AGE_THRESHOLD`](../reference/configuration.md#openstorage_sweep_age_threshold)
(default 24h) are orphan candidates.

Independent flags control what a sweep may *do*:

| Mode | Default | Acts on |
|---|---|---|
| List-only (no flag) | **active** | Reports orphans of every class, deletes nothing |
| [`OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`](../reference/configuration.md#openstorage_sweep_delete_superseded) | `true` | Only blobs a repack fold provably superseded (durable `pack_retained` records); reclaims their quota bytes |
| [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#openstorage_sweep_delete_enabled) | `false` | General pack orphans (crashed pushes, lost races) — the risky class, hence opt-in |
| [`OPENSTORAGE_SWEEP_DELETE_LFS`](../reference/configuration.md#openstorage_sweep_delete_lfs) | `false` | LFS orphans — separate because of the [pre-registry caveat](#lfs-orphans-and-pre-registry-objects); legacy-spelled keys also need the flag below |
| [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](../reference/configuration.md#openstorage_sweep_delete_lfs_legacy) | `false` | Legacy-spelled LFS keys (ADR-41): the post-roll cleanup opt-in. While off, no sweep deletes them, because old replicas still read these adoption sources mid-roll |

### LFS orphans and pre-registry objects

The LFS registry arrived with [#51](https://github.com/coder/openstorage/issues/51).
**Every LFS object uploaded by an older build has no registry row**, so it shows up as an
orphan candidate even if its pointers are live. That is why LFS deletion is a separate
opt-in, not covered by `OPENSTORAGE_SWEEP_DELETE_ENABLED`.

How old objects join the registry:

- A write-scoped touch adopts them: an upload batch naming the object, or a re-upload.
- Adoption verifies the sha256 once (presigned-era uploads never proved their content).
  A mismatched blob is removed and re-uploaded through the verified path.
- With a [storage quota](../reference/configuration.md#openstorage_pack_quota_bytes)
  enforced, the migration copy passes quota admission, and the legacy source counts until
  the sweeper reclaims it. Adoption needs headroom for a second copy and answers `507` at
  the quota edge: free space, raise the quota, or run the post-roll cleanup.

Active repositories converge on their own. Before setting `OPENSTORAGE_SWEEP_DELETE_LFS=true`:

1. Run list-only until the candidate lines stop naming objects that should live.
2. For a repository you cannot afford to lose objects from, re-push its LFS content. This
   is the fastest backfill; batch adoption skips re-transferring stored objects.

### Reading a sweep report

Each run ends with one summary line:

```
orphan pack sweep finished live=<n> fresh=<n> orphans=<n> orphan_bytes=<n> deleted=<n>
bytes_reclaimed=<n> superseded_deleted=<n> retained_bytes_reclaimed=<n>
reservations_expired=<n> delete_enabled=<bool> delete_superseded=<bool> delete_lfs=<bool>
threshold=<age> duration=<d>
```

| Field | Meaning |
|---|---|
| `live` | Blobs with a registry row (pack or LFS) — real data, not candidates |
| `fresh` | Unregistered but younger than the age threshold — likely in-flight writers; never touched |
| `orphans` / `orphan_bytes` | Aged, unregistered candidates this run, **all** classes: repack-superseded blobs and LFS orphans count until reaped (candidate lines' keys tell them apart) |
| `deleted` | Candidates reaped this run, superseded included. Reaping moves bytes to quarantine, so it frees nothing by itself |
| `bytes_reclaimed` | **Net** bytes freed by purging aged quarantine backups whose blob stayed unregistered. A backup whose original was restored is not counted. Lags `deleted` by the purge window, `max(age threshold, 1h)` |
| `superseded_deleted` / `retained_bytes_reclaimed` | The repack-superseded subset of `deleted`, and the **quota headroom** those reaps returned |
| `reservations_expired` | Crashed-writer quota reservations pruned this run |
| `delete_enabled` / `delete_superseded` / `delete_lfs` | The **configured** modes. A [guard downgrade](#s3-bucket-versioning) to list-only is *not* shown here; look for the separate `delete guard failed; this run is list-only` error line |

Each candidate is also logged as it is classified
(`orphan pack candidate key=… size_bytes=… age=… delete_enabled=… delete_superseded=… delete_lfs=…`),
so in list-only mode you can audit exactly what deletion *would* reap.

- **Healthy:** `orphans=0`, or a small stable count.
- **Investigate:** steadily growing `orphan_bytes` means crashed writers or fold races are
  outpacing your (disabled) deletion. Find out why before enabling deletion.

### Deletion is quarantine-first, and heals itself

A sweep never hard-deletes directly:

- **Copy first.** Every blob is copied to `sweep/quarantine/<nonce>/…`, and the registry
  is re-checked after the delete. A registration that landed mid-delete is restored.
- **Every sweep heals**, list-only included. It first restores any registered pack that
  is missing **or truncated** (e.g. a stale presigned overwrite or storage damage) from a
  complete backup. Rolling back to list-only after a scare still repairs a delete-mode
  crash. Look for `orphan pack sweep: healed registered pack from quarantine` and
  `healed truncated registered object from quarantine`.
- **LFS restores are digest-verified** against the key's OID. A backup that never hashed
  to its OID is *never* restored (logged as
  `NOT restoring LFS quarantine backup failing content verification`); the object
  repairs itself on the next upload touch.
- **LFS deletes are serialized with registration.** A concurrent upload either keeps its
  bytes or provably re-publishes them.

Quarantine backups are purged only while a destructive mode is on, and only once older
than `max(age threshold, 1h)`; a low age threshold does not shorten the 1h floor. Every
push must finish within that age minus 30 minutes: a delete handshake takes up to 15
minutes after it copies a backup, and the heal purges a backup within 15 minutes of its
last read of the registry or the original.
An entry whose calls overrun that deadline fails, and the next sweep visits it again. The
restore of a registered original has no deadline, because copying and hashing a large
LFS object can take longer than any fixed one. The 24h default leaves hours of room. A
backup is also kept while its original cannot be verified complete:

- Pack rows registered before the size column existed (`bytes=0`) keep their backups
  until a repack fold records a size (logged as
  `sparing backup of a registered object with unknowable size`).
- A truncated original the heal could not repair this pass (short backup, or another
  worker holds the object's claim) keeps its backup for the next sweep.

### S3 bucket versioning

On a bucket with versioning enabled (or suspended), deletes only write delete markers.
Every version stays stored and billed, so "reclaimed" bytes would be false:

- `OPENSTORAGE_SWEEP_DELETE_ENABLED=true` and `OPENSTORAGE_SWEEP_DELETE_LFS=true`
  **fail startup** against such a bucket, or when versioning state cannot be verified.
  Grant `s3:GetBucketVersioning` or stay list-only.
- The default superseded reap does not fail startup. Each run re-checks and **downgrades
  itself to list-only** on drift, logging
  `orphan pack sweep: delete guard failed; this run is list-only`.

### Before enabling delete mode (fleet prerequisite)

> [!WARNING]
> Enable destructive sweeping only once **every** node — serve replicas *and*
> hook-running git nodes — runs a build with the post-registration publish verify
> (`blob.EnsurePublished`). All current builds have it; this matters only mid-upgrade.
>
> A mixed fleet cannot lose data, but an **old** writer racing a delete-mode sweep can
> register a pack whose blob was just reaped. That pack stays unavailable (failed
> clones/hydrations for that repo) until the next sweep's heal restores it.
>
> **List-only mode is safe on any fleet.** If you must run pre-`EnsurePublished` nodes,
> also set `OPENSTORAGE_SWEEP_DELETE_SUPERSEDED=false`; the same risk applies to both
> destructive classes. Pre-0.15 replicas do not recognize the new prefix; set
> `CODESTORAGE_SWEEP_DELETE_SUPERSEDED=false` on them.

A conservative enablement sequence:

1. **Audit.** Leave `OPENSTORAGE_SWEEP_DELETE_ENABLED=false` (the default) for at least
   one age-threshold window, and audit the candidate lines in the
   [sweep reports](#reading-a-sweep-report). Superseded reaping is on by default; for a
   fully non-destructive audit, set `OPENSTORAGE_SWEEP_DELETE_SUPERSEDED=false` for the
   window, then restore it.
2. **Check the fleet.** Every node, including git-hook nodes, runs a current build.
3. **Keep superseded reaping** (`OPENSTORAGE_SWEEP_DELETE_SUPERSEDED=true`, the default)
   so repack-superseded bytes and quota headroom come back.
4. **Opt in only if needed.** Set `OPENSTORAGE_SWEEP_DELETE_ENABLED=true` only if orphan
   bytes materially accumulate. Crashed pushes are rare; list-only is a fine permanent
   state for many deployments.
5. **Verify.** Check that `deleted` matches the candidates you audited.
   `bytes_reclaimed` stays `0` until quarantine backups age past
   `max(age threshold, 1h)`; that is expected.

## Reclaiming unreferenced LFS objects

Registered LFS objects are permanent by default: the sweeper treats every registered row
as live. A `git:write` holder can abuse this
([#73](https://github.com/coder/openstorage/issues/73)) by direct-PUTting tiny,
unreferenced objects up to the per-repo cap
([`OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO`](../reference/configuration.md#openstorage_lfs_max_objects_per_repo),
default 100,000) — only a few hundred kilobytes. After that, every **new** LFS object
answers `507` for every collaborator, indefinitely.

The opt-in reclamation job
([ADR-42](../adr/042-lfs-reclamation-is-mark-and-sweep-over-reachable-pointer-files.md))
retires objects that no reachable pointer file names, in any ref of the repository's
namespace group. It runs every
[`OPENSTORAGE_LFS_GC_INTERVAL`](../reference/configuration.md#openstorage_lfs_gc_interval)
(unset by default). Each run:

1. Pages repositories holding LFS objects from a rotating cursor, pinning each namespace
   group's ref generations.
2. Hydrates their git caches and walks `git rev-list --objects` across all reachable refs,
   decoding pointer-sized blobs. The whole run is capped by
   [`OPENSTORAGE_LFS_GC_OBJECT_BUDGET`](../reference/configuration.md#openstorage_lfs_gc_object_budget).
   A partial walk never marks or retires anything.
3. Clears the *unreferenced-since* stamp on rows it found and stamps rows it did not.
4. Only with [`OPENSTORAGE_LFS_GC_DELETE_ENABLED`](../reference/configuration.md#openstorage_lfs_gc_delete_enabled):
   retires rows stamped for a full
   [`OPENSTORAGE_LFS_GC_GRACE`](../reference/configuration.md#openstorage_lfs_gc_grace).
5. Refunds the freed object slots and quota bytes on a later run, once a destructive LFS
   sweep that **started after** the retirement completes cleanly.

A `507`-wedged repository accepts new objects again once enough junk rows retire and a
covering sweep settles their refunds.

Live objects are protected three ways:

- The delete re-checks the stamp and pinned ref generations under the registration lock.
- Every "already stored" answer on the batch/PUT surfaces clears the stamp, giving the
  client a full grace window to push its pointer.
- A row retired anyway re-registers on the next batch upload, which gets a real upload
  action.

### Operating LFS reclamation

**Retirement depends on the sweeper.** It deletes registry rows only. The blobs become
orphans that the [sweeper](#the-orphan-sweeper) reaps under `OPENSTORAGE_SWEEP_DELETE_LFS`
(plus the ADR-41 legacy gate for v0.2.0-spelled keys).

- **Startup check:** arming `OPENSTORAGE_LFS_GC_DELETE_ENABLED` on a live sweep schedule
  requires the LFS reap and a grace of at least max(`OPENSTORAGE_SWEEP_AGE_THRESHOLD`,
  `OPENSTORAGE_SWEEP_INTERVAL`, 1 hour).
- **Runtime check:** refunds happen only if a destructive LFS sweep finished its keyspace
  with zero errors within the grace window. Otherwise retirement demotes to report-only
  (`reap_watermark_stale`). Failing sweeps (delete outages, the S3 versioning guard,
  candidates with no mod time) and the emergency stop `OPENSTORAGE_SWEEP_INTERVAL=0`
  therefore freeze refunds automatically.
- A fresh deployment stays report-only until its first clean destructive LFS sweep.
- Legacy-spelled rows also wait for a sweep with legacy reaping armed.

Refund settlement internals are in the
[configuration reference](../reference/configuration.md#openstorage_lfs_gc_delete_enabled)
and ADR-42.

**Scans have deadlines.** Each namespace member's cache hydration and reachability walk
run under fixed, compiled-in 10-minute deadlines. On Linux, the walk's git subprocesses
also run under a compiled-in 2 GiB heap cap, so an oversized tree kills only its own
subprocess. A repository that runs out of time or memory fails that pass and is retried
on a later rotation. If it keeps failing, repack it or accept that it stays unreclaimed.

> [!IMPORTANT]
> **Upgrade the whole fleet before arming deletes.** A pre-reclamation replica answers
> "already stored" without clearing the stamp, so its client can lose the row to a
> retirement racing its pointer push. Report-only workers do not close that race.
>
> 1. Do not set `OPENSTORAGE_LFS_GC_DELETE_ENABLED` until every replica serving LFS
>    requests runs a build with this feature (same staging as the
>    [sweep delete flags](#the-orphan-sweeper)).
> 2. If report-only scans ran **during** the roll, wait one full `OPENSTORAGE_LFS_GC_GRACE`
>    after the last pre-touch replica stops before arming deletes.
> 3. Simpler: leave `OPENSTORAGE_LFS_GC_INTERVAL` unset until the fleet is upgraded. No
>    scan, no marks, no wait.

**Stage the rollout.**

- Run with `OPENSTORAGE_LFS_GC_DELETE_ENABLED` off first. `rows_eligible` shows what a
  delete-enabled run would retire.
- An abusive writer can reset its grace clocks by re-uploading junk. Revoke the token to
  stop it; reclamation converges once the abuse stops.
- To unwedge now, raise
  [`OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO`](../reference/configuration.md#openstorage_lfs_max_objects_per_repo).

Scan progress persists in the ref store (`job_scan_state`), so on multi-replica
PostgreSQL fleets any replica, or a restarted one, resumes where the last pass stopped.
Ref movement in a repository restarts its row settlement from the top.

### Reading a reclamation report

Each run logs one `lfs reclaim pass finished` line:

| Field | Meaning |
|---|---|
| `repos_scanned` / `repos_skipped` / `repos_over_budget` | work-queue outcomes (skipped = purged or metadata-less; over-budget = the repo's walk plus row settlement exceeds the full budget — raise it) |
| `objects_walked` | work units charged against the run budget (each ref: one; each object: one, plus one per 4 KiB of raw size) |
| `rows_examined` / `rows_referenced` | registry rows diffed (each also charges the run budget), and how many a reachable pointer references |
| `rows_marked` / `rows_cleared` | stamps added / removed this run |
| `rows_in_grace` | unreferenced rows still inside the grace window |
| `rows_eligible` / `rows_retired` / `rows_spared` | grace-expired rows; how many retired; how many the guarded delete declined at the last moment (concurrent touch, ref movement, purge fence) |
| `bytes_retired` | quota bytes refunded |
| `dry_run` | `true` while `OPENSTORAGE_LFS_GC_DELETE_ENABLED` is off |
| `reap_watermark_stale` / `rows_deferred_reap_stale` | `true` (and the eligible rows deferred) when no destructive LFS sweep completed cleanly within the grace window — refunds resume once one does |
| `rows_deferred_legacy_unarmed` | eligible legacy-spelled rows deferred because legacy reaping is not armed end-to-end (`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY` off, or the fresh watermark records a legacy-unarmed sweep) |
| `refund_rows_settled` / `refund_bytes_settled` / `refund_buckets_pending` | parked refunds settled this run, and refunds still waiting for a covering sweep |

## Reconciling registered-but-missing bytes

The registry and blob storage normally agree, because every deletion path quarantines
first. Storage-layer loss can still leave a **registered row whose bytes are gone**: a
bucket lifecycle rule, an operator `rm`, a storage incident. That hurts twice:

- **Integrity:** a missing registered pack may back committed refs (clones fail). A
  missing LFS object 404s until a restore source appears.
- **Quota** ([#101](https://github.com/coder/openstorage/issues/101)): the admission floor
  is `max(0, listing − counter)`. Missing bytes shrink the listing but not the counter,
  hiding up to that many rowless legacy bytes from admission. If the object is restored
  later, physical usage can exceed the quota by that amount.

The absence reconciliation job
([ADR-43](../adr/043-absence-reconciliation-covers-registered-but-missing-bytes.md))
limits that window to one pass. Enable it with
[`OPENSTORAGE_ABSENCE_RECONCILE_INTERVAL`](../reference/configuration.md#openstorage_absence_reconcile_interval)
(unset by default). Each pass pages `lfs_objects` and `packs` rows from resumable,
budget-limited cursors and `HEAD`s each row's blob:

| Result | What happens |
|---|---|
| **Present** | Nothing. One probe per row |
| **LFS row absent under its registered spelling but fully present under its ADR-41 twin** (canonical `lfs/<repo>/o2/<oid>` vs legacy `lfs/<repo>/<oid>`) | Reported (`lfs_rows_twin_only`), never charged. Download-time restore heals the spelling |
| **Present but shorter than registered** (truncated blob, or short surviving twin) | Partial loss. The deficit is covered like absence (`lfs_rows_truncated` / `pack_rows_truncated`) |
| **Truly absent** | Logged loudly. With quota enforced, charged as a TTL'd restore cover reservation, so every admission sees the missing bytes until they are restored or the row is retired/purged |
| **Absent pack row registered before this feature** | No recorded size (`pack_rows_unsized`). Reported, but not charged until a repack fold rewrites the row with its size |

What else to know:

- **Non-destructive.** The job only inserts/refreshes reservation rows and bumps a
  generation counter. It is safe to run continuously.
- **Skips** purge-fenced repositories and superseded `pack_retained` blobs (nothing
  restores those).
- **Fleet-safe covers.** Covers live in the database, one per affected repository and
  registry class, and every pass renews them. Any replica keeps every loss covered.
- **Cadence is checked at startup:** interval ≤ TTL/2 and pass wall clock ≤ TTL/4, so
  covers cannot lapse between passes.
- **Budget:** keep it above twice the live cover population. Renewal still covers
  everything when over budget (`renewal_over_budget`), and discovery keeps its
  guaranteed half-budget allowance.
- On PostgreSQL the job has its own single-worker River queue, so slow webhook deliveries
  cannot delay renewals.

> [!WARNING]
> **Fail-closed fence.** If cover renewal fails and the last healthy renewal is too old
> (covers could lapse), finite quota-gated writes (pushes, LFS uploads, adoptions) answer a transient "quota
> accounting temporarily degraded, retry later". Writes resume after a healthy pass
> completes one full error-free recovery rotation of both registries (a rotation dirtied
> by any stat or cover error never counts). Internal restore and cover reservations stay
> exempt so recovery can proceed.

> [!IMPORTANT]
> **Upgrade the whole fleet first** (ADR-43). Only fence-aware binaries enforce the fence
> and record lapse evidence. Complete rolling upgrades within one reservation TTL,
> and enable the job only after every node is upgraded. The boot log warns while
> the job runs with quota enforcement.

Cover and fence details:
[`OPENSTORAGE_ABSENCE_RECONCILE_INTERVAL`](../reference/configuration.md#openstorage_absence_reconcile_interval)
and
[`OPENSTORAGE_ABSENCE_RECONCILE_BUDGET`](../reference/configuration.md#openstorage_absence_reconcile_budget).

### Reading a reconciliation report

Each pass logs one `absence reconcile pass finished` line:

| Field | Meaning |
|---|---|
| `lfs_rows_examined` / `pack_rows_examined` | registry rows point-checked this pass (each charges the budget) |
| `lfs_rows_absent` | LFS rows missing under BOTH spellings — genuine storage loss |
| `lfs_rows_truncated` / `pack_rows_truncated` | rows present but shorter than registered — the deficit is covered like absence |
| `lfs_rows_twin_only` | rows fully present only under their ADR-41 twin spelling (restorable, not charged) |
| `pack_rows_absent` / `pack_rows_unsized` | missing registered packs (integrity!); how many predate size recording and cannot be charged |
| `covers_charged` / `bytes_covered` | restore covers charged/refreshed this pass and their total bytes |
| `cover_errors` / `stat_errors` | covers that failed (e.g. the per-repo reservation-row cap) and probes that errored — both retried next pass |
| `rows_skipped_purged` | rows of purge-fenced repositories skipped |
| `covers_touched` | live cover rows whose TTL the pass-start bulk touch re-stamped |
| `covers_renewed` | live cover rows re-charged by the renewal phase |
| `covers_rotated` / `covers_released` | covers re-minted smaller after heals (stale row deleted), and covers released (fully healed, emptied of registry rows, or a superseded incarnation retired) |
| `covers_decaying` | pre-upgrade nonce-keyed covers left to expire by TTL (never deleted — an old replica may still depend on them mid-roll) |
| `renewal_over_budget` | the live cover population outran the renewal allocation — every cover still renewed; raise the budget above twice the live cover population |
| `renewal_incomplete` | the pass deadline cut renewal short (resumed first next pass). If persistent, investigate the store: covers lapse if renewals cannot land within the TTL |
| `lfs_traversals` / `pack_traversals` | completed full rotations per registry (fleet-durable; heals are shed within one clean traversal) |
| `quota_enforced` | `false` means detection-only (no quota admission to protect) |

Any nonzero `lfs_rows_absent` or `pack_rows_absent` means storage lost registered bytes.
Investigate the storage layer. The loss then either heals (restores re-register and the
covers expire) or converges to removal (unreferenced LFS rows retire through
[reclamation](#reclaiming-unreferenced-lfs-objects); purges drop whole repositories).
