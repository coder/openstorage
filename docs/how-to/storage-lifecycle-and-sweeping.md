# Operate the storage lifecycle: repack, quotas, sweeping

**Goal:** answer the questions this system's storage design raises in operation — *why is
the bucket growing, what does repack do and cost, why was a push denied, and when is it
safe to let the sweeper delete things.*

This is the operator runbook. The design rationale behind every mechanism here — why
orphans exist at all, why repack keeps unreachable objects, how the quota ledger stays
exact across replicas — is in the [storage engine explanation](../explanation/storage-engine.md).
Every tunable lives in the [configuration reference](../reference/configuration.md#maintenance-jobs).

## Why is the bucket growing?

Pack storage is append-oriented: writers only ever add immutable, content-addressed packs,
and nothing is deleted synchronously. Everything under the `packs/` prefix falls into one
of these classes:

| Class | Created by | Reclaimed by |
|---|---|---|
| **Registered packs** | Every push and REST write publishes a pack and registers it | [Repack](#background-repack) folds many into one (the originals become *retained*) |
| **Retained (superseded) packs** | A repack fold replaced their registry rows; the blobs linger | Sweeper's superseded reap ([`OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`](../reference/configuration.md#openstorage_sweep_delete_superseded), on by default) after the age threshold |
| **Orphan packs** | Crashed/interrupted writers (pack published, registration never committed), lost repack races | Sweeper — **list-only by default**; deletion is the explicit [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#openstorage_sweep_delete_enabled) opt-in |
| **LFS objects** (`lfs/<repo_id>/o2/<oid>` canonical keys; `lfs/<repo_id>/<oid>` legacy pre-adoption keys, ADR-41) | Verified LFS uploads publish the object and register it in the LFS registry | Like registered packs, permanent while the repository lives; the [repository purge](#reclaiming-deleted-repositories) reclaims them with the rest of a deleted repository |
| **Orphan LFS objects** | Crashed uploads (published, registration never committed) and objects uploaded before the LFS registry existed | Sweeper — **list-only by default**; deletion is the separate [`OPENSTORAGE_SWEEP_DELETE_LFS`](../reference/configuration.md#openstorage_sweep_delete_lfs) opt-in (see [the caveat](#lfs-orphans-and-pre-registry-objects)) |
| **Quarantine copies** (`sweep/quarantine/<nonce>/…` keys) | Delete-mode sweeps copy every blob aside before deleting it | The sweep itself purges backups older than the purge window — `max(age threshold, 1h)` — while a destructive mode is on |

One more growth source sits outside this lifecycle: on versioned S3 buckets, **old
object versions** (the reason destructive sweeping
[refuses versioned buckets](#s3-bucket-versioning)).

In the experimental zero-infra mode
([ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md))
the bucket also carries the **reserved `_wal/` prefix** — the ref/metadata authority
objects themselves, never blob payloads. They are small, sit outside every class above,
and are invisible to the sweeper (its listing prefixes are `packs/`, `lfs/`, and
`sweep/quarantine/`, and its disposal paths fail-fast assert that no `_wal/` key ever
reaches them). Never write to or delete under `_wal/` yourself.

Also note what does *not* reclaim space: **deleting branches** — repack preserves the
whole registered object set, so unreachable objects are carried forward by design
([why](../explanation/storage-engine.md#why-repack-packs-the-union-not-reachable-objects)) —
and **deleting repositories on its own**: `DELETE /repos` soft-deletes the metadata
record — the name stays burned (recreating the same URL is rejected as already
existing) — while the repository's registry rows and blobs stay put and the sweeper
keeps counting them live. Reclamation is the separate, explicitly-enabled
[repository purge](#reclaiming-deleted-repositories).

## Inspect what a repository stores

List a repository's blobs under `packs/` straight from the blob store:

```sh
openstorage list-packs --repo repo_01k5abc...   # takes the server-generated RepoID
```

`--repo` takes the **RepoID** (`repo_...`, returned by repo creation and
`GET /api/v1/repo`), *not* the repository name. Namespace pushes store under sibling
prefixes — list them by suffixing the key (`--repo repo_01k5abc...+ephemeral`); a
quota denial's `stored` bytes cover the base **and** all namespace prefixes. The command
reads
`OPENSTORAGE_S3_URL`/`OPENSTORAGE_PG_URL` (or `--data-dir` in local mode) from the
environment — see the [CLI reference](../reference/cli.md#openstorage-list-packs).

Fleet-wide, the periodic [sweep report](#reading-a-sweep-report) counts live vs. orphan
blobs per run — but totals bytes for orphan candidates only. `list-packs` reports
**physical** bytes under a repository's prefixes — live, retained, and orphan blobs
alike, the same measure the quota's listing floor uses — without consulting the
registry; no surface reports registry-live bytes on their own, so use blob-store metrics
for fleet-wide capacity tracking.

## Reclaiming deleted repositories

Soft-delete alone reclaims nothing. Two paths — the same purge primitive — permanently
remove a deleted repository's registry and eventing rows (refs, packs, LFS objects,
credentials, upstream link) and its blobs (base packs, namespace packs, LFS objects);
only the repository row itself is retained, stamped as a purge tombstone:

- **Retention window:** set
  [`OPENSTORAGE_REPO_RETENTION`](../reference/configuration.md#openstorage_repo_retention)
  (for example `720h` for 30 days) and a purge pass on the
  [sweep](#the-orphan-sweeper) cadence reclaims every repository soft-deleted longer
  than that.
- **Operator command:**
  [`openstorage purge-repo --yes <repo-id>`](../reference/cli.md#openstorage-purge-repo)
  purges one repository immediately (it must have been soft-deleted at least an hour
  earlier).

Both are **irreversible** — there is no restore after a purge. The repository row is
kept as a permanent tombstone, so the burned `(org, url)` name is **never released**:
an unexpired token carries the repository URL in its claim and would otherwise
authorize against a new repository created at that URL. (In s3wal mode the tombstone is
the repository group's `_wal/repo/<id>` manifest with its terminal `purged_at` fence —
equally permanent; deleting the object by hand would un-burn the name and reopen exactly
those races.) Both also share the
[fleet-upgrade prerequisite](../reference/configuration.md#openstorage_repo_retention):
every serve replica and hook-running git node must run a fence-aware build before the
first purge.

Purged repositories drop out of every registry surface except the tombstoned metadata
row (soft-deleted plus a purge timestamp), which keeps `DELETE`-then-recreate races and
stale-token replays structurally impossible. A purge also write-fences the repository
in the ref store before touching anything: a request that resolved the repository just
before its deletion and is still in flight fails its ref update and its pack/LFS
registration instead of re-registering rows behind the purge, and credential/upstream
writes only ever attach to live repositories. Such a push may still have *published* its blob
after the purge's scan (uploads precede registration) — the fence makes those bytes
provably permanent garbage, so later sweeps reap them in every class once aged
(purge-awareness is built in, independent of the retention setting and the delete
opt-ins), and re-running `purge-repo` on the purged repository rescans its prefixes
immediately.

The purge is crash-safe by ordering, the inverse of the write path's
publish-before-registration: registry rows are tombstoned **before** any blob is
deleted (ref rows first of all), so an interruption can never leave a committed ref
pointing at missing bytes — only tombstones the next pass (or a `purge-repo` re-run)
finishes reaping. Blob deletion itself goes through the sweeper's quarantine-first
handshake: bytes move to `sweep/quarantine/` and are freed once the backups age past
the purge window on a later sweep, so the moment storage actually shrinks is one sweep
behind the purge (purge-aware sweeps age those backups out even when all `*_DELETE_*`
flags are off).

## Background repack

Small pushes accumulate one pack each. When a repository's registered-pack count reaches
[`OPENSTORAGE_REPACK_THRESHOLD`](../reference/configuration.md#openstorage_repack_threshold)
(default 10), a background job folds registered packs into one consolidated pack. It runs
as a durable [River](https://riverqueue.com) job on Postgres (in-process in local mode) —
in a fleet, any replica may execute it.

What to expect in the logs:

```
repack trigger: scheduled fold   repo=<id> packs=<n> threshold=<t>
repack: folded packs             repo=<id> folded=<n> pack=<new-pack-id>
```

Operationally relevant properties:

- **Counts converge below the threshold, not to one pack.** A fold that snapshots all but
  the very latest pushes can legitimately settle at `threshold - 1` packs. That is
  converged, not stuck. The one exception: when no combination of ≥ 2 packs fits the
  fold budgets (next bullet), nothing is foldable and the count legitimately stays at or
  above the threshold.
- **Bounded folds.** Each fold is capped at compiled-in budgets (2 GiB of pack bytes,
  ~1M objects, 5 minutes — not tunable via environment). Repositories above the budgets
  fold incrementally: each fold schedules a successor until counts converge.
  `repack: budgets select nothing to fold` means no combination of ≥ 2 packs fits the
  budgets — a no-op, not an error.
- **Usage grows before it shrinks.** A fold adds the consolidated pack while the
  superseded originals stay in the bucket (and stay
  [quota-counted](#pack-quotas)) until the sweeper reaps them.
- **Repack is exempt from the pack quota, by design** — an at-quota repository must still
  be able to compact itself. Repack never increases the *registered* set; it only adds a
  replacement for rows it removes.
- `repack: lost fold race ...` lines are benign: two replicas raced, one won, the loser's
  upload is left for the sweeper.

Tuning: raise the threshold to fold less often (bigger, rarer folds), lower it (minimum
2) for latency-sensitive read paths on push-heavy repos. `0` disables triggering.

## Pack quotas

Every repository has a storage-bytes quota
([`OPENSTORAGE_PACK_QUOTA_BYTES`](../reference/configuration.md#openstorage_pack_quota_bytes),
default 10 GiB, `0` disables) covering its packs **and its LFS objects**
([#51](https://github.com/coder/openstorage/issues/51)). A repository and its
`+ephemeral` / `+import` namespaces share one quota, accounted against the base
repository — including LFS, whose object space the namespaces share outright.

What a denial looks like — over git:

```
remote: openstorage hook: pre-receive: repository pack quota exceeded: push adds <n> bytes
to <n> bytes stored (+<n> bytes reserved by in-flight pushes), quota is <n> bytes
```

Over REST (commit-pack, diff-commit, restore-commit, merge, notes writes): a `403` with
`repository pack quota exceeded: write adds ... quota is <n> bytes`. A fork whose source
closure alone exceeds the quota is denied up front with
`repository pack quota exceeded: fork closure is <n> bytes, quota is <n> bytes`.

Over Git LFS: the batch API answers per-object `507` errors (no transfer URL is issued)
and the verified upload transfer re-checks the request's actual `Content-Length` — a
batch that under-declares sizes is caught at the transfer. Either way git-lfs surfaces
`repository storage quota exceeded: upload adds <n> bytes to <n> bytes stored (+<n>
bytes reserved by in-flight uploads), quota is <n> bytes` and the push fails. LFS
admission happens in two stages: the batch API runs an **advisory** fast-fail check on
the declared sizes (an over-quota batch answers per-object `507`s and issues no upload
URLs, but reserves nothing durable — batch-time holds proved renewable-for-free), and
the **durable** reservation is charged per object at the verified upload itself, right
before its bytes are published, then settled into the usage counter when registration
completes — expiring after the same
[`OPENSTORAGE_PACK_RESERVATION_TTL`](../reference/configuration.md#openstorage_pack_reservation_ttl)
if registration never lands. Between those stages nothing is held, so two near-quota
clients can both receive upload URLs and one can transfer its whole object before the
durable admission rejects it — wasted transfer, never quota overshoot. Downloads are
never quota-gated.

Reading the numbers: admission is `stored + reserved + incoming ≤ quota`, where

- **stored** counts every registered pack and LFS object **plus retained (superseded)
  bytes** — after a repack fold, headroom stays reduced until the sweeper reaps the
  superseded originals and the report shows `retained_bytes_reclaimed`. It is also
  floored by the *physical* bytes under the repository's pack and LFS prefixes, which
  include aged **orphan** blobs — and since orphan deletion is off by default, a
  repository with abandoned uploads can keep hitting denials that only
  [enabling delete mode](#before-enabling-delete-mode-fleet-prerequisite) resolves;
- **reserved** counts other writers' in-flight admissions. A crashed writer's
  reservation keeps gating only until its
  [`OPENSTORAGE_PACK_RESERVATION_TTL`](../reference/configuration.md#openstorage_pack_reservation_ttl)
  (default 1h) elapses — admission ignores expired rows immediately; the sweeper's
  `reservations_expired` pruning is just garbage collection of those rows, not what
  unblocks pushes. A retry of the *same* push is never double-charged (the same content
  key refreshes the reservation);
- pushing content that is already stored (identical pack key) adds zero incoming bytes.

So a push denied right after heavy write traffic may succeed later without any config
change — once the next sweep reclaims retained bytes and crashed writers' reservations
pass their TTL. If
denials persist, either raise the quota or accept that the repository's registered
history is simply that large (remember:
[deleting branches does not reclaim space](#why-is-the-bucket-growing)).

The server also logs quota pressure on the git surface before the authoritative
pre-receive check:

```
quota: receive-pack for <repo> at/over quota (<n> pack bytes stored >= <n>): enforcement defers to pre-receive
```

## The orphan sweeper

Every [`OPENSTORAGE_SWEEP_INTERVAL`](../reference/configuration.md#openstorage_sweep_interval)
(default 1h), a sweep job: heals the quarantine, prunes expired quota reservations, then
streams the bucket's `packs/` and `lfs/` listings and classifies every blob against its
registry (pack rows for pack keys, LFS rows for LFS keys). Blobs with no registry row
that are older than
[`OPENSTORAGE_SWEEP_AGE_THRESHOLD`](../reference/configuration.md#openstorage_sweep_age_threshold)
(default 24h) are orphan candidates.

Four independent knobs control what a sweep may *do*:

| Mode | Default | Acts on |
|---|---|---|
| List-only (no flag) | **active** | Reports orphans of every class, deletes nothing |
| [`OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`](../reference/configuration.md#openstorage_sweep_delete_superseded) | `true` | Only blobs a repack fold provably superseded (durable `pack_retained` records); reclaims their quota bytes |
| [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#openstorage_sweep_delete_enabled) | `false` | General pack orphans (crashed pushes, lost races) — the risky class, hence opt-in |
| [`OPENSTORAGE_SWEEP_DELETE_LFS`](../reference/configuration.md#openstorage_sweep_delete_lfs) | `false` | LFS orphans — separate from the pack opt-in because of the [pre-registry caveat](#lfs-orphans-and-pre-registry-objects); legacy-spelled keys additionally need the flag below |
| [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](../reference/configuration.md#openstorage_sweep_delete_lfs_legacy) | `false` | Legacy-spelled LFS keys (ADR-41): the post-roll cleanup opt-in — off, no sweep mode deletes them (retained adoption sources feed old replicas' reads mid-roll) |

### LFS orphans and pre-registry objects

The LFS registry (which upload settlement writes) was introduced with
[#51](https://github.com/coder/openstorage/issues/51): **every LFS object uploaded by
an older build has no registry row** and therefore shows up as an orphan candidate,
however live its pointers are. That is why LFS deletion is a separate opt-in and is not
covered by `OPENSTORAGE_SWEEP_DELETE_ENABLED`. Stored objects are adopted into the
registry whenever a write-scoped touch sees them (an upload batch naming the object, or
a re-upload of the same content) — after a one-time sha256 verification, since
presigned-era uploads never proved their content: a mismatched legacy blob is removed
and re-uploaded through the verified path instead of adopted. With a
[storage quota](../reference/configuration.md#openstorage_pack_quota_bytes) enforced,
the migration copy passes real quota admission — the retained legacy source keeps
counting until this sweeper reclaims it, so adopting an object needs headroom for a
second copy and answers `507` at the quota edge (free space, raise the quota, or run
the post-roll cleanup to migrate it). Active repositories
therefore converge on their own. Before
setting `OPENSTORAGE_SWEEP_DELETE_LFS=true`, run list-only until the candidate lines
stop naming objects that should live; re-pushing a repository's LFS content (batch
adoption skips re-transfers of stored objects) is the fastest way to backfill a
repository you cannot afford to lose objects from.

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
| `orphans` / `orphan_bytes` | Aged, unregistered candidates this run — **all** classes: repack-superseded blobs and LFS orphans count here too until reaped (the candidate lines' keys distinguish them) |
| `deleted` | Candidates reaped this run (superseded reaps included). Reaping moves bytes to quarantine, so it frees nothing by itself |
| `bytes_reclaimed` | **Net** bytes returned by purging aged quarantine backups whose blob stayed unregistered. A backup whose original was restored purges without being counted (net zero — the original is live again). Lags `deleted` by the quarantine purge window, `max(age threshold, 1h)` |
| `superseded_deleted` / `retained_bytes_reclaimed` | The provably repack-superseded subset of `deleted`, and the **quota headroom** their reaps returned |
| `reservations_expired` | Crashed-writer quota reservations pruned this run |
| `delete_enabled` / `delete_superseded` / `delete_lfs` | The **configured** modes — a [guard downgrade](#s3-bucket-versioning) to list-only is *not* reflected here; it shows only as the separate `delete guard failed; this run is list-only` error line |

Every candidate is also logged individually as it is classified
(`orphan pack candidate key=… size_bytes=… age=… delete_enabled=… delete_superseded=… delete_lfs=…`),
so in list-only mode you can audit exactly what deletion *would* reap.
A healthy steady state is `orphans=0` or a small, stable count;
monotonically growing `orphan_bytes` means crashed writers or fold races are outpacing
your (disabled) deletion and deserves investigation before you consider enabling it.

### Deletion is quarantine-first, and heals itself

When a sweep does delete, it never hard-deletes directly: every blob is first copied to a
`sweep/quarantine/<nonce>/…` key, the registry is re-checked *after* the delete (a registration that
landed mid-delete is restored on the spot), and **every** sweep — list-only included —
starts by restoring any registered-but-missing **or truncated** pack from quarantine.
The heal is size-aware: a registered blob that exists but is *shorter* than its
registered size (a stale presigned overwrite, storage-layer damage) is healed from a
backup holding the complete bytes, not trusted on existence alone. Rolling back to
list-only after a scare therefore still repairs anything a delete-enabled crash left
behind; look for `orphan pack sweep: healed registered pack from quarantine` and
`healed truncated registered object from quarantine`. LFS
restores are additionally digest-verified against the key's OID: a backup taken from a
pre-registry blob whose bytes never hashed to its OID is *never* restored (logged as
`NOT restoring LFS quarantine backup failing content verification`) — a registered
object in that state repairs itself on the next upload touch instead. LFS deletes are
also serialized with registration (the same in-transaction guard verified uploads use),
so a concurrent upload either keeps its bytes untouched or provably re-publishes them
after the delete. Quarantine
backups are purged only while a destructive mode is on, and only once older than
`max(age threshold, 1h)` — the floor guarantees a delete handshake is provably dead
before its backup goes away, so tuning the age threshold low does not shorten it.
The purge additionally never collects a backup while the registered original cannot be
verified complete: pack rows registered before the size column existed record no size
(`bytes=0`), so their backups are spared until a repack fold records one (logged as
`sparing backup of a registered object with unknowable size`), and a truncated
original the heal could not repair this pass (its backup is itself short, or another
worker holds the object's claim) keeps its backup for the next sweep to re-examine.

### S3 bucket versioning

On a bucket with versioning enabled (or suspended), deletes only write delete markers —
every version stays stored and billed, so "reclaimed" bytes would be a lie:

- `OPENSTORAGE_SWEEP_DELETE_ENABLED=true` and `OPENSTORAGE_SWEEP_DELETE_LFS=true`
  **fail startup** against such a bucket (or when versioning state cannot be verified —
  grant `s3:GetBucketVersioning` or stay list-only).
- The default superseded reap does not fail startup; each run re-checks and **downgrades
  itself to list-only** on drift, logging
  `orphan pack sweep: delete guard failed; this run is list-only`.

### Before enabling delete mode (fleet prerequisite)

> **⚠️ Enable destructive sweeping only once EVERY node — serve replicas *and* any
> hook-running git nodes — runs a build with the post-registration publish verify
> (`blob.EnsurePublished`).** All current builds have it (it shipped together with the
> sweeper), so this matters exactly when a mixed-version fleet still includes older
> builds mid-upgrade.
>
> A mixed fleet cannot lose data — quarantine plus the heal pass restore any mis-reaped
> registered pack — but an **old** writer racing a delete-mode sweep can register a pack
> whose blob was just reaped and not notice, leaving that pack unavailable (failed
> clones/hydrations for that repo) until the next sweep's heal pass restores it.
> **List-only mode is safe on any fleet**, and if you must run pre-`EnsurePublished`
> nodes, set `OPENSTORAGE_SWEEP_DELETE_SUPERSEDED=false` as well — the safety argument is
> the same for both destructive classes. On pre-0.15 replicas, use the old spelling,
> `CODESTORAGE_SWEEP_DELETE_SUPERSEDED=false`; those binaries do not recognize the new prefix.

A conservative enablement sequence:

1. Audit first: leave `OPENSTORAGE_SWEEP_DELETE_ENABLED=false` (the default) for at
   least one age-threshold window; watch [sweep reports](#reading-a-sweep-report) and
   audit the candidate lines. Note the out-of-the-box default is list-only for *general*
   orphans only — superseded reaping is already on. If the audit window must be fully
   non-destructive, set `OPENSTORAGE_SWEEP_DELETE_SUPERSEDED=false` for its duration and
   restore it afterward.
2. Confirm the whole fleet — including any node that executes git hooks — is on a current
   build (see the prerequisite above).
3. Leave `OPENSTORAGE_SWEEP_DELETE_SUPERSEDED=true` (the default) so repack-superseded
   bytes are reclaimed and quota headroom returns.
4. Opt into `OPENSTORAGE_SWEEP_DELETE_ENABLED=true` only if orphan bytes materially
   accumulate — crashed pushes are rare, so for many deployments list-only is a fine
   permanent state.
5. After the first delete-enabled sweeps, check `deleted` matches the candidates you
   audited. `bytes_reclaimed` stays `0` until the quarantine backups age past the purge
   window (`max(age threshold, 1h)`) — deletion and physical reclaim are different
   cohorts, so an initial `0` is expected, not a failure signal.

## Reclaiming unreferenced LFS objects

Registered LFS objects are permanent by default: the sweeper treats every
registered row as live, and nothing in the normal lifecycle deletes one. That
permanence has a failure mode
([#73](https://github.com/coder/openstorage/issues/73)): a `git:write` holder
can direct-PUT up to the per-repo object cap
([`OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO`](../reference/configuration.md#openstorage_lfs_max_objects_per_repo),
default 100,000) in *tiny* objects nobody references — a few hundred kilobytes
of actual bytes — after which every **new** LFS object answers `507` for every
collaborator, indefinitely.

The reclamation job
([ADR-42](../adr/042-lfs-reclamation-is-mark-and-sweep-over-reachable-pointer-files.md))
closes the loop with reachability: an LFS object is *referenced* when some
reachable ref in the repository's namespace group carries a pointer file
naming its OID. Every
[`OPENSTORAGE_LFS_GC_INTERVAL`](../reference/configuration.md#openstorage_lfs_gc_interval)
(unset by default — the job is opt-in), a run:

1. pages repositories holding LFS objects from a rotating cursor, pinning each
   namespace group's ref generations;
2. hydrates their git caches and walks `git rev-list --objects` across all
   reachable refs, decoding pointer-sized blobs, within
   [`OPENSTORAGE_LFS_GC_OBJECT_BUDGET`](../reference/configuration.md#openstorage_lfs_gc_object_budget)
   for the whole run (a partial walk never marks or retires anything);
3. clears the *unreferenced-since* stamp on rows whose OIDs the walk found,
   stamps rows it did not, and — only with
   [`OPENSTORAGE_LFS_GC_DELETE_ENABLED`](../reference/configuration.md#openstorage_lfs_gc_delete_enabled) —
   retires rows whose stamp survived a full
   [`OPENSTORAGE_LFS_GC_GRACE`](../reference/configuration.md#openstorage_lfs_gc_grace)
   window. The freed object count and quota bytes are refunded
   separately: each retirement parks its refund durably, and a later run
   settles it once a destructive LFS sweep that **started after** the
   retirement completes cleanly — only then is the orphaned blob provably
   reaped. A previously `507`-wedged repository accepts new objects again
   once enough junk rows retire and a covering sweep settles their
   refunds.

Retirement is defended in depth: the delete re-checks the stamp and the
pinned ref generations inside the same lock registrations take, and every
zero-growth "already stored" answer on the batch/PUT surfaces clears the
stamp first — a client that just learned an object exists holds a full grace
window to push its pointer. An object whose row was retired anyway is not
lost silently: the next batch upload gets a real upload action and the
verified bytes re-register.

Two operational notes:

- **Rows and bytes retire together.** Retirement deletes registry rows; the
  blobs become ordinary rowless orphans that the [sweeper](#the-orphan-sweeper)
  reaps under its own aged, quarantine-first delete gates
  (`OPENSTORAGE_SWEEP_DELETE_LFS`, plus the ADR-41 legacy gate for
  v0.2.0-spelled keys). The coupling is enforced twice. Startup validates
  the CONFIGURED regime: arming `OPENSTORAGE_LFS_GC_DELETE_ENABLED` on a
  live sweep schedule requires the LFS reap and a grace no shorter than the
  reap cadence (max(`OPENSTORAGE_SWEEP_AGE_THRESHOLD`,
  `OPENSTORAGE_SWEEP_INTERVAL`, the 1-hour quarantine retention floor)) —
  refunded cap slots whose blobs (or quarantine backups) nothing deletes in
  time would let upload → retire → re-upload cycles grow the storage
  prefixes past the cap that quota floors and sweep listings rely on. At
  runtime the coupling is OBSERVED, not assumed: every destructive LFS
  sweep pass that finishes its keyspace with zero errors records a durable
  fleet-wide watermark, and retirement refuses to refund slots — demoting
  to report-only, `reap_watermark_stale` in the run report — whenever no
  such pass completed within the grace window. Sweeps that are armed but
  failing (blob-store delete outages, passes outrunning the reclamation
  cycle, the S3 versioning guard demoting a run to list-only, a backend
  listing that reports no mod time for an offered candidate — its age is
  unknowable, so it is skipped, not reaped) therefore
  freeze refunds automatically until a clean destructive pass completes;
  the sweep emergency stop (`OPENSTORAGE_SWEEP_INTERVAL=0`) is just the
  planned instance of the same demotion. A fresh deployment stays
  report-only until its first clean destructive LFS sweep completes. The
  same observed coupling gates each refund individually: a completion
  watermark only proves a sweep that listed blobs *before* a given
  retirement — that sweep saw the registry row and spared the blob — so
  every retirement records the monotone sweep epoch it ran under and its
  refund waits in a durable per-epoch bucket until a destructive pass
  with a strictly greater epoch completes cleanly (legacy-spelled rows
  additionally wait for a legacy-armed completion). Run reports surface
  the flow as `refund_rows_settled` / `refund_bytes_settled` /
  `refund_buckets_pending`.
- **Hydration and walks are deadline-bounded.** Scanning a repository first
  materializes its git cache (downloading and indexing registered packs on a
  cold replica). Each namespace member's hydration AND its reachability walk
  run under fixed 10-minute deadlines — compiled in, like the repack
  budgets. The walk deadline hard-caps work the object budget cannot see
  coming: `rev-list` must inflate a tree before its children stream, so a
  giant compressible tree burns its expansion before budget cancellation
  lands. On Linux the walk's git subprocesses additionally run under a
  compiled-in 2 GiB heap cap, so a tree too large to inflate kills only its
  own subprocess instead of the worker. A repository that cannot finish in
  time (or in memory) fails that pass with an error and is retried on a
  later rotation; one that repeatedly exceeds the deadlines needs operator
  attention (repack it, or accept it stays unreclaimed).
- **Upgrade the whole fleet first.** The grace guarantee depends on every
  serving replica clearing the reclamation stamp when it answers "already
  stored" on the batch/PUT surfaces. A pre-reclamation replica answers
  without clearing, so a client it just told "stored" can lose the row to a
  retirement racing its pointer push — and report-only mode on the workers
  does not close that race. Do not set `OPENSTORAGE_LFS_GC_DELETE_ENABLED`
  until every replica serving LFS requests runs a build with this feature
  (mixed-fleet staging, same as the
  [sweep delete flags](#the-orphan-sweeper)). If report-only scans ran
  **during** the roll, the marks they stamped may already be grace-expired
  when the roll finishes, while the last pre-touch replica's final "stored"
  answers revoke nothing at shutdown: wait one full
  `OPENSTORAGE_LFS_GC_GRACE` after the last pre-touch replica stops before
  arming deletes, so every promise an old replica issued has had its full
  window to become a pushed pointer. Leaving `OPENSTORAGE_LFS_GC_INTERVAL`
  unset until the fleet is upgraded avoids the drain step entirely — no
  scan, no marks.
- **Stage the rollout.** Leave `OPENSTORAGE_LFS_GC_DELETE_ENABLED` off first
  and watch the run reports: `rows_eligible` is what a delete-enabled run
  would have retired. An actively abusive writer can keep re-uploading junk
  to reset its grace clocks — reclamation converges the moment the abuse
  stops; revoke the token to stop it now. For an immediate unwedge without
  waiting for any grace window, raise
  [`OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO`](../reference/configuration.md#openstorage_lfs_max_objects_per_repo).

A reclamation run logs one `lfs reclaim pass finished` line:

| Field | Meaning |
|---|---|
| `repos_scanned` / `repos_skipped` / `repos_over_budget` | work-queue outcomes (skipped = purged or metadata-less; over-budget = the repo's walk plus row settlement exceeds the full budget — raise it) |
| `objects_walked` | work units the scans charged against the run budget (each ref: one; each object: one, plus one per 4 KiB of raw size) |
| `rows_examined` / `rows_referenced` | registry rows diffed (each also charges the run budget), and how many a reachable pointer references |
| `rows_marked` / `rows_cleared` | stamps added / removed this run |
| `rows_in_grace` | unreferenced rows still inside the grace window |
| `rows_eligible` / `rows_retired` / `rows_spared` | grace-expired rows; how many actually retired; how many the guarded delete declined at the last moment (concurrent touch, ref movement, purge fence) |
| `bytes_retired` | quota bytes refunded |
| `dry_run` | `true` while `OPENSTORAGE_LFS_GC_DELETE_ENABLED` is off |
| `reap_watermark_stale` / `rows_deferred_reap_stale` | `true` (and the eligible rows deferred) when no destructive LFS sweep completed cleanly within the grace window — refunds resume once one does |
| `rows_deferred_legacy_unarmed` | eligible LEGACY-spelled rows deferred because legacy reaping is not armed end-to-end (`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY` off, or the fresh watermark records a legacy-unarmed sweep) — their blobs would never be physically reaped, so the slot refund would be unbacked |

Scan progress is fleet-durable: the rotation cursor and each repository's
row-settlement resume point persist in the ref store (`job_scan_state`), so
on multi-replica PostgreSQL fleets a pass scheduled on any replica — or
after a rolling restart — resumes exactly where the previous pass parked
instead of re-examining the same prefix forever. A settlement cursor is
fenced by the repository's pinned generations: any ref movement invalidates
it and settlement restarts from the top of that repository's keyspace.
Cursor parking also consults the repository purge fence in the same
transaction, so a purge racing a mid-settlement park can never resurrect a
cursor row for a repository that will never be scanned again.

## Reconciling registered-but-missing bytes

Every deletion path in open.storage quarantines before it deletes and heals
through digest-verified restores, so the registry and blob storage normally
never disagree. Storage-layer data loss — a bucket lifecycle rule, an
operator `rm`, a storage incident — can still leave a **registered row whose
bytes are physically absent**. That state is doubly bad:

- **Integrity:** a missing registered pack may be referenced by committed
  refs (clones fail); a missing LFS object 404s until a restore source
  appears.
- **Quota visibility** ([#101](https://github.com/coder/openstorage/issues/101)):
  the admission floor is `max(0, listing − counter)`, and the counter still
  includes the absent bytes the listing lacks — the floor deflates by the
  missing size, hiding up to that many rowless legacy bytes from admission.
  A write admitted through that deflated view is durable; when the missing
  object is later restored (download-time restore, a re-push, quarantine
  healing), physical usage can exceed the quota by the once-hidden amount.

The absence reconciliation job
([ADR-43](../adr/043-absence-reconciliation-covers-registered-but-missing-bytes.md))
closes that window to one pass. Enable it with
[`OPENSTORAGE_ABSENCE_RECONCILE_INTERVAL`](../reference/configuration.md#openstorage_absence_reconcile_interval);
each pass pages `lfs_objects` and `packs` rows from persistent-shape keyset
cursors (budget-limited, resumable — the sweeper's inverse direction) and
`HEAD`s each row's blob:

- **Present** — nothing happens; the common case costs one probe per row.
- **LFS row absent under its registered spelling but FULLY present under
  its ADR-41 twin** (canonical `lfs/<repo>/o2/<oid>` vs legacy
  `lfs/<repo>/<oid>`) — reported (`lfs_rows_twin_only`), never charged: the
  twin's bytes are still physically listed, and download-time restore heals
  the spelling.
- **Physically present but SHORTER than registered** (a truncated blob, or
  a short surviving twin) — a partial loss: the deficit deflates the floor
  exactly like absence and is covered/tracked the same way
  (`lfs_rows_truncated` / `pack_rows_truncated`).
- **Truly absent** — logged loudly and, when quota is enforced, charged as a
  TTL'd restore cover (the same reservation row restore paths renew) plus a
  floor-generation bump, so every admission sees the missing bytes until
  they are restored or their row is retired/purged. Discovered losses
  aggregate into ONE cover reservation row per affected repository and
  registry class — so the per-repository reservation-row cap cannot bind
  regardless of how many rows were lost — and the cover rows THEMSELVES are
  the durable tracking state: every pass starts by re-stamping every live
  cover's TTL in one database-clock statement (`covers_touched` — so
  protection never depends on the per-row work completing within the TTL),
  then lists the live cover rows and re-charges each one
  (`covers_renewed`), one work unit per row, under half the pass budget.
  Because renewal derives entirely from the
  database, any replica of a multi-replica PostgreSQL fleet — or a freshly
  restarted worker — keeps every discovered loss covered; nothing lives in
  process memory. Aggregates stay EXACT: each full traversal of a
  repository's rows recomputes its deficit total from scratch, growing the
  live cover in place as losses appear, minting the next incarnation of
  the key (and deleting the stale row — `covers_rotated`) when heals
  shrink the total, and releasing the row outright (`covers_released`)
  when everything healed. Renewal is the correctness core, so it always
  offers the FULL cover listing: a population beyond its budget
  allocation still renews every pass — flagged `renewal_over_budget`,
  while discovery keeps a guaranteed half-budget allowance — rather than
  round-robining past the TTL; raise
  the budget above twice the live cover population. On PostgreSQL the job runs on its own reserved single-worker River
  queue, so tenant-scalable work (webhook deliveries to slow endpoints)
  cannot delay a renewal past the cover TTL, and the unique periodic job
  serializes passes fleet-wide. Startup enforces the cadence-vs-TTL
  coupling (interval ≤ TTL/2, pass wall clock ≤ TTL/4), so renewals always
  land before live covers can lapse. Renewal is additionally guarded by a
  fail-closed admission fence: each successful renewal stamps a
  database-clock health watermark, and a renewal FAILURE that finds the
  watermark absent or stale (covers could lapse before the next pass)
  fences finite quota-gated writes — pushes, LFS uploads, adoptions
  answer a transient "quota accounting temporarily degraded, retry
  later" — until a healthy pass completes one full error-free recovery
  rotation of both registries (a rotation any pass dirtied with a stat
  or cover error never counts); internal restore and cover reservations
  stay exempt so recovery can proceed. The fence and its lapse-evidence
  trail are enforced only by fence-aware binaries: complete rolling
  upgrades within one reservation TTL and enable the job only after the
  whole fleet is upgraded (mixed-fleet prerequisite, ADR-43) — the boot
  log warns while the job runs with quota enforcement.
- **Absent pack rows registered before this feature** carry no recorded
  size (`pack_rows_unsized`): the loss is reported but cannot be charged
  until a repack fold rewrites the row with its size.

The job is non-destructive everywhere — it inserts/refreshes reservation
rows and bumps a generation counter, nothing else — so it is safe to run
continuously. Purge-fenced repositories are skipped (their rows and blobs
are already on their way out), and superseded `pack_retained` blobs are
deliberately out of scope: nothing ever restores them, so a deflated floor
is their true permanent state.

A reconciliation pass logs one `absence reconcile pass finished` line:

| Field | Meaning |
|---|---|
| `lfs_rows_examined` / `pack_rows_examined` | registry rows point-checked this pass (each charges the budget) |
| `lfs_rows_absent` | LFS rows missing under BOTH spellings — genuine storage loss |
| `lfs_rows_truncated` / `pack_rows_truncated` | rows physically present but shorter than registered — the deficit is covered like absence |
| `lfs_rows_twin_only` | rows fully present only under their ADR-41 twin spelling (restorable, not charged) |
| `pack_rows_absent` / `pack_rows_unsized` | missing registered packs (integrity!); how many predate size recording and cannot be charged |
| `covers_charged` / `bytes_covered` | restore covers charged/refreshed this pass and their total bytes |
| `cover_errors` / `stat_errors` | covers that failed (e.g. the per-repo reservation-row cap) and probes that errored — both retried next pass |
| `rows_skipped_purged` | rows of purge-fenced repositories skipped |
| `covers_touched` | live deterministic cover rows whose TTL the pass-start bulk touch re-stamped in one database-clock statement (protection never waits on the per-row listing) |
| `covers_renewed` | live cover rows re-charged by the renewal phase (fleet-derived from the durable cover listing — any replica renews every cover) |
| `covers_rotated` / `covers_released` | shrink re-mints (heals reduced a repository's exact re-derived total; the stale row was deleted) and outright releases (fully healed, emptied of registry rows, or a superseded incarnation retired) |
| `covers_decaying` | pre-upgrade nonce-keyed covers folded into deterministic incarnations and left to expire by TTL (never deleted — an old replica may still depend on them mid-roll) |
| `renewal_over_budget` | the live cover population outran the renewal allocation — every cover still renewed, and discovery keeps its guaranteed half-budget allowance; raise the budget above twice the live cover population |
| `renewal_incomplete` | the pass deadline cut the renewal listing short (resumed first next pass on the durable cursor) — investigate the store if persistent, covers lapse if renewals cannot land within the TTL |
| `lfs_traversals` / `pack_traversals` | completed origin-to-origin rotations per registry (fleet-durable; heals shed within one clean traversal) |
| `quota_enforced` | `false` means detection-only (no quota admission to protect) |

Any nonzero `lfs_rows_absent` or `pack_rows_absent` means storage lost
registered bytes: investigate the storage layer, and expect the loss to
either heal (restores re-register and the covers expire) or converge to
removal (unreferenced LFS rows retire through
[reclamation](#reclaiming-unreferenced-lfs-objects); purges drop whole
repositories).
