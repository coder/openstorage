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
| **Retained (superseded) packs** | A repack fold replaced their registry rows; the blobs linger | Sweeper's superseded reap ([`CODESTORAGE_SWEEP_DELETE_SUPERSEDED`](../reference/configuration.md#codestorage_sweep_delete_superseded), on by default) after the age threshold |
| **Orphan packs** | Crashed/interrupted writers (pack published, registration never committed), lost repack races | Sweeper — **list-only by default**; deletion is the explicit [`CODESTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#codestorage_sweep_delete_enabled) opt-in |
| **Quarantine copies** (`sweep/quarantine/<nonce>/…` keys) | Delete-mode sweeps copy every blob aside before deleting it | The sweep itself purges backups older than the purge window — `max(age threshold, 1h)` — while a destructive mode is on |

Two more growth sources sit outside this lifecycle: **LFS objects** (stored under their
own keys, never swept or repacked) and, on versioned S3 buckets, **old object versions**
(the reason destructive sweeping [refuses versioned buckets](#s3-bucket-versioning)).

Also note what does *not* reclaim space: **deleting branches** — repack preserves the
whole registered object set, so unreachable objects are carried forward by design
([why](../explanation/storage-engine.md#why-repack-packs-the-union-not-reachable-objects)) —
and **deleting repositories**: `DELETE /repos` soft-deletes the metadata record (freeing
the name), while the repository's pack registry rows and blobs stay put and the sweeper
keeps counting them live. Reclaiming a deleted repository's storage is currently a
manual operation ([#52](https://github.com/coder/code-storage/issues/52)).

## Inspect what a repository stores

List a repository's blobs under `packs/` straight from the blob store:

```sh
codestorage list-packs --repo repo_01k5abc...   # takes the server-generated RepoID
```

`--repo` takes the **RepoID** (`repo_...`, returned by repo creation and
`GET /api/v1/repo`), *not* the repository name. Namespace pushes store under sibling
prefixes — list them by suffixing the key (`--repo repo_01k5abc...+ephemeral`); a
quota denial's `stored` bytes cover the base **and** all namespace prefixes. The command
reads
`CODESTORAGE_S3_URL`/`CODESTORAGE_PG_URL` (or `--data-dir` in local mode) from the
environment — see the [CLI reference](../reference/cli.md#codestorage-list-packs).

Fleet-wide, the periodic [sweep report](#reading-a-sweep-report) counts live vs. orphan
blobs per run — but totals bytes for orphan candidates only. `list-packs` reports
**physical** bytes under a repository's prefixes — live, retained, and orphan blobs
alike, the same measure the quota's listing floor uses — without consulting the
registry; no surface reports registry-live bytes on their own, so use blob-store metrics
for fleet-wide capacity tracking.

## Background repack

Small pushes accumulate one pack each. When a repository's registered-pack count reaches
[`CODESTORAGE_REPACK_THRESHOLD`](../reference/configuration.md#codestorage_repack_threshold)
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

Every repository has a pack-bytes quota
([`CODESTORAGE_PACK_QUOTA_BYTES`](../reference/configuration.md#codestorage_pack_quota_bytes),
default 10 GiB, `0` disables). A repository and its `+ephemeral` / `+import` namespaces
share one quota, accounted against the base repository.

What a denial looks like — over git:

```
remote: codestorage hook: pre-receive: repository pack quota exceeded: push adds <n> bytes
to <n> bytes stored (+<n> bytes reserved by in-flight pushes), quota is <n> bytes
```

Over REST (commit-pack, diff-commit, restore-commit, merge, notes writes): a `403` with
`repository pack quota exceeded: write adds ... quota is <n> bytes`. A fork whose source
closure alone exceeds the quota is denied up front with
`repository pack quota exceeded: fork closure is <n> bytes, quota is <n> bytes`.

Reading the numbers: admission is `stored + reserved + incoming ≤ quota`, where

- **stored** counts every registered pack **plus retained (superseded) bytes** — after a
  repack fold, headroom stays reduced until the sweeper reaps the superseded originals
  and the report shows `retained_bytes_reclaimed`. It is also floored by the *physical*
  bytes under the repository's pack prefixes, which include aged **orphan** blobs — and
  since general-orphan deletion is off by default, a repository with abandoned uploads
  can keep hitting denials that only
  [enabling delete mode](#before-enabling-delete-mode-fleet-prerequisite) resolves;
- **reserved** counts other writers' in-flight admissions. A crashed writer's
  reservation keeps gating only until its
  [`CODESTORAGE_PACK_RESERVATION_TTL`](../reference/configuration.md#codestorage_pack_reservation_ttl)
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

Every [`CODESTORAGE_SWEEP_INTERVAL`](../reference/configuration.md#codestorage_sweep_interval)
(default 1h), a sweep job: heals the quarantine, prunes expired quota reservations, then
streams the bucket's `packs/` listing and classifies every blob against the pack registry.
Blobs with no registry row that are older than
[`CODESTORAGE_SWEEP_AGE_THRESHOLD`](../reference/configuration.md#codestorage_sweep_age_threshold)
(default 24h) are orphan candidates.

Three independent knobs control what a sweep may *do*:

| Mode | Default | Acts on |
|---|---|---|
| List-only (no flag) | **active** | Reports general orphans, deletes nothing |
| [`CODESTORAGE_SWEEP_DELETE_SUPERSEDED`](../reference/configuration.md#codestorage_sweep_delete_superseded) | `true` | Only blobs a repack fold provably superseded (durable `pack_retained` records); reclaims their quota bytes |
| [`CODESTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#codestorage_sweep_delete_enabled) | `false` | General orphans (crashed pushes, lost races) — the risky class, hence opt-in |

### Reading a sweep report

Each run ends with one summary line:

```
orphan pack sweep finished live=<n> fresh=<n> orphans=<n> orphan_bytes=<n> deleted=<n>
bytes_reclaimed=<n> superseded_deleted=<n> retained_bytes_reclaimed=<n>
reservations_expired=<n> delete_enabled=<bool> delete_superseded=<bool>
threshold=<age> duration=<d>
```

| Field | Meaning |
|---|---|
| `live` | Blobs with a registry row — real data, not candidates |
| `fresh` | Unregistered but younger than the age threshold — likely in-flight writers; never touched |
| `orphans` / `orphan_bytes` | Aged, unregistered candidates this run — **both** classes: repack-superseded blobs count here too until reaped |
| `deleted` | Candidates reaped this run (superseded reaps included). Reaping moves bytes to quarantine, so it frees nothing by itself |
| `bytes_reclaimed` | **Net** bytes returned by purging aged quarantine backups whose blob stayed unregistered. A backup whose original was restored purges without being counted (net zero — the original is live again). Lags `deleted` by the quarantine purge window, `max(age threshold, 1h)` |
| `superseded_deleted` / `retained_bytes_reclaimed` | The provably repack-superseded subset of `deleted`, and the **quota headroom** their reaps returned |
| `reservations_expired` | Crashed-writer quota reservations pruned this run |
| `delete_enabled` / `delete_superseded` | The **configured** modes — a [guard downgrade](#s3-bucket-versioning) to list-only is *not* reflected here; it shows only as the separate `delete guard failed; this run is list-only` error line |

Every candidate is also logged individually as it is classified
(`orphan pack candidate key=… size_bytes=… age=… delete_enabled=… delete_superseded=…`),
so in list-only mode you can audit exactly what deletion *would* reap.
A healthy steady state is `orphans=0` or a small, stable count;
monotonically growing `orphan_bytes` means crashed writers or fold races are outpacing
your (disabled) deletion and deserves investigation before you consider enabling it.

### Deletion is quarantine-first, and heals itself

When a sweep does delete, it never hard-deletes directly: every blob is first copied to a
`sweep/quarantine/<nonce>/…` key, the registry is re-checked *after* the delete (a registration that
landed mid-delete is restored on the spot), and **every** sweep — list-only included —
starts by restoring any registered-but-missing pack from quarantine. Rolling back to
list-only after a scare therefore still repairs anything a delete-enabled crash left
behind; look for `orphan pack sweep: healed registered pack from quarantine`. Quarantine
backups are purged only while a destructive mode is on, and only once older than
`max(age threshold, 1h)` — the floor guarantees a delete handshake is provably dead
before its backup goes away, so tuning the age threshold low does not shorten it.

### S3 bucket versioning

On a bucket with versioning enabled (or suspended), deletes only write delete markers —
every version stays stored and billed, so "reclaimed" bytes would be a lie:

- `CODESTORAGE_SWEEP_DELETE_ENABLED=true` **fails startup** against such a bucket (or
  when versioning state cannot be verified — grant `s3:GetBucketVersioning` or stay
  list-only).
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
> nodes, set `CODESTORAGE_SWEEP_DELETE_SUPERSEDED=false` as well — the safety argument is
> the same for both destructive classes.

A conservative enablement sequence:

1. Audit first: leave `CODESTORAGE_SWEEP_DELETE_ENABLED=false` (the default) for at
   least one age-threshold window; watch [sweep reports](#reading-a-sweep-report) and
   audit the candidate lines. Note the out-of-the-box default is list-only for *general*
   orphans only — superseded reaping is already on. If the audit window must be fully
   non-destructive, set `CODESTORAGE_SWEEP_DELETE_SUPERSEDED=false` for its duration and
   restore it afterward.
2. Confirm the whole fleet — including any node that executes git hooks — is on a current
   build (see the prerequisite above).
3. Leave `CODESTORAGE_SWEEP_DELETE_SUPERSEDED=true` (the default) so repack-superseded
   bytes are reclaimed and quota headroom returns.
4. Opt into `CODESTORAGE_SWEEP_DELETE_ENABLED=true` only if orphan bytes materially
   accumulate — crashed pushes are rare, so for many deployments list-only is a fine
   permanent state.
5. After the first delete-enabled sweeps, check `deleted` matches the candidates you
   audited. `bytes_reclaimed` stays `0` until the quarantine backups age past the purge
   window (`max(age threshold, 1h)`) — deletion and physical reclaim are different
   cohorts, so an initial `0` is expected, not a failure signal.
