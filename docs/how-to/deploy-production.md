# Deploy to production (multi-replica)

**Goal:** a hardened codestorage deployment — N stateless `codestorage serve` replicas
behind a TLS-terminating reverse proxy, backed by Postgres (ref CAS + metadata) and an
S3-compatible bucket (immutable packs).

This guide covers what is *specific* to running a fleet: statelessness and local disk,
concurrent boot, health checks, rolling upgrades, proxying, and key rotation. Every
variable it mentions is specified in the [configuration reference](../reference/configuration.md);
values are only repeated here where the deployment context matters. For a disposable
stack to try first, see the [Docker Compose how-to](deploy-docker-compose.md).

## Topology

```
clients ──► reverse proxy (TLS) ──► N × codestorage serve ──► Postgres  (refs + metadata)
                                                          └─► S3/MinIO  (immutable packs)
```

- **Replicas are stateless and identical.** All durable state lives in Postgres and the
  bucket; a push through one replica is immediately visible through every other. Scale
  out by adding replicas; kill any replica at any time.
- **Run every replica with the same configuration** (same org, same key file contents,
  same quota/sweep/repack settings). Nothing enforces this, and skew produces confusing
  behavior — e.g. a quota only some replicas enforce, or a sweeper mode that depends on
  which replica picks the job.
- The container image (repo [`Dockerfile`](../../Dockerfile)) runs as a non-root user and
  needs the `git` binary at runtime — already included; remember it if you build your own
  image.

## Backing services

**Postgres** — one database, reachable from every replica
([`CODESTORAGE_PG_URL`](../reference/configuration.md#codestorage_pg_url)). There is no
separate migration step or job to run: each replica creates/upgrades the schema at boot,
serialized by advisory locks (see [concurrent boot](#concurrent-boot-schema-setup-and-migrations)).
The database is the authority for refs, metadata, quota accounting, and the durable
[River](https://riverqueue.com) job queue.

**S3-compatible bucket** — dedicated to codestorage
([`CODESTORAGE_S3_URL`](../reference/configuration.md#codestorage_s3_url)); credentials come
from the standard AWS chain (env vars, shared config, instance roles).

- Packs are immutable and content-addressed: the server only ever writes new keys and —
  when sweeping is explicitly enabled — deletes provably-unreferenced ones. Nothing is
  overwritten in place.
- **Decide on bucket versioning up front.** Delete-mode sweeping
  ([`CODESTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#codestorage_sweep_delete_enabled))
  refuses to start against a versioned or versioning-suspended bucket, because deletes
  would only write delete markers while every version stays stored and billed. List-only
  sweeping (the default) works with any bucket. See the
  [storage lifecycle guide](storage-lifecycle-and-sweeping.md).

## Replica configuration

The minimum per replica:

| Variable | Production value |
|---|---|
| [`CODESTORAGE_ORG`](../reference/configuration.md#codestorage_org) | Your org name — the `iss` claim your JWTs carry |
| [`CODESTORAGE_ORG_PUBKEYS`](../reference/configuration.md#codestorage_org_pubkeys) | Path to the org public-key PEM file — **identical contents on every replica** ([why](#jwt-keys-across-the-fleet)) |
| [`CODESTORAGE_PG_URL`](../reference/configuration.md#codestorage_pg_url) | `postgres://…` (or `postgresql://…`) |
| [`CODESTORAGE_S3_URL`](../reference/configuration.md#codestorage_s3_url) | `s3://<bucket>?region=…` |
| [`CODESTORAGE_LISTEN`](../reference/configuration.md#codestorage_listen) | `0.0.0.0:<port>` — the default binds localhost on an ephemeral port |
| [`CODESTORAGE_BASE_URL`](../reference/configuration.md#codestorage_base_url) | The **external** `https://` URL clients use — API responses embed it in `http_url` values; the default (derived from the listener) is wrong behind any proxy |

Do **not** set `CODESTORAGE_DATA_DIR` in production — it exists to derive a single-node
SQLite + file-blob stack for [local mode](../../README.md#quickstart-local-mode-60-seconds).

Everything else — rate limiting, trusted proxies, pack quotas, sweep/repack tuning — has
production-safe defaults and is covered by the
[configuration reference](../reference/configuration.md). Startup validation is fail-fast:
a malformed value is a startup error naming the variable, never a silently-applied
default.

## Local disk: the cache directory

Replicas keep exactly one thing on local disk: a rebuildable cache of bare git
repositories that read endpoints and git smart-HTTP serve from. It is set with the
`--cache-dir` flag (there is no `CODESTORAGE_*` env var for it) and defaults to
`<os temp dir>/codestorage-cache`.

- **Give each replica its own local directory** on fast disk. Never share one directory
  between replicas (hydration locking assumes local `flock` semantics), and never mount it
  on network storage.
- Size it for your working set: a cold replica re-downloads the packs of each repository
  it serves on first access ("hydration"), keyed by a per-repository generation counter
  so warm replicas answer reads with a single database point-read. Losing the directory
  is safe — the next request rebuilds it.
- An emptied cache after a node replacement therefore shows as a burst of S3 reads and
  slower first requests, not errors.

## Concurrent boot: schema setup and migrations

Replicas race at boot by design — deploy tooling starts them all at once against the same
database. Schema setup is serialized with Postgres advisory locks, so concurrent boot is
safe; no external migration job or leader election is needed:

| Boot step | Lock |
|---|---|
| Metadata schema | transaction-scoped `0x4D455441` (`"META"`) |
| Ref-store schema | transaction-scoped `0x5350494B45` (`"SPIKE"`) |
| River job-queue migrations | session-scoped `0x5249564552` (`"RIVER"`) |
| `packs` covering index (`CREATE INDEX CONCURRENTLY`) | session-scoped `0x5350494458` (`"SPIDX"`), waiters poll once per second |

What you observe: the first boot on a fresh (or upgraded) database does the DDL on one
replica while the others wait on the locks and proceed once released; steady-state boots
detect the schema is current and skip straight to serving. If a boot ever appears hung,
inspect `pg_locks` for these advisory lock IDs before suspecting the replica itself.

## Health checks

`GET /healthz` is unauthenticated, bypasses rate limiting, and returns
`200 {"status":"ok"}`.

It is a **liveness** probe only: it proves this process (not a stale one squatting the
port) is accepting HTTP. It does **not** ping Postgres or the bucket — a replica that has
lost its database still answers `/healthz` while failing real requests with `5xx`. Use it
for container liveness and load-balancer target registration, and alert on API error
rates for actual serving health.

## Reverse proxy and TLS

The server has no TLS support — terminate TLS at a reverse proxy or load balancer and
forward plain HTTP to the replicas. Then:

- Set [`CODESTORAGE_BASE_URL`](../reference/configuration.md#codestorage_base_url) to the
  external `https://` URL so clone URLs and other `http_url` values in API responses are
  routable.
- Both the REST API and git smart-HTTP (clone/push/LFS) ride the same listener — proxy
  everything, and allow large streaming request/response bodies (pushes and archive
  downloads are unbounded).

### Client identity behind the proxy (`CODESTORAGE_TRUSTED_PROXIES`)

Rate limiting ([`CODESTORAGE_RATE_RPS` / `CODESTORAGE_RATE_BURST`](../reference/configuration.md#codestorage_rate_rps--codestorage_rate_burst))
keys token buckets by client IP. Behind a proxy the TCP peer is the proxy itself, so all
clients would share one bucket — unless you tell the server which peers are proxies via
[`CODESTORAGE_TRUSTED_PROXIES`](../reference/configuration.md#codestorage_trusted_proxies),
a comma-separated CIDR allowlist. The resolution rules (matching the merged behavior
exactly):

- If the **TCP peer is in the allowlist**, both limiters (REST and git) use the
  **rightmost `X-Forwarded-For` hop that is not itself allowlisted** — the client address
  as observed by the nearest trusted proxy. Hops appended by your own proxies are skipped;
  hops a client fabricated are never reached, because the client's real address (appended
  by your proxy) sits to their right.
- Everything else **fails closed to the TCP peer address**: allowlist unset or empty,
  untrusted peer, empty/malformed hop at the boundary, an all-trusted chain, or an
  oversized `X-Forwarded-For` chain (> 4096 bytes).
- The allowlist is validated at startup **even when rate limiting is disabled** — a
  malformed entry is a boot failure, not a runtime surprise. IPv4-mapped IPv6 entries are
  normalized to IPv4; zone-scoped addresses and IPv4-mapped prefixes shorter than `/96`
  are rejected.
- **List only proxies that set or append `X-Forwarded-For` themselves.** Trusting a proxy
  that forwards client-supplied headers verbatim lets any client mint fresh rate-limit
  buckets at will.

## Rolling upgrades and restarts

There is **no in-process connection draining**: the server handles no signals and exits
immediately on `SIGTERM`, dropping in-flight requests (including in-flight pushes — safe
for data, see below, but visible to the client as a failed push). Zero-downtime rolls
therefore lean on the proxy plus statelessness:

1. Deregister the replica from the load balancer / stop routing to it.
2. Wait for in-flight requests to finish (git pushes can be long; give them a generous
   drain window).
3. Stop it, start the new version, wait for `/healthz`.
4. Re-register, move to the next replica.

Interrupted work is safe by construction: pack data is published to the bucket *before*
any ref moves, so a killed push leaves at worst an unreferenced pack for the
[sweeper](storage-lifecycle-and-sweeping.md) — never a ref pointing at missing objects.
Schema upgrades ride the same boot-time locking as [above](#concurrent-boot-schema-setup-and-migrations),
so a mixed-version fleet mid-roll is expected and fine, with one deliberate exception:

> **Do not enable delete-mode sweeping until the whole fleet runs a build that has it.**
> See the [storage lifecycle guide](storage-lifecycle-and-sweeping.md#before-enabling-delete-mode-fleet-prerequisite)
> for the exact prerequisite and why list-only mode is safe on any fleet.

## JWT keys across the fleet

Tokens are verified against the org's public keys in the database, and each replica
**rewrites** that key set at boot from its `CODESTORAGE_ORG_PUBKEYS` file (last boot
wins). Hence: identical file contents everywhere, distributed via your secret store.

Rotation without downtime:

1. Append the new public key block to the PEM file (all replicas).
2. Rolling-restart the fleet so a post-change boot registers the combined set.
3. Start minting tokens with the new private key.
4. After old tokens expire, remove the retired block from the file and rolling-restart
   again.

Two sharp edges:

- PEM parsing stops **silently** at the first undecodable block once at least one key
  parsed — a truncated block appended after a valid key is dropped, not rejected. After a
  rotation, verify a token signed with the *new* key is accepted rather than trusting
  startup success.
- Each boot also registers a per-process loopback key used by the upstream-sync worker.
  Because the boot-time rewrite is last-writer-wins, in a multi-replica fleet only the
  most recently booted replica's loopback key survives, which can fail `pull-upstream`
  sync runs on the other replicas — a known limitation tracked in
  [#45](https://github.com/coder/code-storage/issues/45). Core API and git traffic is
  unaffected.

## Background maintenance in a fleet

Orphan-pack sweeps, repack folds, webhook deliveries, and upstream syncs run as durable
River jobs in Postgres: any replica may pick up any job, jobs survive replica loss, and a
job triggered through one replica may execute (and log) on another. Operational details —
what the jobs do, how to read their reports, and the delete-mode prerequisites — live in
the [storage lifecycle guide](storage-lifecycle-and-sweeping.md).

## What to monitor

| Signal | Why |
|---|---|
| API/git `5xx` rates per replica | Real serving health — `/healthz` won't catch a lost database |
| `orphan pack sweep finished` log line | Per-sweep report: orphan counts/bytes, deletions, reclaimed bytes ([field reference](storage-lifecycle-and-sweeping.md#reading-a-sweep-report)) |
| `repack: folded packs` / `repack trigger: scheduled fold` log lines | Repack progress; a repo that triggers but never folds warrants a look |
| `pre-receive: repository pack quota exceeded` (client-visible) / `quota: receive-pack … at/over quota` (server log) | Tenants hitting the [pack quota](../reference/configuration.md#codestorage_pack_quota_bytes) |
| Postgres availability + advisory-lock waits at boot | The database is the single authority; boot hangs show in `pg_locks` |
