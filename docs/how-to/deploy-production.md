# Deploy to production (multi-replica)

Upgrading an existing installation? Follow the [rename migration guide](migrate-to-openstorage.md)
first; the examples below use the new binary and environment names.

**Goal:** a hardened open.storage deployment — N stateless `openstorage serve` replicas
behind a TLS-terminating reverse proxy, backed by Postgres (ref CAS + metadata) and an
S3-compatible bucket (immutable packs).

This guide covers what is *specific* to running a fleet: statelessness and local disk,
concurrent boot, health checks, rolling upgrades, proxying, and key rotation. Every
variable it mentions is specified in the [configuration reference](../reference/configuration.md);
values are only repeated here where the deployment context matters. For a disposable
stack to try first, see the [Docker Compose how-to](deploy-docker-compose.md). For an AWS
evaluation deployment with infrastructure included, use the [Terraform EC2 reference](deploy-aws-terraform.md).

## Topology

```
clients ──► reverse proxy (TLS) ──► N × openstorage serve ──► Postgres   (refs + metadata)
                                                          └─► S3 bucket  (immutable packs)
```

- **Replicas are stateless and identical.** All durable state lives in Postgres and the
  bucket; a push through one replica is immediately visible through every other. Scale
  out by adding replicas; kill any replica at any time.
- **Run every replica with the same configuration** (same org, same key file contents,
  same quota/sweep/repack settings). Nothing enforces this, and skew produces confusing
  behavior — e.g. a quota only some replicas enforce, or a sweeper mode that depends on
  which replica picks the job.
- The container image (repo [`Dockerfile`](../../Dockerfile)) runs as a non-root user and
  needs the `git` binary at runtime, plus `gpg` and `ssh-keygen` if you protect refs with
  `verify-sig` policies — signature verification shells out to them and fails closed
  ([#50](https://github.com/coder/openstorage/issues/50)). The supplied image includes
  all three (CI asserts this); remember them if you build your own image.

### Runtime base and support

The Dockerfile pins the runtime base to **Alpine 3.24.1 by multi-platform image
index digest**. The Go builder version follows `mise.toml`, but its image tag is
not digest-pinned. `apk add --no-cache` resolves packages from the Alpine v3.24
repositories at build time, but does not upgrade every package inherited from the
base. The runtime base pin therefore does **not** make the full build reproducible
or guarantee current package fixes. An uncached rebuild re-resolves added packages;
base-image and package upgrades still need qualification.

[Alpine's release inventory](https://alpinelinux.org/releases.json), checked on
2026-09-11, lists v3.24 `main` support through **2028-06-01** and `community`
support through **2026-11-01**. These are upstream repository support dates, not
a whole-image support guarantee or a vulnerability-scan result. The previous
v3.21 `main` branch remains supported through **2026-11-01**; this refresh is
planned maintenance, not evidence that it was already unsupported.

## Backing services

**Postgres** — one database, reachable from every replica
([`OPENSTORAGE_PG_URL`](../reference/configuration.md#openstorage_pg_url)). There is no
separate migration step or job to run: each replica creates/upgrades the schema at boot,
serialized by advisory locks (see [concurrent boot](#concurrent-boot-schema-setup-and-migrations)).
The database is the authority for refs, metadata, quota accounting, and the durable
[River](https://riverqueue.com) job queue — and it stores **webhook HMAC secrets and
upstream-sync credentials**, plaintext by default
([#54](https://github.com/coder/openstorage/issues/54)). Set
[`OPENSTORAGE_SECRETS_KEY`](../reference/configuration.md#openstorage_secrets_key) (the
same value on every replica) to envelope-encrypt those columns with AES-256-GCM; the
reference documents the key format, rotation procedure, failure modes, and the required
**staged enablement on multi-replica fleets** (binary rollout first, then the key in
`decrypt-only` staging mode, then encrypt — older binaries misread envelopes as literal
secrets, and unkeyed replicas cannot read envelopes written mid-rollout). Either way,
scope database access tightly and encrypt the database and its backups at rest —
application-level encryption narrows what a database read or backup leak exposes, but the
serve process still holds the key.

**S3-compatible bucket** — dedicated to open.storage
([`OPENSTORAGE_S3_URL`](../reference/configuration.md#openstorage_s3_url)); credentials come
from the standard AWS chain (env vars, shared config, instance roles). AWS S3 is the
primary target; for self-hosting, [SeaweedFS](https://github.com/seaweedfs/seaweedfs) is
the reference store the test suite runs against. Other endpoints (including MinIO) are
untested/best-effort for ordinary blobs, not guaranteed compatible by product name or
basic CRUD success. Check the [capabilities and qualification matrix](../explanation/storage-engine.md#storage-compatibility-and-qualification)
for the used transfer, presign, range/copy and consistency requirements. Blob
acceptance with PG/SQLite authority does not qualify an endpoint for S3WAL authority.

- **Bucket permissions for the replica role.** With default settings the role needs all
  of:
  - object read/write/list (`s3:GetObject`, `s3:PutObject`, `s3:ListBucket`) — serving,
    LFS, sweeping's listings, and quarantine copies — plus `s3:AbortMultipartUpload`:
    large copies are multipart, and a failed copy's abort is best-effort, so without the
    permission (and ideally an `AbortIncompleteMultipartUpload` bucket lifecycle rule)
    incomplete multipart uploads linger stored and billed;
  - `s3:DeleteObject` — the sweeper's destructive modes, one of which (superseded
    reaping) is **on by default**. Without it the service starts fine, but every reap
    fails after writing its quarantine copy, and retained packs stay quota-counted
    indefinitely;
  - `s3:GetBucketVersioning` — also needed by default: superseded reaping re-checks
    versioning per run and silently downgrades every sweep to list-only when the check
    fails (only the explicit general-orphan opt-in checks at startup, where it fails the
    boot instead);
  - `s3:CreateBucket` — only if the bucket should be auto-provisioned at first boot. A
    pre-provisioned bucket needs no create permission: startup still issues
    `CreateBucket` first, but on `AccessDenied` it probes the bucket (`HeadBucket`,
    covered by `s3:ListBucket` above) and boots when it is reachable. A denied create
    with an unreachable bucket stays boot-fatal, naming both errors.

- **Pin S3 access to your account (AWS).** The server trusts whatever bucket the
  configured name resolves to — a pre-provisioned bucket is accepted owner-blind, and
  the create path has always tolerated `BucketAlreadyExists` (a name owned by someone
  else). AWS bucket names are global: a deleted bucket name can be re-registered by a
  third party, who can even grant your replica role access to capture your packs. Add an
  `aws:ResourceAccount` condition to the role's S3 statements so the startup probe *and*
  every data operation fail closed outside your account:

  ```json
  "Condition": { "StringEquals": { "aws:ResourceAccount": "<your-account-id>" } }
  ```

  Self-hosted stores like SeaweedFS have no shared global namespace, so this applies to
  AWS (and AWS-compatible multi-tenant) deployments.

  For defense-in-depth — or when you cannot edit the role's IAM statements — additionally
  set
  [`OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`](../reference/configuration.md#openstorage_s3_expected_bucket_owner)
  to the bucket-owning account ID: the server then sends the S3 bucket owner condition on
  every operation it issues (including the startup probe and presigned LFS URLs), failing
  closed with `403` if the bucket's owner ever changes. The IAM condition remains the
  primary control; the application-level check is independent of role policy.

- Packs are immutable and content-addressed: the server only ever writes new keys and —
  through the sweeper's destructive modes — deletes provably-unreferenced ones. The bytes
  under a key never change, but a key can be *re-PUT with identical content* (concurrent
  replicas publishing the same pack race a non-atomic exists-check, by design) — harmless
  on an unversioned bucket, while a versioned bucket accumulates billed noncurrent
  versions invisible to usage listings and the sweeper. If you keep versioning (and
  therefore report-only sweeps, below), add a noncurrent-version expiration lifecycle
  rule.
- **Decide on bucket versioning up front.** Destructive sweeping and versioning don't
  mix: deletes only write delete markers while every version stays stored and billed.
  The general-orphan delete opt-in
  ([`OPENSTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#openstorage_sweep_delete_enabled))
  therefore refuses to start against a versioned or versioning-suspended bucket, and the
  **default-on** superseded reaping
  ([`OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`](../reference/configuration.md#openstorage_sweep_delete_superseded))
  — which does delete repack-superseded blobs — downgrades itself to list-only per run
  via a runtime guard. On a versioned bucket, either disable both modes or accept
  report-only sweeps. See the [storage lifecycle guide](storage-lifecycle-and-sweeping.md).

## Replica configuration

The minimum per replica:

| Variable | Production value |
|---|---|
| [`OPENSTORAGE_ORG`](../reference/configuration.md#openstorage_org) | Your org name — the `iss` claim your JWTs carry |
| [`OPENSTORAGE_ORG_PUBKEYS`](../reference/configuration.md#openstorage_org_pubkeys) | Path to the org public-key PEM file — **identical contents on every replica** ([why](#jwt-keys-across-the-fleet)) |
| [`OPENSTORAGE_PG_URL`](../reference/configuration.md#openstorage_pg_url) | `postgres://…` (or `postgresql://…`) |
| [`OPENSTORAGE_S3_URL`](../reference/configuration.md#openstorage_s3_url) | `s3://<bucket>?region=…` |
| [`OPENSTORAGE_LISTEN`](../reference/configuration.md#openstorage_listen) | `0.0.0.0:<port>` — the default binds localhost on an ephemeral port |
| [`OPENSTORAGE_BASE_URL`](../reference/configuration.md#openstorage_base_url) | The **external** `https://` URL clients use — API responses embed it in `http_url` values; the default (derived from the listener) is wrong behind any proxy |

Do **not** set `OPENSTORAGE_DATA_DIR` in production — it exists to derive a single-node
SQLite + file-blob stack for [local mode](../../README.md#quickstart-local-mode-60-seconds).

Everything else — pack quotas, sweep/repack tuning — has production-safe defaults and is
covered by the [configuration reference](../reference/configuration.md). The exception is
**rate limiting, which is disabled by default**: an Internet-facing deployment should
either set
[`OPENSTORAGE_RATE_RPS` / `OPENSTORAGE_RATE_BURST`](../reference/configuration.md#openstorage_rate_rps--openstorage_rate_burst)
(plus [trusted proxies](#client-identity-behind-the-proxy-openstorage_trusted_proxies))
or enforce an equivalent per-client limit at the edge — the limiter wraps
authentication, so nothing else bounds unauthenticated traffic per client. Startup
validation is fail-fast:
a malformed value is a startup error naming the variable, never a silently-applied
default.

## Local disk: the cache directory

Replicas keep no *durable* state on local disk, but they use it in two ways. The main
one is a rebuildable cache of bare git repositories that read endpoints and git
smart-HTTP serve from, set with the `--cache-dir` flag (there is no `OPENSTORAGE_*` env
var for it) and defaulting to `<os temp dir>/codestorage-cache`. Separately, several
operations stage potentially large short-lived data in the **OS temp dir** itself
(fork-closure packs, upstream-sync clones, pack assembly) — so when you point
`--cache-dir` at a big volume, provision `$TMPDIR` accordingly too instead of leaving it
on a small container writable layer.

- **Give each replica its own local directory** on fast disk. Never share one directory
  between replicas (hydration locking assumes local `flock` semantics), and never mount it
  on network storage.
- A cold replica re-downloads the packs of each repository it serves on first access
  ("hydration"), keyed by a per-repository generation counter so warm replicas answer
  reads with a single database point-read. Losing the directory is safe — the next
  request rebuilds it.
- **The cache only grows — there is no eviction.** A replica retains every pack it ever
  hydrated, including packs long since superseded by repack and reaped from the bucket,
  and repositories it no longer serves. Working-set sizing alone is not enough: monitor
  the directory and reclaim it periodically — either by replacing the node or by clearing
  the directory **only while the replica is drained and stopped** (deleting it under a
  live process races in-flight requests and the hydration lock file). Losing the data
  costs only re-hydration.
- An emptied cache therefore shows as a burst of S3 reads and slower first requests, not
  errors.

## Concurrent boot: schema setup and migrations

Replicas race at boot by design — deploy tooling starts them all at once against the same
database. Schema setup is serialized with Postgres advisory locks, so concurrent boot is
safe; no external migration job or leader election is needed:

| Boot step | Lock |
|---|---|
| Metadata schema | transaction-scoped `0x4D455441` (`"META"`) |
| Ref-store schema | transaction-scoped `0x5350494B45` (`"SPIKE"`) |
| River job-queue migrations | session-scoped `0x5249564552` (`"RIVER"`) |
| `packs`/`refs` covering indexes (`CREATE INDEX CONCURRENTLY`) | session-scoped `0x5350494458` (`"SPIDX"`), waiters poll once per second |

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
for container liveness (restart-on-hang), but do **not** let it alone drive
load-balancer target health: a `200` from a broken replica would keep it in rotation.
There is no built-in dependency-aware readiness endpoint, so route on an external check
that exercises a real dependency — e.g. a synthetic authenticated read against each
target, or per-target `5xx`-rate ejection at the load balancer — and alert on API error
rates for serving health.

## Reverse proxy and TLS

The server has no TLS support — terminate TLS at a reverse proxy or load balancer and
forward plain HTTP to the replicas. **Restrict ingress to the replica port** (firewall /
security group / network policy allowlisting only the proxy and health-check sources):
the listener binds `0.0.0.0`, and a client that can reach it directly talks plain HTTP
and bypasses every proxy-enforced limit. Then:

- Set [`OPENSTORAGE_BASE_URL`](../reference/configuration.md#openstorage_base_url) to the
  external `https://` URL so clone URLs and other `http_url` values in API responses are
  routable. It is only embedded in responses — internal traffic (the upstream-sync
  worker's loopback pushes) targets each replica's own listener directly, so the
  replicas do not need to reach the external URL themselves
  ([#45](https://github.com/coder/openstorage/issues/45)).
- Both the REST API and git smart-HTTP (clone/push, LFS *batch*) ride the same
  listener — proxy everything. Response bodies (clones, archives) can be very large;
  stream rather than buffer them. Push ingress is bounded natively: the server rejects
  any push whose decoded body exceeds
  [`OPENSTORAGE_MAX_PUSH_BYTES`](../reference/configuration.md#openstorage_max_push_bytes)
  (default 2 × the pack quota, 2 MiB minimum) with a `413`, feeding git at most the cap's worth of
  bytes ([#55](https://github.com/coder/openstorage/issues/55)) — so size temporary
  disk for up to one cap per concurrent push (git discards the truncated quarantine on
  rejection; a declared oversize is rejected without reading at all). Fetch-side
  negotiation bodies are bounded too: a gzip-encoded `git-upload-pack` request is capped
  at a fixed 4 MiB decoded — plus a small compressed-stream allowance, so
  decode-to-nothing gzip streams cannot hold a connection open — and must finish within
  30 seconds. Oversize requests are rejected with a `413` before git runs
  ([#70](https://github.com/coder/openstorage/issues/70)). A proxy
  request-body cap is therefore optional defense-in-depth — if you set one, size it at
  or above the server cap and remember the proxy sees wire bytes while the server counts
  decoded (post-gzip) bytes. Request duration and concurrency limits remain the proxy's
  job.
- **LFS downloads bypass the proxy on S3; uploads do not**: for downloads the
  batch API hands clients presigned bucket URLs whenever the backend can sign (the
  server-proxied transfer is only the fallback for backends that cannot, i.e. local file
  mode), so Git LFS clients need direct network reach to the configured S3
  endpoint — a VPC-internal bucket endpoint breaks LFS pulls for external clients even
  though the batch request itself succeeds. Uploads always go through the server's
  verified proxy transfer ([#51](https://github.com/coder/openstorage/issues/51)):
  a presigned PUT cannot bind content length or checksum, so the server receives, size-
  and sha256-verifies, and meters every upload against the repository
  [storage quota](../reference/configuration.md#openstorage_pack_quota_bytes) before
  publishing it. Size the proxy's request-body cap and timeouts with the largest LFS
  object you intend to accept in mind, and note upload bytes are staged under the
  server's `$TMPDIR` before publication. Per-replica staging is bounded by
  [`OPENSTORAGE_LFS_STAGING_BYTES`](../reference/configuration.md#openstorage_lfs_staging_bytes)
  (default 32 GiB; over-budget uploads answer `429`/`413`, and each object allows one
  in-flight upload per replica). Any single repository's staged bytes are held to at
  most a third of the budget — the budget is a working set sized at roughly `3 ×` the
  largest object you accept, and the default keeps one default-quota-sized (10 GiB)
  object stageable. Give `$TMPDIR` at least the full configured budget (plus git push
  quarantine headroom) or lower the budget to the disk you actually provision, and
  keep the proxy body cap as defense in depth.
  **Upgrading from v0.1.0:** older replicas hand clients presigned upload URLs that
  write to the bucket directly, bypassing verification, and such URLs stay valid up to
  15 minutes after the last old replica exits. Complete the roll promptly; LFS objects
  a client uploaded around the roll window are re-verified on their next write-scoped
  touch only while unregistered, so treat uploads from that window with the same trust
  you gave v0.1.0's presigned path ([#78](https://github.com/coder/openstorage/issues/78)
  tracks closing this window with object-version fencing).

### Client identity behind the proxy (`OPENSTORAGE_TRUSTED_PROXIES`)

Rate limiting ([`OPENSTORAGE_RATE_RPS` / `OPENSTORAGE_RATE_BURST`](../reference/configuration.md#openstorage_rate_rps--openstorage_rate_burst))
keys token buckets by client IP. Buckets are **per replica and per surface** (REST and
git each keep their own in-process state): a client spread across N replicas can consume
roughly N× the configured REST budget plus N separate git budgets. Size the values as
per-replica limits, and enforce at the edge when you need one global per-client ceiling. Behind a proxy the TCP peer is the proxy itself, so all
clients would share one bucket — unless you tell the server which peers are proxies via
[`OPENSTORAGE_TRUSTED_PROXIES`](../reference/configuration.md#openstorage_trusted_proxies),
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

### Webhook egress

Webhook deliveries use a native SSRF guard. By default, every attempt resolves the
registered hostname at dial time and fails closed unless **every** answer is a public,
global-unicast address outside the IANA special-purpose registries. Loopback, RFC 1918,
RFC 6598 shared, link-local, IPv6 ULA, translation/protocol anycast, documentation,
benchmarking, reserved, unspecified, multicast, broadcast, and cloud-metadata targets are
denied; IPv4-mapped IPv6 addresses are normalized before classification. The client connects
to the vetted IP literal, opens a fresh connection for every attempt, ignores environment HTTP proxies, and never
follows redirects.
A denial is an ordinary delivery failure, so the configured queue backend applies its normal
retry and dead-letter/discard behavior instead of crashing the worker.

Registration checks literal IPs and the DNS answers visible at registration time for fast
feedback, but **registration acceptance does not imply delivery reachability**. DNS can be
unavailable or change later; the dial-time check is authoritative, and resolution failures,
empty answers, or mixed allowed/denied answer sets fail closed.

For deployments that intentionally deliver to internal receivers, set
`OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE=true`. This admits loopback, RFC 1918, RFC 6598
shared, link-local, and ULA targets and emits a startup warning. Cloud metadata
(`169.254.169.254` and `fd00:ec2::254`), other IANA special-purpose ranges,
malformed/non-routable targets, and redirects remain blocked. Keep network
egress controls as defense in depth, especially where other workloads share the replica
identity or network policy.

## Rolling upgrades and restarts

On `SIGTERM`/`SIGINT` the server shuts down gracefully: it stops accepting connections,
drains in-flight requests for a **bounded 10 seconds**, then runs its orderly teardown —
stopping background workers and, in [zero-infra mode](#zero-infrastructure-mode-experimental-one-bucket-one-node),
releasing the instance lease so a successor can start immediately instead of waiting out
the lease TTL. A second signal during the drain kills the process immediately. Requests
still running when the drain window closes are dropped (in-flight pushes included — safe
for data, see below, but visible to the client as a failed push), so zero-downtime rolls
still lean on the proxy plus statelessness:

1. Deregister the replica from the load balancer / stop routing to it.
2. Wait for in-flight requests to finish (git pushes can be long; give them a generous
   drain window).
3. Stop it, start the new version, wait for `/healthz`.
4. Re-register, move to the next replica.

Interrupted work is safe by construction: pack data is published to the bucket *before*
any ref moves, so a killed push never leaves a ref pointing at missing objects. What it
can leave behind is storage, not corruption: an unregistered blob (reclaimable by the
[sweeper](storage-lifecycle-and-sweeping.md)), or — when the kill lands between pack
registration and the ref update — a **registered** pack whose objects stay quota-counted
and are carried forward by repack
([why](../explanation/storage-engine.md#why-repack-packs-the-union-not-reachable-objects));
a client retry of the same push deduplicates against it and makes it reachable.
Schema upgrades ride the same boot-time locking as [above](#concurrent-boot-schema-setup-and-migrations),
so a mixed-version fleet mid-roll is expected and fine, with two deliberate exceptions:

> **Do not enable delete-mode sweeping until the whole fleet runs a build that has it.**
> See the [storage lifecycle guide](storage-lifecycle-and-sweeping.md#before-enabling-delete-mode-fleet-prerequisite)
> for the exact prerequisite and why list-only mode is safe on any fleet.

> **Upgrading from v0.2.0 or older with Git LFS in use: set
> [`OPENSTORAGE_LFS_V020_WRITE_COMPAT`](../reference/configuration.md#openstorage_lfs_v020_write_compat)
> on the upgraded replicas for the duration of the roll, then drop it.** Registered
> LFS objects now live at a canonical key old replicas cannot derive (ADR-41); the
> compat flag dual-publishes to the legacy key so old replicas keep serving LFS
> reads until the fleet is fully rolled. **Disable
> [`OPENSTORAGE_SWEEP_DELETE_LFS`](../reference/configuration.md#openstorage_sweep_delete_lfs)
> on EVERY replica — use `CODESTORAGE_SWEEP_DELETE_LFS=false` on pre-0.15 binaries,
> including the not-yet-upgraded v0.2.0 ones — for the duration of the roll**:
> the upgrade migrates registry rows to the canonical spelling in
> place, which makes a touched object's legacy blob look rowless to an old
> binary's sweeper, and old binaries have no legacy gate — a reap-enabled v0.2.0
> sweeper deletes it immediately and breaks old-replica reads mid-roll (upgraded
> replicas refuse to start with both flags set, but only the old nodes' sweepers
> are the hazard, so stage the flags before rolling). After the roll, re-enable
> LFS sweeping if desired; legacy-spelled keys (adoption sources, compat copies)
> are reclaimed only by the separate post-roll
> [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](../reference/configuration.md#openstorage_sweep_delete_lfs_legacy)
> opt-in, whose transition gate dominates every sweep flag on upgraded binaries.
> Enable it only once the last old replica has been gone ≥ 15 minutes.

Upstream-sync claims are leased: a replica that dies mid-sync leaves a claim that the
next run steals automatically once the lease expires, so a crash can delay but never
permanently stop syncing. Claims written by pre-lease replicas (rolling upgrades from
older builds, or wedges left by crashes before the upgrade) carry no lease and none of
the holder guards that make stealing safe; new replicas adopt them under a grace period
and — mirroring the sweep-delete fleet prerequisite above — will not steal them until
[`OPENSTORAGE_SYNC_STEAL_LEGACY`](../reference/configuration.md#openstorage_sync_steal_legacy)
asserts the fleet is drained of pre-lease builds. A live old-version run therefore always
finishes undisturbed; pre-upgrade wedges heal automatically once the opt-in is set.

## JWT keys across the fleet

Tokens are verified against the org's public keys in the database, and each replica
**rewrites** that key set at boot from its `OPENSTORAGE_ORG_PUBKEYS` file (last boot
wins). Hence: identical file contents everywhere, distributed via your secret store.

RSA org keys must be at least **2048 bits**. When upgrading from a version that
accepted smaller RSA keys, complete the rotation below **on the old fleet first**:
reissue client tokens/Git credentials and remove every weak key before upgrading.
A mixed strong+weak key set fails closed at startup and verification. See the
[public-key policy](../reference/authentication.md#organization-public-key-strength).

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
- The upstream-sync worker signs its loopback pushes with a per-process key that never
  leaves that process's memory, and those pushes target the replica's own listener —
  never the external base URL — so replicas cannot interfere with each other's
  `pull-upstream` runs and the database's org key set stays exactly the configured file
  keys. (Historically the loopback key was persisted last-writer-wins, breaking sync on
  every replica but the most recently booted one —
  [#45](https://github.com/coder/openstorage/issues/45).)

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
| `openstorage hook: pre-receive: repository pack quota exceeded` (client-visible) / `quota: receive-pack … at/over quota` (server log) | Tenants hitting the [pack quota](../reference/configuration.md#openstorage_pack_quota_bytes) |
| Postgres availability + advisory-lock waits at boot | The database is the single authority; boot hangs show in `pg_locks` |

## Zero-infrastructure mode (experimental): one bucket, one node

Everything above assumes the production topology: Postgres plus a bucket. The
**experimental** `s3wal://` mode
([ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md),
issue [#133](https://github.com/coder/openstorage/issues/133)) removes Postgres *and*
local disk from the durability story: refs and metadata live in conditional-CAS objects
under the bucket's reserved `_wal/` prefix, next to the ordinary `packs/` and `lfs/`
payloads. One S3-compatible bucket is the sole durable authority — kill the process,
lose the machine, and a fresh node pointed at the bucket reconstructs full service (the
cache dir stays a rebuildable cache, exactly like every other mode). Engine-level
rationale: [the storage engine](../explanation/storage-engine.md#the-zero-infrastructure-backend-s3-as-sole-authority-s3wal).

```sh
OPENSTORAGE_PG_URL='s3wal://my-bucket?endpoint=https://s3.example.net&region=us-east-1' \
OPENSTORAGE_S3_URL='s3://my-bucket?endpoint=https://s3.example.net&s3ForcePathStyle=true&region=us-east-1' \
OPENSTORAGE_ORG=org1 OPENSTORAGE_ORG_PUBKEYS=/etc/openstorage/org.pub.pem \
openstorage serve --listen 0.0.0.0:8080 --cache-dir /var/cache/openstorage
```

The same bucket for both URLs is the expected layout; a `file://` blob URL is refused at
startup ([pairing rule](../reference/configuration.md#openstorage_pg_url)). Create the
bucket before the first boot: the startup conditional-write probe runs against it before
the blob layer's create-if-missing bootstrap, so a missing bucket is boot-fatal. Never
write to the `_wal/` prefix yourself, and never point two serve processes at one
bucket — the
[instance lease](../reference/configuration.md#openstorage_s3wal_lease_ttl) exists to
detect exactly that mistake (startup refusal / fail-stop), best-effort.

**Store requirements.** The mode depends on faithful conditional writes
(`If-Match` / `If-None-Match: *`) **and on strong read-after-write and
list-after-write consistency**: after a successful PUT, every subsequent GET or
LIST from any client must observe it. Conditional writes are verified by a
boot-time probe that refuses stores failing it; consistency cannot be probed and
is a deployment prerequisite you must satisfy — the engine trusts every
unconditional read of its `_wal/` objects, so a store (or a caching proxy in
front of one) that can serve a superseded object version silently produces wrong
read-only verdicts, such as a reservation sweep or a hydration generation check that
misses a competitor's write. AWS S3 has provided this guarantee for all
regions [since December 2020](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/)
([consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel));
verify it for any other store, and never front the bucket with a cache.
Reliable store HTTP `Date` witnesses are also required for expiry decisions; startup
refuses a store that supplies no usable witness during the probe. Passing that probe
is not a support certificate. The [storage qualification matrix](../explanation/storage-engine.md#storage-compatibility-and-qualification)
separates tested releases/configurations from capability requirements and support
status. **All S3WAL targets remain experimental.** AWS S3 and SeaweedFS are conditional
future promotion targets under [#192 G1](https://github.com/coder/openstorage/issues/192),
not a declaration that the remaining real-S3 or operations gates passed. MinIO and
other endpoints are **not qualified for S3WAL authority**; ordinary blob compatibility
is a separate question. Do not enable bucket versioning or object lock on the bucket.

**Credentials and IAM.** Credentials come from the standard AWS chain (env vars, shared
config, instance roles). The node's permission set on the one bucket matches the
[general bucket permissions](#backing-services): object reads and writes
(`s3:GetObject`, `s3:PutObject` — the conditional `If-Match`/`If-None-Match` writes are
ordinary PutObject calls — and `s3:DeleteObject`; the quarantine handshake's
server-side copies ride the same two), `s3:AbortMultipartUpload` (cleanup of a failed
multipart quarantine copy), prefix listing (`s3:ListBucket`, which also covers the
bucket reachability probes), and `s3:GetBucketVersioning` (the delete-mode and purge
guards verify versioning is off).
`s3:CreateBucket` is not required for a pre-provisioned bucket — a denied create falls
back to a reachability probe. Set
[`OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`](../reference/configuration.md#openstorage_s3_expected_bucket_owner):
both S3 clients — the blob data plane *and* the `_wal/` conditional client — stamp the
owner condition on every request, which matters doubly in this mode because the bucket
carries the org's public keys and the ref authority, not just packs.

**Honest scaling boundaries** — this mode trades throughput for zero infrastructure, and
it is a deliberate single-node design:

| Boundary | Consequence |
|---|---|
| One node, ever | No replicas, no failover process — recovery is "start a new node against the bucket". The instance lease detects accidental dual-serve; it does not make it safe. |
| One CAS point per repository group | Concurrent pushes to the *same* repository serialize on one object; hot-repo write throughput is a handful of ref flips per second, and contention shows up as CAS retries. Different repositories don't contend. |
| Every ref/metadata operation is an S3 round trip | Latency floors are S3 latency, not local-disk or Postgres latency. Same-region compute is assumed. |
| Manifest size | A repository group's whole registry lives in one JSON object: growth-guarded mutations warn above 4 MiB and are refused above 16 MiB; see the [size-guard scope](#s3wal-operational-signals) below. |
| Background jobs are in-process | Like local mode: tickers, no durable River queue; queued local work dies with the process (durable request markers are re-enqueued at boot). |

If any of these bind, use local mode (SQLite on a persistent volume) or the Postgres
topology above. Small single-tenant deployments — the target — sit far below them.

**Measured boundaries** — from the recorded benchmark campaigns
([full method and tables](../plans/s3wal-bench-results.md)): loopback SeaweedFS
plus a real AWS S3 campaign (eu-central-1, near-same-region compute; the
qualification suite passed all surfaces against real S3):

- An uncontended small `git push` end-to-end costs ~26 ms (+23%) over local mode on
  loopback (138 vs 112 ms p50). On **real S3** the same push measured ~1.1 s p50 vs
  ~0.4 s for a Postgres+S3 node against the same bucket region — the zero-infra
  tradeoff is ~2.8× per push at real RTT, dominated by sequential S3 round trips.
  The real-S3 figures are **preliminary single-day observations** (the results
  document's method wants ≥ 3 separate-day runs for headline numbers, still
  pending) and were measured **before** the per-push round-trip trims that
  landed later the same day; treat them as a directional upper bound, not
  capacity guidance.
- One hot repository group sustains ~27 successful manifest CAS advances/s on real
  S3 regardless of writer count (the one-CAS-point-per-group ceiling; adding
  writers adds lost races, not throughput). Under the harness's sustained synthetic
  hammering the **CAS-success latency** tail reaches multiple seconds (p99 6.0 s at
  N=8, 16.9 s at N=32 — microbench manifest advances, not full pushes); the
  loopback e2e push bench measured real-push p99 of 5.8 s at N=32 vs local mode's
  2.8 s. Distinct repositories do not contend.
- The campaigns observed client-visible hook rejections on contention exhaustion
  (`s3wal cas ref repo_…: 16 attempts: s3wal: conditional-write contention retries
  exhausted`). Interpret this bounded-attempt failure using the
  [operational signals](#s3wal-operational-signals), not as a capacity diagnosis.
- Zero lost updates observed: every acknowledged push was durable across ~15,300 real
  pushes in the sustained multi-writer campaigns, plus ~790,000 lost-then-retried CAS
  races in the microbench cells.

**Crash recovery: repository state restarts from the bucket.** Node replacement and
crash recovery are the same operation: start a fresh node — empty disk, empty cache
dir — against the same two URLs. Refs, metadata, packs, and LFS objects reconstruct
from the bucket, and the cache re-hydrates on demand; this is a designed-for,
e2e-tested path (the `s3wal` [test lane](../reference/testing.md) drills exactly
this). The node's *configuration* is not in the bucket: a replacement needs the same
environment — the two storage URLs, `OPENSTORAGE_ORG`, the org public-key PEM file on
its own disk, and the same `OPENSTORAGE_SECRETS_KEY` if one encrypts the metadata
store's secret columns — provisioned from your config/secret store like any other
deployment.

**Bucket loss is a separate recovery problem.** Node replacement from the same
bucket is not a backup. Use the [stopped-node backup/restore runbook](backup-restore-s3wal.md)
for an opaque whole-bucket snapshot, independent inventory verification and
fresh-cache recovery into an empty bucket. A live `aws s3 sync` is not a
consistent snapshot. The procedure has a local fixture drill; it is not a
real-S3 disaster-recovery qualification or a promotion/support claim.

**Format changes need a separate upgrade procedure.** Read the
[S3WAL format stability policy](../explanation/storage-engine.md#s3wal-format-stability-policy)
before changing binaries. The stopped-node backup is a precaution, not a
cross-version rollback guarantee.

### S3WAL operational signals

This is an **experimental, single-node** log/error runbook, not a metrics endpoint
or an incident/real-S3 qualification. Use it alongside the [general monitoring
guidance](#what-to-monitor). Returned store errors are not themselves server log
entries: the caller determines their presentation. Terminal `serve` and `hook`
errors print to stderr with `openstorage serve:` / `openstorage hook:` prefixes
and exit 1; hook errors can reach Git clients. `WARN` below means `slog.Warn`.
Ellipses abbreviate variable text, not literal messages.

| Signal/error | Emission conditions and fields | What it establishes | What it does not establish | Safe response |
|---|---|---|---|---|
| Returned `s3wal …: 16 attempts: s3wal: conditional-write contention retries exhausted` | The [manifest loop](../../internal/ref/s3walstore.go#L473-L611) has 16 fresh attempts (an optional cached-seed try is extra), naming operation and base. The [scan-state loop](../../internal/ref/s3walstore_scanstate.go#L184-L281) also has a 16-attempt budget, naming operation and `_wal/scan/` key; shared lapse-marker updates can contend across receive hooks. The [conditional client](../../internal/ref/s3wal/casobj.go#L342-L398) separately retries up to 16 `409 ConditionalRequestConflict` attempts, naming `create`/`advance` and key. | A bounded attempt budget failed for the observed operation/base or key. | Not a global capacity limit or proven root cause. Manifest/scan retries also include never-sent/recovery paths. These loops do not log ordinary retries; that says nothing about other SDK/transport logging. | Preserve the full error, operation and identifier; check store connectivity/errors. For a manifest base, inspect same-base writers and actual refs/results. For a scan-state key, inspect marker work across concurrent receive hooks, not just that repository's refs. Reduce competing work before a controlled retry; do not retry until green. |
| `WARN s3wal manifest approaching size cap` | [Size guard](../../internal/ref/s3walstore.go#L614-L633): growth predicate true; candidate encoded bytes **> 4 MiB and ≤ 16 MiB**. Fields: `base`, `bytes`, `warn`, `cap`. Suppressed after the first warning for that base in that store's map. | A candidate mutation exceeded the soft threshold. | Not proof the write committed, a current-size gauge, or one warning per process/fleet lifetime. No warning does not prove spare capacity. | Record base/bytes and growth-producing operations; plan supported application-level cleanup or a deployment change before the hard cap. Never edit the manifest. |
| Returned `s3wal …: manifest would grow to … bytes (cap 16777216): refusing new registrations` | Same guard, growth predicate true and candidate bytes **> 16 MiB**; text includes operation, base, size and cap. Hard refusal precedes the soft-warning branch. | This candidate was refused before its conditional PUT. | Not a blanket read-only mode or a promise that deletes, updates, reads or purge succeed; other guards and failures remain. | Stop adding registry entries to the affected group. Assess supported cleanup through ordinary APIs; preserve failures and escalate if cleanup is blocked. Do not raise/bypass the guard. |
| Stderr `s3wal instance lease _wal/instance-lease is held by another serve process …` | [Acquisition](../../internal/ref/s3wal/lease.go#L138-L227) found a fresh recorded lease, or a foreign holder won an acquisition race. Fields in text: holder prefix, rounded store-clock age, holder's TTL. | This startup refused that lease. | Not proof the holder is still alive; a stopped process can leave a lease. Not permission to force takeover. | Identify/fence writers and follow the [shutdown/restart procedure](#s3wal-shutdown-and-restart) below. |
| Stderr `s3wal instance lease … fail-stop` | [Renewal](../../internal/ref/s3wal/lease.go#L258-L327) re-read found a foreign holder, corruption or disappearance; or failures persisted until the last accepted renewal was at least TTL − cadence old. Cadence is TTL/4. `unrenewable since …` includes elapsed time, TTL and write/read errors. | The heartbeat returned a loss verdict; [`serve` exits 1](../../cmd/openstorage/main.go#L165-L180). | Not proof of whole-system integrity, safe dual servers, or that every transient renewal failure exits. Fail-stop does not run graceful cleanup. | Preserve stderr and supervisor history; fence traffic/restarts, identify every writer, inspect connectivity and store evidence before restarting one node. |
| Stderr `s3wal init: store supplied no usable HTTP Date witness during the probe …` or lease `… freshness cannot be judged on the store clock …` / `… is corrupt …` | [Startup probe](../../internal/ref/s3walstore.go#L199-L214) lacks a usable Date witness (missing or older than 30s); [lease acquisition](../../internal/ref/s3wal/lease.go#L154-L227) cannot decode the lease or judge its store-clock age. | Startup failed closed on missing/invalid evidence. | Not necessarily host-clock skew or a stale/dead holder. | Preserve errors; inspect store/proxy Date and Last-Modified behavior and connectivity. Keep writers fenced and escalate corruption. Do not follow diagnostic text suggesting object deletion. |
| `WARN s3wal clock skew guard tripped: demoting expiry verdict to report-only` | [Expiry guard](../../internal/ref/s3walstore_quota.go#L68-L124): positive TTL appears elapsed, but clock or store-Date check fails. Fields: `base`, `what`, `manifest_clk`, `local_now`, `wall_drift`, `store_date`, `store_date_age`, `ttl`. | **That expiry verdict** was refused. | Not a diagnosis of a wrong host clock, a global report-only switch, or proof that all other operations are safe/successful. | Compare host time/drift, manifest clock and store/proxy Date evidence. A missing/stale witness also trips the guard. Fix the cause before a controlled restart; do not disable time safeguards. |

**Size-guard scope.** The guard runs only when `mu.grows` is non-nil and returns
true; it does not measure the byte delta against the old manifest. Equality with
either threshold does not trigger that threshold's action. Mutations outside that
predicate bypass **only this guard**. The warning map is allocated by each
[`newS3WALStore`](../../internal/ref/s3walstore.go#L152-L193); `serve` opens a store,
and [hook invocations open their own](../../internal/githttp/hook.go#L1728-L1732).
Reopening a store permits another warning for the same base.

**Clock-guard scope.** The [clock checks](../../internal/ref/s3wal/manifest.go#L334-L427)
compare a manifest clock ahead of local time and wall-vs-monotonic drift against
`min(TTL/4, 15m)`. The store-Date arm additionally requires a fresh witness and
local/store agreement; bounds below the 2s Date-resolution tolerance disable that
arm. A quiet repository's old manifest clock alone is not skew. The conditional
client [records parsed HTTP Date headers](../../internal/ref/s3wal/casobj.go#L178-L213).
A prior wall-clock step can leave the [store's drift baseline](../../internal/ref/s3walstore.go#L636-L650)
tripped until restart; restart does not repair an incorrect clock or missing witness.

### S3WAL shutdown and restart

1. Preserve the full error/log context, binary version, affected base/operation,
   timestamps, holder/TTL evidence and supervisor history. Redact credentials and
   signed URLs. Identify every serving process, hook and maintenance writer;
   prevent replacement launches and fence access before intervening.
2. Stop the old node with `SIGTERM`/`SIGINT` and verify it and its writers exited.
   The [graceful shutdown](#rolling-upgrades-and-restarts) drains HTTP, then attempts
   lease release with a **5s best-effort bound**. Immediate restart is possible
   only if release succeeds; graceful shutdown can also leave the lease behind.
3. After a hard stop, heartbeat fail-stop or failed release, keep the old writers
   fenced. Allow at least the **holder's recorded TTL plus 2s** after confirmed
   stop (default TTL 2m). Acquisition uses store Date and Last-Modified with a
   whole-second resolution margin, not a comparison of host clocks. Start only
   one replacement; if it still refuses a held lease, preserve that evidence,
   remain fenced and wait for its reported expiry before a controlled retry.
   Unjudgeable/corrupt leases require investigation, not repeated starts.
4. Verify authenticated repository reads and expected refs before restoring
   traffic; `/healthz` alone is not recovery proof. The instance lease is a
   diagnostic/fail-stop mechanism, **not** independent proof of integrity or
   support for dual servers. Never manually delete the lease, edit `_wal/`, force
   takeover or disable safeguards to make startup pass.
