# Deploy to production (multi-replica)

Run a hardened open.storage fleet: N stateless `openstorage serve` replicas behind a
TLS-terminating reverse proxy, backed by Postgres (ref CAS + metadata) and an
S3-compatible bucket (immutable packs). This page covers fleet-specific concerns; the
[configuration reference](../reference/configuration.md) specifies every variable.

- Disposable stack to try first: [Docker Compose how-to](deploy-docker-compose.md).
- AWS evaluation with infrastructure included: [Terraform EC2 reference](deploy-aws-terraform.md).
- Upgrading from an older release? Follow the [migration guide](migrate-to-openstorage.md)
  first. The examples below use the current binary and variable names.

## Topology

```
clients ──► reverse proxy (TLS) ──► N × openstorage serve ──► Postgres   (refs + metadata)
                                                          └─► S3 bucket  (immutable packs)
```

- **Replicas are stateless and identical.** All durable state lives in Postgres and the
  bucket, so a push through one replica is immediately visible through every other. Add
  replicas to scale out; kill any replica at any time.
- **Configure every replica the same:** same org, same key file contents, same
  quota/sweep/repack settings. Nothing enforces this, and skew is confusing: for example,
  a quota only some replicas enforce, or a sweeper mode that depends on which replica
  picks the job.
- **Runtime tools.** The image ([`Dockerfile`](../../Dockerfile)) runs as a non-root user
  and needs `git`. `verify-sig` ref policies also need `gpg` and `ssh-keygen`: signature
  verification shells out to them and fails closed. The supplied image has all three (CI
  asserts this). Include them if you build your own image.

### Runtime base and support

The Dockerfile pins the runtime base to **Alpine 3.24.1 by multi-platform image index
digest**. This does **not** make the full build reproducible or guarantee current
package fixes:

- The Go builder version follows `mise.toml`, but its image tag is not digest-pinned.
- `apk add --no-cache` resolves packages from the Alpine v3.24 repositories at build
  time, so an uncached rebuild re-resolves them. It does not upgrade every package
  inherited from the base.
- Base-image and package upgrades still need qualification.

Alpine v3.24 upstream support ([release inventory](https://alpinelinux.org/releases.json),
checked 2026-09-11): `main` through **2028-06-01**, `community` through **2026-11-01**.
These are repository support dates, not a whole-image support guarantee or a
vulnerability-scan result.

## Backing services

### Postgres

- One database, reachable from every replica
  ([`OPENSTORAGE_PG_URL`](../reference/configuration.md#openstorage_pg_url)).
- No separate migration step or job: each replica creates or upgrades the schema at
  boot, serialized by advisory locks ([concurrent boot](#concurrent-boot-schema-setup-and-migrations)).
- The authority for refs, metadata, quota accounting, and the durable
  [River](https://riverqueue.com) job queue.

> [!IMPORTANT]
> Postgres stores **webhook HMAC secrets and upstream-sync credentials**, in plaintext by
> default. Set [`OPENSTORAGE_SECRETS_KEY`](../reference/configuration.md#openstorage_secrets_key)
> (the same value on every replica) to envelope-encrypt those columns with AES-256-GCM.
> The reference covers the key format, rotation procedure, and failure modes.

On a multi-replica fleet, enable the key in stages:

1. Roll out the envelope-aware binary to every node.
2. Set the key in `decrypt-only` staging mode.
3. Switch to encrypt.

Older binaries misread envelopes as literal secrets, and unkeyed replicas cannot read
envelopes written mid-rollout.

Either way, scope database access tightly and encrypt the database and its backups at
rest. The key narrows what a database read or backup leak exposes, but the serve
process still holds it.

### S3-compatible bucket

Use a bucket dedicated to open.storage
([`OPENSTORAGE_S3_URL`](../reference/configuration.md#openstorage_s3_url)). Credentials
come from the standard AWS chain (env vars, shared config, instance roles).

- **AWS S3** is the primary target.
- **[SeaweedFS](https://github.com/seaweedfs/seaweedfs)** is the reference self-hosted
  store; the test suite runs against it.
- **Other endpoints, including MinIO,** are untested/best-effort for ordinary blobs.
  Neither the product name nor basic CRUD success guarantees compatibility.

The [capabilities and qualification matrix](../explanation/storage-engine.md#storage-compatibility-and-qualification)
lists the transfer, presign, range/copy, and consistency features the server uses. Blob
acceptance with PG/SQLite authority does not qualify an endpoint for S3WAL authority.

#### Bucket permissions

With default settings, the replica role needs all of these:

| Permission | Needed for | Without it |
|---|---|---|
| `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` | Serving, LFS, sweep listings, quarantine copies | Required |
| `s3:AbortMultipartUpload` | Large copies are multipart; a failed copy's abort is best-effort | Incomplete multipart uploads linger, stored and billed. Ideally also add an `AbortIncompleteMultipartUpload` lifecycle rule. |
| `s3:DeleteObject` | The sweeper's destructive modes. One, superseded reaping, is **on by default**. | The service starts, but every reap fails after writing its quarantine copy, and retained packs stay quota-counted indefinitely. |
| `s3:GetBucketVersioning` | Superseded reaping re-checks versioning on every run | Every sweep **silently** downgrades to list-only. (The explicit general-orphan opt-in checks at startup instead and fails the boot.) |
| `s3:CreateBucket` | Only auto-provisioning the bucket at first boot | Fine for a pre-provisioned bucket: startup still tries `CreateBucket`, but on `AccessDenied` probes with `HeadBucket` (covered by `s3:ListBucket`) and boots if the bucket is reachable. A denied create plus an unreachable bucket is boot-fatal, naming both errors. |

#### Pin S3 access to your account (AWS)

> [!WARNING]
> The server trusts whatever bucket the configured name resolves to. It accepts a
> pre-provisioned bucket without an owner check, and the create path tolerates
> `BucketAlreadyExists` (a name someone else owns). AWS bucket names are global: a third
> party can re-register a deleted name and even grant your replica role access to
> capture your packs.

Add an `aws:ResourceAccount` condition to the role's S3 statements, so the startup probe
*and* every data operation fail closed outside your account:

```json
"Condition": { "StringEquals": { "aws:ResourceAccount": "<your-account-id>" } }
```

This applies to AWS and AWS-compatible multi-tenant stores. Self-hosted stores like
SeaweedFS have no shared global namespace.

For defense in depth, or if you cannot edit the role's IAM statements, also set
[`OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`](../reference/configuration.md#openstorage_s3_expected_bucket_owner)
to the bucket-owning account ID. Every S3 operation the server issues, including the
startup probe and presigned LFS URLs, then carries the bucket-owner condition and fails
closed with `403` if the owner ever changes. The IAM condition stays the primary control;
this check is independent of role policy.

#### Versioning and lifecycle rules

**Decide on bucket versioning up front.** On a versioned bucket, deletes only write
delete markers; every version stays stored and billed. So:

- The general-orphan delete opt-in
  ([`OPENSTORAGE_SWEEP_DELETE_ENABLED`](../reference/configuration.md#openstorage_sweep_delete_enabled))
  refuses to start on a versioned or versioning-suspended bucket.
- The **default-on** superseded reaping
  ([`OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`](../reference/configuration.md#openstorage_sweep_delete_superseded)),
  which deletes repack-superseded blobs, downgrades itself to list-only per run.
- Either disable both modes or accept report-only sweeps. See the
  [storage lifecycle guide](storage-lifecycle-and-sweeping.md).

**If you keep versioning, add a noncurrent-version expiration rule.** Packs are immutable
and content-addressed: the server only writes new keys, and deletes only provably
unreferenced ones (through destructive sweeps). But concurrent replicas publishing the
same pack can re-PUT a key with identical content (they race a non-atomic exists-check,
by design). That is harmless on an unversioned bucket. A versioned bucket accumulates
billed noncurrent versions that usage listings and the sweeper cannot see.

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

- Do **not** set `OPENSTORAGE_DATA_DIR` in production. It derives a single-node SQLite +
  file-blob stack for [local mode](../../README.md#quickstart-local-mode-60-seconds).
- Everything else (pack quotas, sweep/repack tuning) has production-safe defaults.
- Startup validation is fail-fast: a malformed value is a startup error naming the
  variable, never a silently applied default.

> [!WARNING]
> **Rate limiting is disabled by default.** The limiter wraps authentication, so nothing
> else bounds unauthenticated traffic per client. On an Internet-facing deployment, set
> [`OPENSTORAGE_RATE_RPS` / `OPENSTORAGE_RATE_BURST`](../reference/configuration.md#openstorage_rate_rps--openstorage_rate_burst)
> (plus [trusted proxies](#client-identity-behind-the-proxy-openstorage_trusted_proxies))
> or enforce an equivalent per-client limit at the edge.

## Local disk: the cache directory

Replicas keep no *durable* state on local disk, but use it for two things:

- **Git cache:** rebuildable bare Git repositories that read endpoints and Git
  smart-HTTP serve from. Set it with the `--cache-dir` flag (there is no `OPENSTORAGE_*`
  env var); the default is `<os temp dir>/codestorage-cache`.
- **Staging:** potentially large short-lived data (fork-closure packs, upstream-sync
  clones, pack assembly) goes to the **OS temp dir** itself. If you point `--cache-dir`
  at a big volume, provision `$TMPDIR` too; don't leave it on a small container writable
  layer.

Rules for the cache:

- **One local directory per replica, on fast disk.** Never share it between replicas
  (hydration locking assumes local `flock` semantics) and never mount it on network
  storage.
- **Losing it is safe.** A cold replica re-downloads the packs of each repository it
  serves on first access ("hydration"). Expect a burst of S3 reads and slower first
  requests, not errors. A per-repository generation counter lets warm replicas answer
  reads with a single database point-read.
- **It only grows; there is no eviction.** A replica keeps every pack it ever hydrated,
  including packs superseded by repack and reaped from the bucket, and repositories it
  no longer serves. Monitor the directory and reclaim it periodically: replace the node,
  or clear the directory **only while the replica is drained and stopped**. Deleting it
  under a live process races in-flight requests and the hydration lock file.

### Commit-graph refresh

After each full hydrate, a replica refreshes the cache repository's split commit-graph,
which speeds up history walks
([ADR-47](../adr/047-cache-repos-get-split-commit-graphs-written-by-full-hydrates.md)).
The refresh is best effort. A missing, stale, or failed graph makes reads slower, never
wrong, and never fails a request.

**Log contract.** Each refresh ends in one outcome from a fixed set. A successful refresh
logs nothing. Every other outcome logs exactly one line:

```
commit-graph refresh outcome=<outcome> repo=<cache repo path>: <detail>
```

The `outcome=` and `repo=` fields are stable. The detail text is for people and may
change; line breaks inside it (from Git's stderr) are escaped as `\n`, so a record
never spans lines. These are the outcomes:

| Outcome | Meaning | Action |
|---|---|---|
| `outcome=written` | Graph extended, or already current. Not logged. | None |
| `outcome=written_plain` | The first attempt ran out of its 3 s budget, and the retry without Bloom filters succeeded (it may have found nothing new to write). Later refreshes of that repo on this replica may also skip Bloom filters. | None needed. Expected for very large histories. |
| `outcome=skipped_tip_cap` | The repo's refs point at more than 10,000 distinct commits, so no refresh ran. | None. The existing graph (if any) stays valid. |
| `outcome=skipped_busy` | No slot was free right away, so the refresh was skipped and the next full hydrate retries. Refreshes share the git read API's 8-slot pool, count against the repository's 2-slot share like its reads, and may hold at most 2 of the pool's slots in total. | Look only if it is constant. That means sustained read or full-hydrate load. |
| `outcome=lock_held` | Git found `objects/info/commit-graphs/commit-graph-chain.lock` already present. | If it repeats for one repo, follow the stale-lock runbook below. |
| `outcome=memory_limit` | Git needed more heap than its 256 MiB cap (the read API's Heavy cap; Linux only) and stopped. | If it repeats for one repo, the history is too large for a graph within the cap. Reads stay correct. |
| `outcome=timed_out` | The refresh used its whole 5 s deadline. It held that repo's hydrate lock for up to about 7 s. | If it repeats for one repo, that repo pays this on every full hydrate. Consider the history size. |
| `outcome=canceled` | The request that triggered the full hydrate went away. | None |
| `outcome=failed` | Any other Git failure. The detail carries Git's stderr. | Read the detail. |

The [metrics listener](../reference/metrics.md) counts every outcome, including
`written`, in `openstorage_commit_graph_refresh_total{outcome}`. The labels are exactly
these outcomes, with no repository label: use the log line's `repo=` field to find the
repository.

**Stale-lock runbook.** The symptom is `outcome=lock_held` for the same `repo=` on every
full hydrate. The usual cause is that a Git process was killed (SIGKILL, the OOM killer,
or a host crash) during the short window in which it holds the lock. A person running
Git by hand in the cache can cause it too. Reads stay correct. Only that repo's graph
stops growing on that replica. To recover:

1. Take the repository path from the `repo=` field. A relative path is relative to the
   server's working directory.
2. Drain the replica (stop routing new requests to it) and stop the openstorage process.
3. Make sure nothing else is still writing to the cache. A Git child of the stopped
   server can outlive it, and so can a Git process someone started by hand. With the
   server stopped, `pgrep -af git` on the replica (or inside its container) must list no
   Git process working in the cache directory. If one does, wait for it to exit. Only a
   stopped replica makes this check safe: a live server can start a new writer right
   after the check.
4. Remove the lock file:
   `rm <repo>/objects/info/commit-graphs/commit-graph-chain.lock`. You may also delete
   leftover `objects/info/commit-graphs/tmp_graph_*` files from the killed writer; Git
   ignores them. If in doubt, delete the whole `objects/info/commit-graphs` directory
   instead. The graph is rebuildable.
5. Start the replica. After the next full hydrate of that repository (for example, after
   the next push), check that no new `lock_held` line appears. `git -C <repo>
   commit-graph verify` should succeed.

openstorage never removes this lock itself. It cannot prove that no live process owns
the lock: a crashed server can leave its Git child running, and other Git processes do
not take openstorage's lock.

## Concurrent boot: schema setup and migrations

Replicas race at boot by design: deploy tooling starts them all at once against the same
database. Postgres advisory locks serialize schema setup, so no external migration job
or leader election is needed.

| Boot step | Lock |
|---|---|
| Metadata schema | transaction-scoped `0x4D455441` (`"META"`) |
| Ref-store schema | transaction-scoped `0x5350494B45` (`"SPIKE"`) |
| River job-queue migrations | session-scoped `0x5249564552` (`"RIVER"`) |
| `packs`/`refs` covering indexes (`CREATE INDEX CONCURRENTLY`) | session-scoped `0x5350494458` (`"SPIDX"`), waiters poll once per second |

- **First boot on a fresh or upgraded database:** one replica runs the DDL; the others
  wait on the locks and continue once released.
- **Steady-state boots** see a current schema and go straight to serving.
- **Boot looks hung?** Check `pg_locks` for these advisory lock IDs before suspecting the
  replica.

## Health checks

`GET /healthz` is unauthenticated, bypasses rate limiting, and returns
`200 {"status":"ok"}`. It is a **liveness** probe only: it proves this process (not a
stale one squatting the port) accepts HTTP. It does **not** ping Postgres or the bucket.

- **Use it for** container liveness (restart on hang).
- **Don't let it alone drive** load-balancer target health. A replica that lost its
  database still answers `/healthz` while failing real requests with `5xx`, so it would
  stay in rotation.
- **Route on a check that exercises a real dependency.** There is no built-in
  dependency-aware readiness endpoint. Use, for example, a synthetic authenticated read
  against each target, or per-target `5xx`-rate ejection at the load balancer. Alert on
  API error rates for serving health.

## Reverse proxy and TLS

The server has no TLS support. Terminate TLS at a reverse proxy or load balancer and
forward plain HTTP to the replicas.

> [!WARNING]
> **Restrict ingress to the replica port** (firewall, security group, or network policy)
> to the proxy and health-check sources. The listener binds `0.0.0.0`; a client that
> reaches it directly talks plain HTTP and bypasses every proxy-enforced limit.

- **Set [`OPENSTORAGE_BASE_URL`](../reference/configuration.md#openstorage_base_url)** to
  the external `https://` URL so clone URLs and other `http_url` values are routable. It
  is only embedded in responses: internal traffic (the upstream-sync worker's loopback
  pushes) targets each replica's own listener, so replicas do not need to reach the
  external URL themselves.
- **Proxy everything.** The REST API and Git smart-HTTP (clone/push, LFS *batch*) share
  one listener. Response bodies (clones, archives) can be very large: stream, don't
  buffer.
- **Request duration and concurrency limits** remain the proxy's job.

### Request-body limits

The server bounds request bodies natively:

| Request | Server limit | Over the limit |
|---|---|---|
| Push | Decoded body ≤ [`OPENSTORAGE_MAX_PUSH_BYTES`](../reference/configuration.md#openstorage_max_push_bytes) (default 2 × the pack quota, 2 MiB minimum). Git is fed at most the cap's worth of bytes. | `413`. Git discards the truncated quarantine; a declared oversize is rejected without reading. |
| Gzip-encoded `git-upload-pack` (fetch negotiation) | Fixed 4 MiB decoded, plus a small compressed-stream allowance so decode-to-nothing gzip streams cannot hold a connection open. Must finish within 30 seconds. | `413` before Git runs |

- Size temporary disk for up to one push cap per concurrent push.
- A proxy request-body cap is optional defense in depth. If you set one, size it at or
  above the server cap: the proxy sees wire bytes, the server counts decoded (post-gzip)
  bytes.

### Git LFS traffic

**Downloads bypass the proxy on S3.** The batch API hands clients presigned bucket URLs
whenever the backend can sign; the server-proxied transfer is only the fallback for
backends that cannot (local file mode). Git LFS clients therefore need direct network
reach to the configured S3 endpoint. A VPC-internal bucket endpoint breaks LFS pulls for
external clients, even though the batch request succeeds.

**Uploads always go through the server.** A presigned PUT cannot bind content length or
checksum, so the server receives every upload, verifies its size and sha256, and meters
it against the repository [storage quota](../reference/configuration.md#openstorage_pack_quota_bytes)
before publishing.

- Size the proxy's request-body cap and timeouts for the largest LFS object you accept.
- Uploads stage under the server's `$TMPDIR` before publication.
  [`OPENSTORAGE_LFS_STAGING_BYTES`](../reference/configuration.md#openstorage_lfs_staging_bytes)
  caps per-replica staging (default 32 GiB). Over-budget uploads answer `429`/`413`, and
  each object allows one in-flight upload per replica.
- One repository can hold at most a third of the budget, so size it at roughly `3 ×` the
  largest object you accept. The default keeps one default-quota-sized (10 GiB) object
  stageable.
- Give `$TMPDIR` at least the full budget plus Git push quarantine headroom, or lower the
  budget to the disk you provision.

> [!WARNING]
> **Upgrading from v0.1.0:** older replicas hand clients presigned upload URLs that write
> to the bucket directly, bypassing verification. Those URLs stay valid up to 15 minutes
> after the last old replica exits, so complete the roll promptly. LFS objects uploaded
> around the roll window are re-verified on their next write-scoped touch only while
> unregistered: trust them as much as you trusted v0.1.0's presigned path
> ([#78](https://github.com/coder/openstorage/issues/78) tracks closing this window with
> object-version fencing).

### Client identity behind the proxy (`OPENSTORAGE_TRUSTED_PROXIES`)

Rate limiting ([`OPENSTORAGE_RATE_RPS` / `OPENSTORAGE_RATE_BURST`](../reference/configuration.md#openstorage_rate_rps--openstorage_rate_burst))
keys token buckets by client IP.

- **Buckets are per replica and per surface** (REST and Git each keep their own
  in-process state). A client spread across N replicas can consume roughly N× the REST
  budget plus N separate Git budgets. Size values as per-replica limits; enforce at the
  edge for one global per-client ceiling.
- **Behind a proxy, the TCP peer is the proxy**, so all clients would share one bucket.
  List your proxies in
  [`OPENSTORAGE_TRUSTED_PROXIES`](../reference/configuration.md#openstorage_trusted_proxies),
  a comma-separated CIDR allowlist.

Resolution rules:

- **Trusted peer:** if the TCP peer is allowlisted, both limiters use the **rightmost
  `X-Forwarded-For` hop that is not itself allowlisted**, i.e. the client address seen by
  the nearest trusted proxy. Hops your own proxies appended are skipped. Hops a client
  fabricated are never reached, because your proxy appends the real address to their
  right.
- **Everything else fails closed to the TCP peer address:** allowlist unset or empty,
  untrusted peer, empty/malformed hop at the boundary, an all-trusted chain, or an
  `X-Forwarded-For` chain over 4096 bytes.
- **Validated at startup, even with rate limiting disabled.** A malformed entry fails the
  boot. IPv4-mapped IPv6 entries are normalized to IPv4; zone-scoped addresses and
  IPv4-mapped prefixes shorter than `/96` are rejected.
- **List only proxies that set or append `X-Forwarded-For` themselves.** Trusting a proxy
  that forwards client-supplied headers verbatim lets any client mint fresh rate-limit
  buckets at will.

### Webhook egress

Webhook deliveries use a native SSRF guard. By default, every attempt resolves the
registered hostname at dial time and fails closed unless **every** answer is a public,
global-unicast address outside the IANA special-purpose registries. The
[reference](../reference/configuration.md#openstorage_webhook_allow_private) lists the
denied ranges, including loopback, private, link-local, and cloud-metadata targets.

- **Dial time is authoritative.** Registration checks IP literals and current DNS answers
  only for fast feedback, so acceptance does not imply reachability. At dial time,
  resolution failures, empty answers, and mixed allowed/denied answer sets fail closed.
- **Connections** go to the vetted IP literal, fresh for every attempt. They ignore
  environment HTTP proxies and never follow redirects.
- **A denial is an ordinary delivery failure:** the queue backend applies its normal
  retry and dead-letter/discard behavior; the worker does not crash.

To deliver to internal receivers, set `OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE=true`. It admits
loopback, RFC 1918, RFC 6598 shared, link-local, and ULA targets, and logs a startup
warning. Cloud metadata (`169.254.169.254` and `fd00:ec2::254`), other IANA
special-purpose ranges, malformed/non-routable targets, and redirects stay blocked. Keep
network egress controls as defense in depth, especially where other workloads share the
replica identity or network policy.

## Rolling upgrades and restarts

On `SIGTERM`/`SIGINT` the server drains for a **bounded 10 seconds** in total. With a
SQLite database, it first stops starting upstream sync runs and gives runs in flight up
to 5 seconds of that budget, still accepting connections, because those runs push to the
server's own listener. Runs still going then are cancelled and get up to 2 more seconds
to record that their requests stay pending for the next process. (PostgreSQL and S3WAL do not have this step yet; see
issue #343.) The server then stops accepting connections and drains in-flight requests
for the rest of the budget. It then stops background workers and, in
[zero-infra mode](#zero-infrastructure-mode-experimental-one-bucket-one-node), releases
the instance lease so a successor need not wait out the lease TTL. A second signal
during the drain kills the process immediately. Requests still running after the drain
are dropped; a dropped push is safe for data (see below) but fails for the client.

So roll one replica at a time, using the proxy:

1. Deregister it from the load balancer.
2. Wait for in-flight requests to finish. Git pushes can be long; allow a generous
   drain window.
3. Stop it, start the new version, and wait for `/healthz`.
4. Re-register it and move to the next replica.

**Interrupted work cannot corrupt data.** Pack data reaches the bucket *before* any ref
moves, so a killed push never leaves a ref pointing at missing objects. It can leave
storage behind:

- an unregistered blob, reclaimable by the [sweeper](storage-lifecycle-and-sweeping.md); or
- if the kill lands between pack registration and the ref update, a **registered** pack.
  Its objects stay quota-counted and repack carries them forward
  ([why](../explanation/storage-engine.md#why-repack-packs-the-union-not-reachable-objects)).
  A client retry of the same push deduplicates against it and makes it reachable.

**A mixed-version fleet mid-roll is expected and fine**: schema upgrades use the same
[boot-time locking](#concurrent-boot-schema-setup-and-migrations). Two deliberate
exceptions:

> [!IMPORTANT]
> **Do not enable delete-mode sweeping until the whole fleet runs a build that has it.**
> See the [storage lifecycle guide](storage-lifecycle-and-sweeping.md#before-enabling-delete-mode-fleet-prerequisite)
> for the exact prerequisite and why list-only mode is safe on any fleet.

> [!WARNING]
> **Upgrading from v0.2.0 or older with Git LFS in use.** Stage steps 1 and 2 *before*
> rolling:
>
> 1. On upgraded replicas, set
>    [`OPENSTORAGE_LFS_V020_WRITE_COMPAT`](../reference/configuration.md#openstorage_lfs_v020_write_compat)
>    for the duration of the roll. Registered LFS objects now live at a canonical key old
>    replicas cannot derive (ADR-41); the flag dual-publishes to the legacy key so old
>    replicas keep serving LFS reads until the fleet is fully rolled.
> 2. On **every** replica, disable
>    [`OPENSTORAGE_SWEEP_DELETE_LFS`](../reference/configuration.md#openstorage_sweep_delete_lfs)
>    for the duration of the roll. Pre-0.15 binaries, including the not-yet-upgraded
>    v0.2.0 ones, need `CODESTORAGE_SWEEP_DELETE_LFS=false`.
> 3. After the roll, drop the compat flag and, if desired, re-enable LFS sweeping.
> 4. Legacy-spelled keys (adoption sources, compat copies) are reclaimed only by the
>    separate post-roll
>    [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](../reference/configuration.md#openstorage_sweep_delete_lfs_legacy)
>    opt-in, whose transition gate dominates every sweep flag on upgraded binaries.
>    Enable it only once the last old replica has been gone ≥ 15 minutes.
>
> Why step 2: the upgrade rewrites registry rows to the canonical spelling in place, so a
> touched object's legacy blob looks rowless to an old sweeper. Old binaries have no
> legacy gate, so a reap-enabled v0.2.0 sweeper deletes that blob and breaks old-replica
> reads mid-roll. Upgraded replicas refuse to start with both flags set; the hazard is
> the old nodes.

**Upstream-sync claims are leased.** If a replica dies mid-sync, the next run steals its
claim once the lease expires, so a crash delays syncing but never stops it. Claims from
pre-lease builds (written mid-roll, or wedged by a crash before the upgrade) have no
lease and none of the holder guards that make stealing safe. New replicas adopt them
under a grace period but steal them only after
[`OPENSTORAGE_SYNC_STEAL_LEGACY`](../reference/configuration.md#openstorage_sync_steal_legacy)
asserts no pre-lease builds remain. A live old-version run always finishes undisturbed;
pre-upgrade wedges heal once the opt-in is set.

## JWT keys across the fleet

Tokens are verified against the org's public keys in the database. Each replica
**rewrites** that key set at boot from its `OPENSTORAGE_ORG_PUBKEYS` file, and the last
boot wins. Keep the file contents identical everywhere, distributed via your secret
store.

> [!IMPORTANT]
> RSA org keys must be at least **2048 bits**. When upgrading from a version that
> accepted smaller RSA keys, complete the rotation below **on the old fleet first**:
> reissue client tokens/Git credentials and remove every weak key before upgrading. A
> mixed strong+weak key set fails closed at startup and verification. See the
> [public-key policy](../reference/authentication.md#organization-public-key-strength).

Rotation without downtime:

1. Append the new public key block to the PEM file (all replicas).
2. Rolling-restart the fleet so a post-change boot registers the combined set.
3. Start minting tokens with the new private key.
4. After old tokens expire, remove the retired block from the file and rolling-restart
   again.

Also note:

- **PEM parsing stops silently** at the first undecodable block once at least one key
  parsed: a truncated block appended after a valid key is dropped, not rejected. After a
  rotation, verify that a token signed with the *new* key is accepted; don't rely on
  startup success.
- **Upstream sync leaves the org key set alone.** The sync worker signs its loopback
  pushes with a per-process key that never leaves memory, and sends them to the replica's
  own listener, never the external base URL. Replicas cannot interfere with each other's
  `pull-upstream` runs, and the database key set stays exactly the configured file keys.

## Background maintenance in a fleet

Orphan-pack sweeps, repack folds, webhook deliveries, and upstream syncs run as durable
River jobs in Postgres. Any replica may pick up any job, jobs survive replica loss, and a
job triggered through one replica may run (and log) on another. What the jobs do, how to
read their reports, and delete-mode prerequisites: see the
[storage lifecycle guide](storage-lifecycle-and-sweeping.md).

## What to monitor

| Signal | Why |
|---|---|
| API/git `5xx` rates per replica | Real serving health — `/healthz` won't catch a lost database |
| `orphan pack sweep finished` log line | Per-sweep report: orphan counts/bytes, deletions, reclaimed bytes ([field reference](storage-lifecycle-and-sweeping.md#reading-a-sweep-report)) |
| `repack: folded packs` / `repack trigger: scheduled fold` log lines | Repack progress; a repo that triggers but never folds warrants a look |
| `openstorage hook: pre-receive: repository pack quota exceeded` (client-visible) / `quota: receive-pack … at/over quota` (server log) | Tenants hitting the [pack quota](../reference/configuration.md#openstorage_pack_quota_bytes) |
| `commit-graph refresh outcome=…` log lines | A `repo=` that repeats `lock_held`, `timed_out`, or `failed` needs a look ([outcomes and stale-lock runbook](#commit-graph-refresh)) |
| Postgres availability + advisory-lock waits at boot | The database is the single authority; boot hangs show in `pg_locks` |

The opt-in [metrics listener](../reference/metrics.md) covers some of these signals;
the mapping below says which ones remain log-only.

### Metrics and log signal mapping

Set [`OPENSTORAGE_METRICS_LISTEN`](../reference/configuration.md#openstorage_metrics_listen)
to expose Prometheus metrics on a separate, **unauthenticated** listener. Bind it to
loopback or a private network only the scraper can reach. Metrics are per `serve`
process: work in Git hook subprocesses (the ref updates of a push) is not counted.

Rows marked **log only** have no metric; their log line or stderr message stays the
signal. For those rows the stable parts are the event text or prefix shown, its listed
fields, when it is emitted, and its channel (structured log or stderr). Free-form error
text appended after it is not part of the contract.

| Signal (table) | Metric | Notes |
|---|---|---|
| API/git `5xx` rates per replica (What to monitor) | `openstorage_http_requests_total{code_class="5xx"}` by `surface` | Also `openstorage_http_request_duration_seconds` |
| `orphan pack sweep finished` (What to monitor) | `openstorage_job_runs_total{job="orphan_sweep"}` | Outcome only; orphan counts and bytes stay **log only** in the report line |
| `repack: folded packs` / `repack trigger: scheduled fold` (What to monitor) | `openstorage_job_runs_total{job="repack"}` | Fold outcomes only; trigger lines stay **log only** |
| Pack quota exceeded (What to monitor) | **log only** | Client-visible hook error plus the `quota: receive-pack … at/over quota` server line |
| Postgres availability and advisory-lock waits (What to monitor) | **log only** | Observe PostgreSQL itself (`pg_locks`) |
| S3WAL `… conditional-write contention retries exhausted` (S3WAL signals) | `openstorage_s3wal_cas_contention_exhausted_total` by `scope` | Serve-process mutations only; push-side failures reach the client and hook stderr |
| `WARN s3wal manifest approaching size cap` (S3WAL signals) | `openstorage_s3wal_manifest_size_guard_total{result="warn"}` | Counts when the once-per-repository warning is logged |
| S3WAL `manifest would grow to … refusing new registrations` (S3WAL signals) | `openstorage_s3wal_manifest_size_guard_total{result="refuse"}` | Serve-process refusals only |
| Lease `… is held by another serve process …` (S3WAL signals) | **log only** | Startup refusal; the process never serves |
| Lease `… fail-stop` (S3WAL signals) | **log only** for the verdict | `openstorage_s3wal_lease_renewal_failures_total` counts failed renewal writes before it; `openstorage_s3wal_lease_held` is local state, and the process exits on fail-stop |
| Startup `no usable HTTP Date witness` / lease `freshness cannot be judged` / `is corrupt` (S3WAL signals) | **log only** | Startup refusals |
| `WARN s3wal clock skew guard tripped` (S3WAL signals) | **log only** | |
| `commit-graph refresh outcome=…` (What to monitor) | `openstorage_commit_graph_refresh_total` by `outcome` | Counts every refresh, including the silent `written`; the repository stays **log only** in the line's `repo=` field |

## Zero-infrastructure mode (experimental): one bucket, one node

The **experimental** `s3wal://` mode drops Postgres and removes local disk from the
durability story. Refs and metadata live in conditional-CAS objects under the bucket's
reserved `_wal/` prefix, next to the ordinary `packs/` and `lfs/` payloads. One
S3-compatible bucket is the sole durable authority: kill the process or lose the machine,
and a fresh node pointed at the bucket reconstructs full service. The cache dir stays a
rebuildable cache, as in every mode.

Background: [ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md),
issue [#133](https://github.com/coder/openstorage/issues/133),
[storage engine](../explanation/storage-engine.md#the-zero-infrastructure-backend-s3-as-sole-authority-s3wal).

Example launch:

```sh
OPENSTORAGE_PG_URL='s3wal://my-bucket?endpoint=https://s3.example.net&region=us-east-1' \
OPENSTORAGE_S3_URL='s3://my-bucket?endpoint=https://s3.example.net&s3ForcePathStyle=true&region=us-east-1' \
OPENSTORAGE_ORG=org1 OPENSTORAGE_ORG_PUBKEYS=/etc/openstorage/org.pub.pem \
openstorage serve --listen 0.0.0.0:8080 --cache-dir /var/cache/openstorage
```

### Setup rules

- Use the same bucket for both URLs (the expected layout). A `file://` blob URL is refused at startup
  ([pairing rule](../reference/configuration.md#openstorage_pg_url)).
- **Create the bucket before the first boot.** The startup conditional-write probe runs
  before the blob layer's create-if-missing bootstrap, so a missing bucket is boot-fatal.
- Never write to the `_wal/` prefix yourself.
- Do not enable bucket versioning or object lock.

> [!WARNING]
> Never point two serve processes at one bucket. The
> [instance lease](../reference/configuration.md#openstorage_s3wal_lease_ttl) exists to
> detect exactly that mistake (startup refusal / fail-stop), best-effort.

### Store requirements

| Requirement | How it is checked |
|---|---|
| Faithful conditional writes (`If-Match` / `If-None-Match: *`) | A boot-time probe refuses stores that fail it |
| **Strong read-after-write and list-after-write consistency:** after a successful PUT, every later GET or LIST from any client observes it | Cannot be probed. You must guarantee it. |
| Reliable store HTTP `Date` witnesses, for expiry decisions | Startup refuses a store with no usable witness during the probe |

> [!WARNING]
> The engine trusts every unconditional read of its `_wal/` objects. A store (or a
> caching proxy in front of one) that can serve a superseded object version silently
> produces wrong read-only verdicts, such as a reservation sweep or a hydration
> generation check that misses a competitor's write. AWS S3 has provided this guarantee
> in all regions [since December 2020](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/)
> ([consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)).
> Verify it for any other store, and never front the bucket with a cache.

Support status:

- **All S3WAL targets remain experimental.** Passing the probe is not a support
  certificate; the [storage qualification matrix](../explanation/storage-engine.md#storage-compatibility-and-qualification)
  separates tested releases/configurations from capability requirements and support
  status.
- AWS S3 and SeaweedFS are conditional future promotion targets under
  [#192 G1](https://github.com/coder/openstorage/issues/192). That does not mean the
  remaining real-S3 or operations gates passed.
- MinIO and other endpoints are **not qualified for S3WAL authority**. Ordinary blob
  compatibility is a separate question.

### Credentials and IAM

Credentials come from the standard AWS chain. On the one bucket, the node needs the
[general bucket permissions](#backing-services):

| Permission | Used for |
|---|---|
| `s3:GetObject`, `s3:PutObject` | Object reads and writes. Conditional `If-Match`/`If-None-Match` writes are ordinary PutObject calls, and the quarantine handshake's server-side copies use these two. |
| `s3:DeleteObject` | Object deletes |
| `s3:AbortMultipartUpload` | Cleaning up a failed multipart quarantine copy |
| `s3:ListBucket` | Prefix listing and the bucket reachability probes |
| `s3:GetBucketVersioning` | Delete-mode and purge guards verify versioning is off |

`s3:CreateBucket` is not required for a pre-provisioned bucket; a denied create falls
back to a reachability probe.

Set [`OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`](../reference/configuration.md#openstorage_s3_expected_bucket_owner).
Both S3 clients (the blob data plane *and* the `_wal/` conditional client) stamp the
owner condition on every request. This matters doubly here: the bucket holds the org's
public keys and the ref authority, not just packs.

### Scaling boundaries

This is a deliberate single-node design that trades throughput for zero infrastructure.

| Boundary | Consequence |
|---|---|
| One node, ever | No replicas, no failover process — recovery is "start a new node against the bucket". The instance lease detects accidental dual-serve; it does not make it safe. |
| One CAS point per repository group | Concurrent pushes to the *same* repository serialize on one object; hot-repo write throughput is a handful of ref flips per second, and contention shows up as CAS retries. Different repositories don't contend. |
| Every ref/metadata operation is an S3 round trip | Latency floors are S3 latency, not local-disk or Postgres latency. Same-region compute is assumed. |
| Manifest size | A repository group's whole registry lives in one JSON object: growth-guarded mutations warn above 4 MiB and are refused above 16 MiB; see the [size-guard scope](#s3wal-operational-signals) below. |
| Background jobs are in-process | Tickers, no durable River queue; queued work dies with the process (durable request markers are re-enqueued at boot). Local mode instead keeps its sync and push schedule, including retry backoffs, in SQLite. |

If any of these bind, use local mode (SQLite on a persistent volume) or the Postgres
topology. Small single-tenant deployments, the target, sit far below them.

### Measured boundaries

From the recorded benchmark campaigns ([full method and tables](../plans/s3wal-bench-results.md)):
loopback SeaweedFS plus a real AWS S3 campaign (eu-central-1, near-same-region compute;
the qualification suite passed all surfaces against real S3).

| Measurement | Result |
|---|---|
| Uncontended small `git push`, loopback | 138 vs 112 ms p50 for local mode (~26 ms, +23%) |
| Same push, **real S3** | ~1.1 s p50 vs ~0.4 s for a Postgres+S3 node against the same bucket region: ~2.8× per push, dominated by sequential S3 round trips |
| One hot repository group, real S3 | ~27 successful manifest CAS advances/s regardless of writer count (the one-CAS-point-per-group ceiling); more writers add lost races, not throughput |
| **CAS-success latency** tail under sustained synthetic hammering (microbench manifest advances, not full pushes) | p99 6.0 s at N=8, 16.9 s at N=32 |
| Real-push p99, loopback e2e push bench, N=32 | 5.8 s vs local mode's 2.8 s |
| Lost updates | Zero: every acknowledged push was durable across ~15,300 real pushes in sustained multi-writer campaigns, plus ~790,000 lost-then-retried CAS races in microbench cells |

- The real-S3 figures are **preliminary single-day observations**, measured **before**
  later same-day per-push round-trip trims; ≥ 3 separate-day runs are still pending.
  Treat them as a directional upper bound, not capacity guidance.
- Distinct repositories do not contend.
- The campaigns observed client-visible hook rejections on contention exhaustion
  (`s3wal cas ref repo_…: 16 attempts: s3wal: conditional-write contention retries exhausted`).
  Interpret this bounded-attempt failure with the
  [operational signals](#s3wal-operational-signals), not as a capacity diagnosis.

### Recovery

**Crash recovery restarts repository state from the bucket.** Node replacement and crash
recovery are the same operation: start a fresh node (empty disk, empty cache dir) against
the same two URLs. Refs, metadata, packs, and LFS objects reconstruct from the bucket; the
cache re-hydrates on demand. The `s3wal` [test lane](../reference/testing.md) drills this
path end to end.

The node's *configuration* is not in the bucket. Provision the replacement from your
config/secret store with the same two storage URLs, `OPENSTORAGE_ORG`, the org public-key
PEM file on its own disk, and the same `OPENSTORAGE_SECRETS_KEY` if one encrypts the
metadata store's secret columns.

**Bucket loss is a separate problem.** Node replacement from the same bucket is not a
backup. Use the [stopped-node backup/restore runbook](backup-restore-s3wal.md) for an
opaque whole-bucket snapshot, independent inventory verification, and fresh-cache
recovery into an empty bucket. A live `aws s3 sync` is not a consistent snapshot. The
procedure has a local fixture drill; it is not a real-S3 disaster-recovery qualification
or a promotion/support claim.

**Format changes need a separate upgrade procedure.** Read the
[S3WAL format stability policy](../explanation/storage-engine.md#s3wal-format-stability-policy)
before changing binaries. The stopped-node backup is a precaution, not a cross-version
rollback guarantee.

### S3WAL operational signals

An **experimental, single-node** log/error runbook, not an incident/real-S3
qualification. Use it with the [general monitoring guidance](#what-to-monitor). Rows that
also have a metric are listed in the [metrics and log mapping](#metrics-and-log-signal-mapping).

- Returned store errors are not server log entries themselves; the caller decides how
  they appear.
- Terminal `serve` and `hook` errors print to stderr with `openstorage serve:` /
  `openstorage hook:` prefixes and exit 1. Hook errors can reach Git clients.
- `WARN` means `slog.Warn`. Ellipses abbreviate variable text, not literal messages.

| Signal/error | Emission conditions and fields | What it establishes | What it does not establish | Safe response |
|---|---|---|---|---|
| Returned `s3wal …: 16 attempts: s3wal: conditional-write contention retries exhausted` | The [manifest loop](../../internal/ref/s3walstore.go#L474-L614) has 16 fresh attempts (an optional cached-seed try is extra), naming operation and base. The [scan-state loop](../../internal/ref/s3walstore_scanstate.go#L185-L283) also has a 16-attempt budget, naming operation and `_wal/scan/` key; shared lapse-marker updates can contend across receive hooks. The [conditional client](../../internal/ref/s3wal/casobj.go#L342-L398) separately retries up to 16 `409 ConditionalRequestConflict` attempts, naming `create`/`advance` and key. | A bounded attempt budget failed for the observed operation/base or key. | Not a global capacity limit or proven root cause. Manifest/scan retries also include never-sent/recovery paths. These loops do not log ordinary retries; that says nothing about other SDK/transport logging. | Preserve the full error, operation and identifier; check store connectivity/errors. For a manifest base, inspect same-base writers and actual refs/results. For a scan-state key, inspect marker work across concurrent receive hooks, not just that repository's refs. Reduce competing work before a controlled retry; do not retry until green. |
| `WARN s3wal manifest approaching size cap` | [Size guard](../../internal/ref/s3walstore.go#L617-L637): growth predicate true; candidate encoded bytes **> 4 MiB and ≤ 16 MiB**. Fields: `base`, `bytes`, `warn`, `cap`. Suppressed after the first warning for that base in that store's map. | A candidate mutation exceeded the soft threshold. | Not proof the write committed, a current-size gauge, or one warning per process/fleet lifetime. No warning does not prove spare capacity. | Record base/bytes and growth-producing operations; plan supported application-level cleanup or a deployment change before the hard cap. Never edit the manifest. |
| Returned `s3wal …: manifest would grow to … bytes (cap 16777216): refusing new registrations` | Same guard, growth predicate true and candidate bytes **> 16 MiB**; text includes operation, base, size and cap. Hard refusal precedes the soft-warning branch. | This candidate was refused before its conditional PUT. | Not a blanket read-only mode or a promise that deletes, updates, reads or purge succeed; other guards and failures remain. | Stop adding registry entries to the affected group. Assess supported cleanup through ordinary APIs; preserve failures and escalate if cleanup is blocked. Do not raise/bypass the guard. |
| Stderr `s3wal instance lease _wal/instance-lease is held by another serve process …` | [Acquisition](../../internal/ref/s3wal/lease.go#L140-L229) found a fresh recorded lease, or a foreign holder won an acquisition race. Fields in text: holder prefix, rounded store-clock age, holder's TTL. | This startup refused that lease. | Not proof the holder is still alive; a stopped process can leave a lease. Not permission to force takeover. | Identify/fence writers and follow the [shutdown/restart procedure](#s3wal-shutdown-and-restart) below. |
| Stderr `s3wal instance lease … fail-stop` | [Renewal](../../internal/ref/s3wal/lease.go#L260-L330) re-read found a foreign holder, corruption or disappearance; or failures persisted until the last accepted renewal was at least TTL − cadence old. Cadence is TTL/4. `unrenewable since …` includes elapsed time, TTL and write/read errors. | The heartbeat returned a loss verdict; [`serve` exits 1](../../cmd/openstorage/main.go#L178-L196). | Not proof of whole-system integrity, safe dual servers, or that every transient renewal failure exits. Fail-stop does not run graceful cleanup. | Preserve stderr and supervisor history; fence traffic/restarts, identify every writer, inspect connectivity and store evidence before restarting one node. |
| Stderr `s3wal init: store supplied no usable HTTP Date witness during the probe …` or lease `… freshness cannot be judged on the store clock …` / `… is corrupt …` | [Startup probe](../../internal/ref/s3walstore.go#L200-L215) lacks a usable Date witness (missing or older than 30s); [lease acquisition](../../internal/ref/s3wal/lease.go#L156-L229) cannot decode the lease, finds no holder in it, or cannot judge its store-clock age. | Startup failed closed on missing/invalid evidence. | Not necessarily host-clock skew or a stale/dead holder. | Preserve errors; inspect store/proxy Date and Last-Modified behavior and connectivity. Keep writers fenced and escalate corruption. Do not follow diagnostic text suggesting object deletion. |
| `WARN s3wal clock skew guard tripped: demoting expiry verdict to report-only` | [Expiry guard](../../internal/ref/s3walstore_quota.go#L68-L124): positive TTL appears elapsed, but clock or store-Date check fails. Fields: `base`, `what`, `manifest_clk`, `local_now`, `wall_drift`, `store_date`, `store_date_age`, `ttl`. | **That expiry verdict** was refused. | Not a diagnosis of a wrong host clock, a global report-only switch, or proof that all other operations are safe/successful. | Compare host time/drift, manifest clock and store/proxy Date evidence. A missing/stale witness also trips the guard. Fix the cause before a controlled restart; do not disable time safeguards. |

<details>
<summary>Size-guard and clock-guard scope</summary>

**Size guard:**

- The guard runs only when `mu.grows` is non-nil and returns true. It does not measure
  the byte delta against the old manifest.
- Equality with either threshold does not trigger that threshold's action.
- Mutations outside that predicate bypass **only this guard**.
- Each [`newS3WALStore`](../../internal/ref/s3walstore.go#L153-L194) allocates its own
  warning map. `serve` opens a store, and
  [hook invocations open their own](../../internal/githttp/hook.go#L1728-L1732).
  Reopening a store permits another warning for the same base.

**Clock guard:**

- The [clock checks](../../internal/ref/s3wal/manifest.go#L334-L427) compare a manifest
  clock ahead of local time, and wall-vs-monotonic drift, against `min(TTL/4, 15m)`.
- The store-Date arm also requires a fresh witness and local/store agreement. Bounds
  below the 2s Date-resolution tolerance disable that arm.
- A quiet repository's old manifest clock alone is not skew.
- The conditional client [records parsed HTTP Date headers](../../internal/ref/s3wal/casobj.go#L178-L213).
- A prior wall-clock step can leave the [store's drift baseline](../../internal/ref/s3walstore.go#L640-L654)
  tripped until restart. Restart does not repair an incorrect clock or missing witness.

</details>

### S3WAL shutdown and restart

> [!WARNING]
> Never manually delete the lease, edit `_wal/`, force takeover, or disable safeguards to
> make startup pass.

1. **Preserve evidence and fence.** Keep the full error/log context, binary version,
   affected base/operation, timestamps, holder/TTL evidence, and supervisor history.
   Redact credentials and signed URLs. Identify every serving process, hook, and
   maintenance writer; prevent replacement launches and fence access before intervening.
2. **Stop the old node** with `SIGTERM`/`SIGINT` and verify it and its writers exited.
   [Graceful shutdown](#rolling-upgrades-and-restarts) drains HTTP, then attempts lease
   release with a **5s best-effort bound**. Immediate restart works only if release
   succeeds; graceful shutdown can also leave the lease behind.
3. **Wait out a leftover lease.** After a hard stop, heartbeat fail-stop, or failed
   release, keep the old writers fenced. Allow at least the **holder's recorded TTL plus
   2s** after confirmed stop (default TTL 2m). Acquisition uses store Date and
   Last-Modified with a whole-second resolution margin, not a comparison of host clocks.
   Start only one replacement. If it still refuses a held lease, preserve that evidence,
   stay fenced, and wait for its reported expiry before a controlled retry. Unjudgeable or
   corrupt leases need investigation, not repeated starts.
4. **Verify before restoring traffic.** Check authenticated repository reads and
   expected refs; `/healthz` alone is not recovery proof. The instance lease is a
   diagnostic/fail-stop mechanism, **not** independent proof of integrity or support for
   dual servers.
