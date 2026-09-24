# Configuration Reference

Every `OPENSTORAGE_*` environment variable that the `openstorage` binary reads, with its
default, validation rules, and key interactions. Source of truth:
[`cmd/openstorage/config.go`](../../cmd/openstorage/config.go). Flags are in the
[CLI reference](cli.md).

## Precedence and validation

Each setting takes its value from the first source that is set:

1. Explicit flag.
2. Canonical `OPENSTORAGE_*` variable. It wins even when set to an empty value.
3. `CODESTORAGE_*` fallback. Supported for one minor release, **0.15.x**. `serve` logs a
   boot-time deprecation warning for each fallback it uses, without the value.
4. Older legacy alias (listed per variable).
5. `--data-dir` derivation (local mode).
6. Built-in default.

- An empty canonical value is never refilled from the old prefix or an older alias. It
  follows the setting's normal validation and default rules: an empty storage URL derives
  from `--data-dir` or fails startup; it never falls back to a legacy alias.
- Malformed values are fail-fast startup errors naming the variable. They are never
  replaced by a value from the old prefix.
- Migrate before upgrading to 0.16.0: see the
  [migration guide](../how-to/migrate-to-openstorage.md).

## Summary

| Variable | Default | Purpose |
|---|---|---|
| [`OPENSTORAGE_PG_URL`](#openstorage_pg_url) | — (required*) | Database URL: refs + metadata (`postgres://`, `postgresql://`, `sqlite://`, or the experimental `s3wal://`) |
| [`OPENSTORAGE_S3_URL`](#openstorage_s3_url) | — (required*) | Pack storage URL (`s3://` or `file://`) |
| [`OPENSTORAGE_S3WAL_LEASE_TTL`](#openstorage_s3wal_lease_ttl) | `2m` | s3wal mode only: instance-lease TTL for the dual-serve guard (`0` disables) |
| [`OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`](#openstorage_s3_expected_bucket_owner) | unset (disabled) | Assert the bucket-owner AWS account ID on every S3 request |
| [`OPENSTORAGE_S3_OP_LOG`](#openstorage_s3_op_log) | unset (disabled) | Diagnostics: append one line per S3 HTTP round trip to this file |
| [`OPENSTORAGE_DATA_DIR`](#openstorage_data_dir) | — | Local-mode data directory; derives the two URLs above |
| [`OPENSTORAGE_LISTEN`](#openstorage_listen) | `127.0.0.1:0` | Listen address |
| [`OPENSTORAGE_METRICS_LISTEN`](#openstorage_metrics_listen) | unset (disabled) | Opt-in, unauthenticated Prometheus `/metrics` listener address |
| [`OPENSTORAGE_ORG`](#openstorage_org) | — (required) | Org name (JWT `iss` claim) |
| [`OPENSTORAGE_ORG_PUBKEYS`](#openstorage_org_pubkeys) | — (required) | PEM file with the org's public keys |
| [`OPENSTORAGE_BASE_URL`](#openstorage_base_url) | derived from listener | External base URL for `http_url` values |
| [`OPENSTORAGE_RATE_RPS`](#openstorage_rate_rps--openstorage_rate_burst) | unset (disabled) | Per-client-IP rate limit: sustained requests/second |
| [`OPENSTORAGE_RATE_BURST`](#openstorage_rate_rps--openstorage_rate_burst) | unset (disabled) | Rate-limit token-bucket depth |
| [`OPENSTORAGE_TRUSTED_PROXIES`](#openstorage_trusted_proxies) | unset | CIDR allowlist of reverse proxies for rate-limit client identity |
| [`OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE`](#openstorage_webhook_allow_private) | `false` | Permit private-network webhook receivers (metadata and redirects stay blocked) |
| [`OPENSTORAGE_SYNC_ALLOW_PRIVATE`](#openstorage_sync_allow_private) | unset (transitional) | Unified sync egress policy: permit private-network upstreams for pull and push (metadata stays blocked) |
| [`OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE`](#openstorage_sync_push_allow_private) | `false` | **Deprecated** push-only spelling of the sync egress policy |
| [`OPENSTORAGE_SECRETS_KEY`](#openstorage_secrets_key) | unset (plaintext) | AES-256 key list for at-rest encryption of stored webhook secrets and upstream credentials |
| [`OPENSTORAGE_SECRETS_MODE`](#openstorage_secrets_mode) | `encrypt` | `decrypt-only` stages the key fleet-wide before encrypted writes begin |
| [`OPENSTORAGE_PACK_QUOTA_BYTES`](#openstorage_pack_quota_bytes) | 10 GiB | Per-repo storage quota, packs + LFS (`0` disables) |
| [`OPENSTORAGE_LFS_STAGING_BYTES`](#openstorage_lfs_staging_bytes) | 32 GiB | Per-replica cap on in-flight LFS upload staging (`0` disables) |
| [`OPENSTORAGE_LFS_V020_WRITE_COMPAT`](#openstorage_lfs_v020_write_compat) | `false` | Dual-publish LFS objects to the legacy v0.2.0 key during a rolling upgrade |
| [`OPENSTORAGE_PACK_RESERVATION_TTL`](#openstorage_pack_reservation_ttl) | `1h` | Lifetime of a crashed writer's quota reservation |
| [`OPENSTORAGE_MAX_PUSH_BYTES`](#openstorage_max_push_bytes) | 2 × pack quota (min 2 MiB) | Decoded-byte cap on one git push (`0` disables) |
| [`OPENSTORAGE_VERIFY_SIG_BUDGET`](#openstorage_verify_sig_budget) | `10000` | Per-push `verify-sig` verified-commit budget (`0` disables) |
| [`OPENSTORAGE_SWEEP_INTERVAL`](#openstorage_sweep_interval) | `1h` | Orphan-pack sweep cadence (`0` disables sweeping) |
| [`OPENSTORAGE_SWEEP_AGE_THRESHOLD`](#openstorage_sweep_age_threshold) | `24h` | Minimum blob age before an unregistered pack counts as an orphan |
| [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](#openstorage_sweep_delete_enabled) | `false` | Opt-in: actually delete general orphans (list-only otherwise) |
| [`OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`](#openstorage_sweep_delete_superseded) | `true` | Reap blobs a repack fold provably superseded |
| [`OPENSTORAGE_SWEEP_DELETE_LFS`](#openstorage_sweep_delete_lfs) | `false` | Opt-in: actually delete orphan LFS objects (list-only otherwise) |
| [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](#openstorage_sweep_delete_lfs_legacy) | `false` | Post-roll opt-in: admit legacy-spelled LFS keys to the LFS reap (ADR-41 transition gate) |
| [`OPENSTORAGE_REPO_RETENTION`](#openstorage_repo_retention) | unset | Purge repositories soft-deleted longer than this (unset/`0` keeps them forever) |
| [`OPENSTORAGE_LFS_GC_INTERVAL`](#openstorage_lfs_gc_interval) | unset | Unreferenced-LFS-object reclamation cadence (unset/`0` disables the job) |
| [`OPENSTORAGE_LFS_GC_GRACE`](#openstorage_lfs_gc_grace) | `24h` | How long an object must stay observed-unreferenced before retirement |
| [`OPENSTORAGE_LFS_GC_OBJECT_BUDGET`](#openstorage_lfs_gc_object_budget) | `1000000` | Git objects one reclamation run may walk |
| [`OPENSTORAGE_LFS_GC_DELETE_ENABLED`](#openstorage_lfs_gc_delete_enabled) | `false` | Opt-in: actually retire unreferenced LFS registry rows (report-only otherwise) |
| [`OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO`](#openstorage_lfs_max_objects_per_repo) | `100000` | Per-repo LFS object-count cap override |
| [`OPENSTORAGE_ABSENCE_RECONCILE_INTERVAL`](#openstorage_absence_reconcile_interval) | unset | Registry-vs-storage absence reconciliation cadence (unset/`0` disables the job) |
| [`OPENSTORAGE_ABSENCE_RECONCILE_BUDGET`](#openstorage_absence_reconcile_budget) | `100000` | Work units one reconciliation pass may charge |
| [`OPENSTORAGE_REPACK_THRESHOLD`](#openstorage_repack_threshold) | `10` | Registered-pack count that triggers a background repack (`0` disables) |
| [`OPENSTORAGE_SYNC_STEAL_LEGACY`](#openstorage_sync_steal_legacy) | `false` | Opt-in: steal pre-lease sync claims after their adoption grace lapses |

\* Required unless derived from `OPENSTORAGE_DATA_DIR` / `--data-dir` (local mode).

## Storage

### `OPENSTORAGE_PG_URL`

Database URL for the ref authority (CAS) and metadata store.

- **Default:** none. Startup fails unless set or derived from the data dir
  (`sqlite://<dir>/codestorage.db`).
- **Schemes:**
  - `postgres://` or `postgresql://`: production, any number of stateless replicas.
  - `sqlite://<absolute-path>`: local mode, single node.
  - `s3wal://bucket?endpoint=…&region=…`: **experimental** zero-infra mode, single node.
    Refs and metadata live in conditional-CAS objects under the bucket's `_wal/` prefix
    ([ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md)).
- **Legacy aliases:** `SPIKE_PG_URL`, `PG_URL`, scanned left to right. The first *set*
  name wins, even when empty.
- **PostgreSQL URIs** parse like libpq (as of pgx v5.11): `+` in query values is literal
  (encode spaces as `%20`), repeated parameters use the last value, and invalid percent
  escapes are rejected.
- **Postgres schema setup**, including the durable [River](https://riverqueue.com) job
  tables, runs at startup under an advisory lock. Concurrently booting replicas are safe.

**`s3wal://` pairing rule and requirements:**

- [`OPENSTORAGE_S3_URL`](#openstorage_s3_url) must be `s3://`. `file://`, including the
  data-dir derivation, is refused at startup: the bucket must be the *sole* durable
  authority.
- Point both URLs at the **same bucket** (the expected topology). The `packs/`, `lfs/`,
  and `_wal/` prefixes do not overlap; `_wal/` is reserved.
- Credentials come from the standard AWS env chain.
- A startup probe refuses stores that do not implement `If-Match`/`If-None-Match` PUTs
  faithfully.
- The store must also provide strong read-after-write and list-after-write consistency.
  This is not probed; see the
  [store requirements](../how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node).
- Jobs run on the in-process queue and tickers, like local mode.
- All S3WAL targets remain **experimental**. See the
  [storage qualification matrix](../explanation/storage-engine.md#storage-compatibility-and-qualification)
  for release-scoped evidence and conditional future promotion targets.

### `OPENSTORAGE_S3_URL`

Immutable pack storage URL, opened through [gocloud.dev](https://gocloud.dev/howto/blob/).

- **Default:** none. Startup fails unless set or derived from the data dir
  (`file://<dir>/blobs`).
- **Schemes:**
  - `s3://bucket?region=...`: AWS S3. For an S3-compatible store, add
    `endpoint=http://...&s3ForcePathStyle=true`.
    [SeaweedFS](https://github.com/seaweedfs/seaweedfs) is the tested self-hosted
    reference; others, including MinIO, are best-effort.
  - `file://<absolute-dir>`: local mode.
- **Legacy aliases:** `SPIKE_S3_URL`, `BLOB_URL` (same presence rule as
  `OPENSTORAGE_PG_URL`).
- **Credentials:** the standard AWS chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  shared config, instance roles).
- With an `s3wal://` database URL this **must** be `s3://`, and the same bucket is the
  expected target ([pairing rule](#openstorage_pg_url)).
- Delete-mode sweeping interacts with bucket versioning; see
  [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](#openstorage_sweep_delete_enabled).

### `OPENSTORAGE_S3WAL_LEASE_TTL`

TTL of the s3wal `_wal/instance-lease` dual-serve guard
([ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md)
§2.11). Ignored outside s3wal mode.

- **Default:** unset = 2 minutes (30-second heartbeats).
- **Values:** a [duration](https://pkg.go.dev/time#ParseDuration) of at least `1s` (the
  TTL/4 heartbeat must tolerate ordinary S3 latency jitter), or exactly `0`.
- **`0` disables the lease.** It exists so test suites can run several processes against
  one bucket. `serve` warns loudly, and accidental dual-serve topologies go undetected.
- **Behavior:**
  - At startup, `serve` refuses to run while another process holds a fresh lease on the
    bucket.
  - While running, it heartbeats every TTL/4 and **fail-stops** if the lease is stolen or
    unrenewable.
  - Graceful shutdown (`SIGTERM`/`SIGINT`) attempts a bounded, best-effort release.
    Failed release, hard kills, and fail-stop exits can leave a lease.
  - A successor must respect the **holder's recorded TTL** plus store-clock resolution
    margin, not just its own configured TTL.

> [!WARNING]
> The lease is best-effort *detection* with fail-stop on heartbeat loss. It is not an
> independent integrity guarantee or support for two servers on one bucket. Do not
> disable it or delete the lease by hand to recover. Follow the
> [shutdown/restart procedure](../how-to/deploy-production.md#s3wal-shutdown-and-restart)
> and [operational signals](../how-to/deploy-production.md#s3wal-operational-signals).

### `OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`

Optional [bucket owner
condition](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-owner-condition.html):
the 12-digit AWS account ID that must own the `OPENSTORAGE_S3_URL` bucket. AWS bucket
names are global and a deleted name can be re-registered by anyone; without this, the
blob layer trusts whatever the name resolves to.

- **Default:** unset. No condition is sent.
- **Format:** exactly 12 digits. Anything else fails boot.
- **Effect:** every S3 request carries `x-amz-expected-bucket-owner`, so requests to a
  re-owned bucket fail with `403 Access Denied`. This covers:
  - the shared blob client: gocloud data plane, multipart copies, startup probe, sweeps,
    git-hook subprocesses, and the purge CLI;
  - the `s3wal://` conditional client (its own all-or-nothing middleware): every `_wal/`
    CAS request, including the boot probe and the instance lease. These ref and metadata
    objects include the org's public keys.
- **Startup:** every accepted `CreateBucket` outcome (success, `BucketAlreadyOwnedByYou`,
  `BucketAlreadyExists`, or a least-privilege role's tolerated `AccessDenied`) is followed
  by a `HeadBucket` under the condition. A squatted name and credentials from a different
  account both fail boot. (`CreateBucket` itself cannot carry the condition.)
- **Presigned URLs** (LFS downloads) embed it as a signed lowercase
  `x-amz-expected-bucket-owner` query parameter (the spelling S3 enforces there). Clients
  send no extra header; altering the parameter breaks the SigV4 signature.
- **AWS only.** SeaweedFS, MinIO, and S3 emulators ignore the condition (the parameter
  passes through harmlessly).
- This is defense-in-depth. The primary control is the `aws:ResourceAccount` IAM
  condition in [Deploy to production](../how-to/deploy-production.md).

### `OPENSTORAGE_S3_OP_LOG`

Diagnostics: set to a file path to append one line per S3 HTTP round trip. Covers the blob
client and the `s3wal://` conditional client, in `serve` and in git-hook subprocesses.

```
<unixnano> <pid> <role> <operation> <status> <duration_us> <request-path>
```

- **Default:** unset. No middleware; zero overhead.
- **The path must be absolute.** Hooks run from the cache repository, so a relative path
  would split the log across files; it is rejected with a stderr warning.
- An unopenable path logs a stderr warning and disables the log. It never fails the
  process.
- `role` comes from argv (`openstorage:serve`, `openstorage:hook:pre-receive`, …), so a
  shared log attributes each round trip to its process.
- Use it to count the S3 round trips (WAN round trips on real S3) behind a request.
- Writes are `O_APPEND` one-line syscalls, safe for concurrent processes.
- The log grows without bound and includes object keys. Use it temporarily, like a debug
  log.

### `OPENSTORAGE_DATA_DIR`

Local-mode data directory. The `--data-dir` flag wins over the variable.

- **Derives** every storage slot not set explicitly:
  - database: `sqlite://<dir>/codestorage.db`
  - packs: `file://<dir>/blobs`
  - cache: `<dir>/cache`
- Explicit `OPENSTORAGE_PG_URL` / `OPENSTORAGE_S3_URL` / `--cache-dir` always win.
- The path is made absolute at startup (git hooks re-exec the binary from another
  directory).
- The `codestorage.db` filename is kept for on-disk compatibility: the binary reopens the
  same database with no rename or migration. See the
  [migration guide](../how-to/migrate-to-openstorage.md#3-keep-persistent-data-locations-unchanged).
- Local mode is single-node by design; see ADR-13 in the
  [local-backends notes](../archive/phases/11-local-backends-implementation-notes.md).

## Server identity and network

### `OPENSTORAGE_LISTEN`

Address of the HTTP listener for the REST API, git smart-HTTP, and `/healthz`.
It never serves metrics; see [`OPENSTORAGE_METRICS_LISTEN`](#openstorage_metrics_listen).

- **Default:** `127.0.0.1:0`, an ephemeral port printed as `listening on ...`.
- The `--listen` flag (legacy alias `--addr`) wins.

### `OPENSTORAGE_METRICS_LISTEN`

Address of the opt-in Prometheus metrics listener ([metrics reference](metrics.md),
[ADR-47](../adr/047-metrics-ride-an-opt-in-separate-listener-with-fixed-labels.md)).

- **Default:** unset or empty, which means no metrics listener.
- **Format:** `host:port`, like `127.0.0.1:9464`. `:0` picks a free port, printed as
  `metrics listening on ...`.
- **Validation:** the address is bound at startup before anything is served. An invalid,
  unresolvable, or busy address, including the API listener's own address, stops `serve`
  with exit 1. The value is not trimmed, so a whitespace-only value fails too.
- **Serves:** only `GET /metrics` on its own socket. It shares nothing with the API
  listener.

> [!WARNING]
> The metrics listener has **no authentication**. Bind it to loopback or a private
> network that only your scraper can reach. `serve` warns when the address binds every
> interface, but the warning is not access control.

### `OPENSTORAGE_ORG`

**Required.** Organization name: the JWT `iss` claim every token must carry
([auth spec](authentication.md)).

- `serve` always runs single-org mode: REST and git smart-HTTP share one org-key verifier.
- The org row is bootstrapped from this value at startup.

### `OPENSTORAGE_ORG_PUBKEYS`

**Required.** Path to a PEM file with one or more PKIX public key blocks that verify the
org's JWTs.

- Generate a keypair with [`openstorage keygen`](cli.md#openstorage-keygen).
- **Allowed JWS algorithms:** `ES256`, `RS256`, `EdDSA`, `PS256`, `ES384`, `ES512`. `none`
  and `HS*` are always rejected.
- **Validation:** each block that PEM-decodes must be a `PUBLIC KEY` block of a supported
  family.
- **Caveat:** parsing stops silently at the first undecodable data, so a truncated block
  after a valid key is dropped, not rejected. After a rotation, check that a token signed
  with the new key is accepted; startup success does not prove it.

### `OPENSTORAGE_BASE_URL`

Externally reachable base URL for `http_url` values in API responses.

- **Default:** derived from the bound listener. That is unroutable when binding `0.0.0.0`,
  so set this in any real deployment or behind a reverse proxy.
- **Format:** `http(s)://host[:port]`, no query or fragment. A trailing slash is stripped.

## Rate limiting

### `OPENSTORAGE_RATE_RPS` / `OPENSTORAGE_RATE_BURST`

Per-client-IP token-bucket rate limit for the REST surface **and** (with separate bucket
state) the git smart-HTTP surface. Excess requests get `429` with `Retry-After` plus the
IETF `RateLimit` header (`"default";r=0;t=<retry-after-secs>`), the pair documented
upstream on every 429.

- **Default:** both unset; limiting is disabled.
- **Values** (fail-fast at startup):
  - `RPS`: a non-negative finite number. `BURST`: a non-negative integer.
  - `RPS > 0` requires `BURST >= 1`, and vice versa. Half-set pairs are startup errors.
  - `RPS=0` with `BURST` unset or `0` explicitly disables.
- **Git minimum burst:** the git surface raises depths below 8 to 8. One git operation is
  several requests; a smaller burst would make every clone/fetch/push permanently fail.
- **Client identity** is the TCP `RemoteAddr`. Forwarded headers are **not** trusted
  unless `OPENSTORAGE_TRUSTED_PROXIES` is set; otherwise, behind a proxy that pools
  backend connections, all clients share one bucket.

### `OPENSTORAGE_TRUSTED_PROXIES`

Comma-separated CIDR allowlist of reverse proxies whose forwarded client identity the rate
limiters may trust. Bare IPs count as single-address entries.

- **Default:** unset; buckets key on `RemoteAddr` only.
- **Keying:** when the TCP peer is allowlisted, both limiters key on the **rightmost
  `X-Forwarded-For` hop that is not itself allowlisted**. Otherwise (including malformed
  `X-Forwarded-For`) they use `RemoteAddr`.
- **Validation:** at startup, even while rate limiting is off. Rejects empty entries,
  zone-scoped addresses, and IPv4-mapped prefixes below `/96`. IPv4-mapped entries are
  normalized to IPv4.
- **List only proxies that set or append `X-Forwarded-For` themselves.** A proxy that
  forwards client-supplied values verbatim lets any client mint fresh buckets.

## Egress policy

### `OPENSTORAGE_SYNC_ALLOW_PRIVATE`

Unified egress policy for upstream sync in **both** directions (ADR-45).

- **Default:** unset (transitional; see below).
- **Values:**
  - `true`: both directions may target internal forges (RFC 1918, loopback, ULA,
    link-local). Cloud-metadata and special-purpose ranges stay denied.
  - `false`: strict both ways; only public upstreams are dialable.
  - **Unset:** pushes stay strict (the deprecated
    [`OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE`](#openstorage_sync_push_allow_private) may relax
    them); pulls ALLOW private upstreams, and `serve` logs a boot warning.
  - Malformed booleans are startup errors.
- **Enforcement:** every http(s) sync transfer (`upstream_host` is caller-linked data)
  dials through a vetting proxy with the webhook destination policy: private, loopback,
  link-local, metadata, and special-purpose targets are denied, with every DNS answer
  re-vetted at connect time.
- **Covered:** pulls (`POST /repos/pull-upstream`, the first sync at `POST /repos`,
  freshness pokes) and push forwards (`POST /repos/push-upstream`, `forward_pushes`).
- **Denied destination:** the upstream is never contacted. Pulls fail as
  `repository configuration error`; pushes fail as `invalid sync configuration`, with no
  retry.
- **Registration** (`POST /repos` with `base_repo`) also runs a courtesy check against the
  pull policy. Dial-time enforcement stays authoritative.

> [!IMPORTANT]
> A future release flips the unset pull default to **deny**. Set this explicitly now:
> `true` keeps internal-forge pulls working across the flip; `false` adopts the strict
> policy early.

**Migration note (0.7.x):** pulls previously had no destination policy. They now always
use the vetting proxy, which dials directly, so `HTTP_PROXY`/`HTTPS_PROXY` no longer apply
to upstream fetches (same as webhooks and push forwarding). If you require proxied egress,
enforce an equivalent destination policy at that proxy; the guard cannot be chained
through an external proxy.

### `OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE`

**Deprecated** push-only spelling of the sync egress policy, kept for 0.6.x deployments.

- **Default:** `false`.
- If [`OPENSTORAGE_SYNC_ALLOW_PRIVATE`](#openstorage_sync_allow_private) is unset, `true`
  relaxes the private receiver classes for push forwards only.
- If the unified variable is set, this one is ignored.
- Either way, setting it logs a boot deprecation notice naming the replacement.

### `OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE`

Controls the native webhook SSRF guard.

- **Default:** `false`. Delivery rejects IANA special-purpose ranges, including loopback,
  RFC 1918, RFC 6598 shared, IPv4/IPv6 link-local, IPv6 ULA, translation/protocol
  anycast, documentation, benchmarking, reserved, unspecified, multicast, broadcast, and
  cloud-metadata targets. IPv4-mapped IPv6 is normalized first, so `::ffff:10.0.0.1` is
  private.
- **`true`** (Go boolean; logs a startup warning): admits loopback, RFC 1918, RFC 6598
  shared, link-local, and ULA receivers. Use it only for intentionally internal webhooks.
  It **still blocks** cloud metadata, unspecified/multicast/broadcast, other IANA
  special-purpose ranges (documentation, benchmarking, reserved, translation,
  protocol-anycast), and redirects.
- Malformed booleans are startup errors.

How the guard is enforced:

- **Registration** checks IP literals and the DNS answers visible at that moment, as fast
  feedback only. **Acceptance is not a reachability guarantee:** DNS may fail or change
  later.
- **Every delivery attempt** re-resolves at dial time. All addresses must be allowed. A
  resolution failure, empty answer, or mixed public/private answer fails the attempt
  through the normal retry/dead-letter path.
- The client connects to the vetted IP literal, so DNS cannot be rebound in between.
- **No redirects:** `301`, `302`, `307`, and `308` are delivery failures; the target gets
  no request.
- Delivery ignores `HTTP_PROXY` / `HTTPS_PROXY`, since vetting a proxy socket would not
  vet the proxy's final target.

## Secrets at rest

### `OPENSTORAGE_SECRETS_KEY`

Enables envelope encryption of the metadata database's secret columns: webhook HMAC
signing secrets (`webhooks.secret`) and upstream-sync credential passwords
(`git_credentials.password`).

- **Default:** unset. **Those columns are stored plaintext.** Either way, follow the
  [production guide](../how-to/deploy-production.md#backing-services): scope database
  access tightly and encrypt the database and its backups.
- **Format:** comma-separated base64-encoded 32-byte (AES-256) keys. The **first key
  encrypts every write**; the rest are decrypt-only. Generate a key with:

```sh
head -c 32 /dev/urandom | base64
```

- **Validation:** a malformed list (bad base64, wrong length, duplicate or empty entries)
  is a startup error.
- **Set the same value on every replica.** Hook subprocesses inherit it from `serve`.
- **Startup log:** `secrets at-rest encryption enabled`, with mode, key count, and active
  fingerprint. Key material never appears in logs or errors.
- **Stored format:** `enc:v1:<keyid>:<base64 nonce+ciphertext>` (AES-256-GCM). `<keyid>`
  is the first 8 hex characters of the key's SHA-256 fingerprint: public, safe to log,
  and usable in SQL to audit rotation.
- **Existing plaintext rows** stay readable and are re-encrypted on their next read
  (webhook deliveries, credential use, list/get API calls). A legacy plaintext value that
  happens to start with `enc:` reads verbatim and migrates too.
- **Reserved prefix:** the API rejects new webhook secrets and credential passwords
  starting with `enc:` (HTTP 400), in plaintext mode too.
- **Missing key:** reading a row without the key that wrote it is a request-time error
  naming the row and key fingerprint, never a silent passthrough.
- **Recovery never needs the lost key.** An undecryptable webhook is listed without its
  secret and only its deliveries fail. Replacing (`PUT`) or deleting a credential, and
  deleting and re-registering a webhook, work on undecryptable rows.

> [!WARNING]
> - Removing the variable while encrypted rows exist makes them unreadable. Keep the key
>   list stable once enabled, and back the keys up outside the database.
> - Binaries that predate this variable read `enc:v1:` envelopes verbatim: they sign
>   webhooks with the ciphertext or send it as an upstream password, failing silently.
>   Upgrade every node before setting the key anywhere, and do not roll back to such a
>   binary while encrypted rows exist.

**Enable on a multi-replica fleet.** Stage the rollout so no envelope exists before every
process can decrypt it. A single replica (e.g. local mode) can skip to step 3.

1. **Binary first.** Deploy an envelope-aware binary to *every* node (serve replicas and
   hook-running git nodes) with `OPENSTORAGE_SECRETS_KEY` unset.
2. **Stage the key.** Set `OPENSTORAGE_SECRETS_KEY` and
   [`OPENSTORAGE_SECRETS_MODE=decrypt-only`](#openstorage_secrets_mode) fleet-wide.
   Processes can decrypt but still write plaintext and rewrite nothing.
3. **Encrypt.** Once every replica has the key, drop `OPENSTORAGE_SECRETS_MODE` (or set
   `encrypt`) fleet-wide. Encrypted writes and lazy re-encryption begin.

A drained cutover (stop all replicas, set the key, start) collapses steps 2–3.

**Rotate a key.** A single replica can skip step 1 (one restart swaps the list).

1. **Stage** fleet-wide: `OPENSTORAGE_SECRETS_KEY="<old-key>,<new-key>"`. Writes still
   use the old key; rollout order does not matter.
2. **Promote** fleet-wide: `OPENSTORAGE_SECRETS_KEY="<new-key>,<old-key>"`.
3. **Re-encrypt, verify, then drop the old key.** Each read re-encrypts a row under the
   new key. To avoid waiting, touch every row: list the org's webhooks
   (`GET /api/v1/webhooks`) and, per repo with a stored credential, trigger a sync
   (`POST /api/v1/repos/pull-upstream`) or re-`PUT` the credential. Check progress:

```sql
SELECT count(*) FROM webhooks        WHERE secret   NOT LIKE 'enc:v1:<new-keyid>:%';
SELECT count(*) FROM git_credentials WHERE password NOT LIKE 'enc:v1:<new-keyid>:%';
```

When both counts are zero, remove the old key from the list.

### `OPENSTORAGE_SECRETS_MODE`

Staging switch for [`OPENSTORAGE_SECRETS_KEY`](#openstorage_secrets_key).

- **Default:** `encrypt` (normal operation).
- **`decrypt-only`:** loads the keys for reading but writes plaintext and does no lazy
  rewrites. Use it only while a rolling restart distributes the key; it stores new
  secrets plaintext.
- **Validation:** `decrypt-only` without `OPENSTORAGE_SECRETS_KEY`, or any other value, is
  a startup error.

## Pack quota

### `OPENSTORAGE_PACK_QUOTA_BYTES`

Per-repository storage quota (packs + LFS), enforced at every external storage writer.

- **Default:** 10 GiB (`10737418240`). **`0` disables enforcement**; negative or malformed
  values are startup errors.
- **Where it is enforced:**
  - Git pushes that would grow `packs/` past the quota: refused at pre-receive, before
    anything is stored.
  - REST pack writes (commit-pack, merge, notes, fork): `403` envelope.
  - Git LFS uploads: per-object `507` batch errors before any transfer URL is issued. The
    verified upload re-checks the actual `Content-Length`, so under-declared sizes cannot
    bypass it.
- **Namespaces:** a repo and its `+ephemeral` / `+import` namespaces
  ([git protocol](../explanation/git-protocol-and-namespaces.md)) share the base repo's
  quota. LFS objects always store under the base repo's prefix.
- **Admission** uses a durable reservation ledger in the ref store, atomic across pushes
  and replicas: of two pushes that fit alone but not together, exactly one wins. LFS
  charges it too (advisory batch check, then a durable reserve at the verified upload, settled when
  the upload completes).
- **Population bounds** (compiled in, like the repack fold budgets; apply even with the
  byte quota off):
  - 10,000 **active reservation rows** per repo. Excess batches get transient `429`s until
    rows settle or expire. Admission prunes the repo's expired rows itself, so the table
    stays bounded even with sweeping off.
  - 100,000 **registered LFS objects** per repo. Excess gets `507` naming the limit;
    re-publishing registered objects is exempt.
- **Never quota-gated:** background repack (so an at-quota repo can still compact) and LFS
  downloads. Repack keeps the registered object set; it does not reclaim unreachable
  objects.

### `OPENSTORAGE_LFS_STAGING_BYTES`

Per-replica cap on the total bytes of in-flight LFS uploads. Uploads (the verified proxy
transfer) stage their full body under `$TMPDIR` before verification and publication. This
bound protects the replica's disk, which the per-repo quota does not.

- **Default:** 32 GiB (`34359738368`). **`0` disables the bound**; negative or malformed
  values are startup errors.
- **A working set, not an object-size cap.** One repo can hold at most a third of the
  budget, so objects of size `S` need a budget of about `3 × S`. The default fits one
  10 GiB (default-quota-sized) object with cross-repo capacity left.
- **Sizing:** keep it below the free space guaranteed in `$TMPDIR`, minus headroom for git
  push quarantines and fork closures, which stage there too.
- **Charging:** bytes are charged as they arrive, so a stalled upload holds little more
  than it has transferred, not its declared `Content-Length`. Each repo is held to a fair
  share that scales with free capacity.
- **Responses:**
  - Does not fit right now: `429` with `Retry-After` + `RateLimit`.
  - Declared size above a third of the budget: `413`.
  - A second concurrent upload of the same object on one replica: `429`.
- **Aborts:** an upload is aborted, freeing capacity, after a full minute with no bytes
  **or** when it outlives the [reservation TTL](#openstorage_pack_reservation_ttl). An
  upload that slow (~3 MB/s for 10 GiB at defaults) could not beat its reservation's
  expiry anyway.

### `OPENSTORAGE_LFS_V020_WRITE_COMPAT`

Rolling-upgrade bridge from v0.2.0. Registered LFS objects live at a canonical key
([ADR-41](../adr/041-registered-lfs-objects-live-at-a-canonical-key-legacy-presigned-urls-cannot-address.md))
that old presigned upload URLs cannot address and v0.2.0-and-older replicas cannot see.
Set this on **upgraded** replicas to also copy every verified LFS publication to the
legacy key, so old replicas keep serving objects during the roll.

- **Default:** `false`. The canonical spelling is the secure one; compat copies are exposed
  to still-valid legacy upload URLs, like everything v0.2.0 served. Keep it temporary.
- **Enforced:** if the compat copy fails, the upload answers `502`. The canonical object is
  already registered, so the retry is zero-growth and repairs the copy. Write-scoped
  touches of registered objects also repair a missing legacy copy.
- **Quota:** with a [quota](#openstorage_pack_quota_bytes) enforced, the copy is charged
  normally, so each fresh upload costs about twice its size during the roll. If the copy
  would exceed the quota, the upload answers `507`; the canonical object stays registered,
  and a retry after freeing space repairs the copy.
- **After the roll:** drop the variable once the whole fleet is upgraded. The rowless
  copies are reclaimed by the [orphan sweep](../how-to/storage-lifecycle-and-sweeping.md)
  under its LFS opt-in.

> [!WARNING]
> Destructive LFS sweeping must be off **fleet-wide** during a v0.2.0 roll: an old
> binary's sweeper has no legacy gate and would reap migrated objects' legacy blobs,
> breaking old-replica reads.
> - Combining this flag with [`OPENSTORAGE_SWEEP_DELETE_LFS`](#openstorage_sweep_delete_lfs)
>   while a sweep schedule is active (`OPENSTORAGE_SWEEP_INTERVAL` > 0) is a startup
>   error. With the interval at `0`, no schedule runs, the delete flag is inert, and the
>   fleet-wide emergency stop stays available.
> - That check runs only on upgraded nodes. On pre-0.15 nodes, set
>   `CODESTORAGE_SWEEP_DELETE_LFS=false` yourself before rolling; they do not read the new
>   prefix.

### `OPENSTORAGE_PACK_RESERVATION_TTL`

How long a crashed writer's quota reservation keeps gating admission.

- **Default:** `1h`.
- **Values:** a Go duration (e.g. `30m`, `2h`) greater than 60 seconds (the 30-second
  usage-floor cache lifetime plus the 30-second admission handoff margin). Smaller values
  fail startup; they could silently reopen quota overshoot.
- **Sizing:** comfortably exceed the longest server-side pack upload. The default covers a
  10 GiB upload at ~3 MB/s.
- **LFS** reservations use the same TTL. They are charged at the verified upload, just
  before publish (the batch check holds nothing), so the TTL must cover publication and
  registration, not the client's transfer.
- Admission ignores expired rows immediately; the sweeper's pruning is only garbage
  collection.

## Push work bounds

### `OPENSTORAGE_MAX_PUSH_BYTES`

Cap on one git push's decoded body size. A `git-receive-pack` body over the cap, counted
after any `Content-Encoding: gzip` decode, is rejected with `413` and a JSON error
envelope.

- **Default:** 2 × [`OPENSTORAGE_PACK_QUOTA_BYTES`](#openstorage_pack_quota_bytes), at
  least 2 MiB. The floor keeps delete-only/zero-growth pushes (the over-quota recovery
  path) working under byte-scale quotas.
- **Values:** **`0` disables the cap**; negative or malformed values are startup errors.
  An explicit value is used verbatim (no floor).
- Disabling the quota (`0`) also disables the derived cap. To keep a cap without a quota,
  set this explicitly.
- **Scope:** git pushes only, including the sync worker's loopback pushes. Fetches,
  clones (`upload-pack`), and LFS transfers are unaffected.
- **Bounded work:** only bytes up to the cap reach `git receive-pack`; a declared oversize
  is rejected unread. Git discards the truncated quarantine, and nothing is published or
  registered. (The pack quota
  bounds *retained* storage, after the push has streamed to disk.)
- **Gzip bodies** are also capped on the **compressed** side at the cap + ~6% + 4 KiB.
  Git clients' single-member streams never approach it. The 413 envelope names both
  bounds.
- **Edge case:** a *complete, valid* push padded with junk past the cap still gets `413`,
  but its hooks already ran under normal quota admission.

### `OPENSTORAGE_VERIFY_SIG_BUDGET`

Per-push cap on `verify-sig` signature verifications: how many commits one push may verify
across all refs, pre-receive evaluation and update hooks combined. Each verification spawns
a `git verify-commit` subprocess.

- **Default:** `10000`. **`0` disables the budget** and the walk allowance below; negative
  or malformed values are startup errors.
- **Over budget:** the ref update is rejected fail-closed **before** the excess
  verifications run, with an error naming this variable. Normal per-ref semantics apply:
  a plain multi-ref push denies only over-budget refs, while `git push --atomic` rejects
  the whole push.
- **Charging:** once per unique commit verified. Verdicts are cached and ranges are
  deduplicated across refs within a push.
- **Worst case:** branch creation verifies **everything reachable** from the new tip (per
  the branch-protection contract, it compares against the ref's own prior tip, never other
  refs). Raise the budget before importing histories deeper than it under a `verify-sig`
  policy.
- **Walk allowance:** 100 × the budget per push. Every commit a walk examines counts
  (cached verdicts included), plus a small fee per ref evaluation for the verification
  state it loads.

## Maintenance jobs

Background jobs run through a queue seam: durable [River](https://riverqueue.com) jobs on
Postgres, in-process in local mode. For procedures and run reports, see
[Operate the storage lifecycle](../how-to/storage-lifecycle-and-sweeping.md).

### `OPENSTORAGE_SWEEP_INTERVAL`

Interval between orphan sweeps. The sweeper diffs the blob store's `packs/` and `lfs/`
listings against their registries and reports unregistered blobs older than the age
threshold. It deletes them only when explicitly enabled. It also prunes expired quota
reservations on the same cadence.

- **Default:** `1h`. **Values:** a Go duration; negative values are startup errors.
- **`0` disables sweeping.** Two things still run:
  - a strictly restorative quarantine-heal pass, once at startup;
  - an hourly janitor that reconciles legacy/canonical LFS registry-row twins that
    mixed-version rollouts can strand. Without it, a duplicate row would double-charge
    usage and the object cap indefinitely.

### `OPENSTORAGE_SWEEP_AGE_THRESHOLD`

Minimum blob age before an unregistered pack counts as an orphan.

- **Default:** `24h`. **Values:** a positive Go duration.
- This is the only concurrency guard needed: in-flight pushes register their packs within
  a request timeout, far below any sane threshold.

### `OPENSTORAGE_SWEEP_DELETE_ENABLED`

Opt-in delete mode for **general** orphans.

- **Default:** `false`: list-only. Candidates are logged; nothing is deleted.
  **Values:** a boolean.
- **Quarantine first:** each deleted blob is copied to a quarantine key first. Every sweep
  starts with a heal pass that restores registered-but-missing packs from quarantine, so
  rolling back to list-only still repairs a delete-enabled crash.
- **Fleet prerequisite:** enable only when **every** node (serve replicas *and*
  hook-running git nodes) runs a build with the post-registration publish verify
  (`blob.EnsurePublished`). On a mixed fleet no data is lost, but an old writer's new pack
  can be unavailable until the next sweep heals it. List-only is safe on any fleet.
- **S3 versioning:** on a bucket with versioning enabled or suspended, deletes only write
  delete markers; every version stays stored and billed. Startup **fails** when delete
  mode meets such a bucket, or when versioning cannot be verified (grant
  `s3:GetBucketVersioning` or stay list-only). Each delete-mode sweep re-checks and
  downgrades itself to list-only on drift.

### `OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`

Delete mode for the narrow **superseded-by-repack** class: blobs whose registry rows a
repack fold provably removed (durable `pack_retained` records). They are reaped after the
age threshold, and their bytes return to the repo's quota headroom.

- **Default:** `true` (`false` disables). **Values:** a boolean.
- **Versioned S3 bucket:** does not fail startup, unlike the general opt-in. Each sweep
  run downgrades to list-only instead; the heal pass still runs.

### `OPENSTORAGE_SWEEP_DELETE_LFS`

Delete mode for **LFS orphans**: `lfs/` blobs with no LFS registry row, older than the age
threshold.

- **Default:** `false` (list-only). **Values:** a boolean.
- **Separate from `OPENSTORAGE_SWEEP_DELETE_ENABLED` on purpose:** every LFS object
  uploaded by a build older than the LFS registry
  ([#51](https://github.com/coder/openstorage/issues/51)) has no row and would look
  orphaned. Write-scoped touches (upload batches naming an object, re-uploads of the same
  content) adopt stored objects into the registry.
- Enable only after list-only
  [sweep reports](../how-to/storage-lifecycle-and-sweeping.md#lfs-orphans-and-pre-registry-objects)
  stop naming objects that should live.
- Same quarantine-first disposal and heal pass as the pack classes. On versioned S3
  buckets it fails startup like the general opt-in.
- Legacy-spelled keys (`lfs/<repo_id>/<oid>`, ADR-41) are **excluded** until
  [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](#openstorage_sweep_delete_lfs_legacy) is also set.

### `OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`

Post-roll opt-in to delete **legacy-spelled** LFS keys (`lfs/<repo_id>/<oid>`, the
v0.2.0-and-older spelling,
[ADR-41](../adr/041-registered-lfs-objects-live-at-a-canonical-key-legacy-presigned-urls-cannot-address.md)).

- **Default:** `false`: legacy keys are never deleted. **Values:** a boolean; only
  meaningful with `OPENSTORAGE_SWEEP_DELETE_LFS`.
- **Why:** adoption migrates objects to canonical keys but **keeps** the legacy source for
  old replicas and still-valid presigned GETs. During a roll, those sources are rowless
  orphans that `OPENSTORAGE_SWEEP_DELETE_LFS` alone must not touch.
- **Set it only when the transition is over:** every replica runs the new binary, and the
  last old replica exited more than 15 minutes ago (the presign expiry).
- This gate overrides every other flag: without it, no sweep mode deletes a
  legacy-spelled key.
- Same quarantine-first disposal as every destructive class.

### `OPENSTORAGE_REPO_RETENTION`

Retention window for soft-deleted repositories. Once a repo has been deleted longer than
this, a purge pass on the sweep cadence **permanently** removes its registry and eventing
rows (refs, packs, LFS objects, credentials, upstream link) and deletes its blobs
quarantine-first. Only the repository row remains, as a purge tombstone.
[`openstorage purge-repo`](cli.md#openstorage-purge-repo) runs the same purge
synchronously for one repo.

- **Default:** unset (`0`): soft-deleted repos are kept forever.
- **Values:** a Go duration of at least `1h`; shorter values are startup errors. The purge
  always waits at least an hour after deletion, so no in-flight pre-delete push can race
  it.
- **Requires sweeping:** [`OPENSTORAGE_SWEEP_INTERVAL`](#openstorage_sweep_interval) > 0.
  The purge rides the sweep schedule.
- **Fleet prerequisite** (also for `openstorage purge-repo`): enable only when **every**
  node (serve replicas *and* hook-running git nodes) runs a build with the purge write
  fence (ref updates and pack/LFS registrations reject purged repos). Otherwise an old
  node can accept a write that is stranded behind the purge while reporting success.
  Leaving retention unset is safe on any fleet.
- **Write fence:** each purge fences the repo before touching anything, so a still
  in-flight pre-delete push fails instead of racing the reclamation.
- **Blob bytes** move to quarantine first and are freed when the backups age out. Sweeps
  are purge-aware even with every `*_DELETE_*` flag off, and independent of this variable,
  so `purge-repo`-minted fences get the same cleanup. On versioned S3 buckets it fails
  startup like the delete opt-ins.

> [!WARNING]
> **Purged repositories cannot be restored.** The tombstone never releases the `(org, url)`
> name: JWT repo claims are URL-scoped and can outlive any retention window, so reusing
> the name would let a stale token authorize against the new repository.

### `OPENSTORAGE_LFS_GC_INTERVAL`

Cadence of the unreferenced-LFS-object reclamation job
([ADR-42](../adr/042-lfs-reclamation-is-mark-and-sweep-over-reachable-pointer-files.md)).
Registered LFS objects are otherwise permanent, so a `git:write` holder direct-PUTting
tiny unreferenced objects can exhaust the per-repo object cap for everyone (`507` on
every new object).

- **Default:** unset (`0`): the job never runs. `0` is also the fleet-wide emergency stop.
- **Values:** a non-negative Go duration.
- **Each run** walks reachable pointer files (bounded, resumable) and stamps unreferenced
  rows. With [`OPENSTORAGE_LFS_GC_DELETE_ENABLED`](#openstorage_lfs_gc_delete_enabled), it
  retires rows whose stamp outlived the [grace window](#openstorage_lfs_gc_grace),
  refunding object count and quota bytes. See the
  [operations guide](../how-to/storage-lifecycle-and-sweeping.md#reclaiming-unreferenced-lfs-objects).
- Retirement removes registry rows only; the
  [sweeper's LFS delete opt-in](#openstorage_sweep_delete_lfs) reaps the blobs. Arming
  retirement therefore [requires that opt-in](#openstorage_lfs_gc_delete_enabled).

### `OPENSTORAGE_LFS_GC_GRACE`

How long an LFS object must stay **continuously** unreferenced before reclamation may
retire its registry row.

- **Default:** `24h`. **Values:** a positive Go duration.
- **Clock:** starts when a scan first finds no reachable pointer to the object. Resets on
  any liveness signal: a pointer becoming reachable, a re-upload, or a batch/PUT answering
  "already stored". Retirement re-checks the grace under the registration lock, so no
  signal loses the race.
- **Sizing:** keep it well above your longest expected upload→push gap. Pointers pushed
  through the normal git-lfs flow always touch the object first; raw-git pointer pushes
  get only the reachability scan's protection.
- **With `OPENSTORAGE_LFS_GC_DELETE_ENABLED`:** must be at least the physical reap cadence,
  max([`OPENSTORAGE_SWEEP_AGE_THRESHOLD`](#openstorage_sweep_age_threshold),
  [`OPENSTORAGE_SWEEP_INTERVAL`](#openstorage_sweep_interval), the 1-hour quarantine
  retention floor), or startup fails.
- **Granularity:** local mode (SQLite) stamps whole seconds, so the effective window
  rounds up (never down) and may run up to two seconds longer. It is never shorter than
  configured on either backend.

### `OPENSTORAGE_LFS_GC_OBJECT_BUDGET`

Caps the work units one reclamation run may spend, so its cost stays predictable on any
repository shape.

- **Default:** `1000000`. **Values:** a positive integer.
- **Charges:**
  - each git object a reachability walk lists: one unit, plus one per 4 KiB of its raw
    (inflated) size, so entry-stuffed trees and large blobs pay for what git expands;
  - each ref a scanned namespace holds, and each registry row settlement examines: one
    unit;
  - each attempted repository, including skips: at least one unit.
- A repo whose walk exceeds the remaining budget is retried first next run with a full
  budget. One that exceeds the **full** budget is skipped loudly until you raise it.
- Nothing is marked or retired from a partial walk.

### `OPENSTORAGE_LFS_GC_DELETE_ENABLED`

Arms retirement. When off, the job still scans, stamps, and reports what it *would* retire
(`rows_eligible` in the run report). Roll out like the
[sweep delete flags](#openstorage_sweep_delete_enabled): watch report-only runs until the
eligible rows are the junk you expect, then opt in.

- **Default:** `false`. **Values:** a boolean.
- **Requires** [`OPENSTORAGE_LFS_GC_INTERVAL`](#openstorage_lfs_gc_interval) > 0. Arming a
  disabled job is a startup error.
- **Requires, while the sweep schedule is live** (security; startup fails otherwise):
  [`OPENSTORAGE_SWEEP_DELETE_LFS`](#openstorage_sweep_delete_lfs), and a
  [grace](#openstorage_lfs_gc_grace) of at least the reap cadence. Retirement refunds the
  cap slot, but only that reap deletes the blob; decoupled, re-upload cycles would grow
  `lfs/<repo>/` without bound.
- **Runtime check:** each destructive LFS sweep pass that completes its keyspace with zero
  errors records a durable fleet-wide watermark. Refunds happen only while that watermark
  is fresher than the grace window. Failing or lagging sweeps and mid-flight guard
  demotions freeze refunds (`reap_watermark_stale` /
  `rows_deferred_reap_stale`). A fresh deployment stays report-only until its first clean
  destructive LFS sweep.
- **Legacy-spelled rows** (pre-ADR-41 `lfs/<repo>/<oid>` keys) retire only while
  [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](#openstorage_sweep_delete_lfs_legacy) is set
  **and** the fresh watermark records a legacy-armed sweep. Otherwise they defer
  (`rows_deferred_legacy_unarmed`).
- **Sweep stopped** ([`OPENSTORAGE_SWEEP_INTERVAL=0`](#openstorage_sweep_interval), the
  emergency stop): the process starts but demotes retirement to report-only with a
  warning, since nothing would reap the blobs.
- **Mixed-fleet prerequisite:** arm only when every replica serving LFS runs a build with
  this feature (older replicas answer "already stored" without clearing the stamp). If
  report-only scans ran during the roll, also wait one full grace after the last
  pre-touch replica stops. See the
  [rollout guidance](../how-to/storage-lifecycle-and-sweeping.md#reclaiming-unreferenced-lfs-objects).

### `OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO`

Per-repository LFS object-count cap. Bytes do not bound row count, so the registry needs
its own bound.

- **Default:** `100000`. **Values:** a positive integer.
- **Set it identically on every replica:** the serve process enforces it at registration.
- **Escape hatch:** raise it to unwedge a repo stuck at the cap with unreferenced junk
  immediately, without waiting for a reclamation grace window.

### `OPENSTORAGE_ABSENCE_RECONCILE_INTERVAL`

Cadence of the registry-vs-storage absence reconciliation job
([ADR-43](../adr/043-absence-reconciliation-covers-registered-but-missing-bytes.md)). Each
pass pages registered `lfs_objects` and `packs` rows and point-checks that their bytes
still exist in blob storage. It is the only mechanism that *detects*
registered-but-missing bytes. See the
[operations guide](../how-to/storage-lifecycle-and-sweeping.md#reconciling-registered-but-missing-bytes).

Absence means storage-layer loss (software deletes quarantine first). A missing
registered pack is logged loudly as an integrity loss: committed refs may reference it.
Absent bytes also deflate the quota admission floor, hiding equally sized rowless legacy
bytes, so the pass charges each loss as a TTL'd restore cover and invalidates cached
floors.

- **Default:** unset (`0`): the job never runs.
- **Values:** a non-negative Go duration, at most half of
  [`OPENSTORAGE_PACK_RESERVATION_TTL`](#openstorage_pack_reservation_ttl); otherwise
  startup fails. With passes capped at a quarter of the TTL and mid-pass ticks coalesced,
  a long scan can never let a live cover lapse.
- **Non-destructive:** it only inserts/refreshes reservation rows and bumps the shared
  floor generation. With quota off
  ([`OPENSTORAGE_PACK_QUOTA_BYTES=0`](#openstorage_pack_quota_bytes)), it detects and
  reports without charging.
- **Admission fence** (quota on): each successful pass stamps a renewal-health watermark.
  When the fence arms, finite quota-gated writes (pushes, LFS uploads, adoptions) fail
  closed with "quota accounting temporarily degraded, retry later" until a healthy pass
  completes one full recovery rotation of both registries. Internal restore/cover
  reservations are always exempt, so recovery can proceed. The fence arms when:
  - a cover renewal **fails** while the watermark is absent or older than the cover-lapse
    safety bound (TTL − this interval − 2 × pass wall clock). Blips within the bound never
    fence.
  - a renewal **succeeds** but the prior watermark is a full TTL old (covers may have
    lapsed). A fleet paused past the TTL, or quota re-enabled after an off period whose
    earlier passes had stamped, therefore runs one fenced recovery rotation first.
- **First pass** after enabling the job, or after upgrading to a version that stamps the
  watermark: an absent watermark alone is not lapse evidence. The pass fences only on
  positive evidence (a durable lapse-evidence marker, recorded whenever admission-time GC
  or the reservation sweep deletes an expired cover; a physically present expired cover;
  or a failed evidence check). Otherwise it stamps health and leaves admissions open, so
  an ordinary first pass does **not** block writes.

> [!IMPORTANT]
> **Upgrade the whole fleet before enabling this job.** Only new binaries check the fence
> and record lapse evidence: pre-upgrade replicas reserve without the fence check, and
> their GC deletes lapsed covers without recording evidence. When upgrading a fleet that
> already runs the job, finish the roll within one
> [`OPENSTORAGE_PACK_RESERVATION_TTL`](#openstorage_pack_reservation_ttl), so no cover
> expires for old GC to consume. The serve boot log warns while reconciliation runs with
> quota enforcement (ADR-43).

### `OPENSTORAGE_ABSENCE_RECONCILE_BUDGET`

Caps the work units one reconciliation pass may charge: one per registry row examined,
plus one per extra registration or twin-spelling probe. Each unit is at most one
blob-store HEAD or registry point read.

- **Default:** `100000`. **Values:** an integer of at least `100` (one row can cost up to
  three units, so smaller budgets cannot guarantee progress on both registries).
- **Resumable:** a budget-limited pass parks its keyset cursor, and the next pass resumes
  strictly after it, so any registry size reconciles across passes.
- **Budget split:** up to half the budget first **re-settles known losses** (refreshing
  their covers every pass, independent of the rotation). The two registry rotations split
  the rest with an alternating leader. A one-hour wall-clock cap bounds the pass even
  when a degraded blob store slows probes.
- **Cover rows:** discovered losses share ONE aggregate reservation row per affected repo
  and registry class. Renewal re-charges each live row at one unit, entirely from the
  database, so the per-repo reservation-row cap cannot bind and any replica keeps every
  cover alive. Each full rotation re-derives the exact deficit, so shrinking lags by at
  most one rotation, never renewal. Details:
  [operations guide](../how-to/storage-lifecycle-and-sweeping.md#reconciling-registered-but-missing-bytes).
- **Sizing:** keep the budget above twice the live cover-row population. Renewal always
  covers every row, squeezing discovery when it overruns (`renewal_over_budget`), which
  slows loss discovery and heal shedding.
- **Fleet-shared state:** rotation cursors, traversal bookkeeping, and the
  alternating-leader parity persist in `job_scan_state`, so multi-replica PostgreSQL
  fleets share one rotation whichever replica River schedules.

### `OPENSTORAGE_REPACK_THRESHOLD`

Registered-pack count at which a post-write trigger schedules a background repack fold
for that repository. Repack packs the union of the snapshotted packs' objects into one
consolidated pack; the registered object set never shrinks.

- **Default:** `10`.
- **Values:** **`0` disables this process's triggers and follow-ups** (already-queued
  durable jobs still run). `1` is rejected: folding fewer than two packs is a no-op, so it
  would busy-loop the trigger. Negative or malformed values are startup errors.
- **Per process/replica:** it governs that process's post-write triggers and the
  follow-up checks of jobs it consumes. Setting one push node to `2` does not change other
  consumers of the same River queue. Mixed-threshold fleets do not promise to preserve a
  producer's lower threshold.
- **Convergence:** with the same enabled threshold on every eligible consumer, pack counts
  target below that threshold, not necessarily one pack. Exactly one pack requires all
  those consumers at `2`, quiescent writes, and successful fold progress within the
  [fold budgets](../how-to/storage-lifecycle-and-sweeping.md#background-repack).

### `OPENSTORAGE_SYNC_STEAL_LEGACY`

Opt-in to steal upstream-sync claims left by **pre-lease builds**. Normal claims are
leased and stolen automatically once the lease expires. Pre-lease claims lack the lease
and holder guards that make stealing safe; new replicas adopt them under one grace period
but never steal them without this flag.

- **Default:** `false`: adopted legacy claims are never stolen. Malformed values stay
  disabled.
- **Enable only after the whole fleet runs a lease-aware build** (same fleet prerequisite
  as [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](#openstorage_sweep_delete_enabled)). From then
  on, legacy claims can only be pre-upgrade crash leftovers, and stealing them heals
  those wedges without manual row surgery. Safe to leave enabled permanently afterwards.

## Internal and test variables

Not part of the operator surface; listed for completeness. Never set these manually.

- `OPENSTORAGE_GIT_REPO`, `OPENSTORAGE_GIT_REFS_POLICY`, `OPENSTORAGE_GIT_OPS`,
  `OPENSTORAGE_GIT_ATOMIC`, `OPENSTORAGE_GIT_PACK_QUOTA`,
  `OPENSTORAGE_GIT_PACK_RESERVATION_TTL`, `OPENSTORAGE_GIT_VERIFY_STATE`,
  `OPENSTORAGE_GIT_VERIFY_SIG_BUDGET`: per-request plumbing the server sets for the
  `openstorage hook` subprocess that git re-execs.
- `OPENSTORAGE_ASKPASS_USERNAME`, `OPENSTORAGE_ASKPASS_PASSWORD`: set by the upstream-sync
  worker for its own git credential-askpass subprocess.
- `OPENSTORAGE_TEST_MODE`, `OPENSTORAGE_TEST_CRASH`, `OPENSTORAGE_TEST_CAS_DROP`,
  `OPENSTORAGE_TEST_CAS_DROP_HARD`, `OPENSTORAGE_TEST_HOOK_DELAY_MS`,
  `OPENSTORAGE_TEST_VERIFY_SPAWN_LOG`: crash/fault injection and observability knobs for
  the e2e suites. The server refuses test-mode requests outside test mode.
- `OPENSTORAGE_TEST_BACKEND`, `OPENSTORAGE_SDK_DIR`: test-lane selection (see the
  [testing reference](testing.md)) and the SDK-compatibility suite's vendor checkout
  location.

The Docker Compose file has its own host-port overrides (`PGPORT`, `S3PORT`,
`NODEA_PORT`, `NODEB_PORT`, `CS_KEYS_DIR`); see the
[`docker-compose.yml`](../../docker-compose.yml) header.
