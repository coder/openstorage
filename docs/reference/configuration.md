# Configuration Reference

Every `OPENSTORAGE_*` environment variable the `openstorage` binary reads, with defaults,
validation constraints, and interaction notes. Source of truth:
[`cmd/openstorage/config.go`](../../cmd/openstorage/config.go). Flags are documented in the
[CLI reference](cli.md).

Resolution precedence per setting: **explicit flag > canonical `OPENSTORAGE_*` env var >
`CODESTORAGE_*` fallback > older legacy alias > `--data-dir` derivation (local mode) >
built-in default**. The old prefix is supported for one minor release, **0.15.x**.
An explicitly set `OPENSTORAGE_*` variable wins even when its value is empty: neither
the old prefix nor any older alias can fill it back in. The selected value then follows
that setting's normal validation and default rules (an empty storage URL derives from
`--data-dir` or fails startup; it never falls back to a legacy alias). `serve` emits a boot-time deprecation warning for each
legacy-prefix fallback, without logging the value. Migrate before upgrading to 0.16.0;
see the [migration guide](../how-to/migrate-to-openstorage.md).

Malformed values are fail-fast startup errors naming the variable, never silently
replaced by a value from the old prefix.

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

- **Schemes:** `postgres://` or `postgresql://` (production, any number of stateless
  replicas), `sqlite://<absolute-path>` (local mode, single node), or
  `s3wal://bucket?endpoint=…&region=…` (**experimental** zero-infra mode, single node:
  refs and metadata live in conditional-CAS objects under the bucket's `_wal/` prefix —
  [ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md)).
- **Legacy aliases** (scanned left to right, the first *set* name wins, even when empty): `SPIKE_PG_URL`, `PG_URL`.
- **Default:** none. Startup fails unless set explicitly or derived from the data dir
  (`sqlite://<dir>/codestorage.db`).
- In PostgreSQL URI query values, `+` is literal (encode spaces as `%20`), and
  repeated parameters use the last value. Invalid percent escapes are rejected.
  These parsing rules follow libpq as of pgx v5.11.
- On Postgres, schema setup (including the durable [River](https://riverqueue.com) job
  tables) runs automatically at startup and is advisory-lock-serialized, so concurrently
  booting replicas are safe.
- **`s3wal://` pairing rule:** the scheme requires an `s3://`
  [`OPENSTORAGE_S3_URL`](#openstorage_s3_url) — `file://` (including the data-dir
  derivation) is refused at startup, because the mode's whole premise is that the bucket
  is the *sole* durable authority. Pointing both URLs at the **same bucket** is the
  expected topology (the `packs/`, `lfs/`, and `_wal/` key prefixes coexist without
  overlap; the `_wal/` prefix is reserved). Credentials come from the standard AWS env
  chain; at startup a conditional-write probe refuses any store that does not implement
  `If-Match`/`If-None-Match` PUTs faithfully (qualified: SeaweedFS and AWS S3 — see the
  [benchmark results](../plans/s3wal-bench-results.md) for the AWS campaign). The store
  must also provide strong read-after-write and list-after-write consistency — an
  unprobed deployment prerequisite, see the
  [store requirements](../how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node).
  Jobs run on the in-process queue and tickers, exactly like local mode.

### `OPENSTORAGE_S3_URL`

Immutable pack storage URL, opened through [gocloud.dev](https://gocloud.dev/howto/blob/).

- **Schemes:** `s3://bucket?region=...` (AWS S3, or an S3-compatible store such as
  [SeaweedFS](https://github.com/seaweedfs/seaweedfs) — the tested self-hosted
  reference — by adding `endpoint=http://...&s3ForcePathStyle=true`; other
  S3-compatible stores, including MinIO, are best-effort) or `file://<absolute-dir>`
  (local mode).
- **Legacy aliases** (same presence rule as `OPENSTORAGE_PG_URL`): `SPIKE_S3_URL`, `BLOB_URL`.
- **Default:** none. Startup fails unless set explicitly or derived from the data dir
  (`file://<dir>/blobs`).
- S3 credentials come from the standard AWS chain (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, shared config, instance roles).
- Delete-mode sweeping interacts with bucket versioning — see
  [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](#openstorage_sweep_delete_enabled).
- With an `s3wal://` database URL this **must** be `s3://` — see the
  [pairing rule](#openstorage_pg_url); the same bucket is the expected target.

### `OPENSTORAGE_S3WAL_LEASE_TTL`

s3wal mode only: the TTL of the `_wal/instance-lease` dual-serve guard
([ADR-46](../adr/046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md)
§2.11). At startup, serve refuses to run while another process holds a fresh lease on the
bucket; while running, it heartbeats the lease every TTL/4 and **fail-stops** if the lease
is stolen or unrenewable. A graceful shutdown (`SIGTERM`/`SIGINT`) releases the lease so a
successor can start immediately; only a hard kill (`SIGKILL`, crash, power loss) leaves the
lease behind, and that node's successor can start once the stale lease ages past the TTL —
the [production guide](../how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node)
covers the recovery runbook.

- **Default:** unset — 2 minutes (30-second heartbeats).
- **Validation:** a [duration](https://pkg.go.dev/time#ParseDuration) of at least `1s` —
  the derived heartbeat cadence must tolerate ordinary S3 latency jitter — or exactly `0`,
  which **disables** the lease entirely (a test-suite escape hatch for deliberately
  running several processes against one bucket; serve warns loudly, and accidental
  dual-serve topologies then go undetected).
- The lease is best-effort *detection*: correctness never depends on it (every state
  change is CAS-serialized regardless), so tuning it can only change how quickly an
  accidental second writer is noticed. Ignored outside s3wal mode.

### `OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER`

Optional [bucket owner
condition](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-owner-condition.html):
the 12-digit AWS account ID that must own the `OPENSTORAGE_S3_URL` bucket. AWS bucket
names are global — a deleted bucket name can be re-registered by a third party — and the
blob layer otherwise trusts whatever the name resolves to. When set, every S3 request the
server issues carries `x-amz-expected-bucket-owner` via client middleware (the gocloud
data plane, multipart copies, the startup probe, sweeps, git-hook subprocesses, and the
purge CLI all share that client seam), so operations against a re-owned bucket fail with
`403 Access Denied` instead of silently reading from or writing to an attacker's bucket.
The same env var is also enforced by the `s3wal://` mode's conditional client: every
`_wal/` manifest/metadata CAS request — including the boot probe and the instance
lease — carries the header via its own all-or-nothing middleware, so the ref and
metadata authority objects (which include the org's public keys) get the same protection
as the blob plane.

- **Default:** unset — no condition parameters are sent; behavior is unchanged.
- **Format:** exactly 12 digits. Anything else is boot-fatal (a typo'd ID would 403 every
  operation on AWS, so misconfiguration fails fast instead).
- **Startup:** when set, every accepted `CreateBucket` outcome — success,
  `BucketAlreadyOwnedByYou`, `BucketAlreadyExists`, or the tolerated `AccessDenied` of a
  least-privilege role — is followed by a `HeadBucket` probe under the owner condition,
  so a squatted name *and* credential/account drift (credentials from a different account
  than the configured owner) both fail boot. (AWS ignores the condition on `CreateBucket`
  itself; that operation cannot carry it.)
- **Presigned URLs** (LFS downloads) embed the condition as a signed lowercase
  `x-amz-expected-bucket-owner` query parameter — the spelling S3 enforces on presigned
  requests. The URL stays self-contained (clients send no extra header), and stripping or
  altering the parameter breaks the SigV4 signature.
- **Self-hosted stores (SeaweedFS, MinIO) and S3 emulators ignore the condition** (no
  global bucket namespace); the
  parameter is harmless pass-through there, so the setting is only useful — and only
  meaningfully enforced — against AWS S3. Pair it with the `aws:ResourceAccount` IAM
  condition described in [Deploy to production](../how-to/deploy-production.md), which
  remains the primary control; this application-level check is defense-in-depth.

### `OPENSTORAGE_S3_OP_LOG`

Diagnostics seam (issue #182): when set to a file path, every S3 HTTP round trip issued
through the server's SDK clients — the blob client and the `s3wal://` conditional
client, in the serve process and in the git-hook subprocesses alike — appends one
whitespace-separated line:

```
<unixnano> <pid> <role> <operation> <status> <duration_us> <request-path>
```

`role` is derived from the process argv (`openstorage:serve`,
`openstorage:hook:pre-receive`, …), so a shared log attributes each round trip to the
process that issued it. Use it to census the per-operation S3 round-trip cost of a
request (each round trip is a WAN round trip on real S3). Writes are `O_APPEND`
one-line syscalls, safe for concurrent processes sharing one file.

- **Default:** unset — no middleware is installed; zero overhead.
- **The path must be absolute.** Hook subprocesses inherit the variable but run from the
  cache repository, not the server's working directory, so a relative path would silently
  split the log across per-process files; it is rejected (with a stderr warning).
- An unopenable path logs a warning to stderr and disables the seam; it never fails the
  process (diagnostics must not take down serving).
- The log grows without bound and includes object keys; treat it like a debug log
  (temporary use, not permanent telemetry).

### `OPENSTORAGE_DATA_DIR`

Local-mode data directory (the `--data-dir` flag wins over the env var). Setting it derives
every storage slot **not set explicitly**:

- database: `sqlite://<dir>/codestorage.db`
- packs: `file://<dir>/blobs`
- cache: `<dir>/cache`

The database filename is retained for on-disk compatibility: the renamed binary reopens
the same database, without a file rename or data migration. See the
[migration guide](../how-to/migrate-to-openstorage.md#3-keep-persistent-data-locations-unchanged).

Explicit `OPENSTORAGE_PG_URL` / `OPENSTORAGE_S3_URL` / `--cache-dir` always win over the
derivation. The path is made absolute at startup (git hooks re-exec the binary from a
different working directory). Local mode is intentionally single-node — see ADR-13 in
[local-backends notes](../archive/phases/11-local-backends-implementation-notes.md).

## Server identity and network

### `OPENSTORAGE_LISTEN`

Listen address for the single HTTP listener (REST API + git smart-HTTP + `/healthz`).
The `--listen` flag (or its legacy alias `--addr`) wins. **Default:** `127.0.0.1:0`
(an ephemeral port, printed as `listening on ...` at startup).

### `OPENSTORAGE_ORG`

**Required.** The organization name — the JWT `iss` claim every token must carry
([auth spec](authentication.md)). `serve` always runs single-org mode: the REST API and
the git smart-HTTP path authenticate against the same org-key verifier, and the org row is
bootstrapped from this value at startup.

### `OPENSTORAGE_ORG_PUBKEYS`

**Required.** Path to a PEM file with one or more public key blocks (PKIX) that verify the
org's JWTs. Every block that PEM-decodes is validated at startup (must be a `PUBLIC KEY`
block of a supported family) — but parsing stops silently at the first undecodable data, so
a truncated block appended after a valid key is dropped rather than rejected. After a key
rotation, verify a token signed with the newly added key is accepted instead of trusting
startup success.
Generate a keypair with [`openstorage keygen`](cli.md#openstorage-keygen); allowed JWS
algorithms are `ES256`, `RS256`, `EdDSA`, `PS256`, `ES384`, `ES512` (`none` and `HS*` are
always rejected).

### `OPENSTORAGE_BASE_URL`

Externally reachable base URL (`http(s)://host[:port]`, no query/fragment; a trailing slash
is stripped) used for the `http_url` values in API responses. **Default:** derived from the
bound listener address — which is unroutable when binding `0.0.0.0`, so set this behind any
real deployment or reverse proxy.

## Rate limiting

### `OPENSTORAGE_RATE_RPS` / `OPENSTORAGE_RATE_BURST`

Per-client-IP token-bucket rate limit applied to the REST surface **and** — with separate
bucket state — the git smart-HTTP surface. Exceeding it yields `429` with `Retry-After`
plus the IETF `RateLimit` header (`"default";r=0;t=<retry-after-secs>`), the pair
documented upstream on every 429.

- **Default:** both unset — limiting disabled.
- **Constraints (fail-fast at startup):** `RPS` must be a non-negative finite number;
  `BURST` a non-negative integer. Setting `RPS > 0` requires `BURST >= 1` and vice versa —
  half-set pairs are startup errors. `RPS=0` with `BURST` unset/`0` explicitly disables.
- The git surface raises bucket depths below 8 to 8: one git operation is several
  smart-HTTP requests, and a burst too small for a complete exchange would make every
  clone/fetch/push permanently fail.
- Client identity defaults to the TCP `RemoteAddr`; forwarded headers are **not** trusted
  unless `OPENSTORAGE_TRUSTED_PROXIES` is set. Behind a proxy that pools backend
  connections, all clients otherwise share one bucket.

### `OPENSTORAGE_TRUSTED_PROXIES`

Comma-separated CIDR allowlist (bare IPs accepted as single-address entries) of reverse
proxies whose forwarded client identity the rate limiters may trust.

- When the TCP peer is inside the allowlist, both limiters key buckets on the **rightmost
  `X-Forwarded-For` hop that is not itself allowlisted**; otherwise (including malformed
  `X-Forwarded-For`) they fall back to `RemoteAddr`.
- **Default:** unset — `RemoteAddr`-only keying.
- **Constraints:** validated at startup even while rate limiting is disabled. Empty
  entries, zone-scoped addresses, and IPv4-mapped prefixes below `/96` are rejected;
  IPv4-mapped entries are normalized to their IPv4 form.
- **List only proxies that set or append `X-Forwarded-For` themselves** — trusting a proxy
  that forwards client-supplied values verbatim lets any client mint fresh rate-limit
  buckets.

### `OPENSTORAGE_SYNC_ALLOW_PRIVATE`

Unified sync egress policy for BOTH git transfer directions (ADR-45). `upstream_host` is
caller-linked data, so every http(s) sync transfer — inbound pull-upstream fetches
(`POST /repos/pull-upstream`, the first sync at `POST /repos`, freshness pokes) and
outbound push forwards (`POST /repos/push-upstream`, `forward_pushes`) — routes through a
dial-time vetting proxy that applies the same destination policy as webhook delivery:
private, loopback, link-local, metadata, and special-purpose targets are denied, with
every DNS answer re-resolved and vetted at connect time. A denied destination fails the
run as `repository configuration error` (pull) / `invalid sync configuration` (push, no
retry) and the upstream is never contacted. Registration (`POST /repos` with `base_repo`)
additionally runs a fast-feedback courtesy check against the same pull-lane policy;
dial-time enforcement stays authoritative.

- `true` — both directions may target internal self-hosted forges
  (RFC 1918/loopback/ULA/link-local). Cloud-metadata and special-purpose ranges stay
  denied either way.
- `false` — strict in both directions: only public upstreams are dialable.
- **Unset (the transitional default):** pushes stay strict (the deprecated
  [`OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE`](#openstorage_sync_push_allow_private) may relax
  them), while pulls ALLOW private upstreams and `serve` logs a boot warning. Syncing
  from internal forges is a mainstream released 0.6.x deployment shape, so this release
  does not break it; **a future release flips the unset pull default to deny.** Set the
  variable explicitly now: `true` to keep internal-forge pulls working across the flip,
  `false` to adopt the strict policy early.

Migration notes (0.7.x): pull-upstream fetches previously had no destination policy.
They now always dial through the vetting proxy, which also means ambient
`HTTP_PROXY`/`HTTPS_PROXY` settings no longer apply to upstream fetches — the guard
dials destinations directly, exactly like webhook delivery and push forwarding.
Operators that require proxied egress must enforce an equivalent destination policy at
that proxy; there is no supported way to chain the guard through an external proxy.
Malformed boolean values are startup errors.

### `OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE`

**Deprecated** push-only spelling of the sync egress policy, kept for released 0.6.x
deployments. **Default: `false`.** When
[`OPENSTORAGE_SYNC_ALLOW_PRIVATE`](#openstorage_sync_allow_private) is unset, `true`
relaxes the private receiver classes for outbound push forwards only (its historical
meaning); when the unified variable is set, this one is ignored. Either state logs a
boot-time deprecation notice naming the replacement.

### `OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE`

Controls the native webhook SSRF guard. **Default: `false`.** By default, webhook delivery
rejects IANA special-purpose ranges, including loopback, RFC 1918, RFC 6598 shared,
IPv4/IPv6 link-local, IPv6 ULA, translation/protocol anycast, documentation, benchmarking,
reserved, unspecified, multicast, broadcast, and cloud-metadata targets. IPv4-mapped IPv6
addresses are normalized before classification, so (for example) `::ffff:10.0.0.1` is private.

Registration performs the same target check against IP literals and the DNS answers visible
at that moment as a fast-feedback courtesy. **Registration acceptance is not a reachability
guarantee:** DNS may be unavailable or change after registration. Every delivery attempt is
authoritatively re-resolved at dial time; all returned addresses must be allowed, and a
resolution failure, empty answer, or mixed public/private answer fails the attempt through
the normal retry/dead-letter path. The client connects to the vetted IP literal so DNS cannot
be rebound between validation and connect.

Webhook deliveries never follow redirects. `301`, `302`, `307`, and `308` responses are
delivery failures, and the redirect target receives no request. The delivery transport also
ignores `HTTP_PROXY` / `HTTPS_PROXY`: validating a proxy socket would not validate the URL
target the proxy later connects to.

Set this variable to a Go boolean `true` only when webhooks intentionally target internal
services. It admits loopback, RFC 1918, RFC 6598 shared, link-local, and ULA receivers,
and logs a warning at startup. It **does not** permit cloud metadata, unspecified/multicast/
broadcast addresses, other IANA special-purpose ranges (such as documentation, benchmarking,
reserved, translation, or protocol-anycast space), or redirects. Malformed boolean values
are startup errors.

## Secrets at rest

### `OPENSTORAGE_SECRETS_KEY`

Enables application-level envelope encryption of the metadata database's secret columns:
webhook HMAC signing secrets (`webhooks.secret`) and upstream-sync credential passwords
(`git_credentials.password`). **Default: unset — those columns are stored plaintext** and
the [production guide's](../how-to/deploy-production.md#backing-services) guidance to
scope database access tightly and encrypt the database and its backups applies unchanged
(it remains recommended defense-in-depth even with this variable set).

Format: a comma-separated list of base64-encoded 32-byte (AES-256) keys. The **first key
encrypts every write**; the remaining keys are decrypt-only. Generate a key with:

```sh
head -c 32 /dev/urandom | base64
```

Values are stored as `enc:v1:<keyid>:<base64 nonce+ciphertext>` (AES-256-GCM), where
`<keyid>` is the first 8 hex characters of the key's SHA-256 fingerprint — public, safe to
log, and usable in SQL to audit rotation progress. Rows written before encryption was
enabled stay readable and are transparently re-encrypted the next time they are read
(webhook deliveries, credential use, and list/get API calls all count) — including any
legacy plaintext value that happens to start with `enc:`, which reads verbatim and is
migrated into a real envelope. To keep new values unambiguous, the API rejects new webhook
secrets and credential passwords starting with the reserved prefix `enc:` (HTTP 400) — in
plaintext mode too.

**Enabling on a multi-replica fleet.** A rolling restart applies any environment change
one replica at a time, so enablement is staged so that no envelope is ever written before
every process can decrypt it (a single-replica deployment, e.g. local mode, can skip
straight to step 3):

1. **Binary first:** deploy an envelope-aware binary to *every* node — serve replicas and
   hook-running git nodes — with `OPENSTORAGE_SECRETS_KEY` unset. Plaintext behavior is
   unchanged (see the upgrade-ordering note below for why this must come first).
2. **Stage the key, decrypt-only:** set `OPENSTORAGE_SECRETS_KEY` together with
   [`OPENSTORAGE_SECRETS_MODE=decrypt-only`](#openstorage_secrets_mode) fleet-wide. Every
   process loads the key for reading but keeps writing plaintext and performs no rewrites,
   so replicas restarted earlier never produce a value the rest cannot read.
3. **Switch to encrypt:** once every replica runs with the staged key, drop
   `OPENSTORAGE_SECRETS_MODE` (or set it to `encrypt`) fleet-wide. Encrypted writes and
   lazy re-encryption begin; replicas restarted earlier in this rollout write envelopes
   the not-yet-restarted ones already decrypt, courtesy of step 2.

Alternatively, a drained/atomic cutover (stop all replicas, set the key, start) collapses
steps 2–3.

**Key rotation.** The same staging applies when introducing a new key — no process may
ever encounter a key it cannot decrypt:

1. **Stage the new key fleet-wide, as a decrypt-only list entry:**
   `OPENSTORAGE_SECRETS_KEY="<old-key>,<new-key>"`. Writes still use the old key, but
   every restarted process can already decrypt both, so ordering within the rollout does
   not matter.
2. **Promote, fleet-wide:** `OPENSTORAGE_SECRETS_KEY="<new-key>,<old-key>"`. Replicas
   restarted earlier keep decrypting new-key rows because step 1 listed it everywhere.
3. **Rewrite and verify, then drop the old key** (below).

A single-replica deployment can skip step 1 — one restart swaps the whole list atomically.

Reads decrypt with whichever listed key wrote the row and re-encrypt it under the new
active key. To force full re-encryption instead of waiting for organic reads, touch every
row once — list the org's webhooks (`GET /api/v1/webhooks`) and, per repo with a stored
credential, trigger a sync (`POST /api/v1/repos/pull-upstream`) or re-`PUT` the
credential. Verify progress directly:

```sql
SELECT count(*) FROM webhooks        WHERE secret   NOT LIKE 'enc:v1:<new-keyid>:%';
SELECT count(*) FROM git_credentials WHERE password NOT LIKE 'enc:v1:<new-keyid>:%';
```

When both counts reach zero, drop the old key from the list.

Failure modes are fail-closed and named: a malformed key list (bad base64, wrong length,
duplicate or empty entries) is a startup error; reading an encrypted row without the key
that wrote it is a request-time error naming the row and key fingerprint — never a silent
plaintext or ciphertext passthrough. Removing the variable entirely while encrypted rows
exist makes those rows unreadable (same error), so keep the key list stable once enabled
and back the keys up outside the database. Recovery never requires the lost key: webhook
listings and event fan-out stay up (an undecryptable webhook is listed with its secret
omitted and only its own deliveries fail), and replacing a credential's password (`PUT`)
or deleting it, and deleting + re-registering a webhook, work on undecryptable rows —
those paths never read the stored value. Startup logs one line
(`secrets at-rest encryption enabled`) with the mode, key count, and active fingerprint;
key material never appears in logs or errors.

Hook subprocesses and every replica must see the same value — it is read from the
environment by the serve process and inherited by the re-executed hook binary, so no extra
plumbing is needed beyond setting it consistently across replicas.

**Upgrade ordering (mixed fleets).** Binaries that predate this variable do not understand
the `enc:v1:` envelope: they would read it verbatim and sign webhook deliveries with the
ciphertext string (or present it as an upstream password), producing invalid deliveries
and failed syncs with no error. That is why enablement step 1 above requires every node to
run an envelope-aware binary before the key is set anywhere. The same constraint applies
in reverse: rolling back to a pre-envelope binary while encrypted rows exist silently
misuses them — decrypt requirements do not roll back with the binary.

### `OPENSTORAGE_SECRETS_MODE`

Staging switch for [`OPENSTORAGE_SECRETS_KEY`](#openstorage_secrets_key). **Default:
`encrypt`** (normal operation). `decrypt-only` loads the key list for reading but keeps
writing plaintext and performs no lazy rewrites — the first-enable staging mode described
above, used while a rolling restart distributes the key so that no envelope exists before
every replica can decrypt it. Setting `decrypt-only` without `OPENSTORAGE_SECRETS_KEY`,
or any other value, is a startup error. Do not run `decrypt-only` indefinitely: it stores
new secrets plaintext.

## Pack quota

### `OPENSTORAGE_PACK_QUOTA_BYTES`

Per-repository storage-bytes quota enforced at every external storage writer: git pushes
that would grow a repo's `packs/` usage past the quota are refused at pre-receive before
anything is stored, REST pack writes (commit-pack, merge, notes, fork) are refused with
a `403` envelope, and Git LFS uploads ([#51](https://github.com/coder/openstorage/issues/51))
are refused with per-object `507` batch errors before any transfer URL is issued (the
verified upload transfer re-checks the actual `Content-Length`, so under-declared batch
sizes cannot bypass the gate).

- **Default:** 10 GiB (`10737418240`). **`0` disables enforcement**; negative or malformed
  values are startup errors.
- **Namespace roll-up:** a repository and its `+ephemeral` / `+import` namespaces
  ([git protocol](../explanation/git-protocol-and-namespaces.md)) share one quota, accounted against the
  base repository. LFS objects always store under the base repository's prefix.
- Admission is a durable reservation ledger in the ref store, atomic across concurrent
  pushes and replicas: two pushes that individually fit but together exceed the quota —
  exactly one wins. LFS uploads charge the same ledger (an advisory batch fast-fail,
  then a durable reserve at the verified upload, settled at
  verified upload completion). Independent of bytes, two compiled population bounds
  (like the repack fold budgets) keep tiny declared sizes from growing the shared
  database: each repository is capped at 10,000 **active reservation rows** (excess
  batches answer transient `429`s until rows settle or expire; admission also prunes the
  repo's expired rows itself, so the physical table stays bounded even with sweeping
  disabled) and at 100,000 **registered LFS objects** (excess answers `507` naming the
  limit; re-publication of already-registered objects is exempt). These apply even with
  the byte quota disabled.
- Background repack maintenance is **not** quota-gated (an at-quota repository could never
  compact itself otherwise), and repack preserves the registered object set — it does not
  reclaim unreachable objects. LFS downloads are never quota-gated.

### `OPENSTORAGE_LFS_STAGING_BYTES`

Node-wide cap on the aggregate bytes of Git LFS uploads staging on one replica at a
time. LFS uploads go through the verified proxy transfer
([#51](https://github.com/coder/openstorage/issues/51)) and stage their full body
under the replica's `$TMPDIR` before verification and publication — the
per-repository quota above bounds neither the node (it is per repo) nor anything when
disabled, so this is the replica's disk-protection bound. The budget is charged
incrementally as body bytes actually arrive (a stalled upload holds only slightly more
than it has transferred, not its declared `Content-Length`), and each repository's
staged bytes are held to a fair share that scales with the budget's free capacity, so
one repository's uploads cannot occupy the whole replica budget. Uploads that do not
fit right now answer `429` with `Retry-After` + `RateLimit`; an object whose declared size exceeds
the largest hold a single repository can ever reach — one third of the budget —
answers `413`.

- **Default:** 32 GiB (`34359738368`). The budget is a **working set, not an
  object-size cap**: because a single repository's hold tops out at a third of the
  budget, supporting objects of size `S` needs a budget of about `3 × S` — the default
  keeps one default-quota-sized (10 GiB) object stageable with cross-repository
  capacity intact. **`0` disables the bound**; negative or malformed values are
  startup errors.
- Size it below the free space you guarantee `$TMPDIR`, minus headroom for git push
  quarantines and fork closures, which stage there too.
- Each object additionally allows only one in-flight upload per replica (concurrent
  identical PUTs answer `429`), and an upload body is aborted — releasing its capacity —
  when it delivers no bytes for a full minute **or** outlives the
  [reservation TTL](#openstorage_pack_reservation_ttl) in total (an upload slower than
  the TTL's implied floor — ~3 MB/s for 10 GiB at the defaults — cannot finish before
  its admission reservation expires anyway).

### `OPENSTORAGE_LFS_V020_WRITE_COMPAT`

Registered LFS objects live at a canonical key old presigned upload URLs cannot
address ([ADR-41](../adr/041-registered-lfs-objects-live-at-a-canonical-key-legacy-presigned-urls-cannot-address.md));
replicas running v0.2.0 or older derive only the legacy single-key spelling and
cannot see canonically stored objects. During a rolling upgrade from v0.2.0, set
this on the **upgraded** replicas so every verified LFS publication is also copied
to the legacy key — not-yet-upgraded replicas keep serving those objects for the
duration of the roll. The bridge is enforced, not best-effort: an upload whose
compat copy fails answers `502` (the canonical object is already durable and
registered — the client's retry is zero-growth and repairs the copy), and
write-scoped touches of registered objects repair a missing legacy spelling.
Drop the variable once the whole fleet is upgraded: the
compat copies are rowless, so the [orphan sweep](../how-to/storage-lifecycle-and-sweeping.md)
reclaims them under its LFS opt-in.

- **Default:** `false`. The canonical spelling is the secure one — the compat
  copies are exactly as exposed to still-valid legacy upload URLs as everything
  v0.2.0 served — so compat is opt-in and meant to be temporary.
- **Cannot be combined with [`OPENSTORAGE_SWEEP_DELETE_LFS`](#openstorage_sweep_delete_lfs)
  while a sweep schedule is active** (`OPENSTORAGE_SWEEP_INTERVAL` > 0 — a
  fail-fast startup error; with the interval at `0` no schedule is built, the
  delete flag is inert, and the fleet-wide sweep emergency stop stays
  available): the flag declares a mixed v0.2.0 fleet, and during
  that roll destructive LFS sweeping must be off **fleet-wide** — the in-place row
  migration makes a touched object's legacy blob rowless to an old binary's
  sweeper, which has no legacy gate and reaps it immediately, breaking
  old-replica reads. The tripwire covers upgraded nodes; set
  `CODESTORAGE_SWEEP_DELETE_LFS=false` on pre-0.15 nodes yourself before rolling
  (those binaries do not recognize the new prefix).
- With a [storage quota](#openstorage_pack_quota_bytes) enforced, the bridge copy
  passes real quota admission: each fresh upload effectively costs twice its size
  for the roll's duration, and an upload whose bridge would exceed the quota
  answers `507` instead of overshooting (the canonical object stays registered;
  freeing space lets a retry repair the bridge).

### `OPENSTORAGE_PACK_RESERVATION_TTL`

How long a crashed writer's quota reservation keeps gating admission (Go duration, e.g.
`30m`, `2h`). Admission ignores rows older than the TTL immediately; the sweeper's
periodic pruning of expired rows is garbage collection, not what re-opens headroom.

- **Default:** `1h`. **Constraints:** must be greater than 60 seconds — the 30-second
  usage-floor cache lifetime plus the 30-second admission handoff margin — and the
  server refuses smaller values at startup. A zero TTL would expire live reservations
  instantly, and a TTL at or below the floor's freshness budget would let a cached
  usage listing (plus the admission call it feeds) outlive the reservations covering
  uploads published after it, silently reopening the overshoot the ledger closes.
- Must comfortably exceed the longest plausible server-side pack upload — the default
  clears a default-quota-sized (10 GiB) upload at ~3 MB/s.
- LFS upload reservations (charged durably at the verified upload, right before its
  bytes publish; the batch check is advisory and holds nothing) use the same TTL, so it
  must cover an object's publication and registration tail, not the client's transfer
  time.

## Push work bounds

### `OPENSTORAGE_MAX_PUSH_BYTES`

Decoded-byte cap on one git push: a `git-receive-pack` request whose body — after any
`Content-Encoding: gzip` decode, so compressed streams cannot smuggle a larger payload —
exceeds this many bytes is rejected with `413` and a JSON error envelope. Only bytes
**up to the cap** can ever reach `git receive-pack` (a declared oversize is rejected
without reading at all), so per-push disk and decode work is bounded by the cap: git
discards the truncated partial quarantine when the stream is cut, and nothing is
published or registered on the overflow path. This is the native pre-storage push bound
([#55](https://github.com/coder/openstorage/issues/55)); the pack quota above bounds
*retained* storage and runs only after a push has fully streamed to the replica's disk.
(Degenerate exception: a *complete, valid* push padded with trailing junk past the cap
still answers `413`, but its hooks already ran under the normal quota admission.)

- **Default:** 2 × [`OPENSTORAGE_PACK_QUOTA_BYTES`](#openstorage_pack_quota_bytes), with
  a 2 MiB minimum — a push cannot legitimately need more than quota admission would
  accept plus protocol overhead, and the floor keeps delete-only/zero-growth pushes (the
  over-quota recovery path) working under byte-scale quotas. Disabling the quota (`0`)
  also disables the derived cap; an explicit value is taken verbatim (no floor); set
  this variable explicitly to keep a cap without a quota. **`0` disables the cap**;
  negative or malformed values are startup errors.
- Applies to git pushes only, including the upstream-sync worker's loopback pushes.
  Fetches/clones (`upload-pack`) and LFS object transfers are unaffected.
- Gzip request bodies are additionally bounded on the **compressed** side at the cap
  plus ~6% + 4 KiB of framing allowance (streams that decompress to almost nothing
  cannot otherwise be bounded by a decoded count). Single-member streams — what git
  clients send — never approach it; the 413 envelope names both bounds.

### `OPENSTORAGE_VERIFY_SIG_BUDGET`

Per-push budget for `verify-sig` signature verifications
([#59](https://github.com/coder/openstorage/issues/59)): the maximum number of commits
one push may verify — each verification spawns a `git verify-commit` subprocess — across
all its refs, pre-receive evaluation and update hooks combined. A ref update that would
exceed the remaining budget is rejected fail-closed **before** the excess verifications
run, with an error naming this variable. Rejection follows git's normal per-ref
semantics, like every other policy denial: on a plain multi-ref push only the
over-budget ref updates are denied (refs whose commits fit the budget can still land),
while `git push --atomic` rejects the whole push on the first denial.

- **Default:** `10000`. **`0` disables the budget**; negative or malformed values are
  startup errors.
- The budget is charged per unique commit actually verified: within one push, verdicts
  are cached per commit and whole ranges are deduplicated across refs, so pushing many
  refs over the same history costs each commit one verification.
- Branch creation verifies **everything reachable** from the new tip — per the
  branch-protection contract the comparison is against the ref's own prior tip, never
  against other refs — so creating a ref over a deep signature-verified history is the
  worst case this budget bounds. Raise it deliberately before importing histories deeper
  than the budget under a `verify-sig` policy: such pushes are rejected fail-closed with
  an error naming this variable.
- The budget also derives a per-push **history-walk allowance** of 100 × its value:
  every commit a walk examines (cached verdicts included) counts, and each ref
  evaluation is additionally charged a small fee for the per-push verification state it
  loads — so many refs re-walking or merely re-loading cached results are bounded too.
  `0` disables all of these bounds.

## Maintenance jobs

Background jobs run through a queue seam: durable [River](https://riverqueue.com) jobs on
Postgres, in-process in local mode.

### `OPENSTORAGE_SWEEP_INTERVAL`

Interval between orphan-pack sweeps (Go duration). The sweeper diffs the blob store's
`packs/` and `lfs/` listings against their authoritative registries and reports — and
only when explicitly enabled, deletes — unregistered blobs older than the age threshold.
It also prunes expired quota reservations on the same cadence.

- **Default:** `1h`. **`0` disables sweeping** (a strictly restorative quarantine-heal pass
  still runs once at startup, and an hourly background janitor keeps reconciling
  legacy/canonical LFS registry-row twins that mixed-version rollouts can strand after
  startup — without it a duplicate row would double-charge usage and the object cap
  indefinitely); negative values are startup errors.

### `OPENSTORAGE_SWEEP_AGE_THRESHOLD`

Minimum blob age before an unregistered pack counts as an orphan (Go duration). The
threshold is the only concurrency guard needed: in-flight pushes register their packs
within a request timeout, far below any sane threshold.

- **Default:** `24h`. **Constraints:** must be positive.

### `OPENSTORAGE_SWEEP_DELETE_ENABLED`

Opt-in delete mode for **general** orphans. `false` (the default) is list-only: candidates
are logged, nothing is deleted. Deletes copy each blob to a quarantine key first, and every
sweep starts with a heal pass that restores any registered-but-missing pack from
quarantine — so a rollback to list-only still repairs a delete-enabled crash.

- **Default:** `false`. **Constraints:** must parse as a boolean.
- **Fleet-upgrade prerequisite:** enable only once **every** node — serve replicas *and*
  hook-running git nodes — runs a build with the post-registration publish verify
  (`blob.EnsurePublished`). A mixed fleet cannot lose data (quarantine plus the heal pass
  restore any mis-reaped registered pack), but an old writer racing a delete-mode sweep can
  see its freshly registered pack unavailable until the next sweep heals it. List-only mode
  is safe on any fleet.
- **S3 versioning:** on a bucket with versioning enabled or suspended, deletes only write
  delete markers while every version stays stored and billed. Startup **fails** when delete
  mode is enabled against such a bucket (or when versioning cannot be verified — grant
  `s3:GetBucketVersioning` or stay list-only), and every delete-mode sweep re-checks at
  runtime, downgrading itself to list-only on drift.

### `OPENSTORAGE_SWEEP_DELETE_SUPERSEDED`

Delete mode for the **narrow superseded-by-repack** class: blobs whose registry rows a
repack fold provably removed (durable `pack_retained` records) are reaped after the age
threshold and their retained bytes returned to the repo's quota headroom.

- **Default:** `true` (`false` disables). **Constraints:** must parse as a boolean.
- Unlike the general delete opt-in above, a versioned S3 bucket does not fail startup —
  each sweep run downgrades to list-only instead (the heal pass still runs).

### `OPENSTORAGE_SWEEP_DELETE_LFS`

Delete mode for **LFS orphans**: `lfs/` blobs with no LFS registry row, older than the
age threshold. Deliberately separate from `OPENSTORAGE_SWEEP_DELETE_ENABLED`: the LFS
registry ships with [#51](https://github.com/coder/openstorage/issues/51), so **every
LFS object uploaded by an older build has no row** and would classify as an orphan —
a fleet already running general deletes must not reap its pre-existing LFS population
on upgrade. Stored objects are adopted into the registry by write-scoped touches
(upload batches naming them, re-uploads of the same content).

- **Default:** `false` (list-only). **Constraints:** must parse as a boolean.
- Enable only after list-only [sweep reports](../how-to/storage-lifecycle-and-sweeping.md#lfs-orphans-and-pre-registry-objects)
  stop naming objects that should live.
- Same quarantine-first disposal and heal pass as the pack classes; on versioned S3
  buckets it fails startup exactly like the general delete opt-in.
- Legacy-spelled keys (`lfs/<repo_id>/<oid>`, ADR-41) are **excluded** from this reap
  until [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](#openstorage_sweep_delete_lfs_legacy)
  is also set.

### `OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`

Post-roll cleanup opt-in for **legacy-spelled** LFS keys (`lfs/<repo_id>/<oid>` — the
v0.2.0-and-older spelling,
[ADR-41](../adr/041-registered-lfs-objects-live-at-a-canonical-key-legacy-presigned-urls-cannot-address.md)).
Adoption migrates objects to canonical keys but **retains** the legacy source: old
replicas derive only that spelling, and a just-issued presigned GET must survive its
lifetime — so during a rolling upgrade the retained sources are aged rowless orphans
that [`OPENSTORAGE_SWEEP_DELETE_LFS`](#openstorage_sweep_delete_lfs) alone must not
touch. This flag is the operator's assertion that the transition is over: every
replica runs the new binary and the last old replica exited more than 15 minutes ago
(the presign expiry). The transition gate dominates every other flag combination —
without this flag no sweep mode deletes a legacy-spelled key.

- **Default:** `false` (legacy keys are never deleted). **Constraints:** must parse
  as a boolean; only meaningful together with `OPENSTORAGE_SWEEP_DELETE_LFS`.
- Same quarantine-first disposal as every destructive class.

### `OPENSTORAGE_REPO_RETENTION`

Retention window for soft-deleted repositories: once a repository has been deleted
longer than this, a purge pass on the sweep cadence **permanently** removes its
registry and eventing rows (refs, packs, LFS objects, credentials, upstream link) and
deletes its blobs through the same quarantine-first discipline as every other sweep
class. Only the repository row itself is retained, stamped as a purge tombstone — see
the name-burn note below. The [`openstorage purge-repo`](cli.md#openstorage-purge-repo)
command runs the same primitive synchronously for one repository.

- **Default:** unset (`0`) — soft-deleted repositories are kept forever.
- **Constraints:** a Go duration of at least `1h` (shorter values are startup errors:
  the purge always waits at least an hour after deletion so no in-flight pre-delete
  push can race it), and sweeping must be enabled
  ([`OPENSTORAGE_SWEEP_INTERVAL`](#openstorage_sweep_interval) > 0) — the purge rides
  the sweep schedule.
- **Fleet-upgrade prerequisite:** enable only once **every** node — serve replicas *and*
  hook-running git nodes — runs a build with the purge write fence (ref updates and
  pack/LFS registrations reject purged repositories). Older binaries do not check the
  fence, so on a mixed fleet a write accepted by an old node can commit after the
  purge's verification and be stranded behind the completed purge while reporting
  success. The same prerequisite applies to running
  [`openstorage purge-repo`](cli.md#openstorage-purge-repo). Leaving retention unset is
  safe on any fleet.
- **Purged repositories cannot be restored.** The repository row is kept as a permanent
  tombstone, so the burned `(org, url)` name is never released: JWT repo claims are
  URL-scoped and can outlive any retention window, and a name reuse would let a stale
  token authorize against the successor repository.
- Purged blob bytes move to quarantine first and are freed when the backups age out
  (sweeps are purge-aware even with every `*_DELETE_*` flag off — and independent of
  this variable, so `purge-repo`-minted fences get the same cleanup). On versioned S3
  buckets it fails startup exactly like the delete opt-ins. Each purge also write-fences
  the repository before touching anything, so a still-in-flight pre-delete push fails
  instead of racing the reclamation.

### `OPENSTORAGE_LFS_GC_INTERVAL`

Cadence of the unreferenced-LFS-object reclamation job
([#73](https://github.com/coder/openstorage/issues/73),
[ADR-42](../adr/042-lfs-reclamation-is-mark-and-sweep-over-reachable-pointer-files.md)).
Registered LFS objects are otherwise permanent, so a `git:write` holder
direct-PUTting tiny objects nobody references can exhaust the per-repo
object-count cap for everyone (`507` on every new object). Each run walks the
reachable pointer files of repositories holding LFS objects (bounded, resumable
— see the [operations guide](../how-to/storage-lifecycle-and-sweeping.md#reclaiming-unreferenced-lfs-objects)),
stamps rows no reachable pointer references, and — only with
[`OPENSTORAGE_LFS_GC_DELETE_ENABLED`](#openstorage_lfs_gc_delete_enabled) —
retires rows whose stamp survived a full
[grace window](#openstorage_lfs_gc_grace), refunding their object count and
quota bytes.

- **Default:** unset (`0`) — the job never runs. `0` is also the fleet-wide
  emergency stop.
- **Constraints:** a non-negative Go duration.
- Retirement removes registry rows only; the blobs become ordinary rowless
  orphans reaped by the
  [sweeper's LFS delete opt-in](#openstorage_sweep_delete_lfs). Arming
  retirement therefore
  [requires that opt-in](#openstorage_lfs_gc_delete_enabled) — refunded cap
  slots must never outrun physical deletion.

### `OPENSTORAGE_LFS_GC_GRACE`

How long an LFS object must stay **continuously** observed as unreferenced
before reclamation may retire its registry row. The clock starts when a scan
first finds no reachable pointer referencing the object and resets on every
liveness signal: a pointer becoming reachable, a re-upload, or a batch/PUT
answering "already stored" (the client may be about to push a pointer). The
retirement transaction re-checks the grace under the same lock registrations
take, so no signal can lose the race silently.

- **Default:** `24h`. **Constraints:** a positive Go duration. Keep it
  comfortably above your longest expected upload→push gap — a pointer pushed
  through the normal git-lfs flow always touches the object first, but raw-git
  pointer pushes only get the reachability scan's protection.
- With [`OPENSTORAGE_LFS_GC_DELETE_ENABLED`](#openstorage_lfs_gc_delete_enabled)
  set, the grace must also be at least the physical reap cadence —
  max([`OPENSTORAGE_SWEEP_AGE_THRESHOLD`](#openstorage_sweep_age_threshold),
  [`OPENSTORAGE_SWEEP_INTERVAL`](#openstorage_sweep_interval), the 1-hour
  quarantine retention floor) — a startup error otherwise: cap slots
  refunded faster than the reap (and its quarantine purge) frees their
  blobs would let one repository's upload→retire cycles grow the shared
  storage prefixes without bound.
- **Granularity:** local mode (SQLite) stamps marks at whole-second
  resolution, so the effective window rounds up — never down — and may run up
  to two seconds longer than configured. The window is never shorter than the
  configured grace on either backend.

### `OPENSTORAGE_LFS_GC_OBJECT_BUDGET`

Caps the work units one reclamation run may spend — each git object a
reachability walk lists charges one unit plus one more per 4 KiB of its raw
(inflated) size, so entry-stuffed trees and large blobs pay for the bytes git
must expand; each ref a scanned namespace holds and each registry row
settlement examines charges one unit; and every attempted repository,
including skips, charges at least one — the bound that keeps the job's cost
predictable on any repository shape. A repository whose own walk exceeds the
remaining budget is retried first next run with a full budget; one whose walk
exceeds the FULL budget is skipped loudly until the budget is raised. Nothing
is ever marked or retired from a partial walk.

- **Default:** `1000000`. **Constraints:** a positive integer.

### `OPENSTORAGE_LFS_GC_DELETE_ENABLED`

Arms retirement. Off, the job still scans, stamps, and reports what it *would*
retire (`rows_eligible` in the run report) — the same staged rollout as the
[sweep delete flags](#openstorage_sweep_delete_enabled): watch report-only runs
until the eligible rows are the junk you expect, then opt in.

- **Default:** `false`. **Constraints:** must parse as a boolean; requires
  [`OPENSTORAGE_LFS_GC_INTERVAL`](#openstorage_lfs_gc_interval) > 0 (arming a
  disabled job is a startup error).
- **Physical-deletion coupling** (security): retirement refunds the
  object-cap slot, but only the
  [`OPENSTORAGE_SWEEP_DELETE_LFS`](#openstorage_sweep_delete_lfs) reap deletes
  the stranded blob — decoupled, upload → grace → retire → re-upload cycles
  would grow `lfs/<repo>/` far past the cap that quota floors and sweep
  listings rely on to stay bounded. Arming retirement while the sweep
  schedule is live therefore requires that opt-in, plus a
  [grace](#openstorage_lfs_gc_grace) of at least the reap cadence, including
  the 1-hour quarantine retention floor every reap's backup copy is kept for
  (startup errors otherwise). The coupling is also OBSERVED at runtime
  ([#120](https://github.com/coder/openstorage/issues/120)): each
  destructive LFS sweep pass that completes its keyspace with zero errors
  records a durable fleet-wide watermark, and retirement refunds slots only
  while that watermark is fresher than the grace window — armed-but-failing
  sweeps, passes outrunning the reclamation cycle, and mid-flight guard
  demotions all freeze refunds automatically (`reap_watermark_stale` /
  `rows_deferred_reap_stale` in the run report). A fresh deployment stays
  report-only until its first clean destructive LFS sweep completes.
  Legacy-spelled rows (pre-ADR-41 `lfs/<repo>/<oid>` keys) are gated one
  step further: their blobs are only reaped when
  [`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`](#openstorage_sweep_delete_lfs_legacy)
  is also armed, so they retire only while that flag is set AND the fresh
  watermark records a legacy-armed sweep — otherwise they defer with
  `rows_deferred_legacy_unarmed` in the run report. With
  the sweep schedule stopped
  ([`OPENSTORAGE_SWEEP_INTERVAL=0`](#openstorage_sweep_interval), the
  emergency stop), the process starts but demotes retirement to report-only
  with a warning — nothing would reap the blobs meanwhile.
- **Mixed-fleet prerequisite:** every replica serving LFS requests must run a
  build with this feature before retirement is armed — older replicas answer
  "already stored" without clearing the reclamation stamp, so a grace-expired
  row could retire before that client pushes its pointer. If report-only
  scans ran during the roll, also wait one full
  [grace](#openstorage_lfs_gc_grace) after the last pre-touch replica stops
  before arming deletes: marks stamped mid-roll may already be grace-expired,
  and shutdown revokes none of the old replica's outstanding "stored"
  promises. See
  [the rollout guidance](../how-to/storage-lifecycle-and-sweeping.md#reclaiming-unreferenced-lfs-objects).

### `OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO`

Per-repository LFS object-count cap (issue
[#51](https://github.com/coder/openstorage/issues/51)): bytes bound row count
at nothing, so the registry's population needs its own bound. This override is
the operator escape hatch of [#73](https://github.com/coder/openstorage/issues/73):
a repository wedged at the cap by unreferenced junk can be unwedged immediately
by raising the cap, without waiting for a reclamation grace window.

- **Default:** `100000`. **Constraints:** a positive integer. The cap is
  enforced at registration inside the serve process, so set it identically on
  every replica.

### `OPENSTORAGE_ABSENCE_RECONCILE_INTERVAL`

Cadence of the registry-vs-storage absence reconciliation job
([#101](https://github.com/coder/openstorage/issues/101),
[ADR-43](../adr/043-absence-reconciliation-covers-registered-but-missing-bytes.md)).
Each pass pages registered rows — `lfs_objects` and `packs` — and
point-checks their bytes still physically exist in blob storage. While a
registered blob's bytes are absent (storage-layer loss; every software
deletion path quarantines first), the quota admission floor is deflated by
the absent size, hiding equally sized rowless legacy bytes; the pass makes
the loss visible again by charging it as a TTL'd restore cover and
invalidating cached floors — and it is the mechanism that *detects*
registered-but-missing bytes at all (a missing registered pack is an
integrity loss logged loudly: committed refs may reference it). See the
[operations guide](../how-to/storage-lifecycle-and-sweeping.md#reconciling-registered-but-missing-bytes).

- **Default:** unset (`0`) — the job never runs.
- **Constraints:** a non-negative Go duration, at most half of
  [`OPENSTORAGE_PACK_RESERVATION_TTL`](#openstorage_pack_reservation_ttl)
  (a startup error otherwise): covers are ordinary TTL'd reservations
  renewed once per pass, a pass's wall clock is capped at a quarter of the
  TTL, and periodic ticks that fire mid-pass coalesce — together those
  bound every tracked cover's renewal gap within the TTL, so a long scan
  can never let a live cover lapse.
- The job is non-destructive on every path: it only inserts/refreshes
  reservation rows and bumps the shared floor generation. With quota
  enforcement disabled
  ([`OPENSTORAGE_PACK_QUOTA_BYTES=0`](#openstorage_pack_quota_bytes)) it
  detects and reports without charging anything.
- **Degraded-renewal admission fence** (quota enforcement on): every
  successful pass stamps a renewal-health watermark. If a pass's cover
  renewal FAILS while that watermark is absent or older than the
  cover-lapse safety bound (the TTL minus this interval minus twice the
  pass wall clock — a failure is harmless only while covers survive
  until the NEXT attempt, and the watermark can trail the touch it
  records by up to one more pass), covers may lapse un-renewed and quota
  arithmetic would silently under-count them — the job then fails finite
  quota-gated writes closed (pushes, LFS uploads, adoptions answer
  "quota accounting temporarily degraded, retry later") until a healthy
  pass completes one full recovery rotation of both registries.
  Transient renewal blips within the safety bound never fence; internal
  restore/cover reservations are always exempt so recovery can proceed.
  A SUCCESSFUL renewal whose prior watermark exists but is a full TTL
  old fences too — the strict-TTL touch cannot resurrect covers that
  already lapsed — so a fleet paused past the TTL, or quota re-enabled
  after an off period whose earlier passes had stamped, runs one fenced
  recovery rotation before finite quota-gated writes resume. An ABSENT
  watermark (the first pass after enabling the job, or after upgrading
  to a version that stamps it) is not lapse evidence by itself: that
  pass fences only on positive evidence — a durable lapse-evidence
  marker (recorded whenever admission-time GC or the reservation sweep
  deletes an expired cover), a physically present expired cover, or an
  evidence check that itself fails — and otherwise stamps health with
  admissions left open, so an ordinary first pass after enablement or
  upgrade does NOT block writes.

  **Upgrade ordering (mixed fleets).** The fence check and the
  lapse-evidence markers run only in new binaries: during a rolling
  upgrade, a write routed to a pre-upgrade replica reserves WITHOUT the
  fence check (spending the deflated headroom an armed fence closes on
  new replicas), and a pre-upgrade replica's expired-reservation GC
  deletes a lapsed cover without recording evidence — silently
  defeating the first health stamp's probe afterwards. Both guarantees
  are fleet-wide only once every replica runs a fence-aware binary.
  Enable this job only after the whole fleet is upgraded; when
  upgrading a fleet that already runs it, complete the roll within one
  [`OPENSTORAGE_PACK_RESERVATION_TTL`](#openstorage_pack_reservation_ttl)
  (healthy renewal re-TTLs covers every pass, so a prompt roll leaves
  no expired cover for pre-upgrade GC to consume). The serve boot log
  warns while reconciliation runs with quota enforcement as a reminder
  of this prerequisite (see ADR-43).

### `OPENSTORAGE_ABSENCE_RECONCILE_BUDGET`

Caps the work units one reconciliation pass may charge: one per registry row
examined plus one per extra registration or twin-spelling probe — each unit
is at most one blob-store HEAD or registry point read. A budget-limited pass
parks its keyset cursor and the next pass resumes strictly after it, so
arbitrarily large registries reconcile across passes. Within a pass, up to
half the budget first **re-settles already-discovered losses** (keeping
their covers refreshed every pass, independent of the rotation), then the
two registry rotations split the remainder with an alternating leader; a
one-hour wall-clock cap bounds the pass even when a degraded blob store
stretches individual probes.

- **Default:** `100000`. **Constraints:** an integer of at least `100`
  (one row can cost up to three units, so degenerate budgets cannot
  guarantee progress on both registries).
- Discovered losses are covered by ONE aggregate reservation row per
  affected repository and registry class, and the cover rows themselves
  are the durable tracking state
  ([#125](https://github.com/coder/openstorage/issues/125)): the renewal
  phase lists the live rows and re-charges each at one work unit per row,
  entirely database-derived, so the per-repo reservation-row cap cannot
  bind and any replica keeps every cover alive. Each full rotation over a
  repository's rows re-derives its exact deficit total — growing the live
  cover in place, re-minting the key with the exact smaller total when
  heals shrink it (`covers_rotated`; the stale row is deleted), and
  releasing it outright when everything healed (`covers_released`). Shrink
  therefore lags by at most one rotation (conservative), never renewal.
- **Sizing:** keep the budget above twice the live cover-row population.
  Renewal never lapses on budget — it always offers the full cover
  listing, squeezing discovery when it overruns its allocation
  (`renewal_over_budget`) — but a squeezed rotation slows loss discovery
  and heal shedding. Rotation cursors, traversal bookkeeping, and the
  alternating-leader parity are all fleet-persistent (`job_scan_state`),
  so multi-replica PostgreSQL fleets share one rotation regardless of
  which replica River schedules.

### `OPENSTORAGE_REPACK_THRESHOLD`

Registered-pack count at which a post-write trigger schedules a background repack fold for
that repository. Repack packs the union of the snapshotted packs' objects into one
consolidated pack — the registered object set never shrinks.

- **Default:** `10`. **`0` disables this process's triggers and follow-ups**
  (already-queued durable jobs still run); `1` is rejected (folding fewer than two
  packs is a no-op, so it would busy-loop the trigger); negative or malformed values
  are startup errors.
- The threshold is **per process/replica**, for both its post-write triggers and the
  follow-up checks of jobs it consumes. Setting one push node to `2` does not set
  other consumers of the same River queue to `2`. Mixed-threshold fleets do not
  promise preservation of a producer's lower threshold.
- With the same enabled threshold on every eligible consumer, pack counts target
  below that threshold, not necessarily one pack. Exactly-one convergence requires
  all those consumers to use `2`, plus quiescent writes and successful fold progress
  within the [fold budgets](../how-to/storage-lifecycle-and-sweeping.md#background-repack).

### `OPENSTORAGE_SYNC_STEAL_LEGACY`

Upstream-sync claims are leased: a run that dies mid-sync leaves a claim the next run
steals automatically once the lease expires. Claims written by **pre-lease builds** carry
no lease and none of the holder guards that make stealing safe; new replicas adopt them
under one grace period but will not steal them without this opt-in.

- **Default:** `false` (adopted legacy claims are never stolen). Malformed values stay
  disabled.
- **Enable only after the whole fleet runs a lease-aware build** (same fleet prerequisite
  as [`OPENSTORAGE_SWEEP_DELETE_ENABLED`](#openstorage_sweep_delete_enabled)): from then
  on, legacy claims can only be pre-upgrade crash leftovers, and stealing them heals those
  wedges without operator row-surgery. Safe to leave enabled permanently afterwards.

## Internal and test variables

Variables outside the operator surface, listed for completeness — never set these manually:

- `OPENSTORAGE_GIT_REPO`, `OPENSTORAGE_GIT_REFS_POLICY`, `OPENSTORAGE_GIT_OPS`,
  `OPENSTORAGE_GIT_ATOMIC`, `OPENSTORAGE_GIT_PACK_QUOTA`,
  `OPENSTORAGE_GIT_PACK_RESERVATION_TTL`, `OPENSTORAGE_GIT_VERIFY_STATE`,
  `OPENSTORAGE_GIT_VERIFY_SIG_BUDGET` — per-request plumbing the server sets for the
  `openstorage hook` subprocess git re-execs.
- `OPENSTORAGE_ASKPASS_USERNAME`, `OPENSTORAGE_ASKPASS_PASSWORD` — set by the upstream-sync
  worker for its own git credential-askpass subprocess.
- `OPENSTORAGE_TEST_MODE`, `OPENSTORAGE_TEST_CRASH`, `OPENSTORAGE_TEST_CAS_DROP`,
  `OPENSTORAGE_TEST_CAS_DROP_HARD`, `OPENSTORAGE_TEST_HOOK_DELAY_MS`,
  `OPENSTORAGE_TEST_VERIFY_SPAWN_LOG` — crash/fault injection and observability knobs
  for the e2e suites; the server refuses test-mode requests outside test mode.
- `OPENSTORAGE_TEST_BACKEND`, `OPENSTORAGE_SDK_DIR` — test-lane selection (see the
  [testing reference](testing.md)) and the SDK-compatibility
  suite's vendor checkout location.

The Docker Compose file has its own host-port overrides (`PGPORT`, `S3PORT`,
`NODEA_PORT`, `NODEB_PORT`, `CS_KEYS_DIR`) — see the
[`docker-compose.yml`](../../docker-compose.yml) header.
