# Configuration Reference

Every `CODESTORAGE_*` environment variable the `codestorage` binary reads, with defaults,
validation constraints, and interaction notes. Source of truth:
[`cmd/codestorage/config.go`](../../cmd/codestorage/config.go). Flags are documented in the
[CLI reference](cli.md).

Resolution precedence per setting: **explicit flag > canonical `CODESTORAGE_*` env var >
legacy alias > `--data-dir` derivation (local mode) > built-in default**. Every value is
validated at startup — a malformed value is a fail-fast startup error naming the variable,
never a silently-applied default.

## Summary

| Variable | Default | Purpose |
|---|---|---|
| [`CODESTORAGE_PG_URL`](#codestorage_pg_url) | — (required*) | Database URL: refs + metadata (`postgres://`, `postgresql://`, or `sqlite://`) |
| [`CODESTORAGE_S3_URL`](#codestorage_s3_url) | — (required*) | Pack storage URL (`s3://` or `file://`) |
| [`CODESTORAGE_DATA_DIR`](#codestorage_data_dir) | — | Local-mode data directory; derives the two URLs above |
| [`CODESTORAGE_LISTEN`](#codestorage_listen) | `127.0.0.1:0` | Listen address |
| [`CODESTORAGE_ORG`](#codestorage_org) | — (required) | Org name (JWT `iss` claim) |
| [`CODESTORAGE_ORG_PUBKEYS`](#codestorage_org_pubkeys) | — (required) | PEM file with the org's public keys |
| [`CODESTORAGE_BASE_URL`](#codestorage_base_url) | derived from listener | External base URL for `http_url` values |
| [`CODESTORAGE_RATE_RPS`](#codestorage_rate_rps--codestorage_rate_burst) | unset (disabled) | Per-client-IP rate limit: sustained requests/second |
| [`CODESTORAGE_RATE_BURST`](#codestorage_rate_rps--codestorage_rate_burst) | unset (disabled) | Rate-limit token-bucket depth |
| [`CODESTORAGE_TRUSTED_PROXIES`](#codestorage_trusted_proxies) | unset | CIDR allowlist of reverse proxies for rate-limit client identity |
| [`CODESTORAGE_WEBHOOK_ALLOW_PRIVATE`](#codestorage_webhook_allow_private) | `false` | Permit private-network webhook receivers (metadata and redirects stay blocked) |
| [`CODESTORAGE_SECRETS_KEY`](#codestorage_secrets_key) | unset (plaintext) | AES-256 key list for at-rest encryption of stored webhook secrets and upstream credentials |
| [`CODESTORAGE_SECRETS_MODE`](#codestorage_secrets_mode) | `encrypt` | `decrypt-only` stages the key fleet-wide before encrypted writes begin |
| [`CODESTORAGE_PACK_QUOTA_BYTES`](#codestorage_pack_quota_bytes) | 10 GiB | Per-repo pack-bytes quota (`0` disables) |
| [`CODESTORAGE_PACK_RESERVATION_TTL`](#codestorage_pack_reservation_ttl) | `1h` | Lifetime of a crashed writer's quota reservation |
| [`CODESTORAGE_SWEEP_INTERVAL`](#codestorage_sweep_interval) | `1h` | Orphan-pack sweep cadence (`0` disables sweeping) |
| [`CODESTORAGE_SWEEP_AGE_THRESHOLD`](#codestorage_sweep_age_threshold) | `24h` | Minimum blob age before an unregistered pack counts as an orphan |
| [`CODESTORAGE_SWEEP_DELETE_ENABLED`](#codestorage_sweep_delete_enabled) | `false` | Opt-in: actually delete general orphans (list-only otherwise) |
| [`CODESTORAGE_SWEEP_DELETE_SUPERSEDED`](#codestorage_sweep_delete_superseded) | `true` | Reap blobs a repack fold provably superseded |
| [`CODESTORAGE_REPACK_THRESHOLD`](#codestorage_repack_threshold) | `10` | Registered-pack count that triggers a background repack (`0` disables) |

\* Required unless derived from `CODESTORAGE_DATA_DIR` / `--data-dir` (local mode).

## Storage

### `CODESTORAGE_PG_URL`

Database URL for the ref authority (CAS) and metadata store.

- **Schemes:** `postgres://` or `postgresql://` (production, any number of stateless
  replicas) or `sqlite://<absolute-path>` (local mode, single node).
- **Legacy aliases** (scanned left to right, first non-empty wins): `SPIKE_PG_URL`, `PG_URL`.
- **Default:** none. Startup fails unless set explicitly or derived from the data dir
  (`sqlite://<dir>/codestorage.db`).
- On Postgres, schema setup (including the durable [River](https://riverqueue.com) job
  tables) runs automatically at startup and is advisory-lock-serialized, so concurrently
  booting replicas are safe.

### `CODESTORAGE_S3_URL`

Immutable pack storage URL, opened through [gocloud.dev](https://gocloud.dev/howto/blob/).

- **Schemes:** `s3://bucket?region=...` (S3 or MinIO — add
  `endpoint=http://...&s3ForcePathStyle=true` for MinIO) or `file://<absolute-dir>`
  (local mode).
- **Legacy aliases:** `SPIKE_S3_URL`, `BLOB_URL`.
- **Default:** none. Startup fails unless set explicitly or derived from the data dir
  (`file://<dir>/blobs`).
- S3 credentials come from the standard AWS chain (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, shared config, instance roles).
- Delete-mode sweeping interacts with bucket versioning — see
  [`CODESTORAGE_SWEEP_DELETE_ENABLED`](#codestorage_sweep_delete_enabled).

### `CODESTORAGE_DATA_DIR`

Local-mode data directory (the `--data-dir` flag wins over the env var). Setting it derives
every storage slot **not set explicitly**:

- database: `sqlite://<dir>/codestorage.db`
- packs: `file://<dir>/blobs`
- cache: `<dir>/cache`

Explicit `CODESTORAGE_PG_URL` / `CODESTORAGE_S3_URL` / `--cache-dir` always win over the
derivation. The path is made absolute at startup (git hooks re-exec the binary from a
different working directory). Local mode is intentionally single-node — see ADR-13 in
[docs/11](../11-local-backends-implementation-notes.md).

## Server identity and network

### `CODESTORAGE_LISTEN`

Listen address for the single HTTP listener (REST API + git smart-HTTP + `/healthz`).
The `--listen` flag (or its legacy alias `--addr`) wins. **Default:** `127.0.0.1:0`
(an ephemeral port, printed as `listening on ...` at startup).

### `CODESTORAGE_ORG`

**Required.** The organization name — the JWT `iss` claim every token must carry
([docs/02](../02-authentication.md)). `serve` always runs single-org mode: the REST API and
the git smart-HTTP path authenticate against the same org-key verifier, and the org row is
bootstrapped from this value at startup.

### `CODESTORAGE_ORG_PUBKEYS`

**Required.** Path to a PEM file with one or more public key blocks (PKIX) that verify the
org's JWTs. Every block that PEM-decodes is validated at startup (must be a `PUBLIC KEY`
block of a supported family) — but parsing stops silently at the first undecodable data, so
a truncated block appended after a valid key is dropped rather than rejected. After a key
rotation, verify a token signed with the newly added key is accepted instead of trusting
startup success.
Generate a keypair with [`codestorage keygen`](cli.md#codestorage-keygen); allowed JWS
algorithms are `ES256`, `RS256`, `EdDSA`, `PS256`, `ES384`, `ES512` (`none` and `HS*` are
always rejected).

### `CODESTORAGE_BASE_URL`

Externally reachable base URL (`http(s)://host[:port]`, no query/fragment; a trailing slash
is stripped) used for the `http_url` values in API responses. **Default:** derived from the
bound listener address — which is unroutable when binding `0.0.0.0`, so set this behind any
real deployment or reverse proxy.

## Rate limiting

### `CODESTORAGE_RATE_RPS` / `CODESTORAGE_RATE_BURST`

Per-client-IP token-bucket rate limit applied to the REST surface **and** — with separate
bucket state — the git smart-HTTP surface. Exceeding it yields `429` with `Retry-After`.

- **Default:** both unset — limiting disabled.
- **Constraints (fail-fast at startup):** `RPS` must be a non-negative finite number;
  `BURST` a non-negative integer. Setting `RPS > 0` requires `BURST >= 1` and vice versa —
  half-set pairs are startup errors. `RPS=0` with `BURST` unset/`0` explicitly disables.
- The git surface raises bucket depths below 8 to 8: one git operation is several
  smart-HTTP requests, and a burst too small for a complete exchange would make every
  clone/fetch/push permanently fail.
- Client identity defaults to the TCP `RemoteAddr`; forwarded headers are **not** trusted
  unless `CODESTORAGE_TRUSTED_PROXIES` is set. Behind a proxy that pools backend
  connections, all clients otherwise share one bucket.

### `CODESTORAGE_TRUSTED_PROXIES`

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

### `CODESTORAGE_WEBHOOK_ALLOW_PRIVATE`

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

### `CODESTORAGE_SECRETS_KEY`

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
   hook-running git nodes — with `CODESTORAGE_SECRETS_KEY` unset. Plaintext behavior is
   unchanged (see the upgrade-ordering note below for why this must come first).
2. **Stage the key, decrypt-only:** set `CODESTORAGE_SECRETS_KEY` together with
   [`CODESTORAGE_SECRETS_MODE=decrypt-only`](#codestorage_secrets_mode) fleet-wide. Every
   process loads the key for reading but keeps writing plaintext and performs no rewrites,
   so replicas restarted earlier never produce a value the rest cannot read.
3. **Switch to encrypt:** once every replica runs with the staged key, drop
   `CODESTORAGE_SECRETS_MODE` (or set it to `encrypt`) fleet-wide. Encrypted writes and
   lazy re-encryption begin; replicas restarted earlier in this rollout write envelopes
   the not-yet-restarted ones already decrypt, courtesy of step 2.

Alternatively, a drained/atomic cutover (stop all replicas, set the key, start) collapses
steps 2–3.

**Key rotation.** The same staging applies when introducing a new key — no process may
ever encounter a key it cannot decrypt:

1. **Stage the new key fleet-wide, as a decrypt-only list entry:**
   `CODESTORAGE_SECRETS_KEY="<old-key>,<new-key>"`. Writes still use the old key, but
   every restarted process can already decrypt both, so ordering within the rollout does
   not matter.
2. **Promote, fleet-wide:** `CODESTORAGE_SECRETS_KEY="<new-key>,<old-key>"`. Replicas
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
and back the keys up outside the database. Recovery never requires the lost key: replacing
a credential's password (`PUT`) or deleting it, and deleting + re-registering a webhook,
work on undecryptable rows — those paths never read the stored value. Startup logs one line
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

### `CODESTORAGE_SECRETS_MODE`

Staging switch for [`CODESTORAGE_SECRETS_KEY`](#codestorage_secrets_key). **Default:
`encrypt`** (normal operation). `decrypt-only` loads the key list for reading but keeps
writing plaintext and performs no lazy rewrites — the first-enable staging mode described
above, used while a rolling restart distributes the key so that no envelope exists before
every replica can decrypt it. Setting `decrypt-only` without `CODESTORAGE_SECRETS_KEY`,
or any other value, is a startup error. Do not run `decrypt-only` indefinitely: it stores
new secrets plaintext.

## Pack quota

### `CODESTORAGE_PACK_QUOTA_BYTES`

Per-repository pack-bytes quota enforced at every external pack writer: git pushes that
would grow a repo's `packs/` usage past the quota are refused at pre-receive before
anything is stored, and REST pack writes (commit-pack, merge, notes, fork) are refused with
a `403` envelope.

- **Default:** 10 GiB (`10737418240`). **`0` disables enforcement**; negative or malformed
  values are startup errors.
- **Namespace roll-up:** a repository and its `+ephemeral` / `+import` namespaces
  ([docs/05](../05-git-protocol-and-namespaces.md)) share one quota, accounted against the
  base repository.
- Admission is a durable reservation ledger in the ref store, atomic across concurrent
  pushes and replicas: two pushes that individually fit but together exceed the quota —
  exactly one wins.
- Background repack maintenance is **not** quota-gated (an at-quota repository could never
  compact itself otherwise), and repack preserves the registered object set — it does not
  reclaim unreachable objects.

### `CODESTORAGE_PACK_RESERVATION_TTL`

How long a crashed writer's quota reservation keeps gating admission (Go duration, e.g.
`30m`, `2h`). Admission ignores rows older than the TTL immediately; the sweeper's
periodic pruning of expired rows is garbage collection, not what re-opens headroom.

- **Default:** `1h`. **Constraints:** must be positive; a zero TTL would expire live
  reservations instantly, silently reopening the overshoot the ledger closes.
- Must comfortably exceed the longest plausible server-side pack upload — the default
  clears a default-quota-sized (10 GiB) upload at ~3 MB/s.

## Maintenance jobs

Background jobs run through a queue seam: durable [River](https://riverqueue.com) jobs on
Postgres, in-process in local mode.

### `CODESTORAGE_SWEEP_INTERVAL`

Interval between orphan-pack sweeps (Go duration). The sweeper diffs the blob store's
`packs/` listing against the authoritative pack registry and reports — and only when
explicitly enabled, deletes — unregistered blobs older than the age threshold. It also
prunes expired quota reservations on the same cadence.

- **Default:** `1h`. **`0` disables sweeping** (a strictly restorative quarantine-heal pass
  still runs once at startup); negative values are startup errors.

### `CODESTORAGE_SWEEP_AGE_THRESHOLD`

Minimum blob age before an unregistered pack counts as an orphan (Go duration). The
threshold is the only concurrency guard needed: in-flight pushes register their packs
within a request timeout, far below any sane threshold.

- **Default:** `24h`. **Constraints:** must be positive.

### `CODESTORAGE_SWEEP_DELETE_ENABLED`

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

### `CODESTORAGE_SWEEP_DELETE_SUPERSEDED`

Delete mode for the **narrow superseded-by-repack** class: blobs whose registry rows a
repack fold provably removed (durable `pack_retained` records) are reaped after the age
threshold and their retained bytes returned to the repo's quota headroom.

- **Default:** `true` (`false` disables). **Constraints:** must parse as a boolean.
- Unlike the general delete opt-in above, a versioned S3 bucket does not fail startup —
  each sweep run downgrades to list-only instead (the heal pass still runs).

### `CODESTORAGE_REPACK_THRESHOLD`

Registered-pack count at which a post-write trigger schedules a background repack fold for
that repository. Repack packs the union of the snapshotted packs' objects into one
consolidated pack — the registered object set never shrinks.

- **Default:** `10`. **`0` disables triggering** (already-queued durable jobs still run);
  `1` is rejected (folding fewer than two packs is a no-op, so it would busy-loop the
  trigger); negative or malformed values are startup errors.
- Pack counts converge below the threshold, not necessarily to a single pack.

## Internal and test variables

Variables outside the operator surface, listed for completeness — never set these manually:

- `CODESTORAGE_GIT_REPO`, `CODESTORAGE_GIT_REFS_POLICY`, `CODESTORAGE_GIT_OPS`,
  `CODESTORAGE_GIT_ATOMIC`, `CODESTORAGE_GIT_PACK_QUOTA`,
  `CODESTORAGE_GIT_PACK_RESERVATION_TTL` — per-request plumbing the server sets for the
  `codestorage hook` subprocess git re-execs.
- `CODESTORAGE_ASKPASS_USERNAME`, `CODESTORAGE_ASKPASS_PASSWORD` — set by the upstream-sync
  worker for its own git credential-askpass subprocess.
- `CODESTORAGE_TEST_MODE`, `CODESTORAGE_TEST_CRASH`, `CODESTORAGE_TEST_CAS_DROP`,
  `CODESTORAGE_TEST_CAS_DROP_HARD`, `CODESTORAGE_TEST_HOOK_DELAY_MS` — crash/fault
  injection for the e2e suites; the server refuses test-mode requests outside test mode.
- `CODESTORAGE_TEST_BACKEND`, `CODESTORAGE_SDK_DIR` — test-lane selection (see the
  [testing notes](../11-local-backends-implementation-notes.md)) and the SDK-compatibility
  suite's vendor checkout location.

The Docker Compose file has its own host-port overrides (`PGPORT`, `MINIOPORT`,
`NODEA_PORT`, `NODEB_PORT`, `CS_KEYS_DIR`) — see the
[`docker-compose.yml`](../../docker-compose.yml) header.
