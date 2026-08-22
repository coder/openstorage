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
| [`CODESTORAGE_PACK_QUOTA_BYTES`](#codestorage_pack_quota_bytes) | 10 GiB | Per-repo storage quota, packs + LFS (`0` disables) |
| [`CODESTORAGE_LFS_STAGING_BYTES`](#codestorage_lfs_staging_bytes) | 10 GiB | Per-replica cap on in-flight LFS upload staging (`0` disables) |
| [`CODESTORAGE_PACK_RESERVATION_TTL`](#codestorage_pack_reservation_ttl) | `1h` | Lifetime of a crashed writer's quota reservation |
| [`CODESTORAGE_SWEEP_INTERVAL`](#codestorage_sweep_interval) | `1h` | Orphan-pack sweep cadence (`0` disables sweeping) |
| [`CODESTORAGE_SWEEP_AGE_THRESHOLD`](#codestorage_sweep_age_threshold) | `24h` | Minimum blob age before an unregistered pack counts as an orphan |
| [`CODESTORAGE_SWEEP_DELETE_ENABLED`](#codestorage_sweep_delete_enabled) | `false` | Opt-in: actually delete general orphans (list-only otherwise) |
| [`CODESTORAGE_SWEEP_DELETE_SUPERSEDED`](#codestorage_sweep_delete_superseded) | `true` | Reap blobs a repack fold provably superseded |
| [`CODESTORAGE_SWEEP_DELETE_LFS`](#codestorage_sweep_delete_lfs) | `false` | Opt-in: actually delete orphan LFS objects (list-only otherwise) |
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

## Pack quota

### `CODESTORAGE_PACK_QUOTA_BYTES`

Per-repository storage-bytes quota enforced at every external storage writer: git pushes
that would grow a repo's `packs/` usage past the quota are refused at pre-receive before
anything is stored, REST pack writes (commit-pack, merge, notes, fork) are refused with
a `403` envelope, and Git LFS uploads ([#51](https://github.com/coder/code-storage/issues/51))
are refused with per-object `507` batch errors before any transfer URL is issued (the
verified upload transfer re-checks the actual `Content-Length`, so under-declared batch
sizes cannot bypass the gate).

- **Default:** 10 GiB (`10737418240`). **`0` disables enforcement**; negative or malformed
  values are startup errors.
- **Namespace roll-up:** a repository and its `+ephemeral` / `+import` namespaces
  ([docs/05](../05-git-protocol-and-namespaces.md)) share one quota, accounted against the
  base repository. LFS objects always store under the base repository's prefix.
- Admission is a durable reservation ledger in the ref store, atomic across concurrent
  pushes and replicas: two pushes that individually fit but together exceed the quota —
  exactly one wins. LFS uploads charge the same ledger (reserve at batch, settle at
  verified upload completion).
- Background repack maintenance is **not** quota-gated (an at-quota repository could never
  compact itself otherwise), and repack preserves the registered object set — it does not
  reclaim unreachable objects. LFS downloads are never quota-gated.

### `CODESTORAGE_LFS_STAGING_BYTES`

Node-wide cap on the aggregate bytes of Git LFS uploads staging on one replica at a
time. LFS uploads go through the verified proxy transfer
([#51](https://github.com/coder/code-storage/issues/51)) and stage their full declared
`Content-Length` under the replica's `$TMPDIR` before verification and publication —
the per-repository quota above bounds neither the node (it is per repo) nor anything
when disabled, so this is the replica's disk-protection bound. Uploads that do not fit
right now answer `429` with `Retry-After`; an object larger than the whole budget
answers `413`.

- **Default:** 10 GiB (`10737418240`) — one default-quota-sized object can always
  stage. **`0` disables the bound**; negative or malformed values are startup errors.
- Size it below the free space you guarantee `$TMPDIR`, minus headroom for git push
  quarantines and fork closures, which stage there too.
- Each object additionally allows only one in-flight upload per replica (concurrent
  identical PUTs answer `429`).

### `CODESTORAGE_PACK_RESERVATION_TTL`

How long a crashed writer's quota reservation keeps gating admission (Go duration, e.g.
`30m`, `2h`). Admission ignores rows older than the TTL immediately; the sweeper's
periodic pruning of expired rows is garbage collection, not what re-opens headroom.

- **Default:** `1h`. **Constraints:** must be positive; a zero TTL would expire live
  reservations instantly, silently reopening the overshoot the ledger closes.
- Must comfortably exceed the longest plausible server-side pack upload — the default
  clears a default-quota-sized (10 GiB) upload at ~3 MB/s.
- LFS upload reservations (charged at batch time, settled at upload completion) use the
  same TTL, so it must also cover the gap between a client's batch request and the end
  of its slowest object transfer.

## Maintenance jobs

Background jobs run through a queue seam: durable [River](https://riverqueue.com) jobs on
Postgres, in-process in local mode.

### `CODESTORAGE_SWEEP_INTERVAL`

Interval between orphan-pack sweeps (Go duration). The sweeper diffs the blob store's
`packs/` and `lfs/` listings against their authoritative registries and reports — and
only when explicitly enabled, deletes — unregistered blobs older than the age threshold.
It also prunes expired quota reservations on the same cadence.

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

### `CODESTORAGE_SWEEP_DELETE_LFS`

Delete mode for **LFS orphans**: `lfs/` blobs with no LFS registry row, older than the
age threshold. Deliberately separate from `CODESTORAGE_SWEEP_DELETE_ENABLED`: the LFS
registry ships with [#51](https://github.com/coder/code-storage/issues/51), so **every
LFS object uploaded by an older build has no row** and would classify as an orphan —
a fleet already running general deletes must not reap its pre-existing LFS population
on upgrade. Stored objects are adopted into the registry by write-scoped touches
(upload batches naming them, re-uploads of the same content).

- **Default:** `false` (list-only). **Constraints:** must parse as a boolean.
- Enable only after list-only [sweep reports](../how-to/storage-lifecycle-and-sweeping.md#lfs-orphans-and-pre-registry-objects)
  stop naming objects that should live.
- Same quarantine-first disposal and heal pass as the pack classes; on versioned S3
  buckets it fails startup exactly like the general delete opt-in.

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
