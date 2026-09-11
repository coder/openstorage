# CLI Reference

The `openstorage` binary bundles the server and its operational helpers:

```
openstorage <serve|hook|keygen|mint|signing-key|list-packs|purge-repo|version> [flags]
```

Sources of truth: [`cmd/openstorage/main.go`](../../cmd/openstorage/main.go) and
[`cmd/openstorage/signingkey.go`](../../cmd/openstorage/signingkey.go). Environment
variables are documented in the [configuration reference](configuration.md).

Subcommands that open the stores (`serve`, `signing-key`, `list-packs`, `purge-repo`)
share one storage resolution: explicit env URLs win, then `--data-dir` /
`$OPENSTORAGE_DATA_DIR` (local mode) derives the missing slots. Every backend scheme
works through those env URLs, including the experimental
[`s3wal://`](configuration.md#openstorage_pg_url) zero-infra pair: `signing-key` and
`purge-repo` open the bucket-backed metadata/ref stores from the database URL, and
`list-packs` reads the blob side through the `s3://` URL (it never opens the database
URL).

## `openstorage serve`

Runs one stateless node: the `/api/v1` REST API (plus the `/api/repos/{repo_name}/...`
aliases and `/api/repo-urls/{repo_id}`), the git smart-HTTP path with its LFS batch API,
and an unauthenticated `GET /healthz` liveness probe — all on a single listener. Prints
`listening on <addr>` once bound.

| Flag | Default | Meaning |
|---|---|---|
| `--listen` | `$OPENSTORAGE_LISTEN`, then `127.0.0.1:0` | Listen address |
| `--addr` | — | Legacy alias for `--listen` (`--listen` wins) |
| `--cache-dir` | data-dir derivation, then `$TMPDIR/codestorage-cache` | Rebuildable bare-repo cache directory |
| `--data-dir` | `$OPENSTORAGE_DATA_DIR` | Local-mode data directory; derives database/blob/cache locations for slots not set explicitly |

Requires `OPENSTORAGE_ORG` + `OPENSTORAGE_ORG_PUBKEYS` (single-org mode) and a resolvable
database + blob URL — either explicit env URLs or local mode via `--data-dir`. All
validation happens at startup; see the [configuration reference](configuration.md) for
every knob (rate limiting, quotas, sweeping, repack).

```sh
# Local mode: SQLite + plain files under one directory, no other services.
openstorage serve --listen 127.0.0.1:8080 --data-dir ~/cs-data

# Production mode: Postgres + S3 via env URLs.
OPENSTORAGE_PG_URL='postgres://...' OPENSTORAGE_S3_URL='s3://...' openstorage serve

# Zero-infra mode (experimental): refs + metadata + packs in one pre-created bucket.
OPENSTORAGE_PG_URL='s3wal://bucket?endpoint=...' OPENSTORAGE_S3_URL='s3://bucket?endpoint=...&s3ForcePathStyle=true' openstorage serve
```

## `openstorage hook`

Internal. `openstorage hook <pre-receive|update> [args...]` is installed by the server
into cached bare repositories and re-exec'd by git during pushes: `pre-receive` publishes
and registers the quarantined pack (enforcing the pack quota), `update` runs the ref-CAS
and ref policies ([git protocol](../explanation/git-protocol-and-namespaces.md)). It reads connection
and per-request environment inherited from the serve process — there is no reason to run
it manually.

## `openstorage keygen`

Generates an org keypair for JWT signing.

| Flag | Default | Meaning |
|---|---|---|
| `--out` | required | Output path prefix: writes `<out>.pem` (PKCS#8 private key, created with mode `0600`) and `<out>.pub.pem` (PKIX public key, created with mode `0644`), subject to umask. Without `--if-missing`, existing files are overwritten but keep their old modes |
| `--alg` | `ES256` | Fresh key algorithm: one of `ES256`, `RS256`, `EdDSA`, `PS256`, `ES384`, `ES512`. Ignored when `--if-missing` reuses an existing pair |
| `--if-missing` | `false` | Generate only when both files are absent; otherwise validate and reuse a matching pair without writes |

Point `OPENSTORAGE_ORG_PUBKEYS` at the `.pub.pem` file (it may hold several concatenated
public key blocks). Never commit either file.

```sh
openstorage keygen --out /tmp/cs-key   # /tmp/cs-key.pem + /tmp/cs-key.pub.pem
```

For repeatable bootstrap, use a directory you control:

```sh
mkdir -m 700 ./keys
openstorage keygen --if-missing --out ./keys/org
openstorage keygen --if-missing --out ./keys/org   # validates; no rotation or rewrite
```

With `--if-missing`, both existing files must be regular files, not symlinks. The private
file must have no group or other permission bits; unsafe permissions cause an error,
not an automatic chmod. It must contain a usable PKCS#8 or SEC1 signing key, and the
public file must contain exactly one matching PKIX public key, not a multi-key bundle.
Reuse preserves file bytes, permissions, and modification times. `--alg` only controls
fresh generation: even a different or unsupported value is ignored on successful reuse.
The command prints only the key paths, never the key material.

Missing halves, corrupt keys, mismatches, and unsafe existing files fail without writes.
Fresh files are created exclusively, so a concurrently created file is not overwritten.
The two-file creation is **not atomic**: interruption or an I/O error can leave a partial
pair, which later runs reject rather than silently replace. Inspect and repair that state
explicitly. Do not edit or replace the files or their containing directory while the
command runs; it does not lock the directory against other processes.

## `openstorage mint`

Mints a JWT signed with the org's private key — the local-mode stand-in for your token
service. Claims follow [auth spec](authentication.md).

| Flag | Default | Meaning |
|---|---|---|
| `--key` | required | PEM path of the org private key (PKCS#8 or SEC1) |
| `--iss` | required | Issuer claim — the org name (`OPENSTORAGE_ORG`) |
| `--repo` | required | Repo claim: the target repo, or `org` for org-level operations |
| `--scopes` | `git:read,git:write` | Comma-separated scopes: `git:read`, `git:write`, `repo:write`, `org:read` |
| `--refs` | — | Optional ref-policy JSON, e.g. `[["refs/heads/main",["no-force-push"]],["*",[]]]` — only the JSON tuple structure is validated at mint time |
| `--alg` | inferred from key type | JWS signature algorithm |
| `--ttl` | `1h` | Token lifetime (must be positive) |

Ref-policy patterns support an exact ref name, a standalone `*` (matches everything), or a
single trailing `*` after a `/` (prefix match, e.g. `refs/heads/feature/*`). Any other `*`
placement is **not** a glob — it falls through to exact comparison and never matches, so a
mistyped pattern like `refs/heads/main*` silently leaves the ref unpoliced. Evaluation
semantics: [auth spec](authentication.md).

```sh
TOKEN=$(openstorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git"
```

## `openstorage signing-key`

Manages the org's commit-signing key registry for the `verify-sig` ref-policy op
([auth spec](authentication.md)). Upstream exposes this through its dashboard; the OSS
equivalent is this CLI writing to the metadata store, which the update hook reads fresh on
every push. The org must already be registered (`serve` bootstraps it from
`OPENSTORAGE_ORG`).

Common flags: `--org` (required — the JWT `iss` claim) and `--data-dir` (local mode;
otherwise the database env URL is used).

### `signing-key add`

`--key` (required) points at either a single SSH public key line
(`<type> <base64> [comment]`; types `ssh-ed25519`, `ssh-rsa`, `ecdsa-sha2-nistp256/384/521`
and their `sk-` FIDO variants) or an ASCII-armored PGP public key block. Validation on add
checks framing only, not full key material: for SSH lines the type allow-list, base64
decodability, and the wire-format type echo (a truncated key payload still passes); for
PGP blocks just the armor header/footer. Bad key material surfaces later, at `verify-sig`
push time, where one bad registry entry can block protected pushes until it is removed —
so verify a signed push after adding a key. The hook trusts registry contents verbatim.

### `signing-key list`

Prints one row per key: ID, RFC 3339 creation time, and a key summary.

### `signing-key remove`

`--id` (required) removes a key by the positive ID shown by `list`.

```sh
openstorage signing-key add    --org selfhost --key ~/.ssh/id_ed25519.pub --data-dir ~/cs-data
openstorage signing-key list   --org selfhost --data-dir ~/cs-data
openstorage signing-key remove --org selfhost --id 1 --data-dir ~/cs-data
```

## `openstorage list-packs`

Lists the pack objects under a repository's `packs/` prefix in the blob store (key and
size per line, plus a total) — a storage-inspection helper for dogfooding and debugging.

| Flag | Default | Meaning |
|---|---|---|
| `--repo` | required | Repository **storage key**: the server-generated repo ID (`repo_...` from repo creation), optionally with a `+ephemeral` / `+import` namespace suffix — not the repo name |
| `--data-dir` | `$OPENSTORAGE_DATA_DIR` | Local mode; otherwise the blob env URL is used |

```sh
openstorage list-packs --repo repo_761aac427fd3 --data-dir ~/cs-data
```

## `openstorage purge-repo`

Permanently reclaims one **soft-deleted** repository: unregisters its refs, packs, and
LFS objects (base repository plus `+ephemeral`/`+import` namespaces), deletes its blobs
through the sweeper's quarantine-first discipline, and removes its credentials and
upstream link. The synchronous twin of the
[`OPENSTORAGE_REPO_RETENTION`](configuration.md#openstorage_repo_retention) background
purge — same primitive, same crash-safe ordering (rows tombstone before blobs; an
interrupted purge resumes on re-run).

**Irreversible.** A purged repository cannot be restored, and its `(org, url)` name
stays burned forever (the repository row is kept as a tombstone — URL-scoped tokens may
outlive the purge, so the name is never released). The repository must already be
soft-deleted (`DELETE /api/v1/repos/delete`) at least one hour earlier — younger
deletions are refused so no in-flight pre-delete push can race the purge.

**Fleet-upgrade prerequisite:** run this only once every node — serve replicas *and*
hook-running git nodes — runs a build with the purge write fence, exactly like enabling
[`OPENSTORAGE_REPO_RETENTION`](configuration.md#openstorage_repo_retention): an older
binary does not check the fence, so its in-flight write can be stranded behind the
completed purge.

```
openstorage purge-repo [flags] <repo-id>
```

| Flag | Default | Meaning |
|---|---|---|
| `<repo-id>` | required | The server-generated **base** repository ID (`repo_...`) — never a `+namespace` key |
| `--yes` | `false` | Required confirmation; without it the command prints the repo summary and refuses |
| `--data-dir` | `$OPENSTORAGE_DATA_DIR` | Local mode; otherwise the database and blob env URLs are used |

```sh
openstorage purge-repo --data-dir ~/cs-data --yes repo_761aac427fd3
```

Deleted blob bytes land in `sweep/quarantine/` first and are freed once the backups age
past the quarantine window on a later sweep (the running server's sweep collects them;
with sweeping disabled they remain until one runs). Re-running the command on an
already-purged repository rescans its blob prefixes for stragglers — bytes an in-flight
pre-delete push published after the first pass — and reaps them through the same
quarantine handshake.

## `openstorage version`

Prints the build version on one line and exits (`--version` is an alias):

```
openstorage v<version> (commit <sha>, built <timestamp>, <go-version>)
```

Release binaries have the tag/commit/date stamped at build time
([`scripts/release-archives.sh`](../../scripts/release-archives.sh)); source builds fall
back to Go module build info, reporting `devel` when nothing better is known.
