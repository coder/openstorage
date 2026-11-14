# CLI Reference

The `openstorage` binary contains the server and its operational helpers:

```
openstorage <serve|hook|keygen|mint|signing-key|list-packs|purge-repo|repair-refs|s3wal-meta|version> [flags]
```

Sources: [`cmd/openstorage/main.go`](../../cmd/openstorage/main.go) and
[`cmd/openstorage/signingkey.go`](../../cmd/openstorage/signingkey.go). Environment
variables: [configuration reference](configuration.md).

**Storage resolution** (`serve`, `signing-key`, `list-packs`, `purge-repo`, `repair-refs`):
explicit env URLs win, then `--data-dir` / `$OPENSTORAGE_DATA_DIR` (local mode) fills
missing slots. Every backend scheme works, including the experimental
[`s3wal://`](configuration.md#openstorage_pg_url) zero-infra pair. `signing-key` and
`purge-repo` open its metadata/ref stores from the database URL, and `repair-refs` opens
both stores from it, the metadata store only to read the repository row, and the blob
store from the `s3://` URL (or `--data-dir`) when the repository has a ref that git can lock; `list-packs`
reads blobs through the `s3://` URL and never opens the database URL.

## `openstorage serve`

Runs one stateless node. A single listener serves:

- the `/api/v1` REST API, plus the `/api/repos/{repo_name}/...` aliases and
  `/api/repo-urls/{repo_id}`;
- git smart HTTP with the LFS batch API;
- `GET /healthz`, an unauthenticated liveness probe.

Prints `listening on <addr>` once bound.

| Flag | Default | Meaning |
|---|---|---|
| `--listen` | `$OPENSTORAGE_LISTEN`, then `127.0.0.1:0` | Listen address |
| `--addr` | — | Legacy alias for `--listen` (`--listen` wins) |
| `--cache-dir` | data-dir derivation, then `$TMPDIR/codestorage-cache` | Rebuildable bare-repo cache directory; a relative path resolves against the working directory at startup |
| `--data-dir` | `$OPENSTORAGE_DATA_DIR` | Local-mode data directory; derives database/blob/cache locations for slots not set explicitly |

Requires `OPENSTORAGE_ORG` + `OPENSTORAGE_ORG_PUBKEYS` (single-org mode) and a resolvable
database + blob URL (explicit env URLs, or local mode via `--data-dir`). All validation
happens at startup. Rate limiting, quotas, sweeping, and repack are in the
[configuration reference](configuration.md).

```sh
# Local mode: SQLite + plain files under one directory, no other services.
openstorage serve --listen 127.0.0.1:8080 --data-dir ~/cs-data

# Production mode: Postgres + S3 via env URLs.
OPENSTORAGE_PG_URL='postgres://...' OPENSTORAGE_S3_URL='s3://...' openstorage serve

# Zero-infra mode (experimental): refs + metadata + packs in one pre-created bucket.
OPENSTORAGE_PG_URL='s3wal://bucket?endpoint=...' OPENSTORAGE_S3_URL='s3://bucket?endpoint=...&s3ForcePathStyle=true' openstorage serve
```

## `openstorage hook`

Internal; do not run it manually. `openstorage hook <pre-receive|update> [args...]` is
installed by the server into cached bare repositories and re-exec'd by git during pushes.
It reads connection and per-request environment inherited from the serve process.

| Hook | Action |
|---|---|
| `pre-receive` | Publishes and registers the quarantined pack; enforces the pack quota |
| `update` | Runs the ref CAS and ref policies ([git protocol](../explanation/git-protocol-and-namespaces.md)) |

## `openstorage keygen`

Generates an org keypair for JWT signing.

| Flag | Default | Meaning |
|---|---|---|
| `--out` | required | Output path prefix: writes `<out>.pem` (PKCS#8 private key, created with mode `0600`) and `<out>.pub.pem` (PKIX public key, created with mode `0644`), subject to umask. Without `--if-missing`, existing files are overwritten but keep their old modes |
| `--alg` | `ES256` | Fresh key algorithm: one of `ES256`, `RS256`, `EdDSA`, `PS256`, `ES384`, `ES512`. Ignored when `--if-missing` reuses an existing pair, even if different or unsupported |
| `--if-missing` | `false` | Generate only when both files are absent; otherwise validate and reuse a matching pair without writes |

Point `OPENSTORAGE_ORG_PUBKEYS` at the `.pub.pem` file (it may hold several concatenated
public key blocks). Never commit either file. The command prints only the key paths,
never key material.

```sh
openstorage keygen --out /tmp/cs-key   # /tmp/cs-key.pem + /tmp/cs-key.pub.pem
```

For repeatable bootstrap, use a directory you control:

```sh
mkdir -m 700 ./keys
openstorage keygen --if-missing --out ./keys/org
openstorage keygen --if-missing --out ./keys/org   # validates; no rotation or rewrite
```

**`--if-missing` reuse checks.** Missing halves, corrupt keys, mismatches, and unsafe
files fail without writes; nothing is chmod'ed automatically.

- Both files must be regular files, not symlinks.
- Private file: no group or other permission bits. Public file: no group or other write bits.
- Each file holds exactly one PEM block, with only whitespace outside it. Extra blocks or
  other payloads are rejected.
- The private block holds a usable PKCS#8 or SEC1 signing key (RSA: at least 2048 bits);
  the public block holds the matching PKIX public key.
- Reuse preserves file bytes (including whitespace and CRLF), permissions, and
  modification times.

**Fresh generation** creates files exclusively, so a concurrently created file is not
overwritten. The two-file creation is **not atomic**: an interruption or I/O error can
leave a partial pair, which later runs reject; inspect and repair it yourself. The command
does not lock the directory, so do not touch the files or directory while it runs.

## `openstorage mint`

Mints a JWT signed with the org's private key: the local-mode stand-in for your token
service. Claims: [auth spec](authentication.md).

| Flag | Default | Meaning |
|---|---|---|
| `--key` | required | PEM path of the org private key (PKCS#8 or SEC1) |
| `--iss` | required | Issuer claim — the org name (`OPENSTORAGE_ORG`) |
| `--repo` | required | Repo claim: the target repo, or `org` for org-level operations |
| `--scopes` | `git:read,git:write` | Comma-separated scopes: `git:read`, `git:write`, `repo:write`, `org:read` |
| `--refs` | — | Optional ref-policy JSON, e.g. `[["refs/heads/main",["no-force-push"]],["*",[]]]` — only the JSON tuple structure is validated at mint time |
| `--alg` | inferred from key type | JWS signature algorithm |
| `--ttl` | `1h` | Token lifetime (must be positive) |

Ref-policy patterns (evaluation: [auth spec](authentication.md)):

| Pattern | Matches |
|---|---|
| Exact ref name | That ref |
| `*` alone | Everything |
| Single trailing `*` after `/`, e.g. `refs/heads/feature/*` | Prefix match |
| `*` anywhere else | Nothing — treated as an exact name |

> [!WARNING]
> A mistyped pattern such as `refs/heads/main*` never matches, so the ref is silently
> left unpoliced.

```sh
TOKEN=$(openstorage mint --key /tmp/cs-key.pem --iss selfhost --repo demo --scopes git:read,git:write)
git clone "http://t:${TOKEN}@127.0.0.1:8080/demo.git"
```

## `openstorage signing-key`

Manages the org's commit-signing key registry for the `verify-sig` ref-policy op
([auth spec](authentication.md)); upstream uses a dashboard instead. It writes to the
metadata store, which the update hook reads fresh on every push. The org must already be registered (`serve` bootstraps it from
`OPENSTORAGE_ORG`).

Common flags: `--org` (required; the JWT `iss` claim) and `--data-dir` (local mode;
otherwise the database env URL is used).

### `signing-key add`

`--key` (required) points at one of:

- a single SSH public key line, `<type> <base64> [comment]`. Types: `ssh-ed25519`,
  `ssh-rsa`, `ecdsa-sha2-nistp256/384/521`, and their `sk-` FIDO variants;
- an ASCII-armored PGP public key block.

`add` checks framing only, not key material:

- SSH: type allow-list, base64 decodability, and the wire-format type echo. A truncated
  key payload still passes.
- PGP: armor header and footer only.

> [!WARNING]
> The hook trusts registry contents verbatim. Bad key material surfaces only at
> `verify-sig` push time, and one bad entry can block protected pushes until removed.
> Verify a signed push after adding a key.

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

Lists pack objects under a repository's `packs/` blob-store prefix (key and size per line,
plus a total), for dogfooding and debugging.

| Flag | Default | Meaning |
|---|---|---|
| `--repo` | required | Repository **storage key**: the server-generated repo ID (`repo_...` from repo creation), optionally with a `+ephemeral` / `+import` namespace suffix — not the repo name |
| `--data-dir` | `$OPENSTORAGE_DATA_DIR` | Local mode; otherwise the blob env URL is used |

```sh
openstorage list-packs --repo repo_761aac427fd3 --data-dir ~/cs-data
```

## `openstorage purge-repo`

Permanently reclaims one **soft-deleted** repository, including its `+ephemeral`/`+import`
namespaces:

- unregisters its refs, packs, and LFS objects;
- deletes its blobs through the sweeper's quarantine-first discipline;
- removes its credentials and upstream link.

It is the synchronous twin of the
[`OPENSTORAGE_REPO_RETENTION`](configuration.md#openstorage_repo_retention) background
purge: same primitive, same crash-safe ordering. Rows tombstone before blobs, and an
interrupted purge resumes on re-run.

> [!CAUTION]
> **Irreversible.** A purged repository cannot be restored. Its `(org, url)` name stays
> burned forever: the repository row is kept as a tombstone because URL-scoped tokens may
> outlive the purge.

**Preconditions:**

1. The repository was soft-deleted (`DELETE /api/v1/repos/delete`) at least one hour
   earlier. Younger deletions are refused so no in-flight pre-delete push can race the
   purge.
2. **Fleet upgrade:** every node — serve replicas *and* hook-running git nodes — runs a
   build with the purge write fence, exactly as for enabling
   [`OPENSTORAGE_REPO_RETENTION`](configuration.md#openstorage_repo_retention). An older
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

Deleted blob bytes go to `sweep/quarantine/` first. The running server's sweep frees them
once they age past the quarantine window; with sweeping disabled they stay until a sweep
runs.

Re-running on an already-purged repository rescans its blob prefixes for stragglers (bytes
an in-flight pre-delete push published after the first pass) and reaps them through the
same quarantine handshake.

## `openstorage repair-refs`

Removes the refs of one repository that stop its hydration. Every hydration of a
repository that holds such a ref fails, and git reads of it answer `500`. Three kinds of
ref do that:

- **A ref that git can never lock.** git creates `<ref>.lock` before it writes a ref, so a
  name with a part over 255 bytes, or a last part over 250 bytes, cannot be written into a
  cache repository. Before the API limited ref names ([Branches](http-api.md#branches)), a
  token with `git:write` could create a notes ref with a 251 to 255 byte last part. No API
  route, push or sync can delete a notes ref, so an operator removes it here.
- **A branch whose tip is not a commit.** git refuses to write a blob, a tree or an
  annotated tag under `refs/heads/`. Before the update hook refused such a push
  ([git protocol](../explanation/git-protocol-and-namespaces.md)), a token with
  `git:write` could push a branch at one of them, and the ref store took the value.
- **A ref whose tip object no pack holds.** git refuses to write a ref at an object that the
  cache repository lacks, and the cache holds only what the registered packs hold. The ref
  can be a branch, a tag, a note or an ephemeral branch.

The runbook, with the log lines to look for, is
[Refs that git can never lock](../how-to/deploy-production.md#refs-that-git-can-never-lock).

```
openstorage repair-refs [flags] <repo-id>
```

| Flag | Default | Meaning |
|---|---|---|
| `<repo-id>` | required | The server-generated repository ID (`repo_...`), the key of the repository in the ref store |
| `--yes` | `false` | Delete the listed refs. Without it the command only lists them and changes nothing |
| `--data-dir` | `$OPENSTORAGE_DATA_DIR` | Local mode: derives the database and blob locations when no env URL is set |

The command reads every ref of the repository and picks the ones that git can never lock.
It leaves out the limit on the length of the whole lock path, which depends on where a
node keeps its cache directory. It never touches a ref that git can lock and whose tip
object a pack holds, unless the ref is a branch at a blob, a tree or an annotated tag:
a tag at a blob is healthy.

For each other ref, the command also reads the object type of the tip. It reads the type
from the repository's packs in the blob store, and never from git's error text. The first
read downloads the registered packs into a temporary directory under `$TMPDIR`, so
`$TMPDIR` needs room for them. The command removes the directory as soon as it has read
the tips, before it prints the list, and also after an error, SIGINT, SIGTERM or SIGHUP.
A run that `nohup` started ignores SIGHUP. After SIGKILL or SIGQUIT, delete the
`openstorage-repair-refs-*` directory under `$TMPDIR` by hand. A repository with no ref
that git can lock needs no blob store. A branch is on the list when its tip is a blob, a
tree or an annotated tag, even a tag of a commit: git refuses all three. A ref of any
namespace is on the list when no registered pack holds its tip object. The objects and
packs stay, and a later push can use them. The command downloads the packs once, as of the
start of the run. When the scan found a ref at a missing object and the set of registered
packs has changed since the run began, the command prints an error and lists and deletes
nothing, because a new pack can hold an object that the run read as missing: run it
again.

A pack can also register while `--yes` deletes. After the deletes, the command checks the
registered packs once more. When they changed, it downloads the packs again, reads the
deleted tips again, and creates each deleted ref whose object a pack holds now, except a
branch at a blob, a tree or an annotated tag, which git still refuses. It prints
`restored: <ref>` for each. When a signal or an error stops the run after it deleted a ref
at a missing object, the command prints each such ref with its SHA: check the packs with
`openstorage list-packs`, and create a ref again where a pack holds its object.

```sh
openstorage repair-refs --data-dir ~/cs-data repo_761aac427fd3        # list
openstorage repair-refs --data-dir ~/cs-data --yes repo_761aac427fd3  # delete
```

The list for a repository with two branches at a blob and at an annotated tag:

```text
repo:    repo_761aac427fd3
url:     https://example.com/acme/app.git
scanned: 5 refs
branches whose tip is not a commit (2):
  45b983be36b73c0788dc9cbcb76cbb80fc7bb057 refs/heads/notes  (a blob)
  9d3f0b6b7d1d0f6f8b3a8ac3a8e0d3f0d4a8ab1e refs/heads/v1  (an annotated tag)
nothing changed: run the command again with --yes to delete these refs
```

The command first reads the repository row. An ID with no repository is an error. The
list prints `repo:`, `url:` and `scanned: <n> refs`. Then, when refs match, it prints
`refs that git can never lock (<n>):` with one `<sha> <ref>` line for each match, and
`branches whose tip is not a commit (<n>):` with one `<sha> <ref>  (<type>)` line for each
match, where the type is `a blob`, `a tree` or `an annotated tag`, and
`refs whose tip object is missing (<n>):` with one `<sha> <ref>` line for each match. It
ends with `nothing changed`. When none match, it prints
`no ref that git can never lock, no branch whose tip is not a commit, and no ref whose tip object is missing`.
A scan of 0 refs prints `this repository has no refs` instead: it is empty, it was
purged, or its last refs were just deleted. A database URL or `--data-dir` that is not the
one `serve` uses is an error (`not found`, or an error that names the missing `repos`
table). A wrong local `--data-dir`
can also leave an empty database file there, so check the path.

With `--yes`, the command swaps each ref's listed SHA for the zero SHA, one compare-and-swap
for each ref, and prints `deleted: <ref>`. When a ref no longer holds the listed SHA, it
prints `lost CAS, not deleted`, keeps that ref, does not retry, and exits with an error
after the other refs. Run the command again to list what is left. Put the flags before
`<repo-id>`: a flag after it is an error.

A ref whose tip object no pack holds is deleted with `--yes` like the other kinds, and
the list shows its SHA. Only the ref goes. Check the packs with
[`openstorage list-packs`](#openstorage-list-packs) before you delete it, because the
object can be in a pack that the registry lost, and keep the list. The command downloads
every registered pack, so a pack that the blob store lost stops the run before it lists
anything. A blob URL that the command cannot open, a type that it does not know, and a
read that fails before the deletes are errors, and the command deletes nothing on any of them.

The command keeps a ref that is the repository's default branch, as
`DELETE /repos/branches` does, and the list marks it. `--yes` deletes the other refs,
prints `kept: <ref> is the default branch`, and exits with an error. Set another default
branch with `PATCH /repo`, then run the command again.

The command changes the ref store only. It sends no push webhook and no sync poke. It can
run while `serve` runs. `--yes` lists the refs again and deletes what it finds, so a ref
that appeared after an earlier list goes too. With `--yes` it first runs the ref-store
schema setup that `serve` runs at boot, as `purge-repo` does, so that the delete bumps
the repository generation. The list runs no schema setup. A replica on v0.20.0 or earlier
accepts such names, so run the command after the last replica is upgraded.

## `openstorage s3wal-meta`

Converts an S3WAL bucket's metadata document (`_wal/meta.json`) between format versions 1
and 2 ([ADR-51](../adr/051-s3wal-metadata-v2-is-one-versioned-document-converted-by-a-fenced-command.md)).
`serve` reads only version 2. Run the command once with the new binary when you upgrade
from a v1 release, and with `--to 1` before you go back to one. The steps are in the
[deployment guide](../how-to/deploy-production.md#upgrading-the-s3wal-metadata-format).

```
openstorage s3wal-meta convert --to <1|2> [--force]
```

| Flag | Default | Meaning |
|---|---|---|
| `--to` | required | Target version: `2` to upgrade, `1` to downgrade |
| `--force` | off | With `--to 1` only: convert even while a create without an upstream link (a fork, a copy, or a sync create whose link was removed) is unfinished |

The database URL (`OPENSTORAGE_PG_URL`) must be an `s3wal://` URL. The command:

1. Runs the conditional-write probe, then takes the instance lease. It refuses while
   another process holds a fresh lease, so it never runs under a live `serve`. It
   ignores `OPENSTORAGE_S3WAL_LEASE_TTL=0`.
2. Renews the lease while it runs and stops before its write if a renewal fails.
3. Writes the converted document with a conditional write, re-reads it, and checks that
   it equals the expected conversion. It prints the version, `seq` and row counts.
4. Releases the lease.

`--to 2` changes only the version. `--to 1` drops the four v2 fields and sets
`sync_pending` on every lane whose pull claim is set, so a v1 release restarts that pull
at boot.

`--to 1` also handles creates that had not finished (v1 stores no create state):

- An unfinished sync create (a pending repo with its upstream link) keeps its first
  sync: the command sets `sync_pending`, and the v1 release runs that sync at boot. The
  output lists these repos.
- An unfinished create without an upstream link (a fork, a copy, or a sync create whose
  link was removed) would read as a finished, possibly empty repository on v1. The
  command refuses, writes nothing, and names each repo. Remove them first: start `serve`
  on this release, call `DELETE /repos/delete` for each, stop `serve`, and convert
  again. `--force` converts anyway and lists the repos in its output.

A document already at the target version is left unchanged (exit 0). A bucket without a
metadata document needs nothing: `serve` creates a v2 document.

## `openstorage version`

Prints the build version on one line and exits (`--version` is an alias):

```
openstorage v<version> (commit <sha>, built <timestamp>, <go-version>)
```

Release binaries get the tag, commit, and date stamped at build time
([`scripts/release-archives.sh`](../../scripts/release-archives.sh)). Source builds fall
back to Go module build info and report `devel` when nothing better is known.
