# Migrate to open.storage

Upgrade an existing code-storage or open.storage installation to the current binary,
environment, repository, and module names. Durable stores and the upstream wire-compatibility
contract stay unchanged.

- The binary and environment-prefix change shipped in v0.15.0.
- The one-word repository and Go module path (`github.com/coder/openstorage`)
  shipped in v0.16.0.

## Prerequisites

- A backup of the database, object storage, and keys.
- A backed-up local installation or isolated staging deployment for rehearsal.

## 1. Update executables, images, and source references

| Surface | Before v0.15.0 | Current |
|---|---|---|
| Product name | code-storage | open.storage |
| Repository | `coder/code-storage` | `coder/openstorage` |
| Go module | `github.com/coder/code-storage` | `github.com/coder/openstorage` |
| Binary and command directory | `codestorage`, `cmd/codestorage` | `openstorage`, `cmd/openstorage` |
| Release archives | `codestorage_<version>_<os>_<arch>.tar.gz` | `openstorage_<version>_<os>_<arch>.tar.gz` |
| Environment prefix | `CODESTORAGE_*` | `OPENSTORAGE_*` |

1. Update service-manager commands, scripts, health checks that invoke the binary,
   and installer asset names.
2. Switch images to `ghcr.io/coder/openstorage`. Registry publishing is not
   configured yet: build the Dockerfile locally, then pin a verified version or
   digest once releases publish. Nothing is published to the old image path, and
   the section 2 fallback provides **no old binary name or image alias**.
3. Update checkout remotes and build from a checkout with the corrected `go.mod`.
   The `v0.15.0` tag still declares `github.com/coder/open-storage`, and GitHub
   redirects do not rewrite Go imports or module declarations.

```sh
git remote set-url origin https://github.com/coder/openstorage.git
go install ./cmd/openstorage
openstorage version
```

For a private checkout, use the authenticated-Git and `GOPRIVATE` setup in
[CONTRIBUTING](../../CONTRIBUTING.md#authenticated-installation).

Downstream Go consumers: change imports and the module requirement to
`github.com/coder/openstorage` at a revision that declares that path. Then run
`go mod tidy` (plus `go mod vendor` if you vendor).

From v0.16.0, `go install github.com/coder/openstorage/cmd/openstorage@v0.16.0`
works with the authenticated-Git and `GOPRIVATE` setup above. Older tags keep their
original module paths and asset names.

## 2. Roll out the environment-prefix change

The canonical prefix is `OPENSTORAGE_*`. In 0.15.x and 0.16.x, a `CODESTORAGE_*`
variable is a fallback when the matching new variable is **unset**. 0.17.0 removes
the fallback.

> [!WARNING]
> Migrate all settings before upgrading to 0.17.0. Do not rely on the fallback.

Precedence rules:

- A set `OPENSTORAGE_*` variable always wins, **even when empty**. An empty value
  goes through that setting's normal validation or default; it never falls back.
  For example, an empty required org setting fails startup even if the old org
  variable has a value.
- `serve` logs a **boot-time deprecation warning for each legacy fallback**, without
  values. A shadowed legacy variable is not a fallback.
- Explicit CLI flags still take precedence over environment settings.

Rolling upgrade (pre-0.15 binaries do **not** read the new prefix):

1. Keep the old variables until every replica runs v0.15.0 or later.
2. Rename the keys to the new prefix, keeping the values, and restart each replica.
3. If both names must coexist during the roll, keep their values identical.

Example: `CODESTORAGE_PG_URL` becomes `OPENSTORAGE_PG_URL`, and
`CODESTORAGE_ORG_PUBKEYS` becomes `OPENSTORAGE_ORG_PUBKEYS`, in your service
environment or secret-store templates. Do not rename unrelated settings such as
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or `TMPDIR`. See the
[configuration reference](../reference/configuration.md) for canonical names,
validation rules, and older aliases.

## 3. Keep persistent data locations unchanged

> [!WARNING]
> **Rename configuration keys, not values.** Keep the existing database URL, bucket
> name, object prefix, file-blob directory, data-directory path, and mounted
> volumes. Never create a new empty store to match the new name.

- Key-file paths and `--cache-dir` values can stay as configured. Only the cache is
  rebuildable.
- New examples use `openstorage` bucket names. An existing `codestorage` bucket is
  still valid; do not rename it.
- **On-disk names are unchanged.** `--data-dir <dir>` still derives
  `sqlite://<dir>/codestorage.db`, `file://<dir>/blobs`, and `<dir>/cache`, so the
  new binary reopens the existing database.
- Outside local mode the default cache directory is still
  `<os temp dir>/codestorage-cache`; cache coordination filenames are unchanged.
- There is no automatic file rename or data migration. You do not need to add an
  explicit database URL.

Example: an existing local deployment keeps its data directory:

```sh
OPENSTORAGE_ORG=selfhost OPENSTORAGE_ORG_PUBKEYS=/etc/code-storage/org.pub.pem \
  openstorage serve --listen 127.0.0.1:8080 --data-dir /srv/code-storage
```

Use the **existing** deployment's paths and org settings, not fresh directories.

- Explicitly configured database and blob URLs still override data-dir derivation.
  Preserve them verbatim.
- For Postgres or `s3wal://`, keep the same URLs and credentials. No tables are
  copied, object keys rewritten, or buckets moved.

## 4. Verify the upgrade

Rehearse before touching production, on a backed-up local install or isolated
staging deployment with its existing stores:

1. Start the new `openstorage` binary with the old environment names. Check
   `/healthz`. Confirm the boot log warns for each fallback without printing secrets.
2. Stop it gracefully. Rename the keys to `OPENSTORAGE_*`, keeping all values and
   storage paths. Start it and check `/healthz`; the deprecation warnings are gone.
3. With an existing repository's token, read metadata, clone, and push a test
   commit. Restart and confirm the commit survives. If you use Git LFS, fetch an
   existing LFS object too.
4. **Disposable test environment only:** set conflicting old/new org names, then an
   explicitly empty new org value. The new name must win, and the empty required
   value must fail instead of falling back. Never test a conflicting org on production.
5. Save redacted startup logs, screenshots, and a terminal recording with your
   upgrade evidence. Never record private keys or tokens.

For a fleet, also follow the [production rolling-upgrade guidance](deploy-production.md#rolling-upgrades-and-restarts)
and its storage-lifecycle prerequisites. The prefix fallback does not relax them.
