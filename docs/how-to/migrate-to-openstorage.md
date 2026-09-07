# Migrate to open.storage

**Goal:** upgrade an existing code-storage installation to open.storage v0.15.x
without changing the durable stores it uses. This is a naming change, not a move of
your repositories or a change to the upstream wire-compatibility contract.

## 1. Update executables, images, and source references

| Surface | Before | From v0.15.0 |
|---|---|---|
| Product name | code-storage | open.storage |
| Repository | `coder/code-storage` | `coder/open-storage` |
| Go module | `github.com/coder/code-storage` | `github.com/coder/open-storage` |
| Binary and command directory | `codestorage`, `cmd/codestorage` | `openstorage`, `cmd/openstorage` |
| Release archives | `codestorage_<version>_<os>_<arch>.tar.gz` | `openstorage_<version>_<os>_<arch>.tar.gz` |
| Environment prefix | `CODESTORAGE_*` | `OPENSTORAGE_*` |

Update service-manager commands, scripts, health checks that invoke the binary, and
installer asset names. The new container image target is `ghcr.io/coder/open-storage`.
Registry publishing is not configured in this repository yet: build the Dockerfile
locally until release publishing is available, then pin a verified version or digest.
There is **no dual publishing to the old image path**. The environment compatibility
window below does not provide an old binary name or an old image alias.

The repository is renamed before the v0.15.0 changes merge, so the new Go module path
resolves from GitHub when the renamed version is available. GitHub redirects the old
repository URL, but that redirect does not rewrite Go imports or the module declaration.
After the rename, update checkout remotes and use the new module path:

```sh
git remote set-url origin https://github.com/coder/open-storage.git
go install github.com/coder/open-storage/cmd/openstorage@latest
openstorage version
```

For a private/internal checkout, use the authenticated-Git and `GOPRIVATE` setup in
[Install](../../README.md#install). Downstream Go consumers must update their imports
and module requirement to `github.com/coder/open-storage` at v0.15.0 or later, then run
`go mod tidy` (and `go mod vendor` if they vendor dependencies). Older tags keep their
original module path and original release asset names; do not request an old tag through
the new module path and assume GitHub's redirect changes its `go.mod`.

## 2. Roll out the environment-prefix change

The canonical prefix is `OPENSTORAGE_*`. For **one minor release, 0.15.x only**, a
`CODESTORAGE_*` variable remains a fallback when its corresponding new variable is
**unset**. Migrate all settings before upgrading to 0.16.0.

- A set `OPENSTORAGE_*` variable always wins, **including an empty value**. Empty does
  not mean "try the old name": it is processed by that setting's normal validation or
  default rules. For example, an empty required org setting still fails startup even
  if the old org variable has a value.
- `serve` emits a **boot-time deprecation warning for each legacy-prefix fallback**.
  Values are not included in these warnings. A shadowed legacy variable is not a fallback.
- Explicit CLI flags keep their existing precedence over environment settings.
- Pre-0.15 binaries do **not** recognize the new prefix. During a rolling upgrade,
  keep the old variables until every replica runs v0.15.x; then change the keys to the
  new prefix, preserving their values, and restart each replica. If both names must
  coexist during the roll, keep their values identical.

For example, change `CODESTORAGE_PG_URL` to `OPENSTORAGE_PG_URL` and
`CODESTORAGE_ORG_PUBKEYS` to `OPENSTORAGE_ORG_PUBKEYS` in your service environment or
secret-store templates. Do not rename unrelated settings such as `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, or `TMPDIR`. The [configuration reference](../reference/configuration.md)
lists the canonical names, validation rules, and older aliases.

## 3. Keep persistent data locations unchanged

Back up the database, object storage, and keys before upgrading. **Rename configuration
keys, not configuration values:** keep the existing database URL, bucket name, object
prefix, file-blob directory, data-directory path, and mounted volumes. Do not create a
new empty store just to match the new product name. Existing key-file paths and
`--cache-dir` values can stay as configured too; only the cache is rebuildable.

New examples use `openstorage` for development bucket names. These are examples,
not a requirement to rename existing resources. A bucket named `codestorage` remains
a valid configured target.

**On-disk compatibility names are unchanged.** `--data-dir <dir>` still derives
`sqlite://<dir>/codestorage.db`, `file://<dir>/blobs`, and `<dir>/cache`, so pointing the
new binary at the same data directory reopens the existing database. The default cache
directory outside local mode remains `<os temp dir>/codestorage-cache`; existing cache
coordination filenames also stay unchanged. There is no automatic file rename or data
migration, and no need to add an explicit database URL just for the product rename.

For example, an existing local deployment can keep using its data directory:

```sh
OPENSTORAGE_ORG=selfhost OPENSTORAGE_ORG_PUBKEYS=/etc/code-storage/org.pub.pem \
  openstorage serve --listen 127.0.0.1:8080 --data-dir /srv/code-storage
```

Replace these paths and org settings with the **existing** deployment's values, not
with fresh directories. If explicitly configured, database and blob URLs still take
precedence over data-dir derivation; preserve them verbatim. For Postgres or `s3wal://`,
keep the same URLs and credentials. The rename does not require copying tables,
rewriting object keys, or moving buckets.

## 4. Verify the upgrade

Before changing production, rehearse on a backed-up local installation or an isolated
staging deployment using its existing stores:

1. Start the new `openstorage` binary with the old environment names. Check `/healthz`
   and confirm the boot log warns for each fallback without printing secret values.
2. Stop it gracefully. Change the environment keys to `OPENSTORAGE_*`, retaining all
   values and storage paths, then start it again. Check `/healthz`; prefix-deprecation
   warnings should be gone.
3. Use an existing repository's token to read metadata, clone, and push a test commit.
   Restart once more and confirm the commit is still available. If Git LFS is in use,
   fetch an existing LFS object as well.
4. In a disposable test environment only, set conflicting old/new org names and then
   an explicitly empty new org value. Verify the new name wins and the empty required
   value fails rather than falling back. Never test a conflicting org on production.
5. Save redacted startup logs plus screenshots and a recording of the terminal session
   with the deployment's upgrade evidence. Do not record private keys or tokens.

For a fleet, also follow the [production rolling-upgrade guidance](deploy-production.md#rolling-upgrades-and-restarts)
and its storage-lifecycle prerequisites; the prefix compatibility window does not
relax those safety requirements.
