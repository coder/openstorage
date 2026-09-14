# Try open.storage with Docker only

Run **one local-mode server** using SQLite and files. Docker and a recent Docker
Compose plugin (`up --wait` support) are the only server-side prerequisites. No Go,
standalone `openstorage` binary, Postgres, or S3 service is needed on the host.
Start from a [repository checkout](../../CONTRIBUTING.md#authenticated-installation).

This is **development verification**, not a pinned downloadable image reference.
The recipe requires an explicitly selected local build. Registry publication is
not part of this flow; [issue #227](https://github.com/coder/openstorage/issues/227)
remains open for the downloadable reference. The existing
[two-replica development stack](deploy-docker-compose.md) is a separate option.

## 1. Build and start

Run these commands from the checkout root, in the same shell:

```sh
TAG="openstorage-quickstart:local-$(date +%s)-$$"
docker build -t "$TAG" .
export OPENSTORAGE_IMAGE=$(docker image inspect --format '{{.Id}}' "$TAG")
export OPENSTORAGE_PROJECT=openstorage-demo  # choose another name on a shared daemon
export OPENSTORAGE_PORT=8080               # choose a free loopback port

dc() { docker compose -f compose.quickstart.yml -p "$OPENSTORAGE_PROJECT" "$@"; }
dc up -d --wait
```

[`compose.quickstart.yml`](../../compose.quickstart.yml) never pulls an image. Keep
the image ID, project name, and port for later commands. The server is bound only to
`127.0.0.1:$OPENSTORAGE_PORT`. Git and LFS advertise that same host-reachable URL.
`/healthz` answers after startup initialization; the healthcheck is **liveness**,
not a continuous check of database or blob-storage readiness.

The one-shot root `init` service prepares only the volume roots, generates a keypair
only when absent, and validates an existing pair without rewriting it. It copies
**only the public PEM** to the public volume. Invalid, incomplete, mismatched, or
symlinked key files block startup; a failed initializer is not automatically restarted.
The long-running server is nonroot. It has a read-only public volume and a writable
data volume, but **no private-key mount**. The `cli` helper is opt-in and mounts only
the private volume, read-only. Private directory/file modes are 0700/0600.
Docker administrators can access all these volumes; this does not isolate secrets
from people who control the Docker daemon.

## 2. Mint a token without installing the CLI

Keep credentials out of command history, terminal output, and Git remote URLs.
Do not enable shell or Git tracing. The token below lasts one hour and is scoped
to the `demo` repository:

```sh
set +x
umask 077
CLIENT_DIR=$(mktemp -d)
export OPENSTORAGE_TOKEN_FILE="$CLIENT_DIR/token"
dc run --rm --no-deps -T cli mint --key /private/org.pem --iss selfhost \
  --repo demo --scopes repo:write,git:read,git:write --ttl 1h > "$OPENSTORAGE_TOKEN_FILE"
printf 'Authorization: Bearer %s\n' "$(cat "$OPENSTORAGE_TOKEN_FILE")" > "$CLIENT_DIR/auth"
```

For an optional private-key export, redirect directly into a new restrictive host
file. Do not print the key or place it in the checkout:

```sh
KEY_DIR=$(mktemp -d "$HOME/openstorage-keys.XXXXXX")
(umask 077; set -C
 dc run --rm --no-deps -T --entrypoint /bin/cat cli /private/org.pem > "$KEY_DIR/org.pem"
)
```

Store that file securely. It grants the ability to mint organization tokens. A
private-key export alone is **not** a complete backup of the keypair or repository data.

## 3. Use the API, Git, and LFS

This section additionally needs **client-side** curl, Git, and Git LFS. They are
not needed to start the server or mint tokens. Run from the checkout root; all demo
files stay outside it. The askpass script reads the token file instead of saving a
tokenized remote or invoking your normal credential store.

```sh
BASE="http://127.0.0.1:$OPENSTORAGE_PORT"
curl -q --fail --silent --show-error -H "@$CLIENT_DIR/auth" \
  -H 'Content-Type: application/json' -d '{"default_branch":"main"}' "$BASE/api/v1/repos"
curl -q --fail --silent --show-error -H "@$CLIENT_DIR/auth" "$BASE/api/v1/repo"

cat > "$CLIENT_DIR/askpass" <<'SH'
#!/bin/sh
case "$1" in
  *Username*) printf '%s\n' t ;;
  *Password*) cat "$OPENSTORAGE_TOKEN_FILE" ;;
  *) exit 1 ;;
esac
SH
chmod 700 "$CLIENT_DIR/askpass"
export GIT_ASKPASS="$CLIENT_DIR/askpass" GIT_TERMINAL_PROMPT=0

git clone --config credential.helper= "$BASE/demo.git" "$CLIENT_DIR/source"
git -C "$CLIENT_DIR/source" lfs install --local
git -C "$CLIENT_DIR/source" lfs track '*.bin'
printf 'Hello from Docker\n' > "$CLIENT_DIR/source/hello.txt"
printf 'LFS payload\n' > "$CLIENT_DIR/source/example.bin"
git -C "$CLIENT_DIR/source" add .
git -C "$CLIENT_DIR/source" -c user.name=Demo -c user.email=demo@example.com commit -m demo
git -C "$CLIENT_DIR/source" push origin HEAD:main

GIT_LFS_SKIP_SMUDGE=1 git clone --config credential.helper= "$BASE/demo.git" "$CLIENT_DIR/clone"
git -C "$CLIENT_DIR/clone" lfs install --local
git -C "$CLIENT_DIR/clone" lfs pull
cmp "$CLIENT_DIR/source/hello.txt" "$CLIENT_DIR/clone/hello.txt"
cmp "$CLIENT_DIR/source/example.bin" "$CLIENT_DIR/clone/example.bin"
```

Both comparisons should be silent. Unlike the two-replica S3 development example,
local file storage proxies LFS uploads **and downloads** through the server's
loopback URL. No container-only object-store address reaches the client.

## 4. Stop, resume, or destroy

Three named volumes persist: `${OPENSTORAGE_PROJECT}_private` (keypair),
`${OPENSTORAGE_PROJECT}_public` (public copy), and `${OPENSTORAGE_PROJECT}_data`
(SQLite, blobs, and rebuildable cache).

```sh
dc stop                        # pause, retain containers and all three volumes
dc up -d --wait                 # resume with initialization validation
dc down                        # remove containers/network, KEEP named volumes
dc up -d --wait                 # same keys, repos, Git, and LFS data
dc up -d --wait --force-recreate # also retains the named volumes
```

A previously minted, unexpired token still works after those operations. Verify
with the API read above and another clone to a new directory. Changing the project
name selects different volumes; do not mistake an empty new project for lost data.
Do not scale this SQLite/file-storage topology beyond one server.

**Destructive reset, only for a disposable project:**

```sh
dc --profile tools down --volumes # irreversibly deletes keys AND repository data
rm -rf -- "$CLIENT_DIR"          # remove this walkthrough's host credentials/clones
unset OPENSTORAGE_TOKEN_FILE GIT_ASKPASS GIT_TERMINAL_PROMPT
```

If init fails, inspect `dc logs init` and run `dc down` **without** `--volumes`.
Preserve the volumes before recovery. Restore a known-good, consistent backup of
all three volumes; never delete just one key file or generate replacement keys over
existing state. The initializer deliberately does not repair malformed files,
replace a differing public copy, or regenerate a missing pair while a public copy
remains. It changes ownership only at mount roots, so changing the image's runtime
UID requires an explicit offline data migration, not a recursive startup repair.
Without a usable backup, only reset a project whose data you intend to discard.
Do not run competing initializers or edit key volumes while the server is running.

## Verification and production boundary

CI builds the repository Dockerfile without pushing it, then runs
[`scripts/smoke-compose-quickstart.sh`](../../scripts/smoke-compose-quickstart.sh)
with the exact local image ID. To repeat it, install the client tools above plus
Bash and Python 3, then run `bash scripts/smoke-compose-quickstart.sh` with
`OPENSTORAGE_IMAGE` exported. It uses unique disposable projects, tests API/Git/LFS,
old-token persistence, key isolation, and fail-closed bootstrap faults, then checks
that its containers, volumes, and networks are deleted. It never changes the
reference Compose config. No new parser or hot path is introduced, so this change
adds integration smoke coverage rather than fuzz targets or benchmarks.

This plain-HTTP, single-node default is for a trusted local machine, not a production
security or availability configuration. Before deployment, plan TLS, external URLs,
access control, key management/rotation, consistent backups, resource limits, and
monitoring using the [production guide](deploy-production.md). The two Compose
recipes remain distinct; this one does not harden or change the existing development
Postgres/S3 stack.
