# Try open.storage with Docker only

Run **one local-mode server** (SQLite plus files) using only Docker. For a
two-replica Postgres/S3 stack, use the [development stack](deploy-docker-compose.md).

The Compose file runs the image you choose: a published
`ghcr.io/coder/openstorage` version or a local build.

## Prerequisites

- A [repository checkout](../../CONTRIBUTING.md#authenticated-installation).
- Docker with a recent Compose plugin (supports `up --wait`).
- Step 3 only: client-side curl, Git, and Git LFS.

No Go, `openstorage` binary, Postgres, or S3 is needed on the host.

## 1. Choose an image and start

Run these commands from the checkout root, in the same shell. First set
`OPENSTORAGE_IMAGE` with option A or option B.

### Option A: use a published image

Version `0.20.0` is published for linux/amd64 and linux/arm64. Pin it by version
and index digest.

The package is **private** and inherits this internal repository's access: people
who can read the repository can pull it. Log in first with a GitHub token that has
the `read:packages` scope. Without such a token, use option B.

```sh
docker login ghcr.io  # GitHub token with read:packages
export OPENSTORAGE_IMAGE=ghcr.io/coder/openstorage:0.20.0@sha256:f3f2699d785b8ed2d16e82d1ee1f55ac15a0f4536544011293208de4ddfa989f
```

### Option B: build locally

```sh
TAG="openstorage-quickstart:local-$(date +%s)-$$"
docker build -t "$TAG" .
export OPENSTORAGE_IMAGE=$(docker image inspect --format '{{.Id}}' "$TAG")
```

### Start

```sh
export OPENSTORAGE_PROJECT=openstorage-demo  # choose another name on a shared daemon
export OPENSTORAGE_PORT=8080               # choose a free loopback port

dc() { docker compose -f compose.quickstart.yml -p "$OPENSTORAGE_PROJECT" "$@"; }
dc up -d --wait
```

Keep the image, project name, and port for later commands.

- [`compose.quickstart.yml`](../../compose.quickstart.yml) has no default image. It
  pulls `OPENSTORAGE_IMAGE` only when the image is not present locally.
- The server listens only on `127.0.0.1:$OPENSTORAGE_PORT`; Git and LFS advertise
  that URL.
- `/healthz` is a **liveness** check after startup, not continuous database or
  blob-storage readiness.

<details><summary>How keys and volumes are set up</summary>

- A one-shot root `init` service prepares volume roots, generates a keypair only
  when absent, and validates an existing pair without rewriting it.
- It copies **only the public PEM** to the public volume.
- Invalid, incomplete, mismatched, or symlinked key files block startup. A failed
  `init` is not restarted.
- The nonroot server gets a read-only public volume, a writable data volume, and
  **no private-key mount**.
- The opt-in `cli` helper mounts only the private volume, read-only.
- Private modes are 0700 (directory) and 0600 (file).
- Docker daemon administrators can read all volumes.

</details>

## 2. Mint a token without installing the CLI

Keep credentials out of history, terminal output, and remote URLs. Do not enable
shell or Git tracing. This token lasts one hour and is scoped to repository `demo`:

```sh
set +x
umask 077
CLIENT_DIR=$(mktemp -d)
export OPENSTORAGE_TOKEN_FILE="$CLIENT_DIR/token"
dc run --rm --no-deps -T cli mint --key /private/org.pem --iss selfhost \
  --repo demo --scopes repo:write,git:read,git:write --ttl 1h > "$OPENSTORAGE_TOKEN_FILE"
printf 'Authorization: Bearer %s\n' "$(cat "$OPENSTORAGE_TOKEN_FILE")" > "$CLIENT_DIR/auth"
```

Optional: export the private key into a new restrictive host file, never to the
terminal or the checkout:

```sh
KEY_DIR=$(mktemp -d "$HOME/openstorage-keys.XXXXXX")
(umask 077; set -C
 dc run --rm --no-deps -T --entrypoint /bin/cat cli /private/org.pem > "$KEY_DIR/org.pem"
)
```

> [!WARNING]
> This file can mint organization tokens; store it securely. It is **not** a
> complete backup of the keypair or repository data.

## 3. Use the API, Git, and LFS

Run from the checkout root; demo files stay outside it. The askpass script reads
the token file, bypassing tokenized remotes and your credential store.

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

**Verify:** both `cmp` commands print nothing. Local storage proxies LFS uploads
**and downloads** through the loopback URL, so the client never sees a
container-only object-store address.

## 4. Stop, resume, or destroy

Three named volumes persist:

| Volume | Contents |
| --- | --- |
| `${OPENSTORAGE_PROJECT}_private` | keypair |
| `${OPENSTORAGE_PROJECT}_public` | public key copy |
| `${OPENSTORAGE_PROJECT}_data` | SQLite, blobs, rebuildable cache |

```sh
dc stop                        # pause, retain containers and all three volumes
dc up -d --wait                 # resume with initialization validation
dc down                        # remove containers/network, KEEP named volumes
dc up -d --wait                 # same keys, repos, Git, and LFS data
dc up -d --wait --force-recreate # also retains the named volumes
```

- Unexpired tokens keep working. Verify with the API read above and a fresh clone.
- A different project name selects different volumes; an empty project is not lost
  data.
- Never run more than one server on this SQLite/file topology.

> [!WARNING]
> Destructive reset, only for a disposable project. `down --volumes` irreversibly
> deletes the keys **and** all repository data.

```sh
dc --profile tools down --volumes # irreversibly deletes keys AND repository data
rm -rf -- "$CLIENT_DIR"          # remove this walkthrough's host credentials/clones
unset OPENSTORAGE_TOKEN_FILE GIT_ASKPASS GIT_TERMINAL_PROMPT
```

### If `init` fails

1. Inspect `dc logs init`.
2. Run `dc down` **without** `--volumes` and preserve the volumes.
3. Restore a known-good, consistent backup of **all three** volumes. Without one,
   reset only a project whose data you intend to discard.

> [!IMPORTANT]
> Never delete one key file or generate replacement keys over existing state. Do
> not run competing initializers or edit key volumes while the server runs.

`init` never repairs malformed files, replaces a differing public copy, or
regenerates a missing pair while a public copy remains. It changes ownership only at
mount roots, so changing the runtime UID needs an offline data migration.

## Verification and production boundary

On linux/amd64 and linux/arm64 runners, CI builds the Dockerfile without pushing,
tags it like a published image, and runs
[`scripts/smoke-compose-quickstart.sh`](../../scripts/smoke-compose-quickstart.sh)
on it. To repeat it, install the client tools plus Bash and Python 3, export
`OPENSTORAGE_IMAGE` (an image already present locally, so nothing is pulled), and
run `bash scripts/smoke-compose-quickstart.sh`.

The smoke uses disposable projects to test API/Git/LFS, old-token persistence, key
isolation, and fail-closed bootstrap faults. It then checks that its containers,
volumes, and networks are gone, and never changes the reference Compose config.

> [!IMPORTANT]
> This plain-HTTP, single-node setup is for a trusted local machine, not production.

Before deploying, plan TLS, external URLs, access control, key management and rotation, consistent
backups, resource limits, and monitoring with the
[production guide](deploy-production.md).
