# Test an official release → exact candidate upgrade locally

Run the opt-in upgrade smoke: **official v0.15.0 → one exact, unreleased candidate**
on local SeaweedFS.

> [!IMPORTANT]
> Not final released-pair certification, all-v1-data compatibility, downgrade
> safety, AWS qualification, or S3WAL promotion; the backend stays experimental. The
> published successor still needs its own released-pair run to complete D2.

## Prerequisites

- Linux amd64, Docker, Git, Git-LFS, and authenticated `gh`.
- Pinned tools from [`mise.toml`](../../mise.toml) (`mise install`).
- A committed candidate: dirty source and unstamped binaries are rejected.
- Inputs, logs, and receipts go outside the checkout.

## Prepare immutable inputs

Download and verify the official release, then build the candidate:

```bash
set -euo pipefail
umask 077
EVIDENCE=$(mktemp -d "$HOME/openstorage-upgrade.XXXXXX")
mkdir "$EVIDENCE/inputs"
# Deliberate operator acquisition, never performed by ordinary tests.
TAG=$(git ls-remote origin refs/tags/v0.15.0 | cut -f1)
test "$TAG" = d65da6e53ee62145a0cb78a0f89bf6839c8f9171
gh release download v0.15.0 --repo coder/openstorage \
  --pattern openstorage_0.15.0_linux_amd64.tar.gz --pattern checksums.txt \
  --dir "$EVIDENCE/inputs"
(cd "$EVIDENCE/inputs" && sha256sum --check --ignore-missing checksums.txt)
sha256sum "$EVIDENCE/inputs/"*
test -z "$(git status --porcelain)"
COMMIT=$(git rev-parse HEAD)
TREE=$(git rev-parse HEAD^{tree})
go build -buildvcs=true -o "$EVIDENCE/inputs/candidate" ./cmd/openstorage
CANDIDATE_SHA256=$(sha256sum "$EVIDENCE/inputs/candidate" | cut -d' ' -f1)
go version -m "$EVIDENCE/inputs/candidate"
```

The harness pins these verified official bytes, not a caller-supplied checksum:

| Input | SHA-256 |
|---|---|
| `openstorage_0.15.0_linux_amd64.tar.gz` | `b9465088ebf371f2dcd9bed2c3012ea81fd687d39611aff3c1dfdd6ace842611` |
| `checksums.txt` | `3aea91fdc66009fbf6e67ea00405cfa88869cd8a69fe320aab52f953937c3175` |

A rebuilt v0.15.0 tag is **not** the official artifact. Candidate identity is its
SHA-256, clean VCS build info, and the exact commit and tree.

> [!WARNING]
> Changed official bytes are an integrity failure: keep them and investigate. Never
> update the pins or overwrite the failed attempt.

## Run the input gate, then the actual smoke

**1. Input gate.** The first command runs input/rejection tests by filename,
without suite bootstrap or application binaries. The second tests the require-mode
boundary with the normal local bootstrap:

```bash
go test -count=1 -race -timeout=5m \
  ./internal/githttp/upgrade_inputs_test.go \
  ./internal/githttp/upgrade_inputs_unit_test.go
OPENSTORAGE_TEST_BACKEND=local go test ./internal/githttp \
  -run '^TestUpgradeRequireMode$' -count=1 -race -timeout=5m
```

**2. Upgrade flow.** Its own `TestMain` skips the shared candidate build and shared
node/bucket. Missing inputs, selection overrides, or an existing attempt directory
fail explicitly. It never downloads, disables leases, or retries:

```bash
# Do not create this directory: the harness must create it exclusively.
ATTEMPT="$EVIDENCE/attempt-001"
test ! -e "$ATTEMPT" && test ! -e "$ATTEMPT.log"
set +e
OPENSTORAGE_TEST_UPGRADE=require \
OPENSTORAGE_UPGRADE_ATTEMPT="$ATTEMPT" \
OPENSTORAGE_UPGRADE_ARCHIVE="$EVIDENCE/inputs/openstorage_0.15.0_linux_amd64.tar.gz" \
OPENSTORAGE_UPGRADE_CHECKSUMS="$EVIDENCE/inputs/checksums.txt" \
OPENSTORAGE_UPGRADE_CANDIDATE="$EVIDENCE/inputs/candidate" \
OPENSTORAGE_UPGRADE_CANDIDATE_SHA256="$CANDIDATE_SHA256" \
OPENSTORAGE_UPGRADE_COMMIT="$COMMIT" OPENSTORAGE_UPGRADE_TREE="$TREE" \
go test ./internal/githttp -run '^TestS3WAL_ReleaseUpgrade$' \
  -count=1 -race -v -timeout=10m > "$ATTEMPT.log" 2>&1
status=$?
printf '%s\n' "$status" > "$ATTEMPT.outer-exit.txt"
set -e
printf 'go test exit=%s; evidence=%s\n' "$status" "$EVIDENCE"
test "$status" = 0
test "$(cat "$ATTEMPT/inner-exit.txt")" = 0
```

**If tools are not found:** the fixture isolates `HOME` and Git config. Put native
Git-LFS on `PATH` if a shim needs the original `HOME`, and make subprocess `go`
resolve the pinned compiler. Never change global Git or tool-manager settings.

## What a passing receipt proves

1. The verified archive and candidate are frozen into the exclusive attempt; the
   official executable comes from the archive, not a rebuild.
2. A private, initially empty bucket and fresh caches serve one version at a time,
   each node with an observed active one-minute instance lease.
3. The official writer creates two commits, materialized LFS bytes, a repository
   record, and a webhook with an encrypted secret. Org signing and encryption config
   are generated privately and reused unchanged.
4. After SIGTERM, clean exit/reap, lease absence, and a two-second all-key
   hash/metadata quiet window, the cold candidate:
   - authenticates the old token and rejects an invalid one;
   - matches repository and webhook values (identities, names/URL, default branch,
     creation times);
   - matches clone HEAD, `git fsck --strict`, and materialized LFS bytes.
5. A candidate Git/LFS write yields a webhook whose HMAC verifies with the original
   secret and names the new HEAD. A second cold restart serves the new HEAD and
   bytes, stops cleanly, and releases its lease.

The attempt keeps `inputs.json`, frozen inputs, `phases.log`, active lease documents,
quiescent inventories, per-process stderr, and `inner-exit.txt`.

> [!IMPORTANT]
> The outer exit is separate. A missing inner receipt, killed process, partial log,
> or green recording wrapper is not a pass.

## Evidence and cleanup

**Evidence:** keep raw terminal capture and exact exits. Capture a screenshot and
video of the phase output, labeling any replay, speed change, or trimmed idle time.
The phase output has no credentials, but never upload keys, tokens, environment
dumps, private `git.log`, or server stderr. Inspect artifacts before attaching and
verify uploads with authenticated downloads.

**Cleanup:** the test removes its processes, caches, Git repositories, ephemeral
keys, receiver, and disposable SeaweedFS container with its bucket.

- After an external kill, inspect the recorded process/container state and clean up
  only that attempt's resources.
- Keep failed receipts and original inputs before diagnosing.
- A deliberate rerun uses a **new** attempt name, after explaining the cause. It
  never replaces failed evidence.

The normal [testing gates](../reference/testing.md#test-lanes) still apply. Ordinary
CI skips this flow, so green CI alone is not upgrade coverage.
