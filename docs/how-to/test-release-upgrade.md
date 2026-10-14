# Test an official release → exact candidate upgrade locally

Run the opt-in upgrade smoke: **official v0.18.0 → one exact, unreleased candidate →
official v0.18.0** on local SeaweedFS. v0.18.0 is the last release that writes S3WAL
metadata format v1, so the run also covers the
[ADR-51](../adr/051-s3wal-metadata-v2-is-one-versioned-document-converted-by-a-fenced-command.md)
conversion to v2 and the supported `s3wal-meta convert --to 1` downgrade.

> [!IMPORTANT]
> Not final released-pair certification, all-v1-data compatibility, AWS
> qualification, or S3WAL promotion; the backend stays experimental. The published
> successor still needs its own released-pair run to complete D2.

## Prerequisites

- Linux amd64, Docker, Git, Git-LFS, and authenticated `gh`.
- Pinned tools from [`mise.toml`](../../mise.toml) (`mise install`).
- A committed candidate: dirty source and unstamped binaries are rejected.
- Inputs, logs, and receipts go outside the checkout, under `EVIDENCE_BASE`
  (default `${TMPDIR:-/tmp}`). Pick a filesystem that allows executables (the
  attempt runs its frozen binaries) and keeps files until you archive them.

## Prepare immutable inputs

Download and verify the official release, then build the candidate:

```bash
set -euo pipefail
umask 077
EVIDENCE=$(mktemp -d "${EVIDENCE_BASE:-${TMPDIR:-/tmp}}/openstorage-upgrade.XXXXXX")
mkdir "$EVIDENCE/inputs"
# Deliberate operator acquisition, never performed by ordinary tests.
TAG=$(git ls-remote origin refs/tags/v0.18.0 | cut -f1)
test "$TAG" = 1438d3dc1e8913c5f26995ad4b251077e39be55e
gh release download v0.18.0 --repo coder/openstorage \
  --pattern openstorage_0.18.0_linux_amd64.tar.gz --pattern checksums.txt \
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
| `openstorage_0.18.0_linux_amd64.tar.gz` | `943bba3db97668e76a8dcfe09cb9da6edc09793337468d0ec07b10dae307120d` |
| `checksums.txt` | `491bd396f802e5810cc84e14e5db64e50e68d358189c2edbdabd2decc0495043` |

The archive holds `openstorage` and `LICENSE`. The harness extracts only the binary.
A rebuilt v0.18.0 tag is **not** the official artifact. Candidate identity is its
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
OPENSTORAGE_UPGRADE_ARCHIVE="$EVIDENCE/inputs/openstorage_0.18.0_linux_amd64.tar.gz" \
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
   hash/metadata quiet window:
   - the candidate refuses the v1 bucket (`document version 1 needs conversion`);
   - `s3wal-meta convert --to 2` verifies its result and releases its lease;
   - the official release refuses the v2 bucket (`document version 2 unsupported`).

   Neither refusal changes `meta.json` or leaves a lease. The cold candidate then:
   - authenticates the old token and rejects an invalid one;
   - matches repository and webhook values (identities, names/URL, default branch,
     creation times);
   - matches clone HEAD, `git fsck --strict`, and materialized LFS bytes.
5. A candidate Git/LFS write yields a webhook whose HMAC verifies with the original
   secret and names the new HEAD. A second cold restart serves the new HEAD and
   bytes, creates a repository, stops cleanly, and releases its lease. The stopped
   v2 `meta.json` stores `create_state: 2` for that repository.
6. `s3wal-meta convert --to 1` verifies, the candidate refuses the result, and the
   cold official release passes the same checks as step 4 on the candidate's HEAD and
   LFS bytes. It also answers an authenticated `GET /api/v1/repo` for the
   candidate-created repository with the same ID, name, and default branch.

The attempt keeps `inputs.json`, frozen inputs, `phases.log`, active lease documents,
quiescent inventories, per-process stderr, `convert-to-*.out`, and `inner-exit.txt`.

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
keys, receiver, and disposable SeaweedFS container with its bucket. It never
removes `$EVIDENCE`.

- After an external kill, inspect the recorded process/container state and clean up
  only that attempt's resources.
- Keep failed receipts and original inputs before diagnosing.
- A deliberate rerun uses a **new** attempt name, after explaining the cause. It
  never replaces failed evidence.

The normal [testing gates](../reference/testing.md#test-lanes) still apply. Ordinary
CI skips this flow, so green CI alone is not upgrade coverage.
