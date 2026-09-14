# Test an official release → exact candidate upgrade locally

This opt-in smoke test exercises **official v0.15.0 → one exact, unreleased
candidate** on local SeaweedFS. It is not final released-pair certification,
all-v1-data compatibility, downgrade safety, AWS qualification, or S3WAL promotion.
The backend remains experimental. The final published successor needs its own
released-pair execution before the D2 promotion criterion can be completed.

## Prepare immutable inputs

Use Linux amd64, Docker, Git and Git-LFS, and the pinned tools from
[`mise.toml`](../../mise.toml) (`mise install`). GitHub release access requires
an authenticated `gh`. Keep inputs, logs and receipts outside the checkout.
Commit the candidate first: dirty source and unstamped binaries are rejected.

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

The harness pins these independently verified official bytes, not just a
caller-supplied checksum:

| Input | SHA-256 |
|---|---|
| `openstorage_0.15.0_linux_amd64.tar.gz` | `b9465088ebf371f2dcd9bed2c3012ea81fd687d39611aff3c1dfdd6ace842611` |
| `checksums.txt` | `3aea91fdc66009fbf6e67ea00405cfa88869cd8a69fe320aab52f953937c3175` |

A rebuilt v0.15.0 tag is **not** the official artifact. Changed official bytes
are an integrity failure: retain them and investigate, rather than updating the
pins or overwriting the failed attempt. Candidate identity requires its SHA-256,
clean VCS build information, and the checkout's exact commit and tree.

## Run the input gate, then the actual smoke

First run the input/rejection tests by filename. This bypasses the normal suite
bootstrap and does not execute application binaries. Then test the require-mode
boundary; that second command uses the ordinary local suite bootstrap.

```bash
go test -count=1 -race -timeout=5m \
  ./internal/githttp/upgrade_inputs_test.go \
  ./internal/githttp/upgrade_inputs_unit_test.go
OPENSTORAGE_TEST_BACKEND=local go test ./internal/githttp \
  -run '^TestUpgradeRequireMode$' -count=1 -race -timeout=5m
```

Then run the dedicated require-mode flow. Its `TestMain` bypasses the shared
suite's automatic candidate build and shared node/bucket. Missing inputs,
selection overrides, and an existing attempt directory fail explicitly.
There is no download, lease disabling, or implicit retry in the test.

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

Use native Git-LFS on `PATH` if a tool-manager shim depends on the original
`HOME`; the fixture deliberately isolates `HOME` and Git configuration. Likewise,
ensure subprocess `go` resolves the pinned compiler. Never change global Git or
tool-manager settings to make the test pass.

## What a passing receipt proves

1. The verified archive and candidate are frozen into the exclusive attempt.
   The official executable comes from that archive, not a source rebuild.
2. A private, initially empty bucket and fresh caches serve only one version at
   a time. Every node has an observed active one-minute instance lease.
3. The official writer creates two Git commits, materialized LFS bytes, a
   repository record, and a webhook with an encrypted secret. Org signing and
   encryption configuration are generated privately and reused unchanged.
4. After SIGTERM, clean exit/reap, lease absence and a two-second all-key
   hash/metadata quiet window, the cold candidate authenticates the old token,
   rejects an invalid token, and compares repository and webhook values
   (identities, names/URL, default branch and creation times). Clone HEAD,
   `git fsck --strict`, and materialized LFS bytes must match.
5. A candidate Git/LFS write produces a webhook whose HMAC verifies with the
   original secret and names the new HEAD. The second cold restart must serve
   the new HEAD and bytes. It also stops cleanly and releases its lease.

`inputs.json`, frozen input files, `phases.log`, active lease documents,
quiescent inventories, per-process stderr, and `inner-exit.txt` remain under the
attempt. The outer exit is separate: a missing inner receipt, killed process,
partial log, or green recording wrapper is not a passing smoke.

## Evidence and cleanup

Keep raw terminal capture and exact exits. Capture a screenshot and video of the
phase output for review, labeling any replay, speed change, or trimmed idle time.
Never upload keys, tokens, environment dumps, private `git.log`, or server stderr. Inspect
public artifacts before attaching them; verify uploaded bytes with authenticated
downloads. The phase output contains no credentials.

The test cleans up its own processes, caches, Git repositories, ephemeral keys,
receiver, and disposable SeaweedFS container, including its private bucket.
After an external kill, inspect the recorded process/container state and clean
up only resources belonging to that attempt. Retain failed receipts and original
inputs before diagnosing a failure. A deliberate rerun uses a **new** attempt
name after explaining the cause; it does not replace the failed evidence.

The normal [testing gates](../reference/testing.md#test-lanes) remain required.
Ordinary CI skips this artifact-dependent flow; green CI alone is not upgrade
coverage. No product parser or hot path changed, so this harness adds neither a
product fuzz target nor a performance benchmark.
