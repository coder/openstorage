# Retain private SeaweedFS test logs

The three managed SeaweedFS fixtures can retain server logs through automatic test teardown, including startup failure.
This covers the shared S3WAL helper, both blob authentication variants, and the `githttp` TestMain fixture.
It does not capture external S3 services, Floci, MinIO, or production servers.

## Enable capture

Create a private directory before the test process starts.
Set `OPENSTORAGE_TEST_ARTIFACT_DIR` to that directory and set an explicit `TESTCONTAINERS_SESSION_ID`.
Use a new session and directory for each process.
Session IDs must contain 1–64 ASCII letters, digits, underscores, or hyphens.
The artifact directory must have mode `0700` and must not be a symbolic link.

```sh
umask 077
artifacts=$(mktemp -d)
TESTCONTAINERS_SESSION_ID="capture-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')" \
OPENSTORAGE_TEST_ARTIFACT_DIR="$artifacts" \
  go test ./internal/blob -run '^TestS3_ExpectedBucketOwnerHarmlessOnSeaweedFS$' -count=1 -race
```

If the artifact variable is unset or empty, fixture behavior stays unchanged.
This variable is test-only, not server configuration.
Capture adds file writes and synchronization, so do not enable it for performance comparisons.
No new fuzz target or benchmark is required: this change does not alter application parsing or storage algorithms.
Existing fuzz workers that skip fixture startup do not create captures.

On shared Docker hosts, hold the host's existing container lifecycle lock during real fixture tests.
Check usable disk space before and after lock acquisition and before each heavy phase.
Allow for SeaweedFS's disk floor and the test workload.
Keep the lock until test producers finish and their exact-session resources are gone.
Do not remove resources from other sessions or hold the lock during GitHub review waits.

## Interpret the artifacts

Each fixture creates `seaweed-<session>-<full-container-id>/` with mode `0700`.
The helper verifies the container's Testcontainers session label before capture.
The directory contains three mode-`0600` files:

- `stdout.log`: The first 1 MiB of observed standard output.
- `stderr.log`: The first 1 MiB of observed standard error.
- `status.json`: A version-1 capture receipt, which records capture state and errors.

The receipt contains `version`, `session`, `container`, `phase`, `coverage`, `stdout_bytes`, `stderr_bytes`, `truncated`, and `errors`.
`phase` starts as `pending` and becomes `finalized` after the pinned Testcontainers stop/drain path, before removal.
A missing receipt, a pending receipt after exit, or nonempty `errors` means that evidence is incomplete.
`truncated: true` means that at least one stream exceeded its limit.
An empty stdout file is normal when SeaweedFS writes only to stderr.

Error codes include `startup-error`, `stream-error`, `drain-timeout`, `producer-stop-error`, `producer-ack-timeout`, and `capture-context-canceled`.
File failures use `write-error`, `sync-error`, `close-error`, or `receipt-write-error`.
Missing evidence uses `missing-producer`, `missing-output`, `missing-file`, or `missing-receipt`.
The helper also emits a sanitized `SEAWEED_CAPTURE <json>` line to test stderr after finalization.
Use this fallback if the receipt cannot be written.
Capture errors do not replace the native test result or prevent teardown.

`coverage: "best-effort"` does not promise gapless logs, even with no errors.
The pinned Testcontainers producer can reconnect and skip records.
Capture bounds retained files, not the library's memory use while it reads startup-error logs.
Abrupt process termination or failed container stops can leave pending evidence.
The fixture does not supervise test processes or perform a global Docker cleanup.

Do not publish raw logs without a separate secret review.
Server logs can contain credentials, request details, or private repository names.
The helper suppresses Testcontainers' raw startup-log dumps when capture is enabled.
It does not redact retained files, test output from other sources, or upload anything.

## Prove retention after a failed TestMain test

This procedure uses the real default `githttp` TestMain stack, including Postgres and automatic teardown.
It waits for a captured SeaweedFS startup event, then deliberately fails one selected test.
Ordinary CI skips this diagnostic unless `-seaweed-capture-fail` is supplied.
Run these commands from the repository root under the resource precautions above.

```sh
umask 077
work=$(mktemp -d)
mkdir -m 700 "$work/logs"
export TESTCONTAINERS_SESSION_ID="capture-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
export OPENSTORAGE_TEST_ARTIFACT_DIR="$work/logs"
unset OPENSTORAGE_TEST_BACKEND OPENSTORAGE_TEST_S3_ENDPOINT OPENSTORAGE_TEST_UPGRADE
go test -c -race -o "$work/githttp.test" ./internal/githttp
if (cd internal/githttp && "$work/githttp.test" \
    -test.run '^TestSeaweedCaptureDiagnostic$' -test.count=1 -test.v \
    -test.timeout=4m -seaweed-capture-fail) >"$work/native.log" 2>&1; then
  native_exit=0
else
  native_exit=$?
fi
printf '%s\n' "$native_exit" >"$work/native.exit"
python3 - "$work" <<'PY'
import json, os, pathlib, sys
work = pathlib.Path(sys.argv[1])
assert (work / 'native.exit').read_text().strip() == '1'
output = (work / 'native.log').read_text()
assert 'SEAWEED_CAPTURE_ACK session=' + os.environ['TESTCONTAINERS_SESSION_ID'] in output
assert 'SEAWEED_CAPTURE_DIAGNOSTIC deliberate failure after capture acknowledgment' in output
receipts = list((work / 'logs').glob('seaweed-*/status.json'))
assert len(receipts) == 1
status = json.loads(receipts[0].read_text())
assert status['session'] == os.environ['TESTCONTAINERS_SESSION_ID']
assert status['phase'] == 'finalized' and not status['errors'] and not status['truncated']
assert b'Start Seaweed S3 API Server' in (receipts[0].parent / 'stderr.log').read_bytes()
print('Native test exit: 1 (deliberate failure); retained-evidence proof: PASS')
PY
```

The native test must exit 1 only after the acknowledgment and deliberate failure message.
The Python verifier must exit 0 independently.
Make sure that the process and exact-session Docker resources are gone before releasing the lifecycle lock.
Keep the native exit, raw receipts, and logs together for investigation.
For a review, record only a sanitized verifier session, with screenshots and a video that show both verdicts.
This local emulator proof does not establish real-AWS behavior or promote the experimental S3WAL backend.
