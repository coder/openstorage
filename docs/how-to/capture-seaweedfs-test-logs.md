# Retain private SeaweedFS test logs

Keep SeaweedFS server logs from test fixtures after automatic teardown, including
startup failures. Use this to debug fixture failures.

Covered: the three managed fixtures (the shared S3WAL helper, both blob
authentication variants, and the `githttp` TestMain fixture). Not covered: external
S3 services, Floci, MinIO, or production servers.

## Enable capture

Before the test process starts:

1. Create a private directory: mode `0700`, not a symbolic link.
2. Set `OPENSTORAGE_TEST_ARTIFACT_DIR` to it.
3. Set an explicit `TESTCONTAINERS_SESSION_ID` of 1–64 ASCII letters, digits,
   underscores, or hyphens.

Use a new session and directory for each process:

```sh
umask 077
artifacts=$(mktemp -d)
TESTCONTAINERS_SESSION_ID="capture-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')" \
OPENSTORAGE_TEST_ARTIFACT_DIR="$artifacts" \
  go test ./internal/blob -run '^TestS3_ExpectedBucketOwnerHarmlessOnSeaweedFS$' -count=1 -race
```

- Unset or empty artifact variable: fixtures behave as before.
- The variable is test-only, not server configuration.
- Capture adds file writes and syncs; keep it off for performance comparisons.
- Fuzz workers that skip fixture startup create no captures.

**On shared Docker hosts:**

- Hold the host's container lifecycle lock during real fixture tests.
- Check usable disk space before and after taking the lock and before each heavy
  phase. Allow for SeaweedFS's disk floor plus the test workload.
- Keep the lock until test producers finish and their exact-session resources are
  gone. Never remove other sessions' resources or hold the lock during GitHub review
  waits.

## Interpret the artifacts

Each fixture creates `seaweed-<session>-<full-container-id>/` (mode `0700`) after
checking the container's Testcontainers session label. It holds three mode-`0600`
files:

- `stdout.log`: The first 1 MiB of observed standard output.
- `stderr.log`: The first 1 MiB of observed standard error.
- `status.json`: A version-1 capture receipt, which records capture state and errors.

The receipt contains `version`, `session`, `container`, `phase`, `coverage`, `stdout_bytes`, `stderr_bytes`, `truncated`, and `errors`.

| Receipt state | Meaning |
|---|---|
| `phase: pending` | Capture running. It becomes `finalized` after the pinned Testcontainers stop/drain path, before removal. |
| Missing receipt, `pending` after exit, or nonempty `errors` | Evidence is incomplete. |
| `truncated: true` | At least one stream hit its limit. |
| Empty `stdout.log` | Normal: SeaweedFS may write only to stderr. |

<details><summary>Error codes</summary>

- Capture: `startup-error`, `stream-error`, `drain-timeout`, `producer-stop-error`,
  `producer-ack-timeout`, `capture-context-canceled`.
- Files: `setup-error`, `write-error`, `sync-error`, `close-error`,
  `receipt-write-error`.
- Missing evidence: `missing-producer`, `missing-output`, `missing-file`,
  `missing-receipt`.

</details>

After finalization the helper also prints a sanitized `SEAWEED_CAPTURE <json>` line
to test stderr. Use it if the receipt cannot be written. Capture errors never
change the native test result or block teardown.

**Limits:**

- `coverage: "best-effort"` does not promise gapless logs, even without errors: the
  pinned Testcontainers producer can reconnect and skip records.
- Capture bounds retained files, not the library's memory while it reads
  startup-error logs.
- Abrupt termination or failed container stops can leave pending evidence.
- The fixture does not supervise test processes or clean up Docker globally.

> [!WARNING]
> Never publish raw logs without a separate secret review. They can contain
> credentials, request details, or private repository names. With capture enabled
> the helper suppresses Testcontainers' raw startup-log dumps, but it does not
> redact retained files or other test output, and uploads nothing.

## Prove retention after a failed TestMain test

This runs the real default `githttp` TestMain stack, including Postgres and
automatic teardown. It waits for a captured SeaweedFS startup event, then fails one
selected test on purpose. Ordinary CI skips it unless `-seaweed-capture-fail` is
set. Run from the repository root under the shared-host precautions above:

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

**Verify:**

1. The native test exits 1, and only after the acknowledgment and the deliberate
   failure message.
2. The Python verifier exits 0 on its own.
3. The process and exact-session Docker resources are gone before you release the
   lifecycle lock.

Keep the native exit, raw receipts, and logs together. For a review, record only a
sanitized verifier session, with screenshots and a video showing both verdicts.

> [!NOTE]
> This local emulator proof does not establish real-AWS behavior or promote the
> experimental S3WAL backend.
