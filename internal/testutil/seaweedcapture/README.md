# `internal/testutil/seaweedcapture`

## Overview
Test helper that wraps `testcontainers.GenericContainer`. When `OPENSTORAGE_TEST_ARTIFACT_DIR` is set, `Start` attaches a log consumer to each started container and writes its stdout and stderr logs plus a JSON receipt to a private directory. Otherwise it creates the container unchanged. Setup and attribution checks fail `Start`: a bad session ID, a bad artifact dir, a request that is not started or already has log configuration [capture.go:start], and a container ID, session ID, or session-label mismatch in the `PostCreates` hook [capture.go:attach]. Runtime capture-file failures go into the receipt and never change the fixture result [capture.go:problem].

## Interface
- `Start` — wraps `GenericContainer`; keeps private logs and a receipt when `OPENSTORAGE_TEST_ARTIFACT_DIR` is set, otherwise delegates unchanged [capture.go:Start]
- Env `OPENSTORAGE_TEST_ARTIFACT_DIR` — artifact root, which must exist with mode 0700 [capture.go:start]
- Env `TESTCONTAINERS_SESSION_ID` — must match `^[a-zA-Z0-9_-]{1,64}$` [capture.go:start]
- Label `org.testcontainers.sessionId` — checked through Inspect against the session ID [capture.go:attach]
- Files `status.json`, `status.tmp`, `stdout.log`, `stderr.log` in a per-container directory `seaweed-<session>-<containerID>` [capture.go:save]
- `SEAWEED_CAPTURE` — stderr line prefix for the final receipt JSON [capture.go:report]
- Unexported: `start` (core of `Start` with an injectable create func), `capture` (log consumer with `Accept` and `Printf`), `receipt`, `streamLimit` (1 MiB per stream) [capture.go:start]

## Invariants
- Capture runs only when `OPENSTORAGE_TEST_ARTIFACT_DIR` is non-empty [capture.go:start]
- A bad session ID, an artifact dir that is a symlink, not mode 0700, or cannot be opened, or a request that already has `LogConsumerCfg` or `Logger` or has `Started == false` fails startup before any container is created [capture.go:start]
- The container ID must match `^[a-f0-9]{64}$`, `container.SessionID()` must equal the capture session, and the Inspect session label must equal the capture session. A mismatch returns an error from the `PostCreates` hook, which fails container creation [capture.go:attach]
- Each stream is capped at `streamLimit`; excess bytes are dropped and `Truncated` is set [capture.go:Accept]
- Runtime capture errors never replace the fixture result: `attach` returns nil on file errors [capture.go:attach]
- Logs after `finished` are ignored; `finish` runs once and waits at most 2 s for the producer [capture.go:finish]
- The receipt holds only fixed error codes and validated identities, never log content [capture.go:save]

## Side Effects
- Reads `OPENSTORAGE_TEST_ARTIFACT_DIR` and `TESTCONTAINERS_SESSION_ID` [capture.go:start]
- Creates the per-container dir (0700) and `stdout.log`/`stderr.log` with `O_EXCL` and mode 0600 [capture.go:attach]
- Writes `status.tmp` and renames it to `status.json` [capture.go:save]
- Terminates the container with a 30 s timeout on startup error [capture.go:start]
- Mutates `req.LifecycleHooks`, `req.Logger`, and `req.LogConsumerCfg` before creating the container [capture.go:start]
- Writes the `SEAWEED_CAPTURE` receipt line to stderr [capture.go:report]
- Panics when JSON marshal fails on the save or report path [capture.go:save]

## Verification
- Run: `go test ./internal/testutil/seaweedcapture/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`. This default run uses fake containers and needs no Docker. It skips `TestRealFixtureCapture`, which runs only when `OPENSTORAGE_TEST_ARTIFACT_DIR` is set [fixture_test.go:TestRealFixtureCapture]
- Opt-in real-fixture lane (needs Docker, starts SeaweedFS containers). Use the env-carrying form from `docs/how-to/capture-seaweedfs-test-logs.md` with this package: `umask 077; artifacts=$(mktemp -d); TESTCONTAINERS_SESSION_ID="capture-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')" OPENSTORAGE_TEST_ARTIFACT_DIR="$artifacts" go test ./internal/testutil/seaweedcapture -run '^TestRealFixtureCapture$' -count=1 -race`
