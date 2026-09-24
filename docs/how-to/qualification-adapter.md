# Run a qualification command with supervised cleanup

`scripts/qualification.py` runs one approved qualification command on Linux.
It stops and reaps the command's descendants before Docker cleanup or lock release.
It does not change existing test lanes or production process handling.

## Requirements and limits

Use Python 3.9 or newer and Linux with working child-subreaper, pidfd, and `/proc` child-list support.
A pidfd is a kernel handle for one process.
A child subreaper adopts orphaned descendants so it can wait for their exit.
The adapter refuses admission when its required kernel operations fail.

Run the adapter as a dedicated, single-thread process on the Docker host.
Supply an existing coordinator-owned lifecycle lock, the local Unix socket, and the expected Docker daemon ID and data directory.
The data directory must resolve to the daemon's actual backing filesystem in the adapter's mount namespace.
The artifact directory must use that same filesystem.
Remote Docker, Docker Desktop, alternate mount mappings, and hostile commands are outside this contract.

Use a fresh `TESTCONTAINERS_SESSION_ID` for each command.
It must be 1 to 64 ASCII letters, digits, underscores, or hyphens, the same rule the SeaweedFS log-capture fixture enforces.
The adapter refuses other values before it takes the lock.
The command and every fixture must honor this label and the supplied Docker endpoint.
Do not share the session label or replace the daemon endpoint during a run.
The adapter rejects preexisting resources with that label and conflicting Docker configuration.
It also rejects `~/.testcontainers.properties`, because `tc.host` takes precedence over `DOCKER_HOST` in the pinned Testcontainers library.
Use a private `HOME` when the normal home directory contains that file.

The command must not delegate producers to an external service, debugger, scheduler, or another user.
This adapter supervises descendants. It does not contain hostile code or survive `SIGKILL`, a host crash, or interpreter failure.
Do not wrap it in a timeout that kills the supervisor while it owns the lock.

## Run the harmless tests first

The checked-in tests use private Unix servers, fake resources, finite native commands, and private locks.
They do not access Docker.
They shorten timing constants and supply deterministic filesystem observations inside the adapter test process.

```sh
python3 -B -m unittest discover -s scripts -p 'test_qualification.py' -v
```

The tests cover secondary process groups, new sessions, reparented children, TERM resistance, and native exit status.
They witness pidfd authorization and reap ordering while a separately owned sentinel stays alive.
Query failures, truncated bodies, partial deletion, capacity loss, cancellation, and lock contention must not cause unsafe release.
These tests do not qualify real Docker fixtures, AWS, or any storage backend.

## Invoke an approved command

Before using real Docker, obtain the host coordinator's approval and the existing lifecycle lock path.
Obtain the expected daemon ID, Unix socket, and backing directory from that coordinator.
Do not create a replacement lock.
Keep fixture capture independent: fixtures own capture attachment and draining before their automatic removal.

Create a new private artifact directory on the verified backing filesystem:

```sh
export OPENSTORAGE_TEST_ARTIFACT_DIR="$(mktemp -d /approved/artifact-parent/qualification-XXXXXX)"
chmod 700 "$OPENSTORAGE_TEST_ARTIFACT_DIR"
export TESTCONTAINERS_SESSION_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"

python3 scripts/qualification.py \
  --lock /approved/existing-lifecycle.lock \
  --docker-socket /var/run/docker.sock \
  --daemon-id EXPECTED_DAEMON_ID \
  --backing-directory /var/lib/docker \
  --headroom-bytes 5368709120 \
  --timeout 600 \
  -- APPROVED_COMMAND ARGUMENTS
```

Replace every placeholder with the coordinator's values and the approved command.
The example reserves 5 GiB of workload headroom. Choose enough headroom for the actual workload.
The minimum available capacity is the larger of 60,696,476,550 bytes and 1% of filesystem size plus that headroom.
The adapter uses `f_bavail`, not reserved blocks from `f_bfree`.
It checks capacity before and after lock acquisition, before command launch, and periodically during execution and HOLD.

## Read the receipt and cancel safely

The adapter creates `qualification.json` exclusively in the artifact directory.
It replaces later snapshots atomically and preserves the first error in at most 240 characters.
Treat an unreadable or incomplete receipt as unknown state, never as permission to release a lock.
Command arguments, environment values, and Docker response bodies do not enter this receipt.
Native stdout and stderr still reach the caller, so sanitize captured output before publication.

The version-1 receipt contains these fields:

- `native_status`: The actual leader exit code, or a negative signal number. `null` means no native exit was recorded.
- `quiescent`: The supervisor established kernel `ECHILD` after reaping all owned children, or never launched a command.
- `state`: `admission`, `running`, `cleanup`, `hold`, or `complete`.
- `session_id`, `error`, `interrupted`, `timed_out`, and `lock_held`: Ownership and failure information.
- `capacity_available`, `capacity_required`, `capacity_min_available`, and `capacity_samples`: Bounded capacity observations in bytes and sample count.
- `reaped`: The number of native children reaped by the supervisor.

The leader can exit before its descendants.
Each signal requires a validated pidfd, and each child remains unreaped until its last signal.
The supervisor stays alive until kernel `ECHILD` proves that no children remain, including Linux clone-child wait classes.
An empty process-list read is not this proof.

For cancellation, send `T` or `I` through the private nonblocking `qualification.control` FIFO.
A FIFO is a local byte channel.
Use a nonblocking descriptor and retain the artifact directory until the adapter exits:

```sh
python3 - <<'PY'
import os
from pathlib import Path
path = Path(os.environ["OPENSTORAGE_TEST_ARTIFACT_DIR"]) / "qualification.control"
fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
try:
    os.write(fd, b"T")
finally:
    os.close(fd)
PY
```

The adapter also records direct TERM, INT, and HUP signals without unwinding ownership.
Signals that arrive after it starts writing the final receipt do not change the result, so the receipt and the exit status agree.
It gives stopped producers a grace period, then uses pidfd SIGKILL when required.
The command timeout starts after launch. Lock admission waits remain cancellable through the same channel.

## Recover a HOLD

Do not kill the supervisor or launch another producer when the receipt says `hold`.
The adapter retains the lock and starts no new command.
After a cleanup error, it stops automatic deletion and makes only bounded, read-only reconciliation requests.
Each request has both an inactivity timeout and an absolute deadline.

If `quiescent` is false, restore the failed supervision capability before resource cleanup.
If `quiescent` is true, investigate the recorded error using the original daemon and session label.
Restore query access first. If resources remain, ask the coordinator to inspect and remove only confirmed resources from this run.
Do not adopt preexisting resources from a reused session ID.
The adapter releases the lock only after successful empty container, network, and volume inventories and a writable final receipt.
There is no automatic handoff or force-release command.

Cleanup uses exact session-label filters and checks full container/network IDs or volume names and labels before each deletion.
Another cleaner, such as Ryuk or container auto-removal, can remove a listed resource first.
A 404 response during that resource's inspection or deletion is not an error, and the adapter never deletes a resource it could not inspect.
Cleanup still succeeds only when the final scoped inventories are empty.
It does not prune, remove images, or implicitly delete container volumes.
Volume-name safety also requires the cooperative lifecycle lock and exclusive session label.

A cleanup or supervision error returns `70` after safe reconciliation.
Cancellation returns `128 + signal`, and command timeout returns `124` after shutdown and cleanup.
Otherwise the adapter returns the native command status, with negative signal status converted to `128 + signal`.
Always read `native_status` separately from the adapter's result.
A forced supervisor death requires coordinator recovery and is never a successful result.
