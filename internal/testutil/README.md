# `internal/testutil`

## Overview
Shared test helpers. This directory has no Go files of its own. Its four child packages pin the container images that tests start, capture container logs for CI artifacts, inject COMMIT faults into PostgreSQL connections, and hold a test server's port while the server is down.

## Subdirectories & Boundaries
- `heldport`: [./heldport/README.md](./heldport/README.md) - `Listen` and `Relisten` keep a test server's TCP port reserved while the server is down, so a restart reuses the address, and in between only a socket that also sets `SO_REUSEPORT` and binds that exact port can take it (on Linux, only from the same user)
- `pgfault`: [./pgfault/README.md](./pgfault/README.md) - a TCP proxy for PostgreSQL that loses, drops, holds, or delays the next COMMIT, for tests of a COMMIT whose outcome the client never hears
- `seaweedcapture`: [./seaweedcapture/README.md](./seaweedcapture/README.md) - wraps `testcontainers.GenericContainer`; when `OPENSTORAGE_TEST_ARTIFACT_DIR` is set it writes each container's stdout, stderr, and a JSON receipt to a private directory. Setup checks fail closed: `[seaweedcapture/capture.go:start]` returns an error and creates no container when `TESTCONTAINERS_SESSION_ID` is missing or unsafe, the artifact directory is not a mode-0700 directory or cannot be opened, or the request is not `Started` or already has log configuration, and the PostCreate hook `[seaweedcapture/capture.go:attach]` fails container creation on a container attribution or session-label mismatch. Runtime capture-file failures, a failed container creation (`startup-error`), and a failed cleanup after it (`teardown-error`) become receipt problems (`[seaweedcapture/capture.go:problem]`); capture-file failures never change the fixture result
- `testimages`: [./testimages/README.md](./testimages/README.md) - two pinned image references, `Postgres` and `SeaweedFS`, as tag plus index digest; no executable code
