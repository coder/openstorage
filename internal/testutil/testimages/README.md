# `internal/testutil/testimages`

## Overview
Pins the container images that tests and fixtures start. The package declares two string constants, each a tag plus an OCI image index digest. It has no imports and no executable code.

## Interface
- `Postgres` — pinned `postgres:17-alpine@sha256:...` reference [testimages.go:Postgres]
- `SeaweedFS` — pinned `chrislusf/seaweedfs:4.51@sha256:...` reference [testimages.go:SeaweedFS]

## Invariants
- Each constant has the form `<image>:<tag>@sha256:<digest>` [testimages.go:Postgres]
- `TestDigestCopiesMatch` enforces the pins, not code in this package. Every digest reference to a pinned repository in a tracked text file outside `vendor/`, `docs/plans/`, and `docs/archive/` must equal the constant. A reference with a placeholder digest (`...`, `…` or `<digest>`) must have the constant's tag. `docker-compose.yml` must hold a copy of `Postgres`. `docker-compose.yml`, `README.md`, and `CONTRIBUTING.md` must each hold a copy of `SeaweedFS` [testimages_test.go:TestDigestCopiesMatch]

## Side Effects
- None

## Verification
- Run: `go test ./internal/testutil/testimages/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
