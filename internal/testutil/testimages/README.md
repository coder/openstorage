# `internal/testutil/testimages`

## Overview
Pins the container images that tests, fixtures and CI lanes start. The package declares four string constants, each a tag plus an OCI image index digest. It has no imports and no executable code.

## Interface
- `Postgres` — pinned `postgres:17-alpine@sha256:...` reference [testimages.go:Postgres]
- `SeaweedFS` — pinned `chrislusf/seaweedfs:4.54@sha256:...` reference [testimages.go:SeaweedFS]
- `Floci` — pinned `floci/floci:2.5.0@sha256:...` reference, also exported as `s3waltest.FlociImage` [testimages.go:Floci]
- `AmazonLinux` — pinned `amazonlinux:2023@sha256:...` reference [testimages.go:AmazonLinux]

## Invariants
- Each constant has the form `<image>:<tag>@sha256:<digest>` [testimages.go:Postgres]
- `TestDigestCopiesMatch` enforces the pins, not code in this package. Every digest reference to a pinned repository in a tracked text file outside `vendor/`, `docs/plans/`, and `docs/archive/` must equal the constant. A reference with a placeholder digest (`...`, `…` or `<digest>`) must have the constant's tag. `docker-compose.yml` must hold a copy of `Postgres`. `docker-compose.yml`, `README.md`, and `CONTRIBUTING.md` must each hold a copy of `SeaweedFS`. Both Floci nightly workflows and `docs/reference/testing.md` must each hold a copy of `Floci`, and `deploy/terraform/aws/bootstrap/Dockerfile.test` a copy of `AmazonLinux` [testimages_test.go:TestDigestCopiesMatch]

## Side Effects
- None

## Verification
- Run: `go test ./internal/testutil/testimages/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
