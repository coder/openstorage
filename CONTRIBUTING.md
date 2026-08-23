# Contributing

This document covers the local development loop: building, running, testing,
linting, and what it takes to get a change merged. For what the project *is*,
start at the [README](README.md); for test lanes and the simulation harness in
depth, see the [testing reference](docs/reference/testing.md).

## Prerequisites

| Tool | Why |
|---|---|
| Go ≥ 1.26 | `go.mod` toolchain; CI resolves the version from `go.mod` |
| git | the server shells out to it; every test uses it |
| git-lfs | the LFS end-to-end tests drive a real `git lfs` client (CI fails fast if it is missing) |
| Docker | the default test lane boots real Postgres + MinIO via [testcontainers](https://golang.testcontainers.org/); also the compose stack |
| gpg / ssh-keygen | only for `verify-sig` ref-policy tests and manual dogfooding of signature verification |

## Build and run

```sh
go build ./cmd/codestorage && ./codestorage version
./codestorage keygen --out /tmp/cs-key   # org keypair, used by both serve modes below
```

(A source build lands at `./codestorage` without touching `PATH` — spell the
`./` out, or `go install` it.) Two ways to run what you built:

- **Zero-dependency local mode** — SQLite + plain-file blobs, one flag:

  ```sh
  CODESTORAGE_ORG=selfhost CODESTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
    ./codestorage serve --listen 127.0.0.1:8080 --data-dir /tmp/cs-data
  ```

  The [README quickstart](README.md#quickstart-local-mode-60-seconds) continues from
  here (minting tokens, creating a repo, pushing).

- **Production topology (Postgres + MinIO)** — boot the backing services from the
  repo compose file and run `serve` on the host against them:

  ```sh
  docker compose up -d --wait
  CODESTORAGE_ORG=selfhost CODESTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  CODESTORAGE_PG_URL="postgres://postgres:postgres@127.0.0.1:${PGPORT:-15432}/postgres" \
  CODESTORAGE_S3_URL="s3://codestorage?endpoint=http://127.0.0.1:${MINIOPORT:-19000}&s3ForcePathStyle=true&region=us-east-1" \
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    ./codestorage serve --listen 127.0.0.1:8080
  ```

  The opt-in `replicas` profile adds two containerized server nodes — see the
  [compose guide](docs/how-to/deploy-docker-compose.md). Every `CODESTORAGE_*`
  variable is documented in the [configuration reference](docs/reference/configuration.md).

## Tests

CI runs three lanes; run the same commands locally. The
[testing reference](docs/reference/testing.md) explains what each lane covers
and the conventions for writing new tests.

```sh
# 1. Default lane — the full suite on the production backends.
#    Needs Docker (testcontainers boots Postgres + MinIO) and git-lfs.
go test ./... -count=1 -race -timeout=30m

# 2. Local lane — the git e2e suite on the zero-container backends
#    (SQLite + file blobs). No Docker needed.
CODESTORAGE_TEST_BACKEND=local go test ./internal/githttp -count=1 -race -timeout=15m
```

The third lane is inside the first: the `internal/api`, `internal/meta`, and
`internal/ref` suites fan out into `/pg` + `/sqlite` subtests on their own, so
the default lane already covers both backends for those packages.

Two flags are load-bearing:

- **`-count=1`** — lane selection is an environment variable read in `TestMain`,
  which the Go test cache does not key on. Without `-count=1`, switching lanes
  can silently replay cached results from the other lane.
- **`-race`** — CI runs every lane under the race detector; a test that is only
  green without `-race` is not green.

While iterating, scope down (`go test ./internal/api -run TestName -count=1`)
and save the full matrix for the end.

## Lint

CI pins the linter; use the same version locally so you and CI agree:

```sh
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
golangci-lint run
```

The pin lives in [`.github/workflows/ci.yml`](.github/workflows/ci.yml) (the
`lint` job) — check there if this document drifts. Config is
[`.golangci.yml`](.golangci.yml): the default linter set plus `gofmt`,
`goimports` (local prefix `github.com/coder/code-storage`), `misspell`,
`unconvert`, `nolintlint`.

## Commit and PR conventions

PRs are **squash-merged**, and [release-please derives versions and the
CHANGELOG from the squash titles](docs/how-to/release.md) — so the PR title
must be a [conventional commit](https://www.conventionalcommits.org/):

- `fix: ...` → patch release, `feat: ...` → minor release,
  `feat!:` / `BREAKING CHANGE:` → breaking.
- `docs:`, `test:`, `ci:`, `chore:`, `build:`, `refactor:` → no release, hidden
  from the CHANGELOG.
- Anything else is invisible to the release tooling — releasable work with a
  non-conventional title silently never ships.

Individual commits inside the PR branch can be anything; only the squash title
counts.

## The review bar

A PR merges when all of the following hold:

1. **CI is green** — all four jobs: `lint`, `test` (default lane), `test-local`,
   and `image` (Docker build + runtime-dependency smoke test).
2. **A clean automated review round** — comment `@codex review` on the PR and
   address what comes back: fix genuine errors; when a finding is out of scope,
   say why and resolve the thread. Every push invalidates the round — re-request
   on the new head.
3. **A clean automated security round** — same protocol with
   `@codex security review`.
4. **No unresolved review threads.**
5. **The branch is up to date with `main`** — branch protection blocks merges
   from behind; prefer GitHub's *Update branch* over force-pushing.

House style for the changes themselves: fixes land **red-first** — the PR
contains a test that fails on the pre-fix code (see
[testing reference](docs/reference/testing.md#red-first-tests)). Docs claims
must be verified against the code or a live run, not copied from memory.
