# Contributing

This document covers the local development loop: building, running, testing,
linting, and what it takes to get a change merged. For what the project *is*,
start at the [README](README.md); for test lanes and the simulation harness in
depth, see the [testing reference](docs/reference/testing.md).

## Prerequisites

| Tool | Why |
|---|---|
| Go ≥ 1.27 | `go.mod` toolchain; CI resolves the version from `go.mod` |
| git | the server shells out to it; every test uses it |
| git-lfs | the LFS end-to-end tests drive a real `git lfs` client (CI fails fast if it is missing) |
| Docker | the default test lane boots real Postgres + SeaweedFS via [testcontainers](https://golang.testcontainers.org/); also the compose stack |
| gpg / ssh-keygen | only for `verify-sig` ref-policy tests and manual dogfooding of signature verification |

The Go and golangci-lint pins live in [`mise.toml`](mise.toml); with
[mise](https://mise.jdx.dev/) installed, `mise install` fetches exactly the
versions CI uses. Installing the same versions by hand works too.

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

- **Production topology (Postgres + SeaweedFS)** — boot the backing services from the
  repo compose file and run `serve` on the host against them:

  ```sh
  docker compose up -d --wait
  CODESTORAGE_ORG=selfhost CODESTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  CODESTORAGE_PG_URL="postgres://postgres:postgres@127.0.0.1:${PGPORT:-15432}/postgres" \
  CODESTORAGE_S3_URL="s3://codestorage?endpoint=http://127.0.0.1:${S3PORT:-18333}&s3ForcePathStyle=true&region=us-east-1" \
  AWS_ACCESS_KEY_ID=codestorage AWS_SECRET_ACCESS_KEY=codestorage \
    ./codestorage serve --listen 127.0.0.1:8080
  ```

  The opt-in `replicas` profile adds two containerized server nodes — see the
  [compose guide](docs/how-to/deploy-docker-compose.md). Every `CODESTORAGE_*`
  variable is documented in the [configuration reference](docs/reference/configuration.md).

- **Zero-infrastructure mode (experimental)** — one SeaweedFS bucket as the sole durable
  authority (`s3wal://` refs + metadata and `s3://` blobs on the same bucket, which must
  exist before boot):

  ```sh
  docker run -d --name cs-s3 -p 127.0.0.1:8333:8333 -v cs-s3-data:/data chrislusf/seaweedfs:4.42 server -s3 -dir=/data -ip.bind=0.0.0.0
  until curl -sf http://127.0.0.1:8333/healthz; do sleep 1; done
  # Cold start: bucket creation and object writes can both precede a writable volume,
  # so retry create + a real write roundtrip as one unit (create tolerates already-exists).
  until curl -s -X PUT http://127.0.0.1:8333/codestorage >/dev/null &&
        curl -sf -X PUT -d ready http://127.0.0.1:8333/codestorage/ready &&
        curl -sf -X DELETE http://127.0.0.1:8333/codestorage/ready; do sleep 1; done
  CODESTORAGE_ORG=selfhost CODESTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  CODESTORAGE_PG_URL='s3wal://codestorage?endpoint=http://127.0.0.1:8333&region=us-east-1' \
  CODESTORAGE_S3_URL='s3://codestorage?endpoint=http://127.0.0.1:8333&s3ForcePathStyle=true&region=us-east-1' \
  AWS_ACCESS_KEY_ID=any AWS_SECRET_ACCESS_KEY=any \
    ./codestorage serve --listen 127.0.0.1:8080
  ```

  (This standalone SeaweedFS runs unauthenticated, so any credentials sign.) The
  [README quickstart](README.md#quickstart-zero-infrastructure-mode-one-bucket-experimental)
  continues with the client flow; topology and boundaries are in the
  [production guide](docs/how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node).

## Tests

CI gates every PR on the first two lane commands below; the s3wal lane is
on-demand. The [testing reference](docs/reference/testing.md) explains what
each lane covers and the conventions for writing new tests.

```sh
# 1. Default lane — the full suite on the production backends.
#    Needs Docker (testcontainers boots Postgres + SeaweedFS) and git-lfs.
go test ./... -count=1 -race -timeout=30m

# 2. Local lane — the git e2e suite on the zero-container backends
#    (SQLite + file blobs). No Docker needed.
CODESTORAGE_TEST_BACKEND=local go test ./internal/githttp -count=1 -race -timeout=15m

# 3. s3wal lane (on-demand, not a per-PR CI gate) — the same git e2e suite on
#    the zero-infra topology: ONE SeaweedFS bucket carrying refs + metadata +
#    blobs, plus lane-only drills (restart-from-bucket-only, dual-serve
#    refusal). Needs Docker.
CODESTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp -count=1 -race -timeout=25m
```

A further lane hides inside the first: the `internal/api`, `internal/meta`, and
`internal/ref` suites fan out into `/pg` + `/sqlite` subtests on their own, so
the default lane already covers both backends for those packages — and it also
runs the s3wal store suites there (conformance over the in-memory CAS fake,
qualification against a SeaweedFS container).

Two flags are load-bearing:

- **`-count=1`** — lane selection is an environment variable read in `TestMain`
  before the tests start, which the Go test cache cannot see. Without
  `-count=1`, switching lanes can silently replay cached results from the
  other lane ([details](docs/reference/testing.md#flags-that-are-not-optional)).
- **`-race`** — CI runs every lane under the race detector; a test that is only
  green without `-race` is not green.

While iterating, scope down (`go test ./internal/api -run TestName -count=1`)
and save the full matrix for the end.

## Lint

The linter pin lives in [`mise.toml`](mise.toml) — CI installs from the same
file, so `mise install` guarantees you and CI agree:

```sh
mise install
golangci-lint run
```

Config is [`.golangci.yml`](.golangci.yml) (v2 schema): the default linter set
plus `misspell`, `unconvert`, `nolintlint`, and the `gofmt`/`goimports`
formatters (local prefix `github.com/coder/code-storage`).

## Commit and PR conventions

PRs are **squash-merged**, and [release-please derives versions and the
CHANGELOG from the squash titles](docs/how-to/release.md) — so the PR title
must be a [conventional commit](https://www.conventionalcommits.org/) subject.
The `pr-title` CI check enforces the shape on every PR and re-checks it when
the title is edited:

```
type(scope)?: summary
```

- The type prefix is **required**. Allowed types: `feat`, `fix`, `docs`, `ci`,
  `chore`, `build`, `refactor`, `test`, `perf`.
- The repo's package-prefix habit goes in the **scope**, not the type:
  `fix(githttp): ...`, never `githttp: ...`. A bare package prefix is not a
  conventional commit, so release-please ignores the commit entirely — a
  genuine fix never reaches the CHANGELOG or the version bump (this happened
  to #81).
- `fix: ...` → patch release, `feat: ...` → minor release,
  `feat!:` / `BREAKING CHANGE:` → breaking.
- `perf: ...` → also releasable: with our uncustomized release-please config it
  triggers a patch release and appears in the CHANGELOG under "Performance
  Improvements". Don't use it for refactors that don't change performance.
- `docs:`, `test:`, `ci:`, `chore:`, `build:`, `refactor:` → no release, hidden
  from the CHANGELOG.

Individual commits inside the PR branch can be anything; only the squash title
counts.

## The review bar

A PR merges when all of the following hold:

1. **CI is green** — all five jobs: `lint`, `test` (default lane), `test-local`,
   `test-s3wal` (zero-infra serve lane, issue #193), and `image` (Docker build +
   runtime-dependency smoke test).
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
