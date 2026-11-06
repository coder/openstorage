# Contributing

Build, test, and merge a change. Project overview: [README](README.md).
Test lanes in depth: [testing reference](docs/reference/testing.md).

## TL;DR

1. Install the [prerequisites](#prerequisites), then `mise install`.
2. Build: `go build ./cmd/openstorage`.
3. Test: `go test ./... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` plus the
   [other two lanes](#tests).
4. Lint: `golangci-lint run`.
5. Open a PR with a [conventional title](#commit-and-pr-conventions); meet the [review bar](#the-review-bar).

## Prerequisites

| Tool | Why |
|---|---|
| Go ≥ 1.27.3 | `go.mod` toolchain; CI resolves the version from `go.mod` |
| git | the server shells out to it; every test uses it |
| git-lfs | LFS e2e tests drive a real `git lfs` client; CI fails fast without it |
| Docker | default test lane (Postgres + SeaweedFS via [testcontainers](https://golang.testcontainers.org/)) and the compose stack |
| gpg / ssh-keygen | only `verify-sig` ref-policy tests and manual signature dogfooding |

[`mise.toml`](mise.toml) pins Go, golangci-lint, and the AWS-reference Terraform.
[mise](https://mise.jdx.dev/)'s `mise install` fetches CI's exact versions;
installing them by hand also works.

## Authenticated installation

The repository and release downloads are internal to the Coder organization,
so you need GitHub credentials with repository access.

- **From source**, clone with authenticated Git:

  ```sh
  gh auth setup-git   # or configure another Git credential helper for github.com
  git clone https://github.com/coder/openstorage.git && cd openstorage
  go install ./cmd/openstorage
  ```

  This uses the committed `vendor/` tree. The `v0.15.0` tag still declares
  module path `github.com/coder/open-storage`, not `github.com/coder/openstorage`
  (GitHub redirects do not change tags). Once a release carries the current
  path, you can install with
  `GOPRIVATE='github.com/coder/*' go install github.com/coder/openstorage/cmd/openstorage@latest`.
  `GOPRIVATE` fetches through authenticated Git, bypassing the public module
  proxy and checksum database.

- **With mise**, release downloads need a GitHub token: a logged-in `gh` CLI
  (used when `github.gh_cli_tokens` is on, the default) or `GITHUB_TOKEN`:

  ```sh
  mise use -g github:coder/openstorage@latest
  ```

  Older mise without the `github:` backend:
  `mise use -g 'ubi:coder/openstorage[exe=openstorage]@latest'`.

## Build and run

```sh
git clone https://github.com/coder/openstorage.git && cd openstorage
go build ./cmd/openstorage && ./openstorage version
./openstorage keygen --out /tmp/cs-key   # org keypair, used by both serve modes below
```

The binary lands at `./openstorage`, not on `PATH` (or `go install` it).
Run it one of three ways:

- **Local mode** — SQLite + files; no database or object-storage service:

  ```sh
  OPENSTORAGE_ORG=selfhost OPENSTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
    ./openstorage serve --listen 127.0.0.1:8080 --data-dir /tmp/cs-data
  ```

  Next: [README quickstart](README.md#quickstart-local-mode-60-seconds)
  (mint tokens, create a repo, push).

- **Production topology (Postgres + SeaweedFS)** — compose runs the services;
  `serve` runs on the host:

  ```sh
  docker compose up -d --wait
  OPENSTORAGE_ORG=selfhost OPENSTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  OPENSTORAGE_PG_URL="postgres://postgres:postgres@127.0.0.1:${PGPORT:-15432}/postgres" \
  OPENSTORAGE_S3_URL="s3://codestorage?endpoint=http://127.0.0.1:${S3PORT:-18333}&s3ForcePathStyle=true&region=us-east-1" \
  AWS_ACCESS_KEY_ID=codestorage AWS_SECRET_ACCESS_KEY=codestorage \
    ./openstorage serve --listen 127.0.0.1:8080
  ```

  The opt-in `replicas` profile adds two server containers
  ([compose guide](docs/how-to/deploy-docker-compose.md)). All `OPENSTORAGE_*`
  variables: [configuration reference](docs/reference/configuration.md).

- **S3-only storage (experimental)** — one SeaweedFS bucket holds everything
  durable (`s3wal://` refs + metadata, `s3://` blobs). It must exist before boot.

  ```sh
  docker run -d --name cs-s3 -p 127.0.0.1:8333:8333 -v cs-s3-data:/data chrislusf/seaweedfs:4.51@sha256:f7a49e3f3972ebed5be2ed4befa4a0c68eb7bdddba6e692d42729ceb545134b8 server -s3 -dir=/data -ip.bind=0.0.0.0
  until curl -sf http://127.0.0.1:8333/healthz; do sleep 1; done
  # Cold start: bucket creation and object writes can both precede a writable volume,
  # so retry create + a real write roundtrip as one unit (create tolerates already-exists).
  until curl -s -X PUT http://127.0.0.1:8333/openstorage >/dev/null &&
        curl -sf -X PUT -d ready http://127.0.0.1:8333/openstorage/ready &&
        curl -sf -X DELETE http://127.0.0.1:8333/openstorage/ready; do sleep 1; done
  OPENSTORAGE_ORG=selfhost OPENSTORAGE_ORG_PUBKEYS=/tmp/cs-key.pub.pem \
  OPENSTORAGE_PG_URL='s3wal://openstorage?endpoint=http://127.0.0.1:8333&region=us-east-1' \
  OPENSTORAGE_S3_URL='s3://openstorage?endpoint=http://127.0.0.1:8333&s3ForcePathStyle=true&region=us-east-1' \
  AWS_ACCESS_KEY_ID=any AWS_SECRET_ACCESS_KEY=any \
    ./openstorage serve --listen 127.0.0.1:8080
  ```

  This SeaweedFS is unauthenticated, so any credentials work. Next:
  [README quickstart](README.md#quickstart-zero-infrastructure-mode-one-bucket-experimental);
  limits: [production guide](docs/how-to/deploy-production.md#zero-infrastructure-mode-experimental-one-bucket-one-node).

## Tests

CI gates every PR on these three lanes. The
[testing reference](docs/reference/testing.md) explains coverage and test conventions.

```sh
# 1. Default lane — the full suite on the production backends.
#    Needs Docker (testcontainers boots Postgres + SeaweedFS) and git-lfs.
go test ./... -tags=openstorage_viewcheck -count=1 -race -timeout=30m

# 2. Local lane — the git e2e suite on the zero-container backends
#    (SQLite + file blobs). No Docker needed.
OPENSTORAGE_TEST_BACKEND=local go test ./internal/githttp -count=1 -race -timeout=15m

# 3. s3wal lane — the same git e2e suite on the zero-infra topology: ONE
#    SeaweedFS bucket carrying refs + metadata + blobs, plus lane-only drills
#    (restart-from-bucket-only, dual-serve refusal). Needs Docker.
OPENSTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp -tags=openstorage_viewcheck -count=1 -race -timeout=25m
```

The default lane also fans `internal/api`, `internal/meta`, and `internal/ref`
out into `/pg` + `/sqlite` subtests, and runs the s3wal store suites
(conformance on the in-memory CAS fake, qualification on SeaweedFS).

Two flags are required:

- **`-count=1`** — `TestMain` reads the lane variable, which the test cache
  cannot see. Without it, switching lanes can replay the other lane's cached
  results ([details](docs/reference/testing.md#flags-that-are-not-optional)).
- **`-race`** — CI runs every lane with it. Green without `-race` is not green.

Iterate on a narrow scope (`go test ./internal/api -run TestName -count=1`);
run the full matrix at the end.

## Lint

CI and `mise install` use the same linter pin from [`mise.toml`](mise.toml):

```sh
mise install
golangci-lint run
```

Config: [`.golangci.yml`](.golangci.yml) (v2 schema) — default linters plus
`misspell`, `unconvert`, `nolintlint`, and `gofmt`/`goimports` formatters
(local prefix `github.com/coder/openstorage`).

## Go style: modern idioms

Write modern Go, up to the `go.mod` version. JetBrains'
[Modern Go Guidelines](https://github.com/JetBrains/go-modern-guidelines) ship as
the agent skill [`.agents/skills/use-modern-go/`](.agents/skills/use-modern-go/SKILL.md);
agents load it automatically, humans run the CLI:

```sh
sh .agents/skills/use-modern-go/scripts/run-tool.sh list --file-path path/to/file.go
```

New code should not reintroduce patterns the
[`modernize`](https://pkg.go.dev/golang.org/x/tools/gopls/internal/analysis/modernize)
analyzer rewrites:

```sh
go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...
```

## Commit and PR conventions

PRs are **squash-merged**, and
[release-please builds versions and the CHANGELOG from squash titles](docs/how-to/release.md).
The PR title must be a [conventional commit](https://www.conventionalcommits.org/)
subject; the `pr-title` check enforces it, including after title edits:

```
type(scope)?: summary
```

- The type prefix is **required** and must be a type from the table below.
- Package names go in the **scope**: `fix(githttp): ...`, never `githttp: ...`.
  release-please ignores a bare package prefix: the change still lands on `main`,
  but gets no version bump and no CHANGELOG entry.
- Only the squash title counts; branch commits can be anything.

| Type | Release effect |
|---|---|
| `fix` | patch release |
| `feat` | minor release |
| `feat!:` / `BREAKING CHANGE:` | breaking release |
| `perf` | patch release under "Performance Improvements"; not for refactors that leave performance unchanged |
| `docs`, `test`, `ci`, `chore`, `build`, `refactor` | no release; hidden from the CHANGELOG |

## The review bar

A PR merges when all of these hold:

1. **CI is green** — every job in `ci.yml`: `lint` (golangci-lint + docs
   checks), `terraform` (offline Terraform checks), `test` (default lane),
   `test-local`, `test-s3wal` (S3-only storage lane), and `image` (Docker build +
   runtime-dependency smoke test), plus the `pr-title` check. Release PRs need
   their held `ci` and `pr-title` runs approved; see the
   [release runbook](docs/how-to/release.md).
2. **A clean automated review round** — comment `@codex review`. Fix genuine
   errors; for out-of-scope findings, say why and resolve the thread. Every
   push invalidates the round: re-request on the new head.
3. **A clean automated security round** — same protocol with
   `@codex security review`.
4. **No unresolved review threads.**
5. **The PR merges through the merge queue** — the queue squashes it onto the
   latest `main` and runs the required checks there, so the branch does not
   need to be up to date. When `main` conflicts with it, merge `main` into the
   branch instead of force-pushing.

Fixes land [**red-first**](docs/reference/testing.md#red-first-tests): the PR
includes a test that fails on the pre-fix code. Verify docs claims against the
code or a live run, not memory.
