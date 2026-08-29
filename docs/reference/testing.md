# Testing reference

How this repository is tested: the lanes CI runs, the conventions for writing
new tests, and the deterministic simulation (DST) harness. For the two-command
quick version, see [CONTRIBUTING](../../CONTRIBUTING.md#tests).

## Test lanes

One test suite, two storage stacks. Which stack a test run uses is decided in
`TestMain`, not per test:

| Lane | Command | Backends | Needs |
|---|---|---|---|
| **Default (`pg`)** | `go test ./... -count=1 -race -timeout=30m` | Postgres 17 + SeaweedFS, booted per run via [testcontainers](https://golang.testcontainers.org/) | Docker, git, git-lfs |
| **Local** | `CODESTORAGE_TEST_BACKEND=local go test ./internal/githttp -count=1 -race -timeout=15m` | SQLite (`sqlite://`) + plain-file blobs (`file://`) | git, git-lfs — **no Docker** |
| **s3wal** | `CODESTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp -count=1 -race -timeout=25m` | ONE SeaweedFS bucket carrying refs + metadata (`s3wal://`, the `_wal/` prefix) AND the blobs (`s3://`) — the zero-infra topology (ADR-46), plus its lane-only drills (restart-from-bucket-only, dual-serve refusal) | Docker, git, git-lfs |
| **Backend fan-out** | part of the default lane | `internal/api`, `internal/meta`, and `internal/ref` run every integration/conformance test twice, as `/pg` and `/sqlite` subtests | Docker (for the `/pg` half) |

These are exactly the CI jobs in
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml): `test` runs the
default lane, `test-local` runs the local lane, `test-s3wal` runs the s3wal
lane (issue #193), and `lint` / `image` complete the required set. CI asserts
`git lfs version` up front so a missing LFS client fails the job instead of
silently skipping LFS coverage.

`CODESTORAGE_TEST_BACKEND` accepts `pg` (or unset), `local`, and `s3wal`; anything
else panics in `TestMain` (`internal/githttp/e2e_test.go`). On the s3wal lane the
suite's nodes run with the instance lease disabled (`CODESTORAGE_S3WAL_LEASE_TTL=0`)
because the shared-bucket multi-node tests are exactly the topology the lease refuses;
the dual-serve drill re-enables it explicitly. The `internal/githttp`
e2e harness builds the real `codestorage` binary, boots the chosen backends,
starts server nodes as subprocesses, and drives them with plain `git`, `curl`-level
HTTP, and a real `git lfs` client.

### Flags that are not optional

- **`-count=1` when switching lanes.** The Go test cache does track environment
  variables — but only reads made while the tests run. `CODESTORAGE_TEST_BACKEND`
  is read in `TestMain` *before* `m.Run()`, which the cache cannot see, so
  switching lanes without `-count=1` silently replays the other lane's cached
  results and reports them as fresh (reproducible: a `TestMain`-read variable
  change still yields `(cached)`). Variables read inside test functions, such
  as `DST_SEED`, invalidate the cache on their own; `-count=1` everywhere is
  simply the uniform CI-parity habit.
- **`-race` always.** Every CI lane runs under the race detector, including the
  nightly DST sweep. Local runs without `-race` are for fast iteration only.

## Writing tests: house conventions

### Red-first tests

Bug fixes and behavior changes land red-first:

1. Write the test that captures the desired behavior **before** the fix, run
   it, and confirm it fails — *for the expected reason*, not from a typo in the
   test.
2. Land the fix; the same test goes green with no edits to its assertions.
3. Keep the reproduction narrow: one test that pins the bug beats a broad one
   that happens to trip over it.

Reviewers may ask for the red run; for subtle fixes, note the pre-fix failure
message in the PR.

### E2E conventions (`internal/githttp`)

- **Keep `-count=1`; never `-count=2` or higher on the e2e suites.** E2E tests
  register repositories under fixed names (`t-repack`, ...) against a backend
  that lives for the whole `TestMain` process, so an in-process rerun collides
  with the first run's state. Repeat a run by invoking `go test -count=1`
  again, not by raising `-count`.
- **Repack convergence asserts must use threshold 2.** With
  `CODESTORAGE_REPACK_THRESHOLD=t`, pack rows converge *below* `t`, not to one
  row — a fold that snapshots all but the latest push can settle at `t-1` rows
  permanently. Only `t=2` guarantees eventual convergence to exactly one pack,
  which is what makes it assertable.
- **Budget for job chains generously.** Background work (repack successors,
  sync) runs through the real queue with delays; convergence waits in the suite
  use minutes-scale deadlines because `-race` on a loaded CI runner is slow.
  Tight sleeps are how CI-only flakes are born.
- **Local-only tests skip, not fail, on the pg lane** — follow the existing
  pattern: check the lane and `t.Skipf` with the `CODESTORAGE_TEST_BACKEND=local`
  hint.

### Hermetic git

Tests that shell out to git must not inherit the developer's identity or
hooks. The house pattern (see `internal/simtest/gitenv.go`) pins
`GIT_AUTHOR_*` / `GIT_COMMITTER_*` and neutralizes config:
`GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_SYSTEM=/dev/null`,
`GIT_CONFIG_NOSYSTEM=1`. Host-level `GIT_*` variables and global hooks
otherwise leak into fixture commits and change SHAs.

### Fault injection

The e2e suites use server-side knobs (`CODESTORAGE_TEST_MODE`,
`CODESTORAGE_TEST_CRASH`, `CODESTORAGE_TEST_CAS_DROP`, ...) to force crashes at
precise pipeline stages; the server refuses them outside test mode. The full
list is in the [configuration reference](configuration.md#internal-and-test-variables).

## The DST harness

`internal/simtest` is a deterministic, seeded simulation of a multi-node
cluster — pushes, API writes, hydrations, crashes, restarts, repack races,
upstream-sync claim/steal/finish cycles, and injected storage faults, all in
one process on the local backends, checked against invariant oracles
(ref/pack consistency, CAS atomicity, webhook soundness, sync claim-lease
liveness). **[`internal/simtest/README.md`](../../internal/simtest/README.md)**
is the authoritative description of what it exercises, the oracles, and the
determinism rules; this section covers how to run and triage it.

| What | How |
|---|---|
| PR gate (runs inside `go test ./...`) | `TestDSTFast`, `TestDSTDeterminism` |
| Reproduce a seed (test) | `DST_SEED=<seed> [DST_STEPS=<n>] go test ./internal/simtest -run 'TestDSTSeed' -v -count=1` |
| Reproduce a seed (CLI) | `go run ./cmd/dst -seed <seed> [-steps N] [-nodes N] [-v] [-keep]` |
| Nightly sweep | [`dst-nightly.yml`](../../.github/workflows/dst-nightly.yml) → `TestDSTSweep` |

A run prints one summary line and a verdict. Determinism means: the same
binary run with the same configuration prints the same trace hash. The two
entry points expose different slices of that configuration — `cmd/dst` takes
`-seed`/`-steps`/`-nodes`, `TestDSTSeed` takes `DST_SEED`/`DST_STEPS` with the
default node count — so a replay reproduces a run when its parameters match
the original's (everything not exposed stays at the shared defaults):

```console
$ go run ./cmd/dst -seed 42
seed=42 steps=150 trace=33f26256fcdf... transitions=22 deliveries=19 faults=26
ok: no invariant violations
```

Defaults: 150 steps, 3 nodes. Exit codes: `0` clean, `1` invariant violation,
`2` infrastructure failure (a broken harness, not a found bug).

### Triaging a violation

1. **Replay exactly.** A failure (nightly log or local sweep) names the seed
   and step count: `go run ./cmd/dst -seed <seed> -steps <steps> -v`. `-v`
   streams the op-by-op trace — each line shows the op, the faults it consumed,
   its outcome, and the ground-truth tip after it; the violation names the step
   and the oracle.
2. **Inspect surviving state.** Add `-keep` to retain the run directory
   (printed as `run dir: /tmp/dst-seed<seed>-...`): the SQLite DB, the blob
   directory, and each node's cache, ready for `sqlite3` / `git fsck`.
3. **Attribute the fault.** The trace's fault annotations tie every injected
   fault to the op that consumed it — e.g.
   `ref.CASRef/ambiguous(won=true err=<nil>)` is a CAS that committed under an
   ambiguous fault, the exact class the recovery oracles guard.
4. **Verify the fix against the seed** — carrying the reported step count, or
   a longer sweep's failure point silently falls back to the 150-step default:
   `DST_SEED=<seed> DST_STEPS=<steps> go test ./internal/simtest -run 'TestDSTSeed' -v -count=1`
   must pass. Trace hashes are per-run-content: the failing hash belongs to the
   *pre-fix* reproduction (the `VIOLATION` line is hashed and stops the run);
   the fixed run produces a different hash — what determinism guarantees is
   that repeated runs of the same binary print the *same* one.
5. **Pin it in a committed test.** The reproduction command guards nothing
   after it scrolls by — `TestDSTSeed` skips when `DST_SEED` is unset, and the
   nightly sweep derives fresh seeds from each run id. Commit a regression
   that replays the exact failing configuration in the PR gate: a
   `Run(Config{Seed: <seed>, Steps: <steps>})` call in
   `internal/simtest/dst_test.go`, next to `TestDSTFast` — as a dedicated
   test or a table entry that carries the step count (`TestDSTFast`'s own
   seed-only list runs everything at 120 steps).

### The nightly sweep

[`dst-nightly.yml`](../../.github/workflows/dst-nightly.yml) runs daily (03:17
UTC): 48 seeds × 300 steps under `-race`, seed base derived from the run id so
every night covers fresh seeds while staying reproducible from the log. It can
be dispatched manually with `seeds` / `base` / `steps` inputs, which map to the
`DST_SWEEP_SEEDS` / `DST_SWEEP_BASE` / `DST_STEPS` variables of `TestDSTSweep`.
There are no artifacts — everything needed to replay a failure is in the job
log.

## Fuzzing

Native Go fuzz targets (`func Fuzz*` in `fuzz_test.go` files) cover the
untrusted-input decoders: receive-pack sniffing and the LFS batch decode
(`internal/githttp`), JWT verification and ref-policy parsing
(`internal/auth`), secret envelopes and key parsing (`internal/secrets`),
pack header/idx counts (`internal/jobs`), and rate-limit client keying and
pagination cursors (`internal/api`). Targets assert *graceful* behavior, not
just no-panic: garbage must map to errors — never a fabricated identity,
plaintext, or capability — usually via an independent reference oracle or a
known-answer set built from keys that exist only in the test process.

Seed corpora live in the `f.Add` calls next to each target, so plain
`go test ./...` replays every seed as an ordinary test — normal CI covers
fuzz regressions for free. When a campaign finds a failing input, `go test`
writes it to `<pkg>/testdata/fuzz/<FuzzTarget>/`; check that file in with the
fix so the crasher replays forever.

Run a target locally with (one `-fuzz` target per invocation):

```sh
go test ./internal/auth -run '^$' -fuzz '^FuzzVerify$' -fuzztime 60s -parallel 8
```

Keep `-parallel` around 8 on high-core machines — the default spawns one
worker per core, and each `internal/githttp` fuzz process skips the e2e
environment boot via the fuzz early-out in its `TestMain`.

[`fuzz-nightly.yml`](../../.github/workflows/fuzz-nightly.yml) runs the
sustained sweep daily (02:41 UTC): every target sequentially at 4m each
(dispatch inputs `fuzztime` / `parallel` override the budget). A failing
input turns the workflow red and uploads everything under
`internal/*/testdata/fuzz/` as the `fuzz-crashers` artifact for triage.

## Benchmarks

Hot-path benchmarks (`func Benchmark*` in `*_bench_test.go` files) baseline
the per-request and per-trigger costs optimization work measures against:
the hydration fast path and steady-state `EnsureCacheRepo`
(`internal/githttp`), quota-admission usage listing and ledger reservation
(`internal/blob`, `internal/ref`), repack fold selection (`internal/jobs`),
bearer-token verification per algorithm family (`internal/auth`), and
pre-auth rate limiting under mutex contention (`internal/api`).

House conventions: report allocations (`b.ReportAllocs`), iterate with
`b.Loop()` (setup stays untimed and results stay live), express size and
algorithm matrices as `b.Run` sub-benchmarks (`packs=100`, `candidates=1000`,
`ES256`), and keep fixtures deterministic and container-free — SQLite,
fileblob, or in-memory buckets; never the network, Docker, or real S3. Each
timed iteration also checks its result against the expected value, so a
benchmark that silently stops measuring the intended path fails instead of
reporting numbers for the wrong one.

Run the full sweep locally (`-run '^$'` skips ordinary tests; `-p 1`
serializes the per-package test binaries so benchmark packages never
contend with each other; the local backend keeps the `internal/githttp`
boot container-free):

```sh
CODESTORAGE_TEST_BACKEND=local go test -p 1 ./... -run '^$' -bench . -benchmem
```

Benchmarks are **not** a per-PR gate — shared-runner deltas are too noisy to
block merges on. [`bench-nightly.yml`](../../.github/workflows/bench-nightly.yml)
runs the sweep daily (04:23 UTC) and uploads the output as the
`bench-results` artifact; dispatch inputs `benchtime` / `count` override the
budget (compare runs with `benchstat`, which wants `count` around 10). One
caveat when reading results: `BenchmarkAdmitReserve/parallelism=N` scales
`b.SetParallelism`, so the worker count is N × GOMAXPROCS, not N.

## The Floci S3 lane

[`floci-nightly.yml`](../../.github/workflows/floci-nightly.yml) runs daily
(03:53 UTC): it boots [Floci](https://hub.docker.com/r/floci/floci) — a
lightweight AWS wire-protocol emulator — with S3 auth enforcement on,
provisions an IAM access key through the IAM API, and runs
`go test ./internal/blob` with `CODESTORAGE_TEST_S3_URL` pointed at it. That
variable gates the endpoint tests in `internal/blob/s3endpoint_test.go`
(skipped everywhere else): a CRUD round-trip through the real `blob.S3`
surface, plus the presigned-URL SigV4 test — valid signature ⇒ 200 with the
exact bytes, corrupted signature ⇒ `403 SignatureDoesNotMatch`. The
per-PR SeaweedFS container test also pins presign accept/reject live (with
a static enforcing identity); this nightly lane adds the external-endpoint
path with a key provisioned through a real IAM API rather than a static
config, against a second, independent wire-protocol implementation.

Known Floci gaps, so nobody extends this lane into them (all verified open on
both `1.7.0` and `nightly-08282026` by the #201 spike):

- No IAM policy evaluation on S3 actions (a `Deny` still succeeds), and
  `x-amz-expected-bucket-owner` is ignored — both stay covered by unit-test
  seams.
- Torn reads under same-key overwrite: a GET racing a PUT on the same key can
  return a body that is neither version, with broken HTTP framing (Go logs
  "Unsolicited response received on idle HTTP channel") that poisons
  keep-alive connections. This disqualifies any s3wal-shaped lane — whose
  production shape is manifest reads racing CAS overwrites — until fixed
  upstream.
- `DeleteObject` with `If-Match` is silently ignored: a mismatched `If-Match`
  deletes the object anyway, with no 412 (MinIO-class behavior).
- Header-auth SigV4 signatures are not validated — only that the access-key
  ID exists, so a wrong secret or a forged session token is accepted.
  Presigned-URL signature validation IS enforced, and that is the SigV4
  property this lane pins.

Two positives from the same spike, for future lane design: Floci passes the
full 22-surface s3wal qualification suite (`TestQualifyExternalEndpoint` in
`internal/ref/s3wal`) — the only emulator besides SeaweedFS to do so — and
SeaweedFS 4.42 honors conditional DELETE (412 and the object survives on a
mismatched `If-Match`), unlike both Floci and the legacy MinIO pin.

The same workflow also records the Floci row of the s3wal qualification
matrix: `TestQualifyFloci` (`internal/ref/s3wal/qualify_floci_test.go`) runs
the qualification surfaces plus the `delete-if-match` and
`get-under-overwrite` (torn-read) gap surfaces in record mode against the
pinned `s3waltest.FlociImage`, so a Floci release that closes a gap surfaces
in the VERDICT transcript on the next pin bump — nothing gates on Floci.

Run it locally (Docker + the `aws` CLI):

```sh
docker run -d --name floci -p 4566:4566 \
  -e FLOCI_SERVICES_S3_ENFORCE_AUTH=true floci/floci:latest

# IAM accepts any credentials; only S3 enforces auth.
export AWS_ACCESS_KEY_ID=root AWS_SECRET_ACCESS_KEY=root AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:4566 iam create-user --user-name blobtest
aws --endpoint-url http://127.0.0.1:4566 iam create-access-key --user-name blobtest
# export the returned AccessKeyId / SecretAccessKey, then:

CODESTORAGE_TEST_S3_URL='s3://floci-test?endpoint=http://127.0.0.1:4566&s3ForcePathStyle=true&region=us-east-1' \
  go test ./internal/blob -run 'TestS3Endpoint' -count=1 -race -v
```

The same tests run against real S3: point `CODESTORAGE_TEST_S3_URL` at an
existing bucket (`s3://my-bucket?region=us-east-1`) with real credentials in
the environment. Without an `endpoint` query parameter the bucket is only
probed, never created — re-creating an owned us-east-1 bucket would reset
its ACLs. Versioned (or version-suspended) buckets are refused, because
cleanup relies on `DeleteObject` actually reclaiming the test keys rather
than writing delete markers. Keys are uniquely suffixed and cleaned up, but
use an unversioned scratch bucket, not a production one.

## The Floci githttp E2E lane

[`floci-nightly-githttp.yml`](../../.github/workflows/floci-nightly-githttp.yml)
runs daily (04:53 UTC): it boots a pinned Floci image with S3 auth
enforcement on, provisions an IAM user and access key, assumes a role via
STS, and reruns the full PG-mode `internal/githttp` E2E suite with the blob
store pointed at Floci and the STS **session credentials** in the
environment. `CODESTORAGE_TEST_S3_ENDPOINT` is the seam: a test-only
override read by the suite's `TestMain` (like `CODESTORAGE_TEST_BACKEND`,
it never touches production config parsing) that swaps the managed
SeaweedFS container for the external endpoint on the pg lane — and panics
on any other lane, where it would otherwise be silently ignored. The
workflow's last step asserts the suite's bucket exists on Floci, so broken
override plumbing fails the lane instead of passing green against a
quietly booted SeaweedFS.

What the nightly adds over the per-PR `test` job: a second, independent S3
wire-protocol implementation under the whole git/LFS surface (wire-drift
armor), and the only lane anywhere that *runs* on IAM/STS-minted session
credentials — every other lane signs with long-lived static keys. Be
precise about what that buys: Floci only honors access-key IDs minted
through IAM/STS, so the lane proves the suite operates end-to-end on
freshly minted temporary keys — but its header-auth gap (see the gap list
above: only the key ID's existence is checked) means a dropped or forged
`AWS_SESSION_TOKEN` on header-auth requests would **not** fail the lane.
Verifying token integrity end-to-end needs an endpoint that authenticates
temporary credentials; until then this lane exercises, without asserting,
the session-token plumbing (env → SDK → serve subprocesses). Floci's
default in-memory storage mode is exactly right for a fresh-per-run lane;
persistent mode (`FLOCI_STORAGE_MODE=persistent` plus a volume) was verified
working during the issue #201 spike if the lane ever needs restart survival.

The per-PR blob store deliberately stays SeaweedFS: Floci is not strictly
better — header-auth SigV4 signatures are not validated, reads racing a
same-key overwrite can tear, and conditional DELETE is ignored. The
authoritative gap list, with details, is in
[the Floci S3 lane](#the-floci-s3-lane) section above. The lane is
nightly-only and **not** part of the required PR check set.

Run it locally (Docker + the `aws` CLI):

```sh
docker run -d --name floci -p 4566:4566 \
  -e FLOCI_SERVICES_S3_ENFORCE_AUTH=true floci/floci:1.7.0

# IAM/STS accept any credentials; only the S3 data plane enforces auth.
export AWS_ACCESS_KEY_ID=root AWS_SECRET_ACCESS_KEY=root AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:4566 iam create-user --user-name githttp-lane
aws --endpoint-url http://127.0.0.1:4566 iam create-access-key --user-name githttp-lane
aws --endpoint-url http://127.0.0.1:4566 iam create-role --role-name githttp-lane-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::000000000000:user/githttp-lane"},"Action":"sts:AssumeRole"}]}'
# With the create-access-key pair exported:
aws --endpoint-url http://127.0.0.1:4566 sts assume-role \
  --role-arn arn:aws:iam::000000000000:role/githttp-lane-role \
  --role-session-name githttp-lane
# Export the returned session AccessKeyId / SecretAccessKey / SessionToken
# (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN), then:

AWS_REGION=us-east-1 CODESTORAGE_TEST_S3_ENDPOINT=http://127.0.0.1:4566 \
  go test ./internal/githttp -count=1 -race -timeout=25m
```
