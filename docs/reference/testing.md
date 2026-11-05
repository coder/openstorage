# Testing reference

The test lanes CI runs, the conventions for new tests, and the deterministic
simulation (DST) harness. Quick version: [CONTRIBUTING](../../CONTRIBUTING.md#tests).

## Test lanes

One suite, several storage stacks. `TestMain` picks the stack for the whole run.

**Per-PR lanes** — required by the [review bar](../../CONTRIBUTING.md#the-review-bar):

| Lane | Command | Backends | Needs |
|---|---|---|---|
| **Default (`pg`)** | `go test ./... -tags=openstorage_viewcheck -count=1 -race -timeout=30m` | Postgres 17 + SeaweedFS, booted per run via [testcontainers](https://golang.testcontainers.org/) | Docker, git, git-lfs |
| **Local** | `OPENSTORAGE_TEST_BACKEND=local go test ./internal/githttp -count=1 -race -timeout=15m` | SQLite (`sqlite://`) + plain-file blobs (`file://`) | git, git-lfs — **no Docker** |
| **s3wal** | `OPENSTORAGE_TEST_BACKEND=s3wal go test ./internal/githttp -tags=openstorage_viewcheck -count=1 -race -timeout=25m` | ONE SeaweedFS bucket carrying refs + metadata (`s3wal://`, the `_wal/` prefix) AND the blobs (`s3://`) — the zero-infra topology (ADR-46), plus its lane-only drills (restart-from-bucket-only, dual-serve refusal) | Docker, git, git-lfs |
| **Backend fan-out** | part of the default lane | `internal/api`, `internal/meta`, and `internal/ref` run every integration/conformance test twice, as `/pg` and `/sqlite` subtests | Docker (for the `/pg` half) |

**Nightly lanes** — not PR gates; each can also be dispatched manually:

| Lane | Workflow | Daily (UTC) | Covers |
|---|---|---|---|
| [DST sweep](#the-nightly-sweep) | [`dst-nightly.yml`](../../.github/workflows/dst-nightly.yml) | 03:17 | 48 seeds × 300 steps |
| [Fuzzing](#fuzzing) | [`fuzz-nightly.yml`](../../.github/workflows/fuzz-nightly.yml) | 02:41 | every target, 4m each |
| [Benchmarks](#benchmarks) | [`bench-nightly.yml`](../../.github/workflows/bench-nightly.yml) | 04:23 | full sweep |
| [Floci S3](#the-floci-s3-lane) | [`floci-nightly.yml`](../../.github/workflows/floci-nightly.yml) | 03:53 | blob endpoint + s3wal qualification |
| [Floci githttp E2E](#the-floci-githttp-e2e-lane) | [`floci-nightly-githttp.yml`](../../.github/workflows/floci-nightly-githttp.yml) | 04:53 | git/LFS E2E on STS credentials |

In [`ci.yml`](../../.github/workflows/ci.yml), `test`, `test-local`, and
`test-s3wal` run the three lanes; `lint` and `image` complete the set. Each
test job runs `git lfs version` first, so a missing LFS client fails instead of
silently skipping LFS coverage.

- `OPENSTORAGE_TEST_BACKEND` accepts `pg` (or unset), `local`, and `s3wal`;
  anything else panics in `TestMain` (`internal/githttp/e2e_test.go`).
- The `openstorage_viewcheck` build tag makes every S3WAL metadata store check that no
  caller wrote into its shared, read-only metadata view, and panic if one did. The
  serve binary that `internal/githttp` builds always has the tag. The default and s3wal
  lane commands above pass `-tags=openstorage_viewcheck` to `go test` for the
  in-process stores, as CI's `test` and `test-s3wal` jobs do. Release builds never set
  it.
- The s3wal lane sets `OPENSTORAGE_S3WAL_LEASE_TTL=0`, because its shared-bucket
  multi-node tests are the topology the instance lease refuses. The dual-serve
  drill re-enables the lease.
- The `internal/githttp` harness builds the real `openstorage` binary, starts
  server nodes as subprocesses, and drives them with plain `git`, `curl`-level
  HTTP, and a real `git lfs` client.

### SeaweedFS fixture image

The fixtures and reference Compose file pin SeaweedFS
[4.51](https://github.com/seaweedfs/seaweedfs/releases/tag/4.51), published
2026-10-25. [Changes since 4.48](https://github.com/seaweedfs/seaweedfs/compare/4.48...4.51)
touch S3 authorization (POST policy uploads, wildcard policy targets, POST
Object ACL fields), GET/HEAD tag and attribute echoing, delete-marker reads,
filer and master startup, and the `-ip.bind` listeners. With `-ip.bind` set,
each port now binds only that address, so `-ip.bind=0.0.0.0` still answers
on loopback.

On a fresh `weed server`, 4.50 and 4.51 hold the first object write for about
12 to 15 s: the master defers volume allocation and the filer waits up to
10 s before it joins the S3 lock ring
([seaweedfs#12145](https://github.com/seaweedfs/seaweedfs/issues/12145), fixed
upstream after 4.51). While the lock ring is empty, a PUT waits for its
object-write lock and fails after 15 s. In local runs the ring formed 2 to
12 s after the first lock attempt, so the first PUT waited and succeeded. The
write-readiness check retries a failed write for 60 s.

SHA-256 digests of the Docker Hub image metadata (retrieved 2026-10-29; a
published manifest does not prove runtime qualification):

| Image metadata | 4.48 | 4.51 |
|---|---|---|
| Multi-platform index | `4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d` | `f7a49e3f3972ebed5be2ed4befa4a0c68eb7bdddba6e692d42729ceb545134b8` |
| Linux/amd64 manifest | `aba492e2a4e4c90bff795745e8e660affa1f09e7650f5981bd7bccd1a06cd931` | `3a522c379599e163433a636e75022630848b63a74f08ea37bf06f8d776e9a707` |
| Linux/arm64 manifest | `f1f303474940e9e088edbd2c1658f7ac38cb0dd90f91a2b0a51916d74b8575a1` | `7e68a388de0806ed8d96f30cb7e927182a3d80d42920cb87d97d23311a7a1a77` |

On every pin change:

1. Compare the REQUIRE-mode qualification and the probe-refusal checks
   (`TestProbeRefusesNonCompliantStore` and `TestQualifyLegacyMinIOStub`,
   plus the opt-in [legacy MinIO lane](#legacy-minio-lane-opt-in) on a host
   that holds its image) without changing their assertions.
2. Keep volume capacity for every bucket, including backup and restore. The
   readiness helper requires `HeadBucket` plus a scratch PUT/DELETE; a health
   response does not prove volumes accept writes.
3. Run the full lanes and existing fuzz seeds.

Native-amd64 results do not qualify native ARM, real AWS, or S3WAL promotion.
A fixture upgrade neither updates historical benchmark receipts nor shows a
performance change.

### Legacy MinIO lane (opt-in)

`TestQualifyMinIOLegacy` (`internal/ref/s3wal`) runs the qualification surfaces
in record mode against the legacy MinIO pin `s3waltest.MinIOLegacyImage`
(`RELEASE.2024-01-16T16-07-38Z`, pinned by digest). Record mode logs each store
behavior as a verdict; only a surface that produces no verdict fails. The
historical results are in the
[Phase 1a matrix](../plans/s3wal-phase1a-results.md#5-r7-store-qualification-matrix).

Since 2026-09-24 the image can no longer be pulled: quay.io and Docker Hub
refuse anonymous pulls, and dl.min.io answers 410 Gone for community releases
([#304](https://github.com/coder/openstorage/issues/304)). The lane is therefore
opt-in, and CI does not run real MinIO:

- Unset (default): the test skips and names this reason.
- `OPENSTORAGE_TEST_MINIO_LEGACY=1`: the test runs with the unchanged digest
  pin. A missing image, missing Docker, `-short`, or an incomplete verdict set
  fails the test.
- Any other value fails the test.

```sh
OPENSTORAGE_TEST_MINIO_LEGACY=1 go test ./internal/ref/s3wal \
  -run '^TestQualifyMinIOLegacy$' -count=1 -v
```

The host needs a local copy of that exact image digest, so a fresh machine
cannot run the lane. As of 2026-09-24 the image survives only as private
copies on one maintainer's host. Nothing is published, and if those copies are
lost the lane can no longer run. A maintainer who holds the image runs the lane
on SeaweedFS pin changes (step 1 above) and on changes to the s3wal probe or CAS
error classification. It remains the byte-exact historical qualification
against the real store.

Default CI guards this lane's record-mode classification without Docker or
external network access ([#310](https://github.com/coder/openstorage/issues/310)):

- `TestQualifyLegacyMinIOStub` runs the surfaces that send conditional headers
  in record mode against an in-memory stub. The stub models the behavior this
  lane recorded: it ignores `If-None-Match: *` on PUT and honors `If-Match` and
  GET `If-None-Match`. The test requires the lane's recorded verdict rows:
  `ignores` for `gocloud-ifnotexist`, `conditional-create-duplicate`, and probe
  step 2, `not-reached` for probe steps 3–5, and `pass` for the rest. A
  compliant control run of the same stub must pass every row.
- `TestProbeRefusesNonCompliantStore` requires the probe to refuse a store that
  ignores `If-None-Match: *` creation at step 2.

The stub is not the real store. It models only the recorded behavior on those
surfaces. The other surfaces recorded `pass` on MinIO and take the same
classification branches in the SeaweedFS lane.

### Private SeaweedFS log capture

Managed SeaweedFS fixtures offer opt-in, bounded, private stdout/stderr
capture through teardown. See [the capture procedure](../how-to/capture-seaweedfs-test-logs.md)
for `OPENSTORAGE_TEST_ARTIFACT_DIR`, session attribution, receipts, and the
deliberately failing TestMain diagnostic. Keep raw logs private; capture status
is separate from the test exit code.

### Offline S3WAL recovery drill

`TestS3WAL_OfflineBackupRestore` (S3WAL lane) uses isolated source, backup,
and restore buckets. It copies a stopped node's whole bucket, rejects a damaged
inventory before restore boot, removes source data/cache, and verifies
fresh-cache Git/LFS recovery plus a new push. Command and limits:
[recovery runbook](../how-to/backup-restore-s3wal.md#reproduce-the-local-recovery-drill).
Local fixture proof, not a real-S3 promotion gate.

### Official-release upgrade smoke

The opt-in [upgrade harness](../how-to/test-release-upgrade.md) takes an
explicitly supplied official v0.15.0 archive and an exact clean candidate. It
skips the shared suite bootstrap, uses a private bucket with active leases, and
checks cold-cache Git/LFS, selected metadata, and encrypted-secret continuity.
Ordinary CI neither downloads releases nor claims this coverage. A candidate
smoke does not meet the final released-pair promotion criterion.

### Flags that are not optional

- **`-count=1` when switching lanes.** The test cache only tracks environment
  reads made while tests run. `TestMain` reads `OPENSTORAGE_TEST_BACKEND`
  *before* `m.Run()`, so switching lanes without `-count=1` replays the other
  lane's `(cached)` results as fresh. Variables read inside tests (`DST_SEED`)
  invalidate the cache themselves; `-count=1` everywhere is simply CI parity.
- **`-race` always.** Every CI lane uses it, including the nightly DST sweep.
  Skip it only for fast iteration.

## Writing tests: house conventions

### Red-first tests

Bug fixes and behavior changes land red-first:

1. Write the test for the desired behavior **before** the fix. Run it and
   confirm it fails *for the expected reason*, not from a test typo.
2. Land the fix. The same test goes green with no edits to its assertions.
3. Keep the reproduction narrow: one test that pins the bug beats a broad one
   that trips over it.

Reviewers may ask for the red run. For subtle fixes, quote the pre-fix
failure in the PR.

### E2E conventions (`internal/githttp`)

- **Keep `-count=1`; never `-count=2` or higher on the e2e suites.**
  Repositories use fixed names (`t-repack`, ...) on a backend that lives for
  the whole `TestMain` process, so an in-process rerun collides. To repeat,
  invoke `go test -count=1` again.
- **Exactly-one repack asserts need threshold 2 on every eligible consumer.**
  `OPENSTORAGE_REPACK_THRESHOLD` is per process: the push node's value does not
  configure other replicas on the shared River queue. Isolate the queue or set
  all eligible consumers to `2`. Convergence also assumes writes quiesce and
  folds keep succeeding within the
  [fold budgets](../how-to/storage-lifecycle-and-sweeping.md#background-repack).
  With a uniform threshold `t`, the target is *below* `t`, not necessarily one
  pack; a mixed-threshold fleet does not promise the producer's lower threshold.
- **Budget job chains generously.** Background work (repack successors, sync)
  runs through the real, delayed queue. Use minutes-scale convergence deadlines:
  `-race` on a loaded CI runner is slow, and tight sleeps cause CI-only flakes.
- **Local-only tests skip, not fail, on the pg lane.** Follow the existing
  pattern: check the lane and `t.Skipf` with the `OPENSTORAGE_TEST_BACKEND=local`
  hint.

### Hermetic git

Tests that shell out to git must not inherit the developer's identity or hooks;
host `GIT_*` variables and global hooks leak into fixture commits and change
SHAs. The house pattern (`internal/simtest/gitenv.go`) pins `GIT_AUTHOR_*` /
`GIT_COMMITTER_*` and neutralizes config: `GIT_CONFIG_GLOBAL=/dev/null`,
`GIT_CONFIG_SYSTEM=/dev/null`, `GIT_CONFIG_NOSYSTEM=1`.

### Fault injection

E2E suites force crashes at precise pipeline stages with server-side knobs
(`OPENSTORAGE_TEST_MODE`, `OPENSTORAGE_TEST_CRASH`, `OPENSTORAGE_TEST_CAS_DROP`,
...). The server refuses them outside test mode. Full list:
[configuration reference](configuration.md#internal-and-test-variables).

## The DST harness

`internal/simtest` is a deterministic, seeded, single-process simulation of a
multi-node cluster on the local backends: pushes, API writes, hydrations,
crashes, restarts, repack races, upstream-sync claim/steal/finish cycles, and
injected storage faults. Invariant oracles check ref/pack consistency, CAS
atomicity, webhook soundness, and sync claim-lease liveness.
**[`internal/simtest/README.md`](../../internal/simtest/README.md)** is the
authority on scope, oracles, and determinism rules; this section covers running
and triage.

| What | How |
|---|---|
| PR gate (runs inside `go test ./...`) | `TestDSTFast`, `TestDSTDeterminism`, `TestDSTRegressions` (the pinned-seed corpus) |
| Reproduce a seed (test) | `DST_SEED=<seed> [DST_STEPS=<n>] go test ./internal/simtest -run 'TestDSTSeed' -v -count=1` |
| Reproduce a seed (CLI) | `go run ./cmd/dst -seed <seed> [-steps N] [-nodes N] [-v] [-keep]` |
| Nightly sweep | [`dst-nightly.yml`](../../.github/workflows/dst-nightly.yml) → `TestDSTSweep` |

A run prints a summary line and a verdict. The same binary and configuration
always print the same trace hash. `cmd/dst` takes `-seed`/`-steps`/`-nodes`;
`TestDSTSeed` takes `DST_SEED`/`DST_STEPS` with the default node count. A
replay matches when those parameters match (the rest are shared defaults).

```console
$ go run ./cmd/dst -seed 42
seed=42 steps=150 trace=33f26256fcdf... transitions=22 deliveries=19 faults=26
ok: no invariant violations
```

Defaults: 150 steps, 3 nodes. Exit codes: `0` clean, `1` invariant violation,
`2` infrastructure failure (a broken harness, not a found bug).

### Triaging a violation

1. **Replay exactly** with the reported seed and step count:
   `go run ./cmd/dst -seed <seed> -steps <steps> -v`. `-v` streams each op, the
   faults it consumed, its outcome, and the ground-truth tip after it. The
   violation names the step and the oracle.
2. **Inspect surviving state.** Add `-keep` to retain the run directory
   (printed as `run dir: /tmp/dst-seed<seed>-...`): the SQLite DB, the blob
   directory, and each node's cache, ready for `sqlite3` / `git fsck`.
3. **Attribute the fault.** Annotations tie each injected fault to the op that
   consumed it: `ref.CASRef/ambiguous(won=true err=<nil>)` is a CAS that
   committed under an ambiguous fault, the class the recovery oracles guard.
4. **Verify the fix** with the reported step count (without it, the run falls
   back to 150 steps):
   `DST_SEED=<seed> DST_STEPS=<steps> go test ./internal/simtest -run 'TestDSTSeed' -v -count=1`
   must pass. The hash will differ: the failing hash includes the `VIOLATION`
   line that stopped the pre-fix run.
5. **Pin it in the regression corpus.** `TestDSTSeed` skips without `DST_SEED`
   and the nightly sweep picks fresh seeds, so nothing else guards the bug.
   Add the exact configuration (seed, step count, backend lane, issue) to the
   `dstRegressions` table in `internal/simtest/dst_test.go`;
   `TestDSTRegressions` replays every entry in the PR gate. Prove it red before
   the fix and green after. Entries are tied to the harness+driver pair that
   produced them: if a harness change legitimately re-schedules the PRNG
   stream, re-validate each entry against its issue instead of dropping it.

### The nightly sweep

[`dst-nightly.yml`](../../.github/workflows/dst-nightly.yml) runs 48 seeds ×
300 steps under `-race`, with the seed base derived from the run id: fresh
seeds nightly, reproducible from the log. Dispatch inputs `seeds` / `base` /
`steps` map to `TestDSTSweep`'s `DST_SWEEP_SEEDS` / `DST_SWEEP_BASE` /
`DST_STEPS`. No artifacts; the job log has everything to replay a failure.

## Fuzzing

Native Go fuzz targets (`func Fuzz*` in `fuzz_test.go` files) cover the
untrusted-input decoders:

- `internal/githttp`: receive-pack sniffing, LFS batch decode
- `internal/auth`: JWT verification, ref-policy parsing
- `internal/secrets`: secret envelopes, key parsing
- `internal/jobs`: pack header/idx counts
- `internal/api`: rate-limit client keying, pagination cursors

Targets assert *graceful* behavior, not just no-panic: garbage maps to
errors, never a fabricated identity, plaintext, or capability. Most check an
independent reference oracle or a known-answer set built from test-only keys.

Seed corpora are the `f.Add` calls beside each target, so plain `go test ./...`
replays every seed in normal CI. A campaign writes a failing input to
`<pkg>/testdata/fuzz/<FuzzTarget>/`; commit it with the fix so it replays
forever.

Run one target per invocation:

```sh
go test ./internal/auth -run '^$' -fuzz '^FuzzVerify$' -fuzztime 60s -parallel 8
```

Keep `-parallel` near 8 on high-core machines (the default is one worker per
core). `internal/githttp` fuzz processes skip the e2e boot via a fuzz early-out
in `TestMain`.

[`fuzz-nightly.yml`](../../.github/workflows/fuzz-nightly.yml) runs each target
sequentially for 4m (dispatch inputs `fuzztime` / `parallel` override). A
failing input turns it red and uploads `internal/*/testdata/fuzz/` as the
`fuzz-crashers` artifact.

## Benchmarks

Hot-path benchmarks (`func Benchmark*` in `*_bench_test.go` files) set the
per-request and per-trigger baselines for optimization work:

- `internal/githttp`: hydration fast path, steady-state `EnsureCacheRepo`
- `internal/blob`, `internal/ref`: quota-admission usage listing, ledger reservation
- `internal/jobs`: repack fold selection
- `internal/auth`: bearer-token verification per algorithm family
- `internal/api`: pre-auth rate limiting under mutex contention

House conventions:

- Report allocations (`b.ReportAllocs`).
- Iterate with `b.Loop()` (setup stays untimed, results stay live).
- Express size and algorithm matrices as `b.Run` sub-benchmarks (`packs=100`,
  `candidates=1000`, `ES256`).
- Keep fixtures deterministic and container-free: SQLite, fileblob, or
  in-memory buckets; never the network, Docker, or real S3.
- Check each timed iteration's result, so a benchmark that drifts off the
  intended path fails instead of reporting wrong numbers.

Full local sweep (`-run '^$'` skips tests, `-p 1` stops packages contending,
the local backend keeps `internal/githttp` container-free):

```sh
OPENSTORAGE_TEST_BACKEND=local go test -p 1 ./... -run '^$' -bench . -benchmem
```

Benchmarks are **not** a per-PR gate (shared runners are too noisy).
[`bench-nightly.yml`](../../.github/workflows/bench-nightly.yml) uploads the
`bench-results` artifact; dispatch inputs `benchtime` / `count` override the
budget (`benchstat` wants `count` near 10). `BenchmarkAdmitReserve/parallelism=N`
scales `b.SetParallelism`, so it runs N × GOMAXPROCS workers, not N.

### Push benchmark client-chain failures

`BenchmarkPushE2E` generates each writer's commits after the seed push, before
warmup or measurement. If a client `git commit` fails, the error keeps Git's
message and exit status, followed by `benchGenerateChain diagnostics`
collected synchronously before fixture cleanup:

- UTC timestamp, safe commit arguments, repository/object paths, parent stat
  results, `TMPDIR`/`GOTMPDIR`;
- filesystem identity/capacity: device ID, `statfs` FSID, free blocks and inodes.

It never collects Git config, credentials, other environment variables,
fixture contents, or mount source/target names.

- Object-path resolution has a two-second process-group deadline and a 4 KiB
  stdout cap; stderr is discarded. On failure the report labels the fallback
  (`GIT_OBJECT_DIRECTORY`, or the client's `.git/objects`).
- `statfs` tries the object path and up to seven ancestors, labeling the one
  measured as `statfs_path`. An ancestor's filesystem may differ from the
  failed write's.
- Observations are not an atomic snapshot or a diagnosis of the unexplained
  ENOENT flake.

Commits are never retried; successful runs, timing, and assertions are
unchanged, and these observations are not throughput data.
`TestBenchGenerateChainDiagnostics_Synthetic*` validates the diagnostics with
deliberately invalid fixtures; it does not reproduce the flake.

## The Floci S3 lane

[`floci-nightly.yml`](../../.github/workflows/floci-nightly.yml) boots
[Floci](https://hub.docker.com/r/floci/floci), a lightweight AWS wire-protocol
emulator, with S3 auth enforced. Both Floci workflows and `s3waltest.FlociImage`
pin stable `2.3.0` by the same OCI index digest. The blob job mints an IAM
access key through the IAM API and runs `go test ./internal/blob` with
`OPENSTORAGE_TEST_S3_URL` set. That variable gates the endpoint tests in `internal/blob/s3endpoint_test.go`
(skipped elsewhere): a CRUD round-trip through the real `blob.S3` surface, and
presigned-URL SigV4 (valid ⇒ 200 with the exact bytes; corrupted ⇒
`403 SignatureDoesNotMatch`).

The per-PR SeaweedFS container test already pins presign accept/reject with a
static enforcing identity. This lane adds the external-endpoint path, an
IAM-minted key, and a second, independent wire-protocol implementation.

The #274 qualification compared `nightly-09012026` with stable `2.1.0`, using the
exact linux/amd64 images. Both passed the unchanged required surfaces, the IAM
blob suite, the full Git/LFS STS suite, and 60-second contention runs at 2, 8,
and 32 writers. These observations cover local emulators, not arm64 runtime or
real AWS.

Stable `2.2.0` (upstream revision `17c99047`) replaced `2.1.0` after a
comparison on the exact linux/amd64 images. Both passed the 22 required
surfaces, `get-under-overwrite`, and 60-second contention runs at 2, 8, and 32
writers. `2.2.0` also passed the IAM blob suite and the full Git/LFS STS suite,
and its `delete-if-match` row changed from `ignores` to `pass`.

Stable `2.3.0` (upstream revision `ee15f502`, which descends from `2.2.0`)
replaced `2.2.0` after the same comparison on the exact linux/amd64 images.
Both passed the 22 required surfaces, `get-under-overwrite`, `delete-if-match`,
and 60-second contention runs at 2, 8, and 32 writers. `2.3.0` also passed the
IAM blob suite and the full Git/LFS STS suite. The credential and gap probes
below gave the same result on both releases.

**Known Floci gaps** — do not extend this lane into them. All are verified open
on the pin `2.3.0` (`s3waltest.FlociImage`) with
`FLOCI_SERVICES_S3_ENFORCE_AUTH=true` and an IAM-minted key:

- No IAM policy evaluation on S3 actions (an explicit `Deny s3:GetObject` still
  allows `GetObject`), and `x-amz-expected-bucket-owner` is ignored (a wrong
  12-digit owner passes `GetObject` and `HeadBucket`; AWS returns 403).
  Unit-test seams cover both. With `FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED=true`,
  `2.2.0` and `2.3.0` deny that read (`AccessDenied`), but no lane sets the
  flag: it also makes IAM reject the `root`/`root` bootstrap credentials the
  lanes use.
  The expected-owner header stays ignored with the flag on.
- A long-lived IAM key sent with an unexpected session token still passes.
- Besides IAM- and STS-minted keys, the data plane accepts Floci's built-in
  `test` access key (secret `test`; the signature is checked). That key reads
  buckets and objects that an IAM-minted key created. `2.1.0` and `2.2.0`
  behave the same.

**Fixed in `2.2.0`: conditional DELETE.** Through `2.1.0`, `DeleteObject`
ignored `If-Match`: a mismatch still deleted, with no 412 (MinIO-class
behavior). On `2.2.0` a mismatched `If-Match` returns `412 PreconditionFailed`
and the object survives; a matching `If-Match` deletes
([floci-io/floci#4526](https://github.com/floci-io/floci/pull/4526)).

**Fixed in `2.1.0`: header-auth checks.** On `nightly-09012026`, header-auth
SigV4 checked only that the access-key ID exists (unknown ⇒ `InvalidAccessKeyId`),
so a wrong secret or forged session token passed. In the #274 `GetObject` probes
on `2.1.0`, a wrong secret fails with `SignatureDoesNotMatch` for IAM and STS
keys. A temporary key with a missing, forged, garbage, or another session's
token fails with `InvalidAccessKeyId`. Valid IAM and STS reads return the exact
bytes. Presigned-URL signatures were already validated and still are (tampered
GET or PUT ⇒ `403 SignatureDoesNotMatch`); that is the SigV4 property this lane
pins. The same probes give the same results on `2.2.0` and `2.3.0`.

**Fixed upstream: torn reads.** Through `2.0.1`, a GET racing a same-key PUT
could return a body that was neither version, with broken HTTP framing (Go logs
"Unsolicited response received on idle HTTP channel") that poisoned keep-alive
connections. That disqualifies any s3wal-shaped lane, where manifest reads race
CAS overwrites. [floci-io/floci#2874](https://github.com/floci-io/floci/pull/2874)
(merged 2026-09-01, refined by [#2901](https://github.com/floci-io/floci/pull/2901))
fixed it. `nightly-09012026` was the first build with the fix. Stable
[`2.1.0`](https://github.com/floci-io/floci/releases/tag/2.1.0) contains both
changes and replaced that dated pin after qualification in #274. `2.2.0`
descends from it, and `2.3.0` descends from `2.2.0`. The `get-under-overwrite`
surface guards it in require mode.

**Positives:** Floci passes the full 22-surface s3wal qualification suite
(`TestQualifyExternalEndpoint` in `internal/ref/s3wal`; 22/22 on the pin via
`TestQualifyFloci`), the only emulator besides SeaweedFS to do so. SeaweedFS
4.46 and Floci `2.2.0` and `2.3.0` enforce conditional DELETE (412; the object
survives a mismatched `If-Match`). The legacy MinIO pin does not.

**Floci row of the s3wal qualification matrix.** The same workflow runs
`TestQualifyFloci` (`internal/ref/s3wal/qualify_floci_test.go`) against the
pinned `s3waltest.FlociImage`: the 22 surfaces plus `get-under-overwrite` in
**require mode**, and `delete-if-match` in record mode (`2.3.0` honors the
condition, but s3wal never relies on it — ADR-46).

This is regression armor from a second implementation, **not** s3wal promotion
evidence: the store requirements in [the production guide](../how-to/deploy-production.md)
are unchanged, and Floci is not a qualified `s3wal://` store. Like the
SeaweedFS lane, the test is gated only by `-short`, so it also runs in the
per-PR `test` job. The nightly job also asserts a complete VERDICT transcript.

**Read failures in `get-under-overwrite`.** A racing GET that fails is never a
clean pass. The row ends `mishandles` on a torn body or ETag, and `error`
(inconclusive, possible framing corruption) on any other failed read, except one
that the test process explains. A heartbeat in the test process records every
time it was frozen for 100 ms or more (a paused runner, a throttled cgroup,
`SIGSTOP`). A failed read counts in `stallErrs`, and does not fail the row, only
when it hit the 2 s op deadline and the process was frozen for all but a quarter
of that deadline. The verdict quotes the first three failures with `class`
(`deadline`, `short-body`, `reset`, `status` or `other`), `elapsed`, `awake`
(the time the process ran during the read) and `explainedByStall`. `short-body`
is the framing shape: the headers arrived and the body did not finish.
`deadline` is a read that got no headers in time, and `status` is an error reply
from Floci. A `short-body` or `deadline` read with a long `awake` time is a
stall of Floci or the network, because the test process kept running. `maxRead`
and `maxStall` give the slowest successful GET and the longest freeze of the
run.

Run it locally (Docker + the `aws` CLI):

```sh
docker run -d --name floci -p 4566:4566 \
  -e FLOCI_SERVICES_S3_ENFORCE_AUTH=true \
  floci/floci:2.3.0@sha256:e0b292df4dc9559b7b1bf4f51bdda4bb8e269276236438cd03cae661260ea8a9

# IAM accepts any credentials; only S3 enforces auth.
export AWS_ACCESS_KEY_ID=root AWS_SECRET_ACCESS_KEY=root AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:4566 iam create-user --user-name blobtest
aws --endpoint-url http://127.0.0.1:4566 iam create-access-key --user-name blobtest
# export the returned AccessKeyId / SecretAccessKey, then:

OPENSTORAGE_TEST_S3_URL='s3://floci-test?endpoint=http://127.0.0.1:4566&s3ForcePathStyle=true&region=us-east-1' \
  go test ./internal/blob -run 'TestS3Endpoint' -count=1 -race -v
```

For real S3, point `OPENSTORAGE_TEST_S3_URL` at an existing bucket
(`s3://my-bucket?region=us-east-1`) with real credentials in the environment.

> [!WARNING]
> Use an unversioned scratch bucket, never a production one.
>
> - Without an `endpoint` parameter the bucket is only probed, never created:
>   re-creating an owned us-east-1 bucket would reset its ACLs.
> - Versioned or version-suspended buckets are refused; cleanup needs
>   `DeleteObject` to reclaim test keys, not write delete markers.
> - Keys are uniquely suffixed and cleaned up.

## The Floci githttp E2E lane

[`floci-nightly-githttp.yml`](../../.github/workflows/floci-nightly-githttp.yml)
boots the pinned Floci image with S3 auth enforced, mints an IAM user and key,
and assumes a role via STS. It reruns the full PG-mode `internal/githttp` E2E
suite with blobs on Floci and the STS **session credentials** in the
environment. It uses the same digest-pinned build as `s3waltest.FlociImage`
and the blob workflow: bump all three together.

The seam is `OPENSTORAGE_TEST_S3_ENDPOINT`, a test-only `TestMain` override
like `OPENSTORAGE_TEST_BACKEND` that never touches production config parsing.
On the pg lane it replaces the managed SeaweedFS container with the external
endpoint; on other lanes it panics instead of being ignored. The last workflow
step asserts the suite's bucket exists on Floci, so broken plumbing fails
instead of passing on a quietly booted SeaweedFS.

It adds over the per-PR `test` job:

- a second, independent S3 wire-protocol implementation under the whole
  git/LFS surface (wire-drift armor);
- the only lane that *runs* on IAM/STS-minted session credentials; all others
  sign with long-lived static keys.

Limit: the lane signs only with fresh STS session keys, so the suite provably
works on temporary keys. Floci also accepts its built-in `test` key (see the
gap list), which the lane never uses. On `2.1.0` (the #274 probes), `2.2.0` and
`2.3.0`, header-auth `GetObject` rejects a missing, forged, or mismatched
session token, unlike `nightly-09012026`. A dropped or forged
`AWS_SESSION_TOKEN` on those reads would now fail the lane. Other operations
were not probed, and the lane does not establish IAM policy enforcement or full
AWS auth compatibility.

Floci's default in-memory storage suits a fresh-per-run lane. Persistent mode
(`FLOCI_STORAGE_MODE=persistent` plus a volume) was verified working on an earlier
Floci build, if restart survival is ever needed.

Per-PR blobs stay on SeaweedFS because of the remaining
[Floci gaps](#the-floci-s3-lane), including unevaluated IAM policies and ignored
expected-owner headers. This lane
is nightly-only and **not** a required PR check.

Run it locally (Docker + the `aws` CLI; the tag is `s3waltest.FlociImage`):

```sh
docker run -d --name floci -p 4566:4566 \
  -e FLOCI_SERVICES_S3_ENFORCE_AUTH=true \
  floci/floci:2.3.0@sha256:e0b292df4dc9559b7b1bf4f51bdda4bb8e269276236438cd03cae661260ea8a9

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

AWS_REGION=us-east-1 OPENSTORAGE_TEST_S3_ENDPOINT=http://127.0.0.1:4566 \
  go test ./internal/githttp -count=1 -race -timeout=25m
```
