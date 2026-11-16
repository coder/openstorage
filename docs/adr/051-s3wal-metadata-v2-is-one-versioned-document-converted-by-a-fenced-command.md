# ADR-51: S3WAL metadata v2 is one versioned document, converted by a fenced command

> **Status: Accepted.** This record answers the format question of issue
> [#335](https://github.com/coder/openstorage/issues/335) (S3WAL Git-push
> scheduling) and the S3WAL part of
> [#388](https://github.com/coder/openstorage/issues/388) (create state). It
> follows the [S3WAL format stability policy](../explanation/storage-engine.md#s3wal-format-stability-policy),
> which forbids new or reinterpreted durable fields in `_wal/meta.json`
> without a version bump, a tested in-place migration, and an operator
> upgrade procedure. S3WAL stays experimental. Nothing here changes a
> [#192](https://github.com/coder/openstorage/issues/192) promotion gate.
>
> **Amended by [#445](https://github.com/coder/openstorage/issues/445) and
> [#466](https://github.com/coder/openstorage/issues/466):** a v2 hook on a v1 document
> now refuses the ref update with the conversion hint and writes nothing. A document that
> does not decode is refused the same way, with a pointer to the stopped-node restore.
> The hook no longer accepts such a push without its events. A metadata store the hook
> cannot reach no longer fails open for intent either: the hook refuses every update that
> could record forward intent (fact 5's amendment). Fact 5 and test-plan item 1 below
> predate these changes.
>
> **Amended by [#452](https://github.com/coder/openstorage/issues/452) item 5:**
> `--to 1` now also sets `sync_pending` on every unfinished sync create (pending, with a
> live upstream link), so the v1 release runs its first sync at boot. It refuses, and
> writes nothing, while a fork or copy create is unfinished, and names each repo. `--force`
> converts anyway. The downgrade text below predates that change.
>
> **Known limit (#466):** the hook checks the document before its ref update, not
> inside it. A conversion that runs between the check and the update lets that push
> commit without its events. The procedure stops `serve` and its hooks before any
> conversion, which closes the window. Closing it in code would need a format check
> inside every push's update.

## Context

`_wal/meta.json` holds every metadata table of an S3WAL bucket in one
conditional-CAS object ([ADR-46](046-s3wal-refs-and-metadata-ride-one-conditional-cas-object-per-repo-group.md)).
Its schema is `s3walMetaDoc` in `internal/meta/s3walmeta.go`, with
`s3walMetaVersion = 1`. The v1 schema has not changed since the policy
baseline `d94125f6`. The first release that contains the baseline is
v0.16.0.

Five facts shape the format decision:

1. **A v1 binary rejects every other version.** `decodeS3WALMetaDoc`
   returns `document version N unsupported (want 1)` for any `v` other than
   1, and `encodeAdvance` asserts that it writes v1. A v1 process therefore
   cannot read or overwrite a v2 document.
2. **Every writer is fenced by the object CAS.** A write is a full-document
   `PutObject` with `If-Match` on the ETag the writer read. A writer that
   read v1 before a conversion gets 412 on its next write, re-reads, and
   then fails to decode v2. No writer can put a v1 document over a v2
   document.
3. **More than one process writes the document.** `serve` writes it. The
   `signing-key` and `purge-repo` commands open it directly and do not take
   the instance lease. The `openstorage hook` subprocess reads it today
   (sync fence, signing keys, webhooks), and under #335 it also records push
   and pull intent (`RequestPush`, `RequestSync`). Git runs the hook through a
   script that names the path `os.Executable()` returned to `serve`
   (`internal/githttp/hydrate.go`). If an operator replaces the binary at that
   path while the old `serve` runs, new hook subprocesses run the new binary.
   The `reference-transaction` hook, installed only if absent, can keep an
   older path, but it never reads metadata.
4. **The instance lease detects a second `serve`, it does not enforce
   one.** The background org-key refresher of a running process drops read
   errors and keeps its last snapshot. A process that stays up across a
   conversion keeps answering token checks from that snapshot, while each of
   its foreground metadata operations fails. Git requests then answer 404,
   because repository resolution maps every metadata error to "not found"
   (`cmd/openstorage/main.go`, `ResolveRepo`).
5. **Hook eventing fails open.** If the hook cannot use the metadata store,
   it logs `hook eventing disabled` and accepts the push without events
   (`cmd/openstorage/wire_eventing.go`). These hooks run after the ref update
   commits. A hook that cannot decode the document therefore drops the push's
   webhooks and, under #335, its push and pull intent.

   **Amendment: hook eventing no longer fails open for intent.** A hook that reads a
   document version it cannot write, or a corrupt document, refuses the ref update
   before its CAS (`meta.CheckFormat`, issues #445 and #466). On the local schedule
   the hook records the push's intent before the CAS and refuses the update when it
   cannot ([ADR-50](050-local-scheduling-state-is-row-owned.md)). A hook that cannot
   wire its eventing refuses every update that could record forward intent
   ([ADR-33](033-git-push-eventing-crosses-processes-durable-river-inserts-on-pg-synchronous-deliveries-in-local-mode.md)'s
   amendment). Updates that record no intent still go through without events.

## Fields and features that need v2

Each new field has an absent value that means exactly what v1 means today.
This is the **zero-value rule**. Every v2 field must follow it. It makes the
v1 to v2 conversion a pure version change, and it makes a v2 to v1
conversion lossy but safe.

| Field (v2) | Type | Meaning | Absent value means | Consumer |
|---|---|---|---|---|
| `upstreams[id].push_attempt` | int | Retry position of outstanding push work (0 = first attempt). It stays in the row while the run executes. | first attempt | #335 ([ADR-50](050-local-scheduling-state-is-row-owned.md) parity) |
| `upstreams[id].push_due` | int64 unix ns | Push work is not claimable before this time. While a run holds the lane, it holds the claim time, and `RequestPushNow` sets it to 0 to mark an explicit request made during the run (ADR-50, `internal/meta/sqlite_schedule.go`). | due now, and during a claim: an explicit request arrived | #335 |
| `upstreams[id].sync_due` | int64 unix ns | Pull work is not claimable before this time. Set only by a `restore` after a failed publication wait or a shutdown. | due now | #335, [#343](https://github.com/coder/openstorage/issues/343) |
| `repos[id].create_state` | int | 0 legacy, 1 pending, 2 complete, as in `repos.create_state` on PostgreSQL and SQLite. | legacy (a repeat create answers 409) | [#388](https://github.com/coder/openstorage/issues/388), remaining criteria of [#387](https://github.com/coder/openstorage/issues/387) |

The three schedule fields carry the ADR-50 transitions (`RequestPushNow`,
`ClaimDuePush`, `ClaimDueSync`, `FinishPushOutcome`, `FinishSyncOutcome`,
`ListLocalWork`, `CompleteSyncRepoCreate`) to S3WAL. On S3WAL,
`ClaimDueSync` keeps the existing fence-token write into the
repository-group manifest and the pre-clear on steal
(`internal/meta/s3walmeta_eventing.go`, `TryBeginSync`). With them, S3WAL
gets:

- Git-surface push scheduling: a hook records push or pull intent with one
  metadata CAS, as on SQLite. This closes the S3WAL gap in
  [wire compatibility](../explanation/wire-compatibility.md) and the
  S3WAL part of #192 criterion E1.
- Retries that survive a restart, which replaces the in-process
  `time.AfterFunc` timers and the one-shot boot recovery on S3WAL.
- The shutdown `restore` outcome. A run that shutdown cancels keeps its
  request for the next process. #343 moved this S3WAL work into #335.

`create_state` lets S3WAL store what #391 already stores on the SQL
backends. #388 itself is deferred until a caller needs a repeated create to
answer 200. Carrying the field in v2 now avoids a second version bump later.

These are not consumers:

- [#376](https://github.com/coder/openstorage/issues/376) (`repo-changes`
  feed) closed as obsolete when upstream removed the route. Its per-ref
  change times and tombstones belong to the repository-group manifests
  (`_wal/repo/<baseRepoID>`), a separate frozen format.
- "Retry holds" (sentinel claim values in v1 fields) were rejected in #266:
  they reinterpret frozen fields and lose the attempt while a retry runs.

Repository-group manifests stay v1. This record covers `_wal/meta.json`
only.

**The rule during a claim.** A v1 document can hold a claim left by a
stopped or crashed process. After conversion to v2, that lane reads
`push_due = 0`, which during a claim means "an explicit request arrived".
No run holds that claim any more, so no `retry` or `restore` reads that
mark. The next `ClaimDuePush` steals the expired claim and writes its own
claim time.

Due times use the same process wall clock as the existing claim leases
(`WithNowFunc`). A skewed clock can only start a retry early or late. No
destructive verdict reads these fields, so the manifest-clock skew guards of
ADR-46 do not apply.

## Options

### Option A: a versioned v2 document

`v: 2` with the four fields as typed members of the existing structs. A v2
binary reads v2. Conversion between v1 and v2 is an explicit, fenced command
(see [Rollout](#rollout-variants-for-option-a)).

- **Migration.** v1 to v2 rewrites `v` only. By the zero-value rule, every
  new field starts absent: rows are legacy, schedules are due now at the
  first attempt. This matches the ADR-50 SQLite upgrade, where pending work
  recorded before the upgrade is due at once.
- **Mixed versions.** A v1 binary fails closed on a v2 document (fact 1).
  CAS prevents an old writer from overwriting v2 (fact 2).
- **Rollback.** A v2 to v1 conversion drops the four fields. Rows read
  legacy, which a v1 binary already assumes. Pending levels are v1 fields
  and stay, so the v1 boot recovery runs them at once with a fresh retry
  budget. The cost is at most one extra retry series against an unreachable
  upstream. In-flight backoff and attempt counts are lost.
- **Rollback and in-flight claims.** A pull claim consumes `sync_pending`.
  A lane whose run stopped while it held the claim (for example after a
  failed shutdown `restore`) has `syncing = true` and `sync_pending = false`.
  v1 binaries built after [#427](https://github.com/coder/openstorage/pull/427)
  list such lanes at boot and set `sync_pending` again before they claim
  anything. v0.16.0 and v0.17.0 list pull lanes only by `sync_pending`, so
  they do not restart that pull at boot. It waits for the next push, poll or
  explicit `pull-upstream` request. `--to 1` therefore sets `sync_pending` on every
  lane with `syncing = true`, which is the #427 boot step done ahead of
  time. Every supported v1 target then runs the pull once the dead claim
  expires. The push lane needs no change: every v1 release lists
  `push_syncing` lanes at boot and sets the pending level again.
- **Verdict.** It meets all three policy obligations, and old binaries fail
  closed instead of guessing.

### Option A′: v2 with forward-compatible optional fields

Option A, plus two rules so that later fields need no new version: the
decoder keeps unknown fields and writes them back, and the document carries
a `requires` list of feature names that a reader must understand or refuse.

- **Migration and rollback.** As Option A for the four fields.
- **Mixed versions.** A v2 binary from before a later field keeps that field
  but ignores its meaning. That is safe only for fields whose meaning a
  writer can ignore. None of the four fields qualifies: a writer that keeps
  `push_due` but ignores it claims before the due time, and one that ignores
  `push_attempt` never resets it, which shortens the next retry series. Both
  break the ADR-50 invariant, so each such field needs a `requires` entry,
  and a `requires` entry is a version bump under another name.
- **Verdict.** It adds a preserving codec and a feature registry, and no
  known field uses them. Defer it until a field arrives whose meaning
  writers can safely ignore.

### Option B: a sidecar object

Keep `meta.json` at v1 and put the new state in another object, for example
`_wal/sched.json` or one object per repository.

- **Migration.** None for `meta.json`. The sidecar needs its own version and
  procedure, because it holds durable meaning that the policy covers.
- **Mixed versions.** A v1 binary ignores the sidecar. It claims lanes
  without due times and creates rows without a create state, and nothing
  tells it that the sidecar exists.
- **Atomicity.** S3 has no multi-object CAS. ADR-50 requires the claim and
  the due time to change in one single-row write, but the claim fields stay
  in `meta.json`. Moving claims to the sidecar leaves v1 binaries writing
  the old claim fields: two sources of truth for one lane, the failure that
  sank PR #271. The create state has the same split: `CreateRepoWithState`
  inserts the row in `meta.json`, and the completion and the `DELETE` race
  across two objects.
- **Rollback.** Delete the sidecar. This is easy, but only because v1
  binaries never honored it.
- **Verdict.** Rejected. It recreates the two-sources-of-truth problem and
  still needs a format decision.

### Option C: keep v1 and add unknown-field tolerance (a "v1.1" step)

Release a v1 binary that keeps unknown fields on rewrite, then add the
fields under `v: 1`.

- **Migration.** None.
- **Mixed versions.** Every released v1 binary (v0.16.0, v0.17.0) still
  drops unknown fields on rewrite and accepts the document without error. A
  downgrade, or an old `signing-key` command run from another host, silently
  erases due times and create states and applies v1 meaning to the rest.
  Tolerance protects bytes, not meaning, so even a tolerant binary ignores
  `push_due`, as in Option A′.
- **Rollback.** Implicit and silent, which is the problem.
- **Verdict.** Rejected. It extends frozen v1 without a version bump, which
  the policy forbids, and it removes the fail-closed property of fact 1.

## Rollout variants for Option A

| Variant | Behavior of the v2 release | For | Against |
|---|---|---|---|
| **R1: convert before serve** | `serve`, `hook`, `signing-key` and `purge-repo` read v2 only. On a v1 document they stop with an error that names the conversion command, and they write nothing. Only the conversion command reads v1. A new, empty bucket starts at v2. | One scheduling path per binary. Small test matrix. Every write and its meaning are unambiguous. | The upgrade has one extra step. There is no trial run of the new binary on unconverted data. |
| **R1b: `serve` converts at start** | As R1, but `serve` converts a v1 document after its probe and lease acquisition when an explicit setting asks for it. This follows the SQLite precedent of an idempotent upgrade at start. | No extra command for the upgrade. Reuses the probe, lease and heartbeat of `serve`. | The downgrade still needs a separate command with the same fencing, so two conversion paths exist. A setting left on converts every bucket the process starts against. |
| **R2: dual-format window** | The v2 release also serves v1 documents, with the v2 features off, and converts later on command. A later release drops v1. | Plain binary swap. Rollback before conversion is a plain downgrade. | Each binary keeps the timer path for v1 documents and the durable path for v2, chosen on each read: the two-path state that ADR-50 removed on SQLite. Two releases carry the extra path and its tests. |

## Decision: Option A with rollout R1

1. **Format.** `_wal/meta.json` v2 = v1 plus the four fields above, with no
   other change. `s3walMetaVersion` becomes 2. Repository-group manifests
   stay v1.
2. **Readers.** Normal code paths decode v2 only. The decoder still rejects
   every other version, and the encoder asserts v2.
3. **Conversion command.** A new subcommand,
   `openstorage s3wal-meta convert --to 2` (and `--to 1`):
   - It first runs the conditional-write probe, as `serve` does. Then it
     acquires the instance lease (`s3wal.AcquireInstanceLease`). It refuses
     while a fresh lease held by another process exists, so it cannot run
     under a live `serve`. It ignores `OPENSTORAGE_S3WAL_LEASE_TTL=0`: the
     lease is always on for a conversion. A `serve` started with the lease
     off is invisible to the command, and the procedure says so.
   - It renews the lease (`Heartbeat`) for its whole run, and it stops
     before its write if a renewal fails. It joins the heartbeat before it
     releases the lease.
   - It can steal the expired lease of a `serve` that hangs but is still
     alive. That `serve` stops at its next renewal. CAS keeps the document
     safe meanwhile (fact 2).
   - It reads the document, checks the source version, writes the converted
     document with `If-Match`, and re-reads it to verify. It has its own
     encoder and a recovery read that decodes both versions: `mutate`
     cannot serve here, because its recovery read rejects every version but
     the binary's own. An ambiguous PUT resolves through the writer nonce.
   - It compares the decoded result with the expected document: the source
     with the conversion applied (for `--to 1`, the four fields dropped and
     `sync_pending` set on every lane with `syncing = true`), apart from
     `seq` and `writer`. It reports a mismatch and exits non-zero. It prints the version, `seq`, and row counts per
     table for both documents.
   - Re-running it on a document already at the target version changes
     nothing and exits 0.
   - `--to 1` is the supported downgrade path. It takes the same lease and
     applies the zero-value rule in reverse: it drops the four fields, and
     it sets `sync_pending` on every lane with `syncing = true` (see Option
     A, rollback). Supported targets are v1 releases from v0.16.0 on.
4. **Supported sources.** v1 documents written by v0.16.0 or later (the
   releases that contain the baseline). An older installation first upgrades
   to the last v1 release, as the release-pair harness already covers for
   v0.15.0 to v0.16.0.
5. **No automatic conversion.** Neither `serve` nor any other command
   converts on its own. A format change happens only on an explicit
   operator command.
6. **Create state before #388 resumes.** S3WAL writes a create state only
   where an S3WAL completion path exists. `CreateRepoWithState` stores the
   requested state (today only plain creates call it, with `complete`), and
   `CompleteRepoCreate` stores `complete`. The API calls `CreateLinkedRepo`
   only when the queue tracks create state (`TracksCreateState`), which
   `jobs.Local` reports as false on S3WAL until PR 3. PR 2 implements
   `CompleteSyncRepoCreate` on S3WAL with no callers. PR 3 makes
   `TracksCreateState` true on S3WAL, and from then on S3WAL follows the SQL
   backends: a sync create is pending until its first sync is recorded, and
   `pull-upstream` can finish it
   ([#432](https://github.com/coder/openstorage/pull/432)). No S3WAL row is
   pending without a way to finish.

Reasons:

- It meets the policy: a version bump, an in-place conversion shipped and
  tested in the new binary, and an operator procedure.
- Old binaries fail closed (fact 1), and CAS makes the conversion atomic
  against every writer (fact 2). The design adds no new consistency
  argument.
- One binary has one scheduling path on S3WAL. This keeps the ADR-50
  invariant: the lane row owns the schedule, and nothing process-local
  admits or delays work.
- The zero-value rule makes the conversion a version change only, and it
  makes a real downgrade possible without a backup restore.
- S3WAL is experimental and single-node. R1 costs one upgrade step and needs
  no compatibility window.
- Changing the format while S3WAL is still experimental is cheaper than
  after a beta "format-stable" label (#192 stage 1).

## Operator procedure

**Upgrade (v1 release to the first v2 release).**

1. Stop `serve` with SIGTERM. Wait for the process and its hook children
   to exit, as the [backup runbook](../how-to/backup-restore-s3wal.md)
   requires. Do not run `signing-key` or `purge-repo` until step 5 ends.
2. Take a verified [stopped-node backup](../how-to/backup-restore-s3wal.md).
3. Replace the binary. Replace it only while `serve` is stopped: hook
   subprocesses run whatever binary is at the path (fact 3).
4. Run `openstorage s3wal-meta convert --to 2` with the serve configuration.
   `serve` releases its lease on SIGTERM within a bounded, best-effort
   window. If the command reports a fresh lease after step 1, confirm that
   no `serve` runs, wait for the recorded lease TTL plus the store-clock
   margin, and run it again. The
   command checks its own result (see above) and exits non-zero on a
   mismatch.
5. Start `serve`. Pending push and pull work runs at once, at the first
   attempt.

**Downgrade (back to a v1 release from v0.16.0 on).** This is a supported
path.

1. Stop `serve` with SIGTERM. Wait for the process and its hook children
   to exit. Do not run `signing-key` or `purge-repo` until step 3 ends.
2. Run `openstorage s3wal-meta convert --to 1` with the v2 binary. A fresh
   lease is handled as in upgrade step 4.
3. Replace the binary with the v1 release and start it.

Data written after the upgrade stays. Backoff positions are lost, and every
create state reads legacy. Restoring the step-2 backup is the alternative
when the v2 binary cannot run, and it loses every write made after the
backup.

**What a missed step does.** A v1 binary on a v2 bucket stops at its first
metadata read with `document version 2 unsupported (want 1)`. A v2 binary on
a v1 bucket stops with an error that names the conversion command. Neither
writes `meta.json`. A `serve` can still write its lease and probe objects
before its first metadata read. A binary replaced under a running `serve`
is the exception: its hooks refuse every update with the conversion hint and write
nothing (the #445 and #466 amendment above).

## Mixed-version behavior (R1)

| Process | Document v1 | Document v2 |
|---|---|---|
| v1 `serve`, `hook`, `signing-key`, `purge-repo` | works (today) | fails closed at the first metadata read. A process started before the conversion fails its next foreground metadata operation, because the conditional GET returns the v2 body. Its org-key refresher keeps the last snapshot, so the procedure stops it first. |
| v2 `serve`, `signing-key`, `purge-repo` | fails closed with the conversion hint, no write to `meta.json` | works |
| v2 `hook` (binary replaced under a running v1 `serve`) | Enforcement reads (sync fence, signature keys), where a push needs them, refuse the ref update. Eventing refuses the ref update too, with the conversion hint, and writes nothing (the #445 and #466 amendment above). | works |
| v2 `s3wal-meta convert --to 2` | converts (under the lease) | no change, exit 0 |
| v2 `s3wal-meta convert --to 1` | no change, exit 0 | converts (under the lease) |

## Test plan

Tests are listed with the PR that adds them.

1. **Codec (PR 1).**
   - A v2 round trip keeps all four fields. The v2 decoder rejects
     versions 1 and 3 on normal paths. The encoder asserts v2.
   - Every v2 entry point fails on a v1 document with the conversion hint and
     no write: the document `seq` and ETag do not change.
   - A v2 hook on a v1 document: the push is accepted without events, the
     log line names the version error, and `meta.json` is unchanged.
2. **Conversion (PR 1).**
   - A v1 fixture corpus (documents written by the official v0.16.0 and
     v0.17.0 binaries through the release-upgrade harness) converts to v2.
     Every table decodes equal, except `v`, `seq` and `writer`. New fields
     are absent.
   - Re-running is a no-op. The command refuses under a fresh foreign lease.
   - Race: a v1 writer writes between the command's read and its write. The
     command gets 412, re-reads, and keeps the writer's change. A v1 writer
     whose write lands after the conversion gets 412, then a decode error,
     and the v2 document is unchanged.
   - An injected ambiguous PUT resolves by the writer nonce, both when the
     PUT was lost and when it landed.
   - The command ignores `OPENSTORAGE_S3WAL_LEASE_TTL=0`. It stops before its
     write when a lease renewal fails.
   - `--to 1` sets `sync_pending` on a lane whose pull claim is set. The
     official v0.16.0 binary and the latest v1 release then each run that
     pull after the claim expires.
3. **Zero-value rule (PR 1).**
   - Property test: for random v2 documents d, `to2(to1(d))` equals d with
     the four fields cleared and `sync_pending` set on every lane with
     `syncing = true`.
   - The cleared document behaves as v1: rows are legacy (repeat create 409),
     and lanes are due now at attempt 0.
   - Behavior, not only data: a document with a live claim, an expired
     claim, a backoff and a pending create converts in both directions. The
     official v1 binary and the v2 binary each serve the result, and every
     lane runs at most once per request.
4. **Release pair (PR 1, then the release).** Extend
   [`TestS3WAL_ReleaseUpgrade`](../how-to/test-release-upgrade.md):
   - The official last-v1 binary writes and stops. The candidate converts,
     then serves with the existing checks.
   - Downgrade drill: the candidate runs `--to 1`, then the official v1
     binary serves the same checks.
   - The official v1 binary on a v2 bucket fails at startup and does not
     change `meta.json`.
5. **Schedule (PRs 2 to 4).**
   - The ADR-50 per-transition conformance table and the C12 model check
     (random transitions against a pure-Go reference model) run on SQLite
     and S3WAL from one shared suite.
   - C1 to C11 of the [#266 design note](https://github.com/coder/openstorage/issues/266#issuecomment-5826596313)
     run at worker level on S3WAL in the `test-s3wal` CI job (SeaweedFS).
   - `ClaimDueSync` on S3WAL writes the manifest fence token and pre-clears
     it on steal, as `TryBeginSync` does.
   - The S3WAL skips in `local_hook_e2e_test.go`,
     `local_forward_e2e_test.go`, `local_pull_publication_e2e_test.go` and
     `e2e_sync_shutdown_test.go` are removed.
6. **Cost (PR 4).** `TestS3WAL_PushOpCensus` budgets change by the measured
   delta only, with each new operation named. Hook marking adds one metadata
   CAS per push to a linked repository. The 1 s sweep adds one conditional
   GET per second when idle.
7. **DST.** The nightly `s3wal` backend runs with v2 documents.

## Delivery

Each PR has at most 500 changed lines. A planned PR that exceeds the limit
lands as several PRs in the same order.

1. v2 codec, conversion command, zero-value tests, release-pair extension,
   and the operator procedure in `storage-engine.md`, `deploy-production.md`
   and `backup-restore-s3wal.md`. S3WAL stores create states as in
   decision item 6. Repeat creates on S3WAL keep answering 409 until #388
   adds the 200 answer for every backend.
2. `meta.LocalScheduleStore` on `*meta.S3WAL`, with no callers.
3. The S3WAL worker on the sweep, and shutdown `restore` (#343).
4. Hook marking on S3WAL, the census update, and the wire-compatibility row.

The first release that contains PR 1 is a format release. Its notes state
the procedure above.

## Effect on #335 and #192

**#335.** This record removes the blocker. PRs 1 to 4 deliver the
acceptance criteria of #335: the #266 invariant, transitions and controls on
S3WAL, qualified against SeaweedFS, with the v2 conversion and the upgrade
procedure documented and tested.

**#192.**

- **E1 (beta to GA, stage 2).** PR 4 closes the S3WAL part of E1. Git-surface
  pushes then schedule on all three backends.
- **D1 (format policy).** It stays done. `storage-engine.md` records v2 as
  the frozen metadata format from the format release on. For metadata, it
  replaces "Downgrade after incompatible-format writes is unsupported" with
  the `--to 1` procedure.
- **D2 (release-pair upgrade).** The v1 to v2 pair becomes a required run
  for the format release, with conversion and downgrade drills.
- **A4 (operation census).** It needs a re-pin after PR 4.
- **C1 and F1 (backup and runbooks).** Backups record the format version. A
  restore still uses the binary that made the backup. F1 gains the upgrade
  runbook.
- **A1 to A3, G1, G2.** No effect. Beta still waits for the real-S3 runs.

## Decisions on scope and order

- **Compatibility window (D6).** S3WAL is experimental. The format release
  has a required offline conversion (rollout R1), and no release serves both
  formats.
- **Order (D7).** v2 is implemented now, before the real-S3 starvation
  re-run (#192 A1). If A1 trips again, the deprecation plan covers this work
  too.
- **Downgrade (D8).** `convert --to 1` is a supported downgrade path, and
  the release-pair tests cover it.

## Consequences

- The policy gains one rule for every later field: a field whose absent
  value does not mean what the earlier version means needs its own
  conversion logic, not only a version change.
- `meta.json` stays the only metadata authority. No sidecar object exists.
- Every later metadata field still needs a version bump under this design.
  Option A′ stays available for a field whose meaning writers can safely
  ignore.
- An S3WAL upgrade across the format release is a stopped-node operation,
  as are the backup and restore runbooks today.
