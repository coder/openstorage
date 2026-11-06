---
name: verify-orphan-sweep
description: Model-check the orphan-pack sweeper (internal/jobs/sweep.go) against the pack writers (git pre-receive, REST publishPack, blob.EnsurePublished, and repack publishing its consolidated pack), and the repository purger (RepoPurger and purge-repo, internal/jobs/purge.go) against pre-delete writers, a second purger and purge-aware sweeps, with TLA+ and TLC, so no interleaving of deletes, quarantine restores, heals, registrations and purge phases loses a registered pack's bytes, purges a repository too early or after the delete guard failed, or stamps a purge over rows that are still there. Use before you merge any change to the sweeper's delete, quarantine or heal code, EnsurePublished, a RegisterPacks, ReplacePacks or CASRef call site or implementation, repack's verifyPublished, quarantinePurgeAge or QuarantinePurgeFloor, RepoPurger, the purge fence or the other purge seams in internal/ref and internal/meta; and whenever check.sh says STALE.
---

# Verify the orphan-pack sweeper

The orphan sweeper deletes unregistered pack bytes. It races every pack writer: a git push's pre-receive hook, every REST write (`publishPack`), a repack publishing its consolidated pack, and other replicas' sweeps. Its safety rests on an ordering argument written in comments (`Sweeper.sweep`, `blob.EnsurePublished`). This skill checks that argument with a model checker over every interleaving of the individual store and registry calls, up to the bounds below.

The repository purger (`RepoPurger`) reclaims a soft-deleted repository, on the sweep cadence once `OPENSTORAGE_REPO_RETENTION` has passed, or when an operator runs `openstorage purge-repo`. It fences the repository against new registrations, drops its ref rows, tombstones its pack rows, deletes its blobs through the sweeper's `safeDelete`, and stamps `purged_at`. It races writers that resolved the repository before its soft-delete, a second purger, and purge-aware sweeps. Its safety rests on the tombstone-then-delete argument in the `purge.go` package comment and on the fence comments in `internal/ref/purge.go`. A second model, `model/RepoPurge.tla`, checks that argument the same way.

## What it checks

| Property | Rule, in one sentence | Harm if it breaks |
|---|---|---|
| `NoLostPack` | A registered pack never loses its last copy (its blob, a quarantine backup, or an in-flight writer's local pack file). | Every cold hydrate of the repository fails with `githttp.ErrPackMissing` ("corruption grade") until someone re-pushes the same pack. |
| `NoWaitForHeal` | While nothing crashes, a registered pack is missing only while the write or the sweeper handshake that puts it back is still running. | Clones and reads fail for up to one sweep interval (`OPENSTORAGE_SWEEP_INTERVAL`, default 1 h) after a race the code claims to repair on the spot. |
| `BytesComeBack` | A registered pack with missing bytes gets them back. | Same as `NoLostPack`, through a repair that never happens. |

`NoWaitForHeal` is the claim in the `Sweeper.sweep` doc comment: "the recheck and the writer verify jointly cover every interleaving with no timing assumptions". `NoTiming.cfg` checks it with both timing assumptions off.

The purge model (`model/RepoPurge.tla`) checks these:

| Property | Rule, in one sentence | Harm if it breaks |
|---|---|---|
| `PurgeEligible` | No purge step touches a live repository or one soft-deleted less than `MinRepoPurgeAge` (1 h) ago, and the background pass waits for `max(OPENSTORAGE_REPO_RETENTION, MinRepoPurgeAge)`. | A repository is purged before the retention the operator chose, or while a push that resolved it before its delete still runs. A purge cannot be undone. |
| `GuardRespected` | No destructive step (fence, ref drop, tombstone, quarantine copy, delete, restore, completion stamp, quarantine purge) starts in a purge pass or purge-aware sweep whose `DeleteGuard` returned an error. | On a bucket that turned versioned, deletes only write billed delete markers, and the purge reports bytes reclaimed that are still stored. |
| `NoDanglingRow` | No committed ref row or registered pack row points at bytes that have no copy (the blob, a quarantine backup, or an in-flight writer's local pack file), and no ref row outlives the pack row of the objects it names. The purge form of `NoLostPack`. | The tombstone-then-delete promise breaks: a row names deleted bytes. A soft-deleted repository serves no reads, so no clone fails, but the row is stranded with nothing behind it. |
| `StampedClean` | `purged_at` is stamped only when the repository's group has no ref or pack row left, and no registration or ref write of the repository commits after it. | A write that reported success is stranded behind a completed purge, and its rows and bytes are never reclaimed: the background pass never lists a stamped repository again. |
| `PurgeCompletes` | A purge of a soft-deleted repository resumes after a crash or an error at any phase, stamps `purged_at`, and leaves no blob under the repository's prefixes. | The storage of deleted repositories is never reclaimed. |
| `BackupsCollected` | The quarantine backups that purges and purge-aware sweeps make are purged, with every `*_DELETE_*` flag off. | Reclamation only moves the bytes to `sweep/quarantine/`, where they stay (the `SweepConfig.PurgedRepoProbe` comment promises otherwise). |

## When to run it

Run it when your change touches any code in the [coverage record](#coverage-record), or code that writes `packs/` blobs, pack or ref registry rows, the purge fence, `purged_at`, or `sweep/quarantine/` keys. Also run it when `check.sh` prints STALE.

## Run

From the repository root:

```sh
OUT=$(mktemp -d "${TMPDIR:-/tmp}/orphan-sweep-check.XXXXXX")
.agents/skills/verify-orphan-sweep/check.sh --out "$OUT"
```

- Needs Go (the `go.mod` version), Java 11 or newer, and `curl`. The first run downloads `tla2tools.jar` 1.7.4 (sha256-pinned) into `${XDG_CACHE_HOME:-$HOME/.cache}/openstorage-tla`. Set `JAVA`, `GO`, or `OPENSTORAGE_TLA_CACHE` to override.
- With the default 4 TLC workers (`TLC_WORKERS`) and a 4 GB heap (`TLC_HEAP`), it took 6 to 19 minutes in four full runs on a shared host with a 32-CPU quota. The spread comes from other load on the host: each run explored the same states. No single config took more than 6 minutes. The four `Purge*.cfg` configs took a third to a half of the total.
- `--out` keeps every TLC output, the generated configs, and trace summaries. It must be a new or empty directory. Without it, check.sh makes a temporary directory and prints its path. With `--fingerprints-only` and no `--out`, check.sh deletes that directory on exit.
- `--only <cfg>` runs one config, for example `--only NoTiming.cfg` or `--only PurgeSafety.cfg`. It is for quick iteration. Finish with a full run.
- `--fingerprints-only` checks the code facts and the fingerprints without TLC or Java, in about a second. CI runs it in the `orphan-sweep-fingerprints` job of `.github/workflows/ci.yml`, so a change to covered code fails CI until someone brings the model in line and runs `check.sh --record`. CI does not run TLC: a pass says that the covered code is the code the model was checked against, not that the properties hold.

A clean run ends like this:

```text
== Code facts (HEAD b5897891)
   WriterVerifies             TRUE
   ... (22 facts, all TRUE)
== TLC 1.7.4 (outputs in /tmp/orphan-sweep-check.Ab12Cd)
   PackSweep.cfg               PASS (3950676 distinct states)
   NoTiming.cfg                PASS (6636617 distinct states)
   Liveness.cfg                PASS (1138212 distinct states)
   PurgeControl.cfg            PASS (29362 distinct states)
   PurgeSafety.cfg             PASS (2019265 distinct states)
   PurgeLiveness.cfg           PASS (305681 distinct states)
   PurgeQuarantine.cfg         PASS (305681 distinct states)
   NoThresholdAssumption.cfg   fails as expected (trace: ...)
   SlowHealEntry.cfg           fails as expected (trace: ...)
   PurgeReach.cfg              fails as expected (trace: ...)
== Coverage (sync commit b5897891...)
   every covered function matches model/fingerprints.txt
RESULT: PASS
```

| Exit | Meaning | What you do |
|---|---|---|
| 0 | PASS | Nothing. |
| 1 | FAIL: a property broke | [Read the failure](#read-a-failure). |
| 2 | STALE: covered code changed since the sync commit, a fact is FALSE, or a probe stopped failing. In a full run, the properties hold on the model. With `--fingerprints-only`, TLC did not run, so the result says nothing about the properties | [Bring the model in line](#bring-the-model-in-line). |
| 3 | The check could not run | Fix the tool problem it prints (Go, Java, download, a TLA+ parse error, or a covered function that facts.go cannot find). |

## How the check follows the code

The models (`model/PackSweep.tla` and `model/RepoPurge.tla`) are hand-written descriptions of the code. Two things tie them to the code:

1. **Code facts.** `facts/facts.go` parses the Go source and checks 22 guards by call order or presence. check.sh writes each one, as a constant, into the TLC configs of each module that declares it (the lists `facts_PackSweep` and `facts_RepoPurge` in check.sh). When you remove or reorder a guard, its fact turns FALSE and the model takes the broken branch, so TLC finds the bug. The PackSweep facts:

   | Fact | Guard in the code | Property that fails without it |
   |---|---|---|
   | `WriterVerifies` | `preReceive` (hook.go) and `publishPack` (publish.go) call `blob.EnsurePublished` after `RegisterPacks`; `Repacker.repack` calls `verifyPublished` after `ReplacePacks` on its won, ambiguous and lost-but-registered paths; both re-upload on `ErrNotFound` | `NoWaitForHeal` |
   | `VerifyOnRegisterError` | nothing between `RegisterPacks` and `EnsurePublished` can skip the verify in either writer: no `return`, `break`, `continue` or `goto`, and no `if` on `regErr` around the call | `NoWaitForHeal` |
   | `CopyBeforeDelete` | `safeDelete` calls `s.blobs.Copy(ctx, qKey, key)` before every `s.blobs.Delete` | all three |
   | `RecheckAfterDelete` | `safeDelete` calls `s.registered` after the delete, then `restoreFromQuarantine` | `NoWaitForHeal` |
   | `HealEverySweep` | `sweep` calls `healQuarantine` before it lists `packs/` | `BytesComeBack` |
   | `HealRestores` | `healQuarantineEntry` calls `restoreFromQuarantine` before its purge | `NoLostPack`, `BytesComeBack` |
   | `PurgeAgeGated` | `healQuarantineEntry` returns early when the backup is younger than `s.quarantinePurgeAge()` | all three |
   | `PurgeFloorCoversHandshake` | `QuarantinePurgeFloor = N * sweepRecoveryTimeout` with N >= 2, and `safeDelete` bounds two phases with `context.WithTimeout(..., sweepRecoveryTimeout)` | all three |
   | `PurgeAgeCoversThreshold` | `quarantinePurgeAge` returns `max(s.cfg.Threshold, QuarantinePurgeFloor)` | `NoLostPack`, `BytesComeBack` |

   `RepoPurge` also reads `WriterVerifies`, `VerifyOnRegisterError`, `CopyBeforeDelete`, `RecheckAfterDelete`, `HealRestores`, `PurgeAgeGated` and `PurgeFloorCoversHandshake`, which shape its writers, `safeDelete` and the heal; the table lists the PackSweep property each one breaks. The purge facts:

   | Fact | Guard in the code | Property that fails without it |
   |---|---|---|
   | `PurgeGuarded` | `PurgeExpired` and `PurgeRepo` return when `p.sweeper.cfg.DeleteGuard` fails, before `PurgeableRepos` and `purgeRepo` | `GuardRespected` |
   | `SweepGuardStopsPurgeMode` | `sweep` sets `purgeMode` false when `DeleteGuard` fails, before `healQuarantine` and `classify` | `GuardRespected` |
   | `RefusesLive` | `purgeRepo` returns on `state.Repo.DeletedAt == nil` before `FenceRepoPurge` | `PurgeEligible` |
   | `RefusesYoung` | `purgeRepo` returns when the deletion is younger than `MinRepoPurgeAge`, before `FenceRepoPurge` | `PurgeEligible` |
   | `CutoffCoversRetention` | `PurgeExpired` lists up to `cutoff := p.now().Add(-max(p.retention, MinRepoPurgeAge))` | `PurgeEligible` |
   | `CursorWraps` | `PurgeExpired` resets `p.cursor` and lists again when the batch is empty and the cursor is not | `PurgeCompletes` |
   | `FenceFirst` | `purgeRepo` calls `FenceRepoPurge` before `DropRepoRefs`, `RetireRepoPacks` and `reapPrefix`, and returns when it fails | `NoDanglingRow` |
   | `RefsDieFirst` | `purgeRepo` calls `DropRepoRefs` before `RetireRepoPacks` and `reapPrefix`, and returns when it fails | `NoDanglingRow` |
   | `RegistrationsFenced` | `RegisterPacks` and the LFS registration read the fence in their own transaction, before every write, and return `ErrRepoPurged`: PG (`lockBaseLedger(ctx, tx, baseRepoOf(repo))`, then `repoPurgeFencedPG` on `tx`; the LFS registration locks `repo`, see [Scope](#repopurge); `FenceRepoPurge` takes the same lock), SQLite (`repoPurgeFencedSQLite` on `tx`, in a transaction of a database opened with `_txlock=immediate`), S3WAL (`m.PurgedAt != nil` first in the mutation of the base manifest, where `FenceRepoPurge` sets it) | `StampedClean`, `NoDanglingRow` |
   | `RefWritesFenced` | `casRef` reads the fence the same way on PG, SQLite and S3WAL | `NoDanglingRow` |
   | `FoldsFenced` | `ReplacePacks` reads the fence in its own transaction and returns `ErrRepoPurged`: PG (`lockBaseLedger(ctx, tx, baseRepoOf(repo))`, then `repoPurgeFencedPG` on `tx` in its `pgx.BeginFunc` closure), SQLite (`repoPurgeFencedSQLite` on `tx` after `BeginTx`, before `tx.Commit`), S3WAL (`m.PurgedAt != nil` in the mutation of the base manifest, before the mutation writes it). The old-row delete may come first: the `ErrRepoPurged` return rolls it back | `NoDanglingRow` |
   | `SweepReapsFenced` | `classify` sets `canDelete` from `PurgedRepoProbe` under `purgeMode`, before `safeDelete` | `PurgeCompletes` |
   | `HealPurgesFenced` | `sweep` passes `purgeMode` to `healQuarantine` as `purgeFenced`, and `healQuarantineEntry`'s gate lets `purgeFenced` alone through | `BackupsCollected` |

   The "property that fails" columns are measured: each guard was broken once in the real code, and check.sh failed with that property (see [Validation](#validation)). check.sh names the first violation TLC finds. Where a row names two properties, the second also fails when the config does not check the first. The two order facts (`FenceFirst`, `RefsDieFirst`) move their phase after the reap when FALSE, the latest place before `CountRepoRows`. A phase whose error the run ignores has not happened before the later phases either, so the facts also need the error to end the run.

   `HealPurgesFenced` does not read the `PurgedRepoProbe` block in `healQuarantineEntry` that keeps the backups of repositories without a fence when every delete flag is off. With every fact TRUE, each backup in the model comes from a `safeDelete` after the fence, so no RepoPurge property depends on that block, and a fact would send the model down its "never purge" branch for a change that purges more. `TestSweep_ListOnlyRollbackPreservesOrphanBackups` (`purge_test.go`) fails without the block, and the `healQuarantineEntry` fingerprint turns STALE.

   Four purge guards have no fact, because no property depends on them: `CountRepoRows`' deferral and `reapPrefix`'s spare (with every fact TRUE, no row of the group comes back after `RetireRepoPacks`), and two that [Validation](#validation) broke in the model without a failure, the order of `RetireRepoPacks` before the reap and `ReplacePacks`' rule that it commits only over all of its old rows. The model keeps all four as the code has them, and the fingerprints watch them.

2. **Fingerprints.** facts.go hashes the tokens of every covered function (comments and layout do not count). check.sh compares the hashes with `model/fingerprints.txt`, recorded at the sync commit. A change that the facts do not see, such as a new branch or a new call, still shows as STALE. A STALE result is never a pass.

Each TLA+ action names the Go call it stands for:

| Model action | Code |
|---|---|
| `WriterStat`, `WriterUpload`, `WriterRegister`, `WriterVerify` | `preReceive` publication block (`bl.Stat`, `st.HasPack`, `bl.UploadFile`, `st.RegisterPacks`, `blob.EnsurePublished`), `server.publishPack` (same calls on `s.blobStore`/`s.refStore`), and `Repacker.repack` for its consolidated pack (`r.blobs.UploadFile`, `r.store.ReplacePacks`, `verifyPublished`) |
| `WriterCrash` | the hook process dies, or any writer store call returns an error |
| `SweepStart`, `HealNext` | `Sweeper.sweep` calling `healQuarantine` (`ListPages(quarantinePrefix)`) |
| `HealReg`, `HealStat`, `HealPurge` | `healQuarantineEntry`: `s.registered`, `s.blobs.Stat` plus `restoreFromQuarantine`, the age gate plus `s.blobs.Delete(ctx, q.Key)` |
| `RevalidateReg` | `classify` and `revalidateOrphan` registry reads (merged) |
| `QuarantineCopy`, `Delete`, `Recheck`, `Restore` | `safeDelete`: `s.blobs.Copy`, `s.blobs.Delete(hctx, key)`, `s.registered(rctx, ...)`, `restoreFromQuarantine` |
| `SweeperCrash` | the sweeper dies, or Copy, the recheck or a restore returns an error |
| `Fold` | `Repacker.repack`: `ReplacePacks` drops the row of a folded pack K |
| `AgeBackup` | time passes `quarantinePurgeAge` for one backup |

The purge model's actions:

| Model action | Code |
|---|---|
| `PurgerStart` | `PurgeExpired`: `DeleteGuard`, then `p.meta.PurgeableRepos(ctx, cutoff, p.cursor, ...)` and the relist from the start; `PurgeRepo`: `DeleteGuard`, then `purgeRepo`'s `RepoPurgeState` and eligibility returns (merged: the guard reads nothing the model tracks) |
| `PurgerState` | `purgeRepo` for a listed candidate: `p.meta.RepoPurgeState`, the `PurgedAt` branch to `rescanPurged`, the `DeletedAt == nil` and `MinRepoPurgeAge` returns |
| `PurgerFence`, `PurgerRefs`, `PurgerRetire` | `p.registry.FenceRepoPurge`, `DropRepoRefs`, `RetireRepoPacks` |
| `PurgerReap` | `reapPrefix` (also from `rescanPurged`): `ListPages` over the prefix and `p.sweeper.registered`, sparing a registered key (merged) |
| `QuarantineCopy`, `Delete`, `Recheck`, `Restore` | `safeDelete`, called by `reapPrefix` and by `classify` |
| `PurgerCount` | `p.registry.CountRepoRows`, and the `ErrPurgeDeferred` return on rows or spares |
| `PurgerStamp` | `p.registry.PurgeRepoLedger`, then `p.meta.PurgeRepoMeta` (merged) |
| `PurgerFail` | the purger dies, or a call returns an error that ends the run |
| `SweepStart` | `Sweeper.sweep`: the `DeleteGuard` consult that turns `purgeMode` off, then `healQuarantine` listing the quarantine |
| `HealNext`, `HealStat`, `HealPurge` | `healQuarantineEntry`: `s.registered`; `s.blobs.Stat` plus `restoreFromQuarantine`; the age gate, `PurgedRepoProbe` and `s.blobs.Delete(ctx, q.Key)` |
| `SweepClassify` | `classify` with its `PurgedRepoProbe` branch, and `revalidateOrphan` |
| `WriterStat`, `WriterUpload`, `WriterVerify` | as in PackSweep |
| `WriterRegister` | `RegisterPacks` for a push or a REST write, or `ReplacePacks` for a repack fold |
| `WriterRefOnly`, `WriterCAS` | `CASRef`: a REST branch or tag creation that registers no pack, or a push's ref update after its registration |
| `WriterCrash`, `SweeperCrash` | as in PackSweep |
| `SoftDelete`, `RepoAges` | `SoftDeleteRepo` stamping `deleted_at`; time passing `MinRepoPurgeAge`, then the retention |

## Read a failure

1. Open the trace that check.sh names, `<out>/<config>.trace.txt` (for example `PackSweep.trace.txt`). It lists each step (the TLA+ action) and the variables that changed. A liveness trace ends with the step that loops back. The `Purge*.cfg` configs run `model/RepoPurge.tla`, the others `model/PackSweep.tla`. For a picture in plain words of a PackSweep trace, if you have the pstack `/statechart` skill, give it `model/PackSweep.tla`, `<out>/<config>.run.cfg` (the config with the facts check.sh used) and `model/view.mjs`. It prints the shortest path to the broken rule as numbered steps, such as "the write runs RegisterPacks", "safeDelete deletes K". The passing configs have more states than its chart budget, so for them it only confirms which rules hold.
2. Look at the code facts first. A FALSE fact names the guard your change removed or moved. That is the likely cause. The model treats a FALSE guard as absent. If your change moved the guard instead of removing it, the trace can show a path that the moved guard still handles.
3. Replay the trace against the code, one step per action, using the action table above. Ask for each step: can this call happen at this point in the real code, with these values?
   - If some step cannot happen in the code, the model is wrong. Fix the model to match the code, say what was wrong, and rerun. For a moved guard, model the guard at its new place (see [Bring the model in line](#bring-the-model-in-line)) and rerun: a trace that then replays is a bug.
   - If every step can happen, it is a bug in the code. Report it with the trace in the code's words and the harm from the table above. Do not weaken a property, add a precondition, or shrink a bound to make the check pass. Do not change product code unless the user asks you to.
4. A cheap way to confirm a bug is a test in `internal/jobs` that drives `Sweeper.Sweep` with the in-memory blob store and a stepped writer (see `TestSweep_DeleteRestoresPackRegisteredMidDelete` in `sweep_test.go` for the pattern). For the purge, `purge_test.go` wraps the purge registry with hooks that run a late writer between phases (`TestPurgeRepo_FenceRejectsLateWriter`).

## Bring the model in line

When check.sh says STALE, or when you change covered code on purpose:

1. List what changed: `git diff <sync commit> -- <files in the coverage record>`.
2. For each change, decide: no effect on what the model describes, the model no longer describes the code (a step split in two, a new branch, a moved check, a guard that became conditional, a new path that writes the same keys or rows), or the promise changed on purpose (cite the doc comment, test, or ADR that says so). The facts check call order, not conditions, so a guard wrapped in a new `if` can keep its fact TRUE: read every changed branch around a guard.
3. Update the model (`model/PackSweep.tla` or `model/RepoPurge.tla`) from the code, not from the result. Keep one indivisible call as one action. If a fact's guard moved or was renamed, update `facts/facts.go` too.
4. Rerun check.sh. If it passes, break the new code once in a scratch edit (remove the new check, or reorder the new step) and confirm that check.sh fails. Then drop the scratch edit.
5. Run `check.sh --record`. It rewrites `model/fingerprints.txt` and the sync commit below. Run it only after steps 1 to 4. It refuses when a property fails, a fact is FALSE, a probe passes, `--only` is set, a covered file has uncommitted changes, or SKILL.md does not have exactly one `Sync commit:` line. It records the merge base with `origin/main` when the covered files match it there, so a squash merge does not orphan the sync commit.

## Scope

### PackSweep

**Verified, within these bounds:** one pack key, two writers, two sweepers, two quarantine backups, at most one sweeper crash, ambiguous deletes in every handshake, and one repack fold, with writer crashes at any step (`PackSweep.cfg`, `NoTiming.cfg`). Liveness uses one writer and two sweepers (`Liveness.cfg`). TLC checks every interleaving of the modeled calls. A pass covers the model at these bounds, not the whole system.

**Assumptions:**

- Store and registry calls are atomic and linearizable: the store has strong read-after-write consistency, as the deployment docs require. A call that returns an error took effect, or did not, before it returned. The registry implementations of `RegisterPacks`, `ReplacePacks` and `HasPack` (PostgreSQL, SQLite, s3wal) are each one atomic step; the model does not check them.
- A quarantine backup's ModTime is close to the end of its copy. On AWS S3 a multipart copy (packs over 5 GiB) gets the start time of the upload, and the copy runs on the sweep's context. A copy that takes longer than the purge floor's spare time (about 30 min) can let a backup reach purge age while its handshake still runs. This was not tested against S3.
- Pack keys are content-addressed, so every writer and every restore puts identical bytes under a key.
- Live processes keep taking steps, and faults are bounded (for `BytesComeBack`).
- Two timing assumptions that the code does not enforce. `NoLostPack` and `BytesComeBack` need both. `NoWaitForHeal` needs neither.
  - `WritesFinishWithinThreshold`: a write that is in flight when a sweeper copies or deletes a blob finishes before that backup reaches purge age (`max(OPENSTORAGE_SWEEP_AGE_THRESHOLD, 1 h)`). The delete runs at most 15 min after the copy, so this holds when every write finishes within the purge age minus 15 min. `SweepConfig.Threshold` documents a stronger rule: the threshold "must comfortably exceed the longest possible push/import duration".
  - `HealEntryIsQuick`: `healQuarantineEntry` gets from its registry read to its age gate within that same margin. In local mode (`StartSweepTicker`) no deadline bounds the sweep. In PostgreSQL mode the sweep is a River job, and its pass ends after 2 hours (`passJobTimeout` in `internal/jobs/river.go`). That bound keeps a heal entry within the margin only when the purge age is more than 2 h 15 min. The default threshold of 24 h gives a margin of 23 h 45 min. With a threshold of 2 h 15 min or less, the PostgreSQL bound does not enforce the assumption. Between the two steps the entry makes blob calls on the sweep's context, such as `Stat` and a restore copy when the original is missing (read from code, not run).

**Probes:** `NoThresholdAssumption.cfg` and `SlowHealEntry.cfg` each drop one timing assumption, and `NoLostPack` must then fail. Both fail on the same path: a write finds an old orphan blob present and adopts it, a sweeper quarantines and deletes the blob, the write's `RegisterPacks` commits, the write dies before `EnsurePublished`, and a heal that read the registry before the registration purges the aged backup. The registered pack then has no bytes anywhere. If a probe starts passing, the code or the model changed. Review it before you update this file.

**Simplifications:** each one gives the model at least the behaviors of the code, or the same outcome:

- The sweeper ignores blob age and the `packs/` listing. It can pick K as a candidate at any time, even a blob a writer uploaded a moment ago, so the model checks a more aggressive sweeper than the code.
- Merged calls, where the merged action still has every outcome the two calls can have: the classify and revalidate registry reads, the writer's `HasPack` probe with its upload (upload, or skip while K is registered), `EnsurePublished`'s Stat with its re-upload, and the heal's Stat with its restore.
- Error returns that end a process the same way as a crash are modeled as that crash.
- A behavior has at most two sweeper handshakes (one per backup slot).
- A fold may drop K's row only while K's bytes are present. The code checks presence only when the repack downloads K, so the model allows fewer folds. That is deliberate: a fold that drops the row of a missing pack would hide a lost pack from `NoLostPack`.

**Left out (a pass says nothing about these):** LFS keys (`GuardedLFSDelete`, digest checks, adoption claims, legacy keys), quota covers and floor generations, truncated blobs (`healPresentOriginal`'s size checks), purge-aware sweeps (`PurgedRepoProbe`) and repository purges (both in the RepoPurge model below), `DeleteGuard` for the delete modes (RepoPurge checks it for purge-aware mode), list-only mode, readers and hydration (the model checks the registry-to-bytes invariant that hydration depends on), the ref CAS itself, and the s3wal authority store. Keys do not interact in this protocol, so one key stands for all.

### RepoPurge

**Verified, within these bounds:** one repository and one pack key K, two writers (pushes and REST writes, ref-only REST creates, repack folds), the background pass and one `purge-repo` run, one purge-aware sweeper, two quarantine slots that are reused once free, and one fault (a crash, an error return, a failed guard, or an ambiguous delete), with writer crashes at any step (`PurgeSafety.cfg`). Eligibility and the guard run with no writer (`PurgeControl.cfg`): writers reach the purgers' and the sweeper's control flow only through K's row and bytes, and the initial states hold every value of those. Liveness uses one writer and one quarantine slot (`PurgeLiveness.cfg`, `PurgeQuarantine.cfg`). Pack keys do not interact, and one key stands for a base key and a `+namespace` key: `QuotaRepoGroup` and the fence map both to the base repository, and the reap lists both prefixes. The helpers behind that (`baseRepoOf`, `QuotaRepoGroup` with `packNamespaceSuffixes`, and the sweeper's `baseOf`) are fingerprinted.

**Assumptions:**

- PackSweep's store and registry assumptions, also for `FenceRepoPurge`, `DropRepoRefs`, `RetireRepoPacks`, `CountRepoRows`, `PurgeRepoLedger` and `PurgeRepoMeta`: each is one atomic step.
- Three registry promises, each checked by a code fact on PG, SQLite and S3WAL: registrations (`RegistrationsFenced`), ref writes (`RefWritesFenced`) and repack folds (`FoldsFenced`) read the purge fence in their own transaction, ordered against `FenceRepoPurge`. So once the fence committed, nothing of the repository registers again. The facts read the shape of the code. PG's LFS registration (`registerLFSObjectTx`) takes the lock on `repo`, not on `baseRepoOf(repo)`: LFS objects are keyed by the base repository ([ADR 035](../../../docs/adr/035-lfs-objects-are-keyed-by-base-repoid-namespaces-share-one-object-space.md)), and the fact pins that argument. `TestRepoPurgeSeams` in `internal/ref` checks, on all three backends, that each of these writes fails once the fence is in; neither checks that PG's advisory lock and SQLite's write lock order concurrent transactions as documented.
- Three meta promises, fingerprinted but with no fact: `PurgeableRepos` lists only rows with `deleted_at` at or before the cutoff and no `purged_at`; `PurgeRepoMeta` stamps only a soft-deleted row; nothing clears `deleted_at` once `SoftDeleteRepo` set it.
- The purger's clock agrees with the clock that stamped `deleted_at` (PG: the database's `now()`; SQLite: the clock of the process that deleted). `purgeRepo` and `PurgeExpired` compare `p.now()` with it. A purger clock that runs d ahead purges up to d early, which a user sees as `PurgeEligible` breaking by d.
- `PurgeCompletes` and `BackupsCollected` need purge-aware sweeps that keep running (`OPENSTORAGE_SWEEP_INTERVAL` > 0) and a `DeleteGuard` that passes eventually. A writer can upload a blob after the purge's own reap (an upload precedes its fenced registration), and only a sweep, or a `purge-repo` rerun's `rescanPurged`, reaps it; only a sweep purges the quarantine backups.
- No timing assumption. Once the fence committed, nothing of the repository registers again, so the purge, the purge-aware sweeper and the heal can delete any of its blobs and backups whenever they reach them.

**Probes:** no timing probe, because no property needs a timing assumption. `PurgeReach.cfg` is a reachability probe: with `PurgeSafety.cfg`'s bounds it must reach `RaceUnreachable`'s state, where a push registered K and a repack folded into K before the fence, the purge retired both rows, and its reap is deleting K while both writers still hold their local files. If a model edit cuts any of those steps off, the probe passes, the safety configs may check nothing, and check.sh stops with STALE.

**Simplifications:** each one gives the model at least the behaviors of the code, or the same outcome:

- The sweeper runs with every delete flag off, so it reaps only through `PurgedRepoProbe`. It ignores blob age, as in PackSweep. `serve` turns `DeleteSuperseded` on by default (`resolveSweepSettings`), and with any delete flag on a sweep also deletes the unregistered packs of its delete class (with `DeleteSuperseded`, those that have a `pack_retained` row) and purges every aged backup (`purgeAll`). After the fence that adds no step: no registration lands again, so such a delete is the same `safeDelete` of an unregistered K that `SweepClassify` takes, and the purge of an aged backup is the one `HealPurge` takes. Before the fence there is no purge step yet, and those deletes are PackSweep's model.
- Merged calls: the guard consult with the read after it (the guard reads nothing the model tracks); `reapPrefix`'s listing with its registry read (after the purge's own `RetireRepoPacks` no registration can land between them); `PurgeRepoLedger` with `PurgeRepoMeta`; `DropRepoLFSObjects` with `RetireRepoPacks`; the heal's entry pick with its registry read; a probe read with the step that acts on it (a fence never goes away).
- The `purge-repo` run is one run: a second run takes only steps a background pass takes too. The model's CLI calls `PurgeRepo` directly, without the command's own checks.
- The background cursor is one bit, past the repository or not: with one repository the rotation only orders.
- A copy that finds every quarantine slot in use fails, as a `Copy` error does.

**Left out of RepoPurge (a pass says nothing about these):** LFS objects under the purge (`DropRepoLFSObjects`, `GuardedLFSDelete`, digest-checked restores); `RegistrationsFenced` reads the LFS registration's fence check, and `TestPurgeRepo_FenceRejectsLateWriter` rejects a post-purge `RegisterLFSObject`, but a gap in the LFS delete path would show as an LFS object that outlives the purge (a storage leak). Ledger and quota rows (`PurgeRepoLedger`, reservations, usage counters, `ReclaimRetainedPack`), which a user would see as wrong usage numbers. More than one repository and the cursor's rotation across a batch (`TestPurgeExpired_RotatesPastFailingRepos` covers it), where a gap shows as a deleted repository whose purge waits behind failing ones. The `purge-repo` command around `PurgeRepo` (its `--yes` gate, its refusals of a live or unknown repository, and its summary), where a gap shows as a purge that runs without the operator's `--yes`; `TestCmdPurgeRepo` covers the gate and the refusals. The S3WAL internals behind the facts (the manifest mutation, the WAL and its recovery), where a gap shows as a fence, registration or ref write that is not atomic on S3WAL, which breaks `NoDanglingRow` or `StampedClean` there; `TestRepoPurgeSeams` runs the fence seams on all three backends, and `TestS3WALRecoveryPurgeFence` checks them across S3WAL recovery.

## Validation

Every fact was broken in the real code, in a scratch checkout whose covered code equals the sync commit. Each scratch break compiled, turned its fact FALSE (removing the `EnsurePublished` call also turns `VerifyOnRegisterError` FALSE), and made check.sh fail with a property violation, not only STALE. The other properties passed:

| Scratch break | Fact | Failed |
|---|---|---|
| remove the `blob.EnsurePublished` call from `publishPack` | `WriterVerifies` | `NoWaitForHeal` |
| `return regErr` right after `RegisterPacks` in `preReceive` | `VerifyOnRegisterError` | `NoWaitForHeal` |
| remove the quarantine `s.blobs.Copy` in `safeDelete` | `CopyBeforeDelete` | `NoLostPack`, `NoWaitForHeal`, `BytesComeBack` |
| replace the post-delete `s.registered` with `false` | `RecheckAfterDelete` | `NoWaitForHeal` |
| remove the `healQuarantine` call from `sweep` | `HealEverySweep` | `BytesComeBack` |
| remove the restore from `healQuarantineEntry` | `HealRestores` | `NoLostPack`, `BytesComeBack` |
| drop the age test from the purge gate | `PurgeAgeGated` | `NoLostPack`, `NoWaitForHeal`, `BytesComeBack` |
| `QuarantinePurgeFloor = 1 * sweepRecoveryTimeout` | `PurgeFloorCoversHandshake` | `NoLostPack`, `NoWaitForHeal`, `BytesComeBack` |
| `quarantinePurgeAge` returns `QuarantinePurgeFloor` | `PurgeAgeCoversThreshold` | `NoLostPack`, `BytesComeBack` |
| remove the won-path `verifyPublished` after `ReplacePacks` in `Repacker.repack` | `WriterVerifies` | `NoWaitForHeal` |
| `if regErr != nil { break }` at the top of `preReceive`'s `EnsurePublished` loop | `VerifyOnRegisterError` | `NoWaitForHeal` |

The purge facts were broken the same way. The `FoldsFenced` row is this skill's branch before the fence check in `ReplacePacks` merged into it; every other row ran in a scratch checkout with that fence check in place. Each break compiled (`go build ./internal/jobs/ ./internal/ref/`), turned only its own fact FALSE, and made the config that checks the named property fail with a violation (`PurgeControl.cfg` for `PurgeEligible` and `GuardRespected`, `PurgeSafety.cfg` for `NoDanglingRow` and `StampedClean`, `PurgeLiveness.cfg` for `PurgeCompletes`, `PurgeQuarantine.cfg` for `BackupsCollected`). Each break ran only that config:

| Scratch break | Fact | Failed |
|---|---|---|
| remove the fence check from `ReplacePacks` on PG, SQLite and S3WAL (the code before the fix) | `FoldsFenced` | `NoDanglingRow` |
| `age < 0` in place of `age < MinRepoPurgeAge` in `purgeRepo` | `RefusesYoung` | `PurgeEligible` |
| `false &&` in front of `purgeRepo`'s `state.Repo.DeletedAt == nil` | `RefusesLive` | `PurgeEligible` |
| `cutoff := p.now().Add(-MinRepoPurgeAge)` in `PurgeExpired` | `CutoffCoversRetention` | `PurgeEligible` |
| remove the `DeleteGuard` block from `PurgeRepo` | `PurgeGuarded` | `GuardRespected` |
| leave `purgeMode` on in `sweep`'s guard-error branch | `SweepGuardStopsPurgeMode` | `GuardRespected` |
| move `FenceRepoPurge` after the reap loop in `purgeRepo` | `FenceFirst` | `NoDanglingRow` |
| move `DropRepoRefs` after the reap loop in `purgeRepo` | `RefsDieFirst` | `NoDanglingRow` |
| remove the fence check from PG `RegisterPacks` | `RegistrationsFenced` | `StampedClean` |
| remove the fence check from SQLite `casRef` | `RefWritesFenced` | `NoDanglingRow` |
| remove the empty-batch relist from `PurgeExpired` | `CursorWraps` | `PurgeCompletes` |
| remove `classify`'s `PurgedRepoProbe` branch | `SweepReapsFenced` | `PurgeCompletes` |
| pass `false` as `healQuarantine`'s `purgeFenced` in `sweep` | `HealPurgesFenced` | `BackupsCollected` |

The `FoldsFenced` trace (17 steps) is the race the fence check closes. A repack has uploaded its consolidated pack K for a soft-deleted repository that still has other pack rows. `purge-repo` passes its checks and runs `FenceRepoPurge`. A purge-aware sweep classifies K (unregistered, repository fenced), and `safeDelete` copies K to quarantine and deletes it; the sweeper dies before its recheck. The next sweep's heal reads K as unregistered and goes to its age gate. The repack's `ReplacePacks` commits: it read no fence, and its old rows still exist. The repack dies before `verifyPublished`. The backup passes `quarantinePurgeAge`, and the heal purges it, because the repository is fenced. K has a pack row and no bytes.

Two guards were broken in the model alone, and no property failed, so they have no fact. Each run used `PurgeSafety.cfg` and `PurgeLiveness.cfg`. Moving `RetireRepoPacks` after the reap loop passed both (2788323 and 449627 distinct states): the reap spares the still-registered K, the purge defers once, and the next pass completes. Letting `ReplacePacks` commit without its old rows passed both (2040748 and 304745 distinct states): a fold after `FenceRepoPurge` fails on the fence anyway, and `RetireRepoPacks` tombstones one from before it.

These breaks cover the stricter fact checks, the base-key fingerprints, the reachability probe, and the eligibility and guard invariants in `PurgeSafety.cfg`. Each ran in a scratch checkout with every fact TRUE. Each Go break compiled (`go build ./internal/jobs/ ./internal/ref/ ./internal/blob/`). A break with a config ran `check.sh --only` on it, and a fingerprint break ran `--fingerprints-only` against a `model/fingerprints.txt` that held the hashes of the unbroken code:

| Scratch break | Fact or check | Failed |
|---|---|---|
| `lockBaseLedger(ctx, tx, repo)` in place of `baseRepoOf(repo)` in PG `RegisterPacks` | `RegistrationsFenced` | `StampedClean` (`PurgeSafety.cfg`) |
| the same, with `StampedClean` dropped from `PurgeSafety.cfg` | `RegistrationsFenced` | `NoDanglingRow` |
| PG `casRef` reads the fence on a second transaction (`s.pool.Begin`), not on `tx` | `RefWritesFenced` | `NoDanglingRow` |
| `lockBaseLedger(ctx, tx, repo)` in PG `ReplacePacks` | `FoldsFenced` | `NoDanglingRow` |
| `purgeRepo` goes on after a `FenceRepoPurge` error (`_ = err` in place of the return) | `FenceFirst` | `NoDanglingRow` |
| `purgeRepo` goes on after a `DropRepoRefs` error (`err = nil` in place of the return) | `RefsDieFirst` | `NoDanglingRow` |
| `age < 0` in place of `age < MinRepoPurgeAge` in `purgeRepo` | `RefusesYoung` | `PurgeEligible` in `PurgeSafety.cfg` |
| remove the `DeleteGuard` block from `PurgeRepo` | `PurgeGuarded` | `GuardRespected` in `PurgeSafety.cfg` |
| `WriterRegister` without `RegisterPacks`, in the model | `PurgeReach.cfg` | the probe passes, and check.sh exits 2 |
| `strings.Cut(repo, "/")` in `baseRepoOf` | fingerprint | STALE on `ledger.go:baseRepoOf` alone |
| `QuotaRepoGroup` returns the base repository alone | fingerprint | STALE on `blob.go:QuotaRepoGroup` alone |
| `packNamespaceSuffixes` without `"+import"` | fingerprint | STALE on `blob.go:var packNamespaceSuffixes` alone |
| `strings.Cut(repo, "/")` in `baseOf` | fingerprint | STALE on `sweep.go:baseOf` alone |
| `if false` in place of the `if !purgeAll` around `healQuarantineEntry`'s `PurgedRepoProbe` read | none: every fact stays TRUE | `TestSweep_ListOnlyRollbackPreservesOrphanBackups` ("list-only rollback destroyed an ordinary orphan's backup"), and the `healQuarantineEntry` fingerprint changes |

A worked example of a moved guard: moving `safeDelete`'s `s.registered(rctx, repo, key)` above the delete turns `RecheckAfterDelete` FALSE, and `NoTiming.cfg` fails. Its first trace (the write registers before the sweeper's copy) does not replay, because the moved read still sees that registration. With the read modeled before the delete, TLC finds the real path: the read sees "unregistered", the write registers and verifies, and then the delete removes the bytes of a committed pack.

## Coverage record

Sync commit: `b5897891d6f0eb0e92f6e3bdda0af303ee24aed0` (the last commit at which someone confirmed that the model describes this code; only `check.sh --record` moves it).

| File | Covered code |
|---|---|
| `internal/jobs/sweep.go` | `Sweeper.sweep`, `classify`, `revalidateOrphan`, `freshModTime`, `registered`, `safeDelete`, `restoreFromQuarantine`, `healQuarantine`, `healQuarantineEntry`, `healPresentOriginal`, `quarantinePurgeAge`, `quarantineKey`, `const QuarantinePurgeFloor`, `const sweepRecoveryTimeout`, `SweepSchedule.runOnce`, `baseOf` |
| `internal/blob/blob.go` | `EnsurePublished`, `QuotaRepoGroup`, `var packNamespaceSuffixes` |
| `internal/githttp/hook.go` | `preReceive`, from `published := ...` through the `EnsurePublished` loop |
| `internal/api/publish.go` | `server.publishPack` |
| `internal/jobs/repack.go` | `Repacker.repack` (its consolidated-pack publication and the `ReplacePacks` fold), `verifyPublished` |
| `internal/jobs/purge.go` | `RepoPurger.PurgeExpired`, `PurgeRepo`, `purgeRepo`, `rescanPurged`, `reapPrefix`, `const MinRepoPurgeAge` |
| `internal/ref/ref.go`, `ledger.go`, `purge.go` | `PG.casRef`, `PG.RegisterPacks`, `PG.ReplacePacks`, `validateReplacePacks`, `PG.registerLFSObjectTx`, `lockBaseLedger`, `baseRepoOf`, `PG.FenceRepoPurge`, `PG.IsRepoPurged`, `PG.DropRepoRefs`, `PG.RetireRepoPacks`, `PG.CountRepoRows`, `repoPurgeFencedPG` |
| `internal/ref/sqlite.go`, `ledger_sqlite.go`, `purge_sqlite.go` | `SQLite.casRef`, `SQLite.RegisterPacks`, `SQLite.ReplacePacks`, `SQLite.registerLFSObjectTx`, `SQLite.FenceRepoPurge`, `SQLite.IsRepoPurged`, `SQLite.DropRepoRefs`, `SQLite.RetireRepoPacks`, `SQLite.CountRepoRows`, `repoPurgeFencedSQLite` |
| `internal/ref/s3walstore.go`, `s3walstore_lfs.go`, `s3walstore_purge.go` | `S3WAL.casRef`, `S3WAL.RegisterPacks`, `S3WAL.ReplacePacks`, `S3WAL.registerLFSObject`, `S3WAL.FenceRepoPurge`, `S3WAL.IsRepoPurged`, `S3WAL.DropRepoRefs`, `S3WAL.RetireRepoPacks`, `S3WAL.CountRepoRows` |
| `internal/meta/purge.go`, `purge_sqlite.go`, `s3walmeta_purge.go` | `PurgeableRepos`, `RepoPurgeState`, `PurgeRepoMeta` of PG, SQLite and S3WAL |

`model/fingerprints.txt` holds one hash per entry.

## Files

| File | Purpose |
|---|---|
| `check.sh` | The one command: facts, TLC runs, fingerprints, `--record`, and the TLC-free `--fingerprints-only` that CI runs. Its `configs` table maps each config to its module. |
| `facts/facts.go` | Reads the code facts and fingerprints from the Go source (standard library only; the dot directory keeps it out of `./...`). |
| `model/PackSweep.tla` | The sweeper model. |
| `model/PackSweep.cfg`, `NoTiming.cfg`, `Liveness.cfg` | Its property configs; each must pass. |
| `model/NoThresholdAssumption.cfg`, `SlowHealEntry.cfg` | Its probes; each must fail `NoLostPack`. |
| `model/RepoPurge.tla` | The purge model. |
| `model/PurgeControl.cfg`, `PurgeSafety.cfg`, `PurgeLiveness.cfg`, `PurgeQuarantine.cfg` | Its property configs; each must pass. |
| `model/PurgeReach.cfg` | Its reachability probe; it must fail `RaceUnreachable`. |
| `model/view.mjs` | State and step labels for the pstack `/statechart` skill (optional; not part of this repo). |
| `model/fingerprints.txt` | Fingerprints at the sync commit. |

To bring this verification back in line after changes that skipped this skill, use the pstack `/maintain-formal-verification-skill` skill if you have it, or follow [Bring the model in line](#bring-the-model-in-line).
