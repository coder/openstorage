---
name: verify-orphan-sweep
description: Model-check the orphan-pack sweeper (internal/jobs/sweep.go) against the pack writers (git pre-receive, REST publishPack, blob.EnsurePublished, and repack publishing its consolidated pack) with TLA+ and TLC, so no interleaving of deletes, quarantine restores, heals and registrations loses a registered pack's bytes. Use before you merge any change to the sweeper's delete, quarantine or heal code, EnsurePublished, a RegisterPacks or ReplacePacks call site, repack's verifyPublished, quarantinePurgeAge or QuarantinePurgeFloor; and whenever check.sh says STALE.
---

# Verify the orphan-pack sweeper

The orphan sweeper deletes unregistered pack bytes. It races every pack writer: a git push's pre-receive hook, every REST write (`publishPack`), a repack publishing its consolidated pack, and other replicas' sweeps. Its safety rests on an ordering argument written in comments (`Sweeper.sweep`, `blob.EnsurePublished`). This skill checks that argument with a model checker over every interleaving of the individual store and registry calls, up to the bounds below.

## What it checks

| Property | Rule, in one sentence | Harm if it breaks |
|---|---|---|
| `NoLostPack` | A registered pack never loses its last copy (its blob, a quarantine backup, or an in-flight writer's local pack file). | Every cold hydrate of the repository fails with `githttp.ErrPackMissing` ("corruption grade") until someone re-pushes the same pack. |
| `NoWaitForHeal` | While nothing crashes, a registered pack is missing only while the write or the sweeper handshake that puts it back is still running. | Clones and reads fail for up to one sweep interval (`OPENSTORAGE_SWEEP_INTERVAL`, default 1 h) after a race the code claims to repair on the spot. |
| `BytesComeBack` | A registered pack with missing bytes gets them back. | Same as `NoLostPack`, through a repair that never happens. |

`NoWaitForHeal` is the claim in the `Sweeper.sweep` doc comment: "the recheck and the writer verify jointly cover every interleaving with no timing assumptions". `NoTiming.cfg` checks it with the timing assumption off and `HealEntryBounded` pinned FALSE.

## When to run it

Run it when your change touches any code in the [coverage record](#coverage-record), or code that writes `packs/` blobs, pack registry rows, or `sweep/quarantine/` keys. Also run it when `check.sh` prints STALE.

## Run

From the repository root:

```sh
OUT=$(mktemp -d "${TMPDIR:-/tmp}/orphan-sweep-check.XXXXXX")
.agents/skills/verify-orphan-sweep/check.sh --out "$OUT"
```

- Needs Go (the `go.mod` version), Java 11 or newer, and `curl`. The first run downloads `tla2tools.jar` 1.7.4 (sha256-pinned) into `${XDG_CACHE_HOME:-$HOME/.cache}/openstorage-tla`. Set `JAVA`, `GO`, or `OPENSTORAGE_TLA_CACHE` to override.
- It takes about 3 minutes with the default 4 TLC workers (`TLC_WORKERS`) and a 4 GB heap (`TLC_HEAP`).
- `--out` keeps every TLC output, the generated configs, and trace summaries. It must be a new or empty directory. Without it, check.sh makes a temporary directory and prints its path. With `--fingerprints-only` and no `--out`, check.sh deletes that directory on exit.
- `--only <cfg>` runs one config, for example `--only NoTiming.cfg`. It is for quick iteration. Finish with a full run.
- `--fingerprints-only` checks the code facts and the fingerprints without TLC or Java, in about a second. CI runs it in the `orphan-sweep-fingerprints` job of `.github/workflows/ci.yml`, so a change to covered code fails CI until someone brings the model in line and runs `check.sh --record`. CI does not run TLC: a pass says that the covered code is the code the model was checked against, not that the properties hold.

A clean run ends like this:

```text
== Code facts (HEAD 5620494c)
   WriterVerifies             TRUE
   ... (10 facts, all TRUE)
== TLC 1.7.4 (outputs in /tmp/orphan-sweep-check.Ab12Cd)
   PackSweep.cfg               PASS (3950676 distinct states)
   NoTiming.cfg                PASS (6636617 distinct states)
   Liveness.cfg                PASS (1138212 distinct states)
   NoThresholdAssumption.cfg   fails as expected (trace: ...)
   SlowHealEntry.cfg           fails as expected (trace: ...)
== Coverage (sync commit 5620494c...)
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

The model (`model/PackSweep.tla`) is a hand-written description of the code. Two things tie it to the code:

1. **Code facts.** `facts/facts.go` parses the Go source and checks 10 guards by call order. check.sh writes each one into the TLC config as a constant. When you remove or reorder a guard, its fact turns FALSE and the model takes the broken branch, so TLC finds the bug. The facts:

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
   | `HealEntryBounded` | `healQuarantineEntry` starts with `ectx, cancel := context.WithTimeout(ctx, healEntryTimeout)` and reads the registry under `ectx`. A registered key restores or heals its original on `ctx`, then opens `bound, boundCancel := context.WithTimeout(ctx, healEntryTimeout)` and stats the original under it. No other call passes bare `ctx`, `context.Background()` or `context.WithoutCancel`, and the purge checks `ectx.Err()` before its delete. `healEntryTimeout` plus 2 x `sweepRecoveryTimeout` fits in `QuarantinePurgeFloor` | `NoLostPack`, `BytesComeBack` |
   | `PurgeAgeCoversThreshold` | `quarantinePurgeAge` returns `max(s.cfg.Threshold, QuarantinePurgeFloor)` | `NoLostPack`, `BytesComeBack` |

   The "property that fails" column is measured: each guard was broken once in the real code, and check.sh failed with that property (see [Validation](#validation)).

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

## Read a failure

1. Open the trace that check.sh names, `<out>/<config>.trace.txt` (for example `PackSweep.trace.txt`). It lists each step (the TLA+ action) and the variables that changed. A liveness trace ends with the step that loops back. For a picture in plain words, if you have the pstack `/statechart` skill, give it `model/PackSweep.tla`, `<out>/<config>.run.cfg` (the config with the facts check.sh used) and `model/view.mjs`. It prints the shortest path to the broken rule as numbered steps, such as "the write runs RegisterPacks", "safeDelete deletes K". The passing configs have more states than its chart budget, so for them it only confirms which rules hold.
2. Look at the code facts first. A FALSE fact names the guard your change removed or moved. That is the likely cause. The model treats a FALSE guard as absent. If your change moved the guard instead of removing it, the trace can show a path that the moved guard still handles.
3. Replay the trace against the code, one step per action, using the action table above. Ask for each step: can this call happen at this point in the real code, with these values?
   - If some step cannot happen in the code, the model is wrong. Fix the model to match the code, say what was wrong, and rerun. For a moved guard, model the guard at its new place (see [Bring the model in line](#bring-the-model-in-line)) and rerun: a trace that then replays is a bug.
   - If every step can happen, it is a bug in the code. Report it with the trace in the code's words and the harm from the table above. Do not weaken a property, add a precondition, or shrink a bound to make the check pass. Do not change product code unless the user asks you to.
4. A cheap way to confirm a bug is a test in `internal/jobs` that drives `Sweeper.Sweep` with the in-memory blob store and a stepped writer (see `TestSweep_DeleteRestoresPackRegisteredMidDelete` in `sweep_test.go` for the pattern).

## Bring the model in line

When check.sh says STALE, or when you change covered code on purpose:

1. List what changed: `git diff <sync commit> -- <files in the coverage record>`.
2. For each change, decide: no effect on what the model describes, the model no longer describes the code (a step split in two, a new branch, a moved check, a guard that became conditional, a new path that writes the same keys or rows), or the promise changed on purpose (cite the doc comment, test, or ADR that says so). The facts check call order, not conditions, so a guard wrapped in a new `if` can keep its fact TRUE: read every changed branch around a guard.
3. Update `model/PackSweep.tla` from the code, not from the result. Keep one indivisible call as one action. If a fact's guard moved or was renamed, update `facts/facts.go` too.
4. Rerun check.sh. If it passes, break the new code once in a scratch edit (remove the new check, or reorder the new step) and confirm that check.sh fails. Then drop the scratch edit.
5. Run `check.sh --record`. It rewrites `model/fingerprints.txt` and the sync commit below. Run it only after steps 1 to 4. It refuses when a property fails, a fact is FALSE, a probe passes, `--only` is set, a covered file has uncommitted changes, or SKILL.md does not have exactly one `Sync commit:` line. It records the merge base with `origin/main` when the covered files match it there, so a squash merge does not orphan the sync commit.

## Scope

**Verified, within these bounds:** one pack key, two writers, two sweepers, two quarantine backups, at most one sweeper crash, ambiguous deletes in every handshake, and one repack fold, with writer crashes at any step (`PackSweep.cfg`, `NoTiming.cfg`). Liveness uses one writer and two sweepers (`Liveness.cfg`). TLC checks every interleaving of the modeled calls. A pass covers the model at these bounds, not the whole system.

**Assumptions:**

- Store and registry calls are atomic and linearizable: the store has strong read-after-write consistency, as the deployment docs require. A call that returns an error took effect, or did not, before it returned. The registry implementations of `RegisterPacks`, `ReplacePacks` and `HasPack` (PostgreSQL, SQLite, s3wal) are each one atomic step; the model does not check them.
- A quarantine backup's ModTime is close to the end of its copy. On AWS S3 a multipart copy (packs over 5 GiB) gets the start time of the upload, and the copy runs on the sweep's context. A copy that takes longer than the purge floor's spare time (about 15 min) can let a backup reach purge age while its handshake still runs. This was not tested against S3.
- Pack keys are content-addressed, so every writer and every restore puts identical bytes under a key.
- Live processes keep taking steps, and faults are bounded (for `BytesComeBack`).
- One timing assumption that the code does not enforce: `WritesFinishWithinThreshold`. `NoLostPack` and `BytesComeBack` need it. `NoWaitForHeal` does not. A write that is in flight when a sweeper copies or deletes a blob finishes before that backup reaches purge age (`max(OPENSTORAGE_SWEEP_AGE_THRESHOLD, 1 h)`). The delete runs at most 15 min after the copy, and a heal entry runs at most `healEntryTimeout` (15 min) from its last read to its age gate, so this holds when every write finishes within the purge age minus 30 min. `SweepConfig.Threshold` documents a stronger rule: the threshold "must comfortably exceed the longest possible push/import duration".
- The heal entry bound is a code fact, not an assumption: `HealEntryBounded`. An unregistered key's entry, in PostgreSQL mode and in local mode, runs from its registry read to its purge under its own `healEntryTimeout`, which is one restore copy and does not depend on the sweep's context or on `passJobTimeout`. A registered key restores or heals its original first, with no bound: that copies and hashes the whole backup, which can outlast any fixed bound for a large LFS object. It then opens a new bound and stats the original again, and the purge relies on that read. The purge checks `ectx.Err()` first, because a store that ignores its context, such as the file store, returns from a stalled read late and without an error. A stat that finds the original missing again spares the backup, except for an LFS backup that failed its digest: that one can never be restored, so its purge needs no read. An entry that hits a bound fails with a deadline error and the next sweep visits it again. The model keeps one step for the registered key's restore and second stat (`heal_stat`), then the bounded visit up to the age gate (`heal_purge`). A stall inside `heal_stat` cannot hurt, because the second stat reads the original after it. A stall after the stat can, and the bound removes it.

**Probes:** `NoThresholdAssumption.cfg` drops the timing assumption. `SlowHealEntry.cfg` pins the `HealEntryBounded` fact to FALSE, as if `healQuarantineEntry` had no deadline. Each configuration drops one timing guarantee, and `NoLostPack` must then fail. Both fail on the same path: a write finds an old orphan blob present and adopts it, a sweeper quarantines and deletes the blob, the write's `RegisterPacks` commits, the write dies before `EnsurePublished`, and a heal that read the registry before the registration purges the aged backup. The registered pack then has no bytes anywhere. If a probe starts passing, the code or the model changed. Review it before you update this file.

**Simplifications:** each one gives the model at least the behaviors of the code, or the same outcome:

- The sweeper ignores blob age and the `packs/` listing. It can pick K as a candidate at any time, even a blob a writer uploaded a moment ago, so the model checks a more aggressive sweeper than the code.
- Merged calls, where the merged action still has every outcome the two calls can have: the classify and revalidate registry reads, the writer's `HasPack` probe with its upload (upload, or skip while K is registered), `EnsurePublished`'s Stat with its re-upload, and the heal's Stat with its restore.
- Error returns that end a process the same way as a crash are modeled as that crash.
- A behavior has at most two sweeper handshakes (one per backup slot).
- A fold may drop K's row only while K's bytes are present. The code checks presence only when the repack downloads K, so the model allows fewer folds. That is deliberate: a fold that drops the row of a missing pack would hide a lost pack from `NoLostPack`.

**Left out (a pass says nothing about these):** `RepoPurger` and the `purge-repo` CLI (`internal/jobs/purge.go`), which retire the pack rows of a retention-expired repository (`RetireRepoPacks`) and delete its blobs through `safeDelete` without `revalidateOrphan`; LFS keys (`GuardedLFSDelete`, digest checks, adoption claims, legacy keys), quota covers and floor generations, truncated blobs (`healPresentOriginal`'s size checks), purge-fenced repositories (`PurgedRepoProbe`), `DeleteGuard`, list-only mode, readers and hydration (the model checks the registry-to-bytes invariant that hydration depends on), the ref CAS itself, and the s3wal authority store. Keys do not interact in this protocol, so one key stands for all.

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
| remove `bound, boundCancel := context.WithTimeout(ctx, healEntryTimeout)` from the registered block of `healQuarantineEntry` (stat the original on `ctx` and leave the later calls on the first `ectx`) | `HealEntryBounded` | `NoLostPack`, `BytesComeBack` |
| remove the won-path `verifyPublished` after `ReplacePacks` in `Repacker.repack` | `WriterVerifies` | `NoWaitForHeal` |
| `if regErr != nil { break }` at the top of `preReceive`'s `EnsurePublished` loop | `VerifyOnRegisterError` | `NoWaitForHeal` |

A worked example of a moved guard: moving `safeDelete`'s `s.registered(rctx, repo, key)` above the delete turns `RecheckAfterDelete` FALSE, and `NoTiming.cfg` fails. Its first trace (the write registers before the sweeper's copy) does not replay, because the moved read still sees that registration. With the read modeled before the delete, TLC finds the real path: the read sees "unregistered", the write registers and verifies, and then the delete removes the bytes of a committed pack.

## Coverage record

Sync commit: `4b192fa85e124d918a59f94ae274f06b4099467f` (the last commit at which someone confirmed that the model describes this code; only `check.sh --record` moves it).

| File | Covered code |
|---|---|
| `internal/jobs/sweep.go` | `Sweeper.sweep`, `classify`, `revalidateOrphan`, `freshModTime`, `registered`, `safeDelete`, `restoreFromQuarantine`, `healQuarantine`, `healQuarantineEntry`, `healPresentOriginal`, `quarantinePurgeAge`, `quarantineKey`, `const QuarantinePurgeFloor`, `const sweepRecoveryTimeout`, `const healEntryTimeout` |
| `internal/blob/blob.go` | `EnsurePublished` |
| `internal/githttp/hook.go` | `preReceive`, from `published := ...` through the `EnsurePublished` loop |
| `internal/api/publish.go` | `server.publishPack` |
| `internal/jobs/repack.go` | `Repacker.repack` (its consolidated-pack publication and the `ReplacePacks` fold), `verifyPublished` |

`model/fingerprints.txt` holds one hash per entry.

## Files

| File | Purpose |
|---|---|
| `check.sh` | The one command: facts, TLC runs, fingerprints, `--record`, and the TLC-free `--fingerprints-only` that CI runs. |
| `facts/facts.go` | Reads the code facts and fingerprints from the Go source (standard library only; the dot directory keeps it out of `./...`). |
| `model/PackSweep.tla` | The model. |
| `model/PackSweep.cfg`, `NoTiming.cfg`, `Liveness.cfg` | Property configs; each must pass. A config may pin a fact (`NoTiming.cfg` pins `HealEntryBounded`); check.sh adds the other facts from the code. |
| `model/NoThresholdAssumption.cfg`, `SlowHealEntry.cfg` | Probes; each must fail `NoLostPack`. |
| `model/view.mjs` | State and step labels for the pstack `/statechart` skill (optional; not part of this repo). |
| `model/fingerprints.txt` | Fingerprints at the sync commit. |

To bring this verification back in line after changes that skipped this skill, use the pstack `/maintain-formal-verification-skill` skill if you have it, or follow [Bring the model in line](#bring-the-model-in-line).
