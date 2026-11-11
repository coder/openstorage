---
name: verify-lane-schedule
description: Model-check the local push-lane schedule (ADR-50: meta.LocalScheduleStore on SQLite and S3-WAL, sync.Worker.RunPush, LocalScheduler) with TLA+ and TLC, so no interleaving of git pushes, push-upstream calls, claims, steals, retries, restores, crashes and shutdown loses an accepted push's forward, downgrades a force request, or delays an explicit request. Use before you merge any change to RequestPush, RequestPushNow, ClaimDuePush, FinishPushOutcome, beginPush, RunPush, localPushOutcome, waitPublication, the shutdown restore, events.RefStore (BeforeCAS, HeldBeforeCAS, emitPush), enqueueSyncOrForward, wireHookEventing, restIntentBeforeCAS, or the push-hold code (HoldRef, reservePushHolds, referenceTransactionHook, Hydrate's held-ref skip and its strict sync fetch); and whenever check.sh says STALE.
---

# Verify the local push-lane schedule

On SQLite and S3-WAL, a repository with `forward_on_push` forwards every accepted push to its upstream through one durable lane row (ADR-50, ADR-51). A git push records intent on the row (`RequestPush`), an explicit `POST /repos/push-upstream` records it due now (`RequestPushNow`), a 1 s sweep turns due rows into runs, and each run claims the row, waits for the push's cache ref, takes a loopback snapshot, pushes it upstream, and ends with one holder-guarded write that schedules what follows. Nothing polls the upstream, so a request that the row loses is never forwarded until someone writes again. This skill checks the row protocol with a model checker over every interleaving of those steps, up to the bounds below.

## What it checks

| Property | Rule, in one sentence | Harm if it breaks |
|---|---|---|
| `NoLostForward` | Once no write, request or run is outstanding, the upstream has every accepted write by a caller that may forward, and every value a push-upstream call saw, unless the last run spent its retry budget and nothing was requested since. | The upstream silently misses an accepted push until the next push or push-upstream call. |
| `ForceKept` | A force request keeps level 2 in the row until a claim takes it, and that claim pushes with force, also after a steal or a retry. | A forced push-upstream becomes a plain push; a rewritten branch stays rejected upstream. |
| `ExplicitNotDelayed` | An explicit request stays due now until a run takes it, even when the run that held the lane ends in retry or restore. | A push-upstream call made during a backoff waits up to 16 minutes. |
| `BackoffKept` | A retry or restore with no explicit request during the run waits out its delay. | Retries run back to back and spend the budget (1 + 5 attempts) in seconds; an upstream outage longer than that loses the forward. |
| `NoRewind` | The upstream ref never moves backwards. | A stale forced push rewinds the upstream. |
| `Settles` | The lane always goes idle and stays idle. | Runs repeat forever, or the lane wedges. |

"Accepted write" means a ref creation or update. Ref deletions are not forwarded, by design: `transferPush` uses no `--prune` and no delete refspecs (`internal/sync/worker_push.go`, file comment). A write by a caller that may not forward (a write-only token, any ref-policy operation, or a sync run's own loopback push), on the git or the REST surface, records no intent on purpose (`enqueueSyncOrForward`, `forwardDeniedFromContext`), and stays local until an authorized trigger. The model also takes every write as a descendant of the one before. A rewrite (a force-updated branch, a replaced tag) is out of scope: its plain forward fails as `FailRejected` with a `repo.push_upstream.failed` event, by design, and only a force push-upstream call publishes it.

## When to run it

Run it when your change touches any file in the [coverage record](#coverage-record), or any other code that writes `push_pending`, `push_due`, `push_attempt` or the push claim columns (`PushPending`, `PushDue`, `PushAttempt`, `PushClaim*` on S3-WAL). Also run it when you change what makes a store transition atomic, which the model assumes and the fingerprints do not see: `SQLite.inTx` (`internal/meta/sqlite.go`), and `S3WAL.mutate` with its `recover` handling (`internal/meta/s3walmeta.go`). The same goes for `toRepoUpstream` (`internal/meta/s3walmeta.go`), which is outside coverage too: it copies the S3-WAL push-lane protocol columns (`PushPending`, `PushSyncing`, `PushClaimHolder`, `PushClaimExpiresAt`, `PushClaimForce`) into the `RepoUpstream` that S3-WAL claims and reads return. Also run it when `check.sh` prints STALE.

## Run

From the repository root:

```sh
OUT=$(mktemp -d "${TMPDIR:-/tmp}/lane-schedule-check.XXXXXX")
.agents/skills/verify-lane-schedule/check.sh --out "$OUT"
```

- Needs Go (the `go.mod` version), Java 11 or newer, and `curl`. The first run downloads `tla2tools.jar` 1.7.4 (sha256-pinned) into `${XDG_CACHE_HOME:-$HOME/.cache}/openstorage-tla`, the cache `verify-orphan-sweep` uses too. Set `JAVA`, `GO`, or `OPENSTORAGE_TLA_CACHE` to override.
- A passing run took about 8 minutes with `TLC_WORKERS=8` (default 4 workers, `TLC_WORKERS`) and the default 4 GB heap (`TLC_HEAP`). `OtherWriters.cfg` is most of it. Fewer workers take longer. A failing run usually takes seconds, because TLC stops at the first violation.
- `--out` keeps every TLC output, the generated configs, and trace summaries. It must be a new or empty directory. Without it, check.sh makes a temporary directory and prints its path.
- `--only <cfg>` runs one config, for example `--only LaneSchedule.cfg`. It is for quick iteration. Finish with a full run.
- `--fact <Name>=FALSE` sets one code fact FALSE in the model only, whatever the code says. It shows what the model does without that guard, and a run with it never exits 0. `mutate.sh --model` uses it (see [Validation](#validation)).
- `--fingerprints-only` checks the code facts and the fingerprints without TLC or Java, in about a second. CI runs it in the `lane-schedule-fingerprints` job of `.github/workflows/ci.yml`, so a change to covered code fails CI until someone brings the model in line and runs `check.sh --record`. CI does not run TLC: a pass says that the covered code is the code the model was checked against, not that the properties hold.

A clean run ends like this:

```text
== Code facts (HEAD <head>)
   ClaimConsumesPending       TRUE
   ... (12 facts, all TRUE)
== TLC 1.7.4 (outputs in /tmp/lane-schedule-check.Ab12Cd)
   LaneSchedule.cfg            PASS (3535660 distinct states)
   OtherWriters.cfg            PASS (56705945 distinct states)
   Liveness.cfg                PASS (348735 distinct states)
   NoLeaseAssumption.cfg       fails as expected (trace: ...)
== Coverage (sync commit <sync commit>)
   every covered declaration matches model/fingerprints.txt
RESULT: PASS
```

| Exit | Meaning | What you do |
|---|---|---|
| 0 | PASS | Nothing. |
| 1 | FAIL: a property broke. FAIL outranks STALE: the same run can also print a STALE coverage block, and then `--record` refuses until the failure is gone | [Read the failure](#read-a-failure). |
| 2 | STALE: covered code changed since the sync commit, a fact is FALSE, or a probe stopped failing. In a full run, the properties hold on the model. With `--fingerprints-only`, TLC did not run, so the result says nothing about the properties | [Bring the model in line](#bring-the-model-in-line). |
| 3 | The check could not run | Fix the tool problem it prints (Go, Java, download, a TLA+ parse error, or a covered file or a declaration a fact reads that facts.go cannot find). |

## How the check follows the code

The model (`model/LaneSchedule.tla`) is a hand-written description of the code. Two things tie it to the code:

1. **Code facts.** `facts/facts.go` parses the Go source and checks 12 guards, on both backends where the guard lives in the store. check.sh writes each one into the TLC config as a constant. When you remove or change a guard, its fact turns FALSE and the model takes the broken branch, so TLC finds the bug. The facts:

   | Fact | Guard in the code | Property that fails without it |
   |---|---|---|
   | `ClaimConsumesPending` | the claim write in `SQLite.beginPush` sets `push_pending = 0`; `S3WAL.beginPush` assigns `u.PushPending = 0` | `Settles` |
   | `ClaimMarksDue` | a scheduled claim writes its claim time: `UPDATE ... SET push_due = ?` with `nowNanos` (SQLite), `u.PushDue = nowNanos` (S3-WAL) | `BackoffKept` |
   | `DoneKeepsPending` | `FinishPushOutcome`'s `OutcomeDone` write never touches `push_pending` (SQLite: neither the done UPDATE nor the shared release clause; S3-WAL: `u.PushPending` is assigned only in the retry/restore case) | `ForceKept`, `NoLostForward` |
   | `RetryRemarks` | retry and restore set `push_pending = MAX(push_pending, level)` (S3-WAL `max(u.PushPending, level)`), with `level = 2` when `o.Force` | `ForceKept`, `NoLostForward` |
   | `FinishKeepsExplicit` | retry and restore keep a zero due time: `push_due = CASE WHEN push_due = 0 THEN 0 ELSE ? END` (SQLite), `if u.PushDue != 0` (S3-WAL) | `ExplicitNotDelayed` |
   | `StealInheritsForce` | `force = pending == 2 \|\| (syncing && claim force)` in both `beginPush`es, and the claim persists it | `ForceKept` |
   | `RunPassesForce` | `RunPush` takes `force` from its claim and passes `Force: force` in every restore outcome (publication wait, stale held snapshot, shutdown); `localPushOutcome` passes it in the retry outcome | `ForceKept` |
   | `WaitBeforeSnapshot` | `RunPush` calls `w.waitPublication` before `w.transferPush`, and `buildLocalSyncQueue` wires it to `githttp.WaitForPushPublication` | `NoLostForward` |
   | `ShutdownRestores` | `RunPush`'s `if runErr != nil && shutdownAborted(ctx)` branch restores through `w.restoreForShutdown` with `meta.OutcomeRestore`, before `localPushOutcome` | `NoLostForward` |
   | `MarkBeforeCas` | `events.RefStore.CASRef` and `CASRefGen` return `runBeforeCAS`'s error before the inner CAS, and `runBeforeCAS` calls the `BeforeCAS` option; `wireHookEventing`'s local-schedule branch sets it to `enqueueSyncOrForward(...)` and passes `BeforeCAS: beforeCAS`; `updateHook` reserves the ref's hold (`reservePushHoldsCovering`) before `store.CASRefGen` | `NoLostForward` |
   | `StrictSnapshot` | `Server.hydrate` hydrates strictly for an upload-pack with a sync claim; `hydrateRefs` takes the held-skip short cuts only under `!strictHeld` and returns `ErrHeldRefStale` when a strict sync skipped a held ref; `hydrateFailed` answers it with 423 Locked; `transferPush` wraps `errHeldRefStale` when `heldRefStale(stderr)` sees `HTTP 423`; `RunPush` then restores through `w.restoreLocal` with `meta.OutcomeRestore`, before `localPushOutcome` | `NoLostForward` |
   | `RestMarkBeforeCas` | `buildServeEventing`'s local branch sets `heldBeforeCAS = restIntentBeforeCAS(ms, cacheDir, forwardDeniedFromContext)` and passes `HeldBeforeCAS: heldBeforeCAS`; `runBeforeCAS` calls it; `restIntentBeforeCAS` calls `githttp.HoldRef` before `ms.RequestPush`; `HoldRef` reserves the hold (`reservePushHolds`); `runBeforeCAS` wraps the release in `sync.OnceFunc`; `RefStore.CASRef` and `CASRefGen` defer `release()` before the inner CAS, so a panicking CAS still ends the hold, and call it after the inner CAS, before `s.emitPush` | `NoLostForward` |

   The "property that fails" column is measured: each guard was broken once in the real code, and check.sh failed with the first property listed (see [Validation](#validation)). For the two rows that list `NoLostForward` second, a one-off model run with that fact FALSE and `NoLostForward` as the only invariant of `LaneSchedule.cfg` failed it too. No script reruns that second result.

2. **Fingerprints.** facts.go hashes the tokens of every declaration (import, function, method, const, var, type) in every covered file. Comments and layout do not count. check.sh compares the hashes with `model/fingerprints.txt`, recorded at the sync commit. A change that the facts do not see, such as a new branch, a new call, a changed import or a changed helper or constant in a covered file, still shows as STALE. Whole files are covered on purpose: a covered path calls many helpers, and any of them can change what the model describes. Most STALE results in these files touch nothing the model describes. Confirm that, and run `--record`. A STALE result is never a pass.

Each TLA+ action names the Go call it stands for:

| Model action | Code |
|---|---|
| `WReserve` | `preReceive` reserves the push's holds (`reservePushHolds`), or the update hook reserves a sidecar or just-in-time ref (`reservePushHoldsCovering`), before the ref's CAS. receive-pack's hydrate ran before that, so the cache has any value from the one before up to the authority's (a REST write, a push that died after its CAS, or another hold can leave it older) |
| `WMark` | the update hook's `CASRefGen` through `events.RefStore` first runs `BeforeCAS`: `enqueueSyncOrForward` runs `RequestPush(repo, false)`, except for a push in `QuietWriters` (forward denied, or `EnvSyncClaim`), which records nothing |
| `WAbort` | the push's intent write or CAS fails (a stale base, a denial, a metadata error), or the push dies before its CAS: nothing is accepted |
| `WCas` | `CASRefGen` wins the authority CAS |
| `WPoke` | `events.RefStore.emitPush` pokes the sync seam after the CAS: the same `RequestPush` behind the same gates |
| `WPublish` | receive-pack commits the cache ref; `referenceTransactionHook` releases the hold (`appendPushReleased`) |
| `WCrash` | after the CAS, the hook or receive-pack dies (serve's shutdown kills git children too), or the poke's `RequestPush` returns an error, which `emitPush` only logs as "sync poke lost" |
| `RReserve`, `RMark` | a REST write (`internal/api` handlers' `CASRef` through `events.RefStore`) first runs `HeldBeforeCAS`: `restIntentBeforeCAS` holds the ref (`githttp.HoldRef`), then runs `RequestPush(repo, false)`. A write in `QuietRestWriters` (forward denied) takes no hold and records nothing |
| `RCas`, `RRelease` | the REST write's authority CAS (no cache update), then the hold's `release` when the CAS returns |
| `RPoke` | `emitPush`'s poke after the CAS, through the serve queue, behind the same gates |
| `RAbort`, `RCrashRest` | the REST write's read, hold, intent write or CAS fails, or serve dies before the CAS: nothing is accepted. Or, after the CAS, serve dies or the poke's `RequestPush` fails |
| `Api` | `POST /repos/push-upstream`: `RequestPushNow` |
| `Tick` | a backoff ends (`push_due <= now`) |
| `Expire` | a claim's lease (`claimLease`) runs out |
| `RClaim` | `ClaimDuePush` (`beginPush` with `requireDue`), offered by `LocalScheduler.SweepOnce` or an enqueue |
| `RWait`, `RWaitFail` | `Worker.waitPublication` (`WaitForPushPublication`); a failed wait restores through `restoreLocal` |
| `RSnap` | `transferPush`'s loopback fetch; hydration copies the authority into the cache unless a live hold names the ref |
| `RSnapHeld` | the loopback fetch's strict hydrate skips a held ref whose cache value is older than the authority and answers 423; `RunPush` restores through `restoreLocal` |
| `RBarrier` | `transferPush`'s detach/steal barrier (`cur.PushClaimHolder != holder`) |
| `RPush`, `RTransient` | `git push --atomic` upstream: applied (a plain push only as a fast-forward) or `FailUnreachable` |
| `RFinish` | `localPushOutcome`, then `FinishPushOutcome` through `retryFinish` |
| `RCancel` | `LocalScheduler.Drain` cancels the run with `ErrShutdown` |
| `RCrash` | the run's process dies, its claim commits unseen (`claimContext`), or every `retryFinish` attempt fails |

### Model variables

| Variable | Code |
|---|---|
| `pending` | `push_pending` (S3-WAL `PushPending`): 0, 1 (plain) or 2 (force) |
| `due` | `push_due`: `"zero"` is 0 (due now, or an explicit request during a run), `"claim"` is a claim time, `"future"` a backoff, `"elapsed"` a backoff that ended |
| `attempt` | `push_attempt` |
| `claim` | `push_syncing` (`held`), `push_claim_holder` (`holder`), `push_claim_force` (`force`), lease ran out (`expired`), holder's run ended (`dead`), and the history field `owes` (it took a force request) |
| `authority`, `cacheRef`, `upstream` | the ref's version in the ref store, in the cache repo, and upstream |
| `hold` | the push or REST write whose live push hold names the ref |
| `wState`, `restState`, `rState` | each git push's, REST write's and run's phase, named after the action that entered it |
| `wVer`, `restVer` | the version each write's CAS wrote |
| `owed` | history: the newest version the lane must forward |
| `forceOwed`, `explicitOwed`, `spent`, `rewound`, `hammered` | history variables that only feed the properties |

## Read a failure

1. Open the trace that check.sh names, such as `<out>/LaneSchedule.trace.txt`. It lists each step (the TLA+ action) and the variables that changed (see [Model variables](#model-variables)). A liveness trace ends with the step that loops back. A probe line that says `UNEXPECTED` means the probe failed on another property than its own: a FALSE fact or a model change broke that property too. Read that probe's trace like any other failure. For a picture in plain words, read the result with the pstack `/statechart` skill if you have it: give it `model/LaneSchedule.tla` and `<out>/<config>.run.cfg` (the config with the facts check.sh used). It shows an invariant violation as its shortest path in plain words, and draws how the lane moves between states when the check passes.
2. Look at the code facts first. A FALSE fact names the guard your change removed or moved. That is the likely cause. The model treats a FALSE guard as absent. If your change moved the guard instead of removing it, the trace can show a path that the moved guard still handles.
3. Replay the trace against the code, one step per action, using the action table above. Ask for each step: can this call happen at this point in the real code, with these values?
   - If some step cannot happen in the code, the model is wrong. Fix the model to match the code, say what was wrong, and rerun.
   - If every step can happen, it is a bug in the code. Report it with the trace in the code's words and the harm from the table above. Do not weaken a property, add a precondition, or shrink a bound to make the check pass. Do not change product code unless the user asks you to.
4. A cheap way to confirm a bug is a gate test in `internal/sync/local_schedule_test.go` (the C1 to C11 harness, for example `TestLocalScheduleNewerIntentSurvives`), which drives `Worker.RunPush` against a real SQLite or S3-WAL row one step at a time. `TestLocalScheduleModel` in `internal/meta/sqlite_schedule_test.go` checks single transitions against a reference model, but not orderings.

## Bring the model in line

When check.sh says STALE, or when you change covered code on purpose:

1. List what changed: `git diff <sync commit> -- <files in the coverage record>`. A PR that changes covered code records its own head, which a squash merge leaves off main. If git does not know the sync commit, fetch more history, as check.sh prints: in a shallow clone, `git fetch --unshallow origin`. A sync commit recorded at a PR head is under `refs/pull/<N>/head`: `git fetch origin '+refs/pull/*/head:refs/remotes/origin/pull/*'`.
2. For each change, decide: no effect on what the model describes, the model no longer describes the code (a step split in two, a new branch, a moved check, a guard that became conditional, a new writer of the push lane columns), or the promise changed on purpose (cite the doc comment, test, or ADR that says so). The facts check call order and literal text, not conditions, so a guard wrapped in a new `if` can keep its fact TRUE: read every changed branch around a guard.
3. Update `model/LaneSchedule.tla` from the code, not from the result. Keep one indivisible step (one SQLite transaction, one S3-WAL CAS, one hook step) as one action. If a fact's guard moved or was renamed, update `facts/facts.go` too.
4. Rerun check.sh. If it passes, break the new code once in a scratch edit (remove the new check, or reorder the new step) and confirm that check.sh fails. Then drop the scratch edit.
5. Run `check.sh --record`. It rewrites `model/fingerprints.txt` and the sync commit below. Run it only after steps 1 to 4. It refuses when a property fails, a fact is FALSE, a probe passes, `--only` is set, a covered file has uncommitted changes, or SKILL.md does not have exactly one `Sync commit:` line. It records the merge base with `origin/main` when the covered files match it there, so a squash merge does not orphan the sync commit.

## Scope

**Verified, within these bounds:** one repository and one ref, two runs at a time (from any processes), a plain and a force push-upstream call, a retry budget of 2, three upstream outages (enough to spend the budget), and two faults (run crashes, failed finish writes, shutdown cancels, pushes that die after their CAS), with two git pushes (`LaneSchedule.cfg`). `OtherWriters.cfg` swaps one push for a push that records no intent and adds two REST writes, one of which records no intent, with one force call, a budget of 1, one outage and one fault. Liveness uses two pushes, a budget of 1, one outage and one fault (`Liveness.cfg`). A push's CAS can always fail instead (`WAbort`, not counted as a fault). TLC checks every interleaving of the modeled steps. A pass covers the model at these bounds, not the whole system.

**Assumptions:**

- Each store transition is atomic: one `BEGIN IMMEDIATE` transaction on SQLite, one conditional PUT of `_wal/meta.json` on S3-WAL. The model reads both backends as the same transitions; facts.go checks that both keep each guard.
- An ambiguous S3-WAL PUT is classified correctly by the `recover` predicates of `FinishPushOutcome` and `beginPush`. A finish whose effect is unknown at worst reports `ErrClaimLost` or `ErrNotFound` to the run, which then abandons, so the model treats it like a successful finish followed by an abandon. Checking those predicates against a dropped-versus-applied PUT is left out.
- The upstream has no other writers: a plain push is rejected only when the snapshot is older than the upstream.
- Live processes keep taking steps, and faults are bounded (for `Settles`).
- One assumption that the code relies on but does not enforce. The probe drops it:
  - `LeaseOutlivesRun`: a claim expires only after its run ended. `claimLease` is the run timeout plus two `detachedOpTimeout` windows plus 5 minutes, and every step of `RunPush` runs inside those windows. A process frozen past the lease (SIGSTOP, a VM pause) breaks it.

**Probe:**

- `NoLeaseAssumption.cfg` drops `LeaseOutlivesRun`, and `NoRewind` must fail. The path: a forced run claims and is frozen past its lease, a push lands, a second run steals the claim (keeping force) and forwards the new value, and the frozen run, which passed its steal barrier before the steal, force-pushes its older snapshot. This is the residual window that `transferPush`'s barrier comment names ("only a FORCED zombie inside that window could rewind the upstream").

If the probe starts passing, the code or the model changed. Review it before you update this file.

**Findings.** Both broke domain-model invariant 11 ("Non-ephemeral ref mutations on synced repos schedule background sync"):

1. *Intent lost after the CAS* (fixed). A REST write wins the authority CAS, then serve dies or `RequestPush` fails before the row records the intent. `events.RefStore.emitPush` only logs "sync poke lost". The lane stays idle, and the upstream misses the write until the next push or push-upstream call. TLC's trace: `RCas`, `RCrashRest`. Git pushes had the same window, which serve's shutdown can open (it kills git children, `gitlimit.StopChildren`). The update hook now records the intent before its CAS, after the ref's hold is in place, and a failed write refuses the update (`MarkBeforeCas`). `TestLocalHookIntentAuthorizationAndFailures` in `cmd/openstorage` reads the row at the instant the CAS commits. A REST write now holds the ref and records the intent before its CAS too (`restIntentBeforeCAS`, `RestMarkBeforeCas`): the hold keeps a run that claims the intent from snapshotting the ref before the CAS. `TestServeRestIntentBeforeCAS` reads the row and the hold at the instant the CAS commits.
2. *Stale held snapshot* (fixed). A REST write changes only the authority and records intent. A run claims it and passes its publication wait, because nothing holds the ref yet. A push whose advertisement predates the REST write (here one with a forward-denied token, so it records nothing) then reserves its hold on the ref. The run's loopback fetch skipped hydrating the held ref, snapshotted the old value, forwarded it, and ended `done`. The push's CAS failed on its stale base, and nothing recorded intent again. A push that dies after its CAS and before its cache commit leaves the same stale cache. The strict hydrate of every push run's fetch now refuses such a ref, and the run restores (`StrictSnapshot`, `RSnapHeld`). PostgreSQL had the same stale snapshot. The model leaves PostgreSQL out, but there a River run now restores with `RestorePush` and fails, so River snoozes the job (`TestPushRunStaleHeldSnapshotRestoresRiverRequest` in `internal/sync`). `TestLocalForwardHeldStaleRefRestores` in `internal/githttp` replays the trace: a REST write, `RClaim`, `RWait`, `WReserve`, `RSnap`, `WAbort`, `RBarrier`, `RPush`, `RFinish`.

When a fix lands, its probe passes and check.sh reports STALE. The fix must then delete the assumption it removes (and its probe), and update this section.

**Simplifications:** each one gives the model at least the behaviors of the code, or the same outcome:

- One ref with versions 0..`MaxVersion` stands for the repository: each accepted write is one larger number, and the forward is one atomic `git push --atomic`.
- Hydration happens at a run's snapshot and at a push's reservation (receive-pack's own hydrate). Real reads also hydrate the cache, which only makes it fresher, so the model allows staler caches than the code.
- A loopback fetch hydrates up to three times (ref advertisement, ref listing, pack); `RSnap` and `RSnapHeld` stand for the one that decides. Any of them refusing a held ref restores the run.
- Runs are not limited by the sweep's slots, the `inflight` map, or `ListLocalWork` paging: any idle runner may claim whenever the row allows. `ClaimDuePush` is the only admission gate in the code too.
- Error returns that end a run the same way as a crash are modeled as that crash. A claim that commits unseen is a crash right after the claim.
- A run's other failures (a failed loopback fetch, a rejected or misconfigured push) end `done` with a `repo.push_upstream.failed` event, by design (`TransientPushFailure`): they are modeled only as the rejected plain push.
- `MaxRetries = 2` stands for `MaxPushTransientRetries = 5`, and delays are one abstract "future" due time.

**Left out.** A pass says nothing about these. Each line says what a user sees when that part goes wrong:

- The pull lane (`ClaimDueSync`, `FinishSyncOutcome`, the mirror-back pull that a successful forward marks, the hook's pull poke): the repository stops picking up upstream changes, or keeps a pre-forward snapshot.
- PostgreSQL (River schedules its push jobs): the same lost or stale forward, on PostgreSQL deployments.
- Detach (`DELETE /repos/base`): a forward that runs after the detach answered, or a lane row left behind.
- Ref deletions (not forwarded, see above): none by design. A deleted branch stays upstream.
- The event payloads and their run counts: wrong or missing `repo.push_upstream.*` webhooks, with the forward itself correct.
- The egress guard: a forward that dials a denied destination, or a denied forward to an allowed one.
- The push-hold details that one `hold` stands for (per-ref holds, sidecars, TTL refresh, epochs, held markers): a push rejected though its CAS won, or a run that waits on or skips the wrong ref.
- The S3-WAL ambiguous-PUT recovery above: an outcome applied twice or reported wrongly after a timed-out PUT.

## Validation

Every fact was broken in the real code, one at a time, on a checkout whose covered code equals the sync commit. Each scratch break compiled (`go build` of its package), turned exactly its own fact FALSE, and made `check.sh --only <config>` fail with a property violation, not only STALE. `mutate.sh` runs these breaks and restores each file. To rerun it, commit or discard your changes to the covered files, then run from the repository root:

```sh
.agents/skills/verify-lane-schedule/mutate.sh
```

It needs what check.sh needs, plus git, bash 4 or newer, and python3, and took about 7 minutes with `TLC_WORKERS=8`, most of it the unmodified baseline runs. It prints one line per break and ends with `COUNT: 12/12 mutations failed check.sh as expected`. It first runs each config it uses unmodified. When check.sh cannot run at all there (exit 3, such as a missing Java or a TLC error), mutate.sh stops with exit 3 and check.sh's error line, before any break. When you add, move or rename a fact's guard, update its line in `mutate.sh`:

| Scratch break | Fact | Config | Failed |
|---|---|---|---|
| drop `push_pending = 0` from `SQLite.beginPush`'s claim UPDATE | `ClaimConsumesPending` | `Liveness.cfg` | `Settles` |
| `attempt = u.PushAttempt` without `u.PushDue = nowNanos` in `S3WAL.beginPush` | `ClaimMarksDue` | `LaneSchedule.cfg` | `BackoffKept` |
| add `push_pending = 0` to `SQLite.FinishPushOutcome`'s done UPDATE | `DoneKeepsPending` | `LaneSchedule.cfg` | `ForceKept` |
| remove `u.PushPending = max(u.PushPending, level)` from `s3walLocalSchedule.FinishPushOutcome` | `RetryRemarks` | `LaneSchedule.cfg` | `ForceKept` |
| `push_due = ?` instead of the `CASE WHEN push_due = 0` in the SQLite retry UPDATE | `FinishKeepsExplicit` | `LaneSchedule.cfg` | `ExplicitNotDelayed` |
| `force = u.PushPending == 2` in `S3WAL.beginPush` | `StealInheritsForce` | `LaneSchedule.cfg` | `ForceKept` |
| drop `Force: force` from `RunPush`'s shutdown restore outcome | `RunPassesForce` | `LaneSchedule.cfg` | `ForceKept` |
| replace `w.waitPublication(ctx, job.RepoID)` in `RunPush` with `error(nil)` | `WaitBeforeSnapshot` | `LaneSchedule.cfg` | `NoLostForward` |
| `errors.Is(runErr, ErrShutdown)` instead of `shutdownAborted(ctx)` in `RunPush` (a cancelled git child returns `context.Canceled`) | `ShutdownRestores` | `LaneSchedule.cfg` | `NoLostForward` |
| move `RefStore.CASRefGen`'s `runBeforeCAS` call after the inner CAS | `MarkBeforeCas` | `LaneSchedule.cfg` | `NoLostForward` |
| `&& false` on `hydrateRefs`'s `refsErr == nil && skippedHeld && strictHeld` | `StrictSnapshot` | `OtherWriters.cfg` | `NoLostForward` |
| drop `heldBeforeCAS = restIntentBeforeCAS(...)` from `buildServeEventing` | `RestMarkBeforeCas` | `OtherWriters.cfg` | `NoLostForward` |

With every fact TRUE in the code, setting each fact FALSE in the model alone fails the same property, so no fact is dead weight. `mutate.sh --model` reruns this with `check.sh --fact` and edits no code; it needs no python3 and ends with `COUNT: 12/12`. `StrictSnapshot` needs `OtherWriters.cfg`: a holding push that records intent before its CAS leaves a pending request, so only a quiet push (forward denied) turns the stale snapshot into a lost forward. The probe fails on the current code, and the property configs pass with the bounds in [Scope](#scope).

## Coverage record

Sync commit: `f46b43e5f8f07f6f48d8c871e91b0b122b07a8c7` (the last commit at which someone confirmed that the model describes this code; only `check.sh --record` moves it).

Every declaration in these files is covered:

- `internal/meta/sqlite_schedule.go`, `internal/meta/sqlite_eventing.go` (the SQLite lane transitions)
- `internal/meta/s3walmeta_schedule.go`, `internal/meta/s3walmeta_eventing.go` (the S3-WAL lane transitions)
- `internal/sync/worker_push.go`, `internal/sync/worker.go`, `internal/sync/local_schedule.go` (runs, retries, leases, the sweep, the drain, failure classification, the stale-held restore)
- `internal/sync/metastore.go`, `internal/sync/pushstatus.go` (the store adapter's error mapping, the upstream rejection parser)
- `internal/events/refstore.go` (the intent decorator around the ref CAS, and its `BeforeCAS` and `HeldBeforeCAS` steps)
- `cmd/openstorage/wire_eventing.go`, `cmd/openstorage/wire_eventing_local.go` (hook and serve wiring, forward gates, the intent before the CAS, the sync-claim suppression)
- `internal/api/handlers_sync.go` (`push-upstream` and its `RequestPushNow`)
- `internal/githttp/publication.go`, `internal/githttp/hydrate.go`, `internal/githttp/hook.go` (push holds, their release, the publication wait, the held-ref skip and the strict sync fetch)
- `internal/githttp/server.go` (which fetches hydrate strictly, and the 423 answer)

`model/fingerprints.txt` holds one hash per declaration.

## Files

| File | Purpose |
|---|---|
| `check.sh` | The one command: facts, TLC runs, fingerprints, `--record`, and the TLC-free `--fingerprints-only` that CI runs. |
| `facts/facts.go` | Reads the code facts and fingerprints from the Go source (standard library only; the dot directory keeps it out of `./...`). |
| `mutate.sh` | Breaks each fact once in the real code and checks that check.sh fails with the expected property (the Validation table). `--model` sets each fact FALSE in the model only. |
| `model/LaneSchedule.tla` | The model. |
| `model/LaneSchedule.cfg`, `OtherWriters.cfg`, `Liveness.cfg` | Property configs; each must pass. |
| `model/NoLeaseAssumption.cfg` | Probe; must fail: `NoRewind`. |
| `model/fingerprints.txt` | Fingerprints at the sync commit. |

To bring this verification back in line after changes that skipped this skill, use the pstack `/maintain-formal-verification-skill` skill if you have it, or follow [Bring the model in line](#bring-the-model-in-line).
