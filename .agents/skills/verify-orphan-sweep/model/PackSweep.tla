------------------------------ MODULE PackSweep ------------------------------
(***************************************************************************)
(* The orphan-pack sweeper racing pack writers on ONE pack key K           *)
(* (packs/<repo>/pack-<id>.pack). Pack keys are content-addressed, so      *)
(* every writer and every restore puts identical bytes under K. The model  *)
(* tracks only whether K's blob exists.                                    *)
(*                                                                         *)
(* Each action names the Go call it stands for. An action is one store or  *)
(* registry call, except where a comment says that two calls are merged   *)
(* because no other process can change the outcome between them.          *)
(*                                                                         *)
(*   writers  internal/githttp/hook.go   preReceive, publication block     *)
(*            internal/api/publish.go    server.publishPack                *)
(*            internal/blob/blob.go      EnsurePublished                   *)
(*            internal/jobs/repack.go    Repacker.repack publishing its    *)
(*            consolidated pack (UploadFile, ReplacePacks, verifyPublished) *)
(*   sweeper  internal/jobs/sweep.go     Sweeper.sweep, classify,          *)
(*            revalidateOrphan, safeDelete, restoreFromQuarantine,         *)
(*            healQuarantine, healQuarantineEntry, quarantinePurgeAge      *)
(*   repack   internal/jobs/repack.go    Repacker.repack, ReplacePacks     *)
(*                                                                         *)
(* Abstractions (SKILL.md "Scope" has the full list):                      *)
(*  - The sweeper ignores blob age and the packs/ listing: it may pick K   *)
(*    as a candidate at any time. The real sweeper also needs K listed,    *)
(*    unregistered at classify time, and older than                        *)
(*    OPENSTORAGE_SWEEP_AGE_THRESHOLD, so the model checks a strictly more *)
(*    aggressive sweeper.                                                  *)
(*  - Time enters only as "a quarantine backup reaches purge age"          *)
(*    (AgeBackup), gated by the code facts and the timing assumptions.     *)
(*  - Delete mode is on: OPENSTORAGE_SWEEP_DELETE_ENABLED, or a retained   *)
(*    superseded record (OPENSTORAGE_SWEEP_DELETE_SUPERSEDED, default on). *)
(*    Quota covers, LFS keys and truncated blobs are out of scope.         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Writers,    \* write attempts that publish K: git pushes and REST writes
    Sweepers,   \* sweep runs, one per replica (River periodic job or ticker)
    Nonces,     \* write-once quarantine keys sweep/quarantine/<nonce>/K
    NoNonce,    \* "no backup" marker, not in Nonces
    MaxFaults,  \* bound on sweeper crashes
    MaxFolds,   \* bound on repack folds that drop K's registry row

    \* Code facts. check.sh reads each one from the Go source (facts.go)
    \* and writes it into the config, so a code change flips the model.
    WriterVerifies,
        \* hook.go preReceive and publish.go publishPack call
        \* blob.EnsurePublished after RegisterPacks, Repacker.repack calls
        \* verifyPublished after ReplacePacks, and both re-upload the local
        \* pack file when Stat says ErrNotFound.
    VerifyOnRegisterError,
        \* nothing between RegisterPacks and EnsurePublished in either
        \* writer can skip the verify (no return, break, continue or goto,
        \* no condition on regErr): an ambiguous RegisterPacks error still
        \* gets verified.
    CopyBeforeDelete,
        \* safeDelete calls s.blobs.Copy(qKey, key) before s.blobs.Delete.
    RecheckAfterDelete,
        \* safeDelete calls s.registered after the delete, then
        \* restoreFromQuarantine.
    HealEverySweep,
        \* sweep calls healQuarantine before it lists packs/.
    HealRestores,
        \* healQuarantineEntry calls restoreFromQuarantine for a
        \* registered, missing original before its purge.
    PurgeAgeGated,
        \* healQuarantineEntry deletes a backup only past quarantinePurgeAge.
    PurgeFloorCoversHandshake,
        \* QuarantinePurgeFloor >= 2 x sweepRecoveryTimeout, and safeDelete
        \* bounds its delete phase and its recovery phase by
        \* sweepRecoveryTimeout each: a backup cannot age while its own
        \* handshake still runs.
    PurgeAgeCoversThreshold,
        \* quarantinePurgeAge = max(s.cfg.Threshold, QuarantinePurgeFloor).

    \* Timing assumptions. The code does not enforce either one.
    WritesFinishWithinThreshold,
        \* A write that is in flight when a backup is copied, or when its
        \* handshake deletes K, finishes before that backup reaches purge
        \* age. The delete runs at most sweepRecoveryTimeout (15 min) after
        \* the copy, so this holds when every write finishes within
        \* quarantinePurgeAge minus 15 min, minus the HealEntryIsQuick
        \* bound. SweepConfig.Threshold documents the stronger rule that the
        \* threshold "must comfortably exceed the longest possible
        \* push/import duration"; no code enforces either.
    HealEntryIsQuick
        \* healQuarantineEntry runs from its registry read to its age gate
        \* within the margin above. No deadline bounds it. In the model: a
        \* backup does not reach purge age while a heal visits it.

ASSUME NoNonce \notin Nonces
ASSUME MaxFaults \in Nat /\ MaxFolds \in Nat

VARIABLES
    present,     \* K's blob exists under packs/
    registered,  \* K has a pack registry row (ref.RefStore.HasPack)
    backup,      \* backup[n]: "unused" | "fresh" | "aged" | "gone"
    witness,     \* witness[n]: writers in flight when backup n was copied
                 \*             or when its handshake deleted K
    wpc,         \* writer step
    regOk,       \* the writer's RegisterPacks returned no error
    spc,         \* sweeper step
    todo,        \* heal: listed backups not visited yet
    cur,         \* heal: backup being visited
    hs,          \* safeDelete: this handshake's backup
    faults,      \* sweeper crashes so far
    folds        \* repack folds so far

writerVars  == <<wpc, regOk>>
sweeperVars == <<spc, todo, cur, hs>>
vars == <<present, registered, backup, witness, faults, folds,
          writerVars, sweeperVars>>

WriterSteps  == {"idle", "upload", "register", "verify", "done", "failed",
                 "crashed"}
SweeperSteps == {"idle", "heal_next", "heal_reg", "heal_stat", "heal_purge",
                 "rv_reg", "sd_copy", "sd_delete", "sd_recheck",
                 "sd_restore"}
Live == {"fresh", "aged"}   \* backup states that hold K's bytes

InFlight(w) == wpc[w] \in {"upload", "register", "verify"}
InFlightWriters == {w \in Writers : InFlight(w)}
Handshake == {"sd_delete", "sd_recheck", "sd_restore"}
HealVisit == {"heal_stat", "heal_purge"}

TypeOK ==
    /\ present \in BOOLEAN /\ registered \in BOOLEAN
    /\ backup \in [Nonces -> {"unused", "fresh", "aged", "gone"}]
    /\ witness \in [Nonces -> SUBSET Writers]
    /\ wpc \in [Writers -> WriterSteps]
    /\ regOk \in [Writers -> BOOLEAN]
    /\ spc \in [Sweepers -> SweeperSteps]
    /\ todo \in [Sweepers -> SUBSET Nonces]
    /\ cur \in [Sweepers -> Nonces \cup {NoNonce}]
    /\ hs \in [Sweepers -> Nonces \cup {NoNonce}]
    /\ faults \in 0..MaxFaults /\ folds \in 0..MaxFolds

Init ==
    \* K starts absent, as an unregistered blob (a crashed push's orphan or a
    \* repack-superseded pack), or registered with its bytes.
    /\ present \in BOOLEAN
    /\ registered \in BOOLEAN
    /\ registered => present
    /\ backup = [n \in Nonces |-> "unused"]
    /\ witness = [n \in Nonces |-> {}]
    /\ wpc = [w \in Writers |-> "idle"]
    /\ regOk = [w \in Writers |-> FALSE]
    /\ spc = [s \in Sweepers |-> "idle"]
    /\ todo = [s \in Sweepers |-> {}]
    /\ cur = [s \in Sweepers |-> NoNonce]
    /\ hs = [s \in Sweepers |-> NoNonce]
    /\ faults = 0 /\ folds = 0

-----------------------------------------------------------------------------
(* Writers. hook.go preReceive and publish.go publishPack run the same      *)
(* pipeline: Stat -> UploadFile -> RegisterPacks -> EnsurePublished ->      *)
(* ref CAS. A repack publishing its consolidated pack runs UploadFile ->    *)
(* ReplacePacks -> verifyPublished: the same steps, except that it uploads  *)
(* a present key again, which changes nothing here. A writer can crash at   *)
(* any step in flight (WriterCrash); that also stands for every error       *)
(* return, which ends the write the same way.                               *)

Go(w, step) == wpc' = [wpc EXCEPT ![w] = step]
Finish(w, ok) == Go(w, IF ok THEN "done" ELSE "failed")

\* bl.Stat(ctx, packs[i].Key) / s.blobStore.Stat(ctx, key): a present key is
\* "published" and adopted without an upload.
WriterStat(w) ==
    /\ wpc[w] = "idle"
    /\ Go(w, IF present THEN "register" ELSE "upload")
    /\ UNCHANGED <<present, registered, backup, witness, faults, folds,
                   regOk, sweeperVars>>

\* st.HasPack / s.refStore.HasPack, then bl.UploadFile / s.blobStore.UploadFile.
\* Merged: a probe that finds K registered skips the upload (EnsurePublished
\* uploads K later), and an upload is always allowed, which covers a probe
\* that ran before a registration.
WriterUpload(w) ==
    /\ wpc[w] = "upload"
    /\ \/ present' = TRUE
       \/ registered /\ UNCHANGED present
    /\ Go(w, "register")
    /\ UNCHANGED <<registered, backup, witness, faults, folds, regOk,
                   sweeperVars>>

\* st.RegisterPacks / s.refStore.RegisterPacks. An error can be ambiguous:
\* the row may have committed with the response lost.
WriterRegister(w) ==
    /\ wpc[w] = "register"
    /\ \/ /\ registered' = TRUE
          /\ regOk' = [regOk EXCEPT ![w] = TRUE]
       \/ /\ registered' \in {registered, TRUE}
          /\ regOk' = [regOk EXCEPT ![w] = FALSE]
    /\ IF WriterVerifies /\ (regOk'[w] \/ VerifyOnRegisterError)
       THEN Go(w, "verify")
       ELSE Finish(w, regOk'[w])
    /\ UNCHANGED <<present, backup, witness, faults, folds, sweeperVars>>

\* blob.EnsurePublished: b.Stat(ctx, key), then b.UploadFile(ctx, key, path)
\* on ErrNotFound. Merged: whatever runs between the Stat and the upload,
\* the upload leaves K present. A Stat or upload error is WriterCrash here.
WriterVerify(w) ==
    /\ wpc[w] = "verify"
    /\ present' = TRUE
    /\ Finish(w, regOk[w])
    /\ UNCHANGED <<registered, backup, witness, faults, folds, regOk,
                   sweeperVars>>

\* The write ends in flight: the process dies (OOM, SIGKILL of the hook), or
\* a store call returns an error. Its local pack file is gone with it.
WriterCrash(w) ==
    /\ InFlight(w)
    /\ Go(w, "crashed")
    /\ UNCHANGED <<present, registered, backup, witness, faults, folds,
                   regOk, sweeperVars>>

WriterStep(w) ==
    \/ WriterStat(w) \/ WriterUpload(w) \/ WriterRegister(w) \/ WriterVerify(w)

-----------------------------------------------------------------------------
(* Sweepers. sweep = healQuarantine, then classify each listed blob. A      *)
(* sweeper can crash at any step (SweeperCrash). A crash also stands for    *)
(* the error returns of Copy, the recheck read and restoreFromQuarantine,   *)
(* which end the handshake the same way.                                    *)

S(s, step) == spc' = [spc EXCEPT ![s] = step]
EndHandshake(s) == S(s, "idle") /\ hs' = [hs EXCEPT ![s] = NoNonce]

\* sweep starts. With HealEverySweep, healQuarantine runs first and lists
\* the quarantine prefix: s.blobs.ListPages(ctx, quarantinePrefix, ...).
SweepStart(s) ==
    /\ spc[s] = "idle"
    /\ IF HealEverySweep
       THEN /\ todo' = [todo EXCEPT ![s] = {n \in Nonces : backup[n] \in Live}]
            /\ S(s, "heal_next")
       ELSE /\ S(s, "rv_reg")
            /\ UNCHANGED todo
    /\ UNCHANGED <<present, registered, backup, witness, faults, folds,
                   writerVars, cur, hs>>

\* healQuarantine visits the next listed entry, in any order (the random
\* nonce orders the listing). With none left the sweep moves on to packs/.
HealNext(s) ==
    /\ spc[s] = "heal_next"
    /\ IF todo[s] = {}
       THEN /\ S(s, "rv_reg")
            /\ cur' = [cur EXCEPT ![s] = NoNonce]
            /\ UNCHANGED todo
       ELSE \E n \in todo[s] :
              /\ cur' = [cur EXCEPT ![s] = n]
              /\ todo' = [todo EXCEPT ![s] = @ \ {n}]
              /\ S(s, "heal_reg")
    /\ UNCHANGED <<present, registered, backup, witness, faults, folds,
                   writerVars, hs>>

\* healQuarantineEntry: s.registered(ctx, repo, orig). An unregistered
\* original goes straight to the age gate (its Stat only feeds the
\* ReclaimRetainedPack accounting).
HealReg(s) ==
    /\ spc[s] = "heal_reg"
    /\ S(s, IF registered THEN "heal_stat" ELSE "heal_purge")
    /\ UNCHANGED <<present, registered, backup, witness, faults, folds,
                   writerVars, todo, cur, hs>>

\* healQuarantineEntry: s.blobs.Stat(ctx, orig), then for a missing original
\* restoreFromQuarantine(ctx, orig, q.Key), a server-side copy of the backup
\* over K. Merged: an upload between the two leaves the same bytes. A backup
\* that a peer purged meanwhile makes the entry return before its purge. A
\* present, complete original goes to the age gate (healPresentOriginal
\* returns spare=false).
HealStat(s) ==
    /\ spc[s] = "heal_stat"
    /\ IF ~present /\ HealRestores
       THEN IF backup[cur[s]] \in Live
            THEN present' = TRUE /\ S(s, "heal_purge")
            ELSE UNCHANGED present /\ S(s, "heal_next")
       ELSE UNCHANGED present /\ S(s, "heal_purge")
    /\ UNCHANGED <<registered, backup, witness, faults, folds, writerVars,
                   todo, cur, hs>>

\* healQuarantineEntry: the age gate `s.now().Sub(q.ModTime) <
\* s.quarantinePurgeAge()`, then s.blobs.Delete(ctx, q.Key).
HealPurge(s) ==
    /\ spc[s] = "heal_purge"
    /\ IF ~PurgeAgeGated \/ backup[cur[s]] = "aged"
       THEN backup' = [backup EXCEPT ![cur[s]] = "gone"]
       ELSE UNCHANGED backup
    /\ S(s, "heal_next")
    /\ UNCHANGED <<present, registered, witness, faults, folds, writerVars,
                   todo, cur, hs>>

\* classify reads s.registered and the age, then revalidateOrphan reads
\* s.registered again and the key's fresh ModTime. The model keeps only the
\* last registry read: it lets the sweeper reach it with K in any state.
RevalidateReg(s) ==
    /\ spc[s] = "rv_reg"
    /\ S(s, IF registered THEN "idle" ELSE "sd_copy")
    /\ UNCHANGED <<present, registered, backup, witness, faults, folds,
                   writerVars, todo, cur, hs>>

\* safeDelete: s.blobs.Copy(ctx, qKey, key) to a fresh nonce key. ErrNotFound
\* (K is gone) returns without a delete; so does revalidateOrphan's
\* "gone already". With no unused nonce left, the model stops the sweep
\* here, so a behavior has at most |Nonces| handshakes.
QuarantineCopy(s) ==
    /\ spc[s] = "sd_copy"
    /\ IF ~CopyBeforeDelete
       THEN /\ S(s, "sd_delete")
            /\ UNCHANGED <<backup, witness, hs>>
       ELSE \/ /\ ~present \/ \A n \in Nonces : backup[n] # "unused"
               /\ S(s, "idle")
               /\ UNCHANGED <<backup, witness, hs>>
            \/ \E n \in Nonces :
                 /\ present
                 /\ backup[n] = "unused"
                 /\ backup' = [backup EXCEPT ![n] = "fresh"]
                 /\ witness' = [witness EXCEPT ![n] = InFlightWriters]
                 /\ hs' = [hs EXCEPT ![s] = n]
                 /\ S(s, "sd_delete")
    /\ UNCHANGED <<present, registered, faults, folds, writerVars, todo, cur>>

\* safeDelete: s.blobs.Delete(hctx, key). An error is ambiguous (the delete
\* may have run) and goes straight to restoreFromQuarantine.
Delete(s) ==
    /\ spc[s] = "sd_delete"
    /\ witness' = IF hs[s] = NoNonce THEN witness
                  ELSE [witness EXCEPT ![hs[s]] = @ \cup InFlightWriters]
    /\ \/ /\ present' = FALSE
          /\ IF RecheckAfterDelete
             THEN S(s, "sd_recheck") /\ UNCHANGED hs
             ELSE EndHandshake(s)
       \/ /\ present' \in {present, FALSE}
          /\ S(s, "sd_restore")
          /\ UNCHANGED hs
    /\ UNCHANGED <<registered, backup, faults, folds, writerVars, todo, cur>>

\* safeDelete: s.registered(rctx, repo, key) after the delete. A registered
\* key goes to restoreFromQuarantine; an unregistered key is reaped.
Recheck(s) ==
    /\ spc[s] = "sd_recheck"
    /\ IF registered
       THEN S(s, "sd_restore") /\ UNCHANGED hs
       ELSE EndHandshake(s)
    /\ UNCHANGED <<present, registered, backup, witness, faults, folds,
                   writerVars, todo, cur>>

\* safeDelete: restoreFromQuarantine(rctx, key, qKey): copies the backup over
\* K. Without a backup the copy fails, and the handshake ends either way.
Restore(s) ==
    /\ spc[s] = "sd_restore"
    /\ IF hs[s] # NoNonce /\ backup[hs[s]] \in Live
       THEN present' = TRUE
       ELSE UNCHANGED present
    /\ EndHandshake(s)
    /\ UNCHANGED <<registered, backup, witness, faults, folds, writerVars,
                   todo, cur>>

\* The sweeper process dies at any step: its in-flight state is lost, its
\* backups stay.
SweeperCrash(s) ==
    /\ spc[s] # "idle"
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ spc' = [spc EXCEPT ![s] = "idle"]
    /\ todo' = [todo EXCEPT ![s] = {}]
    /\ cur' = [cur EXCEPT ![s] = NoNonce]
    /\ hs' = [hs EXCEPT ![s] = NoNonce]
    /\ UNCHANGED <<present, registered, backup, witness, folds, writerVars>>

SweeperStep(s) ==
    \/ SweepStart(s) \/ HealNext(s) \/ HealReg(s) \/ HealStat(s)
    \/ HealPurge(s) \/ RevalidateReg(s) \/ QuarantineCopy(s) \/ Delete(s)
    \/ Recheck(s) \/ Restore(s)

-----------------------------------------------------------------------------
(* The environment: repack folds and time.                                 *)

\* repack.go: r.store.ReplacePacks drops K's row after folding its objects
\* into a new pack (K becomes a retained, superseded blob). The repack
\* downloads K before it folds. The model allows a fold only while K is
\* present, so a fold can never hide a lost pack by dropping its row.
Fold ==
    /\ registered /\ present /\ folds < MaxFolds
    /\ registered' = FALSE
    /\ folds' = folds + 1
    /\ UNCHANGED <<present, backup, witness, faults, writerVars, sweeperVars>>

\* Backup n passes quarantinePurgeAge. PurgeFloorCoversHandshake: not while
\* the handshake that wrote it still runs. The threshold assumption plus
\* PurgeAgeCoversThreshold: not while a writer that was in flight at its
\* copy or delete still runs. HealEntryIsQuick: not while a heal visits it.
AgeBackup(n) ==
    /\ backup[n] = "fresh"
    /\ PurgeFloorCoversHandshake =>
         ~\E s \in Sweepers : hs[s] = n /\ spc[s] \in Handshake
    /\ (WritesFinishWithinThreshold /\ PurgeAgeCoversThreshold) =>
         \A w \in witness[n] : ~InFlight(w)
    /\ HealEntryIsQuick =>
         ~\E s \in Sweepers : cur[s] = n /\ spc[s] \in HealVisit
    /\ backup' = [backup EXCEPT ![n] = "aged"]
    /\ UNCHANGED <<present, registered, witness, faults, folds, writerVars,
                   sweeperVars>>

Next ==
    \/ \E w \in Writers : WriterStep(w) \/ WriterCrash(w)
    \/ \E s \in Sweepers : SweeperStep(s) \/ SweeperCrash(s)
    \/ Fold
    \/ \E n \in Nonces : AgeBackup(n)

\* Live processes keep taking steps. Crashes are bounded, so every sweeper
\* eventually runs clean sweeps. Time (AgeBackup) and repack (Fold) need no
\* fairness.
Fairness ==
    /\ \A w \in Writers : WF_vars(WriterStep(w))
    /\ \A s \in Sweepers : WF_vars(SweeperStep(s))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Properties.                                                              *)

\* Some copy of K's bytes is still in the system and something will put it
\* back: K itself, a quarantine backup, or an in-flight writer that will
\* verify (it still holds its local pack file).
Recoverable ==
    \/ present
    \/ \E n \in Nonces : backup[n] \in Live
    \/ WriterVerifies /\ \E w \in Writers : InFlight(w)

\* A registered pack never loses its last copy. If it did, every cold
\* hydrate of the repository would fail with githttp.ErrPackMissing.
NoLostPack == registered => Recoverable

\* The claim in the Sweeper.sweep doc comment: "the recheck and the writer
\* verify jointly cover every interleaving with no timing assumptions".
\* While nothing has crashed, a registered pack is missing only while a
\* writer or a sweeper handshake that puts it back is still running: nobody
\* waits for the next sweep's heal. (A crash, or a store error that ends a
\* write, leaves the repair to the heal: NoLostPack and BytesComeBack.)
NoCrash == faults = 0 /\ \A w \in Writers : wpc[w] # "crashed"
Repairing ==
    \/ \E w \in Writers : InFlight(w)
    \/ \E s \in Sweepers : spc[s] \in Handshake
NoWaitForHeal == (registered /\ ~present /\ NoCrash) => Repairing

\* A registered pack whose blob is missing gets its bytes back (or stops
\* being registered).
BytesComeBack == [](registered /\ ~present => <>(present \/ ~registered))

SweepSymmetry == Permutations(Writers) \cup Permutations(Sweepers)
                 \cup Permutations(Nonces)
=============================================================================
