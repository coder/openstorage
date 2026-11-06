------------------------------ MODULE RepoPurge ------------------------------
(***************************************************************************)
(* RepoPurger reclaiming ONE soft-deleted repository while pre-delete      *)
(* writers, a second purger and a purge-aware sweeper race it, on ONE pack *)
(* key K of the repository (packs/<repo>/pack-<id>.pack, or the same key   *)
(* under a +namespace prefix: the fence and the purge phases treat both    *)
(* alike). Keys do not interact, so one key stands for all.                *)
(*                                                                         *)
(* Each action names the Go call it stands for. An action is one store or  *)
(* registry call, except where a comment says that calls are merged.      *)
(*                                                                         *)
(*   purger   internal/jobs/purge.go     PurgeExpired, PurgeRepo,          *)
(*            purgeRepo, rescanPurged, reapPrefix                          *)
(*   sweeper  internal/jobs/sweep.go     sweep (purgeMode), classify's     *)
(*            PurgedRepoProbe branch, healQuarantineEntry's fenced purge,  *)
(*            safeDelete, restoreFromQuarantine                            *)
(*   writers  RegisterPacks + EnsurePublished + CASRef (git push, REST),   *)
(*            CASRef alone (REST branch/tag creation), and a repack's      *)
(*            ReplacePacks + verifyPublished                               *)
(*   registry internal/ref (PG, SQLite, S3WAL): FenceRepoPurge,            *)
(*            DropRepoRefs, RetireRepoPacks, CountRepoRows, and the fence  *)
(*            checks of RegisterPacks and CASRef                           *)
(*                                                                         *)
(* Abstractions (SKILL.md "Scope" has the full list):                      *)
(*  - Time enters as the repository's deletion age (young < MinRepoPurgeAge *)
(*    <= old < retention <= expired) and as "a quarantine backup reaches   *)
(*    purge age" (AgeBackup).                                              *)
(*  - The sweeper runs with every delete flag off: purge-aware mode only,  *)
(*    which serve and purge-repo always wire. The delete modes are         *)
(*    PackSweep's model. It ignores blob age, so it may reap K at any time. *)
(*  - LFS keys, quota covers and the ledger rows are out of scope.         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Writers,     \* write attempts: pushes, REST writes, repacks
    Purgers,     \* RepoPurger runs: the background pass and purge-repo
    BgPurgers,   \* the background ones (PurgeExpired); the rest are the CLI
    Sweepers,    \* purge-aware sweep runs
    Nonces,      \* quarantine key slots sweep/quarantine/<nonce>/K
    NoNonce,     \* "no backup" marker, not in Nonces
    MaxFaults,   \* bound on crashes, guard failures and ambiguous deletes

    \* Code facts. check.sh reads each one from the Go source (facts.go)
    \* and writes it into the config, so a code change flips the model.
    \* Shared with PackSweep:
    WriterVerifies, VerifyOnRegisterError, CopyBeforeDelete,
    RecheckAfterDelete, HealRestores, PurgeAgeGated,
    PurgeFloorCoversHandshake,
    \* RepoPurger:
    PurgeGuarded,
        \* PurgeExpired and PurgeRepo call DeleteGuard and return on error
        \* before any purge step (once per pass, before purgeRepo).
    SweepGuardStopsPurgeMode,
        \* sweep's DeleteGuard error turns purgeMode off for that run.
    RefusesLive,
        \* purgeRepo returns before FenceRepoPurge when DeletedAt == nil.
    RefusesYoung,
        \* purgeRepo returns before FenceRepoPurge when the repository was
        \* soft-deleted less than MinRepoPurgeAge ago.
    CutoffCoversRetention,
        \* PurgeExpired lists only repositories deleted before
        \* now - max(retention, MinRepoPurgeAge).
    CursorWraps,
        \* PurgeExpired relists from the start when its batch is empty and
        \* its cursor is not: a failed repository is retried.
    FenceFirst,
        \* purgeRepo calls FenceRepoPurge before DropRepoRefs.
    RefsDieFirst,
        \* purgeRepo calls DropRepoRefs before RetireRepoPacks and the reap.
    RegistrationsFenced,
        \* RegisterPacks (and RegisterLFSObject) read the fence in the same
        \* transaction as the insert, ordered against FenceRepoPurge, on PG,
        \* SQLite and S3WAL.
    RefWritesFenced,
        \* CASRef reads the fence in the same transaction as the ref write,
        \* ordered against FenceRepoPurge, on PG, SQLite and S3WAL.
    FoldsFenced,
        \* ReplacePacks reads the fence in the same transaction as its
        \* writes, ordered against FenceRepoPurge, on PG, SQLite and S3WAL.
    SweepReapsFenced,
        \* classify reaps an unregistered blob of a fenced repository under
        \* purgeMode (PurgedRepoProbe).
    HealPurgesFenced
        \* healQuarantine purges aged backups of fenced repositories under
        \* purgeMode with every delete flag off.

ASSUME NoNonce \notin Nonces
ASSUME BgPurgers \subseteq Purgers
ASSUME Purgers \cap Sweepers = {}
ASSUME MaxFaults \in Nat

Procs == Purgers \cup Sweepers

VARIABLES
    repo,        \* "live" | "young" | "old" | "expired": deletion age
    fenced,      \* purged_repos row (S3WAL: manifest PurgedAt) for the base
    stamped,     \* repos.purged_at: the purge's completion commit
    present,     \* K's blob exists under the repository's prefix
    registered,  \* K has a pack registry row
    oldRows,     \* the repository has pack rows of other keys
    refRow,      \* a ref row whose commit needs K's objects
    backup,      \* backup[n]: "unused" | "fresh" | "aged" | "gone"
    wpc,         \* writer step
    regOk,       \* the writer's registration returned no error
    pc,          \* purger and sweeper step
    hs,          \* safeDelete: this handshake's backup
    guardOk,     \* this pass's DeleteGuard returned no error
    purgeMode,   \* sweeper: purge-aware reaping is on for this run
    spared,      \* purger: reapPrefix spared a registered blob this run
    rescan,      \* purger: this run is rescanPurged (repo already stamped)
    cursorPast,  \* background purger: its cursor is past the repository
    todo,        \* sweeper heal: listed backups not visited yet
    cur,         \* sweeper heal: backup being visited
    faults

writerVars == <<wpc, regOk>>
procVars == <<pc, hs, guardOk, purgeMode, spared, rescan, cursorPast, todo, cur>>
vars == <<repo, fenced, stamped, present, registered, oldRows, refRow, backup,
          faults, writerVars, procVars>>

\* purgeRepo's phases in code order. A FALSE order fact moves its phase
\* after the reap, the latest place before CountRepoRows.
Early == (IF FenceFirst THEN <<"fence">> ELSE <<>>)
         \o (IF RefsDieFirst THEN <<"refs">> ELSE <<>>)
         \o <<"retire">>
Late == (IF FenceFirst THEN <<>> ELSE <<"fence">>)
        \o (IF RefsDieFirst THEN <<>> ELSE <<"refs">>)
Phases == Early \o <<"reap">> \o Late \o <<"count", "stamp">>
After(ph) == LET i == CHOOSE i \in 1..Len(Phases) : Phases[i] = ph
             IN Phases[i + 1]

WriterSteps == {"idle", "upload", "register", "verify", "fverify", "cas",
                "done", "failed", "crashed"}
Steps == {"idle", "off", "state", "fence", "refs", "retire", "reap",
          "count", "stamp", "heal_next", "heal_stat", "heal_purge", "rv_reg",
          "sd_copy", "sd_delete", "sd_recheck", "sd_restore", "sd_restore_err"}
Handshake == {"sd_delete", "sd_recheck", "sd_restore", "sd_restore_err"}
Live == {"fresh", "aged"}
Deleted == {"young", "old", "expired"}

\* A writer that still holds its local pack file and will verify it.
InFlight(w) == wpc[w] \in {"upload", "register", "verify", "fverify"}

\* Steps that change durable state (the eligibility reads come before).
Destructive == {"fence", "refs", "retire", "stamp"} \cup
               {"sd_copy", "sd_delete", "sd_recheck", "sd_restore",
                "sd_restore_err"}

TypeOK ==
    /\ repo \in {"live"} \cup Deleted
    /\ fenced \in BOOLEAN /\ stamped \in BOOLEAN
    /\ present \in BOOLEAN /\ registered \in BOOLEAN
    /\ oldRows \in BOOLEAN /\ refRow \in BOOLEAN
    /\ backup \in [Nonces -> {"unused", "fresh", "aged", "gone"}]
    /\ wpc \in [Writers -> WriterSteps]
    /\ regOk \in [Writers -> BOOLEAN]
    /\ pc \in [Procs -> Steps]
    /\ hs \in [Procs -> Nonces \cup {NoNonce}]
    /\ guardOk \in [Procs -> BOOLEAN]
    /\ purgeMode \in [Sweepers -> BOOLEAN]
    /\ spared \in [Purgers -> BOOLEAN]
    /\ rescan \in [Purgers -> BOOLEAN]
    /\ cursorPast \in [Purgers -> BOOLEAN]
    /\ todo \in [Sweepers -> SUBSET Nonces]
    /\ cur \in [Sweepers -> Nonces \cup {NoNonce}]
    /\ faults \in 0..MaxFaults

Init ==
    \* The repository is live or freshly soft-deleted. K is absent, an
    \* unregistered blob, or registered with its bytes, and a ref may need it.
    /\ repo \in {"live", "young"}
    /\ fenced = FALSE /\ stamped = FALSE
    /\ present \in BOOLEAN /\ registered \in BOOLEAN /\ oldRows \in BOOLEAN
    /\ refRow \in BOOLEAN
    /\ registered => present
    /\ refRow => registered
    /\ backup = [n \in Nonces |-> "unused"]
    /\ wpc = [w \in Writers |-> "idle"]
    /\ regOk = [w \in Writers |-> FALSE]
    /\ pc = [p \in Procs |-> "idle"]
    /\ hs = [p \in Procs |-> NoNonce]
    /\ guardOk = [p \in Procs |-> FALSE]
    /\ purgeMode = [s \in Sweepers |-> FALSE]
    /\ spared = [p \in Purgers |-> FALSE]
    /\ rescan = [p \in Purgers |-> FALSE]
    /\ cursorPast = [p \in Purgers |-> FALSE]
    /\ todo = [s \in Sweepers |-> {}]
    /\ cur = [s \in Sweepers |-> NoNonce]
    /\ faults = 0

-----------------------------------------------------------------------------
(* Writers. A write resolved the repository before its soft-delete, or     *)
(* runs a REST or repack step later: it may start at any time and take     *)
(* any time. A writer can crash at any step in flight (WriterCrash); that  *)
(* also stands for every error return that ends the write.                 *)

W(w, step) == wpc' = [wpc EXCEPT ![w] = step]
Finish(w, ok) == W(w, IF ok THEN "done" ELSE "failed")
rowVars == <<repo, fenced, stamped, present, registered, oldRows, refRow>>

\* bl.Stat / s.blobStore.Stat: a present key is adopted without an upload.
WriterStat(w) ==
    /\ wpc[w] = "idle"
    /\ W(w, IF present THEN "register" ELSE "upload")
    /\ UNCHANGED <<rowVars, backup, faults, regOk, procVars>>

\* REST branch/tag creation: CASRef names a commit already in K, with no
\* pack registration.
WriterRefOnly(w) ==
    /\ wpc[w] = "idle" /\ registered
    /\ W(w, "cas")
    /\ UNCHANGED <<rowVars, backup, faults, regOk, procVars>>

\* HasPack, then UploadFile. Merged as in PackSweep: an upload is always
\* allowed, and a probe that finds K registered skips it.
WriterUpload(w) ==
    /\ wpc[w] = "upload"
    /\ \/ present' = TRUE
       \/ registered /\ UNCHANGED present
    /\ W(w, "register")
    /\ UNCHANGED <<repo, fenced, stamped, registered, oldRows, refRow, backup, faults, regOk, procVars>>

\* st.RegisterPacks / s.refStore.RegisterPacks. With RegistrationsFenced the
\* transaction reads the fence: a fenced repository fails it with
\* ErrRepoPurged and no row. Otherwise an error can be ambiguous (the row
\* may have committed). One call may register other keys too.
RegisterPacks(w) ==
    /\ IF fenced /\ RegistrationsFenced
       THEN /\ regOk' = [regOk EXCEPT ![w] = FALSE]
            /\ UNCHANGED <<registered, oldRows>>
       ELSE \/ /\ registered' = TRUE
               /\ oldRows' \in {oldRows, TRUE}
               /\ regOk' = [regOk EXCEPT ![w] = TRUE]
            \/ /\ registered' \in {registered, TRUE}
               /\ UNCHANGED oldRows
               /\ regOk' = [regOk EXCEPT ![w] = FALSE]
    /\ IF WriterVerifies /\ (regOk'[w] \/ VerifyOnRegisterError)
       THEN W(w, "verify")
       ELSE IF regOk'[w] THEN W(w, "cas") ELSE Finish(w, FALSE)

\* r.store.ReplacePacks(repo, old, K): a repack folds old rows into K. It
\* commits only while every old row still exists, and RetireRepoPacks
\* deletes them all. With FoldsFenced the transaction also reads the fence,
\* and a fenced repository fails it with ErrRepoPurged and no row. The
\* repack may still verify K after a failed swap; the verify step covers it.
ReplacePacks(w) ==
    /\ IF ~(fenced /\ FoldsFenced) /\ oldRows
       THEN /\ registered' = TRUE /\ oldRows' = FALSE
            /\ regOk' = [regOk EXCEPT ![w] = TRUE]
       ELSE /\ UNCHANGED <<registered, oldRows>>
            /\ regOk' = [regOk EXCEPT ![w] = FALSE]
    /\ IF WriterVerifies THEN W(w, "fverify") ELSE Finish(w, regOk'[w])

WriterRegister(w) ==
    /\ wpc[w] = "register"
    /\ RegisterPacks(w) \/ ReplacePacks(w)
    /\ UNCHANGED <<repo, fenced, stamped, present, refRow, backup, faults,
                   procVars>>

\* blob.EnsurePublished / verifyPublished: re-upload K when Stat says it is
\* missing. A push goes on to its ref update only after a registration that
\* returned no error (preReceive fails the push otherwise).
WriterVerify(w) ==
    /\ wpc[w] \in {"verify", "fverify"}
    /\ present' = TRUE
    /\ IF wpc[w] = "verify" /\ regOk[w] THEN W(w, "cas") ELSE Finish(w, regOk[w])
    /\ UNCHANGED <<repo, fenced, stamped, registered, oldRows, refRow, backup, faults, regOk, procVars>>

\* CASRef. With RefWritesFenced the transaction reads the fence and a
\* fenced repository fails it (notExecuted(ErrRepoPurged)). Otherwise it
\* commits, or returns an ambiguous error.
WriterCAS(w) ==
    /\ wpc[w] = "cas"
    /\ IF fenced /\ RefWritesFenced
       THEN /\ UNCHANGED refRow /\ Finish(w, FALSE)
       ELSE \/ refRow' = TRUE /\ Finish(w, TRUE)
            \/ refRow' \in {refRow, TRUE} /\ Finish(w, FALSE)
    /\ UNCHANGED <<repo, fenced, stamped, present, registered, oldRows, backup, faults, regOk, procVars>>

WriterCrash(w) ==
    /\ wpc[w] \in {"upload", "register", "verify", "fverify", "cas"}
    /\ W(w, "crashed")
    /\ UNCHANGED <<rowVars, backup, faults, regOk, procVars>>

WriterStep(w) ==
    \/ WriterStat(w) \/ WriterRefOnly(w) \/ WriterUpload(w)
    \/ WriterRegister(w) \/ WriterVerify(w) \/ WriterCAS(w)

-----------------------------------------------------------------------------
(* Purgers. A run ends at an error return the same way as at a crash       *)
(* (PurgerFail). The background purger runs one pass per sweep tick. The   *)
(* CLI is one purge-repo run, started at any time; a second run adds only  *)
(* states that a background pass reaches too.                              *)

P(p, step) == pc' = [pc EXCEPT ![p] = step]
Bg(p) == p \in BgPurgers
\* Where a purger goes when its run ends.
Rest(p) == IF Bg(p) THEN "idle" ELSE "off"

\* p.meta.RepoPurgeState and purgeRepo's eligibility checks. A stamped
\* repository goes to rescanPurged.
StateRead(p) ==
    IF stamped THEN "reap"
    ELSE IF (repo = "live" /\ RefusesLive) \/ (repo = "young" /\ RefusesYoung)
         THEN Rest(p) ELSE Phases[1]

\* PurgeExpired / PurgeRepo: the DeleteGuard consult, once per pass, merged
\* with the next read (the guard reads nothing the model tracks). A failure
\* counts as a fault, so a guard that fails forever is a fault without
\* bound. The background pass then lists: p.meta.PurgeableRepos(ctx,
\* cutoff, p.cursor, ...), and relists from the start when the batch is
\* empty and the cursor is not. With one repository the cursor is past it
\* after every attempt. The CLI reads the repository's state at once.
PurgerStart(p) ==
    LET eligible == ~stamped /\ repo \in (IF CutoffCoversRetention
                                          THEN {"expired"} ELSE Deleted)
        listed == eligible /\ (~cursorPast[p] \/ CursorWraps)
        go(ok) ==
            IF ~ok /\ PurgeGuarded
            THEN P(p, Rest(p)) /\ UNCHANGED <<cursorPast, rescan>>
            ELSE IF Bg(p)
            THEN /\ P(p, IF listed THEN "state" ELSE "idle")
                 /\ cursorPast' = [cursorPast EXCEPT ![p] =
                                     IF listed THEN TRUE ELSE @ /\ ~CursorWraps]
                 /\ UNCHANGED rescan
            ELSE /\ P(p, StateRead(p))
                 /\ rescan' = [rescan EXCEPT ![p] = stamped]
                 /\ UNCHANGED cursorPast
    IN /\ pc[p] = "idle"
       /\ \/ /\ guardOk' = [guardOk EXCEPT ![p] = TRUE]
             /\ go(TRUE)
             /\ UNCHANGED faults
          \/ /\ faults < MaxFaults /\ faults' = faults + 1
             /\ guardOk' = [guardOk EXCEPT ![p] = FALSE]
             /\ go(FALSE)
       /\ spared' = [spared EXCEPT ![p] = FALSE]
       /\ UNCHANGED <<rowVars, backup, writerVars, hs, purgeMode, todo,
                      cur>>

\* purgeRepo for a listed candidate: p.meta.RepoPurgeState, then the checks.
PurgerState(p) ==
    /\ pc[p] = "state"
    /\ P(p, StateRead(p))
    /\ rescan' = [rescan EXCEPT ![p] = stamped]
    /\ UNCHANGED <<rowVars, backup, faults, writerVars, hs, guardOk,
                   purgeMode, spared, cursorPast, todo, cur>>

\* p.registry.FenceRepoPurge: idempotent and permanent.
PurgerFence(p) ==
    /\ pc[p] = "fence"
    /\ fenced' = TRUE
    /\ P(p, After("fence"))
    /\ UNCHANGED <<repo, stamped, present, registered, oldRows, refRow, backup,
                   faults, writerVars, hs, guardOk, purgeMode, spared,
                   rescan, cursorPast, todo, cur>>

\* p.registry.DropRepoRefs(group).
PurgerRefs(p) ==
    /\ pc[p] = "refs"
    /\ refRow' = FALSE
    /\ P(p, After("refs"))
    /\ UNCHANGED <<repo, fenced, stamped, present, registered, oldRows, backup,
                   faults, writerVars, hs, guardOk, purgeMode, spared,
                   rescan, cursorPast, todo, cur>>

\* p.registry.RetireRepoPacks(group): every pack row of the group becomes a
\* pack_retained tombstone. DropRepoLFSObjects (LFS rows) is out of scope.
PurgerRetire(p) ==
    /\ pc[p] = "retire"
    /\ registered' = FALSE /\ oldRows' = FALSE
    /\ P(p, After("retire"))
    /\ UNCHANGED <<repo, fenced, stamped, present, refRow, backup, faults, writerVars, hs, guardOk, purgeMode, spared, rescan,
                   cursorPast, todo, cur>>

\* Where a process goes when its reap of K ends.
AfterReap(q) ==
    IF q \in Sweepers THEN "idle"
    ELSE IF rescan[q] THEN Rest(q) ELSE After("reap")

\* reapPrefix: p.sweeper.blobs.ListPages over the prefix, then
\* p.sweeper.registered for K. Merged: after the purge's own
\* RetireRepoPacks no registration can land in between. A blob uploaded
\* after the listing is a straggler. A registered key is spared, which
\* forces the purge to defer; an unregistered one goes to safeDelete.
PurgerReap(p) ==
    /\ pc[p] = "reap"
    /\ IF present /\ registered
       THEN spared' = [spared EXCEPT ![p] = TRUE] /\ P(p, AfterReap(p))
       ELSE UNCHANGED spared /\ P(p, IF present THEN "sd_copy" ELSE AfterReap(p))
    /\ UNCHANGED <<rowVars, backup, faults, writerVars, hs, guardOk,
                   purgeMode, rescan, cursorPast, todo, cur>>

\* p.registry.CountRepoRows(group), then the deferral on rows or spares.
PurgerCount(p) ==
    /\ pc[p] = "count"
    /\ P(p, IF registered \/ refRow \/ oldRows \/ spared[p] THEN Rest(p)
            ELSE After("count"))
    /\ UNCHANGED <<rowVars, backup, faults, writerVars, hs, guardOk,
                   purgeMode, spared, rescan, cursorPast, todo, cur>>

\* PurgeRepoLedger, then p.meta.PurgeRepoMeta: stamps purged_at, guarded on
\* the row still being soft-deleted. Merged: the ledger rows are not
\* modeled. A crash between the two is PurgerFail at this step.
PurgerStamp(p) ==
    /\ pc[p] = "stamp"
    /\ stamped' = (stamped \/ repo # "live")
    /\ P(p, Rest(p))
    /\ UNCHANGED <<repo, fenced, present, registered, oldRows, refRow, backup,
                   faults, writerVars, hs, guardOk, purgeMode, spared,
                   rescan, cursorPast, todo, cur>>

\* The run ends early: the process dies, or a call returns an error. A
\* process restart also resets the in-memory cursor.
PurgerFail(p) ==
    /\ pc[p] \notin {"idle", "off"}
    /\ faults < MaxFaults /\ faults' = faults + 1
    /\ P(p, Rest(p))
    /\ hs' = [hs EXCEPT ![p] = NoNonce]
    /\ cursorPast' \in {cursorPast, [cursorPast EXCEPT ![p] = FALSE]}
    /\ UNCHANGED <<rowVars, backup, writerVars, guardOk, purgeMode,
                   spared, rescan, todo, cur>>

PurgerStep(p) ==
    \/ PurgerStart(p) \/ PurgerState(p) \/ PurgerFence(p) \/ PurgerRefs(p)
    \/ PurgerRetire(p) \/ PurgerReap(p) \/ PurgerCount(p) \/ PurgerStamp(p)

-----------------------------------------------------------------------------
(* safeDelete, shared by the purger's reap and the sweeper.                *)

\* A quarantine slot can take a new write-once key once nothing refers to
\* its old one: nonces are random, so the model reuses slots.
Free(n) ==
    /\ backup[n] \in {"unused", "gone"}
    /\ \A q \in Procs : hs[q] # n
    /\ \A s \in Sweepers : cur[s] # n /\ n \notin todo[s]

EndHandshake(q, step) ==
    /\ pc' = [pc EXCEPT ![q] = step]
    /\ hs' = [hs EXCEPT ![q] = NoNonce]

hsVars == <<guardOk, purgeMode, spared, rescan, cursorPast, todo, cur>>

\* s.blobs.Copy(ctx, qKey, key) to a fresh key. ErrNotFound (K is gone)
\* returns without a delete. With every slot in use the model fails the
\* copy: a purge run ends with the error, a sweep goes on to its next run.
QuarantineCopy(q) ==
    /\ pc[q] = "sd_copy"
    /\ IF ~CopyBeforeDelete
       THEN /\ pc' = [pc EXCEPT ![q] = "sd_delete"]
            /\ UNCHANGED <<backup, hs>>
       ELSE IF ~present
            THEN EndHandshake(q, AfterReap(q)) /\ UNCHANGED <<backup>>
            ELSE IF \A n \in Nonces : ~Free(n)
            THEN /\ pc' = [pc EXCEPT ![q] = IF q \in Purgers THEN Rest(q) ELSE "idle"]
                 /\ UNCHANGED <<backup, hs>>
            ELSE \E n \in Nonces :
                   /\ Free(n)
                   /\ backup' = [backup EXCEPT ![n] = "fresh"]
                   /\ hs' = [hs EXCEPT ![q] = n]
                   /\ pc' = [pc EXCEPT ![q] = "sd_delete"]
    /\ UNCHANGED <<rowVars, faults, writerVars, hsVars>>

\* s.blobs.Delete(hctx, key). An error is ambiguous (the delete may have
\* run) and goes to restoreFromQuarantine, then safeDelete returns it.
Delete(q) ==
    /\ pc[q] = "sd_delete"
    /\ \/ /\ present' = FALSE
          /\ IF RecheckAfterDelete
             THEN pc' = [pc EXCEPT ![q] = "sd_recheck"] /\ UNCHANGED hs
             ELSE EndHandshake(q, AfterReap(q))
          /\ UNCHANGED faults
       \/ /\ faults < MaxFaults /\ faults' = faults + 1
          /\ present' \in {present, FALSE}
          /\ pc' = [pc EXCEPT ![q] = "sd_restore_err"]
          /\ UNCHANGED hs
    /\ UNCHANGED <<repo, fenced, stamped, registered, oldRows, refRow, backup,
                   writerVars, hsVars>>

\* s.registered(rctx, repo, key) after the delete.
Recheck(q) ==
    /\ pc[q] = "sd_recheck"
    /\ IF registered
       THEN pc' = [pc EXCEPT ![q] = "sd_restore"] /\ UNCHANGED hs
       ELSE EndHandshake(q, AfterReap(q))
    /\ UNCHANGED <<rowVars, backup, faults, writerVars, hsVars>>

\* restoreFromQuarantine(rctx, key, qKey). After a recheck that found K
\* registered, safeDelete returns (false, nil) and the reap goes on; after
\* an ambiguous delete it returns the error, and reapPrefix's joined error
\* ends the purge run before CountRepoRows.
Restore(q) ==
    /\ pc[q] \in {"sd_restore", "sd_restore_err"}
    /\ IF hs[q] # NoNonce /\ backup[hs[q]] \in Live
       THEN present' = TRUE
       ELSE UNCHANGED present
    /\ EndHandshake(q, IF pc[q] = "sd_restore" THEN AfterReap(q)
                       ELSE IF q \in Purgers THEN Rest(q) ELSE "idle")
    /\ UNCHANGED <<repo, fenced, stamped, registered, oldRows, refRow, backup, faults, writerVars, hsVars>>

HandshakeStep(q) ==
    QuarantineCopy(q) \/ Delete(q) \/ Recheck(q) \/ Restore(q)

-----------------------------------------------------------------------------
(* The purge-aware sweeper: every delete flag off, PurgedRepoProbe wired.  *)

S(s, step) == pc' = [pc EXCEPT ![s] = step]
sweepRest == <<hs, guardOk, purgeMode, spared, rescan, cursorPast>>

\* sweep: the DeleteGuard consult (purgeMode joins the destructive set),
\* then healQuarantine lists the quarantine prefix.
SweepStart(s) ==
    /\ pc[s] = "idle"
    /\ \/ /\ guardOk' = [guardOk EXCEPT ![s] = TRUE]
          /\ purgeMode' = [purgeMode EXCEPT ![s] = TRUE]
          /\ UNCHANGED faults
       \/ /\ faults < MaxFaults /\ faults' = faults + 1
          /\ guardOk' = [guardOk EXCEPT ![s] = FALSE]
          /\ purgeMode' = [purgeMode EXCEPT ![s] = ~SweepGuardStopsPurgeMode]
    /\ todo' = [todo EXCEPT ![s] = {n \in Nonces : backup[n] \in Live}]
    /\ S(s, "heal_next")
    /\ UNCHANGED <<rowVars, backup, writerVars, hs, spared, rescan, cursorPast,
                   cur>>

\* healQuarantine visits the next listed entry, and healQuarantineEntry
\* reads s.registered(ctx, repo, orig). Merged: picking an entry reads
\* nothing. With none left the sweep moves on to classify.
HealNext(s) ==
    /\ pc[s] = "heal_next"
    /\ IF todo[s] = {}
       THEN S(s, "rv_reg") /\ cur' = [cur EXCEPT ![s] = NoNonce] /\ UNCHANGED todo
       ELSE \E n \in todo[s] :
              /\ cur' = [cur EXCEPT ![s] = n]
              /\ todo' = [todo EXCEPT ![s] = @ \ {n}]
              /\ S(s, IF registered THEN "heal_stat" ELSE "heal_purge")
    /\ UNCHANGED <<rowVars, backup, faults, writerVars, sweepRest>>

\* healQuarantineEntry: s.blobs.Stat(ctx, orig), then restoreFromQuarantine
\* for a missing original (merged, as in PackSweep).
HealStat(s) ==
    /\ pc[s] = "heal_stat"
    /\ IF ~present /\ HealRestores /\ backup[cur[s]] \notin Live
       THEN /\ S(s, "heal_next") /\ cur' = [cur EXCEPT ![s] = NoNonce]
            /\ UNCHANGED present
       ELSE /\ S(s, "heal_purge") /\ UNCHANGED cur
            /\ present' = (present \/ HealRestores)
    /\ UNCHANGED <<repo, fenced, stamped, registered, oldRows, refRow, backup, faults, writerVars, sweepRest, todo>>

\* healQuarantineEntry with purgeAll off: the age gate, then
\* s.cfg.PurgedRepoProbe, then s.blobs.Delete(ctx, q.Key). Merged: a fence
\* never goes away, so a probe that reads it earlier reads it here too.
HealPurge(s) ==
    /\ pc[s] = "heal_purge"
    /\ IF /\ HealPurgesFenced /\ purgeMode[s] /\ fenced
          /\ (~PurgeAgeGated \/ backup[cur[s]] = "aged")
       THEN backup' = [backup EXCEPT ![cur[s]] = "gone"]
       ELSE UNCHANGED backup
    /\ S(s, "heal_next")
    /\ cur' = [cur EXCEPT ![s] = NoNonce]
    /\ UNCHANGED <<rowVars, faults, writerVars, sweepRest, todo>>

\* classify (registry read, PurgedRepoProbe) and revalidateOrphan (registry
\* read), merged into the last registry read, as in PackSweep.
SweepClassify(s) ==
    /\ pc[s] = "rv_reg"
    /\ S(s, IF ~registered /\ purgeMode[s] /\ fenced /\ SweepReapsFenced
            THEN "sd_copy" ELSE "idle")
    /\ UNCHANGED <<rowVars, backup, faults, writerVars, sweepRest, todo, cur>>

SweeperCrash(s) ==
    /\ pc[s] # "idle"
    /\ faults < MaxFaults /\ faults' = faults + 1
    /\ S(s, "idle")
    /\ todo' = [todo EXCEPT ![s] = {}]
    /\ cur' = [cur EXCEPT ![s] = NoNonce]
    /\ hs' = [hs EXCEPT ![s] = NoNonce]
    /\ UNCHANGED <<rowVars, backup, writerVars, guardOk, purgeMode, spared,
                   rescan, cursorPast>>

SweeperStep(s) ==
    \/ SweepStart(s) \/ HealNext(s) \/ HealStat(s)
    \/ HealPurge(s) \/ SweepClassify(s)

-----------------------------------------------------------------------------
(* The environment: the soft-delete and time.                              *)

\* DELETE /api/v1/repos/delete: meta SoftDeleteRepo stamps deleted_at once.
SoftDelete ==
    /\ repo = "live" /\ repo' = "young"
    /\ UNCHANGED <<fenced, stamped, present, registered, oldRows, refRow,
                   backup, faults, writerVars, procVars>>

\* The deletion ages past MinRepoPurgeAge, then past the retention.
RepoAges ==
    /\ repo \in {"young", "old"}
    /\ repo' = IF repo = "young" THEN "old" ELSE "expired"
    /\ UNCHANGED <<fenced, stamped, present, registered, oldRows, refRow,
                   backup, faults, writerVars, procVars>>

\* Backup n passes quarantinePurgeAge; PurgeFloorCoversHandshake: not while
\* the handshake that wrote it still runs. No timing assumption: after the
\* fence nothing registers, so no backup of the repository is needed again.
AgeBackup(n) ==
    /\ backup[n] = "fresh"
    /\ PurgeFloorCoversHandshake =>
         ~\E q \in Procs : hs[q] = n /\ pc[q] \in Handshake
    /\ backup' = [backup EXCEPT ![n] = "aged"]
    /\ UNCHANGED <<rowVars, faults, writerVars, procVars>>

ProcStep(q) ==
    \/ q \in Purgers /\ PurgerStep(q)
    \/ q \in Sweepers /\ SweeperStep(q)
    \/ HandshakeStep(q)

Next ==
    \/ \E w \in Writers : WriterStep(w) \/ WriterCrash(w)
    \/ \E q \in Procs : ProcStep(q)
    \/ \E p \in Purgers : PurgerFail(p)
    \/ \E s \in Sweepers : SweeperCrash(s)
    \/ SoftDelete \/ RepoAges
    \/ \E n \in Nonces : AgeBackup(n)

\* Live processes keep taking steps; the background purger and the sweeper
\* run a pass on every tick, the CLI finishes a run it started. Faults are
\* bounded, so the guard passes and runs end cleanly eventually. Time
\* passes. The soft-delete itself needs no fairness.
CliWork(p) == pc[p] \notin {"idle", "off"} /\ ProcStep(p)
Fairness ==
    /\ \A w \in Writers : WF_vars(WriterStep(w))
    /\ \A p \in BgPurgers \cup Sweepers : WF_vars(ProcStep(p))
    /\ \A p \in Purgers \ BgPurgers : WF_vars(CliWork(p))
    /\ WF_vars(RepoAges)
    /\ \A n \in Nonces : WF_vars(AgeBackup(n))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Properties.                                                              *)

\* 1. No purge step touches a live repository or one soft-deleted less than
\* MinRepoPurgeAge ago; the background pass waits for the retention too.
PurgeEligible ==
    \A p \in Purgers :
        pc[p] \in Destructive =>
            repo \in (IF Bg(p) THEN {"expired"} ELSE {"old", "expired"})

\* 2. No destructive step starts in a pass whose DeleteGuard returned an
\* error.
GuardRespected ==
    /\ \A p \in Purgers : pc[p] \in Destructive => guardOk[p]
    /\ \A s \in Sweepers :
         pc[s] \in Destructive \/ (pc[s] = "heal_purge" /\ purgeMode[s])
           => guardOk[s]

\* 3. Tombstone-then-delete, the purge form of NoLostPack: no committed ref
\* row or registered pack row points at bytes that have no copy (K, a
\* quarantine backup, or an in-flight writer's local pack file), and no ref
\* row outlives the pack row of the objects it names.
Recoverable ==
    \/ present
    \/ \E n \in Nonces : backup[n] \in Live
    \/ WriterVerifies /\ \E w \in Writers : InFlight(w)
NoDanglingRow ==
    /\ (registered \/ refRow) => Recoverable
    /\ refRow => registered

\* 4. purged_at is stamped only over an empty group, and no registration of
\* the repository commits after it.
StampedClean == stamped => ~registered /\ ~oldRows /\ ~refRow

\* 5. A purge of a soft-deleted repository resumes after any crash, stamps
\* purged_at, and leaves no blob under the repository's prefixes.
PurgeCompletes == <>[](repo = "live" \/ (stamped /\ ~present))

\* The quarantine backups the purge made are collected too, with every
\* delete flag off (SweepConfig.PurgedRepoProbe).
BackupsCollected == <>[](\A n \in Nonces : backup[n] \notin Live)

PurgeSymmetry == Permutations(Writers) \cup Permutations(Nonces)

\* Safety configs fingerprint states through this view. It drops values
\* that no step reads before it overwrites them: the per-run variables of
\* an idle process, regOk outside a verify step, and the cursor of the CLI. A purged slot behaves like
\* an unused one, and an ended write is the same whether it succeeded,
\* failed or crashed. Every invariant reads only what the view keeps.
Idle(p) == pc[p] \in {"idle", "off"}
View == <<repo, fenced, stamped, present, registered, oldRows, refRow, faults,
          [n \in Nonces |-> IF backup[n] = "gone" THEN "unused" ELSE backup[n]],
          [w \in Writers |-> IF wpc[w] \in {"done", "failed", "crashed"}
                              THEN "ended" ELSE wpc[w]],
          [w \in Writers |-> wpc[w] \in {"verify", "fverify"} /\ regOk[w]],
          pc, hs, todo, cur,
          [p \in Procs |-> ~Idle(p) /\ guardOk[p]],
          [s \in Sweepers |-> ~Idle(s) /\ purgeMode[s]],
          [p \in Purgers |-> ~Idle(p) /\ spared[p]],
          [p \in Purgers |-> ~Idle(p) /\ rescan[p]],
          [p \in Purgers |-> Bg(p) /\ cursorPast[p]]>>
=============================================================================
