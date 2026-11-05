---------------------------- MODULE LaneSchedule ----------------------------
(***************************************************************************)
(* The push lane of one repository's lane row (ADR-50), the writers that   *)
(* record forward intent on it, and the runs that forward the repository   *)
(* upstream. Each action is one atomic step of the code: one row           *)
(* transaction on SQLite, one meta.json CAS on S3-WAL, one hook step, or   *)
(* one run step. SKILL.md maps every action to its Go call.                *)
(*                                                                         *)
(* One ref with versions 0..MaxVersion: every accepted write is one       *)
(* larger number, so "the upstream has write v" is upstream >= v.          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Writers,           \* git pushes; each writes the ref once
    QuietWriters,      \* git pushes that record no intent (forward denied, sync loopback)
    RestWriters,       \* REST writes in serve; each writes the ref once
    QuietRestWriters,  \* REST writes that record no intent (forward denied)
    Runners,           \* push runs (any process); a runner runs one at a time
    ApiCalls,          \* explicit push-upstream calls: a subset of {"plain", "force"}
    MaxRetries,        \* MaxPushTransientRetries, scaled down
    MaxOutages,        \* transient upstream failures in one behavior
    MaxFaults,         \* run crashes, failed finish writes, shutdown cancels, writer crashes
    \* Assumptions the code relies on; each probe config drops one.
    LeaseOutlivesRun,  \* a claim expires only after its run ended
    WritesComplete,    \* a REST write whose authority CAS won records its intent
    \* Code facts (facts/facts.go); FALSE takes the broken branch.
    ClaimConsumesPending, ClaimMarksDue, DoneKeepsPending, RetryRemarks, FinishKeepsExplicit,
    StealInheritsForce, RunPassesForce, WaitBeforeSnapshot, ShutdownRestores,
    MarkBeforeCas, StrictSnapshot

ASSUME ApiCalls \subseteq {"plain", "force"}
ASSUME QuietWriters \subseteq Writers /\ QuietRestWriters \subseteq RestWriters
ASSUME MaxRetries \in Nat /\ MaxOutages \in Nat /\ MaxFaults \in Nat

NoOne == "none"
MaxVersion == Cardinality(Writers) + Cardinality(RestWriters)
Max(a, b) == IF a >= b THEN a ELSE b
Level(force) == IF force THEN 2 ELSE 1

Released == [held |-> FALSE, holder |-> NoOne, force |-> FALSE,
             expired |-> FALSE, dead |-> FALSE, owes |-> FALSE]

VARIABLES
    authority,     \* the ref store's value (the authority CAS)
    cacheRef,      \* the cache repo's ref, which a loopback fetch serves
    hold,          \* writer whose live push hold names the ref, or NoOne
    wState, wVer,  \* writer phase and the version its CAS wrote
    restState,     \* REST writer phase
    restVer,       \* the version each REST write's CAS wrote
    pending,       \* push_pending: 0, 1 (plain) or 2 (force)
    due,           \* push_due: "zero", "claim" (claim time), "future", "elapsed"
    attempt,       \* push_attempt
    claim,         \* push_syncing, holder, push_claim_force, lease state
    rState, rSnap, rForce, rAttempt,
    upstream,      \* the upstream's ref
    apiLeft, outages, faults,
    \* History variables: they only feed the properties.
    forceOwed,     \* a force request that no claim has taken yet
    explicitOwed,  \* an explicit request that no claim has taken yet
    spent,         \* a run gave up after its retry budget, and no request came since
    rewound,       \* a push moved the upstream backwards
    hammered,      \* a retry or restore with no explicit request left the lane due now
    owed           \* the newest version the lane must forward: a write by a caller
                   \* that may forward, or a push-upstream call

writerVars == <<authority, cacheRef, hold, wState, wVer, restState, restVer>>
rowVars == <<pending, due, attempt, claim>>
runVars == <<rState, rSnap, rForce, rAttempt>>
envVars == <<upstream, apiLeft, outages, faults>>
ghostVars == <<forceOwed, explicitOwed, spent, rewound, hammered, owed>>
vars == <<writerVars, rowVars, runVars, envVars, ghostVars>>

TypeOK ==
    /\ authority \in 0..MaxVersion
    /\ cacheRef \in 0..MaxVersion
    /\ upstream \in 0..MaxVersion
    /\ hold \in Writers \cup {NoOne}
    /\ wState \in [Writers -> {"idle", "held", "marked", "cas", "poked", "done", "dead"}]
    /\ restState \in [RestWriters -> {"idle", "cas", "done", "dead"}]
    /\ pending \in 0..2
    /\ due \in {"zero", "claim", "future", "elapsed"}
    /\ attempt \in 0..MaxRetries
    /\ claim.held \in BOOLEAN /\ claim.holder \in Runners \cup {NoOne}
    /\ rState \in [Runners -> {"idle", "claimed", "waited", "snapped", "passed",
                               "ok", "rejected", "transient"}]

Init ==
    /\ authority = 0 /\ cacheRef = 0 /\ hold = NoOne
    /\ wState = [w \in Writers |-> "idle"] /\ wVer = [w \in Writers |-> 0]
    /\ restState = [x \in RestWriters |-> "idle"] /\ restVer = [x \in RestWriters |-> 0]
    /\ pending = 0 /\ due = "zero" /\ attempt = 0 /\ claim = Released
    /\ rState = [r \in Runners |-> "idle"] /\ rSnap = [r \in Runners |-> 0]
    /\ rForce = [r \in Runners |-> FALSE] /\ rAttempt = [r \in Runners |-> 0]
    /\ upstream = 0 /\ apiLeft = ApiCalls /\ outages = 0 /\ faults = 0
    /\ forceOwed = FALSE /\ explicitOwed = FALSE /\ spent = FALSE /\ rewound = FALSE
    /\ hammered = FALSE /\ owed = 0

-----------------------------------------------------------------------------
(* Writers: githttp preReceive (or the update hook) reserves the hold;   *)
(* the update hook records the intent (RequestPush through                 *)
(* events.RefStore's BeforeCAS), wins the authority CAS (CASRefGen), and   *)
(* pokes again (emitPush); receive-pack commits the cache ref and the      *)
(* reference-transaction hook releases the hold.                           *)

\* receive-pack's hydrate ran before the reservation, so the cache has the
\* authority's value then, or an older one if a REST write or a push that
\* died after its CAS landed in between, or another hold kept hydration off.
WReserve(w) ==
    /\ wState[w] = "idle" /\ hold = NoOne
    /\ hold' = w
    /\ cacheRef' \in cacheRef..authority
    /\ wState' = [wState EXCEPT ![w] = "held"]
    /\ UNCHANGED <<authority, wVer, restState, restVer, rowVars, runVars, envVars, ghostVars>>

\* The intent before the CAS (MarkBeforeCas): enqueueSyncOrForward's
\* RequestPush, unless the caller may not forward (forwardDenied) or the
\* push is a sync run's own loopback push (EnvSyncClaim). Those quiet
\* pushes record nothing. A failed write refuses the update (WAbort).
WMark(w) ==
    /\ wState[w] = "held"
    /\ wState' = [wState EXCEPT ![w] = "marked"]
    /\ IF w \in QuietWriters \/ ~MarkBeforeCas
       THEN UNCHANGED <<pending, spent>>
       ELSE /\ pending' = Max(pending, 1)
            /\ spent' = FALSE
    /\ UNCHANGED <<authority, cacheRef, hold, wVer, restState, restVer, due, attempt, claim,
                   runVars, envVars, forceOwed, explicitOwed, rewound, hammered, owed>>

WCas(w) ==
    /\ wState[w] = "marked"
    /\ authority' = authority + 1
    /\ wVer' = [wVer EXCEPT ![w] = authority + 1]
    /\ wState' = [wState EXCEPT ![w] = "cas"]
    /\ owed' = IF w \in QuietWriters THEN owed ELSE authority + 1
    /\ UNCHANGED <<cacheRef, hold, restState, restVer, rowVars, runVars, envVars,
                   forceOwed, explicitOwed, spent, rewound, hammered>>

\* emitPush's poke after the CAS: the same RequestPush behind the same gates.
WPoke(w) ==
    /\ wState[w] = "cas"
    /\ wState' = [wState EXCEPT ![w] = "poked"]
    /\ IF w \in QuietWriters
       THEN UNCHANGED <<pending, spent>>
       ELSE /\ pending' = Max(pending, 1)
            /\ spent' = FALSE
    /\ UNCHANGED <<authority, cacheRef, hold, wVer, restState, restVer, due, attempt, claim,
                   runVars, envVars, forceOwed, explicitOwed, rewound, hammered, owed>>

WPublish(w) ==
    /\ wState[w] = "poked"
    /\ cacheRef' = Max(cacheRef, wVer[w])
    /\ hold' = IF hold = w THEN NoOne ELSE hold
    /\ wState' = [wState EXCEPT ![w] = "done"]
    /\ UNCHANGED <<authority, wVer, restState, restVer, rowVars, runVars, envVars, ghostVars>>

\* The push's intent write or CAS fails (a stale base, a policy denial, a
\* metadata error), or the push dies before its CAS. Its hold goes with
\* it, and nothing was accepted. A recorded intent stays: one extra run.
WAbort(w) ==
    /\ wState[w] \in {"held", "marked"}
    /\ hold' = IF hold = w THEN NoOne ELSE hold
    /\ wState' = [wState EXCEPT ![w] = "dead"]
    /\ UNCHANGED <<authority, cacheRef, wVer, restState, restVer, rowVars, runVars,
                   envVars, ghostVars>>

\* The push dies after its CAS: the hook or receive-pack is killed (serve's
\* shutdown kills git children too), or the poke's RequestPush fails, which
\* emitPush only logs as "sync poke lost". Its hold goes with it, and the
\* cache ref stays stale until a read hydrates it.
WCrash(w) ==
    /\ faults < MaxFaults
    /\ wState[w] \in {"cas", "poked"}
    /\ hold' = IF hold = w THEN NoOne ELSE hold
    /\ wState' = [wState EXCEPT ![w] = "dead"]
    /\ faults' = faults + 1
    /\ UNCHANGED <<authority, cacheRef, wVer, restState, restVer, rowVars, runVars,
                   upstream, apiLeft, outages, ghostVars>>

-----------------------------------------------------------------------------
(* REST writes: an internal/api handler's CASRef through events.RefStore   *)
(* changes the authority only (no hold, no cache update), then emitPush    *)
(* records the intent through the serve queue's EnqueuePush, unless the    *)
(* caller may not forward (forwardDeniedFromContext).                      *)

RCas(x) ==
    /\ restState[x] = "idle"
    /\ authority' = authority + 1
    /\ restVer' = [restVer EXCEPT ![x] = authority + 1]
    /\ restState' = [restState EXCEPT ![x] = "cas"]
    /\ owed' = IF x \in QuietRestWriters THEN owed ELSE authority + 1
    /\ UNCHANGED <<cacheRef, hold, wState, wVer, rowVars, runVars, envVars,
                   forceOwed, explicitOwed, spent, rewound, hammered>>

RMark(x) ==
    /\ restState[x] = "cas"
    /\ restState' = [restState EXCEPT ![x] = "done"]
    /\ IF x \in QuietRestWriters
       THEN UNCHANGED <<pending, spent>>
       ELSE /\ pending' = Max(pending, 1)
            /\ spent' = FALSE
    /\ UNCHANGED <<authority, cacheRef, hold, wState, wVer, restVer, due, attempt, claim,
                   runVars, envVars, forceOwed, explicitOwed, rewound, hammered, owed>>

\* Serve dies, or RequestPush fails, between the CAS and the intent write.
\* WritesComplete rules this out (NoCompletionAssumption.cfg drops it).
RCrashRest(x) ==
    /\ ~WritesComplete /\ faults < MaxFaults
    /\ restState[x] = "cas"
    /\ restState' = [restState EXCEPT ![x] = "dead"]
    /\ faults' = faults + 1
    /\ UNCHANGED <<authority, cacheRef, hold, wState, wVer, restVer, rowVars, runVars,
                   upstream, apiLeft, outages, ghostVars>>

-----------------------------------------------------------------------------
(* Explicit push-upstream (RequestPushNow) and time.                       *)

Api(a) ==
    /\ a \in apiLeft
    /\ apiLeft' = apiLeft \ {a}
    /\ pending' = Max(pending, Level(a = "force"))
    /\ due' = "zero"
    /\ explicitOwed' = TRUE
    /\ forceOwed' = (forceOwed \/ a = "force")
    /\ spent' = FALSE
    /\ owed' = Max(owed, authority)
    /\ UNCHANGED <<writerVars, attempt, claim, runVars, upstream, outages,
                   faults, rewound, hammered>>

\* A backoff ends: push_due <= now.
Tick ==
    /\ due = "future"
    /\ due' = "elapsed"
    /\ UNCHANGED <<writerVars, pending, attempt, claim, runVars, envVars, ghostVars>>

\* A lease expires. With LeaseOutlivesRun only a dead run's claim expires.
Expire ==
    /\ claim.held /\ ~claim.expired
    /\ claim.dead \/ ~LeaseOutlivesRun
    /\ claim' = [claim EXCEPT !.expired = TRUE]
    /\ UNCHANGED <<writerVars, pending, due, attempt, runVars, envVars, ghostVars>>

-----------------------------------------------------------------------------
(* Runs: Worker.RunPush on the local schedule.                             *)

Holds(r) == claim.held /\ claim.holder = r

\* ClaimDuePush: beginPush with requireDue.
Claimable ==
    \/ ~claim.held /\ pending > 0 /\ due \in {"zero", "elapsed"}
    \/ claim.held /\ claim.expired

RClaim(r) ==
    /\ rState[r] = "idle" /\ Claimable
    /\ LET steal == claim.held
           f == pending = 2 \/ (StealInheritsForce /\ steal /\ claim.force)
       IN /\ claim' = [held |-> TRUE, holder |-> r, force |-> f, expired |-> FALSE,
                       dead |-> FALSE, owes |-> (forceOwed \/ (steal /\ claim.owes))]
          /\ rForce' = [rForce EXCEPT ![r] = f]
    /\ pending' = IF ClaimConsumesPending THEN 0 ELSE pending
    /\ due' = IF ClaimMarksDue THEN "claim" ELSE due
    /\ rState' = [rState EXCEPT ![r] = "claimed"]
    /\ rAttempt' = [rAttempt EXCEPT ![r] = attempt]
    /\ forceOwed' = FALSE
    /\ explicitOwed' = FALSE
    /\ UNCHANGED <<writerVars, attempt, rSnap, envVars, spent, rewound, hammered, owed>>

\* End-of-run writes (FinishPushOutcome). Each is guarded by the holder:
\* a run that lost its claim abandons without writing.
NextDue == IF FinishKeepsExplicit /\ due = "zero" THEN "zero" ELSE "future"
Remark(r) == IF RetryRemarks THEN Max(pending, Level(RunPassesForce /\ rForce[r]))
             ELSE pending

Done(r) ==
    /\ claim' = Released
    /\ attempt' = 0
    /\ due' = "zero"
    /\ pending' = IF DoneKeepsPending THEN pending ELSE 0
    /\ rState' = [rState EXCEPT ![r] = "idle"]
    /\ UNCHANGED <<forceOwed, hammered>>

Retry(r) ==
    /\ claim' = Released
    /\ attempt' = attempt + 1
    /\ due' = NextDue
    /\ pending' = Remark(r)
    /\ forceOwed' = (forceOwed \/ claim.owes)
    /\ hammered' = (hammered \/ (~explicitOwed /\ NextDue = "zero"))
    /\ rState' = [rState EXCEPT ![r] = "idle"]

Restore(r) ==
    /\ claim' = Released
    /\ due' = NextDue
    /\ pending' = Remark(r)
    /\ forceOwed' = (forceOwed \/ claim.owes)
    /\ hammered' = (hammered \/ (~explicitOwed /\ NextDue = "zero"))
    /\ rState' = [rState EXCEPT ![r] = "idle"]
    /\ UNCHANGED attempt

Abandon(r) ==
    /\ rState' = [rState EXCEPT ![r] = "idle"]
    /\ UNCHANGED <<rowVars, forceOwed, hammered>>

\* waitPublication: blocks while a live hold names the ref.
RWait(r) ==
    /\ rState[r] = "claimed"
    /\ WaitBeforeSnapshot => hold = NoOne
    /\ rState' = [rState EXCEPT ![r] = "waited"]
    /\ UNCHANGED <<writerVars, rowVars, rSnap, rForce, rAttempt, envVars, ghostVars>>

\* The wait times out (localPublicationWaitTimeout): restoreLocal.
RWaitFail(r) ==
    /\ rState[r] = "claimed" /\ WaitBeforeSnapshot /\ hold # NoOne
    /\ IF Holds(r) THEN Restore(r) ELSE Abandon(r)
    /\ UNCHANGED <<writerVars, rSnap, rForce, rAttempt, envVars,
                   explicitOwed, spent, rewound, owed>>

\* transferPush's loopback fetch. Hydration copies the authority into the
\* cache unless a live hold names the ref.
StaleHeld == hold # NoOne /\ cacheRef # authority

RSnap(r) ==
    /\ rState[r] = "waited"
    /\ ~(StrictSnapshot /\ StaleHeld)
    /\ LET v == IF hold = NoOne THEN authority ELSE cacheRef
       IN /\ rSnap' = [rSnap EXCEPT ![r] = v]
          /\ cacheRef' = v
    /\ rState' = [rState EXCEPT ![r] = "snapped"]
    /\ UNCHANGED <<authority, hold, wState, wVer, restState, restVer, rowVars, rForce, rAttempt,
                   envVars, ghostVars>>

\* StrictSnapshot: the strict hydrate refuses a held ref older than the
\* authority (423 Locked), and RunPush restores through restoreLocal.
RSnapHeld(r) ==
    /\ rState[r] = "waited" /\ StrictSnapshot /\ StaleHeld
    /\ IF Holds(r) THEN Restore(r) ELSE Abandon(r)
    /\ UNCHANGED <<writerVars, rSnap, rForce, rAttempt, envVars,
                   explicitOwed, spent, rewound, owed>>

\* The detach/steal barrier before the upstream push.
RBarrier(r) ==
    /\ rState[r] = "snapped"
    /\ rState' = [rState EXCEPT ![r] = IF Holds(r) THEN "passed" ELSE "idle"]
    /\ UNCHANGED <<writerVars, rowVars, rSnap, rForce, rAttempt, envVars, ghostVars>>

\* git push --atomic: a plain push applies only as a fast-forward.
RPush(r) ==
    /\ rState[r] = "passed"
    /\ IF rForce[r] \/ rSnap[r] >= upstream
       THEN /\ upstream' = rSnap[r]
            /\ rewound' = (rewound \/ rSnap[r] < upstream)
            /\ rState' = [rState EXCEPT ![r] = "ok"]
       ELSE /\ rState' = [rState EXCEPT ![r] = "rejected"]
            /\ UNCHANGED <<upstream, rewound>>
    /\ UNCHANGED <<writerVars, rowVars, rSnap, rForce, rAttempt, apiLeft,
                   outages, faults, forceOwed, explicitOwed, spent, hammered, owed>>

\* The upstream is unreachable (FailUnreachable).
RTransient(r) ==
    /\ rState[r] = "passed" /\ outages < MaxOutages
    /\ outages' = outages + 1
    /\ rState' = [rState EXCEPT ![r] = "transient"]
    /\ UNCHANGED <<writerVars, rowVars, rSnap, rForce, rAttempt, upstream,
                   apiLeft, faults, ghostVars>>

\* localPushOutcome, then FinishPushOutcome.
RFinish(r) ==
    /\ rState[r] \in {"ok", "rejected", "transient"}
    /\ IF ~Holds(r) THEN Abandon(r) /\ UNCHANGED spent
       ELSE IF rState[r] = "transient" /\ rAttempt[r] < MaxRetries
            THEN Retry(r) /\ UNCHANGED spent
            ELSE Done(r) /\ spent' = (spent \/ rState[r] = "transient")
    /\ UNCHANGED <<writerVars, rSnap, rForce, rAttempt, envVars, explicitOwed, rewound, owed>>

\* LocalScheduler.Drain cancels the run with ErrShutdown. A run still in
\* its publication wait always restores (restoreLocal); a later one
\* restores only through the shutdownAborted branch of RunPush.
RCancel(r) ==
    /\ rState[r] \in {"claimed", "waited", "snapped", "passed"}
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ IF ~Holds(r) THEN Abandon(r)
       ELSE IF rState[r] = "claimed" \/ ShutdownRestores THEN Restore(r)
       ELSE Done(r)
    /\ UNCHANGED <<writerVars, rSnap, rForce, rAttempt, upstream, apiLeft,
                   outages, explicitOwed, spent, rewound, owed>>

\* The run's process dies, or every retry of its finish write fails. The
\* claim stays in the row until its lease expires.
RCrash(r) ==
    /\ rState[r] # "idle" /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ rState' = [rState EXCEPT ![r] = "idle"]
    /\ claim' = IF Holds(r) THEN [claim EXCEPT !.dead = TRUE] ELSE claim
    /\ UNCHANGED <<writerVars, pending, due, attempt, rSnap, rForce, rAttempt,
                   upstream, apiLeft, outages, ghostVars>>

-----------------------------------------------------------------------------

Next ==
    \/ \E w \in Writers : \/ WReserve(w) \/ WMark(w) \/ WAbort(w) \/ WCas(w) \/ WPoke(w)
                          \/ WPublish(w) \/ WCrash(w)
    \/ \E x \in RestWriters : RCas(x) \/ RMark(x) \/ RCrashRest(x)
    \/ \E a \in ApiCalls : Api(a)
    \/ Tick \/ Expire
    \/ \E r \in Runners : \/ RClaim(r) \/ RWait(r) \/ RWaitFail(r) \/ RSnap(r) \/ RSnapHeld(r)
                          \/ RBarrier(r) \/ RPush(r) \/ RTransient(r) \/ RFinish(r)
                          \/ RCancel(r) \/ RCrash(r)

Fairness ==
    /\ \A w \in Writers : /\ WF_vars(WMark(w)) /\ WF_vars(WCas(w)) /\ WF_vars(WPoke(w))
                          /\ WF_vars(WPublish(w))
    /\ \A x \in RestWriters : WF_vars(RMark(x))
    /\ \A r \in Runners : /\ WF_vars(RWait(r)) /\ WF_vars(RSnap(r)) /\ WF_vars(RSnapHeld(r))
                          /\ WF_vars(RBarrier(r))
                          /\ WF_vars(RPush(r)) /\ WF_vars(RFinish(r))
    /\ WF_vars(\E r \in Runners : RClaim(r))
    /\ WF_vars(Tick) /\ WF_vars(Expire)

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties.                                                             *)

\* Nothing is outstanding: no write in flight, no request, no run.
Quiescent ==
    /\ \A w \in Writers : wState[w] \in {"idle", "done", "dead"}
    /\ \A x \in RestWriters : restState[x] # "cas"
    /\ pending = 0 /\ ~claim.held
    /\ \A r \in Runners : rState[r] = "idle"

\* Once nothing is outstanding, the upstream has every write by a caller that
\* may forward, and every value a push-upstream call saw, unless the last run
\* spent its retry budget (the documented give-up) and no request was
\* recorded after it.
NoLostForward == Quiescent => (upstream >= owed \/ spent)

\* A force request is never downgraded: until a run takes it, the row
\* holds level 2, and the claim that took it pushes with force.
ForceKept ==
    /\ forceOwed => pending = 2
    /\ (claim.held /\ claim.owes) => claim.force

\* An explicit request is due now until a run takes it, even when a run
\* that held the lane ends in retry or restore.
ExplicitNotDelayed == explicitOwed => due = "zero"

\* A retry or restore waits out its delay unless an explicit request came
\* during the run: the claim time in push_due tells the two apart.
BackoffKept == ~hammered

\* Versions grow in write order, and every write descends from the one before.
\* A rewrite (a force-updated branch, a replaced tag) is out of scope: its plain
\* forward fails as FailRejected with a failed event, by design.

\* The upstream ref never moves backwards.
NoRewind == ~rewound

\* The lane always goes idle and stays idle.
Settles == <>[]Quiescent
=============================================================================
