// Outbound push-lane claim/marker scenario ops and oracle (issue #136):
// the push_* edition of sync_ops.go over the lane PR #137 added. A minimal
// forwarder actor drives RequestPush/TryBeginPush/FinishPush cycles against
// the real meta store (no real git — the transfer is a seeded number of
// sim steps) with seeded crash injection between claim and finish, seeded
// transfer outcomes mirroring the worker's failure taxonomy (success /
// transient re-mark / terminal non-FF rejection). Node crash ops kill the node's in-flight
// push runs too. The store's clock is the simulation's logical clock, so
// lease expiry is a pure function of the seed.
//
// Oracles, all against a ground-truth model maintained by the serialized
// scheduler (reads through the clean oracle meta handle), keyed to the
// four #136 invariants:
//
//  1. convergence: refs are abstracted to a per-repo version counter —
//     every push-request bumps the local version, a claim snapshots it
//     (the loopback fetch postdates the claim), and a successful finish
//     covers through its snapshot. At quiescence every accepted request
//     must be covered by a postdating successful run, terminally rejected
//     (non-FF, needs a human), or still CARRIED (pending marker set or an
//     orphaned claim a later steal takes over) — and the final claim+finish per
//     repo converges covered == local (upstream refs equal local refs);
//  2. force monotonicity: the model pending level only coalesces upward
//     until a claim consumes it whole, the force TryBeginPush reports must
//     equal the model mandate (pending == force OR inherited from a dead
//     claim's persisted push_claim_force), and the full-row mirror turns
//     any unmodeled marker downgrade into a violation at the very next
//     observation. A dead claim's force mandate survives on its
//     push_claim_force alone, and the steal inherits it;
//  3. lease safety: at most one unexpired holder per repo (fresh claims
//     only against idle rows, steals only of expired claims naming the
//     exact previous holder, refusals only against live ones), and a
//     stolen holder's FinishPush fails (ErrClaimLost while the thief
//     holds, ErrNotFound after it finished) leaving row and counters
//     untouched;
//  4. lane isolation: the mirror both lanes share (checkUpstreamMirror,
//     sync_ops.go) compares every sync_* AND push_* column after every
//     mutation point in either lane, so a pull-lane transition touching
//     push-lane columns (or vice versa) violates immediately.
package simtest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

const (
	// simPushLeaseSteps is the outbound claim lease in sim steps (1 step =
	// 1 sim second). Same rationale as simSyncLeaseSteps: short relative
	// to transfer durations so both orders occur.
	simPushLeaseSteps = 6
	simPushLease      = simPushLeaseSteps * time.Second
	// simPushMaxTransferSteps bounds the seeded transfer duration (1..max);
	// max > lease keeps late-finish interleavings in play.
	simPushMaxTransferSteps = 10
	// simPushCrashPct is the seeded probability (percent) that a claimed
	// run dies between TryBeginPush and FinishPush — the crash window the
	// force-inheriting steal rescues.
	simPushCrashPct = 30
	// simPushForcePct is the seeded probability (percent) that a push
	// request carries force (level 2). High enough that crashed forced
	// claims — the rescue class the vacuity guard demands — occur on the
	// fast seeds.
	simPushForcePct = 40
	// Seeded transfer outcome split (percent): success, transient failure
	// (worker re-marks and retries), remainder terminal non-FF rejection.
	simPushOKPct        = 60
	simPushTransientPct = 25
)

// pushOutcome is one seeded transfer result, mirroring the worker's
// failure taxonomy (worker_push.go): transient failures re-mark the
// durable pending level with the run's force (schedulePushRetry); terminal
// rejections do not retry.
type pushOutcome int

const (
	pushOK pushOutcome = iota
	pushTransient
	pushRejected
)

func (o pushOutcome) String() string {
	switch o {
	case pushOK:
		return "ok"
	case pushTransient:
		return "transient"
	case pushRejected:
		return "rejected"
	}
	panic(fmt.Sprintf("simtest: unhandled push outcome %d", int(o)))
}

// simPushRun is one claimed outbound run in flight: the actor state
// between TryBeginPush and its FinishPush attempt.
type simPushRun struct {
	repo       string
	node       int
	holder     string
	finishStep int         // sim step at which FinishPush is attempted
	outcome    pushOutcome // seeded transfer outcome
	force      bool        // the force mandate the claim consumed
	snapshot   int         // local version at claim time: what a success covers
	crashed    bool        // actor died mid-run: FinishPush never happens
	stolen     bool        // claim observed stolen: FinishPush must fail
	done       bool
}

// simPushClaim is the model's view of a repo's active push claim.
type simPushClaim struct {
	holder  string
	expires time.Time
	force   bool // persisted push_claim_force
	run     *simPushRun
}

// initPush allocates the outbound-lane model state. The upstream rows both
// lanes share were already seeded by initSync.
func (s *sim) initPush() {
	s.pushClaims = make(map[string]*simPushClaim)
	s.pushPending = make(map[string]int)
	s.pushRunCount = make(map[string]int)
	s.pushSuccessCount = make(map[string]int)
	s.pushLocalVer = make(map[string]int)
	s.pushCoveredVer = make(map[string]int)
	s.pushRejectedVer = make(map[string]int)
}

// pushLevel maps a force flag to the marker level RequestPush writes.
func pushLevel(force bool) int {
	if force {
		return 2
	}
	return 1
}

// doPushRequest accepts one outbound push request (the 202-accepted
// push-upstream trigger / forward-on-push poke surface): the local refs
// moved (version bump) and the durable marker must coalesce UPWARD to the
// request's level. The mirror right after demands the level actually rose
// (a silently dropped or downgraded request would otherwise be
// unobservable once the next claim consumes the marker).
func (s *sim) doPushRequest(name string, node *simNode, repo *simRepo) error {
	ctx := context.Background()
	force := s.rng.IntN(100) < simPushForcePct
	if err := node.metaStore.RequestPush(ctx, repo.key, force); err != nil {
		return fmt.Errorf("step %d: RequestPush %s: %w", s.step, repo.key, err)
	}
	s.pushLocalVer[repo.key]++
	if lvl := pushLevel(force); lvl > s.pushPending[repo.key] {
		s.pushPending[repo.key] = lvl
	}
	if err := s.checkUpstreamMirror(ctx, repo.key, "push-request"); err != nil {
		return err
	}
	s.tracef("step=%04d op=%s node=%d repo=%s force=%t v=%d out=ok",
		s.step, name, node.id, repo.key, force, s.pushLocalVer[repo.key])
	return nil
}

// doPushClaim attempts one outbound claim from node against repo's
// upstream row and cross-checks the outcome (claimed / steal / refusal),
// the consumed force mandate, and the row against the model.
func (s *sim) doPushClaim(name string, node *simNode, repo *simRepo) error {
	ctx := context.Background()
	// Fixed draw block: always consume the same scheduler draws whether or
	// not the claim lands, so the PRNG stream stays aligned.
	duration := 1 + s.rng.IntN(simPushMaxTransferSteps)
	crash := s.rng.IntN(100) < simPushCrashPct
	outcomeDraw := s.rng.IntN(100)
	outcome := pushRejected
	switch {
	case outcomeDraw < simPushOKPct:
		outcome = pushOK
	case outcomeDraw < simPushOKPct+simPushTransientPct:
		outcome = pushTransient
	}
	holder := fmt.Sprintf("pn%d-s%04d", node.id, s.step)
	now := s.simNow()
	pendingWas := s.pushPending[repo.key]

	_, claimed, force, stolenFrom, err := node.metaStore.TryBeginPush(ctx, repo.key, holder, simPushLease)
	if err != nil {
		return fmt.Errorf("step %d: TryBeginPush %s: %w", s.step, repo.key, err)
	}

	prev := s.pushClaims[repo.key]
	prevExpired := prev != nil && !prev.expires.After(now)
	out := "refused"
	switch {
	case claimed && stolenFrom == "":
		if prev != nil {
			s.violate("step %d: push-claim %s: fresh claim by %s but the model holds %s (store lost a push claim without finish or steal)",
				s.step, repo.key, holder, prev.holder)
			return nil
		}
		out = "claimed"
	case claimed && stolenFrom != "":
		if prev == nil {
			s.violate("step %d: push-claim %s: stole %q but the model row is idle", s.step, repo.key, stolenFrom)
			return nil
		}
		if stolenFrom != prev.holder {
			s.violate("step %d: push-claim %s: stole %q, model holder is %q", s.step, repo.key, stolenFrom, prev.holder)
			return nil
		}
		if !prevExpired {
			s.violate("step %d: issue-#136 lease safety: push-claim %s stole a LIVE claim %q (expires %v, now %v)",
				s.step, repo.key, prev.holder, prev.expires, now)
			return nil
		}
		if prev.run != nil && !prev.run.done && !prev.run.crashed {
			prev.run.stolen = true // late finisher: its FinishPush must fail
		}
		s.pushSteals++
		if prev.run != nil && prev.run.crashed {
			s.pushCrashSteals++
			if prev.force && pendingWas < 2 {
				// The force mandate survived ONLY on the DEAD holder's
				// persisted push_claim_force — the #136 rescue class the
				// inheritance rule exists for. Live holders merely
				// outliving their lease inherit too, but only crashed
				// ones count as rescues (the vacuity guard's promise).
				s.pushForcedRescues++
			}
		}
		out = "stole:" + stolenFrom
	default: // refused
		if prev == nil || prevExpired {
			s.violate("step %d: issue-#136 lease liveness: push-claim %s by %s refused with no live claim (model: %s)",
				s.step, repo.key, holder, describePushClaim(prev, now))
			return nil
		}
	}

	if claimed {
		// Force monotonicity at the consumption point: the run's mandate
		// is the consumed marker's force OR the stolen claim's persisted
		// force — never less.
		wantForce := pendingWas == 2 || (prev != nil && prev.force)
		if force != wantForce {
			s.violate("step %d: issue-#136 force monotonicity: push-claim %s by %s ran force=%t, model mandate %t (pending was %d, prev %s)",
				s.step, repo.key, holder, force, wantForce, pendingWas, describePushClaim(prev, now))
			return nil
		}
		run := &simPushRun{
			repo: repo.key, node: node.id, holder: holder,
			finishStep: s.step + duration, outcome: outcome, force: force,
			snapshot: s.pushLocalVer[repo.key], crashed: crash,
		}
		s.pushClaims[repo.key] = &simPushClaim{holder: holder, expires: now.Add(simPushLease), force: force, run: run}
		s.pushPending[repo.key] = 0
		if !crash {
			s.pushRuns = append(s.pushRuns, run)
		}
	}

	if err := s.checkPushRow(ctx, repo.key, now, "push-claim"); err != nil {
		return err
	}
	s.tracef("step=%04d op=%s node=%d repo=%s holder=%s out=%s dur=%d crash=%t force=%t result=%s",
		s.step, name, node.id, repo.key, holder, out, duration, crash, force, outcome)
	return nil
}

// describePushClaim renders the model push claim for violation messages.
func describePushClaim(cl *simPushClaim, now time.Time) string {
	if cl == nil {
		return "idle"
	}
	return fmt.Sprintf("holder %s expired=%t force=%t", cl.holder, !cl.expires.After(now), cl.force)
}

// checkPushRow runs after every claim attempt: lease liveness — at sim
// time now the row must not keep push_syncing=true with an expiry at or
// before now, the attempt must have stolen it — then the full row mirror.
func (s *sim) checkPushRow(ctx context.Context, repoKey string, now time.Time, op string) error {
	cur, err := s.cl.oracleMeta.GetRepoUpstream(ctx, repoKey)
	if err != nil {
		return fmt.Errorf("step %d: oracle GetRepoUpstream %s: %w", s.step, repoKey, err)
	}
	if cur.PushSyncing && !cur.PushClaimExpiresAt.After(now) {
		s.violate("step %d: issue-#136 lease liveness: %s left %s push-claimed by %q with expiry %v <= now %v (expired claim survived a claim attempt)",
			s.step, op, repoKey, cur.PushClaimHolder, cur.PushClaimExpiresAt, now)
		return nil
	}
	return s.checkUpstreamMirror(ctx, repoKey, op)
}

// processPushFinishes completes every live run whose transfer duration has
// elapsed (called at the start of each step, before the step's op).
// Crashed runs are dropped without a FinishPush — their claims must be
// stolen.
func (s *sim) processPushFinishes() error {
	if len(s.pushRuns) == 0 {
		return nil
	}
	ctx := context.Background()
	remaining := s.pushRuns[:0]
	for _, run := range s.pushRuns {
		switch {
		case run.done || run.crashed:
			// dropped: finished already, or killed by a node crash
		case run.finishStep > s.stepNow() && !s.stopped:
			remaining = append(remaining, run)
		case s.stopped:
			// violation earlier in this pass: keep state, stop finishing
			remaining = append(remaining, run)
		default:
			if err := s.finishPushRun(ctx, run); err != nil {
				return err
			}
			if !run.done {
				remaining = append(remaining, run)
			}
		}
	}
	s.pushRuns = remaining
	return nil
}

// finishPushRun attempts one run's FinishPush and checks the outcome
// choreography against the model, then applies the worker's failure
// taxonomy: success covers through the claim-time snapshot, a transient
// failure re-marks the durable pending level with the run's force
// (schedulePushRetry), and a terminal non-FF rejection retries nothing.
func (s *sim) finishPushRun(ctx context.Context, run *simPushRun) error {
	assertf(!run.done && !run.crashed, "finishPushRun: run %s already settled", run.holder)
	node := s.cl.nodes[run.node]
	_, err := node.metaStore.FinishPush(ctx, run.repo, run.holder, run.outcome == pushOK)
	run.done = true
	out := ""
	switch {
	case err == nil:
		if run.stolen {
			s.violate("step %d: push-finish %s: stolen holder %s finished successfully (holder guard failed)",
				s.step, run.repo, run.holder)
			return nil
		}
		cl := s.pushClaims[run.repo]
		if cl == nil || cl.holder != run.holder {
			s.violate("step %d: push-finish %s: %s finished but the model claim is %s",
				s.step, run.repo, run.holder, describePushClaim(cl, s.simNow()))
			return nil
		}
		s.pushRunCount[run.repo]++
		delete(s.pushClaims, run.repo)
		switch run.outcome {
		case pushOK:
			s.pushSuccessCount[run.repo]++
			if run.snapshot > s.pushCoveredVer[run.repo] {
				s.pushCoveredVer[run.repo] = run.snapshot
			}
		case pushTransient:
			// The worker re-marks the durable pending level with the
			// run's force before backing off, so the accepted requests
			// stay carried through the outage.
			if rmErr := node.metaStore.RequestPush(ctx, run.repo, run.force); rmErr != nil {
				return fmt.Errorf("step %d: re-mark push retry %s: %w", s.step, run.repo, rmErr)
			}
			if lvl := pushLevel(run.force); lvl > s.pushPending[run.repo] {
				s.pushPending[run.repo] = lvl
			}
		case pushRejected:
			// Terminal: the upstream refused the batch; only a human
			// decision (pull or force) resolves it. The versions this run
			// consumed are terminally rejected, not stranded.
			if run.snapshot > s.pushRejectedVer[run.repo] {
				s.pushRejectedVer[run.repo] = run.snapshot
			}
		}
		if mErr := s.checkUpstreamMirror(ctx, run.repo, "push-finish"); mErr != nil {
			return mErr
		}
		out = "finished " + run.outcome.String()
	case errors.Is(err, meta.ErrClaimLost):
		if !run.stolen || s.pushClaims[run.repo] == nil {
			s.violate("step %d: push-finish %s: %s got ErrClaimLost but stolen=%t model=%s",
				s.step, run.repo, run.holder, run.stolen, describePushClaim(s.pushClaims[run.repo], s.simNow()))
			return nil
		}
		// The late finisher must have changed nothing: the mirror still
		// expects the thief's claim (force included), counters, and
		// pending marker intact.
		if mErr := s.checkUpstreamMirror(ctx, run.repo, "push-finish-late"); mErr != nil {
			return mErr
		}
		out = "claim-lost"
	case errors.Is(err, meta.ErrNotFound):
		// The thief already finished: the row is idle and the late
		// finisher must still have changed nothing.
		if !run.stolen || s.pushClaims[run.repo] != nil {
			s.violate("step %d: push-finish %s: %s got ErrNotFound but stolen=%t model=%s",
				s.step, run.repo, run.holder, run.stolen, describePushClaim(s.pushClaims[run.repo], s.simNow()))
			return nil
		}
		if mErr := s.checkUpstreamMirror(ctx, run.repo, "push-finish-late"); mErr != nil {
			return mErr
		}
		out = "claim-gone"
	default:
		return fmt.Errorf("step %d: FinishPush %s/%s: %w", s.step, run.repo, run.holder, err)
	}
	s.tracef("step=%04d op=push-finish node=%d repo=%s holder=%s out=%s", s.step, run.node, run.repo, run.holder, out)
	return nil
}

// pushKillNode marks every in-flight push run on a crashed node dead (the
// process died between TryBeginPush and FinishPush). Their claims stay
// held — force mandate included — until stolen. Returns the kill count for
// the crash op's trace line.
func (s *sim) pushKillNode(nodeID int) int {
	killed := 0
	for _, run := range s.pushRuns {
		if run.node == nodeID && !run.done && !run.crashed {
			run.crashed = true
			killed++
		}
	}
	return killed
}

// checkPushFinal is the outbound quiescence oracle: jump the logical clock
// past every push lease and transfer, drain surviving live runs (their
// re-marks included), then check the #79-style coverage argument — an
// accepted request not covered by a postdating successful run and not
// terminally rejected must still be CARRIED by the pending marker or an
// orphaned claim (which a later steal takes over) — and run one final
// claim+finish per repo, which must succeed (a refusal is a wedged row),
// carry any inherited force mandate, and converge the upstream: covered
// version equals local version, i.e. upstream refs equal local refs.
func (s *sim) checkPushFinal() error {
	ctx := context.Background()
	s.clockSkew += simPushLeaseSteps + simPushMaxTransferSteps + 1
	if err := s.processPushFinishes(); err != nil || s.stopped {
		return err
	}
	now := s.simNow()
	for _, repo := range s.repos {
		local, covered, rejected := s.pushLocalVer[repo.key], s.pushCoveredVer[repo.key], s.pushRejectedVer[repo.key]
		prev := s.pushClaims[repo.key]
		pendingWas := s.pushPending[repo.key]
		if local > covered && local > rejected && pendingWas == 0 && prev == nil {
			s.violate("issue-#136 convergence: %s accepted requests through v%d but covered=v%d rejected=v%d with no pending marker and no claim (request stranded)",
				repo.key, local, covered, rejected)
			return nil
		}
		holder := "final-push-" + repo.key
		wantForce := pendingWas == 2 || (prev != nil && prev.force)
		row, claimed, force, stolenFrom, err := s.cl.nodes[0].metaStore.TryBeginPush(ctx, repo.key, holder, simPushLease)
		if err != nil {
			return fmt.Errorf("final push claim %s: %w", repo.key, err)
		}
		if !claimed {
			s.violate("issue-#136 quiescence: %s refused the post-quiescence push claim (row: push_syncing=%t holder=%q expires=%v now=%v)",
				repo.key, row.PushSyncing, row.PushClaimHolder, row.PushClaimExpiresAt, now)
			return nil
		}
		switch {
		case stolenFrom == "" && prev != nil:
			s.violate("issue-#136 quiescence: %s claimed fresh but the model holds %s", repo.key, prev.holder)
			return nil
		case stolenFrom != "" && (prev == nil || prev.holder != stolenFrom):
			s.violate("issue-#136 quiescence: %s stole %q, model %s", repo.key, stolenFrom, describePushClaim(prev, now))
			return nil
		}
		if force != wantForce {
			s.violate("issue-#136 force monotonicity: %s post-quiescence claim ran force=%t, model mandate %t (pending was %d, prev %s)",
				repo.key, force, wantForce, pendingWas, describePushClaim(prev, now))
			return nil
		}
		if stolenFrom != "" {
			s.pushSteals++
			if prev.run != nil && prev.run.crashed {
				s.pushCrashSteals++
				if prev.force && pendingWas < 2 {
					s.pushForcedRescues++
				}
			}
		}
		// The final claim now owns the row: mirror the model through the
		// claimed state (pending consumed) and, after finishing, through
		// the idle state — counters included, so a crashed or stolen run
		// leaking into them violates here.
		s.pushClaims[repo.key] = &simPushClaim{holder: holder, expires: now.Add(simPushLease), force: force}
		s.pushPending[repo.key] = 0
		if err := s.checkUpstreamMirror(ctx, repo.key, "push-quiesce-claim"); err != nil {
			return err
		}
		if s.stopped {
			return nil
		}
		fin, err := s.cl.nodes[0].metaStore.FinishPush(ctx, repo.key, holder, true)
		if err != nil {
			return fmt.Errorf("final push finish %s: %w", repo.key, err)
		}
		s.pushRunCount[repo.key]++
		s.pushSuccessCount[repo.key]++
		delete(s.pushClaims, repo.key)
		// The final run pushed everything: upstream converges to local.
		s.pushCoveredVer[repo.key] = local
		assertf(s.pushCoveredVer[repo.key] == s.pushLocalVer[repo.key],
			"push-quiesce %s: covered v%d != local v%d after the final run", repo.key, s.pushCoveredVer[repo.key], s.pushLocalVer[repo.key])
		if err := s.checkUpstreamMirror(ctx, repo.key, "push-quiesce-finish"); err != nil {
			return err
		}
		if s.stopped {
			return nil
		}
		s.tracef("step=%04d op=push-quiesce repo=%s out=%s force=%t runs=%d successes=%d covered=v%d",
			s.step, repo.key, quiesceToken(stolenFrom), force, fin.PushRunCount, fin.PushSuccessCount, local)
	}
	return nil
}
