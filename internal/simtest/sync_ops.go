// Upstream-sync claim-lease scenario ops and oracle (issues #86/#88): a
// minimal sync actor drives TryBeginSync/FinishSync cycles against the real
// meta store (no real git — the transfer is a seeded number of sim steps),
// with seeded crash injection between claim and finish. Node crash ops kill
// every in-flight run on the node too. The store's clock is the simulation's
// logical clock (meta.WithNowFunc), so lease expiry is a pure function of
// the seed.
//
// Oracles, all against a ground-truth model maintained by the serialized
// scheduler (reads through the clean oracle meta handle):
//
//  1. lease liveness (the #86 invariant this file exists for): after any
//     claim attempt at sim time T, no row may keep syncing=true with
//     sync_claim_expires_at <= T — an expired claim must be stolen by the
//     very next attempt. Exact (no slack): store and oracle share one
//     injected clock, so there is no skew to absorb;
//  2. steal safety: refusals only against unexpired claims, steals only of
//     expired ones, stolenFrom naming the exact previous holder;
//  3. late-finish choreography: a stolen run's FinishSync must fail
//     (ErrClaimLost while the thief holds, ErrNotFound after the thief
//     finished) and leave the row and counters untouched; an unstolen
//     run's FinishSync must succeed;
//  4. counter/pending integrity: RunCount/SuccessCount/sync_pending mirror
//     the model at every claim/finish boundary (only successful FinishSync
//     calls count; crashed and stolen runs never do);
//  5. quiescence: past every lease, one final claim per repo must succeed
//     (steal or fresh) and finish cleanly — no wedged row survives.
package simtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

const (
	// simSyncLeaseSteps is the claim lease in sim steps (1 step = 1 sim
	// second). Short relative to transfer durations so both orders occur:
	// runs that finish inside the lease and late finishers that outlive it.
	simSyncLeaseSteps = 6
	simSyncLease      = simSyncLeaseSteps * time.Second
	// simSyncMaxTransferSteps bounds the seeded transfer duration (1..max).
	// Max > lease keeps late-finish interleavings (steal racing FinishSync)
	// in play.
	simSyncMaxTransferSteps = 10
	// simSyncCrashPct is the seeded probability (percent) that a claimed
	// run dies between TryBeginSync and FinishSync — the #86 crash window.
	simSyncCrashPct = 30
)

// simSyncRun is one claimed sync run in flight: the actor state between
// TryBeginSync and its FinishSync attempt.
type simSyncRun struct {
	repo       string
	node       int
	holder     string
	finishStep int  // sim step at which FinishSync is attempted
	success    bool // seeded transfer outcome passed to FinishSync
	crashed    bool // actor died mid-run: FinishSync never happens
	stolen     bool // claim observed stolen: FinishSync must fail
	done       bool
}

// simSyncClaim is the model's view of a repo's active claim row.
type simSyncClaim struct {
	holder  string
	expires time.Time
	run     *simSyncRun
}

// initSync creates one upstream row per sim repo and the model state.
func (s *sim) initSync(ctx context.Context) error {
	s.syncClaims = make(map[string]*simSyncClaim)
	s.syncPending = make(map[string]bool)
	s.syncRunCount = make(map[string]int)
	s.syncSuccessCount = make(map[string]int)
	// SetRepoUpstream is guarded on a LIVE repos row (issue #52). The sim
	// seeds those rows directly with its deterministic repo keys — the
	// store's CreateRepo mints crypto/rand IDs, which a DST run must not
	// depend on — via raw SQL on the shared sqlite file.
	db, err := sql.Open("sqlite", strings.TrimPrefix(s.cl.metaURL, "sqlite://"))
	if err != nil {
		return fmt.Errorf("open meta db for repo seeding: %w", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO orgs (id, name, created_at) VALUES (1, 'dst', 0) ON CONFLICT DO NOTHING`); err != nil {
		return fmt.Errorf("seed sim org: %w", err)
	}
	for _, r := range s.repos {
		if _, err := db.ExecContext(ctx, `
INSERT INTO repos (repo_id, org_id, url, default_branch, created_at)
VALUES (?, 1, ?, 'main', 0) ON CONFLICT DO NOTHING`, r.key, "dst/"+r.key); err != nil {
			return fmt.Errorf("seed sim repo row %s: %w", r.key, err)
		}
	}
	for _, r := range s.repos {
		up := meta.RepoUpstream{RepoID: r.key, Provider: "github", Owner: "dst", Name: r.key, UpstreamHost: "github.invalid"}
		if _, err := s.cl.oracleMeta.SetRepoUpstream(ctx, up); err != nil {
			return fmt.Errorf("set upstream for %s: %w", r.key, err)
		}
	}
	return nil
}

// doSyncClaim attempts one sync claim from node against repo's upstream row
// and cross-checks the outcome (claimed / steal / refusal) plus the row
// against the model.
func (s *sim) doSyncClaim(name string, node *simNode, repo *simRepo) error {
	ctx := context.Background()
	// Fixed draw block: always consume the same scheduler draws whether or
	// not the claim lands, so the PRNG stream stays aligned.
	duration := 1 + s.rng.IntN(simSyncMaxTransferSteps)
	crash := s.rng.IntN(100) < simSyncCrashPct
	success := s.rng.IntN(10) < 7
	holder := fmt.Sprintf("n%d-s%04d", node.id, s.step)
	now := s.simNow()

	_, claimed, stolenFrom, err := node.metaStore.TryBeginSync(ctx, repo.key, holder, simSyncLease)
	if err != nil {
		return fmt.Errorf("step %d: TryBeginSync %s: %w", s.step, repo.key, err)
	}

	prev := s.syncClaims[repo.key]
	prevExpired := prev != nil && !prev.expires.After(now)
	outcome := "refused"
	switch {
	case claimed && stolenFrom == "":
		if prev != nil {
			s.violate("step %d: sync-claim %s: fresh claim by %s but the model holds %s (store lost a claim without finish or steal)",
				s.step, repo.key, holder, prev.holder)
			return nil
		}
		outcome = "claimed"
	case claimed && stolenFrom != "":
		if prev == nil {
			s.violate("step %d: sync-claim %s: stole %q but the model row is idle", s.step, repo.key, stolenFrom)
			return nil
		}
		if stolenFrom != prev.holder {
			s.violate("step %d: sync-claim %s: stole %q, model holder is %q", s.step, repo.key, stolenFrom, prev.holder)
			return nil
		}
		if !prevExpired {
			s.violate("step %d: sync-claim %s: stole a LIVE claim %q (expires %v, now %v)",
				s.step, repo.key, prev.holder, prev.expires, now)
			return nil
		}
		if prev.run != nil && !prev.run.done && !prev.run.crashed {
			prev.run.stolen = true // late finisher: its FinishSync must fail
		}
		s.syncSteals++
		if prev.run != nil && prev.run.crashed {
			// The stolen holder actually died between TryBeginSync and
			// FinishSync — the #86 dead-holder class (as opposed to a
			// live run merely outliving its lease).
			s.syncCrashSteals++
		}
		outcome = "stole:" + stolenFrom
	default: // refused
		if prev == nil || prevExpired {
			s.violate("step %d: issue-#86 class: sync-claim %s by %s refused with no live claim (model: %s)",
				s.step, repo.key, holder, describeClaim(prev, now))
			return nil
		}
	}

	if claimed {
		run := &simSyncRun{
			repo: repo.key, node: node.id, holder: holder,
			finishStep: s.step + duration, success: success, crashed: crash,
		}
		s.syncClaims[repo.key] = &simSyncClaim{holder: holder, expires: now.Add(simSyncLease), run: run}
		s.syncPending[repo.key] = false
		if !crash {
			s.syncRuns = append(s.syncRuns, run)
		}
	}

	if err := s.checkSyncRow(ctx, repo.key, now, "sync-claim"); err != nil {
		return err
	}
	s.tracef("step=%04d op=%s node=%d repo=%s holder=%s out=%s dur=%d crash=%t ok=%t",
		s.step, name, node.id, repo.key, holder, outcome, duration, crash, success)
	return nil
}

// describeClaim renders the model claim for violation messages.
func describeClaim(cl *simSyncClaim, now time.Time) string {
	if cl == nil {
		return "idle"
	}
	return fmt.Sprintf("holder %s expired=%t", cl.holder, !cl.expires.After(now))
}

// checkSyncRow runs after every claim attempt: lease liveness (the #88
// invariant) — at sim time now the row must not keep syncing=true with an
// expiry at or before now, the attempt must have stolen it — then the full
// row mirror.
func (s *sim) checkSyncRow(ctx context.Context, repoKey string, now time.Time, op string) error {
	cur, err := s.cl.oracleMeta.GetRepoUpstream(ctx, repoKey)
	if err != nil {
		return fmt.Errorf("step %d: oracle GetRepoUpstream %s: %w", s.step, repoKey, err)
	}
	if cur.Syncing && !cur.SyncClaimExpiresAt.After(now) {
		s.violate("step %d: issue-#88 lease liveness: %s left %s claimed by %q with expiry %v <= now %v (expired claim survived a claim attempt)",
			s.step, op, repoKey, cur.SyncClaimHolder, cur.SyncClaimExpiresAt, now)
		return nil
	}
	return s.checkUpstreamMirror(ctx, repoKey, op)
}

// checkUpstreamMirror compares the repo's row field-for-field against the
// model, BOTH lanes at once: claim state (syncing/holder/expiry, plus the
// push lane's persisted force mandate), the pending markers, and all four
// counters. Called after every sync- or push-lane mutation point once the
// model reflects the intended outcome, it turns ANY unmodeled field change
// — by a claim, a refusal, a finish, or a late finisher, in EITHER lane —
// into a violation at the very next observation. One uniform check instead
// of per-call-site field lists, so no observation point can miss a field
// and no lane transition can silently touch the other lane's columns
// (issue #136 lane isolation).
func (s *sim) checkUpstreamMirror(ctx context.Context, repoKey, op string) error {
	cur, err := s.cl.oracleMeta.GetRepoUpstream(ctx, repoKey)
	if err != nil {
		return fmt.Errorf("step %d: oracle GetRepoUpstream %s: %w", s.step, repoKey, err)
	}
	wantSyncing, wantHolder, wantExpires := false, "", time.Unix(0, 0).UTC()
	if cl := s.syncClaims[repoKey]; cl != nil {
		wantSyncing, wantHolder, wantExpires = true, cl.holder, cl.expires
	}
	if cur.Syncing != wantSyncing || cur.SyncClaimHolder != wantHolder || !cur.SyncClaimExpiresAt.Equal(wantExpires) {
		s.violate("step %d: %s: %s claim state (syncing=%t holder=%q expires=%v) diverged from the model (syncing=%t holder=%q expires=%v)",
			s.step, op, repoKey, cur.Syncing, cur.SyncClaimHolder, cur.SyncClaimExpiresAt, wantSyncing, wantHolder, wantExpires)
		return nil
	}
	if cur.SyncPending != s.syncPending[repoKey] {
		s.violate("step %d: %s: %s pending marker %t, model %t",
			s.step, op, repoKey, cur.SyncPending, s.syncPending[repoKey])
		return nil
	}
	if cur.RunCount != s.syncRunCount[repoKey] || cur.SuccessCount != s.syncSuccessCount[repoKey] {
		s.violate("step %d: %s: %s counters %d/%d, model %d/%d",
			s.step, op, repoKey, cur.RunCount, cur.SuccessCount, s.syncRunCount[repoKey], s.syncSuccessCount[repoKey])
		return nil
	}
	wantPSyncing, wantPHolder, wantPExpires, wantPForce := false, "", time.Unix(0, 0).UTC(), false
	if cl := s.pushClaims[repoKey]; cl != nil {
		wantPSyncing, wantPHolder, wantPExpires, wantPForce = true, cl.holder, cl.expires, cl.force
	}
	if cur.PushSyncing != wantPSyncing || cur.PushClaimHolder != wantPHolder ||
		!cur.PushClaimExpiresAt.Equal(wantPExpires) || cur.PushClaimForce != wantPForce {
		s.violate("step %d: %s: %s push claim state (syncing=%t holder=%q expires=%v force=%t) diverged from the model (syncing=%t holder=%q expires=%v force=%t)",
			s.step, op, repoKey, cur.PushSyncing, cur.PushClaimHolder, cur.PushClaimExpiresAt, cur.PushClaimForce,
			wantPSyncing, wantPHolder, wantPExpires, wantPForce)
		return nil
	}
	if cur.PushPending != s.pushPending[repoKey] {
		s.violate("step %d: %s: %s push pending level %d, model %d",
			s.step, op, repoKey, cur.PushPending, s.pushPending[repoKey])
		return nil
	}
	if cur.PushRunCount != s.pushRunCount[repoKey] || cur.PushSuccessCount != s.pushSuccessCount[repoKey] {
		s.violate("step %d: %s: %s push counters %d/%d, model %d/%d",
			s.step, op, repoKey, cur.PushRunCount, cur.PushSuccessCount, s.pushRunCount[repoKey], s.pushSuccessCount[repoKey])
	}
	return nil
}

// doSyncRequest marks a sync request (the 202-accepted pull-upstream /
// push-trigger surface). The mirror check right after demands the marker
// is actually set (a request the store silently dropped would otherwise be
// unobservable once the next claim consumes it) and everything else is
// untouched; the next claim must consume it.
func (s *sim) doSyncRequest(name string, node *simNode, repo *simRepo) error {
	ctx := context.Background()
	if err := node.metaStore.RequestSync(ctx, repo.key); err != nil {
		return fmt.Errorf("step %d: RequestSync %s: %w", s.step, repo.key, err)
	}
	s.syncPending[repo.key] = true
	if err := s.checkUpstreamMirror(ctx, repo.key, "sync-request"); err != nil {
		return err
	}
	s.tracef("step=%04d op=%s node=%d repo=%s out=ok", s.step, name, node.id, repo.key)
	return nil
}

// processSyncFinishes completes every live run whose transfer duration has
// elapsed (called at the start of each step, before the step's op). Crashed
// runs are dropped without a FinishSync — their claims must be stolen.
func (s *sim) processSyncFinishes() error {
	if len(s.syncRuns) == 0 {
		return nil
	}
	ctx := context.Background()
	remaining := s.syncRuns[:0]
	for _, run := range s.syncRuns {
		switch {
		case run.done || run.crashed:
			// dropped: finished already, or killed by a node crash
		case run.finishStep > s.stepNow() && !s.stopped:
			remaining = append(remaining, run)
		case s.stopped:
			// violation earlier in this pass: keep state, stop finishing
			remaining = append(remaining, run)
		default:
			if err := s.finishSyncRun(ctx, run); err != nil {
				return err
			}
			if !run.done {
				remaining = append(remaining, run)
			}
		}
	}
	s.syncRuns = remaining
	return nil
}

// finishSyncRun attempts one run's FinishSync and checks the outcome
// choreography against the model.
func (s *sim) finishSyncRun(ctx context.Context, run *simSyncRun) error {
	assertf(!run.done && !run.crashed, "finishSyncRun: run %s already settled", run.holder)
	node := s.cl.nodes[run.node]
	_, err := node.metaStore.FinishSync(ctx, run.repo, run.holder, run.success)
	run.done = true
	outcome := ""
	switch {
	case err == nil:
		if run.stolen {
			s.violate("step %d: sync-finish %s: stolen holder %s finished successfully (holder guard failed)",
				s.step, run.repo, run.holder)
			return nil
		}
		cl := s.syncClaims[run.repo]
		if cl == nil || cl.holder != run.holder {
			s.violate("step %d: sync-finish %s: %s finished but the model claim is %s",
				s.step, run.repo, run.holder, describeClaim(cl, s.simNow()))
			return nil
		}
		s.syncRunCount[run.repo]++
		if run.success {
			s.syncSuccessCount[run.repo]++
		}
		delete(s.syncClaims, run.repo)
		if err := s.checkUpstreamMirror(ctx, run.repo, "sync-finish"); err != nil {
			return err
		}
		outcome = fmt.Sprintf("finished ok=%t", run.success)
	case errors.Is(err, meta.ErrClaimLost):
		if !run.stolen || s.syncClaims[run.repo] == nil {
			s.violate("step %d: sync-finish %s: %s got ErrClaimLost but stolen=%t model=%s",
				s.step, run.repo, run.holder, run.stolen, describeClaim(s.syncClaims[run.repo], s.simNow()))
			return nil
		}
		// The late finisher must have changed nothing: the mirror still
		// expects the thief's claim, counters, and pending marker intact.
		if err := s.checkUpstreamMirror(ctx, run.repo, "sync-finish-late"); err != nil {
			return err
		}
		outcome = "claim-lost"
	case errors.Is(err, meta.ErrNotFound):
		// The thief already finished: the row is idle and the late
		// finisher must still have changed nothing.
		if !run.stolen || s.syncClaims[run.repo] != nil {
			s.violate("step %d: sync-finish %s: %s got ErrNotFound but stolen=%t model=%s",
				s.step, run.repo, run.holder, run.stolen, describeClaim(s.syncClaims[run.repo], s.simNow()))
			return nil
		}
		if err := s.checkUpstreamMirror(ctx, run.repo, "sync-finish-late"); err != nil {
			return err
		}
		outcome = "claim-gone"
	default:
		return fmt.Errorf("step %d: FinishSync %s/%s: %w", s.step, run.repo, run.holder, err)
	}
	s.tracef("step=%04d op=sync-finish node=%d repo=%s holder=%s out=%s", s.step, run.node, run.repo, run.holder, outcome)
	return nil
}

// syncKillNode marks every in-flight run on a crashed node dead (the
// process died between TryBeginSync and FinishSync — the #86 crash class).
// Their claims stay held until stolen. Returns the kill count for the
// crash op's trace line.
func (s *sim) syncKillNode(nodeID int) int {
	killed := 0
	for _, run := range s.syncRuns {
		if run.node == nodeID && !run.done && !run.crashed {
			run.crashed = true
			killed++
		}
	}
	return killed
}

// checkSyncFinal is the quiescence oracle: jump the logical clock past
// every possible lease and transfer, drain surviving live runs, then one
// claim attempt per repo must succeed (steal or fresh — a refusal means a
// wedged row, the exact pre-#86 failure) and finish cleanly.
func (s *sim) checkSyncFinal() error {
	ctx := context.Background()
	s.clockSkew = simSyncLeaseSteps + simSyncMaxTransferSteps + 1
	if err := s.processSyncFinishes(); err != nil || s.stopped {
		return err
	}
	now := s.simNow()
	for _, repo := range s.repos {
		holder := "final-" + repo.key
		prev := s.syncClaims[repo.key]
		row, claimed, stolenFrom, err := s.cl.nodes[0].metaStore.TryBeginSync(ctx, repo.key, holder, simSyncLease)
		if err != nil {
			return fmt.Errorf("final claim %s: %w", repo.key, err)
		}
		if !claimed {
			s.violate("issue-#88 quiescence: %s refused the post-quiescence claim (row: syncing=%t holder=%q expires=%v now=%v)",
				repo.key, row.Syncing, row.SyncClaimHolder, row.SyncClaimExpiresAt, now)
			return nil
		}
		switch {
		case stolenFrom == "" && prev != nil:
			s.violate("issue-#88 quiescence: %s claimed fresh but the model holds %s", repo.key, prev.holder)
			return nil
		case stolenFrom != "" && (prev == nil || prev.holder != stolenFrom):
			s.violate("issue-#88 quiescence: %s stole %q, model %s", repo.key, stolenFrom, describeClaim(prev, now))
			return nil
		}
		if stolenFrom != "" {
			s.syncSteals++
			if prev.run != nil && prev.run.crashed {
				s.syncCrashSteals++
			}
		}
		// The final claim now owns the row: mirror the model through the
		// claimed state (pending consumed) and, after finishing, through
		// the idle state — counters included, so a crashed or stolen run
		// leaking into them violates here.
		s.syncClaims[repo.key] = &simSyncClaim{holder: holder, expires: now.Add(simSyncLease)}
		s.syncPending[repo.key] = false
		if err := s.checkUpstreamMirror(ctx, repo.key, "sync-quiesce-claim"); err != nil {
			return err
		}
		if s.stopped {
			return nil
		}
		fin, err := s.cl.nodes[0].metaStore.FinishSync(ctx, repo.key, holder, true)
		if err != nil {
			return fmt.Errorf("final finish %s: %w", repo.key, err)
		}
		s.syncRunCount[repo.key]++
		s.syncSuccessCount[repo.key]++
		delete(s.syncClaims, repo.key)
		if err := s.checkUpstreamMirror(ctx, repo.key, "sync-quiesce-finish"); err != nil {
			return err
		}
		if s.stopped {
			return nil
		}
		s.tracef("step=%04d op=sync-quiesce repo=%s out=%s runs=%d successes=%d",
			s.step, repo.key, quiesceToken(stolenFrom), fin.RunCount, fin.SuccessCount)
	}
	return nil
}

func quiesceToken(stolenFrom string) string {
	if stolenFrom == "" {
		return "claimed"
	}
	return "stole:" + stolenFrom
}
