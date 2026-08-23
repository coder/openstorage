package sync

// Issue #86: a holder process that dies between BeginSync and FinishSync
// must not wedge sync forever. The next run steals the provably-expired
// claim; a zombie holder that somehow survives must neither overwrite the
// thief's push (pre-push barrier) nor clear the thief's claim (holder
// guard in FinishSync).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// backdateClaim expires the repo's current claim in place — the test-side
// stand-in for a lease running out while the holder is stuck.
func backdateClaim(t *testing.T, m *MemStore, repoID string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	up, ok := m.upstreams[repoID]
	if !ok || !up.Syncing {
		t.Fatalf("backdateClaim: no claimed upstream %q (row %+v)", repoID, up)
	}
	up.SyncClaimExpiresAt = time.Now().Add(-time.Hour)
	m.upstreams[repoID] = up
}

// A dead holder (crashed process: claim taken, never finished, lease
// expired) must not wedge the repo: the next accepted pull-upstream's run
// steals the claim and syncs.
func TestDeadHolderClaimIsStolenAndSyncHeals(t *testing.T) {
	f := newWorkerFixture(t)
	head := seedUpstream(t, f.upstreamDir, "own", "proj")
	loop := f.addLoopbackRepo(t, "repo_wedge")
	f.putUpstream(t, "repo_wedge", "own", "proj", "file://"+f.upstreamDir)

	// The crashed-process shape: a claim whose holder will never finish
	// and whose lease expires immediately.
	if _, _, err := f.store.BeginSync(context.Background(), "repo_wedge", "dead-holder", time.Nanosecond); err != nil {
		t.Fatalf("dead-holder claim: %v", err)
	}

	q := NewInProcessQueue(f.worker.Run, discardTestLogger())
	defer q.Close()
	f.worker.Requeue = q.EnqueueSync

	// The pull-upstream 202 path against the wedged row.
	if err := f.store.RequestSync(context.Background(), "repo_wedge"); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	if err := q.EnqueueSync(context.Background(), Job{Org: "acme", RepoID: "repo_wedge"}); err != nil {
		t.Fatalf("EnqueueSync: %v", err)
	}

	deadline := time.After(30 * time.Second)
	for loopbackMain(t, loop) != head {
		select {
		case <-deadline:
			up, _ := f.store.GetUpstream(context.Background(), "repo_wedge")
			t.Fatalf("dead holder wedged sync forever: loopback main = %q, want %s (row: %+v)",
				loopbackMain(t, loop), head, up)
		case <-time.After(100 * time.Millisecond):
		}
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_wedge")
	if err != nil {
		t.Fatal(err)
	}
	if up.Syncing || up.SyncPending || up.RunCount != 1 || up.SyncClaimHolder != "" {
		t.Errorf("row after steal-and-sync = %+v, want one counted run with a released, cleared claim", up)
	}
}

// A dead holder whose lease has NOT yet expired refuses the steal, and the
// skipped run was the accepted request's only local vehicle — the worker
// must defer one retry past the observed expiry instead of dropping it.
func TestDeadHolderUnexpiredLeaseDefersRetry(t *testing.T) {
	f := newWorkerFixture(t)
	head := seedUpstream(t, f.upstreamDir, "own", "proj")
	loop := f.addLoopbackRepo(t, "repo_defer")
	f.putUpstream(t, "repo_defer", "own", "proj", "file://"+f.upstreamDir)

	// Crashed process whose lease is still live when the request arrives.
	if _, _, err := f.store.BeginSync(context.Background(), "repo_defer", "dead-holder", 1500*time.Millisecond); err != nil {
		t.Fatalf("dead-holder claim: %v", err)
	}

	q := NewInProcessQueue(f.worker.Run, discardTestLogger())
	defer q.Close()
	f.worker.Requeue = q.EnqueueSync

	if err := f.store.RequestSync(context.Background(), "repo_defer"); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	if err := q.EnqueueSync(context.Background(), Job{Org: "acme", RepoID: "repo_defer"}); err != nil {
		t.Fatalf("EnqueueSync: %v", err)
	}

	// The immediate run skips (lease unexpired); the deferred retry fires
	// just past expiry, steals, and syncs.
	deadline := time.After(30 * time.Second)
	for loopbackMain(t, loop) != head {
		select {
		case <-deadline:
			up, _ := f.store.GetUpstream(context.Background(), "repo_defer")
			t.Fatalf("unexpired dead holder stranded the request: loopback main = %q, want %s (row: %+v)",
				loopbackMain(t, loop), head, up)
		case <-time.After(100 * time.Millisecond):
		}
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_defer")
	if err != nil {
		t.Fatal(err)
	}
	if up.Syncing || up.SyncPending || up.RunCount != 1 {
		t.Errorf("row after deferred steal = %+v, want one counted run with consumed marker", up)
	}
}

// finishGateUpstreams parks the FIRST FinishSync until released (CAS, not
// sync.Once — later calls must flow while the first is held).
type finishGateUpstreams struct {
	UpstreamStore
	entered chan struct{}
	release chan struct{}
	first   atomic.Bool
}

func (g *finishGateUpstreams) FinishSync(ctx context.Context, repoID, holder string, success bool) (Upstream, error) {
	if g.first.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.release
	}
	return g.UpstreamStore.FinishSync(ctx, repoID, holder, success)
}

// A zombie holder whose claim is stolen while it is parked INSIDE
// FinishSync must abandon on the holder guard (ErrClaimLost): no terminal
// event, no counter writes, and the thief's completed run stays intact.
func TestLateFinishAfterStealAbandons(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	f.addLoopbackRepo(t, "repo_zomb")
	f.putUpstream(t, "repo_zomb", "own", "proj", "file://"+f.upstreamDir)

	gate := &finishGateUpstreams{
		UpstreamStore: f.store,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	f.worker.Upstreams = gate

	job := Job{Org: "acme", RepoID: "repo_zomb"}
	done := make(chan error, 1)
	go func() { done <- f.worker.Run(context.Background(), job) }()
	<-gate.entered // run 1 pushed and is parked inside its FinishSync

	backdateClaim(t, f.store, "repo_zomb") // its lease expires while parked

	// The thief steals the expired claim and is STILL mid-run when the
	// zombie's finish lands — the sharpest holder-mismatch shape.
	if _, stolenFrom, err := f.store.BeginSync(context.Background(), "repo_zomb", "thief-holder", time.Minute); err != nil || stolenFrom == "" {
		t.Fatalf("steal = (%q, %v), want a steal", stolenFrom, err)
	}

	close(gate.release) // run 1's FinishSync forwards: ErrClaimLost
	if err := <-done; err != nil {
		t.Fatalf("late run must abandon silently, got %v", err)
	}

	// The thief's in-flight claim is untouched and the zombie counted
	// nothing; run 1 contributed only its started event.
	up, err := f.store.GetUpstream(context.Background(), "repo_zomb")
	if err != nil {
		t.Fatal(err)
	}
	if !up.Syncing || up.SyncClaimHolder != "thief-holder" || up.RunCount != 0 {
		t.Errorf("row after late finish = %+v, want the thief's intact in-flight claim", up)
	}
	assertEventTypes(t, f.rec.list(), EventSyncStarted)

	// The thief completes normally.
	fin, err := f.store.FinishSync(context.Background(), "repo_zomb", "thief-holder", true)
	if err != nil || fin.Syncing || fin.RunCount != 1 {
		t.Errorf("thief finish = (%+v, %v), want one counted run", fin, err)
	}
}

// A zombie holder that is still mid-TRANSFER when its claim is stolen must
// abandon at the pre-push barrier: its stale fetch snapshot never
// overwrites the thief's newer push.
func TestStolenClaimAbandonsBeforePush(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	loop := f.addLoopbackRepo(t, "repo_barrier")
	f.putUpstream(t, "repo_barrier", "own", "proj", "file://"+f.upstreamDir)

	gate := &gatedUpstreams{
		UpstreamStore: f.store,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	f.worker.Upstreams = gate

	job := Job{Org: "acme", RepoID: "repo_barrier"}
	done := make(chan error, 1)
	go func() { done <- f.worker.Run(context.Background(), job) }()
	<-gate.entered // run 1 fetched; parked at the pre-push barrier

	backdateClaim(t, f.store, "repo_barrier")

	// The thief syncs a NEWER upstream head end-to-end.
	head2 := commitUpstream(t, f.upstreamDir, "own", "proj")
	if err := f.worker.Run(context.Background(), job); err != nil {
		t.Fatalf("thief run: %v", err)
	}
	if got := loopbackMain(t, loop); got != head2 {
		t.Fatalf("thief push: loopback main = %q, want %s", got, head2)
	}

	close(gate.release) // run 1's barrier read sees the thief's holder
	if err := <-done; err != nil {
		t.Fatalf("stolen run must abandon silently, got %v", err)
	}
	// The zombie's stale snapshot must not have regressed the ref.
	if got := loopbackMain(t, loop); got != head2 {
		t.Errorf("zombie push regressed loopback main to %q, want %s", got, head2)
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_barrier")
	if err != nil {
		t.Fatal(err)
	}
	if up.Syncing || up.RunCount != 1 || up.SuccessCount != 1 {
		t.Errorf("row after barrier abandon = %+v, want exactly the thief's one counted run", up)
	}
	assertEventTypes(t, f.rec.list(), EventSyncStarted, EventSyncStarted, EventSyncSucceeded)
}
