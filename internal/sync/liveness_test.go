package sync

// Issue #79 liveness: a sync request accepted (pull-upstream 202) while a
// run already holds the claim must eventually be observed by a run whose
// upstream fetch postdates the request. Before the fix, the request's
// spawned run skipped at BeginSync (ErrSyncInProgress) and nothing ever
// rescheduled it — the 202'd upstream state never reached storage.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"
)

// gatedUpstreams wraps an UpstreamStore so the test can hold the FIRST run
// inside the worker's detach barrier (the GetUpstream between fetch and
// loopback push): while held, that run's fetch snapshot is provably older
// than upstream commits the test makes.
type gatedUpstreams struct {
	UpstreamStore
	entered chan struct{} // closed when the first run reaches the barrier
	release chan struct{} // closed by the test to let it continue
	once    stdsync.Once
}

func (g *gatedUpstreams) GetUpstream(ctx context.Context, repoID string) (Upstream, error) {
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return g.UpstreamStore.GetUpstream(ctx, repoID)
}

// commitUpstream adds one commit to the seeded bare upstream and returns
// the new head SHA.
func commitUpstream(t *testing.T, root, owner, name string) string {
	t.Helper()
	bare := filepath.Join(root, owner, name+".git")
	work := t.TempDir()
	testGit(t, work, "clone", "file://"+bare, ".")
	if err := os.WriteFile(filepath.Join(work, "f.txt"), []byte("v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testGit(t, work, "add", ".")
	testGit(t, work, "commit", "-m", "c2")
	testGit(t, work, "push", "origin", "main")
	return testGit(t, work, "rev-parse", "HEAD")
}

// loopbackMain returns the loopback bare repo's refs/heads/main, or "" while
// the ref does not exist yet (for-each-ref exits 0 either way).
func loopbackMain(t *testing.T, bare string) string {
	t.Helper()
	return testGit(t, bare, "for-each-ref", "--format=%(objectname)", "refs/heads/main")
}

func TestSyncRequestDuringClaimedRunIsObserved(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	loop := f.addLoopbackRepo(t, "repo_live")
	f.putUpstream(t, "repo_live", "own", "proj", "file://"+f.upstreamDir)

	gate := &gatedUpstreams{
		UpstreamStore: f.store,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	f.worker.Upstreams = gate

	runDone := make(chan struct{}, 8)
	q := NewInProcessQueue(func(ctx context.Context, job Job) error {
		err := f.worker.Run(ctx, job)
		runDone <- struct{}{}
		return err
	}, discardTestLogger())
	defer q.Close()
	// Production local-mode wiring (cmd/codestorage/wire_eventing.go):
	// the finisher reschedules pending requests through the queue.
	f.worker.Requeue = q.EnqueueSync

	job := Job{Org: "acme", RepoID: "repo_live"}
	if err := q.EnqueueSync(context.Background(), job); err != nil {
		t.Fatalf("enqueue first run: %v", err)
	}
	<-gate.entered // run 1 fetched the seed head; now held before its push

	// The pull-upstream 202 path while the claim is held: newer upstream
	// state exists, the request is marked durable, and the enqueued run
	// can only skip at BeginSync.
	head2 := commitUpstream(t, f.upstreamDir, "own", "proj")
	if err := f.store.RequestSync(context.Background(), "repo_live"); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	if err := q.EnqueueSync(context.Background(), job); err != nil {
		t.Fatalf("enqueue during claimed run: %v", err)
	}
	<-runDone // the request's own run has skipped (run 1 still holds the claim)

	close(gate.release) // run 1 pushes its pre-head2 snapshot and finishes
	<-runDone           // run 1 completed: loopback main is at the stale head

	// Liveness: the marked request must be observed — a follow-up run
	// fetches head2 and lands it in the loopback repo.
	deadline := time.After(30 * time.Second)
	for loopbackMain(t, loop) != head2 {
		select {
		case <-deadline:
			up, _ := f.store.GetUpstream(context.Background(), "repo_live")
			t.Fatalf("pending sync request dropped: loopback main = %q, want %s (row: %+v)",
				loopbackMain(t, loop), head2, up)
		case <-time.After(100 * time.Millisecond):
		}
	}

	up, err := f.store.GetUpstream(context.Background(), "repo_live")
	if err != nil {
		t.Fatal(err)
	}
	if up.SyncPending {
		t.Errorf("SyncPending still set after the follow-up run: %+v", up)
	}

	// Lifecycle ordering: the finisher requeues strictly AFTER emitting
	// its terminal event, so the follow-up's started event can never
	// overtake the finished run's succeeded event.
	<-runDone // the follow-up run completed (its events are recorded)
	assertEventTypes(t, f.rec.list(),
		EventSyncStarted, EventSyncSucceeded, EventSyncStarted, EventSyncSucceeded)
}

// ctxCheckedSink mirrors the production webhookSink, which uses the emit
// context for webhook listing and queue insertion: a dead context drops
// the event. The plain recorder would hide emissions on expired contexts.
type ctxCheckedSink struct{ inner EventSink }

func (s ctxCheckedSink) Emit(ctx context.Context, org, eventType string, payload []byte) {
	if ctx.Err() != nil {
		return
	}
	s.inner.Emit(ctx, org, eventType, payload)
}

// A run whose context expires mid-transfer must still clear its claim AND
// emit its terminal event: finishSync and the terminal emission both run
// on contexts detached from the run's budget. A claim wedged by an
// expired-context FinishSync would make every later run — including
// requeued pull-upstream requests — skip at BeginSync forever.
func TestFinishSyncSurvivesExpiredRunContext(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	f.addLoopbackRepo(t, "repo_exp")
	f.putUpstream(t, "repo_exp", "own", "proj", "file://"+f.upstreamDir)

	gate := &gatedUpstreams{
		UpstreamStore: f.store,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	f.worker.Upstreams = gate
	f.worker.Events = ctxCheckedSink{f.rec}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.worker.Run(ctx, Job{Org: "acme", RepoID: "repo_exp"}) }()
	<-gate.entered      // the run holds the claim, blocked at the barrier
	cancel()            // its budget expires while the claim is held
	close(gate.release) // the remaining transfer fails on the dead context

	if err := <-done; err == nil {
		t.Error("Run with an expired context returned nil, want the transfer failure")
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_exp")
	if err != nil {
		t.Fatal(err)
	}
	if up.Syncing || up.RunCount != 1 || up.SuccessCount != 0 {
		t.Errorf("row after expired-context run = %+v, want cleared claim and one counted failed run", up)
	}
	assertEventTypes(t, f.rec.list(), EventSyncStarted, EventSyncFailed)
}

// A run whose context is already dead when it claims must still emit a
// COMPLETE lifecycle (started + failed): the started emission runs
// detached too, so cancellation between the claim and the emit cannot
// produce a terminal event with no corresponding start.
func TestStartedEventSurvivesExpiredRunContext(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	f.addLoopbackRepo(t, "repo_dead")
	f.putUpstream(t, "repo_dead", "own", "proj", "file://"+f.upstreamDir)
	f.worker.Events = ctxCheckedSink{f.rec}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the budget is gone before the run claims
	if err := f.worker.Run(ctx, Job{Org: "acme", RepoID: "repo_dead"}); err == nil {
		t.Error("Run on a dead context returned nil, want the transfer failure")
	}

	up, err := f.store.GetUpstream(context.Background(), "repo_dead")
	if err != nil {
		t.Fatal(err)
	}
	if up.Syncing || up.RunCount != 1 || up.SuccessCount != 0 {
		t.Errorf("row after dead-context run = %+v, want cleared claim and one counted failed run", up)
	}
	assertEventTypes(t, f.rec.list(), EventSyncStarted, EventSyncFailed)
}

// flakyFinishUpstreams fails the first `fails` FinishSync calls.
type flakyFinishUpstreams struct {
	UpstreamStore
	fails atomic.Int32
}

func (f *flakyFinishUpstreams) FinishSync(ctx context.Context, repoID string, success bool) (Upstream, error) {
	if f.fails.Add(-1) >= 0 {
		return Upstream{}, errors.New("transient store blip")
	}
	return f.UpstreamStore.FinishSync(ctx, repoID, success)
}

// A transient FinishSync failure must be retried while the claim is still
// held — only the holder can clear it.
func TestFinishSyncRetriesTransientFailure(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	f.addLoopbackRepo(t, "repo_flk")
	f.putUpstream(t, "repo_flk", "own", "proj", "file://"+f.upstreamDir)

	flaky := &flakyFinishUpstreams{UpstreamStore: f.store}
	flaky.fails.Store(1)
	f.worker.Upstreams = flaky

	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_flk"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_flk")
	if err != nil {
		t.Fatal(err)
	}
	if up.Syncing || up.RunCount != 1 || up.SuccessCount != 1 {
		t.Errorf("row after retried finish = %+v, want cleared claim and one counted successful run", up)
	}
	assertEventTypes(t, f.rec.list(), EventSyncStarted, EventSyncSucceeded)
}
