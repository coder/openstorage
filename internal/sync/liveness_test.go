package sync

// Issue #79 liveness: a sync request accepted (pull-upstream 202) while a
// run already holds the claim must eventually be observed by a run whose
// upstream fetch postdates the request. Before the fix, the request's
// spawned run skipped at BeginSync (ErrSyncInProgress) and nothing ever
// rescheduled it — the 202'd upstream state never reached storage.

import (
	"context"
	"os"
	"path/filepath"
	stdsync "sync"
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
