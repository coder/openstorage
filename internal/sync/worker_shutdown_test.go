// Shutdown controls for the upstream-store path (PostgreSQL/River, issue
// #343): a run whose context carries ErrShutdown releases its claim and
// re-marks its request without counting a run or emitting repo.sync.failed,
// and still returns an error so River's completion gate keeps its job.

package sync

import (
	"context"
	"errors"
	"testing"
	"time"
)

// runCancelledByShutdown starts run, waits until the upstream holds it,
// cancels it with ErrShutdown, and returns its error.
func runCancelledByShutdown(t *testing.T, up *stallUpstream, run func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	up.hold.Store(true)
	errc := make(chan error, 1)
	go func() { errc <- run(ctx) }()
	up.held(t)
	cancel(ErrShutdown)
	select {
	case err := <-errc:
		up.hold.Store(false)
		return err
	case <-time.After(60 * time.Second):
		t.Fatal("cancelled run did not return")
		return nil
	}
}

func eventsFor(f *workerFixture, repoID string) []Event {
	var out []Event
	for _, e := range f.rec.list() {
		if e.Repository.ID == repoID {
			out = append(out, e)
		}
	}
	return out
}

func TestRunShutdownRestoresRequest(t *testing.T) {
	t.Run("pull", func(t *testing.T) {
		f := newWorkerFixture(t)
		up := newStallUpstream(t)
		head := seedUpstream(t, up.root, "own", "s1")
		f.putUpstream(t, "repo_s1", "own", "s1", up.url)
		loopback := f.addLoopbackRepo(t, "repo_s1")
		if err := f.store.RequestSync(t.Context(), "repo_s1"); err != nil {
			t.Fatal(err)
		}
		job := Job{Org: "org", RepoID: "repo_s1"}
		err := runCancelledByShutdown(t, up, func(ctx context.Context) error { return f.worker.Run(ctx, job) })
		if !errors.Is(err, ErrShutdown) {
			t.Fatalf("Run = %v, want an error wrapping ErrShutdown (River's gate keeps the job)", err)
		}
		r, _ := f.store.GetUpstream(t.Context(), "repo_s1")
		if r.RunCount != 0 || !r.SyncPending || r.Syncing {
			t.Fatalf("after the shutdown: %+v, want no run counted, the request pending, no claim", r)
		}
		assertEventTypes(t, eventsFor(f, "repo_s1"), EventSyncStarted) // no repo.sync.failed

		if err := f.worker.Run(t.Context(), job); err != nil {
			t.Fatalf("rerun: %v", err)
		}
		if r, _ := f.store.GetUpstream(t.Context(), "repo_s1"); r.RunCount != 1 || r.SuccessCount != 1 || r.SyncPending {
			t.Fatalf("after the rerun: %+v, want one successful run", r)
		}
		if got := upstreamHead(t, loopback); got != head {
			t.Fatalf("loopback main = %q, want %q", got, head)
		}
	})

	t.Run("push keeps force", func(t *testing.T) {
		f := newWorkerFixture(t)
		up := newStallUpstream(t)
		bare := addEmptyUpstream(t, up.root, "own", "s2")
		testGit(t, bare, "config", "http.receivepack", "true")
		head := seedLocalRepo(t, f, "repo_s2")
		f.putUpstream(t, "repo_s2", "own", "s2", up.url)
		if err := f.store.RequestPush(t.Context(), "repo_s2", true); err != nil {
			t.Fatal(err)
		}
		job := Job{Org: "org", RepoID: "repo_s2", Retry: 2}
		err := runCancelledByShutdown(t, up, func(ctx context.Context) error { return f.worker.RunPush(ctx, job) })
		if !errors.Is(err, ErrShutdown) {
			t.Fatalf("RunPush = %v, want an error wrapping ErrShutdown", err)
		}
		r, _ := f.store.GetUpstream(t.Context(), "repo_s2")
		if r.PushRunCount != 0 || r.PushPending != PushPendingForce || r.PushSyncing || r.SyncPending {
			t.Fatalf("after the shutdown: %+v, want no run counted, force pending, no claim, no mirror-back", r)
		}
		assertEventTypes(t, eventsFor(f, "repo_s2"), EventSyncStarted)

		if err := f.worker.RunPush(t.Context(), job); err != nil {
			t.Fatalf("rerun: %v", err)
		}
		ev := eventsFor(f, "repo_s2")
		assertEventTypes(t, ev, EventSyncStarted, EventSyncStarted, EventSyncSucceeded)
		if !ev[1].Forced {
			t.Fatal("the rerun lost the kept force level")
		}
		if got := upstreamHead(t, bare); got != head {
			t.Fatalf("upstream main = %q, want %q", got, head)
		}
	})

	t.Run("cancelled before the claim", func(t *testing.T) {
		f := newWorkerFixture(t)
		f.putUpstream(t, "repo_s3", "own", "s3", "http://127.0.0.1:1")
		if err := f.store.RequestSync(t.Context(), "repo_s3"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(ErrShutdown)
		if err := f.worker.Run(ctx, Job{Org: "org", RepoID: "repo_s3"}); !errors.Is(err, ErrShutdown) {
			t.Fatalf("Run = %v, want ErrShutdown", err)
		}
		if r, _ := f.store.GetUpstream(t.Context(), "repo_s3"); r.Syncing || !r.SyncPending || r.RunCount != 0 {
			t.Fatalf("row %+v, want it untouched (request pending, no claim)", r)
		}
		if n := len(eventsFor(f, "repo_s3")); n != 0 {
			t.Fatalf("%d events, want none", n)
		}
	})
}

// stallRestoreStore is a MemStore whose RestoreSync hangs until its context
// ends, like a stalled database.
type stallRestoreStore struct{ *MemStore }

func (s stallRestoreStore) RestoreSync(ctx context.Context, _, _ string) (Upstream, error) {
	<-ctx.Done()
	return Upstream{}, ctx.Err()
}

// A stalled store cannot hold a shutdown restore past its bound (issue
// #343 review, P2): the run returns after shutdownRestoreTimeout.
func TestRunShutdownRestoreIsBounded(t *testing.T) {
	f := newWorkerFixture(t)
	f.worker.Upstreams = stallRestoreStore{f.store}
	up := newStallUpstream(t)
	seedUpstream(t, up.root, "own", "s4")
	f.putUpstream(t, "repo_s4", "own", "s4", up.url)
	f.addLoopbackRepo(t, "repo_s4")
	start := time.Now()
	var elapsed time.Duration
	err := runCancelledByShutdown(t, up, func(ctx context.Context) error {
		err := f.worker.Run(ctx, Job{Org: "org", RepoID: "repo_s4"})
		elapsed = time.Since(start)
		return err
	})
	if err == nil || errors.Is(err, ErrShutdown) {
		t.Fatalf("Run = %v, want the restore's failure", err)
	}
	if elapsed > shutdownRestoreTimeout+5*time.Second {
		t.Fatalf("Run returned after %v, want about shutdownRestoreTimeout (%v)", elapsed, shutdownRestoreTimeout)
	}
}
