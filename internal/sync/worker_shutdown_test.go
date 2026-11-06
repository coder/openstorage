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
		assertEventTypes(t, eventsFor(f, "repo_s2"), EventPushUpstreamStarted)

		if err := f.worker.RunPush(t.Context(), job); err != nil {
			t.Fatalf("rerun: %v", err)
		}
		ev := eventsFor(f, "repo_s2")
		assertEventTypes(t, ev, EventPushUpstreamStarted, EventPushUpstreamStarted, EventPushUpstreamSucceeded)
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
	// Still marked as shutdown-caused: nothing was committed, so River
	// must keep the job (jobs.shutdownSnooze).
	if !errors.Is(err, ErrShutdown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want the restore's timeout, marked ErrShutdown", err)
	}
	if elapsed > shutdownRestoreTimeout+5*time.Second {
		t.Fatalf("Run returned after %v, want about shutdownRestoreTimeout (%v)", elapsed, shutdownRestoreTimeout)
	}
}

// stallRestorePushStore is a MemStore whose RestorePush hangs until its
// context ends, like a stalled database.
type stallRestorePushStore struct{ *MemStore }

func (s stallRestorePushStore) RestorePush(ctx context.Context, _, _ string, _ bool) (Upstream, error) {
	<-ctx.Done()
	return Upstream{}, ctx.Err()
}

// A River push run whose publication wait the drain cancels restores
// within shutdownRestoreTimeout, like a cancelled transfer, not within the
// longer detached finish budget.
func TestPushRunShutdownDuringPublicationWaitRestoreIsBounded(t *testing.T) {
	f := newWorkerFixture(t)
	f.worker.Upstreams = stallRestorePushStore{f.store}
	seedLocalRepo(t, f, "repo_s5")
	f.putUpstream(t, "repo_s5", "own", "s5", "file://"+f.upstreamDir)
	if err := f.store.RequestPush(t.Context(), "repo_s5", false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	f.worker.WaitPublication = func(wctx context.Context, _ string) error {
		cancel(ErrShutdown)
		<-wctx.Done()
		return wctx.Err()
	}
	start := time.Now()
	err := f.worker.RunPush(ctx, Job{Org: "org", RepoID: "repo_s5"})
	elapsed := time.Since(start)
	if !errors.Is(err, ErrShutdown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunPush = %v, want the restore's timeout, marked ErrShutdown", err)
	}
	if elapsed > shutdownRestoreTimeout+5*time.Second {
		t.Fatalf("RunPush returned after %v, want about shutdownRestoreTimeout (%v)", elapsed, shutdownRestoreTimeout)
	}
}

// failRestorePushStore is a MemStore whose RestorePush runs before and
// then fails, like a database that went away.
type failRestorePushStore struct {
	*MemStore
	before func()
}

func (s failRestorePushStore) RestorePush(context.Context, string, string, bool) (Upstream, error) {
	s.before()
	return Upstream{}, errors.New("database unreachable")
}

// A drain that cancels a River push run while it restores after a failed
// publication wait still marks the run's error as shutdown-caused, so
// River keeps the job (jobs.shutdownSnooze) instead of completing it with
// the claim held and the request consumed.
func TestPushRunShutdownDuringFailedWaitRestoreKeepsJob(t *testing.T) {
	f := newWorkerFixture(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	f.worker.Upstreams = failRestorePushStore{MemStore: f.store, before: func() { cancel(ErrShutdown) }}
	seedLocalRepo(t, f, "repo_s6")
	f.putUpstream(t, "repo_s6", "own", "s6", "file://"+f.upstreamDir)
	if err := f.store.RequestPush(t.Context(), "repo_s6", false); err != nil {
		t.Fatal(err)
	}
	f.worker.WaitPublication = func(context.Context, string) error { return errors.New("hold unreadable") }
	if err := f.worker.RunPush(ctx, Job{Org: "org", RepoID: "repo_s6"}); !errors.Is(err, ErrShutdown) {
		t.Fatalf("RunPush = %v, want ErrShutdown", err)
	}
}

// stallClaimStore is a MemStore whose BeginSync hangs until its context
// ends, like a stalled database.
type stallClaimStore struct {
	*MemStore
	entered chan struct{}
}

func (s stallClaimStore) BeginSync(ctx context.Context, _, _ string, _ time.Duration) (Upstream, string, error) {
	close(s.entered)
	<-ctx.Done()
	return Upstream{}, "", ctx.Err()
}

// A stalled claim write cannot hold shutdown past its bound either (issue
// #343 review): it sees the drain's cancellation after
// shutdownRestoreTimeout, and the run reports ErrShutdown so River keeps
// its job.
func TestRunShutdownClaimIsBounded(t *testing.T) {
	f := newWorkerFixture(t)
	st := stallClaimStore{MemStore: f.store, entered: make(chan struct{})}
	f.worker.Upstreams = st
	f.putUpstream(t, "repo_s5", "own", "s5", "http://127.0.0.1:1")
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	errc := make(chan error, 1)
	go func() { errc <- f.worker.Run(ctx, Job{Org: "org", RepoID: "repo_s5"}) }()
	<-st.entered
	start := time.Now()
	cancel(ErrShutdown)
	select {
	case err := <-errc:
		if !errors.Is(err, ErrShutdown) {
			t.Fatalf("Run = %v, want ErrShutdown", err)
		}
		if d := time.Since(start); d > shutdownRestoreTimeout+5*time.Second {
			t.Fatalf("claim held shutdown for %v", d)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("stalled claim ignored the shutdown bound")
	}
}
