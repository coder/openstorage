// Shutdown controls for the SQLite local schedule (issue #342): Drain
// waits for runs while serve's listener still serves, then cancels the rest
// and waits for them to record OutcomeRestore, which keeps each request
// with no repo.sync.failed.

package sync

import (
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// stallUpstream is a smart-HTTP git upstream that holds every request
// while hold is set, until release closes or the client goes away.
type stallUpstream struct {
	root    string
	url     string
	hold    atomic.Bool
	hit     chan struct{}
	release chan struct{}
}

func newStallUpstream(t *testing.T) *stallUpstream {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not on PATH")
	}
	u := &stallUpstream{root: t.TempDir(), hit: make(chan struct{}, 1), release: make(chan struct{})}
	backend := &cgi.Handler{Path: gitPath, Args: []string{"http-backend"},
		Env: []string{"GIT_PROJECT_ROOT=" + u.root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_NOSYSTEM=1", "HOME=" + u.root}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u.hold.Load() {
			select {
			case u.hit <- struct{}{}:
			default:
			}
			select {
			case <-u.release:
			case <-r.Context().Done():
				return
			}
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u.url = srv.URL
	return u
}

// held waits until a run's request is being held.
func (u *stallUpstream) held(t *testing.T) {
	t.Helper()
	select {
	case <-u.hit:
	case <-time.After(60 * time.Second):
		t.Fatal("no run reached the upstream")
	}
}

func (f *localSchedFixture) pullEvents(repoID string) []Event {
	var out []Event
	for _, e := range f.wf.rec.list() {
		if e.Direction != DirectionPush && e.Repository.ID == repoID {
			out = append(out, e)
		}
	}
	return out
}

// addPullRepo registers repo name mirroring host/own/name, with an empty
// loopback repository; returns the repo ID and the loopback bare path.
func (f *localSchedFixture) addPullRepo(t *testing.T, name, host string) (string, string) {
	t.Helper()
	repo, err := f.ms.CreateRepo(t.Context(), f.org, "https://example.com/"+name+".git", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: repo.RepoID, Provider: "gitea", Owner: "own", Name: name, UpstreamHost: host,
	}); err != nil {
		t.Fatal(err)
	}
	return repo.RepoID, f.wf.addLoopbackRepo(t, repo.RepoID)
}

// restart replaces the fixture's scheduler with a fresh one over the same
// database, as the next serve process would build it.
func (f *localSchedFixture) restart(t *testing.T) {
	t.Helper()
	f.sched.Close()
	f.sched = NewLocalScheduler(f.gate, f.org, f.wf.worker.Run, f.wf.worker.RunPush, discardTestLogger())
	t.Cleanup(f.sched.Close)
}

func TestLocalScheduleDrainKeepsInFlightRequests(t *testing.T) {
	t.Run("pull cancelled by the drain keeps its request", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		up := newStallUpstream(t)
		head := seedUpstream(t, up.root, "own", "d1")
		repo, loopback := f.addPullRepo(t, "d1", up.url)
		up.hold.Store(true)
		if err := f.sched.EnqueueSync(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
			t.Fatal(err)
		}
		up.held(t)

		// Drain returns only after the cancelled run recorded its restore.
		f.sched.Drain(t.Context(), 50*time.Millisecond)
		r := f.row(t, repo)
		if !r.SyncPending || r.Syncing || r.RunCount != 0 {
			t.Fatalf("after the drain: row %+v, want the request pending, unclaimed, no run counted", r)
		}
		assertEventTypes(t, f.pullEvents(repo), EventSyncStarted) // no repo.sync.failed

		// The next process runs the kept request once it is due.
		up.hold.Store(false)
		f.restart(t)
		f.advance(shutdownRestoreDelay)
		f.sweep(t)
		if r := f.row(t, repo); r.SyncPending || r.SuccessCount != 1 || r.RunCount != 1 {
			t.Fatalf("after restart: row %+v, want one successful run", r)
		}
		assertEventTypes(t, f.pullEvents(repo), EventSyncStarted, EventSyncStarted, EventSyncSucceeded)
		if got := upstreamHead(t, loopback); got != head {
			t.Fatalf("loopback main = %q, want %q", got, head)
		}
	})

	t.Run("push cancelled by the drain keeps force and retry position", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		up := newStallUpstream(t)
		bare := addEmptyUpstream(t, up.root, "own", "d2")
		testGit(t, bare, "config", "http.receivepack", "true")
		repo, head := f.addRepo(t, "d2", up.url)
		if err := f.ms.RequestPush(t.Context(), repo, true); err != nil {
			t.Fatal(err)
		}
		up.hold.Store(true)
		f.sched.Start(0, time.Hour) // one sweep pass starts the run
		up.held(t)

		f.sched.Drain(t.Context(), 50*time.Millisecond)
		r := f.row(t, repo)
		attempt, _ := f.readSchedule(t, repo)
		if r.PushPending != 2 || r.PushSyncing || r.PushRunCount != 0 || attempt != 0 {
			t.Fatalf("after the drain: row %+v attempt %d, want force pending, unclaimed, no run counted, attempt 0", r, attempt)
		}
		assertPushTypes(t, f.pushEvents(repo), EventSyncStarted)

		up.hold.Store(false)
		f.restart(t)
		f.advance(shutdownRestoreDelay)
		f.sweep(t)
		ev := f.pushEvents(repo)
		assertPushTypes(t, ev, EventSyncStarted, EventSyncStarted, EventSyncSucceeded)
		if !ev[1].Forced {
			t.Fatal("the rerun lost the kept force level")
		}
		if got := upstreamHead(t, bare); got != head {
			t.Fatalf("upstream main = %q, want %q", got, head)
		}
	})

	t.Run("a run that ends within the wait finishes normally", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		up := newStallUpstream(t)
		seedUpstream(t, up.root, "own", "d3")
		repo, _ := f.addPullRepo(t, "d3", up.url)
		up.hold.Store(true)
		if err := f.sched.EnqueueSync(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
			t.Fatal(err)
		}
		up.held(t)
		drained := make(chan struct{})
		go func() {
			f.sched.Drain(t.Context(), time.Hour) // waits for the run
			close(drained)
		}()
		// A request recorded during the drain is kept, but starts no run.
		other, _ := f.addPullRepo(t, "d3-other", up.url)
		deadline := time.Now().Add(10 * time.Second)
		for {
			f.sched.mu.Lock()
			draining := f.sched.draining
			f.sched.mu.Unlock()
			if draining {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("Drain never started")
			}
			time.Sleep(time.Millisecond)
		}
		if err := f.sched.EnqueueSync(t.Context(), Job{Org: f.org, RepoID: other}); err != nil {
			t.Fatalf("enqueue during the drain: %v", err)
		}
		select {
		case <-drained:
			t.Fatal("Drain returned while a run was in flight")
		case <-time.After(20 * time.Millisecond):
		}
		up.hold.Store(false)
		close(up.release)
		<-drained
		if r := f.row(t, repo); r.SuccessCount != 1 || r.SyncPending {
			t.Fatalf("run inside the wait: row %+v, want it finished successfully", r)
		}
		if r := f.row(t, other); !r.SyncPending || r.RunCount != 0 {
			t.Fatalf("request during the drain: row %+v, want it pending with no run", r)
		}
	})
}
