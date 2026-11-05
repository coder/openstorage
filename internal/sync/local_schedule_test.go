// Worker-level controls for the local schedule (issue #266 PR2, ADR-50)
// on SQLite and, with OPENSTORAGE_TEST_BACKEND=s3wal, on S3WAL metadata
// in a SeaweedFS bucket (ADR-51, local_schedule_s3wal_test.go): real metadata, real git transfers,
// store time injected through meta.WithNowFunc. Every control drives the
// scheduler through SweepOnce and explicit enqueues; nothing sleeps for a
// backoff.

package sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// gateLanes wraps LocalLanes so a test can pause or fail one method for
// one repo, and record calls made after the scheduler closed.
type gateLanes struct {
	LocalLanes
	mu      stdsync.Mutex
	holds   map[string]*laneHold
	fails   map[string]error
	closed  atomic.Bool
	lateOps atomic.Int64
}

type laneHold struct {
	reached chan struct{}
	release chan struct{}
	once    stdsync.Once
}

func newGateLanes(inner LocalLanes) *gateLanes {
	return &gateLanes{LocalLanes: inner, holds: map[string]*laneHold{}, fails: map[string]error{}}
}

// hold pauses the next and every later call of method for repo until the
// returned release runs; reached closes when a call is paused.
func (g *gateLanes) hold(method, repo string) (reached <-chan struct{}, release func()) {
	h := &laneHold{reached: make(chan struct{}), release: make(chan struct{})}
	g.mu.Lock()
	g.holds[method+":"+repo] = h
	g.mu.Unlock()
	return h.reached, func() {
		g.mu.Lock()
		delete(g.holds, method+":"+repo)
		g.mu.Unlock()
		close(h.release)
	}
}

func (g *gateLanes) fail(method, repo string, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err == nil {
		delete(g.fails, method+":"+repo)
		return
	}
	g.fails[method+":"+repo] = err
}

func (g *gateLanes) enter(method, repo string) error {
	if g.closed.Load() {
		g.lateOps.Add(1)
	}
	g.mu.Lock()
	h := g.holds[method+":"+repo]
	err := g.fails[method+":"+repo]
	g.mu.Unlock()
	if h != nil {
		h.once.Do(func() { close(h.reached) })
		<-h.release
	}
	return err
}

func (g *gateLanes) RequestSync(ctx context.Context, repoID string) error {
	if err := g.enter("RequestSync", repoID); err != nil {
		return err
	}
	return g.LocalLanes.RequestSync(ctx, repoID)
}

func (g *gateLanes) ClaimDuePush(ctx context.Context, repoID, holder string, lease time.Duration) (LaneClaim, error) {
	if err := g.enter("ClaimDuePush", repoID); err != nil {
		return LaneClaim{}, err
	}
	return g.LocalLanes.ClaimDuePush(ctx, repoID, holder, lease)
}

func (g *gateLanes) ClaimDueSync(ctx context.Context, repoID, holder string, lease time.Duration) (LaneClaim, error) {
	if err := g.enter("ClaimDueSync", repoID); err != nil {
		return LaneClaim{}, err
	}
	return g.LocalLanes.ClaimDueSync(ctx, repoID, holder, lease)
}

func (g *gateLanes) FinishPushOutcome(ctx context.Context, repoID, holder string, o meta.PushOutcome) (Upstream, error) {
	if err := g.enter("FinishPushOutcome", repoID); err != nil {
		return Upstream{}, err
	}
	up, err := g.LocalLanes.FinishPushOutcome(ctx, repoID, holder, o)
	if gerr := g.enter("FinishPushOutcome:after", repoID); gerr != nil {
		return Upstream{}, gerr
	}
	return up, err
}

func (g *gateLanes) FinishSyncOutcome(ctx context.Context, repoID, holder string, o meta.SyncOutcome) (Upstream, error) {
	if err := g.enter("FinishSyncOutcome", repoID); err != nil {
		return Upstream{}, err
	}
	return g.LocalLanes.FinishSyncOutcome(ctx, repoID, holder, o)
}

func (g *gateLanes) ListLocalWork(ctx context.Context, org, after string, limit int) ([]meta.LocalWork, error) {
	if err := g.enter("ListLocalWork", "*"); err != nil {
		return nil, err
	}
	return g.LocalLanes.ListLocalWork(ctx, org, after, limit)
}

// httpUpstream is a git smart-HTTP upstream (git http-backend over CGI)
// that can be taken down and brought back on the same address, so one repo
// can fail as unreachable and later succeed.
type httpUpstream struct {
	root string
	addr string
	srv  *http.Server
}

func newHTTPUpstream(t *testing.T) *httpUpstream {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not on PATH")
	}
	u := &httpUpstream{root: t.TempDir()}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	u.addr = ln.Addr().String()
	u.serve(ln, gitPath)
	t.Cleanup(func() {
		if u.srv != nil {
			_ = u.srv.Close()
		}
	})
	return u
}

func (u *httpUpstream) serve(ln net.Listener, gitPath string) {
	u.srv = &http.Server{Handler: &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + u.root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_NOSYSTEM=1", "HOME=" + u.root},
	}, ReadHeaderTimeout: 10 * time.Second}
	srv := u.srv
	go func() { _ = srv.Serve(ln) }()
}

func (u *httpUpstream) host() string { return "http://" + u.addr }

func (u *httpUpstream) down(t *testing.T) {
	t.Helper()
	if err := u.srv.Close(); err != nil {
		t.Fatal(err)
	}
	u.srv = nil
}

func (u *httpUpstream) up(t *testing.T) {
	t.Helper()
	gitPath, _ := exec.LookPath("git")
	deadline := time.Now().Add(5 * time.Second)
	for {
		ln, err := net.Listen("tcp", u.addr)
		if err == nil {
			u.serve(ln, gitPath)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("relisten %s: %v", u.addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// addRepo creates root/owner/name.git accepting pushes over HTTP.
func (u *httpUpstream) addRepo(t *testing.T, owner, name string) string {
	t.Helper()
	bare := addEmptyUpstream(t, u.root, owner, name)
	testGit(t, bare, "config", "http.receivepack", "true")
	return bare
}

// localSchedFixture is a worker plus LocalScheduler over a real SQLite or
// S3WAL store with an injected, mutable clock.
type localSchedFixture struct {
	wf        *workerFixture
	ms        localSchedStore
	ls        meta.LocalScheduleStore
	db        *sql.DB                // SQLite: read-only view of the schedule columns
	cas       s3wal.ConditionalStore // S3WAL: the bucket holding the metadata document
	gate      *gateLanes
	sched     *LocalScheduler
	clock     atomic.Int64
	org       string
	lookupErr stdsync.Map // repoID -> error returned by the worker's repo lookup
}

// localSchedStore is the metadata surface the fixture and its tests use.
type localSchedStore interface {
	meta.Backend
	meta.EventingStore
}

// newLocalSchedFixture builds the fixture on the backend that
// OPENSTORAGE_TEST_BACKEND selects: unset for SQLite, s3wal for S3WAL.
func newLocalSchedFixture(t *testing.T) *localSchedFixture {
	t.Helper()
	f := &localSchedFixture{wf: newWorkerFixture(t), org: "sched-org"}
	// Store time starts at wall time: the worker derives its loopback
	// token TTL from claim expiries with the real clock.
	f.clock.Store(time.Now().UnixNano())
	now := meta.WithNowFunc(func() time.Time { return time.Unix(0, f.clock.Load()) })
	switch backend := envconfig.Getenv("OPENSTORAGE_TEST_BACKEND"); backend {
	case "", "local", "pg":
		// local and pg name internal/githttp lanes; this package has no
		// PG schedule, so they run SQLite here.
		dbPath := filepath.Join(t.TempDir(), "meta.sqlite")
		ms, err := meta.OpenSQLite(t.Context(), "sqlite://"+dbPath, now)
		if err != nil {
			t.Fatal(err)
		}
		f.ms = ms
		db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		f.db = db
	case "s3wal":
		f.cas = seaweedScheduleStore(t)
		f.ms = meta.NewS3WAL(f.cas, now)
	default:
		t.Fatalf("OPENSTORAGE_TEST_BACKEND=%q: unknown backend (want s3wal, local, pg, or unset)", backend)
	}
	t.Cleanup(f.ms.Close)
	if err := f.ms.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ms.UpsertOrgWithKeys(t.Context(), f.org, []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatal(err)
	}
	ls, ok := meta.LocalSchedule(f.ms)
	if !ok {
		t.Fatalf("%T does not provide the local schedule", f.ms)
	}
	f.ls = ls
	lanes, ok := MetaLocalLanes(f.ms)
	if !ok {
		t.Fatalf("%T does not provide local lanes", f.ms)
	}
	f.gate = newGateLanes(lanes)
	creds, ups := MetaStores(f.ms)
	w := f.wf.worker
	w.Upstreams, w.Credentials, w.Local = ups, creds, f.gate
	w.Repos = func(_ context.Context, _, repoID string) (RepoInfo, error) {
		if err, ok := f.lookupErr.Load(repoID); ok {
			return RepoInfo{}, err.(error)
		}
		return RepoInfo{RepoID: repoID, URL: "acme/" + repoID}, nil
	}
	f.sched = NewLocalScheduler(f.gate, f.org, w.Run, w.RunPush, discardTestLogger())
	t.Cleanup(f.sched.Close)
	return f
}

func (f *localSchedFixture) advance(d time.Duration) { f.clock.Add(d.Nanoseconds()) }

func (f *localSchedFixture) now() time.Time { return time.Unix(0, f.clock.Load()) }

// addRepo registers a repo with an upstream at host/owner/name and seeds
// its loopback (local) repository; returns the repo ID and local head.
func (f *localSchedFixture) addRepo(t *testing.T, name, host string) (string, string) {
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
	return repo.RepoID, seedLocalRepo(t, f.wf, repo.RepoID)
}

func (f *localSchedFixture) row(t *testing.T, repoID string) meta.RepoUpstream {
	t.Helper()
	up, err := f.ms.GetRepoUpstream(t.Context(), repoID)
	if err != nil {
		t.Fatal(err)
	}
	return up
}

// readSchedule reads the schedule fields (push_attempt, push_due) straight
// from the store: the SQLite database file, or the S3WAL v2 document.
func (f *localSchedFixture) readSchedule(t *testing.T, repoID string) (attempt int, due int64) {
	t.Helper()
	if f.cas != nil {
		obj, err := f.cas.Get(t.Context(), "_wal/meta.json")
		if err != nil {
			t.Fatalf("read schedule: %v", err)
		}
		var doc struct {
			V         int `json:"v"`
			Upstreams map[string]struct {
				PushAttempt int   `json:"push_attempt"`
				PushDue     int64 `json:"push_due"`
			} `json:"upstreams"`
		}
		if err := json.Unmarshal(obj.Body, &doc); err != nil {
			t.Fatalf("read schedule: %v", err)
		}
		up, ok := doc.Upstreams[repoID]
		if doc.V != 2 || !ok {
			t.Fatalf("read schedule: document v%d, upstream %q present=%v", doc.V, repoID, ok)
		}
		return up.PushAttempt, up.PushDue
	}
	if err := f.db.QueryRowContext(t.Context(),
		`SELECT push_attempt, push_due FROM repo_upstreams WHERE repo_id = ?`, repoID).Scan(&attempt, &due); err != nil {
		t.Fatalf("read schedule: %v", err)
	}
	return attempt, due
}

// pushEvents returns the outbound events recorded for repoID.
func (f *localSchedFixture) pushEvents(repoID string) []Event {
	var out []Event
	for _, e := range f.wf.rec.list() {
		if e.Direction == DirectionPush && e.Repository.ID == repoID {
			out = append(out, e)
		}
	}
	return out
}

// sweep runs one pass and waits until every job it started has finished.
func (f *localSchedFixture) sweep(t *testing.T) {
	t.Helper()
	if err := f.sched.SweepOnce(t.Context()); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	f.idle(t)
}

// idle waits until no scheduler job is queued or running.
func (f *localSchedFixture) idle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		f.sched.mu.Lock()
		n := len(f.sched.inflight)
		f.sched.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("scheduler still has %d jobs in flight", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func upstreamHead(t *testing.T, bare string) string {
	t.Helper()
	cmd := exec.Command("git", "--git-dir", bare, "rev-parse", "--verify", "-q", "refs/heads/main")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return string(out[:len(out)-1])
}

func assertPushTypes(t *testing.T, events []Event, want ...string) {
	t.Helper()
	assertEventTypes(t, events, want...)
}

// C1: the final #271 P2 replayed. A failing run's retry is one write; a
// covering explicit run resets the schedule; later hook intent is not
// held back by the old backoff.
func TestLocalScheduleFinalP2Replay(t *testing.T) {
	f := newLocalSchedFixture(t)
	up := newHTTPUpstream(t)
	bare := up.addRepo(t, "own", "c1")
	repo, head := f.addRepo(t, "c1", up.host())
	up.down(t)

	if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
		t.Fatal(err)
	}
	reached, release := f.gate.hold("FinishPushOutcome", repo)
	if err := f.sched.SweepOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-reached // run A failed and is paused just before its end-of-run write
	if r := f.row(t, repo); !r.PushSyncing || r.PushPending != 0 {
		t.Fatalf("while A finishes: row %+v, want A's live claim", r)
	}
	// A second run cannot take the lane while A holds it.
	if err := f.wf.worker.RunPush(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
		t.Fatalf("overlapping run: %v", err)
	}
	release()
	f.idle(t)
	r := f.row(t, repo)
	attempt, due := f.readSchedule(t, repo)
	if r.PushSyncing || r.PushPending != 1 || attempt != 1 || due != f.now().Add(time.Minute).UnixNano() {
		t.Fatalf("after A: row %+v attempt %d due %d; want pending 1, attempt 1, due +1m in one write", r, attempt, due)
	}
	assertPushTypes(t, f.pushEvents(repo), EventPushUpstreamStarted, EventPushUpstreamFailed)

	// Not due: a sweep starts nothing.
	f.sweep(t)
	if n := len(f.pushEvents(repo)); n != 2 {
		t.Fatalf("sweep during backoff started a run (%d events)", n)
	}

	// The upstream recovers; an explicit request (API push-upstream:
	// RequestPushNow, then the hint) expedites and covers the retry.
	up.up(t)
	if err := f.ls.RequestPushNow(t.Context(), repo, false); err != nil {
		t.Fatal(err)
	}
	if err := f.sched.EnqueuePush(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
		t.Fatal(err)
	}
	f.idle(t)
	assertPushTypes(t, f.pushEvents(repo), EventPushUpstreamStarted, EventPushUpstreamFailed, EventPushUpstreamStarted, EventPushUpstreamSucceeded)
	if got := upstreamHead(t, bare); got != head {
		t.Fatalf("upstream main = %q, want %q", got, head)
	}
	if attempt, due := f.readSchedule(t, repo); attempt != 0 || due != 0 {
		t.Fatalf("after covering run: attempt %d due %d, want reset", attempt, due)
	}

	// Later hook intent runs on the next sweep, not after A's old backoff.
	if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
		t.Fatal(err)
	}
	f.sweep(t)
	ev := f.pushEvents(repo)
	if len(ev) != 6 || ev[5].Type != EventPushUpstreamSucceeded {
		t.Fatalf("hook intent after the covering run: events %+v", ev)
	}
	if f.wf.worker.deferredRetry != nil {
		t.Fatalf("local schedule used process-local retry timers: %v", f.wf.worker.deferredRetry)
	}
}

// C2: genuinely newer intent survives every run outcome.
func TestLocalScheduleNewerIntentSurvives(t *testing.T) {
	t.Run("mid-run requests after success", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		bare := addEmptyUpstream(t, f.wf.upstreamDir, "own", "c2a")
		repo, head := f.addRepo(t, "c2a", "file://"+f.wf.upstreamDir)
		if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
			t.Fatal(err)
		}
		reached, release := f.gate.hold("FinishPushOutcome", repo)
		if err := f.sched.SweepOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		<-reached
		if err := f.ms.RequestPush(t.Context(), repo, false); err != nil { // hook
			t.Fatal(err)
		}
		if err := f.ms.RequestPush(t.Context(), repo, true); err != nil { // forced API level
			t.Fatal(err)
		}
		release()
		f.idle(t)
		if r := f.row(t, repo); r.PushPending != 2 {
			t.Fatalf("mid-run intent lost: %+v", r)
		}
		f.sweep(t)
		ev := f.pushEvents(repo)
		if len(ev) != 4 || !ev[2].Forced || ev[3].Type != EventPushUpstreamSucceeded {
			t.Fatalf("follow-up run: %+v, want a forced successful second run", ev)
		}
		if upstreamHead(t, bare) != head {
			t.Fatal("upstream not updated")
		}
	})
	t.Run("mid-run hook request joins the backoff; explicit request stays due", func(t *testing.T) {
		for _, explicit := range []bool{false, true} {
			f := newLocalSchedFixture(t)
			repo, _ := f.addRepo(t, fmt.Sprintf("c2b%v", explicit), "http://127.0.0.1:1")
			if err := f.ms.RequestPush(t.Context(), repo, true); err != nil {
				t.Fatal(err)
			}
			reached, release := f.gate.hold("FinishPushOutcome", repo)
			if err := f.sched.SweepOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			<-reached
			var err error
			if explicit {
				err = f.ls.RequestPushNow(t.Context(), repo, false)
			} else {
				err = f.ms.RequestPush(t.Context(), repo, false)
			}
			if err != nil {
				t.Fatal(err)
			}
			release()
			f.idle(t)
			attempt, due := f.readSchedule(t, repo)
			wantDue := f.now().Add(time.Minute).UnixNano()
			if explicit {
				wantDue = 0
			}
			if r := f.row(t, repo); r.PushPending != 2 || attempt != 1 || due != wantDue {
				t.Fatalf("explicit=%v: row %+v attempt %d due %d, want level 2 attempt 1 due %d", explicit, r, attempt, due, wantDue)
			}
			f.sweep(t)
			wantRuns := 1
			if explicit {
				wantRuns = 2 // the explicit request runs at once, forced, as attempt 1
			}
			if n := len(f.pushEvents(repo)) / 2; n != wantRuns {
				t.Fatalf("explicit=%v: %d runs after one sweep, want %d", explicit, n, wantRuns)
			}
			if explicit && !f.pushEvents(repo)[2].Forced {
				t.Fatal("force downgraded")
			}
		}
	})
	t.Run("hook request during a backoff waits for it", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		repo, _ := f.addRepo(t, "c2c", "http://127.0.0.1:1")
		if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
			t.Fatal(err)
		}
		f.sweep(t) // fails: backoff 1m
		if err := f.ms.RequestPush(t.Context(), repo, true); err != nil {
			t.Fatal(err)
		}
		f.advance(time.Minute - time.Nanosecond)
		f.sweep(t)
		if n := len(f.pushEvents(repo)); n != 2 {
			t.Fatalf("run started before the backoff ended (%d events)", n)
		}
		f.advance(time.Nanosecond)
		f.sweep(t)
		ev := f.pushEvents(repo)
		if len(ev) != 4 || !ev[2].Forced {
			t.Fatalf("coalesced hook intent at the due time: %+v", ev)
		}
	})
}

// C3: one repo's blocked store call does not hold back another repo.
func TestLocalScheduleUnrelatedReposProceed(t *testing.T) {
	for _, method := range []string{"ClaimDuePush", "FinishPushOutcome"} {
		t.Run(method, func(t *testing.T) {
			f := newLocalSchedFixture(t)
			x, _ := f.addRepo(t, "x", "file://"+f.wf.upstreamDir)
			addEmptyUpstream(t, f.wf.upstreamDir, "own", "x")
			y, yHead := f.addRepo(t, "y", "file://"+f.wf.upstreamDir)
			yBare := addEmptyUpstream(t, f.wf.upstreamDir, "own", "y")
			for _, r := range []string{x, y} {
				if err := f.ms.RequestPush(t.Context(), r, false); err != nil {
					t.Fatal(err)
				}
			}
			reached, release := f.gate.hold(method, x)
			defer release()
			if err := f.sched.SweepOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			<-reached
			// Repeated sweeps never re-offer x while its job is in flight.
			for range 3 {
				if err := f.sched.SweepOnce(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(60 * time.Second)
			for upstreamHead(t, yBare) != yHead {
				if time.Now().After(deadline) {
					t.Fatal("repo y did not forward while x's store call was blocked")
				}
				time.Sleep(10 * time.Millisecond)
			}
			f.sched.mu.Lock()
			xJobs := 0
			for k := range f.sched.inflight {
				if k == "push:"+x {
					xJobs++
				}
			}
			f.sched.mu.Unlock()
			if xJobs != 1 {
				t.Fatalf("x occupies %d scheduler entries, want 1", xJobs)
			}
		})
	}
	t.Run("ListLocalWork", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		y, yHead := f.addRepo(t, "y2", "file://"+f.wf.upstreamDir)
		yBare := addEmptyUpstream(t, f.wf.upstreamDir, "own", "y2")
		reached, release := f.gate.hold("ListLocalWork", "*")
		defer release()
		go func() { _ = f.sched.SweepOnce(context.Background()) }()
		<-reached
		if err := f.ms.RequestPush(t.Context(), y, false); err != nil {
			t.Fatal(err)
		}
		if err := f.sched.EnqueuePush(t.Context(), Job{Org: f.org, RepoID: y}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(60 * time.Second)
		for upstreamHead(t, yBare) != yHead {
			if time.Now().After(deadline) {
				t.Fatal("explicit request did not run while the sweep's read was blocked")
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// C4: shutdown leaves no timers or late writes; a restart honors the
// durable backoff and claims it exactly once.
func TestLocalScheduleShutdownAndRestart(t *testing.T) {
	f := newLocalSchedFixture(t)
	up := newHTTPUpstream(t)
	bare := up.addRepo(t, "own", "c4")
	repo, head := f.addRepo(t, "c4", up.host())
	up.down(t)
	if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
		t.Fatal(err)
	}
	f.sched.Start(0, 5*time.Millisecond)
	deadline := time.Now().Add(60 * time.Second)
	for len(f.pushEvents(repo)) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("first run did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.idle(t)
	f.sched.Close()
	f.gate.closed.Store(true)
	time.Sleep(50 * time.Millisecond) // several sweep intervals
	if n := f.gate.lateOps.Load(); n != 0 {
		t.Fatalf("%d store calls after Close returned", n)
	}
	if err := f.sched.EnqueuePush(t.Context(), Job{Org: f.org, RepoID: repo}); err == nil {
		t.Fatal("enqueue after Close succeeded")
	}
	if f.wf.worker.deferredRetry != nil {
		t.Fatal("process-local retry timer armed")
	}

	// Restart: a fresh scheduler over the same database.
	f.gate.closed.Store(false)
	up.up(t)
	restarted := NewLocalScheduler(f.gate, f.org, f.wf.worker.Run, f.wf.worker.RunPush, discardTestLogger())
	defer restarted.Close()
	f.sched = restarted
	f.sweep(t)
	if n := len(f.pushEvents(repo)); n != 2 {
		t.Fatalf("restart ignored the durable backoff (%d events)", n)
	}
	f.advance(time.Minute)
	f.sweep(t)
	f.sweep(t)
	ev := f.pushEvents(repo)
	if len(ev) != 4 || ev[3].Type != EventPushUpstreamSucceeded || upstreamHead(t, bare) != head {
		t.Fatalf("after the due time: %+v, want exactly one successful retry", ev)
	}
}

// C5: force and retry position survive pre-claim failures and advance only
// on transient failures, through the whole budget.
func TestLocalScheduleForceRetryPosition(t *testing.T) {
	f := newLocalSchedFixture(t)
	repo, _ := f.addRepo(t, "c5", "http://127.0.0.1:1")
	if err := f.ms.RequestPush(t.Context(), repo, true); err != nil {
		t.Fatal(err)
	}
	f.sweep(t)
	if err := f.ms.RequestPush(t.Context(), repo, false); err != nil { // plain hook request
		t.Fatal(err)
	}
	check := func(wantAttempt int, wantDue int64, wantLevel int) {
		t.Helper()
		attempt, due := f.readSchedule(t, repo)
		if r := f.row(t, repo); attempt != wantAttempt || due != wantDue || r.PushPending != wantLevel || r.PushSyncing {
			t.Fatalf("row %+v attempt %d due %d; want attempt %d due %d level %d", r, attempt, due, wantAttempt, wantDue, wantLevel)
		}
	}
	check(1, f.now().Add(time.Minute).UnixNano(), 2)
	f.advance(time.Minute)

	// Pre-claim failures write nothing.
	f.lookupErr.Store(repo, errors.New("lookup blip"))
	f.sweep(t)
	f.lookupErr.Delete(repo)
	f.gate.fail("ClaimDuePush", repo, errors.New("claim blip"))
	f.sweep(t)
	f.gate.fail("ClaimDuePush", repo, nil)
	check(1, f.now().UnixNano(), 2)

	for retry := 1; retry <= MaxPushTransientRetries; retry++ {
		f.sweep(t) // runs retry `retry`, forced, and fails
		ev := f.pushEvents(repo)
		if last := ev[len(ev)-2]; !last.Forced {
			t.Fatalf("retry %d ran without force", retry)
		}
		if retry < MaxPushTransientRetries {
			check(retry+1, f.now().Add(PushRetryDelay(retry+1)).UnixNano(), 2)
			f.advance(PushRetryDelay(retry + 1))
		}
	}
	// The failure of retry 5 (the sixth attempt) ends the chain.
	check(0, 0, 0)
	if n := len(f.pushEvents(repo)); n != 2*(MaxPushTransientRetries+1) {
		t.Fatalf("%d events, want %d attempts", n/2, MaxPushTransientRetries+1)
	}
}

// C7: stale and duplicate hints never create runs without durable work,
// and a stuck repo does not starve the others.
func TestLocalScheduleStaleHintsAndCapacity(t *testing.T) {
	f := newLocalSchedFixture(t)
	repo, _ := f.addRepo(t, "c7", "file://"+f.wf.upstreamDir)
	addEmptyUpstream(t, f.wf.upstreamDir, "own", "c7")
	// No durable work: direct runs and sweeps do nothing.
	if err := f.wf.worker.RunPush(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
		t.Fatal(err)
	}
	if err := f.wf.worker.Run(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
		t.Fatal(err)
	}
	f.sweep(t)
	if n := len(f.wf.rec.list()); n != 0 {
		t.Fatalf("runs without durable work: %d events", n)
	}
	// Duplicate hints for one request run it once.
	if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := f.sched.EnqueuePush(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
			t.Fatal(err)
		}
	}
	f.sweep(t)
	f.sweep(t)
	if n := len(f.pushEvents(repo)); n != 2 {
		t.Fatalf("duplicate hints ran %d times", n/2)
	}

	// More due repos than slots, one of them stuck in its claim.
	var repos []string
	for i := range localSweepSlots + 2 {
		name := fmt.Sprintf("cap%d", i)
		r, _ := f.addRepo(t, name, "file://"+f.wf.upstreamDir)
		addEmptyUpstream(t, f.wf.upstreamDir, "own", name)
		if err := f.ms.RequestPush(t.Context(), r, false); err != nil {
			t.Fatal(err)
		}
		repos = append(repos, r)
	}
	reached, release := f.gate.hold("ClaimDuePush", repos[0])
	defer release()
	// The stuck repo gets a slot on some pass (passes rotate by repo ID and
	// free slots as runs finish); keep sweeping until it is stuck.
	deadline := time.Now().Add(60 * time.Second)
	for stuck := false; !stuck; {
		if err := f.sched.SweepOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		select {
		case <-reached:
			stuck = true
		case <-time.After(5 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("stuck repo was never offered")
		}
	}
	for _, r := range repos[1:] {
		for len(f.pushEvents(r)) < 2 {
			if time.Now().After(deadline) {
				t.Fatalf("repo %s starved behind a stuck repo", r)
			}
			if err := f.sched.SweepOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// C8 and C9: a run that dies after its claim, or whose end-of-run write
// fails, leaves the claim to expire; the steal keeps attempt and force.
func TestLocalScheduleClaimRecovery(t *testing.T) {
	for _, mode := range []string{"crash", "finish-write-fails"} {
		t.Run(mode, func(t *testing.T) {
			f := newLocalSchedFixture(t)
			repo, _ := f.addRepo(t, "c8"+mode, "http://127.0.0.1:1")
			if err := f.ms.RequestPush(t.Context(), repo, true); err != nil {
				t.Fatal(err)
			}
			f.sweep(t) // attempt 0 fails: attempt 1 due in 1m
			f.advance(time.Minute)
			switch mode {
			case "crash":
				c, err := f.ls.ClaimDuePush(t.Context(), repo, "dead-process", claimLease(time.Minute))
				if err != nil || !c.Claimed {
					t.Fatalf("simulated claim: %+v %v", c, err)
				}
			case "finish-write-fails":
				f.gate.fail("FinishPushOutcome", repo, errors.New("disk blip"))
				f.sweep(t)
				f.gate.fail("FinishPushOutcome", repo, nil)
				if r := f.row(t, repo); !r.PushSyncing {
					t.Fatalf("failed end-of-run write released the claim: %+v", r)
				}
			}
			before := len(f.pushEvents(repo))
			f.sweep(t)
			if len(f.pushEvents(repo)) != before {
				t.Fatal("claim taken over before its lease expired")
			}
			f.advance(claimLease(time.Minute))
			f.sweep(t)
			ev := f.pushEvents(repo)
			if len(ev) != before+2 || !ev[before].Forced {
				t.Fatalf("recovery run: %+v, want one forced run", ev[before:])
			}
			if attempt, _ := f.readSchedule(t, repo); attempt != 2 {
				t.Fatalf("attempt after recovered retry 1 = %d, want 2", attempt)
			}
		})
	}
}

// C11: mirror-back intent and the initial sync are durable.
func TestLocalScheduleMirrorBackAndInitialSyncDurable(t *testing.T) {
	f := newLocalSchedFixture(t)
	seedUpstream(t, f.wf.upstreamDir, "own", "c11")
	repo, err := f.ms.CreateRepo(t.Context(), f.org, "https://example.com/c11.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: repo.RepoID, Provider: "gitea", Owner: "own", Name: "c11", UpstreamHost: "file://" + f.wf.upstreamDir,
	}); err != nil {
		t.Fatal(err)
	}
	f.wf.addLoopbackRepo(t, repo.RepoID)
	// The initial sync's hint is dropped (scheduler closed); its durable
	// request still reaches a fresh scheduler.
	f.sched.Close()
	if err := f.sched.EnqueueSync(t.Context(), Job{Org: f.org, RepoID: repo.RepoID}); err == nil {
		t.Fatal("enqueue on a closed scheduler succeeded")
	}
	if err := f.gate.RequestSync(t.Context(), repo.RepoID); err != nil {
		t.Fatal(err)
	}
	f.sched = NewLocalScheduler(f.gate, f.org, f.wf.worker.Run, f.wf.worker.RunPush, discardTestLogger())
	defer f.sched.Close()
	f.sweep(t)
	if r := f.row(t, repo.RepoID); r.SuccessCount != 1 {
		t.Fatalf("initial sync did not run from durable intent: %+v", r)
	}
	// A successful forward marks its mirror-back in the same write.
	if err := f.ms.RequestPush(t.Context(), repo.RepoID, false); err != nil {
		t.Fatal(err)
	}
	reached, release := f.gate.hold("ClaimDueSync", repo.RepoID)
	f.sweep(t) // the forward succeeds
	select {
	case <-reached:
		t.Fatal("mirror-back pull ran inside the forward's run instead of a later pass")
	default:
	}
	release()
	if r := f.row(t, repo.RepoID); !r.SyncPending || r.PushSuccessCount != 1 {
		t.Fatalf("forward success did not mark the mirror-back: %+v", r)
	}
	f.sweep(t)
	if r := f.row(t, repo.RepoID); r.SuccessCount != 2 || r.SyncPending {
		t.Fatalf("mirror-back pull did not run: %+v", r)
	}
}

// TestLocalScheduleEnqueueHints: on SQLite the callers record requests and
// the scheduler's enqueues only offer runs (review of #341). An implicit
// forward poke joins a waiting backoff, a pull-upstream offer does not
// re-mark a request a run already claimed, and Close waits for an enqueue
// that passed its gate.
func TestLocalScheduleEnqueueHints(t *testing.T) {
	t.Run("forward poke joins the backoff; explicit push-upstream expedites", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		up := newHTTPUpstream(t)
		bare := up.addRepo(t, "own", "h1")
		repo, head := f.addRepo(t, "h1", up.host())
		up.down(t)
		if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
			t.Fatal(err)
		}
		f.sweep(t) // transient failure: attempt 1, due in 1 min
		_, due := f.readSchedule(t, repo)
		// enqueueSyncOrForward on API ref writes: RequestPush, then the hint.
		for range 3 {
			if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
				t.Fatal(err)
			}
			if err := f.sched.EnqueuePush(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
				t.Fatal(err)
			}
			f.idle(t)
		}
		if attempt, got := f.readSchedule(t, repo); attempt != 1 || got != due || len(f.pushEvents(repo)) != 2 {
			t.Fatalf("forward pokes during the backoff: attempt %d due %d events %d; want attempt 1, due %d, no new run",
				attempt, got, len(f.pushEvents(repo)), due)
		}
		// API push-upstream records through the upstream store adapter.
		_, ups := MetaStores(f.ms)
		pn, ok := ups.(interface {
			RequestPushNow(ctx context.Context, repoID string, force bool) error
		})
		if !ok {
			t.Fatal("meta upstream store has no RequestPushNow")
		}
		up.up(t)
		if err := pn.RequestPushNow(t.Context(), repo, false); err != nil {
			t.Fatal(err)
		}
		if err := f.sched.EnqueuePush(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
			t.Fatal(err)
		}
		f.idle(t)
		assertPushTypes(t, f.pushEvents(repo), EventPushUpstreamStarted, EventPushUpstreamFailed, EventPushUpstreamStarted, EventPushUpstreamSucceeded)
		if got := upstreamHead(t, bare); got != head {
			t.Fatalf("upstream main = %q, want %q", got, head)
		}
	})
	t.Run("pull-upstream offer does not re-mark a claimed request", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		seedUpstream(t, f.wf.upstreamDir, "own", "h2")
		repo, _ := f.addRepo(t, "h2", "file://"+f.wf.upstreamDir)
		if err := f.ms.RequestSync(t.Context(), repo); err != nil { // the handler's write
			t.Fatal(err)
		}
		reached, release := f.gate.hold("FinishSyncOutcome", repo)
		if err := f.sched.SweepOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		<-reached // a sweep-started run claimed the request before the handler offered
		if err := f.sched.OfferSync(t.Context(), Job{Org: f.org, RepoID: repo}); err != nil {
			t.Fatal(err)
		}
		release()
		f.idle(t)
		f.sweep(t)
		if r := f.row(t, repo); r.SuccessCount != 1 || r.SyncPending {
			t.Fatalf("one pull-upstream request: row %+v, want exactly one run", r)
		}
	})
	t.Run("Close waits for an enqueue past its gate", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		repo, _ := f.addRepo(t, "h3", "file://"+f.wf.upstreamDir)
		reached, releaseOnce := f.gate.hold("RequestSync", repo)
		var once stdsync.Once
		release := func() { once.Do(releaseOnce) }
		defer release()
		errc := make(chan error, 1)
		go func() { errc <- f.sched.EnqueueSync(context.Background(), Job{Org: f.org, RepoID: repo}) }()
		<-reached
		closed := make(chan struct{})
		go func() { f.sched.Close(); close(closed) }()
		select {
		case <-closed:
			t.Fatal("Close returned while an enqueue was still writing to the store")
		case <-time.After(100 * time.Millisecond):
		}
		release()
		<-closed
		if err := <-errc; err == nil {
			t.Fatal("an enqueue that lost the race with Close reported success")
		}
	})
}

// TestLocalScheduleMirrorBackAfterPushLifecycle: the success write marks
// the mirror-back pull, but the sweep does not start it until the push run
// has emitted its terminal event, as on the non-local path (AfterForward
// runs after the emit). Review of #341.
func TestLocalScheduleMirrorBackAfterPushLifecycle(t *testing.T) {
	f := newLocalSchedFixture(t)
	up := newHTTPUpstream(t)
	up.addRepo(t, "own", "h4")
	repo, _ := f.addRepo(t, "h4", up.host())
	if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
		t.Fatal(err)
	}
	reached, release := f.gate.hold("FinishPushOutcome:after", repo)
	var once stdsync.Once
	defer once.Do(release)
	if err := f.sched.SweepOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-reached // the success write is done; the push has not emitted yet
	if r := f.row(t, repo); !r.SyncPending {
		t.Fatalf("success write did not mark the mirror-back: %+v", r)
	}
	if err := f.sched.SweepOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // a wrongly offered pull would start here
	once.Do(release)
	f.idle(t)
	f.sweep(t)
	pushDone, pullStarted := -1, -1
	for i, e := range f.wf.rec.list() {
		switch {
		case e.Direction == DirectionPush && e.Type == EventPushUpstreamSucceeded:
			pushDone = i
		case e.Direction == "" && e.Type == EventSyncStarted && pullStarted < 0:
			pullStarted = i
		}
	}
	if pushDone < 0 || pullStarted < 0 || pullStarted < pushDone {
		t.Fatalf("push succeeded at %d, mirror-back pull started at %d: want the push lifecycle first (%+v)",
			pushDone, pullStarted, f.wf.rec.list())
	}
}

// TestLocalSchedulePublicationFence (C6 unit half, C5 failed wait): a local
// run waits for the claimed repo's cache publication after its claim and
// before its snapshot and started event. A failed wait restores the
// consumed request (force and attempt kept, due after a short delay),
// emits nothing, and counts no run.
func TestLocalSchedulePublicationFence(t *testing.T) {
	t.Run("push waits after the claim, before the started event", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		up := newHTTPUpstream(t)
		bare := up.addRepo(t, "own", "p1")
		repo, head := f.addRepo(t, "p1", up.host())
		entered, release := make(chan struct{}), make(chan struct{})
		f.wf.worker.WaitPublication = func(ctx context.Context, repoID string) error {
			if repoID == repo {
				close(entered)
				<-release
			}
			return nil
		}
		if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
			t.Fatal(err)
		}
		if err := f.sched.SweepOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		<-entered
		if r := f.row(t, repo); !r.PushSyncing || len(f.pushEvents(repo)) != 0 {
			t.Fatalf("while waiting: row %+v, %d events; want a live claim and no events", r, len(f.pushEvents(repo)))
		}
		close(release)
		f.idle(t)
		assertPushTypes(t, f.pushEvents(repo), EventPushUpstreamStarted, EventPushUpstreamSucceeded)
		if got := upstreamHead(t, bare); got != head {
			t.Fatalf("upstream main = %q, want %q", got, head)
		}
	})
	t.Run("failed push wait restores force and attempt", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		up := newHTTPUpstream(t)
		bare := up.addRepo(t, "own", "p2")
		repo, _ := f.addRepo(t, "p2", up.host())
		before := upstreamHead(t, bare)
		up.down(t)
		if err := f.ms.RequestPush(t.Context(), repo, true); err != nil {
			t.Fatal(err)
		}
		f.sweep(t) // transient failure: attempt 1, force kept
		f.advance(time.Minute)
		f.wf.worker.WaitPublication = func(context.Context, string) error {
			return errors.New("publication unverified")
		}
		f.sweep(t) // the retry claims, then its wait fails
		r := f.row(t, repo)
		attempt, due := f.readSchedule(t, repo)
		if r.PushSyncing || r.PushPending != 2 || r.PushRunCount != 1 || attempt != 1 ||
			due != f.now().Add(localPublicationRetryDelay).UnixNano() || len(f.pushEvents(repo)) != 2 {
			t.Fatalf("after a failed wait: row %+v attempt %d due %d events %d; want force kept, attempt 1, due +%v, no new run",
				r, attempt, due, len(f.pushEvents(repo)), localPublicationRetryDelay)
		}
		if got := upstreamHead(t, bare); got != before {
			t.Fatalf("upstream changed after a failed wait: %q", got)
		}
	})
	t.Run("stale held snapshot restores force and attempt", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		up := newHTTPUpstream(t)
		bare := up.addRepo(t, "own", "p4")
		repo, head := f.addRepo(t, "p4", up.host())
		before := upstreamHead(t, bare)
		up.down(t)
		if err := f.ms.RequestPush(t.Context(), repo, true); err != nil {
			t.Fatal(err)
		}
		f.sweep(t) // transient failure: attempt 1, force kept
		f.advance(time.Minute)
		// The server refuses the loopback fetch: a push holds a ref the
		// cache has not caught up with (githttp.Server.StrictSyncFetch).
		locked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "held", http.StatusLocked)
		}))
		t.Cleanup(locked.Close)
		loopback := f.wf.worker.Loopback.BaseURL
		f.wf.worker.Loopback.BaseURL = locked.URL
		up.up(t)
		f.sweep(t) // the retry claims, then its snapshot is refused
		r := f.row(t, repo)
		attempt, due := f.readSchedule(t, repo)
		if r.PushSyncing || r.PushPending != 2 || r.PushRunCount != 1 || attempt != 1 ||
			due != f.now().Add(localPublicationRetryDelay).UnixNano() {
			t.Fatalf("after a refused snapshot: row %+v attempt %d due %d; want force kept, attempt 1, due +%v, no new run",
				r, attempt, due, localPublicationRetryDelay)
		}
		assertPushTypes(t, f.pushEvents(repo), EventPushUpstreamStarted, EventPushUpstreamFailed, EventPushUpstreamStarted)
		if got := upstreamHead(t, bare); got != before {
			t.Fatalf("upstream changed after a refused snapshot: %q", got)
		}
		f.wf.worker.Loopback.BaseURL = loopback
		f.advance(localPublicationRetryDelay)
		f.sweep(t)
		if r := f.row(t, repo); r.PushPending != 0 || r.PushSuccessCount != 1 || upstreamHead(t, bare) != head {
			t.Fatalf("restored run did not forward: row %+v upstream %q want %q", r, upstreamHead(t, bare), head)
		}
	})
	t.Run("failed pull wait restores the request", func(t *testing.T) {
		f := newLocalSchedFixture(t)
		seedUpstream(t, f.wf.upstreamDir, "own", "p3")
		repo, _ := f.addRepo(t, "p3", "file://"+f.wf.upstreamDir)
		f.wf.worker.WaitPublication = func(context.Context, string) error {
			return errors.New("publication unverified")
		}
		if err := f.ms.RequestSync(t.Context(), repo); err != nil {
			t.Fatal(err)
		}
		f.sweep(t)
		if r := f.row(t, repo); r.Syncing || !r.SyncPending || r.RunCount != 0 || len(f.wf.rec.list()) != 0 {
			t.Fatalf("after a failed wait: row %+v events %d; want the request kept and no run", r, len(f.wf.rec.list()))
		}
		f.wf.worker.WaitPublication = func(context.Context, string) error { return nil }
		f.sweep(t) // not due yet
		if r := f.row(t, repo); r.RunCount != 0 {
			t.Fatalf("restored pull ran before its delay: %+v", r)
		}
		f.advance(localPublicationRetryDelay)
		f.sweep(t)
		if r := f.row(t, repo); r.SuccessCount != 1 || r.SyncPending {
			t.Fatalf("restored pull did not run after its delay: %+v", r)
		}
	})
}
