package sync

// Outbound push-upstream run tests (issue #90), mirroring worker_test.go's
// hermetic file:// fixture: the loopback dir holds the LOCAL repo the run
// fetches from, and the upstream dir holds the destination bare repo.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// seedLocalRepo populates the fixture's loopback repo with one commit on
// main and one tag, returning the head SHA — the local state a push run
// publishes.
func seedLocalRepo(t *testing.T, f *workerFixture, repoID string) string {
	t.Helper()
	bare := f.addLoopbackRepo(t, repoID)
	work := t.TempDir()
	testGit(t, work, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(work, "local.txt"), []byte("local\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testGit(t, work, "add", ".")
	testGit(t, work, "commit", "-m", "local c1")
	testGit(t, work, "tag", "v1")
	testGit(t, work, "push", "file://"+bare, "main", "v1")
	return testGit(t, work, "rev-parse", "HEAD")
}

// addEmptyUpstream creates the destination bare repo without content.
func addEmptyUpstream(t *testing.T, root, owner, name string) string {
	t.Helper()
	bare := filepath.Join(root, owner, name+".git")
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatal(err)
	}
	testGit(t, root, "init", "--bare", "-b", "main", bare)
	return bare
}

func TestPushRunCleanPushSucceeds(t *testing.T) {
	f := newWorkerFixture(t)
	headSHA := seedLocalRepo(t, f, "repo_pa")
	bare := addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	f.putUpstream(t, "repo_pa", "own", "proj", "file://"+f.upstreamDir)
	if err := f.store.RequestPush(context.Background(), "repo_pa", false); err != nil {
		t.Fatal(err)
	}

	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pa"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}

	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncSucceeded)
	started, done := events[0], events[1]
	if started.Direction != DirectionPush || started.Forced || started.RunCount != 0 || !started.IsFirstSync {
		t.Fatalf("bad started event: %+v", started)
	}
	if done.Direction != DirectionPush || done.RunCount != 1 || done.Error != "" || done.CompletedAt == "" {
		t.Fatalf("bad succeeded event: %+v", done)
	}

	if got := testGit(t, f.upstreamDir, "-C", bare, "rev-parse", "refs/heads/main"); got != headSHA {
		t.Fatalf("upstream main = %s, want %s", got, headSHA)
	}
	if got := testGit(t, f.upstreamDir, "-C", bare, "rev-parse", "refs/tags/v1"); got != headSHA {
		t.Fatalf("upstream tag = %s, want %s", got, headSHA)
	}

	up, err := f.store.GetUpstream(context.Background(), "repo_pa")
	if err != nil {
		t.Fatal(err)
	}
	if up.PushRunCount != 1 || up.PushSuccessCount != 1 || up.PushSyncing || up.PushPending != PushPendingNone {
		t.Fatalf("bad upstream state after run: %+v", up)
	}
	if up.RunCount != 0 || up.SuccessCount != 0 {
		t.Fatalf("push run leaked into pull counters: %+v", up)
	}
}

// divergeUpstream gives the destination bare repo a main history unrelated
// to the local one, so a non-force push is a non-fast-forward.
func divergeUpstream(t *testing.T, bare string) string {
	t.Helper()
	work := t.TempDir()
	testGit(t, work, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(work, "upstream.txt"), []byte("theirs\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testGit(t, work, "add", ".")
	testGit(t, work, "commit", "-m", "upstream c1")
	testGit(t, work, "push", "--force", "file://"+bare, "main")
	return testGit(t, work, "rev-parse", "HEAD")
}

// TestPushRunNonFastForwardRejectsAtomically: the local snapshot carries a
// stale main AND a brand-new tag; the refused main must fail the WHOLE
// forward (--atomic) — the innocent tag reports "atomic push failed" in
// rejected_refs and never lands upstream, because a partially-updated
// upstream behind a failed run would have no mirror-back scheduled to
// converge it.
func TestPushRunNonFastForwardRejectsAtomically(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pb")
	bare := addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	upstreamHead := divergeUpstream(t, bare)
	f.putUpstream(t, "repo_pb", "own", "proj", "file://"+f.upstreamDir)

	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pb"}); err == nil {
		t.Fatal("RunPush succeeded pushing a non-fast-forward without force")
	}

	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	failed := events[1]
	if failed.Error != FailRejected || failed.Direction != DirectionPush {
		t.Fatalf("bad failed event: %+v", failed)
	}
	reasons := map[string]string{}
	for _, r := range failed.RejectedRefs {
		if r.Reason == "" {
			t.Fatalf("rejected ref carries no reason: %+v", failed.RejectedRefs)
		}
		reasons[r.Ref] = r.Reason
	}
	if len(reasons) != 2 || reasons["refs/heads/main"] == "" || reasons["refs/tags/v1"] == "" {
		t.Fatalf("rejected_refs = %+v, want refs/heads/main + refs/tags/v1", failed.RejectedRefs)
	}
	// Upstream history was NOT rewritten AND the innocent tag did not
	// land: the batch is one transaction.
	if got := testGit(t, f.upstreamDir, "-C", bare, "rev-parse", "refs/heads/main"); got != upstreamHead {
		t.Fatalf("upstream main = %s, want untouched %s", got, upstreamHead)
	}
	if refs := testGit(t, f.upstreamDir, "-C", bare, "for-each-ref", "refs/tags"); refs != "" {
		t.Fatalf("atomic rejection still landed tags upstream: %q", refs)
	}
}

func TestPushRunForceOverridesNonFastForward(t *testing.T) {
	f := newWorkerFixture(t)
	localHead := seedLocalRepo(t, f, "repo_pc")
	bare := addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	divergeUpstream(t, bare)
	f.putUpstream(t, "repo_pc", "own", "proj", "file://"+f.upstreamDir)
	if err := f.store.RequestPush(context.Background(), "repo_pc", true); err != nil {
		t.Fatal(err)
	}

	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pc"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}

	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncSucceeded)
	if !events[0].Forced || !events[1].Forced {
		t.Fatalf("force flag missing from events: %+v", events)
	}
	if got := testGit(t, f.upstreamDir, "-C", bare, "rev-parse", "refs/heads/main"); got != localHead {
		t.Fatalf("upstream main = %s, want force-updated %s", got, localHead)
	}
}

func TestPushRunEmptyLocalRepoSucceeds(t *testing.T) {
	f := newWorkerFixture(t)
	f.addLoopbackRepo(t, "repo_pd") // no commits: nothing to publish
	addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	f.putUpstream(t, "repo_pd", "own", "proj", "file://"+f.upstreamDir)

	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pd"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	assertEventTypes(t, f.rec.list(), EventSyncStarted, EventSyncSucceeded)
}

// TestPushRunAuthFailure: an upstream demanding credentials nobody stored
// classifies as the documented auth failure (and is NOT retried: auth is
// permanent until a human acts).
func TestPushRunAuthFailure(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pe")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
		http.Error(w, "auth required", http.StatusUnauthorized)
	}))
	defer srv.Close()
	f.putUpstream(t, "repo_pe", "own", "proj", srv.URL)

	err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pe"})
	if err == nil {
		t.Fatal("RunPush succeeded against a 401 upstream")
	}
	if TransientPushFailure(err) {
		t.Fatalf("auth failure classified transient: %v", err)
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailAuth {
		t.Fatalf("error = %q, want %q", events[1].Error, FailAuth)
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_pe")
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending != PushPendingNone {
		t.Fatalf("permanent failure re-marked pending: %+v", up)
	}
}

// TestPushRunUnreachableRemarksPending: an unreachable upstream is the
// transient class — the run re-marks the durable pending level so the
// backoff retry (or a restart's boot recovery) can converge later.
func TestPushRunUnreachableRemarksPending(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pf")
	// A closed port: git fails with a connection-refused class error.
	f.putUpstream(t, "repo_pf", "own", "proj", "http://127.0.0.1:1")
	if err := f.store.RequestPush(context.Background(), "repo_pf", false); err != nil {
		t.Fatal(err)
	}

	err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pf"})
	if err == nil {
		t.Fatal("RunPush succeeded against an unreachable upstream")
	}
	if !TransientPushFailure(err) {
		t.Fatalf("unreachable failure not classified transient: %v", err)
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailUnreachable {
		t.Fatalf("error = %q, want %q", events[1].Error, FailUnreachable)
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_pf")
	if err != nil {
		t.Fatal(err)
	}
	// RequeuePush is nil (river-lane shape): the run re-marks the durable
	// level and leaves scheduling to the completion gate.
	if up.PushPending != PushPendingPush {
		t.Fatalf("transient failure did not re-mark pending: %+v", up)
	}
}

// TestPushRunRetryBudgetExhausted: past MaxPushTransientRetries the run
// stops re-marking — the chain ends and the failed event stands.
func TestPushRunRetryBudgetExhausted(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pg")
	f.putUpstream(t, "repo_pg", "own", "proj", "http://127.0.0.1:1")

	err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pg", Retry: MaxPushTransientRetries})
	if err == nil {
		t.Fatal("RunPush succeeded against an unreachable upstream")
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_pg")
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending != PushPendingNone {
		t.Fatalf("exhausted retry chain still re-marked pending: %+v", up)
	}
}

func TestPushRunDetachedBeforeRunIsSilent(t *testing.T) {
	f := newWorkerFixture(t)
	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_ph"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	assertEventTypes(t, f.rec.list()) // no events at all
}

// detachDuringPushBarrier deletes the row when the pre-push barrier reads
// it — DELETE /repos/base landing between the loopback fetch and the
// upstream push.
type detachDuringPushBarrier struct {
	*MemStore
}

func (d *detachDuringPushBarrier) GetUpstream(ctx context.Context, repoID string) (Upstream, error) {
	_ = d.MemStore.DeleteUpstream(ctx, repoID)
	return d.MemStore.GetUpstream(ctx, repoID)
}

func TestPushRunDetachMidRunEmitsNoTerminalEvent(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pi")
	bare := addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	f.putUpstream(t, "repo_pi", "own", "proj", "file://"+f.upstreamDir)
	f.worker.Upstreams = &detachDuringPushBarrier{MemStore: f.store}

	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pi"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	// started was emitted before the detach; no succeeded/failed after it,
	// and nothing reached the upstream.
	assertEventTypes(t, f.rec.list(), EventSyncStarted)
	if refs := testGit(t, f.upstreamDir, "-C", bare, "for-each-ref"); refs != "" {
		t.Fatalf("detached run still pushed refs upstream: %q", refs)
	}
}

// TestPushRunDetachDuringUpstreamPushKillsSubprocess: DELETE /repos/base
// committing while `git push` is already talking to a slow upstream must
// interrupt the subprocess (the mid-push watchdog): the detach response
// claims cancellation, so the upstream must not be able to accept refs
// afterward.
func TestPushRunDetachDuringUpstreamPushKillsSubprocess(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pw")
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done() // stall the transfer until the client dies
	}))
	defer srv.Close()
	defer srv.CloseClientConnections() // LIFO: unblock the stalled handler first
	f.putUpstream(t, "repo_pw", "own", "proj", srv.URL)
	f.worker.DetachPoll = 10 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		done <- f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pw"})
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("RunPush returned before the upstream transfer started: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("upstream transfer never started")
	}
	if err := f.store.DeleteUpstream(context.Background(), "repo_pw"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunPush after mid-push detach: %v", err)
		}
	case <-time.After(30 * time.Second): // well under the 1m run timeout: the watchdog, not the deadline, must kill it
		t.Fatal("detach did not interrupt the in-flight push (watchdog dead?)")
	}
	// started was emitted before the detach; the canceled run emits no
	// terminal event.
	assertEventTypes(t, f.rec.list(), EventSyncStarted)
}

// TestPushRunPrivateUpstreamDeniedByDefault: with the production egress
// policy (PushEgressAllowPrivate off), a loopback/private upstream_host is
// refused at dial time by the vetting proxy — the upstream is NEVER
// contacted — and the run fails permanently as configuration (no retry,
// no re-marked pending level): upstream_host is caller-linked data, so
// unguarded egress would be an SSRF/write primitive into the server's
// network.
func TestPushRunPrivateUpstreamDeniedByDefault(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pe2")
	var reached atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Store(true)
		http.Error(w, "unreachable", http.StatusTeapot)
	}))
	defer srv.Close()
	f.putUpstream(t, "repo_pe2", "own", "proj", srv.URL)
	f.worker.PushEgressAllowPrivate = false // production default

	err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pe2"})
	if err == nil {
		t.Fatal("RunPush succeeded against a policy-denied private upstream")
	}
	if TransientPushFailure(err) {
		t.Fatalf("egress denial classified transient: %v", err)
	}
	if reached.Load() {
		t.Fatal("denied egress still reached the private upstream")
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailConfig {
		t.Fatalf("error = %q, want %q", events[1].Error, FailConfig)
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_pe2")
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending != PushPendingNone {
		t.Fatalf("permanent egress denial re-marked pending: %+v", up)
	}
}

// TestEgressProxyConnectTunnel exercises the CONNECT arm (https upstreams)
// hermetically: a vetted tunnel splices bytes end-to-end, and a denied
// target is refused before any dial.
func TestEgressProxyConnectTunnel(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("tunneled"))
	}))
	defer backend.Close()

	p, err := startEgressProxy(true) // loopback backend needs the hatch
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	connect := func(target string) (net.Conn, string, error) {
		conn, err := net.Dial("tcp", p.ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
		br := bufio.NewReader(conn)
		resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
		if err != nil {
			conn.Close()
			return nil, "", err
		}
		defer resp.Body.Close()
		return conn, resp.Status, nil
	}

	backendAddr := strings.TrimPrefix(backend.URL, "http://")
	conn, status, err := connect(backendAddr)
	if err != nil {
		t.Fatalf("CONNECT: %v", err)
	}
	defer conn.Close()
	if !strings.HasPrefix(status, "200") {
		t.Fatalf("CONNECT status = %q, want 200", status)
	}
	// Speak HTTP through the established tunnel.
	fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", backendAddr)
	tunneled, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read through tunnel: %v", err)
	}
	if !strings.Contains(string(tunneled), "tunneled") {
		t.Fatalf("tunneled response = %q, want backend body", tunneled)
	}

	// Metadata addresses stay denied even with the private hatch on.
	if _, status, err := connect("169.254.169.254:443"); err == nil && !strings.HasPrefix(status, "403") {
		t.Fatalf("metadata CONNECT status = %q, want 403", status)
	}
	if p.deniedReason() == "" {
		t.Fatal("denied CONNECT left no recorded denial")
	}
}

func TestPushRunOverlappingRunSkips(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pj")
	addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	f.putUpstream(t, "repo_pj", "own", "proj", "file://"+f.upstreamDir)
	if _, _, _, err := f.store.BeginPush(context.Background(), "repo_pj", "other-holder", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pj"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	assertEventTypes(t, f.rec.list()) // skipped: no events
}

// TestPushRunIndependentOfPullClaim: an in-flight PULL claim must not block
// the outbound lane — the claims protect different resources (local refs
// vs upstream refs) and serializing them would let a stuck pull starve
// forwarding.
func TestPushRunIndependentOfPullClaim(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pk")
	addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	f.putUpstream(t, "repo_pk", "own", "proj", "file://"+f.upstreamDir)
	if _, _, err := f.store.BeginSync(context.Background(), "repo_pk", "pull-holder", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pk"}); err != nil {
		t.Fatalf("RunPush under a pull claim: %v", err)
	}
	assertEventTypes(t, f.rec.list(), EventSyncStarted, EventSyncSucceeded)
}

// TestPushRunAfterForwardSchedulesMirrorBack: EVERY successful outbound
// run enqueues exactly one inbound sync — ForwardOnPush repos for the
// documented mirror-back, explicit push-upstream repos because a
// concurrent pull that fetched pre-forward would otherwise force-write
// its stale snapshot locally with nothing scheduled to converge it — and
// the request is marked durable (SyncPending) BEFORE the enqueue, so an
// enqueue that coalesces with an in-flight pull run survives to that
// run's completion gate.
func TestPushRunAfterForwardSchedulesMirrorBack(t *testing.T) {
	for _, tc := range []struct {
		name    string
		repoID  string
		forward bool
	}{
		{"forward_on_push", "repo_pl", true},
		{"explicit_push_upstream", "repo_pl2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWorkerFixture(t)
			seedLocalRepo(t, f, tc.repoID)
			addEmptyUpstream(t, f.upstreamDir, "own", "proj")
			if _, err := f.store.PutUpstream(context.Background(), Upstream{
				RepoID: tc.repoID, Provider: "gitea", Owner: "own", Name: "proj",
				UpstreamHost: "file://" + f.upstreamDir, ForwardOnPush: tc.forward,
			}); err != nil {
				t.Fatal(err)
			}
			var mirrored []Job
			var markedAtEnqueue []bool
			f.worker.AfterForward = func(ctx context.Context, job Job) error {
				up, err := f.store.GetUpstream(ctx, job.RepoID)
				if err != nil {
					t.Errorf("GetUpstream inside AfterForward: %v", err)
				}
				markedAtEnqueue = append(markedAtEnqueue, up.SyncPending)
				mirrored = append(mirrored, job)
				return nil
			}

			if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: tc.repoID}); err != nil {
				t.Fatalf("RunPush: %v", err)
			}
			if len(mirrored) != 1 || mirrored[0].RepoID != tc.repoID {
				t.Fatalf("mirror-back jobs = %+v, want exactly one for %s", mirrored, tc.repoID)
			}
			if !markedAtEnqueue[0] {
				t.Fatal("mirror-back enqueued without the durable SyncPending marker (RequestSync-before-enqueue)")
			}
		})
	}
}

func TestPushRunPendingFollowUpRequeues(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pm")
	addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	f.putUpstream(t, "repo_pm", "own", "proj", "file://"+f.upstreamDir)

	// A request arriving after the claim: simulate by wrapping FinishPush
	// to mark pending right before completion.
	requeued := make(chan Job, 1)
	f.worker.RequeuePush = func(_ context.Context, job Job) error {
		requeued <- job
		return nil
	}
	f.worker.Upstreams = &markPendingOnFinish{MemStore: f.store}

	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pm"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	select {
	case job := <-requeued:
		if job.RepoID != "repo_pm" || job.Retry != 0 {
			t.Fatalf("bad requeued job: %+v", job)
		}
	default:
		t.Fatal("pending request was not requeued")
	}
}

// markPendingOnFinish marks a fresh push request just before FinishPush
// completes the run — the request-arrived-after-claim shape.
type markPendingOnFinish struct {
	*MemStore
}

func (m *markPendingOnFinish) FinishPush(ctx context.Context, repoID, holder string, success bool) (Upstream, error) {
	_ = m.MemStore.RequestPush(ctx, repoID, false)
	return m.MemStore.FinishPush(ctx, repoID, holder, success)
}

// TestMemStoreForceInheritance keeps the test double honest against the
// durable backends' steal semantics (meta eventing_pushlane_test.go): a
// stolen expired claim inherits its persisted force mandate.
func TestMemStoreForceInheritance(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	if _, err := m.PutUpstream(ctx, Upstream{RepoID: "r1", Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}); err != nil {
		t.Fatal(err)
	}
	if err := m.RequestPush(ctx, "r1", true); err != nil {
		t.Fatal(err)
	}
	if _, force, _, err := m.BeginPush(ctx, "r1", "h-crash", time.Nanosecond); err != nil || !force {
		t.Fatalf("forced claim = (%v, %v), want forced", force, err)
	}
	time.Sleep(2 * time.Millisecond) // let the 1ns lease expire
	up, force, stolenFrom, err := m.BeginPush(ctx, "r1", "h-rescue", time.Minute)
	if err != nil || stolenFrom != "h-crash" || !force || !up.PushClaimForce {
		t.Fatalf("steal = (%+v, %v, %q, %v), want inherited force", up, force, stolenFrom, err)
	}
	if fin, err := m.FinishPush(ctx, "r1", "h-rescue", true); err != nil || fin.PushClaimForce {
		t.Fatalf("finish = (%+v, %v), want cleared mandate", fin, err)
	}
}

func TestPushRetryDelayBackoff(t *testing.T) {
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute}
	for i, w := range want {
		if got := PushRetryDelay(i + 1); got != w {
			t.Fatalf("PushRetryDelay(%d) = %v, want %v", i+1, got, w)
		}
	}
}
