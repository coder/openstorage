package githttp

// Local-lane acceptance for issue #342: SIGTERM while a local-schedule
// pull is in flight must not use up the pull's request. Before the fix,
// serve stopped its listener first; the run then finished its upstream
// fetch, failed its loopback push to this process's own listener, and the
// failed run consumed the request. Now serve drains the scheduler before
// the listener stops, and a run cancelled by that drain keeps its request
// (OutcomeRestore, ADR-50), so the next boot completes it.

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref"
)

// stallHandler holds every request while hold is set, until release closes
// (then serves it) or the client goes away.
type stallHandler struct {
	next    http.Handler
	hold    atomic.Bool
	hit     chan struct{} // receives once a request is held
	release chan struct{}
}

func (h *stallHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.hold.Load() {
		select {
		case h.hit <- struct{}{}:
		default:
		}
		select {
		case <-h.release:
		case <-r.Context().Done():
			return
		}
	}
	h.next.ServeHTTP(w, r)
}

// newStallingSyncUpstream is newSyncUpstream behind a stallHandler.
func newStallingSyncUpstream(t *testing.T) (*syncUpstream, *stallHandler) {
	t.Helper()
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatal(err)
	}
	mustGit(t, "", "init", "--bare", bare)
	work := filepath.Join(root, "work")
	mustGit(t, "", "init", "-b", "main", work)
	u := &syncUpstream{bare: bare, work: work}
	u.commitAndPublish(t, "seed")
	stall := &stallHandler{next: http.FileServer(http.Dir(root)), hit: make(chan struct{}, 1), release: make(chan struct{})}
	u.srv = httptest.NewServer(stall)
	t.Cleanup(u.srv.Close)
	return u, stall
}

// waitForRepoMain polls st until repoID's refs/heads/main is want.
func waitForRepoMain(t *testing.T, st ref.RefStore, repoID, want string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		refs, err := st.ListRefs(t.Context(), repoID)
		if err == nil && refs["refs/heads/main"] == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("repo %s: refs/heads/main never reached %s (refs %v, err %v)", repoID, want, refs, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func TestSync_ShutdownKeepsInFlightPullRequest(t *testing.T) {
	if testBackend != backendLocal {
		t.Skip("the durable local schedule is SQLite-only; PostgreSQL and S3-WAL shutdown is issue #343")
	}
	// A private database and blob store: the lane's shared nodes run their
	// own schedule sweeps and would otherwise pick up (or steal) the run.
	dataDir := t.TempDir()
	dbURL := "sqlite://" + filepath.Join(dataDir, "codestorage.db")
	env := []string{"OPENSTORAGE_PG_URL=" + dbURL, "OPENSTORAGE_S3_URL=file://" + filepath.Join(dataDir, "blobs")}
	cacheDir := t.TempDir()

	up, stall := newStallingSyncUpstream(t)
	head := up.commitAndPublish(t, "one")
	node1, err := startNode(cacheDir, env...)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node1.stop)
	st, err := ref.Open(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	repo := createSyncedRepo(t, node1, up, "shutdown-keeps-pull")
	waitForRepoMain(t, st, repo, head)

	// A pull-upstream whose run is held mid-fetch by the upstream.
	head = up.commitAndPublish(t, "two")
	stall.hold.Store(true)
	pullToken := mintTestToken(t, repo, "git:write", "")
	if status := apiJSON(t, node1, http.MethodPost, pullToken, "/api/v1/repos/pull-upstream", "{}", nil); status != http.StatusAccepted {
		t.Fatalf("pull-upstream: status %d, want 202", status)
	}
	select {
	case <-stall.hit:
	case <-time.After(60 * time.Second):
		t.Fatal("the pull run never reached the upstream")
	}

	if err := node1.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal: %v", err)
	}
	// Let the held fetch go on only once the listener is gone, so a run
	// still alive at that point pushes to a stopped listener (the #342
	// failure). A run the drain cancelled has already restored.
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", node1.addr, time.Second)
		if err != nil {
			break
		}
		_ = c.Close()
		if time.Now().After(deadline) {
			t.Fatal("listener still accepting 30s after SIGTERM")
		}
		time.Sleep(50 * time.Millisecond)
	}
	stall.hold.Store(false)
	close(stall.release)
	if err := waitNodeExit(t, node1, 60*time.Second); err != nil {
		t.Fatalf("serve did not exit cleanly on SIGTERM: %v", err)
	}

	// The next boot's sweep completes the kept request.
	node2, err := startNode(cacheDir, env...)
	if err != nil {
		t.Fatalf("restart node: %v", err)
	}
	t.Cleanup(node2.stop)
	waitForRepoMain(t, st, repo, head)
}
