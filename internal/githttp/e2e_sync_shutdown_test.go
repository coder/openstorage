package githttp

// Acceptance for issues #342 (SQLite) and #343 (PostgreSQL/River):
// SIGTERM while a pull is in flight must not use up the pull's request.
// Before the fixes, serve stopped its listener first; the run then finished
// its upstream fetch, failed its loopback push to this process's own
// listener, and the failed run consumed the request. Now serve drains its
// sync runs before the listener stops, and a run cancelled by that drain
// keeps its request, so the next boot completes it.

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// privateDatabase returns a database URL no other serve process in the lane
// uses: the lane's shared nodes run their own sync workers (SQLite sweeps,
// River workers) and would otherwise pick up or steal the run. On
// PostgreSQL it is a fresh database on the lane's server.
func privateDatabase(t *testing.T, dataDir string) string {
	t.Helper()
	switch testBackend {
	case backendLocal:
		return "sqlite://" + filepath.Join(dataDir, "codestorage.db")
	case backendPG:
		conn, err := pgx.Connect(t.Context(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(t.Context()) }()
		name := fmt.Sprintf("shutdown_%d", time.Now().UnixNano())
		if _, err := conn.Exec(t.Context(), "CREATE DATABASE "+name); err != nil {
			t.Fatal(err)
		}
		// Registered first, so it runs after the nodes and stores close.
		t.Cleanup(func() {
			c, err := pgx.Connect(context.Background(), dbURL)
			if err != nil {
				t.Errorf("drop %s: %v", name, err)
				return
			}
			defer func() { _ = c.Close(context.Background()) }()
			if _, err := c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
				t.Errorf("drop %s: %v", name, err)
			}
		})
		u, err := url.Parse(dbURL)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + name
		return u.String()
	default:
		t.Skip("S3-WAL shutdown is issue #335")
		return ""
	}
}

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
	// A private database and blob store (see privateDatabase).
	dataDir := t.TempDir()
	dbURL := privateDatabase(t, dataDir)
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
	ms, err := meta.Open(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
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

	held, err := ms.GetRepoUpstream(t.Context(), repo)
	if err != nil || !held.Syncing {
		t.Fatalf("held run's row: %+v, %v", held, err)
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
	// The cancelled run is not counted (so it emitted no repo.sync.failed)
	// and left its request pending with the claim released.
	kept, err := ms.GetRepoUpstream(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if kept.RunCount != held.RunCount || !kept.SyncPending || kept.Syncing {
		t.Fatalf("after shutdown: row %+v, want run_count %d, the request pending, no claim", kept, held.RunCount)
	}

	// The next boot's sweep completes the kept request.
	node2, err := startNode(cacheDir, env...)
	if err != nil {
		t.Fatalf("restart node: %v", err)
	}
	t.Cleanup(node2.stop)
	waitForRepoMain(t, st, repo, head)

	// The push hooks commit the ref store before receive-pack finishes in
	// cacheDir: its local ref update, reference-transaction and
	// post-receive hooks still run (issue #431). node2.stop's SIGKILL does
	// not reach those git processes, so they kept writing while TempDir
	// cleanup removed cacheDir. Wait for the run's terminal row, then stop
	// node2 with SIGTERM: serve's drain waits for its in-flight receive-pack
	// requests before it exits.
	deadline = time.Now().Add(60 * time.Second)
	for {
		row, err := ms.GetRepoUpstream(t.Context(), repo)
		if err == nil && !row.Syncing && !row.SyncPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sync run never settled: row %+v, err %v", row, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := node2.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal node2: %v", err)
	}
	if err := waitNodeExit(t, node2, 60*time.Second); err != nil {
		t.Fatalf("node2 did not exit cleanly on SIGTERM: %v", err)
	}
}
