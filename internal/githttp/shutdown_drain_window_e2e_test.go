package githttp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestServeDrainWindowIgnoresRunDrain (issue #473 item 2): a request in
// flight when SIGTERM arrives keeps its whole HTTP drain window, whether or
// not a scheduled sync run holds the run drain for its full 5+2 seconds.
// The window ends at a fixed deadline (8 of the 10 seconds), so the run
// drain, which serves requests while it waits, takes nothing from it. The
// request below finishes right after the listener closes, which is the
// moment the HTTP drain starts. The drain must complete without `drain
// incomplete` and without stopping any git process.
func TestServeDrainWindowIgnoresRunDrain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		heldRun    bool
		wantListen time.Duration // the listener closes no sooner than this after SIGTERM
		afterClose time.Duration // the request finishes this long after the listener closes
	}{
		// No run drain: the HTTP drain starts at once and its window
		// lasts until 8 seconds, far past afterClose.
		{name: "idle", afterClose: 4 * time.Second},
		// The run drain takes 5 seconds and the run another 2 to restore:
		// the HTTP drain starts at 7 seconds, with 1 second left.
		{name: "run drain holds its whole wait", heldRun: true, wantListen: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			dbURL, blobURL := privateStores(t, dataDir)
			env := []string{"OPENSTORAGE_PG_URL=" + dbURL, "OPENSTORAGE_S3_URL=" + blobURL}
			up, stall := newStallingSyncUpstream(t)
			up.commitAndPublish(t, "one")
			var stderr bytes.Buffer
			node, err := startBinaryNode(binPath, t.TempDir(), io.MultiWriter(os.Stderr, &stderr), env...)
			if err != nil {
				t.Fatalf("start node: %v", err)
			}
			t.Cleanup(node.stop)
			repo := createSyncedRepo(t, node, up, "drain-window")
			token := mintTestToken(t, repo, "git:write", "")
			if tc.heldRun {
				up.commitAndPublish(t, "two")
				stall.hold.Store(true)
				if status := apiJSON(t, node, http.MethodPost, token, "/api/v1/repos/pull-upstream", "{}", nil); status != http.StatusAccepted {
					t.Fatalf("pull-upstream: status %d, want 202", status)
				}
				select {
				case <-stall.hit:
				case <-time.After(60 * time.Second):
					t.Fatal("the pull run never reached the upstream")
				}
			}

			// The handler blocks reading the body, so the request stays in
			// flight until the test sends its last byte.
			conn, err := net.Dial("tcp", node.addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			fmt.Fprintf(conn, "POST /api/v1/repos/pull-upstream HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{", node.addr, token)
			time.Sleep(100 * time.Millisecond) // the handler is reading by now

			sigterm := time.Now()
			if err := node.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			closedAfter := waitListenerClosed(t, node.addr, sigterm)
			if closedAfter < tc.wantListen {
				t.Fatalf("listener closed %v after SIGTERM, want at least %v (the run drain ended early)", closedAfter, tc.wantListen)
			}
			time.Sleep(tc.afterClose)
			fmt.Fprint(conn, "}")
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil || !strings.Contains(line, " 202 ") {
				t.Fatalf("request in flight at the HTTP drain: status line %q, err %v, want 202", line, err)
			}
			stall.hold.Store(false)
			close(stall.release)
			if err := waitNodeExit(t, node, 30*time.Second); err != nil {
				t.Fatalf("serve did not exit cleanly on SIGTERM: %v", err)
			}
			for _, bad := range []string{"drain incomplete", "stopped "} {
				if strings.Contains(stderr.String(), bad) {
					t.Errorf("serve stderr holds %q although the only request finished inside the window", bad)
				}
			}
		})
	}
}

// waitListenerClosed returns how long after since the listener at addr
// stopped accepting connections.
func waitListenerClosed(t *testing.T, addr string, since time.Time) time.Duration {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			return time.Since(since)
		}
		_ = c.Close()
		if time.Now().After(deadline) {
			t.Fatal("listener still accepting 30s after SIGTERM")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
