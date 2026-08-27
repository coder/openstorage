// Graceful-shutdown drill (issue #174): a termination signal must make
// serve drain and exit cleanly — running its deferred teardown — instead
// of dying mid-http.Serve with every cleanup skipped. Lane-agnostic: the
// signal plumbing is identical for pg, local, and s3wal backends (the
// s3wal lease-release consequence has its own drill in s3wal_e2e_test.go).
package githttp

import (
	"syscall"
	"testing"
	"time"
)

// waitNodeExit waits for a node the test already signaled to exit on its
// own, returning cmd.Wait's verdict (nil = exit code 0). A node still
// running after the deadline is killed and fails the test.
func waitNodeExit(t *testing.T, n *testNode, timeout time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- n.cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		_ = n.cmd.Process.Kill()
		t.Fatalf("serve still running %v after the termination signal", timeout)
		return nil
	}
}

func TestServeSigtermExitsGracefully(t *testing.T) {
	node := startNodeT(t)
	if err := node.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal: %v", err)
	}
	// Prompt AND clean: the drain is bounded (no in-flight requests here,
	// so it returns immediately) and the signal path exits 0 — a non-nil
	// Wait error is the pre-fix behavior (killed by the signal itself).
	if err := waitNodeExit(t, node, 30*time.Second); err != nil {
		t.Fatalf("serve did not exit cleanly on SIGTERM: %v", err)
	}
}
