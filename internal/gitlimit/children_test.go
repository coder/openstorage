package gitlimit

import (
	"bufio"
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestGitChildrenStopKillsAfterGrace: a group that ignores SIGTERM gets
// SIGKILL once the grace ends, grandchildren included, and stop refuses
// later children. It runs the platform's wait and the wait of a platform
// that cannot hold a leader. (The SIGTERM path runs in the e2e
// TestServeDrainTimeoutStopsGitChildren.)
func TestGitChildrenStopKillsAfterGrace(t *testing.T) {
	t.Run("platform", func(t *testing.T) { stopKillsAfterGrace(t, &registry{}) })
	t.Run("leader cannot be held", func(t *testing.T) { stopKillsAfterGrace(t, &registry{hold: cannotHold}) })
}

func stopKillsAfterGrace(t *testing.T, g *registry) {
	// The ignored SIGTERM is inherited by sleep across exec.
	cmd := exec.Command("sh", "-c", "trap '' TERM; sleep 60 & echo $!; wait")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.start(cmd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- g.wait(cmd) }()

	start := time.Now()
	if n := g.stop(200 * time.Millisecond); n != 1 {
		t.Fatalf("stop signaled %d groups, want 1", n)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("stop returned after %v, before the grace ended", elapsed)
	}
	var exitErr *exec.ExitError
	if err := <-done; !errors.As(err, &exitErr) || exitErr.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child wait = %v, want death by SIGKILL", err)
	}
	for deadline := time.Now().Add(5 * time.Second); !errors.Is(syscall.Kill(grandchild, 0), syscall.ESRCH); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("grandchild %d still exists", grandchild)
		}
	}
	if err := g.start(exec.Command("true")); !errors.Is(err, ErrStopping) {
		t.Fatalf("start after stop = %v, want ErrStopping", err)
	}
}
