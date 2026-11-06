//go:build linux

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

// TestStopSignalsOnlyGroupsWithAnUnreapedLeader (issue #473 item 1): the
// final SIGKILL must never reach a recycled PID, which can happen only for
// a group whose leader was reaped and whose ID the kernel handed out
// again. A leader that exits during the grace therefore stays a zombie,
// holding its PID, until stop has sent SIGKILL, and that SIGKILL still
// ends the member the leader left behind.
func TestStopSignalsOnlyGroupsWithAnUnreapedLeader(t *testing.T) {
	var rec groupSignals
	g := registry{signalGroup: rec.send}
	// The leader dies on SIGTERM; its background child ignores it (the
	// ignored SIGTERM survives the exec).
	cmd := exec.Command("sh", "-c", "(trap '' TERM; exec sleep 60) & echo $!; exec sleep 60")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.start(cmd); err != nil {
		t.Fatal(err)
	}
	leader := cmd.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(-leader, syscall.SIGKILL) })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- g.wait(cmd) }()

	grace := 30 * time.Second
	began := time.Now()
	if n := g.stop(grace); n != 1 {
		t.Fatalf("stop signaled %d groups, want 1", n)
	}
	if elapsed := time.Since(began); elapsed >= grace/2 {
		t.Fatalf("stop took %v, want it to end as soon as the leader exited, not at the %v grace", elapsed, grace)
	}

	var exitErr *exec.ExitError
	if err := <-done; !errors.As(err, &exitErr) || exitErr.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
		t.Fatalf("leader wait = %v, want death by SIGTERM", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.sent) != 2 || rec.sent[0].sig != syscall.SIGTERM || rec.sent[1].sig != syscall.SIGKILL {
		t.Fatalf("signals = %+v, want SIGTERM then SIGKILL to group %d", rec.sent, leader)
	}
	if !rec.sent[1].leaderExists {
		t.Fatalf("the leader was reaped before the final SIGKILL, want it exited and not reaped")
	}
	for deadline := time.Now().Add(5 * time.Second); !errors.Is(syscall.Kill(orphan, 0), syscall.ESRCH); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("member %d of the stopped group is still alive", orphan)
		}
	}
}

// TestWaitDropsTheGroupBeforeItReapsTheLeader pins the order in wait: the
// group leaves the registry first, so a stop that holds the lock never finds
// a registered group whose leader is reaped. hold reports an exit while the
// leader still runs, which makes the reap block on it. If wait reaped
// first, the group would stay registered until the leader dies.
func TestWaitDropsTheGroupBeforeItReapsTheLeader(t *testing.T) {
	exited := make(chan struct{})
	g := registry{hold: func(int) bool { <-exited; return true }}
	cmd := exec.Command("sleep", "60")
	if err := g.start(cmd); err != nil {
		t.Fatal(err)
	}
	leader := cmd.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(-leader, syscall.SIGKILL) })
	done := make(chan error, 1)
	go func() { done <- g.wait(cmd) }()
	close(exited)

	registered := func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		_, ok := g.groups[leader]
		return ok
	}
	for deadline := time.Now().Add(10 * time.Second); registered(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("group %d is still registered while wait blocks on its running leader: wait must drop the group before it reaps", leader)
		}
	}
	_ = syscall.Kill(leader, syscall.SIGKILL)
	var exitErr *exec.ExitError
	if err := <-done; !errors.As(err, &exitErr) || exitErr.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("leader wait = %v, want death by SIGKILL", err)
	}
}
