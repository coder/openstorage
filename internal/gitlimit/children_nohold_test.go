//go:build unix

package gitlimit

import (
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"
)

// groupSignals records every signal the registry sends to a group, and
// whether the group's leader still existed (running or a zombie) at that
// moment: a reaped leader gives its PID back to the kernel.
type groupSignals struct {
	mu   sync.Mutex
	sent []sentSignal
}

type sentSignal struct {
	pgid         int
	sig          syscall.Signal
	leaderExists bool
}

func (r *groupSignals) send(pgid int, sig syscall.Signal) error {
	exists := syscall.Kill(pgid, 0) == nil
	r.mu.Lock()
	r.sent = append(r.sent, sentSignal{pgid, sig, exists})
	r.mu.Unlock()
	return syscall.Kill(-pgid, sig)
}

// cannotHold is awaitExit as every platform but Linux ships it: a leader is
// reaped before the registry drops its group.
func cannotHold(int) bool { return false }

// TestStopSendsNoFinalKillToAReapedLeadersGroup (issue #473 item 1, a
// platform that cannot hold a leader): a leader that exits on SIGTERM
// during the grace is reaped and its group leaves the registry, so the
// final SIGKILL must not go to that group: its ID may belong to another
// process by then. The signals are recorded, not left to a real recycled
// PID.
func TestStopSendsNoFinalKillToAReapedLeadersGroup(t *testing.T) {
	var rec groupSignals
	g := registry{signalGroup: rec.send, hold: cannotHold}
	cmd := exec.Command("sleep", "60")
	if err := g.start(cmd); err != nil {
		t.Fatal(err)
	}
	leader := cmd.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(-leader, syscall.SIGKILL) })
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
	if len(rec.sent) != 1 || rec.sent[0].sig != syscall.SIGTERM {
		t.Fatalf("signals = %+v, want only the SIGTERM: the leader was reaped, so its group gets no final SIGKILL", rec.sent)
	}
}
