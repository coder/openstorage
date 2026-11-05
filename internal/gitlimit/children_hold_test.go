//go:build unix

package gitlimit

import (
	"bufio"
	"errors"
	"os/exec"
	"strconv"
	"strings"
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

// TestStopSignalsOnlyGroupsWithAnUnreapedLeader (issue #473 item 1): the
// final SIGKILL must never reach a recycled PID, which can happen only for
// a group whose leader was reaped and whose ID the kernel handed out
// again. A leader that exits during the grace therefore stays a zombie,
// holding its PID, until stop has sent SIGKILL, and that SIGKILL still
// ends the member the leader left behind.
func TestStopSignalsOnlyGroupsWithAnUnreapedLeader(t *testing.T) {
	if !leaderHoldSupported() {
		t.Skip("this platform cannot wait for an exit without reaping")
	}
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

	if n := g.stop(30 * time.Second); n != 1 {
		t.Fatalf("stop signaled %d groups, want 1", n)
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

// TestStopNeverSignalsAReapedLeader runs short children against stops: no
// signal may go to a group whose leader is already reaped.
func TestStopNeverSignalsAReapedLeader(t *testing.T) {
	if !leaderHoldSupported() {
		t.Skip("this platform reaps a leader before it removes the group")
	}
	var rec groupSignals
	g := registry{signalGroup: rec.send}
	var wg sync.WaitGroup
	for i := range 60 {
		cmd := exec.Command("sh", "-c", "exec sleep 0.05")
		if err := g.start(cmd); err != nil {
			if errors.Is(err, ErrStopping) {
				break
			}
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = g.wait(cmd)
		}()
		if i == 30 {
			go g.stop(20 * time.Millisecond)
		}
	}
	wg.Wait()
	g.stop(0)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, s := range rec.sent {
		if !s.leaderExists {
			t.Fatalf("%v sent to group %d whose leader was already reaped", s.sig, s.pgid)
		}
	}
}
