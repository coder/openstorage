//go:build linux

package gitlimit

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// procState returns the state letter of process pid from /proc/<pid>/stat
// (the field after the parenthesized name), or "" when the process is gone.
// A zombie shows as "Z": the PID is still taken.
func procState(pid int) string {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		panic(fmt.Sprintf("unparsable /proc/%d/stat: %q", pid, data))
	}
	return string(data[end+2])
}

// groupSignals records every signal the registry sends to a group, with the
// state of the group's leader at that moment.
type groupSignals struct {
	mu   sync.Mutex
	sent []sentSignal
}

type sentSignal struct {
	pgid        int
	sig         syscall.Signal
	leaderState string
}

func (r *groupSignals) send(pgid int, sig syscall.Signal) error {
	state := procState(pgid)
	r.mu.Lock()
	r.sent = append(r.sent, sentSignal{pgid, sig, state})
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
	if got := rec.sent[1].leaderState; got != "Z" {
		t.Fatalf("leader state at the final SIGKILL = %q, want %q (exited, not reaped)", got, "Z")
	}
	for deadline := time.Now().Add(5 * time.Second); !errors.Is(syscall.Kill(orphan, 0), syscall.ESRCH); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("member %d of the stopped group is still alive", orphan)
		}
	}
}

// TestStopNeverSignalsAReapedLeader runs short children against stops: no
// signal may go to a group whose leader is already gone from /proc.
func TestStopNeverSignalsAReapedLeader(t *testing.T) {
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
		if s.leaderState == "" {
			t.Fatalf("%v sent to group %d whose leader was already reaped", s.sig, s.pgid)
		}
	}
}
