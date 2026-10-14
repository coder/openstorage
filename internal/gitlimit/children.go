package gitlimit

import (
	"bytes"
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// ErrStopping refuses a git child started after StopChildren began.
var ErrStopping = errors.New("server is shutting down")

// children tracks the git children serve starts for requests and for cache
// hydration: upload-pack, receive-pack and its hooks, REST reads and
// writes, and hydration (issues #440, #462). serve ends them before it
// exits. Each child leads its own process group, which its own children
// inherit. A group is tracked from Start until Wait reaps its leader.
// Hook processes must not start children through it: a hook's child would
// leave receive-pack's group and escape the stop.
var children registry

type registry struct {
	mu      sync.Mutex
	pgids   map[int]struct{}
	stopped bool
}

// Start starts cmd as the leader of a new process group and tracks it.
func Start(cmd *exec.Cmd) error { return children.start(cmd) }

// Wait waits for a child that Start started and stops tracking its group.
func Wait(cmd *exec.Cmd) error { return children.wait(cmd) }

// Run is Start followed by Wait.
func Run(cmd *exec.Cmd) error {
	if err := Start(cmd); err != nil {
		return err
	}
	return Wait(cmd)
}

// Output is exec.Cmd.Output through the registry. cmd.Stdout must be nil.
func Output(cmd *exec.Cmd) ([]byte, error) {
	assertf(cmd.Stdout == nil, "gitlimit.Output: Stdout already set")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := Run(cmd)
	return out.Bytes(), err
}

// CombinedOutput is exec.Cmd.CombinedOutput through the registry.
func CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
	assertf(cmd.Stdout == nil && cmd.Stderr == nil, "gitlimit.CombinedOutput: Stdout or Stderr already set")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := Run(cmd)
	return out.Bytes(), err
}

// Stopping reports whether StopChildren began.
func Stopping() bool { return children.isStopped() }

// StopChildren ends every tracked git child and refuses new ones: serve
// calls it when the shutdown drain times out, so no tracked child outlives
// serve. It sends SIGTERM to each child's process group, waits up to grace
// for them to be reaped, then sends SIGKILL to those groups. It returns
// the number of groups it signaled.
func StopChildren(grace time.Duration) int { return children.stop(grace) }

func (g *registry) start(cmd *exec.Cmd) error {
	assertf(cmd.SysProcAttr == nil, "gitlimit.Start: cmd already has SysProcAttr")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if g.isStopped() {
		return ErrStopping
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		// stop ran during Start, so its signals missed this group.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		return ErrStopping
	}
	if g.pgids == nil {
		g.pgids = make(map[int]struct{})
	}
	g.pgids[cmd.Process.Pid] = struct{}{}
	return nil
}

func (g *registry) wait(cmd *exec.Cmd) error {
	err := cmd.Wait()
	g.mu.Lock()
	delete(g.pgids, cmd.Process.Pid)
	g.mu.Unlock()
	return err
}

func (g *registry) isStopped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopped
}

// stop sends SIGTERM first, because git removes its lock files and
// quarantine on SIGTERM while a SIGKILL inside a ref transaction leaves a
// stale ref lock in the cache repo. After grace (or once every leader is
// reaped) each group gets SIGKILL, which also ends a child left in a
// reaped leader's group. Linux does not reuse a PID while a group with
// that ID has members, so that SIGKILL can reach another group only if the
// kernel hands out the same PID again within grace (#473).
func (g *registry) stop(grace time.Duration) int {
	g.mu.Lock()
	g.stopped = true
	pgids := make([]int, 0, len(g.pgids))
	for pgid := range g.pgids {
		pgids = append(pgids, pgid)
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	}
	g.mu.Unlock()
	for deadline := time.Now().Add(grace); len(pgids) > 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		g.mu.Lock()
		left := len(g.pgids)
		g.mu.Unlock()
		if left == 0 {
			break
		}
	}
	for _, pgid := range pgids {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
	return len(pgids)
}
