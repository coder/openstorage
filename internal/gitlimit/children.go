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
	groups  map[int]*group // by group ID, which is the leader's PID
	stopped bool
	// killed is made by the first stop and closed once a stop has sent its
	// final SIGKILLs. A leader that exits after the stop began stays
	// unreaped until then (see wait).
	killed chan struct{}
	// signalGroup sends sig to the process group pgid. Nil means
	// syscall.Kill(-pgid, sig). Tests replace it to observe the leader at
	// the moment the signal goes out.
	signalGroup func(pgid int, sig syscall.Signal) error
}

// group is a tracked process group whose leader has not been reaped.
type group struct {
	exited bool // the leader exited while stop ran, so wait holds it unreaped
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
// for the leaders to exit, then sends SIGKILL to those groups. It returns
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
	if g.groups == nil {
		g.groups = make(map[int]*group)
	}
	g.groups[cmd.Process.Pid] = &group{}
	return nil
}

// wait returns once cmd's leader is reaped. A signal sent to a group whose
// leader was reaped can reach an unrelated group: once the group has no
// member left, the kernel can hand its ID to a new process (#473). So the
// registry never signals a group whose leader may be reaped:
//
//   - A leader leaves the registry under the lock before it is reaped, and
//     stop signals only groups it finds in the registry while holding the
//     lock. Every signal reaches a group whose leader still owns its ID, as
//     a running process or as an unreaped zombie.
//   - A leader that exits after stop began is not reaped until stop has
//     sent its final SIGKILL. That kill still reaches a member the leader
//     left behind, such as a hook that ignored SIGTERM.
//
// awaitExit waits with waitid and WNOWAIT on Linux and on macOS. Where it
// cannot (macOS when its startup check of waitid fails, and every other
// platform), the leader is reaped first and removed right after, so a
// signal that stop sends between the two can still reach a recycled ID.
// That window lasts until wait takes the lock.
func (g *registry) wait(cmd *exec.Cmd) error {
	pid := cmd.Process.Pid
	if !awaitExit(pid) {
		err := cmd.Wait()
		g.mu.Lock()
		delete(g.groups, pid)
		g.mu.Unlock()
		return err
	}
	g.mu.Lock()
	if gr := g.groups[pid]; gr != nil && g.stopped {
		gr.exited = true
		killed := g.killed
		g.mu.Unlock()
		<-killed
		g.mu.Lock()
	}
	delete(g.groups, pid)
	g.mu.Unlock()
	return cmd.Wait()
}

func (g *registry) isStopped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopped
}

// signal sends sig to the process group pgid. The caller holds g.mu and
// found pgid in g.groups.
func (g *registry) signal(pgid int, sig syscall.Signal) {
	if g.signalGroup != nil {
		_ = g.signalGroup(pgid, sig)
		return
	}
	_ = syscall.Kill(-pgid, sig)
}

// stop sends SIGTERM first, because git removes its lock files and
// quarantine on SIGTERM while a SIGKILL inside a ref transaction leaves a
// stale ref lock in the cache repo. After grace (or once every leader has
// exited) each group still registered gets SIGKILL, which also ends a
// child left behind by a leader that exited meanwhile.
func (g *registry) stop(grace time.Duration) int {
	g.mu.Lock()
	if !g.stopped {
		g.stopped = true
		g.killed = make(chan struct{})
	}
	killed := g.killed
	pgids := make([]int, 0, len(g.groups))
	for pgid := range g.groups {
		pgids = append(pgids, pgid)
		g.signal(pgid, syscall.SIGTERM)
	}
	g.mu.Unlock()
	for deadline := time.Now().Add(grace); g.running(pgids) > 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
	}
	g.mu.Lock()
	for _, pgid := range pgids {
		if _, ok := g.groups[pgid]; ok {
			g.signal(pgid, syscall.SIGKILL)
		}
	}
	select {
	case <-killed:
	default:
		close(killed)
	}
	g.mu.Unlock()
	return len(pgids)
}

// running counts the groups among pgids whose leader has not exited.
func (g *registry) running(pgids []int) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, pgid := range pgids {
		if gr, ok := g.groups[pgid]; ok && !gr.exited {
			n++
		}
	}
	return n
}
