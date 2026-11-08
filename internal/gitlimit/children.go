package gitlimit

import (
	"bytes"
	"errors"
	"os"
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
	// hold waits for the leader pid to exit without reaping it and reports
	// whether it did. Nil means awaitExit. Tests replace it to run the path
	// of a platform that cannot hold a leader.
	hold func(pid int) bool
	// signalGroup sends sig to the process group pgid. Nil means
	// syscall.Kill(-pgid, sig). Tests replace it to observe the leader at
	// the moment the signal goes out.
	signalGroup func(pgid int, sig syscall.Signal) error
}

// group is a tracked process group whose leader wait has not removed yet.
type group struct {
	leader *os.Process
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
	g.groups[cmd.Process.Pid] = &group{leader: cmd.Process}
	return nil
}

// wait returns once cmd's leader is reaped. A signal sent to a group whose
// leader was reaped can reach an unrelated group: once the group has no
// member left, the kernel can hand its ID to a new process (#473). So the
// registry signals only groups whose leader it has not reaped:
//
//   - A leader leaves the registry under the lock before it is reaped, and
//     stop signals only groups it finds in the registry while holding the
//     lock. Every signal reaches a group whose leader still owns its ID, as
//     a running process or as an unreaped zombie.
//   - A leader that exits after stop began is not reaped until stop has
//     sent its final SIGKILL. That kill still reaches a member the leader
//     left behind, such as a hook that ignored SIGTERM.
//
// Both rest on awaitExit, a wait that leaves the child unreaped. Only Linux
// has one. Elsewhere wait reaps first and removes the group right after, so
// stop sends no signal to a group that has left the registry, and signal
// skips a registered group whose leader os reports reaped. A leader that
// exits during the grace gets no final SIGKILL there, and a member it left
// behind lives on. What stays open is the race that os.Process.Signal has
// on macOS (golang/go#13987): the leader is reaped between the check and
// the kill.
func (g *registry) wait(cmd *exec.Cmd) error {
	pid := cmd.Process.Pid
	hold := g.hold
	if hold == nil {
		hold = awaitExit
	}
	if !hold(pid) {
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

// signal sends sig to gr's process group. The caller holds g.mu and found gr
// in g.groups. Where wait reaps first, a reaped leader stays registered
// until wait takes the lock, and its group ID may belong to another group
// by then. os reports a reap as soon as the wait call returns, so a leader
// it reports done gets no signal.
func (g *registry) signal(gr *group, sig syscall.Signal) {
	if gr.leader.Signal(syscall.Signal(0)) != nil {
		return
	}
	pgid := gr.leader.Pid
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
	for pgid, gr := range g.groups {
		pgids = append(pgids, pgid)
		g.signal(gr, syscall.SIGTERM)
	}
	g.mu.Unlock()
	for deadline := time.Now().Add(grace); g.running(pgids) > 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
	}
	g.mu.Lock()
	for _, pgid := range pgids {
		if gr, ok := g.groups[pgid]; ok {
			g.signal(gr, syscall.SIGKILL)
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
