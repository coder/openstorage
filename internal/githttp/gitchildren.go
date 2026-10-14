package githttp

import (
	"errors"
	"net/http"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// errServerStopping refuses a git child started after stop began.
var errServerStopping = errors.New("server is shutting down")

// gitChildren tracks the handlers' git service children (upload-pack,
// receive-pack and its hooks) so serve can end them before it exits
// (issue #440). Each child leads its own process group, which its hooks
// inherit; a group is tracked from start until wait reaps its leader.
type gitChildren struct {
	mu      sync.Mutex
	pgids   map[int]struct{}
	stopped bool
}

// start starts cmd as the leader of a new process group and tracks it.
func (g *gitChildren) start(cmd *exec.Cmd) error {
	assertf(cmd.SysProcAttr == nil, "gitChildren.start: cmd already has SysProcAttr")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if g.isStopped() {
		return errServerStopping
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
		return errServerStopping
	}
	if g.pgids == nil {
		g.pgids = make(map[int]struct{})
	}
	g.pgids[cmd.Process.Pid] = struct{}{}
	return nil
}

// wait waits for a child that start started and stops tracking its group.
func (g *gitChildren) wait(cmd *exec.Cmd) error {
	err := cmd.Wait()
	g.mu.Lock()
	delete(g.pgids, cmd.Process.Pid)
	g.mu.Unlock()
	return err
}

func (g *gitChildren) run(cmd *exec.Cmd) error {
	if err := g.start(cmd); err != nil {
		return err
	}
	return g.wait(cmd)
}

func (g *gitChildren) isStopped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopped
}

// abortIfStopped aborts the response when the child failed after stop
// began, so most likely by stop's signals: git 2.55 push waits forever on
// a 200 whose stream was cut off, but fails at once on a dropped
// connection. The panic still runs the handler's deferred cleanup.
func (g *gitChildren) abortIfStopped(err error) {
	if err != nil && g.isStopped() {
		panic(http.ErrAbortHandler)
	}
}

// stop refuses new children and ends the tracked groups: SIGTERM first,
// because git removes its lock files and quarantine on SIGTERM while a
// SIGKILL inside receive-pack's ref transaction leaves a stale ref lock in
// the cache repo. After grace (or once every leader is reaped) each group
// gets SIGKILL, which also ends a hook left in a reaped leader's group.
// Linux does not reuse a PID while a group with that ID has members, so
// that SIGKILL can reach another group only if the kernel hands out the
// same PID again within grace. stop returns the number of groups.
func (g *gitChildren) stop(grace time.Duration) int {
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

// StopGitChildren ends the git children that handlers still run and
// refuses new ones (issue #440): SIGTERM to each child's process group,
// SIGKILL after grace. It returns the number of groups it signaled.
func (s *Server) StopGitChildren(grace time.Duration) int {
	return s.gitChildren.stop(grace)
}
