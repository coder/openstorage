//go:build linux

package githttp

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// nodeSysProcAttr starts serve as a session leader, so testNode.stop can
// kill serve together with its git children (issue #439). The new
// session also leaves the test binary's process group, so a signal sent
// to that group (an interactive Ctrl-C, a runner killing the job) no longer
// reaches serve: Pdeathsig kills serve when the test binary dies instead.
// The kernel sends it when the forking OS thread exits, and the Go runtime
// keeps its threads alive unless a goroutine exits while locked to one,
// which no test here does.
func nodeSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true, Pdeathsig: syscall.SIGKILL}
}

// sessionMembers lists the live (not zombie) processes whose session is
// sid, from /proc/<pid>/stat: state is field 3 and session field 6, both
// after the parenthesized command name. A zombie can no longer write, and
// an init that does not reap adopted children keeps it forever.
func sessionMembers(sid int) []int {
	stats, err := filepath.Glob("/proc/[0-9]*/stat")
	if err != nil {
		panic(err)
	}
	var pids []int
	for _, path := range stats {
		data, err := os.ReadFile(path)
		if err != nil {
			continue // The process exited between the glob and the read.
		}
		end := bytes.LastIndexByte(data, ')')
		if end < 0 {
			panic(fmt.Sprintf("unparsable %s: %q", path, data))
		}
		fields := bytes.Fields(data[end+1:])
		if len(fields) < 4 {
			panic(fmt.Sprintf("unparsable %s: %q", path, data))
		}
		if s, err := strconv.Atoi(string(fields[3])); err == nil && s == sid && string(fields[0]) != "Z" {
			pid, err := strconv.Atoi(filepath.Base(filepath.Dir(path)))
			if err != nil {
				panic(err)
			}
			pids = append(pids, pid)
		}
	}
	return pids
}

// liveNodeSessions holds the session IDs of nodes not yet stopped, for
// killNodesOnSignal.
var (
	liveNodeSessions    sync.Map // session ID (int) -> struct{}
	installSignalKiller sync.Once
)

// trackNodeSession records a started node's session. The first call also
// installs killNodesOnSignal: the nodes' sessions are outside the test
// binary's process group, so a Ctrl-C or a runner's SIGTERM to that group
// reaches neither serve nor its git children. Pdeathsig ends serve when
// the test binary dies, but not serve's children.
func trackNodeSession(sid int) {
	installSignalKiller.Do(func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
		go killNodesOnSignal(ch)
	})
	liveNodeSessions.Store(sid, struct{}{})
}

func untrackNodeSession(sid int) { liveNodeSessions.Delete(sid) }

// killNodesOnSignal SIGKILLs every process in every live node session when
// the test binary gets a termination signal, then dies by that signal.
func killNodesOnSignal(ch <-chan os.Signal) {
	sig := <-ch
	liveNodeSessions.Range(func(sid, _ any) bool {
		for _, pid := range sessionMembers(sid.(int)) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		return true
	})
	signal.Reset(os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	_ = syscall.Kill(os.Getpid(), sig.(syscall.Signal))
}

// serveChildren lists the processes in n's session other than serve.
func serveChildren(n *testNode) []int {
	var pids []int
	for _, pid := range sessionMembers(n.cmd.Process.Pid) {
		if pid != n.cmd.Process.Pid {
			pids = append(pids, pid)
		}
	}
	return pids
}

// TestNodeStopKillsGitChildren: testNode.stop must take serve's git
// children down with serve (issue #439). A push parks in its update hook
// (EnvHookDelay), so receive-pack and the hook run while stop kills serve.
// Before the fix, stop killed serve alone and both kept running in the
// test's cache dir.
func TestNodeStopKillsGitChildren(t *testing.T) {
	repo := registerRepo(t, "t-stop-children-"+strconv.FormatInt(time.Now().UnixNano(), 36))
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	nodeCache := t.TempDir()
	node, err := startNode(nodeCache, EnvHookDelay+"=60000")
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	done := make(chan error, 1)
	go func() {
		_, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
		done <- err
	}()
	// pre-receive reserves the hold before the update hook's delay, so the
	// hook and receive-pack are alive once the hold shows.
	waitForPushHold(t, nodeCache, repo, "refs/heads/main", done)
	children := serveChildren(node)
	if len(children) == 0 {
		t.Fatalf("no git child in serve's session while a push is parked in its hook")
	}

	node.stop()
	if left := sessionMembers(node.cmd.Process.Pid); len(left) != 0 {
		t.Errorf("serve children %v still run after stop (before: %v)", left, children)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("push succeeded although stop killed serve mid-push")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("client push still running 30s after stop")
	}
}
