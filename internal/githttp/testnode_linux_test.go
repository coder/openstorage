//go:build linux

package githttp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// nodeSysProcAttr starts serve as a session leader, so testNode.stop can
// kill serve together with its git children (issue #439). The new
// session also leaves the test binary's process group, so a signal sent
// to that group (an interactive Ctrl-C, a runner killing the job) no longer
// reaches serve: Pdeathsig kills serve when the test binary dies instead,
// and the node's guard (trackNodeSession) kills what serve leaves behind.
// The kernel sends Pdeathsig when the forking OS thread exits, and the Go
// runtime keeps its threads alive unless a goroutine exits while locked to
// one, which no test here does.
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

// nodeGuardArg is the first argument of the guard process, which the test
// binary starts as a copy of itself (see nodeHelperMain).
const nodeGuardArg = "nodeguard"

// nodeGuards holds the write end of each live node's guard pipe, by the
// node's session ID.
var nodeGuards sync.Map // session ID (int) -> *os.File

// nodeGuardExe is the program the guard runs as. Tests replace it to make
// the guard fail to start.
var nodeGuardExe = os.Executable

// trackNodeSession starts the guard of the node whose session leader is
// sid: a process in a session of its own that holds the read end of a pipe
// only this test binary writes to. When the test binary dies, however it
// dies (a panic, the -timeout panic, SIGKILL, a signal), the kernel closes
// the pipe, and the guard kills every process left in the node's session.
// serve itself ends through Pdeathsig, but its git children keep running
// without the guard (issue #471). A node that stops normally closes the
// pipe in untrackNodeSession, and the guard finds an empty session.
//
// No signal handler is involved: the test binary keeps the disposition it
// started with, so a SIGINT that its parent shell ignored (nohup, a
// background job) neither kills the nodes nor escapes the test binary.
//
// An error means the node has no guard: the caller must stop the node.
func trackNodeSession(sid int) error {
	// A leader that is already reaped has no start time (0): the guard then
	// leaves any process that owns the ID alone, whatever its start.
	start, err := processStart(sid)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("node session leader %d: %w", sid, err)
	}
	exe, err := nodeGuardExe()
	if err != nil {
		return fmt.Errorf("find the test binary for the node guard: %w", err)
	}
	r, w, err := os.Pipe() // close-on-exec: no child inherits the write end
	if err != nil {
		return err
	}
	guard := exec.Command(exe, nodeGuardArg, strconv.Itoa(sid), strconv.FormatUint(start, 10))
	guard.Stdin = r
	guard.Stderr = os.Stderr
	guard.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	err = guard.Start()
	_ = r.Close()
	if err != nil {
		_ = w.Close()
		return fmt.Errorf("start node guard: %w", err)
	}
	go func() { _ = guard.Wait() }()
	nodeGuards.Store(sid, w)
	return nil
}

func untrackNodeSession(sid int) {
	if w, ok := nodeGuards.LoadAndDelete(sid); ok {
		_ = w.(*os.File).Close()
	}
}

// procStat reads /proc/<pid>/stat: the state letter (field 3), the session
// (field 6) and the start time in clock ticks since boot (field 22). The
// fields follow the parenthesized command name.
func procStat(pid int) (state string, session int, start uint64, err error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", 0, 0, err
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return "", 0, 0, fmt.Errorf("unparsable /proc/%d/stat: %q", pid, data)
	}
	fields := bytes.Fields(data[end+1:])
	if len(fields) < 20 {
		return "", 0, 0, fmt.Errorf("unparsable /proc/%d/stat: %q", pid, data)
	}
	if session, err = strconv.Atoi(string(fields[3])); err != nil {
		return "", 0, 0, fmt.Errorf("unparsable /proc/%d/stat: %q", pid, data)
	}
	if start, err = strconv.ParseUint(string(fields[19]), 10, 64); err != nil {
		return "", 0, 0, fmt.Errorf("unparsable /proc/%d/stat: %q", pid, data)
	}
	return string(fields[0]), session, start, nil
}

// processStart returns the start time of process pid, in clock ticks since
// boot.
func processStart(pid int) (uint64, error) {
	_, _, start, err := procStat(pid)
	return start, err
}

// killSessionMembers SIGKILLs the live processes of session sid and returns
// them. The kernel hands out a PID again once its owner has exited and has
// been reaped, so a PID read from /proc can name another process by the time
// kill(2) runs. Each member is therefore pinned with a pidfd first and
// signaled through it: the signal reaches the process that was checked, or
// nothing.
func killSessionMembers(sid int) []int {
	var killed []int
	for _, pid := range sessionMembers(sid) {
		fd, err := unix.PidfdOpen(pid, 0)
		if errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil { // no pidfd (kernel before 5.3): the unpinned kill
			_ = syscall.Kill(pid, syscall.SIGKILL)
			killed = append(killed, pid)
			continue
		}
		// The pinned process is alive now and was alive when /proc was read
		// if it has not exited since: only then did the read describe it.
		if state, session, _, err := procStat(pid); err == nil && session == sid && state != "Z" && !pidfdExited(fd) {
			_ = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
			killed = append(killed, pid)
		}
		_ = unix.Close(fd)
	}
	return killed
}

// pidfdExited reports whether the process behind pidfd fd has exited (the
// descriptor is readable then).
func pidfdExited(fd int) bool {
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for {
		n, err := unix.Poll(fds, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err != nil || n > 0
	}
}

// reapNodeSession SIGKILLs every live process in session sid until none is
// left, and returns 0 (1 after timeout). The kernel reuses a session ID once
// the session is empty, so a process with ID sid and a start time other than
// start is an unrelated session leader, and its session stays alone.
func reapNodeSession(sid int, start uint64, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		if cur, err := processStart(sid); err == nil && cur != start {
			return 0
		}
		members := killSessionMembers(sid)
		if len(members) == 0 {
			return 0
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "node guard: session %d still has live members %v after %v\n", sid, members, timeout)
			return 1
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// nodeGuardMain is the guard process: args are the node's session ID and
// its leader's start time. It returns once stdin reaches EOF (the test
// binary closed the pipe or died) and the session is empty.
func nodeGuardMain(args []string) int {
	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "node guard: want <sid> <start>, got %q\n", args)
		return 2
	}
	sid, err := strconv.Atoi(args[0])
	start, err2 := strconv.ParseUint(args[1], 10, 64)
	if err != nil || err2 != nil || sid <= 1 {
		fmt.Fprintf(os.Stderr, "node guard: bad arguments %q\n", args)
		return 2
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	return reapNodeSession(sid, start, 10*time.Second)
}

// nodeCrasherEnv selects the crasher helper: the test binary run as a
// stand-in for a test process that dies. Its value is the crasher's mode,
// "wait" or "panic". nodeCrasherOutEnv names the file it reports its nodes in.
const (
	nodeCrasherEnv    = "OPENSTORAGE_TEST_NODE_CRASHER"
	nodeCrasherOutEnv = "OPENSTORAGE_TEST_NODE_CRASHER_OUT"
)

// nodeHelperMain runs the helper processes of the node guard before
// TestMain boots the environment: the guard itself, and the crasher
// (a test binary that runs only TestNodeCrasherHelper).
func nodeHelperMain(m *testing.M) (code int, ok bool) {
	switch {
	case len(os.Args) > 1 && os.Args[1] == nodeGuardArg:
		return nodeGuardMain(os.Args[2:]), true
	case os.Getenv(nodeCrasherEnv) != "":
		return m.Run(), true
	}
	return 0, false
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

// TestServeDrainTimeoutStopsGitChildren (issue #440): a push outlives the
// SIGTERM drain. serve must name the request still running, stop the
// push's git children before it exits 0, and leave the repository in a
// state the next node serves correctly: either the push never reached the
// authority (the client retries), or its ref CAS committed and hydration
// brings the cache up to the authority.
func TestServeDrainTimeoutStopsGitChildren(t *testing.T) {
	for _, tc := range []struct {
		name      string
		committed bool // the authority CAS commits before the stop
	}{
		{name: "before CAS"},
		{name: "after CAS", committed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := registerRepo(t, "t-drain-stop-"+strconv.FormatInt(time.Now().UnixNano(), 36))
			work := initWorkRepo(t)
			head := revParse(t, work, "HEAD")
			token := mintTestToken(t, repo, "git:read,git:write", "")
			nodeCache := t.TempDir()
			barrier := filepath.Join(t.TempDir(), "update-barrier")
			// Both knobs park the update hook past the drain: the delay
			// before its CAS, the barrier after it (for up to 30s).
			env := EnvHookDelay + "=60000"
			if tc.committed {
				env = EnvUpdateBarrier + "=" + barrier
			}
			var stderr bytes.Buffer
			node, err := startBinaryNode(binPath, nodeCache, io.MultiWriter(os.Stderr, &stderr), env)
			if err != nil {
				t.Fatalf("start node: %v", err)
			}
			t.Cleanup(node.stop)

			done := make(chan error, 1)
			go func() {
				_, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
				done <- err
			}()
			if tc.committed {
				waitForFile(t, barrier+".ready", 30*time.Second)
			} else {
				waitForPushHold(t, nodeCache, repo, "refs/heads/main", done)
			}
			children := serveChildren(node)
			if len(children) == 0 {
				t.Fatal("no git child of serve while a push is parked in its hook")
			}
			for _, pid := range children {
				if pgid, err := syscall.Getpgid(pid); err == nil && pgid == node.cmd.Process.Pid {
					t.Fatalf("git child %d runs in serve's process group, want its own", pid)
				}
			}

			sigterm := time.Now()
			if err := node.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err := waitNodeExit(t, node, 30*time.Second); err != nil {
				t.Fatalf("serve did not exit cleanly after the drain timed out: %v", err)
			}
			t.Logf("serve exited %v after SIGTERM", time.Since(sigterm))
			// A child SIGKILLed just before serve exited can still be in the
			// kernel's exit path: allow it a moment, never minutes (the
			// hook would otherwise sleep for 30-60s).
			for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				running := sessionMembers(node.cmd.Process.Pid)
				if len(running) == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("git children %v still run after serve exited", running)
				}
			}
			log := stderr.String()
			for _, want := range []string{
				"openstorage serve: drain incomplete",
				"openstorage serve: still active: POST \"/" + repo + ".git/git-receive-pack\" running for ",
				"openstorage serve: stopped 1 git process group(s)\n",
			} {
				if !strings.Contains(log, want) {
					t.Errorf("serve stderr lacks %q", want)
				}
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("push succeeded although serve stopped it")
				}
			case <-time.After(30 * time.Second):
				t.Fatal("client push still running 30s after serve exited")
			}

			// What the stopped push left in the cache repo: the handler
			// returned before serve exited, so its hold is released, and
			// git ended on SIGTERM, so no ref lock or quarantine remains.
			repoPath := RepoPath(nodeCache, repo)
			if holds, err := readPushHolds(filepath.Join(repoPath, pushHoldDir), ""); err != nil || len(holds.names) != 0 {
				t.Errorf("push holds after the stop = %v (err %v), want none", holds.names, err)
			}
			_ = filepath.WalkDir(repoPath, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				rel, _ := filepath.Rel(repoPath, path)
				if strings.HasPrefix(rel, "objects/tmp_objdir-incoming-") ||
					rel == "packed-refs.lock" || (strings.HasPrefix(rel, "refs/") && strings.HasSuffix(rel, ".lock")) {
					t.Errorf("stopped push left %s in the cache repo", rel)
				}
				return nil
			})

			// A new node on the same cache serves the authority's state.
			next, err := startNode(nodeCache)
			if err != nil {
				t.Fatalf("restart node: %v", err)
			}
			t.Cleanup(next.stop)
			url := remoteURL(next, repo, token)
			refs := mustGit(t, "", "ls-remote", url)
			if tc.committed != strings.Contains(refs, head+"\trefs/heads/main") {
				t.Fatalf("after the stop, ls-remote = %q, want main at %s: %v", refs, head, tc.committed)
			}
			if !tc.committed {
				mustGit(t, work, "push", url, "HEAD:refs/heads/main")
			}
			clone := filepath.Join(t.TempDir(), "clone")
			mustGit(t, "", "clone", "--config", "maintenance.auto=false", url, clone)
			if got := revParse(t, clone, "HEAD"); got != head {
				t.Fatalf("clone HEAD = %s, want %s", got, head)
			}
			mustGit(t, clone, "fsck", "--full")
		})
	}
}

// TestServeDrainTimeoutStopsRESTAndHydrationGit (issue #462): the drain
// timeout also stops the git children of REST reads, REST writes and cache
// hydration. A `git` wrapper first on serve's PATH parks the one git
// subcommand under test (it sleeps before exec-ing the real git), so the
// request outlives the drain.
func TestServeDrainTimeoutStopsRESTAndHydrationGit(t *testing.T) {
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, park, method, path, body, scopes string
	}{
		{name: "REST read", park: "log", method: http.MethodGet, path: "/api/v1/repos/commits?branch=main", scopes: "git:read"},
		{name: "REST write", park: "notes", method: http.MethodPost, path: "/api/v1/repos/notes", body: `{"sha":"%s","note":"n"}`, scopes: "git:read,git:write"},
		{name: "hydration", park: "index-pack", method: http.MethodGet, path: "/%s.git/info/refs?service=git-upload-pack", scopes: "git:read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := registerRepo(t, "t-drain-rest-"+strconv.FormatInt(time.Now().UnixNano(), 36))
			work := initWorkRepo(t)
			head := revParse(t, work, "HEAD")
			mustGit(t, work, "push", remoteURL(nodeA, repo, mintTestToken(t, repo, "git:read,git:write", "")), "HEAD:refs/heads/main")

			bin := t.TempDir()
			parked := filepath.Join(bin, "parked")
			script := fmt.Sprintf("#!/bin/sh\nfor a in \"$@\"; do if [ \"$a\" = %q ]; then : > %q; sleep 60; fi; done\nexec %q \"$@\"\n", tc.park, parked, realGit)
			if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cache := t.TempDir()
			node, err := startBinaryNode(binPath, cache, io.MultiWriter(os.Stderr, &stderr), "PATH="+bin+":"+os.Getenv("PATH"))
			if err != nil {
				t.Fatalf("start node: %v", err)
			}
			t.Cleanup(node.stop)

			path, body := tc.path, tc.body
			if strings.Contains(path, "%s") {
				path = fmt.Sprintf(path, repo)
			}
			if body != "" {
				body = fmt.Sprintf(body, head)
			}
			req, err := http.NewRequest(tc.method, "http://"+node.addr+path, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			token := mintTestToken(t, repo, tc.scopes, "")
			if strings.HasPrefix(path, "/api/") {
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", "application/json")
			} else {
				req.SetBasicAuth("t", token)
			}
			done := make(chan error, 1)
			go func() {
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				done <- err
			}()
			waitForFile(t, parked, 30*time.Second)

			if err := node.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err := waitNodeExit(t, node, 30*time.Second); err != nil {
				t.Fatalf("serve did not exit cleanly after the drain timed out: %v", err)
			}
			for deadline := time.Now().Add(2 * time.Second); len(sessionMembers(node.cmd.Process.Pid)) > 0; time.Sleep(10 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("git children %v still run after serve exited", sessionMembers(node.cmd.Process.Pid))
				}
			}
			reqPath, _, _ := strings.Cut(path, "?")
			for _, want := range []string{
				"openstorage serve: still active: " + tc.method + " \"" + reqPath + "\" running for ",
				"openstorage serve: stopped 1 git process group(s)\n",
			} {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("serve stderr lacks %q", want)
				}
			}
			select {
			case <-done: //nolint:errcheck // the request may fail once serve exits: only its end matters
			case <-time.After(30 * time.Second):
				t.Fatal("client request still running 30s after serve exited")
			}

			// A new node on the same cache serves the repository.
			next, err := startNode(cache)
			if err != nil {
				t.Fatalf("restart node: %v", err)
			}
			t.Cleanup(next.stop)
			clone := filepath.Join(t.TempDir(), "clone")
			mustGit(t, "", "clone", "--config", "maintenance.auto=false", remoteURL(next, repo, mintTestToken(t, repo, "git:read", "")), clone)
			if got := revParse(t, clone, "HEAD"); got != head {
				t.Fatalf("clone HEAD = %s, want %s", got, head)
			}
			mustGit(t, clone, "fsck", "--full")
		})
	}
}

// TestNodeCrasherHelper is the crasher: it starts a stand-in node and then
// waits to be killed or panics. The node is a bash process (serve's
// stand-in, a session leader) with a child in a process group of its own
// (the stand-in of a git service child). TestNodeGuardKillsSessionWhenTestBinaryDies
// runs it in a copy of the test binary and kills that copy.
func TestNodeCrasherHelper(t *testing.T) {
	mode := os.Getenv(nodeCrasherEnv)
	if mode == "" {
		t.Skip("helper process of TestNodeGuardKillsSessionWhenTestBinaryDies")
	}
	cmd := exec.Command("bash", "-c", "set -m; sleep 600 & echo $!; wait")
	cmd.SysProcAttr = nodeSysProcAttr()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := trackNodeSession(cmd.Process.Pid); err != nil {
		// Pdeathsig would kill only the leader, so end the whole session.
		start, _ := processStart(cmd.Process.Pid)
		reapNodeSession(cmd.Process.Pid, start, 10*time.Second)
		_ = cmd.Wait()
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read the stand-in's child PID: %v", err)
	}
	child, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("stand-in child PID %q: %v", line, err)
	}
	serve := cmd.Process.Pid
	if pgid, err := syscall.Getpgid(child); err != nil || pgid == serve {
		t.Fatalf("stand-in git child %d: process group %d (err %v), want one of its own", child, pgid, err)
	}
	if sid, err := unix.Getsid(child); err != nil || sid != serve {
		t.Fatalf("stand-in git child %d: session %d (err %v), want the node's session %d", child, sid, err, serve)
	}
	start, err := processStart(serve)
	if err != nil {
		t.Fatal(err)
	}
	childStart, err := processStart(child)
	if err != nil {
		t.Fatal(err)
	}
	out := os.Getenv(nodeCrasherOutEnv)
	if err := os.WriteFile(out+".tmp", []byte(fmt.Sprintf("%d %d %d %d\n", serve, child, start, childStart)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(out+".tmp", out); err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "panic":
		panic("node crasher: test panic")
	case "wait":
		time.Sleep(time.Hour) // until killed, or until -test.timeout panics
	default:
		t.Fatalf("unknown crasher mode %q", mode)
	}
}

// TestNodeGuardKillsSessionWhenTestBinaryDies (issue #471): serve ends
// with the test binary through Pdeathsig, but its git children keep running
// unless something kills them. Each case starts a copy of the test binary
// that owns a stand-in node, ends the copy the way the case says, and
// requires the node's whole session to be gone, the child in its own
// process group included. A test binary that ignores SIGINT (nohup, a
// background job) is not killed by it, and its nodes must survive.
func TestNodeGuardKillsSessionWhenTestBinaryDies(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, mode   string
		timeout      string // -test.timeout of the crasher
		ignoreSIGINT bool   // the crasher starts with SIGINT ignored
		die          func(t *testing.T, crasher *exec.Cmd, childAlive func() bool)
		wantExit     int // exit code of a crasher that ends by itself, or -1
	}{
		{name: "SIGKILL", mode: "wait", wantExit: -1, die: func(t *testing.T, c *exec.Cmd, _ func() bool) { _ = c.Process.Kill() }},
		{name: "panic", mode: "panic", wantExit: 2},
		{name: "test timeout", mode: "wait", timeout: "5s", wantExit: 2}, // long enough to report its node on a loaded host
		{name: "SIGINT", mode: "wait", wantExit: -1, die: func(t *testing.T, c *exec.Cmd, _ func() bool) { _ = c.Process.Signal(syscall.SIGINT) }},
		{name: "SIGINT ignored, then SIGKILL", mode: "wait", ignoreSIGINT: true, wantExit: -1, die: func(t *testing.T, c *exec.Cmd, childAlive func() bool) {
			_ = c.Process.Signal(syscall.SIGINT)
			time.Sleep(time.Second)
			if err := c.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("the test binary died of an ignored SIGINT: %v", err)
			}
			if !childAlive() {
				t.Fatal("the node's git child died of a SIGINT the test binary ignores")
			}
			_ = c.Process.Kill()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outFile := filepath.Join(t.TempDir(), "nodes")
			args := []string{"-test.run=^TestNodeCrasherHelper$", "-test.count=1"}
			if tc.timeout != "" {
				args = append(args, "-test.timeout="+tc.timeout)
			}
			crasher := exec.Command(exe, args...)
			if tc.ignoreSIGINT {
				crasher = exec.Command("sh", append([]string{"-c", `trap '' INT; exec "$@"`, "sh", exe}, args...)...)
			}
			crasher.Env = append(os.Environ(), nodeCrasherEnv+"="+tc.mode, nodeCrasherOutEnv+"="+outFile)
			var log bytes.Buffer
			crasher.Stdout, crasher.Stderr = &log, &log
			crasher.WaitDelay = 30 * time.Second
			if err := crasher.Start(); err != nil {
				t.Fatal(err)
			}
			reaped := false
			t.Cleanup(func() {
				if !reaped {
					_ = crasher.Process.Kill()
					_ = crasher.Wait()
				}
			})

			var serve, child int
			var start, childStart uint64
			for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				data, err := os.ReadFile(outFile)
				if err == nil {
					if _, err := fmt.Sscanf(string(data), "%d %d %d %d", &serve, &child, &start, &childStart); err != nil {
						t.Fatalf("unparsable node report %q: %v", data, err)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the test binary reported no node within 30s; output:\n%s", log.String())
				}
			}
			t.Cleanup(func() {
				reapNodeSession(serve, start, 10*time.Second)
			})

			// The kernel reuses PIDs: a process counts as the node's only
			// while its start time is the one reported.
			childAlive := func() bool {
				state, _, cs, err := procStat(child)
				return err == nil && state != "Z" && cs == childStart
			}
			sessionLive := func() []int {
				if cur, err := processStart(serve); err == nil && cur != start {
					return nil // an unrelated session took the ID over
				}
				return sessionMembers(serve)
			}
			if tc.die != nil {
				tc.die(t, crasher, childAlive)
			}
			waitErr := crasher.Wait()
			reaped = true
			var exitErr *exec.ExitError
			switch {
			case tc.wantExit >= 0:
				if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != tc.wantExit {
					t.Fatalf("test binary ended with %v, want exit code %d; output:\n%s", waitErr, tc.wantExit, log.String())
				}
			case waitErr == nil:
				t.Fatalf("test binary ended cleanly, want death by a signal")
			}

			for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				members := sessionLive()
				if len(members) == 0 && !childAlive() {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("15s after the test binary died, node session %d still has live members %v (git child %d alive: %v)", serve, members, child, childAlive())
				}
			}
		})
	}
}

// procState returns the state letter of process pid from /proc/<pid>/stat,
// or "" when the process is gone. A zombie is "Z".
func procState(pid int) string {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return ""
	}
	return string(data[end+2])
}

// startStandInNode starts a stand-in node as the crasher does: bash as
// session leader, and a child in a process group of its own. It returns
// both PIDs and kills their session when the test ends.
func startStandInNode(t *testing.T) (serve, child int, cmd *exec.Cmd) {
	t.Helper()
	cmd = exec.Command("bash", "-c", "set -m; sleep 600 & echo $!; wait")
	cmd.SysProcAttr = nodeSysProcAttr()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	serve = cmd.Process.Pid
	start, err := processStart(serve)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		reapNodeSession(serve, start, 10*time.Second)
		_ = cmd.Wait()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if child, err = strconv.Atoi(strings.TrimSpace(line)); err != nil {
		t.Fatal(err)
	}
	return serve, child, cmd
}

// TestKillSessionMembers: killSessionMembers ends every live process of the
// node's session, a child in a process group of its own included, and
// leaves a process in another session alone.
func TestKillSessionMembers(t *testing.T) {
	serve, child, cmd := startStandInNode(t)
	bystander := exec.Command("sleep", "600")
	bystander.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := bystander.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = bystander.Process.Kill()
		_ = bystander.Wait()
	})

	killed := killSessionMembers(serve)
	if len(killed) != 2 {
		t.Fatalf("killed %v, want the leader %d and its child %d", killed, serve, child)
	}
	_ = cmd.Wait()
	for deadline := time.Now().Add(10 * time.Second); sessionMembers(serve) != nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("session %d still has live members %v", serve, sessionMembers(serve))
		}
	}
	if st := procState(bystander.Process.Pid); st == "" || st == "Z" {
		t.Fatalf("a process of another session died (state %q)", st)
	}
	if got := killSessionMembers(serve); len(got) != 0 {
		t.Fatalf("an empty session still yields members %v", got)
	}
}

// TestTrackNodeSessionReportsGuardStartFailure: a node whose guard cannot
// start (the process or memory limit of a loaded host) is reported to the
// caller, which stops the node. It must not panic and leave the node running
// without a guard.
func TestTrackNodeSessionReportsGuardStartFailure(t *testing.T) {
	serve, _, _ := startStandInNode(t)
	old := nodeGuardExe
	nodeGuardExe = func() (string, error) { return "/nonexistent/node-guard", nil }
	t.Cleanup(func() { nodeGuardExe = old })

	if err := trackNodeSession(serve); err == nil {
		t.Fatal("trackNodeSession succeeded although the guard cannot start")
	}
	if _, ok := nodeGuards.Load(serve); ok {
		t.Fatal("a failed guard start left its pipe registered")
	}
}
