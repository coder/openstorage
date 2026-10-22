//go:build linux

package githttp

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
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
