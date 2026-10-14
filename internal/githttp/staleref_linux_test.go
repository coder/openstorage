//go:build linux

package githttp

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestRemoveStaleRefLocks: an old ref lock is removed only while no
// process works in the repository (issue #464).
func TestRemoveStaleRefLocks(t *testing.T) {
	old := time.Now().Add(-2 * staleRefLockAge)
	for _, tc := range []struct {
		name    string
		lock    string    // relative to the repo
		mtime   time.Time // zero: now
		process func(repo, real string) *exec.Cmd
		removed int
	}{
		{name: "old loose lock", lock: "refs/heads/x.lock", mtime: old, removed: 1},
		{name: "old packed-refs lock", lock: "packed-refs.lock", mtime: old, removed: 1},
		{name: "old lock below the ref", lock: "refs/heads/x/y.lock", mtime: old, removed: 1},
		{name: "young lock", lock: "refs/heads/x.lock"},
		{name: "process works in the repo", lock: "refs/heads/x.lock", mtime: old, process: func(repo, _ string) *exec.Cmd {
			cmd := exec.Command("sleep", "30")
			cmd.Dir = filepath.Join(repo, "refs")
			return cmd
		}},
		{name: "process names the repo", lock: "refs/heads/x.lock", mtime: old, process: func(repo, _ string) *exec.Cmd {
			return exec.Command("sh", "-c", "sleep 30", repo) // $0 = repo
		}},
		{name: "process in the symlink target", lock: "refs/heads/x.lock", mtime: old, process: func(_, real string) *exec.Cmd {
			cmd := exec.Command("sleep", "30")
			cmd.Dir = real
			return cmd
		}},
		{name: "process names the repo relatively", lock: "refs/heads/x.lock", mtime: old, process: func(repo, _ string) *exec.Cmd {
			cmd := exec.Command("sh", "-c", "sleep 30", "--git-dir="+filepath.Base(repo))
			cmd.Dir = filepath.Dir(repo)
			return cmd
		}},
		{name: "process with GIT_DIR", lock: "refs/heads/x.lock", mtime: old, process: func(repo, _ string) *exec.Cmd {
			cmd := exec.Command("sleep", "30")
			cmd.Dir = filepath.Dir(repo)
			cmd.Env = append(os.Environ(), "GIT_DIR="+filepath.Base(repo))
			return cmd
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			real := filepath.Join(t.TempDir(), "r.git")
			if err := os.MkdirAll(filepath.Join(real, "refs", "heads"), 0o700); err != nil {
				t.Fatal(err)
			}
			lock := filepath.Join(real, tc.lock)
			if err := os.MkdirAll(filepath.Dir(lock), 0o700); err != nil {
				t.Fatal(err)
			}
			repo := filepath.Join(t.TempDir(), "link.git") // the path hydration uses
			if err := os.Symlink(real, repo); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(lock, []byte("0000000000000000000000000000000000000000\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if !tc.mtime.IsZero() {
				if err := os.Chtimes(lock, tc.mtime, tc.mtime); err != nil {
					t.Fatal(err)
				}
			}
			if tc.process != nil {
				cmd := tc.process(repo, real)
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
			}
			if got := removeStaleRefLocks(repo, []string{"refs/heads/x"}); got != tc.removed {
				t.Fatalf("removed %d locks, want %d", got, tc.removed)
			}
			if _, err := os.Lstat(lock); (err == nil) != (tc.removed == 0) {
				t.Fatalf("lock exists = %v after removing %d", err == nil, tc.removed)
			}
		})
	}
}

// TestHydrateRemovesStaleRefLock (issue #464): receive-pack's process group
// is SIGKILLed while its ref transaction holds refs/heads/feat.lock, after
// the ref store committed feat. Every fetch of that repository on the node
// then failed. A young lock is still kept; once it is old, the next fetch
// removes it and serves feat.
func TestHydrateRemovesStaleRefLock(t *testing.T) {
	repo := registerRepo(t, "t-stale-lock-"+strconv.FormatInt(time.Now().UnixNano(), 36))
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	cache := t.TempDir()
	var stderr syncBuffer // serve writes it while the test reads
	node, err := startBinaryNode(binPath, cache, io.MultiWriter(os.Stderr, &stderr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.stop)
	url := remoteURL(node, repo, token)
	mustGit(t, "", "ls-remote", url) // creates the cache repo and its hooks
	// The reference-transaction hook is installed only if absent, so a
	// wrapper survives: it parks the first "prepared" call, which git
	// makes while it holds the transaction's ref locks.
	repoPath := RepoPath(cache, repo)
	hook := filepath.Join(repoPath, "hooks", "reference-transaction")
	if err := os.Rename(hook, hook+".managed"); err != nil {
		t.Fatal(err)
	}
	mark := filepath.Join(t.TempDir(), "prepared")
	wrapper := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = prepared ] && [ ! -e %q ]; then : > %q; sleep 60; fi\nexec /bin/sh %q \"$@\"\n", mark, mark, hook+".managed")
	if err := os.WriteFile(hook, []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := tryGit(work, "push", url, "HEAD:refs/heads/feat")
		done <- err
	}()
	waitForFile(t, mark, 30*time.Second)
	lock := filepath.Join(repoPath, "refs", "heads", "feat.lock")
	if _, err := os.Lstat(lock); err != nil {
		t.Fatalf("no ref lock while the transaction is prepared: %v", err)
	}
	killed := false
	for _, pid := range serveChildren(node) {
		if cmdline, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); bytes.HasPrefix(cmdline, []byte("git\x00receive-pack")) {
			pgid, err := syscall.Getpgid(pid)
			if err != nil || pgid == node.cmd.Process.Pid {
				t.Fatalf("receive-pack %d group %d: %v", pid, pgid, err)
			}
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			killed = true
		}
	}
	if !killed {
		t.Fatal("no receive-pack in serve's session")
	}
	if err := <-done; err == nil {
		t.Fatal("push succeeded although its receive-pack was killed")
	}
	if pgRefs(t, repo)["refs/heads/feat"] != head {
		t.Fatal("the ref store did not commit feat before the kill")
	}
	if _, err := os.Lstat(lock); err != nil {
		t.Fatalf("the kill left no ref lock: %v", err)
	}

	if out, err := tryGit("", "ls-remote", url); err == nil {
		t.Fatalf("ls-remote succeeded with a young lock in place (it must be kept):\n%s", out)
	}
	if err := os.Chtimes(lock, time.Now().Add(-2*staleRefLockAge), time.Now().Add(-2*staleRefLockAge)); err != nil {
		t.Fatal(err)
	}
	refs := mustGit(t, "", "ls-remote", url)
	if !strings.Contains(refs, head+"\trefs/heads/feat") {
		t.Fatalf("ls-remote after the stale lock aged = %q, want feat at %s", refs, head)
	}
	if _, err := os.Lstat(lock); err == nil {
		t.Fatal("stale lock still present")
	}
	if !strings.Contains(stderr.String(), "removed stale ref lock refs/heads/feat.lock") {
		t.Error("serve did not log the removal")
	}
}
