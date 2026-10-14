//go:build linux

package githttp

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestHydrateReportsStaleRefLock (issue #464): receive-pack's process group
// is SIGKILLed while its ref transaction holds refs/heads/feat.lock, after
// the ref store committed feat. Every fetch of that repository on the node
// then fails. Once the lock is old, hydration names it in the log and
// keeps it; the runbook (stop the node, remove the lock, start it) heals
// the node.
func TestHydrateReportsStaleRefLock(t *testing.T) {
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

	if err := os.Chtimes(lock, time.Now().Add(-2*staleRefLockAge), time.Now().Add(-2*staleRefLockAge)); err != nil {
		t.Fatal(err)
	}
	if out, err := tryGit("", "ls-remote", url); err == nil {
		t.Fatalf("ls-remote succeeded with the stale lock in place:\n%s", out)
	}
	if _, err := os.Lstat(lock); err != nil {
		t.Fatalf("hydration removed the lock: %v", err)
	}
	if !strings.Contains(stderr.String(), "ref lock refs/heads/feat.lock is ") {
		t.Error("serve did not report the stale lock")
	}

	// The runbook: stop the node, remove the lock, start the node.
	node.stop()
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	node, err = startBinaryNode(binPath, cache, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.stop)
	refs := mustGit(t, "", "ls-remote", remoteURL(node, repo, token))
	if !strings.Contains(refs, head+"\trefs/heads/feat") {
		t.Fatalf("ls-remote after the runbook = %q, want feat at %s", refs, head)
	}
}
