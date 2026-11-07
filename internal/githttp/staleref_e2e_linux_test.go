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

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/metrics"
)

// parkedPush is a push whose receive-pack is parked inside its ref
// transaction, after the ref store committed refs/heads/feat (issue #464).
// The parked git holds refs/heads/feat.lock, the lease and the hold of feat.
type parkedPush struct {
	repo, token string
	head        string
	cache       string
	repoPath    string
	lock        string
	stderr      *syncBuffer
	node        *testNode
	pushDone    chan error
	work        string
}

func parkPush(t *testing.T) *parkedPush {
	t.Helper()
	p := &parkedPush{
		repo:   registerRepo(t, "t-stale-lock-"+strconv.FormatInt(time.Now().UnixNano(), 36)),
		work:   initWorkRepo(t),
		cache:  t.TempDir(),
		stderr: &syncBuffer{}, // serve writes it while the test reads
	}
	p.token = mintTestToken(t, p.repo, "git:read,git:write", "")
	mustGit(t, p.work, "push", remoteURL(nodeA, p.repo, p.token), "HEAD:refs/heads/main")
	p.head = revParse(t, p.work, "HEAD")

	node, err := startBinaryNode(binPath, p.cache, io.MultiWriter(os.Stderr, p.stderr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.stop)
	p.node = node
	url := remoteURL(node, p.repo, p.token)
	mustGit(t, "", "ls-remote", url) // creates the cache repo and its hooks
	// The reference-transaction hook is installed only if absent, so a
	// wrapper survives: it parks the first "prepared" call, which git
	// makes while it holds the transaction's ref locks.
	p.repoPath = RepoPath(p.cache, p.repo)
	hook := filepath.Join(p.repoPath, "hooks", "reference-transaction")
	if err := os.Rename(hook, hook+".managed"); err != nil {
		t.Fatal(err)
	}
	mark := filepath.Join(t.TempDir(), "prepared")
	wrapper := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = prepared ] && [ ! -e %q ]; then : > %q; sleep 60; fi\nexec /bin/sh %q \"$@\"\n", mark, mark, hook+".managed")
	if err := os.WriteFile(hook, []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	p.pushDone = make(chan error, 1)
	go func() {
		_, err := tryGit(p.work, "push", url, "HEAD:refs/heads/feat")
		p.pushDone <- err
	}()
	waitForFile(t, mark, 30*time.Second)
	p.lock = filepath.Join(p.repoPath, "refs", "heads", "feat.lock")
	if _, err := os.Lstat(p.lock); err != nil {
		t.Fatalf("no ref lock while the transaction is prepared: %v", err)
	}
	return p
}

// killReceivePack SIGKILLs the process group of every receive-pack of the
// node: the killed git leaves its lock behind. It returns once the push has
// failed, as a push does whose receive-pack died.
func (p *parkedPush) killReceivePack(t *testing.T) {
	t.Helper()
	killed := false
	for _, pid := range serveChildren(p.node) {
		if cmdline, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); bytes.HasPrefix(cmdline, []byte("git\x00receive-pack")) {
			pgid, err := syscall.Getpgid(pid)
			if err != nil || pgid == p.node.cmd.Process.Pid {
				t.Fatalf("receive-pack %d group %d: %v", pid, pgid, err)
			}
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			killed = true
		}
	}
	if !killed {
		t.Fatal("no receive-pack in serve's session")
	}
	if err := <-p.pushDone; err == nil {
		t.Fatal("push succeeded although its receive-pack was killed")
	}
	if pgRefs(t, p.repo)["refs/heads/feat"] != p.head {
		t.Fatal("the ref store did not commit feat before the kill")
	}
	if _, err := os.Lstat(p.lock); err != nil {
		t.Fatalf("the kill left no ref lock: %v", err)
	}
}

// ageLock makes the lock two staleRefLockAge old, as one that a killed git
// left a while ago, and the lease epoch an hour old, as one that the first
// start of a lease-aware serve in this cache fixed long before. Without the
// second, the lock is older than the epoch and cleanup keeps it.
func (p *parkedPush) ageLock(t *testing.T, epochAge time.Duration) {
	t.Helper()
	old := time.Now().Add(-2 * staleRefLockAge)
	if err := os.Chtimes(p.lock, old, old); err != nil {
		t.Fatal(err)
	}
	epoch := time.Now().Add(-epochAge)
	if err := os.Chtimes(filepath.Join(p.repoPath, gitlimit.EpochFile), epoch, epoch); err != nil {
		t.Fatal(err)
	}
}

func (p *parkedPush) url() string { return remoteURL(p.node, p.repo, p.token) }

// TestHydrateRemovesAStaleRefLock (issues #464 and #490): receive-pack's
// process group is SIGKILLed while its ref transaction holds
// refs/heads/feat.lock, after the ref store committed feat. Once the lock is
// old and newer than the lease epoch, the next hydration finds that no git
// that serve started is alive, removes the lock, retries its sync, and the
// fetch of that repository works again with no operator.
func TestHydrateRemovesAStaleRefLock(t *testing.T) {
	p := parkPush(t)
	p.killReceivePack(t)
	p.ageLock(t, time.Hour)

	out, err := tryGit("", "ls-remote", p.url())
	if err != nil {
		t.Fatalf("ls-remote with an old lock that no git owns: %v\n%s", err, out)
	}
	if !strings.Contains(out, p.head+"\trefs/heads/feat") {
		t.Fatalf("ls-remote after the cleanup = %q, want feat at %s", out, p.head)
	}
	if _, err := os.Lstat(p.lock); err == nil {
		t.Fatal("the stale lock is still there")
	}
	if entries, _ := os.ReadDir(filepath.Join(p.repoPath, staleLocksDir)); len(entries) != 0 {
		t.Fatalf("the quarantine directory holds %d file(s) after the delete", len(entries))
	}
	if got := strings.Count(p.stderr.String(), p.repoPath+": removed 1 of 1 ref lock(s)"); got != 1 {
		t.Errorf("serve logged the removal %d times, want once:\n%s", got, p.stderr.String())
	}
	mustGit(t, "", "ls-remote", p.url())
}

// A lock that is not newer than the lease epoch may belong to a git that
// held no lease, so hydration keeps it and logs it. The runbook (stop the
// node, delete the repository's cache directory, start it) heals the node.
func TestHydrateKeepsAStaleRefLockOlderThanTheEpoch(t *testing.T) {
	p := parkPush(t)
	p.killReceivePack(t)
	// The epoch was fixed seconds ago, so the lock, however old it looks,
	// predates it.
	old := time.Now().Add(-2 * staleRefLockAge)
	if err := os.Chtimes(p.lock, old, old); err != nil {
		t.Fatal(err)
	}
	if out, err := tryGit("", "ls-remote", p.url()); err == nil {
		t.Fatalf("ls-remote succeeded with a lock older than the epoch:\n%s", out)
	}
	if _, err := os.Lstat(p.lock); err != nil {
		t.Fatalf("hydration removed a lock older than the epoch: %v", err)
	}
	if !strings.Contains(p.stderr.String(), p.repoPath+": removed 0 of 1 ref lock(s)") {
		t.Errorf("serve did not report the kept lock:\n%s", p.stderr.String())
	}

	p.node.stop()
	if err := os.RemoveAll(p.repoPath); err != nil {
		t.Fatal(err)
	}
	node, err := startBinaryNode(binPath, p.cache, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.stop)
	refs := mustGit(t, "", "ls-remote", remoteURL(node, p.repo, p.token))
	if !strings.Contains(refs, p.head+"\trefs/heads/feat") {
		t.Fatalf("ls-remote after the runbook = %q, want feat at %s", refs, p.head)
	}
}

// An orphan keeps the lease. serve is SIGKILLed while receive-pack is parked
// in its ref transaction, so receive-pack and its hook live on as orphans
// that hold the lock and the lease, and the descriptors of a real serve and a
// real git carry the lease to them. A cleanup keeps the lock and counts busy
// while an orphan lives, and removes the lock after the orphans are gone. A
// hydration cannot show this: the push hold of the parked ref keeps hydration
// away from that ref, so the lock never fails a batch.
func TestStaleRefLockCleanupKeepsALockWhileAnOrphanedGitLives(t *testing.T) {
	p := parkPush(t)
	orphans := serveChildren(p.node)
	if len(orphans) == 0 {
		t.Fatal("serve has no git children to orphan")
	}
	if err := syscall.Kill(p.node.cmd.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	// serve is not waited for: its stderr pipe stays open in the orphans.
	p.ageLock(t, time.Hour)
	names := []string{"refs/heads/feat"}
	busy := srCount("busy")

	res, err := cleanStaleRefLocks(p.repoPath, names, time.Now())
	if err != nil || res.Outcome != metrics.StaleRefLockBusy || res.Removed != 0 {
		t.Fatalf("cleanup with the orphans alive = %+v, %v, want busy and nothing removed", res, err)
	}
	if got := srCount("busy") - busy; got != 1 {
		t.Fatalf("busy outcomes counted = %v, want 1", got)
	}
	if _, err := os.Lstat(p.lock); err != nil {
		t.Fatalf("cleanup removed the lock of a live orphan: %v", err)
	}

	for _, pid := range orphans {
		if pgid, err := syscall.Getpgid(pid); err == nil && pgid != p.node.cmd.Process.Pid {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		res, err := cleanStaleRefLocks(p.repoPath, names, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if res.Outcome == metrics.StaleRefLockRemoved && res.Removed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cleanup after the orphans were killed = %+v, want the lock removed", res)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := os.Lstat(p.lock); err == nil {
		t.Fatal("the stale lock is still there after the orphans exited")
	}
}
