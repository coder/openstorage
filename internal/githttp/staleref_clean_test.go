package githttp

import (
	"bytes"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/metrics"
)

const srEmptyTree = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

// srGit runs git in dir with a hermetic environment and returns its combined
// output.
func srGit(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=a", "GIT_AUTHOR_EMAIL=a@example.com", "GIT_COMMITTER_NAME=a", "GIT_COMMITTER_EMAIL=a@example.com")
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// srRepo makes a bare repository whose lease epoch is 3 hours old, so a lock
// of 2 hours is newer than the epoch and older than staleRefLockAge. It
// returns the repository and the id of a commit in it.
func srRepo(t *testing.T) (repo, commit string) {
	t.Helper()
	repo = filepath.Join(t.TempDir(), "r.git")
	if out, err := srGit(t, "", "init", "-q", "--bare", repo); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	lease, err := gitlimit.OpenLease(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	srSetAge(t, filepath.Join(repo, gitlimit.EpochFile), 3*time.Hour)
	commit, err = srGit(t, repo, "commit-tree", srEmptyTree, "-m", "x")
	if err != nil {
		t.Fatalf("commit-tree: %v: %s", err, commit)
	}
	return repo, commit
}

func srSetAge(t *testing.T, path string, age time.Duration) {
	t.Helper()
	at := time.Now().Add(-age)
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}

// srLock writes a ref lock below repo that is age old.
func srLock(t *testing.T, repo, rel string, age time.Duration) string {
	t.Helper()
	p := filepath.Join(repo, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srSetAge(t, p, age)
	return p
}

func srCount(outcome string) float64 {
	return metrics.Value("openstorage_stale_ref_lock_cleanup_total", map[string]string{"outcome": outcome})
}

// srLogLines captures the log lines that name repo.
func srLogLines(t *testing.T, repo string) func() []string {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	return func() []string {
		var out []string
		for line := range strings.SplitSeq(buf.String(), "\n") {
			if strings.Contains(line, repo) {
				out = append(out, line)
			}
		}
		return out
	}
}

func srExists(t *testing.T, p string) bool {
	t.Helper()
	_, err := os.Lstat(p)
	return err == nil
}

func srQuarantine(t *testing.T, repo string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(repo, staleLocksDir))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// The #464 repair: a ref lock that a killed git left blocks git's own ref
// update, and a cleanup that holds the lease removes it, so the same update
// then works. The quarantine directory is not part of refs/.
func TestCleanStaleRefLocksRepairsABlockedRefUpdate(t *testing.T) {
	repo, commit := srRepo(t)
	lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	if out, err := srGit(t, repo, "update-ref", "refs/heads/x", commit); err == nil || !strings.Contains(out, "File exists") {
		t.Fatalf("update-ref with a stale lock = %v: %s, want a File exists failure", err, out)
	}
	lines := srLogLines(t, repo)
	removed := srCount("removed")

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Old != 1 || res.Removed != 1 || res.Outcome != metrics.StaleRefLockRemoved {
		t.Fatalf("result = %+v, want 1 old, 1 removed, outcome removed", res)
	}
	if srExists(t, lock) {
		t.Fatal("the stale lock is still there")
	}
	if left := srQuarantine(t, repo); len(left) != 0 {
		t.Fatalf("quarantine holds %v after the delete", left)
	}
	if out, err := srGit(t, repo, "update-ref", "refs/heads/x", commit); err != nil {
		t.Fatalf("update-ref after the cleanup: %v: %s", err, out)
	}
	if out, _ := srGit(t, repo, "for-each-ref", "--format=%(refname)"); out != "refs/heads/x" {
		t.Fatalf("for-each-ref = %q, want only refs/heads/x", out)
	}
	if got := srCount("removed") - removed; got != 1 {
		t.Fatalf("removed outcomes counted = %v, want 1", got)
	}
	if l := lines(); len(l) != 1 || !strings.Contains(l[0], "removed 1 of 1 ref lock(s)") {
		t.Fatalf("log lines = %q, want one saying removed 1 of 1", l)
	}
}

// Every lock that can block a batch for the names is covered: the ref's own
// lock, one below it, and packed-refs.lock.
func TestCleanStaleRefLocksCoversEveryLockOfABatch(t *testing.T) {
	repo, _ := srRepo(t)
	locks := []string{
		srLock(t, repo, "refs/heads/x.lock", 2*time.Hour),
		srLock(t, repo, "refs/heads/x/y.lock", 2*time.Hour),
		srLock(t, repo, "packed-refs.lock", 2*time.Hour),
	}
	other := srLock(t, repo, "refs/heads/other.lock", 2*time.Hour)

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x", "../escape"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 3 || res.Outcome != metrics.StaleRefLockRemoved {
		t.Fatalf("result = %+v, want 3 removed", res)
	}
	for _, l := range locks {
		if srExists(t, l) {
			t.Errorf("%s was not removed", l)
		}
	}
	if !srExists(t, other) {
		t.Error("the lock of a ref outside the batch was removed")
	}
}

// A live git that serve started keeps the lease, so cleanup keeps every lock
// and counts busy. It removes them once that git has gone.
func TestCleanStaleRefLocksKeepsEveryLockWhileAGitIsAlive(t *testing.T) {
	repo, _ := srRepo(t)
	lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	live := exec.Command("sleep", "60")
	if err := gitlimit.StartIn(t.Context(), repo, live); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Process.Kill(); _ = gitlimit.Wait(live) })
	lines := srLogLines(t, repo)
	busy, removed := srCount("busy"), srCount("removed")

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Old != 1 || res.Removed != 0 || res.Outcome != metrics.StaleRefLockBusy {
		t.Fatalf("result = %+v, want 1 old, 0 removed, outcome busy", res)
	}
	if !srExists(t, lock) {
		t.Fatal("the lock was removed under a live git")
	}
	if got := srCount("busy") - busy; got != 1 {
		t.Fatalf("busy outcomes counted = %v, want 1", got)
	}
	if l := lines(); len(l) != 1 || !strings.Contains(l[0], "still running") {
		t.Fatalf("log lines = %q, want one saying a git is still running", l)
	}

	_ = live.Process.Kill()
	_ = gitlimit.Wait(live)
	res, err = cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err != nil || res.Removed != 1 {
		t.Fatalf("after the git exited: %+v, %v, want 1 removed", res, err)
	}
	if got := srCount("removed") - removed; got != 1 {
		t.Fatalf("removed outcomes counted = %v, want 1", got)
	}
}

// A lock that is not older than staleRefLockAge is a live git's, so a run
// that finds only young locks does nothing: it takes no lease, logs nothing
// and counts nothing. It cannot disturb a git that runs.
func TestCleanStaleRefLocksIgnoresYoungLocksWithoutTheLease(t *testing.T) {
	repo, _ := srRepo(t)
	lock := srLock(t, repo, "refs/heads/x.lock", 10*time.Second)
	held, err := gitlimit.OpenLease(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	lines := srLogLines(t, repo)
	before := srCount("busy") + srCount("kept") + srCount("removed")

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res != (staleRefCleanup{}) {
		t.Fatalf("result = %+v, want nothing done", res)
	}
	if !srExists(t, lock) {
		t.Fatal("a young lock was removed")
	}
	if after := srCount("busy") + srCount("kept") + srCount("removed"); after != before {
		t.Fatalf("a run that did nothing counted an outcome: %v -> %v", before, after)
	}
	if l := lines(); len(l) != 0 {
		t.Fatalf("log lines = %q, want none", l)
	}
}

// A lock that is not newer than the lease epoch may belong to a git that
// held no lease: it stays for the manual runbook. The same holds when the
// repository has no epoch yet, because the cleanup then creates it.
func TestCleanStaleRefLocksKeepsLocksOlderThanTheEpoch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, repo string)
	}{
		{"lock older than the epoch", func(t *testing.T, repo string) {
			srSetAge(t, filepath.Join(repo, gitlimit.EpochFile), time.Hour)
		}},
		{"no epoch yet", func(t *testing.T, repo string) {
			if err := os.Remove(filepath.Join(repo, gitlimit.EpochFile)); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, _ := srRepo(t)
			lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
			tc.setup(t, repo)
			kept := srCount("kept")

			res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if res.Old != 1 || res.Removed != 0 || res.Outcome != metrics.StaleRefLockKept {
				t.Fatalf("result = %+v, want 1 old, 0 removed, outcome kept", res)
			}
			if !srExists(t, lock) {
				t.Fatal("a lock older than the epoch was removed")
			}
			if got := srCount("kept") - kept; got != 1 {
				t.Fatalf("kept outcomes counted = %v, want 1", got)
			}
		})
	}
}

// Only a regular file is a lock that git made: a symlink or a directory at
// the lock path is left alone, and so is what a symlink points to.
func TestCleanStaleRefLocksLeavesWhatIsNotARegularFile(t *testing.T) {
	repo, _ := srRepo(t)
	target := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.WriteFile(target, []byte("keep\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(repo, "refs", "heads"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(repo, "refs", "heads", "x.lock")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(repo, "refs", "heads", "y.lock")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	srSetAge(t, target, 2*time.Hour)

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x", "refs/heads/y"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res != (staleRefCleanup{}) {
		t.Fatalf("result = %+v, want nothing done", res)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "keep\n" {
		t.Fatalf("symlink target = %q, %v", got, err)
	}
	if !srExists(t, link) || !srExists(t, dir) {
		t.Fatal("the symlink or the directory was removed")
	}
}

// A run logs one line and counts one outcome, however many locks it removes.
func TestCleanStaleRefLocksLogsOneLinePerRun(t *testing.T) {
	repo, _ := srRepo(t)
	for i := range 200 {
		srLock(t, repo, "refs/heads/many/"+strings.Repeat("a", i%7+1)+"-"+strings.Repeat("b", i)+".lock", 2*time.Hour)
	}
	lines := srLogLines(t, repo)
	removed := srCount("removed")

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/many"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 200 {
		t.Fatalf("removed %d locks, want 200", res.Removed)
	}
	if l := lines(); len(l) != 1 {
		t.Fatalf("%d log lines, want 1", len(l))
	}
	if got := srCount("removed") - removed; got != 1 {
		t.Fatalf("removed outcomes counted = %v, want 1 for the run", got)
	}
}

// The identity check. A writer that is out of contract (a git without a
// lease) replaces the lock between cleanup's look and its move. The moved
// file is not the one that was inspected, so cleanup puts it back and keeps
// it. Skipping the check deletes a live writer's lock.
func TestCleanStaleRefLocksPutsBackALockThatWasReplaced(t *testing.T) {
	repo, _ := srRepo(t)
	lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	replace := func(string) {
		fresh := lock + ".new"
		if err := os.WriteFile(fresh, []byte("live writer\n"), 0o600); err != nil {
			t.Error(err)
		}
		if err := os.Rename(fresh, lock); err != nil {
			t.Error(err)
		}
	}
	staleLockBeforeMove.Store(&replace)
	t.Cleanup(func() { staleLockBeforeMove.Store(nil) })
	lines := srLogLines(t, repo)

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 0 || res.Outcome != metrics.StaleRefLockKept {
		t.Fatalf("result = %+v, want 0 removed, outcome kept", res)
	}
	if got, err := os.ReadFile(lock); err != nil || string(got) != "live writer\n" {
		t.Fatalf("lock after the run = %q, %v, want the live writer's file back in place", got, err)
	}
	if left := srQuarantine(t, repo); len(left) != 0 {
		t.Fatalf("quarantine holds %v after the put-back", left)
	}
	if l := lines(); len(l) != 1 || !strings.Contains(l[0], "1 replaced while moved") {
		t.Fatalf("log lines = %q, want one naming the replaced lock", l)
	}
}

// When the replaced file cannot go back because a new lock took its path,
// that new lock stays untouched and the moved file stays in quarantine.
func TestCleanStaleRefLocksNeverReplacesANewLockWhenPuttingBack(t *testing.T) {
	repo, _ := srRepo(t)
	lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	swap := func(string) {
		fresh := lock + ".new"
		if err := os.WriteFile(fresh, []byte("first writer\n"), 0o600); err != nil {
			t.Error(err)
		}
		if err := os.Rename(fresh, lock); err != nil {
			t.Error(err)
		}
	}
	newer := func(string) {
		if err := os.WriteFile(lock, []byte("second writer\n"), 0o600); err != nil {
			t.Error(err)
		}
	}
	staleLockBeforeMove.Store(&swap)
	staleLockAfterMove.Store(&newer)
	t.Cleanup(func() { staleLockBeforeMove.Store(nil); staleLockAfterMove.Store(nil) })

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 0 {
		t.Fatalf("result = %+v, want 0 removed", res)
	}
	if got, _ := os.ReadFile(lock); string(got) != "second writer\n" {
		t.Fatalf("lock = %q, want the second writer's file untouched", got)
	}
	left := srQuarantine(t, repo)
	if len(left) != 1 {
		t.Fatalf("quarantine holds %v, want the first writer's moved file", left)
	}
	if got, _ := os.ReadFile(filepath.Join(repo, staleLocksDir, left[0])); string(got) != "first writer\n" {
		t.Fatalf("quarantined file = %q, want the first writer's", got)
	}
	// A file in the quarantine directory is not a ref.
	if out, err := srGit(t, repo, "for-each-ref", "--format=%(refname)"); err != nil || out != "" {
		t.Fatalf("for-each-ref = %q, %v, want no refs", out, err)
	}
}

// A run that fails still counts one outcome and logs one line, so that
// monitoring sees an attempt that left the repository blocked.
func TestCleanStaleRefLocksCountsAFailedRunAsKept(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(t *testing.T, repo string)
		want   string
	}{
		{"the quarantine directory cannot be made", func(t *testing.T, repo string) {
			if err := os.WriteFile(filepath.Join(repo, staleLocksDir), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}, "move a ref lock"},
		{"the epoch is not a regular file", func(t *testing.T, repo string) {
			p := filepath.Join(repo, gitlimit.EpochFile)
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("/nonexistent", p); err != nil {
				t.Fatal(err)
			}
		}, "not a regular file"},
		{"the lease cannot be opened", func(t *testing.T, repo string) {
			p := filepath.Join(repo, gitlimit.LeaseFile)
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(p, 0o700); err != nil {
				t.Fatal(err)
			}
		}, "take the git lease"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, _ := srRepo(t)
			lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
			tc.break_(t, repo)
			lines := srLogLines(t, repo)
			kept := srCount("kept")

			res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("cleanup = %+v, %v, want an error that names %q", res, err, tc.want)
			}
			if res.Old != 1 || res.Removed != 0 || res.Outcome != metrics.StaleRefLockKept {
				t.Fatalf("result = %+v, want 1 old, 0 removed, outcome kept", res)
			}
			if !srExists(t, lock) {
				t.Fatal("a failed run removed the lock")
			}
			if got := srCount("kept") - kept; got != 1 {
				t.Fatalf("kept outcomes counted = %v, want 1", got)
			}
			if l := lines(); len(l) != 1 || !strings.Contains(l[0], tc.want) {
				t.Fatalf("log lines = %q, want one that names %q", l, tc.want)
			}
		})
	}
}

// A replaced file that cannot go back to its path for a reason other than a
// new lock is an error, not a successful put-back: the file may be a live
// writer's lock, and now it is only in quarantine. The run still counts one
// outcome, and its log line says how far it got.
func TestCleanStaleRefLocksReportsAReplacedLockThatCannotBePutBack(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	repo, _ := srRepo(t)
	lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	other := srLock(t, repo, "refs/heads/x/y.lock", 2*time.Hour)
	dir := filepath.Dir(lock)
	swap := func(path string) {
		if path != lock {
			return
		}
		fresh := lock + ".new"
		if err := os.WriteFile(fresh, []byte("live writer\n"), 0o600); err != nil {
			t.Error(err)
		}
		if err := os.Rename(fresh, lock); err != nil {
			t.Error(err)
		}
	}
	lockDown := func(path string) {
		if path != lock {
			return
		}
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Error(err)
		}
	}
	staleLockBeforeMove.Store(&swap)
	staleLockAfterMove.Store(&lockDown)
	t.Cleanup(func() {
		staleLockBeforeMove.Store(nil)
		staleLockAfterMove.Store(nil)
		_ = os.Chmod(dir, 0o700)
	})
	lines := srLogLines(t, repo)
	kept := srCount("kept")

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "put back") {
		t.Fatalf("cleanup = %+v, %v, want an error that names the put-back", res, err)
	}
	if res.Outcome != metrics.StaleRefLockKept {
		t.Fatalf("result = %+v, want outcome kept", res)
	}
	if got := srCount("kept") - kept; got != 1 {
		t.Fatalf("kept outcomes counted = %v, want 1", got)
	}
	if left := srQuarantine(t, repo); len(left) != 1 {
		t.Fatalf("quarantine holds %v, want the replaced file", left)
	}
	if l := lines(); len(l) != 1 || !strings.Contains(l[0], "then stopped") || !strings.Contains(l[0], "put back") {
		t.Fatalf("log lines = %q, want one that says the run stopped at the put-back", l)
	}
	_ = other
}

// A run that removed some locks before it failed says so in its log line and
// counts removed, so the line and the count agree.
func TestCleanStaleRefLocksLogsPartialRemovalWhenALaterMoveFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	repo, _ := srRepo(t)
	srLock(t, repo, "refs/heads/a.lock", 2*time.Hour)
	srLock(t, repo, "refs/heads/m/b.lock", 2*time.Hour)
	dir := filepath.Join(repo, "refs", "heads", "m")
	// refLockPaths lists a.lock first, then the locks below m: m's
	// directory is read-only, so moving its lock out fails.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	lines := srLogLines(t, repo)
	removed := srCount("removed")

	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/a", "refs/heads/m"}, time.Now())
	if err == nil {
		t.Fatalf("cleanup = %+v, want the move out of the read-only directory to fail", res)
	}
	if res.Removed != 1 || res.Outcome != metrics.StaleRefLockRemoved {
		t.Fatalf("result = %+v, want 1 removed and outcome removed", res)
	}
	if got := srCount("removed") - removed; got != 1 {
		t.Fatalf("removed outcomes counted = %v, want 1", got)
	}
	if l := lines(); len(l) != 1 || !strings.Contains(l[0], "removed 1 of 2 ref lock(s)") {
		t.Fatalf("log lines = %q, want one that says removed 1 of 2", l)
	}
}
