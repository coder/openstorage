package githttp

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
)

// srSync runs the sync that hydration runs, with a small batch so that the
// updates overflow into the second pass.
func srSync(t *testing.T, repo string, want map[string]string, batch int) (skipped bool, refsErr error) {
	t.Helper()
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	newStore := func() refSeq { return &sliceRefSeq{names: names, shas: want} }
	skipped, refsErr, scanErr := syncRefsWithCleanup(t.Context(), repo, "refs/heads/main", newStore, batch)
	if scanErr != nil {
		t.Fatalf("scan error: %v", scanErr)
	}
	return skipped, refsErr
}

// A batch that a ref lock fails names its refs, so that hydration can find
// the lock, and keeps git's error text.
func TestRefCmdBatchFlushNamesTheBatchWhenALockFailsIt(t *testing.T) {
	repo, commit := srRepo(t)
	srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	b := &refCmdBatch{repoPath: repo, cap: 10}
	if err := b.add("refs/heads/x", "update refs/heads/x "+commit+"\n"); err != nil {
		t.Fatal(err)
	}
	err := b.flush()
	failed, ok := errors.AsType[*refBatchError](err)
	if !ok || len(failed.names) != 1 || failed.names[0] != "refs/heads/x" {
		t.Fatalf("flush = %v, want a refBatchError that names refs/heads/x", err)
	}
	if !strings.Contains(err.Error(), "File exists") {
		t.Fatalf("flush error %q lost git's text", err)
	}
}

// Rows 3 and 8 of the #490 design. A stale lock fails the update pass or the
// deletion pass, also when the updates overflow into a second pass. The sync
// removes the lock once and retries, and ends in the store's state.
func TestSyncRefsWithCleanupRepairsAStaleLockInEveryPass(t *testing.T) {
	for _, tc := range []struct {
		name string
		lock string
		// local refs that exist before the sync, besides main
		local []string
		want  []string // refs the store holds, besides main
	}{
		{"updates in the first pass", "refs/heads/b.lock", nil, []string{"refs/heads/b"}},
		{"updates in the second pass", "refs/heads/e.lock", nil, []string{"refs/heads/a", "refs/heads/b", "refs/heads/c", "refs/heads/d", "refs/heads/e"}},
		{"a deletion", "refs/heads/old.lock", []string{"refs/heads/old"}, nil},
		{"a lock below the ref", "refs/heads/new/deep.lock", nil, []string{"refs/heads/new/deep"}},
		{"packed-refs", "packed-refs.lock", []string{"refs/heads/old"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, commit := srRepo(t)
			for _, name := range append([]string{"refs/heads/main"}, tc.local...) {
				if out, err := srGit(t, repo, "update-ref", name, commit); err != nil {
					t.Fatalf("seed %s: %v: %s", name, err, out)
				}
			}
			lock := srLock(t, repo, tc.lock, 2*time.Hour)
			want := map[string]string{"refs/heads/main": commit}
			for _, name := range tc.want {
				want[name] = commit
			}
			lines := srLogLines(t, repo)
			removed := srCount("removed")

			skipped, err := srSync(t, repo, want, 1)
			if err != nil || skipped {
				t.Fatalf("sync = skipped %v, %v, want a clean sync after the cleanup", skipped, err)
			}
			if srExists(t, lock) {
				t.Fatal("the stale lock is still there")
			}
			got := localRefs(t, repo)
			if len(got) != len(want) {
				t.Fatalf("local refs = %v, want %v", got, want)
			}
			for name := range want {
				if got[name] != commit {
					t.Fatalf("local refs = %v, want %v", got, want)
				}
			}
			if got := srCount("removed") - removed; got != 1 {
				t.Fatalf("removed outcomes counted = %v, want 1", got)
			}
			if l := lines(); len(l) != 1 || !strings.Contains(l[0], "removed 1 of 1") {
				t.Fatalf("log lines = %q, want one saying removed 1 of 1", l)
			}
		})
	}
}

// While a git that serve started is alive in the repository, the sync keeps
// the lock and fails as it did before the cleanup existed. The cleanup runs
// after each of the three failed attempts, and each run counts busy. When
// that git has gone, the next sync heals.
func TestSyncRefsWithCleanupKeepsALockWhileAGitIsAlive(t *testing.T) {
	repo, commit := srRepo(t)
	lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	live := exec.Command("sleep", "60")
	if err := gitlimit.StartIn(t.Context(), repo, live); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Process.Kill(); _ = gitlimit.Wait(live) })
	lines := srLogLines(t, repo)
	busy := srCount("busy")
	want := map[string]string{"refs/heads/x": commit}

	if _, err := srSync(t, repo, want, 10); err == nil || !strings.Contains(err.Error(), "File exists") {
		t.Fatalf("sync under a live git = %v, want git's File exists", err)
	}
	if !srExists(t, lock) {
		t.Fatal("the lock was removed under a live git")
	}
	if got := srCount("busy") - busy; got != 3 {
		t.Fatalf("busy outcomes counted = %v, want 3, one per failed attempt", got)
	}
	if l := lines(); len(l) != 3 || !strings.Contains(l[0], "still running") {
		t.Fatalf("log lines = %q, want three saying a git is still running", l)
	}

	_ = live.Process.Kill()
	_ = gitlimit.Wait(live)
	if _, err := srSync(t, repo, want, 10); err != nil {
		t.Fatalf("sync after the git exited: %v", err)
	}
	if srExists(t, lock) {
		t.Fatal("the lock is still there after the git exited")
	}
}

// A lock that is not old, or not newer than the lease epoch, stays, and the
// sync fails as before. A young lock does not even take the lease.
func TestSyncRefsWithCleanupLeavesLocksItMustNotRemove(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lockAge   time.Duration
		epochAge  time.Duration
		wantLines int
	}{
		{"young lock", 10 * time.Second, 3 * time.Hour, 0},
		{"lock older than the epoch", 2 * time.Hour, time.Hour, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, commit := srRepo(t)
			srSetAge(t, filepath.Join(repo, gitlimit.EpochFile), tc.epochAge)
			lock := srLock(t, repo, "refs/heads/x.lock", tc.lockAge)
			lines := srLogLines(t, repo)
			before := srCount("busy") + srCount("kept") + srCount("removed")

			if _, err := srSync(t, repo, map[string]string{"refs/heads/x": commit}, 10); err == nil {
				t.Fatal("sync succeeded with the lock in place")
			}
			if !srExists(t, lock) {
				t.Fatal("the lock was removed")
			}
			if l := lines(); len(l) != tc.wantLines {
				t.Fatalf("log lines = %q, want %d", l, tc.wantLines)
			}
			wantCount := before + float64(tc.wantLines)
			if after := srCount("busy") + srCount("kept") + srCount("removed"); after != wantCount {
				t.Fatalf("outcomes counted: %v -> %v, want %v", before, after, wantCount)
			}
		})
	}
}

// The lock files that serve's hydration cleanup finds come from git's own
// refusal: a stale lock of a ref that no batch names stays.
func TestSyncRefsWithCleanupLeavesTheLockOfAnotherRef(t *testing.T) {
	repo, commit := srRepo(t)
	other := srLock(t, repo, "refs/heads/other.lock", 2*time.Hour)
	if _, err := srSync(t, repo, map[string]string{"refs/heads/main": commit}, 10); err != nil {
		t.Fatal(err)
	}
	if !srExists(t, other) {
		t.Fatal("the lock of a ref outside the sync was removed")
	}
	if _, err := os.Stat(filepath.Join(repo, staleLocksDir)); err == nil {
		t.Fatal("a sync with no failed batch made the quarantine directory")
	}
}

// One killed transaction can leave locks in several batches. The cleanup runs
// after each failed attempt, so one hydration cleans the failed batch of each
// of its three attempts and then fails, and the next hydration finishes.
func TestSyncRefsWithCleanupCleansTheFailedBatchOfEveryAttempt(t *testing.T) {
	repo, commit := srRepo(t)
	want := map[string]string{"refs/heads/main": commit}
	var locks []string
	for _, name := range []string{"a", "b", "c", "d"} {
		want["refs/heads/"+name] = commit
		locks = append(locks, srLock(t, repo, "refs/heads/"+name+".lock", 2*time.Hour))
	}
	lines := srLogLines(t, repo)
	removed := srCount("removed")

	if _, err := srSync(t, repo, want, 1); err == nil {
		t.Fatal("sync succeeded although a fourth batch is locked and a hydration has three attempts")
	}
	left := 0
	for _, l := range locks {
		if srExists(t, l) {
			left++
		}
	}
	if left != 1 {
		t.Fatalf("%d locks are left after one hydration, want 1: three attempts clean three batches", left)
	}
	if got := srCount("removed") - removed; got != 3 {
		t.Fatalf("removed outcomes counted = %v, want 3 (one per failed attempt)", got)
	}
	if l := lines(); len(l) != 3 {
		t.Fatalf("%d log lines, want 3: %q", len(l), l)
	}

	if skipped, err := srSync(t, repo, want, 1); err != nil || skipped {
		t.Fatalf("the next sync = skipped %v, %v, want a clean sync", skipped, err)
	}
	for _, l := range locks {
		if srExists(t, l) {
			t.Errorf("%s is still there after the second hydration", l)
		}
	}
}

// A lock that blocks one batch for a moment (here a young lock that goes
// before the next attempt) does not stop the cleanup of the batches after it:
// the next failed batch has a stale lock, and the hydration cleans it.
func TestSyncRefsWithCleanupCleansADifferentBatchAfterAFruitlessOne(t *testing.T) {
	repo, commit := srRepo(t)
	young := srLock(t, repo, "refs/heads/a.lock", 10*time.Second)
	stale := srLock(t, repo, "refs/heads/b.lock", 2*time.Hour)
	want := map[string]string{"refs/heads/main": commit, "refs/heads/a": commit, "refs/heads/b": commit}
	names := []string{"refs/heads/a", "refs/heads/b", "refs/heads/main"}
	stores := 0
	newStore := func() refSeq {
		stores++
		// An attempt reads the store once for each of its two passes (the
		// updates overflow the batch), so the second attempt starts at the
		// third read.
		if stores == 3 {
			if err := os.Remove(young); err != nil {
				t.Error(err)
			}
		}
		return &sliceRefSeq{names: names, shas: want}
	}
	lines := srLogLines(t, repo)

	skipped, refsErr, scanErr := syncRefsWithCleanup(t.Context(), repo, "refs/heads/main", newStore, 1)
	if scanErr != nil || refsErr != nil || skipped {
		t.Fatalf("sync = skipped %v, %v, %v, want a clean sync", skipped, refsErr, scanErr)
	}
	if srExists(t, stale) {
		t.Fatal("the stale lock of the later batch is still there")
	}
	if l := lines(); len(l) != 1 {
		t.Fatalf("log lines = %q, want one: the young lock is no cleanup run", l)
	}
}
