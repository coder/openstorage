package githttp

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/metrics"
)

// The lease epoch (gitlimit.EpochFile) marks a repository that a lease-aware
// serve created. EnsureCacheRepoCtx writes it when its own os.Mkdir makes the
// directory, before git runs there, and never in a directory that already
// existed: an older release can have a git alive in such a directory that
// holds no lease, and a late epoch would make its locks look newer than the
// epoch (#490).
func TestEnsureCacheRepoWritesTheLeaseEpochOnlyWhenItCreatesTheDirectory(t *testing.T) {
	t.Run("a new repository", func(t *testing.T) {
		cacheDir := t.TempDir()
		before := time.Now().Add(-time.Second)
		repo, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-new")
		if err != nil {
			t.Fatal(err)
		}
		epoch, err := gitlimit.LeaseEpoch(repo)
		if err != nil {
			t.Fatalf("LeaseEpoch of a repository that serve created = %v", err)
		}
		if epoch.Before(before) || epoch.After(time.Now().Add(time.Second)) {
			t.Fatalf("epoch = %v, want the moment of creation", epoch)
		}
	})

	t.Run("the epoch is not written again", func(t *testing.T) {
		cacheDir := t.TempDir()
		repo, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-twice")
		if err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-3 * time.Hour)
		if err := os.Chtimes(filepath.Join(repo, gitlimit.EpochFile), old, old); err != nil {
			t.Fatal(err)
		}
		cacheRepoMemos.Delete(repo) // take the full pass, as a restarted serve does
		if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-twice"); err != nil {
			t.Fatal(err)
		}
		if got, err := gitlimit.LeaseEpoch(repo); err != nil || got.Sub(old).Abs() > time.Second {
			t.Fatalf("epoch after a second pass = %v, %v, want %v", got, err, old)
		}
	})

	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, repo string)
	}{
		{"a repository that an older release created", func(t *testing.T, repo string) {
			if out, err := srGit(t, "", "init", "-q", "--bare", repo); err != nil {
				t.Fatalf("git init: %v: %s", err, out)
			}
		}},
		{"a directory that an interrupted creation left", func(t *testing.T, repo string) {
			if err := os.MkdirAll(repo, 0o755); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cacheDir := t.TempDir()
			repo := RepoPath(cacheDir, "t-epoch-existing")
			tc.setup(t, repo)
			lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
			if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-existing"); err != nil {
				t.Fatal(err)
			}
			if _, err := gitlimit.LeaseEpoch(repo); err == nil {
				t.Fatal("a directory that already existed got a lease epoch")
			}

			res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if !res.NoEpoch || res.Removed != 0 || res.Outcome != metrics.StaleRefLockKept {
				t.Fatalf("result = %+v, want no epoch, 0 removed, outcome kept", res)
			}
			if !srExists(t, lock) {
				t.Fatal("cleanup removed a lock of a repository without an epoch")
			}
			if _, err := gitlimit.LeaseEpoch(repo); err == nil {
				t.Fatal("cleanup wrote a lease epoch")
			}
		})
	}
}
