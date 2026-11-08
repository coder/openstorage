package githttp

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/metrics"
)

// The lease epoch (gitlimit.EpochFile) marks a repository that a lease-aware
// serve created. EnsureCacheRepoCtx writes it for a directory that has no HEAD
// (no git has held a ref lock there), before git init, and never in a
// repository that already has HEAD: an older release can have a git alive in
// such a repository that holds no lease, and a late epoch would make its locks
// look newer than the epoch (#490). The test for a directory that an
// interrupted creation left is
// TestEnsureCacheRepoWritesTheEpochForADirectoryWithoutHEAD.
func TestEnsureCacheRepoWritesTheLeaseEpochOnlyBeforeHEADExists(t *testing.T) {
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
		{"an older release's repository with HEAD but without objects", func(t *testing.T, repo string) {
			if err := os.MkdirAll(repo, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(repo, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
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

// The epoch is written before any git runs in the new repository, so that no
// git of this release ever runs in a directory that it created without one.
// A git wrapper on PATH records whether the epoch file exists at each start.
func TestEnsureCacheRepoWritesTheEpochBeforeGitRuns(t *testing.T) {
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Skip("no git")
	}
	cacheDir := t.TempDir()
	epoch := filepath.Join(RepoPath(cacheDir, "t-epoch-order"), gitlimit.EpochFile)
	probes := filepath.Join(t.TempDir(), "probes")
	bin := t.TempDir()
	script := fmt.Sprintf("#!/bin/sh\nif [ -e %q ]; then echo present >> %q; else echo absent >> %q; fi\nexec %q \"$@\"\n", epoch, probes, probes, realGit)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-order"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(probes)
	if err != nil {
		t.Fatalf("the wrapper saw no git start: %v", err)
	}
	for i, line := range strings.Fields(string(got)) {
		if line != "present" {
			t.Fatalf("git start %d of the new repository ran without the epoch file: %q", i+1, got)
		}
	}
}

// A cache directory that does not exist yet is made, as before the epoch:
// the repository's parent is made with MkdirAll.
func TestEnsureCacheRepoMakesAMissingCacheDirectory(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "missing", "cache")
	repo, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-parent")
	if err != nil {
		t.Fatalf("EnsureCacheRepoCtx in a cache directory that does not exist = %v", err)
	}
	if _, err := gitlimit.LeaseEpoch(repo); err != nil {
		t.Fatalf("the new repository has no epoch: %v", err)
	}
}

// A creation that stopped right after the os.Mkdir, or before git init, leaves
// a directory with no HEAD. No git has held a ref lock in a directory that is
// no repository, so the next call writes the epoch (under the hydrate lock)
// before git init, and the cleanup then treats the repository like one that
// serve created.
func TestEnsureCacheRepoWritesTheEpochForADirectoryWithoutHEAD(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, repo string)
	}{
		{"an empty directory", func(t *testing.T, repo string) {
			if err := os.MkdirAll(repo, 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{"a directory with objects and refs but no HEAD", func(t *testing.T, repo string) {
			for _, d := range []string{"objects", "refs"} {
				if err := os.MkdirAll(filepath.Join(repo, d), 0o755); err != nil {
					t.Fatal(err)
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cacheDir := t.TempDir()
			repo := RepoPath(cacheDir, "t-epoch-nohead")
			tc.setup(t, repo)
			before := time.Now().Add(-time.Second)
			if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-nohead"); err != nil {
				t.Fatal(err)
			}
			epoch, err := gitlimit.LeaseEpoch(repo)
			if err != nil {
				t.Fatalf("LeaseEpoch of a directory without HEAD that serve finished = %v", err)
			}
			if epoch.Before(before) || epoch.After(time.Now().Add(time.Second)) {
				t.Fatalf("epoch = %v, want the moment of this call", epoch)
			}

			srSetAge(t, filepath.Join(repo, gitlimit.EpochFile), 3*time.Hour)
			lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
			res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
			if err != nil || res.Removed != 1 || srExists(t, lock) {
				t.Fatalf("cleanup = %+v, %v (lock left: %v), want the lock newer than the epoch removed", res, err, srExists(t, lock))
			}
		})
	}
}

// A racing call can have written the epoch first (CreateEpoch opens the file
// with O_EXCL), so ErrExist from the epoch write is no failure.
func TestEnsureCacheRepoToleratesAnEpochThatAnotherCallWrote(t *testing.T) {
	cacheDir := t.TempDir()
	calls := 0
	createEpoch = func(dir string) error {
		calls++
		if err := gitlimit.CreateEpoch(dir); err != nil {
			return err
		}
		return gitlimit.CreateEpoch(dir) // fails with ErrExist, as when a second call wrote first
	}
	t.Cleanup(func() { createEpoch = gitlimit.CreateEpoch })

	repo, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-exist")
	if err != nil {
		t.Fatalf("EnsureCacheRepoCtx = %v, want the ErrExist of the epoch write tolerated", err)
	}
	if calls != 1 {
		t.Fatalf("the epoch was written %d times, want 1", calls)
	}
	if _, err := gitlimit.LeaseEpoch(repo); err != nil {
		t.Fatalf("LeaseEpoch = %v", err)
	}
}

// A call that cannot write the epoch fails, and so does the next call while
// the write still fails. The directory stays without HEAD, so once the write
// works, it gets its epoch and finishes, and the cleanup removes a lock that
// is newer than the epoch.
func TestEnsureCacheRepoFailsWhenTheEpochCannotBeWritten(t *testing.T) {
	cacheDir := t.TempDir()
	boom := errors.New("disk full")
	createEpoch = func(string) error { return boom }
	t.Cleanup(func() { createEpoch = gitlimit.CreateEpoch })

	for i := range 2 {
		if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-fails"); !errors.Is(err, boom) {
			t.Fatalf("EnsureCacheRepoCtx call %d = %v, want the epoch error", i+1, err)
		}
	}
	createEpoch = gitlimit.CreateEpoch
	repo, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-fails")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gitlimit.LeaseEpoch(repo); err != nil {
		t.Fatalf("LeaseEpoch of a repository whose creation was finished = %v", err)
	}
	srSetAge(t, filepath.Join(repo, gitlimit.EpochFile), 3*time.Hour)
	lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err != nil || res.Removed != 1 || srExists(t, lock) {
		t.Fatalf("cleanup = %+v, %v (lock left: %v), want the lock removed", res, err, srExists(t, lock))
	}
}
