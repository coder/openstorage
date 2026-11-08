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

// A creation that fails after the directory was made, here because the epoch
// cannot be written, fails the call. The directory stays without an epoch, so
// the next call finishes the repository and the cleanup keeps its locks.
func TestEnsureCacheRepoFailsWhenTheEpochCannotBeWritten(t *testing.T) {
	cacheDir := t.TempDir()
	boom := errors.New("disk full")
	createEpoch = func(string) error { return boom }
	t.Cleanup(func() { createEpoch = gitlimit.CreateEpoch })

	if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-fails"); !errors.Is(err, boom) {
		t.Fatalf("EnsureCacheRepoCtx = %v, want the epoch error", err)
	}
	createEpoch = gitlimit.CreateEpoch
	repo, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "t-epoch-fails")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gitlimit.LeaseEpoch(repo); !errors.Is(err, gitlimit.ErrNoEpoch) {
		t.Fatalf("LeaseEpoch of a repository whose creation stopped = %v, want ErrNoEpoch", err)
	}
	lock := srLock(t, repo, "refs/heads/x.lock", 2*time.Hour)
	res, err := cleanStaleRefLocks(repo, []string{"refs/heads/x"}, time.Now())
	if err != nil || !res.NoEpoch || !srExists(t, lock) {
		t.Fatalf("cleanup = %+v, %v (lock kept: %v), want the lock kept for want of an epoch", res, err, srExists(t, lock))
	}
}
