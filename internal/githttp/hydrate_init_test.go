package githttp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnsureCacheRepoCtx_InitializationContention(t *testing.T) {
	for _, repair := range []bool{false, true} {
		name := "cold"
		if repair {
			name = "repair"
		}
		t.Run(name, func(t *testing.T) {
			cacheDir := t.TempDir()
			const repo = "contended"
			path := RepoPath(cacheDir, repo)
			if repair {
				if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, repo); err != nil {
					t.Fatal(err)
				}
				mustGit(t, path, "config", "receive.unpackLimit", "1")
			} else if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatal(err)
			}
			// Model a peer initializer holding Git's config lock inside the
			// shared filesystem critical section. A contender must wait for
			// that section, not execute Git and fail on config.lock.
			unlock, err := flockExclusiveCtx(t.Context(), filepath.Join(path, "codestorage.hydrate.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()
			configLock := filepath.Join(path, "config.lock")
			if err := os.WriteFile(configLock, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			var spawns atomic.Int32
			obs := func(_ string, _ []string) { spawns.Add(1) }
			gitSpawnObserver.Store(&obs)
			defer gitSpawnObserver.Store(nil)
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if _, err := EnsureCacheRepoCtx(ctx, cacheDir, repo); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("contended initialization = %v, want context deadline", err)
			}
			if got := spawns.Load(); got != 0 {
				t.Errorf("contended initialization spawned Git %d times, want 0", got)
			}
			gitSpawnObserver.Store(nil)
			// No global lock: another repository can initialize while this
			// repository's initialization is still held.
			otherCtx, otherCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer otherCancel()
			if _, err := EnsureCacheRepoCtx(otherCtx, cacheDir, "independent"); err != nil {
				t.Fatalf("independent repository: %v", err)
			}
			if err := os.Remove(configLock); err != nil {
				t.Fatal(err)
			}
			unlock()
			if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, repo); err != nil {
				t.Fatalf("after release: %v", err)
			}
			if _, err := verifyLocalConfig(t.Context(), path, filepath.Join(path, "config")); err != nil {
				t.Fatalf("initialized config: %v", err)
			}
			// A memo hit must stay fork-free and must not take the lock.
			unlockWarm, err := flockExclusiveCtx(t.Context(), filepath.Join(path, "codestorage.hydrate.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer unlockWarm()
			spawns.Store(0)
			gitSpawnObserver.Store(&obs)
			warmCtx, warmCancel := context.WithTimeout(t.Context(), time.Second)
			defer warmCancel()
			if _, err := EnsureCacheRepoCtx(warmCtx, cacheDir, repo); err != nil {
				t.Fatalf("warm repository: %v", err)
			}
			if got := spawns.Load(); got != 0 {
				t.Errorf("warm initialization spawned Git %d times, want 0", got)
			}
		})
	}
}

func TestEnsureCacheRepoCtx_InterruptedMutation(t *testing.T) {
	for _, mode := range []string{"init", "head-lock", "config"} {
		t.Run(mode, func(t *testing.T) {
			cacheDir := t.TempDir()
			const repo = "interrupted"
			path := RepoPath(cacheDir, repo)
			var head, refs string
			if mode == "config" {
				mustGit(t, "", "clone", "--bare", initWorkRepo(t), path)
				head = mustGit(t, path, "rev-parse", "HEAD")
				refs = mustGit(t, path, "show-ref")
			} else if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatal(err)
			}
			lockPath := filepath.Join(path, "codestorage.hydrate.lock")
			unlock, err := flockExclusiveCtx(t.Context(), lockPath)
			if err != nil {
				t.Fatal(err)
			}
			unlock()
			lockBefore, err := os.Stat(lockPath)
			if err != nil {
				t.Fatal(err)
			}
			realGit, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			bin := t.TempDir()
			ready := filepath.Join(bin, "ready")
			// Pause after the mutation's artifacts exist, then let the real
			// CommandContext kill/reap its child. No timing-sensitive Git kill.
			const wrapper = `#!/bin/sh
set -eu
if [ "$1" = "$CANCEL_GIT_MODE" ] || { [ "$1" = init ] && [ "$CANCEL_GIT_MODE" = head-lock ]; }; then
  if [ "$1" = init ]; then
    mkdir -p "$CANCEL_GIT_REPO/refs"
    if [ "$CANCEL_GIT_MODE" = head-lock ]; then
      : > "$CANCEL_GIT_REPO/HEAD.lock"
    else
      printf 'ref: refs/heads/main\n' > "$CANCEL_GIT_REPO/HEAD"
    fi
    printf '[core]\n\tbare = true\n' > "$CANCEL_GIT_REPO/config"
  elif [ "$2" != --replace-all ]; then
    exec "$CANCEL_REAL_GIT" "$@"
  fi
  : > "$CANCEL_GIT_REPO/config.lock"
  : > "$CANCEL_GIT_READY"
  exec sleep 30
fi
exec "$CANCEL_REAL_GIT" "$@"
`
			if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0o755); err != nil {
				t.Fatal(err)
			}
			oldPath := os.Getenv("PATH")
			t.Setenv("PATH", bin+string(os.PathListSeparator)+oldPath)
			t.Setenv("CANCEL_REAL_GIT", realGit)
			t.Setenv("CANCEL_GIT_MODE", mode)
			t.Setenv("CANCEL_GIT_REPO", path)
			t.Setenv("CANCEL_GIT_READY", ready)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := EnsureCacheRepoCtx(ctx, cacheDir, repo)
				done <- err
			}()
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					cancel()
					<-done //nolint:errcheck // only wait for the canceled call to return
					t.Fatal(err)
				}
				select {
				case err := <-done:
					t.Fatalf("mutation exited before cancellation: %v", err)
				case <-time.After(10 * time.Millisecond):
				}
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("interrupted mutation = %v, want context canceled", err)
			}
			t.Setenv("PATH", oldPath)
			for _, name := range []string{"config.lock", "HEAD.lock"} {
				if _, err := os.Stat(filepath.Join(path, name)); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("interrupted mutation left %s: %v", name, err)
				}
			}
			if mode != "config" {
				if _, err := os.Stat(filepath.Join(path, "HEAD")); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("interrupted initialization left HEAD: %v", err)
				}
			} else {
				if got := mustGit(t, path, "rev-parse", "HEAD"); got != head {
					t.Errorf("HEAD changed: got %q, want %q", got, head)
				}
				if got := mustGit(t, path, "show-ref"); got != refs {
					t.Errorf("refs changed: got %q, want %q", got, refs)
				}
				mustGit(t, path, "fsck", "--full")
			}
			lockAfter, err := os.Stat(lockPath)
			if err != nil || !os.SameFile(lockBefore, lockAfter) {
				t.Fatalf("hydrate lock inode changed: %v", err)
			}
			if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, repo); err != nil {
				t.Fatalf("recovery after cancellation: %v", err)
			}
			if _, err := verifyLocalConfig(t.Context(), path, filepath.Join(path, "config")); err != nil {
				t.Fatalf("recovered config: %v", err)
			}
		})
	}
}

func TestEnsureCacheRepoCtx_PartialInitialization(t *testing.T) {
	for _, missing := range []string{"objects", "refs"} {
		t.Run(missing, func(t *testing.T) {
			cacheDir := t.TempDir()
			const repo = "partial"
			path := RepoPath(cacheDir, repo)
			mustGit(t, "", "init", "--bare", "--initial-branch=preserved", path)
			// A real interrupted Git init can leave HEAD/config/refs but no
			// objects directory. HEAD alone must not bypass initialization.
			if err := os.RemoveAll(filepath.Join(path, missing)); err != nil {
				t.Fatal(err)
			}
			if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, repo); err != nil {
				t.Fatalf("partial initialization: %v", err)
			}
			if got := strings.TrimSpace(mustGit(t, path, "symbolic-ref", "HEAD")); got != "refs/heads/preserved" {
				t.Fatalf("preexisting HEAD changed: %q", got)
			}
			if got := strings.TrimSpace(mustGit(t, path, "rev-parse", "--is-bare-repository")); got != "true" {
				t.Fatalf("recovered repository is not bare: %q", got)
			}
			if _, err := verifyLocalConfig(t.Context(), path, filepath.Join(path, "config")); err != nil {
				t.Fatalf("recovered config: %v", err)
			}
		})
	}
}

func TestHydrate_ColdInitializationCancellation(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "cold-cancel"
	path := RepoPath(cacheDir, repo)
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	unlock, err := flockExclusiveCtx(t.Context(), filepath.Join(path, "codestorage.hydrate.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := Hydrate(ctx, st, bl, cacheDir, repo, "main")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cold hydrate = %v, want context deadline", err)
		}
	case <-time.After(5 * time.Second):
		unlock()
		<-done //nolint:errcheck // only wait for the unblocked call to return
		t.Fatal("cold hydrate ignored cancellation while waiting for initialization")
	}
	if lp, lr, gg := st.counts(); lp != 0 || lr != 0 || gg != 0 {
		t.Fatalf("canceled initialization reached authority: packs=%d refs=%d generation=%d", lp, lr, gg)
	}
	if _, err := os.Stat(filepath.Join(path, "HEAD")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled initialization created HEAD: %v", err)
	}
}

func TestEnsureCacheRepoCtx_GitErrorReleasesLock(t *testing.T) {
	for _, name := range []string{"config.lock", "HEAD.lock"} {
		t.Run(name, func(t *testing.T) {
			cacheDir := t.TempDir()
			path := RepoPath(cacheDir, "failed")
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatal(err)
			}
			gitLock := filepath.Join(path, name)
			const lockContent = "preexisting lock\n"
			if err := os.WriteFile(gitLock, []byte(lockContent), 0o644); err != nil {
				t.Fatal(err)
			}
			lockBefore, err := os.Stat(gitLock)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "failed"); err == nil {
				t.Fatal("Git lock error was suppressed")
			}
			lockAfter, err := os.Stat(gitLock)
			if err != nil || !os.SameFile(lockBefore, lockAfter) {
				t.Fatalf("preexisting Git lock inode changed: %v", err)
			}
			if got, err := os.ReadFile(gitLock); err != nil || string(got) != lockContent {
				t.Fatalf("preexisting Git lock changed: %q, %v", got, err)
			}
			if err := os.Remove(gitLock); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := EnsureCacheRepoCtx(ctx, cacheDir, "failed"); err != nil {
				t.Fatalf("initialization after failure: %v", err)
			}
		})
	}
}
