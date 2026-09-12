package githttp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
		<-done
		t.Fatal("cold hydrate ignored cancellation while waiting for initialization")
	}
	if lp, lr, gg := st.counts(); lp != 0 || lr != 0 || gg != 0 {
		t.Fatalf("canceled initialization reached authority: packs=%d refs=%d generation=%d", lp, lr, gg)
	}
	if _, err := os.Stat(filepath.Join(path, "HEAD")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled initialization created HEAD: %v", err)
	}
}

func TestEnsureCacheRepoCtx_ConfigErrorReleasesLock(t *testing.T) {
	cacheDir := t.TempDir()
	path := RepoPath(cacheDir, "failed")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	configLock := filepath.Join(path, "config.lock")
	if err := os.WriteFile(configLock, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureCacheRepoCtx(t.Context(), cacheDir, "failed"); err == nil {
		t.Fatal("Git config-lock error was suppressed")
	}
	if err := os.Remove(configLock); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := EnsureCacheRepoCtx(ctx, cacheDir, "failed"); err != nil {
		t.Fatalf("initialization after failure: %v", err)
	}
}
