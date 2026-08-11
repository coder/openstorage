package githttp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// SyncRefs must delete local refs that PG does not know about (PG wins).
func TestSyncRefs_DeletesStaleLocalRef(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	mustGit(t, dir, "branch", "stale")

	if err := SyncRefs(dir, map[string]string{"refs/heads/main": head}); err != nil {
		t.Fatalf("SyncRefs: %v", err)
	}

	out := mustGit(t, dir, "for-each-ref", "--format=%(refname) %(objectname)")
	if strings.Contains(out, "refs/heads/stale") {
		t.Fatalf("stale local ref survived sync:\n%s", out)
	}
	if !strings.Contains(out, "refs/heads/main "+head) {
		t.Fatalf("refs/heads/main missing or moved:\n%s", out)
	}
}

// A ref replaced across a directory/file boundary (refs/heads/foo deleted,
// refs/heads/foo/bar created in PG) must sync: deletions have to run before
// updates and in their own update-ref transaction, because git's files
// backend rejects the delete+create pair in one transaction.
func TestSyncRefs_DirectoryFileConflict(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	mustGit(t, dir, "branch", "foo")

	if err := SyncRefs(dir, map[string]string{
		"refs/heads/main":    head,
		"refs/heads/foo/bar": head,
	}); err != nil {
		t.Fatalf("SyncRefs: %v", err)
	}

	out := mustGit(t, dir, "for-each-ref", "--format=%(refname) %(objectname)")
	if strings.Contains(out, "refs/heads/foo "+head) {
		t.Fatalf("refs/heads/foo survived sync:\n%s", out)
	}
	if !strings.Contains(out, "refs/heads/foo/bar "+head) {
		t.Fatalf("refs/heads/foo/bar missing:\n%s", out)
	}
}

// EnsureCacheRepo must repair hooks and config even when HEAD already
// exists: a crash between `git init` and hook installation (or a deleted
// hook file) must not yield a receive-pack that bypasses the S3 upload, PG
// registration, and policy hooks.
func TestEnsureCacheRepo_RepairsHooksAndConfig(t *testing.T) {
	cacheDir := t.TempDir()
	repoPath, err := EnsureCacheRepo(cacheDir, "t-ensure-repair")
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}

	// Simulate a crash right after `git init`: hooks gone, config reverted.
	for _, name := range []string{"pre-receive", "update"} {
		if err := os.Remove(filepath.Join(repoPath, "hooks", name)); err != nil {
			t.Fatal(err)
		}
	}
	mustGit(t, repoPath, "config", "--unset", "receive.unpackLimit")

	again, err := EnsureCacheRepo(cacheDir, "t-ensure-repair")
	if err != nil {
		t.Fatalf("EnsureCacheRepo (repair): %v", err)
	}
	if again != repoPath {
		t.Fatalf("repo path changed on repair: %q != %q", again, repoPath)
	}
	for _, name := range []string{"pre-receive", "update"} {
		fi, err := os.Stat(filepath.Join(repoPath, "hooks", name))
		if err != nil {
			t.Fatalf("hook %s not reinstalled: %v", name, err)
		}
		if fi.Mode().Perm() != 0o755 {
			t.Fatalf("hook %s not executable: %v", name, fi.Mode())
		}
	}
	if got := strings.TrimSpace(mustGit(t, repoPath, "config", "--get", "receive.unpackLimit")); got != "0" {
		t.Fatalf("receive.unpackLimit = %q, want 0", got)
	}
}

// Regression for the corrupt-pack race: a corrupt pack file left at the
// hydration target (crashed downloader, partial write) must never wedge
// hydration. The temp-file + rename scheme downloads fresh bytes beside it
// and atomically replaces it, so hydrate succeeds on the FIRST attempt.
func TestHydratePacks_ReplacesCorruptPack(t *testing.T) {
	ctx := context.Background()
	repo := "t-hydrate-corrupt"
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	packs := pgPacks(t, repo)
	if len(packs) == 0 {
		t.Fatal("push registered no packs")
	}

	// Fresh cache repo with a corrupt pack pre-placed at the hydration target.
	repoPath, err := EnsureCacheRepo(t.TempDir(), repo)
	if err != nil {
		t.Fatalf("ensure cache repo: %v", err)
	}
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		t.Fatal(err)
	}
	corrupt := filepath.Join(packDir, "pack-"+packs[0].ID+".pack")
	if err := os.WriteFile(corrupt, []byte("not a packfile"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := HydratePacks(ctx, testStore, testBlob, repoPath, repo); err != nil {
		t.Fatalf("hydrate with corrupt pre-placed pack failed: %v", err)
	}
	for _, p := range packs {
		idx := filepath.Join(packDir, "pack-"+p.ID+".idx")
		if _, err := os.Stat(idx); err != nil {
			t.Fatalf("pack %s not indexed: %v", p.ID, err)
		}
	}
	// The corrupt bytes were replaced by the real pack.
	if out := mustGit(t, repoPath, "verify-pack", "-v", corrupt); out == "" {
		t.Fatal("verify-pack produced no output for replaced pack")
	}
	// No stray temp files survive a successful hydrate.
	leftovers, err := filepath.Glob(filepath.Join(packDir, "*.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}
}

// Concurrent hydrators of the same pack must serialize on the per-pack lock
// and both succeed (no partial-file os.Remove race, no double index-pack
// failure).
func TestHydratePacks_ConcurrentHydratorsSerialize(t *testing.T) {
	ctx := context.Background()
	repo := "t-hydrate-concurrent"
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	repoPath, err := EnsureCacheRepo(t.TempDir(), repo)
	if err != nil {
		t.Fatalf("ensure cache repo: %v", err)
	}

	const n = 4
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			errs <- HydratePacks(ctx, testStore, testBlob, repoPath, repo)
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent hydrate failed: %v", err)
		}
	}
	packs := pgPacks(t, repo)
	if len(packs) == 0 {
		t.Fatal("push registered no packs")
	}
	for _, p := range packs {
		idx := filepath.Join(repoPath, "objects", "pack", "pack-"+p.ID+".idx")
		if _, err := os.Stat(idx); err != nil {
			t.Fatalf("pack %s not indexed: %v", p.ID, err)
		}
	}
}
