package spike

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

// Regression for the index-pack fix: a corrupt pack file left on disk must be
// deleted on failure so a retry re-downloads it (previously the corrupt file
// suppressed the download forever).
func TestHydratePacks_RedownloadsAfterCorruptPack(t *testing.T) {
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

	if err := HydratePacks(ctx, testStore, testBlob, repoPath, repo); err == nil {
		t.Fatal("first hydrate succeeded despite corrupt pack")
	}
	if _, err := os.Stat(corrupt); !os.IsNotExist(err) {
		t.Fatalf("corrupt pack not removed after failed index-pack (stat err: %v)", err)
	}

	if err := HydratePacks(ctx, testStore, testBlob, repoPath, repo); err != nil {
		t.Fatalf("second hydrate (re-download) failed: %v", err)
	}
	for _, p := range packs {
		idx := filepath.Join(packDir, "pack-"+p.ID+".idx")
		if _, err := os.Stat(idx); err != nil {
			t.Fatalf("pack %s not indexed after retry: %v", p.ID, err)
		}
	}
}
