package gitread

// Materialize unit test (p4 read-API burn-down item 7): hydrate a bare cache
// repo from the zero-container local backends (sqlite ref store + file blob
// store) and prove the DB stays the ref authority across
// re-materializations. No containers, no network.

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/open-storage/internal/blob"
	"github.com/coder/open-storage/internal/ref"
)

func TestMaterialize(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	scratch := t.TempDir()
	st, err := ref.Open(ctx, "sqlite://"+filepath.Join(scratch, "codestorage.db"))
	if err != nil {
		t.Fatalf("open ref store: %v", err)
	}
	defer st.Close()
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	bl, err := blob.Open(ctx, "file://"+filepath.Join(scratch, "blobs"))
	if err != nil {
		t.Fatalf("open blob store: %v", err)
	}
	defer bl.Close()
	if err := bl.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	// Seed the stores the way a push would leave them: one pack holding
	// every fixture object plus CAS-inserted ref rows.
	const repoID = "materialize-fixture"
	packPath := filepath.Join(scratch, "seed.pack")
	packFile, err := os.Create(packPath)
	if err != nil {
		t.Fatal(err)
	}
	packCmd := exec.Command("git", "pack-objects", "--all", "--stdout")
	packCmd.Dir = f.dir
	packCmd.Stdout = packFile
	if err := packCmd.Run(); err != nil {
		t.Fatalf("git pack-objects: %v", err)
	}
	if err := packFile.Close(); err != nil {
		t.Fatal(err)
	}
	const packID = "seedpack01"
	key := blob.PackKey(repoID, packID)
	if err := bl.UploadFile(ctx, key, packPath); err != nil {
		t.Fatalf("upload pack: %v", err)
	}
	if err := st.RegisterPacks(ctx, repoID, []ref.Pack{{ID: packID, Key: key}}); err != nil {
		t.Fatalf("register pack: %v", err)
	}
	if won, err := st.CASRef(ctx, repoID, "refs/heads/main", ref.ZeroSHA, f.c2); err != nil || !won {
		t.Fatalf("seed main ref: won=%v err=%v", won, err)
	}

	cacheDir := t.TempDir()
	repoPath, err := Materialize(ctx, st, bl, cacheDir, repoID, "main")
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if got, err := ResolveRef(ctx, repoPath, "main", false); err != nil || got != f.c2 {
		t.Fatalf("materialized main = (%q, %v), want %s", got, err, f.c2)
	}
	// Objects must be hydrated from the registered pack, not just refs.
	entries, err := LsTree(ctx, repoPath, f.c2, "", true)
	if err != nil || len(entries) == 0 {
		t.Fatalf("materialized ls-tree = (%d entries, %v), want fixture tree", len(entries), err)
	}

	// The DB is the ref authority: move main in the store only and
	// re-materialize — the cache repo must follow without any new pack.
	if won, err := st.CASRef(ctx, repoID, "refs/heads/main", f.c2, f.c1); err != nil || !won {
		t.Fatalf("move main ref: won=%v err=%v", won, err)
	}
	repoPath2, err := Materialize(ctx, st, bl, cacheDir, repoID, "main")
	if err != nil {
		t.Fatalf("re-Materialize: %v", err)
	}
	if repoPath2 != repoPath {
		t.Fatalf("re-Materialize path = %q, want stable %q", repoPath2, repoPath)
	}
	if got, err := ResolveRef(ctx, repoPath, "main", false); err != nil || got != f.c1 {
		t.Fatalf("re-materialized main = (%q, %v), want %s", got, err, f.c1)
	}

	// A DB ref naming an object no registered pack contains must fail the
	// materialization (read plumbing over wrong refs would serve wrong
	// answers), not silently drop the ref.
	const missing = "1111111111111111111111111111111111111111"
	if won, err := st.CASRef(ctx, repoID, "refs/heads/broken", ref.ZeroSHA, missing); err != nil || !won {
		t.Fatalf("seed broken ref: won=%v err=%v", won, err)
	}
	if _, err := Materialize(ctx, st, bl, cacheDir, repoID, "main"); err == nil {
		t.Fatal("Materialize succeeded despite a ref to a missing object")
	}
}

// TestMaterializeUnknownRepo pins the empty-repo behavior: no packs and no
// refs is a valid (unborn) state, not an error.
func TestMaterializeUnknownRepo(t *testing.T) {
	ctx := context.Background()
	scratch := t.TempDir()
	st, err := ref.Open(ctx, "sqlite://"+filepath.Join(scratch, "codestorage.db"))
	if err != nil {
		t.Fatalf("open ref store: %v", err)
	}
	defer st.Close()
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	bl, err := blob.Open(ctx, "file://"+filepath.Join(scratch, "blobs"))
	if err != nil {
		t.Fatalf("open blob store: %v", err)
	}
	defer bl.Close()
	if err := bl.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	repoPath, err := Materialize(ctx, st, bl, t.TempDir(), "never-pushed", "main")
	if err != nil {
		t.Fatalf("Materialize(empty repo): %v", err)
	}
	if !strings.HasPrefix(filepath.Base(repoPath), "never-pushed") && !strings.Contains(repoPath, "never-pushed") {
		t.Fatalf("repoPath %q does not reference the repo key", repoPath)
	}
	if _, err := ResolveRef(ctx, repoPath, "main", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unborn main resolved: %v", err)
	}
}
