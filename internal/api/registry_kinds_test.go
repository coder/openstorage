package api

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/gitread"
)

// RegisteredKinds answers from the registered packs alone: a commit that only
// the cache holds reads as missing, and a registered pack that the cache lacks
// (a fold replaced the packs since the cache was hydrated) is downloaded first,
// so that a healthy object never reads as missing.
func TestRegisteredKindsReadsTheRegisteredPacksOnly(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		ctx := context.Background()
		repoPath, err := gitread.Materialize(ctx, e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
		if err != nil {
			t.Fatal(err)
		}
		planted := plantCacheOnlyCommit(t, repoPath, fx.c2)
		kinds, err := gitread.RegisteredKinds(ctx, e.refStore, e.blobStore, repoPath, fx.repoID, []string{fx.c1, fx.c2, fx.tagObj, planted})
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]string{fx.c1: "commit", fx.c2: "commit", fx.tagObj: "tag", planted: "missing"}
		for id, k := range want {
			if kinds[id] != k {
				t.Fatalf("kind of %s = %q, want %q (all: %v)", id, kinds[id], k, kinds)
			}
		}

		// The cache loses its pack files: the registry still lists the packs.
		packs, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "pack-*"))
		if err != nil || len(packs) == 0 {
			t.Fatalf("pack files in the cache: %v, %v", packs, err)
		}
		for _, p := range packs {
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
		}
		kinds, err = gitread.RegisteredKinds(ctx, e.refStore, e.blobStore, repoPath, fx.repoID, []string{fx.c2})
		if err != nil || kinds[fx.c2] != "commit" {
			t.Fatalf("after the cache lost its packs: %v, %v", kinds, err)
		}
	})
}

// With no way to fetch a pack that the cache lacks, the view refuses, so that
// a caller never reads "missing" for an object that a registered pack holds.
func TestRegisteredPackViewRefusesWhenAPackIsMissingAndCannotBeFetched(t *testing.T) {
	e := testEnvironment(t, "sqlite")
	fx := wrefSeedFixture(t, e)
	repoPath := filepath.Join(t.TempDir(), "empty.git")
	if err := os.MkdirAll(filepath.Join(repoPath, "objects", "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := githttp.NewRegisteredPackView(context.Background(), e.refStore, repoPath, fx.repoID, nil); !errors.Is(err, githttp.ErrPackMissing) {
		t.Fatalf("NewRegisteredPackView with no heal = %v, want ErrPackMissing", err)
	}
	calls := 0
	_, err := githttp.NewRegisteredPackView(context.Background(), e.refStore, repoPath, fx.repoID, func(context.Context) error { calls++; return nil })
	if !errors.Is(err, githttp.ErrPackMissing) || calls != 1 {
		t.Fatalf("NewRegisteredPackView with a heal that fetches nothing = %v after %d heals, want ErrPackMissing after 1", err, calls)
	}
}
