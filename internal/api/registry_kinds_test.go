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

// With no way to fetch a pack that the cache lacks, the view is built without
// it, and CheckAnswer fails a "missing" answer, so that a caller never reads
// "missing" for an object that a registered pack may hold. An answer that holds
// every object stands.
func TestRegisteredPackViewFailsAMissingAnswerWhenAPackCannotBeFetched(t *testing.T) {
	e := testEnvironment(t, "sqlite")
	fx := wrefSeedFixture(t, e)
	repoPath := filepath.Join(t.TempDir(), "empty.git")
	if err := os.MkdirAll(filepath.Join(repoPath, "objects", "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	calls := 0
	for name, heal := range map[string]func(context.Context) error{
		"no heal":                     nil,
		"a heal that fetches nothing": func(context.Context) error { calls++; return nil },
	} {
		view, err := githttp.NewRegisteredPackView(context.Background(), e.refStore, repoPath, fx.repoID, heal)
		if err != nil {
			t.Fatalf("%s: NewRegisteredPackView = %v, want a view without the packs that the cache lacks", name, err)
		}
		if err := view.CheckAnswer(map[string]string{"a": "commit"}); err != nil {
			t.Fatalf("%s: CheckAnswer(held) = %v, want nil", name, err)
		}
		if err := view.CheckAnswer(map[string]string{"a": "commit", "b": "missing"}); !errors.Is(err, githttp.ErrViewIncomplete) {
			t.Fatalf("%s: CheckAnswer(missing) = %v, want ErrViewIncomplete", name, err)
		}
		view.Close()
	}
	if calls == 0 {
		t.Fatal("the heal was never asked to fetch the missing pack")
	}
}
