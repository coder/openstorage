// Hot-path benchmarks for the hydration fast path (issue #92): every
// smart-HTTP and REST read calls Hydrate first, so the generation-marker
// short circuit is the read path's per-request floor.
// BenchmarkHydrateFastPath times the whole short-circuited Hydrate
// (memoized EnsureCacheRepo verification + one GetGeneration point-read +
// marker read); BenchmarkEnsureCacheRepo isolates the EnsureCacheRepo
// component, whose two `git config --get` subprocess forks per call used
// to dominate the fast path until issue #93 memoized the verification —
// these benchmarks are its before/after evidence. Fixtures are
// container-free (SQLite ref store + fileblob, the local-mode wiring) and
// run identically on both TestMain lanes.
package githttp

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/ref"
)

// benchHydrateEnv returns a SQLite ref store, a fileblob store, and a cache
// dir where repo is fully hydrated once, so every timed Hydrate takes the
// generation-marker short circuit (nothing mutates the store, and the
// benchmark finishes well inside the marker TTL).
func benchHydrateEnv(b *testing.B, repo string) (ref.RefStore, blob.BlobStore, string) {
	b.Helper()
	ctx := context.Background()
	st, err := ref.OpenSQLite(ctx, "sqlite://"+filepath.Join(b.TempDir(), "refs.sqlite"))
	if err != nil {
		b.Fatalf("ref.OpenSQLite: %v", err)
	}
	b.Cleanup(st.Close)
	if err := st.InitSchema(ctx); err != nil {
		b.Fatalf("InitSchema: %v", err)
	}
	bl, err := blob.OpenFile("file://" + filepath.Join(b.TempDir(), "blobs"))
	if err != nil {
		b.Fatalf("blob.OpenFile: %v", err)
	}
	cacheDir := b.TempDir()
	repoPath, refsErr, err := Hydrate(ctx, st, bl, cacheDir, repo, "main")
	if refsErr != nil || err != nil {
		b.Fatalf("warm-up Hydrate = (refsErr %v, err %v), want clean", refsErr, err)
	}
	// Fail fast if the warm-up did not leave a matching fresh marker: the
	// benchmark would silently measure full syncs instead of the fast path.
	if _, head, fresh, ok := readGenerationMarker(repoPath); !ok || !fresh || head != HeadTarget("main") {
		b.Fatalf("warm-up marker = (head %q, fresh %v, ok %v), want a fresh marker for %q", head, fresh, ok, HeadTarget("main"))
	}
	return st, bl, cacheDir
}

// BenchmarkHydrateFastPath measures one short-circuited Hydrate: the
// per-request hydration floor on every read when packs, refs, and HEAD are
// provably unchanged.
func BenchmarkHydrateFastPath(b *testing.B) {
	const repo = "benchrepo"
	st, bl, cacheDir := benchHydrateEnv(b, repo)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, refsErr, err := Hydrate(ctx, st, bl, cacheDir, repo, "main"); refsErr != nil || err != nil {
			b.Fatalf("Hydrate = (refsErr %v, err %v), want clean fast path", refsErr, err)
		}
	}
}

// BenchmarkEnsureCacheRepo measures the steady-state (already configured)
// EnsureCacheRepo alone: the memoized fingerprint verification (issue #93)
// that replaced the two `git config --get` subprocess forks plus hook
// ReadFile/Stat checks that previously ran on EVERY request even when the
// generation marker short-circuited everything else.
func BenchmarkEnsureCacheRepo(b *testing.B) {
	const repo = "benchrepo"
	_, _, cacheDir := benchHydrateEnv(b, repo)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
			b.Fatalf("EnsureCacheRepo: %v", err)
		}
	}
}
