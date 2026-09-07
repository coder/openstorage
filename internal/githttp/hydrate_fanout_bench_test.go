// Ref-fanout benchmark for the full-sync hydration path (issue #119): a
// fetch that cannot take the generation-marker short circuit enumerates the
// repo's whole ref set and diffs it against the local cache repo. Ref
// fanout is attacker-scaled (millions of same-tip refs cost one row each),
// so the enumeration/diff cost — wall clock and transient heap — is the
// serving path's exposure. The fixture is container-free (SQLite +
// fileblob, the local-mode wiring); each iteration removes the generation
// marker so Hydrate takes the full-sync path over an unchanged ref set,
// isolating enumeration + diff (no update-ref churn).
package githttp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/coder/open-storage/internal/blob"
	"github.com/coder/open-storage/internal/ref"
)

// benchFanoutEnv builds a repo whose ref store holds n same-tip refs and
// whose cache repo already contains the tip object (written loose directly
// into the bare repo, so no pack registration is needed).
func benchFanoutEnv(b *testing.B, repo string, n int) (ref.RefStore, blob.BlobStore, string, string) {
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
		b.Fatalf("bootstrap Hydrate = (refsErr %v, err %v), want clean", refsErr, err)
	}
	// One commit object, written loose into the bare cache repo: every ref
	// points at it (the attack shape — fanout without unique objects).
	tree, err := runGit(repoPath, "hash-object", "-t", "tree", "-w", "/dev/null")
	if err != nil {
		b.Fatalf("hash-object: %v", err)
	}
	tip, err := runGit(repoPath, "-c", "user.name=bench", "-c", "user.email=bench@invalid",
		"commit-tree", strings.TrimSpace(tree), "-m", "tip")
	if err != nil {
		b.Fatalf("commit-tree: %v", err)
	}
	sha := strings.TrimSpace(tip)
	for i := range n {
		name := fmt.Sprintf("refs/heads/load-%07d", i)
		if won, err := st.CASRef(ctx, repo, name, ref.ZeroSHA, sha); err != nil || !won {
			b.Fatalf("CASRef %s = (%v, %v), want win", name, won, err)
		}
	}
	// Warm the cache repo once so timed iterations diff an unchanged set.
	if _, refsErr, err := Hydrate(ctx, st, bl, cacheDir, repo, "main"); refsErr != nil || err != nil {
		b.Fatalf("warm-up Hydrate = (refsErr %v, err %v), want clean", refsErr, err)
	}
	return st, bl, cacheDir, repoPath
}

// BenchmarkHydrateFullSyncRefFanout measures one full-sync Hydrate over an
// unchanged ref set of the given size: the marker is removed before every
// iteration, forcing pack listing, ref enumeration, and the local diff.
// peak-live-B reports the high-water live heap across the run (sampled
// after a forced GC before and after each iteration): the bounded-
// materialization evidence that ns/op and cumulative B/op cannot show.
func BenchmarkHydrateFullSyncRefFanout(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("refs=%d", n), func(b *testing.B) {
			repo := fmt.Sprintf("fanout%d", n)
			st, bl, cacheDir, repoPath := benchFanoutEnv(b, repo, n)
			ctx := context.Background()
			marker := filepath.Join(repoPath, generationMarker)
			var ms runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&ms)
			baseline, peak := ms.HeapAlloc, uint64(0)
			b.ReportAllocs()
			for b.Loop() {
				if err := os.Remove(marker); err != nil {
					b.Fatalf("remove marker: %v", err)
				}
				if _, refsErr, err := Hydrate(ctx, st, bl, cacheDir, repo, "main"); refsErr != nil || err != nil {
					b.Fatalf("Hydrate = (refsErr %v, err %v), want clean full sync", refsErr, err)
				}
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peak {
					peak = ms.HeapAlloc
				}
				runtime.GC()
			}
			if peak > baseline {
				b.ReportMetric(float64(peak-baseline), "peak-live-B")
			}
		})
	}
}
