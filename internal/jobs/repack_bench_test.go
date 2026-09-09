// Hot-path benchmarks for repack fold selection (issue #92): selection runs
// on every repack trigger over the repo's full registered row set, so its
// cost scales with pack count. jointGreedy and pairAndExtend are benched
// directly on deterministic in-memory candidates — pairAndExtend on both
// its shapes: seed pair found (clone + stable sort + prefix minima + first
// feasible j), and the unpairable worst case, where it must scan every j to
// PROVE no feasible pair exists. BenchmarkSelectFoldInput covers the full
// selection pass — two blob point reads (Stat + 12-byte header) per
// candidate plus both algorithms — on fileblob, with pack sizes making
// every pair infeasible so the proven-no-op path is measured without
// materializing a fold.
package jobs

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/ref"
)

// benchFoldBudgets are fixed selection budgets for the in-memory benchmarks.
const (
	benchByteBudget = int64(2 << 30)
	benchObjBudget  = int64(1 << 20)
)

// benchCandidates builds n deterministic candidates. Pairable ones cycle
// through small fractions of both budgets (many feasible folds, the greedy
// steady state); unpairable ones each fit alone but any two together bust
// the byte budget, forcing pairAndExtend's full proof scan.
func benchCandidates(n int, pairable bool) []*foldCandidate {
	cands := make([]*foldCandidate, n)
	for i := range cands {
		bytes := benchByteBudget/2 + 1 + int64(i%16) // unpairable: any two > budget
		if pairable {
			bytes = (int64(i%7) + 1) * benchByteBudget / 32
		}
		cands[i] = &foldCandidate{
			pack:    ref.Pack{ID: fmt.Sprintf("%040d", i), Key: fmt.Sprintf("packs/benchrepo/pack-%040d.pack", i), Size: bytes},
			bytes:   bytes,
			objects: (int64(i%5) + 1) * benchObjBudget / 32,
		}
	}
	return cands
}

func BenchmarkJointGreedy(b *testing.B) {
	for _, n := range []int{10, 50, 200, 1000} {
		b.Run(fmt.Sprintf("candidates=%d", n), func(b *testing.B) {
			cands := benchCandidates(n, true)
			b.ReportAllocs()
			for b.Loop() {
				if sel := jointGreedy(cands, benchByteBudget, benchObjBudget); len(sel) < 2 {
					b.Fatalf("jointGreedy selected %d candidates, want >= 2", len(sel))
				}
			}
		})
	}
}

func BenchmarkPairAndExtend(b *testing.B) {
	for _, shape := range []string{"pairable", "unpairable"} {
		for _, n := range []int{10, 50, 200, 1000} {
			b.Run(fmt.Sprintf("%s/candidates=%d", shape, n), func(b *testing.B) {
				pairable := shape == "pairable"
				cands := benchCandidates(n, pairable)
				b.ReportAllocs()
				for b.Loop() {
					sel := pairAndExtend(cands, benchByteBudget, benchObjBudget)
					if pairable && len(sel) < 2 {
						b.Fatalf("pairAndExtend selected %d candidates, want >= 2", len(sel))
					}
					if !pairable && sel != nil {
						b.Fatalf("pairAndExtend selected %d candidates, want a proven nil", len(sel))
					}
				}
			})
		}
	}
}

// BenchmarkSelectFoldInput measures the complete selection pass over n
// registered rows on fileblob. Every pack fits the byte budget alone but no
// two fit together, so selection performs all 2n point reads, runs both
// algorithms, and returns the proven no-op — the steady-state trigger cost
// on a repo with nothing pairable, and an upper bound on selection overhead
// (a feasible fold would additionally materialize and pack, which this
// benchmark deliberately excludes).
func BenchmarkSelectFoldInput(b *testing.B) {
	ctx := context.Background()
	const repo = "benchrepo"
	const byteBudget = int64(1 << 20)
	blobSize := byteBudget/2 + 1
	for _, n := range []int{10, 50, 200, 1000} {
		b.Run(fmt.Sprintf("candidates=%d", n), func(b *testing.B) {
			bl, err := blob.OpenFile("file://" + filepath.Join(b.TempDir(), "blobs"))
			if err != nil {
				b.Fatalf("blob.OpenFile: %v", err)
			}
			// A minimal valid pack header ("PACK", version 2, object count)
			// padded to blobSize: selection trusts the header, so no real
			// pack content is needed for the two point reads.
			content := make([]byte, blobSize)
			copy(content, "PACK")
			binary.BigEndian.PutUint32(content[4:8], 2)
			binary.BigEndian.PutUint32(content[8:12], 64)
			src := filepath.Join(b.TempDir(), "pack")
			if err := os.WriteFile(src, content, 0o644); err != nil {
				b.Fatalf("write source pack: %v", err)
			}
			rows := make([]ref.Pack, n)
			for i := range rows {
				id := fmt.Sprintf("%040d", i)
				key := blob.PackKey(repo, id)
				if err := bl.UploadFile(ctx, key, src); err != nil {
					b.Fatalf("seed pack %d: %v", i, err)
				}
				rows[i] = ref.Pack{ID: id, Key: key, Size: blobSize}
			}
			// The ref store is never touched by selection (rows are passed
			// in) and a selected fold would panic in materialize — both by
			// construction, keeping this a pure selection benchmark.
			st, err := ref.OpenSQLite(ctx, "sqlite://"+filepath.Join(b.TempDir(), "refs.sqlite"))
			if err != nil {
				b.Fatalf("ref.OpenSQLite: %v", err)
			}
			b.Cleanup(st.Close)
			r := NewRepacker(st, bl, func(context.Context, string, []ref.Pack) (string, error) {
				panic("selection benchmark must never materialize a fold")
			}, slog.New(slog.DiscardHandler))
			r.MaxFoldBytes = byteBudget
			b.ReportAllocs()
			for b.Loop() {
				repoPath, fold, err := r.selectFoldInput(ctx, repo, rows)
				if err != nil {
					b.Fatalf("selectFoldInput: %v", err)
				}
				if fold != nil || repoPath != "" {
					b.Fatalf("selectFoldInput = (%q, %d packs), want a proven no-op", repoPath, len(fold))
				}
			}
		})
	}
}
