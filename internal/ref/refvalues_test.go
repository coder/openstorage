package ref

import (
	"context"
	"testing"
)

// TestRefValues_AcrossBackends pins the batched exact-value probe on every
// production store (issue #170 atomic-push base check; Codex round-14 P1
// on coder/code-storage#173: atomic pushes must verify every command's
// authority base before any update hook's CAS, in ONE push-proportional
// batch): present names map to their exact sha, absent names are omitted,
// and the empty candidate set does no store work.
func TestRefValues_AcrossBackends(t *testing.T) {
	runRefBackends(t, testRefValues)
}

// TestRefValues_ChunkedProbeKeepsValues re-runs the conformance suite
// with a tiny chunk so the multi-chunk path (Codex round-21 P2 on #173:
// PG must not encode an unbounded push's every name into one array
// parameter) provably returns the same values as the single-chunk path.
func TestRefValues_ChunkedProbeKeepsValues(t *testing.T) {
	prev := dfProbeChunk
	dfProbeChunk = 2
	t.Cleanup(func() { dfProbeChunk = prev })
	runRefBackends(t, testRefValues)
}

func testRefValues(t *testing.T, s Store) {
	prober, ok := s.(interface {
		RefValues(ctx context.Context, repo string, names []string) (map[string]string, error)
	})
	if !ok {
		t.Fatalf("%T does not implement RefValues — the githttp atomic base check would regress to per-name reads", s)
	}
	ctx := context.Background()
	repo := t.Name()
	shaA, shaB := testSHA(0x2a), testSHA(0x2b)
	for name, sha := range map[string]string{
		"refs/heads/a":    shaA,
		"refs/heads/b":    shaB,
		"refs/heads/deep": shaA,
	} {
		if won, err := s.CASRef(ctx, repo, name, ZeroSHA, sha); err != nil || !won {
			t.Fatalf("seed CASRef %s = (%v, %v), want win", name, won, err)
		}
	}
	got, err := prober.RefValues(ctx, repo, []string{
		"refs/heads/a",
		"refs/heads/b",
		"refs/heads/absent",
		"refs/heads/a/b", // D/F child of an existing ref: still just absent here
	})
	if err != nil {
		t.Fatalf("RefValues: %v", err)
	}
	want := map[string]string{
		"refs/heads/a": shaA,
		"refs/heads/b": shaB,
	}
	if len(got) != len(want) {
		t.Fatalf("RefValues = %v, want exactly %v", got, want)
	}
	for name, sha := range want {
		if got[name] != sha {
			t.Errorf("RefValues[%s] = %q, want %q", name, got[name], sha)
		}
	}
	// Empty candidate set: no store work, empty result.
	if got, err := prober.RefValues(ctx, repo, nil); err != nil || len(got) != 0 {
		t.Errorf("RefValues(nil) = (%v, %v), want empty", got, err)
	}
}
