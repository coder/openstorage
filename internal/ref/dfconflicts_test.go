package ref

import (
	"context"
	"testing"
)

// TestDFConflicts_AcrossBackends pins the batched authority D/F probe's
// verdict semantics on every production store (issue #170 push holds;
// Codex round-13 on coder/open-storage#173: the probe must be batched and
// candidate-proportional, so each backend implements it natively): strict
// prefix ancestors and strict descendants conflict; exact matches,
// siblings, and clean names do not. The '.' fixture pins byte-order
// handling — '.' (0x2E) sorts below '/' (0x2F), the classic decorated-key
// trap.
func TestDFConflicts_AcrossBackends(t *testing.T) {
	runRefBackends(t, testDFConflictsVerdicts)
}

// TestDFConflicts_ChunkedProbeKeepsVerdicts (Codex round-16 security P2 on
// #173): the PG/SQLite probes process candidates in bounded chunks so a
// huge no-pack command list cannot amplify into millions of resident
// prefix strings; verdicts must be identical across chunk boundaries. The
// 7-candidate fixture spans three chunks at size 3.
func TestDFConflicts_ChunkedProbeKeepsVerdicts(t *testing.T) {
	prev := dfProbeChunk
	dfProbeChunk = 3
	t.Cleanup(func() { dfProbeChunk = prev })
	runRefBackends(t, testDFConflictsVerdicts)
}

func testDFConflictsVerdicts(t *testing.T, s Store) {
	prober, ok := s.(interface {
		DFConflicts(ctx context.Context, repo string, names []string) (map[string]string, error)
	})
	if !ok {
		t.Fatalf("%T does not implement DFConflicts — the githttp push-hold probe would regress to per-name reads", s)
	}
	ctx := context.Background()
	repo := t.Name()
	sha := testSHA(0x2a)
	for _, name := range []string{
		"refs/heads/a",
		"refs/heads/x/y",
		"refs/heads/plain",
		"refs/heads/dot.name/child",
		"refs/heads/deep/nested/leaf",
	} {
		if won, err := s.CASRef(ctx, repo, name, ZeroSHA, sha); err != nil || !won {
			t.Fatalf("seed CASRef %s = (%v, %v), want win", name, won, err)
		}
	}
	got, err := prober.DFConflicts(ctx, repo, []string{
		"refs/heads/a/b",      // strict ancestor a exists
		"refs/heads/x",        // strict descendant x/y exists
		"refs/heads/plain",    // exact match: NOT a D/F conflict
		"refs/heads/new",      // clean
		"refs/heads/ab",       // sibling of a: clean
		"refs/heads/dot.name", // descendant exists; '.' sorts below '/'
		"refs/heads/deep",     // deep strict descendant exists
	})
	if err != nil {
		t.Fatalf("DFConflicts: %v", err)
	}
	want := map[string]string{
		"refs/heads/a/b":      "refs/heads/a",
		"refs/heads/x":        "refs/heads/x/y",
		"refs/heads/dot.name": "refs/heads/dot.name/child",
		"refs/heads/deep":     "refs/heads/deep/nested/leaf",
	}
	for cand, witness := range want {
		if got[cand] != witness {
			t.Errorf("DFConflicts[%s] = %q, want %q", cand, got[cand], witness)
		}
	}
	for _, clean := range []string{"refs/heads/plain", "refs/heads/new", "refs/heads/ab"} {
		if w, ok := got[clean]; ok {
			t.Errorf("DFConflicts[%s] = %q, want no conflict", clean, w)
		}
	}
	// Empty candidate set: no store work, empty result.
	if got, err := prober.DFConflicts(ctx, repo, nil); err != nil || len(got) != 0 {
		t.Errorf("DFConflicts(nil) = (%v, %v), want empty", got, err)
	}
}
