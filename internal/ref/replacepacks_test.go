// ReplacePacks conformance tests (issue #1): the repack fold-install CAS
// must behave identically on both backends — a won swap replaces exactly
// the old rows in one transaction and bumps the generation, a lost race
// leaves zero DB changes, and concurrently registered rows survive.
package ref

import (
	"context"
	"fmt"
	"testing"
)

func TestReplacePacks(t *testing.T) {
	runRefBackends(t, testReplacePacks)
}

func testReplacePacks(t *testing.T, s Store) {
	ctx := context.Background()
	repo := t.Name()

	mustRegister := func(packs ...Pack) {
		t.Helper()
		if err := s.RegisterPacks(ctx, repo, packs); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
	}
	packIDs := func() map[string]string {
		t.Helper()
		packs, err := s.ListPacks(ctx, repo)
		if err != nil {
			t.Fatalf("ListPacks: %v", err)
		}
		out := make(map[string]string, len(packs))
		for _, p := range packs {
			out[p.ID] = p.Key
		}
		return out
	}
	gen := func() int64 {
		t.Helper()
		g, err := s.GetGeneration(ctx, repo)
		if err != nil {
			t.Fatalf("GetGeneration: %v", err)
		}
		return g
	}
	replace := func(old []Pack, newPack Pack) bool {
		t.Helper()
		won, err := s.ReplacePacks(ctx, repo, old, newPack)
		if err != nil {
			t.Fatalf("ReplacePacks: %v", err)
		}
		return won
	}
	wantPacks := func(want map[string]string) {
		t.Helper()
		got := packIDs()
		if len(got) != len(want) {
			t.Fatalf("pack set = %v, want %v", got, want)
		}
		for id, key := range want {
			if got[id] != key {
				t.Fatalf("pack set = %v, want %v", got, want)
			}
		}
	}

	a, b, c := Pack{ID: "aa1", Key: "ka"}, Pack{ID: "bb2", Key: "kb"}, Pack{ID: "cc3", Key: "kc"}
	fold1 := Pack{ID: "f01", Key: "kf1"}

	// Won swap: old rows deleted, new row inserted, generation bumped.
	mustRegister(a, b, c)
	genBefore := gen()
	if !replace([]Pack{a, b, c}, fold1) {
		t.Fatal("ReplacePacks lost with the full current set as precondition")
	}
	wantPacks(map[string]string{fold1.ID: fold1.Key})
	if g := gen(); g <= genBefore {
		t.Fatalf("won ReplacePacks did not bump generation: %d -> %d", genBefore, g)
	}

	// Lost race: the same old set again (rows already folded) must change
	// nothing — no partial delete, no inserted row, no generation bump.
	genBefore = gen()
	if replace([]Pack{a, b, c}, Pack{ID: "f02", Key: "kf2"}) {
		t.Fatal("ReplacePacks won against an already-folded old set")
	}
	wantPacks(map[string]string{fold1.ID: fold1.Key})
	if g := gen(); g != genBefore {
		t.Fatalf("lost ReplacePacks bumped generation: %d -> %d", genBefore, g)
	}

	// Mismatched set: one live row plus one unknown row must lose without
	// deleting the live row (rowcount CAS covers partial matches).
	genBefore = gen()
	if replace([]Pack{fold1, {ID: "ghost", Key: "kg"}}, Pack{ID: "f03", Key: "kf3"}) {
		t.Fatal("ReplacePacks won with a partially-missing old set")
	}
	wantPacks(map[string]string{fold1.ID: fold1.Key})
	if g := gen(); g != genBefore {
		t.Fatalf("lost ReplacePacks bumped generation: %d -> %d", genBefore, g)
	}

	// Concurrently registered rows are not part of the precondition and
	// survive the swap (a mid-repack push must not starve folding).
	mustRegister(b, c) // "concurrent" pushes after the fold1 snapshot
	fold2 := Pack{ID: "f04", Key: "kf4"}
	if !replace([]Pack{fold1, b}, fold2) {
		t.Fatal("ReplacePacks lost despite its old rows being present")
	}
	wantPacks(map[string]string{fold2.ID: fold2.Key, c.ID: c.Key})

	// Identity edge: a fold whose new pack id equals an old row's id (the
	// consolidated pack can be byte-identical to one input) must keep the
	// row present after the swap.
	if !replace([]Pack{fold2, c}, fold2) {
		t.Fatal("ReplacePacks lost on identity fold")
	}
	wantPacks(map[string]string{fold2.ID: fold2.Key})
}

// TestReplacePacksSequentialRace drives the documented two-node interleave:
// both folders snapshot the same old set, the first swap wins, the second
// must lose with zero DB changes (its uploaded blob is the caller's to
// delete — covered in internal/jobs).
func TestReplacePacksSequentialRace(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		repo := t.Name()
		old := []Pack{{ID: "p1", Key: "k1"}, {ID: "p2", Key: "k2"}}
		if err := s.RegisterPacks(ctx, repo, old); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
		wonA, err := s.ReplacePacks(ctx, repo, old, Pack{ID: "na", Key: "kna"})
		if err != nil {
			t.Fatalf("ReplacePacks A: %v", err)
		}
		wonB, err := s.ReplacePacks(ctx, repo, old, Pack{ID: "nb", Key: "knb"})
		if err != nil {
			t.Fatalf("ReplacePacks B: %v", err)
		}
		if !wonA || wonB {
			t.Fatalf("exactly the first fold must win: wonA=%v wonB=%v", wonA, wonB)
		}
		packs, err := s.ListPacks(ctx, repo)
		if err != nil {
			t.Fatalf("ListPacks: %v", err)
		}
		if len(packs) != 1 || packs[0].ID != "na" {
			t.Fatalf("pack set after race = %+v, want only na", packs)
		}
	})
}

// TestReplacePacksConcurrent hammers one repo with parallel identical folds:
// exactly one must win per round regardless of scheduling (the transactional
// analog of the CASRef concurrency contract).
func TestReplacePacksConcurrent(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		repo := t.Name()
		old := []Pack{{ID: "p1", Key: "k1"}, {ID: "p2", Key: "k2"}, {ID: "p3", Key: "k3"}}
		if err := s.RegisterPacks(ctx, repo, old); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
		const racers = 8
		results := make(chan bool, racers)
		errs := make(chan error, racers)
		for i := range racers {
			go func() {
				won, err := s.ReplacePacks(ctx, repo, old, Pack{ID: fmt.Sprintf("n%d", i), Key: fmt.Sprintf("kn%d", i)})
				results <- won
				errs <- err
			}()
		}
		wins := 0
		for range racers {
			if err := <-errs; err != nil {
				t.Fatalf("ReplacePacks: %v", err)
			}
			if <-results {
				wins++
			}
		}
		if wins != 1 {
			t.Fatalf("got %d winning folds, want exactly 1", wins)
		}
		packs, err := s.ListPacks(ctx, repo)
		if err != nil {
			t.Fatalf("ListPacks: %v", err)
		}
		if len(packs) != 1 {
			t.Fatalf("pack set after concurrent folds = %+v, want exactly one row", packs)
		}
	})
}
