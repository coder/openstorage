// Absence reconciliation paging tests (issue #101), fanned out over both
// backends via runRefBackends like ledger_test.go.
package ref

import (
	"context"
	"testing"
)

// absenceStore is the concrete paging surface both backends carry
// (jobs.AbsenceRegistry mirrors it; the methods are deliberately not on
// RefStore — only the background job drives them).
type absenceStore interface {
	Store
	ListFleetLFSObjects(ctx context.Context, afterRepo, afterKey string, limit int) ([]AbsenceLFSRow, error)
	ListFleetPacks(ctx context.Context, afterRepo, afterID string, limit int) ([]AbsencePackRow, error)
}

func absenceTestStore(t *testing.T, s Store) absenceStore {
	t.Helper()
	as, ok := s.(absenceStore)
	if !ok {
		t.Fatalf("store %T lacks the absence paging surface", s)
	}
	return as
}

// TestAbsenceListFleetLFSObjects pages registered rows across repository
// boundaries in (repo, key) keyset order, resumes from any cursor, and
// reports the counted bytes.
func TestAbsenceListFleetLFSObjects(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		as := absenceTestStore(t, s)
		ctx := context.Background()
		repoA, repoB := t.Name()+"-a", t.Name()+"-b"
		keys := []struct {
			repo, key string
			bytes     int64
		}{
			{repoA, canonicalLFSKey(repoA, "aa"), 11},
			{repoA, canonicalLFSKey(repoA, "bb"), 22},
			{repoB, canonicalLFSKey(repoB, "cc"), 33},
		}
		for _, k := range keys {
			if err := s.RegisterLFSObject(ctx, k.repo, k.key, k.bytes); err != nil {
				t.Fatal(err)
			}
		}

		var got []AbsenceLFSRow
		afterRepo, afterKey := "", ""
		for {
			page, err := as.ListFleetLFSObjects(ctx, afterRepo, afterKey, 2)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			got = append(got, page...)
			last := page[len(page)-1]
			afterRepo, afterKey = last.Repo, last.Key
		}
		// Other tests' rows may coexist in the shared pg container: filter
		// to this test's repos, order must be preserved.
		var mine []AbsenceLFSRow
		for _, r := range got {
			if r.Repo == repoA || r.Repo == repoB {
				mine = append(mine, r)
			}
		}
		if len(mine) != len(keys) {
			t.Fatalf("paged %d rows, want %d: %+v", len(mine), len(keys), mine)
		}
		for i, k := range keys {
			if mine[i].Repo != k.repo || mine[i].Key != k.key || mine[i].Bytes != k.bytes {
				t.Fatalf("row %d = %+v, want %+v", i, mine[i], k)
			}
		}

		// Keyset resume: starting after the first row skips exactly it.
		page, err := as.ListFleetLFSObjects(ctx, keys[0].repo, keys[0].key, lfsReclaimPageCap)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page {
			if r.Repo == keys[0].repo && r.Key == keys[0].key {
				t.Fatalf("cursor-resumed page re-listed the cursor row: %+v", r)
			}
		}
	})
}

// TestAbsenceListFleetPacks_BytesRoundtrip verifies RegisterPacks and
// ReplacePacks persist the registered pack size (issue #101) and that rows
// registered before the packs.bytes migration read back as 0 (unknown).
func TestAbsenceListFleetPacks_BytesRoundtrip(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		as := absenceTestStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		p1 := k.pack(k.base(), "a1", 111)
		p2 := k.pack(k.base(), "a2", 222)
		k.register(k.base(), p1, p2)

		folded := k.pack(k.base(), "f1", 300)
		if won, err := s.ReplacePacks(ctx, k.base(), []Pack{p1, p2}, folded); err != nil || !won {
			t.Fatalf("ReplacePacks = (%v, %v), want won", won, err)
		}

		rows := listPacksFor(t, as, k.base())
		if len(rows) != 1 || rows[0].ID != folded.ID || rows[0].Key != folded.Key || rows[0].Bytes != folded.Size {
			t.Fatalf("post-fold rows = %+v, want one %+v", rows, folded)
		}

		// A zero-size registration models a pre-migration row (the column
		// default): reported as unknown, never covered.
		legacy := Pack{ID: "l1", Key: "packs/" + k.base() + "/pack-l1.pack"}
		k.register(k.base(), legacy)
		rows = listPacksFor(t, as, k.base())
		if len(rows) != 2 {
			t.Fatalf("rows = %+v, want 2", rows)
		}
		for _, r := range rows {
			if r.ID == legacy.ID && r.Bytes != 0 {
				t.Fatalf("legacy row bytes = %d, want 0", r.Bytes)
			}
		}
	})
}

// listPacksFor pages the fleet pack listing and filters to repo.
func listPacksFor(t *testing.T, as absenceStore, repo string) []AbsencePackRow {
	t.Helper()
	var mine []AbsencePackRow
	afterRepo, afterID := "", ""
	for {
		page, err := as.ListFleetPacks(context.Background(), afterRepo, afterID, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			return mine
		}
		for _, r := range page {
			if r.Repo == repo {
				mine = append(mine, r)
			}
		}
		last := page[len(page)-1]
		afterRepo, afterID = last.Repo, last.ID
	}
}
