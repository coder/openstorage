// Repository-purge row seam tests (issue #52): tombstoning, isolation
// between repos, idempotence, and ledger cleanup must behave identically on
// both backends. Bodies fan out via runRefBackends.
package ref

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// purgeStore is the concrete-backend purge surface under test (the seams
// deliberately live off RefStore; jobs.RepoPurgeRegistry mirrors this).
type purgeStore interface {
	Store
	FenceRepoPurge(ctx context.Context, base string) error
	DropRepoRefs(ctx context.Context, group []string) (int64, error)
	RetireRepoPacks(ctx context.Context, group []string) (int64, error)
	DropRepoLFSObjects(ctx context.Context, group []string) (int64, error)
	CountRepoRows(ctx context.Context, group []string) (int64, error)
	PurgeRepoLedger(ctx context.Context, group []string) (int64, error)
}

func purgeGroupOf(base string) []string {
	return []string{base, base + "+ephemeral", base + "+import"}
}

func TestRepoPurgeSeams(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ps, ok := s.(purgeStore)
		if !ok {
			t.Fatalf("store %T lacks purge seams", s)
		}
		ctx := context.Background()
		base := t.Name()
		group := purgeGroupOf(base)
		other := base + "-other" // isolation control (never purged)
		otherGroup := purgeGroupOf(other)

		lfsOID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

		// Seed refs across the group and the control repo.
		for _, repo := range []string{base, base + "+ephemeral", other} {
			for _, name := range []string{"refs/heads/main", "refs/heads/dev"} {
				if won, err := s.CASRef(ctx, repo, name, ZeroSHA, "1111111111111111111111111111111111111111"); err != nil || !won {
					t.Fatalf("CASRef(%s, %s): won=%v err=%v", repo, name, won, err)
				}
			}
		}
		// Seed packs: two in base (one later superseded by a fold so a
		// REAL retained row with bytes exists), one namespaced, one in
		// the control repo.
		p1 := Pack{ID: "p1", Key: "packs/" + base + "/pack-p1.pack", Size: 100}
		p2 := Pack{ID: "p2", Key: "packs/" + base + "/pack-p2.pack", Size: 40}
		pn := Pack{ID: "pn", Key: "packs/" + base + "+import/pack-pn.pack", Size: 7}
		po := Pack{ID: "po", Key: "packs/" + other + "/pack-po.pack", Size: 11}
		if err := s.RegisterPacks(ctx, base, []Pack{p1, p2}); err != nil {
			t.Fatalf("RegisterPacks(base): %v", err)
		}
		if err := s.RegisterPacks(ctx, base+"+import", []Pack{pn}); err != nil {
			t.Fatalf("RegisterPacks(ns): %v", err)
		}
		if err := s.RegisterPacks(ctx, other, []Pack{po}); err != nil {
			t.Fatalf("RegisterPacks(other): %v", err)
		}
		fold := Pack{ID: "fold", Key: "packs/" + base + "/pack-fold.pack", Size: 100}
		if won, err := s.ReplacePacks(ctx, base, []Pack{p1}, fold); err != nil || !won {
			t.Fatalf("ReplacePacks: won=%v err=%v", won, err)
		}
		// Seed LFS rows for base and control.
		if err := s.RegisterLFSObject(ctx, base, "lfs/"+base+"/"+lfsOID, 9); err != nil {
			t.Fatalf("RegisterLFSObject(base): %v", err)
		}
		if err := s.RegisterLFSObject(ctx, other, "lfs/"+other+"/"+lfsOID, 3); err != nil {
			t.Fatalf("RegisterLFSObject(other): %v", err)
		}
		// One reservation row that the ledger purge must remove.
		if _, err := s.ReservePackBytes(ctx, group, []Pack{{ID: "r", Key: "packs/" + base + "/pack-r.pack", Size: 5}},
			NewReservationOwner(), 0, 0, 1<<30, time.Hour); err != nil {
			t.Fatalf("ReservePackBytes: %v", err)
		}

		usageBefore := ledgerState(t, s, group).UsageBytes
		if usageBefore == 0 {
			t.Fatal("seed produced no usage bytes")
		}

		// Phase 0: the write fence. Once fenced, pack and LFS
		// registrations for ANY group member fail with ErrRepoPurged —
		// including namespaced keys, which fence by their base — while
		// the control repo keeps writing. Idempotent.
		if err := ps.FenceRepoPurge(ctx, base); err != nil {
			t.Fatalf("FenceRepoPurge: %v", err)
		}
		if err := ps.FenceRepoPurge(ctx, base); err != nil {
			t.Fatalf("FenceRepoPurge rerun: %v", err)
		}
		late := Pack{ID: "late", Key: "packs/" + base + "/pack-late.pack", Size: 1}
		if err := s.RegisterPacks(ctx, base, []Pack{late}); !errors.Is(err, ErrRepoPurged) {
			t.Fatalf("RegisterPacks(fenced base) = %v, want ErrRepoPurged", err)
		}
		if err := s.RegisterPacks(ctx, base+"+import", []Pack{{ID: "ln", Key: "packs/" + base + "+import/pack-ln.pack"}}); !errors.Is(err, ErrRepoPurged) {
			t.Fatalf("RegisterPacks(fenced namespace) = %v, want ErrRepoPurged", err)
		}
		lateOID := strings.Repeat("b", 64)
		if err := s.RegisterLFSObject(ctx, base, "lfs/"+base+"/"+lateOID, 1); !errors.Is(err, ErrRepoPurged) {
			t.Fatalf("RegisterLFSObject(fenced) = %v, want ErrRepoPurged", err)
		}
		// Ref CAS is fenced too (PR #105 round 4): REST branch/tag
		// creation reaches CASRef without registering a pack, so create,
		// update, AND delete must all reject on a fenced repo.
		for _, cas := range []struct{ tag, old, new string }{
			{"create", ZeroSHA, "2222222222222222222222222222222222222222"},
			{"update", "1111111111111111111111111111111111111111", "2222222222222222222222222222222222222222"},
			{"delete", "1111111111111111111111111111111111111111", ZeroSHA},
		} {
			if _, err := s.CASRef(ctx, base, "refs/heads/main", cas.old, cas.new); !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("CASRef %s (fenced) = %v, want ErrRepoPurged", cas.tag, err)
			}
			if _, err := s.CASRef(ctx, base+"+ephemeral", "refs/heads/x", cas.old, cas.new); !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("CASRef %s (fenced namespace) = %v, want ErrRepoPurged", cas.tag, err)
			}
		}
		if err := s.RegisterPacks(ctx, other, []Pack{{ID: "ok", Key: "packs/" + other + "/pack-ok.pack", Size: 2}}); err != nil {
			t.Fatalf("control RegisterPacks after fence: %v", err)
		}
		if won, err := s.CASRef(ctx, other, "refs/heads/post-fence", ZeroSHA, "3333333333333333333333333333333333333333"); err != nil || !won {
			t.Fatalf("control CASRef after fence: won=%v err=%v", won, err)
		}

		// Phase 1: refs die, group-wide, control untouched, idempotent.
		if n, err := ps.DropRepoRefs(ctx, group); err != nil || n != 4 {
			t.Fatalf("DropRepoRefs: n=%d err=%v (want 4)", n, err)
		}
		if refs, err := s.ListRefs(ctx, base); err != nil || len(refs) != 0 {
			t.Fatalf("base refs after drop: %v err=%v", refs, err)
		}
		if refs, err := s.ListRefs(ctx, other); err != nil || len(refs) != 3 {
			t.Fatalf("control refs after drop: %v err=%v (want 3)", refs, err)
		}
		if n, err := ps.DropRepoRefs(ctx, group); err != nil || n != 0 {
			t.Fatalf("DropRepoRefs rerun: n=%d err=%v (want 0)", n, err)
		}

		// Phase 2: packs rows become retained tombstones; the fold's REAL
		// retained row keeps its bytes; usage counter stays credited.
		if n, err := ps.RetireRepoPacks(ctx, group); err != nil || n != 3 {
			t.Fatalf("RetireRepoPacks: n=%d err=%v (want 3: p2, fold, pn)", n, err)
		}
		for _, k := range []struct{ repo, key string }{
			{base, p2.Key}, {base, fold.Key}, {base + "+import", pn.Key},
		} {
			if has, err := s.HasPack(ctx, k.repo, k.key); err != nil || has {
				t.Fatalf("HasPack(%s) after retire: has=%v err=%v", k.key, has, err)
			}
			if retained, err := s.IsRetainedPack(ctx, k.repo, k.key); err != nil || !retained {
				t.Fatalf("IsRetainedPack(%s) after retire: %v err=%v", k.key, retained, err)
			}
		}
		if has, err := s.HasPack(ctx, other, po.Key); err != nil || !has {
			t.Fatalf("control pack after retire: has=%v err=%v", has, err)
		}
		if got := ledgerState(t, s, group).UsageBytes; got != usageBefore {
			t.Fatalf("usage debited by retire: got %d want %d", got, usageBefore)
		}
		// p1 was superseded by the fold BEFORE the purge: its retained row
		// carries real bytes and must survive the retire upsert untouched.
		if got, err := s.ReclaimRetainedPack(ctx, base, p1.Key); err != nil || got != p1.Size {
			t.Fatalf("ReclaimRetainedPack(p1): got %d err=%v (want %d)", got, err, p1.Size)
		}
		if n, err := ps.RetireRepoPacks(ctx, group); err != nil || n != 0 {
			t.Fatalf("RetireRepoPacks rerun: n=%d err=%v (want 0)", n, err)
		}

		// Phase 3: LFS rows die; control survives; idempotent.
		if n, err := ps.DropRepoLFSObjects(ctx, group); err != nil || n != 1 {
			t.Fatalf("DropRepoLFSObjects: n=%d err=%v (want 1)", n, err)
		}
		if has, err := s.HasLFSObject(ctx, base, "lfs/"+base+"/"+lfsOID); err != nil || has {
			t.Fatalf("HasLFSObject after drop: has=%v err=%v", has, err)
		}
		if has, err := s.HasLFSObject(ctx, other, "lfs/"+other+"/"+lfsOID); err != nil || !has {
			t.Fatalf("control lfs after drop: has=%v err=%v", has, err)
		}
		if n, err := ps.DropRepoLFSObjects(ctx, group); err != nil || n != 0 {
			t.Fatalf("DropRepoLFSObjects rerun: n=%d err=%v (want 0)", n, err)
		}

		// Phase 5 verification read: purged group empty, control not.
		if n, err := ps.CountRepoRows(ctx, group); err != nil || n != 0 {
			t.Fatalf("CountRepoRows(group): n=%d err=%v (want 0)", n, err)
		}
		if n, err := ps.CountRepoRows(ctx, otherGroup); err != nil || n != 6 {
			t.Fatalf("CountRepoRows(other): n=%d err=%v (want 6: 3 refs + 2 packs + lfs)", n, err)
		}

		// Phase 6: ledger rows die wholesale; control accounting intact.
		if n, err := ps.PurgeRepoLedger(ctx, group); err != nil || n == 0 {
			t.Fatalf("PurgeRepoLedger: n=%d err=%v (want > 0)", n, err)
		}
		state := ledgerState(t, s, group)
		if state.UsageBytes != 0 || state.ReservedBytes != 0 || state.RetainedBytes != 0 {
			t.Fatalf("ledger state after purge: %+v (want zeros)", state)
		}
		if gen, err := s.LFSAdoptionGen(ctx, base); err != nil || gen != 0 {
			t.Fatalf("LFSAdoptionGen after purge: %d err=%v", gen, err)
		}
		if gen, err := s.GetGeneration(ctx, base); err != nil || gen != 0 {
			t.Fatalf("GetGeneration after purge: %d err=%v", gen, err)
		}
		if got := ledgerState(t, s, otherGroup).UsageBytes; got != po.Size+3+2 {
			t.Fatalf("control usage after purge: %d (want %d)", got, po.Size+3+2)
		}
		if n, err := ps.PurgeRepoLedger(ctx, group); err != nil || n != 0 {
			t.Fatalf("PurgeRepoLedger rerun: n=%d err=%v (want 0)", n, err)
		}
	})
}

func ledgerState(t *testing.T, s Store, group []string) PackQuotaLedgerState {
	t.Helper()
	state, err := s.QuotaLedgerState(context.Background(), group, time.Hour)
	if err != nil {
		t.Fatalf("QuotaLedgerState: %v", err)
	}
	return state
}
