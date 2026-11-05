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

	"github.com/jackc/pgx/v5"
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
		// Quota reservations are fenced too (PR #105 round 9): admission
		// must fail before a late writer can mint a reservation row that
		// outlives the purge's ledger cleanup.
		if _, err := s.ReservePackBytes(ctx, group, []Pack{{ID: "lr", Key: "packs/" + base + "/pack-lr.pack", Size: 1}},
			NewReservationOwner(), 0, 0, 1<<30, time.Hour); !errors.Is(err, ErrRepoPurged) {
			t.Fatalf("ReservePackBytes(fenced) = %v, want ErrRepoPurged", err)
		}
		// Adoption claims are the last row-creating path (round 13): a
		// fenced repo's claim silently fails, so no TTL'd row can be
		// minted behind a completed purge.
		if claimed, _, err := s.TryAdoptionClaim(ctx, base, "lfs/"+base+"/"+strings.Repeat("e", 64)); err != nil || claimed {
			t.Fatalf("TryAdoptionClaim(fenced) = (%v, %v), want no claim", claimed, err)
		}
		if claimed, _, err := s.TryAdoptionClaim(ctx, other, "lfs/"+other+"/"+strings.Repeat("e", 64)); err != nil || !claimed {
			t.Fatalf("control TryAdoptionClaim = (%v, %v), want claimed", claimed, err)
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
		// A post-cleanup generation bump (a pre-delete writer's repair
		// handoff, PR #105 round 10) must not recreate the gens row.
		if err := s.BumpLFSAdoptionGen(ctx, base); err != nil {
			t.Fatalf("BumpLFSAdoptionGen(fenced): %v", err)
		}
		if gen, err := s.LFSAdoptionGen(ctx, base); err != nil || gen != 0 {
			t.Fatalf("gens row recreated after purge: gen=%d err=%v", gen, err)
		}
		if err := s.BumpLFSAdoptionGen(ctx, other); err != nil {
			t.Fatalf("control BumpLFSAdoptionGen: %v", err)
		}
		if gen, err := s.LFSAdoptionGen(ctx, other); err != nil || gen == 0 {
			t.Fatalf("control gens bump lost: gen=%d err=%v", gen, err)
		}
	})
}

// TestReplacePacksPurgeFence: a repack fold that uploaded its pack before
// the purge fenced the repository must not register it afterwards — a
// purge-aware sweep treats a fenced repo's unregistered blobs as permanent
// garbage and may already have reaped the fold's bytes. The refused fold
// changes nothing; the unfenced control repo still folds.
func TestReplacePacksPurgeFence(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ps, ok := s.(purgeStore)
		if !ok {
			t.Fatalf("store %T lacks purge seams", s)
		}
		ctx := context.Background()
		base := t.Name()
		other := base + "-other"
		packsOf := func(repo string) []Pack {
			return []Pack{
				{ID: "a", Key: "packs/" + repo + "/pack-a.pack", Size: 10},
				{ID: "b", Key: "packs/" + repo + "/pack-b.pack", Size: 20},
			}
		}
		foldOf := func(repo string) Pack {
			return Pack{ID: "fold", Key: "packs/" + repo + "/pack-fold.pack", Size: 25}
		}
		for _, repo := range []string{base, base + "+import", other} {
			if err := s.RegisterPacks(ctx, repo, packsOf(repo)); err != nil {
				t.Fatalf("RegisterPacks(%s): %v", repo, err)
			}
		}
		if err := ps.FenceRepoPurge(ctx, base); err != nil {
			t.Fatalf("FenceRepoPurge: %v", err)
		}

		for _, repo := range []string{base, base + "+import"} {
			ledgerBefore := ledgerState(t, s, purgeGroupOf(base))
			genBefore, err := s.GetGeneration(ctx, repo)
			if err != nil {
				t.Fatalf("GetGeneration(%s): %v", repo, err)
			}
			won, err := s.ReplacePacks(ctx, repo, packsOf(repo), foldOf(repo))
			if won || !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("ReplacePacks(fenced %s) = (%v, %v), want ErrRepoPurged", repo, won, err)
			}
			packs, err := s.ListPacks(ctx, repo)
			if err != nil || len(packs) != 2 || packs[0].ID != "a" || packs[1].ID != "b" {
				t.Fatalf("packs of %s after refused fold: %v err=%v (want a, b)", repo, packs, err)
			}
			if has, err := s.HasPack(ctx, repo, foldOf(repo).Key); err != nil || has {
				t.Fatalf("fold row of %s registered: has=%v err=%v", repo, has, err)
			}
			if retained, err := s.IsRetainedPack(ctx, repo, packsOf(repo)[0].Key); err != nil || retained {
				t.Fatalf("old row of %s retained by refused fold: %v err=%v", repo, retained, err)
			}
			if got := ledgerState(t, s, purgeGroupOf(base)); got != ledgerBefore {
				t.Fatalf("ledger moved by refused fold of %s: %+v -> %+v", repo, ledgerBefore, got)
			}
			if gen, err := s.GetGeneration(ctx, repo); err != nil || gen != genBefore {
				t.Fatalf("generation of %s moved by refused fold: %d -> %d err=%v", repo, genBefore, gen, err)
			}
		}

		if won, err := s.ReplacePacks(ctx, other, packsOf(other), foldOf(other)); err != nil || !won {
			t.Fatalf("control ReplacePacks = (%v, %v), want won", won, err)
		}
		if packs, err := s.ListPacks(ctx, other); err != nil || len(packs) != 1 || packs[0].ID != "fold" {
			t.Fatalf("control packs after fold: %v err=%v (want fold)", packs, err)
		}
	})
}

// TestReplacePacksSerializesWithPurgeFence (PG only — SQLite's IMMEDIATE
// transactions serialize every writer already, and S3-WAL's fold and fence
// CAS the same manifest): a fold must read the fence under the base-ledger
// lock, or a fold that reads "not fenced" while FenceRepoPurge is still
// open would register its pack after the fence commits.
func TestReplacePacksSerializesWithPurgeFence(t *testing.T) {
	ctx := context.Background()
	s := pgTestStore(t)
	base := t.Name()
	old := []Pack{
		{ID: "a", Key: "packs/" + base + "/pack-a.pack", Size: 10},
		{ID: "b", Key: "packs/" + base + "/pack-b.pack", Size: 20},
	}
	if err := s.RegisterPacks(ctx, base, old); err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(ctx, pgTestURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// FenceRepoPurge's transaction, held open just before its commit.
	if err := lockBaseLedger(ctx, tx, base); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO purged_repos (repo) VALUES ($1)`, base); err != nil {
		t.Fatal(err)
	}

	type result struct {
		won bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		won, err := s.ReplacePacks(ctx, base, old, Pack{ID: "fold", Key: "packs/" + base + "/pack-fold.pack", Size: 25})
		done <- result{won, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("ReplacePacks completed while the fence transaction held the base-ledger lock: (%v, %v)", r.won, r.err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if r := <-done; r.won || !errors.Is(r.err, ErrRepoPurged) {
		t.Fatalf("ReplacePacks after the fence committed = (%v, %v), want ErrRepoPurged", r.won, r.err)
	}
}

func ledgerState(t *testing.T, s Store, group []string) PackQuotaLedgerState {
	t.Helper()
	state, err := s.QuotaLedgerState(context.Background(), group, time.Hour)
	if err != nil {
		t.Fatalf("QuotaLedgerState: %v", err)
	}
	return state
}
