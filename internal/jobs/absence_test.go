// Absence reconciler tests (issue #101): SQLite + fileblob, reusing the
// sweep fixture (same package) for the store/blob pair.
package jobs

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

func testReconciler(t *testing.T, f *sweepFixture, quotaEnforced bool, budget int64) *AbsenceReconciler {
	t.Helper()
	reg, ok := f.store.(AbsenceRegistry)
	if !ok {
		t.Fatalf("store %T lacks the absence reconciliation seams", f.store)
	}
	return NewAbsenceReconciler(reg, f.blobs, quotaEnforced, budget, time.Hour, slog.New(slog.DiscardHandler))
}

func absenceOID(b byte) string { return strings.Repeat(string([]byte{b, b}), 32)[:64] }

func registerLFS(t *testing.T, f *sweepFixture, repo, key string, size int64) {
	t.Helper()
	if err := f.store.RegisterLFSObject(context.Background(), repo, key, size); err != nil {
		t.Fatalf("RegisterLFSObject(%q): %v", key, err)
	}
}

func ledgerReserved(t *testing.T, f *sweepFixture, base string) int64 {
	t.Helper()
	st, err := f.store.QuotaLedgerState(context.Background(), blob.QuotaRepoGroup(base), time.Hour)
	if err != nil {
		t.Fatalf("QuotaLedgerState: %v", err)
	}
	return st.ReservedBytes
}

func adoptionGen(t *testing.T, f *sweepFixture, base string) int64 {
	t.Helper()
	gen, err := f.store.LFSAdoptionGen(context.Background(), base)
	if err != nil {
		t.Fatalf("LFSAdoptionGen: %v", err)
	}
	return gen
}

// TestAbsenceReconcile_AllPresent: physically present rows produce no
// covers, no bumps, and clean counters on both registries.
func TestAbsenceReconcile_AllPresent(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-present"
	lfsKey := blob.LFSKey(base, absenceOID('a'))
	size := f.upload(lfsKey, "lfs-bytes")
	registerLFS(t, f, base, lfsKey, size)
	packKey := blob.PackKey(base, "p1")
	packSize := f.upload(packKey, "pack-bytes")
	if err := f.store.RegisterPacks(context.Background(), base, []ref.Pack{{ID: "p1", Key: packKey, Size: packSize}}); err != nil {
		t.Fatal(err)
	}

	genBefore := adoptionGen(t, f, base)
	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsExamined != 1 || report.PackRowsExamined != 1 ||
		report.LFSRowsAbsent != 0 || report.PackRowsAbsent != 0 || report.CoversCharged != 0 {
		t.Fatalf("report = %+v, want 1+1 examined, none absent", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
	if gen := adoptionGen(t, f, base); gen != genBefore {
		t.Fatalf("adoption gen moved %d -> %d without a cover", genBefore, gen)
	}
}

// TestAbsenceReconcile_LFSAbsentCoversAndBumps is the core issue-#101
// behavior: a registered LFS object missing under both spellings is
// charged as a restore cover visible to admission, the floor generation is
// bumped, and re-running re-charges the same cover row without doubling.
func TestAbsenceReconcile_LFSAbsentCoversAndBumps(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-lfs-lost"
	lost := blob.LFSKey(base, absenceOID('b'))
	registerLFS(t, f, base, lost, 1000)

	genBefore := adoptionGen(t, f, base)
	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsAbsent != 1 || report.CoversCharged != 1 || report.BytesCovered != 1000 {
		t.Fatalf("report = %+v, want 1 absent covered for 1000", report)
	}
	if got := ledgerReserved(t, f, base); got != 1000 {
		t.Fatalf("reserved = %d, want the 1000-byte cover", got)
	}
	if gen := adoptionGen(t, f, base); gen <= genBefore {
		t.Fatalf("adoption gen = %d, want a bump past %d", gen, genBefore)
	}

	// Re-running refreshes the same-key cover row: still one cover's
	// bytes, not two.
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversCharged != 1 {
		t.Fatalf("second pass covers = %d, want 1 (re-charge)", report.CoversCharged)
	}
	if got := ledgerReserved(t, f, base); got != 1000 {
		t.Fatalf("reserved after re-charge = %d, want 1000 (same-key refresh)", got)
	}
}

// TestAbsenceReconcile_AdmissionVisibility: an admission that fit while the
// loss was invisible is denied after one reconciliation pass — the window
// the issue closes.
func TestAbsenceReconcile_AdmissionVisibility(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-admission"
	group := blob.QuotaRepoGroup(base)
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('c')), 1000) // bytes lost

	incoming := []ref.Pack{{ID: "in", Key: blob.PackKey(base, "in"), Size: 500}}
	dec, err := f.store.ReservePackBytes(ctx, group, incoming, ref.NewReservationOwner(), 0, ref.ReserveNoFloorGen, 1500, time.Hour)
	if err != nil || !dec.OK {
		t.Fatalf("pre-reconcile admission = (%+v, %v), want admitted (counter 1000 + 500 <= 1500)", dec, err)
	}

	r := testReconciler(t, f, true, 1000)
	if _, err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	dec, err = f.store.ReservePackBytes(ctx, group, incoming, ref.NewReservationOwner(), 0, ref.ReserveNoFloorGen, 1500, time.Hour)
	if err != nil {
		t.Fatalf("post-reconcile ReservePackBytes: %v", err)
	}
	if dec.OK {
		t.Fatalf("post-reconcile admission = %+v, want denied (cover makes the absent 1000 visible)", dec)
	}
}

// TestAbsenceReconcile_TwinPresentNotCovered: a row absent under its
// registered spelling but present under the ADR-41 twin is physically
// listed — classified, reported, never covered.
func TestAbsenceReconcile_TwinPresentNotCovered(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-twin"
	oid := absenceOID('d')
	canonical := blob.LFSKey(base, oid)
	size := f.upload(blob.LFSLegacyKey(base, oid), "legacy-bytes")
	registerLFS(t, f, base, canonical, size)

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsTwinOnly != 1 || report.LFSRowsAbsent != 0 || report.CoversCharged != 0 {
		t.Fatalf("report = %+v, want 1 twin-only, none covered", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
}

// TestAbsenceReconcile_PackAbsent: a missing sized pack is covered; a
// missing pre-migration (bytes 0) pack row is reported as unsized.
func TestAbsenceReconcile_PackAbsent(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-pack-lost"
	packs := []ref.Pack{
		{ID: "sized", Key: blob.PackKey(base, "sized"), Size: 700},
		{ID: "unsized", Key: blob.PackKey(base, "unsized")}, // pre-#101 row: bytes 0
	}
	if err := f.store.RegisterPacks(context.Background(), base, packs); err != nil {
		t.Fatal(err)
	}

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.PackRowsAbsent != 2 || report.PackRowsUnsized != 1 || report.CoversCharged != 1 || report.BytesCovered != 700 {
		t.Fatalf("report = %+v, want 2 absent (1 unsized), one 700-byte cover", report)
	}
	if got := ledgerReserved(t, f, base); got != 700 {
		t.Fatalf("reserved = %d, want 700", got)
	}
}

// TestAbsenceReconcile_BudgetResume: a budget-limited pass advances the
// flat cursor and the next pass resumes strictly after it, reaching rows
// past the first pass's horizon.
func TestAbsenceReconcile_BudgetResume(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-budget"
	first := blob.LFSKey(base, absenceOID('a'))
	second := blob.LFSKey(base, absenceOID('b')) // sorts after first; bytes lost
	size := f.upload(first, "present-bytes")
	registerLFS(t, f, base, first, size)
	registerLFS(t, f, base, second, 400)

	// Budget 2: the leading LFS phase gets 1 unit per pass — one row each.
	r := testReconciler(t, f, true, 2)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.LFSRowsExamined != 1 || report.LFSRowsAbsent != 0 {
		t.Fatalf("pass 1 report = %+v, want the present first row only", report)
	}
	r.passes = 0 // pin the LFS phase as leader; alternation is tested separately
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.LFSRowsExamined != 1 || report.LFSRowsAbsent != 1 || report.CoversCharged != 1 {
		t.Fatalf("pass 2 report = %+v, want the resumed absent row covered", report)
	}
}

// TestAbsenceReconcile_PhaseAlternation (Codex round 1, P2): a twin probe
// can overdraw the leading LFS allocation at degenerate budgets; the
// leading phase alternates so the pack registry still gets examined every
// other pass. Quota is disabled so the refresh set cannot skew budgets.
func TestAbsenceReconcile_PhaseAlternation(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-alternate"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('a')), 300) // absent: settle costs 2
	packKey := blob.PackKey(base, "p1")
	packSize := f.upload(packKey, "pack-bytes")
	if err := f.store.RegisterPacks(context.Background(), base, []ref.Pack{{ID: "p1", Key: packKey, Size: packSize}}); err != nil {
		t.Fatal(err)
	}

	r := testReconciler(t, f, false, 2)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.PackRowsExamined != 0 || report.LFSRowsExamined != 1 {
		t.Fatalf("pass 1 report = %+v, want the overdrawing LFS row to consume the pass", report)
	}
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.PackRowsExamined != 1 {
		t.Fatalf("pass 2 report = %+v, want the pack phase to lead and examine its row", report)
	}
}

// TestAbsenceReconcile_RefreshKeepsCoversAlive (Codex round 1, P1): a
// discovered loss re-charges its cover every pass through the known-absent
// refresh, independent of where the bounded rotation happens to be.
func TestAbsenceReconcile_RefreshKeepsCoversAlive(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-refresh"
	for _, b := range []byte{'a', 'b', 'c'} {
		key := blob.LFSKey(base, absenceOID(b))
		size := f.upload(key, "present-"+string(b))
		registerLFS(t, f, base, key, size)
	}
	lost := blob.LFSKey(base, absenceOID('f')) // sorts after the present rows
	registerLFS(t, f, base, lost, 900)

	r := testReconciler(t, f, true, 30)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.CoversCharged != 1 || report.AbsentTracked != 1 {
		t.Fatalf("pass 1 report = %+v, want the loss covered and tracked", report)
	}

	// Pass 2 with budget 6: the refresh (3 units) re-charges the cover;
	// the rotation's remainder is exhausted by the three present rows and
	// never reaches the lost row itself.
	r.budget = 6
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversCharged != 1 || report.AbsentTracked != 1 {
		t.Fatalf("pass 2 report = %+v, want the refresh to re-charge the cover", report)
	}
	if got := ledgerReserved(t, f, base); got != 900 {
		t.Fatalf("reserved = %d, want the refreshed 900-byte cover", got)
	}
}

// TestAbsenceReconcile_RefreshDropsRemovedRows: a tracked loss whose
// registry row disappears (repack fold, retirement, purge completion) is
// dropped from the refresh set instead of being covered forever.
func TestAbsenceReconcile_RefreshDropsRemovedRows(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-refresh-drop"
	lostPack := ref.Pack{ID: "old", Key: blob.PackKey(base, "old"), Size: 500}
	if err := f.store.RegisterPacks(ctx, base, []ref.Pack{lostPack}); err != nil {
		t.Fatal(err)
	}

	r := testReconciler(t, f, true, 100)
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.PackRowsAbsent != 1 || report.AbsentTracked != 1 {
		t.Fatalf("pass 1 report = %+v, want the pack loss tracked", report)
	}

	// A repack fold replaces the lost row; the successor's bytes exist.
	folded := ref.Pack{ID: "new", Key: blob.PackKey(base, "new")}
	folded.Size = f.upload(folded.Key, "folded-pack-bytes")
	if won, err := f.store.ReplacePacks(ctx, base, []ref.Pack{lostPack}, folded); err != nil || !won {
		t.Fatalf("ReplacePacks = (%v, %v), want won", won, err)
	}

	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.AbsentTracked != 0 || report.CoversCharged != 0 || report.PackRowsAbsent != 0 {
		t.Fatalf("pass 2 report = %+v, want the folded-away row dropped from the refresh set", report)
	}
}

// TestAbsenceReconcile_RefreshDropsHealedRows: a tracked loss whose bytes
// return leaves the refresh set on the next pass.
func TestAbsenceReconcile_RefreshDropsHealedRows(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-refresh-heal"
	lost := blob.LFSKey(base, absenceOID('d'))
	registerLFS(t, f, base, lost, 12)

	r := testReconciler(t, f, true, 100)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.AbsentTracked != 1 {
		t.Fatalf("pass 1 report = %+v, want the loss tracked", report)
	}

	if got := f.upload(lost, "healed-bytes"); got != 12 {
		t.Fatalf("restored %d bytes, want 12", got)
	}
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.AbsentTracked != 0 || report.CoversCharged != 0 {
		t.Fatalf("pass 2 report = %+v, want the healed row dropped from the refresh set", report)
	}
}

// TestAbsenceReconcile_PurgedRepoSkipped: rows of a purge-fenced repo are
// skipped — the purge drops them and reaps their blobs.
func TestAbsenceReconcile_PurgedRepoSkipped(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-purged"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('e')), 300)
	fencer, ok := f.store.(interface {
		FenceRepoPurge(ctx context.Context, base string) error
	})
	if !ok {
		t.Fatalf("store %T lacks FenceRepoPurge", f.store)
	}
	if err := fencer.FenceRepoPurge(context.Background(), base); err != nil {
		t.Fatal(err)
	}

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.RowsSkippedPurged != 1 || report.LFSRowsAbsent != 0 || report.CoversCharged != 0 {
		t.Fatalf("report = %+v, want the fenced row skipped", report)
	}
}

// TestAbsenceReconcile_QuotaDisabled: detection still reports, but nothing
// is charged or bumped when quota admission is off.
func TestAbsenceReconcile_QuotaDisabled(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-noquota"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('f')), 800)

	genBefore := adoptionGen(t, f, base)
	r := testReconciler(t, f, false, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsAbsent != 1 || report.CoversCharged != 0 || report.QuotaEnforced {
		t.Fatalf("report = %+v, want detection without covers", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
	if gen := adoptionGen(t, f, base); gen != genBefore {
		t.Fatalf("adoption gen moved %d -> %d with quota disabled", genBefore, gen)
	}
}

// TestAbsenceReconcile_ZeroSizeSkipped: zero-size registered objects
// deflate nothing and are settled without probes or covers.
func TestAbsenceReconcile_ZeroSizeSkipped(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-zero"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('9')), 0)

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsExamined != 1 || report.LFSRowsAbsent != 0 || report.CoversCharged != 0 {
		t.Fatalf("report = %+v, want the zero-size row examined and skipped", report)
	}
}
