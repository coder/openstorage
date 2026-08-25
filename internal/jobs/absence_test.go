// Absence reconciler tests (issue #101): SQLite + fileblob, reusing the
// sweep fixture (same package) for the store/blob pair.
package jobs

import (
	"context"
	"fmt"
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
	// The renewal sub-phase re-charges the base aggregate once before
	// re-verification discovers the fold and drops the row (conservative
	// over-cover for at most one TTL); the tracking itself must be gone.
	if report.AbsentTracked != 0 || report.CoversCharged != 1 || report.PackRowsAbsent != 0 {
		t.Fatalf("pass 2 report = %+v, want the folded-away row dropped from the refresh set", report)
	}
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversCharged != 0 {
		t.Fatalf("pass 3 report = %+v, want no covers once nothing is tracked", report)
	}
}

// TestAbsenceReconcile_RefreshRoundRobin (Codex rounds 2+3, P1): a tracked
// set larger than one pass's refresh capacity renews deterministically —
// the ring cursor persists across passes, so the entries a pass could not
// reach are exactly where the next pass starts.
func TestAbsenceReconcile_RefreshRoundRobin(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-roundrobin"
	for i := 0; i < 18; i++ {
		oid := strings.Repeat("a", 62) + string([]byte{'0' + byte(i/10), '0' + byte(i%10)})
		registerLFS(t, f, base, blob.LFSKey(base, oid), 10)
	}

	// Refresh half-budget 50 at the 3-unit worst cost refreshes 17 of the
	// 18 tracked losses per pass (the last entry may start with any
	// budget remainder, overdrawing by design like the rotations).
	r := testReconciler(t, f, true, 100)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.LFSRowsAbsent != 18 || report.AbsentTracked != 18 || report.AbsentUntracked != 0 {
		t.Fatalf("pass 1 report = %+v, want all 18 losses tracked", report)
	}
	// All 18 losses aggregate into ONE per-base cover row totaling their
	// bytes (Codex round 4: per-key rows would collide with the per-base
	// active-reservation-row cap on big incidents).
	if got := ledgerReserved(t, f, base); got != 180 {
		t.Fatalf("reserved = %d, want the 180-byte base aggregate", got)
	}
	for pass := 2; pass <= 3; pass++ {
		report, err = r.Run(context.Background())
		if err != nil {
			t.Fatalf("Run %d: %v", pass, err)
		}
		if report.AbsentRefreshed != 17 || report.AbsentTracked != 18 {
			t.Fatalf("pass %d report = %+v, want 17 of 18 tracked losses refreshed (ring capacity)", pass, report)
		}
	}
}

// stubAbsenceRegistry/stubStatter drive the ring mechanics without a store:
// every row stays registered and absent, and covers always admit.
type stubAbsenceRegistry struct{}

func (stubAbsenceRegistry) ListFleetLFSObjects(context.Context, string, string, int) ([]ref.AbsenceLFSRow, error) {
	return nil, nil
}

func (stubAbsenceRegistry) ListFleetPacks(context.Context, string, string, int) ([]ref.AbsencePackRow, error) {
	return nil, nil
}
func (stubAbsenceRegistry) IsRepoPurged(context.Context, string) (bool, error)    { return false, nil }
func (stubAbsenceRegistry) BumpLFSAdoptionGen(context.Context, string) error      { return nil }
func (stubAbsenceRegistry) HasPack(context.Context, string, string) (bool, error) { return true, nil }
func (stubAbsenceRegistry) HasLFSObject(context.Context, string, string) (bool, error) {
	return true, nil
}

func (stubAbsenceRegistry) ReservePackBytes(context.Context, []string, []ref.Pack, string, int64, int64, int64, time.Duration) (ref.ReserveDecision, error) {
	return ref.ReserveDecision{OK: true}, nil
}

// recordingAbsenceRegistry wraps the stub to capture reserved keys.
type recordingAbsenceRegistry struct {
	stubAbsenceRegistry
	reserved []string
}

func (r *recordingAbsenceRegistry) ReservePackBytes(_ context.Context, _ []string, packs []ref.Pack, _ string, _, _, _ int64, _ time.Duration) (ref.ReserveDecision, error) {
	for _, p := range packs {
		r.reserved = append(r.reserved, p.Key)
	}
	return ref.ReserveDecision{OK: true}, nil
}

type stubStatter struct{ stats []string }

func (s *stubStatter) Stat(_ context.Context, key string) (int64, error) {
	s.stats = append(s.stats, key)
	return 0, blob.ErrNotFound
}

// TestAbsenceRefreshRingOrder: the refresh visits tracked losses in ring
// order across calls, resuming at the persistent cursor and wrapping.
func TestAbsenceRefreshRingOrder(t *testing.T) {
	statter := &stubStatter{}
	r := NewAbsenceReconciler(stubAbsenceRegistry{}, statter, true, 100, time.Hour, slog.New(slog.DiscardHandler))
	keys := make([]string, 5)
	var report AbsenceReport
	for i := range keys {
		oid := strings.Repeat("b", 63) + string([]byte{'0' + byte(i)})
		keys[i] = blob.LFSKey("base", oid)
		r.trackAbsent(absentRow{repo: "base", key: keys[i], bytes: 10, registered: 10}, &report)
	}

	refresh := func(budget int64) []string {
		statter.stats = nil
		var errs boundedErrs
		rep := AbsenceReport{QuotaEnforced: true}
		r.refreshKnownAbsent(context.Background(), budget, &rep, newAbsencePassState(), &errs)
		if err := errs.join(); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		var primaries []string
		for _, s := range statter.stats {
			if strings.Contains(s, "/o2/") {
				primaries = append(primaries, s)
			}
		}
		return primaries
	}
	assertSeq := func(got, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("visited %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("visited %v, want %v", got, want)
			}
		}
	}
	// Budget 6 = two rows per call at the 3-unit worst cost.
	assertSeq(refresh(6), []string{keys[0], keys[1]})
	assertSeq(refresh(6), []string{keys[2], keys[3]})
	assertSeq(refresh(6), []string{keys[4], keys[0]}) // wrapped
}

// TestAbsencePassTimeoutClampedToTTL (Codex round 3, security): the pass
// wall-clock cap tightens to a quarter of the cover TTL so a long scan can
// never hold the next renewal past a live cover's expiry.
func TestAbsencePassTimeoutClampedToTTL(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	r := NewAbsenceReconciler(stubAbsenceRegistry{}, &stubStatter{}, true, 100, 40*time.Minute, log)
	if r.passTimeout != 10*time.Minute {
		t.Fatalf("passTimeout = %v with a 40m TTL, want 10m (TTL/4)", r.passTimeout)
	}
	r = NewAbsenceReconciler(stubAbsenceRegistry{}, &stubStatter{}, true, 100, 8*time.Hour, log)
	if r.passTimeout != time.Hour {
		t.Fatalf("passTimeout = %v with an 8h TTL, want the 1h ceiling", r.passTimeout)
	}
}

// TestAbsenceShrinkRotationCoalesced (Codex round 8): heals interleaved
// with re-charges within one pass must mint at most ONE fresh aggregate
// incarnation per base — eager per-heal rotation could fill the per-base
// reservation-row cap in a single pass.
func TestAbsenceShrinkRotationCoalesced(t *testing.T) {
	reg := &recordingAbsenceRegistry{}
	r := NewAbsenceReconciler(reg, &stubStatter{}, true, 1000, time.Hour, slog.New(slog.DiscardHandler))
	var report AbsenceReport
	base := "repo-coalesce"
	keys := make([]string, 8)
	for i := range keys {
		oid := strings.Repeat("d", 63) + string([]byte{'0' + byte(i)})
		keys[i] = blob.LFSKey(base, oid)
		r.trackAbsent(absentRow{repo: base, key: keys[i], bytes: 10, registered: 10}, &report)
	}

	// One pass state: alternate heal (untrack → shrink) with a re-charge
	// (coverBase), eight times.
	st := newAbsencePassState()
	var errs boundedErrs
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		r.untrackAbsent(keys[i])
		r.coverBase(ctx, base, &report, st, &errs)
	}
	if err := errs.join(); err != nil {
		t.Fatalf("coverBase: %v", err)
	}
	distinct := map[string]bool{}
	for _, k := range reg.reserved {
		distinct[k] = true
	}
	if len(distinct) > 1 {
		t.Fatalf("pass minted %d distinct aggregate incarnations (%v), want at most 1", len(distinct), reg.reserved)
	}
}

// TestAbsenceUntrackSwapRemove: O(1) removal keeps the map and ring
// consistent, and the refresh still renews every survivor.
func TestAbsenceUntrackSwapRemove(t *testing.T) {
	statter := &stubStatter{}
	r := NewAbsenceReconciler(stubAbsenceRegistry{}, statter, true, 100, time.Hour, slog.New(slog.DiscardHandler))
	var report AbsenceReport
	keys := make([]string, 4)
	for i := range keys {
		oid := strings.Repeat("c", 63) + string([]byte{'0' + byte(i)})
		keys[i] = blob.LFSKey("base", oid)
		r.trackAbsent(absentRow{repo: "base", key: keys[i], bytes: 10, registered: 10}, &report)
	}
	r.untrackAbsent(keys[1])
	r.untrackAbsent(keys[1]) // idempotent
	r.untrackAbsent(keys[3])
	if len(r.knownAbsent) != 2 || len(r.refreshRing) != 2 {
		t.Fatalf("tracked=%d ring=%d, want 2/2", len(r.knownAbsent), len(r.refreshRing))
	}
	var errs boundedErrs
	rep := AbsenceReport{QuotaEnforced: true}
	r.refreshKnownAbsent(context.Background(), 100, &rep, newAbsencePassState(), &errs)
	if err := errs.join(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if rep.AbsentRefreshed != 2 {
		t.Fatalf("refreshed %d survivors, want 2", rep.AbsentRefreshed)
	}
}

// TestAbsenceReconcile_TruncatedCovered (Codex round 4, security): a blob
// physically present but SHORTER than its registered size deflates the
// floor by the deficit — covered and tracked exactly like partial absence,
// and released once the full bytes return.
func TestAbsenceReconcile_TruncatedCovered(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-truncated"
	lfsKey := blob.LFSKey(base, absenceOID('7'))
	f.upload(lfsKey, "short")           // 5 physical bytes
	registerLFS(t, f, base, lfsKey, 40) // registered as 40
	packKey := blob.PackKey(base, "trunc")
	f.upload(packKey, "abc") // 3 physical bytes
	if err := f.store.RegisterPacks(context.Background(), base, []ref.Pack{{ID: "trunc", Key: packKey, Size: 9}}); err != nil {
		t.Fatal(err)
	}

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsTruncated != 1 || report.PackRowsTruncated != 1 || report.LFSRowsAbsent != 0 {
		t.Fatalf("report = %+v, want one truncated row per registry", report)
	}
	if got := ledgerReserved(t, f, base); got != (40-5)+(9-3) {
		t.Fatalf("reserved = %d, want the 41-byte deficit aggregate", got)
	}

	// Full bytes return: one final renewal precedes the re-verification
	// that releases the tracking.
	if got := f.upload(lfsKey, strings.Repeat("x", 40)); got != 40 {
		t.Fatalf("healed to %d bytes, want 40", got)
	}
	if got := f.upload(packKey, strings.Repeat("y", 9)); got != 9 {
		t.Fatalf("healed to %d bytes, want 9", got)
	}
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.AbsentTracked != 0 || report.LFSRowsTruncated != 0 || report.PackRowsTruncated != 0 {
		t.Fatalf("pass 2 report = %+v, want healed truncations released", report)
	}
}

// TestAbsenceReconcile_TruncationPartialHeal (Codex round 5, security): a
// tracked truncation stores its DEFICIT, but re-verification must compare
// fresh Stats against the authoritative REGISTERED size — comparing
// against the deficit would untrack a partially healed row while bytes
// are still missing.
func TestAbsenceReconcile_TruncationPartialHeal(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-partial-heal"
	key := blob.LFSKey(base, absenceOID('5'))
	f.upload(key, "short")           // 5 of 40 bytes
	registerLFS(t, f, base, key, 40) // deficit 35

	r := testReconciler(t, f, true, 1000)
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 35 {
		t.Fatalf("reserved = %d, want the 35-byte deficit", got)
	}

	// Partial heal: 25 of 40 bytes now physical. Deficit-relative
	// comparison would see 25 >= 35-deficit... wrong on every axis; the
	// registered-size comparison keeps a 15-byte deficit tracked.
	if got := f.upload(key, strings.Repeat("p", 25)); got != 25 {
		t.Fatalf("healed to %d bytes, want 25", got)
	}
	var report AbsenceReport
	var err error
	for pass := 2; pass <= 3; pass++ {
		if report, err = r.Run(context.Background()); err != nil {
			t.Fatalf("Run %d: %v", pass, err)
		}
	}
	if report.AbsentTracked != 1 || report.LFSRowsTruncated == 0 {
		t.Fatalf("report = %+v, want the 15-byte residual deficit still tracked", report)
	}
	if got := ledgerReserved(t, f, base); got < 15 {
		t.Fatalf("reserved = %d, want at least the residual 15-byte deficit", got)
	}
}

// TestAbsenceReconcile_ShortTwinCovered: a row absent under its registered
// spelling whose twin holds FEWER bytes than registered is a partial loss —
// only the twin's physical size compensates the floor.
func TestAbsenceReconcile_ShortTwinCovered(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-shorttwin"
	oid := absenceOID('8')
	canonical := blob.LFSKey(base, oid)
	f.upload(blob.LFSLegacyKey(base, oid), "tiny") // 4 of 32 bytes survive
	registerLFS(t, f, base, canonical, 32)

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsTruncated != 1 || report.LFSRowsTwinOnly != 0 || report.LFSRowsAbsent != 0 {
		t.Fatalf("report = %+v, want the short twin classified as truncation", report)
	}
	if got := ledgerReserved(t, f, base); got != 28 {
		t.Fatalf("reserved = %d, want the 28-byte deficit", got)
	}
}

// TestAbsenceReconcile_AggregateShrinksOnPartialHeal (Codex round 5): the
// ledger's same-key upsert keeps GREATEST(old, new), so a shrinking
// aggregate must rotate its reservation key — the healed bytes' old row
// expires by TTL while the fresh row charges the exact remaining total.
func TestAbsenceReconcile_AggregateShrinksOnPartialHeal(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-shrink"
	small := blob.LFSKey(base, absenceOID('1'))
	big := blob.LFSKey(base, absenceOID('2'))
	registerLFS(t, f, base, small, 30)
	registerLFS(t, f, base, big, 70)

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	// Discovery charges the aggregate as it grows mid-pass (30, then
	// 100); the same-key upsert nets ONE 100-byte reservation row.
	if report.AbsentTracked != 2 {
		t.Fatalf("pass 1 report = %+v, want both losses tracked", report)
	}
	if got := ledgerReserved(t, f, base); got != 100 {
		t.Fatalf("reserved = %d, want the netted 100-byte aggregate", got)
	}

	// Heal the 70-byte loss. Pass 2's renewal (which precedes
	// re-verification) still charges the pre-heal 100; re-verification
	// then shrinks the aggregate and rotates its key.
	if got := f.upload(big, strings.Repeat("z", 70)); got != 70 {
		t.Fatalf("healed to %d bytes, want 70", got)
	}
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.AbsentTracked != 1 {
		t.Fatalf("pass 2 report = %+v, want the healed loss released", report)
	}

	// Pass 3 renews under the rotated key with the exact 30-byte total;
	// the stale 100-byte row lingers only to its TTL. A GREATEST-pinned
	// single key would renew 100 forever instead.
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.BytesCovered != 30 || report.CoversCharged != 1 {
		t.Fatalf("pass 3 report = %+v, want one 30-byte renewal on the rotated key", report)
	}
	if got := ledgerReserved(t, f, base); got != 130 {
		t.Fatalf("reserved = %d, want 130 (fresh 30-byte row + the dying 100-byte row inside its TTL)", got)
	}
}

// TestAbsenceReconcile_BaseRenewalCapacity (Codex round 5): tracked bases
// are capped at the renewal phase's per-pass allocation so every tracked
// aggregate provably renews every pass; losses in bases beyond it get
// one-shot covers and report as untracked.
func TestAbsenceReconcile_BaseRenewalCapacity(t *testing.T) {
	f := newSweepFixture(t)
	// Budget 100 → base capacity 25 (half the refresh allocation, so
	// revalidation always keeps the other half). Mint 27 bases with one
	// 10-byte absent LFS row each.
	for i := 0; i < 27; i++ {
		base := fmt.Sprintf("repo-cap-%02d", i)
		registerLFS(t, f, base, blob.LFSKey(base, absenceOID('a')), 10)
	}

	r := testReconciler(t, f, true, 100)
	var tracked, untrackedTotal int64
	for pass := 0; pass < 6; pass++ {
		report, err := r.Run(context.Background())
		if err != nil {
			t.Fatalf("Run %d: %v", pass, err)
		}
		tracked = report.AbsentTracked
		untrackedTotal += report.AbsentUntracked
		if tracked == 25 && untrackedTotal > 0 {
			break
		}
	}
	if tracked != 25 {
		t.Fatalf("tracked = %d, want the 25-base renewal capacity", tracked)
	}
	if untrackedTotal == 0 {
		t.Fatal("losses beyond the base capacity must report as untracked one-shot covers")
	}
}

// TestAbsenceReconcile_AggregateKeyNotReusedAfterEmpty (Codex round 6): a
// base whose aggregate empties and re-forms within one TTL must mint a
// FRESH reservation key — a resettable counter would re-adopt the stale
// GREATEST-pinned row and renew the old larger total forever.
func TestAbsenceReconcile_AggregateKeyNotReusedAfterEmpty(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-key-reuse"
	bigKey := blob.LFSKey(base, absenceOID('3'))
	registerLFS(t, f, base, bigKey, 100)

	r := testReconciler(t, f, true, 1000)
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 100 {
		t.Fatalf("reserved = %d, want 100", got)
	}

	// Heal the loss entirely (aggregate empties), then lose a SMALLER
	// object; its fresh aggregate must charge 10 on a new key.
	if got := f.upload(bigKey, strings.Repeat("q", 100)); got != 100 {
		t.Fatalf("healed to %d bytes, want 100", got)
	}
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('4')), 10)
	for pass := 2; pass <= 3; pass++ {
		if _, err := r.Run(context.Background()); err != nil {
			t.Fatalf("Run %d: %v", pass, err)
		}
	}
	// Stale 100-byte row (inside its TTL) + fresh 10-byte row. Key reuse
	// would GREATEST-pin the total at 100 instead.
	if got := ledgerReserved(t, f, base); got != 110 {
		t.Fatalf("reserved = %d, want 110 (dying 100-byte row + fresh 10-byte incarnation)", got)
	}
}

// TestAbsenceRefreshDropsUnrenewedBases (Codex round 6): tracking must
// never outlive its renewal guarantee — a renewal aborted by the pass
// deadline drops the unrenewed bases' rows (loudly) instead of leaving
// covers to lapse silently.
func TestAbsenceRefreshDropsUnrenewedBases(t *testing.T) {
	statter := &stubStatter{}
	r := NewAbsenceReconciler(stubAbsenceRegistry{}, statter, true, 100, time.Hour, slog.New(slog.DiscardHandler))
	var report AbsenceReport
	for i := 0; i < 3; i++ {
		base := fmt.Sprintf("repo-drop-%d", i)
		r.trackAbsent(absentRow{repo: base, key: blob.LFSKey(base, absenceOID('a')), bytes: 10, registered: 10}, &report)
	}
	if len(r.knownAbsent) != 3 || len(r.baseRing) != 3 {
		t.Fatalf("tracked %d rows / %d bases, want 3/3", len(r.knownAbsent), len(r.baseRing))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the pass deadline already expired: zero renewals possible
	var errs boundedErrs
	rep := AbsenceReport{QuotaEnforced: true}
	r.refreshKnownAbsent(ctx, 100, &rep, newAbsencePassState(), &errs)
	if rep.AbsentDropped != 3 || len(r.knownAbsent) != 0 || len(r.baseRing) != 0 {
		t.Fatalf("dropped=%d tracked=%d bases=%d, want everything dropped when nothing renewed",
			rep.AbsentDropped, len(r.knownAbsent), len(r.baseRing))
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
	// One final conservative renewal charge precedes the re-verification
	// that observes the heal; the tracking must end empty.
	if report.AbsentTracked != 0 || report.CoversCharged != 1 {
		t.Fatalf("pass 2 report = %+v, want the healed row dropped from the refresh set", report)
	}
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.AbsentTracked != 0 || report.CoversCharged != 0 {
		t.Fatalf("pass 3 report = %+v, want steady state with nothing tracked or charged", report)
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
