// Absence reconciler tests (issues #101 + #125): SQLite + fileblob, reusing
// the sweep fixture (same package) for the store/blob pair. All cross-pass
// state is fleet-durable, so several tests hand the same fixture to a FRESH
// reconciler to simulate River scheduling a pass on another replica.
package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

func absenceRegistry(t *testing.T, f *sweepFixture) AbsenceRegistry {
	t.Helper()
	reg, ok := f.store.(AbsenceRegistry)
	if !ok {
		t.Fatalf("store %T lacks the absence reconciliation seams", f.store)
	}
	return reg
}

func testReconciler(t *testing.T, f *sweepFixture, quotaEnforced bool, budget int64) *AbsenceReconciler {
	t.Helper()
	return NewAbsenceReconciler(absenceRegistry(t, f), f.blobs, quotaEnforced, budget, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
}

// pinLFSLeader resets the durable pass counter so the next pass leads with
// the LFS phase (alternation is durable state now, not a struct field).
func pinLFSLeader(t *testing.T, f *sweepFixture) {
	t.Helper()
	reg := absenceRegistry(t, f)
	st, ok, err := reg.GetJobScanState(context.Background(), ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewal)
	if err != nil {
		t.Fatalf("read renewal state: %v", err)
	}
	payload := ""
	if ok {
		payload = st.Payload
	}
	if err := reg.PutJobScanState(context.Background(), ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewal, payload, 0); err != nil {
		t.Fatalf("pin leader: %v", err)
	}
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
	if report.LFSTraversals != 1 || report.PackTraversals != 1 {
		t.Fatalf("report = %+v, want one completed traversal per class", report)
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
// bumped, and re-running renews the same cover row without doubling.
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

	// Re-running renews the same incarnation key: still one cover's
	// bytes, not two — steady-state cover stays EXACT (1×).
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversRenewed != 1 || report.CoversCharged != 1 {
		t.Fatalf("second pass = %+v, want exactly the renewal charge", report)
	}
	if got := ledgerReserved(t, f, base); got != 1000 {
		t.Fatalf("reserved after renewal = %d, want 1000 (same-key refresh)", got)
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

// TestAbsenceReconcile_HandoffResumesCursorAndCovers (issue #125): the
// rotation cursor and cover renewal both derive from durable state, so a
// pass on a DIFFERENT worker resumes exactly where the previous one
// parked — no re-examined prefix — and keeps discovered covers alive.
func TestAbsenceReconcile_HandoffResumesCursorAndCovers(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-handoff"
	first := blob.LFSKey(base, absenceOID('a'))
	second := blob.LFSKey(base, absenceOID('b')) // sorts after first; bytes lost
	size := f.upload(first, "present-bytes")
	registerLFS(t, f, base, first, size)
	registerLFS(t, f, base, second, 400)

	// Worker A: budget 2 → the leading LFS phase gets 1 unit — the
	// present first row only; the cursor parks durably after it.
	workerA := testReconciler(t, f, true, 2)
	report, err := workerA.Run(context.Background())
	if err != nil {
		t.Fatalf("worker A: %v", err)
	}
	if report.LFSRowsExamined != 1 || report.LFSRowsAbsent != 0 {
		t.Fatalf("worker A report = %+v, want the present first row only", report)
	}

	// Worker B is a FRESH reconciler (a different replica): it must
	// resume strictly after the parked cursor and discover the loss
	// without re-examining the prefix.
	pinLFSLeader(t, f)
	workerB := testReconciler(t, f, true, 2)
	report, err = workerB.Run(context.Background())
	if err != nil {
		t.Fatalf("worker B: %v", err)
	}
	if report.LFSRowsExamined != 1 || report.LFSRowsAbsent != 1 || report.CoversCharged != 1 {
		t.Fatalf("worker B report = %+v, want the resumed absent row covered", report)
	}
	if got := ledgerReserved(t, f, base); got != 400 {
		t.Fatalf("reserved = %d, want the 400-byte cover", got)
	}

	// Worker C (fresh again, rotation nowhere near the row): the renewal
	// phase re-derives the cover from the durable listing and re-charges
	// it — the exact continuity issue #125 closes.
	workerC := testReconciler(t, f, true, 6)
	report, err = workerC.Run(context.Background())
	if err != nil {
		t.Fatalf("worker C: %v", err)
	}
	if report.CoversRenewed != 1 {
		t.Fatalf("worker C report = %+v, want the cover renewed from durable state", report)
	}
	if got := ledgerReserved(t, f, base); got != 400 {
		t.Fatalf("reserved after handoff renewal = %d, want 400", got)
	}
}

// TestAbsenceReconcile_PhaseAlternation (Codex round 1, P2): a twin probe
// can overdraw the leading LFS allocation at degenerate budgets; the
// leading phase alternates (durably) so the pack registry still gets
// examined every other pass. Quota is disabled so renewal cannot skew
// budgets.
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

// TestAbsenceReconcile_RenewalKeepsCoversAliveWithoutRevisit (Codex round 1
// on PR #123, P1): a discovered loss re-charges its cover every pass
// through the renewal listing, independent of where the bounded rotation
// happens to be.
func TestAbsenceReconcile_RenewalKeepsCoversAliveWithoutRevisit(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-renewal"
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
	if report.CoversCharged != 1 || report.LFSRowsAbsent != 1 {
		t.Fatalf("pass 1 report = %+v, want the loss covered", report)
	}

	// Pass 2 with budget 6: the renewal re-charges the cover; the
	// rotation's remainder is exhausted by the present rows and never
	// reaches the lost row itself.
	pinLFSLeader(t, f)
	r.budget = 6
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversRenewed != 1 || report.LFSRowsAbsent != 0 {
		t.Fatalf("pass 2 report = %+v, want the renewal to re-charge the cover without revisiting the row", report)
	}
	if got := ledgerReserved(t, f, base); got != 900 {
		t.Fatalf("reserved = %d, want the renewed 900-byte cover", got)
	}
}

// TestAbsenceReconcile_FoldedAwayRowReleasesCover: a covered loss whose
// registry row disappears (repack fold, retirement, purge completion) has
// its cover RELEASED once a full range re-derivation proves nothing is
// missing — never renewed forever.
func TestAbsenceReconcile_FoldedAwayRowReleasesCover(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-fold"
	lostPack := ref.Pack{ID: "old", Key: blob.PackKey(base, "old"), Size: 500}
	if err := f.store.RegisterPacks(ctx, base, []ref.Pack{lostPack}); err != nil {
		t.Fatal(err)
	}

	r := testReconciler(t, f, true, 100)
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.PackRowsAbsent != 1 || report.CoversCharged != 1 {
		t.Fatalf("pass 1 report = %+v, want the pack loss covered", report)
	}

	// A repack fold replaces the lost row; the successor's bytes exist.
	folded := ref.Pack{ID: "new", Key: blob.PackKey(base, "new")}
	folded.Size = f.upload(folded.Key, "folded-pack-bytes")
	if won, err := f.store.ReplacePacks(ctx, base, []ref.Pack{lostPack}, folded); err != nil || !won {
		t.Fatalf("ReplacePacks = (%v, %v), want won", won, err)
	}

	genBefore := adoptionGen(t, f, base)
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	// The renewal re-charges once (conservative), then the rotation's
	// full range re-derivation finds nothing missing and RELEASES the
	// row — no TTL tail.
	if report.CoversReleased != 1 || report.PackRowsAbsent != 0 {
		t.Fatalf("pass 2 report = %+v, want the folded-away loss's cover released", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved = %d, want 0 after the release", got)
	}
	// Releasing the compensating cover must invalidate warm floors FIRST
	// (Codex security round): a floor measured while the bytes were
	// absent may still be cached, and deleting the compensation while it
	// validates would let admission overshoot.
	if genAfter := adoptionGen(t, f, base); genAfter <= genBefore {
		t.Fatalf("adoption gen = %d -> %d across the release, want a bump invalidating warm floors", genBefore, genAfter)
	}
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversCharged != 0 {
		t.Fatalf("pass 3 report = %+v, want no covers once nothing is tracked", report)
	}
}

// TestAbsenceReconcile_RenewalNeverLapsesOnBudget (Codex round 1, P1): a
// live cover population larger than the renewal allocation still renews IN
// FULL every pass — renewal is the correctness core (a lapsed cover
// silently reopens the admission window), so overruns starve discovery
// loudly instead of letting covers round-robin past their TTL.
func TestAbsenceReconcile_RenewalNeverLapsesOnBudget(t *testing.T) {
	f := newSweepFixture(t)
	for i := range 60 {
		base := fmt.Sprintf("repo-rr-%02d", i)
		registerLFS(t, f, base, blob.LFSKey(base, absenceOID('a')), 10)
	}

	// Pass 1: a large budget discovers and covers all 60 losses.
	r := testReconciler(t, f, true, 400)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.LFSRowsAbsent != 60 || report.CoversCharged != 60 {
		t.Fatalf("pass 1 report = %+v, want 60 losses covered", report)
	}

	// Pass 2 at budget 100: the renewal allocation (50) is smaller than
	// the 60 live covers — every cover still renews, loudly over budget,
	// and discovery keeps its GUARANTEED half-budget allowance (50
	// units = 25 two-unit rows; Codex round 14: the overrun must never
	// clamp discovery toward zero, an absorbing state under the bulk
	// touch).
	r.budget = 100
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if !report.RenewalOverBudget || report.CoversRenewed != 60 || report.RenewalIncomplete {
		t.Fatalf("pass 2 report = %+v, want all 60 covers renewed over budget", report)
	}
	if report.LFSRowsExamined != 25 {
		t.Fatalf("pass 2 report = %+v, want discovery kept at the guaranteed half-budget allowance", report)
	}

	// Pass 3 with the budget raised: renewal fits its allocation again.
	r.budget = 400
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.RenewalOverBudget || report.CoversRenewed != 60 {
		t.Fatalf("pass 3 report = %+v, want all 60 covers renewed within budget", report)
	}
}

// TestAbsenceReconcile_ResumedRangeNeverShrinks (Codex round 1, P1): a
// pass whose cursor resumes INSIDE a repository's row range must not treat
// the observed suffix as the whole range — releasing/shrinking on it would
// un-cover still-missing rows in the skipped prefix.
func TestAbsenceReconcile_ResumedRangeNeverShrinks(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-resume"
	// Two ABSENT rows sorting first, two present rows after them.
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('a')), 10)
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('b')), 10)
	for _, b := range []byte{'c', 'd'} {
		key := blob.LFSKey(base, absenceOID(b))
		size := f.upload(key, "present-"+string(b))
		registerLFS(t, f, base, key, size)
	}

	// Pass 1 (budget 8 → LFS allocation 4): examines exactly the two
	// absent rows (2 units each), covers 20, parks mid-range.
	workerA := testReconciler(t, f, true, 8)
	report, err := workerA.Run(ctx)
	if err != nil {
		t.Fatalf("worker A: %v", err)
	}
	if report.LFSRowsAbsent != 2 || report.BytesCovered != 10+20 {
		t.Fatalf("worker A report = %+v, want both losses covered (grow-charged 10 then 20)", report)
	}
	if got := ledgerReserved(t, f, base); got != 20 {
		t.Fatalf("reserved = %d, want 20", got)
	}

	// A FOREIGN mutation lands while the cursor is parked (an
	// application restore bumping the base's floor generation): the
	// resumed range loses fence continuity.
	if err := absenceRegistry(t, f).BumpLFSAdoptionGen(ctx, base); err != nil {
		t.Fatalf("foreign bump: %v", err)
	}

	// Pass 2 (fresh worker): resumes on the present suffix and reaches
	// the range end with running still 20 — but the observation is
	// TAINTED, so no shrink, release, or proof may fire.
	pinLFSLeader(t, f)
	workerB := testReconciler(t, f, true, 100)
	report, err = workerB.Run(ctx)
	if err != nil {
		t.Fatalf("worker B: %v", err)
	}
	if report.CoversReleased != 0 || report.CoversRotated != 0 {
		t.Fatalf("worker B report = %+v, want no shrink from a tainted resumed range", report)
	}
	if got := ledgerReserved(t, f, base); got != 20 {
		t.Fatalf("reserved = %d, want the prefix losses still covered", got)
	}
}

// TestAbsenceReconcile_MultiPassBaseRetiresLegacy (Codex round 20, P1): a
// base whose row range exceeds one pass's class allowance can only ever
// complete its range RESUMED — a blanket resumed-proof denial would
// bulk-renew its legacy nonce forever. With the base group's mutation
// fence verified across the park gap, the resumed completion is a
// legitimate clean proof and the nonce retires.
func TestAbsenceReconcile_MultiPassBaseRetiresLegacy(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-multipass"
	// Six present rows: more than pass 1's LFS allocation can settle.
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	// Pass 1 parks mid-range (budget 8: the nonce renewal charges 1,
	// the LFS allocation settles 4 of 6 rows). The packs gap proof
	// alone must not retire the nonce.
	pinLFSLeader(t, f)
	workerA := testReconciler(t, f, true, 8)
	report, err := workerA.Run(ctx)
	if err != nil {
		t.Fatalf("worker A: %v", err)
	}
	if report.CoversDecaying != 0 {
		t.Fatalf("pass 1 report = %+v, want no retirement before the LFS range completes", report)
	}

	// Pass 2 (fresh worker — another replica): the fence held across
	// the gap, the resumed range completes cleanly, and the double
	// proof retires the nonce.
	workerB := testReconciler(t, f, true, 100)
	report, err = workerB.Run(ctx)
	if err != nil {
		t.Fatalf("worker B: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("pass 2 report = %+v, want the nonce retired on the fence-continuous proof", report)
	}
	if got := ledgerReserved(t, f, base); got != 555 {
		t.Fatalf("reserved = %d, want only the decaying nonce (no deficits, no heads)", got)
	}
}

// TestAbsenceReconcile_OutOfPhaseClassProofsRetireLegacy (Codex round
// 21, P1): the two class rotations traverse independent populations, so
// their clean completions for one base need not coincide — fold
// candidacies persist (loadProofs/saveProofs) and completions trigger
// fresh verification, so a multi-pass base in BOTH classes still retires
// its nonce within a bounded number of passes (budget covers the
// combined class rows, per the sizing rule).
func TestAbsenceReconcile_OutOfPhaseClassProofsRetireLegacy(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "m-target"
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	for i := range byte(9) {
		id := fmt.Sprintf("p%02d", i)
		key := blob.PackKey(base, id)
		size := f.upload(key, strings.Repeat("y", 10))
		if err := f.store.RegisterPacks(ctx, base, []ref.Pack{{ID: id, Key: key, Size: size}}); err != nil {
			t.Fatal(err)
		}
	}
	// Fleet filler surrounding the target in keyset order: the class
	// rotations spend their per-pass allocations here, so the target's
	// two class ranges complete in DIFFERENT passes.
	for i := range byte(20) {
		key := blob.LFSKey("a-filler", absenceOID('a'+i))
		registerLFS(t, f, "a-filler", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	for i := range byte(20) {
		id := fmt.Sprintf("zp%02d", i)
		key := blob.PackKey("z-filler", id)
		size := f.upload(key, strings.Repeat("y", 10))
		if err := f.store.RegisterPacks(ctx, "z-filler", []ref.Pack{{ID: id, Key: key, Size: size}}); err != nil {
			t.Fatal(err)
		}
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	pinLFSLeader(t, f)
	// Pass 1 parks both rotations inside the filler/target: no
	// retirement.
	worker := testReconciler(t, f, true, 20)
	report, err := worker.Run(ctx)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if report.CoversDecaying != 0 {
		t.Fatalf("pass 1: premature retirement (%+v)", report)
	}
	// The class completions land in different passes; the persisted
	// half-proof carries the earlier one and the fold converges within
	// a bounded number of passes (budget 20 covers the target's
	// combined class rows, 6+9, per the sizing rule).
	retired := false
	for pass := 2; pass <= 8 && !retired; pass++ {
		worker := testReconciler(t, f, true, 20)
		report, err := worker.Run(ctx)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		switch report.CoversDecaying {
		case 0:
		case 1:
			retired = true
		default:
			t.Fatalf("pass %d: retired %d covers, want 1 total", pass, report.CoversDecaying)
		}
	}
	if !retired {
		t.Fatal("nonce never retired within 6 passes")
	}
	if got := ledgerReserved(t, f, base); got != 555 {
		t.Fatalf("reserved = %d, want only the decaying nonce", got)
	}
}

// TestAbsenceReconcile_PersistedProofRetiresWithFreshVerification (Codex
// round 21): a persisted half-proof is a TRIGGER — consuming it for
// retirement re-verifies the class fresh at fold time. Here the LFS
// rotation is still mid-range in the only pass run, so retirement can
// come ONLY from the persisted half-proof plus its fold-time
// verification (the packs gap proof is fresh every pass).
func TestAbsenceReconcile_PersistedProofRetiresWithFreshVerification(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-persisted-proof"
	reg := absenceRegistry(t, f)
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	if err := ref.ReserveRestoreCover(ctx, reg, blob.QuotaRepoGroup(base),
		ref.AbsenceCoverPrefix+base+"/"+ref.NewReservationOwner(), 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}
	// Simulate an earlier pass's parked half-proof: the LFS class proved
	// clean at the current group generation.
	var gen int64
	for _, repo := range blob.QuotaRepoGroup(base) {
		g, err := reg.LFSAdoptionGen(ctx, repo)
		if err != nil {
			t.Fatal(err)
		}
		gen += g
	}
	payload := fmt.Sprintf(`{"%s":{"%s":{"gen":%d}}}`, base, absenceClassLFS, gen)
	if err := reg.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceProofs, payload, 0); err != nil {
		t.Fatal(err)
	}

	// One pass, budget 8: the LFS rotation parks mid-range (allocation
	// 4 of 6 rows), so only the persisted half-proof can complete the
	// pair — after fold-time fresh verification of all 6 rows.
	r := testReconciler(t, f, true, 8)
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("report = %+v, want the nonce retired via the persisted trigger", report)
	}
	if report.RowsVerified < 6 {
		t.Fatalf("report = %+v, want the full base re-verified before retirement", report)
	}
	if got := ledgerReserved(t, f, base); got != 555 {
		t.Fatalf("reserved = %d, want only the decaying nonce", got)
	}
}

// TestAbsenceReconcile_GapLossCaughtByVerification (Codex round 21,
// security): out-of-band byte loss during a park gap has no DB event, so
// the generation fence alone cannot see it — the verdict-time fresh
// verification must. A row settled in pass 1 loses its bytes while the
// cursor is parked; pass 2's resumed completion triggers verification,
// which observes the loss, covers it, and only then retires the nonce.
func TestAbsenceReconcile_GapLossCaughtByVerification(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "m-gap-loss"
	// Filler sorting BEFORE the target keeps pass 2's wrapped tail away
	// from the lost row: only the verification may observe it.
	for i := range byte(10) {
		key := blob.LFSKey("a-filler", absenceOID('a'+i))
		registerLFS(t, f, "a-filler", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	// Pass 1 (LFS allocation 13) settles the filler plus target rows
	// a, b, c and parks: b is behind the cursor, observed present.
	pinLFSLeader(t, f)
	workerA := testReconciler(t, f, true, 27)
	if _, err := workerA.Run(ctx); err != nil {
		t.Fatalf("worker A: %v", err)
	}

	// Out-of-band loss behind the parked cursor: no DB mutation, the
	// generation fence stays intact.
	if err := f.blobs.Delete(ctx, blob.LFSKey(base, absenceOID('b'))); err != nil {
		t.Fatal(err)
	}

	// Pass 2: the rotation settles only the unvisited suffix (d, e, f)
	// and its wrapped tail stays inside the filler — it never revisits
	// b. ONLY the completion-triggered fresh verification can observe
	// the loss, and it must: the deficit is covered before the nonce
	// retires — never a false retirement.
	workerB := testReconciler(t, f, true, 12)
	report, err := workerB.Run(ctx)
	if err != nil {
		t.Fatalf("worker B: %v", err)
	}
	if report.CoversDecaying != 1 || report.LFSRowsAbsent == 0 || report.RowsVerified < 7 {
		t.Fatalf("pass 2 report = %+v, want the loss observed by verification and the nonce retired", report)
	}
	if got := ledgerReserved(t, f, base); got != 555+10 {
		t.Fatalf("reserved = %d, want the decaying nonce plus the fresh loss cover", got)
	}
}

// TestAbsenceReconcile_CombinedClassesExceedingPoolConverge (Codex
// rounds 23-24): a base whose combined class verification work exceeds
// the per-pass pool must still converge — whichever class goes fresh in
// a pass persists a fold candidacy, and the fold re-stats the other
// class run-to-completion past the pool once started (starts gate on a
// non-exhausted pool). Without that, the fold order would drain the pool
// on the same class every pass and the counterpart could never complete.
func TestAbsenceReconcile_CombinedClassesExceedingPoolConverge(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-combined"
	// 12 LFS rows + 16 pack rows: each class fits the pool (budget 20)
	// alone, never together (12+16 > 20).
	for i := range byte(12) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	for i := range byte(16) {
		id := fmt.Sprintf("p%02d", i)
		key := blob.PackKey(base, id)
		size := f.upload(key, strings.Repeat("y", 10))
		if err := f.store.RegisterPacks(ctx, base, []ref.Pack{{ID: id, Key: key, Size: size}}); err != nil {
			t.Fatal(err)
		}
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	pinLFSLeader(t, f)
	retired := false
	for pass := 1; pass <= 10 && !retired; pass++ {
		worker := testReconciler(t, f, true, 20)
		report, err := worker.Run(ctx)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		switch report.CoversDecaying {
		case 0:
		case 1:
			retired = true
		default:
			t.Fatalf("pass %d: retired %d covers, want 1 total", pass, report.CoversDecaying)
		}
	}
	if !retired {
		t.Fatal("nonce never retired within 10 passes: combined-class verification livelocked")
	}
	if got := ledgerReserved(t, f, base); got != 555 {
		t.Fatalf("reserved = %d, want only the decaying nonce", got)
	}
}

// TestAbsenceReconcile_StaleHintLossRechargeReverifiedAtFold (Codex
// round 24, P1 + security): a persisted class candidacy spans a park gap
// no DB fence can vouch for — out-of-band byte loss has no DB event, and
// an old worker's nonce recharge before this pass's listing shows up as
// ordinary liveness whose (owner, bytes) the fold CAS would match. The
// fold must re-stat the hinted class FRESH: the re-stat observes the
// loss and charges it into the deterministic head BEFORE the recharged
// nonce may retire — never a retirement that leaves the loss
// uncompensated (the pre-fix age shortcut folded without re-stat,
// deflating the floor until the class rotation returned).
func TestAbsenceReconcile_StaleHintLossRechargeReverifiedAtFold(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "m-stale-hint"
	reg := absenceRegistry(t, f)
	// Filler sorting BEFORE the target: the pass's LFS rotation spends
	// its whole allocation there and parks, so only the fold-time
	// re-stat can observe the target this pass.
	for i := range byte(10) {
		key := blob.LFSKey("a-filler", absenceOID('a'+i))
		registerLFS(t, f, "a-filler", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, reg, blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed nonce: %v", err)
	}
	// The old worker's recharge: a same-key TTL refresh at a grown
	// total, overwriting owner — listed by this pass as ordinary
	// liveness the fold CAS matches.
	if err := ref.ReserveRestoreCover(ctx, reg, blob.QuotaRepoGroup(base), legacyKey, 700, time.Hour); err != nil {
		t.Fatalf("recharge nonce: %v", err)
	}
	// The persisted candidacy from an earlier pass, fenced at the
	// current group generation — the out-of-band loss below has no DB
	// event, so the fence stays valid.
	var gen int64
	for _, repo := range blob.QuotaRepoGroup(base) {
		g, err := reg.LFSAdoptionGen(ctx, repo)
		if err != nil {
			t.Fatal(err)
		}
		gen += g
	}
	payload := fmt.Sprintf(`{"%s":{"%s":{"gen":%d}}}`, base, absenceClassLFS, gen)
	if err := reg.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceProofs, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.blobs.Delete(ctx, blob.LFSKey(base, absenceOID('b'))); err != nil {
		t.Fatal(err)
	}

	// One pass: the LFS rotation parks inside the filler (never reaching
	// the target), the target's packs proof is a fresh row-less gap, and
	// the candidacy schedules the fold-time LFS re-stat — which observes
	// the loss and covers it before the recharged nonce retires.
	pinLFSLeader(t, f)
	worker := testReconciler(t, f, true, 12)
	report, err := worker.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsAbsent == 0 || report.RowsVerified < 6 {
		t.Fatalf("report = %+v, want the loss observed by the fold-time re-stat, never trusted from the hint", report)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("report = %+v, want the recharged nonce retired only WITH the loss covered", report)
	}
	if got := ledgerReserved(t, f, base); got != 700+10 {
		t.Fatalf("reserved = %d, want the decaying nonce plus the fresh loss cover", got)
	}
}

// TestAbsenceReconcile_CandidacySurvivesSelfRenewalBumps (Codex round
// 25, P1): a base holding a live deterministic cover gets its group
// generation bumped by the reconciler's OWN renewal re-charge every pass
// (chargeCover pairs each charge with a BumpLFSAdoptionGen). A carried
// candidacy must be re-snapshotted at save time, not retained under its
// original fence — otherwise it self-expires after one carry and
// out-of-phase class completions more than one pass apart can never
// converge, keeping the legacy nonce bulk-renewed forever.
func TestAbsenceReconcile_CandidacySurvivesSelfRenewalBumps(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "m-carry"
	reg := absenceRegistry(t, f)
	// LFS filler large enough that the LFS rotation NEVER reaches the
	// target: only the candidacy can ever complete the LFS side.
	for i := range 60 {
		key := blob.LFSKey("a-fill", strings.Repeat(fmt.Sprintf("%02x", i), 32))
		registerLFS(t, f, "a-fill", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	// Pack filler the packs rotation needs multiple passes to cross;
	// the target has NO pack rows, so its packs proof is the row-less
	// window of the traversal that finally wraps.
	for i := range byte(10) {
		id := fmt.Sprintf("fp%02d", i)
		key := blob.PackKey("a-fill", id)
		size := f.upload(key, strings.Repeat("y", 10))
		if err := f.store.RegisterPacks(ctx, "a-fill", []ref.Pack{{ID: id, Key: key, Size: size}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	// The live deterministic cover whose per-pass renewal re-charge
	// bumps the base's group generation.
	if err := ref.ReserveRestoreCover(ctx, reg, blob.QuotaRepoGroup(base),
		absenceIncarnationKey(base, absenceClassLFS, 1), 50, time.Hour); err != nil {
		t.Fatalf("seed deterministic cover: %v", err)
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, reg, blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed nonce: %v", err)
	}
	// The LFS candidacy from an earlier pass, fenced at the current
	// group generation.
	var gen int64
	for _, repo := range blob.QuotaRepoGroup(base) {
		g, err := reg.LFSAdoptionGen(ctx, repo)
		if err != nil {
			t.Fatal(err)
		}
		gen += g
	}
	payload := fmt.Sprintf(`{"%s":{"%s":{"gen":%d}}}`, base, absenceClassLFS, gen)
	if err := reg.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceProofs, payload, 0); err != nil {
		t.Fatal(err)
	}

	// Two small passes: both rotations stay parked inside the filler,
	// so the candidacy must be CARRIED across two self-renewal bumps.
	pinLFSLeader(t, f)
	for pass := 1; pass <= 2; pass++ {
		worker := testReconciler(t, f, true, 12)
		report, err := worker.Run(ctx)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if report.CoversDecaying != 0 {
			t.Fatalf("pass %d report = %+v, want no retirement while packs is unproven", pass, report)
		}
	}

	// A moderate pass: the packs rotation finishes the filler and wraps
	// (the target's row-less window goes fresh) while the LFS rotation
	// is still inside its filler — retirement is possible ONLY through
	// the carried candidacy plus its fold-time re-stat.
	worker := testReconciler(t, f, true, 40)
	report, err := worker.Run(ctx)
	if err != nil {
		t.Fatalf("final pass: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("final report = %+v, want the nonce retired via the carried candidacy", report)
	}
	if report.RowsVerified < 6 {
		t.Fatalf("final report = %+v, want the fold-time re-stat of the target's LFS rows", report)
	}
}

// cancelOnRotationRegistry kills the pass the moment a ROTATION listing
// starts (the origin keyset call): fold-phase re-stats list from
// (base, "") and pass through untouched, so a retirement observed
// despite the cancel proves the hint-driven fold ran BEFORE the
// rotations.
type cancelOnRotationRegistry struct {
	AbsenceRegistry
	cancel context.CancelFunc
}

func (c *cancelOnRotationRegistry) ListFleetLFSObjects(ctx context.Context, afterRepo, afterKey string, limit int) ([]ref.AbsenceLFSRow, error) {
	if afterRepo == "" && afterKey == "" {
		c.cancel()
		return nil, context.Canceled
	}
	return c.AbsenceRegistry.ListFleetLFSObjects(ctx, afterRepo, afterKey, limit)
}

// TestAbsenceReconcile_HintFoldPrecedesRotations (Codex round 25,
// security): the fold-time re-stat is pool-unbounded but
// wall-clock-bounded and non-resumable (same-pass evidence only), so
// hint-driven candidacies must fold BEFORE the rotations consume the
// pass deadline — otherwise a large base's re-stat starts on an expiring
// context every pass, never reaches the folded CAS, and the bulk-renewed
// nonce retains its stale over-reservation indefinitely. Here the pass
// dies the moment the rotations begin: retirement can only come from the
// pre-rotation fold.
func TestAbsenceReconcile_HintFoldPrecedesRotations(t *testing.T) {
	f := newSweepFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := "m-priority"
	reg := absenceRegistry(t, f)
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, reg, blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed nonce: %v", err)
	}
	// Both class candidacies persisted by earlier passes (the packs
	// side is a row-less re-stat).
	var gen int64
	for _, repo := range blob.QuotaRepoGroup(base) {
		g, err := reg.LFSAdoptionGen(ctx, repo)
		if err != nil {
			t.Fatal(err)
		}
		gen += g
	}
	payload := fmt.Sprintf(`{"%s":{"%s":{"gen":%d},"%s":{"gen":%d}}}`,
		base, absenceClassLFS, gen, absenceClassPacks, gen)
	if err := reg.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceProofs, payload, 0); err != nil {
		t.Fatal(err)
	}

	pinLFSLeader(t, f)
	r := NewAbsenceReconciler(&cancelOnRotationRegistry{AbsenceRegistry: reg, cancel: cancel},
		f.blobs, true, 100, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	report, err := r.Run(ctx)
	if err == nil {
		t.Fatal("Run: want the injected rotation-start cancellation reported")
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("report = %+v, want the nonce retired by the pre-rotation fold", report)
	}
	if report.RowsVerified < 6 {
		t.Fatalf("report = %+v, want the fold-time re-stat of the target's rows", report)
	}
}

// TestAbsenceReconcile_RenewalBlipDoesNotFenceAdmission (Codex round 26,
// security): a renewal failure while the health watermark is FRESH
// cannot have let any cover lapse — every live cover carries a full TTL
// from the last successful renewal — so the admission fence must stay
// unarmed and quota-gated writes unaffected.
func TestAbsenceReconcile_RenewalBlipDoesNotFenceAdmission(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	if err := reg.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth, "", 0); err != nil {
		t.Fatal(err)
	}
	ft := &failTouchRegistry{AbsenceRegistry: reg, fail: true}
	r := NewAbsenceReconciler(ft, f.blobs, true, 100, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := r.Run(ctx); err == nil {
		t.Fatal("Run: want the injected renewal failure reported")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || ok {
		t.Fatalf("fence after fresh-watermark blip = (ok=%v, %v), want unarmed", ok, err)
	}
}

// TestAbsenceReconcile_SafetyBoundCoversNextCadence (Codex round 27,
// P1): a renewal failure is provably harmless only while the covers
// survive until the NEXT scheduled attempt — the safety bound must
// subtract the cadence AND the pass wall clock from the TTL, not just
// the pass. At the documented maximum interval (TTL/2), a failure at
// age ~TTL/2 must fence: by the next attempt the covers are past the
// TTL, where the strict-TTL bulk touch cannot resurrect them, yet it
// would stamp renewal health and leave admissions consuming the
// uncovered headroom while recovery is still rotations away.
func TestAbsenceReconcile_SafetyBoundCoversNextCadence(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	if err := reg.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth, "", 0); err != nil {
		t.Fatal(err)
	}
	// coverTTL 8s → pass wall clock 2s; interval 4s → safety bound 2s.
	// An age of ~3s is fresh under a pass-only bound (TTL−pass = 6s)
	// but PAST the cadence-aware bound: the next attempt lands up to
	// interval+pass = 6s out, when these covers are already 9s old.
	time.Sleep(3 * time.Second)
	ft := &failTouchRegistry{AbsenceRegistry: reg, fail: true}
	r := NewAbsenceReconciler(ft, f.blobs, true, 100, 8*time.Second, 4*time.Second, slog.New(slog.DiscardHandler))
	if _, err := r.Run(ctx); err == nil {
		t.Fatal("Run: want the injected renewal failure reported")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after mid-cadence-stale failure = (ok=%v, %v), want armed", ok, err)
	}
}

// TestAbsenceReconcile_SafetyBoundCoversTouchToStampLag (Codex round 38,
// security — the failure-path twin of round 32's success-path bound):
// the health watermark stamps up to a pass AFTER the touch it records
// (separate statements under the pass deadline), so the observed age
// under-reports the covers' true age by up to the pass wall clock. A
// failure accepted just inside a cadence-only bound could let covers
// expire before the next attempt, letting finite writes reserve against
// the deflated floor; the bound must subtract the lag too.
func TestAbsenceReconcile_SafetyBoundCoversTouchToStampLag(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	if err := reg.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth, "", 0); err != nil {
		t.Fatal(err)
	}
	// coverTTL 8s → pass wall clock 2s; interval 2s → cadence-only
	// bound 4s, lag-aware bound 2s. An age of ~3s is fresh under the
	// cadence-only bound but PAST the lag-aware one: the recorded touch
	// may have run a full pass before the stamp, so the covers can
	// already be ~5s old, and the next attempt lands up to
	// interval+pass = 4s out — past the 8s TTL, where the strict-TTL
	// bulk touch cannot resurrect them.
	time.Sleep(3 * time.Second)
	ft := &failTouchRegistry{AbsenceRegistry: reg, fail: true}
	r := NewAbsenceReconciler(ft, f.blobs, true, 100, 8*time.Second, 2*time.Second, slog.New(slog.DiscardHandler))
	if _, err := r.Run(ctx); err == nil {
		t.Fatal("Run: want the injected renewal failure reported")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after lag-window failure = (ok=%v, %v), want armed", ok, err)
	}
}

// TestAbsenceReconcile_StaleRenewalFailureFencesAdmission (Codex round
// 26, security): a renewal failure with NO health watermark (or a stale
// one) may let TTL'd covers lapse un-renewed — strict-TTL admission
// arithmetic would silently under-count them, reopening the quota
// headroom they compensate. The fence arms durably, fails finite
// quota-gated admissions closed while unbounded internal reservations
// proceed, and clears only after a healthy pass completes a full
// recovery rotation of both classes.
func TestAbsenceReconcile_StaleRenewalFailureFencesAdmission(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	base := "repo-fence"
	key := blob.LFSKey(base, absenceOID('a'))
	registerLFS(t, f, base, key, 10)
	f.upload(key, strings.Repeat("x", 10))

	ft := &failTouchRegistry{AbsenceRegistry: reg, fail: true}
	rec := NewAbsenceReconciler(ft, f.blobs, true, 100, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := rec.Run(ctx); err == nil {
		t.Fatal("Run 1: want the injected renewal failure reported")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after unproven-freshness failure = (ok=%v, %v), want armed", ok, err)
	}

	// Finite quota-gated admission fails closed with the distinct
	// verdict; unbounded internal reservations (restores, covers) are
	// exempt so recovery itself can proceed.
	res, ok := f.store.(ref.RestoreCoverStore)
	if !ok {
		t.Fatalf("store %T lacks ReservePackBytes", f.store)
	}
	dec, err := res.ReservePackBytes(ctx, blob.QuotaRepoGroup(base),
		[]ref.Pack{{Key: blob.PackKey(base, "p1"), Size: 10}}, ref.NewReservationOwner(),
		0, ref.ReserveNoFloorGen, 1000, time.Hour)
	if err != nil || dec.OK || !dec.RenewalDegraded {
		t.Fatalf("fenced finite reserve = (%+v, %v), want RenewalDegraded denial", dec, err)
	}
	if err := ref.ReserveRestoreCover(ctx, reg, blob.QuotaRepoGroup(base), key, 5, time.Hour); err != nil {
		t.Fatalf("unbounded reserve under fence: %v", err)
	}

	// A healthy pass completes a full rotation of both classes (tiny
	// keyspace: one wrap per pass) and clears the fence.
	pinLFSLeader(t, f)
	rec2 := testReconciler(t, f, true, 100)
	if _, err := rec2.Run(ctx); err != nil {
		t.Fatalf("recovery pass: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || ok {
		t.Fatalf("fence after recovery rotation = (ok=%v, %v), want cleared", ok, err)
	}
	dec, err = res.ReservePackBytes(ctx, blob.QuotaRepoGroup(base),
		[]ref.Pack{{Key: blob.PackKey(base, "p2"), Size: 10}}, ref.NewReservationOwner(),
		0, ref.ReserveNoFloorGen, 1000, time.Hour)
	if err != nil || !dec.OK || dec.RenewalDegraded {
		t.Fatalf("post-recovery finite reserve = (%+v, %v), want admitted", dec, err)
	}
}

// TestAbsenceReconcile_UnprovableContinuitySuccessArmsFence (Codex round
// 29, P1): a SUCCESSFUL bulk touch proves covers stayed alive only when
// the prior health watermark shows nothing could have lapsed before it
// ran — the strict-TTL update leaves already-dead rows dead. A fleet
// paused past the TTL, or quota enforcement re-enabled after an off
// period (no watermark at all), must arm the recovery fence BEFORE
// stamping fresh health, and the fence must hold across passes until a
// full rotation of both classes completes on a multi-pass registry.
func TestAbsenceReconcile_UnprovableContinuitySuccessArmsFence(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	// Multi-pass LFS keyspace: 30 rows, budget 12 — the first pass
	// parks mid-rotation, so the armed fence is observable at pass end.
	for i := range 30 {
		key := blob.LFSKey("a-fill", strings.Repeat(fmt.Sprintf("%02x", i), 32))
		registerLFS(t, f, "a-fill", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}

	// Pass 1: the touch succeeds but NO health watermark exists AND the
	// lapse probe finds an expired cover (injected) — recovery, so the
	// fence arms and health stamps. Without evidence the absent
	// watermark alone must not arm (round 32; see the upgrade test).
	pinLFSLeader(t, f)
	rec := NewAbsenceReconciler(&lapsedRegistry{reg}, f.blobs, true, 12, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := rec.Run(ctx); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after unprovable-continuity success = (ok=%v, %v), want armed", ok, err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil || !ok {
		t.Fatalf("health watermark = (ok=%v, %v), want stamped", ok, err)
	}
	res, ok := f.store.(ref.RestoreCoverStore)
	if !ok {
		t.Fatalf("store %T lacks ReservePackBytes", f.store)
	}
	dec, err := res.ReservePackBytes(ctx, blob.QuotaRepoGroup("a-fill"),
		[]ref.Pack{{Key: blob.PackKey("a-fill", "p1"), Size: 10}}, ref.NewReservationOwner(),
		0, ref.ReserveNoFloorGen, 10_000, time.Hour)
	if err != nil || dec.OK || !dec.RenewalDegraded {
		t.Fatalf("fenced finite reserve = (%+v, %v), want RenewalDegraded denial", dec, err)
	}

	// Later passes have provable continuity (fresh watermark) and clear
	// the fence once both rotations complete a full recovery traversal.
	cleared := false
	for pass := 2; pass <= 6 && !cleared; pass++ {
		rec := testReconciler(t, f, true, 12)
		if _, err := rec.Run(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		_, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence)
		if err != nil {
			t.Fatalf("pass %d fence read: %v", pass, err)
		}
		cleared = !ok
	}
	if !cleared {
		t.Fatal("fence never cleared after full recovery rotations")
	}
	dec, err = res.ReservePackBytes(ctx, blob.QuotaRepoGroup("a-fill"),
		[]ref.Pack{{Key: blob.PackKey("a-fill", "p2"), Size: 10}}, ref.NewReservationOwner(),
		0, ref.ReserveNoFloorGen, 10_000, time.Hour)
	if err != nil || !dec.OK {
		t.Fatalf("post-recovery finite reserve = (%+v, %v), want admitted", dec, err)
	}
}

// TestAbsenceReconcile_UpgradeFirstPassKeepsAdmissionOpen (Codex round
// 32, security): the very first pass under this feature finds no
// renewal-health watermark — the upgrade/first-enable signature, not
// lapse evidence. With no expired cover row present the fleet-global
// admission fence must NOT arm: on a multi-pass registry it would
// reject EVERY tenant's finite reservations until a later scheduled
// pass completes the recovery rotation. Health stamps and finite
// admission stays open.
func TestAbsenceReconcile_UpgradeFirstPassKeepsAdmissionOpen(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	// Multi-pass keyspace: an armed fence would outlive this pass.
	for i := range 30 {
		key := blob.LFSKey("a-fill", strings.Repeat(fmt.Sprintf("%02x", i), 32))
		registerLFS(t, f, "a-fill", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	pinLFSLeader(t, f)
	rec := testReconciler(t, f, true, 12)
	if _, err := rec.Run(ctx); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || ok {
		t.Fatalf("fence after evidence-free first pass = (ok=%v, %v), want NOT armed", ok, err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil || !ok {
		t.Fatalf("health watermark = (ok=%v, %v), want stamped", ok, err)
	}
	res, ok := f.store.(ref.RestoreCoverStore)
	if !ok {
		t.Fatalf("store %T lacks ReservePackBytes", f.store)
	}
	dec, err := res.ReservePackBytes(ctx, blob.QuotaRepoGroup("a-fill"),
		[]ref.Pack{{Key: blob.PackKey("a-fill", "p1"), Size: 10}}, ref.NewReservationOwner(),
		0, ref.ReserveNoFloorGen, 10_000, time.Hour)
	if err != nil || !dec.OK || dec.RenewalDegraded {
		t.Fatalf("first-pass finite reserve = (%+v, %v), want admitted", dec, err)
	}
}

// TestAbsenceReconcile_PreUpgradeGCWindowStaysOpen (Codex round 40 on PR
// #132, security) PINS the documented mixed-fleet window of ADR-43, it
// does not fix one: a PRE-UPGRADE replica's expired-reservation GC
// deletes a lapsed un-folded cover with a plain row delete — no
// lapse-evidence marker — and the first health stamp afterwards sees
// neither row nor marker and takes the deliberate open-admission
// default (round 32: an absent watermark alone is the first-enable
// signature, not fleet-wide lapse evidence). Destroyed evidence is
// undetectable by construction, so the guarantee is operational: enable
// the job only on a fully-upgraded fleet and complete rolls within one
// reservation TTL (boot warning + configuration reference + ADR-43).
// If a future mechanism makes marker-less deletions detectable, this
// test documents the behavior it must change.
func TestAbsenceReconcile_PreUpgradeGCWindowStaysOpen(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	lost := blob.LFSKey("z-lost", absenceOID('f'))
	registerLFS(t, f, "z-lost", lost, 10)

	// Pass 1: the full rotation charges the z-lost cover and stamps
	// first health (no lapse anywhere).
	pinLFSLeader(t, f)
	if _, err := testReconciler(t, f, true, 100).Run(ctx); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	covers, err := reg.ListAbsenceCovers(ctx, "", 10, time.Hour)
	if err != nil || len(covers) != 1 || covers[0].Base != "z-lost" {
		t.Fatalf("covers after pass 1 = (%+v, %v), want the z-lost cover", covers, err)
	}

	// Simulate the post-upgrade first stamp: no health row exists yet.
	if err := reg.DeleteJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil {
		t.Fatal(err)
	}
	// Simulate the PRE-UPGRADE GC: a plain cover-row delete — the exact
	// data state an old replica's marker-unaware expired-row pruning
	// leaves behind (row gone, no ScanScopeAbsenceLapse marker).
	if err := reg.DeleteAbsenceCover(ctx, covers[0].Key); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceLapse); err != nil || ok {
		t.Fatalf("lapse marker after plain delete = (ok=%v, %v), want none — old GC writes no evidence", ok, err)
	}

	// The first stamp sees neither row nor marker: open-admission
	// default — health stamps, no fence. This is the ADR-43 window.
	if _, err := testReconciler(t, f, true, 100).Run(ctx); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || ok {
		t.Fatalf("fence after evidence-destroyed first stamp = (ok=%v, %v), want open admission (the documented window)", ok, err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil || !ok {
		t.Fatalf("health after first stamp = (ok=%v, %v), want stamped", ok, err)
	}
}

// TestAbsenceReconcile_AdmissionGCDurableLapseMarker (Codex round 33 on
// PR #132, P1): ReservePackBytes' self-healing GC deletes a base's
// expired reservation rows at admission time — the very writers the
// admission fence should block destroy the physical evidence the
// first-health-stamp probe arms on. The GC records a durable
// lapse-evidence marker in the same transaction; the reconciler consults
// it BEFORE the physical probe, arms the fence, and consumes the marker
// only when the recovery rotation settles.
func TestAbsenceReconcile_AdmissionGCDurableLapseMarker(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	// Multi-pass keyspace (an armed fence must outlive one pass) plus one
	// ABSENT row whose cover is the future lapse evidence.
	for i := range 30 {
		key := blob.LFSKey("a-fill", strings.Repeat(fmt.Sprintf("%02x", i), 32))
		registerLFS(t, f, "a-fill", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	lost := blob.LFSKey("z-lost", absenceOID('f'))
	registerLFS(t, f, "z-lost", lost, 10)

	// Pass 1 (generous budget): full rotation charges the z-lost cover
	// and stamps first health with admission open (no lapse anywhere).
	pinLFSLeader(t, f)
	if _, err := testReconciler(t, f, true, 100).Run(ctx); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if covers, err := reg.ListAbsenceCovers(ctx, "", 10, time.Hour); err != nil || len(covers) != 1 || covers[0].Base != "z-lost" {
		t.Fatalf("covers after pass 1 = (%+v, %v), want the z-lost cover", covers, err)
	}

	// Simulate the pre-watermark regime: no health row was ever stamped.
	if err := reg.DeleteJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil {
		t.Fatal(err)
	}

	// An ordinary finite admission under a tiny TTL: the GC prunes the
	// now-expired cover row — the marker must survive the consumption.
	res, ok := f.store.(ref.RestoreCoverStore)
	if !ok {
		t.Fatalf("store %T lacks ReservePackBytes", f.store)
	}
	dec, err := res.ReservePackBytes(ctx, blob.QuotaRepoGroup("z-lost"),
		[]ref.Pack{{Key: blob.PackKey("z-lost", "p1"), Size: 1}}, ref.NewReservationOwner(),
		0, ref.ReserveNoFloorGen, 10_000, time.Nanosecond)
	if err != nil || !dec.OK {
		t.Fatalf("gc reserve = (%+v, %v), want admitted", dec, err)
	}
	if lapsed, err := reg.LapsedAbsenceCoverExists(ctx, time.Nanosecond); err != nil || lapsed {
		t.Fatalf("physical probe after GC = (%v, %v), want no evidence left", lapsed, err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceLapse); err != nil || !ok {
		t.Fatalf("lapse marker after GC = (ok=%v, %v), want recorded", ok, err)
	}

	// Pass 2 (budget-starved): the first health stamp finds the marker —
	// the fence arms and holds past the pass; finite admission fails
	// closed.
	if _, err := testReconciler(t, f, true, 12).Run(ctx); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after marker-armed pass = (ok=%v, %v), want armed", ok, err)
	}
	dec, err = res.ReservePackBytes(ctx, blob.QuotaRepoGroup("a-fill"),
		[]ref.Pack{{Key: blob.PackKey("a-fill", "p1"), Size: 10}}, ref.NewReservationOwner(),
		0, ref.ReserveNoFloorGen, 10_000, time.Hour)
	if err != nil || dec.OK || !dec.RenewalDegraded {
		t.Fatalf("fenced finite reserve = (%+v, %v), want RenewalDegraded denial", dec, err)
	}

	// Recovery rotations clear the fence AND consume the marker.
	cleared := false
	for pass := 3; pass <= 10 && !cleared; pass++ {
		if _, err := testReconciler(t, f, true, 12).Run(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		_, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence)
		if err != nil {
			t.Fatalf("pass %d fence read: %v", pass, err)
		}
		cleared = !ok
	}
	if !cleared {
		t.Fatal("fence never cleared after full recovery rotations")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceLapse); err != nil || ok {
		t.Fatalf("lapse marker after settlement = (ok=%v, %v), want consumed", ok, err)
	}
}

// gcDuringProbeRegistry interleaves a REAL admission-time GC between the
// reconciler's marker read and its physical probe: the reserve consumes
// the expired un-folded cover and records the marker atomically, then the
// underlying probe truthfully reports no expired row.
type gcDuringProbeRegistry struct {
	AbsenceRegistry
	t     *testing.T
	res   ref.RestoreCoverStore
	group []string
	fired bool
}

func (g *gcDuringProbeRegistry) LapsedAbsenceCoverExists(ctx context.Context, ttl time.Duration) (bool, error) {
	if !g.fired {
		g.fired = true
		dec, err := g.res.ReservePackBytes(ctx, g.group,
			[]ref.Pack{{Key: blob.PackKey(g.group[0], "race"), Size: 1}}, ref.NewReservationOwner(),
			0, ref.ReserveNoFloorGen, 10_000, time.Nanosecond)
		if err != nil || !dec.OK {
			g.t.Errorf("injected mid-probe GC = (%+v, %v), want admitted", dec, err)
		}
	}
	return g.AbsenceRegistry.LapsedAbsenceCoverExists(ctx, ttl)
}

// TestAbsenceReconcile_MarkerRereadClosesProbeRace (Codex round 34, P2
// security): the first-health-stamp evidence checks are two separate
// reads — marker, then physical probe. An admission GC landing between
// them consumes the expired cover AND writes the marker, so the probe
// sees no row while the pass holds a stale marker miss: health would
// stamp unfenced and the surviving marker would never be consulted
// again. A clean probe must re-read the marker before declaring no
// lapse.
func TestAbsenceReconcile_MarkerRereadClosesProbeRace(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	// Multi-pass keyspace (the armed fence must be observable at pass
	// end) plus one ABSENT row whose cover becomes the racing evidence.
	for i := range 30 {
		key := blob.LFSKey("a-fill", strings.Repeat(fmt.Sprintf("%02x", i), 32))
		registerLFS(t, f, "a-fill", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	lost := blob.LFSKey("z-lost", absenceOID('d'))
	registerLFS(t, f, "z-lost", lost, 10)
	pinLFSLeader(t, f)
	if _, err := testReconciler(t, f, true, 100).Run(ctx); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if err := reg.DeleteJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil {
		t.Fatal(err)
	}

	// Pass 2: the marker read finds nothing, the injected GC consumes
	// the expired cover mid-probe, the probe finds nothing — only the
	// post-probe marker re-read can arm the fence.
	res, ok := f.store.(ref.RestoreCoverStore)
	if !ok {
		t.Fatalf("store %T lacks ReservePackBytes", f.store)
	}
	gr := &gcDuringProbeRegistry{AbsenceRegistry: reg, t: t, res: res, group: blob.QuotaRepoGroup("z-lost")}
	rec := NewAbsenceReconciler(gr, f.blobs, true, 12, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := rec.Run(ctx); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if !gr.fired {
		t.Fatal("fixture defect: the mid-probe GC never ran")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after racing GC = (ok=%v, %v), want armed via the marker re-read", ok, err)
	}
}

// TestAbsenceReconcile_ContinuityBoundCoversStampDelay (Codex round 32,
// P1): the health watermark stamps up to one pass wall clock AFTER the
// bulk touch it records, so a watermark age inside [coverTTL-passTimeout,
// coverTTL) does NOT prove the touched covers are still alive — a pause
// landing in that gap can expire them while the raw TTL bound reads as
// continuity. A successful touch at such an age must arm the fence.
// Real clocks: coverTTL=8s → passTimeout=2s → continuity bound 6s; the
// 6.5s pause lands in the gap the raw bound would wave through.
func TestAbsenceReconcile_ContinuityBoundCoversStampDelay(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	base := "repo-stamp-delay"
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}

	// Pass 1 (first ever): no watermark, no lapse evidence — the fence
	// must NOT arm (round 32 upgrade path) and health stamps.
	pinLFSLeader(t, f)
	rec := NewAbsenceReconciler(reg, f.blobs, true, 4, 8*time.Second, time.Second, slog.New(slog.DiscardHandler))
	if _, err := rec.Run(ctx); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || ok {
		t.Fatalf("fence after first pass = (ok=%v, %v), want NOT armed", ok, err)
	}

	// The pause: longer than coverTTL-passTimeout, shorter than
	// coverTTL. Covers touched a pass before the stamp may be dead.
	time.Sleep(6500 * time.Millisecond)

	// Pass 2: the touch succeeds but continuity is no longer provable —
	// the fence arms, and the budget-starved rotation cannot settle it
	// within the pass.
	rec2 := NewAbsenceReconciler(reg, f.blobs, true, 4, 8*time.Second, time.Second, slog.New(slog.DiscardHandler))
	if _, err := rec2.Run(ctx); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after in-gap touch = (ok=%v, %v), want armed", ok, err)
	}
}

// lapsedRegistry injects positive lapse evidence: the probe consulted at
// a first health stamp (Codex round 32, security) reports an expired
// cover row exists, so absent-watermark arming proceeds.
type lapsedRegistry struct {
	AbsenceRegistry
}

func (lr *lapsedRegistry) LapsedAbsenceCoverExists(context.Context, time.Duration) (bool, error) {
	return true, nil
}

// failFenceArmRegistry injects one transient failure into the
// admission-fence arm write, leaving every other scan-state write intact.
type failFenceArmRegistry struct {
	AbsenceRegistry
	fail bool
}

func (fr *failFenceArmRegistry) ArmAbsenceAdmissionFence(ctx context.Context, payload string) error {
	if fr.fail {
		fr.fail = false
		return fmt.Errorf("flaky: injected fence-arm failure")
	}
	return fr.AbsenceRegistry.ArmAbsenceAdmissionFence(ctx, payload)
}

// TestAbsenceReconcile_FailedFenceArmSuppressesHealthStamp (Codex round
// 30, P1): when continuity is unprovable and the required fence arm
// FAILS transiently, the fresh health stamp must not run — it would make
// the next pass treat renewal as continuous and never retry arming,
// leaving admissions open while expired covers stay dead. The stale
// watermark instead re-detects the unprovable continuity next pass, the
// arm retries, and only a durable arm lets health stamp.
func TestAbsenceReconcile_FailedFenceArmSuppressesHealthStamp(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	fr := &failFenceArmRegistry{AbsenceRegistry: &lapsedRegistry{reg}, fail: true}
	r := NewAbsenceReconciler(fr, f.blobs, true, 100, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := r.Run(ctx); err == nil {
		t.Fatal("Run 1: want the injected fence-arm failure reported")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil || ok {
		t.Fatalf("health after failed arm = (ok=%v, %v), want NOT stamped", ok, err)
	}

	// Pass 2 (no injection): the stale watermark re-detects the
	// unprovable continuity, the arm succeeds durably, health stamps,
	// and the tiny registry's same-pass full rotation clears the fence.
	r2 := NewAbsenceReconciler(fr, f.blobs, true, 100, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := r2.Run(ctx); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil || !ok {
		t.Fatalf("health after durable arm = (ok=%v, %v), want stamped", ok, err)
	}
}

// failCursorReadRegistry injects transient failures into the durable
// class-cursor reads, leaving every other scan-state access intact.
type failCursorReadRegistry struct {
	AbsenceRegistry
	fail bool
}

func (fr *failCursorReadRegistry) GetJobScanState(ctx context.Context, job, scope string) (ref.JobScanState, bool, error) {
	if fr.fail && job == ref.ScanJobAbsence && (scope == ref.ScanScopeAbsenceLFS || scope == ref.ScanScopeAbsencePacks) {
		return ref.JobScanState{}, false, fmt.Errorf("flaky: injected cursor read failure")
	}
	return fr.AbsenceRegistry.GetJobScanState(ctx, job, scope)
}

// durableCleanTraversals decodes one persisted class cursor's lifetime
// clean-traversal counter.
func durableCleanTraversals(t *testing.T, reg AbsenceRegistry, scope string) int64 {
	t.Helper()
	st, ok, err := reg.GetJobScanState(context.Background(), ref.ScanJobAbsence, scope)
	if err != nil || !ok {
		t.Fatalf("read %s cursor = (ok=%v, %v)", scope, ok, err)
	}
	var cur absenceCursorState
	if err := json.Unmarshal([]byte(st.Payload), &cur); err != nil {
		t.Fatalf("decode %s cursor: %v", scope, err)
	}
	return cur.CleanTraversals
}

// TestAbsenceReconcile_TransientCursorReadErrorSnapshotSentinel (Codex
// round 33, P1): a pass whose durable cursor reads fail transiently holds
// untrustworthy in-memory zeros — arming the admission fence that pass
// must write the UNKNOWN sentinel (-1), never snapshot the zeros: the
// next healthy pass would reload the real (large) clean counters and
// settle the fence instantly without any recovery rotation. The failing
// pass must also leave the durable cursor rows un-stamped, or the
// counters themselves would be destroyed. The sentinel re-anchors at the
// next healthy load's counters, so release still requires one full fresh
// clean rotation — and eventually happens (liveness).
func TestAbsenceReconcile_TransientCursorReadErrorSnapshotSentinel(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	// Multi-pass keyspace: 30 present rows, so a budget-12 pass cannot
	// complete the recovery rotation the sentinel demands.
	for i := range 30 {
		key := blob.LFSKey("a-fill", strings.Repeat(fmt.Sprintf("%02x", i), 32))
		registerLFS(t, f, "a-fill", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	pinLFSLeader(t, f)
	// Passes 1-2 (generous budget) accumulate durable clean counters —
	// the zeros-vs-reality gap the defect exploited.
	for pass := 1; pass <= 2; pass++ {
		if _, err := testReconciler(t, f, true, 100).Run(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}
	cleanLFS := durableCleanTraversals(t, reg, ref.ScanScopeAbsenceLFS)
	if cleanLFS == 0 {
		t.Fatal("fixture accumulated no clean traversals")
	}

	// Health vanishes (fleet-pause simulation); pass 3's cursor reads
	// fail while injected lapse evidence forces an arm.
	if err := reg.DeleteJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil {
		t.Fatal(err)
	}
	fr := &failCursorReadRegistry{AbsenceRegistry: &lapsedRegistry{reg}, fail: true}
	rec3 := NewAbsenceReconciler(fr, f.blobs, true, 100, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := rec3.Run(ctx); err == nil {
		t.Fatal("pass 3: want the injected read failures reported")
	}
	st, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence)
	if err != nil || !ok {
		t.Fatalf("fence after failing pass = (ok=%v, %v), want armed", ok, err)
	}
	var snap map[string]int64
	if err := json.Unmarshal([]byte(st.Payload), &snap); err != nil {
		t.Fatalf("decode fence snapshot: %v", err)
	}
	if snap["lfs"] != -1 || snap["packs"] != -1 {
		t.Fatalf("fence snapshot = %v, want the unknown sentinel", snap)
	}
	// The unreadable cursors were NOT re-stamped: the durable lifetime
	// counters survive the failing pass.
	if got := durableCleanTraversals(t, reg, ref.ScanScopeAbsenceLFS); got != cleanLFS {
		t.Fatalf("durable clean traversals after failing pass = %d, want %d preserved", got, cleanLFS)
	}

	// Pass 4 (healthy, budget-starved): the sentinel re-anchors at the
	// counters just loaded — a zero snapshot would have compared the
	// reloaded counters against 0 and cleared instantly.
	if _, err := testReconciler(t, f, true, 12).Run(ctx); err != nil {
		t.Fatalf("pass 4: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after one starved healthy pass = (ok=%v, %v), want still armed", ok, err)
	}
	// Liveness: complete fresh clean rotations still release it.
	cleared := false
	for pass := 5; pass <= 12 && !cleared; pass++ {
		if _, err := testReconciler(t, f, true, 12).Run(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		_, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence)
		if err != nil {
			t.Fatalf("pass %d fence read: %v", pass, err)
		}
		cleared = !ok
	}
	if !cleared {
		t.Fatal("fence never cleared after full recovery rotations")
	}
}

// TestAbsenceReconcile_SeededCandidaciesRetainedPastCap (Codex round 33,
// P2): saveProofs enforced the candidacy cap over an UNORDERED map walk,
// so a long-waiting seeded half-proof could be evicted by fresh
// candidacies pass after pass — its base's legacy nonce then bulk-renews
// forever. Retention is now deterministic and seeded-first. Exercised at
// the saveProofs seam (the pass-level candidacy machinery has its own
// tests); repeated iterations make a random-order regression essentially
// certain to surface.
func TestAbsenceReconcile_SeededCandidaciesRetainedPastCap(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	rec := testReconciler(t, f, true, 100)
	rec.proofCap = 1
	for iter := range 10 {
		rec.legacyRows = nil
		rec.proofGens = map[string]map[string]absenceProofEntry{}
		rec.cleanBases = map[string]map[string]bool{absenceClassLFS: {}, absenceClassPacks: {}}
		rec.renewalComplete = true
		// 40 fresh candidacies, every one sorting BEFORE the seeded base.
		for i := range 40 {
			base := fmt.Sprintf("a-fresh-%02d", i)
			rec.legacyRows = append(rec.legacyRows, ref.AbsenceCoverRow{Base: base})
			rec.cleanBases[absenceClassLFS][base] = true
		}
		rec.legacyRows = append(rec.legacyRows, ref.AbsenceCoverRow{Base: "zz-seeded"})
		rec.proofGens["zz-seeded"] = map[string]absenceProofEntry{absenceClassLFS: {}}
		var errs boundedErrs
		rec.saveProofs(ctx, &errs)
		if !errs.ok() {
			t.Fatalf("iteration %d: saveProofs errors: %v", iter, errs.join())
		}
		st, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceProofs)
		if err != nil || !ok {
			t.Fatalf("iteration %d: proofs row = (ok=%v, %v)", iter, ok, err)
		}
		var kept map[string]map[string]absenceProofEntry
		if err := json.Unmarshal([]byte(st.Payload), &kept); err != nil {
			t.Fatalf("iteration %d: decode proofs: %v", iter, err)
		}
		if len(kept) != 1 || kept["zz-seeded"] == nil {
			t.Fatalf("iteration %d: kept %v, want exactly the seeded candidacy", iter, kept)
		}
	}
}

// recordingStatter records every statted key, pass-through otherwise.
type recordingStatter struct {
	inner BlobStatter
	mu    sync.Mutex
	keys  []string
}

func (rs *recordingStatter) Stat(ctx context.Context, key string) (int64, error) {
	rs.mu.Lock()
	rs.keys = append(rs.keys, key)
	rs.mu.Unlock()
	return rs.inner.Stat(ctx, key)
}

func (rs *recordingStatter) statted(substr string) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for _, k := range rs.keys {
		if strings.Contains(k, substr) {
			return true
		}
	}
	return false
}

// TestAbsenceReconcile_FencedPassDefersFoldsToRecovery (Codex round 33,
// P2 security): the pre-rotation hint fold's re-stat is run-to-completion
// and can burn the whole pass deadline on one large base — while the
// admission fence is armed, that starves the recovery rotations whose
// CLEAN traversal advance is the only thing that settles the fence,
// denying finite quota-gated writes fleet-wide indefinitely. A fenced
// pass must not fold at all (deferral is always safe: hints persist
// seeded-first, nonces keep renewing); once the fence settles, folding
// resumes and the deferred candidacy completes.
func TestAbsenceReconcile_FencedPassDefersFoldsToRecovery(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	// Budget-starved keyspace: 30 a-fill rows sort BEFORE zz-legacy's,
	// so any zz-legacy Stat in the fenced pass can only come from a fold
	// re-stat.
	for i := range 30 {
		key := blob.LFSKey("a-fill", strings.Repeat(fmt.Sprintf("%02x", i), 32))
		registerLFS(t, f, "a-fill", key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	lkey := blob.LFSKey("zz-legacy", absenceOID('e'))
	registerLFS(t, f, "zz-legacy", lkey, 10)
	f.upload(lkey, strings.Repeat("x", 10))
	nonce := ref.AbsenceCoverPrefix + "zz-legacy/0123456789abcdef0123"
	res, ok := f.store.(ref.RestoreCoverStore)
	if !ok {
		t.Fatalf("store %T lacks ReserveRestoreCover", f.store)
	}
	if err := ref.ReserveRestoreCover(ctx, res, blob.QuotaRepoGroup("zz-legacy"), nonce, 10, time.Hour); err != nil {
		t.Fatalf("plant legacy nonce: %v", err)
	}
	// Persist a complete two-class fold candidacy at the CURRENT fence
	// generation — the mixed-version half-proof waiting for commitment.
	var gen int64
	for _, member := range blob.QuotaRepoGroup("zz-legacy") {
		g, err := f.store.LFSAdoptionGen(ctx, member)
		if err != nil {
			t.Fatalf("LFSAdoptionGen(%q): %v", member, err)
		}
		gen += g
	}
	payload := fmt.Sprintf(`{"zz-legacy":{"lfs":{"gen":%d},"packs":{"gen":%d}}}`, gen, gen)
	if err := reg.PutJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceProofs, payload, 0); err != nil {
		t.Fatal(err)
	}

	// Pass 1: no health watermark plus injected lapse evidence — the
	// fence arms at renewal, BEFORE the fold decision. The fenced pass
	// must spend its deadline on recovery only: no zz-legacy re-stat, no
	// fold, nonce stays live.
	pinLFSLeader(t, f)
	rs := &recordingStatter{inner: f.blobs}
	rec := NewAbsenceReconciler(&lapsedRegistry{reg}, rs, true, 12, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := rec.Run(ctx); err != nil {
		t.Fatalf("fenced pass: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after pass 1 = (ok=%v, %v), want armed", ok, err)
	}
	if rs.statted("zz-legacy") {
		t.Fatal("fenced pass re-statted the fold candidate: fold work stole the recovery deadline")
	}
	nonceState := func() (live, folded bool) {
		t.Helper()
		for after := ""; ; {
			listed, err := reg.ListAbsenceCovers(ctx, after, 100, time.Hour)
			if err != nil {
				t.Fatalf("list covers: %v", err)
			}
			if len(listed) == 0 {
				return live, folded
			}
			for _, row := range listed {
				if row.Key == nonce {
					live = true
					folded = row.Folded
				}
			}
			after = listed[len(listed)-1].RawKey
		}
	}
	if live, folded := nonceState(); !live || folded {
		t.Fatalf("nonce after fenced pass = (live=%v, folded=%v), want live un-folded", live, folded)
	}

	// Recovery rotations settle the fence; the deferred candidacy then
	// folds on a later un-fenced pass (hints persisted seeded-first).
	cleared := false
	for pass := 2; pass <= 10 && !cleared; pass++ {
		if _, err := testReconciler(t, f, true, 12).Run(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		_, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence)
		if err != nil {
			t.Fatalf("pass %d fence read: %v", pass, err)
		}
		cleared = !ok
	}
	if !cleared {
		t.Fatal("fence never cleared after full recovery rotations")
	}
	foldedYet := false
	for pass := 0; pass < 4 && !foldedYet; pass++ {
		if _, err := testReconciler(t, f, true, 100).Run(ctx); err != nil {
			t.Fatalf("post-clear pass: %v", err)
		}
		live, folded := nonceState()
		foldedYet = !live || folded
	}
	if !foldedYet {
		t.Fatal("deferred candidacy never folded after the fence settled")
	}
}

// TestAbsenceReconcile_DirtyRecoveryRotationKeepsFenceArmed (Codex round
// 31, P1): a recovery rotation spanning multiple passes whose EARLY pass
// hits a cover-charge error must not release the admission fence when a
// LATER error-free pass crosses the origin — the failed row was neither
// verified nor covered, and only the persisted Dirty flag remembers it.
// Fence release gates on the CLEAN traversal counter: the dirty rotation
// never advances it, and the next fully clean rotation does.
func TestAbsenceReconcile_DirtyRecoveryRotationKeepsFenceArmed(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	// Multi-pass LFS keyspace: 30 rows, budget 12. Row 0 sorts first
	// and is registered but MISSING — its deficit charge is the error
	// injection target for the rotation's first pass.
	for i := range 30 {
		key := blob.LFSKey("a-fill", strings.Repeat(fmt.Sprintf("%02x", i), 32))
		registerLFS(t, f, "a-fill", key, 10)
		if i != 0 {
			f.upload(key, strings.Repeat("x", 10))
		}
	}

	// Pass 1: no health watermark and injected lapse evidence — the
	// fence arms with a zero clean-counter snapshot. The missing row's
	// deficit charge fails (settle, then the range-end retry), so the
	// rotation is durably DIRTY.
	pinLFSLeader(t, f)
	flaky := &flakyRegistry{AbsenceRegistry: &lapsedRegistry{reg}, failLeft: 2}
	rec := NewAbsenceReconciler(flaky, f.blobs, true, 12, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := rec.Run(ctx); err == nil {
		t.Fatal("pass 1: want the injected charge failures reported")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after pass 1 = (ok=%v, %v), want armed", ok, err)
	}

	// Error-free passes complete the dirty rotation. At its origin
	// crossing the raw counter advances but the CLEAN counter must not —
	// the fence stays armed even though this pass itself saw no error.
	wrapped := false
	for pass := 2; pass <= 8 && !wrapped; pass++ {
		rec := testReconciler(t, f, true, 12)
		if _, err := rec.Run(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		wrapped = rec.lfs.Traversals >= 1
		if wrapped && rec.lfs.CleanTraversals != 0 {
			t.Fatalf("clean traversals after dirty rotation = %d, want 0", rec.lfs.CleanTraversals)
		}
	}
	if !wrapped {
		t.Fatal("dirty rotation never crossed the origin")
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after dirty rotation wrap = (ok=%v, %v), want STILL armed", ok, err)
	}

	// The next rotation is fully clean (the deficit charged successfully
	// once the injection lapsed) and releases the fence.
	cleared := false
	for pass := 0; pass < 8 && !cleared; pass++ {
		rec := testReconciler(t, f, true, 12)
		if _, err := rec.Run(ctx); err != nil {
			t.Fatalf("recovery pass %d: %v", pass, err)
		}
		_, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence)
		if err != nil {
			t.Fatalf("recovery pass %d fence read: %v", pass, err)
		}
		cleared = !ok
	}
	if !cleared {
		t.Fatal("fence never cleared after a fully clean rotation")
	}
}

// TestAbsenceReconcile_StaleCursorRestartPreservesFenceCounters (Codex
// round 31, P1): a stale-cursor restart under an ARMED fence must
// preserve the lifetime traversal counters — zeroing them would leave
// settleAdmissionFence comparing a restart-at-zero count against the old
// arming snapshot, blocking finite quota admissions for as many extra
// rotations as the job ever completed. One clean rotation from the reset
// origin must still release the fence.
func TestAbsenceReconcile_StaleCursorRestartPreservesFenceCounters(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	reg := absenceRegistry(t, f)
	base := "repo-stale-fence"
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}

	// Passes 1-3 build up clean-traversal history (pass 1 arms on the
	// missing health watermark and settles the same pass: tiny keyspace).
	pinLFSLeader(t, f)
	var built int64
	for pass := 1; pass <= 3; pass++ {
		rec := testReconciler(t, f, true, 100)
		if _, err := rec.Run(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		built = rec.lfs.CleanTraversals
	}
	if built == 0 {
		t.Fatal("no clean traversals accumulated")
	}

	// Deleting the health watermark plus injected lapse evidence makes
	// pass 4's SUCCESSFUL touch arm the fence with the accumulated
	// counters as its snapshot; the budget-starved pass parks
	// mid-rotation without settling. Health is freshly restamped.
	if err := reg.DeleteJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceRenewalHealth); err != nil {
		t.Fatalf("drop health watermark: %v", err)
	}
	rec4 := NewAbsenceReconciler(&lapsedRegistry{reg}, f.blobs, true, 4, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := rec4.Run(ctx); err != nil {
		t.Fatalf("pass 4: %v", err)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || !ok {
		t.Fatalf("fence after pass 4 = (ok=%v, %v), want armed", ok, err)
	}

	// A recovery pause outlives the (shrunk) cursor bound while renewal
	// health stays young: the restart must keep the counters, so ONE
	// clean rotation from origin releases the fence.
	rec5 := testReconciler(t, f, true, 100)
	rec5.cursorStaleAge = time.Millisecond
	time.Sleep(1200 * time.Millisecond) // cross SQLite's second stamp
	if _, err := rec5.Run(ctx); err != nil {
		t.Fatalf("recovery pass: %v", err)
	}
	if rec5.lfs.CleanTraversals <= built {
		t.Fatalf("clean traversals after restart = %d, want counters preserved past %d", rec5.lfs.CleanTraversals, built)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceAdmissionFence); err != nil || ok {
		t.Fatalf("fence after one clean restarted rotation = (ok=%v, %v), want cleared", ok, err)
	}
}

// TestAbsenceReconcile_GapMutationDefersLegacyRetirement (Codex rounds
// 19-20): a foreign mutation landing while the cursor is parked mid-range
// breaks fence continuity — the resumed completion must NOT prove the
// base (rows may have changed while no segment was watching), so the
// nonce stays live. The next traversal's continuous observation retires
// it.
func TestAbsenceReconcile_GapMutationDefersLegacyRetirement(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-gap-mutation"
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	// Pass 1 parks mid-range; a foreign mutation bumps the group while
	// parked.
	pinLFSLeader(t, f)
	workerA := testReconciler(t, f, true, 8)
	if _, err := workerA.Run(ctx); err != nil {
		t.Fatalf("worker A: %v", err)
	}
	if err := absenceRegistry(t, f).BumpLFSAdoptionGen(ctx, base); err != nil {
		t.Fatalf("foreign bump: %v", err)
	}

	// Pass 2: the resumed range completes but is tainted — no proof, no
	// retirement.
	workerB := testReconciler(t, f, true, 100)
	report, err := workerB.Run(ctx)
	if err != nil {
		t.Fatalf("worker B: %v", err)
	}
	if report.CoversDecaying != 0 {
		t.Fatalf("pass 2 report = %+v, want retirement deferred on the broken fence", report)
	}

	// Pass 3: the next observation completes with the fence intact (its
	// fresh range began in pass 2's wrapped tail and the park gap
	// stayed quiet) and retires the nonce.
	workerC := testReconciler(t, f, true, 100)
	report, err = workerC.Run(ctx)
	if err != nil {
		t.Fatalf("worker C: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("pass 3 report = %+v, want the nonce retired after the continuous re-observation", report)
	}
	if got := ledgerReserved(t, f, base); got != 555 {
		t.Fatalf("reserved = %d, want only the decaying nonce", got)
	}
}

// TestAbsencePassTimeoutClampedToTTL (Codex round 3, security): the pass
// wall-clock cap tightens to a quarter of the cover TTL so a long scan can
// never hold the next renewal past a live cover's expiry.
func TestAbsencePassTimeoutClampedToTTL(t *testing.T) {
	f := newSweepFixture(t)
	log := slog.New(slog.DiscardHandler)
	r := NewAbsenceReconciler(absenceRegistry(t, f), f.blobs, true, 100, 40*time.Minute, time.Minute, log)
	if r.passTimeout != 10*time.Minute {
		t.Fatalf("passTimeout = %v with a 40m TTL, want 10m (TTL/4)", r.passTimeout)
	}
	r = NewAbsenceReconciler(absenceRegistry(t, f), f.blobs, true, 100, 8*time.Hour, time.Minute, log)
	if r.passTimeout != time.Hour {
		t.Fatalf("passTimeout = %v with an 8h TTL, want the 1h ceiling", r.passTimeout)
	}
}

// TestAbsenceReconcile_TruncatedCovered (Codex round 4, security): a blob
// physically present but SHORTER than its registered size deflates the
// floor by the deficit — covered exactly like partial absence, and
// RELEASED once the full bytes return.
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
		t.Fatalf("reserved = %d, want the 41-byte deficit across both classes", got)
	}

	// Full bytes return: the next pass's range re-derivation releases
	// both covers outright — no TTL tail.
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
	if report.CoversReleased != 2 || report.LFSRowsTruncated != 0 || report.PackRowsTruncated != 0 {
		t.Fatalf("pass 2 report = %+v, want both healed covers released", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved = %d, want 0 after the releases", got)
	}
}

// TestAbsenceReconcile_TruncationPartialHeal (Codex round 5, security): the
// re-derived total compares fresh Stats against the authoritative
// REGISTERED size, so a partial heal shrinks the cover to the residual
// deficit — never releases it while bytes are still missing.
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

	// Partial heal: 25 of 40 bytes now physical → residual deficit 15.
	// The next pass's exact re-derivation ROTATES the cover down to 15
	// and deletes the stale 35-byte row.
	if got := f.upload(key, strings.Repeat("p", 25)); got != 25 {
		t.Fatalf("healed to %d bytes, want 25", got)
	}
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversRotated != 1 || report.LFSRowsTruncated != 1 {
		t.Fatalf("pass 2 report = %+v, want the cover rotated to the residual deficit", report)
	}
	if got := ledgerReserved(t, f, base); got != 15 {
		t.Fatalf("reserved = %d, want exactly the residual 15-byte deficit", got)
	}
}

// TestAbsenceReconcile_PartialLossBecomesTotal (Codex round 9, security):
// a covered short blob that later disappears entirely must grow its cover
// to the full registered size — the same-key grow path recomputes totals,
// never renews a stale smaller deficit forever.
func TestAbsenceReconcile_PartialLossBecomesTotal(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-part-to-total"
	key := blob.LFSKey(base, absenceOID('6'))
	f.upload(key, strings.Repeat("s", 99)) // 99 of 100 bytes
	registerLFS(t, f, base, key, 100)      // deficit 1

	r := testReconciler(t, f, true, 1000)
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 1 {
		t.Fatalf("reserved = %d, want the 1-byte deficit", got)
	}

	// The remaining bytes vanish: the cover must grow to 100.
	if err := f.blobs.Delete(context.Background(), key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 100 {
		t.Fatalf("reserved = %d, want the full 100-byte loss covered", got)
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

// TestAbsenceReconcile_ShortSpellingFullTwinNotCovered (Codex round 12): a
// truncated registered spelling whose twin still holds the COMPLETE bytes
// is physically compensated — both copies are in the usage listing, so a
// covered deficit would over-cover indefinitely.
func TestAbsenceReconcile_ShortSpellingFullTwinNotCovered(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-fulltwin"
	oid := absenceOID('9')
	canonical := blob.LFSKey(base, oid)
	f.upload(canonical, "tiny")                                     // 4 of 32 bytes
	f.upload(blob.LFSLegacyKey(base, oid), strings.Repeat("f", 32)) // full twin
	registerLFS(t, f, base, canonical, 32)

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsTwinOnly != 1 || report.LFSRowsTruncated != 0 || report.CoversCharged != 0 {
		t.Fatalf("report = %+v, want twin compensation without covers", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
}

// TestAbsenceReconcile_AggregateGrowsInPlace (Codex round 5 on PR #123):
// multiple losses in one base aggregate into ONE cover row that grows
// in place as the traversal discovers them — per-key rows would collide
// with the per-base reservation-row cap on big incidents.
func TestAbsenceReconcile_AggregateGrowsInPlace(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-aggregate"
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
	if report.LFSRowsAbsent != 2 {
		t.Fatalf("pass 1 report = %+v, want both losses discovered", report)
	}
	if got := ledgerReserved(t, f, base); got != 100 {
		t.Fatalf("reserved = %d, want the netted 100-byte aggregate", got)
	}

	// Heal the 70-byte loss: the next pass's exact re-derivation rotates
	// the aggregate down to 30 and deletes the stale 100-byte row.
	if got := f.upload(big, strings.Repeat("z", 70)); got != 70 {
		t.Fatalf("healed to %d bytes, want 70", got)
	}
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversRotated != 1 {
		t.Fatalf("pass 2 report = %+v, want the healed loss rotated out", report)
	}
	if got := ledgerReserved(t, f, base); got != 30 {
		t.Fatalf("reserved = %d, want the exact 30-byte residual", got)
	}

	// Steady state: the next pass renews the rotated key at 30.
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversRenewed != 1 || report.BytesCovered != 30 {
		t.Fatalf("pass 3 report = %+v, want one 30-byte renewal on the rotated key", report)
	}
}

// TestAbsenceReconcile_HealedThenNewLossRotatesKey (Codex round 6 on PR
// #123): a base whose loss heals entirely and later loses a SMALLER object
// must not re-adopt the old GREATEST-pinned row. The release deletes the
// old row and the re-derivation rotates to a fresh incarnation charged
// exactly.
func TestAbsenceReconcile_HealedThenNewLossRotatesKey(t *testing.T) {
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

	// Heal the loss entirely, then lose a SMALLER object.
	if got := f.upload(bigKey, strings.Repeat("q", 100)); got != 100 {
		t.Fatalf("healed to %d bytes, want 100", got)
	}
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('4')), 10)
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	// The exact re-derivation charges 10, never the old GREATEST-pinned
	// 100 (the stale row was rotated away or released).
	if got := ledgerReserved(t, f, base); got != 10 {
		t.Fatalf("reserved = %d, want exactly the fresh 10-byte cover", got)
	}
}

// TestAbsenceReconcile_SplitShortCopiesClassifiedTruncated (Codex round
// 13): two individually short copies whose sizes sum past the registered
// size charge nothing (all bytes are listed) but must classify as
// truncation — neither spelling is complete, so nothing can restore it.
func TestAbsenceReconcile_SplitShortCopiesClassifiedTruncated(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-split-short"
	oid := absenceOID('e')
	f.upload(blob.LFSKey(base, oid), strings.Repeat("a", 60))
	f.upload(blob.LFSLegacyKey(base, oid), strings.Repeat("b", 40))
	registerLFS(t, f, base, blob.LFSKey(base, oid), 100)

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.LFSRowsTruncated != 1 || report.LFSRowsTwinOnly != 0 || report.CoversCharged != 0 {
		t.Fatalf("report = %+v, want split short copies reported as truncation without covers", report)
	}
}

// TestAbsenceReconcile_LegacyCoverRetiredOnProof (Codex rounds 4, 5, 17,
// 18, 19): a nonce-keyed cover minted by a pre-#125 worker is ITSELF the
// cover for whatever it aggregates — no bytes are ever transferred into
// the deterministic incarnations (every transfer scheme re-interpreted an
// old worker's periodic same-key refresh as new deficit and re-added it,
// round 19 P1). The row stays live (bulk-touched) until BOTH rotations
// prove its base clean within one pass — the recount charges the true
// deficits into the deterministic heads, subsuming the nonce — and then
// decays by TTL under the folded sentinel; the row itself is never
// deleted (an old replica may depend on it mid-roll).
func TestAbsenceReconcile_LegacyCoverRetiredOnProof(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-legacy-cover"
	// Registered-but-missing rows in BOTH classes: the rotations charge
	// their exact deficits into the deterministic heads; the legacy
	// cover carries a LARGER total (the old replica had seen more).
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('a')), 100)
	if err := f.store.RegisterPacks(ctx, base, []ref.Pack{{ID: "lost", Key: blob.PackKey(base, "lost"), Size: 200}}); err != nil {
		t.Fatal(err)
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	// Pass 1: discovery charges the exact per-class deficits; both clean
	// same-pass ranges prove the nonce redundant, and it starts decaying
	// at its own 555 — never re-added anywhere.
	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.CoversDecaying != 1 || report.CoversRotated != 0 {
		t.Fatalf("pass 1 report = %+v, want the nonce retired without any transfer or right-sizing", report)
	}
	if got := ledgerReserved(t, f, base); got != 555+100+200 {
		t.Fatalf("reserved = %d, want the decaying nonce plus both exact incarnations", got)
	}

	// Pass 2: steady state — the exact incarnations renew; the nonce
	// keeps decaying (never deleted, still inside its TTL).
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversDecaying != 1 || report.CoversRenewed < 2 || report.CoversReleased != 0 {
		t.Fatalf("pass 2 report = %+v, want incarnations renewed and nonce still decaying", report)
	}
	if got := ledgerReserved(t, f, base); got != 555+100+200 {
		t.Fatalf("reserved after pass 2 = %d, want unchanged", got)
	}

	// An old replica re-charges the SAME nonce mid-roll at a GROWN total
	// (Codex rounds 7+19): the re-charge overwrites the folded sentinel —
	// a new reliance — so pass 3 re-proves the base and re-retires the
	// nonce at its grown total. The deterministic heads must NOT grow:
	// the nonce's own row admits its claim until it decays.
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 900, time.Hour); err != nil {
		t.Fatalf("re-grow legacy cover: %v", err)
	}
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversDecaying != 1 || report.CoversRotated != 0 {
		t.Fatalf("pass 3 report = %+v, want the re-charged nonce re-retired without transfers", report)
	}
	if got := ledgerReserved(t, f, base); got != 900+100+200 {
		t.Fatalf("reserved after pass 3 = %d, want the grown nonce plus unchanged incarnations", got)
	}
}

// TestAbsenceChargeCover_SameKeyBypassesSkip (Codex round 7, P1): the
// pass-level coverSkip memo predicts that further fresh MINTS for a base
// would fail like the denial that set it (e.g. the active-row cap) — but a
// same-key re-charge of an existing row is excluded from that cap by the
// ledger, so renewals and fallback renewals must bypass the memo or a
// transient denial lets live covers decay.
func TestAbsenceChargeCover_SameKeyBypassesSkip(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-cover-skip"
	r := testReconciler(t, f, true, 1000)
	existing := absenceIncarnationKey(base, absenceClassLFS, 1)
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), existing, 100, time.Hour); err != nil {
		t.Fatalf("seed cover: %v", err)
	}

	st := newAbsencePassState()
	st.coverSkip[base] = true
	report := &AbsenceReport{}
	errs := &boundedErrs{}
	if r.chargeCover(ctx, base, absenceIncarnationKey(base, absenceClassPacks, 1), 50, false, report, st, errs) {
		t.Fatal("fresh mint under coverSkip succeeded, want skipped")
	}
	if report.CoverErrors != 1 {
		t.Fatalf("report = %+v, want the skipped mint counted as a cover error", report)
	}
	if !r.chargeCover(ctx, base, existing, 100, true, report, st, errs) {
		t.Fatalf("same-key re-charge under coverSkip failed, want bypass (errs=%v)", errs.errs)
	}
	if got := ledgerReserved(t, f, base); got != 100 {
		t.Fatalf("reserved = %d, want only the renewed existing cover", got)
	}
}

// flakyRegistry injects failures into cover reservations (only rows under
// the absence-cover prefix, so registry fixtures build normally): the first
// failLeft cover charges error out, everything after succeeds.
type flakyRegistry struct {
	AbsenceRegistry
	failLeft int
}

func (fr *flakyRegistry) ReservePackBytes(ctx context.Context, group []string, packs []ref.Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ref.ReserveDecision, error) {
	if fr.failLeft > 0 && len(packs) == 1 && strings.HasPrefix(packs[0].Key, ref.AbsenceCoverPrefix) {
		fr.failLeft--
		return ref.ReserveDecision{}, fmt.Errorf("flaky: injected reserve failure")
	}
	return fr.AbsenceRegistry.ReservePackBytes(ctx, group, packs, owner, usageFloor, floorGen, quotaBytes, ttl)
}

// TestAbsenceReconcile_DiscoveryRunsWhenCoversExceedBudget (Codex round
// 14, security): renewal always offers the full cover listing, so a live
// population at or beyond the whole budget used to clamp the rotation
// allowance to zero — and the bulk touch keeps those covers live, making
// starvation an absorbing state. Discovery must retain a guaranteed
// allowance: a fresh loss is still found and covered in the same pass.
func TestAbsenceReconcile_DiscoveryRunsWhenCoversExceedBudget(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	// 8 live deterministic covers = the whole budget.
	for i := range 8 {
		base := fmt.Sprintf("repo-crowd-%d", i)
		if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), absenceIncarnationKey(base, absenceClassLFS, 1), 10, time.Hour); err != nil {
			t.Fatalf("seed cover %d: %v", i, err)
		}
	}
	base := "repo-fresh-loss"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('j')), 100)

	r := testReconciler(t, f, true, 8)
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.RenewalOverBudget {
		t.Fatalf("report = %+v, want the overrun flagged", report)
	}
	if report.LFSRowsExamined == 0 || report.LFSRowsAbsent == 0 {
		t.Fatalf("report = %+v, want discovery to run despite the cover crowd", report)
	}
	if got := ledgerReserved(t, f, base); got != 100 {
		t.Fatalf("fresh loss reserved = %d, want covered in the same pass", got)
	}
}

// TestAbsenceReconcile_LegacyRefoldAfterHeadsReleased (Codex rounds
// 13+19): the deterministic incarnations can be RELEASED by any worker
// after a heal. When the bytes vanish AGAIN and an old replica re-charges
// the SAME nonce at an EQUAL total for the new incident, the re-charge
// un-marks the row (owner overwritten) — so the next pass must re-prove
// the base (discovery re-charges the exact deficits) before the nonce may
// decay again.
func TestAbsenceReconcile_LegacyRefoldAfterHeadsReleased(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-refold"
	lfsKey := blob.LFSKey(base, absenceOID('i'))
	registerLFS(t, f, base, lfsKey, 100)
	packKey := blob.PackKey(base, "lost")
	if err := f.store.RegisterPacks(ctx, base, []ref.Pack{{ID: "lost", Key: packKey, Size: 200}}); err != nil {
		t.Fatal(err)
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	// Pass 1: discovery charges the exact deficits (100 lfs, 200 packs)
	// and the clean double proof retires the nonce.
	r := testReconciler(t, f, true, 1000)
	if _, err := r.Run(ctx); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	// Every byte heals; pass 2 releases both incarnations (the nonce
	// keeps decaying under its durable fold marker).
	f.upload(lfsKey, strings.Repeat("a", 100))
	f.upload(packKey, strings.Repeat("b", 200))
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversReleased != 2 {
		t.Fatalf("pass 2 report = %+v, want both healed incarnations released", report)
	}

	// The bytes disappear AGAIN and an old replica — whose in-memory
	// absentBase survived — re-charges the SAME nonce at the SAME total.
	if err := f.blobs.Delete(ctx, lfsKey); err != nil {
		t.Fatal(err)
	}
	if err := f.blobs.Delete(ctx, packKey); err != nil {
		t.Fatal(err)
	}
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("re-charge legacy cover: %v", err)
	}

	// Pass 3 (same process): the re-charge un-marked the row; discovery
	// re-charges the exact deficits and the clean double proof retires
	// the nonce again — nothing is ever transferred from it.
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversDecaying != 1 || report.CoversRotated != 0 {
		t.Fatalf("pass 3 report = %+v, want the nonce re-retired after the fresh proof", report)
	}
	if got := ledgerReserved(t, f, base); got != 555+100+200 {
		t.Fatalf("reserved = %d, want the decaying nonce plus both re-minted exact incarnations", got)
	}
}

// TestAbsenceReconcile_RowlessLegacyCoversRetiredByGapProof (Codex rounds
// 17+19): pre-#125 workers minted one random nonce PER REPLICA/PASS, so
// several live nonces can compensate DISJOINT deficits that admission
// sums — no transfer into the deterministic heads can reconcile that with
// old replicas still re-charging their own rows mid-roll (rounds 17-18
// tried GREATEST- and additive-transfer variants; round 19 P1 broke
// both), so each nonce row stays live as its own cover until proven
// redundant. Here the base holds no rows in EITHER class, so the proof is
// the row-less keyset gap: both rotations' page adjacency proves a zero
// deficit, ALL of the base's nonces retire in the same pass, and no
// deterministic head is ever minted — nothing is transferred anywhere.
func TestAbsenceReconcile_RowlessLegacyCoversRetiredByGapProof(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-rowless-legacy"
	reg := absenceRegistry(t, f)
	for _, bytes := range []int64{555, 300} {
		if err := ref.ReserveRestoreCover(ctx, reg, blob.QuotaRepoGroup(base),
			ref.AbsenceCoverPrefix+base+"/"+ref.NewReservationOwner(), bytes, time.Hour); err != nil {
			t.Fatalf("seed nonce: %v", err)
		}
	}

	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.CoversDecaying != 2 {
		t.Fatalf("report = %+v, want both nonces retired by the gap proof", report)
	}
	for _, class := range []string{absenceClassLFS, absenceClassPacks} {
		if _, ok, err := reg.AbsenceCoverHead(ctx, absenceIncarnationPrefix(base, class), time.Hour); err != nil || ok {
			t.Fatalf("%s head = (%v, %v), want no deterministic head minted", class, ok, err)
		}
	}
	if got := ledgerReserved(t, f, base); got != 555+300 {
		t.Fatalf("reserved = %d, want only the decaying nonces (nothing transferred)", got)
	}
}

// foldCASRegistry intercepts the fold CAS: hook (if set) runs once
// strictly before the first attempt — simulating an old replica's
// re-charge racing the listing — and failMarkOnce injects one transient
// CAS error. Everything else passes through.
type foldCASRegistry struct {
	AbsenceRegistry
	hook         func()
	failMarkOnce bool
}

func (fr *foldCASRegistry) CASAbsenceCoverOwner(ctx context.Context, rawKey, expectedOwner string, expectedBytes int64, newOwner string) (bool, error) {
	if newOwner == ref.AbsenceFoldedOwner {
		if fr.hook != nil {
			hook := fr.hook
			fr.hook = nil
			hook()
		}
		if fr.failMarkOnce {
			fr.failMarkOnce = false
			return false, fmt.Errorf("flaky: injected mark failure")
		}
	}
	return fr.AbsenceRegistry.CASAbsenceCoverOwner(ctx, rawKey, expectedOwner, expectedBytes, newOwner)
}

// TestAbsenceReconcile_RechargeRaceLosesFoldCAS (Codex round 19, P1): an
// old replica's periodic same-key TTL refresh re-charges the nonce
// BETWEEN the renewal listing and the fold CAS — the refresh overwrites
// owner (and may GREATEST-grow bytes), so the CAS must miss: the row is a
// newer reliance, stays live under the bulk touch, and nothing is written
// — the transfer designs this model replaced re-interpreted exactly this
// refresh as fresh deficit and re-added it every pass. The next pass
// re-proves and retires the refreshed row.
func TestAbsenceReconcile_RechargeRaceLosesFoldCAS(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-recharge-race"
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed nonce: %v", err)
	}
	reg := &foldCASRegistry{AbsenceRegistry: absenceRegistry(t, f)}
	reg.hook = func() {
		// The old replica refreshes its nonce at a grown total.
		if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 700, time.Hour); err != nil {
			t.Fatalf("re-charge nonce: %v", err)
		}
	}
	r := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.CoversDecaying != 0 {
		t.Fatalf("pass 1 report = %+v, want the raced fold CAS to miss", report)
	}
	if got := ledgerReserved(t, f, base); got != 700 {
		t.Fatalf("reserved = %d, want only the refreshed nonce (no head, no re-add)", got)
	}

	// Pass 2: no race — the refreshed row is re-proven and retired.
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("pass 2 report = %+v, want the refreshed nonce retired", report)
	}
	if got := ledgerReserved(t, f, base); got != 700 {
		t.Fatalf("reserved after pass 2 = %d, want unchanged", got)
	}
}

// TestAbsenceReconcile_MarkFailureRetried (Codex rounds 18+19): a
// transient error on the fold CAS leaves the nonce un-marked — still
// live, still bulk-touched, nothing else written — and the next pass
// simply re-proves and retires it. There is no partial state to repair:
// retirement is a single CAS, never a byte transfer.
func TestAbsenceReconcile_MarkFailureRetried(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-mark-fail"
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base),
		ref.AbsenceCoverPrefix+base+"/"+ref.NewReservationOwner(), 555, time.Hour); err != nil {
		t.Fatalf("seed nonce: %v", err)
	}
	reg := &foldCASRegistry{AbsenceRegistry: absenceRegistry(t, f), failMarkOnce: true}
	r := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	report, err := r.Run(ctx)
	if err == nil || report.CoversDecaying != 0 {
		t.Fatalf("run 1 = (%+v, %v), want the injected mark failure reported and no decay", report, err)
	}
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("run 2 report = %+v, want the nonce marked on retry", report)
	}
	if got := ledgerReserved(t, f, base); got != 555 {
		t.Fatalf("reserved = %d, want only the decaying nonce", got)
	}
}

// TestAbsenceReconcile_StaleCursorRestartsFromOrigin (Codex round 17,
// security): a rotation cursor whose row's DB-clock age outlives the
// cover-renewal guarantee (a fleet pause approaching the cover TTL) may
// have covers expired behind it — the losses they compensated are
// uncovered, so loadState must restart discovery from origin instead of
// resuming mid-keyspace and letting them wait a full wrap.
func TestAbsenceReconcile_StaleCursorRestartsFromOrigin(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-stale-cursor"
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	// A budget-limited pass parks the LFS cursor mid-keyspace.
	r := testReconciler(t, f, true, 4)
	pinLFSLeader(t, f)
	if _, err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.lfs.AfterRepo == "" {
		t.Fatalf("lfs cursor = %+v, want parked mid-keyspace", r.lfs)
	}

	// Fresh cursor rows resume under the production bound.
	errs := &boundedErrs{}
	r.loadState(ctx, errs)
	if err := errs.join(); err != nil {
		t.Fatal(err)
	}
	if r.lfs.AfterRepo == "" {
		t.Fatal("fresh cursor was reset, want resumed")
	}

	// The same rows outliving the (shrunk) bound reset to origin.
	r.cursorStaleAge = time.Millisecond
	time.Sleep(1200 * time.Millisecond) // cross SQLite's second stamp
	r.loadState(ctx, errs)
	if err := errs.join(); err != nil {
		t.Fatal(err)
	}
	if r.lfs.AfterRepo != "" || r.lfs.AfterKey != "" {
		t.Fatalf("stale cursor = %+v, want reset to origin", r.lfs)
	}
}

// TestAbsenceReconcile_QuotaFlipRestartsFromOrigin (Codex round 17,
// security): quota-off passes advance the rotation cursors but skip all
// cover maintenance, so a cursor parked under quota-off must not be
// resumed once enforcement turns on — losses its traversal already passed
// were never covered.
func TestAbsenceReconcile_QuotaFlipRestartsFromOrigin(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-quota-flip"
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	rOff := testReconciler(t, f, false, 4)
	pinLFSLeader(t, f)
	if _, err := rOff.Run(ctx); err != nil {
		t.Fatalf("Run off: %v", err)
	}
	if rOff.lfs.AfterRepo == "" {
		t.Fatalf("lfs cursor = %+v, want parked mid-keyspace", rOff.lfs)
	}

	// Enforcement turns on: the off-parked cursor resets to origin.
	rOn := testReconciler(t, f, true, 4)
	errs := &boundedErrs{}
	rOn.loadState(ctx, errs)
	if err := errs.join(); err != nil {
		t.Fatal(err)
	}
	if rOn.lfs.AfterRepo != "" || rOn.lfs.AfterKey != "" {
		t.Fatalf("off-parked cursor = %+v, want reset to origin", rOn.lfs)
	}

	// An on-parked cursor resumes under enforcement (control).
	if _, err := rOn.Run(ctx); err != nil {
		t.Fatalf("Run on: %v", err)
	}
	if rOn.lfs.AfterRepo == "" {
		t.Fatalf("on cursor = %+v, want parked mid-keyspace", rOn.lfs)
	}
	rOn2 := testReconciler(t, f, true, 4)
	rOn2.loadState(ctx, errs)
	if err := errs.join(); err != nil {
		t.Fatal(err)
	}
	if rOn2.lfs.AfterRepo == "" {
		t.Fatal("on-parked cursor was reset, want resumed")
	}
}

// bumpFailRegistry injects failures into BumpLFSAdoptionGen while fail is
// set — the floor-invalidation seam every compensation-shrinking delete
// must prove first (Codex round 12).
type bumpFailRegistry struct {
	AbsenceRegistry
	fail bool
}

func (br *bumpFailRegistry) BumpLFSAdoptionGen(ctx context.Context, repo string) error {
	if br.fail {
		return fmt.Errorf("flaky: injected bump failure")
	}
	return br.AbsenceRegistry.BumpLFSAdoptionGen(ctx, repo)
}

// TestAbsenceReconcile_ShrinkRetainsCoverOnBumpFailure (Codex round 12,
// P2): rotating a cover DOWN deletes the larger row — compensation
// shrinks by the healed delta, so the delete must not proceed unless the
// floor-generation invalidation is proven. On bump failure both rows
// stay; a later pass's superseded cleanup completes the rotation under
// the same proven-bump rule.
func TestAbsenceReconcile_ShrinkRetainsCoverOnBumpFailure(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-bump-fail"
	key := blob.LFSKey(base, absenceOID('h'))
	registerLFS(t, f, base, key, 300)

	// Pass 1: the full 300-byte loss is covered.
	reg := &bumpFailRegistry{AbsenceRegistry: absenceRegistry(t, f)}
	r := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	if _, err := r.Run(ctx); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 300 {
		t.Fatalf("reserved = %d, want the full loss covered", got)
	}

	// Bytes partially reappear OUTSIDE the application restore path
	// (backend-side recovery): the row is now a 200-byte prefix of its
	// 300 registered bytes — deficit 100.
	f.upload(key, strings.Repeat("x", 200))

	// Pass 2 with the invalidation seam broken: the smaller replacement
	// may mint, but the LARGER row must survive — deleting it without a
	// proven bump would leave a warm deflated floor under-compensated.
	reg.fail = true
	if _, err := r.Run(ctx); err == nil {
		t.Fatal("Run 2: want the injected bump failure surfaced")
	}
	if got := ledgerReserved(t, f, base); got < 300 {
		t.Fatalf("reserved = %d after a failed invalidation, want the larger cover retained (>= 300)", got)
	}

	// Pass 3 with the seam healed (fresh reconciler — another replica):
	// the superseded cleanup completes the rotation bump-first and the
	// ledger converges to the exact 100-byte deficit.
	reg.fail = false
	r2 := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	report, err := r2.Run(ctx)
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversReleased == 0 {
		t.Fatalf("pass 3 report = %+v, want the superseded larger row released once the bump lands", report)
	}
	if got := ledgerReserved(t, f, base); got != 100 {
		t.Fatalf("reserved = %d after convergence, want the exact remaining deficit", got)
	}
}

// touchRecorder proves the bulk touch runs strictly before the renewal
// listing and keeps covers protected even when the listing fails outright
// (Codex round 9).
type touchRecorder struct {
	AbsenceRegistry
	calls []string
}

func (rec *touchRecorder) RenewAbsenceCovers(ctx context.Context, ttl time.Duration) (int64, error) {
	rec.calls = append(rec.calls, "touch")
	return rec.AbsenceRegistry.RenewAbsenceCovers(ctx, ttl)
}

func (rec *touchRecorder) ListAbsenceCovers(ctx context.Context, afterRaw string, limit int, ttl time.Duration) ([]ref.AbsenceCoverRow, error) {
	rec.calls = append(rec.calls, "list")
	return nil, fmt.Errorf("flaky: injected list failure")
}

// TestAbsenceReconcile_BulkTouchPrecedesListing (Codex round 9, P1): cover
// TTL protection must not depend on the keyset renewal listing — the
// DB-clock bulk touch re-stamps every live deterministic cover FIRST, so
// even a pass whose listing fails outright (or round-robins across many
// passes) cannot let a live cover lapse.
func TestAbsenceReconcile_BulkTouchPrecedesListing(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-touch"
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), absenceIncarnationKey(base, absenceClassLFS, 1), 100, time.Hour); err != nil {
		t.Fatalf("seed cover: %v", err)
	}
	rec := &touchRecorder{AbsenceRegistry: absenceRegistry(t, f)}
	r := NewAbsenceReconciler(rec, f.blobs, true, 1000, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	report, err := r.Run(ctx)
	if err == nil {
		t.Fatal("Run: want the injected listing failure reported")
	}
	if report.CoversTouched != 1 || !report.RenewalIncomplete {
		t.Fatalf("report = %+v, want the live cover touched and the listing marked incomplete", report)
	}
	if len(rec.calls) < 2 || rec.calls[0] != "touch" || rec.calls[1] != "list" {
		t.Fatalf("calls = %v, want the bulk touch strictly before the listing", rec.calls)
	}
}

// failTouchRegistry fails the pass-start bulk cover renewal once.
type failTouchRegistry struct {
	AbsenceRegistry
	fail bool
}

func (ft *failTouchRegistry) RenewAbsenceCovers(ctx context.Context, ttl time.Duration) (int64, error) {
	if ft.fail {
		ft.fail = false
		return 0, fmt.Errorf("flaky: injected bulk-renew failure")
	}
	return ft.AbsenceRegistry.RenewAbsenceCovers(ctx, ttl)
}

// TestAbsenceReconcile_TouchFailureSkipsCursorStamp (Codex round 19, P1):
// a pass whose bulk cover renewal FAILED must not re-stamp the advanced
// rotation cursors — the cursor row's fresh DB-clock age is loadState's
// proof that the covers behind it were renewed within the pass timeout,
// and re-stamping it without renewing would keep a fleet whose renewals
// keep failing looking perpetually fresh while every cover ages toward
// expiry. The rotation cursors stay parked at their previous rows; the
// segment is merely re-examined next pass.
func TestAbsenceReconcile_TouchFailureSkipsCursorStamp(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-touch-skip"
	for i := range byte(6) {
		key := blob.LFSKey(base, absenceOID('a'+i))
		registerLFS(t, f, base, key, 10)
		f.upload(key, strings.Repeat("x", 10))
	}
	// Pass 1 parks the lfs cursor mid-keyspace.
	pinLFSLeader(t, f)
	r1 := testReconciler(t, f, true, 4)
	if _, err := r1.Run(ctx); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	store := absenceRegistry(t, f)
	parked, ok, err := store.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceLFS)
	if err != nil || !ok || parked.Payload == "" {
		t.Fatalf("parked cursor = (%+v, %v, %v)", parked, ok, err)
	}

	// Pass 2's bulk renewal fails: the rotation advances in memory but
	// the durable cursor must stay at the pass-1 park.
	reg := &failTouchRegistry{AbsenceRegistry: store, fail: true}
	r2 := NewAbsenceReconciler(reg, f.blobs, true, 4, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	report, err := r2.Run(ctx)
	if err == nil || !report.RenewalIncomplete {
		t.Fatalf("Run 2 = (%+v, %v), want the injected renewal failure reported", report, err)
	}
	after, ok, err := store.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceLFS)
	if err != nil || !ok || after.Payload != parked.Payload {
		t.Fatalf("cursor after failed touch = (%+v, %v, %v), want the pass-1 park unchanged", after, ok, err)
	}

	// Control: a healthy pass re-stamps and advances the cursor.
	r3 := testReconciler(t, f, true, 4)
	if _, err := r3.Run(ctx); err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	after, ok, err = store.GetJobScanState(ctx, ref.ScanJobAbsence, ref.ScanScopeAbsenceLFS)
	if err != nil || !ok || after.Payload == parked.Payload {
		t.Fatalf("cursor after healthy pass = (%+v, %v, %v), want advanced", after, ok, err)
	}
}

// TestAbsenceReconcile_ChargeFailureRetriedAtRangeEnd (Codex round 8, P1):
// a transient charge failure on a range's last deficits leaves the running
// total accumulated but UNCHARGED; the bounded range-end retry — forced
// past the coverSkip memo — must cover it within the same pass instead of
// waiting a whole class traversal.
func TestAbsenceReconcile_ChargeFailureRetriedAtRangeEnd(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-flaky-charge"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('f')), 100)
	reg := &flakyRegistry{AbsenceRegistry: absenceRegistry(t, f), failLeft: 1}
	r := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	report, err := r.Run(ctx)
	if err == nil {
		t.Fatal("Run: want the injected charge failure reported")
	}
	if report.CoverErrors == 0 {
		t.Fatalf("report = %+v, want the injected failure counted", report)
	}
	if got := ledgerReserved(t, f, base); got != 100 {
		t.Fatalf("reserved = %d, want the deficit covered by the range-end retry", got)
	}
}

// TestAbsenceReconcile_DirtyRangeDefersLegacyRetirement (Codex rounds
// 8+19): a class whose range observation is DIRTY — here the deficit
// charge fails at settle AND at the bounded range-end retry — yields no
// ground-truth proof, so the base's legacy nonce must NOT retire this
// pass: the packs-class gap proof alone is not enough, and the nonce
// stays live under the bulk touch until a clean observation lands.
func TestAbsenceReconcile_DirtyRangeDefersLegacyRetirement(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-flaky-legacy"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('g')), 100)
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	// Pass 1: both injected failures land on the lfs deficit charge
	// (settle, then the range-end retry) — the range stays dirty and
	// unproven.
	reg := &flakyRegistry{AbsenceRegistry: absenceRegistry(t, f), failLeft: 2}
	r := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, time.Minute, slog.New(slog.DiscardHandler))
	report, err := r.Run(ctx)
	if err == nil {
		t.Fatal("Run 1: want the injected failures reported")
	}
	if report.CoversDecaying != 0 {
		t.Fatalf("pass 1 report = %+v, want retirement deferred on the dirty range", report)
	}
	if got := ledgerReserved(t, f, base); got != 555 {
		t.Fatalf("reserved after pass 1 = %d, want only the live nonce", got)
	}

	// Pass 2 (fresh reconciler, same durable state — another replica):
	// the clean observation charges the exact deficit into the lfs head,
	// the packs gap proof completes the pair, and the nonce retires.
	r2 := testReconciler(t, f, true, 1000)
	report, err = r2.Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("pass 2 report = %+v, want the nonce retired after the clean proof", report)
	}
	if got := ledgerReserved(t, f, base); got != 555+100 {
		t.Fatalf("reserved after pass 2 = %d, want the decaying nonce plus the exact lfs cover", got)
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

// TestAbsenceIncarnationKeyRoundTrip: the deterministic cover-key spelling
// parses back exactly, and every other segment under the prefix classifies
// as legacy.
func TestAbsenceIncarnationKeyRoundTrip(t *testing.T) {
	key := absenceIncarnationKey("base/repo", absenceClassPacks, 42)
	base, class, n, legacy, ok := parseAbsenceCoverKey(key)
	if !ok || legacy || base != "base/repo" || class != absenceClassPacks || n != 42 {
		t.Fatalf("parse(%q) = (%q, %q, %d, legacy=%v, ok=%v)", key, base, class, n, legacy, ok)
	}
	// Zero-padding makes byte order numeric order across magnitudes.
	if k9, k10 := absenceIncarnationKey("b", absenceClassLFS, 9), absenceIncarnationKey("b", absenceClassLFS, 10); !(k9 < k10) {
		t.Fatalf("incarnation keys out of order: %q >= %q", k9, k10)
	}
	for _, legacyKey := range []string{
		ref.AbsenceCoverPrefix + "b/" + ref.NewReservationOwner(), // pre-#125 nonce
		ref.AbsenceCoverPrefix + "b/lfs-e1",                       // unpadded: not ours
		ref.AbsenceCoverPrefix + "b/packs-exyz",                   // malformed number
	} {
		if _, _, _, legacy, ok := parseAbsenceCoverKey(legacyKey); !ok || !legacy {
			t.Fatalf("parse(%q): want legacy classification, got legacy=%v ok=%v", legacyKey, legacy, ok)
		}
	}
	if _, _, _, _, ok := parseAbsenceCoverKey("packs/other/prefix"); ok {
		t.Fatalf("foreign prefix parsed as a cover key")
	}
}

// TestAbsenceReconcile_MultiPassRangeAccumulates (Codex round 2, P1): a
// base whose missing rows span multiple phase allocations must accumulate
// its deficit ACROSS passes — the pending range parks with the durable
// cursor, so resumed chunks continue the running total instead of
// restarting at zero (which could never grow the cover past one chunk's
// subtotal). Once everything heals, a full clean traversal releases the
// cover exactly.
func TestAbsenceReconcile_MultiPassRangeAccumulates(t *testing.T) {
	f := newSweepFixture(t)
	base := "repo-multipass"
	keys := make([]string, 4)
	for i, b := range []byte{'a', 'b', 'c', 'd'} {
		keys[i] = blob.LFSKey(base, absenceOID(b))
		registerLFS(t, f, base, keys[i], 10) // all absent
	}

	// Budget 8 → LFS allocation 4 → two 2-unit absent rows per pass.
	workerA := testReconciler(t, f, true, 8)
	if _, err := workerA.Run(context.Background()); err != nil {
		t.Fatalf("worker A: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 20 {
		t.Fatalf("reserved after pass 1 = %d, want the first chunk's 20", got)
	}

	// A FRESH worker resumes mid-range: the parked running total (20)
	// continues through the remaining rows, growing the cover to the
	// true 40-byte sum. Pre-fix, the restarted total (10, 20) never
	// exceeded the live 20 and the cover stayed under-reserved forever.
	pinLFSLeader(t, f)
	workerB := testReconciler(t, f, true, 8)
	if _, err := workerB.Run(context.Background()); err != nil {
		t.Fatalf("worker B: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 40 {
		t.Fatalf("reserved after pass 2 = %d, want the accumulated 40", got)
	}

	// Everything heals: a fresh worker's full clean traversal re-derives
	// a zero total and releases the cover outright.
	for _, key := range keys {
		if got := f.upload(key, strings.Repeat("h", 10)); got != 10 {
			t.Fatalf("heal %s: %d bytes", key, got)
		}
	}
	pinLFSLeader(t, f)
	workerC := testReconciler(t, f, true, 100)
	report, err := workerC.Run(context.Background())
	if err != nil {
		t.Fatalf("worker C: %v", err)
	}
	if report.CoversReleased != 1 {
		t.Fatalf("worker C report = %+v, want the healed cover released", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved after heal = %d, want 0", got)
	}
}

// TestAbsenceReconcile_EmptiedBaseReleasesCover: a base whose registry
// rows ALL disappear (retirement, folds — without a purge fence) never
// streams through the rotation again, so no range evaluation would visit
// it; the keyset gap proof releases its cover instead of renewing it
// forever.
func TestAbsenceReconcile_EmptiedBaseReleasesCover(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-emptied"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('a')), 10) // absent

	r := testReconciler(t, f, true, 1000)
	if _, err := r.Run(ctx); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 10 {
		t.Fatalf("reserved = %d, want the 10-byte cover", got)
	}

	// Every registry row of the base vanishes (simulating retirement or
	// a fold away — no purge fence).
	dropper, ok := f.store.(interface {
		DropRepoLFSObjects(ctx context.Context, group []string) (int64, error)
	})
	if !ok {
		t.Fatalf("store %T lacks DropRepoLFSObjects", f.store)
	}
	if n, err := dropper.DropRepoLFSObjects(ctx, blob.QuotaRepoGroup(base)); err != nil || n != 1 {
		t.Fatalf("drop rows = (%d, %v), want 1", n, err)
	}

	// A fresh worker's rotation proves the base row-less and releases
	// the cover outright.
	report, err := testReconciler(t, f, true, 1000).Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversReleased != 1 {
		t.Fatalf("pass 2 report = %+v, want the row-less base's cover released", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
}
