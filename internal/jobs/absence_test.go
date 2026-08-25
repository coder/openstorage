// Absence reconciler tests (issues #101 + #125): SQLite + fileblob, reusing
// the sweep fixture (same package) for the store/blob pair. All cross-pass
// state is fleet-durable, so several tests hand the same fixture to a FRESH
// reconciler to simulate River scheduling a pass on another replica.
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
	return NewAbsenceReconciler(absenceRegistry(t, f), f.blobs, quotaEnforced, budget, time.Hour, slog.New(slog.DiscardHandler))
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
	for i := 0; i < 60; i++ {
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
	report, err := workerA.Run(context.Background())
	if err != nil {
		t.Fatalf("worker A: %v", err)
	}
	if report.LFSRowsAbsent != 2 || report.BytesCovered != 10+20 {
		t.Fatalf("worker A report = %+v, want both losses covered (grow-charged 10 then 20)", report)
	}
	if got := ledgerReserved(t, f, base); got != 20 {
		t.Fatalf("reserved = %d, want 20", got)
	}

	// Pass 2 (fresh worker): resumes on the present suffix and reaches
	// the range end with a zero running total — but the range began
	// before this pass, so the cover MUST survive.
	pinLFSLeader(t, f)
	workerB := testReconciler(t, f, true, 100)
	report, err = workerB.Run(context.Background())
	if err != nil {
		t.Fatalf("worker B: %v", err)
	}
	if report.CoversReleased != 0 || report.CoversRotated != 0 {
		t.Fatalf("worker B report = %+v, want no shrink from a resumed partial range", report)
	}
	if got := ledgerReserved(t, f, base); got != 20 {
		t.Fatalf("reserved = %d, want the prefix losses still covered", got)
	}
}

// TestAbsencePassTimeoutClampedToTTL (Codex round 3, security): the pass
// wall-clock cap tightens to a quarter of the cover TTL so a long scan can
// never hold the next renewal past a live cover's expiry.
func TestAbsencePassTimeoutClampedToTTL(t *testing.T) {
	f := newSweepFixture(t)
	log := slog.New(slog.DiscardHandler)
	r := NewAbsenceReconciler(absenceRegistry(t, f), f.blobs, true, 100, 40*time.Minute, log)
	if r.passTimeout != 10*time.Minute {
		t.Fatalf("passTimeout = %v with a 40m TTL, want 10m (TTL/4)", r.passTimeout)
	}
	r = NewAbsenceReconciler(absenceRegistry(t, f), f.blobs, true, 100, 8*time.Hour, log)
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

// TestAbsenceReconcile_LegacyCoverMigrated (Codex round 4+5, P1s):
// nonce-keyed covers minted by pre-#125 workers — at ANY point of a
// mixed-version roll — are folded into the base's deterministic
// incarnations on first sight, which every new-replica pass keeps renewed;
// the nonce row itself is never deleted (an old replica may depend on it)
// and expires by TTL. A pre-#125 cover aggregates BOTH classes' deficits,
// so the total folds into BOTH class incarnations and each class's own
// clean range right-sizes its copy — the pack portion must survive the
// nonce's decay even though the fold happens during the LFS-keyed renewal.
func TestAbsenceReconcile_LegacyCoverMigrated(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-legacy-cover"
	// Registered-but-missing rows in BOTH classes keep the base's ranges
	// alive so neither migrated incarnation is released as row-less; the
	// legacy cover carries a LARGER total (the old replica had seen more
	// losses) than either class's exact deficit.
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('a')), 100)
	if err := f.store.RegisterPacks(ctx, base, []ref.Pack{{ID: "lost", Key: blob.PackKey(base, "lost"), Size: 200}}); err != nil {
		t.Fatal(err)
	}
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	// Pass 1: the renewal listing migrates the 555-byte legacy total into
	// BOTH deterministic incarnations (grow-only GREATEST merges) and
	// stops renewing the nonce row; the rotations' own discovery keeps
	// charging the same incarnations.
	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("pass 1 report = %+v, want the legacy cover migrated and decaying", report)
	}
	// The same pass's clean full-range observations right-size each
	// migrated 555 down to its class's registry-exact deficit (rotate +
	// release the superseded rows). Ledger: nonce row (555, decaying
	// inside its TTL) + right-sized lfs (100) and packs (200)
	// incarnations.
	if report.CoversRotated != 2 {
		t.Fatalf("pass 1 report = %+v, want both migrated totals right-sized by the rotations", report)
	}
	if got := ledgerReserved(t, f, base); got != 555+100+200 {
		t.Fatalf("reserved = %d, want the decaying nonce row plus both right-sized incarnations", got)
	}

	// Pass 2: steady state — the exact incarnations renew; the nonce row
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

	// An old replica re-grows the SAME nonce mid-roll (Codex round 7,
	// P1): the fold memo is a byte watermark, not a boolean — the grown
	// total must re-fold into both incarnations before the nonce decays.
	// Pass 3 re-inflates both incarnations to 900 and its own clean
	// ranges right-size them again (a rotate per class proves the
	// re-fold happened); the nonce decays at its grown total.
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 900, time.Hour); err != nil {
		t.Fatalf("re-grow legacy cover: %v", err)
	}
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversDecaying != 1 || report.CoversRotated != 2 {
		t.Fatalf("pass 3 report = %+v, want the grown nonce re-folded and both incarnations right-sized", report)
	}
	if got := ledgerReserved(t, f, base); got != 900+100+200 {
		t.Fatalf("reserved after pass 3 = %d, want the grown nonce plus right-sized incarnations", got)
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
	for i := 0; i < 8; i++ {
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

// TestAbsenceReconcile_LegacyRefoldAfterHeadsReleased (Codex round 13,
// P1): the legacy-fold memo shortcut is process-local, but the
// incarnations it vouches for can be RELEASED by any worker after a heal.
// When an old replica then re-charges the SAME nonce at an EQUAL total
// for a NEW incident, the shortcut must notice the missing heads
// (DB-authoritative) and re-fold — otherwise the nonce decays with no
// compensation left.
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

	// Pass 1: fold + right-size both incarnations (555 -> 100 lfs,
	// 555 -> 200 packs).
	r := testReconciler(t, f, true, 1000)
	if _, err := r.Run(ctx); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	// Every byte heals; pass 2 releases both incarnations (the nonce
	// keeps decaying under the memo — its heads were live at renewal).
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

	// Pass 3 (same process, same memo): the equal-total shortcut must
	// fail its heads-live check and re-fold — both incarnations are
	// re-minted and right-sized by the same pass's clean ranges.
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversDecaying != 1 || report.CoversRotated != 2 {
		t.Fatalf("pass 3 report = %+v, want the nonce re-folded and both incarnations right-sized", report)
	}
	if got := ledgerReserved(t, f, base); got != 555+100+200 {
		t.Fatalf("reserved = %d, want the decaying nonce plus both re-minted right-sized incarnations", got)
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
	r := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, slog.New(slog.DiscardHandler))
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
	r2 := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, slog.New(slog.DiscardHandler))
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
	r := NewAbsenceReconciler(rec, f.blobs, true, 1000, time.Hour, slog.New(slog.DiscardHandler))
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
	r := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, slog.New(slog.DiscardHandler))
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

// TestAbsenceReconcile_FailedLegacyMigrationReoffered (Codex round 8, P1):
// when one legacy cover can NEITHER fold nor renew, the renewal cursor
// parks strictly before it — the next pass re-offers the nonce first
// instead of letting it decay unprotected until the listing wraps.
func TestAbsenceReconcile_FailedLegacyMigrationReoffered(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-flaky-legacy"
	registerLFS(t, f, base, blob.LFSKey(base, absenceOID('g')), 100)
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}

	// Pass 1: the lfs fold's mint AND the fallback nonce renewal both
	// fail (injected). The renewal phase must abort parked BEFORE the
	// nonce rather than skipping past it.
	reg := &flakyRegistry{AbsenceRegistry: absenceRegistry(t, f), failLeft: 2}
	r := NewAbsenceReconciler(reg, f.blobs, true, 1000, time.Hour, slog.New(slog.DiscardHandler))
	report, err := r.Run(ctx)
	if err == nil {
		t.Fatal("Run 1: want the injected failures reported")
	}
	if !report.RenewalIncomplete || report.CoversDecaying != 0 || report.CoversRenewed != 0 {
		t.Fatalf("pass 1 report = %+v, want renewal parked before the failed nonce", report)
	}

	// Pass 2 (fresh reconciler, same durable state — another replica):
	// the parked cursor re-offers the nonce first; the migration folds
	// it into both incarnations and the rotations right-size them.
	r2 := testReconciler(t, f, true, 1000)
	report, err = r2.Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversDecaying != 1 {
		t.Fatalf("pass 2 report = %+v, want the re-offered nonce migrated and decaying", report)
	}
	if got := ledgerReserved(t, f, base); got != 555+100 {
		t.Fatalf("reserved after pass 2 = %d, want the decaying nonce plus the right-sized lfs cover", got)
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
