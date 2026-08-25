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
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.CoversCharged != 0 {
		t.Fatalf("pass 3 report = %+v, want no covers once nothing is tracked", report)
	}
}

// TestAbsenceReconcile_RenewalRoundRobin (issue #125): a live cover
// population larger than the renewal half-budget renews round-robin across
// passes on the DURABLE cursor, loudly flagged; a raised budget renews all.
func TestAbsenceReconcile_RenewalRoundRobin(t *testing.T) {
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

	// Pass 2 at budget 100: the renewal half (50) cannot renew 60 rows —
	// round-robin, loud.
	r.budget = 100
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if !report.RenewalIncomplete || report.CoversRenewed != 50 {
		t.Fatalf("pass 2 report = %+v, want a loud 50-row renewal backlog", report)
	}

	// Pass 3 with the budget raised: the durable cursor resumes at the
	// un-renewed tail first, then wraps — every cover renews.
	r.budget = 400
	report, err = r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	if report.RenewalIncomplete || report.CoversRenewed != 60 {
		t.Fatalf("pass 3 report = %+v, want all 60 covers renewed", report)
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

// TestAbsenceReconcile_LegacyCoverRetired: nonce-keyed covers minted by
// pre-#125 workers renew until both classes complete one clean traversal,
// then are released — their losses live in incarnation keys by then.
func TestAbsenceReconcile_LegacyCoverRetired(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	base := "repo-legacy-cover"
	// Seed a legacy-format aggregate cover the way a pre-#125 worker
	// minted it: absence-lost/<base>/<random-owner-nonce>.
	legacyKey := ref.AbsenceCoverPrefix + base + "/" + ref.NewReservationOwner()
	if err := ref.ReserveRestoreCover(ctx, absenceRegistry(t, f), blob.QuotaRepoGroup(base), legacyKey, 555, time.Hour); err != nil {
		t.Fatalf("seed legacy cover: %v", err)
	}
	if got := ledgerReserved(t, f, base); got != 555 {
		t.Fatalf("reserved = %d, want the seeded 555", got)
	}

	// Pass 1: no clean traversals recorded yet — the legacy cover is
	// renewed conservatively while its rows' rediscovery is unproven.
	r := testReconciler(t, f, true, 1000)
	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if report.CoversRenewed != 1 || report.CoversReleased != 0 {
		t.Fatalf("pass 1 report = %+v, want the legacy cover renewed", report)
	}

	// Pass 1 completed clean traversals of both (empty) registries, so
	// pass 2 retires the legacy spelling outright.
	report, err = r.Run(ctx)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if report.CoversReleased != 1 || report.CoversRenewed != 0 {
		t.Fatalf("pass 2 report = %+v, want the legacy cover released", report)
	}
	if got := ledgerReserved(t, f, base); got != 0 {
		t.Fatalf("reserved = %d, want 0 after the legacy release", got)
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
