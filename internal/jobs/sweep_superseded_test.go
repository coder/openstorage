// Superseded-class sweeping and reservation pruning (issues #29/#31): the
// sweeper reaps retained (provably-folded) blobs even while general orphan
// deletion is off, reclaims their retained bytes from the quota ledger, and
// prunes crash-leaked admission reservations on its cadence.
package jobs

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// foldWorld seeds one repo with two registered packs folded into a
// consolidated one, leaving the two superseded blobs lingering under the
// pack prefix (retained rows recorded by ReplacePacks). Returns the two
// superseded keys and their total size.
func foldWorld(f *sweepFixture, repo string) (aKey, bKey string, retained int64) {
	f.t.Helper()
	ctx := context.Background()
	aKey, bKey = blob.PackKey(repo, "aa1"), blob.PackKey(repo, "bb2")
	foldKey := blob.PackKey(repo, "f01")
	aSize := f.upload(aKey, "superseded-a")
	bSize := f.upload(bKey, "superseded-bb")
	f.upload(foldKey, "consolidated-union")
	a := ref.Pack{ID: "aa1", Key: aKey, Size: aSize}
	b := ref.Pack{ID: "bb2", Key: bKey, Size: bSize}
	if err := f.store.RegisterPacks(ctx, repo, []ref.Pack{a, b}); err != nil {
		f.t.Fatalf("RegisterPacks: %v", err)
	}
	won, err := f.store.ReplacePacks(ctx, repo, []ref.Pack{a, b},
		ref.Pack{ID: "f01", Key: foldKey, Size: int64(len("consolidated-union"))})
	if err != nil || !won {
		f.t.Fatalf("ReplacePacks = %v, %v", won, err)
	}
	return aKey, bKey, aSize + bSize
}

// TestSweep_SupersededModeReapsFoldedBlobsOnly: with general deletion OFF
// and superseded mode ON (the shipped default), aged retained blobs are
// reaped through the quarantine handshake and their bytes reclaimed from
// the ledger, while equally aged general orphans are only reported.
func TestSweep_SupersededModeReapsFoldedBlobsOnly(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	const repo = "r1"
	aKey, bKey, retained := foldWorld(f, repo)
	orphanKey := blob.PackKey(repo, "0rphan")
	f.upload(orphanKey, "plain-orphan")
	for _, k := range []string{aKey, bKey, orphanKey} {
		f.backdate(k, 25*time.Hour)
	}

	cfg := SweepConfig{Threshold: 24 * time.Hour, DeleteSuperseded: true}
	report, err := f.sweeper(cfg).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 2 || report.SupersededDeleted != 2 || report.RetainedBytesReclaimed != retained {
		t.Fatalf("report = %+v, want deleted=2 superseded=2 retained_reclaimed=%d", report, retained)
	}
	for _, k := range []string{aKey, bKey} {
		if f.exists(k) {
			t.Fatalf("superseded blob %q survived", k)
		}
		if !f.hasBackup(k) {
			t.Fatalf("superseded blob %q reaped without a quarantine backup", k)
		}
	}
	if !f.exists(orphanKey) {
		t.Fatal("superseded-only mode deleted a general orphan")
	}
	if report.OrphanCount != 3 { // both retained + the general orphan are reported
		t.Fatalf("OrphanCount = %d, want 3", report.OrphanCount)
	}

	// The ledger returned the retained headroom: usage drops to the fold's
	// size, retained set empties. (Quota state read via the Store's
	// inspection hook.)
	st, err := f.store.QuotaLedgerState(ctx, []string{repo}, time.Hour)
	if err != nil {
		t.Fatalf("QuotaLedgerState: %v", err)
	}
	if st.RetainedBytes != 0 || st.UsageBytes != int64(len("consolidated-union")) {
		t.Fatalf("ledger after superseded sweep = %+v, want retained=0 usage=%d", st, len("consolidated-union"))
	}

	// Aged backups purge under superseded mode too (Delete stays off).
	for _, k := range []string{aKey, bKey} {
		for _, q := range f.backupKeys(k) {
			f.backdate(q, 25*time.Hour)
		}
	}
	purged, err := f.sweeper(cfg).Sweep(ctx)
	if err != nil {
		t.Fatalf("purge Sweep: %v", err)
	}
	if purged.BytesReclaimed != retained {
		t.Fatalf("purge report = %+v, want bytes_reclaimed=%d", purged, retained)
	}
}

// TestSweep_SupersededDisabledLeavesFoldedBlobs: turning the flag off
// restores pure list-only behavior for the retained class.
func TestSweep_SupersededDisabledLeavesFoldedBlobs(t *testing.T) {
	f := newSweepFixture(t)
	aKey, bKey, _ := foldWorld(f, "r1")
	for _, k := range []string{aKey, bKey} {
		f.backdate(k, 25*time.Hour)
	}
	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 0 || report.SupersededDeleted != 0 {
		t.Fatalf("report = %+v, want nothing deleted", report)
	}
	for _, k := range []string{aKey, bKey} {
		if !f.exists(k) {
			t.Fatalf("list-only sweep deleted %q", k)
		}
	}
}

// TestSweep_SupersededFreshBlobNotReaped: the age threshold guards retained
// blobs exactly like general orphans — a just-folded pack may still be
// mid-download by a hydrator.
func TestSweep_SupersededFreshBlobNotReaped(t *testing.T) {
	f := newSweepFixture(t)
	aKey, bKey, _ := foldWorld(f, "r1")
	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, DeleteSuperseded: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want fresh retained blobs kept", report)
	}
	for _, k := range []string{aKey, bKey} {
		if !f.exists(k) {
			t.Fatalf("fresh retained blob %q reaped", k)
		}
	}
}

// TestSweep_HealFinishesInterruptedReclaim: a reap that crashed between the
// blob delete and the ledger reclaim leaves a retained row for a gone blob;
// the next sweep's heal pass finishes the reclaim.
func TestSweep_HealFinishesInterruptedReclaim(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	const repo = "r1"
	aKey, bKey, _ := foldWorld(f, repo)

	// Simulate the crash window: quarantine-copy + delete happened (we
	// reproduce it manually), ReclaimRetainedPack did not run.
	qKey := quarantineKey(aKey)
	if err := f.blobs.Copy(ctx, qKey, aKey); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := f.blobs.Delete(ctx, aKey); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, DeleteSuperseded: true}).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.RetainedBytesReclaimed != int64(len("superseded-a")) {
		t.Fatalf("report = %+v, want heal to reclaim %d retained bytes", report, len("superseded-a"))
	}
	retained, err := f.store.IsRetainedPack(ctx, repo, aKey)
	if err != nil || retained {
		t.Fatalf("IsRetainedPack after heal = %v, %v; want false", retained, err)
	}
	// The untouched sibling stays retained (its blob is fresh and present).
	retained, err = f.store.IsRetainedPack(ctx, repo, bKey)
	if err != nil || !retained {
		t.Fatalf("sibling IsRetainedPack = %v, %v; want true", retained, err)
	}
}

// TestSweep_PrunesExpiredReservations: the sweep deletes reservation rows
// older than the configured TTL — the reserve → crash → sweep recovery.
func TestSweep_PrunesExpiredReservations(t *testing.T) {
	f := newSweepFixture(t)
	ctx := context.Background()
	const repo = "r1"
	dec, err := f.store.ReservePackBytes(ctx, []string{repo},
		[]ref.Pack{{ID: "aa1", Key: blob.PackKey(repo, "aa1"), Size: 10}},
		"crashed-push", 0, 1000, time.Hour)
	if err != nil || !dec.OK {
		t.Fatalf("ReservePackBytes = %+v, %v", dec, err)
	}

	// TTL not yet reached: the row survives.
	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, ReservationTTL: time.Hour}).Sweep(ctx)
	if err != nil || report.ReservationsExpired != 0 {
		t.Fatalf("early sweep = %+v, %v; want no reservations pruned", report, err)
	}

	// Simulate expiry with a zero-age TTL sweep configuration.
	report, err = f.sweeper(SweepConfig{Threshold: 24 * time.Hour, ReservationTTL: time.Nanosecond}).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.ReservationsExpired != 1 {
		t.Fatalf("report = %+v, want 1 reservation pruned", report)
	}
	st, err := f.store.QuotaLedgerState(ctx, []string{repo}, time.Hour)
	if err != nil || st.ReservedBytes != 0 {
		t.Fatalf("ledger after prune = %+v, %v; want no reserved bytes", st, err)
	}
}

// TestSweep_SupersededGuardFailsClosed: the DeleteGuard downgrades
// superseded mode too — a versioned bucket must stop ALL destructive work.
func TestSweep_SupersededGuardFailsClosed(t *testing.T) {
	f := newSweepFixture(t)
	aKey, bKey, _ := foldWorld(f, "r1")
	for _, k := range []string{aKey, bKey} {
		f.backdate(k, 25*time.Hour)
	}
	guardErr := context.DeadlineExceeded
	sweeper := NewSweeper(f.blobs, f.store, SweepConfig{
		Threshold:        24 * time.Hour,
		DeleteSuperseded: true,
		DeleteGuard:      func(context.Context) error { return guardErr },
	}, slog.New(slog.DiscardHandler))
	report, err := sweeper.Sweep(context.Background())
	if err == nil {
		t.Fatal("guard failure not reported")
	}
	if report.Deleted != 0 || report.SupersededDeleted != 0 {
		t.Fatalf("report = %+v, want guarded run to delete nothing", report)
	}
	for _, k := range []string{aKey, bKey} {
		if !f.exists(k) {
			t.Fatalf("guarded sweep deleted %q", k)
		}
	}
}
