// Retained-byte accounting through the REAL Repacker (issue #31): a fold
// records its inputs' blob sizes as retained, the retained bytes gate
// admission (bounding the drip-write amplification), and a
// superseded-class sweep returns exactly that headroom.
package jobs

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/ref"
)

// TestRepack_RetainedBytesAccountedAndSwept: fold → retained recorded with
// real sizes and usage grows by the consolidated pack only → admission
// charges the retained bytes → superseded-class sweep reclaims them.
func TestRepack_RetainedBytesAccountedAndSwept(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()

	var oldIDs []string
	var pushedBytes int64
	for range 3 {
		id := f.push(t)
		size, err := f.blobs.Stat(ctx, blob.PackKey(f.repo, id))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		oldIDs = append(oldIDs, id)
		pushedBytes += size
	}

	if folded, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil || !folded {
		t.Fatalf("Repack = %v, %v", folded, err)
	}
	rows := f.packRows(t)
	if len(rows) != 1 {
		t.Fatalf("pack rows = %+v, want 1", rows)
	}
	foldKey := rows[0].Key
	foldSize, err := f.blobs.Stat(ctx, foldKey)
	if err != nil {
		t.Fatalf("stat fold: %v", err)
	}

	// Ledger truth after the fold: usage counts pushed + consolidated
	// bytes (everything under the prefix), retained records the inputs.
	st, err := f.store.QuotaLedgerState(ctx, []string{f.repo}, time.Hour)
	if err != nil {
		t.Fatalf("QuotaLedgerState: %v", err)
	}
	if st.UsageBytes != pushedBytes+foldSize || st.RetainedBytes != pushedBytes {
		t.Fatalf("post-fold ledger = %+v, want usage=%d retained=%d", st, pushedBytes+foldSize, pushedBytes)
	}
	for _, id := range oldIDs {
		retained, err := f.store.IsRetainedPack(ctx, f.repo, blob.PackKey(f.repo, id))
		if err != nil || !retained {
			t.Fatalf("IsRetainedPack(%s) = %v, %v; want true", id, retained, err)
		}
	}
	// The prefix listing agrees with the counter — both usage sources see
	// the fold's amplification until the sweep reclaims it.
	if listed, err := blob.StorageUsageBytes(ctx, f.blobs, f.repo); err != nil || listed != st.UsageBytes {
		t.Fatalf("listing = %d, %v; want counter %d", listed, err, st.UsageBytes)
	}

	// Amplification bounded: a would-be drip write is denied while the
	// retained bytes hold the headroom...
	quota := st.UsageBytes + 10
	dec, err := f.store.ReservePackBytes(ctx, []string{f.repo},
		[]ref.Pack{{ID: "drip", Key: blob.PackKey(f.repo, "drip"), Size: 20}}, "drip-writer", 0, ref.ReserveNoFloorGen, quota, time.Hour)
	if err != nil || dec.OK {
		t.Fatalf("drip reservation = %+v, %v; want denial against retained bytes", dec, err)
	}

	// ...and the superseded-class sweep (general deletion OFF) returns it.
	past := time.Now().Add(-25 * time.Hour)
	for _, id := range oldIDs {
		p := filepath.Join(f.blobDir, filepath.FromSlash(blob.PackKey(f.repo, id)))
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}
	sweeper := NewSweeper(f.blobs, f.store, SweepConfig{Threshold: 24 * time.Hour, DeleteSuperseded: true}, slog.New(slog.DiscardHandler))
	report, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.SupersededDeleted != len(oldIDs) || report.RetainedBytesReclaimed != pushedBytes {
		t.Fatalf("sweep report = %+v, want superseded=%d retained_reclaimed=%d", report, len(oldIDs), pushedBytes)
	}
	st, err = f.store.QuotaLedgerState(ctx, []string{f.repo}, time.Hour)
	if err != nil || st.UsageBytes != foldSize || st.RetainedBytes != 0 {
		t.Fatalf("post-sweep ledger = %+v, %v; want usage=%d retained=0", st, err, foldSize)
	}
	dec, err = f.store.ReservePackBytes(ctx, []string{f.repo},
		[]ref.Pack{{ID: "drip", Key: blob.PackKey(f.repo, "drip"), Size: 20}}, "drip-writer", 0, ref.ReserveNoFloorGen, quota, time.Hour)
	if err != nil || !dec.OK {
		t.Fatalf("post-sweep reservation = %+v, %v; want admission", dec, err)
	}
	// The consolidated pack alone still reproduces the repo cold.
	f.checkColdHydrate(t)
}
