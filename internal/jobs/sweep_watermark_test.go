// Destructive-reap watermark tests (issue #120): the sweeper records the
// fleet's "last completed destructive LFS pass" only when the class was
// armed end-to-end and the pass finished its keyspace with zero errors —
// observed evidence, never configured cadence.
package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/coder/open-storage/internal/blob"
	"github.com/coder/open-storage/internal/ref"
)

// wireReapRecorder wires the production watermark write (wire.go's shape)
// into a test SweepConfig.
func wireReapRecorder(t *testing.T, f *sweepFixture, cfg SweepConfig) SweepConfig {
	t.Helper()
	scanState, ok := f.store.(ref.ScanStateStore)
	if !ok {
		t.Fatalf("store %T lacks the scan-state seams", f.store)
	}
	cfg.BeginLFSReapEpoch = scanState.BeginLFSReapEpoch
	cfg.RecordLFSReapComplete = scanState.RecordLFSReapComplete
	return cfg
}

func reapWatermark(t *testing.T, f *sweepFixture) (ref.JobScanState, bool) {
	t.Helper()
	st, ok, err := f.store.(ref.ScanStateStore).GetJobScanState(context.Background(), ref.ScanJobSweep, ref.ScanScopeLFSDestructiveReap)
	if err != nil {
		t.Fatalf("read reap watermark: %v", err)
	}
	return st, ok
}

// failingDeleteBlob simulates a blob store whose deletes fail — the
// "armed but not actually completing" regime the watermark must observe.
type failingDeleteBlob struct {
	blob.BlobStore
}

func (f *failingDeleteBlob) Delete(context.Context, string) error {
	return fmt.Errorf("simulated delete outage")
}

// TestSweepReapWatermark_RecordedOnCleanArmedPass: a delete-mode LFS pass
// that finishes both keyspace listings with zero errors advances the
// watermark; re-running advances it again.
func TestSweepReapWatermark_RecordedOnCleanArmedPass(t *testing.T) {
	f := newSweepFixture(t)
	// One aged rowless orphan so the pass exercises a real reap.
	orphan := blob.LFSKey("wm-repo", strings.Repeat("ab", 32))
	f.upload(orphan, "junk-bytes")
	f.backdate(orphan, 48*time.Hour)

	cfg := wireReapRecorder(t, f, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true})
	report, err := f.sweeper(cfg).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 1 {
		t.Fatalf("report = %+v, want the orphan reaped", report)
	}
	if _, ok := reapWatermark(t, f); !ok {
		t.Fatal("clean armed pass did not record the reap watermark")
	}
}

// TestSweepReapWatermark_LegacyArming (Codex round 17 on PR #132): the
// watermark's payload certifies which delete classes the recorded sweep
// reaped — a legacy-armed pass records the legacy payload, and a later
// unarmed pass overwrites it, so legacy row retirement pauses until the
// next legacy-armed sweep.
func TestSweepReapWatermark_LegacyArming(t *testing.T) {
	f := newSweepFixture(t)
	cfg := wireReapRecorder(t, f, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, DeleteLFSLegacy: true})
	if _, err := f.sweeper(cfg).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if st, ok := reapWatermark(t, f); !ok || !ref.DecodeReapWatermark(st.Payload).Legacy {
		t.Fatalf("watermark after legacy-armed pass = (%+v, %v), want legacy-armed payload", st, ok)
	}

	cfg = wireReapRecorder(t, f, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true})
	if _, err := f.sweeper(cfg).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if st, ok := reapWatermark(t, f); !ok || ref.DecodeReapWatermark(st.Payload).Legacy {
		t.Fatalf("watermark after unarmed pass = (%+v, %v), want canonical-only payload", st, ok)
	}
}

// TestSweepReapWatermark_NotRecordedWithoutArm: list-only passes (DeleteLFS
// off) never advance the watermark, no matter how clean.
func TestSweepReapWatermark_NotRecordedWithoutArm(t *testing.T) {
	f := newSweepFixture(t)
	cfg := wireReapRecorder(t, f, SweepConfig{Threshold: 24 * time.Hour, Delete: true})
	if _, err := f.sweeper(cfg).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, ok := reapWatermark(t, f); ok {
		t.Fatal("list-only LFS pass recorded a destructive-reap watermark")
	}
}

// TestSweepReapWatermark_NotRecordedOnGuardDemotion: the versioning-drift
// guard demoting a delete-mode pass to list-only mid-flight must freeze
// the watermark — the exact "configured but not observed" case issue #120
// closes.
func TestSweepReapWatermark_NotRecordedOnGuardDemotion(t *testing.T) {
	f := newSweepFixture(t)
	cfg := wireReapRecorder(t, f, SweepConfig{
		Threshold: 24 * time.Hour,
		DeleteLFS: true,
		DeleteGuard: func(context.Context) error {
			return fmt.Errorf("bucket versioning drifted on")
		},
	})
	if _, err := f.sweeper(cfg).Sweep(context.Background()); err == nil {
		t.Fatal("Sweep: want the guard demotion reported as an error")
	}
	if _, ok := reapWatermark(t, f); ok {
		t.Fatal("guard-demoted pass recorded a destructive-reap watermark")
	}
}

// TestSweepReapWatermark_NotRecordedOnDeleteFailure: an armed pass whose
// reaps FAIL must not advance the watermark — reclamation refunds would
// otherwise keep flowing while stranded blobs pile up.
func TestSweepReapWatermark_NotRecordedOnDeleteFailure(t *testing.T) {
	f := newSweepFixture(t)
	orphan := blob.LFSKey("wm-fail-repo", strings.Repeat("cd", 32))
	f.upload(orphan, "junk-bytes")
	f.backdate(orphan, 48*time.Hour)

	cfg := wireReapRecorder(t, f, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true})
	sw := NewSweeper(&failingDeleteBlob{BlobStore: f.blobs}, f.store, cfg, slog.New(slog.DiscardHandler))
	if _, err := sw.Sweep(context.Background()); err == nil {
		t.Fatal("Sweep: want the delete outage reported")
	}
	if _, ok := reapWatermark(t, f); ok {
		t.Fatal("failing armed pass recorded a destructive-reap watermark")
	}
}

// TestSweepReapWatermark_RecorderFailureReported: a watermark write failure
// is a sweep error (loud), and the watermark simply does not advance.
func TestSweepReapWatermark_RecorderFailureReported(t *testing.T) {
	f := newSweepFixture(t)
	cfg := SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true,
		RecordLFSReapComplete: func(context.Context, int64, bool) error {
			return fmt.Errorf("scan-state write refused")
		}}
	if _, err := f.sweeper(cfg).Sweep(context.Background()); err == nil || !strings.Contains(err.Error(), "record lfs reap watermark") {
		t.Fatalf("Sweep err = %v, want the recording failure joined", err)
	}
}
