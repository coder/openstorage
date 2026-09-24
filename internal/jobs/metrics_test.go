package jobs

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/metrics"
)

const jobRunsMetric = "openstorage_job_runs_total"

func jobDelta(job, result string, fn func()) float64 {
	labels := map[string]string{"job": job, "result": result}
	before := metrics.Value(jobRunsMetric, labels)
	fn()
	return metrics.Value(jobRunsMetric, labels) - before
}

// TestMetricsSweepOutcomes: each Sweep execution counts exactly one
// outcome (ADR-48) on the choke point both queue backends share.
func TestMetricsSweepOutcomes(t *testing.T) {
	f := newSweepFixture(t)
	seedSweepWorld(f)
	if d := jobDelta("orphan_sweep", "success", func() {
		if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background()); err != nil {
			t.Fatalf("Sweep: %v", err)
		}
	}); d != 1 {
		t.Fatalf("successful sweep moved success by %v, want 1", d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if d := jobDelta("orphan_sweep", "error", func() {
		if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(ctx); err == nil {
			t.Fatal("Sweep with a cancelled context succeeded")
		}
	}); d != 1 {
		t.Fatalf("failed sweep moved error by %v, want 1", d)
	}
}

// TestMetricsRepackOutcomes: success = folded cleanly, noop = nothing to
// fold, error = any error (here the injected rolled-back swap).
func TestMetricsRepackOutcomes(t *testing.T) {
	ctx := context.Background()

	f := newRepackFixture(t)
	f.push(t)
	if d := jobDelta("repack", "noop", func() {
		if folded, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil || folded {
			t.Fatalf("single-pack repack = (%v, %v), want a no-op", folded, err)
		}
	}); d != 1 {
		t.Fatalf("no-op repack moved noop by %v, want 1", d)
	}
	f.push(t)
	if d := jobDelta("repack", "success", func() {
		if folded, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil || !folded {
			t.Fatalf("two-pack repack = (%v, %v), want folded", folded, err)
		}
	}); d != 1 {
		t.Fatalf("clean fold moved success by %v, want 1", d)
	}

	g := newRepackFixture(t)
	for range 3 {
		g.push(t)
	}
	r := NewRepacker(failingReplaceStore{g.store}, g.blobs, fixtureMaterialize(g.blobs, t.TempDir()), slog.New(slog.DiscardHandler))
	if d := jobDelta("repack", "error", func() {
		if _, err := r.Repack(ctx, g.repo); err == nil {
			t.Fatal("repack with a failing swap succeeded")
		}
	}); d != 1 {
		t.Fatalf("failed repack moved error by %v, want 1", d)
	}
}
