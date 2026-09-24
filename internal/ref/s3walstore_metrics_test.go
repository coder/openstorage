package ref

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/openstorage/internal/metrics"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// metricDelta runs fn and returns how much the series moved.
func metricDelta(name string, labels map[string]string, fn func()) float64 {
	before := metrics.Value(name, labels)
	fn()
	return metrics.Value(name, labels) - before
}

const (
	contentionMetric = "openstorage_s3wal_cas_contention_exhausted_total"
	sizeGuardMetric  = "openstorage_s3wal_manifest_size_guard_total"
)

// TestS3WALMetricsContention drives the existing MemCAS contention fault
// seam through a refs-manifest CAS and a scan-state write and asserts the
// ADR-48 counter moves exactly once per terminal mutation, and not at all
// on success.
func TestS3WALMetricsContention(t *testing.T) {
	ctx := context.Background()
	sha1, sha2 := testSHA(0x11), testSHA(0x22)

	t.Run("refs", func(t *testing.T) {
		mc, a, _ := newRecoveryStores(t)
		refs := map[string]string{"scope": "refs"}
		if d := metricDelta(contentionMetric, refs, func() {
			if won, err := a.CASRef(ctx, "r-metrics", "refs/heads/main", ZeroSHA, sha1); err != nil || !won {
				t.Fatalf("seed = (%v, %v)", won, err)
			}
		}); d != 0 {
			t.Fatalf("successful CAS moved the contention counter by %v", d)
		}
		armPutFault(mc, s3wal.FaultContention)
		if d := metricDelta(contentionMetric, refs, func() {
			if _, err := a.CASRef(ctx, "r-metrics", "refs/heads/main", sha1, sha2); !errors.Is(err, s3wal.ErrCASContention) {
				t.Fatalf("err = %v, want ErrCASContention", err)
			}
		}); d != 1 {
			t.Fatalf("contention exhaustion moved refs counter by %v, want 1", d)
		}
	})

	t.Run("scan_state", func(t *testing.T) {
		mc, a, _ := newRecoveryStores(t)
		scan := map[string]string{"scope": "scan_state"}
		armPutFault(mc, s3wal.FaultContention)
		if d := metricDelta(contentionMetric, scan, func() {
			if err := a.PutJobScanState(ctx, "metrics-job", "scope", "payload", 1); !errors.Is(err, s3wal.ErrCASContention) {
				t.Fatalf("err = %v, want ErrCASContention", err)
			}
		}); d != 1 {
			t.Fatalf("contention exhaustion moved scan_state counter by %v, want 1", d)
		}
	})
}

// TestS3WALMetricsSizeGuard drives the existing threshold overrides: a
// refusal counts once, and the warning counts only when it is logged
// (once per base).
func TestS3WALMetricsSizeGuard(t *testing.T) {
	ctx := context.Background()
	sha1 := testSHA(0x11)

	_, a, _ := newRecoveryStores(t)
	if won, err := a.CASRef(ctx, "r-guard", "refs/heads/main", ZeroSHA, sha1); err != nil || !won {
		t.Fatalf("seed = (%v, %v)", won, err)
	}
	a.hardCapBytes = 1
	if d := metricDelta(sizeGuardMetric, map[string]string{"result": "refuse"}, func() {
		if _, err := a.CASRef(ctx, "r-guard", "refs/heads/refused", ZeroSHA, sha1); err == nil {
			t.Fatal("growing CAS over the hard cap succeeded")
		}
	}); d != 1 {
		t.Fatalf("refusal moved the refuse counter by %v, want 1", d)
	}

	_, w, _ := newRecoveryStores(t)
	w.softWarnBytes = 1
	warn := map[string]string{"result": "warn"}
	if d := metricDelta(sizeGuardMetric, warn, func() {
		for i, branch := range []string{"refs/heads/a", "refs/heads/b"} {
			if won, err := w.CASRef(ctx, "r-warn", branch, ZeroSHA, sha1); err != nil || !won {
				t.Fatalf("growth %d = (%v, %v)", i, won, err)
			}
		}
	}); d != 1 {
		t.Fatalf("two over-threshold growths of one base moved warn by %v, want 1 (logged once)", d)
	}
}
