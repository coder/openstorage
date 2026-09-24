package s3wal

import (
	"context"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/metrics"
)

// TestInstanceLeaseMetricsRenewalFailures drives the existing dropped-write
// fault seam: every failed renewal write counts once (ADR-48), including
// the attempt whose verdict is the fail-stop, and a clean renewal does not.
func TestInstanceLeaseMetricsRenewalFailures(t *testing.T) {
	ctx := context.Background()
	const name = "openstorage_s3wal_lease_renewal_failures_total"
	cas := NewMemCAS()
	clk := newLeaseClock()
	l, err := AcquireInstanceLease(ctx, cas, LeaseConfig{TTL: 2 * time.Minute, Cadence: 30 * time.Second, Now: clk.Now})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	before := metrics.Value(name, nil)
	clk.Advance(30 * time.Second)
	if err := l.renew(ctx); err != nil {
		t.Fatalf("clean renew: %v", err)
	}
	if d := metrics.Value(name, nil) - before; d != 0 {
		t.Fatalf("clean renewal moved the failure counter by %v", d)
	}
	cas.Hooks.PutFault = func(op, key string) PutFault {
		if key == LeaseKey && op == "advance" {
			return FaultAmbiguousDropped
		}
		return FaultNone
	}
	for range 2 {
		clk.Advance(30 * time.Second)
		if err := l.renew(ctx); err != nil {
			t.Fatalf("tolerated renew: %v", err)
		}
	}
	clk.Advance(60 * time.Second)
	if err := l.renew(ctx); err == nil {
		t.Fatal("renew past the headroom: want a fail-stop verdict")
	}
	if d := metrics.Value(name, nil) - before; d != 3 {
		t.Fatalf("three failed renewal writes moved the counter by %v, want 3", d)
	}
}
