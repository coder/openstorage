// Instance-lease protocol tests (§2.11) over MemCAS: fresh-foreign
// refusal, expired steal, heartbeat loss detection, and clean release.
// In-package so renew() can be driven directly with injected clocks
// instead of waiting on real heartbeat ticks.
package s3wal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// leaseClock is a settable fake clock shared by lease contenders.
type leaseClock struct{ now time.Time }

func (c *leaseClock) Now() time.Time          { return c.now }
func (c *leaseClock) Advance(d time.Duration) { c.now = c.now.Add(d) }
func newLeaseClock() *leaseClock {
	return &leaseClock{now: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
}

func TestInstanceLeaseFreshForeignRefusal(t *testing.T) {
	ctx := context.Background()
	cas := NewMemCAS()
	clk := newLeaseClock()
	cfg := LeaseConfig{TTL: 2 * time.Minute, Now: clk.Now}

	first, err := AcquireInstanceLease(ctx, cas, cfg)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// A second contender inside the TTL must refuse with the dual-serve
	// error naming the live holder.
	clk.Advance(90 * time.Second) // < 2 min TTL
	_, err = AcquireInstanceLease(ctx, cas, cfg)
	var held *LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("second acquire inside the TTL: want *LeaseHeldError, got %v", err)
	}
	if !strings.HasPrefix(first.Holder(), held.Holder[:8]) {
		t.Fatalf("refusal names holder %q, first lease is %q", held.Holder, first.Holder())
	}
	if held.Age != 90*time.Second || held.TTL != 2*time.Minute {
		t.Fatalf("refusal age/ttl = %v/%v, want 90s/2m", held.Age, held.TTL)
	}
}

func TestInstanceLeaseExpiredSteal(t *testing.T) {
	ctx := context.Background()
	cas := NewMemCAS()
	clk := newLeaseClock()
	cfg := LeaseConfig{TTL: 2 * time.Minute, Now: clk.Now}

	first, err := AcquireInstanceLease(ctx, cas, cfg)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	clk.Advance(2 * time.Minute) // exactly the TTL: no longer fresh
	second, err := AcquireInstanceLease(ctx, cas, cfg)
	if err != nil {
		t.Fatalf("steal of an expired lease: %v", err)
	}
	if second.Holder() == first.Holder() {
		t.Fatal("stealer inherited the dead holder's nonce")
	}
	// The dead holder's next renewal must detect the steal and demand a
	// fail-stop.
	if err := first.renew(ctx); err == nil || !strings.Contains(err.Error(), "stolen") {
		t.Fatalf("stolen holder's renew: want a stolen fail-stop verdict, got %v", err)
	}
}

func TestInstanceLeaseHeartbeatLossDetection(t *testing.T) {
	ctx := context.Background()
	t.Run("vanished object", func(t *testing.T) {
		cas := NewMemCAS()
		clk := newLeaseClock()
		l, err := AcquireInstanceLease(ctx, cas, LeaseConfig{TTL: 2 * time.Minute, Now: clk.Now})
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		if err := cas.Delete(ctx, LeaseKey); err != nil {
			t.Fatalf("delete: %v", err)
		}
		clk.Advance(30 * time.Second)
		if err := l.renew(ctx); err == nil || !strings.Contains(err.Error(), "vanished") {
			t.Fatalf("renew after a foreign delete: want a vanished fail-stop verdict, got %v", err)
		}
	})
	t.Run("dropped renewals age out headroom", func(t *testing.T) {
		cas := NewMemCAS()
		clk := newLeaseClock()
		l, err := AcquireInstanceLease(ctx, cas, LeaseConfig{TTL: 2 * time.Minute, Cadence: 30 * time.Second, Now: clk.Now})
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		cas.Hooks.PutFault = func(op, key string) PutFault {
			if key == LeaseKey && op == "advance" {
				return FaultAmbiguousDropped
			}
			return FaultNone
		}
		// Ticks at +30 s and +60 s stay inside the ttl-cadence (90 s)
		// headroom: tolerated, retried.
		for _, at := range []time.Duration{30 * time.Second, 60 * time.Second} {
			clk.Advance(30 * time.Second)
			if err := l.renew(ctx); err != nil {
				t.Fatalf("renew at +%v (headroom left): %v", at, err)
			}
		}
		// The +90 s tick exhausts the headroom: exclusivity is no longer
		// provable before a foreign starter may steal — fail-stop.
		clk.Advance(30 * time.Second)
		if err := l.renew(ctx); err == nil || !strings.Contains(err.Error(), "unrenewable") {
			t.Fatalf("renew past the headroom: want an unrenewable fail-stop verdict, got %v", err)
		}
	})
	t.Run("ambiguous-applied renewal restores headroom", func(t *testing.T) {
		cas := NewMemCAS()
		clk := newLeaseClock()
		l, err := AcquireInstanceLease(ctx, cas, LeaseConfig{TTL: 2 * time.Minute, Cadence: 30 * time.Second, Now: clk.Now})
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		cas.Hooks.PutFault = func(op, key string) PutFault {
			if key == LeaseKey && op == "advance" {
				return FaultAmbiguousApplied
			}
			return FaultNone
		}
		// Every renewal write LANDS (then loses its response): the
		// re-read adopts the fresh stamp, so no tick ever exhausts the
		// headroom no matter how long this goes on.
		for i := 0; i < 8; i++ {
			clk.Advance(30 * time.Second)
			if err := l.renew(ctx); err != nil {
				t.Fatalf("renew %d (ambiguous but landed): %v", i, err)
			}
		}
	})
}

func TestInstanceLeaseCleanRelease(t *testing.T) {
	ctx := context.Background()
	cas := NewMemCAS()
	clk := newLeaseClock()
	cfg := LeaseConfig{TTL: 2 * time.Minute, Now: clk.Now}

	l, err := AcquireInstanceLease(ctx, cas, cfg)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	l.Release(ctx)
	if _, err := cas.Get(ctx, LeaseKey); !errors.Is(err, ErrNotExist) {
		t.Fatalf("lease object after release: want ErrNotExist, got %v", err)
	}
	// A successor starts immediately — no TTL wait after a clean exit.
	if _, err := AcquireInstanceLease(ctx, cas, cfg); err != nil {
		t.Fatalf("acquire after clean release: %v", err)
	}
	l.Release(ctx) // idempotent, and never deletes the successor's lease
	if _, err := cas.Get(ctx, LeaseKey); err != nil {
		t.Fatalf("successor's lease after a stale re-release: %v", err)
	}
}

// TestInstanceLeaseReleaseStopsHeartbeat proves the Heartbeat loop exits
// nil on Release (orderly shutdown, not a loss verdict).
func TestInstanceLeaseReleaseStopsHeartbeat(t *testing.T) {
	ctx := context.Background()
	cas := NewMemCAS()
	l, err := AcquireInstanceLease(ctx, cas, LeaseConfig{TTL: 200 * time.Millisecond, Cadence: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- l.Heartbeat(ctx) }()
	time.Sleep(120 * time.Millisecond) // let a couple of real renewals run
	l.Release(ctx)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("heartbeat after release: want nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat did not stop after release")
	}
}
