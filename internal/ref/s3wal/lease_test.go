// Instance-lease protocol tests (§2.11) over MemCAS: fresh-foreign
// refusal, expired steal, heartbeat loss detection, and clean release.
// In-package so renew() can be driven directly with injected clocks
// instead of waiting on real heartbeat ticks.
package s3wal

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
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
	cas.Now = clk.Now // store clock agrees with the lease clock
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
	// Age is store-clock (fake) plus the real monotonic elapsed since the
	// Get's witness observation — microseconds here, bounded loosely.
	if held.Age < 90*time.Second || held.Age > 91*time.Second || held.TTL != 2*time.Minute {
		t.Fatalf("refusal age/ttl = %v/%v, want ~90s/2m", held.Age, held.TTL)
	}
}

func TestInstanceLeaseExpiredSteal(t *testing.T) {
	ctx := context.Background()
	cas := NewMemCAS()
	clk := newLeaseClock()
	cas.Now = clk.Now // store clock agrees with the lease clock
	cfg := LeaseConfig{TTL: 2 * time.Minute, Now: clk.Now}

	first, err := AcquireInstanceLease(ctx, cas, cfg)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// TTL plus the Date-resolution margin: no longer fresh.
	clk.Advance(2*time.Minute + leaseDateResolution)
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

// TestInstanceLeaseSkewedStarterRefusal is the PR #168 P2 regression: the
// holder's wall clock runs ≥ TTL BEHIND the starter's while its lease is
// freshly written. A wall-clock judgment (starter now − holder stamp)
// reads ≥ TTL and steals — manufacturing exactly the dual-serve topology
// the guard targets. The store-clock judgment must refuse.
func TestInstanceLeaseSkewedStarterRefusal(t *testing.T) {
	ctx := context.Background()
	cas := NewMemCAS()
	storeClk := newLeaseClock()
	cas.Now = storeClk.Now
	cfg := LeaseConfig{TTL: 2 * time.Minute}

	// Holder: wall clock 3 minutes (> TTL) behind the store/starter clock.
	holderCfg := cfg
	holderCfg.Now = func() time.Time { return storeClk.Now().Add(-3 * time.Minute) }
	first, err := AcquireInstanceLease(ctx, cas, holderCfg)
	if err != nil {
		t.Fatalf("skewed holder's acquire: %v", err)
	}

	// Starter: wall clock in sync with the store. Its local delta against
	// the recorded stamp is 3m ≥ TTL, but the lease is seconds old on the
	// store's clock — it must refuse, never steal.
	starterCfg := cfg
	starterCfg.Now = storeClk.Now
	_, err = AcquireInstanceLease(ctx, cas, starterCfg)
	var held *LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("starter ahead of the holder by > TTL: want *LeaseHeldError, got %v", err)
	}
	if held.Holder != first.Holder() {
		t.Fatalf("refusal names holder %q, live lease is %q", held.Holder, first.Holder())
	}
	if held.Age >= time.Minute {
		t.Fatalf("store-clock age = %v — a wall-clock comparison leaked into the verdict", held.Age)
	}
	// The live holder still owns the lease.
	if err := first.renew(ctx); err != nil {
		t.Fatalf("live holder's renew after the refused contender: %v", err)
	}
}

// noWitnessStore serves objects but never a store Date witness — a
// substrate whose §2.4-3 Date capture is broken.
type noWitnessStore struct{ ConditionalStore }

func (noWitnessStore) StoreDate() (date, observedAt time.Time, ok bool) {
	return time.Time{}, time.Time{}, false
}

// noLastModifiedStore strips the Last-Modified stamp from every Get.
type noLastModifiedStore struct{ ConditionalStore }

func (s noLastModifiedStore) Get(ctx context.Context, key string) (Object, error) {
	obj, err := s.ConditionalStore.Get(ctx, key)
	obj.LastModified = time.Time{}
	return obj, err
}

// leaseReadOnlyStore fails on any mutation attempt during a refused acquisition.
type leaseReadOnlyStore struct {
	ConditionalStore
	t *testing.T
}

func (s leaseReadOnlyStore) Create(context.Context, string, []byte) (string, error) {
	s.t.Fatal("refused acquisition attempted Create")
	return "", nil
}

func (s leaseReadOnlyStore) Advance(context.Context, string, []byte, string) (string, error) {
	s.t.Fatal("refused acquisition attempted Advance")
	return "", nil
}

func (s leaseReadOnlyStore) Delete(context.Context, string) error {
	s.t.Fatal("refused acquisition attempted Delete")
	return nil
}

func TestInstanceLeaseUnsafeEvidenceRefusal(t *testing.T) {
	for _, tc := range []struct {
		name  string
		body  string
		wrap  func(ConditionalStore) ConditionalStore
		cause string
	}{
		{name: "malformed JSON", body: `{`},
		{name: "missing holder", body: `{}`},
		{name: "empty holder", body: `{"holder":""}`},
		{
			name: "no Date witness", body: `{"holder":"previous","ttl":"2m"}`,
			wrap:  func(cs ConditionalStore) ConditionalStore { return noWitnessStore{cs} },
			cause: "no store Date witness observed",
		},
		{
			name: "no Last-Modified", body: `{"holder":"previous","ttl":"2m"}`,
			wrap:  func(cs ConditionalStore) ConditionalStore { return noLastModifiedStore{cs} },
			cause: "store returned no Last-Modified stamp",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			cas := NewMemCAS()
			clk := newLeaseClock()
			cas.Now = clk.Now
			if _, err := cas.Create(ctx, LeaseKey, []byte(tc.body)); err != nil {
				t.Fatal(err)
			}
			before, err := cas.Get(ctx, LeaseKey)
			if err != nil {
				t.Fatal(err)
			}
			clk.Advance(10 * time.Minute) // TTL expiry cannot make invalid evidence usable.
			var cs ConditionalStore = leaseReadOnlyStore{ConditionalStore: cas, t: t}
			if tc.wrap != nil {
				cs = tc.wrap(cs)
			}
			lease, err := AcquireInstanceLease(ctx, cs, LeaseConfig{Now: clk.Now})
			if err == nil || lease != nil {
				t.Fatalf("want refusal with no lease, got lease %v, error %v", lease, err)
			}
			reason := "is corrupt"
			if tc.cause != "" {
				reason = "freshness cannot be judged on the store clock"
				if cause := errors.Unwrap(err); cause == nil || cause.Error() != tc.cause {
					t.Errorf("want wrapped store-clock error %q, got %v", tc.cause, cause)
				}
			}
			for _, want := range []string{
				LeaseKey, reason, "refusing to start", "preserve the lease object and error evidence",
				"keep writers fenced", "investigate", "fencing, restart, or TTL expiry does not",
				"https://github.com/coder/openstorage/blob/main/docs/how-to/deploy-production.md#s3wal-operational-signals",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("diagnostic missing %q: %v", want, err)
				}
			}
			for _, unsafe := range []string{"delete", "remove"} {
				if strings.Contains(strings.ToLower(err.Error()), unsafe) {
					t.Errorf("diagnostic suggests destructive recovery: %v", err)
				}
			}
			after, getErr := cas.Get(ctx, LeaseKey)
			if getErr != nil {
				t.Fatalf("lease object no longer readable: %v", getErr)
			}
			if string(after.Body) != string(before.Body) || after.ETag != before.ETag || !after.LastModified.Equal(before.LastModified) {
				t.Fatalf("refused acquisition changed lease object: before %+v, after %+v", before, after)
			}
		})
	}
}

// TestInstanceLeaseStoreClockUnjudgeableFailsClosed: without a Date
// witness or a Last-Modified stamp the store-clock age cannot be judged —
// acquisition must refuse (never fall back to wall clocks, never steal),
// even when the lease is long expired.
func TestInstanceLeaseStoreClockUnjudgeableFailsClosed(t *testing.T) {
	ctx := context.Background()
	cas := NewMemCAS()
	clk := newLeaseClock()
	cas.Now = clk.Now
	cfg := LeaseConfig{TTL: 2 * time.Minute, Now: clk.Now}

	first, err := AcquireInstanceLease(ctx, cas, cfg)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	clk.Advance(10 * time.Minute) // long past the TTL

	for name, cs := range map[string]ConditionalStore{
		"no Date witness":  noWitnessStore{cas},
		"no Last-Modified": noLastModifiedStore{cas},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := AcquireInstanceLease(ctx, cs, cfg)
			if err == nil || !strings.Contains(err.Error(), "cannot be judged") {
				t.Fatalf("want the fail-closed refusal, got %v", err)
			}
		})
	}
	// No blind steal happened: the object still carries the first holder.
	if err := first.renew(ctx); err != nil {
		t.Fatalf("holder's renew after fail-closed contenders: %v", err)
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
		for i := range 8 {
			clk.Advance(30 * time.Second)
			if err := l.renew(ctx); err != nil {
				t.Fatalf("renew %d (ambiguous but landed): %v", i, err)
			}
		}
	})
}

// hangingStore wraps a ConditionalStore; once armed, Advance and Get
// block until ctx cancellation — an endpoint that accepts requests and
// never responds. The SDK surfaces ctx expiry as an error, which the
// wrapper mimics with the ambiguous class (no response ever arrived).
type hangingStore struct {
	ConditionalStore
	armed atomic.Bool
}

func (s *hangingStore) Advance(ctx context.Context, key string, body []byte, etag string) (string, error) {
	if s.armed.Load() {
		<-ctx.Done()
		return "", &AmbiguousError{Op: "advance", Key: key, Cause: ctx.Err()}
	}
	return s.ConditionalStore.Advance(ctx, key, body, etag)
}

func (s *hangingStore) Get(ctx context.Context, key string) (Object, error) {
	if s.armed.Load() {
		<-ctx.Done()
		return Object{}, &AmbiguousError{Op: "get", Key: key, Cause: ctx.Err()}
	}
	return s.ConditionalStore.Get(ctx, key)
}

// TestInstanceLeaseHungStoreFailStops (PR #168 round 2, P1): an endpoint
// that accepts renewal requests and never responds must not park the
// heartbeat forever — each attempt is bounded by one cadence, so the
// headroom check still runs and the fail-stop verdict lands at least one
// cadence before the lease turns stealable on the store clock. Real
// clocks: the per-attempt bound is a real context deadline.
func TestInstanceLeaseHungStoreFailStops(t *testing.T) {
	ctx := context.Background()
	hs := &hangingStore{ConditionalStore: NewMemCAS()}
	l, err := AcquireInstanceLease(ctx, hs, LeaseConfig{TTL: 400 * time.Millisecond, Cadence: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	hs.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- l.Heartbeat(ctx) }()
	// Expected verdict ≈ ttl−cadence (300ms); scheduling delays only age
	// the baseline further, so the ceiling is pure slack.
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "unrenewable") {
			t.Fatalf("heartbeat against a hung store: want an unrenewable fail-stop verdict, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat still parked after 5s — a hung endpoint suppressed the fail-stop")
	}
}

// TestInstanceLeaseRecoveredRenewalKeepsMonotonicBaseline (PR #168 round
// 2, P2): renewals recovered through the re-read path must keep renewedAt
// a LOCAL clock observation — production observations carry a monotonic
// reading (String() shows an " m=±…" suffix) that a JSON-decoded stamp
// never has. Adopting the decoded stamp silently switches the headroom
// check to wall arithmetic: a backward NTP step would extend perceived
// freshness past ttl−cadence while a successor (judging on the store
// clock) steals. Landed writes restore headroom to the attempt-start
// instant; dropped writes must keep the old baseline aging.
func TestInstanceLeaseRecoveredRenewalKeepsMonotonicBaseline(t *testing.T) {
	for name, fault := range map[string]PutFault{
		"ambiguous landed":  FaultAmbiguousApplied,
		"ambiguous dropped": FaultAmbiguousDropped,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cas := NewMemCAS()
			// Real clock: only time.Now observations carry the monotonic
			// reading this test guards.
			l, err := AcquireInstanceLease(ctx, cas, LeaseConfig{TTL: 2 * time.Minute})
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			base := l.renewedAt
			if !strings.Contains(base.String(), " m=") {
				t.Fatalf("acquire baseline %v carries no monotonic reading", base)
			}
			cas.Hooks.PutFault = func(op, key string) PutFault {
				if key == LeaseKey && op == "advance" {
					return fault
				}
				return FaultNone
			}
			if err := l.renew(ctx); err != nil {
				t.Fatalf("recovered renew: %v", err)
			}
			if !strings.Contains(l.renewedAt.String(), " m=") {
				t.Fatalf("recovered renewedAt %v lost its monotonic reading: headroom fell back to wall arithmetic", l.renewedAt)
			}
			switch fault {
			case FaultAmbiguousApplied:
				if l.renewedAt.Before(base) {
					t.Fatalf("landed renewal moved the baseline backward: %v < %v", l.renewedAt, base)
				}
			case FaultAmbiguousDropped:
				if l.renewedAt != base {
					t.Fatalf("dropped renewal refreshed the baseline: %v, want the untouched %v", l.renewedAt, base)
				}
			}
		})
	}
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
