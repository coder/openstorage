// Instance lease (plan §2.11): the best-effort dual-serve guard for the
// zero-infra serve mode (issue #133 Phase 3). One CAS-heartbeat object,
// LeaseKey, detects the accidental two-writers topology — two serve
// processes pointed at one bucket — and turns it into a refused startup or
// a fail-stop, instead of the silent TTL-clock weirdness and cache
// staleness §2.4 describes.
//
// Correctness does NOT depend on this lease: every state change is
// CAS-serialized by the manifest/meta objects regardless. It is purely a
// detection device, and an honest one — freshness comparisons use local
// wall clocks against a stamp another machine may have written, so
// cross-machine skew can delay (never break) detection. The lease body
// records the writer's TTL so a starting process judges freshness by the
// HOLDER's contract, not its own (possibly reconfigured) one.
package s3wal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// LeaseKey is the dual-serve guard object (§2.1 key table).
const LeaseKey = "_wal/instance-lease"

// DefaultLeaseTTL is the §2.11 protocol default; the derived cadence
// (TTL/4 = 30 s) leaves three missed renewals before a foreign starter
// may steal.
const DefaultLeaseTTL = 2 * time.Minute

// LeaseHeldError is the dual-serve refusal: a fresh foreign lease exists,
// so another serve process is (or very recently was) running against this
// bucket. errors.As-able so serve can surface it verbatim.
type LeaseHeldError struct {
	Holder string
	Age    time.Duration
	TTL    time.Duration
}

func (e *LeaseHeldError) Error() string {
	return fmt.Sprintf("s3wal instance lease %s is held by another serve process (holder %.8s…, renewed %v ago, ttl %v): refusing to start — running two nodes against one bucket is the dual-serve topology the zero-infra mode forbids; stop the other process or wait for its lease to expire", LeaseKey, e.Holder, e.Age.Round(time.Second), e.TTL)
}

// leaseDoc is the lease object body. The stamp changes every renewal
// (nanosecond resolution), so successive writes carry distinct bytes and
// therefore distinct ETags (the §5-R4 ABA argument needs no seq here).
type leaseDoc struct {
	Holder string    `json:"holder"` // 128-bit random nonce, fixed per process
	Stamp  time.Time `json:"stamp"`  // holder's wall clock at the write
	TTL    string    `json:"ttl"`    // holder's TTL contract (time.Duration string)
}

// ttlOrDefault parses the recorded TTL contract; unparseable or
// non-positive values fall back to DefaultLeaseTTL (fail toward
// OVER-detection: a bogus TTL never fabricates an expiry).
func (d leaseDoc) ttlOrDefault() time.Duration {
	ttl, err := time.ParseDuration(d.TTL)
	if err != nil || ttl <= 0 {
		return DefaultLeaseTTL
	}
	return ttl
}

// LeaseConfig tunes the lease protocol; the zero value is production.
type LeaseConfig struct {
	TTL     time.Duration    // 0 = DefaultLeaseTTL
	Cadence time.Duration    // 0 = TTL/4 (30 s at the default TTL)
	Now     func() time.Time // nil = time.Now (unit tests inject clocks)
}

// InstanceLease is one held lease: acquired by AcquireInstanceLease,
// renewed by Heartbeat, released by Release.
type InstanceLease struct {
	cs      ConditionalStore
	holder  string
	ttl     time.Duration
	cadence time.Duration
	now     func() time.Time

	stopOnce sync.Once
	stop     chan struct{}

	// etag of our last accepted write; renewedAt is the local instant of
	// the stamp it carries. Heartbeat-goroutine-owned after acquire
	// (Release never touches them).
	etag      string
	renewedAt time.Time
}

// AcquireInstanceLease claims LeaseKey for a fresh holder nonce: create
// when absent, steal when the recorded stamp has aged past the RECORDED
// TTL, refuse with *LeaseHeldError while fresh. Exactly one contender wins
// any create/steal race — both paths are conditional writes.
func AcquireInstanceLease(ctx context.Context, cs ConditionalStore, cfg LeaseConfig) (*InstanceLease, error) {
	assertf(cs != nil, "AcquireInstanceLease: nil conditional store")
	ttl := cfg.TTL
	if ttl == 0 {
		ttl = DefaultLeaseTTL
	}
	assertf(ttl > 0, "AcquireInstanceLease: negative ttl %v", ttl)
	cadence := cfg.Cadence
	if cadence == 0 {
		cadence = ttl / 4
	}
	// The fail-stop threshold in renew is ttl-cadence, which must leave
	// at least one full cadence of failed-renewal headroom to be
	// meaningful (2·cadence ≤ ttl ⇒ threshold ≥ cadence).
	assertf(cadence > 0 && 2*cadence <= ttl, "AcquireInstanceLease: cadence %v leaves no renewal headroom under ttl %v", cadence, ttl)
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	l := &InstanceLease{
		cs:      cs,
		holder:  newHexNonce(),
		ttl:     ttl,
		cadence: cadence,
		now:     now,
		stop:    make(chan struct{}),
	}

	obj, err := cs.Get(ctx, LeaseKey)
	switch {
	case errors.Is(err, ErrNotExist):
		etag, cerr := cs.Create(ctx, LeaseKey, l.encode(now()))
		if cerr != nil {
			// resolveAcquire returns nil when the re-read proves our
			// ambiguous create actually landed (state adopted in place).
			if rerr := l.resolveAcquire(ctx, cerr); rerr != nil {
				return nil, rerr
			}
			return l, nil
		}
		l.etag, l.renewedAt = etag, now()
	case err != nil:
		return nil, fmt.Errorf("s3wal instance lease: read %s: %w", LeaseKey, err)
	default:
		var doc leaseDoc
		if derr := json.Unmarshal(obj.Body, &doc); derr != nil || doc.Holder == "" {
			// Fail closed on a corrupt lease object: freshness cannot be
			// judged, and blind-stealing garbage could mask a live
			// holder. The operator resolves it by deleting the key.
			return nil, fmt.Errorf("s3wal instance lease: %s is corrupt (%v): refusing to start; delete the object after confirming no other serve process runs against this bucket", LeaseKey, derr)
		}
		age := now().Sub(doc.Stamp)
		if heldTTL := doc.ttlOrDefault(); age < heldTTL {
			return nil, &LeaseHeldError{Holder: doc.Holder, Age: age, TTL: heldTTL}
		}
		// Expired: steal. The If-Match CAS lets exactly one stealer win.
		etag, aerr := cs.Advance(ctx, LeaseKey, l.encode(now()), obj.ETag)
		if aerr != nil {
			if rerr := l.resolveAcquire(ctx, aerr); rerr != nil {
				return nil, rerr
			}
			return l, nil
		}
		l.etag, l.renewedAt = etag, now()
	}
	return l, nil
}

// resolveAcquire classifies a failed create/steal write: a re-read showing
// OUR holder proves an ambiguous write landed (win); a foreign holder is
// the dual-serve refusal; anything else surfaces the original error.
func (l *InstanceLease) resolveAcquire(ctx context.Context, werr error) error {
	obj, gerr := l.cs.Get(ctx, LeaseKey)
	if gerr != nil {
		return fmt.Errorf("s3wal instance lease: acquire %s: %w", LeaseKey, werr)
	}
	var doc leaseDoc
	if derr := json.Unmarshal(obj.Body, &doc); derr == nil && doc.Holder != "" {
		if doc.Holder == l.holder {
			l.etag, l.renewedAt = obj.ETag, l.now()
			return nil // our ambiguous write landed — errors.Is(nil) at the caller
		}
		return &LeaseHeldError{Holder: doc.Holder, Age: l.now().Sub(doc.Stamp), TTL: doc.ttlOrDefault()}
	}
	return fmt.Errorf("s3wal instance lease: acquire %s: %w", LeaseKey, werr)
}

func (l *InstanceLease) encode(stamp time.Time) []byte {
	body, err := json.Marshal(leaseDoc{Holder: l.holder, Stamp: stamp.UTC(), TTL: l.ttl.String()})
	assertf(err == nil, "instance lease encode: %v", err)
	return body
}

// Heartbeat renews the lease every cadence until Release or ctx
// cancellation (both return nil — orderly shutdown). A non-nil return is
// LEASE LOSS: the caller must fail-stop the process (crash, not limp —
// §2.11); continuing to serve would be exactly the dual-serve topology
// the lease exists to detect.
func (l *InstanceLease) Heartbeat(ctx context.Context) error {
	ticker := time.NewTicker(l.cadence)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := l.renew(ctx); err != nil {
				return err
			}
		}
	}
}

// renew advances the lease with a fresh stamp. Loss verdicts (non-nil):
// a foreign holder on re-read (stolen), a vanished or corrupt object
// (foreign interference), or renewal failures that persist until the last
// accepted stamp is one cadence away from expiring — past that point a
// foreign starter may steal before the next tick could notice, so
// exclusivity can no longer be proven. Transient store trouble inside
// that window returns nil and retries next tick.
func (l *InstanceLease) renew(ctx context.Context) error {
	now := l.now()
	etag, werr := l.cs.Advance(ctx, LeaseKey, l.encode(now), l.etag)
	if werr == nil {
		l.etag, l.renewedAt = etag, now
		return nil
	}
	obj, gerr := l.cs.Get(ctx, LeaseKey)
	switch {
	case gerr == nil:
		var doc leaseDoc
		if derr := json.Unmarshal(obj.Body, &doc); derr != nil || doc.Holder == "" {
			return fmt.Errorf("s3wal instance lease: %s became corrupt while held (%v): fail-stop", LeaseKey, derr)
		}
		if doc.Holder != l.holder {
			return fmt.Errorf("s3wal instance lease stolen by holder %.8s…: another serve process is running against this bucket (dual-serve): fail-stop", doc.Holder)
		}
		// Our own write is current: adopt its ETag and the stamp it
		// actually carries, then FALL THROUGH to the freshness check —
		// an ambiguous-applied write refreshed the stamp (headroom
		// restored), while a dropped write left the old stamp aging
		// toward stealability despite the healthy read.
		l.etag, l.renewedAt = obj.ETag, doc.Stamp
	case errors.Is(gerr, ErrNotExist):
		select {
		case <-l.stop:
			return nil // Release deleted it mid-tick: orderly shutdown
		default:
		}
		return fmt.Errorf("s3wal instance lease: %s vanished while held (foreign delete): fail-stop", LeaseKey)
	}
	if now.Sub(l.renewedAt) >= l.ttl-l.cadence {
		return fmt.Errorf("s3wal instance lease unrenewable since %v ago (ttl %v): exclusivity can no longer be proven before expiry: fail-stop (write: %v; read: %v)", now.Sub(l.renewedAt).Round(time.Second), l.ttl, werr, gerr)
	}
	return nil // renewal trouble with headroom left: retry next tick
}

// Release stops the heartbeat and deletes the lease object when it is
// still ours (owner-guarded read-then-delete: §2.7 forbids conditional
// DELETE, and the residual TOCTOU — a thief overwriting between our read
// and delete — can only shorten the thief's own detection window, never
// affect correctness). Best-effort: any failure just leaves the lease to
// expire on its TTL. Idempotent.
func (l *InstanceLease) Release(ctx context.Context) {
	l.stopOnce.Do(func() { close(l.stop) })
	obj, err := l.cs.Get(ctx, LeaseKey)
	if err != nil {
		return
	}
	var doc leaseDoc
	if derr := json.Unmarshal(obj.Body, &doc); derr != nil || doc.Holder != l.holder {
		return
	}
	_ = l.cs.Delete(ctx, LeaseKey)
}

// Holder exposes the lease's holder nonce for logs and tests.
func (l *InstanceLease) Holder() string { return l.holder }
