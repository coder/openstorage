package s3wal

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
)

// ProbeKey is the fixed scratch key the boot-time probe exercises (plan
// §2.1). It lives outside `_wal/repo/` so probe writes can never collide
// with a manifest.
const ProbeKey = "_wal/probe"

// Probe failure classes: how the store deviated on a probe step. The
// distinction matters operationally — "ignored" is the silent-corruption
// class (the store accepted a write the condition forbids), "rejected" is
// loud-but-wrong, "mishandled" is any other divergence, and "ambiguous"
// means the probe could not obtain a definitive verdict.
const (
	ProbeRejected   = "rejected"
	ProbeIgnored    = "ignored"
	ProbeMishandled = "mishandled"
	ProbeAmbiguous  = "ambiguous"
)

// ProbeError is the refuse-to-serve verdict from a failed conditional-write
// probe: which step failed, on which surface, classified and diagnosed.
type ProbeError struct {
	Step      int    // 1-5, the §2.6 step that deviated
	Surface   string // the conditional surface under test
	Class     string // ProbeRejected | ProbeIgnored | ProbeMishandled | ProbeAmbiguous
	Diagnosis string
}

func (e *ProbeError) Error() string {
	return fmt.Sprintf("s3wal: store failed conditional-write probe step %d (%s): %s: %s — refusing to serve",
		e.Step, e.Surface, e.Class, e.Diagnosis)
}

// Probe proves the store's conditional-write fidelity before serving (plan
// §2.6): a store that ignores the conditional headers corrupts silently —
// the worst failure mode — so any deviation refuses to serve with an
// explicit diagnosis. The five steps: create must succeed; a duplicate
// create must be rejected; a matched advance must succeed with a new ETag;
// a stale-ETag advance must be rejected; two racing advances from one
// snapshot must produce exactly one winner.
func Probe(ctx context.Context, c *Client) error {
	return probeAtKey(ctx, c, ProbeKey)
}

// probeAtKey carries the probe logic at an arbitrary key so the external
// endpoint qualification lane can isolate under a random prefix without
// touching a real deployment's ProbeKey.
func probeAtKey(ctx context.Context, c *Client, key string) error {
	assertf(c != nil, "probeAtKey: nil client")
	assertf(key != "", "probeAtKey: empty key")

	// Step 1: scratch-clean, then create must succeed. The delete is pure
	// cleanup so a leftover object from a prior boot cannot fake a step-2
	// verdict — explicitly NOT a conditional-delete correctness dependency
	// (§2.7 stands: the protocol never conditions a DELETE).
	if err := c.Delete(ctx, key); err != nil {
		return &ProbeError{Step: 1, Surface: "unconditional delete (scratch cleanup)", Class: ProbeRejected,
			Diagnosis: fmt.Sprintf("store rejected the probe-key cleanup delete: %v", err)}
	}
	etag1, err := c.Create(ctx, key, probeBody())
	if err != nil {
		return probeStepError(1, "create with If-None-Match: *", err,
			"store rejected creating a key that does not exist")
	}

	// Step 2: duplicate create must be rejected with a 412 precondition.
	switch _, err := c.Create(ctx, key, probeBody()); {
	case err == nil:
		return &ProbeError{Step: 2, Surface: "duplicate create with If-None-Match: *", Class: ProbeIgnored,
			Diagnosis: "store IGNORES If-None-Match: * — it accepted a duplicate create (silent-corruption class)"}
	case errors.Is(err, ErrExists):
		// Correct: rejected with 412, not executed.
	default:
		return probeStepError(2, "duplicate create with If-None-Match: *", err,
			"want a 412-precondition rejection (ErrExists)")
	}

	// Step 3: a matched advance must succeed and return a NEW ETag — the
	// bodies differ by nonce, so an unchanged ETag would break CAS chaining.
	etag3, err := c.Advance(ctx, key, probeBody(), etag1)
	if err != nil {
		return probeStepError(3, "advance with matching If-Match", err,
			"store rejected an advance whose If-Match ETag was current")
	}
	if etag3 == etag1 {
		return &ProbeError{Step: 3, Surface: "advance with matching If-Match", Class: ProbeMishandled,
			Diagnosis: fmt.Sprintf("store returned the same ETag %q for different object bytes — CAS chaining would ABA", etag3)}
	}

	// Step 4: an advance carrying the stale pre-step-3 ETag must be
	// rejected with 412.
	switch _, err := c.Advance(ctx, key, probeBody(), etag1); {
	case err == nil:
		return &ProbeError{Step: 4, Surface: "advance with stale If-Match", Class: ProbeIgnored,
			Diagnosis: "store IGNORES If-Match — it accepted an advance conditioned on a superseded ETag (silent-corruption class)"}
	case errors.Is(err, ErrLostRace):
		// Correct: rejected with 412, not executed.
	default:
		return probeStepError(4, "advance with stale If-Match", err,
			"want a 412-precondition rejection (ErrLostRace)")
	}

	// Step 5: two racing advances from one snapshot ETag — exactly one may
	// win; the loser must observe a definitive lost-race rejection.
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := c.Advance(ctx, key, probeBody(), etag3)
			results <- err
		}()
	}
	wins := 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			wins++
		case errors.Is(err, ErrLostRace):
			// The expected loser outcome.
		default:
			return probeStepError(5, "racing advances from one snapshot", err,
				"want exactly one winner and one 412 lost-race rejection")
		}
	}
	switch wins {
	case 1:
		// Correct: the store serialized the race.
	case 2:
		return &ProbeError{Step: 5, Surface: "racing advances from one snapshot", Class: ProbeIgnored,
			Diagnosis: "store LOST AN UPDATE — both racing advances from the same snapshot ETag succeeded (silent-corruption class)"}
	default:
		return &ProbeError{Step: 5, Surface: "racing advances from one snapshot", Class: ProbeMishandled,
			Diagnosis: "no racing advance succeeded — the store over-rejects or livelocks conditional writes"}
	}

	// Cleanup is best-effort: a leftover probe object is inert scratch and
	// the next probe deletes it first.
	_ = c.Delete(ctx, key)
	return nil
}

// probeStepError classifies an unexpected step error: an ambiguous outcome
// means the probe cannot certify the store (a boot probe must be
// conclusive); anything else is a mishandled conditional.
func probeStepError(step int, surface string, err error, want string) *ProbeError {
	var amb *AmbiguousError
	if errors.As(err, &amb) {
		return &ProbeError{Step: step, Surface: surface, Class: ProbeAmbiguous,
			Diagnosis: fmt.Sprintf("ambiguous outcome during probe — cannot certify the store: %v", err)}
	}
	return &ProbeError{Step: step, Surface: surface, Class: ProbeMishandled,
		Diagnosis: fmt.Sprintf("%s; got: %v", want, err)}
}

// probeBody returns a fresh random-nonce body so every accepted probe write
// is byte-distinguishable (distinct ETags on stores with content-derived
// ETags, and distinguishable winners in the racing step).
func probeBody() []byte {
	nonce := make([]byte, 16)
	_, err := rand.Read(nonce)
	assertf(err == nil, "probeBody: crypto/rand failed: %v", err)
	return append([]byte("s3wal-probe-"), nonce...)
}
