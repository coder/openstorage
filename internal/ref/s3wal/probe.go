package s3wal

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ProbeKey is the scratch-key PREFIX the boot-time probe exercises. Plan
// §2.1 names the fixed key `_wal/probe`; the code refines that with a
// per-invocation `/<128-bit-hex-nonce>` suffix because two instances
// probing one bucket simultaneously on a shared fixed key can interleave —
// one deleting/recreating it between the other's steps — forging
// ErrExists/ErrLostRace outcomes that falsely reject a compliant store and
// block replica startup. The prefix lives outside `_wal/repo/` so probe
// writes can never collide with a manifest.
const ProbeKey = "_wal/probe"

// Probe failure classes: how a probe step failed. The distinction matters
// operationally — the first three are DEFINITIVE store behavior ("ignored"
// is the silent-corruption class: the store accepted a write the condition
// forbids; "rejected" is loud-but-wrong: a real rejection response that is
// not the required precondition outcome, such as a 403 or the wrong
// conditional sentinel; "mishandled" is any other definitive divergence,
// such as success where failure was required or an unchanged ETag after an
// advance) and mean the store is non-compliant. "inconclusive"
// means the probe could not obtain a definitive store verdict at all
// (ambiguous post-send failure where the write may have landed, a
// transport/not-executed failure, or a failed scratch cleanup): the store
// is NOT proven non-compliant, but a boot probe must be conclusive (§2.6),
// so serving is refused pending a retry.
const (
	ProbeRejected     = "rejected"
	ProbeIgnored      = "ignored"
	ProbeMishandled   = "mishandled"
	ProbeInconclusive = "inconclusive"
)

// ProbeError is the refuse-to-serve verdict from a failed conditional-write
// probe: which step failed, on which surface, classified and diagnosed.
type ProbeError struct {
	Step      int    // 1-5, the §2.6 step that deviated
	Surface   string // the conditional surface under test
	Class     string // ProbeRejected | ProbeIgnored | ProbeMishandled | ProbeInconclusive
	Diagnosis string
}

func (e *ProbeError) Error() string {
	if e.Class == ProbeInconclusive {
		return fmt.Sprintf("s3wal: conditional-write probe step %d (%s) was inconclusive: %s — cannot certify the store (transient/infrastructure, not proven non-compliant); refusing to serve",
			e.Step, e.Surface, e.Diagnosis)
	}
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

// probeAtKey carries the probe logic under an arbitrary scratch-key prefix
// so the external-endpoint qualification lane can isolate under a random
// namespace without touching a real deployment's ProbeKey prefix.
func probeAtKey(ctx context.Context, c *Client, keyPrefix string) error {
	assertf(c != nil, "probeAtKey: nil client")
	assertf(keyPrefix != "", "probeAtKey: empty key prefix")
	key := probeScratchKey(keyPrefix)

	// Best-effort cleanup on EVERY exit path: early step failures — and
	// even an ambiguous create whose bytes may have landed despite the
	// error return — would otherwise strand objects on nonce keys that no
	// later probe revisits, so boot retries against a noncompliant or
	// flaky store would accumulate unreachable scratch forever. The
	// independent bounded context survives probe-context cancellation;
	// cleanup failure never changes the verdict (a leftover is inert
	// scratch under the dedicated probe prefix).
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = c.Delete(cleanupCtx, key)
	}()

	// Step 1: create must succeed. The key carries a fresh per-invocation
	// nonce, so no stale leftover from a prior boot can exist and no
	// pre-clean delete is needed; a create conflict on the fresh key is
	// therefore a REAL verdict (the store claims a never-written key
	// exists), not tolerated noise.
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
	// win; the loser must observe a definitive lost-race rejection. Both
	// goroutines park on a start barrier and are released together:
	// without it the first Advance can complete before the second is even
	// scheduled, degrading this step into a repeat of step 4's SEQUENTIAL
	// stale-write test that a non-atomic check-then-write store would
	// pass. The barrier maximizes client-side overlap but cannot
	// guarantee server-side concurrency on one sample — that residual is
	// covered by the sustained contention lost-update detector recorded
	// in the results doc.
	results := make(chan error, 2)
	startBarrier := make(chan struct{})
	var parked sync.WaitGroup
	parked.Add(2)
	for range 2 {
		go func() {
			body := probeBody()
			parked.Done()
			<-startBarrier
			_, err := c.Advance(ctx, key, body, etag3)
			results <- err
		}()
	}
	parked.Wait()
	close(startBarrier)
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

	return nil
}

// probeStepError classifies an unexpected step error. An AmbiguousError
// (post-send 5xx/timeout: the write may have landed) or a failure that
// never produced a store verdict (ErrNotExecuted: expired context or
// dial-class transport failure) is ProbeInconclusive — a boot probe must
// be conclusive (§2.6), so serving is refused, but the store is not
// accused of non-compliance; those two shapes are exhaustive for
// non-definitive Create/Advance outcomes by the §2.3 taxonomy (see
// putConditional/classifyTransport). Among DEFINITIVE responses, a
// rejection that is simply the wrong one for the step — an unexpected
// conditional-write sentinel or a real non-conditional 4xx — is
// ProbeRejected (loud-but-wrong); any other definitive divergence is
// ProbeMishandled.
func probeStepError(step int, surface string, err error, want string) *ProbeError {
	if _, ok := errors.AsType[*AmbiguousError](err); ok {
		return &ProbeError{Step: step, Surface: surface, Class: ProbeInconclusive,
			Diagnosis: fmt.Sprintf("ambiguous outcome — the write may have landed without a definitive store response: %v", err)}
	}
	if errors.Is(err, ErrNotExecuted) {
		return &ProbeError{Step: step, Surface: surface, Class: ProbeInconclusive,
			Diagnosis: fmt.Sprintf("step was never executed — no store verdict was produced: %v", err)}
	}
	if definitiveRejection(err) {
		return &ProbeError{Step: step, Surface: surface, Class: ProbeRejected,
			Diagnosis: fmt.Sprintf("%s; got a definitive-but-wrong rejection: %v", want, err)}
	}
	return &ProbeError{Step: step, Surface: surface, Class: ProbeMishandled,
		Diagnosis: fmt.Sprintf("%s; got: %v", want, err)}
}

// definitiveRejection reports whether err is a REAL store rejection that is
// merely the wrong one for the probe step: an unexpected conditional-write
// sentinel (each minted from a definitive store status: 412 or exhausted
// 409s) or an SDK error carrying a real 4xx HTTP response. The expected
// sentinel of every step is consumed by the probe before classification,
// so any sentinel reaching here is the wrong one.
func definitiveRejection(err error) bool {
	for _, sentinel := range []error{ErrExists, ErrLostRace, ErrCASContention} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	status, _ := errorStatus(err)
	return status >= 400 && status < 500
}

// probeScratchKey derives the per-invocation scratch key: the prefix plus a
// fresh 128-bit hex nonce.
func probeScratchKey(prefix string) string {
	assertf(prefix != "", "probeScratchKey: empty prefix")
	nonce := make([]byte, 16)
	_, err := rand.Read(nonce)
	assertf(err == nil, "probeScratchKey: crypto/rand failed: %v", err)
	return fmt.Sprintf("%s/%x", prefix, nonce)
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
