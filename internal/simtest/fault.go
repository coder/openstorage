// Package simtest is the deterministic simulation testing harness (issue
// #3, ported from waltier's SimStore + DST design): fault-injection
// decorators for blob.BlobStore and ref.RefStore plus a seeded, serialized
// multi-node simulation with invariant oracles.
//
// The package is test-only by convention: nothing under cmd/codestorage or
// the serve wiring imports it. It ships as regular (non _test.go) files so
// the seed-reproduction CLI (cmd/dst) and the go-test entry points share one
// implementation.
//
// The fault model distinguishes the two classes that matter for storage
// correctness:
//
//   - clean fault: the operation did NOT reach the backing store; the caller
//     sees an error. The injected error satisfies pgconn's SafeToRetry
//     contract, exactly like the real stores' acquire-stage failures, so
//     ref.IdempotentCAS classifies it as definitive non-execution.
//   - ambiguous fault: the operation DID execute against the backing store,
//     but its result is discarded and the caller sees an error — the
//     "committed, then the connection dropped" class behind issue #2.
//
// Blob reads additionally support a transient not-found fault (wrapping
// blob.ErrNotFound) modeling a listed pack vanishing mid-hydration — the
// class behind issue #6's bounded retry.
package simtest

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
)

// FaultClass selects what an injected fault does to the decorated call.
type FaultClass int

const (
	// FaultNone means the call proceeds to the backing store untouched.
	FaultNone FaultClass = iota
	// FaultClean fails the call WITHOUT executing it.
	FaultClean
	// FaultAmbiguous executes the call, discards its result, and fails it.
	FaultAmbiguous
	// FaultNotFound fails a blob read with blob.ErrNotFound without
	// executing it (transient missing-object; only valid on
	// OpBlobDownloadFile and OpBlobStat).
	FaultNotFound
)

// String returns the fault class name for traces.
func (c FaultClass) String() string {
	switch c {
	case FaultNone:
		return "none"
	case FaultClean:
		return "clean"
	case FaultAmbiguous:
		return "ambiguous"
	case FaultNotFound:
		return "notfound"
	}
	return fmt.Sprintf("FaultClass(%d)", int(c))
}

// Operation names the decorators inject on. The ref/blob prefixes keep one
// flat namespace so a single Injector can serve both decorators.
const (
	OpRefListRefs         = "ref.ListRefs"
	OpRefListRefsWithMeta = "ref.ListRefsWithMeta"
	OpRefGetRef           = "ref.GetRef"
	OpRefCASRef           = "ref.CASRef"
	OpRefRegisterPacks    = "ref.RegisterPacks"
	OpRefListPacks        = "ref.ListPacks"
	OpRefGetGeneration    = "ref.GetGeneration"

	OpBlobUploadFile   = "blob.UploadFile"
	OpBlobDownloadFile = "blob.DownloadFile"
	OpBlobList         = "blob.List"
	OpBlobSignedURL    = "blob.SignedURL"
	OpBlobStat         = "blob.Stat"
	OpBlobEnsureBucket = "blob.EnsureBucket"
)

// Sentinel errors for injected faults; match with errors.Is. The concrete
// returned errors carry the op name.
var (
	// ErrInjectedClean marks a clean fault: the op did not execute.
	ErrInjectedClean = errors.New("simtest: injected clean fault (op did not execute)")
	// ErrInjectedAmbiguous marks an ambiguous fault: the op executed but
	// the result was lost.
	ErrInjectedAmbiguous = errors.New("simtest: injected ambiguous fault (op executed, result lost)")
)

// cleanFaultError is the concrete clean-fault error. It satisfies pgconn's
// SafeToRetry contract (the request never reached the backend), which is
// how the real stores mark their acquire-stage failures — so
// ref.IdempotentCAS must treat it as definitive and never recover it.
type cleanFaultError struct{ op string }

func (e cleanFaultError) Error() string {
	return fmt.Sprintf("%s: %v", e.op, ErrInjectedClean)
}
func (e cleanFaultError) Unwrap() error   { return ErrInjectedClean }
func (cleanFaultError) SafeToRetry() bool { return true }

// ambiguousFaultError is the concrete ambiguous-fault error: a bare
// transport-shaped error (no SafeToRetry, no backend error type), which
// ref.IdempotentCAS classifies as ambiguous.
type ambiguousFaultError struct{ op string }

func (e ambiguousFaultError) Error() string {
	return fmt.Sprintf("%s: %v", e.op, ErrInjectedAmbiguous)
}
func (e ambiguousFaultError) Unwrap() error { return ErrInjectedAmbiguous }

// FaultEvent records one injected fault, for traces and oracle bookkeeping.
type FaultEvent struct {
	Seq   int // 1-based injector-wide sequence number of the faulted call
	Op    string
	Class FaultClass
	// InnerOutcome describes the backing store's real result for ambiguous
	// faults (e.g. "won=true err=<nil>"); empty for non-executed classes.
	InnerOutcome string
}

// Probs are per-op fault probabilities; the remainder is FaultNone. Draws
// happen once per matching call in call order, so a fixed seed yields a
// fixed decision sequence under serialized execution.
type Probs struct {
	Clean     float64
	Ambiguous float64
	NotFound  float64
}

type plannedFault struct {
	remaining int // fires when the countdown reaches zero
	class     FaultClass
}

// Injector decides, per decorated call, whether to inject a fault. One
// injector is shared by all decorators of a scenario: a single seeded PRNG
// stream plus FIFO one-shot plans keep decisions deterministic as long as
// calls are serialized (the DST scheduler runs one op at a time; the
// decorators still lock so misuse cannot corrupt state).
type Injector struct {
	mu     sync.Mutex
	rng    *rand.Rand
	seq    int
	plans  map[string][]plannedFault
	probs  map[string]Probs
	counts map[string]int
	fired  []FaultEvent
}

// NewInjector returns an Injector seeded with seed. The zero probability
// state injects nothing until FailNext/FailNth/SetProbs are called.
func NewInjector(seed uint64) *Injector {
	return &Injector{
		rng:    rand.New(rand.NewPCG(seed, 0x51D3_57A7_E5D5_71ED)), //nolint:gosec // deterministic simulation PRNG, not crypto
		plans:  make(map[string][]plannedFault),
		probs:  make(map[string]Probs),
		counts: make(map[string]int),
	}
}

// FailNext queues a one-shot fault on the next call of op.
func (in *Injector) FailNext(op string, class FaultClass) { in.FailNth(op, 1, class) }

// FailNth queues a one-shot fault on the n-th call of op counted from now
// (n=1 is the next call). Queued plans fire in FIFO order: each plan's
// countdown starts only after the previous plan for the same op has fired.
func (in *Injector) FailNth(op string, n int, class FaultClass) {
	assertf(op != "", "FailNth: empty op")
	assertf(n >= 1, "FailNth(%s): n %d < 1", op, n)
	assertf(class == FaultClean || class == FaultAmbiguous || class == FaultNotFound,
		"FailNth(%s): invalid class %v", op, class)
	in.mu.Lock()
	defer in.mu.Unlock()
	in.plans[op] = append(in.plans[op], plannedFault{remaining: n, class: class})
}

// SetProbs configures probabilistic fault injection for op. Passing all
// zeros clears it. One-shot plans take precedence over probabilities.
func (in *Injector) SetProbs(op string, p Probs) {
	assertf(op != "", "SetProbs: empty op")
	assertf(p.Clean >= 0 && p.Ambiguous >= 0 && p.NotFound >= 0 &&
		p.Clean+p.Ambiguous+p.NotFound <= 1,
		"SetProbs(%s): invalid probabilities %+v", op, p)
	in.mu.Lock()
	defer in.mu.Unlock()
	if p == (Probs{}) {
		delete(in.probs, op)
		return
	}
	in.probs[op] = p
}

// Count returns how many calls of op the decorators have seen (including
// faulted ones).
func (in *Injector) Count(op string) int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.counts[op]
}

// Counts returns a copy of all op counters.
func (in *Injector) Counts() map[string]int {
	in.mu.Lock()
	defer in.mu.Unlock()
	out := make(map[string]int, len(in.counts))
	for op, n := range in.counts {
		out[op] = n
	}
	return out
}

// Fired returns a copy of every injected fault so far, in injection order.
func (in *Injector) Fired() []FaultEvent {
	in.mu.Lock()
	defer in.mu.Unlock()
	out := make([]FaultEvent, len(in.fired))
	copy(out, in.fired)
	return out
}

// FiredSince returns injected faults with Seq > seq, plus the current
// high-water mark to pass back next time (per-step fault attribution). It
// does not mutate injector state, so overlapping windows are fine.
func (in *Injector) FiredSince(seq int) ([]FaultEvent, int) {
	in.mu.Lock()
	defer in.mu.Unlock()
	var out []FaultEvent
	for _, ev := range in.fired {
		if ev.Seq > seq {
			out = append(out, ev)
		}
	}
	return out, in.seq
}

// Seq returns the current call sequence number (a mark for FiredSince).
func (in *Injector) Seq() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.seq
}

// begin counts one call of op and decides its fault class. Decorators call
// it exactly once per operation.
func (in *Injector) begin(op string) FaultClass {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.counts[op]++
	in.seq++

	if q := in.plans[op]; len(q) > 0 {
		q[0].remaining--
		assertf(q[0].remaining >= 0, "begin(%s): negative countdown", op)
		if q[0].remaining == 0 {
			class := q[0].class
			in.plans[op] = q[1:]
			in.record(op, class)
			return class
		}
		return FaultNone
	}

	p, ok := in.probs[op]
	if !ok {
		return FaultNone
	}
	r := in.rng.Float64()
	var class FaultClass
	switch {
	case r < p.Clean:
		class = FaultClean
	case r < p.Clean+p.Ambiguous:
		class = FaultAmbiguous
	case r < p.Clean+p.Ambiguous+p.NotFound:
		class = FaultNotFound
	default:
		return FaultNone
	}
	in.record(op, class)
	return class
}

// record appends a FaultEvent for the current seq. Callers hold in.mu.
func (in *Injector) record(op string, class FaultClass) {
	in.fired = append(in.fired, FaultEvent{Seq: in.seq, Op: op, Class: class})
}

// noteInnerOutcome attaches the backing store's real outcome to the most
// recent fired event (ambiguous faults execute the op after begin returns).
func (in *Injector) noteInnerOutcome(op, outcome string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	assertf(len(in.fired) > 0, "noteInnerOutcome(%s): no fired events", op)
	last := &in.fired[len(in.fired)-1]
	assertf(last.Op == op, "noteInnerOutcome(%s): last fired event is %s", op, last.Op)
	last.InnerOutcome = outcome
}

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("simtest assertion failed: "+format, args...))
	}
}
