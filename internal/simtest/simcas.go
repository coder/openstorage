package simtest

import (
	"context"
	"fmt"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// SimCAS is one node's fault-injecting transport view of the SHARED
// in-memory conditional store (s3wal.MemCAS) — the s3wal DST fault plan
// (plan §4 Phase 2). It sits BELOW the manifest driver, so its faults
// exercise the driver's own mutate/recover machinery (§2.3), while the
// SimRefStore decorator above the driver keeps exercising
// ref.IdempotentCAS — both recovery layers run in one simulation. Rebuilt
// on node crash, like the process it stands for.
//
// Fault classes per op (drawn from the shared Injector, so decisions are
// deterministic under the serialized scheduler):
//
//   - cas.Create / cas.Advance: FaultClean returns s3wal.ErrNotExecuted
//     without executing (the provably-never-sent class); FaultAmbiguous
//     splits deterministically into applied (execute, discard the result,
//     report *s3wal.AmbiguousError — the §2.3 committed-response-lost
//     window) and dropped (never execute, same error).
//   - cas.Get / cas.List: FaultClean fails the read without executing.
//     Reads are idempotent, so a lost response is indistinguishable from
//     a failed request — one class covers both.
//
// Stale reads are NOT injected (issue #216). The s3wal store contract
// requires strong read-after-write consistency (plan §1/§2.6), the driver
// trusts every unconditional read by construction, and its seq watermark
// only catches a read that regresses below this process's own last
// observation — so a served-stale manifest yields a silently wrong
// read-only verdict (a sweep that "finds nothing to prune", a CAS that
// "lost") with no conditional PUT to expose it. Modelling that class makes
// the exact-state oracles unsatisfiable by any read-only path; the
// retired one-shot arm (PR #159) drove precisely that violation on seed
// 833534285 (pinned in dstRegressions). The 412 lost-race recovery the arm
// was meant to trigger is exercised dozens of times per run through the
// multi-node seed cache (issue #182): every cross-node write leaves the
// other nodes' cached snapshots stale, and their next seeded PUT 412s.
//
// 409-exhaustion (s3wal.ErrCASContention) is deliberately not injected:
// genuine CAS contention arises naturally from the multi-node shared-fake
// topology, and the exhaustion taxonomy is unit-tested in internal/ref.
// Deletes pass through unfaulted: the protocol never guards DELETEs
// (§2.7), and the W6 delete-lease races are unit-tested in internal/ref.
type SimCAS struct {
	mem *s3wal.MemCAS
	inj *Injector
}

var _ s3wal.ConditionalStore = (*SimCAS)(nil)

// NewSimCAS wraps the shared in-memory conditional store for one node.
func NewSimCAS(mem *s3wal.MemCAS, inj *Injector) *SimCAS {
	assertf(mem != nil, "NewSimCAS: nil MemCAS")
	assertf(inj != nil, "NewSimCAS: nil injector")
	return &SimCAS{mem: mem, inj: inj}
}

// Get fetches key, possibly failing clean.
func (c *SimCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	if c.inj.begin(OpCASGet) == FaultClean {
		return s3wal.Object{}, cleanFaultError{op: OpCASGet}
	}
	return c.mem.Get(ctx, key)
}

// GetIfChanged fetches key unless etag is current, possibly failing clean.
func (c *SimCAS) GetIfChanged(ctx context.Context, key, etag string) (s3wal.Object, error) {
	if c.inj.begin(OpCASGet) == FaultClean {
		return s3wal.Object{}, cleanFaultError{op: OpCASGet}
	}
	return c.mem.GetIfChanged(ctx, key, etag)
}

// Create writes key iff absent, under the injector's verdict.
func (c *SimCAS) Create(ctx context.Context, key string, body []byte) (string, error) {
	return c.put(OpCASCreate, "create", key, func() (string, error) {
		return c.mem.Create(ctx, key, body)
	})
}

// Advance overwrites key iff etag is current, under the injector's verdict.
func (c *SimCAS) Advance(ctx context.Context, key string, body []byte, etag string) (string, error) {
	return c.put(OpCASAdvance, "advance", key, func() (string, error) {
		return c.mem.Advance(ctx, key, body, etag)
	})
}

// put runs one conditional PUT under the injector's verdict; kind is the
// s3wal op name carried by AmbiguousError ("create"/"advance").
func (c *SimCAS) put(op, kind, key string, exec func() (string, error)) (string, error) {
	switch class := c.inj.begin(op); class {
	case FaultNone:
		return exec()
	case FaultClean:
		return "", fmt.Errorf("%s %s: %w", op, key, s3wal.ErrNotExecuted)
	case FaultAmbiguous:
		if !c.inj.coin() {
			c.inj.noteInnerOutcome(op, "dropped")
			return "", &s3wal.AmbiguousError{Op: kind, Key: key, Cause: ErrInjectedAmbiguous}
		}
		if _, err := exec(); err != nil {
			// A definitive store rejection (412 lost-race / exists) is a
			// real verdict, not an ambiguity: nothing landed, and MemCAS
			// evaluates preconditions before fault injection for the
			// same reason. Propagate it so the driver exercises its
			// lost-race retry path, not ambiguity recovery — the
			// client-sees-error-nothing-landed class is the dropped
			// arm's job (Codex on PR #159).
			c.inj.noteInnerOutcome(op, fmt.Sprintf("rejected err=%v", err))
			return "", err
		}
		// No ETag in the outcome: manifest bodies carry the crypto-random
		// Writer nonce, so their ETags would poison TraceHash determinism
		// (Codex round 2 on PR #159).
		c.inj.noteInnerOutcome(op, "applied")
		return "", &s3wal.AmbiguousError{Op: kind, Key: key, Cause: ErrInjectedAmbiguous}
	default:
		panic(fmt.Sprintf("simtest: invalid SimCAS put fault class %v", class))
	}
}

// Delete removes key unconditionally (never faulted; see package comment).
func (c *SimCAS) Delete(ctx context.Context, key string) error {
	return c.mem.Delete(ctx, key)
}

// List pages keys under prefix, possibly failing clean.
func (c *SimCAS) List(ctx context.Context, prefix, startAfter string, limit int) ([]string, error) {
	if c.inj.begin(OpCASList) == FaultClean {
		return nil, cleanFaultError{op: OpCASList}
	}
	return c.mem.List(ctx, prefix, startAfter, limit)
}

// StoreDate delegates to the shared store's Date witness.
func (c *SimCAS) StoreDate() (date, observedAt time.Time, ok bool) {
	return c.mem.StoreDate()
}
