package simtest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// SimCAS is one node's fault-injecting transport view of the SHARED
// in-memory conditional store (s3wal.MemCAS) — the s3wal DST fault plan
// (plan §4 Phase 2). It sits BELOW the manifest driver, so its faults
// exercise the driver's own mutate/recover machinery (§2.3), while the
// SimRefStore decorator above the driver keeps exercising
// ref.IdempotentCAS — both recovery layers run in one simulation. Rebuilt
// on node crash (process death drops armed one-shots with the process).
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
//   - Stale reads are TARGETED, not probabilistic: armStaleGet serves the
//     previous manifest version on this node's next Get. Only doCrash
//     arms it, immediately after the node's handles were rebuilt, because
//     a stale read served to a live driver instance trips its
//     seq-watermark fidelity assert BY DESIGN (plan §2.3) — only a fresh
//     instance (empty watermark) can legally observe one, exactly like a
//     freshly booted node reading a laggy replica.
//
// 409-exhaustion (s3wal.ErrCASContention) is deliberately not injected:
// genuine CAS contention arises naturally from the multi-node shared-fake
// topology, and the exhaustion taxonomy is unit-tested in internal/ref.
// Deletes pass through unfaulted: the protocol never guards DELETEs
// (§2.7), and the W6 delete-lease races are unit-tested in internal/ref.
type SimCAS struct {
	mem *s3wal.MemCAS
	inj *Injector

	mu    sync.Mutex
	stale bool // serve the previous version on the next Get
}

var _ s3wal.ConditionalStore = (*SimCAS)(nil)

// NewSimCAS wraps the shared in-memory conditional store for one node.
func NewSimCAS(mem *s3wal.MemCAS, inj *Injector) *SimCAS {
	assertf(mem != nil, "NewSimCAS: nil MemCAS")
	assertf(inj != nil, "NewSimCAS: nil injector")
	return &SimCAS{mem: mem, inj: inj}
}

// armStaleGet directs this node's next Get to serve the key's previous
// version when one exists (see the package comment for why only fresh
// driver instances may observe one).
func (c *SimCAS) armStaleGet() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stale = true
}

// takeStale consumes the one-shot stale arm.
func (c *SimCAS) takeStale() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stale
	c.stale = false
	return s
}

// Get fetches key, possibly failing clean or serving the armed stale
// version.
func (c *SimCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	if c.inj.begin(OpCASGet) == FaultClean {
		return s3wal.Object{}, cleanFaultError{op: OpCASGet}
	}
	if c.takeStale() {
		if v, ok := c.mem.Prev(key); ok {
			c.inj.noteInjected(OpCASGet, FaultStale)
			return v, nil
		}
	}
	return c.mem.Get(ctx, key)
}

// GetIfChanged fetches key unless etag is current (no stale injection:
// the armed stale read targets the driver's unconditional manifest read).
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
