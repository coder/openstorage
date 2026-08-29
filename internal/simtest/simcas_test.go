package simtest

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// coinSeed finds an injector seed whose FIRST rng draw puts an ambiguous
// PUT on the requested coin arm (one-shot plans consume no rng, so the
// coin is the first draw).
func coinSeed(t *testing.T, applied bool) uint64 {
	t.Helper()
	for seed := range uint64(100) {
		if NewInjector(seed).coin() == applied {
			return seed
		}
	}
	t.Fatal("no seed found for the requested coin arm")
	return 0
}

// TestSimCASAmbiguousAppliedCommits pins the applied arm: the write lands
// in the store, and the caller sees only the ambiguity.
func TestSimCASAmbiguousAppliedCommits(t *testing.T) {
	ctx := context.Background()
	mem := s3wal.NewMemCAS()
	etag, err := mem.Create(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	inj := NewInjector(coinSeed(t, true))
	sc := NewSimCAS(mem, inj)
	inj.FailNext(OpCASAdvance, FaultAmbiguous)
	var amb *s3wal.AmbiguousError
	if _, err := sc.Advance(ctx, "k", []byte("v2"), etag); !errors.As(err, &amb) {
		t.Fatalf("applied-arm Advance = %v, want AmbiguousError", err)
	}
	obj, err := mem.Get(ctx, "k")
	if err != nil || string(obj.Body) != "v2" {
		t.Fatalf("write did not land: (%q, %v)", obj.Body, err)
	}
}

// TestSimCASAmbiguousDroppedNeverExecutes pins the dropped arm: nothing
// lands, same caller-visible ambiguity.
func TestSimCASAmbiguousDroppedNeverExecutes(t *testing.T) {
	ctx := context.Background()
	mem := s3wal.NewMemCAS()
	etag, err := mem.Create(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	inj := NewInjector(coinSeed(t, false))
	sc := NewSimCAS(mem, inj)
	inj.FailNext(OpCASAdvance, FaultAmbiguous)
	var amb *s3wal.AmbiguousError
	if _, err := sc.Advance(ctx, "k", []byte("v2"), etag); !errors.As(err, &amb) {
		t.Fatalf("dropped-arm Advance = %v, want AmbiguousError", err)
	}
	obj, err := mem.Get(ctx, "k")
	if err != nil || string(obj.Body) != "v1" {
		t.Fatalf("dropped write landed anyway: (%q, %v)", obj.Body, err)
	}
}

// TestSimCASAmbiguousRejectionPassesThrough pins the Codex finding on PR
// #159: a definitive store rejection (412 lost-race / already-exists)
// under an ambiguous-applied injection must surface AS the rejection —
// the store's verdict is real even when a response might have been lost
// (MemCAS evaluates preconditions before fault injection for the same
// reason), and the client-sees-error-nothing-landed class is already the
// dropped arm's job. Wrapping it in AmbiguousError would route the
// driver's lost-race retry path into ambiguity recovery and skew the
// advertised fault matrix.
func TestSimCASAmbiguousRejectionPassesThrough(t *testing.T) {
	ctx := context.Background()
	mem := s3wal.NewMemCAS()
	etag, err := mem.Create(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Advance(ctx, "k", []byte("v2"), etag); err != nil {
		t.Fatal(err) // supersede: etag is now stale
	}
	inj := NewInjector(coinSeed(t, true))
	sc := NewSimCAS(mem, inj)

	inj.FailNext(OpCASAdvance, FaultAmbiguous)
	if _, err := sc.Advance(ctx, "k", []byte("v3"), etag); !errors.Is(err, s3wal.ErrLostRace) {
		t.Fatalf("stale-etag Advance under ambiguous-applied = %v, want ErrLostRace", err)
	}

	inj2 := NewInjector(coinSeed(t, true))
	sc2 := NewSimCAS(mem, inj2)
	inj2.FailNext(OpCASCreate, FaultAmbiguous)
	if _, err := sc2.Create(ctx, "k", []byte("v3")); !errors.Is(err, s3wal.ErrExists) {
		t.Fatalf("duplicate Create under ambiguous-applied = %v, want ErrExists", err)
	}
	if obj, err := mem.Get(ctx, "k"); err != nil || string(obj.Body) != "v2" {
		t.Fatalf("rejected writes mutated the store: (%q, %v)", obj.Body, err)
	}
}
