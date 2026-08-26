// Unit suite for the Phase 3 sync-claim write-through seam
// (s3walstore_syncclaim.go, plan §2.8): owner-guarded set/clear
// semantics, the purge-tombstone no-op, CASRef fence integration, and the
// ambiguous-PUT recovery predicates — over the MemCAS fault fake, no
// Docker, using the shared recovery-suite helpers.
package ref

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// TestS3WALSyncClaimSetClear covers the seam's guarded state machine:
// install, same-holder idempotence, supersede, owner-guarded clear, and
// the fence wired through CASRef.
func TestS3WALSyncClaimSetClear(t *testing.T) {
	ctx := context.Background()
	const base = "r-syncclaim"
	const branch = "refs/heads/main"
	sha1, sha2 := testSHA(0x11), testSHA(0x22)
	_, a, _ := newRecoveryStores(t)

	if err := a.SetSyncClaim(ctx, base, "h1"); err != nil {
		t.Fatalf("SetSyncClaim: %v", err)
	}
	m := mustManifest(t, a, base)
	if m.SyncClaim == nil || m.SyncClaim.Holder != "h1" {
		t.Fatalf("claim = %+v, want h1", m.SyncClaim)
	}

	// Idempotent re-set writes nothing (seq unchanged).
	seq := m.Seq
	if err := a.SetSyncClaim(ctx, base, "h1"); err != nil {
		t.Fatalf("re-set: %v", err)
	}
	if got := mustManifest(t, a, base).Seq; got != seq {
		t.Fatalf("seq %d -> %d: an idempotent re-set must write nothing", seq, got)
	}

	// The fence admits the holder and rejects everyone else.
	if won, err := a.CASRef(WithSyncClaimFence(ctx, base, "h1"), base, branch, ZeroSHA, sha1); err != nil || !won {
		t.Fatalf("fenced create = (%v, %v), want win", won, err)
	}
	if won, err := a.CASRef(WithSyncClaimFence(ctx, base, "h2"), base, branch, sha1, sha2); err != nil || won {
		t.Fatalf("foreign fenced CAS = (%v, %v), want clean loss", won, err)
	}

	// A steal supersedes the token; the old holder's clear is a no-op.
	if err := a.SetSyncClaim(ctx, base, "h2"); err != nil {
		t.Fatalf("supersede: %v", err)
	}
	if err := a.ClearSyncClaim(ctx, base, "h1"); err != nil {
		t.Fatalf("owner-guarded clear: %v", err)
	}
	if got := mustManifest(t, a, base).SyncClaim; got == nil || got.Holder != "h2" {
		t.Fatalf("claim after foreign clear = %+v, want h2 intact", got)
	}

	// The owner's clear removes it; the fence fails closed again.
	if err := a.ClearSyncClaim(ctx, base, "h2"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if got := mustManifest(t, a, base).SyncClaim; got != nil {
		t.Fatalf("claim after clear = %+v, want none", got)
	}
	if won, err := a.CASRef(WithSyncClaimFence(ctx, base, "h2"), base, branch, sha1, sha2); err != nil || won {
		t.Fatalf("fenced CAS after clear = (%v, %v), want clean loss", won, err)
	}

	// Invalid-UTF-8 holders: typed rejection on set, clean no-op on clear.
	if err := a.SetSyncClaim(ctx, base, "h\xff"); !errors.Is(err, ErrRefNameNotUTF8) {
		t.Fatalf("SetSyncClaim(bad holder) err = %v, want ErrRefNameNotUTF8", err)
	}
	if err := a.ClearSyncClaim(ctx, base, "h\xff"); err != nil {
		t.Fatalf("ClearSyncClaim(bad holder) err = %v, want clean no-op", err)
	}
}

// TestS3WALSyncClaimPurged: purge is terminal — the seam neither installs
// nor clears tokens on a tombstone, and writes nothing.
func TestS3WALSyncClaimPurged(t *testing.T) {
	ctx := context.Background()
	const base = "r-syncclaim-purged"
	_, a, _ := newRecoveryStores(t)
	if err := a.FenceRepoPurge(ctx, base); err != nil {
		t.Fatalf("FenceRepoPurge: %v", err)
	}
	seq := mustManifest(t, a, base).Seq
	if err := a.SetSyncClaim(ctx, base, "h1"); err != nil {
		t.Fatalf("SetSyncClaim on tombstone: %v", err)
	}
	if err := a.ClearSyncClaim(ctx, base, "h1"); err != nil {
		t.Fatalf("ClearSyncClaim on tombstone: %v", err)
	}
	m := mustManifest(t, a, base)
	if m.SyncClaim != nil || m.Seq != seq {
		t.Fatalf("tombstone mutated: claim %+v seq %d -> %d, want untouched", m.SyncClaim, seq, m.Seq)
	}
}

// TestS3WALSyncClaimRecovery exercises the ambiguous-PUT predicates: a
// holder token is unique per claim, so its presence proves a landed set
// (applied) and its absence re-runs; the clear predicate is the semantic
// effect ("holder's token gone"), satisfied even when a competitor
// superseded the token.
func TestS3WALSyncClaimRecovery(t *testing.T) {
	ctx := context.Background()
	const base = "r-syncclaim-recovery"

	for _, tc := range []struct {
		name  string
		fault s3wal.PutFault
	}{
		{"set applied", s3wal.FaultAmbiguousApplied},
		{"set dropped", s3wal.FaultAmbiguousDropped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc, a, b := newRecoveryStores(t)
			// Seed so the faulted PUT is an Advance with a competitor on top.
			if err := a.SetSyncClaim(ctx, base, "h-seed"); err != nil {
				t.Fatalf("seed: %v", err)
			}
			armPutFault(mc, tc.fault)
			armAfterPut(mc, func() {
				// Competing writer between the faulted PUT and the
				// recovery read: the writer nonce can no longer answer.
				recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
					m.EnsureMember(base).Gen++
				})
			})
			if err := a.SetSyncClaim(ctx, base, "h-new"); err != nil {
				t.Fatalf("SetSyncClaim through ambiguity: %v", err)
			}
			if got := mustManifest(t, a, base).SyncClaim; got == nil || got.Holder != "h-new" {
				t.Fatalf("claim = %+v, want h-new", got)
			}
		})
	}

	t.Run("clear superseded by competitor", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		if err := a.SetSyncClaim(ctx, base, "h-old"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		// Our clear's PUT is dropped; the competitor installs ITS token
		// before our recovery read. The semantic effect — h-old's token
		// gone — holds, so the clear reports success without touching the
		// competitor's token.
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		armAfterPut(mc, func() {
			if err := b.SetSyncClaim(ctx, base, "h-thief"); err != nil {
				t.Errorf("competitor set: %v", err)
			}
		})
		if err := a.ClearSyncClaim(ctx, base, "h-old"); err != nil {
			t.Fatalf("ClearSyncClaim through ambiguity: %v", err)
		}
		if got := mustManifest(t, a, base).SyncClaim; got == nil || got.Holder != "h-thief" {
			t.Fatalf("claim = %+v, want the competitor's token intact", got)
		}
	})
}
