// Overflow safety for attacker-declared sizes (Codex round 2 on #51): LFS
// batch sizes are client-supplied int64s, so admission sums must saturate
// instead of wrapping negative and admitting anything.
package ref

import (
	"context"
	"math"
	"testing"
)

func TestReserveOverflowingSizesDenied(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		a := Pack{Key: "lfs/" + k.base() + "/" + oid64("aa"), Size: math.MaxInt64}
		b := Pack{Key: "lfs/" + k.base() + "/" + oid64("bb"), Size: math.MaxInt64}
		// Two MaxInt64 objects: the naive sum wraps to -2 and would admit.
		if dec := k.reserve([]Pack{a, b}, "owner-a", 0, 1000); dec.OK {
			t.Fatalf("overflowing batch admitted: %+v", dec)
		}
		k.wantState(0, 0, 0) // denied: no rows inserted

		// A single huge object against a huge quota must also deny when it
		// cannot fit on top of existing usage without wrapping.
		k.register(k.base(), k.pack(k.base(), "p1", 1000))
		if dec := k.reserve([]Pack{a}, "owner-b", 0, math.MaxInt64); dec.OK {
			t.Fatalf("usage+MaxInt64 wrapped past a MaxInt64 quota: %+v", dec)
		}
	})
}

// oid64 mints a 64-hex oid from a 2-char seed.
func oid64(seed string) string {
	out := ""
	for range 32 {
		out += seed
	}
	return out
}

// TestReserveActiveRowCapDenied (Codex security round on #51): declared
// bytes bound reservation ROW count at nothing, so the ledger caps active
// rows per base repo independently — tiny-size batches must stop minting
// durable rows at the cap, denials must report KeyCapExceeded, and expiry
// must reopen admission.
func TestReserveActiveRowCapDenied(t *testing.T) {
	old := maxActiveReservations
	maxActiveReservations = 3
	t.Cleanup(func() { maxActiveReservations = old })
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		batch := []Pack{
			{Key: "lfs/" + k.base() + "/" + oid64("1a"), Size: 1},
			{Key: "lfs/" + k.base() + "/" + oid64("2b"), Size: 1},
			{Key: "lfs/" + k.base() + "/" + oid64("3c"), Size: 1},
		}
		if dec := k.reserve(batch, "owner-a", 0, 1000); !dec.OK {
			t.Fatalf("cap-sized batch denied: %+v", dec)
		}
		// One more row would exceed the cap of 3.
		dec := k.reserve([]Pack{{Key: "lfs/" + k.base() + "/" + oid64("4d"), Size: 1}}, "owner-b", 0, 1000)
		if dec.OK || !dec.KeyCapExceeded {
			t.Fatalf("over-cap reservation = %+v, want denied with KeyCapExceeded", dec)
		}
		// Re-reserving the SAME keys refreshes rows rather than adding —
		// never capped by its own live rows (crash retries must not wedge).
		if dec := k.reserve(batch, "owner-a", 0, 1000); !dec.OK {
			t.Fatalf("same-key refresh denied by row cap: %+v", dec)
		}
		// Consuming a reservation (registration) frees a slot.
		if err := s.RegisterLFSObject(context.Background(), k.base(), batch[0].Key, 1); err != nil {
			t.Fatal(err)
		}
		if dec := k.reserve([]Pack{{Key: "lfs/" + k.base() + "/" + oid64("4d"), Size: 1}}, "owner-b", 0, 1000); !dec.OK {
			t.Fatalf("post-consume reservation denied: %+v", dec)
		}
	})
}
