// Overflow safety for attacker-declared sizes (Codex round 2 on #51): LFS
// batch sizes are client-supplied int64s, so admission sums must saturate
// instead of wrapping negative and admitting anything.
package ref

import (
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
