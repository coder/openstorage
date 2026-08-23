// Hot-path benchmark for quota-admission ledger reservation (issue #92):
// ReservePackBytes runs in every push/write admission — one IMMEDIATE
// transaction on the SQLite lane — and its snapshot read sums the base
// repo's active in-flight reservations (excluding the incoming keys) on
// every call, so the matrix scales that reservation count. The timed
// iteration re-reserves the SAME key, exercising the documented crash-retry
// upsert, which keeps the table stationary at n+1 rows across b.N. The PG
// twin shares the SQL shape but its latency is dominated by the network
// round-trips a local benchmark cannot represent, so only the in-process
// backend is benched.
package ref

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkReservePackBytesSQLite(b *testing.B) {
	ctx := context.Background()
	group := []string{"benchrepo", "benchrepo+ephemeral", "benchrepo+import"}
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("reservations=%d", n), func(b *testing.B) {
			s, err := OpenSQLite(ctx, "sqlite://"+filepath.Join(b.TempDir(), "refs.sqlite"))
			if err != nil {
				b.Fatalf("OpenSQLite: %v", err)
			}
			b.Cleanup(s.Close)
			if err := s.InitSchema(ctx); err != nil {
				b.Fatalf("InitSchema: %v", err)
			}
			// Seed n active in-flight reservations (other pushes' keys, all
			// inside the TTL window, so the admission snapshot sums them).
			seeded := make([]Pack, n)
			for i := range seeded {
				seeded[i] = Pack{Key: fmt.Sprintf("packs/benchrepo/pack-%04d.pack", i), Size: 1 << 20}
			}
			if dec, err := s.ReservePackBytes(ctx, group, seeded, NewReservationOwner(), 0, 1<<40, time.Hour); err != nil || !dec.OK {
				b.Fatalf("seed ReservePackBytes = (%+v, %v), want admitted", dec, err)
			}
			owner := NewReservationOwner()
			incoming := []Pack{{Key: "packs/benchrepo/pack-incoming.pack", Size: 1 << 20}}
			b.ReportAllocs()
			for b.Loop() {
				dec, err := s.ReservePackBytes(ctx, group, incoming, owner, 1<<30, 1<<40, time.Hour)
				if err != nil {
					b.Fatalf("ReservePackBytes: %v", err)
				}
				if !dec.OK || dec.ReservedBytes != int64(n)<<20 {
					b.Fatalf("ReservePackBytes = %+v, want admitted with %d reserved bytes", dec, int64(n)<<20)
				}
			}
		})
	}
}
