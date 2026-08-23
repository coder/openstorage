// Pack-quota ledger conformance tests (issues #29/#31): admission,
// reservation lifecycle, and retained-byte accounting must behave
// identically on both backends. Test bodies fan out via runRefBackends.
package ref

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ledgerTTL is generous so nothing expires mid-test unless a test says so.
const ledgerTTL = time.Hour

// ledgerHelpers builds the closure kit the ledger tests share.
type ledgerKit struct {
	t     *testing.T
	s     Store
	group []string
}

func newLedgerKit(t *testing.T, s Store) *ledgerKit {
	base := t.Name()
	return &ledgerKit{t: t, s: s, group: []string{base, base + "+ephemeral", base + "+import"}}
}

func (k *ledgerKit) base() string { return k.group[0] }

func (k *ledgerKit) reserve(packs []Pack, owner string, floor, quota int64) ReserveDecision {
	k.t.Helper()
	dec, err := k.s.ReservePackBytes(context.Background(), k.group, packs, owner, floor, quota, ledgerTTL)
	if err != nil {
		k.t.Fatalf("ReservePackBytes: %v", err)
	}
	return dec
}

func (k *ledgerKit) register(repo string, packs ...Pack) {
	k.t.Helper()
	if err := k.s.RegisterPacks(context.Background(), repo, packs); err != nil {
		k.t.Fatalf("RegisterPacks: %v", err)
	}
}

func (k *ledgerKit) state() PackQuotaLedgerState {
	k.t.Helper()
	st, err := k.s.QuotaLedgerState(context.Background(), k.group, ledgerTTL)
	if err != nil {
		k.t.Fatalf("QuotaLedgerState: %v", err)
	}
	return st
}

func (k *ledgerKit) wantState(usage, reserved, retained int64) {
	k.t.Helper()
	if st := k.state(); st.UsageBytes != usage || st.ReservedBytes != reserved || st.RetainedBytes != retained {
		k.t.Fatalf("ledger state = %+v, want usage=%d reserved=%d retained=%d", st, usage, reserved, retained)
	}
}

// pack mints a deterministic test pack keyed under repo.
func (k *ledgerKit) pack(repo, id string, size int64) Pack {
	return Pack{ID: id, Key: "packs/" + repo + "/pack-" + id + ".pack", Size: size}
}

// TestReserveOvershootDenied is the issue-#29 exploit: two pushes that
// individually fit but together exceed the quota, both admitting against
// the same stale listing — the second reservation must lose.
func TestReserveOvershootDenied(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		const quota = 100
		a := k.pack(k.base(), "aa1", 60)
		b := k.pack(k.base(), "bb2", 60)

		// Both pushes listed usage before either uploaded: floor 0 for both.
		if dec := k.reserve([]Pack{a}, "owner-a", 0, quota); !dec.OK {
			t.Fatalf("first reservation denied: %+v", dec)
		}
		dec := k.reserve([]Pack{b}, "owner-b", 0, quota)
		if dec.OK {
			t.Fatalf("second reservation admitted: 60+60 > 100 must lose, got %+v", dec)
		}
		if dec.UsedBytes != 0 || dec.ReservedBytes != 60 {
			t.Fatalf("denial arithmetic = %+v, want used=0 reserved=60", dec)
		}

		// A registers: bytes move reservation -> counter atomically. B's
		// retry must still lose against the counter even with a stale floor.
		k.register(k.base(), a)
		k.wantState(60, 0, 0)
		if dec := k.reserve([]Pack{b}, "owner-b", 0, quota); dec.OK {
			t.Fatalf("retry admitted against counter: %+v", dec)
		}
		// A smaller push that genuinely fits is admitted.
		if dec := k.reserve([]Pack{k.pack(k.base(), "cc3", 40)}, "owner-c", 0, quota); !dec.OK {
			t.Fatalf("fitting reservation denied: %+v", dec)
		}
	})
}

// TestReserveConcurrentRace drives the same exploit with truly concurrent
// reservations: exactly one may win.
func TestReserveConcurrentRace(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		const quota = 100
		packs := []Pack{k.pack(k.base(), "ra1", 60), k.pack(k.base(), "rb2", 60)}
		decs := make([]ReserveDecision, len(packs))
		var wg sync.WaitGroup
		for i := range packs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dec, err := s.ReservePackBytes(context.Background(), k.group,
					[]Pack{packs[i]}, fmt.Sprintf("owner-%d", i), 0, quota, ledgerTTL)
				if err != nil {
					t.Errorf("ReservePackBytes: %v", err)
					return
				}
				decs[i] = dec
			}()
		}
		wg.Wait()
		wins := 0
		for _, d := range decs {
			if d.OK {
				wins++
			}
		}
		if wins != 1 {
			t.Fatalf("concurrent 60+60 vs quota 100: %d wins, want exactly 1 (%+v)", wins, decs)
		}
	})
}

// TestRegisterConsumesReservation: registration credits the counter and
// deletes the reservation in one transaction; idempotent retries credit
// nothing extra.
func TestRegisterConsumesReservation(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		a := k.pack(k.base(), "aa1", 30)
		if dec := k.reserve([]Pack{a}, "owner-a", 0, 1000); !dec.OK {
			t.Fatalf("reserve: %+v", dec)
		}
		k.wantState(0, 30, 0)
		k.register(k.base(), a)
		k.wantState(30, 0, 0)
		k.register(k.base(), a) // idempotent retry
		k.wantState(30, 0, 0)
	})
}

// TestReserveSameKeyRefreshes: a crash-window retry (or concurrent
// identical push) re-reserving the same key counts its bytes once and never
// wedges. The row is settle-or-TTL (Codex round 22): NO failure-release
// API exists, because the row is shared by every same-key writer via
// refresh — whichever writer's failure released it would free headroom the
// others still stage under, and a third distinct write could consume that
// headroom before their registration lands, overshooting the quota.
func TestReserveSameKeyRefreshes(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		a := k.pack(k.base(), "aa1", 60)
		decA := k.reserve([]Pack{a}, "owner-old", 0, 100)
		if !decA.OK {
			t.Fatalf("first reserve: %+v", decA)
		}
		// Same key, new owner, same stale floor: must be admitted (the
		// existing row is excluded from the reserved sum) and refresh the
		// row rather than double-count. The refresh preserves the row's
		// gen (Codex round 33), so both writers hold the SAME token.
		decB := k.reserve([]Pack{a}, "owner-new", 0, 100)
		if !decB.OK {
			t.Fatalf("same-key re-reserve wedged: %+v", decB)
		}
		if decA.Gens[a.Key].Gen == "" || decA.Gens[a.Key].Gen != decB.Gens[a.Key].Gen {
			t.Fatalf("refresh changed the row gen: %q vs %q", decA.Gens[a.Key].Gen, decB.Gens[a.Key].Gen)
		}
		k.wantState(0, 60, 0)

		// Per-attempt refcounts (Codex round 30, refining round 22's
		// settle-or-TTL): writer A's clean failure retires only ITS
		// attempt — the shared row keeps gating while B still stages
		// behind it, so a third distinct write that would fit only in the
		// freed headroom stays denied.
		if err := s.RetirePackAttempts(context.Background(), k.group, decA.Gens); err != nil {
			t.Fatalf("first retire: %v", err)
		}
		c := k.pack(k.base(), "cc3", 50)
		if dec := k.reserve([]Pack{c}, "owner-third", 0, 100); dec.OK {
			t.Fatalf("third writer admitted into headroom a shared reservation still gates: %+v", dec)
		}
		k.wantState(0, 60, 0)

		// B's clean failure retires the LAST attempt: the row ends and
		// the headroom frees without waiting out the TTL.
		if err := s.RetirePackAttempts(context.Background(), k.group, decB.Gens); err != nil {
			t.Fatalf("second retire: %v", err)
		}
		k.wantState(0, 0, 0)
		decC := k.reserve([]Pack{c}, "owner-third", 0, 100)
		if !decC.OK {
			t.Fatalf("post-retire reserve wedged: %+v", decC)
		}
		k.wantState(0, 50, 0)

		// Settlement still consumes the row by KEY, whoever registers.
		k.register(k.base(), c)
		k.wantState(50, 0, 0)

		// Retiring an absent key is a no-op, not an error.
		if err := s.RetirePackAttempts(context.Background(), k.group, map[string]ReservationToken{"packs/absent/pack-x.pack": {Gen: "g", CreatedAt: time.Now()}}); err != nil {
			t.Fatalf("absent-key retire: %v", err)
		}
	})
}

// TestRetireIsGenerationBound (Codex round 33): a stale writer whose row
// expired and was pruned must not retire a REPLACEMENT row another writer
// minted for the same key — retirement presents the row-incarnation token
// its own reserve returned, and a fresh INSERT mints a new one.
func TestRetireIsGenerationBound(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		a := k.pack(k.base(), "aa1", 60)
		decOld := k.reserve([]Pack{a}, "owner-old", 0, 100)
		if !decOld.OK {
			t.Fatalf("old reserve: %+v", decOld)
		}
		// The old writer's row expires and is pruned (the test/recovery
		// hatch expires everything), then a NEW writer re-reserves the
		// same key: a fresh row with a fresh gen.
		if _, err := s.SweepExpiredPackReservations(ctx, 0); err != nil {
			t.Fatalf("expire: %v", err)
		}
		decNew := k.reserve([]Pack{a}, "owner-new", 0, 100)
		if !decNew.OK {
			t.Fatalf("new reserve: %+v", decNew)
		}
		if decNew.Gens[a.Key].Gen == decOld.Gens[a.Key].Gen {
			t.Fatalf("replacement row kept the old gen %q", decOld.Gens[a.Key].Gen)
		}
		// The old writer's late clean failure retires with ITS token: the
		// new writer's only reservation must survive.
		if err := s.RetirePackAttempts(ctx, k.group, decOld.Gens); err != nil {
			t.Fatalf("stale retire: %v", err)
		}
		k.wantState(0, 60, 0)
		// The new writer's own retire (its token) still works.
		if err := s.RetirePackAttempts(ctx, k.group, decNew.Gens); err != nil {
			t.Fatalf("new retire: %v", err)
		}
		k.wantState(0, 0, 0)
	})
}

// TestRetireRequiresUnrefreshedRow (Codex round 33, mixed-fleet half): the
// DELETE additionally requires created_at unchanged since the retiring
// writer's own reserve — a row refreshed AFTER that reserve (by any
// binary, including a pre-attempt one that bumps created_at without
// incrementing attempts) is retained for the TTL even at zero attempts.
func TestRetireRequiresUnrefreshedRow(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		a := k.pack(k.base(), "aa1", 60)
		decA := k.reserve([]Pack{a}, "owner-a", 0, 100)
		if !decA.OK {
			t.Fatalf("reserve A: %+v", decA)
		}
		time.Sleep(20 * time.Millisecond) // ensure a later refresh timestamp
		decB := k.reserve([]Pack{a}, "owner-b", 0, 100)
		if !decB.OK {
			t.Fatalf("reserve B: %+v", decB)
		}
		// B retires first: attempts 2→1, no delete.
		if err := s.RetirePackAttempts(ctx, k.group, decB.Gens); err != nil {
			t.Fatalf("retire B: %v", err)
		}
		// A retires with ITS token: attempts 1→0, but the row was
		// refreshed after A's reserve — the delete must not fire.
		if err := s.RetirePackAttempts(ctx, k.group, decA.Gens); err != nil {
			t.Fatalf("retire A: %v", err)
		}
		k.wantState(0, 60, 0) // row retained; the TTL is its only end now
	})
}

// TestReservePrunesExpiredPhysicalRows (Codex round 5 on #51): admission
// itself prunes the base repo's EXPIRED rows — with sweeping disabled, a
// writer cycling TTL windows must not grow the table without bound.
// Physical row counts are read directly (repo-scoped, so exact even on the
// shared PG test store).
func TestReservePrunesExpiredPhysicalRows(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		countRows := func() int {
			t.Helper()
			var n int
			switch st := s.(type) {
			case *PG:
				if err := st.pool.QueryRow(ctx,
					`SELECT COUNT(*) FROM pack_reservations WHERE repo = $1`, k.base()).Scan(&n); err != nil {
					t.Fatal(err)
				}
			case *SQLite:
				if err := st.db.QueryRowContext(ctx,
					`SELECT COUNT(*) FROM pack_reservations WHERE repo = ?`, k.base()).Scan(&n); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unknown store %T", s)
			}
			return n
		}

		ttl := 100 * time.Millisecond
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{k.pack(k.base(), "aa1", 10)}, "o1", 0, 1000, ttl); err != nil || !dec.OK {
			t.Fatalf("reserve: %+v, %v", dec, err)
		}
		if got := countRows(); got != 1 {
			t.Fatalf("rows after reserve = %d, want 1", got)
		}
		time.Sleep(2 * ttl)
		// The next admission prunes the expired row in its own transaction.
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{k.pack(k.base(), "bb2", 10)}, "o2", 0, 1000, ttl); err != nil || !dec.OK {
			t.Fatalf("reserve after expiry: %+v, %v", dec, err)
		}
		if got := countRows(); got != 1 {
			t.Fatalf("rows after expiry-pruning reserve = %d, want 1 (fresh row only)", got)
		}
	})
}

// TestReservationExpiryAndSweep: leaked reservations (crash between reserve
// and register) stop gating admission after the TTL and are pruned by the
// sweep — the reserve → crash → sweep recovery path.
func TestReservationExpiryAndSweep(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		const quota = 100
		leak := k.pack(k.base(), "aa1", 60)
		ttl := time.Second // generous: the pre-expiry check below must run within it
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{leak}, "crashed", 0, quota, ttl); err != nil || !dec.OK {
			t.Fatalf("reserve: %+v, %v", dec, err)
		}
		// While live, the leak gates admission.
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{k.pack(k.base(), "bb2", 60)}, "live", 0, quota, ttl); err != nil || dec.OK {
			t.Fatalf("reserve during leak = %+v, %v; want denial", dec, err)
		}
		time.Sleep(2 * ttl)
		// Expired: ignored by admission even before any sweep runs.
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{k.pack(k.base(), "cc3", 60)}, "live", 0, quota, ttl); err != nil || !dec.OK {
			t.Fatalf("reserve after expiry = %+v, %v; want admission", dec, err)
		}
		// The TTL-scoped sweep is now a second line of defense: admission
		// itself prunes the caller repo's expired rows (issue #51 round
		// 5), so this sweep may find nothing left — assert only that it
		// runs and that the fresh cc3 row survives.
		if _, err := s.SweepExpiredPackReservations(ctx, ttl); err != nil {
			t.Fatalf("SweepExpiredPackReservations: %v", err)
		}
		k.wantState(0, 60, 0) // cc3 survives a TTL-scoped sweep
		// olderThan=0 expires everything, cc3 included.
		if n, err := s.SweepExpiredPackReservations(ctx, 0); err != nil || n < 1 {
			t.Fatalf("sweep all = %d, %v; want >= 1", n, err)
		}
		k.wantState(0, 0, 0)
	})
}

// TestRetainedBytesGateAdmission is issue #31's drip-write amplification
// bound: a fold's superseded bytes keep counting against the quota until
// reclaimed, and reclaiming restores exactly that headroom.
func TestRetainedBytesGateAdmission(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		const quota = 60
		a := k.pack(k.base(), "aa1", 10)
		b := k.pack(k.base(), "bb2", 20)
		k.register(k.base(), a, b)
		k.wantState(30, 0, 0)

		// Fold: the union pack adds 25; a and b linger as retained.
		fold := k.pack(k.base(), "f01", 25)
		won, err := s.ReplacePacks(ctx, k.base(), []Pack{a, b}, fold)
		if err != nil || !won {
			t.Fatalf("ReplacePacks = %v, %v", won, err)
		}
		k.wantState(55, 0, 30)
		for key, want := range map[string]bool{a.Key: true, b.Key: true, fold.Key: false} {
			got, err := s.IsRetainedPack(ctx, k.base(), key)
			if err != nil || got != want {
				t.Fatalf("IsRetainedPack(%s) = %v, %v; want %v", key, got, err, want)
			}
		}

		// The retained bytes gate admission: 55 + 10 > 60.
		if dec := k.reserve([]Pack{k.pack(k.base(), "cc3", 10)}, "o", 0, quota); dec.OK {
			t.Fatalf("reservation ignored retained bytes: %+v", dec)
		}
		// Reclaiming a's blob (the sweeper's safeDelete moment) frees its
		// headroom; the fitting push is admitted again.
		if got, err := s.ReclaimRetainedPack(ctx, k.base(), a.Key); err != nil || got != 10 {
			t.Fatalf("ReclaimRetainedPack = %d, %v; want 10", got, err)
		}
		k.wantState(45, 0, 20)
		if got, err := s.ReclaimRetainedPack(ctx, k.base(), a.Key); err != nil || got != 0 {
			t.Fatalf("double reclaim = %d, %v; want 0", got, err)
		}
		if dec := k.reserve([]Pack{k.pack(k.base(), "cc3", 10)}, "o", 0, quota); !dec.OK {
			t.Fatalf("reservation after reclaim denied: %+v", dec)
		}
	})
}

// TestRetainedReregistration: re-registering a superseded key (identical
// content re-pushed while the blob still lingers) consumes the retained row
// without double-counting, and an identity fold leaves the counter alone.
func TestRetainedReregistration(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		a := k.pack(k.base(), "aa1", 10)
		b := k.pack(k.base(), "bb2", 20)
		k.register(k.base(), a, b)
		fold := k.pack(k.base(), "f01", 25)
		if won, err := s.ReplacePacks(ctx, k.base(), []Pack{a, b}, fold); err != nil || !won {
			t.Fatalf("ReplacePacks = %v, %v", won, err)
		}
		k.wantState(55, 0, 30)

		// Zero-growth re-registration of superseded a: retained row
		// consumed, counter unchanged (its bytes never left).
		k.register(k.base(), a)
		k.wantState(55, 0, 20)

		// Identity fold: new pack IS one of the inputs — no new bytes, so
		// the counter must not move; the other input becomes retained.
		if won, err := s.ReplacePacks(ctx, k.base(), []Pack{a, fold}, a); err != nil || !won {
			t.Fatalf("identity ReplacePacks = %v, %v", won, err)
		}
		k.wantState(55, 0, 45) // b(20) + fold(25) retained; a live
	})
}

// TestReserveGroupRollup: namespace usage rolls up under the base repo, so
// one token cannot multiply its quota across +ephemeral/+import.
func TestReserveGroupRollup(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		eph := k.group[1]
		a := k.pack(eph, "aa1", 70)
		k.register(eph, a)
		k.wantState(70, 0, 0)
		if dec := k.reserve([]Pack{k.pack(k.base(), "bb2", 40)}, "o", 0, 100); dec.OK {
			t.Fatalf("namespace usage not rolled up: %+v", dec)
		}
	})
}

// TestReserveUsageFloor: the caller's blob-listing floor is charged when it
// exceeds the counter (legacy repos, orphaned uploads).
func TestReserveUsageFloor(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		if dec := k.reserve([]Pack{k.pack(k.base(), "aa1", 20)}, "o", 90, 100); dec.OK {
			t.Fatalf("floor ignored: %+v", dec)
		}
		if dec := k.reserve([]Pack{k.pack(k.base(), "aa1", 20)}, "o", 70, 100); !dec.OK {
			t.Fatalf("fitting floor denied: %+v", dec)
		}
	})
}
