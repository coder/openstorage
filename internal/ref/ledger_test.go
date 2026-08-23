// Pack-quota ledger conformance tests (issues #29/#31): admission,
// reservation lifecycle, and retained-byte accounting must behave
// identically on both backends. Test bodies fan out via runRefBackends.
package ref

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
		if decA.Gens[a.Key] == "" || decA.Gens[a.Key] != decB.Gens[a.Key] {
			t.Fatalf("refresh changed the row gen: %q vs %q", decA.Gens[a.Key], decB.Gens[a.Key])
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
		if err := s.RetirePackAttempts(context.Background(), k.group, map[string]string{"packs/absent/pack-x.pack": "g"}); err != nil {
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
		if decNew.Gens[a.Key] == decOld.Gens[a.Key] {
			t.Fatalf("replacement row kept the old gen %q", decOld.Gens[a.Key])
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

// legacyRelease simulates a pre-round-22 (v0.1.0) binary's owner-scoped
// ReleasePackReservations: DELETE ... WHERE owner = ? AND key = ?.
func legacyRelease(t *testing.T, s Store, owner, key string) {
	t.Helper()
	ctx := context.Background()
	switch st := s.(type) {
	case *PG:
		if _, err := st.pool.Exec(ctx,
			`DELETE FROM pack_reservations WHERE owner = $1 AND key = $2`, owner, key); err != nil {
			t.Fatalf("legacy release: %v", err)
		}
	case *SQLite:
		if _, err := st.db.ExecContext(ctx,
			`DELETE FROM pack_reservations WHERE owner = ? AND key = ?`, owner, key); err != nil {
			t.Fatalf("legacy release: %v", err)
		}
	default:
		t.Fatalf("legacyRelease: unknown store %T", s)
	}
}

// legacyReserve simulates a v0.1.0 binary's ReservePackBytes upsert
// VERBATIM (git show v0.1.0:internal/ref/ledger*.go): raw blob-key row,
// owner captured, bytes CLOBBERED (no GREATEST), created_at refreshed,
// attempts/gen untouched — the statement that captures any row it shares
// a key with.
func legacyReserve(t *testing.T, s Store, repo, key string, bytes int64, owner string) {
	t.Helper()
	ctx := context.Background()
	switch st := s.(type) {
	case *PG:
		if _, err := st.pool.Exec(ctx, `
INSERT INTO pack_reservations (key, repo, bytes, owner) VALUES ($1, $2, $3, $4)
ON CONFLICT (key) DO UPDATE SET repo = EXCLUDED.repo, bytes = EXCLUDED.bytes,
                                owner = EXCLUDED.owner, created_at = now()`,
			key, repo, bytes, owner); err != nil {
			t.Fatalf("legacy reserve: %v", err)
		}
	case *SQLite:
		if _, err := st.db.ExecContext(ctx, `
INSERT INTO pack_reservations (key, repo, bytes, owner, created_at)
VALUES (?, ?, ?, ?, `+sqliteEpochMS+`)
ON CONFLICT (key) DO UPDATE SET repo = excluded.repo, bytes = excluded.bytes,
                                owner = excluded.owner, created_at = excluded.created_at`,
			key, repo, bytes, owner); err != nil {
			t.Fatalf("legacy reserve: %v", err)
		}
	default:
		t.Fatalf("legacyReserve: unknown store %T", s)
	}
}

// TestLegacyStatementsCannotEndTargetReservations (Codex rounds 34+35, P1):
// during the v0.1.0 rolling upgrade both binaries write pack_reservations,
// and the legacy statements address rows by RAW blob key. The legacy
// upsert CAPTURES any row it reaches (owner rotated to the legacy token,
// bytes clobbered — v0.1.0 has no GREATEST), and the legacy owner-scoped
// ReleasePackReservations then deletes it on a clean failure, freeing
// headroom the target writer still stages under. Round 34's owner
// rotation fenced only the legacy-FIRST ordering; with the legacy refresh
// last, the capture wins regardless of any column value this binary
// writes (round 35). The fence is therefore the row SPELLING
// (reservRowKey): no legacy plain-key statement can reach a target-era
// row in EITHER order, while both eras still meet in the repo-scoped
// admission SUMs (a cross-era same-key twin double-counts conservatively
// until the legacy row settles, releases, or expires).
func TestLegacyStatementsCannotEndTargetReservations(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)

		// REVERSE ordering (round 35): the target writer reserves first...
		a := k.pack(k.base(), "aa1", 60)
		if dec := k.reserve([]Pack{a}, "new-owner", 0, 1000); !dec.OK {
			t.Fatalf("target reserve: %+v", dec)
		}
		// ...then a legacy replica reserves the same key. Its upsert must
		// MISS the target row — a capture would rotate the owner and
		// clobber bytes 60→1. Both rows now sum (the documented
		// conservative cross-era double-count).
		legacyReserve(t, s, k.base(), a.Key, 1, "legacy-owner")
		k.wantState(0, 61, 0)
		// The legacy writer's clean failure fires its owner-scoped
		// release: it must delete ONLY the legacy row. Pre-fence this
		// read 0 — capture-then-release ended the reservation protecting
		// the target writer mid-publication.
		legacyRelease(t, s, "legacy-owner", a.Key)
		k.wantState(0, 60, 0)

		// FORWARD ordering (round 34): the legacy row exists first, the
		// target reserve lands beside it, and the legacy release again
		// takes only its own row.
		b := k.pack(k.base(), "bb2", 40)
		legacyReserve(t, s, k.base(), b.Key, 40, "legacy-owner")
		if dec := k.reserve([]Pack{b}, "new-owner", 0, 1000); !dec.OK {
			t.Fatalf("target reserve beside legacy row: %+v", dec)
		}
		k.wantState(0, 140, 0)
		legacyRelease(t, s, "legacy-owner", b.Key)
		k.wantState(0, 100, 0)

		// Rows only legacy writers touch keep full legacy semantics.
		c := k.pack(k.base(), "cc3", 20)
		legacyReserve(t, s, k.base(), c.Key, 20, "legacy-owner")
		k.wantState(0, 120, 0)
		legacyRelease(t, s, "legacy-owner", c.Key)
		k.wantState(0, 100, 0)
	})
}

// TestSettlementConsumesBothEraRows (Codex round 35): settlement credits
// the key's bytes to the usage counter, so ANY reservation row for that
// key — this binary's era row AND a legacy twin — becomes a stale
// double-count the settling transaction must consume (v0.1.0's own settle
// consumes by raw key the same way). Covers both settlement surfaces.
func TestSettlementConsumesBothEraRows(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)

		// RegisterPacks.
		a := k.pack(k.base(), "aa1", 60)
		if dec := k.reserve([]Pack{a}, "new-owner", 0, 1000); !dec.OK {
			t.Fatalf("reserve: %+v", dec)
		}
		legacyReserve(t, s, k.base(), a.Key, 60, "legacy-owner")
		k.wantState(0, 120, 0)
		k.register(k.base(), a)
		k.wantState(60, 0, 0)

		// RegisterLFSObject.
		lfsKey := "lfs/" + k.base() + "/" + strings.Repeat("ab", 32)
		if dec := k.reserve([]Pack{{Key: lfsKey, Size: 30}}, "new-owner", 0, 1000); !dec.OK {
			t.Fatalf("lfs reserve: %+v", dec)
		}
		legacyReserve(t, s, k.base(), lfsKey, 30, "legacy-owner")
		k.wantState(60, 60, 0)
		if err := s.RegisterLFSObject(ctx, k.base(), lfsKey, 30); err != nil {
			t.Fatalf("RegisterLFSObject: %v", err)
		}
		k.wantState(90, 0, 0)
	})
}

// TestRetireLeavesLegacyTwinRow (Codex round 35): retirement is a
// clean-FAILURE path — nothing was credited to usage — so it must end
// only this binary's era row. The legacy twin may still be protecting a
// live legacy writer's staging; only its own release, its own settle, or
// the TTL may end it. A cap-denied RegisterLFSObject (Codex round 37)
// deletes NO reservation rows at all — a same-key peer's attempt may
// still be publishing under the shared era row — and each cap-denied
// caller retires its own attempt with its gen token instead.
func TestRetireLeavesLegacyTwinRow(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		a := k.pack(k.base(), "aa1", 60)
		dec := k.reserve([]Pack{a}, "new-owner", 0, 1000)
		if !dec.OK {
			t.Fatalf("reserve: %+v", dec)
		}
		legacyReserve(t, s, k.base(), a.Key, 60, "legacy-owner")
		k.wantState(0, 120, 0)
		if err := s.RetirePackAttempts(ctx, k.group, dec.Gens); err != nil {
			t.Fatalf("retire: %v", err)
		}
		k.wantState(0, 60, 0) // the legacy twin row alone remains

		// Cap denial: consume the cap with one registered object, then
		// cap-deny a reserved second key that also has a legacy twin.
		old := MaxLFSObjectsPerRepo
		MaxLFSObjectsPerRepo = 1
		t.Cleanup(func() { MaxLFSObjectsPerRepo = old })
		if err := s.RegisterLFSObject(ctx, k.base(),
			"lfs/"+k.base()+"/"+strings.Repeat("aa", 32), 10); err != nil {
			t.Fatal(err)
		}
		lfsKey := "lfs/" + k.base() + "/" + strings.Repeat("bb", 32)
		decLFS := k.reserve([]Pack{{Key: lfsKey, Size: 30}}, "new-owner", 0, 1000)
		if !decLFS.OK {
			t.Fatalf("lfs reserve: %+v", decLFS)
		}
		legacyReserve(t, s, k.base(), lfsKey, 30, "legacy-owner")
		if err := s.RegisterLFSObject(ctx, k.base(), lfsKey, 30); !errors.Is(err, ErrLFSObjectCapExceeded) {
			t.Fatalf("cap-exceeded register err = %v", err)
		}
		// The denial deleted NOTHING: 10 registered + 60 legacy pack twin
		// + 30 era row + 30 legacy LFS twin (a same-key peer may still be
		// publishing under the shared era row).
		k.wantState(10, 120, 0)
		// The cap-denied caller retires its own attempt (evidence-based,
		// as on every clean failure): the era row ends at zero attempts,
		// the legacy twin stays.
		if err := s.RetirePackAttempts(ctx, k.group, decLFS.Gens); err != nil {
			t.Fatalf("cap-denied retire: %v", err)
		}
		k.wantState(10, 90, 0)
	})
}

// TestReverseOrderRetirementEndsRow (Codex round 35, security): with the
// era spelling fencing legacy statements out entirely, every refresh of an
// era row increments attempts — the refcount alone proves a zero-attempt
// row has no live writer, in EITHER retirement order. Round 33's extra
// created_at guard (needed while a pre-attempt binary could bump
// created_at without incrementing attempts) retained reverse-order
// zero-attempt rows to the TTL: ~10,000 failed tiny same-key upload pairs
// could wedge the row cap — and with it ALL of the repository's git, REST,
// and LFS growth — for the full default hour. The row must end when its
// LAST attempt retires, regardless of order.
func TestReverseOrderRetirementEndsRow(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		a := k.pack(k.base(), "aa1", 60)
		decA := k.reserve([]Pack{a}, "owner-a", 0, 100)
		if !decA.OK {
			t.Fatalf("reserve A: %+v", decA)
		}
		time.Sleep(20 * time.Millisecond) // B's refresh gets a later timestamp
		decB := k.reserve([]Pack{a}, "owner-b", 0, 100)
		if !decB.OK {
			t.Fatalf("reserve B: %+v", decB)
		}
		// B retires first: attempts 2→1, the row keeps gating for A.
		if err := s.RetirePackAttempts(ctx, k.group, decB.Gens); err != nil {
			t.Fatalf("retire B: %v", err)
		}
		k.wantState(0, 60, 0)
		// A retires LAST with the earlier token: attempts 1→0 — no live
		// writer remains, so the row must end now, not at the TTL.
		if err := s.RetirePackAttempts(ctx, k.group, decA.Gens); err != nil {
			t.Fatalf("retire A: %v", err)
		}
		k.wantState(0, 0, 0)
	})
}

// TestReserveCountsPostScanSettlements (Codex round 44, security): the
// floor is a listing SNAPSHOT — bytes registered after its scan live in
// the usage counter but not in the floor, so combining them with max()
// undercounts whenever uncounted physical bytes (pre-registry LFS,
// orphans) coexist with fresh settlements. The floor parameter is
// therefore the UNCOUNTED-bytes floor (listing minus the counter at scan,
// clamped at zero) and admission SUMS it with the live counter: with 80
// uncounted bytes and a 100-byte quota, a 20-byte upload that settles
// must leave the next 20-byte upload DENIED (counter 20 + floor 80 +
// incoming 20 = 120 > 100), even though the floor snapshot predates the
// settlement.
func TestReserveCountsPostScanSettlements(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		const quota, uncounted = 100, 80

		// Upload A: 20 bytes against 80 uncounted bytes — fits exactly.
		a := "lfs/" + k.base() + "/" + strings.Repeat("aa", 32)
		if dec := k.reserve([]Pack{{Key: a, Size: 20}}, "owner-a", uncounted, quota); !dec.OK {
			t.Fatalf("first reserve: %+v", dec)
		}
		if err := s.RegisterLFSObject(ctx, k.base(), a, 20); err != nil {
			t.Fatal(err)
		}
		// Upload B admits against the SAME floor snapshot (its scan
		// predates A's settlement — the cache window). The counter now
		// carries A's 20 bytes the floor cannot see: B must be denied.
		b := "lfs/" + k.base() + "/" + strings.Repeat("bb", 32)
		if dec := k.reserve([]Pack{{Key: b, Size: 20}}, "owner-b", uncounted, quota); dec.OK {
			t.Fatalf("post-settlement reserve admitted past quota: %+v (counter and floor must SUM)", dec)
		}
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
		gen0, err := s.LFSAdoptionGen(ctx, k.base())
		if err != nil {
			t.Fatal(err)
		}
		if got, err := s.ReclaimRetainedPack(ctx, k.base(), a.Key); err != nil || got != 10 {
			t.Fatalf("ReclaimRetainedPack = %d, %v; want 10", got, err)
		}
		// The debit shrinks the counter AFTER floors snapshotted it
		// (Codex round 59, security): the reclaim must bump the shared
		// floor generation atomically, or a floor scanned between the
		// aged blob's deletion and this debit keeps hiding legacy bytes.
		gen1, err := s.LFSAdoptionGen(ctx, k.base())
		if err != nil {
			t.Fatal(err)
		}
		if gen1 == gen0 {
			t.Fatal("ReclaimRetainedPack did not bump the shared floor generation with its debit")
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
