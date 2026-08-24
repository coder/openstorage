// RegisterLFSObject / HasLFSObject ledger tests (issue #51), fanned out
// over both backends via runRefBackends like ledger_test.go.
package ref

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// lfsKey mints a test LFS blob key under the kit's base repo.
func lfsLedgerKey(k *ledgerKit, oid string) string {
	return "lfs/" + k.base() + "/" + oid
}

// TestQuotaLedgerState_ExcludesAdoptionClaims (Codex round 102): round 95
// excluded claim leases from ReservePackBytes' admission sums, but
// QuotaLedgerState still counted them — at exact-headroom boundaries the
// one-byte lease flipped advisory verdicts (a batch 507s with no upload
// action and never reaches the authoritative PUT admission) for the
// claim's whole lifetime. Claims are leases, not storage.
func TestQuotaLedgerState_ExcludesAdoptionClaims(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		ctx := context.Background()
		key := "lfs/" + k.base() + "/o2/" + strings.Repeat("cd", 32)
		claimed, _, err := s.TryAdoptionClaim(ctx, k.base(), key)
		if err != nil || !claimed {
			t.Fatalf("claim = (%v, %v)", claimed, err)
		}
		ls, err := s.QuotaLedgerState(ctx, []string{k.base()}, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if ls.ReservedBytes != 0 {
			t.Fatalf("ReservedBytes with only a claim lease = %d, want 0 — claims are leases, not storage", ls.ReservedBytes)
		}
	})
}

// TestRegisterLFSObject_SettlesReservationAndCredits: registration consumes
// the admission reservation and credits the usage counter atomically, and a
// retry is a pure no-op (bytes counted once).
func TestRegisterLFSObject_SettlesReservationAndCredits(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		key := lfsLedgerKey(k, "aaaa")
		if dec := k.reserve([]Pack{{Key: key, Size: 40}}, "owner-a", 0, 100); !dec.OK {
			t.Fatalf("reservation denied: %+v", dec)
		}
		k.wantState(0, 40, 0)

		if err := s.RegisterLFSObject(context.Background(), k.base(), key, 40); err != nil {
			t.Fatalf("RegisterLFSObject: %v", err)
		}
		k.wantState(40, 0, 0)
		if has, err := s.HasLFSObject(context.Background(), k.base(), key); err != nil || !has {
			t.Fatalf("HasLFSObject = %v, %v; want true", has, err)
		}

		// Idempotent retry: no double credit, still no reservation.
		if err := s.RegisterLFSObject(context.Background(), k.base(), key, 40); err != nil {
			t.Fatalf("RegisterLFSObject retry: %v", err)
		}
		k.wantState(40, 0, 0)
	})
}

// TestRegisterLFSObject_WithoutReservation: adoption of a pre-#51 object
// (no reservation ever existed) credits usage and registers the row.
func TestRegisterLFSObject_WithoutReservation(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		key := lfsLedgerKey(k, "bbbb")
		if err := s.RegisterLFSObject(context.Background(), k.base(), key, 25); err != nil {
			t.Fatalf("RegisterLFSObject: %v", err)
		}
		k.wantState(25, 0, 0)
		if has, err := s.HasLFSObject(context.Background(), k.base(), key); err != nil || !has {
			t.Fatalf("HasLFSObject = %v, %v; want true", has, err)
		}
	})
}

// TestRegisterLFSObject_ZeroBytes: the empty object registers (the sweeper
// must classify it live) without touching the usage counter.
func TestRegisterLFSObject_ZeroBytes(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		key := lfsLedgerKey(k, "cccc")
		if err := s.RegisterLFSObject(context.Background(), k.base(), key, 0); err != nil {
			t.Fatalf("RegisterLFSObject: %v", err)
		}
		k.wantState(0, 0, 0)
		if has, err := s.HasLFSObject(context.Background(), k.base(), key); err != nil || !has {
			t.Fatalf("HasLFSObject = %v, %v; want true", has, err)
		}
	})
}

// TestReserveSameKeyRefreshNeverShrinks (Codex round 7): declared sizes
// are client input, so a same-key refresh takes the MAX of the byte values
// — a one-byte refresh must not free headroom a concurrent larger writer
// is still staging under; a larger refresh must still grow the charge.
func TestReserveSameKeyRefreshNeverShrinks(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		key := lfsLedgerKey(k, "eeee")
		if dec := k.reserve([]Pack{{Key: key, Size: 60}}, "owner-a", 0, 1000); !dec.OK {
			t.Fatalf("reserve 60: %+v", dec)
		}
		k.wantState(0, 60, 0)
		// Shrinking refresh: row stays at 60.
		if dec := k.reserve([]Pack{{Key: key, Size: 1}}, "owner-b", 0, 1000); !dec.OK {
			t.Fatalf("refresh 1: %+v", dec)
		}
		k.wantState(0, 60, 0)
		// Growing refresh: row grows to 80.
		if dec := k.reserve([]Pack{{Key: key, Size: 80}}, "owner-c", 0, 1000); !dec.OK {
			t.Fatalf("refresh 80: %+v", dec)
		}
		k.wantState(0, 80, 0)
	})
}

// TestRegisterLFSObject_AtomicCap (Codex round 6): the object-count cap is
// enforced inside the registration transaction itself — a denial commits
// nothing (no row, no credit, reservation untouched), idempotent
// re-registration at cap stays allowed, and the typed sentinel surfaces.
func TestRegisterLFSObject_AtomicCap(t *testing.T) {
	old := MaxLFSObjectsPerRepo
	MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { MaxLFSObjectsPerRepo = old })
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		first, second := lfsLedgerKey(k, "1111"), lfsLedgerKey(k, "2222")
		if err := s.RegisterLFSObject(ctx, k.base(), first, 10); err != nil {
			t.Fatalf("first register: %v", err)
		}
		// Over cap: typed denial, nothing committed.
		dec := k.reserve([]Pack{{Key: second, Size: 5}}, "owner-b", 0, 1000)
		if !dec.OK {
			t.Fatalf("reserve: %+v", dec)
		}
		if err := s.RegisterLFSObject(ctx, k.base(), second, 5); !errors.Is(err, ErrLFSObjectCapExceeded) {
			t.Fatalf("over-cap register error = %v, want ErrLFSObjectCapExceeded", err)
		}
		if has, err := s.HasLFSObject(ctx, k.base(), second); err != nil || has {
			t.Fatalf("denied register left a row: %v, %v", has, err)
		}
		// Credit untouched; the reservation SURVIVES the denial (Codex
		// round 37, reversing round 13's key-wide retire — a same-key
		// peer's attempt may still be publishing under the shared row).
		// The cap-denied caller then retires its OWN attempt with its gen
		// token, so a permanent denial still does not gate unrelated
		// admissions for the TTL.
		k.wantState(10, 5, 0)
		if err := s.RetirePackAttempts(ctx, k.group, dec.Gens); err != nil {
			t.Fatalf("cap-denied retire: %v", err)
		}
		k.wantState(10, 0, 0)
		if n, err := s.CountLFSObjects(ctx, k.base()); err != nil || n != 1 {
			t.Fatalf("CountLFSObjects = %d, %v; want 1", n, err)
		}
		// Idempotent re-registration of the existing object at cap: fine.
		if err := s.RegisterLFSObject(ctx, k.base(), first, 10); err != nil {
			t.Fatalf("idempotent register at cap: %v", err)
		}
	})
}

// TestGuardedLFSDelete (Codex round 18): the mismatch delete runs only
// while the key is provably unregistered, serialized against
// registrations; a registered key aborts without running del, and a del
// failure propagates without claiming deletion.
func TestGuardedLFSDelete(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := lfsLedgerKey(k, "ffff")

		ran := false
		deleted, err := s.GuardedLFSDelete(ctx, k.base(), key, func(context.Context) error {
			ran = true
			return nil
		})
		if err != nil || !deleted || !ran {
			t.Fatalf("unregistered delete = (%v, %v), ran=%v; want (true, nil, true)", deleted, err, ran)
		}

		// Registered key: del must never run.
		if err := s.RegisterLFSObject(ctx, k.base(), key, 5); err != nil {
			t.Fatal(err)
		}
		ran = false
		deleted, err = s.GuardedLFSDelete(ctx, k.base(), key, func(context.Context) error {
			ran = true
			return nil
		})
		if err != nil || deleted || ran {
			t.Fatalf("registered delete = (%v, %v), ran=%v; want (false, nil, false)", deleted, err, ran)
		}

		// del failure propagates, deleted stays false.
		other := lfsLedgerKey(k, "abab")
		deleted, err = s.GuardedLFSDelete(ctx, k.base(), other, func(context.Context) error {
			return fmt.Errorf("backend wedged")
		})
		if err == nil || deleted {
			t.Fatalf("failing delete = (%v, %v); want error and false", deleted, err)
		}
	})
}

// TestHasLFSObject_UnregisteredAndForeignRepo: point reads answer false for
// unknown keys and for another repo's registration.
func TestHasLFSObject_UnregisteredAndForeignRepo(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		k := newLedgerKit(t, s)
		key := lfsLedgerKey(k, "dddd")
		if has, err := s.HasLFSObject(context.Background(), k.base(), key); err != nil || has {
			t.Fatalf("HasLFSObject(unregistered) = %v, %v; want false", has, err)
		}
		if err := s.RegisterLFSObject(context.Background(), k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if has, err := s.HasLFSObject(context.Background(), "other-repo", key); err != nil || has {
			t.Fatalf("HasLFSObject(foreign repo) = %v, %v; want false", has, err)
		}
	})
}

// TestRegisterAdoptedLFSObjectBumpsGenAtomically (Codex round 51): the
// adoption generation must move in the SAME transaction as an adopted
// registration — a separate best-effort bump could be lost after the
// registration committed, leaving remote floors double-counting forever
// (adoption short-circuits once the row exists). Idempotent
// re-registration commits nothing and must not bump again; plain
// (new-upload) registrations never bump.
func TestRegisterAdoptedLFSObjectBumpsGenAtomically(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := lfsLedgerKey(k, "adop")

		if gen, err := s.LFSAdoptionGen(ctx, k.base()); err != nil || gen != 0 {
			t.Fatalf("initial gen = (%d, %v), want (0, nil)", gen, err)
		}
		if err := s.RegisterAdoptedLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if gen, err := s.LFSAdoptionGen(ctx, k.base()); err != nil || gen != 1 {
			t.Fatalf("post-adoption gen = (%d, %v), want (1, nil)", gen, err)
		}
		k.wantState(10, 0, 0)
		// Idempotent re-adoption: no new row, no bump, no double credit.
		if err := s.RegisterAdoptedLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if gen, err := s.LFSAdoptionGen(ctx, k.base()); err != nil || gen != 1 {
			t.Fatalf("re-adoption gen = (%d, %v), want (1, nil)", gen, err)
		}
		k.wantState(10, 0, 0)
		// A verified NEW upload's registration never bumps.
		if err := s.RegisterLFSObject(ctx, k.base(), lfsLedgerKey(k, "new1"), 5); err != nil {
			t.Fatal(err)
		}
		if gen, err := s.LFSAdoptionGen(ctx, k.base()); err != nil || gen != 1 {
			t.Fatalf("new-upload gen = (%d, %v), want (1, nil)", gen, err)
		}
	})
}

// TestRegisterPacksAdoptedBumpsGen (Codex round 53): registering a pack
// whose BLOB pre-existed (a crash-window retry — the pack analog of LFS
// adoption) moves listed bytes into the counter, so it must bump the
// shared floor generation atomically with the credit. Fresh packs never
// bump (per-push bumps would defeat the floor cache), and idempotent
// re-registration commits nothing and bumps nothing.
func TestRegisterPacksAdoptedBumpsGen(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		k := newLedgerKit(t, s)

		fresh := k.pack(k.base(), "aa1", 10)
		k.register(k.base(), fresh)
		if gen, err := s.LFSAdoptionGen(ctx, k.base()); err != nil || gen != 0 {
			t.Fatalf("fresh-pack gen = (%d, %v), want (0, nil)", gen, err)
		}

		adopted := k.pack(k.base(), "bb2", 20)
		adopted.Adopted = true
		k.register(k.base(), adopted)
		if gen, err := s.LFSAdoptionGen(ctx, k.base()); err != nil || gen != 1 {
			t.Fatalf("adopted-pack gen = (%d, %v), want (1, nil)", gen, err)
		}
		k.wantState(30, 0, 0)

		// Idempotent re-registration: no new row, no bump.
		k.register(k.base(), adopted)
		if gen, err := s.LFSAdoptionGen(ctx, k.base()); err != nil || gen != 1 {
			t.Fatalf("re-registration gen = (%d, %v), want (1, nil)", gen, err)
		}
		k.wantState(30, 0, 0)
	})
}
