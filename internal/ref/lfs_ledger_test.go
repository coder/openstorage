// RegisterLFSObject / HasLFSObject ledger tests (issue #51), fanned out
// over both backends via runRefBackends like ledger_test.go.
package ref

import (
	"context"
	"testing"
)

// lfsKey mints a test LFS blob key under the kit's base repo.
func lfsLedgerKey(k *ledgerKit, oid string) string {
	return "lfs/" + k.base() + "/" + oid
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
