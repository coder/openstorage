// S3WAL recovery-predicate unit suite (issue #133, plan §2.3 + Appendix
// A): every ambiguous-PUT recovery class the driver implements — CAS,
// PRED, OPS, LEASE — plus the fences (purge, sync-claim) and the §2.2/§2.4
// protocol guards (size caps, skew demotion, seq monotonicity), exercised
// over the MemCAS fault fake with NO Docker. Competing writers are a
// second S3WAL store sharing the same MemCAS, interleaved between a
// faulted PUT and the driver's recovery read via the AfterPut hook.
package ref

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// newRecoveryStores returns two independent S3WAL drivers sharing one
// MemCAS — the store under test and the competing writer — with fast
// lost-race backoff.
func newRecoveryStores(t *testing.T) (*s3wal.MemCAS, *S3WAL, *S3WAL) {
	t.Helper()
	mc := s3wal.NewMemCAS()
	a := newS3WALStore(mc, nil, nil)
	b := newS3WALStore(mc, nil, nil)
	for _, s := range []*S3WAL{a, b} {
		s.backoffBase = time.Microsecond
		s.backoffCap = time.Millisecond
	}
	return mc, a, b
}

// recoveryEdit applies one direct manifest mutation through s — the test
// stand-in for an arbitrary competing writer (including the Phase 3
// sync-claim write-through that Phase 1b only reads).
func recoveryEdit(t *testing.T, s *S3WAL, base string, edit func(m *s3wal.Manifest, clk time.Time)) {
	t.Helper()
	err := s.mutate(context.Background(), base, s3walMutation{
		op: "recovery test edit",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			edit(m, clk)
			return true, nil
		},
	})
	if err != nil {
		t.Fatalf("recovery test edit: %v", err)
	}
}

// armPutFault directs the NEXT conditional PUT (ours) to fail with f;
// every later PUT — the competitor's included — runs clean.
func armPutFault(mc *s3wal.MemCAS, f s3wal.PutFault) {
	armed := true
	mc.Hooks.PutFault = func(op, key string) s3wal.PutFault {
		if armed {
			armed = false
			return f
		}
		return s3wal.FaultNone
	}
}

// armAfterPut runs fn once, after the next conditional PUT attempt
// returns — the competing-writer interleaving point. fn's own PUTs do not
// re-trigger it (disarmed before the call).
func armAfterPut(mc *s3wal.MemCAS, fn func()) {
	armed := true
	mc.Hooks.AfterPut = func(op, key string) {
		if !armed {
			return
		}
		armed = false
		fn()
	}
}

// armStaleGet serves the key's previous version on the next Get — the
// deterministic lost-race (412) trigger: the store under test reads a
// stale snapshot, so its conditional PUT carries a superseded ETag.
func armStaleGet(mc *s3wal.MemCAS) {
	armed := true
	mc.Hooks.StaleGet = func(key string) bool {
		if armed {
			armed = false
			return true
		}
		return false
	}
}

// fakeClock is the store's now-injection knob (§2.4 tests).
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{t: t} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// mustManifest reads base's manifest for state assertions.
func mustManifest(t *testing.T, s *S3WAL, base string) *s3wal.Manifest {
	t.Helper()
	m, err := s.viewManifest(context.Background(), base)
	if err != nil {
		t.Fatalf("viewManifest(%s): %v", base, err)
	}
	return m
}

// wantAmbiguous asserts err carries the *AmbiguousError the fail-closed
// verdict must surface (retryable — never silently dropped, never a blind
// re-apply).
func wantAmbiguous(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("want fail-closed ambiguous error, got nil")
	}
	var amb *s3wal.AmbiguousError
	if !errors.As(err, &amb) {
		t.Fatalf("fail-closed error must surface the ambiguity, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// CAS class (Appendix A: CASRef won/lost protocol).

func TestS3WALRecoveryCASRef(t *testing.T) {
	ctx := context.Background()
	const branch = "refs/heads/main"
	sha1, sha2, sha3 := testSHA(0x11), testSHA(0x22), testSHA(0x33)

	seed := func(t *testing.T) (*s3wal.MemCAS, *S3WAL, *S3WAL, string) {
		t.Helper()
		mc, a, b := newRecoveryStores(t)
		base := "r-cas"
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("seed create = (%v, %v)", won, err)
		}
		return mc, a, b, base
	}

	t.Run("ambiguous applied, nonce match = win", func(t *testing.T) {
		mc, a, _, base := seed(t)
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		won, err := a.CASRef(ctx, base, branch, sha1, sha2)
		if err != nil || !won {
			t.Fatalf("CASRef = (%v, %v), want recovered win", won, err)
		}
		m := mustManifest(t, a, base)
		if got := m.Members[base].Refs[branch].SHA; got != sha2 {
			t.Fatalf("ref = %s, want %s", got, sha2)
		}
		if m.Seq != 2 {
			t.Fatalf("seq = %d, want 2 (exactly one write for the swap)", m.Seq)
		}
		if g := m.Members[base].Gen; g != 2 {
			t.Fatalf("gen = %d, want 2 (exactly one bump for the recovered win)", g)
		}
	})

	t.Run("ambiguous applied then superseded = fail closed", func(t *testing.T) {
		mc, a, b, base := seed(t)
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		armAfterPut(mc, func() {
			// The competitor supersedes our (landed) write before the
			// recovery read: precondition broken, last writer foreign.
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
				m.Members[base].Refs[branch] = s3wal.RefRecord{SHA: sha3, CreatedAt: clk}
			})
		})
		won, err := a.CASRef(ctx, base, branch, sha1, sha2)
		// Exactly the lost-commit-response shape on SQL: the driver cannot
		// distinguish "landed then superseded" from "never landed", so it
		// surfaces the ambiguity (the IdempotentCAS decorator's point-read
		// recheck — current == new — is the recovery seam).
		wantAmbiguous(t, err)
		if won {
			t.Fatal("ambiguous CASRef must not report won")
		}
		if got := mustManifest(t, a, base).Members[base].Refs[branch].SHA; got != sha3 {
			t.Fatalf("ref = %s, want the competitor's %s", got, sha3)
		}
	})

	t.Run("lost race, ref moved = clean won=false", func(t *testing.T) {
		mc, a, b, base := seed(t)
		recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
			m.Members[base].Refs[branch] = s3wal.RefRecord{SHA: sha3, CreatedAt: clk}
		})
		// The stale read hands the driver a superseded ETag: its PUT is a
		// REAL 412 (definitively not executed), and the fresh re-read
		// no longer matches old — a clean loss, never retried, no error.
		armStaleGet(mc)
		won, err := a.CASRef(ctx, base, branch, sha1, sha2)
		if err != nil || won {
			t.Fatalf("CASRef = (%v, %v), want clean (false, nil)", won, err)
		}
		if got := mustManifest(t, a, base).Members[base].Refs[branch].SHA; got != sha3 {
			t.Fatalf("ref = %s, want untouched %s", got, sha3)
		}
	})

	t.Run("lost race, still matching = retried win", func(t *testing.T) {
		mc, a, b, base := seed(t)
		recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
			// Unrelated write: advances the manifest without touching our ref.
			m.Members[base].Refs["refs/heads/other"] = s3wal.RefRecord{SHA: sha3, CreatedAt: clk}
		})
		armStaleGet(mc)
		won, err := a.CASRef(ctx, base, branch, sha1, sha2)
		if err != nil || !won {
			t.Fatalf("CASRef = (%v, %v), want retried win", won, err)
		}
		if got := mustManifest(t, a, base).Members[base].Refs[branch].SHA; got != sha2 {
			t.Fatalf("ref = %s, want %s", got, sha2)
		}
	})
}

// ---------------------------------------------------------------------------
// PRED class (semantic-effect predicate on re-read).

func TestS3WALRecoveryRegisterPacks(t *testing.T) {
	ctx := context.Background()
	const base = "r-pred-reg"
	pack := Pack{ID: "p1", Key: "k1", Size: 10}

	for _, tc := range []struct {
		name  string
		fault s3wal.PutFault
	}{
		// Applied: the rerun sees its own row and reservation state —
		// changed=false, zero growth, no second write.
		{"ambiguous applied rerun is zero growth", s3wal.FaultAmbiguousApplied},
		// Dropped: the rerun re-applies on the fresh snapshot — the SQL
		// upsert contract (ON CONFLICT DO NOTHING twin).
		{"ambiguous dropped rerun re-applies", s3wal.FaultAmbiguousDropped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc, a, b := newRecoveryStores(t)
			armPutFault(mc, tc.fault)
			armAfterPut(mc, func() {
				recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {}) // foreign nonce
			})
			if err := a.RegisterPacks(ctx, base, []Pack{pack}); err != nil {
				t.Fatalf("RegisterPacks: %v", err)
			}
			m := mustManifest(t, a, base)
			mem := m.Members[base]
			if len(mem.Packs) != 1 || mem.Packs[0].ID != "p1" {
				t.Fatalf("packs = %+v, want exactly one p1 row", mem.Packs)
			}
			if m.UsageBytes != 10 {
				t.Fatalf("usage = %d, want 10 (credited exactly once)", m.UsageBytes)
			}
			if mem.Gen != 1 {
				t.Fatalf("gen = %d, want 1 (bumped exactly once)", mem.Gen)
			}
		})
	}
}

func TestS3WALRecoveryReplacePacks(t *testing.T) {
	ctx := context.Background()
	const base = "r-pred-fold"
	p1 := Pack{ID: "p1", Key: "k1", Size: 10}
	p2 := Pack{ID: "p2", Key: "k2", Size: 20}
	fold := Pack{ID: "f1", Key: "kf", Size: 25}

	seed := func(t *testing.T) (*s3wal.MemCAS, *S3WAL, *S3WAL) {
		t.Helper()
		mc, a, b := newRecoveryStores(t)
		if err := a.RegisterPacks(ctx, base, []Pack{p1, p2}); err != nil {
			t.Fatal(err)
		}
		return mc, a, b
	}
	wantFolded := func(t *testing.T, a *S3WAL) {
		t.Helper()
		m := mustManifest(t, a, base)
		mem := m.Members[base]
		if len(mem.Packs) != 1 || mem.Packs[0].ID != "f1" {
			t.Fatalf("packs = %+v, want exactly the fold", mem.Packs)
		}
		if mem.Retained["k1"].Bytes != 10 || mem.Retained["k2"].Bytes != 20 || len(mem.Retained) != 2 {
			t.Fatalf("retained = %+v, want k1:10 k2:20", mem.Retained)
		}
		if m.UsageBytes != 55 {
			t.Fatalf("usage = %d, want 55 (30 retained-pending + 25 fold, credited once)", m.UsageBytes)
		}
	}

	t.Run("ambiguous applied, fold present = recovered won", func(t *testing.T) {
		mc, a, b := seed(t)
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		armAfterPut(mc, func() {
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {}) // foreign nonce
		})
		won, err := a.ReplacePacks(ctx, base, []Pack{p1, p2}, fold)
		if err != nil || !won {
			t.Fatalf("ReplacePacks = (%v, %v), want recovered won", won, err)
		}
		wantFolded(t, a)
	})

	t.Run("ambiguous dropped, old intact = rerun wins", func(t *testing.T) {
		mc, a, b := seed(t)
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		armAfterPut(mc, func() {
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {}) // foreign nonce
		})
		won, err := a.ReplacePacks(ctx, base, []Pack{p1, p2}, fold)
		if err != nil || !won {
			t.Fatalf("ReplacePacks = (%v, %v), want rerun win", won, err)
		}
		wantFolded(t, a)
	})

	t.Run("inconclusive = fail closed", func(t *testing.T) {
		mc, a, b := seed(t)
		other := Pack{ID: "g1", Key: "kg", Size: 15}
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		armAfterPut(mc, func() {
			// A competing fold consumes the old set: our fold is absent
			// AND the old rows are gone — neither applied nor provably
			// unapplied. A false loss would make the repack caller delete
			// a possibly-registered fold blob, so this MUST fail closed.
			if won, err := b.ReplacePacks(ctx, base, []Pack{p1, p2}, other); err != nil || !won {
				t.Fatalf("competing fold = (%v, %v)", won, err)
			}
		})
		won, err := a.ReplacePacks(ctx, base, []Pack{p1, p2}, fold)
		wantAmbiguous(t, err)
		if won {
			t.Fatal("inconclusive fold must not report won")
		}
		mem := mustManifest(t, a, base).Members[base]
		if len(mem.Packs) != 1 || mem.Packs[0].ID != "g1" {
			t.Fatalf("packs = %+v, want the competitor's fold only", mem.Packs)
		}
	})
}

func TestS3WALRecoveryReclaimRetainedPack(t *testing.T) {
	ctx := context.Background()
	const base = "r-pred-reclaim"

	seed := func(t *testing.T) (*s3wal.MemCAS, *S3WAL, *S3WAL) {
		t.Helper()
		mc, a, b := newRecoveryStores(t)
		recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
			mem := m.EnsureMember(base)
			mem.Retained = map[string]s3wal.RetainedPack{"k1": {Bytes: 10, SupersededAt: clk}}
			m.UsageBytes = 10
		})
		return mc, a, b
	}
	wantReclaimed := func(t *testing.T, a *S3WAL) {
		t.Helper()
		m := mustManifest(t, a, base)
		if len(m.Members[base].Retained) != 0 {
			t.Fatalf("retained = %+v, want empty", m.Members[base].Retained)
		}
		if m.UsageBytes != 0 {
			t.Fatalf("usage = %d, want 0 (debited exactly once)", m.UsageBytes)
		}
		if m.LFS.AdoptionGen != 1 {
			t.Fatalf("adoption gen = %d, want 1 (bumped exactly once)", m.LFS.AdoptionGen)
		}
	}

	t.Run("ambiguous applied, nonce match reports bytes", func(t *testing.T) {
		mc, a, _ := seed(t)
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		bytes, err := a.ReclaimRetainedPack(ctx, base, "k1")
		if err != nil || bytes != 10 {
			t.Fatalf("ReclaimRetainedPack = (%d, %v), want (10, nil)", bytes, err)
		}
		wantReclaimed(t, a)
	})

	t.Run("ambiguous applied then superseded reports zero, no double debit", func(t *testing.T) {
		mc, a, b := seed(t)
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		armAfterPut(mc, func() {
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {}) // foreign nonce
		})
		// The rerun finds the row gone and reports 0 — the documented
		// PRED/ID contract: the debit rode the landed write, and a second
		// debit must never happen.
		bytes, err := a.ReclaimRetainedPack(ctx, base, "k1")
		if err != nil || bytes != 0 {
			t.Fatalf("ReclaimRetainedPack = (%d, %v), want degraded (0, nil)", bytes, err)
		}
		wantReclaimed(t, a)
	})
}

func TestS3WALRecoveryTryAdoptionClaim(t *testing.T) {
	ctx := context.Background()
	const base = "r-pred-claim"
	key := "lfs/" + base + "/" + strings.Repeat("ab", 32)

	t.Run("ambiguous applied, owner token = recovered claim", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		armAfterPut(mc, func() {
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {}) // foreign nonce
		})
		claimed, owner, err := a.TryAdoptionClaim(ctx, base, key)
		if err != nil || !claimed || owner == "" {
			t.Fatalf("TryAdoptionClaim = (%v, %q, %v), want recovered claim", claimed, owner, err)
		}
		if got := mustManifest(t, a, base).Reservations[key+AdoptionClaimSuffix]; got == nil || got.Owner != owner {
			t.Fatalf("claim entry = %+v, want owner %q", got, owner)
		}
	})

	t.Run("ambiguous dropped, competitor claimed = clean loss", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		var competitorOwner string
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		armAfterPut(mc, func() {
			claimed, owner, err := b.TryAdoptionClaim(ctx, base, key)
			if err != nil || !claimed {
				t.Errorf("competitor claim = (%v, %v)", claimed, err)
				return
			}
			competitorOwner = owner
		})
		claimed, owner, err := a.TryAdoptionClaim(ctx, base, key)
		if err != nil || claimed || owner != "" {
			t.Fatalf("TryAdoptionClaim = (%v, %q, %v), want clean (false, \"\", nil)", claimed, owner, err)
		}
		if got := mustManifest(t, a, base).Reservations[key+AdoptionClaimSuffix]; got == nil || got.Owner != competitorOwner {
			t.Fatalf("claim entry = %+v, want the competitor's %q", got, competitorOwner)
		}
	})
}

// ---------------------------------------------------------------------------
// OPS class (op-ID membership: ring + per-entry record; fail closed on
// inconclusive — attempt counters are non-idempotent, never blindly
// re-applied).

func TestS3WALRecoveryOps(t *testing.T) {
	ctx := context.Background()
	const base = "r-ops"
	group := []string{base}
	const quota, ttl = int64(1 << 30), time.Hour

	reserve := func(t *testing.T, s *S3WAL, key string, size int64, owner string) ReserveDecision {
		t.Helper()
		dec, err := s.ReservePackBytes(ctx, group, []Pack{{Key: key, Size: size}}, owner, 0, ReserveNoFloorGen, quota, ttl)
		if err != nil || !dec.OK {
			t.Fatalf("reserve %s = (%+v, %v), want admit", key, dec, err)
		}
		return dec
	}

	t.Run("ReservePackBytes ring hit = decision stands", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		armAfterPut(mc, func() {
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {}) // foreign nonce
		})
		dec := reserve(t, a, "kk", 10, "o1")
		if dec.Gens["kk"] == "" {
			t.Fatalf("decision carries no gen token: %+v", dec)
		}
		r := mustManifest(t, a, base).Reservations["kk"]
		if r == nil || r.Owner != "o1" || r.Bytes != 10 {
			t.Fatalf("reservation = %+v, want o1/10", r)
		}
		if r.Attempts != 1 {
			t.Fatalf("attempts = %d, want exactly 1 (the landed increment, never re-applied)", r.Attempts)
		}
	})

	t.Run("ReservePackBytes both miss = fail closed", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		armAfterPut(mc, func() {
			// A foreign OPS write: the ring and entry op-IDs are all the
			// competitor's, so our membership test is a definitive miss on
			// a write that still MIGHT have landed — fail closed.
			reserve(t, b, "other", 1, "ob")
		})
		dec, err := a.ReservePackBytes(ctx, group, []Pack{{Key: "kk", Size: 10}}, "o1", 0, ReserveNoFloorGen, quota, ttl)
		wantAmbiguous(t, err)
		if dec.OK {
			t.Fatalf("fail-closed reserve must not report OK: %+v", dec)
		}
		if r := mustManifest(t, a, base).Reservations["kk"]; r != nil {
			t.Fatalf("dropped write must leave no entry, got %+v", r)
		}
	})

	t.Run("RetirePackAttempts ring hit = single decrement", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		dec := reserve(t, a, "kk", 10, "o1")
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		armAfterPut(mc, func() {
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {}) // foreign nonce
		})
		if err := a.RetirePackAttempts(ctx, group, dec.Gens); err != nil {
			t.Fatalf("RetirePackAttempts: %v", err)
		}
		if r := mustManifest(t, a, base).Reservations["kk"]; r != nil {
			t.Fatalf("attempts 1→0 must delete the row, got %+v", r)
		}
	})

	t.Run("RetirePackAttempts entry op-ID hit after ring eviction", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		reserve(t, a, "kk", 10, "o1")
		dec := reserve(t, a, "kk", 10, "o1") // attempts 2, same gen token
		armPutFault(mc, s3wal.FaultAmbiguousApplied)
		armAfterPut(mc, func() {
			// Force >OpsRingSize foreign ops so OUR op-ID is evicted from
			// the ring; the touched reservation entry still records it, so
			// eviction cannot forge a "not applied" verdict (plan §2.3).
			for range s3wal.OpsRingSize + 1 {
				reserve(t, b, "flood", 1, "flooder")
			}
		})
		if err := a.RetirePackAttempts(ctx, group, dec.Gens); err != nil {
			t.Fatalf("RetirePackAttempts: %v", err)
		}
		m := mustManifest(t, a, base)
		if got := len(m.Ops); got != s3wal.OpsRingSize {
			t.Fatalf("ops ring = %d entries, want the full %d (eviction forced)", got, s3wal.OpsRingSize)
		}
		r := m.Reservations["kk"]
		if r == nil || r.Attempts != 1 {
			t.Fatalf("reservation = %+v, want attempts exactly 1 (single landed decrement)", r)
		}
	})

	t.Run("RetirePackAttempts both miss = fail closed, row intact", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		dec := reserve(t, a, "kk", 10, "o1")
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		armAfterPut(mc, func() {
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {}) // foreign nonce
		})
		err := a.RetirePackAttempts(ctx, group, dec.Gens)
		wantAmbiguous(t, err)
		r := mustManifest(t, a, base).Reservations["kk"]
		if r == nil || r.Attempts != 1 {
			t.Fatalf("reservation = %+v, want attempts 1 untouched (never blindly re-applied)", r)
		}
	})
}

// ---------------------------------------------------------------------------
// LEASE class (§2.5 W6 delete-lease protocol).

func TestS3WALRecoveryDeleteLease(t *testing.T) {
	ctx := context.Background()
	const base = "r-lease"
	key := "lfs/" + base + "/" + strings.Repeat("cd", 32)

	t.Run("registration blocked for the full fencing window, expiry unblocks", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		clock := newFakeClock(t0)
		a.now, b.now = clock.now, clock.now
		mc.Now = clock.now // store clock agrees (§2.4-3 Date witness)

		// A foreign delete lease, injected as if its holder crashed after
		// its last renewal at t0.
		recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
			m.DeleteLocks = map[string]*s3wal.DeleteLock{key: {Owner: "crashed", OpID: "op-x", CreatedAt: clk}}
		})

		// The registration fencing window is TTL + deleteIOTimeout +
		// safety: even a lease that missed its final renewal still fences
		// the worst-case in-flight DELETE.
		window := s3walDeleteLockTTL + s3walDeleteIOTimeout + s3walDeleteLockSafety

		// One tick inside the window: the registration must poll, not
		// proceed — bounded here by a real ctx deadline.
		clock.set(t0.Add(window - time.Millisecond))
		blockedCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		err := a.RegisterLFSObject(blockedCtx, base, key, 5)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("register under live lease = %v, want ctx deadline (blocked)", err)
		}
		if ok, err := a.HasLFSObject(ctx, base, key); err != nil || ok {
			t.Fatalf("HasLFSObject = (%v, %v), want unregistered while fenced", ok, err)
		}

		// At exactly the window boundary the stale lock expires and the
		// registration proceeds immediately (no unconditional sleep).
		clock.set(t0.Add(window))
		if err := a.RegisterLFSObject(ctx, base, key, 5); err != nil {
			t.Fatalf("register past the window: %v", err)
		}
		if ok, err := a.HasLFSObject(ctx, base, key); err != nil || !ok {
			t.Fatalf("HasLFSObject = (%v, %v), want registered", ok, err)
		}
	})

	t.Run("renewal loss in flight refuses the deleted verdict", func(t *testing.T) {
		// Shrink the whole protocol to milliseconds (the vars exist for
		// exactly this) while keeping the TTL >= io+safety+skew invariant:
		// 60 >= 25 + 10 + min(15m, 15ms).
		restore := func(ttl, io, safety, poll time.Duration) func() {
			oldTTL, oldIO, oldSafety, oldPoll := s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll
			s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll = ttl, io, safety, poll
			return func() {
				s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll = oldTTL, oldIO, oldSafety, oldPoll
			}
		}
		t.Cleanup(restore(60*time.Millisecond, 25*time.Millisecond, 10*time.Millisecond, 2*time.Millisecond))

		_, a, b := newRecoveryStores(t)
		deleted, err := a.GuardedLFSDelete(ctx, base, key, func(context.Context) error {
			// The lease is stolen while our DELETE is in flight; the
			// renewal loop (ticker TTL/3 = 20ms) must observe the loss and
			// the driver must refuse deleted=true — the caller may NOT
			// treat the key as deleted while another writer owns the slot.
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
				lock := m.DeleteLocks[key]
				if lock == nil {
					t.Error("no delete lease to steal")
					return
				}
				lock.Owner = "thief"
				lock.CreatedAt = clk
			})
			time.Sleep(150 * time.Millisecond) // several renewal periods
			return nil
		})
		if deleted {
			t.Fatal("lease lost in flight must refuse deleted=true")
		}
		if err == nil || !strings.Contains(err.Error(), "may not be treated as deleted") {
			t.Fatalf("err = %v, want the in-flight lease-loss refusal", err)
		}
	})

	t.Run("renewal loss before dispatch", func(t *testing.T) {
		// Genuinely not expressible over the fake: the pre-dispatch loss
		// check sits microseconds after lease acquisition, before any
		// renewal tick can fire, and the driver exposes no seam between
		// acquire and dispatch (a production-code injection point would be
		// required — rejected as test scaffolding in serving code). The
		// rule it double-checks — a lost lease refuses the deleted verdict
		// — is pinned by the in-flight case above.
		t.Skip("no deterministic seam between lease acquire and DELETE dispatch")
	})
}

// ---------------------------------------------------------------------------
// Fences (same-atom with the CAS: purge fence, sync-claim fence).

func TestS3WALRecoveryPurgeFence(t *testing.T) {
	ctx := context.Background()
	const base = "r-purge"
	member := base + "+ephemeral"
	lfsKey := "lfs/" + base + "/" + strings.Repeat("ef", 32)

	_, a, _ := newRecoveryStores(t)
	if err := a.FenceRepoPurge(ctx, base); err != nil {
		t.Fatalf("FenceRepoPurge: %v", err)
	}

	// Every row-creating method must reject (or provably no-op) against
	// the fenced group — the purge's zero-row verification depends on it.
	cases := []struct {
		name string
		call func(t *testing.T)
	}{
		{"CASRef", func(t *testing.T) {
			won, err := a.CASRef(ctx, base, "refs/heads/main", ZeroSHA, testSHA(0x11))
			if won || !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("CASRef = (%v, %v), want ErrRepoPurged", won, err)
			}
			// The rejection must carry the not-executed marker (issue
			// #52 taxonomy): IdempotentCAS must never recover it as a win.
			var ne interface{ SafeToRetry() bool }
			if !errors.As(err, &ne) || !ne.SafeToRetry() {
				t.Fatalf("CASRef fence rejection must be marked not-executed, got %v", err)
			}
		}},
		{"CASRef on namespace member", func(t *testing.T) {
			won, err := a.CASRef(ctx, member, "refs/heads/main", ZeroSHA, testSHA(0x11))
			if won || !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("member CASRef = (%v, %v), want ErrRepoPurged (fence covers the group)", won, err)
			}
		}},
		{"RegisterPacks", func(t *testing.T) {
			if err := a.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "k1", Size: 1}}); !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("RegisterPacks = %v, want ErrRepoPurged", err)
			}
		}},
		{"RegisterLFSObject", func(t *testing.T) {
			if err := a.RegisterLFSObject(ctx, base, lfsKey, 1); !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("RegisterLFSObject = %v, want ErrRepoPurged", err)
			}
		}},
		{"RegisterAdoptedLFSObject", func(t *testing.T) {
			if err := a.RegisterAdoptedLFSObject(ctx, base, lfsKey, 1); !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("RegisterAdoptedLFSObject = %v, want ErrRepoPurged", err)
			}
		}},
		{"ReservePackBytes", func(t *testing.T) {
			_, err := a.ReservePackBytes(ctx, []string{base}, []Pack{{Key: "k1", Size: 1}}, "o1", 0, ReserveNoFloorGen, 100, time.Hour)
			if !errors.Is(err, ErrRepoPurged) {
				t.Fatalf("ReservePackBytes = %v, want ErrRepoPurged", err)
			}
		}},
		{"BumpLFSAdoptionGen", func(t *testing.T) {
			// Silent no-op (PR #105 round 10): no admission consults a
			// purged repo's floors, and the bump must not recreate rows.
			if err := a.BumpLFSAdoptionGen(ctx, base); err != nil {
				t.Fatalf("BumpLFSAdoptionGen = %v, want silent no-op", err)
			}
			if g, err := a.LFSAdoptionGen(ctx, base); err != nil || g != 0 {
				t.Fatalf("adoption gen = (%d, %v), want untouched 0", g, err)
			}
		}},
		{"TryAdoptionClaim", func(t *testing.T) {
			claimed, owner, err := a.TryAdoptionClaim(ctx, base, lfsKey)
			if err != nil || claimed || owner != "" {
				t.Fatalf("TryAdoptionClaim = (%v, %q, %v), want unclaimed no-op", claimed, owner, err)
			}
		}},
		{"RecordLFSMismatch", func(t *testing.T) {
			if err := a.RecordLFSMismatch(ctx, base, lfsKey, "fp"); err != nil {
				t.Fatalf("RecordLFSMismatch = %v, want silent no-op", err)
			}
			if _, ok, err := a.LFSMismatchFingerprint(ctx, base, lfsKey); err != nil || ok {
				t.Fatalf("mismatch row = (%v, %v), want none", ok, err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seqBefore := mustManifest(t, a, base).Seq
			tc.call(t)
			if seqAfter := mustManifest(t, a, base).Seq; seqAfter != seqBefore {
				t.Fatalf("seq %d -> %d: a fenced rejection must write NOTHING", seqBefore, seqAfter)
			}
		})
	}
}

func TestS3WALRecoverySyncClaimFence(t *testing.T) {
	ctx := context.Background()
	const base = "r-syncfence"
	const branch = "refs/heads/main"
	sha1, sha2 := testSHA(0x11), testSHA(0x22)

	_, a, b := newRecoveryStores(t)
	if won, err := a.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
		t.Fatalf("seed create = (%v, %v)", won, err)
	}

	// Phase 1b only READS the claim (the write-through seam is Phase 3):
	// with no claim recorded, a fenced CAS fails CLOSED (issue #86 — the
	// stale sync push must not land after a steal/detach).
	won, err := a.CASRef(WithSyncClaimFence(ctx, base, "h1"), base, branch, sha1, sha2)
	if err != nil || won {
		t.Fatalf("fenced CASRef without claim = (%v, %v), want clean loss", won, err)
	}

	recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
		m.SyncClaim = &s3wal.SyncClaim{Holder: "h1", CreatedAt: clk}
	})

	won, err = a.CASRef(WithSyncClaimFence(ctx, base, "h2"), base, branch, sha1, sha2)
	if err != nil || won {
		t.Fatalf("fenced CASRef with foreign holder = (%v, %v), want clean loss", won, err)
	}
	if got := mustManifest(t, a, base).Members[base].Refs[branch].SHA; got != sha1 {
		t.Fatalf("ref = %s, want untouched %s", got, sha1)
	}

	won, err = a.CASRef(WithSyncClaimFence(ctx, base, "h1"), base, branch, sha1, sha2)
	if err != nil || !won {
		t.Fatalf("fenced CASRef with matching holder = (%v, %v), want win", won, err)
	}
	if got := mustManifest(t, a, base).Members[base].Refs[branch].SHA; got != sha2 {
		t.Fatalf("ref = %s, want %s", got, sha2)
	}
}

// ---------------------------------------------------------------------------
// Protocol guards (§2.2 size thresholds, §2.4 skew demotion, §2.3 seq
// monotonicity).

func TestS3WALRecoverySkewGuard(t *testing.T) {
	ctx := context.Background()
	const base = "r-skew"
	key := "lfs/" + base + "/" + strings.Repeat("aa", 32)
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	mc, a, b := newRecoveryStores(t)
	clock := newFakeClock(t0)
	a.now, b.now = clock.now, clock.now
	mc.Now = clock.now // store clock agrees (§2.4-3 Date witness)

	claimed, owner1, err := a.TryAdoptionClaim(ctx, base, key)
	if err != nil || !claimed {
		t.Fatalf("seed claim = (%v, %v)", claimed, err)
	}

	// A (wrong-clocked) writer pushes the manifest clock 2h ahead …
	clock.set(t0.Add(2 * time.Hour))
	recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {})

	// … then OUR local clock reads 10m BEHIND that manifest clock — past
	// the min(15m, AdoptionClaimTTL/4) = 7.5m bound. The claim's stamp is
	// 1h50m old (>> the 30m TTL), yet the steal must demote to
	// report-only: over-retention is the only reachable failure direction.
	clock.set(t0.Add(2*time.Hour - 10*time.Minute))
	seqBefore := mustManifest(t, a, base).Seq
	claimed, _, err = a.TryAdoptionClaim(ctx, base, key)
	if err != nil || claimed {
		t.Fatalf("skewed steal = (%v, %v), want demoted to report-only", claimed, err)
	}
	m := mustManifest(t, a, base)
	if m.Seq != seqBefore {
		t.Fatalf("seq %d -> %d: a demoted verdict must write NOTHING", seqBefore, m.Seq)
	}
	if r := m.Reservations[key+AdoptionClaimSuffix]; r == nil || r.Owner != owner1 {
		t.Fatalf("claim = %+v, want owner %q untouched", r, owner1)
	}

	// Clocks agree again: the steal proceeds.
	clock.set(t0.Add(2 * time.Hour))
	claimed, owner2, err := a.TryAdoptionClaim(ctx, base, key)
	if err != nil || !claimed || owner2 == owner1 {
		t.Fatalf("steal after skew cleared = (%v, %q, %v), want fresh owner", claimed, owner2, err)
	}
}

// TestS3WALRecoveryForwardJumpGuard (Codex on PR #156): a FORWARD local
// clock jump past a lease window must not expire the lease early — the
// §2.4 wall-vs-monotonic drift arm of the skew guard demotes destruction
// and steal to report-only until the drift clears.
func TestS3WALRecoveryForwardJumpGuard(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("delete lease survives a jump past its fencing window", func(t *testing.T) {
		const base = "r-jump-lease"
		key := "lfs/" + base + "/" + strings.Repeat("ef", 32)
		mc, a, b := newRecoveryStores(t)
		clock := newFakeClock(t0)
		a.now, b.now = clock.now, clock.now
		mc.Now = clock.now // store clock follows: this test exercises the DRIFT arm

		// An ACTIVE foreign delete lease renewed moments ago: its
		// guarded DELETE may still be in flight.
		recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
			m.DeleteLocks = map[string]*s3wal.DeleteLock{key: {Owner: "deleter", OpID: "op-y", CreatedAt: clk}}
		})

		// The wall clock jumps forward past the WHOLE fencing window;
		// the monotonic clock exposes the jump as drift. Registration
		// must stay fenced — committing here could leave a registered
		// row pointing at bytes the in-flight DELETE removes.
		window := s3walDeleteLockTTL + s3walDeleteIOTimeout + s3walDeleteLockSafety
		jump := window + time.Minute
		clock.set(t0.Add(jump))
		a.wallDrift = func() time.Duration { return jump }
		blockedCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		err := a.RegisterLFSObject(blockedCtx, base, key, 5)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("register under a jumped clock = %v, want ctx deadline (still fenced)", err)
		}
		if lock := mustManifest(t, a, base).DeleteLocks[key]; lock == nil || lock.Owner != "deleter" {
			t.Fatalf("lock = %+v, want untouched", lock)
		}

		// Drift cleared (the same reading is now a genuinely elapsed
		// window): the stale lease expires and registration proceeds.
		a.wallDrift = func() time.Duration { return 0 }
		if err := a.RegisterLFSObject(ctx, base, key, 5); err != nil {
			t.Fatalf("register after the drift cleared: %v", err)
		}
	})

	t.Run("adoption claim survives a jump past its TTL", func(t *testing.T) {
		const base = "r-jump-claim"
		key := "lfs/" + base + "/" + strings.Repeat("ab", 32)
		mc, a, _ := newRecoveryStores(t)
		clock := newFakeClock(t0)
		a.now = clock.now
		mc.Now = clock.now // store clock follows: this test exercises the DRIFT arm

		claimed, owner1, err := a.TryAdoptionClaim(ctx, base, key)
		if err != nil || !claimed {
			t.Fatalf("seed claim = (%v, %v)", claimed, err)
		}

		jump := AdoptionClaimTTL + time.Minute
		clock.set(t0.Add(jump))
		a.wallDrift = func() time.Duration { return jump }

		seqBefore := mustManifest(t, a, base).Seq
		claimed, _, err = a.TryAdoptionClaim(ctx, base, key)
		if err != nil || claimed {
			t.Fatalf("steal under a jumped clock = (%v, %v), want demoted to report-only", claimed, err)
		}
		m := mustManifest(t, a, base)
		if m.Seq != seqBefore {
			t.Fatalf("seq %d -> %d: a demoted verdict must write NOTHING", seqBefore, m.Seq)
		}
		if r := m.Reservations[key+AdoptionClaimSuffix]; r == nil || r.Owner != owner1 {
			t.Fatalf("claim = %+v, want owner %q untouched", r, owner1)
		}
		// The live-leaning twin: the holder keeps its lease through the
		// jump (owner fences still protect stolen claims).
		if renewed, err := a.RenewAdoptionClaim(ctx, key, owner1); err != nil || !renewed {
			t.Fatalf("renew under a jumped clock = (%v, %v), want renewed", renewed, err)
		}

		// Drift cleared and the renewed lease genuinely ages out: the
		// steal proceeds normally.
		a.wallDrift = func() time.Duration { return 0 }
		clock.set(t0.Add(jump + AdoptionClaimTTL + time.Minute))
		claimed, owner2, err := a.TryAdoptionClaim(ctx, base, key)
		if err != nil || !claimed || owner2 == owner1 {
			t.Fatalf("steal after the drift cleared = (%v, %q, %v), want fresh owner", claimed, owner2, err)
		}
	})
}

func TestS3WALRecoveryStaleReadPanics(t *testing.T) {
	ctx := context.Background()
	const base = "r-stale"
	const branch = "refs/heads/main"

	mc, a, _ := newRecoveryStores(t)
	if won, err := a.CASRef(ctx, base, branch, ZeroSHA, testSHA(0x11)); err != nil || !won {
		t.Fatalf("seed create = (%v, %v)", won, err)
	}
	if won, err := a.CASRef(ctx, base, branch, testSHA(0x11), testSHA(0x22)); err != nil || !won {
		t.Fatalf("seed update = (%v, %v)", won, err)
	}

	// The store now serves seq 1 after this process observed seq 2 — a
	// store-fidelity violation (plan §2.3): crash-and-burn, never a silent
	// time-travel read.
	armStaleGet(mc)
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("stale read must panic")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "stale read") {
			t.Fatalf("panic = %q, want the stale-read assert", msg)
		}
	}()
	_, _ = a.ListRefs(ctx, base)
}

// TestS3WALRecoveryRefNameUTF8 (Codex round 4 on PR #156, security): git
// accepts raw non-UTF-8 bytes in ref names, the update hook compares raw
// bytes to policy (raw-0xff ≠ the U+FFFD-spelled protected ref, so policy
// passes), and encoding/json would collapse the raw key onto the U+FFFD
// spelling — duplicate JSON keys whose decode keeps the LAST: a
// repo-scoped writer overwriting a protected ref's SHA. CASRef must
// reject such names with the typed error BEFORE any manifest mutation.
func TestS3WALRecoveryRefNameUTF8(t *testing.T) {
	ctx := context.Background()
	const base = "r-utf8"
	_, a, _ := newRecoveryStores(t)

	// The policy-protected ref, spelled with a REAL U+FFFD.
	protected := "refs/heads/p\uFFFD"
	sha := testSHA(0x11)
	if won, err := a.CASRef(ctx, base, protected, ZeroSHA, sha); err != nil || !won {
		t.Fatalf("seed protected ref = (%v, %v)", won, err)
	}
	seqBefore := mustManifest(t, a, base).Seq

	// The attack spelling: raw 0xff, distinct bytes, same JSON encoding.
	raw := "refs/heads/p\xff"
	for _, tc := range []struct{ what, old, new string }{
		{"create", ZeroSHA, testSHA(0x22)},
		{"update", sha, testSHA(0x22)},
		{"delete", sha, ZeroSHA},
	} {
		won, err := a.CASRef(ctx, base, raw, tc.old, tc.new)
		if won || !errors.Is(err, ErrRefNameNotUTF8) {
			t.Fatalf("raw-byte %s = (%v, %v), want ErrRefNameNotUTF8", tc.what, won, err)
		}
	}
	m := mustManifest(t, a, base)
	if m.Seq != seqBefore {
		t.Fatalf("seq %d -> %d: the rejection must precede any mutation", seqBefore, m.Seq)
	}
	if got := m.Members[base].Refs[protected].SHA; got != sha {
		t.Fatalf("protected ref = %s, want %s untouched", got, sha)
	}
	// Reads with raw-byte names are clean misses: no manifest can hold
	// such a key (writers fail closed).
	if _, err := a.GetRef(ctx, base, raw); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRef(raw) = %v, want clean ErrNotFound", err)
	}
}

// TestS3WALRecoveryBootClockGuard (Codex round 2 on PR #156): a process
// that BOOTS with its wall clock already ahead reports ~zero
// wall-vs-monotonic drift, and the manifest-ahead arm reads an old clk as
// a quiet repo — so only the §2.4-3 store-Date witness can catch it.
// Store b plays the previous, correctly clocked process; store a boots
// wall-ahead beyond every bound. Every destructive verdict class must
// demote to report-only; once local and store clocks agree again the
// same expiries proceed normally.
func TestS3WALRecoveryBootClockGuard(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	const skew = 2 * time.Hour // beyond the 15m cap of every §2.4 bound

	// bootSkewed builds the two-process scene: b (and the store's Date
	// witness) run on the true clock, a boots ahead by skew.
	bootSkewed := func(t *testing.T) (store, local *fakeClock, a, b *S3WAL) {
		t.Helper()
		store, local = newFakeClock(t0), newFakeClock(t0.Add(skew))
		mc, sa, sb := newRecoveryStores(t)
		sa.now, sb.now, mc.Now = local.now, store.now, store.now
		return store, local, sa, sb
	}
	// agree advances both clocks to the same instant: the witness
	// agrees again and genuinely elapsed windows may expire.
	agree := func(store, local *fakeClock, at time.Time) {
		store.set(at)
		local.set(at)
	}

	t.Run("delete lease", func(t *testing.T) {
		const base = "r-boot-lease"
		key := "lfs/" + base + "/" + strings.Repeat("cd", 32)
		store, local, a, b := bootSkewed(t)
		recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
			m.DeleteLocks = map[string]*s3wal.DeleteLock{key: {Owner: "deleter", OpID: "op-b", CreatedAt: clk}}
		})
		// The lease is seconds old in true time; a's clock alone says
		// the whole window elapsed. Registration must stay fenced.
		blockedCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		err := a.RegisterLFSObject(blockedCtx, base, key, 5)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("register on a boot-skewed clock = %v, want ctx deadline (still fenced)", err)
		}
		if lock := mustManifest(t, a, base).DeleteLocks[key]; lock == nil || lock.Owner != "deleter" {
			t.Fatalf("lock = %+v, want untouched", lock)
		}
		// Clocks agree and the window has truly elapsed: proceed.
		agree(store, local, t0.Add(s3walDeleteLockWindow()+time.Minute))
		if err := a.RegisterLFSObject(ctx, base, key, 5); err != nil {
			t.Fatalf("register after clocks agree: %v", err)
		}
	})

	t.Run("adoption claim steal", func(t *testing.T) {
		const base = "r-boot-claim"
		key := "lfs/" + base + "/" + strings.Repeat("ab", 32)
		store, local, a, b := bootSkewed(t)
		claimed, owner1, err := b.TryAdoptionClaim(ctx, base, key)
		if err != nil || !claimed {
			t.Fatalf("seed claim = (%v, %v)", claimed, err)
		}
		seqBefore := mustManifest(t, a, base).Seq
		claimed, _, err = a.TryAdoptionClaim(ctx, base, key)
		if err != nil || claimed {
			t.Fatalf("steal on a boot-skewed clock = (%v, %v), want demoted to report-only", claimed, err)
		}
		m := mustManifest(t, a, base)
		if m.Seq != seqBefore {
			t.Fatalf("seq %d -> %d: a demoted verdict must write NOTHING", seqBefore, m.Seq)
		}
		if r := m.Reservations[key+AdoptionClaimSuffix]; r == nil || r.Owner != owner1 {
			t.Fatalf("claim = %+v, want owner %q untouched", r, owner1)
		}
		agree(store, local, t0.Add(skew))
		claimed, owner2, err := a.TryAdoptionClaim(ctx, base, key)
		if err != nil || !claimed || owner2 == owner1 {
			t.Fatalf("steal after clocks agree = (%v, %q, %v), want fresh owner", claimed, owner2, err)
		}
	})

	t.Run("reservation prune", func(t *testing.T) {
		const base = "r-boot-resv"
		store, local, a, b := bootSkewed(t)
		ttl := time.Hour
		if dec, err := b.ReservePackBytes(ctx, []string{base}, []Pack{{Key: "k1", Size: 10}}, "crashed", 0, ReserveNoFloorGen, 1000, ttl); err != nil || !dec.OK {
			t.Fatalf("seed reserve = (%+v, %v)", dec, err)
		}
		if n, err := a.SweepExpiredPackReservations(ctx, ttl); err != nil || n != 0 {
			t.Fatalf("sweep on a boot-skewed clock = (%d, %v), want 0 pruned", n, err)
		}
		if r := mustManifest(t, a, base).Reservations["k1"]; r == nil {
			t.Fatal("reservation must survive the boot-skewed sweep")
		}
		agree(store, local, t0.Add(skew))
		if n, err := a.SweepExpiredPackReservations(ctx, ttl); err != nil || n != 1 {
			t.Fatalf("sweep after clocks agree = (%d, %v), want 1 pruned", n, err)
		}
	})

	t.Run("unreferenced reap", func(t *testing.T) {
		const base = "r-boot-reap"
		key := "lfs/" + base + "/" + strings.Repeat("ef", 32)
		store, local, a, b := bootSkewed(t)
		if err := b.RegisterLFSObject(ctx, base, key, 5); err != nil {
			t.Fatalf("seed register: %v", err)
		}
		if n, err := b.MarkLFSObjectsUnreferenced(ctx, base, []string{key}); err != nil || n != 1 {
			t.Fatalf("mark = (%d, %v)", n, err)
		}
		// A fresh legacy-armed reap watermark stamped from a's OWN
		// frame: the scan-state freshness gate is a separate lane, and
		// this subtest isolates the grace verdict's Date arm.
		if err := a.RecordLFSReapComplete(ctx, 1, true); err != nil {
			t.Fatalf("stamp reap watermark: %v", err)
		}
		gens := map[string]int64{base: 0}
		retired, err := a.RetireUnreferencedLFSObject(ctx, base, key, gens, time.Hour, time.Hour)
		if err != nil || retired {
			t.Fatalf("retire on a boot-skewed clock = (%v, %v), want spared", retired, err)
		}
		agree(store, local, t0.Add(skew))
		retired, err = a.RetireUnreferencedLFSObject(ctx, base, key, gens, time.Hour, time.Hour)
		if err != nil || !retired {
			t.Fatalf("retire after clocks agree = (%v, %v), want retired", retired, err)
		}
	})
}

// TestS3WALRecoveryDeleteLeaseReap (Codex round 2 on PR #156): a leaked
// delete lease — an ambiguous DELETE or failed post-delete clear leaves
// its entry, and registration used to only IGNORE expired ones — must be
// reaped once strictly past the full blocking window: by the reservation
// sweep and opportunistically by the registration path itself.
func TestS3WALRecoveryDeleteLeaseReap(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("sweep reaps only past the full window", func(t *testing.T) {
		const base = "r-reap-sweep"
		key := "lfs/" + base + "/" + strings.Repeat("aa", 32)
		mc, a, b := newRecoveryStores(t)
		clock := newFakeClock(t0)
		a.now, b.now, mc.Now = clock.now, clock.now, clock.now
		recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
			m.DeleteLocks = map[string]*s3wal.DeleteLock{key: {Owner: "crashed", OpID: "op-x", CreatedAt: clk}}
		})

		// Inside the window — even under the olderThan=0 hatch — the
		// lease survives: it may fence an in-flight DELETE.
		clock.set(t0.Add(s3walDeleteLockWindow() - time.Millisecond))
		if _, err := a.SweepExpiredPackReservations(ctx, 0); err != nil {
			t.Fatalf("in-window sweep: %v", err)
		}
		if lock := mustManifest(t, a, base).DeleteLocks[key]; lock == nil {
			t.Fatal("an in-window lease must survive the sweep (it may fence an in-flight DELETE)")
		}

		clock.set(t0.Add(s3walDeleteLockWindow()))
		if _, err := a.SweepExpiredPackReservations(ctx, time.Hour); err != nil {
			t.Fatalf("post-window sweep: %v", err)
		}
		if lock := mustManifest(t, a, base).DeleteLocks[key]; lock != nil {
			t.Fatalf("lock = %+v, want reaped past the full window", lock)
		}
	})

	t.Run("registration clears the observed expired lease", func(t *testing.T) {
		const base = "r-reap-register"
		key := "lfs/" + base + "/" + strings.Repeat("bb", 32)
		mc, a, b := newRecoveryStores(t)
		clock := newFakeClock(t0)
		a.now, b.now, mc.Now = clock.now, clock.now, clock.now
		recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
			m.DeleteLocks = map[string]*s3wal.DeleteLock{key: {Owner: "crashed", OpID: "op-y", CreatedAt: clk}}
		})
		clock.set(t0.Add(s3walDeleteLockWindow()))
		if err := a.RegisterLFSObject(ctx, base, key, 5); err != nil {
			t.Fatalf("register past the window: %v", err)
		}
		m := mustManifest(t, a, base)
		if lock := m.DeleteLocks[key]; lock != nil {
			t.Fatalf("lock = %+v, want opportunistically reaped by registration", lock)
		}
		if m.LFS.Objects[key] == nil {
			t.Fatal("registration must have landed alongside the reap")
		}
	})
}

// TestS3WALRecoveryCrossProcessFence (Codex on PR #156, security): the
// in-process absenceFenceMu cannot serialize a receive-hook process
// against the serve process's arming pass, so the finite reserve path
// re-reads the fence marker AFTER its CAS commits and revokes its own
// rows when the marker is armed. Store b plays the arming process: its
// separate mutex is exactly the cross-process hole.
func TestS3WALRecoveryCrossProcessFence(t *testing.T) {
	ctx := context.Background()
	const branch = "refs/heads/main"
	pack := func(key string, size int64) []Pack { return []Pack{{Key: key, Size: size}} }

	t.Run("fence armed between pre-read and post-read revokes the admission", func(t *testing.T) {
		const base = "r-xfence-mid"
		mc, a, b := newRecoveryStores(t)
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, testSHA(0x11)); err != nil || !won {
			t.Fatalf("seed ref = (%v, %v)", won, err)
		}
		// The OTHER process arms the fence the instant our admission
		// CAS hits the store — after the in-CAS pre-read, before the
		// post-commit re-read. One-shot and scoped to the manifest key
		// so the arm's own scan-object PUT and the revocation CAS do
		// not recurse.
		armed := false
		mc.Hooks.AfterPut = func(op, key string) {
			if armed || key != s3wal.ManifestKey(base) {
				return
			}
			armed = true
			if err := b.ArmAbsenceAdmissionFence(ctx, "lapse-evidence"); err != nil {
				t.Errorf("arm during admission: %v", err)
			}
		}
		defer func() { mc.Hooks.AfterPut = nil }()

		dec, err := a.ReservePackBytes(ctx, []string{base}, pack("k1", 10), "o1", 0, ReserveNoFloorGen, 1000, time.Hour)
		if err != nil || dec.OK || !dec.RenewalDegraded {
			t.Fatalf("mid-window fenced reserve = (%+v, %v), want the fail-closed RenewalDegraded deny", dec, err)
		}
		if !armed {
			t.Fatal("the arming hook never fired: the admission CAS was not observed")
		}
		m := mustManifest(t, a, base)
		if r := m.Reservations["k1"]; r != nil {
			t.Fatalf("reservation = %+v, want revoked (self-undo)", r)
		}
		if _, ok, err := a.scanGet(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence); err != nil || !ok {
			t.Fatalf("fence marker = (ok=%v, %v), want still armed", ok, err)
		}
	})

	t.Run("revocation spares foreign rows", func(t *testing.T) {
		const base = "r-xfence-foreign"
		mc, a, b := newRecoveryStores(t)
		// A live foreign reservation on a DIFFERENT key must survive
		// the self-undo untouched (owner+op-ID keyed delete).
		if dec, err := a.ReservePackBytes(ctx, []string{base}, pack("kf", 7), "other", 0, ReserveNoFloorGen, 1000, time.Hour); err != nil || !dec.OK {
			t.Fatalf("foreign reserve = (%+v, %v)", dec, err)
		}
		armed := false
		mc.Hooks.AfterPut = func(op, key string) {
			if armed || key != s3wal.ManifestKey(base) {
				return
			}
			armed = true
			if err := b.ArmAbsenceAdmissionFence(ctx, "lapse-evidence"); err != nil {
				t.Errorf("arm during admission: %v", err)
			}
		}
		defer func() { mc.Hooks.AfterPut = nil }()
		dec, err := a.ReservePackBytes(ctx, []string{base}, pack("k1", 10), "o1", 0, ReserveNoFloorGen, 1000, time.Hour)
		if err != nil || dec.OK || !dec.RenewalDegraded {
			t.Fatalf("mid-window fenced reserve = (%+v, %v), want deny", dec, err)
		}
		m := mustManifest(t, a, base)
		if m.Reservations["k1"] != nil {
			t.Fatalf("reservation = %+v, want revoked", m.Reservations["k1"])
		}
		if r := m.Reservations["kf"]; r == nil || r.Owner != "other" || r.Bytes != 7 {
			t.Fatalf("foreign reservation = %+v, want untouched", r)
		}
	})

	t.Run("fence armed before the admission denies with no write", func(t *testing.T) {
		const base = "r-xfence-pre"
		_, a, b := newRecoveryStores(t)
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, testSHA(0x11)); err != nil || !won {
			t.Fatalf("seed ref = (%v, %v)", won, err)
		}
		if err := b.ArmAbsenceAdmissionFence(ctx, "lapse-evidence"); err != nil {
			t.Fatalf("arm: %v", err)
		}
		seqBefore := mustManifest(t, a, base).Seq
		dec, err := a.ReservePackBytes(ctx, []string{base}, pack("k1", 10), "o1", 0, ReserveNoFloorGen, 1000, time.Hour)
		if err != nil || dec.OK || !dec.RenewalDegraded {
			t.Fatalf("pre-armed reserve = (%+v, %v), want deny", dec, err)
		}
		if seq := mustManifest(t, a, base).Seq; seq != seqBefore {
			t.Fatalf("seq %d -> %d: a pre-armed deny must write NOTHING", seqBefore, seq)
		}
	})

	t.Run("no fence: exactly one manifest write plus the post-read GET", func(t *testing.T) {
		const base = "r-xfence-clean"
		mc, a, _ := newRecoveryStores(t)
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, testSHA(0x11)); err != nil || !won {
			t.Fatalf("seed ref = (%v, %v)", won, err)
		}
		writes := 0
		mc.Hooks.AfterPut = func(op, key string) {
			if key == s3wal.ManifestKey(base) {
				writes++
			}
		}
		defer func() { mc.Hooks.AfterPut = nil }()
		dec, err := a.ReservePackBytes(ctx, []string{base}, pack("k1", 10), "o1", 0, ReserveNoFloorGen, 1000, time.Hour)
		if err != nil || !dec.OK {
			t.Fatalf("clean reserve = (%+v, %v), want granted", dec, err)
		}
		if writes != 1 {
			t.Fatalf("manifest writes = %d, want exactly 1 on the happy path", writes)
		}
		if r := mustManifest(t, a, base).Reservations["k1"]; r == nil || r.Owner != "o1" {
			t.Fatalf("reservation = %+v, want granted row", r)
		}
	})

	t.Run("unbounded internal admissions are exempt from the fence", func(t *testing.T) {
		const base = "r-xfence-internal"
		_, a, b := newRecoveryStores(t)
		if err := b.ArmAbsenceAdmissionFence(ctx, "lapse-evidence"); err != nil {
			t.Fatalf("arm: %v", err)
		}
		// The SQL twins fence only finite quota-gated admissions;
		// unbounded internal reservations proceed so recovery can
		// re-cover — same scoping here, pre-read and post-read alike.
		dec, err := a.ReservePackBytes(ctx, []string{base}, pack("k1", 10), "o1", 0, ReserveNoFloorGen, math.MaxInt64, time.Hour)
		if err != nil || !dec.OK {
			t.Fatalf("internal reserve under an armed fence = (%+v, %v), want granted", dec, err)
		}
		if r := mustManifest(t, a, base).Reservations["k1"]; r == nil {
			t.Fatal("internal reservation must survive the armed fence")
		}
	})
}

func TestS3WALRecoverySizeCap(t *testing.T) {
	ctx := context.Background()
	const base = "r-cap"
	const branch = "refs/heads/main"
	sha1, sha2 := testSHA(0x11), testSHA(0x22)

	t.Run("growing mutations refused at the hard cap", func(t *testing.T) {
		_, a, _ := newRecoveryStores(t)
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("seed create = (%v, %v)", won, err)
		}
		a.hardCapBytes = 1

		wantRefused := func(what string, err error) {
			t.Helper()
			if err == nil || !strings.Contains(err.Error(), "refusing new registrations") {
				t.Fatalf("%s over the cap = %v, want the documented refusal", what, err)
			}
		}
		_, err := a.CASRef(ctx, base, "refs/heads/new", ZeroSHA, sha1)
		wantRefused("ref create", err)
		wantRefused("RegisterPacks", a.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "k1", Size: 1}}))
		_, err = a.ReservePackBytes(ctx, []string{base}, []Pack{{Key: "k1", Size: 1}}, "o1", 0, ReserveNoFloorGen, 100, time.Hour)
		wantRefused("ReservePackBytes", err)

		// Shrinking mutations always pass — an oversized manifest must
		// stay cleanable.
		if won, err := a.CASRef(ctx, base, branch, sha1, sha2); err != nil || !won {
			t.Fatalf("update under cap = (%v, %v), want win (not a growing mutation)", won, err)
		}
		if won, err := a.CASRef(ctx, base, branch, sha2, ZeroSHA); err != nil || !won {
			t.Fatalf("delete under cap = (%v, %v), want win (shrinking)", won, err)
		}
	})

	t.Run("purge pipeline exempt from the hard cap", func(t *testing.T) {
		// An oversized manifest must stay purgeable (Codex on PR #156):
		// the purge pipeline is the machinery that shrinks it, so the
		// cap refusing the fence would wedge the repo permanently.
		_, a, _ := newRecoveryStores(t)
		lfsKey := "lfs/" + base + "/" + strings.Repeat("aa", 32)
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("seed ref = (%v, %v)", won, err)
		}
		if err := a.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "k1", Size: 9}}); err != nil {
			t.Fatalf("seed pack: %v", err)
		}
		if err := a.RegisterLFSObject(ctx, base, lfsKey, 7); err != nil {
			t.Fatalf("seed lfs: %v", err)
		}
		a.hardCapBytes = 1

		if err := a.FenceRepoPurge(ctx, base); err != nil {
			t.Fatalf("FenceRepoPurge at the cap: %v", err)
		}
		if n, err := a.DropRepoRefs(ctx, []string{base}); err != nil || n != 1 {
			t.Fatalf("DropRepoRefs at the cap = (%d, %v), want 1 dropped", n, err)
		}
		if n, err := a.RetireRepoPacks(ctx, []string{base}); err != nil || n != 1 {
			t.Fatalf("RetireRepoPacks at the cap = (%d, %v), want 1 retired", n, err)
		}
		if n, err := a.DropRepoLFSObjects(ctx, []string{base}); err != nil || n != 1 {
			t.Fatalf("DropRepoLFSObjects at the cap = (%d, %v), want 1 dropped", n, err)
		}
		if _, err := a.PurgeRepoLedger(ctx, []string{base}); err != nil {
			t.Fatalf("PurgeRepoLedger at the cap: %v", err)
		}
		if n, err := a.CountRepoRows(ctx, []string{base}); err != nil || n != 0 {
			t.Fatalf("CountRepoRows after purge = (%d, %v), want 0", n, err)
		}
		if purged, err := a.IsRepoPurged(ctx, base); err != nil || !purged {
			t.Fatalf("IsRepoPurged = (%v, %v), want fenced", purged, err)
		}
	})

	t.Run("soft threshold warns without refusing", func(t *testing.T) {
		_, a, _ := newRecoveryStores(t)
		a.softWarnBytes = 1
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("create over the soft threshold = (%v, %v), want win", won, err)
		}
		if !a.warned[base] {
			t.Fatal("soft threshold crossing must record the once-per-base warning")
		}
	})
}
