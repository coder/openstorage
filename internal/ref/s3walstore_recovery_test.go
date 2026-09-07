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

	"github.com/coder/open-storage/internal/ref/s3wal"
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
	if _, ok := errors.AsType[*s3wal.AmbiguousError](err); !ok {
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

	t.Run("overlapping subset fold with pre-registered output = fail closed", func(t *testing.T) {
		// Codex round 10 on PR #156: with the fold OUTPUT already
		// registered before the call, "output present + not all old
		// present" is satisfiable by a competing overlapping fold that
		// removed only a SUBSET of our old set — our invocation never
		// landed and p2 stays registered, so reporting won would violate
		// the atomic replacement contract. Applied requires the fold's
		// FULL effect: output present and ZERO old rows remaining.
		mc, a, b := seed(t)
		if err := a.RegisterPacks(ctx, base, []Pack{fold}); err != nil {
			t.Fatalf("pre-register fold output: %v", err)
		}
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		armAfterPut(mc, func() {
			// The competitor folds ONLY p1 into the same output.
			if won, err := b.ReplacePacks(ctx, base, []Pack{p1}, fold); err != nil || !won {
				t.Fatalf("competing subset fold = (%v, %v)", won, err)
			}
		})
		won, err := a.ReplacePacks(ctx, base, []Pack{p1, p2}, fold)
		wantAmbiguous(t, err)
		if won {
			t.Fatal("subset-fold state must not report won: p2 is still registered and our fold never landed")
		}
		mem := mustManifest(t, a, base).Members[base]
		if !s3walHasPackID(mem, "p2") {
			t.Fatal("p2 must remain registered (only the competitor's subset fold landed)")
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

func TestS3WALRecoveryRenewAdoptionClaim(t *testing.T) {
	ctx := context.Background()
	const base = "r-renew-claim"
	key := "lfs/" + base + "/" + strings.Repeat("cd", 32)
	claimKey := key + AdoptionClaimSuffix

	t.Run("ambiguous dropped renewal reruns until the stamp advances", func(t *testing.T) {
		// Codex round 6 on PR #156 (security): renewal changes only
		// CreatedAt, so the recovery GET after a dropped PUT still shows
		// the same owner. Owner match alone must not report success — the
		// lease would not be extended, and a near-expiry claim could be
		// stolen before the next renewal tick while the holder resumes a
		// size-scaled copy believing the TTL restarted.
		mc, a, _ := newRecoveryStores(t)
		claimed, owner, err := a.TryAdoptionClaim(ctx, base, key)
		if err != nil || !claimed {
			t.Fatalf("TryAdoptionClaim = (%v, %v)", claimed, err)
		}
		before := mustManifest(t, a, base).Reservations[claimKey].CreatedAt
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		renewed, err := a.RenewAdoptionClaim(ctx, key, owner)
		if err != nil || !renewed {
			t.Fatalf("RenewAdoptionClaim = (%v, %v), want renewed", renewed, err)
		}
		after := mustManifest(t, a, base).Reservations[claimKey].CreatedAt
		if !after.After(before) {
			t.Fatalf("claim stamp = %v (was %v): renewal reported success without extending the lease", after, before)
		}
	})

	t.Run("stolen while ambiguous = lease lost", func(t *testing.T) {
		// Pins the predicate's owner arm: the thief's fresh stamp DOES
		// advance past our attempt clock, so stamp advancement alone must
		// never report a foreign renewal as ours.
		mc, a, b := newRecoveryStores(t)
		claimed, owner, err := a.TryAdoptionClaim(ctx, base, key)
		if err != nil || !claimed {
			t.Fatalf("TryAdoptionClaim = (%v, %v)", claimed, err)
		}
		armPutFault(mc, s3wal.FaultAmbiguousDropped)
		armAfterPut(mc, func() {
			recoveryEdit(t, b, base, func(m *s3wal.Manifest, clk time.Time) {
				r := m.Reservations[claimKey]
				if r == nil {
					t.Error("no claim to steal")
					return
				}
				r.Owner = "thief"
				r.CreatedAt = clk
			})
		})
		renewed, err := a.RenewAdoptionClaim(ctx, key, owner)
		if err != nil || renewed {
			t.Fatalf("RenewAdoptionClaim = (%v, %v), want clean lease-lost", renewed, err)
		}
		if got := mustManifest(t, a, base).Reservations[claimKey]; got == nil || got.Owner != "thief" {
			t.Fatalf("claim entry = %+v, want the thief's", got)
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

	t.Run("ambiguously dropped renewals must not report deleted", func(t *testing.T) {
		// Codex round 6 on PR #156 (security), sibling of the
		// adoption-claim finding: a renewal changes only CreatedAt, so an
		// owner-only recovery predicate reports a dropped PUT as success
		// and the loop keeps believing the lease is fresh while it
		// silently expires under the in-flight DELETE — a registration
		// could then reap the "leaked" lock and register over bytes being
		// deleted. With every renewal PUT ambiguously dropped, the loop
		// must prove the stamp advanced, observe the failure, and refuse
		// deleted=true.
		restore := func(ttl, io, safety, poll time.Duration) func() {
			oldTTL, oldIO, oldSafety, oldPoll := s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll
			s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll = ttl, io, safety, poll
			return func() {
				s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll = oldTTL, oldIO, oldSafety, oldPoll
			}
		}
		t.Cleanup(restore(60*time.Millisecond, 25*time.Millisecond, 10*time.Millisecond, 2*time.Millisecond))

		mc, a, _ := newRecoveryStores(t)
		// Armed BEFORE the call (a hook write during del would race the
		// renewal goroutine's read): the lease acquisition on the fresh
		// manifest is a create and passes; every later advance — the
		// renewal loop's PUTs — drops ambiguously.
		mc.Hooks.PutFault = func(op, k string) s3wal.PutFault {
			if op == "advance" {
				return s3wal.FaultAmbiguousDropped
			}
			return s3wal.FaultNone
		}
		deleted, err := a.GuardedLFSDelete(ctx, base, key, func(context.Context) error {
			time.Sleep(150 * time.Millisecond) // several renewal periods
			return nil
		})
		if deleted {
			t.Fatal("persistently dropped renewals must refuse deleted=true")
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

// TestS3WALRecoveryRefundParkPurgeRace (Codex round 4 on PR #156): the
// retirement's manifest CAS and its refund park are two separate writes;
// a purge fence plus a COMPLETE PurgeRepoLedger (including the
// refund-prefix sweep) can land in the gap, so the park would recreate a
// bucket the purge already cleaned — settlement refuses purged repos, so
// the side object would leak forever. The park re-reads the fence after
// landing and self-undoes when fenced; all three orderings must end with
// an empty refund namespace.
func TestS3WALRecoveryRefundParkPurgeRace(t *testing.T) {
	ctx := context.Background()

	// seedRetirable registers, marks, and watermark-arms one legacy LFS
	// row so RetireUnreferencedLFSObject(grace=0) is decidable.
	seedRetirable := func(t *testing.T, a *S3WAL, base, key string) {
		t.Helper()
		if err := a.RegisterLFSObject(ctx, base, key, 7); err != nil {
			t.Fatalf("seed register: %v", err)
		}
		if n, err := a.MarkLFSObjectsUnreferenced(ctx, base, []string{key}); err != nil || n != 1 {
			t.Fatalf("mark = (%d, %v)", n, err)
		}
		if err := a.RecordLFSReapComplete(ctx, 1, true); err != nil {
			t.Fatalf("stamp reap watermark: %v", err)
		}
	}
	wantNoRefunds := func(t *testing.T, a *S3WAL, base string) {
		t.Helper()
		prefix := ScanScopeReclaimRefundPrefix(base)
		scopes, err := a.ListJobScanStateScopes(ctx, ScanJobLFSReclaim, prefix, prefix, 512)
		if err != nil || len(scopes) != 0 {
			t.Fatalf("refund scopes = (%v, %v), want none (leaked side object)", scopes, err)
		}
	}
	gens := func(base string) map[string]int64 { return map[string]int64{base: 0} }

	t.Run("fence lands in the park gap", func(t *testing.T) {
		const base = "r-park-gap"
		key := "lfs/" + base + "/" + strings.Repeat("aa", 32)
		mc, a, b := newRecoveryStores(t)
		seedRetirable(t, a, base, key)
		// The purge pipeline (store b: its own process, its own locks)
		// completes the instant the RETIREMENT's manifest CAS lands —
		// strictly before the separate park PUT, so the purge's refund
		// sweep finds nothing and only the post-park fence re-read can
		// prevent the leak.
		raced := false
		mc.Hooks.AfterPut = func(op, putKey string) {
			if raced || putKey != s3wal.ManifestKey(base) {
				return
			}
			raced = true
			if err := b.FenceRepoPurge(ctx, base); err != nil {
				t.Errorf("fence during park: %v", err)
			}
			if _, err := b.PurgeRepoLedger(ctx, []string{base}); err != nil {
				t.Errorf("purge ledger during park: %v", err)
			}
		}
		defer func() { mc.Hooks.AfterPut = nil }()
		retired, err := a.RetireUnreferencedLFSObject(ctx, base, key, gens(base), 0, time.Hour)
		if err != nil || !retired {
			t.Fatalf("retire = (%v, %v), want retired (the row deletion landed)", retired, err)
		}
		if !raced {
			t.Fatal("the purge hook never fired: the park PUT was not observed")
		}
		mc.Hooks.AfterPut = nil
		wantNoRefunds(t, a, base)
	})

	t.Run("retire then purge", func(t *testing.T) {
		const base = "r-park-then-purge"
		key := "lfs/" + base + "/" + strings.Repeat("bb", 32)
		_, a, b := newRecoveryStores(t)
		seedRetirable(t, a, base, key)
		if retired, err := a.RetireUnreferencedLFSObject(ctx, base, key, gens(base), 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire = (%v, %v)", retired, err)
		}
		prefix := ScanScopeReclaimRefundPrefix(base)
		if scopes, err := a.ListJobScanStateScopes(ctx, ScanJobLFSReclaim, prefix, prefix, 512); err != nil || len(scopes) != 1 {
			t.Fatalf("parked refund scopes = (%v, %v), want exactly one", scopes, err)
		}
		if err := b.FenceRepoPurge(ctx, base); err != nil {
			t.Fatalf("fence: %v", err)
		}
		if _, err := b.PurgeRepoLedger(ctx, []string{base}); err != nil {
			t.Fatalf("purge ledger: %v", err)
		}
		wantNoRefunds(t, a, base)
	})

	t.Run("purge then retire", func(t *testing.T) {
		const base = "r-purge-then-park"
		key := "lfs/" + base + "/" + strings.Repeat("cd", 32)
		_, a, b := newRecoveryStores(t)
		seedRetirable(t, a, base, key)
		if err := b.FenceRepoPurge(ctx, base); err != nil {
			t.Fatalf("fence: %v", err)
		}
		retired, err := a.RetireUnreferencedLFSObject(ctx, base, key, gens(base), 0, time.Hour)
		if err != nil || retired {
			t.Fatalf("retire on a fenced repo = (%v, %v), want (false, nil): the purge pipeline owns fenced rows", retired, err)
		}
		wantNoRefunds(t, a, base)
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

	t.Run("revocation preserves earlier shared attempts", func(t *testing.T) {
		// Codex round 11 on PR #156 (security): concurrent identical
		// uploads share one reservation row — each admission increments
		// Attempts and rotates Owner/OpID. Revoking the LATEST attempt
		// by deleting the row would also strip the earlier in-flight
		// push's quota cover: after recovery clears the fence, a new
		// admission could fit before the older publisher registers, and
		// both then credit usage beyond quota. Undo only our attempt.
		const base = "r-xfence-shared"
		mc, a, b := newRecoveryStores(t)
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, testSHA(0x11)); err != nil || !won {
			t.Fatalf("seed ref = (%v, %v)", won, err)
		}
		// The earlier identical upload holds the shared row (Attempts=1).
		if dec, err := a.ReservePackBytes(ctx, []string{base}, pack("k1", 10), "o1", 0, ReserveNoFloorGen, 1000, time.Hour); err != nil || !dec.OK {
			t.Fatalf("first reserve = (%+v, %v), want granted", dec, err)
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
		dec, err := a.ReservePackBytes(ctx, []string{base}, pack("k1", 10), "o2", 0, ReserveNoFloorGen, 1000, time.Hour)
		if err != nil || dec.OK || !dec.RenewalDegraded {
			t.Fatalf("fenced second reserve = (%+v, %v), want the fail-closed deny", dec, err)
		}
		if !armed {
			t.Fatal("the arming hook never fired")
		}
		r := mustManifest(t, a, base).Reservations["k1"]
		if r == nil || r.Attempts != 1 {
			t.Fatalf("shared reservation = %+v, want preserved with Attempts=1 (the first pusher's quota cover)", r)
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

	t.Run("existing-object re-registration passes the cap", func(t *testing.T) {
		// Codex round 4 on PR #156: growth is per-invocation, not
		// per-method — re-registering an EXISTING LFS object clears its
		// unreferenced mark in place (the strongest liveness signal)
		// and must never be size-refused while the row stays
		// reclamation-eligible; only a NEW insert grows.
		_, a, _ := newRecoveryStores(t)
		existing := "lfs/" + base + "/" + strings.Repeat("aa", 32)
		fresh := "lfs/" + base + "/" + strings.Repeat("bb", 32)
		if err := a.RegisterLFSObject(ctx, base, existing, 5); err != nil {
			t.Fatalf("seed register: %v", err)
		}
		if n, err := a.MarkLFSObjectsUnreferenced(ctx, base, []string{existing}); err != nil || n != 1 {
			t.Fatalf("mark = (%d, %v)", n, err)
		}
		a.hardCapBytes = 1

		if err := a.RegisterLFSObject(ctx, base, existing, 5); err != nil {
			t.Fatalf("re-registration at the cap = %v, want accepted (non-growing)", err)
		}
		if row := mustManifest(t, a, base).LFS.Objects[existing]; row == nil || row.UnreferencedSince != nil {
			t.Fatalf("row = %+v, want mark cleared by the re-registration", row)
		}
		err := a.RegisterLFSObject(ctx, base, fresh, 5)
		if err == nil || !strings.Contains(err.Error(), "refusing new registrations") {
			t.Fatalf("NEW insert at the cap = %v, want the documented refusal", err)
		}
	})

	t.Run("reservation-only pack settlement passes the cap", func(t *testing.T) {
		// Codex round 5 on PR #156: RegisterPacks growth is
		// per-invocation too — when every pack row already exists,
		// settlement only DELETES the keys' still-live reservations
		// (e.g. a reservation that raced an earlier registration).
		// Refusing that shrink at the hard cap would leave
		// already-accounted bytes reserved until TTL expiry.
		_, a, _ := newRecoveryStores(t)
		if err := a.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "k1", Size: 9}}); err != nil {
			t.Fatalf("seed register: %v", err)
		}
		if dec, err := a.ReservePackBytes(ctx, []string{base}, []Pack{{Key: "k1", Size: 9}}, "o1", 0, ReserveNoFloorGen, math.MaxInt64, time.Hour); err != nil || !dec.OK {
			t.Fatalf("re-mint reservation = (%+v, %v), want granted", dec, err)
		}
		a.hardCapBytes = 1

		if err := a.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "k1", Size: 9}}); err != nil {
			t.Fatalf("idempotent settlement at the cap = %v, want accepted (non-growing)", err)
		}
		if r := mustManifest(t, a, base).Reservations["k1"]; r != nil {
			t.Fatalf("reservation = %+v, want consumed by the settlement", r)
		}
		if err := a.RegisterPacks(ctx, base, []Pack{{ID: "p2", Key: "k2", Size: 9}}); err == nil || !strings.Contains(err.Error(), "refusing new registrations") {
			t.Fatalf("NEW pack insert at the cap = %v, want the documented refusal", err)
		}
	})

	t.Run("zero-growth folds pass the cap", func(t *testing.T) {
		// Codex round 7 on PR #156: a fold REMOVES the old pack rows and
		// converts them to retained tombstones — the only genuinely new
		// entry is a fresh fold row. Folds whose output is already
		// registered and identity folds (which consume their own retained
		// row) net zero or shrink, and refusing them at the hard cap
		// would block exactly the consolidation an oversized manifest
		// needs. Only a net entry ADDITION is growth.
		_, a, _ := newRecoveryStores(t)
		seed := []Pack{{ID: "p1", Key: "k1", Size: 4}, {ID: "p2", Key: "k2", Size: 5}, {ID: "pf", Key: "kf", Size: 9}}
		if err := a.RegisterPacks(ctx, base, seed); err != nil {
			t.Fatalf("seed register: %v", err)
		}
		a.hardCapBytes = 1

		// Fold output already registered: −2 pack rows, +2 retained.
		won, err := a.ReplacePacks(ctx, base, seed[:2], seed[2])
		if err != nil || !won {
			t.Fatalf("registered-output fold at the cap = (%v, %v), want win (non-growing)", won, err)
		}
		mem := mustManifest(t, a, base).Member(base)
		if len(mem.Packs) != 1 || mem.Packs[0].ID != "pf" {
			t.Fatalf("packs = %+v, want only the fold row", mem.Packs)
		}
		if len(mem.Retained) != 2 {
			t.Fatalf("retained = %+v, want the two superseded keys", mem.Retained)
		}

		// Identity fold: the fold row replaces a same-key row and
		// consumes its own retained tombstone — net zero.
		won, err = a.ReplacePacks(ctx, base, []Pack{{ID: "pf", Key: "kf", Size: 9}}, Pack{ID: "pf2", Key: "kf", Size: 9})
		if err != nil || !won {
			t.Fatalf("identity fold at the cap = (%v, %v), want win (net zero)", won, err)
		}

		// A fresh fold row over a live old set is a genuine +1 insert:
		// still refused at the cap.
		_, err = a.ReplacePacks(ctx, base, []Pack{{ID: "pf2", Key: "kf", Size: 9}}, Pack{ID: "pn", Key: "kn", Size: 9})
		if err == nil || !strings.Contains(err.Error(), "refusing new registrations") {
			t.Fatalf("growing fold at the cap = %v, want the documented refusal", err)
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

// TestS3WALRecoveryScanClockGuard (Codex round 8 on PR #156, P1): a
// scan-state stamp AHEAD of the local clock (restart after a backward
// clock step, or an at-boot-slow clock) must not read as age-0 fresh —
// the destructive-reap watermark gates registry retirement, and a stale
// watermark accepted as fresh permits refunds without a recent physical
// reap. Beyond a small tolerance the age is unverifiable: fail closed.
func TestS3WALRecoveryScanClockGuard(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	seed := func(t *testing.T, a *S3WAL, base, key string) {
		t.Helper()
		if err := a.RegisterLFSObject(ctx, base, key, 7); err != nil {
			t.Fatalf("seed register: %v", err)
		}
		if n, err := a.MarkLFSObjectsUnreferenced(ctx, base, []string{key}); err != nil || n != 1 {
			t.Fatalf("mark = (%d, %v)", n, err)
		}
		if err := a.RecordLFSReapComplete(ctx, 1, true); err != nil {
			t.Fatalf("stamp reap watermark: %v", err)
		}
	}

	t.Run("backward clock step demotes the destructive watermark", func(t *testing.T) {
		const base = "r-scan-clock"
		key := "lfs/" + base + "/" + strings.Repeat("aa", 32)
		mc, a, _ := newRecoveryStores(t)
		clock := newFakeClock(t0)
		a.now = clock.now
		mc.Now = clock.now // store clock agrees (§2.4-3 Date witness)
		seed(t, a, base, key)
		// Restart with the wall clock 10 minutes BEHIND the persisted
		// stamp: the watermark is unverifiable, never "fresh now".
		clock.set(t0.Add(-10 * time.Minute))
		retired, err := a.RetireUnreferencedLFSObject(ctx, base, key, map[string]int64{base: 0}, 0, time.Hour)
		if retired || !errors.Is(err, ErrReapWatermarkStale) {
			t.Fatalf("retire under a future watermark = (%v, %v), want ErrReapWatermarkStale (fail closed)", retired, err)
		}
		if age, ok, err := a.JobScanStateAge(ctx, ScanJobSweep, ScanScopeLFSDestructiveReap); err != nil || !ok || age < time.Hour {
			t.Fatalf("watermark age = (%v, %v, %v), want unverifiable-stale", age, ok, err)
		}
	})

	t.Run("sub-tolerance future stamps stay fresh", func(t *testing.T) {
		// ClkNext chains stamps milliseconds ahead of a busy writer's
		// clock and 1s-granular steps are noise against the >=30s
		// freshness windows: inside the tolerance the row reads fresh.
		const base = "r-scan-clock-tol"
		key := "lfs/" + base + "/" + strings.Repeat("bb", 32)
		mc, a, _ := newRecoveryStores(t)
		clock := newFakeClock(t0)
		a.now = clock.now
		mc.Now = clock.now
		seed(t, a, base, key)
		clock.set(t0.Add(-time.Second))
		if retired, err := a.RetireUnreferencedLFSObject(ctx, base, key, map[string]int64{base: 0}, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire inside the tolerance = (%v, %v), want retired", retired, err)
		}
	})
}

// TestS3WALRecoveryLedgerStateFence (Codex round 8 on PR #156): the
// advisory ledger read mirrors the admission-fence marker into
// RenewalDegraded exactly like the SQL twins' EXISTS subquery, so the LFS
// adoption preflight rejects hopeless work BEFORE streaming and hashing
// the legacy object.
func TestS3WALRecoveryLedgerStateFence(t *testing.T) {
	ctx := context.Background()
	const base = "r-ledger-fence"
	_, a, b := newRecoveryStores(t)

	st, err := a.QuotaLedgerState(ctx, []string{base}, time.Hour)
	if err != nil || st.RenewalDegraded {
		t.Fatalf("unfenced ledger state = (%+v, %v), want RenewalDegraded=false", st, err)
	}
	if err := b.ArmAbsenceAdmissionFence(ctx, "lapse-evidence"); err != nil {
		t.Fatalf("arm: %v", err)
	}
	st, err = a.QuotaLedgerState(ctx, []string{base}, time.Hour)
	if err != nil || !st.RenewalDegraded {
		t.Fatalf("fenced ledger state = (%+v, %v), want RenewalDegraded=true", st, err)
	}
	if err := a.DeleteJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence); err != nil {
		t.Fatalf("disarm: %v", err)
	}
	st, err = a.QuotaLedgerState(ctx, []string{base}, time.Hour)
	if err != nil || st.RenewalDegraded {
		t.Fatalf("disarmed ledger state = (%+v, %v), want RenewalDegraded=false", st, err)
	}
}

// TestS3WALRecoveryLapseMarkerContention (Codex round 8 on PR #156): the
// fleet-global lapse-evidence marker is legitimately upserted by
// CONCURRENT receive-hook processes (each a fresh S3WAL instance — the
// per-key mutex cannot serialize them), so a raced conditional write is
// contention to retry, not dual-serve evidence: the single-writer failure
// policy would propagate out of ReservePackBytes and reject a valid push.
func TestS3WALRecoveryLapseMarkerContention(t *testing.T) {
	ctx := context.Background()
	mapKey := AbsenceCoverPrefix + "r-lapse-race/lfs-e0000000001" + RepairCoverSuffix

	mc, a, b := newRecoveryStores(t)
	// Two prior versions from a concurrent hook process give the
	// stale-read hook history to serve — a's advance then carries a
	// superseded ETag, the deterministic 412.
	if err := b.PutJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse, "absence-lost/other/lfs-e0000000001", 0); err != nil {
		t.Fatalf("competitor upsert 1: %v", err)
	}
	if err := b.PutJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse, "absence-lost/other/lfs-e0000000002", 0); err != nil {
		t.Fatalf("competitor upsert 2: %v", err)
	}
	lapseKey := s3walScanKey(ScanJobAbsence, ScanScopeAbsenceLapse)
	armed := true
	mc.Hooks.StaleGet = func(key string) bool {
		if armed && key == lapseKey {
			armed = false
			return true
		}
		return false
	}
	if err := a.s3walRecordLapse(ctx, mapKey); err != nil {
		t.Fatalf("raced lapse upsert = %v, want retried success (idempotent multi-writer marker)", err)
	}
	st, ok, err := a.GetJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse)
	if err != nil || !ok || st.Payload != s3walCoverRaw(mapKey) {
		t.Fatalf("marker = (%+v, %v, %v), want our cover's raw spelling", st, ok, err)
	}
}

// TestS3WALRecoveryDefinitiveErrorTaxonomy (Codex round 9 on PR #156):
// mutate's definitive failures (rejected PUTs, never-sent attempts) must
// carry the not-executed marker or IdempotentCAS's ambiguity recheck can
// recover them: with a competitor meanwhile placing `new`, the point read
// shows current == new and fabricates won=true — a successful update and
// its webhook — for a PUT the store definitively rejected.
func TestS3WALRecoveryDefinitiveErrorTaxonomy(t *testing.T) {
	ctx := context.Background()
	const base = "r-taxonomy"
	const branch = "refs/heads/main"
	sha1, sha2 := testSHA(0x11), testSHA(0x22)

	t.Run("definitive rejection is never recovered as a win", func(t *testing.T) {
		mc, a, b := newRecoveryStores(t)
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("seed = (%v, %v)", won, err)
		}
		// Our PUT is DEFINITIVELY rejected (exhausted 409 contention from
		// the conditional client); the competitor then commits the same
		// transition before the IdempotentCAS recheck reads.
		armPutFault(mc, s3wal.FaultContention)
		armAfterPut(mc, func() {
			if won, err := b.CASRef(ctx, base, branch, sha1, sha2); err != nil || !won {
				t.Errorf("competitor CAS = (%v, %v)", won, err)
			}
		})
		won, err := IdempotentCAS{RefStore: a}.CASRef(ctx, base, branch, sha1, sha2)
		if won {
			t.Fatal("IdempotentCAS recovered a definitively rejected PUT as won: fabricated update")
		}
		if !errors.Is(err, s3wal.ErrCASContention) {
			t.Fatalf("err = %v, want the surfaced definitive failure", err)
		}
	})

	t.Run("never-sent refusals carry the not-executed marker", func(t *testing.T) {
		_, a, _ := newRecoveryStores(t)
		if won, err := a.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("seed = (%v, %v)", won, err)
		}
		a.hardCapBytes = 1
		_, err := a.CASRef(ctx, base, "refs/heads/new", ZeroSHA, sha1)
		var ne interface{ SafeToRetry() bool }
		if err == nil || !errors.As(err, &ne) || !ne.SafeToRetry() {
			t.Fatalf("size-cap refusal = %v, want the not-executed marker (never sent)", err)
		}
	})
}

// TestS3WALRecoveryLockTableBounded (Codex round 11 on PR #156): the
// per-key mutex table must not retain an entry per base/scan key forever —
// LFS refund scopes embed a rotating reap epoch and manifests come and go,
// so a long-lived server would grow the map without bound. Entries are
// refcounted and retired on release.
func TestS3WALRecoveryLockTableBounded(t *testing.T) {
	_, a, _ := newRecoveryStores(t)
	unlockA := a.lockBase("bounded-a")
	unlockScan := a.lockScan("scan-key")
	a.mu.Lock()
	if n := len(a.locks); n != 2 {
		a.mu.Unlock()
		t.Fatalf("held locks = %d entries, want 2 (distinct keys stay distinct)", n)
	}
	a.mu.Unlock()
	unlockA()
	unlockScan()
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := len(a.locks); n != 0 {
		t.Fatalf("released locks leave %d table entries, want 0 (unbounded growth)", n)
	}
}

// listCountingStore wraps MemCAS and counts fleet-walk listing calls.
type listCountingStore struct {
	*s3wal.MemCAS
	lists int
}

func (c *listCountingStore) List(ctx context.Context, prefix, after string, limit int) ([]string, error) {
	c.lists++
	return c.MemCAS.List(ctx, prefix, after, limit)
}

// TestS3WALRecoveryReconcileCapStopsWalk (Codex round 11 on PR #156):
// once the reconcile invocation cap is reached, the fleet walk itself must
// stop — returning nil per base keeps listing every remaining manifest
// page, defeating the cap's bound on startup work.
func TestS3WALRecoveryReconcileCapStopsWalk(t *testing.T) {
	ctx := context.Background()
	oldPages := reconcileLegacyLFSMaxPages
	reconcileLegacyLFSMaxPages = 1
	t.Cleanup(func() { reconcileLegacyLFSMaxPages = oldPages })

	cs := &listCountingStore{MemCAS: s3wal.NewMemCAS()}
	a := newS3WALStore(cs, nil, nil)
	a.backoffBase, a.backoffCap = time.Microsecond, time.Millisecond

	// One reconcilable twin pair on the byte-order-first base (raw rows:
	// the public register canonicalizes legacy keys, so genuine twins
	// only exist as pre-migration history), then enough manifests to
	// force a second listing page (walk page size 512).
	oid := strings.Repeat("cd", 32)
	first := "recona000"
	recoveryEdit(t, a, first, func(m *s3wal.Manifest, clk time.Time) {
		m.LFS.Objects = map[string]*s3wal.LFSObject{
			"lfs/" + first + "/" + oid:    {Bytes: 5, CreatedAt: clk},
			"lfs/" + first + "/o2/" + oid: {Bytes: 5, CreatedAt: clk},
		}
		m.LFS.Count = 2
		m.UsageBytes = 10
	})
	for i := 1; i <= 512; i++ {
		base := fmt.Sprintf("recona%03d", i)
		if won, err := a.CASRef(ctx, base, "refs/heads/main", ZeroSHA, testSHA(0x33)); err != nil || !won {
			t.Fatalf("seed base %s = (%v, %v)", base, won, err)
		}
	}
	cs.lists = 0
	n, partial, err := a.ReconcileLegacyLFSObjects(ctx)
	if err != nil || n != 1 || !partial {
		t.Fatalf("reconcile = (%d, %v, %v), want (1, true, nil): capped partial success", n, partial, err)
	}
	if cs.lists != 1 {
		t.Fatalf("fleet walk issued %d listing pages after the cap, want 1 (the cap must stop the walk)", cs.lists)
	}
}

// TestS3WALRecoveryDeleteLeaseCapExempt (Codex round 11 on PR #156): the
// transient delete lease is DELETION machinery — an over-cap manifest
// (reachable through zero-growth mutations) must still be able to reap
// unregistered LFS blobs, or reclamation wedges until an unrelated
// mutation shrinks the manifest.
func TestS3WALRecoveryDeleteLeaseCapExempt(t *testing.T) {
	ctx := context.Background()
	const base = "r-lease-cap"
	key := "lfs/" + base + "/" + strings.Repeat("ee", 32)
	_, a, _ := newRecoveryStores(t)
	if won, err := a.CASRef(ctx, base, "refs/heads/main", ZeroSHA, testSHA(0x11)); err != nil || !won {
		t.Fatalf("seed = (%v, %v)", won, err)
	}
	a.hardCapBytes = 1
	deleted, err := a.GuardedLFSDelete(ctx, base, key, func(context.Context) error { return nil })
	if err != nil || !deleted {
		t.Fatalf("guarded delete at the cap = (%v, %v), want deleted (lease acquisition is cap-exempt)", deleted, err)
	}
	if locks := mustManifest(t, a, base).DeleteLocks; len(locks) != 0 {
		t.Fatalf("delete locks = %+v, want cleared after the delete", locks)
	}
}

// noDateStore hides MemCAS's Date witness: a compatible endpoint that
// omits or mangles the HTTP Date header.
type noDateStore struct{ *s3wal.MemCAS }

func (noDateStore) StoreDate() (time.Time, time.Time, bool) {
	return time.Time{}, time.Time{}, false
}

// TestS3WALRecoveryInitRequiresDateWitness (Codex round 11 on PR #156): a
// store that passes the 5-step conditional probe but supplies no usable
// Date header would leave every §2.4-3 destructive expiry verdict
// permanently report-only (crashed reservations stay charged, claims and
// delete locks never expire). Initialization must refuse to serve it.
func TestS3WALRecoveryInitRequiresDateWitness(t *testing.T) {
	ctx := context.Background()

	t.Run("no witness = refuse to serve", func(t *testing.T) {
		mc := s3wal.NewMemCAS()
		a := newS3WALStore(noDateStore{mc}, func(context.Context) error { return nil }, nil)
		if err := a.InitSchema(ctx); err == nil {
			t.Fatal("InitSchema accepted a store with no Date witness: expiry verdicts would be permanently report-only")
		}
	})

	t.Run("fresh witness = serve", func(t *testing.T) {
		mc := s3wal.NewMemCAS()
		probe := func(ctx context.Context) error {
			_, err := mc.Get(ctx, "_wal/probe/witness")
			if !errors.Is(err, s3wal.ErrNotExist) {
				return err
			}
			return nil // the exchange observed a Date witness
		}
		a := newS3WALStore(mc, probe, nil)
		if err := a.InitSchema(ctx); err != nil {
			t.Fatalf("InitSchema with a fresh witness = %v, want nil", err)
		}
	})
}

// TestS3WALRecoveryPurgeClearsDeleteLeases (Codex round 11 on PR #156):
// the purge fence makes delete leases permanently unnecessary (fenced
// repos delete directly and registrations can never commit), so the
// terminal ledger cleanup must clear them — the burned-name tombstone
// stays small instead of retaining stale lease state forever.
func TestS3WALRecoveryPurgeClearsDeleteLeases(t *testing.T) {
	ctx := context.Background()
	const base = "r-purge-leases"
	key := "lfs/" + base + "/" + strings.Repeat("ff", 32)
	_, a, _ := newRecoveryStores(t)
	recoveryEdit(t, a, base, func(m *s3wal.Manifest, clk time.Time) {
		m.DeleteLocks = map[string]*s3wal.DeleteLock{
			key: {Owner: "crashed", OpID: s3wal.NewOpID(), CreatedAt: clk},
		}
	})
	if err := a.FenceRepoPurge(ctx, base); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if n, err := a.PurgeRepoLedger(ctx, []string{base}); err != nil || n != 1 {
		t.Fatalf("purge ledger = (%d, %v), want the leaked lease counted as 1 cleared row", n, err)
	}
	if locks := mustManifest(t, a, base).DeleteLocks; len(locks) != 0 {
		t.Fatalf("delete locks = %+v, want cleared by the ledger purge", locks)
	}
	if n, err := a.PurgeRepoLedger(ctx, []string{base}); err != nil || n != 0 {
		t.Fatalf("purge rerun = (%d, %v), want idempotent 0", n, err)
	}
}
