// §2.5 crash-window matrix (plan §4 Phase 2): one subtest per window
// W1–W9. Each scenario runs its pipeline UP TO the kill point — a crash
// is simply the absence of the remaining calls, because every driver op
// is one manifest CAS and there is no torn intra-op state to model —
// then "restarts" (a fresh driver instance over the same MemCAS: locks,
// seq watermarks, and in-flight state die with the process; the bucket
// survives), runs the documented recovery action, and asserts the §2.5
// invariants:
//
//	P  publish-before-CAS: committed refs never point at unavailable
//	   packs — every registered pack row's blob key exists.
//	T  tombstone-before-delete: pack bytes are deleted only while a
//	   retained tombstone still counts them.
//	Q  quota oracle: usage == registered packs + retained + LFS bytes.
//	A  absence covers (live reservation rows) survive unrelated
//	   recovery; only the purge terminal CAS clears them.
//
// W10 (meta/ref cross-store windows) is Phase 3's zero-infra serve mode
// and intentionally absent here. Deep per-predicate recovery coverage
// lives in s3walstore_recovery_test.go; this file is the requested
// end-to-end kill/restart enumeration in one place.
package ref

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/open-storage/internal/ref/s3wal"
)

// crashEnv is one crash-window fixture: the shared "bucket" (MemCAS), the
// live driver, and the modeled blob store (key -> bytes).
type crashEnv struct {
	t     *testing.T
	mc    *s3wal.MemCAS
	s     *S3WAL
	blobs map[string]int64
}

func newCrashEnv(t *testing.T) *crashEnv {
	t.Helper()
	mc, a, _ := newRecoveryStores(t)
	return &crashEnv{t: t, mc: mc, s: a, blobs: make(map[string]int64)}
}

// restart replaces the driver with a fresh instance over the same MemCAS
// — the process-death + reboot seam.
func (e *crashEnv) restart() {
	e.t.Helper()
	e.s = newS3WALStore(e.mc, nil, nil)
	e.s.backoffBase = time.Microsecond
	e.s.backoffCap = time.Millisecond
}

// upload models the pack/LFS blob PUT that precedes every registration.
func (e *crashEnv) upload(key string, size int64) {
	e.t.Helper()
	assertf(key != "" && size > 0, "crashEnv.upload(%q, %d)", key, size)
	e.blobs[key] = size
}

// deleteBlob models a guarded blob DELETE and enforces T at the moment of
// deletion: the key must be tombstoned (retained) or entirely
// unregistered — never a live registered pack row.
func (e *crashEnv) deleteBlob(base, key string) {
	e.t.Helper()
	m := mustManifest(e.t, e.s, base)
	for repo, mem := range m.Members {
		for _, p := range mem.Packs {
			if p.Key == key {
				e.t.Fatalf("T violated: deleting blob %s while %s still registers it", key, repo)
			}
		}
	}
	delete(e.blobs, key)
}

// assertPQ asserts P and Q over base's manifest (T is enforced at delete
// time by deleteBlob; A by the explicit cover checks). Q sums the MODELED
// blob sizes for live pack/LFS rows — not the manifest's own recorded
// bytes, which would make the equation self-referential and blind to a
// registration recording wrong counts consistently (Codex round 4 on PR
// #159). Retained tombstones keep manifest-recorded bytes: their blobs
// are legally deleted under T.
func (e *crashEnv) assertPQ(base string) {
	e.t.Helper()
	m := mustManifest(e.t, e.s, base)
	var packs, retained int64
	for repo, mem := range m.Members {
		for _, p := range mem.Packs {
			size, ok := e.blobs[p.Key]
			if !ok {
				e.t.Errorf("P violated: %s registers pack %s (key %s) with no blob", repo, p.ID, p.Key)
				continue
			}
			if p.Bytes != size {
				e.t.Errorf("Q violated: %s pack %s records %d bytes, modeled blob is %d", repo, p.ID, p.Bytes, size)
			}
			packs += size
		}
		for _, r := range mem.Retained {
			retained += r.Bytes
		}
	}
	var lfs int64
	for key, o := range m.LFS.Objects {
		size, ok := e.blobs[key]
		if !ok {
			e.t.Errorf("P violated: registered LFS object %s has no blob", key)
			continue
		}
		if o.Bytes != size {
			e.t.Errorf("Q violated: LFS object %s records %d bytes, modeled blob is %d", key, o.Bytes, size)
		}
		lfs += size
	}
	if want := packs + retained + lfs; m.UsageBytes != want {
		e.t.Errorf("Q violated: usage %d != packs %d + retained %d + lfs %d", m.UsageBytes, packs, retained, lfs)
	}
}

// seedCover plants one live absence-cover reservation row (invariant A's
// subject) and returns its map key.
func (e *crashEnv) seedCover(base string) string {
	e.t.Helper()
	key := AbsenceCoverPrefix + base + "/lfs-e0000000001" + RepairCoverSuffix
	recoveryEdit(e.t, e.s, base, func(m *s3wal.Manifest, clk time.Time) {
		if m.Reservations == nil {
			m.Reservations = make(map[string]*s3wal.Reservation)
		}
		m.Reservations[key] = &s3wal.Reservation{Owner: "cover-owner", Bytes: 33, Attempts: 1, CreatedAt: clk}
	})
	return key
}

// wantCover asserts the seeded cover row's presence (invariant A).
func (e *crashEnv) wantCover(base, key string, present bool) {
	e.t.Helper()
	m := mustManifest(e.t, e.s, base)
	if _, ok := m.Reservations[key]; ok != present {
		e.t.Errorf("A violated: cover %s present=%v, want %v", key, ok, present)
	}
}

// packRows returns repo's registered pack IDs.
func (e *crashEnv) packRows(base, repo string) []Pack {
	e.t.Helper()
	packs, err := e.s.ListPacks(context.Background(), repo)
	if err != nil {
		e.t.Fatalf("ListPacks(%s): %v", repo, err)
	}
	return packs
}

func TestS3WALCrashWindows(t *testing.T) {
	ctx := context.Background()
	const (
		branch = "refs/heads/main"
		sha1   = "1111111111111111111111111111111111111111"
		sha2   = "2222222222222222222222222222222222222222"
		ttl    = time.Hour
	)

	t.Run("W1 killed after blob PUT before RegisterPacks", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w1repo"
		cover := e.seedCover(base)
		dec, err := e.s.ReservePackBytes(ctx, []string{base}, []Pack{{Key: "packs/w1/k1", Size: 10}}, "o1", 0, ReserveNoFloorGen, 1000, ttl)
		if err != nil || !dec.OK {
			t.Fatalf("ReservePackBytes = (%+v, %v)", dec, err)
		}
		e.upload("packs/w1/k1", 10)
		// CRASH before RegisterPacks; restart.
		e.restart()
		// P: nothing registered; the ref never moved. Q: usage untouched
		// (reservations are charged separately until TTL).
		e.assertPQ(base)
		if rows := e.packRows(base, base); len(rows) != 0 {
			t.Fatalf("orphan blob got registered: %+v", rows)
		}
		m := mustManifest(t, e.s, base)
		if m.Reservations["packs/w1/k1"] == nil {
			t.Fatal("crashed push's reservation vanished before its TTL")
		}
		// Recovery: age the row past the sweep window, then sweep. The
		// live cover row (A) must survive the same sweep.
		recoveryEdit(t, e.s, base, func(m *s3wal.Manifest, clk time.Time) {
			r := m.Reservations["packs/w1/k1"]
			r.CreatedAt = r.CreatedAt.Add(-2 * ttl)
		})
		if _, err := e.s.SweepExpiredPackReservations(ctx, ttl); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		m = mustManifest(t, e.s, base)
		if m.Reservations["packs/w1/k1"] != nil {
			t.Fatal("expired reservation survived the sweep")
		}
		e.wantCover(base, cover, true)
		e.assertPQ(base)
		// The orphan blob itself is the blob sweeper's job; the manifest
		// must simply never reference it.
	})

	t.Run("W2 RegisterPacks CAS ambiguous, killed, re-applied", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w2repo"
		dec, err := e.s.ReservePackBytes(ctx, []string{base}, []Pack{{Key: "packs/w2/k1", Size: 10}}, "o1", 0, ReserveNoFloorGen, 1000, ttl)
		if err != nil || !dec.OK {
			t.Fatalf("ReservePackBytes = (%+v, %v)", dec, err)
		}
		e.upload("packs/w2/k1", 10)
		// The registration CAS lands but the response is lost, and the
		// process dies before its own recovery read.
		armPutFault(e.mc, s3wal.FaultAmbiguousApplied)
		err = e.s.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "packs/w2/k1", Size: 10}})
		if err == nil {
			// The driver may prove its own write via the fresh nonce —
			// also a legal outcome of this window.
			t.Log("driver self-recovered the ambiguous registration")
		}
		e.restart()
		// Client retry after reboot: idempotent re-apply.
		if err := e.s.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "packs/w2/k1", Size: 10}}); err != nil {
			t.Fatalf("re-applied RegisterPacks: %v", err)
		}
		rows := e.packRows(base, base)
		if len(rows) != 1 || rows[0].ID != "p1" {
			t.Fatalf("want exactly one registered pack, got %+v", rows)
		}
		m := mustManifest(t, e.s, base)
		if m.Reservations["packs/w2/k1"] != nil {
			t.Fatal("reservation not consumed exactly once")
		}
		if m.UsageBytes != 10 {
			t.Fatalf("usage %d, want 10 (credited exactly once)", m.UsageBytes)
		}
		e.assertPQ(base)
	})

	t.Run("W3 killed between RegisterPacks and CASRef", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w3repo"
		e.upload("packs/w3/k1", 10)
		if err := e.s.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "packs/w3/k1", Size: 10}}); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
		// CRASH before CASRef; restart.
		e.restart()
		// P: the ref never moved — identical to the SQL window.
		if _, err := e.s.GetRef(ctx, base, branch); !errors.Is(err, ErrNotFound) {
			t.Fatalf("ref moved without CASRef: err=%v", err)
		}
		e.assertPQ(base)
		// Retry re-runs the CAS; the registered pack is already durable.
		if won, err := e.s.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("retry CASRef = (%v, %v)", won, err)
		}
		e.assertPQ(base)
	})

	t.Run("W4 CASRef ambiguous, killed, retried through IdempotentCAS", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w4repo"
		e.upload("packs/w4/k1", 10)
		if err := e.s.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "packs/w4/k1", Size: 10}}); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
		if won, err := e.s.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("seed CASRef = (%v, %v)", won, err)
		}
		// The update CAS lands, the response is lost, the process dies
		// before the driver's recovery read resolves it.
		armPutFault(e.mc, s3wal.FaultAmbiguousApplied)
		if won, err := e.s.CASRef(ctx, base, branch, sha1, sha2); err == nil && won {
			t.Log("driver self-recovered the ambiguous CAS — also legal")
		}
		e.restart()
		// The rebooted client retries the same transition through the
		// production decorator: the write already landed, so the raw CAS
		// reports a clean conflict, and IdempotentCAS's point-read
		// recheck must recover it to a win (issue #2's contract).
		won, err := IdempotentCAS{e.s}.CASRef(ctx, base, branch, sha1, sha2)
		if err != nil || !won {
			t.Fatalf("post-restart retry = (%v, %v), want recovered win", won, err)
		}
		if got, err := e.s.GetRef(ctx, base, branch); err != nil || got != sha2 {
			t.Fatalf("GetRef = (%q, %v), want %s", got, err, sha2)
		}
		e.assertPQ(base)
	})

	t.Run("W5 ReplacePacks ambiguous fold, restart, tombstoned reclaim", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w5repo"
		e.upload("packs/w5/k1", 10)
		e.upload("packs/w5/k2", 20)
		if err := e.s.RegisterPacks(ctx, base, []Pack{
			{ID: "p1", Key: "packs/w5/k1", Size: 10}, {ID: "p2", Key: "packs/w5/k2", Size: 20},
		}); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
		e.upload("packs/w5/fold", 25)
		// ONE CAS drops the old rows, inserts the fold, tombstones the
		// old keys; its response is lost.
		armPutFault(e.mc, s3wal.FaultAmbiguousApplied)
		won, err := e.s.ReplacePacks(ctx, base, []Pack{
			{ID: "p1", Key: "packs/w5/k1", Size: 10}, {ID: "p2", Key: "packs/w5/k2", Size: 20},
		}, Pack{ID: "pf", Key: "packs/w5/fold", Size: 25})
		if err != nil || !won {
			// The driver's own recovery must prove the fold applied
			// (output present AND all old rows gone) — single-object
			// atomicity means no partial fold can exist.
			t.Fatalf("ReplacePacks under ambiguous CAS = (%v, %v)", won, err)
		}
		e.restart()
		// T+Q: old bytes remain counted as retained tombstones.
		m := mustManifest(t, e.s, base)
		mem := m.Members[base]
		if mem == nil || len(mem.Packs) != 1 || mem.Packs[0].ID != "pf" {
			t.Fatalf("fold not the sole registered pack: %+v", mem)
		}
		if mem.Retained["packs/w5/k1"].Bytes != 10 || mem.Retained["packs/w5/k2"].Bytes != 20 {
			t.Fatalf("old keys not tombstoned: %+v", mem.Retained)
		}
		e.assertPQ(base)
		// Sweep half: quarantine-first blob delete (T holds: tombstone
		// exists strictly before the delete), then the reclaim CAS.
		e.deleteBlob(base, "packs/w5/k1")
		if n, err := e.s.ReclaimRetainedPack(ctx, base, "packs/w5/k1"); err != nil || n != 10 {
			t.Fatalf("ReclaimRetainedPack = (%d, %v)", n, err)
		}
		// W6 (pack side): killed between blob delete and reclaim — the
		// tombstone plus bytes remain, a conservative over-count healed
		// by the next pass re-running reclaim idempotently.
		e.deleteBlob(base, "packs/w5/k2")
		e.restart()
		e.assertPQ(base)
		if n, err := e.s.ReclaimRetainedPack(ctx, base, "packs/w5/k2"); err != nil || n != 20 {
			t.Fatalf("healed ReclaimRetainedPack = (%d, %v)", n, err)
		}
		if n, err := e.s.ReclaimRetainedPack(ctx, base, "packs/w5/k2"); err != nil || n != 0 {
			t.Fatalf("re-run ReclaimRetainedPack = (%d, %v), want 0 (idempotent)", n, err)
		}
		e.assertPQ(base)
	})

	t.Run("W6 GuardedLFSDelete killed around the delete", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w6repo"
		const key = "lfs/w6repo/oid1"

		// Shrink the lease protocol to test scale (same knob the
		// recovery suite uses).
		oldTTL, oldIO, oldSafety, oldPoll := s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll
		s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll =
			40*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, time.Millisecond
		t.Cleanup(func() {
			s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety, s3walDeleteLockPoll = oldTTL, oldIO, oldSafety, oldPoll
		})

		// (a) Killed BEFORE the DELETE was sent: the stale lock briefly
		// blocks registration, then expires — the safe direction.
		recoveryEdit(t, e.s, base, func(m *s3wal.Manifest, clk time.Time) {
			m.DeleteLocks = map[string]*s3wal.DeleteLock{
				key: {Owner: "crashed-proc", OpID: s3wal.NewOpID(), CreatedAt: clk},
			}
		})
		e.restart()
		blockedCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
		err := e.s.RegisterLFSObject(blockedCtx, base, key, 5)
		cancel()
		if err == nil {
			t.Fatal("registration ignored a live delete lease")
		}
		// After the FULL blocking window the lease no longer fences.
		time.Sleep(s3walDeleteLockTTL + s3walDeleteIOTimeout + s3walDeleteLockSafety + 10*time.Millisecond)
		e.upload(key, 5)
		if err := e.s.RegisterLFSObject(ctx, base, key, 5); err != nil {
			t.Fatalf("post-expiry registration: %v", err)
		}
		e.assertPQ(base)

		// (b) Killed BETWEEN the DELETE and the lease clear: the modeled
		// del removes the bytes and then reports the crash (the driver
		// LEAVES the acquired lease on a failed delete — the blocking
		// window must keep fencing the possibly-in-flight DELETE), so
		// the REAL lease survives into the restart (Codex round 2 on PR
		// #159: a synthetic re-planted lock would not catch a regression
		// that clears the real one early). The target is an orphan
		// (never-registered) key — the exclusion refuses to delete
		// registered bytes at all, which W6 exists to guarantee.
		const key2 = "lfs/w6repo/oid2"
		e.upload(key2, 6)
		deleted, err := e.s.GuardedLFSDelete(ctx, base, key2, func(context.Context) error {
			e.deleteBlob("w6repo", key2)
			return errors.New("process died mid-delete")
		})
		if err == nil || deleted {
			t.Fatalf("GuardedLFSDelete = (%v, %v), want the crash surfaced", deleted, err)
		}
		if m := mustManifest(t, e.s, base); m.DeleteLocks[key2] == nil {
			t.Fatalf("crashed delete's lease missing: %+v", m.DeleteLocks)
		}
		e.restart()
		// Within the blocking window the crashed delete's lease still
		// fences registration.
		blockedCtx2, cancel2 := context.WithTimeout(ctx, 15*time.Millisecond)
		err = e.s.RegisterLFSObject(blockedCtx2, base, key2, 6)
		cancel2()
		if err == nil {
			t.Fatal("registration ignored the crashed delete's live lease")
		}
		// Past the FULL window the sweeper reaps the leaked lease and
		// registration proceeds.
		time.Sleep(s3walDeleteLockTTL + s3walDeleteIOTimeout + s3walDeleteLockSafety + 10*time.Millisecond)
		if _, err := e.s.SweepExpiredPackReservations(ctx, ttl); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if m := mustManifest(t, e.s, base); len(m.DeleteLocks) != 0 {
			t.Fatalf("leaked lease survived the sweep: %+v", m.DeleteLocks)
		}
		e.upload(key2, 6)
		if err := e.s.RegisterLFSObject(ctx, base, key2, 6); err != nil {
			t.Fatalf("registration after leaked lease reap: %v", err)
		}
		e.assertPQ(base)
	})

	t.Run("W7 killed after the purge fence", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w7repo"
		e.upload("packs/w7/k1", 10)
		if err := e.s.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "packs/w7/k1", Size: 10}}); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
		if won, err := e.s.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("CASRef = (%v, %v)", won, err)
		}
		if err := e.s.FenceRepoPurge(ctx, base); err != nil {
			t.Fatalf("FenceRepoPurge: %v", err)
		}
		// CRASH; restart. The fence lives in the same object every write
		// CASes, so all row-creating writes reject.
		e.restart()
		if err := e.s.RegisterPacks(ctx, base, []Pack{{ID: "p2", Key: "packs/w7/k2", Size: 1}}); !errors.Is(err, ErrRepoPurged) {
			t.Fatalf("RegisterPacks after fence = %v, want ErrRepoPurged", err)
		}
		if _, err := e.s.CASRef(ctx, base, branch, sha1, sha2); !errors.Is(err, ErrRepoPurged) {
			t.Fatalf("CASRef after fence = %v, want ErrRepoPurged", err)
		}
		if err := e.s.RegisterLFSObject(ctx, base, "lfs/w7repo/o1", 1); !errors.Is(err, ErrRepoPurged) {
			t.Fatalf("RegisterLFSObject after fence = %v, want ErrRepoPurged", err)
		}
		// The purge re-run continues from the fence.
		if _, err := e.s.DropRepoRefs(ctx, []string{base}); err != nil {
			t.Fatalf("DropRepoRefs: %v", err)
		}
		if _, err := e.s.GetRef(ctx, base, branch); !errors.Is(err, ErrNotFound) {
			t.Fatal("refs survived DropRepoRefs")
		}
	})

	t.Run("W8 killed mid purge pipeline, re-run to terminal", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w8repo"
		cover := e.seedCover(base)
		e.upload("packs/w8/k1", 10)
		if err := e.s.RegisterPacks(ctx, base, []Pack{{ID: "p1", Key: "packs/w8/k1", Size: 10}}); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
		if won, err := e.s.CASRef(ctx, base, branch, ZeroSHA, sha1); err != nil || !won {
			t.Fatalf("CASRef = (%v, %v)", won, err)
		}
		e.upload("lfs/w8repo/o1", 7)
		if err := e.s.RegisterLFSObject(ctx, base, "lfs/w8repo/o1", 7); err != nil {
			t.Fatalf("RegisterLFSObject: %v", err)
		}
		group := []string{base}
		if err := e.s.FenceRepoPurge(ctx, base); err != nil {
			t.Fatalf("FenceRepoPurge: %v", err)
		}
		if _, err := e.s.DropRepoRefs(ctx, group); err != nil {
			t.Fatalf("DropRepoRefs: %v", err)
		}
		// CRASH between phases; restart; the job re-runs every phase —
		// all idempotent behind the fence.
		e.restart()
		if _, err := e.s.DropRepoRefs(ctx, group); err != nil {
			t.Fatalf("re-run DropRepoRefs: %v", err)
		}
		if _, err := e.s.RetireRepoPacks(ctx, group); err != nil {
			t.Fatalf("RetireRepoPacks: %v", err)
		}
		if _, err := e.s.DropRepoLFSObjects(ctx, group); err != nil {
			t.Fatalf("DropRepoLFSObjects: %v", err)
		}
		// Blob reap happens HERE, before the terminal ledger purge —
		// jobs/purge.go phases 4 and 6 — so T is load-bearing: the pack
		// blob is deleted strictly while its retained tombstone still
		// exists (Codex on PR #159). Purge-retained tombstones carry
		// bytes=0 by design: the terminal ledger purge zeroes the whole
		// group's usage rather than debiting per row.
		if m := mustManifest(t, e.s, base); m.Members[base] == nil || len(m.Members[base].Packs) != 0 {
			t.Fatalf("packs not retired before blob reap: %+v", m.Members[base])
		} else if _, ok := m.Members[base].Retained["packs/w8/k1"]; !ok {
			t.Fatalf("pack not tombstoned before blob reap: %+v", m.Members[base].Retained)
		}
		e.deleteBlob(base, "packs/w8/k1")
		e.deleteBlob(base, "lfs/w8repo/o1")
		if n, err := e.s.CountRepoRows(ctx, group); err != nil || n != 0 {
			t.Fatalf("CountRepoRows = (%d, %v), want 0", n, err)
		}
		if _, err := e.s.PurgeRepoLedger(ctx, group); err != nil {
			t.Fatalf("PurgeRepoLedger: %v", err)
		}
		m := mustManifest(t, e.s, base)
		if m.PurgedAt == nil {
			t.Fatal("terminal purge left no tombstone stamp")
		}
		if m.UsageBytes != 0 || len(m.Reservations) != 0 {
			t.Fatalf("ledger not cleared: usage=%d reservations=%+v", m.UsageBytes, m.Reservations)
		}
		// A: the purge terminal CAS is the ONE recovery that clears
		// cover rows.
		e.wantCover(base, cover, false)
		e.assertPQ(base)
	})

	t.Run("W9 LFS register ambiguous, killed, re-applied", func(t *testing.T) {
		e := newCrashEnv(t)
		const base = "w9repo"
		const key = "lfs/w9repo/oid1"
		dec, err := e.s.ReservePackBytes(ctx, []string{base}, []Pack{{Key: key, Size: 5}}, "o1", 0, ReserveNoFloorGen, 1000, ttl)
		if err != nil || !dec.OK {
			t.Fatalf("ReservePackBytes = (%+v, %v)", dec, err)
		}
		e.upload(key, 5)
		// Reservation consume + row insert + count + usage + adoption-gen
		// bump land in ONE CAS whose response is lost; the process dies.
		armPutFault(e.mc, s3wal.FaultAmbiguousApplied)
		if err := e.s.RegisterLFSObject(ctx, base, key, 5); err == nil {
			t.Log("driver self-recovered the ambiguous LFS registration")
		}
		e.restart()
		// Idempotent re-apply after reboot.
		if err := e.s.RegisterLFSObject(ctx, base, key, 5); err != nil {
			t.Fatalf("re-applied RegisterLFSObject: %v", err)
		}
		if n, err := e.s.CountLFSObjects(ctx, base); err != nil || n != 1 {
			t.Fatalf("CountLFSObjects = (%d, %v), want 1", n, err)
		}
		m := mustManifest(t, e.s, base)
		if m.UsageBytes != 5 {
			t.Fatalf("usage %d, want 5 (credited exactly once)", m.UsageBytes)
		}
		if m.Reservations[key] != nil {
			t.Fatal("reservation not consumed exactly once")
		}
		e.assertPQ(base)
	})
}
