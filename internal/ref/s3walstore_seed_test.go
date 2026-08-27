package ref

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// opCountingStore counts Get/Create/Advance round trips through a MemCAS —
// the unit-level twin of the CODESTORAGE_S3_OP_LOG census (issue #182).
type opCountingStore struct {
	*s3wal.MemCAS
	gets, puts atomic.Int64
}

func (s *opCountingStore) Get(ctx context.Context, key string) (s3wal.Object, error) {
	s.gets.Add(1)
	return s.MemCAS.Get(ctx, key)
}

func (s *opCountingStore) Create(ctx context.Context, key string, body []byte) (string, error) {
	s.puts.Add(1)
	return s.MemCAS.Create(ctx, key, body)
}

func (s *opCountingStore) Advance(ctx context.Context, key string, body []byte, etag string) (string, error) {
	s.puts.Add(1)
	return s.MemCAS.Advance(ctx, key, body, etag)
}

func newSeedStores(t *testing.T) (*opCountingStore, *S3WAL, *S3WAL) {
	t.Helper()
	cs := &opCountingStore{MemCAS: s3wal.NewMemCAS()}
	a := newS3WALStore(cs, nil, nil)
	b := newS3WALStore(cs, nil, nil)
	for _, s := range []*S3WAL{a, b} {
		s.backoffBase = time.Microsecond
		s.backoffCap = time.Millisecond
	}
	return cs, a, b
}

const (
	seedSHA1 = "1111111111111111111111111111111111111111"
	seedSHA2 = "2222222222222222222222222222222222222222"
	seedSHA3 = "3333333333333333333333333333333333333333"
)

// TestS3WALMutateSeedSkipsInitialRead pins the issue #182 trim: a mutation
// following a same-process read or write of the same base starts from the
// cached (body, etag) snapshot — zero GETs — and the If-Match PUT
// validates it. A competing writer staling the seed costs exactly one 412
// plus the ordinary fresh-read retry, never a wrong outcome.
func TestS3WALMutateSeedSkipsInitialRead(t *testing.T) {
	ctx := context.Background()
	cs, a, b := newSeedStores(t)
	const repo = "seedrepo"

	// Genesis create: no snapshot exists (a virgin base has no etag to
	// seed), so the first mutation reads (the miss) then creates.
	if won, err := a.CASRef(ctx, repo, "refs/heads/main", ZeroSHA, seedSHA1); err != nil || !won {
		t.Fatalf("genesis CASRef = (%v, %v), want won", won, err)
	}

	// Warm path: the create's acknowledged body+etag seed the next CAS.
	gets, puts := cs.gets.Load(), cs.puts.Load()
	if won, err := a.CASRef(ctx, repo, "refs/heads/main", seedSHA1, seedSHA2); err != nil || !won {
		t.Fatalf("seeded CASRef = (%v, %v), want won", won, err)
	}
	if dg, dp := cs.gets.Load()-gets, cs.puts.Load()-puts; dg != 0 || dp != 1 {
		t.Fatalf("seeded CASRef cost %d GETs + %d PUTs, want 0 + 1", dg, dp)
	}

	// Stale seed: an independent driver (fresh process stand-in) advances
	// the manifest behind a's back. a's next CAS must land the correct
	// outcome anyway — seeded PUT loses with 412, the fresh-read retry
	// wins — at one extra GET, one extra PUT.
	if won, err := b.CASRef(ctx, repo, "refs/heads/other", ZeroSHA, seedSHA3); err != nil || !won {
		t.Fatalf("competing CASRef = (%v, %v), want won", won, err)
	}
	gets, puts = cs.gets.Load(), cs.puts.Load()
	if won, err := a.CASRef(ctx, repo, "refs/heads/main", seedSHA2, seedSHA3); err != nil || !won {
		t.Fatalf("stale-seeded CASRef = (%v, %v), want won", won, err)
	}
	if dg, dp := cs.gets.Load()-gets, cs.puts.Load()-puts; dg != 1 || dp != 2 {
		t.Fatalf("stale-seeded CASRef cost %d GETs + %d PUTs, want 1 + 2", dg, dp)
	}
	if sha, err := a.GetRef(ctx, repo, "refs/heads/main"); err != nil || sha != seedSHA3 {
		t.Fatalf("GetRef = (%q, %v), want %s", sha, err, seedSHA3)
	}
}

// TestS3WALMutateSeededVerdictRederivesFresh pins the linearizability
// hazard the seed design must dodge: an apply=false verdict computed on a
// stale seed (a "lost" CAS, a no-op) is unvalidated — no PUT proves the
// seed current — so mutate must re-derive it from a fresh read instead of
// returning it. Here the seed says the precondition FAILS while live
// state says it HOLDS; trusting the seed would report a bogus loss.
func TestS3WALMutateSeededVerdictRederivesFresh(t *testing.T) {
	ctx := context.Background()
	cs, a, b := newSeedStores(t)
	const repo = "seedrepo"

	if won, err := a.CASRef(ctx, repo, "refs/heads/main", ZeroSHA, seedSHA1); err != nil || !won {
		t.Fatalf("genesis CASRef = (%v, %v), want won", won, err)
	}
	// b moves main 1->2; a's seed still says 1.
	if won, err := b.CASRef(ctx, repo, "refs/heads/main", seedSHA1, seedSHA2); err != nil || !won {
		t.Fatalf("competing CASRef = (%v, %v), want won", won, err)
	}
	// Against a's stale seed the precondition (cur == 2) fails; against
	// live state it holds. The win must be reported, at exactly one
	// fresh GET + one PUT after the seeded evaluation.
	gets := cs.gets.Load()
	won, err := a.CASRef(ctx, repo, "refs/heads/main", seedSHA2, seedSHA3)
	if err != nil || !won {
		t.Fatalf("CASRef against stale seed = (%v, %v), want won (seeded loss verdicts must re-derive fresh)", won, err)
	}
	if dg := cs.gets.Load() - gets; dg != 1 {
		t.Fatalf("re-derived CASRef cost %d GETs, want exactly 1 fresh read", dg)
	}
	if sha, err := a.GetRef(ctx, repo, "refs/heads/main"); err != nil || sha != seedSHA3 {
		t.Fatalf("GetRef = (%q, %v), want %s", sha, err, seedSHA3)
	}
}

// TestS3WALManifestSnapBounds pins the seed cache's memory bounds (Codex
// review on PR #185): entries, total retained bytes, and the per-entry
// body cap — an outsized body must not only be skipped but also evict the
// base's stale entry.
func TestS3WALManifestSnapBounds(t *testing.T) {
	_, a, _ := newSeedStores(t)
	hasSnap := func(base string) bool {
		a.snapMu.Lock()
		defer a.snapMu.Unlock()
		_, ok := a.snaps[base]
		return ok
	}

	// Per-entry cap: an over-cap body drops the base's existing entry.
	a.storeSnap("big", []byte("small"), "e1")
	if !hasSnap("big") {
		t.Fatal("small entry not cached")
	}
	a.storeSnap("big", make([]byte, maxManifestSnapBody+1), "e2")
	if hasSnap("big") {
		t.Fatal("over-cap body cached, want dropped (including the stale entry)")
	}

	// Byte budget: storing more at-cap bodies than the byte budget holds
	// must evict down to the budget, never empty the cache.
	body := make([]byte, maxManifestSnapBody)
	const stored = maxManifestSnapBytes/maxManifestSnapBody + 4
	for i := range stored {
		a.storeSnap(fmt.Sprintf("base%d", i), body, "e")
	}
	a.snapMu.Lock()
	entries, bytes := len(a.snaps), a.snapBytes
	a.snapMu.Unlock()
	if bytes > maxManifestSnapBytes {
		t.Fatalf("retained %d bytes, budget %d", bytes, maxManifestSnapBytes)
	}
	if entries >= stored {
		t.Fatalf("retained all %d entries, want byte-budget eviction", entries)
	}
	if entries == 0 {
		t.Fatal("eviction emptied the cache entirely")
	}

	// Entry cap with tiny bodies.
	for i := range maxManifestSnaps + 16 {
		a.storeSnap(fmt.Sprintf("tiny%d", i), []byte("x"), "e")
	}
	a.snapMu.Lock()
	entries = len(a.snaps)
	a.snapMu.Unlock()
	if entries > maxManifestSnaps {
		t.Fatalf("retained %d entries, cap %d", entries, maxManifestSnaps)
	}
}

// TestS3WALHydrateSnapshotAgreesWithPortableReads pins the
// ref.HydrateSnapshotter contract (issue #182): one snapshot read serves
// exactly what GetGeneration + ListPacks + a full ListRefsAfter scan
// serve, in their documented orders.
func TestS3WALHydrateSnapshotAgreesWithPortableReads(t *testing.T) {
	ctx := context.Background()
	_, a, _ := newSeedStores(t)
	const repo = "snaprepo"

	for _, name := range []string{"refs/heads/main", "refs/heads/a", "refs/tags/v1"} {
		if won, err := a.CASRef(ctx, repo, name, ZeroSHA, seedSHA1); err != nil || !won {
			t.Fatalf("CASRef(%s) = (%v, %v), want won", name, won, err)
		}
	}
	packs := []Pack{
		{ID: "p1", Key: "packs/" + repo + "/p1.pack", Size: 10},
		{ID: "p2", Key: "packs/" + repo + "/p2.pack", Size: 20},
	}
	if err := a.RegisterPacks(ctx, repo, packs); err != nil {
		t.Fatalf("RegisterPacks: %v", err)
	}

	snap, err := a.HydrateSnapshot(ctx, repo)
	if err != nil {
		t.Fatalf("HydrateSnapshot: %v", err)
	}
	gen, err := a.GetGeneration(ctx, repo)
	if err != nil {
		t.Fatalf("GetGeneration: %v", err)
	}
	if snap.Generation != gen {
		t.Fatalf("snapshot generation %d, portable read %d", snap.Generation, gen)
	}
	listed, err := a.ListPacks(ctx, repo)
	if err != nil {
		t.Fatalf("ListPacks: %v", err)
	}
	if len(snap.Packs) != len(listed) {
		t.Fatalf("snapshot packs %v, listed %v", snap.Packs, listed)
	}
	for i := range listed {
		if snap.Packs[i] != listed[i] {
			t.Fatalf("snapshot pack[%d] = %+v, listed %+v", i, snap.Packs[i], listed[i])
		}
	}
	paged, err := a.ListRefsAfter(ctx, repo, "", 1000)
	if err != nil {
		t.Fatalf("ListRefsAfter: %v", err)
	}
	if len(snap.Refs) != len(paged) {
		t.Fatalf("snapshot refs %v, paged %v", snap.Refs, paged)
	}
	for i := range paged {
		if snap.Refs[i] != paged[i] {
			t.Fatalf("snapshot ref[%d] = %+v, paged %+v", i, snap.Refs[i], paged[i])
		}
	}

	// Virgin repo: zero-value snapshot, matching the portable reads.
	empty, err := a.HydrateSnapshot(ctx, "neverseen")
	if err != nil {
		t.Fatalf("HydrateSnapshot(virgin): %v", err)
	}
	if empty.Generation != 0 || len(empty.Packs) != 0 || len(empty.Refs) != 0 {
		t.Fatalf("virgin snapshot = %+v, want zero value", empty)
	}
}
