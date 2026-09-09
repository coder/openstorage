package githttp

// Unit tests for the pack-bytes quota support code (issue #21, quota.go):
// pack-prefix usage accounting, the pressure-check TTL cache (listing-work
// bounds + per-repo miss coalescing), and the HTTP layer's quota-neutral
// behavior (auth ordering preserved; no quota rejection at HTTP time —
// pre-receive owns enforcement). Uses the zero-container fileblob backend;
// the enforcement halves live in quota_e2e_test.go.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// seedPack publishes size bytes at repo's pack key with the given id.
func seedPack(t *testing.T, bl blob.BlobStore, repo, id string, size int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seed.pack")
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := bl.UploadFile(context.Background(), blob.PackKey(repo, id), path); err != nil {
		t.Fatal(err)
	}
}

func seedLFS(t *testing.T, bl blob.BlobStore, repoID, oid string, size int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seed.lfs")
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := bl.UploadFile(context.Background(), blob.LFSKey(repoID, oid), path); err != nil {
		t.Fatal(err)
	}
}

func openTestBlob(t *testing.T) blob.BlobStore {
	t.Helper()
	bl, err := blob.Open(context.Background(), "file://"+t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bl.Close() })
	return bl
}

// countingBlob wraps a BlobStore counting ListPages calls, proving the
// pressure check's TTL cache bounds listing work (Codex round 2: replayed
// cheap receive-pack POSTs must not amplify into per-request O(pack-count)
// listings against the shared blob backend). The counter is atomic so
// coalescing tests can hammer it concurrently under -race.
type countingBlob struct {
	blob.BlobStore
	lists atomic.Int64
}

func (c *countingBlob) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	c.lists.Add(1)
	return c.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

// failingListBlob errors every ListPages call (Codex round 7: a blob
// backend outage must not turn the observational pressure check into a
// write outage).
type failingListBlob struct{ blob.BlobStore }

func (failingListBlob) ListPages(context.Context, string, int, func([]blob.ListedObject) error) error {
	return context.DeadlineExceeded
}

// awaitPeek polls peekUsage until it returns a fresh value (the async
// refresh a cold/expired peek triggered has published) or times out.
func awaitPeek(t *testing.T, srv *Server, repo string) int64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if usage, ok := srv.peekUsage(repo); ok {
			return usage
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("peekUsage(%q) never became fresh", repo)
	return 0
}

func TestPackUsageCacheBoundsListings(t *testing.T) {
	cb := &countingBlob{BlobStore: openTestBlob(t)}
	seedPack(t, cb.BlobStore, "repo_c", "aaaa", 10)

	now := time.Unix(1000, 0)
	var nowMu sync.Mutex
	srv := &Server{Blob: cb}
	srv.usage.now = func() time.Time { nowMu.Lock(); defer nowMu.Unlock(); return now }

	// A cold peek returns no value, triggers one async refresh, and later
	// peeks serve from cache without further listings.
	if _, ok := srv.peekUsage("repo_c"); ok {
		t.Fatal("cold peek returned a value")
	}
	if got := awaitPeek(t, srv, "repo_c"); got != 10 {
		t.Fatalf("refreshed usage = %d, want 10", got)
	}
	for i := range 5 {
		if usage, ok := srv.peekUsage("repo_c"); !ok || usage != 10 {
			t.Fatalf("cached peek %d: (%d, %v), want (10, true)", i, usage, ok)
		}
	}
	// One refresh = three listings (base + namespace + LFS prefixes roll up).
	if cb.lists.Load() != 3 {
		t.Fatalf("cold+cached peeks cost %d listings, want 3", cb.lists.Load())
	}

	// A different repo has its own entry (one more refresh)...
	srv.peekUsage("repo_d")
	awaitPeek(t, srv, "repo_d")
	if cb.lists.Load() != 6 {
		t.Fatalf("second repo cost %d total listings, want 6", cb.lists.Load())
	}

	// ...and TTL expiry refreshes: the stale entry is re-listed, observing
	// growth that happened meanwhile.
	seedPack(t, cb.BlobStore, "repo_c", "bbbb", 5)
	nowMu.Lock()
	now = now.Add(usageCacheTTL + time.Second)
	nowMu.Unlock()
	if _, ok := srv.peekUsage("repo_c"); ok {
		t.Fatal("expired peek returned a value")
	}
	if got := awaitPeek(t, srv, "repo_c"); got != 15 {
		t.Fatalf("post-TTL usage = %d, want 15", got)
	}
	if cb.lists.Load() != 9 {
		t.Fatalf("post-TTL refresh cost %d total listings, want 9", cb.lists.Load())
	}
}

// blockingBlob parks every ListPages call until release is closed,
// counting entries and completions, so tests can hold refresh slots
// deterministically and converge on finished listings.
type blockingBlob struct {
	blob.BlobStore
	release chan struct{}
	calls   atomic.Int64 // entered listings (parked flights count here)
	done    atomic.Int64 // completed listings (the delegated call returned)
}

func (b *blockingBlob) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	b.calls.Add(1)
	<-b.release
	err := b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
	b.done.Add(1)
	return err
}

// TestPackUsageRefreshConcurrencyBounded (Codex round 10): background
// refreshes are bounded node-wide by usageRefreshSlots — distinct-repo cold
// peeks beyond the pool size start NO goroutine (deterministic: no flight
// exists, so nothing can list) and are picked up once slots free.
func TestPackUsageRefreshConcurrencyBounded(t *testing.T) {
	// Test-local slot pool (the swap pattern the usage-floor slot tests
	// below use): the flights spawned here release their slots only after
	// the final convergence check has already passed, so they must not
	// hold the package-global pool into subsequent tests.
	oldSlots := usageRefreshSlots
	usageRefreshSlots = make(chan struct{}, 4)
	t.Cleanup(func() { usageRefreshSlots = oldSlots })
	slots := cap(usageRefreshSlots)
	bb := &blockingBlob{BlobStore: openTestBlob(t), release: make(chan struct{})}
	srv := &Server{Blob: bb}

	// Fill every slot with distinct repos; their flights park in ListPages.
	for i := range slots {
		srv.peekUsage(fmt.Sprintf("repo_slot_%d", i))
	}
	deadline := time.Now().Add(5 * time.Second)
	for bb.calls.Load() < int64(slots) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := bb.calls.Load(); got != int64(slots) {
		t.Fatalf("expected %d parked listings, got %d", slots, got)
	}

	// With the pool exhausted, an extra repo's peeks start nothing.
	for range 10 {
		if _, ok := srv.peekUsage("repo_extra"); ok {
			t.Fatal("cold peek returned a value")
		}
	}
	if got := bb.calls.Load(); got != int64(slots) {
		t.Fatalf("pool-exhausted peeks started listings: %d, want %d", got, slots)
	}

	// Freeing the pool lets a later peek refresh the extra repo.
	close(bb.release)
	if got := awaitPeek(t, srv, "repo_extra"); got != 0 {
		t.Fatalf("refreshed usage = %d, want 0 (empty repo)", got)
	}
	// Each completed refresh lists three times (base + namespace + LFS
	// prefixes). Converge on COMPLETED listings instead of reading the
	// counter right after the single awaited peek: a slot frees as soon
	// as ONE released flight finishes, so repo_extra can publish while
	// other flights are still mid-scan on a loaded runner (issue #106) —
	// and an entry count alone can hit its target while the last
	// delegated listing is still running, letting teardown close the
	// bucket under it. Exactly slots+1 flights exist — the pool-exhausted
	// assertion above proved no extra flight started — so the completion
	// count is stable once reached, and an entry overshoot (a bound
	// regression) still fails the equality check below.
	want := int64(slots+1) * 3
	deadline = time.Now().Add(5 * time.Second)
	for bb.done.Load() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := bb.done.Load(); got != want {
		t.Fatalf("completed listings = %d, want %d", got, want)
	}
	if got := bb.calls.Load(); got != want {
		t.Fatalf("total listings = %d, want %d", got, want)
	}
}

// scanAdvancingBlob advances the injected clock at the start of each
// scan's first ListPages call (limit bounds how many scans it slows),
// simulating listings that take longer than the cache TTL.
type scanAdvancingBlob struct {
	blob.BlobStore
	advance func()
	limit   int
	mu      sync.Mutex
	scans   int
}

func (b *scanAdvancingBlob) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	b.mu.Lock()
	// Three ListPages per refresh (base + namespace + LFS): the first of
	// each triple marks a new scan.
	if b.scans%3 == 0 && b.scans/3 < b.limit {
		b.advance()
	}
	b.scans++
	b.mu.Unlock()
	return b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

// TestUsageFloorEntryAgesFromScanStart (Codex round 40): the cached
// floor's age must count from the START of the listing, not its
// completion — a scan approaching usageListTimeout otherwise yields an
// entry whose true age exceeds the recorded expiry by the whole scan
// duration, and with the reservation TTL configured near its 30s minimum,
// an orphan published after its page was scanned loses reservation cover
// while the stale floor is still served. A scan that itself outlives the
// TTL must produce an entry that is already expired (dead on arrival), so
// the peek path keeps electing refreshes until a fast scan lands.
func TestUsageFloorEntryAgesFromScanStart(t *testing.T) {
	now := time.Unix(1000, 0)
	var nowMu sync.Mutex
	srv := &Server{}
	srv.usage.now = func() time.Time { nowMu.Lock(); defer nowMu.Unlock(); return now }
	cb := &countingBlob{BlobStore: openTestBlob(t)}
	srv.Blob = &scanAdvancingBlob{BlobStore: cb, limit: 1, advance: func() {
		nowMu.Lock()
		now = now.Add(usageCacheTTL + 10*time.Second) // the scan outlives the TTL
		nowMu.Unlock()
	}}

	// The first refresh's entry is dead on arrival, so peeks keep
	// re-electing flights until the second (fast) scan stores a fresh
	// entry: exactly two refreshes = six listings.
	if got := awaitPeek(t, srv, "repo_slow"); got != 0 {
		t.Fatalf("refreshed usage = %d, want 0", got)
	}
	if got := cb.lists.Load(); got != 6 {
		t.Fatalf("scan-outlived refresh cost %d listings, want 6 (dead-on-arrival entry must re-elect)", got)
	}
}

// TestUsageFloorSubtractsCounterAtScan (Codex round 44): the admission
// floor is the UNCOUNTED-bytes measurement — listing minus the usage
// counter at scan start — because ReservePackBytes SUMS it with the live
// counter. Returning the raw listing would double-charge every registered
// byte; returning it under max() semantics hid post-scan settlements. A
// repo with 30 listed bytes of which 20 are registered must floor at 10.
func TestUsageFloorSubtractsCounterAtScan(t *testing.T) {
	ctx := context.Background()
	st := openTestRefStore(t)
	bl := openTestBlob(t)
	srv := &Server{Blob: bl, Store: st}

	seedPack(t, bl, "repo_unc", "aaaa", 10) // pre-registry: uncounted
	seedLFS(t, bl, "repo_unc", strings.Repeat("ab", 32), 20)
	if err := st.RegisterLFSObject(ctx, "repo_unc",
		blob.LFSKey("repo_unc", strings.Repeat("ab", 32)), 20); err != nil {
		t.Fatal(err)
	}

	if floor, err := srv.CachedUsageFloor(ctx, "repo_unc"); err != nil || floor != 10 {
		t.Fatalf("uncounted floor = (%d, %v), want (10, nil) — registered bytes must not double-charge", floor, err)
	}
	// The observational peek keeps the RAW listing (the pressure log
	// compares total usage against the quota).
	if usage, ok := srv.peekUsage("repo_unc"); !ok || usage != 30 {
		t.Fatalf("peek usage = (%d, %v), want (30, true)", usage, ok)
	}
}

// slowListBlob delays each ListPages call, simulating a scan that
// outlives the freshness window (bounded by limit scans).
type slowListBlob struct {
	blob.BlobStore
	delay time.Duration
	limit int
	mu    sync.Mutex
	scans int
}

func (b *slowListBlob) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	b.mu.Lock()
	if b.scans%3 == 0 && b.scans/3 < b.limit {
		time.Sleep(b.delay)
	}
	b.scans++
	b.mu.Unlock()
	return b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

// TestHookSelfListFloorRejectsSlowScans (Codex round 46): the hook's
// SELF-LISTED floor has no cache in front of it, so it needs the same
// scan-age discipline as the cached flights — a listing that outlives the
// freshness window may miss an orphan whose reservation expires before
// the hook's bounded reserve lands. One retry covers a transient stall;
// persistently slow scans fail CLOSED (deny the push) instead of
// admitting against the stale floor.
func TestHookSelfListFloorRejectsSlowScans(t *testing.T) {
	old := hookFloorFreshness
	hookFloorFreshness = 50 * time.Millisecond
	t.Cleanup(func() { hookFloorFreshness = old })

	t.Run("retry-covers-a-transient-stall", func(t *testing.T) {
		bl := &slowListBlob{BlobStore: openTestBlob(t), delay: 100 * time.Millisecond, limit: 1}
		seedPack(t, bl.BlobStore, "repo_slowhook", "aaaa", 25)
		got, _, err := hookSelfListFloor(context.Background(), bl, openTestRefStore(t), "repo_slowhook")
		if err != nil || got != 25 {
			t.Fatalf("floor after one slow scan = (%d, %v), want (25, nil) via retry", got, err)
		}
	})

	t.Run("persistently-slow-fails-closed", func(t *testing.T) {
		bl := &slowListBlob{BlobStore: openTestBlob(t), delay: 100 * time.Millisecond, limit: 1 << 30}
		if got, _, err := hookSelfListFloor(context.Background(), bl, openTestRefStore(t), "repo_alwaysslow"); err == nil {
			t.Fatalf("persistently slow self-list served floor %d, want fail-closed error", got)
		}
	})
}

// TestUsageFloorInvalidatedByAdoption (Codex round 48): adopting a
// pre-registry LFS object moves bytes a cached scan already listed into
// the usage counter — an entry scanned before the adoption would then
// double-count them in the floor+counter sum (false denials for the
// cache lifetime, and via any hook floor minted from it). Adoption must
// invalidate the repo's entry so the next admission re-scans.
func TestUsageFloorInvalidatedByAdoption(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 1000, time.Hour)
	tok := mint(auth.ScopeGitWrite)

	// A rowless pre-registry object: listed, not counted.
	legacy := []byte("pre-registry legacy object body")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// Warm the cache: the whole object is uncounted.
	if floor, err := srv.CachedUsageFloor(ctx, lfsTestRepoID); err != nil || floor != int64(len(legacy)) {
		t.Fatalf("pre-adoption floor = (%d, %v), want (%d, nil)", floor, err, len(legacy))
	}
	// Adopt it (write-scoped batch touch registers the stored object).
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("adoption failed: %+v", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
	// The stale entry must be gone: the next serve RE-SCANS instead of
	// serving the cached pre-adoption value. The floor VALUE no longer
	// distinguishes stale from fresh (round 80): the retained legacy
	// source keeps exactly the adopted size uncounted — physically
	// stored until the sweeper reclaims it — so the invalidation is
	// proven by the scan itself.
	cb := &countingBlob{BlobStore: srv.Blob}
	srv.Blob = cb
	floor, err := srv.CachedUsageFloor(ctx, lfsTestRepoID)
	if err != nil || floor != int64(len(legacy)) {
		t.Fatalf("post-adoption floor = (%d, %v), want (%d, nil) — the RETAINED source stays uncounted-but-listed until swept", floor, err, len(legacy))
	}
	if cb.lists.Load() == 0 {
		t.Fatal("post-adoption floor served from the stale cache — the adoption generation bump did not invalidate")
	}
}

// TestUsageFloorInvalidatedAcrossReplicas (Codex round 49): round 48's
// local map delete cannot reach OTHER replicas' caches, so adoption bumps
// a shared generation in the ref store and every replica's floor serve is
// generation-checked. A second Server sharing the same stores (a replica)
// must stop serving its pre-adoption floor immediately after the adopting
// replica registers the object.
func TestUsageFloorInvalidatedAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	srv1, st, mint := newLFSQuotaServer(t, 1000, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	// The "other replica": its own floor cache, the same shared stores.
	srv2 := &Server{Blob: srv1.Blob, Store: st}

	legacy := []byte("cross-replica legacy object")
	oid := sha256Hex(legacy)
	if err := srv1.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// Replica 2 caches the pre-adoption floor.
	if floor, err := srv2.CachedUsageFloor(ctx, lfsTestRepoID); err != nil || floor != int64(len(legacy)) {
		t.Fatalf("pre-adoption floor = (%d, %v), want (%d, nil)", floor, err, len(legacy))
	}
	// Replica 1 adopts the object (write-scoped batch touch).
	resp := decodeBatch(t, doLFS(t, srv1, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("adoption failed: %+v", resp.Objects[0])
	}
	// Replica 2's cached entry must be rejected by the generation check:
	// the serve re-scans (the VALUE alone no longer distinguishes stale
	// from fresh — the retained source keeps the adopted size listed and
	// uncounted, round 80).
	cb := &countingBlob{BlobStore: srv2.Blob}
	srv2.Blob = cb
	floor, err := srv2.CachedUsageFloor(ctx, lfsTestRepoID)
	if err != nil || floor != int64(len(legacy)) {
		t.Fatalf("post-adoption remote floor = (%d, %v), want (%d, nil) — the retained source stays uncounted-but-listed", floor, err, len(legacy))
	}
	if cb.lists.Load() == 0 {
		t.Fatal("remote floor served from the stale cache — the shared generation must invalidate")
	}
}

// TestUsageFloorInvalidatedByZeroGrowthRepair (Codex round 56): a
// registered object's blob can vanish (storage loss, a crash between an
// ambiguous registration and republication). A floor scanned during the
// absence subtracts the full counter from a listing that no longer holds
// the object's bytes — hiding equally sized rowless legacy data — and the
// zero-growth PUT that repairs the object re-materializes those bytes
// without touching the counter. The repair must bump the shared floor
// generation (the PUT analogue of the sweeper's quarantine-restore bump,
// round 54) so every replica's floor cached in the absence window is
// invalidated.
func TestUsageFloorInvalidatedByZeroGrowthRepair(t *testing.T) {
	ctx := context.Background()
	srv1, st, mint := newLFSQuotaServer(t, 1000, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	// The "other replica": its own floor cache, the same shared stores.
	srv2 := &Server{Blob: srv1.Blob, Store: st}

	// A registered, counter-accounted object.
	body := []byte("registered object body, counter-accounted")
	oid := sha256Hex(body)
	key := blob.LFSKey(lfsTestRepoID, oid)
	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	if w := doLFS(t, srv1, http.MethodPut, objPath, "basic:"+tok, string(body)); w.Code != http.StatusOK {
		t.Fatalf("upload PUT status = %d, body %s", w.Code, w.Body)
	}
	// Rowless legacy bytes, smaller than the registered object.
	legacy := []byte("rowless legacy")
	if err := srv1.Blob.UploadFile(ctx, blob.LFSKey(lfsTestRepoID, sha256Hex(legacy)), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// The registered blob vanishes; replica 2 scans during the absence:
	// the listing (legacy only) minus the counter (the registered object)
	// clamps to zero — the legacy bytes are hidden by the missing object.
	if err := srv1.Blob.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if floor, err := srv2.CachedUsageFloor(ctx, lfsTestRepoID); err != nil || floor != 0 {
		t.Fatalf("absence-window floor = (%d, %v), want (0, nil)", floor, err)
	}
	// The client repairs the object (zero-growth re-PUT).
	if w := doLFS(t, srv1, http.MethodPut, objPath, "basic:"+tok, string(body)); w.Code != http.StatusOK {
		t.Fatalf("repair PUT status = %d, body %s", w.Code, w.Body)
	}
	// Replica 2's cached zero floor must be rejected by the generation
	// check: a fresh scan lists legacy + registered bytes, and the
	// uncounted legacy bytes resurface in the floor.
	if floor, err := srv2.CachedUsageFloor(ctx, lfsTestRepoID); err != nil || floor != int64(len(legacy)) {
		t.Fatalf("post-repair floor = (%d, %v), want (%d, nil) — the repair must bump the shared floor generation",
			floor, err, len(legacy))
	}
}

// genStallStore stalls the FIRST adoption-generation read via the
// injected callback (advancing a fake clock or sleeping), simulating a
// database stall during the serve-time generation check.
type genStallStore struct {
	ref.Store
	stall func()
	once  sync.Once
}

func (s *genStallStore) LFSAdoptionGen(ctx context.Context, repo string) (int64, error) {
	s.once.Do(s.stall)
	return s.Store.LFSAdoptionGen(ctx, repo)
}

// TestUsageFloorGenReadStallRechecksExpiry (Codex round 50): the
// generation check is an unbounded database read — if it stalls, the
// entry it validated can expire before the caller reserves, reopening the
// scan-freshness hole for orphans published after the scan. After the
// generation read, the entry's expiry must be RE-CHECKED: a consumed
// window forces a fresh scan instead of serving the stale floor.
func TestUsageFloorGenReadStallRechecksExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	var nowMu sync.Mutex
	cb := &countingBlob{BlobStore: openTestBlob(t)}
	st := openTestRefStore(t)
	srv := &Server{Blob: cb, Store: &genStallStore{Store: st, stall: func() {
		nowMu.Lock()
		now = now.Add(usageCacheTTL + time.Second) // the stall consumes the window
		nowMu.Unlock()
	}}}
	srv.usage.now = func() time.Time { nowMu.Lock(); defer nowMu.Unlock(); return now }

	seedPack(t, cb.BlobStore, "repo_genstall", "aaaa", 30)
	// Warm the entry directly (a fresh scan at t0 with floor 30, gen 0).
	srv.usage.mu.Lock()
	srv.usage.putLocked("repo_genstall", 30, 0, 0, now)
	srv.usage.mu.Unlock()

	// The serve validates the entry, the gen read stalls past its
	// expiry: the floor must come from a NEW scan (3 listings), never
	// the stale entry (0 listings).
	if floor, err := srv.CachedUsageFloor(context.Background(), "repo_genstall"); err != nil || floor != 30 {
		t.Fatalf("floor after gen-read stall = (%d, %v), want (30, nil)", floor, err)
	}
	if got := cb.lists.Load(); got != 3 {
		t.Fatalf("gen-read stall served after %d listings, want 3 (expiry recheck must force a fresh scan)", got)
	}
}

// TestUsageFloorZeroFloorsAreGenChecked (Codex round 54, security): a
// sweeper quarantine restore re-materializes counter-accounted bytes a
// scan could not see, so a stale ZERO floor can under-measure — the
// zero-floor generation-check skip was unsound. A cached zero floor whose
// generation moved (here bumped directly, as the restore path does) must
// be dropped and re-scanned, never served.
func TestUsageFloorZeroFloorsAreGenChecked(t *testing.T) {
	ctx := context.Background()
	st := openTestRefStore(t)
	cb := &countingBlob{BlobStore: openTestBlob(t)}
	srv := &Server{Blob: cb, Store: st}

	// A fully counted repo: listing 30, counter 30 → floor 0, cached.
	key := blob.LFSKey("repo_zerogen", strings.Repeat("ef", 32))
	seedLFS(t, cb.BlobStore, "repo_zerogen", strings.Repeat("ef", 32), 30)
	if err := st.RegisterLFSObject(ctx, "repo_zerogen", key, 30); err != nil {
		t.Fatal(err)
	}
	if floor, err := srv.CachedUsageFloor(ctx, "repo_zerogen"); err != nil || floor != 0 {
		t.Fatalf("warm floor = (%d, %v), want (0, nil)", floor, err)
	}
	warmLists := cb.lists.Load()

	// A sweeper restore elsewhere bumps the shared generation.
	if err := st.BumpLFSAdoptionGen(ctx, "repo_zerogen"); err != nil {
		t.Fatal(err)
	}
	// The cached zero must NOT be served: the serve re-scans.
	if floor, err := srv.CachedUsageFloor(ctx, "repo_zerogen"); err != nil || floor != 0 {
		t.Fatalf("post-bump floor = (%d, %v), want (0, nil) via re-scan", floor, err)
	}
	if got := cb.lists.Load(); got != warmLists+3 {
		t.Fatalf("post-bump serve cost %d listings total, want %d (stale zero floor must re-scan)", got, warmLists+3)
	}
}

// adoptDuringListBlob performs an adoption (register + gen bump) during
// the FIRST listing, simulating another replica adopting mid-scan.
type adoptDuringListBlob struct {
	blob.BlobStore
	st    ref.Store
	repo  string
	key   string
	bytes int64
	once  sync.Once
}

func (b *adoptDuringListBlob) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	b.once.Do(func() {
		if err := b.st.RegisterAdoptedLFSObject(ctx, b.repo, b.key, b.bytes); err != nil {
			panic(err)
		}
	})
	return b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

// TestHookSelfListFloorDetectsMidScanAdoption (Codex round 54): the hook's
// self-list measurement is bracketed by floor-generation reads — an
// adoption committing between the counter read and the listing would skew
// the subtraction (the listing sees bytes the counter snapshot missed AND
// the live counter now carries), so a moved generation rejects the
// attempt and the retry measures post-adoption reality.
func TestHookSelfListFloorDetectsMidScanAdoption(t *testing.T) {
	ctx := context.Background()
	st := openTestRefStore(t)
	bl := openTestBlob(t)
	oid := strings.Repeat("ba", 32)
	key := blob.LFSKey("repo_midscan", oid)
	seedLFS(t, bl, "repo_midscan", oid, 30)

	adopting := &adoptDuringListBlob{BlobStore: bl, st: st, repo: "repo_midscan", key: key, bytes: 30}
	// Attempt 1: counter read 0, adoption fires inside the listing
	// (listing 30, counter now 30) — the bracketing rejects it. Attempt
	// 2: counter 30, listing 30 → floor 0.
	got, _, err := hookSelfListFloor(ctx, adopting, st, "repo_midscan")
	if err != nil || got != 0 {
		t.Fatalf("mid-scan-adoption self-list floor = (%d, %v), want (0, nil) via retry", got, err)
	}
}

// TestUsageFloorInFlightScanInvalidatedByAdoption (Codex round 49,
// security): adoption racing a COLD in-flight scan must not hand the
// scan's waiters a stale floor — the flight snapshotted the generation
// before scanning, so the serve-time generation check catches the
// mid-scan adoption, invalidates the repopulated entry, and retries onto
// a post-adoption scan.
func TestUsageFloorInFlightScanInvalidatedByAdoption(t *testing.T) {
	ctx := context.Background()
	st := openTestRefStore(t)
	bb := &blockingBlob{BlobStore: openTestBlob(t), release: make(chan struct{})}
	srv := &Server{Blob: bb, Store: st}

	legacy := []byte("mid-scan adopted legacy object")
	key := blob.LFSKey("repo_race", strings.Repeat("cd", 32))
	if err := bb.BlobStore.UploadFile(ctx, key, writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}

	// Cold floor request: its flight snapshots gen 0 and parks in the
	// listing.
	type res struct {
		floor int64
		err   error
	}
	done := make(chan res, 1)
	go func() {
		floor, err := srv.CachedUsageFloor(ctx, "repo_race")
		done <- res{floor, err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for bb.calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("flight never reached the listing")
		}
		time.Sleep(time.Millisecond)
	}
	// The adoption commits mid-scan (another replica or this one).
	if err := st.RegisterLFSObject(ctx, "repo_race", key, int64(len(legacy))); err != nil {
		t.Fatal(err)
	}
	if err := st.BumpLFSAdoptionGen(ctx, "repo_race"); err != nil {
		t.Fatal(err)
	}
	close(bb.release)

	r := <-done
	if r.err != nil || r.floor != 0 {
		t.Fatalf("mid-scan-adoption floor = (%d, %v), want (0, nil) — the stale flight result must be rejected", r.floor, r.err)
	}
}

// TestHookFloorScansAreSlotBoundedAcrossProcesses (Codex round 47): hook
// self-list scans run in fresh processes the serve-side pool cannot see,
// so they take flock slot files instead — with every slot held (here by
// this test, in production by other hook processes), a scan must wait
// bounded by its attempt deadline and fail closed WITHOUT listing, and
// releasing a slot must let the next attempt succeed.
func TestHookFloorScansAreSlotBoundedAcrossProcesses(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir()) // isolate the slot files from parallel suites
	oldFresh := hookFloorFreshness
	hookFloorFreshness = 100 * time.Millisecond
	t.Cleanup(func() { hookFloorFreshness = oldFresh })

	// Hold every slot, as concurrent hook processes would.
	var unlocks []func()
	for range hookFloorScanSlots {
		u, err := acquireHookFloorScanSlot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		unlocks = append(unlocks, u)
	}

	cb := &countingBlob{BlobStore: openTestBlob(t)}
	st := openTestRefStore(t)
	if _, _, err := hookSelfListFloor(context.Background(), cb, st, "repo_slots"); err == nil {
		t.Fatal("slot-starved floor scan did not fail closed")
	}
	if got := cb.lists.Load(); got != 0 {
		t.Fatalf("slot-starved floor scan ran %d listings, want 0 (the slot must gate the scan)", got)
	}

	// Freeing one slot lets the scan through.
	unlocks[0]()
	if floor, _, err := hookSelfListFloor(context.Background(), cb, st, "repo_slots"); err != nil || floor != 0 {
		t.Fatalf("post-release floor = (%d, %v), want (0, nil)", floor, err)
	}
	for _, u := range unlocks[1:] {
		u()
	}
}

// TestUsageFloorScanExpiredResultFailsClosed (Codex round 41): the
// flight-result path must not hand CURRENT waiters a value whose scan
// already outlived the freshness window — under the minimum reservation
// TTL, an orphan published after its page was scanned has lost
// reservation cover by the time such a listing completes, so admitting
// against it overshoots quota. One retry covers a transiently slow scan;
// persistently slow scans FAIL CLOSED instead of serving the stale floor.
func TestUsageFloorScanExpiredResultFailsClosed(t *testing.T) {
	now := time.Unix(1000, 0)
	var nowMu sync.Mutex
	srv := &Server{}
	srv.usage.now = func() time.Time { nowMu.Lock(); defer nowMu.Unlock(); return now }
	srv.Blob = &scanAdvancingBlob{BlobStore: openTestBlob(t), limit: 1 << 30, advance: func() {
		nowMu.Lock()
		now = now.Add(usageCacheTTL + 10*time.Second) // EVERY scan outlives the TTL
		nowMu.Unlock()
	}}

	if usage, err := srv.CachedUsageFloor(context.Background(), "repo_always_slow"); err == nil {
		t.Fatalf("scan-expired floor served (%d, nil), want fail-closed error", usage)
	}
}

// TestUsageFloorAbandonedFlightIsCanceled (Codex round 41): a parked
// flight whose last waiter departs must be canceled and removed — slots
// bound only ACTIVE listings, so without cancellation a writer with many
// repositories could park one goroutine and inflight entry per repo and
// every abandoned flight would still run its detached one-minute listing
// with no caller remaining.
func TestUsageFloorAbandonedFlightIsCanceled(t *testing.T) {
	oldSlots := usageRefreshSlots
	usageRefreshSlots = make(chan struct{}, 1)
	usageRefreshSlots <- struct{}{} // the pool is full
	t.Cleanup(func() { usageRefreshSlots = oldSlots })

	cb := &countingBlob{BlobStore: openTestBlob(t)}
	srv := &Server{Blob: cb}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := srv.CachedUsageFloor(ctx, "repo_abandoned"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool-full cold floor error = %v, want context.DeadlineExceeded", err)
	}
	// The departed waiter was the last: the flight must be gone.
	srv.usage.mu.Lock()
	inflight := len(srv.usage.inflight)
	srv.usage.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("abandoned flight still registered (%d inflight entries), want 0", inflight)
	}
	// Freeing the slot must NOT resurrect the abandoned listing.
	<-usageRefreshSlots
	time.Sleep(50 * time.Millisecond)
	if got := cb.lists.Load(); got != 0 {
		t.Fatalf("abandoned flight ran %d listings after its last waiter left, want 0", got)
	}
}

// TestUsageFloorColdLeadersAreSlotBounded (Codex round 40): cold
// admission-floor requests must start their listing through the SAME
// bounded node-wide pool as the observational refreshes — inline leader
// listings bypassed usageRefreshSlots, so a writer with tokens for many
// repositories could disconnect many cold requests and leave one detached
// backend listing per repository running for up to usageListTimeout. With
// the pool full, a cold caller must wait (bounded by ITS context), and no
// listing may start until a slot frees.
func TestUsageFloorColdLeadersAreSlotBounded(t *testing.T) {
	oldSlots := usageRefreshSlots
	usageRefreshSlots = make(chan struct{}, 1)
	usageRefreshSlots <- struct{}{} // the pool is full
	t.Cleanup(func() { usageRefreshSlots = oldSlots })

	cb := &countingBlob{BlobStore: openTestBlob(t)}
	srv := &Server{Blob: cb}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := srv.CachedUsageFloor(ctx, "repo_cold"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool-full cold floor error = %v, want context.DeadlineExceeded", err)
	}
	if got := cb.lists.Load(); got != 0 {
		t.Fatalf("pool-full cold floor started %d listings, want 0 (slot must gate the flight)", got)
	}

	// Freeing the slot lets the parked flight run; a fresh caller
	// coalesces onto it and gets the value.
	<-usageRefreshSlots
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if usage, err := srv.CachedUsageFloor(ctx2, "repo_cold"); err != nil || usage != 0 {
		t.Fatalf("post-release floor = (%d, %v), want (0, nil)", usage, err)
	}
	if got := cb.lists.Load(); got != 3 {
		t.Fatalf("completed refresh cost %d listings, want 3", got)
	}
}

// TestPackUsageCachePruneIsTimeBounded (Codex round 8): inserts drop
// expired entries at most once per usageCachePruneInterval — never a
// per-insert full-map walk — and fresh entries survive the sweep.
func TestPackUsageCachePruneIsTimeBounded(t *testing.T) {
	now := time.Unix(1000, 0)
	c := &usageCache{now: func() time.Time { return now }}

	c.mu.Lock()
	for i := range 2000 {
		c.putLocked(fmt.Sprintf("repo_%04d", i), 1, 0, 0, now)
	}
	if got := len(c.entries); got != 2000 {
		t.Fatalf("fresh entries pruned early: %d, want 2000", got)
	}

	// Past the prune interval every old entry has also expired (TTL ==
	// interval); the next insert sweeps them all, keeping only itself.
	now = now.Add(usageCachePruneInterval + time.Second)
	c.putLocked("repo_new", 1, 0, 0, now)
	if got := len(c.entries); got != 1 {
		t.Fatalf("post-interval insert kept %d entries, want 1", got)
	}
	if _, ok := c.entries["repo_new"]; !ok {
		t.Fatal("the fresh insert itself was pruned")
	}

	// Immediately after a sweep, inserts must not sweep again (lastPrune
	// advanced): an expired straggler survives until the next interval.
	c.entries["repo_stale"] = usageEntry{usage: 1, expires: now.Add(-time.Second)}
	c.putLocked("repo_new2", 1, 0, 0, now)
	if _, ok := c.entries["repo_stale"]; !ok {
		t.Fatal("insert swept before the prune interval elapsed")
	}
	c.mu.Unlock()
}

// TestPackUsageCacheCoalescesMisses (Codex round 4): concurrent cold peeks
// for one repo elect a single background refresh; none of them block, and
// the flight's result serves every later peek. Without coalescing, N
// racing cheap POSTs would cost N full scans.
func TestPackUsageCacheCoalescesMisses(t *testing.T) {
	cb := &countingBlob{BlobStore: openTestBlob(t)}
	seedPack(t, cb.BlobStore, "repo_e", "aaaa", 10)
	srv := &Server{Blob: cb}

	const goroutines = 16
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			srv.peekUsage("repo_e") // cold: returns !ok, may start the one flight
		})
	}
	wg.Wait()
	if got := awaitPeek(t, srv, "repo_e"); got != 10 {
		t.Fatalf("refreshed usage = %d, want 10", got)
	}
	// One coalesced refresh = three listings (base + namespace + LFS
	// prefixes).
	if got := cb.lists.Load(); got != 3 {
		t.Fatalf("%d concurrent cold peeks cost %d listings, want 3", goroutines, got)
	}
}

func TestStorageUsageBytes(t *testing.T) {
	bl := openTestBlob(t)
	seedPack(t, bl, "repo_a", "1111", 100)
	seedPack(t, bl, "repo_a", "2222", 50)
	// Namespaces roll up under the base repo (Codex round 15: one token
	// writes to all of them, so they share the quota); other repos do not.
	seedPack(t, bl, "repo_a+ephemeral", "4444", 1000)
	seedPack(t, bl, "repo_a+import", "5555", 20)
	seedPack(t, bl, "repo_b", "3333", 4000)
	// LFS objects count too (issue #51): keyed under the base RepoID, so
	// they roll up like namespace packs; another repo's LFS space does not.
	seedLFS(t, bl, "repo_a", strings.Repeat("aa", 32), 7)
	seedLFS(t, bl, "repo_b", strings.Repeat("bb", 32), 9000)

	got, err := blob.StorageUsageBytes(context.Background(), bl, "repo_a")
	if err != nil {
		t.Fatal(err)
	}
	if got != 1177 {
		t.Fatalf("StorageUsageBytes(repo_a) = %d, want 1177 (base+namespaces+lfs)", got)
	}
	// A namespaced key measures the same rolled-up usage.
	got, err = blob.StorageUsageBytes(context.Background(), bl, "repo_a+ephemeral")
	if err != nil || got != 1177 {
		t.Fatalf("StorageUsageBytes(repo_a+ephemeral) = (%d, %v), want (1177, nil)", got, err)
	}
	// Pack admission floors on the SAME full measure (issue #51, Codex
	// round 18): pre-ledger LFS bytes must gate pack pushes too.
	got, err = packUsageBytes(context.Background(), bl, "repo_a")
	if err != nil || got != 1177 {
		t.Fatalf("packUsageBytes(repo_a) = (%d, %v), want (1177, nil)", got, err)
	}
	// An empty prefix is zero usage, not an error.
	got, err = packUsageBytes(context.Background(), bl, "repo_missing")
	if err != nil || got != 0 {
		t.Fatalf("packUsageBytes(repo_missing) = (%d, %v), want (0, nil)", got, err)
	}
}

// TestHookUsageFloor (Codex rounds 21+30): the pre-receive hook is a
// fresh process per push, so per-push self-listings of the pack and LFS
// prefixes amplify against the shared backend. The serve process mints
// "<bytes>:<unix mint time>" into EnvUsageFloor from its shared cache;
// the hook must use a FRESH floor WITHOUT listing (nil blob store proves
// no listing — StorageUsageBytes asserts on nil), self-list when the
// floor aged past hookFloorMaxAge (the client controls the mint-to-hook
// gap), self-list on absent or timestampless values (binary skew), and
// fail closed on nonsense (the env comes from our own server).
// openTestRefStore opens a throwaway SQLite ref store (the hook floor
// reads the usage counter at scan start — Codex round 44).
func openTestRefStore(t *testing.T) ref.Store {
	t.Helper()
	st, err := ref.Open(context.Background(), "sqlite://"+t.TempDir()+"/refs.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.InitSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestHookUsageFloor(t *testing.T) {
	now := time.Now().Unix()
	t.Run("fresh-floor-used-without-listing", func(t *testing.T) {
		t.Setenv(EnvUsageFloor, fmt.Sprintf("1234:%d:0", now))
		got, _, err := hookUsageFloor(context.Background(), nil, openTestRefStore(t), "repo_a")
		if err != nil || got != 1234 {
			t.Fatalf("hookUsageFloor = (%d, %v), want (1234, nil)", got, err)
		}
	})
	t.Run("stale-adoption-gen-falls-back-to-listing", func(t *testing.T) {
		// Codex round 49: an adoption on ANY replica during the client's
		// transfer moved listed bytes into the counter — a floor minted
		// under the older generation must self-list.
		st := openTestRefStore(t)
		if err := st.BumpLFSAdoptionGen(context.Background(), "repo_a"); err != nil {
			t.Fatal(err)
		}
		t.Setenv(EnvUsageFloor, fmt.Sprintf("1234:%d:0", now))
		bl := openTestBlob(t)
		seedPack(t, bl, "repo_a", "1111", 100)
		got, _, err := hookUsageFloor(context.Background(), bl, st, "repo_a")
		if err != nil || got != 100 {
			t.Fatalf("stale-gen floor = (%d, %v), want (100, nil) — must self-list", got, err)
		}
	})
	t.Run("gen-lookup-stall-rechecks-age", func(t *testing.T) {
		// Codex round 50: the hook's gen lookup can stall; the trusted
		// age must be re-evaluated afterwards — a floor whose window the
		// stall consumed self-lists instead of being trusted.
		t.Setenv(EnvPackReservationTTL, "90s") // maxAge = 90s − 60s = 30s
		st := &genStallStore{Store: openTestRefStore(t), stall: func() { time.Sleep(1500 * time.Millisecond) }}
		// Minted 29.2s ago: inside maxAge at parse, outside after the stall.
		t.Setenv(EnvUsageFloor, fmt.Sprintf("1234:%d:0", time.Now().Add(-29200*time.Millisecond).Unix()))
		bl := openTestBlob(t)
		seedPack(t, bl, "repo_a", "1111", 100)
		got, _, err := hookUsageFloor(context.Background(), bl, st, "repo_a")
		if err != nil || got != 100 {
			t.Fatalf("stalled-gen floor = (%d, %v), want (100, nil) — the consumed age must self-list", got, err)
		}
	})
	t.Run("malformed-gen-fails-closed", func(t *testing.T) {
		t.Setenv(EnvUsageFloor, fmt.Sprintf("1234:%d:banana", now))
		if _, _, err := hookUsageFloor(context.Background(), nil, nil, "repo_a"); err == nil {
			t.Fatal("malformed adoption gen did not fail closed")
		}
	})
	for name, raw := range map[string]string{
		"malformed-fails-closed":       "not-a-number",
		"negative-fails-closed":        fmt.Sprintf("-1:%d", now),
		"empty-fails-closed":           "",
		"bad-timestamp-fails-closed":   "1234:xyz",
		"bad-bytes-piece-fails-closed": fmt.Sprintf("abc:%d", now),
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(EnvUsageFloor, raw)
			if _, _, err := hookUsageFloor(context.Background(), nil, nil, "repo_a"); err == nil {
				t.Fatalf("hookUsageFloor(%s=%q) did not fail closed", EnvUsageFloor, raw)
			}
		})
	}
	listingCases := map[string]struct {
		raw func() string
		ttl string
	}{
		"absent-falls-back-to-listing":      {raw: func() string { return "" }},
		"stale-floor-falls-back-to-listing": {raw: func() string { return fmt.Sprintf("1234:%d:0", time.Now().Add(-hookFloorMaxAge-time.Minute).Unix()) }},
		// A serve binary predating the adoption generation (round 49)
		// minted this two-field floor: no generation evidence, self-list.
		"genless-legacy-falls-back-to-listing": {raw: func() string { return fmt.Sprintf("1234:%d", time.Now().Unix()) }},
		"timestampless-falls-back-to-listing":  {raw: func() string { return "1234" }},
		// Codex round 32: the trusted age never reaches the CONFIGURED
		// reservation TTL — with a 90s TTL, a 70s-old floor (fresh by the
		// five-minute default) can no longer see an orphan whose
		// reservation expired, so it must self-list.
		"ttl-bounded-age-falls-back-to-listing": {
			raw: func() string { return fmt.Sprintf("1234:%d:0", time.Now().Add(-70*time.Second).Unix()) },
			ttl: "90s",
		},
		// A TTL at or below the mint cache's own staleness trusts nothing.
		"tiny-ttl-always-falls-back-to-listing": {
			raw: func() string { return fmt.Sprintf("1234:%d:0", time.Now().Unix()) },
			ttl: "30s",
		},
		// Codex round 42: the trusted age must also leave the
		// floor-to-reservation HANDOFF margin — with a 90s TTL, a
		// 40s-old floor leaves only 90−30−40 = 20s < the 30s margin, so
		// an orphan's reservation could expire during the reserve call.
		"handoff-margin-falls-back-to-listing": {
			raw: func() string { return fmt.Sprintf("1234:%d:0", time.Now().Add(-40*time.Second).Unix()) },
			ttl: "90s",
		},
	}
	for name, tc := range listingCases {
		t.Run(name, func(t *testing.T) {
			if tc.ttl != "" {
				t.Setenv(EnvPackReservationTTL, tc.ttl)
			}
			raw := tc.raw()
			if raw == "" {
				// t.Setenv registers cleanup; unset AFTER it so the var is
				// absent during the test but restored afterwards.
				t.Setenv(EnvUsageFloor, "sentinel")
				os.Unsetenv(EnvUsageFloor)
			} else {
				t.Setenv(EnvUsageFloor, raw)
			}
			bl := openTestBlob(t)
			seedPack(t, bl, "repo_a", "1111", 100)
			seedLFS(t, bl, "repo_a", strings.Repeat("aa", 32), 7)
			got, _, err := hookUsageFloor(context.Background(), bl, openTestRefStore(t), "repo_a")
			if err != nil || got != 107 {
				t.Fatalf("listing fallback = (%d, %v), want (107, nil) — the untrusted floor must not be used", got, err)
			}
		})
	}
}

// TestServiceRPC_QuotaNeverRejectsAtHTTP pins the HTTP layer's
// quota-neutral contract (Codex rounds 3–6): even a repo far over quota is
// NOT rejected at HTTP time — the request proceeds toward receive-pack
// (observable here as hydrate's nil-Store assert), because only
// pre-receive's content-addressed dedup can soundly decide. Auth ordering
// is unchanged: 401s keep their Basic challenge (RFC 9110 §11.6.1).
func TestServiceRPC_QuotaNeverRejectsAtHTTP(t *testing.T) {
	key, pemBlock := genKeyPEM(t, "ES256")
	bl := openTestBlob(t)
	seedPack(t, bl, "repo_quota1", "aaaa", 64)
	srv := &Server{
		Blob:     bl,
		CacheDir: t.TempDir(),
		Verifier: auth.NewVerifier(memKeys{"org1": {pemBlock}}),
		Org:      "org1",
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != "repo_quota1" {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: "repo_quota1", URL: "quota-repo", DefaultBranch: "main"}, true
		},
		PackQuotaBytes: 1, // usage (64) far over quota (1)
	}
	token := mintUnitToken(t, key, "ES256", unitClaims("org1", "repo_quota1", auth.ScopeGitWrite))

	// Over-quota receive-pack proceeds past the pressure check into
	// hydrate (nil Store => loud panic, proving no quota rejection).
	req := httptest.NewRequest(http.MethodPost, "/repo_quota1.git/git-receive-pack", strings.NewReader("0000"))
	req.SetBasicAuth("t", token)
	rec := httptest.NewRecorder()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected nil-Store panic past the quota pressure check; got %d: %s", rec.Code, rec.Body.String())
			}
		}()
		srv.Handler().ServeHTTP(rec, req)
	}()

	// Auth stays ahead of everything: a missing token on the same
	// over-quota repo is a 401 carrying the Basic challenge.
	req = httptest.NewRequest(http.MethodPost, "/repo_quota1.git/git-receive-pack", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
		t.Fatalf("WWW-Authenticate = %q, want Basic challenge preserved", got)
	}

	// A blob-listing outage in the pressure check must not become a write
	// outage (Codex round 7): the request still proceeds toward
	// receive-pack (nil-Store panic), never a 500. A separate Server:
	// swapping Blob on the live one would race its background refresh.
	srv = &Server{
		Blob:           failingListBlob{},
		CacheDir:       t.TempDir(),
		Verifier:       srv.Verifier,
		Org:            srv.Org,
		ResolveRepo:    srv.ResolveRepo,
		PackQuotaBytes: srv.PackQuotaBytes,
	}
	req = httptest.NewRequest(http.MethodPost, "/repo_quota1.git/git-receive-pack", strings.NewReader("0000"))
	req.SetBasicAuth("t", token)
	rec = httptest.NewRecorder()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected nil-Store panic past the failed pressure check; got %d: %s", rec.Code, rec.Body.String())
			}
		}()
		srv.Handler().ServeHTTP(rec, req)
	}()
}
