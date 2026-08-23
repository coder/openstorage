// Hydrate short-circuit tests (issue #4): an unchanged repo generation skips
// ListPacks/ListRefs (and therefore every SyncRefs subprocess — SyncRefs is
// only reachable with a ListRefs result), a mutation-bumped generation
// invalidates the marker, the TTL forces a periodic full sync, and the
// marker advances only after a fully successful hydrate. The countable fake
// store keeps these container-free; they run identically on both TestMain
// lanes.
package githttp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

// countingStore is a ref.RefStore whose read methods count calls. Methods
// that Hydrate never touches panic loudly, so the fast path cannot silently
// grow new store dependencies without this test noticing.
type countingStore struct {
	mu   sync.Mutex
	gen  int64
	refs map[string]string

	refsErr  error         // returned by ListRefs when set
	refsGate chan struct{} // when set, ListRefs blocks until it closes

	listPacksCalls, listRefsCalls, getGenerationCalls int
}

func (s *countingStore) GetGeneration(context.Context, string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getGenerationCalls++
	return s.gen, nil
}

func (s *countingStore) ListPacks(context.Context, string) ([]ref.Pack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listPacksCalls++
	return nil, nil
}

func (s *countingStore) HasPack(context.Context, string, string) (bool, error) {
	return false, nil
}

func (s *countingStore) ReplacePacks(context.Context, string, []ref.Pack, ref.Pack) (bool, error) {
	panic("Hydrate must never replace packs")
}

func (s *countingStore) ReservePackBytes(context.Context, []string, []ref.Pack, string, int64, int64, time.Duration) (ref.ReserveDecision, error) {
	panic("Hydrate must never reserve pack bytes")
}

func (s *countingStore) SweepExpiredPackReservations(context.Context, time.Duration) (int64, error) {
	panic("Hydrate must never sweep pack reservations")
}

func (s *countingStore) IsRetainedPack(context.Context, string, string) (bool, error) {
	panic("Hydrate must never read retained packs")
}

func (s *countingStore) ReclaimRetainedPack(context.Context, string, string) (int64, error) {
	panic("Hydrate must never reclaim retained packs")
}

func (s *countingStore) RegisterLFSObject(context.Context, string, string, int64) error {
	panic("Hydrate must never register LFS objects")
}

func (s *countingStore) HasLFSObject(context.Context, string, string) (bool, error) {
	panic("Hydrate must never read LFS objects")
}

func (s *countingStore) CountLFSObjects(context.Context, string) (int64, error) {
	panic("Hydrate must never count LFS objects")
}

func (s *countingStore) GuardedLFSDelete(context.Context, string, string, func(context.Context) error) (bool, error) {
	panic("Hydrate must never delete LFS blobs")
}

func (s *countingStore) QuotaLedgerState(context.Context, []string, time.Duration, ...string) (ref.PackQuotaLedgerState, error) {
	panic("Hydrate must never read the quota ledger")
}

func (s *countingStore) RetirePackAttempts(context.Context, []string, map[string]ref.ReservationToken) error {
	panic("Hydrate must never retire pack reservations")
}

func (s *countingStore) ListRefs(context.Context, string) (map[string]string, error) {
	s.mu.Lock()
	s.listRefsCalls++
	gate := s.refsGate
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refsErr != nil {
		return nil, s.refsErr
	}
	out := make(map[string]string, len(s.refs))
	for k, v := range s.refs {
		out[k] = v
	}
	return out, nil
}

func (s *countingStore) counts() (listPacks, listRefs, getGeneration int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listPacksCalls, s.listRefsCalls, s.getGenerationCalls
}

func (s *countingStore) setGen(gen int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen = gen
}

func (s *countingStore) ListRefsWithMeta(context.Context, string) ([]ref.RefInfo, error) {
	panic("countingStore: Hydrate must not call ListRefsWithMeta")
}

func (s *countingStore) GetRef(context.Context, string, string) (string, error) {
	panic("countingStore: Hydrate must not call GetRef")
}

func (s *countingStore) CASRef(context.Context, string, string, string, string) (bool, error) {
	panic("countingStore: Hydrate must not call CASRef")
}

func (s *countingStore) RegisterPacks(context.Context, string, []ref.Pack) error {
	panic("countingStore: Hydrate must not call RegisterPacks")
}

// hydrateEnv returns a fake store at generation 7, a real fileblob store
// (never touched: the fake registers no packs), and a fresh cache dir.
func hydrateEnv(t *testing.T) (*countingStore, blob.BlobStore, string) {
	t.Helper()
	st := &countingStore{gen: 7, refs: map[string]string{}}
	bl, err := blob.OpenFile("file://" + filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatalf("blob.OpenFile: %v", err)
	}
	return st, bl, t.TempDir()
}

// mustHydrate runs Hydrate and fails the test on either error return.
func mustHydrate(t *testing.T, st ref.RefStore, bl blob.BlobStore, cacheDir, repo string) string {
	t.Helper()
	repoPath, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, repo, "main")
	if err != nil || refsErr != nil {
		t.Fatalf("Hydrate = (refsErr %v, err %v), want clean", refsErr, err)
	}
	return repoPath
}

func markerContent(t *testing.T, repoPath string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoPath, generationMarker))
	if err != nil {
		t.Fatalf("read generation marker: %v", err)
	}
	return strings.TrimSpace(string(b))
}

func TestHydrate_ShortCircuitsOnUnchangedGeneration(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-gen-idle"

	repoPath := mustHydrate(t, st, bl, cacheDir, repo)
	if lp, lr, _ := st.counts(); lp != 1 || lr != 1 {
		t.Fatalf("first hydrate: ListPacks=%d ListRefs=%d, want 1/1 (full sync)", lp, lr)
	}
	if got := markerContent(t, repoPath); got != "7 refs/heads/main" {
		t.Fatalf("marker = %q, want \"7 refs/heads/main\"", got)
	}

	// Idle repo, back-to-back reads: one generation point-read each, zero
	// ListPacks/ListRefs (and no SyncRefs, which needs a ListRefs result).
	for i := 0; i < 3; i++ {
		if again := mustHydrate(t, st, bl, cacheDir, repo); again != repoPath {
			t.Fatalf("repoPath changed on short-circuit: %q != %q", again, repoPath)
		}
	}
	lp, lr, gg := st.counts()
	if lp != 1 || lr != 1 {
		t.Fatalf("idle rehydrates hit the store: ListPacks=%d ListRefs=%d, want 1/1", lp, lr)
	}
	// The full first hydrate reads the generation twice (pre-lock fast
	// path + under-lock recheck); each short-circuit reads it once.
	if gg != 5 {
		t.Fatalf("GetGeneration calls = %d, want 5 (2 for the full hydrate + 1 per short-circuit)", gg)
	}
}

// A default-branch change must defeat the short-circuit even when the
// ref-store generation is unchanged (issue #18): default-branch updates
// live in the meta row and bump no generation, so the marker additionally
// records the HEAD target the last full sync wrote, and a mismatch forces
// a full sync that repoints HEAD.
func TestHydrate_DefaultBranchChangeInvalidatesShortCircuit(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-gen-head"

	repoPath := mustHydrate(t, st, bl, cacheDir, repo) // full sync, HEAD -> main
	if lp, lr, _ := st.counts(); lp != 1 || lr != 1 {
		t.Fatalf("first hydrate: ListPacks=%d ListRefs=%d, want 1/1 (full sync)", lp, lr)
	}

	// Unchanged generation + fresh marker + changed default branch: the
	// hydrate must do a full sync and repoint HEAD.
	if _, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, repo, "dev"); err != nil || refsErr != nil {
		t.Fatalf("Hydrate(dev) = (refsErr %v, err %v), want clean", refsErr, err)
	}
	if lp, lr, _ := st.counts(); lp != 2 || lr != 2 {
		t.Fatalf("after default-branch change: ListPacks=%d ListRefs=%d, want 2/2 (full sync)", lp, lr)
	}
	if out, err := runGit(repoPath, "symbolic-ref", "HEAD"); err != nil || strings.TrimSpace(out) != "refs/heads/dev" {
		t.Fatalf("HEAD = %q (err %v), want refs/heads/dev", strings.TrimSpace(out), err)
	}
	if got := markerContent(t, repoPath); got != "7 refs/heads/dev" {
		t.Fatalf("marker = %q, want \"7 refs/heads/dev\"", got)
	}

	// Same branch again: the short-circuit is restored (no store scans).
	if _, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, repo, "dev"); err != nil || refsErr != nil {
		t.Fatalf("Hydrate(dev, again) = (refsErr %v, err %v), want clean", refsErr, err)
	}
	if lp, lr, _ := st.counts(); lp != 2 || lr != 2 {
		t.Fatalf("unchanged-branch rehydrate hit the store: ListPacks=%d ListRefs=%d, want 2/2", lp, lr)
	}
}

// TestHydrate_ConcurrentFullSyncsSerialize pins the fix for the marker
// ordering race (PR #14 review): overlapping full hydrates spanning a
// mutation must serialize on the per-repo hydrate lock, so an older ref
// sync can never land after a newer one whose marker survives. Hydrate A is
// held mid-sync (blocked inside ListRefs, holding the lock) while B — at a
// newer generation — must wait rather than sync-and-publish underneath it.
func TestHydrate_ConcurrentFullSyncsSerialize(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-gen-serialize"
	gate := make(chan struct{})
	st.refsGate = gate

	aDone := make(chan error, 1)
	go func() {
		_, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, repo, "main")
		if err == nil {
			err = refsErr
		}
		aDone <- err
	}()
	// Wait until A is inside ListRefs — past its generation reads (gen 7),
	// holding the hydrate lock.
	waitFor(t, func() bool { _, lr, _ := st.counts(); return lr == 1 })

	// The mutation lands, and B starts a hydrate at the new generation.
	st.setGen(8)
	bDone := make(chan error, 1)
	go func() {
		_, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, repo, "main")
		if err == nil {
			err = refsErr
		}
		bDone <- err
	}()
	// B must park on the hydrate lock: its pre-lock GetGeneration runs (A's
	// 2 + B's 1 = 3 calls) but it must not reach its under-lock re-read or
	// ListRefs while A holds the lock.
	waitFor(t, func() bool { _, _, gg := st.counts(); return gg == 3 })
	time.Sleep(100 * time.Millisecond) // would be long enough for B to barge
	if _, lr, gg := st.counts(); lr != 1 || gg != 3 {
		t.Fatalf("B progressed under A's lock: ListRefs=%d GetGeneration=%d, want 1/3", lr, gg)
	}

	close(gate) // release A's sync
	if err := <-aDone; err != nil {
		t.Fatalf("hydrate A: %v", err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("hydrate B: %v", err)
	}

	// B ran strictly after A: it re-read the generation under the lock (8),
	// full-synced, and published last — marker must hold the NEWEST
	// generation, matching the refs actually in the cache.
	if got := markerContent(t, RepoPath(cacheDir, repo)); got != "8 refs/heads/main" {
		t.Fatalf("marker = %q, want \"8 refs/heads/main\" (newest sync publishes last)", got)
	}
	if lp, lr, _ := st.counts(); lp != 2 || lr != 2 {
		t.Fatalf("ListPacks=%d ListRefs=%d, want 2/2 (exactly one full sync each)", lp, lr)
	}
}

// TestHydrate_LockWaitHonorsCancellation pins the cancelable lock wait
// (PR #14 review): a full hydrate parked on the repo-wide hydrate lock must
// return promptly with the context's error when its client hangs up, not
// stay pinned until the lock holder finishes.
func TestHydrate_LockWaitHonorsCancellation(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-gen-cancel"

	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	// Impersonate a stalled hydrate: hold the lock for the whole test.
	unlock, err := flockExclusive(filepath.Join(repoPath, "codestorage.hydrate.lock"))
	if err != nil {
		t.Fatalf("hold hydrate lock: %v", err)
	}
	defer unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, refsErr, err := Hydrate(ctx, st, bl, cacheDir, repo, "main")
		if err == nil {
			err = refsErr
		}
		done <- err
	}()
	// Hydrate is past its pre-lock generation read and parked on the lock
	// (no marker exists, so it cannot short-circuit).
	waitFor(t, func() bool { _, _, gg := st.counts(); return gg == 1 })
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Hydrate returned before cancel: %v", err)
	default:
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Hydrate = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Hydrate stayed parked on the hydrate lock after cancellation")
	}
	// The canceled waiter must not have synced or published anything.
	if lp, lr, _ := st.counts(); lp != 0 || lr != 0 {
		t.Fatalf("canceled waiter hit the store: ListPacks=%d ListRefs=%d, want 0/0", lp, lr)
	}
	if _, err := os.Stat(filepath.Join(repoPath, generationMarker)); !os.IsNotExist(err) {
		t.Fatalf("canceled waiter left a marker (stat err %v)", err)
	}
}

// waitFor polls cond until it holds (test-scoped; fails after ~5s).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached within 5s")
}

func TestHydrate_MutationInvalidatesShortCircuit(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-gen-invalidate"

	repoPath := mustHydrate(t, st, bl, cacheDir, repo)

	// A mutation bumped the store generation: the next hydrate must do the
	// full sync and re-arm the marker at the new generation.
	st.setGen(8)
	mustHydrate(t, st, bl, cacheDir, repo)
	if lp, lr, _ := st.counts(); lp != 2 || lr != 2 {
		t.Fatalf("post-mutation hydrate: ListPacks=%d ListRefs=%d, want 2/2 (full sync)", lp, lr)
	}
	if got := markerContent(t, repoPath); got != "8 refs/heads/main" {
		t.Fatalf("marker = %q, want \"8 refs/heads/main\"", got)
	}

	mustHydrate(t, st, bl, cacheDir, repo)
	if lp, lr, _ := st.counts(); lp != 2 || lr != 2 {
		t.Fatalf("re-armed short-circuit hit the store: ListPacks=%d ListRefs=%d, want 2/2", lp, lr)
	}
}

func TestHydrate_TTLForcesFullSync(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-gen-ttl"

	repoPath := mustHydrate(t, st, bl, cacheDir, repo)

	// Age the marker past the TTL: the generation still matches, but the
	// staleness guard must force a full sync anyway (a missed bump anywhere
	// would otherwise serve stale forever).
	stale := time.Now().Add(-generationMarkerTTL - time.Second)
	if err := os.Chtimes(filepath.Join(repoPath, generationMarker), stale, stale); err != nil {
		t.Fatal(err)
	}
	mustHydrate(t, st, bl, cacheDir, repo)
	if lp, lr, _ := st.counts(); lp != 2 || lr != 2 {
		t.Fatalf("TTL-expired hydrate: ListPacks=%d ListRefs=%d, want 2/2 (full sync)", lp, lr)
	}

	// The full sync rewrote the marker, restarting the TTL window.
	mustHydrate(t, st, bl, cacheDir, repo)
	if lp, lr, _ := st.counts(); lp != 2 || lr != 2 {
		t.Fatalf("refreshed marker did not short-circuit: ListPacks=%d ListRefs=%d, want 2/2", lp, lr)
	}
}

func TestHydrate_MarkerWrittenOnlyAfterSuccess(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-gen-failure"

	// Failure leg 1: the ref listing itself fails — Hydrate errors, and no
	// marker may exist (a marker would short-circuit the retry into serving
	// a never-synced repo).
	st.refsErr = errors.New("synthetic ListRefs outage")
	_, _, err := Hydrate(context.Background(), st, bl, cacheDir, repo, "main")
	if err == nil {
		t.Fatal("Hydrate must fail when ListRefs fails")
	}
	repoPath := RepoPath(cacheDir, repo)
	if _, err := os.Stat(filepath.Join(repoPath, generationMarker)); !os.IsNotExist(err) {
		t.Fatalf("failed hydrate left a marker (stat err %v)", err)
	}

	// Failure leg 2: ListRefs succeeds but SyncRefs cannot apply the result
	// (ref points at an object the cache repo does not have). refsErr is
	// reported and the marker still must not advance.
	st.refsErr = nil
	st.refs = map[string]string{"refs/heads/main": strings.Repeat("ab", 20)}
	_, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, repo, "main")
	if err != nil {
		t.Fatalf("Hydrate err = %v, want refsErr-only failure", err)
	}
	if refsErr == nil {
		t.Fatal("SyncRefs must fail for a missing object")
	}
	if _, err := os.Stat(filepath.Join(repoPath, generationMarker)); !os.IsNotExist(err) {
		t.Fatalf("refsErr hydrate left a marker (stat err %v)", err)
	}

	// Success finally writes it.
	st.refs = map[string]string{}
	mustHydrate(t, st, bl, cacheDir, repo)
	if got := markerContent(t, repoPath); got != "7 refs/heads/main" {
		t.Fatalf("marker = %q, want \"7 refs/heads/main\"", got)
	}
}
