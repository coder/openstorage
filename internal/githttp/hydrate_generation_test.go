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

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// countingStore is a ref.RefStore whose read methods count calls. Methods
// that Hydrate never touches panic loudly, so the fast path cannot silently
// grow new store dependencies without this test noticing.
type countingStore struct {
	mu   sync.Mutex
	gen  int64
	refs map[string]string

	refsErr error // returned by ListRefs when set

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

func (s *countingStore) ListRefs(context.Context, string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listRefsCalls++
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
	repoPath, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, repo)
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
	if got := markerContent(t, repoPath); got != "7" {
		t.Fatalf("marker = %q, want \"7\"", got)
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
	if gg != 4 {
		t.Fatalf("GetGeneration calls = %d, want 4 (one per Hydrate)", gg)
	}
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
	if got := markerContent(t, repoPath); got != "8" {
		t.Fatalf("marker = %q, want \"8\"", got)
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
	_, _, err := Hydrate(context.Background(), st, bl, cacheDir, repo)
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
	_, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, repo)
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
	if got := markerContent(t, repoPath); got != "7" {
		t.Fatalf("marker = %q, want \"7\"", got)
	}
}
