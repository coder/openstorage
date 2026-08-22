package githttp

// Unit tests for the pack-bytes quota support code (issue #21, quota.go):
// pack-prefix usage accounting, the pressure-check TTL cache (listing-work
// bounds + per-repo miss coalescing), and the HTTP layer's quota-neutral
// behavior (auth ordering preserved; no quota rejection at HTTP time —
// pre-receive owns enforcement). Uses the zero-container fileblob backend;
// the enforcement halves live in quota_e2e_test.go.

import (
	"context"
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

	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/meta"
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
	for i := 0; i < 5; i++ {
		if usage, ok := srv.peekUsage("repo_c"); !ok || usage != 10 {
			t.Fatalf("cached peek %d: (%d, %v), want (10, true)", i, usage, ok)
		}
	}
	// One refresh = two listings (base + namespace prefixes roll up).
	if cb.lists.Load() != 2 {
		t.Fatalf("cold+cached peeks cost %d listings, want 2", cb.lists.Load())
	}

	// A different repo has its own entry (one more listing)...
	srv.peekUsage("repo_d")
	awaitPeek(t, srv, "repo_d")
	if cb.lists.Load() != 4 {
		t.Fatalf("second repo cost %d total listings, want 4", cb.lists.Load())
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
	if cb.lists.Load() != 6 {
		t.Fatalf("post-TTL refresh cost %d total listings, want 6", cb.lists.Load())
	}
}

// blockingBlob parks every ListPages call until release is closed,
// counting entries, so tests can hold refresh slots deterministically.
type blockingBlob struct {
	blob.BlobStore
	release chan struct{}
	calls   atomic.Int64
}

func (b *blockingBlob) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	b.calls.Add(1)
	<-b.release
	return b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

// TestPackUsageRefreshConcurrencyBounded (Codex round 10): background
// refreshes are bounded node-wide by usageRefreshSlots — distinct-repo cold
// peeks beyond the pool size start NO goroutine (deterministic: no flight
// exists, so nothing can list) and are picked up once slots free.
func TestPackUsageRefreshConcurrencyBounded(t *testing.T) {
	slots := cap(usageRefreshSlots)
	bb := &blockingBlob{BlobStore: openTestBlob(t), release: make(chan struct{})}
	srv := &Server{Blob: bb}

	// Fill every slot with distinct repos; their flights park in ListPages.
	for i := 0; i < slots; i++ {
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
	for i := 0; i < 10; i++ {
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
	// Each completed refresh lists twice (base + namespace prefixes).
	if got := bb.calls.Load(); got != int64(slots+1)*2 {
		t.Fatalf("total listings = %d, want %d", got, (slots+1)*2)
	}
}

// TestPackUsageCachePruneIsTimeBounded (Codex round 8): inserts drop
// expired entries at most once per usageCachePruneInterval — never a
// per-insert full-map walk — and fresh entries survive the sweep.
func TestPackUsageCachePruneIsTimeBounded(t *testing.T) {
	now := time.Unix(1000, 0)
	c := &usageCache{now: func() time.Time { return now }}

	c.mu.Lock()
	for i := 0; i < 2000; i++ {
		c.putLocked(fmt.Sprintf("repo_%04d", i), 1)
	}
	if got := len(c.entries); got != 2000 {
		t.Fatalf("fresh entries pruned early: %d, want 2000", got)
	}

	// Past the prune interval every old entry has also expired (TTL ==
	// interval); the next insert sweeps them all, keeping only itself.
	now = now.Add(usageCachePruneInterval + time.Second)
	c.putLocked("repo_new", 1)
	if got := len(c.entries); got != 1 {
		t.Fatalf("post-interval insert kept %d entries, want 1", got)
	}
	if _, ok := c.entries["repo_new"]; !ok {
		t.Fatal("the fresh insert itself was pruned")
	}

	// Immediately after a sweep, inserts must not sweep again (lastPrune
	// advanced): an expired straggler survives until the next interval.
	c.entries["repo_stale"] = usageEntry{usage: 1, expires: now.Add(-time.Second)}
	c.putLocked("repo_new2", 1)
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
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.peekUsage("repo_e") // cold: returns !ok, may start the one flight
		}()
	}
	wg.Wait()
	if got := awaitPeek(t, srv, "repo_e"); got != 10 {
		t.Fatalf("refreshed usage = %d, want 10", got)
	}
	// One coalesced refresh = two listings (base + namespace prefixes).
	if got := cb.lists.Load(); got != 2 {
		t.Fatalf("%d concurrent cold peeks cost %d listings, want 2", goroutines, got)
	}
}

func TestPackUsageBytes(t *testing.T) {
	bl := openTestBlob(t)
	seedPack(t, bl, "repo_a", "1111", 100)
	seedPack(t, bl, "repo_a", "2222", 50)
	// Namespaces roll up under the base repo (Codex round 15: one token
	// writes to all of them, so they share the quota); other repos do not.
	seedPack(t, bl, "repo_a+ephemeral", "4444", 1000)
	seedPack(t, bl, "repo_a+import", "5555", 20)
	seedPack(t, bl, "repo_b", "3333", 4000)

	got, err := packUsageBytes(context.Background(), bl, "repo_a")
	if err != nil {
		t.Fatal(err)
	}
	if got != 1170 {
		t.Fatalf("packUsageBytes(repo_a) = %d, want 1170 (base+namespaces)", got)
	}
	// A namespaced key measures the same rolled-up usage.
	got, err = packUsageBytes(context.Background(), bl, "repo_a+ephemeral")
	if err != nil || got != 1170 {
		t.Fatalf("packUsageBytes(repo_a+ephemeral) = (%d, %v), want (1170, nil)", got, err)
	}
	// An empty prefix is zero usage, not an error.
	got, err = packUsageBytes(context.Background(), bl, "repo_missing")
	if err != nil || got != 0 {
		t.Fatalf("packUsageBytes(repo_missing) = (%d, %v), want (0, nil)", got, err)
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
