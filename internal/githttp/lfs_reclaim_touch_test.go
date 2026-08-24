package githttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

// lfsMarkSurface is the reclamation-mark slice of the concrete ref store
// the touch handshake test drives (jobs.LFSReclaimRegistry mirrors it).
type lfsMarkSurface interface {
	MarkLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error)
	ListRepoLFSObjects(ctx context.Context, repo, after string, limit int) ([]ref.LFSObjectRow, error)
}

// TestLFSBatchUpload_StoredAnswerClearsReclamationMark (issue #73): a batch
// "stored, no actions" answer promises the client it may commit a pointer
// referencing the OID, so it must clear the reclamation mark — otherwise a
// scan verdict minted before the batch could retire the row the moment the
// client pushes.
func TestLFSBatchUpload_StoredAnswerClearsReclamationMark(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	writeTok := mint("git:write")
	ctx := context.Background()
	content := []byte("touch handshake bytes")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	// Store + register the object, then mark it unreferenced (a past scan
	// observed no reachable pointer).
	scratch := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(scratch, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, key, scratch); err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	marks, ok := srv.Store.(interface {
		MarkLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error)
	})
	if !ok {
		t.Fatalf("store %T lacks the reclamation mark surface", srv.Store)
	}
	if n, err := marks.MarkLFSObjectsUnreferenced(ctx, lfsTestRepoID, []string{key}); err != nil || n != 1 {
		t.Fatalf("mark = (%d, %v), want 1", n, err)
	}

	// The batch answers "stored" (no actions) — and the mark is gone.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+writeTok,
		batchBody("upload", oid, int64(len(content)))))
	if o := resp.Objects[0]; o.Error != nil || len(o.Actions) != 0 {
		t.Fatalf("stored batch object = %+v, want no actions and no error", o)
	}
	if touched, err := srv.Store.TouchLFSObject(ctx, lfsTestRepoID, key); err != nil || !touched {
		t.Fatalf("row missing after stored answer: (%v, %v)", touched, err)
	}
	rows, ok := srv.Store.(lfsMarkSurface)
	if !ok {
		t.Fatalf("store %T lacks the row listing surface", srv.Store)
	}
	listed, err := rows.ListRepoLFSObjects(ctx, lfsTestRepoID, "", 10)
	if err != nil || len(listed) != 1 {
		t.Fatalf("rows = (%+v, %v), want 1", listed, err)
	}
	if !listed[0].UnreferencedSince.IsZero() {
		t.Fatalf("stored answer left the reclamation mark: %+v", listed[0])
	}
}

// TestLFSDirectPUT_StoredFastPathClearsReclamationMark (issue #73, review
// round 1): a direct PUT of an already-registered object answers 200 via
// adoptStoredLFSObject's registered fast path without re-registering — it
// must clear the reclamation mark exactly like the batch "stored" answer,
// or a grace-expired mark could retire the row before the client pushes
// its pointer.
func TestLFSDirectPUT_StoredFastPathClearsReclamationMark(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	writeTok := mint("git:write")
	ctx := context.Background()
	content := []byte("direct put handshake bytes")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	scratch := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(scratch, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, key, scratch); err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	marks, ok := srv.Store.(lfsMarkSurface)
	if !ok {
		t.Fatalf("store %T lacks the reclamation mark surface", srv.Store)
	}
	if n, err := marks.MarkLFSObjectsUnreferenced(ctx, lfsTestRepoID, []string{key}); err != nil || n != 1 {
		t.Fatalf("mark = (%d, %v), want 1", n, err)
	}

	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	if w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+writeTok, string(content)); w.Code != http.StatusOK {
		t.Fatalf("re-PUT of stored object status = %d, body %s", w.Code, w.Body)
	}
	listed, err := marks.ListRepoLFSObjects(ctx, lfsTestRepoID, "", 10)
	if err != nil || len(listed) != 1 {
		t.Fatalf("rows = (%+v, %v), want 1", listed, err)
	}
	if !listed[0].UnreferencedSince.IsZero() {
		t.Fatalf("stored fast-path PUT left the reclamation mark: %+v", listed[0])
	}
}

// TestLFSDirectPUT_RetiredMidBodyDemotesToNewGrowth (issue #73, review
// round 9): a PUT admitted as a zero-growth repair snapshots its verdict
// before the body streams — and retirement can retire and refund that row
// mid-body, letting another admission spend the freed headroom. The
// post-body registry re-read must demote the PUT to real new-growth
// admission, denying it at the quota edge instead of registering bytes no
// admission covers.
func TestLFSDirectPUT_RetiredMidBodyDemotesToNewGrowth(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	srv.PackQuotaBytes = 100
	writeTok := mint("git:write")
	ctx := context.Background()
	content := bytes.Repeat([]byte("x"), 60)
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	// A registered-but-blobless row: the PUT enters as a zero-growth
	// repair, with 60 of the 100-byte quota credited to the row.
	if err := srv.Store.RegisterLFSObject(ctx, lfsTestRepoID, key, 60); err != nil {
		t.Fatal(err)
	}
	retire, ok := srv.Store.(interface {
		MarkLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error)
		RetireUnreferencedLFSObject(ctx context.Context, repo, key string, pinnedGens map[string]int64, grace time.Duration) (bool, error)
		GetGeneration(ctx context.Context, repo string) (int64, error)
	})
	if !ok {
		t.Fatalf("store %T lacks the retirement surface", srv.Store)
	}

	pr, pw := io.Pipe()
	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	req := httptest.NewRequest(http.MethodPut, objPath, pr)
	req.ContentLength = int64(len(content))
	req.SetBasicAuth("t", writeTok)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.Handler().ServeHTTP(w, req)
	}()

	// The synchronous pipe write returns only once the handler's body copy
	// consumed the bytes — the pre-body zero-growth snapshot has committed
	// by then, and the handler blocks awaiting the rest.
	if _, err := pw.Write(content[:10]); err != nil {
		t.Fatal(err)
	}
	// Mid-body: the reclaimer retires the row (explicitly waived grace)
	// and refunds its 60 bytes; a different object's registration then
	// spends the freed headroom.
	if n, err := retire.MarkLFSObjectsUnreferenced(ctx, lfsTestRepoID, []string{key}); err != nil || n != 1 {
		t.Fatalf("mark = (%d, %v), want 1", n, err)
	}
	gens := map[string]int64{}
	for _, member := range blob.QuotaRepoGroup(lfsTestRepoID) {
		g, err := retire.GetGeneration(ctx, member)
		if err != nil {
			t.Fatal(err)
		}
		gens[member] = g
	}
	if retired, err := retire.RetireUnreferencedLFSObject(ctx, lfsTestRepoID, key, gens, 0); err != nil || !retired {
		t.Fatalf("retire = (%v, %v), want retired", retired, err)
	}
	other := blob.LFSKey(lfsTestRepoID, sha256Hex([]byte("other")))
	if err := srv.Store.RegisterLFSObject(ctx, lfsTestRepoID, other, 60); err != nil {
		t.Fatal(err)
	}
	if _, err := pw.Write(content[10:]); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	<-done

	// 60 already-counted bytes elsewhere + 60 incoming > 100: the demoted
	// PUT must be denied, not registered as uncovered zero-growth.
	if w.Code != http.StatusInsufficientStorage {
		t.Fatalf("mid-body-retired PUT status = %d, body %s", w.Code, w.Body)
	}
}

// markAfterRegisterStore stamps the reclamation mark the moment a
// registration for key commits — the deterministic stand-in for a scan
// verdict landing in the window between the registration and the handler's
// success answer (issue #73, review round 13), which the sweep-race probe
// and the size-scaled compat copy can stretch past a short grace.
type markAfterRegisterStore struct {
	ref.RefStore
	key string
}

func (m *markAfterRegisterStore) markNow(ctx context.Context, repo, key string) error {
	if key != m.key {
		return nil
	}
	if n, err := m.RefStore.(lfsMarkSurface).MarkLFSObjectsUnreferenced(ctx, repo, []string{key}); err != nil || n != 1 {
		return fmt.Errorf("mid-flight mark = (%d, %v), want 1", n, err)
	}
	return nil
}

func (m *markAfterRegisterStore) RegisterLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	if err := m.RefStore.RegisterLFSObject(ctx, repo, key, bytes); err != nil {
		return err
	}
	return m.markNow(ctx, repo, key)
}

func (m *markAfterRegisterStore) RegisterAdoptedLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	if err := m.RefStore.RegisterAdoptedLFSObject(ctx, repo, key, bytes); err != nil {
		return err
	}
	return m.markNow(ctx, repo, key)
}

// capRaceCountStore makes the adoption cap precheck observe a full
// repository whose last slot was just taken by a twin registration of the
// very key being adopted — with a scan verdict already stamped on the
// twin's fresh row (issue #73, review round 13).
type capRaceCountStore struct {
	ref.RefStore
	key   string
	bytes int64
	once  sync.Once
	err   error
}

func (c *capRaceCountStore) CountLFSObjects(ctx context.Context, repo string) (int64, error) {
	c.once.Do(func() {
		if err := c.RefStore.RegisterLFSObject(ctx, repo, c.key, c.bytes); err != nil {
			c.err = err
			return
		}
		if n, err := c.RefStore.(lfsMarkSurface).MarkLFSObjectsUnreferenced(ctx, repo, []string{c.key}); err != nil || n != 1 {
			c.err = fmt.Errorf("twin mark = (%d, %v), want 1", n, err)
		}
	})
	if c.err != nil {
		return 0, c.err
	}
	return ref.MaxLFSObjectsPerRepo, nil
}

// assertLFSRowUnmarked fails when key's row is missing or still carries a
// reclamation mark.
func assertLFSRowUnmarked(t *testing.T, st ref.RefStore, key, when string) {
	t.Helper()
	rows, ok := st.(lfsMarkSurface)
	if !ok {
		t.Fatalf("store %T lacks the row listing surface", st)
	}
	listed, err := rows.ListRepoLFSObjects(context.Background(), lfsTestRepoID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range listed {
		if row.Key != key {
			continue
		}
		if !row.UnreferencedSince.IsZero() {
			t.Fatalf("%s left the reclamation mark: %+v", when, row)
		}
		return
	}
	t.Fatalf("%s: row %s missing (rows: %+v)", when, key, listed)
}

// TestLFSDirectPUT_LateAdoptionRegistrationRetouches (issue #73, review
// round 13): a rowless stored object adopted by a direct PUT registers its
// row BEFORE the sweep-race probe and bridge copy — a scan verdict landing
// in that window must not survive the 200, or a short grace could retire
// the row before the client pushes its pointer.
func TestLFSDirectPUT_LateAdoptionRegistrationRetouches(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	writeTok := mint("git:write")
	ctx := context.Background()
	content := []byte("late adoption retouch bytes")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	scratch := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(scratch, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, key, scratch); err != nil {
		t.Fatal(err)
	}
	inner := srv.Store
	srv.Store = &markAfterRegisterStore{RefStore: inner, key: key}

	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	if w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+writeTok, string(content)); w.Code != http.StatusOK {
		t.Fatalf("adopting PUT status = %d, body %s", w.Code, w.Body)
	}
	assertLFSRowUnmarked(t, inner, key, "late adoption registration")
}

// TestLFSDirectPUT_CapRaceWinnerRetouches (issue #73, review round 13): at
// the cap boundary a twin can register the adopted key between the entry
// lookup and the count — the race winner answers success through the
// bridge seam and must re-arm the twin's row like every other write-scoped
// 200.
func TestLFSDirectPUT_CapRaceWinnerRetouches(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	writeTok := mint("git:write")
	ctx := context.Background()
	content := []byte("cap race retouch bytes")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	scratch := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(scratch, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, key, scratch); err != nil {
		t.Fatal(err)
	}
	inner := srv.Store
	srv.Store = &capRaceCountStore{RefStore: inner, key: key, bytes: int64(len(content))}

	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	if w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+writeTok, string(content)); w.Code != http.StatusOK {
		t.Fatalf("cap-race PUT status = %d, body %s", w.Code, w.Body)
	}
	assertLFSRowUnmarked(t, inner, key, "cap-boundary race winner")
}

// TestLFSDirectPUT_FreshUploadRetouches (issue #73, review round 13): a
// fresh proxied upload registers BEFORE the sweep-race verify and compat
// copy; a scan verdict landing in that window must not survive the 200.
func TestLFSDirectPUT_FreshUploadRetouches(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	writeTok := mint("git:write")
	content := []byte("fresh upload retouch bytes")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	inner := srv.Store
	srv.Store = &markAfterRegisterStore{RefStore: inner, key: key}

	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	if w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+writeTok, string(content)); w.Code != http.StatusOK {
		t.Fatalf("fresh PUT status = %d, body %s", w.Code, w.Body)
	}
	assertLFSRowUnmarked(t, inner, key, "fresh upload")
}

// countingTouchStore counts TouchLFSObject calls.
type countingTouchStore struct {
	ref.RefStore
	touches int
}

func (c *countingTouchStore) TouchLFSObject(ctx context.Context, repo, key string) (bool, error) {
	c.touches++
	return c.RefStore.TouchLFSObject(ctx, repo, key)
}

// TestLFSBatchUpload_DuplicateStoredOIDsSettleOnce (issue #73, Codex
// security round 15): a batch may repeat one stored OID hundreds of times,
// and the stored fast path must run once per unique key — per-occurrence
// settlement amplified a small request body into standalone registry
// writes (two touches each) plus blob probes.
func TestLFSBatchUpload_DuplicateStoredOIDsSettleOnce(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	writeTok := mint("git:write")
	ctx := context.Background()
	content := []byte("duplicate batch entry bytes")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	scratch := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(scratch, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, key, scratch); err != nil {
		t.Fatal(err)
	}
	if err := srv.Store.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	counter := &countingTouchStore{RefStore: srv.Store}
	srv.Store = counter

	const dupes = 8
	entry := fmt.Sprintf(`{"oid":%q,"size":%d}`, oid, len(content))
	entries := entry
	for range dupes - 1 {
		entries += "," + entry
	}
	body := `{"operation":"upload","transfers":["basic"],"objects":[` + entries + `]}`
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+writeTok, body))
	if len(resp.Objects) != dupes {
		t.Fatalf("batch answered %d objects, want %d", len(resp.Objects), dupes)
	}
	for i, o := range resp.Objects {
		if o.Error != nil || len(o.Actions) != 0 {
			t.Fatalf("duplicate %d = %+v, want stored (no actions, no error)", i, o)
		}
	}
	// Entry touch + post-compat re-touch: exactly two registry writes for
	// the whole batch, not two per occurrence.
	if counter.touches != 2 {
		t.Fatalf("stored batch touched %d times, want 2 (one settlement per unique key)", counter.touches)
	}
}
