package githttp

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
