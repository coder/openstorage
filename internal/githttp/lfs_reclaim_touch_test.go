package githttp

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

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
