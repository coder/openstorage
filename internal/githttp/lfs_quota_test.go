package githttp

// LFS storage-quota perimeter tests (issue #51): batch-time admission
// through the durable reservation ledger, PUT-time self-admission against
// declared-size lies, settle-on-completion accounting, reservation release
// on failed uploads, TTL expiry, and namespace rollup. Written red-first:
// before the fix, every one of these demonstrated unmetered LFS writes.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

// newLFSQuotaServer wires newLFSServer plus a SQLite ref store and a quota.
func newLFSQuotaServer(t *testing.T, quotaBytes int64, ttl time.Duration) (*Server, ref.Store, func(scopes ...string) string) {
	t.Helper()
	srv, mint := newLFSServer(t, openFileBlob(t))
	st, err := ref.Open(context.Background(), "sqlite://"+t.TempDir()+"/refs.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.InitSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv.Store = st
	srv.PackQuotaBytes = quotaBytes
	srv.PackReservationTTL = ttl
	return srv, st, mint
}

// ledgerState reads the test repo group's quota ledger.
func ledgerState(t *testing.T, st ref.Store, ttl time.Duration) ref.PackQuotaLedgerState {
	t.Helper()
	ls, err := st.QuotaLedgerState(context.Background(), blob.QuotaRepoGroup(lfsTestRepoID), ttl)
	if err != nil {
		t.Fatal(err)
	}
	return ls
}

// TestLFSQuota_OverQuotaBatchDenied: an upload batch whose declared size
// exceeds the quota must be denied per-object (507, no actions) before any
// transfer URL is issued, and must leave no reservation behind.
func TestLFSQuota_OverQuotaBatchDenied(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 100, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("x"), 1000) // 1000 > quota 100
	oid := sha256Hex(content)

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if len(resp.Objects) != 1 {
		t.Fatalf("batch objects = %+v", resp.Objects)
	}
	obj := resp.Objects[0]
	if obj.Error == nil || obj.Error.Code != http.StatusInsufficientStorage {
		t.Fatalf("over-quota upload not denied: %+v", obj)
	}
	if len(obj.Actions) != 0 {
		t.Fatalf("over-quota upload got actions: %+v", obj.Actions)
	}
	if !strings.Contains(obj.Error.Message, "quota") {
		t.Fatalf("denial message %q does not mention the quota", obj.Error.Message)
	}
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != 0 || ls.UsageBytes != 0 {
		t.Fatalf("denied batch left ledger state %+v", ls)
	}
}

// TestLFSQuota_DeclaredSizeLieDeniedAtPut: a batch that declares a tiny
// size is admitted, but the PUT re-admits against the request's actual
// Content-Length — a body exceeding the remaining quota is denied and
// nothing is published.
func TestLFSQuota_DeclaredSizeLieDeniedAtPut(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 100, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("y"), 1000) // real size 1000 > quota 100
	oid := sha256Hex(content)

	// Declared size 10 fits: the batch admits it and issues an action.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, 10)))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions["upload"] == nil {
		t.Fatalf("declared-size-10 batch not admitted: %+v", resp.Objects[0])
	}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusInsufficientStorage {
		t.Fatalf("size-lie PUT status = %d, body %s (want 507)", w.Code, w.Body)
	}
	if _, err := srv.Blob.Stat(context.Background(), blob.LFSKey(lfsTestRepoID, oid)); err == nil {
		t.Fatal("size-lie PUT published the object")
	}
}

// TestLFSQuota_UploadSettlesLedger: a successful proxy upload consumes its
// reservation and credits the usage counter with the actual byte size.
func TestLFSQuota_UploadSettlesLedger(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 1000, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("settled lfs bytes")
	oid := sha256Hex(content)

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error != nil {
		t.Fatalf("batch denied: %+v", resp.Objects[0])
	}
	// Batches are ADVISORY and insert nothing (Codex round 17: batch-time
	// durable holds are rotatable via fresh OIDs).
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != 0 || ls.UsageBytes != 0 {
		t.Fatalf("advisory batch touched the ledger: %+v", ls)
	}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body %s", w.Code, w.Body)
	}
	ls := ledgerState(t, st, time.Hour)
	if ls.UsageBytes != int64(len(content)) || ls.ReservedBytes != 0 {
		t.Fatalf("settled ledger state %+v, want usage=%d reserved=0", ls, len(content))
	}
}

// TestLFSQuota_WrongSHAReservationLifecycle: a proxy upload whose content
// hash does not match the oid is rejected and publishes nothing. Since the
// PUT path takes NO durable reservation before body progress (Codex
// security round 3 + round 16 — its only durable admission runs after the
// verified body, which a hash mismatch never reaches), a failed direct PUT
// leaves the ledger completely untouched; a batch-minted reservation
// (declared size) survives independently until settle or TTL, and a
// correct retry settles it.
func TestLFSQuota_WrongSHAReservationLifecycle(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 1000, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("the real content")
	oid := sha256Hex(content)
	put := func(body string) *httptest.ResponseRecorder {
		return doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
			"basic:"+tok, body)
	}

	// Failed direct PUT: rejected, nothing published, NOTHING reserved.
	w := put("the fake content")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("hash-mismatch PUT status = %d, body %s", w.Code, w.Body)
	}
	if _, err := srv.Blob.Stat(context.Background(), blob.LFSKey(lfsTestRepoID, oid)); err == nil {
		t.Fatal("hash-mismatch PUT published the object")
	}
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != 0 || ls.UsageBytes != 0 {
		t.Fatalf("failed PUT touched the ledger: %+v", ls)
	}

	// A correct retry admits post-body and settles straight into usage.
	if w := put(string(content)); w.Code != http.StatusOK {
		t.Fatalf("correct retry status = %d, body %s", w.Code, w.Body)
	}
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != 0 || ls.UsageBytes != int64(len(content)) {
		t.Fatalf("retry did not settle into usage: %+v", ls)
	}
}

// TestLFSQuota_ReservationTTLExpiryFreesAdmission: a crash-leaked
// reservation (a post-body upload that reserved, then died before
// registering) gates both the batch's advisory check and the PUT's
// admission only until it ages past the TTL.
func TestLFSQuota_ReservationTTLExpiryFreesAdmission(t *testing.T) {
	const ttl = 400 * time.Millisecond
	srv, st, mint := newLFSQuotaServer(t, 100, ttl)
	tok := mint(auth.ScopeGitWrite)
	crashed := bytes.Repeat([]byte("a"), 90)
	second := bytes.Repeat([]byte("b"), 90)
	// Simulate the crash window: the ledger reservation exists (as the
	// PUT's post-body admission would leave it) but no registration ever
	// followed.
	if dec, err := st.ReservePackBytes(context.Background(), blob.QuotaRepoGroup(lfsTestRepoID),
		[]ref.Pack{{Key: blob.LFSKey(lfsTestRepoID, sha256Hex(crashed)), Size: 90}},
		ref.NewReservationOwner(), 0, 100, ttl); err != nil || !dec.OK {
		t.Fatalf("seed reservation: %+v, %v", dec, err)
	}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(second), 90)))
	if resp.Objects[0].Error == nil {
		t.Fatalf("batch admitted against a live reservation: %+v", resp.Objects[0])
	}

	time.Sleep(2 * ttl) // the crash-leaked reservation expires
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(second), 90)))
	if resp.Objects[0].Error != nil {
		t.Fatalf("expired reservation still gates admission: %+v", resp.Objects[0])
	}
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(second),
		"basic:"+tok, string(second))
	if w.Code != http.StatusOK {
		t.Fatalf("post-expiry PUT status = %d, body %s", w.Code, w.Body)
	}
}

// failingLFSStore wraps a ref.Store, failing every LFS-registry call —
// the transient ref-store outage of Codex round 1's P1.
type failingLFSStore struct {
	ref.Store
}

func (failingLFSStore) HasLFSObject(context.Context, string, string) (bool, error) {
	return false, fmt.Errorf("ref store unavailable (injected)")
}

// TestLFSAdopt_FailurePropagates (Codex round 1): a stored-but-rowless
// object whose adoption fails must FAIL the request — a success would let
// the client commit a pointer to a blob the DeleteLFS sweep still
// classifies as an orphan.
func TestLFSAdopt_FailurePropagates(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("stored but rowless")
	oid := sha256Hex(content)
	// Publish the blob directly: stored, no lfs_objects row (pre-#51 state).
	if err := srv.Blob.UploadFile(context.Background(),
		blob.LFSKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Store = failingLFSStore{Store: st}

	// Batch upload naming the stored object: per-object error, not the
	// silent "no actions = already stored" success.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusInternalServerError {
		t.Fatalf("failed adoption reported success: %+v", resp.Objects[0])
	}

	// Re-PUT of the stored object: 500, not 200.
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("failed adoption PUT status = %d, want 500", w.Code)
	}
}

// reapOnRegister deletes the blob just before the registration commits,
// deterministically reproducing Codex round 1's P2 race: a DeleteLFS sweep
// reaps the still-rowless blob after the caller's Stat, with the sweep's
// post-delete registry recheck missing the row.
type reapOnRegister struct {
	ref.Store
	bl  blob.BlobStore
	key string
}

func (r reapOnRegister) RegisterLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	if key == r.key {
		if err := r.bl.Delete(ctx, key); err != nil {
			return fmt.Errorf("test reap: %v", err)
		}
	}
	return r.Store.RegisterLFSObject(ctx, repo, key, bytes)
}

// TestLFSAdopt_RecheckCatchesRacingSweep (Codex round 1): adoption whose
// blob a racing sweep reaped before the registration committed must fail
// the request — the post-registration existence recheck catches it, and the
// client's retry re-uploads for real.
func TestLFSAdopt_RecheckCatchesRacingSweep(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("reaped mid-adoption")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(context.Background(), key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Store = reapOnRegister{Store: st, bl: srv.Blob, key: key}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error == nil {
		t.Fatalf("race-reaped adoption reported stored: %+v", resp.Objects[0])
	}

	// The retry converges: the key is now missing, so the next batch
	// issues a real upload action and the PUT re-publishes (idempotent
	// re-registration — the row already exists).
	srv.Store = st
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions["upload"] == nil {
		t.Fatalf("retry batch did not issue an upload action: %+v", resp.Objects[0])
	}
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("retry PUT status = %d, body %s", w.Code, w.Body)
	}
	if _, err := srv.Blob.Stat(context.Background(), key); err != nil {
		t.Fatalf("retry did not republish the object: %v", err)
	}
}

// TestLFSUpload_SameKeyConcurrencyBounded (Codex security round on #51):
// while one PUT for a key is staging, a second PUT for the same key is
// turned away (429 + Retry-After) BEFORE touching disk — same-key
// reservations refresh rather than sum, so without this guard N identical
// concurrent uploads would each stage a quota-sized temp file. Once the
// winner publishes, a retry succeeds via the stored-object adoption path.
func TestLFSUpload_SameKeyConcurrencyBounded(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("contended lfs object body")
	oid := sha256Hex(content)
	path := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid

	// First PUT: body delivered through a pipe so it parks mid-staging.
	pr, pw := io.Pipe()
	req := httptest.NewRequest(http.MethodPut, path, pr)
	req.ContentLength = int64(len(content))
	req.SetBasicAuth("t", tok)
	first := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.Handler().ServeHTTP(first, req)
	}()
	if _, err := pw.Write(content[:4]); err != nil { // parked: handler holds the key
		t.Fatal(err)
	}

	// Second PUT for the same key while the first stages: rejected.
	w := doLFS(t, srv, http.MethodPut, path, "basic:"+tok, string(content))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("concurrent same-key PUT status = %d, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("429 without Retry-After")
	}

	// Let the winner finish; the retry now succeeds (adoption path).
	if _, err := pw.Write(content[4:]); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	if first.Code != http.StatusOK {
		t.Fatalf("winning PUT status = %d, body %s", first.Code, first.Body)
	}
	w = doLFS(t, srv, http.MethodPut, path, "basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("retry after winner status = %d, body %s", w.Code, w.Body)
	}
}

// TestLFSUpload_NodeStagingBudget (Codex round 4): distinct-key uploads
// stage real disk bytes, so the node-wide budget must gate them before any
// temp file exists — an object bigger than the whole budget is permanent
// (413), one that merely does not fit right now is transient (429), and
// completed uploads return their reservation.
func TestLFSUpload_NodeStagingBudget(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSStagingBytes = 40
	tok := mint(auth.ScopeGitWrite)

	// Permanently over budget: 413.
	huge := bytes.Repeat([]byte("h"), 50)
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(huge),
		"basic:"+tok, string(huge))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("over-budget PUT status = %d, want 413", w.Code)
	}

	// Park one 30-byte upload; a concurrent DISTINCT 30-byte upload no
	// longer fits the 40-byte budget: 429.
	first := bytes.Repeat([]byte("a"), 30)
	second := bytes.Repeat([]byte("b"), 30)
	pr, pw := io.Pipe()
	req := httptest.NewRequest(http.MethodPut,
		"/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(first), pr)
	req.ContentLength = int64(len(first))
	req.SetBasicAuth("t", tok)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); srv.Handler().ServeHTTP(rec, req) }()
	if _, err := pw.Write(first[:4]); err != nil { // parked holding 30 bytes
		t.Fatal(err)
	}
	w = doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(second),
		"basic:"+tok, string(second))
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" {
		t.Fatalf("budget-exhausted PUT = %d (Retry-After %q), want 429 with Retry-After", w.Code, w.Header().Get("Retry-After"))
	}

	// The winner finishes and releases its reservation; the retry fits.
	if _, err := pw.Write(first[4:]); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	if rec.Code != http.StatusOK {
		t.Fatalf("parked PUT status = %d, body %s", rec.Code, rec.Body)
	}
	w = doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(second),
		"basic:"+tok, string(second))
	if w.Code != http.StatusOK {
		t.Fatalf("post-release PUT status = %d, body %s", w.Code, w.Body)
	}
}

// TestLFSUpload_StalledClientReleasesStaging (Codex security round on
// #51): a client that declares a Content-Length and stops transmitting
// must not pin its staging reservation and per-key slot forever — the idle
// read deadline aborts the copy and releases both. Runs over a REAL HTTP
// server (httptest recorders do not support read deadlines).
func TestLFSUpload_StalledClientReleasesStaging(t *testing.T) {
	oldIdle := lfsUploadIdleTimeout
	lfsUploadIdleTimeout = 200 * time.Millisecond
	t.Cleanup(func() { lfsUploadIdleTimeout = oldIdle })

	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSStagingBytes = 100
	tok := mint(auth.ScopeGitWrite)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	content := bytes.Repeat([]byte("s"), 100) // the whole staging budget
	oid := sha256Hex(content)
	url := ts.URL + "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid

	// Stalling PUT: declare 100 bytes, send 4, then park the connection.
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, url, pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(content))
	req.SetBasicAuth("t", tok)
	stalled := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		stalled <- err
	}()
	if _, err := pw.Write(content[:4]); err != nil {
		t.Fatal(err)
	}

	// The idle deadline must abort the stalled upload and release its
	// reservations; a fresh full-budget upload for another oid then fits.
	deadline := time.Now().Add(10 * time.Second)
	other := bytes.Repeat([]byte("t"), 100)
	otherURL := ts.URL + "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + sha256Hex(other)
	for {
		req2, err := http.NewRequest(http.MethodPut, otherURL, bytes.NewReader(other))
		if err != nil {
			t.Fatal(err)
		}
		req2.SetBasicAuth("t", tok)
		resp, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			break // stalled upload's capacity was released
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("competing PUT status = %d, want 200 or transient 429", resp.StatusCode)
		}
		if time.Now().After(deadline) {
			t.Fatal("stalled upload never released the staging budget")
		}
		time.Sleep(50 * time.Millisecond)
	}
	pw.Close()
	<-stalled // the stalled request ends with a connection/HTTP error either way
}

// TestLFSQuota_ZeroGrowthRepairAtQuota (Codex round 5): a registered
// object whose blob vanished re-admits without a fresh charge — an
// at-quota repository must be able to repair itself.
func TestLFSQuota_ZeroGrowthRepairAtQuota(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 100, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("r"), 100) // exactly the whole quota
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	// Registered and fully counted (usage == quota), but the blob is gone
	// (racing DeleteLFS reap).
	if err := st.RegisterLFSObject(context.Background(), lfsTestRepoID, key, 100); err != nil {
		t.Fatal(err)
	}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, 100)))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions["upload"] == nil {
		t.Fatalf("zero-growth repair denied at quota: %+v", resp.Objects[0])
	}
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("zero-growth PUT status = %d, body %s", w.Code, w.Body)
	}
	// Counted once: usage stays at the original credit, nothing reserved.
	if ls := ledgerState(t, st, time.Hour); ls.UsageBytes != 100 || ls.ReservedBytes != 0 {
		t.Fatalf("zero-growth repair changed the ledger: %+v", ls)
	}
	if _, err := srv.Blob.Stat(context.Background(), key); err != nil {
		t.Fatalf("repair did not republish: %v", err)
	}
}

// TestLFSQuota_ObjectCountCap (Codex security round 2): the per-repo
// registered-object population is bounded independent of bytes, at both
// upload entrances, with zero-growth repairs exempt.
func TestLFSQuota_ObjectCountCap(t *testing.T) {
	old := ref.MaxLFSObjectsPerRepo
	ref.MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour) // byte quota DISABLED: the cap must hold anyway
	tok := mint(auth.ScopeGitWrite)
	existing := []byte("the one allowed object")
	if err := st.RegisterLFSObject(context.Background(), lfsTestRepoID,
		blob.LFSKey(lfsTestRepoID, sha256Hex(existing)), int64(len(existing))); err != nil {
		t.Fatal(err)
	}

	// New object via batch: denied with the count-limit message.
	fresh := []byte("one object too many")
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(fresh), int64(len(fresh)))))
	if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusInsufficientStorage ||
		!strings.Contains(resp.Objects[0].Error.Message, "object count limit") {
		t.Fatalf("over-cap batch not denied: %+v", resp.Objects[0])
	}
	// New object via direct PUT: same denial.
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(fresh),
		"basic:"+tok, string(fresh))
	if w.Code != http.StatusInsufficientStorage {
		t.Fatalf("over-cap PUT status = %d, want 507", w.Code)
	}
	// Zero-growth repair of the REGISTERED object stays allowed at cap.
	w = doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(existing),
		"basic:"+tok, string(existing))
	if w.Code != http.StatusOK {
		t.Fatalf("at-cap zero-growth repair status = %d, body %s", w.Code, w.Body)
	}
}

// staleCountStore simulates the concurrent burst at the cap (Codex round
// 9): the fast pre-check reads a stale count (0), so the upload publishes
// and only the ledger's atomic in-transaction cap denies it.
type staleCountStore struct {
	ref.Store
}

func (s staleCountStore) CountLFSObjects(context.Context, string) (int64, error) { return 0, nil }

// TestLFSQuota_AtomicCapLoserLeavesNoBlob (Codex round 9): an upload denied
// by the ATOMIC cap (after the fast check passed on a stale count) must
// remove its just-published blob — DeleteLFS defaults off, so a stranded
// orphan would permanently consume quota per cap loser.
func TestLFSQuota_AtomicCapLoserLeavesNoBlob(t *testing.T) {
	old := ref.MaxLFSObjectsPerRepo
	ref.MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	// The repo is at cap, but the fast pre-check sees a stale count.
	if err := st.RegisterLFSObject(context.Background(), lfsTestRepoID,
		blob.LFSKey(lfsTestRepoID, strings.Repeat("aa", 32)), 10); err != nil {
		t.Fatal(err)
	}
	srv.Store = staleCountStore{Store: st}

	content := []byte("the cap race loser")
	oid := sha256Hex(content)
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusInsufficientStorage {
		t.Fatalf("atomic-cap loser status = %d, body %s (want 507)", w.Code, w.Body)
	}
	if _, err := srv.Blob.Stat(context.Background(), blob.LFSKey(lfsTestRepoID, oid)); err == nil {
		t.Fatal("atomic-cap loser left its blob published")
	}
	if has, err := st.HasLFSObject(context.Background(), lfsTestRepoID, blob.LFSKey(lfsTestRepoID, oid)); err != nil || has {
		t.Fatalf("atomic-cap loser left a registry row: %v, %v", has, err)
	}
}

// TestLFSQuota_TwoMaxInt64BatchDenied (Codex round 21): TWO distinct
// objects each declaring MaxInt64 wrap a naive batch sum negative, and
// ref.Admitted panics its non-negative assertion instead of denying —
// repeatable panic logs for any write-scoped client. The batch's declared
// total must use the ledger's saturating arithmetic and answer 507s.
func TestLFSQuota_TwoMaxInt64BatchDenied(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	body := fmt.Sprintf(
		`{"operation":"upload","transfers":["basic"],"objects":[{"oid":%q,"size":%d},{"oid":%q,"size":%d}]}`,
		strings.Repeat("ab", 32), int64(math.MaxInt64), strings.Repeat("cd", 32), int64(math.MaxInt64))
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok, body))
	if len(resp.Objects) != 2 {
		t.Fatalf("objects = %d, want 2", len(resp.Objects))
	}
	for i, obj := range resp.Objects {
		if obj.Error == nil || obj.Error.Code != http.StatusInsufficientStorage {
			t.Fatalf("overflow object %d not denied with 507: %+v", i, obj)
		}
	}
}

// staleStatBlob reports a stale (smaller) size from Stat while reads
// serve the real stored bytes — the HA race where the blob is replaced
// between the caller's Stat and the verification.
type staleStatBlob struct {
	blob.BlobStore
	staleSize int64
}

func (b staleStatBlob) Stat(context.Context, string) (int64, error) { return b.staleSize, nil }

// TestLFSQuota_AdoptionRegistersVerifiedSize (Codex rounds 23+24): the
// registered size must be the byte count the verification hash actually
// covered, never the caller's pre-verification Stat — in an HA upgrade a
// small corrupt rowless blob can be replaced by the larger correct object
// between the two. With a store whose Stat keeps reporting the stale
// small size, verification hashes a prefix, fails the digest, removes the
// blob under the registration guard, and the PUT re-uploads verified
// bytes — either way pack_usage must record the REAL size (37), never the
// stale Stat (3) that would mint phantom quota headroom.
func TestLFSQuota_AdoptionRegistersVerifiedSize(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
	tok := mint(auth.ScopeGitWrite)

	content := []byte("the larger correct replacement object")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(context.Background(), key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	// Every Stat sees the pre-replacement size; downloads see the truth.
	srv.Blob = staleStatBlob{BlobStore: srv.Blob, staleSize: 3}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("adopting PUT status = %d, body %s", w.Code, w.Body)
	}
	if has, err := st.HasLFSObject(context.Background(), lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("adoption did not register: %v, %v", has, err)
	}
	if ls := ledgerState(t, st, time.Hour); ls.UsageBytes != int64(len(content)) {
		t.Fatalf("registered usage = %d, want %d (the verified download's size, not the stale Stat)",
			ls.UsageBytes, len(content))
	}
}

// downloadCountingBlob counts verification reads (adoptLFSObject's
// verifyStoredLFSObject streams via ReadRange; DownloadFile counted too so
// a regression to disk staging cannot slip past the at-cap test).
type downloadCountingBlob struct {
	blob.BlobStore
	downloads int
}

func (b *downloadCountingBlob) DownloadFile(ctx context.Context, key, destPath string) error {
	b.downloads++
	return b.BlobStore.DownloadFile(ctx, key, destPath)
}

func (b *downloadCountingBlob) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	b.downloads++
	return b.BlobStore.ReadRange(ctx, key, offset, length)
}

// TestLFSQuota_RetryAfterFailedPutNotDoubleCounted (Codex round 24): a
// verified PUT that reserved most of the remaining quota and failed
// before publishing leaves a settle-or-TTL reservation for its key. The
// retry's batch admission must exclude that same-key reservation from the
// reserved sum (mirroring ReservePackBytes) — counting it ON TOP of the
// batch's own incoming bytes 507-wedged the retry until the TTL expired.
func TestLFSQuota_RetryAfterFailedPutNotDoubleCounted(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 100, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("r"), 90)
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	dec, err := st.ReservePackBytes(context.Background(), blob.QuotaRepoGroup(lfsTestRepoID),
		[]ref.Pack{{Key: key, Size: 90}}, ref.NewReservationOwner(), 0, 100, time.Hour)
	if err != nil || !dec.OK {
		t.Fatalf("seed failed-PUT reservation: %+v, %v", dec, err)
	}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, 90)))
	if resp.Objects[0].Error != nil {
		t.Fatalf("retry batch denied (its own reservation double-counted?): %+v", resp.Objects[0].Error)
	}
	if resp.Objects[0].Actions == nil {
		t.Fatalf("retry batch issued no upload action: %+v", resp.Objects[0])
	}
	// A DIFFERENT key of the same size must still be denied: the
	// exclusion is per requested key, not a blanket reservation waiver.
	other := bytes.Repeat([]byte("x"), 90)
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(other), 90)))
	if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusInsufficientStorage {
		t.Fatalf("unrelated over-quota batch not denied: %+v", resp.Objects[0])
	}
}

// TestLFSAdoption_VerificationBuffersAreBudgeted (Codex rounds 24–26):
// legacy-blob verification streams ranged reads through the hasher — no
// disk staging, no disk-budget charge at all (round 24's guarantee in
// full: adoption works under ANY LFSStagingBytes) — while its per-read
// HEAP buffer is bounded by a dedicated memory-sized slot pool
// (lfsMaxVerifyBuffers × lfsStagingChunk; round 26: charging the
// disk-sized budget still admitted ~2 GiB of concurrent buffers). With
// every slot taken, adoption denies transiently instead of buffering
// unbounded heap.
func TestLFSAdoption_VerificationBuffersAreBudgeted(t *testing.T) {
	legacy := []byte("a legacy object bigger than any disk staging budget")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)

	t.Run("adopts-under-any-disk-budget", func(t *testing.T) {
		srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
		srv.LFSStagingBytes = 1 // pathological disk budget: verification needs none of it
		tok := mint(auth.ScopeGitWrite)
		if err := srv.Blob.UploadFile(context.Background(), key, writeTempFile(t, legacy)); err != nil {
			t.Fatal(err)
		}
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, int64(len(legacy)))))
		if resp.Objects[0].Error != nil {
			t.Fatalf("adoption failed under a tiny disk budget: %+v", resp.Objects[0].Error)
		}
		if resp.Objects[0].Actions != nil {
			t.Fatalf("adopted object still got an upload action: %+v", resp.Objects[0])
		}
		if has, err := st.HasLFSObject(context.Background(), lfsTestRepoID, key); err != nil || !has {
			t.Fatalf("legacy blob was not adopted: %v, %v", has, err)
		}
	})

	t.Run("slots-exhausted-denies-transiently", func(t *testing.T) {
		srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		if err := srv.Blob.UploadFile(context.Background(), key, writeTempFile(t, legacy)); err != nil {
			t.Fatal(err)
		}
		for range lfsMaxVerifyBuffers { // every buffer slot held elsewhere
			lfsVerifyBuffers <- struct{}{}
		}
		t.Cleanup(func() {
			for range lfsMaxVerifyBuffers {
				<-lfsVerifyBuffers
			}
		})
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, int64(len(legacy)))))
		if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusTooManyRequests {
			t.Fatalf("slot-exhausted verification did not deny transiently: %+v", resp.Objects[0])
		}
	})
}

// TestLFSQuota_AtCapAdoptionDeniesBeforeDownload (Codex security round 4):
// an at-cap repository holding a stored-but-rowless legacy blob must be
// denied BEFORE the verification download. Registration would 507 the
// adoption anyway, but only after a full download+hash of the stored
// object — so every write-scoped touch of that oid becomes a repeatable
// bandwidth/staging drain (a near-budget blob blocks the replica's
// unrelated uploads for the whole hash).
func TestLFSQuota_AtCapAdoptionDeniesBeforeDownload(t *testing.T) {
	old := ref.MaxLFSObjectsPerRepo
	ref.MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)

	// The cap is consumed by a registered object…
	if err := st.RegisterLFSObject(context.Background(), lfsTestRepoID,
		blob.LFSKey(lfsTestRepoID, strings.Repeat("aa", 32)), 10); err != nil {
		t.Fatal(err)
	}
	// …while a rowless pre-registry blob sits stored under another oid.
	legacy := []byte("legacy rowless bytes")
	legacyOID := sha256Hex(legacy)
	if err := srv.Blob.UploadFile(context.Background(),
		blob.LFSKey(lfsTestRepoID, legacyOID), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	counting := &downloadCountingBlob{BlobStore: srv.Blob}
	srv.Blob = counting

	// Direct PUT touch: denied at the cap without downloading.
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+legacyOID,
		"basic:"+tok, string(legacy))
	if w.Code != http.StatusInsufficientStorage {
		t.Fatalf("at-cap legacy PUT status = %d, body %s (want 507)", w.Code, w.Body)
	}
	// Batch upload touch: per-object 507, still no download.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", legacyOID, int64(len(legacy)))))
	if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusInsufficientStorage {
		t.Fatalf("at-cap legacy batch not denied: %+v", resp.Objects[0])
	}
	if counting.downloads != 0 {
		t.Fatalf("at-cap adoption performed %d verification downloads, want 0", counting.downloads)
	}
}

// TestLFSStaging_DripperCannotPinBudget (Codex security round 5): a PUT
// declaring the whole staging budget as Content-Length while dripping
// bytes must NOT reserve the declared size up front — staging grows as
// data actually arrives (one lfsStagingChunk ahead), so an unrelated
// upload still admits while the dripper stalls.
func TestLFSStaging_DripperCannotPinBudget(t *testing.T) {
	oldChunk := lfsStagingChunk
	lfsStagingChunk = 4
	t.Cleanup(func() { lfsStagingChunk = oldChunk })

	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSStagingBytes = 10
	tok := mint(auth.ScopeGitWrite)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	// The dripper: declares the WHOLE budget, sends one byte, stalls.
	dripContent := bytes.Repeat([]byte("d"), 10)
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut,
		ts.URL+"/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(dripContent), pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = 10
	req.SetBasicAuth("t", tok)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	if _, err := pw.Write([]byte("d")); err != nil {
		t.Fatal(err)
	}
	// Wait until the dripper's handler holds its initial chunk (4 of 10).
	deadline := time.Now().Add(10 * time.Second)
	for {
		srv.lfsStagingMu.Lock()
		held := srv.lfsStagingInFlight
		srv.lfsStagingMu.Unlock()
		if held == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dripper staging hold = %d, want 4 (one chunk)", held)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// An unrelated small upload must admit and complete: with the
	// pre-fix upfront reservation the dripper held all 10 budget bytes
	// and this returned 429.
	content := []byte("abcdef")
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content),
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("unrelated upload status = %d, body %s (dripper pinned the staging budget?)", w.Code, w.Body)
	}

	pw.CloseWithError(io.ErrClosedPipe) // abort the dripper
	<-done
}

// twinWinsStore simulates a concurrent identical upload on another replica
// winning DURING this handler's body copy: right after the first
// HasLFSObject read for the target key (the PUT's pre-body check), it
// registers the key in the underlying store.
type twinWinsStore struct {
	ref.Store
	key   string
	size  int64
	calls int
}

func (s *twinWinsStore) HasLFSObject(ctx context.Context, repo, key string) (bool, error) {
	has, err := s.Store.HasLFSObject(ctx, repo, key)
	if err == nil && key == s.key {
		s.calls++
		if s.calls == 1 {
			if rerr := s.Store.RegisterLFSObject(ctx, repo, key, s.size); rerr != nil {
				return false, rerr
			}
		}
	}
	return has, err
}

// TestLFSPut_TwinRegistrationDuringBodyIsZeroGrowth (Codex round 26): the
// pre-body registered flag goes stale when a concurrent identical upload
// registers the OID while this handler receives the body. The post-body
// admission must re-read the registry and skip the reservation — the
// upload is zero-growth now, and charging n as new growth 507s a push
// whose object is already durably stored, purely because the twin won.
func TestLFSPut_TwinRegistrationDuringBodyIsZeroGrowth(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 100, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("t"), 90)
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	srv.Store = &twinWinsStore{Store: st, key: key, size: 90}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("twin-raced PUT status = %d, body %s (stale registered flag double-charged?)", w.Code, w.Body)
	}
	// The twin's 90 bytes are the only charge: no reservation left behind,
	// no double credit.
	if ls := ledgerState(t, st, time.Hour); ls.UsageBytes != 90 || ls.ReservedBytes != 0 {
		t.Fatalf("ledger after twin race = %+v, want usage=90 reserved=0", ls)
	}
}

// statCountingBlob counts Stat calls (the PUT dedup probe).
type statCountingBlob struct {
	blob.BlobStore
	stats int
}

func (b *statCountingBlob) Stat(ctx context.Context, key string) (int64, error) {
	b.stats++
	return b.BlobStore.Stat(ctx, key)
}

// TestLFSPut_OpSlotAcquiredBeforeProbe (Codex round 25): the PUT path must
// take its node-wide operation slot BEFORE any blob-store work — with the
// slots exhausted, the handler must 429 without ever probing the backend,
// or distinct-OID PUTs against a wedged backend would pile up goroutines
// and connections that lfsMaxStagingOps never counts.
func TestLFSPut_OpSlotAcquiredBeforeProbe(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	counting := &statCountingBlob{BlobStore: srv.Blob}
	srv.Blob = counting

	srv.lfsStagingMu.Lock()
	srv.lfsStagingOps = lfsMaxStagingOps // every slot taken
	srv.lfsStagingMu.Unlock()
	t.Cleanup(func() {
		srv.lfsStagingMu.Lock()
		srv.lfsStagingOps = 0
		srv.lfsStagingMu.Unlock()
	})

	content := []byte("slotless put probe")
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content),
		"basic:"+tok, string(content))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("slot-exhausted PUT status = %d, want 429", w.Code)
	}
	if counting.stats != 0 {
		t.Fatalf("slot-exhausted PUT probed the blob store %d times, want 0 (slot must come first)", counting.stats)
	}
}

// wedgedStatBlob blocks Stat until the caller's context expires — the
// wedged-backend scenario.
type wedgedStatBlob struct {
	blob.BlobStore
}

func (b wedgedStatBlob) Stat(ctx context.Context, string2 string) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestLFSPut_ProbeIsDeadlineBound (Codex round 25): the dedup probe holds
// an op slot, so a wedged backend must hand it back within lfsProbeTimeout
// instead of pinning it for as long as the client keeps the connection
// open.
func TestLFSPut_ProbeIsDeadlineBound(t *testing.T) {
	oldTimeout := lfsProbeTimeout
	lfsProbeTimeout = 50 * time.Millisecond
	t.Cleanup(func() { lfsProbeTimeout = oldTimeout })

	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	srv.Blob = wedgedStatBlob{BlobStore: srv.Blob}

	content := []byte("wedged backend probe")
	start := time.Now()
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content),
		"basic:"+tok, string(content))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("wedged-probe PUT status = %d, want 500", w.Code)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("wedged probe took %v — deadline not applied", elapsed)
	}
	// The op slot must be back.
	srv.lfsStagingMu.Lock()
	ops := srv.lfsStagingOps
	srv.lfsStagingMu.Unlock()
	if ops != 0 {
		t.Fatalf("op slots after wedged probe = %d, want 0 (slot leaked)", ops)
	}
}

// TestLFSUpload_AbsoluteDeadlineBoundsDrip (Codex security round 2): a
// client dripping one byte per idle window must still hit the ABSOLUTE
// bound (the reservation TTL) and release its staging capacity.
func TestLFSUpload_AbsoluteDeadlineBoundsDrip(t *testing.T) {
	oldIdle := lfsUploadIdleTimeout
	lfsUploadIdleTimeout = 10 * time.Second // idle alone would never fire
	t.Cleanup(func() { lfsUploadIdleTimeout = oldIdle })

	srv, _, mint := newLFSQuotaServer(t, 0, 400*time.Millisecond) // TTL = absolute bound
	srv.LFSStagingBytes = 100
	tok := mint(auth.ScopeGitWrite)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	content := bytes.Repeat([]byte("d"), 100)
	req, err := http.NewRequest(http.MethodPut,
		ts.URL+"/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content), nil)
	if err != nil {
		t.Fatal(err)
	}
	pr, pw := io.Pipe()
	req.Body = pr
	req.ContentLength = int64(len(content))
	req.SetBasicAuth("t", tok)
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.DefaultClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		_ = err
	}()
	// Drip one byte every 100ms: each write renews the idle window, but
	// the absolute bound must abort the request within ~TTL regardless.
	start := time.Now()
	for i := range content {
		if _, err := pw.Write(content[i : i+1]); err != nil {
			break // server hung up: the absolute deadline fired
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(start) > 5*time.Second {
			t.Fatal("dripping upload was never aborted by the absolute deadline")
		}
	}
	pw.Close()
	<-done
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("drip upload survived %v, want abort within ~the 400ms TTL", elapsed)
	}
}

// TestLFSQuota_ExpiredMidUploadRefreshDenies (Codex round 8): an upload
// whose reservation expires during the body copy must re-admit the
// verified size BEFORE publishing — if another writer consumed the freed
// headroom meanwhile, the upload is denied with nothing durable, instead
// of publishing past the quota.
func TestLFSQuota_ExpiredMidUploadRefreshDenies(t *testing.T) {
	const ttl = 300 * time.Millisecond
	srv, st, mint := newLFSQuotaServer(t, 100, ttl)
	tok := mint(auth.ScopeGitWrite)
	first := bytes.Repeat([]byte("a"), 60)
	second := bytes.Repeat([]byte("b"), 60)
	firstOID := sha256Hex(first)

	// Park the first upload mid-body: its reservation (60) is live now.
	pr, pw := io.Pipe()
	req := httptest.NewRequest(http.MethodPut,
		"/"+lfsTestRepoURL+".git/info/lfs/objects/"+firstOID, pr)
	req.ContentLength = int64(len(first))
	req.SetBasicAuth("t", tok)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); srv.Handler().ServeHTTP(rec, req) }()
	if _, err := pw.Write(first[:4]); err != nil {
		t.Fatal(err)
	}

	// Let the reservation expire, then let a second upload claim the
	// freed headroom and settle it into usage.
	time.Sleep(2 * ttl)
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(second),
		"basic:"+tok, string(second))
	if w.Code != http.StatusOK {
		t.Fatalf("second upload status = %d, body %s", w.Code, w.Body)
	}

	// Resume the first upload: its pre-publish refresh must deny (60
	// settled + 60 incoming > 100) and publish nothing.
	if _, err := pw.Write(first[4:]); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("expired-mid-upload PUT status = %d, body %s (want 507)", rec.Code, rec.Body)
	}
	if _, err := srv.Blob.Stat(context.Background(), blob.LFSKey(lfsTestRepoID, firstOID)); err == nil {
		t.Fatal("denied upload still published the object")
	}
	if ls := ledgerState(t, st, ttl); ls.UsageBytes != int64(len(second)) {
		t.Fatalf("ledger usage = %+v, want only the second object settled", ls)
	}
}

// stallingUploadBlob wedges UploadFile until the context expires,
// simulating a stalled S3 backend (Codex round 11).
type stallingUploadBlob struct {
	blob.BlobStore
}

func (b stallingUploadBlob) UploadFile(ctx context.Context, key, path string) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestLFSQuota_PublicationBoundedByLease (Codex rounds 11+12): publication
// against a stalled backend must abort within the reservation's lease with
// nothing registered — durable usage can never be created by an upload
// whose admission reservation already expired, and the deadline holds even
// with the byte quota DISABLED (a stalled publication pins the staging
// budget and per-key slot regardless of quota configuration).
func TestLFSQuota_PublicationBoundedByLease(t *testing.T) {
	const ttl = 400 * time.Millisecond
	for name, quota := range map[string]int64{"quota-enabled": 1000, "quota-disabled": 0} {
		t.Run(name, func(t *testing.T) {
			srv, st, mint := newLFSQuotaServer(t, quota, ttl)
			srv.Blob = stallingUploadBlob{BlobStore: srv.Blob}
			tok := mint(auth.ScopeGitWrite)
			content := []byte("stalled publication")
			oid := sha256Hex(content)

			start := time.Now()
			w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
				"basic:"+tok, string(content))
			if w.Code != http.StatusInternalServerError {
				t.Fatalf("stalled-publication PUT status = %d, body %s (want 500)", w.Code, w.Body)
			}
			if elapsed := time.Since(start); elapsed > 3*ttl {
				t.Fatalf("stalled publication held the request %v, want ~90%% of the %v TTL", elapsed, ttl)
			}
			if has, err := st.HasLFSObject(context.Background(), lfsTestRepoID, blob.LFSKey(lfsTestRepoID, oid)); err != nil || has {
				t.Fatalf("aborted publication left a registry row: %v, %v", has, err)
			}
			if ls := ledgerState(t, st, time.Hour); ls.UsageBytes != 0 {
				t.Fatalf("aborted publication credited usage: %+v", ls)
			}
		})
	}
}

// stallingDeleteBlob wedges Delete until its context expires (Codex round
// 14: the cap-denial cleanup must be deadline-bounded).
type stallingDeleteBlob struct {
	blob.BlobStore
}

func (b stallingDeleteBlob) Delete(ctx context.Context, key string) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestLFSQuota_CapDenialCleanupBounded (Codex round 14): a stalled backend
// delete during cap-denial cleanup must not pin the handler — the 507
// answer arrives within the cleanup bound and the per-key slot frees.
func TestLFSQuota_CapDenialCleanupBounded(t *testing.T) {
	oldCap, oldTimeout := ref.MaxLFSObjectsPerRepo, lfsCleanupTimeout
	ref.MaxLFSObjectsPerRepo = 1
	lfsCleanupTimeout = 200 * time.Millisecond
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo, lfsCleanupTimeout = oldCap, oldTimeout })

	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	if err := st.RegisterLFSObject(context.Background(), lfsTestRepoID,
		blob.LFSKey(lfsTestRepoID, strings.Repeat("aa", 32)), 10); err != nil {
		t.Fatal(err)
	}
	srv.Store = staleCountStore{Store: st} // force the atomic-loser path
	srv.Blob = stallingDeleteBlob{BlobStore: srv.Blob}

	content := []byte("cap loser with stalled cleanup")
	start := time.Now()
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content),
		"basic:"+tok, string(content))
	if w.Code != http.StatusInsufficientStorage {
		t.Fatalf("cap-loser status = %d, body %s (want 507)", w.Code, w.Body)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("stalled cleanup held the handler %v, want ~the %v bound", elapsed, lfsCleanupTimeout)
	}
}

// TestLFSAdopt_LegacyMismatchReplaced (Codex round 15): a stored blob from
// the presigned-upload era whose bytes do NOT hash to its key must not be
// adopted — existence never proved content back then. The batch treats it
// as missing (upload action, mismatched blob removed) and the verified
// re-upload replaces it.
func TestLFSAdopt_LegacyMismatchReplaced(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("the genuine content")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	// A legacy presigned PUT stored garbage under the oid's key.
	if err := srv.Blob.UploadFile(context.Background(), key,
		writeTempFile(t, []byte("attacker-substituted garbage"))); err != nil {
		t.Fatal(err)
	}

	// Batch upload naming the oid: NOT reported stored — the mismatch is
	// detected, the garbage removed, and an upload action issued.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions["upload"] == nil {
		t.Fatalf("mismatched legacy object not offered an upload action: %+v", resp.Objects[0])
	}
	if _, err := srv.Blob.Stat(context.Background(), key); err == nil {
		t.Fatal("mismatched legacy blob still stored")
	}
	if has, err := st.HasLFSObject(context.Background(), lfsTestRepoID, key); err != nil || has {
		t.Fatalf("mismatched legacy blob was adopted: %v, %v", has, err)
	}

	// The verified re-upload replaces it and registers.
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("verified re-upload status = %d, body %s", w.Code, w.Body)
	}
	if size, err := srv.Blob.Stat(context.Background(), key); err != nil || size != int64(len(content)) {
		t.Fatalf("replacement blob = (%d, %v), want the genuine %d bytes", size, err, len(content))
	}
}

// TestLFSQuota_BatchReservationsNotRenewable (Codex security round 3): a
// batch-only writer must not sustain a quota hold past one TTL by
// re-batching the same OID — batches skip keys with live reservations
// (still issuing actions), so only actual transfers refresh the clock.
func TestLFSQuota_BatchReservationsNotRenewable(t *testing.T) {
	const ttl = 400 * time.Millisecond
	srv, _, mint := newLFSQuotaServer(t, 100, ttl)
	tok := mint(auth.ScopeGitWrite)
	hog := bytes.Repeat([]byte("h"), 90)
	victim := bytes.Repeat([]byte("v"), 90)

	// Batch 1 reserves 90 of 100 and never transfers.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(hog), 90)))
	if resp.Objects[0].Error != nil {
		t.Fatalf("first batch denied: %+v", resp.Objects[0])
	}
	// Halfway through the TTL, re-batching the same OID still yields an
	// action (no wedge) but must NOT refresh the reservation clock.
	time.Sleep(ttl / 2)
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(hog), 90)))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions["upload"] == nil {
		t.Fatalf("re-batch of a held key lost its action: %+v", resp.Objects[0])
	}
	// One full TTL after the ORIGINAL batch, the hold must be gone even
	// though the re-batch happened mid-window: a competing upload admits.
	time.Sleep(ttl/2 + ttl/4)
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(victim), 90)))
	if resp.Objects[0].Error != nil {
		t.Fatalf("batch-only hold survived past its TTL despite re-batching: %+v", resp.Objects[0])
	}
}

// TestLFSUpload_StagingOpCountBounded (Codex round 17): tiny declared
// sizes pass a byte-only budget while each holds a goroutine/fd/temp file —
// the op-count cap bounds concurrent staging operations regardless of
// bytes.
func TestLFSUpload_StagingOpCountBounded(t *testing.T) {
	old := lfsMaxStagingOps
	lfsMaxStagingOps = 1
	t.Cleanup(func() { lfsMaxStagingOps = old })
	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSStagingBytes = 1 << 20
	tok := mint(auth.ScopeGitWrite)
	first := []byte("op slot holder")
	second := []byte("op slot loser!")

	pr, pw := io.Pipe()
	req := httptest.NewRequest(http.MethodPut,
		"/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(first), pr)
	req.ContentLength = int64(len(first))
	req.SetBasicAuth("t", tok)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); srv.Handler().ServeHTTP(rec, req) }()
	if _, err := pw.Write(first[:2]); err != nil { // parked holding the one op slot
		t.Fatal(err)
	}

	// A DISTINCT-key 1-byte-ish PUT passes every byte check but must be
	// turned away by the op-count cap.
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(second),
		"basic:"+tok, string(second))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("op-cap-exceeded PUT status = %d, want 429", w.Code)
	}

	if _, err := pw.Write(first[2:]); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	<-done
	if rec.Code != http.StatusOK {
		t.Fatalf("parked PUT status = %d, body %s", rec.Code, rec.Body)
	}
	// Slot released: the loser converges.
	w = doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(second),
		"basic:"+tok, string(second))
	if w.Code != http.StatusOK {
		t.Fatalf("post-release PUT status = %d, body %s", w.Code, w.Body)
	}
}

// TestLFSQuota_NamespaceUsageRollsUpToBase: pack bytes registered under a
// namespace member of the repo group charge the same quota LFS admission
// uses — one token must not obtain fresh headroom per surface.
func TestLFSQuota_NamespaceUsageRollsUpToBase(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 100, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	// 80 of the 100-byte quota is already consumed by a pack registered
	// under the +ephemeral namespace of the same base repo.
	if err := st.RegisterPacks(context.Background(), lfsTestRepoID+"+ephemeral",
		[]ref.Pack{{ID: "p1", Key: blob.PackKey(lfsTestRepoID+"+ephemeral", "p1"), Size: 80}}); err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("z"), 30) // 80 + 30 > 100
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(content), 30)))
	if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusInsufficientStorage {
		t.Fatalf("namespace pack usage did not gate LFS admission: %+v", resp.Objects[0])
	}
}
