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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/ref"
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
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != int64(len(content)) {
		t.Fatalf("batch did not reserve declared size: %+v", ls)
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
// hash does not match the oid is rejected and publishes nothing. The
// reservation outcome depends on who minted the row (Codex round 5's
// keep-owner semantics): a direct PUT's own row is released immediately,
// while a batch-minted row survives the failed PUT — its upload action is
// still exercisable and a cross-replica release must never free headroom
// under a possibly-still-staging writer — until settle or TTL expiry.
func TestLFSQuota_WrongSHAReservationLifecycle(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 1000, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("the real content")
	oid := sha256Hex(content)
	put := func(body string) *httptest.ResponseRecorder {
		return doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
			"basic:"+tok, body)
	}

	// Direct PUT (no batch): the PUT inserted the row, so failure frees it.
	w := put("not the real content")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("hash-mismatch PUT status = %d, body %s", w.Code, w.Body)
	}
	if _, err := srv.Blob.Stat(context.Background(), blob.LFSKey(lfsTestRepoID, oid)); err == nil {
		t.Fatal("hash-mismatch PUT published the object")
	}
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != 0 || ls.UsageBytes != 0 {
		t.Fatalf("failed direct PUT left ledger state %+v", ls)
	}

	// Batch-then-bad-PUT: the batch's row (its owner) survives the failed
	// PUT — the action is still exercisable — and a correct retry settles
	// it into usage. The wrong body has the same length so the surviving
	// row's refreshed byte value stays comparable.
	decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if w := put("the fake content"); w.Code != http.StatusBadRequest {
		t.Fatalf("hash-mismatch PUT status = %d", w.Code)
	}
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != int64(len(content)) {
		t.Fatalf("batch reservation did not survive the failed PUT: %+v", ls)
	}
	if w := put(string(content)); w.Code != http.StatusOK {
		t.Fatalf("correct retry status = %d, body %s", w.Code, w.Body)
	}
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != 0 || ls.UsageBytes != int64(len(content)) {
		t.Fatalf("retry did not settle the batch reservation: %+v", ls)
	}
}

// TestLFSQuota_ReservationTTLExpiryFreesAdmission: an abandoned batch
// reservation stops gating admission once it ages past the TTL.
func TestLFSQuota_ReservationTTLExpiryFreesAdmission(t *testing.T) {
	const ttl = 50 * time.Millisecond
	srv, _, mint := newLFSQuotaServer(t, 100, ttl)
	tok := mint(auth.ScopeGitWrite)
	first := bytes.Repeat([]byte("a"), 90)
	second := bytes.Repeat([]byte("b"), 90)

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(first), 90)))
	if resp.Objects[0].Error != nil {
		t.Fatalf("first batch denied: %+v", resp.Objects[0])
	}
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(second), 90)))
	if resp.Objects[0].Error == nil {
		t.Fatalf("second batch admitted against a live reservation: %+v", resp.Objects[0])
	}

	time.Sleep(2 * ttl) // the abandoned reservation expires
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", sha256Hex(second), 90)))
	if resp.Objects[0].Error != nil {
		t.Fatalf("expired reservation still gates admission: %+v", resp.Objects[0])
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
	old := maxLFSObjectsPerRepo
	maxLFSObjectsPerRepo = 1
	t.Cleanup(func() { maxLFSObjectsPerRepo = old })
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
