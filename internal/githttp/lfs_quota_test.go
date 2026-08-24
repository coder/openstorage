package githttp

// LFS storage-quota perimeter tests (issue #51): batch-time admission
// through the durable reservation ledger, PUT-time self-admission against
// declared-size lies, settle-on-completion accounting, reservation release
// on failed uploads, TTL expiry, and namespace rollup. Written red-first:
// before the fix, every one of these demonstrated unmetered LFS writes.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
		ref.NewReservationOwner(), 0, ref.ReserveNoFloorGen, 100, ttl); err != nil || !dec.OK {
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

func (r reapOnRegister) RegisterAdoptedLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	if key == r.key {
		if err := r.bl.Delete(ctx, key); err != nil {
			return fmt.Errorf("test reap: %v", err)
		}
	}
	return r.Store.RegisterAdoptedLFSObject(ctx, repo, key, bytes)
}

// TestLFSStaging_SubChunkBudgetCannotBeMonopolized (Codex round 55,
// security): with a configured budget at or below one default chunk, the
// chunk-floored share allowance covered the WHOLE pool, so a header-only
// PUT declaring the budget reserved every byte before reading any body
// and starved all repositories for the idle minute. The staging quantum
// must scale down with the budget: a whole-budget declaration is 413'd
// outright (no single repository's share can ever hold it), and a parked
// in-share upload leaves unrelated repositories admissible.
func TestLFSStaging_SubChunkBudgetCannotBeMonopolized(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSStagingBytes = 3 << 20 // below one default 8 MiB chunk
	tok := mint(auth.ScopeGitWrite)

	// Declaring the whole budget must be permanently rejected — the
	// per-repository capability is a third of the budget.
	huge := bytes.Repeat([]byte("h"), 3<<20)
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(huge),
		"basic:"+tok, string(huge))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("whole-budget PUT status = %d, want 413", w.Code)
	}

	// An in-share upload (1 MiB) holds at most its share: an unrelated
	// repository must still reserve and grow.
	content := bytes.Repeat([]byte("s"), 1<<20)
	w = doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content),
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("in-share PUT status = %d, body %s", w.Code, w.Body)
	}
	if !srv.reserveLFSStaging("repo_other", 0) {
		t.Fatal("victim op slot starved")
	}
	if !srv.growLFSStaging("repo_other", 256<<10) {
		t.Fatal("sub-chunk budget: unrelated repository's byte growth starved")
	}
	srv.releaseLFSStaging("repo_other", 256<<10)
}

// TestLFSPublicationLeaseLeavesFloorCoverage (Codex round 51): the
// publication lease must end with a FULL floor-freshness-plus-handoff
// window of reservation cover remaining — a registration failing at the
// lease's very end otherwise leaves the durable rowless orphan with less
// cover than a just-scanned floor's usable life, letting a second writer
// consume the same headroom. 90% binds only when the residual 10% still
// clears MinPackReservationTTL.
func TestLFSPublicationLeaseLeavesFloorCoverage(t *testing.T) {
	for _, tc := range []struct {
		ttl  time.Duration
		want time.Duration
	}{
		{90 * time.Second, 30 * time.Second},               // absolute reserve binds
		{10 * time.Minute, 9 * time.Minute},                // boundary: both agree
		{time.Hour, 54 * time.Minute},                      // 90% binds (6m residue ≥ 60s)
		{61 * time.Second, time.Second},                    // pathological: clamped nominal
		{400 * time.Millisecond, time.Second},              // sub-minimum test TTLs clamp
		{20 * time.Minute, 18 * time.Minute},               // 90% binds
		{2 * time.Minute, time.Minute},                     // absolute reserve binds
		{MinPackReservationTTL + time.Second, time.Second}, // just above the config floor
	} {
		srv := &Server{PackReservationTTL: tc.ttl}
		if got := srv.lfsPublicationLease(); got != tc.want {
			t.Fatalf("lease(ttl=%v) = %v, want %v", tc.ttl, got, tc.want)
		}
	}
}

// TestLFSAdoption_UsableLeaseAtMinimumTTL (Codex round 52): adoption
// verification must NOT run under the publication lease's residual-cover
// cap — its bytes are already durable and floor-covered, no reservation
// protects them — or near-minimum TTLs (61s → a one-second lease) would
// make any legacy object slower than that to read and hash permanently
// unadoptable, wedging the write-touch migration. At a 61-second TTL the
// adoption of a (deliberately slow-to-read) legacy object must succeed.
func TestLFSAdoption_UsableLeaseAtMinimumTTL(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, MinPackReservationTTL+time.Second)
	tok := mint(auth.ScopeGitWrite)

	legacy := []byte("legacy object slower than the capped lease")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// Reads take ~1.2s: longer than the 1s capped publication lease, far
	// under the 90%-of-TTL adoption lease.
	srv.Blob = slowReadBlob{BlobStore: srv.Blob, delay: 1200 * time.Millisecond}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("near-minimum-TTL adoption failed: %+v", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
}

// slowReadBlob delays ranged reads, simulating a slow-to-hash legacy blob.
type slowReadBlob struct {
	blob.BlobStore
	delay time.Duration
}

func (b slowReadBlob) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	select {
	case <-time.After(b.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.BlobStore.ReadRange(ctx, key, offset, length)
}

// TestLFSAdoption_OutlivesReservationTTL (Codex round 56, security): the
// adoption hash was bounded by 90% of the reservation TTL, so an object
// slower to read than that lease could NEVER adopt — every write touch
// replayed the full ranged read up to the lease, released the per-key
// slot, and the next tiny disconnecting batch restarted it from offset
// zero: a repeatable, client-cost-free drain of the repo's verification
// share and shared backend bandwidth. The verification bound must scale
// with the object (per-chunk stall guard + size-scaled allowance), never
// the TTL: an adoption slower than the WHOLE TTL must complete once and
// register, making every later touch free.
func TestLFSAdoption_OutlivesReservationTTL(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, 400*time.Millisecond)
	tok := mint(auth.ScopeGitWrite)

	legacy := []byte("legacy object slower to hash than the whole reservation TTL")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// The single ranged read takes 2.5× the TTL (~2.8× the old lease).
	srv.Blob = slowReadBlob{BlobStore: srv.Blob, delay: time.Second}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("TTL-outliving adoption failed: %+v", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
}

// stallOnceBlob stalls the first ranged read at stallOff past its context
// deadline and records every read offset, simulating one slow range on a
// congested backend.
type stallOnceBlob struct {
	blob.BlobStore
	stallOff int64
	stalled  atomic.Bool
	mu       sync.Mutex
	reads    []int64
	readKeys []string
}

func (b *stallOnceBlob) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	b.mu.Lock()
	b.reads = append(b.reads, offset)
	b.readKeys = append(b.readKeys, key)
	b.mu.Unlock()
	if offset == b.stallOff && !b.stalled.Swap(true) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return b.BlobStore.ReadRange(ctx, key, offset, length)
}

// keyReads returns the recorded offsets read from key. Callers hold b.mu.
func (b *stallOnceBlob) keyReads(key string) []int64 {
	var offs []int64
	for i, k := range b.readKeys {
		if k == key {
			offs = append(offs, b.reads[i])
		}
	}
	return offs
}

// TestLFSAdoption_ResumesAfterSlowRange (Codex round 57): a ranged read
// slower than the per-chunk stall guard aborts the verification and
// releases the per-key slot — if the next attempt restarted from offset
// zero, chronic per-range slowness would replay completed ranges forever,
// never adopting and amplifying the congestion that aborted it. Aborts
// must save their progress: the retry continues after the completed
// ranges (offset zero is read exactly once across both attempts) and the
// adoption converges.
func TestLFSAdoption_ResumesAfterSlowRange(t *testing.T) {
	ctx := context.Background()
	oldChunk, oldTimeout := lfsStagingChunk, lfsVerifyChunkTimeout
	lfsStagingChunk, lfsVerifyChunkTimeout = 4, 100*time.Millisecond
	t.Cleanup(func() { lfsStagingChunk, lfsVerifyChunkTimeout = oldChunk, oldTimeout })

	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)

	legacy := []byte("legacy 12b!!") // three 4-byte chunks
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// The second chunk stalls once: attempt 1 hashes [0,4), aborts on
	// [4,8), and must save its progress.
	bl := &stallOnceBlob{BlobStore: srv.Blob, stallOff: 4}
	srv.Blob = bl

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error == nil {
		t.Fatalf("stalled adoption reported stored: %+v", resp.Objects[0])
	}
	// The abort PARKED the fleet lease with the saved progress (Codex
	// round 75): the sha256 midstate lives only in this process, so a
	// foreign replica claiming the key would restart the hash from
	// offset zero — retries rotating across replicas could replay
	// completed ranges forever. The held claim must keep denying them.
	if again, _, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, key); err != nil || again {
		t.Fatalf("foreign claim while parked progress exists = (%v, %v), want denied", again, err)
	}
	// Attempt 2 converges — and never re-reads the completed range. It
	// can only proceed by RENEWING the parked lease: the claim row is
	// live, so a fresh TryAdoptionClaim would be denied.
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("resumed adoption failed: %+v", resp.Objects[0])
	}
	// Terminal success releases the parked lease (registration makes the
	// row dead weight); the key is free for whoever wants it.
	if again, _, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, key); err != nil || !again {
		t.Fatalf("claim after terminal adoption = (%v, %v), want free", again, err)
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()
	// The SOURCE must never replay a completed range; the canonical
	// DESTINATION's single full read is the round-81 re-verification that
	// binds registration to the bytes legacy URLs cannot touch — a
	// deliberate, once-per-object cost, not a resume failure.
	srcZero := 0
	for _, off := range bl.keyReads(blob.LFSLegacyKey(lfsTestRepoID, oid)) {
		if off == 0 {
			srcZero++
		}
	}
	if srcZero != 1 {
		t.Fatalf("source offset 0 read %d times across attempts (reads: %v / %v), want exactly 1 — completed ranges must not replay", srcZero, bl.readKeys, bl.reads)
	}
	if destReads := bl.keyReads(key); len(destReads) != (len(legacy)+3)/4 {
		t.Fatalf("destination reads = %v, want exactly one full re-verification pass", destReads)
	}
}

// blockedRenewStore wedges every adoption-claim renewal until its context
// expires, simulating a stalled ref store mid-verification.
type blockedRenewStore struct {
	ref.RefStore
}

func (b *blockedRenewStore) RenewAdoptionClaim(ctx context.Context, key, owner string) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

// TestLFSAdoption_RenewalIsBounded (Codex round 75): claim renewals run
// inside the size-scaled verification window, whose deadline can be hours
// away for a large object. A wedged ref-store call must abort the attempt
// within the dedicated renewal timeout — freeing the op slot and verify
// buffer — never pin them for the whole allowance while the 30-minute
// claim expires underneath.
func TestLFSAdoption_RenewalIsBounded(t *testing.T) {
	ctx := context.Background()
	oldInterval, oldTimeout := lfsAdoptClaimRenewInterval, lfsClaimRenewTimeout
	lfsAdoptClaimRenewInterval, lfsClaimRenewTimeout = 0, 50*time.Millisecond
	t.Cleanup(func() { lfsAdoptClaimRenewInterval, lfsClaimRenewTimeout = oldInterval, oldTimeout })

	srv, _, _ := newLFSQuotaServer(t, 0, time.Hour)
	legacy := []byte("legacy object whose claim renewal wedges")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	srv.Store = &blockedRenewStore{RefStore: srv.Store}

	errc := make(chan error, 1)
	go func() { errc <- srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid) }()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("adoption succeeded under a wedged renewal")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("adoption not bounded by the renewal timeout — a wedged ref store pinned the verification slots for the size-scaled allowance")
	}
}

// TestLFSAdoption_EvictedParkedLeaseIsNotReleased (Codex round 76):
// evicting a parked resume entry under cache pressure discards the sha256
// midstate — nothing can save the completed prefix then — but RELEASING
// the victim's lease on top of that let the key's next retry win a fresh
// claim on any replica and restart from offset zero immediately: cycling
// more keys than the cache bound turned every eviction into a full
// re-read. Eviction must have crash semantics instead: the orphaned lease
// keeps denying every replica read-free until ref.AdoptionClaimTTL, so a
// key restarts from zero at most once per TTL window regardless of cache
// pressure.
func TestLFSAdoption_EvictedParkedLeaseIsNotReleased(t *testing.T) {
	ctx := context.Background()
	oldMax := lfsMaxVerifyResumes
	lfsMaxVerifyResumes = 1
	t.Cleanup(func() { lfsMaxVerifyResumes = oldMax })

	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	keyA := blob.LFSKey(lfsTestRepoID, sha256Hex([]byte("evicted object")))
	keyB := blob.LFSKey(lfsTestRepoID, sha256Hex([]byte("evictor object")))
	claimedA, ownerA, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, keyA)
	if err != nil || !claimedA {
		t.Fatalf("seed claim A = (%v, %v), want held", claimedA, err)
	}
	claimedB, ownerB, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, keyB)
	if err != nil || !claimedB {
		t.Fatalf("seed claim B = (%v, %v), want held", claimedB, err)
	}
	h := sha256.New()
	h.Write([]byte("prefix"))
	srv.saveLFSVerifyResume(keyA, 10, 6, "fp", h, ownerA)
	srv.saveLFSVerifyResume(keyB, 10, 6, "fp", h, ownerB) // cap 1: evicts A's entry
	// Give any (buggy) async release a chance to land before asserting.
	time.Sleep(200 * time.Millisecond)
	if again, _, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, keyA); err != nil || again {
		t.Fatalf("claim on evicted key = (%v, %v), want denied — an evicted parked lease must be orphaned to its TTL, not released", again, err)
	}
	// The survivor's parked lease is untouched.
	if again, _, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, keyB); err != nil || again {
		t.Fatalf("claim on surviving key = (%v, %v), want denied", again, err)
	}
}

// failOnceRegisterStore fails the first adopted-object registration,
// simulating a transient/contended ref store at commit time.
type failOnceRegisterStore struct {
	ref.RefStore
	failed atomic.Bool
}

func (f *failOnceRegisterStore) RegisterAdoptedLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	if !f.failed.Swap(true) {
		return fmt.Errorf("simulated registration failure")
	}
	return f.RefStore.RegisterAdoptedLFSObject(ctx, repo, key, bytes)
}

// TestLFSAdoption_CompletedHashSurvivesRegistrationFailure (Codex round
// 76): a fully verified hash whose registration fails transiently was
// discarded with the released claim — the next write-scoped touch
// downloaded and hashed the ENTIRE object again just to retry the same
// idempotent transaction, so a contended ref store repeatedly amplified
// reads of large objects. The completed verification must be parked with
// the lease: the retry skips straight to registration, re-reading nothing.
func TestLFSAdoption_CompletedHashSurvivesRegistrationFailure(t *testing.T) {
	ctx := context.Background()
	oldChunk := lfsStagingChunk
	lfsStagingChunk = 4
	t.Cleanup(func() { lfsStagingChunk = oldChunk })

	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	legacy := []byte("legacy 12b!!") // three 4-byte chunks
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	srv.Store = &failOnceRegisterStore{RefStore: srv.Store}
	bl := &stallOnceBlob{BlobStore: srv.Blob, stallOff: -1} // never stalls: pure read recorder
	srv.Blob = bl

	// Attempt 1: the hash completes, the registration fails.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error == nil {
		t.Fatalf("adoption with failed registration reported stored: %+v", resp.Objects[0])
	}
	// Attempt 2: converges by retrying the registration alone.
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("registration retry failed: %+v", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()
	// Attempt 1 read the source once and the canonical destination once
	// (the round-81 re-verification that binds registration to the
	// destination bytes); attempt 2's retry re-read ONLY the destination's
	// final chunk from the parked midstate (the digest over exactly size
	// bytes is always re-checked before the registration — Codex round 77
	// — but the completed prefix is not).
	chunks := (len(legacy) + 3) / 4
	if srcReads := bl.keyReads(blob.LFSLegacyKey(lfsTestRepoID, oid)); len(srcReads) != chunks {
		t.Fatalf("source reads = %v, want exactly one verification pass", srcReads)
	}
	if destReads := bl.keyReads(key); len(destReads) != chunks+1 {
		t.Fatalf("destination reads = %v, want one full pass plus the retry's final chunk", destReads)
	}
	zeroReads := 0
	for _, off := range bl.keyReads(key) {
		if off == 0 {
			zeroReads++
		}
	}
	if zeroReads != 1 {
		t.Fatalf("destination offset 0 read %d times (reads: %v), want exactly 1 — the completed prefix must not replay on the retry", zeroReads, bl.reads)
	}
}

// TestLFSAdoption_ImmuneToLegacyPresignReplay (Codex security round 78):
// v0.1.x issued presigned PUT URLs for the legacy key spelling with a
// 15-minute expiry, and a presigned URL cannot be revoked — after an
// adoption REGISTERED the object, a still-valid URL could overwrite the
// single shared key with arbitrary bytes that every later read served as
// trusted content. Registered objects now live at the canonical `o2/`
// key, which no legacy URL can address: replaying the legacy PUT after
// registration must leave every served byte untouched.
func TestLFSAdoption_ImmuneToLegacyPresignReplay(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite, auth.ScopeGitRead)
	legacy := []byte("true legacy content, adopted")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, legacyKey, writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// Write-scoped touch adopts (copy → verify → register canonically).
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("adoption failed: %+v", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
	// The legacy source is RETAINED after adoption (Codex round 80): a
	// presigned GET issued moments before the registration must survive
	// its 15-minute lifetime, and mid-roll v0.2.0 replicas can only read
	// this spelling. The age-thresholded sweep reclaims it later.
	if _, err := srv.Blob.Stat(ctx, legacyKey); err != nil {
		t.Fatalf("adopted legacy source missing: %v — must be retained for live URL lifetimes", err)
	}
	// The still-valid legacy presigned PUT replays with forged bytes (any
	// size — the attacker controls the body entirely).
	forged := []byte("forged replacement written through the legacy presigned URL")
	if err := srv.Blob.UploadFile(ctx, legacyKey, writeTempFile(t, forged)); err != nil {
		t.Fatal(err)
	}
	// Every read of the registered object must serve the TRUE bytes.
	w := doLFS(t, srv, http.MethodGet, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid, "basic:"+tok, "")
	if w.Code != http.StatusOK {
		t.Fatalf("download status = %d, body %s", w.Code, w.Body)
	}
	if got := sha256Hex(w.Body.Bytes()); got != oid {
		t.Fatalf("download served %q (digest %s) — the legacy presign replay reached the registered object", w.Body.Bytes(), got)
	}
	// And the batch download must size/serve the canonical object too.
	dresp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		`{"operation":"download","transfers":["basic"],"objects":[{"oid":"`+oid+`","size":1}]}`))
	if dresp.Objects[0].Error != nil {
		t.Fatalf("batch download failed: %+v", dresp.Objects[0])
	}
	if dresp.Objects[0].Size != int64(len(legacy)) {
		t.Fatalf("batch download size = %d, want %d (the true object)", dresp.Objects[0].Size, len(legacy))
	}
}

// staleRegistryOnce returns "not registered" for the FIRST canonical-key
// registry read, simulating a read that raced the object's adoption
// commit; every later read delegates.
type staleRegistryOnce struct {
	ref.RefStore
	missed atomic.Bool
}

func (s *staleRegistryOnce) HasLFSObject(ctx context.Context, repo, key string) (bool, error) {
	if strings.Contains(key, "/o2/") && !s.missed.Swap(true) {
		return false, nil
	}
	return s.RefStore.HasLFSObject(ctx, repo, key)
}

// TestLFSDownload_RetriesCanonicalAfterAdoptionRace (Codex round 79): a
// download's registry read can race the object's first post-upgrade
// adoption — the read sees no row, the legacy source is deleted by the
// adoption commit an instant later, and the legacy stat 404s even though
// the verified canonical object is healthy. Both download surfaces must
// re-check the registry and retry canonically instead of returning a
// false 404.
func TestLFSDownload_RetriesCanonicalAfterAdoptionRace(t *testing.T) {
	ctx := context.Background()
	for name, download := range map[string]func(t *testing.T, srv *Server, tok, oid string, size int64){
		"batch": func(t *testing.T, srv *Server, tok, oid string, size int64) {
			resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
				`{"operation":"download","transfers":["basic"],"objects":[{"oid":"`+oid+`","size":1}]}`))
			if resp.Objects[0].Error != nil || resp.Objects[0].Actions["download"] == nil {
				t.Fatalf("raced download batch = %+v, want a download action — the false 404", resp.Objects[0])
			}
			if resp.Objects[0].Size != size {
				t.Fatalf("raced download size = %d, want %d", resp.Objects[0].Size, size)
			}
		},
		"object-get": func(t *testing.T, srv *Server, tok, oid string, size int64) {
			w := doLFS(t, srv, http.MethodGet, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid, "basic:"+tok, "")
			if w.Code != http.StatusOK {
				t.Fatalf("raced GET status = %d, body %s — the false 404", w.Code, w.Body)
			}
			if int64(w.Body.Len()) != size {
				t.Fatalf("raced GET body = %d bytes, want %d", w.Body.Len(), size)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
			tok := mint(auth.ScopeGitWrite, auth.ScopeGitRead)
			content := []byte("adopted an instant before the read")
			oid := sha256Hex(content)
			key := blob.LFSKey(lfsTestRepoID, oid)
			// The post-adoption world: canonical bytes + row, NO legacy.
			if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, content)); err != nil {
				t.Fatal(err)
			}
			if err := st.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
				t.Fatal(err)
			}
			// The download's first registry read predates the commit.
			srv.Store = &staleRegistryOnce{RefStore: srv.Store}
			download(t, srv, tok, oid, int64(len(content)))
		})
	}
}

// TestLFSAdopt_CorruptCanonicalUnderRegisteredRowIsRemoved (Codex round
// 80): an adoption's canonical copy can land AFTER a verified publisher's
// upload+registration (the fleet claim fences only adopters), overwriting
// verified bytes with unverified legacy content. The old mismatch path
// then declined to delete because the registry row existed — leaving
// forged bytes served under a trusted OID forever. Bytes that provably
// fail their key's digest must be removed even under a registered row
// (fingerprint-conditionally, so a concurrent true-bytes republish
// stands); the registered-but-blobless remainder self-heals.
func TestLFSAdopt_CorruptCanonicalUnderRegisteredRowIsRemoved(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	content := []byte("the true registered content")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	// The verified publisher registered the true bytes…
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	// …but the adoption's late copy overwrote them with the legacy
	// content (the fabricated end state of the race), sourced from a
	// forged legacy blob.
	forged := []byte("forged legacy bytes copied late")
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, forged)); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, forged)); err != nil {
		t.Fatal(err)
	}
	// This adoption's registered early-out predated the publisher's
	// commit: it believes the object is rowless.
	srv.Store = &staleRegistryOnce{RefStore: srv.Store}

	err := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid)
	if !errors.Is(err, errLFSContentMismatch) {
		t.Fatalf("adopt over corrupt registered canonical = %v, want errLFSContentMismatch", err)
	}
	if _, serr := srv.Blob.Stat(ctx, key); serr == nil {
		t.Fatal("forged canonical bytes still stored under the registered row")
	}
	// The row survives: registered-but-blobless heals via the publisher's
	// writer verify or the client's zero-growth re-upload.
	if has, herr := st.HasLFSObject(ctx, lfsTestRepoID, key); herr != nil || !has {
		t.Fatalf("row after corrupt-bytes cleanup = (%v, %v), want intact", has, herr)
	}
}

// swapOnPublishBlob replaces the SOURCE with forged bytes at the moment
// the adoption's canonical publish copy runs — the copy then carries
// content the verification never hashed.
type swapOnPublishBlob struct {
	blob.BlobStore
	forged []byte
	tmp    string
}

func (b *swapOnPublishBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	// Outlast the coarse mtime tick so the file backend's mtime-derived
	// fingerprint provably moves (see bucketStatFingerprint's caveat).
	time.Sleep(25 * time.Millisecond)
	if err := b.BlobStore.UploadFile(ctx, srcKey, b.tmp); err != nil {
		return err
	}
	return b.BlobStore.Copy(ctx, dstKey, srcKey)
}

// readBlobBytes downloads key into memory through the store's own
// transfer path.
func readBlobBytes(t *testing.T, bl blob.BlobStore, key string) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "obj")
	if err := bl.DownloadFile(context.Background(), key, path); err != nil {
		t.Fatalf("download %s: %v", key, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// abaSwapBlob models S3's content-derived ETag semantics for the ABA
// attack (Codex round 81): the LEGACY key's fingerprint is a function of
// its CONTENT (identical bytes ⇒ identical ETag), and the wrapper swaps
// the source to forged bytes exactly while the adoption's canonical
// publish copy reads it, restoring the true bytes afterwards. Every
// source fingerprint read therefore sees the TRUE content's value —
// before/after equality proves nothing about which version the copy
// carried. The canonical destination's fingerprints pass through
// unmodified.
type abaSwapBlob struct {
	blob.BlobStore
	legacyKey string
	trueTmp   string
	forgedTmp string
}

func (b *abaSwapBlob) StatFingerprint(ctx context.Context, key string) (int64, string, error) {
	size, _, err := b.BlobStore.StatFingerprint(ctx, key)
	if key == b.legacyKey {
		// Content-derived: both the pre-copy and post-copy reads observe
		// the true content, so they see one stable value.
		return size, "content-etag-of-true-bytes", err
	}
	return b.BlobStore.StatFingerprint(ctx, key)
}

func (b *abaSwapBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	if srcKey != b.legacyKey {
		return b.BlobStore.Copy(ctx, dstKey, srcKey)
	}
	// A → B: the still-valid legacy PUT lands forged bytes…
	if err := b.BlobStore.UploadFile(ctx, srcKey, b.forgedTmp); err != nil {
		return err
	}
	// …the copy reads B…
	if err := b.BlobStore.Copy(ctx, dstKey, srcKey); err != nil {
		return err
	}
	// …B → A: and the true bytes return before any fingerprint check.
	return b.BlobStore.UploadFile(ctx, srcKey, b.trueTmp)
}

// TestLFSAdopt_SourceABACannotRegisterForgedBytes (Codex round 81, normal
// P1 + security P1): source fingerprinting cannot close ABA — with S3's
// content-derived ETags, swapping true→forged→true around the publish
// copy restores the original fingerprint, so a before/after sandwich
// accepts a copy that carried the forged version. Registration must be
// bound to a digest proof of the bytes actually at the CANONICAL key
// (which no legacy URL can touch); forged bytes must never end up
// registered, whatever the source does.
func TestLFSAdopt_SourceABACannotRegisterForgedBytes(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	trueBytes := []byte("the true content of this object!")
	oid := sha256Hex(trueBytes)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	forged := []byte("forged bytes of identical length")
	if len(forged) != len(trueBytes) {
		t.Fatal("fixture: ABA needs same-size forged bytes")
	}
	if err := srv.Blob.UploadFile(ctx, legacyKey, writeTempFile(t, trueBytes)); err != nil {
		t.Fatal(err)
	}
	srv.Blob = &abaSwapBlob{BlobStore: srv.Blob, legacyKey: legacyKey,
		trueTmp: writeTempFile(t, trueBytes), forgedTmp: writeTempFile(t, forged)}

	err := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid)
	registered, herr := st.HasLFSObject(ctx, lfsTestRepoID, key)
	if herr != nil {
		t.Fatal(herr)
	}
	if registered {
		if err != nil {
			t.Fatalf("registered but errored: %v", err)
		}
		// Registration is only acceptable if the canonical bytes are the
		// TRUE content (e.g. a re-verified copy of the restored source).
		w := readBlobBytes(t, srv.Blob, key)
		if got := sha256Hex(w); got != oid {
			t.Fatalf("ABA registered forged canonical bytes (digest %s, want %s)", got, oid)
		}
		return
	}
	// Not registered: fine — but no forged canonical bytes may linger
	// served... rowless canonical bytes are never served, so absence OR
	// any content is acceptable here. The essential property held.
	t.Logf("adoption returned %v without registering (acceptable fail-closed outcome)", err)
}

// TestLFSAdopt_PublishSandwichCatchesSourceSwap (Codex round 80): the
// canonical publish copies the source AFTER verification — if a
// still-valid legacy presigned PUT swaps the source in that window, the
// copy carries bytes the digest never proved. The source-fingerprint
// sandwich must catch the swap, delete the suspect canonical copy, and
// deny transiently; nothing may register.
func TestLFSAdopt_PublishSandwichCatchesSourceSwap(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	legacy := []byte("true bytes verified pre-publish")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, legacyKey, writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	forged := []byte("forged bytes swapped mid-publish!")
	srv.Blob = &swapOnPublishBlob{BlobStore: srv.Blob, forged: forged, tmp: writeTempFile(t, forged)}

	err := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid)
	if err == nil {
		t.Fatal("adoption succeeded across a mid-publish source swap")
	}
	if has, herr := st.HasLFSObject(ctx, lfsTestRepoID, key); herr != nil || has {
		t.Fatalf("registered after source swap = (%v, %v), want unregistered", has, herr)
	}
	if _, serr := srv.Blob.Stat(ctx, key); serr == nil {
		t.Fatal("suspect canonical copy retained after the sandwich caught the swap")
	}
}

// deadlineCapturingBlob records the context deadline its Copy runs under.
type deadlineCapturingBlob struct {
	blob.BlobStore
	budget atomic.Int64 // nanoseconds until the deadline, 0 = none
}

func (b *deadlineCapturingBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	if dl, ok := ctx.Deadline(); ok {
		b.budget.Store(int64(time.Until(dl)))
	}
	return b.BlobStore.Copy(ctx, dstKey, srcKey)
}

// TestLFSCompatCopy_BudgetScalesWithSize (Codex round 81): the v0.2.0
// compatibility copy was bounded by the one-minute cleanup timeout, but
// accepted LFS objects reach many GiB and S3's multipart copy is
// sequential — the copy was guaranteed to be cancelled for large objects,
// silently 404ing them on old replicas for the whole roll. The bridge must
// get the publication-style size-scaled allowance.
func TestLFSCompatCopy_BudgetScalesWithSize(t *testing.T) {
	srv, _, _ := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSV020WriteCompat = true
	cb := &deadlineCapturingBlob{BlobStore: srv.Blob}
	srv.Blob = cb
	// Seed a canonical object so the copy has a source; the declared size
	// models a 10 GiB object without staging one.
	content := []byte("stand-in for a very large object")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(context.Background(), key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	if err := srv.maybeLegacyCompatCopy(context.Background(), lfsTestRepoID, oid, key, 10<<30); err != nil {
		t.Fatalf("compat copy: %v", err)
	}
	if got := time.Duration(cb.budget.Load()); got < 2*time.Hour {
		t.Fatalf("compat copy budget = %v for a 10 GiB object, want a size-scaled allowance (>= ~2h51m) — a fixed 1m cap cancels every large copy", got)
	}
}

// TestLFSPut_V020WriteCompatDualPublishes (Codex round 80, ADR-41):
// during a rolling upgrade, old v0.2.0 replicas derive only the legacy
// spelling — with the compat flag set, a verified publication must also
// land there so mid-roll reads keep working; without it, publication is
// canonical-only.
func TestLFSPut_V020WriteCompatDualPublishes(t *testing.T) {
	for name, compat := range map[string]bool{"compat-on": true, "compat-off": false} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
			srv.LFSV020WriteCompat = compat
			tok := mint(auth.ScopeGitWrite)
			content := []byte("dual-published during the roll")
			oid := sha256Hex(content)
			w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
				"basic:"+tok, string(content))
			if w.Code != http.StatusOK {
				t.Fatalf("PUT status = %d, body %s", w.Code, w.Body)
			}
			size, err := srv.Blob.Stat(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid))
			if compat {
				if err != nil || size != int64(len(content)) {
					t.Fatalf("legacy compat copy = (%d, %v), want the published bytes", size, err)
				}
			} else if err == nil {
				t.Fatal("compat-off publication wrote the legacy spelling")
			}
		})
	}
}

// TestLFSCompatCopy_CoversQuotaBytes (Codex round 82): with quota
// enforcement on, the compat copy materializes ANOTHER size bytes after
// registration consumed the upload's reservation — a floor scanned before
// the copy sees neither the legacy bytes nor a reservation, so admissions
// against it could durably exceed the quota. The copy must run under the
// renewed-cover machinery (initial cover + mandatory stale-floor tail =
// >= 2 reservations on the LEGACY key) and hand off a floor-generation
// bump.
func TestLFSCompatCopy_CoversQuotaBytes(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour) // quota ENABLED
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	cs := &coverCountingStore{RefStore: srv.Store}
	srv.Store = cs
	content := []byte("compat copy under quota")
	oid := sha256Hex(content)
	preGen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID)
	if err != nil {
		t.Fatal(err)
	}
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body %s", w.Code, w.Body)
	}
	legacyCover := blob.LFSLegacyKey(lfsTestRepoID, oid) + ref.RepairCoverSuffix
	if got := cs.coversFor(legacyCover); got < 2 {
		t.Fatalf("compat-copy covers = %d, want >= 2 (initial + mandatory tail) — uncovered legacy bytes let stale floors over-admit", got)
	}
	if postGen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID); err != nil || postGen == preGen {
		t.Fatalf("floor generation after compat copy = (%d -> %d, %v), want bumped (the invalidation handoff)", preGen, postGen, err)
	}
}

// TestLFSAdopt_CompatCopyAfterCanonicalAdoption (Codex round 82): an
// object adopted from ROWLESS CANONICAL bytes (a verified publication
// whose registration failed, registered by the retry) has no legacy
// spelling — with the compat bridge active, old replicas would 404 it
// despite the flag. Adoption must publish the compat copy too.
func TestLFSAdopt_CompatCopyAfterCanonicalAdoption(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSV020WriteCompat = true
	content := []byte("canonical-only survivor")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	// The crash residue: canonical bytes, no row, no legacy spelling.
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	if err := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
	size, err := srv.Blob.Stat(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid))
	if err != nil || size != int64(len(content)) {
		t.Fatalf("legacy compat spelling after canonical adoption = (%d, %v), want the object — old replicas 404 otherwise", size, err)
	}
}

// failLegacyCopyBlob fails every copy to a legacy-spelled destination a
// bounded number of times, then succeeds — the transient-backend shape.
type failLegacyCopyBlob struct {
	blob.BlobStore
	failures atomic.Int64
}

func (b *failLegacyCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	if !strings.Contains(dstKey, "/o2/") && b.failures.Add(-1) >= 0 {
		return fmt.Errorf("simulated compat copy failure")
	}
	return b.BlobStore.Copy(ctx, dstKey, srcKey)
}

// TestLFSCompatCopy_FailurePropagatesAndRetryConverges (Codex round 83):
// a swallowed compat-copy failure returned 200 while v0.2.0 replicas 404
// the object — and no client retry could ever repair it, because the
// registered-object fast paths returned success without re-attempting the
// copy. The upload must FAIL when the compat bridge cannot land the legacy
// spelling, and a retry of the registered object must repair it.
func TestLFSCompatCopy_FailurePropagatesAndRetryConverges(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSV020WriteCompat = true
	fb := &failLegacyCopyBlob{BlobStore: srv.Blob}
	fb.failures.Store(1) // the first compat copy fails, the repair succeeds
	srv.Blob = fb
	tok := mint(auth.ScopeGitWrite)
	content := []byte("compat copy fails once")
	oid := sha256Hex(content)
	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid

	w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+tok, string(content))
	if w.Code == http.StatusOK {
		t.Fatalf("PUT with a failed compat copy = 200 — old replicas 404 the object and no retry ever repairs it")
	}
	// The canonical object is durable and registered regardless…
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, blob.LFSKey(lfsTestRepoID, oid)); err != nil || !has {
		t.Fatalf("canonical registration = (%v, %v), want committed", has, err)
	}
	// …and the client's retry lands on the registered fast path, which
	// must REPAIR the missing legacy spelling instead of short-circuiting.
	w = doLFS(t, srv, http.MethodPut, objPath, "basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("retry PUT = %d, body %s", w.Code, w.Body)
	}
	size, err := srv.Blob.Stat(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid))
	if err != nil || size != int64(len(content)) {
		t.Fatalf("legacy spelling after retry = (%d, %v), want repaired", size, err)
	}
}

// TestLFSCompatCopy_BatchRetryRepairsBridge (Codex round 84): after a
// failed compat copy, the NATURAL client retry is another batch request —
// its registered/canonical-present fast path returned "stored" with no
// actions and no repair, so the missing legacy spelling could never
// converge through the protocol's own flow. The batch fast path must
// repair the bridge (or fail the entry retryably).
func TestLFSCompatCopy_BatchRetryRepairsBridge(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	content := []byte("bridge repaired from the batch path")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	// The post-failure state: canonical bytes + row, NO legacy spelling.
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error != nil || len(resp.Objects[0].Actions) != 0 {
		t.Fatalf("batch touch of stored object = %+v, want stored", resp.Objects[0])
	}
	size, err := srv.Blob.Stat(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid))
	if err != nil || size != int64(len(content)) {
		t.Fatalf("legacy spelling after batch retry = (%d, %v), want repaired — old replicas 404 and nothing else converges", size, err)
	}
}

// TestLFSPut_PublicationHoldsFleetAdoptionClaim (Codex round 84): the
// registered-row corrupt-byte delete in the adoption mismatch path is
// fingerprint-conditional, but the check and the delete are not atomic — a
// verified PUT publishing valid bytes in that window (after its own final
// writer verify) would lose them with no repair left. Canonical publishers
// therefore serialize on the SAME fleet claim adoption holds: with the
// oid's claim held elsewhere, a verified PUT answers 429 and publishes
// nothing; once released, the retry succeeds. Under that serialization no
// publisher can be mid-flight while adoption's mismatch delete runs.
func TestLFSPut_PublicationHoldsFleetAdoptionClaim(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("upload racing a fleet adoption claim")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	ctx := context.Background()
	claimed, owner, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, key)
	if err != nil || !claimed {
		t.Fatalf("seed claim = (%v, %v), want held", claimed, err)
	}

	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+tok, string(content))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("PUT under a held fleet claim = %d, body %s (want 429 — publication must serialize with adoption's canonical writes)", w.Code, w.Body)
	}
	if _, serr := srv.Blob.Stat(ctx, key); !errors.Is(serr, blob.ErrNotFound) {
		t.Fatalf("claim-blocked PUT published canonical bytes (stat err %v)", serr)
	}
	if err := st.ReleaseAdoptionClaim(ctx, key, owner); err != nil {
		t.Fatal(err)
	}
	if w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+tok, string(content)); w.Code != http.StatusOK {
		t.Fatalf("PUT after claim release = %d, body %s", w.Code, w.Body)
	}
}

// TestLFSCompatCopy_ChargedAgainstQuota (Codex security round 84): the
// bridge copy materializes a SECOND size bytes, but its cover reserved
// under math.MaxInt64 — an upload equal to the remaining headroom returned
// 200 and left physical usage near 2Q. The bridge is fresh growth and must
// pass real quota admission; at the quota edge the upload fails (the
// canonical object stays registered — headroom later lets a retry repair
// the bridge), and physical usage never exceeds the quota.
func TestLFSCompatCopy_ChargedAgainstQuota(t *testing.T) {
	ctx := context.Background()
	srv, _, mint := newLFSQuotaServer(t, 100, time.Hour)
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("q"), 60) // 60 fits; 2×60 exceeds Q=100
	oid := sha256Hex(content)

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code == http.StatusOK {
		t.Fatal("PUT whose bridge copy exceeds the quota = 200 — physical usage reached ~2Q uncharged")
	}
	usage, err := blob.StorageUsageBytes(ctx, srv.Blob, lfsTestRepoID)
	if err != nil {
		t.Fatal(err)
	}
	if usage > 100 {
		t.Fatalf("physical usage after quota-edge compat upload = %d, want <= 100 — the bridge must be admitted before it materializes", usage)
	}
}

// TestLFSPut_RegisteredFastPathCompatQuotaDenialIs507 (Codex round 103):
// a direct PUT of an already-registered canonical object takes
// adoptLFSObject's registered fast path, whose write-compat repair of a
// missing legacy spelling can be quota-denied. The PUT switch handled only
// the adoption copy's sentinel, so the bridge's sentinel fell through to a
// generic 500 — hiding the documented 507 free-space remedy that the
// batch and fresh-upload paths already report.
func TestLFSPut_RegisteredFastPathCompatQuotaDenialIs507(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 100, time.Hour)
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("t"), 60) // 60 registered; the bridge's second 60 exceeds Q=100
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	// Registered canonical object with intact bytes and NO legacy copy:
	// the PUT re-upload lands on the registered fast path and enters the
	// compat bridge, whose admission (60 stored + 60 incoming > 100)
	// denies the copy.
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusInsufficientStorage {
		t.Fatalf("registered fast-path PUT whose compat copy exceeds the quota = %d, body %s — quota denials are 507, not a generic failure", w.Code, w.Body)
	}
}

// TestLFSCompatCopy_ExistingLegacySkipsAdmission (Codex round 89): a
// zero-growth repair PUT — registered-but-blobless canonical whose legacy
// copy survived — reaches the bridge with the legacy spelling ALREADY
// present: the copy would only overwrite those bytes, yet the admission
// charged the full size as fresh growth. With canonical restored (S) and
// legacy retained (S) at quota 2S, the bridge evaluated S+S+S=3S and the
// otherwise-successful repair 507'd forever. An existing legacy spelling
// satisfies the bridge (ensureLegacyCompat's semantics: the legacy key is
// writable by v0.1.0 presigned PUTs, so overwriting buys no integrity).
func TestLFSCompatCopy_ExistingLegacySkipsAdmission(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 120, time.Hour)
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("r"), 60) // 2×60 fits Q=120 exactly; 3×60 does not
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)

	// The repair state: a registered canonical row whose blob is gone,
	// with its legacy bridge copy intact.
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, legacyKey, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("zero-growth repair with an intact legacy copy = %d, body %s — the bridge must not re-charge an overwrite", w.Code, w.Body)
	}
	if _, err := srv.Blob.Stat(ctx, key); err != nil {
		t.Fatalf("canonical bytes missing after repair: %v", err)
	}
	if _, err := srv.Blob.Stat(ctx, legacyKey); err != nil {
		t.Fatalf("legacy bytes missing after repair: %v", err)
	}
}

// TestLFSAdopt_CopyChargedAgainstQuota (Codex round 85, P1 + security
// twin): the adoption copy creates a second size-byte canonical object
// while the legacy source is retained until the post-roll sweep — with
// only the unbounded restore cover admitting it, adopting an S-byte object
// at quota Q=S durably left 2S physical bytes. The copy must pass real
// quota admission (the reservation settles at registration; the retained
// source stays floor-charged), and a denial fails the adoption with 507.
func TestLFSAdopt_CopyChargedAgainstQuota(t *testing.T) {
	ctx := context.Background()
	seed := func(t *testing.T, srv *Server, content []byte) string {
		oid := sha256Hex(content)
		if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
			t.Fatal(err)
		}
		return oid
	}
	t.Run("at-quota-denied", func(t *testing.T) {
		srv, st, mint := newLFSQuotaServer(t, 100, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		content := bytes.Repeat([]byte("a"), 60) // legacy 60 + copy 60 > Q=100
		oid := seed(t, srv, content)
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, int64(len(content)))))
		if resp.Objects[0].Error == nil {
			t.Fatal("over-quota adoption copy succeeded — physical usage reached 2S at Q=S")
		}
		if code := resp.Objects[0].Error.Code; code != http.StatusInsufficientStorage {
			t.Fatalf("over-quota adoption error code = %d, want 507", code)
		}
		if has, err := st.HasLFSObject(ctx, lfsTestRepoID, blob.LFSKey(lfsTestRepoID, oid)); err != nil || has {
			t.Fatalf("registered despite quota denial = (%v, %v)", has, err)
		}
		usage, err := blob.StorageUsageBytes(ctx, srv.Blob, lfsTestRepoID)
		if err != nil {
			t.Fatal(err)
		}
		if usage > 100 {
			t.Fatalf("physical usage after denied adoption = %d, want <= 100", usage)
		}
	})
	t.Run("with-headroom-adopts", func(t *testing.T) {
		srv, st, mint := newLFSQuotaServer(t, 300, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		content := bytes.Repeat([]byte("b"), 60)
		oid := seed(t, srv, content)
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, int64(len(content)))))
		if resp.Objects[0].Error != nil || len(resp.Objects[0].Actions) != 0 {
			t.Fatalf("in-quota adoption = %+v, want stored", resp.Objects[0])
		}
		if has, err := st.HasLFSObject(ctx, lfsTestRepoID, blob.LFSKey(lfsTestRepoID, oid)); err != nil || !has {
			t.Fatalf("adopted = (%v, %v), want registered", has, err)
		}
	})
}

// TestLFSCompatCopy_AdmissionRetiredAfterSuccess (Codex round 85): the
// bridge's quota admission row lingered until the reservation TTL even
// after the copy succeeded and the refreshed floor charged the same bytes
// — double-charging unrelated uploads for up to an hour. After the cover
// handoff, only the deliberate tail cover may remain reserved.
func TestLFSCompatCopy_AdmissionRetiredAfterSuccess(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("r"), 50)
	oid := sha256Hex(content)
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body %s", w.Code, w.Body)
	}
	// Canonical settled at registration; the bridge tail cover (50) is
	// deliberate and TTL-bounded — the bridge ADMISSION row must be gone.
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != int64(len(content)) {
		t.Fatalf("reserved after compat success = %d, want %d (tail cover only) — a lingering admission double-charges until TTL", ls.ReservedBytes, len(content))
	}
}

// TestLFSAdopt_LegacyRowMigratesAtObjectCap (Codex round 86): a v0.2.0
// legacy-spelled registry row already holds the object's cap slot, and
// registration migrates it in place — zero growth for both ledgers — but
// the adoption cap precheck recovered only the CANONICAL spelling, so at
// MaxLFSObjectsPerRepo every write-scoped touch of such an object
// permanently answered 507, blocking exactly the migration that frees
// nothing and adds nothing.
func TestLFSAdopt_LegacyRowMigratesAtObjectCap(t *testing.T) {
	old := ref.MaxLFSObjectsPerRepo
	ref.MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("legacy-registered at the cap")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	// The v0.2.0 state occupying the only cap slot: a legacy-spelled row
	// with its legacy blob.
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, legacyKey, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, legacyKey, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error != nil || len(resp.Objects[0].Actions) != 0 {
		t.Fatalf("at-cap touch of a legacy-registered object = %+v, want stored — the in-place migration is zero-growth", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("canonical row after migration = (%v, %v), want registered", has, err)
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, legacyKey); err != nil || has {
		t.Fatalf("legacy row after migration = (%v, %v), want migrated away", has, err)
	}
}

// TestLFS_BloblessLegacyRowIsZeroGrowthRepair (Codex round 87): a v0.2.0
// legacy-spelled registry row whose blob is GONE (storage loss before the
// upgrade) is a zero-growth repair — registration migrates the row in
// place, adding no count and no bytes — but both write surfaces classified
// the OID as NEW: at the byte quota or object cap they answered 507 with
// no upload action, so the row could never be repaired without an
// operator raising a limit. The round-86 fix ran only after a successful
// legacy blob stat, which this blobless state never reaches.
func TestLFS_BloblessLegacyRowIsZeroGrowthRepair(t *testing.T) {
	old := ref.MaxLFSObjectsPerRepo
	ref.MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })
	ctx := context.Background()
	content := []byte(strings.Repeat("z", 64))
	oid := sha256Hex(content)

	t.Run("batch-issues-upload-action", func(t *testing.T) {
		// At the byte quota AND the object cap: the repair must be
		// exempt from both.
		srv, st, mint := newLFSQuotaServer(t, 64, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		if err := st.RegisterLFSObject(ctx, lfsTestRepoID, blob.LFSLegacyKey(lfsTestRepoID, oid), 64); err != nil {
			t.Fatal(err)
		}
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, 64)))
		if resp.Objects[0].Error != nil {
			t.Fatalf("blobless legacy-row batch touch at quota+cap = %+v, want an upload action — a 507 here is permanent", resp.Objects[0])
		}
		if len(resp.Objects[0].Actions) == 0 {
			t.Fatal("no upload action for a blobless legacy-row object — nothing can ever repair the row")
		}
	})
	t.Run("put-repairs-and-migrates", func(t *testing.T) {
		srv, st, mint := newLFSQuotaServer(t, 64, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		key := blob.LFSKey(lfsTestRepoID, oid)
		legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
		if err := st.RegisterLFSObject(ctx, lfsTestRepoID, legacyKey, 64); err != nil {
			t.Fatal(err)
		}
		w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
			"basic:"+tok, string(content))
		if w.Code != http.StatusOK {
			t.Fatalf("repair PUT of a blobless legacy-row object = %d, body %s — must be admitted as a zero-growth repair", w.Code, w.Body)
		}
		if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
			t.Fatalf("canonical row after repair = (%v, %v), want migrated in place", has, err)
		}
		if has, err := st.HasLFSObject(ctx, lfsTestRepoID, legacyKey); err != nil || has {
			t.Fatalf("legacy row after repair = (%v, %v), want migrated away", has, err)
		}
		if _, err := srv.Blob.Stat(ctx, key); err != nil {
			t.Fatalf("canonical bytes missing after repair: %v", err)
		}
	})
}

// bridgeOnClaimStore lands the legacy spelling exactly when the compat
// claim is WON — the serialized peer (or an old replica's presigned PUT)
// completing the bridge between the lock-free probe and this worker's
// claim acquisition.
type bridgeOnClaimStore struct {
	ref.RefStore
	t         *testing.T
	blobs     blob.BlobStore
	legacyKey string
	content   []byte
}

func (b *bridgeOnClaimStore) TryAdoptionClaim(ctx context.Context, repo, key string) (bool, string, error) {
	claimed, owner, err := b.RefStore.TryAdoptionClaim(ctx, repo, key)
	if err == nil && claimed && key == b.legacyKey {
		if uerr := b.blobs.UploadFile(ctx, b.legacyKey, writeTempFile(b.t, b.content)); uerr != nil {
			return false, "", uerr
		}
	}
	return claimed, owner, err
}

// TestLFSCompatCopy_ClaimWinnerRechecksDestination (Codex round 90): the
// destination probe runs BEFORE the fleet claim is acquired, so a peer's
// compat copy landing in between left this worker treating the bridge as
// missing — recharging the full size as fresh growth (a spurious 507 at
// the quota edge) and recopying identical bytes. The claim winner must
// re-probe the destination before admitting anything.
func TestLFSCompatCopy_ClaimWinnerRechecksDestination(t *testing.T) {
	ctx := context.Background()
	// Q holds both objects plus admission slack, but NOT a third copy:
	// a fresh-growth bridge admission (60 more) must 507.
	srv, st, mint := newLFSQuotaServer(t, 130, time.Hour)
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	// An unrelated registered object consumes half the quota.
	other := bytes.Repeat([]byte("o"), 60)
	otherOID := sha256Hex(other)
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, blob.LFSKey(lfsTestRepoID, otherOID), 60); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, blob.LFSKey(lfsTestRepoID, otherOID), writeTempFile(t, other)); err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("c"), 60)
	oid := sha256Hex(content)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	srv.Store = &bridgeOnClaimStore{RefStore: st, t: t, blobs: srv.Blob, legacyKey: legacyKey, content: content}

	// The fresh upload fits (60+60 = Q); the bridge lands via the "peer"
	// as the claim is won — a recheck sees it, a fresh-growth admission
	// would demand 60 more and 507.
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT whose bridge a peer completed under the claim = %d, body %s — the claim winner must re-probe before admitting fresh growth", w.Code, w.Body)
	}
	if _, err := srv.Blob.Stat(ctx, legacyKey); err != nil {
		t.Fatalf("legacy spelling missing: %v", err)
	}
}

// TestLFSCompatCopy_BoundedByOpSlots (Codex round 92): distinct-OID batch
// touches each launch a size-scaled server-side bridge copy, and the
// per-key claim dedupes only one OID while quota reservations are
// per-repository (or disabled) — without a cross-key bound, one
// write-scoped client could fan out unbounded simultaneous multipart
// copies and exhaust backend connections despite the op-slot protection on
// uploads and adoptions. The bridge must take the same repository-fair
// node-wide operation slot.
func TestLFSCompatCopy_BoundedByOpSlots(t *testing.T) {
	oldOps := lfsMaxStagingOps
	lfsMaxStagingOps = 1
	t.Cleanup(func() { lfsMaxStagingOps = oldOps })
	ctx := context.Background()
	srv, _, _ := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSV020WriteCompat = true
	content := []byte("bridge copy needs an op slot")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	// Every op slot is busy (another repository's upload holds it).
	if !srv.reserveLFSStaging("repo_other", 0) {
		t.Fatal("seed op slot")
	}
	err := srv.maybeLegacyCompatCopy(ctx, lfsTestRepoID, oid, key, int64(len(content)))
	if !errors.Is(err, errLFSBusy) {
		t.Fatalf("bridge copy with zero free op slots = %v, want errLFSBusy — unbounded concurrent multipart copies exhaust the backend", err)
	}
	if _, serr := srv.Blob.Stat(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid)); !errors.Is(serr, blob.ErrNotFound) {
		t.Fatalf("copy ran despite op-slot exhaustion (stat err %v)", serr)
	}
	// Slot freed: the retry converges.
	srv.releaseLFSStaging("repo_other", 0)
	if err := srv.maybeLegacyCompatCopy(ctx, lfsTestRepoID, oid, key, int64(len(content))); err != nil {
		t.Fatalf("bridge copy with a free slot: %v", err)
	}
	if _, serr := srv.Blob.Stat(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid)); serr != nil {
		t.Fatalf("legacy spelling missing after freed-slot retry: %v", serr)
	}
}

// lateLossStore renews the canonical claim truthfully until registration
// commits, then reports every renewal as lost — a lease takeover landing
// exactly in the publication's tail.
type lateLossStore struct {
	ref.RefStore
	key  string
	lost atomic.Bool
}

func (s *lateLossStore) RegisterLFSObject(ctx context.Context, repo, key string, n int64) error {
	err := s.RefStore.RegisterLFSObject(ctx, repo, key, n)
	if key == s.key {
		s.lost.Store(true)
	}
	return err
}

func (s *lateLossStore) RenewAdoptionClaim(ctx context.Context, key, owner string) (bool, error) {
	if key == s.key && s.lost.Load() {
		return false, nil // expired and taken over
	}
	return s.RefStore.RenewAdoptionClaim(ctx, key, owner)
}

// commitDespiteLossBlob completes the post-registration verify stat
// exactly as the region is canceled — the backend read that returns
// success concurrently with the renewer's loss verdict.
type commitDespiteLossBlob struct {
	blob.BlobStore
	key  string
	lost *atomic.Bool
}

func (b *commitDespiteLossBlob) Stat(ctx context.Context, key string) (int64, error) {
	if key == b.key && b.lost.Load() {
		select {
		case <-ctx.Done(): // the loss canceled the region; the read already returned
			return b.BlobStore.Stat(context.WithoutCancel(ctx), key)
		case <-time.After(5 * time.Second):
			return 0, fmt.Errorf("lease loss never canceled the publication region")
		}
	}
	return b.BlobStore.Stat(ctx, key)
}

// TestLFSPut_ClaimLossRejectsSuccessfulPublication (Codex round 93, P1 —
// the PUT twin of round 88's adoption-copy fix): the publication region's
// renewer was deferred but never JOINED and checked, and cancellation
// alone cannot reject work that COMMITS concurrently with a recorded loss
// — the final region op can return nil as the renewer cancels, and the
// handler then continues to a 200 on a lease it no longer provably holds,
// while the claim's successor may be writing different canonical bytes
// past this worker's verify. The handler must join the renewer and reject
// any recorded loss before reporting success.
func TestLFSPut_ClaimLossRejectsSuccessfulPublication(t *testing.T) {
	oldInterval := lfsAdoptClaimRenewInterval
	lfsAdoptClaimRenewInterval = 10 * time.Millisecond
	t.Cleanup(func() { lfsAdoptClaimRenewInterval = oldInterval })
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("publication outlives its lease")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	ls := &lateLossStore{RefStore: st, key: key}
	srv.Store = ls
	srv.Blob = &commitDespiteLossBlob{BlobStore: srv.Blob, key: key, lost: &ls.lost}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code == http.StatusOK {
		t.Fatal("PUT answered 200 on a lease lost during publication — success must be rejected after a recorded claim loss")
	}
}

// gateCanonicalCopyBlob holds copies toward canonical destinations open —
// the adoption-copy twin of gateCopyBlob.
type gateCanonicalCopyBlob struct {
	blob.BlobStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *gateCanonicalCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	if strings.Contains(dstKey, "/o2/") {
		b.once.Do(func() { close(b.started) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.BlobStore.Copy(ctx, dstKey, srcKey)
}

// TestLFSCopyAdmission_SingleChargeDuringCopy (Codex round 94): both
// server-side copies reserved TWICE for the same planned bytes — a real
// admission row (the object key) plus the renewed repair cover (the
// suffixed key) — for the copy's whole, potentially hours-long window:
// with usage S and quota 3S, adopting an S-byte object left S of genuine
// post-copy headroom while unrelated uploads saw S used + 2S reserved and
// were denied throughout. One reservation must both admit and cover.
func TestLFSCopyAdmission_SingleChargeDuringCopy(t *testing.T) {
	content := bytes.Repeat([]byte("d"), 60)
	oid := sha256Hex(content)

	t.Run("adoption", func(t *testing.T) {
		ctx := context.Background()
		srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
			t.Fatal(err)
		}
		gate := &gateCanonicalCopyBlob{BlobStore: srv.Blob, started: make(chan struct{}), release: make(chan struct{})}
		srv.Blob = gate
		done := make(chan struct{})
		go func() {
			defer close(done)
			doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
				batchBody("upload", oid, int64(len(content))))
		}()
		select {
		case <-gate.started:
		case <-time.After(10 * time.Second):
			t.Fatal("adoption copy never started")
		}
		reserved := ledgerState(t, st, time.Hour).ReservedBytes
		close(gate.release)
		<-done
		if reserved >= 2*int64(len(content)) {
			t.Fatalf("mid-copy reserved = %d for a %d-byte adoption — the admission row and the renewed cover double-charge the same planned bytes", reserved, len(content))
		}
	})
	t.Run("bridge", func(t *testing.T) {
		srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
		srv.LFSV020WriteCompat = true
		tok := mint(auth.ScopeGitWrite)
		gate := &gateCopyBlob{BlobStore: srv.Blob, started: make(chan struct{}), release: make(chan struct{})}
		srv.Blob = gate
		done := make(chan struct{})
		go func() {
			defer close(done)
			doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
				"basic:"+tok, string(content))
		}()
		select {
		case <-gate.started:
		case <-time.After(10 * time.Second):
			t.Fatal("bridge copy never started")
		}
		reserved := ledgerState(t, st, time.Hour).ReservedBytes
		close(gate.release)
		<-done
		if reserved >= 2*int64(len(content)) {
			t.Fatalf("mid-copy reserved = %d for a %d-byte bridge — the admission row and the renewed cover double-charge the same planned bytes", reserved, len(content))
		}
	})
}

// commitButFailCopyBlob performs the legacy copy, then reports failure —
// the committed-but-response-lost S3 outcome.
type commitButFailCopyBlob struct {
	blob.BlobStore
	failed atomic.Bool
}

func (b *commitButFailCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	err := b.BlobStore.Copy(ctx, dstKey, srcKey)
	if err == nil && !strings.Contains(dstKey, "/o2/") && !b.failed.Swap(true) {
		return fmt.Errorf("simulated lost CopyObject response")
	}
	return err
}

// TestLFSCompatCopy_AmbiguousCommitHandsOff (Codex round 95, P1): an S3
// copy can commit its destination and still return an error (response
// lost, context canceled concurrently) — the bridge then skipped both the
// cover tail and the floor-generation handoff, and every retry saw the
// legacy key present and skipped them too: the durable bytes stayed
// invisible to pre-copy cached floors, whose 60-second serve-and-handoff
// window could admit another upload without accounting for them. An
// ambiguous copy failure must probe the destination; a present spelling
// is a COMMIT and takes the success path's handoff.
func TestLFSCompatCopy_AmbiguousCommitHandsOff(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("g"), 50)
	oid := sha256Hex(content)
	srv.Blob = &commitButFailCopyBlob{BlobStore: srv.Blob}
	preGen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID)
	if err != nil {
		t.Fatal(err)
	}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT whose bridge copy committed despite the error = %d, body %s — the destination probe proves the commit", w.Code, w.Body)
	}
	if _, serr := srv.Blob.Stat(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid)); serr != nil {
		t.Fatalf("legacy spelling missing: %v", serr)
	}
	if gen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID); err != nil || gen == preGen {
		t.Fatalf("adoption gen after ambiguous-commit bridge = (%d -> %d, %v), want bumped — the durable copy stays floor-invisible without the handoff", preGen, gen, err)
	}
}

// commitButFailCanonicalCopyBlob performs the canonical adoption copy,
// then reports failure — the committed-but-response-lost outcome on the
// adoption path.
type commitButFailCanonicalCopyBlob struct {
	blob.BlobStore
	failed atomic.Bool
}

func (b *commitButFailCanonicalCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	err := b.BlobStore.Copy(ctx, dstKey, srcKey)
	if err == nil && strings.Contains(dstKey, "/o2/") && !b.failed.Swap(true) {
		return fmt.Errorf("simulated lost CopyObject response")
	}
	return err
}

// TestLFSAdopt_AmbiguousCommitContinues (Codex round 96, P1 — the
// adoption twin of round 95's bridge fix): a canonical adoption copy can
// commit its destination and still return an error. Returning immediately
// skipped the cover tail, leaving the durable ROWLESS canonical bytes
// invisible to a pre-copy cached floor for its whole serve-and-handoff
// window — registration's atomic bump only heals after some later retry.
// A present destination under a still-held lease is a commit: re-cover
// and continue into the destination re-verify, which digest-proves the
// bytes exactly as after a clean copy and registers with the atomic
// handoff.
func TestLFSAdopt_AmbiguousCommitContinues(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := bytes.Repeat([]byte("h"), 50)
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Blob = &commitButFailCanonicalCopyBlob{BlobStore: srv.Blob}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error != nil || len(resp.Objects[0].Actions) != 0 {
		t.Fatalf("adoption whose copy committed despite the error = %+v, want stored — the destination probe proves the commit and the re-verify trusts only the digest", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("registered = (%v, %v), want adopted", has, err)
	}
}

// flakyAccountingStore fails suffixed-cover reservations and generation
// bumps a bounded number of times once armed — the transiently unavailable
// ref store behind an ambiguous-commit recovery.
type flakyAccountingStore struct {
	ref.RefStore
	armed         atomic.Bool
	coverFails    atomic.Int64
	bumpFails     atomic.Int64
	coverAttempts atomic.Int64
}

func (f *flakyAccountingStore) ReservePackBytes(ctx context.Context, group []string, packs []ref.Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ref.ReserveDecision, error) {
	if f.armed.Load() && len(packs) == 1 && strings.HasSuffix(packs[0].Key, ref.RepairCoverSuffix) {
		f.coverAttempts.Add(1)
		if f.coverFails.Add(-1) >= 0 {
			return ref.ReserveDecision{}, fmt.Errorf("simulated ref-store outage (cover)")
		}
	}
	return f.RefStore.ReservePackBytes(ctx, group, packs, owner, usageFloor, floorGen, quotaBytes, ttl)
}

func (f *flakyAccountingStore) BumpLFSAdoptionGen(ctx context.Context, repo string) error {
	if f.armed.Load() && f.bumpFails.Add(-1) >= 0 {
		return fmt.Errorf("simulated ref-store outage (bump)")
	}
	return f.RefStore.BumpLFSAdoptionGen(ctx, repo)
}

// armingCommitButFailCopyBlob is commitButFailCopyBlob that also arms the
// flaky store the moment the ambiguous commit happens.
type armingCommitButFailCopyBlob struct {
	blob.BlobStore
	arm       *atomic.Bool
	canonical bool // fail the canonical-destination copy instead of legacy
	failed    atomic.Bool
}

func (b *armingCommitButFailCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	err := b.BlobStore.Copy(ctx, dstKey, srcKey)
	if err == nil && strings.Contains(dstKey, "/o2/") == b.canonical && !b.failed.Swap(true) {
		b.arm.Store(true)
		return fmt.Errorf("simulated lost CopyObject response")
	}
	return err
}

// TestLFSAmbiguousCommit_AccountingConverges (Codex round 97, two P1s):
// the rounds-95/96 ambiguous-commit recoveries attempted their cover and
// generation handoff ONCE and swallowed failures — yet every later retry
// sees the destination present and skips the seam entirely, so a
// transiently unavailable ref store left the durable bytes floor-invisible
// forever. The recovery must retry until the accounting lands (or every
// pre-copy floor has provably aged out).
func TestLFSAmbiguousCommit_AccountingConverges(t *testing.T) {
	oldRetry := ambiguousAccountingRetryInterval
	ambiguousAccountingRetryInterval = 5 * time.Millisecond
	t.Cleanup(func() { ambiguousAccountingRetryInterval = oldRetry })
	content := bytes.Repeat([]byte("k"), 50)
	oid := sha256Hex(content)

	t.Run("bridge-bump-lands", func(t *testing.T) {
		ctx := context.Background()
		srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
		srv.LFSV020WriteCompat = true
		tok := mint(auth.ScopeGitWrite)
		fs := &flakyAccountingStore{RefStore: st}
		fs.coverFails.Store(1)
		fs.bumpFails.Store(1)
		srv.Store = fs
		srv.Blob = &armingCommitButFailCopyBlob{BlobStore: srv.Blob, arm: &fs.armed}
		preGen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID)
		if err != nil {
			t.Fatal(err)
		}
		w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
			"basic:"+tok, string(content))
		if w.Code != http.StatusOK {
			t.Fatalf("PUT = %d, body %s", w.Code, w.Body)
		}
		if gen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID); err != nil || gen == preGen {
			t.Fatalf("adoption gen after transiently failing handoff = (%d -> %d, %v), want bumped — a swallowed one-shot failure leaves the durable bytes floor-invisible forever", preGen, gen, err)
		}
	})
	t.Run("adoption-cover-retries", func(t *testing.T) {
		ctx := context.Background()
		srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
			t.Fatal(err)
		}
		fs := &flakyAccountingStore{RefStore: st}
		fs.coverFails.Store(1)
		srv.Store = fs
		srv.Blob = &armingCommitButFailCopyBlob{BlobStore: srv.Blob, arm: &fs.armed, canonical: true}
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, int64(len(content)))))
		if resp.Objects[0].Error != nil || len(resp.Objects[0].Actions) != 0 {
			t.Fatalf("adoption = %+v, want stored", resp.Objects[0])
		}
		if got := fs.coverAttempts.Load(); got < 2 {
			t.Fatalf("ambiguous-commit cover attempts = %d, want >= 2 — the recovery must retry until the cover lands before continuing into the re-hash", got)
		}
	})
}

// TestLFSAdopt_CapRaceWinnerRepairsBridge (Codex round 98): at the object
// cap, a concurrent upload can register THIS canonical key between the
// initial registered check and the cap recheck — and the recheck's early
// success return bypassed ensureLegacyCompat, unlike the registered fast
// path. The client could then commit its pointer while the concurrent
// writer's potentially long bridge copy was still in flight; if that copy
// failed (or its requester vanished), the legacy spelling stayed absent
// and old replicas 404'd with no retry driven by this success.
func TestLFSAdopt_CapRaceWinnerRepairsBridge(t *testing.T) {
	old := ref.MaxLFSObjectsPerRepo
	ref.MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSV020WriteCompat = true
	content := []byte("cap-race twin with a missing bridge")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	// The twin's committed state: a registered canonical (holding the
	// only cap slot) with bytes present but NO legacy spelling — its
	// bridge copy failed after registration.
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	// The initial registry read misses the twin's registration; the
	// at-cap recheck sees it — the round-27 boundary race.
	srv.Store = &staleRegistryOnce{RefStore: st}

	if aerr := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid); aerr != nil {
		t.Fatalf("cap-race twin-win adoption = %v", aerr)
	}
	if _, err := srv.Blob.Stat(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid)); err != nil {
		t.Fatalf("legacy spelling missing after cap-race success: %v — the race winner must repair the bridge like the registered fast path", err)
	}
}

// legacyTwinOnCapCountStore registers this OID's LEGACY row just before
// the PUT's cap count reads — a mid-roll old replica registering the same
// object between the entry lookup and the count.
type legacyTwinOnCapCountStore struct {
	ref.RefStore
	repo, legacyKey string
	size            int64
	done            atomic.Bool
}

func (s *legacyTwinOnCapCountStore) CountLFSObjects(ctx context.Context, repo string) (int64, error) {
	if !s.done.Swap(true) {
		if err := s.RefStore.RegisterLFSObject(ctx, s.repo, s.legacyKey, s.size); err != nil {
			return 0, err
		}
	}
	return s.RefStore.CountLFSObjects(ctx, repo)
}

// TestLFSPut_CapRaceRechecksLegacyRow (Codex round 99, the PUT twin of the
// adoption path's round-86 recheck): mid-roll, an old replica can register
// this OID's LEGACY row between the PUT's entry lookup and its cap count —
// the boundary recheck consulted only the canonical spelling and answered
// 507, even though registration migrates the legacy row in place and adds
// no object.
func TestLFSPut_CapRaceRechecksLegacyRow(t *testing.T) {
	old := ref.MaxLFSObjectsPerRepo
	ref.MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	content := []byte("legacy twin lands mid-race")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	srv.Store = &legacyTwinOnCapCountStore{RefStore: st, repo: lfsTestRepoID, legacyKey: legacyKey, size: int64(len(content))}

	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT racing a legacy-row twin at the cap = %d, body %s — migration adds no object, the cap must not deny it", w.Code, w.Body)
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("canonical row = (%v, %v), want migrated in place", has, err)
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, legacyKey); err != nil || has {
		t.Fatalf("legacy row = (%v, %v), want migrated away", has, err)
	}
}

// coverRenewalOutageStore fails every unbounded cover renewal (the
// MaxInt64-quota reservations) while real admissions succeed — a ref-store
// outage spanning the copy's whole cover-continuity budget.
type coverRenewalOutageStore struct {
	ref.RefStore
}

func (s *coverRenewalOutageStore) ReservePackBytes(ctx context.Context, group []string, packs []ref.Pack, owner string, floor, floorGen, quota int64, ttl time.Duration) (ref.ReserveDecision, error) {
	if quota == math.MaxInt64 {
		return ref.ReserveDecision{}, fmt.Errorf("simulated cover-renewal outage")
	}
	return s.RefStore.ReservePackBytes(ctx, group, packs, owner, floor, floorGen, quota, ttl)
}

// TestLFSAdopt_UncoveredGapCommitRejected (Codex round 105, P1): a copy
// can commit while cover continuity is lost — WithRenewedCover surfaces
// renewErr — and both copy paths merely logged it: the durable bytes were
// then accepted (and registered) although a stale-floor admission may have
// consumed headroom that never accounted them, durably exceeding the
// quota. The adoption must reject the copy, remove the rowless
// destination, and let the retry re-admit from scratch. Runs the REAL
// cover-continuity budget (~50s); the bridge twin shares the seam.
func TestLFSAdopt_UncoveredGapCommitRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the real ~45s cover-continuity budget")
	}
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 1<<20, time.Hour)
	content := []byte("uncovered gap commit")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Store = &coverRenewalOutageStore{RefStore: st}

	if aerr := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid); aerr == nil {
		t.Fatal("adoption accepted a copy whose cover continuity broke — a stale-floor admission may already have spent this headroom")
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || has {
		t.Fatalf("registered after an uncovered-gap copy = (%v, %v), want not registered", has, err)
	}
	if _, err := srv.Blob.Stat(ctx, key); err == nil {
		t.Fatal("rowless uncovered destination retained — physical usage could exceed quota until the sweep")
	}
}

// uncoveredCleanupOutageStore forces the round-107 shape: cover renewals
// fail (an outage spanning the continuity budget) until the uncovered-copy
// cleanup runs, whose guarded delete then errors — an INDETERMINATE
// outcome with the bytes still present. The outage lifts afterwards, so
// the accounting recovery's own cover can land.
type uncoveredCleanupOutageStore struct {
	ref.RefStore
	cleanupRan      atomic.Bool
	cleanupAttempts atomic.Int32
	failDeletes     int32 // attempts that fail before the real delete runs
}

func (s *uncoveredCleanupOutageStore) ReservePackBytes(ctx context.Context, group []string, packs []ref.Pack, owner string, floor, floorGen, quota int64, ttl time.Duration) (ref.ReserveDecision, error) {
	if quota == math.MaxInt64 && !s.cleanupRan.Load() {
		return ref.ReserveDecision{}, fmt.Errorf("simulated cover-renewal outage")
	}
	return s.RefStore.ReservePackBytes(ctx, group, packs, owner, floor, floorGen, quota, ttl)
}

func (s *uncoveredCleanupOutageStore) GuardedLFSDelete(ctx context.Context, repo, key string, del func(context.Context) error) (bool, error) {
	s.cleanupRan.Store(true)
	if s.cleanupAttempts.Add(1) <= s.failDeletes {
		return false, fmt.Errorf("simulated indeterminate cleanup outcome")
	}
	return s.RefStore.GuardedLFSDelete(ctx, repo, key, del)
}

// TestLFSAdopt_IndeterminateCleanupRecoversAccounting (Codex round 107,
// P1 — the adoption twin of round 106's compat recovery): when the
// uncovered-gap cleanup's guarded delete ERRORS, the outcome is
// indeterminate — the rowless canonical bytes may remain, and the
// crashed-adoption retry registers them without any copy admission, so
// leaving them unaccounted lets physical usage exceed quota for the whole
// stale-floor window. The failure path must recover the accounting: a
// fresh cover plus the fleet floor invalidation before the error returns.
// Runs the real ~45s cover-continuity budget.
func TestLFSAdopt_IndeterminateCleanupRecoversAccounting(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the real ~45s cover-continuity budget")
	}
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 1<<20, time.Hour)
	content := []byte("indeterminate cleanup keeps the charge")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Store = &uncoveredCleanupOutageStore{RefStore: st, failDeletes: 1 << 30} // never proven: the retry window exhausts
	preGen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID)
	if err != nil {
		t.Fatal(err)
	}

	if aerr := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid); aerr == nil {
		t.Fatal("adoption accepted an uncovered-gap copy")
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || has {
		t.Fatalf("registered = (%v, %v), want not registered", has, err)
	}
	// The delete never provably ran: the canonical bytes remain — the
	// recovery must cover their charge AND invalidate cached floors (the
	// generation bump is the recovery's observable handoff; nothing else
	// on this failure path bumps it).
	if _, err := srv.Blob.Stat(ctx, key); err != nil {
		t.Fatalf("canonical bytes unexpectedly gone: %v", err)
	}
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes < int64(len(content)) {
		t.Fatalf("reserved after an indeterminate cleanup = %d, want >= %d — the retained bytes must stay covered until floors refresh", ls.ReservedBytes, len(content))
	}
	if gen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID); err != nil || gen == preGen {
		t.Fatalf("adoption gen after an indeterminate cleanup = (%d -> %d, %v), want bumped — stale floors would hide the retained bytes otherwise", preGen, gen, err)
	}
}

// commitAfterCancelBlob commits the copy only AFTER its context is
// canceled, then returns the cancellation — S3 durably committing while
// the response reports the (cover-loss) failure.
type commitAfterCancelBlob struct {
	blob.BlobStore
	dst string
}

func (b *commitAfterCancelBlob) Copy(ctx context.Context, dst, src string) error {
	if dst != b.dst {
		return b.BlobStore.Copy(ctx, dst, src)
	}
	<-ctx.Done()
	if err := b.BlobStore.Copy(context.WithoutCancel(ctx), dst, src); err != nil {
		return err
	}
	return context.Cause(ctx)
}

// TestLFSAdopt_CoverCanceledCommitRejected (Codex security round 107): a
// cover-canceled copy surfaces through copyErr — renewErr stays nil — so
// only the round-96 ambiguous-commit branch saw it: the destination was
// present, the claim renewable, and the committed-but-uncovered bytes were
// ACCEPTED and registered although a stale-floor admission may have spent
// their headroom during the gap. The cover-loss verdict must take round
// 105's remedy — delete under the held claim and fail — regardless of
// which return value carried it. Runs the real ~45s continuity budget.
func TestLFSAdopt_CoverCanceledCommitRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the real ~45s cover-continuity budget")
	}
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 1<<20, time.Hour)
	content := []byte("cover-canceled commit")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Store = &coverRenewalOutageStore{RefStore: st}
	srv.Blob = &commitAfterCancelBlob{BlobStore: srv.Blob, dst: key}

	aerr := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid)
	if aerr == nil {
		t.Fatal("adoption accepted a cover-canceled commit — a stale-floor admission may already have spent this headroom")
	}
	if !errors.Is(aerr, blob.ErrCoverContinuityLost) {
		t.Fatalf("adoption error = %v, want the cover-loss verdict surfaced", aerr)
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || has {
		t.Fatalf("registered after a cover-canceled commit = (%v, %v), want not registered", has, err)
	}
	if _, err := srv.Blob.Stat(ctx, key); err == nil {
		t.Fatal("cover-canceled destination retained — physical usage could exceed quota until the sweep")
	}
}

// readCountingBlob counts ranged verification reads per key.
type readCountingBlob struct {
	blob.BlobStore
	reads atomic.Int64
}

func (b *readCountingBlob) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	b.reads.Add(1)
	return b.BlobStore.ReadRange(ctx, key, offset, length)
}

// TestLFSAdopt_QuotaPreflightSkipsHash (Codex security round 108): the
// copy admission ran AFTER the full-object verification hash, so an
// over-quota repository re-read the entire legacy blob on every
// write-scoped touch — a 1,000-entry batch of one OID re-hashed it 1,000
// times with no byte ever uploading. A cheap advisory ledger read must
// fail the hopeless adoption BEFORE any ranged read.
func TestLFSAdopt_QuotaPreflightSkipsHash(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 100, time.Hour)
	// The repo is at quota: an unrelated registered object fills it.
	filler := bytes.Repeat([]byte("f"), 100)
	fillerOID := sha256Hex(filler)
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, blob.LFSKey(lfsTestRepoID, fillerOID), 100); err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("x"), 50)
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	rc := &readCountingBlob{BlobStore: srv.Blob}
	srv.Blob = rc

	aerr := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid)
	if !errors.Is(aerr, errLFSAdoptQuotaExceeded) {
		t.Fatalf("over-quota adoption = %v, want errLFSAdoptQuotaExceeded", aerr)
	}
	if n := rc.reads.Load(); n != 0 {
		t.Fatalf("over-quota adoption performed %d ranged reads before denying — every retry re-reads the whole blob for free", n)
	}
}

// TestLFSAdopt_UncoveredCleanupRetriesUntilProven (Codex round 108, P1):
// accounting recovery cannot revoke an admission that already exploited
// the uncovered gap — only deleting the bytes restores the quota
// invariant. A transiently failing cleanup must therefore be RETRIED until
// the outcome is proven, not immediately traded for a cover. Runs the real
// cover-continuity budget plus two retry intervals (~55s).
func TestLFSAdopt_UncoveredCleanupRetriesUntilProven(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the real ~45s cover-continuity budget")
	}
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 1<<20, time.Hour)
	content := []byte("transient cleanup failure resolves by retry")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Store = &uncoveredCleanupOutageStore{RefStore: st, failDeletes: 2}

	if aerr := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid); aerr == nil {
		t.Fatal("adoption accepted an uncovered-gap copy")
	}
	if _, err := srv.Blob.Stat(ctx, key); err == nil {
		t.Fatal("uncovered destination survived transient cleanup failures — deletion must be retried until proven, not traded for a cover")
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || has {
		t.Fatalf("registered = (%v, %v), want not registered", has, err)
	}
}

// forgedFirstReadBlob serves length-matched forged bytes for the FIRST
// ranged read only — the verification hash reads a mismatching object that
// a presigned PUT then restores before the cleanup re-examines it.
type forgedFirstReadBlob struct {
	blob.BlobStore
	served atomic.Bool
}

func (b *forgedFirstReadBlob) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	chunk, err := b.BlobStore.ReadRange(ctx, key, offset, length)
	if err == nil && !b.served.Swap(true) {
		forged := bytes.Repeat([]byte("z"), len(chunk))
		return forged, nil
	}
	return chunk, err
}

// TestLFSAdopt_SourceMismatchCleanupSparesRestoredSource (Codex round 110,
// the source twin of round 108's destination-mismatch guard): the initial
// source verification proves only the bytes IT READ wrong — a still-valid
// legacy presigned PUT can restore the correct object between the hash and
// the cleanup delete, and the unconditional delete removed that newly
// valid, potentially only, copy. The cleanup must re-hash the CURRENT
// source and delete only bytes still proven mismatched.
func TestLFSAdopt_SourceMismatchCleanupSparesRestoredSource(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	content := []byte("restored by a presigned PUT mid-cleanup")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, legacyKey, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Blob = &forgedFirstReadBlob{BlobStore: srv.Blob}

	aerr := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid)
	if !errors.Is(aerr, errLFSContentMismatch) {
		t.Fatalf("adoption of a mismatching-then-restored source = %v, want errLFSContentMismatch", aerr)
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || has {
		t.Fatalf("registered = (%v, %v), want not registered", has, err)
	}
	if _, err := srv.Blob.Stat(ctx, legacyKey); err != nil {
		t.Fatalf("restored legacy source deleted by the source-mismatch cleanup: %v — it was the object's only valid copy", err)
	}
}

// forgeOnCopyBlob simulates the ABA the destination re-verification
// exists for: the copy "catches" forged interim bytes — the destination
// receives them — while the source's good content stands (a presigned PUT
// restored it during verification).
type forgeOnCopyBlob struct {
	blob.BlobStore
	t      *testing.T
	forged []byte
}

func (b *forgeOnCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	if strings.Contains(dstKey, "/o2/") {
		return b.BlobStore.UploadFile(ctx, dstKey, writeTempFile(b.t, b.forged))
	}
	return b.BlobStore.Copy(ctx, dstKey, srcKey)
}

// TestLFSAdopt_MismatchCleanupSparesRestoredSource (Codex round 108, P2):
// the destination's failed post-copy digest proves only what the COPY
// caught — the legacy source may have been restored to the correct object
// while the destination verified, and the unconditional cleanup deleted
// that only valid copy (404s on old replicas, forced client re-uploads).
// The cleanup must re-hash the CURRENT source and delete only proven-wrong
// bytes.
func TestLFSAdopt_MismatchCleanupSparesRestoredSource(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	content := []byte("the good bytes a presigned PUT restored")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, legacyKey, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	srv.Blob = &forgeOnCopyBlob{BlobStore: srv.Blob, t: t, forged: []byte("forged interim bytes the copy caught")}

	aerr := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid)
	if !errors.Is(aerr, errLFSContentMismatch) {
		t.Fatalf("adoption whose copy caught forged bytes = %v, want errLFSContentMismatch", aerr)
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || has {
		t.Fatalf("registered = (%v, %v), want not registered", has, err)
	}
	if _, err := srv.Blob.Stat(ctx, legacyKey); err != nil {
		t.Fatalf("restored legacy source deleted by the mismatch cleanup: %v — it was the object's only valid copy", err)
	}
}

// TestLFSCompat_RepairSerializedOnLegacyClaim (Codex security round 85):
// the batch fast path's bridge repair launched an unserialized full copy —
// every tiny concurrent touch of the object started another one. Compat
// copies now serialize on the LEGACY key's fleet claim: with it held
// elsewhere, the repair answers retryable contention and copies nothing.
func TestLFSCompat_RepairSerializedOnLegacyClaim(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSV020WriteCompat = true
	tok := mint(auth.ScopeGitWrite)
	content := []byte("repair blocked by the legacy claim")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, content)); err != nil {
		t.Fatal(err)
	}
	if err := st.RegisterLFSObject(ctx, lfsTestRepoID, key, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	claimed, _, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, legacyKey)
	if err != nil || !claimed {
		t.Fatalf("seed legacy claim = (%v, %v), want held", claimed, err)
	}

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(content)))))
	if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusTooManyRequests {
		t.Fatalf("bridge repair under a held legacy claim = %+v, want per-object 429", resp.Objects[0])
	}
	if _, serr := srv.Blob.Stat(ctx, legacyKey); !errors.Is(serr, blob.ErrNotFound) {
		t.Fatalf("repair copied despite the held claim (stat err %v)", serr)
	}
}

// renewCountingStore counts adoption-claim renewals for one key.
type renewCountingStore struct {
	ref.RefStore
	renews atomic.Int64
}

func (r *renewCountingStore) RenewAdoptionClaim(ctx context.Context, key, owner string) (bool, error) {
	r.renews.Add(1)
	return r.RefStore.RenewAdoptionClaim(ctx, key, owner)
}

// slowLegacySourceCopyBlob delays copies FROM legacy sources long enough
// for renewal ticks to fire.
type slowLegacySourceCopyBlob struct {
	blob.BlobStore
	delay time.Duration
}

func (b *slowLegacySourceCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	if !strings.Contains(srcKey, "/o2/") {
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.BlobStore.Copy(ctx, dstKey, srcKey)
}

// TestLFSAdopt_PublishCopyRenewsClaimThroughout (Codex security round 83):
// the publish copy was clamped to ref.AdoptionClaimTTL−5m (25 minutes), so
// any sequential multipart copy slower than that was CANCELED with all
// completed parts aborted — a guaranteed-lost, repeatable full-object cost
// for large legacy blobs. The copy must instead run under its size-scaled
// allowance with the claim RENEWED on the adoption cadence throughout, so
// a live backend always finishes.
func TestLFSAdopt_PublishCopyRenewsClaimThroughout(t *testing.T) {
	ctx := context.Background()
	oldInterval := lfsAdoptClaimRenewInterval
	lfsAdoptClaimRenewInterval = 20 * time.Millisecond
	t.Cleanup(func() { lfsAdoptClaimRenewInterval = oldInterval })

	srv, _, _ := newLFSQuotaServer(t, 0, time.Hour)
	legacy := []byte("large legacy object with a slow publish copy")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	rs := &renewCountingStore{RefStore: srv.Store}
	srv.Store = rs
	srv.Blob = &slowLegacySourceCopyBlob{BlobStore: srv.Blob, delay: 150 * time.Millisecond}

	if err := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid); err != nil {
		t.Fatalf("adopt with slow publish copy: %v", err)
	}
	// Entry fence (1) plus at least one mid-copy renewal: without the
	// renewal loop, a copy outliving the lease runs unfenced and any
	// >25m copy is canceled outright.
	if got := rs.renews.Load(); got < 2 {
		t.Fatalf("claim renewals across the publish copy = %d, want >= 2 (entry fence + mid-copy cadence)", got)
	}
}

// TestLFSAdopt_ClaimLossRejectsSuccessfulOp (Codex round 88, P1): the
// renewal loop cancels the op region when the lease is lost, but a long S3
// copy can COMMIT and return nil concurrently with that cancellation. An
// unconditional success verdict would let the stale worker proceed to
// destination verification and registration after another replica took the
// claim — the successor can overwrite the canonical key between those two
// steps, registering bytes nobody verified at rest. A recorded loss must
// dominate even a successful op, and the renewer must be JOINED before the
// verdict so a loss it records cannot race it.
func TestLFSAdopt_ClaimLossRejectsSuccessfulOp(t *testing.T) {
	ctx := context.Background()
	oldInterval := lfsAdoptClaimRenewInterval
	lfsAdoptClaimRenewInterval = 20 * time.Millisecond
	t.Cleanup(func() { lfsAdoptClaimRenewInterval = oldInterval })

	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	key := blob.LFSKey(lfsTestRepoID, strings.Repeat("ab", 32))
	claimed, owner, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, key)
	if err != nil || !claimed {
		t.Fatalf("claim = (%v, %v)", claimed, err)
	}
	err = srv.withRenewedAdoptionClaim(ctx, key, owner, func(opCtx context.Context) error {
		// The lease is lost mid-op: the owner's claim goes away and a
		// competitor takes it over, so the next renewal records the loss
		// and cancels the region...
		if rerr := st.ReleaseAdoptionClaim(ctx, key, owner); rerr != nil {
			t.Fatal(rerr)
		}
		if taken, _, terr := st.TryAdoptionClaim(ctx, lfsTestRepoID, key); terr != nil || !taken {
			t.Fatalf("takeover claim = (%v, %v)", taken, terr)
		}
		<-opCtx.Done()
		// ...but the in-flight copy still commits and reports success.
		return nil
	})
	if err == nil {
		t.Fatal("successful op after a recorded lease loss must be rejected, not registered")
	}
}

// TestLFSAdopt_PublishCopyBudgetNotClampedByClaimTTL (Codex security round
// 83): the copy deadline must scale with the object size — a 10 GiB
// multipart copy at realistic throughput needs ~an hour, and the old
// 25-minute clamp guaranteed cancellation.
func TestLFSAdopt_PublishCopyBudgetNotClampedByClaimTTL(t *testing.T) {
	srv, _, _ := newLFSQuotaServer(t, 0, time.Hour)
	cb := &deadlineCapturingBlob{BlobStore: srv.Blob}
	srv.Blob = cb
	claimed, owner, err := srv.Store.TryAdoptionClaim(context.Background(), lfsTestRepoID, "lfs/"+lfsTestRepoID+"/o2/"+strings.Repeat("aa", 32))
	if err != nil || !claimed {
		t.Fatalf("claim = (%v, %v)", claimed, err)
	}
	key := blob.LFSKey(lfsTestRepoID, strings.Repeat("aa", 32))
	legacyKey := blob.LFSLegacyKey(lfsTestRepoID, strings.Repeat("aa", 32))
	if err := srv.Blob.UploadFile(context.Background(), legacyKey, writeTempFile(t, []byte("x"))); err != nil {
		t.Fatal(err)
	}
	if err := srv.publishVerifiedLFSObject(context.Background(), lfsTestRepoID, key, legacyKey, 10<<30, owner); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := time.Duration(cb.budget.Load()); got < 2*time.Hour {
		t.Fatalf("publish copy budget = %v for a 10 GiB object, want the size-scaled allowance (>= ~2h51m), not the 25m claim-TTL clamp", got)
	}
}

// gateCopyBlob blocks copies TO legacy-spelled destinations until release
// is closed, capturing the moment the compat copy is in flight.
type gateCopyBlob struct {
	blob.BlobStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *gateCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	if !strings.Contains(dstKey, "/o2/") {
		b.once.Do(func() { close(b.started) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.BlobStore.Copy(ctx, dstKey, srcKey)
}

// TestLFSPut_CompatCopyReleasesUploadResources (Codex round 82): the
// compat copy can legitimately run for a size-scaled window — holding the
// upload's staging budget, op slot, and per-key lock through it would let
// a few slow copies pin the node and 429 unrelated transfers whose
// canonical bytes are already durable. The handler must free the upload
// resources BEFORE the copy.
func TestLFSPut_CompatCopyReleasesUploadResources(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSV020WriteCompat = true
	gate := &gateCopyBlob{BlobStore: srv.Blob, started: make(chan struct{}), release: make(chan struct{})}
	srv.Blob = gate
	tok := mint(auth.ScopeGitWrite)
	content := []byte("slow compat copy")
	oid := sha256Hex(content)

	done := make(chan int, 1)
	go func() {
		w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
			"basic:"+tok, string(content))
		done <- w.Code
	}()
	select {
	case <-gate.started:
	case <-time.After(10 * time.Second):
		t.Fatal("compat copy never started")
	}
	// The copy is in flight: the UPLOAD's resources must already be free —
	// its staged bytes and its op slot are returned before the bridge
	// runs. The copy holds exactly ONE operation-only slot of its own
	// (Codex round 92): the bridge is bounded by the same repository-fair
	// pool as uploads and adoptions, so ops == 1 here is the copy's slot,
	// not a leaked upload slot; bytes stay zero (a server-side copy
	// stages nothing).
	srv.lfsStagingMu.Lock()
	ops, inflight := srv.lfsStagingOps, srv.lfsStagingInFlight
	srv.lfsStagingMu.Unlock()
	if ops != 1 || inflight != 0 {
		close(gate.release)
		t.Fatalf("staging during compat copy = ops %d, bytes %d — want the copy's own op slot (1) and zero staged bytes", ops, inflight)
	}
	if _, busy := srv.lfsUploadsInFlight.Load(blob.LFSKey(lfsTestRepoID, oid)); busy {
		close(gate.release)
		t.Fatal("per-key lock still held during the compat copy")
	}
	close(gate.release)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", code)
	}
}

// coverCountingStore counts repair-cover reservations (key suffix
// ref.RepairCoverSuffix) passing through ReservePackBytes.
type coverCountingStore struct {
	ref.RefStore
	covers atomic.Int64
	mu     sync.Mutex
	perKey map[string]int
}

func (c *coverCountingStore) ReservePackBytes(ctx context.Context, group []string, packs []ref.Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ref.ReserveDecision, error) {
	if len(packs) == 1 && strings.HasSuffix(packs[0].Key, ref.RepairCoverSuffix) {
		c.covers.Add(1)
		c.mu.Lock()
		if c.perKey == nil {
			c.perKey = map[string]int{}
		}
		c.perKey[packs[0].Key]++
		c.mu.Unlock()
	}
	return c.RefStore.ReservePackBytes(ctx, group, packs, owner, usageFloor, floorGen, quotaBytes, ttl)
}

// coversFor returns how many cover reservations named exactly coverKey.
func (c *coverCountingStore) coversFor(coverKey string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.perKey[coverKey]
}

// TestLFSAdoption_CopyCoverIsRenewed (Codex round 79): the adoption copy's
// stale-floor cover must run under blob.WithRenewedCover — a one-shot
// reservation with a near-minimum TTL (~61s) expires under a multipart
// copy allowed many minutes, leaving admissions an uncovered window. The
// renewed seam provably takes the initial cover AND the mandatory
// post-copy tail refresh (≥2 reservations); the one-shot took exactly 1.
func TestLFSAdoption_CopyCoverIsRenewed(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour) // quota ENABLED: covers required
	tok := mint(auth.ScopeGitWrite)
	legacy := []byte("covered adoption copy")
	oid := sha256Hex(legacy)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	cs := &coverCountingStore{RefStore: srv.Store}
	srv.Store = cs

	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("covered adoption failed: %+v", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, blob.LFSKey(lfsTestRepoID, oid)); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
	if got := cs.covers.Load(); got < 2 {
		t.Fatalf("cover reservations during the copy = %d, want >= 2 (initial + mandatory tail refresh)", got)
	}
}

// TestLFSAdoption_RegistrationRetryReverifiesReplacedBlob (Codex round
// 77): the round-76 completed-hash memo trusted SIZE alone. During a
// v0.1.0 rolling upgrade a still-valid legacy presigned PUT can overwrite
// the rowless key with different bytes of the SAME length between the
// failed registration and the retry — a memo that skips the digest then
// registers the forgery as verified content, and adopted objects never
// re-verify. The retry may skip the completed prefix, but it must always
// re-check the full digest over exactly size bytes before registering.
func TestLFSAdoption_RegistrationRetryReverifiesReplacedBlob(t *testing.T) {
	ctx := context.Background()
	oldChunk := lfsStagingChunk
	lfsStagingChunk = 4
	t.Cleanup(func() { lfsStagingChunk = oldChunk })

	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	legacy := []byte("legacy 12b!!")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	srv.Store = &failOnceRegisterStore{RefStore: srv.Store}

	// Attempt 1: the hash completes, the registration fails.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error == nil {
		t.Fatalf("adoption with failed registration reported stored: %+v", resp.Objects[0])
	}
	// The still-valid legacy presigned PUT swaps in same-length forged
	// bytes while the completed verification is parked. The pause outlasts
	// the kernel's coarse mtime tick: the FILE backend's fingerprint is
	// mtime-derived (see bucketStatFingerprint's granularity caveat), and
	// this test proves the fingerprint BINDING, which needs the rewrite to
	// be distinguishable — production S3 ETags are content-derived and
	// need no such spacing.
	time.Sleep(25 * time.Millisecond)
	forged := []byte("forged 12b!!")
	if len(forged) != len(legacy) {
		t.Fatal("fixture: forged bytes must match the legacy length")
	}
	if err := srv.Blob.UploadFile(ctx, key, writeTempFile(t, forged)); err != nil {
		t.Fatal(err)
	}
	// Attempt 2 must NOT register the forgery as verified content.
	decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || has {
		t.Fatalf("HasLFSObject after same-size replacement = (%v, %v), want unregistered — the retry skipped the digest re-check", has, err)
	}
}

// TestLFSRepair_CoversRepublishedBytesAgainstStaleFloors (Codex round 57,
// security): the repair's generation bump only stops FUTURE floor serves —
// an admission already holding a floor scanned during the blob's absence
// (deflated by the missing object's size, hiding equally sized rowless
// legacy bytes) can commit its reservation after the repair republished,
// durably exceeding quota. The repair must account its bytes BEFORE
// publication: a cover reservation that every stale-floor admission sees
// in active_reservations, compensating the worst-case deflation until the
// TTL outlives every absence-window floor.
func TestLFSRepair_CoversRepublishedBytesAgainstStaleFloors(t *testing.T) {
	ctx := context.Background()
	body := []byte("registered object body, counter-accounted") // 41 bytes
	quota := int64(len(body)) + 20                              // exactly fits body + the racer
	srv, st, mint := newLFSQuotaServer(t, quota, time.Hour)
	tok := mint(auth.ScopeGitWrite)

	oid := sha256Hex(body)
	key := blob.LFSKey(lfsTestRepoID, oid)
	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	if w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+tok, string(body)); w.Code != http.StatusOK {
		t.Fatalf("upload PUT status = %d, body %s", w.Code, w.Body)
	}
	// Rowless legacy bytes a deflated absence-window floor would hide.
	legacy := []byte("rowless legacy")
	if err := srv.Blob.UploadFile(ctx, blob.LFSKey(lfsTestRepoID, sha256Hex(legacy)), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// The registered blob vanishes; a racing admission's floor is scanned
	// now (deflated to 0) and held as a plain integer across the repair.
	if err := srv.Blob.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	staleFloor := int64(0)
	// The racer validated the shared floor generation BEFORE the repair
	// (the round-60 transactional recheck also rejects gens that move; the
	// cover must deny even a racer whose recheck token predates the bump).
	staleGen, err := st.LFSAdoptionGen(ctx, lfsTestRepoID)
	if err != nil {
		t.Fatal(err)
	}
	// The repair republishes the counter-accounted bytes (zero-growth).
	if w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+tok, string(body)); w.Code != http.StatusOK {
		t.Fatalf("repair PUT status = %d, body %s", w.Code, w.Body)
	}
	// The racer commits with its stale floor. Without the repair's cover,
	// counter (41) + floor (0) + reservations (0) + incoming (20) == 61
	// fits the quota exactly — admitting 20 fresh bytes while the hidden
	// legacy bytes and the repaired object both occupy storage.
	dec, err := st.ReservePackBytes(ctx, blob.QuotaRepoGroup(lfsTestRepoID),
		[]ref.Pack{{Key: "packs/" + lfsTestRepoID + "/racer.pack", Size: 20}},
		ref.NewReservationOwner(), staleFloor, staleGen, quota, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if dec.OK {
		t.Fatalf("stale-floor admission passed inside the repair window (used=%d reserved=%d) — repaired bytes must be reserved before republication",
			dec.UsedBytes, dec.ReservedBytes)
	}
}

// reapFreshOnRegister deletes the blob just before a FRESH verified
// upload's registration commits — the writer-half sweep race whose repair
// the PUT's post-registration verify performs (issue #5).
type reapFreshOnRegister struct {
	ref.Store
	bl  blob.BlobStore
	key string
}

func (r reapFreshOnRegister) RegisterLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	if key == r.key {
		if err := r.bl.Delete(ctx, key); err != nil {
			return fmt.Errorf("test reap: %v", err)
		}
	}
	return r.Store.RegisterLFSObject(ctx, repo, key, bytes)
}

// TestLFSPut_SweepRaceRestoreIsCoveredAndBumpsGen (Codex round 58): the
// PUT's post-registration verify re-uploads bytes a racing sweep reaped —
// counter-accounted bytes made physical again. Like every restore, it must
// take a pre-publication cover (admissions holding an absence-window floor
// see the returning bytes in active_reservations) and bump the shared
// floor generation (future serves re-scan).
func TestLFSPut_SweepRaceRestoreIsCoveredAndBumpsGen(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 1000, time.Hour)
	tok := mint(auth.ScopeGitWrite)

	content := []byte("fresh object reaped mid-registration")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)
	srv.Store = reapFreshOnRegister{Store: st, bl: srv.Blob, key: key}

	gen0, err := st.LFSAdoptionGen(ctx, lfsTestRepoID)
	if err != nil {
		t.Fatal(err)
	}
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusOK {
		t.Fatalf("sweep-raced PUT status = %d, body %s", w.Code, w.Body)
	}
	if _, err := srv.Blob.Stat(ctx, key); err != nil {
		t.Fatalf("restore did not republish: %v", err)
	}
	// The restore's cover holds the republished bytes beyond the settled
	// upload reservation (settlement consumed the upload's own row).
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != int64(len(content)) {
		t.Fatalf("ledger after sweep-race restore = %+v, want the %d-byte cover reserved", ls, len(content))
	}
	gen1, err := st.LFSAdoptionGen(ctx, lfsTestRepoID)
	if err != nil {
		t.Fatal(err)
	}
	if gen1 == gen0 {
		t.Fatal("sweep-race restore did not bump the shared floor generation")
	}
}

// TestLFSAdopt_FleetClaimDedupesConcurrentHashes (Codex round 73,
// security): the per-key in-flight map dedupes only within one process —
// replicas racing the same legacy oid each downloaded and hashed the
// whole object, multiplying backend reads by fleet size for every
// distinct legacy oid an attacker touches. Adoption now takes a shared
// TTL'd claim first: while one replica holds it, another replica's
// adoption of the same oid denies transiently WITHOUT downloading, and
// converges once the claim is released.
func TestLFSAdopt_FleetClaimDedupesConcurrentHashes(t *testing.T) {
	ctx := context.Background()
	srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	legacy := []byte("legacy object hashed on one replica only")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(ctx, blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	cb := &downloadCountingBlob{BlobStore: srv.Blob}
	srv.Blob = cb

	// "Another replica" holds the fleet claim mid-hash.
	claimed, foreignOwner, err := st.TryAdoptionClaim(ctx, lfsTestRepoID, key)
	if err != nil || !claimed {
		t.Fatalf("seed claim = (%v, %v), want held", claimed, err)
	}
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error == nil {
		t.Fatalf("adoption proceeded under a foreign fleet claim: %+v", resp.Objects[0])
	}
	if cb.downloads != 0 {
		t.Fatalf("losing replica performed %d verification reads, want 0 — the whole point is not downloading", cb.downloads)
	}
	// A stale owner's release must NOT free the live lease (Codex round
	// 74): the fence keeps a taken-over worker from unlocking the
	// successor's claim.
	if err := st.ReleaseAdoptionClaim(ctx, key, "stale-owner"); err != nil {
		t.Fatal(err)
	}
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error == nil {
		t.Fatal("a stale owner's release freed the live fleet claim")
	}
	// The true owner releases (crash → TTL expiry); the retry converges.
	if err := st.ReleaseAdoptionClaim(ctx, key, foreignOwner); err != nil {
		t.Fatal(err)
	}
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error != nil || resp.Objects[0].Actions != nil {
		t.Fatalf("post-release adoption failed: %+v", resp.Objects[0])
	}
	if has, err := st.HasLFSObject(ctx, lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("object not adopted: %v, %v", has, err)
	}
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
	if err := srv.Blob.UploadFile(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
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
	srv.LFSStagingBytes = 100 // share bound (round 55 quantum): 100/3 = 33
	tok := mint(auth.ScopeGitWrite)

	// Permanently over any repository's share: 413.
	huge := bytes.Repeat([]byte("h"), 50)
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(huge),
		"basic:"+tok, string(huge))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("over-budget PUT status = %d, want 413", w.Code)
	}

	// Park one 30-byte upload; a concurrent DISTINCT 30-byte upload no
	// longer fits the repository's free-scaled byte share: 429.
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
	srv.LFSStagingBytes = 300 // per-repo share bound (round 55): 100
	tok := mint(auth.ScopeGitWrite)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	content := bytes.Repeat([]byte("s"), 100) // the whole repository share
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
	// reservations; a fresh full-share upload for another oid then fits.
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
// at-quota repository must be able to repair itself. Since round 57 the
// repair leaves a TTL-collected COVER reservation equal to the repaired
// bytes (usage is still credited once): stale absence-window floors held
// by concurrent admissions must see the republished bytes in
// active_reservations, and deny-side conservatism until the TTL is the
// accepted cost.
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
	// Counted once: usage stays at the original credit; the repair's
	// cover reservation (round 57) holds the republished bytes until the
	// TTL collects it.
	if ls := ledgerState(t, st, time.Hour); ls.UsageBytes != 100 || ls.ReservedBytes != 100 {
		t.Fatalf("ledger after repair = %+v, want usage=100 reserved=100 (the cover)", ls)
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
		[]ref.Pack{{Key: key, Size: 90}}, ref.NewReservationOwner(), 0, ref.ReserveNoFloorGen, 100, time.Hour)
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
		if err := srv.Blob.UploadFile(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
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
		if err := srv.Blob.UploadFile(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
			t.Fatal(err)
		}
		srv.lfsStagingMu.Lock() // every buffer slot held elsewhere on the node
		srv.lfsVerifyOps = lfsMaxVerifyBuffers
		srv.lfsStagingMu.Unlock()
		t.Cleanup(func() {
			srv.lfsStagingMu.Lock()
			srv.lfsVerifyOps = 0
			srv.lfsStagingMu.Unlock()
		})
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, int64(len(legacy)))))
		if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusTooManyRequests {
			t.Fatalf("slot-exhausted verification did not deny transiently: %+v", resp.Objects[0])
		}
	})

	t.Run("repo-share-isolates-repos", func(t *testing.T) {
		// Codex round 33: one repository's adoptions occupy only its own
		// share of the verification buffers, never the whole pool.
		srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		if err := srv.Blob.UploadFile(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
			t.Fatal(err)
		}
		srv.lfsStagingMu.Lock()
		srv.lfsRepoVerifyOps = map[string]int{lfsTestRepoID: lfsMaxRepoVerifyBuffers}
		srv.lfsVerifyOps = lfsMaxRepoVerifyBuffers // pool mostly free
		srv.lfsStagingMu.Unlock()
		t.Cleanup(func() {
			srv.lfsStagingMu.Lock()
			srv.lfsRepoVerifyOps, srv.lfsVerifyOps = nil, 0
			srv.lfsStagingMu.Unlock()
		})
		// Same repository: denied at its share.
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, int64(len(legacy)))))
		if resp.Objects[0].Error == nil || resp.Objects[0].Error.Code != http.StatusTooManyRequests {
			t.Fatalf("share-exhausted same-repo adoption not denied: %+v", resp.Objects[0])
		}
		// Another repository still reserves.
		if !srv.reserveLFSVerifyBuffer("repo_other") {
			t.Fatal("unrelated repository starved by another repository's verify share")
		}
		srv.releaseLFSVerifyBuffer("repo_other")
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
// declaring the largest permissible Content-Length while dripping bytes
// must NOT reserve the declared size up front — staging grows as data
// actually arrives (one lfsStagingChunk ahead), so an unrelated upload
// still admits while the dripper stalls. The budget is 10 chunks (round
// 38's byte fairness caps a declaration at the budget-third allowance, so
// the historical whole-budget declaration is 413-rejected outright now —
// strictly stronger; the drip scenario continues inside the allowance).
func TestLFSStaging_DripperCannotPinBudget(t *testing.T) {
	oldChunk := lfsStagingChunk
	lfsStagingChunk = 4
	t.Cleanup(func() { lfsStagingChunk = oldChunk })

	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	srv.LFSStagingBytes = 40
	tok := mint(auth.ScopeGitWrite)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	// The dripper: declares the whole per-repo allowance (40/3 = 13),
	// sends one byte, stalls.
	dripContent := bytes.Repeat([]byte("d"), 13)
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut,
		ts.URL+"/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(dripContent), pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = 13
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

// TestLFSCap_TwinRegistrationAtBoundaryIsZeroGrowth (Codex round 27): at
// the object-cap boundary, a concurrent identical upload can register THIS
// key between the registered read and the count read, consuming the last
// slot — the request is then zero-growth, not over-cap, and must succeed
// instead of 507ing during a successful identical upload. Covers both
// single-key fast-fail sites: batch adoption of stored bytes and the
// direct PUT.
func TestLFSCap_TwinRegistrationAtBoundaryIsZeroGrowth(t *testing.T) {
	old := ref.MaxLFSObjectsPerRepo
	ref.MaxLFSObjectsPerRepo = 1
	t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })

	content := []byte("boundary twin object")
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	t.Run("batch-adoption-of-stored-bytes", func(t *testing.T) {
		srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		if err := srv.Blob.UploadFile(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, content)); err != nil {
			t.Fatal(err)
		}
		// The twin registers this key right after the first registered
		// read, taking the cap's only slot.
		srv.Store = &twinWinsStore{Store: st, key: key, size: int64(len(content))}
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, int64(len(content)))))
		if resp.Objects[0].Error != nil {
			t.Fatalf("boundary-raced adoption denied: %+v", resp.Objects[0].Error)
		}
		if resp.Objects[0].Actions != nil {
			t.Fatalf("registered object still got an upload action: %+v", resp.Objects[0])
		}
	})

	t.Run("direct-put", func(t *testing.T) {
		srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		srv.Store = &twinWinsStore{Store: st, key: key, size: int64(len(content))}
		w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
			"basic:"+tok, string(content))
		if w.Code != http.StatusOK {
			t.Fatalf("boundary-raced PUT status = %d, body %s", w.Code, w.Body)
		}
	})
}

// TestLFSBatch_TwinRegistrationBeforeDenialIsZeroGrowth (Codex round 28):
// the batch's quota and object-cap denials can rest on a STALE
// classification — a twin can register a requested OID after the initial
// registry read. On the brink of either denial the classification is
// refreshed and newly registered keys flip to zero-growth (their bytes and
// row are already counted), so the batch issues actions instead of 507ing
// during a successful identical upload.
func TestLFSBatch_TwinRegistrationBeforeDenialIsZeroGrowth(t *testing.T) {
	content := bytes.Repeat([]byte("b"), 90)
	oid := sha256Hex(content)
	key := blob.LFSKey(lfsTestRepoID, oid)

	t.Run("quota-denial-path", func(t *testing.T) {
		srv, st, mint := newLFSQuotaServer(t, 100, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		// The twin registers the key (crediting its 90 bytes) right after
		// the batch's classification read: stale incoming would then
		// double the bytes past the quota.
		srv.Store = &twinWinsStore{Store: st, key: key, size: 90}
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, 90)))
		if resp.Objects[0].Error != nil {
			t.Fatalf("twin-raced batch denied on quota: %+v", resp.Objects[0].Error)
		}
		if resp.Objects[0].Actions == nil {
			t.Fatalf("twin-raced batch issued no action: %+v", resp.Objects[0])
		}
	})

	t.Run("cap-denial-path", func(t *testing.T) {
		old := ref.MaxLFSObjectsPerRepo
		ref.MaxLFSObjectsPerRepo = 1
		t.Cleanup(func() { ref.MaxLFSObjectsPerRepo = old })
		srv, st, mint := newLFSQuotaServer(t, 0, time.Hour)
		tok := mint(auth.ScopeGitWrite)
		// The twin's registration consumes the only slot — with the key
		// registered, this batch adds no object and must not 507.
		srv.Store = &twinWinsStore{Store: st, key: key, size: 90}
		resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
			batchBody("upload", oid, 90)))
		if resp.Objects[0].Error != nil {
			t.Fatalf("twin-raced batch denied on the object cap: %+v", resp.Objects[0].Error)
		}
		if resp.Objects[0].Actions == nil {
			t.Fatalf("twin-raced batch issued no action: %+v", resp.Objects[0])
		}
	})
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

// TestLFSStaging_RepoSlotShareIsolatesRepos (Codex round 32, security):
// parked bodyless PUTs hold op slots for the idle window, and tokens are
// repo-scoped — so one repository's writer filling slots must be confined
// to its own share (lfsMaxRepoStagingOps), never starving other
// repositories' uploads and adoptions on the replica.
func TestLFSStaging_RepoSlotShareIsolatesRepos(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	// The attacker's repository holds its whole share; the node-wide cap
	// still has plenty of slots left.
	srv.lfsStagingMu.Lock()
	srv.lfsRepoStagingOps = map[string]int{lfsTestRepoID: lfsMaxRepoStagingOps}
	srv.lfsStagingOps = lfsMaxRepoStagingOps
	srv.lfsStagingMu.Unlock()
	t.Cleanup(func() {
		srv.lfsStagingMu.Lock()
		srv.lfsRepoStagingOps, srv.lfsStagingOps = nil, 0
		srv.lfsStagingMu.Unlock()
	})

	// Same repository: denied at its share.
	content := []byte("same-repo parked slot victim")
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content),
		"basic:"+tok, string(content))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("share-exhausted same-repo PUT status = %d, want 429", w.Code)
	}
	// A DIFFERENT repository still reserves: the share isolates.
	if !srv.reserveLFSStaging("repo_other", 0) {
		t.Fatal("unrelated repository starved by another repository's slot share")
	}
	srv.releaseLFSStaging("repo_other", 0)
}

// TestLFSStaging_RepoCoalitionCannotExhaustNode (Codex round 37,
// security): tokens carry no caller identity beyond the repo claim
// (auth.SubjectLiteral is one fixed literal for every vendor SDK), so a
// writer holding git:write tokens for several repositories could park
// bodyless PUTs whose fixed per-repo shares SUM to the node cap
// (8 × 32 = 256) and starve every other repository's uploads and
// adoptions. The allowance must shrink with pool occupancy so a small
// coalition equilibrates well below the cap: a greedy 8-repo coalition
// runs until every member is denied, and an unrelated repository must
// still reserve.
func TestLFSStaging_RepoCoalitionCannotExhaustNode(t *testing.T) {
	srv := &Server{}
	for {
		progress := false
		for i := range 8 {
			if srv.reserveLFSStaging(fmt.Sprintf("coalition_%d", i), 0) {
				progress = true
			}
		}
		if !progress {
			break // every coalition member is at its allowance
		}
	}
	if !srv.reserveLFSStaging("repo_victim", 0) {
		t.Fatal("an 8-repo coalition exhausted the node-wide LFS staging pool")
	}
}

// TestLFSStaging_RepoByteShareIsolatesRepos (Codex round 38): op-slot
// shares scale with free capacity, but staged BYTES were only counted
// globally — one repository's writer could transfer LFSStagingBytes of
// real data (invalid digests make it repeatable and quota-free) and drip
// within the reserved chunks until the absolute deadline, 429ing every
// other repository's uploads. Byte allowances now scale with the free
// budget (max(chunk, free/lfsRepoStagingByteShareDiv)): a greedy repo
// stalls at the B/(div+1) fixed point — which is also the single-object
// capability the 413 pre-check advertises — and a victim repository must
// still grow a chunk afterwards.
func TestLFSStaging_RepoByteShareIsolatesRepos(t *testing.T) {
	const budget = int64(30) << 20 // 30 MiB with the default 8 MiB chunk
	srv := &Server{LFSStagingBytes: budget}
	if !srv.reserveLFSStaging("repo_greedy", 0) {
		t.Fatal("greedy op slot")
	}
	var held int64
	for held < budget {
		if !srv.growLFSStaging("repo_greedy", 1<<20) {
			break
		}
		held += 1 << 20
	}
	if held >= budget {
		t.Fatalf("one repository grew its staging hold to the whole %d-byte node budget", budget)
	}
	// Large-object capability floor: the greedy writer must reach the
	// documented B/(div+1) fixed point (chunk-granular), not stall early.
	if want := budget/(lfsRepoStagingByteShareDiv+1) - lfsStagingChunk; held < want {
		t.Fatalf("solo staging hold stalled at %d bytes, want at least %d", held, want)
	}
	// A victim repository still reserves its op slot and grows a chunk.
	if !srv.reserveLFSStaging("repo_victim", 0) {
		t.Fatal("victim op slot starved")
	}
	if !srv.growLFSStaging("repo_victim", lfsStagingChunk) {
		t.Fatal("one repository's staging hold starved another repository's byte growth")
	}
	srv.releaseLFSStaging("repo_victim", lfsStagingChunk)
	srv.releaseLFSStaging("repo_greedy", held)
}

// TestLFSVerify_RepoCoalitionCannotExhaustPool (Codex round 38,
// security): fixed 8-slot verification shares summed to the 32-slot pool,
// so git:write tokens for four repositories could park long adoptions and
// 429 every other repository's legacy uploads on an upgrade node. Like
// the staging pools, the allowance must shrink with free capacity: a
// greedy 4-repo coalition runs to denial and an unrelated repository must
// still reserve a verification buffer.
func TestLFSVerify_RepoCoalitionCannotExhaustPool(t *testing.T) {
	srv := &Server{}
	for {
		progress := false
		for i := range 4 {
			if srv.reserveLFSVerifyBuffer(fmt.Sprintf("coalition_%d", i)) {
				progress = true
			}
		}
		if !progress {
			break
		}
	}
	if !srv.reserveLFSVerifyBuffer("repo_victim") {
		t.Fatal("a 4-repo coalition exhausted the node-wide LFS verification pool")
	}
}

// wedgedStatBlob blocks Stat until the caller's context expires — the
// wedged-backend scenario. When after > 0, only the Nth and later Stat
// calls wedge (earlier ones pass through).
type wedgedStatBlob struct {
	blob.BlobStore
	after int
	calls int
}

func (b *wedgedStatBlob) Stat(ctx context.Context, key string) (int64, error) {
	b.calls++
	if b.after > 0 && b.calls < b.after {
		return b.BlobStore.Stat(ctx, key)
	}
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestLFSAdoption_PostRegistrationRecheckIsBounded (Codex round 29): the
// post-adoption existence recheck sits outside the verification deadline
// and ran on the raw request context — a wedged backend pinned the per-key
// and op slots for as long as the batch client stayed connected. It now
// runs under lfsProbeTimeout and fails closed (the committed registration
// is idempotent; the retry lands on the adopted path), returning the slots
// within a bounded window.
func TestLFSAdoption_PostRegistrationRecheckIsBounded(t *testing.T) {
	oldTimeout := lfsProbeTimeout
	lfsProbeTimeout = 50 * time.Millisecond
	t.Cleanup(func() { lfsProbeTimeout = oldTimeout })

	srv, _, mint := newLFSQuotaServer(t, 0, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	legacy := []byte("legacy blob with a wedged recheck")
	oid := sha256Hex(legacy)
	if err := srv.Blob.UploadFile(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	// Stat calls on the adoption path (rounds 78+80): 1 = batch legacy
	// probe, 2 = the verify-target canonical probe; 3 = the
	// post-registration recheck, which wedges. (Verification and the
	// publish sandwich go through StatFingerprint — round 77 — which
	// this wrapper passes through.)
	srv.Blob = &wedgedStatBlob{BlobStore: srv.Blob, after: 3}

	start := time.Now()
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("upload", oid, int64(len(legacy)))))
	if resp.Objects[0].Error == nil {
		t.Fatalf("wedged-recheck adoption reported success: %+v", resp.Objects[0])
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("wedged recheck took %v — deadline not applied", elapsed)
	}
	srv.lfsStagingMu.Lock()
	ops := srv.lfsStagingOps
	srv.lfsStagingMu.Unlock()
	if ops != 0 {
		t.Fatalf("op slots after wedged recheck = %d, want 0 (slot leaked)", ops)
	}
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
	srv.Blob = &wedgedStatBlob{BlobStore: srv.Blob}

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

// readCancelingBlob cancels the request context on the first ReadRange —
// the client disconnecting mid-verification — and surfaces the passed
// context's error if it propagated the cancellation.
type readCancelingBlob struct {
	blob.BlobStore
	cancel context.CancelFunc
}

func (b readCancelingBlob) ReadRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b.cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.BlobStore.ReadRange(ctx, key, off, n)
}

// TestLFSAdoption_CompletesAfterClientCancellation (Codex round 31,
// security): adoption is server-side repair work — a client canceling
// mid-verification must not abort the ranged hash before registration, or
// the blob stays rowless and every next tiny batch restarts the whole
// hash at offset zero (repeatable backend reads over large legacy blobs).
// Once started, adoption completes under its own lease.
func TestLFSAdoption_CompletesAfterClientCancellation(t *testing.T) {
	srv, st, _ := newLFSQuotaServer(t, 0, time.Hour)
	legacy := []byte("large legacy blob the client abandons mid-hash")
	oid := sha256Hex(legacy)
	key := blob.LFSKey(lfsTestRepoID, oid)
	if err := srv.Blob.UploadFile(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid), writeTempFile(t, legacy)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.Blob = readCancelingBlob{BlobStore: srv.Blob, cancel: cancel}

	if err := srv.adoptLFSObject(ctx, lfsTestRepoID, key, oid); err != nil {
		t.Fatalf("adoption aborted with the canceling client: %v", err)
	}
	if has, err := st.HasLFSObject(context.Background(), lfsTestRepoID, key); err != nil || !has {
		t.Fatalf("adoption did not register after client cancellation: %v, %v", has, err)
	}
}

// failingUploadBlob fails every UploadFile — the aborted-transfer /
// wedged-publication clean-failure path.
type failingUploadBlob struct {
	blob.BlobStore
}

func (b failingUploadBlob) UploadFile(context.Context, string, string) error {
	return fmt.Errorf("upload aborted")
}

// TestLFSPut_CleanFailureRetiresReservation (Codex security round on
// round 30): an aborted PUT whose publication cleanly failed must retire
// its reservation attempt instead of leaving the row until the TTL —
// sequential aborted tiny PUTs (~3 bytes each) otherwise accumulate
// maxActiveReservations rows from ~30 KB of bodies and wedge ALL of the
// repository's git/REST/LFS growth for an hour.
func TestLFSPut_CleanFailureRetiresReservation(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	srv.Blob = failingUploadBlob{BlobStore: srv.Blob}

	for i := range 3 {
		content := []byte(fmt.Sprintf("tiny-%d", i))
		w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content),
			"basic:"+tok, string(content))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("aborted PUT %d status = %d, want 500", i, w.Code)
		}
	}
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != 0 {
		t.Fatalf("aborted PUTs left %d reserved bytes pinned to the TTL, want 0 (attempts retired)", ls.ReservedBytes)
	}
}

// commitThenFailBlob commits the upload and THEN reports failure — the
// ambiguous outcome where object storage made the bytes durable but the
// response was lost.
type commitThenFailBlob struct {
	blob.BlobStore
}

func (b commitThenFailBlob) UploadFile(ctx context.Context, key, path string) error {
	if err := b.BlobStore.UploadFile(ctx, key, path); err != nil {
		return err
	}
	return fmt.Errorf("response lost after commit")
}

// TestLFSPut_AmbiguousUploadFailurePreservesReservation (Codex round 33):
// an UploadFile error can be ambiguous — the bytes durable, the response
// lost. Retiring the reservation then would leave a rowless durable blob
// unaccounted until the floor cache refreshes, letting a distinct writer
// consume the freed headroom and later adoption push durable usage over
// quota. Retirement requires EVIDENCE of non-publication (the blob
// provably absent); ambiguous outcomes leave the row to settlement or TTL.
func TestLFSPut_AmbiguousUploadFailurePreservesReservation(t *testing.T) {
	srv, st, mint := newLFSQuotaServer(t, 1<<20, time.Hour)
	tok := mint(auth.ScopeGitWrite)
	srv.Blob = commitThenFailBlob{BlobStore: srv.Blob}

	content := bytes.Repeat([]byte("a"), 40)
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+sha256Hex(content),
		"basic:"+tok, string(content))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("ambiguous PUT status = %d, want 500", w.Code)
	}
	// The bytes are durable and rowless: the reservation must keep
	// covering them until settlement or TTL.
	if ls := ledgerState(t, st, time.Hour); ls.ReservedBytes != 40 {
		t.Fatalf("ambiguous failure left %d reserved bytes, want 40 (row preserved)", ls.ReservedBytes)
	}
}

// TestLFSPut_WriterVerifyBoundedByLease (Codex round 30): the
// post-registration writer-verify must stay within the publication lease —
// blob.EnsurePublished's own size-scaled detached allowance (~175 minutes
// at 10 GiB) would hold the op slot, temp file, and full staging
// reservation far past it, letting one wedged repair monopolize the node
// budget. A wedged verify Stat now fails the request within the lease
// with every slot reclaimed.
func TestLFSPut_WriterVerifyBoundedByLease(t *testing.T) {
	srv, _, mint := newLFSQuotaServer(t, 1<<20, 2*time.Second) // lease ≈ 1.8s
	tok := mint(auth.ScopeGitWrite)
	content := []byte("verify-wedge object")
	oid := sha256Hex(content)
	// Calls: 1–2 = the dedup probes of both spellings (round 78 —
	// NotFound passes through); 3 = the post-registration writer-verify,
	// which wedges.
	srv.Blob = &wedgedStatBlob{BlobStore: srv.Blob, after: 3}

	start := time.Now()
	w := doLFS(t, srv, http.MethodPut, "/"+lfsTestRepoURL+".git/info/lfs/objects/"+oid,
		"basic:"+tok, string(content))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("wedged-verify PUT status = %d, body %s (want 500)", w.Code, w.Body)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("wedged writer-verify took %v — lease not applied", elapsed)
	}
	srv.lfsStagingMu.Lock()
	ops, inflight := srv.lfsStagingOps, srv.lfsStagingInFlight
	srv.lfsStagingMu.Unlock()
	if ops != 0 || inflight != 0 {
		t.Fatalf("staging after wedged verify = ops %d, bytes %d — slots leaked", ops, inflight)
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
	srv.LFSStagingBytes = 300                                     // per-repo share (round 55): 100 — the drip stays in-share
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
	// A legacy presigned PUT stored garbage under the oid's LEGACY key
	// (the only spelling those URLs can address — round 78).
	if err := srv.Blob.UploadFile(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid),
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
		t.Fatal("mismatched canonical copy still stored")
	}
	if _, err := srv.Blob.Stat(context.Background(), blob.LFSLegacyKey(lfsTestRepoID, oid)); err == nil {
		t.Fatal("mismatched legacy source still stored — the next touch would re-copy the same garbage")
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
