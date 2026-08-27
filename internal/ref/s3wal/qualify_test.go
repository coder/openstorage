// Store-agnostic S3 qualification harness (issue #133, plan §4 Phase 1a
// step 1): a neutral, endpoint-parameterized test of exactly the S3
// surfaces the manifest-WAL design depends on — conditional PUT fidelity
// (the §2.6 probe), gocloud writer behavior at small and multipart sizes,
// IfNotExist duplicate rejection, ListObjectsV2 pagination/ordering
// including the §2.9 member-contiguity prefix families, presigned GET, and
// path-style + SigV4 bootstrap.
//
// Lanes: SeaweedFS (the decided self-hosted store, §5-R7 — REQUIRED to
// pass), the legacy MinIO CI pin (RECORD mode: non-compliance is a finding
// logged as a structured verdict, never a test failure), and an external
// endpoint gated on CODESTORAGE_TEST_S3WAL_URL (required to pass when
// opted in). Deterministic stub-server tests prove the probe refuses
// non-compliant stores and pin the §2.3 CAS error taxonomy without Docker.
//
// Every verdict is logged as `VERDICT store=… surface=… verdict=… detail=…`
// so a lane transcript is the raw material for the §5-R7 qualification
// matrix (docs/plans/s3wal-phase1a-results.md).
package s3wal_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	gcblob "gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"

	"github.com/coder/code-storage/internal/ref/s3wal"

	"github.com/coder/code-storage/internal/ref/s3wal/s3waltest"
)

// EnvTestS3WALURL names an external S3 endpoint the qualification lane runs
// against — see s3waltest.EnvTestS3WALURL. Unset skips the lane.
const EnvTestS3WALURL = s3waltest.EnvTestS3WALURL

// Store image pins live in s3waltest (SeaweedFSImage / MinIOLegacyImage):
// SeaweedFS is the self-hosted store of record (plan §5-R7); the MinIO pin
// is the repo's legacy CI image, kept in the matrix only to document its
// conditional-write behavior empirically.

// ---------------------------------------------------------------------------
// {seq, nonce} payload codec — the Phase 1a stand-in for the Phase 1b
// manifest: every CAS body carries a strictly monotonic seq plus a fresh
// random nonce so distinct logical versions are byte-distinguishable and
// the contention detector can prove exactly-once winners.

type casPayload struct {
	Seq   int64  `json:"seq"`
	Nonce string `json:"nonce"`
	Fill  string `json:"fill,omitempty"`
}

func newNonce(tb testing.TB) string {
	tb.Helper()
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		tb.Fatalf("nonce: %v", err)
	}
	return hex.EncodeToString(buf)
}

// encodePayloadNonce marshals a {seq, nonce} payload padded to roughly size
// bytes (never below the bare encoding).
func encodePayloadNonce(tb testing.TB, seq int64, nonce string, size int) []byte {
	tb.Helper()
	p := casPayload{Seq: seq, Nonce: nonce}
	base, err := json.Marshal(p)
	if err != nil {
		tb.Fatalf("marshal payload: %v", err)
	}
	if pad := size - len(base) - len(`,"fill":""`); pad > 0 {
		p.Fill = strings.Repeat("x", pad)
	}
	out, err := json.Marshal(p)
	if err != nil {
		tb.Fatalf("marshal payload: %v", err)
	}
	return out
}

func encodePayload(tb testing.TB, seq int64, size int) []byte {
	tb.Helper()
	return encodePayloadNonce(tb, seq, newNonce(tb), size)
}

func decodePayload(body []byte) (casPayload, bool) {
	var p casPayload
	if err := json.Unmarshal(body, &p); err != nil || p.Nonce == "" {
		return casPayload{}, false
	}
	return p, true
}

// decodeSeq is the Client.DecodeSeq hook for {seq, nonce} payloads: it arms
// the §2.3 read-side seq-monotonicity assert in every test/bench client.
func decodeSeq(body []byte) (int64, bool) {
	p, ok := decodePayload(body)
	return p.Seq, ok
}

// randomHexTB returns n crypto-random bytes hex-encoded so keys stay unique
// across reruns against a shared long-lived bucket.
func randomHexTB(tb testing.TB, n int) string {
	tb.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		tb.Fatal(err)
	}
	return hex.EncodeToString(buf)
}

// ---------------------------------------------------------------------------
// Store lifecycles: containers + client construction. The concrete
// fixtures live in the s3waltest subpackage (shared with the bench harness
// and package ref's conformance lane — s3waltest never imports s3wal, so
// these internal tests may import it); the thin wrappers below keep the
// original call sites unchanged.

func startSeaweedFS(tb testing.TB) string { return s3waltest.StartSeaweedFS(tb) }

func startMinIOLegacy(tb testing.TB) (endpoint, user, pass string) {
	return s3waltest.StartMinIOLegacy(tb)
}

func rawS3Client(tb testing.TB, endpoint, region, accessKey, secretKey string) *s3.Client {
	tb.Helper()
	return s3waltest.RawS3Client(endpoint, region, accessKey, secretKey)
}

func ensureBucket(tb testing.TB, client *s3.Client, bucket string) {
	tb.Helper()
	s3waltest.MustEnsureBucket(tb, client, bucket)
}

func openExternalS3(tb testing.TB, blobURL string) (*s3.Client, string) {
	tb.Helper()
	return s3waltest.OpenExternalS3(tb, blobURL)
}

func deleteAllPrefix(tb testing.TB, client *s3.Client, bucket, prefix string) {
	tb.Helper()
	assertf(prefix != "", "deleteAllPrefix: empty prefix would delete the whole bucket")
	s3waltest.DeleteAllPrefix(tb, client, bucket, prefix)
}

// ---------------------------------------------------------------------------
// Qualification environment, verdict vocabulary, and the surface runner.

// Verdict vocabulary (§5-R7 matrix): pass | rejects (fails loudly with a
// wrong/coarse error) | ignores (silently violates the condition — the
// corruption class) | mishandles (other behavioral divergence) | error
// (call failed in a way that prevents a verdict) | not-reached.
const (
	verdictPass       = "pass"
	verdictRejects    = "rejects"
	verdictIgnores    = "ignores"
	verdictMishandles = "mishandles"
	verdictError      = "error"
	verdictNotReached = "not-reached"
)

type verdict struct {
	surface string
	class   string
	detail  string
}

func vPass(surface string) verdict { return verdict{surface, verdictPass, ""} }

// rejectVerdict classifies an unexpected operation error for the matrix.
// verdictRejects is reserved for DEFINITIVE store responses proving the
// store rejected the operation: a Client sentinel minted from a specific
// store status (412/409-exhausted/404/304) or an SDK error carrying a real
// 4xx HTTP response. Everything else — s3wal.AmbiguousError (post-send
// 5xx/timeout: the write may have landed), raw 5xx responses, and
// transport/timeout failures that never produced a store verdict — is
// verdictError: the surface could not establish how the store behaves, so
// the lane fails in BOTH require and record modes instead of recording a
// behavioral finding it never observed.
func rejectVerdict(err error) string {
	assertf(err != nil, "rejectVerdict: nil error")
	var amb *s3wal.AmbiguousError
	if errors.As(err, &amb) {
		return verdictError
	}
	for _, definitive := range []error{s3wal.ErrExists, s3wal.ErrLostRace, s3wal.ErrCASContention, s3wal.ErrNotExist, s3wal.ErrNotModified} {
		if errors.Is(err, definitive) {
			return verdictRejects
		}
	}
	if status, code := s3wal.ErrorStatus(err); status >= 400 && status < 500 {
		// A missing bucket is a bootstrap/infrastructure failure: the
		// store answered definitively, but about the environment, not the
		// probed operation — never a recordable behavioral finding.
		if code == "NoSuchBucket" {
			return verdictError
		}
		return verdictRejects
	}
	return verdictError
}

type qualEnv struct {
	label    string
	bucket   string
	prefix   string // key namespace; "" on disposable containers
	probeKey string // probe scratch-key PREFIX (s3wal.ProbeAtKey appends a per-invocation nonce)
	raw      *s3.Client
	cas      *s3wal.Client
	gcb      *gcblob.Bucket
	presign  *s3.PresignClient
}

func newQualEnv(t *testing.T, label string, raw *s3.Client, bucket, prefix, probeKey string) *qualEnv {
	t.Helper()
	gcb, err := s3blob.OpenBucket(context.Background(), raw, bucket, nil)
	if err != nil {
		t.Fatalf("open gocloud bucket: %v", err)
	}
	t.Cleanup(func() { _ = gcb.Close() })
	cas := s3wal.New(raw, bucket)
	cas.DecodeSeq = decodeSeq
	return &qualEnv{
		label:    label,
		bucket:   bucket,
		prefix:   prefix,
		probeKey: probeKey,
		raw:      raw,
		cas:      cas,
		gcb:      gcb,
		presign:  s3.NewPresignClient(raw),
	}
}

// runQualification executes every surface and logs one structured VERDICT
// line per matrix row. require=true (SeaweedFS, external endpoint) turns
// any non-pass verdict into a test failure; require=false (legacy MinIO)
// records behavioral findings (ignores/mishandles/rejects/not-reached)
// only — the store's behavior is the deliverable, not a gate. verdictError
// fails the test in BOTH modes: an error means the surface could not
// produce a verdict at all (failed seed, listing, presigned request, …),
// never a recordable behavioral finding — passing would green-light an
// incomplete qualification run.
func runQualification(t *testing.T, e *qualEnv, require bool) {
	t.Helper()
	surfaces := []struct {
		name string
		run  func(t *testing.T, e *qualEnv) []verdict
	}{
		{"raw-roundtrip", surfaceRawRoundTrip},
		{"gocloud-write-small", surfaceGocloudSmall},
		{"gocloud-write-multipart", surfaceGocloudMultipart},
		{"gocloud-ifnotexist", surfaceGocloudIfNotExist},
		{"conditional-create", surfaceConditionalCreate},
		{"conditional-advance", surfaceConditionalAdvance},
		{"get-if-none-match", surfaceGetIfNoneMatch},
		{"probe", surfaceProbe},
		{"list", surfaceList},
		{"member-contiguity", surfaceMemberContiguity},
		{"presigned-get", surfacePresignedGet},
		{"bucket-bootstrap", surfaceBucketBootstrap},
	}
	for _, s := range surfaces {
		for _, v := range s.run(t, e) {
			t.Logf("VERDICT store=%s surface=%s verdict=%s detail=%q", e.label, v.surface, v.class, v.detail)
			switch {
			case v.class == verdictError:
				t.Errorf("store %s surface %s could not produce a verdict: %s — infrastructure/harness error, fails in both require and record modes", e.label, v.surface, v.detail)
			case require && v.class != verdictPass:
				t.Errorf("required store %s failed surface %s: %s (%s)", e.label, v.surface, v.class, v.detail)
			}
		}
	}
}

// surfaceRawRoundTrip: PutObject → GetObject → HeadObject → DeleteObject →
// 404 on the raw SDK client.
func surfaceRawRoundTrip(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	ctx := context.Background()
	const name = "raw-roundtrip"
	key := e.prefix + "roundtrip-" + randomHexTB(t, 4)
	content := make([]byte, 1024)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	if _, err := e.raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(e.bucket), Key: aws.String(key), Body: bytes.NewReader(content),
	}); err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("PutObject: %v", err)}}
	}
	got, err := e.raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(e.bucket), Key: aws.String(key)})
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("GetObject: %v", err)}}
	}
	body, err := io.ReadAll(got.Body)
	_ = got.Body.Close()
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("read body: %v", err)}}
	}
	if !bytes.Equal(body, content) {
		return []verdict{{name, verdictMishandles, fmt.Sprintf("GetObject returned %d bytes, want %d identical", len(body), len(content))}}
	}
	head, err := e.raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(e.bucket), Key: aws.String(key)})
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("HeadObject: %v", err)}}
	}
	if aws.ToInt64(head.ContentLength) != int64(len(content)) {
		return []verdict{{name, verdictMishandles, fmt.Sprintf("HeadObject ContentLength = %d, want %d", aws.ToInt64(head.ContentLength), len(content))}}
	}
	if _, err := e.raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(e.bucket), Key: aws.String(key)}); err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("DeleteObject: %v", err)}}
	}
	switch _, err := e.cas.Get(ctx, key); {
	case errors.Is(err, s3wal.ErrNotExist):
		// The delete took effect.
	case err == nil:
		return []verdict{{name, verdictMishandles, "GET after DELETE served the object, want 404/s3wal.ErrNotExist"}}
	default:
		return []verdict{{name, rejectVerdict(err), fmt.Sprintf("GET after DELETE = %v, want 404/s3wal.ErrNotExist", err)}}
	}
	return []verdict{vPass(name)}
}

// gocloudWriteReadBack writes content through the gocloud writer and
// verifies readback size + SHA-256 digest (never assuming multipart ETag ==
// MD5). The stored object's raw-SDK HeadObject ETag is returned alongside
// the verdicts ("" on failure) so callers can record path evidence such as
// the multipart part-count suffix.
func gocloudWriteReadBack(t *testing.T, e *qualEnv, name, key string, content []byte, opts *gcblob.WriterOptions) ([]verdict, string) {
	t.Helper()
	ctx := context.Background()
	w, err := e.gcb.NewWriter(ctx, key, opts)
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("NewWriter: %v", err)}}, ""
	}
	if _, err := w.Write(content); err != nil {
		_ = w.Close()
		return []verdict{{name, rejectVerdict(err), fmt.Sprintf("Write: %v", err)}}, ""
	}
	if err := w.Close(); err != nil {
		return []verdict{{name, rejectVerdict(err), fmt.Sprintf("Close: %v", err)}}, ""
	}
	back, err := e.gcb.ReadAll(ctx, key)
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("ReadAll: %v", err)}}, ""
	}
	if len(back) != len(content) || sha256.Sum256(back) != sha256.Sum256(content) {
		return []verdict{{name, verdictMishandles, fmt.Sprintf("readback %d bytes, want %d with identical SHA-256", len(back), len(content))}}, ""
	}
	head, err := e.raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(e.bucket), Key: aws.String(key)})
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("HeadObject for ETag evidence: %v", err)}}, ""
	}
	if err := e.gcb.Delete(ctx, key); err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("Delete: %v", err)}}, ""
	}
	return []verdict{vPass(name)}, aws.ToString(head.ETag)
}

func surfaceGocloudSmall(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	content := make([]byte, 1024)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	vs, _ := gocloudWriteReadBack(t, e, "gocloud-write-small", e.prefix+"gocloud-small-"+randomHexTB(t, 4), content, nil)
	return vs
}

// surfaceGocloudMultipart forces the multipart path deterministically. The
// vendored transfer manager takes the multipart path only when the body
// reaches its FIXED 16 MiB MultipartUploadThreshold — independent of the
// part size, and not configurable through gocloud (s3blob's
// WriterOptions.BufferSize sets only PartSizeBytes) — so the body must
// exceed 16 MiB: 20 MiB at 5 MiB parts is exactly 4 parts by construction.
// (A previous 12 MiB body sat under the threshold and was silently
// uploaded as one PutObject — the surface passed without exercising
// multipart at all.) The stored ETag's shape is recorded as store-side
// evidence of which path ran; the hard assertions remain readback size +
// SHA-256.
func surfaceGocloudMultipart(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	content := make([]byte, 20<<20)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	vs, etag := gocloudWriteReadBack(t, e, "gocloud-write-multipart", e.prefix+"gocloud-multipart-"+randomHexTB(t, 4),
		content, &gcblob.WriterOptions{BufferSize: 5 << 20})
	for i := range vs {
		if vs[i].class == verdictPass {
			vs[i].detail = "forced multipart: 20 MiB body above the transfer manager's fixed 16 MiB threshold at 5 MiB part size (4 parts); " +
				multipartETagEvidence(etag)
		}
	}
	return vs
}

// multipartETagEvidence describes the stored ETag's shape: S3-compatible
// stores mark multipart-assembled objects with a `-<partCount>` ETag
// suffix. A store may legitimately use another shape, so the observed form
// is recorded as evidence rather than hard-failed.
func multipartETagEvidence(etag string) string {
	if m := regexp.MustCompile(`-([0-9]+)"?$`).FindStringSubmatch(etag); m != nil {
		return fmt.Sprintf("stored ETag %s carries the multipart part-count suffix (%s parts)", etag, m[1])
	}
	return fmt.Sprintf("stored ETag %s has no multipart part-count suffix (store-specific shape)", etag)
}

// surfaceGocloudIfNotExist: the gocloud IfNotExist duplicate-write must be
// rejected with FailedPrecondition — acceptance means the store ignores
// If-None-Match: * on the gocloud write path.
func surfaceGocloudIfNotExist(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	ctx := context.Background()
	const name = "gocloud-ifnotexist"
	key := e.prefix + "gocloud-ifnotexist-" + randomHexTB(t, 4)
	if err := e.gcb.WriteAll(ctx, key, []byte("first write"), nil); err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("seed write: %v", err)}}
	}
	defer func() { _ = e.gcb.Delete(ctx, key) }()
	err := e.gcb.WriteAll(ctx, key, []byte("duplicate write"), &gcblob.WriterOptions{IfNotExist: true})
	switch {
	case err == nil:
		return []verdict{{name, verdictIgnores, "store accepted an IfNotExist duplicate write (silent-corruption class)"}}
	case gcerrors.Code(err) == gcerrors.FailedPrecondition:
		return []verdict{vPass(name)}
	default:
		// The surface qualifies the STORE, so the wire evidence is
		// authoritative: a smithy 412 PreconditionFailed is a correct
		// rejection even when gocloud's gcerrors mapping misfires.
		// (s3blob's ErrorCode treats any operation error whose text
		// contains "301" as a NoSuchBucket redirect, so a store request
		// ID containing "301" mismaps the 412 to NotFound — a client-side
		// gocloud quirk recorded in the results doc, not store behavior.)
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "PreconditionFailed" {
			return []verdict{{name, verdictPass, fmt.Sprintf(
				"store rejected with 412 PreconditionFailed; gocloud gcerrors mapped it to %v (s3blob \"301\" redirect heuristic)", gcerrors.Code(err))}}
		}
		return []verdict{{name, rejectVerdict(err), fmt.Sprintf("want FailedPrecondition, got: %v", err)}}
	}
}

// surfaceConditionalCreate: raw-SDK If-None-Match: * create on a fresh key
// must succeed; the duplicate must fail 412 (s3wal.ErrExists).
func surfaceConditionalCreate(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	ctx := context.Background()
	key := e.prefix + "cond-create-" + randomHexTB(t, 4)
	if _, err := e.cas.Create(ctx, key, encodePayload(t, 1, 64)); err != nil {
		return []verdict{
			{"conditional-create-fresh", rejectVerdict(err), fmt.Sprintf("create on a fresh key failed: %v", err)},
			{"conditional-create-duplicate", verdictNotReached, ""},
		}
	}
	defer func() { _ = e.cas.Delete(ctx, key) }()
	vs := []verdict{vPass("conditional-create-fresh")}
	switch _, err := e.cas.Create(ctx, key, encodePayload(t, 1, 64)); {
	case err == nil:
		vs = append(vs, verdict{"conditional-create-duplicate", verdictIgnores, "store accepted a duplicate If-None-Match: * create (silent-corruption class)"})
	case errors.Is(err, s3wal.ErrExists):
		vs = append(vs, vPass("conditional-create-duplicate"))
	default:
		vs = append(vs, verdict{"conditional-create-duplicate", rejectVerdict(err), fmt.Sprintf("want 412/s3wal.ErrExists, got: %v", err)})
	}
	return vs
}

// surfaceConditionalAdvance: matched If-Match must succeed with a NEW ETag;
// a stale If-Match must fail 412 (s3wal.ErrLostRace).
func surfaceConditionalAdvance(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	ctx := context.Background()
	key := e.prefix + "cond-advance-" + randomHexTB(t, 4)
	etag1, err := e.cas.Create(ctx, key, encodePayload(t, 1, 64))
	if err != nil {
		return []verdict{
			{"conditional-advance-matched", verdictError, fmt.Sprintf("seed create failed: %v", err)},
			{"conditional-advance-stale", verdictNotReached, ""},
		}
	}
	defer func() { _ = e.cas.Delete(ctx, key) }()
	etag2, err := e.cas.Advance(ctx, key, encodePayload(t, 2, 64), etag1)
	if err != nil {
		return []verdict{
			{"conditional-advance-matched", rejectVerdict(err), fmt.Sprintf("advance with the current ETag failed: %v", err)},
			{"conditional-advance-stale", verdictNotReached, ""},
		}
	}
	vs := []verdict{}
	if etag2 == etag1 {
		vs = append(vs, verdict{"conditional-advance-matched", verdictMishandles, fmt.Sprintf("store returned the same ETag %q for different bytes", etag2)})
	} else {
		vs = append(vs, vPass("conditional-advance-matched"))
	}
	switch _, err := e.cas.Advance(ctx, key, encodePayload(t, 3, 64), etag1); {
	case err == nil:
		vs = append(vs, verdict{"conditional-advance-stale", verdictIgnores, "store accepted an advance conditioned on a superseded ETag (silent-corruption class)"})
	case errors.Is(err, s3wal.ErrLostRace):
		vs = append(vs, vPass("conditional-advance-stale"))
	default:
		vs = append(vs, verdict{"conditional-advance-stale", rejectVerdict(err), fmt.Sprintf("want 412/s3wal.ErrLostRace, got: %v", err)})
	}
	return vs
}

// surfaceGetIfNoneMatch: the §2.3 generation-poll path — a matching ETag
// must yield 304 without a body; a non-matching ETag must yield the body.
func surfaceGetIfNoneMatch(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	ctx := context.Background()
	key := e.prefix + "get-inm-" + randomHexTB(t, 4)
	body := encodePayload(t, 1, 64)
	etag, err := e.cas.Create(ctx, key, body)
	if err != nil {
		return []verdict{
			{"get-304-matched", verdictError, fmt.Sprintf("seed create failed: %v", err)},
			{"get-304-changed", verdictNotReached, ""},
		}
	}
	defer func() { _ = e.cas.Delete(ctx, key) }()
	vs := []verdict{}
	switch _, err := e.cas.GetIfChanged(ctx, key, etag); {
	case errors.Is(err, s3wal.ErrNotModified):
		vs = append(vs, vPass("get-304-matched"))
	case err == nil:
		vs = append(vs, verdict{"get-304-matched", verdictIgnores, "store served the full body for a matching If-None-Match ETag (no 304 path)"})
	default:
		vs = append(vs, verdict{"get-304-matched", rejectVerdict(err), fmt.Sprintf("want 304/s3wal.ErrNotModified, got: %v", err)})
	}
	switch obj, err := e.cas.GetIfChanged(ctx, key, `"d41d8cd98f00b204e9800998ecf8427e"`); {
	case err == nil && bytes.Equal(obj.Body, body):
		vs = append(vs, vPass("get-304-changed"))
	case err == nil:
		vs = append(vs, verdict{"get-304-changed", verdictMishandles, fmt.Sprintf("store served %d bytes for a non-matching If-None-Match GET, want the original %d", len(obj.Body), len(body))})
	case errors.Is(err, s3wal.ErrNotModified):
		vs = append(vs, verdict{"get-304-changed", verdictMishandles, "store returned 304 for a NON-matching If-None-Match ETag"})
	default:
		vs = append(vs, verdict{"get-304-changed", rejectVerdict(err), fmt.Sprintf("want the body, got: %v", err)})
	}
	return vs
}

var probeStepNames = [5]string{"create", "duplicate-create-412", "matched-advance", "stale-advance-412", "racing-advances"}

// surfaceProbe runs the §2.6 boot probe and decomposes its verdict into the
// five step rows of the qualification matrix.
func surfaceProbe(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	err := s3wal.ProbeAtKey(context.Background(), e.cas, e.probeKey)
	vs := make([]verdict, 0, 5)
	stepName := func(i int) string { return fmt.Sprintf("probe-step-%d-%s", i, probeStepNames[i-1]) }
	if err == nil {
		for i := 1; i <= 5; i++ {
			vs = append(vs, vPass(stepName(i)))
		}
		return vs
	}
	var pe *s3wal.ProbeError
	if !errors.As(err, &pe) {
		return []verdict{{"probe", verdictError, err.Error()}}
	}
	classMap := map[string]string{
		s3wal.ProbeIgnored:      verdictIgnores,
		s3wal.ProbeRejected:     verdictRejects,
		s3wal.ProbeMishandled:   verdictMishandles,
		s3wal.ProbeInconclusive: verdictError,
	}
	for i := 1; i <= 5; i++ {
		switch {
		case i < pe.Step:
			vs = append(vs, vPass(stepName(i)))
		case i == pe.Step:
			vs = append(vs, verdict{stepName(i), classMap[pe.Class], pe.Diagnosis})
		default:
			vs = append(vs, verdict{stepName(i), verdictNotReached, "probe aborted at an earlier step"})
		}
	}
	return vs
}

// surfaceList: ListObjectsV2 over 1,100 keys — pagination past the 1,000
// MaxKeys default, full-listing lexicographic byte order, Prefix, and
// StartAfter.
func surfaceList(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	ctx := context.Background()
	const n = 1100
	listPrefix := e.prefix + "list-" + randomHexTB(t, 4) + "/"
	expected := make([]string, n)
	for i := range expected {
		expected[i] = fmt.Sprintf("%sk%05d", listPrefix, i)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(32)
	for _, key := range expected {
		g.Go(func() error {
			_, err := e.raw.PutObject(gctx, &s3.PutObjectInput{
				Bucket: aws.String(e.bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("list-seed")),
			})
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return []verdict{{"list-pagination", verdictError, fmt.Sprintf("seeding %d keys failed: %v", n, err)}}
	}
	t.Cleanup(func() { deleteAllPrefix(t, e.raw, e.bucket, listPrefix) })

	collect := func(in *s3.ListObjectsV2Input) ([]string, int, error) {
		var keys []string
		pages := 0
		p := s3.NewListObjectsV2Paginator(e.raw, in)
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return nil, pages, err
			}
			pages++
			for _, o := range page.Contents {
				keys = append(keys, aws.ToString(o.Key))
			}
		}
		return keys, pages, nil
	}

	vs := []verdict{}
	all, pages, err := collect(&s3.ListObjectsV2Input{Bucket: aws.String(e.bucket), Prefix: aws.String(listPrefix)})
	switch {
	case err != nil:
		vs = append(vs, verdict{"list-pagination", rejectVerdict(err), fmt.Sprintf("paginated listing failed: %v", err)},
			verdict{"list-order", verdictNotReached, ""})
	case len(all) != n || pages < 2:
		vs = append(vs, verdict{"list-pagination", verdictMishandles, fmt.Sprintf("listed %d keys over %d pages, want %d keys over ≥2 pages", len(all), pages, n)},
			verdict{"list-order", verdictNotReached, ""})
	default:
		vs = append(vs, vPass("list-pagination"))
		order := vPass("list-order")
		for i := range all {
			if all[i] != expected[i] {
				order = verdict{"list-order", verdictMishandles, fmt.Sprintf("position %d: got %q, want %q (lexicographic byte order)", i, all[i], expected[i])}
				break
			}
		}
		vs = append(vs, order)
	}

	narrow, _, err := collect(&s3.ListObjectsV2Input{Bucket: aws.String(e.bucket), Prefix: aws.String(listPrefix + "k0000")})
	switch {
	case err != nil:
		vs = append(vs, verdict{"list-prefix", rejectVerdict(err), fmt.Sprintf("Prefix listing failed: %v", err)})
	case len(narrow) != 10 || narrow[0] != expected[0] || narrow[9] != expected[9]:
		vs = append(vs, verdict{"list-prefix", verdictMishandles, fmt.Sprintf("Prefix=%sk0000 returned %d keys, want exactly k00000..k00009", listPrefix, len(narrow))})
	default:
		vs = append(vs, vPass("list-prefix"))
	}

	after, _, err := collect(&s3.ListObjectsV2Input{
		Bucket: aws.String(e.bucket), Prefix: aws.String(listPrefix), StartAfter: aws.String(expected[549]),
	})
	switch {
	case err != nil:
		vs = append(vs, verdict{"list-startafter", rejectVerdict(err), fmt.Sprintf("StartAfter listing failed: %v", err)})
	case len(after) != n-550 || after[0] != expected[550]:
		vs = append(vs, verdict{"list-startafter", verdictMishandles, fmt.Sprintf("StartAfter=%s returned %d keys starting %q, want %d starting %q", expected[549], len(after), first(after), n-550, expected[550])})
	default:
		vs = append(vs, vPass("list-startafter"))
	}
	return vs
}

func first(keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}

// surfaceMemberContiguity pins the §2.9 contiguity lemma on real listings:
// extension-free member keys `<base>` and `<base>+<suffix>` stay contiguous
// because '+' (0x2B) sorts below '-' (0x2D) and the whole repo-ID alphabet
// [0-9a-z-].
func surfaceMemberContiguity(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	ctx := context.Background()
	const name = "member-contiguity"
	fam := e.prefix + "family-" + randomHexTB(t, 4) + "/_wal/repo/"
	expected := []string{
		fam + "abc",
		fam + "abc+ephemeral",
		fam + "abc+import",
		fam + "abc-1",
		fam + "abc-1+ephemeral",
		fam + "abc0",
	}
	// Insert in reverse so listing order cannot ride insertion order.
	for i := len(expected) - 1; i >= 0; i-- {
		if _, err := e.raw.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(e.bucket), Key: aws.String(expected[i]), Body: bytes.NewReader([]byte("m")),
		}); err != nil {
			return []verdict{{name, verdictError, fmt.Sprintf("seed %s: %v", expected[i], err)}}
		}
	}
	t.Cleanup(func() { deleteAllPrefix(t, e.raw, e.bucket, fam) })
	out, err := e.raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(e.bucket), Prefix: aws.String(fam)})
	if err != nil {
		return []verdict{{name, rejectVerdict(err), fmt.Sprintf("listing failed: %v", err)}}
	}
	got := make([]string, 0, len(out.Contents))
	for _, o := range out.Contents {
		got = append(got, aws.ToString(o.Key))
	}
	if len(got) != len(expected) {
		return []verdict{{name, verdictMishandles, fmt.Sprintf("listed %d keys, want %d", len(got), len(expected))}}
	}
	for i := range expected {
		if got[i] != expected[i] {
			return []verdict{{name, verdictMishandles, fmt.Sprintf("position %d: got %q, want %q — member contiguity broken ('+'=0x2B must sort before '-'=0x2D and [0-9a-z])", i, got[i], expected[i])}}
		}
	}
	return []verdict{vPass(name)}
}

// surfacePresignedGet: a presigned GET redeemed by a plain HTTP client (no
// SDK, no credentials) must serve the exact bytes — the LFS download path.
func surfacePresignedGet(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	ctx := context.Background()
	const name = "presigned-get"
	key := e.prefix + "presign-" + randomHexTB(t, 4)
	content := make([]byte, 1024)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	if _, err := e.raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(e.bucket), Key: aws.String(key), Body: bytes.NewReader(content),
	}); err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("PutObject: %v", err)}}
	}
	defer func() { _ = e.cas.Delete(ctx, key) }()
	signed, err := e.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(e.bucket), Key: aws.String(key),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		// Presigning is a local signing operation — no store response is
		// involved, so a failure can never be store behavior.
		return []verdict{{name, verdictError, fmt.Sprintf("presign: %v", err)}}
	}
	resp, err := http.Get(signed.URL)
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("redeem presigned URL: %v", err)}}
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("read presigned body: %v", err)}}
	}
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, content) {
		return []verdict{{name, verdictMishandles, fmt.Sprintf("presigned GET = %d with %d bytes, want 200 with the original %d", resp.StatusCode, len(body), len(content))}}
	}
	return []verdict{vPass(name)}
}

// surfaceBucketBootstrap: HeadBucket health-check on the already-bootstrapped
// bucket (CreateBucket-if-emulator ran during env setup).
func surfaceBucketBootstrap(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	const name = "bucket-bootstrap"
	if _, err := e.raw.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(e.bucket)}); err != nil {
		return []verdict{{name, rejectVerdict(err), fmt.Sprintf("HeadBucket: %v", err)}}
	}
	return []verdict{vPass(name)}
}

// ---------------------------------------------------------------------------
// Lanes.

// TestQualifySeaweedFS is the require-mode lane for the decided self-hosted
// store: any non-pass verdict fails the test (a gate-stopping finding for
// plan §4 Gate 1a-A).
func TestQualifySeaweedFS(t *testing.T) {
	if testing.Short() {
		t.Skip("seaweedfs qualification needs docker")
	}
	endpoint := startSeaweedFS(t)
	raw := rawS3Client(t, endpoint, "us-east-1", "seaweedfs", "seaweedfs")
	ensureBucket(t, raw, "s3wal-qualify")
	e := newQualEnv(t, "seaweedfs-4.42", raw, "s3wal-qualify", "", s3wal.ProbeKey)
	runQualification(t, e, true)
}

// TestQualifyMinIOLegacy is the record-mode lane for the old MinIO CI pin:
// its behavior — compliant or not — is a documented finding for the §5-R7
// matrix, never a test failure. Infrastructure/harness errors (verdictError)
// still fail: they mean a surface produced no verdict, so the matrix row
// would be incomplete, not a finding.
func TestQualifyMinIOLegacy(t *testing.T) {
	if testing.Short() {
		t.Skip("minio qualification needs docker")
	}
	endpoint, user, pass := startMinIOLegacy(t)
	raw := rawS3Client(t, endpoint, "us-east-1", user, pass)
	ensureBucket(t, raw, "s3wal-qualify")
	e := newQualEnv(t, "minio-legacy-2024-01", raw, "s3wal-qualify", "", s3wal.ProbeKey)
	runQualification(t, e, false)
}

// TestQualifyExternalEndpoint runs the full require-mode surface set
// (including the 1,100-key listing) against an externally provided
// endpoint. All keys are namespaced under qualify/<hex>/ and deleted on
// cleanup; the probe runs at a prefixed key so it can never collide with a
// real deployment's _wal/probe.
func TestQualifyExternalEndpoint(t *testing.T) {
	blobURL := os.Getenv(EnvTestS3WALURL)
	if blobURL == "" {
		t.Skipf("set %s (s3://bucket?endpoint=http://127.0.0.1:8333&region=us-east-1&s3ForcePathStyle=true) plus AWS credentials to run", EnvTestS3WALURL)
	}
	raw, bucket := openExternalS3(t, blobURL)
	prefix := "qualify/" + randomHexTB(t, 8) + "/"
	t.Cleanup(func() { deleteAllPrefix(t, raw, bucket, prefix) })
	e := newQualEnv(t, "external", raw, bucket, prefix, prefix+"_wal/probe")
	runQualification(t, e, true)
}

// ---------------------------------------------------------------------------
// Contention runner + lost-update detector (shared with bench_test.go).

type seqTransition struct {
	to    int64
	nonce string
}

type contentionResult struct {
	// latencies holds one PER-PUSH SUCCESS latency sample per win: the
	// logical attempt runs from the pusher's previous win (worker start
	// for its first) until this win, INCLUDING every lost race, re-read,
	// and backoff in between — the plan §3 quantity. A lone successful
	// Advance call's duration would be CAS-attempt latency and understate
	// the real push wait under contention.
	latencies      []time.Duration
	wins           int
	lostRaces      int64
	contentionErrs int64
	ambiguous      int64
	busyRetries    int64 // SQLite lane only: definitive SQLITE_BUSY rejections retried
	maxStarve      time.Duration
	elapsed        time.Duration // window start → last worker completion (throughput denominator)
}

// §2.3 lost-race backoff for the contention runner: full-jitter
// exponential, 25 ms base → 2 s cap — the policy plan §3 cell 3 measures
// under ("under the §2.3 backoff policy"), mirroring the production mutate
// loop (s3walstore.go sleepJitter). Constants, not knobs: the bench must
// always measure the specified policy (issue #181).
const (
	contentionBackoffBase = 25 * time.Millisecond
	contentionBackoffCap  = 2 * time.Second
)

// contentionBackoffCeiling returns the full-jitter ceiling for the
// attempt'th retry within one push: min(cap, base·2^attempt). The shift is
// clamped once the cap is reached so arbitrarily long losing streaks
// cannot overflow — unlike production's 16-attempt budget, the runner
// never abandons a push (its job is to keep offering load; a pusher pinned
// at the 2 s cap is what an exhaust-and-retry client converges to anyway).
func contentionBackoffCeiling(attempt int) time.Duration {
	assertf(attempt >= 0, "contentionBackoffCeiling: attempt %d", attempt)
	const capShift = 7 // 25ms<<7 = 3.2s already exceeds the 2s cap
	return min(contentionBackoffCap, contentionBackoffBase<<uint(min(attempt, capShift)))
}

// runContentionWindow races `writers` goroutines advancing ONE object for a
// wall-clock window under the §2.3 backoff, then runs the lost-update
// detector: every winning transition seq must have exactly one recorded
// winner, the final seq must equal initial + wins, the winner set must be
// contiguous, and the final nonce must belong to the last recorded winner.
// A window containing any ambiguous outcome (possible on real S3) is
// INCONCLUSIVE and fails the run — see verifyTransitions.
//
// Loop fidelity (issue #181): every contender re-reads the object before
// EVERY advance — production pushers run read-mutate-CAS with no local
// copy to chain from — and a lost race sleeps full-jitter
// rand(0, min(2s, 25ms·2^attempt)) before its re-read, where attempt
// counts the retries within the current push and resets on a win. The
// starve metric therefore measures real §2.3 pusher wait, not
// winner-pinning at PUT wire speed.
//
// The measured window is preceded by an UNTIMED warmup on a separate
// scratch key (warmupContention): connection setup, pool fill, and
// first-request costs land before the timer starts, honoring the
// campaign's 20-op-warmup methodology. All accounting — latency samples,
// wins, lost races, and the detector's seq/winner baseline — belongs to
// the measured window only (the warmup key's transitions are never
// recorded, and the measured seed is created fresh after warmup).
func runContentionWindow(tb testing.TB, cas *s3wal.Client, key string, writers int, window time.Duration, payloadSize int) contentionResult {
	tb.Helper()
	assertf(writers > 0, "runContentionWindow: %d writers", writers)
	warmupContention(tb, cas, key+"-warmup", writers, payloadSize)
	ctx, cancel := context.WithTimeout(context.Background(), window+2*time.Minute)
	defer cancel()
	const initialSeq = int64(1)
	if _, err := cas.Create(ctx, key, encodePayload(tb, initialSeq, payloadSize)); err != nil {
		tb.Fatalf("contention: create %s: %v", key, err)
	}

	var (
		mu          sync.Mutex
		transitions []seqTransition
		res         contentionResult
	)
	start := time.Now()
	deadline := start.Add(window)
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		// Per-writer client: the read-side seq assert is session-scoped
		// (casobj.go DecodeSeq), and concurrent writers sharing one Client
		// would trip it on benign response reordering.
		wcas := s3wal.PerSessionClient(cas)
		go func() {
			defer wg.Done()
			var (
				localLat                          []time.Duration
				localTrans                        []seqTransition
				lostRaces, contentions, ambiguous int64
				maxStarve                         time.Duration
			)
			lastSuccess := start
			// Merge in a defer so a writer that errors out mid-window
			// still contributes its recorded wins: discarding them would
			// desync the detector's final-seq accounting and report a
			// phantom update that never happened.
			defer func() {
				maxStarve = max(maxStarve, time.Since(lastSuccess))
				mu.Lock()
				res.latencies = append(res.latencies, localLat...)
				transitions = append(transitions, localTrans...)
				res.lostRaces += lostRaces
				res.contentionErrs += contentions
				res.ambiguous += ambiguous
				res.maxStarve = max(res.maxStarve, maxStarve)
				mu.Unlock()
			}()
			reRead := func() (s3wal.Object, bool) {
				obj, err := wcas.Get(ctx, key)
				if err != nil {
					tb.Errorf("contention: re-read %s: %v", key, err)
					return s3wal.Object{}, false
				}
				return obj, true
			}
			// attempt counts the retries within the current push (any
			// cause, like the production mutate loop's iteration index)
			// and resets on a win; only lost races sleep on it.
			attempt := 0
			backoff := func() bool {
				timer := time.NewTimer(mrand.N(contentionBackoffCeiling(attempt)))
				defer timer.Stop()
				select {
				case <-ctx.Done():
					tb.Errorf("contention: backoff on %s: %v", key, ctx.Err())
					return false
				case <-timer.C:
					return true
				}
			}
			obj, ok := reRead()
			if !ok {
				return
			}
			for time.Now().Before(deadline) {
				p, decOK := decodePayload(obj.Body)
				if !decOK {
					tb.Errorf("contention: undecodable payload on %s", key)
					return
				}
				nonce := newNonce(tb)
				next := encodePayloadNonce(tb, p.Seq+1, nonce, payloadSize)
				// The returned ETag is deliberately discarded: chaining
				// advances from it is an optimization no production
				// pusher has (issue #181).
				_, err := wcas.Advance(ctx, key, next, obj.ETag)
				switch {
				case err == nil:
					now := time.Now()
					// Per-push success latency: previous win (or worker
					// start) → this win, spanning intervening lost races,
					// re-reads, and backoffs (see contentionResult).
					localLat = append(localLat, now.Sub(lastSuccess))
					localTrans = append(localTrans, seqTransition{to: p.Seq + 1, nonce: nonce})
					maxStarve = max(maxStarve, now.Sub(lastSuccess))
					lastSuccess = now
					attempt = 0
					// No local-copy chaining (issue #181): the next push
					// re-reads the object like every production pusher —
					// the winner holds no privileged state.
					if obj, ok = reRead(); !ok {
						return
					}
				case errors.Is(err, s3wal.ErrLostRace):
					lostRaces++
					// The window is over and the loop is about to exit:
					// starting a backoff + re-read now would be off-window
					// recovery work that cannot lead to another CAS, only
					// inflating elapsed and the deferred starve reading by
					// up to the 2 s cap (PR #184 review).
					if !time.Now().Before(deadline) {
						return
					}
					// §2.3: full-jitter backoff before the re-read +
					// re-evaluate, exactly like the production mutate loop.
					if !backoff() {
						return
					}
					attempt++
					if obj, ok = reRead(); !ok {
						return
					}
				case errors.Is(err, s3wal.ErrCASContention):
					// 409 exhaustion — the client already backed off
					// through its full internal budget.
					contentions++
					attempt++
					if obj, ok = reRead(); !ok {
						return
					}
				default:
					var amb *s3wal.AmbiguousError
					if !errors.As(err, &amb) {
						tb.Errorf("contention: advance %s: %v", key, err)
						return
					}
					ambiguous++
					attempt++
					if obj, ok = reRead(); !ok {
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	res.elapsed = time.Since(start)
	res.wins = len(transitions)

	final, err := cas.Get(ctx, key)
	if err != nil {
		tb.Fatalf("contention: final read %s: %v", key, err)
	}
	fp, ok := decodePayload(final.Body)
	if !ok {
		tb.Fatalf("contention: undecodable final payload on %s", key)
	}
	verifyTransitions(tb, initialSeq, fp.Seq, fp.Nonce, transitions, res.ambiguous)
	return res
}

// warmupContention primes the concurrent CAS path before a measured
// contention window: `writers` goroutines race a bounded number of
// advances on a separate warmup scratch key so connection setup, pool
// fill, and first-request costs happen before the measured timer starts
// (collectively ≥ benchWarmupOps operations; a lost race still exercises
// the full advance+re-read path, which is what needs warming). Warmup
// state is fully separate from the measured window: distinct key,
// throwaway per-session clients, and no accounting of any kind.
func warmupContention(tb testing.TB, cas *s3wal.Client, warmupKey string, writers int, payloadSize int) {
	tb.Helper()
	assertf(writers > 0, "warmupContention: %d writers", writers)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := cas.Create(ctx, warmupKey, encodePayload(tb, 1, payloadSize)); err != nil {
		tb.Fatalf("contention warmup: create %s: %v", warmupKey, err)
	}
	perWorker := (benchWarmupOps + writers - 1) / writers
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		wcas := s3wal.PerSessionClient(cas)
		go func() {
			defer wg.Done()
			obj, err := wcas.Get(ctx, warmupKey)
			if err != nil {
				tb.Errorf("contention warmup: get %s: %v", warmupKey, err)
				return
			}
			for range perWorker {
				p, ok := decodePayload(obj.Body)
				if !ok {
					tb.Errorf("contention warmup: undecodable payload on %s", warmupKey)
					return
				}
				next := encodePayloadNonce(tb, p.Seq+1, newNonce(tb), payloadSize)
				newETag, err := wcas.Advance(ctx, warmupKey, next, obj.ETag)
				if err == nil {
					obj = s3wal.Object{Body: next, ETag: newETag}
					continue
				}
				// Lost race, contention, or ambiguity: re-read and keep
				// going — warmup records nothing, so no accounting can be
				// corrupted.
				if obj, err = wcas.Get(ctx, warmupKey); err != nil {
					tb.Errorf("contention warmup: re-read %s: %v", warmupKey, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	_ = cas.Delete(ctx, warmupKey)
}

// verifyTransitions is the lost-update detector shared by the S3 and SQLite
// contention runners. finalNonce == "" skips the last-winner nonce check
// (the SQLite analog encodes seq directly in the sha and has no nonce).
//
// A window containing any ambiguous outcome is INCONCLUSIVE for the
// correctness verdict: an ambiguous write may have landed without a
// recorded winner, so the exactly-once checks would hold only vacuously
// and could mask a real lost update. Rather than weaken the checks, the
// detector fails such a run outright — re-run until an ambiguity-free
// window is observed. Winner uniqueness is still checked first because a
// duplicate recorded winner is a definite violation regardless of
// ambiguity. Ambiguous outcomes stay counted in contentionResult as a
// metric.
func verifyTransitions(tb testing.TB, initialSeq, finalSeq int64, finalNonce string, transitions []seqTransition, ambiguous int64) {
	tb.Helper()
	wins := int64(len(transitions))
	winners := make(map[int64]string, len(transitions))
	for _, tr := range transitions {
		if prev, dup := winners[tr.to]; dup {
			tb.Fatalf("LOST UPDATE: two recorded winners for seq %d (nonces %q and %q)", tr.to, prev, tr.nonce)
		}
		winners[tr.to] = tr.nonce
	}
	if ambiguous > 0 {
		tb.Fatalf("INCONCLUSIVE: %d ambiguous outcomes in the window — a possibly-landed write has no recorded winner, so the exactly-once checks cannot render a verdict; re-run until an ambiguity-free window is observed", ambiguous)
	}
	if finalSeq != initialSeq+wins {
		tb.Fatalf("LOST/PHANTOM UPDATE: final seq %d != initial %d + recorded wins %d", finalSeq, initialSeq, wins)
	}
	for s := initialSeq + 1; s <= finalSeq; s++ {
		if _, ok := winners[s]; !ok {
			tb.Fatalf("MISSING WINNER: no recorded winner for seq %d although the object reached %d", s, finalSeq)
		}
	}
	if finalNonce != "" {
		if nonce := winners[finalSeq]; nonce != finalNonce {
			tb.Fatalf("final nonce %q does not match the recorded last winner %q", finalNonce, nonce)
		}
	}
}

// TestContentionLostUpdateDetectorSeaweedFS is the bounded fast variant of
// the §3 sustained lost-update detector that runs in normal `go test`
// (plan §4 step 2): 8 writers hammering one manifest key for ~3 s must
// produce exactly-once winners and no lost writes.
func TestContentionLostUpdateDetectorSeaweedFS(t *testing.T) {
	if testing.Short() {
		t.Skip("contention detector needs docker")
	}
	endpoint := startSeaweedFS(t)
	raw := rawS3Client(t, endpoint, "us-east-1", "seaweedfs", "seaweedfs")
	ensureBucket(t, raw, "s3wal-detect")
	cas := s3wal.New(raw, "s3wal-detect")
	cas.DecodeSeq = decodeSeq
	res := runContentionWindow(t, cas, "_wal/repo/detect", 8, 3*time.Second, 256)
	if res.wins == 0 {
		t.Fatal("no advance succeeded during the detector window — over-rejection or livelock")
	}
	t.Logf("detector: wins=%d lostRaces=%d contention=%d ambiguous=%d maxStarve=%v",
		res.wins, res.lostRaces, res.contentionErrs, res.ambiguous, res.maxStarve)
}

// ---------------------------------------------------------------------------
// §2.3 runner-fidelity checks (issue #181): the contention runner must
// follow the production read-mutate-CAS shape it claims to measure.

// recordingStore is an HONEST in-memory path-style S3 for bucket "stub" —
// GET serves the stored body and ETag, PUT enforces If-None-Match:* /
// If-Match with versioned ETags — that records every object request in
// order. Unlike fakeStore (defect injection for probe refusal), its only
// twist is deterministic lost races: with rejectAlternate set, every ODD
// If-Match PUT on rejectKey is 412-rejected, so a single writer exercises
// the lost-race path on every other advance while the window still records
// real wins (verifyTransitions needs the final nonce to name a winner).
type recordingStore struct {
	mu              sync.Mutex
	objs            map[string]recordedObject
	next            int
	events          []storeEvent
	rejectKey       string
	rejectAlternate bool
	advancePUTs     int // If-Match PUTs seen on rejectKey (alternation counter)
}

type recordedObject struct {
	body []byte
	etag string
}

type storeEvent struct {
	method  string
	key     string
	ifMatch bool // PUT carried If-Match — a CAS advance attempt
	status  int
}

func (s *recordingStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "IncompleteBody")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/stub/")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objs == nil {
		s.objs = map[string]recordedObject{}
	}
	record := func(ifMatch bool, status int) {
		s.events = append(s.events, storeEvent{method: r.Method, key: key, ifMatch: ifMatch, status: status})
	}
	switch r.Method {
	case http.MethodPut:
		im := r.Header.Get("If-Match")
		cur, exists := s.objs[key]
		if r.Header.Get("If-None-Match") == "*" && exists {
			record(false, http.StatusPreconditionFailed)
			writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		if im != "" {
			if s.rejectAlternate && key == s.rejectKey {
				s.advancePUTs++
				if s.advancePUTs%2 == 1 {
					record(true, http.StatusPreconditionFailed)
					writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
					return
				}
			}
			if !exists || im != cur.etag {
				record(true, http.StatusPreconditionFailed)
				writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
		}
		s.next++
		obj := recordedObject{body: body, etag: fmt.Sprintf("%q", fmt.Sprintf("v%d", s.next))}
		s.objs[key] = obj
		record(im != "", http.StatusOK)
		w.Header().Set("ETag", obj.etag)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		obj, exists := s.objs[key]
		if !exists {
			record(false, http.StatusNotFound)
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		record(false, http.StatusOK)
		w.Header().Set("ETag", obj.etag)
		_, _ = w.Write(obj.body)
	case http.MethodDelete:
		delete(s.objs, key)
		record(false, http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, http.StatusNotImplemented, "NotImplemented")
	}
}

// snapshot returns a copy of the ordered request log (workers are joined
// before tests read it; the copy keeps -race honest anyway).
func (s *recordingStore) snapshot() []storeEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storeEvent(nil), s.events...)
}

// TestContentionRunnerCASDiscipline pins — deterministically, without
// Docker — that runContentionWindow measures the §2.3 loop shape it claims
// (issue #181): every contender re-reads the object before every advance
// (production pushers never chain from a local copy), and a lost race
// backs off full-jitter before its re-read instead of spinning at wire
// speed.
func TestContentionRunnerCASDiscipline(t *testing.T) {
	const key = "_wal/repo/discipline"
	newStubEnv := func(t *testing.T, store *recordingStore) *s3wal.Client {
		t.Helper()
		srv := httptest.NewServer(store)
		t.Cleanup(srv.Close)
		return stubClient(t, srv.URL)
	}

	t.Run("re-read-before-every-advance", func(t *testing.T) {
		store := &recordingStore{}
		c := newStubEnv(t, store)
		res := runContentionWindow(t, c, key, 1, 150*time.Millisecond, 64)
		if res.wins < 2 {
			t.Fatalf("wins = %d, want >= 2 (local-copy chaining is only visible across consecutive wins)", res.wins)
		}
		// Every successful advance of the measured key must be preceded
		// by a GET issued after the previous win: a winner chaining from
		// its local copy emits back-to-back OK If-Match PUTs instead.
		gets, wins := 0, 0
		for _, ev := range store.snapshot() {
			if ev.key != key || ev.status != http.StatusOK {
				continue
			}
			switch {
			case ev.method == http.MethodGet:
				gets++
			case ev.method == http.MethodPut && ev.ifMatch:
				if gets == 0 {
					t.Fatalf("advance #%d succeeded without a re-read since the previous win — the runner is chaining from its local copy", wins+1)
				}
				wins++
				gets = 0
			}
		}
		if wins != res.wins {
			t.Fatalf("request log saw %d winning advances, runner recorded %d", wins, res.wins)
		}
	})

	t.Run("lost-races-back-off", func(t *testing.T) {
		store := &recordingStore{rejectKey: key, rejectAlternate: true}
		c := newStubEnv(t, store)
		const window = 300 * time.Millisecond
		res := runContentionWindow(t, c, key, 1, window, 64)
		if res.lostRaces < 2 {
			t.Fatalf("lost races = %d, want >= 2 (the stub rejects every other advance)", res.lostRaces)
		}
		// Every lost race must sleep full-jitter with a 25 ms first-retry
		// ceiling (~12.5 ms expected — the attempt counter resets on the
		// win that follows each alternated rejection), so a 300 ms window
		// fits ~24 rejected attempts (~48 total) in expectation. The
		// bound leaves ~2.5x headroom for lucky jitter draws; the
		// backoff-free tight loop lands in the thousands on loopback and
		// cannot pass.
		attempts := 0
		for _, ev := range store.snapshot() {
			if ev.key == key && ev.method == http.MethodPut && ev.ifMatch {
				attempts++
			}
		}
		if attempts > 120 {
			t.Fatalf("%d advance attempts in a %v window — the lost-race path is not backing off per §2.3 (full jitter, 25 ms -> 2 s)", attempts, window)
		}
	})

	t.Run("backoff-ceiling-schedule", func(t *testing.T) {
		// §2.3: full-jitter exponential, 25 ms base doubling to a 2 s cap.
		want := []time.Duration{
			25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond,
			200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
			1600 * time.Millisecond, 2 * time.Second, 2 * time.Second,
		}
		for attempt, w := range want {
			if got := contentionBackoffCeiling(attempt); got != w {
				t.Errorf("ceiling(%d) = %v, want %v", attempt, got, w)
			}
		}
		// The clamp keeps the shift in range for arbitrarily long losing
		// streaks (the runner never abandons a push).
		if got := contentionBackoffCeiling(400); got != 2*time.Second {
			t.Errorf("ceiling(400) = %v, want the 2s cap", got)
		}
	})
}

// TestStoreDateWitnessSeaweedFS qualifies the §2.4-3 Date-capture
// middleware against a real store: after any operation the client must
// hold a fresh witness agreeing with the local clock (the container
// shares the host clock). Destructive verdicts FAIL CLOSED without a
// witness, so a silently broken capture would demote every genuine
// expiry fleet-wide — this is the direct gate on the seam itself.
func TestStoreDateWitnessSeaweedFS(t *testing.T) {
	if testing.Short() {
		t.Skip("store-date qualification needs docker")
	}
	endpoint := startSeaweedFS(t)
	raw := rawS3Client(t, endpoint, "us-east-1", "seaweedfs", "seaweedfs")
	ensureBucket(t, raw, "s3wal-date")
	cas := s3wal.New(raw, "s3wal-date")
	if _, _, ok := cas.StoreDate(); ok {
		t.Fatal("a fresh client must hold no witness before its first operation")
	}
	ctx := context.Background()
	if _, err := cas.Create(ctx, "_wal/repo/date-witness", []byte(`{"seq":1}`)); err != nil {
		t.Fatalf("create: %v", err)
	}
	assertWitness := func(op string) {
		t.Helper()
		date, observedAt, ok := cas.StoreDate()
		if !ok {
			t.Fatalf("no store-date witness after %s", op)
		}
		if age := time.Since(observedAt); age < 0 || age > time.Minute {
			t.Fatalf("witness observation age after %s = %v, want fresh", op, age)
		}
		if delta := time.Since(date); delta < -time.Minute || delta > time.Minute {
			t.Fatalf("store date after %s = %v, disagrees with the local clock by %v", op, date, time.Since(date))
		}
	}
	assertWitness("create")
	obj, err := cas.Get(ctx, "_wal/repo/date-witness")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertWitness("get")
	// The instance lease judges freshness by Last-Modified against the
	// witness (lease.go storeAge) and FAILS CLOSED on a zero stamp — a
	// real store must serve one agreeing with the local clock.
	if obj.LastModified.IsZero() {
		t.Fatal("get served no Last-Modified stamp")
	}
	if delta := time.Since(obj.LastModified); delta < -time.Minute || delta > time.Minute {
		t.Fatalf("Last-Modified %v disagrees with the local clock by %v", obj.LastModified, delta)
	}
}

// ---------------------------------------------------------------------------
// Deterministic stub servers: probe refusal + §2.3 taxonomy without Docker.

// writeS3Error writes a minimal S3 XML error response.
func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

// fakeStore is a minimal in-memory path-style S3 for bucket "stub" with
// deliberately injectable conditional-write defects: it can ignore
// If-None-Match: *, ignore If-Match, or return a wrong status for a stale
// advance — the misbehavior classes the probe must refuse.
type fakeStore struct {
	mu                 sync.Mutex
	objs               map[string]int // key → version
	next               int
	ignoreIfNoneMatch  bool
	ignoreIfMatch      bool
	staleAdvanceStatus int  // when >0, a stale If-Match gets this status instead of 412
	putStatus          int  // when >0, every PUT gets this status (post-send failure injection)
	staticETag         bool // respond to every accepted PUT with the same ETag (CAS-chaining defect)
}

func (f *fakeStore) etag(v int) string { return fmt.Sprintf("%q", fmt.Sprintf("v%d", v)) }

func (f *fakeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	key := strings.TrimPrefix(r.URL.Path, "/stub/")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objs == nil {
		f.objs = map[string]int{}
	}
	switch r.Method {
	case http.MethodPut:
		if f.putStatus != 0 {
			writeS3Error(w, f.putStatus, "SlowDown")
			return
		}
		cur, exists := f.objs[key]
		if inm := r.Header.Get("If-None-Match"); inm == "*" && exists && !f.ignoreIfNoneMatch {
			writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		if im := r.Header.Get("If-Match"); im != "" && !f.ignoreIfMatch {
			if !exists || im != f.etag(cur) {
				if f.staleAdvanceStatus != 0 {
					writeS3Error(w, f.staleAdvanceStatus, "AccessDenied")
					return
				}
				writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
		}
		f.next++
		f.objs[key] = f.next
		// staticETag pins only the RESPONSE ETag (internal versioning stays
		// consistent) so a matched advance appears to return an unchanged
		// ETag — the CAS-chaining defect the probe's step 3 must catch.
		if f.staticETag {
			w.Header().Set("ETag", f.etag(1))
		} else {
			w.Header().Set("ETag", f.etag(f.next))
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		cur, exists := f.objs[key]
		if !exists {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", f.etag(cur))
		_, _ = w.Write([]byte("stub-body"))
	case http.MethodDelete:
		delete(f.objs, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, http.StatusNotImplemented, "NotImplemented")
	}
}

// stubClient wraps a CAS client around a stub server with fast backoff so
// exhaustion paths stay quick.
func stubClient(tb testing.TB, endpoint string) *s3wal.Client {
	tb.Helper()
	c := s3wal.New(rawS3Client(tb, endpoint, "us-east-1", "stub", "stub"), "stub")
	s3wal.SetTestBackoff(c, 500*time.Microsecond, 2*time.Millisecond)
	return c
}

// TestProbeRefusesNonCompliantStore proves — deterministically, without
// Docker — that the §2.6 probe refuses a store that ignores or mishandles
// the conditional headers, naming the failing step and diagnosis class,
// and that non-definitive failures (post-send 5xx, unreachable endpoint)
// surface as s3wal.ProbeInconclusive rather than accusing the store.
func TestProbeRefusesNonCompliantStore(t *testing.T) {
	cases := []struct {
		name      string
		store     *fakeStore
		wantStep  int // 0 = probe must pass
		wantClass string
		preClose  bool // close the server before probing (dial failure)
	}{
		{"compliant-store-passes", &fakeStore{}, 0, "", false},
		{"ignores-if-none-match", &fakeStore{ignoreIfNoneMatch: true}, 2, s3wal.ProbeIgnored, false},
		{"ignores-if-match", &fakeStore{ignoreIfMatch: true}, 4, s3wal.ProbeIgnored, false},
		// A 403 on a stale advance is a REAL rejection with the wrong
		// error — loud-but-wrong is the rejects class, not mishandling.
		{"rejects-stale-advance-403", &fakeStore{staleAdvanceStatus: http.StatusForbidden}, 4, s3wal.ProbeRejected, false},
		// An unchanged ETag after a matched advance is a definitive
		// non-rejection divergence — mishandling (CAS chaining would ABA).
		{"mishandles-static-etag", &fakeStore{staticETag: true}, 3, s3wal.ProbeMishandled, false},
		// A post-send 503 on the step-1 create is an s3wal.AmbiguousError (the
		// write may have landed) — inconclusive, not store behavior.
		{"inconclusive-put-5xx", &fakeStore{putStatus: http.StatusServiceUnavailable}, 1, s3wal.ProbeInconclusive, false},
		// An unreachable endpoint means the step-1 create is never
		// executed (dial-class failure) — inconclusive.
		{"inconclusive-unreachable-endpoint", &fakeStore{}, 1, s3wal.ProbeInconclusive, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.store)
			defer srv.Close()
			if tc.preClose {
				srv.Close()
			}
			err := s3wal.Probe(context.Background(), stubClient(t, srv.URL))
			if tc.wantStep == 0 {
				if err != nil {
					t.Fatalf("probe on a compliant store = %v, want nil", err)
				}
				return
			}
			var pe *s3wal.ProbeError
			if !errors.As(err, &pe) {
				t.Fatalf("probe = %v, want *s3wal.ProbeError", err)
			}
			if pe.Step != tc.wantStep || pe.Class != tc.wantClass {
				t.Fatalf("probe failed step %d class %s (%s), want step %d class %s", pe.Step, pe.Class, pe.Diagnosis, tc.wantStep, tc.wantClass)
			}
			if !strings.Contains(err.Error(), "refusing to serve") {
				t.Fatalf("probe error %q does not refuse to serve", err)
			}
		})
	}
}

// scriptedS3 serves a fixed queue of responses regardless of request
// content, recording wire attempts and the conditional headers actually
// sent — the substrate for pinning the §2.3 taxonomy classification.
type scriptedS3 struct {
	mu              sync.Mutex
	queue           []scriptedResponse
	attempts        int
	lastIfMatch     string
	lastIfNoneMatch string
}

type scriptedResponse struct {
	status int
	code   string // XML error code for status >= 300 (except 304)
	etag   string
	body   string
}

func (s *scriptedS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	s.mu.Lock()
	s.attempts++
	s.lastIfMatch = r.Header.Get("If-Match")
	s.lastIfNoneMatch = r.Header.Get("If-None-Match")
	if len(s.queue) == 0 {
		s.mu.Unlock()
		writeS3Error(w, http.StatusInternalServerError, "ScriptExhausted")
		return
	}
	resp := s.queue[0]
	s.queue = s.queue[1:]
	s.mu.Unlock()
	if resp.code != "" {
		writeS3Error(w, resp.status, resp.code)
		return
	}
	if resp.etag != "" {
		w.Header().Set("ETag", resp.etag)
	}
	w.WriteHeader(resp.status)
	if resp.body != "" {
		_, _ = w.Write([]byte(resp.body))
	}
}

func (s *scriptedS3) wireAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// TestCASTaxonomy pins every §2.3 classification branch deterministically:
// each store outcome maps to exactly one typed error, conditional headers
// are actually on the wire, and the SDK never silently re-sends a
// conditional write (NopRetryer by construction).
func TestCASTaxonomy(t *testing.T) {
	ctx := context.Background()
	newScripted := func(t *testing.T, queue ...scriptedResponse) (*scriptedS3, *s3wal.Client) {
		t.Helper()
		s := &scriptedS3{queue: queue}
		srv := httptest.NewServer(s)
		t.Cleanup(srv.Close)
		return s, stubClient(t, srv.URL)
	}

	t.Run("get-ok", func(t *testing.T) {
		s, c := newScripted(t, scriptedResponse{status: 200, etag: `"e1"`, body: "hello"})
		obj, err := c.Get(ctx, "k")
		if err != nil || string(obj.Body) != "hello" || obj.ETag != `"e1"` {
			t.Fatalf("Get = (%+v, %v), want body hello etag \"e1\"", obj, err)
		}
		if s.wireAttempts() != 1 {
			t.Fatalf("wire attempts = %d, want 1", s.wireAttempts())
		}
	})

	t.Run("get-not-exist", func(t *testing.T) {
		_, c := newScripted(t, scriptedResponse{status: 404, code: "NoSuchKey"})
		if _, err := c.Get(ctx, "k"); !errors.Is(err, s3wal.ErrNotExist) {
			t.Fatalf("Get = %v, want s3wal.ErrNotExist", err)
		}
	})

	t.Run("get-404-nosuchbucket-not-a-missing-object", func(t *testing.T) {
		_, c := newScripted(t, scriptedResponse{status: 404, code: "NoSuchBucket"})
		_, err := c.Get(ctx, "k")
		var amb *s3wal.AmbiguousError
		switch {
		case err == nil:
			t.Fatal("Get = nil, want a definitive infrastructure error")
		case errors.Is(err, s3wal.ErrNotExist):
			t.Fatalf("Get = %v — a NoSuchBucket 404 must not classify as a missing object", err)
		case errors.As(err, &amb):
			t.Fatalf("Get = %v, want a plain definitive error (the store executed and answered)", err)
		case !strings.Contains(err.Error(), "NoSuchBucket"):
			t.Fatalf("Get = %v, want the diagnostic to name the NoSuchBucket code", err)
		}
	})

	t.Run("get-if-changed-304", func(t *testing.T) {
		s, c := newScripted(t, scriptedResponse{status: 304})
		if _, err := c.GetIfChanged(ctx, "k", `"e1"`); !errors.Is(err, s3wal.ErrNotModified) {
			t.Fatalf("GetIfChanged = %v, want s3wal.ErrNotModified", err)
		}
		if s.lastIfNoneMatch != `"e1"` {
			t.Fatalf("If-None-Match on the wire = %q, want %q", s.lastIfNoneMatch, `"e1"`)
		}
	})

	t.Run("create-duplicate-412", func(t *testing.T) {
		s, c := newScripted(t, scriptedResponse{status: 412, code: "PreconditionFailed"})
		if _, err := c.Create(ctx, "k", []byte("b")); !errors.Is(err, s3wal.ErrExists) {
			t.Fatalf("Create = %v, want s3wal.ErrExists", err)
		}
		if s.lastIfNoneMatch != "*" {
			t.Fatalf("If-None-Match on the wire = %q, want *", s.lastIfNoneMatch)
		}
		if s.wireAttempts() != 1 {
			t.Fatalf("wire attempts = %d, want 1 (412 is not-executed, never retried)", s.wireAttempts())
		}
	})

	t.Run("advance-lost-race-412", func(t *testing.T) {
		s, c := newScripted(t, scriptedResponse{status: 412, code: "PreconditionFailed"})
		if _, err := c.Advance(ctx, "k", []byte("b"), `"e1"`); !errors.Is(err, s3wal.ErrLostRace) {
			t.Fatalf("Advance = %v, want s3wal.ErrLostRace", err)
		}
		if s.lastIfMatch != `"e1"` {
			t.Fatalf("If-Match on the wire = %q, want %q", s.lastIfMatch, `"e1"`)
		}
		if s.wireAttempts() != 1 {
			t.Fatalf("wire attempts = %d, want 1", s.wireAttempts())
		}
	})

	t.Run("conflict-409-retries-then-wins", func(t *testing.T) {
		s, c := newScripted(t,
			scriptedResponse{status: 409, code: "ConditionalRequestConflict"},
			scriptedResponse{status: 409, code: "ConditionalRequestConflict"},
			scriptedResponse{status: 200, etag: `"e2"`},
		)
		etag, err := c.Advance(ctx, "k", []byte("b"), `"e1"`)
		if err != nil || etag != `"e2"` {
			t.Fatalf("Advance = (%q, %v), want e2 after 409 retries", etag, err)
		}
		if s.wireAttempts() != 3 {
			t.Fatalf("wire attempts = %d, want exactly 3 (2×409 + success)", s.wireAttempts())
		}
	})

	t.Run("conflict-409-exhaustion", func(t *testing.T) {
		queue := make([]scriptedResponse, s3wal.DefaultMaxAttempts)
		for i := range queue {
			queue[i] = scriptedResponse{status: 409, code: "ConditionalRequestConflict"}
		}
		s, c := newScripted(t, queue...)
		if _, err := c.Create(ctx, "k", []byte("b")); !errors.Is(err, s3wal.ErrCASContention) {
			t.Fatalf("Create = %v, want s3wal.ErrCASContention", err)
		}
		if s.wireAttempts() != s3wal.DefaultMaxAttempts {
			t.Fatalf("wire attempts = %d, want the full budget %d", s.wireAttempts(), s3wal.DefaultMaxAttempts)
		}
	})

	t.Run("server-5xx-ambiguous-single-attempt", func(t *testing.T) {
		s, c := newScripted(t, scriptedResponse{status: 500, code: "InternalError"})
		_, err := c.Advance(ctx, "k", []byte("b"), `"e1"`)
		var amb *s3wal.AmbiguousError
		if !errors.As(err, &amb) {
			t.Fatalf("Advance = %v, want s3wal.AmbiguousError", err)
		}
		// One wire attempt proves SDK auto-retry is disabled by
		// construction: a silent SDK re-send after a 5xx could turn our own
		// landed write into a spurious 412 lost race.
		if s.wireAttempts() != 1 {
			t.Fatalf("wire attempts = %d, want 1 (SDK auto-retry must be off)", s.wireAttempts())
		}
	})

	t.Run("definitive-4xx-plain-error", func(t *testing.T) {
		_, c := newScripted(t, scriptedResponse{status: 403, code: "AccessDenied"})
		_, err := c.Advance(ctx, "k", []byte("b"), `"e1"`)
		var amb *s3wal.AmbiguousError
		if err == nil || errors.As(err, &amb) || errors.Is(err, s3wal.ErrLostRace) || errors.Is(err, s3wal.ErrNotExecuted) {
			t.Fatalf("Advance = %v, want a plain definitive error (not ambiguous, not a sentinel)", err)
		}
	})

	t.Run("dial-refused-not-executed", func(t *testing.T) {
		// Port 1 refuses immediately: the request provably never reached a
		// store, so the write is not-executed — never ambiguous.
		c := stubClient(t, "http://127.0.0.1:1")
		if _, err := c.Create(ctx, "k", []byte("b")); !errors.Is(err, s3wal.ErrNotExecuted) {
			t.Fatalf("Create = %v, want s3wal.ErrNotExecuted", err)
		}
	})

	t.Run("ctx-pre-expired-not-executed", func(t *testing.T) {
		s, c := newScripted(t)
		expired, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := c.Advance(expired, "k", []byte("b"), `"e1"`); !errors.Is(err, s3wal.ErrNotExecuted) {
			t.Fatalf("Advance = %v, want s3wal.ErrNotExecuted", err)
		}
		if s.wireAttempts() != 0 {
			t.Fatalf("wire attempts = %d, want 0 (expired before dispatch)", s.wireAttempts())
		}
	})

	t.Run("stale-read-seq-regression-panics", func(t *testing.T) {
		nonce := newNonce(t)
		_, c := newScripted(t,
			scriptedResponse{status: 200, etag: `"e5"`, body: string(encodePayloadNonce(t, 5, nonce, 0))},
			scriptedResponse{status: 200, etag: `"e3"`, body: string(encodePayloadNonce(t, 3, nonce, 0))},
		)
		c.DecodeSeq = decodeSeq
		if _, err := c.Get(ctx, "k"); err != nil {
			t.Fatalf("first Get: %v", err)
		}
		defer func() {
			if recover() == nil {
				t.Fatal("stale read (seq 3 after 5) must trip the read-side monotonicity assert")
			}
		}()
		_, _ = c.Get(ctx, "k")
	})

	t.Run("stale-read-after-own-write-panics", func(t *testing.T) {
		nonce := newNonce(t)
		staleBody := string(encodePayloadNonce(t, 4, nonce, 0))
		_, c := newScripted(t,
			scriptedResponse{status: 200, etag: `"e4"`, body: staleBody}, // read seq 4
			scriptedResponse{status: 200, etag: `"e5"`},                  // acknowledged advance to seq 5
			scriptedResponse{status: 200, etag: `"e4"`, body: staleBody}, // stale read: seq 4 again
		)
		c.DecodeSeq = decodeSeq
		obj, err := c.Get(ctx, "k")
		if err != nil {
			t.Fatalf("first Get: %v", err)
		}
		if _, err := c.Advance(ctx, "k", encodePayloadNonce(t, 5, nonce, 0), obj.ETag); err != nil {
			t.Fatalf("Advance: %v", err)
		}
		defer func() {
			if recover() == nil {
				t.Fatal("stale read (seq 4 after this session's ACKNOWLEDGED write of seq 5) must trip the assert — the write path must raise the watermark")
			}
		}()
		_, _ = c.Get(ctx, "k")
	})
}
