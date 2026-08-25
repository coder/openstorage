// Env-gated S3 endpoint tests (issue #112): unlike the MinIO container
// tests in minio_test.go, these run against an externally provided S3
// endpoint named by CODESTORAGE_TEST_S3_URL (a full s3:// blob URL;
// credentials come from the standard AWS environment). The nightly Floci
// lane (.github/workflows/floci-nightly.yml) points them at floci/floci
// with FLOCI_SERVICES_S3_ENFORCE_AUTH=true and an IAM-provisioned (not
// root) access key — a backend that enforces SigV4 on every request, so
// presigned-URL accept/reject behavior is pinned end-to-end here: no other
// lane covers corrupted-signature rejection (fileblob never issues
// presigned URLs at all — ErrSignedURLUnsupported). With real AWS
// credentials and an existing bucket the same tests run against real S3
// unchanged.
//
// Known Floci 1.7.0 gaps — do NOT add tests here that require IAM policy
// evaluation on S3 actions (Deny policies are not enforced, issue #48
// stays mock-covered) or x-amz-expected-bucket-owner enforcement (the
// header is ignored, issue #58 stays seam-covered).
package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// EnvTestS3URL names the external S3 endpoint the tests in this file run
// against; unset skips them.
const EnvTestS3URL = "CODESTORAGE_TEST_S3_URL"

// openS3Endpoint opens the externally provided bucket or skips the test.
func openS3Endpoint(t *testing.T) *S3 {
	t.Helper()
	blobURL := os.Getenv(EnvTestS3URL)
	if blobURL == "" {
		t.Skipf("set %s (s3://bucket?endpoint=http://127.0.0.1:4566&s3ForcePathStyle=true&region=us-east-1) plus AWS credentials to run", EnvTestS3URL)
	}
	ctx := context.Background()
	b, err := OpenS3(ctx, blobURL)
	if err != nil {
		t.Fatalf("OpenS3(%s): %v", blobURL, err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
	return b
}

// randomHex returns n crypto-random bytes hex-encoded, so keys stay unique
// across reruns against a shared long-lived bucket (real S3).
func randomHex(t *testing.T, n int) string {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(buf)
}

// corruptSignature flips the first character of the X-Amz-Signature query
// parameter — the smallest tamper that leaves everything the signature
// covers untouched, so acceptance would prove the backend never checked it.
func corruptSignature(t *testing.T, signed string) string {
	t.Helper()
	u, err := url.Parse(signed)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	sig := q.Get("X-Amz-Signature")
	if sig == "" {
		t.Fatalf("presigned URL missing X-Amz-Signature: %s", signed)
	}
	flipped := "0"
	if sig[0] == '0' {
		flipped = "1"
	}
	q.Set("X-Amz-Signature", flipped+sig[1:])
	u.RawQuery = q.Encode()
	return u.String()
}

// TestS3Endpoint_RoundTrip is the CRUD smoke against the external
// endpoint: the same UploadFile → Stat → List → ReadRange → Copy →
// DownloadFile loop the pack paths use, on a namespaced key so
// copy-source escaping is exercised too.
func TestS3Endpoint_RoundTrip(t *testing.T) {
	b := openS3Endpoint(t)
	ctx := context.Background()

	repo := "endpoint-rt-" + randomHex(t, 8) + "+import"
	key := PackKey(repo, "rt")
	quarantined := "sweep/quarantine/" + key
	t.Cleanup(func() {
		_ = b.Delete(ctx, key)
		_ = b.Delete(ctx, quarantined)
	})

	content := []byte("s3 endpoint round trip")
	src := filepath.Join(t.TempDir(), "src.pack")
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.UploadFile(ctx, key, src); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if size, err := b.Stat(ctx, key); err != nil || size != int64(len(content)) {
		t.Fatalf("Stat = %d, %v; want %d", size, err, len(content))
	}
	if objs, err := b.List(ctx, PackPrefix(repo)); err != nil || len(objs) != 1 || objs[0].Key != key {
		t.Fatalf("List = %v, %v; want exactly %q", objs, err, key)
	}
	if got, err := b.ReadRange(ctx, key, 0, 8); err != nil || !bytes.Equal(got, content[:8]) {
		t.Fatalf("ReadRange = %q, %v", got, err)
	}
	if err := b.Copy(ctx, quarantined, key); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	dl := filepath.Join(t.TempDir(), "dl.pack")
	if err := b.DownloadFile(ctx, quarantined, dl); err != nil {
		t.Fatalf("DownloadFile(copy): %v", err)
	}
	if got, err := os.ReadFile(dl); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("copied content = %q, %v; want original", got, err)
	}
	if err := b.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := b.Stat(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stat after Delete = %v, want ErrNotFound", err)
	}
}

// TestS3Endpoint_PresignedSigV4 pins real SigV4 verification on presigned
// URLs — the property the LFS presigned-GET path (internal/githttp/lfs.go)
// relies on but no other integration lane can check: a valid signature
// serves the exact bytes, and corrupting one character of the signature is
// rejected with 403 SignatureDoesNotMatch instead of being served.
func TestS3Endpoint_PresignedSigV4(t *testing.T) {
	b := openS3Endpoint(t)
	ctx := context.Background()

	base := "endpoint-sig-" + randomHex(t, 8)
	key := PackKey(base, "get")
	putKey := PackKey(base, "put")
	deniedKey := PackKey(base, "denied")
	t.Cleanup(func() {
		for _, k := range []string{key, putKey, deniedKey} {
			_ = b.Delete(ctx, k)
		}
	})

	content := make([]byte, 1024)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(t.TempDir(), "src.pack")
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.UploadFile(ctx, key, src); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	// Presigned GET, valid signature: 200 with the exact bytes.
	signedGet, err := b.SignedURL(ctx, key, http.MethodGet, 15*time.Minute)
	if err != nil {
		t.Fatalf("SignedURL(GET): %v", err)
	}
	status, body := httpDo(t, http.MethodGet, signedGet, nil)
	if status != http.StatusOK || !bytes.Equal(body, content) {
		t.Fatalf("valid presigned GET = %d (%d bytes), want 200 with %d original bytes", status, len(body), len(content))
	}

	// Presigned GET, corrupted signature: 403 SignatureDoesNotMatch —
	// never the bytes.
	status, body = httpDo(t, http.MethodGet, corruptSignature(t, signedGet), nil)
	if status != http.StatusForbidden || !bytes.Contains(body, []byte("SignatureDoesNotMatch")) {
		t.Fatalf("tampered presigned GET = %d %q, want 403 SignatureDoesNotMatch", status, body)
	}

	// Presigned PUT, valid signature: the object lands byte-identical.
	signedPut, err := b.SignedURL(ctx, putKey, http.MethodPut, 15*time.Minute)
	if err != nil {
		t.Fatalf("SignedURL(PUT): %v", err)
	}
	if status, body = httpDo(t, http.MethodPut, signedPut, content); status < 200 || status > 299 {
		t.Fatalf("valid presigned PUT = %d %q, want 2xx", status, body)
	}
	if got, err := b.ReadRange(ctx, putKey, 0, int64(len(content))); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("ReadRange after presigned PUT = %d bytes, %v; want original", len(got), err)
	}

	// Presigned PUT, corrupted signature: 403 SignatureDoesNotMatch and
	// nothing is created under the key.
	signedDenied, err := b.SignedURL(ctx, deniedKey, http.MethodPut, 15*time.Minute)
	if err != nil {
		t.Fatalf("SignedURL(PUT denied): %v", err)
	}
	status, body = httpDo(t, http.MethodPut, corruptSignature(t, signedDenied), content)
	if status != http.StatusForbidden || !bytes.Contains(body, []byte("SignatureDoesNotMatch")) {
		t.Fatalf("tampered presigned PUT = %d %q, want 403 SignatureDoesNotMatch", status, body)
	}
	if _, err := b.Stat(ctx, deniedKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stat(%s) after rejected PUT = %v, want ErrNotFound", deniedKey, err)
	}
}

// httpDo redeems a presigned URL with a plain HTTP client — no SDK, no
// credentials — and returns status plus full body.
func httpDo(t *testing.T, method, rawURL string, payload []byte) (int, []byte) {
	t.Helper()
	var reqBody io.Reader
	if payload != nil {
		reqBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequest(method, rawURL, reqBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}
