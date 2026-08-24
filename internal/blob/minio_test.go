// S3-backend tests against a real MinIO container (docker-gated, like the
// jobs river tests): the multipart Copy path — which unit tests cannot
// reach through fileblob — plus copy-source escaping for namespaced keys.
package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestS3_CopyMultipart(t *testing.T) {
	if testing.Short() {
		t.Skip("minio copy test needs docker")
	}
	ctx := context.Background()
	minioC, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	defer func() { _ = minioC.Terminate(ctx) }()
	addr, err := minioC.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", minioC.Username)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioC.Password)

	b, err := OpenS3(ctx, fmt.Sprintf("s3://copy-test?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", addr))
	if err != nil {
		t.Fatalf("OpenS3: %v", err)
	}
	defer b.Close()
	if err := b.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}

	// Force the multipart path with a small object: 12 MiB in 5 MiB parts
	// (S3's minimum non-final part size), single-part limit lowered to
	// 8 MiB. Restore the real limits afterwards.
	origLimit, origPart := s3SinglePartCopyLimit, s3CopyPartSize
	s3SinglePartCopyLimit, s3CopyPartSize = 8*1024*1024, 5*1024*1024
	t.Cleanup(func() { s3SinglePartCopyLimit, s3CopyPartSize = origLimit, origPart })

	content := make([]byte, 12*1024*1024)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(t.TempDir(), "src.pack")
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// A namespaced repo key ('+' in the path) through the multipart path
	// pins the copy-source escaping.
	key := PackKey("r1+import", "big")
	if err := b.UploadFile(ctx, key, src); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	dst := "sweep/quarantine/" + key
	if err := b.Copy(ctx, dst, key); err != nil {
		t.Fatalf("multipart Copy: %v", err)
	}
	got := filepath.Join(t.TempDir(), "back.pack")
	if err := b.DownloadFile(ctx, dst, got); err != nil {
		t.Fatalf("DownloadFile(copy): %v", err)
	}
	gotBytes, err := os.ReadFile(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBytes, content) {
		t.Fatalf("multipart copy corrupted content: got %d bytes, want %d identical", len(gotBytes), len(content))
	}

	// Small object stays on the single-request path.
	small := PackKey("r1", "small")
	smallSrc := filepath.Join(t.TempDir(), "small.pack")
	if err := os.WriteFile(smallSrc, []byte("tiny"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.UploadFile(ctx, small, smallSrc); err != nil {
		t.Fatal(err)
	}
	if err := b.Copy(ctx, "sweep/quarantine/"+small, small); err != nil {
		t.Fatalf("single-part Copy: %v", err)
	}
	if size, err := b.Stat(ctx, "sweep/quarantine/"+small); err != nil || size != 4 {
		t.Fatalf("Stat(small copy) = %d, %v", size, err)
	}

	// Missing source maps to ErrNotFound on the S3 backend too.
	if err := b.Copy(ctx, "x", PackKey("r1", "missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Copy of missing source = %v, want ErrNotFound", err)
	}

	// Versioning guard states: a fresh bucket is unversioned; Enabled and
	// then Suspended must BOTH report versioned (suspension retains every
	// previously stored version).
	versioned, err := b.VersioningEnabled(ctx)
	if err != nil {
		t.Fatalf("VersioningEnabled: %v", err)
	}
	if versioned {
		t.Fatal("fresh MinIO bucket reported as versioned")
	}
	for _, status := range []s3types.BucketVersioningStatus{
		s3types.BucketVersioningStatusEnabled,
		s3types.BucketVersioningStatusSuspended,
	} {
		if _, err := b.client.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
			Bucket:                  aws.String("copy-test"),
			VersioningConfiguration: &s3types.VersioningConfiguration{Status: status},
		}); err != nil {
			t.Fatalf("PutBucketVersioning(%s): %v", status, err)
		}
		versioned, err := b.VersioningEnabled(ctx)
		if err != nil {
			t.Fatalf("VersioningEnabled after %s: %v", status, err)
		}
		if !versioned {
			t.Fatalf("bucket with versioning %s must report versioned", status)
		}
	}
}

// TestS3_ExpectedBucketOwnerHarmlessOnMinIO (issue #58): MinIO ignores the
// bucket-owner condition (no global bucket namespace), so with the env var
// SET every operation must keep succeeding — the header is harmless
// pass-through on self-hosted stacks — and presigned URLs must stay
// redeemable because the hoisted x-amz-expected-bucket-owner query
// parameter is signature-covered, which the tamper probe proves live:
// stripping it breaks SigV4 validation. Enforcement itself cannot be
// integration-tested here (MinIO and the emulators ignore the condition);
// the stub-transport unit tests pin the threading instead.
func TestS3_ExpectedBucketOwnerHarmlessOnMinIO(t *testing.T) {
	if testing.Short() {
		t.Skip("minio expected-owner test needs docker")
	}
	ctx := context.Background()
	minioC, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	defer func() { _ = minioC.Terminate(ctx) }()
	addr, err := minioC.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", minioC.Username)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioC.Password)
	t.Setenv(EnvExpectedBucketOwner, "111122223333")

	b, err := OpenS3(ctx, fmt.Sprintf("s3://owner-test?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", addr))
	if err != nil {
		t.Fatalf("OpenS3: %v", err)
	}
	defer b.Close()
	if b.expectedOwner != "111122223333" {
		t.Fatalf("OpenS3 expectedOwner = %q, env not threaded", b.expectedOwner)
	}
	if err := b.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket with expected owner set: %v", err)
	}

	// The full data-plane loop the sweep/push paths use, all with the
	// condition header on the wire.
	content := []byte("expected-owner pass-through")
	src := filepath.Join(t.TempDir(), "src.pack")
	if err := os.WriteFile(src, content, 0o644); err != nil {
		t.Fatal(err)
	}
	key := PackKey("r1", "owner")
	if err := b.UploadFile(ctx, key, src); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if size, err := b.Stat(ctx, key); err != nil || size != int64(len(content)) {
		t.Fatalf("Stat = %d, %v", size, err)
	}
	if _, _, err := b.StatFingerprint(ctx, key); err != nil {
		t.Fatalf("StatFingerprint: %v", err)
	}
	if objs, err := b.List(ctx, PackPrefix("r1")); err != nil || len(objs) != 1 {
		t.Fatalf("List = %v, %v", objs, err)
	}
	if got, err := b.ReadRange(ctx, key, 0, 8); err != nil || !bytes.Equal(got, content[:8]) {
		t.Fatalf("ReadRange = %q, %v", got, err)
	}
	if err := b.Copy(ctx, "sweep/quarantine/"+key, key); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	dl := filepath.Join(t.TempDir(), "dl.pack")
	if err := b.DownloadFile(ctx, "sweep/quarantine/"+key, dl); err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	if _, err := b.VersioningEnabled(ctx); err != nil {
		t.Fatalf("VersioningEnabled: %v", err)
	}

	// Presigned GET: the URL carries the SIGNED owner query parameter and
	// redeems as-is with a plain HTTP client — no extra header.
	signed, err := b.SignedURL(ctx, key, http.MethodGet, time.Hour)
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}
	u, err := url.Parse(signed)
	if err != nil {
		t.Fatal(err)
	}
	if got := u.Query().Get(expectedOwnerHeader); got != "111122223333" {
		t.Fatalf("presigned URL query %s = %q, want owner", expectedOwnerHeader, got)
	}
	resp, err := http.Get(signed)
	if err != nil {
		t.Fatalf("redeem presigned URL: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, content) {
		t.Fatalf("presigned GET = %d %q, want 200 with content", resp.StatusCode, body)
	}

	// Tamper probe: stripping the owner parameter must break the SigV4
	// signature — the condition is not silently droppable from the URL.
	q := u.Query()
	q.Del(expectedOwnerHeader)
	u.RawQuery = q.Encode()
	resp, err = http.Get(u.String())
	if err != nil {
		t.Fatalf("redeem tampered presigned URL: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("presigned GET without owner param = %d, want 403 (signature must cover it)", resp.StatusCode)
	}

	if err := b.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
