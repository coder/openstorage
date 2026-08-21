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
	"os"
	"path/filepath"
	"testing"

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
}
