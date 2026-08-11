package blob

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gocloud.dev/blob/memblob"
)

// memS3 builds an S3 backed by an in-memory bucket. DownloadFile only touches
// the bucket, so client/name may stay zero (EnsureBucket asserts otherwise).
func memS3(t *testing.T, key, content string) *S3 {
	t.Helper()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { bucket.Close() })
	if err := bucket.WriteAll(context.Background(), key, []byte(content), nil); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	return &S3{bucket: bucket}
}

// Regression for the spike dirOf helper, which panicked on relative and
// bare-filename destination paths: DownloadFile must handle any dest path.
func TestDownloadFile_DestPathShapes(t *testing.T) {
	ctx := context.Background()
	const content = "pack-bytes"

	tmp := t.TempDir()
	t.Chdir(tmp)

	cases := map[string]string{
		"absolute":      filepath.Join(tmp, "abs.pack"),
		"bare filename": "bare.pack",
		"dot relative":  "./dotrel.pack",
		"subdirectory":  "sub/nested.pack",
	}
	if err := os.Mkdir("sub", 0o755); err != nil {
		t.Fatal(err)
	}
	for name, dest := range cases {
		t.Run(name, func(t *testing.T) {
			b := memS3(t, "packs/r/pack-x.pack", content)
			if err := b.DownloadFile(ctx, "packs/r/pack-x.pack", dest); err != nil {
				t.Fatalf("DownloadFile(%q): %v", dest, err)
			}
			got, err := os.ReadFile(dest)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != content {
				t.Fatalf("content = %q, want %q", got, content)
			}
		})
	}
}

func TestPackKeyHelpers(t *testing.T) {
	if got, want := PackKey("r1", "abc"), "packs/r1/pack-abc.pack"; got != want {
		t.Fatalf("PackKey = %q, want %q", got, want)
	}
	if got, want := PackPrefix("r1"), "packs/r1/"; got != want {
		t.Fatalf("PackPrefix = %q, want %q", got, want)
	}
}

// TestOpenSchemeDispatch covers the factory's container-free paths: s3://
// dispatches to OpenS3 (opening a bucket handle needs no network) and
// unknown schemes fail fast listing the supported set.
func TestOpenSchemeDispatch(t *testing.T) {
	ctx := context.Background()

	bl, err := Open(ctx, "s3://some-bucket?region=us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := bl.(*S3); !ok {
		t.Fatalf("s3:// must open the S3 backend, got %T", bl)
	}
	if err := bl.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(ctx, "gs://bucket")
	if err == nil || !strings.Contains(err.Error(), `scheme "gs" unsupported`) {
		t.Fatalf("want unsupported-scheme error, got %v", err)
	}
	for _, scheme := range []string{"s3", "file"} {
		if !strings.Contains(err.Error(), scheme) {
			t.Fatalf("unsupported-scheme error must list %q, got %v", scheme, err)
		}
	}
}
