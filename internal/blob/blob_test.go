package blob

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
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
// Runs against every gocloud-backed store (mem-backed S3 and file) since the
// atomic tmp+rename contract is shared.
func TestDownloadFile_DestPathShapes(t *testing.T) {
	ctx := context.Background()
	const content = "pack-bytes"

	backends := map[string]func(t *testing.T, key, content string) BlobStore{
		"s3-mem": func(t *testing.T, key, content string) BlobStore { return memS3(t, key, content) },
		"file":   seededFile,
	}
	for backend, seeded := range backends {
		t.Run(backend, func(t *testing.T) {
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
					b := seeded(t, "packs/r/pack-x.pack", content)
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
		})
	}
}

// A missing key must surface ErrNotFound through the S3 backend too: the
// mem-backed bucket drives the same shared bucketDownloadFile mapping as
// production s3blob (the file backend lane lives in file_test.go).
func TestS3_DownloadMissingKeyIsErrNotFound(t *testing.T) {
	b := memS3(t, PackKey("r", "present"), "bytes")
	dest := filepath.Join(t.TempDir(), "missing.pack")
	if err := b.DownloadFile(context.Background(), PackKey("r", "gone"), dest); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DownloadFile of missing key = %v, want ErrNotFound", err)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destPath must not exist after failed download, stat err = %v", err)
	}
}

func TestPackKeyHelpers(t *testing.T) {
	if got, want := PackKey("r1", "abc"), "packs/r1/pack-abc.pack"; got != want {
		t.Fatalf("PackKey = %q, want %q", got, want)
	}
	if got, want := PackPrefix("r1"), "packs/r1/"; got != want {
		t.Fatalf("PackPrefix = %q, want %q", got, want)
	}
	if got, want := LFSKey("r1", "abc"), "lfs/r1/abc"; got != want {
		t.Fatalf("LFSKey = %q, want %q", got, want)
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

	bl, err = Open(ctx, "file://"+t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := bl.(*File); !ok {
		t.Fatalf("file:// must open the File backend, got %T", bl)
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

// fakeBucketClient fakes the CreateBucket/HeadBucket seam so EnsureBucket's
// error handling is unit-testable without a live store.
type fakeBucketClient struct {
	createErr error
	headErr   error
	headCalls int
}

func (f *fakeBucketClient) CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	return &s3.CreateBucketOutput{}, f.createErr
}

func (f *fakeBucketClient) HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.headCalls++
	return &s3.HeadBucketOutput{}, f.headErr
}

// EnsureBucket must keep create-first semantics (fresh MinIO/dev stacks stay
// zero-setup) while accepting AccessDenied on create when the bucket is
// reachable: a least-privilege role for a pre-provisioned bucket has no
// s3:CreateBucket, and the denial is not proof the bucket is missing
// (issue #48).
func TestEnsureBucket(t *testing.T) {
	ctx := context.Background()
	apiErr := func(code string) error {
		return &smithy.GenericAPIError{Code: code, Message: code + " (fake)"}
	}

	cases := map[string]struct {
		client        *fakeBucketClient
		wantErr       []string // substrings; empty = success
		wantHeadCalls int
	}{
		"created": {
			client: &fakeBucketClient{},
		},
		"already owned by you": {
			client: &fakeBucketClient{createErr: apiErr("BucketAlreadyOwnedByYou")},
		},
		"already exists": {
			client: &fakeBucketClient{createErr: apiErr("BucketAlreadyExists")},
		},
		"access denied, bucket reachable": {
			client:        &fakeBucketClient{createErr: apiErr("AccessDenied")},
			wantHeadCalls: 1,
		},
		"access denied, probe fails": {
			client:        &fakeBucketClient{createErr: apiErr("AccessDenied"), headErr: apiErr("NotFound")},
			wantErr:       []string{"AccessDenied", "NotFound", `create bucket "b" denied`},
			wantHeadCalls: 1,
		},
		"other api error stays fatal without probe": {
			client:  &fakeBucketClient{createErr: apiErr("InternalError")},
			wantErr: []string{`create bucket "b"`, "InternalError"},
		},
		"non-api error stays fatal without probe": {
			client:  &fakeBucketClient{createErr: errors.New("dial tcp: connection refused")},
			wantErr: []string{`create bucket "b"`, "connection refused"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := ensureBucket(ctx, tc.client, "b")
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("ensureBucket: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("ensureBucket succeeded, want error")
				}
				for _, want := range tc.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q must mention %q", err, want)
					}
				}
			}
			if tc.client.headCalls != tc.wantHeadCalls {
				t.Errorf("HeadBucket called %d times, want %d", tc.client.headCalls, tc.wantHeadCalls)
			}
		})
	}
}
