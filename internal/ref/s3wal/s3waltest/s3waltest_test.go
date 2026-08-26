package s3waltest_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/code-storage/internal/ref/s3wal/s3waltest"
)

// bootingFakeS3 models the SeaweedFS boot window behind issue #165: the
// gateway serves bucket operations (CreateBucket/HeadBucket run on the
// filer alone) while the first object PUTs fail with 500 InternalError
// because no writable volume has been assigned yet. The failure count is
// deterministic, so the flake reproduces on every run instead of one CI
// run in dozens.
type bootingFakeS3 struct {
	mu          sync.Mutex
	putFailures int // object PUTs left to fail with 500
	objectPuts  int
	deletes     int
}

func (f *bootingFakeS3) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucketOnly := !strings.Contains(strings.TrimPrefix(r.URL.Path, "/"), "/")
		switch {
		case bucketOnly && (r.Method == http.MethodPut || r.Method == http.MethodHead):
			// CreateBucket / HeadBucket: filer-only, succeeds during boot.
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			f.mu.Lock()
			f.objectPuts++
			failing := f.putFailures > 0
			if failing {
				f.putFailures--
			}
			f.mu.Unlock()
			if failing {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>We encountered an internal error, please try again.</Message></Error>`)
				return
			}
			w.Header().Set("ETag", `"fake-etag"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete:
			f.mu.Lock()
			f.deletes++
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}

// TestEnsureBucketGatesWriteReadiness pins the issue #165 contract:
// EnsureBucket must not hand the bucket to a test before an object-write
// roundtrip has succeeded, because the s3wal qualification lanes issue
// their first object PUT on a deliberately no-retry client whose
// post-send 5xx outcome (ambiguous — the write may have landed) is fatal
// by design. Against the pre-gate EnsureBucket this test fails exactly
// like TestStoreDateWitnessSeaweedFS did on main run 33016151055: the
// first store write after EnsureBucket draws the boot-window 500.
func TestEnsureBucketGatesWriteReadiness(t *testing.T) {
	fake := &bootingFakeS3{putFailures: 3}
	srv := httptest.NewServer(fake.handler(t))
	defer srv.Close()

	client := s3waltest.RawS3Client(srv.URL, "us-east-1", "test", "test")
	if err := s3waltest.EnsureBucket(context.Background(), client, "gate-bucket"); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}

	// Mirror the qualification lanes' first object write: one wire
	// attempt, no SDK retries (s3wal CAS semantics — retrying a
	// conditional write could double-apply, so auto-retry is disabled).
	noRetry := s3.New(client.Options(), func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
	})
	if _, err := noRetry.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("gate-bucket"),
		Key:    aws.String("_wal/repo/date-witness"),
		Body:   strings.NewReader(`{"seq":1}`),
	}); err != nil {
		t.Fatalf("first no-retry store write after EnsureBucket failed — write readiness was not gated: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.putFailures != 0 {
		t.Errorf("gate returned with %d boot-window PUT failures unconsumed", fake.putFailures)
	}
	if fake.deletes == 0 {
		t.Error("readiness scratch key was never deleted")
	}
}
