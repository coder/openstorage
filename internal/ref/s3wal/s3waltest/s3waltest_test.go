package s3waltest_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/code-storage/internal/ref/s3wal/s3waltest"
)

// TestEnsureBucketGatesWriteReadiness pins the issue #165 contract:
// EnsureBucket must not hand the bucket to a test before an object-write
// roundtrip has succeeded, because the s3wal qualification lanes issue
// their first object PUT on a deliberately no-retry client whose
// post-send 5xx outcome (ambiguous — the write may have landed) is fatal
// by design. Against the pre-gate EnsureBucket this test fails exactly
// like TestStoreDateWitnessSeaweedFS did on main run 33016151055: the
// first store write after EnsureBucket draws the boot-window 500.
func TestEnsureBucketGatesWriteReadiness(t *testing.T) {
	fake := &s3waltest.BootingFakeS3{PutFailures: 3}
	srv := httptest.NewServer(fake.Handler())
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

	remaining, _, deletes := fake.Snapshot()
	if remaining != 0 {
		t.Errorf("gate returned with %d boot-window PUT failures unconsumed", remaining)
	}
	if deletes == 0 {
		t.Error("readiness scratch key was never deleted")
	}
}
