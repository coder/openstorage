// Expected-bucket-owner enforcement for the s3wal conditional client
// (PR #168 security review; the issue #58 pattern — see
// internal/blob/owner_test.go): the Dial-installed middleware must stamp
// x-amz-expected-bucket-owner onto EVERY request the client emits, and an
// unset owner must change nothing. No self-hosted store enforces the
// condition (SeaweedFS ignores it — AWS documents unsupported operations
// as ignore-not-reject), so these tests capture the HTTP requests the SDK
// actually sends against a local stub endpoint, driving the REAL Dial
// wiring end-to-end: env read, validation, middleware install, and New's
// no-retry client re-derivation.
package s3wal

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// ownerCapture records the op and condition header of every request a
// Dial-built client emits, synthesizing minimal valid S3 responses.
type ownerCapture struct {
	mu   sync.Mutex
	reqs []struct{ op, owner string }
}

func (c *ownerCapture) captured() []struct{ op, owner string } {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]struct{ op, owner string }(nil), c.reqs...)
}

// classifyOwnerOp labels a request with the S3 operation the conditional
// client serializes — the full surface is GetObject, PutObject,
// DeleteObject, and ListObjectsV2 (no copy-source operations).
func classifyOwnerOp(r *http.Request) string {
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		return "ListObjectsV2"
	case r.Method == http.MethodGet:
		return "GetObject"
	case r.Method == http.MethodPut:
		return "PutObject"
	case r.Method == http.MethodDelete:
		return "DeleteObject"
	}
	return "UNKNOWN(" + r.Method + " " + r.URL.String() + ")"
}

func (c *ownerCapture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
	}
	op := classifyOwnerOp(r)
	c.mu.Lock()
	c.reqs = append(c.reqs, struct{ op, owner string }{op, r.Header.Get(expectedOwnerHeader)})
	c.mu.Unlock()
	switch op {
	case "PutObject":
		w.Header().Set("ETag", `"stub-etag"`)
	case "GetObject":
		w.Header().Set("ETag", `"stub-etag"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		_, _ = io.WriteString(w, "x")
	case "ListObjectsV2":
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>`)
	case "DeleteObject":
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unexpected op "+op, http.StatusBadRequest)
	}
}

// dialStub points Dial at the capture endpoint with deterministic static
// env credentials (LoadDefaultConfig resolves the env provider first).
func dialStub(t *testing.T, endpoint string) *Client {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	c, err := Dial(context.Background(), fmt.Sprintf("s3wal://owner-test?endpoint=%s&region=us-east-1", endpoint))
	if err != nil {
		t.Fatalf("dial stub endpoint: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// driveAllOps issues every operation type the conditional client can
// emit: Create and Advance (PutObject), Get and GetIfChanged (GetObject),
// List (ListObjectsV2), and Delete (DeleteObject).
func driveAllOps(t *testing.T, c *Client) {
	t.Helper()
	ctx := context.Background()
	etag, err := c.Create(ctx, "_wal/repo/owner", []byte("v1"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := c.Advance(ctx, "_wal/repo/owner", []byte("v2"), etag); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if _, err := c.Get(ctx, "_wal/repo/owner"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.GetIfChanged(ctx, "_wal/repo/owner", `"other-etag"`); err != nil {
		t.Fatalf("GetIfChanged: %v", err)
	}
	if _, err := c.List(ctx, "_wal/", "", 10); err != nil {
		t.Fatalf("List: %v", err)
	}
	if err := c.Delete(ctx, "_wal/repo/owner"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// allOwnerOps is every S3 API operation driveAllOps must have emitted — a
// missed label means an operation type silently stopped being exercised.
var allOwnerOps = []string{"PutObject", "GetObject", "ListObjectsV2", "DeleteObject"}

func TestExpectedBucketOwner_ThreadedOnEveryRequest(t *testing.T) {
	const owner = "111122223333"
	capture := &ownerCapture{}
	srv := httptest.NewServer(capture)
	defer srv.Close()

	t.Setenv(envExpectedBucketOwner, owner)
	driveAllOps(t, dialStub(t, srv.URL))

	seen := map[string]bool{}
	for _, r := range capture.captured() {
		seen[r.op] = true
		if r.owner != owner {
			t.Errorf("%s: %s = %q, want %q", r.op, expectedOwnerHeader, r.owner, owner)
		}
	}
	for _, op := range allOwnerOps {
		if !seen[op] {
			t.Errorf("operation %s never exercised — header coverage unproven", op)
		}
	}
}

func TestExpectedBucketOwner_UnsetChangesNothing(t *testing.T) {
	capture := &ownerCapture{}
	srv := httptest.NewServer(capture)
	defer srv.Close()

	t.Setenv(envExpectedBucketOwner, "")
	driveAllOps(t, dialStub(t, srv.URL))

	reqs := capture.captured()
	if len(reqs) == 0 {
		t.Fatal("no requests captured")
	}
	for _, r := range reqs {
		if r.owner != "" {
			t.Errorf("%s: unexpected %s = %q with owner unset", r.op, expectedOwnerHeader, r.owner)
		}
	}
}

// TestExpectedBucketOwner_MalformedIsBootFatal: a typo'd owner would 403
// every operation on real AWS, so Dial must refuse it outright — before
// any client exists.
func TestExpectedBucketOwner_MalformedIsBootFatal(t *testing.T) {
	for _, bad := range []string{"12345", "1111222233334", "11112222333x", "arn:aws:iam::111122223333:role/x"} {
		t.Setenv(envExpectedBucketOwner, bad)
		_, err := Dial(context.Background(), "s3wal://bkt?endpoint=http://127.0.0.1:1&region=us-east-1")
		if err == nil || !strings.Contains(err.Error(), envExpectedBucketOwner) {
			t.Errorf("Dial with %s=%q: want a boot-fatal error naming the variable, got %v", envExpectedBucketOwner, bad, err)
		}
	}
}
