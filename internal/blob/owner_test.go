// Expected-bucket-owner enforcement tests (issue #58): the client
// middleware must stamp x-amz-expected-bucket-owner onto EVERY HTTP
// request the SDK emits — gocloud data operations, raw-client bucket and
// multipart-copy calls, and presigned URLs — and stamp
// x-amz-source-expected-bucket-owner onto copy requests. Neither MinIO nor
// any emulator enforces the condition, so these tests capture the real
// SDK-serialized HTTP requests with a stub transport and assert the
// headers directly; the MinIO integration test proves the header is
// harmless pass-through. The unset lane pins the load-bearing
// compatibility claim: no owner configured = no condition headers at all.
package blob

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob/s3blob"
)

// capturedRequest is one HTTP request the stub transport saw: the op label
// classified from the request shape plus a copy of its headers.
type capturedRequest struct {
	op     string
	header http.Header
}

// stubS3Transport intercepts every SDK HTTP request, records it, and
// synthesizes a minimal valid S3 response so real network never happens.
type stubS3Transport struct {
	mu       sync.Mutex
	requests []capturedRequest
	// headSize is the Content-Length HeadObject responses report.
	headSize int64
	// failOps maps op labels to an S3 error code returned as an HTTP 4xx,
	// forcing specific SDK error paths (multipart abort, bucket probes).
	failOps map[string]string
}

func (t *stubS3Transport) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.requests = nil
}

func (t *stubS3Transport) captured() []capturedRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]capturedRequest(nil), t.requests...)
}

// classify labels the request with the S3 API operation it serializes.
func classify(r *http.Request) string {
	q := r.URL.Query()
	// Path-style: /bucket or /bucket/key...
	_, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		return "CreateMultipartUpload"
	case r.Method == http.MethodPost && q.Has("uploadId"):
		return "CompleteMultipartUpload"
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		return "AbortMultipartUpload"
	case r.Method == http.MethodPut && q.Has("partNumber"):
		return "UploadPartCopy"
	case r.Method == http.MethodPut && r.Header.Get("x-amz-copy-source") != "":
		return "CopyObject"
	case r.Method == http.MethodGet && q.Has("versioning"):
		return "GetBucketVersioning"
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		return "ListObjectsV2"
	case r.Method == http.MethodPut && key == "":
		return "CreateBucket"
	case r.Method == http.MethodHead && key == "":
		return "HeadBucket"
	case r.Method == http.MethodHead:
		return "HeadObject"
	case r.Method == http.MethodGet:
		return "GetObject"
	case r.Method == http.MethodPut:
		return "PutObject"
	case r.Method == http.MethodDelete:
		return "DeleteObject"
	}
	return "UNKNOWN(" + r.Method + " " + r.URL.String() + ")"
}

func xmlResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Header:        http.Header{"Content-Type": []string{"application/xml"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func (t *stubS3Transport) Do(r *http.Request) (*http.Response, error) {
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
	}
	op := classify(r)
	t.mu.Lock()
	t.requests = append(t.requests, capturedRequest{op: op, header: r.Header.Clone()})
	failCode := t.failOps[op]
	headSize := t.headSize
	t.mu.Unlock()

	if failCode != "" {
		return xmlResponse(400, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>`+failCode+`</Code><Message>forced by test</Message></Error>`), nil
	}
	switch op {
	case "CreateBucket", "HeadBucket", "PutObject":
		resp := xmlResponse(200, "")
		resp.Header.Set("ETag", `"stub-etag"`)
		return resp, nil
	case "GetBucketVersioning":
		return xmlResponse(200, `<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`), nil
	case "ListObjectsV2":
		return xmlResponse(200, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>`), nil
	case "HeadObject":
		resp := xmlResponse(200, "")
		resp.Header.Set("Content-Length", fmt.Sprint(headSize))
		resp.Header.Set("ETag", `"stub-etag"`)
		resp.Header.Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		resp.ContentLength = headSize
		return resp, nil
	case "GetObject":
		body := strings.Repeat("x", int(headSize))
		status := 200
		header := http.Header{}
		if rng := r.Header.Get("Range"); rng != "" {
			var start, end int64
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err == nil {
				status = 206
				header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, headSize))
				body = body[start : end+1]
			}
		}
		resp := xmlResponse(status, body)
		for k, v := range header {
			resp.Header[k] = v
		}
		resp.Header.Set("Content-Length", fmt.Sprint(len(body)))
		resp.Header.Set("ETag", `"stub-etag"`)
		resp.Header.Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		return resp, nil
	case "DeleteObject":
		return xmlResponse(204, ""), nil
	case "CopyObject":
		return xmlResponse(200, `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>"stub-etag"</ETag><LastModified>2006-01-02T15:04:05.000Z</LastModified></CopyObjectResult>`), nil
	case "CreateMultipartUpload":
		return xmlResponse(200, `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>owner-test</Bucket><Key>k</Key><UploadId>stub-upload</UploadId></InitiateMultipartUploadResult>`), nil
	case "UploadPartCopy":
		return xmlResponse(200, `<?xml version="1.0" encoding="UTF-8"?><CopyPartResult><ETag>"stub-etag"</ETag></CopyPartResult>`), nil
	case "CompleteMultipartUpload":
		return xmlResponse(200, `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><ETag>"stub-etag"</ETag></CompleteMultipartUploadResult>`), nil
	case "AbortMultipartUpload":
		return xmlResponse(204, ""), nil
	}
	return xmlResponse(400, `<Error><Code>NotImplementedByStub</Code><Message>`+op+`</Message></Error>`), nil
}

// newStubbedS3 builds an *S3 exactly like OpenS3 does — same client option
// wiring, so the production middleware is what runs — but on the stub
// transport with static credentials.
func newStubbedS3(t *testing.T, transport *stubS3Transport, owner string) *S3 {
	t.Helper()
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient:  transport,
	}
	client := s3.NewFromConfig(cfg, s3ClientOptions("http://stub.invalid:9", owner))
	bucket, err := s3blob.OpenBucket(context.Background(), client, "owner-test", nil)
	if err != nil {
		t.Fatalf("open stub bucket: %v", err)
	}
	store := &S3{bucket: bucket, client: client, name: "owner-test", expectedOwner: owner}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// runAllOps drives every S3 API operation the store can issue through the
// public BlobStore surface (plus VersioningEnabled), returning any errors
// keyed by step so one failure doesn't hide the header assertions.
func runAllOps(t *testing.T, store *S3, transport *stubS3Transport) {
	t.Helper()
	ctx := context.Background()

	step := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	transport.mu.Lock()
	transport.headSize = 5
	transport.mu.Unlock()

	step("EnsureBucket", store.EnsureBucket(ctx))
	_, err := store.VersioningEnabled(ctx)
	step("VersioningEnabled", err)

	src := filepath.Join(t.TempDir(), "src.bin")
	step("write local", os.WriteFile(src, []byte("hello"), 0o644))
	step("UploadFile", store.UploadFile(ctx, "packs/r/pack-a.pack", src))

	step("DownloadFile", store.DownloadFile(ctx, "packs/r/pack-a.pack", filepath.Join(t.TempDir(), "dl.bin")))
	_, err = store.Stat(ctx, "packs/r/pack-a.pack")
	step("Stat", err)
	_, _, err = store.StatFingerprint(ctx, "packs/r/pack-a.pack")
	step("StatFingerprint", err)
	_, err = store.ReadRange(ctx, "packs/r/pack-a.pack", 1, 2)
	step("ReadRange", err)
	_, err = store.List(ctx, "packs/r/")
	step("List", err)
	step("Delete", store.Delete(ctx, "packs/r/pack-a.pack"))

	// Small copy: HeadObject + CopyObject.
	step("Copy small", store.Copy(ctx, "packs/r/pack-b.pack", "packs/r/pack-a.pack"))

	// Multipart copy: shrink the single-part limit below the object size.
	origLimit, origPart := s3SinglePartCopyLimit, s3CopyPartSize
	s3SinglePartCopyLimit, s3CopyPartSize = 3, 2
	defer func() { s3SinglePartCopyLimit, s3CopyPartSize = origLimit, origPart }()
	step("Copy multipart", store.Copy(ctx, "packs/r/pack-c.pack", "packs/r/pack-a.pack"))

	// Failing part copy: the abort path must also carry the condition.
	transport.mu.Lock()
	transport.failOps = map[string]string{"UploadPartCopy": "InvalidRequest"}
	transport.mu.Unlock()
	if err := store.Copy(ctx, "packs/r/pack-d.pack", "packs/r/pack-a.pack"); err == nil {
		t.Fatal("Copy with failing UploadPartCopy succeeded, want error")
	}
	transport.mu.Lock()
	transport.failOps = nil
	transport.mu.Unlock()
	s3SinglePartCopyLimit, s3CopyPartSize = origLimit, origPart
}

// allOps is every S3 API operation runAllOps must have emitted — a missed
// label means an operation type silently stopped being exercised.
var allOps = []string{
	"CreateBucket", "GetBucketVersioning", "PutObject", "GetObject",
	"HeadObject", "ListObjectsV2", "DeleteObject", "CopyObject",
	"CreateMultipartUpload", "UploadPartCopy", "CompleteMultipartUpload",
	"AbortMultipartUpload",
}

func TestExpectedBucketOwner_ThreadedOnEveryOperation(t *testing.T) {
	const owner = "111122223333"
	transport := &stubS3Transport{}
	store := newStubbedS3(t, transport, owner)

	runAllOps(t, store, transport)

	reqs := transport.captured()
	seen := map[string]bool{}
	for _, r := range reqs {
		seen[r.op] = true
		if got := r.header.Get(expectedOwnerHeader); got != owner {
			t.Errorf("%s: %s = %q, want %q", r.op, expectedOwnerHeader, got, owner)
		}
		hasCopySource := r.header.Get("x-amz-copy-source") != ""
		if got := r.header.Get(expectedSourceOwnerHeader); (got == owner) != hasCopySource {
			t.Errorf("%s: %s = %q (copy-source present: %v)", r.op, expectedSourceOwnerHeader, got, hasCopySource)
		}
	}
	for _, op := range allOps {
		if !seen[op] {
			t.Errorf("operation %s never exercised — header coverage unproven", op)
		}
	}

	// The EnsureBucket squatting probe: CreateBucket → BucketAlreadyExists
	// forces the HeadBucket owner probe, which must carry the condition.
	transport.reset()
	transport.mu.Lock()
	transport.failOps = map[string]string{"CreateBucket": "BucketAlreadyExists"}
	transport.mu.Unlock()
	if err := store.EnsureBucket(context.Background()); err != nil {
		t.Fatalf("EnsureBucket with existing bucket and passing probe: %v", err)
	}
	probe := transport.captured()
	if len(probe) != 2 || probe[0].op != "CreateBucket" || probe[1].op != "HeadBucket" {
		t.Fatalf("squatting probe requests = %+v, want CreateBucket then HeadBucket", probe)
	}
	if got := probe[1].header.Get(expectedOwnerHeader); got != owner {
		t.Fatalf("HeadBucket probe %s = %q, want %q", expectedOwnerHeader, got, owner)
	}
}

// TestExpectedBucketOwner_SignedURLCarriesSignedQueryParam: the presigner
// hoists the middleware's x-amz-expected-bucket-owner header into a SIGNED
// lowercase query parameter — the spelling S3 enforces on presigned
// requests (aws-sdk-go-v2 #2508: header-in-SignedHeaders or lowercase
// query param; the SDK emits the latter). The parameter sits before
// X-Amz-Signature in the canonical request, so stripping or altering it
// breaks the signature (the MinIO integration test proves that live).
// This test also pins the hoisting behavior across SDK upgrades: if a
// future SDK stopped hoisting (leaving the header in SignedHeaders),
// redeemers would suddenly need to send the header and this fails loudly.
func TestExpectedBucketOwner_SignedURLCarriesSignedQueryParam(t *testing.T) {
	const owner = "111122223333"
	store := newStubbedS3(t, &stubS3Transport{}, owner)

	for _, method := range []string{http.MethodGet, http.MethodPut} {
		signed, err := store.SignedURL(context.Background(), "lfs/r/o2/abc", method, time.Hour)
		if err != nil {
			t.Fatalf("SignedURL %s: %v", method, err)
		}
		u, err := url.Parse(signed)
		if err != nil {
			t.Fatalf("parse presigned URL: %v", err)
		}
		q := u.Query()
		if got := q.Get(expectedOwnerHeader); got != owner {
			t.Errorf("%s presign query %s = %q, want %q (URL: %s)", method, expectedOwnerHeader, got, owner, signed)
		}
		if sh := q.Get("X-Amz-SignedHeaders"); strings.Contains(sh, expectedOwnerHeader) {
			t.Errorf("%s presign left %s in SignedHeaders (%q): redeemers would need to send the header — hoisting regressed",
				method, expectedOwnerHeader, sh)
		}
		if q.Get("X-Amz-Signature") == "" {
			t.Errorf("%s presign URL missing X-Amz-Signature: %s", method, signed)
		}
	}
}

// TestExpectedBucketOwner_UnsetChangesNothing pins the compatibility
// claim: with no owner configured, no request carries either condition
// header and presigned URLs sign host only.
func TestExpectedBucketOwner_UnsetChangesNothing(t *testing.T) {
	transport := &stubS3Transport{}
	store := newStubbedS3(t, transport, "")

	runAllOps(t, store, transport)

	for _, r := range transport.captured() {
		if got := r.header.Get(expectedOwnerHeader); got != "" {
			t.Errorf("%s: unexpected %s = %q with owner unset", r.op, expectedOwnerHeader, got)
		}
		if got := r.header.Get(expectedSourceOwnerHeader); got != "" {
			t.Errorf("%s: unexpected %s = %q with owner unset", r.op, expectedSourceOwnerHeader, got)
		}
	}

	signed, err := store.SignedURL(context.Background(), "lfs/r/o2/abc", http.MethodGet, time.Hour)
	if err != nil {
		t.Fatalf("SignedURL: %v", err)
	}
	u, err := url.Parse(signed)
	if err != nil {
		t.Fatal(err)
	}
	if got := u.Query().Get(expectedOwnerHeader); got != "" {
		t.Errorf("presign query %s = %q, must be absent with owner unset", expectedOwnerHeader, got)
	}
	if sh := u.Query().Get("X-Amz-SignedHeaders"); strings.Contains(sh, expectedOwnerHeader) {
		t.Errorf("presign SignedHeaders = %q, must not include %s with owner unset", sh, expectedOwnerHeader)
	}
}

func TestExpectedBucketOwner_EnvValidation(t *testing.T) {
	cases := map[string]struct {
		env     string
		want    string
		wantErr bool
	}{
		"unset":               {env: "", want: ""},
		"valid":               {env: "111122223333", want: "111122223333"},
		"whitespace trimmed":  {env: " 111122223333\n", want: "111122223333"},
		"too short":           {env: "12345", wantErr: true},
		"too long":            {env: "1111222233334", wantErr: true},
		"non-digits":          {env: "11112222333x", wantErr: true},
		"role arn not an id":  {env: "arn:aws:iam::111122223333:role/x", wantErr: true},
		"negative or symbols": {env: "-11122223333", wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv(EnvExpectedBucketOwner, tc.env)
			got, err := expectedBucketOwnerFromEnv()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expectedBucketOwnerFromEnv(%q) succeeded, want error", tc.env)
				}
				if !strings.Contains(err.Error(), EnvExpectedBucketOwner) {
					t.Errorf("error %q must name %s", err, EnvExpectedBucketOwner)
				}
				return
			}
			if err != nil {
				t.Fatalf("expectedBucketOwnerFromEnv(%q): %v", tc.env, err)
			}
			if got != tc.want {
				t.Fatalf("expectedBucketOwnerFromEnv(%q) = %q, want %q", tc.env, got, tc.want)
			}
		})
	}

	// OpenS3 must refuse a malformed owner before touching the network.
	t.Setenv(EnvExpectedBucketOwner, "not-an-account")
	if _, err := OpenS3(context.Background(), "s3://b?endpoint=http://stub.invalid:9"); err == nil ||
		!strings.Contains(err.Error(), EnvExpectedBucketOwner) {
		t.Fatalf("OpenS3 with malformed owner = %v, want boot-fatal validation error", err)
	}
}
