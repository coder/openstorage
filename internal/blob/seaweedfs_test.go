// S3-backend tests against a real SeaweedFS container (docker-gated, like
// the jobs river tests): the multipart Copy path — which unit tests cannot
// reach through fileblob — plus copy-source escaping for namespaced keys.
// SeaweedFS is the repo's tested reference self-hosted S3 store (issue
// #133, qualified in the Gate 1a-A campaign — PR #142, which adds
// docs/plans/s3wal-phase1a-results.md); MinIO remains best-effort
// compatible and no longer backs any blob test lane — its only CI
// presence is the record-mode legacy qualification lane
// (internal/ref/s3wal TestQualifyMinIOLegacy), which documents findings
// without gating.
package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/coder/open-storage/internal/ref/s3wal/s3waltest"
)

// Qualified SeaweedFS recipe (Gate 1a-A, PR #142): pinned image,
// one-process `weed server -s3`, S3 gateway on 8333. Versioning and
// object-lock are never enabled on data buckets (seaweedfs#8073 avoidance).
const seaweedFSImage = "chrislusf/seaweedfs:4.45"

// Static identity for the auth-enforcing lane (see startSeaweedFS): with an
// s3.config mounted, SeaweedFS validates SigV4 on every request, which the
// presigned-URL tamper probe needs. Dummy values — the container is
// throwaway and unreachable beyond the mapped test port.
const (
	seaweedFSAccessKey = "codestorage-test"
	seaweedFSSecretKey = "codestorage-test-secret"
)

const seaweedFSS3Config = `{
  "identities": [
    {
      "name": "codestorage-test",
      "credentials": [{"accessKey": "` + seaweedFSAccessKey + `", "secretKey": "` + seaweedFSSecretKey + `"}],
      "actions": ["Admin", "Read", "List", "Tagging", "Write"]
    }
  ]
}`

// startSeaweedFS boots the pinned SeaweedFS image and returns the mapped S3
// gateway address (host:port). The wait strategy only proves the gateway
// port accepts TCP — weed starts master/volume/filer/s3 asynchronously — so
// callers must treat the first successful bucket create as the actual
// readiness signal (ensureBucketReady). enforceAuth mounts the static
// identity above, turning on SigV4 validation; false runs the gateway
// anonymous, exactly like the Phase 1a qualification harness.
func startSeaweedFS(t *testing.T, enforceAuth bool) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        seaweedFSImage,
		ExposedPorts: []string{"8333/tcp"},
		Cmd:          []string{"server", "-s3", "-dir=/data", "-ip.bind=0.0.0.0"},
		WaitingFor:   wait.ForListeningPort("8333/tcp").WithStartupTimeout(2 * time.Minute),
	}
	if enforceAuth {
		req.Cmd = append(req.Cmd, "-s3.config=/etc/seaweedfs/s3.json")
		req.Files = []testcontainers.ContainerFile{{
			Reader:            strings.NewReader(seaweedFSS3Config),
			ContainerFilePath: "/etc/seaweedfs/s3.json",
			FileMode:          0o444,
		}}
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start seaweedfs container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	// PortEndpoint brackets IPv6 hosts (net.JoinHostPort), so the address
	// stays a valid URL host under IPv6 Docker daemons.
	addr, err := c.PortEndpoint(ctx, "8333/tcp", "")
	if err != nil {
		t.Fatalf("seaweedfs endpoint: %v", err)
	}
	return addr
}

// ensureBucketReady retries EnsureBucket until the gateway chain behind the
// already-listening S3 port has finished booting, then proves WRITE
// readiness through the shared s3wal harness gate. Port readiness alone is
// not enough (the first CreateBucket calls fail while the filer warms up),
// and bucket existence alone is not enough either: SeaweedFS 500s object
// PUTs until the master assigns a writable volume, and a slow runner can
// exhaust the SDK's 3-attempt budget inside that window (issue #177, the
// blob-harness recurrence of #165). The gate rides b.client, so it carries
// the same middleware (expected-bucket-owner) as the test traffic.
func ensureBucketReady(t *testing.T, b *S3) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(60 * time.Second)
	for {
		err := b.EnsureBucket(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("EnsureBucket never succeeded (SeaweedFS not ready?): %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := s3waltest.WaitWriteReady(ctx, b.client, b.name); err != nil {
		t.Fatalf("write readiness: %v", err)
	}
}

// TestEnsureBucketReadyGatesWriteReadiness pins the issue #177 contract
// (the internal/blob recurrence of #165): ensureBucketReady must not hand
// the store to a test before an object-write roundtrip has succeeded.
// SeaweedFS serves bucket operations from the filer while object PUTs 500
// until a writable volume is assigned, and a slow runner can exhaust the
// SDK's default 3-attempt budget inside that window — exactly how
// TestS3_ExpectedBucketOwnerHarmlessOnSeaweedFS failed on PR #176 run
// 33047829563. The deterministic fake fails one more PUT than the SDK
// budget, so against a pre-gate ensureBucketReady the first upload fails
// on every run instead of one CI run in dozens. No Docker required.
func TestEnsureBucketReadyGatesWriteReadiness(t *testing.T) {
	fake := &s3waltest.BootingFakeS3{PutFailures: 4}
	srv := httptest.NewServer(fake.Handler())
	defer srv.Close()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx := context.Background()
	b, err := OpenS3(ctx, fmt.Sprintf("s3://gate-bucket?endpoint=%s&s3ForcePathStyle=true&region=us-east-1", srv.URL))
	if err != nil {
		t.Fatalf("OpenS3: %v", err)
	}
	defer b.Close()
	ensureBucketReady(t, b)

	// The suite's first real write after the harness declares readiness.
	src := filepath.Join(t.TempDir(), "src.pack")
	if err := os.WriteFile(src, []byte("boot-window readiness probe"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.UploadFile(ctx, PackKey("r1", "gate"), src); err != nil {
		t.Fatalf("first upload after ensureBucketReady failed — write readiness was not gated: %v", err)
	}

	remaining, _, deletes := fake.Snapshot()
	if remaining != 0 {
		t.Errorf("gate returned with %d boot-window PUT failures unconsumed", remaining)
	}
	if deletes == 0 {
		t.Error("readiness scratch key was never deleted")
	}
}

func TestS3_CopyMultipart(t *testing.T) {
	if testing.Short() {
		t.Skip("seaweedfs copy test needs docker")
	}
	ctx := context.Background()
	addr := startSeaweedFS(t, false)
	t.Setenv("AWS_ACCESS_KEY_ID", seaweedFSAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", seaweedFSSecretKey)

	b, err := OpenS3(ctx, fmt.Sprintf("s3://copy-test?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", addr))
	if err != nil {
		t.Fatalf("OpenS3: %v", err)
	}
	defer b.Close()
	ensureBucketReady(t, b)

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
	// previously stored version). This is the last thing the test does, on
	// a throwaway bucket: SeaweedFS data buckets must otherwise never see
	// versioning (seaweedfs#8073 avoidance), and no data operation runs
	// after the status flips.
	versioned, err := b.VersioningEnabled(ctx)
	if err != nil {
		t.Fatalf("VersioningEnabled: %v", err)
	}
	if versioned {
		t.Fatal("fresh SeaweedFS bucket reported as versioned")
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

// TestS3_ExpectedBucketOwnerHarmlessOnSeaweedFS (issue #58): SeaweedFS —
// like the other self-hosted stores and emulators — ignores the
// bucket-owner condition (no global bucket namespace), so with the env var
// SET every operation must keep succeeding — the header is harmless
// pass-through on self-hosted stacks — and presigned URLs must stay
// redeemable because the hoisted x-amz-expected-bucket-owner query
// parameter is signature-covered, which the tamper probe proves live:
// stripping it breaks SigV4 validation. The container therefore runs with
// auth enforcement on (static s3.config identity) — anonymous mode accepts
// unsigned requests, which would make the tamper probe vacuous.
// Enforcement of the condition itself cannot be integration-tested here
// (the self-hosted stores ignore it); the stub-transport unit tests pin
// the threading instead.
func TestS3_ExpectedBucketOwnerHarmlessOnSeaweedFS(t *testing.T) {
	if testing.Short() {
		t.Skip("seaweedfs expected-owner test needs docker")
	}
	ctx := context.Background()
	addr := startSeaweedFS(t, true)
	t.Setenv("AWS_ACCESS_KEY_ID", seaweedFSAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", seaweedFSSecretKey)
	t.Setenv(EnvExpectedBucketOwner, "111122223333")

	b, err := OpenS3(ctx, fmt.Sprintf("s3://owner-test?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", addr))
	if err != nil {
		t.Fatalf("OpenS3: %v", err)
	}
	defer b.Close()
	if b.expectedOwner != "111122223333" {
		t.Fatalf("OpenS3 expectedOwner = %q, env not threaded", b.expectedOwner)
	}
	ensureBucketReady(t, b)

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
