// Package s3waltest carries the shared s3wal test fixtures: the pinned
// SeaweedFS/MinIO testcontainers, raw path-style S3 client construction,
// bucket bootstrap/cleanup helpers, and the Phase 1a {seq, nonce} payload
// codec. It deliberately does NOT import package s3wal — s3wal's own
// internal tests import these fixtures, and an s3waltest that imported
// s3wal back would be an import cycle in that test binary. Callers build
// s3wal.Client / run s3wal.Probe themselves from the raw client returned
// here.
package s3waltest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Store image pins. SeaweedFS is the self-hosted store of record (plan
// §5-R7); the MinIO pin is the repo's legacy CI image, kept in the
// qualification matrix only to document its conditional-write behavior.
const (
	SeaweedFSImage   = "chrislusf/seaweedfs:4.45"
	MinIOLegacyImage = "minio/minio:RELEASE.2024-01-16T16-07-38Z"
	// FlociImage pins the Floci row of the qualification matrix (issues
	// #205, #212): the nightly lane runs the 22 qualification surfaces plus
	// get-under-overwrite in REQUIRE mode against this build, so bumping
	// the pin is how a new Floci build gets re-qualified and re-probed for
	// the remaining documented gap (DeleteObject+If-Match ignored — issue
	// #201). nightly-09012026 (image sha256:3d415bb82d47…, upstream
	// revision f2b4c084) is the first published build carrying the
	// GetObject torn-read fix floci-io/floci#2874 (plus its follow-up
	// #2901); no release tag contains that fix yet — 2.0.1 was cut 25 min
	// before it merged.
	// TODO(#212): move to the first release tag after 2.0.1 once upstream
	// cuts one (verify `git tag --contains b4aab999` in a floci clone).
	FlociImage = "floci/floci:nightly-09012026"
)

// SeaweedFS's `weed server -s3` anonymous mode accepts any static
// credential pair; every fixture client signs with these.
const (
	SeaweedFSAccessKey = "seaweedfs"
	SeaweedFSSecretKey = "seaweedfs"
)

// StartSeaweedFSContainer runs the pinned SeaweedFS image (`weed server
// -s3`: master + volume + filer + S3 gateway in one process; versioning
// and object-lock never enabled — seaweedfs#8073 avoidance) and returns
// the S3 endpoint URL plus a terminate func. The gateway components start
// asynchronously after the port listens — and a created bucket still has
// no writable volume for a while (issue #165) — so callers must gate on
// EnsureBucket, whose write-readiness roundtrip is the readiness signal.
func StartSeaweedFSContainer(ctx context.Context) (endpoint string, terminate func(), err error) {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Image:        SeaweedFSImage,
		ExposedPorts: []string{"8333/tcp"},
		Cmd:          []string{"server", "-s3", "-dir=/data", "-ip.bind=0.0.0.0"},
		WaitingFor:   wait.ForListeningPort("8333/tcp").WithStartupTimeout(2 * time.Minute),
		Started:      true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("start seaweedfs container: %w", err)
	}
	terminate = func() { _ = c.Terminate(context.Background()) }
	host, err := c.Host(ctx)
	if err != nil {
		terminate()
		return "", nil, fmt.Errorf("seaweedfs host: %w", err)
	}
	port, err := c.MappedPort(ctx, "8333/tcp")
	if err != nil {
		terminate()
		return "", nil, fmt.Errorf("seaweedfs mapped port: %w", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port()), terminate, nil
}

// StartSeaweedFS is the testing-scoped convenience: the container
// terminates with tb.
func StartSeaweedFS(tb testing.TB) string {
	tb.Helper()
	endpoint, terminate, err := StartSeaweedFSContainer(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(terminate)
	return endpoint
}

// StartFloci runs the pinned Floci image with the S3 service in its
// default non-enforcing auth mode: the data plane then accepts any static
// SigV4 credential pair, so the lane needs no IAM provisioning (the only
// vendored AWS service clients are s3 and sts). That is the right shape
// for the qualification matrix, which probes data-plane semantics —
// Floci's header-auth enforcement validates only key-ID existence anyway
// (issue #201), and the SigV4/IAM enforcement path already has its own
// nightly lane (floci-nightly.yml's blob job). Callers must still gate on
// EnsureBucket for write readiness.
func StartFloci(tb testing.TB) string {
	tb.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Image:        FlociImage,
		ExposedPorts: []string{"4566/tcp"},
		WaitingFor:   wait.ForListeningPort("4566/tcp").WithStartupTimeout(2 * time.Minute),
		Started:      true,
	})
	if err != nil {
		tb.Fatalf("start floci container: %v", err)
	}
	tb.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, err := c.Host(ctx)
	if err != nil {
		tb.Fatalf("floci host: %v", err)
	}
	port, err := c.MappedPort(ctx, "4566/tcp")
	if err != nil {
		tb.Fatalf("floci mapped port: %v", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// StartMinIOLegacy runs the repo's legacy MinIO CI pin exactly like
// internal/blob/minio_test.go does.
func StartMinIOLegacy(tb testing.TB) (endpoint, user, pass string) {
	tb.Helper()
	ctx := context.Background()
	minioC, err := tcminio.Run(ctx, MinIOLegacyImage)
	if err != nil {
		tb.Fatalf("start minio container: %v", err)
	}
	tb.Cleanup(func() { _ = minioC.Terminate(context.Background()) })
	addr, err := minioC.ConnectionString(ctx)
	if err != nil {
		tb.Fatalf("minio connection string: %v", err)
	}
	return "http://" + addr, minioC.Username, minioC.Password
}

// HTTPClient is shared by every fixture S3 client. The SDK default
// transport keeps only a handful of idle connections per host; the 60 s
// contention campaign (writers=32) churns thousands of short-lived
// connections through one endpoint, piling up TIME_WAIT sockets until the
// kernel runs out of ephemeral ports and dials fail pre-send. Pooling well
// above the maximum harness concurrency keeps connections reused.
// (s3wal.New derives its no-retry client from Options(), so the pooled
// client propagates to CAS calls too.)
var HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
	t.MaxIdleConns = 256
	t.MaxIdleConnsPerHost = 256
})

// RawS3Client builds a path-style SigV4 client with static credentials —
// the same wiring shape as internal/blob's s3ClientOptions, but with
// explicit credentials so container lanes never depend on process env.
func RawS3Client(endpoint, region, accessKey, secretKey string) *s3.Client {
	return RawS3ClientSession(endpoint, region, accessKey, secretKey, "")
}

// RawS3ClientSession is RawS3Client for STS session credentials: temporary
// keys must send their session token (X-Amz-Security-Token) alongside the
// SigV4 signature or an enforcing endpoint rejects the request. Used by the
// nightly Floci githttp lane, whose credentials come from IAM
// CreateAccessKey → STS AssumeRole rather than a static emulator identity.
func RawS3ClientSession(endpoint, region, accessKey, secretKey, sessionToken string) *s3.Client {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken),
		HTTPClient:  HTTPClient,
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

// EnsureBucket creates the bucket, retrying while the store boots (the
// SeaweedFS S3 gateway accepts TCP before the filer is ready) and
// tolerating already-owned rejections, verifies it with HeadBucket, then
// proves WRITE readiness via WaitWriteReady — the harness must absorb the
// store's boot window instead of handing out a bucket that cannot yet be
// written (issue #165).
func EnsureBucket(ctx context.Context, client *s3.Client, bucket string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		if err == nil {
			break
		}
		var owned *s3types.BucketAlreadyOwnedByYou
		var exists *s3types.BucketAlreadyExists
		if errors.As(err, &owned) || errors.As(err, &exists) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("CreateBucket(%s) never succeeded (store not ready?): %w", bucket, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return fmt.Errorf("HeadBucket(%s) after create: %w", bucket, err)
	}
	return WaitWriteReady(ctx, client, bucket)
}

// WaitWriteReady proves the bucket accepts OBJECT WRITES with a retried
// scratch PutObject/DeleteObject roundtrip (60 s bound). Bucket existence
// alone is not a write-readiness signal: SeaweedFS serves CreateBucket
// from the filer while object PUTs need a writable volume assignment from
// the master, which returns 500 InternalError until the volume server has
// registered and volumes are grown (issues #165, #177). The scratch key
// lives outside every store-scanned prefix (`_wal/...`, `packs/...`,
// `lfs/...`) and carries a fresh nonce so concurrent harnesses sharing an
// external emulator bucket never collide. The PUT/DELETE pair retries as
// a unit so a transient boot 500 on the delete cannot leak the key.
// Shared by every SeaweedFS-booting harness: the s3wal qualification
// lanes (whose no-retry CAS clients turn one boot 500 into a fatal
// ambiguous verdict), the internal/blob container tests (whose SDK
// 3-attempt budget can be exhausted inside the boot window), and the
// githttp e2e lanes.
func WaitWriteReady(ctx context.Context, client *s3.Client, bucket string) error {
	deadline := time.Now().Add(60 * time.Second)
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("readiness nonce: %w", err)
	}
	key := fmt.Sprintf("harness-readiness/%s", hex.EncodeToString(nonce))
	for {
		err := func() error {
			if _, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("s3waltest write-readiness probe"),
			}); err != nil {
				return fmt.Errorf("readiness PutObject(%s/%s): %w", bucket, key, err)
			}
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			}); err != nil {
				return fmt.Errorf("readiness DeleteObject(%s/%s): %w", bucket, key, err)
			}
			return nil
		}()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("write-readiness roundtrip never succeeded (no writable volume?): %w", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// MustEnsureBucket is EnsureBucket for tests.
func MustEnsureBucket(tb testing.TB, client *s3.Client, bucket string) {
	tb.Helper()
	if err := EnsureBucket(context.Background(), client, bucket); err != nil {
		tb.Fatal(err)
	}
}

// OpenExternalS3 opens an externally provided endpoint from a blob-style
// URL (s3://bucket?endpoint=…&region=…&s3ForcePathStyle=true; credentials
// from the standard AWS env). Real-endpoint safety follows
// internal/blob/s3endpoint_test.go: buckets are created only on emulator
// endpoints (endpoint= present); against real S3 the bucket must already
// exist (HeadBucket only — never CreateBucket, which can reset ACLs on an
// owned us-east-1 bucket), and versioned buckets are refused because the
// per-key DeleteObject cleanup would silently accumulate noncurrent
// versions.
func OpenExternalS3(tb testing.TB, blobURL string) (*s3.Client, string) {
	tb.Helper()
	ctx := context.Background()
	u, err := url.Parse(blobURL)
	if err != nil {
		tb.Fatalf("parse %s: %v", blobURL, err)
	}
	if u.Scheme != "s3" || u.Host == "" {
		tb.Fatalf("external URL %q must be s3://bucket?endpoint=…&region=…", blobURL)
	}
	bucket := u.Host
	q := u.Query()
	region := q.Get("region")
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithHTTPClient(HTTPClient))
	if err != nil {
		tb.Fatalf("aws config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if ep := q.Get("endpoint"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
		o.UsePathStyle = true
	})
	if q.Get("endpoint") != "" {
		MustEnsureBucket(tb, client, bucket)
	} else if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		tb.Fatalf("bucket %q must already exist on real S3 (refusing to create or mutate it): %v", bucket, err)
	}
	ver, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		tb.Fatalf("GetBucketVersioning(%s): %v", bucket, err)
	}
	if ver.Status == s3types.BucketVersioningStatusEnabled || ver.Status == s3types.BucketVersioningStatusSuspended {
		tb.Fatalf("bucket %q has versioning enabled or suspended; the harness relies on DeleteObject cleanup — use an unversioned scratch bucket", bucket)
	}
	return client, bucket
}

// DeleteAllPrefix lists then deletes every key under prefix, reporting
// real deletion failures so a PASS can never silently leak objects into a
// user-supplied bucket.
func DeleteAllPrefix(tb testing.TB, client *s3.Client, bucket, prefix string) {
	tb.Helper()
	if prefix == "" {
		tb.Fatal("DeleteAllPrefix: empty prefix would delete the whole bucket")
	}
	ctx := context.Background()
	var keys []string
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			tb.Errorf("cleanup list %s: %v", prefix, err)
			return
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	for _, key := range keys {
		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}); err != nil {
			tb.Errorf("cleanup delete %s: %v", key, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Deterministic boot-window fault injection.

// BootingFakeS3 models the SeaweedFS boot window behind issues #165/#177
// as a plain HTTP handler: bucket operations succeed immediately
// (CreateBucket/HeadBucket run on the filer alone) while the first
// PutFailures OBJECT PUTs fail with the store's literal 500 InternalError
// (no writable volume assigned yet). The failure count is deterministic,
// so the boot-window flake reproduces on every run instead of one CI run
// in dozens. Zero value is ready to use; set PutFailures before serving.
type BootingFakeS3 struct {
	mu          sync.Mutex
	PutFailures int // object PUTs left to fail with 500
	objectPuts  int
	deletes     int
}

// Handler serves the fake: bucket PUT/HEAD → 200, object PUT → 500 while
// PutFailures remain (then 200 with an ETag), object DELETE → 204,
// anything else → 400.
func (f *BootingFakeS3) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucketOnly := !strings.Contains(strings.TrimPrefix(r.URL.Path, "/"), "/")
		switch {
		case bucketOnly && (r.Method == http.MethodPut || r.Method == http.MethodHead):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			f.mu.Lock()
			f.objectPuts++
			failing := f.PutFailures > 0
			if failing {
				f.PutFailures--
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
			w.WriteHeader(http.StatusBadRequest)
		}
	})
}

// Snapshot returns (remaining put failures, object puts served, deletes
// served) under the lock, so assertions never race late keep-alive
// handler goroutines.
func (f *BootingFakeS3) Snapshot() (putFailures, objectPuts, deletes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.PutFailures, f.objectPuts, f.deletes
}

// ---------------------------------------------------------------------------
// {seq, nonce} payload codec — the Phase 1a stand-in for the manifest:
// every CAS body carries a strictly monotonic seq plus a fresh random
// nonce so distinct logical versions are byte-distinguishable.

// Payload is one {seq, nonce} CAS body.
type Payload struct {
	Seq   int64  `json:"seq"`
	Nonce string `json:"nonce"`
	Fill  string `json:"fill,omitempty"`
}

// NewNonce returns 128 bits of crypto-random hex.
func NewNonce(tb testing.TB) string {
	tb.Helper()
	return RandomHex(tb, 16)
}

// RandomHex returns n crypto-random bytes hex-encoded so keys stay unique
// across reruns against a shared long-lived bucket.
func RandomHex(tb testing.TB, n int) string {
	tb.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		tb.Fatal(err)
	}
	return hex.EncodeToString(buf)
}

// EncodePayloadNonce marshals a {seq, nonce} payload padded to roughly
// size bytes (never below the bare encoding).
func EncodePayloadNonce(tb testing.TB, seq int64, nonce string, size int) []byte {
	tb.Helper()
	p := Payload{Seq: seq, Nonce: nonce}
	base, err := json.Marshal(p)
	if err != nil {
		tb.Fatalf("marshal payload: %v", err)
	}
	if pad := size - len(base) - len(`,"fill":""`); pad > 0 {
		p.Fill = strings.Repeat("x", pad)
	}
	out, err := json.Marshal(p)
	if err != nil {
		tb.Fatalf("marshal payload: %v", err)
	}
	return out
}

// EncodePayload is EncodePayloadNonce with a fresh nonce.
func EncodePayload(tb testing.TB, seq int64, size int) []byte {
	tb.Helper()
	return EncodePayloadNonce(tb, seq, NewNonce(tb), size)
}

// DecodePayload parses one payload body (ok=false on foreign bytes).
func DecodePayload(body []byte) (Payload, bool) {
	var p Payload
	if err := json.Unmarshal(body, &p); err != nil || p.Nonce == "" {
		return Payload{}, false
	}
	return p, true
}

// DecodeSeq is the s3wal Client.DecodeSeq hook for payload bodies: it arms
// the §2.3 read-side seq-monotonicity assert in test/bench clients.
func DecodeSeq(body []byte) (int64, bool) {
	p, ok := DecodePayload(body)
	return p.Seq, ok
}

// EnvTestS3WALURL names an external S3 endpoint the qualification lane
// runs against (same URL grammar as CODESTORAGE_TEST_S3_URL in
// internal/blob; distinct variable so blob-package and s3wal runs never
// collide on cleanup). Unset skips the lane.
const EnvTestS3WALURL = "CODESTORAGE_TEST_S3WAL_URL"
