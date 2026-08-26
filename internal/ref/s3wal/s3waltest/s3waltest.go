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
	"net/http"
	"net/url"
	"strings"
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
	SeaweedFSImage   = "chrislusf/seaweedfs:4.42"
	MinIOLegacyImage = "minio/minio:RELEASE.2024-01-16T16-07-38Z"
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
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        SeaweedFSImage,
			ExposedPorts: []string{"8333/tcp"},
			Cmd:          []string{"server", "-s3", "-dir=/data", "-ip.bind=0.0.0.0"},
			WaitingFor:   wait.ForListeningPort("8333/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
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
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
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
// proves WRITE readiness with a scratch PutObject/DeleteObject roundtrip.
// Bucket existence alone is not a write-readiness signal: SeaweedFS
// serves CreateBucket from the filer while object PUTs need a writable
// volume assignment from the master, which returns 500 InternalError
// until the volume server has registered and volumes are grown (issue
// #165). The qualification lanes issue their first object write on
// deliberately no-retry CAS clients whose ambiguous post-send 5xx
// outcome is fatal by design, so the harness must absorb the boot window
// here instead of handing out a bucket that cannot yet be written.
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
	// Write-readiness gate. The scratch key lives outside every
	// store-scanned prefix (`_wal/...`) and carries a fresh nonce so
	// concurrent harnesses sharing an external emulator bucket never
	// collide. The PUT/DELETE pair retries as a unit under the same
	// deadline so a transient boot 500 on the delete cannot leak the key.
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
