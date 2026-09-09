// ConditionalStore is the seam between the manifest driver (package ref's
// s3wal store) and its CAS substrate: the real conditional S3 client
// (*Client) in production, the fault-injecting in-memory fake (*MemCAS) in
// unit/DST tests. Byte-ordered List is part of the contract — fleet scans
// (§2.9) depend on it.
package s3wal

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/coder/openstorage/internal/envconfig"
)

// ConditionalStore is the conditional-object surface the manifest driver
// consumes (§2.3 taxonomy: every method returns the casobj.go sentinels /
// *AmbiguousError classes).
type ConditionalStore interface {
	// Get fetches key; ErrNotExist reports a missing object.
	Get(ctx context.Context, key string) (Object, error)
	// GetIfChanged fetches key unless its ETag still matches (304 ⇒
	// ErrNotModified) — the cheap generation-poll path.
	GetIfChanged(ctx context.Context, key, etag string) (Object, error)
	// Create writes key iff it does not exist (If-None-Match: *).
	Create(ctx context.Context, key string, body []byte) (string, error)
	// Advance overwrites key iff etag is current (If-Match).
	Advance(ctx context.Context, key string, body []byte, etag string) (string, error)
	// Delete removes key unconditionally (§2.7: never conditional).
	Delete(ctx context.Context, key string) error
	// List returns up to limit keys under prefix, strictly byte-ordered,
	// strictly greater than startAfter ("" = from the beginning).
	List(ctx context.Context, prefix, startAfter string, limit int) ([]string, error)
	// StoreDate returns the store wall clock freshest observed on this
	// client's responses (the HTTP Date header, 1s resolution), the
	// local monotonic-anchored instant of the observation, and whether
	// any observation exists — the §2.4-3 absolute clock witness that
	// destructive TTL verdicts cross-check against the local clock.
	StoreDate() (date, observedAt time.Time, ok bool)
}

var _ ConditionalStore = (*Client)(nil)

// List pages keys under prefix in S3's lexicographic (byte) order via
// ListObjectsV2 — the §2.9 fleet-scan primitive. startAfter is exclusive.
func (c *Client) List(ctx context.Context, prefix, startAfter string, limit int) ([]string, error) {
	assertf(prefix != "", "List: empty prefix")
	assertf(limit > 0, "List: non-positive limit %d", limit)
	in := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}
	if startAfter != "" {
		in.StartAfter = aws.String(startAfter)
	}
	p := s3.NewListObjectsV2Paginator(c.s3, in)
	var keys []string
	for p.HasMorePages() && len(keys) < limit {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3wal: list %s after %q: %w", prefix, startAfter, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			assertf(key > startAfter, "list %s: store returned key %q not after cursor %q", prefix, key, startAfter)
			keys = append(keys, key)
			if len(keys) == limit {
				break
			}
		}
	}
	return keys, nil
}

// Close releases pooled transport resources when this Client owns them
// (Dial-built clients); Clients wrapped around caller-owned s3.Clients
// close nothing.
func (c *Client) Close() {
	if c.httpCloser != nil {
		c.httpCloser.CloseIdleConnections()
	}
}

// envExpectedBucketOwner is blob.EnvExpectedBucketOwner's twin (issue #58;
// see internal/blob/blob.go for the full rationale): the SAME environment
// variable pins every S3 request to a 12-digit bucket-owner account ID,
// and the conditional client must enforce it too (PR #168 security
// review) — the _wal/ authority objects (manifests, meta.json carrying
// the org public keys, the instance lease, scan state) are exactly what a
// deleted-and-re-registered bucket name could otherwise swap out from
// under a role that was granted access by the squatter. Read here in the
// package, not in cmd config, so every dialer — serve's ref and meta
// stores alike — inherits enforcement uniformly. Empty (the default)
// attaches no middleware and changes no behavior.
const envExpectedBucketOwner = "OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER"

// expectedOwnerHeader is the AWS bucket-owner condition header. Unlike
// internal/blob there is no x-amz-source-expected-bucket-owner spelling:
// this client issues no copy-source requests (no CopyObject or
// UploadPartCopy).
const expectedOwnerHeader = "x-amz-expected-bucket-owner"

// awsAccountIDPattern mirrors blob.go: the only valid non-empty owner is
// exactly 12 digits. A typo'd ID would 403 every operation on real AWS,
// so misconfiguration is boot-fatal instead (crash-and-burn).
var awsAccountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)

// expectedBucketOwnerFromEnv reads and validates envExpectedBucketOwner.
func expectedBucketOwnerFromEnv() (string, error) {
	owner := strings.TrimSpace(envconfig.Getenv(envExpectedBucketOwner))
	if owner != "" && !awsAccountIDPattern.MatchString(owner) {
		return "", fmt.Errorf("%s must be a 12-digit AWS account ID, got %q", envExpectedBucketOwner, owner)
	}
	return owner, nil
}

// expectedBucketOwnerAPIOption stamps the bucket-owner condition onto
// EVERY request this client emits — the same structurally all-or-nothing
// client seam as internal/blob (a future call site cannot forget to
// thread it). The Build step runs after serialization and before SigV4
// signing, so the header is signed. New's derived no-retry client copies
// the source client's options, so the middleware survives re-derivation.
func expectedBucketOwnerAPIOption(owner string) func(*middleware.Stack) error {
	assertf(awsAccountIDPattern.MatchString(owner), "expectedBucketOwnerAPIOption(%q)", owner)
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("s3walExpectedBucketOwner",
			func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
				req, ok := in.Request.(*smithyhttp.Request)
				assertf(ok, "expected-bucket-owner middleware: request type %T", in.Request)
				req.Header.Set(expectedOwnerHeader, owner)
				return next.HandleBuild(ctx, in)
			}), middleware.After)
	}
}

// Dial opens an s3wal store URL and returns the conditional CAS client for
// its bucket, with the manifest seq watermark armed:
//
//	s3wal://bucket?endpoint=http://127.0.0.1:8333&region=us-east-1
//
// The URL grammar mirrors blob.OpenS3 (bucket = host, endpoint/region =
// query, credentials from the standard AWS env chain, path-style always on
// for self-hosted stores); the legacy s3ForcePathStyle query key is
// accepted and ignored for symmetry. The optional expected-bucket-owner
// enforcement comes from envExpectedBucketOwner — the same knob as
// internal/blob, and a malformed value is boot-fatal here too. Dial
// performs no network I/O — probe and reachability run at InitSchema
// (ADR-12: Open never runs DDL).
func Dial(ctx context.Context, rawURL string) (*Client, error) {
	assertf(rawURL != "", "Dial: empty s3wal URL")
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse s3wal URL: %w", err)
	}
	if u.Scheme != "s3wal" {
		return nil, fmt.Errorf("s3wal URL scheme %q unsupported (want s3wal)", u.Scheme)
	}
	bucket := u.Host
	if bucket == "" {
		return nil, fmt.Errorf("s3wal URL has no bucket name")
	}
	expectedOwner, err := expectedBucketOwnerFromEnv()
	if err != nil {
		return nil, err
	}
	q := u.Query()
	region := q.Get("region")
	if region == "" {
		region = "us-east-1"
	}
	// A pooled transport shared by every conditional call: manifest CAS is
	// many small requests against one endpoint, so connection reuse — not
	// the SDK default's handful of idle conns — sets the latency floor.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 64
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		return nil, fmt.Errorf("s3wal aws config: %w", err)
	}
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if ep := q.Get("endpoint"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
		o.UsePathStyle = true
		if expectedOwner != "" {
			o.APIOptions = append(o.APIOptions, expectedBucketOwnerAPIOption(expectedOwner))
		}
	})
	c := New(s3c, bucket)
	c.DecodeSeq = DecodeSeq
	c.httpCloser = transport
	return c, nil
}
