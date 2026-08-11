// Package blob provides immutable pack-object storage: the BlobStore
// contract plus the S3/MinIO implementation (gocloud.dev s3blob).
package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	gcblob "gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("blob assertion failed: "+format, args...))
	}
}

// BlobStore is the pack-object storage contract. Keys are opaque to callers
// except through PackKey/PackPrefix. Objects are immutable once written.
type BlobStore interface {
	// UploadFile streams a local file to key.
	UploadFile(ctx context.Context, key, path string) error
	// DownloadFile fetches key into destPath atomically (tmp + rename), so a
	// partially downloaded object is never visible at destPath.
	DownloadFile(ctx context.Context, key, destPath string) error
	// List returns all objects under prefix.
	List(ctx context.Context, prefix string) ([]ListedObject, error)
	// EnsureBucket idempotently creates the backing bucket.
	EnsureBucket(ctx context.Context) error
	Close() error
}

// ListedObject is one blob under a prefix.
type ListedObject struct {
	Key  string
	Size int64
}

// PackKey is the immutable object key for a repo's pack with the given id.
func PackKey(repo, id string) string {
	assertf(repo != "" && id != "", "PackKey(%q, %q)", repo, id)
	return "packs/" + repo + "/pack-" + id + ".pack"
}

// PackPrefix is the listing prefix for a repo's packs.
func PackPrefix(repo string) string { return "packs/" + repo + "/" }

// S3 implements BlobStore on a gocloud S3 bucket plus the raw client
// (needed for EnsureBucket at serve startup).
type S3 struct {
	bucket *gcblob.Bucket
	client *s3.Client
	name   string
}

var _ BlobStore = (*S3)(nil)

// OpenS3 opens a blob URL, e.g.
// s3://spike?endpoint=http://127.0.0.1:9000&s3ForcePathStyle=true&region=us-east-1
// Credentials come from the standard AWS env vars. The URL is parsed by hand
// (bucket = host, endpoint/region = query) so we control path-style behavior
// deterministically against MinIO regardless of gocloud URL-param renames.
func OpenS3(ctx context.Context, blobURL string) (*S3, error) {
	assertf(blobURL != "", "OpenS3: empty blob URL")
	u, err := url.Parse(blobURL)
	if err != nil {
		return nil, fmt.Errorf("parse blob URL: %w", err)
	}
	if u.Scheme != s3blob.Scheme {
		return nil, fmt.Errorf("blob URL scheme %q unsupported (want s3)", u.Scheme)
	}
	bucketName := u.Host
	if bucketName == "" {
		return nil, errors.New("blob URL has no bucket name")
	}
	q := u.Query()
	region := q.Get("region")
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	endpoint := q.Get("endpoint")
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		// MinIO needs path-style addressing; harmless otherwise.
		o.UsePathStyle = true
	})
	bucket, err := s3blob.OpenBucket(ctx, client, bucketName, nil)
	if err != nil {
		return nil, fmt.Errorf("open bucket %q: %w", bucketName, err)
	}
	return &S3{bucket: bucket, client: client, name: bucketName}, nil
}

func (b *S3) Close() error { return b.bucket.Close() }

// EnsureBucket creates the bucket, tolerating "already exists/owned by you"
// (so `serve` works against a fresh MinIO).
func (b *S3) EnsureBucket(ctx context.Context) error {
	assertf(b.client != nil, "EnsureBucket: S3 opened without a client")
	_, err := b.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b.name)})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			code := apiErr.ErrorCode()
			if code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
				return nil
			}
		}
		return fmt.Errorf("create bucket %q: %w", b.name, err)
	}
	return nil
}

// UploadFile streams a local file to key.
func (b *S3) UploadFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w, err := b.bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, f); err != nil {
		_ = w.Close()
		return fmt.Errorf("upload %s: %w", key, err)
	}
	return w.Close()
}

// DownloadFile fetches key into destPath (via tmp + rename so a partially
// downloaded pack is never visible under objects/pack).
func (b *S3) DownloadFile(ctx context.Context, key, destPath string) error {
	r, err := b.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("download %s: %w", key, err)
	}
	defer r.Close()
	tmp, err := os.CreateTemp(dirOf(destPath), ".blob-download-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return fmt.Errorf("download %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), destPath)
}

func dirOf(p string) string {
	i := strings.LastIndexByte(p, '/')
	assertf(i > 0, "dirOf(%q): need an absolute-ish path", p)
	return p[:i]
}

// List returns all objects under prefix.
func (b *S3) List(ctx context.Context, prefix string) ([]ListedObject, error) {
	iter := b.bucket.List(&gcblob.ListOptions{Prefix: prefix})
	var out []ListedObject
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, ListedObject{Key: obj.Key, Size: obj.Size})
	}
}
