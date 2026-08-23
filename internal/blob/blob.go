// Package blob provides immutable pack-object storage: the BlobStore
// contract plus the S3/MinIO implementation (gocloud.dev s3blob).
package blob

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	gcblob "gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

// ErrSignedURLUnsupported is returned by SignedURL on backends without
// presign support (fileblob). Callers (the LFS batch handler) fall back to
// proxying the transfer through the server.
var ErrSignedURLUnsupported = errors.New("blob: signed URLs unsupported by this backend")

// ErrNotFound is returned (wrapped, match with errors.Is) by Stat and
// DownloadFile for missing objects, so callers can distinguish absence from
// transport failures without depending on gocloud error codes.
var ErrNotFound = errors.New("blob: object not found")

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
	// partially downloaded object is never visible at destPath. Missing keys
	// yield an error matching ErrNotFound via errors.Is (the hydration
	// missing-pack retry keys on it).
	DownloadFile(ctx context.Context, key, destPath string) error
	// List returns all objects under prefix.
	List(ctx context.Context, prefix string) ([]ListedObject, error)
	// ListPages streams objects under prefix in lexicographic key order,
	// invoking fn once per page of at most pageSize objects, so callers
	// scanning large prefixes (the orphan-pack sweeper) hold one page at a
	// time instead of the whole listing. fn must not retain the slice; a
	// non-nil fn error aborts the scan and is returned unwrapped.
	ListPages(ctx context.Context, prefix string, pageSize int, fn func([]ListedObject) error) error
	// Delete removes the object at key; missing objects yield an error
	// matching ErrNotFound via errors.Is. Objects are immutable, so Delete
	// exists solely for the orphan-pack sweeper (issue #5): reaping blobs
	// that were uploaded but never registered because of a crash.
	Delete(ctx context.Context, key string) error
	// Copy duplicates srcKey to dstKey inside the store (server-side S3
	// CopyObject / local file copy — never staged through local disk, so
	// pack-sized copies work regardless of temp-dir capacity). A missing
	// srcKey yields an error matching ErrNotFound via errors.Is. Used by
	// the orphan-pack sweeper's quarantine handshake.
	Copy(ctx context.Context, dstKey, srcKey string) error
	// SignedURL returns a presigned URL for the given HTTP method
	// (http.MethodGet or http.MethodPut) on key, valid for expiry.
	// Backends without presign support return ErrSignedURLUnsupported.
	SignedURL(ctx context.Context, key, method string, expiry time.Duration) (string, error)
	// Stat returns the size of the object at key; missing objects yield an
	// error matching ErrNotFound via errors.Is.
	Stat(ctx context.Context, key string) (int64, error)
	// ReadRange returns exactly the length bytes at offset from the object
	// at key — a ranged point read, never the whole object (repack fold
	// selection reads exact object counts from 12-byte pack headers with
	// it). Missing keys yield an error matching ErrNotFound via errors.Is;
	// an object shorter than offset+length is a non-ErrNotFound error.
	ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	// EnsureBucket idempotently creates the backing bucket.
	EnsureBucket(ctx context.Context) error
	Close() error
}

// assertSignedURLArgs fail-fast-validates the shared SignedURL contract:
// non-empty key, GET or PUT (the only methods LFS transfers need), and a
// positive expiry.
func assertSignedURLArgs(key, method string, expiry time.Duration) {
	assertf(key != "", "SignedURL: empty key")
	assertf(method == http.MethodGet || method == http.MethodPut,
		"SignedURL(%q): method %q (want GET or PUT)", key, method)
	assertf(expiry > 0, "SignedURL(%s %s): non-positive expiry %v", method, key, expiry)
}

// ListedObject is one blob under a prefix.
type ListedObject struct {
	Key  string
	Size int64
	// ModTime is the backend's last-modified time for the object (S3
	// LastModified / file mtime). The orphan-pack sweeper uses it as the
	// upload age: objects are immutable, so it never moves after publish.
	ModTime time.Time
}

// PackKey is the immutable object key for a repo's pack with the given id.
func PackKey(repo, id string) string {
	assertf(repo != "" && id != "", "PackKey(%q, %q)", repo, id)
	return "packs/" + repo + "/pack-" + id + ".pack"
}

// PacksRoot is the listing prefix covering every repo's packs (the
// orphan-pack sweeper scans it in one listing instead of enumerating
// repos). Namespaced repos (`<repo>+import`/`<repo>+ephemeral`) keep fully
// separate pack spaces keyed by the full name, so each key's repo segment
// identifies its authoritative pack registry row set.
const PacksRoot = "packs/"

// PackPrefix is the listing prefix for a repo's packs.
func PackPrefix(repo string) string { return PacksRoot + repo + "/" }

// packUsagePageSize bounds one StorageUsageBytes listing page, mirroring the
// sweeper's page-bounded scans so a pathological pack count cannot balloon
// memory.
const packUsagePageSize = 1000

// packNamespaceSuffixes are the docs/explanation/git-protocol-and-namespaces.md namespace suffixes a repo key may
// carry. Each namespace has its own pack PREFIX (storage isolation), but
// quota accounting rolls all of them up under the base repo: auth strips
// the suffix, so one git:write token can push to base, +ephemeral, and
// +import — separate quotas would hand that token 3x the advertised
// per-repository bound (issue #21, Codex round 15).
var packNamespaceSuffixes = []string{"+ephemeral", "+import"}

// QuotaRepoGroup returns repo's quota accounting group: the BASE repository
// first (suffix stripped), then every docs/explanation/git-protocol-and-namespaces.md namespace that rolls up under
// it. The ref-store quota ledger (issue #29) keys reservations under
// group[0] and sums usage over the whole group, mirroring StorageUsageBytes'
// rollup so both usage sources measure the same unit.
func QuotaRepoGroup(repo string) []string {
	assertf(repo != "", "QuotaRepoGroup: empty repo")
	base := repo
	for _, s := range packNamespaceSuffixes {
		base = strings.TrimSuffix(base, s)
	}
	assertf(base != "", "QuotaRepoGroup: empty base repo for %q", repo)
	group := make([]string, 0, 1+len(packNamespaceSuffixes))
	group = append(group, base)
	for _, s := range packNamespaceSuffixes {
		group = append(group, base+s)
	}
	return group
}

// StorageUsageBytes sums the sizes of every object under the pack prefixes
// of repo's BASE repository and all of its namespaces PLUS the repo's LFS
// prefix (issue #51): registered packs and LFS objects, not-yet-swept
// orphans, AND retained superseded blobs — exactly the durable growth the
// per-repo storage quota (issue #21) bounds. The suffix, if any, is
// stripped first, so every namespace write is measured against the same
// rolled-up usage. Shared by the pre-receive hook, the REST pack writers,
// the LFS batch/transfer gates, and the git surface's pressure check.
//
// Since issue #29 this listing is the admission check's usage FLOOR rather
// than its sole source: the ref-store ledger (ref.ReservePackBytes) charges
// max(counter, this listing) plus in-flight reservations, closing the
// concurrent-push overshoot this listing alone admitted. Retained
// superseded bytes (issue #31) are counted by BOTH sources — here as
// lingering prefix objects, there as pack_retained rows — until a sweep
// reclaims them. LFS bytes are likewise counted by both once uploads
// settle (ref.RegisterLFSObject); before #51's ledger existed, only this
// listing could see them, which is one more reason the floor stays.
func StorageUsageBytes(ctx context.Context, b BlobStore, repo string) (int64, error) {
	assertf(b != nil && repo != "", "StorageUsageBytes: missing blob store or repo")
	base := QuotaRepoGroup(repo)[0]
	// Three listings cover base + every namespace + LFS without matching
	// sibling repos: "packs/<base>/" is the base pack prefix,
	// "packs/<base>+" only matches this repo's namespace prefixes ('+'
	// cannot start another repo's key segment), and "lfs/<base>/" is the
	// shared LFS object space every namespace rolls up to (ADR-35).
	total, err := usageBytes(ctx, b, repo,
		[]string{PackPrefix(base), PacksRoot + base + "+", LFSPrefix(base)})
	assertf(err != nil || total >= 0, "StorageUsageBytes: negative total %d for %s", total, repo)
	return total, err
}

// Deliberately NO packs-only floor variant for pack admission (issue #51,
// Codex rounds 17–18): omitting the LFS prefix from the pack writers'
// floor let a repository's PRE-LEDGER LFS bytes (rowless, so invisible to
// the usage counter until adopted) admit a full quota of new packs on top.
// The full listing per push is the accepted cost: it is bounded by the
// per-repo LFS object cap (≤ ~101 pages) — the same order as the pack
// prefix listing this path always did — runs once per heavyweight push
// (the per-TRANSFER amplification lives on the LFS surface, which caches),
// and the alternative (a sweeper-reconciled per-repo baseline) requires
// exactly the per-repo sweep state the sweeper's design forbids.

// usageBytes sums object sizes under the given prefixes, page-bounded.
func usageBytes(ctx context.Context, b BlobStore, repo string, prefixes []string) (int64, error) {
	var total int64
	for _, prefix := range prefixes {
		err := b.ListPages(ctx, prefix, packUsagePageSize, func(page []ListedObject) error {
			for _, o := range page {
				total += o.Size
			}
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("list usage for %s: %w", repo, err)
		}
	}
	return total, nil
}

// LFSKey is the immutable object key for a repo's LFS object with the given
// sha256 oid. Keys are repo-scoped (docs/explanation/git-protocol-and-namespaces.md): forks inherit LFS pointers
// only, never the objects, which this keying yields automatically.
func LFSKey(repoID, oid string) string {
	assertf(repoID != "" && oid != "", "LFSKey(%q, %q)", repoID, oid)
	return LFSRoot + repoID + "/" + oid
}

// LFSRoot is the listing prefix covering every repo's LFS objects (the
// orphan sweeper scans it like PacksRoot, issue #51). LFS keys always use
// the BASE RepoID (ADR-35): namespaces share one object space, so there is
// no "+suffix" variant to cover.
const LFSRoot = "lfs/"

// LFSPrefix is the listing prefix for a repo's LFS objects.
func LFSPrefix(repoID string) string {
	assertf(repoID != "", "LFSPrefix: empty repo")
	return LFSRoot + repoID + "/"
}

// EnsurePublished re-uploads the local file at path to key when key is
// absent. It is the writer half of the orphan-sweep coordination (issue
// #5), called strictly AFTER a successful RegisterPacks: registration
// follows upload, so an orphan sweeper that deleted the freshly uploaded
// key just before the registration committed would otherwise leave a
// registered pack with no bytes. Ordering proof: the sweeper deletes (D)
// then re-reads the registry (C); the writer registers (R) then verifies
// here (V). If C observes R the sweeper restores the pack itself; if C ran
// before R then D happened before V, so V observes the missing key and
// re-uploads. Both writers produce identical bytes (pack keys are
// content-addressed), so the overlap is harmless either way.
//
// The verification runs on a bounded context detached from the caller's
// cancellation: the registration it covers has already committed, so a
// client hanging up right after the commit must not skip the check — a
// skipped verification could leave the persistent pack row pointing at a
// blob a concurrent sweep just deleted (Codex round 14).
//
// A restore re-materializes bytes the usage counter already includes
// (Codex round 58): a quota floor scanned during the absence subtracted
// the full counter from a listing missing them, hiding equally sized
// rowless legacy bytes. onRestore (nil-ok) runs BEFORE the repair upload
// so the caller can account the returning bytes first (a cover
// reservation — see the LFS repair in githttp); an onRestore error aborts
// the restore, leaving the registered-row-without-bytes state the next
// retry heals. The returned restored flag tells the caller to invalidate
// cached floors (bump the shared floor generation) after the bytes are
// back.
func EnsurePublished(ctx context.Context, b BlobStore, key, path string, onRestore func(context.Context, int64) error) (restored bool, err error) {
	assertf(b != nil && key != "" && path != "", "EnsurePublished(%q, %q)", key, path)
	fi, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("stat local pack %s: %w", path, err)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ensurePublishedTimeout(fi.Size()))
	defer cancel()
	_, err = b.Stat(ctx, key)
	if errors.Is(err, ErrNotFound) {
		if onRestore == nil {
			if err := b.UploadFile(ctx, key, path); err != nil {
				return false, err
			}
			return true, nil
		}
		if err := onRestore(ctx, fi.Size()); err != nil {
			return false, fmt.Errorf("account restore of %s: %w", key, err)
		}
		// The cover must stay alive from here until the stale-floor tail
		// closes (Codex round 59): this allowance deliberately exceeds
		// the minimum reservation TTL (repairs must finish), so one
		// reservation cannot span the upload. Renew it through the
		// upload and refresh it once after publication — the refresh
		// restarts the TTL clock, which the config floor guarantees
		// outlives the last stale-floor commit (usageCacheTTL + handoff
		// margin past publication).
		renew := func(rctx context.Context) error { return onRestore(rctx, fi.Size()) }
		upErr, renewErr := WithRenewedCover(ctx, renew, func() error {
			return b.UploadFile(ctx, key, path)
		})
		if upErr != nil {
			return false, upErr
		}
		if renewErr != nil {
			// The bytes ARE durably back — report the restore so the
			// caller still invalidates floors — but surface the failed
			// post-publication refresh: the cover's tail coverage now
			// rests on the last mid-upload renewal (at most one renewal
			// interval short of the full guarantee).
			return true, fmt.Errorf("refresh restore cover of %s after publication: %w", key, renewErr)
		}
		return true, nil
	}
	return false, err
}

// coverRenewInterval is WithRenewedCover's renewal cadence (a var for
// tests): strictly under a third of the minimum reservation TTL (61s, see
// ref.MinPackReservationTTL's config gate), so a restore cover stays
// continuously alive even across two consecutive failed or missed
// renewals.
var coverRenewInterval = 20 * time.Second

// WithRenewedCover runs op while keeping a restore's cover reservation
// alive (Codex round 59): renew is invoked every coverRenewInterval for
// op's duration (best-effort — the ledger failing renewals is the same
// ledger competing admissions need) and once more after op succeeds, the
// post-publication refresh whose restarted TTL outlives every floor
// scanned during the absence. The caller takes the initial cover BEFORE
// calling this. opErr reports op's own failure; renewErr reports only the
// final refresh.
func WithRenewedCover(ctx context.Context, renew func(context.Context) error, op func() error) (opErr, renewErr error) {
	assertf(renew != nil && op != nil, "WithRenewedCover: nil renew or op")
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(coverRenewInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = renew(ctx) // best-effort; the final refresh is checked
			}
		}
	}()
	if opErr = op(); opErr != nil {
		return opErr, nil
	}
	return nil, renew(ctx)
}

// ensurePublishedTimeout bounds EnsurePublished's detached verification.
// The bound scales with the pack size — a repair upload MUST be able to
// finish, or the committed registry row keeps pointing at an absent object
// until the next heal — while still guaranteeing a wedged store cannot pin
// request goroutines forever. The floor covers round-trips and small packs;
// the per-byte allowance assumes a worst-case sustained ~1 MiB/s to the
// store, far below any realistic server→store link.
func ensurePublishedTimeout(size int64) time.Duration {
	assertf(size >= 0, "ensurePublishedTimeout: negative size %d", size)
	const perMiB = time.Second
	return 5*time.Minute + time.Duration(size>>20)*perMiB
}

// Open dispatches on the URL scheme to the matching BlobStore backend:
//
//	s3://bucket?region=&endpoint=  → OpenS3
//	file:///abs/dir                → local filesystem bucket
//
// BlobStore already carries the lifecycle (EnsureBucket/Close), so there is
// no separate lifecycle interface. Open never calls EnsureBucket: hooks
// reconstruct stores from env URLs; the serve seam owns bucket creation.
func Open(ctx context.Context, blobURL string) (BlobStore, error) {
	assertf(blobURL != "", "Open: empty blob URL")
	u, err := url.Parse(blobURL)
	if err != nil {
		return nil, fmt.Errorf("parse blob URL: %w", err)
	}
	switch u.Scheme {
	case "s3":
		return OpenS3(ctx, blobURL)
	case "file":
		return OpenFile(blobURL)
	default:
		return nil, fmt.Errorf("blob URL scheme %q unsupported (want s3 or file)", u.Scheme)
	}
}

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

// bucketLifecycleClient is the subset of the S3 API EnsureBucket needs — a
// seam so unit tests can fake CreateBucket/HeadBucket outcomes without a
// live store. *s3.Client satisfies it.
type bucketLifecycleClient interface {
	CreateBucket(ctx context.Context, in *s3.CreateBucketInput, opts ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

// EnsureBucket creates the bucket, tolerating "already exists/owned by you"
// (so `serve` works against a fresh MinIO). A least-privilege deployment
// role for a pre-provisioned bucket has no s3:CreateBucket, so AccessDenied
// on create is not proof the bucket is missing: probe it with HeadBucket and
// accept when reachable. Any other create error, or a denied create with an
// unreachable bucket, stays boot-fatal.
func (b *S3) EnsureBucket(ctx context.Context) error {
	assertf(b.client != nil, "EnsureBucket: S3 opened without a client")
	return ensureBucket(ctx, b.client, b.name)
}

func ensureBucket(ctx context.Context, client bucketLifecycleClient, name string) error {
	assertf(name != "", "ensureBucket: empty bucket name")
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return nil
		case "AccessDenied":
			if _, headErr := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)}); headErr != nil {
				return fmt.Errorf("create bucket %q denied (%v) and probe failed: %w", name, err, headErr)
			}
			return nil
		}
	}
	return fmt.Errorf("create bucket %q: %w", name, err)
}

// UploadFile streams a local file to key.
func (b *S3) UploadFile(ctx context.Context, key, path string) error {
	return bucketUploadFile(ctx, b.bucket, key, path)
}

// DownloadFile fetches key into destPath atomically (tmp + rename).
func (b *S3) DownloadFile(ctx context.Context, key, destPath string) error {
	return bucketDownloadFile(ctx, b.bucket, key, destPath)
}

// List returns all objects under prefix.
func (b *S3) List(ctx context.Context, prefix string) ([]ListedObject, error) {
	return bucketList(ctx, b.bucket, prefix)
}

// ListPages streams objects under prefix in key order, one page at a time.
func (b *S3) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]ListedObject) error) error {
	return bucketListPages(ctx, b.bucket, prefix, pageSize, fn)
}

// VersioningEnabled reports whether the backing bucket has ever had S3
// versioning turned on (status Enabled OR Suspended — suspension keeps all
// previously stored versions, and unversioned deletes still only hide the
// current version behind a delete marker). The orphan-pack sweeper's delete
// mode refuses such buckets: its deletes would reclaim nothing while
// reporting that they did.
func (b *S3) VersioningEnabled(ctx context.Context) (bool, error) {
	assertf(b.client != nil, "VersioningEnabled: S3 opened without a client")
	out, err := b.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(b.name)})
	if err != nil {
		return false, fmt.Errorf("get bucket versioning %q: %w", b.name, err)
	}
	return out.Status == s3types.BucketVersioningStatusEnabled ||
		out.Status == s3types.BucketVersioningStatusSuspended, nil
}

// SignedURL returns a presigned S3 URL for method (GET or PUT) on key.
// ContentType is deliberately left unset in the options: gocloud's
// aws-sdk-go-v2 presigner returns Unimplemented when asked to sign a
// Content-Type for PUT, and an unsigned Content-Type means git-lfs's
// `Content-Type: application/octet-stream` PUT verifies against MinIO/S3
// SigV4 without extra configuration.
func (b *S3) SignedURL(ctx context.Context, key, method string, expiry time.Duration) (string, error) {
	assertSignedURLArgs(key, method, expiry)
	return b.bucket.SignedURL(ctx, key, &gcblob.SignedURLOptions{Method: method, Expiry: expiry})
}

// Stat returns the size of the object at key (ErrNotFound when absent).
func (b *S3) Stat(ctx context.Context, key string) (int64, error) {
	return bucketStat(ctx, b.bucket, key)
}

// ReadRange returns exactly the length bytes at offset from the object at
// key (a ranged GET; ErrNotFound when absent).
func (b *S3) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	return bucketReadRange(ctx, b.bucket, key, offset, length)
}

// Delete removes the object at key (ErrNotFound when absent).
func (b *S3) Delete(ctx context.Context, key string) error {
	return bucketDelete(ctx, b.bucket, key)
}

// S3 CopyObject handles at most 5 GiB per request; larger objects (packs
// can exceed it — uploads go through the multipart-capable writer) must be
// copied part-wise with UploadPartCopy. Vars, not consts, so tests can
// exercise the multipart path with small objects against MinIO.
var (
	s3SinglePartCopyLimit int64 = 5 * 1024 * 1024 * 1024
	s3CopyPartSize        int64 = 1024 * 1024 * 1024
)

// Copy duplicates srcKey to dstKey: server-side CopyObject for objects up
// to the single-request limit, multipart UploadPartCopy beyond it (still
// fully server-side — bytes never flow through this process).
func (b *S3) Copy(ctx context.Context, dstKey, srcKey string) error {
	size, err := b.Stat(ctx, srcKey)
	if err != nil {
		return fmt.Errorf("copy %s -> %s: %w", srcKey, dstKey, err)
	}
	if size <= s3SinglePartCopyLimit {
		return bucketCopy(ctx, b.bucket, dstKey, srcKey)
	}
	return b.multipartCopy(ctx, dstKey, srcKey, size)
}

// copySourceHeader encodes bucket/key for the X-Amz-Copy-Source header:
// URL-encoded path segments, with '+' escaped explicitly (url.PathEscape
// leaves it literal, but namespaced repo keys contain '+', and copy-source
// decoders are inconsistent about bare plus signs).
func copySourceHeader(bucket, key string) string {
	segs := strings.Split(key, "/")
	for i := range segs {
		segs[i] = strings.ReplaceAll(url.PathEscape(segs[i]), "+", "%2B")
	}
	return bucket + "/" + strings.Join(segs, "/")
}

// multipartCopy copies srcKey (size bytes) to dstKey in s3CopyPartSize
// ranges via UploadPartCopy, aborting the multipart upload on any failure
// so no billable incomplete upload leaks.
func (b *S3) multipartCopy(ctx context.Context, dstKey, srcKey string, size int64) (err error) {
	assertf(b.client != nil, "multipartCopy: S3 opened without a client")
	assertf(size > 0, "multipartCopy(%q): non-positive size %d", srcKey, size)
	create, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.name), Key: aws.String(dstKey),
	})
	if err != nil {
		return fmt.Errorf("copy %s -> %s: create multipart: %w", srcKey, dstKey, err)
	}
	defer func() {
		if err == nil {
			return
		}
		// Abort must survive a canceled ctx, or the incomplete upload leaks.
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_, _ = b.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(b.name), Key: aws.String(dstKey), UploadId: create.UploadId,
		})
	}()

	source := copySourceHeader(b.name, srcKey)
	var parts []s3types.CompletedPart
	for start := int64(0); start < size; start += s3CopyPartSize {
		end := min(start+s3CopyPartSize, size) - 1
		num := int32(len(parts) + 1) //nolint:gosec // bounded by S3's 10k part cap
		part, err := b.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: aws.String(b.name), Key: aws.String(dstKey),
			CopySource:      aws.String(source),
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
			PartNumber:      aws.Int32(num),
			UploadId:        create.UploadId,
		})
		if err != nil {
			return fmt.Errorf("copy %s -> %s: part %d: %w", srcKey, dstKey, num, err)
		}
		parts = append(parts, s3types.CompletedPart{ETag: part.CopyPartResult.ETag, PartNumber: aws.Int32(num)})
	}
	if _, err := b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(b.name), Key: aws.String(dstKey), UploadId: create.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return fmt.Errorf("copy %s -> %s: complete multipart: %w", srcKey, dstKey, err)
	}
	return nil
}
