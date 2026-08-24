// Local-filesystem BlobStore backend (gocloud.dev fileblob) for local mode
// (`serve --data-dir`): packs are plain files under a directory, so a test
// binary needs zero external dependencies. This file also holds the shared
// bucket helpers (upload/download/list over *gcblob.Bucket) so the atomic
// tmp+rename download contract has exactly one implementation.
package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	gcblob "gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/gcerrors"
)

// File implements BlobStore on a local directory via gocloud fileblob.
type File struct {
	bucket *gcblob.Bucket
	dir    string
}

var _ BlobStore = (*File)(nil)

// OpenFile opens a file:// blob URL, e.g. file:///abs/dir (empty host, so the
// directory is always absolute). The root directory is created up front —
// hooks reconstruct stores from env URLs without calling EnsureBucket, and a
// missing directory must not fail them. MetadataDontWrite keeps listings
// clean: no .attrs sidecars next to pack files.
func OpenFile(blobURL string) (*File, error) {
	assertf(blobURL != "", "OpenFile: empty blob URL")
	u, err := url.Parse(blobURL)
	if err != nil {
		return nil, fmt.Errorf("parse blob URL: %w", err)
	}
	if u.Scheme != fileblob.Scheme {
		return nil, fmt.Errorf("blob URL scheme %q unsupported (want file)", u.Scheme)
	}
	if u.Host != "" {
		return nil, fmt.Errorf("file blob URL must have an empty host (file:///abs/dir), got host %q", u.Host)
	}
	dir := u.Path
	if dir == "" {
		return nil, errors.New("file blob URL has no directory path")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create blob dir %q: %w", dir, err)
	}
	bucket, err := fileblob.OpenBucket(dir, &fileblob.Options{
		CreateDir: true,
		Metadata:  fileblob.MetadataDontWrite,
		// Write temp files next to their destination, not in os.TempDir:
		// the bucket dir (e.g. --data-dir on /home) and /tmp are often
		// different mounts, and fileblob's publish rename would then fail
		// with EXDEV "invalid cross-device link" on every pack upload.
		NoTempDir: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open file bucket %q: %w", dir, err)
	}
	return &File{bucket: bucket, dir: dir}, nil
}

func (b *File) Close() error { return b.bucket.Close() }

// EnsureBucket idempotently creates the backing directory (the file://
// analogue of S3 CreateBucket at serve startup).
func (b *File) EnsureBucket(_ context.Context) error {
	assertf(b.dir != "", "EnsureBucket: File opened without a directory")
	if err := os.MkdirAll(b.dir, 0o755); err != nil {
		return fmt.Errorf("create blob dir %q: %w", b.dir, err)
	}
	return nil
}

// UploadFile streams a local file to key.
func (b *File) UploadFile(ctx context.Context, key, path string) error {
	return bucketUploadFile(ctx, b.bucket, key, path)
}

// DownloadFile fetches key into destPath atomically (tmp + rename).
func (b *File) DownloadFile(ctx context.Context, key, destPath string) error {
	return bucketDownloadFile(ctx, b.bucket, key, destPath, -1)
}

// DownloadFileCapped fetches key into destPath atomically, aborting with
// ErrCapExceeded once the stream would exceed capBytes on disk.
func (b *File) DownloadFileCapped(ctx context.Context, key, destPath string, capBytes int64) error {
	assertf(capBytes >= 0, "DownloadFileCapped(%q, %d)", key, capBytes)
	return bucketDownloadFile(ctx, b.bucket, key, destPath, capBytes)
}

// List returns all objects under prefix.
func (b *File) List(ctx context.Context, prefix string) ([]ListedObject, error) {
	return bucketList(ctx, b.bucket, prefix)
}

// ListPages streams objects under prefix in key order, one page at a time.
func (b *File) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]ListedObject) error) error {
	return bucketListPages(ctx, b.bucket, prefix, pageSize, fn)
}

// SignedURL is unsupported on the filesystem backend: it returns
// ErrSignedURLUnsupported so callers fall back to proxying transfers
// through the server (docs/explanation/git-protocol-and-namespaces.md local mode).
func (b *File) SignedURL(_ context.Context, key, method string, expiry time.Duration) (string, error) {
	assertSignedURLArgs(key, method, expiry)
	return "", ErrSignedURLUnsupported
}

// Stat returns the size of the object at key (ErrNotFound when absent).
func (b *File) Stat(ctx context.Context, key string) (int64, error) {
	return bucketStat(ctx, b.bucket, key)
}

// StatFingerprint — see BlobStore.
func (b *File) StatFingerprint(ctx context.Context, key string) (int64, string, error) {
	return bucketStatFingerprint(ctx, b.bucket, key)
}

// ReadRange returns exactly the length bytes at offset from the object at
// key (a seeked local read; ErrNotFound when absent).
func (b *File) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	return bucketReadRange(ctx, b.bucket, key, offset, length)
}

// Delete removes the object at key (ErrNotFound when absent).
func (b *File) Delete(ctx context.Context, key string) error {
	return bucketDelete(ctx, b.bucket, key)
}

// Copy duplicates srcKey to dstKey (local file copy inside the bucket dir).
func (b *File) Copy(ctx context.Context, dstKey, srcKey string) error {
	return bucketCopy(ctx, b.bucket, dstKey, srcKey)
}

// bucketUploadFile streams the local file at path into bucket under key. It
// is the single upload implementation for gocloud-backed BlobStores.
func bucketUploadFile(ctx context.Context, bucket *gcblob.Bucket, key, path string) error {
	assertf(bucket != nil && key != "", "bucketUploadFile(%q)", key)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w, err := bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, f); err != nil {
		_ = w.Close()
		return fmt.Errorf("upload %s: %w", key, err)
	}
	return w.Close()
}

// bucketDownloadFile fetches key into destPath via tmp + rename, so a
// partially downloaded pack is never visible at destPath. Missing keys map
// onto ErrNotFound (like bucketStat), so callers can tell absence — a pack
// folded away by a concurrent repack — from transport failures.
func bucketDownloadFile(ctx context.Context, bucket *gcblob.Bucket, key, destPath string, capBytes int64) error {
	assertf(bucket != nil && key != "", "bucketDownloadFile(%q)", key)
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return fmt.Errorf("download %s: %w", key, ErrNotFound)
		}
		return fmt.Errorf("download %s: %w", key, err)
	}
	defer r.Close()
	// filepath.Dir handles relative and bare-filename paths ("." for the
	// latter); the tmp file must live in the destination directory so the
	// final rename is atomic (same filesystem).
	tmp, err := os.CreateTemp(filepath.Dir(destPath), ".blob-download-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	src := io.Reader(r)
	if capBytes >= 0 {
		// One sentinel byte past the cap distinguishes exactly-at-cap
		// from over-cap without another round-trip; it is only ever
		// read when the stream genuinely exceeds the cap.
		src = io.LimitReader(r, capBytes+1)
	}
	n, err := io.Copy(tmp, src)
	if err != nil {
		tmp.Close()
		return fmt.Errorf("download %s: %w", key, err)
	}
	if capBytes >= 0 && n > capBytes {
		tmp.Close()
		return fmt.Errorf("download %s: %w (stream exceeded %d bytes)", key, ErrCapExceeded, capBytes)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), destPath)
}

// bucketStat returns the size of the object at key, mapping gocloud's
// NotFound code onto the package-level ErrNotFound sentinel. It is the
// single Stat implementation for gocloud-backed BlobStores.
func bucketStat(ctx context.Context, bucket *gcblob.Bucket, key string) (int64, error) {
	assertf(bucket != nil && key != "", "bucketStat(%q)", key)
	attrs, err := bucket.Attributes(ctx, key)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return 0, fmt.Errorf("stat %s: %w", key, ErrNotFound)
		}
		return 0, fmt.Errorf("stat %s: %w", key, err)
	}
	return attrs.Size, nil
}

// bucketStatFingerprint is the single StatFingerprint implementation for
// gocloud-backed BlobStores: the driver ETag when present (content-derived
// on S3), else an mtime+size fallback. The fingerprint is OPAQUE — callers
// may only compare for equality.
//
// Granularity caveat: fileblob's ETag is mtime-derived, and filesystem
// mtimes tick on the kernel's coarse clock (1–10ms), so two same-size
// rewrites inside one tick can collide. That weakness is acceptable
// because the adversarial same-size rewriter the fingerprint defends
// against is a still-valid presigned PUT (ADR-41) — and file backends
// never issue presigned URLs (ErrSignedURLUnsupported), while S3's
// content-derived ETag cannot be preserved across a rewrite at all.
func bucketStatFingerprint(ctx context.Context, bucket *gcblob.Bucket, key string) (int64, string, error) {
	assertf(bucket != nil && key != "", "bucketStatFingerprint(%q)", key)
	attrs, err := bucket.Attributes(ctx, key)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return 0, "", fmt.Errorf("stat fingerprint %s: %w", key, ErrNotFound)
		}
		return 0, "", fmt.Errorf("stat fingerprint %s: %w", key, err)
	}
	fp := attrs.ETag
	if fp == "" {
		fp = fmt.Sprintf("mt:%d-%d", attrs.ModTime.UnixNano(), attrs.Size)
	}
	return attrs.Size, fp, nil
}

// bucketReadRange reads exactly [offset, offset+length) from the object at
// key via a ranged reader (S3 ranged GET / seeked local read), mapping
// gocloud's NotFound code onto ErrNotFound. A short read — the object ends
// before offset+length — is a loud non-ErrNotFound error, never silently
// truncated bytes. It is the single ReadRange implementation for
// gocloud-backed BlobStores.
func bucketReadRange(ctx context.Context, bucket *gcblob.Bucket, key string, offset, length int64) ([]byte, error) {
	assertf(bucket != nil && key != "" && offset >= 0 && length > 0,
		"bucketReadRange(%q, %d, %d)", key, offset, length)
	r, err := bucket.NewRangeReader(ctx, key, offset, length, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, fmt.Errorf("read range %s: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("read range %s: %w", key, err)
	}
	defer r.Close()
	buf, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read range %s: %w", key, err)
	}
	if int64(len(buf)) != length {
		return nil, fmt.Errorf("read range %s: object too short: %d bytes at offset %d, want %d",
			key, len(buf), offset, length)
	}
	return buf, nil
}

// bucketCopy duplicates srcKey to dstKey inside the bucket, mapping
// gocloud's NotFound code onto ErrNotFound. It is the single Copy
// implementation for gocloud-backed BlobStores.
func bucketCopy(ctx context.Context, bucket *gcblob.Bucket, dstKey, srcKey string) error {
	assertf(bucket != nil && dstKey != "" && srcKey != "", "bucketCopy(%q, %q)", dstKey, srcKey)
	if err := bucket.Copy(ctx, dstKey, srcKey, nil); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return fmt.Errorf("copy %s -> %s: %w", srcKey, dstKey, ErrNotFound)
		}
		return fmt.Errorf("copy %s -> %s: %w", srcKey, dstKey, err)
	}
	return nil
}

// bucketList returns all objects under prefix.
func bucketList(ctx context.Context, bucket *gcblob.Bucket, prefix string) ([]ListedObject, error) {
	var out []ListedObject
	err := bucketListPages(ctx, bucket, prefix, 1000, func(page []ListedObject) error {
		out = append(out, page...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// bucketListPages streams objects under prefix in key order (the gocloud
// driver contract orders listings lexicographically), invoking fn per page
// of at most pageSize objects. The page slice is reused between calls. It
// is the single streaming-list implementation for gocloud-backed
// BlobStores; the underlying iterator fetches provider pages lazily, so
// memory stays bounded by pageSize regardless of prefix population.
func bucketListPages(ctx context.Context, bucket *gcblob.Bucket, prefix string, pageSize int, fn func([]ListedObject) error) error {
	assertf(bucket != nil, "bucketListPages(%q): nil bucket", prefix)
	assertf(pageSize > 0, "bucketListPages(%q): non-positive page size %d", prefix, pageSize)
	assertf(fn != nil, "bucketListPages(%q): nil fn", prefix)
	iter := bucket.List(&gcblob.ListOptions{Prefix: prefix})
	page := make([]ListedObject, 0, pageSize)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		page = append(page, ListedObject{Key: obj.Key, Size: obj.Size, ModTime: obj.ModTime})
		if len(page) == pageSize {
			if err := fn(page); err != nil {
				return err
			}
			page = page[:0]
		}
	}
	if len(page) > 0 {
		return fn(page)
	}
	return nil
}

// bucketDelete removes the object at key, mapping gocloud's NotFound code
// onto the package-level ErrNotFound sentinel. It is the single Delete
// implementation for gocloud-backed BlobStores.
func bucketDelete(ctx context.Context, bucket *gcblob.Bucket, key string) error {
	assertf(bucket != nil && key != "", "bucketDelete(%q)", key)
	if err := bucket.Delete(ctx, key); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return fmt.Errorf("delete %s: %w", key, ErrNotFound)
		}
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}
