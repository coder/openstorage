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
	return bucketDownloadFile(ctx, b.bucket, key, destPath)
}

// List returns all objects under prefix.
func (b *File) List(ctx context.Context, prefix string) ([]ListedObject, error) {
	return bucketList(ctx, b.bucket, prefix)
}

// SignedURL is unsupported on the filesystem backend: it returns
// ErrSignedURLUnsupported so callers fall back to proxying transfers
// through the server (docs/05 local mode).
func (b *File) SignedURL(_ context.Context, key, method string, expiry time.Duration) (string, error) {
	assertSignedURLArgs(key, method, expiry)
	return "", ErrSignedURLUnsupported
}

// Stat returns the size of the object at key (ErrNotFound when absent).
func (b *File) Stat(ctx context.Context, key string) (int64, error) {
	return bucketStat(ctx, b.bucket, key)
}

// Delete removes the object at key (ErrNotFound when absent).
func (b *File) Delete(ctx context.Context, key string) error {
	return bucketDelete(ctx, b.bucket, key)
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
func bucketDownloadFile(ctx context.Context, bucket *gcblob.Bucket, key, destPath string) error {
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
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return fmt.Errorf("download %s: %w", key, err)
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

// bucketList returns all objects under prefix.
func bucketList(ctx context.Context, bucket *gcblob.Bucket, prefix string) ([]ListedObject, error) {
	assertf(bucket != nil, "bucketList(%q): nil bucket", prefix)
	iter := bucket.List(&gcblob.ListOptions{Prefix: prefix})
	var out []ListedObject
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, ListedObject{Key: obj.Key, Size: obj.Size, ModTime: obj.ModTime})
	}
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
