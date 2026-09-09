package simtest

import (
	"context"
	"fmt"
	"time"

	"github.com/coder/openstorage/internal/blob"
)

// SimBlobStore decorates a blob.BlobStore with seeded fault injection.
// Uploads support the clean/ambiguous split (an ambiguous upload publishes
// the object, then loses the result — the "S3 PUT succeeded, response
// dropped" class); downloads and stats additionally support a transient
// FaultNotFound wrapping blob.ErrNotFound, the class issue #6's bounded
// hydration retry absorbs.
type SimBlobStore struct {
	inner blob.BlobStore
	inj   *Injector
}

var _ blob.BlobStore = (*SimBlobStore)(nil)

// NewSimBlobStore wraps inner with fault injection driven by inj.
func NewSimBlobStore(inner blob.BlobStore, inj *Injector) *SimBlobStore {
	assertf(inner != nil, "NewSimBlobStore: nil inner store")
	assertf(inj != nil, "NewSimBlobStore: nil injector")
	return &SimBlobStore{inner: inner, inj: inj}
}

// notFoundFaultError wraps blob.ErrNotFound so errors.Is matches, while the
// message still identifies it as injected.
func notFoundFaultError(op string) error {
	return fmt.Errorf("%s: simtest: injected transient fault: %w", op, blob.ErrNotFound)
}

// UploadFile delegates unless a fault is injected; an ambiguous fault
// uploads the object and then reports failure.
func (s *SimBlobStore) UploadFile(ctx context.Context, key, path string) error {
	switch class := s.inj.begin(OpBlobUploadFile); class {
	case FaultClean:
		return cleanFaultError{op: OpBlobUploadFile}
	case FaultAmbiguous:
		err := s.inner.UploadFile(ctx, key, path)
		s.inj.noteInnerOutcome(OpBlobUploadFile, fmt.Sprintf("err=%v", err))
		return ambiguousFaultError{op: OpBlobUploadFile}
	case FaultNone:
		return s.inner.UploadFile(ctx, key, path)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobUploadFile))
	}
}

// DownloadFile delegates unless a fault is injected:
//
//   - clean: no download happens, transport-shaped error.
//   - ambiguous: the file IS materialized at destPath, then an error is
//     reported (callers must tolerate the atomic download landing).
//   - notfound: blob.ErrNotFound without executing — a listed pack
//     transiently missing, which hydration must absorb by re-listing.
func (s *SimBlobStore) DownloadFile(ctx context.Context, key, destPath string) error {
	switch class := s.inj.begin(OpBlobDownloadFile); class {
	case FaultClean:
		return cleanFaultError{op: OpBlobDownloadFile}
	case FaultAmbiguous:
		err := s.inner.DownloadFile(ctx, key, destPath)
		s.inj.noteInnerOutcome(OpBlobDownloadFile, fmt.Sprintf("err=%v", err))
		return ambiguousFaultError{op: OpBlobDownloadFile}
	case FaultNotFound:
		return notFoundFaultError(OpBlobDownloadFile)
	case FaultNone:
		return s.inner.DownloadFile(ctx, key, destPath)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobDownloadFile))
	}
}

// DownloadFileCapped shares DownloadFile's fault point: the cap is a
// local disk-budget concern, not a distinct backend operation.
func (s *SimBlobStore) DownloadFileCapped(ctx context.Context, key, destPath string, capBytes int64) error {
	switch class := s.inj.begin(OpBlobDownloadFile); class {
	case FaultClean:
		return cleanFaultError{op: OpBlobDownloadFile}
	case FaultAmbiguous:
		err := s.inner.DownloadFileCapped(ctx, key, destPath, capBytes)
		s.inj.noteInnerOutcome(OpBlobDownloadFile, fmt.Sprintf("err=%v", err))
		return ambiguousFaultError{op: OpBlobDownloadFile}
	case FaultNotFound:
		return notFoundFaultError(OpBlobDownloadFile)
	case FaultNone:
		return s.inner.DownloadFileCapped(ctx, key, destPath, capBytes)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobDownloadFile))
	}
}

// List delegates to the inner store unless a fault is injected.
func (s *SimBlobStore) List(ctx context.Context, prefix string) ([]blob.ListedObject, error) {
	switch class := s.inj.begin(OpBlobList); class {
	case FaultClean:
		return nil, cleanFaultError{op: OpBlobList}
	case FaultAmbiguous:
		return nil, ambiguousFaultError{op: OpBlobList}
	case FaultNone:
		return s.inner.List(ctx, prefix)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobList))
	}
}

// ListPages delegates to the inner store unless a fault is injected; it
// shares OpBlobList with List (one listing surface, one fault point).
func (s *SimBlobStore) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	switch class := s.inj.begin(OpBlobList); class {
	case FaultClean:
		return cleanFaultError{op: OpBlobList}
	case FaultAmbiguous:
		return ambiguousFaultError{op: OpBlobList}
	case FaultNone:
		return s.inner.ListPages(ctx, prefix, pageSize, fn)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobList))
	}
}

// SignedURL delegates to the inner store unless a fault is injected.
func (s *SimBlobStore) SignedURL(ctx context.Context, key, method string, expiry time.Duration) (string, error) {
	switch class := s.inj.begin(OpBlobSignedURL); class {
	case FaultClean:
		return "", cleanFaultError{op: OpBlobSignedURL}
	case FaultAmbiguous:
		return "", ambiguousFaultError{op: OpBlobSignedURL}
	case FaultNone:
		return s.inner.SignedURL(ctx, key, method, expiry)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobSignedURL))
	}
}

// Stat delegates unless a fault is injected; FaultNotFound reports the
// object as missing via blob.ErrNotFound.
func (s *SimBlobStore) Stat(ctx context.Context, key string) (int64, error) {
	switch class := s.inj.begin(OpBlobStat); class {
	case FaultClean:
		return 0, cleanFaultError{op: OpBlobStat}
	case FaultAmbiguous:
		return 0, ambiguousFaultError{op: OpBlobStat}
	case FaultNotFound:
		return 0, notFoundFaultError(OpBlobStat)
	case FaultNone:
		return s.inner.Stat(ctx, key)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobStat))
	}
}

// StatFingerprint delegates unless a fault is injected; it shares Stat's
// fault op — it IS a stat with a version fingerprint attached.
func (s *SimBlobStore) StatFingerprint(ctx context.Context, key string) (int64, string, error) {
	switch class := s.inj.begin(OpBlobStat); class {
	case FaultClean:
		return 0, "", cleanFaultError{op: OpBlobStat}
	case FaultAmbiguous:
		return 0, "", ambiguousFaultError{op: OpBlobStat}
	case FaultNotFound:
		return 0, "", notFoundFaultError(OpBlobStat)
	case FaultNone:
		return s.inner.StatFingerprint(ctx, key)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobStat))
	}
}

// ReadRange delegates unless a fault is injected. It deliberately shares
// OpBlobStat with Stat rather than adding a fault point: both are the
// repack selection's per-candidate point reads (size, pack-header object
// count) with identical failure handling — one metadata surface, one
// fault point (the List/ListPages precedent). FaultNotFound reports the
// object as missing via blob.ErrNotFound.
func (s *SimBlobStore) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	switch class := s.inj.begin(OpBlobStat); class {
	case FaultClean:
		return nil, cleanFaultError{op: OpBlobStat}
	case FaultAmbiguous:
		return nil, ambiguousFaultError{op: OpBlobStat}
	case FaultNotFound:
		return nil, notFoundFaultError(OpBlobStat)
	case FaultNone:
		return s.inner.ReadRange(ctx, key, offset, length)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobStat))
	}
}

// Delete delegates unless a fault is injected; an ambiguous fault deletes
// the object and then reports failure (the sweeper's delete outcome lost in
// transit); FaultNotFound reports the object as already missing.
func (s *SimBlobStore) Delete(ctx context.Context, key string) error {
	switch class := s.inj.begin(OpBlobDelete); class {
	case FaultClean:
		return cleanFaultError{op: OpBlobDelete}
	case FaultAmbiguous:
		err := s.inner.Delete(ctx, key)
		s.inj.noteInnerOutcome(OpBlobDelete, fmt.Sprintf("err=%v", err))
		return ambiguousFaultError{op: OpBlobDelete}
	case FaultNotFound:
		return notFoundFaultError(OpBlobDelete)
	case FaultNone:
		return s.inner.Delete(ctx, key)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobDelete))
	}
}

// Copy delegates unless a fault is injected; an ambiguous fault performs
// the copy and then reports failure (a quarantine copy that landed but
// whose result was lost); FaultNotFound reports a missing source.
func (s *SimBlobStore) Copy(ctx context.Context, dstKey, srcKey string) error {
	switch class := s.inj.begin(OpBlobCopy); class {
	case FaultClean:
		return cleanFaultError{op: OpBlobCopy}
	case FaultAmbiguous:
		err := s.inner.Copy(ctx, dstKey, srcKey)
		s.inj.noteInnerOutcome(OpBlobCopy, fmt.Sprintf("err=%v", err))
		return ambiguousFaultError{op: OpBlobCopy}
	case FaultNotFound:
		return notFoundFaultError(OpBlobCopy)
	case FaultNone:
		return s.inner.Copy(ctx, dstKey, srcKey)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobCopy))
	}
}

// EnsureBucket delegates to the inner store unless a fault is injected.
func (s *SimBlobStore) EnsureBucket(ctx context.Context) error {
	switch class := s.inj.begin(OpBlobEnsureBucket); class {
	case FaultClean:
		return cleanFaultError{op: OpBlobEnsureBucket}
	case FaultAmbiguous:
		err := s.inner.EnsureBucket(ctx)
		s.inj.noteInnerOutcome(OpBlobEnsureBucket, fmt.Sprintf("err=%v", err))
		return ambiguousFaultError{op: OpBlobEnsureBucket}
	case FaultNone:
		return s.inner.EnsureBucket(ctx)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpBlobEnsureBucket))
	}
}

// Close always delegates: lifecycle teardown is never fault-injected (a
// crash is simulated by dropping the handle, not by failing Close).
func (s *SimBlobStore) Close() error { return s.inner.Close() }
