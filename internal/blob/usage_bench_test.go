// Hot-path benchmark for the quota-admission usage listing (issue #92):
// StorageUsageBytes runs on every push/write admission as the usage floor, and
// its cost scales with the repo's registered pack count (page-bounded
// listings, page size packUsagePageSize). The pack-count matrix exposes the
// listing-page allocation profile on both test backends: the in-memory
// bucket (pure listing cost) and fileblob (the local-mode wiring, adding
// filesystem stats). Compare against ref's BenchmarkReservePackBytesSQLite
// for the cached-counter admission read this listing floors.
package blob

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gocloud.dev/blob/memblob"
)

// benchUsageStore builds one seeded store: n pack objects of contentSize
// bytes each under repo's pack prefix, uploaded through the public
// BlobStore surface.
func benchUsageStore(b *testing.B, backend, repo string, n int, contentSize int64) BlobStore {
	b.Helper()
	var store BlobStore
	switch backend {
	case "mem":
		bucket := memblob.OpenBucket(nil)
		b.Cleanup(func() { bucket.Close() })
		store = &S3{bucket: bucket}
	case "file":
		f, err := OpenFile("file://" + filepath.Join(b.TempDir(), "blobs"))
		if err != nil {
			b.Fatalf("OpenFile: %v", err)
		}
		store = f
	default:
		b.Fatalf("unknown backend %q", backend)
	}
	src := filepath.Join(b.TempDir(), "pack")
	if err := os.WriteFile(src, make([]byte, contentSize), 0o644); err != nil {
		b.Fatalf("write source pack: %v", err)
	}
	ctx := context.Background()
	for i := range n {
		if err := store.UploadFile(ctx, PackKey(repo, fmt.Sprintf("%040d", i)), src); err != nil {
			b.Fatalf("seed pack %d: %v", i, err)
		}
	}
	return store
}

func BenchmarkStorageUsageBytes(b *testing.B) {
	const repo = "benchrepo"
	const contentSize = int64(256)
	ctx := context.Background()
	for _, backend := range []string{"mem", "file"} {
		for _, n := range []int{10, 100, 1000} {
			b.Run(fmt.Sprintf("store=%s/packs=%d", backend, n), func(b *testing.B) {
				store := benchUsageStore(b, backend, repo, n, contentSize)
				want := int64(n) * contentSize
				b.ReportAllocs()
				for b.Loop() {
					total, err := StorageUsageBytes(ctx, store, repo)
					if err != nil {
						b.Fatalf("StorageUsageBytes: %v", err)
					}
					if total != want {
						b.Fatalf("StorageUsageBytes = %d, want %d", total, want)
					}
				}
			})
		}
	}
}
