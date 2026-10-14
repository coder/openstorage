package sync

import (
	"context"
	"fmt"
	"os"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
	"github.com/coder/openstorage/internal/ref/s3wal/s3waltest"
)

// The S3WAL local-schedule fixture runs against SeaweedFS (issue #335,
// ADR-51 test plan item 5). One container and one bucket serve the whole
// package run; each fixture gets its own key prefix, so its metadata
// document is private and the bucket's volume count stays fixed.
const seaweedScheduleBucket = "sync-local-schedule"

var (
	seaweedOnce     stdsync.Once
	seaweedClient   *s3wal.Client
	seaweedErr      error
	seaweedStop     func()
	seaweedPrefixes atomic.Int64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if seaweedClient != nil {
		seaweedClient.Close()
	}
	if seaweedStop != nil {
		seaweedStop()
	}
	os.Exit(code)
}

// seaweedScheduleStore returns a private view of the package's SeaweedFS
// bucket, starting the container on first use.
func seaweedScheduleStore(t *testing.T) s3wal.ConditionalStore {
	t.Helper()
	seaweedOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		endpoint, stop, err := s3waltest.StartSeaweedFSContainer(ctx)
		if err != nil {
			seaweedErr = err
			return
		}
		seaweedStop = stop
		raw := s3waltest.RawS3Client(endpoint, "us-east-1", s3waltest.SeaweedFSAccessKey, s3waltest.SeaweedFSSecretKey)
		if err := s3waltest.EnsureBucket(ctx, raw, seaweedScheduleBucket); err != nil {
			seaweedErr = err
			return
		}
		c := s3wal.New(raw, seaweedScheduleBucket)
		c.DecodeSeq = s3wal.DecodeSeq
		// The conditional-write probe serve runs at boot (InitSchema).
		if err := s3wal.Probe(ctx, c); err != nil {
			c.Close()
			seaweedErr = fmt.Errorf("probe: %w", err)
			return
		}
		seaweedClient = c
	})
	if seaweedErr != nil {
		t.Fatalf("seaweedfs schedule store: %v", seaweedErr)
	}
	return prefixedCAS{inner: seaweedClient, prefix: fmt.Sprintf("fixture-%d/", seaweedPrefixes.Add(1))}
}

// prefixedCAS places every key of one fixture under prefix.
type prefixedCAS struct {
	inner  s3wal.ConditionalStore
	prefix string
}

func (p prefixedCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	return p.inner.Get(ctx, p.prefix+key)
}

func (p prefixedCAS) GetIfChanged(ctx context.Context, key, etag string) (s3wal.Object, error) {
	return p.inner.GetIfChanged(ctx, p.prefix+key, etag)
}

func (p prefixedCAS) Create(ctx context.Context, key string, body []byte) (string, error) {
	return p.inner.Create(ctx, p.prefix+key, body)
}

func (p prefixedCAS) Advance(ctx context.Context, key string, body []byte, etag string) (string, error) {
	return p.inner.Advance(ctx, p.prefix+key, body, etag)
}

func (p prefixedCAS) Delete(ctx context.Context, key string) error {
	return p.inner.Delete(ctx, p.prefix+key)
}

func (p prefixedCAS) List(ctx context.Context, prefix, startAfter string, limit int) ([]string, error) {
	if startAfter != "" {
		startAfter = p.prefix + startAfter
	}
	keys, err := p.inner.List(ctx, p.prefix+prefix, startAfter, limit)
	for i, k := range keys {
		assertf(strings.HasPrefix(k, p.prefix), "prefixedCAS: listed key %q outside %q", k, p.prefix)
		keys[i] = strings.TrimPrefix(k, p.prefix)
	}
	return keys, err
}

func (p prefixedCAS) StoreDate() (date, observedAt time.Time, ok bool) {
	return p.inner.StoreDate()
}

var _ s3wal.ConditionalStore = prefixedCAS{}
