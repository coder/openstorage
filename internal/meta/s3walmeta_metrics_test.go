package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/openstorage/internal/metrics"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// TestS3WALMetaMetricsContention drives the MemCAS contention fault seam
// through a metadata-document mutation and asserts the ADR-47 counter
// moves once for the terminal failure.
func TestS3WALMetaMetricsContention(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, _ := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)
	cas.Hooks.PutFault = func(op, key string) s3wal.PutFault { return s3wal.FaultContention }
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	labels := map[string]string{"scope": "meta"}
	before := metrics.Value("openstorage_s3wal_cas_contention_exhausted_total", labels)
	if err := metaStore.RequestSync(ctx, repoID); !errors.Is(err, s3wal.ErrCASContention) {
		t.Fatalf("RequestSync = %v, want ErrCASContention", err)
	}
	if d := metrics.Value("openstorage_s3wal_cas_contention_exhausted_total", labels) - before; d != 1 {
		t.Fatalf("meta contention counter delta = %v, want 1", d)
	}
}
