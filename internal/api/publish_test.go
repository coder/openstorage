package api

// Unit tests for publishPack's reservation-denial classification (issue
// #51, Codex round 43): the ledger's two denial classes must map to
// distinct error identities — byte-quota denials are permanent (403 at
// the handlers), while the active-row cap is transient (429, retryable).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

// capBusyRefStore returns a KeyCapExceeded denial from every reserve.
type capBusyRefStore struct{ ref.RefStore }

func (capBusyRefStore) ReservePackBytes(context.Context, []string, []ref.Pack, string, int64, int64, time.Duration) (ref.ReserveDecision, error) {
	return ref.ReserveDecision{KeyCapExceeded: true}, nil
}

// TestPublishPackReservationCapIsRetryable (Codex round 43): a
// KeyCapExceeded denial means the repository's ACTIVE-ROW cap is full —
// rows expire with the TTL, so the correct client behavior is retry, not
// the permanent quota-exceeded failure (whose arithmetic may even show
// the write fits). publishPack must surface the transient sentinel, never
// errPackQuotaExceeded.
func TestPublishPackReservationCapIsRetryable(t *testing.T) {
	ctx := context.Background()
	bl, err := blob.Open(ctx, "file://"+t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bl.Close() })
	packPath := filepath.Join(t.TempDir(), "pack")
	if err := os.WriteFile(packPath, []byte("pack bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := &server{
		refStore:           capBusyRefStore{},
		blobStore:          bl,
		packQuotaBytes:     1000,
		packReservationTTL: time.Hour,
	}
	err = s.publishPack(ctx, "repo_capbusy", "aaaa", packPath)
	if !errors.Is(err, errPackReservationsBusy) {
		t.Fatalf("row-cap denial error = %v, want errPackReservationsBusy", err)
	}
	if errors.Is(err, errPackQuotaExceeded) {
		t.Fatal("transient row-cap denial classified as the permanent quota denial")
	}
}
