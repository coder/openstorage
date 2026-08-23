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
	"strings"
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

func (capBusyRefStore) QuotaLedgerState(context.Context, []string, time.Duration, ...string) (ref.PackQuotaLedgerState, error) {
	return ref.PackQuotaLedgerState{}, nil // the uncached floor's counter read
}

// TestUsageFloorFallbackSubtractsCounter (Codex round 45): the uncached
// fallback (nil Config.UsageFloor — direct construction) must return the
// UNCOUNTED-bytes floor like every other producer: ReservePackBytes now
// SUMS the floor with its live counter, so a raw listing double-charges
// every registered byte — a repository physically storing 60% of quota
// would deny writes as though it used 120%.
func TestUsageFloorFallbackSubtractsCounter(t *testing.T) {
	ctx := context.Background()
	bl, err := blob.Open(ctx, "file://"+t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bl.Close() })
	st, err := ref.Open(ctx, "sqlite://"+t.TempDir()+"/refs.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}

	// 30 physical bytes, 20 of them registered: the floor must be 10.
	writeBlob := func(key string, n int) {
		t.Helper()
		p := filepath.Join(t.TempDir(), "blob")
		if err := os.WriteFile(p, make([]byte, n), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := bl.UploadFile(ctx, key, p); err != nil {
			t.Fatal(err)
		}
	}
	key := blob.LFSKey("repo_fb", strings.Repeat("ab", 32))
	writeBlob(key, 20)
	writeBlob(blob.PackKey("repo_fb", "aaaa"), 10)
	if err := st.RegisterLFSObject(ctx, "repo_fb", key, 20); err != nil {
		t.Fatal(err)
	}

	s := &server{refStore: st, blobStore: bl}
	if floor, err := s.usageFloor(ctx, "repo_fb"); err != nil || floor != 10 {
		t.Fatalf("fallback floor = (%d, %v), want (10, nil) — registered bytes must not double-charge", floor, err)
	}
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
