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

func (capBusyRefStore) ReservePackBytes(context.Context, []string, []ref.Pack, string, int64, int64, int64, time.Duration) (ref.ReserveDecision, error) {
	return ref.ReserveDecision{KeyCapExceeded: true}, nil
}

func (capBusyRefStore) QuotaLedgerState(context.Context, []string, time.Duration, ...string) (ref.PackQuotaLedgerState, error) {
	return ref.PackQuotaLedgerState{}, nil // the uncached floor's counter read
}

func (capBusyRefStore) HasPack(context.Context, string, string) (bool, error) {
	return false, nil // fresh growth: the admission path is under test
}

func (capBusyRefStore) LFSAdoptionGen(context.Context, string) (int64, error) {
	return 0, nil // the uncached floor's generation read (Codex round 60)
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
	if floor, gen, err := s.usageFloor(ctx, "repo_fb"); err != nil || floor != 10 || gen != 0 {
		t.Fatalf("fallback floor = (%d, gen %d, %v), want (10, 0, nil) — registered bytes must not double-charge", floor, gen, err)
	}
}

// restoreRaceBlob simulates a sweeper racing every existence probe: Stat
// always reports the key missing, uploads succeed, and the SECOND upload
// (EnsurePublished's restore) cancels the request context first — a
// client hanging up while the deliberately-detached restore completes.
type restoreRaceBlob struct {
	blob.BlobStore // nil: only the methods below are exercised
	uploads        int
	cancel         context.CancelFunc
}

func (b *restoreRaceBlob) Stat(context.Context, string) (int64, error) {
	return 0, blob.ErrNotFound
}

func (b *restoreRaceBlob) UploadFile(context.Context, string, string) error {
	b.uploads++
	if b.uploads == 2 {
		b.cancel()
	}
	return nil
}

// bumpRecordingStore records the liveness of the context the
// floor-generation bump receives.
type bumpRecordingStore struct {
	ref.RefStore // nil: only the methods below are exercised
	registered   bool
	reserved     bool
	covered      bool
	bumped       bool
	bumpCtxErr   error
	bumpDeadline time.Time
}

func (s *bumpRecordingStore) RegisterPacks(context.Context, string, []ref.Pack) error {
	return nil
}

func (s *bumpRecordingStore) HasPack(context.Context, string, string) (bool, error) {
	return s.registered, nil
}

func (s *bumpRecordingStore) ReservePackBytes(_ context.Context, _ []string, packs []ref.Pack, _ string, _, _, _ int64, _ time.Duration) (ref.ReserveDecision, error) {
	for _, p := range packs {
		if strings.HasSuffix(p.Key, ref.RepairCoverSuffix) {
			s.covered = true // the seam's restore cover: required, not a charge
		} else {
			s.reserved = true // an admission charge on the real key
		}
	}
	return ref.ReserveDecision{OK: true}, nil
}

func (s *bumpRecordingStore) BumpLFSAdoptionGen(ctx context.Context, _ string) error {
	s.bumped = true
	s.bumpCtxErr = ctx.Err()
	s.bumpDeadline, _ = ctx.Deadline()
	return ctx.Err()
}

// TestPublishPackRestoreBumpSurvivesClientCancel (Codex round 68,
// security): EnsurePublished deliberately survives request cancellation,
// but the post-restore floor-generation bump reused the request context —
// a client disconnecting after registration skipped the invalidation, and
// with the covers expired a later writer could settle against the
// absence-window floor. The bump must run detached and bounded, like the
// restore it concludes.
func TestPublishPackRestoreBumpSurvivesClientCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	packPath := filepath.Join(t.TempDir(), "pack")
	if err := os.WriteFile(packPath, []byte("pack-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	st := &bumpRecordingStore{}
	s := &server{
		refStore:  st,
		blobStore: &restoreRaceBlob{cancel: cancel},
		logger:    discardLogger(),
		// Quota off: the restore path itself is under test, not admission.
	}
	if err := s.publishPack(ctx, "repo_rb", "abcd", packPath); err != nil {
		t.Fatalf("publishPack: %v", err)
	}
	if !st.bumped {
		t.Fatal("restore did not bump the floor generation")
	}
	if st.bumpCtxErr != nil {
		t.Fatalf("bump ran on the canceled request context (ctx err %v) — it must be detached like the restore", st.bumpCtxErr)
	}
	// The bump must commit while the last cover row still lives (Codex
	// round 69): after a broken tail refresh that can be ~16s, so the
	// handoff bound must be CoverHandoffTimeout, not a generous minute.
	if st.bumpDeadline.IsZero() || time.Until(st.bumpDeadline) > blob.CoverHandoffTimeout+time.Second {
		t.Fatalf("bump deadline %v exceeds the cover handoff window (%v)", time.Until(st.bumpDeadline), blob.CoverHandoffTimeout)
	}
}

// TestPublishPackRegisteredMissingIsRestoredNotCharged (Codex round 72 —
// the REST twin of the hook's round-70 classification): a pack whose key
// is registered but whose blob is missing must be treated as a RESTORE:
// no admission charge (its bytes are already in the counter — charging
// them can falsely reject an at-quota repair, and the zero-credit
// registration no-op would consume the reservation without any bump),
// with the republication going through the covered, generation-bumping
// seam.
func TestPublishPackRegisteredMissingIsRestoredNotCharged(t *testing.T) {
	ctx := context.Background()
	packPath := filepath.Join(t.TempDir(), "pack")
	if err := os.WriteFile(packPath, []byte("pack-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	st := &bumpRecordingStore{registered: true}
	s := &server{
		refStore:  st,
		blobStore: &restoreRaceBlob{cancel: func() {}},
		logger:    discardLogger(),
		// Quota ON: the restore must bypass the admission charge.
		packQuotaBytes:     1 << 20,
		packReservationTTL: time.Hour,
		usageFloorFn: func(context.Context, string) (int64, int64, error) {
			t.Fatal("restore consulted the usage floor (admission path)")
			return 0, 0, nil
		},
	}
	if err := s.publishPack(ctx, "repo_rm", "abcd", packPath); err != nil {
		t.Fatalf("publishPack: %v", err)
	}
	if st.reserved {
		t.Fatal("registered-but-missing pack was charged as fresh growth")
	}
	if !st.covered {
		t.Fatal("restore took no distinct cover reservation")
	}
	if !st.bumped {
		t.Fatal("restore did not hand off the floor-generation bump")
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
