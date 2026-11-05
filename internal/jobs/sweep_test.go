// Orphan-pack sweeper tests (issue #5): classification (live / fresh /
// orphan / namespace / foreign), list-only vs delete disposal, the
// local-mode ticker, and the River periodic wiring (docker).
package jobs

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/sync"
	"github.com/coder/openstorage/internal/testutil/testimages"
)

// sweepFixture is one zero-container sweep environment: a fileblob store, a
// sqlite pack registry, and helpers to seed packs and backdate blobs (the
// fileblob ModTime is the file mtime, so os.Chtimes ages an upload for
// real, exercising the ModTime plumbing end-to-end).
type sweepFixture struct {
	t     *testing.T
	blobs blob.BlobStore
	dir   string
	store ref.Store
}

func newSweepFixture(t *testing.T) *sweepFixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	blobs, err := blob.OpenFile("file://" + filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	store, err := ref.Open(ctx, "sqlite://"+filepath.Join(dir, "refs.db"))
	if err != nil {
		t.Fatalf("open sqlite ref store: %v", err)
	}
	t.Cleanup(store.Close)
	if err := store.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return &sweepFixture{t: t, blobs: blobs, dir: filepath.Join(dir, "blobs"), store: store}
}

// upload puts content at key and returns its size.
func (f *sweepFixture) upload(key, content string) int64 {
	f.t.Helper()
	src := filepath.Join(f.t.TempDir(), "src")
	if err := os.WriteFile(src, []byte(content), 0o644); err != nil {
		f.t.Fatal(err)
	}
	if err := f.blobs.UploadFile(context.Background(), key, src); err != nil {
		f.t.Fatalf("UploadFile(%q): %v", key, err)
	}
	return int64(len(content))
}

// backdate ages a fileblob object by moving its mtime into the past.
func (f *sweepFixture) backdate(key string, age time.Duration) {
	f.t.Helper()
	past := time.Now().Add(-age)
	if err := os.Chtimes(filepath.Join(f.dir, filepath.FromSlash(key)), past, past); err != nil {
		f.t.Fatalf("backdate %q: %v", key, err)
	}
}

// register records the pack in the authoritative registry.
func (f *sweepFixture) register(repo, id, key string) {
	f.t.Helper()
	if err := f.store.RegisterPacks(context.Background(), repo, []ref.Pack{{ID: id, Key: key}}); err != nil {
		f.t.Fatalf("RegisterPacks(%q): %v", repo, err)
	}
}

func (f *sweepFixture) exists(key string) bool {
	f.t.Helper()
	_, err := f.blobs.Stat(context.Background(), key)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		f.t.Fatalf("Stat(%q): %v", key, err)
	}
	return err == nil
}

// backupKeys returns the quarantine entries (any nonce) backing key.
func (f *sweepFixture) backupKeys(key string) []string {
	f.t.Helper()
	entries, err := f.blobs.List(context.Background(), quarantinePrefix)
	if err != nil {
		f.t.Fatalf("List(%q): %v", quarantinePrefix, err)
	}
	var keys []string
	for _, e := range entries {
		if orig, ok := parseQuarantineKey(e.Key); ok && orig == key {
			keys = append(keys, e.Key)
		}
	}
	return keys
}

// hasBackup reports whether some quarantine entry (any nonce) backs key.
func (f *sweepFixture) hasBackup(key string) bool {
	f.t.Helper()
	return len(f.backupKeys(key)) > 0
}

func (f *sweepFixture) sweeper(cfg SweepConfig) *Sweeper {
	f.t.Helper()
	return NewSweeper(f.blobs, f.store, cfg, slog.New(slog.DiscardHandler))
}

// seedSweepWorld builds the classification matrix shared by the list-only
// and delete tests. Returns the orphan keys (base repo + namespace).
func seedSweepWorld(f *sweepFixture) (oldOrphan, nsOrphan, liveKey, nsLiveKey, freshKey string) {
	liveKey = blob.PackKey("r1", "live")
	f.upload(liveKey, "live-pack-bytes")
	f.register("r1", "live", liveKey)

	// Namespaced repos keep separate pack spaces registered under the FULL
	// name (packs/r1+import/...): the sweeper must resolve them against
	// their own registry rows, never misclassify them against "r1".
	nsLiveKey = blob.PackKey("r1+import", "nslive")
	f.upload(nsLiveKey, "ns-live-bytes")
	f.register("r1+import", "nslive", nsLiveKey)
	f.backdate(nsLiveKey, 25*time.Hour) // old AND registered: still live

	oldOrphan = blob.PackKey("r1", "orphan")
	f.upload(oldOrphan, "orphan-pack-bytes-!")
	f.backdate(oldOrphan, 25*time.Hour)

	nsOrphan = blob.PackKey("r1+import", "nsorphan")
	f.upload(nsOrphan, "ns-orphan")
	f.backdate(nsOrphan, 25*time.Hour)

	freshKey = blob.PackKey("r1", "fresh")
	f.upload(freshKey, "fresh-unregistered")

	// Foreign keys under packs/ that do not match the exact PackKey shape:
	// never classified, never deleted, no matter how old (Codex P2).
	for _, foreign := range []string{"packs/stray", "packs/r1/manifest.json", "packs/r1/archive/data"} {
		f.upload(foreign, "not-a-pack-key")
		f.backdate(foreign, 30*24*time.Hour)
	}
	return
}

func TestSweep_ListOnlyReportsWithoutDeleting(t *testing.T) {
	f := newSweepFixture(t)
	oldOrphan, nsOrphan, liveKey, nsLiveKey, freshKey := seedSweepWorld(f)

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	wantOrphans := map[string]int64{oldOrphan: int64(len("orphan-pack-bytes-!")), nsOrphan: int64(len("ns-orphan"))}
	if len(report.Orphans) != len(wantOrphans) {
		t.Fatalf("orphans = %+v, want keys %v", report.Orphans, wantOrphans)
	}
	var wantBytes int64
	for _, o := range report.Orphans {
		size, ok := wantOrphans[o.Key]
		if !ok || o.Size != size || o.Age < 24*time.Hour {
			t.Fatalf("unexpected orphan %+v (want one of %v, age >= 24h)", o, wantOrphans)
		}
		wantBytes += size
	}
	if report.OrphanBytes != wantBytes {
		t.Fatalf("OrphanBytes = %d, want %d", report.OrphanBytes, wantBytes)
	}
	if report.Live != 2 || report.Fresh != 1 || report.Deleted != 0 || report.BytesReclaimed != 0 {
		t.Fatalf("report = %+v, want live=2 fresh=1 deleted=0", report)
	}
	// List-only NEVER deletes, orphans included.
	for _, key := range []string{liveKey, nsLiveKey, freshKey, oldOrphan, nsOrphan,
		"packs/stray", "packs/r1/manifest.json", "packs/r1/archive/data"} {
		if !f.exists(key) {
			t.Fatalf("list-only sweep deleted %q", key)
		}
	}
}

func TestSweep_DeleteReapsOrphansOnly(t *testing.T) {
	f := newSweepFixture(t)
	oldOrphan, nsOrphan, liveKey, nsLiveKey, freshKey := seedSweepWorld(f)

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// Reaping moves bytes into quarantine — nothing is reclaimed yet.
	if report.Deleted != 2 || report.BytesReclaimed != 0 {
		t.Fatalf("report = %+v, want deleted=2 bytes_reclaimed=0 (bytes still quarantined)", report)
	}
	for _, key := range []string{oldOrphan, nsOrphan} {
		if f.exists(key) {
			t.Fatalf("orphan %q survived a delete-enabled sweep", key)
		}
	}
	// Live packs, fresh uploads, and foreign keys (however old) survive.
	for _, key := range []string{liveKey, nsLiveKey, freshKey,
		"packs/stray", "packs/r1/manifest.json", "packs/r1/archive/data"} {
		if !f.exists(key) {
			t.Fatalf("delete-enabled sweep deleted non-orphan %q", key)
		}
	}

	// Reaped orphans leave quarantine backups behind (healQuarantine purges
	// them once they age past the threshold).
	for _, key := range []string{oldOrphan, nsOrphan} {
		if !f.hasBackup(key) {
			t.Fatalf("reaped orphan %q has no quarantine backup", key)
		}
	}

	// Idempotence: a second sweep finds nothing left to do (fresh backups
	// are kept, so still nothing reclaimed).
	again, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if len(again.Orphans) != 0 || again.Deleted != 0 || again.BytesReclaimed != 0 {
		t.Fatalf("second sweep report = %+v, want no orphans and nothing reclaimed", again)
	}

	// Once the backups age past the threshold, the purge reclaims exactly
	// the orphans' bytes — the point storage consumption actually drops.
	for _, key := range []string{oldOrphan, nsOrphan} {
		for _, qkey := range f.backupKeys(key) {
			f.backdate(qkey, 25*time.Hour)
		}
	}
	purged, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("purge Sweep: %v", err)
	}
	if purged.BytesReclaimed != report.OrphanBytes {
		t.Fatalf("purge report = %+v, want bytes_reclaimed=%d", purged, report.OrphanBytes)
	}
	for _, key := range []string{oldOrphan, nsOrphan} {
		if f.hasBackup(key) {
			t.Fatalf("aged backup %q was not purged", key)
		}
	}
}

// A blob exactly at the threshold boundary is swept; just under it is not.
// The injected clock pins the boundary deterministically.
func TestSweep_ThresholdBoundary(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "boundary")
	f.upload(key, "x")

	objs, err := f.blobs.List(context.Background(), blob.PacksRoot)
	if err != nil || len(objs) != 1 {
		t.Fatalf("List = %v, %v; want the one upload", objs, err)
	}
	uploaded := objs[0].ModTime

	s := f.sweeper(SweepConfig{Threshold: 24 * time.Hour})
	s.now = func() time.Time { return uploaded.Add(24*time.Hour - time.Nanosecond) }
	report, err := s.Sweep(context.Background())
	if err != nil || len(report.Orphans) != 0 || report.Fresh != 1 {
		t.Fatalf("just-under-threshold report = %+v (err %v), want fresh=1", report, err)
	}

	s.now = func() time.Time { return uploaded.Add(24 * time.Hour) }
	report, err = s.Sweep(context.Background())
	if err != nil || len(report.Orphans) != 1 {
		t.Fatalf("at-threshold report = %+v (err %v), want one orphan", report, err)
	}
}

// stubLedger supplies no-op quota-ledger methods for PackRegistry fakes
// that only script HasPack behavior (the real ledger paths are covered by
// the fixtures running over an actual ref.Store).
type stubLedger struct{}

func (stubLedger) IsRetainedPack(context.Context, string, string) (bool, error) {
	return false, nil
}
func (stubLedger) LFSObjectRegisteredAt(context.Context, string, string) (time.Time, int64, bool, error) {
	return time.Time{}, 0, false, nil
}
func (stubLedger) ReclaimRetainedPack(context.Context, string, string) (int64, error) {
	return 0, nil
}
func (stubLedger) SweepExpiredPackReservations(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

func (stubLedger) ReconcileLegacyLFSObjects(context.Context) (int64, bool, error) {
	return 0, false, nil
}
func (stubLedger) PackRegisteredBytes(context.Context, string, string) (int64, bool, error) {
	return 0, false, nil
}
func (stubLedger) TryAdoptionClaim(context.Context, string, string) (bool, string, error) {
	return true, "stub-owner", nil
}
func (stubLedger) RenewAdoptionClaim(context.Context, string, string) (bool, error) {
	return true, nil
}
func (stubLedger) ReleaseAdoptionClaim(context.Context, string, string) error {
	return nil
}
func (stubLedger) HasLFSObject(context.Context, string, string) (bool, error) {
	return false, nil
}
func (stubLedger) BumpLFSAdoptionGen(context.Context, string) error {
	return nil
}
func (stubLedger) ReservePackBytes(context.Context, []string, []ref.Pack, string, int64, int64, int64, time.Duration) (ref.ReserveDecision, error) {
	return ref.ReserveDecision{OK: true}, nil
}

// GuardedLFSDelete runs del directly and reports it executed: stub-based
// tests inject races via HasLFSObject call counting, so the guard itself
// stays transparent unless a test overrides it.
func (stubLedger) GuardedLFSDelete(ctx context.Context, _, _ string, del func(context.Context) error) (bool, error) {
	if err := del(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// registeredMidSweep reports unregistered on the first HasPack call and
// registered afterwards, simulating a pack whose RegisterPacks commits
// between the sweep's classification read and its delete.
type registeredMidSweep struct {
	stubLedger
	calls int
}

func (p *registeredMidSweep) HasPack(context.Context, string, string) (bool, error) {
	p.calls++
	return p.calls > 1, nil
}

// A pack registered mid-sweep must survive: the delete-time revalidation
// re-reads the registry instead of trusting the classification snapshot
// (the retried-push race from the Codex review).
func TestSweep_DeleteSkipsPackRegisteredMidSweep(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "racer")
	f.upload(key, "racer-bytes")
	f.backdate(key, 25*time.Hour)

	packs := &registeredMidSweep{}
	s := NewSweeper(f.blobs, packs, SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	report, err := s.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want no deletes (pack registered mid-sweep)", report)
	}
	if len(report.Orphans) != 1 { // classified from the snapshot, then spared
		t.Fatalf("orphans = %+v, want the snapshot candidate", report.Orphans)
	}
	if !f.exists(key) {
		t.Fatalf("mid-sweep-registered pack %q was deleted", key)
	}
	if packs.calls != 2 {
		t.Fatalf("HasPack calls = %d, want 2 (classification + revalidation)", packs.calls)
	}
}

// registeredAfterCall flips from unregistered to registered strictly after
// the Nth HasPack call, simulating a RegisterPacks landing at a chosen
// point in the sweep.
type registeredAfterCall struct {
	stubLedger
	after int
	calls int
}

func (p *registeredAfterCall) HasPack(context.Context, string, string) (bool, error) {
	p.calls++
	return p.calls > p.after, nil
}

// A registration landing between revalidation and the delete (the
// narrowest race) must be repaired by safeDelete's post-delete recheck:
// the identical bytes are restored from the local backup (Codex P1 round 2).
func TestSweep_DeleteRestoresPackRegisteredMidDelete(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "middelete")
	content := "mid-delete-bytes"
	f.upload(key, content)
	f.backdate(key, 25*time.Hour)

	// Calls: 1 = classification read, 2 = revalidateOrphan (both see it
	// unregistered), 3 = safeDelete's post-delete recheck (registered).
	packs := &registeredAfterCall{after: 2}
	s := NewSweeper(f.blobs, packs, SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	report, err := s.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want no deletes (registered mid-delete)", report)
	}
	if !f.exists(key) {
		t.Fatalf("pack registered mid-delete %q was not restored", key)
	}
	dest := filepath.Join(t.TempDir(), "restored.pack")
	if err := f.blobs.DownloadFile(context.Background(), key, dest); err != nil {
		t.Fatalf("download restored pack: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != content {
		t.Fatalf("restored content = %q (err %v), want %q", got, err, content)
	}
	// Retained on purpose: purging is strictly age-based so a fresh entry
	// can never be yanked out from under another replica's handshake.
	if !f.hasBackup(key) {
		t.Fatalf("quarantine backup must be retained after a mid-delete restore")
	}
}

// A list-only sweep (the operator's rollback mode) must still heal a
// registered-but-missing pack from quarantine — but never purge anything.
func TestSweep_ListOnlyStillHealsQuarantine(t *testing.T) {
	f := newSweepFixture(t)

	// Delete-enabled crash mid-handshake, then rollback to list-only.
	lost := blob.PackKey("r1", "lost")
	f.upload(quarantinePrefix+"00112233aabbccdd/"+lost, "lost-bytes")
	f.register("r1", "lost", lost)

	// An aged unregistered backup: list-only must NOT purge it.
	stale := blob.PackKey("r1", "stale")
	f.upload(quarantinePrefix+"00112233aabbccdd/"+stale, "stale-bytes")
	f.backdate(quarantinePrefix+"00112233aabbccdd/"+stale, 25*time.Hour)

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(lost) {
		t.Fatalf("list-only sweep did not heal registered-but-missing pack %q", lost)
	}
	if !f.exists(quarantinePrefix + "00112233aabbccdd/" + lost) {
		t.Fatalf("list-only heal purged fresh backup %q (purging is delete- and age-gated)", lost)
	}
	if !f.exists(quarantinePrefix + "00112233aabbccdd/" + stale) {
		t.Fatalf("list-only sweep purged %q (purging must stay delete-gated)", stale)
	}
	if report.Deleted != 0 || report.BytesReclaimed != 0 {
		t.Fatalf("report = %+v, want nothing deleted or reclaimed in list-only", report)
	}
}

// Heal is the standalone restorative pass serve runs at startup when the
// periodic schedule is disabled (interval 0): registered-but-missing packs
// are restored, and nothing unregistered is ever purged — even with
// Delete=true in the config, interval zero means "no sweeping", so an
// emergency stop must never be destructive.
func TestSweeper_HealStandalone(t *testing.T) {
	f := newSweepFixture(t)
	lost := blob.PackKey("r1", "lost")
	f.upload(quarantinePrefix+"00112233aabbccdd/"+lost, "lost-bytes")
	f.register("r1", "lost", lost)
	stale := blob.PackKey("r1", "stale")
	f.upload(quarantinePrefix+"00112233aabbccdd/"+stale, "stale-bytes")
	f.backdate(quarantinePrefix+"00112233aabbccdd/"+stale, 25*time.Hour)

	// Delete=true on purpose: the standalone heal must force purging off.
	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Heal(context.Background()); err != nil {
		t.Fatalf("Heal: %v", err)
	}
	if !f.exists(lost) {
		t.Fatalf("standalone heal did not restore %q", lost)
	}
	if !f.exists(quarantinePrefix + "00112233aabbccdd/" + lost) {
		t.Fatalf("standalone heal purged backup %q (must be strictly restorative)", lost)
	}
	if !f.exists(quarantinePrefix + "00112233aabbccdd/" + stale) {
		t.Fatalf("standalone heal purged %q despite Delete=true (must be strictly restorative)", stale)
	}
}

// healQuarantine finishes a crashed handshake: a quarantine backup whose
// original key is registered but missing is restored; any backup is purged
// only once it ages past the threshold (fresh entries may be another
// handshake's restore source).
func TestSweep_HealQuarantine(t *testing.T) {
	f := newSweepFixture(t)

	// Crashed handshake: registered pack, blob missing, backup present and
	// aged (so this delete-mode sweep may purge it after restoring).
	lost := blob.PackKey("r1", "lost")
	f.upload(quarantinePrefix+"00112233aabbccdd/"+lost, "lost-bytes")
	f.backdate(quarantinePrefix+"00112233aabbccdd/"+lost, 25*time.Hour)
	f.register("r1", "lost", lost)

	// Reaped long ago: unregistered, backup aged past the threshold.
	stale := blob.PackKey("r1", "stale")
	f.upload(quarantinePrefix+"00112233aabbccdd/"+stale, "stale-bytes")
	f.backdate(quarantinePrefix+"00112233aabbccdd/"+stale, 25*time.Hour)

	// In-flight sweep's backup: unregistered but fresh — must be kept.
	inflight := blob.PackKey("r1", "inflight")
	f.upload(quarantinePrefix+"00112233aabbccdd/"+inflight, "inflight-bytes")

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(lost) {
		t.Fatalf("registered-but-missing pack %q was not healed from quarantine", lost)
	}
	if f.exists(quarantinePrefix + "00112233aabbccdd/" + lost) {
		t.Fatalf("aged healed backup %q was not purged", lost)
	}
	if f.exists(quarantinePrefix + "00112233aabbccdd/" + stale) {
		t.Fatalf("aged unregistered backup %q was not purged", stale)
	}
	if !f.exists(quarantinePrefix + "00112233aabbccdd/" + inflight) {
		t.Fatalf("fresh backup %q must be kept (possibly an in-flight sweep)", inflight)
	}
	// The healed pack is registered => live in the same sweep's report.
	if report.Live != 1 {
		t.Fatalf("report = %+v, want the healed pack counted live", report)
	}
	// Reclaimed = the aged unregistered purge only; the heal's
	// restore-then-purge is net zero and must not be credited.
	if want := int64(len("stale-bytes")); report.BytesReclaimed != want {
		t.Fatalf("BytesReclaimed = %d, want %d (stale purge only)", report.BytesReclaimed, want)
	}
}

// content reads key's full bytes (test-sized objects only).
func (f *sweepFixture) content(key string) string {
	f.t.Helper()
	size, err := f.blobs.Stat(context.Background(), key)
	if err != nil {
		f.t.Fatalf("Stat(%q): %v", key, err)
	}
	b, err := f.blobs.ReadRange(context.Background(), key, 0, size)
	if err != nil {
		f.t.Fatalf("ReadRange(%q): %v", key, err)
	}
	return string(b)
}

// registerSized records the pack with its byte size (packs.bytes > 0), the
// post-#101 shape the size-aware heal classifies against.
func (f *sweepFixture) registerSized(repo, id, key string, size int64) {
	f.t.Helper()
	if err := f.store.RegisterPacks(context.Background(), repo, []ref.Pack{{ID: id, Key: key, Size: size}}); err != nil {
		f.t.Fatalf("RegisterPacks(%q): %v", repo, err)
	}
}

// A registered pack that is PRESENT but SHORTER than its registered size
// (issue #129: still-valid presigned overwrite, storage-layer damage) must
// be healed from an aged quarantine backup holding the complete bytes —
// mere existence of the original must not let the age purge collect the
// object's only complete copy.
func TestSweep_HealRestoresTruncatedRegisteredPack(t *testing.T) {
	f := newSweepFixture(t)
	full := "complete-pack-bytes"
	key := blob.PackKey("r1", "shorted")
	f.upload(key, full[:5])
	f.registerSized("r1", "shorted", key, int64(len(full)))
	qKey := quarantinePrefix + "00112233aabbccdd/" + key
	f.upload(qKey, full)
	f.backdate(qKey, 25*time.Hour)

	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got := f.content(key); got != full {
		t.Fatalf("truncated registered pack %q = %q after sweep, want the backup's complete bytes restored", key, got)
	}
	// Healed => the original is complete again; the aged backup purges.
	if f.exists(qKey) {
		t.Fatalf("aged backup %q survived after the truncated original was healed", qKey)
	}
}

// A registered pack row that predates the packs.bytes migration (bytes=0)
// makes completeness unknowable: the aged purge must SPARE the backup —
// it may hold the object's only complete bytes and nothing can prove the
// original whole (issue #129's conservative unknown-size rule).
func TestSweep_HealSparesBackupOfUnknownSizePack(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "presize")
	f.upload(key, "maybe-truncated")
	f.register("r1", "presize", key) // Size 0 => pre-migration row shape
	qKey := quarantinePrefix + "00112233aabbccdd/" + key
	f.upload(qKey, "possibly-the-only-complete-copy")
	f.backdate(qKey, 25*time.Hour)

	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(qKey) {
		t.Fatalf("aged backup %q of an unknown-size registered pack was purged — completeness was unverifiable", qKey)
	}
	// Unknown size classifies nothing: the original must not be rewritten.
	if got := f.content(key); got != "maybe-truncated" {
		t.Fatalf("unknown-size original %q = %q, want untouched", key, got)
	}
}

// A truncated registered pack whose backup is ALSO shorter than the
// registered size cannot be completed by a restore: the heal must not
// rewrite the original (the restore could even regress a longer prefix),
// and the purge must spare the backup (issue #129).
func TestSweep_HealSparesTruncatedPackWithShortBackup(t *testing.T) {
	f := newSweepFixture(t)
	full := "complete-pack-bytes"
	key := blob.PackKey("r1", "bothshort")
	f.upload(key, full[:10])
	f.registerSized("r1", "bothshort", key, int64(len(full)))
	qKey := quarantinePrefix + "00112233aabbccdd/" + key
	f.upload(qKey, full[:7])
	f.backdate(qKey, 25*time.Hour)

	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got := f.content(key); got != full[:10] {
		t.Fatalf("original %q = %q, want untouched (backup cannot complete it)", key, got)
	}
	if !f.exists(qKey) {
		t.Fatalf("backup %q was purged while the registered original is provably incomplete", qKey)
	}
}

// refreshedMidSweep serves the real store's listing for the snapshot but
// reports a fresh ModTime on the exact-key revalidation read, simulating a
// retried push overwriting the orphaned key mid-sweep.
type refreshedMidSweep struct {
	blob.BlobStore
	key string
}

func (b *refreshedMidSweep) List(ctx context.Context, prefix string) ([]blob.ListedObject, error) {
	objs, err := b.BlobStore.List(ctx, prefix)
	if prefix != b.key {
		return objs, err
	}
	for i := range objs {
		if objs[i].Key == b.key {
			objs[i].ModTime = time.Now()
		}
	}
	return objs, err
}

// An orphaned key overwritten by a retried push mid-sweep must survive: the
// revalidation reads the object's age at delete time, not list time.
func TestSweep_DeleteSkipsKeyRefreshedMidSweep(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "refreshed")
	f.upload(key, "refreshed-bytes")
	f.backdate(key, 25*time.Hour)

	s := NewSweeper(&refreshedMidSweep{BlobStore: f.blobs, key: key}, f.store,
		SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	report, err := s.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want no deletes (key refreshed mid-sweep)", report)
	}
	if !f.exists(key) {
		t.Fatalf("mid-sweep-refreshed key %q was deleted", key)
	}
}

// The report's orphan list is a bounded sample: counts and bytes stay
// exact however large the backlog, but memory must not scale with it
// (Codex round 15).
func TestSweep_OrphanSampleBounded(t *testing.T) {
	orig := maxOrphanSample
	maxOrphanSample = 2
	t.Cleanup(func() { maxOrphanSample = orig })

	f := newSweepFixture(t)
	var total int64
	for _, id := range []string{"a", "b", "c"} {
		key := blob.PackKey("r1", id)
		total += f.upload(key, "orphan-"+id)
		f.backdate(key, 25*time.Hour)
	}
	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.OrphanCount != 3 || report.OrphanBytes != total {
		t.Fatalf("report = %+v, want exact count 3 and bytes %d", report, total)
	}
	if len(report.Orphans) != 2 {
		t.Fatalf("sample holds %d entries, want the maxOrphanSample cap of 2", len(report.Orphans))
	}
}

// quarantineKey mints unique write-once backup keys that parse back to the
// original pack key (Codex round 14); anything not in the nonce layout must
// never resolve to a valid pack key (the heal skips it as foreign).
func TestQuarantineKey(t *testing.T) {
	orig := blob.PackKey("r1", "abc")
	k1, k2 := quarantineKey(orig), quarantineKey(orig)
	if k1 == k2 {
		t.Fatalf("quarantineKey minted the same key twice: %q", k1)
	}
	for _, k := range []string{k1, k2} {
		got, ok := parseQuarantineKey(k)
		if !ok || got != orig {
			t.Fatalf("parseQuarantineKey(%q) = %q, %v; want %q, true", k, got, ok, orig)
		}
	}
	for _, k := range []string{
		"other/key",
		quarantinePrefix + "junk",
		quarantinePrefix + orig,                         // legacy shared-key layout: foreign
		quarantinePrefix + "not-a-nonce/" + orig,        // non-hex first segment
		quarantinePrefix + "00112233AABBCCDD/" + orig,   // uppercase hex: not ours
		quarantinePrefix + "00112233aabbccdd42/" + orig, // wrong length
	} {
		if _, ok := parseQuarantineKey(k); ok {
			t.Fatalf("parseQuarantineKey(%q) accepted a foreign key", k)
		}
	}
}

// A failing DeleteGuard downgrades the run to list-only: candidates are
// reported, nothing is reaped or purged, and the guard error is returned
// (Codex round 21).
func TestSweep_DeleteGuardFailsClosed(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "guarded")
	f.upload(key, "guarded-bytes")
	f.backdate(key, 25*time.Hour)
	aged := quarantinePrefix + "00112233aabbccdd/" + blob.PackKey("r1", "stale")
	f.upload(aged, "stale-bytes")
	f.backdate(aged, QuarantinePurgeFloor+time.Hour)

	guardErr := errors.New("simulated: versioning enabled mid-flight")
	s := f.sweeper(SweepConfig{
		Threshold:   24 * time.Hour,
		Delete:      true,
		DeleteGuard: func(context.Context) error { return guardErr },
	})
	report, err := s.Sweep(context.Background())
	if !errors.Is(err, guardErr) {
		t.Fatalf("Sweep err = %v, want the guard error", err)
	}
	if report.OrphanCount != 1 || report.Deleted != 0 || report.BytesReclaimed != 0 {
		t.Fatalf("report = %+v, want 1 orphan reported and nothing destroyed", report)
	}
	if !f.exists(key) || !f.exists(aged) {
		t.Fatalf("guard-failed sweep destroyed objects (key=%v aged backup=%v)", f.exists(key), f.exists(aged))
	}

	// A passing guard restores full delete mode.
	s2 := f.sweeper(SweepConfig{
		Threshold:   24 * time.Hour,
		Delete:      true,
		DeleteGuard: func(context.Context) error { return nil },
	})
	report2, err := s2.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep with passing guard: %v", err)
	}
	if report2.Deleted != 1 {
		t.Fatalf("report = %+v, want the orphan reaped once the guard passes", report2)
	}
}

// TestSweep_WatermarkWithheldOnGuardDrift (Codex round 10 on PR #132): the
// destructive guard is re-consulted IMMEDIATELY before recording the reap
// watermark — a precondition that drifts DURING the pass (e.g. S3
// versioning enabled mid-sweep turns deletes into delete markers) must
// withhold the watermark, or reclamation refunds slots for up to the grace
// window on reaps that did not physically happen.
func TestSweep_WatermarkWithheldOnGuardDrift(t *testing.T) {
	f := newSweepFixture(t)
	guardCalls, recorded := 0, 0
	s := f.sweeper(SweepConfig{
		Threshold: 24 * time.Hour,
		DeleteLFS: true,
		DeleteGuard: func(context.Context) error {
			guardCalls++
			if guardCalls > 1 {
				return errors.New("simulated: versioning enabled mid-pass")
			}
			return nil
		},
		RecordLFSReapComplete: func(context.Context, int64, bool) error { recorded++; return nil },
	})
	if _, err := s.Sweep(context.Background()); err == nil {
		t.Fatal("Sweep: want the drifted guard surfaced in the error join")
	}
	if recorded != 0 || guardCalls != 2 {
		t.Fatalf("recorded=%d guardCalls=%d, want the watermark withheld after the start + pre-record consults", recorded, guardCalls)
	}

	// Control: a steady guard records the watermark exactly once.
	recorded = 0
	s2 := f.sweeper(SweepConfig{
		Threshold:             24 * time.Hour,
		DeleteLFS:             true,
		DeleteGuard:           func(context.Context) error { return nil },
		RecordLFSReapComplete: func(context.Context, int64, bool) error { recorded++; return nil },
	})
	if _, err := s2.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep with steady guard: %v", err)
	}
	if recorded != 1 {
		t.Fatalf("recorded=%d, want exactly one watermark write under a steady guard", recorded)
	}
}

// zeroModTime hides one key's mod time from listings, simulating a backend
// entry whose age is unknowable.
type zeroModTime struct {
	blob.BlobStore
	key string
}

func (b *zeroModTime) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	return b.BlobStore.ListPages(ctx, prefix, pageSize, func(objs []blob.ListedObject) error {
		for i := range objs {
			if objs[i].Key == b.key {
				objs[i].ModTime = time.Time{}
			}
		}
		return fn(objs)
	})
}

// TestSweep_WatermarkWithheldOnUnknownAgeLFS (Codex round 37 on PR #132): a
// zero-ModTime backend listing makes an offered LFS candidate's age
// unknowable, so classify skips it — but the pass stamped the reap epoch at
// start, and certifying completion would settle refund buckets (refilling
// object-cap slots and byte headroom) for a blob that was offered and NOT
// reaped. An armed-class skip must withhold the completion watermark
// exactly like guard drift. An UNARMED legacy-spelled skip must not: the
// completion is not legacy-armed, legacy buckets stay pending regardless,
// and blocking canonical settlement on it would starve settleable buckets.
func TestSweep_WatermarkWithheldOnUnknownAgeLFS(t *testing.T) {
	f := newSweepFixture(t)
	oid := strings.Repeat("ab", 32)
	key := blob.LFSKey("r1", oid)
	f.upload(key, "orphan-lfs-bytes")
	f.backdate(key, 25*time.Hour)

	recorded := 0
	cfg := SweepConfig{
		Threshold:             24 * time.Hour,
		DeleteLFS:             true,
		RecordLFSReapComplete: func(context.Context, int64, bool) error { recorded++; return nil },
	}
	s := NewSweeper(&zeroModTime{BlobStore: f.blobs, key: key}, f.store, cfg, slog.New(slog.DiscardHandler))
	if _, err := s.Sweep(context.Background()); err == nil {
		t.Fatal("Sweep: want the unknowable-age armed skip surfaced in the error join")
	}
	if recorded != 0 {
		t.Fatalf("recorded=%d, want the watermark withheld while the offered candidate was not reaped", recorded)
	}

	// Control: once the backend reports a real mod time again, the same
	// pass shape reaps the orphan and certifies completion.
	report, err := f.sweeper(cfg).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep with real mod times: %v", err)
	}
	if report.Deleted != 1 || recorded != 1 {
		t.Fatalf("deleted=%d recorded=%d, want the orphan reaped and the watermark recorded", report.Deleted, recorded)
	}

	// An unarmed legacy-spelled skip stays warn-only.
	legacyKey := blob.LFSLegacyKey("r1", oid)
	f.upload(legacyKey, "legacy-lfs-bytes")
	f.backdate(legacyKey, 25*time.Hour)
	recorded = 0
	s3 := NewSweeper(&zeroModTime{BlobStore: f.blobs, key: legacyKey}, f.store, cfg, slog.New(slog.DiscardHandler))
	if _, err := s3.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep with unarmed legacy skip: %v", err)
	}
	if recorded != 1 {
		t.Fatalf("recorded=%d, want the watermark recorded despite the unarmed legacy skip", recorded)
	}
}

// A tiny classification threshold must not shrink the quarantine purge
// age below the handshake-lifetime floor: a backup older than the
// threshold but younger than QuarantinePurgeFloor may still be an
// in-flight handshake's restore source (Codex round 19).
func TestSweep_PurgeEnforcesFloorUnderTinyThreshold(t *testing.T) {
	f := newSweepFixture(t)
	young := quarantinePrefix + "00112233aabbccdd/" + blob.PackKey("r1", "young")
	f.upload(young, "young-bytes")
	f.backdate(young, 10*time.Minute) // aged past the 1m threshold, under the floor
	old := quarantinePrefix + "ddeeff0011223344/" + blob.PackKey("r1", "old")
	f.upload(old, "old-bytes")
	f.backdate(old, QuarantinePurgeFloor+time.Hour) // past the floor too

	report, err := f.sweeper(SweepConfig{Threshold: time.Minute, Delete: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(young) {
		t.Fatalf("backup %q under the purge floor was purged (threshold must not shrink the floor)", young)
	}
	if f.exists(old) {
		t.Fatalf("backup %q past the purge floor was not purged", old)
	}
	if want := int64(len("old-bytes")); report.BytesReclaimed != want {
		t.Fatalf("BytesReclaimed = %d, want %d (old purge only)", report.BytesReclaimed, want)
	}
}

// failingRegistry fails every HasPack, simulating a registry outage.
type failingRegistry struct{ stubLedger }

func (failingRegistry) HasPack(context.Context, string, string) (bool, error) {
	return false, errors.New("simulated registry outage")
}

// A registry outage during a quarantine scan must not accumulate one error
// per entry: retention is bounded while the elided count stays exact
// (Codex round 17).
func TestSweep_HealErrorsBounded(t *testing.T) {
	orig := maxSweepErrs
	maxSweepErrs = 1
	t.Cleanup(func() { maxSweepErrs = orig })

	f := newSweepFixture(t)
	for _, id := range []string{"a", "b", "c"} {
		f.upload(quarantinePrefix+"00112233aabbccdd/"+blob.PackKey("r1", id), "bytes")
	}
	s := NewSweeper(f.blobs, failingRegistry{}, SweepConfig{Threshold: 24 * time.Hour}, slog.New(slog.DiscardHandler))
	_, err := s.Sweep(context.Background())
	if err == nil {
		t.Fatal("Sweep: want joined heal errors, got nil")
	}
	if !strings.Contains(err.Error(), "2 additional errors elided") {
		t.Fatalf("error = %v, want exactly the elided-overflow count for 3 entries with cap 1", err)
	}
}

// An aged quarantine entry purged by a concurrent sweep between this
// sweep's listing and its delete must not be credited as reclaimed here:
// the deleting sweep already counted those bytes (Codex round 17).
func TestSweep_PurgeDoesNotCreditPeerDeletedEntry(t *testing.T) {
	f := newSweepFixture(t)
	orig := blob.PackKey("r1", "peer")
	f.upload(orig, "peer-bytes")
	f.register("r1", "peer", orig)
	// Listed as aged and unregistered-looking, but never uploaded: the
	// peer's purge already removed it.
	qkey := quarantinePrefix + "dddddddddddddddd/" + blob.PackKey("r1", "gone")

	s := NewSweeper(&peerPurged{BlobStore: f.blobs, orig: orig, qkey: qkey}, f.store,
		SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	report, err := s.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.BytesReclaimed != 0 {
		t.Fatalf("BytesReclaimed = %d, want 0 (peer's sweep already credited the purge)", report.BytesReclaimed)
	}
}

// cancelDuringDelete cancels the sweep's context while the delete request
// is in flight and reports the delete as failed — the shutdown-races-the-
// response class. Recovery (restore) must still complete (Codex round 16).
type cancelDuringDelete struct {
	blob.BlobStore
	key    string
	cancel context.CancelFunc
}

func (b *cancelDuringDelete) Delete(ctx context.Context, key string) error {
	err := b.BlobStore.Delete(ctx, key)
	if key == b.key {
		b.cancel()
		if err != nil {
			return err
		}
		return errors.New("simulated: sweep canceled while delete response outstanding")
	}
	return err
}

// A sweep canceled mid-delete must still restore the deleted bytes: the
// recovery phase runs detached from the sweep's cancellation.
func TestSweep_CanceledSweepStillRestores(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "shutdown")
	f.upload(key, "shutdown-bytes")
	f.backdate(key, 25*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewSweeper(&cancelDuringDelete{BlobStore: f.blobs, key: key, cancel: cancel}, f.store,
		SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	if _, err := s.Sweep(ctx); err == nil {
		t.Fatal("Sweep: want the ambiguous-delete error, got nil")
	}
	if !f.exists(key) {
		t.Fatalf("pack %q not restored after cancellation mid-delete", key)
	}
}

// Two reaps of the same content-addressed key (reap, retried push
// re-creates it, reap again) must keep BOTH backups: shared quarantine keys
// let one handshake overwrite another's restore source, which was the root
// of the round-13/14 purge races.
func TestSweep_RepeatedReapsKeepDistinctBackups(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "twice")
	s := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true})

	for i, content := range []string{"bytes-one", "bytes-two"} {
		f.upload(key, content)
		f.backdate(key, 25*time.Hour)
		report, err := s.Sweep(context.Background())
		if err != nil {
			t.Fatalf("Sweep %d: %v", i+1, err)
		}
		if report.Deleted != 1 {
			t.Fatalf("Sweep %d report = %+v, want 1 deleted", i+1, report)
		}
	}
	backups, err := f.blobs.List(context.Background(), quarantinePrefix)
	if err != nil {
		t.Fatalf("List quarantine: %v", err)
	}
	if len(backups) != 2 {
		t.Fatalf("quarantine holds %d backups, want 2 distinct (write-once keys)", len(backups))
	}
}

// peerPurged simulates a concurrent replica healing the same aged entry
// between this sweep's quarantine listing and its restore: the listed entry
// no longer exists, the original was missing at the first stat and present
// (peer-restored) at the re-stat.
type peerPurged struct {
	blob.BlobStore
	orig    string
	qkey    string
	statted bool
}

func (b *peerPurged) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	if prefix == quarantinePrefix {
		return fn([]blob.ListedObject{{Key: b.qkey, Size: 1, ModTime: time.Now().Add(-25 * time.Hour)}})
	}
	return b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

func (b *peerPurged) Stat(ctx context.Context, key string) (int64, error) {
	if key == b.orig && !b.statted {
		b.statted = true
		return 0, blob.ErrNotFound
	}
	return b.BlobStore.Stat(ctx, key)
}

// A heal whose restore source vanished mid-sweep must verify the peer's
// restore instead of failing: the peer purges only after restoring, so the
// original existing again is the success condition (Codex round 14).
func TestSweep_HealToleratesPeerPurgedBackup(t *testing.T) {
	f := newSweepFixture(t)
	orig := blob.PackKey("r1", "peer")
	f.upload(orig, "peer-bytes")
	f.register("r1", "peer", orig)
	qkey := quarantinePrefix + "cccccccccccccccc/" + orig // never uploaded: peer already purged it

	s := NewSweeper(&peerPurged{BlobStore: f.blobs, orig: orig, qkey: qkey}, f.store,
		SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	if _, err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(orig) {
		t.Fatalf("registered pack %q lost during peer-purged heal", orig)
	}
}

// deadlineCalls records the time left on the context of each call a heal
// entry makes, by call name.
type deadlineCalls struct {
	left map[string]time.Duration
}

func (d *deadlineCalls) record(call string, ctx context.Context) {
	d.left[call] = timeLeft(ctx)
}

type deadlineBlobs struct {
	blob.BlobStore
	calls *deadlineCalls
}

func (b *deadlineBlobs) Stat(ctx context.Context, key string) (int64, error) {
	b.calls.record("Stat", ctx)
	return b.BlobStore.Stat(ctx, key)
}

func (b *deadlineBlobs) Copy(ctx context.Context, dst, src string) error {
	b.calls.record("Copy", ctx)
	return b.BlobStore.Copy(ctx, dst, src)
}

func (b *deadlineBlobs) Delete(ctx context.Context, key string) error {
	b.calls.record("Delete", ctx)
	return b.BlobStore.Delete(ctx, key)
}

type deadlinePacks struct {
	PackRegistry
	calls *deadlineCalls
}

func (p *deadlinePacks) HasPack(ctx context.Context, repo, key string) (bool, error) {
	p.calls.record("HasPack", ctx)
	return p.PackRegistry.HasPack(ctx, repo, key)
}

// A quarantine heal entry runs from its registry read to its purge under its
// own deadline, even when the sweep's context has none: the quarantine purge
// age model needs a stalled entry to end well before the smallest margin.
func TestSweep_HealEntryRunsUnderItsOwnDeadline(t *testing.T) {
	f := newSweepFixture(t)
	lost := blob.PackKey("r1", "lost")
	qKey := quarantinePrefix + "00112233aabbccdd/" + lost
	f.upload(qKey, "lost-bytes")
	f.register("r1", "lost", lost) // registered, original missing: restore, then purge the aged backup

	calls := &deadlineCalls{left: map[string]time.Duration{}}
	s := NewSweeper(&deadlineBlobs{BlobStore: f.blobs, calls: calls}, &deadlinePacks{PackRegistry: f.store, calls: calls},
		SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	entry := blob.ListedObject{Key: qKey, Size: int64(len("lost-bytes")), ModTime: time.Now().Add(-25 * time.Hour)}
	if err := s.healQuarantineEntry(context.Background(), entry, &SweepReport{}, true, false); err != nil {
		t.Fatalf("healQuarantineEntry: %v", err)
	}
	if !f.exists(lost) || f.exists(qKey) {
		t.Fatalf("heal did not restore %q and purge its aged backup (original exists %v, backup exists %v)", lost, f.exists(lost), f.exists(qKey))
	}
	for _, call := range []string{"HasPack", "Stat", "Copy", "Delete"} {
		got, ok := calls.left[call]
		if !ok {
			t.Errorf("heal entry never called %s", call)
			continue
		}
		if got > healEntryTimeout || got < healEntryTimeout-time.Minute {
			t.Errorf("%s ran with %v left, want about the %v entry deadline", call, got, healEntryTimeout)
		}
	}
}

// ambiguousDelete executes deletes of the target key for real but reports
// failure — the "delete executed, response lost" class (simtest's
// FaultAmbiguous on blob.Delete).
type ambiguousDelete struct {
	blob.BlobStore
	key string
}

func (b *ambiguousDelete) Delete(ctx context.Context, key string) error {
	err := b.BlobStore.Delete(ctx, key)
	if key == b.key {
		if err != nil {
			return err
		}
		return errors.New("simulated: delete executed, response lost")
	}
	return err
}

// An ambiguous delete outcome must not end the handshake early: safeDelete
// restores the quarantined bytes unconditionally, so a racing registration
// never finds its key missing (Codex round 7).
func TestSweep_AmbiguousDeleteRestoresFromQuarantine(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "ambig")
	f.upload(key, "ambig-bytes")
	f.backdate(key, 25*time.Hour)

	s := NewSweeper(&ambiguousDelete{BlobStore: f.blobs, key: key}, f.store,
		SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	report, err := s.Sweep(context.Background())
	if err == nil {
		t.Fatal("Sweep must surface the ambiguous delete error")
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want no reap counted on ambiguous delete", report)
	}
	if !f.exists(key) {
		t.Fatalf("key %q not restored after ambiguous delete", key)
	}
	dest := filepath.Join(t.TempDir(), "restored.pack")
	if err := f.blobs.DownloadFile(context.Background(), key, dest); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dest); string(got) != "ambig-bytes" {
		t.Fatalf("restored content = %q, want original bytes", got)
	}
}

// failRecheck serves the classification and revalidation HasPack reads,
// then fails the post-delete recheck (call 3) with a transient error.
type failRecheck struct {
	stubLedger
	calls int
}

func (p *failRecheck) HasPack(context.Context, string, string) (bool, error) {
	p.calls++
	if p.calls == 3 {
		return false, errors.New("simulated transient registry failure")
	}
	return false, nil
}

// A failed post-delete recheck must restore the key from quarantine before
// surfacing the error — a registration that landed just before the delete
// must never be exposed as missing (Codex round 8).
func TestSweep_FailedRecheckRestoresFromQuarantine(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "recheck")
	f.upload(key, "recheck-bytes")
	f.backdate(key, 25*time.Hour)

	s := NewSweeper(f.blobs, &failRecheck{}, SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	report, err := s.Sweep(context.Background())
	if err == nil {
		t.Fatal("Sweep must surface the recheck error")
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want no reap counted on failed recheck", report)
	}
	if !f.exists(key) {
		t.Fatalf("key %q not restored after failed post-delete recheck", key)
	}
}

// The local-mode ticker is the real wiring: an aged orphan disappears
// without any explicit Sweep call once the ticker is started.
func TestStartSweepTicker_ReapsOrphan(t *testing.T) {
	f := newSweepFixture(t)
	liveKey := blob.PackKey("r1", "live")
	f.upload(liveKey, "live")
	f.register("r1", "live", liveKey)
	orphan := blob.PackKey("r1", "orphan")
	f.upload(orphan, "orphan")
	f.backdate(orphan, 25*time.Hour)

	stop := StartSweepTicker(&SweepSchedule{
		Sweeper:  f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}),
		Interval: 10 * time.Millisecond,
	})
	defer stop()
	waitFor(t, "ticker sweep to reap the orphan", func() bool { return !f.exists(orphan) })
	if !f.exists(liveKey) {
		t.Fatalf("ticker sweep deleted the live pack %q", liveKey)
	}
}

// River wiring: the periodic job (RunOnStart) executes the sweep through
// the durable queue against a real Postgres pack registry.
func TestRiverSweepPeriodic(t *testing.T) {
	if testing.Short() {
		t.Skip("river periodic sweep needs docker")
	}
	ctx := context.Background()
	pgC, err := tcpostgres.Run(ctx, testimages.Postgres,
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	defer func() { _ = pgC.Terminate(ctx) }()
	pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	store, err := ref.Open(ctx, pgURL)
	if err != nil {
		t.Fatalf("open pg ref store: %v", err)
	}
	defer store.Close()
	if err := store.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}

	blobDir := t.TempDir()
	blobs, err := blob.OpenFile("file://" + blobDir)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer blobs.Close()

	seed := func(key, content string) {
		src := filepath.Join(t.TempDir(), strings.ReplaceAll(key, "/", "_"))
		if err := os.WriteFile(src, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := blobs.UploadFile(ctx, key, src); err != nil {
			t.Fatalf("UploadFile(%q): %v", key, err)
		}
	}
	liveKey := blob.PackKey("r1", "live")
	seed(liveKey, "live")
	if err := store.RegisterPacks(ctx, "r1", []ref.Pack{{ID: "live", Key: liveKey}}); err != nil {
		t.Fatalf("RegisterPacks: %v", err)
	}
	orphan := blob.PackKey("r1", "orphan")
	seed(orphan, "orphan")
	past := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(filepath.Join(blobDir, "packs", "r1", "pack-orphan.pack"), past, past); err != nil {
		t.Fatal(err)
	}

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	sweeper := NewSweeper(blobs, store, SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	q, err := NewRiver(ctx, pgURL, deliverer, func(context.Context, sync.Job) error { return nil }, func(context.Context, sync.Job) error { return nil }, sync.NewMemStore(),
		&SweepSchedule{Sweeper: sweeper, Interval: time.Minute}, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	exists := func(key string) bool {
		_, err := blobs.Stat(ctx, key)
		if err != nil && !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("Stat(%q): %v", key, err)
		}
		return err == nil
	}
	waitFor(t, "periodic river sweep to reap the orphan", func() bool { return !exists(orphan) })
	if !exists(liveKey) {
		t.Fatalf("periodic sweep deleted the live pack %q", liveKey)
	}
}

// The packs/ scan is streamed: with a page size smaller than any repo
// group, contiguous keys still classify per repo exactly like the
// materialized scan did (memory-bound fix from the security review).
func TestSweep_StreamsAcrossPages(t *testing.T) {
	f := newSweepFixture(t)
	orig := sweepListPageSize
	sweepListPageSize = 2
	t.Cleanup(func() { sweepListPageSize = orig })

	// Two repos, three packs each (so groups straddle page boundaries):
	// one live, one aged orphan, one fresh per repo.
	for _, repo := range []string{"ra", "rb"} {
		live := blob.PackKey(repo, "alive")
		f.upload(live, "live")
		f.register(repo, "alive", live)
		orphan := blob.PackKey(repo, "orphan")
		f.upload(orphan, "orphan")
		f.backdate(orphan, 25*time.Hour)
		f.upload(blob.PackKey(repo, "zfresh"), "fresh")
	}

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Live != 2 || report.Fresh != 2 ||
		len(report.Orphans) != 2 || report.Deleted != 2 {
		t.Fatalf("report = %+v, want 2 live / 2 fresh / 2 orphans reaped", report)
	}
	for _, repo := range []string{"ra", "rb"} {
		if f.exists(blob.PackKey(repo, "orphan")) {
			t.Fatalf("orphan in %s survived", repo)
		}
		if !f.exists(blob.PackKey(repo, "alive")) || !f.exists(blob.PackKey(repo, "zfresh")) {
			t.Fatalf("non-orphan in %s deleted", repo)
		}
	}
}

// TestSweepNeverTouchesWALPrefix (issue #133 Phase 3): a zero-infra bucket
// mixes the s3wal authority objects (_wal/…) with packs/, lfs/, and
// sweep/quarantine/. A destructive pass over such a bucket must reap its
// orphans while never listing, classifying, quarantining, or deleting a
// _wal/ key — the sweeper's prefixes cannot even see them, and the
// assertNotWAL guards crash the pass if a listing or key-parsing bug ever
// routes one into a disposal path.
func TestSweepNeverTouchesWALPrefix(t *testing.T) {
	f := newSweepFixture(t)

	walKeys := []string{
		"_wal/probe",
		"_wal/instance-lease",
		"_wal/meta.json",
		"_wal/repo/r1",
		"_wal/scan/lfs-reap/global",
	}
	for _, k := range walKeys {
		f.upload(k, "authority state at "+k)
		f.backdate(k, 48*time.Hour) // ancient: age alone must never matter
	}
	orphanPack := blob.PackKey("r1", "orphan")
	f.upload(orphanPack, "orphan-pack-bytes")
	f.backdate(orphanPack, 25*time.Hour)
	orphanLFS := blob.LFSKey("r1", lfsOID("ab"))
	f.upload(orphanLFS, "orphan-lfs-bytes")
	f.backdate(orphanLFS, 25*time.Hour)

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true, DeleteLFS: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// Vacuity guard: the pass really ran destructively.
	if report.Deleted != 2 || f.exists(orphanPack) || f.exists(orphanLFS) {
		t.Fatalf("report = %+v (pack exists=%v, lfs exists=%v), want both orphans reaped", report, f.exists(orphanPack), f.exists(orphanLFS))
	}
	for _, k := range walKeys {
		if !f.exists(k) {
			t.Fatalf("sweep deleted reserved authority key %q", k)
		}
	}
	// The heal pass over the quarantine backups must spare them too.
	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Heal(context.Background()); err != nil {
		t.Fatalf("Heal: %v", err)
	}
	for _, k := range walKeys {
		if !f.exists(k) {
			t.Fatalf("quarantine heal deleted reserved authority key %q", k)
		}
	}
}
