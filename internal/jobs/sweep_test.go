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

	"codestorage/internal/blob"
	"codestorage/internal/events"
	"codestorage/internal/ref"
	"codestorage/internal/sync"
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

	// A foreign key under packs/ without a repo segment: never touched.
	f.upload("packs/stray", "not-a-pack-key")
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
	if report.ReposScanned != 2 { // r1 and r1+import
		t.Fatalf("ReposScanned = %d, want 2", report.ReposScanned)
	}

	// List-only NEVER deletes, orphans included.
	for _, key := range []string{liveKey, nsLiveKey, freshKey, oldOrphan, nsOrphan, "packs/stray"} {
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
	if report.Deleted != 2 || report.BytesReclaimed != report.OrphanBytes {
		t.Fatalf("report = %+v, want deleted=2 bytes_reclaimed=orphan_bytes", report)
	}
	for _, key := range []string{oldOrphan, nsOrphan} {
		if f.exists(key) {
			t.Fatalf("orphan %q survived a delete-enabled sweep", key)
		}
	}
	// Live packs, fresh uploads, and foreign keys all survive.
	for _, key := range []string{liveKey, nsLiveKey, freshKey, "packs/stray"} {
		if !f.exists(key) {
			t.Fatalf("delete-enabled sweep deleted non-orphan %q", key)
		}
	}

	// Idempotence: a second sweep finds nothing left to do.
	again, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if len(again.Orphans) != 0 || again.Deleted != 0 {
		t.Fatalf("second sweep report = %+v, want no orphans", again)
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

// registeredMidSweep returns the empty snapshot on the first ListPacks call
// and the registered set afterwards, simulating a pack whose RegisterPacks
// commits between the sweep's classification snapshot and its delete.
type registeredMidSweep struct {
	packs []ref.Pack
	calls int
}

func (p *registeredMidSweep) ListPacks(context.Context, string) ([]ref.Pack, error) {
	p.calls++
	if p.calls == 1 {
		return nil, nil
	}
	return p.packs, nil
}

// A pack registered mid-sweep must survive: the delete-time revalidation
// re-reads the registry instead of trusting the classification snapshot
// (the retried-push race from the Codex review).
func TestSweep_DeleteSkipsPackRegisteredMidSweep(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.PackKey("r1", "racer")
	f.upload(key, "racer-bytes")
	f.backdate(key, 25*time.Hour)

	packs := &registeredMidSweep{packs: []ref.Pack{{ID: "racer", Key: key}}}
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
		t.Fatalf("ListPacks calls = %d, want 2 (snapshot + revalidation)", packs.calls)
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

	stop := StartSweepTicker(f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}), 10*time.Millisecond)
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
	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
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
	q, err := NewRiver(ctx, pgURL, deliverer, func(context.Context, sync.Job) error { return nil },
		&SweepSchedule{Sweeper: sweeper, Interval: time.Minute}, slog.New(slog.DiscardHandler))
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
