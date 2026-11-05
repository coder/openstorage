// Repository-purge tests (issue #52): the end-to-end tombstone-then-delete
// flow, retention filtering, refusal guards, crash-injection resume,
// late-writer deferral, sweep-schedule integration, and quarantine aging
// under retention mode. Zero-container: sqlite stores + fileblob.
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

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// purgeFixture extends the sweep fixture with a sqlite meta store and an
// injectable meta clock (so tests can soft-delete "in the past").
type purgeFixture struct {
	*sweepFixture
	meta    meta.Backend
	org     string
	metaNow func() time.Time // consulted by the meta store's writes
}

func newPurgeFixture(t *testing.T) *purgeFixture {
	t.Helper()
	ctx := context.Background()
	f := &purgeFixture{sweepFixture: newSweepFixture(t), org: "purge-org", metaNow: time.Now}
	ms, err := meta.Open(ctx, "sqlite://"+filepath.Join(t.TempDir(), "meta.db"),
		meta.WithNowFunc(func() time.Time { return f.metaNow() }))
	if err != nil {
		t.Fatalf("open meta store: %v", err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatalf("meta InitSchema: %v", err)
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, f.org, []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	f.meta = ms
	return f
}

// seedRepo creates a repo with refs, a base pack, a namespaced pack, and an
// LFS object — rows and blobs both — and returns it.
func (f *purgeFixture) seedRepo(url string) meta.Repo {
	f.t.Helper()
	ctx := context.Background()
	repo, err := f.meta.CreateRepo(ctx, f.org, url, "main")
	if err != nil {
		f.t.Fatalf("CreateRepo(%s): %v", url, err)
	}
	base := repo.RepoID
	if won, err := f.store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, "1111111111111111111111111111111111111111"); err != nil || !won {
		f.t.Fatalf("CASRef: won=%v err=%v", won, err)
	}
	packKey := blob.PackKey(base, "p1")
	nsKey := blob.PackKey(base+"+ephemeral", "pn")
	lfsKey := blob.LFSKey(base, strings.Repeat("a", 64))
	f.upload(packKey, "pack bytes of "+base)
	f.upload(nsKey, "namespace bytes of "+base)
	f.upload(lfsKey, "lfs bytes of "+base)
	f.register(base, "p1", packKey)
	f.register(base+"+ephemeral", "pn", nsKey)
	if err := f.store.RegisterLFSObject(ctx, base, lfsKey, int64(len("lfs bytes of "+base))); err != nil {
		f.t.Fatalf("RegisterLFSObject: %v", err)
	}
	return repo
}

func (f *purgeFixture) softDelete(url string) {
	f.t.Helper()
	if _, err := f.meta.SoftDeleteRepo(context.Background(), f.org, url); err != nil {
		f.t.Fatalf("SoftDeleteRepo(%s): %v", url, err)
	}
}

// purger builds a RepoPurger over the fixture's stores. The purger clock is
// shifted two hours ahead so freshly soft-deleted repos clear
// MinRepoPurgeAge without real waiting.
func (f *purgeFixture) purger(sweeper *Sweeper, retention time.Duration) *RepoPurger {
	f.t.Helper()
	p := NewRepoPurger(sweeper, f.store.(RepoPurgeRegistry), f.meta.(RepoPurgeMeta), retention, slog.New(slog.DiscardHandler))
	p.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	return p
}

// purgeAwareSweeper builds a sweeper wired like serve: purge-aware via the
// store's fence probe, every delete flag off.
func (f *purgeFixture) purgeAwareSweeper() *Sweeper {
	f.t.Helper()
	return f.sweeper(SweepConfig{Threshold: 24 * time.Hour, PurgedRepoProbe: f.store.(RepoPurgeRegistry).IsRepoPurged})
}

// readBlob returns the content at key.
func (f *purgeFixture) readBlob(key string) string {
	f.t.Helper()
	dst := filepath.Join(f.t.TempDir(), "dl")
	if err := f.blobs.DownloadFile(context.Background(), key, dst); err != nil {
		f.t.Fatalf("DownloadFile(%s): %v", key, err)
	}
	data, err := os.ReadFile(dst)
	if err != nil {
		f.t.Fatal(err)
	}
	return string(data)
}

// repoKeys returns the seeded blob keys of repo.
func repoKeys(base string) (packKey, nsKey, lfsKey string) {
	return blob.PackKey(base, "p1"), blob.PackKey(base+"+ephemeral", "pn"), blob.LFSKey(base, strings.Repeat("a", 64))
}

// assertIntact fails unless the repo's rows, blobs, and meta row all exist.
func (f *purgeFixture) assertIntact(base string) {
	f.t.Helper()
	ctx := context.Background()
	packKey, nsKey, lfsKey := repoKeys(base)
	for _, key := range []string{packKey, nsKey, lfsKey} {
		if !f.exists(key) {
			f.t.Fatalf("blob %s missing", key)
		}
	}
	if n, err := f.store.(RepoPurgeRegistry).CountRepoRows(ctx, blob.QuotaRepoGroup(base)); err != nil || n != 4 {
		f.t.Fatalf("CountRepoRows(%s) = %d err=%v (want 4: ref + 2 packs + lfs)", base, n, err)
	}
	if state, found, err := f.meta.(RepoPurgeMeta).RepoPurgeState(ctx, base); err != nil || !found || state.PurgedAt != nil {
		f.t.Fatalf("meta row of %s missing or stamped: found=%v state=%+v err=%v", base, found, state, err)
	}
}

// assertPurged fails unless every trace of the repo is gone (except
// quarantine backups) and the accounting is zeroed.
func (f *purgeFixture) assertPurged(base string) {
	f.t.Helper()
	ctx := context.Background()
	packKey, nsKey, lfsKey := repoKeys(base)
	for _, key := range []string{packKey, nsKey, lfsKey} {
		if f.exists(key) {
			f.t.Fatalf("blob %s survived the purge", key)
		}
	}
	group := blob.QuotaRepoGroup(base)
	if n, err := f.store.(RepoPurgeRegistry).CountRepoRows(ctx, group); err != nil || n != 0 {
		f.t.Fatalf("CountRepoRows(%s) = %d err=%v (want 0)", base, n, err)
	}
	// The repos row is KEPT as the permanent name tombstone, stamped
	// purged_at (the completion commit).
	state, found, err := f.meta.(RepoPurgeMeta).RepoPurgeState(ctx, base)
	if err != nil || !found || state.PurgedAt == nil {
		f.t.Fatalf("purge tombstone of %s wrong: found=%v state=%+v err=%v", base, found, state, err)
	}
	if state, err := f.store.QuotaLedgerState(ctx, group, time.Hour); err != nil ||
		state.UsageBytes != 0 || state.ReservedBytes != 0 || state.RetainedBytes != 0 {
		f.t.Fatalf("ledger state of %s after purge: %+v err=%v (want zeros)", base, state, err)
	}
	if usage, err := blob.StorageUsageBytes(ctx, f.blobs, base); err != nil || usage != 0 {
		f.t.Fatalf("StorageUsageBytes(%s) = %d err=%v (want 0)", base, usage, err)
	}
}

func TestRepoPurge_EndToEnd(t *testing.T) {
	f := newPurgeFixture(t)
	victim := f.seedRepo("https://example.com/victim.git")
	control := f.seedRepo("https://example.com/control.git")
	f.softDelete(victim.URL)

	sweeper := f.purgeAwareSweeper()
	packKey, nsKey, lfsKey := repoKeys(victim.RepoID)
	contents := map[string]string{
		packKey: f.readBlob(packKey), nsKey: f.readBlob(nsKey), lfsKey: f.readBlob(lfsKey),
	}

	report, err := f.purger(sweeper, 0).PurgeRepo(context.Background(), victim.RepoID)
	if err != nil {
		t.Fatalf("PurgeRepo: %v", err)
	}
	if report.RefsDropped != 1 || report.PacksRetired != 2 || report.LFSDropped != 1 || report.BlobsReaped != 3 {
		t.Fatalf("report = %+v (want 1 ref, 2 packs, 1 lfs, 3 blobs)", report)
	}
	f.assertPurged(victim.RepoID)
	f.assertIntact(control.RepoID)

	// Quarantine discipline: every reaped blob left an identical-bytes
	// backup — nothing was hard-deleted.
	for key, want := range contents {
		backups := f.backupKeys(key)
		if len(backups) != 1 {
			t.Fatalf("blob %s has %d quarantine backups (want 1)", key, len(backups))
		}
		if got := f.readBlob(backups[0]); got != want {
			t.Fatalf("backup of %s holds %q, want %q", key, got, want)
		}
	}

	// The burned name is NEVER released (URL-scoped tokens may outlive
	// any retention — Codex security round on #105).
	if _, err := f.meta.CreateRepo(context.Background(), f.org, victim.URL, "main"); !errors.Is(err, meta.ErrAlreadyExists) {
		t.Fatalf("purge released the burned name: CreateRepo = %v, want ErrAlreadyExists", err)
	}
	// Idempotence: re-purging a completed repo is a straggler rescan —
	// success with nothing left to reap.
	rep2, err := f.purger(sweeper, 0).PurgeRepo(context.Background(), victim.RepoID)
	if err != nil || rep2.BlobsReaped != 0 {
		t.Fatalf("re-purge = %+v err=%v, want clean no-op rescan", rep2, err)
	}
}

func TestPurgedRepoStragglers(t *testing.T) {
	// An in-flight pre-delete writer PUBLISHES its blob after the purge's
	// prefix scan; its fenced registration then fails, stranding an
	// unregistered blob under the purged repo's prefix. Both recovery
	// paths must reap it: the sweeper's PurgedRepoProbe classification on
	// the normal cadence, and a purge-repo re-run's rescan.
	f := newPurgeFixture(t)
	ctx := context.Background()
	repo := f.seedRepo("https://example.com/straggler-blob.git")
	f.softDelete(repo.URL)
	probe := f.store.(RepoPurgeRegistry).IsRepoPurged
	sweeper := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, PurgedRepoProbe: probe})
	purger := f.purger(sweeper, 0)
	if _, err := purger.PurgeRepo(ctx, repo.RepoID); err != nil {
		t.Fatalf("PurgeRepo: %v", err)
	}

	// The late publication lands after completion (uploads are not
	// fenced), in both classes; age it past the threshold.
	stragglerPack := blob.PackKey(repo.RepoID, "late")
	stragglerLFS := blob.LFSKey(repo.RepoID, strings.Repeat("c", 64))
	f.upload(stragglerPack, "late pack bytes")
	f.upload(stragglerLFS, "late lfs bytes")
	f.backdate(stragglerPack, 25*time.Hour)
	f.backdate(stragglerLFS, 25*time.Hour)

	// Path 1: the ordinary sweep, retention mode alone (every delete flag
	// off), reaps both classes via the fence probe.
	report, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 2 {
		t.Fatalf("sweep reaped %d stragglers, want 2 (report %+v)", report.Deleted, report)
	}
	if f.exists(stragglerPack) || f.exists(stragglerLFS) {
		t.Fatal("straggler blobs survived the retention-mode sweep")
	}
	if len(f.backupKeys(stragglerPack)) != 1 || len(f.backupKeys(stragglerLFS)) != 1 {
		t.Fatal("straggler reaps skipped quarantine")
	}
	// The straggler reaps recreated NO ledger rows (PR #105 round 14:
	// GuardedLFSDelete skips its counter-row upsert for fenced repos).
	if n, err := f.store.(RepoPurgeRegistry).PurgeRepoLedger(ctx, blob.QuotaRepoGroup(repo.RepoID)); err != nil || n != 0 {
		t.Fatalf("straggler reaps left %d ledger rows (err=%v), want 0", n, err)
	}

	// A live repo's aged unregistered blob stays untouched by the probe
	// path (it is not fenced): retention mode alone must not reap it.
	control := f.seedRepo("https://example.com/straggler-control.git")
	controlOrphan := blob.PackKey(control.RepoID, "orphan")
	f.upload(controlOrphan, "orphan bytes")
	f.backdate(controlOrphan, 25*time.Hour)
	if _, err := sweeper.Sweep(ctx); err != nil {
		t.Fatalf("control sweep: %v", err)
	}
	if !f.exists(controlOrphan) {
		t.Fatal("retention-mode sweep reaped a live repo's orphan")
	}

	// Path 2: a purge-repo re-run rescans and reaps a fresh straggler
	// immediately (no age threshold — rows are provably gone). A late
	// adoption claim — the last row-creating ledger path — is rejected by
	// the fence outright (round 13), so nothing is left for the rescan's
	// defense-in-depth ledger cleanup to remove.
	straggler2 := blob.PackKey(repo.RepoID, "late2")
	f.upload(straggler2, "second straggler")
	if claimed, _, err := f.store.TryAdoptionClaim(ctx, repo.RepoID, blob.LFSKey(repo.RepoID, strings.Repeat("d", 64))); err != nil || claimed {
		t.Fatalf("TryAdoptionClaim on purged repo = (%v, %v), want fenced no-claim", claimed, err)
	}
	rep, err := purger.PurgeRepo(ctx, repo.RepoID)
	if err != nil {
		t.Fatalf("rescan PurgeRepo: %v", err)
	}
	if rep.BlobsReaped != 1 || f.exists(straggler2) {
		t.Fatalf("rescan reaped %d (blob present=%v), want the straggler gone", rep.BlobsReaped, f.exists(straggler2))
	}
	if rep.RowsPurged != 0 {
		t.Fatalf("rescan purged %d ledger rows, want 0 (the fence rejected the late claim)", rep.RowsPurged)
	}
}

func TestPurgeExpired_RotatesPastFailingRepos(t *testing.T) {
	// A persistently failing repo stays purgeable forever; the rotating
	// cursor must let later deletions purge anyway instead of the failing
	// batch monopolizing every pass (PR #105 round 9).
	oldLimit := purgeBatchLimit
	purgeBatchLimit = 1
	t.Cleanup(func() { purgeBatchLimit = oldLimit })

	f := newPurgeFixture(t)
	ctx := context.Background()
	// bad deletes first (oldest), so every pass's first batch would be
	// [bad] without rotation.
	bad := f.seedRepo("https://example.com/bad.git")
	f.metaNow = func() time.Time { return time.Now().Add(-4 * time.Hour) }
	f.softDelete(bad.URL)
	good := f.seedRepo("https://example.com/good.git")
	f.metaNow = func() time.Time { return time.Now().Add(-3 * time.Hour) }
	f.softDelete(good.URL)
	f.metaNow = time.Now

	sweeper := f.purgeAwareSweeper()
	hooks := &purgeRegistryHooks{RepoPurgeRegistry: f.store.(RepoPurgeRegistry)}
	purger := NewRepoPurger(sweeper, hooks, f.meta.(RepoPurgeMeta), time.Hour, slog.New(slog.DiscardHandler))

	// Pass 1 attempts bad (oldest) and fails; the cursor advances anyway.
	hooks.retireFailRepo = bad.RepoID
	if report, err := purger.PurgeExpired(ctx); err == nil || report.Failed != 1 || report.Purged != 0 {
		t.Fatalf("pass 1 = %+v err=%v, want the failing repo attempted", report, err)
	}
	// Pass 2 resumes after bad and purges good despite bad still failing.
	report, err := purger.PurgeExpired(ctx)
	if err != nil || report.Purged != 1 {
		t.Fatalf("pass 2 = %+v err=%v, want the later repo purged", report, err)
	}
	f.assertPurged(good.RepoID)
	// Pass 3 wraps to the oldest pending repo: bad gets its retry (and
	// succeeds now that the fault is gone).
	hooks.retireFailRepo = ""
	if report, err := purger.PurgeExpired(ctx); err != nil || report.Purged != 1 {
		t.Fatalf("pass 3 = %+v err=%v, want the wrapped retry to purge", report, err)
	}
	f.assertPurged(bad.RepoID)
}

func TestPurgeExpired_RespectsRetention(t *testing.T) {
	f := newPurgeFixture(t)
	// expired was soft-deleted three hours "ago" (injected meta clock);
	// recent was soft-deleted just now — inside the 1h retention.
	expired := f.seedRepo("https://example.com/expired.git")
	recent := f.seedRepo("https://example.com/recent.git")
	f.metaNow = func() time.Time { return time.Now().Add(-3 * time.Hour) }
	f.softDelete(expired.URL)
	f.metaNow = time.Now
	f.softDelete(recent.URL)

	sweeper := f.purgeAwareSweeper()
	purger := f.purger(sweeper, time.Hour)
	purger.now = time.Now // real clock: retention filtering is under test

	report, err := purger.PurgeExpired(context.Background())
	if err != nil {
		t.Fatalf("PurgeExpired: %v", err)
	}
	if report.Eligible != 1 || report.Purged != 1 || report.Failed != 0 {
		t.Fatalf("report = %+v (want exactly the expired repo purged)", report)
	}
	f.assertPurged(expired.RepoID)
	// Soft-deleted within retention: completely untouched.
	f.assertIntact(recent.RepoID)
	if len(f.backupKeys(blob.PackKey(recent.RepoID, "p1"))) != 0 {
		t.Fatal("within-retention repo's blob was quarantined")
	}
}

func TestPurgeRepo_Refusals(t *testing.T) {
	f := newPurgeFixture(t)
	live := f.seedRepo("https://example.com/live.git")
	recent := f.seedRepo("https://example.com/recent.git")
	f.softDelete(recent.URL)

	sweeper := f.purgeAwareSweeper()
	purger := f.purger(sweeper, 0)
	ctx := context.Background()

	if _, err := purger.PurgeRepo(ctx, live.RepoID); !errors.Is(err, meta.ErrRepoNotDeleted) {
		t.Fatalf("purge live = %v, want ErrRepoNotDeleted", err)
	}
	realNow := NewRepoPurger(sweeper, f.store.(RepoPurgeRegistry), f.meta.(RepoPurgeMeta), 0, slog.New(slog.DiscardHandler))
	if _, err := realNow.PurgeRepo(ctx, recent.RepoID); !errors.Is(err, ErrRepoRecentlyDeleted) {
		t.Fatalf("purge recent = %v, want ErrRepoRecentlyDeleted", err)
	}
	if _, err := purger.PurgeRepo(ctx, "repo_nonexistent0"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("purge absent = %v, want ErrNotFound", err)
	}
	for _, bad := range []string{"", live.RepoID + "+ephemeral", "repo/../escape"} {
		if _, err := purger.PurgeRepo(ctx, bad); err == nil || !strings.Contains(err.Error(), "invalid repo ID") {
			t.Fatalf("purge %q = %v, want invalid-ID error", bad, err)
		}
	}
	// Nothing was touched by any refusal.
	f.assertIntact(live.RepoID)
	f.assertIntact(recent.RepoID)

	// A failing delete guard blocks the purge before any destructive step.
	guarded := f.sweeper(SweepConfig{Threshold: 24 * time.Hour,
		PurgedRepoProbe: f.store.(RepoPurgeRegistry).IsRepoPurged,
		DeleteGuard:     func(context.Context) error { return errors.New("versioning drifted") }})
	if _, err := f.purger(guarded, 0).PurgeRepo(ctx, recent.RepoID); err == nil || !strings.Contains(err.Error(), "delete guard") {
		t.Fatalf("guarded purge = %v, want delete-guard error", err)
	}
	f.assertIntact(recent.RepoID)
}

// purgeRegistryHooks wraps the real purge registry with one-shot fault and
// late-writer hooks.
type purgeRegistryHooks struct {
	RepoPurgeRegistry
	retireErr      error  // one-shot RetireRepoPacks failure
	retireFailRepo string // persistent RetireRepoPacks failure for this base repo
	ledgerErr      error  // one-shot PurgeRepoLedger failure
	afterRetire    func() // one-shot post-retire hook (late writer)
}

func (h *purgeRegistryHooks) RetireRepoPacks(ctx context.Context, group []string) (int64, error) {
	if h.retireFailRepo != "" && group[0] == h.retireFailRepo {
		return 0, errors.New("persistent failure")
	}
	if err := h.retireErr; err != nil {
		h.retireErr = nil
		return 0, err
	}
	n, err := h.RepoPurgeRegistry.RetireRepoPacks(ctx, group)
	if hook := h.afterRetire; hook != nil && err == nil {
		h.afterRetire = nil
		hook()
	}
	return n, err
}

func (h *purgeRegistryHooks) PurgeRepoLedger(ctx context.Context, group []string) (int64, error) {
	if err := h.ledgerErr; err != nil {
		h.ledgerErr = nil
		return 0, err
	}
	return h.RepoPurgeRegistry.PurgeRepoLedger(ctx, group)
}

// deleteFailBlobs fails the first Delete of failKey, then delegates.
type deleteFailBlobs struct {
	blob.BlobStore
	failKey string
}

func (b *deleteFailBlobs) Delete(ctx context.Context, key string) error {
	if key == b.failKey {
		b.failKey = ""
		return errors.New("injected delete failure")
	}
	return b.BlobStore.Delete(ctx, key)
}

func TestPurgeRepo_CrashResume(t *testing.T) {
	ctx := context.Background()

	t.Run("before_tombstoning_completes", func(t *testing.T) {
		// Fault at RetireRepoPacks: refs are gone, but NO blob may have
		// been touched — rows always tombstone before blob deletion.
		f := newPurgeFixture(t)
		repo := f.seedRepo("https://example.com/crash-a.git")
		f.softDelete(repo.URL)
		sweeper := f.purgeAwareSweeper()
		hooks := &purgeRegistryHooks{RepoPurgeRegistry: f.store.(RepoPurgeRegistry), retireErr: errors.New("injected crash")}
		purger := NewRepoPurger(sweeper, hooks, f.meta.(RepoPurgeMeta), 0, slog.New(slog.DiscardHandler))
		purger.now = func() time.Time { return time.Now().Add(2 * time.Hour) }

		if _, err := purger.PurgeRepo(ctx, repo.RepoID); err == nil || !strings.Contains(err.Error(), "injected crash") {
			t.Fatalf("PurgeRepo = %v, want injected crash", err)
		}
		packKey, nsKey, lfsKey := repoKeys(repo.RepoID)
		for _, key := range []string{packKey, nsKey, lfsKey} {
			if !f.exists(key) {
				t.Fatalf("blob %s deleted before its rows were tombstoned", key)
			}
			if len(f.backupKeys(key)) != 0 {
				t.Fatalf("blob %s quarantined before tombstoning finished", key)
			}
		}
		if has, err := f.store.HasPack(ctx, repo.RepoID, packKey); err != nil || !has {
			t.Fatalf("pack row lost to the failed retire: has=%v err=%v", has, err)
		}
		// The soft-deleted meta row survived: the purge is resumable.
		if _, found, err := f.meta.(RepoPurgeMeta).RepoPurgeState(ctx, repo.RepoID); err != nil || !found {
			t.Fatalf("meta tombstone lost: found=%v err=%v", found, err)
		}
		if _, err := purger.PurgeRepo(ctx, repo.RepoID); err != nil {
			t.Fatalf("resumed purge: %v", err)
		}
		f.assertPurged(repo.RepoID)
	})

	t.Run("mid_blob_reap", func(t *testing.T) {
		f := newPurgeFixture(t)
		repo := f.seedRepo("https://example.com/crash-b.git")
		f.softDelete(repo.URL)
		packKey, _, _ := repoKeys(repo.RepoID)
		blobs := &deleteFailBlobs{BlobStore: f.blobs, failKey: packKey}
		sweeper := NewSweeper(blobs, f.store, SweepConfig{Threshold: 24 * time.Hour, PurgedRepoProbe: f.store.(RepoPurgeRegistry).IsRepoPurged}, slog.New(slog.DiscardHandler))
		hooks := f.store.(RepoPurgeRegistry)
		purger := NewRepoPurger(sweeper, hooks, f.meta.(RepoPurgeMeta), 0, slog.New(slog.DiscardHandler))
		purger.now = func() time.Time { return time.Now().Add(2 * time.Hour) }

		if _, err := purger.PurgeRepo(ctx, repo.RepoID); err == nil {
			t.Fatal("PurgeRepo succeeded despite injected delete failure")
		}
		// The ambiguous delete was healed from quarantine: bytes present,
		// rows tombstoned, meta row retained for resume.
		if !f.exists(packKey) {
			t.Fatalf("blob %s lost to ambiguous delete", packKey)
		}
		if _, found, err := f.meta.(RepoPurgeMeta).RepoPurgeState(ctx, repo.RepoID); err != nil || !found {
			t.Fatalf("meta tombstone lost: found=%v err=%v", found, err)
		}
		if _, err := purger.PurgeRepo(ctx, repo.RepoID); err != nil {
			t.Fatalf("resumed purge: %v", err)
		}
		f.assertPurged(repo.RepoID)
	})

	t.Run("before_completion_commit", func(t *testing.T) {
		f := newPurgeFixture(t)
		repo := f.seedRepo("https://example.com/crash-c.git")
		f.softDelete(repo.URL)
		sweeper := f.purgeAwareSweeper()
		hooks := &purgeRegistryHooks{RepoPurgeRegistry: f.store.(RepoPurgeRegistry), ledgerErr: errors.New("injected crash")}
		purger := NewRepoPurger(sweeper, hooks, f.meta.(RepoPurgeMeta), 0, slog.New(slog.DiscardHandler))
		purger.now = func() time.Time { return time.Now().Add(2 * time.Hour) }

		if _, err := purger.PurgeRepo(ctx, repo.RepoID); err == nil || !strings.Contains(err.Error(), "injected crash") {
			t.Fatalf("PurgeRepo = %v, want injected crash", err)
		}
		// Blobs reaped, but the completion commit did not run: the meta
		// row must survive so the next pass finishes the job.
		if _, found, err := f.meta.(RepoPurgeMeta).RepoPurgeState(ctx, repo.RepoID); err != nil || !found {
			t.Fatalf("meta tombstone lost: found=%v err=%v", found, err)
		}
		if _, err := purger.PurgeRepo(ctx, repo.RepoID); err != nil {
			t.Fatalf("resumed purge: %v", err)
		}
		f.assertPurged(repo.RepoID)
	})
}

func TestPurgeRepo_FenceRejectsLateWriter(t *testing.T) {
	f := newPurgeFixture(t)
	ctx := context.Background()
	repo := f.seedRepo("https://example.com/late.git")
	f.softDelete(repo.URL)
	packKey, _, lfsKey := repoKeys(repo.RepoID)

	sweeper := f.purgeAwareSweeper()
	hooks := &purgeRegistryHooks{RepoPurgeRegistry: f.store.(RepoPurgeRegistry)}
	// An in-flight pre-delete push tries to re-register its pack right
	// after the tombstoning phase (content-addressed retry: same id, same
	// key). The purge fence written in phase 0 must reject it — this is
	// what closes the verify-then-complete window (PR #105 security
	// round): no registration can settle between the zero-row
	// verification and the completion commit.
	var lateErr error
	hooks.afterRetire = func() {
		lateErr = f.store.RegisterPacks(ctx, repo.RepoID, []ref.Pack{{ID: "p1", Key: packKey}})
	}
	purger := NewRepoPurger(sweeper, hooks, f.meta.(RepoPurgeMeta), 0, slog.New(slog.DiscardHandler))
	purger.now = func() time.Time { return time.Now().Add(2 * time.Hour) }

	// The purge completes in ONE pass: the late writer never lands a row.
	if _, err := purger.PurgeRepo(ctx, repo.RepoID); err != nil {
		t.Fatalf("PurgeRepo = %v, want success (fenced late writer)", err)
	}
	if !errors.Is(lateErr, ref.ErrRepoPurged) {
		t.Fatalf("late RegisterPacks = %v, want ErrRepoPurged", lateErr)
	}
	f.assertPurged(repo.RepoID)
	// The fence outlives the purge: post-completion registrations of any
	// class are rejected too, so nothing can ever strand rows behind the
	// stamped tombstone.
	if err := f.store.RegisterPacks(ctx, repo.RepoID, []ref.Pack{{ID: "p2", Key: packKey}}); !errors.Is(err, ref.ErrRepoPurged) {
		t.Fatalf("post-purge RegisterPacks = %v, want ErrRepoPurged", err)
	}
	if err := f.store.RegisterLFSObject(ctx, repo.RepoID, lfsKey, 1); !errors.Is(err, ref.ErrRepoPurged) {
		t.Fatalf("post-purge RegisterLFSObject = %v, want ErrRepoPurged", err)
	}
	// Ref-only writers (REST branch/tag creation reaches CASRef with no
	// pack registration, PR #105 round 4) are fenced too.
	if _, err := f.store.CASRef(ctx, repo.RepoID, "refs/heads/late", ref.ZeroSHA, "2222222222222222222222222222222222222222"); !errors.Is(err, ref.ErrRepoPurged) {
		t.Fatalf("post-purge CASRef = %v, want ErrRepoPurged", err)
	}
}

// stragglerRowRegistry simulates a row that appears after tombstoning
// through a path the fence cannot see (defense-in-depth: no such path
// exists today — pack and LFS registrations are fenced — but the
// verification must stay authoritative regardless).
type stragglerRowRegistry struct {
	RepoPurgeRegistry
	stragglers int64 // one-shot CountRepoRows override
}

func (s *stragglerRowRegistry) CountRepoRows(ctx context.Context, group []string) (int64, error) {
	if n := s.stragglers; n > 0 {
		s.stragglers = 0
		return n, nil
	}
	return s.RepoPurgeRegistry.CountRepoRows(ctx, group)
}

func TestPurgeRepo_StragglerRowsDefer(t *testing.T) {
	f := newPurgeFixture(t)
	ctx := context.Background()
	repo := f.seedRepo("https://example.com/straggler.git")
	f.softDelete(repo.URL)

	sweeper := f.purgeAwareSweeper()
	hooks := &stragglerRowRegistry{RepoPurgeRegistry: f.store.(RepoPurgeRegistry), stragglers: 1}
	purger := NewRepoPurger(sweeper, hooks, f.meta.(RepoPurgeMeta), 0, slog.New(slog.DiscardHandler))
	purger.now = func() time.Time { return time.Now().Add(2 * time.Hour) }

	if _, err := purger.PurgeRepo(ctx, repo.RepoID); !errors.Is(err, ErrPurgeDeferred) {
		t.Fatalf("PurgeRepo = %v, want ErrPurgeDeferred", err)
	}
	// Deferred: no completion stamp, so the next pass retries and wins.
	if state, found, err := f.meta.(RepoPurgeMeta).RepoPurgeState(ctx, repo.RepoID); err != nil || !found || state.PurgedAt != nil {
		t.Fatalf("deferred purge stamped the tombstone: found=%v state=%+v err=%v", found, state, err)
	}
	if _, err := purger.PurgeRepo(ctx, repo.RepoID); err != nil {
		t.Fatalf("retried purge: %v", err)
	}
	f.assertPurged(repo.RepoID)
}

func TestSweepSchedule_RunsPurge(t *testing.T) {
	f := newPurgeFixture(t)
	repo := f.seedRepo("https://example.com/scheduled.git")
	f.metaNow = func() time.Time { return time.Now().Add(-3 * time.Hour) }
	f.softDelete(repo.URL)
	f.metaNow = time.Now

	sweeper := f.purgeAwareSweeper()
	purger := NewRepoPurger(sweeper, f.store.(RepoPurgeRegistry), f.meta.(RepoPurgeMeta), time.Hour, slog.New(slog.DiscardHandler))
	schedule := &SweepSchedule{Sweeper: sweeper, Interval: time.Hour, Purger: purger}
	if err := schedule.runOnce(context.Background(), 0, 0); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	f.assertPurged(repo.RepoID)
}

// listUntilDone is a blob store whose listings never finish: they return when
// their context ends, like a sweep that needs more than its budget.
type listUntilDone struct {
	blob.BlobStore
}

func (listUntilDone) ListPages(ctx context.Context, _ string, _ int, _ func([]blob.ListedObject) error) error {
	<-ctx.Done()
	return ctx.Err()
}

// A sweep that uses its whole budget must still leave the purge its own: the
// purge ran on the sweep's expired context before, so soft-deleted
// repositories were never reclaimed on a fleet whose sweep outgrew its pass.
func TestSweepSchedule_PurgeKeepsItsOwnBudgetWhenTheSweepUsesItsWhole(t *testing.T) {
	f := newPurgeFixture(t)
	repo := f.seedRepo("https://example.com/starved.git")
	f.metaNow = func() time.Time { return time.Now().Add(-3 * time.Hour) }
	f.softDelete(repo.URL)
	f.metaNow = time.Now

	stuck := NewSweeper(listUntilDone{f.blobs}, f.store,
		SweepConfig{Threshold: 24 * time.Hour, PurgedRepoProbe: f.store.(RepoPurgeRegistry).IsRepoPurged}, slog.New(slog.DiscardHandler))
	purger := NewRepoPurger(f.purgeAwareSweeper(), f.store.(RepoPurgeRegistry), f.meta.(RepoPurgeMeta), time.Hour, slog.New(slog.DiscardHandler))
	schedule := &SweepSchedule{Sweeper: stuck, Interval: time.Hour, Purger: purger}

	err := schedule.runOnce(context.Background(), 50*time.Millisecond, time.Minute)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runOnce = %v, want the sweep's deadline error", err)
	}
	f.assertPurged(repo.RepoID)
}

func TestSweep_ListOnlyRollbackPreservesOrphanBackups(t *testing.T) {
	// The operator's rollback move — every delete flag off — must keep
	// preserving ORDINARY orphan backups even though sweeps are now
	// always purge-aware (PR #105 round 12): only purge-FENCED repos'
	// backups may age out in that state.
	f := newPurgeFixture(t)
	ctx := context.Background()
	live := f.seedRepo("https://example.com/rollback.git")
	orphanKey := blob.PackKey(live.RepoID, "orphan")
	f.upload(orphanKey, "orphan bytes")
	f.backdate(orphanKey, 25*time.Hour)

	// A delete-enabled sweep quarantines the aged orphan.
	deleter := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true,
		PurgedRepoProbe: f.store.(RepoPurgeRegistry).IsRepoPurged})
	if _, err := deleter.Sweep(ctx); err != nil {
		t.Fatalf("delete-enabled sweep: %v", err)
	}
	backups := f.backupKeys(orphanKey)
	if len(backups) != 1 {
		t.Fatalf("want 1 quarantined orphan backup, got %d", len(backups))
	}
	f.backdate(backups[0], 25*time.Hour)

	// Rollback: all delete flags off. The aged backup of a NON-fenced
	// repo must survive purge-aware sweeps — it may be the last copy.
	if _, err := f.purgeAwareSweeper().Sweep(ctx); err != nil {
		t.Fatalf("list-only sweep: %v", err)
	}
	if len(f.backupKeys(orphanKey)) != 1 {
		t.Fatal("list-only rollback destroyed an ordinary orphan's backup")
	}
}

func TestSweep_RetentionModeAgesQuarantine(t *testing.T) {
	f := newPurgeFixture(t)
	repo := f.seedRepo("https://example.com/aged.git")
	f.softDelete(repo.URL)
	sweeper := f.purgeAwareSweeper()
	if _, err := f.purger(sweeper, 0).PurgeRepo(context.Background(), repo.RepoID); err != nil {
		t.Fatalf("PurgeRepo: %v", err)
	}
	packKey, _, _ := repoKeys(repo.RepoID)
	backups := f.backupKeys(packKey)
	if len(backups) != 1 {
		t.Fatalf("want 1 backup, got %d", len(backups))
	}
	f.backdate(backups[0], 25*time.Hour)

	// With every delete flag off, retention mode alone must age the
	// purge's quarantine backups out — reclamation must eventually FREE
	// bytes, not just move them.
	report, err := sweeper.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.BytesReclaimed == 0 {
		t.Fatalf("no quarantine bytes reclaimed: %+v", report)
	}
	if len(f.backupKeys(packKey)) != 0 {
		t.Fatal("aged quarantine backup survived a retention-mode sweep")
	}
}
