// LFS-prefix sweep tests (issue #51): list-only classification, the
// separate DeleteLFS opt-in (general Delete must NOT reap LFS keys — the
// pre-registry rollout hazard), quarantine backups, heal restoration, and
// foreign-key immunity. Runs over the same sweepFixture as the pack tests.
package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

// lfsOID mints a deterministic 64-hex oid from a two-hex-char seed.
func lfsOID(seed string) string {
	if len(seed) != 2 {
		panic("lfsOID seed must be 2 hex chars")
	}
	return strings.Repeat(seed, 32)
}

// registerLFS records an LFS object in the fixture's ref store.
func (f *sweepFixture) registerLFS(repo, key string, bytes int64) {
	f.t.Helper()
	if err := f.store.RegisterLFSObject(context.Background(), repo, key, bytes); err != nil {
		f.t.Fatalf("RegisterLFSObject: %v", err)
	}
}

// seedLFSWorld builds the LFS classification matrix: a registered (live)
// object, an aged orphan, a fresh orphan, and foreign keys under lfs/.
func seedLFSWorld(f *sweepFixture) (liveKey, orphanKey, freshKey string) {
	liveKey = blob.LFSKey("repo_lfs", lfsOID("aa"))
	f.upload(liveKey, "live-lfs-bytes")
	f.registerLFS("repo_lfs", liveKey, int64(len("live-lfs-bytes")))
	f.backdate(liveKey, 25*time.Hour) // old AND registered: still live

	orphanKey = blob.LFSKey("repo_lfs", lfsOID("bb"))
	f.upload(orphanKey, "orphan-lfs-bytes")
	f.backdate(orphanKey, 25*time.Hour)

	freshKey = blob.LFSKey("repo_lfs", lfsOID("cc"))
	f.upload(freshKey, "fresh-lfs-bytes")

	// Foreign keys under lfs/ that do not match the exact LFSKey shape
	// (bad oid, deeper nesting): never classified, never deleted.
	for _, foreign := range []string{"lfs/stray", "lfs/repo_lfs/not-an-oid", "lfs/repo_lfs/" + lfsOID("dd") + "/nested"} {
		f.upload(foreign, "not-an-lfs-key")
		f.backdate(foreign, 30*24*time.Hour)
	}
	return
}

func TestSweepLFS_ListOnlyReportsWithoutDeleting(t *testing.T) {
	f := newSweepFixture(t)
	liveKey, orphanKey, freshKey := seedLFSWorld(f)

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Live != 1 || report.Fresh != 1 || report.OrphanCount != 1 || report.Deleted != 0 {
		t.Fatalf("report = %+v, want live=1 fresh=1 orphans=1 deleted=0", report)
	}
	if len(report.Orphans) != 1 || report.Orphans[0].Key != orphanKey {
		t.Fatalf("orphans = %+v, want %q", report.Orphans, orphanKey)
	}
	for _, key := range []string{liveKey, orphanKey, freshKey,
		"lfs/stray", "lfs/repo_lfs/not-an-oid", "lfs/repo_lfs/" + lfsOID("dd") + "/nested"} {
		if !f.exists(key) {
			t.Fatalf("list-only sweep deleted %q", key)
		}
	}
}

// TestSweepLFS_LegacySpellingSweepsAsLFS (ADR-41, Codex security round
// 78): the v0.1.x single-key spelling must classify as LFS — never foreign
// — and sweep exactly like a canonical orphan: adoption RETAINS its legacy
// source (live presigned GETs and mid-roll old replicas still read it —
// round 80), leaving this sweep as the designed reclamation path, and a
// registered CANONICAL row must not make the legacy spelling live.
func TestSweepLFS_LegacySpellingSweepsAsLFS(t *testing.T) {
	f := newSweepFixture(t)
	canonical := blob.LFSKey("repo_lfs", lfsOID("aa"))
	f.upload(canonical, "adopted-bytes")
	f.registerLFS("repo_lfs", canonical, int64(len("adopted-bytes")))
	f.backdate(canonical, 25*time.Hour)
	legacy := blob.LFSLegacyKey("repo_lfs", lfsOID("aa"))
	f.upload(legacy, "adopted-bytes")
	f.backdate(legacy, 25*time.Hour)

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Live != 1 || report.OrphanCount != 1 {
		t.Fatalf("report = %+v, want live=1 (canonical) orphans=1 (legacy source)", report)
	}
	if report.Orphans[0].Key != legacy {
		t.Fatalf("orphan = %q, want the legacy source %q", report.Orphans[0].Key, legacy)
	}
}

// TestSweepHeal_ReconcilesTwinRows (Codex round 85): with periodic
// sweeping disabled (CODESTORAGE_SWEEP_INTERVAL=0), startup runs Heal —
// which never invoked the twin-row janitor, so a mixed-roll duplicate
// (legacy row stranded beside the canonical one) double-charged usage and
// the object cap indefinitely. Heal must reconcile too.
func TestSweepHeal_ReconcilesTwinRows(t *testing.T) {
	f := newSweepFixture(t)
	oid := lfsOID("ab")
	canonical := blob.LFSKey("repo_lfs", oid)
	legacy := blob.LFSLegacyKey("repo_lfs", oid)
	f.registerLFS("repo_lfs", canonical, 40)
	f.registerLFS("repo_lfs", legacy, 40)

	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Heal(context.Background()); err != nil {
		t.Fatalf("Heal: %v", err)
	}
	if has, err := f.store.HasLFSObject(context.Background(), "repo_lfs", legacy); err != nil || has {
		t.Fatalf("legacy twin after Heal = (%v, %v), want reconciled away", has, err)
	}
	if has, err := f.store.HasLFSObject(context.Background(), "repo_lfs", canonical); err != nil || !has {
		t.Fatalf("canonical row after Heal = (%v, %v), want intact", has, err)
	}
}

// TestSweepLFS_LegacyReapNeedsExplicitOptIn (Codex round 81): during a
// rolling upgrade, old replicas derive ONLY the legacy spelling — but
// adopting any legacy object older than the sweep threshold immediately
// makes its retained source an aged rowless orphan, so DeleteLFS alone
// would reap it mid-roll and 404 those replicas' downloads. Legacy-spelled
// keys therefore need the SEPARATE post-roll opt-in
// (CODESTORAGE_SWEEP_DELETE_LFS_LEGACY): the transition gate dominates
// every other flag combination. Canonical orphans keep sweeping under
// DeleteLFS alone.
func TestSweepLFS_LegacyReapNeedsExplicitOptIn(t *testing.T) {
	seed := func(f *sweepFixture) (canonicalOrphan, legacyOrphan string) {
		canonicalOrphan = blob.LFSKey("repo_lfs", lfsOID("ee"))
		f.upload(canonicalOrphan, "aged canonical orphan")
		f.backdate(canonicalOrphan, 25*time.Hour)
		legacyOrphan = blob.LFSLegacyKey("repo_lfs", lfsOID("ff"))
		f.upload(legacyOrphan, "retained legacy source")
		f.backdate(legacyOrphan, 25*time.Hour)
		return
	}
	t.Run("delete-lfs-alone-spares-legacy", func(t *testing.T) {
		f := newSweepFixture(t)
		canonicalOrphan, legacyOrphan := seed(f)
		report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true, DeleteLFS: true}).Sweep(context.Background())
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if f.exists(legacyOrphan) == false {
			t.Fatal("DeleteLFS alone reaped a legacy-spelled key — old replicas' reads break mid-roll")
		}
		if f.exists(canonicalOrphan) {
			t.Fatal("DeleteLFS did not reap the canonical orphan")
		}
		if report.Deleted != 1 {
			t.Fatalf("report = %+v, want deleted=1 (canonical only)", report)
		}
	})
	t.Run("post-roll-opt-in-reaps-legacy", func(t *testing.T) {
		f := newSweepFixture(t)
		canonicalOrphan, legacyOrphan := seed(f)
		report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, DeleteLFSLegacy: true}).Sweep(context.Background())
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if f.exists(legacyOrphan) || f.exists(canonicalOrphan) {
			t.Fatalf("post-roll opt-in left orphans (legacy exists=%v canonical exists=%v)", f.exists(legacyOrphan), f.exists(canonicalOrphan))
		}
		if report.Deleted != 2 {
			t.Fatalf("report = %+v, want deleted=2", report)
		}
	})
	t.Run("legacy-opt-in-requires-lfs-mode", func(t *testing.T) {
		f := newSweepFixture(t)
		_, legacyOrphan := seed(f)
		if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, DeleteLFSLegacy: true}).Sweep(context.Background()); err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if !f.exists(legacyOrphan) {
			t.Fatal("DeleteLFSLegacy without DeleteLFS reaped an LFS key — the class opt-in must still gate")
		}
	})
}

// TestSweepLFS_GeneralDeleteSparesLFS: the general Delete opt-in must NOT
// reap LFS orphans — pre-#51 LFS objects have no registry rows, so a fleet
// already running Delete=true would otherwise reap its entire existing LFS
// population on upgrade.
func TestSweepLFS_GeneralDeleteSparesLFS(t *testing.T) {
	f := newSweepFixture(t)
	_, orphanKey, _ := seedLFSWorld(f)

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, Delete: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want no deletes (DeleteLFS off)", report)
	}
	if !f.exists(orphanKey) {
		t.Fatalf("Delete=true without DeleteLFS reaped LFS orphan %q", orphanKey)
	}
}

func TestSweepLFS_DeleteLFSReapsOrphansOnly(t *testing.T) {
	f := newSweepFixture(t)
	liveKey, orphanKey, freshKey := seedLFSWorld(f)
	// A pack orphan alongside: DeleteLFS alone must not touch pack keys.
	packOrphan := blob.PackKey("r1", "orphan")
	f.upload(packOrphan, "pack-orphan-bytes")
	f.backdate(packOrphan, 25*time.Hour)

	report, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 1 {
		t.Fatalf("report = %+v, want deleted=1 (the aged LFS orphan)", report)
	}
	if f.exists(orphanKey) {
		t.Fatalf("LFS orphan %q survived a DeleteLFS sweep", orphanKey)
	}
	if !f.hasBackup(orphanKey) {
		t.Fatalf("reaped LFS orphan %q has no quarantine backup", orphanKey)
	}
	for _, key := range []string{liveKey, freshKey, packOrphan,
		"lfs/stray", "lfs/repo_lfs/not-an-oid", "lfs/repo_lfs/" + lfsOID("dd") + "/nested"} {
		if !f.exists(key) {
			t.Fatalf("DeleteLFS sweep deleted non-candidate %q", key)
		}
	}

	// Idempotence: a second sweep has nothing left to reap.
	report, err = f.sweeper(SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true}).Sweep(context.Background())
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if report.Deleted != 0 {
		t.Fatalf("second sweep report = %+v, want deleted=0", report)
	}
}

// TestSweepLFS_HealRestoresRegisteredFromQuarantine: a registered LFS
// object whose only copy sits in quarantine (delete-enabled crash
// mid-handshake) is restored by the heal pass in every sweep mode —
// PROVIDED the backup's bytes hash to the key's oid (restores are
// digest-verified since the round-19 security finding).
func TestSweepLFS_HealRestoresRegisteredFromQuarantine(t *testing.T) {
	f := newSweepFixture(t)
	content := "stranded-lfs-bytes"
	key := blob.LFSKey("repo_lfs", lfsContentOID(content))
	f.registerLFS("repo_lfs", key, int64(len(content)))
	// Simulate the crash: the backup exists, the original does not.
	f.upload(quarantineKey(key), content)

	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, QuotaEnforced: true}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(key) {
		t.Fatalf("heal pass did not restore registered LFS object %q", key)
	}
	// The restore re-materialized bytes floors scanned during their
	// absence could not see: it must bump the shared floor generation so
	// every replica's cached floor from that window is invalidated
	// (Codex round 54, security).
	if gen, err := f.store.LFSAdoptionGen(context.Background(), "repo_lfs"); err != nil || gen == 0 {
		t.Fatalf("floor generation after quarantine restore = (%d, %v), want > 0", gen, err)
	}
	// The bump only stops FUTURE floor serves: an admission already
	// holding an absence-window floor must see the restored bytes in
	// active_reservations, so the heal takes a pre-publication cover
	// reservation (Codex round 58, security).
	ls, err := f.store.QuotaLedgerState(context.Background(), blob.QuotaRepoGroup("repo_lfs"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if ls.ReservedBytes != int64(len(content)) {
		t.Fatalf("ledger after quarantine restore = %+v, want the %d-byte cover reserved", ls, len(content))
	}
}

// A registered LFS object that is PRESENT but SHORTER than its registered
// size (issue #129: a still-valid v0.1 presigned PUT, storage-layer damage)
// must be healed from an aged quarantine backup whose bytes hash to the
// key's oid — mere existence of the truncated original must not let the
// age purge collect the object's only complete copy.
func TestSweepLFS_HealRestoresTruncatedRegisteredObject(t *testing.T) {
	f := newSweepFixture(t)
	content := "complete-lfs-bytes"
	key := blob.LFSKey("repo_lfs", lfsContentOID(content))
	f.upload(key, content[:4])
	f.registerLFS("repo_lfs", key, int64(len(content)))
	qKey := quarantineKey(key)
	f.upload(qKey, content)
	f.backdate(qKey, 25*time.Hour)

	cfg := SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, QuotaEnforced: true}
	if _, err := f.sweeper(cfg).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got := f.content(key); got != content {
		t.Fatalf("truncated registered object %q = %q after sweep, want the backup's complete bytes restored", key, got)
	}
	// Healed => the original is complete again; the aged backup purges.
	if f.exists(qKey) {
		t.Fatalf("aged backup %q survived after the truncated original was healed", qKey)
	}
	// The overwrite re-materializes bytes exactly like the into-absence
	// restore: floors scanned before the heal must be invalidated and the
	// returning delta covered (Codex rounds 54/58 discipline).
	if gen, err := f.store.LFSAdoptionGen(context.Background(), "repo_lfs"); err != nil || gen == 0 {
		t.Fatalf("floor generation after truncated heal = (%d, %v), want > 0", gen, err)
	}
	ls, err := f.store.QuotaLedgerState(context.Background(), blob.QuotaRepoGroup("repo_lfs"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if ls.ReservedBytes != int64(len(content)) {
		t.Fatalf("ledger after truncated heal = %+v, want the %d-byte cover reserved", ls, len(content))
	}
}

// The truncated-original overwrite is a canonical LFS materialization: it
// must run under the per-key fleet adoption claim (ADR-41/#104 exclusion
// with the adopter's corrupt-canonical check→delete). While another
// worker holds the claim, the heal must neither rewrite the original nor
// let the purge collect the backup (the next sweep re-examines).
func TestSweepLFS_HealTruncatedSkipsWhileClaimHeld(t *testing.T) {
	f := newSweepFixture(t)
	content := "claim-held-lfs-bytes"
	key := blob.LFSKey("repo_lfs", lfsContentOID(content))
	f.upload(key, content[:4])
	f.registerLFS("repo_lfs", key, int64(len(content)))
	qKey := quarantineKey(key)
	f.upload(qKey, content)
	f.backdate(qKey, 25*time.Hour)

	claimed, _, err := f.store.TryAdoptionClaim(context.Background(), "repo_lfs", key)
	if err != nil || !claimed {
		t.Fatalf("TryAdoptionClaim = (%v, %v), want the test to hold the claim", claimed, err)
	}
	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got := f.content(key); got != content[:4] {
		t.Fatalf("original %q = %q, want untouched while the claim is held", key, got)
	}
	if !f.exists(qKey) {
		t.Fatalf("backup %q was purged while the truncated original is unhealed", qKey)
	}
}

// lostLeaseLedger reports the adoption-claim lease LOST on the first
// renewal attempt after the restore's copy begins (blocking renewals until
// then), simulating an expiry takeover racing the overwrite.
type lostLeaseLedger struct {
	PackRegistry
	copyStarted chan struct{}
}

func (l *lostLeaseLedger) RenewAdoptionClaim(context.Context, string, string) (bool, error) {
	<-l.copyStarted
	return false, nil
}

// copyStartSignal delegates to the real store but signals when the
// restore's Copy begins and detaches it from cancellation, so a lease
// loss recorded mid-copy cannot abort the copy itself — forcing the
// "successful copy under a lost lease" interleaving.
type copyStartSignal struct {
	blob.BlobStore
	started chan struct{}
	once    sync.Once
}

func (b *copyStartSignal) Copy(ctx context.Context, dst, src string) error {
	b.once.Do(func() { close(b.started) })
	time.Sleep(50 * time.Millisecond) // let the renewer record the loss
	return b.BlobStore.Copy(context.WithoutCancel(ctx), dst, src)
}

// A lease lost while the overwrite copy is in flight must override the
// successful copy (Codex P1 on this PR, the adoption pipeline's
// discipline): the successor's claim-fenced window may delete the key
// after the unfenced bytes land, so the heal is unproven and the aged
// purge must spare the backup for the next sweep to re-examine.
func TestSweepLFS_HealTruncatedLostLeaseSparesBackup(t *testing.T) {
	f := newSweepFixture(t)
	content := "lost-lease-lfs-bytes"
	key := blob.LFSKey("repo_lfs", lfsContentOID(content))
	f.upload(key, content[:4])
	f.registerLFS("repo_lfs", key, int64(len(content)))
	qKey := quarantineKey(key)
	f.upload(qKey, content)
	f.backdate(qKey, 25*time.Hour)

	oldInterval := sweepClaimRenewInterval
	sweepClaimRenewInterval = time.Millisecond
	t.Cleanup(func() { sweepClaimRenewInterval = oldInterval })

	started := make(chan struct{})
	packs := &lostLeaseLedger{PackRegistry: f.store, copyStarted: started}
	blobs := &copyStartSignal{BlobStore: f.blobs, started: started}
	s := NewSweeper(blobs, packs, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true}, slog.New(slog.DiscardHandler))
	if _, err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(qKey) {
		t.Fatalf("backup %q was purged although the lease was lost mid-copy — the heal is unproven", qKey)
	}
}

// A truncated registered original whose backup fails the key's own digest
// gets no restore (pre-registry legacy bytes must never launder into a
// registered row) and no spare: bytes that cannot be the registered
// content follow the same aged-purge policy as the into-absence mismatch.
func TestSweepLFS_HealTruncatedPurgesMismatchedBackup(t *testing.T) {
	f := newSweepFixture(t)
	content := "mismatch-lfs-bytes"
	key := blob.LFSKey("repo_lfs", lfsContentOID(content))
	f.upload(key, content[:4])
	f.registerLFS("repo_lfs", key, int64(len(content)))
	qKey := quarantineKey(key)
	f.upload(qKey, "bytes that never hashed to the oid")
	f.backdate(qKey, 25*time.Hour)

	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got := f.content(key); got != content[:4] {
		t.Fatalf("original %q = %q, want untouched (mismatched backup must not restore)", key, got)
	}
	if f.exists(qKey) {
		t.Fatalf("aged digest-mismatched backup %q was spared — it cannot be the registered content", qKey)
	}
}

// TestSweepLFS_ReconcileTickerReconcilesLateTwins (Codex round 88 +
// security twin): with CODESTORAGE_SWEEP_INTERVAL=0 the startup Heal was
// the ONLY reconciliation pass, but a mixed-roll old replica can insert a
// legacy row beside an existing canonical one at any time after startup —
// registered fast paths never re-register, so the duplicate count and
// usage charge persisted indefinitely (507 cap denials until restart).
// Sweep-disabled deployments need an always-live janitor.
func TestSweepLFS_ReconcileTickerReconcilesLateTwins(t *testing.T) {
	f := newSweepFixture(t)
	oid := lfsOID("cd")
	canonical := blob.LFSKey("repo_lfs", oid)
	legacy := blob.LFSLegacyKey("repo_lfs", oid)
	f.registerLFS("repo_lfs", canonical, 40)

	stop := StartReconcileTicker(f.sweeper(SweepConfig{Threshold: 24 * time.Hour}), 10*time.Millisecond, false)
	defer stop()
	// The twin lands AFTER the ticker started — an old replica writing
	// mid-roll, past the startup Heal.
	f.registerLFS("repo_lfs", legacy, 40)

	deadline := time.Now().Add(10 * time.Second)
	for {
		has, err := f.store.HasLFSObject(context.Background(), "repo_lfs", legacy)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("legacy twin inserted after startup was never reconciled — sweep-disabled deployments double-charge it forever")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if has, err := f.store.HasLFSObject(context.Background(), "repo_lfs", canonical); err != nil || !has {
		t.Fatalf("canonical row after ticker reconcile = (%v, %v), want intact", has, err)
	}
}

// TestSweepLFS_LegacyReapSparesFreshAdoptions (Codex round 88): a batch
// can presign a 15-minute GET against a rowless legacy key right up to
// the moment a concurrent write adopts it — and the retained source's own
// ModTime is its ORIGINAL upload time, already past any age threshold, so
// the next post-roll sweep reaped it immediately and broke the fresh URL.
// The canonical blob's ModTime IS the adoption time: a legacy source with
// a REGISTERED canonical twin may be reaped only once the canonical copy
// is older than the reap grace (which dwarfs the URL lifetime).
func TestSweepLFS_LegacyReapSparesFreshAdoptions(t *testing.T) {
	f := newSweepFixture(t)
	content := "just-adopted legacy source"
	oid := lfsContentOID(content)
	canonical := blob.LFSKey("repo_lfs", oid)
	legacy := blob.LFSLegacyKey("repo_lfs", oid)
	// Seconds after adoption: an AGED legacy source retained beside a
	// fresh canonical copy under a registered row.
	f.upload(legacy, content)
	f.backdate(legacy, 48*time.Hour)
	f.upload(canonical, content)
	f.registerLFS("repo_lfs", canonical, int64(len(content)))

	cfg := SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, DeleteLFSLegacy: true}
	if _, err := f.sweeper(cfg).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(legacy) {
		t.Fatal("aged legacy source reaped moments after adoption — presigned GETs issued against the rowless key just before adoption break mid-lifetime")
	}
	// Once the REGISTRATION itself has aged past the grace, the source
	// reaps (the clock is the row's created_at, so advance the sweeper's
	// now past it — blob mtimes are already aged).
	aged := f.sweeper(cfg)
	aged.now = func() time.Time { return time.Now().Add(25 * time.Hour) }
	report, err := aged.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep after grace: %v", err)
	}
	if f.exists(legacy) {
		t.Fatalf("aged-adoption legacy source survived the post-grace sweep (report %+v)", report)
	}
}

// TestSweepLFS_LegacyReapGraceKeysOnRegistrationTime (Codex round 89): the
// grace keyed on the canonical BLOB's ModTime — but a rowless canonical
// copy can be hours old (publication or adoption copied it long ago and
// only the registration failed) while downloads keep presigning the
// still-rowless LEGACY key right up to the successful registration RETRY,
// which verifies the old copy in place without refreshing its ModTime. The
// grace must key on the row's registration time, not the blob timestamp.
func TestSweepLFS_LegacyReapGraceKeysOnRegistrationTime(t *testing.T) {
	f := newSweepFixture(t)
	content := "old copy, fresh registration"
	oid := lfsContentOID(content)
	canonical := blob.LFSKey("repo_lfs", oid)
	legacy := blob.LFSLegacyKey("repo_lfs", oid)
	// The rowless-canonical retry state: BOTH blobs are old (the copy
	// landed hours ago; its registration failed), presigned GETs against
	// the rowless legacy key were issued moments ago, and the retry
	// registers NOW without touching the canonical bytes.
	f.upload(legacy, content)
	f.backdate(legacy, 48*time.Hour)
	f.upload(canonical, content)
	f.backdate(canonical, 48*time.Hour)
	f.registerLFS("repo_lfs", canonical, int64(len(content)))

	cfg := SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, DeleteLFSLegacy: true}
	if _, err := f.sweeper(cfg).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(legacy) {
		t.Fatal("legacy source reaped moments after a late registration — the old canonical ModTime says nothing about presigned GETs issued up to the registration retry")
	}
}

// TestSweepLFS_LegacyReapSparesLastCopyOfBloblessRow (Codex round 89
// follow-up): keying the grace on registration time dropped round 88's
// registered-but-blobless spare. A registered row whose canonical blob
// was LOST serves reads through the legacy download fallback, and its
// retained legacy source may be the object's only remaining bytes — an
// aged registration alone must not reap it, or a repairable degradation
// becomes data loss (nothing server-side can ever converge the row again).
func TestSweepLFS_LegacyReapSparesLastCopyOfBloblessRow(t *testing.T) {
	f := newSweepFixture(t)
	content := "blobless row's last copy"
	oid := lfsContentOID(content)
	canonical := blob.LFSKey("repo_lfs", oid)
	legacy := blob.LFSLegacyKey("repo_lfs", oid)
	// An old registration (past any grace) whose canonical blob is gone:
	// only the retained legacy source still holds the bytes.
	f.upload(legacy, content)
	f.backdate(legacy, 48*time.Hour)
	f.registerLFS("repo_lfs", canonical, int64(len(content)))

	cfg := SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, DeleteLFSLegacy: true}
	aged := f.sweeper(cfg)
	aged.now = func() time.Time { return time.Now().Add(25 * time.Hour) }
	if _, err := aged.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(legacy) {
		t.Fatal("reaped the ONLY copy of a registered-but-blobless object — fallback reads lose the bytes and the row can never converge")
	}
}

// TestSweepLFS_LegacyReapSparesLastCompleteCopyOfTruncatedCanonical
// (issue #127, Codex round 14 on PR #123): round 88's
// registered-but-blobless spare checked mere EXISTENCE of the canonical
// copy — a TRUNCATED canonical blob (corrupted by a still-valid v0.1
// presigned PUT, or storage-layer damage) passed the gate and the
// post-grace sweep reaped the complete legacy twin that was the object's
// only full copy, turning a repairable degradation (download-time restore
// adopts the full twin, PR #104) into permanent data loss. The gate must
// compare the canonical copy's size against the REGISTERED size and spare
// the source while the canonical copy is deficient.
func TestSweepLFS_LegacyReapSparesLastCompleteCopyOfTruncatedCanonical(t *testing.T) {
	f := newSweepFixture(t)
	content := "the only complete copy of these bytes"
	oid := lfsContentOID(content)
	canonical := blob.LFSKey("repo_lfs", oid)
	legacy := blob.LFSLegacyKey("repo_lfs", oid)
	// An old registration (past any grace) whose canonical blob is SHORT
	// of the registered size: only the retained legacy source still
	// holds the full bytes.
	f.upload(legacy, content)
	f.backdate(legacy, 48*time.Hour)
	f.upload(canonical, content[:len(content)/2])
	f.backdate(canonical, 48*time.Hour)
	f.registerLFS("repo_lfs", canonical, int64(len(content)))

	cfg := SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, DeleteLFSLegacy: true}
	aged := f.sweeper(cfg)
	aged.now = func() time.Time { return time.Now().Add(25 * time.Hour) }
	if _, err := aged.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(legacy) {
		t.Fatal("reaped the last COMPLETE copy of a registered object whose canonical blob is truncated — the existence-only gate turns repairable corruption into data loss")
	}
	// Once the canonical copy is whole again the gate opens: the aged
	// source reaps as today.
	f.upload(canonical, content)
	f.backdate(canonical, 48*time.Hour)
	healed := f.sweeper(cfg)
	healed.now = func() time.Time { return time.Now().Add(25 * time.Hour) }
	report, err := healed.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep after canonical repair: %v", err)
	}
	if f.exists(legacy) {
		t.Fatalf("aged legacy source survived the sweep after its canonical copy became complete (report %+v)", report)
	}
}

// adoptOnQuarantineCopyBlob registers the canonical twin exactly as the
// reap's quarantine copy runs — an adoption completing between classify's
// grace check and the destructive handshake.
type adoptOnQuarantineCopyBlob struct {
	blob.BlobStore
	store     ref.Store
	repo      string
	canonical string
	legacy    string
	size      int64
	done      atomic.Bool
}

func (b *adoptOnQuarantineCopyBlob) Copy(ctx context.Context, dstKey, srcKey string) error {
	if srcKey == b.legacy && strings.HasPrefix(dstKey, quarantinePrefix) && !b.done.Swap(true) {
		if err := b.store.RegisterAdoptedLFSObject(ctx, b.repo, b.canonical, b.size); err != nil {
			return err
		}
	}
	return b.BlobStore.Copy(ctx, dstKey, srcKey)
}

// TestSweepLFS_LegacyReapRechecksTwinInsideGuard (Codex round 100):
// classify's reap-grace check races adoption — a rowless legacy source can
// be adopted (canonical registered, created_at fresh) between the check
// and safeDelete's destructive handshake, whose revalidation and guard
// consult only the LEGACY key. The just-retained source was then reaped
// inside the fresh presigned-GET window despite the grace. The guarded
// delete must recheck the canonical twin's grace: the guard's count-row
// lock blocks in-flight registrations from committing mid-handshake, so a
// committed fresh registration is always visible there.
func TestSweepLFS_LegacyReapRechecksTwinInsideGuard(t *testing.T) {
	f := newSweepFixture(t)
	content := "adopted mid-handshake"
	oid := lfsContentOID(content)
	canonical := blob.LFSKey("repo_lfs", oid)
	legacy := blob.LFSLegacyKey("repo_lfs", oid)
	f.upload(legacy, content)
	f.backdate(legacy, 48*time.Hour)
	f.upload(canonical, content)

	wrapped := &adoptOnQuarantineCopyBlob{
		BlobStore: f.blobs, store: f.store, repo: "repo_lfs",
		canonical: canonical, legacy: legacy, size: int64(len(content)),
	}
	sw := NewSweeper(wrapped, f.store, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, DeleteLFSLegacy: true}, slog.New(slog.DiscardHandler))
	if _, err := sw.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(legacy) {
		t.Fatal("retained source reaped despite an adoption registering mid-handshake — presigned GETs issued before the adoption break inside the grace window")
	}
}

// TestSweepLFS_PurgeVsLegacyGate pins the #105 × ADR-41 intersection: a
// WHOLE-REPO purge dominates the legacy transition gate (a deleted repo
// has no read-availability contract left — its legacy keys are permanent
// garbage like every other class), while for LIVE repos the transition
// gate keeps dominating even with the purge probe wired.
func TestSweepLFS_PurgeVsLegacyGate(t *testing.T) {
	t.Run("purged-repo-legacy-reaps-without-optins", func(t *testing.T) {
		f := newSweepFixture(t)
		legacy := blob.LFSLegacyKey("repo_purged", lfsOID("aa"))
		f.upload(legacy, "purged repo legacy source")
		f.backdate(legacy, 25*time.Hour)
		if err := f.store.(RepoPurgeRegistry).FenceRepoPurge(context.Background(), "repo_purged"); err != nil {
			t.Fatal(err)
		}
		cfg := SweepConfig{Threshold: 24 * time.Hour, PurgedRepoProbe: f.store.(RepoPurgeRegistry).IsRepoPurged}
		if _, err := f.sweeper(cfg).Sweep(context.Background()); err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if f.exists(legacy) {
			t.Fatal("purge-fenced repo's legacy key survived the purge-aware sweep — whole-repo reclamation includes legacy spellings, no opt-in needed")
		}
	})
	t.Run("live-repo-legacy-gate-holds-with-purge-wired", func(t *testing.T) {
		f := newSweepFixture(t)
		legacy := blob.LFSLegacyKey("repo_live", lfsOID("bb"))
		f.upload(legacy, "live repo legacy source")
		f.backdate(legacy, 25*time.Hour)
		cfg := SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true, PurgedRepoProbe: f.store.(RepoPurgeRegistry).IsRepoPurged}
		if _, err := f.sweeper(cfg).Sweep(context.Background()); err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if !f.exists(legacy) {
			t.Fatal("live repo's legacy key reaped without CODESTORAGE_SWEEP_DELETE_LFS_LEGACY — the transition gate must dominate even with the purge probe wired")
		}
	})
}

// TestSweepLFS_HealTakesNoCoverWhenQuotaDisabled (Codex round 73): with
// quota enforcement off, no admission consults floors or covers — a
// restore-cover row would only burn the per-repo reservation cap, and
// enough repairs in one TTL could wedge later restores outright. The heal
// must restore WITHOUT touching the reservation ledger.
func TestSweepLFS_HealTakesNoCoverWhenQuotaDisabled(t *testing.T) {
	f := newSweepFixture(t)
	content := "quota-off-stranded-bytes"
	key := blob.LFSKey("repo_qoff", lfsContentOID(content))
	f.registerLFS("repo_qoff", key, int64(len(content)))
	f.upload(quarantineKey(key), content)

	// QuotaEnforced deliberately unset: the production default when
	// CODESTORAGE_PACK_QUOTA_BYTES is absent.
	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !f.exists(key) {
		t.Fatalf("heal pass did not restore registered LFS object %q", key)
	}
	ls, err := f.store.QuotaLedgerState(context.Background(), blob.QuotaRepoGroup("repo_qoff"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if ls.ReservedBytes != 0 {
		t.Fatalf("quota-disabled heal reserved %d bytes — covers must be skipped when nothing consults them", ls.ReservedBytes)
	}
}

// TestSweepLFS_HealNeverRestoresMismatchedBackup (Codex round 19,
// security): a quarantine backup whose bytes do NOT hash to the key's oid
// descends from a pre-registry legacy blob — the heal pass must never
// restore it over a registered row, or fast paths that trust the registry
// would serve durable corruption. The row stays in the zero-growth repair
// state instead.
func TestSweepLFS_HealNeverRestoresMismatchedBackup(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.LFSKey("repo_lfs", lfsOID("ee")) // oid ≠ sha256 of the backup bytes
	f.registerLFS("repo_lfs", key, 10)
	f.upload(quarantineKey(key), "legacy garbage that never hashed to ee...")

	if _, err := f.sweeper(SweepConfig{Threshold: 24 * time.Hour}).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if f.exists(key) {
		t.Fatalf("heal pass restored a digest-mismatched LFS backup to %q", key)
	}
}

// TestSweepLFS_RegisteredMidSweepSurvives: an LFS object registered between
// classification and the delete is spared by revalidation, mirroring the
// pack race test.
func TestSweepLFS_RegisteredMidSweepSurvives(t *testing.T) {
	f := newSweepFixture(t)
	key := blob.LFSKey("repo_lfs", lfsOID("ff"))
	f.upload(key, "racer-lfs-bytes")
	f.backdate(key, 25*time.Hour)

	packs := &lfsRegisteredMidSweep{after: 1}
	s := NewSweeper(f.blobs, packs, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true}, slog.New(slog.DiscardHandler))
	report, err := s.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want no deletes (object registered mid-sweep)", report)
	}
	if !f.exists(key) {
		t.Fatalf("mid-sweep-registered LFS object %q was deleted", key)
	}
	if packs.calls != 2 {
		t.Fatalf("HasLFSObject calls = %d, want 2 (classification + revalidation)", packs.calls)
	}
}

// lfsRegisteredMidSweep mirrors registeredMidSweep for the LFS registry:
// HasLFSObject flips to registered strictly after the Nth call.
type lfsRegisteredMidSweep struct {
	stubLedger
	after, calls int
}

func (p *lfsRegisteredMidSweep) HasPack(context.Context, string, string) (bool, error) {
	return false, nil
}

func (p *lfsRegisteredMidSweep) HasLFSObject(context.Context, string, string) (bool, error) {
	p.calls++
	return p.calls > p.after, nil
}

// lfsContentOID returns content's sha256 — the oid a genuine LFS object's
// key embeds.
func lfsContentOID(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// lfsGuardWins simulates a verified upload whose registration commits
// between the sweep's quarantine copy and the guarded delete: the guard's
// in-transaction re-read sees the row, never runs del, and reports
// deleted=false.
type lfsGuardWins struct {
	stubLedger
	guardCalls int
}

func (p *lfsGuardWins) HasPack(context.Context, string, string) (bool, error) { return false, nil }

func (p *lfsGuardWins) GuardedLFSDelete(context.Context, string, string, func(context.Context) error) (bool, error) {
	p.guardCalls++
	return false, nil // registration won: del never executed
}

// TestSweepLFS_GuardedDeleteSparesRaceWinner (Codex round 20, P1): a
// verified upload whose register→verify pair completes between the
// quarantine copy and the delete must keep its bytes. An unguarded delete
// would remove the verified replacement with no writer check left behind
// it, and the digest check would rightly refuse to restore the corrupt
// backup — a registered row blobless forever. The sweep's LFS delete
// therefore runs inside GuardedLFSDelete: when the guard reports a
// registration won, the candidate is spared untouched.
func TestSweepLFS_GuardedDeleteSparesRaceWinner(t *testing.T) {
	f := newSweepFixture(t)
	content := "verified winner bytes"
	key := blob.LFSKey("repo_lfs", lfsContentOID(content))
	f.upload(key, content)
	f.backdate(key, 25*time.Hour)

	packs := &lfsGuardWins{}
	s := NewSweeper(f.blobs, packs, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true}, slog.New(slog.DiscardHandler))
	report, err := s.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if packs.guardCalls != 1 {
		t.Fatalf("GuardedLFSDelete calls = %d, want 1 (LFS deletes must be guarded)", packs.guardCalls)
	}
	if report.Deleted != 0 {
		t.Fatalf("report = %+v, want no deletes (guard spared the race winner)", report)
	}
	if !f.exists(key) {
		t.Fatalf("guard-spared LFS object %q was deleted", key)
	}
	if !f.hasBackup(key) {
		t.Fatalf("quarantine backup for %q was not retained", key)
	}
}

// TestSweepLFS_MidDeleteRestoreIsDigestVerified (Codex round 19, security):
// the mid-delete restore must not launder a corrupt pre-registry blob into
// a registered object. Attack ordering: an aged legacy blob whose bytes do
// NOT hash to its oid is quarantined and deleted; a verified upload
// registers the same key mid-delete; the recheck sees the row. Restoring
// the quarantined bytes now would overwrite the verified upload's content
// with garbage that the registry vouches for — the digest check must skip
// it. The genuine twin (backup bytes DO hash to the oid) must keep
// restoring exactly as before.
func TestSweepLFS_MidDeleteRestoreIsDigestVerified(t *testing.T) {
	genuine := "genuine lfs bytes"
	for name, tc := range map[string]struct {
		oid         string
		wantPresent bool
	}{
		"mismatched-not-restored": {oid: lfsOID("dd"), wantPresent: false},
		"genuine-restored":        {oid: lfsContentOID(genuine), wantPresent: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newSweepFixture(t)
			key := blob.LFSKey("repo_lfs", tc.oid)
			f.upload(key, genuine)
			f.backdate(key, 25*time.Hour)

			// Calls: 1 = classification, 2 = revalidateOrphan (both
			// unregistered), 3 = safeDelete's post-delete recheck
			// (registered — the racing verified upload landed).
			packs := &lfsRegisteredMidSweep{after: 2}
			s := NewSweeper(f.blobs, packs, SweepConfig{Threshold: 24 * time.Hour, DeleteLFS: true}, slog.New(slog.DiscardHandler))
			report, err := s.Sweep(context.Background())
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if report.Deleted != 0 {
				t.Fatalf("report = %+v, want no counted deletes (registered mid-delete)", report)
			}
			if got := f.exists(key); got != tc.wantPresent {
				t.Fatalf("exists(%q) = %v, want %v", key, got, tc.wantPresent)
			}
			// The backup is retained either way for the aged purge.
			if !f.hasBackup(key) {
				t.Fatalf("quarantine backup for %q was not retained", key)
			}
		})
	}
}
