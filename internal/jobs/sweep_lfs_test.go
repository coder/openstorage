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
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
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
// — and sweep exactly like a canonical orphan: adoption leaves its legacy
// source rowless (best-effort deleted, this is the failure path), and a
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
