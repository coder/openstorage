// §2.8 sync-claim write-through integration (meta ↔ ref over one shared
// MemCAS bucket): the meta claim lease and the ref manifest fence must
// move together — claim installs the token, finish clears it, a failed
// manifest write refuses the claim after rolling the meta side back, and
// a steal racing the fence install is caught by the post-install
// revalidation. Every failure direction is FAIL CLOSED: a fenced CASRef
// with a missing or foreign token loses cleanly.

package meta

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// ref.S3WAL is the production SyncClaimSink.
var _ SyncClaimSink = (*ref.S3WAL)(nil)

// newWriteThroughStores builds the zero-infra pairing: one shared MemCAS
// "bucket" holding both the meta document and the ref manifests, a meta
// store wired to the ref store as its claim sink, and the ref store
// itself for fenced CASRef checks.
func newWriteThroughStores(t *testing.T) (*s3wal.MemCAS, *S3WAL, *ref.S3WAL) {
	t.Helper()
	cas := s3wal.NewMemCAS()
	refStore := ref.NewS3WAL(cas, nil)
	metaStore := NewS3WAL(cas, WithSyncClaimSink(refStore))
	t.Cleanup(func() { metaStore.Close(); refStore.Close() })
	return cas, metaStore, refStore
}

// manifestSyncClaim reads the repo's manifest fence token ("" = none).
func manifestSyncClaim(t *testing.T, cas *s3wal.MemCAS, repoID string) string {
	t.Helper()
	obj, err := cas.Get(context.Background(), s3wal.ManifestKey(repoID))
	if err != nil {
		return "" // no manifest yet = no claim
	}
	m, err := s3wal.DecodeManifest(obj.Body)
	if err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if m.SyncClaim == nil {
		return ""
	}
	return m.SyncClaim.Holder
}

const testSHA1 = "1111111111111111111111111111111111111111"
const testSHA2 = "2222222222222222222222222222222222222222"

// TestS3WALMetaWriteThroughLifecycle: claim installs the manifest token
// (fenced CASRef wins), FinishSync clears it BEFORE finalizing (fenced
// CASRef fails closed afterwards), and a steal moves the token to the
// thief while the late finisher leaves it untouched.
func TestS3WALMetaWriteThroughLifecycle(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	// No claim yet: the fenced CAS fails closed.
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h1"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || won {
		t.Fatalf("fenced CASRef without claim = (%v, %v), want clean loss", won, err)
	}

	// Claim: the token lands in the manifest and fences the holder's writes.
	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h1", time.Minute); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "h1" {
		t.Fatalf("manifest token = %q, want h1", got)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h1"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || !won {
		t.Fatalf("fenced CASRef under claim = (%v, %v), want win", won, err)
	}
	// A foreign holder is fenced out while the claim is live.
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h-other"), repoID, "refs/heads/main", testSHA1, testSHA2); err != nil || won {
		t.Fatalf("foreign fenced CASRef = (%v, %v), want clean loss", won, err)
	}

	// Finish clears the token; further fenced writes fail closed.
	if _, err := metaStore.FinishSync(ctx, repoID, "h1", true); err != nil {
		t.Fatalf("FinishSync: %v", err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("manifest token after finish = %q, want cleared", got)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h1"), repoID, "refs/heads/main", testSHA1, testSHA2); err != nil || won {
		t.Fatalf("fenced CASRef after finish = (%v, %v), want clean loss", won, err)
	}

	// Steal: the thief's claim replaces the dead holder's token; the late
	// finisher gets ErrClaimLost and must NOT clear the thief's token.
	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h-dead", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("dead-holder claim = (%v, %v), want claimed", claimed, err)
	}
	if _, claimed, stolenFrom, err := metaStore.TryBeginSync(ctx, repoID, "h-thief", time.Minute); err != nil || !claimed || stolenFrom != "h-dead" {
		t.Fatalf("steal = (%v, %q, %v), want steal from h-dead", claimed, stolenFrom, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "h-thief" {
		t.Fatalf("manifest token after steal = %q, want h-thief", got)
	}
	if _, err := metaStore.FinishSync(ctx, repoID, "h-dead", true); err == nil {
		t.Fatal("late FinishSync succeeded, want ErrClaimLost")
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "h-thief" {
		t.Fatalf("manifest token after late finish = %q, want the thief's intact token", got)
	}
	// The dead holder's fenced writes fail closed under the thief's token.
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h-dead"), repoID, "refs/heads/main", testSHA1, testSHA2); err != nil || won {
		t.Fatalf("stolen holder's fenced CASRef = (%v, %v), want clean loss", won, err)
	}
	if _, err := metaStore.FinishSync(ctx, repoID, "h-thief", true); err != nil {
		t.Fatalf("thief FinishSync: %v", err)
	}
}

// TestS3WALMetaWriteThroughFenceFailure: when the manifest write fails,
// TryBeginSync must refuse the claim (claimed=false, surfaced error),
// roll the meta claim back — restoring the consumed sync_pending marker —
// and leave NO manifest token, so every fenced write stays fail-closed.
func TestS3WALMetaWriteThroughFenceFailure(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)
	if err := metaStore.RequestSync(ctx, repoID); err != nil {
		t.Fatal(err)
	}

	// Every manifest PUT fails definitively; the meta object is untouched.
	cas.Hooks.PutFault = func(op, key string) s3wal.PutFault {
		if strings.HasPrefix(key, s3wal.RepoPrefix) {
			return s3wal.FaultContention
		}
		return s3wal.FaultNone
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	_, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h1", time.Minute)
	if err == nil || claimed {
		t.Fatalf("claim with failing fence = (claimed=%v, err=%v), want refused with error", claimed, err)
	}
	// Meta rollback: idle row, lease cleared, pending marker restored.
	got, gerr := metaStore.GetRepoUpstream(ctx, repoID)
	if gerr != nil {
		t.Fatalf("GetRepoUpstream: %v", gerr)
	}
	if got.Syncing || got.SyncClaimHolder != "" || !got.SyncPending || got.RunCount != 0 {
		t.Fatalf("rolled-back row = %+v, want idle with restored SyncPending", got)
	}
	// No manifest token: the would-be holder's fenced writes fail closed.
	if tok := manifestSyncClaim(t, cas, repoID); tok != "" {
		t.Fatalf("manifest token after failed write-through = %q, want none", tok)
	}
	cas.Hooks = s3wal.MemCASHooks{}
	if won, cerr := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h1"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); cerr != nil || won {
		t.Fatalf("fenced CASRef after failed write-through = (%v, %v), want clean loss", won, cerr)
	}

	// The rolled-back row is claimable again once the fence heals.
	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h2", time.Minute); err != nil || !claimed {
		t.Fatalf("claim after heal = (%v, %v), want claimed", claimed, err)
	}
}

// TestS3WALMetaWriteThroughRevalidation reproduces the W10 late-fence
// hazard: the victim's meta claim (1ns lease) is stolen between its meta
// commit and its manifest write, so the victim's token lands AFTER the
// thief's and overwrites it. The post-install revalidation must catch the
// steal, refuse the victim's claim, and clear only the victim's own token
// — leaving the manifest fail-closed for BOTH holders (the documented
// liveness cost; the thief's next claim reinstalls its fence).
func TestS3WALMetaWriteThroughRevalidation(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	// The thief is a second full stack over the same bucket.
	thiefStore := NewS3WAL(cas, WithSyncClaimSink(refStore))
	t.Cleanup(thiefStore.Close)

	// After the victim's META claim commits (the first advance of the
	// meta object once armed), the thief steals the instantly-expired
	// claim and installs its own fence — all before the victim's fence
	// write goes out.
	var thiefClaimed bool
	stealing := false
	stole := false
	cas.Hooks.AfterPut = func(op, key string) {
		if key != s3walMetaKey || stealing || stole {
			return
		}
		stealing = true
		defer func() { stealing = false }()
		stole = true
		_, claimed, stolenFrom, err := thiefStore.TryBeginSync(ctx, repoID, "h-thief", time.Minute)
		if err != nil || !claimed || stolenFrom != "h-victim" {
			t.Errorf("thief steal = (%v, %q, %v), want steal from h-victim", claimed, stolenFrom, err)
		}
		thiefClaimed = claimed
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	up, claimed, stolenFrom, err := metaStore.TryBeginSync(ctx, repoID, "h-victim", time.Nanosecond)
	if err != nil || claimed {
		t.Fatalf("victim claim = (%+v, %v, %q, %v), want revalidation refusal without error", up, claimed, stolenFrom, err)
	}
	if !thiefClaimed {
		t.Fatal("the interleaved thief never claimed — the hazard was not reproduced")
	}
	cas.Hooks = s3wal.MemCASHooks{}

	// The meta claim belongs to the thief; the victim's late token was
	// cleared owner-guarded, so the manifest fences BOTH fail closed.
	got, gerr := metaStore.GetRepoUpstream(ctx, repoID)
	if gerr != nil || got.SyncClaimHolder != "h-thief" {
		t.Fatalf("meta claim = (%+v, %v), want the thief's", got, gerr)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "" {
		t.Fatalf("manifest token = %q, want cleared (victim overwrote the thief's, then self-revoked)", tok)
	}
	for _, holder := range []string{"h-victim", "h-thief"} {
		if won, cerr := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, holder), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); cerr != nil || won {
			t.Fatalf("fenced CASRef for %s = (%v, %v), want clean loss", holder, won, cerr)
		}
	}
}

// TestS3WALMetaWriteThroughDetachRevokesFence: DeleteRepoUpstream with a
// live claim must clear the manifest token, so the detached upstream's
// in-flight worker (between its fence check and its CASRef) loses
// cleanly AFTER the detach committed — the SQL twins' free guarantee.
func TestS3WALMetaWriteThroughDetachRevokesFence(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h1", time.Minute); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "h1" {
		t.Fatalf("manifest token = %q, want h1", got)
	}

	if err := metaStore.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("manifest token after detach = %q, want cleared", got)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h1"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || won {
		t.Fatalf("detached holder's fenced CASRef = (%v, %v), want clean loss", won, err)
	}
	if _, err := metaStore.GetRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("row after detach: %v, want ErrNotFound", err)
	}
}

// refusingSink fails every call: wiring it proves the exercised path
// makes no sink calls at all (any call would fail the operation).
type refusingSink struct{}

func (refusingSink) SetSyncClaim(context.Context, string, string) error {
	return errors.New("unexpected SetSyncClaim")
}

func (refusingSink) ClearSyncClaim(context.Context, string, string) error {
	return errors.New("unexpected ClearSyncClaim")
}

// TestS3WALMetaWriteThroughDetachNoClaim: a detach with no claim on the
// row needs no sink calls at all — there is no token to revoke.
func TestS3WALMetaWriteThroughDetachNoClaim(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	metaStore := NewS3WAL(cas, WithSyncClaimSink(refusingSink{}))
	t.Cleanup(metaStore.Close)
	repoID := seedS3WALUpstream(t, metaStore)

	if err := metaStore.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("detach without claim: %v", err)
	}
	if _, err := metaStore.GetRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("row after detach: %v, want ErrNotFound", err)
	}
	// A repeat detach classifies as ErrNotFound on the pre-read too.
	if err := metaStore.DeleteRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("repeat detach: %v, want ErrNotFound", err)
	}
}

// TestS3WALMetaWriteThroughDetachClearFailureResumes: when the phase-2
// fence clear fails, the detach must abort with the marked row — and the
// holder needed for cleanup — still durable: the claim and token stay
// intact, new claims are already refused, and a retry RESUMES the detach
// and completes the revocation. Only after that does the row read as
// ErrNotFound.
func TestS3WALMetaWriteThroughDetachClearFailureResumes(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h1", time.Minute); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}

	// Every manifest PUT fails definitively; the meta object is untouched.
	cas.Hooks.PutFault = func(op, key string) s3wal.PutFault {
		if strings.HasPrefix(key, s3wal.RepoPrefix) {
			return s3wal.FaultContention
		}
		return s3wal.FaultNone
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	if err := metaStore.DeleteRepoUpstream(ctx, repoID); err == nil {
		t.Fatal("detach with failing fence clear succeeded, want abort")
	}
	got, gerr := metaStore.GetRepoUpstream(ctx, repoID)
	if gerr != nil || !got.Syncing || got.SyncClaimHolder != "h1" {
		t.Fatalf("row after aborted detach = (%+v, %v), want the intact claim", got, gerr)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "h1" {
		t.Fatalf("manifest token after aborted detach = %q, want the intact h1", tok)
	}
	// The phase-1 mark survived the abort: a new claim on the detaching
	// row is refused before it could install a fresh fence (refused in fn,
	// before any manifest write — the armed fault proves none was sent).
	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h2", time.Hour); !errors.Is(err, ErrNotFound) || claimed {
		t.Fatalf("claim on detaching row = (%v, %v), want ErrNotFound refusal", claimed, err)
	}

	// The aborted detach RESUMES once the manifest heals: the retry
	// re-enters at the no-op mark, re-reads the holder, and completes.
	cas.Hooks = s3wal.MemCASHooks{}
	if err := metaStore.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("retried detach: %v", err)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "" {
		t.Fatalf("manifest token after retried detach = %q, want cleared", tok)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h1"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || won {
		t.Fatalf("detached holder's fenced CASRef = (%v, %v), want clean loss", won, err)
	}
	// ErrNotFound only now — with the fence provably revoked.
	if err := metaStore.DeleteRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("repeat detach: %v, want ErrNotFound", err)
	}
}

// TestS3WALMetaWriteThroughDetachFreshProcessResume: the crash-shaped
// variant — the process whose detach failed mid-way dies, and a FRESH
// process (new meta store and sink over the same bucket) retries. The
// marked row carries everything the retry needs; no state may live only
// in the dead process's memory.
func TestS3WALMetaWriteThroughDetachFreshProcessResume(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h1", time.Minute); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}
	cas.Hooks.PutFault = func(op, key string) s3wal.PutFault {
		if strings.HasPrefix(key, s3wal.RepoPrefix) {
			return s3wal.FaultContention
		}
		return s3wal.FaultNone
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })
	if err := metaStore.DeleteRepoUpstream(ctx, repoID); err == nil {
		t.Fatal("detach with failing fence clear succeeded, want abort")
	}
	cas.Hooks = s3wal.MemCASHooks{}

	// The "crashed" store is abandoned; a fresh stack resumes the detach.
	freshRef := ref.NewS3WAL(cas, nil)
	freshStore := NewS3WAL(cas, WithSyncClaimSink(freshRef))
	t.Cleanup(func() { freshStore.Close(); freshRef.Close() })
	if err := freshStore.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("fresh-process detach resume: %v", err)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "" {
		t.Fatalf("manifest token after resumed detach = %q, want cleared", tok)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h1"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || won {
		t.Fatalf("detached holder's fenced CASRef = (%v, %v), want clean loss", won, err)
	}
	if _, err := freshStore.GetRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("row after resumed detach: %v, want ErrNotFound", err)
	}
}

// TestS3WALMetaWriteThroughDetachStealRace: the r1 hazard — a thief
// stealing an expired claim between the detacher's fence clear and its
// row delete — is now EXCLUDED by construction: the phase-1 mark is
// durable before any manifest activity, so the thief's claim is refused
// mid-detach and no new fence can appear on the dying row.
func TestS3WALMetaWriteThroughDetachStealRace(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	// The thief is a second full stack over the same bucket (its own ref
	// store, so the hook's re-entrant claim takes no in-process lock the
	// detacher's phase-2 clear is still holding).
	thiefRef := ref.NewS3WAL(cas, nil)
	thiefStore := NewS3WAL(cas, WithSyncClaimSink(thiefRef))
	t.Cleanup(func() { thiefStore.Close(); thiefRef.Close() })

	// h-old's instantly-expired claim leaves a live token for the
	// detacher's phase-2 clear to observe and remove — exactly the state
	// a thief would steal.
	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h-old", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("h-old claim = (%v, %v), want claimed", claimed, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "h-old" {
		t.Fatalf("manifest token = %q, want h-old", got)
	}

	// The detacher's phase-2 clear PUT (the first manifest advance after
	// arming, with the phase-1 mark already durable) wakes the thief,
	// whose steal attempt races the still-running detach.
	thiefRefused := false
	stealing, stole := false, false
	cas.Hooks.AfterPut = func(op, key string) {
		if !strings.HasPrefix(key, s3wal.RepoPrefix) || stealing || stole {
			return
		}
		stealing = true
		defer func() { stealing = false }()
		stole = true
		_, claimed, _, err := thiefStore.TryBeginSync(ctx, repoID, "h-thief", time.Minute)
		if !errors.Is(err, ErrNotFound) || claimed {
			t.Errorf("thief steal on detaching row = (%v, %v), want ErrNotFound refusal", claimed, err)
		}
		thiefRefused = errors.Is(err, ErrNotFound) && !claimed
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	if err := metaStore.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if !thiefRefused {
		t.Fatal("the interleaved thief never raced the detach — the interleaving was not reproduced")
	}
	cas.Hooks = s3wal.MemCASHooks{}

	// No token survived: both the dead holder and the refused thief fail
	// closed after the detach committed.
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("manifest token after detach = %q, want cleared", got)
	}
	for _, holder := range []string{"h-old", "h-thief"} {
		if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, holder), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || won {
			t.Fatalf("fenced CASRef for %s = (%v, %v), want clean loss", holder, won, err)
		}
	}
	if _, err := metaStore.GetRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("row after detach: %v, want ErrNotFound", err)
	}
}

// TestS3WALMetaWriteThroughDetachLateInstall covers the revalidation arm
// of the detach ordering proof: a claim whose meta commit precedes the
// phase-1 mark installs its fence AFTER the detacher's phase-2 clear
// no-oped on the not-yet-written token. The claimant's revalidation must
// see the mark, refuse the claim, and self-revoke its token — even while
// the interrupted detach (phase-3 delete faulted) has not deleted the
// row yet — and the resumed detach then completes the revocation.
func TestS3WALMetaWriteThroughDetachLateInstall(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	// The detacher is a second full stack over the same bucket (the hook
	// below runs inside the victim's meta mutate, which holds the
	// victim's store mutex).
	detachRef := ref.NewS3WAL(cas, nil)
	detachStore := NewS3WAL(cas, WithSyncClaimSink(detachRef))
	t.Cleanup(func() { detachStore.Close(); detachRef.Close() })

	// After the victim's meta claim commits (its claim PUT on the meta
	// object) and BEFORE its fence install, the whole detach runs: the
	// mark lands (meta advance #1), the phase-2 clear no-ops on the
	// missing token (no manifest PUT), and the phase-3 delete is faulted
	// (meta advances #2+ dropped) so the marked row survives, interrupted.
	interposing, interposed := false, false
	var detachErr error
	metaAdvances := 0
	cas.Hooks.PutFault = func(op, key string) s3wal.PutFault {
		if !interposing || key != s3walMetaKey || op != "advance" {
			return s3wal.FaultNone
		}
		metaAdvances++
		if metaAdvances >= 2 {
			return s3wal.FaultAmbiguousDropped
		}
		return s3wal.FaultNone
	}
	cas.Hooks.AfterPut = func(op, key string) {
		if key != s3walMetaKey || interposing || interposed {
			return
		}
		interposing = true
		defer func() { interposing = false }()
		interposed = true
		detachErr = detachStore.DeleteRepoUpstream(ctx, repoID)
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	up, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h-late", time.Minute)
	if err != nil || claimed {
		t.Fatalf("victim claim = (%+v, %v, %v), want revalidation refusal without error", up, claimed, err)
	}
	if !interposed {
		t.Fatal("the detach never interposed — the interleaving was not reproduced")
	}
	if detachErr == nil {
		t.Fatal("interposed detach succeeded, want the faulted phase-3 abort")
	}
	cas.Hooks = s3wal.MemCASHooks{}

	// The victim installed its token after the detacher's clear, then
	// self-revoked on the marked row: the manifest fences everyone out.
	if tok := manifestSyncClaim(t, cas, repoID); tok != "" {
		t.Fatalf("manifest token after refused late install = %q, want self-revoked", tok)
	}

	// The interrupted detach resumes: the marked row still carries the
	// victim's holder (its claim was never rolled back), so the retry
	// re-clears it (a no-op) and deletes the row.
	if err := detachStore.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("resumed detach: %v", err)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h-late"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || won {
		t.Fatalf("late claimant's fenced CASRef = (%v, %v), want clean loss", won, err)
	}
	if _, err := metaStore.GetRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("row after detach: %v, want ErrNotFound", err)
	}
}

// TestS3WALMetaWriteThroughStealPreClearsVictimFence closes the
// expired-victim window: a thief that commits its meta steal and then
// "crashes" (every subsequent write faulted) before installing its own
// fence must ALREADY have cleared the victim's token — otherwise the
// expired-but-alive victim would keep writing under a claim it lost,
// where the SQL row fence excludes it atomically with the steal.
func TestS3WALMetaWriteThroughStealPreClearsVictimFence(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	// The victim is handed out under an instantly-expired lease and is
	// still alive: its fenced writes pass while its token stands.
	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h-victim", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("victim claim = (%v, %v), want claimed", claimed, err)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h-victim"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || !won {
		t.Fatalf("victim's fenced CASRef under its token = (%v, %v), want win", won, err)
	}

	// The thief is a second full stack over the same bucket.
	thiefRef := ref.NewS3WAL(cas, nil)
	thiefStore := NewS3WAL(cas, WithSyncClaimSink(thiefRef))
	t.Cleanup(func() { thiefStore.Close(); thiefRef.Close() })

	// The instant the thief's steal CAS lands (the first meta advance
	// after arming), capture the manifest token and "crash" the thief:
	// every later write fails, so neither its fence install nor its
	// rollback can land — the meta claim strands with the dead thief.
	tokenAtSteal := "unset"
	crashed := false
	cas.Hooks.AfterPut = func(op, key string) {
		if key != s3walMetaKey || crashed {
			return
		}
		crashed = true
		tokenAtSteal = manifestSyncClaim(t, cas, repoID)
		cas.Hooks.PutFault = func(op, key string) s3wal.PutFault {
			if key == s3walMetaKey {
				return s3wal.FaultAmbiguousDropped
			}
			return s3wal.FaultContention
		}
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	if _, claimed, _, err := thiefStore.TryBeginSync(ctx, repoID, "h-thief", time.Nanosecond); err == nil || claimed {
		t.Fatalf("crashed thief claim = (%v, %v), want write-through failure", claimed, err)
	}
	if !crashed {
		t.Fatal("the thief never committed its steal — the crash was not reproduced")
	}
	cas.Hooks = s3wal.MemCASHooks{}

	// The victim's token was gone BEFORE the steal committed, nothing
	// reinstalled it, and the stranded claim names the dead thief.
	if tokenAtSteal != "" {
		t.Fatalf("manifest token at steal commit = %q, want already cleared", tokenAtSteal)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "" {
		t.Fatalf("manifest token after thief crash = %q, want none", tok)
	}
	got, gerr := metaStore.GetRepoUpstream(ctx, repoID)
	if gerr != nil || !got.Syncing || got.SyncClaimHolder != "h-thief" {
		t.Fatalf("row after thief crash = (%+v, %v), want the stranded stolen claim", got, gerr)
	}
	// The expired-but-alive victim is now excluded, exactly like SQL.
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h-victim"), repoID, "refs/heads/main", testSHA1, testSHA2); err != nil || won {
		t.Fatalf("victim's fenced CASRef after steal = (%v, %v), want clean loss", won, err)
	}
	// The lease recovers the crash: a later thief steals the stranded
	// claim (its pre-clear no-ops on the token-less manifest).
	if _, claimed, stolenFrom, err := metaStore.TryBeginSync(ctx, repoID, "h-thief2", time.Minute); err != nil || !claimed || stolenFrom != "h-thief" {
		t.Fatalf("recovery steal = (%v, %q, %v), want steal from h-thief", claimed, stolenFrom, err)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "h-thief2" {
		t.Fatalf("manifest token after recovery steal = %q, want h-thief2", tok)
	}
}

// TestS3WALMetaWriteThroughStealPreClearFailureAborts: when the victim's
// fence cannot be cleared, the steal must abort with NOTHING consumed —
// the victim's expired claim and token stay intact — and a retry after
// the manifest heals completes the steal.
func TestS3WALMetaWriteThroughStealPreClearFailureAborts(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, _ := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h-victim", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("victim claim = (%v, %v), want claimed", claimed, err)
	}

	// Every manifest PUT fails definitively; the meta object is untouched.
	cas.Hooks.PutFault = func(op, key string) s3wal.PutFault {
		if strings.HasPrefix(key, s3wal.RepoPrefix) {
			return s3wal.FaultContention
		}
		return s3wal.FaultNone
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h-thief", time.Minute); err == nil || claimed {
		t.Fatalf("steal with failing victim clear = (%v, %v), want abort", claimed, err)
	}
	got, gerr := metaStore.GetRepoUpstream(ctx, repoID)
	if gerr != nil || !got.Syncing || got.SyncClaimHolder != "h-victim" {
		t.Fatalf("row after aborted steal = (%+v, %v), want the victim's intact claim", got, gerr)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "h-victim" {
		t.Fatalf("manifest token after aborted steal = %q, want the victim's intact token", tok)
	}

	// The aborted steal is plainly retryable once the manifest heals.
	cas.Hooks = s3wal.MemCASHooks{}
	if _, claimed, stolenFrom, err := metaStore.TryBeginSync(ctx, repoID, "h-thief", time.Minute); err != nil || !claimed || stolenFrom != "h-victim" {
		t.Fatalf("retried steal = (%v, %q, %v), want steal from h-victim", claimed, stolenFrom, err)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "h-thief" {
		t.Fatalf("manifest token after retried steal = %q, want h-thief", tok)
	}
}

// TestS3WALMetaWriteThroughStealVsFinishRace: the victim's FinishSync
// racing the thief's pre-clear stays correct — FinishSync's own clear
// no-ops on the already-removed token (owner-guarded idempotence), the
// finish lands because the meta claim is still the victim's, and the
// thief then takes the FRESH-claim path (no steal) on the finished row.
func TestS3WALMetaWriteThroughStealVsFinishRace(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, _ := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h-victim", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("victim claim = (%v, %v), want claimed", claimed, err)
	}

	// The thief is a second full stack over the same bucket (its own ref
	// store, so the interposed FinishSync shares no in-process locks with
	// the pre-clear it interrupts).
	thiefRef := ref.NewS3WAL(cas, nil)
	thiefStore := NewS3WAL(cas, WithSyncClaimSink(thiefRef))
	t.Cleanup(func() { thiefStore.Close(); thiefRef.Close() })

	// The thief's pre-clear PUT (the first manifest advance after arming,
	// removing the victim's token) wakes the victim, which finishes its
	// run before the steal CAS is attempted.
	var finished RepoUpstream
	var finishErr error
	interposing, interposed := false, false
	cas.Hooks.AfterPut = func(op, key string) {
		if !strings.HasPrefix(key, s3wal.RepoPrefix) || interposing || interposed {
			return
		}
		interposing = true
		defer func() { interposing = false }()
		interposed = true
		finished, finishErr = metaStore.FinishSync(ctx, repoID, "h-victim", true)
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	up, claimed, stolenFrom, err := thiefStore.TryBeginSync(ctx, repoID, "h-thief", time.Minute)
	if err != nil || !claimed || stolenFrom != "" {
		t.Fatalf("thief claim = (%+v, %v, %q, %v), want a fresh (non-steal) claim", up, claimed, stolenFrom, err)
	}
	if !interposed {
		t.Fatal("the victim's finish never interposed — the race was not reproduced")
	}
	cas.Hooks = s3wal.MemCASHooks{}

	// The interposed finish landed on the victim's still-live meta claim.
	if finishErr != nil || finished.RunCount != 1 || finished.SuccessCount != 1 || finished.Syncing {
		t.Fatalf("interposed FinishSync = (%+v, %v), want a clean finish", finished, finishErr)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "h-thief" {
		t.Fatalf("manifest token after fresh claim = %q, want h-thief", tok)
	}
}

// TestS3WALMetaWriteThroughSameHolderReclaim pins the uniform handling of
// a same-holder re-claim after expiry (test-only: production holders are
// per-run unique): the pre-clear removes the holder's own stale token and
// the post-commit write-through reinstalls it, so the re-claimed holder's
// fenced writes win again.
func TestS3WALMetaWriteThroughSameHolderReclaim(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h1", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}
	if _, claimed, stolenFrom, err := metaStore.TryBeginSync(ctx, repoID, "h1", time.Minute); err != nil || !claimed || stolenFrom != "h1" {
		t.Fatalf("same-holder re-claim = (%v, %q, %v), want a self-steal", claimed, stolenFrom, err)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "h1" {
		t.Fatalf("manifest token after re-claim = %q, want the reinstalled h1", tok)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h1"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || !won {
		t.Fatalf("re-claimed holder's fenced CASRef = (%v, %v), want win", won, err)
	}
	if _, err := metaStore.FinishSync(ctx, repoID, "h1", true); err != nil {
		t.Fatalf("FinishSync after re-claim: %v", err)
	}
}
