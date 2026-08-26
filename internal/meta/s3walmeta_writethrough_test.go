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

	"github.com/coder/code-storage/internal/ref"
	"github.com/coder/code-storage/internal/ref/s3wal"
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

// TestS3WALMetaWriteThroughDetachPreClearFailure: when the fence clear
// fails, the detach must abort with NOTHING consumed — the meta row and
// its claim stay intact, the token stays fencing, and a retry after the
// manifest heals completes the detach.
func TestS3WALMetaWriteThroughDetachPreClearFailure(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, _ := newWriteThroughStores(t)
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

	// The aborted detach is plainly retryable once the manifest heals.
	cas.Hooks = s3wal.MemCASHooks{}
	if err := metaStore.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("retried detach: %v", err)
	}
	if tok := manifestSyncClaim(t, cas, repoID); tok != "" {
		t.Fatalf("manifest token after retried detach = %q, want cleared", tok)
	}
}

// TestS3WALMetaWriteThroughDetachStealRace reproduces the interleaving
// the pre-clear alone cannot cover: between the detacher's pre-clear PUT
// and its row delete, a thief steals the expired claim, installs its
// fence, and REVALIDATES successfully (the row is still present, so it
// does NOT self-revoke). The delete must capture the thief's holder from
// the deleted snapshot and clear its token post-delete, leaving the
// detached thief's in-flight fenced CASRef a clean loss.
func TestS3WALMetaWriteThroughDetachStealRace(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, refStore := newWriteThroughStores(t)
	repoID := seedS3WALUpstream(t, metaStore)

	// The thief is a second full stack over the same bucket (its own ref
	// store, so the hook's re-entrant claim takes no in-process lock the
	// detacher's pre-clear is still holding).
	thiefRef := ref.NewS3WAL(cas, nil)
	thiefStore := NewS3WAL(cas, WithSyncClaimSink(thiefRef))
	t.Cleanup(func() { thiefStore.Close(); thiefRef.Close() })

	// h-old's instantly-expired claim leaves a live token for the
	// detacher's pre-clear to observe and remove.
	if _, claimed, _, err := metaStore.TryBeginSync(ctx, repoID, "h-old", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("h-old claim = (%v, %v), want claimed", claimed, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "h-old" {
		t.Fatalf("manifest token = %q, want h-old", got)
	}

	// The detacher's pre-clear PUT (the first manifest advance after
	// arming) wakes the thief, whose full claim — meta steal, fence
	// install, passing revalidation — lands before the row delete.
	var thiefClaimed bool
	stealing, stole := false, false
	cas.Hooks.AfterPut = func(op, key string) {
		if !strings.HasPrefix(key, s3wal.RepoPrefix) || stealing || stole {
			return
		}
		stealing = true
		defer func() { stealing = false }()
		stole = true
		_, claimed, stolenFrom, err := thiefStore.TryBeginSync(ctx, repoID, "h-thief", time.Minute)
		if err != nil || !claimed || stolenFrom != "h-old" {
			t.Errorf("thief steal = (%v, %q, %v), want steal from h-old", claimed, stolenFrom, err)
		}
		thiefClaimed = claimed
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })

	if err := metaStore.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if !thiefClaimed {
		t.Fatal("the interleaved thief never claimed — the race was not reproduced")
	}
	cas.Hooks = s3wal.MemCASHooks{}

	// The captured-holder post-delete clear revoked the thief's token:
	// the detached thief's in-flight fenced CASRef loses cleanly.
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("manifest token after detach = %q, want cleared", got)
	}
	if won, err := refStore.CASRef(ref.WithSyncClaimFence(ctx, repoID, "h-thief"), repoID, "refs/heads/main", ref.ZeroSHA, testSHA1); err != nil || won {
		t.Fatalf("detached thief's fenced CASRef = (%v, %v), want clean loss", won, err)
	}
	if _, err := metaStore.GetRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("row after detach: %v, want ErrNotFound", err)
	}
}
