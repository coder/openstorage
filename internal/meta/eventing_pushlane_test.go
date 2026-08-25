// Push-lane regression tests (issue #90): the outbound claim lease and
// three-level pending marker mirror the inbound lane (#86/#79 semantics)
// on separate columns, so the two directions never interfere.

package meta

import (
	"errors"
	"testing"
	"time"
)

func TestPushClaimLease(t *testing.T) { runBackends(t, testPushClaimLease) }

func testPushClaimLease(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	// An UNEXPIRED claim is never stolen.
	row, claimed, force, stolenFrom, err := s.TryBeginPush(ctx, repoID, "holder-live", time.Minute)
	if err != nil || !claimed || force || stolenFrom != "" {
		t.Fatalf("first claim = (%+v, %v, %v, %q, %v), want clean non-force claim", row, claimed, force, stolenFrom, err)
	}
	if row.PushClaimHolder != "holder-live" || !row.PushClaimExpiresAt.After(row.CreatedAt) {
		t.Errorf("claimed row lease = holder %q expires %v, want holder-live with future expiry",
			row.PushClaimHolder, row.PushClaimExpiresAt)
	}
	if _, claimed, _, stolenFrom, err = s.TryBeginPush(ctx, repoID, "holder-thief", time.Minute); err != nil || claimed || stolenFrom != "" {
		t.Fatalf("contended claim = (%v, %q, %v), want refused without steal", claimed, stolenFrom, err)
	}
	if _, err := s.FinishPush(ctx, repoID, "holder-live", true); err != nil {
		t.Fatalf("FinishPush: %v", err)
	}

	// Dead-holder choreography: an expired push lease is stolen outright —
	// no legacy grace exists on this lane. A FORCED request marked while
	// the row is wedged survives to the thief's claim, which consumes it.
	if _, claimed, _, _, err := s.TryBeginPush(ctx, repoID, "holder-dead", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("dead-holder claim = (%v, %v), want claimed", claimed, err)
	}
	if err := s.RequestPush(ctx, repoID, true); err != nil {
		t.Fatalf("RequestPush while wedged: %v", err)
	}
	row, claimed, force, stolenFrom, err = s.TryBeginPush(ctx, repoID, "holder-thief", time.Minute)
	if err != nil || !claimed || stolenFrom != "holder-dead" {
		t.Fatalf("steal = (%+v, %v, %q, %v), want steal from holder-dead", row, claimed, stolenFrom, err)
	}
	if !force {
		t.Error("stolen claim dropped the forced pending level")
	}
	if row.PushClaimHolder != "holder-thief" || row.PushPending != 0 {
		t.Errorf("stolen row = %+v, want thief's lease with consumed pending marker", row)
	}

	// The dead holder completes late: ErrClaimLost, counters untouched.
	if _, err := s.FinishPush(ctx, repoID, "holder-dead", true); !errors.Is(err, ErrClaimLost) {
		t.Errorf("late finish err = %v, want ErrClaimLost", err)
	}
	got, err := s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatalf("GetRepoUpstream: %v", err)
	}
	if !got.PushSyncing || got.PushClaimHolder != "holder-thief" || got.PushRunCount != 1 || got.PushSuccessCount != 1 {
		t.Errorf("row after late finish = %+v, want thief's intact claim and one counted run", got)
	}

	fin, err := s.FinishPush(ctx, repoID, "holder-thief", false)
	if err != nil {
		t.Fatalf("thief FinishPush: %v", err)
	}
	if fin.PushSyncing || fin.PushRunCount != 2 || fin.PushSuccessCount != 1 ||
		fin.PushClaimHolder != "" || !fin.PushClaimExpiresAt.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("row after thief finish = %+v, want idle row with cleared lease", fin)
	}
}

func TestPushClaimForceInheritance(t *testing.T) { runBackends(t, testPushClaimForceInheritance) }

// A crash between a forced claim and its finish must not downgrade the
// accepted forced request: the claim persists the consumed mandate
// (push_claim_force) and the steal inherits it.
func testPushClaimForceInheritance(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	// Forced request, claimed by a holder that then "crashes" (1ns lease,
	// never finishes). The claim consumed the marker but persists force.
	if err := s.RequestPush(ctx, repoID, true); err != nil {
		t.Fatal(err)
	}
	row, claimed, force, _, err := s.TryBeginPush(ctx, repoID, "holder-crash", time.Nanosecond)
	if err != nil || !claimed || !force {
		t.Fatalf("forced claim = (%v, %v, %v), want forced claim", claimed, force, err)
	}
	if !row.PushClaimForce || row.PushPending != 0 {
		t.Fatalf("claimed row = %+v, want persisted force with consumed marker", row)
	}

	// The rescue steals the expired claim with NO fresh marker: force must
	// be inherited from the dead claim, not silently dropped.
	row, claimed, force, stolenFrom, err := s.TryBeginPush(ctx, repoID, "holder-rescue", time.Minute)
	if err != nil || !claimed || stolenFrom != "holder-crash" {
		t.Fatalf("steal = (%v, %q, %v), want steal from holder-crash", claimed, stolenFrom, err)
	}
	if !force || !row.PushClaimForce {
		t.Fatalf("steal dropped the inherited force mandate: force=%v row=%+v", force, row)
	}

	// FinishPush clears the mandate; the next claim is non-forced.
	if _, err := s.FinishPush(ctx, repoID, "holder-rescue", true); err != nil {
		t.Fatalf("FinishPush: %v", err)
	}
	row, claimed, force, _, err = s.TryBeginPush(ctx, repoID, "holder-next", time.Minute)
	if err != nil || !claimed || force || row.PushClaimForce {
		t.Fatalf("post-finish claim = (%v, %v, %+v), want clean non-force claim", claimed, force, row)
	}
}

func TestPendingPushIncludesOrphanedClaims(t *testing.T) { runBackends(t, testPendingPushOrphans) }

// Local-mode boot recovery must see repos whose previous process died AFTER
// its claim consumed the pending marker: PendingPushRepoIDs lists in-flight
// claims too, not only marked rows.
func testPendingPushOrphans(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	if err := s.RequestPush(ctx, repoID, false); err != nil {
		t.Fatal(err)
	}
	if _, claimed, _, _, err := s.TryBeginPush(ctx, repoID, "holder-orphan", time.Minute); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}
	// Marker consumed, claim in flight — the crashed-process row shape.
	ids, err := s.PendingPushRepoIDs(ctx)
	if err != nil {
		t.Fatalf("PendingPushRepoIDs: %v", err)
	}
	found := false
	for _, id := range ids {
		found = found || id == repoID
	}
	if !found {
		t.Fatalf("PendingPushRepoIDs = %v, want to include orphan-claimed %s", ids, repoID)
	}
	// A finished run clears the row out of the listing.
	if _, err := s.FinishPush(ctx, repoID, "holder-orphan", true); err != nil {
		t.Fatal(err)
	}
	ids, err = s.PendingPushRepoIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id == repoID {
			t.Fatalf("idle repo %s still listed for recovery", repoID)
		}
	}
}

func TestPushPendingLevels(t *testing.T) { runBackends(t, testPushPendingLevels) }

func testPushPendingLevels(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	// Levels coalesce upward: force is never downgraded by a plain request.
	if err := s.RequestPush(ctx, repoID, true); err != nil {
		t.Fatalf("RequestPush(force): %v", err)
	}
	if err := s.RequestPush(ctx, repoID, false); err != nil {
		t.Fatalf("RequestPush(plain): %v", err)
	}
	up, err := s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending != 2 {
		t.Fatalf("PushPending = %d after force+plain, want 2", up.PushPending)
	}

	// PendingPushRepoIDs surfaces the marked repo for boot recovery.
	ids, err := s.PendingPushRepoIDs(ctx)
	if err != nil {
		t.Fatalf("PendingPushRepoIDs: %v", err)
	}
	found := false
	for _, id := range ids {
		if id == repoID {
			found = true
		}
	}
	if !found {
		t.Fatalf("PendingPushRepoIDs = %v, want to include %s", ids, repoID)
	}

	// The claim consumes the marker whole and reports its force bit.
	if _, claimed, force, _, err := s.TryBeginPush(ctx, repoID, "h1", time.Minute); err != nil || !claimed || !force {
		t.Fatalf("claim = (%v, %v, %v), want forced claim", claimed, force, err)
	}
	up, err = s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending != 0 {
		t.Fatalf("PushPending = %d after claim, want consumed", up.PushPending)
	}

	// Unknown repo: ErrNotFound.
	if err := s.RequestPush(ctx, "repo_nope", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RequestPush on unknown repo = %v, want ErrNotFound", err)
	}
}

func TestPushLaneIndependentOfSyncLane(t *testing.T) { runBackends(t, testPushLaneIndependent) }

// The two directions lease different resources: an in-flight pull claim
// must not refuse a push claim, and vice versa; markers stay per-lane.
func testPushLaneIndependent(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	if _, claimed, _, err := s.TryBeginSync(ctx, repoID, "pull-holder", time.Minute); err != nil || !claimed {
		t.Fatalf("pull claim = (%v, %v), want claimed", claimed, err)
	}
	if err := s.RequestSync(ctx, repoID); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	row, claimed, _, _, err := s.TryBeginPush(ctx, repoID, "push-holder", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("push claim under pull claim = (%v, %v), want claimed", claimed, err)
	}
	if !row.SyncPending {
		t.Errorf("push claim consumed the PULL pending marker: %+v", row)
	}
	if !row.Syncing || row.SyncClaimHolder != "pull-holder" {
		t.Errorf("push claim disturbed the pull lease: %+v", row)
	}
	if _, err := s.FinishPush(ctx, repoID, "push-holder", true); err != nil {
		t.Fatalf("FinishPush: %v", err)
	}
	fin, err := s.FinishSync(ctx, repoID, "pull-holder", true)
	if err != nil {
		t.Fatalf("FinishSync: %v", err)
	}
	if fin.RunCount != 1 || fin.PushRunCount != 1 {
		t.Errorf("counters crossed lanes: %+v", fin)
	}
}

func TestSetRepoUpstreamForwardOnPush(t *testing.T) { runBackends(t, testSetUpstreamForward) }

func testSetUpstreamForward(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo",
		UpstreamHost: "github.com", ForwardOnPush: true,
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	up, err := s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if !up.ForwardOnPush {
		t.Fatalf("ForwardOnPush not persisted: %+v", up)
	}
	if up.PushPending != 0 || up.PushSyncing || up.PushRunCount != 0 || up.PushSuccessCount != 0 {
		t.Fatalf("fresh row has non-zero push-lane state: %+v", up)
	}
}
