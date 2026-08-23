// Regression tests for the sync_pending request marker (issue #79): the
// pull-upstream 202 promise is durable — RequestSync sets it, a claim
// consumes it (the claimed run's fetch postdates the request), and a
// request arriving while a run holds the claim survives to FinishSync so
// the finisher can schedule a follow-up run.

package meta

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestRequestSyncPendingLifecycle(t *testing.T) {
	runBackends(t, testRequestSyncPendingLifecycle)
}

func testRequestSyncPendingLifecycle(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")

	// Detached repo: ErrNotFound.
	if err := s.RequestSync(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Errorf("RequestSync without upstream err = %v, want ErrNotFound", err)
	}

	up := RepoUpstream{RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}
	if _, err := s.SetRepoUpstream(ctx, up); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	got, err := s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatalf("GetRepoUpstream: %v", err)
	}
	if got.SyncPending {
		t.Errorf("fresh row has SyncPending set: %+v", got)
	}

	// Request before any claim: pending until the next claim consumes it,
	// and visible to boot recovery.
	if err := s.RequestSync(ctx, repoID); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	if got, err = s.GetRepoUpstream(ctx, repoID); err != nil || !got.SyncPending {
		t.Fatalf("after RequestSync: row = %+v, err = %v, want SyncPending", got, err)
	}
	// Containment, not equality: the backend database is shared across
	// the package's tests, which may leave their own pending rows.
	if ids, err := s.PendingSyncRepoIDs(ctx); err != nil || !slices.Contains(ids, repoID) {
		t.Errorf("PendingSyncRepoIDs = (%v, %v), want %s listed", ids, err, repoID)
	}
	claimed, ok, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim = (%+v, %v, %v), want claimed", claimed, ok, err)
	}
	if claimed.SyncPending {
		t.Errorf("claim did not consume SyncPending: %+v", claimed)
	}

	// Request while the claim is held: FinishSync must report it (the
	// finished run's fetch may predate the request) and leave it set for
	// the follow-up run's claim to consume.
	if err := s.RequestSync(ctx, repoID); err != nil {
		t.Fatalf("RequestSync during claim: %v", err)
	}
	fin, err := s.FinishSync(ctx, repoID, testClaimHolder, true)
	if err != nil {
		t.Fatalf("FinishSync: %v", err)
	}
	if !fin.SyncPending || fin.Syncing || fin.RunCount != 1 || fin.SuccessCount != 1 {
		t.Errorf("FinishSync row = %+v, want SyncPending with cleared claim and counted run", fin)
	}
	claimed, ok, _, err = s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute)
	if err != nil || !ok {
		t.Fatalf("follow-up claim = (%+v, %v, %v), want claimed", claimed, ok, err)
	}
	if claimed.SyncPending {
		t.Errorf("follow-up claim did not consume SyncPending: %+v", claimed)
	}
	if fin, err = s.FinishSync(ctx, repoID, testClaimHolder, true); err != nil || fin.SyncPending {
		t.Errorf("quiet FinishSync row = %+v, err = %v, want SyncPending clear", fin, err)
	}

	// A conflicting claim attempt must NOT consume the marker: only the
	// holder's post-claim fetch can cover the request.
	if _, ok, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute); err != nil || !ok {
		t.Fatalf("re-claim failed: (%v, %v)", ok, err)
	}
	if err := s.RequestSync(ctx, repoID); err != nil {
		t.Fatalf("RequestSync during claim: %v", err)
	}
	if _, ok, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute); err != nil || ok {
		t.Fatalf("conflicting claim = (%v, %v), want not claimed", ok, err)
	}
	if got, err = s.GetRepoUpstream(ctx, repoID); err != nil || !got.SyncPending {
		t.Errorf("conflicting claim consumed SyncPending: row = %+v, err = %v", got, err)
	}
}
