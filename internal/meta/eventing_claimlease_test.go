// Regression tests for the sync claim lease (issue #86): a holder process
// that dies between TryBeginSync and FinishSync must not wedge the repo —
// a conflicting claimant steals a provably-expired claim atomically, and
// the dead holder's late FinishSync (if the process somehow survives)
// fails the holder-token guard instead of clearing the thief's claim.

package meta

import (
	"errors"
	"testing"
	"time"
)

func TestSyncClaimLease(t *testing.T) { runBackends(t, testSyncClaimLease) }

func testSyncClaimLease(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, "")
	up := RepoUpstream{RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}
	if _, err := s.SetRepoUpstream(ctx, up); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	// An UNEXPIRED claim is never stolen.
	row, claimed, stolenFrom, err := s.TryBeginSync(ctx, repoID, "holder-live", time.Minute)
	if err != nil || !claimed || stolenFrom != "" {
		t.Fatalf("first claim = (%+v, %v, %q, %v), want clean claim", row, claimed, stolenFrom, err)
	}
	if row.SyncClaimHolder != "holder-live" || !row.SyncClaimExpiresAt.After(row.CreatedAt) {
		t.Errorf("claimed row lease = holder %q expires %v, want holder-live with future expiry",
			row.SyncClaimHolder, row.SyncClaimExpiresAt)
	}
	if _, claimed, stolenFrom, err = s.TryBeginSync(ctx, repoID, "holder-thief", time.Minute); err != nil || claimed || stolenFrom != "" {
		t.Fatalf("contended claim = (%v, %q, %v), want refused without steal", claimed, stolenFrom, err)
	}
	if _, err := s.FinishSync(ctx, repoID, "holder-live", true); err != nil {
		t.Fatalf("FinishSync: %v", err)
	}

	// Dead-holder choreography: claim with a lease that expires
	// immediately and never finish — the crashed-process shape. A request
	// marked while the row is wedged must survive to the thief's claim.
	if _, claimed, _, err := s.TryBeginSync(ctx, repoID, "holder-dead", time.Nanosecond); err != nil || !claimed {
		t.Fatalf("dead-holder claim = (%v, %v), want claimed", claimed, err)
	}
	if err := s.RequestSync(ctx, repoID); err != nil {
		t.Fatalf("RequestSync while wedged: %v", err)
	}
	row, claimed, stolenFrom, err = s.TryBeginSync(ctx, repoID, "holder-thief", time.Minute)
	if err != nil || !claimed || stolenFrom != "holder-dead" {
		t.Fatalf("steal = (%+v, %v, %q, %v), want steal from holder-dead", row, claimed, stolenFrom, err)
	}
	if row.SyncClaimHolder != "holder-thief" || row.SyncPending {
		t.Errorf("stolen row = %+v, want thief's lease with consumed pending marker", row)
	}

	// The dead holder completes late: it must NOT clear the thief's claim
	// or touch the counters.
	if _, err := s.FinishSync(ctx, repoID, "holder-dead", true); !errors.Is(err, ErrClaimLost) {
		t.Errorf("late finish err = %v, want ErrClaimLost", err)
	}
	got, err := s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatalf("GetRepoUpstream: %v", err)
	}
	if !got.Syncing || got.SyncClaimHolder != "holder-thief" || got.RunCount != 1 || got.SuccessCount != 1 {
		t.Errorf("row after late finish = %+v, want thief's intact claim and one counted run", got)
	}

	// The thief finishes normally; the lease is cleared with the claim.
	fin, err := s.FinishSync(ctx, repoID, "holder-thief", true)
	if err != nil {
		t.Fatalf("thief FinishSync: %v", err)
	}
	if fin.Syncing || fin.RunCount != 2 || fin.SuccessCount != 2 ||
		fin.SyncClaimHolder != "" || !fin.SyncClaimExpiresAt.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("row after thief finish = %+v, want idle row with cleared lease", fin)
	}
}
