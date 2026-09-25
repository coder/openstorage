package meta

// PG shutdown restores (issue #343): RestoreSync/RestorePush release the
// holder's claim and re-mark the request without counting a run, and keep
// FinishSync's guard answers for stolen and missing claims.

import (
	"errors"
	"testing"
	"time"
)

func TestPGRestoreRuns(t *testing.T) {
	ctx := t.Context()
	s := pgBackend(t).(*PG)
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatal(err)
	}

	// Pull lane.
	if err := s.RequestSync(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if _, claimed, _, err := s.TryBeginSync(ctx, repoID, "h1", time.Minute); err != nil || !claimed {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	if _, err := s.RestoreSync(ctx, repoID, "other"); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("restore by another holder = %v, want ErrClaimLost", err)
	}
	row, err := s.RestoreSync(ctx, repoID, "h1")
	if err != nil {
		t.Fatal(err)
	}
	if row.Syncing || !row.SyncPending || row.RunCount != 0 || row.SyncClaimHolder != "" {
		t.Fatalf("restored row %+v, want no claim, the request pending, no run counted", row)
	}
	if _, err := s.RestoreSync(ctx, repoID, "h1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second restore = %v, want ErrNotFound (no run in flight)", err)
	}

	// Push lane: the consumed force level comes back; a higher pending
	// level is never lowered.
	if err := s.RequestPush(ctx, repoID, true); err != nil {
		t.Fatal(err)
	}
	if _, claimed, force, _, err := s.TryBeginPush(ctx, repoID, "p1", time.Minute); err != nil || !claimed || !force {
		t.Fatalf("push claim: %v %v %v", claimed, force, err)
	}
	row, err = s.RestorePush(ctx, repoID, "p1", true)
	if err != nil {
		t.Fatal(err)
	}
	if row.PushSyncing || row.PushPending != 2 || row.PushRunCount != 0 || row.PushClaimForce || row.PushClaimHolder != "" {
		t.Fatalf("restored push row %+v, want no claim, force pending, no run counted", row)
	}
	if _, claimed, _, _, err := s.TryBeginPush(ctx, repoID, "p2", time.Minute); err != nil || !claimed {
		t.Fatalf("push reclaim: %v %v", claimed, err)
	}
	if err := s.RequestPush(ctx, repoID, true); err != nil { // forced request during the run
		t.Fatal(err)
	}
	if row, err = s.RestorePush(ctx, repoID, "p2", false); err != nil || row.PushPending != 2 {
		t.Fatalf("restore under a forced request: %+v %v, want force kept", row, err)
	}
}
