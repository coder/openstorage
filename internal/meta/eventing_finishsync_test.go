// Regression tests for the FinishSync `AND syncing` claim guard (docs/14
// deferred #7): finishing without an in-flight claim must not touch the
// row's counters — in particular a stale finisher whose run was detached
// mid-flight and whose repo was then re-attached.

package meta

import (
	"errors"
	"testing"
)

func TestFinishSyncRequiresInFlightRun(t *testing.T) {
	runBackends(t, testFinishSyncRequiresInFlightRun)
}

func testFinishSyncRequiresInFlightRun(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, "")
	up := RepoUpstream{RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}
	if _, err := s.SetRepoUpstream(ctx, up); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	// Attached but idle (never claimed): FinishSync must refuse.
	if _, err := s.FinishSync(ctx, repoID, true); !errors.Is(err, ErrNotFound) {
		t.Errorf("finish without claim err = %v, want ErrNotFound", err)
	}
	got, err := s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatalf("GetRepoUpstream: %v", err)
	}
	if got.Syncing || got.RunCount != 0 || got.SuccessCount != 0 {
		t.Errorf("idle row after refused finish = %+v, want untouched zero counters", got)
	}

	// The stats-pollution race the guard exists for: claim, detach
	// mid-run, re-attach — the stale run's FinishSync must not count
	// against the fresh row.
	if _, claimed, err := s.TryBeginSync(ctx, repoID); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}
	if err := s.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("detach mid-run: %v", err)
	}
	if _, err := s.SetRepoUpstream(ctx, up); err != nil {
		t.Fatalf("re-attach: %v", err)
	}
	if _, err := s.FinishSync(ctx, repoID, true); !errors.Is(err, ErrNotFound) {
		t.Errorf("stale finish err = %v, want ErrNotFound", err)
	}
	got, err = s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatalf("GetRepoUpstream after re-attach: %v", err)
	}
	if got.Syncing || got.RunCount != 0 || got.SuccessCount != 0 {
		t.Errorf("re-attached row after stale finish = %+v, want zero counters", got)
	}
}
