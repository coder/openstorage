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
	repoID := testRepoID(t, s, "")
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

func TestSyncClaimLegacyAdoption(t *testing.T) { runBackends(t, testSyncClaimLegacyAdoption) }

// Mixed-fleet staging: a claim written by a pre-lease replica (empty
// holder, epoch expiry) may belong to a LIVE run — it must be adopted
// under a full grace, never stolen on sight; only a grace that lapses
// unfinished proves the holder dead.
func testSyncClaimLegacyAdoption(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	// Raw-SQL surgery reproducing the exact writes of pre-lease replicas
	// (their UPDATEs never touch the lease columns).
	exec := func(pgSQL, liteSQL string) {
		t.Helper()
		var err error
		switch b := s.(type) {
		case *PG:
			_, err = b.pool.Exec(ctx, pgSQL, repoID)
		case *SQLite:
			_, err = b.db.ExecContext(ctx, liteSQL, repoID)
		default:
			t.Fatalf("unexpected backend %T", s)
		}
		if err != nil {
			t.Fatalf("surgery: %v", err)
		}
	}
	legacyClaim := func() {
		exec(`UPDATE repo_upstreams SET syncing = true, sync_claim_holder = '', sync_claim_expires_at = 'epoch' WHERE repo_id = $1`,
			`UPDATE repo_upstreams SET syncing = 1, sync_claim_holder = '', sync_claim_expires_at = 0 WHERE repo_id = ?`)
	}
	legacyFinish := func() { // the old replica's FinishSync shape
		exec(`UPDATE repo_upstreams SET syncing = false, run_count = run_count + 1, success_count = success_count + 1 WHERE repo_id = $1 AND syncing`,
			`UPDATE repo_upstreams SET syncing = 0, run_count = run_count + 1, success_count = success_count + 1 WHERE repo_id = ? AND syncing = 1`)
	}
	lapseGrace := func() {
		exec(`UPDATE repo_upstreams SET sync_claim_expires_at = now() - interval '1 second' WHERE repo_id = $1`,
			`UPDATE repo_upstreams SET sync_claim_expires_at = 1 WHERE repo_id = ?`)
	}

	// A live old-version run: the contender ADOPTS, never steals.
	legacyClaim()
	up, claimed, stolenFrom, err := s.TryBeginSync(ctx, repoID, "holder-new", time.Minute)
	if err != nil || claimed || stolenFrom != "" {
		t.Fatalf("legacy contend = (%v, %q, %v), want adoption without claim or steal", claimed, stolenFrom, err)
	}
	if !up.Syncing || up.SyncClaimHolder != LegacyClaimHolder {
		t.Errorf("adopted row = %+v, want intact claim under %q", up, LegacyClaimHolder)
	}
	// Within the grace the adopted claim is simply refused (no re-stamp).
	if _, claimed, stolenFrom, err := s.TryBeginSync(ctx, repoID, "holder-new", time.Minute); err != nil || claimed || stolenFrom != "" {
		t.Fatalf("contend within grace = (%v, %q, %v), want plain refusal", claimed, stolenFrom, err)
	}
	// The live old replica finishes: the claim frees normally, no steal.
	legacyFinish()
	up, claimed, stolenFrom, err = s.TryBeginSync(ctx, repoID, "holder-new", time.Minute)
	if err != nil || !claimed || stolenFrom != "" {
		t.Fatalf("claim after legacy finish = (%v, %q, %v), want clean claim", claimed, stolenFrom, err)
	}
	if _, err := s.FinishSync(ctx, repoID, "holder-new", true); err != nil {
		t.Fatalf("FinishSync: %v", err)
	}

	// A crashed old-version run: adoption, lapse — and WITHOUT the
	// operator's drained-fleet assertion the claim stays unstealable (a
	// paused pre-lease binary has no holder guards, so no finite grace
	// proves it dead).
	legacyClaim()
	if _, claimed, _, err := s.TryBeginSync(ctx, repoID, "holder-new", time.Minute); err != nil || claimed {
		t.Fatalf("adopt crashed legacy = (%v, %v), want adoption", claimed, err)
	}
	lapseGrace()
	up, claimed, stolenFrom, err = s.TryBeginSync(ctx, repoID, "holder-new", time.Minute)
	if err != nil || claimed || stolenFrom != "" {
		t.Fatalf("lapsed-grace steal without opt-in = (%v, %q, %v), want refusal", claimed, stolenFrom, err)
	}
	if up.SyncClaimHolder != LegacyClaimHolder {
		t.Errorf("row without opt-in = %+v, want the adopted claim intact", up)
	}

	// With EnvSyncStealLegacy (fleet drained of pre-lease binaries), the
	// lapsed legacy claim is stolen and syncing heals.
	t.Setenv(EnvSyncStealLegacy, "true")
	up, claimed, stolenFrom, err = s.TryBeginSync(ctx, repoID, "holder-new", time.Minute)
	if err != nil || !claimed || stolenFrom != LegacyClaimHolder {
		t.Fatalf("opted-in steal after lapsed grace = (%v, %q, %v), want steal from %q", claimed, stolenFrom, err, LegacyClaimHolder)
	}
	if up.SyncClaimHolder != "holder-new" {
		t.Errorf("stolen row = %+v, want the new holder's claim", up)
	}
}
