// WithNowFunc pins the SQLite store's clock (the DST harness depends on
// this: sync-claim lease expiry must be a pure function of the injected
// logical clock, never wall time).

package meta

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteWithNowFunc(t *testing.T) {
	ctx := context.Background()
	clock := time.Unix(1700000000, 0).UTC()
	dbURL := "sqlite://" + filepath.Join(t.TempDir(), "meta.sqlite")
	s, err := OpenSQLite(ctx, dbURL, WithNowFunc(func() time.Time { return clock }))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	// SetRepoUpstream is guarded on a live repos row (issue #52).
	if _, err := s.UpsertOrgWithKeys(ctx, "clock-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	repo, err := s.CreateRepo(ctx, "clock-org", "https://example.com/clock.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}

	up, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repo.RepoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"})
	if err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	if !up.CreatedAt.Equal(clock) {
		t.Errorf("CreatedAt = %v, want the injected instant %v", up.CreatedAt, clock)
	}

	// Claim under the injected clock: expiry lands exactly at clock+lease.
	row, claimed, stolenFrom, err := s.TryBeginSync(ctx, repo.RepoID, "holder-a", 10*time.Second)
	if err != nil || !claimed || stolenFrom != "" {
		t.Fatalf("claim = (%v, %q, %v), want clean claim", claimed, stolenFrom, err)
	}
	if want := clock.Add(10 * time.Second); !row.SyncClaimExpiresAt.Equal(want) {
		t.Errorf("expiry = %v, want %v (injected clock + lease)", row.SyncClaimExpiresAt, want)
	}

	// One tick before expiry the claim is refused; wall time never enters.
	clock = clock.Add(9 * time.Second)
	if _, claimed, stolenFrom, err := s.TryBeginSync(ctx, repo.RepoID, "holder-b", 10*time.Second); err != nil || claimed || stolenFrom != "" {
		t.Fatalf("pre-expiry claim = (%v, %q, %v), want refused", claimed, stolenFrom, err)
	}

	// At expiry (expires <= now) the claim is stolen deterministically.
	clock = clock.Add(1 * time.Second)
	row, claimed, stolenFrom, err = s.TryBeginSync(ctx, repo.RepoID, "holder-b", 10*time.Second)
	if err != nil || !claimed || stolenFrom != "holder-a" {
		t.Fatalf("at-expiry claim = (%v, %q, %v), want steal from holder-a", claimed, stolenFrom, err)
	}
	if want := clock.Add(10 * time.Second); !row.SyncClaimExpiresAt.Equal(want) {
		t.Errorf("stolen-claim expiry = %v, want %v", row.SyncClaimExpiresAt, want)
	}

	// The dead holder's late finish still fails the holder guard.
	if _, err := s.FinishSync(ctx, repo.RepoID, "holder-a", true); !errors.Is(err, ErrClaimLost) {
		t.Errorf("late FinishSync err = %v, want ErrClaimLost", err)
	}
}
