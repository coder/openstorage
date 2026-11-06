package meta

import (
	"context"
	"testing"
)

// TestPGRecordDefaultBranchChecked (issue #609): a worker with the adoption
// rule leaves the success count at which it handled a flagged repo. The
// evidence never moves down, and a row that is not flagged ignores the write.
func TestPGRecordDefaultBranchChecked(t *testing.T) {
	s, ok := pgBackend(t).(*PG)
	if !ok {
		t.Fatal("pgBackend is not a *PG")
	}
	ctx := t.Context()
	org := seedOrg(t, s)
	create := func(name string, omitted bool) string {
		t.Helper()
		repo, err := s.CreateLinkedRepo(ctx, org, name, "main",
			RepoUpstream{Provider: "github", Owner: "acme", Name: name, UpstreamHost: "github.com", DefaultBranchOmitted: omitted}, nil)
		if err != nil {
			t.Fatalf("CreateLinkedRepo %s: %v", name, err)
		}
		return repo.RepoID
	}
	evidence := func(repoID string) int {
		t.Helper()
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT default_branch_checked_success FROM repo_upstreams WHERE repo_id = $1`, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	flagged, plain := create("flagged", true), create("plain", false)
	for _, step := range []struct{ write, want int }{{3, 3}, {1, 3}, {5, 5}} {
		if err := s.RecordDefaultBranchChecked(ctx, flagged, step.write); err != nil {
			t.Fatal(err)
		}
		if got := evidence(flagged); got != step.want {
			t.Fatalf("after recording %d: evidence %d, want %d", step.write, got, step.want)
		}
	}
	if err := s.RecordDefaultBranchChecked(ctx, plain, 4); err != nil || evidence(plain) != 0 {
		t.Fatalf("an unflagged row: err %v, evidence %d; want no write", err, evidence(plain))
	}
	if err := s.RecordDefaultBranchChecked(ctx, "repo_000000000000", 1); err != nil {
		t.Fatalf("an unknown repo: %v; want no error", err)
	}
}

// TestPGDefaultBranchCheckedSuccessUpgrade: a database that predates the
// evidence column gains it at boot, its rows read 0, and a second boot is a
// no-op.
func TestPGDefaultBranchCheckedSuccessUpgrade(t *testing.T) {
	s, ok := pgBackend(t).(*PG)
	if !ok {
		t.Fatal("pgBackend is not a *PG")
	}
	ctx := t.Context()
	org := seedOrg(t, s)
	repo, err := s.CreateLinkedRepo(ctx, org, "old", "main",
		RepoUpstream{Provider: "github", Owner: "acme", Name: "old", UpstreamHost: "github.com", DefaultBranchOmitted: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rawExec(t, s, `ALTER TABLE repo_upstreams DROP COLUMN default_branch_checked_success`)
	t.Cleanup(func() { _ = s.InitSchema(context.Background()) })
	for range 2 {
		if err := s.InitSchema(ctx); err != nil {
			t.Fatalf("InitSchema: %v", err)
		}
	}
	var evidence int
	if err := s.pool.QueryRow(ctx, `SELECT default_branch_checked_success FROM repo_upstreams WHERE repo_id = $1`, repo.RepoID).Scan(&evidence); err != nil || evidence != 0 {
		t.Fatalf("upgraded row evidence = %d (%v), want 0", evidence, err)
	}
	var indexes int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE indexname = 'repo_upstreams_default_branch_omitted_idx'`).Scan(&indexes); err != nil || indexes != 1 {
		t.Fatalf("flagged-row index count = %d (%v), want 1", indexes, err)
	}
}
