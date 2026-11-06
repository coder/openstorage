package meta

import (
	"fmt"
	"slices"
	"testing"
)

// TestPGDefaultBranchReconcileCandidates (issue #609, case 1): a flagged,
// live repository whose success count passed the evidence of a worker with
// the adoption rule is the shape that an older worker's successful first
// sync leaves. Every other shape stays out, so the reconcile job asks for
// one sync per such run and then stops.
func TestPGDefaultBranchReconcileCandidates(t *testing.T) {
	s, ok := pgBackend(t).(*PG)
	if !ok {
		t.Fatal("pgBackend is not a *PG")
	}
	ctx := t.Context()
	org := seedOrg(t, s)
	repoOf := map[string]string{}
	add := func(name string, omitted bool, success int, set string) string {
		t.Helper()
		repo, err := s.CreateLinkedRepo(ctx, org, name, "main",
			RepoUpstream{Provider: "github", Owner: "acme", Name: name, UpstreamHost: "github.com", DefaultBranchOmitted: omitted}, nil)
		if err != nil {
			t.Fatalf("CreateLinkedRepo %s: %v", name, err)
		}
		rawExec(t, s, fmt.Sprintf(`UPDATE repo_upstreams SET success_count = %d%s WHERE repo_id = '%s'`, success, set, repo.RepoID))
		repoOf[name] = repo.RepoID
		return repo.RepoID
	}
	oldWorker := add("old-worker", true, 1, "")
	add("evidence-equal", true, 2, ", default_branch_checked_success = 2")
	add("never-synced", true, 0, "")
	add("unflagged", false, 3, "")
	add("syncing", true, 1, ", syncing = true")
	add("pending", true, 1, ", sync_pending = true")
	add("deleted", true, 1, "")
	if _, err := s.SoftDeleteRepo(ctx, org, "deleted"); err != nil {
		t.Fatal(err)
	}
	second := add("old-worker-two", true, 4, ", default_branch_checked_success = 1")

	list := func(limit int) []DefaultBranchReconcileRepo {
		t.Helper()
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		all, err := PGDefaultBranchReconcileCandidates(ctx, tx, limit)
		if err != nil {
			t.Fatal(err)
		}
		var mine []DefaultBranchReconcileRepo
		for _, c := range all {
			if c.Org == org {
				mine = append(mine, c)
			}
		}
		return mine
	}
	ids := func(cs []DefaultBranchReconcileRepo) []string {
		var out []string
		for _, c := range cs {
			out = append(out, c.RepoID)
		}
		slices.Sort(out)
		return out
	}
	want := []string{oldWorker, second}
	slices.Sort(want)
	if got := ids(list(100)); !slices.Equal(got, want) {
		t.Fatalf("candidates = %v, want the two old-worker shapes %v", got, want)
	}
	if got := list(1); len(got) != 1 {
		t.Fatalf("limit 1 returned %d candidates", len(got))
	}

	// A worker with the rule records its success count: the repo leaves the
	// set until a later success raises the count, and the evidence never
	// moves down. A row that is no longer flagged ignores the write.
	if err := s.RecordDefaultBranchChecked(ctx, oldWorker, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordDefaultBranchChecked(ctx, second, 2); err != nil { // below its success_count 4
		t.Fatal(err)
	}
	if err := s.RecordDefaultBranchChecked(ctx, second, 1); err != nil { // lower than the stored 2
		t.Fatal(err)
	}
	if got := ids(list(100)); !slices.Equal(got, []string{second}) {
		t.Fatalf("after evidence = %v, want only the repo whose count is still ahead", got)
	}
	rawExec(t, s, `UPDATE repo_upstreams SET success_count = 2 WHERE repo_id = '`+oldWorker+`'`) // an older worker's next success
	if got := ids(list(100)); !slices.Equal(got, want) {
		t.Fatalf("after another success = %v, want %v", got, want)
	}
	if err := s.RecordDefaultBranchChecked(ctx, repoOf["unflagged"], 3); err != nil {
		t.Fatal(err)
	}
	var evidence int
	if err := s.pool.QueryRow(ctx, `SELECT default_branch_checked_success FROM repo_upstreams WHERE repo_id = $1`, repoOf["unflagged"]).Scan(&evidence); err != nil || evidence != 0 {
		t.Fatalf("an unflagged row kept evidence %d (%v), want 0", evidence, err)
	}
}
