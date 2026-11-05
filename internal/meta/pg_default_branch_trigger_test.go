package meta

import (
	"context"
	"testing"
)

// TestPGDefaultBranchTrigger (issue #609, case 2): during a rolling upgrade
// a replica from before the adoption rule commits a plain UPDATE of
// repos.default_branch. Its PATCH to "main" changes no value, so adoption
// could not see it. The trigger clears the flag on any such write, so the
// caller's explicit "main" survives the first sync.
func TestPGDefaultBranchTrigger(t *testing.T) {
	s, ok := pgBackend(t).(*PG)
	if !ok {
		t.Fatal("pgBackend is not a *PG")
	}
	ctx := t.Context()
	org := seedOrg(t, s)
	flagged := func(name string) Repo {
		t.Helper()
		repo, err := s.CreateLinkedRepo(ctx, org, name, "main",
			RepoUpstream{Provider: "github", Owner: "acme", Name: name, UpstreamHost: "github.com", DefaultBranchOmitted: true}, nil)
		if err != nil {
			t.Fatalf("CreateLinkedRepo %s: %v", name, err)
		}
		return repo
	}
	omitted := func(repo Repo) bool {
		t.Helper()
		up, err := s.GetRepoUpstream(ctx, repo.RepoID)
		if err != nil {
			t.Fatalf("GetRepoUpstream %s: %v", repo.URL, err)
		}
		return up.DefaultBranchOmitted
	}
	// The statement an older binary's UpdateRepoDefaultBranch runs.
	oldPatch := func(repo Repo, to, from string) {
		t.Helper()
		if _, err := s.pool.Exec(ctx,
			`UPDATE repos SET default_branch = $1 WHERE org_id = $2 AND url = $3 AND deleted_at IS NULL AND default_branch = $4`,
			to, repo.OrgID, repo.URL, from); err != nil {
			t.Fatalf("old-binary PATCH of %s: %v", repo.URL, err)
		}
	}

	t.Run("a PATCH to main clears the flag, so adoption keeps main", func(t *testing.T) {
		repo := flagged("patched-main")
		oldPatch(repo, "main", "main")
		if omitted(repo) {
			t.Fatal("the flag survived a PATCH to main")
		}
		if adopted, err := s.AdoptUpstreamDefaultBranch(ctx, repo.RepoID, "trunk"); err != nil || adopted {
			t.Fatalf("adopt after the PATCH = %v, %v; want false", adopted, err)
		}
		if got, err := s.GetRepoByURL(ctx, org, repo.URL); err != nil || got.DefaultBranch != "main" {
			t.Fatalf("repo = %+v, %v; want the caller's main", got, err)
		}
	})

	t.Run("a PATCH to another branch clears the flag", func(t *testing.T) {
		repo := flagged("patched-dev")
		oldPatch(repo, "dev", "main")
		if omitted(repo) {
			t.Fatal("the flag survived a PATCH to dev")
		}
	})

	t.Run("a lost CAS and writes that skip the column keep the flag", func(t *testing.T) {
		repo, other := flagged("untouched"), flagged("neighbour")
		oldPatch(repo, "dev", "stale") // matches no row
		if _, err := s.pool.Exec(ctx, `UPDATE repos SET deleted_at = NULL WHERE repo_id = $1`, repo.RepoID); err != nil {
			t.Fatal(err)
		}
		if !omitted(repo) {
			t.Fatal("a write that does not name default_branch cleared the flag")
		}
		oldPatch(other, "main", "main")
		if !omitted(repo) {
			t.Fatal("a PATCH of one repository cleared another's flag")
		}
		if omitted(other) {
			t.Fatal("the neighbour's own PATCH did not clear its flag")
		}
	})

	t.Run("InitSchema installs it once and a later boot leaves it alone", func(t *testing.T) {
		oid := func() (n, id int64) {
			t.Helper()
			err := s.pool.QueryRow(ctx,
				`SELECT count(*), coalesce(min(oid::bigint), 0) FROM pg_trigger
				 WHERE tgname = 'repos_clear_default_branch_omitted' AND tgrelid = 'repos'::regclass`).Scan(&n, &id)
			if err != nil {
				t.Fatal(err)
			}
			return n, id
		}
		n, first := oid()
		if n != 1 {
			t.Fatalf("%d triggers named repos_clear_default_branch_omitted, want 1", n)
		}
		for range 2 {
			if err := s.InitSchema(ctx); err != nil {
				t.Fatalf("InitSchema: %v", err)
			}
		}
		if n, again := oid(); n != 1 || again != first {
			t.Fatalf("after two boots: %d triggers, oid %d, want one trigger with oid %d (a drop and create changes it)", n, again, first)
		}
	})

	t.Run("a boot installs it on a database that lacks it", func(t *testing.T) {
		repo := flagged("predates-trigger")
		dropTrigger(ctx, t, s)
		oldPatch(repo, "main", "main")
		if !omitted(repo) {
			t.Fatal("test setup: the flag cleared without the trigger")
		}
		if err := s.InitSchema(ctx); err != nil {
			t.Fatalf("InitSchema: %v", err)
		}
		oldPatch(repo, "main", "main")
		if omitted(repo) {
			t.Fatal("the flag survived a PATCH after the boot installed the trigger")
		}
	})
}

// dropTrigger removes the trigger the way a database from before this rule
// lacks it, and restores it when the test ends.
func dropTrigger(ctx context.Context, t *testing.T, s *PG) {
	t.Helper()
	if _, err := s.pool.Exec(ctx, `DROP TRIGGER repos_clear_default_branch_omitted ON repos`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	t.Cleanup(func() {
		if err := s.InitSchema(context.Background()); err != nil {
			t.Errorf("restore trigger: %v", err)
		}
	})
}
