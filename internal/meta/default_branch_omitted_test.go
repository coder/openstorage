package meta

import (
	"context"
	"errors"
	"net/url"
	"testing"
)

func TestAdoptUpstreamDefaultBranch(t *testing.T) {
	runBackends(t, testAdoptUpstreamDefaultBranch)
	t.Run("s3wal", func(t *testing.T) { testAdoptUpstreamDefaultBranch(t, s3walBackend(t)) })
}

// testAdoptUpstreamDefaultBranch: a GitHub sync create without a default
// branch stores DefaultBranchOmitted (issue #599). Adopting the upstream
// HEAD sets the branch and clears the flag in one write, exactly once; an
// explicit default-branch change clears the flag first, so it wins.
func testAdoptUpstreamDefaultBranch(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	create := func(name string, omitted bool) Repo {
		t.Helper()
		up := RepoUpstream{Provider: "github", Owner: "acme", Name: name, UpstreamHost: "github.com", DefaultBranchOmitted: omitted}
		repo, err := s.CreateLinkedRepo(ctx, org, name, "main", up, nil)
		if err != nil {
			t.Fatalf("CreateLinkedRepo %s: %v", name, err)
		}
		if got, err := s.GetRepoUpstream(ctx, repo.RepoID); err != nil || got.DefaultBranchOmitted != omitted {
			t.Fatalf("%s upstream = %+v, %v; want DefaultBranchOmitted %v", name, got, err, omitted)
		}
		return repo
	}
	expect := func(step string, repo Repo, wantBranch string, wantOmitted bool) {
		t.Helper()
		got, err := s.GetRepoByURL(ctx, org, repo.URL)
		if err != nil || got.DefaultBranch != wantBranch {
			t.Fatalf("%s: repo = %+v, %v; want branch %q", step, got, err, wantBranch)
		}
		up, err := s.GetRepoUpstream(ctx, repo.RepoID)
		if err != nil || up.DefaultBranchOmitted != wantOmitted {
			t.Fatalf("%s: upstream = %+v, %v; want DefaultBranchOmitted %v", step, up, err, wantOmitted)
		}
	}
	adopt := func(step string, repo Repo, branch string, want bool) {
		t.Helper()
		if got, err := s.AdoptUpstreamDefaultBranch(ctx, repo.RepoID, branch); err != nil || got != want {
			t.Fatalf("%s: adopt %q = %v, %v; want %v", step, branch, got, err, want)
		}
	}

	omitted := create("omitted", true)
	adopt("first adopt", omitted, "trunk", true)
	expect("first adopt", omitted, "trunk", false)
	// A later upstream HEAD change never moves the adopted branch.
	adopt("second adopt", omitted, "next", false)
	expect("second adopt", omitted, "trunk", false)

	explicit := create("explicit", false)
	adopt("unflagged adopt", explicit, "trunk", false)
	expect("unflagged adopt", explicit, "main", false)

	patched := create("patched", true)
	if won, err := s.UpdateRepoDefaultBranch(ctx, org, patched.URL, "main", "dev"); err != nil || !won {
		t.Fatalf("PATCH = %v, %v; want won", won, err)
	}
	expect("after PATCH", patched, "dev", false)
	adopt("adopt after PATCH", patched, "trunk", false)
	expect("adopt after PATCH", patched, "dev", false)
	// A lost CAS changes nothing, the flag included.
	lost := create("lost", true)
	if won, err := s.UpdateRepoDefaultBranch(ctx, org, lost.URL, "stale", "dev"); err != nil || won {
		t.Fatalf("stale PATCH = %v, %v; want lost", won, err)
	}
	expect("stale PATCH", lost, "main", true)

	// An older binary's PATCH (rolling upgrade) changes the branch without
	// clearing the flag: adopt then only clears the flag.
	oldPatch := create("old-patch", true)
	oldBinaryPatch(t, s, oldPatch.RepoID, "dev")
	adopt("adopt after an old-binary PATCH", oldPatch, "trunk", false)
	expect("adopt after an old-binary PATCH", oldPatch, "dev", false)

	detached := create("detached", true)
	if err := s.DeleteRepoUpstream(ctx, detached.RepoID); err != nil {
		t.Fatal(err)
	}
	adopt("detached adopt", detached, "trunk", false)
	if got, err := s.GetRepoByURL(ctx, org, detached.URL); err != nil || got.DefaultBranch != "main" {
		t.Fatalf("detached repo = %+v, %v; want main", got, err)
	}

	deleted := create("deleted", true)
	if _, err := s.SoftDeleteRepo(ctx, org, deleted.URL); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdoptUpstreamDefaultBranch(ctx, deleted.RepoID, "trunk"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("adopt on a deleted repo err = %v, want ErrNotFound", err)
	}
}

// TestDefaultBranchOmittedUpgrade: a database that predates the column
// gains it on InitSchema, its upstream rows read unflagged, and a second
// InitSchema is a no-op.
func TestDefaultBranchOmittedUpgrade(t *testing.T) {
	t.Run("pg", func(t *testing.T) {
		pgBackend(t) // boots the shared container and sets pgTestURL
		ctx := t.Context()
		if _, err := pgStore.pool.Exec(ctx, `CREATE DATABASE default_branch_omitted_upgrade`); err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(pgTestURL)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/default_branch_omitted_upgrade"
		s, err := OpenPG(ctx, u.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		upgradeDefaultBranchOmitted(t, s, s.InitSchema, func(q string) error {
			_, err := s.pool.Exec(ctx, q)
			return err
		})
	})
	t.Run("sqlite", func(t *testing.T) {
		s, _ := openTestSQLite(t)
		upgradeDefaultBranchOmitted(t, s, s.InitSchema, func(q string) error {
			_, err := s.db.ExecContext(t.Context(), q)
			return err
		})
	})
}

func upgradeDefaultBranchOmitted(t *testing.T, s Backend, initSchema func(context.Context) error, exec func(string) error) {
	ctx := t.Context()
	if err := initSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	org := seedOrg(t, s)
	repo, err := s.CreateRepo(ctx, org, "old", "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := exec(`ALTER TABLE repo_upstreams DROP COLUMN default_branch_omitted`); err != nil {
		t.Fatalf("drop default_branch_omitted: %v", err)
	}
	// An upstream row written by an old binary, under the old schema.
	now := map[bool]string{true: "now()", false: "0"}[isPG(s)]
	if err := exec(`INSERT INTO repo_upstreams (repo_id, provider, owner, name, upstream_host, run_count, success_count, syncing, created_at)
		VALUES ('` + repo.RepoID + `', 'github', 'acme', 'demo', 'github.com', 0, 0, false, ` + now + `)`); err != nil {
		t.Fatalf("old-schema insert: %v", err)
	}
	for range 2 { // the second run checks idempotency
		if err := initSchema(ctx); err != nil {
			t.Fatalf("InitSchema upgrade: %v", err)
		}
	}
	if up, err := s.GetRepoUpstream(ctx, repo.RepoID); err != nil || up.DefaultBranchOmitted {
		t.Fatalf("upgraded upstream = %+v, %v; want unflagged", up, err)
	}
	if adopted, err := s.AdoptUpstreamDefaultBranch(ctx, repo.RepoID, "trunk"); err != nil || adopted {
		t.Fatalf("adopt on an upgraded row = %v, %v; want false", adopted, err)
	}
}

// oldBinaryPatch changes a repo's default branch the way a binary from
// before the adoption rule does: the branch moves and the flag stays.
func oldBinaryPatch(t *testing.T, s Backend, repoID, branch string) {
	t.Helper()
	w, ok := s.(*S3WAL)
	if !ok {
		rawExec(t, s, `UPDATE repos SET default_branch = '`+branch+`' WHERE repo_id = '`+repoID+`'`)
		return
	}
	err := w.mutate(t.Context(), s3walMetaMutation{
		op: "old-binary PATCH",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			doc.Repos[repoID].DefaultBranch = branch
			return true, nil
		},
	})
	if err != nil {
		t.Fatalf("old-binary PATCH: %v", err)
	}
}

// rawExec runs one SQL statement on the backend's own handle, to write
// what an older binary would.
func rawExec(t *testing.T, s Backend, q string) {
	t.Helper()
	var err error
	switch b := s.(type) {
	case *PG:
		_, err = b.pool.Exec(t.Context(), q)
	case *SQLite:
		_, err = b.db.ExecContext(t.Context(), q)
	default:
		t.Fatalf("rawExec: backend %T", s)
	}
	if err != nil {
		t.Fatalf("rawExec %q: %v", q, err)
	}
}
