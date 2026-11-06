package sync

import (
	"path/filepath"
	"testing"

	"github.com/coder/openstorage/internal/meta"
)

// TestLocalScheduleAdoptsDefaultBranch (issue #603): on both local metadata
// backends (SQLite, and S3WAL under OPENSTORAGE_TEST_BACKEND=s3wal) a GitHub
// sync create without a default branch takes the upstream HEAD at its first
// successful sync and clears its flag. A create that named a branch keeps it.
func TestLocalScheduleAdoptsDefaultBranch(t *testing.T) {
	f := newLocalSchedFixture(t)
	up := newHTTPUpstream(t)
	seedUpstream(t, up.root, "own", "adopt")
	bare := filepath.Join(up.root, "own", "adopt.git")
	testGit(t, bare, "branch", "trunk", "main")
	testGit(t, bare, "symbolic-ref", "HEAD", "refs/heads/trunk")

	create := func(name string, omitted bool) string {
		t.Helper()
		if name != "adopt" {
			seedUpstream(t, up.root, "own", name)
		}
		repo, err := f.ms.CreateRepo(t.Context(), f.org, "https://example.com/"+name+".git", "main")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
			RepoID: repo.RepoID, Provider: "github", Owner: "own", Name: name, UpstreamHost: up.host(), DefaultBranchOmitted: omitted,
		}); err != nil {
			t.Fatal(err)
		}
		f.wf.addLoopbackRepo(t, repo.RepoID)
		if err := f.gate.RequestSync(t.Context(), repo.RepoID); err != nil {
			t.Fatal(err)
		}
		return repo.RepoID
	}
	omitted, explicit := create("adopt", true), create("explicit", false)
	f.sweep(t)

	for _, tc := range []struct {
		name, id, wantBranch string
	}{{"adopt", omitted, "trunk"}, {"explicit", explicit, "main"}} {
		row := f.row(t, tc.id)
		repo, err := f.ms.GetRepoByURL(t.Context(), f.org, "https://example.com/"+tc.name+".git")
		if err != nil || repo.DefaultBranch != tc.wantBranch || row.DefaultBranchOmitted || row.SuccessCount != 1 {
			t.Fatalf("%s: repo %+v (%v), upstream %+v; want branch %q, a clear flag and one successful sync", tc.name, repo, err, row, tc.wantBranch)
		}
	}
}
