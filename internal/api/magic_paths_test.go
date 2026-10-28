package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestPathsNamedLikePathspecMagic pins #577: a path whose name is git
// pathspec magic is an ordinary path. Before the fix, ls-tree rejected it
// (500 on files, files/metadata, diff and merge preview) and log matched
// nothing.
func TestPathsNamedLikePathspecMagic(t *testing.T) {
	const dir, file = ":(attr:x)p", ":(attr:x)p/f"
	stream := strings.Join([]string{
		"commit refs/heads/main", "mark :1", "committer D <d@example.com> 1700000000 +0000", "data 4", "root",
		"M 100644 inline " + file, "data 2", "0", "",
		"commit refs/heads/main", "mark :2", "committer D <d@example.com> 1700000001 +0000", "data 4", "edit", "from :1",
		"M 100644 inline " + file, "data 2", "1", "",
		"commit refs/heads/x", "committer D <d@example.com> 1700000002 +0000", "data 2", "x", "from :2",
		"M 100644 inline " + file, "data 2", "x", "",
		"commit refs/heads/y", "committer D <d@example.com> 1700000003 +0000", "data 2", "y", "from :2",
		"M 100644 inline " + file, "data 2", "y", "",
	}, "\n")

	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/magic-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		work := t.TempDir()
		coreGit(t, work, "init", "-q", "-b", "main", ".")
		cmd := exec.Command("git", "fast-import", "--quiet")
		cmd.Dir = work
		cmd.Stdin = strings.NewReader(stream)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fast-import: %v\n%s", err, out)
		}
		refs := map[string]string{}
		for _, b := range []string{"main", "x", "y"} {
			refs["refs/heads/"+b] = coreRevParse(t, work, b)
		}
		coreSeedStores(t, e, repo.RepoID, work, refs)
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
		root, tip := coreRevParse(t, work, "main~1"), refs["refs/heads/main"]

		get := func(t *testing.T, path string, out any) {
			t.Helper()
			resp, body := coreGet(t, e, "GET", path, token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			if err := json.Unmarshal(body, out); err != nil {
				t.Fatalf("decode %s: %v\n%s", path, err, body)
			}
		}
		q := url.QueryEscape

		for _, p := range []string{dir, dir + "/", file} {
			for _, route := range []string{"files", "files/metadata"} {
				if l := getFilesListing(t, e, "/api/v1/repos/"+route+"?path="+q(p), token); !slices.Equal(l.paths, []string{file}) {
					t.Errorf("%s?path=%s lists %v, want [%s]", route, p, l.paths, file)
				}
			}
		}

		var commits listCommitsResponse
		get(t, "/api/v1/repos/commits?path="+q(file), &commits)
		var shas []string
		for _, c := range commits.Commits {
			shas = append(shas, c.SHA)
		}
		if want := []string{tip, root}; !slices.Equal(shas, want) {
			t.Errorf("commits?path=%s = %v, want %v", file, shas, want)
		}

		// No path filter: the diff stats each changed file with ls-tree.
		var diff struct {
			Files []struct {
				Path string `json:"path"`
			} `json:"files"`
		}
		get(t, "/api/v1/repos/diff?ref="+tip+"&base_ref="+root, &diff)
		if len(diff.Files) != 1 || diff.Files[0].Path != file {
			t.Errorf("diff files = %+v, want [%s]", diff.Files, file)
		}

		var preview wmPreviewResponse
		get(t, "/api/v1/repos/merge/preview?source_branch=x&target_branch=y&include_content=true", &preview)
		if preview.Status != "conflicted" || !slices.Equal(preview.ConflictPaths, []string{file}) || len(preview.Conflicts) != 1 {
			t.Errorf("merge preview = %+v, want one conflict on %s", preview, file)
		}
	})
}
