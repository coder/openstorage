package api

// #587: merge conflict paths that are not valid UTF-8 carry
// conflict_paths_base64 and path_base64 siblings.

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// rpCommitTree commits exactly the given files, with parent as its parent
// when set, without touching HEAD or a work tree.
func rpCommitTree(t *testing.T, dir, parent string, files map[string]string, msg string) string {
	t.Helper()
	coreGit(t, dir, "read-tree", "--empty")
	for name, content := range files {
		rpStage(t, dir, name, content)
	}
	args := []string{"commit-tree", strings.TrimSpace(coreGit(t, dir, "write-tree")), "-m", msg}
	if parent != "" {
		args = append(args, "-p", parent)
	}
	return strings.TrimSpace(coreGit(t, dir, args...))
}

func rpBase64List(t *testing.T, where string, doc map[string]any, key string) []string {
	t.Helper()
	list, ok := doc[key].([]any)
	if !ok {
		t.Fatalf("%s: %q is %T in %v", where, key, doc[key], doc)
	}
	out := make([]string, len(list))
	for i, v := range list {
		raw, err := base64.StdEncoding.DecodeString(v.(string))
		if err != nil {
			t.Fatalf("%s: %s[%d] %q: %v", where, key, i, v, err)
		}
		out[i] = string(raw)
	}
	return out
}

func TestRawPathMergeConflicts(t *testing.T) { runBackends(t, testRawPathMergeConflicts) }

func testRawPathMergeConflicts(t *testing.T, e *testEnv) {
	srv := wmServer(t, e)
	org := e.newOrg(t)
	repoURL := "https://example.com/" + org + "/rawpath-merge-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	const bigName = "big\xfa.txt"
	names := []string{rpFF, rpFE, rpPlain, bigName}
	side := func(content string) map[string]string {
		files := map[string]string{}
		for _, n := range names {
			files[n] = content
		}
		return files
	}
	base := rpCommitTree(t, dir, "", side("base\n"), "base")
	mainTip := rpCommitTree(t, dir, base, side("main\n"), "main")
	bigSide := side("feature\n")
	bigSide[bigName] = strings.Repeat("x", (1<<20)+64) + "\n" // above the inline conflict limit
	featureTip := rpCommitTree(t, dir, base, bigSide, "feature")
	plainTip := rpCommitTree(t, dir, base, map[string]string{rpFF: "base\n", rpFE: "base\n", rpPlain: "plain side\n", bigName: "base\n"}, "plain only")
	for ref, sha := range map[string]string{"main": mainTip, "feature": featureTip, "plainonly": plainTip} {
		coreGit(t, dir, "update-ref", "refs/heads/"+ref, sha)
	}
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": mainTip, "refs/heads/feature": featureTip, "refs/heads/plainonly": plainTip})
	token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})

	wantRaw := rpSorted(rpFF, rpFE, rpPlain, bigName)
	checkPaths := func(t *testing.T, where string, doc map[string]any) {
		t.Helper()
		paths, _ := doc["conflict_paths"].([]any)
		raws := rpBase64List(t, where, doc, "conflict_paths_base64")
		if len(paths) != len(raws) || !slices.Equal(rpSorted(raws...), wantRaw) {
			t.Fatalf("%s: conflict_paths %v with conflict_paths_base64 %q, want one entry per path holding %q", where, paths, raws, wantRaw)
		}
		for i, p := range paths {
			if p != string([]rune(raws[i])) {
				t.Errorf("%s: conflict_paths[%d] = %q, want %q", where, i, p, string([]rune(raws[i])))
			}
		}
	}

	t.Run("merge 409", func(t *testing.T) {
		resp, raw := wmDo(t, srv, http.MethodPost, "/api/v1/repos/merge", token,
			wmMergeBody("feature", "main", "merge", map[string]any{"author": wmTestAuthor}))
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
		}
		checkPaths(t, "merge 409", wmDecode(t, raw))
	})

	t.Run("preview without content", func(t *testing.T) {
		resp, raw := wmDo(t, srv, http.MethodGet, "/api/v1/repos/merge/preview?source_branch=feature&target_branch=main", token, nil)
		coreRequireStatus(t, resp, raw, http.StatusOK)
		checkPaths(t, "preview", wmDecode(t, raw))
	})

	t.Run("preview with content", func(t *testing.T) {
		resp, raw := wmDo(t, srv, http.MethodGet, "/api/v1/repos/merge/preview?source_branch=feature&target_branch=main&include_content=true", token, nil)
		coreRequireStatus(t, resp, raw, http.StatusOK)
		doc := wmDecode(t, raw)
		checkPaths(t, "preview", doc)
		conflicts := rpObjects(t, doc, "conflicts")
		if got := rpRawSet(t, "conflicts", conflicts, "path"); !slices.Equal(rpSorted(got...), rpSorted(rpFF, rpFE, rpPlain)) {
			t.Errorf("conflicts = %q, want %q", got, rpSorted(rpFF, rpFE, rpPlain))
		}
		for _, c := range conflicts {
			// The content comes from the raw path, so it is the main side.
			if ours := c["ours"].(map[string]any)["content"]; ours != "main\n" {
				t.Errorf("conflict %v: ours content = %q, want main", c["path"], ours)
			}
		}
		filtered := rpObjects(t, doc, "filtered_conflicts")
		if got := rpRawSet(t, "filtered_conflicts", filtered, "path"); !slices.Equal(got, []string{bigName}) {
			t.Errorf("filtered_conflicts = %q, want %q", got, []string{bigName})
		}
	})

	t.Run("valid names only add nothing", func(t *testing.T) {
		// Only plain.txt conflicts: the other three paths merge cleanly.
		for _, route := range []struct{ method, path string }{
			{http.MethodPost, "/api/v1/repos/merge"},
			{http.MethodGet, "/api/v1/repos/merge/preview?include_content=true&source_branch=" + url.QueryEscape("plainonly") + "&target_branch=main"},
		} {
			var body any
			if route.method == http.MethodPost {
				body = wmMergeBody("plainonly", "main", "merge", map[string]any{"author": wmTestAuthor})
			}
			resp, raw := wmDo(t, srv, route.method, route.path, token, body)
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
				t.Fatalf("%s: status = %d, body %s", route.path, resp.StatusCode, raw)
			}
			if strings.Contains(string(raw), "_base64") {
				t.Errorf("%s: a conflict on a valid path answers a _base64 key: %s", route.path, raw)
			}
			if !strings.Contains(string(raw), `"plain.txt"`) {
				t.Errorf("%s: no conflict on plain.txt: %s", route.path, raw)
			}
		}
	})
}
