package api

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// Any token with git:write can push refs/replace/<id>. The REST
// no-force-push checks read each commit as itself: a replacement that
// makes a root commit read as a child of the protected tip leaves the move
// a non-fast-forward, for branches/create with force and for a merge.
func TestRefPoliciesIgnoreReplacementObjects(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		ctx := context.Background()
		org := e.newOrg(t)
		url := "https://example.com/" + org + "/replace-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(ctx, org, url, "main")
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", "v1\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "c1")
		c1 := coreRevParse(t, dir, "HEAD")
		tree := coreRevParse(t, dir, "HEAD^{tree}")
		// root has no parent; child, a child of c1, is what the
		// replacement makes root read as.
		root := strings.TrimSpace(coreGit(t, dir, "commit-tree", tree, "-m", "root"))
		child := strings.TrimSpace(coreGit(t, dir, "commit-tree", tree, "-p", c1, "-m", "child"))
		coreGit(t, dir, "update-ref", "refs/heads/root", root)
		coreGit(t, dir, "update-ref", "refs/heads/child", child)
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{
			"refs/heads/main": c1, "refs/heads/guarded": c1, "refs/heads/merged": c1,
			"refs/heads/root": root, "refs/heads/child": child,
			"refs/replace/" + root: child,
		})
		tok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite},
			refs: `[["refs/heads/guarded",["no-force-push"]],["refs/heads/merged",["no-force-push"]],["*",[]]]`})

		resp, body := wrefDo(t, newWrefServer(t, e), "POST", "/api/v1/repos/branches/create", tok,
			map[string]any{"target_branch": "guarded", "base_ref": root, "force": true})
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("force move of guarded to root: status %d body %v, want 403", resp.StatusCode, body)
		}
		mresp, raw := wmDo(t, wmServer(t, e), "POST", "/api/v1/repos/merge", tok, wmMergeBody("root", "merged", "ff_only", nil))
		if mresp.StatusCode != http.StatusPreconditionFailed {
			t.Errorf("ff_only merge of root into merged: status %d body %.300s, want 412 (not a fast-forward)", mresp.StatusCode, raw)
		}
		refs := wrefRefs(t, e, repo.RepoID)
		if refs["refs/heads/guarded"] != c1 || refs["refs/heads/merged"] != c1 {
			t.Errorf("guarded = %s, merged = %s, want both at c1 %s", refs["refs/heads/guarded"], refs["refs/heads/merged"], c1)
		}
	})
}

// The write path's git reads every object as itself: a replacement changes
// neither the tree that a commit builds on nor the ancestry that the
// no-force-push check reads.
func TestWritePathGitIgnoresReplacements(t *testing.T) {
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	coreWriteFile(t, dir, "a.txt", "one\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c1")
	c1 := coreRevParse(t, dir, "HEAD")
	tree1 := coreRevParse(t, dir, "HEAD^{tree}")
	coreWriteFile(t, dir, "a.txt", "two\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c2")
	c2 := coreRevParse(t, dir, "HEAD")
	root := strings.TrimSpace(coreGit(t, dir, "commit-tree", tree1, "-m", "root"))
	coreGit(t, dir, "update-ref", "refs/replace/"+c1, c2)
	coreGit(t, dir, "update-ref", "refs/replace/"+root, c2)

	ctx := context.Background()
	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	if tree, err := sc.resolveTree(ctx, c1); err != nil || tree != tree1 {
		t.Errorf("resolveTree(c1) = %q, %v, want c1's own tree %q", tree, err, tree1)
	}
	if ff, err := wrefIsAncestor(ctx, dir, nil, c1, root); err != nil || ff {
		t.Errorf("wrefIsAncestor(c1, root) = %v, %v, want false: root has no parent", ff, err)
	}
}
