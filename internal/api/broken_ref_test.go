package api

import (
	"context"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/ref"
)

// A tag whose commit is in no pack is an existing ref that git cannot
// read: each endpoint that resolves a ref answers 500 for it, and for the
// tag's object id, not 404. A ref that does not exist is still 404.
func TestReadEndpointsAnswer500ForARefGitCannotRead(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		ctx := context.Background()
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/broken-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(ctx, org, repoURL, "main")
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", "one\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "c1")
		c1 := coreRevParse(t, dir, "HEAD")
		c2 := strings.TrimSpace(coreGit(t, dir, "commit-tree", coreRevParse(t, dir, "HEAD^{tree}"), "-p", c1, "-m", "c2"))
		coreGit(t, dir, "tag", "-a", "-m", "gone", "gone-tag", c2)
		tagObj := coreRevParse(t, dir, "refs/tags/gone-tag")
		coreGit(t, dir, "tag", "-d", "gone-tag")
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": c1})

		// A pack with the tag object alone: its commit is in no pack.
		packDir := t.TempDir()
		pk := exec.Command("git", "pack-objects", filepath.Join(packDir, "pack"))
		pk.Dir = dir
		pk.Stdin = strings.NewReader(tagObj + "\n")
		out, err := pk.Output()
		if err != nil {
			t.Fatal(err)
		}
		id := strings.TrimSpace(string(out))
		key := blob.PackKey(repo.RepoID, id)
		if err := e.blobStore.UploadFile(ctx, key, filepath.Join(packDir, "pack-"+id+".pack")); err != nil {
			t.Fatal(err)
		}
		if err := e.refStore.RegisterPacks(ctx, repo.RepoID, []ref.Pack{{ID: id, Key: key}}); err != nil {
			t.Fatal(err)
		}
		if won, err := e.refStore.CASRef(ctx, repo.RepoID, "refs/tags/gone-tag", ref.ZeroSHA, tagObj); err != nil || !won {
			t.Fatalf("CASRef(refs/tags/gone-tag) = %v, %v", won, err)
		}

		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
		for _, tc := range []struct {
			path string
			want int
		}{
			{"/api/v1/repos/commits?ref=gone-tag", http.StatusInternalServerError},
			{"/api/v1/repos/commits?ref=" + tagObj, http.StatusInternalServerError},
			{"/api/v1/repos/commit?ref=gone-tag", http.StatusInternalServerError},
			{"/api/v1/repos/files?ref=gone-tag", http.StatusInternalServerError},
			{"/api/v1/repos/file?ref=gone-tag&path=a.txt", http.StatusInternalServerError},
			{"/api/v1/repos/blame?ref=gone-tag&path=a.txt", http.StatusInternalServerError},
			{"/api/v1/repos/diff?ref=gone-tag", http.StatusInternalServerError},
			{"/api/v1/repos/commits?ref=main", http.StatusOK},
			{"/api/v1/repos/commits?ref=nope", http.StatusNotFound},
		} {
			resp, body := e.doReq(t, http.MethodGet, tc.path, token, nil, "")
			if resp.StatusCode != tc.want {
				t.Errorf("GET %s = %d, want %d: %.200s", tc.path, resp.StatusCode, tc.want, body)
			}
		}
	})
}
