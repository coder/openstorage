package api

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// A type change (a file that becomes a symlink or a submodule, and back)
// is one entry with state T, and the diff answers 200: git's --patch
// splits it into a deletion and an addition.
func TestDiffTypeChange(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/typechange-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "typefile", "one\n")
		coreWriteFile(t, dir, "filetosub", "two\n")
		coreWriteFile(t, dir, "keep", "x\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "files")
		files := coreRevParse(t, dir, "HEAD")
		if err := os.Remove(filepath.Join(dir, "typefile")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("keep", filepath.Join(dir, "typefile")); err != nil {
			t.Fatal(err)
		}
		coreGit(t, dir, "rm", "-q", "--cached", "filetosub")
		if err := os.Remove(filepath.Join(dir, "filetosub")); err != nil {
			t.Fatal(err)
		}
		coreGit(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+files+",filetosub")
		coreGit(t, dir, "add", "typefile")
		coreGit(t, dir, "commit", "-q", "-m", "a symlink and a submodule")
		other := coreRevParse(t, dir, "HEAD")
		// The files again, on top: the diff reads base...ref.
		back := strings.TrimSpace(coreGit(t, dir, "commit-tree", "-p", other, "-m", "files again", coreRevParse(t, dir, files+"^{tree}")))
		coreGit(t, dir, "update-ref", "refs/heads/main", back)
		coreGit(t, dir, "update-ref", "refs/heads/files", files)
		coreGit(t, dir, "update-ref", "refs/heads/other", other)
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": back, "refs/heads/files": files, "refs/heads/other": other})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
		for _, tc := range []struct {
			route string
			want  map[string]string // path -> "state bytes is_eof"
		}{
			{"/api/v1/repos/diff?ref=" + other + "&base_ref=" + files, map[string]string{"typefile": "T 4 false", "filetosub": "T 0 false"}},
			{"/api/v1/repos/diff?ref=" + back + "&base_ref=" + other, map[string]string{"typefile": "T 4 true", "filetosub": "T 4 true"}},
			{"/api/v1/repos/branches/diff?branch=other&base=files", map[string]string{"typefile": "T 4 false", "filetosub": "T 0 false"}},
			{"/api/v1/repos/branches/diff?branch=main&base=other", map[string]string{"typefile": "T 4 true", "filetosub": "T 4 true"}},
		} {
			resp, body := e.doReq(t, http.MethodGet, tc.route, token, nil, "")
			if resp.StatusCode != http.StatusOK {
				t.Errorf("%s: status = %d, want 200 (body %.300s)", tc.route, resp.StatusCode, body)
				continue
			}
			var diff struct {
				Files []struct {
					Path  string `json:"path"`
					State string `json:"state"`
					Bytes int64  `json:"bytes"`
					IsEOF bool   `json:"is_eof"`
				} `json:"files"`
			}
			if err := json.Unmarshal(body, &diff); err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for _, f := range diff.Files {
				got[f.Path] = fmt.Sprintf("%s %d %v", f.State, f.Bytes, f.IsEOF)
			}
			if !maps.Equal(got, tc.want) {
				t.Errorf("%s: files %v, want %v", tc.route, got, tc.want)
			}
		}
	})
}
