package api

// Issue #590: blame original_path is the tree entry name, not git's
// C-quoted form. A name that is not valid UTF-8 reads with U+FFFD in JSON,
// like every other path field (#587).

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

func TestBlameOriginalPathIsTheTreeName(t *testing.T) {
	const oldName = "r\u00e9sum\u00e9 \"old\".txt" // renamed to renamed.txt
	cases := []struct{ name, want string }{
		{"plain.txt", "plain.txt"},
		{"caf\u00e9-utf8.txt", "caf\u00e9-utf8.txt"},
		{`q"t`, `q"t`},
		{`back\slash`, `back\slash`},
		{"ctl\t\n\x01\x7f.txt", "ctl\t\n\x01\x7f.txt"},
		{"a\xffb", "a\ufffdb"},
		{"caf\xe9.txt", "caf\ufffd.txt"},
	}
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/blame-path-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, oldName, "renamed line\n")
		for i, c := range cases {
			coreWriteFile(t, dir, c.name, fmt.Sprintf("line %d\n", i))
		}
		coreGit(t, dir, "add", "-A")
		coreGit(t, dir, "commit", "-q", "-m", "names")
		coreGit(t, dir, "mv", "--", oldName, "renamed.txt")
		coreGit(t, dir, "commit", "-q", "-m", "rename")
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": coreRevParse(t, dir, "HEAD")})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

		check := func(name, wantPath, wantOriginal string) {
			t.Helper()
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/blame?path="+url.QueryEscape(name), token, nil)
			coreRequireStatus(t, resp, body, 200)
			var out struct {
				Path  string `json:"path"`
				Lines []struct {
					OriginalPath string `json:"original_path"`
				} `json:"lines"`
			}
			coreDecode(t, body, &out)
			if out.Path != wantPath || len(out.Lines) != 1 || out.Lines[0].OriginalPath != wantOriginal {
				t.Errorf("blame %q: %s, want path %q and one line with original_path %q", name, body, wantPath, wantOriginal)
			}
		}
		for _, c := range cases {
			check(c.name, c.want, c.want)
		}
		check("renamed.txt", "renamed.txt", oldName)
	})
}
