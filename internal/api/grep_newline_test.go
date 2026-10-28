package api

// Issue #583: a pushed file whose name holds a newline (`git fsck --strict`
// accepts it) is a normal grep result, not a 500.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

func TestGrepPathWithNewline(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/grep-nl-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a\nb", "needle one\n")
		coreWriteFile(t, dir, "--\nz", "x\nneedle two\ny\n")
		coreWriteFile(t, dir, "c.txt", "needle three\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "newline paths")
		head := strings.TrimSpace(coreGit(t, dir, "rev-parse", "HEAD"))
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": head})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

		grep := func(t *testing.T, body map[string]any) grepResponse {
			t.Helper()
			resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos/grep", token, rqGrepBody(t, body), "")
			var out grepResponse
			rqDecode(t, resp, respBody, &out)
			return out
		}
		render := func(matches []grepFileMatch) string {
			var b strings.Builder
			for _, m := range matches {
				for _, l := range m.Lines {
					fmt.Fprintf(&b, "%q:%d:%s:%q ", m.Path, l.LineNumber, l.Type, l.Text)
				}
			}
			return b.String()
		}
		needle := map[string]any{"pattern": "needle"}
		all := `"--\nz":2:match:"needle two" "a\nb":1:match:"needle one" "c.txt":1:match:"needle three" `

		if got := render(grep(t, map[string]any{"query": needle}).Matches); got != all {
			t.Errorf("matches:\n got %s\nwant %s", got, all)
		}

		got := render(grep(t, map[string]any{"query": needle, "context": map[string]any{"before": 1, "after": 1}}).Matches)
		want := `"--\nz":1:context:"x" "--\nz":2:match:"needle two" "--\nz":3:context:"y" "a\nb":1:match:"needle one" "c.txt":1:match:"needle three" `
		if got != want {
			t.Errorf("context:\n got %s\nwant %s", got, want)
		}

		if got, want := render(grep(t, map[string]any{"query": needle, "paths": []string{"a\nb"}}).Matches), `"a\nb":1:match:"needle one" `; got != want {
			t.Errorf("paths [a\\nb]:\n got %s\nwant %s", got, want)
		}

		// One line per page: the cursor carries the newline path.
		paged, cursor := "", ""
		for page := 0; ; page++ {
			if page > 3 {
				t.Fatalf("more than 3 pages, so far %s", paged)
			}
			body := map[string]any{"query": needle, "pagination": map[string]any{"limit": 1, "cursor": cursor}}
			out := grep(t, body)
			paged += render(out.Matches)
			if !out.HasMore {
				break
			}
			cursor = out.NextCursor
		}
		if paged != all {
			t.Errorf("paged:\n got %s\nwant %s", paged, all)
		}
	})
}
