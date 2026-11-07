package api

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// Every read endpoint that takes a path answers 400 before git runs for a
// path outside the repository (git refuses it with exit 128) and for a
// NUL byte (exec refuses it). A path inside the repository keeps its
// answer, and "/" keeps main's: the root for a listing, no file for grep.
func TestReadPathsOutsideTheRepository(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/read-paths-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", "one\n")
		coreWriteFile(t, dir, "d/f.txt", "one in a folder\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "c1")
		main := coreRevParse(t, dir, "HEAD")
		coreGit(t, dir, "checkout", "-q", "-b", "b")
		coreWriteFile(t, dir, "d/f.txt", "one, changed\n")
		coreGit(t, dir, "commit", "-q", "-am", "c2")
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": main, "refs/heads/b": coreRevParse(t, dir, "HEAD")})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

		endpoints := []struct {
			name, prefix string // prefix + url-encoded path
		}{
			{"blame", "/api/v1/repos/blame?path="},
			{"file", "/api/v1/repos/file?path="},
			{"files", "/api/v1/repos/files?path="},
			{"files metadata", "/api/v1/repos/files/metadata?path="},
			{"commits", "/api/v1/repos/commits?path="},
			{"diff", "/api/v1/repos/diff?ref=b&path="},
			{"branches diff", "/api/v1/repos/branches/diff?branch=b&path="},
		}
		get := func(prefix, p string) (*http.Response, []byte) {
			t.Helper()
			return e.doReq(t, http.MethodGet, prefix+url.QueryEscape(p), token, nil, "")
		}
		grep := func(p string) (*http.Response, []byte) {
			t.Helper()
			return e.doReq(t, http.MethodPost, "/api/v1/repos/grep", token,
				rqGrepBody(t, map[string]any{"query": map[string]any{"pattern": "one"}, "paths": []string{p}}), "")
		}
		check := func(what string, want int, resp *http.Response, body []byte) {
			t.Helper()
			if resp.StatusCode != want {
				t.Errorf("%s: status = %d, want %d (body %.200s)", what, resp.StatusCode, want, body)
			}
		}

		for _, p := range []string{"..", "../", "../x", "/x", "//", "a/../../x", "\x00", "a\x00.txt"} {
			for _, ep := range endpoints {
				resp, body := get(ep.prefix, p)
				check(ep.name+" "+url.QueryEscape(p), http.StatusBadRequest, resp, body)
			}
			resp, body := grep(p)
			check("grep "+url.QueryEscape(p), http.StatusBadRequest, resp, body)
			if !bytes.Contains(body, []byte(`"paths.0"`)) {
				t.Errorf("grep %s: the problem does not name paths.0: %.300s", url.QueryEscape(p), body)
			}
		}

		// Paths inside the repository.
		for _, tc := range []struct {
			ep, path string
		}{
			{"blame", "a.txt"}, {"blame", "d/../a.txt"}, {"file", "a.txt"}, {"file", "d/f.txt"},
			{"files", "d"}, {"files", "d/"}, {"files", "/"}, {"files metadata", "d"},
			{"commits", "a.txt"}, {"commits", "d/"}, {"diff", "d/f.txt"}, {"branches diff", "d"},
		} {
			for _, ep := range endpoints {
				if ep.name == tc.ep {
					resp, body := get(ep.prefix, tc.path)
					check(ep.name+" "+tc.path, http.StatusOK, resp, body)
				}
			}
		}
		for _, p := range []string{"d", "d/", "/", "./d"} {
			resp, body := grep(p)
			check("grep "+p, http.StatusOK, resp, body)
		}
	})
}
