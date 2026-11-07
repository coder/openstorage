package api

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// A git failure that is the server's (gitFailsOnArg's "unable to read")
// answers 500, and git's own "no" or the client's bad input keeps its
// 4xx. No response echoes git's stderr.
func TestGitFailureStatus(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/git-status-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", "one\ntwo\nthree\n")
		coreWriteFile(t, dir, "no such path x", "named like git's message\n")
		coreWriteFile(t, dir, "d/f.txt", "in a folder\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "c")
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": coreRevParse(t, dir, "HEAD")})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

		blame := func(path string, ranges ...string) (*http.Response, []byte) {
			t.Helper()
			q := url.Values{"path": {path}}
			for _, r := range ranges {
				q.Add("range", r)
			}
			return e.doReq(t, http.MethodGet, "/api/v1/repos/blame?"+q.Encode(), token, nil, "")
		}
		grep := func(pattern string) (*http.Response, []byte) {
			t.Helper()
			return e.doReq(t, http.MethodPost, "/api/v1/repos/grep", token,
				rqGrepBody(t, map[string]any{"query": map[string]any{"pattern": pattern}}), "")
		}
		check := func(t *testing.T, what string, want int, resp *http.Response, body []byte) {
			t.Helper()
			if resp.StatusCode != want {
				t.Errorf("%s: status = %d, want %d (body %.200s)", what, resp.StatusCode, want, body)
			}
			if bytes.Contains(body, []byte("unable to read")) {
				t.Errorf("%s: the response echoes git's stderr: %.200s", what, body)
			}
		}

		// The real git's answers.
		for _, tc := range []struct {
			name   string
			path   string
			ranges []string
			want   int
		}{
			{"blame", "a.txt", nil, http.StatusOK},
			{"blame of a missing path", "missing.txt", nil, http.StatusNotFound},
			{"blame of a path named like git's message", "no such path x", nil, http.StatusOK},
			{"range past the end", "a.txt", []string{"5,6"}, http.StatusBadRequest},
			{"range git cannot parse", "a.txt", []string{"x"}, http.StatusBadRequest},
			{"range with no match", "a.txt", []string{"/zzz/"}, http.StatusBadRequest},
			// git's message names the path: "file no such path x has only 1 line".
			{"range past the end of a path named like git's message", "no such path x", []string{"5,6"}, http.StatusBadRequest},
			// git's messages name the path as git rewrote it: "./x",
			// "a//b" and a control byte read as "x", "a/b" and "?".
			{"blame of a missing ./path", "./missing.txt", nil, http.StatusNotFound},
			{"blame of a missing path with a double slash", "no//such", nil, http.StatusNotFound},
			{"blame of a missing path with a control byte", "mi\x01ss", nil, http.StatusNotFound},
			{"blame of ./a.txt", "./a.txt", nil, http.StatusOK},
			{"range past the end of ./a.txt", "./a.txt", []string{"5,6"}, http.StatusBadRequest},
			// blame needs a file: a folder, with or without its slash, is
			// not found (d/ lists d/f.txt, d lists the folder).
			{"blame of a folder", "d", nil, http.StatusNotFound},
			{"blame of a folder with its slash", "d/", nil, http.StatusNotFound},
			{"a NUL in the path", "a\x00.txt", nil, http.StatusBadRequest},
		} {
			resp, body := blame(tc.path, tc.ranges...)
			check(t, tc.name, tc.want, resp, body)
		}
		resp, body := grep("a\\{1")
		check(t, "grep with a pattern git cannot compile", http.StatusBadRequest, resp, body)

		// A server-side failure of the same git calls.
		t.Run("server failures", func(t *testing.T) {
			gitFailsOnArg(t, "blame", "128")
			resp, body := blame("a.txt")
			check(t, "blame without a range", http.StatusInternalServerError, resp, body)
			resp, body = blame("no such path x")
			check(t, "blame of a path named like git's message", http.StatusInternalServerError, resp, body)
			resp, body = blame("a.txt", "1,2")
			check(t, "blame with a range", http.StatusInternalServerError, resp, body)
			resp, body = blame("./a.txt")
			check(t, "blame of ./a.txt", http.StatusInternalServerError, resp, body)
			resp, body = blame("d/f.txt")
			check(t, "blame of a file in a folder", http.StatusInternalServerError, resp, body)
		})
		// Only "fatal: file <path> has only N lines" is the range's
		// fault, not every message that starts like it.
		t.Run("server failure that names the file", func(t *testing.T) {
			gitFailsOnArgWith(t, "blame", "128", "fatal: file a.txt has a bad object")
			resp, body := blame("a.txt", "1,2")
			check(t, "blame with a range", http.StatusInternalServerError, resp, body)
		})
		t.Run("grep read failure", func(t *testing.T) {
			gitFailsOnArg(t, "grep", "128")
			resp, body := grep("one")
			check(t, "grep", http.StatusInternalServerError, resp, body)
		})
	})
}

// hash-object takes any bytes, so its failure is the server's git: 500,
// with no stderr in the response.
func TestCommitPackHashFailureIs500(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, _ := wctRepo(t, e)
		for _, how := range []string{"128", "kill"} {
			t.Run("git "+how, func(t *testing.T) {
				gitFailsOnArg(t, "hash-object", how)
				body := wctLines(t,
					wctMeta("main", "m", []map[string]any{{"path": "f.txt", "content_id": "id-f", "operation": "upsert"}}, nil),
					wctChunk("id-f", []byte(strings.Repeat("f", 1<<10)), true))
				resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
				if resp.StatusCode != http.StatusInternalServerError {
					t.Errorf("status = %d, want 500 (body %.200s)", resp.StatusCode, respBody)
				}
				if bytes.Contains(respBody, []byte("unable to read")) || bytes.Contains(respBody, []byte("hash-object")) {
					t.Errorf("the response echoes git's error: %.200s", respBody)
				}
			})
		}
	})
}
