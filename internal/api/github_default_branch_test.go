package api

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	upsync "github.com/coder/openstorage/internal/sync"
)

// TestSyncAdoptsGitHubDefaultBranch: a GitHub sync create without a
// default branch takes the GitHub repository's default branch at its first
// successful sync, and GET shows it by the time repo.sync.succeeded
// arrives (issue #599). The upstream is a fake GitHub (smart HTTP) whose
// HEAD names trunk.
func TestSyncAdoptsGitHubDefaultBranch(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newPendingSyncStack(t, e, true)
		upRoot := t.TempDir()
		forge := usyncCGIServer(t, upRoot, func(*http.Request) bool { return true }, false)
		seed := func(name, head string) string {
			usyncSeedUpstream(t, upRoot, "octo", name)
			bare := filepath.Join(upRoot, "octo", name+".git")
			usyncGit(t, bare, "branch", "trunk", "main")
			usyncGit(t, bare, "symbolic-ref", "HEAD", head)
			return bare
		}
		hello := seed("hello", "refs/heads/trunk")
		github := func(name, extra string) string {
			return `{` + extra + `"upstream":{"provider":"github","owner":"octo","name":"` + name + `","access":"public","host":"` + forge.URL + `"}}`
		}
		// sync creates (or pulls) and returns GET's default branch read
		// as soon as the run's repo.sync.succeeded event arrives.
		sync := func(step, repoName, path, body string) string {
			t.Helper()
			tok := s.token(t, repoName, "", auth.ScopeRepoWrite, auth.ScopeGitWrite)
			if code, _, msg := s.do(t, path, tok, body); code != http.StatusCreated && code != http.StatusAccepted {
				t.Fatalf("%s: %s = %d %q", step, path, code, msg)
			}
			for {
				select {
				case ev := <-s.events:
					if ev.Repository.URL != repoName || ev.Type == upsync.EventSyncStarted {
						continue
					}
					if ev.Type != upsync.EventSyncSucceeded {
						t.Fatalf("%s: sync event %+v, want succeeded", step, ev)
					}
					resp, raw := e.doReq(t, http.MethodGet, "/api/v1/repo",
						e.mint(t, tokenSpec{iss: s.org, repo: repoName, scopes: []string{auth.ScopeGitRead}}), nil, "")
					var entry repoEntry
					if resp.StatusCode != http.StatusOK || json.Unmarshal(raw, &entry) != nil {
						t.Fatalf("%s: GET = %d %s", step, resp.StatusCode, raw)
					}
					return entry.DefaultBranch
				case <-time.After(60 * time.Second):
					t.Fatalf("%s: no terminal sync event", step)
				}
			}
		}
		omitted := func(step, repoName string) bool {
			t.Helper()
			r, err := e.store.GetRepoByURL(context.Background(), s.org, repoName)
			if err != nil {
				t.Fatal(err)
			}
			up, err := s.q.es.GetRepoUpstream(context.Background(), r.RepoID)
			if err != nil {
				t.Fatalf("%s: %v", step, err)
			}
			return up.DefaultBranchOmitted
		}

		if got := sync("omitted", "adopt", "/api/v1/repos", github("hello", "")); got != "trunk" || omitted("omitted", "adopt") {
			t.Fatalf("omitted default branch: GET = %q (flag %v), want trunk with the flag cleared", got, omitted("", "adopt"))
		}
		if got := sync("explicit main", "explicit", "/api/v1/repos", github("hello", `"default_branch":"main",`)); got != "main" {
			t.Fatalf("explicit main: GET = %q, want main", got)
		}
		// A later change of the GitHub default branch never moves ours.
		usyncGit(t, hello, "branch", "next", "main")
		usyncGit(t, hello, "symbolic-ref", "HEAD", "refs/heads/next")
		if got := sync("remote HEAD moved", "adopt", "/api/v1/repos/pull-upstream", "{}"); got != "trunk" {
			t.Fatalf("after the upstream HEAD moved: GET = %q, want trunk", got)
		}
		// Other hosts keep main.
		gitea := `{"base_repo":{"provider":"gitea","owner":"octo","name":"hello","upstream_host":"` + forge.URL + `"}}`
		if got := sync("gitea", "gitea", "/api/v1/repos", gitea); got != "main" {
			t.Fatalf("gitea: GET = %q, want main", got)
		}

		// An unresolvable HEAD keeps main and the flag; a later sync retries.
		unborn := seed("unborn", "refs/heads/missing")
		if got := sync("unborn HEAD", "retry", "/api/v1/repos", github("unborn", "")); got != "main" || !omitted("unborn HEAD", "retry") {
			t.Fatalf("unborn upstream HEAD: GET = %q, want main with the flag kept", got)
		}
		usyncGit(t, unborn, "symbolic-ref", "HEAD", "refs/heads/trunk")
		if got := sync("retry", "retry", "/api/v1/repos/pull-upstream", "{}"); got != "trunk" || omitted("retry", "retry") {
			t.Fatalf("retry: GET = %q, want trunk with the flag cleared", got)
		}
	})
}

// TestValidBranchNameMatchesAPI pins the sync worker's copy of the branch
// rule to the API's: an adopted branch must pass the create check.
func TestValidBranchNameMatchesAPI(t *testing.T) {
	for _, name := range []string{
		"main", "trunk", "release/1.0", "a", "a.b", "a_b-c", "", ".x", "-x", "x.", "x.lock", "a..b",
		"a//b", "/a", "a/", "a b", "a~b", "a^b", "a:b", "a\\b", "a@{b", "é", string(make([]byte, 256)),
		"refs/heads/main", "HEAD",
		strings.Repeat("a", 250), strings.Repeat("a", 251), "x/" + strings.Repeat("a", 251), strings.Repeat("a", 252) + "/b",
	} {
		if got, want := upsync.ValidBranchName(name), validNewBranchName(name); got != want {
			t.Errorf("ValidBranchName(%q) = %v, API validNewBranchName = %v", name, got, want)
		}
	}
}
