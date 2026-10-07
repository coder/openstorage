package api

// Issue #359: POST /repos accepts upstream's top-level `upstream`
// (UpstreamConfig) as the Git Sync form, the base_repo copy form
// ({name, ref?, auth.token}) as a one-time copy, and GET /repo(s) expose
// `upstream` (UpstreamRepo) next to the deprecated base_repo.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	neturl "net/url"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

// Property names of upstream's Repository and UpstreamRepo schemas
// (docs/upstream/openapi.yaml, 2026-09-28 19:47 GMT snapshot). Both set
// additionalProperties: false.
var (
	upstreamRepositoryProps   = map[string]bool{"base_repo": true, "created_at": true, "default_branch": true, "repo_id": true, "repo_name": true, "upstream": true, "url": true}
	upstreamRepositoryReq     = []string{"repo_id", "repo_name", "url", "default_branch", "created_at"}
	upstreamUpstreamRepoProps = map[string]bool{"name": true, "owner": true, "provider": true}
)

// assertRepositoryShape fails unless entry has only Repository schema keys,
// every required key, and UpstreamRepo-shaped upstream/base_repo objects.
func assertRepositoryShape(t *testing.T, entry json.RawMessage) {
	t.Helper()
	var got map[string]json.RawMessage
	if err := json.Unmarshal(entry, &got); err != nil {
		t.Fatal(err)
	}
	for k := range got {
		if !upstreamRepositoryProps[k] {
			t.Errorf("repository entry carries %q, which upstream's Repository schema forbids: %s", k, entry)
		}
	}
	for _, k := range upstreamRepositoryReq {
		if _, ok := got[k]; !ok {
			t.Errorf("repository entry lacks required %q: %s", k, entry)
		}
	}
	for _, k := range []string{"upstream", "base_repo"} {
		raw, ok := got[k]
		if !ok {
			continue
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("%s is not an object: %s", k, raw)
		}
		for p := range obj {
			if !upstreamUpstreamRepoProps[p] {
				t.Errorf("%s carries %q, which upstream's UpstreamRepo schema forbids: %s", k, p, raw)
			}
		}
		if _, ok := obj["provider"]; !ok {
			t.Errorf("%s lacks required provider: %s", k, raw)
		}
	}
}

// TestCreateRepoUpstreamSync: `upstream` {provider: github, access: public}
// creates a sync-linked repo whose first sync mirrors the upstream. host
// points at a local smart-HTTP forge (an http:// base URL, accepted like
// base_repo.upstream_host).
func TestCreateRepoUpstreamSync(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		upRoot := t.TempDir()
		forge := usyncCGIServer(t, upRoot, func(*http.Request) bool { return true }, false)
		headSHA := usyncSeedUpstream(t, upRoot, "octo", "hello")

		repoURL := "team/upstream-" + randomHex(t, 4)
		body, err := json.Marshal(map[string]any{
			"upstream": map[string]any{
				"provider": "github", "owner": "octo", "name": "hello",
				"access": "public", "host": forge.URL,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		repoID := s.createRepo(t, repoURL, body)

		started := s.waitEvent(t, upsync.EventSyncStarted)
		if started.Repository.ID != repoID || !started.IsFirstSync {
			t.Fatalf("bad started event: %+v", started)
		}
		if done := s.waitEvent(t, upsync.EventSyncSucceeded); done.Error != "" || done.RunCount != 1 {
			t.Fatalf("bad succeeded event: %+v", done)
		}
		if got := s.loopbackSHA(t, repoID, "refs/heads/main"); got != headSHA {
			t.Fatalf("loopback main = %q, want %q", got, headSHA)
		}
		up, err := s.mem.GetUpstream(context.Background(), repoID)
		if err != nil {
			t.Fatalf("upstream row: %v", err)
		}
		if up.Provider != "github" || up.Owner != "octo" || up.Name != "hello" || up.UpstreamHost != forge.URL || up.ForwardOnPush {
			t.Fatalf("upstream row = %+v", up)
		}
		// access=public stores no credential.
		if _, err := s.mem.CredentialForRepo(context.Background(), repoID); !errors.Is(err, upsync.ErrNotFound) {
			t.Fatalf("credential for public upstream: err %v, want ErrNotFound", err)
		}
	})
}

// TestCreateRepoUpstreamValidation: every unsupported or malformed
// UpstreamConfig is a 400 that creates nothing (never a repo without sync).
func TestCreateRepoUpstreamValidation(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		cases := []struct {
			name, body, wantMsg string
		}{
			{"both forms", `{"upstream":{"provider":"github","owner":"o","name":"n","access":"public"},"base_repo":{"provider":"github","owner":"o","name":"n"}}`, "base_repo and upstream cannot both be set"},
			{"unknown provider", `{"upstream":{"provider":"svn","owner":"o","name":"n"}}`, `unsupported upstream provider "svn"`},
			{"code.storage provider", `{"upstream":{"provider":"code.storage","owner":"o","name":"n"}}`, `unsupported upstream provider "code.storage"`},
			{"missing owner", `{"upstream":{"provider":"gitlab","name":"n"}}`, "upstream provider, owner, and name are required"},
			{"github default access", `{"upstream":{"provider":"github","owner":"o","name":"n"}}`, upstreamAppAccessMsg},
			{"github app access", `{"upstream":{"provider":"github","owner":"o","name":"n","access":"app"}}`, upstreamAppAccessMsg},
			{"bad access", `{"upstream":{"provider":"github","owner":"o","name":"n","access":"private"}}`, `invalid upstream access "private"`},
			{"access on gitlab", `{"upstream":{"provider":"gitlab","owner":"o","name":"n","access":"public"}}`, "upstream access applies to provider github only"},
			{"gitea needs host", `{"upstream":{"provider":"gitea","owner":"o","name":"n"}}`, `upstream host is required for provider "gitea"`},
			{"file host", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"file:///srv/git"}}`, `invalid upstream host "file:///srv/git"`},
			{"metadata host", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"169.254.169.254"}}`, "upstream host denied by sync egress policy: egress target 169.254.169.254 denied: cloud metadata address"},
			{"unknown field", `{"upstream":{"provider":"gitlab","owner":"o","name":"n","branch":"main"}}`, `invalid upstream: json: unknown field "branch"`},
			{"not an object", `{"upstream":"github"}`, "invalid upstream: json: cannot unmarshal string into Go value of type api.upstreamConfig"},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				repoURL := "team/invalid-" + randomHex(t, 4)
				token := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
				resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos", token, []byte(c.body))
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status %d: %s", resp.StatusCode, respBody)
				}
				if msg := requireEnvelope(t, resp, respBody); msg != c.wantMsg {
					t.Fatalf("error = %q, want %q", msg, c.wantMsg)
				}
				if _, err := e.store.GetRepoByURL(context.Background(), s.org, repoURL); !errors.Is(err, meta.ErrNotFound) {
					t.Fatalf("repo %q exists after rejected create (err %v)", repoURL, err)
				}
			})
		}
		s.assertNoEvent(t)
	})
}

// TestCreateRepoUpstreamShownOnGet runs through the full api.New stack
// (fork dispatcher included): a repo created with `upstream` reports it on
// GET /repo, the named-repo alias, and GET /repos, with base_repo kept as
// the deprecated mirror. Both forms together are rejected even when
// base_repo is a fork/copy the dispatcher would otherwise take.
func TestCreateRepoUpstreamShownOnGet(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "team/linked-" + randomHex(t, 4)
		body := []byte(`{"default_branch":"trunk","upstream":{"provider":"gitea","owner":"own","name":"proj","host":"forge.example.invalid"}}`)
		created := createRepo(t, e, org, repoURL, body, http.StatusCreated)
		want := baseRepoEntry{Provider: "gitea", Owner: "own", Name: "proj"}

		readTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
		for _, path := range []string{"/api/v1/repo", "/api/repos/" + neturl.PathEscape(repoURL)} {
			resp, got := e.doReq(t, http.MethodGet, path, readTok, nil, "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s = %d %s", path, resp.StatusCode, got)
			}
			assertRepositoryShape(t, got)
			var entry repoEntry
			if err := json.Unmarshal(got, &entry); err != nil {
				t.Fatal(err)
			}
			if entry.Upstream == nil || *entry.Upstream != want || entry.BaseRepo == nil || *entry.BaseRepo != want {
				t.Fatalf("GET %s = %s, want upstream and base_repo %+v", path, got, want)
			}
			if entry.RepoID != created.RepoID || entry.DefaultBranch != "trunk" {
				t.Fatalf("GET %s = %s", path, got)
			}
		}

		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
		resp, got := e.doReq(t, http.MethodGet, "/api/v1/repos", orgTok, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /repos = %d %s", resp.StatusCode, got)
		}
		var list struct {
			Repos []json.RawMessage `json:"repos"`
		}
		if err := json.Unmarshal(got, &list); err != nil || len(list.Repos) != 1 {
			t.Fatalf("GET /repos = %s (err %v)", got, err)
		}
		assertRepositoryShape(t, list.Repos[0])
		var entry repoEntry
		if err := json.Unmarshal(list.Repos[0], &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Upstream == nil || *entry.Upstream != want {
			t.Fatalf("GET /repos entry = %s, want upstream %+v", list.Repos[0], want)
		}

		for _, base := range []string{
			`{"provider":"code","operation":"fork","auth":{"token":"x"}}`,
			`{"name":"team/src","auth":{"token":"x"}}`,
		} {
			other := "team/both-" + randomHex(t, 4)
			tok := e.mint(t, tokenSpec{iss: org, repo: other, scopes: []string{auth.ScopeRepoWrite}})
			resp, got := e.doReq(t, http.MethodPost, "/api/v1/repos", tok,
				[]byte(`{"base_repo":`+base+`,"upstream":{"provider":"gitlab","owner":"o","name":"n"}}`), "")
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("both forms (%s) = %d %s", base, resp.StatusCode, got)
			}
			if msg := requireEnvelope(t, resp, got); msg != "base_repo and upstream cannot both be set" {
				t.Fatalf("both forms (%s) error = %q", base, msg)
			}
			if _, err := e.store.GetRepoByURL(context.Background(), org, other); !errors.Is(err, meta.ErrNotFound) {
				t.Fatalf("repo %q exists after rejected create (err %v)", other, err)
			}
		}
	})
}

// TestCreateRepoBaseRepoCopy: base_repo without provider is upstream's
// one-time copy ({name, ref?, auth.token}); it runs the fork machinery and
// leaves no upstream link. name must identify the repository the token
// grants.
func TestCreateRepoBaseRepoCopy(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newForkServer(t, e)
		src := wrefSeedFixture(t, e)
		srcReadTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeGitRead}})

		t.Run("copy at default branch", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, map[string]any{
				"base_repo": map[string]any{"name": src.url, "auth": map[string]any{"token": srcReadTok}},
			})
			copyID := forkRequireCreated(t, resp, body)
			if refs := wrefRefs(t, e, copyID); len(refs) != 1 || refs["refs/heads/main"] != src.c2 {
				t.Fatalf("copy refs = %v, want exactly refs/heads/main=%s", refs, src.c2)
			}
			if _, err := e.store.GetRepoUpstream(context.Background(), copyID); !errors.Is(err, meta.ErrNotFound) {
				t.Fatalf("copy has an upstream link (err %v)", err)
			}
		})

		t.Run("copy at ref", func(t *testing.T) {
			caller := newForkCaller(t, e, "")
			resp, body := forkDo(t, srv, caller.token, map[string]any{
				"base_repo": map[string]any{"name": src.url, "ref": "feature", "auth": map[string]any{"token": srcReadTok}},
			})
			copyID := forkRequireCreated(t, resp, body)
			if refs := wrefRefs(t, e, copyID); refs["refs/heads/main"] != src.c1 {
				t.Fatalf("copy refs = %v, want refs/heads/main=%s", refs, src.c1)
			}
		})

		t.Run("name is not the token's repository", func(t *testing.T) {
			sibling := "team/sibling-" + randomHex(t, 4)
			if _, err := e.store.CreateRepo(context.Background(), src.org, sibling, "main"); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{sibling, src.url + "-missing"} {
				caller := newForkCaller(t, e, "")
				resp, body := forkDo(t, srv, caller.token, map[string]any{
					"base_repo": map[string]any{"name": name, "auth": map[string]any{"token": srcReadTok}},
				})
				if resp.StatusCode != http.StatusForbidden || body["error"] != copyNameMismatchMsg {
					t.Fatalf("name %q: status %d body %v, want 403 %q", name, resp.StatusCode, body, copyNameMismatchMsg)
				}
				forkRequireNoRepoRow(t, e, caller.org, caller.url)
			}
		})

		// base_repo has additionalProperties: false; a typo must not
		// copy the wrong history, and sync-only fields mean nothing to a
		// copy.
		t.Run("unknown or sync-only fields", func(t *testing.T) {
			tok := map[string]any{"token": srcReadTok}
			for _, c := range []struct {
				base    map[string]any
				wantMsg string
			}{
				{map[string]any{"name": src.url, "reff": "feature", "auth": tok}, `invalid base_repo: json: unknown field "reff"`},
				{map[string]any{"name": src.url, "auth": map[string]any{"token": srcReadTok, "user": "x"}}, `invalid base_repo: json: unknown field "user"`},
				{map[string]any{"name": src.url, "owner": "o", "auth": tok}, copySyncFieldsMsg},
				{map[string]any{"name": src.url, "upstream_host": "github.com", "auth": tok}, copySyncFieldsMsg},
				{map[string]any{"name": src.url, "forward_pushes": true, "auth": tok}, copySyncFieldsMsg},
				{map[string]any{"name": src.url, "auth": map[string]any{"token": srcReadTok, "auth_type": "jwt"}}, copySyncFieldsMsg},
			} {
				caller := newForkCaller(t, e, "")
				resp, body := forkDo(t, srv, caller.token, map[string]any{"base_repo": c.base})
				if resp.StatusCode != http.StatusBadRequest || body["error"] != c.wantMsg {
					t.Fatalf("%v: status %d body %v, want 400 %q", c.base, resp.StatusCode, body, c.wantMsg)
				}
				forkRequireNoRepoRow(t, e, caller.org, caller.url)
			}
		})

		t.Run("missing name or token", func(t *testing.T) {
			for _, c := range []struct {
				base    map[string]any
				wantMsg string
			}{
				{map[string]any{"auth": map[string]any{"token": srcReadTok}}, "base_repo.name is required for a copy"},
				{map[string]any{"name": src.url}, "base_repo.auth.token is required for a copy"},
			} {
				caller := newForkCaller(t, e, "")
				resp, body := forkDo(t, srv, caller.token, map[string]any{"base_repo": c.base})
				if resp.StatusCode != http.StatusBadRequest || body["error"] != c.wantMsg {
					t.Fatalf("%v: status %d body %v, want 400 %q", c.base, resp.StatusCode, body, c.wantMsg)
				}
				forkRequireNoRepoRow(t, e, caller.org, caller.url)
			}
		})
	})
}
