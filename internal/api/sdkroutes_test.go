package api

// Route coverage for the stable upstream SDK (issue #656): every operation
// that github.com/pierrecomputer/sdk/go v0.1.0 calls must reach a handler,
// never unmatchedAPIRoute's 404/405, plus the behavior of the three routes
// that #656 added (GET/POST /api/repos and DELETE .../tags/{tag_name}).

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// sdk010Routes lists all 39 operations of the Go SDK at tag go/v0.1.0, from
// the `// X calls <id>: METHOD /path` comments in go/*_gen.go. The npm and
// PyPI 0.1.0 SDKs are generated from the same spec. repos.delete is last
// because the authenticated pass below deletes the fixture repo with it.
var sdk010Routes = []struct{ id, method, path string }{
	{"meta.get", http.MethodGet, "/api/meta"},
	{"repos.archive.get", http.MethodPost, "/api/repos/{repo_name}/archive"},
	{"repos.blame.get", http.MethodGet, "/api/repos/{repo_name}/blame"},
	{"repos.branches.create", http.MethodPost, "/api/repos/{repo_name}/branches/create"},
	{"repos.branches.delete", http.MethodDelete, "/api/repos/{repo_name}/branches"},
	{"repos.branches.get", http.MethodGet, "/api/repos/{repo_name}/branch"},
	{"repos.branches.list", http.MethodGet, "/api/repos/{repo_name}/branches"},
	{"repos.commits.create", http.MethodPost, "/api/repos/{repo_name}/commit-pack"},
	{"repos.commits.createFromDiff", http.MethodPost, "/api/repos/{repo_name}/diff-commit"},
	{"repos.commits.get", http.MethodGet, "/api/repos/{repo_name}/commit"},
	{"repos.commits.list", http.MethodGet, "/api/repos/{repo_name}/commits"},
	{"repos.commits.restore", http.MethodPost, "/api/repos/{repo_name}/restore-commit"},
	{"repos.create", http.MethodPost, "/api/repos"},
	{"repos.diff.get", http.MethodGet, "/api/repos/{repo_name}/diff"},
	{"repos.files.get", http.MethodGet, "/api/repos/{repo_name}/file"},
	{"repos.files.list", http.MethodGet, "/api/repos/{repo_name}/files"},
	{"repos.files.metadata.get", http.MethodHead, "/api/repos/{repo_name}/file"},
	{"repos.files.metadata.list", http.MethodGet, "/api/repos/{repo_name}/files/metadata"},
	{"repos.get", http.MethodGet, "/api/repos/{repo_name}"},
	{"repos.gitCredentials.create", http.MethodPost, "/api/repos/{repo_name}/git-credentials"},
	{"repos.gitCredentials.delete", http.MethodDelete, "/api/repos/{repo_name}/git-credentials/{git_credential_id}"},
	{"repos.gitCredentials.update", http.MethodPut, "/api/repos/{repo_name}/git-credentials/{git_credential_id}"},
	{"repos.grep", http.MethodPost, "/api/repos/{repo_name}/grep"},
	{"repos.list", http.MethodGet, "/api/repos"},
	{"repos.merge", http.MethodPost, "/api/repos/{repo_name}/merge"},
	{"repos.notes.append", http.MethodPost, "/api/repos/{repo_name}/notes/append"},
	{"repos.notes.create", http.MethodPost, "/api/repos/{repo_name}/notes"},
	{"repos.notes.delete", http.MethodDelete, "/api/repos/{repo_name}/notes"},
	{"repos.notes.get", http.MethodGet, "/api/repos/{repo_name}/notes"},
	{"repos.notes.refs.list", http.MethodGet, "/api/repos/{repo_name}/notes/refs"},
	{"repos.previewMerge", http.MethodGet, "/api/repos/{repo_name}/merge/preview"},
	{"repos.tags.create", http.MethodPost, "/api/repos/{repo_name}/tags"},
	{"repos.tags.delete", http.MethodDelete, "/api/repos/{repo_name}/tags/{tag_name}"},
	{"repos.tags.get", http.MethodGet, "/api/repos/{repo_name}/tag"},
	{"repos.tags.list", http.MethodGet, "/api/repos/{repo_name}/tags"},
	{"repos.update", http.MethodPatch, "/api/repos/{repo_name}"},
	{"repos.upstream.delete", http.MethodDelete, "/api/repos/{repo_name}/upstream"},
	{"repos.upstream.pull", http.MethodPost, "/api/repos/{repo_name}/upstream/pull"},
	{"repos.delete", http.MethodDelete, "/api/repos/{repo_name}"},
}

// muxFallback reports whether a response came from unmatchedAPIRoute (no
// pattern matched the path, or none matched the method) rather than from a
// registered route: those answer route_not_found or method_not_allowed,
// which no route sends.
func muxFallback(resp *http.Response, body []byte) bool {
	var p struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(body, &p)
	return resp.StatusCode == http.StatusNotFound && p.Code == codeRouteNotFound ||
		resp.StatusCode == http.StatusMethodNotAllowed && p.Code == genericProblemCode(http.StatusMethodNotAllowed)
}

func TestSDK010RoutesServed(t *testing.T) {
	if len(sdk010Routes) != 39 {
		t.Fatalf("sdk010Routes has %d operations, SDK 0.1.0 has 39", len(sdk010Routes))
	}
	e := testEnvironment(t, "sqlite")
	org := e.newOrg(t)
	fx := coreSeedFixture(t, e, org)
	repoTok := e.mint(t, tokenSpec{iss: org, repo: fx.repo.URL,
		scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite, auth.ScopeRepoWrite}})
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})

	// Control: an unregistered path and an unregistered method get
	// unmatchedAPIRoute's answers, which muxFallback must recognize.
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/api/repos/" + url.PathEscape(fx.repo.URL) + "/no-such-route"},
		{http.MethodPut, "/api/repos"},
	} {
		resp, body := e.doReq(t, c.method, c.path, repoTok, nil, "")
		if !muxFallback(resp, body) {
			t.Fatalf("control %s %s = %d %q (%s), want unmatchedAPIRoute's 404/405",
				c.method, c.path, resp.StatusCode, resp.Header.Get("Content-Type"), body)
		}
	}

	expand := strings.NewReplacer(
		"{repo_name}", url.PathEscape(fx.repo.URL), // has a slash, so this sends %2F
		"{tag_name}", url.PathEscape("no-such/tag"),
		"{git_credential_id}", "00000000-0000-0000-0000-000000000000",
	)
	for _, op := range sdk010Routes {
		path := expand.Replace(op.path)
		if strings.Contains(path, "{") {
			t.Fatalf("%s: unexpanded path parameter in %q", op.id, path)
		}
		// Without a token, the route's auth answers 401 in JSON.
		resp, body := e.doReq(t, op.method, path, "", nil, "")
		if resp.StatusCode != http.StatusUnauthorized || muxFallback(resp, body) {
			t.Errorf("%s %s %s without token = %d %q (%s), want the route's 401",
				op.id, op.method, path, resp.StatusCode, resp.Header.Get("Content-Type"), body)
		}
		// With a valid token, the request must get past auth and any alias
		// re-dispatch to a handler. A malformed body stops every write
		// before it changes anything.
		tok := repoTok
		if op.id == "meta.get" || op.id == "repos.list" {
			tok = orgTok
		}
		var reqBody []byte
		if op.method == http.MethodPost || op.method == http.MethodPut || op.method == http.MethodPatch {
			reqBody = []byte(`{`)
		}
		resp, body = e.doReq(t, op.method, path, tok, reqBody, "")
		if muxFallback(resp, body) || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			t.Errorf("%s %s %s with token = %d %q (%s), want a handler answer",
				op.id, op.method, path, resp.StatusCode, resp.Header.Get("Content-Type"), body)
		}
	}
}

// TestCanonicalRepoCollection pins GET and POST /api/repos (issue #656) to
// the v1 list and create handlers: same responses, scopes and filters.
func TestCanonicalRepoCollection(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		name := "sdk/canonical-" + randomHex(t, 4)
		createTok := e.mint(t, tokenSpec{iss: org, repo: name, scopes: []string{auth.ScopeRepoWrite}})
		resp, body := e.doReq(t, http.MethodPost, "/api/repos", createTok, []byte(`{"default_branch":"trunk"}`), "")
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("POST /api/repos = %d (%s), want 201", resp.StatusCode, body)
		}
		var created createRepoResponse
		if err := json.Unmarshal(body, &created); err != nil || created.RepoName != name || created.RepoID == "" {
			t.Fatalf("POST /api/repos body %s (err %v), want repo_name %q and a repo_id", body, err, name)
		}
		createRepo(t, e, org, "sdk/other-"+randomHex(t, 4), nil, http.StatusCreated)
		createRepo(t, e, org, "acme/canonical-"+randomHex(t, 4), nil, http.StatusCreated)

		// A matching repeat create is a side-effect-free 200 on both routes.
		aliasRequireIdentical(t, e, http.MethodPost, "/api/repos", "/api/v1/repos", createTok, []byte(`{"default_branch":"trunk"}`))
		// Error answers match too: wrong scope, and a bad body.
		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
		aliasRequireIdentical(t, e, http.MethodPost, "/api/repos", "/api/v1/repos", orgTok, []byte(`{}`))
		aliasRequireIdentical(t, e, http.MethodPost, "/api/repos", "/api/v1/repos", createTok, []byte(`{"default_branch":1}`))

		for _, query := range []string{"", "?prefix=SDK/", "?prefix=sdk/&q=canonical", "?q=canonical&limit=1", "?limit=0"} {
			aliasRequireIdentical(t, e, http.MethodGet, "/api/repos"+query, "/api/v1/repos"+query, orgTok, nil)
		}
		aliasRequireIdentical(t, e, http.MethodGet, "/api/repos", "/api/v1/repos", createTok, nil) // repo token: 403

		resp, body = e.doReq(t, http.MethodGet, "/api/repos?prefix=sdk/&q=canonical", orgTok, nil, "")
		var page listReposResponse
		if err := json.Unmarshal(body, &page); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /api/repos = %d %s (err %v)", resp.StatusCode, body, err)
		}
		if len(page.Repos) != 1 || page.Repos[0].RepoName != name {
			t.Fatalf("GET /api/repos?prefix=sdk/&q=canonical = %s, want only %q", body, name)
		}
	})
}

// TestTagDeleteInPath pins DELETE /api/repos/{repo_name}/tags/{tag_name}
// (issue #656): the body-form handler with the tag name from the path.
func TestTagDeleteInPath(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)
		esc := url.PathEscape(fx.repo.URL)
		tok := e.mint(t, tokenSpec{iss: org, repo: fx.repo.URL, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})

		tag := "release/v" + randomHex(t, 4) // a slash travels as %2F
		other := "keep-" + randomHex(t, 4)
		for _, name := range []string{tag, other} {
			resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/tags", tok,
				fmt.Appendf(nil, `{"name": %q, "ref": %q}`, name, fx.c1), "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("create tag %q = %d (%s)", name, resp.StatusCode, body)
			}
		}

		// The path names the tag; a body naming another tag is not read.
		resp, body := e.doReq(t, http.MethodDelete, "/api/repos/"+esc+"/tags/"+url.PathEscape(tag), tok,
			fmt.Appendf(nil, `{"name": %q}`, other), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("DELETE tag in path = %d (%s), want 200", resp.StatusCode, body)
		}
		var deleted wrefDeleteTagResponse
		if err := json.Unmarshal(body, &deleted); err != nil || deleted.Name != tag || deleted.Message != "tag deleted" {
			t.Fatalf("DELETE tag in path body %s (err %v), want name %q, message \"tag deleted\"", body, err, tag)
		}
		for name, want := range map[string]int{tag: http.StatusNotFound, other: http.StatusOK} {
			resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/tag?name="+url.QueryEscape(name), tok, nil, "")
			if resp.StatusCode != want {
				t.Errorf("GET tag %q after delete = %d (%s), want %d", name, resp.StatusCode, body, want)
			}
		}

		// Failures match the body form: a missing tag, an invalid name, and
		// a token without git:read.
		for _, name := range []string{tag, "bad..name"} {
			aliasRequireIdentical(t, e, http.MethodDelete, "/api/repos/"+esc+"/tags/"+url.PathEscape(name),
				"/api/v1/repos/tags", tok, fmt.Appendf(nil, `{"name": %q}`, name))
		}
		writeOnly := e.mint(t, tokenSpec{iss: org, repo: fx.repo.URL, scopes: []string{auth.ScopeGitWrite}})
		resp, body = e.doReq(t, http.MethodDelete, "/api/repos/"+esc+"/tags/"+other, writeOnly, nil, "")
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("DELETE tag in path without git:read = %d (%s), want 403", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)
	})
}
