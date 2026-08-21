package api

// PATCH /api/v1/repo and its named-repo alias PATCH /api/repos/{repo_name}
// (issue #18, openapi.yaml update_repo): change the default branch of the
// JWT repo claim's repository. Live-spec semantics under test: repo:write
// scope, required {"default_branch"} body, target branch must already
// exist, CAS against the previously read value, and a response mirroring
// exactly repo_id/id/repo_name/url/default_branch (no created_at).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"codestorage/internal/auth"
)

// patchRepoBody decodes a PATCH /repo 200 body and pins the exact live-spec
// field set: the five required fields and nothing else.
func patchRepoBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode PATCH body %q: %v", body, err)
	}
	for _, key := range []string{"repo_id", "id", "repo_name", "url", "default_branch"} {
		if _, ok := out[key]; !ok {
			t.Fatalf("PATCH body missing %q: %q", key, body)
		}
	}
	if len(out) != 5 {
		t.Fatalf("PATCH body has %d fields, want exactly 5 (openapi.yaml additionalProperties: false): %q", len(out), body)
	}
	return out
}

// getDefaultBranch reads GET /api/v1/repo's default_branch with a git:read
// token (the endpoint PATCH must be reflected by).
func getDefaultBranch(t *testing.T, e *testEnv, fx wrefFixture) string {
	t.Helper()
	readTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitRead}})
	resp, body := e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repo = %d: %s", resp.StatusCode, body)
	}
	var out struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode GET /repo body %q: %v", body, err)
	}
	return out.DefaultBranch
}

func TestPatchRepoDefaultBranch(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e) // main@c2 plus branch feature@c1
		writeTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeRepoWrite}})

		// Happy path, v1 shape: main -> feature.
		resp, body := e.doReq(t, http.MethodPatch, "/api/v1/repo", writeTok,
			[]byte(`{"default_branch":"feature"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PATCH /api/v1/repo = %d: %s", resp.StatusCode, body)
		}
		out := patchRepoBody(t, body)
		if out["repo_id"] != fx.repoID || out["id"] != fx.repoID ||
			out["repo_name"] != fx.url || out["url"] != fx.url ||
			out["default_branch"] != "feature" {
			t.Fatalf("PATCH body = %v, want repo %s/%s default_branch feature", out, fx.repoID, fx.url)
		}
		if got := getDefaultBranch(t, e, fx); got != "feature" {
			t.Fatalf("GET /repo default_branch = %q after PATCH, want feature", got)
		}

		// Alias shape: back to main.
		resp, body = e.doReq(t, http.MethodPatch, "/api/repos/"+url.PathEscape(fx.url), writeTok,
			[]byte(`{"default_branch":"main"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PATCH alias = %d: %s", resp.StatusCode, body)
		}
		if out := patchRepoBody(t, body); out["default_branch"] != "main" {
			t.Fatalf("alias PATCH default_branch = %v, want main", out["default_branch"])
		}
		if got := getDefaultBranch(t, e, fx); got != "main" {
			t.Fatalf("GET /repo default_branch = %q after alias PATCH, want main", got)
		}

		// An unborn branch is accepted (openapi.yaml: "The branch itself is
		// not created by this call"; the default may point at nothing, like
		// a freshly created repo before its first push).
		resp, body = e.doReq(t, http.MethodPatch, "/api/v1/repo", writeTok,
			[]byte(`{"default_branch":"unborn"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PATCH unborn branch = %d, want 200: %s", resp.StatusCode, body)
		}
		if got := getDefaultBranch(t, e, fx); got != "unborn" {
			t.Fatalf("GET /repo default_branch = %q, want unborn", got)
		}

		// A refs/-qualified value is stored and mirrored verbatim
		// (openapi.yaml: "a value that already starts with `refs/` is used
		// verbatim").
		resp, body = e.doReq(t, http.MethodPatch, "/api/v1/repo", writeTok,
			[]byte(`{"default_branch":"refs/heads/main"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PATCH qualified ref = %d: %s", resp.StatusCode, body)
		}
		if out := patchRepoBody(t, body); out["default_branch"] != "refs/heads/main" {
			t.Fatalf("qualified PATCH default_branch = %v, want refs/heads/main (verbatim)", out["default_branch"])
		}

		// Restore, then check both route shapes answer identically for a
		// non-mutating failure (invalid name -> 400).
		resp, _ = e.doReq(t, http.MethodPatch, "/api/v1/repo", writeTok, []byte(`{"default_branch":"main"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("restore PATCH = %d", resp.StatusCode)
		}
		aliasRequireIdentical(t, e, http.MethodPatch,
			"/api/repos/"+url.PathEscape(fx.url), "/api/v1/repo", writeTok,
			[]byte(`{"default_branch":"bad..name"}`))
	})
}

func TestPatchRepoDefaultBranchErrors(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		writeTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeRepoWrite}})

		cases := []struct {
			name string
			body []byte
			want int
		}{
			{"missing body", nil, http.StatusBadRequest},
			{"empty object", []byte(`{}`), http.StatusBadRequest},
			{"empty branch", []byte(`{"default_branch":""}`), http.StatusBadRequest},
			{"invalid JSON", []byte(`{"default_branch":`), http.StatusBadRequest},
			{"invalid name", []byte(`{"default_branch":"bad..name"}`), http.StatusBadRequest},
			{"lock suffix", []byte(`{"default_branch":"dev.lock"}`), http.StatusBadRequest},
			{"trailing dot", []byte(`{"default_branch":"main."}`), http.StatusBadRequest},
			{"qualified trailing dot", []byte(`{"default_branch":"refs/heads/main."}`), http.StatusBadRequest},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := e.doReq(t, http.MethodPatch, "/api/v1/repo", writeTok, tc.body, "")
				if resp.StatusCode != tc.want {
					t.Fatalf("PATCH = %d, want %d: %s", resp.StatusCode, tc.want, body)
				}
				requireEnvelope(t, resp, body)
			})
		}

		// Scope gate: git:read+git:write without repo:write -> 403.
		gitTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url,
			scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
		resp, body := e.doReq(t, http.MethodPatch, "/api/v1/repo", gitTok,
			[]byte(`{"default_branch":"feature"}`), "")
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("bad-scope PATCH = %d, want 403: %s", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// No token -> 401 with WWW-Authenticate: Bearer.
		resp, body = e.doReq(t, http.MethodPatch, "/api/v1/repo", "",
			[]byte(`{"default_branch":"feature"}`), "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("anonymous PATCH = %d, want 401: %s", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// Repo claim that resolves to nothing -> 404.
		ghostTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url + "-absent",
			scopes: []string{auth.ScopeRepoWrite}})
		resp, body = e.doReq(t, http.MethodPatch, "/api/v1/repo", ghostTok,
			[]byte(`{"default_branch":"feature"}`), "")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("absent-repo PATCH = %d, want 404: %s", resp.StatusCode, body)
		}

		// Alias for a foreign repo name with this repo's token -> 404 (no
		// existence probing).
		other := wrefSeedFixture(t, e)
		resp, body = e.doReq(t, http.MethodPatch, "/api/repos/"+url.PathEscape(other.url), writeTok,
			[]byte(`{"default_branch":"feature"}`), "")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("foreign-name alias PATCH = %d, want 404: %s", resp.StatusCode, body)
		}

		// Every failure above left the default branch untouched.
		if got := getDefaultBranch(t, e, fx); got != "main" {
			t.Fatalf("default_branch = %q after error matrix, want main", got)
		}
	})
}

// The default-branch deletion protection must hold when the default is
// stored in refs/-qualified form (PATCH stores such values verbatim): the
// short-name DELETE must still be refused, comparing stored ref names.
func TestPatchRepoQualifiedDefaultStillDeleteProtected(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		writeTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeRepoWrite}})

		resp, body := e.doReq(t, http.MethodPatch, "/api/v1/repo", writeTok,
			[]byte(`{"default_branch":"refs/heads/feature"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PATCH qualified default = %d: %s", resp.StatusCode, body)
		}

		gitTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url,
			scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
		resp, body = e.doReq(t, http.MethodDelete, "/api/v1/repos/branches", gitTok,
			[]byte(`{"target_branch":"feature"}`), "")
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("DELETE default (qualified form) = %d, want 403: %s", resp.StatusCode, body)
		}
		if got := wrefRefs(t, e, fx.repoID)["refs/heads/feature"]; got == "" {
			t.Fatal("refs/heads/feature was deleted despite being the default branch")
		}
	})
}

// A fork inheriting a refs/heads/-qualified stored default must create its
// initial ref at the trimmed branch name — never a double-qualified
// refs/heads/refs/heads/... with a dangling HEAD.
func TestPatchRepoQualifiedDefaultForkInheritance(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newForkServer(t, e)
		src := wrefSeedFixture(t, e)
		srcWriteTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeRepoWrite}})
		resp, body := e.doReq(t, http.MethodPatch, "/api/v1/repo", srcWriteTok,
			[]byte(`{"default_branch":"refs/heads/feature"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PATCH qualified default = %d: %s", resp.StatusCode, body)
		}

		srcReadTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeGitRead}})
		caller := newForkCaller(t, e, "")
		fresp, fbody := forkDo(t, srv, caller.token, forkBody(srcReadTok, nil))
		forkID := forkRequireCreated(t, fresp, fbody)

		// The fork's only ref is the trimmed name at the source default's
		// tip (feature@c1 in the fixture), and the row records the name.
		refs := wrefRefs(t, e, forkID)
		if len(refs) != 1 || refs["refs/heads/feature"] != src.c1 {
			t.Fatalf("fork refs = %v, want exactly refs/heads/feature=%s", refs, src.c1)
		}
		row, err := e.store.GetRepoByURL(context.Background(), caller.org, caller.url)
		if err != nil || row.DefaultBranch != "feature" {
			t.Fatalf("fork repo row = %+v err %v, want default_branch feature", row, err)
		}
	})
}

// Ref policies gate the HEAD repoint: no-push and unknown ops on a rule
// covering HEAD or the new target deny with 403; no-force-push and
// verify-sig (and rules covering only other refs) do not.
func TestPatchRepoDefaultBranchPolicies(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)

		cases := []struct {
			name   string
			refs   string
			branch string // PATCH target; "" means "feature"
			want   int
		}{
			{"no-push wildcard", `[["*",["no-push"]]]`, "", http.StatusForbidden},
			{"no-push on target", `[["refs/heads/feature",["no-push"]]]`, "", http.StatusForbidden},
			{"no-push on HEAD", `[["HEAD",["no-push"]]]`, "", http.StatusForbidden},
			// A branch literally named HEAD: the policy target must be the
			// derived refs/heads/HEAD, not the NormalizeRefName pseudoref.
			{"no-push on refs/heads/HEAD literal", `[["refs/heads/HEAD",["no-push"]]]`, "HEAD", http.StatusForbidden},
			{"unknown op", `[["*",["freeze"]]]`, "", http.StatusForbidden},
			{"malformed claim", `[["*"]]`, "", http.StatusForbidden},
			{"no-push elsewhere", `[["refs/heads/main",["no-push"]]]`, "", http.StatusOK},
			{"no-force-push", `[["*",["no-force-push"]]]`, "", http.StatusOK},
			{"verify-sig", `[["*",["verify-sig"]]]`, "", http.StatusOK},
			{"empty rule", `[["*",[]]]`, "", http.StatusOK},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				branch := tc.branch
				if branch == "" {
					branch = "feature"
				}
				tok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url,
					scopes: []string{auth.ScopeRepoWrite}, refs: tc.refs})
				resp, body := e.doReq(t, http.MethodPatch, "/api/v1/repo", tok,
					[]byte(`{"default_branch":"`+branch+`"}`), "")
				if resp.StatusCode != tc.want {
					t.Fatalf("PATCH %s with refs=%s = %d, want %d: %s", branch, tc.refs, resp.StatusCode, tc.want, body)
				}
				if tc.want == http.StatusOK {
					// Restore for the next case.
					restore := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeRepoWrite}})
					if r2, _ := e.doReq(t, http.MethodPatch, "/api/v1/repo", restore,
						[]byte(`{"default_branch":"main"}`), ""); r2.StatusCode != http.StatusOK {
						t.Fatalf("restore PATCH = %d", r2.StatusCode)
					}
				}
			})
		}

		// Denied requests never reached the store.
		if got := getDefaultBranch(t, e, fx); got != "main" {
			t.Fatalf("default_branch = %q after policy matrix, want main", got)
		}
	})
}
