package api

// PATCH /api/v1/repo and its named-repo alias PATCH /api/repos/{repo_name}
// (issue #18, openapi.yaml update_repo): change the default branch of the
// JWT repo claim's repository. Live-spec semantics under test: repo:write
// scope, required {"default_branch"} body, target branch must already
// exist, CAS against the previously read value, and a response mirroring
// exactly repo_id/id/repo_name/url/default_branch (no created_at).

import (
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

		// Alias shape with a refs/heads/-qualified name (openapi.yaml: values
		// are qualified for you): back to main, recorded in short form.
		resp, body = e.doReq(t, http.MethodPatch, "/api/repos/"+url.PathEscape(fx.url), writeTok,
			[]byte(`{"default_branch":"refs/heads/main"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PATCH alias = %d: %s", resp.StatusCode, body)
		}
		if out := patchRepoBody(t, body); out["default_branch"] != "main" {
			t.Fatalf("alias PATCH default_branch = %v, want main (short form)", out["default_branch"])
		}
		if got := getDefaultBranch(t, e, fx); got != "main" {
			t.Fatalf("GET /repo default_branch = %q after alias PATCH, want main", got)
		}

		// Both route shapes answer identically for a non-mutating case
		// (unknown branch -> 404 branch not found).
		aliasRequireIdentical(t, e, http.MethodPatch,
			"/api/repos/"+url.PathEscape(fx.url), "/api/v1/repo", writeTok,
			[]byte(`{"default_branch":"ghost"}`))
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
			{"non-branch ref", []byte(`{"default_branch":"refs/tags/annot"}`), http.StatusBadRequest},
			{"invalid name", []byte(`{"default_branch":"bad..name"}`), http.StatusBadRequest},
			{"unknown branch", []byte(`{"default_branch":"ghost"}`), http.StatusNotFound},
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
