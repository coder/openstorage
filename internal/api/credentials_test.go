package api

// Integration tests for the git-credentials CRUD surface (docs/03,
// handlers_credentials.go), running over both storage backend lanes via the
// shared usyncStack from sync_test.go.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"codestorage/internal/auth"
)

// gcredCreate stores a credential for repoID via POST and returns its id.
func gcredCreate(t *testing.T, s *usyncStack, repoURL, repoID string) string {
	t.Helper()
	token := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
	body := []byte(`{"repo_id":"` + repoID + `","username":"git","password":"tok-1"}`)
	resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", token, body)
	// 201 per openapi.yaml (docs/14 deferred #4).
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create credential status %d, want 201: %s", resp.StatusCode, respBody)
	}
	var out map[string]string
	if err := json.Unmarshal(respBody, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out["id"], "ggc_") || len(out) != 1 {
		t.Fatalf("create credential body = %s", respBody)
	}
	return out["id"]
}

func TestCredentialCreate(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		repoURL := "team/cred-" + randomHex(t, 4)
		repoID := s.createRepo(t, repoURL, nil)
		repoToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})

		// Validation failures: missing body, missing fields.
		for name, body := range map[string][]byte{
			"no body":     nil,
			"no repo_id":  []byte(`{"password":"p"}`),
			"no password": []byte(`{"repo_id":"` + repoID + `"}`),
			"bad JSON":    []byte(`{`),
		} {
			resp, _ := s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", repoToken, body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s: status %d, want 400", name, resp.StatusCode)
			}
		}

		// Unknown repo -> 404.
		resp, _ := s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", repoToken,
			[]byte(`{"repo_id":"repo_000000000000","password":"p"}`))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown repo: status %d, want 404", resp.StatusCode)
		}

		// Claim/target mismatch -> 403: token scoped to another repo.
		otherURL := "team/other-" + randomHex(t, 4)
		s.createRepo(t, otherURL, nil)
		otherToken := s.e.mint(t, tokenSpec{iss: s.org, repo: otherURL, scopes: []string{auth.ScopeRepoWrite}})
		resp, _ = s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", otherToken,
			[]byte(`{"repo_id":"`+repoID+`","password":"p"}`))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("claim mismatch: status %d, want 403", resp.StatusCode)
		}

		// Org-scoped token on the repo-scoped POST route -> 403 (middleware).
		orgToken := s.e.mint(t, tokenSpec{iss: s.org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		resp, _ = s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"repo_id":"`+repoID+`","password":"p"}`))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("org token on POST: status %d, want 403", resp.StatusCode)
		}

		// Missing token -> 401.
		resp, _ = s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", "",
			[]byte(`{"repo_id":"`+repoID+`","password":"p"}`))
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("no token: status %d, want 401", resp.StatusCode)
		}

		// Happy path: the token may carry the repo url (docs/02) while the
		// body carries the repo_id.
		gcredCreate(t, s, repoURL, repoID)

		// Duplicate -> 409 (docs/03: 409 = exists).
		resp, _ = s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", repoToken,
			[]byte(`{"repo_id":"`+repoID+`","password":"p2"}`))
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("duplicate: status %d, want 409", resp.StatusCode)
		}
	})
}

func TestCredentialUpdate(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		repoURL := "team/cred-" + randomHex(t, 4)
		repoID := s.createRepo(t, repoURL, nil)
		credID := gcredCreate(t, s, repoURL, repoID)
		orgToken := s.e.mint(t, tokenSpec{iss: s.org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})

		// Happy path -> {id, created_at}.
		resp, respBody := s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"id":"`+credID+`","username":"u2","password":"p2"}`))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("update status %d: %s", resp.StatusCode, respBody)
		}
		var out map[string]string
		if err := json.Unmarshal(respBody, &out); err != nil {
			t.Fatal(err)
		}
		if out["id"] != credID || len(out) != 2 {
			t.Fatalf("update body = %s", respBody)
		}
		if _, err := time.Parse(time.RFC3339, out["created_at"]); err != nil {
			t.Fatalf("created_at %q: %v", out["created_at"], err)
		}

		// Missing/unknown id and validation failures.
		resp, _ = s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"id":"ggc_0000000000000000","password":"p"}`))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown id: status %d, want 404", resp.StatusCode)
		}
		resp, _ = s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"password":"p"}`))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("missing id: status %d, want 400", resp.StatusCode)
		}
		resp, _ = s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"id":"`+credID+`"}`))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("missing password: status %d, want 400", resp.StatusCode)
		}

		// Repo-scoped token on the org-scoped route -> 403 (middleware).
		repoToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
		resp, _ = s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", repoToken,
			[]byte(`{"id":"`+credID+`","password":"p"}`))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("repo token on PUT: status %d, want 403", resp.StatusCode)
		}

		// Another org's token must see 404, not this org's credential.
		otherOrg := e.newOrg(t)
		otherOrgToken := s.e.mint(t, tokenSpec{iss: otherOrg, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		resp, _ = s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", otherOrgToken,
			[]byte(`{"id":"`+credID+`","password":"p"}`))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("cross-org update: status %d, want 404", resp.StatusCode)
		}
	})
}

func TestCredentialDelete(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		repoURL := "team/cred-" + randomHex(t, 4)
		repoID := s.createRepo(t, repoURL, nil)
		credID := gcredCreate(t, s, repoURL, repoID)
		orgToken := s.e.mint(t, tokenSpec{iss: s.org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})

		// Cross-org delete -> 404, credential untouched.
		otherOrg := e.newOrg(t)
		otherOrgToken := s.e.mint(t, tokenSpec{iss: otherOrg, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		resp, _ := s.req(t, http.MethodDelete, "/api/v1/repos/git-credentials", otherOrgToken,
			[]byte(`{"id":"`+credID+`"}`))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("cross-org delete: status %d, want 404", resp.StatusCode)
		}

		// Happy path -> 204 with empty body.
		resp, respBody := s.req(t, http.MethodDelete, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"id":"`+credID+`"}`))
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("delete status %d: %s", resp.StatusCode, respBody)
		}
		if len(respBody) != 0 {
			t.Fatalf("204 body = %q, want empty", respBody)
		}

		// Second delete -> 404; missing id -> 400.
		resp, _ = s.req(t, http.MethodDelete, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"id":"`+credID+`"}`))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("second delete: status %d, want 404", resp.StatusCode)
		}
		resp, _ = s.req(t, http.MethodDelete, "/api/v1/repos/git-credentials", orgToken, []byte(`{}`))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("missing id: status %d, want 400", resp.StatusCode)
		}

		// The repo slot frees up: a new credential can be stored.
		gcredCreate(t, s, repoURL, repoID)
	})
}
