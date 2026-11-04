package api

// Integration tests for the git-credentials CRUD surface (docs/reference/http-api.md,
// handlers_credentials.go), running over both storage backend lanes via the
// shared usyncStack from sync_test.go.

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
)

// gcredCreate stores a credential for repoID via POST and returns its id.
func gcredCreate(t *testing.T, s *usyncStack, repoURL, repoID string) string {
	t.Helper()
	token := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
	body := []byte(`{"repo_id":"` + repoID + `","username":"git","password":"tok-1"}`)
	resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", token, body)
	// 201 per openapi.yaml (docs/archive/phases/14-phase5-implementation-notes.md deferred #4).
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

		// Validation failures: missing body, missing fields. (A missing
		// repo_id is NOT one: it falls back to the token's repo claim,
		// covered by TestCredentialCreateByPathName.)
		for name, body := range map[string][]byte{
			"no body":     nil,
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

		// Happy path: the token may carry the repo url (docs/reference/authentication.md) while the
		// body carries the repo_id.
		gcredCreate(t, s, repoURL, repoID)

		// Duplicate -> 409 (docs/reference/http-api.md: 409 = exists).
		resp, body := s.req(t, http.MethodPost, "/api/v1/repos/git-credentials", repoToken,
			[]byte(`{"repo_id":"`+repoID+`","password":"p2"}`))
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("duplicate: status %d, want 409", resp.StatusCode)
		}
		requireProblemCode(t, resp, body, "credential_exists")
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
		resp, body := s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"id":"ggc_0000000000000000","password":"p"}`))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown id: status %d, want 404", resp.StatusCode)
		}
		requireProblemCode(t, resp, body, "credential_not_found")
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

		// Another org's token never reaches the handler: the served-org
		// gate (issue #47) answers 403 at the middleware, mirroring the
		// git surface.
		otherOrg := e.newOrg(t)
		otherOrgToken := s.e.mint(t, tokenSpec{iss: otherOrg, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		resp, _ = s.req(t, http.MethodPut, "/api/v1/repos/git-credentials", otherOrgToken,
			[]byte(`{"id":"`+credID+`","password":"p"}`))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("cross-org update: status %d, want 403", resp.StatusCode)
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

		// Cross-org delete -> 403 at the served-org gate (issue #47),
		// credential untouched.
		otherOrg := e.newOrg(t)
		otherOrgToken := s.e.mint(t, tokenSpec{iss: otherOrg, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		resp, _ := s.req(t, http.MethodDelete, "/api/v1/repos/git-credentials", otherOrgToken,
			[]byte(`{"id":"`+credID+`"}`))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("cross-org delete: status %d, want 403", resp.StatusCode)
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
		resp, body := s.req(t, http.MethodDelete, "/api/v1/repos/git-credentials", orgToken,
			[]byte(`{"id":"`+credID+`"}`))
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("second delete: status %d, want 404", resp.StatusCode)
		}
		requireProblemCode(t, resp, body, "credential_not_found")
		resp, _ = s.req(t, http.MethodDelete, "/api/v1/repos/git-credentials", orgToken, []byte(`{}`))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("missing id: status %d, want 400", resp.StatusCode)
		}

		// The repo slot frees up: a new credential can be stored.
		gcredCreate(t, s, repoURL, repoID)
	})
}

// Path-id route tests (docs/archive/phases/15-phase6-implementation-notes.md deferred #1): PUT/DELETE
// /api/repos/{repo_name}/git-credentials/{git_credential_id} per
// openapi.yaml, served by the FULL api.New server (testEnv) so the
// registration in registerAliasRoutes is what is exercised.

// gcredCreateEnv stores a credential for repoID through the shared full
// server (durable meta-backed stores) and returns its id.
func gcredCreateEnv(t *testing.T, e *testEnv, org, repoURL, repoID string) string {
	t.Helper()
	token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
	body := []byte(`{"repo_id":"` + repoID + `","username":"git","password":"tok-1"}`)
	resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos/git-credentials", token, body, "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create credential status %d, want 201: %s", resp.StatusCode, respBody)
	}
	var out map[string]string
	if err := json.Unmarshal(respBody, &out); err != nil {
		t.Fatal(err)
	}
	if out["id"] == "" {
		t.Fatalf("create credential body = %s", respBody)
	}
	return out["id"]
}

// requireGcredPutBody asserts the exact v1-PUT response shape
// {id, created_at} with a parseable RFC 3339 timestamp.
func requireGcredPutBody(t *testing.T, body []byte, wantID string) {
	t.Helper()
	var out map[string]string
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out["id"] != wantID || len(out) != 2 {
		t.Fatalf("PUT body = %s, want exactly {id: %q, created_at}", body, wantID)
	}
	if _, err := time.Parse(time.RFC3339, out["created_at"]); err != nil {
		t.Fatalf("created_at %q: %v", out["created_at"], err)
	}
}

func TestCredentialPathIDRoundTrip(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "team/credpath-" + randomHex(t, 4)
		repo := createRepo(t, e, org, repoURL, nil, http.StatusCreated)
		credID := gcredCreateEnv(t, e, org, repoURL, repo.RepoID)
		esc := url.PathEscape(repoURL) // the name spans a slash: %2F handling included
		writeTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})

		// PUT rotate -> 200 {id, created_at} (identical to the v1 PUT shape).
		resp, body := e.doReq(t, http.MethodPut, "/api/repos/"+esc+"/git-credentials/"+credID, writeTok,
			[]byte(`{"username":"git","password":"tok-2"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status %d: %s", resp.StatusCode, body)
		}
		requireGcredPutBody(t, body, credID)

		// Username is optional (token-only providers), and the path also
		// resolves by repo_id (meta.ResolveRepoKey ByID branch).
		resp, body = e.doReq(t, http.MethodPut,
			"/api/repos/"+url.PathEscape(repo.RepoID)+"/git-credentials/"+credID, writeTok,
			[]byte(`{"password":"tok-3"}`), "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT by repo_id status %d: %s", resp.StatusCode, body)
		}
		requireGcredPutBody(t, body, credID)

		// DELETE -> 204 with empty body (identical to the v1 DELETE).
		resp, body = e.doReq(t, http.MethodDelete, "/api/repos/"+esc+"/git-credentials/"+credID, writeTok, nil, "")
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE status %d: %s", resp.StatusCode, body)
		}
		if len(body) != 0 {
			t.Fatalf("204 body = %q, want empty", body)
		}

		// Repeat DELETE -> 404 credential_not_found.
		resp, body = e.doReq(t, http.MethodDelete, "/api/repos/"+esc+"/git-credentials/"+credID, writeTok, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("second DELETE status %d: %s", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// The repo slot frees up: a new credential can be stored.
		gcredCreateEnv(t, e, org, repoURL, repo.RepoID)
	})
}

func TestCredentialPathIDNegatives(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "team/credpath-" + randomHex(t, 4)
		repo := createRepo(t, e, org, repoURL, nil, http.StatusCreated)
		credID := gcredCreateEnv(t, e, org, repoURL, repo.RepoID)
		esc := url.PathEscape(repoURL)
		writeTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
		putBody := []byte(`{"username":"git","password":"p2"}`)
		putPath := "/api/repos/" + esc + "/git-credentials/" + credID

		// Unknown repo_name -> 404.
		resp, body := e.doReq(t, http.MethodPut, "/api/repos/no-such-repo/git-credentials/"+credID, writeTok, putBody, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("unknown repo_name = %d (%s), want 404", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// Unknown credential id -> 404.
		resp, body = e.doReq(t, http.MethodPut, "/api/repos/"+esc+"/git-credentials/ggc_0000000000000000", writeTok, putBody, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("unknown credential id = %d (%s), want 404", resp.StatusCode, body)
		}

		// A credential attached to ANOTHER repo answers 404 like a missing
		// one (no cross-repo existence oracle).
		otherURL := "team/credpath-other-" + randomHex(t, 4)
		other := createRepo(t, e, org, otherURL, nil, http.StatusCreated)
		otherCred := gcredCreateEnv(t, e, org, otherURL, other.RepoID)
		resp, body = e.doReq(t, http.MethodPut, "/api/repos/"+esc+"/git-credentials/"+otherCred, writeTok, putBody, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("foreign credential = %d (%s), want 404", resp.StatusCode, body)
		}

		// Claim repo != path repo -> 404 (alias rule: mismatches answer 404,
		// not 403, so foreign tokens cannot probe which names exist).
		otherTok := e.mint(t, tokenSpec{iss: org, repo: otherURL, scopes: []string{auth.ScopeRepoWrite}})
		for _, method := range []string{http.MethodPut, http.MethodDelete} {
			resp, body = e.doReq(t, method, putPath, otherTok, putBody, "")
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("%s claim mismatch = %d (%s), want 404", method, resp.StatusCode, body)
			}
			requireEnvelope(t, resp, body)
		}

		// Missing password / missing body -> 400.
		resp, body = e.doReq(t, http.MethodPut, putPath, writeTok, []byte(`{"username":"git"}`), "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("missing password = %d (%s), want 400", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)
		resp, body = e.doReq(t, http.MethodPut, putPath, writeTok, nil, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("missing body = %d (%s), want 400", resp.StatusCode, body)
		}

		// Org token on the per-repo route -> 403 (middleware, ADR-26).
		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		resp, body = e.doReq(t, http.MethodPut, putPath, orgTok, putBody, "")
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("org token = %d (%s), want 403", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// No token -> 401 with WWW-Authenticate: Bearer.
		resp, body = e.doReq(t, http.MethodPut, putPath, "", putBody, "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("no token = %d (%s), want 401", resp.StatusCode, body)
		}
		if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
			t.Errorf("401 WWW-Authenticate = %q, want Bearer", got)
		}
		requireEnvelope(t, resp, body)

		// None of the negatives touched the credential: rotate + delete
		// still succeed with the right token.
		resp, body = e.doReq(t, http.MethodPut, putPath, writeTok, putBody, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("final PUT status %d: %s", resp.StatusCode, body)
		}
		resp, body = e.doReq(t, http.MethodDelete, putPath, writeTok, nil, "")
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("final DELETE status %d: %s", resp.StatusCode, body)
		}
	})
}

// TestCredentialCreateByPathName covers POST
// /api/repos/{repo_name}/git-credentials (openapi.yaml): the body carries
// {username?, password} and NO repo_id — the alias re-dispatch reaches
// handleCreateCredential, which falls back to the token's repo claim.
func TestCredentialCreateByPathName(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "team/credalias-" + randomHex(t, 4)
		createRepo(t, e, org, repoURL, nil, http.StatusCreated)
		writeTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})

		resp, body := e.doReq(t, http.MethodPost, "/api/repos/"+url.PathEscape(repoURL)+"/git-credentials",
			writeTok, []byte(`{"username":"git","password":"tok-1"}`), "")
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("alias POST status %d, want 201: %s", resp.StatusCode, body)
		}
		var out map[string]string
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatal(err)
		}
		if out["id"] == "" {
			t.Fatalf("alias POST body = %s, want {id}", body)
		}
	})
}
