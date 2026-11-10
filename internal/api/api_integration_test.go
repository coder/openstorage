package api

// Acceptance tests for the repo CRUD surface against real Postgres
// (testcontainers) with the server in-process on 127.0.0.1:0: the docs/reference/http-api.md
// status-code matrix (401/403/404/409/200), pagination/q, and per-algorithm
// JWT positives. Keys are generated at runtime and never committed.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
)

var repoIDRe = regexp.MustCompile(`\Arepo_[0-9a-f]{12}\z`)

// createRepo POSTs /api/v1/repos for org/urlClaim and returns the decoded
// response, failing the test unless the status is wantStatus.
func createRepo(t *testing.T, e *testEnv, org, urlClaim string, body []byte, wantStatus int) createRepoResponse {
	t.Helper()
	tok := e.mint(t, tokenSpec{iss: org, repo: urlClaim, scopes: []string{auth.ScopeRepoWrite}})
	resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos", tok, body, "")
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST /repos (%s) status = %d, want %d (body %s)", urlClaim, resp.StatusCode, wantStatus, respBody)
	}
	var out createRepoResponse
	if wantStatus < 300 {
		if err := json.Unmarshal(respBody, &out); err != nil {
			t.Fatalf("decode create response %q: %v", respBody, err)
		}
	} else {
		requireEnvelope(t, resp, respBody)
	}
	return out
}

// requireDeleteRepoShape asserts the exact delete-repo wire shape: exactly
// {repo_id, repo_name, message} (upstream openapi.yaml requires all three;
// repo_id is deprecated upstream but still present). Shared by the v1 and
// named-repo (alias) delete route tests.
func requireDeleteRepoShape(t *testing.T, body []byte, wantID, wantName string) {
	t.Helper()
	var got map[string]string
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode delete response %q: %v", body, err)
	}
	if len(got) != 3 || got["repo_id"] != wantID || got["repo_name"] != wantName || got["message"] != "repository deleted" {
		t.Fatalf("delete response = %s, want exactly {repo_id: %q, repo_name: %q, message: \"repository deleted\"}", body, wantID, wantName)
	}
}

func TestCRUDHappyPath(t *testing.T) { runBackends(t, testCRUDHappyPath) }

func testCRUDHappyPath(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	repoURL := "repo-" + randomHex(t, 8)

	// Create.
	created := createRepo(t, e, org, repoURL, []byte(`{"default_branch":"main"}`), http.StatusCreated)
	if !repoIDRe.MatchString(created.RepoID) {
		t.Fatalf("repo_id %q does not match %v", created.RepoID, repoIDRe)
	}
	if want := testHTTPBaseURL + "/" + created.RepoID + ".git"; created.HTTPURL != want {
		t.Fatalf("http_url = %q, want %q", created.HTTPURL, want)
	}
	if created.RepoName != repoURL {
		t.Fatalf("repo_name = %q, want %q", created.RepoName, repoURL)
	}
	if created.Message == "" {
		t.Fatal("create response has empty message")
	}

	// Get.
	readTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
	resp, body := e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repo status = %d (body %s)", resp.StatusCode, body)
	}
	var got repoEntry
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	// openapi.yaml GET /api/v1/repo response schema requires repo_id,
	// repo_name, url (deprecated mirror), default_branch, created_at.
	if got.RepoID != created.RepoID {
		t.Fatalf("repo_id = %q, want %q", got.RepoID, created.RepoID)
	}
	if got.RepoName != repoURL {
		t.Fatalf("repo_name = %q, want %q", got.RepoName, repoURL)
	}
	if got.URL != repoURL {
		t.Fatalf("url = %q, want %q", got.URL, repoURL)
	}
	if got.DefaultBranch != "main" {
		t.Fatalf("default_branch = %q, want main", got.DefaultBranch)
	}
	createdAt, err := time.Parse(time.RFC3339, got.CreatedAt)
	if err != nil {
		t.Fatalf("created_at %q is not RFC 3339: %v", got.CreatedAt, err)
	}
	if loc := createdAt.Location(); loc != time.UTC {
		t.Fatalf("created_at %q not rendered in UTC (zone %v)", got.CreatedAt, loc)
	}

	// List (org-scoped token).
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
	resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos", orgTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repos status = %d (body %s)", resp.StatusCode, body)
	}
	var list listReposResponse
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Repos) != 1 || list.Repos[0].URL != repoURL || list.Repos[0].RepoID != created.RepoID {
		t.Fatalf("list = %+v, want exactly the created repo", list)
	}
	if list.Repos[0].RepoName != repoURL {
		t.Fatalf("list repo_name = %q, want %q", list.Repos[0].RepoName, repoURL)
	}
	if list.HasMore || list.NextCursor != "" {
		t.Fatalf("single-page list claims more: %+v", list)
	}

	// Delete.
	writeTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
	resp, body = e.doReq(t, http.MethodDelete, "/api/v1/repos/delete", writeTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status = %d (body %s)", resp.StatusCode, body)
	}
	var deleted deleteRepoResponse
	if err := json.Unmarshal(body, &deleted); err != nil {
		t.Fatal(err)
	}
	if deleted.RepoID != created.RepoID || deleted.RepoName != repoURL || deleted.Message == "" {
		t.Fatalf("delete response = %+v", deleted)
	}
	requireDeleteRepoShape(t, body, created.RepoID, repoURL)

	// Soft-deleted: GET -> 404, hidden from list, re-delete -> 409,
	// re-create same url -> 409.
	resp, body = e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
	requireEnvelope(t, resp, body)

	resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos", orgTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repos status = %d", resp.StatusCode)
	}
	list = listReposResponse{NextCursor: "sentinel"}
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	if list.Repos == nil {
		t.Fatalf("repos must be [] (never null) when empty: %s", body)
	}
	if len(list.Repos) != 0 {
		t.Fatalf("deleted repo still listed: %+v", list)
	}

	resp, body = e.doReq(t, http.MethodDelete, "/api/v1/repos/delete", writeTok, nil, "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("re-delete status = %d, want 409 (body %s)", resp.StatusCode, body)
	}
	if msg := requireEnvelope(t, resp, body); msg != "repository already deleted" {
		t.Fatalf("re-delete error = %q", msg)
	}
	// The canonical alias resolves names without deleted repositories, so
	// its re-delete is a repository-lookup 404 (issue #352).
	resp, body = e.doReq(t, http.MethodDelete, "/api/repos/"+url.PathEscape(repoURL), writeTok, nil, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("alias re-delete status = %d, want 404 (body %s)", resp.StatusCode, body)
	}
	requireProblemCode(t, resp, body, "repository_not_found")

	createRepo(t, e, org, repoURL, nil, http.StatusNotFound) // the deleted repo keeps its name (#387)
}

func TestCreateRepoValidation(t *testing.T) { runBackends(t, testCreateRepoValidation) }

func testCreateRepoValidation(t *testing.T, e *testEnv) {
	org := e.newOrg(t)

	// repo claim "org" / "" are not repo-scoped: requireAuth rejects them
	// with 403 per docs/reference/authentication.md (claim mismatch with target).
	createRepo(t, e, org, auth.RepoClaimOrg, nil, http.StatusForbidden)
	// Empty repo claim.
	createRepo(t, e, org, "", nil, http.StatusForbidden)
	// Malformed JSON.
	createRepo(t, e, org, "v-"+randomHex(t, 4), []byte(`{not json`), http.StatusBadRequest)
	// Invalid default branch.
	createRepo(t, e, org, "v-"+randomHex(t, 4), []byte(`{"default_branch":"bad branch"}`), http.StatusBadRequest)
	// base_repo with a provider this backend cannot mirror-sync is
	// rejected (valid providers are exercised by sync_test.go).
	createRepo(t, e, org, "v-"+randomHex(t, 4),
		[]byte(`{"base_repo":{"provider":"unknown-forge","owner":"o","name":"n"}}`), http.StatusBadRequest)
	// The fork dispatcher is wired into api.New: a code-provider fork spec
	// reaches handleForkRepo — whose 400 names the missing source token —
	// never the sync creator's `operation "fork" not supported` envelope.
	{
		tok := e.mint(t, tokenSpec{iss: org, repo: "v-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
		resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos", tok,
			[]byte(`{"base_repo":{"provider":"code","operation":"fork"}}`), "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("fork without source token status = %d (body %s)", resp.StatusCode, body)
		}
		if msg := requireEnvelope(t, resp, body); msg != `base_repo.auth.token is required for provider "code"` {
			t.Fatalf("fork dispatch error = %q, want the fork handler's missing-token message", msg)
		}
	}

	// Empty body defaults the branch to main.
	repoURL := "v-" + randomHex(t, 4)
	createRepo(t, e, org, repoURL, nil, http.StatusCreated)
	readTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
	resp, body := e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repo status = %d", resp.StatusCode)
	}
	var got repoEntry
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.DefaultBranch != "main" {
		t.Fatalf("default_branch = %q, want main", got.DefaultBranch)
	}
}

// TestCreateRepoRepoName covers the optional body `repo_name` (issue #293).
// Upstream's 2026-10-02 snapshot shortened the field text to "It can contain
// a slash." but still documents the mismatch as a 400 ("did not match the JWT
// repo claim") and keeps create repo-scoped. A mismatch is that 400, in this backend's
// recorded {"error"} envelope (docs/explanation/wire-compatibility.md). The
// security invariant: the body name can only confirm the claim, never
// override it, so a mismatch creates nothing under either name.
func TestCreateRepoRepoName(t *testing.T) { runBackends(t, testCreateRepoRepoName) }

func testCreateRepoRepoName(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	requireRow := func(t *testing.T, url, wantID string) {
		t.Helper()
		readTok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /repo %q status = %d (body %s)", url, resp.StatusCode, body)
		}
		var got repoEntry
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if got.RepoName != url || (wantID != "" && got.RepoID != wantID) {
			t.Fatalf("GET /repo %q = %+v, want repo_name %q repo_id %q", url, got, url, wantID)
		}
	}
	requireNoRow := func(t *testing.T, url string) {
		t.Helper()
		readTok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET /repo %q status = %d, want 404 (body %s)", url, resp.StatusCode, body)
		}
	}
	mismatch := func(t *testing.T, claim string, body []byte) {
		t.Helper()
		tok := e.mint(t, tokenSpec{iss: org, repo: claim, scopes: []string{auth.ScopeRepoWrite}})
		resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos", tok, body, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("POST /repos claim %q body %s status = %d, want 400 (body %s)", claim, body, resp.StatusCode, respBody)
		}
		if msg := requireEnvelope(t, resp, respBody); msg != "repo_name must equal the JWT repo claim" {
			t.Fatalf("mismatch error = %q", msg)
		}
		requireNoRow(t, claim)
	}

	t.Run("absent", func(t *testing.T) {
		url := "rn-" + randomHex(t, 4)
		created := createRepo(t, e, org, url, []byte(`{"default_branch":"main"}`), http.StatusCreated)
		if created.RepoName != url {
			t.Fatalf("repo_name = %q, want %q", created.RepoName, url)
		}
		requireRow(t, url, created.RepoID)
	})

	t.Run("null is absent", func(t *testing.T) {
		url := "rn-" + randomHex(t, 4)
		created := createRepo(t, e, org, url, []byte(`{"repo_name":null}`), http.StatusCreated)
		requireRow(t, url, created.RepoID)
	})

	t.Run("equal", func(t *testing.T) {
		url := "rn-" + randomHex(t, 4)
		created := createRepo(t, e, org, url, []byte(`{"repo_name":"`+url+`"}`), http.StatusCreated)
		if created.RepoName != url {
			t.Fatalf("repo_name = %q, want %q", created.RepoName, url)
		}
		requireRow(t, url, created.RepoID)
	})

	t.Run("equal with slash", func(t *testing.T) {
		url := "pierre/rn-" + randomHex(t, 4)
		body, err := json.Marshal(map[string]any{"repo_name": url, "default_branch": "trunk"})
		if err != nil {
			t.Fatal(err)
		}
		created := createRepo(t, e, org, url, body, http.StatusCreated)
		if created.RepoName != url {
			t.Fatalf("repo_name = %q, want %q", created.RepoName, url)
		}
		requireRow(t, url, created.RepoID)
	})

	t.Run("mismatch", func(t *testing.T) {
		claim := "pierre/rn-" + randomHex(t, 4)
		other := "pierre/rn-" + randomHex(t, 4)
		for _, name := range []string{
			other,                                // a different name entirely
			"",                                   // present but empty is not absent
			strings.ToUpper(claim),               // exact, case-sensitive equality
			claim + "/",                          // no path normalization
			" " + claim,                          // no trimming
			strings.TrimPrefix(claim, "pierre/"), // no owner-prefix inference
		} {
			body, err := json.Marshal(map[string]any{"repo_name": name})
			if err != nil {
				t.Fatal(err)
			}
			mismatch(t, claim, body)
		}
		// The body name never becomes a repo either.
		requireNoRow(t, other)
		// Non-string repo_name is malformed input.
		tok := e.mint(t, tokenSpec{iss: org, repo: claim, scopes: []string{auth.ScopeRepoWrite}})
		resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos", tok, []byte(`{"repo_name":42}`), "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("numeric repo_name status = %d, want 400 (body %s)", resp.StatusCode, respBody)
		}
		requireEnvelope(t, resp, respBody)
		requireNoRow(t, claim)
		// A mismatch next to a type-invalid sibling fails decoding first
		// (generic invalid-JSON 400); either way nothing is created.
		resp, respBody = e.doReq(t, http.MethodPost, "/api/v1/repos", tok, []byte(`{"repo_name":"`+other+`","default_branch":42}`), "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("mismatch + numeric default_branch status = %d, want 400 (body %s)", resp.StatusCode, respBody)
		}
		requireEnvelope(t, resp, respBody)
		requireNoRow(t, claim)
		requireNoRow(t, other)
	})

	t.Run("mismatch naming an existing repo cannot touch it", func(t *testing.T) {
		// A token for a new repo that names someone else's existing repo in
		// the body must not create, re-create, or collide (409) with it.
		victim := "pierre/rn-victim-" + randomHex(t, 4)
		victimRepo := createRepo(t, e, org, victim, nil, http.StatusCreated)
		claim := "pierre/rn-" + randomHex(t, 4)
		body, err := json.Marshal(map[string]any{"repo_name": victim, "default_branch": "evil"})
		if err != nil {
			t.Fatal(err)
		}
		mismatch(t, claim, body)
		requireRow(t, victim, victimRepo.RepoID)
		readTok := e.mint(t, tokenSpec{iss: org, repo: victim, scopes: []string{auth.ScopeGitRead}})
		_, getBody := e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
		var got repoEntry
		if err := json.Unmarshal(getBody, &got); err != nil {
			t.Fatal(err)
		}
		if got.DefaultBranch != "main" {
			t.Fatalf("victim default_branch = %q, want main (untouched)", got.DefaultBranch)
		}
	})

	t.Run("mismatch is rejected before base_repo validation", func(t *testing.T) {
		// Sync (non-fork) base_repo: the name check comes first, so no row
		// or upstream link is created, whatever the base_repo says.
		claim := "rn-" + randomHex(t, 4)
		mismatch(t, claim, []byte(`{"repo_name":"other","base_repo":{"provider":"github","owner":"o","name":"n","auth":{"auth_type":"public"}}}`))
		// Fork base_repo through the production dispatcher: rejected before
		// the fork handler even looks at the (missing) source token.
		mismatch(t, claim, []byte(`{"repo_name":"other","base_repo":{"provider":"code","operation":"fork"}}`))
	})
}

// TestPerRepoRoutesRejectNonRepoClaims locks in the docs/reference/authentication.md failure map for
// per-repo routes: tokens whose repo claim is empty or "org" get 403 — never
// a 500 (regression: empty claims previously reached the store and panicked).
func TestPerRepoRoutesRejectNonRepoClaims(t *testing.T) {
	runBackends(t, testPerRepoRoutesRejectNonRepoClaims)
}

func testPerRepoRoutesRejectNonRepoClaims(t *testing.T, e *testEnv) {
	org := e.newOrg(t)

	routes := []struct {
		name   string
		method string
		path   string
		scope  string
	}{
		{"create", http.MethodPost, "/api/v1/repos", auth.ScopeRepoWrite},
		{"get", http.MethodGet, "/api/v1/repo", auth.ScopeGitRead},
		{"delete", http.MethodDelete, "/api/v1/repos/delete", auth.ScopeRepoWrite},
	}
	for _, rt := range routes {
		for _, claim := range []string{"", auth.RepoClaimOrg} {
			name := rt.name + "/claim=" + claim
			tok := e.mint(t, tokenSpec{iss: org, repo: claim, scopes: []string{rt.scope}})
			resp, body := e.doReq(t, rt.method, rt.path, tok, nil, "")
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s: status = %d, want 403 (body %s)", name, resp.StatusCode, body)
				continue
			}
			requireEnvelope(t, resp, body)
		}
	}
}

// TestRefPolicyOnWriteEndpoints covers docs/reference/authentication.md ref-policy enforcement on
// the API write path: repo deletion destroys every ref, so a token whose
// policies could deny deleting some ref must not delete the repo; malformed
// policy claims fail closed on both write endpoints; verify-sig-only and
// empty-ops policies never block (deletion introduces no commits).
func TestRefPolicyOnWriteEndpoints(t *testing.T) { runBackends(t, testRefPolicyOnWriteEndpoints) }

func testRefPolicyOnWriteEndpoints(t *testing.T, e *testEnv) {
	org := e.newOrg(t)

	deleteRepo := func(t *testing.T, spec tokenSpec, wantStatus int) {
		t.Helper()
		tok := e.mint(t, spec)
		resp, body := e.doReq(t, http.MethodDelete, "/api/v1/repos/delete", tok, nil, "")
		if resp.StatusCode != wantStatus {
			t.Fatalf("DELETE status = %d, want %d (body %s)", resp.StatusCode, wantStatus, body)
		}
		if wantStatus != http.StatusOK {
			requireEnvelope(t, resp, body)
		}
	}

	repoURL := "repo-" + randomHex(t, 8)
	createRepo(t, e, org, repoURL, nil, http.StatusCreated)

	// Malformed policy claims fail closed (403) on both write endpoints.
	badTok := e.mint(t, tokenSpec{iss: org, repo: "repo-" + randomHex(t, 8),
		scopes: []string{auth.ScopeRepoWrite}, refs: `["not-a-tuple"]`})
	resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos", badTok, nil, "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("create with malformed refs claim: status = %d, want 403 (body %s)", resp.StatusCode, body)
	}
	requireEnvelope(t, resp, body)
	deleteRepo(t, tokenSpec{iss: org, repo: repoURL,
		scopes: []string{auth.ScopeRepoWrite}, refs: `["not-a-tuple"]`}, http.StatusForbidden)

	// Any rule that could deny a ref deletion denies whole-repo deletion:
	// an explicit no-push rule, and the deprecated repo-wide ops fallback.
	deleteRepo(t, tokenSpec{iss: org, repo: repoURL,
		scopes: []string{auth.ScopeRepoWrite}, refs: `[["main", ["no-push"]]]`}, http.StatusForbidden)
	deleteRepo(t, tokenSpec{iss: org, repo: repoURL,
		scopes: []string{auth.ScopeRepoWrite}, ops: []string{"no-force-push"}}, http.StatusForbidden)

	// A restrictive-but-well-formed policy does not block repo creation
	// (a new repo has no refs to protect).
	createTok := e.mint(t, tokenSpec{iss: org, repo: "repo-" + randomHex(t, 8),
		scopes: []string{auth.ScopeRepoWrite}, refs: `[["*", ["no-push"]]]`})
	resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos", createTok, nil, "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create with no-push policy: status = %d, want 201 (body %s)", resp.StatusCode, body)
	}

	// verify-sig and empty ops never deny deletions.
	deleteRepo(t, tokenSpec{iss: org, repo: repoURL,
		scopes: []string{auth.ScopeRepoWrite}, refs: `[["main", ["verify-sig"]], ["*", []]]`}, http.StatusOK)
}

// TestCreateRepoDefaultBranchQueryParam covers the openapi.yaml POST /repos
// `default_branch` query parameter: used when the body does not specify a
// branch; the body wins when both are present.
func TestCreateRepoDefaultBranchQueryParam(t *testing.T) {
	runBackends(t, testCreateRepoDefaultBranchQueryParam)
}

func testCreateRepoDefaultBranchQueryParam(t *testing.T, e *testEnv) {
	org := e.newOrg(t)

	getBranch := func(repoURL string) string {
		t.Helper()
		readTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repo", readTok, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /repo status = %d (body %s)", resp.StatusCode, body)
		}
		var got repoEntry
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		return got.DefaultBranch
	}
	create := func(repoURL, query string, body []byte) {
		t.Helper()
		tok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
		resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos"+query, tok, body, "")
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create %s status = %d (body %s)", repoURL, resp.StatusCode, respBody)
		}
	}

	// Query param alone sets the branch.
	fromQuery := "q-" + randomHex(t, 4)
	create(fromQuery, "?default_branch=develop", nil)
	if got := getBranch(fromQuery); got != "develop" {
		t.Fatalf("query-only default_branch = %q, want develop", got)
	}
	// Body takes precedence over the query param.
	fromBody := "q-" + randomHex(t, 4)
	create(fromBody, "?default_branch=develop", []byte(`{"default_branch":"trunk"}`))
	if got := getBranch(fromBody); got != "trunk" {
		t.Fatalf("body-vs-query default_branch = %q, want trunk", got)
	}
}

func TestListPaginationAndQ(t *testing.T) { runBackends(t, testListPaginationAndQ) }

func testListPaginationAndQ(t *testing.T, e *testEnv) {
	org := e.newOrg(t) // fresh org => deterministic counts

	const total = 25
	urls := make(map[string]bool, total)
	for i := range total {
		// Mixed case to exercise the case-insensitive q filter.
		u := fmt.Sprintf("PgN-%02d-%s", i, randomHex(t, 3))
		createRepo(t, e, org, u, nil, http.StatusCreated)
		urls[strings.ToLower(u)] = true
	}
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})

	listPage := func(params url.Values, wantStatus int) listReposResponse {
		t.Helper()
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos?"+params.Encode(), orgTok, nil, "")
		if resp.StatusCode != wantStatus {
			t.Fatalf("GET /repos?%s status = %d, want %d (body %s)", params.Encode(), resp.StatusCode, wantStatus, body)
		}
		var list listReposResponse
		if wantStatus == http.StatusOK {
			if err := json.Unmarshal(body, &list); err != nil {
				t.Fatal(err)
			}
		} else {
			requireEnvelope(t, resp, body)
		}
		return list
	}

	// Walk 25 repos with limit 10: pages of 10/10/5.
	seen := make(map[string]bool)
	cursor := ""
	wantPages := []struct {
		count   int
		hasMore bool
	}{{10, true}, {10, true}, {5, false}}
	for i, want := range wantPages {
		params := url.Values{"limit": {"10"}}
		if cursor != "" {
			params.Set("cursor", cursor)
		}
		page := listPage(params, http.StatusOK)
		if len(page.Repos) != want.count || page.HasMore != want.hasMore {
			t.Fatalf("page %d: got %d repos hasMore=%v, want %d/%v",
				i, len(page.Repos), page.HasMore, want.count, want.hasMore)
		}
		if want.hasMore && page.NextCursor == "" {
			t.Fatalf("page %d: hasMore without next_cursor", i)
		}
		if !want.hasMore && page.NextCursor != "" {
			t.Fatalf("page %d: exhausted but next_cursor = %q", i, page.NextCursor)
		}
		if i == 0 && cursor == "" {
			// Cursor stability: re-fetching the same page yields identical rows.
			again := listPage(url.Values{"limit": {"10"}}, http.StatusOK)
			for j := range page.Repos {
				if again.Repos[j] != page.Repos[j] {
					t.Fatalf("page 0 not stable across fetches: %+v vs %+v", page.Repos[j], again.Repos[j])
				}
			}
		}
		for _, r := range page.Repos {
			if seen[r.RepoID] {
				t.Fatalf("repo %s returned on two pages", r.RepoID)
			}
			seen[r.RepoID] = true
			if !urls[strings.ToLower(r.URL)] {
				t.Fatalf("unexpected repo url %q in listing", r.URL)
			}
		}
		cursor = page.NextCursor
	}
	if len(seen) != total {
		t.Fatalf("walk saw %d repos, want %d", len(seen), total)
	}

	// Case-insensitive substring q: "pgn-0" matches PgN-00..PgN-09.
	page := listPage(url.Values{"q": {"pgn-0"}, "limit": {"100"}}, http.StatusOK)
	if len(page.Repos) != 10 || page.HasMore {
		t.Fatalf("q=pgn-0: got %d repos hasMore=%v, want 10/false", len(page.Repos), page.HasMore)
	}
	// LIKE metacharacters in q must not act as wildcards.
	page = listPage(url.Values{"q": {"%"}}, http.StatusOK)
	if len(page.Repos) != 0 {
		t.Fatalf("q=%%: matched %d repos, want 0", len(page.Repos))
	}
	// No match.
	page = listPage(url.Values{"q": {"no-such-repo"}}, http.StatusOK)
	if page.Repos == nil || len(page.Repos) != 0 {
		t.Fatalf("q miss: repos = %#v, want []", page.Repos)
	}

	// Invalid cursor / limit -> 400.
	listPage(url.Values{"cursor": {"!!!not-base64!!!"}}, http.StatusBadRequest)
	listPage(url.Values{"cursor": {base64.RawURLEncoding.EncodeToString([]byte("garbage"))}}, http.StatusBadRequest)
	listPage(url.Values{"limit": {"0"}}, http.StatusBadRequest)
	listPage(url.Values{"limit": {"101"}}, http.StatusBadRequest)
	listPage(url.Values{"limit": {"abc"}}, http.StatusBadRequest)
}

// TestListPrefix pins GET /api/v1/repos `prefix` (upstream spec 34ac6de1):
// trimmed, case-insensitive start of repo_name, and with `prefix` set `q`
// matches only the rest after it.
func TestListPrefix(t *testing.T) { runBackends(t, testListPrefix) }

func testListPrefix(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	for _, name := range []string{"pierre/sdk-documentation", "pierre/example", "Pierre/Upper-SDK", "acme/pierre-tools", "acme/sdk-tools"} {
		createRepo(t, e, org, name, nil, http.StatusCreated)
	}
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})

	// list walks every page and returns the sorted repo_names.
	list := func(params url.Values) []string {
		t.Helper()
		params = maps.Clone(params)
		var names []string
		for range 10 {
			resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos?"+params.Encode(), orgTok, nil, "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET /repos?%s status = %d (body %s)", params.Encode(), resp.StatusCode, body)
			}
			var page listReposResponse
			if err := json.Unmarshal(body, &page); err != nil {
				t.Fatal(err)
			}
			for _, r := range page.Repos {
				names = append(names, r.RepoName)
			}
			if !page.HasMore {
				slices.Sort(names)
				return names
			}
			params.Set("cursor", page.NextCursor)
		}
		t.Fatalf("GET /repos?%s did not end after 10 pages", params.Encode())
		return nil
	}

	for _, tc := range []struct {
		params url.Values
		want   []string
	}{
		{url.Values{"prefix": {"pierre/"}}, []string{"Pierre/Upper-SDK", "pierre/example", "pierre/sdk-documentation"}},
		{url.Values{"prefix": {"  PIERRE/ "}, "limit": {"1"}}, []string{"Pierre/Upper-SDK", "pierre/example", "pierre/sdk-documentation"}},
		{url.Values{"prefix": {"pierre/"}, "q": {"sdk"}}, []string{"Pierre/Upper-SDK", "pierre/sdk-documentation"}},
		{url.Values{"prefix": {"pierre/"}, "q": {"pierre"}}, nil},
		{url.Values{"prefix": {"acme/"}, "q": {"pierre"}}, []string{"acme/pierre-tools"}},
		{url.Values{"prefix": {"  "}, "limit": {"1"}}, []string{"Pierre/Upper-SDK", "acme/pierre-tools", "acme/sdk-tools", "pierre/example", "pierre/sdk-documentation"}},
		{url.Values{"prefix": {"pierre_"}}, nil},
	} {
		if got := list(tc.params); !slices.Equal(got, tc.want) {
			t.Errorf("GET /repos?%s -> %v, want %v", tc.params.Encode(), got, tc.want)
		}
	}
}

// TestListFilterValues pins q and prefix values that no repo_name holds:
// a value of 200 KiB lists no repo (SQLite refuses a LIKE pattern over
// 50,000 bytes), and a NUL byte or invalid UTF-8, which PostgreSQL refuses
// in a text parameter, is the client's 400.
func TestListFilterValues(t *testing.T) { runBackends(t, testListFilterValues) }

func testListFilterValues(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	createRepo(t, e, org, "acme/alpha", nil, http.StatusCreated)
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
	long := strings.Repeat("a", 200<<10)
	for _, tc := range []struct {
		name   string
		params url.Values
		status int
	}{
		{"q of 200 KiB", url.Values{"q": {long}}, http.StatusOK},
		{"prefix of 200 KiB", url.Values{"prefix": {long}}, http.StatusOK},
		{"q of 25,001 percent signs", url.Values{"q": {strings.Repeat("%", 25_001)}}, http.StatusOK},
		{"q with a NUL byte", url.Values{"q": {"a\x00b"}}, http.StatusBadRequest},
		{"prefix with a NUL byte", url.Values{"prefix": {"acme\x00"}}, http.StatusBadRequest},
		{"q not UTF-8", url.Values{"q": {"a\xffb"}}, http.StatusBadRequest},
		{"prefix not UTF-8", url.Values{"prefix": {"\xff"}}, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos?"+tc.params.Encode(), orgTok, nil, "")
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d (body %.300s), want %d", resp.StatusCode, body, tc.status)
			}
			if tc.status != http.StatusOK {
				return
			}
			var page listReposResponse
			if err := json.Unmarshal(body, &page); err != nil || len(page.Repos) != 0 || page.HasMore {
				t.Fatalf("body %.300s (err %v), want an empty last page", body, err)
			}
		})
	}
}

// TestAuthMatrix drives the docs/reference/http-api.md 401/403 failure map across the
// repo lifecycle endpoints (create and list on both path styles), asserting status,
// envelope shape, and WWW-Authenticate on 401.
func TestAuthMatrix(t *testing.T) { runBackends(t, testAuthMatrix) }

func testAuthMatrix(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	repoURL := "matrix-" + randomHex(t, 6)
	createRepo(t, e, org, repoURL, nil, http.StatusCreated)

	endpoints := []struct {
		name     string
		method   string
		path     string
		scope    string
		orgLevel bool
	}{
		{"create", http.MethodPost, "/api/v1/repos", auth.ScopeRepoWrite, false},
		{"get", http.MethodGet, "/api/v1/repo", auth.ScopeGitRead, false},
		{"list", http.MethodGet, "/api/v1/repos", auth.ScopeOrgRead, true},
		{"create canonical", http.MethodPost, "/api/repos", auth.ScopeRepoWrite, false},
		{"list canonical", http.MethodGet, "/api/repos", auth.ScopeOrgRead, true},
		{"delete", http.MethodDelete, "/api/v1/repos/delete", auth.ScopeRepoWrite, false},
	}

	// repoClaim returns the claim an endpoint's happy-path token would use.
	repoClaim := func(orgLevel bool) string {
		if orgLevel {
			return auth.RepoClaimOrg
		}
		return repoURL
	}

	// algNone builds an unsigned token (alg: none) with plausible claims.
	algNone := func(iss, repo string, scopes []string) string {
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
		payload, err := json.Marshal(map[string]any{
			"iss": iss, "sub": auth.SubjectLiteral, "repo": repo, "scopes": scopes,
			"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return header + "." + base64.RawURLEncoding.EncodeToString(payload) + "."
	}

	for _, ep := range endpoints {
		claim := repoClaim(ep.orgLevel)
		goodScopes := []string{ep.scope}

		cases := []struct {
			name       string
			token      string // "" = none
			authHeader string // verbatim override
			wantStatus int
			wantDetail string // "" = not checked
		}{
			{name: "no authorization header", wantStatus: http.StatusUnauthorized, wantDetail: "Authentication required"},
			{name: "basic auth header", authHeader: "Basic dXNlcjpwYXNz", wantStatus: http.StatusUnauthorized, wantDetail: "Authentication required"},
			{name: "bearer without token", authHeader: "Bearer ", wantStatus: http.StatusUnauthorized, wantDetail: "Authentication required"},
			{name: "garbage token", token: "garbage.token.value", wantStatus: http.StatusForbidden, wantDetail: "Invalid or expired token"},
			{
				name:       "expired token",
				token:      e.mint(t, tokenSpec{iss: org, repo: claim, scopes: goodScopes, expIn: -time.Minute}),
				wantStatus: http.StatusForbidden,
				wantDetail: "Invalid or expired token",
			},
			{
				name:       "alg none",
				token:      algNone(org, claim, goodScopes),
				wantStatus: http.StatusForbidden,
				wantDetail: "Invalid or expired token",
			},
			{
				name:       "HS256 signed",
				token:      e.mint(t, tokenSpec{alg: "HS256", iss: org, repo: claim, scopes: goodScopes}),
				wantStatus: http.StatusForbidden,
				wantDetail: "Invalid or expired token",
			},
			{
				name:       "signed by unregistered key",
				token:      e.mint(t, tokenSpec{iss: org, repo: claim, scopes: goodScopes, key: newUnregisteredES256Key(t)}),
				wantStatus: http.StatusForbidden,
				wantDetail: "Invalid or expired token",
			},
			{
				name:       "unknown issuer",
				token:      e.mint(t, tokenSpec{iss: "no-such-org", repo: claim, scopes: goodScopes}),
				wantStatus: http.StatusForbidden,
				wantDetail: "Invalid or expired token",
			},
			{
				name:       "empty sub",
				token:      e.mint(t, tokenSpec{iss: org, repo: claim, scopes: goodScopes, claims: map[string]any{"sub": ""}}),
				wantStatus: http.StatusForbidden,
				wantDetail: "Invalid or expired token",
			},
			{
				name:       "missing sub",
				token:      e.mint(t, tokenSpec{iss: org, repo: claim, scopes: goodScopes, claims: map[string]any{"sub": nil}}),
				wantStatus: http.StatusForbidden,
				wantDetail: "Invalid or expired token",
			},
			{
				name:       "repo claim with a NUL",
				token:      e.mint(t, tokenSpec{iss: org, repo: claim + "\x00x", scopes: goodScopes}),
				wantStatus: http.StatusForbidden,
				wantDetail: "Invalid or expired token",
			},
			{
				name:       "missing required scope",
				token:      e.mint(t, tokenSpec{iss: org, repo: claim, scopes: []string{auth.ScopeGitWrite}}),
				wantStatus: http.StatusForbidden,
			},
		}
		if ep.orgLevel {
			cases = append(cases, struct {
				name       string
				token      string
				authHeader string
				wantStatus int
				wantDetail string
			}{
				name:       "repo claim not org",
				token:      e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: goodScopes}),
				wantStatus: http.StatusForbidden,
			})
		}

		for _, tc := range cases {
			t.Run(ep.name+"/"+tc.name, func(t *testing.T) {
				resp, body := e.doReq(t, ep.method, ep.path, tc.token, nil, tc.authHeader)
				if resp.StatusCode != tc.wantStatus {
					t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, tc.wantStatus, body)
				}
				if msg := requireEnvelope(t, resp, body); tc.wantDetail != "" && msg != tc.wantDetail {
					t.Fatalf("detail = %q, want %q", msg, tc.wantDetail)
				}
			})
		}
	}

	// 404s: valid tokens for a repo that does not exist.
	missing := "never-created-" + randomHex(t, 6)
	for _, tc := range []struct {
		name, method, path string
		scopes             []string
	}{
		{"get nonexistent", http.MethodGet, "/api/v1/repo", []string{auth.ScopeGitRead}},
		{"delete nonexistent", http.MethodDelete, "/api/v1/repos/delete", []string{auth.ScopeRepoWrite}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tok := e.mint(t, tokenSpec{iss: org, repo: missing, scopes: tc.scopes})
			resp, body := e.doReq(t, tc.method, tc.path, tok, nil, "")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (body %s)", resp.StatusCode, body)
			}
			requireEnvelope(t, resp, body)
		})
	}
}

// TestJWTPerAlgorithmPositive proves a token signed with each allowed
// algorithm's registered key authenticates (docs/reference/authentication.md allow-list).
func TestJWTPerAlgorithmPositive(t *testing.T) { runBackends(t, testJWTPerAlgorithmPositive) }

func testJWTPerAlgorithmPositive(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	repoURL := "alg-" + randomHex(t, 6)
	createRepo(t, e, org, repoURL, nil, http.StatusCreated)

	for _, alg := range auth.AllowedAlgorithms {
		t.Run(alg, func(t *testing.T) {
			tok := e.mint(t, tokenSpec{alg: alg, iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
			resp, body := e.doReq(t, http.MethodGet, "/api/v1/repo", tok, nil, "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s token rejected: status = %d (body %s)", alg, resp.StatusCode, body)
			}
		})
	}
}
