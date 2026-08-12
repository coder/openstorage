package api

// Integration tests for GET /api/repo-urls/{repo_id} (docs/03, openapi
// get_repo_url_by_id; note: no /v1 prefix): 200 {"repo_id","url"} shape,
// 404 for unknown / cross-org / soft-deleted IDs, org-level auth (403 for
// repo-scoped tokens and missing org:read scope), 401 without a token.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"codestorage/internal/auth"
)

func TestRepoURLByID(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		ctx := context.Background()
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/ru-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(ctx, org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
		path := "/api/repo-urls/" + url.PathEscape(repo.RepoID)

		// 200: exactly {"repo_id","url"} (openapi: additionalProperties false).
		resp, body := e.doReq(t, http.MethodGet, path, orgTok, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d (%s), want 200", path, resp.StatusCode, body)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		var got map[string]string
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("unmarshal %q: %v", body, err)
		}
		if len(got) != 2 || got["repo_id"] != repo.RepoID || got["url"] != repoURL {
			t.Fatalf("body = %q, want exactly {repo_id: %q, url: %q}", body, repo.RepoID, repoURL)
		}

		// 404: unknown id.
		resp, body = e.doReq(t, http.MethodGet, "/api/repo-urls/no-such-id", orgTok, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("unknown id = %d (%s), want 404", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// 404: a valid id from another org must be invisible.
		otherOrg := e.newOrg(t)
		otherTok := e.mint(t, tokenSpec{iss: otherOrg, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
		resp, body = e.doReq(t, http.MethodGet, path, otherTok, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("cross-org lookup = %d (%s), want 404", resp.StatusCode, body)
		}

		// 403: repo-scoped token on the org-level route.
		repoTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeOrgRead}})
		resp, body = e.doReq(t, http.MethodGet, path, repoTok, nil, "")
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("repo-scoped token = %d (%s), want 403", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// 403: org token missing the org:read scope.
		noScopeTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeGitRead}})
		resp, body = e.doReq(t, http.MethodGet, path, noScopeTok, nil, "")
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("missing org:read = %d (%s), want 403", resp.StatusCode, body)
		}

		// 401: no token (requireEnvelope also asserts WWW-Authenticate).
		resp, body = e.doReq(t, http.MethodGet, path, "", nil, "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("no token = %d (%s), want 401", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)

		// 404: soft-deleted repos are gone (last: mutates the fixture).
		if _, err := e.store.SoftDeleteRepo(ctx, org, repoURL); err != nil {
			t.Fatalf("soft-delete repo: %v", err)
		}
		resp, body = e.doReq(t, http.MethodGet, path, orgTok, nil, "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("soft-deleted id = %d (%s), want 404", resp.StatusCode, body)
		}
		requireEnvelope(t, resp, body)
	})
}
