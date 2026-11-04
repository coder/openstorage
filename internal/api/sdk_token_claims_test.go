package api

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestSDKTokenClaims pins the claims that SDK 0.1.0 signs (issue #658): any
// non-empty `sub` authenticates, and a token without a `repo` claim is
// org-wide. Such a token passes org-level routes and is refused on
// repo-scoped routes with the same answer as a `repo: "org"` token.
func TestSDKTokenClaims(t *testing.T) { runBackends(t, testSDKTokenClaims) }

func testSDKTokenClaims(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	repoURL := "sdk-claims-" + randomHex(t, 6)
	createRepo(t, e, org, repoURL, nil, http.StatusCreated)
	noRepo := map[string]any{"repo": nil}

	for _, sub := range []string{"pierre-sdk-go", "pierre-sdk-python", "pierre-sdk-typescript", "ci-pipeline-prod"} {
		t.Run(sub, func(t *testing.T) {
			for _, req := range []struct {
				method, path string
				spec         tokenSpec
			}{
				{http.MethodGet, "/api/meta", tokenSpec{scopes: []string{auth.ScopeOrgRead}, claims: noRepo}},
				{http.MethodGet, "/api/repos", tokenSpec{scopes: []string{auth.ScopeOrgRead}, claims: noRepo}},
				{http.MethodGet, "/api/v1/repo", tokenSpec{repo: repoURL, scopes: []string{auth.ScopeGitRead}}},
			} {
				req.spec.iss, req.spec.sub = org, sub
				resp, body := e.doReq(t, req.method, req.path, e.mint(t, req.spec), nil, "")
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("%s %s: status = %d, want 200 (body %s)", req.method, req.path, resp.StatusCode, body)
				}
			}
		})
	}

	// A repo-scoped route answers a token without `repo` exactly as it
	// answers `repo: "org"`.
	for _, req := range []struct {
		method, path, scope string
		body                []byte
	}{
		{http.MethodGet, "/api/v1/repo", auth.ScopeGitRead, nil},
		{http.MethodPost, "/api/repos", auth.ScopeRepoWrite, []byte(`{}`)},
		{http.MethodDelete, "/api/v1/repos/delete", auth.ScopeRepoWrite, nil},
	} {
		t.Run("repo route "+req.method+" "+req.path, func(t *testing.T) {
			orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{req.scope}})
			wantResp, wantBody := e.doReq(t, req.method, req.path, orgTok, req.body, "")
			if wantResp.StatusCode != http.StatusForbidden {
				t.Fatalf("repo org: status = %d, want 403 (body %s)", wantResp.StatusCode, wantBody)
			}
			noRepoTok := e.mint(t, tokenSpec{iss: org, sub: "pierre-sdk-go", scopes: []string{req.scope}, claims: noRepo})
			resp, body := e.doReq(t, req.method, req.path, noRepoTok, req.body, "")
			if resp.StatusCode != http.StatusForbidden || !bytes.Equal(body, wantBody) {
				t.Fatalf("no repo claim: status = %d body %s, want 403 body %s", resp.StatusCode, body, wantBody)
			}
			requireEnvelope(t, resp, body)
		})
	}

	// A present but empty `repo` is not org-wide.
	tok := e.mint(t, tokenSpec{iss: org, repo: "", scopes: []string{auth.ScopeOrgRead}})
	if resp, body := e.doReq(t, http.MethodGet, "/api/repos", tok, nil, ""); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("repo \"\": status = %d, want 403 (body %s)", resp.StatusCode, body)
	}
}
