package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestRepoClaimWithNUL: a repo claim with a NUL names no new repository
// (400 on create) and no PostgreSQL repository (404, not the 500 of
// PostgreSQL's SQLSTATE 22021). A row that a backend stored before keeps
// answering to its claim.
func TestRepoClaimWithNUL(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		claim := "team/a\x00b"
		write := e.mint(t, tokenSpec{iss: org, repo: claim, scopes: []string{auth.ScopeRepoWrite}})
		read := e.mint(t, tokenSpec{iss: org, repo: claim, scopes: []string{auth.ScopeGitRead}})

		for _, path := range []string{"/api/v1/repos", "/api/repos"} {
			resp, body := e.doReq(t, http.MethodPost, path, write, nil, "")
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("POST %s = %d (%s), want 400", path, resp.StatusCode, body)
			}
			if msg := requireEnvelope(t, resp, body); msg != "invalid repo claim (want UTF-8 text without NUL)" {
				t.Fatalf("POST %s detail = %q", path, msg)
			}
		}
		resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos", write,
			[]byte(`{"base_repo": {"provider": "code", "operation": "fork", "name": "x"}}`), "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("POST fork form = %d (%s), want 400", resp.StatusCode, body)
		}

		for _, rq := range []struct{ method, path, tok string }{
			{http.MethodGet, "/api/v1/repo", read},
			{http.MethodGet, "/api/v1/repos/branches", read},
			{http.MethodDelete, "/api/v1/repos/delete", write},
		} {
			resp, body := e.doReq(t, rq.method, rq.path, rq.tok, nil, "")
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("%s %s = %d (%s), want 404", rq.method, rq.path, resp.StatusCode, body)
			}
		}

		// SQLite stores such a url (PostgreSQL refuses it): a repository
		// created before this check still answers to its claim.
		if _, err := e.store.CreateRepo(context.Background(), org, claim, "main"); err != nil {
			return
		}
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repo", read, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("legacy repo GET = %d (%s), want 200", resp.StatusCode, body)
		}
		resp, body = e.doReq(t, http.MethodDelete, "/api/v1/repos/delete", write, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("legacy repo DELETE = %d (%s), want 200", resp.StatusCode, body)
		}
	})
}
