package api

import (
	"context"
	"net/http"
	"net/url"
	"strings"
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
		// The claim check runs before the fork dispatch, not only in the
		// fallback creator: the fork and copy forms get the same detail.
		if msg := requireEnvelope(t, resp, body); msg != "invalid repo claim (want UTF-8 text without NUL)" {
			t.Fatalf("POST fork form detail = %q", msg)
		}
		resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos", write,
			[]byte(`{"base_repo": {"name": "x", "auth": {"token": "t"}}}`), "")
		if msg := requireEnvelope(t, resp, body); resp.StatusCode != http.StatusBadRequest ||
			msg != "invalid repo claim (want UTF-8 text without NUL)" {
			t.Fatalf("POST copy form = %d %q, want 400 with the claim detail", resp.StatusCode, msg)
		}

		// Valid text with control characters and non-ASCII still creates.
		odd := e.mint(t, tokenSpec{iss: org, repo: "team/c\x01\x7fé\uFFFD", scopes: []string{auth.ScopeRepoWrite}})
		resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos", odd, nil, "")
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("POST with an unusual valid claim = %d (%s), want 201", resp.StatusCode, body)
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
		// created before this check still answers to its claim, through
		// direct url lookups and through ResolveRepoKey alike.
		_, err := e.store.CreateRepo(context.Background(), org, claim, "main")
		if !strings.HasSuffix(t.Name(), "/sqlite") {
			if err == nil {
				t.Fatalf("CreateRepo(%q) succeeded on %s, want PostgreSQL's refusal", claim, t.Name())
			}
			return
		}
		if err != nil {
			t.Fatalf("CreateRepo(%q) on sqlite: %v", claim, err)
		}
		for _, path := range []string{"/api/v1/repo", "/api/v1/repos/branches", "/api/repos/" + url.PathEscape(claim)} {
			resp, body = e.doReq(t, http.MethodGet, path, read, nil, "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("legacy repo GET %s = %d (%s), want 200", path, resp.StatusCode, body)
			}
		}
		resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos", write, nil, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("legacy repo repeat create = %d (%s), want 400", resp.StatusCode, body)
		}
		resp, body = e.doReq(t, http.MethodDelete, "/api/v1/repos/delete", write, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("legacy repo DELETE = %d (%s), want 200", resp.StatusCode, body)
		}
	})
}
