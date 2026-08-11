package spike

// Unit tests for Server.authenticate's repo-claim matching: docs/05 git URLs
// address repos by repo_id while docs/02 JWT repo claims carry the url, so
// authenticate must resolve the path element when a resolver is wired.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuthenticate_ResolvesRepoIDToURLClaim(t *testing.T) {
	priv, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	const (
		repoID  = "repo_abc123def456"
		repoURL = "team/project-alpha"
	)
	token, err := MintToken(priv, "spike", repoURL, []string{ScopeGitRead}, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/"+repoID+".git/info/refs", nil)
		req.SetBasicAuth("t", token)
		return req
	}
	srv := &Server{PubKey: &priv.PublicKey}

	// Without a resolver, a url claim cannot match a repo_id path.
	if _, status, err := srv.authenticate(newReq(), repoID, ScopeGitRead); err == nil || status != http.StatusForbidden {
		t.Fatalf("no resolver: status = %d, err = %v; want 403", status, err)
	}

	srv.ResolveRepoURL = func(_ context.Context, id string) (string, bool) {
		if id == repoID {
			return repoURL, true
		}
		return "", false
	}

	// Resolver maps the repo_id to the claimed url: authenticated.
	claims, status, err := srv.authenticate(newReq(), repoID, ScopeGitRead)
	if err != nil {
		t.Fatalf("resolved auth failed: status = %d, err = %v", status, err)
	}
	if claims.Repo != repoURL {
		t.Fatalf("claims.Repo = %q, want %q", claims.Repo, repoURL)
	}

	// Namespace suffixes are stripped before resolution (docs/05).
	if _, status, err := srv.authenticate(newReq(), repoID+"+ephemeral", ScopeGitRead); err != nil {
		t.Fatalf("namespaced auth failed: status = %d, err = %v", status, err)
	}

	// Unknown repo_id stays 403.
	if _, status, err := srv.authenticate(newReq(), "repo_unknown", ScopeGitRead); err == nil || status != http.StatusForbidden {
		t.Fatalf("unknown id: status = %d, err = %v; want 403", status, err)
	}

	// Verbatim claim==path matching still works without a resolver
	// (spike-style repos addressed by name).
	verbatim, err := MintToken(priv, "spike", "plainrepo", []string{ScopeGitRead}, "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/plainrepo.git/info/refs", nil)
	req.SetBasicAuth("t", verbatim)
	if _, status, err := (&Server{PubKey: &priv.PublicKey}).authenticate(req, "plainrepo", ScopeGitRead); err != nil {
		t.Fatalf("verbatim auth failed: status = %d, err = %v", status, err)
	}
}
