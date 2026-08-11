package githttp

// Unit tests for Server.authenticate: docs/02 failure mapping on the git
// path (401 for any Verifier failure, 403 for org/repo/scope mismatch) and
// the docs/05 repo_id->url resolution. Uses the real auth.OrgKeyVerifier
// over an in-memory key source — no containers needed.

import (
	"context"
	"crypto"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codestorage/internal/auth"
)

// memKeys is an in-memory auth.KeySource: org name -> PEM public key blocks.
type memKeys map[string][]string

func (m memKeys) PublicKeysForOrg(_ context.Context, name string) ([]string, error) {
	pems, ok := m[name]
	if !ok {
		return nil, errors.New("unknown org")
	}
	return pems, nil
}

func genKeyPEM(t *testing.T, alg string) (crypto.Signer, string) {
	t.Helper()
	key, err := auth.GenerateKey(alg)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes, err := auth.MarshalPublicKeyPEM(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	return key, string(pemBytes)
}

func mintUnitToken(t *testing.T, key crypto.Signer, alg string, c auth.Claims) string {
	t.Helper()
	token, err := auth.MintToken(key, alg, c)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func unitClaims(iss, repo string, scopes ...string) auth.Claims {
	now := time.Now()
	return auth.Claims{
		Iss:    iss,
		Sub:    auth.SubjectLiteral,
		Repo:   repo,
		Scopes: scopes,
		Iat:    now.Unix(),
		Exp:    now.Add(time.Hour).Unix(),
	}
}

func authReq(path, user, token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.SetBasicAuth(user, token)
	return req
}

func TestAuthenticate_FailureMapping(t *testing.T) {
	orgKey, orgPEM := genKeyPEM(t, "ES256")
	otherKey, otherPEM := genKeyPEM(t, "ES256")
	verifier := auth.NewVerifier(memKeys{"org1": {orgPEM}, "org2": {otherPEM}})
	srv := &Server{Verifier: verifier, Org: "org1"}

	const repo = "myrepo"
	okToken := mintUnitToken(t, orgKey, "ES256", unitClaims("org1", repo, auth.ScopeGitRead))

	cases := []struct {
		name       string
		user       string
		token      string
		scope      string
		wantStatus int
	}{
		{"ok", "t", okToken, auth.ScopeGitRead, 0},
		{"wrong basic user", "x", okToken, auth.ScopeGitRead, http.StatusUnauthorized},
		{"garbage token", "t", "not-a-jwt", auth.ScopeGitRead, http.StatusUnauthorized},
		{"expired token", "t", mintUnitToken(t, orgKey, "ES256", func() auth.Claims {
			c := unitClaims("org1", repo, auth.ScopeGitRead)
			c.Exp = time.Now().Add(-time.Minute).Unix()
			return c
		}()), auth.ScopeGitRead, http.StatusUnauthorized},
		{"unknown issuer", "t", mintUnitToken(t, orgKey, "ES256", unitClaims("ghost", repo, auth.ScopeGitRead)),
			auth.ScopeGitRead, http.StatusUnauthorized},
		// Valid signature for a different REGISTERED org: 403, not 401.
		{"other org token", "t", mintUnitToken(t, otherKey, "ES256", unitClaims("org2", repo, auth.ScopeGitRead)),
			auth.ScopeGitRead, http.StatusForbidden},
		{"repo claim mismatch", "t", mintUnitToken(t, orgKey, "ES256", unitClaims("org1", "otherrepo", auth.ScopeGitRead)),
			auth.ScopeGitRead, http.StatusForbidden},
		{"missing scope", "t", okToken, auth.ScopeGitWrite, http.StatusForbidden},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			claims, status, err := srv.authenticate(authReq("/"+repo+".git/info/refs", c.user, c.token), repo, c.scope)
			if c.wantStatus == 0 {
				if err != nil {
					t.Fatalf("authenticate: status = %d, err = %v; want success", status, err)
				}
				if claims.Iss != "org1" || claims.Repo != repo {
					t.Fatalf("claims = %+v", claims)
				}
				return
			}
			if err == nil || status != c.wantStatus {
				t.Fatalf("authenticate: status = %d, err = %v; want %d", status, err, c.wantStatus)
			}
		})
	}
}

// The git path must accept every allowed signature algorithm the org
// registered keys for, not just ES256 (docs/02 multi-alg requirement).
func TestAuthenticate_MultiAlgorithm(t *testing.T) {
	for _, alg := range []string{"ES256", "EdDSA", "RS256"} {
		t.Run(alg, func(t *testing.T) {
			key, pemBlock := genKeyPEM(t, alg)
			srv := &Server{Verifier: auth.NewVerifier(memKeys{"org1": {pemBlock}}), Org: "org1"}
			token := mintUnitToken(t, key, alg, unitClaims("org1", "r1", auth.ScopeGitRead))
			if _, status, err := srv.authenticate(authReq("/r1.git/info/refs", "t", token), "r1", auth.ScopeGitRead); err != nil {
				t.Fatalf("status = %d, err = %v", status, err)
			}
		})
	}
}

func TestAuthenticate_ResolvesRepoIDToURLClaim(t *testing.T) {
	key, pemBlock := genKeyPEM(t, "ES256")
	verifier := auth.NewVerifier(memKeys{"org1": {pemBlock}})
	const (
		repoID  = "repo_abc123def456"
		repoURL = "team/project-alpha"
	)
	token := mintUnitToken(t, key, "ES256", unitClaims("org1", repoURL, auth.ScopeGitRead))
	newReq := func() *http.Request { return authReq("/"+repoID+".git/info/refs", "t", token) }
	srv := &Server{Verifier: verifier, Org: "org1"}

	// Without a resolver, a url claim cannot match a repo_id path.
	if _, status, err := srv.authenticate(newReq(), repoID, auth.ScopeGitRead); err == nil || status != http.StatusForbidden {
		t.Fatalf("no resolver: status = %d, err = %v; want 403", status, err)
	}

	srv.ResolveRepoURL = func(_ context.Context, id string) (string, bool) {
		if id == repoID {
			return repoURL, true
		}
		return "", false
	}

	// Resolver maps the repo_id to the claimed url: authenticated.
	claims, status, err := srv.authenticate(newReq(), repoID, auth.ScopeGitRead)
	if err != nil {
		t.Fatalf("resolved auth failed: status = %d, err = %v", status, err)
	}
	if claims.Repo != repoURL {
		t.Fatalf("claims.Repo = %q, want %q", claims.Repo, repoURL)
	}

	// Namespace suffixes are stripped before resolution (docs/05).
	if _, status, err := srv.authenticate(newReq(), repoID+"+ephemeral", auth.ScopeGitRead); err != nil {
		t.Fatalf("namespaced auth failed: status = %d, err = %v", status, err)
	}

	// Unknown repo_id stays 403.
	if _, status, err := srv.authenticate(newReq(), "repo_unknown", auth.ScopeGitRead); err == nil || status != http.StatusForbidden {
		t.Fatalf("unknown id: status = %d, err = %v; want 403", status, err)
	}

	// Verbatim claim==path matching still works without a resolver.
	verbatim := mintUnitToken(t, key, "ES256", unitClaims("org1", "plainrepo", auth.ScopeGitRead))
	req := authReq("/plainrepo.git/info/refs", "t", verbatim)
	if _, status, err := (&Server{Verifier: verifier, Org: "org1"}).authenticate(req, "plainrepo", auth.ScopeGitRead); err != nil {
		t.Fatalf("verbatim auth failed: status = %d, err = %v", status, err)
	}
}

// The crash-injection header must be refused outside test mode and must
// never leak into hook env (fault injection in production would be a
// data-loss hazard).
func TestServiceRPC_CrashHeaderRefusedOutsideTestMode(t *testing.T) {
	key, pemBlock := genKeyPEM(t, "ES256")
	srv := &Server{
		// Store/Blob stay nil: the refusal must trigger before hydration,
		// and an unexpected pass-through then fails loudly.
		CacheDir: t.TempDir(),
		Verifier: auth.NewVerifier(memKeys{"org1": {pemBlock}}),
		Org:      "org1",
		TestMode: false,
	}
	token := mintUnitToken(t, key, "ES256", unitClaims("org1", "crash-refuse", auth.ScopeGitWrite))
	req := httptest.NewRequest(http.MethodPost, "/crash-refuse.git/git-receive-pack", nil)
	req.SetBasicAuth("t", token)
	req.Header.Set(CrashHeader, "after-s3")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body: %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
		t.Fatalf("expected JSON error body, got %q (err %v)", rec.Body.String(), err)
	}
}
