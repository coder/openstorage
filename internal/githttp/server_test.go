package githttp

// Unit tests for Server.authenticate: docs/reference/authentication.md failure mapping on the git
// path (401 for any Verifier failure, 403 for org/repo/scope mismatch) and
// the docs/explanation/git-protocol-and-namespaces.md repo_id->url resolution. Uses the real auth.OrgKeyVerifier
// over an in-memory key source — no containers needed.

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
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
		// `sub` is free text (issue #658): an SDK 0.1.0 subject passes, an
		// empty one does not.
		{"sdk subject", "t", mintUnitToken(t, orgKey, "ES256", func() auth.Claims {
			c := unitClaims("org1", repo, auth.ScopeGitRead)
			c.Sub = "pierre-sdk-typescript"
			return c
		}()), auth.ScopeGitRead, 0},
		{"empty subject", "t", mintUnitToken(t, orgKey, "ES256", func() auth.Claims {
			c := unitClaims("org1", repo, auth.ScopeGitRead)
			c.Sub = ""
			return c
		}()), auth.ScopeGitRead, http.StatusUnauthorized},
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

// A token with the org-level sentinel (or an empty, or no) `repo` claim must
// never pass the git repo gate — not even for a repo literally named "org",
// which would otherwise satisfy the verbatim claim/target compare.
func TestAuthenticate_RejectsNonRepoScopedClaims(t *testing.T) {
	key, pemBlock := genKeyPEM(t, "ES256")
	srv := &Server{Verifier: auth.NewVerifier(memKeys{"org1": {pemBlock}}), Org: "org1"}
	tokens := map[string]string{}
	for _, claim := range []string{auth.RepoClaimOrg, ""} {
		tokens["repo "+claim] = mintUnitToken(t, key, "ES256", unitClaims("org1", claim, auth.ScopeGitRead))
	}
	// SDK 0.1.0 omits `repo` for org-wide tokens (issue #658).
	now := time.Now()
	payload, err := json.Marshal(map[string]any{
		"iss": "org1", "sub": "pierre-sdk-go", "scopes": []string{auth.ScopeGitRead},
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	signed, err := jws.Sign(payload, jws.WithKey(jwa.ES256(), key))
	if err != nil {
		t.Fatal(err)
	}
	tokens["no repo claim"] = string(signed)
	for name, token := range tokens {
		_, status, err := srv.authenticate(authReq("/org.git/info/refs", "t", token), "org", auth.ScopeGitRead)
		if err == nil || status != http.StatusForbidden {
			t.Fatalf("%s: status = %d, err = %v; want 403", name, status, err)
		}
	}
}

// The git path must accept every allowed signature algorithm the org
// registered keys for, not just ES256 (docs/reference/authentication.md multi-alg requirement).
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
	rec := meta.Repo{RepoID: repoID, URL: repoURL}
	token := mintUnitToken(t, key, "ES256", unitClaims("org1", repoURL, auth.ScopeGitRead))
	newReq := func() *http.Request { return authReq("/"+repoID+".git/info/refs", "t", token) }
	srv := &Server{Verifier: verifier, Org: "org1"}

	// Without a resolver, a url claim cannot match a repo_id path.
	if _, status, err := srv.authenticate(newReq(), repoID, auth.ScopeGitRead); err == nil || status != http.StatusForbidden {
		t.Fatalf("no resolver: status = %d, err = %v; want 403", status, err)
	}

	srv.ResolveRepo = func(_ context.Context, base string) (meta.Repo, bool) {
		if base == repoID {
			return rec, true
		}
		return meta.Repo{}, false
	}

	// Resolver maps the repo_id path to a record whose url matches the
	// claim: authenticated.
	claims, status, err := srv.authenticate(newReq(), repoID, auth.ScopeGitRead)
	if err != nil {
		t.Fatalf("resolved auth failed: status = %d, err = %v", status, err)
	}
	if claims.Repo != repoURL {
		t.Fatalf("claims.Repo = %q, want %q", claims.Repo, repoURL)
	}

	// Namespace suffixes are stripped before resolution (docs/explanation/git-protocol-and-namespaces.md).
	if _, status, err := srv.authenticate(newReq(), repoID+"+ephemeral", auth.ScopeGitRead); err != nil {
		t.Fatalf("namespaced auth failed: status = %d, err = %v", status, err)
	}

	// Unknown repo_id stays 403.
	if _, status, err := srv.authenticate(newReq(), "repo_unknown", auth.ScopeGitRead); err == nil || status != http.StatusForbidden {
		t.Fatalf("unknown id: status = %d, err = %v; want 403", status, err)
	}

	// The reverse direction: a repo_id claim matches a url-style path
	// element through the same record (ADR-21 accepts both key forms).
	idToken := mintUnitToken(t, key, "ES256", unitClaims("org1", repoID, auth.ScopeGitRead))
	srvByURL := &Server{Verifier: verifier, Org: "org1", ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
		if base == "project-alpha" {
			return rec, true
		}
		return meta.Repo{}, false
	}}
	req := authReq("/project-alpha.git/info/refs", "t", idToken)
	if _, status, err := srvByURL.authenticate(req, "project-alpha", auth.ScopeGitRead); err != nil {
		t.Fatalf("id-claim/url-path auth failed: status = %d, err = %v", status, err)
	}

	// Verbatim claim==path matching still works without a resolver.
	verbatim := mintUnitToken(t, key, "ES256", unitClaims("org1", "plainrepo", auth.ScopeGitRead))
	req = authReq("/plainrepo.git/info/refs", "t", verbatim)
	if _, status, err := (&Server{Verifier: verifier, Org: "org1"}).authenticate(req, "plainrepo", auth.ScopeGitRead); err != nil {
		t.Fatalf("verbatim auth failed: status = %d, err = %v", status, err)
	}
}

// TestAuthenticate_ResolveRepoCanonicalizes pins the repo-key-split fix
// (ADR-21): resolveTarget maps any accepted path element to the canonical
// RepoID(+ephemeral) storage key (the push-only `+import` remote keys the
// base RepoID), and unresolvable elements (including every
// element when no resolver is wired) are not targets at all — the handlers
// answer 404 instead of keying storage by the raw path element.
func TestAuthenticate_ResolveRepoCanonicalizes(t *testing.T) {
	rec := meta.Repo{RepoID: "repo_abc123def456", URL: "project-alpha"}
	srv := &Server{ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
		if base == rec.RepoID || base == rec.URL {
			return rec, true
		}
		return meta.Repo{}, false
	}}

	for pathElem, wantKey := range map[string]string{
		rec.URL:                   rec.RepoID,
		rec.RepoID:                rec.RepoID,
		rec.URL + "+ephemeral":    rec.RepoID + "+ephemeral",
		rec.RepoID + "+import":    rec.RepoID,
		rec.URL + "+import":       rec.RepoID,
		rec.RepoID + "+ephemeral": rec.RepoID + "+ephemeral",
	} {
		target, ok := srv.resolveTarget(context.Background(), pathElem)
		if !ok {
			t.Fatalf("resolveTarget(%q): not resolved", pathElem)
		}
		if target.key != wantKey {
			t.Fatalf("resolveTarget(%q).key = %q, want %q", pathElem, target.key, wantKey)
		}
		if target.repo.RepoID != rec.RepoID {
			t.Fatalf("resolveTarget(%q).repo = %+v, want %+v", pathElem, target.repo, rec)
		}
		if want := strings.HasSuffix(pathElem, "+import"); target.viaImport != want {
			t.Fatalf("resolveTarget(%q).viaImport = %v, want %v", pathElem, target.viaImport, want)
		}
	}

	if _, ok := srv.resolveTarget(context.Background(), "unregistered"); ok {
		t.Fatal("unregistered element must not resolve")
	}
	if _, ok := (&Server{}).resolveTarget(context.Background(), "anything"); ok {
		t.Fatal("nil resolver must resolve nothing")
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
	req.Header.Set("X-Openstorage-Test-Crash", "after-s3")
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

// A gzip negotiation that stays under both byte caps but withholds EOF must
// not pin the connection indefinitely. The deadline fires before hydration,
// returns a structured 408, and is test-overridden to keep the unit fast.
func TestServiceRPC_UploadPackReadTimeoutIs408(t *testing.T) {
	key, pemBlock := genKeyPEM(t, "ES256")
	repo := meta.Repo{RepoID: "repo_timeout1234", URL: "fetch-timeout"}
	srv := &Server{
		Verifier: auth.NewVerifier(memKeys{"org1": {pemBlock}}),
		Org:      "org1",
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			return repo, base == repo.URL
		},
		uploadPackReadTimeout: 100 * time.Millisecond,
	}
	httpServer := httptest.NewServer(srv.Handler())
	defer httpServer.Close()
	token := mintUnitToken(t, key, "ES256", unitClaims("org1", repo.URL, auth.ScopeGitRead))

	var member bytes.Buffer
	zw := gzip.NewWriter(&member)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		prefix []byte
	}{
		{"partial-header", []byte{0x1f}}, // gzip.NewReader blocks in its fixed header
		{"between-members", member.Bytes()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pr, pw := io.Pipe()
			release := make(chan struct{})
			writerDone := make(chan struct{})
			go func() {
				defer close(writerDone)
				_, _ = pw.Write(tc.prefix)
				<-release // withhold the rest/EOF until the server has answered
				_ = pw.Close()
			}()

			req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/fetch-timeout.git/git-upload-pack", pr)
			if err != nil {
				t.Fatal(err)
			}
			req.SetBasicAuth("t", token)
			req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
			req.Header.Set("Content-Encoding", "gzip")
			resp, err := httpServer.Client().Do(req)
			close(release)
			<-writerDone
			if err != nil {
				t.Fatalf("POST git-upload-pack: %v", err)
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			if resp.StatusCode != http.StatusRequestTimeout {
				t.Fatalf("status = %d, want 408; body: %s", resp.StatusCode, got)
			}
			var body map[string]string
			if err := json.Unmarshal(got, &body); err != nil {
				t.Fatalf("decode JSON error: %v; body: %s", err, got)
			}
			if body["error"] != "fetch negotiation body timed out" {
				t.Fatalf("error = %q, want timeout message", body["error"])
			}
		})
	}
}
