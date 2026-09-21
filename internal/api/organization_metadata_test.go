package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

func TestOrganizationMetadata(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		foreignOrg := e.newOrg(t)
		h, err := New(Config{
			Store: e.store, Verifier: auth.NewVerifier(e.store), Logger: discardLogger(),
			Org: org, HTTPBaseURL: testHTTPBaseURL, RefStore: e.refStore,
			BlobStore: e.blobStore, CacheDir: e.cacheDir, SyncQueue: envSyncQueue{},
		})
		if err != nil {
			t.Fatal(err)
		}
		orgRead := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
		cases := []struct {
			name, token string
			status      int
		}{
			{"org read", orgRead, http.StatusOK},
			{"unauthenticated", "", http.StatusUnauthorized},
			{"invalid token", "invalid", http.StatusUnauthorized},
			{"missing scope", e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeGitRead}}), http.StatusForbidden},
			{"wrong org", e.mint(t, tokenSpec{iss: foreignOrg, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}}), http.StatusForbidden},
			{"repo scoped", e.mint(t, tokenSpec{iss: org, repo: "a-repo", scopes: []string{auth.ScopeOrgRead}}), http.StatusForbidden},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				r := httptest.NewRequest(http.MethodGet, "/api/meta", nil)
				if tc.token != "" {
					r.Header.Set("Authorization", "Bearer "+tc.token)
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != tc.status {
					t.Fatalf("status = %d, want %d; body %s", w.Code, tc.status, w.Body.String())
				}
				if tc.status != http.StatusOK {
					decodeEnvelope(t, w)
					if tc.status == http.StatusUnauthorized && w.Header().Get("WWW-Authenticate") != "Bearer" {
						t.Fatal("401 must carry WWW-Authenticate: Bearer")
					}
					return
				}
				if got := w.Header().Get("Content-Type"); got != "application/json" {
					t.Fatalf("Content-Type = %q", got)
				}
				// Exact bytes pin the property names, types, empty arrays (not
				// null), empty strings, and absence of extra properties.
				const want = `{"ips":{"api":[],"git":[]},"provider":{"name":"","region":""}}`
				if got := strings.TrimSpace(w.Body.String()); got != want {
					t.Fatalf("body = %s, want %s", got, want)
				}
			})
		}
		t.Run("no v1 alias", func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
			r.Header.Set("Authorization", "Bearer "+orgRead)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", w.Code)
			}
		})
	})
}

func TestOrganizationMetadataPreAuthRateLimit(t *testing.T) {
	e := testEnvironment(t, "sqlite")
	h, err := New(Config{
		Store: e.store, Verifier: auth.NewVerifier(e.store), Logger: discardLogger(),
		Org: e.newOrg(t), HTTPBaseURL: testHTTPBaseURL, RefStore: e.refStore,
		BlobStore: e.blobStore, CacheDir: e.cacheDir, SyncQueue: envSyncQueue{},
		RateLimit: RateLimitConfig{RPS: 0.001, Burst: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Missing auth still consumes the shared REST bucket before verification.
	for i, path := range []string{"/api/meta", "/api/meta", "/api/v1/repos"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		want := http.StatusTooManyRequests
		if i == 0 {
			want = http.StatusUnauthorized
		}
		if w.Code != want {
			t.Fatalf("request %d %s: status = %d, want %d", i, path, w.Code, want)
		}
		decodeEnvelope(t, w)
		if want == http.StatusTooManyRequests {
			assertRateLimited(t, w.Header())
		}
	}
}
