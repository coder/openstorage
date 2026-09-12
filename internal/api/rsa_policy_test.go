package api

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

func TestAuth_ExistingWeakRSAKeys(t *testing.T) {
	weak, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	weakPEM, err := auth.MarshalPublicKeyPEM(&weak.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		strongPEM, err := auth.MarshalPublicKeyPEM(&e.keys.rsa.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		strongToken := e.mint(t, tokenSpec{alg: "RS256", iss: org, repo: "org", scopes: []string{auth.ScopeOrgRead}})
		weakToken := e.mint(t, tokenSpec{alg: "RS256", key: weak, iss: org, repo: "org", scopes: []string{auth.ScopeOrgRead}})
		for _, keys := range [][]string{
			{string(weakPEM)}, {string(strongPEM), string(weakPEM)}, {string(strongPEM) + string(weakPEM)},
		} {
			// Bypass boot validation to model rows registered before upgrade.
			if _, err := e.store.UpsertOrgWithKeys(t.Context(), org, keys); err != nil {
				t.Fatal(err)
			}
			for _, token := range []string{weakToken, strongToken} {
				resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos", token, nil, "")
				if resp.StatusCode != http.StatusUnauthorized {
					t.Fatalf("legacy weak keys: status %d, want 401; body %s", resp.StatusCode, body)
				}
				requireEnvelope(t, resp, body)
				resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos/branches/create", token, []byte("{}"), "")
				if resp.StatusCode != http.StatusUnauthorized {
					t.Fatalf("write with legacy weak keys: status %d, want 401; body %s", resp.StatusCode, body)
				}
				requireAuthResultEnvelope(t, body, "unauthorized")
			}
		}
		if _, err := e.store.UpsertOrgWithKeys(t.Context(), org, []string{string(strongPEM)}); err != nil {
			t.Fatal(err)
		}
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos", strongToken, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("rotated key: status %d, want 200; body %s", resp.StatusCode, body)
		}
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos", weakToken, nil, "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("removed key: status %d, want 401; body %s", resp.StatusCode, body)
		}
	})
}
