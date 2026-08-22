package api

// Integration tests for the served-org gate (issue #47): a verifiable token
// whose iss is another registered org — the retained-org-row scenario — must
// 403 on every REST route, mirroring githttp.Server.verifyClaims, with the
// route-shaped failure envelope (read: {"error"}, write: ADR-26 result
// envelope). The shared multi-org harness server (Config.Org == "") keeps
// the gate off; these tests build a dedicated single-org handler per lane.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/code-storage/internal/auth"
)

// newOrgGateServer builds a full production handler (api.New) serving
// exactly servedOrg over the lane's shared stores.
func newOrgGateServer(t *testing.T, e *testEnv, servedOrg string) *httptest.Server {
	t.Helper()
	handler, err := New(Config{
		Store:       e.store,
		Verifier:    auth.NewVerifier(e.store),
		Logger:      discardLogger(),
		Org:         servedOrg,
		HTTPBaseURL: testHTTPBaseURL,
		RefStore:    e.refStore,
		BlobStore:   e.blobStore,
		CacheDir:    e.cacheDir,
		SyncQueue:   envSyncQueue{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func orgGateDo(t *testing.T, srv *httptest.Server, method, path, token string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("%s %s: decode body: %v", method, path, err)
	}
	return resp.StatusCode, body
}

func TestOrgGate_ForeignIssuer403(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		servedOrg := e.newOrg(t)
		foreignOrg := e.newOrg(t) // the retained org row: still registered, still verifiable
		srv := newOrgGateServer(t, e, servedOrg)

		repoURL := "gate-" + randomHex(t, 6)
		foreignRead := e.mint(t, tokenSpec{iss: foreignOrg, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
		foreignWrite := e.mint(t, tokenSpec{iss: foreignOrg, repo: repoURL, scopes: []string{auth.ScopeGitWrite}})

		// Read route: 403 with the standard {"error"} envelope.
		status, body := orgGateDo(t, srv, http.MethodGet, "/api/v1/repo", foreignRead)
		if status != http.StatusForbidden {
			t.Fatalf("read route with foreign-issuer token: status %d, want 403 (body %v)", status, body)
		}
		msg, ok := body["error"].(string)
		if !ok || !strings.Contains(msg, "not served here") {
			t.Fatalf(`read route 403 body = %v, want {"error": "token issued for org ..."}`, body)
		}

		// Write route: 403 with the ADR-26 result envelope.
		status, body = orgGateDo(t, srv, http.MethodPost, "/api/v1/repos/branches/create", foreignWrite)
		if status != http.StatusForbidden {
			t.Fatalf("write route with foreign-issuer token: status %d, want 403 (body %v)", status, body)
		}
		result, ok := body["result"].(map[string]any)
		if !ok {
			t.Fatalf("write route 403 body = %v, want a result envelope", body)
		}
		if result["success"] != false || result["status"] != "forbidden" ||
			!strings.Contains(result["message"].(string), "not served here") {
			t.Fatalf("write route result envelope = %v", result)
		}

		// The same token shape for the SERVED org passes the gate (the
		// repo does not exist, so the handler answers 404 — proving the
		// request got past auth).
		servedRead := e.mint(t, tokenSpec{iss: servedOrg, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
		status, _ = orgGateDo(t, srv, http.MethodGet, "/api/v1/repo", servedRead)
		if status != http.StatusNotFound {
			t.Fatalf("read route with served-org token: status %d, want 404", status)
		}

		// 401 semantics are untouched: no token still answers 401 with
		// WWW-Authenticate: Bearer on both envelopes.
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/repo", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") != "Bearer" {
			t.Fatalf("missing token: status %d, WWW-Authenticate %q; want 401 + Bearer",
				resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
		}
	})
}
