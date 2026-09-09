package api

// Webhook CRUD surface tests. The meta-backed events.WebhookStore lands
// with the phase-5 schema slice, so these tests drive the handlers with an
// in-memory store; auth flows through the production verifier + the lane's
// real meta store (org keys). The sqlite lane suffices — the fake store is
// backend-independent — and keeps the suite container-free.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
)

// memWebhookStore is an in-memory events.WebhookStore keyed like the real
// one: org-scoped CRUD by org name, global lookup by ID.
type memWebhookStore struct {
	mu    sync.Mutex
	seq   int
	hooks []events.Webhook // registration order
	orgs  map[string]int64 // org name -> fake org id
}

var _ events.WebhookStore = (*memWebhookStore)(nil)

func newMemWebhookStore() *memWebhookStore {
	return &memWebhookStore{orgs: map[string]int64{}}
}

func (m *memWebhookStore) orgID(org string) int64 {
	if id, ok := m.orgs[org]; ok {
		return id
	}
	id := int64(len(m.orgs) + 1)
	m.orgs[org] = id
	return id
}

func (m *memWebhookStore) CreateWebhook(_ context.Context, org, url, secret string) (events.Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	wh := events.Webhook{
		ID:        fmt.Sprintf("wh_%06d", m.seq),
		OrgID:     m.orgID(org),
		URL:       url,
		Secret:    secret,
		CreatedAt: time.Now().UTC(),
	}
	m.hooks = append(m.hooks, wh)
	return wh, nil
}

func (m *memWebhookStore) ListWebhooks(_ context.Context, org string) ([]events.Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.orgID(org)
	var out []events.Webhook
	for _, wh := range m.hooks {
		if wh.OrgID == id {
			out = append(out, wh)
		}
	}
	return out, nil
}

func (m *memWebhookStore) GetWebhook(_ context.Context, id string) (events.Webhook, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, wh := range m.hooks {
		if wh.ID == id {
			return wh, nil
		}
	}
	return events.Webhook{}, fmt.Errorf("webhook %s: %w", id, meta.ErrNotFound)
}

func (m *memWebhookStore) DeleteWebhook(_ context.Context, org, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	orgID := m.orgID(org)
	for i, wh := range m.hooks {
		if wh.ID == id && wh.OrgID == orgID {
			m.hooks = append(m.hooks[:i], m.hooks[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("webhook %s: %w", id, meta.ErrNotFound)
}

// newWebhooksServer mounts ONLY the webhook routes (api.New wires them in
// production once the meta store implements events.WebhookStore).
func newWebhooksServer(t *testing.T, e *testEnv) (*httptest.Server, *memWebhookStore) {
	t.Helper()
	// Stub public DNS so ordinary registration tests never touch the
	// network; dedicated guard tests inject denied answers (issue #53).
	guard := events.TargetGuard{LookupNetIP: func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	}}
	return newWebhooksServerWithGuard(t, e, guard)
}

func newWebhooksServerWithGuard(t *testing.T, e *testEnv, guard events.TargetGuard) (*httptest.Server, *memWebhookStore) {
	t.Helper()
	store := newMemWebhookStore()
	mux := http.NewServeMux()
	RegisterWebhookRoutes(mux, auth.NewVerifier(e.store), "", store, guard, discardLogger(), nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, store
}

// whRequest fires one authed request and returns status + parsed JSON body
// (nil target skips parsing, e.g. for 204s).
func whRequest(t *testing.T, method, url, token string, body []byte, target any) int {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if target != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, target); err != nil {
			t.Fatalf("unmarshal %s %s response %q: %v", method, url, raw, err)
		}
	}
	return resp.StatusCode
}

func orgToken(t *testing.T, e *testEnv, org string) string {
	t.Helper()
	return e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
}

var hexSecretRe = regexp.MustCompile(`\A[0-9a-f]{64}\z`)

func TestWebhookCRUD(t *testing.T) {
	e := testEnvironment(t, "sqlite")
	srv, _ := newWebhooksServer(t, e)
	org := e.newOrg(t)
	token := orgToken(t, e, org)
	base := srv.URL + "/api/v1/webhooks"

	// Create without a secret: the server generates one, returned once.
	var created map[string]any
	if status := whRequest(t, "POST", base, token, []byte(`{"url":"https://example.com/hook"}`), &created); status != http.StatusOK {
		t.Fatalf("create status = %d, body %v", status, created)
	}
	id1, _ := created["id"].(string)
	secret1, _ := created["secret"].(string)
	if id1 == "" || created["url"] != "https://example.com/hook" || created["created_at"] == "" {
		t.Fatalf("create response = %v", created)
	}
	if !hexSecretRe.MatchString(secret1) {
		t.Errorf("generated secret %q is not 64 hex chars", secret1)
	}

	// Create with a caller-chosen secret: echoed back verbatim, once.
	var created2 map[string]any
	if status := whRequest(t, "POST", base, token, []byte(`{"url":"https://example.com/hook2","secret":"my-secret-value"}`), &created2); status != http.StatusOK {
		t.Fatalf("create2 status = %d", status)
	}
	if created2["secret"] != "my-secret-value" {
		t.Errorf("caller secret not echoed: %v", created2["secret"])
	}

	// List: both webhooks, registration order, NO secret field at all.
	var listed struct {
		Webhooks []map[string]any `json:"webhooks"`
	}
	if status := whRequest(t, "GET", base, token, nil, &listed); status != http.StatusOK {
		t.Fatalf("list status = %d", status)
	}
	if len(listed.Webhooks) != 2 {
		t.Fatalf("listed %d webhooks, want 2", len(listed.Webhooks))
	}
	if listed.Webhooks[0]["id"] != id1 {
		t.Errorf("list order: first id = %v, want %v", listed.Webhooks[0]["id"], id1)
	}
	for _, wh := range listed.Webhooks {
		if _, leaked := wh["secret"]; leaked {
			t.Errorf("list leaked a secret for %v", wh["id"])
		}
	}

	// Another org sees an empty list (org scoping), never null.
	otherToken := orgToken(t, e, e.newOrg(t))
	var otherList struct {
		Webhooks []map[string]any `json:"webhooks"`
	}
	if status := whRequest(t, "GET", base, otherToken, nil, &otherList); status != http.StatusOK {
		t.Fatalf("other-org list status = %d", status)
	}
	if otherList.Webhooks == nil || len(otherList.Webhooks) != 0 {
		t.Errorf("other org list = %v, want empty non-null array", otherList.Webhooks)
	}
	// ... and cannot delete this org's webhook.
	if status := whRequest(t, "DELETE", base+"/"+id1, otherToken, nil, nil); status != http.StatusNotFound {
		t.Errorf("cross-org delete status = %d, want 404", status)
	}

	// Delete: 204, then 404 on repeat.
	if status := whRequest(t, "DELETE", base+"/"+id1, token, nil, nil); status != http.StatusNoContent {
		t.Errorf("delete status = %d, want 204", status)
	}
	var delErr errorEnvelope
	if status := whRequest(t, "DELETE", base+"/"+id1, token, nil, &delErr); status != http.StatusNotFound {
		t.Errorf("second delete status = %d, want 404", status)
	} else if delErr.Error == "" {
		t.Errorf("404 body missing the error envelope")
	}
	if status := whRequest(t, "GET", base, token, nil, &listed); status != http.StatusOK || len(listed.Webhooks) != 1 {
		t.Errorf("after delete: %d webhooks left, want 1", len(listed.Webhooks))
	}
}

func TestWebhookCreateValidation(t *testing.T) {
	e := testEnvironment(t, "sqlite")
	srv, store := newWebhooksServer(t, e)
	token := orgToken(t, e, e.newOrg(t))
	base := srv.URL + "/api/v1/webhooks"

	for name, body := range map[string]string{
		"missing url":     `{}`,
		"relative url":    `{"url":"/hook"}`,
		"no host":         `{"url":"http://"}`,
		"non-http scheme": `{"url":"ftp://example.com/hook"}`,
		"not a url":       `{"url":"::not a url::"}`,
		"blank secret":    `{"url":"https://example.com/h","secret":"   "}`,
		"malformed json":  `{"url":`,
		"non-object body": `[1,2,3]`,
	} {
		t.Run(name, func(t *testing.T) {
			var envlp errorEnvelope
			status := whRequest(t, "POST", base, token, []byte(body), &envlp)
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", status)
			}
			if envlp.Error == "" {
				t.Errorf("400 body missing the {\"error\"} envelope")
			}
		})
	}
	if n := len(store.hooks); n != 0 {
		t.Errorf("invalid requests persisted %d webhooks", n)
	}
}

func TestWebhookCreateTargetGuard(t *testing.T) {
	e := testEnvironment(t, "sqlite")
	org := e.newOrg(t)
	token := orgToken(t, e, org)

	t.Run("strict guard rejects literal and resolved private targets", func(t *testing.T) {
		guard := events.TargetGuard{LookupNetIP: func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("10.0.0.7")}, nil
		}}
		srv, store := newWebhooksServerWithGuard(t, e, guard)
		for _, rawURL := range []string{
			"http://127.0.0.1/hook",
			"http://[::ffff:192.168.1.2]/hook",
			"https://internal.example/hook",
		} {
			body, err := json.Marshal(createWebhookRequest{URL: rawURL})
			if err != nil {
				t.Fatal(err)
			}
			var envlp errorEnvelope
			if status := whRequest(t, http.MethodPost, srv.URL+"/api/v1/webhooks", token, body, &envlp); status != http.StatusBadRequest {
				t.Fatalf("register %s: status = %d, want 400", rawURL, status)
			}
			if !strings.Contains(envlp.Error, "denied") {
				t.Fatalf("register %s: error = %q, want denied-target detail", rawURL, envlp.Error)
			}
		}
		if got := len(store.hooks); got != 0 {
			t.Fatalf("strict target guard persisted %d webhooks, want 0", got)
		}
	})

	t.Run("AllowPrivate accepts a private target", func(t *testing.T) {
		srv, store := newWebhooksServerWithGuard(t, e, events.TargetGuard{AllowPrivate: true})
		body := []byte(`{"url":"http://127.0.0.1/hook"}`)
		var created webhookResponse
		if status := whRequest(t, http.MethodPost, srv.URL+"/api/v1/webhooks", token, body, &created); status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
		if got := len(store.hooks); got != 1 {
			t.Fatalf("AllowPrivate persisted %d webhooks, want 1", got)
		}
	})
}

func TestWebhookAuth(t *testing.T) {
	e := testEnvironment(t, "sqlite")
	srv, _ := newWebhooksServer(t, e)
	org := e.newOrg(t)
	base := srv.URL + "/api/v1/webhooks"
	body := []byte(`{"url":"https://example.com/hook"}`)

	cases := map[string]struct {
		token string
		want  int
	}{
		"no token":      {"", http.StatusUnauthorized},
		"garbage token": {"not-a-jwt", http.StatusUnauthorized},
		"repo-scoped token": {
			e.mint(t, tokenSpec{iss: org, repo: "acme/repo", scopes: []string{auth.ScopeRepoWrite}}),
			http.StatusForbidden,
		},
		"missing repo:write": {
			e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeGitRead}}),
			http.StatusForbidden,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			for _, m := range []struct {
				method, url string
				body        []byte
			}{
				{"POST", base, body},
				{"GET", base, nil},
				{"DELETE", base + "/wh_000001", nil},
			} {
				var envlp errorEnvelope
				status := whRequest(t, m.method, m.url, tc.token, m.body, &envlp)
				if status != tc.want {
					t.Errorf("%s %s status = %d, want %d", m.method, m.url, status, tc.want)
				}
				if envlp.Error == "" {
					t.Errorf("%s auth failure missing the {\"error\"} envelope", m.method)
				}
			}
		})
	}
}
