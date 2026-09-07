package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/open-storage/internal/auth"
	"github.com/coder/open-storage/internal/events"
	"github.com/coder/open-storage/internal/meta"
)

// Webhook CRUD surface. Endpoint management is NOT part of the public SDK
// surface (docs/reference/webhooks.md: upstream handles it in the dashboard/control-plane), so
// the OSS backend defines its own org-scoped routes, following the
// git-credentials precedent: org-wide tokens with repo:write.
//
//	POST   /api/v1/webhooks       {url, secret?} -> 200 {id,url,secret,created_at}
//	GET    /api/v1/webhooks                      -> 200 {webhooks:[{id,url,created_at}]}
//	DELETE /api/v1/webhooks/{id}                 -> 204 | 404
//
// The signing secret is returned exactly once, from POST (server-generated
// when the body omits it); GET never echoes secrets.

// webhooksAPI carries the webhook-route dependencies. It is separate from
// the main server struct so the surface stays self-contained: api.New (or
// the serve wiring) mounts it with one RegisterWebhookRoutes call once the
// meta backend provides an events.WebhookStore.
type webhooksAPI struct {
	store  events.WebhookStore
	guard  events.TargetGuard
	logger *slog.Logger
}

// RegisterWebhookRoutes mounts the webhook CRUD routes on mux. All three
// routes are org-level (docs/reference/authentication.md: `repo` claim == "org") and require
// repo:write, with the plain {"error"} envelope on failures — these are
// management routes, not docs/explanation/streaming-commit-protocols.md ref-write routes, so requireAuth (not
// requireAuthWrite) is correct.
func RegisterWebhookRoutes(mux *http.ServeMux, verifier auth.Verifier, org string, store events.WebhookStore, guard events.TargetGuard, logger *slog.Logger, rl *rateLimiter) {
	assertf(mux != nil, "RegisterWebhookRoutes: nil mux")
	assertf(verifier != nil, "RegisterWebhookRoutes: nil verifier")
	assertf(store != nil, "RegisterWebhookRoutes: nil store")
	if logger == nil {
		logger = slog.Default()
	}
	h := &webhooksAPI{store: store, guard: guard, logger: logger}
	route := func(pattern string, hf http.HandlerFunc) {
		mux.Handle(pattern, rl.limit(routeRead)(requireAuth(verifier, org, true, auth.ScopeRepoWrite)(hf)))
	}
	route("POST /api/v1/webhooks", h.handleCreate)
	route("GET /api/v1/webhooks", h.handleList)
	route("DELETE /api/v1/webhooks/{id}", h.handleDelete)
}

// internalErr mirrors server.internalErr: log the real error, answer with
// the generic 500 envelope (docs/reference/http-api.md: raw errors never reach the wire).
func (h *webhooksAPI) internalErr(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(err != nil, "internalErr called with nil error")
	assertf(op != "", "internalErr called with empty op")
	h.logger.Error("internal error", "op", op, "method", r.Method, "path", r.URL.Path, "err", err)
	writeErr(w, http.StatusInternalServerError, internalErrorMsg)
}

type createWebhookRequest struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
}

type webhookResponse struct {
	ID        string `json:"id"`
	URL       string `json:"url"`
	Secret    string `json:"secret,omitempty"` // POST response only, shown once
	CreatedAt string `json:"created_at"`       // RFC 3339 UTC
}

// validWebhookURL accepts absolute http(s) URLs with a host — anything
// else can never receive a delivery.
func validWebhookURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// generateWebhookSecret returns a fresh 64-hex-char (256-bit) signing
// secret. Runtime-generated only, never persisted outside the meta store.
func generateWebhookSecret() string {
	buf := make([]byte, 32)
	_, err := rand.Read(buf)
	assertf(err == nil, "generateWebhookSecret: crypto/rand failed: %v", err)
	return hex.EncodeToString(buf)
}

// handleCreate implements POST /api/v1/webhooks: register a delivery
// endpoint for the org. The secret is optional; when omitted the server
// generates one and returns it exactly once (docs/archive/phases/12-phase3-implementation-notes.md deviation style:
// 200, not 201).
func (h *webhooksAPI) handleCreate(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	var req createWebhookRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if !validWebhookURL(req.URL) {
		writeErr(w, http.StatusBadRequest, "invalid webhook url: must be an absolute http(s) URL")
		return
	}
	// Registration-time SSRF courtesy check (issue #53): reject targets
	// that are denied IP literals or currently resolve to denied
	// addresses. Fast feedback only — acceptance here is NOT a delivery
	// guarantee, because DNS can change afterwards; the delivery client's
	// dial-time guard (events.TargetGuard.NewClient) is authoritative.
	if err := h.guard.CheckRegistration(r.Context(), req.URL); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid webhook url: "+err.Error())
		return
	}
	secret := req.Secret
	if secret == "" {
		secret = generateWebhookSecret()
	} else if strings.TrimSpace(secret) == "" {
		// A whitespace-only secret is always a caller bug: it would sign
		// deliveries with a key nobody can plausibly have intended.
		writeErr(w, http.StatusBadRequest, "invalid secret: must not be blank")
		return
	}

	wh, err := h.store.CreateWebhook(r.Context(), claims.Iss, req.URL, secret)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case errors.Is(err, meta.ErrReservedSecret):
		writeErr(w, http.StatusBadRequest, "invalid secret: "+meta.ErrReservedSecret.Error())
		return
	case err != nil:
		h.internalErr(w, r, "create webhook", err)
		return
	}
	assertf(wh.ID != "", "CreateWebhook returned empty ID")
	writeJSON(w, http.StatusOK, webhookResponse{
		ID:        wh.ID,
		URL:       wh.URL,
		Secret:    wh.Secret,
		CreatedAt: wh.CreatedAt.UTC().Format(time.RFC3339),
	})
}

// handleList implements GET /api/v1/webhooks: the org's registered
// endpoints in registration order, secrets omitted.
func (h *webhooksAPI) handleList(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	hooks, err := h.store.ListWebhooks(r.Context(), claims.Iss)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case err != nil:
		h.internalErr(w, r, "list webhooks", err)
		return
	}
	out := make([]webhookResponse, 0, len(hooks)) // never null in JSON
	for _, wh := range hooks {
		out = append(out, webhookResponse{
			ID:        wh.ID,
			URL:       wh.URL,
			CreatedAt: wh.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"webhooks": out})
}

// handleDelete implements DELETE /api/v1/webhooks/{id}: 204 on removal,
// 404 when the org has no such webhook (idempotent second delete).
func (h *webhooksAPI) handleDelete(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	id := r.PathValue("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, "missing webhook id")
		return
	}
	err := h.store.DeleteWebhook(r.Context(), claims.Iss, id)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "webhook not found")
		return
	case err != nil:
		h.internalErr(w, r, "delete webhook", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
