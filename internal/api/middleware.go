package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/auth"
)

// middleware is a plain http.Handler decorator.
type middleware func(http.Handler) http.Handler

// recoverPanics converts handler panics into a 500 {"error"} response and
// logs the stack. http.ErrAbortHandler is re-panicked per net/http contract.
func recoverPanics(logger *slog.Logger) middleware {
	assertf(logger != nil, "recoverPanics: nil logger")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				logger.Error("panic in handler",
					"method", r.Method, "path", r.URL.Path,
					"panic", rec, "stack", string(debug.Stack()))
				// Best effort: if the handler already started the response,
				// this is a no-op superfluous WriteHeader.
				writeErr(w, http.StatusInternalServerError, "internal server error")
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// statusRecorder captures the response status for request logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

// Unwrap exposes the wrapped ResponseWriter to http.ResponseController so
// optional interfaces (Flusher, deadlines) reach the underlying writer.
// Load-bearing for git smart HTTP, which streams through this wrapper and
// must flush pack data mid-response.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// logRequests logs one line per request: method, path, status, duration.
func logRequests(logger *slog.Logger) middleware {
	assertf(logger != nil, "logRequests: nil logger")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w}
			start := time.Now()
			next.ServeHTTP(rec, r)
			status := rec.status
			if status == 0 {
				status = http.StatusOK // handler wrote nothing
			}
			logger.Info("http request",
				"method", r.Method, "path", r.URL.Path,
				"status", status, "duration", time.Since(start))
		})
	}
}

// bearerToken extracts the token from an `Authorization: Bearer <jwt>`
// header. ok=false when the header is absent or not a Bearer credential.
func bearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}

// requireAuth enforces the docs/02 failure map for one route: missing or
// unverifiable token -> 401 (+WWW-Authenticate via writeErr); verified token
// issued for another org (issue #47), missing ANY of the required scopes
// (docs/02: e.g. DELETE /repos/tags demands both git:read and git:write), a
// non-"org" repo claim on an org-level route, or an empty/"org" repo claim
// on a per-repo route -> 403 (claim mismatch with target). On success the
// verified claims are injected into the request context; per-repo handlers
// can rely on a non-empty, non-"org" repo claim.
func requireAuth(verifier auth.Verifier, org string, orgLevel bool, requiredScopes ...string) middleware {
	return requireAuthWith(verifier, org, orgLevel, writeErr, requiredScopes...)
}

// authFailStatus maps the two auth-failure HTTP codes onto the docs/04
// result-envelope status strings.
func authFailStatus(httpCode int) string {
	switch httpCode {
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "forbidden"
	}
	panic(fmt.Sprintf("api assertion failed: authFailStatus: unexpected code %d", httpCode))
}

// writeAuthEnvelope is the docs/04 §3 / ADR-26 failure writer for write
// routes: auth failures carry the result envelope ({"result": {success,
// status, message}}) instead of the plain {"error"} body, so SDKs can map
// them onto typed RefUpdateErrors. 401 keeps WWW-Authenticate (RFC 9110).
func writeAuthEnvelope(w http.ResponseWriter, httpCode int, msg string) {
	assertf(msg != "", "writeAuthEnvelope: empty message")
	if httpCode == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeJSON(w, httpCode, map[string]any{"result": map[string]any{
		"success": false,
		"status":  authFailStatus(httpCode),
		"message": msg,
	}})
}

// requireAuthWrite is requireAuth with ADR-26 envelope-shaped 401/403
// bodies; every docs/04 write route must use it.
func requireAuthWrite(verifier auth.Verifier, org string, orgLevel bool, requiredScopes ...string) middleware {
	return requireAuthWith(verifier, org, orgLevel, writeAuthEnvelope, requiredScopes...)
}

func requireAuthWith(verifier auth.Verifier, org string, orgLevel bool, fail func(http.ResponseWriter, int, string), requiredScopes ...string) middleware {
	assertf(verifier != nil, "requireAuth: nil verifier")
	assertf(fail != nil, "requireAuth: nil failure writer")
	assertf(len(requiredScopes) > 0, "requireAuth: no required scopes")
	for _, s := range requiredScopes {
		assertf(s != "", "requireAuth: empty required scope")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := bearerToken(r)
			if !ok {
				fail(w, http.StatusUnauthorized, "missing or malformed authorization header")
				return
			}
			claims, err := verifier.Verify(r.Context(), token)
			if err != nil {
				fail(w, http.StatusUnauthorized, "invalid token")
				return
			}
			assertf(claims != nil, "Verifier returned nil claims without error")
			// Served-org gate (issue #47), mirroring githttp.Server.verifyClaims:
			// a signature valid for some *other* registered org (e.g. a row
			// retained after CODESTORAGE_ORG changed) must not grant access to
			// this deployment's REST surface. Checked before scopes so the
			// failure mode matches the git path's ordering.
			if org != "" && claims.Iss != org {
				fail(w, http.StatusForbidden, fmt.Sprintf("token issued for org %q, not served here", claims.Iss))
				return
			}
			for _, scope := range requiredScopes {
				if !claims.HasScope(scope) {
					fail(w, http.StatusForbidden, "missing required scope "+scope)
					return
				}
			}
			if orgLevel && claims.Repo != auth.RepoClaimOrg {
				fail(w, http.StatusForbidden, "token is not org-scoped")
				return
			}
			if !orgLevel && (claims.Repo == "" || claims.Repo == auth.RepoClaimOrg) {
				fail(w, http.StatusForbidden, "token is not repo-scoped")
				return
			}
			next.ServeHTTP(w, r.WithContext(auth.WithClaims(r.Context(), claims)))
		})
	}
}
