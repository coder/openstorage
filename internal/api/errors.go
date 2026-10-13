package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

// internalErrorMsg is the only 500 body clients ever see: raw error strings
// (often verbatim DB/driver output) are logged server-side, never leaked
// onto the wire (docs/reference/http-api.md: clients branch on status codes only).
const internalErrorMsg = "internal error"

// internalErr logs the real error with request context and writes the
// generic {"error":"internal error"} envelope with status 500 — except for
// read-side git limit errors, which get their documented answer
// (writeGitLimitErr).
func (s *server) internalErr(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(err != nil, "internalErr called with nil error")
	assertf(op != "", "internalErr called with empty op")
	// Read-side git limits (#313) are not internal errors: every handler
	// reports its git failures through here, so this is the one place that
	// maps them to their documented 422/503 answers.
	if s.writeGitLimitErr(w, r, op, err) {
		return
	}
	s.logger.Error("internal error", "op", op, "method", r.Method, "path", r.URL.Path, "err", err)
	writeErr(w, http.StatusInternalServerError, internalErrorMsg)
}

// errorEnvelope is the plain wire error shape (docs/reference/http-api.md):
// the legacy {"error": "<unstable human string>"} plus the RFC 9457
// problem-details fields upstream documents (issue #349). `code` is the
// generic per-status code only; `detail` mirrors `error`; `instance` is the
// request path (issue #352). Clients branch on the status code or `code`,
// never on the message. The content type stays application/json: SDK 1.17.1
// parses error JSON only for that type.
type errorEnvelope struct {
	Error    string `json:"error"`
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Code     string `json:"code"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}

// writeErr writes the plain error envelope with the given status and
// upstream's generic code for it.
// 401 responses additionally carry `WWW-Authenticate: Bearer` (docs/reference/authentication.md).
func writeErr(w http.ResponseWriter, status int, msg string) {
	writeProblem(w, status, genericProblemCode(status), msg)
}

// writeProblem writes the plain error envelope. It is the only writer of
// errorEnvelope. code is the generic code for status, or a specific code
// from specificProblemCodes whose upstream status is status.
func writeProblem(w http.ResponseWriter, status int, code, msg string) {
	assertf(status >= 400 && status < 600, "writeProblem called with non-error status %d", status)
	assertf(msg != "", "writeProblem called with empty message")
	// RFC 9457 `title` is the status text, so only registered statuses are
	// valid here (499 is never emitted: ADR-26).
	title := http.StatusText(status)
	assertf(title != "", "writeProblem called with unregistered status %d", status)
	if specific, ok := specificProblemCodes[code]; ok {
		assertf(specific == status, "writeProblem: code %q is upstream's code for %d, not %d", code, specific, status)
	} else {
		assertf(code == genericProblemCode(status), "writeProblem: code %q is neither specific nor the generic code for %d", code, status)
	}
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeJSON(w, status, errorEnvelope{
		Error:    msg,
		Type:     "about:blank",
		Title:    title,
		Status:   status,
		Code:     code,
		Detail:   msg,
		Instance: problemInstance(w),
	})
}

// Specific problem codes open.storage emits, from the "Specific codes:"
// list in the "Error codes" section of docs/upstream/openapi.yaml.
// A specific code replaces the generic one only where the meaning matches
// upstream's exactly (issue #352).
const (
	codeBranchNotFound     = "branch_not_found"
	codeCredentialExists   = "credential_exists"
	codeCredentialNotFound = "credential_not_found"
	codeInsufficientScope  = "insufficient_scope"
	codeRefNotFound        = "ref_not_found"
	codeRepoNameMismatch   = "repo_name_mismatch"
	codeRepositoryDeleted  = "repository_deleted"
	codeRepositoryNotFound = "repository_not_found"
)

// specificProblemCodes maps each specific code to its upstream status
// (TestSpecificProblemCodesMatchUpstream pins it to the snapshot).
var specificProblemCodes = map[string]int{
	codeBranchNotFound:     http.StatusNotFound,
	codeCredentialExists:   http.StatusConflict,
	codeCredentialNotFound: http.StatusNotFound,
	codeInsufficientScope:  http.StatusForbidden,
	codeRefNotFound:        http.StatusNotFound,
	codeRepoNameMismatch:   http.StatusBadRequest,
	codeRepositoryDeleted:  http.StatusConflict,
	codeRepositoryNotFound: http.StatusNotFound,
}

// repoNotFoundMsg is the message of every repository-lookup 404.
const repoNotFoundMsg = "repository not found"

// writeRepoNotFound writes the repository-lookup 404: the repository does
// not exist or the token cannot access it (upstream's
// `repository_not_found`).
func writeRepoNotFound(w http.ResponseWriter) {
	writeProblem(w, http.StatusNotFound, codeRepositoryNotFound, repoNotFoundMsg)
}

// instanceWriter carries the RFC 9457 `instance` of one request: the
// escaped request path the client sent, without the query (upstream's
// example answers a bad ?cursor on GET /api/repos with `instance:
// /api/repos`).
type instanceWriter struct {
	http.ResponseWriter
	instance string
}

// Unwrap exposes the wrapped writer to http.ResponseController.
func (w *instanceWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// withProblemInstance records the request path for writeErr. It must wrap
// the whole handler: the /api/repos/{repo_name}/... aliases rewrite
// r.URL.Path to the v1 path before they dispatch (alias.go), and `instance`
// names the path the client sent.
func withProblemInstance(next http.Handler) http.Handler {
	assertf(next != nil, "withProblemInstance: nil handler")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&instanceWriter{ResponseWriter: w, instance: r.URL.EscapedPath()}, r)
	})
}

// problemInstance finds the instanceWriter under w through the Unwrap
// chain, the http.ResponseController convention that every writer wrapper
// in this package follows. It returns "" (no `instance` member) when w was
// not served through withProblemInstance, as in unit tests.
func problemInstance(w http.ResponseWriter) string {
	for {
		switch t := w.(type) {
		case *instanceWriter:
			return t.instance
		case interface{ Unwrap() http.ResponseWriter }:
			w = t.Unwrap()
		default:
			return ""
		}
	}
}

// genericProblemCodes is upstream's generic per-status `code` table,
// verbatim from the "Error codes" section of docs/upstream/openapi.yaml
// (TestGenericProblemCodeMatchesUpstream pins it to the snapshot).
var genericProblemCodes = map[int]string{
	http.StatusBadRequest:            "bad_request",
	http.StatusUnauthorized:          "unauthorized",
	http.StatusForbidden:             "forbidden",
	http.StatusNotFound:              "not_found",
	http.StatusMethodNotAllowed:      "method_not_allowed",
	http.StatusRequestTimeout:        "request_timeout",
	http.StatusConflict:              "conflict",
	http.StatusPreconditionFailed:    "precondition_failed",
	http.StatusRequestEntityTooLarge: "payload_too_large",
	http.StatusUnprocessableEntity:   "unprocessable_entity",
	http.StatusTooManyRequests:       "rate_limited",
	499:                              "request_canceled",
	http.StatusInternalServerError:   "internal_error",
	http.StatusNotImplemented:        "not_implemented",
	http.StatusBadGateway:            "bad_gateway",
	http.StatusServiceUnavailable:    "service_unavailable",
	http.StatusGatewayTimeout:        "gateway_timeout",
}

// genericProblemCode returns upstream's generic `code` for status. Upstream:
// "Any other status uses its status text in snake_case"; that fallback uses
// Go's status text, which may name a few statuses differently from upstream.
// Every status writeErr emits today is in the table.
func genericProblemCode(status int) string {
	if code, ok := genericProblemCodes[status]; ok {
		return code
	}
	text := http.StatusText(status)
	assertf(text != "", "genericProblemCode: unregistered status %d", status)
	var b strings.Builder
	sep := false
	for _, r := range strings.ToLower(text) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			if sep && b.Len() > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
			sep = false
			continue
		}
		sep = true
	}
	return b.String()
}

// writeJSON writes v as the JSON response body with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	assertf(status >= 200 && status < 600, "writeJSON called with invalid status %d", status)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Encoding a value we constructed cannot fail; a write error just means
	// the client went away, which is not actionable here.
	_ = json.NewEncoder(w).Encode(v)
}

// statusForHTTP maps an HTTP failure code onto the docs/explanation/streaming-commit-protocols.md result-envelope
// status string the SDKs convert into typed RefUpdateErrors. This is the
// single authoritative table for the write surface (wcFail, wmFailConflict,
// wrefNoteFail); an unmapped code is a programming error and fails fast.
func statusForHTTP(httpStatus int) string {
	switch httpStatus {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return "invalid"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusPreconditionFailed:
		return "precondition_failed"
	case http.StatusTooManyRequests:
		return "unavailable" // docs/reference/http-api.md: rate limit ("unavailable" to ref-update clients)
	case http.StatusInternalServerError:
		return "internal"
	default:
		assertf(false, "statusForHTTP: unmapped status %d", httpStatus)
		return ""
	}
}
