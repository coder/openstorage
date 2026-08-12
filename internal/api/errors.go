package api

import (
	"encoding/json"
	"net/http"
)

// internalErrorMsg is the only 500 body clients ever see: raw error strings
// (often verbatim DB/driver output) are logged server-side, never leaked
// onto the wire (docs/03: clients branch on status codes only).
const internalErrorMsg = "internal error"

// internalErr logs the real error with request context and writes the
// generic {"error":"internal error"} envelope with status 500.
func (s *server) internalErr(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(err != nil, "internalErr called with nil error")
	assertf(op != "", "internalErr called with empty op")
	s.logger.Error("internal error", "op", op, "method", r.Method, "path", r.URL.Path, "err", err)
	writeErr(w, http.StatusInternalServerError, internalErrorMsg)
}

// errorEnvelope is the wire error shape: {"error": "<unstable human string>"}
// (docs/03). Clients branch on status codes only, never on the message.
type errorEnvelope struct {
	Error string `json:"error"`
}

// writeErr writes the {"error": msg} envelope with the given status.
// 401 responses additionally carry `WWW-Authenticate: Bearer` (docs/02).
func writeErr(w http.ResponseWriter, status int, msg string) {
	assertf(status >= 400 && status < 600, "writeErr called with non-error status %d", status)
	assertf(msg != "", "writeErr called with empty message")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeJSON(w, status, errorEnvelope{Error: msg})
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

// statusForHTTP maps an HTTP failure code onto the docs/04 result-envelope
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
	case http.StatusInternalServerError:
		return "internal"
	default:
		assertf(false, "statusForHTTP: unmapped status %d", httpStatus)
		return ""
	}
}
