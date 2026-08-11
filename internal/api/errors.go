package api

import (
	"encoding/json"
	"net/http"
)

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
