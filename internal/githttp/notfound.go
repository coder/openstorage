package githttp

import "net/http"

// NotFound answers 404 "not a smart-http path", the answer of
// Server.Handler to a path that names no smart-HTTP endpoint.
func NotFound(w http.ResponseWriter, _ *http.Request) {
	httpError(w, http.StatusNotFound, "not a smart-http path")
}
