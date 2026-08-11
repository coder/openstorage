// Read-endpoint stubs, query group (docs/07 p3 phases 2c/2d): diffs, blame,
// grep, and archive streaming. Every handler below is owned by an endpoint
// slice; until it lands, the route answers 501 so the router, auth wiring,
// and contract scaffolding compile and run end-to-end.
package api

import "net/http"

// handleDiff implements GET /api/v1/repos/diff (2c) — NB: camelCase query
// params `sha`, `baseSha`, `gitApplyCompatible`, repeatable `path`.
func (s *server) handleDiff(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleBranchesDiff implements GET /api/v1/repos/branches/diff (2c).
func (s *server) handleBranchesDiff(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleBlame implements GET /api/v1/repos/blame (2d).
func (s *server) handleBlame(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleGrep implements POST /api/v1/repos/grep (2d).
func (s *server) handleGrep(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleArchive implements POST /api/v1/repos/archive (2d).
func (s *server) handleArchive(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}
