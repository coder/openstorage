// Read-endpoint stubs, core group (docs/07 p3 phases 2a/2b): refs, commits,
// files, and conditional/range file streaming. Every handler below is owned
// by an endpoint slice; until it lands, the route answers 501 so the router,
// auth wiring, and contract scaffolding compile and run end-to-end.
package api

import "net/http"

// notImplemented is the shared 501 stub body (docs/03 {"error"} envelope).
func notImplemented(w http.ResponseWriter) {
	writeErr(w, http.StatusNotImplemented, "not implemented")
}

// handleListBranches implements GET /api/v1/repos/branches (2a).
func (s *server) handleListBranches(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleListTags implements GET /api/v1/repos/tags (2a).
func (s *server) handleListTags(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleListCommits implements GET /api/v1/repos/commits (2a).
func (s *server) handleListCommits(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleGetCommit implements GET /api/v1/repos/commit (2a).
func (s *server) handleGetCommit(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleListFiles implements GET /api/v1/repos/files (2b).
func (s *server) handleListFiles(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleFilesMetadata implements GET /api/v1/repos/files/metadata (2b).
func (s *server) handleFilesMetadata(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}

// handleGetFile implements GET+HEAD /api/v1/repos/file (2b): conditional and
// range blob streaming via http.ServeContent.
func (s *server) handleGetFile(w http.ResponseWriter, r *http.Request) {
	notImplemented(w)
}
