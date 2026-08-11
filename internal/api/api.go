// Package api implements the SDK-compatible HTTP API surface (docs/03):
// repo CRUD under /api/v1 with the {"error": "..."} envelope, JWT bearer
// auth (docs/02), and panic-recovery/request-log middleware.
//
// The package depends only on the frozen phase-1 contracts meta.Store and
// auth.Verifier; wiring concrete implementations is the serve command's job.
package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"codestorage/internal/auth"
	"codestorage/internal/meta"
)

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("api assertion failed: "+format, args...))
	}
}

// Config wires the API service. Store, Verifier, and HTTPBaseURL are
// required; Logger defaults to slog.Default(); Fallback (optional) receives
// every request not matching an /api/v1 route — the serve command mounts the
// git smart-HTTP handler there.
type Config struct {
	Store    meta.Store
	Verifier auth.Verifier
	Logger   *slog.Logger
	// HTTPBaseURL is the externally reachable git base URL (scheme://host),
	// used to build the `http_url` in the POST /repos response, e.g.
	// "https://selfhost.code.storage".
	HTTPBaseURL string
	Fallback    http.Handler
}

// New builds the full HTTP handler: recover -> request log -> router, with
// per-route JWT auth (scope + org-level enforcement per docs/02).
func New(cfg Config) http.Handler {
	assertf(cfg.Store != nil, "Config.Store is required")
	assertf(cfg.Verifier != nil, "Config.Verifier is required")
	assertf(strings.TrimSpace(cfg.HTTPBaseURL) != "", "Config.HTTPBaseURL is required")

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	s := &server{store: cfg.Store, httpBaseURL: strings.TrimRight(cfg.HTTPBaseURL, "/"), logger: logger}

	mux := http.NewServeMux()
	route := func(pattern, scope string, orgLevel bool, h http.HandlerFunc) {
		mux.Handle(pattern, requireAuth(cfg.Verifier, scope, orgLevel)(h))
	}
	route("POST /api/v1/repos", auth.ScopeRepoWrite, false, s.handleCreateRepo)
	route("GET /api/v1/repo", auth.ScopeGitRead, false, s.handleGetRepo)
	route("GET /api/v1/repos", auth.ScopeOrgRead, true, s.handleListRepos)
	route("DELETE /api/v1/repos/delete", auth.ScopeRepoWrite, false, s.handleDeleteRepo)
	if cfg.Fallback != nil {
		mux.Handle("/", cfg.Fallback)
	}

	return recoverPanics(logger)(logRequests(logger)(mux))
}
