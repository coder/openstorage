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
	"codestorage/internal/blob"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("api assertion failed: "+format, args...))
	}
}

// Config wires the API service. Store, Verifier, HTTPBaseURL, RefStore,
// BlobStore, and CacheDir are required; Logger defaults to slog.Default();
// Fallback (optional) receives every request not matching an /api/v1 route —
// the serve command mounts the git smart-HTTP handler there.
type Config struct {
	Store    meta.Store
	Verifier auth.Verifier
	Logger   *slog.Logger
	// HTTPBaseURL is the externally reachable git base URL (scheme://host),
	// used to build the `http_url` in the POST /repos response, e.g.
	// "https://selfhost.code.storage".
	HTTPBaseURL string
	Fallback    http.Handler

	// RefStore/BlobStore/CacheDir back the git:read endpoints (docs/07 p3):
	// handlers materialize the repo cache via gitread.Materialize(RefStore,
	// BlobStore, CacheDir, ...) and run read plumbing over it. The serve
	// command passes the same instances the githttp fallback uses.
	RefStore  ref.RefStore
	BlobStore blob.BlobStore
	CacheDir  string
}

// New builds the full HTTP handler: recover -> request log -> router, with
// per-route JWT auth (scope + org-level enforcement per docs/02).
func New(cfg Config) http.Handler {
	assertf(cfg.Store != nil, "Config.Store is required")
	assertf(cfg.Verifier != nil, "Config.Verifier is required")
	assertf(strings.TrimSpace(cfg.HTTPBaseURL) != "", "Config.HTTPBaseURL is required")
	assertf(cfg.RefStore != nil, "Config.RefStore is required")
	assertf(cfg.BlobStore != nil, "Config.BlobStore is required")
	assertf(cfg.CacheDir != "", "Config.CacheDir is required")

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	s := &server{
		store:       cfg.Store,
		httpBaseURL: strings.TrimRight(cfg.HTTPBaseURL, "/"),
		logger:      logger,
		refStore:    cfg.RefStore,
		blobStore:   cfg.BlobStore,
		cacheDir:    cfg.CacheDir,
	}

	mux := http.NewServeMux()
	route := func(pattern string, orgLevel bool, h http.HandlerFunc, scopes ...string) {
		mux.Handle(pattern, requireAuth(cfg.Verifier, orgLevel, scopes...)(h))
	}
	route("POST /api/v1/repos", false, s.handleCreateRepo, auth.ScopeRepoWrite)
	route("GET /api/v1/repo", false, s.handleGetRepo, auth.ScopeGitRead)
	route("GET /api/v1/repos", true, s.handleListRepos, auth.ScopeOrgRead)
	route("DELETE /api/v1/repos/delete", false, s.handleDeleteRepo, auth.ScopeRepoWrite)

	// git:read surface (docs/07 p3, docs/03; scope table docs/02:56). Go's
	// ServeMux "GET" patterns also match HEAD, covering HEAD /repos/file.
	route("GET /api/v1/repos/file", false, s.handleGetFile, auth.ScopeGitRead)
	route("GET /api/v1/repos/files", false, s.handleListFiles, auth.ScopeGitRead)
	route("GET /api/v1/repos/files/metadata", false, s.handleFilesMetadata, auth.ScopeGitRead)
	route("GET /api/v1/repos/branches", false, s.handleListBranches, auth.ScopeGitRead)
	route("GET /api/v1/repos/branches/diff", false, s.handleBranchesDiff, auth.ScopeGitRead)
	route("GET /api/v1/repos/commits", false, s.handleListCommits, auth.ScopeGitRead)
	route("GET /api/v1/repos/commit", false, s.handleGetCommit, auth.ScopeGitRead)
	route("GET /api/v1/repos/diff", false, s.handleDiff, auth.ScopeGitRead)
	route("GET /api/v1/repos/tags", false, s.handleListTags, auth.ScopeGitRead)
	route("GET /api/v1/repos/blame", false, s.handleBlame, auth.ScopeGitRead)
	route("POST /api/v1/repos/grep", false, s.handleGrep, auth.ScopeGitRead)
	route("POST /api/v1/repos/archive", false, s.handleArchive, auth.ScopeGitRead)

	// Notes read surface (docs/03 "Notes"; handlers_read_notes.go). The
	// notes write routes live in registerWriteRefRoutes below.
	route("GET /api/v1/repos/notes", false, s.handleGetNote, auth.ScopeGitRead)
	route("GET /api/v1/repos/notes/refs", false, s.handleListNotesRefs, auth.ScopeGitRead)

	// git:write surface (docs/07 p4). The route/scope tables live next to
	// the handlers (handlers_write_commit.go, handlers_write_refs.go).
	registerWriteCommitRoutes(mux, s, cfg.Verifier)
	s.registerWriteRefRoutes(mux, cfg.Verifier)
	s.registerWriteMergeRoutes(mux, cfg.Verifier)

	if cfg.Fallback != nil {
		mux.Handle("/", cfg.Fallback)
	}

	return recoverPanics(logger)(logRequests(logger)(mux))
}
