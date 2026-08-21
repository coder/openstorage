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
	"codestorage/internal/events"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
	upsync "codestorage/internal/sync"
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

	// SyncQueue schedules upstream sync runs (docs/07 p5): base_repo
	// creates and pull-upstream enqueue through it. The serve command
	// passes the jobs seam (River on pg, in-process in local mode); tests
	// may pass a recorder/no-op.
	SyncQueue upsync.Queue

	// RateLimit enables the pre-auth per-client-IP token bucket on every
	// REST route (docs/03 gateway-level 429; see ratelimit.go). The zero
	// value disables limiting. The serve command plumbs it from
	// CODESTORAGE_RATE_RPS / CODESTORAGE_RATE_BURST.
	RateLimit RateLimitConfig
}

// New builds the full HTTP handler: recover -> request log -> router, with
// per-route JWT auth (scope + org-level enforcement per docs/02). Invalid
// operator-supplied config (RateLimit) returns an error instead of
// panicking (docs/15 deferred #4); nil required dependencies are still
// programming errors and assert.
func New(cfg Config) (http.Handler, error) {
	assertf(cfg.Store != nil, "Config.Store is required")
	assertf(cfg.Verifier != nil, "Config.Verifier is required")
	assertf(strings.TrimSpace(cfg.HTTPBaseURL) != "", "Config.HTTPBaseURL is required")
	assertf(cfg.RefStore != nil, "Config.RefStore is required")
	assertf(cfg.BlobStore != nil, "Config.BlobStore is required")
	assertf(cfg.CacheDir != "", "Config.CacheDir is required")
	assertf(cfg.SyncQueue != nil, "Config.SyncQueue is required")

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

	// Phase-5 eventing/sync surface (docs/07 p5): the syncServer extends
	// the frozen create flow with base_repo support, backed by the durable
	// meta rows through the sync-package adapters.
	creds, ups := upsync.MetaStores(cfg.Store)
	ss := newSyncServer(cfg.Store, creds, ups, cfg.SyncQueue, strings.TrimRight(cfg.HTTPBaseURL, "/"), logger)

	// Pre-auth rate limiting (docs/03; ratelimit.go). rl is nil when
	// disabled; rl.limit(...) is valid either way and wraps every REST
	// route registered below (v1, aliases, repo-urls) before auth.
	rl, err := newRateLimiter(cfg.RateLimit)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	route := func(pattern string, orgLevel bool, h http.HandlerFunc, scopes ...string) {
		mux.Handle(pattern, rl.limit(routeRead)(requireAuth(cfg.Verifier, orgLevel, scopes...)(h)))
	}
	// POST /repos is the base_repo-aware create: code/code.storage fork
	// specs dispatch to the synchronous fork (handlers_fork.go); everything
	// else falls back to the sync-aware creator (which embeds the frozen
	// (*server).handleCreateRepo flow).
	route("POST /api/v1/repos", false, s.forkAwareCreateRepo(cfg.Verifier, ss.handleCreateRepo), auth.ScopeRepoWrite)
	route("GET /api/v1/repo", false, s.handleGetRepo, auth.ScopeGitRead)
	route("PATCH /api/v1/repo", false, s.handlePatchRepo, auth.ScopeRepoWrite)
	route("GET /api/v1/repos", true, s.handleListRepos, auth.ScopeOrgRead)
	route("DELETE /api/v1/repos/delete", false, s.handleDeleteRepo, auth.ScopeRepoWrite)

	// git:read surface (docs/07 p3, docs/03; scope table docs/02:56). Go's
	// ServeMux "GET" patterns also match HEAD, covering HEAD /repos/file.
	route("GET /api/v1/repos/file", false, s.handleGetFile, auth.ScopeGitRead)
	route("GET /api/v1/repos/files", false, s.handleListFiles, auth.ScopeGitRead)
	route("GET /api/v1/repos/files/metadata", false, s.handleFilesMetadata, auth.ScopeGitRead)
	route("GET /api/v1/repos/branches", false, s.handleListBranches, auth.ScopeGitRead)
	route("GET /api/v1/repos/branch", false, s.handleGetBranch, auth.ScopeGitRead)
	route("GET /api/v1/repos/branches/diff", false, s.handleBranchesDiff, auth.ScopeGitRead)
	route("GET /api/v1/repos/commits", false, s.handleListCommits, auth.ScopeGitRead)
	route("GET /api/v1/repos/commit", false, s.handleGetCommit, auth.ScopeGitRead)
	route("GET /api/v1/repos/diff", false, s.handleDiff, auth.ScopeGitRead)
	route("GET /api/v1/repos/tags", false, s.handleListTags, auth.ScopeGitRead)
	route("GET /api/v1/repos/tag", false, s.handleGetTag, auth.ScopeGitRead)
	route("GET /api/v1/repos/blame", false, s.handleBlame, auth.ScopeGitRead)
	route("POST /api/v1/repos/grep", false, s.handleGrep, auth.ScopeGitRead)
	route("POST /api/v1/repos/archive", false, s.handleArchive, auth.ScopeGitRead)

	// Notes read surface (docs/03 "Notes"; handlers_read_notes.go). The
	// notes write routes live in registerWriteRefRoutes below.
	route("GET /api/v1/repos/notes", false, s.handleGetNote, auth.ScopeGitRead)
	route("GET /api/v1/repos/notes/refs", false, s.handleListNotesRefs, auth.ScopeGitRead)

	// git:write surface (docs/07 p4). The route/scope tables live next to
	// the handlers (handlers_write_commit.go, handlers_write_refs.go).
	registerWriteCommitRoutes(mux, s, cfg.Verifier, rl)
	s.registerWriteRefRoutes(mux, cfg.Verifier, rl)
	s.registerWriteMergeRoutes(mux, cfg.Verifier, rl)

	// Phase-5 routes: webhook CRUD (docs/06), git-credentials CRUD and
	// upstream sync (docs/03).
	RegisterWebhookRoutes(mux, cfg.Verifier, events.MetaWebhookStore{Store: cfg.Store}, logger, rl)
	registerCredentialRoutes(mux, cfg.Verifier, ss, rl)
	registerSyncRoutes(mux, cfg.Verifier, ss, rl)

	// Phase-6 routes: named-repo aliases /api/repos/{repo_name}/...
	// (alias.go) and GET /api/repo-urls/{repo_id} (repourls.go). Both are
	// registered last so their patterns cannot shadow the v1 surface they
	// dispatch to.
	registerAliasRoutes(mux, s, ss, cfg.Verifier, rl)
	registerRepoURLRoutes(mux, s, cfg.Verifier, rl)

	if cfg.Fallback != nil {
		mux.Handle("/", cfg.Fallback)
	}

	return recoverPanics(logger)(logRequests(logger)(mux)), nil
}
