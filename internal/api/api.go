// Package api implements the SDK-compatible HTTP API surface (docs/reference/http-api.md):
// repo CRUD under /api/v1 with the {"error": "..."} envelope, JWT bearer
// auth (docs/reference/authentication.md), and panic-recovery/request-log middleware.
//
// The package depends only on the frozen phase-1 contracts meta.Store and
// auth.Verifier; wiring concrete implementations is the serve command's job.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	upsync "github.com/coder/openstorage/internal/sync"
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
	// Org is the single served org (ADR-8), the same value the serve
	// command passes to githttp.NewServer. When set, every REST route
	// rejects verified tokens whose `iss` differs with 403 (issue #47),
	// mirroring the git surface's served-org gate. The zero value disables
	// the gate (matching this Config's other zero-value-disables knobs);
	// it exists ONLY for this repo's multi-org integration harness — the
	// serve command always sets Org.
	Org string
	// HTTPBaseURL is the externally reachable git base URL (scheme://host),
	// used to build the `http_url` in the POST /repos response, e.g.
	// "https://selfhost.open.storage".
	HTTPBaseURL string
	Fallback    http.Handler

	// RefStore/BlobStore/CacheDir back the git:read endpoints (docs/archive/phases/07-implementation-roadmap.md p3):
	// handlers materialize the repo cache via gitread.Materialize(RefStore,
	// BlobStore, CacheDir, ...) and run read plumbing over it. The serve
	// command passes the same instances the githttp fallback uses.
	RefStore  ref.RefStore
	BlobStore blob.BlobStore
	CacheDir  string

	// SyncQueue schedules upstream sync runs (docs/archive/phases/07-implementation-roadmap.md p5): base_repo
	// creates and pull-upstream enqueue through it. The serve command
	// passes the jobs seam (River on pg, in-process in local mode); tests
	// may pass a recorder/no-op.
	SyncQueue upsync.Queue

	// RateLimit enables the pre-auth per-client-IP token bucket on every
	// REST route (docs/reference/http-api.md gateway-level 429; see ratelimit.go). The zero
	// value disables limiting. The serve command plumbs it from
	// OPENSTORAGE_RATE_RPS / OPENSTORAGE_RATE_BURST.
	RateLimit RateLimitConfig

	// MaybeRepack, when set, runs after every successful RegisterPacks on
	// a write pipeline (issue #1): the serve command passes the repack
	// trigger's threshold check, which enqueues at most one unique-by-repo
	// background fold. It must be quick and never block on the fold. Nil
	// disables triggering.
	MaybeRepack func(ctx context.Context, repo string)

	// PackQuotaBytes is the per-repo pack-bytes quota (issue #21), the same
	// value the git surface enforces at pre-receive: REST pack writers
	// (commit-pack, merge, notes, fork via publishPack) publish under the
	// same packs/ prefix and must not bypass it. <= 0 disables enforcement.
	// The serve command plumbs it from OPENSTORAGE_PACK_QUOTA_BYTES.
	PackQuotaBytes int64

	// PackReservationTTL bounds how long a crashed writer's quota
	// reservation gates admission (issue #29, ref/ledger.go). <= 0 falls
	// back to a safe one-hour default. The serve command plumbs it from
	// OPENSTORAGE_PACK_RESERVATION_TTL.
	PackReservationTTL time.Duration

	// UsageFloor, when set, resolves the pack-admission usage floor for a
	// repo. The serve command wires the git server's shared single-flight
	// cached listing (githttp.Server.CachedUsageFloor), so REST pack
	// writers do not re-list the repo's pack and LFS prefixes on every
	// write (issue #51, Codex round 21 — per-write listings amplify at
	// the LFS object cap). Nil falls back to a fresh listing per write.
	// Returns the floor plus the shared floor generation it was
	// validated against (forwarded into ReservePackBytes' transactional
	// recheck — Codex round 60).
	UsageFloor func(ctx context.Context, repo string) (int64, int64, error)

	// WebhookAllowPrivate relaxes the webhook SSRF guard's private-range
	// denial (issue #53) for both the registration-time courtesy check
	// here and — plumbed separately into the delivery workers — the
	// authoritative dial-time enforcement. The zero value keeps the guard
	// strict (the safe default). The serve command plumbs it from
	// OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE.
	WebhookAllowPrivate bool

	// SyncPullAllowPrivate mirrors the resolved pull-lane sync egress
	// policy (issue #139) for the registration-time courtesy check on
	// base_repo upstream_host; the authoritative dial-time enforcement is
	// the sync worker's vetting proxy, plumbed separately. The serve
	// command resolves it from OPENSTORAGE_SYNC_ALLOW_PRIVATE, whose
	// transitional unset default keeps it TRUE (0.6.x compatibility), so
	// unlike the webhook knob the zero value here is the STRICT policy.
	SyncPullAllowPrivate bool
}

// New builds the full HTTP handler: recover -> request log -> router, with
// per-route JWT auth (scope + org-level enforcement per docs/reference/authentication.md). Invalid
// operator-supplied config (RateLimit) returns an error instead of
// panicking (docs/archive/phases/15-phase6-implementation-notes.md deferred #4); nil required dependencies are still
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
		store:              cfg.Store,
		httpBaseURL:        strings.TrimRight(cfg.HTTPBaseURL, "/"),
		logger:             logger,
		refStore:           cfg.RefStore,
		blobStore:          cfg.BlobStore,
		cacheDir:           cfg.CacheDir,
		maybeRepack:        cfg.MaybeRepack,
		packQuotaBytes:     cfg.PackQuotaBytes,
		packReservationTTL: cfg.PackReservationTTL,
		usageFloorFn:       cfg.UsageFloor,
	}

	// Phase-5 eventing/sync surface (docs/archive/phases/07-implementation-roadmap.md p5): the syncServer extends
	// the frozen create flow with base_repo support, backed by the durable
	// meta rows through the sync-package adapters.
	creds, ups := upsync.MetaStores(cfg.Store)
	ss := newSyncServer(cfg.Store, creds, ups, cfg.SyncQueue, strings.TrimRight(cfg.HTTPBaseURL, "/"),
		events.TargetGuard{AllowPrivate: cfg.SyncPullAllowPrivate}, logger)

	// Pre-auth rate limiting (docs/reference/http-api.md; ratelimit.go). rl is nil when
	// disabled; rl.limit(...) is valid either way and wraps every REST
	// route registered below (v1, aliases, repo-urls) before auth.
	rl, err := newRateLimiter(cfg.RateLimit)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	route := func(pattern string, orgLevel bool, h http.HandlerFunc, scopes ...string) {
		mux.Handle(pattern, rl.limit(routeRead)(requireAuth(cfg.Verifier, cfg.Org, orgLevel, scopes...)(h)))
	}
	route("GET /api/meta", true, handleOrganizationMetadata, auth.ScopeOrgRead)

	// POST /repos is the base_repo-aware create: code/code.storage fork
	// specs dispatch to the synchronous fork (handlers_fork.go); everything
	// else falls back to the sync-aware creator (which embeds the frozen
	// (*server).handleCreateRepo flow).
	route("POST /api/v1/repos", false, s.forkAwareCreateRepo(cfg.Verifier, ss.handleCreateRepo), auth.ScopeRepoWrite)
	route("GET /api/v1/repo", false, s.handleGetRepo, auth.ScopeGitRead)
	route("PATCH /api/v1/repo", false, s.handlePatchRepo, auth.ScopeRepoWrite)
	route("GET /api/v1/repos", true, s.handleListRepos, auth.ScopeOrgRead)
	route("DELETE /api/v1/repos/delete", false, s.handleDeleteRepo, auth.ScopeRepoWrite)

	// git:read surface (docs/archive/phases/07-implementation-roadmap.md p3, docs/reference/http-api.md; scope table docs/reference/authentication.md:56). Go's
	// ServeMux "GET" patterns also match HEAD, covering HEAD /repos/file.
	// Routes that start git run under a per-request git budget (#313,
	// git_budget.go); the /api/repos/{repo_name}/... aliases re-dispatch
	// through these registrations and inherit it.
	route("GET /api/v1/repos/file", false, withGitFileBudget(s.handleGetFile), auth.ScopeGitRead)
	route("GET /api/v1/repos/files", false, withGitReadBudget(s.handleListFiles), auth.ScopeGitRead)
	route("GET /api/v1/repos/files/metadata", false, withGitReadBudget(s.handleFilesMetadata), auth.ScopeGitRead)
	route("GET /api/v1/repos/branches", false, s.handleListBranches, auth.ScopeGitRead)
	route("GET /api/v1/repos/branch", false, s.handleGetBranch, auth.ScopeGitRead)
	route("GET /api/v1/repos/branches/diff", false, withGitReadBudget(s.handleBranchesDiff), auth.ScopeGitRead)
	route("GET /api/v1/repos/commits", false, withGitReadBudget(s.handleListCommits), auth.ScopeGitRead)
	route("GET /api/v1/repos/commit", false, withGitReadBudget(s.handleGetCommit), auth.ScopeGitRead)
	route("GET /api/v1/repos/diff", false, withGitReadBudget(s.handleDiff), auth.ScopeGitRead)
	route("GET /api/v1/repos/tags", false, withGitReadBudget(s.handleListTags), auth.ScopeGitRead)
	route("GET /api/v1/repos/tag", false, withGitReadBudget(s.handleGetTag), auth.ScopeGitRead)
	route("GET /api/v1/repos/blame", false, withGitReadBudget(s.handleBlame), auth.ScopeGitRead)
	route("POST /api/v1/repos/grep", false, withGitReadBudget(s.handleGrep), auth.ScopeGitRead)
	route("POST /api/v1/repos/archive", false, withGitArchiveBudget(s.handleArchive), auth.ScopeGitRead)

	// Notes read surface (docs/reference/http-api.md "Notes"; handlers_read_notes.go). The
	// notes write routes live in registerWriteRefRoutes below.
	route("GET /api/v1/repos/notes", false, withGitReadBudget(s.handleGetNote), auth.ScopeGitRead)
	route("GET /api/v1/repos/notes/refs", false, s.handleListNotesRefs, auth.ScopeGitRead)

	// git:write surface (docs/archive/phases/07-implementation-roadmap.md p4). The route/scope tables live next to
	// the handlers (handlers_write_commit.go, handlers_write_refs.go).
	registerWriteCommitRoutes(mux, s, cfg.Verifier, cfg.Org, rl)
	s.registerWriteRefRoutes(mux, cfg.Verifier, cfg.Org, rl)
	s.registerWriteMergeRoutes(mux, cfg.Verifier, cfg.Org, rl)

	// Phase-5 routes: webhook CRUD (docs/reference/webhooks.md), git-credentials CRUD and
	// upstream sync (docs/reference/http-api.md).
	RegisterWebhookRoutes(mux, cfg.Verifier, cfg.Org, events.MetaWebhookStore{Store: cfg.Store}, events.TargetGuard{AllowPrivate: cfg.WebhookAllowPrivate}, logger, rl)
	registerCredentialRoutes(mux, cfg.Verifier, cfg.Org, ss, rl)
	registerSyncRoutes(mux, cfg.Verifier, cfg.Org, ss, rl)

	// Phase-6 routes: named-repo aliases /api/repos/{repo_name}/...
	// (alias.go) and GET /api/repo-urls/{repo_id} (repourls.go). Both are
	// registered last so their patterns cannot shadow the v1 surface they
	// dispatch to.
	registerAliasRoutes(mux, s, ss, cfg.Verifier, cfg.Org, rl)
	registerRepoURLRoutes(mux, s, cfg.Verifier, cfg.Org, rl)

	if cfg.Fallback != nil {
		mux.Handle("/", cfg.Fallback)
	}

	return recoverPanics(logger)(logRequests(logger)(mux)), nil
}
