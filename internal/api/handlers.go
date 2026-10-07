package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// server holds handler dependencies; one instance backs all routes.
type server struct {
	store       meta.Store
	httpBaseURL string       // no trailing slash (normalized in New)
	logger      *slog.Logger // never nil (defaulted in New)

	// Read-plumbing dependencies for the git:read endpoints (all required,
	// validated in New): handlers call gitread.Materialize(refStore,
	// blobStore, cacheDir, repo) and run plumbing over the returned path.
	refStore  ref.RefStore
	blobStore blob.BlobStore
	cacheDir  string

	// maybeRepack is the post-RegisterPacks repack-threshold trigger
	// (issue #1); nil disables triggering. Write pipelines invoke it via
	// triggerRepack after a successful pack registration.
	maybeRepack func(ctx context.Context, repo string)

	// packQuotaBytes mirrors the git surface's per-repo pack quota onto
	// the REST pack writers (issue #21); <= 0 disables enforcement.
	packQuotaBytes int64
	// packReservationTTL bounds how long a crashed writer's quota
	// reservation gates admission (issue #29, ref/ledger.go).
	packReservationTTL time.Duration
	// usageFloorFn resolves the pack-admission usage floor (Config.
	// UsageFloor); nil falls back to a fresh listing per write.
	usageFloorFn func(ctx context.Context, repo string) (int64, int64, error)
}

// usageFloor resolves the pack-admission UNCOUNTED-bytes floor: the serve
// command wires the git server's shared single-flight cached measurement
// (Codex round 21 — a fresh listing per REST pack write re-scans the
// repo's pack and LFS prefixes, ~100 pages per write at the LFS object
// cap). Staleness is safe for the FLOOR: registered bytes are covered by
// the ledger counter and in-flight writes by reservations; the floor
// exists for pre-ledger and orphaned bytes (see
// githttp.Server.CachedUsageFloor). Direct construction without the
// wiring keeps a fresh measurement: the counter is read BEFORE the
// listing (the conservative ordering — Codex round 44) and subtracted
// like every other floor producer, because ReservePackBytes SUMS the
// floor with its live counter — a raw listing would double-charge every
// registered byte (Codex round 45).
func (s *server) usageFloor(ctx context.Context, repo string) (int64, int64, error) {
	if s.usageFloorFn != nil {
		return s.usageFloorFn(ctx, repo)
	}
	// The generation is read BEFORE the measurement (Codex round 60):
	// any restore, adoption, or debit committing after this read moves
	// the generation, and ReservePackBytes' transactional recheck then
	// rejects the floor rather than admitting against it.
	gen, err := s.refStore.LFSAdoptionGen(ctx, githttp.BaseRepo(repo))
	if err != nil {
		return 0, 0, fmt.Errorf("floor gen read: %w", err)
	}
	ls, err := s.refStore.QuotaLedgerState(ctx, blob.QuotaRepoGroup(repo), time.Hour)
	if err != nil {
		return 0, 0, fmt.Errorf("usage counter read: %w", err)
	}
	listing, err := blob.StorageUsageBytes(ctx, s.blobStore, repo)
	if err != nil {
		return 0, 0, err
	}
	return max(0, listing-ls.UsageBytes), gen, nil
}

// triggerRepack runs the repack-threshold check after a successful pack
// registration. Cancel-free context: the trigger enqueues background work
// that must not be suppressed by the request ending right after its write.
func (s *server) triggerRepack(ctx context.Context, repo string) {
	if s.maybeRepack == nil {
		return
	}
	s.maybeRepack(context.WithoutCancel(ctx), repo)
}

// maxBodyBytes caps request bodies; the only JSON body we accept
// ({default_branch}) is tiny.
const maxBodyBytes = 1 << 20

// mustClaims returns the verified claims injected by requireAuth. Handlers
// are only reachable through the auth middleware, so absence is a bug.
func mustClaims(r *http.Request) *auth.Claims {
	claims, ok := auth.ClaimsFromContext(r.Context())
	assertf(ok, "handler reached without verified claims (route not wrapped in requireAuth?)")
	return claims
}

// refPolicies parses the token's ref-policy claims the same way the git
// update hook does (docs/reference/authentication.md): the ordered `refs` tuples with the deprecated
// repo-wide `ops` claim folded in exactly once. Malformed claims fail
// closed — callers must map the error to 403.
func refPolicies(claims *auth.Claims) ([]auth.RefPolicy, error) {
	var policies []auth.RefPolicy
	if len(claims.Refs) > 0 {
		var err error
		policies, err = auth.ParseRefPolicies(string(claims.Refs))
		if err != nil {
			return nil, err
		}
	}
	return auth.FoldOps(policies, claims.Ops), nil
}

// policyDeniesRepoDelete reports whether any policy rule carries an op that
// denies deleting a matching ref: no-push denies every update, no-force-push
// denies deletions (hook semantics), and unknown ops fail closed. Only
// verify-sig never denies a deletion — it polices introduced commits, and
// deletions introduce none.
func policyDeniesRepoDelete(policies []auth.RefPolicy) (string, bool) {
	for _, rule := range policies {
		for _, op := range rule.Ops {
			if op != auth.OpVerifySig {
				return op, true
			}
		}
	}
	return "", false
}

// branchComponentRe matches one conservative git ref component: leading
// alphanumeric, then alphanumerics plus . _ - (docs/reference/http-api.md: invalid input -> 400).
var branchComponentRe = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]*\z`)

// validBranchName reports whether name is an acceptable branch/tag name:
// slash-separated conservative ref components, no "..", no ".lock" suffix,
// no trailing dot. Within this charset those are exactly git's own
// check-ref-format rules — every accepted name must survive as a real ref,
// because stored default branches now drive `symbolic-ref HEAD` on every
// hydrate (issue #18) and a git-rejected name would poison hydration.
func validBranchName(name string) bool {
	if name == "" || len(name) > 255 || strings.Contains(name, "..") {
		return false
	}
	for comp := range strings.SplitSeq(name, "/") {
		if !branchComponentRe.MatchString(comp) ||
			strings.HasSuffix(comp, ".lock") || strings.HasSuffix(comp, ".") {
			return false
		}
	}
	return true
}

type createRepoRequest struct {
	// RepoName is the optional upstream `repo_name` (issue #293). It never
	// names the repo: the JWT repo claim always does, and a present
	// repo_name must equal it (repoNameMatches). nil = absent or null.
	RepoName      *string         `json:"repo_name"`
	DefaultBranch string          `json:"default_branch"`
	BaseRepo      json.RawMessage `json:"base_repo"`
	// Upstream is upstream's UpstreamConfig Git Sync form (issue #359);
	// only (*syncServer).handleCreateRepo accepts it.
	Upstream json.RawMessage `json:"upstream"`
}

// jsonPresent reports whether an optional raw JSON field was sent with a
// non-null value.
func jsonPresent(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// repoNameMismatchMsg is the 400 body for a repo_name that differs from the
// JWT repo claim (upstream documents 400 for this case).
const repoNameMismatchMsg = "repo_name must equal the JWT repo claim"

// repoNameMatches reports whether the optional body repo_name is absent or
// exactly (byte-for-byte, no normalization) equal to the JWT repo claim.
// Every POST /repos creator checks it right after decoding the body, before
// validating default_branch or base_repo values and before any side effect,
// so a body name can confirm the claim but never override it. A body that
// fails to decode (e.g. a non-string sibling field) is rejected earlier with
// the generic invalid-JSON 400; either way nothing is created.
func (req *createRepoRequest) repoNameMatches(claim string) bool {
	assertf(req != nil, "repoNameMatches: nil request")
	assertf(claim != "" && claim != auth.RepoClaimOrg, "repoNameMatches: non-repo claim %q", claim)
	return req.RepoName == nil || *req.RepoName == claim
}

type createRepoResponse struct {
	RepoID   string `json:"repo_id"`
	RepoName string `json:"repo_name"`
	HTTPURL  string `json:"http_url"`
	Message  string `json:"message"`
}

// repoCreatedMsg is upstream's ReposCreateResponse message for a 201.
func repoCreatedMsg(name string) string { return "Repository '" + name + "' created" }

// answerTakenName answers a create whose name is taken (CreateRepo returned
// meta.ErrAlreadyExists). A live repo is 409, whatever the create form:
// a fork's repo row is visible before its pack is published, so a 200
// could name a repo that the failing fork then deletes (issue #388). A
// soft-deleted or purged repo keeps its name reserved: 404, and nothing is
// restored. The caller already passed the create route's auth (repo:write,
// repo claim = this name, the token's org), so the lookup reads nothing
// outside what that token can see.
func answerTakenName(w http.ResponseWriter, r *http.Request, store meta.Store,
	internalErr func(http.ResponseWriter, *http.Request, string, error), org, url string,
) {
	assertf(store != nil && internalErr != nil && org != "" && url != "", "answerTakenName: missing inputs")
	_, err := store.GetRepoByURL(r.Context(), org, url)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository "+strconv.Quote(url)+" was deleted, and its name cannot be reused")
	case err != nil:
		internalErr(w, r, "get existing repository", err)
	default:
		writeErr(w, http.StatusConflict, "repository already exists")
	}
}

// handleCreateRepo implements POST /api/v1/repos (scope repo:write). The new
// repo's url is the JWT `repo` claim; the optional body carries
// {repo_name?, default_branch?, base_repo?} (docs/reference/http-api.md).
func (s *server) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	url := claims.Repo
	// requireAuth(orgLevel=false) rejects empty/"org" repo claims with 403.
	assertf(url != "" && url != auth.RepoClaimOrg, "create: repo claim %q passed requireAuth", url)

	// docs/reference/authentication.md: ref policies apply to every HTTP write endpoint. A new repo
	// has no refs, so any well-formed policy allows creation — but a
	// malformed policy claim fails closed, matching the git hook.
	if _, err := refPolicies(claims); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	var req createRepoRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
	}
	if !req.repoNameMatches(url) {
		writeErr(w, http.StatusBadRequest, repoNameMismatchMsg)
		return
	}
	// Fork/upstream sync is a later phase; silently dropping base_repo would
	// lie to clients, so reject it outright.
	if len(req.BaseRepo) > 0 && string(req.BaseRepo) != "null" {
		writeErr(w, http.StatusBadRequest, "base_repo not supported")
		return
	}
	if jsonPresent(req.Upstream) {
		writeErr(w, http.StatusBadRequest, "upstream not supported")
		return
	}
	branch := req.DefaultBranch
	if branch == "" {
		// openapi.yaml: optional query override, used only when the body
		// does not already specify a default branch.
		branch = r.URL.Query().Get("default_branch")
	}
	if branch == "" {
		branch = "main"
	}
	if !validBranchName(branch) {
		writeErr(w, http.StatusBadRequest, "invalid default_branch")
		return
	}

	repo, err := s.store.CreateRepo(r.Context(), claims.Iss, url, branch)
	switch {
	case errors.Is(err, meta.ErrAlreadyExists):
		answerTakenName(w, r, s.store, s.internalErr, claims.Iss, url)
		return
	case errors.Is(err, meta.ErrNotFound):
		// Unreachable after successful auth (the org must exist to have
		// verification keys), but map it defensively.
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case err != nil:
		s.internalErr(w, r, "create repository", err)
		return
	}
	assertf(repo.RepoID != "", "store returned repo with empty RepoID")
	writeJSON(w, http.StatusCreated, createRepoResponse{
		RepoID:   repo.RepoID,
		RepoName: repo.URL,
		HTTPURL:  s.httpBaseURL + "/" + repo.RepoID + ".git",
		Message:  repoCreatedMsg(repo.URL),
	})
}

// handleGetRepo implements GET /api/v1/repo (scope git:read). The target
// repo is the JWT `repo` claim; 404 when absent or soft-deleted. The
// response schema (openapi.yaml) requires repo_id, repo_name, url
// (deprecated mirror of repo_name), default_branch, created_at —
// repoEntry carries exactly those, plus upstream and its deprecated
// base_repo mirror when the repo has an upstream link (docs/reference/http-api.md, docs/archive/phases/14-phase5-implementation-notes.md deferred #2).
func (s *server) handleGetRepo(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	repo, err := s.store.GetRepoByURL(r.Context(), claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case err != nil:
		s.internalErr(w, r, "get repository", err)
		return
	}
	base, err := s.baseRepoOf(r.Context(), repo.RepoID)
	if err != nil {
		s.internalErr(w, r, "resolve base_repo", err)
		return
	}
	writeJSON(w, http.StatusOK, repoEntry{
		RepoID:        repo.RepoID,
		RepoName:      repo.URL,
		URL:           repo.URL,
		DefaultBranch: repo.DefaultBranch,
		CreatedAt:     repo.CreatedAt.UTC().Format(time.RFC3339),
		Upstream:      base,
		BaseRepo:      base,
	})
}

// validRefFormat reports whether git itself accepts target as a ref name
// (`git check-ref-format`, no repository needed). validBranchName's
// conservative charset is necessary but not sufficient — git additionally
// rejects e.g. trailing dots — and only git's own verdict guarantees the
// later `symbolic-ref HEAD <target>` succeeds.
func validRefFormat(ctx context.Context, target string) bool {
	assertf(strings.HasPrefix(target, "refs/"), "validRefFormat: unqualified target %q", target)
	return exec.CommandContext(ctx, "git", "check-ref-format", target).Run() == nil
}

// policyDeniesDefaultBranchChange reports whether the token's ref policies
// deny repointing HEAD at target (a fully qualified ref, HeadTarget form).
// The consulted rules are the first match for the pseudoref HEAD and for
// the new target ref (first-match-wins, like the update hook). Repointing
// HEAD moves no refs and introduces no commits, so no-force-push
// (deletions/rewrites) and verify-sig (introduced commits) never deny —
// but no-push denies every update to a covered ref, including which ref
// default clones follow, and unknown ops fail closed.
func policyDeniesDefaultBranchChange(policies []auth.RefPolicy, target string) (string, bool) {
	for _, ref := range []string{"HEAD", target} {
		rule, ok := auth.MatchPolicy(policies, ref)
		if !ok {
			continue
		}
		for _, op := range rule.Ops {
			if op != auth.OpVerifySig && op != auth.OpNoForcePush {
				return op, true
			}
		}
	}
	return "", false
}

// patchRepoRequest is the PATCH /repo body: default_branch is the only
// supported setting (openapi.yaml update_repo).
type patchRepoRequest struct {
	DefaultBranch string `json:"default_branch"`
}

// patchRepoResponse mirrors the live-spec PATCH schema exactly: repo_id and
// repo_name plus their deprecated mirrors, and the branch name now recorded
// as the default — unlike GET /repo's repoEntry, no created_at.
type patchRepoResponse struct {
	RepoID        string `json:"repo_id"`
	ID            string `json:"id"` // deprecated: mirrors repo_id
	RepoName      string `json:"repo_name"`
	URL           string `json:"url"` // deprecated: mirrors repo_name
	DefaultBranch string `json:"default_branch"`
}

// handlePatchRepo implements PATCH /api/v1/repo (scope repo:write): change
// the default branch of the JWT `repo` claim's repository (openapi.yaml
// update_repo). The row update is a CAS against the default branch read in
// this request (upstream: "a transactional compare-and-swap"; a concurrent
// change is a 409). Upstream contract: plain names are qualified for you
// (`main` becomes the symref target `refs/heads/main`) while values already
// starting with `refs/` are used verbatim, and the branch is never created
// by this call — the target may be unborn (POST /repos likewise accepts an
// unborn default on an empty repo), in which case clones see a dangling
// HEAD until the branch is pushed, exactly like a freshly created repo.
// Cache-repo HEADs follow on the next hydrate: the hydrate marker records
// the synced HEAD target, so a changed default branch defeats the issue-#4
// short-circuit (githttp/hydrate.go).
func (s *server) handlePatchRepo(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	// docs/reference/authentication.md: ref policies apply to every HTTP write endpoint; malformed
	// policy claims fail closed, and the parsed rules gate the HEAD
	// repoint below (policyDeniesDefaultBranchChange).
	policies, err := refPolicies(claims)
	if err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	var req patchRepoRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
	}
	branch := req.DefaultBranch
	if branch == "" {
		// openapi.yaml: the body is required and default_branch must be a
		// non-empty branch name.
		writeErr(w, http.StatusBadRequest, "default_branch is required")
		return
	}
	// default_branch names a BRANCH: a plain name (validated with the same
	// conservative validBranchName as POST /repos' identical field) or its
	// refs/heads/-qualified spelling, stored verbatim (upstream contract).
	// Other ref namespaces are rejected: a refs/tags/... HEAD would evade
	// every default-deletion protection and dangle on tag deletion — and
	// upstream's field is, by name and description, a branch. The derived
	// HEAD target must additionally satisfy git's own ref-format rules
	// (e.g. no trailing dot, which validBranchName's charset permits):
	// hydration runs `symbolic-ref HEAD <target>` on every node, and a
	// persisted target git rejects would break the repo's git/read
	// surface until another PATCH repairs the row.
	target := githttp.HeadTarget(branch)
	shortName := strings.TrimPrefix(branch, "refs/heads/")
	if strings.HasPrefix(shortName, "refs/") || !validBranchName(shortName) || !validRefFormat(r.Context(), target) {
		writeErr(w, http.StatusBadRequest, "invalid default_branch")
		return
	}
	// The policy target is the derived HEAD symref target, not the
	// NormalizeRefName shorthand: for the literal branch name "HEAD" the
	// two disagree (refs/heads/HEAD vs the pseudoref), and rules written
	// against refs/heads/HEAD must match. The pseudoref itself is checked
	// separately inside the helper.
	if op, denied := policyDeniesDefaultBranchChange(policies, target); denied {
		writeErr(w, http.StatusForbidden, "ref policy op "+strconv.Quote(op)+" denies changing the default branch")
		return
	}
	// ADR-21: the repo claim carries the url or the repo_id; resolve either
	// (resolveRepo writes the 404/500 itself).
	repo, ok := s.resolveRepo(w, r)
	if !ok {
		return
	}
	won, err := s.store.UpdateRepoDefaultBranch(r.Context(), claims.Iss, repo.URL, repo.DefaultBranch, branch)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		// The repo was soft-deleted between the read above and the CAS.
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case err != nil:
		s.internalErr(w, r, "update default branch", err)
		return
	case !won:
		writeErr(w, http.StatusConflict, "default branch was modified concurrently, please retry")
		return
	}
	writeJSON(w, http.StatusOK, patchRepoResponse{
		RepoID:        repo.RepoID,
		ID:            repo.RepoID,
		RepoName:      repo.URL,
		URL:           repo.URL,
		DefaultBranch: branch,
	})
}

// baseRepoEntry is the GET-response `upstream` object and its deprecated
// `base_repo` mirror, upstream's UpstreamRepo schema
// (docs/reference/http-api.md): the upstream coordinates a synced repo
// mirrors.
type baseRepoEntry struct {
	Provider string `json:"provider"`
	Owner    string `json:"owner"`
	Name     string `json:"name"`
}

// baseRepoOf resolves the repo's repo_upstreams row to the response
// base_repo object; nil (field omitted) when the repo has no upstream.
func (s *server) baseRepoOf(ctx context.Context, repoID string) (*baseRepoEntry, error) {
	assertf(repoID != "", "baseRepoOf: empty repo id")
	up, err := s.store.GetRepoUpstream(ctx, repoID)
	if errors.Is(err, meta.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	assertf(up.Provider != "" && up.Owner != "" && up.Name != "",
		"upstream row for %q has empty coordinates: %+v", repoID, up)
	return &baseRepoEntry{Provider: up.Provider, Owner: up.Owner, Name: up.Name}, nil
}

type repoEntry struct {
	RepoID        string `json:"repo_id"`
	RepoName      string `json:"repo_name"`
	URL           string `json:"url"` // deprecated: mirrors repo_name
	DefaultBranch string `json:"default_branch"`
	CreatedAt     string `json:"created_at"`
	// Upstream and BaseRepo (deprecated upstream) carry the same value;
	// both are omitted when the repo has no upstream.
	Upstream *baseRepoEntry `json:"upstream,omitempty"`
	BaseRepo *baseRepoEntry `json:"base_repo,omitempty"`
}

type listReposResponse struct {
	Repos      []repoEntry `json:"repos"`
	NextCursor string      `json:"next_cursor,omitempty"`
	HasMore    bool        `json:"has_more"`
}

const (
	defaultListLimit = 20
	maxListLimit     = 100
)

// handleListRepos implements GET /api/v1/repos (scope org:read, repo claim
// "org"): keyset pagination via opaque cursor, `q` substring filter on url.
func (s *server) handleListRepos(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	query := r.URL.Query()

	limit := defaultListLimit
	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxListLimit {
			writeErr(w, http.StatusBadRequest, "invalid limit (want 1..100)")
			return
		}
		limit = n
	}

	res, err := s.store.ListRepos(r.Context(), claims.Iss, meta.ListParams{
		Q:      strings.TrimSpace(query.Get("q")),
		Cursor: query.Get("cursor"),
		Limit:  limit,
	})
	switch {
	case errors.Is(err, meta.ErrInvalidCursor):
		writeErr(w, http.StatusBadRequest, "invalid cursor")
		return
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case err != nil:
		s.internalErr(w, r, "list repositories", err)
		return
	}
	assertf(res.Repos != nil, "store returned nil Repos slice")

	entries := make([]repoEntry, 0, len(res.Repos))
	for _, repo := range res.Repos {
		// Per-repo upstream lookup (docs/archive/phases/14-phase5-implementation-notes.md deferred #2); pages are capped
		// at 100 entries, keeping the N+1 bounded.
		base, err := s.baseRepoOf(r.Context(), repo.RepoID)
		if err != nil {
			s.internalErr(w, r, "resolve base_repo", err)
			return
		}
		entries = append(entries, repoEntry{
			RepoID:        repo.RepoID,
			RepoName:      repo.URL,
			URL:           repo.URL,
			DefaultBranch: repo.DefaultBranch,
			CreatedAt:     repo.CreatedAt.UTC().Format(time.RFC3339),
			Upstream:      base,
			BaseRepo:      base,
		})
	}
	assertPageCursor(res.HasMore, res.NextCursor)
	writeJSON(w, http.StatusOK, listReposResponse{
		Repos:      entries,
		NextCursor: res.NextCursor,
		HasMore:    res.HasMore,
	})
}

type deleteRepoResponse struct {
	RepoID   string `json:"repo_id"` // deprecated upstream: use repo_name
	RepoName string `json:"repo_name"`
	Message  string `json:"message"`
}

// handleDeleteRepo implements DELETE /api/v1/repos/delete (scope
// repo:write): soft delete of the JWT `repo` claim target. 404 when the
// repo never existed, 409 when already soft-deleted (docs/explanation/domain-model.md, docs/reference/http-api.md).
func (s *server) handleDeleteRepo(w http.ResponseWriter, r *http.Request) {
	claims := mustClaims(r)
	// docs/reference/authentication.md: ref policies apply to every HTTP write endpoint, and deleting
	// a repository destroys every ref in it. The API has no ref inventory
	// (meta.Store carries no refs), so this fails closed: any rule that
	// could deny deleting some ref denies the whole-repo delete, even when
	// an earlier rule would shadow it for every existing ref.
	policies, err := refPolicies(claims)
	if err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	if op, denied := policyDeniesRepoDelete(policies); denied {
		writeErr(w, http.StatusForbidden, "ref policy op "+strconv.Quote(op)+" denies repository deletion")
		return
	}
	repo, err := s.store.SoftDeleteRepo(r.Context(), claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	case errors.Is(err, meta.ErrAlreadyDeleted):
		writeErr(w, http.StatusConflict, "repository already deleted")
		return
	case err != nil:
		s.internalErr(w, r, "delete repository", err)
		return
	}
	assertf(repo.RepoID != "", "store returned deleted repo with empty RepoID")
	assertf(repo.URL != "", "store returned deleted repo %s with empty URL", repo.RepoID)
	writeJSON(w, http.StatusOK, deleteRepoResponse{
		RepoID:   repo.RepoID,
		RepoName: repo.URL,
		Message:  "repository deleted",
	})
}
