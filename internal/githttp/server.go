// Package githttp is the git smart HTTP surface (docs/05): stateless HTTP
// nodes with Postgres as ref authority, S3/MinIO as immutable pack storage,
// and local disk as a rebuildable cache. Authentication follows docs/02:
// customer-signed JWTs verified against the issuing org's registered keys.
package githttp

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

// assertf is a fail-fast invariant check for conditions that indicate a
// programming error (never for request-dependent failures, which must map
// to 4xx responses).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("githttp assertion failed: "+format, args...))
	}
}

// Server is one stateless HTTP node: all durable truth lives in PG (refs,
// pack registry) and S3 (pack bytes); CacheDir is rebuildable at any time.
type Server struct {
	Store    ref.RefStore
	Blob     blob.BlobStore
	CacheDir string
	// Verifier authenticates docs/02 JWTs (multi-algorithm, iss-resolved
	// org keys, exp/sub checks). Any Verify failure maps to 401.
	Verifier auth.Verifier
	// Org is the single organization this node serves. A valid token from a
	// *different* registered org must not grant access (403); unknown orgs
	// already fail key resolution inside Verifier (401).
	Org string
	// TestMode gates fault-injection knobs (crash header). Only ever set
	// from CODESTORAGE_TEST_MODE=1; the crash header is refused otherwise.
	TestMode bool
	// ResolveRepo maps a base path element (docs/05: repo_id, but ADR-21
	// accepts name-style urls too) to its live repo record. ok=false when
	// no live repo matches — both handlers then answer 404 (repos must be
	// created via POST /api/v1/repos first). The record's RepoID is the
	// only storage key (repo-key-split fix); it also lets tokens carrying
	// either the url or the repo_id claim (docs/02) authenticate. When
	// nil, every path element is unresolvable.
	ResolveRepo func(ctx context.Context, base string) (meta.Repo, bool)
	// MaybeRepack, when set, runs after every receive-pack POST with the
	// resolved storage key (issue #1): pre-receive registers pushed packs
	// strictly before receive-pack returns, so this is the git surface's
	// post-RegisterPacks repack-threshold trigger. It must be quick
	// (a pack-count read plus at most one job enqueue) and must never
	// block on the fold itself. Nil disables triggering.
	MaybeRepack func(ctx context.Context, repo string)
	// PackQuotaBytes is the per-repo pack-bytes quota (issue #21, see
	// quota.go); <= 0 disables quota enforcement. Set by the serve wiring
	// from the resolved config, like ResolveRepo.
	PackQuotaBytes int64
	// PackReservationTTL bounds how long a crashed push's quota
	// reservation gates admission (issue #29, ref/ledger.go). Set by the
	// serve wiring; <= 0 leaves the hook on defaultReservationTTL.
	PackReservationTTL time.Duration
	// BaseURL is the externally reachable base URL (scheme://host, no
	// trailing slash) LFS proxy transfer hrefs are built on (issue #51:
	// uploads always proxy, so behind a reverse proxy the href must be
	// the canonical external URL — the backend request's Host header may
	// be internal). Set by the serve wiring from $CODESTORAGE_BASE_URL
	// (or the bound listener); when empty, hrefs fall back to the
	// request's Host and X-Forwarded-Proto.
	BaseURL string
	// usage memoizes per-repo pack usage for the quota-pressure log so
	// replayed cheap POSTs cannot amplify into per-request blob listings.
	usage usageCache
	// LFSStagingBytes bounds the aggregate bytes of proxy LFS uploads
	// staging on this replica at once (issue #51, Codex round 4): uploads
	// stage their full Content-Length under $TMPDIR before verification,
	// and the per-repository quota is neither node-wide nor guaranteed
	// enabled, so without this budget concurrent distinct-key uploads
	// could exhaust a replica's disk. Set by the serve wiring
	// ($CODESTORAGE_LFS_STAGING_BYTES, default 10 GiB); <= 0 disables the
	// bound (the operator's explicit opt-out).
	LFSStagingBytes int64
	// lfsUploadsInFlight tracks blob keys with a proxy upload currently
	// staging on this replica (issue #51, Codex security round): at most
	// one in-flight upload per key, so concurrent identical PUTs cannot
	// multiply quota-sized temp files (see lfsProxyUpload).
	lfsUploadsInFlight sync.Map
	// lfsStagingInFlight is the aggregate Content-Length currently
	// reserved against LFSStagingBytes; lfsStagingOps counts the
	// operations holding it (Codex round 17: tiny Content-Lengths pass a
	// byte-only budget while each holds a goroutine, connection, fd, and
	// temp file — the op count bounds that). Guarded by lfsStagingMu.
	lfsStagingInFlight int64
	lfsStagingOps      int
	lfsStagingMu       sync.Mutex
}

// lfsMaxStagingOps bounds concurrent staging operations per replica
// (uploads and legacy verifications alike), independent of bytes. A var so
// tests can shrink it.
var lfsMaxStagingOps = 256

// reserveLFSStaging reserves n bytes of the node-wide LFS staging budget
// and one staging-operation slot. ok=false when either does not fit right
// now (transient — the caller answers 429). n larger than the whole byte
// budget can never fit; the caller distinguishes that case up front. The
// op slot is counted even for n==0 and with the byte budget disabled: the
// operation count bounds goroutines/fds/temp files, which exist regardless
// of declared size.
func (s *Server) reserveLFSStaging(n int64) bool {
	assertf(n >= 0, "reserveLFSStaging(%d)", n)
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	if s.lfsStagingOps >= lfsMaxStagingOps {
		return false
	}
	if s.LFSStagingBytes > 0 && n > 0 && s.lfsStagingInFlight > s.LFSStagingBytes-n {
		return false
	}
	s.lfsStagingOps++
	if s.LFSStagingBytes > 0 {
		s.lfsStagingInFlight += n
	}
	return true
}

// lfsStagingChunk is the staging-budget growth quantum (a var for tests).
// Proxy uploads reserve one chunk ahead of the bytes actually received
// instead of their full declared Content-Length (Codex security round 5 on
// #51): an upfront declared-size reservation let a single header-plus-drip
// PUT declare the entire node budget and pin it for the whole reservation
// TTL while sending one byte per idle window, 429ing every unrelated
// upload on the replica. Growing as data arrives caps a dripper's hold at
// one chunk beyond what it genuinely transferred; the idle/absolute
// deadlines still bound the duration.
var lfsStagingChunk = int64(8 << 20)

// growLFSStaging adds n bytes to an existing staging reservation whose op
// slot is already held; the caller releases the grown total through one
// releaseLFSStaging call.
func (s *Server) growLFSStaging(n int64) bool {
	assertf(n > 0, "growLFSStaging(%d)", n)
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	if s.LFSStagingBytes > 0 {
		if s.lfsStagingInFlight > s.LFSStagingBytes-n {
			return false
		}
		s.lfsStagingInFlight += n
	}
	return true
}

// releaseLFSStaging returns a reserveLFSStaging reservation.
func (s *Server) releaseLFSStaging(n int64) {
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	s.lfsStagingOps--
	assertf(s.lfsStagingOps >= 0, "releaseLFSStaging: op count went negative")
	if s.LFSStagingBytes > 0 {
		s.lfsStagingInFlight -= n
		assertf(s.lfsStagingInFlight >= 0, "releaseLFSStaging(%d): in-flight went negative", n)
	}
}

// repoTarget is a resolved request target: the live repo record plus the
// canonical storage key — RepoID with the URL element's namespace suffix
// (`+ephemeral`/`+import`) reattached. The key is the ONLY value handed to
// hydrate, the pack/ref stores, and the update hook env; the raw path
// element must never key storage (ADR-21).
type repoTarget struct {
	repo meta.Repo
	key  string
}

// resolveTarget canonicalizes a validated path element to its repoTarget.
// ok=false means no live repo record matches the base element.
func (s *Server) resolveTarget(ctx context.Context, pathElem string) (repoTarget, bool) {
	assertf(ValidRepoName(pathElem), "resolveTarget: invalid path element %q", pathElem)
	if s.ResolveRepo == nil {
		return repoTarget{}, false
	}
	base := BaseRepo(pathElem)
	repo, ok := s.ResolveRepo(ctx, base)
	if !ok {
		return repoTarget{}, false
	}
	assertf(repo.RepoID != "", "resolveTarget: resolver returned empty RepoID for %q", base)
	suffix := strings.TrimPrefix(pathElem, base) // "" | "+ephemeral" | "+import"
	return repoTarget{repo: repo, key: repo.RepoID + suffix}, true
}

func NewServer(st ref.RefStore, bl blob.BlobStore, cacheDir string, verifier auth.Verifier, org string) *Server {
	assertf(st != nil && bl != nil && verifier != nil && cacheDir != "" && org != "", "NewServer: missing dependency")
	return &Server{
		Store:    st,
		Blob:     bl,
		CacheDir: cacheDir,
		Verifier: verifier,
		Org:      org,
		TestMode: os.Getenv(EnvTestMode) == "1",
	}
}

func httpError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// Handler routes the smart-HTTP and LFS endpoints per repo (docs/05):
//
//	GET  /{repo}.git/info/refs?service=git-{upload|receive}-pack
//	POST /{repo}.git/git-{upload|receive}-pack
//	POST /{repo}.git/info/lfs/objects/batch
//	GET  /{repo}.git/info/lfs/objects/{oid}   (proxy transfer, local mode)
//	PUT  /{repo}.git/info/lfs/objects/{oid}   (proxy transfer, local mode)
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		repo, rest, ok := splitRepoPath(r.URL.Path)
		if !ok {
			httpError(w, http.StatusNotFound, "not a smart-http path")
			return
		}
		switch {
		case r.Method == http.MethodGet && rest == "info/refs":
			s.handleInfoRefs(w, r, repo)
		case r.Method == http.MethodPost && (rest == "git-upload-pack" || rest == "git-receive-pack"):
			s.handleServiceRPC(w, r, repo, strings.TrimPrefix(rest, "git-"))
		case strings.HasPrefix(rest, "info/lfs/"):
			s.handleLFS(w, r, repo, strings.TrimPrefix(rest, "info/lfs/"))
		default:
			httpError(w, http.StatusNotFound, "unsupported endpoint")
		}
	})
}

// splitRepoPath parses "/{repo}.git/{rest}" and validates the repo element,
// which may carry a `+ephemeral`/`+import` namespace suffix (docs/05).
func splitRepoPath(path string) (repo, rest string, ok bool) {
	path = strings.TrimPrefix(path, "/")
	elem, rest, found := strings.Cut(path, "/")
	if !found || !strings.HasSuffix(elem, ".git") {
		return "", "", false
	}
	repo = strings.TrimSuffix(elem, ".git")
	if !ValidRepoName(repo) {
		return "", "", false
	}
	return repo, rest, true
}

// authenticate enforces the docs/02 failure mapping on the git path: 401 for
// a missing/invalid token (any Verifier failure), 403 for a valid token that
// does not fit this org, repo, or scope.
func (s *Server) authenticate(r *http.Request, repo, scope string) (*auth.Claims, int, error) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != "t" {
		return nil, http.StatusUnauthorized, fmt.Errorf("basic auth with username 't' required")
	}
	return s.verifyClaims(r.Context(), pass, repo, scope)
}

// verifyClaims is the token-to-claims half of authenticate, shared with the
// LFS endpoints (which also accept `Authorization: Bearer <jwt>` per the LFS
// batch spec). Same failure mapping: 401 for Verifier failures, 403 for
// org/repo/scope mismatch.
func (s *Server) verifyClaims(ctx context.Context, token, repo, scope string) (*auth.Claims, int, error) {
	assertf(s.Verifier != nil && s.Org != "", "Server used without Verifier/Org")
	claims, err := s.Verifier.Verify(ctx, token)
	if err != nil {
		return nil, http.StatusUnauthorized, err
	}
	// A signature valid for some *other* registered org must not grant
	// access to this org's repos.
	if claims.Iss != s.Org {
		return nil, http.StatusForbidden, fmt.Errorf("token issued for org %q, not served here", claims.Iss)
	}
	// Git endpoints are always repo-scoped (docs/02): an org-level or empty
	// `repo` claim never grants git access — not even to a repo literally
	// named "org", which would otherwise satisfy the verbatim compare
	// below. Mirrors the API middleware's repo-scope gate.
	if claims.Repo == "" || claims.Repo == auth.RepoClaimOrg {
		return nil, http.StatusForbidden, fmt.Errorf("token is not repo-scoped")
	}
	// docs/02: the repo claim carries the repo url (or id); docs/05 paths
	// address repos by repo_id but name-style urls resolve too (ADR-21).
	// Accept a verbatim match, else resolve the path element and compare
	// against both the record's repo_id and url. Auth stays before the
	// existence check: an unresolvable element with a mismatched claim is
	// a 403 here, never a repo-existence oracle.
	target := BaseRepo(repo)
	if claims.Repo != target {
		matched := false
		if s.ResolveRepo != nil {
			if rec, ok := s.ResolveRepo(ctx, target); ok && (claims.Repo == rec.RepoID || claims.Repo == rec.URL) {
				matched = true
			}
		}
		if !matched {
			return nil, http.StatusForbidden, fmt.Errorf("jwt repo claim mismatch")
		}
	}
	if !claims.HasScope(scope) {
		return nil, http.StatusForbidden, fmt.Errorf("missing scope %s", scope)
	}
	return claims, 0, nil
}

func serviceScope(service string) string {
	switch service {
	case "upload-pack":
		return auth.ScopeGitRead
	case "receive-pack":
		return auth.ScopeGitWrite
	}
	panic("githttp assertion failed: unknown service " + service)
}

// hydrate rebuilds cache state for the request. Ref-sync failures are fatal
// on the read path (stale advertisement would break clone correctness) but
// tolerated on the receive path, where a concurrent receive-pack may hold
// ref locks and git re-validates client old-SHAs itself.
func (s *Server) hydrate(ctx context.Context, repo, service, defaultBranch string) (string, error) {
	repoPath, refsErr, err := Hydrate(ctx, s.Store, s.Blob, s.CacheDir, repo, defaultBranch)
	if err != nil {
		return "", err
	}
	if refsErr != nil {
		if service == "upload-pack" {
			return "", refsErr
		}
		log.Printf("hydrate %s: tolerated ref-sync failure on receive path: %v", repo, refsErr)
	}
	return repoPath, nil
}

func pktLine(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }

func (s *Server) handleInfoRefs(w http.ResponseWriter, r *http.Request, repo string) {
	svcParam := r.URL.Query().Get("service")
	if svcParam != "git-upload-pack" && svcParam != "git-receive-pack" {
		httpError(w, http.StatusBadRequest, "dumb protocol not supported; use service=git-upload-pack|git-receive-pack")
		return
	}
	service := strings.TrimPrefix(svcParam, "git-")
	if _, status, err := s.authenticate(r, repo, serviceScope(service)); err != nil {
		// RFC 9110 §11.6.1: the challenge belongs to 401 only; a 403 means
		// the presented credentials were understood but denied.
		if status == http.StatusUnauthorized {
			w.Header().Set("WWW-Authenticate", `Basic realm="codestorage"`)
		}
		httpError(w, status, err.Error())
		return
	}
	// Auth before existence: only authenticated callers learn whether the
	// repo is registered (ADR-21: unregistered path elements are 404).
	target, ok := s.resolveTarget(r.Context(), repo)
	if !ok {
		httpError(w, http.StatusNotFound, "repository not found")
		return
	}
	repoPath, err := s.hydrate(r.Context(), target.key, service, target.repo.DefaultBranch)
	if err != nil {
		log.Printf("hydrate %s: %v", target.key, err)
		httpError(w, http.StatusInternalServerError, "hydration failed")
		return
	}
	w.Header().Set("Content-Type", "application/x-"+svcParam+"-advertisement")
	w.Header().Set("Cache-Control", "no-cache")
	if _, err := io.WriteString(w, pktLine("# service="+svcParam+"\n")+"0000"); err != nil {
		return
	}
	cmd := exec.CommandContext(r.Context(), "git", service, "--stateless-rpc", "--advertise-refs", repoPath)
	cmd.Stdout = w
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Printf("advertise %s %s: %v", service, repo, err)
	}
}

func (s *Server) handleServiceRPC(w http.ResponseWriter, r *http.Request, repo, service string) {
	claims, status, err := s.authenticate(r, repo, serviceScope(service))
	if err != nil {
		// RFC 9110 §11.6.1: challenge on 401 only (see handleInfoRefs).
		if status == http.StatusUnauthorized {
			w.Header().Set("WWW-Authenticate", `Basic realm="codestorage"`)
		}
		httpError(w, status, err.Error())
		return
	}
	// Fault-injection is only ever honored under CODESTORAGE_TEST_MODE=1;
	// otherwise the request is refused loudly rather than silently ignored.
	crash := r.Header.Get(CrashHeader)
	if crash != "" && !s.TestMode {
		httpError(w, http.StatusForbidden, CrashHeader+" refused: server not in test mode")
		return
	}
	// Auth before existence (see handleInfoRefs); target.key is the only
	// storage key from here on (hydrate + hook env).
	target, ok := s.resolveTarget(r.Context(), repo)
	if !ok {
		httpError(w, http.StatusNotFound, "repository not found")
		return
	}
	body := io.Reader(r.Body)
	// git clients gzip large request bodies.
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			httpError(w, http.StatusBadRequest, "bad gzip body: "+err.Error())
			return
		}
		defer gz.Close()
		body = gz
	}
	// Atomic-capability sniff (issue #28, atomic.go): the wire request is
	// the only place the server can learn a push is `--atomic` (githooks(5)
	// exposes no indicator to hooks), and pre-receive needs that bit to
	// deny mixed-policy atomic pushes before publication.
	atomicPush := false
	if service == "receive-pack" {
		atomicPush, body = sniffAtomicPush(body)
	}
	// Pack-quota pressure log (issue #21, quota.go). Enforcement lives
	// exclusively in pre-receive: only it sees the quarantined packs'
	// content-addressed keys, so only it can tell a quota-breaching push
	// from a zero-growth one (delete-only pushes, refs to already-stored
	// objects, and crash-window retries whose identical packs are already
	// published and/or registered — Codex rounds 3, 4, 5, 6). An HTTP-time
	// rejection of a pack-carrying request is unsound by construction: the
	// incoming pack's key is unknowable before the stream arrives, and a
	// wrong guess wedges the retry that would complete an interrupted
	// publication. Denials therefore happen in the hook (nothing is
	// stored; the client sees the sideband message); this cached, cheap
	// check gives operators the server-side signal that a repo is pushing
	// against its quota.
	if service == "receive-pack" && s.PackQuotaBytes > 0 {
		// Non-blocking peek (Codex rounds 7+9): the check is purely
		// observational — pre-receive enforces without any listing — so
		// neither a stalled blob backend nor a cold cache may delay or
		// fail the request. A miss triggers an async coalesced refresh
		// that serves later requests' peeks.
		if usage, ok := s.peekUsage(target.key); ok && usage >= s.PackQuotaBytes {
			log.Printf("quota: receive-pack for %s at/over quota (%d pack bytes stored >= %d): enforcement defers to pre-receive", target.key, usage, s.PackQuotaBytes)
		}
	}
	repoPath, err := s.hydrate(r.Context(), target.key, service, target.repo.DefaultBranch)
	if err != nil {
		log.Printf("hydrate %s: %v", target.key, err)
		httpError(w, http.StatusInternalServerError, "hydration failed")
		return
	}

	cmd := exec.CommandContext(r.Context(), "git", service, "--stateless-rpc", repoPath)
	cmd.Env = os.Environ()
	if service == "receive-pack" {
		// Never inherit EnvAtomic from the serve process environment
		// (Codex round 2): the pre-receive hook trusts it as THIS
		// request's atomic bit, so a stale operator-exported value would
		// make every mixed-policy push denial-happy — and appending
		// without stripping would not even override it, since duplicate
		// env entries resolve first-match in Go children. os.Environ()
		// returns a fresh copy, so the in-place filter aliases nothing.
		env := cmd.Env[:0]
		for _, kv := range cmd.Env {
			// EnvUsageFloor is stripped for the same reason: the hook
			// trusts it as THIS push's admission floor.
			if !strings.HasPrefix(kv, EnvAtomic+"=") && !strings.HasPrefix(kv, EnvUsageFloor+"=") {
				env = append(env, kv)
			}
		}
		cmd.Env = append(env,
			EnvRepo+"="+target.key,
			EnvOrg+"="+s.Org,
		)
		// Resolved quota for the pre-receive hook's authoritative
		// usage+incoming check (quota.go); absent means disabled.
		if s.PackQuotaBytes > 0 {
			// The hook is a fresh process per push and cannot cache
			// across pushes, so its admission floor is minted HERE from
			// the shared single-flight cache (Codex round 21: hook-side
			// self-listings scan the pack AND LFS prefixes per push —
			// ~100 pages at the LFS object cap — amplifying cheap pushes
			// into shared-backend listing load). Fail closed: spawning
			// without the env would just reopen the per-push listing
			// path via the hook's skew fallback.
			floor, err := s.CachedUsageFloor(r.Context(), target.key)
			if err != nil {
				log.Printf("quota: %s: usage floor for receive-pack: %v", target.key, err)
				httpError(w, http.StatusInternalServerError, "storage quota check failed")
				return
			}
			cmd.Env = append(cmd.Env, EnvPackQuota+"="+strconv.FormatInt(s.PackQuotaBytes, 10))
			cmd.Env = append(cmd.Env, EnvUsageFloor+"="+strconv.FormatInt(floor, 10))
			if s.PackReservationTTL > 0 {
				cmd.Env = append(cmd.Env, EnvPackReservationTTL+"="+s.PackReservationTTL.String())
			}
		}
		if atomicPush {
			cmd.Env = append(cmd.Env, EnvAtomic+"=1")
		}
		// The refs claim is passed through raw (docs/02 tuple encoding);
		// the update hook owns parsing and fails closed on malformed JSON.
		if len(claims.Refs) > 0 {
			cmd.Env = append(cmd.Env, EnvRefsPolicy+"="+string(claims.Refs))
		}
		// Deprecated repo-wide ops claim: folded into the ref policies by
		// the hook (docs/02 fallback semantics).
		if len(claims.Ops) > 0 {
			ops, err := json.Marshal(claims.Ops)
			assertf(err == nil, "marshal ops claim: %v", err)
			cmd.Env = append(cmd.Env, EnvOps+"="+string(ops))
		}
		if s.TestMode && crash != "" {
			cmd.Env = append(cmd.Env, EnvCrash+"="+crash)
		}
	}
	cmd.Stdin = body
	cmd.Stderr = os.Stderr

	w.Header().Set("Content-Type", "application/x-git-"+service+"-result")
	w.Header().Set("Cache-Control", "no-cache")
	cmd.Stdout = w
	if err := cmd.Run(); err != nil {
		// Headers are already sent; the pkt-line stream carries per-ref
		// errors to the client. Just log.
		log.Printf("%s %s: %v", service, repo, err)
	}
	if service == "receive-pack" && s.MaybeRepack != nil {
		// Unconditionally on completion, not only on success: pre-receive
		// may have registered packs even when a later per-ref update was
		// denied. Cancel-free context — the client hanging up right after
		// its push must not suppress the trigger.
		s.MaybeRepack(context.WithoutCancel(r.Context()), target.key)
	}
}
