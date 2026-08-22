// Package githttp is the git smart HTTP surface (docs/05): stateless HTTP
// nodes with Postgres as ref authority, S3/MinIO as immutable pack storage,
// and local disk as a rebuildable cache. Authentication follows docs/02:
// customer-signed JWTs verified against the issuing org's registered keys.
package githttp

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
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
	// MaxPushBytes caps the DECODED bytes one receive-pack request may
	// stream into git (issue #55, pushcap.go); <= 0 disables the cap. Set
	// by the serve wiring (default: a small multiple of the pack quota).
	MaxPushBytes int64
	// VerifySigBudget caps how many commits one push may signature-verify
	// (issue #59, verifystate.go); <= 0 disables the budget. Set by the
	// serve wiring.
	VerifySigBudget int
	// usage memoizes per-repo pack usage for the quota-pressure log so
	// replayed cheap POSTs cannot amplify into per-request blob listings.
	usage usageCache
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
	// Decoded-byte ingress cap (issue #55, pushcap.go): bound the bytes a
	// push may stream into git BEFORE receive-pack quarantines them on
	// disk. Gzip pushes are bounded on BOTH sides of the decoder — the
	// compressed wire stream (wireCap; empty gzip members decode to
	// nothing, so the decoded counter alone cannot bound wire consumption)
	// and the decoded stream (pushCap, which a gzip bomb inflates past).
	// The decoded counter sits under the atomic sniffer below, so the teed
	// prefix is counted exactly once. upload-pack requests carry small
	// negotiation bodies and stay uncapped (out of #55's scope).
	capped := service == "receive-pack" && s.MaxPushBytes > 0
	gzipped := r.Header.Get("Content-Encoding") == "gzip"
	// A body that declares a size past its bound is rejected before
	// reading a single byte: the decoded cap for plain bodies, the
	// slack-padded wire limit for gzip ones (whose decoded size the
	// Content-Length does not bound).
	if capped {
		limit := s.MaxPushBytes
		if gzipped {
			limit = wirePushLimit(s.MaxPushBytes)
		}
		if r.ContentLength > limit {
			httpError(w, http.StatusRequestEntityTooLarge, pushTooLargeMsg(s.MaxPushBytes))
			return
		}
	}
	body := io.Reader(r.Body)
	var wireCap *cappedReader
	if gzipped {
		wire := io.Reader(r.Body)
		if capped {
			wireCap = newCappedReader(wire, wirePushLimit(s.MaxPushBytes))
			wire = wireCap
		}
		gz, err := gzip.NewReader(wire)
		if err != nil {
			httpError(w, http.StatusBadRequest, "bad gzip body: "+err.Error())
			return
		}
		defer gz.Close()
		body = gz
	}
	var pushCap *cappedReader
	if capped {
		pushCap = newCappedReader(body, s.MaxPushBytes)
		body = pushCap
	}
	// pushOverflowed reports whether either counter has tripped; the wire
	// counter surfaces through gzip reads, so both must be consulted.
	pushOverflowed := func() bool {
		return (pushCap != nil && pushCap.exceeded) || (wireCap != nil && wireCap.exceeded)
	}
	// Atomic-capability sniff (issue #28, atomic.go): the wire request is
	// the only place the server can learn a push is `--atomic` (githooks(5)
	// exposes no indicator to hooks), and pre-receive needs that bit to
	// deny mixed-policy atomic pushes before publication.
	atomicPush := false
	if service == "receive-pack" {
		atomicPush, body = sniffAtomicPush(body)
		// Overflow during the sniff (cap below the command-list prefix):
		// reject before hydrate or git ever run. The sniffer's non-atomic
		// verdict on the failed read is irrelevant — the push dies here.
		if pushOverflowed() {
			httpError(w, http.StatusRequestEntityTooLarge, pushTooLargeMsg(s.MaxPushBytes))
			return
		}
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
		// Never inherit per-push hook env from the serve process
		// environment (Codex round 2): the hooks trust these as THIS
		// request's state, so a stale operator-exported value would poison
		// every push (EnvAtomic: denial-happy mixed-policy handling;
		// EnvVerifyState: verdicts and budget shared ACROSS pushes;
		// EnvVerifySigBudget: an unvalidated value failing every
		// verify-sig push closed) — and appending without stripping would
		// not even override it, since duplicate env entries resolve
		// first-match in Go children. os.Environ() returns a fresh copy,
		// so the in-place filter aliases nothing.
		env := cmd.Env[:0]
		for _, kv := range cmd.Env {
			if !strings.HasPrefix(kv, EnvAtomic+"=") &&
				!strings.HasPrefix(kv, EnvVerifyState+"=") &&
				!strings.HasPrefix(kv, EnvVerifySigBudget+"=") {
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
			cmd.Env = append(cmd.Env, EnvPackQuota+"="+strconv.FormatInt(s.PackQuotaBytes, 10))
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
		// Per-push verify-sig verdict state (issue #59, verifystate.go): a
		// fresh empty file per receive-pack request, appended to by the
		// pre-receive gate and every update hook. Only the request scopes
		// "one push" — hooks are separate processes — so it is created and
		// removed here.
		stateFile, err := os.CreateTemp("", "codestorage-verify-sig-*.state")
		if err != nil {
			log.Printf("%s %s: verify state: %v", service, repo, err)
			httpError(w, http.StatusInternalServerError, "push setup failed")
			return
		}
		if err := stateFile.Close(); err != nil {
			log.Printf("%s %s: verify state: %v", service, repo, err)
			httpError(w, http.StatusInternalServerError, "push setup failed")
			return
		}
		defer os.Remove(stateFile.Name())
		// The hooks keep the cumulative walk counter and the range cache
		// in sibling files (verifystate.go); remove them with the state.
		defer os.Remove(stateFile.Name() + verifyWalkedCounterSuffix)
		defer os.Remove(stateFile.Name() + verifyRangesSuffix)
		cmd.Env = append(cmd.Env, EnvVerifyState+"="+stateFile.Name())
		if s.VerifySigBudget > 0 {
			cmd.Env = append(cmd.Env, EnvVerifySigBudget+"="+strconv.Itoa(s.VerifySigBudget))
		}
	}
	cmd.Stderr = os.Stderr

	w.Header().Set("Content-Type", "application/x-git-"+service+"-result")
	w.Header().Set("Cache-Control", "no-cache")
	if capped {
		// Capped receive path (issue #55): own the stdin copy so overflow
		// stays observable, and gate stdout so the response can still
		// become a 413 until the body has fully arrived under the cap
		// (pushcap.go).
		gate := newPushGate(w)
		cmd.Stdout = gate
		stdin, err := cmd.StdinPipe()
		if err != nil {
			log.Printf("%s %s: stdin pipe: %v", service, repo, err)
			httpError(w, http.StatusInternalServerError, "push setup failed")
			return
		}
		if err := cmd.Start(); err != nil {
			log.Printf("%s %s: start: %v", service, repo, err)
			httpError(w, http.StatusInternalServerError, "push setup failed")
			return
		}
		// exec's stdout goroutine drains git into the gate concurrently,
		// so this synchronous copy cannot deadlock. Overflow surfaces on
		// the cap counters; other copy errors (client hangup, gzip
		// corruption, git dying early) just end the copy — git sees EOF
		// and reports through the gate as usual.
		if _, err := io.Copy(stdin, body); err != nil && !errors.Is(err, errPushTooLarge) {
			log.Printf("%s %s: request body: %v", service, repo, err)
		}
		// The copy can also stop on the WRITE side before the counters
		// decide anything: receive-pack closes stdin once it has what it
		// needs (a delete-only push reads just the command list), so a
		// padded request would otherwise sneak past the cap undetected
		// (Codex round 11). Drain the capped reader — bounded by the cap
		// itself — so oversized bodies uniformly reach the 413 path.
		if _, err := io.Copy(io.Discard, body); err != nil && !errors.Is(err, errPushTooLarge) {
			log.Printf("%s %s: request body drain: %v", service, repo, err)
		}
		_ = stdin.Close()
		// Overflow is fully decided the moment the copy ends (the counters
		// only trip while feeding git), so settle the gate BEFORE waiting:
		// hook-phase output — sideband progress and keepalives during
		// pre-receive/update hooks that can legitimately run for minutes —
		// must stream live on the within-cap path, not buffer until exit
		// (Codex round 2). Overflow rejects regardless of git's exit
		// status, which CANNOT discriminate anything here: receive-pack
		// reports a truncated-pack unpack failure as a normal
		// report-status response and exits 0.
		overflow := pushOverflowed()
		if err := gate.settle(overflow); err != nil {
			log.Printf("%s %s: response: %v", service, repo, err)
		}
		runErr := cmd.Wait()
		if overflow {
			// On the normal overflow path git saw the truncated stream
			// during unpack — before any hook could run — and discarded
			// its own quarantine: nothing was published or registered, so
			// the repack trigger below has nothing to see and the 413 can
			// short-circuit it. The one exception is a client that padded
			// a complete push with trailing junk past the cap; its hooks
			// may have run, which is loud-logged and still rejected (only
			// a non-git client can produce it, and its effects are
			// quota-admitted like any other push).
			if runErr == nil {
				log.Printf("%s %s: receive-pack completed but the request body continued past the %d-byte cap (padded push?)", service, repo, s.MaxPushBytes)
				// A completed receive-pack means its hooks ran and may
				// have registered packs (the push was whole before the
				// padding): the repack threshold trigger must still fire
				// even though the response is a 413 (Codex round 4).
				if s.MaybeRepack != nil {
					s.MaybeRepack(context.WithoutCancel(r.Context()), target.key)
				}
			}
			if gate.responseStarted() {
				// Response bytes already streamed (gateBufferMax safety
				// valve): the status is committed, only log.
				log.Printf("%s %s: push exceeded the %d-byte cap after the response started", service, repo, s.MaxPushBytes)
				return
			}
			log.Printf("%s %s: push exceeded the %d-byte decoded cap, rejected with 413", service, repo, s.MaxPushBytes)
			httpError(w, http.StatusRequestEntityTooLarge, pushTooLargeMsg(s.MaxPushBytes))
			return
		}
		if runErr != nil {
			log.Printf("%s %s: %v", service, repo, runErr)
		}
	} else {
		cmd.Stdin = body
		cmd.Stdout = w
		if err := cmd.Run(); err != nil {
			// Headers are already sent; the pkt-line stream carries per-ref
			// errors to the client. Just log.
			log.Printf("%s %s: %v", service, repo, err)
		}
	}
	if service == "receive-pack" && s.MaybeRepack != nil {
		// Unconditionally on completion, not only on success: pre-receive
		// may have registered packs even when a later per-ref update was
		// denied. Cancel-free context — the client hanging up right after
		// its push must not suppress the trigger.
		s.MaybeRepack(context.WithoutCancel(r.Context()), target.key)
	}
}
