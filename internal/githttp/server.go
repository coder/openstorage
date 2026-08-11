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
	"strings"

	"codestorage/internal/auth"
	"codestorage/internal/blob"
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
	// ResolveRepoURL (optional) maps the repo_id path element (docs/05 URL
	// style) to the repo's url value so tokens carrying the url claim
	// (docs/02) authenticate. ok=false when the repo_id is unknown. When
	// nil, only tokens whose claim equals the path element verbatim match.
	ResolveRepoURL func(ctx context.Context, repoID string) (url string, ok bool)
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

// Handler routes the two smart-HTTP endpoints per repo (docs/05):
//
//	GET  /{repo}.git/info/refs?service=git-{upload|receive}-pack
//	POST /{repo}.git/git-{upload|receive}-pack
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
	assertf(s.Verifier != nil && s.Org != "", "Server used without Verifier/Org")
	user, pass, ok := r.BasicAuth()
	if !ok || user != "t" {
		return nil, http.StatusUnauthorized, fmt.Errorf("basic auth with username 't' required")
	}
	claims, err := s.Verifier.Verify(r.Context(), pass)
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
	// address repos by repo_id. Accept a verbatim match, else resolve the
	// path element to its url and compare.
	target := BaseRepo(repo)
	if claims.Repo != target {
		matched := false
		if s.ResolveRepoURL != nil {
			if url, ok := s.ResolveRepoURL(r.Context(), target); ok && claims.Repo == url {
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
func (s *Server) hydrate(ctx context.Context, repo, service string) (string, error) {
	repoPath, refsErr, err := Hydrate(ctx, s.Store, s.Blob, s.CacheDir, repo)
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
		w.Header().Set("WWW-Authenticate", `Basic realm="codestorage"`)
		httpError(w, status, err.Error())
		return
	}
	repoPath, err := s.hydrate(r.Context(), repo, service)
	if err != nil {
		log.Printf("hydrate %s: %v", repo, err)
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
		w.Header().Set("WWW-Authenticate", `Basic realm="codestorage"`)
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
	repoPath, err := s.hydrate(r.Context(), repo, service)
	if err != nil {
		log.Printf("hydrate %s: %v", repo, err)
		httpError(w, http.StatusInternalServerError, "hydration failed")
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

	cmd := exec.CommandContext(r.Context(), "git", service, "--stateless-rpc", repoPath)
	cmd.Env = os.Environ()
	if service == "receive-pack" {
		cmd.Env = append(cmd.Env,
			EnvRepo+"="+repo,
			EnvOrg+"="+s.Org,
		)
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
}
