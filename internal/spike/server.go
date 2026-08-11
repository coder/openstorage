package spike

import (
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// Server is one stateless HTTP node: all durable truth lives in PG (refs,
// pack registry) and S3 (pack bytes); CacheDir is rebuildable at any time.
type Server struct {
	Store    ref.RefStore
	Blob     blob.BlobStore
	CacheDir string
	PubKey   *ecdsa.PublicKey
	TestMode bool
}

func NewServer(st ref.RefStore, bl blob.BlobStore, cacheDir string, pub *ecdsa.PublicKey) *Server {
	assertf(st != nil && bl != nil && pub != nil && cacheDir != "", "NewServer: missing dependency")
	return &Server{
		Store:    st,
		Blob:     bl,
		CacheDir: cacheDir,
		PubKey:   pub,
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

// authenticate enforces docs/02 failure mapping: 401 for missing/invalid
// token, 403 for scope or repo mismatch.
func (s *Server) authenticate(r *http.Request, repo, scope string) (*Claims, int, error) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != "t" {
		return nil, http.StatusUnauthorized, fmt.Errorf("basic auth with username 't' required")
	}
	claims, err := VerifyToken(s.PubKey, pass)
	if err != nil {
		return nil, http.StatusUnauthorized, err
	}
	if claims.Repo != BaseRepo(repo) {
		return nil, http.StatusForbidden, fmt.Errorf("jwt repo claim mismatch")
	}
	if !claims.HasScope(scope) {
		return nil, http.StatusForbidden, fmt.Errorf("missing scope %s", scope)
	}
	return claims, 0, nil
}

func serviceScope(service string) string {
	switch service {
	case "upload-pack":
		return ScopeGitRead
	case "receive-pack":
		return ScopeGitWrite
	}
	panic("spike assertion failed: unknown service " + service)
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
		httpError(w, status, err.Error())
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
			httpError(w, http.StatusBadRequest, "bad gzip body")
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
			EnvRefsPolicy+"="+claims.RefsPolicyJSON(),
		)
		// Crash knob is honored only under SPIKE_TEST_MODE=1.
		if s.TestMode {
			if crash := r.Header.Get("X-Spike-Crash"); crash != "" {
				cmd.Env = append(cmd.Env, EnvCrash+"="+crash)
			}
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
