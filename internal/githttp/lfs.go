// Git LFS Batch API (docs/explanation/git-protocol-and-namespaces.md): the storage host serves the standard LFS
// discovery path POST /{repo}.git/info/lfs/objects/batch on the same
// JWT-authenticated remote — no separate LFS server or config. Object bytes
// live in the BlobStore under blob.LFSKey(repoID, oid); transfers use the
// "basic" adapter with presigned S3 URLs when the backend supports them,
// falling back to proxying through this server (fileblob local mode) via
// GET/PUT /{repo}.git/info/lfs/objects/{oid}.
package githttp

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/blob"
)

const (
	lfsMediaType = "application/vnd.git-lfs+json"
	// lfsURLExpiry bounds presigned transfer URLs; also reported to the
	// client as expires_in.
	lfsURLExpiry = 15 * time.Minute
	// lfsMaxBatchBody bounds the JSON batch request (defensive: a batch of
	// 100 objects is ~10KB; 4MiB is far beyond any legitimate request).
	lfsMaxBatchBody = 4 << 20
)

// lfsOIDPattern is the sha256 content address git-lfs uses: exactly 64
// lowercase hex digits. Anything else is rejected before it can reach the
// blob store as a key component.
var lfsOIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type lfsBatchRequest struct {
	Operation string          `json:"operation"`
	Transfers []string        `json:"transfers"`
	HashAlgo  string          `json:"hash_algo"`
	Objects   []lfsObjectSpec `json:"objects"`
}

type lfsObjectSpec struct {
	OID  string `json:"oid"`
	Size int64  `json:"size"`
}

type lfsAction struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresIn int               `json:"expires_in,omitempty"`
}

type lfsObjectError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type lfsObjectResult struct {
	OID           string                `json:"oid"`
	Size          int64                 `json:"size"`
	Authenticated bool                  `json:"authenticated,omitempty"`
	Actions       map[string]*lfsAction `json:"actions,omitempty"`
	Error         *lfsObjectError       `json:"error,omitempty"`
}

type lfsBatchResponse struct {
	Transfer string            `json:"transfer"`
	Objects  []lfsObjectResult `json:"objects"`
	HashAlgo string            `json:"hash_algo"`
}

// lfsError writes an LFS-spec error body ({"message": ...}) with the LFS
// media type, so git-lfs surfaces the message to the user.
func lfsError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", lfsMediaType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": msg})
}

// lfsToken extracts the JWT from either Basic auth (username "t" — what
// git-lfs sends when the remote URL carries credentials, same as the smart
// HTTP endpoints) or an `Authorization: Bearer <jwt>` header (the LFS API's
// native form).
func lfsToken(r *http.Request) (string, bool) {
	if user, pass, ok := r.BasicAuth(); ok && user == "t" {
		return pass, true
	}
	if tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok && tok != "" {
		return tok, true
	}
	return "", false
}

// authenticateLFS enforces the docs/reference/authentication.md failure mapping on the LFS path,
// accepting both credential forms (see lfsToken).
func (s *Server) authenticateLFS(r *http.Request, repo, scope string) (*auth.Claims, int, error) {
	token, ok := lfsToken(r)
	if !ok {
		return nil, http.StatusUnauthorized, fmt.Errorf(`basic auth with username 't' or a bearer token required`)
	}
	return s.verifyClaims(r.Context(), token, repo, scope)
}

// handleLFS routes everything under /{repo}.git/info/lfs/. rest is the path
// after "info/lfs/". Unknown subpaths (e.g. the optional locking API's
// locks/verify probe) answer 404, which git-lfs treats as "not supported".
func (s *Server) handleLFS(w http.ResponseWriter, r *http.Request, repo, rest string) {
	switch {
	case r.Method == http.MethodPost && rest == "objects/batch":
		s.handleLFSBatch(w, r, repo)
	case (r.Method == http.MethodGet || r.Method == http.MethodPut) && strings.HasPrefix(rest, "objects/"):
		s.handleLFSObject(w, r, repo, strings.TrimPrefix(rest, "objects/"))
	default:
		lfsError(w, http.StatusNotFound, "unsupported LFS endpoint")
	}
}

// handleLFSBatch implements the LFS Batch API. The operation in the body
// selects the required scope (download → git:read, upload → git:write), so
// the body is parsed (size-bounded) before authentication.
func (s *Server) handleLFSBatch(w http.ResponseWriter, r *http.Request, repo string) {
	body := io.Reader(http.MaxBytesReader(w, r.Body, lfsMaxBatchBody))
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(body)
		if err != nil {
			lfsError(w, http.StatusBadRequest, "bad gzip body: "+err.Error())
			return
		}
		defer gz.Close()
		body = gz
	}
	var req lfsBatchRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		lfsError(w, http.StatusBadRequest, "bad batch request: "+err.Error())
		return
	}
	var scope string
	switch req.Operation {
	case "download":
		scope = auth.ScopeGitRead
	case "upload":
		scope = auth.ScopeGitWrite
	default:
		lfsError(w, http.StatusBadRequest, `operation must be "download" or "upload"`)
		return
	}
	if _, status, err := s.authenticateLFS(r, repo, scope); err != nil {
		if status == http.StatusUnauthorized {
			// LFS-spec discovery header alongside the standard challenge.
			w.Header().Set("LFS-Authenticate", `Basic realm="codestorage"`)
			w.Header().Set("WWW-Authenticate", `Basic realm="codestorage"`)
		}
		lfsError(w, status, err.Error())
		return
	}
	// Auth before existence (same ordering as the smart-HTTP handlers).
	target, ok := s.resolveTarget(r.Context(), repo)
	if !ok {
		lfsError(w, http.StatusNotFound, "repository not found")
		return
	}
	if len(req.Transfers) > 0 && !slices.Contains(req.Transfers, "basic") {
		lfsError(w, http.StatusUnprocessableEntity, `only the "basic" transfer adapter is supported`)
		return
	}
	if req.HashAlgo != "" && req.HashAlgo != "sha256" {
		// 409 per the batch spec's hash_algo negotiation.
		lfsError(w, http.StatusConflict, `only hash_algo "sha256" is supported`)
		return
	}
	resp := lfsBatchResponse{Transfer: "basic", HashAlgo: "sha256", Objects: make([]lfsObjectResult, 0, len(req.Objects))}
	for _, obj := range req.Objects {
		resp.Objects = append(resp.Objects, s.lfsBatchObject(r, repo, target, req.Operation, obj))
	}
	w.Header().Set("Content-Type", lfsMediaType)
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(resp)
}

// lfsBatchObject resolves one batch entry to actions or a per-object error.
// LFS keys are scoped to the base RepoID (blob.LFSKey): the `+ephemeral` /
// `+import` namespaces share the repo's LFS object space, since pointers in
// any namespace address the same content.
func (s *Server) lfsBatchObject(r *http.Request, repoElem string, target repoTarget, op string, obj lfsObjectSpec) lfsObjectResult {
	res := lfsObjectResult{OID: obj.OID, Size: obj.Size}
	fail := func(code int, msg string) lfsObjectResult {
		res.Actions = nil
		res.Error = &lfsObjectError{Code: code, Message: msg}
		return res
	}
	if !lfsOIDPattern.MatchString(obj.OID) {
		return fail(http.StatusUnprocessableEntity, "oid must be 64 lowercase hex digits (sha256)")
	}
	if obj.Size < 0 {
		return fail(http.StatusUnprocessableEntity, "size must be non-negative")
	}
	key := blob.LFSKey(target.repo.RepoID, obj.OID)
	size, err := s.Blob.Stat(r.Context(), key)
	missing := errors.Is(err, blob.ErrNotFound)
	if err != nil && !missing {
		log.Printf("lfs stat %s: %v", key, err)
		return fail(http.StatusInternalServerError, "object storage unavailable")
	}
	switch op {
	case "download":
		if missing {
			return fail(http.StatusNotFound, "object not found")
		}
		res.Size = size // authoritative stored size
		action, err := s.lfsAction(r, repoElem, http.MethodGet, key, obj.OID)
		if err != nil {
			log.Printf("lfs sign %s: %v", key, err)
			return fail(http.StatusInternalServerError, "signing transfer URL failed")
		}
		res.Authenticated = true
		res.Actions = map[string]*lfsAction{"download": action}
	case "upload":
		if !missing {
			// Already stored: no actions means the client skips the upload.
			return res
		}
		action, err := s.lfsAction(r, repoElem, http.MethodPut, key, obj.OID)
		if err != nil {
			log.Printf("lfs sign %s: %v", key, err)
			return fail(http.StatusInternalServerError, "signing transfer URL failed")
		}
		res.Authenticated = true
		res.Actions = map[string]*lfsAction{"upload": action}
	default:
		panic("githttp assertion failed: unknown LFS operation " + op)
	}
	return res
}

// lfsAction builds the basic-transfer action for key: a presigned blob URL
// when the backend supports it, else a proxy href back through this server
// (docs/explanation/git-protocol-and-namespaces.md local mode). The proxy href carries the caller's Authorization
// header so git-lfs re-presents the same credentials on the transfer.
func (s *Server) lfsAction(r *http.Request, repoElem, method, key, oid string) (*lfsAction, error) {
	signed, err := s.Blob.SignedURL(r.Context(), key, method, lfsURLExpiry)
	if err == nil {
		return &lfsAction{Href: signed, ExpiresIn: int(lfsURLExpiry.Seconds())}, nil
	}
	if !errors.Is(err, blob.ErrSignedURLUnsupported) {
		return nil, err
	}
	action := &lfsAction{Href: requestBaseURL(r) + "/" + repoElem + ".git/info/lfs/objects/" + oid}
	if a := r.Header.Get("Authorization"); a != "" {
		action.Header = map[string]string{"Authorization": a}
	}
	return action, nil
}

// requestBaseURL reconstructs the externally visible scheme://host of the
// request for proxy transfer hrefs.
func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = p
	}
	assertf(r.Host != "", "requestBaseURL: request without Host")
	return scheme + "://" + r.Host
}

// handleLFSObject serves the proxy transfer endpoints used when the blob
// backend cannot presign (fileblob local mode): GET streams the object, PUT
// stores it after verifying the sha256 content address.
func (s *Server) handleLFSObject(w http.ResponseWriter, r *http.Request, repo, oid string) {
	if !lfsOIDPattern.MatchString(oid) {
		lfsError(w, http.StatusNotFound, "invalid oid")
		return
	}
	scope := auth.ScopeGitRead
	if r.Method == http.MethodPut {
		scope = auth.ScopeGitWrite
	}
	if _, status, err := s.authenticateLFS(r, repo, scope); err != nil {
		if status == http.StatusUnauthorized {
			w.Header().Set("WWW-Authenticate", `Basic realm="codestorage"`)
		}
		lfsError(w, status, err.Error())
		return
	}
	target, ok := s.resolveTarget(r.Context(), repo)
	if !ok {
		lfsError(w, http.StatusNotFound, "repository not found")
		return
	}
	key := blob.LFSKey(target.repo.RepoID, oid)
	if r.Method == http.MethodGet {
		s.lfsProxyDownload(w, r, key)
		return
	}
	s.lfsProxyUpload(w, r, key, oid)
}

func (s *Server) lfsProxyDownload(w http.ResponseWriter, r *http.Request, key string) {
	size, err := s.Blob.Stat(r.Context(), key)
	if errors.Is(err, blob.ErrNotFound) {
		lfsError(w, http.StatusNotFound, "object not found")
		return
	}
	if err != nil {
		log.Printf("lfs stat %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object storage unavailable")
		return
	}
	// BlobStore only exposes whole-file transfers (atomic tmp+rename), so
	// stage the object in a scratch dir and stream it out.
	dir, err := os.MkdirTemp("", "lfs-download-*")
	if err != nil {
		lfsError(w, http.StatusInternalServerError, "scratch dir failed")
		return
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "obj")
	if err := s.Blob.DownloadFile(r.Context(), key, path); err != nil {
		log.Printf("lfs download %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object download failed")
		return
	}
	f, err := os.Open(path)
	if err != nil {
		lfsError(w, http.StatusInternalServerError, "object open failed")
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	if _, err := io.Copy(w, f); err != nil {
		log.Printf("lfs stream %s: %v", key, err) // headers already sent
	}
}

func (s *Server) lfsProxyUpload(w http.ResponseWriter, r *http.Request, key, oid string) {
	dir, err := os.MkdirTemp("", "lfs-upload-*")
	if err != nil {
		lfsError(w, http.StatusInternalServerError, "scratch dir failed")
		return
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "obj")
	f, err := os.Create(path)
	if err != nil {
		lfsError(w, http.StatusInternalServerError, "scratch file failed")
		return
	}
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, h), r.Body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		lfsError(w, http.StatusBadRequest, "reading upload body failed: "+err.Error())
		return
	}
	// The oid IS the content address: publishing bytes under a key whose
	// hash doesn't match would corrupt every future download of that oid.
	if got := hex.EncodeToString(h.Sum(nil)); got != oid {
		lfsError(w, http.StatusBadRequest, fmt.Sprintf("content sha256 %s does not match oid %s", got, oid))
		return
	}
	if err := s.Blob.UploadFile(r.Context(), key, path); err != nil {
		log.Printf("lfs upload %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object upload failed")
		return
	}
	w.WriteHeader(http.StatusOK)
}
