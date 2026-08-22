// Git LFS Batch API (docs/05): the storage host serves the standard LFS
// discovery path POST /{repo}.git/info/lfs/objects/batch on the same
// JWT-authenticated remote — no separate LFS server or config. Object bytes
// live in the BlobStore under blob.LFSKey(repoID, oid); transfers use the
// "basic" adapter.
//
// DOWNLOADS use presigned S3 URLs when the backend supports them, falling
// back to proxying through this server (fileblob local mode) via
// GET /{repo}.git/info/lfs/objects/{oid}. Reads are never quota-gated.
//
// UPLOADS always go through the verified proxy transfer
// (PUT /{repo}.git/info/lfs/objects/{oid}, issue #51): a presigned PUT
// binds neither content length nor digest with the current signer
// (gocloud.dev SignedURLOptions cannot sign Content-Length), so only the
// server-verified path can anchor the storage-quota ledger and the sha256
// content address. Upload admission is a durable reservation
// (ref.ReservePackBytes) charged at batch time on the DECLARED size and
// re-charged at PUT time on the request's ACTUAL Content-Length (the
// declared-size-lie gate); a verified upload settles the reservation into
// the usage counter via ref.RegisterLFSObject, which is also the registry
// the LFS orphan sweep classifies against.
package githttp

import (
	"compress/gzip"
	"context"
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

	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

const (
	lfsMediaType = "application/vnd.git-lfs+json"
	// lfsURLExpiry bounds presigned transfer URLs; also reported to the
	// client as expires_in.
	lfsURLExpiry = 15 * time.Minute
	// lfsMaxBatchBody bounds the JSON batch request (defensive: a batch of
	// 100 objects is ~10KB; 4MiB is far beyond any legitimate request).
	lfsMaxBatchBody = 4 << 20
	// lfsMaxBatchObjects bounds one batch's object count (defensive:
	// git-lfs sends at most 100 per batch; each entry costs a blob Stat
	// and, on upload, a ledger reservation row, so the count must not be
	// attacker-sized up to the body limit's ~40k entries).
	lfsMaxBatchObjects = 1000
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

// authenticateLFS enforces the docs/02 failure mapping on the LFS path,
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
	if len(req.Objects) > lfsMaxBatchObjects {
		lfsError(w, http.StatusUnprocessableEntity,
			fmt.Sprintf("too many objects in one batch (%d, max %d)", len(req.Objects), lfsMaxBatchObjects))
		return
	}
	resp := lfsBatchResponse{Transfer: "basic", HashAlgo: "sha256"}
	if req.Operation == "upload" {
		resp.Objects = s.lfsBatchUpload(r, repo, target, req.Objects)
	} else {
		resp.Objects = make([]lfsObjectResult, 0, len(req.Objects))
		for _, obj := range req.Objects {
			resp.Objects = append(resp.Objects, s.lfsBatchDownload(r, repo, target, obj))
		}
	}
	w.Header().Set("Content-Type", lfsMediaType)
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(resp)
}

// lfsObjectFail marks one batch entry failed with a per-object error.
func lfsObjectFail(res *lfsObjectResult, code int, msg string) {
	res.Authenticated = false
	res.Actions = nil
	res.Error = &lfsObjectError{Code: code, Message: msg}
}

// lfsValidateSpec applies the shared per-object validation and key
// resolution. LFS keys are scoped to the base RepoID (blob.LFSKey): the
// `+ephemeral` / `+import` namespaces share the repo's LFS object space,
// since pointers in any namespace address the same content.
func lfsValidateSpec(res *lfsObjectResult, target repoTarget, obj lfsObjectSpec) (key string, ok bool) {
	if !lfsOIDPattern.MatchString(obj.OID) {
		lfsObjectFail(res, http.StatusUnprocessableEntity, "oid must be 64 lowercase hex digits (sha256)")
		return "", false
	}
	if obj.Size < 0 {
		lfsObjectFail(res, http.StatusUnprocessableEntity, "size must be non-negative")
		return "", false
	}
	return blob.LFSKey(target.repo.RepoID, obj.OID), true
}

// lfsBatchDownload resolves one download batch entry to an action or a
// per-object error. Downloads are never quota-gated (issue #51 scopes the
// perimeter to writers).
func (s *Server) lfsBatchDownload(r *http.Request, repoElem string, target repoTarget, obj lfsObjectSpec) lfsObjectResult {
	res := lfsObjectResult{OID: obj.OID, Size: obj.Size}
	key, ok := lfsValidateSpec(&res, target, obj)
	if !ok {
		return res
	}
	size, err := s.Blob.Stat(r.Context(), key)
	if errors.Is(err, blob.ErrNotFound) {
		lfsObjectFail(&res, http.StatusNotFound, "object not found")
		return res
	}
	if err != nil {
		log.Printf("lfs stat %s: %v", key, err)
		lfsObjectFail(&res, http.StatusInternalServerError, "object storage unavailable")
		return res
	}
	res.Size = size // authoritative stored size
	action, err := s.lfsDownloadAction(r, repoElem, key, obj.OID)
	if err != nil {
		log.Printf("lfs sign %s: %v", key, err)
		lfsObjectFail(&res, http.StatusInternalServerError, "signing transfer URL failed")
		return res
	}
	res.Authenticated = true
	res.Actions = map[string]*lfsAction{"download": action}
	return res
}

// lfsBatchUpload resolves a whole upload batch: it validates and Stats each
// entry, adopts already-stored objects into the LFS registry, admits every
// missing object's DECLARED size through the durable quota ledger in ONE
// reservation call (issue #51 — one admission snapshot per batch, listed
// floor included, so a batch cannot admit piecemeal against itself), and
// issues verified-proxy upload actions only for admitted objects. An
// over-quota batch answers per-object 507 errors and issues no transfer
// hrefs at all.
func (s *Server) lfsBatchUpload(r *http.Request, repoElem string, target repoTarget, objs []lfsObjectSpec) []lfsObjectResult {
	ctx := r.Context()
	repoID := target.repo.RepoID
	results := make([]lfsObjectResult, len(objs))
	// Deduplicate by key before charging: identical oids are identical
	// bytes, and the ledger admits each incoming key once (a duplicate
	// entry in one batch shares its first occurrence's outcome). The max
	// declared size wins so a duplicate cannot shrink the charge.
	needIdx := make(map[string][]int)
	needSize := make(map[string]int64)
	var order []string
	for i, obj := range objs {
		results[i] = lfsObjectResult{OID: obj.OID, Size: obj.Size}
		key, ok := lfsValidateSpec(&results[i], target, obj)
		if !ok {
			continue
		}
		size, err := s.Blob.Stat(ctx, key)
		switch {
		case err == nil:
			// Already stored: no actions means the client skips the
			// upload — but ONLY once the object is durably registered
			// (pre-#51 uploads have no rows; without adoption the orphan
			// sweep would classify referenced objects as reapable, so a
			// swallowed adoption failure here would let the client commit
			// a pointer to a reapable blob).
			if aerr := s.adoptLFSObject(ctx, repoID, key, size); aerr != nil {
				log.Printf("lfs adopt %s: %v", key, aerr)
				lfsObjectFail(&results[i], http.StatusInternalServerError, "object registration failed, retry")
			}
			continue
		case !errors.Is(err, blob.ErrNotFound):
			log.Printf("lfs stat %s: %v", key, err)
			lfsObjectFail(&results[i], http.StatusInternalServerError, "object storage unavailable")
			continue
		}
		if _, seen := needIdx[key]; !seen {
			order = append(order, key)
		}
		needIdx[key] = append(needIdx[key], i)
		needSize[key] = max(needSize[key], obj.Size)
	}
	// Admission: reserve the declared sizes before ANY transfer href is
	// issued. Zero-size entries charge nothing (the empty object is
	// zero growth) but still get actions.
	if s.PackQuotaBytes > 0 {
		var packs []ref.Pack
		var incoming int64
		for _, key := range order {
			if sz := needSize[key]; sz > 0 {
				packs = append(packs, ref.Pack{Key: key, Size: sz})
				incoming += sz
			}
		}
		if len(packs) > 0 {
			denyAll := func(code int, msg string) []lfsObjectResult {
				for _, key := range order {
					for _, i := range needIdx[key] {
						lfsObjectFail(&results[i], code, msg)
					}
				}
				return results
			}
			floor, err := blob.StorageUsageBytes(ctx, s.Blob, repoID)
			if err != nil {
				log.Printf("lfs quota: %s: usage listing failed: %v", repoID, err)
				return denyAll(http.StatusInternalServerError, "storage quota check failed")
			}
			dec, err := s.Store.ReservePackBytes(ctx, blob.QuotaRepoGroup(repoID), packs,
				ref.NewReservationOwner(), floor, s.PackQuotaBytes, s.lfsReservationTTL())
			if err != nil {
				log.Printf("lfs quota: %s: reserve failed: %v", repoID, err)
				return denyAll(http.StatusInternalServerError, "storage quota check failed")
			}
			if !dec.OK {
				return denyAll(http.StatusInsufficientStorage, fmt.Sprintf(
					"repository storage quota exceeded: upload adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight uploads), quota is %d bytes",
					incoming, dec.UsedBytes, dec.ReservedBytes, s.PackQuotaBytes))
			}
		}
	}
	for _, key := range order {
		for _, i := range needIdx[key] {
			results[i].Authenticated = true
			results[i].Actions = map[string]*lfsAction{"upload": s.lfsUploadAction(r, repoElem, results[i].OID)}
		}
	}
	return results
}

// lfsReservationTTL resolves the TTL LFS admission charges: the serve
// wiring's PackReservationTTL, or the shared quota.go default.
func (s *Server) lfsReservationTTL() time.Duration {
	if s.PackReservationTTL > 0 {
		return s.PackReservationTTL
	}
	return defaultReservationTTL
}

// adoptLFSObject backfills the lfs_objects registry row for an object that
// is already stored but was never registered (uploads predating issue #51,
// or a verified upload whose registration failed after publish). Callers
// MUST propagate a failure instead of reporting the object stored (Codex
// round 1): telling the client "stored" lets it commit its pointer, and
// with DeleteLFS enabled a still-rowless object eventually classifies as an
// orphan and is reaped — a transient ref-store outage must fail the request
// so the client retries, never silently downgrade to data loss.
func (s *Server) adoptLFSObject(ctx context.Context, repoID, key string, size int64) error {
	registered, err := s.Store.HasLFSObject(ctx, repoID, key)
	if err != nil || registered {
		return err
	}
	if err := s.Store.RegisterLFSObject(ctx, repoID, key, size); err != nil {
		return err
	}
	// Post-registration existence recheck (Codex round 1) — the adoption
	// analogue of blob.EnsurePublished's writer-verify half: a DeleteLFS
	// sweep can reap the still-rowless blob between the caller's Stat and
	// the registration commit, with its post-delete registry recheck also
	// missing the row. Adoption holds no bytes to re-upload, so it fails
	// instead: the client retries, the next batch sees the key missing
	// and issues a real upload action, and the idempotent re-registration
	// credits nothing twice (meanwhile the next sweep's heal pass would
	// restore the now-registered blob from quarantine anyway). Ordering
	// proof mirrors EnsurePublished's: a reap whose delete lands after
	// this Stat necessarily runs its registry recheck after our committed
	// row and restores the blob itself.
	if _, err := s.Blob.Stat(ctx, key); err != nil {
		return fmt.Errorf("adopted object vanished (racing sweep?): %w", err)
	}
	return nil
}

// lfsDownloadAction builds the basic-transfer download action for key: a
// presigned blob URL when the backend supports it, else a proxy href back
// through this server (docs/05 local mode). The proxy href carries the
// caller's Authorization header so git-lfs re-presents the same
// credentials on the transfer.
func (s *Server) lfsDownloadAction(r *http.Request, repoElem, key, oid string) (*lfsAction, error) {
	signed, err := s.Blob.SignedURL(r.Context(), key, http.MethodGet, lfsURLExpiry)
	if err == nil {
		return &lfsAction{Href: signed, ExpiresIn: int(lfsURLExpiry.Seconds())}, nil
	}
	if !errors.Is(err, blob.ErrSignedURLUnsupported) {
		return nil, err
	}
	return s.lfsProxyAction(r, repoElem, oid), nil
}

// lfsUploadAction builds the basic-transfer upload action: ALWAYS the
// verified proxy PUT (issue #51) — see the package comment for why
// presigned uploads are not issued.
func (s *Server) lfsUploadAction(r *http.Request, repoElem, oid string) *lfsAction {
	return s.lfsProxyAction(r, repoElem, oid)
}

// lfsProxyAction builds a proxy transfer href through this server: on the
// configured external base URL when the serve wiring set one (production
// reverse proxies may present an internal Host and omit X-Forwarded-Proto
// — Codex round 2), else reconstructed from the request.
func (s *Server) lfsProxyAction(r *http.Request, repoElem, oid string) *lfsAction {
	base := strings.TrimRight(s.BaseURL, "/")
	if base == "" {
		base = requestBaseURL(r)
	}
	action := &lfsAction{Href: base + "/" + repoElem + ".git/info/lfs/objects/" + oid}
	if a := r.Header.Get("Authorization"); a != "" {
		action.Header = map[string]string{"Authorization": a}
	}
	return action
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
	s.lfsProxyUpload(w, r, target, key, oid)
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

// lfsProxyUpload is the verified upload transfer and the quota ledger's
// settle point (issue #51): it re-admits the request's ACTUAL
// Content-Length (closing the batch-time declared-size lie), verifies the
// sha256 content address, publishes, and registers the object — consuming
// the reservation and crediting the usage counter atomically.
func (s *Server) lfsProxyUpload(w http.ResponseWriter, r *http.Request, target repoTarget, key, oid string) {
	ctx := r.Context()
	repoID := target.repo.RepoID
	// Dedup before charging (the zero-growth rule): a re-PUT of a stored
	// object (crash-window retry, concurrent identical upload) adds
	// nothing — adopt it into the registry and succeed without reading
	// the body (the key is content-addressed, so stored bytes match). A
	// failed adoption fails the PUT: 200 would let the client commit a
	// pointer to a blob the sweeper still sees as an orphan.
	switch size, err := s.Blob.Stat(ctx, key); {
	case err == nil:
		if aerr := s.adoptLFSObject(ctx, repoID, key, size); aerr != nil {
			log.Printf("lfs adopt %s: %v", key, aerr)
			lfsError(w, http.StatusInternalServerError, "object registration failed")
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	case !errors.Is(err, blob.ErrNotFound):
		log.Printf("lfs stat %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object storage unavailable")
		return
	}
	// The transfer must declare its size up front: the Go HTTP server
	// caps a Content-Length body at the declared length, making it a hard
	// bound the admission below can trust. Chunked bodies have no bound.
	if r.ContentLength < 0 {
		lfsError(w, http.StatusLengthRequired, "upload requires Content-Length")
		return
	}
	// One in-flight upload per key (Codex security round on #51): staging
	// happens on local disk before verification, and while DISTINCT keys'
	// aggregate staging is bounded by the quota (each reservation counts
	// the others' rows), a SAME-key reservation deliberately refreshes
	// rather than sums — so N concurrent identical PUTs would each admit
	// and stage N quota-sized temp files. Identical keys are identical
	// bytes: let one win, turn the rest away before any disk is touched,
	// and their retry lands on the stored-object adoption path above.
	if _, busy := s.lfsUploadsInFlight.LoadOrStore(key, struct{}{}); busy {
		w.Header().Set("Retry-After", "10")
		lfsError(w, http.StatusTooManyRequests, "an upload for this object is already in progress")
		return
	}
	defer s.lfsUploadsInFlight.Delete(key)
	// PUT-time admission: the batch charged the DECLARED size; re-reserve
	// what actually arrives (a same-key reservation refreshes, so honest
	// clients are counted once and a liar's oversized body re-runs the
	// full admission check).
	var reserveOwner string
	release := func() {
		if reserveOwner != "" {
			// Best-effort: an unreleased row only gates admission until
			// the TTL expires (owner-scoped, so it can never free a
			// reservation a concurrent identical upload re-acquired).
			_ = s.Store.ReleasePackReservations(ctx, []string{key}, reserveOwner)
		}
	}
	if s.PackQuotaBytes > 0 && r.ContentLength > 0 {
		floor, err := blob.StorageUsageBytes(ctx, s.Blob, repoID)
		if err != nil {
			log.Printf("lfs quota: %s: usage listing failed: %v", repoID, err)
			lfsError(w, http.StatusInternalServerError, "storage quota check failed")
			return
		}
		reserveOwner = ref.NewReservationOwner()
		dec, err := s.Store.ReservePackBytes(ctx, blob.QuotaRepoGroup(repoID),
			[]ref.Pack{{Key: key, Size: r.ContentLength}}, reserveOwner,
			floor, s.PackQuotaBytes, s.lfsReservationTTL())
		if err != nil {
			log.Printf("lfs quota: %s: reserve failed: %v", repoID, err)
			lfsError(w, http.StatusInternalServerError, "storage quota check failed")
			return
		}
		if !dec.OK {
			lfsError(w, http.StatusInsufficientStorage, fmt.Sprintf(
				"repository storage quota exceeded: upload adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight uploads), quota is %d bytes",
				r.ContentLength, dec.UsedBytes, dec.ReservedBytes, s.PackQuotaBytes))
			return
		}
	}
	dir, err := os.MkdirTemp("", "lfs-upload-*")
	if err != nil {
		release()
		lfsError(w, http.StatusInternalServerError, "scratch dir failed")
		return
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "obj")
	f, err := os.Create(path)
	if err != nil {
		release()
		lfsError(w, http.StatusInternalServerError, "scratch file failed")
		return
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r.Body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		release()
		lfsError(w, http.StatusBadRequest, "reading upload body failed: "+err.Error())
		return
	}
	// The server-enforced Content-Length cap makes n <= ContentLength; a
	// short body errors above. Assert rather than assume.
	assertf(n == r.ContentLength, "lfs upload %s: read %d bytes with Content-Length %d", key, n, r.ContentLength)
	// The oid IS the content address: publishing bytes under a key whose
	// hash doesn't match would corrupt every future download of that oid.
	if got := hex.EncodeToString(h.Sum(nil)); got != oid {
		release()
		lfsError(w, http.StatusBadRequest, fmt.Sprintf("content sha256 %s does not match oid %s", got, oid))
		return
	}
	if err := s.Blob.UploadFile(ctx, key, path); err != nil {
		release()
		log.Printf("lfs upload %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object upload failed")
		return
	}
	// Publish-before-commit: the bytes are durable, now settle the ledger
	// and register the object. A failure here may still have committed
	// (ambiguous), so the reservation is deliberately left to the TTL and
	// the sweeper — mirroring the pre-receive hook's crash semantics — and
	// the client's retry lands on the stored-object adoption path above.
	regErr := s.Store.RegisterLFSObject(ctx, repoID, key, n)
	if regErr != nil {
		regErr = fmt.Errorf("lfs register %s: %w", key, regErr)
	}
	// Writer half of the orphan-sweep coordination (issue #5): a sweeper
	// may have deleted the just-uploaded key right before the registration
	// committed; re-upload from the local file if so. Runs even when
	// registration failed — an ambiguous error may have registered the
	// object, and a committed row must never point at missing bytes.
	if err := blob.EnsurePublished(ctx, s.Blob, key, path); err != nil {
		log.Printf("lfs verify %s: %v", key, errors.Join(regErr, err))
		lfsError(w, http.StatusInternalServerError, "object verification failed")
		return
	}
	if regErr != nil {
		log.Printf("%v", regErr)
		lfsError(w, http.StatusInternalServerError, "object registration failed")
		return
	}
	w.WriteHeader(http.StatusOK)
}
