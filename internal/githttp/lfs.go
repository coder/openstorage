// Git LFS Batch API (docs/explanation/git-protocol-and-namespaces.md): the storage host serves the standard LFS
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
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
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

// lfsCleanupTimeout bounds the cap-denial blob cleanup (Codex round 14):
// the delete must finish independently of the client's connection lifetime
// — detached from request cancellation (a hang-up must not skip it) and
// deadline-bounded (a stalled backend must not pin the handler's staging
// reservation and per-key slot). A var so tests can shrink it.
var lfsCleanupTimeout = time.Minute

// lfsUploadIdleTimeout bounds how long a proxy upload body may go without
// delivering ANY bytes (Codex security round on #51): an upload holds a
// per-key slot and a staging-budget reservation from before its first body
// read, so a client that stalls after declaring Content-Length would
// otherwise pin that capacity until it hangs up on its own. An idle bound
// — reset on every chunk — caps the stall without capping object size or
// penalizing slow-but-moving links. A var so tests can shrink it.
var lfsUploadIdleTimeout = time.Minute

// The per-repo registered-object cap lives in the ledger
// (ref.MaxLFSObjectsPerRepo) and is enforced atomically inside
// RegisterLFSObject's serialized transaction (Codex round 6). The checks
// at this layer's two upload entrances are fast-fail UX only — they stop
// wasted transfers, the ledger stops overshoot.

// idleDeadlineBody wraps an upload body, pushing the connection's read
// deadline forward on every chunk of progress — but never past the
// absolute bound (Codex security round 2: renewing on every nonempty read
// alone lets one byte per idle window pin staging capacity forever). The
// absolute bound is the reservation TTL: an upload outliving its own
// admission reservation breaks accounting anyway, and the TTL's documented
// sizing (1h clears a default-quota 10 GiB object at ~3 MB/s) is exactly a
// minimum-sustained-rate bound. Backed by
// http.ResponseController.SetReadDeadline, which the standard net/http
// server supports on HTTP/1 and HTTP/2 connections alike; wrappers that do
// not (httptest recorders) are detected once by the caller and left
// unbounded — the guard targets real deployments.
type idleDeadlineBody struct {
	r        io.Reader
	rc       *http.ResponseController
	absolute time.Time
}

// deadline returns the next idle deadline, clamped to the absolute bound.
func (b idleDeadlineBody) deadline(now time.Time) time.Time {
	d := now.Add(lfsUploadIdleTimeout)
	if d.After(b.absolute) {
		return b.absolute
	}
	return d
}

func (b idleDeadlineBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if n > 0 {
		// Errors here mean the connection lost deadline support mid-read
		// (it cannot — the caller probed it); the read itself still
		// succeeded, so pass the data through regardless.
		_ = b.rc.SetReadDeadline(b.deadline(time.Now()))
	}
	return n, err
}

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
	// Registered objects are served ONLY from the canonical key; rowless
	// objects fall back to the legacy spelling (the read-compat surface
	// for not-yet-adopted pre-#51 blobs). Rowless canonical bytes are
	// never served: they may be a mid-adoption unverified copy (Codex
	// security round 78).
	registered, rerr := s.Store.HasLFSObject(r.Context(), target.repo.RepoID, key)
	if rerr != nil {
		log.Printf("lfs registry %s: %v", key, rerr)
		lfsObjectFail(&res, http.StatusInternalServerError, "object registry unavailable")
		return res
	}
	if !registered {
		key = blob.LFSLegacyKey(target.repo.RepoID, obj.OID)
	}
	size, err := s.Blob.Stat(r.Context(), key)
	if errors.Is(err, blob.ErrNotFound) && !registered {
		// The registry read races the object's first post-upgrade
		// adoption (Codex round 79): a commit between the read and this
		// stat deletes the legacy source, and a plain 404 here would be
		// false — the verified canonical object is healthy. Re-check and
		// retry canonically.
		if reg2, rerr2 := s.Store.HasLFSObject(r.Context(), target.repo.RepoID, blob.LFSKey(target.repo.RepoID, obj.OID)); rerr2 == nil && reg2 {
			key = blob.LFSKey(target.repo.RepoID, obj.OID)
			size, err = s.Blob.Stat(r.Context(), key)
		}
	}
	if errors.Is(err, blob.ErrNotFound) && registered {
		// Registered but blobless (Codex round 119): the canonical bytes
		// were lost to storage, yet the adoption-retained legacy copy may
		// be the object's only surviving bytes — reap grace deliberately
		// spares exactly this state, and a plain 404 would strand it
		// until some client re-uploads. Recover through the verified
		// adoption seam (digest-proof, canonical publish under the fleet
		// claim, idempotent re-registration): legacy bytes are NEVER
		// served directly under a registered row (round 78 — presigned
		// PUTs bound no digest). A concurrent restorer answers 429 so
		// the client retries into its result; any other failure leaves
		// the honest 404.
		switch aerr := s.restoreLFSObjectLocked(r.Context(), target.repo.RepoID, key, obj.OID); {
		case aerr == nil:
			size, err = s.Blob.Stat(r.Context(), key)
		case errors.Is(aerr, errLFSBusy):
			lfsObjectFail(&res, http.StatusTooManyRequests, "object recovery in progress; retry")
			return res
		default:
			log.Printf("lfs download restore %s: %v", key, aerr)
		}
	}
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
	zeroGrowth := make(map[string]bool)
	checked := make(map[string]bool)
	checkFailed := make(map[string]bool)
	var order []string
	for i, obj := range objs {
		results[i] = lfsObjectResult{OID: obj.OID, Size: obj.Size}
		key, ok := lfsValidateSpec(&results[i], target, obj)
		if !ok {
			continue
		}
		// The registry row is authoritative for "stored" (Codex security
		// round 78): registered objects live at the canonical key, and a
		// rowless canonical blob — a mid-adoption unverified copy — must
		// never satisfy an upload. This read doubles as the zero-growth
		// classification below (Codex round 5): a registered object whose
		// canonical blob vanished (racing DeleteLFS sweep, manual bucket
		// surgery) is already counted by the usage counter AND the
		// object-count cap, so its re-publication must be charged by
		// neither — an at-quota (or at-cap) repository could otherwise
		// never repair itself.
		registered := false
		if checked[key] {
			registered = zeroGrowth[key]
		} else {
			var rerr error
			registered, rerr = s.Store.HasLFSObject(ctx, repoID, key)
			if rerr != nil {
				log.Printf("lfs registry %s: %v", key, rerr)
				checked[key] = true
				checkFailed[key] = true
			}
		}
		if checkFailed[key] {
			// Every occurrence of an uncheckable key fails: without the
			// zero-growth classification the entry can be neither charged
			// correctly nor safely admitted free.
			lfsObjectFail(&results[i], http.StatusInternalServerError, "object registry unavailable")
			continue
		}
		if registered {
			switch _, err := s.Blob.Stat(ctx, key); {
			case err == nil:
				// Liveness handshake (issue #73): answering "stored"
				// promises the client it may commit a pointer file
				// referencing this OID, so clear the reclamation mark
				// FIRST — the touch buys the client a full grace window
				// before retirement can reconsider the row. touched=false
				// means the reclaimer retired the row between the
				// registry read above and here: answer retryable instead
				// of promising a vanished object (the retry re-reads the
				// registry and issues a real upload action).
				if touched, terr := s.Store.TouchLFSObject(ctx, repoID, key); terr != nil {
					log.Printf("lfs touch %s: %v", key, terr)
					lfsObjectFail(&results[i], http.StatusInternalServerError, "object registry unavailable")
					continue
				} else if !touched {
					lfsObjectFail(&results[i], http.StatusTooManyRequests,
						"a concurrent operation reclaimed this object, retry")
					continue
				}
				// Registered with canonical bytes present: stored — no
				// actions means the client skips the upload. A memo
				// parked for a registration that (indeterminately)
				// committed is dead weight; free it and its lease.
				s.dropLFSVerifyResume(key)
				// Bridge convergence for the BATCH fast path (Codex round
				// 84): after a failed compat copy, the natural client
				// retry is another batch request landing exactly here —
				// reporting "stored" without repairing the missing legacy
				// spelling would strand old replicas on 404 with no flow
				// left to converge.
				if cerr := s.ensureLegacyCompat(ctx, repoID, obj.OID, key); cerr != nil {
					log.Printf("lfs compat %s: %v", key, cerr)
					lfsObjectFail(&results[i], compatFailStatus(cerr), "compatibility copy failed, retry")
					continue
				}
				// Re-arm the grace clock immediately before the stored
				// answer (Codex round 12): the compat copy above is
				// synchronous and size-scaled, so the entry touch's
				// window may be largely consumed — or, past a short
				// grace, the row may have been re-marked and retired
				// while the copy held the claim's spare only for its
				// own duration. The touch is the LAST registry write
				// before success; touched=false answers retryable.
				if touched, terr := s.Store.TouchLFSObject(ctx, repoID, key); terr != nil {
					log.Printf("lfs touch %s: %v", key, terr)
					lfsObjectFail(&results[i], http.StatusInternalServerError, "object registry unavailable")
				} else if !touched {
					lfsObjectFail(&results[i], http.StatusTooManyRequests,
						"a concurrent operation reclaimed this object, retry")
				}
				continue
			case !errors.Is(err, blob.ErrNotFound):
				log.Printf("lfs stat %s: %v", key, err)
				lfsObjectFail(&results[i], http.StatusInternalServerError, "object storage unavailable")
				continue
			}
			// Registered but blobless: fall through to the zero-growth
			// repair admission.
		} else {
			switch _, err := s.Blob.Stat(ctx, blob.LFSLegacyKey(repoID, obj.OID)); {
			case err == nil:
				// Rowless legacy bytes: adopt — copy to the canonical
				// key, verify, register (pre-#51 uploads have no rows;
				// without adoption the orphan sweep would classify
				// referenced objects as reapable, so a swallowed adoption
				// failure here would let the client commit a pointer to a
				// reapable blob).
				switch aerr := s.adoptLFSObjectLocked(ctx, repoID, key, obj.OID); {
				case aerr == nil:
					continue
				case errors.Is(aerr, ref.ErrLFSObjectCapExceeded):
					lfsObjectFail(&results[i], http.StatusInsufficientStorage, fmt.Sprintf(
						"repository LFS object count limit reached (limit %d)", ref.MaxLFSObjectsPerRepo))
					continue
				case errors.Is(aerr, errLFSAdoptQuotaExceeded), errors.Is(aerr, errLFSCompatQuotaExceeded):
					// See the PUT switch (Codex round 103): adoption's tail
					// compat repair can surface the bridge's quota
					// sentinel, a 507 verdict rather than a failure.
					lfsObjectFail(&results[i], http.StatusInsufficientStorage, aerr.Error())
					continue
				case errors.Is(aerr, errLFSBusy):
					lfsObjectFail(&results[i], http.StatusTooManyRequests,
						"a concurrent operation holds this object, retry")
					continue
				case errors.Is(aerr, errLFSContentMismatch):
					// A mismatched legacy blob was removed: the object is
					// now genuinely missing — fall through and issue an
					// upload action so the client's correct bytes arrive
					// verified.
				default:
					log.Printf("lfs adopt %s: %v", key, aerr)
					lfsObjectFail(&results[i], http.StatusInternalServerError, "object registration failed, retry")
					continue
				}
			case !errors.Is(err, blob.ErrNotFound):
				log.Printf("lfs stat %s: %v", key, err)
				lfsObjectFail(&results[i], http.StatusInternalServerError, "object storage unavailable")
				continue
			}
			// Nothing stored anywhere — but a v0.2.0 legacy-spelled ROW
			// may survive without its blob (Codex round 87): that object
			// is a zero-growth REPAIR, not a new upload — registration
			// migrates the row in place, adding no count and no bytes,
			// with the repair cover accounting the returning bytes.
			// Classifying it as new made byte-quota and object-cap
			// denials PERMANENT: a 507 with no upload action leaves
			// nothing that could ever repair the row.
			if !checked[key] {
				legacyRegistered, rerr := s.Store.HasLFSObject(ctx, repoID, blob.LFSLegacyKey(repoID, obj.OID))
				if rerr != nil {
					log.Printf("lfs registry %s: %v", key, rerr)
					checked[key] = true
					checkFailed[key] = true
					lfsObjectFail(&results[i], http.StatusInternalServerError, "object registry unavailable")
					continue
				}
				registered = legacyRegistered
			}
		}
		if !checked[key] {
			checked[key] = true
			zeroGrowth[key] = registered
			order = append(order, key)
		}
		needIdx[key] = append(needIdx[key], i)
		needSize[key] = max(needSize[key], obj.Size)
	}
	// refreshZeroGrowth re-reads the still-new keys' registry rows: any
	// denial below may rest on a STALE classification (Codex round 28) —
	// a concurrent identical upload on another replica can register a
	// requested OID after the initial read, making that key zero-growth
	// (its bytes and row are already counted). Called only on the brink
	// of a denial, so the happy path keeps one registry read per key;
	// registration is terminal per key, so flips to zero-growth are
	// final.
	refreshZeroGrowth := func() error {
		for _, key := range order {
			if zeroGrowth[key] {
				continue
			}
			registered, rerr := s.Store.HasLFSObject(ctx, repoID, key)
			if rerr != nil {
				return rerr
			}
			if registered {
				zeroGrowth[key] = true
			}
		}
		return nil
	}
	countNew := func() int64 {
		var n int64
		for _, key := range order {
			if !zeroGrowth[key] {
				n++
			}
		}
		return n
	}
	// Object-count cap (Codex security round 2): NEW objects — keys with
	// no registry row — bound the repo's permanent object population at
	// admission, independent of bytes. Zero-growth repairs and stored
	// objects are exempt: they add no rows.
	if newObjects := countNew(); newObjects > 0 {
		count, err := s.Store.CountLFSObjects(ctx, repoID)
		if err != nil {
			log.Printf("lfs registry count %s: %v", repoID, err)
			return s.denyAllNeeded(order, needIdx, results, http.StatusInternalServerError, "object registry unavailable")
		}
		if count+newObjects > ref.MaxLFSObjectsPerRepo {
			if err := refreshZeroGrowth(); err != nil {
				log.Printf("lfs registry %s: %v", repoID, err)
				return s.denyAllNeeded(order, needIdx, results, http.StatusInternalServerError, "object registry unavailable")
			}
			if count+countNew() > ref.MaxLFSObjectsPerRepo {
				return s.denyAllNeeded(order, needIdx, results, http.StatusInsufficientStorage, fmt.Sprintf(
					"repository LFS object count limit reached (%d objects stored, limit %d)",
					count, ref.MaxLFSObjectsPerRepo))
			}
		}
	}
	// Admission: reserve the declared sizes before ANY transfer href is
	// issued. Zero-size entries charge nothing (the empty object is
	// zero growth) but still get actions.
	//
	// ADVISORY admission only — the batch inserts NO reservation rows
	// (Codex round 17): any batch-time durable hold is rotatable (a
	// fresh OID per TTL re-acquires the whole headroom without ever
	// transferring a byte), so durable ledger writes require transfer
	// progress — the PUT's post-body admission is authoritative. This
	// check is the fast-fail UX: an already-over-quota batch answers
	// per-object 507s before any transfer href is issued. A denial
	// verdict refreshes the zero-growth classification once and re-runs
	// (Codex round 28): the verdict may rest on a twin's registration
	// whose bytes ls.UsageBytes already counts while incoming still
	// charges them again.
	if s.PackQuotaBytes > 0 {
		buildPacks := func() []ref.Pack {
			var packs []ref.Pack
			for _, key := range order {
				if sz := needSize[key]; sz > 0 && !zeroGrowth[key] {
					packs = append(packs, ref.Pack{Key: key, Size: sz})
				}
			}
			return packs
		}
		denyAll := func(code int, msg string) []lfsObjectResult {
			return s.denyAllNeeded(order, needIdx, results, code, msg)
		}
		for attempt, packs := 0, buildPacks(); len(packs) > 0; attempt++ {
			// Saturating sum (Codex round 21): declared sizes are
			// arbitrary client int64s, and two MaxInt64 entries wrap a
			// naive sum negative — panicking Admitted's non-negative
			// assertion instead of denying. The ledger's own arithmetic
			// keeps the total on the denying side.
			incoming := ref.IncomingBytes(packs)
			floor, err := s.CachedUsageFloor(ctx, repoID)
			if err != nil {
				log.Printf("lfs quota: %s: usage listing failed: %v", repoID, err)
				return denyAll(http.StatusInternalServerError, "storage quota check failed")
			}
			// The batch's OWN keys are excluded from the reserved sum
			// (Codex round 24), mirroring ReservePackBytes's same-key
			// exclusion: a failed verified PUT leaves a settle-or-TTL
			// reservation for exactly these keys, and counting it here
			// ON TOP of incoming would 507 the retry that settles it
			// until the TTL expired. incoming still charges each key's
			// declared size, so nothing is admitted for free.
			keys := make([]string, len(packs))
			for i, p := range packs {
				keys[i] = p.Key
			}
			ls, err := s.Store.QuotaLedgerState(ctx, blob.QuotaRepoGroup(repoID), s.lfsReservationTTL(), keys...)
			if err != nil {
				log.Printf("lfs quota: %s: ledger read failed: %v", repoID, err)
				return denyAll(http.StatusInternalServerError, "storage quota check failed")
			}
			// SUM, not max (Codex round 44) — the floor is the
			// uncounted-bytes snapshot; see ref.ReservePackBytes.
			used := ref.AddSat(ls.UsageBytes, floor)
			if ref.Admitted(used, ls.ReservedBytes, incoming, s.PackQuotaBytes) {
				break
			}
			if attempt > 0 {
				return denyAll(http.StatusInsufficientStorage, fmt.Sprintf(
					"repository storage quota exceeded: upload adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight uploads), quota is %d bytes",
					incoming, used, ls.ReservedBytes, s.PackQuotaBytes))
			}
			if err := refreshZeroGrowth(); err != nil {
				log.Printf("lfs registry %s: %v", repoID, err)
				return denyAll(http.StatusInternalServerError, "storage quota check failed")
			}
			packs = buildPacks()
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

// denyAllNeeded fails every batch entry that still needs an upload with
// one shared per-object error (all-or-nothing admission).
func (s *Server) denyAllNeeded(order []string, needIdx map[string][]int, results []lfsObjectResult, code int, msg string) []lfsObjectResult {
	for _, key := range order {
		for _, i := range needIdx[key] {
			lfsObjectFail(&results[i], code, msg)
		}
	}
	return results
}

// lfsReservationTTL resolves the TTL LFS admission charges: the serve
// wiring's PackReservationTTL, or the shared quota.go default. Production
// wiring guarantees the value strictly exceeds the usage-floor cache
// lifetime (serve's config rejects smaller TTLs at startup — see
// MinPackReservationTTL); tests drive shorter TTLs deliberately to
// exercise expiry.
func (s *Server) lfsReservationTTL() time.Duration {
	if s.PackReservationTTL > 0 {
		return s.PackReservationTTL
	}
	return defaultReservationTTL
}

// lfsPublicationLease bounds publication + registration work anchored at
// the pre-publish reservation refresh: at most 90% of the TTL, AND never
// past TTL − MinPackReservationTTL (Codex round 51) — if registration
// fails at the lease's very end, the durable rowless orphan must still
// have a FULL floor-freshness-plus-handoff window (60s) of reservation
// cover left, or a floor scanned just before publication could outlive
// the reservation and admit a second writer into the same headroom. For
// TTLs ≥ 10 minutes the 90% bound is the tighter one (10% ≥ 60s); below
// that the absolute reserve binds. Sub-minimum test TTLs yield a
// non-positive lease clamped to a nominal second (production TTLs cannot:
// the config floor is MinPackReservationTTL, exclusive).
func (s *Server) lfsPublicationLease() time.Duration {
	ttl := s.lfsReservationTTL()
	lease := min(ttl*9/10, ttl-MinPackReservationTTL)
	if lease <= 0 {
		lease = time.Second
	}
	return lease
}

// errLFSContentMismatch reports a stored blob whose bytes do not hash to
// its content address — a legacy presigned-era upload that was never
// digest-verified (issue #51's original hole).
var errLFSContentMismatch = errors.New("stored LFS object does not match its content address")

// errLFSBusy is a transient denial: a concurrent operation holds the
// resources (per-key slot, staging budget) this request needs. Callers
// answer 429 + Retry-After.
var errLFSBusy = errors.New("concurrent LFS operation in progress")

// errLFSCompatQuotaExceeded is the v0.2.0 bridge copy's quota denial
// (Codex security round 84): the bridge doubles a fresh upload's physical
// bytes for the roll's duration, and at the quota edge the copy is DENIED
// rather than overshooting — callers map it to 507.
var errLFSCompatQuotaExceeded = errors.New("repository storage quota exceeded by the v0.2.0 compatibility copy")

// errLFSAdoptQuotaExceeded is the adoption copy's quota denial (Codex
// round 85): canonicalizing a legacy object materializes a second
// size-byte copy while the source stays retained (and floor-charged) until
// the post-roll sweep, so the copy passes real admission — at the quota
// edge adoption fails with 507 instead of durably exceeding the quota.
var errLFSAdoptQuotaExceeded = errors.New("repository storage quota exceeded by the adoption copy (free space or raise the quota to migrate this object)")

// compatFailStatus maps a compat-bridge failure to its response status:
// quota denials are 507 (the client must free space or finish the roll),
// contention is 429, everything else is a retryable upstream failure.
func compatFailStatus(err error) int {
	switch {
	case errors.Is(err, errLFSCompatQuotaExceeded):
		return http.StatusInsufficientStorage
	case errors.Is(err, errLFSBusy):
		return http.StatusTooManyRequests
	default:
		return http.StatusBadGateway
	}
}

// adoptLFSObjectLocked runs adoptLFSObject under the per-key in-flight
// slot — the batch path's entrance (the PUT path already holds the slot
// for its whole lifetime). errLFSBusy when the slot is taken.
func (s *Server) adoptLFSObjectLocked(ctx context.Context, repoID, key, oid string) error {
	if _, busy := s.lfsUploadsInFlight.LoadOrStore(key, struct{}{}); busy {
		return errLFSBusy
	}
	defer s.lfsUploadsInFlight.Delete(key)
	return s.adoptLFSObject(ctx, repoID, key, oid)
}

// restoreLFSObjectLocked re-materializes a REGISTERED object whose
// canonical blob was lost to storage from its adoption-retained legacy
// copy (Codex round 119): the registered fast path deliberately
// short-circuits every ordinary touch, so a download that just proved the
// canonical spelling absent must bypass it to reach the verify → publish →
// re-verify → register loop. Zero-growth by construction — the row already
// holds its cap slot and its bytes stay counted; registration is the
// documented idempotent no-op.
func (s *Server) restoreLFSObjectLocked(ctx context.Context, repoID, key, oid string) error {
	if _, busy := s.lfsUploadsInFlight.LoadOrStore(key, struct{}{}); busy {
		return errLFSBusy
	}
	defer s.lfsUploadsInFlight.Delete(key)
	return s.adoptStoredLFSObject(ctx, repoID, key, oid, true)
}

// adoptLFSObject backfills the lfs_objects registry row for an object that
// is already stored but was never registered (uploads predating issue #51,
// or a verified upload whose registration failed after publish). Callers
// MUST propagate a failure instead of reporting the object stored (Codex
// round 1): telling the client "stored" lets it commit its pointer, and
// with DeleteLFS enabled a still-rowless object eventually classifies as an
// orphan and is reaped — a transient ref-store outage must fail the request
// so the client retries, never silently downgrade to data loss.
// errLFSContentMismatch means the stored blob was provably wrong for its
// key and has been removed — the caller must treat the object as MISSING
// and take the verified upload path.
func (s *Server) adoptLFSObject(ctx context.Context, repoID, key, oid string) error {
	return s.adoptStoredLFSObject(ctx, repoID, key, oid, false)
}

// adoptStoredLFSObject is adoptLFSObject with the round-119 restore mode:
// restore bypasses the registered fast path (which never republishes) so a
// registered-but-blobless canonical can be re-materialized from its
// retained legacy source through the full verify → publish → re-verify →
// register loop. The registered row keeps its cap slot and byte credit, so
// restore skips the cap precheck; the copy's cover admission stays real —
// at the quota edge a restore may be conservatively denied, which fails
// the download honestly rather than overshooting.
func (s *Server) adoptStoredLFSObject(ctx context.Context, repoID, key, oid string, restore bool) error {
	registered, err := s.Store.HasLFSObject(ctx, repoID, key)
	if err != nil {
		return err
	}
	if registered && !restore {
		// Liveness handshake (issue #73, review round 1): this fast path
		// answers every direct PUT of an already-stored object, and a 200
		// promises the client it may commit a pointer referencing the
		// OID — clear the reclamation mark FIRST, exactly like the batch
		// "stored" answer, so a grace-expired mark cannot let the
		// reclaimer retire the row before the client pushes. touched=false
		// means the reclaimer retired the row between the registry read
		// above and here: fall through to the full adoption below, which
		// re-verifies the stored bytes and mints a fresh registered row
		// instead of promising a vanished one.
		touched, terr := s.Store.TouchLFSObject(ctx, repoID, key)
		if terr != nil {
			return terr
		}
		if touched {
			// A prior attempt's registration may have committed after
			// its bounded context expired (indeterminate failure), with
			// the lease parked for a retry that now has nothing to do
			// (Codex round 76): free the memo and its lease.
			s.dropLFSVerifyResume(key)
			// The registered fast path is the ONLY path retries of a
			// stored object ever reach, so bridge convergence lives here
			// (Codex round 83): a compat copy that failed after
			// registration is repaired by the next write-scoped touch
			// instead of 404ing on old replicas forever.
			if cerr := s.ensureLegacyCompat(ctx, repoID, oid, key); cerr != nil {
				return cerr
			}
			// Re-arm the grace clock after the size-scaled compat copy
			// (Codex round 12), mirroring the batch stored answer: the
			// 200 must be the promise of a freshly touched row, not one
			// whose window the copy consumed. A vanished row answers
			// retryable; the retry re-verifies and re-registers.
			if touched, terr := s.Store.TouchLFSObject(ctx, repoID, key); terr != nil {
				return terr
			} else if !touched {
				return errLFSBusy
			}
			return nil
		}
		registered = false
	}
	// Fast-fail the object cap BEFORE the verification download (Codex
	// security round 4): an at-cap repository holding a rowless legacy
	// blob would otherwise pay a full download+hash on every write-scoped
	// touch and then 507 in RegisterLFSObject anyway — a repeatable
	// bandwidth/staging drain (a near-budget blob blocks the replica's
	// unrelated uploads for the whole hash). O(1) counter read; advisory
	// only — RegisterLFSObject's in-transaction check stays authoritative.
	// A registered row already holds its slot (the restore entrance):
	// zero growth, no precheck.
	count := int64(0)
	if !registered {
		count, err = s.Store.CountLFSObjects(ctx, repoID)
		if err != nil {
			return err
		}
	}
	if count >= ref.MaxLFSObjectsPerRepo {
		// At the boundary the count races a twin registering THIS key
		// (Codex round 27): between the registered check above and this
		// count, a concurrent identical upload on another replica can
		// take the last slot with the very key being adopted — this
		// request is then zero-growth, not over-cap. Re-read before
		// failing; registration is terminal per key, so a true here is
		// final.
		registered, rerr := s.Store.HasLFSObject(ctx, repoID, key)
		if rerr != nil {
			return rerr
		}
		if registered {
			// The race winner repairs the bridge like the registered fast
			// path (Codex round 98): a bare success here let the client
			// commit its pointer while the twin's potentially long bridge
			// copy was still in flight — a copy failure then stranded the
			// legacy spelling with no request left to converge it.
			s.dropLFSVerifyResume(key)
			if cerr := s.ensureLegacyCompat(ctx, repoID, oid, key); cerr != nil {
				return cerr
			}
			// Re-arm the grace clock after the size-scaled bridge copy,
			// exactly like the registered fast path (Codex round 13): the
			// twin minted this row BEFORE a potentially grace-long copy,
			// so a scan can mark it mid-copy and a write-scoped 200 must
			// still be the promise of a freshly touched row. Restores
			// answer downloads — no pointer-commit promise — and must
			// never extend an unreferenced row's life.
			if restore {
				return nil
			}
			// Bounded + detached like every post-registration registry
			// write here: adoption completes under its own lease once
			// started, so a canceled client must not abort the touch.
			tctx, tcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
			defer tcancel()
			if touched, terr := s.Store.TouchLFSObject(tctx, repoID, key); terr != nil {
				return terr
			} else if !touched {
				return errLFSBusy
			}
			return nil
		}
		// A v0.2.0 legacy-spelled row for THIS oid already holds the
		// object's cap slot (Codex round 86): registration migrates it in
		// place — zero growth for both the count and usage ledgers — so
		// the cap must not deny the touch. Failing here made every
		// write-scoped touch of an existing legacy object answer 507
		// forever once the repository reached MaxLFSObjectsPerRepo,
		// permanently blocking exactly the migrations that add nothing.
		legacyRegistered, lerr := s.Store.HasLFSObject(ctx, repoID, blob.LFSLegacyKey(repoID, oid))
		if lerr != nil {
			return lerr
		}
		if !legacyRegistered {
			return ref.ErrLFSObjectCapExceeded
		}
		// Zero-growth in-place migration: proceed.
	}
	// Fleet-wide adoption claim (Codex security round 73): the in-flight
	// map below this call dedupes only within one process, so replicas
	// racing the same legacy oid would EACH download and hash the whole
	// object — tiny write-scoped batches fanned across the fleet
	// multiply backend reads by fleet size. One replica wins the shared
	// TTL'd claim; losers deny transiently (the client's retry lands on
	// the winner's committed registration — zero-growth — or re-claims
	// after a crash's claim expires).
	// A prior resumable abort may have PARKED the lease with its saved
	// progress (Codex round 75): renew the parked token instead of
	// competing for a fresh claim, so the resume midstate — which lives
	// only in this process — stays paired with the live lease and no
	// replica restarts the hash from offset zero.
	claimOwner := s.takeParkedLFSClaimOwner(key)
	if claimOwner != "" {
		rctx, rcancel := context.WithTimeout(ctx, lfsClaimRenewTimeout)
		renewed, rerr := s.Store.RenewAdoptionClaim(rctx, key, claimOwner)
		rcancel()
		if rerr != nil {
			// Indeterminate: the lease may still be live. Re-park the
			// token for the next attempt; if a concurrent eviction
			// discarded the midstate meanwhile, free the lease instead
			// of orphaning a token nobody can renew or release.
			if !s.parkLFSClaimOwner(key, claimOwner) {
				s.releaseAdoptionClaim(key, claimOwner)
			}
			return fmt.Errorf("renew parked adoption claim: %w", rerr)
		}
		if !renewed {
			claimOwner = "" // expired or taken over: compete for a fresh claim
		}
	}
	if claimOwner == "" {
		claimed, owner, err := s.Store.TryAdoptionClaim(ctx, repoID, key)
		if err != nil {
			return err
		}
		if !claimed {
			return errLFSBusy
		}
		claimOwner = owner
	}
	// Best-effort early release on every TERMINAL exit (success included:
	// the committed registration terminalizes the key, so the claim row
	// is dead weight either way); a crash leaves it to the TTL.
	// Owner-fenced (Codex round 74): after an expiry takeover, this stale
	// worker's release must not delete the successor's live lease. NOT
	// released when the verification below parked the lease with
	// resumable progress (Codex round 75): the held claim is what routes
	// the remaining work back to this process's saved midstate instead of
	// a foreign replica's offset-zero restart.
	defer func() {
		if s.parkedLFSClaimOwner(key) == claimOwner {
			return // parked: the next attempt renews and resumes
		}
		s.releaseAdoptionClaim(key, claimOwner)
	}()
	// Legacy blobs were written through presigned PUTs that bound no
	// digest, so existence does not prove content (Codex round 15).
	// Hash the stored bytes once before adoption; a mismatch is provably
	// wrong for its content address — no valid pointer can want those
	// bytes under this key — so the blob is removed and the client's
	// correct bytes come in through the verified path. At most one
	// hash per object lifetime: adopted objects never re-verify,
	// mismatches are deleted. CALLERS HOLD the per-key in-flight slot, so
	// same-replica verifications of one key are serialized. Verification
	// streams ranged reads through the hasher (one chunk of memory, no
	// disk — Codex round 24, replacing the round-16 disk metering whose
	// Stat-sized reservation could not bound a concurrently replaced
	// blob), so it takes only an op slot: the replica-wide concurrency
	// bound, not staging bytes.
	if !s.reserveLFSStaging(repoID, 0) {
		return errLFSBusy
	}
	defer s.releaseLFSStaging(repoID, 0)
	// VERIFY FIRST, publish after (Codex round 80, hardening round 78):
	// the legacy spelling is presigned-PUT-addressable in v0.1.x, so the
	// bytes must move to the canonical `o2/` key no legacy URL can
	// address — but a copy taken BEFORE verification carries unverified
	// bytes, and any interleaving (or crash) that leaves such a copy
	// where a verified publisher just registered serves forged bytes
	// under a trusted row. Adoption therefore verifies the SOURCE first
	// and publishes canonically only content the digest already proved:
	// no ordering with a verified publisher can put unverified bytes at
	// the canonical key, and the worst crash residue is a rowless (never
	// served) canonical blob the next touch re-verifies. The verify
	// target is the canonical bytes when they exist (a crashed verified
	// publish or a prior adoption's publish — re-proven either way),
	// else the legacy source.
	target := key
	tctx, tcancel := context.WithTimeout(ctx, lfsProbeTimeout)
	_, terr := s.Blob.Stat(tctx, key)
	tcancel()
	switch {
	case errors.Is(terr, blob.ErrNotFound):
		target = blob.LFSLegacyKey(repoID, oid)
	case terr != nil:
		return fmt.Errorf("canonical probe %s: %w", key, terr)
	}
	// Advisory quota preflight BEFORE the ranged verification hash
	// (Codex security round 108): the copy admission lives in
	// publishVerifiedLFSObject — after the full-object hash — so an
	// over-quota repository re-read entire legacy blobs on every
	// write-scoped touch (a 1,000-entry batch of one OID re-hashes it
	// 1,000 times) with no byte ever uploading: repeatable,
	// client-cost-free blob-store traffic. A cheap ledger read fails the
	// hopeless adoption first; the real admission below stays
	// authoritative, and this read takes NO rows (the batch's round-17
	// advisory shape — a durable hold here would be rotatable). Only the
	// legacy→canonical copy needs headroom; re-adopting existing
	// canonical bytes is zero-growth.
	if target != key && s.PackQuotaBytes > 0 {
		sctx, scancel := context.WithTimeout(ctx, lfsProbeTimeout)
		srcSize, serr := s.Blob.Stat(sctx, target)
		scancel()
		if serr == nil && srcSize > 0 {
			floor, ferr := s.CachedUsageFloor(ctx, repoID)
			if ferr != nil {
				return fmt.Errorf("adoption preflight %s: usage listing: %w", key, ferr)
			}
			// The repair-cover spelling is excluded too (Codex round
			// 112): a failed covered copy leaves key+suffix reserved
			// until TTL by design, and the authoritative admission
			// refills exactly that row — counting it here ON TOP of the
			// planned copy 507'd every retry at exact headroom for up to
			// the reservation TTL.
			ls, lerr := s.Store.QuotaLedgerState(ctx, blob.QuotaRepoGroup(repoID), s.lfsReservationTTL(), key, key+lfsRepairCoverSuffix)
			if lerr != nil {
				return fmt.Errorf("adoption preflight %s: ledger read: %w", key, lerr)
			}
			if used := ref.AddSat(ls.UsageBytes, floor); !ref.Admitted(used, ls.ReservedBytes, srcSize, s.PackQuotaBytes) {
				return fmt.Errorf("adopt %s: %w (copy adds %d bytes to %d stored, +%d reserved, quota %d)",
					key, errLFSAdoptQuotaExceeded, srcSize, used, ls.ReservedBytes, s.PackQuotaBytes)
			}
		}
	}
	// Bounded like publication (Codex round 17) and DETACHED from the
	// client (Codex round 31, security): adoption is server-side repair
	// work. A client canceling mid-verification would abort the ranged
	// hash before RegisterLFSObject, leave the blob rowless, and let the
	// next tiny batch restart the whole hash at offset zero — repeatable
	// backend reads over large legacy blobs, one full pass per cancel.
	// Once adoption starts it runs to completion under the lease
	// regardless of the requester's fate; the committed row makes the
	// work once-per-object.
	// Adoption is NOT bounded by the reservation TTL at all (Codex round
	// 52, re-derived in security round 56): it holds no reservation — its
	// bytes are already durable, listed, and floor-covered — so no
	// TTL-coverage argument applies, and ANY TTL-derived cap makes some
	// object size permanently unadoptable: every write touch then replays
	// the full ranged read up to the cap, releases the per-key slot, and
	// the next tiny batch restarts it from offset zero — a repeatable,
	// client-cost-free bandwidth drain that never converges. The hash
	// must instead be bounded by its own progress (per-chunk stall guard
	// + size-scaled allowance inside verifyStoredLFSObject, mirroring
	// blob.EnsurePublished's repair allowance): every attempt against a
	// live backend COMPLETES and registers, making the work truly
	// once-per-object, while a wedged backend still frees the op slot
	// and verify buffer within one chunk window.
	// The verify→publish→RE-VERIFY loop (Codex round 81, hardening round
	// 80): iteration one hashes the SOURCE (so provably-wrong legacy
	// bytes are removed without ever writing the canonical key), the
	// publish copies the proven content canonically, and iteration two
	// re-hashes the DESTINATION — registration is bound to a digest proof
	// of the bytes actually at the canonical key, which no legacy URL can
	// address. Source fingerprinting alone cannot close the ABA: with
	// content-derived ETags, a still-valid legacy PUT can swap
	// true→forged→true around the copy and restore the original
	// fingerprint, so no before/after source comparison proves which
	// version the copy carried. The destination re-hash costs one extra
	// ranged read exactly once per object lifetime (the committed row
	// short-circuits every later touch).
	// Memoized-mismatch fast path (Codex security round 118): when the
	// legacy source still carries the exact fingerprint a previous FULL
	// verification proved wrong, repeating that whole-object read buys
	// nothing — a client spamming tiny batches for a corrupt object
	// would replay it for free every touch (deterministically, by
	// holding the repo's staging share so the cleanup below keeps
	// sparing). Press only the budgeted cleanup; deletion itself still
	// requires the single-version snapshot proof (round 115 — the
	// memo's own verify may have mixed ranged reads across a swap, so
	// the memo can gate WORK, never a delete). A moved fingerprint
	// invalidates the memo and the version proves itself in full.
	if target != key {
		memoFP, memoOK, memoErr := s.lfsMismatchMemo(ctx, repoID, target)
		if memoErr != nil {
			return memoErr // an unreadable marker is never absence (round 132)
		}
		if memoOK {
			fctx, fcancel := context.WithTimeout(ctx, lfsProbeTimeout)
			_, nowFP, ferr := s.Blob.StatFingerprint(fctx, target)
			fcancel()
			switch {
			case ferr == nil && nowFP == memoFP:
				// The snapshot can outlive ref.AdoptionClaimTTL (Codex
				// round 126's hazard on the compat twin): keep the held
				// canonical claim renewed for its whole size-scaled
				// life, join before judging (round 88), and spare on a
				// recorded loss.
				snapCtx, snapCancel := context.WithCancelCause(context.WithoutCancel(ctx))
				defer snapCancel(nil)
				stopRenew := s.keepAdoptionClaimRenewed(snapCtx, snapCancel, key, claimOwner)
				m, decisive := s.legacySourceProvenMismatched(snapCtx, repoID, target, oid)
				stopRenew()
				if cause := context.Cause(snapCtx); cause != nil && !errors.Is(cause, context.Canceled) {
					return errLFSContentMismatch // lease lost mid-snapshot: spared, memo stands
				}
				switch {
				case m:
					delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
					defer delCancel()
					if err := s.Blob.Delete(delCtx, target); err != nil && !errors.Is(err, blob.ErrNotFound) {
						return fmt.Errorf("remove mismatched legacy object %s: %w", target, err)
					}
					s.forgetLFSMismatch(ctx, repoID, target)
					log.Printf("lfs adopt %s: removed memoized mismatched legacy blob", key)
					return errLFSContentMismatch
				case decisive:
					// Proven VALID (Codex round 125): the memo itself
					// can be an A→B→A artifact of its own ranged
					// verification — retaining it made every touch of a
					// HEALTHY object re-snapshot forever, and returning
					// mismatch denied adoption of true bytes. Clear the
					// memo and fall through: this touch adopts the
					// now-proven source through the normal full
					// verification.
					s.forgetLFSMismatch(ctx, repoID, target)
				default:
					return errLFSContentMismatch // unknowable: spared, memo stands
				}
			case ferr == nil:
				s.forgetLFSMismatch(ctx, repoID, target) // replaced: prove in full
			}
		}
	}
	var verifiedSize int64
	var pen *lfsVerifyResume
	for attempt := 0; ; attempt++ {
		assertf(attempt < 2, "adoptLFSObject(%s): verify loop did not converge", key)
		var contentFP string
		var err error
		verifiedSize, pen, contentFP, err = s.verifyStoredLFSObject(context.WithoutCancel(ctx), repoID, key, target, oid, claimOwner)
		if err == nil {
			if target == key {
				break // canonical bytes digest-proven: register them
			}
			// PUBLISH the digest-proven source canonically (racing a
			// verified publisher at worst overwrites bytes that the
			// re-verification below then re-proves), then loop with the
			// DESTINATION as the verify target.
			if perr := s.publishVerifiedLFSObject(ctx, repoID, key, target, verifiedSize, claimOwner); perr != nil {
				return perr
			}
			target = key
			continue
		}
		if !errors.Is(err, errLFSContentMismatch) {
			return err
		}
		dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
		defer dcancel()
		if target != key {
			// The hash just proved the bytes IT READ wrong — but a
			// still-valid legacy presigned PUT can replace them with the
			// correct object between the hash and this delete (Codex
			// round 110, the source twin of round 108's
			// destination-mismatch guard): deleting unconditionally would
			// remove the newly valid — and potentially only — copy,
			// 404ing old replicas and outstanding downloads. Re-hash the
			// CURRENT content and delete only bytes still proven
			// mismatched; a spared survivor converges via the next
			// touch's verification. The canonical key was never written.
			// Memoize the failed verification bound to the observed
			// fingerprint (Codex security round 118): repeat touches of
			// the unchanged corrupt source skip the full re-hash and
			// press only the budgeted cleanup below. The mint is
			// REQUIRED before the snapshot (Codex security round 134):
			// the spared and lease-lost arms below return the mismatch
			// verdict — whose canonical upload registers — trusting
			// exactly this marker to press the surviving corrupt
			// spelling later, so a silently failed write laundered the
			// survivor as implicitly clean.
			if merr := s.rememberLFSMismatch(ctx, repoID, target, contentFP); merr != nil {
				return merr
			}
			// Claim-renewed, request-detached snapshot region (rounds
			// 126/127): the re-hash can outlive both the request and
			// ref.AdoptionClaimTTL, and a recorded lease loss must
			// cancel the download rather than be stripped.
			snapCtx, snapCancel := context.WithCancelCause(context.WithoutCancel(ctx))
			defer snapCancel(nil)
			stopRenew := s.keepAdoptionClaimRenewed(snapCtx, snapCancel, key, claimOwner)
			m, _ := s.legacySourceProvenMismatched(snapCtx, repoID, target, oid)
			stopRenew()
			if cause := context.Cause(snapCtx); cause != nil && !errors.Is(cause, context.Canceled) {
				return errLFSContentMismatch // lease lost mid-snapshot: spared, memo stands
			}
			if m {
				// The delete gets its OWN bounded window (Codex round
				// 116): a size-scaled snapshot can outlive dctx, and a
				// proven verdict must not lose its delete to the
				// consumed parent deadline.
				delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
				defer delCancel()
				if err := s.Blob.Delete(delCtx, target); err != nil && !errors.Is(err, blob.ErrNotFound) {
					return fmt.Errorf("remove mismatched legacy object %s: %w", target, err)
				}
				s.forgetLFSMismatch(ctx, repoID, target)
				log.Printf("lfs adopt %s: removed legacy blob failing content verification", key)
			} else {
				log.Printf("lfs adopt %s: sparing legacy source not proven mismatched", key)
			}
			return errLFSContentMismatch
		}
		// Canonical-bytes mismatch. Cross-replica coordination (Codex
		// rounds 16+18): the delete runs inside GuardedLFSDelete,
		// serialized against registrations — a concurrent verified
		// upload either registered first (the guard aborts; the
		// registered branch below re-examines) or blocks until the
		// delete commits and then republishes via its post-registration
		// writer verify (blob.EnsurePublished). No interleaving can
		// leave a registered row pointing at deleted bytes. Bounded and
		// detached from the client's connection like every cleanup.
		deleted, derr := s.Store.GuardedLFSDelete(dctx, repoID, key, func(delCtx context.Context) error {
			if err := s.Blob.Delete(delCtx, key); err != nil && !errors.Is(err, blob.ErrNotFound) {
				return err
			}
			return nil
		})
		if derr != nil {
			return fmt.Errorf("remove mismatched legacy object %s: %w", key, derr)
		}
		if !deleted {
			// A registered row exists. Historically that meant "a
			// concurrent verified upload won, its bytes stand" (round
			// 16) — but the canonical copy seam adds another ordering
			// (Codex round 80): this adoption's own copy can land AFTER
			// a verified publisher's upload+registration (the claim
			// fences only adopters), overwriting verified bytes with the
			// unverified legacy content just hashed. The digest proof is
			// decisive either way: bytes carrying THIS fingerprint are
			// provably not the row's content-addressed bytes. Delete
			// them only while the fingerprint still matches — a
			// concurrent verified/restore writer moves it, and then its
			// bytes rightly stand. The check-then-delete pair is further
			// serialized against verified publishers (Codex round 84):
			// the PUT's publish/register/verify region now holds this
			// same fleet claim, so no publisher can land valid bytes
			// between the fingerprint read and this delete — the only
			// unfenced canonical writer left is the sweeper's quarantine
			// heal, whose restores are digest-verified content with a
			// NEW fingerprint (the conditional spares them; the residual
			// heal-mid-window race at worst leaves
			// registered-but-blobless, which self-heals zero-growth) —
			// never forged bytes served under a trusted OID.
			fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
			_, nowFP, ferr := s.Blob.StatFingerprint(fctx, key)
			fcancel()
			switch {
			case errors.Is(ferr, blob.ErrNotFound):
				return errLFSContentMismatch // the corrupt bytes are already gone
			case ferr != nil:
				return fmt.Errorf("fingerprint mismatched registered object %s: %w", key, ferr)
			case nowFP != contentFP:
				return nil // replaced since the hash: the new writer's verified bytes stand
			}
			if err := s.Blob.Delete(dctx, key); err != nil && !errors.Is(err, blob.ErrNotFound) {
				return fmt.Errorf("remove corrupt canonical object %s: %w", key, err)
			}
			log.Printf("lfs adopt %s: removed corrupt canonical bytes under a registered row (heals via writer verify or client retry)", key)
			return errLFSContentMismatch
		}
		// The forged bytes came from the legacy source (the canonical
		// copy is adoption-written only) — but the destination's failed
		// digest proves only what the COPY caught, not what the source
		// holds NOW (Codex round 108, P2): a legacy presigned PUT can
		// restore the correct object while the destination verifies, and
		// an unconditional delete would remove the only valid copy (404s
		// on old replicas until a client re-uploads). Re-hash the CURRENT
		// source — fingerprints are cross-key incomparable and ABA-prone,
		// only a fresh digest is decisive — and delete only proven-wrong
		// bytes; unknowable state spares. Best-effort like before (Codex
		// security round 78): a spared survivor is re-copied, re-fails
		// the digest, and lands back here.
		legacySrc := blob.LFSLegacyKey(repoID, oid)
		// Claim-renewed, request-detached snapshot region (rounds
		// 126/127) — see the source-mismatch twin above.
		snapCtx, snapCancel := context.WithCancelCause(context.WithoutCancel(ctx))
		defer snapCancel(nil)
		stopRenew := s.keepAdoptionClaimRenewed(snapCtx, snapCancel, key, claimOwner)
		m, _ := s.legacySourceProvenMismatched(snapCtx, repoID, legacySrc, oid)
		stopRenew()
		if cause := context.Cause(snapCtx); cause != nil && !errors.Is(cause, context.Canceled) {
			return errLFSContentMismatch // lease lost mid-snapshot: spared, memo stands
		}
		if m {
			// The delete gets its OWN bounded window (Codex round 116):
			// a size-scaled snapshot can outlive dctx, and a proven
			// verdict must not lose its delete to the consumed parent
			// deadline.
			delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
			defer delCancel()
			if err := s.Blob.Delete(delCtx, legacySrc); err != nil && !errors.Is(err, blob.ErrNotFound) {
				// A FAILED delete keeps the mismatch PENDING (Codex
				// round 133): clearing the marker here — while this
				// best-effort branch still returns mismatch and the
				// batch offers a canonical upload — let the surviving
				// proven-corrupt spelling pass the post-registration
				// compat probes as implicitly clean for the roll's
				// remainder. Record the fresh verdict instead so the
				// pending machinery presses the delete on every later
				// touch until it lands or the version proves valid.
				log.Printf("lfs adopt %s: remove mismatched legacy source: %v", key, err)
				fpctx, fpcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
				_, curFP, fperr := s.Blob.StatFingerprint(fpctx, legacySrc)
				fpcancel()
				// The marker's persistence is REQUIRED here, not
				// best-effort (Codex round 134): this branch still
				// returns mismatch — the batch offers the canonical
				// upload, and once it registers, the compat probes
				// trust marker ABSENCE as clean (round 132's rule). A
				// swallowed probe or record failure would leave the
				// surviving proven-corrupt spelling unmarked forever,
				// so either failure FAILS the request instead: no
				// upload is offered, the client retries, and the next
				// touch re-proves and re-presses. Only a vanished
				// spelling needs no marker — a peer's delete landed,
				// nothing survives for the probes to bless.
				switch {
				case errors.Is(fperr, blob.ErrNotFound):
					s.forgetLFSMismatch(ctx, repoID, legacySrc)
				case fperr != nil:
					return fmt.Errorf("fingerprint surviving mismatched legacy source %s: %w", legacySrc, fperr)
				default:
					rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
					rerr := s.Store.RecordLFSMismatch(rctx, repoID, legacySrc, curFP)
					rcancel()
					if rerr != nil {
						return fmt.Errorf("record mismatch marker for surviving legacy source %s: %w", legacySrc, rerr)
					}
				}
			} else {
				s.forgetLFSMismatch(ctx, repoID, legacySrc)
				log.Printf("lfs adopt %s: removed legacy blob failing content verification", key)
			}
		} else {
			log.Printf("lfs adopt %s: sparing legacy source not proven mismatched", key)
		}
		return errLFSContentMismatch
	}
	// Register the size of the bytes that were ACTUALLY downloaded and
	// hashed, never the caller's pre-verification Stat (Codex round 23):
	// between that Stat and this download, a concurrent replica can
	// guarded-delete a small corrupt legacy blob and publish the larger
	// correct object — verification then passes against the REPLACEMENT,
	// and registering the stale Stat size would under-charge pack_usage
	// (the later idempotent re-registration corrects nothing), minting
	// phantom headroom until the cached floor refreshes.
	// Adoption moves already-LISTED bytes into the counter (Codex rounds
	// 48+49): every replica's cached uncounted floor scanned before this
	// registration still contains them, so their next admission's
	// floor+counter sum would double-count the object — a false denial
	// preserved for the cache lifetime and any hook floors minted from
	// it. The registration therefore bumps the SHARED adoption
	// generation IN ITS OWN TRANSACTION (Codex round 51 — a separate
	// best-effort bump could time out after the registration committed
	// and never retry, because this function short-circuits once the row
	// exists); all replicas' floor serves and hook floors are
	// generation-checked. This replica's entry is dropped directly.
	// Bounded on its own window (the verify allowance above ended with
	// the hash): a quick transaction that must not inherit an unbounded
	// detached context.
	// pen is the DESTINATION verification's midstate (the loop's final
	// iteration always hashes the canonical bytes), so a
	// registration-failure retry — whose verify target is the canonical
	// key — resumes against exactly the fingerprint it parked.
	regCtx, regCancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
	defer regCancel()
	if err := s.Store.RegisterAdoptedLFSObject(regCtx, repoID, key, verifiedSize); err != nil {
		// The hash COMPLETED — only the idempotent registration failed
		// (Codex round 76). Park the lease with the hash's PENULTIMATE
		// midstate: the retry re-reads just the final chunk instead of
		// the whole object, so a transiently contended ref store cannot
		// amplify backend reads — yet it still re-checks the full digest
		// before registering (Codex round 77: a completed memo that
		// trusted size alone would register same-length forged bytes a
		// still-valid legacy presigned PUT swapped in mid-park). Nil for
		// single-chunk objects: their full re-read is one chunk, and the
		// released lease lets any replica pick that up.
		if pen != nil {
			pen.owner = claimOwner
			s.putLFSVerifyResume(key, *pen)
		}
		return err
	}
	s.invalidateUsageFloor(repoID)
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
	// Bounded like the PUT path's probe (Codex round 29): this recheck
	// runs while the caller holds the per-key slot and an op slot, and it
	// sits OUTSIDE regCtx (registration already committed under it), so a
	// wedged backend must hand the slots back within a bounded window
	// instead of pinning them for as long as the batch client stays
	// connected. A timeout fails closed: the registration is committed
	// and idempotent, so the client's retry lands on the adopted path.
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	defer rcancel()
	if _, err := s.Blob.Stat(rctx, key); err != nil {
		return fmt.Errorf("adopted object vanished (racing sweep?): %w", err)
	}
	// The legacy source is deliberately RETAINED (Codex round 80): a
	// rowless legacy object can have live download capabilities — a
	// 15-minute presigned GET issued moments before this registration, a
	// proxy stream between its stat and read, and (during a rolling
	// upgrade) every not-yet-upgraded replica's reads — and an inline
	// delete here would break them mid-lifetime. The source is rowless,
	// so the ordinary age-thresholded orphan sweep (its threshold dwarfs
	// any URL lifetime) reclaims it under the DeleteLFS opt-in; until
	// then floors count it alongside the canonical bytes — conservative
	// over-statement, never a quota hole.
	//
	// Objects adopted from ROWLESS CANONICAL bytes have no legacy source
	// at all (a verified publication whose registration failed or timed
	// out, registered by this retry) — with the compat bridge active,
	// old replicas would 404 them despite the flag (Codex round 82).
	// Ensure the legacy spelling exists; failures PROPAGATE (Codex round
	// 83) so the client keeps retrying — the registration is committed
	// and idempotent, and the retry's registered fast path converges the
	// bridge through the same seam.
	if cerr := s.ensureLegacyCompat(ctx, repoID, oid, key); cerr != nil {
		return cerr
	}
	// Final liveness re-touch (issue #73, Codex round 13): the
	// registration above committed BEFORE the sweep-race probe and the
	// size-scaled bridge copy, either of which can outlast a short GC
	// grace — a scan can mark the still-unreferenced row meanwhile, and
	// once the adoption claim lapses a grace-expired mark retires it even
	// though this success promises the writer it may commit its pointer.
	// Restores answer downloads — no pointer-commit promise — and must
	// never extend an unreferenced row's life. touched=false means the
	// row already vanished: answer retryable so the retry re-registers
	// instead of promising a retired row.
	if restore {
		return nil
	}
	// Bounded + detached like regCtx/rctx above: adoption completes under
	// its own lease once started, so a canceled client must not abort the
	// touch after the registration committed.
	touchCtx, touchCancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
	defer touchCancel()
	if touched, terr := s.Store.TouchLFSObject(touchCtx, repoID, key); terr != nil {
		return terr
	} else if !touched {
		return errLFSBusy
	}
	return nil
}

// verifyStoredLFSObject streams the blob at key through sha256 in
// lfsStagingChunk-sized ranged reads and checks the digest against oid,
// returning the verified byte count — the ONLY size the caller may
// register (Codex round 23): the stored object can be replaced between the
// caller's earlier Stat and this verification, so no earlier measurement
// may describe the bytes verified here. Streaming replaced the
// download-to-temp-disk approach (Codex round 24): a Stat-sized staging
// reservation could not bound what DownloadFile actually staged when a
// concurrent replica swapped a small legacy blob for a larger valid
// object. Ranged reads bound memory at one chunk and touch NO disk, so
// verification needs no staging-byte reservation at all (callers still
// hold the per-key slot and an op slot). A blob replaced mid-stream hashes
// as a mismatch (a prefix of different content) — the same safe path as
// any corrupt legacy blob; the digest covering exactly size bytes is what
// makes the returned count trustworthy.
// On success it also returns the hash's PENULTIMATE midstate — captured
// just before the final chunk — so a failed registration can park it
// (Codex rounds 76+77): the retry then re-reads only the final chunk yet
// still re-checks the full digest over exactly size bytes before
// registering. Nil for single-chunk objects, whose full re-read is one
// chunk anyway.
// It also returns the content-version fingerprint the digest verdict was
// proven against (Codex round 80) — on success the caller's canonical
// publish sandwiches the source with it; on errLFSContentMismatch the
// caller's cleanup may delete the corrupt bytes ONLY while that exact
// content is still stored.
//
// targetKey is the blob actually read (the canonical bytes when they
// exist, else the legacy source); key is the object's canonical identity,
// which keys the resume cache and the fleet claim — entries carry the
// TARGET's size and fingerprint, so a retry whose target flipped (the
// prior attempt published canonically) discards them and re-verifies.
func (s *Server) verifyStoredLFSObject(ctx context.Context, repo, key, targetKey, oid, claimOwner string) (size int64, pen *lfsVerifyResume, contentFP string, err error) {
	sctx, scancel := context.WithTimeout(ctx, lfsProbeTimeout)
	size, fp, err := s.Blob.StatFingerprint(sctx, targetKey)
	scancel()
	if err != nil {
		return 0, nil, "", fmt.Errorf("stat for verification: %w", err)
	}
	// The hash is bounded by its own progress, never the reservation TTL
	// (Codex security round 56): a TTL-derived cap makes objects slower
	// to read than the cap permanently unadoptable, and every failed
	// attempt replays the FULL ranged read — a tiny disconnecting batch
	// per replay drains the repo's verification share and shared backend
	// bandwidth at no client cost. Two bounds replace it: a size-scaled
	// absolute allowance (blob.EnsurePublished's repair-allowance shape —
	// a worst-case ~1 MiB/s floor far below any realistic server→store
	// link), so any live backend finishes and the committed row makes
	// the work once-per-object; and a per-chunk stall guard below, so a
	// WEDGED backend frees the op slot and verify buffer within one
	// chunk window instead of the whole allowance.
	vctx, vcancel := context.WithTimeout(ctx, 5*time.Minute+time.Duration(size>>20)*time.Second)
	defer vcancel()
	// The ranged reads below buffer up to one chunk in MEMORY, so the
	// bound must be memory-sized (Codex round 26, security, refining
	// round 25): the op slots alone admit 256 concurrent 8 MiB buffers
	// (~2 GiB of heap), and charging the DISK-sized staging budget — 10
	// GiB by default — still admits nearly all of them. The slot pool
	// caps concurrent verification buffers at lfsMaxVerifyBuffers ×
	// lfsStagingChunk (~256 MiB) and charges each slot to the
	// repository's share (Codex round 33: one repository's upgrade
	// traffic could otherwise occupy the whole pool and 429 other
	// repositories' adoptions). Excess adoptions deny transiently like
	// every budget denial (the client retries, the object is unchanged).
	if !s.reserveLFSVerifyBuffer(repo) {
		return 0, nil, "", errLFSBusy
	}
	defer s.releaseLFSVerifyBuffer(repo)
	// Resume rather than replay (Codex round 57): an abort below — chunk
	// stall, allowance expiry, transient read error — released the
	// per-key slot, so the next write-scoped touch restarted the whole
	// ranged read from offset zero: chronic per-range slowness (a
	// congested backend needing over a chunk window per range) could
	// replay completed ranges forever, never adopting and amplifying the
	// very congestion that aborted it. Aborts now save the offset and
	// sha256 midstate; the next attempt continues after the completed
	// ranges, so total backend reads are bounded by ~one object plus one
	// lost chunk per abort — monotone progress. The final digest check
	// stays the correctness authority: resume state is only ever an
	// accelerant (single-use, evicted on use) and is BOUND to the blob's
	// size AND content fingerprint (Codex round 77 hardening the round-57
	// "at worst the mismatch path" rationale, which was adversarially
	// wrong: a same-size replacement sharing the true content's tail
	// splices a stale midstate into a FALSE MATCH) — any replacement
	// discards the entry and the hash restarts over the current bytes.
	h := sha256.New()
	var off int64
	if r, ok := s.takeLFSVerifyResume(key, size, fp); ok {
		if u, uok := h.(encoding.BinaryUnmarshaler); uok && u.UnmarshalBinary(r.state) == nil {
			off = r.off
		} else {
			h = sha256.New() // defensive: never hash on a half-restored state
		}
	}
	lastRenew := time.Now()
	for off < size {
		// The fleet-wide claim must stay live for the hash's whole
		// size-scaled lifetime (Codex round 74): renew its TTL clock on
		// a cadence well under ref.AdoptionClaimTTL. A lost lease
		// (expired and taken over, or a renewal the store rejects)
		// aborts the hash — continuing would waste the very bandwidth
		// the claim exists to protect, and the progress is saved for
		// this replica's next winning attempt.
		if claimOwner != "" && time.Since(lastRenew) >= lfsAdoptClaimRenewInterval {
			// Each renewal gets its own short deadline (Codex round 75):
			// inheriting vctx — hours away for a large object — would let
			// one wedged ref-store call pin the op slot and verify buffer
			// for the whole allowance while the claim expires under it.
			rctx, rcancel := context.WithTimeout(vctx, lfsClaimRenewTimeout)
			renewed, rerr := s.Store.RenewAdoptionClaim(rctx, key, claimOwner)
			rcancel()
			if rerr != nil {
				// Indeterminate: the lease may well still be live (only
				// this renewal call failed), so PARK it with the progress
				// — no other replica restarts from zero, and this
				// replica's next attempt renews the token and resumes.
				if off > 0 {
					s.saveLFSVerifyResume(key, size, off, fp, h, claimOwner)
				}
				return 0, nil, "", fmt.Errorf("renew adoption claim: %w", rerr)
			}
			if !renewed {
				// Lease definitively lost (expired and taken over):
				// another replica owns the hash now. Keep the midstate
				// local-only — parking a dead token would skip the
				// release of a lease this process no longer holds.
				if off > 0 {
					s.saveLFSVerifyResume(key, size, off, fp, h, "")
				}
				return 0, nil, "", errLFSBusy
			}
			lastRenew = time.Now()
		}
		n := min(lfsStagingChunk, size-off)
		if off > 0 && off+n == size {
			// Capture the midstate BEFORE the final chunk (Codex round
			// 77): a failed registration parks THIS, never a completed
			// "trust the size" memo — the retry re-reads one chunk and
			// re-checks the full digest. Like every resume entry it is
			// fingerprint-bound, so bytes a still-valid legacy presigned
			// PUT swapped in (same length, v0.1.0 rolling upgrade) while
			// the park waited discard it and re-verify in full.
			if m, mok := h.(encoding.BinaryMarshaler); mok {
				if st, serr := m.MarshalBinary(); serr == nil {
					pen = &lfsVerifyResume{size: size, off: off, state: st, fp: fp}
				}
			}
		}
		cctx, ccancel := context.WithTimeout(vctx, lfsVerifyChunkTimeout)
		chunk, err := s.Blob.ReadRange(cctx, targetKey, off, n)
		ccancel()
		if err != nil {
			// The lease is still this process's: park it with the
			// progress (Codex round 75) so the retry resumes here.
			if off > 0 {
				s.saveLFSVerifyResume(key, size, off, fp, h, claimOwner)
			}
			return 0, nil, "", fmt.Errorf("read for verification [%d,%d): %w", off, off+n, err)
		}
		h.Write(chunk)
		off += n
	}
	// NEITHER verdict below is trustworthy if the object was REPLACED
	// while we hashed (Codex round 77): ranged reads spanning a swap can
	// splice old-prefix+new-suffix into a digest that MATCHES oid even
	// though the stored bytes differ — a still-valid legacy presigned PUT
	// (v0.1.0 rolling upgrade) can write same-length forged bytes sharing
	// the true content's tail — and a spliced MISMATCH would
	// guarded-delete bytes this hash never covered. A moved fingerprint
	// therefore fails transiently: the retry hashes the current bytes,
	// with any parked resume discarded by its own fingerprint binding.
	ectx, ecancel := context.WithTimeout(ctx, lfsProbeTimeout)
	_, endFP, err := s.Blob.StatFingerprint(ectx, targetKey)
	ecancel()
	if err != nil {
		return 0, nil, "", fmt.Errorf("fingerprint after verification: %w", err)
	}
	if endFP != fp {
		return 0, nil, "", fmt.Errorf("object %s replaced during verification: %w", targetKey, errLFSBusy)
	}
	if hex.EncodeToString(h.Sum(nil)) != oid {
		return 0, nil, fp, errLFSContentMismatch
	}
	return size, pen, fp, nil
}

// lfsRepairCoverSuffix — see ref.RepairCoverSuffix (shared with the pack
// restore paths since Codex round 58).
const lfsRepairCoverSuffix = ref.RepairCoverSuffix

// lfsAdoptClaimRenewInterval is how often the hashing winner renews its
// fleet-wide adoption claim (Codex round 74): the size-scaled hash of a
// large legacy object legitimately outlives any fixed lease, so the
// verify loop restarts the ref.AdoptionClaimTTL clock on this cadence — a
// small fraction of the TTL, so scheduler noise and slow chunks never let
// a LIVE hash's lease expire under it.
var lfsAdoptClaimRenewInterval = 5 * time.Minute

// lfsClaimRenewTimeout bounds ONE adoption-claim renewal round-trip (Codex
// round 75): renewals run inside the size-scaled verification window,
// whose deadline can be hours away for a large object — inheriting it
// would let a single wedged ref-store call pin the handler's op slot and
// verify buffer for the whole allowance while the claim expires
// underneath. A renewal that cannot complete within this window aborts
// the attempt with the progress saved.
var lfsClaimRenewTimeout = 15 * time.Second

// lfsVerifyResume is a partially completed adoption hash: the verified
// prefix length and the sha256 midstate over it (Codex round 57).
type lfsVerifyResume struct {
	size  int64
	off   int64
	state []byte
	// fp is the blob's content-version fingerprint when the hashed
	// prefix was read (Codex round 77): a resume is only valid against
	// the SAME stored bytes — a midstate over replaced bytes could
	// splice old-prefix+new-suffix into a false digest match (or a
	// false mismatch that guarded-deletes unverified bytes). take
	// discards entries whose fingerprint moved; the retry re-hashes the
	// current bytes in full.
	fp string
	// owner, when non-empty, is the fleet adoption-claim lease PARKED
	// with this progress (Codex round 75): the abort deliberately kept
	// the claim held, because the midstate lives only in this process —
	// releasing would route the client's next retry to whichever replica
	// wins the fresh claim, restarting the hash from offset zero and
	// discarding the completed ranges. The next attempt on THIS replica
	// renews the token and resumes; a parked lease nobody retries
	// expires via ref.AdoptionClaimTTL exactly like a crashed winner's.
	owner string
}

// lfsMaxVerifyResumes bounds the resume cache (eviction beyond it, see
// saveLFSVerifyResume). Entries are a few hundred bytes; the cap only
// exists so unbounded distinct aborting keys cannot grow replica memory.
var lfsMaxVerifyResumes = 128

// saveLFSVerifyResume records a verification's progress for the key so
// the next attempt can continue after the completed ranges. A non-empty
// owner parks the fleet adoption-claim lease with the progress (Codex
// round 75) — pass "" when the lease was definitively lost or never held.
func (s *Server) saveLFSVerifyResume(key string, size, off int64, fp string, h hash.Hash, owner string) {
	assertf(off > 0 && off < size, "saveLFSVerifyResume(%q): off %d, size %d", key, off, size)
	m, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return
	}
	state, err := m.MarshalBinary()
	if err != nil {
		return
	}
	s.putLFSVerifyResume(key, lfsVerifyResume{size: size, off: off, state: state, fp: fp, owner: owner})
}

// putLFSVerifyResume inserts one resume entry, evicting beyond the cache
// bound. Eviction prefers entries with NO parked lease, and NEVER releases
// a parked victim's lease (Codex round 76): releasing let the evicted
// key's next retry win a fresh claim on any replica and restart from
// offset zero immediately — cycling more failure-prone keys than the cap
// turned every eviction into a full re-read. The orphaned lease instead
// keeps denying every replica READ-FREE until ref.AdoptionClaimTTL — crash
// semantics — so a key restarts from zero at most once per TTL window
// regardless of cache pressure.
func (s *Server) putLFSVerifyResume(key string, r lfsVerifyResume) {
	s.lfsVerifyResumeMu.Lock()
	defer s.lfsVerifyResumeMu.Unlock()
	if s.lfsVerifyResumes == nil {
		s.lfsVerifyResumes = make(map[string]lfsVerifyResume)
	}
	if _, exists := s.lfsVerifyResumes[key]; !exists && len(s.lfsVerifyResumes) >= lfsMaxVerifyResumes {
		victim := ""
		for k, v := range s.lfsVerifyResumes {
			victim = k
			if v.owner == "" {
				break
			}
		}
		delete(s.lfsVerifyResumes, victim)
	}
	s.lfsVerifyResumes[key] = r
}

// dropLFSVerifyResume discards the key's saved verification state, if any,
// releasing a lease parked with it — used once the key is registered and
// the memo can never be consumed (Codex round 76: an indeterminate
// registration timeout can commit AFTER parking the lease for a retry that
// then early-outs on the registered row).
func (s *Server) dropLFSVerifyResume(key string) {
	s.lfsVerifyResumeMu.Lock()
	r, ok := s.lfsVerifyResumes[key]
	delete(s.lfsVerifyResumes, key)
	s.lfsVerifyResumeMu.Unlock()
	if ok && r.owner != "" {
		s.releaseAdoptionClaim(key, r.owner)
	}
}

// takeParkedLFSClaimOwner consumes the lease owner parked with the key's
// saved progress, if any, leaving the midstate in place for the
// verification that is about to run under the renewed lease.
func (s *Server) takeParkedLFSClaimOwner(key string) string {
	s.lfsVerifyResumeMu.Lock()
	defer s.lfsVerifyResumeMu.Unlock()
	r, ok := s.lfsVerifyResumes[key]
	if !ok || r.owner == "" {
		return ""
	}
	owner := r.owner
	r.owner = ""
	s.lfsVerifyResumes[key] = r
	return owner
}

// parkedLFSClaimOwner reads the key's parked lease owner without
// consuming it.
func (s *Server) parkedLFSClaimOwner(key string) string {
	s.lfsVerifyResumeMu.Lock()
	defer s.lfsVerifyResumeMu.Unlock()
	return s.lfsVerifyResumes[key].owner
}

// parkLFSClaimOwner re-attaches a lease owner to the key's saved progress
// (used when the renewal that consumed it failed indeterminately).
// Reports whether the entry still existed; false means a concurrent
// eviction discarded the midstate and the caller should free the lease
// instead of orphaning the token.
func (s *Server) parkLFSClaimOwner(key, owner string) bool {
	assertf(owner != "", "parkLFSClaimOwner(%q): empty owner", key)
	s.lfsVerifyResumeMu.Lock()
	defer s.lfsVerifyResumeMu.Unlock()
	r, ok := s.lfsVerifyResumes[key]
	if !ok {
		return false
	}
	r.owner = owner
	s.lfsVerifyResumes[key] = r
	return true
}

// publishVerifiedLFSObject copies digest-verified legacy bytes to the
// canonical key. The copy itself proves NOTHING about what it carried
// (Codex round 81: Blob.Copy binds no source version, and content-derived
// ETags make before/after source fingerprints ABA-blind) — the caller's
// loop therefore RE-VERIFIES the destination before registering, which is
// the only digest proof registration trusts. This write stays fenced by
// the fleet lease (renewed immediately before the copy) so a stale worker
// can never overwrite a successor's canonical bytes; racing a verified
// publisher at worst overwrites content the destination re-hash then
// re-proves or condemns.
func (s *Server) publishVerifiedLFSObject(ctx context.Context, repoID, key, legacyKey string, size int64, claimOwner string) error {
	assertf(claimOwner != "", "publishVerifiedLFSObject(%q)", key)
	doCopy := func(opCtx context.Context) error {
		// Lease fence: renew immediately before the copy, then keep the
		// lease renewed on the adoption cadence for the copy's WHOLE
		// size-scaled life (Codex security round 83 — the previous
		// clamp to ref.AdoptionClaimTTL−5m guaranteed cancellation of
		// every sequential multipart copy slower than 25 minutes,
		// aborting all completed parts on each retry: a repeatable
		// full-object cost that never converges for large objects).
		// Only a live lease holder ever writes the canonical key on the
		// adoption side; a definitive renewal loss cancels the copy, so
		// a stale worker still cannot overwrite a successor's verified
		// or registered canonical bytes.
		rctx, rcancel := context.WithTimeout(opCtx, lfsClaimRenewTimeout)
		renewed, rerr := s.Store.RenewAdoptionClaim(rctx, key, claimOwner)
		rcancel()
		if rerr != nil {
			return fmt.Errorf("renew claim before canonical copy: %w", rerr)
		}
		if !renewed {
			return errLFSBusy // lease lost: the successor owns the copy now
		}
		return s.withRenewedAdoptionClaim(opCtx, key, claimOwner, func(copyCtx context.Context) error {
			bound := 5*time.Minute + time.Duration(size>>20)*time.Second
			cctx, ccancel := context.WithTimeout(copyCtx, bound)
			defer ccancel()
			if err := s.Blob.Copy(cctx, key, legacyKey); err != nil {
				return fmt.Errorf("canonicalize %s: %w", legacyKey, err)
			}
			return nil
		})
	}
	// Detached from the requester like the verification itself (Codex
	// round 31's argument applies to the copy equally): a client
	// canceling mid-copy would otherwise replay multipart copies of a
	// large object on every retry. The copy stays bounded above and, when
	// covered below, cancels on cover-continuity loss.
	dctx := context.WithoutCancel(ctx)
	if size <= 0 || s.PackQuotaBytes <= 0 {
		// No floors to protect (quota disabled) or nothing to account.
		return doCopy(dctx)
	}
	// REAL quota admission for the canonical copy (Codex round 85, the
	// adoption twin of the bridge's round-84 admission): the copy
	// materializes a second size-byte object while the legacy source
	// stays retained — and floor-charged — until the post-roll sweep, so
	// admitting it under an unbounded quota let an at-quota repository
	// durably reach 2S. A denial fails the adoption with 507 — freeing
	// space or finishing the roll (and reclaiming sources) lets a retry
	// migrate the object.
	// ONE reservation both admits and covers (Codex round 94): a separate
	// admission row beside the renewed cover double-charged the same
	// planned bytes for the copy's whole — potentially hours-long —
	// window, denying unrelated uploads the post-copy state would admit.
	// The cover's FIRST reservation performs the real admission (floor +
	// generation recheck, real PackQuotaBytes) on the SUFFIXED key;
	// renewals extend it unbounded, since the planned bytes must never be
	// quota-denied mid-copy. Registration cannot settle the suffixed row,
	// so it lingers to its TTL exactly like every restore cover — the
	// deliberate stale-floor tail, unchanged from the two-row design.
	// renew invocations are strictly serialized by WithRenewedCover, so
	// the plain flag needs no synchronization.
	admitted := false
	// Stale-floor cover (the round-57/58 pattern): the copy creates NEW
	// physical bytes invisible both to floors scanned before it and to
	// the usage counter until registration commits — an admission holding
	// such a floor would under-count by the copy's size for the whole
	// copy window. A ONE-SHOT cover is not enough (Codex round 79): the
	// reservation TTL may be as short as ~61s while a large multipart
	// copy legitimately runs for many minutes, so the cover is renewed
	// through the copy and refreshed for the stale-floor tail by
	// blob.WithRenewedCover, exactly like the restore paths. Continuity
	// loss aborts the copy — uncovered bytes must never become physical.
	// No handoff: the registration that follows bumps the adoption
	// generation atomically, and a copy whose registration never happens
	// is re-covered by the next attempt (floors rescan both spellings
	// meanwhile — conservative).
	renewCover := func(rctx context.Context) error {
		if admitted {
			return ref.ReserveRestoreCover(rctx, s.Store, blob.QuotaRepoGroup(repoID), key, size, s.lfsReservationTTL())
		}
		for attempt := 0; ; attempt++ {
			floor, floorGen, err := s.cachedUsageFloorGen(rctx, repoID)
			if err != nil {
				return fmt.Errorf("adoption admission %s: usage listing: %w", key, err)
			}
			rsvCtx, rsvCancel := context.WithTimeout(rctx, UsageFloorHandoffMargin)
			dec, err := s.Store.ReservePackBytes(rsvCtx, blob.QuotaRepoGroup(repoID),
				[]ref.Pack{{Key: key + lfsRepairCoverSuffix, Size: size}}, ref.NewReservationOwner(),
				floor, floorGen, s.PackQuotaBytes, s.lfsReservationTTL())
			rsvCancel()
			if err != nil {
				return fmt.Errorf("adoption admission %s: %w", key, err)
			}
			if dec.FloorStale {
				s.invalidateUsageFloor(repoID)
				if attempt >= 1 {
					return fmt.Errorf("adoption admission %s: usage floor went stale twice", key)
				}
				continue
			}
			if dec.KeyCapExceeded {
				return fmt.Errorf("adoption admission %s: %w", key, errLFSBusy)
			}
			if !dec.OK {
				return fmt.Errorf("adopt %s: %w (copy adds %d bytes to %d stored, +%d reserved, quota %d)",
					key, errLFSAdoptQuotaExceeded, size, dec.UsedBytes, dec.ReservedBytes, s.PackQuotaBytes)
			}
			admitted = true
			return nil
		}
	}
	copyErr, renewErr := blob.WithRenewedCover(dctx, renewCover, doCopy, nil)
	if copyErr != nil {
		// Ambiguous-commit probe (Codex round 96, the adoption twin of
		// round 95's bridge fix): the copy can commit its destination and
		// still return an error — returning here skipped the cover tail,
		// leaving the durable ROWLESS canonical bytes invisible to a
		// pre-copy cached floor for its whole serve-and-handoff window
		// (registration's atomic bump only heals after some later retry
		// re-verifies and registers). A present destination is a commit —
		// but only under a still-held lease may this worker CONTINUE into
		// the re-verify and registration (rounds 88/93: success must
		// never stand on a lost lease; a renewal failure propagates the
		// copy error and leaves the key to the claim's successor).
		actx, acancel := context.WithTimeout(dctx, lfsProbeTimeout)
		_, aerr := s.Blob.Stat(actx, key)
		acancel()
		if aerr != nil {
			return fmt.Errorf("adoption publish %s: %w", key, copyErr)
		}
		lctx, lcancel := context.WithTimeout(dctx, lfsClaimRenewTimeout)
		renewed, lerr := s.Store.RenewAdoptionClaim(lctx, key, claimOwner)
		lcancel()
		if lerr != nil || !renewed {
			if errors.Is(copyErr, blob.ErrCoverContinuityLost) {
				// Cover AND claim lost around a committed copy (Codex
				// round 113, P1): accounting alone is not recovery — a
				// gap admission may already have settled, and once the
				// expired claim leaves the rowless bytes unowned, any
				// later toucher registers them WITHOUT re-admission via
				// the crashed-adoption path. Press to REACQUIRE the
				// claim through the floor window and prove deletion or
				// registration under it; only an unproven window falls
				// back to accounting (a LIVE successor holding the claim
				// resolves the key itself: registration counts the
				// bytes, and its own cover-loss handling deletes them).
				if !s.reclaimUncoveredCommit(dctx, repoID, key) {
					s.ensureAmbiguousCommitAccounting(dctx, repoID, key, size, nil)
				}
			}
			return fmt.Errorf("adoption publish %s: %w", key, copyErr)
		}
		if errors.Is(copyErr, blob.ErrCoverContinuityLost) {
			// A commit whose error IS the cover cancellation is round
			// 105's unsafe commit wearing this branch's clothes (Codex
			// security round 107): WithRenewedCover reports a canceled op
			// through copyErr — renewErr stays nil — so accepting here
			// would register bytes a stale-floor admission may already
			// have spent during the uncovered gap. Same verdict, same
			// remedy, under the just-renewed claim: deletion pressed
			// until proven, accounting only as the unproven-past-window
			// fallback (the round-108 shared seam — see the renewErr twin
			// below).
			if proven, _ := s.uncoveredCleanupProven(dctx, repoID, key); !proven {
				s.ensureAmbiguousCommitAccounting(dctx, repoID, key, size, nil)
			}
			return fmt.Errorf("adoption publish %s: %w", key, copyErr)
		}
		// The cover must LAND (or the stale-floor window must pass)
		// before the potentially long re-hash continues (Codex round 97):
		// a swallowed one-shot failure left the rowless canonical bytes
		// floor-invisible until registration's bump, which some pre-copy
		// floor's admission could beat. No bump here — the registration
		// that follows performs the handoff atomically.
		s.ensureAmbiguousCommitAccounting(dctx, repoID, key, size, nil)
		log.Printf("lfs adopt %s: destination committed despite copy error: %v", key, copyErr)
		return nil // the caller's destination re-verify digest-proves the bytes
	}
	if renewErr != nil {
		// An uncovered-gap commit is UNSAFE to accept (Codex round 105,
		// P1): continuity was lost while — or right after — the copy
		// committed, so a stale-floor admission may already have consumed
		// headroom that never accounted these bytes; registering them
		// would durably exceed the quota. Remove the rowless destination
		// under the held claim (GuardedLFSDelete spares it if a
		// registration somehow landed) and FAIL — the retry re-admits and
		// re-copies from scratch. A failed cleanup leaves only the
		// crashed-publisher orphan shape the next fresh floor lists.
		// Deletion — not accounting — is the recovery that REVOKES the
		// hazard (Codex round 108, P1): a stale-floor admission that
		// exploited the uncovered gap may already have settled, and no
		// new cover or generation bump un-admits it; only removing the
		// bytes restores the quota invariant. Keep retrying the guarded
		// delete until the outcome is PROVEN — deleted, already absent,
		// or spared by a landed registration whose atomic credit counts
		// the bytes — across the whole stale-floor window.
		if proven, _ := s.uncoveredCleanupProven(dctx, repoID, key); !proven {
			// Terminal fallback (Codex round 107): the whole stale-floor
			// window passed without a proven deletion — the rowless bytes
			// may remain, and the crashed-adoption retry registers them
			// WITHOUT any copy admission. Cover them and invalidate
			// floors so at least no FURTHER admission can omit them; the
			// gap-exploiting admission that already settled is beyond
			// recall without the deletion above.
			s.ensureAmbiguousCommitAccounting(dctx, repoID, key, size, func(bctx context.Context) error {
				return s.Store.BumpLFSAdoptionGen(bctx, BaseRepo(repoID))
			})
			s.invalidateUsageFloor(repoID)
		}
		return fmt.Errorf("adoption publish %s: cover continuity lost: %w", key, renewErr)
	}
	return nil
}

// uncoveredCleanupProven presses the guarded delete of an uncovered-gap
// commit until its outcome is PROVEN — deleted, already absent, or spared
// by a landed registration whose atomic credit counts the bytes — across
// the whole stale-floor window (Codex round 108, P1): deletion, not
// accounting, is the only recovery that REVOKES the hazard, because no
// cover or generation bump un-admits a stale-floor admission that already
// exploited the gap. One shared seam for all four uncovered-commit sites
// (adoption/compat × copyErr/renewErr) — the verdict is identical, so the
// remedy must be too. proven=false means the window closed with no proven
// outcome; callers fall back to accounting recovery (visibility is the
// only repair left, and past the window fresh floors list the bytes).
func (s *Server) uncoveredCleanupProven(ctx context.Context, repoID, key string) (proven, deleted bool) {
	del := func(gctx context.Context) error {
		if err := s.Blob.Delete(gctx, key); err != nil && !errors.Is(err, blob.ErrNotFound) {
			return err
		}
		return nil
	}
	deadline := time.Now().Add(usageCacheTTL + UsageFloorHandoffMargin)
	for {
		rctx, rcancel := context.WithTimeout(ctx, lfsCleanupTimeout)
		d, derr := s.Store.GuardedLFSDelete(rctx, repoID, key, del)
		rcancel()
		if derr == nil {
			return true, d
		}
		log.Printf("lfs uncovered-copy cleanup %s: %v", key, derr)
		if time.Now().After(deadline) {
			return false, false
		}
		select {
		case <-ctx.Done():
		case <-time.After(ambiguousAccountingRetryInterval):
		}
		if ctx.Err() != nil {
			return false, false
		}
	}
}

// reclaimUncoveredCommit handles the round-113 double loss — cover AND
// claim gone around a committed canonical copy: it presses to REACQUIRE
// the fleet claim through the pre-copy floor window and, once held, proves
// the cleanup verdict under it (mutual exclusion holds again — no
// successor's publish/verify/register region can be live while we own the
// claim). A denial means a LIVE holder owns the key; keep pressing — its
// release (or expiry) lets a later iteration win. A registration observed
// while waiting is NOT proof (Codex round 114, P1): the successor reached
// these rowless bytes through the crashed-adoption path, which skips copy
// admission as zero-growth, so its registration only CREDITS the bytes —
// it cannot revoke a writer already admitted against a stale pre-copy
// floor during the uncovered gap. It is terminal, though: registered
// bytes are live objects no cleanup may delete, so the caller's
// accounting fallback is all that remains (over-covering an already
// credited object is the safe side and expires by TTL), and the residual
// — a successor registering before the claim could be won — is a
// one-time, size-bounded, fully COUNTED overshoot that future admissions
// see and deny against, not an invisible leak. Returns false whenever
// deletion was not proven.
func (s *Server) reclaimUncoveredCommit(ctx context.Context, repoID, key string) bool {
	deadline := time.Now().Add(usageCacheTTL + UsageFloorHandoffMargin)
	for {
		cctx, ccancel := context.WithTimeout(ctx, lfsClaimRenewTimeout)
		claimed, owner, err := s.Store.TryAdoptionClaim(cctx, repoID, key)
		ccancel()
		switch {
		case err != nil:
			log.Printf("lfs adopt %s: reclaim uncovered commit: %v", key, err)
		case claimed:
			proven, deleted := s.uncoveredCleanupProven(ctx, repoID, key)
			rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsClaimRenewTimeout)
			if rerr := s.Store.ReleaseAdoptionClaim(rctx, key, owner); rerr != nil {
				log.Printf("lfs adopt %s: release reclaimed claim: %v", key, rerr)
			}
			rcancel()
			// A DECLINED delete here is a successor's registration that
			// landed before this reclaim won the released claim (Codex
			// security round 125) — reached through the crashed-adoption
			// path, zero-growth, NO copy admission: exactly the round-114
			// verdict arriving through the claimed arm instead of the
			// waiting arm. Terminal for cleanup, but UNPROVEN: only a
			// deletion revokes the gap; the caller's accounting fallback
			// must still run.
			return proven && deleted
		default:
			hctx, hcancel := context.WithTimeout(ctx, lfsProbeTimeout)
			registered, herr := s.Store.HasLFSObject(hctx, repoID, key)
			hcancel()
			if herr == nil && registered {
				log.Printf("lfs adopt %s: successor registered the uncovered commit before the claim could be reclaimed — bytes credited, gap admission unrevoked", key)
				return false
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(ambiguousAccountingRetryInterval):
		}
	}
}

// rememberLFSMismatch persists that the version of legacyKey carrying fp
// failed content verification (Codex security round 118; FLEET-persistent
// through the ref store since round 132 — the process-local map was
// per-replica and evictable, and a lost marker made a registered object's
// corrupt legacy spelling implicitly clean forever). The write failure
// PROPAGATES (Codex security round 134): the source-mismatch arms below
// the mint return the mismatch verdict trusting exactly this marker to
// press the surviving corrupt spelling after the verdict's upload
// registers — a swallowed failure left the survivor unmarked, and the
// compat probes trust marker absence as clean (round 132's rule).
// Callers must fail the touch instead; the retry re-proves and re-mints.
func (s *Server) rememberLFSMismatch(ctx context.Context, repoID, legacyKey, fp string) error {
	assertf(fp != "", "rememberLFSMismatch(%s, %s): empty fingerprint", repoID, legacyKey)
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	defer rcancel()
	if err := s.Store.RecordLFSMismatch(rctx, repoID, legacyKey, fp); err != nil {
		return fmt.Errorf("record mismatch marker %s: %w", legacyKey, err)
	}
	return nil
}

// lfsMismatchMemo returns the persisted failed-verification fingerprint
// for legacyKey, if any. Errors propagate: a marker that cannot be read
// must never be treated as absent (round 132's rule — absence implies
// clean on the registered paths).
func (s *Server) lfsMismatchMemo(ctx context.Context, repoID, legacyKey string) (string, bool, error) {
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	defer rcancel()
	return s.Store.LFSMismatchFingerprint(rctx, repoID, legacyKey)
}

// forgetLFSMismatch drops legacyKey's marker after a decisive verdict.
// Best-effort in the safe direction: a lingering marker is re-pressed —
// and cleared — by the next touch's decisive snapshot.
func (s *Server) forgetLFSMismatch(ctx context.Context, repoID, legacyKey string) {
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	defer rcancel()
	if err := s.Store.ClearLFSMismatch(rctx, repoID, legacyKey); err != nil {
		log.Printf("lfs mismatch marker %s: clear: %v", legacyKey, err)
	}
}

// legacySourceProvenMismatched re-hashes the legacy source at cleanup time
// (Codex round 108, P2): the destination's failed digest proves the bytes
// the copy caught were wrong, not that the source still holds them. Only a
// fresh digest over one SINGLE stored version is decisive — computed over
// an atomically downloaded local snapshot (round 115). mismatched is the
// deletion verdict; decisive separates a PROVEN-VALID version (memo
// clearing, security round 122) from unknowable state — errors report
// (false, false), sparing, and the next touch re-examines.
// Size-scale-bounded; runs only on the rare mismatch path, under the
// caller's held claim and op slot.
func (s *Server) legacySourceProvenMismatched(ctx context.Context, repoID, legacyKey, oid string) (mismatched, decisive bool) {
	// SINGLE-VERSION snapshot by construction (Codex round 115): chunked
	// ranged reads can interleave with legacy PUTs, and content-derived
	// fingerprints are ABA-blind (A→B→A restores the fingerprint around
	// a mixed read), so no read-side stat pinning can prove the ranges
	// came from one version. One atomic DownloadFile is one GET: the
	// local snapshot is exactly one stored version's bytes, and the
	// digest verdict binds to that version. The residual — a replacement
	// between this proof and the caller's delete — is one RTT and
	// irreducible here: conditional DELETE If-Match is silently ignored
	// by MinIO (verified round 84) and the legacy namespace takes no
	// fleet claims by design (still-valid presigned PUTs, old replicas)
	// — ADR-41's documented legacy-mutability residual class.
	// The context is CALLER-MANAGED (Codex round 127): every caller
	// passes a region already detached from the request (round 116's
	// requirement — a parent lfsCleanupTimeout silently expired large
	// snapshots, permanently sparing corrupt bytes) yet still carrying
	// the claim-renewal region's lease-loss cancellation (round 126) —
	// an internal WithoutCancel here stripped that loss, letting a
	// multi-hour download run on while a successor redid the same work.
	base := ctx
	pctx, pcancel := context.WithTimeout(base, lfsProbeTimeout)
	size, perr := s.Blob.Stat(pctx, legacyKey)
	pcancel()
	if perr != nil {
		return false, false // absent or unknowable spares; the next touch re-examines
	}
	// The snapshot is real replica disk: charge it to the staging budget
	// like every proxy transfer (Codex round 116, P1) — bytes grown
	// through growLFSStaging so the PER-REPOSITORY byte-share fairness
	// applies (Codex security round 117), against the operation slot the
	// CALLER already holds (Codex round 120: a second slot made a full
	// cohort of concurrent mismatches livelock the op pool — every
	// waiter held its first slot while waiting for its second, so none
	// could progress until their cleanup contexts expired together).
	// Contention is PRESSED through the standard bounded window rather
	// than judged once (Codex round 118): a one-shot denial spared
	// corrupt legacy bytes that the imminent verified upload's
	// registration then locks in — round-89's exists-probe makes every
	// later compat repair skip the spelling. Persistent denial through
	// the window still spares: unknowable is never proof, and the
	// residual equals the background legacy-mutability exposure (a
	// still-valid presigned PUT can re-corrupt the spelling at any time
	// regardless).
	if size > 0 {
		deadline := time.Now().Add(usageCacheTTL + UsageFloorHandoffMargin)
		for !s.growLFSStaging(repoID, size) {
			if ctx.Err() != nil || time.Now().After(deadline) {
				return false, false
			}
			select {
			case <-ctx.Done():
				return false, false
			case <-time.After(ambiguousAccountingRetryInterval):
			}
		}
		defer s.shrinkLFSStaging(repoID, size)
	}
	dir, err := os.MkdirTemp("", "lfs-mismatch-*")
	if err != nil {
		return false, false
	}
	defer os.RemoveAll(dir)
	snap := filepath.Join(dir, "snap")
	// The size-scaled allowance every LFS transfer uses (round 81's
	// shape): a live backend finishes, a wedged one is cut loose. The
	// download is CAPPED at the reserved size (Codex round 117, P1): a
	// still-valid presigned PUT can replace the object with a larger
	// version between the Stat above and the GET, and an uncapped
	// stream would consume unreserved replica disk. A cap abort means
	// the version changed — unknowable, spared, re-examined next touch.
	dctx, dcancel := context.WithTimeout(base, 5*time.Minute+time.Duration(size>>20)*time.Second)
	derr := s.Blob.DownloadFileCapped(dctx, legacyKey, snap, size)
	dcancel()
	if derr != nil {
		return false, false // unknowable state spares; the next touch re-examines
	}
	f, ferr := os.Open(snap)
	if ferr != nil {
		return false, false
	}
	defer f.Close()
	h := sha256.New()
	if _, cerr := io.Copy(h, f); cerr != nil {
		return false, false
	}
	if hex.EncodeToString(h.Sum(nil)) == oid {
		return false, true // proven VALID: this version is the true object
	}
	return true, true
}

// withRenewedAdoptionClaim runs op while renewing the fleet claim for key
// on the adoption cadence (Codex security round 83): work that may
// legitimately outlast one ref.AdoptionClaimTTL — the size-scaled publish
// copy — must hold a LIVE lease for its whole duration, or a takeover
// mid-copy would let two workers write the canonical key concurrently. A
// renewal failure is treated as loss (indeterminate is not "held") and
// cancels op with the loss as its cause.
func (s *Server) withRenewedAdoptionClaim(ctx context.Context, key, owner string, op func(context.Context) error) error {
	opCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := s.keepAdoptionClaimRenewed(opCtx, cancel, key, owner)
	defer stop()
	err := op(opCtx)
	// JOIN the renewer before judging the lease (Codex round 88): stop
	// blocks until the renewal goroutine has exited, so any loss it
	// recorded is visible now and no later cancel can race the verdict.
	stop()
	// A recorded loss dominates even a SUCCESSFUL op (Codex round 88): a
	// long copy can commit and return nil concurrently with the takeover
	// cancellation, and the successor may overwrite the canonical key
	// between this worker's destination verification and registration —
	// success on a lost lease would register bytes nobody verified at
	// rest. Surface the loss cause rather than the bare context error.
	if cause := context.Cause(opCtx); cause != nil && !errors.Is(cause, context.Canceled) && opCtx.Err() != nil {
		return cause
	}
	return err
}

// keepAdoptionClaimRenewed renews the fleet claim for key on the adoption
// cadence until the returned stop function runs (or region ends), and
// cancels the region — with the loss as its cause — on any definitive loss
// or renewal failure: no canonical write may continue on a lease the
// process no longer provably holds. Renewal errors are treated as loss
// (indeterminate is not "held").
func (s *Server) keepAdoptionClaimRenewed(region context.Context, cancel context.CancelCauseFunc, key, owner string) (stop func()) {
	assertf(key != "" && owner != "", "keepAdoptionClaimRenewed(%q, %q)", key, owner)
	done := make(chan struct{})
	exited := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(exited)
		ticker := time.NewTicker(lfsAdoptClaimRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-region.Done():
				return
			case <-ticker.C:
			}
			rctx, rcancel := context.WithTimeout(region, lfsClaimRenewTimeout)
			renewed, err := s.Store.RenewAdoptionClaim(rctx, key, owner)
			rcancel()
			if err != nil {
				cancel(fmt.Errorf("renew claim for %s: %w", key, err))
				return
			}
			if !renewed {
				cancel(fmt.Errorf("claim for %s expired and was taken over: %w", key, errLFSBusy))
				return
			}
		}
	}()
	return func() {
		once.Do(func() { close(done) })
		// Stop JOINS (Codex round 88): callers judge the lease from
		// context.Cause(region) immediately after stopping, so an
		// un-joined renewer could record a loss a moment later and the
		// verdict would miss it. A renewal already in flight delays this
		// by at most lfsClaimRenewTimeout.
		<-exited
	}
}

// ambiguousAccountingRetryInterval paces the round-97 ambiguous-commit
// accounting convergence loop.
var ambiguousAccountingRetryInterval = 2 * time.Second

// ensureAmbiguousCommitAccounting converges the quota accounting for a
// copy that COMMITTED its destination while reporting an error (Codex
// round 97): the durable bytes need a live cover and — when the caller
// passes one — the floor-generation handoff, and a swallowed one-shot
// failure could never converge, because every later retry sees the
// destination present and skips the seam entirely. Retry both until they
// land, or until every usage floor scanned BEFORE the copy has provably
// aged out (usageCacheTTL + UsageFloorHandoffMargin): floors scanned
// after the commit list the durable bytes, so from then on the missing
// rows can hide nothing. The context is detached and never client-paced;
// a ref store down for the whole window also blocks every admission that
// could exploit the gap.
func (s *Server) ensureAmbiguousCommitAccounting(ctx context.Context, repoID, coverKey string, size int64, bump func(context.Context) error) {
	covered := size <= 0 || s.PackQuotaBytes <= 0
	bumped := bump == nil
	deadline := time.Now().Add(usageCacheTTL + UsageFloorHandoffMargin)
	for !covered || !bumped {
		if !covered {
			cctx, ccancel := context.WithTimeout(ctx, blob.CoverHandoffTimeout)
			err := ref.ReserveRestoreCover(cctx, s.Store, blob.QuotaRepoGroup(repoID), coverKey, size, s.lfsReservationTTL())
			ccancel()
			if err == nil {
				covered = true
			} else {
				log.Printf("lfs ambiguous-commit cover %s: %v", coverKey, err)
			}
		}
		if !bumped {
			bctx, bcancel := context.WithTimeout(ctx, blob.CoverHandoffTimeout)
			err := bump(bctx)
			bcancel()
			if err == nil {
				bumped = true
			} else {
				log.Printf("lfs ambiguous-commit handoff %s: %v", coverKey, err)
			}
		}
		if covered && bumped {
			return
		}
		if time.Now().After(deadline) {
			// Every pre-copy floor has aged out: fresh scans list the
			// durable bytes, so the accounting is safe without the rows.
			log.Printf("lfs ambiguous-commit accounting %s: outwaited the stale-floor window", coverKey)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(ambiguousAccountingRetryInterval):
		}
	}
}

// releaseAdoptionClaim best-effort frees a fleet adoption claim on a
// bounded context detached from any request; a failure leaves the row to
// ref.AdoptionClaimTTL like a crashed winner's.
func (s *Server) releaseAdoptionClaim(key, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), lfsCleanupTimeout)
	defer cancel()
	if err := s.Store.ReleaseAdoptionClaim(ctx, key, owner); err != nil {
		log.Printf("lfs adopt %s: release claim: %v", key, err)
	}
}

// takeLFSVerifyResume claims (and removes — single use, re-saved on the
// next abort) the key's saved progress. A size or FINGERPRINT change means
// the blob was replaced since the abort (Codex round 77 — same-length
// replacement is the adversarial case): the state is discarded and the
// hash restarts over the current bytes.
func (s *Server) takeLFSVerifyResume(key string, size int64, fp string) (lfsVerifyResume, bool) {
	s.lfsVerifyResumeMu.Lock()
	defer s.lfsVerifyResumeMu.Unlock()
	r, ok := s.lfsVerifyResumes[key]
	if !ok {
		return lfsVerifyResume{}, false
	}
	delete(s.lfsVerifyResumes, key)
	if r.size != size || r.fp != fp {
		return lfsVerifyResume{}, false
	}
	return r, true
}

// stagingFileWriter covers every byte with staging budget BEFORE it lands
// on disk, growing the reservation one lfsStagingChunk at a time as the
// body actually arrives (Codex security round 5 on #51): reserving the
// full declared Content-Length up front let a drip-fed PUT pin the whole
// node budget without transferring. The op slot and the initial chunk are
// taken by the handler; *reserved is shared with its deferred release so
// the final grown total is returned exactly once. A grow denial surfaces
// as errLFSBusy (transient 429): the budget genuinely filled with other
// uploads' real bytes while this body streamed.
type stagingFileWriter struct {
	s             *Server
	repo          string
	f             *os.File
	contentLength int64
	written       int64
	reserved      *int64
}

func (w *stagingFileWriter) Write(p []byte) (int, error) {
	if need := w.written + int64(len(p)) - *w.reserved; need > 0 {
		grow := max(need, w.s.lfsStagingQuantum())
		if maxGrow := w.contentLength - *w.reserved; grow > maxGrow {
			grow = maxGrow
		}
		// The server-enforced Content-Length cap bounds the body, so the
		// clamped chunk always covers the shortfall.
		assertf(grow >= need, "stagingFileWriter: grow %d < need %d (CL %d, reserved %d, written %d)",
			grow, need, w.contentLength, *w.reserved, w.written)
		if !w.s.growLFSStaging(w.repo, grow) {
			return 0, errLFSBusy
		}
		*w.reserved += grow
	}
	n, err := w.f.Write(p)
	w.written += int64(n)
	return n, err
}

// lfsDownloadAction builds the basic-transfer download action for key: a
// presigned blob URL when the backend supports it, else a proxy href back
// through this server (docs/explanation/git-protocol-and-namespaces.md local mode). The proxy href carries the
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
		s.lfsProxyDownload(w, r, target.repo.RepoID, oid)
		return
	}
	s.lfsProxyUpload(w, r, target, key, oid)
}

func (s *Server) lfsProxyDownload(w http.ResponseWriter, r *http.Request, repoID, oid string) {
	// Same serve rule as the batch (Codex security round 78): registered
	// → canonical only; rowless → legacy read-compat, never a
	// mid-adoption unverified canonical copy.
	key := blob.LFSKey(repoID, oid)
	registered, rerr := s.Store.HasLFSObject(r.Context(), repoID, key)
	if rerr != nil {
		log.Printf("lfs registry %s: %v", key, rerr)
		lfsError(w, http.StatusInternalServerError, "object registry unavailable")
		return
	}
	if !registered {
		key = blob.LFSLegacyKey(repoID, oid)
	}
	size, err := s.Blob.Stat(r.Context(), key)
	if errors.Is(err, blob.ErrNotFound) && !registered {
		// Check-then-use race with the object's first post-upgrade
		// adoption (Codex round 79) — same recheck as the batch path.
		if reg2, rerr2 := s.Store.HasLFSObject(r.Context(), repoID, blob.LFSKey(repoID, oid)); rerr2 == nil && reg2 {
			key = blob.LFSKey(repoID, oid)
			size, err = s.Blob.Stat(r.Context(), key)
		}
	}
	if errors.Is(err, blob.ErrNotFound) && registered {
		// Registered but blobless — recover from the retained legacy
		// copy through the verified adoption seam (Codex round 119),
		// same as the batch path: never a direct legacy serve.
		switch aerr := s.restoreLFSObjectLocked(r.Context(), repoID, key, oid); {
		case aerr == nil:
			size, err = s.Blob.Stat(r.Context(), key)
		case errors.Is(aerr, errLFSBusy):
			lfsError(w, http.StatusTooManyRequests, "object recovery in progress; retry")
			return
		default:
			log.Printf("lfs download restore %s: %v", key, aerr)
		}
	}
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
	// One in-flight operation per key, acquired before ANY per-key work
	// (Codex rounds — security 1 and 16): concurrent identical PUTs must
	// not multiply staging files, and concurrent adoption verifications
	// of one legacy key must not race each other's mismatch deletes.
	if _, busy := s.lfsUploadsInFlight.LoadOrStore(key, struct{}{}); busy {
		w.Header().Set("Retry-After", "10")
		lfsError(w, http.StatusTooManyRequests, "an operation for this object is already in progress")
		return
	}
	// The upload's node resources release EARLY on the compat-copy path
	// (Codex round 82): releaseUploadResources is idempotent and runs
	// before the size-scaled server-side copy, so slow copies hold one
	// goroutine, never the staging budget, op slot, per-key lock, or
	// scratch file.
	uploadResourcesReleased := false
	stagingReserved := int64(0)
	scratchDir := ""
	releaseUploadResources := func() {
		if uploadResourcesReleased {
			return
		}
		uploadResourcesReleased = true
		s.lfsUploadsInFlight.Delete(key)
		s.releaseLFSStaging(repoID, stagingReserved)
		if scratchDir != "" {
			_ = os.RemoveAll(scratchDir)
		}
	}
	// Node-wide operation slot BEFORE any blob-store work (Codex round
	// 25): the dedup probe below can block on a slow or wedged backend,
	// and handlers not yet holding a slot would accumulate goroutines and
	// connections past lfsMaxStagingOps — distinct-OID PUTs are free to
	// mint. The slot is taken with ZERO bytes; the byte budget is grown
	// below only once a body will actually stream, and stagingReserved
	// tracks the grown total for the single deferred release. (Adoption
	// briefly takes a second slot of its own — it must hold one on the
	// batch path too — which only makes the cap more conservative.)
	if !s.reserveLFSStaging(repoID, 0) {
		w.Header().Set("Retry-After", "10")
		lfsError(w, http.StatusTooManyRequests, "server LFS staging budget exhausted, retry")
		s.lfsUploadsInFlight.Delete(key)
		uploadResourcesReleased = true // nothing else was acquired
		return
	}
	defer releaseUploadResources()
	// Dedup before charging (the zero-growth rule): a re-PUT of a stored
	// object (crash-window retry, concurrent identical upload) adds
	// nothing — adopt it into the registry and succeed without reading
	// the body (the key is content-addressed, so stored bytes match). A
	// failed adoption fails the PUT: 200 would let the client commit a
	// pointer to a blob the sweeper still sees as an orphan. The probe is
	// deadline-bound (Codex round 25): a wedged backend must hand the op
	// slot back instead of pinning it until the client hangs up.
	pctx, pcancel := context.WithTimeout(ctx, lfsProbeTimeout)
	defer pcancel()
	// Probe both spellings (Codex security round 78): canonical bytes
	// (crashed adoption copy or registered object) and rowless legacy
	// bytes (pre-#51 upload) are each adoptable — adoption itself
	// copies/verifies/registers canonically and early-outs on a row. A
	// REGISTERED object whose canonical bytes are missing is repaired by
	// THIS verified body (the zero-growth path below), never by
	// re-adopting a legacy source: that spelling remains writable by
	// still-valid legacy presigned URLs, so the true bytes must come in
	// through the digest-verified proxy.
	_, probeErr := s.Blob.Stat(pctx, key)
	if errors.Is(probeErr, blob.ErrNotFound) {
		switch _, lerr := s.Blob.Stat(pctx, blob.LFSLegacyKey(repoID, oid)); {
		case lerr == nil:
			registered, rerr := s.Store.HasLFSObject(ctx, repoID, key)
			if rerr != nil {
				log.Printf("lfs registry %s: %v", key, rerr)
				lfsError(w, http.StatusInternalServerError, "object registry unavailable")
				return
			}
			if !registered {
				probeErr = nil // rowless legacy bytes: adopt them
			}
		case !errors.Is(lerr, blob.ErrNotFound):
			probeErr = lerr
		}
	}
	switch err := probeErr; {
	case err == nil:
		switch aerr := s.adoptLFSObject(ctx, repoID, key, oid); {
		case aerr == nil:
			w.WriteHeader(http.StatusOK)
			return
		case errors.Is(aerr, ref.ErrLFSObjectCapExceeded):
			lfsError(w, http.StatusInsufficientStorage, fmt.Sprintf(
				"repository LFS object count limit reached (limit %d)", ref.MaxLFSObjectsPerRepo))
			return
		case errors.Is(aerr, errLFSAdoptQuotaExceeded), errors.Is(aerr, errLFSCompatQuotaExceeded):
			// Both copy denials are quota verdicts (Codex round 103): the
			// registered fast path surfaces the BRIDGE's sentinel when
			// write compat repairs a missing legacy spelling, and the
			// generic 500 hid the documented free-space-or-finish-the-roll
			// remedy behind a retryable-looking failure.
			lfsError(w, http.StatusInsufficientStorage, aerr.Error())
			return
		case errors.Is(aerr, errLFSBusy):
			w.Header().Set("Retry-After", "10")
			lfsError(w, http.StatusTooManyRequests, "server LFS staging budget exhausted, retry")
			return
		case errors.Is(aerr, errLFSContentMismatch):
			// The mismatched legacy blob was removed: proceed as a normal
			// verified upload of the request body.
		default:
			log.Printf("lfs adopt %s: %v", key, aerr)
			lfsError(w, http.StatusInternalServerError, "object registration failed")
			return
		}
	case !errors.Is(err, blob.ErrNotFound):
		log.Printf("lfs stat %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object storage unavailable")
		return
	}
	// The transfer must declare its size up front: the Go HTTP server
	// caps a Content-Length body at the declared length, making it a hard
	// bound the staging budget below can trust. Chunked bodies have no
	// bound.
	if r.ContentLength < 0 {
		lfsError(w, http.StatusLengthRequired, "upload requires Content-Length")
		return
	}
	// Node-wide staging budget (Codex round 4): every proxy upload stages
	// its full Content-Length on this replica's disk before verification,
	// and neither the per-key guard nor the per-repo quota bounds that
	// node-wide. An object no solo repository allowance can ever hold —
	// the B/(div+1) fixed point of the byte-fairness growth (round 38;
	// see lfsRepoStagingByteShareDiv), clamped chunk-granular and never
	// above the whole budget — is permanent (413); anything that does not
	// fit right now is transient (429). The reservation is INCREMENTAL
	// (Codex security round 5): only the op slot plus one chunk is taken
	// before body bytes arrive, and stagingFileWriter grows it as data
	// actually lands — an upfront declared-length reservation let one
	// drip-fed PUT pin the entire budget for the TTL without
	// transferring.
	if s.LFSStagingBytes > 0 {
		bound := min(s.LFSStagingBytes,
			max(s.lfsStagingQuantum(), s.LFSStagingBytes/(lfsRepoStagingByteShareDiv+1)))
		if r.ContentLength > bound {
			lfsError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf(
				"object size %d exceeds this server's LFS staging allowance (%d bytes)",
				r.ContentLength, bound))
			return
		}
	}
	if initial := min(r.ContentLength, s.lfsStagingQuantum()); initial > 0 {
		if !s.growLFSStaging(repoID, initial) {
			w.Header().Set("Retry-After", "10")
			lfsError(w, http.StatusTooManyRequests, "server LFS staging budget exhausted, retry")
			return
		}
		stagingReserved = initial
	}
	// NO durable reservation before body progress, deliberately (Codex
	// security round 3 + round 16): a header-only PUT repeated once per
	// TTL could otherwise hold the repository's whole headroom forever
	// without transferring a byte. The batch's declared-size reservation
	// (non-renewable) covers the batch→transfer window; the PUT's own
	// durable admission happens AFTER the body is fully received and
	// verified (the pre-publish block below), so only real transfer
	// progress touches the ledger. Until then the upload is bounded by
	// the node staging budget, not the quota. The zero-growth rule (Codex
	// round 5) still applies there: a registered key's re-publication is
	// charged by neither the counter nor the object cap. The count-cap
	// fast-fail here stops hopeless transfers early (reads only; the
	// atomic cap inside RegisterLFSObject remains authoritative).
	registered, err := s.Store.HasLFSObject(ctx, repoID, key)
	if err != nil {
		log.Printf("lfs registry %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object registry unavailable")
		return
	}
	if !registered {
		// A v0.2.0 legacy-spelled row without its blob is a zero-growth
		// repair too (Codex round 87): registration migrates the row in
		// place — no count, no bytes — so this PUT must be admitted
		// like every registered-but-blobless repair, not denied as
		// fresh growth at the byte quota or object cap (a denial is
		// PERMANENT: no other flow can ever repair the row). The entry
		// probe already proved both spellings blobless — a present
		// legacy blob takes the adoption path above instead.
		registered, err = s.Store.HasLFSObject(ctx, repoID, blob.LFSLegacyKey(repoID, oid))
		if err != nil {
			log.Printf("lfs registry %s: %v", key, err)
			lfsError(w, http.StatusInternalServerError, "object registry unavailable")
			return
		}
	}
	// repair records whether this PUT is a zero-growth REPAIR — a row
	// that predates the request whose blob the entry probe found missing
	// (Codex rounds 56+57): only then did counter-accounted bytes spend
	// time absent, deflating floors scanned meanwhile. Later re-reads may
	// flip `registered` when a TWIN registers this key mid-request; that
	// is fresh growth (the twin publishes before it registers, so the
	// counter and the listing move together, no absence window) and must
	// take neither the repair cover nor the generation bump.
	repair := registered
	if !registered {
		count, err := s.Store.CountLFSObjects(ctx, repoID)
		if err != nil {
			log.Printf("lfs registry count %s: %v", repoID, err)
			lfsError(w, http.StatusInternalServerError, "object registry unavailable")
			return
		}
		if count >= ref.MaxLFSObjectsPerRepo {
			// Boundary race twin of adoptLFSObject's (Codex round 27): a
			// concurrent identical upload can register THIS key between
			// the registered read above and this count, taking the last
			// slot — the upload is then zero-growth, not over-cap.
			registered, err = s.Store.HasLFSObject(ctx, repoID, key)
			if err != nil {
				log.Printf("lfs registry %s: %v", key, err)
				lfsError(w, http.StatusInternalServerError, "object registry unavailable")
				return
			}
			if !registered {
				// The boundary race includes the LEGACY spelling (Codex
				// round 99, the PUT twin of adoption's round-86
				// recheck): mid-roll, an old replica can register this
				// OID's legacy row between the entry lookup and the
				// count — registration migrates that row in place, so
				// the object adds nothing and the cap must not deny it.
				registered, err = s.Store.HasLFSObject(ctx, repoID, blob.LFSLegacyKey(repoID, oid))
				if err != nil {
					log.Printf("lfs registry %s: %v", key, err)
					lfsError(w, http.StatusInternalServerError, "object registry unavailable")
					return
				}
			}
			if !registered {
				lfsError(w, http.StatusInsufficientStorage, fmt.Sprintf(
					"repository LFS object count limit reached (%d objects stored, limit %d)",
					count, ref.MaxLFSObjectsPerRepo))
				return
			}
		}
	}
	dir, err := os.MkdirTemp("", "lfs-upload-*")
	if err != nil {
		lfsError(w, http.StatusInternalServerError, "scratch dir failed")
		return
	}
	scratchDir = dir
	path := filepath.Join(dir, "obj")
	f, err := os.Create(path)
	if err != nil {
		lfsError(w, http.StatusInternalServerError, "scratch file failed")
		return
	}
	// Stall guard: the reservations above are held for the duration of
	// this copy, so the body must keep moving AND finish within the
	// reservation TTL — see idleDeadlineBody.
	body := io.Reader(r.Body)
	rc := http.NewResponseController(w)
	idle := idleDeadlineBody{r: r.Body, rc: rc, absolute: time.Now().Add(s.lfsReservationTTL())}
	if err := rc.SetReadDeadline(idle.deadline(time.Now())); err == nil {
		body = idle
	}
	h := sha256.New()
	sfw := &stagingFileWriter{s: s, repo: repoID, f: f, contentLength: r.ContentLength, reserved: &stagingReserved}
	n, err := io.Copy(io.MultiWriter(sfw, h), body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		if errors.Is(err, errLFSBusy) {
			// The budget filled while this body streamed (other uploads'
			// real bytes claimed it): transient, like the entry check.
			w.Header().Set("Retry-After", "10")
			lfsError(w, http.StatusTooManyRequests, "server LFS staging budget exhausted, retry")
			return
		}
		lfsError(w, http.StatusBadRequest, "reading upload body failed: "+err.Error())
		return
	}
	// The server-enforced Content-Length cap makes n <= ContentLength; a
	// short body errors above. Assert rather than assume.
	assertf(n == r.ContentLength, "lfs upload %s: read %d bytes with Content-Length %d", key, n, r.ContentLength)
	// The oid IS the content address: publishing bytes under a key whose
	// hash doesn't match would corrupt every future download of that oid.
	if got := hex.EncodeToString(h.Sum(nil)); got != oid {
		lfsError(w, http.StatusBadRequest, fmt.Sprintf("content sha256 %s does not match oid %s", got, oid))
		return
	}
	// Canonical-publisher serialization (Codex round 84): from here to
	// the end of the handler this PUT publishes, registers, and
	// writer-verifies the canonical key, so it holds the SAME fleet claim
	// every adoption-side canonical write runs under. The adoption
	// mismatch path deletes provably-corrupt canonical bytes under a
	// registered row with a fingerprint-conditional check-then-delete —
	// which is only sound if no publisher can land valid bytes between
	// the check and the delete, and the claim is exactly that exclusion:
	// while adoption holds it, this publisher waits (429, and the
	// adopter's outcome makes the retry zero-growth); while this
	// publisher holds it, no adoption reaches its mismatch delete. The
	// claim is renewed on the adoption cadence for the region's whole
	// life (publication may legitimately outlast one ref.AdoptionClaimTTL
	// against a slow backend); a definitive loss cancels the region.
	claimed, claimOwner, err := s.Store.TryAdoptionClaim(ctx, repoID, key)
	if err != nil {
		log.Printf("lfs claim %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object registry unavailable")
		return
	}
	if !claimed {
		w.Header().Set("Retry-After", "10")
		lfsError(w, http.StatusTooManyRequests, "a concurrent operation holds this object, retry")
		return
	}
	defer s.releaseAdoptionClaim(key, claimOwner)
	// The claim-guarded region is DETACHED from the client (the adoption
	// side's round-31 discipline): a hangup mid-publish must not abort
	// work the claim serializes — the publication lease below bounds it
	// and returns the staging slots either way.
	regionCtx, regionCancel := context.WithCancelCause(context.WithoutCancel(ctx))
	defer regionCancel(nil)
	stopRenew := s.keepAdoptionClaimRenewed(regionCtx, regionCancel, key, claimOwner)
	defer stopRenew()
	// Reservation freshness (Codex round 8): the body copy above may have
	// consumed most of the TTL, and an expired row's headroom is claimable
	// by other admissions while this object would still become durable.
	// Re-admit the VERIFIED size immediately before publication — the
	// same-key refresh restarts the TTL clock, restoring the pack path's
	// reserve→publish→register ordering with only server-side work (well
	// under any sane TTL) left inside the reservation's lifetime. A denial
	// here means the freed headroom is genuinely gone: reject while
	// nothing durable exists yet.
	//
	// pubCtx then bounds publication + registration by that refreshed
	// reservation's lease (Codex round 11): UploadFile against a stalled
	// backend could otherwise outlast the TTL, letting another admission
	// consume the expired row's headroom while this object still becomes
	// durable. Registration must COMMIT while the reservation lives, so
	// the deadline is 90% of the TTL from just before the refresh — the
	// 10% guard band absorbs app-vs-database clock skew. Deadline expiry
	// aborts the upload with nothing registered (any published bytes are
	// a sweepable orphan). The deadline applies even when no reservation
	// needs protecting (quota disabled, zero-growth repairs — Codex round
	// 12): a stalled publication holds this replica's staging budget and
	// the per-key slot, and those must be reclaimable regardless of quota
	// configuration.
	// pubAnchor is taken BEFORE the refresh below, so when a reservation
	// exists the deadline is measured from a conservative point strictly
	// before the row's created_at.
	pubAnchor := time.Now()
	// resGens holds the reservation's row-incarnation tokens when one was
	// taken below; the upload-failure path retires against exactly these
	// (Codex round 33).
	var resGens map[string]string
	if s.PackQuotaBytes > 0 && n > 0 {
		// The pre-body registered flag can go STALE during the body copy,
		// in BOTH directions. A concurrent identical upload on another
		// replica can publish and register this OID meanwhile (Codex
		// round 26), making this upload zero-growth — charging n as new
		// growth would 507 a push whose object is already durably stored,
		// purely because the twin won. And reclamation makes registration
		// NON-terminal (issue #73, review round 9): a grace-expired row
		// can retire and refund its bytes while the body streams, and
		// another admission may already have consumed the freed headroom
		// — keeping the stale zero-growth verdict would register bytes no
		// admission covers, pushing usage past the quota. Re-read both
		// spellings after verification, immediately before admission: a
		// vanished row demotes this PUT to real new-growth admission, and
		// it is no longer an absence-window repair (retirement bumped the
		// adoption generation, invalidating the floors its cover would
		// have guarded against).
		registered, err = s.Store.HasLFSObject(ctx, repoID, key)
		if err != nil {
			log.Printf("lfs registry %s: %v", key, err)
			lfsError(w, http.StatusInternalServerError, "object registry unavailable")
			return
		}
		if !registered {
			registered, err = s.Store.HasLFSObject(ctx, repoID, blob.LFSLegacyKey(repoID, oid))
			if err != nil {
				log.Printf("lfs registry %s: %v", key, err)
				lfsError(w, http.StatusInternalServerError, "object registry unavailable")
				return
			}
		}
		repair = repair && registered
	}
	if s.PackQuotaBytes > 0 && n > 0 && !registered {
		var dec ref.ReserveDecision
		for attempt := 0; ; attempt++ {
			floor, floorGen, err := s.cachedUsageFloorGen(ctx, repoID)
			if err != nil {
				log.Printf("lfs quota: %s: usage listing failed: %v", repoID, err)
				lfsError(w, http.StatusInternalServerError, "storage quota check failed")
				return
			}
			// The reserve is deadline-bounded by the handoff margin (Codex
			// round 42): the floor above may be served up to usageCacheTTL
			// after its scan, so the admission snapshot must land within the
			// margin for MinPackReservationTTL's coverage argument to hold —
			// a stalled database fails the request rather than admitting
			// against a floor whose orphan cover may have lapsed.
			rsvCtx, rsvCancel := context.WithTimeout(ctx, UsageFloorHandoffMargin)
			dec, err = s.Store.ReservePackBytes(rsvCtx, blob.QuotaRepoGroup(repoID),
				[]ref.Pack{{Key: key, Size: n}}, ref.NewReservationOwner(),
				floor, floorGen, s.PackQuotaBytes, s.lfsReservationTTL())
			rsvCancel()
			if err != nil {
				log.Printf("lfs quota: %s: pre-publish refresh failed: %v", repoID, err)
				lfsError(w, http.StatusInternalServerError, "storage quota check failed")
				return
			}
			if !dec.FloorStale {
				break
			}
			// The ledger's transactional generation recheck rejected the
			// floor (Codex round 60): a restore, adoption, or
			// retained-bytes debit moved it between the serve-time check
			// and the reserve. Invalidate and re-derive once; a second
			// rejection fails closed (the client retries the PUT).
			s.invalidateUsageFloor(repoID)
			if attempt >= 1 {
				log.Printf("lfs quota: %s: usage floor went stale during admission twice", repoID)
				lfsError(w, http.StatusInternalServerError, "storage quota check failed")
				return
			}
		}
		if dec.KeyCapExceeded {
			w.Header().Set("Retry-After", "10")
			lfsError(w, http.StatusTooManyRequests,
				"too many in-flight uploads reserved for this repository, retry later")
			return
		}
		if !dec.OK {
			lfsError(w, http.StatusInsufficientStorage, fmt.Sprintf(
				"repository storage quota exceeded: upload adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight uploads), quota is %d bytes",
				n, dec.UsedBytes, dec.ReservedBytes, s.PackQuotaBytes))
			return
		}
		resGens = dec.Gens
	}
	if s.PackQuotaBytes > 0 && n > 0 && repair {
		// Zero-growth repair COVER (Codex security round 57): the
		// republication below re-materializes counter-accounted bytes,
		// and the post-publish generation bump only stops FUTURE floor
		// serves — an admission already holding a floor scanned during
		// the absence (deflated by up to n, hiding equally sized rowless
		// legacy bytes) can commit after the bytes are back and durably
		// exceed quota. No recheck ordering can close that: a repair can
		// always start after such an admission committed. So the repair
		// accounts its bytes BEFORE publication with a reservation row
		// under an unbounded quota (repairs must never be quota-denied —
		// a registered row must not stay blobless; the key cap still
		// binds). Every stale-floor admission then sees the repaired
		// bytes in active_reservations, exactly compensating the
		// worst-case deflation. The row is left to the TTL: absence
		// floors were scanned before publication, publication ends
		// within the lease (≤ ttl−MinPackReservationTTL, round 51), and
		// scans serve for usageCacheTTL with commits margin-bounded — so
		// the last stale commit lands before publish+cacheTTL+margin ≤
		// T0+ttl, inside the cover's life. The distinct suffix keeps
		// settlement from consuming the cover early (it consumes only
		// the blob key's own spellings).
		rsvCtx, rsvCancel := context.WithTimeout(ctx, UsageFloorHandoffMargin)
		dec, err := s.Store.ReservePackBytes(rsvCtx, blob.QuotaRepoGroup(repoID),
			[]ref.Pack{{Key: key + lfsRepairCoverSuffix, Size: n}}, ref.NewReservationOwner(),
			0, ref.ReserveNoFloorGen, math.MaxInt64, s.lfsReservationTTL())
		rsvCancel()
		if err != nil {
			log.Printf("lfs repair cover %s: %v", key, err)
			lfsError(w, http.StatusInternalServerError, "storage quota check failed")
			return
		}
		if dec.KeyCapExceeded {
			w.Header().Set("Retry-After", "10")
			lfsError(w, http.StatusTooManyRequests,
				"too many in-flight uploads reserved for this repository, retry later")
			return
		}
		if !dec.OK {
			// Unreachable with an unbounded quota (admission saturates);
			// fail closed rather than republish uncovered.
			log.Printf("lfs repair cover %s: denied: %+v", key, dec)
			lfsError(w, http.StatusInternalServerError, "storage quota check failed")
			return
		}
	}
	pubCtx, cancel := context.WithDeadline(regionCtx, pubAnchor.Add(s.lfsPublicationLease()))
	defer cancel()
	if err := s.Blob.UploadFile(pubCtx, key, path); err != nil {
		// PROVEN-clean failure: retire THIS attempt's share of the
		// reservation (Codex round 30): leaving every aborted tiny PUT's
		// row to the TTL let ~30 KB of aborted bodies pin
		// maxActiveReservations and wedge all growth for an hour.
		// Per-attempt refcounts keep a concurrent same-key writer's
		// protection intact (round 22), the gen token keeps a stale
		// retire off any replacement row (round 33), and the retire runs
		// only on EVIDENCE the upload did not commit (round 33): an
		// UploadFile error can be ambiguous — bytes durable, response
		// lost — and retiring then would leave a rowless durable blob
		// uncovered until the floor cache refreshes. Detached from the
		// client (the abort IS the attack) and bounded; a failed retire
		// or an ambiguous verdict only leaves what the TTL always
		// collected.
		if len(resGens) > 0 {
			rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
			defer rcancel()
			if _, serr := s.Blob.Stat(rctx, key); errors.Is(serr, blob.ErrNotFound) {
				if rerr := s.Store.RetirePackAttempts(rctx, blob.QuotaRepoGroup(repoID), resGens); rerr != nil {
					log.Printf("lfs retire %s: %v", key, rerr)
				}
			} else {
				log.Printf("lfs retire %s: skipped, upload outcome ambiguous (stat: %v)", key, serr)
			}
		}
		log.Printf("lfs upload %s: %v", key, err)
		lfsError(w, http.StatusInternalServerError, "object upload failed")
		return
	}
	// Publish-before-commit: the bytes are durable, now settle the ledger
	// and register the object. A failure here may still have committed
	// (ambiguous), so the reservation is deliberately left to the TTL and
	// the sweeper — mirroring the pre-receive hook's crash semantics — and
	// the client's retry lands on the stored-object adoption path above.
	regErr := s.Store.RegisterLFSObject(pubCtx, repoID, key, n)
	if errors.Is(regErr, ref.ErrLFSObjectCapExceeded) {
		// The ledger's atomic cap lost the race our fast-fail pre-check
		// won't have seen: nothing committed. Remove the just-published
		// blob (Codex round 9: DeleteLFS defaults off, so leaving it would
		// strand a quota-counted orphan per cap loser). Safe against
		// concurrent same-key writers: the registered population never
		// shrinks, so once this key's registration is cap-denied, every
		// concurrent NEW-key registration is denied too — nothing can be
		// pointing at these bytes. Best-effort: a failed delete only
		// leaves what pre-fix behavior always left, an orphan for the
		// sweeper. Bounded and detached from the request context (Codex
		// round 14): the cleanup must run even if the client hung up, and
		// a stalled backend must not pin this handler's staging
		// reservation and per-key slot behind an unbounded delete.
		dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
		defer dcancel()
		derr := s.Blob.Delete(dctx, key)
		if derr != nil && !errors.Is(derr, blob.ErrNotFound) {
			log.Printf("lfs cap-denied cleanup %s: %v", key, derr)
		}
		// Retire only THIS writer's attempt (Codex round 37 — the ledger
		// no longer key-wide-deletes on cap denial, because a same-key
		// peer's attempt may still be publishing under the shared row).
		// Same evidence discipline as the upload-failure retire above:
		// the successful delete (or proven absence) is the
		// non-publication evidence; an ambiguous delete leaves the row
		// to settlement or TTL.
		if len(resGens) > 0 && (derr == nil || errors.Is(derr, blob.ErrNotFound)) {
			if rerr := s.Store.RetirePackAttempts(dctx, blob.QuotaRepoGroup(repoID), resGens); rerr != nil {
				log.Printf("lfs cap-denied retire %s: %v", key, rerr)
			}
		}
		lfsError(w, http.StatusInsufficientStorage, fmt.Sprintf(
			"repository LFS object count limit reached (limit %d)", ref.MaxLFSObjectsPerRepo))
		return
	}
	if regErr != nil {
		regErr = fmt.Errorf("lfs register %s: %w", key, regErr)
	}
	// Writer half of the orphan-sweep coordination (issue #5): a sweeper
	// may have deleted the just-uploaded key right before the registration
	// committed; re-upload from the local file if so. Runs even when
	// registration failed — an ambiguous error may have registered the
	// object, and a committed row must never point at missing bytes.
	// Detached from client cancellation (a hangup must not skip the
	// repair) but bounded by the SAME publication lease as everything
	// else in this handler (Codex round 30): blob.EnsurePublished's own
	// size-scaled detached allowance (~175 minutes at 10 GiB) would hold
	// this handler's op slot, temp file, and full staging reservation
	// far past the ~54-minute lease, letting one wedged repair
	// monopolize the node's staging budget. Lease expiry fails the
	// request with the row — if registration committed — in the
	// documented zero-growth repair state, which the client's retry
	// republishes.
	ectx, ecancel := context.WithDeadline(regionCtx, pubAnchor.Add(s.lfsPublicationLease()))
	defer ecancel()
	restoredHere := false
	verifyErr := func() error {
		_, err := s.Blob.Stat(ectx, key)
		if errors.Is(err, blob.ErrNotFound) {
			// This re-upload restores counter-accounted bytes a racing
			// sweep just made absent (Codex round 58, the fresh-path twin
			// of the zero-growth repair): cover it BEFORE publication so
			// admissions holding an absence-window floor see the
			// returning bytes. A repair's cover from above still lives
			// (same TTL), so only the fresh path takes one here.
			if s.PackQuotaBytes > 0 && n > 0 && !repair {
				if cerr := ref.ReserveRestoreCover(ectx, s.Store, blob.QuotaRepoGroup(repoID), key, n, s.lfsReservationTTL()); cerr != nil {
					return cerr
				}
			}
			if uerr := s.Blob.UploadFile(ectx, key, path); uerr != nil {
				return uerr
			}
			restoredHere = true
			return nil
		}
		return err
	}()
	if verifyErr != nil {
		log.Printf("lfs verify %s: %v", key, errors.Join(regErr, verifyErr))
		lfsError(w, http.StatusInternalServerError, "object verification failed")
		return
	}
	if regErr != nil {
		log.Printf("%v", regErr)
		lfsError(w, http.StatusInternalServerError, "object registration failed")
		return
	}
	// Zero-growth repair republication (Codex round 56): repair means
	// the entry probe found NO blob for a key whose bytes the usage
	// counter already included BEFORE this request — a floor scanned
	// during that absence subtracted the full counter from a listing
	// missing these bytes, hiding equally sized rowless legacy data
	// (a twin-flipped `registered` is fresh growth instead — no bump,
	// see `repair`). The republication
	// above re-materialized them without touching the counter, so floors
	// cached in the absence window now undercount physical usage: bump
	// the shared floor generation — the PUT analogue of the sweeper's
	// quarantine-restore bump (round 54). Best-effort with logging, like
	// that bump: the bytes ARE durably back and registered, and a failed
	// bump leaves only the TTL-bounded staleness a crash right here
	// would always leave. Zero-byte objects never appear in floors.
	if (repair || restoredHere) && n > 0 {
		// Bounded by the cover handoff window (Codex round 69): the
		// repair cover's remaining life at this point is at least the
		// publication lease's residual (≥ MinPackReservationTTL), and
		// the bump must commit inside it.
		bctx, bcancel := context.WithTimeout(context.WithoutCancel(ctx), blob.CoverHandoffTimeout)
		defer bcancel()
		if err := s.Store.BumpLFSAdoptionGen(bctx, BaseRepo(repoID)); err != nil {
			log.Printf("lfs repair %s: floor generation bump failed: %v", key, err)
		}
		s.invalidateUsageFloor(repoID)
	}
	releaseUploadResources()
	// A failed compat copy FAILS the request (Codex round 83): the
	// canonical object is durable and registered, so the client's retry
	// is zero-growth and lands on the registered fast path, whose
	// ensureLegacyCompat seam repairs the missing spelling — returning
	// 200 here instead would leave old replicas 404ing this object with
	// nothing left to converge it.
	if err := s.maybeLegacyCompatCopy(ctx, repoID, oid, key, n); err != nil {
		log.Printf("lfs compat %s: %v", key, err)
		lfsError(w, compatFailStatus(err), "compatibility copy failed, retry")
		return
	}
	// Terminal lease verdict (Codex round 93, the PUT twin of round 88's
	// adoption-copy fix): cancellation alone cannot reject work that
	// COMMITS concurrently with a recorded loss — the region's final op
	// can return nil as the renewer cancels, and the handler would
	// answer 200 on a lease it no longer provably holds, while the
	// claim's successor may be writing different canonical bytes past
	// this worker's verify. JOIN the renewer (stopRenew blocks until it
	// exits) and reject any recorded loss before reporting success; the
	// registration is committed and idempotent, so the client's retry
	// is zero-growth.
	stopRenew()
	if cause := context.Cause(regionCtx); cause != nil && !errors.Is(cause, context.Canceled) {
		log.Printf("lfs claim %s: %v", key, cause)
		lfsError(w, http.StatusInternalServerError, "object claim lost during publication, retry")
		return
	}
	// Final liveness re-touch (issue #73, Codex round 13): registration
	// left the row unmarked, but it committed BEFORE the sweep-race
	// verify and the size-scaled compat copy, either of which can outlast
	// a short GC grace — a scan can mark the still-unreferenced row
	// meanwhile. The 200 below promises the client it may commit its
	// pointer, so it must be the promise of a freshly touched row.
	// touched=false means the row is already gone: answer retryable so
	// the retry re-registers instead.
	tctx, tcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
	defer tcancel()
	if touched, terr := s.Store.TouchLFSObject(tctx, repoID, key); terr != nil {
		log.Printf("lfs touch %s: %v", key, terr)
		lfsError(w, http.StatusInternalServerError, "liveness touch failed, retry")
		return
	} else if !touched {
		lfsError(w, http.StatusTooManyRequests, "object retired during upload, retry")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// maybeLegacyCompatCopy maintains the v0.2.0 read surface during a rolling
// upgrade (ADR-41, Codex round 80): with LFSV020WriteCompat set, a
// verified publication is also copied to the legacy spelling so
// not-yet-upgraded replicas — which derive only that key — keep serving
// the object until the fleet rolls. Detached from the client but NOT
// best-effort (Codex round 83): failures propagate so the request fails
// and the client retries — the canonical copy stays the registered truth,
// and the retry converges the bridge through ensureLegacyCompat's
// registered fast path.
func (s *Server) maybeLegacyCompatCopy(ctx context.Context, repoID, oid, key string, size int64) error {
	if !s.LFSV020WriteCompat {
		return nil
	}
	legacyKey := blob.LFSLegacyKey(repoID, oid)
	// An EXISTING legacy spelling already satisfies the bridge (Codex
	// round 89): the copy would only overwrite those bytes — zero growth —
	// while the admission below charges the full size as fresh, so the
	// zero-growth repair flow (a registered-but-blobless canonical whose
	// legacy copy survived) spuriously 507'd at the quota edge, forever.
	// Skipping matches ensureLegacyCompat's semantics: the legacy key
	// stays writable by v0.1.0 presigned PUTs for its whole retention, so
	// overwriting it buys no integrity old replicas could rely on.
	pctx, pcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	_, perr := s.Blob.Stat(pctx, legacyKey)
	pcancel()
	switch {
	case perr == nil:
		// Existence is NOT satisfaction while a mismatch memo stands
		// (Codex round 129): the verified PUT's tail reaches this probe
		// directly — never ensureLegacyCompat — so without this check a
		// resource-denied cleanup's memo was ignored, the handler
		// returned 200, and old replicas served the known-corrupt bytes
		// until some other write-scoped touch. Only a REMOVED spelling
		// proceeds to the re-bridge below; spared or proven-valid
		// returns as before.
		if _, ok, merr := s.lfsMismatchMemo(ctx, repoID, legacyKey); merr != nil {
			return merr // an unreadable marker is never absence (round 132)
		} else if ok {
			switch v, rerr := s.resolveLegacyMismatchMemo(ctx, repoID, oid, key, legacyKey); {
			case rerr != nil:
				return rerr
			case v == legacyMemoRemoved:
				// The corrupt spelling is gone: re-bridge below.
			case v == legacyMemoValid:
				return nil // the true object converged
			default:
				// PENDING must not become 200 (Codex security round
				// 129): this is the verified PUT's tail — after success
				// the client never touches the object again, so a
				// spared cleanup would leave old replicas on the
				// known-corrupt bytes for the roll. Answer retryable
				// contention; the client's retry lands on the
				// registered fast path, which re-presses the cleanup.
				return fmt.Errorf("compat cleanup pending %s: %w", legacyKey, errLFSBusy)
			}
			break
		}
		return nil
	case !errors.Is(perr, blob.ErrNotFound):
		return fmt.Errorf("compat probe %s: %w", legacyKey, perr)
	}
	// Compat copies serialize on the LEGACY key's fleet claim (Codex
	// security round 85): the bridge repair runs from lock-free fast
	// paths, so without a shared exclusion every tiny concurrent touch of
	// a not-yet-bridged object launched another full server-side copy —
	// fleet-multiplied backend read/write cost for identical bytes.
	// One winner copies; losers answer retryable contention (429). The
	// claim is renewed through the size-scaled copy like every long
	// canonical operation. Lock ordering: callers may hold the CANONICAL
	// key's claim; nothing acquires canonical while holding legacy.
	claimed, claimOwner, err := s.Store.TryAdoptionClaim(ctx, repoID, legacyKey)
	if err != nil {
		return fmt.Errorf("compat claim %s: %w", legacyKey, err)
	}
	if !claimed {
		return fmt.Errorf("compat copy %s: %w", legacyKey, errLFSBusy)
	}
	defer s.releaseAdoptionClaim(legacyKey, claimOwner)
	// Re-probe under the claim (Codex round 90): a serialized peer — or
	// an old replica's still-valid presigned PUT — can land the legacy
	// spelling between the lock-free probe above and this acquisition.
	// Without the recheck the winner recharges the full size as fresh
	// growth (a spurious 507 at the quota edge) and recopies identical
	// bytes; presence still satisfies the bridge.
	rpctx, rpcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	_, rperr := s.Blob.Stat(rpctx, legacyKey)
	rpcancel()
	switch {
	case rperr == nil:
		return nil // a peer completed the bridge while we queued
	case !errors.Is(rperr, blob.ErrNotFound):
		return fmt.Errorf("compat recheck %s: %w", legacyKey, rperr)
	}
	// Node-wide operation slot for the copy (Codex round 92): distinct-OID
	// batch touches each launch a size-scaled server-side copy, and the
	// per-key claim dedupes only one OID while quota reservations are
	// per-repository (or disabled) — without a cross-key bound, one
	// write-scoped client could fan out unbounded simultaneous multipart
	// copies and exhaust backend connections. The same repository-fair
	// slot that bounds uploads and adoptions bounds the bridge; denial is
	// transient (429) like every op-slot contention, and the next
	// write-scoped touch repairs via ensureLegacyCompat.
	if !s.reserveLFSStaging(repoID, 0) {
		return fmt.Errorf("compat copy %s: %w", legacyKey, errLFSBusy)
	}
	defer s.releaseLFSStaging(repoID, 0)
	// Size-scaled allowance, not a fixed wall-clock cap (Codex round 81):
	// accepted objects reach many GiB and S3's multipart copy is
	// sequential, so the one-minute cleanup bound was guaranteed to
	// cancel every large copy — silently 404ing exactly the objects the
	// flag exists to keep readable on old replicas. The publication
	// allowance (a worst-case ~1 MiB/s progress floor, the
	// verifyStoredLFSObject shape) lets any live backend finish while a
	// wedged one is still cut loose; the copy runs detached and
	// best-effort, so the held resources are one goroutine, not the
	// upload's staging slots.
	doCopy := func(opCtx context.Context) error {
		return s.withRenewedAdoptionClaim(opCtx, legacyKey, claimOwner, func(copyCtx context.Context) error {
			bound := 5*time.Minute + time.Duration(size>>20)*time.Second
			cctx, cancel := context.WithTimeout(copyCtx, bound)
			defer cancel()
			return s.Blob.Copy(cctx, legacyKey, key)
		})
	}
	dctx := context.WithoutCancel(ctx)
	if size <= 0 || s.PackQuotaBytes <= 0 {
		if err := doCopy(dctx); err != nil {
			return fmt.Errorf("compat copy %s: %w", legacyKey, err)
		}
		return nil
	}
	// REAL quota admission for the bridge bytes (Codex security round
	// 84): the copy is fresh growth — a second size-byte object the
	// canonical upload's reservation never covered — and admitting it
	// under an unbounded quota let an upload equal to the remaining
	// headroom leave physical usage near 2Q. A denial fails the bridge,
	// never overshoots — the canonical object stays registered and a
	// later retry repairs the bridge if headroom appears.
	// ONE reservation both admits and covers (Codex round 94, replacing
	// round 85's post-handoff admission retirement): a separate admission
	// row beside the renewed cover double-charged the same planned bytes
	// for the copy's whole window. The cover's FIRST reservation performs
	// the real admission (floor + generation recheck, real
	// PackQuotaBytes) on the SUFFIXED key; renewals extend it unbounded.
	// The suffixed row is never settled — it lingers to its TTL as the
	// deliberate stale-floor tail, exactly as before. renew invocations
	// are strictly serialized by WithRenewedCover.
	admitted := false
	// The compat copy materializes ANOTHER size bytes after registration
	// consumed the canonical upload's reservation (Codex round 82): a
	// floor scanned before this copy sees neither the legacy bytes nor
	// any reservation for them, so an admission committing against it
	// could durably exceed the quota. Same machinery as the adoption
	// publish: a cover renewed through the copy and its stale-floor tail,
	// with the floor-generation bump as the invalidation handoff. The
	// cover row (legacy key + repair suffix) is never settled — the TTL
	// collects it after every absence-window floor has aged out.
	renewCover := func(rctx context.Context) error {
		if admitted {
			return ref.ReserveRestoreCover(rctx, s.Store, blob.QuotaRepoGroup(repoID), legacyKey, size, s.lfsReservationTTL())
		}
		for attempt := 0; ; attempt++ {
			floor, floorGen, err := s.cachedUsageFloorGen(rctx, repoID)
			if err != nil {
				return fmt.Errorf("compat admission %s: usage listing: %w", legacyKey, err)
			}
			rsvCtx, rsvCancel := context.WithTimeout(rctx, UsageFloorHandoffMargin)
			dec, err := s.Store.ReservePackBytes(rsvCtx, blob.QuotaRepoGroup(repoID),
				[]ref.Pack{{Key: legacyKey + lfsRepairCoverSuffix, Size: size}}, ref.NewReservationOwner(),
				floor, floorGen, s.PackQuotaBytes, s.lfsReservationTTL())
			rsvCancel()
			if err != nil {
				return fmt.Errorf("compat admission %s: %w", legacyKey, err)
			}
			if dec.FloorStale {
				s.invalidateUsageFloor(repoID)
				if attempt >= 1 {
					return fmt.Errorf("compat admission %s: usage floor went stale twice", legacyKey)
				}
				continue
			}
			if dec.KeyCapExceeded {
				return fmt.Errorf("compat admission %s: %w", legacyKey, errLFSBusy)
			}
			if !dec.OK {
				return fmt.Errorf("compat copy %s: %w (bridge adds %d bytes to %d stored, +%d reserved, quota %d)",
					legacyKey, errLFSCompatQuotaExceeded, size, dec.UsedBytes, dec.ReservedBytes, s.PackQuotaBytes)
			}
			admitted = true
			return nil
		}
	}
	bump := func(bctx context.Context) error {
		return s.Store.BumpLFSAdoptionGen(bctx, BaseRepo(repoID))
	}
	copyErr, renewErr := blob.WithRenewedCover(dctx, renewCover, doCopy, bump)
	if copyErr != nil {
		// Ambiguous-commit probe (Codex round 95, P1): an S3 copy can
		// COMMIT its destination and still return an error (response
		// lost, context canceled concurrently). Returning here skipped
		// the cover tail and the floor-generation handoff — and every
		// retry saw the spelling present and skipped them too, so the
		// durable bytes stayed invisible to pre-copy cached floors for
		// their whole serve-and-handoff window. A present destination is
		// a commit: re-cover best-effort, hand off, and succeed.
		actx, acancel := context.WithTimeout(dctx, lfsProbeTimeout)
		_, aerr := s.Blob.Stat(actx, legacyKey)
		acancel()
		if aerr != nil {
			// Truly absent (or unknowable: fail closed, the retry
			// repairs via ensureLegacyCompat). PROPAGATED, not swallowed
			// (Codex round 83): a 200 with no legacy spelling does not
			// converge — the canonical row is registered, so no client
			// retry would ever reach a path that re-attempts this copy
			// without the registered-retry repair seam.
			return fmt.Errorf("compat copy %s: %w", legacyKey, copyErr)
		}
		if errors.Is(copyErr, blob.ErrCoverContinuityLost) {
			// The bridge twin of the adoption rule (Codex security round
			// 107): a commit whose error IS the cover cancellation is an
			// unsafe uncovered-gap commit — accepting it would keep bytes
			// whose headroom a stale-floor admission may already have
			// spent. Remove it under the registration fence; a DECLINED
			// or ERRORED cleanup retains bytes round-89's probe will make
			// every retry skip, so recover their accounting (round 106)
			// before failing. Deletion is pressed until proven (the
			// round-108 shared seam): only removing the bytes revokes the
			// gap's spent headroom.
			if proven, deleted := s.uncoveredCleanupProven(dctx, repoID, legacyKey); !proven || !deleted {
				s.ensureAmbiguousCommitAccounting(dctx, repoID, legacyKey, size, bump)
			}
			return fmt.Errorf("compat copy %s: %w", legacyKey, copyErr)
		}
		// The cover AND the handoff must land (or the stale-floor window
		// must pass) before success (Codex round 97): one swallowed
		// attempt never converges — every retry sees the spelling
		// present and skips this seam.
		s.ensureAmbiguousCommitAccounting(dctx, repoID, legacyKey, size, bump)
		s.invalidateUsageFloor(repoID)
		log.Printf("lfs compat copy %s: destination committed despite copy error: %v", legacyKey, copyErr)
		return nil
	}
	s.invalidateUsageFloor(repoID)
	if renewErr != nil {
		// The adoption path's round-105 rule applies to the bridge twin:
		// an uncovered-gap commit (or a handoff that never landed under a
		// live cover) may already have let a stale-floor admission consume
		// headroom these bytes never occupied — accepting the copy could
		// durably exceed the quota. Remove the destination under the held
		// legacy claim and FAIL; the request's failure keeps the client
		// retrying until ensureLegacyCompat re-bridges under a fresh
		// admission. The legacy spelling carries no integrity contract,
		// so deleting our own copy loses nothing old replicas could
		// trust.
		if proven, deleted := s.uncoveredCleanupProven(dctx, repoID, legacyKey); !proven || !deleted {
			// The bytes (may) remain: a DECLINED delete means an old
			// replica registered this spelling mid-roll (Codex round 106,
			// P1) and the fence rightly spared it; an UNPROVEN one closed
			// the round-108 retry window with the outcome unknown. Either
			// way round-89's legacy-present probe makes every retry skip
			// this seam, so no later flow would account the retained
			// bytes — recover the accounting the lost cover abandoned
			// before failing: block until a fresh cover and the fleet
			// floor invalidation land, or every pre-copy floor has
			// expired.
			s.ensureAmbiguousCommitAccounting(dctx, repoID, legacyKey, size, bump)
		}
		return fmt.Errorf("compat copy %s: cover continuity lost: %w", legacyKey, renewErr)
	}
	return nil
}

// legacyMemoVerdict is resolveLegacyMismatchMemo's outcome: pending
// (spared — guard busy, budget denied, claim held, lease lost, or the
// snapshot unknowable; the memo stands), valid (the current version
// decisively proved true; memo cleared), or removed (the corrupt
// spelling was deleted; the caller may re-bridge).
type legacyMemoVerdict int

const (
	legacyMemoPending legacyMemoVerdict = iota
	legacyMemoValid
	legacyMemoRemoved
)

// resolveLegacyMismatchMemo presses the memoized-mismatch cleanup for
// legacyKey — the rounds 121-127 machinery: per-key guard and op slot
// taken non-blockingly, legacy fleet claim fencing the verdict, a
// claim-renewed single-version snapshot, decisive three-way judgment,
// and an owner-fenced renewal before the delete. One shared seam for
// BOTH compat probes (Codex round 129: the PUT tail reaches
// maybeLegacyCompatCopy directly, never ensureLegacyCompat, so a memo
// pending there was ignored and the known-corrupt spelling served old
// replicas for the roll's remainder). See legacyMemoVerdict for the
// three-way outcome (Codex security round 129).
func (s *Server) resolveLegacyMismatchMemo(ctx context.Context, repoID, oid, key, legacyKey string) (legacyMemoVerdict, error) {
	// A pending memo is cleared ONLY by a decisive verdict on the
	// CURRENT version (Codex security round 122): the previous
	// moved-fingerprint arm dropped the memo and returned success,
	// so replacing memoized corrupt bytes with DIFFERENT corrupt
	// bytes was accepted unverified forever — no later touch had a
	// memo to consult. The single-version snapshot below hashes
	// whatever stands now: proven wrong deletes and re-bridges,
	// proven valid clears the memo (the true object converged —
	// also what stops a stale memo from re-hashing a healthy
	// object on every touch), and unknowable keeps the marker
	// pending for the next touch.
	// The registered fast paths reach here with NO op slot and NO
	// per-key guard (Codex round 122): concurrent batches for this
	// OID would each launch a snapshot download, bypassing
	// lfsMaxStagingOps, the repo op share, and same-key dedup. Take
	// both NON-blockingly — one cleanup per key at a time, one op
	// slot for its snapshot's bytes to meter against — and a busy
	// or exhausted verdict spares: the memo stands, the next touch
	// re-presses (round 121: contention delays cleanup, never
	// terminalizes it).
	if _, busy := s.lfsUploadsInFlight.LoadOrStore(legacyKey, struct{}{}); busy {
		return legacyMemoPending, nil // another worker is pressing this cleanup
	}
	defer s.lfsUploadsInFlight.Delete(legacyKey)
	if !s.reserveLFSStaging(repoID, 0) {
		return legacyMemoPending, nil // spared; the memo stands for the next touch
	}
	defer s.releaseLFSStaging(repoID, 0)
	// The verdict must be obtained AND acted on under the legacy
	// fleet claim (Codex security round 123): the process-local
	// guard above let replica B act on a snapshot predating replica
	// A's delete+rebridge, deleting A's fresh VALID bridge. The
	// claim serializes every new-replica legacy actor (round 85);
	// the owner-fenced renewal just before the delete proves no
	// peer interleaved (release deletes the row, takeover moves the
	// owner — renewal succeeds only untouched), so no verdict can
	// go stale across a peer's action. The fall-through re-bridge
	// re-acquires the claim itself, safely: after an under-claim
	// delete the spelling is absent, and every peer's snapshot runs
	// under the claim too, so none can hold a pre-delete verdict.
	cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), lfsClaimRenewTimeout)
	claimed, claimOwner, cerr := s.Store.TryAdoptionClaim(cctx, repoID, legacyKey)
	ccancel()
	if cerr != nil || !claimed {
		return legacyMemoPending, nil // held elsewhere or unknowable: spared, memo stands
	}
	releaseClaim := func() {
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsClaimRenewTimeout)
		if rerr := s.Store.ReleaseAdoptionClaim(rctx, legacyKey, claimOwner); rerr != nil {
			log.Printf("lfs compat %s: release cleanup claim: %v", legacyKey, rerr)
		}
		rcancel()
	}
	// The snapshot's size-scaled life can exceed ref.AdoptionClaimTTL
	// (Codex round 126): a claim acquired here but first renewed
	// AFTER a multi-hour download always found an expired row — the
	// proven-corrupt spelling was spared forever, with every later
	// touch repeating the full snapshot. Keep the claim renewed on
	// the adoption cadence for the snapshot's whole life, join
	// before judging (round 88), and spare on a recorded loss.
	snapCtx, snapCancel := context.WithCancelCause(context.WithoutCancel(ctx))
	defer snapCancel(nil)
	stopRenew := s.keepAdoptionClaimRenewed(snapCtx, snapCancel, legacyKey, claimOwner)
	m, decisive := s.legacySourceProvenMismatched(snapCtx, repoID, legacyKey, oid)
	stopRenew()
	if cause := context.Cause(snapCtx); cause != nil && !errors.Is(cause, context.Canceled) {
		releaseClaim()
		return legacyMemoPending, nil // lease lost mid-snapshot: spared, memo stands
	}
	switch {
	case !decisive:
		releaseClaim()
		return legacyMemoPending, nil // spared; the memo stands pending for the next touch
	case !m:
		s.forgetLFSMismatch(ctx, repoID, legacyKey)
		releaseClaim()
		return legacyMemoValid, nil // proven VALID: the true object converged
	}
	rnCtx, rnCancel := context.WithTimeout(context.WithoutCancel(ctx), lfsClaimRenewTimeout)
	renewed, rnErr := s.Store.RenewAdoptionClaim(rnCtx, legacyKey, claimOwner)
	rnCancel()
	if rnErr != nil || !renewed {
		releaseClaim()
		return legacyMemoPending, nil // possibly stale verdict: spared, memo stands
	}
	delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
	defer delCancel()
	if err := s.Blob.Delete(delCtx, legacyKey); err != nil && !errors.Is(err, blob.ErrNotFound) {
		releaseClaim()
		return legacyMemoPending, fmt.Errorf("remove memoized mismatched legacy object %s: %w", legacyKey, err)
	}
	s.forgetLFSMismatch(ctx, repoID, legacyKey)
	releaseClaim()
	log.Printf("lfs compat %s: removed memoized mismatched legacy blob before re-bridging", key)
	return legacyMemoRemoved, nil
}

// ensureLegacyCompat repairs a missing legacy spelling for a REGISTERED
// object (Codex round 83): a compat copy that failed after registration
// left the object 404ing on old replicas, and every retry lands on the
// registered fast paths — so those paths must converge the bridge, or
// nothing ever will. One extra legacy stat per registered write-touch,
// only while the compat flag is on; the copy budget is derived from the
// canonical object's stored size.
func (s *Server) ensureLegacyCompat(ctx context.Context, repoID, oid, key string) error {
	if !s.LFSV020WriteCompat {
		return nil
	}
	legacyKey := blob.LFSLegacyKey(repoID, oid)
	lctx, lcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	_, lerr := s.Blob.Stat(lctx, legacyKey)
	lcancel()
	switch {
	case lerr == nil:
		// Existence is NOT satisfaction while a memoized failed
		// verification stands (Codex round 121): a corrupt legacy
		// survivor whose budgeted cleanup was spared by staging
		// contention would otherwise lock in behind this very probe for
		// the roll's remainder — the registered fast paths are the ONLY
		// touches a registered object gets. If the spelling still
		// carries the proven-wrong fingerprint, press the cleanup here;
		// a deletion falls through to the bridge repair below, which
		// republishes verified canonical bytes. A still-spared verdict
		// keeps the memo for the next touch.
		switch _, ok, merr := s.lfsMismatchMemo(ctx, repoID, legacyKey); {
		case merr != nil:
			return merr // an unreadable marker is never absence (round 132)
		case !ok:
			return nil // the spelling exists: nothing to converge
		}
		switch v, rerr := s.resolveLegacyMismatchMemo(ctx, repoID, oid, key, legacyKey); {
		case rerr != nil:
			return rerr
		case v == legacyMemoValid:
			return nil // the true object converged; nothing to re-bridge
		case v == legacyMemoPending:
			// PENDING propagates as retryable contention here too
			// (Codex round 131): the PUT tail's 429 sends its retry to
			// exactly this seam, and mapping pending to success would
			// close that loop with a 200 over the known-corrupt
			// spelling. Every caller maps errLFSBusy to a retryable
			// verdict (429), so clients keep pressing until the cleanup
			// deletes, proves the version valid, or the memo clears.
			return fmt.Errorf("compat cleanup pending %s: %w", legacyKey, errLFSBusy)
		}
		// Fall through: the spelling is now absent and the bridge
		// repair below republishes the verified canonical bytes.
	case !errors.Is(lerr, blob.ErrNotFound):
		return fmt.Errorf("compat probe %s: %w", legacyKey, lerr)
	}
	sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	size, serr := s.Blob.Stat(sctx, key)
	scancel()
	if serr != nil {
		// Registered-but-blobless is the zero-growth repair state; the
		// compat bridge has nothing to copy until it heals.
		return fmt.Errorf("compat source %s: %w", key, serr)
	}
	return s.maybeLegacyCompatCopy(ctx, repoID, oid, key, size)
}
