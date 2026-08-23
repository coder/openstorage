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
		_, err := s.Blob.Stat(ctx, key)
		switch {
		case err == nil:
			// Already stored: no actions means the client skips the
			// upload — but ONLY once the object is durably registered
			// (pre-#51 uploads have no rows; without adoption the orphan
			// sweep would classify referenced objects as reapable, so a
			// swallowed adoption failure here would let the client commit
			// a pointer to a reapable blob).
			switch aerr := s.adoptLFSObjectLocked(ctx, repoID, key, obj.OID); {
			case aerr == nil:
				continue
			case errors.Is(aerr, ref.ErrLFSObjectCapExceeded):
				lfsObjectFail(&results[i], http.StatusInsufficientStorage, fmt.Sprintf(
					"repository LFS object count limit reached (limit %d)", ref.MaxLFSObjectsPerRepo))
				continue
			case errors.Is(aerr, errLFSBusy):
				lfsObjectFail(&results[i], http.StatusTooManyRequests,
					"a concurrent operation holds this object, retry")
				continue
			case errors.Is(aerr, errLFSContentMismatch):
				// A mismatched legacy blob was removed: the object is now
				// genuinely missing — fall through and issue an upload
				// action so the client's correct bytes arrive verified.
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
		if !checked[key] {
			checked[key] = true
			// The zero-growth rule (Codex round 5): a missing object
			// whose registry row exists — an adopted or settled object
			// whose blob vanished (racing DeleteLFS sweep, manual bucket
			// surgery) — is already counted by the usage counter AND the
			// object-count cap, so its re-publication must be charged by
			// neither: an at-quota (or at-cap) repository could otherwise
			// never repair itself.
			registered, rerr := s.Store.HasLFSObject(ctx, repoID, key)
			if rerr != nil {
				log.Printf("lfs registry %s: %v", key, rerr)
				checkFailed[key] = true
			} else {
				zeroGrowth[key] = registered
			}
			if !checkFailed[key] {
				order = append(order, key)
			}
		}
		if checkFailed[key] {
			// Every occurrence of an uncheckable key fails: without the
			// zero-growth classification the entry can be neither charged
			// correctly nor safely admitted free.
			lfsObjectFail(&results[i], http.StatusInternalServerError, "object registry unavailable")
			continue
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
			used := max(ls.UsageBytes, floor)
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

// errLFSContentMismatch reports a stored blob whose bytes do not hash to
// its content address — a legacy presigned-era upload that was never
// digest-verified (issue #51's original hole).
var errLFSContentMismatch = errors.New("stored LFS object does not match its content address")

// errLFSBusy is a transient denial: a concurrent operation holds the
// resources (per-key slot, staging budget) this request needs. Callers
// answer 429 + Retry-After.
var errLFSBusy = errors.New("concurrent LFS operation in progress")

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
	registered, err := s.Store.HasLFSObject(ctx, repoID, key)
	if err != nil || registered {
		return err
	}
	// Fast-fail the object cap BEFORE the verification download (Codex
	// security round 4): an at-cap repository holding a rowless legacy
	// blob would otherwise pay a full download+hash on every write-scoped
	// touch and then 507 in RegisterLFSObject anyway — a repeatable
	// bandwidth/staging drain (a near-budget blob blocks the replica's
	// unrelated uploads for the whole hash). O(1) counter read; advisory
	// only — RegisterLFSObject's in-transaction check stays authoritative.
	count, err := s.Store.CountLFSObjects(ctx, repoID)
	if err != nil {
		return err
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
			return nil // the twin registered this same key: nothing to add
		}
		return ref.ErrLFSObjectCapExceeded
	}
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
	// Bounded like publication (Codex round 17) and DETACHED from the
	// client (Codex round 31, security): adoption is server-side repair
	// work. A client canceling mid-verification would abort the ranged
	// hash before RegisterLFSObject, leave the blob rowless, and let the
	// next tiny batch restart the whole hash at offset zero — repeatable
	// backend reads over large legacy blobs, one full pass per cancel.
	// Once adoption starts it runs to completion under the lease
	// regardless of the requester's fate; the committed row makes the
	// work once-per-object. The lease still bounds the op slot against a
	// wedged backend or a parked connection.
	vctx, vcancel := context.WithTimeout(context.WithoutCancel(ctx), s.lfsReservationTTL()*9/10)
	defer vcancel()
	verifiedSize, err := s.verifyStoredLFSObject(vctx, repoID, key, oid)
	if err != nil {
		if !errors.Is(err, errLFSContentMismatch) {
			return err
		}
		// Cross-replica coordination (Codex rounds 16+18): the delete runs
		// inside GuardedLFSDelete, serialized against registrations — a
		// concurrent verified upload either registered first (the guard
		// aborts, its bytes stand) or blocks until the delete commits and
		// then republishes via its post-registration writer verify
		// (blob.EnsurePublished). No interleaving can leave a registered
		// row pointing at deleted bytes. Bounded and detached from the
		// client's connection like every cleanup.
		dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsCleanupTimeout)
		defer dcancel()
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
			return nil // replaced and registered by a concurrent verified upload
		}
		log.Printf("lfs adopt %s: removed legacy blob failing content verification", key)
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
	if err := s.Store.RegisterLFSObject(vctx, repoID, key, verifiedSize); err != nil {
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
	// Bounded like the PUT path's probe (Codex round 29): this recheck
	// runs while the caller holds the per-key slot and an op slot, and it
	// sits OUTSIDE vctx (registration already committed under it), so a
	// wedged backend must hand the slots back within a bounded window
	// instead of pinning them for as long as the batch client stays
	// connected. A timeout fails closed: the registration is committed
	// and idempotent, so the client's retry lands on the adopted path.
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), lfsProbeTimeout)
	defer rcancel()
	if _, err := s.Blob.Stat(rctx, key); err != nil {
		return fmt.Errorf("adopted object vanished (racing sweep?): %w", err)
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
func (s *Server) verifyStoredLFSObject(ctx context.Context, repo, key, oid string) (int64, error) {
	size, err := s.Blob.Stat(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("stat for verification: %w", err)
	}
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
		return 0, errLFSBusy
	}
	defer s.releaseLFSVerifyBuffer(repo)
	h := sha256.New()
	for off := int64(0); off < size; {
		n := min(lfsStagingChunk, size-off)
		chunk, err := s.Blob.ReadRange(ctx, key, off, n)
		if err != nil {
			return 0, fmt.Errorf("read for verification [%d,%d): %w", off, off+n, err)
		}
		h.Write(chunk)
		off += n
	}
	if hex.EncodeToString(h.Sum(nil)) != oid {
		return 0, errLFSContentMismatch
	}
	return size, nil
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
		grow := max(need, lfsStagingChunk)
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
	// One in-flight operation per key, acquired before ANY per-key work
	// (Codex rounds — security 1 and 16): concurrent identical PUTs must
	// not multiply staging files, and concurrent adoption verifications
	// of one legacy key must not race each other's mismatch deletes.
	if _, busy := s.lfsUploadsInFlight.LoadOrStore(key, struct{}{}); busy {
		w.Header().Set("Retry-After", "10")
		lfsError(w, http.StatusTooManyRequests, "an operation for this object is already in progress")
		return
	}
	defer s.lfsUploadsInFlight.Delete(key)
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
		return
	}
	stagingReserved := int64(0)
	defer func() { s.releaseLFSStaging(repoID, stagingReserved) }()
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
	switch _, err := s.Blob.Stat(pctx, key); {
	case err == nil:
		switch aerr := s.adoptLFSObject(ctx, repoID, key, oid); {
		case aerr == nil:
			w.WriteHeader(http.StatusOK)
			return
		case errors.Is(aerr, ref.ErrLFSObjectCapExceeded):
			lfsError(w, http.StatusInsufficientStorage, fmt.Sprintf(
				"repository LFS object count limit reached (limit %d)", ref.MaxLFSObjectsPerRepo))
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
			max(lfsStagingChunk, s.LFSStagingBytes/(lfsRepoStagingByteShareDiv+1)))
		if r.ContentLength > bound {
			lfsError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf(
				"object size %d exceeds this server's LFS staging allowance (%d bytes)",
				r.ContentLength, bound))
			return
		}
	}
	if initial := min(r.ContentLength, lfsStagingChunk); initial > 0 {
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
	defer os.RemoveAll(dir)
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
	if s.PackQuotaBytes > 0 && n > 0 && !registered {
		// The pre-body registered flag can go STALE during the body copy
		// (Codex round 26): a concurrent identical upload on another
		// replica can publish and register this OID meanwhile, making
		// this upload zero-growth — charging n as new growth would 507 a
		// push whose object is already durably stored, purely because the
		// twin won. Re-read the registry after verification, immediately
		// before admission, and skip the reservation when the key is now
		// registered (registration is terminal for a key, so this read
		// cannot go stale in the dangerous direction; the reverse race —
		// registered row losing its blob — is the zero-growth repair
		// path, whose republication needs no reservation either).
		registered, err = s.Store.HasLFSObject(ctx, repoID, key)
		if err != nil {
			log.Printf("lfs registry %s: %v", key, err)
			lfsError(w, http.StatusInternalServerError, "object registry unavailable")
			return
		}
	}
	if s.PackQuotaBytes > 0 && n > 0 && !registered {
		floor, err := s.CachedUsageFloor(ctx, repoID)
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
		dec, err := s.Store.ReservePackBytes(rsvCtx, blob.QuotaRepoGroup(repoID),
			[]ref.Pack{{Key: key, Size: n}}, ref.NewReservationOwner(),
			floor, s.PackQuotaBytes, s.lfsReservationTTL())
		rsvCancel()
		if err != nil {
			log.Printf("lfs quota: %s: pre-publish refresh failed: %v", repoID, err)
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
			lfsError(w, http.StatusInsufficientStorage, fmt.Sprintf(
				"repository storage quota exceeded: upload adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight uploads), quota is %d bytes",
				n, dec.UsedBytes, dec.ReservedBytes, s.PackQuotaBytes))
			return
		}
		resGens = dec.Gens
	}
	pubCtx, cancel := context.WithDeadline(ctx, pubAnchor.Add(s.lfsReservationTTL()*9/10))
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
	ectx, ecancel := context.WithDeadline(context.WithoutCancel(ctx), pubAnchor.Add(s.lfsReservationTTL()*9/10))
	defer ecancel()
	verifyErr := func() error {
		_, err := s.Blob.Stat(ectx, key)
		if errors.Is(err, blob.ErrNotFound) {
			return s.Blob.UploadFile(ectx, key, path)
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
	w.WriteHeader(http.StatusOK)
}
