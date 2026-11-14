// Package githttp is the git smart HTTP surface (docs/explanation/git-protocol-and-namespaces.md): stateless HTTP
// nodes with Postgres as ref authority, S3/MinIO as immutable pack storage,
// and local disk as a rebuildable cache. Authentication follows docs/reference/authentication.md:
// customer-signed JWTs verified against the issuing org's registered keys.
package githttp

import (
	"bytes"
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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// assertf is a fail-fast invariant check for conditions that indicate a
// programming error (never for request-dependent failures, which must map
// to 4xx responses).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("githttp assertion failed: "+format, args...))
	}
}

// isTimeoutError reports network/request read deadlines without depending on
// a concrete net.Error type; both gzip.NewReader and later decode reads can
// surface the connection's timeout (Codex final review).
func isTimeoutError(err error) bool {
	var timeoutErr interface{ Timeout() bool }
	return errors.As(err, &timeoutErr) && timeoutErr.Timeout()
}

// Server is one stateless HTTP node: all durable truth lives in PG (refs,
// pack registry) and S3 (pack bytes); CacheDir is rebuildable at any time.
type Server struct {
	Store    ref.RefStore
	Blob     blob.BlobStore
	CacheDir string
	// Verifier authenticates docs/reference/authentication.md JWTs (multi-algorithm, iss-resolved
	// org keys, exp/sub checks). Any Verify failure maps to 401.
	Verifier auth.Verifier
	// Org is the single organization this node serves. A valid token from a
	// *different* registered org must not grant access (403); unknown orgs
	// already fail key resolution inside Verifier (401).
	Org string
	// TestMode gates fault-injection knobs (crash header). Only ever set
	// from OPENSTORAGE_TEST_MODE=1; the crash header is refused otherwise.
	TestMode bool
	// ResolveRepo maps a base path element (docs/explanation/git-protocol-and-namespaces.md: repo_id, but ADR-21
	// accepts name-style urls too) to its live repo record. ok=false when
	// no live repo matches — both handlers then answer 404 (repos must be
	// created via POST /api/v1/repos first). The record's RepoID is the
	// only storage key (repo-key-split fix); it also lets tokens carrying
	// either the url or the repo_id claim (docs/reference/authentication.md) authenticate. When
	// nil, every path element is unresolvable.
	ResolveRepo func(ctx context.Context, base string) (meta.Repo, bool)
	// MaybeRepack, when set, runs after every receive-pack POST with the
	// resolved storage key (issue #1): pre-receive registers pushed packs
	// strictly before receive-pack returns, so this is the git surface's
	// post-RegisterPacks repack-threshold trigger. It must be quick
	// (a pack-count read plus at most one job enqueue) and must never
	// block on the fold itself. Nil disables triggering.
	MaybeRepack func(ctx context.Context, repo string)
	// repackProbeMu guards the asynchronous post-push repack probe
	// machinery (see dispatchRepackProbe): repackProbeQueued marks repos
	// queued or running (per-repo coalescing), repackProbeRunning the
	// subset whose probe is executing right now, repackProbeAgain their
	// one trailing re-run, repackProbeQueue feeds the fixed worker pool,
	// and repackProbeWorkers starts that pool once.
	repackProbeMu      sync.Mutex
	repackProbeQueued  map[string]bool
	repackProbeRunning map[string]bool
	repackProbeAgain   map[string]bool
	repackProbeQueue   chan string
	repackProbeWorkers sync.Once
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
	// be internal). Set by the serve wiring from $OPENSTORAGE_BASE_URL
	// (or the bound listener); when empty, hrefs fall back to the
	// request's Host and X-Forwarded-Proto.
	BaseURL string
	// MaxPushBytes caps the DECODED bytes one receive-pack request may
	// stream into git (issue #55, pushcap.go); <= 0 disables the cap. Set
	// by the serve wiring (default: a small multiple of the pack quota).
	MaxPushBytes int64
	// uploadPackReadTimeout overrides the fixed gzip negotiation body-read
	// deadline in tests; zero uses defaultUploadPackReadTimeout. It is not an
	// operator knob — production always leaves it zero.
	uploadPackReadTimeout time.Duration
	// VerifySigBudget caps how many commits one push may signature-verify
	// (issue #59, verifystate.go); <= 0 disables the budget. Set by the
	// serve wiring.
	VerifySigBudget int
	// keyFailures rate-limits the log lines of key failures (see
	// verifyFailure).
	keyFailures KeyFailureLog
	// usage memoizes per-repo pack usage for the quota-pressure log so
	// replayed cheap POSTs cannot amplify into per-request blob listings.
	usage usageCache
	// LFSStagingBytes bounds the aggregate bytes of proxy LFS uploads
	// staging on this replica at once (issue #51, Codex round 4): uploads
	// stage their full Content-Length under $TMPDIR before verification,
	// and the per-repository quota is neither node-wide nor guaranteed
	// enabled, so without this budget concurrent distinct-key uploads
	// could exhaust a replica's disk. Set by the serve wiring
	// ($OPENSTORAGE_LFS_STAGING_BYTES, default 10 GiB); <= 0 disables the
	// bound (the operator's explicit opt-out).
	LFSStagingBytes int64
	// LFSV020WriteCompat keeps the v0.2.0 LFS read surface alive during a
	// rolling upgrade (ADR-41, Codex round 80): old replicas derive only
	// the legacy single-key spelling, so a mixed-version fleet would 404
	// objects that exist only canonically. When set (serve wiring,
	// $OPENSTORAGE_LFS_V020_WRITE_COMPAT), every verified publication
	// also copies the bytes to the legacy key — rowless copies the orphan
	// sweep reclaims after the flag is dropped post-roll. Off by default:
	// the canonical spelling is the secure one, and the compat copies are
	// exactly as forgeable as everything v0.2.0 served.
	LFSV020WriteCompat bool
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
	lfsRepoStagingOps  map[string]int
	// lfsRepoStagingBytes is each base repository's share of
	// lfsStagingInFlight (Codex round 38): the global byte budget alone
	// let one repository's transferred-and-dripping uploads 429 every
	// other repository — see lfsRepoStagingByteShareDiv. Guarded by
	// lfsStagingMu.
	lfsRepoStagingBytes map[string]int64
	// lfsVerifyOps / lfsRepoVerifyOps count in-flight legacy-verification
	// buffers (node-wide and per base repo — Codex rounds 26+33); guarded
	// by lfsStagingMu like the op counters.
	lfsVerifyOps     int
	lfsRepoVerifyOps map[string]int
	lfsStagingMu     sync.Mutex
	// lfsVerifyResumes holds partially completed adoption hashes by blob
	// key (Codex round 57): an aborted verification saves its offset and
	// sha256 midstate so the next attempt continues instead of replaying
	// completed ranges from offset zero. Guarded by lfsVerifyResumeMu;
	// bounded by lfsMaxVerifyResumes.
	lfsVerifyResumes  map[string]lfsVerifyResume
	lfsVerifyResumeMu sync.Mutex
}

// lfsMaxStagingOps bounds concurrent staging operations per replica
// (uploads and legacy verifications alike), independent of bytes. A var so
// tests can shrink it.
var lfsMaxStagingOps = 256

// lfsMaxRepoStagingOps bounds ONE base repository's share of the op slots
// (Codex round 32, security): tokens are repo-scoped, so a writer parking
// bodyless PUTs for distinct OIDs — each slot held up to the idle deadline
// — could otherwise occupy the whole replica-global cap with ~256 live
// connections and starve every OTHER repository's uploads and adoptions.
// Capping the per-repo share confines that outage to the attacker's own
// repository; unrelated repositories retain the remaining slots. A var so
// tests can shrink it.
var lfsMaxRepoStagingOps = 32

// lfsRepoStagingShareDiv scales a repository's slot allowance by the
// pool's FREE capacity (Codex round 37, security): the fixed shares above
// still summed to the global cap, so an actor holding git:write tokens
// for just 8 repositories — the wire carries no finer caller identity;
// `sub` is free text that any token signer picks — could park
// 8 × 32 bodyless PUTs and starve the node. Each new slot must fit
// min(lfsMaxRepoStagingOps, max(1, free/lfsRepoStagingShareDiv)), so
// allowances shrink as the pool fills: a k-repo coalition equilibrates
// near cap·k/(div+k) — half the pool for the 8-repo attack, leaving every
// other repository a ≥free/div allowance — and SATURATING the pool takes
// a coalition on the order of the cap itself (~250 write-scoped repos),
// not 8. A quiet node still grants a single busy repository ~cap/(div+1)
// (~28) concurrent transfers. Exhaustion by an arbitrarily large
// authorized coalition is inherent to any finite pool; the residual is
// accepted and edge rate limiting remains the operator lever beyond it.
const lfsRepoStagingShareDiv = 8

// reserveLFSStaging reserves n bytes of the node-wide LFS staging budget
// and one staging-operation slot charged to repo's share. ok=false when
// the node cap, the repo share, or the byte budget does not fit right now
// (transient — the caller answers 429). n larger than the whole byte
// budget can never fit; the caller distinguishes that case up front. The
// op slot is counted even for n==0 and with the byte budget disabled: the
// operation count bounds goroutines/fds/temp files, which exist regardless
// of declared size.
func (s *Server) reserveLFSStaging(repo string, n int64) bool {
	assertf(repo != "" && n >= 0, "reserveLFSStaging(%q, %d)", repo, n)
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	if s.lfsStagingOps >= lfsMaxStagingOps {
		return false
	}
	// The repo allowance scales with free capacity — see
	// lfsRepoStagingShareDiv. The floor of 1 keeps a first slot available
	// to any repository while the global cap has room.
	free := lfsMaxStagingOps - s.lfsStagingOps
	if s.lfsRepoStagingOps[repo] >= min(lfsMaxRepoStagingOps, max(1, free/lfsRepoStagingShareDiv)) {
		return false
	}
	if s.LFSStagingBytes > 0 && n > 0 && s.lfsStagingInFlight > s.LFSStagingBytes-n {
		return false
	}
	s.lfsStagingOps++
	if s.lfsRepoStagingOps == nil {
		s.lfsRepoStagingOps = make(map[string]int)
	}
	s.lfsRepoStagingOps[repo]++
	if s.LFSStagingBytes > 0 && n > 0 {
		s.lfsStagingInFlight += n
		if s.lfsRepoStagingBytes == nil {
			s.lfsRepoStagingBytes = make(map[string]int64)
		}
		s.lfsRepoStagingBytes[repo] += n
	}
	return true
}

// lfsProbeTimeout bounds the PUT path's pre-body blob probe (a var for
// tests): the dedup Stat runs while holding a node-wide op slot (Codex
// round 25), so a wedged backend must return the slot within a bounded
// window instead of letting 256 stuck probes disable the surface for as
// long as clients keep their connections open.
var lfsProbeTimeout = time.Minute

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

// lfsVerifyChunkTimeout bounds ONE ranged read of an adoption
// verification hash (Codex security round 56; a var for tests). The
// hash's absolute bound scales with the object so it can always finish
// (verifyStoredLFSObject); this per-chunk stall guard is what protects
// the op slot and verify buffer from a WEDGED backend — the slots come
// back within one chunk window, not the whole size-scaled allowance. An
// abort never discards progress: the saved resume state (Codex round 57)
// lets the next attempt continue after the completed ranges.
var lfsVerifyChunkTimeout = time.Minute

// lfsMaxVerifyBuffers caps concurrent legacy-verification buffer slots
// node-wide (Codex round 26, security): each in-flight verification holds
// one lfsStagingChunk-sized ReadRange slice in HEAP, a resource the
// disk-sized staging budget cannot meaningfully meter. 32 slots bound
// verification memory at ~256 MiB with the default 8 MiB chunk.
const lfsMaxVerifyBuffers = 32

// lfsMaxRepoVerifyBuffers caps ONE base repository's share of the
// verification buffers (Codex round 33, security): the buffer pool and the
// per-repo op-slot share are both 32, so one repository's concurrent
// adoptions could otherwise occupy every process-wide verification slot
// and 429 other repositories' legacy adoptions. Eight slots keep a single
// repository's upgrade traffic brisk while confining its outages to
// itself.
const lfsMaxRepoVerifyBuffers = 8

// lfsRepoVerifyShareDiv scales one repository's verification-buffer
// allowance by the pool's free capacity (Codex round 38, security): the
// fixed 8-slot shares summed to the 32-slot pool, so git:write tokens for
// four repositories could park long adoptions and 429 every other
// repository's legacy uploads on an upgrade node. Same construction as
// lfsRepoStagingShareDiv: a k-repo coalition equilibrates near
// pool·k/(div+k) and every victim keeps a free/div allowance, while a
// quiet node still grants a single repository its full 8-slot share
// (32/4 = 8).
const lfsRepoVerifyShareDiv = 4

// reserveLFSVerifyBuffer takes one node-wide verification buffer slot
// charged to repo's share; ok=false is transient (429).
func (s *Server) reserveLFSVerifyBuffer(repo string) bool {
	assertf(repo != "", "reserveLFSVerifyBuffer(%q)", repo)
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	if s.lfsVerifyOps >= lfsMaxVerifyBuffers {
		return false
	}
	free := lfsMaxVerifyBuffers - s.lfsVerifyOps
	if s.lfsRepoVerifyOps[repo] >= min(lfsMaxRepoVerifyBuffers, max(1, free/lfsRepoVerifyShareDiv)) {
		return false
	}
	s.lfsVerifyOps++
	if s.lfsRepoVerifyOps == nil {
		s.lfsRepoVerifyOps = make(map[string]int)
	}
	s.lfsRepoVerifyOps[repo]++
	return true
}

// releaseLFSVerifyBuffer returns a reserveLFSVerifyBuffer slot.
func (s *Server) releaseLFSVerifyBuffer(repo string) {
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	s.lfsVerifyOps--
	assertf(s.lfsVerifyOps >= 0, "releaseLFSVerifyBuffer: count went negative")
	s.lfsRepoVerifyOps[repo]--
	assertf(s.lfsRepoVerifyOps[repo] >= 0, "releaseLFSVerifyBuffer(%q): repo count went negative", repo)
	if s.lfsRepoVerifyOps[repo] == 0 {
		delete(s.lfsRepoVerifyOps, repo)
	}
}

// lfsStagingQuantum is the effective staging growth quantum and share
// floor: the default chunk, scaled DOWN to a third of the byte budget
// when the budget is smaller (Codex round 55, security). With a budget at
// or below one default chunk, the chunk-floored share allowance covered
// the whole pool — a header-only PUT declaring the budget then reserved
// every byte before reading any body and starved all repositories for the
// idle minute, renewable once a minute. Scaling the quantum keeps the
// share floor (and the 413 capability bound it implies) strictly below
// the pool, so a victim repository always retains admissible capacity.
func (s *Server) lfsStagingQuantum() int64 {
	if s.LFSStagingBytes > 0 {
		return min(lfsStagingChunk, max(1, s.LFSStagingBytes/(lfsRepoStagingByteShareDiv+1)))
	}
	return lfsStagingChunk
}

// lfsRepoStagingByteShareDiv scales one repository's staged-BYTE
// allowance by the byte budget's free capacity, the byte analog of
// lfsRepoStagingShareDiv (Codex round 38): with only the global budget, a
// writer for one repository could transfer LFSStagingBytes of real data
// and drip inside its reserved chunks until the absolute deadline —
// invalid digests make the traffic repeatable without consuming quota or
// object-count entries — 429ing every other repository's uploads despite
// the op-slot shares. Each grow must fit
// max(lfsStagingChunk, freeBytes/div); bytes get a SMALLER divisor than
// op slots because the byte allowance bounds the largest stageable
// object: a solo repository's hold stalls at the B/(div+1) fixed point
// (div 2 → B/3; the 413 pre-check advertises exactly that bound), so the
// default budget must be ≈(div+1)× the largest expected object — see
// defaultLFSStagingBytes. A k-repo coalition equilibrates near
// B·k/(div+k), leaving every victim a free/div allowance; the residual
// (arbitrarily many authorized repos exhausting any finite budget) is
// accepted, with edge rate limiting the operator lever beyond it.
const lfsRepoStagingByteShareDiv = 2

// growLFSStaging adds n bytes to repo's existing staging reservation
// whose op slot is already held; the caller releases the grown total
// through one releaseLFSStaging call.
func (s *Server) growLFSStaging(repo string, n int64) bool {
	assertf(repo != "" && n > 0, "growLFSStaging(%q, %d)", repo, n)
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	if s.LFSStagingBytes > 0 {
		if s.lfsStagingInFlight > s.LFSStagingBytes-n {
			return false
		}
		// The repo byte allowance scales with the budget's free capacity
		// — see lfsRepoStagingByteShareDiv. The chunk floor keeps a
		// minimal upload growable while the global budget has room.
		free := s.LFSStagingBytes - s.lfsStagingInFlight
		if s.lfsRepoStagingBytes[repo] > max(s.lfsStagingQuantum(), free/lfsRepoStagingByteShareDiv)-n {
			return false
		}
		s.lfsStagingInFlight += n
		if s.lfsRepoStagingBytes == nil {
			s.lfsRepoStagingBytes = make(map[string]int64)
		}
		s.lfsRepoStagingBytes[repo] += n
	}
	return true
}

// shrinkLFSStaging returns growLFSStaging bytes WITHOUT releasing an
// operation slot (Codex round 120 on #104): work that meters extra bytes
// against its caller's already-held slot — the mismatch snapshot — must
// not decrement the op count the caller still owns.
func (s *Server) shrinkLFSStaging(repo string, n int64) {
	assertf(repo != "" && n > 0, "shrinkLFSStaging(%q, %d)", repo, n)
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	if s.LFSStagingBytes > 0 {
		s.lfsStagingInFlight -= n
		assertf(s.lfsStagingInFlight >= 0, "shrinkLFSStaging(%d): in-flight went negative", n)
		s.lfsRepoStagingBytes[repo] -= n
		assertf(s.lfsRepoStagingBytes[repo] >= 0, "shrinkLFSStaging(%q, %d): repo bytes went negative", repo, n)
		if s.lfsRepoStagingBytes[repo] == 0 {
			delete(s.lfsRepoStagingBytes, repo)
		}
	}
}

// releaseLFSStaging returns a reserveLFSStaging reservation.
func (s *Server) releaseLFSStaging(repo string, n int64) {
	s.lfsStagingMu.Lock()
	defer s.lfsStagingMu.Unlock()
	s.lfsStagingOps--
	assertf(s.lfsStagingOps >= 0, "releaseLFSStaging: op count went negative")
	s.lfsRepoStagingOps[repo]--
	assertf(s.lfsRepoStagingOps[repo] >= 0, "releaseLFSStaging(%q): repo op count went negative", repo)
	if s.lfsRepoStagingOps[repo] == 0 {
		delete(s.lfsRepoStagingOps, repo) // bound the map by active repos
	}
	if s.LFSStagingBytes > 0 && n > 0 {
		s.lfsStagingInFlight -= n
		assertf(s.lfsStagingInFlight >= 0, "releaseLFSStaging(%d): in-flight went negative", n)
		s.lfsRepoStagingBytes[repo] -= n
		assertf(s.lfsRepoStagingBytes[repo] >= 0, "releaseLFSStaging(%q, %d): repo bytes went negative", repo, n)
		if s.lfsRepoStagingBytes[repo] == 0 {
			delete(s.lfsRepoStagingBytes, repo) // bound the map by active repos
		}
	}
}

// repoTarget is a resolved request target: the live repo record plus the
// canonical storage key — RepoID, with a `+ephemeral` suffix reattached for
// the ephemeral remote. The `+import` remote keys the base RepoID: an import
// push writes normal branches (code.storage docs/repos/imports), and
// viaImport marks it for the read refusal and the hook. The key is the ONLY
// value handed to hydrate, the pack/ref stores, and the update hook env; the
// raw path element must never key storage (ADR-21).
type repoTarget struct {
	repo      meta.Repo
	key       string
	viaImport bool
}

// importSuffix is the URL suffix of the push-only import remote.
const importSuffix = "+import"

// importReadRefusal is upstream's text for a clone or fetch through the
// import remote (code.storage docs/repos/imports, "Caveats").
const importReadRefusal = "reads are disabled for +import remotes; use the repository remote for clone/fetch"

// refuseImportRead answers a clone or fetch through the import remote with
// 403 text/plain: git prints a text/plain body as "remote:" lines, while it
// hides a JSON one.
func refuseImportRead(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusForbidden)
	_, _ = io.WriteString(w, importReadRefusal+"\n")
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
	if suffix == importSuffix {
		return repoTarget{repo: repo, key: repo.RepoID, viaImport: true}, true
	}
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
		TestMode: envconfig.Getenv(EnvTestMode) == "1",
	}
}

func httpError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// Handler routes the smart-HTTP and LFS endpoints per repo (docs/explanation/git-protocol-and-namespaces.md):
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
// which may carry a `+ephemeral`/`+import` namespace suffix (docs/explanation/git-protocol-and-namespaces.md).
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

// authenticate enforces the docs/reference/authentication.md failure mapping on the git path: 401 for
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
		return nil, http.StatusUnauthorized, s.verifyFailure(ctx, err)
	}
	// A signature valid for some *other* registered org must not grant
	// access to this org's repos.
	if claims.Iss != s.Org {
		return nil, http.StatusForbidden, fmt.Errorf("token issued for org %q, not served here", claims.Iss)
	}
	// Git endpoints are always repo-scoped (docs/reference/authentication.md): an org-level or empty
	// `repo` claim never grants git access — not even to a repo literally
	// named "org", which would otherwise satisfy the verbatim compare
	// below. Mirrors the API middleware's repo-scope gate.
	if claims.Repo == "" || claims.Repo == auth.RepoClaimOrg {
		return nil, http.StatusForbidden, fmt.Errorf("token is not repo-scoped")
	}
	// docs/reference/authentication.md: the repo claim carries the repo url (or id); docs/explanation/git-protocol-and-namespaces.md paths
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
//
// A push run's loopback fetch (an upload-pack whose token carries a sync
// claim) hydrates strictly: a held ref the cache has not caught up with
// fails it with ErrHeldRefStale, which hydrateFailed answers with 423
// Locked, so the run restores its request instead of forwarding the stale
// value (ADR-50).
func (s *Server) hydrate(ctx context.Context, repo, service, defaultBranch string, claims *auth.Claims) (string, error) {
	assertf(claims != nil, "hydrate: nil claims")
	strict := service == "upload-pack" && claims.SyncClaim != ""
	repoPath, refsErr, err := hydrateRefs(ctx, s.Store, s.Blob, s.CacheDir, repo, defaultBranch, strict)
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

// hydrateFailed answers a failed hydrate. ErrHeldRefStale is 423 Locked:
// the push run that asked restores its request and retries once the
// push that holds the ref has published or exited.
func hydrateFailed(w http.ResponseWriter, repo string, err error) {
	log.Printf("hydrate %s: %v", repo, err)
	if errors.Is(err, ErrHeldRefStale) {
		httpError(w, http.StatusLocked, "a push holds a ref this fetch would serve stale; retry")
		return
	}
	httpError(w, http.StatusInternalServerError, "hydration failed")
}

func pktLine(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }

// persistentHiddenRefs is the ref prefix that the persistent remote hides
// (docs/explanation/git-protocol-and-namespaces.md: persistent remotes must
// not see ephemeral refs). The API stores its ephemeral branches under the
// base repository as refs/namespaces/ephemeral/... (auth.EphemeralRefName).
const persistentHiddenRefs = "refs/namespaces/"

// persistentKey reports whether storage key repo is a persistent remote:
// the base repository's own key. An import push uses that key too
// (resolveTarget), so these rules cover it.
func persistentKey(repo string) bool { return BaseRepo(repo) == repo }

// serviceCmd builds the upload-pack or receive-pack child of every git
// smart-HTTP request. On the persistent remote, transfer.hideRefs leaves
// persistentHiddenRefs out of both advertisements, and receive-pack refuses
// each update or delete of such a ref with its own ng ("deny updating a
// hidden ref"). The settings change no other git command: hydration, REST
// reads and LFS reachability read those refs directly.
//
// upload-pack also serves no want of a hidden ref's commit, whatever the
// repository, global or system git config says: with the tip and reachable
// bits off, git also ignores uploadpack.allowAnySHA1InWant. (HEAD never
// points at a hidden ref of a persistent key: see hydrateRefs.)
func serviceCmd(ctx context.Context, target repoTarget, service string, args ...string) *exec.Cmd {
	assertf(service == "upload-pack" || service == "receive-pack", "serviceCmd: unknown service %q", service)
	var gitArgs []string
	if persistentKey(target.key) {
		gitArgs = []string{
			"-c", "transfer.hideRefs=" + persistentHiddenRefs,
			"-c", "uploadpack.allowTipSHA1InWant=false",
			"-c", "uploadpack.allowReachableSHA1InWant=false",
		}
	}
	gitArgs = append(gitArgs, service, "--stateless-rpc")
	cmd := exec.CommandContext(ctx, "git", append(gitArgs, args...)...)
	// The server speaks protocol v0 only: v2's fetch serves any want, so a
	// GIT_PROTOCOL in serve's own environment would undo the want settings.
	cmd.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "GIT_PROTOCOL=") })
	return cmd
}

func (s *Server) handleInfoRefs(w http.ResponseWriter, r *http.Request, repo string) {
	svcParam := r.URL.Query().Get("service")
	if svcParam != "git-upload-pack" && svcParam != "git-receive-pack" {
		httpError(w, http.StatusBadRequest, "dumb protocol not supported; use service=git-upload-pack|git-receive-pack")
		return
	}
	service := strings.TrimPrefix(svcParam, "git-")
	claims, status, err := s.authenticate(r, repo, serviceScope(service))
	if err != nil {
		// RFC 9110 §11.6.1: the challenge belongs to 401 only; a 403 means
		// the presented credentials were understood but denied.
		if status == http.StatusUnauthorized {
			w.Header().Set("WWW-Authenticate", `Basic realm="open.storage"`)
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
	if service == "upload-pack" && target.viaImport {
		refuseImportRead(w)
		return
	}
	repoPath, err := s.hydrate(r.Context(), target.key, service, target.repo.DefaultBranch, claims)
	if err != nil {
		hydrateFailed(w, target.key, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-"+svcParam+"-advertisement")
	w.Header().Set("Cache-Control", "no-cache")
	if _, err := io.WriteString(w, pktLine("# service="+svcParam+"\n")+"0000"); err != nil {
		return
	}
	cmd := serviceCmd(r.Context(), target, service, "--advertise-refs", repoPath)
	cmd.Stdout = w
	cmd.Stderr = os.Stderr
	if err := gitlimit.RunIn(r.Context(), repoPath, cmd); err != nil {
		log.Printf("advertise %s %s: %v", service, repo, err)
		abortIfStopped(err)
	}
}

func (s *Server) handleServiceRPC(w http.ResponseWriter, r *http.Request, repo, service string) {
	claims, status, err := s.authenticate(r, repo, serviceScope(service))
	if err != nil {
		// RFC 9110 §11.6.1: challenge on 401 only (see handleInfoRefs).
		if status == http.StatusUnauthorized {
			w.Header().Set("WWW-Authenticate", `Basic realm="open.storage"`)
		}
		httpError(w, status, err.Error())
		return
	}
	// Fault-injection is only ever honored under OPENSTORAGE_TEST_MODE=1;
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
	if service == "upload-pack" && target.viaImport {
		refuseImportRead(w)
		return
	}
	// Decoded-byte ingress cap (issue #55, pushcap.go): bound the bytes a
	// push may stream into git BEFORE receive-pack quarantines them on
	// disk. Gzip pushes are bounded on BOTH sides of the decoder — the
	// compressed wire stream (wireCap; empty gzip members decode to
	// nothing, so the decoded counter alone cannot bound wire consumption)
	// and the decoded stream (pushCap, which a gzip bomb inflates past).
	// The decoded counter sits under the atomic sniffer below, so the teed
	// prefix is counted exactly once.
	capped := service == "receive-pack" && s.MaxPushBytes > 0
	gzipped := r.Header.Get("Content-Encoding") == "gzip"
	// Gzip upload-pack negotiation bodies get the same two-sided caps at a
	// fixed limit, resolved BEFORE git runs (issue #70, uploadcap.go).
	// Plain upload-pack bodies stay unbuffered: without a decoder the
	// server does no work git does not see, and git ends garbage streams
	// itself by closing stdin.
	uploadCapped := service == "upload-pack" && gzipped
	capMsg := ""
	switch {
	case capped:
		capMsg = pushTooLargeMsg(s.MaxPushBytes)
	case uploadCapped:
		capMsg = uploadPackTooLargeMsg()
	}
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
			httpError(w, http.StatusRequestEntityTooLarge, capMsg)
			return
		}
	}
	if uploadCapped && r.ContentLength > wirePushLimit(maxUploadPackBytes) {
		httpError(w, http.StatusRequestEntityTooLarge, capMsg)
		return
	}
	// Bound under-cap clients that stall between gzip members or before EOF.
	// Production's statusRecorder unwraps to net/http's deadline-capable
	// writer; unsupported test/custom writers keep functional cap coverage.
	uploadReadDeadlineSet := false
	clearUploadReadDeadline := func() {
		if !uploadReadDeadlineSet {
			return
		}
		if err := http.NewResponseController(w).SetReadDeadline(time.Time{}); err != nil {
			log.Printf("%s %s: clear negotiation read deadline: %v", service, repo, err)
		}
		uploadReadDeadlineSet = false
	}
	if uploadCapped {
		readTimeout := s.uploadPackReadTimeout
		if readTimeout <= 0 {
			readTimeout = defaultUploadPackReadTimeout
		}
		assertf(readTimeout > 0, "upload-pack read timeout = %v", readTimeout)
		if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			log.Printf("%s %s: set negotiation read deadline: %v", service, repo, err)
		} else {
			uploadReadDeadlineSet = true
			defer clearUploadReadDeadline()
		}
	}
	body := io.Reader(r.Body)
	var wireCap *cappedReader
	if gzipped {
		wire := io.Reader(r.Body)
		switch {
		case capped:
			wireCap = newCappedReader(wire, wirePushLimit(s.MaxPushBytes))
			wire = wireCap
		case uploadCapped:
			wireCap = newCappedReader(wire, wirePushLimit(maxUploadPackBytes))
			wire = wireCap
		}
		gz, err := gzip.NewReader(wire)
		if err != nil {
			// An oversized optional gzip header (FNAME/FCOMMENT) can trip
			// the wire counter inside NewReader itself; that is a cap
			// overflow, not malformed compression, and must keep the 413
			// contract (Codex round 15).
			if wireCap != nil && wireCap.exceeded {
				httpError(w, http.StatusRequestEntityTooLarge, capMsg)
				return
			}
			// The body deadline can fire while NewReader is still reading
			// the fixed or optional gzip header, before io.ReadAll below.
			if uploadCapped && isTimeoutError(err) {
				httpError(w, http.StatusRequestTimeout, "fetch negotiation body timed out")
			} else {
				httpError(w, http.StatusBadRequest, "bad gzip body: "+err.Error())
			}
			return
		}
		defer gz.Close()
		body = gz
	}
	if uploadCapped {
		// Read the whole decoded negotiation in memory under both caps
		// BEFORE hydrate or git upload-pack runs (uploadcap.go). This keeps
		// 413 deterministic without replica-global temp-disk pressure.
		assertf(wireCap != nil, "uploadCapped without a wire counter")
		decodedCap := newCappedReader(body, maxUploadPackBytes)
		decoded, err := io.ReadAll(decodedCap)
		clearUploadReadDeadline()
		if decodedCap.exceeded || wireCap.exceeded {
			log.Printf("%s %s: negotiation exceeded the %d-byte cap or its wire allowance, rejected with 413", service, repo, maxUploadPackBytes)
			httpError(w, http.StatusRequestEntityTooLarge, capMsg)
			return
		}
		if err != nil {
			if isTimeoutError(err) {
				httpError(w, http.StatusRequestTimeout, "fetch negotiation body timed out")
			} else {
				httpError(w, http.StatusBadRequest, "bad gzip body: "+err.Error())
			}
			return
		}
		body = bytes.NewReader(decoded)
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
	repoPath, err := s.hydrate(r.Context(), target.key, service, target.repo.DefaultBranch, claims)
	if err != nil {
		hydrateFailed(w, target.key, err)
		return
	}

	cmd := serviceCmd(r.Context(), target, service, repoPath)
	// No-op for upload-pack; receive-pack assigns the real hold release.
	releasePushHold := func() {}
	if service == "receive-pack" {
		cmd.Env = pushHookEnv(cmd.Env, target.key, s.Org)
		if target.viaImport {
			cmd.Env = envconfig.Set(cmd.Env, EnvImportPush, "1")
		}
		// Loopback sync pushes carry the run's claim-holder token
		// (verified JWT claim, issue #86); the update hook fences the ref
		// CAS on it. Stripped above so operator env can never poison it.
		if claims.SyncClaim != "" {
			cmd.Env = envconfig.Set(cmd.Env, EnvSyncClaim, claims.SyncClaim)
		}
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
			floor, gen, err := s.cachedUsageFloorGen(r.Context(), target.key)
			if err != nil {
				log.Printf("quota: %s: usage floor for receive-pack: %v", target.key, err)
				httpError(w, http.StatusInternalServerError, "storage quota check failed")
				return
			}
			cmd.Env = envconfig.Set(cmd.Env, EnvPackQuota, strconv.FormatInt(s.PackQuotaBytes, 10))
			// The floor carries its mint time (Codex round 30): the hook
			// runs only after the client's transfer, whose duration the
			// client controls, so the hook must be able to judge the
			// floor's age rather than trust an arbitrarily stale value.
			// It also carries the shared adoption generation its scan
			// snapshotted (Codex round 49), so the hook can reject a
			// floor whose listed bytes another replica adopted while the
			// client transferred.
			cmd.Env = envconfig.Set(cmd.Env, EnvUsageFloor, strconv.FormatInt(floor, 10)+
				":"+strconv.FormatInt(time.Now().Unix(), 10)+":"+strconv.FormatInt(gen, 10))
			if s.PackReservationTTL > 0 {
				cmd.Env = envconfig.Set(cmd.Env, EnvPackReservationTTL, s.PackReservationTTL.String())
			}
		}
		if atomicPush {
			cmd.Env = envconfig.Set(cmd.Env, EnvAtomic, "1")
		}
		// The refs claim is passed through raw (docs/reference/authentication.md tuple encoding);
		// the update hook owns parsing and fails closed on malformed JSON.
		if len(claims.Refs) > 0 {
			cmd.Env = envconfig.Set(cmd.Env, EnvRefsPolicy, string(claims.Refs))
		}
		// Scopes feed the hook-side forward_on_push poke's git:read gate
		// (see EnvScopes doc); receive-pack callers always carry at least
		// git:write, so the marshal is never empty in practice.
		if len(claims.Scopes) > 0 {
			scopes, err := json.Marshal(claims.Scopes)
			assertf(err == nil, "marshal scopes claim: %v", err)
			cmd.Env = envconfig.Set(cmd.Env, EnvScopes, string(scopes))
		}
		// Deprecated repo-wide ops claim: folded into the ref policies by
		// the hook (docs/reference/authentication.md fallback semantics).
		if len(claims.Ops) > 0 {
			ops, err := json.Marshal(claims.Ops)
			assertf(err == nil, "marshal ops claim: %v", err)
			cmd.Env = envconfig.Set(cmd.Env, EnvOps, string(ops))
		}
		if s.TestMode && crash != "" {
			cmd.Env = envconfig.Set(cmd.Env, EnvCrash, crash)
		}
		// Per-push verify-sig verdict state (issue #59, verifystate.go):
		// fresh files per receive-pack request, appended to by the
		// pre-receive gate and every update hook. Only the request scopes
		// "one push" — hooks are separate processes — so they are created
		// and removed here. Everything lives inside a PRIVATE 0700 temp
		// directory: the hooks derive sibling paths (the range sidecar and
		// walk counter) from the state path by name, and on a host whose
		// shared temp dir admits other local users, predictable siblings
		// next to a visible random name could otherwise be pre-created
		// with forged verdicts (Codex round 13).
		stateDir, err := os.MkdirTemp("", "openstorage-verify-sig-*")
		if err != nil {
			log.Printf("%s %s: verify state: %v", service, repo, err)
			httpError(w, http.StatusInternalServerError, "push setup failed")
			return
		}
		defer os.RemoveAll(stateDir)
		statePath := filepath.Join(stateDir, "state")
		if err := os.WriteFile(statePath, nil, 0o600); err != nil {
			log.Printf("%s %s: verify state: %v", service, repo, err)
			httpError(w, http.StatusInternalServerError, "push setup failed")
			return
		}
		cmd.Env = envconfig.Set(cmd.Env, EnvVerifyState, statePath)
		// Hook errors (hookerrors.go): the store errors that the hooks
		// must not show the pusher. serve logs them after receive-pack
		// ends, before the state directory goes (defers run last first).
		hookErrors, err := createHookErrors(stateDir)
		if err != nil {
			log.Printf("%s %s: hook errors: %v", service, repo, err)
			httpError(w, http.StatusInternalServerError, "push setup failed")
			return
		}
		defer logHookErrors(service+" "+repo, hookErrors)
		cmd.Env = envconfig.Set(cmd.Env, EnvHookErrors, hookErrors)
		if s.VerifySigBudget > 0 {
			cmd.Env = envconfig.Set(cmd.Env, EnvVerifySigBudget, strconv.Itoa(s.VerifySigBudget))
		}
		// Push hold (issue #170): the file this push's update hooks record
		// their in-flight refs in, so concurrent full syncs skip those refs
		// until receive-pack has performed its own local ref updates. The
		// PRIMARY release is the post-receive hook (Codex round-22 P2:
		// it runs the moment the local ref transaction completes,
		// independent of response delivery — cmd.Wait below blocks on
		// the stdout copy into the client, which a non-reading client
		// can stall indefinitely). The explicit releasePushHold calls
		// after cmd.Wait/cmd.Run stay as the fallback for pushes git
		// never runs post-receive for (nothing updated), placed BEFORE
		// the repack trigger (Codex round-19 P2); the deferred call
		// covers early returns (removal is idempotent), and the TTL
		// machinery covers crash leaks.
		holdPath, err := mintPushHold(repoPath)
		if err != nil {
			log.Printf("%s %s: push hold: %v", service, repo, err)
			httpError(w, http.StatusInternalServerError, "push setup failed")
			return
		}
		// The handler owns a refresher across receive-pack's WHOLE
		// lifetime (Codex round-23 P1): the hooks' own refreshers cover
		// only their runtimes, and git's final local ref transaction runs
		// AFTER the last update hook exited — per githooks(5), update
		// fires before updating the ref, post-receive after all of them —
		// so a high-fanout atomic transaction outlasting the TTL would
		// otherwise lose its hold mid-transaction and a concurrent
		// hydrate could materialize an already-CASed authority value
		// under it, aborting the local transaction. Touches after
		// post-receive's release are ENOENT no-ops, so the client's
		// response drainage cannot extend a released hold; only a push
		// git updates NO refs for keeps its hold (normally already empty
		// — every denied command self-releases) until the request ends.
		stopHoldRefresh := startPushHoldRefresh(holdPath)
		var releaseOnce sync.Once
		releasePushHold = func() {
			releaseOnce.Do(func() {
				stopHoldRefresh()
				removePushHoldFiles(holdPath)
			})
		}
		defer releasePushHold()
		cmd.Env = envconfig.Set(cmd.Env, EnvPushHold, holdPath)
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
		if err := gitlimit.StartIn(r.Context(), repoPath, cmd); err != nil {
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
		// in bytes and by a read deadline in time, because git is already
		// done and a stalling client must not pin the handler (Codex
		// round 17; a deadline error just resolves the push from whatever
		// the counters saw) — so oversized bodies uniformly reach the 413
		// path.
		drainTimeout := pushDrainTimeout
		if s.TestMode {
			// Test knob: shrink the drain window below an injected hook
			// delay (EnvHookDelay) so the e2e lane can prove hook phases
			// outlive it (issue #141's receive-pack sibling).
			if ms := envconfig.Getenv(EnvPushDrainMS); ms != "" {
				n, err := strconv.Atoi(ms)
				assertf(err == nil && n > 0, "bad %s=%q", EnvPushDrainMS, ms)
				drainTimeout = time.Duration(n) * time.Millisecond
			}
		}
		drainDeadlineSet := false
		if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(drainTimeout)); err != nil {
			log.Printf("%s %s: set drain deadline: %v", service, repo, err)
		} else {
			drainDeadlineSet = true
		}
		// drainedToEOF tracks whether the drains actually observed the wire
		// body's EOF — the deadline may only be disarmed then (Codex P1 on
		// this PR): an overflow (errPushTooLarge) or read error stops the
		// drain with unread bytes still on the wire, the post-handler body
		// close reads more of them, and production serves without a global
		// read timeout, so a client that overflows the cap and stalls would
		// pin that goroutine indefinitely if the deadline were cleared here.
		drainedToEOF := false
		if _, err := io.Copy(io.Discard, body); err != nil {
			if !errors.Is(err, errPushTooLarge) {
				log.Printf("%s %s: request body drain: %v", service, repo, err)
			}
		} else {
			drainedToEOF = true
		}
		// The decoded drain stops at the first gzip framing error without
		// consuming the wire underneath: a valid push + corrupt member +
		// padding would evade both counters (Codex round 14). Drain the
		// compressed side too, bounded by the wire limit. Its verdict alone
		// decides EOF here: it reads the raw wire past wherever the decoded
		// drain stopped, so a clean finish means the wire is exhausted and
		// an error (cap trip, timeout, hangup) means it is not.
		if wireCap != nil {
			drainedToEOF = false
			if _, err := io.Copy(io.Discard, wireCap); err != nil {
				if !errors.Is(err, errPushTooLarge) {
					log.Printf("%s %s: wire body drain: %v", service, repo, err)
				}
			} else {
				drainedToEOF = true
			}
		}
		// The body is fully received: DISARM the deadline (issue #141's
		// receive-pack sibling). net/http's background connection read
		// starts at body EOF and sits blocked with this deadline armed; on
		// expiry it cancels r.Context() (connReader.handleReadError), and
		// CommandContext translates that into killing receive-pack mid-hook
		// phase — which can legitimately outlive any fixed drain bound (the
		// sideband-progress note above). Only server-side work remains;
		// nothing reads the body after this point.
		if drainDeadlineSet && drainedToEOF {
			if err := http.NewResponseController(w).SetReadDeadline(time.Time{}); err != nil {
				log.Printf("%s %s: clear drain deadline: %v", service, repo, err)
			}
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
		runErr := gitlimit.Wait(cmd)
		// receive-pack has exited: its local ref transaction is done and
		// nothing the holds protect can still happen — release them
		// before the repack trigger below (round-19 P2; the deferred
		// release becomes a no-op).
		releasePushHold()
		abortIfStopped(runErr)
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
					s.dispatchRepackProbe(target.key)
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
		runErr := gitlimit.RunIn(r.Context(), repoPath, cmd)
		if runErr != nil {
			// Headers are already sent; the pkt-line stream carries per-ref
			// errors to the client. Just log.
			log.Printf("%s %s: %v", service, repo, runErr)
		}
		// As on the capped path: the process has exited, so release the
		// push holds before the repack trigger below (round-19 P2).
		releasePushHold()
		abortIfStopped(runErr)
	}
	if service == "receive-pack" && s.MaybeRepack != nil {
		// Unconditionally on completion, not only on success: pre-receive
		// may have registered packs even when a later per-ref update was
		// denied. Cancel-free context — the client hanging up right after
		// its push must not suppress the trigger. Asynchronous (issue
		// #182): the probe is a store round trip, and running it before
		// the handler returns held the client's response open for it. A
		// trigger lost to process exit mid-probe is the documented
		// trade already — failures are logged, never propagated, and the
		// pack count only grows until a fold lands, so the next push
		// retries.
		s.dispatchRepackProbe(target.key)
	}
}

// repackProbeConcurrency is the fixed probe worker-pool size and
// repackProbeQueueCap its admission bound: every resource the async
// probe path holds — workers, queued repo names, coalescing map entries
// — is capped by these two constants, so a writer pushing to arbitrarily
// many distinct repos against a slow store cannot accumulate goroutines
// or map entries (Codex security round on #182's PR: a per-goroutine
// semaphore bounded executions but let admissions park unboundedly).
const (
	repackProbeConcurrency = 4
	repackProbeQueueCap    = 1024
)

// dispatchRepackProbe schedules s.MaybeRepack(repo) on the bounded probe
// worker pool. At most one probe is queued or running per repo; pushes
// completing mid-probe mark ONE trailing re-run — skipping them outright
// could leave a threshold crossing unprobed until the next push, while
// the trailing run re-reads state that includes every registration the
// mark coalesced. A FULL queue drops the probe instead of blocking or
// spawning: the trigger is documented best-effort (failures are logged,
// never propagated), the pack count only grows until a fold lands, and
// the next push to that repo re-dispatches.
func (s *Server) dispatchRepackProbe(repo string) {
	assertf(s.MaybeRepack != nil, "dispatchRepackProbe: nil MaybeRepack")
	assertf(repo != "", "dispatchRepackProbe: empty repo")
	s.repackProbeMu.Lock()
	defer s.repackProbeMu.Unlock()
	if s.repackProbeQueued == nil {
		s.repackProbeQueued = make(map[string]bool)
		s.repackProbeRunning = make(map[string]bool)
		s.repackProbeAgain = make(map[string]bool)
		s.repackProbeQueue = make(chan string, repackProbeQueueCap)
	}
	if s.repackProbeQueued[repo] {
		// A trailing re-run is needed only when the probe is already
		// EXECUTING (it may have read state before this push's
		// registration committed). A probe still parked in the queue
		// starts strictly after this dispatch and observes the
		// registration, so re-arming it would just burn a redundant
		// queue pass (Codex round 7).
		if s.repackProbeRunning[repo] {
			s.repackProbeAgain[repo] = true
		}
		return
	}
	select {
	case s.repackProbeQueue <- repo:
		s.repackProbeQueued[repo] = true
		s.repackProbeWorkers.Do(func() {
			for range repackProbeConcurrency {
				go s.repackProbeWorker()
			}
		})
	default:
		log.Printf("repack probe queue full (%d pending): dropping probe for %s (next push retries)", repackProbeQueueCap, repo)
	}
}

// repackProbeWorker drains the probe queue for the server's lifetime,
// probing on a cancel-free background context (the requests that queued
// the work have long returned). A trailing re-run is REQUEUED at the
// channel tail rather than run inline (Codex round 6): a repo hot enough
// to re-arm its mark during every probe would otherwise pin its worker,
// and a handful of such repos could occupy the whole pool and starve
// every other queued repo. The repo stays marked queued across the
// requeue, so later pushes keep coalescing onto the trailing entry; a
// full queue drops it — the same best-effort trade as dispatch.
func (s *Server) repackProbeWorker() {
	for repo := range s.repackProbeQueue {
		s.repackProbeMu.Lock()
		s.repackProbeRunning[repo] = true
		s.repackProbeMu.Unlock()
		s.MaybeRepack(context.Background(), repo)
		s.repackProbeMu.Lock()
		delete(s.repackProbeRunning, repo)
		if s.repackProbeAgain[repo] {
			delete(s.repackProbeAgain, repo)
			select {
			case s.repackProbeQueue <- repo:
				// Still marked queued: coalescing continues.
			default:
				delete(s.repackProbeQueued, repo)
				log.Printf("repack probe queue full (%d pending): dropping trailing probe for %s (next push retries)", repackProbeQueueCap, repo)
			}
		} else {
			delete(s.repackProbeQueued, repo)
		}
		s.repackProbeMu.Unlock()
	}
}

// abortIfStopped aborts the response when a git child failed after
// gitlimit.StopChildren began, so most likely by its signals: git 2.55
// push waits forever on a 200 whose stream was cut off, but fails at once
// on a dropped connection. The panic still runs the handler's deferred
// cleanup.
func abortIfStopped(err error) {
	if err != nil && gitlimit.Stopping() {
		panic(http.ErrAbortHandler)
	}
}
