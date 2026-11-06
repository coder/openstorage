// Package blob provides immutable pack-object storage: the BlobStore
// contract plus the S3/MinIO implementation (gocloud.dev s3blob).
package blob

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	gcblob "gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"

	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/s3oplog"
)

// ErrSignedURLUnsupported is returned by SignedURL on backends without
// presign support (fileblob). Callers (the LFS batch handler) fall back to
// proxying the transfer through the server.
var ErrSignedURLUnsupported = errors.New("blob: signed URLs unsupported by this backend")

// ErrNotFound is returned (wrapped, match with errors.Is) by Stat and
// DownloadFile for missing objects, so callers can distinguish absence from
// transport failures without depending on gocloud error codes.
var ErrNotFound = errors.New("blob: object not found")

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("blob assertion failed: "+format, args...))
	}
}

// BlobStore is the pack-object storage contract. Keys are opaque to callers
// except through PackKey/PackPrefix. Objects are immutable once written.
type BlobStore interface {
	// UploadFile streams a local file to key.
	UploadFile(ctx context.Context, key, path string) error
	// DownloadFile fetches key into destPath atomically (tmp + rename), so a
	// partially downloaded object is never visible at destPath. Missing keys
	// yield an error matching ErrNotFound via errors.Is (the hydration
	// missing-pack retry keys on it).
	DownloadFile(ctx context.Context, key, destPath string) error
	// DownloadFileCapped is DownloadFile bounded to capBytes of local
	// disk (Codex round 117 on #104): one GET, atomic rename — but a
	// stream exceeding capBytes aborts with ErrCapExceeded instead of
	// consuming unreserved space (the caller reserved capBytes BEFORE a
	// window in which a mutable-key object can be replaced by a larger
	// version).
	DownloadFileCapped(ctx context.Context, key, destPath string, capBytes int64) error
	// List returns all objects under prefix.
	List(ctx context.Context, prefix string) ([]ListedObject, error)
	// ListPages streams objects under prefix in lexicographic key order,
	// invoking fn once per page of at most pageSize objects, so callers
	// scanning large prefixes (the orphan-pack sweeper) hold one page at a
	// time instead of the whole listing. fn must not retain the slice; a
	// non-nil fn error aborts the scan and is returned unwrapped.
	ListPages(ctx context.Context, prefix string, pageSize int, fn func([]ListedObject) error) error
	// Delete removes the object at key; missing objects yield an error
	// matching ErrNotFound via errors.Is. Objects are immutable, so Delete
	// exists solely for the orphan-pack sweeper (issue #5): reaping blobs
	// that were uploaded but never registered because of a crash.
	Delete(ctx context.Context, key string) error
	// Copy duplicates srcKey to dstKey inside the store (server-side S3
	// CopyObject / local file copy — never staged through local disk, so
	// pack-sized copies work regardless of temp-dir capacity). A missing
	// srcKey yields an error matching ErrNotFound via errors.Is. Used by
	// the orphan-pack sweeper's quarantine handshake.
	Copy(ctx context.Context, dstKey, srcKey string) error
	// SignedURL returns a presigned URL for the given HTTP method
	// (http.MethodGet or http.MethodPut) on key, valid for expiry.
	// Backends without presign support return ErrSignedURLUnsupported.
	SignedURL(ctx context.Context, key, method string, expiry time.Duration) (string, error)
	// Stat returns the size of the object at key; missing objects yield an
	// error matching ErrNotFound via errors.Is.
	Stat(ctx context.Context, key string) (int64, error)
	// StatFingerprint returns the size plus an opaque content-version
	// fingerprint for the object at key: any overwrite changes it (S3
	// ETag — content-derived, so an attacker cannot preserve it across a
	// same-length rewrite; file backends fall back to mtime+size).
	// Verification flows that hash an object across multiple reads use
	// fingerprint equality to prove the bytes were not REPLACED between
	// reads (issue #51, Codex round 77): ranged reads spanning a swap
	// can splice old-prefix+new-suffix into a digest that matches even
	// though the stored bytes differ. Missing objects yield an error
	// matching ErrNotFound via errors.Is.
	StatFingerprint(ctx context.Context, key string) (int64, string, error)
	// ReadRange returns exactly the length bytes at offset from the object
	// at key — a ranged point read, never the whole object (repack fold
	// selection reads exact object counts from 12-byte pack headers with
	// it). Missing keys yield an error matching ErrNotFound via errors.Is;
	// an object shorter than offset+length is a non-ErrNotFound error.
	ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	// EnsureBucket idempotently creates the backing bucket.
	EnsureBucket(ctx context.Context) error
	Close() error
}

// assertSignedURLArgs fail-fast-validates the shared SignedURL contract:
// non-empty key, GET or PUT (the only methods LFS transfers need), and a
// positive expiry.
func assertSignedURLArgs(key, method string, expiry time.Duration) {
	assertf(key != "", "SignedURL: empty key")
	assertf(method == http.MethodGet || method == http.MethodPut,
		"SignedURL(%q): method %q (want GET or PUT)", key, method)
	assertf(expiry > 0, "SignedURL(%s %s): non-positive expiry %v", method, key, expiry)
}

// PageListerAfter is the optional BlobStore capability behind ListPagesAfter.
// A store that lacks it, a decorator that embeds a BlobStore for example, is
// still correct through ListPagesAfter: the helper filters its ListPages, so
// that ListPages must list in key order.
type PageListerAfter interface {
	ListPagesAfter(ctx context.Context, prefix, after string, pageSize int, fn func([]ListedObject) error) error
}

// ListPagesAfter streams the objects under prefix whose key sorts strictly
// after after, in lexicographic key order, with the page rules of
// BlobStore.ListPages. after need not be a key that exists, and an empty after
// lists everything. It lets a scan that saved its place resume without reading
// the keys it already finished, on stores that can start there.
func ListPagesAfter(ctx context.Context, b BlobStore, prefix, after string, pageSize int, fn func([]ListedObject) error) error {
	assertf(b != nil, "ListPagesAfter(%q): nil store", prefix)
	assertf(pageSize > 0, "ListPagesAfter(%q): non-positive page size %d", prefix, pageSize)
	assertf(fn != nil, "ListPagesAfter(%q): nil fn", prefix)
	if l, ok := b.(PageListerAfter); ok {
		return l.ListPagesAfter(ctx, prefix, after, pageSize, fn)
	}
	return b.ListPages(ctx, prefix, pageSize, func(page []ListedObject) error {
		kept := page[:0]
		for _, o := range page {
			if o.Key > after {
				kept = append(kept, o)
			}
		}
		if len(kept) == 0 {
			return nil
		}
		return fn(kept)
	})
}

// ListedObject is one blob under a prefix.
type ListedObject struct {
	Key  string
	Size int64
	// ModTime is the backend's last-modified time for the object (S3
	// LastModified / file mtime). The orphan-pack sweeper uses it as the
	// upload age: objects are immutable, so it never moves after publish.
	ModTime time.Time
}

// PackKey is the immutable object key for a repo's pack with the given id.
func PackKey(repo, id string) string {
	assertf(repo != "" && id != "", "PackKey(%q, %q)", repo, id)
	return "packs/" + repo + "/pack-" + id + ".pack"
}

// PacksRoot is the listing prefix covering every repo's packs (the
// orphan-pack sweeper scans it in one listing instead of enumerating
// repos). Namespaced repos (`<repo>+import`/`<repo>+ephemeral`) keep fully
// separate pack spaces keyed by the full name, so each key's repo segment
// identifies its authoritative pack registry row set.
const PacksRoot = "packs/"

// PackPrefix is the listing prefix for a repo's packs.
func PackPrefix(repo string) string { return PacksRoot + repo + "/" }

// packUsagePageSize bounds one StorageUsageBytes listing page, mirroring the
// sweeper's page-bounded scans so a pathological pack count cannot balloon
// memory.
const packUsagePageSize = 1000

// packNamespaceSuffixes are the docs/explanation/git-protocol-and-namespaces.md namespace suffixes a repo key may
// carry. Each namespace has its own pack PREFIX (storage isolation), but
// quota accounting rolls all of them up under the base repo: auth strips
// the suffix, so one git:write token can push to base, +ephemeral, and
// +import — separate quotas would hand that token 3x the advertised
// per-repository bound (issue #21, Codex round 15).
var packNamespaceSuffixes = []string{"+ephemeral", "+import"}

// QuotaRepoGroup returns repo's quota accounting group: the BASE repository
// first (suffix stripped), then every docs/explanation/git-protocol-and-namespaces.md namespace that rolls up under
// it. The ref-store quota ledger (issue #29) keys reservations under
// group[0] and sums usage over the whole group, mirroring StorageUsageBytes'
// rollup so both usage sources measure the same unit.
func QuotaRepoGroup(repo string) []string {
	assertf(repo != "", "QuotaRepoGroup: empty repo")
	base := repo
	for _, s := range packNamespaceSuffixes {
		base = strings.TrimSuffix(base, s)
	}
	assertf(base != "", "QuotaRepoGroup: empty base repo for %q", repo)
	group := make([]string, 0, 1+len(packNamespaceSuffixes))
	group = append(group, base)
	for _, s := range packNamespaceSuffixes {
		group = append(group, base+s)
	}
	return group
}

// StorageUsageBytes sums the sizes of every object under the pack prefixes
// of repo's BASE repository and all of its namespaces PLUS the repo's LFS
// prefix (issue #51): registered packs and LFS objects, not-yet-swept
// orphans, AND retained superseded blobs — exactly the durable growth the
// per-repo storage quota (issue #21) bounds. The suffix, if any, is
// stripped first, so every namespace write is measured against the same
// rolled-up usage. Shared by the pre-receive hook, the REST pack writers,
// the LFS batch/transfer gates, and the git surface's pressure check.
//
// Since issue #29 this listing is the admission check's usage FLOOR rather
// than its sole source: the ref-store ledger (ref.ReservePackBytes) charges
// max(counter, this listing) plus in-flight reservations, closing the
// concurrent-push overshoot this listing alone admitted. Retained
// superseded bytes (issue #31) are counted by BOTH sources — here as
// lingering prefix objects, there as pack_retained rows — until a sweep
// reclaims them. LFS bytes are likewise counted by both once uploads
// settle (ref.RegisterLFSObject); before #51's ledger existed, only this
// listing could see them, which is one more reason the floor stays.
func StorageUsageBytes(ctx context.Context, b BlobStore, repo string) (int64, error) {
	assertf(b != nil && repo != "", "StorageUsageBytes: missing blob store or repo")
	base := QuotaRepoGroup(repo)[0]
	// Three listings cover base + every namespace + LFS without matching
	// sibling repos: "packs/<base>/" is the base pack prefix,
	// "packs/<base>+" only matches this repo's namespace prefixes ('+'
	// cannot start another repo's key segment), and "lfs/<base>/" is the
	// shared LFS object space every namespace rolls up to (ADR-35).
	total, err := usageBytes(ctx, b, repo,
		[]string{PackPrefix(base), PacksRoot + base + "+", LFSPrefix(base)})
	assertf(err != nil || total >= 0, "StorageUsageBytes: negative total %d for %s", total, repo)
	return total, err
}

// Deliberately NO packs-only floor variant for pack admission (issue #51,
// Codex rounds 17–18): omitting the LFS prefix from the pack writers'
// floor let a repository's PRE-LEDGER LFS bytes (rowless, so invisible to
// the usage counter until adopted) admit a full quota of new packs on top.
// The full listing per push is the accepted cost: it is bounded by the
// per-repo LFS object cap (≤ ~101 pages) — the same order as the pack
// prefix listing this path always did — runs once per heavyweight push
// (the per-TRANSFER amplification lives on the LFS surface, which caches),
// and the alternative (a sweeper-reconciled per-repo baseline) requires
// exactly the per-repo sweep state the sweeper's design forbids.

// usageBytes sums object sizes under the given prefixes, page-bounded.
func usageBytes(ctx context.Context, b BlobStore, repo string, prefixes []string) (int64, error) {
	var total int64
	for _, prefix := range prefixes {
		err := b.ListPages(ctx, prefix, packUsagePageSize, func(page []ListedObject) error {
			for _, o := range page {
				total += o.Size
			}
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("list usage for %s: %w", repo, err)
		}
	}
	return total, nil
}

// LFSKey is the immutable object key for a repo's LFS object with the given
// sha256 oid. Keys are repo-scoped (docs/explanation/git-protocol-and-namespaces.md): forks inherit LFS pointers
// only, never the objects, which this keying yields automatically.
//
// LFSKey is the CANONICAL object key: bytes here are written only by the
// server's digest-verified paths (verified proxy uploads, adoption copies,
// registered-object restores) — never by presigned PUTs. The `o2/` segment
// makes the spelling unreachable by any v0.1.x presigned upload URL, which
// signed exactly the bare LFSLegacyKey spelling (Codex security round 78
// on issue #51): a still-valid legacy URL can overwrite ONLY the legacy
// key, so registered content — always canonical — is immune to replay for
// the URL's whole lifetime, with no quiesce-window reasoning. The oid is
// caller-validated 64-hex (lfsOIDPattern / isLFSOID); the assert rules out
// path tricks reaching the canonical namespace.
func LFSKey(repoID, oid string) string {
	assertf(repoID != "" && oid != "" && !strings.Contains(oid, "/"),
		"LFSKey(%q, %q)", repoID, oid)
	return LFSRoot + repoID + "/o2/" + oid
}

// LFSLegacyKey is the v0.1.x object spelling — the only one old presigned
// PUT URLs can address. It is never registered and never served for
// registered objects: it exists as the adoption SOURCE (rowless pre-#51
// blobs live here until adopted, then the source is deleted) and as the
// read-compat location for not-yet-adopted rowless objects.
func LFSLegacyKey(repoID, oid string) string {
	assertf(repoID != "" && oid != "" && !strings.Contains(oid, "/"),
		"LFSLegacyKey(%q, %q)", repoID, oid)
	return LFSRoot + repoID + "/" + oid
}

// LFSRoot is the listing prefix covering every repo's LFS objects (the
// orphan sweeper scans it like PacksRoot, issue #51). LFS keys always use
// the BASE RepoID (ADR-35): namespaces share one object space, so there is
// no "+suffix" variant to cover.
const LFSRoot = "lfs/"

// LFSPrefix is the listing prefix for a repo's LFS objects.
func LFSPrefix(repoID string) string {
	assertf(repoID != "", "LFSPrefix: empty repo")
	return LFSRoot + repoID + "/"
}

// EnsurePublished re-uploads the local file at path to key when key is
// absent. It is the writer half of the orphan-sweep coordination (issue
// #5), called strictly AFTER a successful RegisterPacks: registration
// follows upload, so an orphan sweeper that deleted the freshly uploaded
// key just before the registration committed would otherwise leave a
// registered pack with no bytes. Ordering proof: the sweeper deletes (D)
// then re-reads the registry (C); the writer registers (R) then verifies
// here (V). If C observes R the sweeper restores the pack itself; if C ran
// before R then D happened before V, so V observes the missing key and
// re-uploads. Both writers produce identical bytes (pack keys are
// content-addressed), so the overlap is harmless either way.
//
// The verification runs on a bounded context detached from the caller's
// cancellation: the registration it covers has already committed, so a
// client hanging up right after the commit must not skip the check — a
// skipped verification could leave the persistent pack row pointing at a
// blob a concurrent sweep just deleted (Codex round 14).
//
// A restore re-materializes bytes the usage counter already includes
// (Codex round 58): a quota floor scanned during the absence subtracted
// the full counter from a listing missing them, hiding equally sized
// rowless legacy bytes. onRestore (nil-ok) runs BEFORE the repair upload
// so the caller can account the returning bytes first (a cover
// reservation — see the LFS repair in githttp); an onRestore error aborts
// the restore, leaving the registered-row-without-bytes state the next
// retry heals. The returned restored flag tells the caller to invalidate
// cached floors (bump the shared floor generation) after the bytes are
// back.
// onRestored (nil-ok) is the invalidation HANDOFF (Codex round 70): it
// runs under a live, freshly proven cover and is retried — alternating
// with cover renewals — until it commits or continuity is lost, so a
// transient ledger failure cannot strand durable restored bytes with
// neither cover nor invalidation.
func EnsurePublished(ctx context.Context, b BlobStore, key, path string, onRestore func(context.Context, int64) error, onRestored func(context.Context) error) (restored bool, err error) {
	assertf(b != nil && key != "" && path != "", "EnsurePublished(%q, %q)", key, path)
	fi, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("stat local pack %s: %w", path, err)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ensurePublishedTimeout(fi.Size()))
	defer cancel()
	_, err = b.Stat(ctx, key)
	if errors.Is(err, ErrNotFound) {
		if onRestore == nil {
			if err := b.UploadFile(ctx, key, path); err != nil {
				return false, err
			}
			if onRestored != nil {
				hctx, hcancel := context.WithTimeout(context.WithoutCancel(ctx), CoverHandoffTimeout)
				defer hcancel()
				if herr := onRestored(hctx); herr != nil {
					return true, fmt.Errorf("hand off restore of %s: %w", key, herr)
				}
			}
			return true, nil
		}
		// The cover must exist BEFORE any byte becomes physical and stay
		// alive until the stale-floor tail closes (Codex rounds 58+59):
		// the restore allowance deliberately exceeds the minimum
		// reservation TTL (repairs must finish), so one reservation
		// cannot span the upload. WithRenewedCover takes the initial
		// cover itself (anchored inside its continuity discipline —
		// Codex round 64), renews it through the upload, and refreshes
		// it after publication — the refresh restarts the TTL clock,
		// which the config floor guarantees outlives the last
		// stale-floor commit (usageCacheTTL + handoff margin past
		// publication).
		renew := func(rctx context.Context) error { return onRestore(rctx, fi.Size()) }
		upErr, renewErr := WithRenewedCover(ctx, renew, func(opCtx context.Context) error {
			return b.UploadFile(opCtx, key, path)
		}, onRestored)
		if upErr != nil {
			return false, fmt.Errorf("restore %s: %w", key, upErr)
		}
		if renewErr != nil {
			// The bytes ARE durably back — report the restore — but the
			// invalidation handoff never landed under a live cover
			// (continuity was lost first): surface it, the caller logs.
			return true, fmt.Errorf("hand off restore of %s: %w", key, renewErr)
		}
		return true, nil
	}
	return false, err
}

// staleFloorTail is how long an absence-window floor can remain
// admissible after publication re-materialized the bytes (a var for
// tests): deflated floors were scanned while the bytes were absent —
// strictly before publication ended — and every consumer serves a floor
// for at most its cache lifetime (30s, githttp usageCacheTTL) plus the
// admission handoff margin (30s, githttp UsageFloorHandoffMargin). Once
// this tail passes, no floor that could hide the restored bytes can
// still commit, and abandoning cover/invalidation repairs becomes safe.
var staleFloorTail = 60 * time.Second

// CoverHandoffTimeout bounds the floor-generation bump a caller runs
// after a restore (Codex round 69): the bump must COMMIT while the last
// cover row still lives, and after a broken tail refresh that row's
// remaining lifetime can be as short as minTTL − coverLapseBudget ≈ 16s
// (the loss is reported at covered+budget; the row lives to covered+TTL).
// Ten seconds fits inside that worst case with slack, and trivially
// inside the fresh full TTL a successful tail refresh leaves.
const CoverHandoffTimeout = 10 * time.Second

// coverRenewInterval is WithRenewedCover's renewal cadence (a var for
// tests). Together with coverRenewTimeout and coverLapseBudget it keeps a
// restore cover continuously alive under the minimum reservation TTL
// (61s, see ref.MinPackReservationTTL's config gate): a single failed or
// stalled renewal still leaves the next attempt committing within the
// prior success's TTL (20s tick + 20s tick + 10s bound = 50s < 61s).
var coverRenewInterval = 20 * time.Second

// coverRenewTimeout bounds ONE in-progress renewal attempt (Codex round
// 62): an unbounded renew stalling on a wedged connection would block the
// renewal loop past the cover's TTL while the restore upload continues on
// its own healthy connection.
var coverRenewTimeout = 10 * time.Second

// ErrCapExceeded reports a capped download whose stream exceeded the
// caller's reserved size (Codex round 117 on #104): on a mutable legacy
// key, the object can be replaced by a larger version between the
// caller's Stat and the GET, and streaming past the reservation would
// consume unbudgeted replica disk.
var ErrCapExceeded = errors.New("blob: stream exceeds the reserved cap")

// ErrCoverContinuityLost marks work canceled by the cover-continuity
// machinery (Codex security round 107 on #104): callers must be able to
// distinguish a copy that erred BECAUSE its cover lapsed — whose committed
// destination is an unsafe, unaccounted commit exactly like a renewErr
// verdict — from ordinary op failures, because WithRenewedCover reports a
// canceled op through opErr with renewErr nil.
var ErrCoverContinuityLost = errors.New("cover continuity lost")

// coverLapseBudget is how long WithRenewedCover tolerates renewal
// failures before it must assume the cover row expired and ABORT the
// publication (Codex round 62): declared strictly before the minimum TTL
// (45s < 61s) so the abort lands while the last successful renewal still
// covers the bytes — an uncovered restore must never become physical.
var coverLapseBudget = 45 * time.Second

// WithRenewedCover runs op while keeping a restore's cover reservation
// alive (Codex round 59): it takes the initial cover itself (round 64),
// renews it every coverRenewInterval — each attempt deadline-bounded and
// continuity-checked — for op's duration, and after op succeeds it
// alternates fresh cover refreshes with bounded attempts of the caller's
// handoff (the floor invalidation) until the handoff COMMITS under a
// live cover or continuity is lost (Codex round 70 — a single
// best-effort handoff could fail and strand durable bytes with neither
// cover nor invalidation). Renewal failures are tolerated while a recent
// success still covers the bytes; once coverLapseBudget passes without
// one, op's context is canceled — the restore aborts rather than
// publishing uncovered bytes (Codex round 62). opErr reports op's own
// failure; renewErr reports a handoff that never landed.
func WithRenewedCover(ctx context.Context, renew func(context.Context) error, op func(context.Context) error, handoff func(context.Context) error) (opErr, renewErr error) {
	assertf(renew != nil && op != nil, "WithRenewedCover: nil renew or op")
	opCtx, opCancel := context.WithCancelCause(ctx)
	defer opCancel(nil)
	// covered is the conservative anchor of the newest PROVEN cover row:
	// each accepted attempt anchors at its own START (the row's
	// created_at is at or after it). The INITIAL cover is taken inside
	// this function under the same discipline (Codex round 64): a
	// caller-side reservation anchored at entry let a process suspension
	// between the caller's commit and entry fabricate a fresh anchor
	// over an already-aging row. Written by the renewal goroutine, read
	// by the final-refresh loop strictly after wg.Wait — no concurrent
	// access.
	var covered time.Time
	// attemptRenew runs ONE bounded, continuity-checked renewal: a
	// renewal proves continuity only if it COMPLETED while the prior
	// cover still lived (Codex round 63) — a success committing after
	// the prior row expired leaves an uncovered gap a stale-floor
	// admission can use. Completion is an upper bound on the commit
	// time; the budget is strictly under the minimum TTL, so
	// completion-before-deadline proves commit-before-expiry. The first
	// success (no prior cover to prove continuity against) is anchored
	// at its own conservative start. The context is detached: renewals
	// are restore accounting, not client work.
	attemptRenew := func() (ok, lost bool, err error) {
		start := time.Now()
		// Each attempt is capped at the REMAINING continuity budget
		// (Codex round 66): continuity is only checked when renew
		// returns, so an attempt allowed to run past covered+budget
		// would leave op uncancelled while the prior cover expires
		// under it — the full per-attempt timeout applies only while
		// the budget has that much room.
		bound := coverRenewTimeout
		if !covered.IsZero() {
			remaining := time.Until(covered.Add(coverLapseBudget))
			if remaining <= 0 {
				return false, true, errors.New("continuity budget exhausted")
			}
			bound = min(bound, remaining)
		}
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
		err = renew(rctx)
		rcancel()
		if err == nil && (covered.IsZero() || time.Since(covered) <= coverLapseBudget) {
			covered = start
			return true, false, nil
		}
		return false, !covered.IsZero() && time.Since(covered) > coverLapseBudget, err
	}
	// No bytes may become physical before the initial cover commits: an
	// initial failure aborts before op starts.
	if ok, _, err := attemptRenew(); !ok {
		return fmt.Errorf("initial restore cover: %w", err), nil
	}
	// renew invocations are strictly serialized: the renewal goroutine is
	// joined before the final refresh, so callers need no concurrency
	// handling in the callback.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(coverRenewInterval)
		defer t.Stop()
		for {
			// The budget deadline is enforced by its own timer (Codex
			// round 66): a tick-driven check alone lets the budget
			// expire BETWEEN ticks (quick failures early, the next tick
			// scheduled past covered+budget) with op still running while
			// the prior cover ages out.
			budget := time.NewTimer(time.Until(covered.Add(coverLapseBudget)))
			select {
			case <-done:
				budget.Stop()
				return
			case <-budget.C:
				opCancel(fmt.Errorf("restore %w (budget expired without a renewed cover)", ErrCoverContinuityLost))
				return
			case <-t.C:
				budget.Stop()
				if _, lost, err := attemptRenew(); lost {
					opCancel(fmt.Errorf("restore %w (last renewal: %v)", ErrCoverContinuityLost, err))
					return
				}
				// A failed attempt inside the budget: the prior cover
				// still lives, the next tick retries.
			}
		}
	})
	opErr = op(opCtx)
	close(done)
	wg.Wait()
	if opErr != nil {
		if cause := context.Cause(opCtx); cause != nil && !errors.Is(cause, context.Canceled) {
			// %w on the cause too (Codex security round 107): an op that
			// returns the bare cancellation must still let callers match
			// ErrCoverContinuityLost through opErr.
			opErr = fmt.Errorf("%w (%w)", opErr, cause)
		}
		return opErr, nil
	}
	// The MANDATORY tail refresh runs under the SAME continuity
	// discipline (Codex rounds 60+63, security): it must COMMIT while the
	// prior cover still lives — a single wide-timeout attempt could
	// finish after that row expired, and the gap before the caller's
	// generation bump is exactly the uncovered window a stale-floor
	// admission exploits. Attempts stay bounded and retry on the renewal
	// cadence while continuity holds; losing it surfaces renewErr (the
	// bytes are already durable — the caller reports the broken tail and
	// still bumps the floor generation).
	pubEnd := time.Now()
	attemptHandoff := func() error {
		if handoff == nil {
			return nil
		}
		hctx, hcancel := context.WithTimeout(context.WithoutCancel(ctx), CoverHandoffTimeout)
		defer hcancel()
		return handoff(hctx)
	}
	lossRegime := false
	var gap error // first continuity loss, if any — never forgotten
	for {
		ok, lost, err := attemptRenew()
		if ok {
			lossRegime = false // a proven cover (or re-cover) is live again
			// The handoff runs under the freshly proven cover, bounded
			// so a stalled attempt cannot outlive it (Codex rounds
			// 69+70). A failure keeps the loop alive: the cover is
			// renewed and the handoff retried until it commits — the
			// invalidation must LAND, not be logged away.
			if herr := attemptHandoff(); herr == nil {
				// A post-gap success repaired what remained repairable —
				// future admissions are covered and invalidated — but an
				// UNCOVERED WINDOW existed and a write settled in it
				// cannot be revoked (Codex round 73): never report a
				// gapped restore as clean.
				if gap != nil {
					return nil, fmt.Errorf("restore repaired after an uncovered cover gap: %w", gap)
				}
				return nil, nil
			}
			if wait := min(coverRenewInterval, time.Until(covered.Add(coverLapseBudget))); wait > 0 {
				time.Sleep(wait)
			}
			continue
		}
		if lost || lossRegime {
			// CONTINUITY LOST — but abandoning here is premature (Codex
			// round 72, security): absence-window floors scanned before
			// publication stay admissible until pubEnd+staleFloorTail,
			// and the last cover can expire before they do. Both
			// repairs remain strictly safety-improving, so keep
			// attempting through the tail: a handoff success
			// invalidates every stale floor, a renewal success
			// re-covers the bytes (the anchor is zeroed so the next
			// success re-anchors without a false continuity claim —
			// lossRegime keeps failures in this branch meanwhile). Only
			// when every floor that could hide the restored bytes has
			// expired is giving up safe.
			lossRegime = true
			if gap == nil {
				gap = fmt.Errorf("cover continuity lost after publication (last renewal: %v)", err)
			}
			if time.Since(pubEnd) > staleFloorTail {
				return nil, fmt.Errorf("restore handoff abandoned after the stale-floor tail: %w", gap)
			}
			// A nil handoff has nothing to land: only a successful
			// re-cover (above, via the zeroed anchor) or the tail's end
			// exits the loop.
			if handoff != nil {
				if herr := attemptHandoff(); herr == nil {
					return nil, fmt.Errorf("restore repaired after an uncovered cover gap: %w", gap)
				}
			}
			covered = time.Time{}
			if wait := min(coverRenewInterval, time.Until(pubEnd.Add(staleFloorTail))); wait > 0 {
				time.Sleep(wait)
			}
			continue
		}
		// Wake at the earlier of the retry cadence and the budget
		// deadline (Codex round 67, security): sleeping a full interval
		// past covered+budget deferred the loss report to ~T+60 on a
		// 20s cadence while the budget expired at T+45 — under a
		// just-over-60s TTL the cover could expire before the caller
		// learned the tail broke and bumped the floor generation. The
		// next attempt then reports the exhausted budget immediately.
		if wait := min(coverRenewInterval, time.Until(covered.Add(coverLapseBudget))); wait > 0 {
			time.Sleep(wait)
		}
	}
}

// ensurePublishedTimeout bounds EnsurePublished's detached verification.
// The bound scales with the pack size — a repair upload MUST be able to
// finish, or the committed registry row keeps pointing at an absent object
// until the next heal — while still guaranteeing a wedged store cannot pin
// request goroutines forever. The floor covers round-trips and small packs;
// the per-byte allowance assumes a worst-case sustained ~1 MiB/s to the
// store, far below any realistic server→store link.
func ensurePublishedTimeout(size int64) time.Duration {
	assertf(size >= 0, "ensurePublishedTimeout: negative size %d", size)
	const perMiB = time.Second
	return 5*time.Minute + time.Duration(size>>20)*perMiB
}

// Open dispatches on the URL scheme to the matching BlobStore backend:
//
//	s3://bucket?region=&endpoint=  → OpenS3
//	file:///abs/dir                → local filesystem bucket
//
// BlobStore already carries the lifecycle (EnsureBucket/Close), so there is
// no separate lifecycle interface. Open never calls EnsureBucket: hooks
// reconstruct stores from env URLs; the serve seam owns bucket creation.
func Open(ctx context.Context, blobURL string) (BlobStore, error) {
	assertf(blobURL != "", "Open: empty blob URL")
	u, err := url.Parse(blobURL)
	if err != nil {
		return nil, fmt.Errorf("parse blob URL: %w", err)
	}
	switch u.Scheme {
	case "s3":
		return OpenS3(ctx, blobURL)
	case "file":
		return OpenFile(blobURL)
	default:
		return nil, fmt.Errorf("blob URL scheme %q unsupported (want s3 or file)", u.Scheme)
	}
}

// S3 implements BlobStore on a gocloud S3 bucket plus the raw client
// (needed for EnsureBucket at serve startup).
type S3 struct {
	bucket *gcblob.Bucket
	client *s3.Client
	name   string
	// region is the client's resolved AWS region (never empty; OpenS3
	// defaults it to us-east-1). EnsureBucket needs it: CreateBucket
	// against a regional endpoint outside us-east-1 must carry the
	// region as its LocationConstraint or real S3 rejects the request
	// with 400 IllegalLocationConstraintException.
	region string
	// expectedOwner is the optional 12-digit AWS account ID every S3
	// request asserts as the bucket owner (issue #58). Empty = disabled.
	expectedOwner string
}

var (
	_ BlobStore       = (*S3)(nil)
	_ PageListerAfter = (*S3)(nil)
)

// EnvExpectedBucketOwner optionally pins every S3 request to a bucket
// owner (issue #58): when set to a 12-digit AWS account ID, the client
// middleware sends the bucket-owner condition on EVERY operation, so a
// bucket whose global name was re-registered by another account yields
// 403 Access Denied instead of silently serving/accepting private packs.
// Read here (not in cmd config) so every process that opens the store —
// serve, git hooks (inheriting the serve environment), and the purge CLI
// — enforces it uniformly; a probe-only or serve-only check would leave
// the rest of the data plane owner-blind. Empty (the default) attaches no
// middleware and changes no behavior.
const EnvExpectedBucketOwner = "OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER"

// Bucket-owner condition headers (AWS S3 REST). The source spelling
// asserts the owner of the bucket named in x-amz-copy-source on
// CopyObject/UploadPartCopy; all copies here are same-bucket.
const (
	expectedOwnerHeader       = "x-amz-expected-bucket-owner"
	expectedSourceOwnerHeader = "x-amz-source-expected-bucket-owner"
)

// awsAccountIDPattern is the only valid non-empty shape for the expected
// owner: exactly 12 digits. A typo'd ID would 403 every operation on real
// AWS, so misconfiguration is boot-fatal instead (crash-and-burn).
var awsAccountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)

// expectedBucketOwnerFromEnv reads and validates EnvExpectedBucketOwner.
func expectedBucketOwnerFromEnv() (string, error) {
	owner := strings.TrimSpace(envconfig.Getenv(EnvExpectedBucketOwner))
	if owner != "" && !awsAccountIDPattern.MatchString(owner) {
		return "", fmt.Errorf("%s must be a 12-digit AWS account ID, got %q", EnvExpectedBucketOwner, owner)
	}
	return owner, nil
}

// expectedBucketOwnerAPIOption returns the client middleware that stamps
// the bucket-owner condition onto EVERY outgoing S3 request. The client
// seam makes enforcement structurally all-or-nothing (issue #58): gocloud
// data operations, transfermanager multipart uploads, the raw-client
// bucket/copy calls, and presigned URLs all build requests through this
// one stack, so a future call site cannot forget to thread the condition.
// The Build step runs after serialization (the copy-source header is
// visible) and before SigV4 signing, so direct requests sign the headers
// and presigning hoists them into signed lowercase query parameters (see
// S3.SignedURL). AWS documents the condition as ignored — not rejected —
// on the operations that cannot carry it (CreateBucket, ListBuckets).
func expectedBucketOwnerAPIOption(owner string) func(*middleware.Stack) error {
	assertf(awsAccountIDPattern.MatchString(owner), "expectedBucketOwnerAPIOption(%q)", owner)
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("openstorageExpectedBucketOwner",
			func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
				req, ok := in.Request.(*smithyhttp.Request)
				assertf(ok, "expected-bucket-owner middleware: request type %T", in.Request)
				req.Header.Set(expectedOwnerHeader, owner)
				if req.Header.Get("x-amz-copy-source") != "" {
					req.Header.Set(expectedSourceOwnerHeader, owner)
				}
				return next.HandleBuild(ctx, in)
			}), middleware.After)
	}
}

// s3ClientOptions is the shared client wiring for OpenS3 and the unit
// tests that capture the HTTP requests the SDK actually emits.
func s3ClientOptions(endpoint, expectedOwner string) func(*s3.Options) {
	return func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		// MinIO needs path-style addressing; harmless otherwise.
		o.UsePathStyle = true
		if expectedOwner != "" {
			o.APIOptions = append(o.APIOptions, expectedBucketOwnerAPIOption(expectedOwner))
		}
		// Round-trip census seam (issue #182): one line per S3 HTTP
		// attempt when OPENSTORAGE_S3_OP_LOG is set; inert otherwise.
		if opt, ok := s3oplog.APIOption(); ok {
			o.APIOptions = append(o.APIOptions, opt)
		}
	}
}

// OpenS3 opens a blob URL, e.g.
// s3://spike?endpoint=http://127.0.0.1:9000&s3ForcePathStyle=true&region=us-east-1
// Credentials come from the standard AWS env vars, and the optional
// expected-bucket-owner enforcement comes from EnvExpectedBucketOwner (a
// malformed value is boot-fatal). The URL is parsed by hand (bucket =
// host, endpoint/region = query) so we control path-style behavior
// deterministically against MinIO regardless of gocloud URL-param renames.
func OpenS3(ctx context.Context, blobURL string) (*S3, error) {
	assertf(blobURL != "", "OpenS3: empty blob URL")
	u, err := url.Parse(blobURL)
	if err != nil {
		return nil, fmt.Errorf("parse blob URL: %w", err)
	}
	if u.Scheme != s3blob.Scheme {
		return nil, fmt.Errorf("blob URL scheme %q unsupported (want s3)", u.Scheme)
	}
	bucketName := u.Host
	if bucketName == "" {
		return nil, errors.New("blob URL has no bucket name")
	}
	expectedOwner, err := expectedBucketOwnerFromEnv()
	if err != nil {
		return nil, err
	}
	q := u.Query()
	region := q.Get("region")
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg, s3ClientOptions(q.Get("endpoint"), expectedOwner))
	bucket, err := s3blob.OpenBucket(ctx, client, bucketName, nil)
	if err != nil {
		return nil, fmt.Errorf("open bucket %q: %w", bucketName, err)
	}
	return &S3{bucket: bucket, client: client, name: bucketName, region: region, expectedOwner: expectedOwner}, nil
}

func (b *S3) Close() error { return b.bucket.Close() }

// bucketLifecycleClient is the subset of the S3 API EnsureBucket needs — a
// seam so unit tests can fake CreateBucket/HeadBucket outcomes without a
// live store. *s3.Client satisfies it.
type bucketLifecycleClient interface {
	CreateBucket(ctx context.Context, in *s3.CreateBucketInput, opts ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

// EnsureBucket creates the bucket, tolerating "already exists/owned by you"
// (so `serve` works against a fresh MinIO). A least-privilege deployment
// role for a pre-provisioned bucket has no s3:CreateBucket, so AccessDenied
// on create is not proof the bucket is missing: probe it with HeadBucket and
// accept when reachable. Any other create error, or a denied create with an
// unreachable bucket, stays boot-fatal. With an expected bucket owner
// configured (issue #58), EVERY accepted outcome additionally passes an
// owner-conditioned HeadBucket probe, so squatted names and
// credential/account drift fail boot instead of 403ing data operations.
func (b *S3) EnsureBucket(ctx context.Context) error {
	assertf(b.client != nil, "EnsureBucket: S3 opened without a client")
	return ensureBucket(ctx, b.client, b.name, b.region, b.expectedOwner)
}

func ensureBucket(ctx context.Context, client bucketLifecycleClient, name, region, expectedOwner string) error {
	assertf(name != "", "ensureBucket: empty bucket name")
	assertf(region != "", "ensureBucket: empty region (OpenS3 defaults it)")
	// The probe's HeadBucket carries x-amz-expected-bucket-owner via the
	// client middleware whenever an owner is configured.
	probe := func() error {
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
		return err
	}
	in := &s3.CreateBucketInput{Bucket: aws.String(name)}
	// Real S3 routes CreateBucket by LocationConstraint: a regional
	// endpoint outside us-east-1 rejects a request without one (400
	// IllegalLocationConstraintException) BEFORE any existence or
	// authorization check, so even an already-owned bucket would fail
	// boot. us-east-1 is the opposite: it is not a valid constraint
	// value and must stay omitted (the empty constraint means
	// us-east-1). S3-compatible stores (SeaweedFS, MinIO) accept both
	// spellings.
	if region != "us-east-1" {
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}
	_, err := client.CreateBucket(ctx, in)
	if err == nil {
		// Created — but S3 ignores the owner condition on CreateBucket
		// itself, and the new bucket's owner is the CREDENTIALS' account,
		// which may not be the configured expected owner (Codex round 1:
		// credential/account drift). Probe so the mismatch fails boot
		// here instead of 403ing every later data operation.
		if expectedOwner == "" {
			return nil
		}
		if headErr := probe(); headErr != nil {
			return fmt.Errorf("created bucket %q but the expected-owner probe (account %s) failed: %w",
				name, expectedOwner, headErr)
		}
		return nil
	}
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			// AlreadyOwnedByYou proves the CREDENTIALS' account owns the
			// bucket — not that it matches the configured owner (drift,
			// as above). AlreadyExists means SOME account owns the name —
			// exactly the squatting scenario the owner condition targets
			// (issue #58). Without a configured owner both stay accepted
			// as-is (compatibility); with one, both must pass the
			// conditioned probe so a reowned or wrong-account bucket
			// fails boot.
			if expectedOwner == "" {
				return nil
			}
			if headErr := probe(); headErr != nil {
				return fmt.Errorf("bucket %q exists but the expected-owner probe (account %s) failed: %w",
					name, expectedOwner, headErr)
			}
			return nil
		case "AccessDenied", "IllegalLocationConstraintException":
			// Least-privilege roles may lack s3:CreateBucket; the denial
			// is not proof the bucket is missing. The reachability probe
			// doubles as the owner check when one is configured.
			// IllegalLocationConstraintException gets the same treatment:
			// endpoint-level constraint validation runs before existence
			// checks, so a rejected create says nothing about whether the
			// (possibly pre-provisioned) bucket exists — the probe decides.
			if headErr := probe(); headErr != nil {
				return fmt.Errorf("create bucket %q rejected (%v) and probe failed: %w", name, err, headErr)
			}
			return nil
		}
	}
	return fmt.Errorf("create bucket %q: %w", name, err)
}

// UploadFile streams a local file to key.
func (b *S3) UploadFile(ctx context.Context, key, path string) error {
	return bucketUploadFile(ctx, b.bucket, key, path)
}

// DownloadFile fetches key into destPath atomically (tmp + rename).
func (b *S3) DownloadFile(ctx context.Context, key, destPath string) error {
	return bucketDownloadFile(ctx, b.bucket, key, destPath, -1)
}

// DownloadFileCapped fetches key into destPath atomically, aborting with
// ErrCapExceeded once the stream would exceed capBytes on disk.
func (b *S3) DownloadFileCapped(ctx context.Context, key, destPath string, capBytes int64) error {
	assertf(capBytes >= 0, "DownloadFileCapped(%q, %d)", key, capBytes)
	return bucketDownloadFile(ctx, b.bucket, key, destPath, capBytes)
}

// List returns all objects under prefix.
func (b *S3) List(ctx context.Context, prefix string) ([]ListedObject, error) {
	return bucketList(ctx, b.bucket, prefix)
}

// ListPages streams objects under prefix in key order, one page at a time.
func (b *S3) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]ListedObject) error) error {
	return bucketListPages(ctx, b.bucket, prefix, pageSize, fn)
}

// ListPagesAfter is ListPages from the first key after after. It asks S3 to
// start there (StartAfter on ListObjectsV2, Marker on the legacy ListObjects),
// and filters the keys it gets either way, so a server that ignores the
// request still answers correctly.
func (b *S3) ListPagesAfter(ctx context.Context, prefix, after string, pageSize int, fn func([]ListedObject) error) error {
	var beforeList func(func(any) bool) error
	if s3KeyIsPlain(after) {
		beforeList = s3StartAfter(after)
	}
	return bucketListPagesAfter(ctx, b.bucket, prefix, after, beforeList, pageSize, fn)
}

// s3KeyIsPlain reports whether gocloud's s3blob sends key unchanged. It
// escapes control characters, the slash of "../", and anything that looks
// like its own "__0x<hex>__" escape, and the request must carry the escaped
// spelling of a key. A key that is not plain is not sent as StartAfter.
func s3KeyIsPlain(key string) bool {
	if key == "" || strings.Contains(key, "__") || strings.Contains(key, "../") {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x20 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

// s3StartAfter is a gocloud ListOptions.BeforeList hook that starts a listing
// after key on the first page of either S3 list API. Later pages carry their
// own continuation token or marker, which the hook leaves alone.
func s3StartAfter(key string) func(func(any) bool) error {
	return func(as func(any) bool) error {
		var v2 *s3.ListObjectsV2Input
		if as(&v2) {
			if v2.ContinuationToken == nil {
				v2.StartAfter = aws.String(key)
			}
			return nil
		}
		var v1 *s3.ListObjectsInput
		if as(&v1) && v1.Marker == nil {
			v1.Marker = aws.String(key)
		}
		return nil
	}
}

// VersioningEnabled reports whether the backing bucket has ever had S3
// versioning turned on (status Enabled OR Suspended — suspension keeps all
// previously stored versions, and unversioned deletes still only hide the
// current version behind a delete marker). The orphan-pack sweeper's delete
// mode refuses such buckets: its deletes would reclaim nothing while
// reporting that they did.
func (b *S3) VersioningEnabled(ctx context.Context) (bool, error) {
	assertf(b.client != nil, "VersioningEnabled: S3 opened without a client")
	out, err := b.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(b.name)})
	if err != nil {
		return false, fmt.Errorf("get bucket versioning %q: %w", b.name, err)
	}
	return out.Status == s3types.BucketVersioningStatusEnabled ||
		out.Status == s3types.BucketVersioningStatusSuspended, nil
}

// SignedURL returns a presigned S3 URL for method (GET or PUT) on key.
// ContentType is deliberately left unset in the options: gocloud's
// aws-sdk-go-v2 presigner returns Unimplemented when asked to sign a
// Content-Type for PUT, and an unsigned Content-Type means git-lfs's
// `Content-Type: application/octet-stream` PUT verifies against MinIO/S3
// SigV4 without extra configuration.
//
// With expected-bucket-owner enforcement on, presigning runs through the
// same client middleware as every other operation and the presigner
// hoists the x-amz-* header into a SIGNED lowercase query parameter
// (x-amz-expected-bucket-owner=...), the spelling S3 documents and
// enforces on presigned requests (aws-sdk-go-v2 #2508). The URL is
// self-contained: redeemers send no extra header, and stripping or
// altering the parameter breaks the SigV4 signature.
func (b *S3) SignedURL(ctx context.Context, key, method string, expiry time.Duration) (string, error) {
	assertSignedURLArgs(key, method, expiry)
	return b.bucket.SignedURL(ctx, key, &gcblob.SignedURLOptions{Method: method, Expiry: expiry})
}

// Stat returns the size of the object at key (ErrNotFound when absent).
func (b *S3) Stat(ctx context.Context, key string) (int64, error) {
	return bucketStat(ctx, b.bucket, key)
}

// StatFingerprint — see BlobStore.
func (b *S3) StatFingerprint(ctx context.Context, key string) (int64, string, error) {
	return bucketStatFingerprint(ctx, b.bucket, key)
}

// ReadRange returns exactly the length bytes at offset from the object at
// key (a ranged GET; ErrNotFound when absent).
func (b *S3) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	return bucketReadRange(ctx, b.bucket, key, offset, length)
}

// Delete removes the object at key (ErrNotFound when absent).
func (b *S3) Delete(ctx context.Context, key string) error {
	return bucketDelete(ctx, b.bucket, key)
}

// S3 CopyObject handles at most 5 GiB per request; larger objects (packs
// can exceed it — uploads go through the multipart-capable writer) must be
// copied part-wise with UploadPartCopy. Vars, not consts, so tests can
// exercise the multipart path with small objects against MinIO.
var (
	s3SinglePartCopyLimit int64 = 5 * 1024 * 1024 * 1024
	s3CopyPartSize        int64 = 1024 * 1024 * 1024
)

// Copy duplicates srcKey to dstKey: server-side CopyObject for objects up
// to the single-request limit, multipart UploadPartCopy beyond it (still
// fully server-side — bytes never flow through this process).
func (b *S3) Copy(ctx context.Context, dstKey, srcKey string) error {
	size, err := b.Stat(ctx, srcKey)
	if err != nil {
		return fmt.Errorf("copy %s -> %s: %w", srcKey, dstKey, err)
	}
	if size <= s3SinglePartCopyLimit {
		return bucketCopy(ctx, b.bucket, dstKey, srcKey)
	}
	return b.multipartCopy(ctx, dstKey, srcKey, size)
}

// copySourceHeader encodes bucket/key for the X-Amz-Copy-Source header:
// URL-encoded path segments, with '+' escaped explicitly (url.PathEscape
// leaves it literal, but namespaced repo keys contain '+', and copy-source
// decoders are inconsistent about bare plus signs).
func copySourceHeader(bucket, key string) string {
	segs := strings.Split(key, "/")
	for i := range segs {
		segs[i] = strings.ReplaceAll(url.PathEscape(segs[i]), "+", "%2B")
	}
	return bucket + "/" + strings.Join(segs, "/")
}

// multipartCopy copies srcKey (size bytes) to dstKey in s3CopyPartSize
// ranges via UploadPartCopy, aborting the multipart upload on any failure
// so no billable incomplete upload leaks.
func (b *S3) multipartCopy(ctx context.Context, dstKey, srcKey string, size int64) (err error) {
	assertf(b.client != nil, "multipartCopy: S3 opened without a client")
	assertf(size > 0, "multipartCopy(%q): non-positive size %d", srcKey, size)
	create, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.name), Key: aws.String(dstKey),
	})
	if err != nil {
		return fmt.Errorf("copy %s -> %s: create multipart: %w", srcKey, dstKey, err)
	}
	defer func() {
		if err == nil {
			return
		}
		// Abort must survive a canceled ctx, or the incomplete upload leaks.
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_, _ = b.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(b.name), Key: aws.String(dstKey), UploadId: create.UploadId,
		})
	}()

	source := copySourceHeader(b.name, srcKey)
	var parts []s3types.CompletedPart
	for start := int64(0); start < size; start += s3CopyPartSize {
		end := min(start+s3CopyPartSize, size) - 1
		num := int32(len(parts) + 1) //nolint:gosec // bounded by S3's 10k part cap
		part, err := b.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: aws.String(b.name), Key: aws.String(dstKey),
			CopySource:      aws.String(source),
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
			PartNumber:      aws.Int32(num),
			UploadId:        create.UploadId,
		})
		if err != nil {
			return fmt.Errorf("copy %s -> %s: part %d: %w", srcKey, dstKey, num, err)
		}
		parts = append(parts, s3types.CompletedPart{ETag: part.CopyPartResult.ETag, PartNumber: aws.Int32(num)})
	}
	if _, err := b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(b.name), Key: aws.String(dstKey), UploadId: create.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return fmt.Errorf("copy %s -> %s: complete multipart: %w", srcKey, dstKey, err)
	}
	return nil
}
