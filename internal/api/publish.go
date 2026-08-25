package api

// publishPack is the single pack-publication step shared by every REST
// pack writer (issue #21): commit-pack and merge (via wcPublish), notes
// (wrefPublish), and fork. It runs the hook-order pipeline — content-
// addressed dedup, per-repo quota, upload, registration, and the
// sweeper-coordination verify — so no REST door can bypass the quota the
// git surface enforces at pre-receive, and no writer re-uploads an
// already-published key (a versioned bucket would retain billed noncurrent
// copies). Quota breaches return errPackQuotaExceeded BEFORE any upload;
// callers map it to their route's error shape.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/githttp"
	"github.com/coder/code-storage/internal/ref"
)

// errPackQuotaExceeded marks a publishPack denial for a write that would
// take the repo's packs/ prefix past the configured quota (issue #21).
// The wrapped message carries the byte arithmetic.
var errPackQuotaExceeded = errors.New("repository pack quota exceeded")

// errPackReservationsBusy marks a publishPack denial by the ACTIVE
// reservation-row cap (issue #51, Codex round 43): rows expire with the
// TTL, so unlike the byte-quota denial this is transient — handlers
// answer 429 with the Retry-After + RateLimit pair, never the permanent 403 whose quota
// arithmetic may even show the write fits.
var errPackReservationsBusy = errors.New("too many in-flight uploads reserved for this repository, retry later")

// publishPack publishes the local pack file at packPath under repoID's
// content-addressed key. Nothing is stored when the quota would be
// exceeded. A nil return means the pack is durably published, registered,
// and verified (blob.EnsurePublished's sweeper-race guarantee).
func (s *server) publishPack(ctx context.Context, repoID, packID, packPath string) error {
	assertf(repoID != "" && packID != "" && packPath != "", "publishPack(%q, %q, %q)", repoID, packID, packPath)
	info, err := os.Stat(packPath)
	if err != nil {
		return fmt.Errorf("stat pack: %w", err)
	}
	key := blob.PackKey(repoID, packID)
	// Stat/upload is not atomic: concurrent identical publications can
	// both see the key absent and upload. Byte-identical last-writer-wins
	// is harmless (content-addressed keys); on versioned buckets the cost
	// is bounded by concurrency-many noncurrent versions per unique pack,
	// reclaimable via a lifecycle rule (see the pre-receive hook's twin
	// note: issue #29's reservation ledger deliberately leaves this window
	// open — exclusive same-key reservations would wedge crash-window
	// retries for the TTL; conditional create is not portable through
	// gocloud).
	published := false
	if _, err := s.blobStore.Stat(ctx, key); err == nil {
		published = true
	} else if !errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("stat published pack %s: %w", key, err)
	}
	pack := ref.Pack{ID: packID, Key: key, Size: info.Size()}
	assertf(pack.Size > 0, "publishPack: empty pack file %s", packPath)
	// An unpublished pack whose key is already REGISTERED is a RESTORE,
	// not fresh growth (Codex round 72 — the REST twin of the
	// pre-receive hook's round-70 classification): its bytes are in the
	// usage counter, so charging them as incoming can falsely reject an
	// at-quota repair, and the fresh path would re-materialize them with
	// no distinct cover (the zero-credit registration no-op consumes the
	// reservation without bumping). Restores skip the admission charge
	// and the fresh upload; EnsurePublished below Stats them missing and
	// restores them through the covered, generation-bumping seam.
	restore := false
	if !published {
		has, err := s.refStore.HasPack(ctx, repoID, key)
		if err != nil {
			return fmt.Errorf("registry probe %s: %w", key, err)
		}
		restore = has
	}
	if !published && !restore {
		if s.packQuotaBytes > 0 {
			// Durable admission (issue #29, ref/ledger.go): the listing is
			// the usage floor; the reservation closes concurrent-writer
			// overshoot and is consumed by RegisterPacks below. Failed
			// uploads and crashes alike leave an expiring row the sweeper
			// prunes (settle-or-TTL, Codex round 22).
			owner := ref.NewReservationOwner()
			ttl := s.packReservationTTL
			if ttl <= 0 {
				// Unwired server (direct construction in tests, or a
				// caller predating the ledger): fall back like the
				// pre-receive hook does on an absent env — quota
				// enforcement must never silently weaken.
				ttl = time.Hour
			}
			var dec ref.ReserveDecision
			for attempt := 0; ; attempt++ {
				floor, floorGen, err := s.usageFloor(ctx, repoID)
				if err != nil {
					return fmt.Errorf("pack quota usage: %w", err)
				}
				// Deadline-bounded by the handoff margin (Codex round 42 on
				// #51) — see the LFS PUT twin: the floor's orphan cover must
				// outlive the admission snapshot.
				rsvCtx, rsvCancel := context.WithTimeout(ctx, githttp.UsageFloorHandoffMargin)
				dec, err = s.refStore.ReservePackBytes(rsvCtx, blob.QuotaRepoGroup(repoID),
					[]ref.Pack{pack}, owner, floor, floorGen, s.packQuotaBytes, ttl)
				rsvCancel()
				if err != nil {
					return fmt.Errorf("reserve pack bytes: %w", err)
				}
				if !dec.FloorStale {
					break
				}
				// The ledger's transactional generation recheck rejected
				// the floor (Codex round 60): re-derive once, then fail
				// closed (the client retries).
				if attempt >= 1 {
					return fmt.Errorf("usage floor went stale during admission twice, retry")
				}
			}
			if dec.KeyCapExceeded {
				// Transient row-cap denial, not a quota breach (Codex
				// round 43): rows expire with the TTL, so this maps to
				// retry — the byte arithmetic below would even suggest
				// the write fits.
				return errPackReservationsBusy
			}
			if !dec.OK {
				return fmt.Errorf("%w: write adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight writes), quota is %d bytes",
					errPackQuotaExceeded, pack.Size, dec.UsedBytes, dec.ReservedBytes, s.packQuotaBytes)
			}
			if err := s.blobStore.UploadFile(ctx, key, packPath); err != nil {
				// PROVEN-clean failure: retire THIS attempt (Codex round
				// 30; replacing round 22's settle-or-TTL-only rule). The
				// per-attempt refcount never removes a live same-key
				// writer's protection, the gen token keeps a stale retire
				// off any replacement row (round 33), and the retire runs
				// only on EVIDENCE the upload did not commit (round 33:
				// the error may be ambiguous with the bytes durable).
				// Detached from the request context and bounded (Codex
				// round 31): the most common clean failure here IS a
				// client cancellation, and a retire on the
				// already-canceled context can never reach the database.
				rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
				defer rcancel()
				if _, serr := s.blobStore.Stat(rctx, key); errors.Is(serr, blob.ErrNotFound) {
					if rerr := s.refStore.RetirePackAttempts(rctx, blob.QuotaRepoGroup(repoID), dec.Gens); rerr != nil {
						s.logger.Warn("retire pack reservation", "key", key, "error", rerr)
					}
				} else {
					s.logger.Warn("retire skipped, upload outcome ambiguous", "key", key, "stat_error", serr)
				}
				return fmt.Errorf("upload pack %s: %w", key, err)
			}
		} else if err := s.blobStore.UploadFile(ctx, key, packPath); err != nil {
			return fmt.Errorf("upload pack %s: %w", key, err)
		}
	}
	// Writer half of the orphan-sweep coordination (issue #5): re-upload
	// if a sweeper deleted the key before the registration committed.
	// Verification runs even when RegisterPacks fails: an ambiguous error
	// may have committed the row (see the pre-receive hook).
	// A pre-existing blob means this registration ADOPTS bytes cached
	// floors may already contain (Codex round 53) — the registration
	// bumps the shared floor generation for it.
	pack.Adopted = published
	regErr := s.refStore.RegisterPacks(ctx, repoID, []ref.Pack{pack})
	if regErr != nil {
		regErr = fmt.Errorf("register pack %s: %w", packID, regErr)
	}
	// A restore re-materializes counter-accounted bytes (Codex round 58,
	// the pack twin of the LFS zero-growth repair): cover it with a
	// reservation BEFORE the upload — stale absence-window floors held by
	// concurrent admissions must see the returning bytes — and bump the
	// shared floor generation afterward (best-effort, like the sweeper's
	// quarantine-restore bump) so future serves re-scan.
	var cover func(context.Context, int64) error
	if s.packQuotaBytes > 0 {
		cover = func(cctx context.Context, size int64) error {
			ttl := s.packReservationTTL
			if ttl <= 0 {
				ttl = time.Hour // unwired server: match the admission fallback above
			}
			return ref.ReserveRestoreCover(cctx, s.refStore, blob.QuotaRepoGroup(repoID), key, size, ttl)
		}
	}
	// The floor-generation bump is the restore's invalidation HANDOFF
	// (Codex rounds 68–70): it runs inside the seam, under a live cover,
	// detached from the request's fate (EnsurePublished deliberately
	// survives client cancellation), retried until it commits or cover
	// continuity is lost.
	if _, err := blob.EnsurePublished(ctx, s.blobStore, key, packPath, cover, func(bctx context.Context) error {
		return s.refStore.BumpLFSAdoptionGen(bctx, githttp.BaseRepo(repoID))
	}); err != nil {
		return errors.Join(regErr, fmt.Errorf("verify pack %s: %w", key, err))
	}
	// Threshold check regardless of regErr (issue #1): an ambiguous
	// registration error may have committed the row — the same reason the
	// verification above runs unconditionally — and skipping the trigger
	// would strand a threshold crossing until the next write. The check
	// reads the committed count, so it safely no-ops on a true failure.
	// Repack REPLACES bytes rather than adding them (ReplacePacks), so its
	// own uploads are deliberately not quota-gated; the trigger only fires
	// here, after a quota-passing publish.
	s.triggerRepack(ctx, repoID)
	return regErr
}
