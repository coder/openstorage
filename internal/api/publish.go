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
	"github.com/coder/code-storage/internal/ref"
)

// errPackQuotaExceeded marks a publishPack denial for a write that would
// take the repo's packs/ prefix past the configured quota (issue #21).
// The wrapped message carries the byte arithmetic.
var errPackQuotaExceeded = errors.New("repository pack quota exceeded")

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
	if !published {
		if s.packQuotaBytes > 0 {
			// Durable admission (issue #29, ref/ledger.go): the listing is
			// the usage floor; the reservation closes concurrent-writer
			// overshoot and is consumed by RegisterPacks below. Failed
			// uploads and crashes alike leave an expiring row the sweeper
			// prunes (settle-or-TTL, Codex round 22).
			floor, err := s.usageFloor(ctx, repoID)
			if err != nil {
				return fmt.Errorf("pack quota usage: %w", err)
			}
			owner := ref.NewReservationOwner()
			ttl := s.packReservationTTL
			if ttl <= 0 {
				// Unwired server (direct construction in tests, or a
				// caller predating the ledger): fall back like the
				// pre-receive hook does on an absent env — quota
				// enforcement must never silently weaken.
				ttl = time.Hour
			}
			dec, err := s.refStore.ReservePackBytes(ctx, blob.QuotaRepoGroup(repoID),
				[]ref.Pack{pack}, owner, floor, s.packQuotaBytes, ttl)
			if err != nil {
				return fmt.Errorf("reserve pack bytes: %w", err)
			}
			if !dec.OK {
				return fmt.Errorf("%w: write adds %d bytes to %d bytes stored (+%d bytes reserved by in-flight writes), quota is %d bytes",
					errPackQuotaExceeded, pack.Size, dec.UsedBytes, dec.ReservedBytes, s.packQuotaBytes)
			}
			if err := s.blobStore.UploadFile(ctx, key, packPath); err != nil {
				// Clean failure: retire THIS attempt (Codex round 30;
				// replacing round 22's settle-or-TTL-only rule). The
				// per-attempt refcount never removes a live same-key
				// writer's protection; a failed retire only leaves what
				// the TTL always collected. Detached from the request
				// context and bounded (Codex round 31): the most common
				// clean failure here IS a client cancellation, and a
				// retire on the already-canceled context can never reach
				// the database — repeated canceled writes with distinct
				// keys would pin maxActiveReservations rows for the TTL.
				rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
				defer rcancel()
				if rerr := s.refStore.RetirePackAttempts(rctx, blob.QuotaRepoGroup(repoID), []string{key}); rerr != nil {
					s.logger.Warn("retire pack reservation", "key", key, "error", rerr)
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
	regErr := s.refStore.RegisterPacks(ctx, repoID, []ref.Pack{pack})
	if regErr != nil {
		regErr = fmt.Errorf("register pack %s: %w", packID, regErr)
	}
	if err := blob.EnsurePublished(ctx, s.blobStore, key, packPath); err != nil {
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
