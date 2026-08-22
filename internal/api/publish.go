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

	"codestorage/internal/blob"
	"codestorage/internal/ref"
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
	published := false
	if _, err := s.blobStore.Stat(ctx, key); err == nil {
		published = true
	} else if !errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("stat published pack %s: %w", key, err)
	}
	if !published {
		if s.packQuotaBytes > 0 {
			usage, err := blob.PackUsageBytes(ctx, s.blobStore, repoID)
			if err != nil {
				return fmt.Errorf("pack quota usage: %w", err)
			}
			if usage+info.Size() > s.packQuotaBytes {
				return fmt.Errorf("%w: write adds %d bytes to %d bytes stored, quota is %d bytes", errPackQuotaExceeded, info.Size(), usage, s.packQuotaBytes)
			}
		}
		if err := s.blobStore.UploadFile(ctx, key, packPath); err != nil {
			return fmt.Errorf("upload pack %s: %w", key, err)
		}
	}
	// Writer half of the orphan-sweep coordination (issue #5): re-upload
	// if a sweeper deleted the key before the registration committed.
	// Verification runs even when RegisterPacks fails: an ambiguous error
	// may have committed the row (see the pre-receive hook).
	regErr := s.refStore.RegisterPacks(ctx, repoID, []ref.Pack{{ID: packID, Key: key}})
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
