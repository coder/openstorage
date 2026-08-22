//go:build !codestorage_stub

// wire.go is the single integration seam between the CLI and the phase-1
// implementation slices. It is the ONLY file referencing the constructors
// owned by sibling slices:
//
//   - meta.Open / meta.Backend{InitSchema, Close}           (internal/meta)
//   - auth.NewVerifier(store)                               (internal/auth)
//   - api.New(api.Config{Store, Verifier, HTTPBaseURL})     (internal/api)
//
// If a slice landed different names/signatures, fix the drift HERE only.
// The `codestorage_stub` build tag (constraint above) used to swap in a
// wire_stub.go stub so the CLI could build in isolation before the slices
// landed; the stub was deleted once they did.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"codestorage/internal/api"
	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/githttp"
	"codestorage/internal/jobs"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

// buildAPIHandler opens the metadata store, bootstraps the single org with
// its PEM public key blocks (idempotent upsert), and returns the /api/v1
// handler, the store (for the git path's repo_id->url resolution), the
// verifier (shared with the git path per ADR-8), and a cleanup func. The
// handler registers full paths under /api (v1 routes, the /api/repos named
// aliases, and /api/repo-urls), so serve mounts it un-stripped at
// mux.Handle("/api/", h). refStore/blobStore/cacheDir back the
// git:read endpoints — serve passes the same instances the githttp fallback
// uses so both surfaces share one cache.
func buildAPIHandler(ctx context.Context, dbURL, org, httpBaseURL, loopbackBaseURL string, orgPEMBlocks []string, refStore ref.RefStore, blobStore blob.BlobStore, cacheDir string, rate api.RateLimitConfig, packQuotaBytes int64, packReservationTTL time.Duration, sweep sweepSettings, repackThreshold int) (http.Handler, meta.Store, auth.Verifier, func(ctx context.Context, repo string), func(), error) {
	assertf(dbURL != "" && org != "" && httpBaseURL != "" && len(orgPEMBlocks) > 0, "buildAPIHandler: missing dbURL/org/httpBaseURL/keys")
	assertf(loopbackBaseURL != "", "buildAPIHandler: missing loopbackBaseURL")
	assertf(refStore != nil && blobStore != nil && cacheDir != "", "buildAPIHandler: missing refStore/blobStore/cacheDir")
	ms, msCleanup, err := openMetaStore(ctx, dbURL)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	fail := func(err error) (http.Handler, meta.Store, auth.Verifier, func(ctx context.Context, repo string), func(), error) {
		msCleanup()
		return nil, nil, nil, nil, nil, err
	}

	// Eventing/sync stack (docs/06): jobs queue with delivery + sync
	// workers, and the events.RefStore decorator so API ref writes emit
	// push events. The git surface gets the same decorator in the hook
	// subprocess via wireHookEventing.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Org bootstrap + verifier, including the per-process loopback
	// sync-push key (docs/07 p5, issue #45).
	key, verifier, err := bootstrapOrgAuth(ctx, ms, org, orgPEMBlocks, logger)
	if err != nil {
		return fail(err)
	}

	// Orphan-pack sweep (issue #5): built over the RAW ref store — pack
	// listing is a read, no events to emit — and the shared blob store.
	// interval 0 disables the periodic schedule; nil schedule keeps both
	// queue backends inert.
	if (sweep.deleteEnabled || sweep.deleteLFS) && sweep.interval > 0 {
		// Versioned S3 buckets break delete-mode invariants: Delete only
		// writes a delete marker while every pack and quarantine version
		// stays stored and billed, so sweeps would report reclaimed bytes
		// without reclaiming anything. Fail closed: delete mode requires
		// the probe to definitively report an unversioned bucket — a role
		// without s3:GetBucketVersioning must not silently run destructive
		// sweeps against a possibly versioned bucket. Gated on a live
		// schedule: interval zero (the emergency stop) runs only the
		// strictly restorative Heal, so nothing destructive needs this
		// validation and the stop switch must never be blocked by it.
		deleteFlag := envSweepDeleteEnabled
		if !sweep.deleteEnabled {
			deleteFlag = envSweepDeleteLFS
		}
		if s3store, ok := blobStore.(*blob.S3); ok {
			versioned, err := s3store.VersioningEnabled(ctx)
			switch {
			case err != nil:
				return fail(fmt.Errorf("%s: cannot verify bucket versioning (grant s3:GetBucketVersioning, or keep the sweeper list-only): %w", deleteFlag, err))
			case versioned:
				return fail(fmt.Errorf("%s: bucket has S3 versioning enabled or suspended; delete-mode sweeps only write delete markers there (add a noncurrent-version lifecycle policy, or keep the sweeper list-only)", deleteFlag))
			}
		}
	}
	sweepCfg := jobs.SweepConfig{
		Threshold:        sweep.ageThreshold,
		Delete:           sweep.deleteEnabled,
		DeleteSuperseded: sweep.deleteSuperseded,
		DeleteLFS:        sweep.deleteLFS,
		// The sweep prunes crash-leaked admission reservations (issue #29)
		// on its cadence with the same TTL the write gates charge.
		ReservationTTL: packReservationTTL,
	}
	if s3store, ok := blobStore.(*blob.S3); ok && (sweep.deleteEnabled || sweep.deleteSuperseded || sweep.deleteLFS) {
		// Versioning can be enabled on the bucket while the process runs,
		// silently turning startup's validation stale; every delete-mode
		// sweep re-checks and downgrades itself to list-only on drift or
		// probe failure (fail closed, matching the startup gate).
		sweepCfg.DeleteGuard = func(ctx context.Context) error {
			versioned, err := s3store.VersioningEnabled(ctx)
			if err != nil {
				return fmt.Errorf("verify bucket versioning: %w", err)
			}
			if versioned {
				return fmt.Errorf("bucket has S3 versioning enabled or suspended; deletes would only write delete markers")
			}
			return nil
		}
	}
	sweeper := jobs.NewSweeper(blobStore, refStore, sweepCfg, logger)
	var schedule *jobs.SweepSchedule
	if sweep.interval > 0 {
		schedule = &jobs.SweepSchedule{Sweeper: sweeper, Interval: sweep.interval}
	} else if err := sweeper.Heal(ctx); err != nil {
		// Disabling the schedule must not strand a registered pack whose
		// only copy a crashed delete-enabled handshake left in quarantine:
		// run one strictly restorative heal pass at startup regardless.
		// With no ticker there is no retry, so a failure here is fatal —
		// serving would otherwise fail hydrations of the stranded pack
		// until another restart anyway (fail-fast beats limping).
		return fail(fmt.Errorf("startup quarantine heal (sweeping disabled): %w", err))
	}

	// Background repack (issue #1): the worker folds over the RAW ref
	// store — ReplacePacks is a pack mutation, no ref events to emit — and
	// the shared blob store and cache dir. Always constructed so durable
	// jobs queued before a threshold change still run; the post-write
	// TRIGGER below is what a zero threshold disables. The materialize
	// seam fetches EXACTLY the budget-selected packs into the shared cache
	// (never a full hydrate: the fold reads no refs, and materializing
	// unselected packs would defeat its byte budget on cold workers).
	repacker := jobs.NewRepacker(refStore, blobStore,
		func(ctx context.Context, repo string, packs []ref.Pack) (string, error) {
			repoPath, err := githttp.EnsureCacheRepoCtx(ctx, cacheDir, repo)
			if err != nil {
				return "", err
			}
			for _, p := range packs {
				if err := githttp.MaterializePack(ctx, blobStore, repoPath, p); err != nil {
					return "", err
				}
			}
			return repoPath, nil
		}, logger)
	// Post-fold follow-ups share the trigger threshold: packs registered
	// while a fold ran survive its snapshot, and the queue backends
	// schedule one more fold when the count is still at/above it.
	repacker.FollowUpThreshold = repackThreshold

	queue, decorate, queueCleanup, err := buildServeEventing(ctx, ms, dbURL, org, loopbackBaseURL, key, schedule, repacker, logger)
	if err != nil {
		return fail(err)
	}

	// maybeRepack is handed to both write surfaces (API handlers here, the
	// git receive path via cmdServe): after a write that registered packs,
	// check the threshold and enqueue the unique-by-repo fold job.
	var maybeRepack func(ctx context.Context, repo string)
	if repackThreshold > 0 {
		maybeRepack = jobs.NewRepackTrigger(refStore, repackThreshold, queue.EnqueueRepack, logger).MaybeEnqueue
	}

	h, err := api.New(api.Config{
		Store:    ms,
		Verifier: verifier,
		// Served-org gate (issue #47): REST rejects verified tokens whose
		// iss is another (e.g. retained) org row, exactly like the git
		// surface's verifyClaims.
		Org:         org,
		HTTPBaseURL: httpBaseURL,
		RefStore:    decorate(refStore),
		BlobStore:   blobStore,
		CacheDir:    cacheDir,
		SyncQueue:   queue,
		RateLimit:   rate,
		MaybeRepack: maybeRepack,
		// The same per-repo pack quota the git surface enforces at
		// pre-receive (issue #21): REST pack writers must not bypass it.
		PackQuotaBytes:     packQuotaBytes,
		PackReservationTTL: packReservationTTL,
	})
	if err != nil {
		queueCleanup() // stop workers before closing their meta store
		return fail(err)
	}
	cleanup := func() {
		queueCleanup() // stop workers before closing their meta store
		msCleanup()
	}
	return h, ms, verifier, maybeRepack, cleanup, nil
}

// bootstrapOrgAuth registers the configured file keys for the served org
// (idempotent last-writer-wins upsert — correct for file keys, every replica
// re-inserts the same shared set) and builds the process verifier: database
// org keys unioned in-memory with this process's loopback public key.
//
// The loopback key is deliberately NOT persisted (issue #45): the org key
// set is one shared slot, so a persisted per-process key would be deleted by
// the next replica's boot, 403ing this process's own sync pushes. In-memory
// verification is sound because loopback pushes terminate at this process's
// own listener (see selfLoopbackURL), never at another replica.
//
// It also warns about retained org rows that no longer match the configured
// org (issue #47): both surfaces reject their tokens, but the rows (and any
// registered keys) stay in the database — cleanup is operator-driven.
func bootstrapOrgAuth(ctx context.Context, ms meta.Store, org string, orgPEMBlocks []string, logger *slog.Logger) (loopbackKey, auth.Verifier, error) {
	assertf(ms != nil && org != "" && len(orgPEMBlocks) > 0 && logger != nil, "bootstrapOrgAuth: missing deps")
	key, loopbackPubPEM, err := newLoopbackKey()
	if err != nil {
		return loopbackKey{}, nil, err
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, org, orgPEMBlocks); err != nil {
		return loopbackKey{}, nil, fmt.Errorf("bootstrap org %q: %w", org, err)
	}
	names, err := ms.OrgNames(ctx)
	if err != nil {
		return loopbackKey{}, nil, fmt.Errorf("list org rows: %w", err)
	}
	for _, name := range names {
		if name != org {
			logger.Warn("retained org row does not match the configured org; its tokens are rejected on both surfaces (issue #47)",
				"org_row", name, "configured_org", org)
		}
	}
	return key, auth.NewVerifier(auth.UnionKeySource(ms, org, loopbackPubPEM)), nil
}

// openMetaStore opens the metadata store (URL-scheme dispatch via meta.Open)
// and ensures its schema exists (idempotent). Shared by serve (via
// buildAPIHandler) and the signing-key CLI, which talks to the database
// directly — there is no HTTP surface for the signing-key registry
// (docs/02: upstream manages keys in its dashboard).
func openMetaStore(ctx context.Context, dbURL string) (meta.Store, func(), error) {
	assertf(dbURL != "", "openMetaStore: empty dbURL")
	ms, err := meta.Open(ctx, dbURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open metadata store: %w", err)
	}
	if err := ms.InitSchema(ctx); err != nil {
		ms.Close()
		return nil, nil, fmt.Errorf("init metadata schema: %w", err)
	}
	return ms, func() { ms.Close() }, nil
}
