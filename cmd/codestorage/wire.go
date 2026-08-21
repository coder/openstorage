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
func buildAPIHandler(ctx context.Context, dbURL, org, httpBaseURL string, orgPEMBlocks []string, refStore ref.RefStore, blobStore blob.BlobStore, cacheDir string, rate api.RateLimitConfig, sweep sweepSettings, repackThreshold int) (http.Handler, meta.Store, auth.Verifier, func(ctx context.Context, repo string), func(), error) {
	assertf(dbURL != "" && org != "" && httpBaseURL != "" && len(orgPEMBlocks) > 0, "buildAPIHandler: missing dbURL/org/httpBaseURL/keys")
	assertf(refStore != nil && blobStore != nil && cacheDir != "", "buildAPIHandler: missing refStore/blobStore/cacheDir")
	ms, msCleanup, err := openMetaStore(ctx, dbURL)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	fail := func(err error) (http.Handler, meta.Store, auth.Verifier, func(ctx context.Context, repo string), func(), error) {
		msCleanup()
		return nil, nil, nil, nil, nil, err
	}

	// Loopback sync-push key (docs/07 p5): per-process keypair whose public
	// half joins the org's registered keys, so the sync worker's internally
	// minted git:write JWTs verify on our own git endpoint.
	key, loopbackPubPEM, err := newLoopbackKey()
	if err != nil {
		return fail(err)
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, org, append(orgPEMBlocks, loopbackPubPEM)); err != nil {
		return fail(fmt.Errorf("bootstrap org %q: %w", org, err))
	}
	verifier := auth.NewVerifier(ms)

	// Eventing/sync stack (docs/06): jobs queue with delivery + sync
	// workers, and the events.RefStore decorator so API ref writes emit
	// push events. The git surface gets the same decorator in the hook
	// subprocess via wireHookEventing.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Orphan-pack sweep (issue #5): built over the RAW ref store — pack
	// listing is a read, no events to emit — and the shared blob store.
	// interval 0 disables the periodic schedule; nil schedule keeps both
	// queue backends inert.
	if sweep.deleteEnabled && sweep.interval > 0 {
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
		if s3store, ok := blobStore.(*blob.S3); ok {
			versioned, err := s3store.VersioningEnabled(ctx)
			switch {
			case err != nil:
				return fail(fmt.Errorf("%s: cannot verify bucket versioning (grant s3:GetBucketVersioning, or keep the sweeper list-only): %w", envSweepDeleteEnabled, err))
			case versioned:
				return fail(fmt.Errorf("%s: bucket has S3 versioning enabled or suspended; delete-mode sweeps only write delete markers there (add a noncurrent-version lifecycle policy, or keep the sweeper list-only)", envSweepDeleteEnabled))
			}
		}
	}
	sweepCfg := jobs.SweepConfig{
		Threshold: sweep.ageThreshold,
		Delete:    sweep.deleteEnabled,
	}
	if s3store, ok := blobStore.(*blob.S3); ok && sweep.deleteEnabled {
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
	// TRIGGER below is what a zero threshold disables. The default branch
	// is resolved fresh per fold from the base repo's meta row (issue #18;
	// namespaced keys share the base repo's row); the fold itself packs
	// idx-enumerated objects and only needs HEAD placement to be valid.
	repacker := jobs.NewRepacker(refStore, blobStore,
		func(ctx context.Context, repo string) (string, error, error) {
			rec, err := meta.ResolveRepoKey(ctx, ms, org, githttp.BaseRepo(repo))
			if err != nil {
				return "", nil, fmt.Errorf("resolve default branch for %s: %w", repo, err)
			}
			return githttp.Hydrate(ctx, refStore, blobStore, cacheDir, repo, rec.DefaultBranch)
		}, logger)

	queue, decorate, queueCleanup, err := buildServeEventing(ctx, ms, dbURL, org, httpBaseURL, key, schedule, repacker, logger)
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
		Store:       ms,
		Verifier:    verifier,
		HTTPBaseURL: httpBaseURL,
		RefStore:    decorate(refStore),
		BlobStore:   blobStore,
		CacheDir:    cacheDir,
		SyncQueue:   queue,
		RateLimit:   rate,
		MaybeRepack: maybeRepack,
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
