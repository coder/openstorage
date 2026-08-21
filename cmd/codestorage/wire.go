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
func buildAPIHandler(ctx context.Context, dbURL, org, httpBaseURL string, orgPEMBlocks []string, refStore ref.RefStore, blobStore blob.BlobStore, cacheDir string, rate api.RateLimitConfig, sweep sweepSettings) (http.Handler, meta.Store, auth.Verifier, func(), error) {
	assertf(dbURL != "" && org != "" && httpBaseURL != "" && len(orgPEMBlocks) > 0, "buildAPIHandler: missing dbURL/org/httpBaseURL/keys")
	assertf(refStore != nil && blobStore != nil && cacheDir != "", "buildAPIHandler: missing refStore/blobStore/cacheDir")
	ms, msCleanup, err := openMetaStore(ctx, dbURL)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	fail := func(err error) (http.Handler, meta.Store, auth.Verifier, func(), error) {
		msCleanup()
		return nil, nil, nil, nil, err
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
	sweeper := jobs.NewSweeper(blobStore, refStore, jobs.SweepConfig{
		Threshold: sweep.ageThreshold,
		Delete:    sweep.deleteEnabled,
	}, logger)
	var schedule *jobs.SweepSchedule
	if sweep.interval > 0 {
		schedule = &jobs.SweepSchedule{Sweeper: sweeper, Interval: sweep.interval}
	} else if err := sweeper.Heal(ctx); err != nil {
		// Disabling the schedule must not strand a registered pack whose
		// only copy a crashed delete-enabled handshake left in quarantine:
		// run one restorative heal pass at startup regardless. Failure is
		// logged, not fatal — the pass re-runs on next boot.
		logger.Warn("startup quarantine heal failed", "err", err)
	}

	queue, decorate, queueCleanup, err := buildServeEventing(ctx, ms, dbURL, org, httpBaseURL, key, schedule, logger)
	if err != nil {
		return fail(err)
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
	})
	if err != nil {
		queueCleanup() // stop workers before closing their meta store
		return fail(err)
	}
	cleanup := func() {
		queueCleanup() // stop workers before closing their meta store
		msCleanup()
	}
	return h, ms, verifier, cleanup, nil
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
