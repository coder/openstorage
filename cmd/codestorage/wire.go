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
	"net/http"

	"codestorage/internal/api"
	"codestorage/internal/auth"
	"codestorage/internal/meta"
)

// buildAPIHandler opens the metadata store, bootstraps the single org with
// its PEM public key blocks (idempotent upsert), and returns the /api/v1
// handler, the store (for the git path's repo_id->url resolution), the
// verifier (shared with the git path per ADR-8), and a cleanup func. The
// handler registers full "/api/v1/..." paths, so serve mounts it un-stripped
// at mux.Handle("/api/v1/", h).
func buildAPIHandler(ctx context.Context, pgURL, org, httpBaseURL string, orgPEMBlocks []string) (http.Handler, meta.Store, auth.Verifier, func(), error) {
	assertf(pgURL != "" && org != "" && httpBaseURL != "" && len(orgPEMBlocks) > 0, "buildAPIHandler: missing pgURL/org/httpBaseURL/keys")
	ms, cleanup, err := openMetaStore(ctx, pgURL)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, org, orgPEMBlocks); err != nil {
		cleanup()
		return nil, nil, nil, nil, fmt.Errorf("bootstrap org %q: %w", org, err)
	}
	verifier := auth.NewVerifier(ms)
	h := api.New(api.Config{Store: ms, Verifier: verifier, HTTPBaseURL: httpBaseURL})
	return h, ms, verifier, cleanup, nil
}

// openMetaStore opens the metadata store (URL-scheme dispatch via meta.Open)
// and ensures its schema exists (idempotent). Shared by serve (via
// buildAPIHandler) and the signing-key CLI, which talks to the database
// directly — there is no HTTP surface for the signing-key registry
// (docs/02: upstream manages keys in its dashboard).
func openMetaStore(ctx context.Context, pgURL string) (meta.Store, func(), error) {
	assertf(pgURL != "", "openMetaStore: empty pgURL")
	ms, err := meta.Open(ctx, pgURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open metadata store: %w", err)
	}
	if err := ms.InitSchema(ctx); err != nil {
		ms.Close()
		return nil, nil, fmt.Errorf("init metadata schema: %w", err)
	}
	return ms, func() { ms.Close() }, nil
}
