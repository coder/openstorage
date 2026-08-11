//go:build !codestorage_stub

// wire.go is the single integration seam between the CLI and the phase-1
// implementation slices. It is the ONLY file referencing the concrete
// constructors owned by sibling slices:
//
//   - meta.OpenPG / (*meta.PG).InitSchema / (*meta.PG).Close  (internal/meta)
//   - auth.NewVerifier(store)                                 (internal/auth)
//   - api.New(api.Config{Store, Verifier, HTTPBaseURL})       (internal/api)
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
// handler, the store (for the git path's repo_id->url resolution), and a
// cleanup func. The handler registers full "/api/v1/..." paths, so serve
// mounts it un-stripped at mux.Handle("/api/v1/", h).
func buildAPIHandler(ctx context.Context, pgURL, org, httpBaseURL string, orgPEMBlocks []string) (http.Handler, meta.Store, func(), error) {
	assertf(pgURL != "" && org != "" && httpBaseURL != "" && len(orgPEMBlocks) > 0, "buildAPIHandler: missing pgURL/org/httpBaseURL/keys")
	ms, err := meta.OpenPG(ctx, pgURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open metadata store: %w", err)
	}
	if err := ms.InitSchema(ctx); err != nil {
		ms.Close()
		return nil, nil, nil, fmt.Errorf("init metadata schema: %w", err)
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, org, orgPEMBlocks); err != nil {
		ms.Close()
		return nil, nil, nil, fmt.Errorf("bootstrap org %q: %w", org, err)
	}
	verifier := auth.NewVerifier(ms)
	h := api.New(api.Config{Store: ms, Verifier: verifier, HTTPBaseURL: httpBaseURL})
	return h, ms, func() { ms.Close() }, nil
}
