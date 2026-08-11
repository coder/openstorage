//go:build codestorage_stub

// wire_stub.go lets this package build/test in isolation while the sibling
// phase-1 slices (internal/meta PG store, internal/auth verifier,
// internal/api handlers) land. Normal builds use wire.go; nothing selects
// this file unless -tags codestorage_stub is passed explicitly.
package main

import (
	"context"
	"errors"
	"net/http"
)

func buildAPIHandler(_ context.Context, pgURL, org, httpBaseURL string, orgPEMBlocks []string) (http.Handler, func(), error) {
	assertf(pgURL != "" && org != "" && httpBaseURL != "" && len(orgPEMBlocks) > 0, "buildAPIHandler: missing pgURL/org/httpBaseURL/keys")
	return nil, nil, errors.New("built with -tags codestorage_stub: phase-1 API wiring disabled (see cmd/codestorage/wire.go)")
}
