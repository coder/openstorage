// Serve/CLI configuration resolution: flags take precedence, then the
// canonical CODESTORAGE_* environment variables, then the SPIKE_*/legacy
// spike aliases (PG_URL, BLOB_URL) so phase-0 invocations keep working.
package main

import (
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Environment variable names, canonical first; envFirst scans left to right.
var (
	envPGURL   = []string{"CODESTORAGE_PG_URL", "SPIKE_PG_URL", "PG_URL"}
	envBlobURL = []string{"CODESTORAGE_S3_URL", "SPIKE_S3_URL", "BLOB_URL"}
)

const (
	envListen     = "CODESTORAGE_LISTEN"
	envOrg        = "CODESTORAGE_ORG"
	envOrgPubKeys = "CODESTORAGE_ORG_PUBKEYS"
	envBaseURL    = "CODESTORAGE_BASE_URL"

	defaultListen = "127.0.0.1:0"
)

// envFirst returns the first non-empty value among names. getenv is injected
// so tests never mutate process env.
func envFirst(getenv func(string) string, names ...string) string {
	assertf(getenv != nil && len(names) > 0, "envFirst: missing getenv or names")
	for _, n := range names {
		if v := getenv(n); v != "" {
			return v
		}
	}
	return ""
}

// serveConfig is the fully resolved `serve` configuration.
type serveConfig struct {
	listen   string
	cacheDir string
	pgURL    string
	blobURL  string

	// org/orgKeysPath configure single-org serve mode (ADR-8): the /api/v1
	// HTTP API and the git smart-HTTP path both authenticate against the
	// org's registered keys. Both are required.
	org         string
	orgKeysPath string

	// baseURL, when non-empty, is the externally reachable base
	// (scheme://host[:port], no trailing slash) used for `http_url` values
	// in API responses. Empty means derive it from the bound listen
	// address — which is unroutable when listening on 0.0.0.0.
	baseURL string
}

// resolveServeConfig merges flags and environment into a validated config.
func resolveServeConfig(getenv func(string) string, flagListen, flagAddr, flagCacheDir string) (serveConfig, error) {
	assertf(getenv != nil, "resolveServeConfig: nil getenv")
	cfg := serveConfig{
		listen:      firstNonEmpty(flagListen, flagAddr, getenv(envListen), defaultListen),
		cacheDir:    firstNonEmpty(flagCacheDir, filepath.Join(os.TempDir(), "codestorage-cache")),
		pgURL:       envFirst(getenv, envPGURL...),
		blobURL:     envFirst(getenv, envBlobURL...),
		org:         getenv(envOrg),
		orgKeysPath: getenv(envOrgPubKeys),
	}
	if cfg.pgURL == "" {
		return serveConfig{}, fmt.Errorf("postgres URL required: set %s (or legacy %s)", envPGURL[0], envPGURL[2])
	}
	if cfg.blobURL == "" {
		return serveConfig{}, fmt.Errorf("blob URL required: set %s (or legacy %s)", envBlobURL[0], envBlobURL[2])
	}
	if cfg.org == "" || cfg.orgKeysPath == "" {
		return serveConfig{}, fmt.Errorf("%s and %s must both be set (single-org serve mode)", envOrg, envOrgPubKeys)
	}
	if raw := getenv(envBaseURL); raw != "" {
		baseURL, err := normalizeBaseURL(raw)
		if err != nil {
			return serveConfig{}, fmt.Errorf("%s: %w", envBaseURL, err)
		}
		cfg.baseURL = baseURL
	}
	return cfg, nil
}

// normalizeBaseURL validates an externally reachable http(s) base URL and
// strips any trailing slash (http_url values append "/<repo_id>.git").
func normalizeBaseURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("want http(s)://host[:port], got %q", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("base URL must not carry a query or fragment: %q", raw)
	}
	return strings.TrimRight(u.String(), "/"), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// splitPEMBlocks re-encodes each PEM block of data as its own string, the
// shape meta.Store.UpsertOrgWithKeys expects. Errors when no block decodes.
func splitPEMBlocks(data []byte) ([]string, error) {
	var blocks []string
	for {
		var b *pem.Block
		b, data = pem.Decode(data)
		if b == nil {
			break
		}
		blocks = append(blocks, string(pem.EncodeToMemory(b)))
	}
	if len(blocks) == 0 {
		return nil, errors.New("no PEM blocks found")
	}
	return blocks, nil
}

// assertf is a fail-fast invariant check (matches the internal packages'
// crash-and-burn style). Only for impossible conditions, never input errors.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("codestorage assertion failed: "+format, args...))
	}
}
