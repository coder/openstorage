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
	envDataDir    = "CODESTORAGE_DATA_DIR"

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
	dbURL    string
	blobURL  string

	// dataDir, when set, is the local-mode data directory (always absolute):
	// database/blob/cache locations are derived from it for slots not set
	// explicitly (sqlite://<dir>/codestorage.db, file://<dir>/blobs,
	// <dir>/cache). Explicit URLs/flags always win.
	dataDir string

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
// Precedence per slot: explicit flag > explicit env > --data-dir derivation
// (local mode) > built-in default (cacheDir only).
func resolveServeConfig(getenv func(string) string, flagListen, flagAddr, flagCacheDir, flagDataDir string) (serveConfig, error) {
	assertf(getenv != nil, "resolveServeConfig: nil getenv")
	cfg := serveConfig{
		listen:      firstNonEmpty(flagListen, flagAddr, getenv(envListen), defaultListen),
		cacheDir:    flagCacheDir,
		dbURL:       envFirst(getenv, envPGURL...),
		blobURL:     envFirst(getenv, envBlobURL...),
		dataDir:     firstNonEmpty(flagDataDir, getenv(envDataDir)),
		org:         getenv(envOrg),
		orgKeysPath: getenv(envOrgPubKeys),
	}
	if cfg.dataDir != "" {
		// Absolute up front: hooks re-exec this binary from a different
		// working directory (the bare cache repo), so relative sqlite/file
		// URLs would silently point somewhere else across the re-exec.
		abs, err := filepath.Abs(cfg.dataDir)
		if err != nil {
			return serveConfig{}, fmt.Errorf("--data-dir %q: %w", cfg.dataDir, err)
		}
		cfg.dataDir = abs
		if cfg.dbURL == "" {
			cfg.dbURL = "sqlite://" + filepath.Join(abs, "codestorage.db")
		}
		if cfg.blobURL == "" {
			cfg.blobURL = "file://" + filepath.Join(abs, "blobs")
		}
		if cfg.cacheDir == "" {
			cfg.cacheDir = filepath.Join(abs, "cache")
		}
	}
	if cfg.cacheDir == "" {
		cfg.cacheDir = filepath.Join(os.TempDir(), "codestorage-cache")
	}
	if cfg.dbURL == "" {
		return serveConfig{}, fmt.Errorf("database URL required: set %s (or legacy %s), or pass --data-dir / set %s for local mode", envPGURL[0], envPGURL[2], envDataDir)
	}
	if cfg.blobURL == "" {
		return serveConfig{}, fmt.Errorf("blob URL required: set %s (or legacy %s), or pass --data-dir / set %s for local mode", envBlobURL[0], envBlobURL[2], envDataDir)
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
	assertf(cfg.dbURL != "" && cfg.blobURL != "" && cfg.cacheDir != "", "resolveServeConfig: unresolved slot in %+v", cfg)
	assertf(cfg.dataDir == "" || filepath.IsAbs(cfg.dataDir), "resolveServeConfig: dataDir %q not absolute", cfg.dataDir)
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
