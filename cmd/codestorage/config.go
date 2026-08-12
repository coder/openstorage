// Serve/CLI configuration resolution: flags take precedence, then the
// canonical CODESTORAGE_* environment variables, then the SPIKE_*/legacy
// spike aliases (PG_URL, BLOB_URL) so phase-0 invocations keep working.
package main

import (
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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
	envRateRPS    = "CODESTORAGE_RATE_RPS"
	envRateBurst  = "CODESTORAGE_RATE_BURST"

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

	// rateRPS/rateBurst configure the per-client-IP REST rate limit
	// (docs/03 gateway-level 429). Both zero means disabled.
	rateRPS   float64
	rateBurst int
}

// storageURLs is the resolved storage location set shared by every
// subcommand that opens the stores (serve, signing-key, list-packs):
// explicit env URLs win, then --data-dir/$CODESTORAGE_DATA_DIR derivation
// (local mode) fills the empty slots. Callers validate the slots they need.
type storageURLs struct {
	dataDir string // absolute when set; "" outside local mode
	dbURL   string // "" when neither env nor data-dir provided one
	blobURL string // "" when neither env nor data-dir provided one
	// cacheDir is derived from dataDir only ("" otherwise); serve applies
	// its own flag/default precedence on top.
	cacheDir string
}

// resolveStorageURLs merges the storage env vars with the local-mode data
// dir (flag > env). The data dir is made absolute up front: hooks re-exec
// this binary from a different working directory (the bare cache repo), so
// relative sqlite/file URLs would silently point somewhere else across the
// re-exec.
func resolveStorageURLs(getenv func(string) string, flagDataDir string) (storageURLs, error) {
	assertf(getenv != nil, "resolveStorageURLs: nil getenv")
	u := storageURLs{
		dbURL:   envFirst(getenv, envPGURL...),
		blobURL: envFirst(getenv, envBlobURL...),
		dataDir: firstNonEmpty(flagDataDir, getenv(envDataDir)),
	}
	if u.dataDir != "" {
		abs, err := filepath.Abs(u.dataDir)
		if err != nil {
			return storageURLs{}, fmt.Errorf("--data-dir %q: %w", u.dataDir, err)
		}
		u.dataDir = abs
		if u.dbURL == "" {
			u.dbURL = "sqlite://" + filepath.Join(abs, "codestorage.db")
		}
		if u.blobURL == "" {
			u.blobURL = "file://" + filepath.Join(abs, "blobs")
		}
		u.cacheDir = filepath.Join(abs, "cache")
	}
	assertf(u.dataDir == "" || filepath.IsAbs(u.dataDir), "resolveStorageURLs: dataDir %q not absolute", u.dataDir)
	return u, nil
}

// errDBURLRequired/errBlobURLRequired are the shared missing-slot errors so
// every subcommand names the same remediation (env URL or local mode).
func errDBURLRequired() error {
	return fmt.Errorf("database URL required: set %s (or legacy %s), or pass --data-dir / set %s for local mode", envPGURL[0], envPGURL[2], envDataDir)
}

func errBlobURLRequired() error {
	return fmt.Errorf("blob URL required: set %s (or legacy %s), or pass --data-dir / set %s for local mode", envBlobURL[0], envBlobURL[2], envDataDir)
}

// resolveServeConfig merges flags and environment into a validated config.
// Precedence per slot: explicit flag > explicit env > --data-dir derivation
// (local mode) > built-in default (cacheDir only).
func resolveServeConfig(getenv func(string) string, flagListen, flagAddr, flagCacheDir, flagDataDir string) (serveConfig, error) {
	assertf(getenv != nil, "resolveServeConfig: nil getenv")
	urls, err := resolveStorageURLs(getenv, flagDataDir)
	if err != nil {
		return serveConfig{}, err
	}
	cfg := serveConfig{
		listen:      firstNonEmpty(flagListen, flagAddr, getenv(envListen), defaultListen),
		cacheDir:    firstNonEmpty(flagCacheDir, urls.cacheDir),
		dbURL:       urls.dbURL,
		blobURL:     urls.blobURL,
		dataDir:     urls.dataDir,
		org:         getenv(envOrg),
		orgKeysPath: getenv(envOrgPubKeys),
	}
	if cfg.cacheDir == "" {
		cfg.cacheDir = filepath.Join(os.TempDir(), "codestorage-cache")
	}
	if cfg.dbURL == "" {
		return serveConfig{}, errDBURLRequired()
	}
	if cfg.blobURL == "" {
		return serveConfig{}, errBlobURLRequired()
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
	cfg.rateRPS, cfg.rateBurst, err = resolveRateLimit(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	assertf(cfg.dbURL != "" && cfg.blobURL != "" && cfg.cacheDir != "", "resolveServeConfig: unresolved slot in %+v", cfg)
	assertf(cfg.dataDir == "" || filepath.IsAbs(cfg.dataDir), "resolveServeConfig: dataDir %q not absolute", cfg.dataDir)
	return cfg, nil
}

// resolveRateLimit parses CODESTORAGE_RATE_RPS / CODESTORAGE_RATE_BURST into
// the (rps, burst) pair for api.RateLimitConfig. Fail-fast semantics:
//
//   - both unset, or RPS explicitly 0 with burst unset/0 → (0, 0) disabled
//   - RPS > 0 requires BURST >= 1 (half-set pairs are misconfigurations)
//   - BURST set without RPS is a misconfiguration
//   - malformed, negative, NaN, or infinite values error naming the env var
func resolveRateLimit(getenv func(string) string) (float64, int, error) {
	assertf(getenv != nil, "resolveRateLimit: nil getenv")
	rpsRaw, burstRaw := getenv(envRateRPS), getenv(envRateBurst)

	var rps float64
	if rpsRaw != "" {
		v, err := strconv.ParseFloat(rpsRaw, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("%s: %w", envRateRPS, err)
		}
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return 0, 0, fmt.Errorf("%s must be a non-negative finite number, got %q", envRateRPS, rpsRaw)
		}
		rps = v
	}
	var burst int
	if burstRaw != "" {
		v, err := strconv.Atoi(burstRaw)
		if err != nil {
			return 0, 0, fmt.Errorf("%s: %w", envRateBurst, err)
		}
		if v < 0 {
			return 0, 0, fmt.Errorf("%s must be non-negative, got %q", envRateBurst, burstRaw)
		}
		burst = v
	}

	switch {
	case rps == 0 && burst == 0:
		return 0, 0, nil // disabled (unset or explicit zero)
	case rps == 0:
		return 0, 0, fmt.Errorf("%s must be set (> 0) when %s is set", envRateRPS, envRateBurst)
	case burst == 0:
		return 0, 0, fmt.Errorf("%s must be set (>= 1) when %s is set", envRateBurst, envRateRPS)
	}
	return rps, burst, nil
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
