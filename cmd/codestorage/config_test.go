package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"codestorage/internal/auth"
)

// mapEnv builds a getenv func from a fixed map (no process env mutation).
func mapEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// spikeEnv is the minimal legacy environment the phase-0 spike used.
var spikeEnv = map[string]string{
	"PG_URL":   "postgres://legacy",
	"BLOB_URL": "s3://legacy",
}

func TestEnvFirstPrecedence(t *testing.T) {
	env := mapEnv(map[string]string{
		"CODESTORAGE_PG_URL": "postgres://new",
		"PG_URL":             "postgres://legacy",
	})
	if got := envFirst(env, envPGURL...); got != "postgres://new" {
		t.Fatalf("canonical name should win, got %q", got)
	}
	if got := envFirst(mapEnv(spikeEnv), envPGURL...); got != "postgres://legacy" {
		t.Fatalf("legacy fallback, got %q", got)
	}
	if got := envFirst(mapEnv(nil), envPGURL...); got != "" {
		t.Fatalf("unset env should be empty, got %q", got)
	}
}

func TestResolveServeConfigLegacyEnvCompat(t *testing.T) {
	// Legacy PG_URL/BLOB_URL names and the --addr alias still resolve; the
	// org config is mandatory since ADR-8 dropped git-only serve mode.
	env := map[string]string{
		"CODESTORAGE_ORG":         "org1",
		"CODESTORAGE_ORG_PUBKEYS": "/tmp/org.pub.pem",
	}
	for k, v := range spikeEnv {
		env[k] = v
	}
	cfg, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "127.0.0.1:0" || cfg.cacheDir != "/tmp/cache" {
		t.Fatalf("spike flags not honored: %+v", cfg)
	}
	if cfg.dbURL != "postgres://legacy" || cfg.blobURL != "s3://legacy" {
		t.Fatalf("legacy env not honored: %+v", cfg)
	}
}

func TestResolveServeConfigCanonicalEnv(t *testing.T) {
	env := mapEnv(map[string]string{
		"CODESTORAGE_PG_URL":      "postgres://new",
		"CODESTORAGE_S3_URL":      "s3://new",
		"CODESTORAGE_LISTEN":      "127.0.0.1:8199",
		"CODESTORAGE_ORG":         "selfhost",
		"CODESTORAGE_ORG_PUBKEYS": "/tmp/org.pub.pem",
		"PG_URL":                  "postgres://legacy",
		"BLOB_URL":                "s3://legacy",
	})
	cfg, err := resolveServeConfig(env, "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dbURL != "postgres://new" || cfg.blobURL != "s3://new" {
		t.Fatalf("CODESTORAGE_* must beat legacy names: %+v", cfg)
	}
	if cfg.listen != "127.0.0.1:8199" {
		t.Fatalf("env listen not honored: %+v", cfg)
	}
	if cfg.org != "selfhost" || cfg.orgKeysPath != "/tmp/org.pub.pem" {
		t.Fatalf("org config not honored: %+v", cfg)
	}
	if cfg.cacheDir == "" {
		t.Fatalf("cacheDir must default to a non-empty path: %+v", cfg)
	}
}

func TestResolveServeConfigListenPrecedence(t *testing.T) {
	env := map[string]string{
		"PG_URL": "p", "BLOB_URL": "b", "CODESTORAGE_LISTEN": "env:1",
		"CODESTORAGE_ORG": "o", "CODESTORAGE_ORG_PUBKEYS": "/k",
	}
	cfg, err := resolveServeConfig(mapEnv(env), "flag:1", "addr:1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "flag:1" {
		t.Fatalf("--listen must beat --addr and env, got %q", cfg.listen)
	}
	cfg, err = resolveServeConfig(mapEnv(env), "", "addr:1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "addr:1" {
		t.Fatalf("--addr must beat env, got %q", cfg.listen)
	}
	delete(env, "CODESTORAGE_LISTEN")
	cfg, err = resolveServeConfig(mapEnv(env), "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != defaultListen {
		t.Fatalf("default listen expected, got %q", cfg.listen)
	}
}

func TestResolveServeConfigDataDir(t *testing.T) {
	orgEnv := map[string]string{
		"CODESTORAGE_ORG": "o", "CODESTORAGE_ORG_PUBKEYS": "/k",
	}
	withOrg := func(extra map[string]string) func(string) string {
		m := map[string]string{}
		for k, v := range orgEnv {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return mapEnv(m)
	}

	t.Run("derives db, blob, and cache slots", func(t *testing.T) {
		cfg, err := resolveServeConfig(withOrg(nil), "", "", "", "/data")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.dataDir != "/data" {
			t.Fatalf("--data-dir must thread through, got %q", cfg.dataDir)
		}
		if want := "sqlite://" + filepath.Join("/data", "codestorage.db"); cfg.dbURL != want {
			t.Fatalf("dbURL = %q, want %q", cfg.dbURL, want)
		}
		if want := "file://" + filepath.Join("/data", "blobs"); cfg.blobURL != want {
			t.Fatalf("blobURL = %q, want %q", cfg.blobURL, want)
		}
		if want := filepath.Join("/data", "cache"); cfg.cacheDir != want {
			t.Fatalf("cacheDir = %q, want %q", cfg.cacheDir, want)
		}
	})

	t.Run("env sets the data dir", func(t *testing.T) {
		cfg, err := resolveServeConfig(withOrg(map[string]string{"CODESTORAGE_DATA_DIR": "/envdata"}), "", "", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.dataDir != "/envdata" {
			t.Fatalf("dataDir = %q, want /envdata", cfg.dataDir)
		}
		if want := "sqlite://" + filepath.Join("/envdata", "codestorage.db"); cfg.dbURL != want {
			t.Fatalf("dbURL = %q, want %q", cfg.dbURL, want)
		}
	})

	t.Run("flag beats env", func(t *testing.T) {
		cfg, err := resolveServeConfig(withOrg(map[string]string{"CODESTORAGE_DATA_DIR": "/envdata"}), "", "", "", "/flagdata")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.dataDir != "/flagdata" {
			t.Fatalf("dataDir = %q, want /flagdata (flag beats env)", cfg.dataDir)
		}
	})

	t.Run("explicit slots win over derivation", func(t *testing.T) {
		cfg, err := resolveServeConfig(withOrg(map[string]string{
			"CODESTORAGE_PG_URL": "postgres://explicit",
			"CODESTORAGE_S3_URL": "s3://explicit",
		}), "", "", "/explicit-cache", "/data")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.dbURL != "postgres://explicit" || cfg.blobURL != "s3://explicit" || cfg.cacheDir != "/explicit-cache" {
			t.Fatalf("explicit slots must beat derivation: %+v", cfg)
		}
		if cfg.dataDir != "/data" {
			t.Fatalf("dataDir = %q, want /data", cfg.dataDir)
		}
	})

	t.Run("relative dir becomes absolute", func(t *testing.T) {
		cfg, err := resolveServeConfig(withOrg(nil), "", "", "", "rel-data")
		if err != nil {
			t.Fatal(err)
		}
		if !filepath.IsAbs(cfg.dataDir) {
			t.Fatalf("dataDir %q must be absolute (hook re-exec safety)", cfg.dataDir)
		}
		if want := "sqlite://" + filepath.Join(cfg.dataDir, "codestorage.db"); cfg.dbURL != want {
			t.Fatalf("dbURL = %q, want %q (derived from the absolute dir)", cfg.dbURL, want)
		}
	})

	t.Run("no urls and no data dir errors mentioning --data-dir", func(t *testing.T) {
		_, err := resolveServeConfig(withOrg(nil), "", "", "", "")
		if err == nil || !strings.Contains(err.Error(), "--data-dir") || !strings.Contains(err.Error(), "CODESTORAGE_DATA_DIR") {
			t.Fatalf("want error naming --data-dir/CODESTORAGE_DATA_DIR as the local-mode alternative, got %v", err)
		}
	})
}

// resolveStorageURLs is the derivation shared by serve, signing-key, and
// list-packs: env URLs win, --data-dir fills the empty slots, and missing
// slots stay empty (each subcommand validates only what it needs).
func TestResolveStorageURLs(t *testing.T) {
	t.Run("data dir fills empty slots", func(t *testing.T) {
		u, err := resolveStorageURLs(mapEnv(nil), "/data")
		if err != nil {
			t.Fatal(err)
		}
		if want := "sqlite://" + filepath.Join("/data", "codestorage.db"); u.dbURL != want {
			t.Fatalf("dbURL = %q, want %q", u.dbURL, want)
		}
		if want := "file://" + filepath.Join("/data", "blobs"); u.blobURL != want {
			t.Fatalf("blobURL = %q, want %q", u.blobURL, want)
		}
		if want := filepath.Join("/data", "cache"); u.cacheDir != want {
			t.Fatalf("cacheDir = %q, want %q", u.cacheDir, want)
		}
	})

	t.Run("env urls beat derivation", func(t *testing.T) {
		u, err := resolveStorageURLs(mapEnv(map[string]string{
			"CODESTORAGE_PG_URL": "postgres://explicit",
			"CODESTORAGE_S3_URL": "s3://explicit",
		}), "/data")
		if err != nil {
			t.Fatal(err)
		}
		if u.dbURL != "postgres://explicit" || u.blobURL != "s3://explicit" {
			t.Fatalf("explicit env URLs must win: %+v", u)
		}
	})

	t.Run("env data dir honored without flag", func(t *testing.T) {
		u, err := resolveStorageURLs(mapEnv(map[string]string{"CODESTORAGE_DATA_DIR": "/envdata"}), "")
		if err != nil {
			t.Fatal(err)
		}
		if u.dataDir != "/envdata" {
			t.Fatalf("dataDir = %q, want /envdata", u.dataDir)
		}
	})

	t.Run("relative data dir becomes absolute", func(t *testing.T) {
		u, err := resolveStorageURLs(mapEnv(nil), "rel-data")
		if err != nil {
			t.Fatal(err)
		}
		if !filepath.IsAbs(u.dataDir) {
			t.Fatalf("dataDir %q must be absolute (hook re-exec safety)", u.dataDir)
		}
	})

	t.Run("nothing set leaves slots empty", func(t *testing.T) {
		u, err := resolveStorageURLs(mapEnv(nil), "")
		if err != nil {
			t.Fatal(err)
		}
		if u.dbURL != "" || u.blobURL != "" || u.dataDir != "" || u.cacheDir != "" {
			t.Fatalf("want all slots empty, got %+v", u)
		}
	})
}

func TestResolveServeConfigErrors(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		wantSub string
	}{
		{"missing pg", map[string]string{"BLOB_URL": "b"}, "CODESTORAGE_PG_URL"},
		{"missing blob", map[string]string{"PG_URL": "p"}, "CODESTORAGE_S3_URL"},
		{"org without keys", map[string]string{"PG_URL": "p", "BLOB_URL": "b", "CODESTORAGE_ORG": "o"}, "must both be set"},
		{"keys without org", map[string]string{"PG_URL": "p", "BLOB_URL": "b", "CODESTORAGE_ORG_PUBKEYS": "/o"}, "must both be set"},
		{"no org config at all", map[string]string{"PG_URL": "p", "BLOB_URL": "b"}, "must both be set"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveServeConfig(mapEnv(tc.env), "", "", "", "")
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("want error containing %q, got %v", tc.wantSub, err)
			}
		})
	}
}

func TestResolveServeConfigBaseURL(t *testing.T) {
	base := map[string]string{
		"PG_URL": "p", "BLOB_URL": "b",
		"CODESTORAGE_ORG": "o", "CODESTORAGE_ORG_PUBKEYS": "/k",
	}
	env := func(baseURL string) func(string) string {
		m := map[string]string{"CODESTORAGE_BASE_URL": baseURL}
		for k, v := range base {
			m[k] = v
		}
		return mapEnv(m)
	}

	// Unset: derive from the bound listener (cmdServe).
	cfg, err := resolveServeConfig(mapEnv(base), "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.baseURL != "" {
		t.Fatalf("baseURL should default to empty, got %q", cfg.baseURL)
	}

	// Valid values, trailing slash trimmed.
	for raw, want := range map[string]string{
		"http://127.0.0.1:8081":       "http://127.0.0.1:8081",
		"https://git.example.com/":    "https://git.example.com",
		"https://git.example.com:443": "https://git.example.com:443",
	} {
		cfg, err := resolveServeConfig(env(raw), "", "", "", "")
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if cfg.baseURL != want {
			t.Fatalf("%q: baseURL = %q, want %q", raw, cfg.baseURL, want)
		}
	}

	// Invalid values fail fast at startup, naming the env var.
	for _, raw := range []string{"not-a-url", "ftp://x", "http://", "http://h?x=1", "http://h#frag"} {
		if _, err := resolveServeConfig(env(raw), "", "", "", ""); err == nil || !strings.Contains(err.Error(), "CODESTORAGE_BASE_URL") {
			t.Fatalf("%q: want CODESTORAGE_BASE_URL error, got %v", raw, err)
		}
	}
}

// rsaPEM returns one PKIX "PUBLIC KEY" PEM block for a fresh RSA key.
func rsaPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

// ecdsaKeyPEM generates an ES256 keypair and returns the public PEM bytes.
func ecdsaKeyPEM(t *testing.T) (pubPath string, pubPEM []byte) {
	t.Helper()
	priv, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	pubPEM, err = auth.MarshalPublicKeyPEM(priv.Public())
	if err != nil {
		t.Fatal(err)
	}
	pubPath = filepath.Join(t.TempDir(), "key.pub.pem")
	if err := os.WriteFile(pubPath, pubPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	return pubPath, pubPEM
}

func TestSplitPEMBlocks(t *testing.T) {
	_, ec := ecdsaKeyPEM(t)
	rsa := rsaPEM(t)
	blocks, err := splitPEMBlocks(append(append([]byte("junk before\n"), ec...), rsa...))
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("want 2 blocks, got %d", len(blocks))
	}
	if blocks[0] != string(ec) || blocks[1] != string(rsa) {
		t.Fatalf("blocks must round-trip byte-identically")
	}
	if _, err := splitPEMBlocks([]byte("not pem at all")); err == nil {
		t.Fatal("want error on zero PEM blocks")
	}
}

func TestResolveServeConfigRateLimit(t *testing.T) {
	base := func(extra map[string]string) func(string) string {
		env := map[string]string{
			"CODESTORAGE_PG_URL":      "postgres://x",
			"CODESTORAGE_S3_URL":      "s3://x",
			"CODESTORAGE_ORG":         "org1",
			"CODESTORAGE_ORG_PUBKEYS": "/tmp/org.pub.pem",
		}
		for k, v := range extra {
			env[k] = v
		}
		return mapEnv(env)
	}

	t.Run("unset-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(nil), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.rateRPS != 0 || cfg.rateBurst != 0 {
			t.Fatalf("unset rate env resolved to rps=%v burst=%d, want 0/0", cfg.rateRPS, cfg.rateBurst)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{
			envRateRPS: "2.5", envRateBurst: "7",
		}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.rateRPS != 2.5 || cfg.rateBurst != 7 {
			t.Fatalf("resolved rps=%v burst=%d, want 2.5/7", cfg.rateRPS, cfg.rateBurst)
		}
	})

	t.Run("explicit-zero-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{
			envRateRPS: "0", envRateBurst: "0",
		}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.rateRPS != 0 || cfg.rateBurst != 0 {
			t.Fatalf("explicit zero resolved to rps=%v burst=%d, want 0/0", cfg.rateRPS, cfg.rateBurst)
		}
	})

	errCases := []struct {
		name string
		env  map[string]string
		want string // substring the error must name
	}{
		{"rps-without-burst", map[string]string{envRateRPS: "5"}, envRateBurst},
		{"burst-without-rps", map[string]string{envRateBurst: "5"}, envRateRPS},
		{"rps-zero-burst-set", map[string]string{envRateRPS: "0", envRateBurst: "5"}, envRateRPS},
		{"rps-malformed", map[string]string{envRateRPS: "fast", envRateBurst: "5"}, envRateRPS},
		{"rps-negative", map[string]string{envRateRPS: "-1", envRateBurst: "5"}, envRateRPS},
		{"rps-nan", map[string]string{envRateRPS: "NaN", envRateBurst: "5"}, envRateRPS},
		{"rps-inf", map[string]string{envRateRPS: "+Inf", envRateBurst: "5"}, envRateRPS},
		{"burst-malformed", map[string]string{envRateRPS: "5", envRateBurst: "many"}, envRateBurst},
		{"burst-negative", map[string]string{envRateRPS: "5", envRateBurst: "-2"}, envRateBurst},
		{"burst-float", map[string]string{envRateRPS: "5", envRateBurst: "1.5"}, envRateBurst},
	}
	for _, tc := range errCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveServeConfig(base(tc.env), "", "", "/tmp/cache", "")
			if err == nil {
				t.Fatalf("env %v resolved without error", tc.env)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name %s", err, tc.want)
			}
		})
	}
}
