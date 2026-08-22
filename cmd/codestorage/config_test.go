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
	"time"

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

func TestResolveServeConfigTrustedProxies(t *testing.T) {
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

	t.Run("unset-means-none", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(nil), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.trustedProxies) != 0 {
			t.Fatalf("unset env resolved to %v, want none", cfg.trustedProxies)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{
			// CIDRs, a bare IPv4 (single-address entry), a bare
			// IPv4-mapped IPv6 (unmapped to /32), an IPv4-mapped CIDR
			// (normalized to IPv4, Codex round 1), and whitespace padding.
			envTrustedProxies: "10.0.0.0/8, 192.0.2.7 ,2001:db8::/32,::ffff:198.51.100.9,::ffff:172.16.0.0/108",
		}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"10.0.0.0/8", "192.0.2.7/32", "2001:db8::/32", "198.51.100.9/32", "172.16.0.0/12"}
		if len(cfg.trustedProxies) != len(want) {
			t.Fatalf("resolved %v, want %v", cfg.trustedProxies, want)
		}
		for i, p := range cfg.trustedProxies {
			if p.String() != want[i] {
				t.Errorf("prefix %d = %s, want %s", i, p, want[i])
			}
		}
	})

	// Validated even without CODESTORAGE_RATE_* set: a typo must surface at
	// startup, not when limits are later enabled.
	for name, val := range map[string]string{
		"malformed-cidr": "10.0.0.0/33",
		"not-an-ip":      "proxy.example",
		"empty-entry":    "10.0.0.1,,10.0.0.2",
		"trailing-comma": "10.0.0.1,",
		"zoned-address":  "fe80::1%eth0",
		// A 4-in-6 prefix below /96 covers more than the mapped-IPv4
		// space and has no IPv4 equivalent (Codex round 1).
		"mapped-cidr-below-96": "::ffff:10.0.0.0/64",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := resolveServeConfig(base(map[string]string{envTrustedProxies: val}), "", "", "/tmp/cache", "")
			if err == nil {
				t.Fatalf("%s=%q resolved without error", envTrustedProxies, val)
			}
			if !strings.Contains(err.Error(), envTrustedProxies) {
				t.Fatalf("error %q does not name %s", err, envTrustedProxies)
			}
		})
	}
}

func TestResolveServeConfigPackQuota(t *testing.T) {
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

	t.Run("unset-applies-default", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(nil), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.packQuotaBytes != defaultPackQuotaBytes {
			t.Fatalf("unset quota resolved to %d, want default %d", cfg.packQuotaBytes, defaultPackQuotaBytes)
		}
	})

	t.Run("explicit-zero-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envPackQuota: "0"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.packQuotaBytes != 0 {
			t.Fatalf("explicit zero resolved to %d, want 0 (disabled)", cfg.packQuotaBytes)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envPackQuota: "1048576"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.packQuotaBytes != 1<<20 {
			t.Fatalf("resolved quota %d, want %d", cfg.packQuotaBytes, 1<<20)
		}
	})

	for _, tc := range []struct{ name, val string }{
		{"malformed", "plenty"},
		{"negative", "-1"},
		{"float", "1.5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveServeConfig(base(map[string]string{envPackQuota: tc.val}), "", "", "/tmp/cache", "")
			if err == nil {
				t.Fatalf("%s=%q resolved without error", envPackQuota, tc.val)
			}
			if !strings.Contains(err.Error(), envPackQuota) {
				t.Fatalf("error %q does not name %s", err, envPackQuota)
			}
		})
	}
}

func TestResolveServeConfigSweep(t *testing.T) {
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

	t.Run("defaults", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(nil), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		// deleteSuperseded defaults ON (issue #31): the provably-folded
		// class ships delete-enabled while general orphans stay list-only.
		want := sweepSettings{interval: defaultSweepInterval, ageThreshold: defaultSweepAgeThreshold, deleteSuperseded: true}
		if cfg.sweep != want {
			t.Fatalf("default sweep = %+v, want %+v (hourly, 24h threshold, list-only)", cfg.sweep, want)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{
			envSweepInterval:         "30m",
			envSweepAgeThreshold:     "48h",
			envSweepDeleteEnabled:    "true",
			envSweepDeleteSuperseded: "false",
		}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		want := sweepSettings{interval: 30 * time.Minute, ageThreshold: 48 * time.Hour, deleteEnabled: true}
		if cfg.sweep != want {
			t.Fatalf("sweep = %+v, want %+v", cfg.sweep, want)
		}
	})

	t.Run("zero-interval-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envSweepInterval: "0"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.sweep.interval != 0 {
			t.Fatalf("interval = %v, want 0 (disabled)", cfg.sweep.interval)
		}
	})

	t.Run("errors", func(t *testing.T) {
		cases := map[string]map[string]string{
			"malformed interval":  {envSweepInterval: "soon"},
			"negative interval":   {envSweepInterval: "-1h"},
			"malformed threshold": {envSweepAgeThreshold: "old"},
			"zero threshold":      {envSweepAgeThreshold: "0"},
			"negative threshold":  {envSweepAgeThreshold: "-24h"},
			"malformed delete":    {envSweepDeleteEnabled: "yes please"},
		}
		for name, extra := range cases {
			t.Run(name, func(t *testing.T) {
				if _, err := resolveServeConfig(base(extra), "", "", "/tmp/cache", ""); err == nil {
					t.Fatalf("%v must fail resolution", extra)
				}
			})
		}
	})
}

func TestResolveServeConfigRepackThreshold(t *testing.T) {
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

	t.Run("default", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(nil), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.repackThreshold != defaultRepackThreshold {
			t.Fatalf("repackThreshold = %d, want default %d", cfg.repackThreshold, defaultRepackThreshold)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envRepackThreshold: "3"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.repackThreshold != 3 {
			t.Fatalf("repackThreshold = %d, want 3", cfg.repackThreshold)
		}
	})

	t.Run("zero-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envRepackThreshold: "0"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.repackThreshold != 0 {
			t.Fatalf("repackThreshold = %d, want 0 (disabled)", cfg.repackThreshold)
		}
	})

	t.Run("errors", func(t *testing.T) {
		for name, raw := range map[string]string{
			"malformed": "many", "negative": "-1",
			"one is a no-op fold": "1",
		} {
			t.Run(name, func(t *testing.T) {
				_, err := resolveServeConfig(base(map[string]string{envRepackThreshold: raw}), "", "", "/tmp/cache", "")
				if err == nil || !strings.Contains(err.Error(), envRepackThreshold) {
					t.Fatalf("%s=%q: error %v must name the env var", envRepackThreshold, raw, err)
				}
			})
		}
	})
}
