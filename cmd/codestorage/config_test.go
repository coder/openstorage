package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/jobs"
	"github.com/coder/code-storage/internal/ref"
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

// TestResolveServeConfigCompatForbidsDestructiveLFSSweep (Codex round
// 101, P1): the write-compat flag declares a mixed v0.2.0 fleet — and
// during that roll, DESTRUCTIVE LFS sweeping must be off fleet-wide: the
// in-place row migration makes touched objects' legacy blobs rowless from
// a v0.2.0 sweeper's perspective, and an old reap-enabled sweeper deletes
// them immediately (retained sources are commonly past any threshold),
// breaking old-replica reads mid-roll. Old binaries cannot be guarded
// retroactively; the new binary's startup tripwire forces the operator to
// stage the flags correctly.
func TestResolveServeConfigCompatForbidsDestructiveLFSSweep(t *testing.T) {
	env := map[string]string{
		"CODESTORAGE_ORG":                   "org1",
		"CODESTORAGE_ORG_PUBKEYS":           "/tmp/org.pub.pem",
		"CODESTORAGE_LFS_V020_WRITE_COMPAT": "true",
		"CODESTORAGE_SWEEP_DELETE_LFS":      "true",
	}
	for k, v := range spikeEnv {
		env[k] = v
	}
	if _, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", ""); err == nil {
		t.Fatal("write-compat + destructive LFS sweeping resolved — the combination breaks v0.2.0 replicas mid-roll and must fail startup")
	}
	// The fleet-wide sweep EMERGENCY STOP stays available (Codex round
	// 111): a zero interval constructs no schedule, the delete flag is
	// inert, and re-enabling the interval restarts the process and
	// re-runs this validation — rejecting the combination would block
	// the documented v0.2.0 rollout for operators who stop sweeping
	// without also unsetting the stale delete preference.
	env["CODESTORAGE_SWEEP_INTERVAL"] = "0"
	if _, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", ""); err != nil {
		t.Fatalf("write-compat + inert delete flag under the sweep emergency stop: %v", err)
	}
	delete(env, "CODESTORAGE_SWEEP_INTERVAL")
	// Compat with list-only sweeping stays valid.
	delete(env, "CODESTORAGE_SWEEP_DELETE_LFS")
	if _, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", ""); err != nil {
		t.Fatalf("write-compat with list-only sweeping: %v", err)
	}
}

// TestResolveServeConfigLFSGCDeleteRequiresLFSReap (issue #73, Codex
// security round 13): retirement refunds the object-cap slot, but only the
// sweeper's destructive LFS opt-in deletes the stranded canonical blob —
// without that coupling, a write-scoped token cycles upload → grace →
// retire → re-upload and grows lfs/<repo>/ far past the cap that quota
// floors and sweep listings rely on. Delete-mode reclamation therefore
// requires the reap on an active sweep schedule, with a grace no shorter
// than the reap cadence.
func TestResolveServeConfigLFSGCDeleteRequiresLFSReap(t *testing.T) {
	env := map[string]string{
		"CODESTORAGE_ORG":                   "org1",
		"CODESTORAGE_ORG_PUBKEYS":           "/tmp/org.pub.pem",
		"CODESTORAGE_LFS_GC_INTERVAL":       "1h",
		"CODESTORAGE_LFS_GC_DELETE_ENABLED": "true",
	}
	for k, v := range spikeEnv {
		env[k] = v
	}
	if _, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", ""); err == nil {
		t.Fatal("delete-mode reclamation without the LFS reap resolved — retirement would refund cap slots while stranding their blobs")
	}
	env["CODESTORAGE_SWEEP_DELETE_LFS"] = "true"
	if _, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", ""); err != nil {
		t.Fatalf("delete-mode reclamation with the LFS reap armed: %v", err)
	}
	// A grace below the reap cadence retires slots faster than the reap
	// frees their blobs — the same growth, restated as a transient.
	env["CODESTORAGE_LFS_GC_GRACE"] = "1h" // < the 24h default age threshold
	if _, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", ""); err == nil {
		t.Fatal("grace below the reap cadence resolved — refunds would outrun physical deletion")
	}
	env["CODESTORAGE_SWEEP_AGE_THRESHOLD"] = "30m" // floor becomes the 1h sweep interval
	if _, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", ""); err != nil {
		t.Fatalf("grace at the reap cadence: %v", err)
	}
	// The fleet-wide sweep emergency stop stays available: with the
	// schedule stopped the delete flag demotes to report-only at wiring
	// instead of failing startup.
	env["CODESTORAGE_SWEEP_INTERVAL"] = "0"
	if _, err := resolveServeConfig(mapEnv(env), "", "127.0.0.1:0", "/tmp/cache", ""); err != nil {
		t.Fatalf("sweep emergency stop with armed reclamation delete: %v", err)
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

// TestResolveServeConfigPackReservationTTL (issue #29; floor-cache bound
// added in Codex round 36 on #51): the reservation TTL must STRICTLY
// exceed the usage-floor cache lifetime (githttp.MinPackReservationTTL) —
// with a TTL at or below it, a cached floor can outlive the reservations
// covering objects published after that listing, and a subsequent writer
// sees neither the orphaned bytes nor a live reservation.
func TestResolveServeConfigPackReservationTTL(t *testing.T) {
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
		if cfg.packReservationTTL != defaultPackReservationTTL {
			t.Fatalf("unset TTL resolved to %v, want default %v", cfg.packReservationTTL, defaultPackReservationTTL)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envPackReservationTTL: "61s"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.packReservationTTL != 61*time.Second {
			t.Fatalf("resolved TTL %v, want 61s", cfg.packReservationTTL)
		}
	})

	for _, tc := range []struct{ name, val string }{
		{"malformed", "plenty"},
		{"zero", "0"},
		{"negative", "-5m"},
		{"below-floor-cache", "10s"},
		{"at-floor-cache-boundary", "30s"},
		{"inside-handoff-margin", "31s"},
		{"at-margin-boundary", "60s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveServeConfig(base(map[string]string{envPackReservationTTL: tc.val}), "", "", "/tmp/cache", "")
			if err == nil {
				t.Fatalf("%s=%q resolved without error", envPackReservationTTL, tc.val)
			}
			if !strings.Contains(err.Error(), envPackReservationTTL) {
				t.Fatalf("error %q does not name %s", err, envPackReservationTTL)
			}
		})
	}
}

func TestResolveServeConfigLFSStaging(t *testing.T) {
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
		if cfg.lfsStagingBytes != defaultLFSStagingBytes {
			t.Fatalf("unset staging budget resolved to %d, want default %d", cfg.lfsStagingBytes, defaultLFSStagingBytes)
		}
	})

	t.Run("explicit-zero-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envLFSStagingBytes: "0"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.lfsStagingBytes != 0 {
			t.Fatalf("explicit zero resolved to %d, want 0 (disabled)", cfg.lfsStagingBytes)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envLFSStagingBytes: "1048576"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.lfsStagingBytes != 1<<20 {
			t.Fatalf("resolved staging budget %d, want %d", cfg.lfsStagingBytes, 1<<20)
		}
	})

	for _, tc := range []struct{ name, val string }{
		{"malformed", "plenty"},
		{"negative", "-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveServeConfig(base(map[string]string{envLFSStagingBytes: tc.val}), "", "", "/tmp/cache", "")
			if err == nil {
				t.Fatalf("%s=%q resolved without error", envLFSStagingBytes, tc.val)
			}
			if !strings.Contains(err.Error(), envLFSStagingBytes) {
				t.Fatalf("error %q does not name %s", err, envLFSStagingBytes)
			}
		})
	}
}

func TestResolveServeConfigMaxPushBytes(t *testing.T) {
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

	t.Run("unset-derives-from-quota", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(nil), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if want := int64(defaultMaxPushQuotaMultiple) * defaultPackQuotaBytes; cfg.maxPushBytes != want {
			t.Fatalf("unset cap resolved to %d, want %d (multiple of default quota)", cfg.maxPushBytes, want)
		}
	})

	t.Run("unset-floors-on-tiny-quota", func(t *testing.T) {
		// A byte-scale quota (which the quota resolver accepts) must not
		// derive a cap below protocol overhead: delete-only/zero-growth
		// pushes are the over-quota recovery path and still carry command
		// lists plus the sniffable shallow prefix.
		cfg, err := resolveServeConfig(base(map[string]string{envPackQuota: "1"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.maxPushBytes != defaultMaxPushFloorBytes {
			t.Fatalf("cap for 1-byte quota resolved to %d, want floor %d", cfg.maxPushBytes, defaultMaxPushFloorBytes)
		}
	})

	t.Run("explicit-value-not-floored", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envMaxPushBytes: "512"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.maxPushBytes != 512 {
			t.Fatalf("explicit tiny cap resolved to %d, want 512 verbatim", cfg.maxPushBytes)
		}
	})

	t.Run("unset-with-disabled-quota-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envPackQuota: "0"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.maxPushBytes != 0 {
			t.Fatalf("cap with disabled quota resolved to %d, want 0 (disabled)", cfg.maxPushBytes)
		}
	})

	t.Run("unset-saturates-on-huge-quota", func(t *testing.T) {
		huge := strconv.FormatInt(math.MaxInt64-1, 10)
		cfg, err := resolveServeConfig(base(map[string]string{envPackQuota: huge}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.maxPushBytes != math.MaxInt64 {
			t.Fatalf("cap for quota %s resolved to %d, want MaxInt64 saturation", huge, cfg.maxPushBytes)
		}
	})

	t.Run("explicit-zero-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envMaxPushBytes: "0"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.maxPushBytes != 0 {
			t.Fatalf("explicit zero resolved to %d, want 0 (disabled)", cfg.maxPushBytes)
		}
	})

	t.Run("explicit-overrides-quota-derivation", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envMaxPushBytes: "4096"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.maxPushBytes != 4096 {
			t.Fatalf("resolved cap %d, want 4096", cfg.maxPushBytes)
		}
	})

	for _, tc := range []struct{ name, val string }{
		{"malformed", "plenty"},
		{"negative", "-1"},
		{"float", "1.5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveServeConfig(base(map[string]string{envMaxPushBytes: tc.val}), "", "", "/tmp/cache", "")
			if err == nil {
				t.Fatalf("%s=%q resolved without error", envMaxPushBytes, tc.val)
			}
			if !strings.Contains(err.Error(), envMaxPushBytes) {
				t.Fatalf("error %q does not name %s", err, envMaxPushBytes)
			}
		})
	}
}

func TestResolveServeConfigVerifySigBudget(t *testing.T) {
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
		if cfg.verifySigBudget != defaultVerifySigBudget {
			t.Fatalf("unset budget resolved to %d, want default %d", cfg.verifySigBudget, defaultVerifySigBudget)
		}
	})

	t.Run("explicit-zero-disables", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envVerifySigBudget: "0"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.verifySigBudget != 0 {
			t.Fatalf("explicit zero resolved to %d, want 0 (disabled)", cfg.verifySigBudget)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envVerifySigBudget: "25"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.verifySigBudget != 25 {
			t.Fatalf("resolved budget %d, want 25", cfg.verifySigBudget)
		}
	})

	for _, tc := range []struct{ name, val string }{
		{"malformed", "plenty"},
		{"negative", "-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveServeConfig(base(map[string]string{envVerifySigBudget: tc.val}), "", "", "/tmp/cache", "")
			if err == nil {
				t.Fatalf("%s=%q resolved without error", envVerifySigBudget, tc.val)
			}
			if !strings.Contains(err.Error(), envVerifySigBudget) {
				t.Fatalf("error %q does not name %s", err, envVerifySigBudget)
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
			envSweepDeleteLFS:        "true",
		}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		want := sweepSettings{interval: 30 * time.Minute, ageThreshold: 48 * time.Hour, deleteEnabled: true, deleteLFS: true}
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

	t.Run("repo-retention", func(t *testing.T) {
		cfg, err := resolveServeConfig(base(map[string]string{envRepoRetention: "720h"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.sweep.repoRetention != 720*time.Hour {
			t.Fatalf("repoRetention = %v, want 720h", cfg.sweep.repoRetention)
		}
		// "0" keeps the background purge disabled explicitly.
		cfg, err = resolveServeConfig(base(map[string]string{envRepoRetention: "0"}), "", "", "/tmp/cache", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.sweep.repoRetention != 0 {
			t.Fatalf("repoRetention = %v, want 0 (disabled)", cfg.sweep.repoRetention)
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
			"malformed lfs":       {envSweepDeleteLFS: "sure"},
			// Repo retention (issue #52): fail fast on values below the
			// in-flight-writer floor or with sweeping disabled.
			"malformed retention":        {envRepoRetention: "forever"},
			"negative retention":         {envRepoRetention: "-24h"},
			"retention below floor":      {envRepoRetention: "30m"},
			"retention without sweeping": {envRepoRetention: "72h", envSweepInterval: "0"},
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

func TestResolveWebhookAllowPrivate(t *testing.T) {
	for name, tc := range map[string]struct {
		raw     string
		want    bool
		wantErr bool
	}{
		"unset defaults strict": {},
		"false":                 {raw: "false"},
		"true":                  {raw: "true", want: true},
		"malformed":             {raw: "sometimes", wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := resolveWebhookAllowPrivate(mapEnv(map[string]string{envWebhookAllowPrivate: tc.raw}))
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), envWebhookAllowPrivate) {
					t.Fatalf("error = %v, want one naming %s", err, envWebhookAllowPrivate)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("resolveWebhookAllowPrivate(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestResolveSecretsCodec(t *testing.T) {
	// 32 zero bytes, base64 — a syntactically valid AES-256 key.
	validKey := base64.StdEncoding.EncodeToString(make([]byte, 32))

	for name, tc := range map[string]struct {
		key, mode       string
		wantNil         bool
		wantDecryptOnly bool
		wantErrNaming   string // env var an error must name; "" = no error
	}{
		"unset disables":            {wantNil: true},
		"valid key encrypts":        {key: validKey},
		"explicit encrypt mode":     {key: validKey, mode: "encrypt"},
		"decrypt-only staging":      {key: validKey, mode: "decrypt-only", wantDecryptOnly: true},
		"malformed key":             {key: "not-base64!!!", wantErrNaming: envSecretsKey},
		"short key":                 {key: base64.StdEncoding.EncodeToString(make([]byte, 16)), wantErrNaming: envSecretsKey},
		"unknown mode":              {key: validKey, mode: "sideways", wantErrNaming: envSecretsMode},
		"decrypt-only without key":  {mode: "decrypt-only", wantErrNaming: envSecretsMode},
		"mode alone still rejected": {mode: "encrypt", wantNil: true},
	} {
		t.Run(name, func(t *testing.T) {
			codec, err := resolveSecretsCodec(mapEnv(map[string]string{envSecretsKey: tc.key, envSecretsMode: tc.mode}))
			if tc.wantErrNaming != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrNaming) {
					t.Fatalf("error = %v, want one naming %s", err, tc.wantErrNaming)
				}
				if tc.key == validKey {
					return
				}
				// Key-list errors must never echo the key entry itself.
				if tc.key != "" && strings.Contains(err.Error(), tc.key) {
					t.Fatalf("error %q echoes the key entry", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (codec == nil) != tc.wantNil {
				t.Fatalf("codec nil = %v, want %v", codec == nil, tc.wantNil)
			}
			if codec != nil && codec.DecryptOnly != tc.wantDecryptOnly {
				t.Fatalf("DecryptOnly = %v, want %v", codec.DecryptOnly, tc.wantDecryptOnly)
			}
		})
	}
}

// TestResolveLFSGCSettings (issue #73): the reclamation knobs parse
// fail-fast like the sweep settings; the delete opt-in without a schedule
// is a loud misconfiguration.
func TestResolveLFSGCSettings(t *testing.T) {
	env := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}

	t.Run("defaults", func(t *testing.T) {
		s, err := resolveLFSGCSettings(env(nil))
		if err != nil {
			t.Fatal(err)
		}
		want := lfsGCSettings{grace: jobs.DefaultLFSReclaimGrace, objectBudget: jobs.DefaultLFSReclaimObjectBudget}
		if s != want {
			t.Fatalf("defaults = %+v, want %+v (disabled, 24h grace, 1M budget)", s, want)
		}
	})

	t.Run("parse-through", func(t *testing.T) {
		s, err := resolveLFSGCSettings(env(map[string]string{
			envLFSGCInterval:      "30m",
			envLFSGCGrace:         "48h",
			envLFSGCObjectBudget:  "5000",
			envLFSGCDeleteEnabled: "true",
		}))
		if err != nil {
			t.Fatal(err)
		}
		want := lfsGCSettings{interval: 30 * time.Minute, grace: 48 * time.Hour, objectBudget: 5000, deleteEnabled: true}
		if s != want {
			t.Fatalf("settings = %+v, want %+v", s, want)
		}
	})

	t.Run("delete-requires-schedule", func(t *testing.T) {
		if _, err := resolveLFSGCSettings(env(map[string]string{envLFSGCDeleteEnabled: "true"})); err == nil {
			t.Fatal("delete opt-in with no schedule must fail loudly")
		}
	})

	for name, bad := range map[string]map[string]string{
		"negative interval": {envLFSGCInterval: "-1h"},
		"zero grace":        {envLFSGCGrace: "0"},
		"zero budget":       {envLFSGCInterval: "1h", envLFSGCObjectBudget: "0"},
		"malformed budget":  {envLFSGCObjectBudget: "many"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveLFSGCSettings(env(bad)); err == nil {
				t.Fatalf("%v must fail", bad)
			}
		})
	}
}

// TestResolveLFSMaxObjectsPerRepo (issue #73 option 3): the operator escape
// hatch parses positively or fails loudly.
func TestResolveLFSMaxObjectsPerRepo(t *testing.T) {
	env := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	if v, err := resolveLFSMaxObjectsPerRepo(env(nil)); err != nil || v != ref.MaxLFSObjectsPerRepo {
		t.Fatalf("default = (%d, %v), want built-in %d", v, err, ref.MaxLFSObjectsPerRepo)
	}
	if v, err := resolveLFSMaxObjectsPerRepo(env(map[string]string{envLFSMaxObjectsPerRepo: "250000"})); err != nil || v != 250000 {
		t.Fatalf("override = (%d, %v), want 250000", v, err)
	}
	for _, bad := range []string{"0", "-5", "lots"} {
		if _, err := resolveLFSMaxObjectsPerRepo(env(map[string]string{envLFSMaxObjectsPerRepo: bad})); err == nil {
			t.Fatalf("%q must fail", bad)
		}
	}
}
