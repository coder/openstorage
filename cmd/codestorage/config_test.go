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

func TestResolveServeConfigSpikeCompat(t *testing.T) {
	// Exactly how internal/githttp/spike_test.go invokes serve.
	cfg, err := resolveServeConfig(mapEnv(spikeEnv), "", "127.0.0.1:0", "/tmp/cache", "/tmp/key.pub.pem")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "127.0.0.1:0" || cfg.cacheDir != "/tmp/cache" || cfg.gitPubKeyPath != "/tmp/key.pub.pem" {
		t.Fatalf("spike flags not honored: %+v", cfg)
	}
	if cfg.pgURL != "postgres://legacy" || cfg.blobURL != "s3://legacy" {
		t.Fatalf("legacy env not honored: %+v", cfg)
	}
	if cfg.org != "" || cfg.orgKeysPath != "" {
		t.Fatalf("API mode should be off: %+v", cfg)
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
	if cfg.pgURL != "postgres://new" || cfg.blobURL != "s3://new" {
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
	}
	cfg, err := resolveServeConfig(mapEnv(env), "flag:1", "addr:1", "", "/k")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "flag:1" {
		t.Fatalf("--listen must beat --addr and env, got %q", cfg.listen)
	}
	cfg, err = resolveServeConfig(mapEnv(env), "", "addr:1", "", "/k")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "addr:1" {
		t.Fatalf("--addr must beat env, got %q", cfg.listen)
	}
	delete(env, "CODESTORAGE_LISTEN")
	cfg, err = resolveServeConfig(mapEnv(env), "", "", "", "/k")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != defaultListen {
		t.Fatalf("default listen expected, got %q", cfg.listen)
	}
}

func TestResolveServeConfigErrors(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		pubKey  string
		wantSub string
	}{
		{"missing pg", map[string]string{"BLOB_URL": "b"}, "/k", "CODESTORAGE_PG_URL"},
		{"missing blob", map[string]string{"PG_URL": "p"}, "/k", "CODESTORAGE_S3_URL"},
		{"org without keys", map[string]string{"PG_URL": "p", "BLOB_URL": "b", "CODESTORAGE_ORG": "o"}, "/k", "must be set together"},
		{"keys without org", map[string]string{"PG_URL": "p", "BLOB_URL": "b", "CODESTORAGE_ORG_PUBKEYS": "/o"}, "/k", "must be set together"},
		{"nothing to serve", map[string]string{"PG_URL": "p", "BLOB_URL": "b"}, "", "nothing to serve"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveServeConfig(mapEnv(tc.env), "", "", "", tc.pubKey)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("want error containing %q, got %v", tc.wantSub, err)
			}
		})
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

func TestResolveGitPublicKey(t *testing.T) {
	pubPath, ecPEM := ecdsaKeyPEM(t)

	// Explicit --jwt-pubkey path wins.
	key, err := resolveGitPublicKey(pubPath, nil)
	if err != nil || key == nil {
		t.Fatalf("explicit path: key=%v err=%v", key, err)
	}

	// Fallback: first ECDSA key among the org blocks (RSA skipped).
	key, err = resolveGitPublicKey("", []string{string(rsaPEM(t)), string(ecPEM)})
	if err != nil || key == nil {
		t.Fatalf("org-block fallback: key=%v err=%v", key, err)
	}

	// RSA-only org keys: git path disabled, not an error.
	key, err = resolveGitPublicKey("", []string{string(rsaPEM(t))})
	if err != nil || key != nil {
		t.Fatalf("rsa-only must yield (nil, nil), got key=%v err=%v", key, err)
	}

	// No sources at all: disabled.
	key, err = resolveGitPublicKey("", nil)
	if err != nil || key != nil {
		t.Fatalf("no sources must yield (nil, nil), got key=%v err=%v", key, err)
	}
}
