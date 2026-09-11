package main

// Container-free tests for the keygen/mint CLI helpers (migrated from the
// spike single-ECDSA helpers to internal/auth in phase 2).

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
)

// decodeJWTClaims base64url-decodes the payload segment of a compact JWT.
func decodeJWTClaims(t *testing.T, token string) auth.Claims {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d segments, want 3: %q", len(parts), token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var c auth.Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGenerateKeyPEMFilesAndMint(t *testing.T) {
	// One asymmetric key family per signing path: EC, RSA, Ed25519.
	for _, alg := range []string{"ES256", "RS256", "EdDSA"} {
		t.Run(alg, func(t *testing.T) {
			prefix := filepath.Join(t.TempDir(), "org1")
			privPath, pubPath, err := generateKeyPEMFiles(alg, prefix, false)
			if err != nil {
				t.Fatal(err)
			}
			if privPath != prefix+".pem" || pubPath != prefix+".pub.pem" {
				t.Fatalf("unexpected paths %q, %q", privPath, pubPath)
			}
			pubPEM, err := os.ReadFile(pubPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := auth.ParsePublicKeysPEM(pubPEM); err != nil {
				t.Fatalf("public PEM does not round-trip through the org registry parser: %v", err)
			}

			// Mint with the generated private key; alg inferred from key type.
			token, err := mintToken(privPath, "org1", "acme/widgets",
				"git:read,git:write", `[["refs/heads/main",["no-force-push"]]]`, "", time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			c := decodeJWTClaims(t, token)
			if c.Iss != "org1" || c.Sub != auth.SubjectLiteral || c.Repo != "acme/widgets" {
				t.Fatalf("claims = %+v", c)
			}
			if len(c.Scopes) != 2 || c.Scopes[0] != "git:read" || c.Scopes[1] != "git:write" {
				t.Fatalf("scopes = %v", c.Scopes)
			}
			if c.Exp != c.Iat+3600 {
				t.Fatalf("exp-iat = %d, want 3600", c.Exp-c.Iat)
			}
			if string(c.Refs) != `[["refs/heads/main",["no-force-push"]]]` {
				t.Fatalf("refs passed through modified: %s", c.Refs)
			}
		})
	}
}

func TestGenerateKeyPEMFilesRejectsUnknownAlg(t *testing.T) {
	if _, _, err := generateKeyPEMFiles("HS256", filepath.Join(t.TempDir(), "k"), false); err == nil {
		t.Fatal("want error for disallowed algorithm")
	}
}

func TestMintTokenValidation(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "k")
	privPath, _, err := generateKeyPEMFiles("ES256", prefix, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mintToken(privPath, "o", "r", "git:read", `{"not":"tuples"}`, "", time.Hour); err == nil {
		t.Fatal("want error for malformed --refs JSON")
	}
	if _, err := mintToken(privPath, "o", "r", "git:read", "", "", -time.Second); err == nil {
		t.Fatal("want error for non-positive ttl")
	}
	if _, err := mintToken(privPath, "o", "r", "git:read", "", "EdDSA", time.Hour); err == nil {
		t.Fatal("want error when --alg does not fit the key type")
	}
}
