package auth

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/v2"
	"encoding/pem"
	"strings"
	"testing"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"
)

// TestGenerateKey_DefaultAlgorithm checks each allowed algorithm yields a
// key of the right family and that DefaultAlgorithm inverts GenerateKey
// (except PS256, which shares RSA keys with RS256 and defaults to RS256).
func TestGenerateKey_DefaultAlgorithm(t *testing.T) {
	wantDefault := map[string]string{
		"ES256": "ES256", "ES384": "ES384", "ES512": "ES512",
		"RS256": "RS256", "PS256": "RS256", "EdDSA": "EdDSA",
	}
	for _, alg := range AllowedAlgorithms {
		key := mustGenerateKey(t, alg)
		switch alg {
		case "ES256", "ES384", "ES512":
			if _, ok := key.(*ecdsa.PrivateKey); !ok {
				t.Errorf("%s: key = %T, want *ecdsa.PrivateKey", alg, key)
			}
		case "RS256", "PS256":
			if _, ok := key.(*rsa.PrivateKey); !ok {
				t.Errorf("%s: key = %T, want *rsa.PrivateKey", alg, key)
			}
		case "EdDSA":
			if _, ok := key.(ed25519.PrivateKey); !ok {
				t.Errorf("%s: key = %T, want ed25519.PrivateKey", alg, key)
			}
		}
		got, err := DefaultAlgorithm(key)
		if err != nil {
			t.Errorf("%s: DefaultAlgorithm: %v", alg, err)
		} else if got != wantDefault[alg] {
			t.Errorf("%s: DefaultAlgorithm = %s, want %s", alg, got, wantDefault[alg])
		}
	}

	if _, err := GenerateKey("HS256"); err == nil {
		t.Error("GenerateKey(HS256) succeeded")
	}
}

// TestPrivateKeyPEMRoundTrip marshals and re-parses one private key per
// family, comparing public halves via their PKIX encoding.
func TestPrivateKeyPEMRoundTrip(t *testing.T) {
	for _, alg := range []string{"ES256", "RS256", "EdDSA"} {
		key := mustGenerateKey(t, alg)
		pemBytes, err := MarshalPrivateKeyPEM(key)
		if err != nil {
			t.Fatalf("%s: MarshalPrivateKeyPEM: %v", alg, err)
		}
		parsed, err := ParsePrivateKeyPEM(pemBytes)
		if err != nil {
			t.Fatalf("%s: ParsePrivateKeyPEM: %v", alg, err)
		}
		want, err := x509.MarshalPKIXPublicKey(key.Public())
		if err != nil {
			t.Fatal(err)
		}
		got, err := x509.MarshalPKIXPublicKey(parsed.Public())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: round-tripped key has a different public half", alg)
		}
	}
}

// TestParsePrivateKeyPEM_SEC1 accepts the legacy "EC PRIVATE KEY" encoding
// named in docs/reference/authentication.md.
func TestParsePrivateKeyPEM_SEC1(t *testing.T) {
	key := mustGenerateKey(t, "ES256").(*ecdsa.PrivateKey)
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	sec1 := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
	parsed, err := ParsePrivateKeyPEM(sec1)
	if err != nil {
		t.Fatalf("ParsePrivateKeyPEM(SEC1): %v", err)
	}
	if !key.PublicKey.Equal(parsed.Public()) {
		t.Fatal("SEC1 round-tripped key has a different public half")
	}
}

func TestParsePrivateKeyPEM_Rejections(t *testing.T) {
	key := mustGenerateKey(t, "ES256")
	pubPEM, err := MarshalPublicKeyPEM(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"empty":            nil,
		"garbage":          []byte("not pem"),
		"public key block": pubPEM,
	} {
		if _, err := ParsePrivateKeyPEM(data); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}

// TestMintToken_Rejections covers key/alg mechanics; claim contents are
// deliberately unchecked (tests must be able to mint invalid tokens).
func TestMintToken_Rejections(t *testing.T) {
	ecKey := mustGenerateKey(t, "ES256")
	if _, err := MintToken(ecKey, "RS256", validClaims("acme")); err == nil {
		t.Error("MintToken signed RS256 with an ECDSA key")
	}
	if _, err := MintToken(ecKey, "ES384", validClaims("acme")); err == nil {
		t.Error("MintToken signed ES384 with a P-256 key")
	}
	if _, err := MintToken(ecKey, "HS256", validClaims("acme")); err == nil {
		t.Error("MintToken accepted a disallowed algorithm")
	}
}

// Guard the JWX member-name escaping fix against dependency downgrades.
// MintToken currently uses only fixed header names, not caller-supplied names.
func TestJWS_CustomHeaderNameEscaped(t *testing.T) {
	key := mustGenerateKey(t, "EdDSA")
	const name = `x":0,"admin`
	headers := jws.NewHeaders()
	if err := headers.Set(name, true); err != nil {
		t.Fatal(err)
	}
	signed, err := jws.Sign([]byte(`{}`), jws.WithKey(jwa.EdDSA(), key, jws.WithProtectedHeaders(headers)))
	if err != nil {
		t.Fatal(err)
	}
	encoded, _, ok := strings.Cut(string(signed), ".")
	if !ok {
		t.Fatal("signed JWS has no protected header segment")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(decoded, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["admin"]; ok {
		t.Fatalf("custom name injected an admin header: %s", decoded)
	}
	if got[name] != true || len(got) != 2 || got["alg"] != "EdDSA" {
		t.Fatalf("protected headers = %s, want only alg and the exact custom name", decoded)
	}
}

// TestMarshalPublicKeyPEM_ParsesBack ensures minted public PEM feeds the
// contract-side parser for every key family.
func TestMarshalPublicKeyPEM_ParsesBack(t *testing.T) {
	for _, alg := range []string{"ES256", "ES384", "ES512", "RS256", "EdDSA"} {
		key := mustGenerateKey(t, alg)
		pemBytes, err := MarshalPublicKeyPEM(key.Public())
		if err != nil {
			t.Fatalf("%s: MarshalPublicKeyPEM: %v", alg, err)
		}
		keys, err := ParsePublicKeysPEM(pemBytes)
		if err != nil {
			t.Fatalf("%s: ParsePublicKeysPEM: %v", alg, err)
		}
		if len(keys) != 1 {
			t.Fatalf("%s: got %d keys, want 1", alg, len(keys))
		}
	}
}
