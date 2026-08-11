package api

// storeVerifier is a TEST-SCOPED implementation of the frozen auth.Verifier
// contract (jwx v3, keys resolved through meta.Store), used to drive the
// integration matrix until the sibling phase-1 slice lands the production
// internal/auth verifier. INTEGRATION NOTE: once that verifier exists,
// newTestDeps (env_test.go) should construct it instead and this file can be
// deleted — nothing else refers to storeVerifier.

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"

	"codestorage/internal/auth"
	"codestorage/internal/meta"
)

// signatureAlgorithms maps the docs/02 allow-list to jwx algorithm objects.
// `none` and HS* are absent by construction and therefore always rejected.
var signatureAlgorithms = map[string]jwa.SignatureAlgorithm{
	"ES256": jwa.ES256(),
	"RS256": jwa.RS256(),
	"EdDSA": jwa.EdDSA(),
	"PS256": jwa.PS256(),
	"ES384": jwa.ES384(),
	"ES512": jwa.ES512(),
}

type storeVerifier struct {
	store meta.Store
}

var _ auth.Verifier = (*storeVerifier)(nil)

// keyMatchesAlg reports whether key's family can carry alg, so we do not
// attempt e.g. an RSA verification with a P-256 key.
func keyMatchesAlg(key crypto.PublicKey, alg string) bool {
	switch k := key.(type) {
	case *ecdsa.PublicKey:
		switch alg {
		case "ES256":
			return k.Curve == elliptic.P256()
		case "ES384":
			return k.Curve == elliptic.P384()
		case "ES512":
			return k.Curve == elliptic.P521()
		}
		return false
	case *rsa.PublicKey:
		return alg == "RS256" || alg == "PS256"
	case ed25519.PublicKey:
		return alg == "EdDSA"
	default:
		return false
	}
}

// decodeSegment base64url-decodes one compact-JWT segment.
func decodeSegment(seg string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(seg)
}

func (v *storeVerifier) Verify(ctx context.Context, token string) (*auth.Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("token has %d segments, want 3", len(parts))
	}

	headerJSON, err := decodeSegment(parts[0])
	if err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}
	alg, allowed := signatureAlgorithms[header.Alg]
	if !allowed {
		return nil, fmt.Errorf("algorithm %q not allowed", header.Alg)
	}

	// Peek at the unverified iss claim to resolve the org's keys; the claims
	// are only trusted after jws.Verify succeeds below.
	unverified, err := decodeSegment(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	var issOnly struct {
		Iss string `json:"iss"`
	}
	if err := json.Unmarshal(unverified, &issOnly); err != nil {
		return nil, fmt.Errorf("parse payload: %w", err)
	}
	if issOnly.Iss == "" {
		return nil, errors.New("missing iss claim")
	}

	pems, err := v.store.PublicKeysForOrg(ctx, issOnly.Iss)
	if err != nil {
		return nil, fmt.Errorf("resolve org %q keys: %w", issOnly.Iss, err)
	}
	var keys []crypto.PublicKey
	for _, pem := range pems {
		parsed, err := auth.ParsePublicKeysPEM([]byte(pem))
		if err != nil {
			return nil, fmt.Errorf("org %q has an unparseable registered key: %w", issOnly.Iss, err)
		}
		keys = append(keys, parsed...)
	}

	var payload []byte
	verified := false
	for _, key := range keys {
		if !keyMatchesAlg(key, header.Alg) {
			continue
		}
		payload, err = jws.Verify([]byte(token), jws.WithKey(alg, key))
		if err == nil {
			verified = true
			break
		}
	}
	if !verified {
		return nil, errors.New("signature verification failed against all registered org keys")
	}

	var claims auth.Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("parse claims: %w", err)
	}
	assertf(claims.Iss == issOnly.Iss, "verified iss %q differs from unverified peek %q", claims.Iss, issOnly.Iss)
	if claims.Sub != auth.SubjectLiteral {
		return nil, fmt.Errorf("sub %q != %q", claims.Sub, auth.SubjectLiteral)
	}
	if claims.Exp <= time.Now().Unix() {
		return nil, errors.New("token expired")
	}
	return &claims, nil
}
