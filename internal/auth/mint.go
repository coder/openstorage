package auth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"

	"github.com/lestrrat-go/jwx/v3/jws"
)

// This file holds key-generation and token-minting helpers for tests and the
// CLI (keygen/mint subcommands). The service itself never mints tokens —
// customers sign their own (docs/reference/authentication.md).

// GenerateKey creates a fresh private key for the named algorithm: the
// curve-matched ECDSA key for ES256/ES384/ES512, RSA-2048 for RS256/PS256,
// Ed25519 for EdDSA.
func GenerateKey(alg string) (crypto.Signer, error) {
	switch alg {
	case "ES256":
		return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "ES384":
		return ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "ES512":
		return ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	case "RS256", "PS256":
		return rsa.GenerateKey(rand.Reader, 2048)
	case "EdDSA":
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		return priv, err
	default:
		return nil, fmt.Errorf("unsupported algorithm %q (want one of %v)", alg, AllowedAlgorithms)
	}
}

// DefaultAlgorithm infers the natural signing algorithm from the key type,
// matching vendor SDK auto-detection: EC by curve, RSA → RS256 (PS256 must
// be requested explicitly), Ed25519 → EdDSA.
func DefaultAlgorithm(key crypto.Signer) (string, error) {
	assertf(key != nil, "DefaultAlgorithm called with nil key")
	switch pub := key.Public().(type) {
	case *ecdsa.PublicKey:
		switch pub.Curve {
		case elliptic.P256():
			return "ES256", nil
		case elliptic.P384():
			return "ES384", nil
		case elliptic.P521():
			return "ES512", nil
		}
		return "", fmt.Errorf("unsupported ECDSA curve %q", pub.Curve.Params().Name)
	case *rsa.PublicKey:
		return "RS256", nil
	case ed25519.PublicKey:
		return "EdDSA", nil
	default:
		return "", fmt.Errorf("unsupported key type %T", pub)
	}
}

// MintToken signs c into a compact JWT with header {"alg": alg, "typ":
// "JWT"}. Claims are marshaled exactly as given so tests can mint invalid
// tokens (expired, wrong sub, ...); typical callers set Sub:
// SubjectLiteral, Iat: now, Exp: now+ttl. Errors when alg is not allowed or
// does not fit the key.
func MintToken(key crypto.Signer, alg string, c Claims) (string, error) {
	assertf(key != nil, "MintToken called with nil key")
	algObj, ok := signatureAlgorithms[alg]
	if !ok {
		return "", fmt.Errorf("unsupported algorithm %q (want one of %v)", alg, AllowedAlgorithms)
	}
	if !keySupportsAlg(key.Public(), alg) {
		return "", fmt.Errorf("key type %T cannot sign %s", key.Public(), alg)
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	hdrs := jws.NewHeaders()
	if err := hdrs.Set("typ", "JWT"); err != nil {
		return "", err
	}
	signed, err := jws.Sign(payload, jws.WithKey(algObj, key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return string(signed), nil
}

// MarshalPrivateKeyPEM encodes key as one PKCS#8 "PRIVATE KEY" PEM block.
// Never commit the result.
func MarshalPrivateKeyPEM(key crypto.Signer) ([]byte, error) {
	assertf(key != nil, "MarshalPrivateKeyPEM called with nil key")
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// MarshalPublicKeyPEM encodes pub as one PKIX "PUBLIC KEY" PEM block (the
// format ParsePublicKeysPEM and the org registry accept).
func MarshalPublicKeyPEM(pub crypto.PublicKey) ([]byte, error) {
	assertf(pub != nil, "MarshalPublicKeyPEM called with nil key")
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

// ParsePrivateKeyPEM parses the first PEM block as a PKCS#8 (EC, RSA, or
// Ed25519) or SEC1 EC private key — the client-side formats named in
// docs/reference/authentication.md.
func ParsePrivateKeyPEM(data []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		signer, ok := key.(crypto.Signer)
		if !ok {
			return nil, fmt.Errorf("unsupported private key type %T", key)
		}
		switch key.(type) {
		case *ecdsa.PrivateKey, *rsa.PrivateKey, ed25519.PrivateKey:
			return signer, nil
		}
		return nil, fmt.Errorf("unsupported private key type %T (want ECDSA, RSA, or Ed25519)", key)
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key (PKCS#8 or SEC1): %w", err)
	}
	return key, nil
}
