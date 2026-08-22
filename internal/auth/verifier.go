package auth

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"
)

// assertf is a fail-fast invariant check for conditions that indicate a
// programming error (never for request-dependent failures).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("auth assertion failed: "+format, args...))
	}
}

// signatureAlgorithms maps every AllowedAlgorithms name to its jwx object.
// Lookup failure doubles as the alg allow-list check: `none` and HS* are
// simply absent, so tokens using them can never reach jws.Verify.
var signatureAlgorithms = map[string]jwa.SignatureAlgorithm{
	"ES256": jwa.ES256(),
	"RS256": jwa.RS256(),
	"EdDSA": jwa.EdDSA(),
	"PS256": jwa.PS256(),
	"ES384": jwa.ES384(),
	"ES512": jwa.ES512(),
}

func init() {
	// Startup check: the map and the frozen contract's allow-list must agree.
	assertf(len(signatureAlgorithms) == len(AllowedAlgorithms),
		"signatureAlgorithms has %d entries, AllowedAlgorithms has %d", len(signatureAlgorithms), len(AllowedAlgorithms))
	for _, name := range AllowedAlgorithms {
		_, ok := signatureAlgorithms[name]
		assertf(ok, "allowed algorithm %q has no jwa mapping", name)
	}
}

// KeySource resolves an org's registered PEM public key blocks by org name
// (the JWT `iss` claim). meta.Store satisfies this. Implementations return
// an error for unknown orgs.
type KeySource interface {
	PublicKeysForOrg(ctx context.Context, name string) ([]string, error)
}

// OrgKeyVerifier implements Verifier: it resolves the issuing org's public
// keys through a KeySource and verifies the token signature against them,
// restricted to AllowedAlgorithms.
type OrgKeyVerifier struct {
	keys KeySource
}

var _ Verifier = (*OrgKeyVerifier)(nil)

// NewVerifier returns an OrgKeyVerifier backed by keys.
func NewVerifier(keys KeySource) *OrgKeyVerifier {
	assertf(keys != nil, "NewVerifier called with nil KeySource")
	return &OrgKeyVerifier{keys: keys}
}

// unionKeySource overlays in-memory PEM blocks onto one org's registered
// keys (issue #45): the serve process's ephemeral loopback public key must
// verify that process's own sync-push tokens without ever being persisted —
// a shared database slot would make replica boots delete each other's keys.
type unionKeySource struct {
	inner KeySource
	org   string
	pems  []string
}

// UnionKeySource returns a KeySource that appends pems to inner's result
// for exactly org; every other org resolves through inner unchanged.
func UnionKeySource(inner KeySource, org string, pems ...string) KeySource {
	assertf(inner != nil, "UnionKeySource: nil inner KeySource")
	assertf(org != "", "UnionKeySource: empty org")
	assertf(len(pems) > 0, "UnionKeySource: no PEM blocks to union")
	for i, p := range pems {
		_, err := ParsePublicKeysPEM([]byte(p))
		assertf(err == nil, "UnionKeySource: PEM block %d does not parse: %v", i, err)
	}
	return unionKeySource{inner: inner, org: org, pems: pems}
}

func (s unionKeySource) PublicKeysForOrg(ctx context.Context, name string) ([]string, error) {
	keys, err := s.inner.PublicKeysForOrg(ctx, name)
	if err != nil {
		return nil, err
	}
	if name != s.org {
		return keys, nil
	}
	// Copy before appending: inner may return a slice it retains.
	out := make([]string, 0, len(keys)+len(s.pems))
	out = append(out, keys...)
	return append(out, s.pems...), nil
}

// Verify authenticates a compact JWT per the Verifier contract: allowed
// algorithm, valid signature for one of the issuer's registered keys,
// `exp` in the future, `sub` == SubjectLiteral. Any failure maps to 401.
func (v *OrgKeyVerifier) Verify(ctx context.Context, token string) (*Claims, error) {
	// Bearer tokens are compact JWS only; rejecting other serializations up
	// front keeps the single-signature/protected-header assumptions below.
	if strings.Count(token, ".") != 2 {
		return nil, errors.New("jwt must be a compact JWS (header.payload.signature)")
	}
	msg, err := jws.Parse([]byte(token))
	if err != nil {
		return nil, fmt.Errorf("malformed jwt: %w", err)
	}
	// jws.Parse auto-detects JSON serializations, and a JSON-serialized JWS
	// can contain exactly two '.' characters (e.g. in an unprotected header),
	// slipping past the compact-form pre-check. Signature count is therefore
	// attacker-controlled input, not an invariant: reject, never assert.
	sigs := msg.Signatures()
	if len(sigs) != 1 {
		return nil, fmt.Errorf("jwt must have exactly one signature, got %d", len(sigs))
	}
	alg, ok := sigs[0].ProtectedHeaders().Algorithm()
	if !ok {
		return nil, errors.New("jwt missing alg header")
	}
	algObj, allowed := signatureAlgorithms[alg.String()]
	if !allowed {
		return nil, fmt.Errorf("jwt algorithm %q not allowed", alg.String())
	}

	// Peek at the UNVERIFIED iss claim solely to select the org's keys;
	// nothing else is trusted until the signature verifies.
	var unverified struct {
		Iss string `json:"iss"`
	}
	if err := json.Unmarshal(msg.Payload(), &unverified); err != nil {
		return nil, fmt.Errorf("jwt claims: %w", err)
	}
	if unverified.Iss == "" {
		return nil, errors.New("jwt missing iss claim")
	}

	pems, err := v.keys.PublicKeysForOrg(ctx, unverified.Iss)
	if err != nil {
		return nil, fmt.Errorf("issuer %q: %w", unverified.Iss, err)
	}
	var pubs []crypto.PublicKey
	for i, p := range pems {
		keys, err := ParsePublicKeysPEM([]byte(p))
		if err != nil {
			return nil, fmt.Errorf("org %q registered key %d: %w", unverified.Iss, i+1, err)
		}
		pubs = append(pubs, keys...)
	}
	if len(pubs) == 0 {
		return nil, fmt.Errorf("issuer %q has no registered public keys", unverified.Iss)
	}

	var payload []byte
	verified := false
	for _, pub := range pubs {
		if !keySupportsAlg(pub, alg.String()) {
			continue
		}
		p, err := jws.Verify([]byte(token), jws.WithKey(algObj, pub))
		if err == nil {
			payload = p
			verified = true
			break
		}
	}
	if !verified {
		return nil, errors.New("jwt signature verification failed")
	}
	// jws.Verify re-parsed the same token; the verified payload must be the
	// exact bytes we peeked at above.
	assertf(bytes.Equal(payload, msg.Payload()), "verified payload differs from parsed payload")

	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return nil, fmt.Errorf("jwt claims: %w", err)
	}
	if err := validateClaims(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// validateClaims enforces the claim requirements from the Verifier contract:
// `exp` in the future (a missing exp unmarshals to 0 and fails too) and the
// fixed `sub` literal. `iat` is carried but intentionally not validated —
// docs/02 imposes no requirement on it and clock skew would break SDKs.
func validateClaims(c *Claims) error {
	if c.Exp <= time.Now().Unix() {
		return errors.New("jwt expired")
	}
	if c.Sub != SubjectLiteral {
		return fmt.Errorf("jwt sub %q != %q", c.Sub, SubjectLiteral)
	}
	return nil
}

// keySupportsAlg reports whether pub can verify signatures produced with the
// named algorithm: ECDSA is pinned to the curve-matched ES* variant, RSA
// verifies both RS256 and PS256, Ed25519 verifies EdDSA. Cross-family
// combinations (e.g. an ES256 token against an RSA-only org) never reach
// jws.Verify.
func keySupportsAlg(pub crypto.PublicKey, alg string) bool {
	switch k := pub.(type) {
	case *ecdsa.PublicKey:
		switch k.Curve {
		case elliptic.P256():
			return alg == "ES256"
		case elliptic.P384():
			return alg == "ES384"
		case elliptic.P521():
			return alg == "ES512"
		}
		return false
	case *rsa.PublicKey:
		return alg == "RS256" || alg == "PS256"
	case ed25519.PublicKey:
		return alg == "EdDSA"
	default:
		// ParsePublicKeysPEM admits only the three families above.
		panic(fmt.Sprintf("auth assertion failed: unsupported public key type %T", pub))
	}
}
