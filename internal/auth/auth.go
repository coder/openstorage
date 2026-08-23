// Package auth declares the JWT authentication contract for the HTTP API
// (docs/reference/authentication.md): customer-signed tokens verified against per-org public keys.
//
// FROZEN CONTRACT: parallel phase-1 slices implement Verifier (jwx v3, keys
// resolved via the meta store) and build middleware/handlers on these exact
// signatures. Do not change them without coordinating across slices.
package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
)

// SubjectLiteral is the fixed `sub` claim used by all vendor SDKs (docs/reference/authentication.md).
const SubjectLiteral = "@pierre/storage"

// RepoClaimOrg is the sentinel `repo` claim value for org-level operations
// (GET /repos).
const RepoClaimOrg = "org"

// Scopes (docs/reference/authentication.md scope table). Matching is exact: no scope implies another.
const (
	ScopeGitRead   = "git:read"
	ScopeGitWrite  = "git:write"
	ScopeRepoWrite = "repo:write"
	ScopeOrgRead   = "org:read"
)

// AllowedAlgorithms is the JWS signature algorithm allow-list (docs/reference/authentication.md).
// `none` and HS* are always rejected.
var AllowedAlgorithms = []string{"ES256", "RS256", "EdDSA", "PS256", "ES384", "ES512"}

// Claims is the verified JWT claim set (docs/reference/authentication.md). Refs/Ops are passed
// through raw for the git path, which owns ref-policy parsing.
type Claims struct {
	Iss    string          `json:"iss"`
	Sub    string          `json:"sub"`
	Repo   string          `json:"repo"` // target repo url, or RepoClaimOrg
	Scopes []string        `json:"scopes"`
	Iat    int64           `json:"iat"`            // unix seconds
	Exp    int64           `json:"exp"`            // unix seconds
	Refs   json.RawMessage `json:"refs,omitempty"` // ordered [pattern, ops] tuples
	Ops    []string        `json:"ops,omitempty"`  // deprecated repo-wide policy ops
}

// HasScope reports whether claims grant exactly the given scope.
func (c *Claims) HasScope(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// Verifier authenticates a compact JWT. Implementations must verify the
// signature against the issuing org's registered public keys (per
// AllowedAlgorithms), require `exp` in the future and `sub` ==
// SubjectLiteral, and resolve the org from the `iss` claim.
//
// Any failure — malformed token, bad signature, disallowed algorithm,
// expired, unknown issuer — returns a non-nil error and maps to 401.
// Scope and repo-claim checks are the caller's responsibility (403).
type Verifier interface {
	Verify(ctx context.Context, token string) (*Claims, error)
}

type claimsKey struct{}

// WithClaims returns a context carrying the verified claims (set by the
// auth middleware after Verify succeeds).
func WithClaims(ctx context.Context, c *Claims) context.Context {
	if c == nil {
		panic("auth assertion failed: WithClaims called with nil claims")
	}
	return context.WithValue(ctx, claimsKey{}, c)
}

// ClaimsFromContext returns the verified claims injected by WithClaims,
// or ok=false when the request was not authenticated.
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsKey{}).(*Claims)
	return c, ok
}

// ParsePublicKeysPEM parses one or more PEM "PUBLIC KEY" (PKIX) blocks and
// returns the contained keys. Accepted key types: ECDSA (P-256/384/521),
// RSA, and Ed25519 — the families backing AllowedAlgorithms. Errors on zero
// blocks or any unsupported/undecodable block.
func ParsePublicKeysPEM(data []byte) ([]crypto.PublicKey, error) {
	var keys []crypto.PublicKey
	for len(data) > 0 {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "PUBLIC KEY" {
			return nil, fmt.Errorf("unsupported PEM block %q (want PUBLIC KEY)", block.Type)
		}
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse public key block %d: %w", len(keys)+1, err)
		}
		switch key.(type) {
		case *ecdsa.PublicKey, *rsa.PublicKey, ed25519.PublicKey:
			keys = append(keys, key)
		default:
			return nil, fmt.Errorf("unsupported public key type %T (want ECDSA, RSA, or Ed25519)", key)
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no PEM public key blocks found")
	}
	return keys, nil
}
