// Package githttp is the git smart HTTP surface (promoted from the p0/p1
// de-risk spike): git smart HTTP nodes with
// Postgres as ref authority, S3/MinIO as immutable pack storage, and local
// disk as a rebuildable cache. Spike-local decisions are flagged in comments.
package githttp

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"
)

// SubjectLiteral is the fixed `sub` claim used by all vendor SDKs (docs/02).
const SubjectLiteral = "@pierre/storage"

const (
	ScopeGitRead  = "git:read"
	ScopeGitWrite = "git:write"
)

// assertf is a fail-fast invariant check (spike style: crash and burn).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("spike assertion failed: "+format, args...))
	}
}

// Claims is the subset of the JWT claim set the spike cares about (docs/02).
type Claims struct {
	Iss    string      `json:"iss"`
	Sub    string      `json:"sub"`
	Repo   string      `json:"repo"`
	Scopes []string    `json:"scopes"`
	Iat    int64       `json:"iat"`
	Exp    int64       `json:"exp"`
	Refs   []RefPolicy `json:"refs,omitempty"`
}

// RefPolicy is one `[pattern, ops]` tuple from the `refs` claim. Ordered,
// first-match-wins.
type RefPolicy struct {
	Pattern string
	Ops     []string
}

func (p *RefPolicy) UnmarshalJSON(data []byte) error {
	var tuple []json.RawMessage
	if err := json.Unmarshal(data, &tuple); err != nil {
		return fmt.Errorf("ref policy must be a [pattern, ops] tuple: %w", err)
	}
	if len(tuple) != 2 {
		return fmt.Errorf("ref policy tuple must have exactly 2 elements, got %d", len(tuple))
	}
	if err := json.Unmarshal(tuple[0], &p.Pattern); err != nil {
		return fmt.Errorf("ref policy pattern: %w", err)
	}
	if err := json.Unmarshal(tuple[1], &p.Ops); err != nil {
		return fmt.Errorf("ref policy ops: %w", err)
	}
	return nil
}

func (p RefPolicy) MarshalJSON() ([]byte, error) {
	ops := p.Ops
	if ops == nil {
		ops = []string{}
	}
	return json.Marshal([]any{p.Pattern, ops})
}

// NormalizeRefName applies the shorthand rule from docs/02: anything not
// starting with `refs/` (and not exactly `*` or `HEAD`) becomes
// `refs/heads/<value>`.
func NormalizeRefName(s string) string {
	if s == "*" || s == "HEAD" || strings.HasPrefix(s, "refs/") {
		return s
	}
	return "refs/heads/" + s
}

// NormalizePolicies normalizes every pattern in place and returns the slice.
func NormalizePolicies(policies []RefPolicy) []RefPolicy {
	for i := range policies {
		policies[i].Pattern = NormalizeRefName(policies[i].Pattern)
	}
	return policies
}

// MatchPolicy scans normalized policies in order; first pattern match wins.
// Glob `*` matches across `/` (spike simplification, noted in the plan).
func MatchPolicy(policies []RefPolicy, ref string) (RefPolicy, bool) {
	ref = NormalizeRefName(ref)
	for _, p := range policies {
		if globMatch(p.Pattern, ref) {
			return p, true
		}
	}
	return RefPolicy{}, false
}

func globMatch(pattern, s string) bool {
	// Translate glob to an anchored regexp; `*` matches any run including `/`.
	var b strings.Builder
	b.WriteString(`\A`)
	for i, part := range strings.Split(pattern, "*") {
		if i > 0 {
			b.WriteString(`.*`)
		}
		b.WriteString(regexp.QuoteMeta(part))
	}
	b.WriteString(`\z`)
	re, err := regexp.Compile(b.String())
	assertf(err == nil, "glob pattern %q compiled to invalid regexp: %v", pattern, err)
	return re.MatchString(s)
}

// ParseRefPolicies parses the JSON tuple encoding and normalizes patterns.
func ParseRefPolicies(raw string) ([]RefPolicy, error) {
	var policies []RefPolicy
	if err := json.Unmarshal([]byte(raw), &policies); err != nil {
		return nil, fmt.Errorf("parse refs policy %q: %w", raw, err)
	}
	return NormalizePolicies(policies), nil
}

// VerifyToken verifies an ES256 compact JWT against pub and validates the
// spike-relevant claims (exp in the future, fixed sub literal).
func VerifyToken(pub *ecdsa.PublicKey, token string) (*Claims, error) {
	assertf(pub != nil, "VerifyToken called with nil public key")
	payload, err := jws.Verify([]byte(token), jws.WithKey(jwa.ES256(), pub))
	if err != nil {
		return nil, fmt.Errorf("jwt signature: %w", err)
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return nil, fmt.Errorf("jwt claims: %w", err)
	}
	if c.Exp <= time.Now().Unix() {
		return nil, errors.New("jwt expired")
	}
	if c.Sub != SubjectLiteral {
		return nil, fmt.Errorf("jwt sub %q != %q", c.Sub, SubjectLiteral)
	}
	NormalizePolicies(c.Refs)
	return &c, nil
}

// HasScope reports whether claims grant the given scope.
func (c *Claims) HasScope(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// RefsPolicyJSON returns the normalized refs policy in tuple encoding for
// passing to hooks via env, or "" when the claim is absent.
func (c *Claims) RefsPolicyJSON() string {
	if len(c.Refs) == 0 {
		return ""
	}
	b, err := json.Marshal(c.Refs)
	assertf(err == nil, "marshal refs policy: %v", err)
	return string(b)
}

// MintToken signs an ES256 JWT with the given claims. Used by the `mint`
// dogfooding subcommand and by tests (keys are always generated at runtime).
func MintToken(priv *ecdsa.PrivateKey, iss, repo string, scopes []string, refsJSON string, ttl time.Duration) (string, error) {
	assertf(priv != nil, "MintToken called with nil private key")
	now := time.Now()
	claims := map[string]any{
		"iss":    iss,
		"sub":    SubjectLiteral,
		"repo":   repo,
		"scopes": scopes,
		"iat":    now.Unix(),
		"exp":    now.Add(ttl).Unix(),
	}
	if refsJSON != "" {
		// Parse (validates + normalizes) and re-marshal through RefPolicy's
		// tuple encoding instead of embedding the caller's raw JSON.
		policies, err := ParseRefPolicies(refsJSON)
		if err != nil {
			return "", err
		}
		claims["refs"] = policies
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	hdrs := jws.NewHeaders()
	if err := hdrs.Set("typ", "JWT"); err != nil {
		return "", err
	}
	signed, err := jws.Sign(payload, jws.WithKey(jwa.ES256(), priv, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		return "", err
	}
	return string(signed), nil
}

// GenerateKeyPair creates a fresh ES256 (P-256) keypair.
func GenerateKeyPair() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

// WriteKeyPairPEM writes `<out>.pem` (PKCS#8 private) and `<out>.pub.pem`
// (PKIX public). Never commit the private key.
func WriteKeyPairPEM(priv *ecdsa.PrivateKey, outPrefix string) (privPath, pubPath string, err error) {
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", "", err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return "", "", err
	}
	privPath = outPrefix + ".pem"
	pubPath = outPrefix + ".pub.pem"
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	if err := os.WriteFile(privPath, privPEM, 0o600); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(pubPath, pubPEM, 0o644); err != nil {
		return "", "", err
	}
	return privPath, pubPath, nil
}

// LoadPublicKeyPEM loads a PKIX-encoded ECDSA public key.
func LoadPublicKeyPEM(path string) (*ecdsa.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("%s: no PEM block", path)
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	ec, ok := key.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an ECDSA public key (%T)", path, key)
	}
	return ec, nil
}

// LoadPrivateKeyPEM loads a PKCS#8 or SEC1 ECDSA private key.
func LoadPrivateKeyPEM(path string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("%s: no PEM block", path)
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		ec, ok := key.(*ecdsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("%s: not an ECDSA private key (%T)", path, key)
		}
		return ec, nil
	}
	ec, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return ec, nil
}
