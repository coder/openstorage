package auth

import (
	"bytes"
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
	"log/slog"
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

// errNonCanonicalSegment marks a token rejected because a segment is not
// canonical unpadded base64url (issue #312).
var errNonCanonicalSegment = errors.New("jwt segment is not canonical unpadded base64url")

// strictRawURL rejects '=' padding, the standard alphabet, and non-zero
// padding bits. Like every encoding/base64 decoder it still skips CR/LF,
// which is why checkCanonicalSegments also compares the re-encoding.
var strictRawURL = base64.RawURLEncoding.Strict()

// checkCanonicalSegments requires every segment of a compact JWS to be
// non-empty, canonical unpadded base64url (RFC 7515 §2; RFC 4648 §3.5 lets
// decoders reject non-zero padding bits). jwx's default decoder is lenient
// (it guesses std/url, padded/raw, is not Strict), and jws.Verify rebuilds
// the signing input by re-encoding the decoded header and payload, so
// without this check one signed token had many accepted spellings (issue
// #312). This does not make token text unique: an ECDSA signature (r, s)
// has a valid twin (r, n-s) with different bytes. Never key caches, replay
// or deny lists on the raw token string.
func checkCanonicalSegments(token string) error {
	segs := strings.Split(token, ".")
	assertf(len(segs) == 3, "checkCanonicalSegments: %d segments; caller must check the compact shape", len(segs))
	for i, seg := range segs {
		name := [...]string{"header", "payload", "signature"}[i]
		if seg == "" {
			return fmt.Errorf("jwt %s: %w", name, errNonCanonicalSegment)
		}
		raw, err := strictRawURL.DecodeString(seg)
		if err != nil || base64.RawURLEncoding.EncodeToString(raw) != seg {
			return fmt.Errorf("jwt %s: %w", name, errNonCanonicalSegment)
		}
	}
	return nil
}

// errCritNotSupported marks a token refused because its protected header has
// a `crit` member (RFC 7515 §4.1.11). A client reads this text: the git and
// LFS transports answer with the text of the verifier's error.
var errCritNotSupported = errors.New("jwt crit header is not supported")

// errHeaderNotOneObject is the cause of a MalformedError for a protected
// header that is not exactly one JSON object.
var errHeaderNotOneObject = errors.New("protected header is not one JSON object")

// checkHeader requires the decoded protected header of a compact token to be
// exactly one JSON object, with at most whitespace around it, and refuses a
// `crit` member (RFC 7515 §4.1.11). It does so on the text of the whole
// header. jwx decodes only the first JSON value of the header, ignores any
// bytes after it, and drops a null `crit` member and all but the last of two
// members with one name, so a check on its parsed header misses a `crit` that
// this check sees. A client reads the answer, so both texts are fixed. The
// check is not jws.WithCritValidation(true): the key loop in Verify drops
// that error, and a token that a registered key signed would read "signature
// verification failed". The caller has checked that the segments are
// canonical base64url.
func checkHeader(token string) error {
	seg, _, _ := strings.Cut(token, ".")
	raw, err := strictRawURL.DecodeString(seg)
	if err != nil {
		return &MalformedError{What: "jwt", Err: err}
	}
	if !json.Valid(raw) {
		return &MalformedError{What: "jwt", Err: errHeaderNotOneObject}
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil || members == nil {
		return &MalformedError{What: "jwt", Err: errHeaderNotOneObject}
	}
	if _, has := members[jws.CriticalKey]; has {
		return errCritNotSupported
	}
	return nil
}

// KeySource resolves an org's registered PEM public key blocks by org name
// (the JWT `iss` claim). meta.Store satisfies this. Implementations return
// an error for unknown orgs.
type KeySource interface {
	PublicKeysForOrg(ctx context.Context, name string) ([]string, error)
}

// KeySourceError is a Verify failure from the KeySource: the org is not
// registered, or the key store failed. The caller tells them apart with
// errors.Is on the store's not-found error. Error names only the issuer: the
// store's error can name its database host, user and name or its S3
// endpoint and bucket, and the git and LFS transports can show Error to a
// client. The store's error stays behind Unwrap, for a log.
type KeySourceError struct {
	Org string
	Err error
}

func (e *KeySourceError) Error() string { return fmt.Sprintf("issuer %q: keys unavailable", e.Org) }

// Unwrap exposes the KeySource error.
func (e *KeySourceError) Unwrap() error { return e.Err }

// StoredKeyError is a Verify failure because ParsePublicKeysPEM refuses a
// public key that the org has registered. The token is not at fault. Error
// names only the issuer, because the parser's text is for a log, and the git
// and LFS transports can show Error to a client. Unwrap has the parser's
// error.
type StoredKeyError struct {
	Org string
	Err error
}

func (e *StoredKeyError) Error() string {
	return fmt.Sprintf("issuer %q: a registered key cannot be used", e.Org)
}

// Unwrap exposes the parser's error.
func (e *StoredKeyError) Unwrap() error { return e.Err }

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
// algorithm, no `crit` header, valid signature for one of the issuer's
// registered keys, `exp` in the future, a non-empty `sub`. Any failure
// returns an error. A token without a `repo` claim verifies with Repo ==
// RepoClaimOrg.
func (v *OrgKeyVerifier) Verify(ctx context.Context, token string) (*Claims, error) {
	// Bearer tokens are compact JWS only; rejecting other serializations up
	// front keeps the single-signature/protected-header assumptions below.
	if strings.Count(token, ".") != 2 {
		return nil, errors.New("jwt must be a compact JWS (header.payload.signature)")
	}
	// Check the original text before any parsing; never normalize and retry.
	if err := checkCanonicalSegments(token); err != nil {
		return nil, err
	}
	// Before jws.Parse: jwx reads only the first JSON value of the header,
	// and rejects some `crit` values with its own text.
	if err := checkHeader(token); err != nil {
		return nil, err
	}
	msg, err := jws.Parse([]byte(token))
	if err != nil {
		// The parser's text can hold the text of a JSON decoder (see
		// MalformedError). It is not logged: the client chose these bytes.
		return nil, &MalformedError{What: "jwt", Err: err}
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
	// nothing else is trusted until the signature verifies. The signature
	// cannot be checked first: no key is known until iss names the org, and
	// a token that another registered org signed must keep its own answer,
	// which differs from the answer to a bad signature. The error carries no
	// log line: the client chose these bytes.
	var unverified struct {
		Iss string `json:"iss"`
	}
	if err := json.Unmarshal(msg.Payload(), &unverified); err != nil {
		return nil, &MalformedError{What: "jwt claims", Err: err}
	}
	if unverified.Iss == "" {
		return nil, errors.New("jwt missing iss claim")
	}
	// A NUL fails the PostgreSQL lookup: refuse it here, so a
	// KeySourceError always means the key store failed, not that the token
	// is malformed. No configured org has one (an environment variable
	// cannot hold a NUL). json.Unmarshal already made iss valid UTF-8.
	if strings.ContainsRune(unverified.Iss, 0) {
		return nil, errors.New("jwt iss claim has a NUL character")
	}

	pems, err := v.keys.PublicKeysForOrg(ctx, unverified.Iss)
	if err != nil {
		return nil, &KeySourceError{Org: unverified.Iss, Err: err}
	}
	var pubs []crypto.PublicKey
	for i, p := range pems {
		keys, err := ParsePublicKeysPEM([]byte(p))
		if err != nil {
			return nil, &StoredKeyError{Org: unverified.Iss, Err: fmt.Errorf("registered key %d: %w", i+1, err)}
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
		// A registered key signed these claims, so the operator who minted
		// the token can fix them and needs the decoder's text. The client
		// does not get it (see MalformedError).
		slog.Warn("token claims signed by a registered key do not decode", "iss", unverified.Iss, "err", err)
		return nil, &MalformedError{What: "jwt claims", Err: err}
	}
	// A token without a `repo` claim is org-wide ("omit for org-wide
	// tokens", docs/upstream/getting-started/authentication.md), so it gets
	// RepoClaimOrg and every caller's org/repo gate treats it exactly like
	// `repo: "org"`. A present `""` or null stays "" and is refused by both
	// gates. The second decode uses the same struct-tag key matching as the
	// first, so "present" means the key that filled c.Repo.
	var repo struct {
		Repo json.RawMessage `json:"repo"`
	}
	err = json.Unmarshal(payload, &repo)
	assertf(err == nil, "claims decoded but the repo presence decode failed: %v", err)
	if repo.Repo == nil {
		c.Repo = RepoClaimOrg
	}
	if err := validateClaims(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// validateClaims enforces the claim requirements from the Verifier contract:
// `exp` in the future (a missing exp unmarshals to 0 and fails too) and a
// non-empty `sub`. `sub` is caller-chosen free text ("Agent identity (for
// logging)", docs/upstream/getting-started/authentication.md), so any
// non-empty value passes and nothing may treat it as an identity. `iat` is
// carried but intentionally not validated — docs/reference/authentication.md
// imposes no requirement on it and clock skew would break SDKs.
func validateClaims(c *Claims) error {
	if c.Exp <= time.Now().Unix() {
		return errors.New("jwt expired")
	}
	if c.Sub == "" {
		return errors.New("jwt missing sub claim")
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
