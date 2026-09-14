package auth

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeKeySource is an in-memory KeySource: org name → PEM public key blocks.
// All keys are generated at runtime; nothing is committed.
type fakeKeySource map[string][]string

func (f fakeKeySource) PublicKeysForOrg(_ context.Context, name string) ([]string, error) {
	pems, ok := f[name]
	if !ok {
		return nil, errors.New("org not found")
	}
	return pems, nil
}

// mustGenerateKey generates a private key for alg.
func mustGenerateKey(t *testing.T, alg string) crypto.Signer {
	t.Helper()
	key, err := GenerateKey(alg)
	if err != nil {
		t.Fatalf("GenerateKey(%s): %v", alg, err)
	}
	return key
}

// mustPublicPEM encodes key's public half as a PEM string.
func mustPublicPEM(t *testing.T, key crypto.Signer) string {
	t.Helper()
	pemBytes, err := MarshalPublicKeyPEM(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	return string(pemBytes)
}

// mustMint signs c with key/alg.
func mustMint(t *testing.T, key crypto.Signer, alg string, c Claims) string {
	t.Helper()
	token, err := MintToken(key, alg, c)
	if err != nil {
		t.Fatalf("MintToken(%s): %v", alg, err)
	}
	return token
}

// validClaims returns a fresh claim set that passes validation for iss.
func validClaims(iss string) Claims {
	now := time.Now()
	return Claims{
		Iss:    iss,
		Sub:    SubjectLiteral,
		Repo:   "repo-1",
		Scopes: []string{ScopeGitRead},
		Iat:    now.Unix(),
		Exp:    now.Add(time.Hour).Unix(),
	}
}

// TestVerify_AlgorithmMatrix covers each allowed algorithm: a token signed
// with the registered key verifies; a token signed with an unregistered key
// of the same algorithm does not.
func TestVerify_AlgorithmMatrix(t *testing.T) {
	for _, alg := range AllowedAlgorithms {
		t.Run(alg, func(t *testing.T) {
			key := mustGenerateKey(t, alg)
			strangerKey := mustGenerateKey(t, alg)
			v := NewVerifier(fakeKeySource{"acme": {mustPublicPEM(t, key)}})

			token := mustMint(t, key, alg, validClaims("acme"))
			got, err := v.Verify(context.Background(), token)
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if got.Iss != "acme" || got.Sub != SubjectLiteral || got.Repo != "repo-1" {
				t.Fatalf("claims = %+v", got)
			}
			if len(got.Scopes) != 1 || got.Scopes[0] != ScopeGitRead {
				t.Fatalf("scopes = %v", got.Scopes)
			}

			stranger := mustMint(t, strangerKey, alg, validClaims("acme"))
			if _, err := v.Verify(context.Background(), stranger); err == nil {
				t.Fatal("token signed by unregistered key verified")
			}
		})
	}
}

// TestVerify_MultipleRegisteredKeys registers keys of several families for
// one org (including two PEM blocks concatenated in a single entry) and
// checks tokens from each key verify — plus PS256 with the RSA key that was
// registered for RS256.
func TestVerify_MultipleRegisteredKeys(t *testing.T) {
	ecKey := mustGenerateKey(t, "ES256")
	rsaKey := mustGenerateKey(t, "RS256")
	edKey := mustGenerateKey(t, "EdDSA")
	v := NewVerifier(fakeKeySource{"acme": {
		// One entry with two concatenated blocks + one single-block entry.
		mustPublicPEM(t, ecKey) + mustPublicPEM(t, rsaKey),
		mustPublicPEM(t, edKey),
	}})

	for _, tc := range []struct {
		alg string
		key crypto.Signer
	}{
		{"ES256", ecKey},
		{"RS256", rsaKey},
		{"PS256", rsaKey}, // same RSA key, PSS padding
		{"EdDSA", edKey},
	} {
		token := mustMint(t, tc.key, tc.alg, validClaims("acme"))
		if _, err := v.Verify(context.Background(), token); err != nil {
			t.Errorf("%s: Verify: %v", tc.alg, err)
		}
	}
}

// b64seg encodes one JWS segment.
func b64seg(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func TestVerify_Negatives(t *testing.T) {
	key := mustGenerateKey(t, "ES256")
	otherOrgKey := mustGenerateKey(t, "ES256")
	v := NewVerifier(fakeKeySource{
		"acme":  {mustPublicPEM(t, key)},
		"other": {mustPublicPEM(t, otherOrgKey)},
	})

	claimsJSON, err := json.Marshal(validClaims("acme"))
	if err != nil {
		t.Fatal(err)
	}
	// alg "none": unsigned token that still names a registered org.
	noneToken := b64seg([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + b64seg(claimsJSON) + "."
	// HS256: correctly HMAC-signed token — rejected by the alg allow-list,
	// never by signature comparison.
	hsInput := b64seg([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + b64seg(claimsJSON)
	mac := hmac.New(sha256.New, []byte("shared-secret"))
	mac.Write([]byte(hsInput))
	hsToken := hsInput + "." + b64seg(mac.Sum(nil))

	expired := validClaims("acme")
	expired.Exp = time.Now().Add(-time.Minute).Unix()
	noExp := validClaims("acme")
	noExp.Exp = 0
	wrongSub := validClaims("acme")
	wrongSub.Sub = "@evil/storage"
	unknownIss := validClaims("ghost")
	noIss := validClaims("")

	// Tampered payload: flip claims inside a validly signed token.
	valid := mustMint(t, key, "ES256", validClaims("acme"))
	parts := strings.Split(valid, ".")
	escalated := validClaims("acme")
	escalated.Scopes = []string{ScopeRepoWrite}
	escalatedJSON, err := json.Marshal(escalated)
	if err != nil {
		t.Fatal(err)
	}
	tampered := parts[0] + "." + b64seg(escalatedJSON) + "." + parts[2]

	// Alg/key family mismatch: valid ES384 token, but acme registered only a
	// P-256 key, so no registered key is compatible.
	es384Key := mustGenerateKey(t, "ES384")
	es384Token := mustMint(t, es384Key, "ES384", validClaims("acme"))

	for name, token := range map[string]string{
		"empty":                     "",
		"garbage":                   "not-a-jwt",
		"two segments":              "aaaa.bbbb",
		"four segments":             "a.b.c.d",
		"undecodable segments":      "!!.!!.!!",
		"alg none":                  noneToken,
		"alg HS256":                 hsToken,
		"expired":                   mustMint(t, key, "ES256", expired),
		"missing exp":               mustMint(t, key, "ES256", noExp),
		"wrong sub":                 mustMint(t, key, "ES256", wrongSub),
		"unknown issuer":            mustMint(t, key, "ES256", unknownIss),
		"missing iss":               mustMint(t, key, "ES256", noIss),
		"other org's key":           mustMint(t, otherOrgKey, "ES256", validClaims("acme")),
		"tampered payload":          tampered,
		"no compatible key for alg": es384Token,
	} {
		if _, err := v.Verify(context.Background(), token); err == nil {
			t.Errorf("%s: Verify accepted the token", name)
		}
	}
}

// TestVerify_NonCompactSerializations feeds JWS JSON (flattened and general
// multi-signature) serializations crafted to contain exactly two '.'
// characters, so they slip past the compact-form pre-check and exercise
// jws.Parse's JSON auto-detection. Verify must return an error for every
// variant — never panic (verifier.go once assertf'd on the signature count,
// turning an attacker-controlled token into a crash).
func TestVerify_NonCompactSerializations(t *testing.T) {
	key := mustGenerateKey(t, "ES256")
	v := NewVerifier(fakeKeySource{"acme": {mustPublicPEM(t, key)}})

	claimsJSON, err := json.Marshal(validClaims("acme"))
	if err != nil {
		t.Fatal(err)
	}
	payload := b64seg(claimsJSON)
	protected := b64seg([]byte(`{"alg":"ES256"}`))
	sig := b64seg([]byte("not-a-real-signature"))

	// The unprotected header value ".." supplies the exactly-two dots the
	// compact-form pre-check looks for; base64url segments contain none.
	multiSig := `{"payload":"` + payload + `","signatures":[` +
		`{"protected":"` + protected + `","header":{"kid":".."},"signature":"` + sig + `"},` +
		`{"protected":"` + protected + `","signature":"` + sig + `"}]}`
	flattened := `{"payload":"` + payload + `","protected":"` + protected +
		`","header":{"kid":".."},"signature":"` + sig + `"}`

	for name, token := range map[string]string{
		"general multi-signature": multiSig,
		"flattened":               flattened,
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Verify panicked on attacker-controlled token: %v", r)
				}
			}()
			if _, err := v.Verify(context.Background(), token); err == nil {
				t.Error("Verify accepted a non-compact JWS")
			}
		})
	}
}

// TestVerify_RefsAndOpsPassthrough checks the git-path claims survive
// verification byte-for-byte (parsing them is the git path's job).
func TestVerify_RefsAndOpsPassthrough(t *testing.T) {
	key := mustGenerateKey(t, "ES256")
	v := NewVerifier(fakeKeySource{"acme": {mustPublicPEM(t, key)}})

	c := validClaims("acme")
	c.Refs = json.RawMessage(`[["refs/heads/main",["no-force-push"]],["feature/*",[]]]`)
	c.Ops = []string{"no-force-push"}

	got, err := v.Verify(context.Background(), mustMint(t, key, "ES256", c))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if string(got.Refs) != string(c.Refs) {
		t.Errorf("refs = %s, want %s", got.Refs, c.Refs)
	}
	if len(got.Ops) != 1 || got.Ops[0] != "no-force-push" {
		t.Errorf("ops = %v", got.Ops)
	}
}

func TestNewVerifier_NilKeySourcePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewVerifier(nil) did not panic")
		}
	}()
	NewVerifier(nil)
}

// --- UnionKeySource (issue #45) ---------------------------------------------

// TestUnionKeySource_OverlaysOnlyTheTargetOrg pins the loopback-key seam:
// the unioned PEM verifies tokens for exactly the configured org, other
// orgs resolve through the inner source unchanged, and inner errors
// propagate.
func TestUnionKeySource_OverlaysOnlyTheTargetOrg(t *testing.T) {
	fileKey := mustGenerateKey(t, "ES256")
	loopKey := mustGenerateKey(t, "ES256")
	otherKey := mustGenerateKey(t, "ES256")
	inner := fakeKeySource{
		"acme":  {mustPublicPEM(t, fileKey)},
		"other": {mustPublicPEM(t, otherKey)},
	}
	src := UnionKeySource(inner, "acme", mustPublicPEM(t, loopKey))

	acme, err := src.PublicKeysForOrg(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if len(acme) != 2 || acme[0] != mustPublicPEM(t, fileKey) || acme[1] != mustPublicPEM(t, loopKey) {
		t.Fatalf("acme keys = %d blocks, want inner file key + loopback key", len(acme))
	}
	other, err := src.PublicKeysForOrg(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 1 || other[0] != mustPublicPEM(t, otherKey) {
		t.Fatalf("other org keys = %d blocks, want inner key only", len(other))
	}
	if _, err := src.PublicKeysForOrg(context.Background(), "ghost"); err == nil {
		t.Fatal("unknown org must propagate the inner error")
	}

	// End-to-end through the verifier: a loopback-signed token verifies
	// for acme, and the same signature never verifies for the other org
	// (its claims select the other org's keys).
	v := NewVerifier(src)
	if _, err := v.Verify(context.Background(), mustMint(t, loopKey, "ES256", validClaims("acme"))); err != nil {
		t.Fatalf("loopback token for acme: %v", err)
	}
	if _, err := v.Verify(context.Background(), mustMint(t, loopKey, "ES256", validClaims("other"))); err == nil {
		t.Fatal("loopback key must not verify tokens for other orgs")
	}
}

// TestUnionKeySource_RejectsBadInputs pins the constructor's crash-early
// contract: an unparsable PEM block is a programming error, not a latent
// verification gap.
func TestUnionKeySource_RejectsBadInputs(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("UnionKeySource with a bad PEM block must panic")
		}
	}()
	UnionKeySource(fakeKeySource{}, "acme", "not a pem block")
}

func TestVerify_RejectsWeakRSAKeySource(t *testing.T) {
	weak, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	strong, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		t.Fatal(err)
	}
	weakPEM, strongPEM := mustPublicPEM(t, weak), mustPublicPEM(t, strong)
	for _, alg := range []string{"RS256", "PS256"} {
		t.Run(alg, func(t *testing.T) {
			weakToken := mustMint(t, weak, alg, validClaims("acme"))
			strongToken := mustMint(t, strong, alg, validClaims("acme"))
			source := fakeKeySource{"acme": {strongPEM}}
			// Exercise the production overlay as well as mutable registered
			// rows; all keys must be revalidated on every request.
			v := NewVerifier(UnionKeySource(source, "acme", mustPublicPEM(t, mustGenerateKey(t, "ES256"))))
			if _, err := v.Verify(t.Context(), strongToken); err != nil {
				t.Fatal(err)
			}
			for _, pems := range [][]string{
				{weakPEM}, {strongPEM, weakPEM}, {weakPEM, strongPEM}, {strongPEM + weakPEM},
			} {
				source["acme"] = pems
				for _, token := range []string{weakToken, strongToken} {
					claims, err := v.Verify(t.Context(), token)
					if err == nil || claims != nil || !strings.Contains(err.Error(), "minimum 2048") {
						t.Fatalf("Verify returned claims=%v, err=%v; want RSA minimum error", claims, err)
					}
				}
			}
			source["acme"] = []string{strongPEM}
			if _, err := v.Verify(t.Context(), strongToken); err != nil {
				t.Fatalf("strong key after rotation: %v", err)
			}
			if claims, err := v.Verify(t.Context(), weakToken); err == nil || claims != nil {
				t.Fatal("removed weak key still authenticates")
			}
		})
	}
}
