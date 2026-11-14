package auth

import (
	"context"
	"crypto"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// signRawHeader signs c with an EdDSA key under exactly the protected header
// that header spells, so a test can write a header that jwx would refuse to
// sign or would normalize.
func signRawHeader(t *testing.T, key crypto.Signer, header string, c Claims) string {
	t.Helper()
	payload, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return signRawSegments(t, key, header, payload)
}

// signRawSegments signs the exact header and payload bytes.
func signRawSegments(t *testing.T, key crypto.Signer, header string, payload []byte) string {
	t.Helper()
	input := b64seg([]byte(header)) + "." + b64seg(payload)
	sig, err := key.Sign(nil, []byte(input), crypto.Hash(0))
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + b64seg(sig)
}

// withHeaderTail returns token with tail after the object in its header
// segment. The signature still covers the object alone, and jwx verifies it
// against the object alone.
func withHeaderTail(t *testing.T, token, tail string) string {
	t.Helper()
	seg, rest, _ := strings.Cut(token, ".")
	obj, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		t.Fatal(err)
	}
	return b64seg(append(obj, tail...)) + "." + rest
}

// tailsAfterJSON are bytes that can follow a JSON object: other values, bytes
// that start no value, and a NUL.
var tailsAfterJSON = []string{"x", "}", "]", "{}", "\x00", " x", `"`, "0", `{"crit":null}`}

// TestVerify_RefusesCritHeader: RFC 7515 §4.1.11 says a recipient must refuse
// a token whose `crit` header lists an extension that it does not understand.
// Verify understands none, so a signed token with a `crit` member of any value
// is refused, with the one fixed text that the git and LFS transports show a
// client, and before the key store is asked.
func TestVerify_RefusesCritHeader(t *testing.T) {
	key := mustGenerateKey(t, "EdDSA")
	v := NewVerifier(fakeKeySource{"acme": {mustPublicPEM(t, key)}})
	storeDown := NewVerifier(failingKeySource{err: errors.New("store down")})
	ctx := context.Background()

	const plain = `{"alg":"EdDSA","typ":"JWT"}`
	if _, err := v.Verify(ctx, signRawHeader(t, key, plain, validClaims("acme"))); err != nil {
		t.Fatalf("a token without crit: %v", err)
	}
	for name, header := range map[string]string{
		"unknown extension":             `{"alg":"EdDSA","typ":"JWT","crit":["x-ext"],"x-ext":true}`,
		"extension missing from header": `{"alg":"EdDSA","typ":"JWT","crit":["x-ext"]}`,
		"empty list":                    `{"alg":"EdDSA","typ":"JWT","crit":[]}`,
		"b64, which is not processed":   `{"alg":"EdDSA","typ":"JWT","crit":["b64"],"b64":true}`,
		"a standard header name":        `{"alg":"EdDSA","typ":"JWT","crit":["alg"]}`,
		"null":                          `{"alg":"EdDSA","typ":"JWT","crit":null}`,
		"a later null hides the list":   `{"alg":"EdDSA","typ":"JWT","crit":["x-ext"],"x-ext":true,"crit":null}`,
		"an escaped name":               `{"alg":"EdDSA","typ":"JWT","cr\u0069t":["x-ext"],"x-ext":true}`,
		"a number":                      `{"alg":"EdDSA","typ":"JWT","crit":1}`,
		"an object":                     `{"alg":"EdDSA","typ":"JWT","crit":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			token := signRawHeader(t, key, header, validClaims("acme"))
			for _, vv := range []*OrgKeyVerifier{v, storeDown} {
				if _, err := vv.Verify(ctx, token); err == nil || err.Error() != "jwt crit header is not supported" {
					t.Fatalf("error %v, want the fixed crit text", err)
				}
			}
		})
	}
}

// TestVerify_RefusesBytesAfterTheHeaderObject: jwx decodes only the first JSON
// value of a protected header and signs and verifies over that object alone.
// A token whose header segment holds anything else after the object, such as
// a `crit` that a decode of the whole segment does not see, would verify. The
// header must be exactly one JSON object, and every other header is refused
// with the fixed `malformed jwt`, with or without `crit`.
func TestVerify_RefusesBytesAfterTheHeaderObject(t *testing.T) {
	key := mustGenerateKey(t, "EdDSA")
	v := NewVerifier(fakeKeySource{"acme": {mustPublicPEM(t, key)}})
	ctx := context.Background()

	for name, header := range map[string]string{
		"without crit": `{"alg":"EdDSA","typ":"JWT"}`,
		"with crit":    `{"alg":"EdDSA","typ":"JWT","crit":["x-ext"],"x-ext":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			token := signRawHeader(t, key, header, validClaims("acme"))
			_, err := v.Verify(ctx, token)
			if name == "without crit" && err != nil {
				t.Fatalf("a token without a tail: %v", err)
			}
			for _, tail := range tailsAfterJSON {
				if _, err := v.Verify(ctx, withHeaderTail(t, token, tail)); err == nil || err.Error() != "malformed jwt" {
					t.Errorf("tail %q: error %v, want malformed jwt", tail, err)
				}
			}
		})
	}
	t.Run("a null header", func(t *testing.T) {
		if _, err := v.Verify(ctx, signRawHeader(t, key, "null", validClaims("acme"))); err == nil || err.Error() != "malformed jwt" {
			t.Fatalf("error %v, want malformed jwt", err)
		}
	})
}

// TestVerify_RefusesBytesAfterTheClaims: the claims of a signed token must be
// exactly one JSON object too. jwx does not decode the payload, and Verify
// decodes the whole payload, so no tail passes.
func TestVerify_RefusesBytesAfterTheClaims(t *testing.T) {
	key := mustGenerateKey(t, "EdDSA")
	v := NewVerifier(fakeKeySource{"acme": {mustPublicPEM(t, key)}})
	claims, err := json.Marshal(validClaims("acme"))
	if err != nil {
		t.Fatal(err)
	}
	const header = `{"alg":"EdDSA","typ":"JWT"}`
	if _, err := v.Verify(context.Background(), signRawSegments(t, key, header, claims)); err != nil {
		t.Fatalf("claims without a tail: %v", err)
	}
	for _, tail := range tailsAfterJSON {
		token := signRawSegments(t, key, header, append(claims, tail...))
		if _, err := v.Verify(context.Background(), token); err == nil || err.Error() != "malformed jwt claims" {
			t.Errorf("tail %q: error %v, want malformed jwt claims", tail, err)
		}
	}
}
