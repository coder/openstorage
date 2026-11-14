package auth

import (
	"context"
	"crypto"
	"encoding/json"
	"errors"
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
	input := b64seg([]byte(header)) + "." + b64seg(payload)
	sig, err := key.Sign(nil, []byte(input), crypto.Hash(0))
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + b64seg(sig)
}

// TestVerify_RefusesCritHeader: RFC 7515 §4.1.11 says a recipient must refuse
// a token whose `crit` header lists an extension that it does not understand.
// Verify understands none, so a signed token with any `crit` is refused, and
// with the one fixed text that the git and LFS transports show a client.
func TestVerify_RefusesCritHeader(t *testing.T) {
	key := mustGenerateKey(t, "EdDSA")
	v := NewVerifier(fakeKeySource{"acme": {mustPublicPEM(t, key)}})
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
	} {
		t.Run(name, func(t *testing.T) {
			_, err := v.Verify(ctx, signRawHeader(t, key, header, validClaims("acme")))
			if !errors.Is(err, errCritNotSupported) || err.Error() != "jwt crit header is not supported" {
				t.Fatalf("error %v, want the fixed crit text", err)
			}
		})
	}
}
