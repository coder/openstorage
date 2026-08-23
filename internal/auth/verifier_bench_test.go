// Hot-path benchmarks for bearer-token verification (issue #92): Verify
// runs on every authenticated request across both surfaces. The algorithm
// matrix covers the families upstream SDKs mint with (ES256, Ed25519's
// EdDSA, RS256). BenchmarkParsePublicKeysPEM isolates the per-Verify PEM
// parse: the verifier re-parses the issuer's registered PEM blocks on every
// call (no key cache), so the delta between the two benchmarks bounds what
// a cache could save. Keys are generated in-process at setup; nothing is
// committed and no fixture leaves the benchmark process.
package auth

import (
	"context"
	"testing"
	"time"
)

// benchVerifyAlgorithms is the benched subset of AllowedAlgorithms: one
// algorithm per registered key family (ES384/ES512/PS256 share their
// family's code paths).
var benchVerifyAlgorithms = []string{"ES256", "EdDSA", "RS256"}

func BenchmarkVerify(b *testing.B) {
	ctx := context.Background()
	for _, alg := range benchVerifyAlgorithms {
		b.Run(alg, func(b *testing.B) {
			key, err := GenerateKey(alg)
			if err != nil {
				b.Fatalf("GenerateKey(%s): %v", alg, err)
			}
			pemBytes, err := MarshalPublicKeyPEM(key.Public())
			if err != nil {
				b.Fatalf("MarshalPublicKeyPEM: %v", err)
			}
			v := NewVerifier(fakeKeySource{"acme": {string(pemBytes)}})
			claims := validClaims("acme")
			// Far-future expiry: a long -benchtime run must never cross exp
			// mid-benchmark and start measuring the rejection path.
			claims.Exp = time.Now().Add(24 * time.Hour).Unix()
			token, err := MintToken(key, alg, claims)
			if err != nil {
				b.Fatalf("MintToken(%s): %v", alg, err)
			}
			b.ReportAllocs()
			for b.Loop() {
				c, err := v.Verify(ctx, token)
				if err != nil {
					b.Fatalf("Verify(%s): %v", alg, err)
				}
				if c.Iss != "acme" {
					b.Fatalf("Verify(%s) iss = %q, want %q", alg, c.Iss, "acme")
				}
			}
		})
	}
}

func BenchmarkParsePublicKeysPEM(b *testing.B) {
	for _, alg := range benchVerifyAlgorithms {
		b.Run(alg, func(b *testing.B) {
			key, err := GenerateKey(alg)
			if err != nil {
				b.Fatalf("GenerateKey(%s): %v", alg, err)
			}
			pemBytes, err := MarshalPublicKeyPEM(key.Public())
			if err != nil {
				b.Fatalf("MarshalPublicKeyPEM: %v", err)
			}
			b.ReportAllocs()
			for b.Loop() {
				keys, err := ParsePublicKeysPEM(pemBytes)
				if err != nil || len(keys) != 1 {
					b.Fatalf("ParsePublicKeysPEM = (%d keys, %v), want exactly 1", len(keys), err)
				}
			}
		})
	}
}
