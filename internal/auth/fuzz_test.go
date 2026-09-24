// Fuzz targets for package auth's untrusted-input surfaces (issue #91):
// OrgKeyVerifier.Verify consumes raw Bearer/`t:<jwt>` tokens on every
// endpoint, and ParseRefPolicies consumes the `refs` claim of a verified
// token. Both must degrade to errors on arbitrary input — never a panic,
// and never a fabricated identity.
package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// FuzzVerify's fixtures for the ECDSA and RSA families: public halves plus
// pre-signed tokens carrying exactly the fuzzVerifyClaims claim set. They
// are constants because worker processes re-run the fuzz setup, so every
// process must trust and expect identical material (Codex review), and
// ecdsa/rsa.GenerateKey are deliberately nondeterministic even with a fixed
// randomness stream (randutil.MaybeReadByte) — unlike ed25519.NewKeyFromSeed,
// which stays runtime-derived below. Only PUBLIC keys appear here; the
// private halves were discarded after signing. Regenerate by minting tokens
// for fresh keys with MintToken(key, alg, fuzzVerifyClaims) and
// MarshalPublicKeyPEM(key.Public()) — see PR #94. If these tokens' claims
// ever drift from fuzzVerifyClaims, the seed replay fails the claims oracle
// immediately and loudly.
const (
	fuzzES256PEM = `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE0kUaSn+vRPCX7GsnSJ7IS5Ykxe08
sXJiEO1ZYa10mu4CAWBiWzIhkLePKFSTs3mpMy/b5YTLzfZbWZHpF730GA==
-----END PUBLIC KEY-----
`
	fuzzES256Token = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhY21lIiwic3ViIjoiQHBpZXJyZS9zdG9yYWdlIiwicmVwbyI6InJlcG8tMSIsInNjb3BlcyI6WyJnaXQ6cmVhZCJdLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6NDEwMjQ0NDgwMCwicmVmcyI6W1sicmVmcy9oZWFkcy8qIixbIm5vLXB1c2giXV1dLCJvcHMiOlsibm8tZm9yY2UtcHVzaCJdfQ.TFo4EUD4CBX67-a1yeLBbI8Rxucb26YsevYCzpZSX9Ba0ck9Y1Ifi8HA2uWHtF4Crp7ZRgr3qEhQctwo-dIa-Q"
	fuzzES384PEM   = `-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAE5pvVvb2mTaOTZIdjXQpOe6m31PQai6MH
06UGw3KqDC22ksBa5hGUNVn9dY2c1IFAJQMrbn/dOewqSt1a9A87IocdyoSt2KRv
pu/7dQAbe1bevi56m3McqJiEdNBE6lM4
-----END PUBLIC KEY-----
`
	fuzzES384Token = "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhY21lIiwic3ViIjoiQHBpZXJyZS9zdG9yYWdlIiwicmVwbyI6InJlcG8tMSIsInNjb3BlcyI6WyJnaXQ6cmVhZCJdLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6NDEwMjQ0NDgwMCwicmVmcyI6W1sicmVmcy9oZWFkcy8qIixbIm5vLXB1c2giXV1dLCJvcHMiOlsibm8tZm9yY2UtcHVzaCJdfQ.ACNEaw1JBxGuwfi24tfPs7BYG2buTFxi_5_YiEL61_YtULcIUISKmANlZDVyXY0UfHHoYfdM3Z56fMMKrhFj3lUaTIOKU34bQg-65HR1aUe2mmyHV2dKV8naEmBMafEM"
	fuzzES512PEM   = `-----BEGIN PUBLIC KEY-----
MIGbMBAGByqGSM49AgEGBSuBBAAjA4GGAAQBVpDJymm9aaPSs3/c11zkERqJ77y2
edo1Ijv81gHXz5Bt6f74P8+zMKoCJeQYGCCnSlYFDBMx5MVFY30tDr4t9zoBFa8y
jEVgO1EH2TILQt1uss+NbNtntoi/GlfwG1QwMnbjeXa6+VHJXaLZcyvKSCw7mTea
cgv2pYD/06CixXE8Ous=
-----END PUBLIC KEY-----
`
	fuzzES512Token = "eyJhbGciOiJFUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhY21lIiwic3ViIjoiQHBpZXJyZS9zdG9yYWdlIiwicmVwbyI6InJlcG8tMSIsInNjb3BlcyI6WyJnaXQ6cmVhZCJdLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6NDEwMjQ0NDgwMCwicmVmcyI6W1sicmVmcy9oZWFkcy8qIixbIm5vLXB1c2giXV1dLCJvcHMiOlsibm8tZm9yY2UtcHVzaCJdfQ.AfZSeLlE6H0AtuXYXSj7_LSViKsS2rmpBVfDIR0lyBK9XClHyIpQiEX0Yf9G2YPGJqTzSx3rOEJ1YGt2VWECZGqwADSXqbI777ockowUd8o5z6gS-q4gp8I-8BANoqBfIayOQsxSlqGuLfgsSSBtsncYs_2oXj6wezzN-gQ4MtcN6Du1"
	fuzzRSAPEM     = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyHrPuzEvsiqA7CsF5Bqx
U9CORlWyzVQq3HU5e1IsIi9RUb1YwqDBBzgbXftjll0b4+CYFNP1LE9k2eXZnV1e
4do2y9H0v/UM1NKH7cscrKHhdgSPo662P27/bnBzFXf+3eLlZVL/LM4cEJF7HywQ
0sLzpj9Wulr1cp3dN1Pg6IVnCFvrYJGi2rItaVHZgrlYljx1rVY6fItP8id3s9uC
CQyLr7pMJHIuqmBXb+Sk2EvclM6wdNT8/C5zbhxO6MIuactQy23I0XDtIVJXHcDF
Ar0qajyIXz5zVCKe2MNbzbnEny5S8cy+wNify5JiOcksTi8K3Bcqp0ypBRUNx3cM
qQIDAQAB
-----END PUBLIC KEY-----
`
	fuzzRS256Token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhY21lIiwic3ViIjoiQHBpZXJyZS9zdG9yYWdlIiwicmVwbyI6InJlcG8tMSIsInNjb3BlcyI6WyJnaXQ6cmVhZCJdLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6NDEwMjQ0NDgwMCwicmVmcyI6W1sicmVmcy9oZWFkcy8qIixbIm5vLXB1c2giXV1dLCJvcHMiOlsibm8tZm9yY2UtcHVzaCJdfQ.rD1ilHYGVaWzGOU0ssLepnaz4iVoh_IyTaJ4v-kPcrIpdqlUMWFMRjvu8IMkYqpueLU4TgINvJjGcLy8TFthxRCjZB-uOWSz-g3OEkYHERzYQxoAJRYJzuGrDmsmkg5MviwhKFKP_WsHpwrVWBEGfJVx13-bx2iN4p_E5J1huzg9GCJZU-iExfcM0DbiEtq38GFzl0ORJtf-a6tt951QckrV5pCThcZxdixwbqt2_CoFOQqBrsIa7PDJkP2qVaRnppPWQWmUhqXN2g97UQiAQPevzZBpI-MGaa-w7d6U4iD0KGzJSTx_rVUsE50Oxy3g24R6zuMAcYiaPHYkY3fkyg"
	fuzzPS256Token = "eyJhbGciOiJQUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhY21lIiwic3ViIjoiQHBpZXJyZS9zdG9yYWdlIiwicmVwbyI6InJlcG8tMSIsInNjb3BlcyI6WyJnaXQ6cmVhZCJdLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6NDEwMjQ0NDgwMCwicmVmcyI6W1sicmVmcy9oZWFkcy8qIixbIm5vLXB1c2giXV1dLCJvcHMiOlsibm8tZm9yY2UtcHVzaCJdfQ.b9JnBBxGgX9Ci9-Lv2I8Nx91tQTNCMmBJ9GolHr1k-osf8j27HmbhWWS5p7T-L4xEVTJ-LZGTBnQCrtBgWiRfpPRHdXD9bvEFJN08nrGRl2miEqv0zHJ0pjhQ-jczraOovtbXBq0ubcjQA2OV7K7EUCzjCAdWiuV2-ZqCGt-4N6qzsxVd9jEFflP9BWO4ca_8qgjCa2rMr4_jl5_e5lV9o7H3io4RHaeUeS0TtYJzaZcpHCB_n3ElHnHVN4iZRjoWRtE31YFyTYOtv9sfD-CG5ck2Ge-Mwq0E8COD4h--X0-wzXVygHMkNNVLXKDgCU4xupx4x6MBcZf6GFhknhloA"
)

// fuzzVerifyClaims is the single claim set behind every genuinely signed
// valid seed (constant fixtures above and runtime EdDSA mints below). None
// of the signing keys are registered anywhere real, so any input Verify
// accepts must carry exactly this identity — anything else would mean the
// fuzzer forged a signature.
var fuzzVerifyClaims = Claims{
	Iss:    "acme",
	Sub:    SubjectLiteral,
	Repo:   "repo-1",
	Scopes: []string{ScopeGitRead},
	Iat:    1700000000, // fixed: 2023-11-14T22:13:20Z
	Exp:    4102444800, // fixed: 2100-01-01T00:00:00Z, outlives any campaign
	Refs:   json.RawMessage(`[["refs/heads/*",["no-push"]]]`),
	Ops:    []string{OpNoForcePush},
}

func FuzzVerify(f *testing.F) {
	// Everything below is deterministic — constant ECDSA/RSA fixtures,
	// fixed-seed Ed25519 keys (deterministic signatures per RFC 8032), and
	// fixed claim timestamps — so the fuzz coordinator and every worker
	// process trust identical keys and expect identical claims. Worker
	// processes re-run this setup (Codex review): with fresh per-process
	// randomness each worker would reject the coordinator's genuinely
	// signed corpus entries and never fuzz the success path, and
	// process-local time.Now() claims would make a re-verified seed trip
	// the claims oracle spuriously.
	registered := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xA1}, ed25519.SeedSize))
	secondary := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xB2}, ed25519.SeedSize))
	unregistered := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xC3}, ed25519.SeedSize))
	regPEM, err := MarshalPublicKeyPEM(registered.Public())
	if err != nil {
		f.Fatal(err)
	}
	secPEM, err := MarshalPublicKeyPEM(secondary.Public())
	if err != nil {
		f.Fatal(err)
	}
	v := NewVerifier(fakeKeySource{"acme": {
		string(regPEM), string(secPEM),
		fuzzES256PEM, fuzzES384PEM, fuzzES512PEM, fuzzRSAPEM,
	}})

	good := fuzzVerifyClaims
	mint := func(key ed25519.PrivateKey, c Claims) string {
		tok, err := MintToken(key, "EdDSA", c)
		if err != nil {
			f.Fatal(err)
		}
		return tok
	}
	expired := good
	expired.Exp = 946684800 // fixed: 2000-01-01T00:00:00Z
	wrongSub := good
	wrongSub.Sub = "someone-else"
	wrongOrg := good
	wrongOrg.Iss = "evil"

	goodJSON, err := json.Marshal(good)
	if err != nil {
		f.Fatal(err)
	}
	// Classic alg-confusion seed: HS256 "signed" with the registered public
	// PEM text as the HMAC secret. Must be rejected by the algorithm
	// allow-list, never verified.
	hsInput := b64seg([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + b64seg(goodJSON)
	mac := hmac.New(sha256.New, regPEM)
	mac.Write([]byte(hsInput))
	hs256 := hsInput + "." + b64seg(mac.Sum(nil))
	// Same attack with the RSA public PEM as the secret (the textbook
	// RS256→HS256 downgrade).
	macRSA := hmac.New(sha256.New, []byte(fuzzRSAPEM))
	macRSA.Write([]byte(hsInput))
	hs256RSA := hsInput + "." + b64seg(macRSA.Sum(nil))
	noneTok := b64seg([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + b64seg(goodJSON) + "."
	// JSON-serialized JWS with exactly two dots hidden in an unprotected
	// header value (slips the compact-form pre-check; see verifier.go).
	flattened := `{"payload":"` + b64seg(goodJSON) + `","protected":"` +
		b64seg([]byte(`{"alg":"EdDSA"}`)) + `","header":{"kid":".."},"signature":"` +
		b64seg([]byte("nope")) + `"}`

	valid := mint(registered, good)
	// Startup check: every accepted fixture must actually verify. The fuzz
	// oracle is one-directional (verified ⇒ good claims), so without this a
	// broken fixture would silently degrade to the error path and the
	// campaign would stop covering that family's success path.
	for i, tok := range []string{
		valid, mint(secondary, good),
		fuzzES256Token, fuzzES384Token, fuzzES512Token, fuzzRS256Token, fuzzPS256Token,
	} {
		if _, err := v.Verify(context.Background(), tok); err != nil {
			f.Fatalf("accepted fixture %d does not verify: %v", i, err)
		}
	}
	tampered := []byte(valid)
	tampered[len(tampered)/2] ^= 0x01
	for _, seed := range []string{
		valid,
		mint(secondary, good),    // second registered key: multi-key iteration
		mint(unregistered, good), // right shape, unknown key: must fail
		// Constant accepted fixtures: successful verification for every
		// remaining allowed algorithm family (Codex review round 2).
		fuzzES256Token,
		fuzzES384Token,
		fuzzES512Token,
		fuzzRS256Token,
		fuzzPS256Token,
		mint(registered, expired),  // signed but expired
		mint(registered, wrongSub), // signed but wrong sub
		mint(registered, wrongOrg), // signed but unknown issuer
		hs256,
		hs256RSA,
		noneTok,
		flattened,
		string(tampered),
		"",
		"..",
		"a.b.c",
		valid + ".",
	} {
		f.Add(seed)
	}
	// Issue #312: encoding-only re-spellings of accepted tokens (padding
	// bits, '=', standard alphabet, CR/LF) must all be rejected.
	for _, tok := range []string{valid, fuzzES256Token, fuzzRS256Token} {
		for _, m := range encodingOnlyMutants(tok) {
			f.Add(m.token)
		}
	}

	f.Fuzz(func(t *testing.T, token string) {
		claims, err := v.Verify(context.Background(), token)
		if err != nil {
			if claims != nil {
				t.Fatalf("Verify returned claims alongside error %v", err)
			}
			return
		}
		if claims == nil {
			t.Fatal("Verify returned nil error and nil claims")
		}
		// Canonical-encoding oracle (issue #312), checked with a second
		// algorithm independent of checkCanonicalSegments.
		if !isCanonicalJWT(token) {
			t.Fatalf("Verify accepted a non-canonical token: %q", token)
		}
		// Graceful-behavior oracle: only material signed by this process's
		// keys can verify, and every valid seed carries the `good` claim
		// set; a verified token with any other identity is a forgery.
		if claims.Iss != good.Iss || claims.Sub != good.Sub || claims.Repo != good.Repo ||
			!slices.Equal(claims.Scopes, good.Scopes) || claims.Iat != good.Iat ||
			claims.Exp != good.Exp || !bytes.Equal(claims.Refs, good.Refs) ||
			!slices.Equal(claims.Ops, good.Ops) {
			t.Fatalf("Verify accepted a token with fabricated claims: %+v", claims)
		}
	})
}

func FuzzParseRefPolicies(f *testing.F) {
	for _, seed := range []string{
		``,
		`null`,
		`[]`,
		`[["refs/heads/*",["no-push"]]]`,
		`[["main",["no-force-push","verify-sig"]],["*",[]]]`,
		`[["*",["no-push"]],["HEAD",["verify-sig"]]]`,
		`[["refs/heads/main"]]`,          // 1-element tuple
		`[["a",["b"],"c"]]`,              // 3-element tuple
		`[[42,["no-push"]]]`,             // non-string pattern
		`[["x",[1,2]]]`,                  // non-string ops
		`[[["nested"],["no-push"]]]`,     // array pattern
		`[{"pattern":"x","ops":[]}]`,     // object, not tuple
		`[["feature/*",["no-push"]]]`,    // shorthand prefix glob
		`[["refs/tags/v*",["no-push"]]]`, // trailing * not after /
		strings.Repeat(`[`, 500) + strings.Repeat(`]`, 500),
		`[["` + strings.Repeat("a", 4096) + `",[]]]`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		policies, err := ParseRefPolicies(raw)
		if err != nil {
			if policies != nil {
				t.Fatalf("ParseRefPolicies returned policies alongside error %v", err)
			}
			return
		}
		for _, p := range policies {
			if want := NormalizeRefName(p.Pattern); p.Pattern != want {
				t.Fatalf("parsed pattern %q is not normalized (want %q)", p.Pattern, want)
			}
		}
		// Marshal → reparse must be a fixpoint: the tuple encoding and the
		// normalization rule agree, so a stored/re-minted claim can never
		// drift on a second pass.
		b1, err := json.Marshal(policies)
		if err != nil {
			t.Fatalf("marshal parsed policies: %v", err)
		}
		again, err := ParseRefPolicies(string(b1))
		if err != nil {
			t.Fatalf("reparse of marshaled policies failed: %v", err)
		}
		b2, err := json.Marshal(again)
		if err != nil {
			t.Fatalf("marshal reparsed policies: %v", err)
		}
		if !bytes.Equal(b1, b2) {
			t.Fatalf("marshal/reparse not a fixpoint:\n first %s\nsecond %s", b1, b2)
		}
		// Downstream consumers must stay total on whatever parsed.
		for _, ref := range []string{"refs/heads/main", "main", "HEAD", ""} {
			_, _ = MatchPolicy(policies, ref)
		}
		_ = FoldOps(policies, []string{OpNoPush})
	})
}
