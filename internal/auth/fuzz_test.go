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

func FuzzVerify(f *testing.F) {
	// Everything below is deterministic — fixture key seeds, fixed claim
	// timestamps, and Ed25519's deterministic signatures (RFC 8032) — so
	// the fuzz coordinator and every worker process trust identical keys
	// and mint byte-identical seeds. Worker processes re-run this setup
	// (Codex review): with fresh per-process randomness each worker would
	// reject the coordinator's genuinely signed corpus entries and never
	// fuzz the success path, and process-local time.Now() claims would
	// make a re-verified seed trip the claims oracle spuriously. These are
	// throwaway fuzz fixtures for the in-process verifier only — never
	// register them anywhere real.
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
	v := NewVerifier(fakeKeySource{"acme": {string(regPEM), string(secPEM)}})

	// The single claim set behind every genuinely signed valid seed. The
	// signing keys are never registered outside this fuzz target, so any
	// input Verify accepts must carry exactly this identity — anything
	// else would mean the fuzzer forged a signature.
	good := Claims{
		Iss:    "acme",
		Sub:    SubjectLiteral,
		Repo:   "repo-1",
		Scopes: []string{ScopeGitRead},
		Iat:    1700000000, // fixed: 2023-11-14T22:13:20Z
		Exp:    4102444800, // fixed: 2100-01-01T00:00:00Z, outlives any campaign
		Refs:   json.RawMessage(`[["refs/heads/*",["no-push"]]]`),
		Ops:    []string{OpNoForcePush},
	}
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
	noneTok := b64seg([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + b64seg(goodJSON) + "."
	// JSON-serialized JWS with exactly two dots hidden in an unprotected
	// header value (slips the compact-form pre-check; see verifier.go).
	flattened := `{"payload":"` + b64seg(goodJSON) + `","protected":"` +
		b64seg([]byte(`{"alg":"EdDSA"}`)) + `","header":{"kid":".."},"signature":"` +
		b64seg([]byte("nope")) + `"}`

	valid := mint(registered, good)
	tampered := []byte(valid)
	tampered[len(tampered)/2] ^= 0x01
	for _, seed := range []string{
		valid,
		mint(secondary, good),      // second registered key: multi-key iteration
		mint(unregistered, good),   // right shape, unknown key: must fail
		mint(registered, expired),  // signed but expired
		mint(registered, wrongSub), // signed but wrong sub
		mint(registered, wrongOrg), // signed but unknown issuer
		hs256,
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
