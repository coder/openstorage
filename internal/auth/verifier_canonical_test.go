package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lestrrat-go/jwx/v3/jws"
)

// Issue #312: RFC 7515 §2 fixes each compact-JWS segment to unpadded
// base64url, and RFC 4648 §3.5 lets decoders reject non-zero padding bits.
// jwx's default decoder is lenient (it guesses std/url, padded/raw, and is
// not Strict), and jws.Verify rebuilds the signing input by RE-ENCODING the
// decoded header and payload, so before the fix every segment accepted
// alternative spellings of the same bytes. These tests pin the canonical-only
// contract.

const b64urlAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

var segmentLabels = [3]string{"header", "payload", "signature"}

// lenientDecode mirrors what jwx v3.3.0's default decoder tolerates: CR/LF
// anywhere, '=' padding, the standard alphabet, and non-zero padding bits.
// It exists to prove that a mutant is encoding-only (same bytes as its
// canonical parent), so its rejection can only come from the encoding check.
func lenientDecode(t *testing.T, seg string) []byte {
	t.Helper()
	s := strings.NewReplacer("\r", "", "\n", "", "+", "-", "/", "_").Replace(seg)
	s = strings.TrimRight(s, "=")
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("lenient decode of %q: %v", seg, err)
	}
	return b
}

// isCanonicalJWT is an independent second algorithm for the canonical rule
// (character class + length + padding-bit mask, no decode/re-encode), used
// as the fuzz oracle and to cross-check the fixtures.
func isCanonicalJWT(token string) bool {
	segs := strings.Split(token, ".")
	if len(segs) != 3 {
		return false
	}
	for _, s := range segs {
		if s == "" || len(s)%4 == 1 {
			return false
		}
		for i := 0; i < len(s); i++ {
			if strings.IndexByte(b64urlAlphabet, s[i]) < 0 {
				return false
			}
		}
		last := strings.IndexByte(b64urlAlphabet, s[len(s)-1])
		switch len(s) % 4 {
		case 2: // last char carries 2 data bits, 4 padding bits
			if last&0x0f != 0 {
				return false
			}
		case 3: // last char carries 4 data bits, 2 padding bits
			if last&0x03 != 0 {
				return false
			}
		}
	}
	return true
}

type encodingMutant struct {
	label string // "<segment>/<kind>", used for coverage accounting
	token string
}

// encodingOnlyMutants returns alternative spellings of tok that decode to the
// same header, payload, and signature bytes under a lenient decoder.
func encodingOnlyMutants(tok string) []encodingMutant {
	segs := strings.Split(tok, ".")
	var out []encodingMutant
	add := func(i int, kind, seg string) {
		mod := append([]string(nil), segs...)
		mod[i] = seg
		out = append(out, encodingMutant{segmentLabels[i] + "/" + kind, strings.Join(mod, ".")})
	}
	for i, s := range segs {
		if tail := len(s) % 4; tail == 2 || tail == 3 {
			// Bit 0 of the last character is a padding bit for both tail
			// shapes (4 padding bits when tail==2, 2 when tail==3).
			idx := strings.IndexByte(b64urlAlphabet, s[len(s)-1])
			add(i, fmt.Sprintf("padbits-tail%d", tail), s[:len(s)-1]+string(b64urlAlphabet[idx^1]))
			add(i, "padded", s+strings.Repeat("=", 4-tail))
		}
		if strings.ContainsAny(s, "-_") {
			add(i, "stdalphabet", strings.NewReplacer("-", "+", "_", "/").Replace(s))
		}
		add(i, "lf-inside", s[:len(s)/2]+"\n"+s[len(s)/2:])
		add(i, "crlf-end", s+"\r\n")
		add(i, "cr-start", "\r"+s)
	}
	return out
}

// signWithKid signs c with an extra `kid` protected header so tests can
// steer the header segment's length (and hence its tail shape).
func signWithKid(t *testing.T, key crypto.Signer, alg string, c Claims, kid string) string {
	t.Helper()
	payload, err := json.Marshal(c) // same encoding as MintToken
	if err != nil {
		t.Fatal(err)
	}
	hdrs := jws.NewHeaders()
	if err := hdrs.Set("typ", "JWT"); err != nil {
		t.Fatal(err)
	}
	if err := hdrs.Set("kid", kid); err != nil {
		t.Fatal(err)
	}
	signed, err := jws.Sign(payload, jws.WithKey(signatureAlgorithms[alg], key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("sign with kid: %v", err)
	}
	return string(signed)
}

// canonicalCorpus returns genuinely signed canonical tokens covering every
// allowed algorithm, the constant fuzz fixtures, and both padding-bit tail
// shapes (encoded length mod 4 of 2 and 3) in every segment, plus a verifier
// that trusts all of their keys.
func canonicalCorpus(t *testing.T) (*OrgKeyVerifier, map[string]string) {
	t.Helper()
	pems := []string{fuzzES256PEM, fuzzES384PEM, fuzzES512PEM, fuzzRSAPEM}
	corpus := map[string]string{
		"fixture/ES256": fuzzES256Token,
		"fixture/ES384": fuzzES384Token,
		"fixture/ES512": fuzzES512Token,
		"fixture/RS256": fuzzRS256Token,
		"fixture/PS256": fuzzPS256Token,
	}
	keys := map[string]crypto.Signer{}
	for _, alg := range AllowedAlgorithms {
		key := mustGenerateKey(t, alg)
		keys[alg] = key
		pems = append(pems, mustPublicPEM(t, key))
		corpus["minted/"+alg] = mustMint(t, key, alg, validClaims("acme"))
	}
	// A 2056-bit RSA key yields a 257-byte signature: 343 characters, the
	// tail-3 shape that no default-size key produces.
	rsa2056, err := rsa.GenerateKey(rand.Reader, 2056)
	if err != nil {
		t.Fatal(err)
	}
	pems = append(pems, mustPublicPEM(t, rsa2056))
	corpus["minted/RS256-2056"] = mustMint(t, rsa2056, "RS256", validClaims("acme"))
	corpus["minted/PS256-2056"] = mustMint(t, rsa2056, "PS256", validClaims("acme"))
	// Header and payload tail shapes: vary kid / repo length. Six '~'
	// bytes always contain an aligned 0x7e7e7e group, which encodes as
	// "fn5-", so these header and payload segments also exercise the
	// standard-alphabet mutant.
	for n := 0; n < 3; n++ {
		c := validClaims("acme")
		c.Repo = "repo-~~~~~~" + strings.Repeat("x", n)
		corpus[fmt.Sprintf("shape/%d", n)] = signWithKid(t, keys["ES256"], "ES256", c, "~~~~~~"+strings.Repeat("k", n))
	}
	return NewVerifier(fakeKeySource{"acme": pems}), corpus
}

func TestVerify_RejectsNonCanonicalBase64url(t *testing.T) {
	v, corpus := canonicalCorpus(t)
	covered := map[string]int{}
	for name, tok := range corpus {
		t.Run(name, func(t *testing.T) {
			if !isCanonicalJWT(tok) {
				t.Fatalf("fixture is not canonical: %q", tok)
			}
			if _, err := v.Verify(context.Background(), tok); err != nil {
				t.Fatalf("canonical token rejected: %v", err)
			}
			parent := strings.Split(tok, ".")
			for _, m := range encodingOnlyMutants(tok) {
				if m.token == tok {
					t.Fatalf("%s: mutant equals its parent", m.label)
				}
				if isCanonicalJWT(m.token) {
					t.Fatalf("%s: mutant unexpectedly canonical: %q", m.label, m.token)
				}
				got := strings.Split(m.token, ".")
				for i := range parent {
					if string(lenientDecode(t, got[i])) != string(lenientDecode(t, parent[i])) {
						t.Fatalf("%s: mutant changes %s bytes, not just their encoding", m.label, segmentLabels[i])
					}
				}
				_, err := v.Verify(context.Background(), m.token)
				if !errors.Is(err, errNonCanonicalSegment) {
					t.Errorf("%s: Verify err = %v, want errNonCanonicalSegment", m.label, err)
				}
				covered[m.label]++
			}
		})
	}
	// Coverage: every segment saw a padding-bit mutation for both tail
	// shapes, plus every other mutation kind.
	for _, seg := range segmentLabels {
		for _, kind := range []string{"padbits-tail2", "padbits-tail3", "padded", "stdalphabet", "lf-inside", "crlf-end", "cr-start"} {
			if covered[seg+"/"+kind] == 0 {
				t.Errorf("no %s/%s mutant was exercised; extend canonicalCorpus", seg, kind)
			}
		}
	}
}

// TestVerify_RejectsMalformedSegments covers shape errors that are not mere
// re-spellings: empty segments, impossible lengths, foreign characters.
func TestVerify_RejectsMalformedSegments(t *testing.T) {
	v, corpus := canonicalCorpus(t)
	segs := strings.Split(corpus["minted/ES256"], ".")
	mod1 := segs[2]
	for len(mod1)%4 != 1 {
		mod1 += "A"
	}
	for name, tok := range map[string]string{
		"empty header":     "." + segs[1] + "." + segs[2],
		"empty payload":    segs[0] + ".." + segs[2],
		"empty signature":  segs[0] + "." + segs[1] + ".",
		"all empty":        "..",
		"length mod 4 = 1": segs[0] + "." + segs[1] + "." + mod1,
		"space inside":     segs[0] + "." + segs[1] + "." + segs[2][:8] + " " + segs[2][8:],
		"tab at end":       segs[0] + "." + segs[1] + "." + segs[2] + "\t",
		"leading space":    " " + segs[0] + "." + segs[1] + "." + segs[2],
	} {
		if _, err := v.Verify(context.Background(), tok); !errors.Is(err, errNonCanonicalSegment) {
			t.Errorf("%s: Verify err = %v, want errNonCanonicalSegment", name, err)
		}
	}
}
