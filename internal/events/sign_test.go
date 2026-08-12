package events

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"testing"
)

// TestSignGoldenVector locks the exact docs/06 header format against an
// externally computed HMAC (python3 hmac/hashlib over "1737369000.{body}").
func TestSignGoldenVector(t *testing.T) {
	got := Sign("whsec_test_0123456789abcdef", 1737369000, []byte(`{"hello":"world"}`))
	want := "t=1737369000,sha256=8e87d3e0fafc83af38cdb8239c9e358d93102119c88b30f66c4e5966dad19106"
	if got != want {
		t.Fatalf("Sign golden vector mismatch:\n got %s\nwant %s", got, want)
	}
}

var sigRe = regexp.MustCompile(`\At=\d+,sha256=[0-9a-f]{64}\z`)

// TestSignFormatAndSecondAlgorithm cross-checks Sign against an
// independently constructed one-shot HMAC over the concatenated signed
// string (second-algorithm validation) and pins the header shape.
func TestSignFormatAndSecondAlgorithm(t *testing.T) {
	secret, ts, body := "s3cret", int64(42), []byte("payload bytes")
	got := Sign(secret, ts, body)
	if !sigRe.MatchString(got) {
		t.Fatalf("Sign format %q does not match t=<unix>,sha256=<64 hex>", got)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(fmt.Appendf(nil, "%d.%s", ts, body))
	want := fmt.Sprintf("t=%d,sha256=%s", ts, hex.EncodeToString(mac.Sum(nil)))
	if got != want {
		t.Fatalf("Sign disagrees with reference HMAC:\n got %s\nwant %s", got, want)
	}
}

// TestSignTimestampChangesDigest: a different timestamp must change the
// digest, not just the t= prefix — that is what makes re-signing on retry
// observable.
func TestSignTimestampChangesDigest(t *testing.T) {
	body := []byte(`{"a":1}`)
	sig1 := Sign("k", 1000, body)
	sig2 := Sign("k", 2000, body)
	digest := func(s string) string { return s[len(s)-64:] }
	if digest(sig1) == digest(sig2) {
		t.Fatalf("digest did not change with timestamp: %s vs %s", sig1, sig2)
	}
}
