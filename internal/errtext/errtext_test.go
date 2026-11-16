package errtext

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBound(t *testing.T) {
	if MaxBytes != 16384 {
		t.Fatalf("MaxBytes = %d, the docs say 16384", MaxBytes)
	}
	exact := strings.Repeat("a", 16384)
	for _, tc := range []struct{ name, in, want string }{
		{"a text of exactly the limit", exact, exact},
		{"a text one byte over", exact + "b", exact + "…"},
		// After "a", byte 16384 is the second byte of an "é": the cut moves
		// back to the start of that rune.
		{"a cut inside a two-byte rune", "a" + strings.Repeat("é", 8200), "a" + strings.Repeat("é", 8191) + "…"},
		// 16384 is not a multiple of 3: the cut is one byte into a rune.
		{"a cut inside a three-byte rune", strings.Repeat("€", 5500), strings.Repeat("€", 5461) + "…"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Bound(tc.in)
			if got != tc.want || !utf8.ValidString(got) {
				t.Fatalf("Bound of %d bytes = %d bytes (valid %v), want %d bytes", len(tc.in), len(got), utf8.ValidString(got), len(tc.want))
			}
		})
	}
}

func TestQuote(t *testing.T) {
	if MaxQuotedBytes != 256 {
		t.Fatalf("MaxQuotedBytes = %d, the docs say 256", MaxQuotedBytes)
	}
	exact := strings.Repeat("<", 256)
	for _, tc := range []struct{ name, prefix, in, want string }{
		{"a short value", "", "gitea", `"gitea"`},
		{"a value of exactly the limit", "", exact, `"` + exact + `"`},
		{"a value one byte over", "", exact + "<", `"` + exact + `…" (257 bytes)`},
		{"a long value", "", strings.Repeat("x", 1_000_000), `"` + strings.Repeat("x", 256) + `…" (1000000 bytes)`},
		// The prefix is never cut, and the length is the value's own.
		{"a prefix", "base_repo.", exact + "<", `"base_repo.` + exact + `…" (257 bytes)`},
		// A cut inside a rune moves back to its start; Go quotes "é" as is.
		{"a cut inside a rune", "", "a" + strings.Repeat("é", 200), `"a` + strings.Repeat("é", 127) + `…" (401 bytes)`},
		{"control characters are escaped", "", "a\nb", `"a\nb"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Quote(tc.prefix, tc.in); got != tc.want {
				t.Fatalf("Quote(%q, %d bytes) = %.300q, want %.300q", tc.prefix, len(tc.in), got, tc.want)
			}
		})
	}
}
