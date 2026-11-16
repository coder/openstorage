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
