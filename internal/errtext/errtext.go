// Package errtext bounds the request text that an error answer repeats.
package errtext

import "unicode/utf8"

// MaxBytes bounds a text that an error answer can repeat from the request:
// the `error`, `detail` and `invalid_params` names of an API problem body,
// the `result.message` of a commit write, and the error text of a git or
// LFS route. A 1 MiB value that a message names, or one of bytes that JSON
// escapes to six, would otherwise make an answer of several MiB.
const MaxBytes = 16384

// Bound cuts s to MaxBytes (Cut).
func Bound(s string) string { return Cut(s, MaxBytes) }

// Cut cuts s to limit bytes at a rune boundary and ends it with an
// ellipsis. A shorter s comes back unchanged.
func Cut(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
