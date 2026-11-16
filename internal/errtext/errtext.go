// Package errtext bounds the request text that an error answer repeats.
package errtext

import (
	"strconv"
	"unicode/utf8"
)

// MaxBytes bounds a text that an error answer can repeat from the request:
// the `error`, `detail` and `invalid_params` names of an API problem body,
// the `result.message` of a commit write, and the error text of a git or
// LFS route. A 1 MiB value that a message names, or one of bytes that JSON
// escapes to six, would otherwise make an answer of several MiB.
const MaxBytes = 16384

// MaxQuotedBytes bounds one request value that an answer quotes (Quote):
// an unknown member name, a sync provider, operation, access or host, a
// ref policy op of a token, a webhook URL or its host. A value that the
// server can use is far shorter, so it is quoted whole. A body value can
// reach 1 MiB, and JSON escapes up to six bytes for each byte, so a value
// quoted whole could make an answer of several MiB.
const MaxQuotedBytes = 256

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

// Quote quotes prefix+v with strconv.Quote for an answer. A v of more than
// MaxQuotedBytes is cut (Cut), and its length in bytes follows the quote:
// "xx…" (1000000 bytes). prefix is text of the server, such as the path of
// a known field, and is never cut.
func Quote(prefix, v string) string {
	q := strconv.Quote(prefix + Cut(v, MaxQuotedBytes))
	if len(v) > MaxQuotedBytes {
		q += " (" + strconv.Itoa(len(v)) + " bytes)"
	}
	return q
}
