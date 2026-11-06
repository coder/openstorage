package api

import (
	"encoding/base64"
	"slices"
	"unicode/utf8"
)

// A git tree entry name may hold any byte except NUL and "/". JSON strings
// hold Unicode text, so encoding/json writes each byte of a name that is
// not valid UTF-8 as U+FFFD, and two names can list as the same path (#587).
// A response field F that carries such a path therefore has a sibling
// F_base64: standard base64 (RFC 4648 section 4, padded) of the raw bytes,
// present only when the bytes are not valid UTF-8. A string-array field has
// an array sibling of the same length and order, present only when any
// element is not valid UTF-8.
//
// Every sibling field is `omitempty` or `omitzero`, so a response for names
// that are valid UTF-8 stays byte for byte what it was. The path-field guard
// (rawpath_guard_test.go) fails the build when a path field has no sibling.

// rawPathBase64 returns the base64 of p's bytes when p is not valid UTF-8,
// and "" when p is valid UTF-8 (including the empty path).
func rawPathBase64(p string) string {
	if utf8.ValidString(p) {
		return ""
	}
	return encodeRawPath(p)
}

// rawPathsBase64 returns the base64 of every element of paths, in order,
// when any element is not valid UTF-8, and nil when all are valid UTF-8.
func rawPathsBase64(paths []string) []string {
	if !anyInvalidUTF8(paths) {
		return nil
	}
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = encodeRawPath(p)
	}
	return out
}

// encodeRawPath is the base64 of p's bytes, whether or not p is valid UTF-8.
func encodeRawPath(p string) string {
	return base64.StdEncoding.EncodeToString([]byte(p))
}

func anyInvalidUTF8(paths []string) bool {
	return slices.ContainsFunc(paths, func(p string) bool { return !utf8.ValidString(p) })
}
