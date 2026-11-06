package api

import (
	"encoding/base64"
	"io"
	"slices"
	"strings"
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
//
// A request that names a path has the same sibling, which holds the base64 of
// the raw bytes: paths_base64 and *_globs_base64 in grep and archive bodies,
// path_base64 in a commit-pack file operation. decodeRawPath reads them.

// rawPathBase64 returns the base64 of p's bytes when p is not valid UTF-8,
// and "" when p is valid UTF-8 (including the empty path).
func rawPathBase64(p string) string {
	if utf8.ValidString(p) {
		return ""
	}
	return encodeRawPath(p)
}

// rawPathsBase64List is the streamed list key, holding the base64 of every
// element of paths in order. The caller adds it only when anyInvalidUTF8(paths),
// with an empty list for key in the response template.
func rawPathsBase64List(key string, paths []string) jsonList {
	return jsonList{key, func(yield func(any) bool) {
		for _, p := range paths {
			if !yield(base64JSONString(p)) {
				return
			}
		}
	}}
}

// encodeRawPath is the base64 of p's bytes, whether or not p is valid UTF-8.
func encodeRawPath(p string) string {
	return base64.StdEncoding.EncodeToString([]byte(p))
}

// base64JSONString is a streamed list item: the base64 of its bytes as a JSON
// string. It writes through a base64 encoder in fixed chunks, so a path of any
// length costs no second string and no marshal buffer. Base64 text holds no
// character that JSON escapes, so the bytes equal json.Marshal of
// encodeRawPath.
type base64JSONString string

func (s base64JSONString) writeJSONItem(w io.Writer) error {
	if _, err := w.Write([]byte{'"'}); err != nil {
		return err
	}
	enc := base64.NewEncoder(base64.StdEncoding, w)
	var chunk [32 << 10]byte
	for rest := string(s); rest != ""; {
		n := copy(chunk[:], rest)
		rest = rest[n:]
		if _, err := enc.Write(chunk[:n]); err != nil {
			return err
		}
	}
	if err := enc.Close(); err != nil {
		return err
	}
	_, err := w.Write([]byte{'"'})
	return err
}

func anyInvalidUTF8(paths []string) bool {
	return slices.ContainsFunc(paths, func(p string) bool { return !utf8.ValidString(p) })
}

// decodeRawPath returns the bytes of s when s is canonical padded standard
// base64. Go's decoder alone also accepts CR and LF inside the text and
// non-zero padding bits, so two texts could name one path.
func decodeRawPath(s string) (string, bool) {
	raw, err := base64.StdEncoding.Strict().DecodeString(s)
	if err != nil || base64.StdEncoding.EncodeToString(raw) != s {
		return "", false
	}
	return string(raw), true
}

// decodeRawPatterns decodes the elements of a request list such as
// paths_base64. Each decoded value must pass the check that the plain list
// applies: not blank, and no NUL byte. bad is the index of the first
// element that fails, or -1.
func decodeRawPatterns(list []string) (decoded []string, bad int) {
	decoded = make([]string, 0, len(list))
	for i, enc := range list {
		p, ok := decodeRawPath(enc)
		if !ok || strings.TrimSpace(p) == "" || strings.ContainsRune(p, 0) {
			return nil, i
		}
		decoded = append(decoded, p)
	}
	return decoded, -1
}
