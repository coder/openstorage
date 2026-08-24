package lfsscan

import (
	"strings"
)

// LFSPointerMaxSize is the largest blob the reclamation scan treats as a
// candidate Git LFS pointer file. git-lfs itself refuses to decode blobs of
// 1024 bytes or more (its blobSizeCutoff); scanning one byte further errs on
// the inclusive side — a false positive only protects an object from
// retirement, never endangers one.
const LFSPointerMaxSize = 1024

// lfsPointerVersionPrefixes are the `version` values a pointer file may
// declare, per the git-lfs pointer spec (the hawser URL is the pre-rename
// spelling git-lfs still accepts).
var lfsPointerVersionPrefixes = []string{
	"version https://git-lfs.github.com/spec/",
	"version https://hawser.github.com/spec/",
}

// ParseLFSPointer decodes data as a Git LFS pointer file and returns the
// referenced sha256 OID (64 lowercase hex characters). ok is false when data
// is not a pointer.
//
// The decoder is deliberately MORE lenient than git-lfs: reclamation deletes
// registered objects that no reachable pointer references, so treating a
// dubious blob as a pointer merely spares an object, while rejecting a real
// pointer could retire live data. It therefore requires only the spec's two
// load-bearing properties — a `version` line before any key line and an
// `oid sha256:` key line — and ignores leading blank lines and per-line
// indentation (git-lfs itself decodes blank-led files and
// whitespace-indented version lines; Codex security rounds 8 and 9),
// unknown keys, ordering, a missing size line, and surrounding
// whitespace. It never panics on arbitrary input.
func ParseLFSPointer(data []byte) (oid string, ok bool) {
	if len(data) > LFSPointerMaxSize {
		return "", false
	}
	// Pointer files are UTF-8 text; a NUL byte marks binary content and
	// cannot appear in any spec key or value.
	if strings.IndexByte(string(data), 0) >= 0 {
		return "", false
	}
	lines := strings.Split(string(data), "\n")
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:] // git-lfs tolerates leading blank lines
	}
	if len(lines) == 0 {
		return "", false
	}
	first := strings.TrimSpace(lines[0])
	versioned := false
	for _, p := range lfsPointerVersionPrefixes {
		if strings.HasPrefix(first, p) {
			versioned = true
			break
		}
	}
	if !versioned {
		return "", false
	}
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		rest, found := strings.CutPrefix(line, "oid sha256:")
		if !found {
			continue
		}
		if !validLFSOID(rest) {
			return "", false
		}
		return rest, true
	}
	return "", false
}

// validLFSOID reports whether s is a 64-character lowercase hex sha256 OID.
func validLFSOID(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
