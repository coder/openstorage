package lfsscan

import (
	"strings"
)

// LFSPointerMaxSize is the pointer-decode window: the prefix of a candidate
// blob the reclamation scan parses, mirroring git-lfs's blobSizeCutoff —
// its decoder reads at most this many bytes and accepts a blob of ANY total
// size whose window decodes (a padded pointer smudges fine, Codex security
// round 19). The scan therefore parses this window of every reachable blob
// rather than filtering on total size; a false positive only protects an
// object from retirement, never endangers one.
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
// load-bearing properties — a `version` line and an `oid sha256:` key line,
// each ANYWHERE in the file — and ignores line ordering entirely, blank
// lines, per-line indentation, unknown keys, and a missing size line.
// Ordering blindness is deliberate (Codex security rounds 8-10): git-lfs
// itself decodes blank-led files, whitespace-indented version lines, and
// ext-* metadata lines preceding `version`, and each positional requirement
// this parser carried became a false negative that could retire live data —
// order-free matching closes the class instead of the instance. It never
// panics on arbitrary input.
func ParseLFSPointer(data []byte) (oid string, ok bool) {
	if len(data) > LFSPointerMaxSize {
		return "", false
	}
	// Pointer files are UTF-8 text; a NUL byte marks binary content and
	// cannot appear in any spec key or value.
	if strings.IndexByte(string(data), 0) >= 0 {
		return "", false
	}
	versioned := false
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if !versioned {
			for _, p := range lfsPointerVersionPrefixes {
				if strings.HasPrefix(line, p) {
					versioned = true
					break
				}
			}
		}
		if oid == "" {
			if rest, found := strings.CutPrefix(line, "oid sha256:"); found {
				if !validLFSOID(rest) {
					return "", false
				}
				oid = rest
			}
		}
	}
	if !versioned || oid == "" {
		return "", false
	}
	return oid, true
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
