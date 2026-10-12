// Package envconfig resolves OPENSTORAGE_* settings. Presence, not a nonempty
// value, selects a name. The old CODESTORAGE_* names are not read: CheckLegacy
// refuses a process that still sets one without its replacement.
package envconfig

import (
	"fmt"
	"os"
	"slices"
	"strings"
)

const (
	prefix       = "OPENSTORAGE_"
	legacyPrefix = "CODESTORAGE_"
)

// LookupEnv is os.LookupEnv, kept as the injectable reader for settings.
// Lookups never log: hooks and lazy runtime settings read it on every call.
func LookupEnv(name string) (string, bool) {
	return os.LookupEnv(name)
}

// Getenv is the value-only form of LookupEnv, suitable for injected config readers.
func Getenv(name string) string {
	value, _ := LookupEnv(name)
	return value
}

// First returns the value of the first PRESENT name in an alias chain. An
// explicitly empty earlier name ends the scan instead of exposing a later alias.
func First(names ...string) string {
	for _, name := range names {
		if value, ok := LookupEnv(name); ok {
			return value
		}
	}
	return ""
}

func legacyName(name string) string {
	suffix, ok := strings.CutPrefix(name, prefix)
	if !ok {
		panic("envconfig: expected an OPENSTORAGE_* name")
	}
	return legacyPrefix + suffix
}

// CheckLegacy fails when a CODESTORAGE_* variable is set and its OPENSTORAGE_*
// replacement is unset. The error names every such pair, sorted, and never
// includes a value. A legacy name next to its set replacement is ignored, so a
// fleet that still runs pre-0.15 binaries can keep both names. Do not call this
// in hook subprocesses: git relays hook stderr to the pushing client.
func CheckLegacy() error {
	var pairs []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		suffix, legacy := strings.CutPrefix(name, legacyPrefix)
		if !legacy {
			continue
		}
		if _, set := os.LookupEnv(prefix + suffix); !set {
			pairs = append(pairs, name+" -> "+prefix+suffix)
		}
	}
	if len(pairs) == 0 {
		return nil
	}
	slices.Sort(pairs)
	pairs = slices.Compact(pairs)
	return fmt.Errorf("CODESTORAGE_* variables are no longer read; rename them (see docs/how-to/migrate-to-openstorage.md): %s",
		strings.Join(pairs, ", "))
}

// Strip removes both spellings of each canonical name from a child environment,
// so no inherited value under either prefix reaches the child.
// It reuses env's backing array; callers must own that slice.
func Strip(env []string, names ...string) []string {
	return slices.DeleteFunc(env, func(entry string) bool {
		name, _, _ := strings.Cut(entry, "=")
		for _, canonical := range names {
			if name == canonical || name == legacyName(canonical) {
				return true
			}
		}
		return false
	})
}

// Set replaces both spellings with one trusted canonical value. Children read
// only OPENSTORAGE_* names: every hook script runs this binary (EnsureCacheRepo
// rewrites managed hooks before each request), and 0.15.x and 0.16.x hook
// binaries also read the canonical name first.
func Set(env []string, name, value string) []string {
	return append(Strip(env, name), name+"="+value)
}
