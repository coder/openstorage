// Package envconfig resolves OPENSTORAGE_* settings with one minor release of
// CODESTORAGE_* compatibility. Presence, not a nonempty value, selects the new name.
package envconfig

import (
	"os"
	"slices"
	"strings"
	"sync"
)

const (
	prefix       = "OPENSTORAGE_"
	legacyPrefix = "CODESTORAGE_"
)

// LookupEnv is os.LookupEnv with a legacy fallback for OPENSTORAGE_* names.
// Other names pass through unchanged. Lookups never log: hooks and lazy runtime
// settings must not emit a warning on every read.
func LookupEnv(name string) (string, bool) {
	if value, ok := os.LookupEnv(name); ok {
		return value, true
	}
	if strings.HasPrefix(name, prefix) {
		return os.LookupEnv(legacyName(name))
	}
	return "", false
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

var warnOnce sync.Once

// WarnDeprecated reports legacy settings once at server startup, including knobs
// first read lazily after startup. Only names are logged, never environment values.
// Do not call this in hook subprocesses.
func WarnDeprecated(warn func(string, ...any)) {
	warnOnce.Do(func() {
		seen := make(map[string]bool)
		for _, entry := range os.Environ() {
			name, _, _ := strings.Cut(entry, "=")
			suffix, legacy := strings.CutPrefix(name, legacyPrefix)
			if !legacy || seen[name] {
				continue
			}
			seen[name] = true
			if _, set := os.LookupEnv(prefix + suffix); !set {
				warn("warning: %s is deprecated; use %s instead (legacy support lasts one minor release)", name, prefix+suffix)
			}
		}
	})
}

// Environ returns a child environment with canonical settings also exported
// under their legacy names. This keeps old hook binaries in shared caches working;
// an explicitly empty canonical setting must also clear any inherited old value.
func Environ() []string {
	env := os.Environ()
	for _, entry := range slices.Clone(env) {
		name, value, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, prefix) {
			env = Set(env, name, value)
		}
	}
	return env
}

// Strip removes both spellings of each canonical name from a child environment.
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

// Set replaces both spellings with a trusted value. Exporting both lets older
// hook binaries use a new server's request context during rolling upgrades.
func Set(env []string, name, value string) []string {
	return append(Strip(env, name), name+"="+value, legacyName(name)+"="+value)
}
