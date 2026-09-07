// Package version reports the build version of the openstorage binary.
//
// Release builds stamp the package variables below via -ldflags -X (see
// scripts/release-archives.sh and .github/workflows/release.yml). Unstamped
// builds (plain `go build`, `go install module@version`) fall back to Go
// module build info where available.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
)

// Stamped by the release build. Keep the defaults in sync with String's
// fallback logic: "devel" marks an unstamped binary.
var (
	version = "devel"
	commit  = ""
	date    = ""
)

// String returns a single-line human-readable description like
//
//	openstorage v0.1.0 (commit 4594422de478…, built 2026-08-23T00:00:00Z, go1.26.0)
//
// omitting the commit/built fields when neither the linker nor the build
// info supplies them.
func String() string {
	v, c, d := version, commit, date
	if bi, ok := debug.ReadBuildInfo(); ok {
		if v == "devel" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			v = bi.Main.Version
		}
		for _, s := range bi.Settings {
			switch {
			case s.Key == "vcs.revision" && c == "":
				c = s.Value
			case s.Key == "vcs.time" && d == "":
				d = s.Value
			}
		}
	}
	details := make([]string, 0, 3)
	if c != "" {
		details = append(details, "commit "+c)
	}
	if d != "" {
		details = append(details, "built "+d)
	}
	details = append(details, runtime.Version())
	return fmt.Sprintf("openstorage %s (%s)", v, strings.Join(details, ", "))
}
