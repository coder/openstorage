package version

import (
	"runtime"
	"strings"
	"testing"
)

// TestStringStamped pins the exact output shape the release workflow greps
// for (tag and commit must appear verbatim in `openstorage --version`).
func TestStringStamped(t *testing.T) {
	defer func(v, c, d string) { version, commit, date = v, c, d }(version, commit, date)
	version, commit, date = "v9.9.9", "abcdef123456", "2026-01-02T03:04:05Z"

	want := "openstorage v9.9.9 (commit abcdef123456, built 2026-01-02T03:04:05Z, " + runtime.Version() + ")"
	if got := String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestStringUnstamped(t *testing.T) {
	got := String()
	if !strings.HasPrefix(got, "openstorage ") {
		t.Fatalf("String() = %q, want openstorage prefix", got)
	}
	if !strings.Contains(got, runtime.Version()) {
		t.Fatalf("String() = %q, want go toolchain version %q", got, runtime.Version())
	}
}
