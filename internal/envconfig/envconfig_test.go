package envconfig

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
)

func unset(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "") // register restoration before unsetting
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
}

func TestLookupEnv(t *testing.T) {
	const name = "OPENSTORAGE_ENVCONFIG_TEST"
	const legacy = "CODESTORAGE_ENVCONFIG_TEST"
	for _, tt := range []struct {
		name           string
		canonical, old *string
		want           string
		set            bool
	}{
		{name: "unset"},
		{name: "canonical", canonical: new("new"), want: "new", set: true},
		{name: "legacy", old: new("old"), want: "old", set: true},
		{name: "both", canonical: new("new"), old: new("old"), want: "new", set: true},
		{name: "canonical empty", canonical: new(""), old: new("old"), set: true},
		{name: "legacy empty", old: new(""), set: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unset(t, name)
			unset(t, legacy)
			if tt.canonical != nil {
				t.Setenv(name, *tt.canonical)
			}
			if tt.old != nil {
				t.Setenv(legacy, *tt.old)
			}
			if got, ok := LookupEnv(name); got != tt.want || ok != tt.set {
				t.Fatalf("LookupEnv = %q, %v; want %q, %v", got, ok, tt.want, tt.set)
			}
			if got := Getenv(name); got != tt.want {
				t.Fatalf("Getenv = %q; want %q", got, tt.want)
			}
		})
	}
	t.Setenv("ENVCONFIG_OTHER", "unchanged")
	if value, ok := LookupEnv("ENVCONFIG_OTHER"); !ok || value != "unchanged" {
		t.Fatalf("non-product lookup = %q, %v", value, ok)
	}
}

func TestFirstStopsAtPresentName(t *testing.T) {
	const canonical, legacy, alias = "OPENSTORAGE_ENVCONFIG_FIRST", "CODESTORAGE_ENVCONFIG_FIRST", "ENVCONFIG_FIRST_ALIAS"
	for _, name := range []string{canonical, legacy, alias} {
		unset(t, name)
	}
	t.Setenv(alias, "alias")
	if got := First(canonical, alias); got != "alias" {
		t.Fatalf("unset canonical must fall through, got %q", got)
	}
	t.Setenv(legacy, "old")
	if got := First(canonical, alias); got != "old" {
		t.Fatalf("legacy prefix precedes the older alias, got %q", got)
	}
	t.Setenv(canonical, "")
	if got := First(canonical, alias); got != "" {
		t.Fatalf("explicitly empty canonical must stop the scan, got %q", got)
	}
	t.Setenv(canonical, "new")
	if got := First(canonical, alias); got != "new" {
		t.Fatalf("canonical wins, got %q", got)
	}
}

func TestWarnDeprecated(t *testing.T) {
	// Each test invocation represents a fresh boot, including -count runs.
	warnOnce = sync.Once{}
	t.Cleanup(func() { warnOnce = sync.Once{} })
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, legacyPrefix) {
			unset(t, name)
		}
	}
	for _, suffix := range []string{"SECRETS_KEY", "S3_OP_LOG", "LISTEN", "ORG"} {
		unset(t, prefix+suffix)
		t.Setenv(legacyPrefix+suffix, "secret-value-must-not-appear")
	}
	t.Setenv("OPENSTORAGE_LISTEN", "new-value-must-not-appear")
	t.Setenv("OPENSTORAGE_ORG", "")
	var output strings.Builder
	warn := func(format string, args ...any) { fmt.Fprintf(&output, format+"\n", args...) }
	WarnDeprecated(warn)
	WarnDeprecated(warn)
	// A lazy setting is warned at boot, even before its first read.
	Getenv("OPENSTORAGE_S3_OP_LOG")
	Getenv("OPENSTORAGE_S3_OP_LOG")
	got := output.String()
	for _, suffix := range []string{"SECRETS_KEY", "S3_OP_LOG"} {
		if strings.Count(got, legacyPrefix+suffix) != 1 || strings.Count(got, prefix+suffix) != 1 {
			t.Fatalf("missing or repeated replacement warning: %s", got)
		}
	}
	if strings.Count(got, "\n") != 2 || strings.Contains(got, "must-not-appear") {
		t.Fatalf("unexpected warning or leaked value: %s", got)
	}
}

func TestChildEnvironment(t *testing.T) {
	const name = "OPENSTORAGE_ENVCONFIG_TEST"
	const old = "CODESTORAGE_ENVCONFIG_TEST"
	env := []string{"OTHER=keep", name + "=stale", old + "=older", name + "=duplicate", old + "=duplicate"}
	got := Set(slices.Clone(env), name, "trusted=secret")
	want := []string{"OTHER=keep", name + "=trusted=secret", old + "=trusted=secret"}
	if !slices.Equal(got, want) {
		t.Fatalf("Set = %q; want %q", got, want)
	}
	if got := Strip(env, name); !slices.Equal(got, []string{"OTHER=keep"}) {
		t.Fatalf("Strip = %q", got)
	}
	for _, value := range []string{"new", ""} {
		t.Setenv(name, value)
		t.Setenv(old, "stale")
		got := Environ()
		if !slices.Contains(got, name+"="+value) || !slices.Contains(got, old+"="+value) || slices.Contains(got, old+"=stale") {
			t.Fatal("child environment did not override the legacy alias")
		}
	}
}
