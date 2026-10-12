package envconfig

import (
	"os"
	"slices"
	"strings"
	"testing"
)

func unset(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "") // register restoration before unsetting
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
}

func TestLookupEnvIgnoresLegacyPrefix(t *testing.T) {
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
		{name: "legacy only", old: new("old")},
		{name: "both", canonical: new("new"), old: new("old"), want: "new", set: true},
		{name: "canonical empty", canonical: new(""), old: new("old"), set: true},
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
}

func TestFirstStopsAtPresentName(t *testing.T) {
	const canonical, legacy, alias = "OPENSTORAGE_ENVCONFIG_FIRST", "CODESTORAGE_ENVCONFIG_FIRST", "ENVCONFIG_FIRST_ALIAS"
	for _, name := range []string{canonical, legacy, alias} {
		unset(t, name)
	}
	t.Setenv(alias, "alias")
	t.Setenv(legacy, "old")
	if got := First(canonical, alias); got != "alias" {
		t.Fatalf("unset canonical must fall through to the alias, not the legacy prefix, got %q", got)
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

func TestCheckLegacy(t *testing.T) {
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, legacyPrefix) {
			unset(t, name)
		}
	}
	if err := CheckLegacy(); err != nil {
		t.Fatalf("no legacy names: %v", err)
	}
	for _, suffix := range []string{"SECRETS_KEY", "S3_OP_LOG", "LISTEN", "ORG", "PG_URL"} {
		unset(t, prefix+suffix)
		t.Setenv(legacyPrefix+suffix, "secret-value-must-not-appear")
	}
	t.Setenv(legacyPrefix+"PG_URL", "") // an empty legacy value is still refused
	t.Setenv("OPENSTORAGE_LISTEN", "new-value-must-not-appear")
	t.Setenv("OPENSTORAGE_ORG", "") // a set canonical name shadows the legacy one
	err := CheckLegacy()
	if err == nil {
		t.Fatal("legacy-only names must be refused")
	}
	got := err.Error()
	want := "CODESTORAGE_PG_URL -> OPENSTORAGE_PG_URL, CODESTORAGE_S3_OP_LOG -> OPENSTORAGE_S3_OP_LOG, " +
		"CODESTORAGE_SECRETS_KEY -> OPENSTORAGE_SECRETS_KEY"
	if !strings.HasSuffix(got, ": "+want) || strings.Contains(got, "must-not-appear") ||
		strings.Contains(got, "LISTEN") || strings.Contains(got, "_ORG") {
		t.Fatalf("CheckLegacy = %q; want sorted pairs %q and no values or shadowed names", got, want)
	}
}

func TestChildEnvironment(t *testing.T) {
	const name = "OPENSTORAGE_ENVCONFIG_TEST"
	const old = "CODESTORAGE_ENVCONFIG_TEST"
	env := []string{"OTHER=keep", name + "=stale", old + "=older", name + "=duplicate", old + "=duplicate"}
	got := Set(slices.Clone(env), name, "trusted=secret")
	if want := []string{"OTHER=keep", name + "=trusted=secret"}; !slices.Equal(got, want) {
		t.Fatalf("Set = %q; want %q", got, want)
	}
	if got := Strip(env, name); !slices.Equal(got, []string{"OTHER=keep"}) {
		t.Fatalf("Strip = %q", got)
	}
}
