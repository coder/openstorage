package meta

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestOpenSchemeDispatch covers the factory's container-free paths: unknown
// schemes fail fast listing the supported set, and postgres:// dispatches
// into OpenPG (proved by a connect error against a dead endpoint, never an
// unsupported-scheme error).
func TestOpenSchemeDispatch(t *testing.T) {
	ctx := context.Background()

	_, err := Open(ctx, "mysql://h/db")
	if err == nil || !strings.Contains(err.Error(), `scheme "mysql" unsupported`) {
		t.Fatalf("want unsupported-scheme error, got %v", err)
	}
	for _, scheme := range []string{"postgres", "postgresql", "sqlite", "s3wal"} {
		if !strings.Contains(err.Error(), scheme) {
			t.Fatalf("unsupported-scheme error must list %q, got %v", scheme, err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = Open(ctx, "postgres://127.0.0.1:1/db?connect_timeout=1")
	if err == nil || strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("postgres:// must dispatch to OpenPG, got %v", err)
	}

	st, err := Open(ctx, "sqlite://"+filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.(*SQLite); !ok {
		t.Fatalf("sqlite:// must open the SQLite backend, got %T", st)
	}
	st.Close()

	// s3wal:// dispatches to the S3 meta backend (Dial does no I/O, so a
	// fake endpoint opens fine; the probe runs at InitSchema only).
	st, err = Open(ctx, "s3wal://bucket?endpoint=http://127.0.0.1:1&region=us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.(*S3WAL); !ok {
		t.Fatalf("s3wal:// must open the S3WAL backend, got %T", st)
	}
	st.Close()
}

func TestValidTextRejectsNULAndNonUTF8(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, s string
		want    bool
	}{
		{"empty", "", true},
		{"ASCII", "acme/alpha", true},
		{"multi-byte UTF-8", "stra\u00dfe \U0001F600", true},
		{"U+FFFD", "a\ufffdb", true},
		{"other control bytes", "a\x01\t\n\x7fb", true},
		{"NUL", "a\x00b", false},
		{"only a NUL", "\x00", false},
		{"a trailing NUL", "ab\x00", false},
		{"a lone continuation byte", "a\x80b", false},
		{"a truncated sequence", "a\xe2\x82", false},
		{"0xff", "\xff", false},
		{"an encoded surrogate", "\xed\xa0\x80", false},
		{"an overlong NUL", "\xc0\x80", false},
	} {
		if got := ValidText(tc.s); got != tc.want {
			t.Errorf("%s: ValidText(%q) = %v, want %v", tc.name, tc.s, got, tc.want)
		}
	}
}
