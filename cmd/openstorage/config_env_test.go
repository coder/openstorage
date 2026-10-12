package main

import (
	"os"
	"testing"

	"github.com/coder/openstorage/internal/envconfig"
)

// Use the presence-aware production reader here, not mapEnv: a value-only mock
// cannot distinguish an explicitly empty canonical setting from an unset one.
func TestResolvePackQuotaEnvCompatibility(t *testing.T) {
	for _, tt := range []struct {
		name              string
		canonical, legacy *string
		want              int64
	}{
		{name: "canonical", canonical: new("123"), want: 123},
		{name: "legacy ignored", legacy: new("456"), want: defaultPackQuotaBytes},
		{name: "both", canonical: new("123"), legacy: new("456"), want: 123},
		{name: "canonical empty", canonical: new(""), legacy: new("456"), want: defaultPackQuotaBytes},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, name := range []string{envPackQuota, "CODESTORAGE_PACK_QUOTA_BYTES"} {
				t.Setenv(name, "")
				if err := os.Unsetenv(name); err != nil {
					t.Fatal(err)
				}
			}
			if tt.canonical != nil {
				t.Setenv(envPackQuota, *tt.canonical)
			}
			if tt.legacy != nil {
				t.Setenv("CODESTORAGE_PACK_QUOTA_BYTES", *tt.legacy)
			}
			got, err := resolvePackQuota(envconfig.Getenv)
			if err != nil || got != tt.want {
				t.Fatalf("resolvePackQuota = %d, %v; want %d", got, err, tt.want)
			}
		})
	}
}
