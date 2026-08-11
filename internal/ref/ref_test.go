package ref

import (
	"context"
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
	for _, scheme := range []string{"postgres", "postgresql", "sqlite"} {
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
}
