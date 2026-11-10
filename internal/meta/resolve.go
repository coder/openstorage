package meta

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ResolveRepoKey resolves key — a git URL path element (docs/explanation/git-protocol-and-namespaces.md, after the
// caller strips any `+ephemeral`/`+import` namespace suffix) or a JWT repo
// claim (docs/reference/authentication.md, url or repo_id) — to its live repo row. RepoID takes
// precedence (docs/explanation/git-protocol-and-namespaces.md addresses repos by repo_id; ADR-21), then url, so
// name-style URLs work too. Unknown or soft-deleted keys yield a wrapped
// ErrNotFound.
func ResolveRepoKey(ctx context.Context, store Store, org, key string) (Repo, error) {
	assertf(store != nil, "ResolveRepoKey: nil store")
	assertf(org != "", "ResolveRepoKey: empty org")
	if key == "" {
		return Repo{}, fmt.Errorf("resolve repo key: empty key: %w", ErrNotFound)
	}
	// No repository has such a key: a url comes from a JWT repo claim,
	// which JSON makes valid UTF-8 and the verifier refuses with a NUL.
	// PostgreSQL refuses such text (SQLSTATE 22021), so asking it would
	// turn a path name into a 500.
	if strings.ContainsRune(key, 0) || !utf8.ValidString(key) {
		return Repo{}, fmt.Errorf("resolve repo key %q: not UTF-8 text without NUL: %w", key, ErrNotFound)
	}
	repo, err := store.GetRepoByID(ctx, org, key)
	if err == nil {
		return repo, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Repo{}, fmt.Errorf("resolve repo key %q by id: %w", key, err)
	}
	repo, err = store.GetRepoByURL(ctx, org, key)
	if err == nil {
		return repo, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Repo{}, fmt.Errorf("resolve repo key %q by url: %w", key, err)
	}
	return Repo{}, fmt.Errorf("repo %q: %w", key, ErrNotFound)
}
