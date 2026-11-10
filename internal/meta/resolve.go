package meta

import (
	"context"
	"errors"
	"fmt"
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
