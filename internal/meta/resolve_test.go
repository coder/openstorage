package meta

import (
	"errors"
	"testing"
)

// TestResolveRepoKey pins the ADR-21 path-element/claim resolution order:
// RepoID first, then url; unknown and soft-deleted keys are ErrNotFound.
func TestResolveRepoKey(t *testing.T) { runBackends(t, testResolveRepoKey) }

func testResolveRepoKey(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)

	created, err := s.CreateRepo(ctx, org, "team/project-alpha", "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	byID, err := ResolveRepoKey(ctx, s, org, created.RepoID)
	if err != nil {
		t.Fatalf("resolve by id: %v", err)
	}
	if byID.RepoID != created.RepoID || byID.URL != created.URL {
		t.Fatalf("resolve by id = %+v, want %+v", byID, created)
	}

	byURL, err := ResolveRepoKey(ctx, s, org, created.URL)
	if err != nil {
		t.Fatalf("resolve by url: %v", err)
	}
	if byURL.RepoID != created.RepoID {
		t.Fatalf("resolve by url RepoID = %q, want %q", byURL.RepoID, created.RepoID)
	}

	// Valid text with control characters or non-ASCII resolves as itself.
	for _, url := range []string{"team/a\x01\x7fb", "team/café-\uFFFD"} {
		odd, err := s.CreateRepo(ctx, org, url, "main")
		if err != nil {
			t.Fatalf("create repo %q: %v", url, err)
		}
		got, err := ResolveRepoKey(ctx, s, org, url)
		if err != nil || got.RepoID != odd.RepoID {
			t.Fatalf("resolve %q = %+v, %v, want %q", url, got, err, odd.RepoID)
		}
	}

	for _, key := range []string{"", "no-such-repo", "repo_ffffffffffffffff", "team/a\x00b", "team/a\xffb"} {
		if _, err := ResolveRepoKey(ctx, s, org, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("resolve %q: err = %v, want ErrNotFound", key, err)
		}
	}

	// Soft-deleted repos resolve by neither key.
	if _, err := s.SoftDeleteRepo(ctx, org, created.URL); err != nil {
		t.Fatalf("soft-delete: %v", err)
	}
	for _, key := range []string{created.RepoID, created.URL} {
		if _, err := ResolveRepoKey(ctx, s, org, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("resolve deleted %q: err = %v, want ErrNotFound", key, err)
		}
	}
}
