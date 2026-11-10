package meta

import (
	"errors"
	"testing"
)

// TestPGRepoLookupsRefuseInvalidTextWithoutQuery pins that PostgreSQL's
// GetRepoByURL and GetRepoByID answer ErrNotFound for a key that ValidText
// refuses before they touch the database: on a closed store any query
// fails with another error.
func TestPGRepoLookupsRefuseInvalidTextWithoutQuery(t *testing.T) {
	pgBackend(t)
	s, err := OpenPG(t.Context(), pgTestURL)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	s.Close()
	for _, key := range []string{"team/a\x00b", "team/a\xffb"} {
		if _, err := s.GetRepoByURL(t.Context(), "org", key); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetRepoByURL(%q) on a closed store: err = %v, want ErrNotFound", key, err)
		}
		if _, err := s.GetRepoByID(t.Context(), "org", key); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetRepoByID(%q) on a closed store: err = %v, want ErrNotFound", key, err)
		}
	}
	if _, err := s.GetRepoByURL(t.Context(), "org", "team/ok"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRepoByURL(valid) on a closed store: err = %v, want a database error", err)
	}
}
