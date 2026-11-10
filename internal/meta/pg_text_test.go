package meta

import (
	"errors"
	"testing"
)

// TestPGRepoLookupsRefuseInvalidTextWithoutQuery pins that PostgreSQL's
// GetRepoByURL, GetRepoByID and SoftDeleteRepo answer ErrNotFound for a key
// that ValidText refuses before they touch the database: the store has no
// pool, so any database access panics.
func TestPGRepoLookupsRefuseInvalidTextWithoutQuery(t *testing.T) {
	s := &PG{}
	for _, key := range []string{"team/a\x00b", "team/a\xffb"} {
		if _, err := s.GetRepoByURL(t.Context(), "org", key); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetRepoByURL(%q): err = %v, want ErrNotFound", key, err)
		}
		if _, err := s.GetRepoByID(t.Context(), "org", key); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetRepoByID(%q): err = %v, want ErrNotFound", key, err)
		}
		if _, err := s.SoftDeleteRepo(t.Context(), "org", key); !errors.Is(err, ErrNotFound) {
			t.Errorf("SoftDeleteRepo(%q): err = %v, want ErrNotFound", key, err)
		}
	}
}
