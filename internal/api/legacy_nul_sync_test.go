package api

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// TestLegacyNULPendingSyncFinishes: SQLite stored a pending sync create for
// a repo claim with a NUL before POST /repos refused such claims.
// pull-upstream with that claim still finishes it, as
// docs/reference/authentication.md says. PostgreSQL cannot hold the row.
func TestLegacyNULPendingSyncFinishes(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		if !strings.HasSuffix(t.Name(), "/sqlite") {
			return
		}
		s := newPendingSyncStack(t, e, true)
		name := "team/a\x00b"
		repo := s.createPending(t, name, pendingSyncBody)
		if code, msg := s.pull(t, s.token(t, name, "", auth.ScopeGitWrite)); code != http.StatusAccepted {
			t.Fatalf("pull-upstream with the NUL claim = %d %q, want 202", code, msg)
		}
		got, err := e.store.GetRepoByID(context.Background(), s.org, repo.RepoID)
		if err != nil || got.CreateState != meta.CreateStateComplete {
			t.Fatalf("repo after pull-upstream = %+v, %v; want create_state complete", got, err)
		}
	})
}
