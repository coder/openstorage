package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// TestCreateMarksOmittedGitHubDefaultBranch: a sync create records that it
// named no default branch only for a GitHub upstream, whichever form names
// it (issue #599). Every repo starts on main.
func TestCreateMarksOmittedGitHubDefaultBranch(t *testing.T) {
	const gh = `"provider":"github","owner":"o","name":"n"`
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newPendingSyncStack(t, e, true)
		for i, tc := range []struct {
			query, body string
			want        bool
		}{
			{"", `{"upstream":{` + gh + `,"access":"public","host":"upstream.invalid"}}`, true},
			{"", `{"base_repo":{` + gh + `,"upstream_host":"upstream.invalid"}}`, true},
			{"", `{"default_branch":"main","upstream":{` + gh + `,"access":"public","host":"upstream.invalid"}}`, false},
			{"", `{"base_repo":{` + gh + `,"default_branch":"main","upstream_host":"upstream.invalid"}}`, false},
			{"?default_branch=main", `{"upstream":{` + gh + `,"access":"public","host":"upstream.invalid"}}`, false},
			{"", pendingSyncBody, false}, // gitea
		} {
			name := "flag-" + randomHex(t, 4)
			tok := s.token(t, name, "", auth.ScopeRepoWrite)
			if code, _, msg := s.do(t, "/api/v1/repos"+tc.query, tok, tc.body); code != http.StatusCreated {
				t.Fatalf("case %d: create = %d %q", i, code, msg)
			}
			repo, err := e.store.GetRepoByURL(context.Background(), s.org, name)
			if err != nil || repo.DefaultBranch != "main" {
				t.Fatalf("case %d: repo = %+v, %v; want main", i, repo, err)
			}
			up, err := e.store.(meta.EventingStore).GetRepoUpstream(context.Background(), repo.RepoID)
			if err != nil || up.DefaultBranchOmitted != tc.want {
				t.Fatalf("case %d (%s%s): upstream = %+v, %v; want DefaultBranchOmitted %v", i, tc.query, tc.body, up, err, tc.want)
			}
		}
	})
}
