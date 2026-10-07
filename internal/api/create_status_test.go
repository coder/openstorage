package api

// Issue #387: POST /repos answers a new create 201 with upstream's message,
// keeps 409 for every live taken name (the 200 repeat-create answer waits
// on issue #388), and 404 for a name a deleted repo holds.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	upsync "github.com/coder/openstorage/internal/sync"
)

func TestCreateRepoStatusCodes(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		upRoot := t.TempDir()
		forge := usyncCGIServer(t, upRoot, func(*http.Request) bool { return true }, false)
		usyncSeedUpstream(t, upRoot, "octo", "hello")
		syncBody, err := json.Marshal(map[string]any{"upstream": map[string]any{
			"provider": "github", "owner": "octo", "name": "hello", "access": "public", "host": forge.URL,
		}})
		if err != nil {
			t.Fatal(err)
		}

		post := func(t *testing.T, repoURL string, body []byte, wantStatus int) map[string]any {
			t.Helper()
			tok := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
			resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos", tok, body)
			if resp.StatusCode != wantStatus {
				t.Fatalf("POST /repos %s: status %d, want %d (body %s)", repoURL, resp.StatusCode, wantStatus, respBody)
			}
			var out map[string]any
			if err := json.Unmarshal(respBody, &out); err != nil {
				t.Fatalf("decode %q: %v", respBody, err)
			}
			return out
		}

		for _, c := range []struct {
			name string
			body []byte
		}{{"plain", nil}, {"sync", syncBody}} {
			t.Run(c.name, func(t *testing.T) {
				repoURL := "team/status-" + c.name + "-" + randomHex(t, 4)
				created := post(t, repoURL, c.body, http.StatusCreated)
				if created["message"] != "Repository '"+repoURL+"' created" || created["repo_id"] == "" {
					t.Fatalf("create response = %v", created)
				}
				if c.body != nil {
					s.waitEvent(t, upsync.EventSyncStarted)
					s.waitEvent(t, upsync.EventSyncSucceeded)
				}
				// A live name stays 409 until issue #388, even for the same request.
				if got := post(t, repoURL, c.body, http.StatusConflict); got["error"] != "repository already exists" {
					t.Fatalf("repeat create = %v", got)
				}
				if _, err := e.store.SoftDeleteRepo(context.Background(), s.org, repoURL); err != nil {
					t.Fatal(err)
				}
				got := post(t, repoURL, c.body, http.StatusNotFound)
				if want := `repository "` + repoURL + `" was deleted, and its name cannot be reused`; got["error"] != want {
					t.Fatalf("create of a deleted name = %v, want error %q", got, want)
				}
				// Nothing was restored or recreated.
				if _, err := e.store.GetRepoByURL(context.Background(), s.org, repoURL); err == nil {
					t.Fatal("deleted repo is live again")
				}
			})
		}
	})
}
