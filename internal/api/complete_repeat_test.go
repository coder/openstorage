package api

// Issue #388, PR 4: a repeat create of a finished repo without an upstream
// link (plain, copy or fork) answers 200 and changes nothing.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/meta"
)

// TestCreateRepoRepeatAnswers is T5 in the #388 plan: every create form
// against every create state of the row that holds the name. Each cell
// seeds its own name. A sync form uses a git:write token, so it may finish
// a pending sync create.
func TestCreateRepoRepeatAnswers(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newPendingForkStack(t, e, e.blobStore, e.store)
		ctx := context.Background()
		syncOther := strings.Replace(pendingSyncBody, `"name":"n"`, `"name":"other"`, 1)
		seeds := []struct {
			row  string
			seed func(t *testing.T, name string)
		}{
			{"deleted", func(t *testing.T, name string) {
				if _, err := e.store.CreateRepoWithState(ctx, s.org, name, "main", meta.CreateStateComplete); err != nil {
					t.Fatal(err)
				}
				if _, err := e.store.SoftDeleteRepo(ctx, s.org, name); err != nil {
					t.Fatal(err)
				}
			}},
			{"legacy", func(t *testing.T, name string) {
				if _, err := e.store.CreateRepo(ctx, s.org, name, "main"); err != nil {
					t.Fatal(err)
				}
			}},
			{"pending sync", func(t *testing.T, name string) { s.createPending(t, name, pendingSyncBody) }},
			{"pending fork", func(t *testing.T, name string) {
				if _, err := e.store.CreateRepoWithState(ctx, s.org, name, "main", meta.CreateStatePending); err != nil {
					t.Fatal(err)
				}
			}},
			{"complete unlinked", func(t *testing.T, name string) {
				if code, _, msg := s.create(t, name, ""); code != http.StatusCreated {
					t.Fatalf("seed plain create = %d %q", code, msg)
				}
			}},
			{"complete sync", func(t *testing.T, name string) {
				if code, _, msg := s.create(t, name, pendingSyncBody); code != http.StatusCreated {
					t.Fatalf("seed sync create = %d %q", code, msg)
				}
			}},
		}
		forms := []struct{ name, body string }{
			{"plain", ""}, {"sync same", pendingSyncBody}, {"sync other", syncOther}, {"copy", s.copy}, {"fork", s.fork},
		}
		// want[row][form], in the order of seeds and forms.
		want := map[string][5]int{
			"deleted":           {404, 404, 404, 404, 404},
			"legacy":            {409, 409, 409, 409, 409},
			"pending sync":      {409, 200, 409, 409, 409},
			"pending fork":      {503, 409, 409, 503, 503},
			"complete unlinked": {200, 409, 409, 200, 200},
			"complete sync":     {409, 200, 409, 409, 409},
		}
		for i, sd := range seeds {
			for j, f := range forms {
				name := "cell-" + string(rune('a'+i)) + string(rune('a'+j))
				sd.seed(t, name)
				before, err := e.store.GetRepoByURL(ctx, s.org, name)
				live := err == nil
				code, msg := s.retry(t, name, f.body)
				if code != want[sd.row][j] {
					t.Fatalf("%s row, %s create = %d %q, want %d", sd.row, f.name, code, msg, want[sd.row][j])
				}
				if code == http.StatusOK {
					var got createRepoResponse
					if err := json.Unmarshal([]byte(msg), &got); err != nil || !live || got.RepoID != before.RepoID ||
						got.Message != "Repository '"+name+"' already exists" {
						t.Fatalf("%s row, %s create 200 body = %q, %v; want repo %s", sd.row, f.name, msg, err, before.RepoID)
					}
				}
			}
		}
	})
}

// TestCopyRepeatSkipsSource is T6 in the #388 plan: a repeat copy of a
// finished copy answers 200 before any source work, so it succeeds after
// its source is deleted and with a token that no longer verifies. A bad
// request still answers 400.
func TestCopyRepeatSkipsSource(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newPendingForkStack(t, e, e.blobStore, e.store)
		code, _, raw := s.create(t, "copied", s.copy)
		var created createRepoResponse
		if err := json.Unmarshal([]byte(raw), &created); code != http.StatusCreated || err != nil {
			t.Fatalf("copy = %d %q, %v; want 201", code, raw, err)
		}
		if _, err := e.store.SoftDeleteRepo(context.Background(), s.src.org, s.src.url); err != nil {
			t.Fatal(err)
		}
		bad := `{"base_repo":{"name":"` + s.src.url + `","auth":{"token":"not-a-jwt"}}}`
		for _, body := range []string{s.copy, bad} {
			code, _, raw := s.create(t, "copied", body)
			var got createRepoResponse
			if err := json.Unmarshal([]byte(raw), &got); code != http.StatusOK || err != nil || got.RepoID != created.RepoID {
				t.Fatalf("repeat copy %q = %d %q, %v; want 200 for repo %s", body, code, raw, err, created.RepoID)
			}
		}
		// The source is gone, so a new copy fails at the source (404).
		if code, _, msg := s.create(t, "copied-again", s.copy); code != http.StatusNotFound {
			t.Fatalf("new copy of a deleted source = %d %q, want 404", code, msg)
		}
		// Request validation still runs first.
		if code, _, msg := s.create(t, "copied", `{"base_repo":{"auth":{"token":"x"}}}`); code != http.StatusBadRequest {
			t.Fatalf("copy without a name = %d %q, want 400", code, msg)
		}
		if _, err := e.store.GetRepoByURL(context.Background(), s.org, "copied-again"); !errors.Is(err, meta.ErrNotFound) {
			t.Fatalf("a failed copy left a repo row: %v", err)
		}
	})
}
