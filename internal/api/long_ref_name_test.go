package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestRefWritesRefuseLongLastPart: each write path that names a new ref
// (commit writes, default_branch, notes) refuses a last part over 250 bytes
// with 400, so git can always create the ref's .lock file, and takes a
// 250-byte one.
func TestRefWritesRefuseLongLastPart(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		testRefWritesRefuseLongLastPart(t, e)
	})
}

func testRefWritesRefuseLongLastPart(t *testing.T, e *testEnv) {
	fx := wrefSeedFixture(t, e)
	long, ok250 := strings.Repeat("a", 251), strings.Repeat("b", 250)
	rw := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeRepoWrite}})
	cp := func(target string) []byte {
		return []byte(fmt.Sprintf(`{"metadata":{"target_branch":%q,"base_branch":"main","commit_message":"m","author":{"name":"D","email":"d@example.com"},"files":[{"path":"z.txt","content_id":"x","operation":"upsert"}]}}`+"\n"+`{"blob_chunk":{"content_id":"x","data":"aGkK","eof":true}}`+"\n", target))
	}
	js := func(v any) []byte { b, _ := json.Marshal(v); return b }
	for _, c := range []struct {
		name, method, path, token string
		body                      []byte
		want                      int
	}{
		{"commit-pack long", "POST", "/api/v1/repos/commit-pack", fx.token, cp(long), 400},
		{"commit-pack 250", "POST", "/api/v1/repos/commit-pack", fx.token, cp(ok250), 200},
		{"patch long", "PATCH", "/api/v1/repo", rw, js(map[string]any{"default_branch": long}), 400},
		{"patch qualified long", "PATCH", "/api/v1/repo", rw, js(map[string]any{"default_branch": "refs/heads/" + long}), 400},
		{"notes bare long", "POST", "/api/v1/repos/notes", fx.token, js(map[string]any{"object_ref": fx.c1, "note": "n", "notes_ref": long}), 400},
		{"notes qualified long", "POST", "/api/v1/repos/notes", fx.token, js(map[string]any{"object_ref": fx.c1, "note": "n", "notes_ref": "refs/notes/" + long}), 400},
		{"notes 250", "POST", "/api/v1/repos/notes", fx.token, js(map[string]any{"object_ref": fx.c1, "note": "n", "notes_ref": ok250}), 200},
	} {
		t.Run(c.name, func(t *testing.T) {
			resp, body := e.doReq(t, c.method, c.path, c.token, c.body, "")
			if resp.StatusCode != c.want {
				t.Errorf("%s %s: status %d, want %d: %.200s", c.method, c.path, resp.StatusCode, c.want, body)
			}
		})
	}
	t.Run("create long", func(t *testing.T) {
		name := "https://example.com/" + fx.org + "/zzlong"
		tok := e.mint(t, tokenSpec{iss: fx.org, repo: name, scopes: []string{auth.ScopeRepoWrite}})
		resp, body := e.doReq(t, "POST", "/api/v1/repos", tok, js(map[string]any{"default_branch": long}), "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("create: status %d, want 400: %.200s", resp.StatusCode, body)
		}
	})
}
