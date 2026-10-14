package api

// Integration tests for the path-style operations of the 2026-10-05
// upstream spec (issue #378): POST .../notes/append, POST .../upstream/pull
// and DELETE .../upstream, on the pg and sqlite lanes. The deprecated
// routes they replace stay covered by write_refs_test.go and sync_test.go.

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/ref"
	upsync "github.com/coder/openstorage/internal/sync"
)

func TestNotesAppendRoute(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		path := "/api/repos/" + url.PathEscape(fx.url) + "/notes/append"
		post := func(t *testing.T, token string, body any) (*http.Response, map[string]any) {
			t.Helper()
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			resp, respBody := e.doReq(t, http.MethodPost, path, token, raw, "")
			out := map[string]any{}
			if err := json.Unmarshal(respBody, &out); err != nil {
				t.Fatalf("decode %q: %v", respBody, err)
			}
			return resp, out
		}
		noteOf := func(t *testing.T, sha string) string {
			t.Helper()
			resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/notes?object_ref="+sha, fx.token, nil, "")
			if resp.StatusCode != http.StatusOK {
				return ""
			}
			var out struct {
				Note string `json:"note"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatal(err)
			}
			return out.Note
		}

		t.Run("creates then appends with 201", func(t *testing.T) {
			resp, body := post(t, fx.token, map[string]any{"object_ref": fx.c1, "note": "first"})
			if resp.StatusCode != http.StatusCreated || wrefNoteResultOf(t, body)["success"] != true {
				t.Fatalf("create via append: status %d body %v", resp.StatusCode, body)
			}
			resp, body = post(t, fx.token, map[string]any{"object_ref": fx.c1, "note": "second"})
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("append: status %d body %v", resp.StatusCode, body)
			}
			if got := noteOf(t, fx.c1); !strings.Contains(got, "first") || !strings.Contains(got, "second") {
				t.Fatalf("note = %q, want both lines", got)
			}
		})

		t.Run("stale guard is 409 and appends nothing", func(t *testing.T) {
			tip := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]
			resp, body := post(t, fx.token, map[string]any{"object_ref": fx.c1, "note": "retry",
				"expected_notes_ref_sha": ref.ZeroSHA})
			if resp.StatusCode != http.StatusConflict || wrefNoteResultOf(t, body)["status"] != "conflict" {
				t.Fatalf("status %d body %v, want 409 conflict", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]; got != tip {
				t.Fatalf("notes ref moved %s -> %s", tip, got)
			}
			resp, body = post(t, fx.token, map[string]any{"object_ref": fx.c1, "note": "guarded",
				"expected_notes_ref_sha": tip})
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("matching guard: status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("strict schema: action and unknown fields are 400", func(t *testing.T) {
			for name, body := range map[string]map[string]any{
				"action add":     {"object_ref": fx.c2, "note": "x", "action": "add"},
				"unknown field":  {"object_ref": fx.c2, "note": "x", "bogus": true},
				"missing note":   {"object_ref": fx.c2},
				"null author":    {"object_ref": fx.c2, "note": "x", "author": nil},
				"null notes_ref": {"object_ref": fx.c2, "note": "x", "notes_ref": nil},
				"null author name": {"object_ref": fx.c2, "note": "x",
					"author": map[string]any{"name": nil, "email": "a@x"}},
				"case-variant key": {"Object_Ref": fx.c2, "note": "x"},
				"case-variant nested": {"object_ref": fx.c2, "note": "x",
					"author": map[string]any{"Name": "a", "email": "a@x"}},
			} {
				if resp, out := post(t, fx.token, body); resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status %d body %v, want 400", name, resp.StatusCode, out)
				}
			}
			// A duplicate member name (issue #381): encoding/json alone
			// would let the last one win.
			dup := json.RawMessage(`{"object_ref":"` + fx.c2 + `","note":"x","note":"y"}`)
			if resp, out := post(t, fx.token, dup); resp.StatusCode != http.StatusBadRequest {
				t.Errorf("duplicate note: status %d body %v, want 400", resp.StatusCode, out)
			}
			if got := noteOf(t, fx.c2); got != "" {
				t.Fatalf("rejected requests wrote a note on c2: %q", got)
			}
		})

		t.Run("legacy POST notes keeps 200 and 412", func(t *testing.T) {
			raw := []byte(`{"sha":"` + fx.c2 + `","action":"append","note":"x","expected_ref_sha":"` + ref.ZeroSHA + `"}`)
			resp, _ := e.doReq(t, http.MethodPost, "/api/repos/"+url.PathEscape(fx.url)+"/notes", fx.token, raw, "")
			if resp.StatusCode != http.StatusPreconditionFailed {
				t.Fatalf("legacy stale guard = %d, want 412", resp.StatusCode)
			}
			raw = []byte(`{"sha":"` + fx.c2 + `","action":"append","note":"legacy"}`)
			resp, _ = e.doReq(t, http.MethodPost, "/api/v1/repos/notes", fx.token, raw, "")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("legacy append = %d, want 200", resp.StatusCode)
			}
		})

		t.Run("auth: scope, foreign repo, foreign org", func(t *testing.T) {
			body := map[string]any{"object_ref": fx.c2, "note": "intruder"}
			readOnly := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitRead}})
			if resp, out := post(t, readOnly, body); resp.StatusCode != http.StatusForbidden {
				t.Errorf("git:read only = %d %v, want 403", resp.StatusCode, out)
			}
			if resp, out := post(t, "", body); resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("no token = %d %v, want 401", resp.StatusCode, out)
			}
			other := wrefSeedFixture(t, e) // another org, another repo
			if resp, out := post(t, other.token, body); resp.StatusCode != http.StatusNotFound {
				t.Errorf("foreign org token = %d %v, want 404", resp.StatusCode, out)
			}
			sameOrgURL := "https://example.com/" + fx.org + "/sibling-" + randomHex(t, 4)
			if _, err := e.store.CreateRepo(t.Context(), fx.org, sameOrgURL, "main"); err != nil {
				t.Fatal(err)
			}
			sibling := e.mint(t, tokenSpec{iss: fx.org, repo: sameOrgURL, scopes: []string{auth.ScopeGitWrite}})
			if resp, out := post(t, sibling, body); resp.StatusCode != http.StatusNotFound {
				t.Errorf("sibling repo token = %d %v, want 404", resp.StatusCode, out)
			}
			if got := noteOf(t, fx.c2); strings.Contains(got, "intruder") {
				t.Fatalf("unauthorized append landed: %q", got)
			}
		})
	})
}

func TestUpstreamPathRoutes(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		upRoot := t.TempDir()
		const upstreamPass = "up-secret"
		upstream := usyncCGIServer(t, upRoot, func(r *http.Request) bool {
			user, pass, ok := r.BasicAuth()
			// provider gitea, no stored username: the token is the
			// username, with an empty password (issue #400).
			return ok && basicAuthEquals(user, pass, upstreamPass, "")
		}, false)
		usyncSeedUpstream(t, upRoot, "own", "proj")
		repoURL := "team/path-" + randomHex(t, 4)
		body, err := json.Marshal(map[string]any{"base_repo": map[string]any{
			"provider": "gitea", "owner": "own", "name": "proj",
			"upstream_host": upstream.URL, "auth": map[string]string{"token": upstreamPass},
		}})
		if err != nil {
			t.Fatal(err)
		}
		repoID := s.createRepo(t, repoURL, body)
		s.waitEvent(t, upsync.EventSyncStarted)
		s.waitEvent(t, upsync.EventSyncSucceeded)
		base := "/api/repos/" + url.PathEscape(repoURL)

		writeTok := e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeGitWrite}})
		repoTok := e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})

		t.Run("upstream/pull matches pull-upstream", func(t *testing.T) {
			newHead := usyncAddUpstreamCommit(t, upRoot, "own", "proj")
			resp, respBody := s.req(t, http.MethodPost, base+"/upstream/pull", writeTok, []byte(`{"ref":"main"}`))
			if resp.StatusCode != http.StatusAccepted || !strings.Contains(string(respBody), "Repository sync initiated successfully") {
				t.Fatalf("status %d body %s", resp.StatusCode, respBody)
			}
			s.waitEvent(t, upsync.EventSyncStarted)
			s.waitEvent(t, upsync.EventSyncSucceeded)
			if got := s.loopbackSHA(t, repoID, "refs/heads/main"); got != newHead {
				t.Fatalf("loopback main = %q, want %q", got, newHead)
			}
			// No body works too (the body is optional upstream).
			resp, respBody = s.req(t, http.MethodPost, base+"/upstream/pull", writeTok, nil)
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("no body: status %d body %s", resp.StatusCode, respBody)
			}
			s.waitEvent(t, upsync.EventSyncStarted)
			s.waitEvent(t, upsync.EventSyncSucceeded)
		})

		t.Run("upstream/pull decodes its body strictly", func(t *testing.T) {
			for name, b := range map[string]string{
				"unknown field": `{"refs":"feature"}`,
				"null ref":      `{"ref":null}`,
				"trailing data": `{"ref":"main"} {}`,
				"null body":     `null`,
				"array body":    `[]`,
				"case-variant":  `{"Ref":"feature"}`,
			} {
				if resp, rb := s.req(t, http.MethodPost, base+"/upstream/pull", writeTok, []byte(b)); resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status %d %s, want 400", name, resp.StatusCode, rb)
				}
			}
			s.assertNoEvent(t)
			// The deprecated route keeps its lenient decode.
			resp, rb := s.req(t, http.MethodPost, base+"/pull-upstream", writeTok, []byte(`{"ref":"main","extra":1}`))
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("legacy lenient pull = %d %s, want 202", resp.StatusCode, rb)
			}
			s.waitEvent(t, upsync.EventSyncStarted)
			s.waitEvent(t, upsync.EventSyncSucceeded)
		})

		t.Run("upstream/pull auth and policy", func(t *testing.T) {
			for name, tok := range map[string]string{
				"repo:write only": repoTok,
				"policed":         e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeGitWrite}, refs: `[["main", ["no-push"]]]`}),
			} {
				if resp, b := s.req(t, http.MethodPost, base+"/upstream/pull", tok, nil); resp.StatusCode != http.StatusForbidden {
					t.Errorf("%s = %d %s, want 403", name, resp.StatusCode, b)
				}
			}
			otherURL := "team/other-" + randomHex(t, 4)
			s.createRepo(t, otherURL, nil)
			foreign := e.mint(t, tokenSpec{iss: s.org, repo: otherURL, scopes: []string{auth.ScopeGitWrite, auth.ScopeRepoWrite}})
			if resp, b := s.req(t, http.MethodPost, base+"/upstream/pull", foreign, nil); resp.StatusCode != http.StatusNotFound {
				t.Errorf("sibling repo token pull = %d %s, want 404", resp.StatusCode, b)
			}
			if resp, b := s.req(t, http.MethodDelete, base+"/upstream", foreign, nil); resp.StatusCode != http.StatusNotFound {
				t.Errorf("sibling repo token detach = %d %s, want 404", resp.StatusCode, b)
			}
			otherOrg := e.newOrg(t)
			alien := e.mint(t, tokenSpec{iss: otherOrg, repo: repoURL, scopes: []string{auth.ScopeGitWrite, auth.ScopeRepoWrite}})
			if resp, b := s.req(t, http.MethodDelete, base+"/upstream", alien, nil); resp.StatusCode < 400 {
				t.Errorf("foreign org detach = %d %s, want a 4xx", resp.StatusCode, b)
			}
			s.assertNoEvent(t)
			// The rejected detaches left the link in place.
			if resp, b := s.req(t, http.MethodPost, base+"/upstream/pull", writeTok, nil); resp.StatusCode != http.StatusAccepted {
				t.Fatalf("pull after rejected detaches = %d %s, want 202", resp.StatusCode, b)
			}
			s.waitEvent(t, upsync.EventSyncStarted)
			s.waitEvent(t, upsync.EventSyncSucceeded)
		})

		t.Run("DELETE upstream detaches idempotently", func(t *testing.T) {
			if resp, b := s.req(t, http.MethodDelete, base+"/upstream", writeTok, nil); resp.StatusCode != http.StatusForbidden {
				t.Fatalf("git:write only detach = %d %s, want 403", resp.StatusCode, b)
			}
			// The first detach removes the link; the repeat finds none
			// and names it (changelog 2026-10-07, issue #459).
			for _, want := range []string{"repository detached", "repository already detached"} {
				resp, respBody := s.req(t, http.MethodDelete, base+"/upstream", repoTok, nil)
				var got map[string]string
				if err := json.Unmarshal(respBody, &got); err != nil {
					t.Fatalf("decode %s: %v", respBody, err)
				}
				if resp.StatusCode != http.StatusOK || len(got) != 1 || got["message"] != want {
					t.Fatalf("detach: status %d body %s, want 200 {\"message\": %q}", resp.StatusCode, respBody, want)
				}
			}
			resp, respBody := s.req(t, http.MethodPost, base+"/upstream/pull", writeTok, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("pull after detach = %d %s, want 400", resp.StatusCode, respBody)
			}
			s.assertNoEvent(t)
		})
	})
}

// TestDetachMessages pins the detach text of all three detach routes (issue
// #459): only DELETE .../upstream names a repeat detach. The deprecated base
// routes answer "repository detached" in both cases.
func TestDetachMessages(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		verifier := auth.NewVerifier(e.store)
		mem := upsync.NewMemStore()
		ss := newSyncServer(e.store, mem, mem, envSyncQueue{}, testHTTPBaseURL, events.TargetGuard{}, discardLogger())
		mux := http.NewServeMux()
		registerSyncRoutes(mux, verifier, org, ss, nil)
		registerAliasRoutes(mux, &server{store: e.store, logger: discardLogger()}, ss, verifier, org, nil)
		srv := httptest.NewServer(withProblemInstance(mux))
		t.Cleanup(srv.Close)

		for _, tc := range []struct {
			name      string
			path      func(repoURL string) string
			wantAgain string
		}{
			{"v1 base", func(string) string { return "/api/v1/repos/base" }, "repository detached"},
			{"alias base", func(u string) string { return "/api/repos/" + url.PathEscape(u) + "/base" }, "repository detached"},
			{"alias upstream", func(u string) string { return "/api/repos/" + url.PathEscape(u) + "/upstream" }, "repository already detached"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				repoURL := "team/detach-" + randomHex(t, 4)
				repo, err := e.store.CreateRepo(t.Context(), org, repoURL, "main")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := mem.PutUpstream(t.Context(), upsync.Upstream{RepoID: repo.RepoID,
					Provider: "github", Owner: "own", Name: "proj"}); err != nil {
					t.Fatal(err)
				}
				tok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
				for i, want := range []string{"repository detached", tc.wantAgain} {
					req, err := http.NewRequest(http.MethodDelete, srv.URL+tc.path(repoURL), nil)
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set("Authorization", "Bearer "+tok)
					resp, err := srv.Client().Do(req)
					if err != nil {
						t.Fatal(err)
					}
					var got map[string]string
					err = json.NewDecoder(resp.Body).Decode(&got)
					resp.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					if resp.StatusCode != http.StatusOK || len(got) != 1 || got["message"] != want {
						t.Fatalf("detach %d: status %d body %v, want 200 {\"message\": %q}", i+1, resp.StatusCode, got, want)
					}
					if _, err := mem.GetUpstream(t.Context(), repo.RepoID); !errors.Is(err, upsync.ErrNotFound) {
						t.Fatalf("detach %d: upstream still linked (err %v)", i+1, err)
					}
				}
			})
		}
	})
}
