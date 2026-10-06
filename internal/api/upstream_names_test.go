package api

// Regression tests for the 2026-10-05 upstream spec refresh (issue #378):
// the current metadata names of the commit write endpoints, and the
// query-parameter forms of DELETE branches/notes. Both run on the pg and
// sqlite lanes; the deprecated forms stay covered by write_commit_test.go
// and write_refs_test.go.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/ref"
)

func TestCommitMetadataCurrentNames(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, readTok := wctRepo(t, e)

		pack := func(t *testing.T, branch, msg, path string, extra map[string]any) (*http.Response, []byte) {
			t.Helper()
			body := wctLines(t,
				wctMeta(branch, msg, []map[string]any{
					{"path": path, "content_id": "id-" + path, "operation": "upsert"},
				}, extra),
				wctChunk("id-"+path, []byte(msg+"\n"), true),
			)
			return wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
		}

		resp, body := pack(t, "main", "c1", "a.txt", nil)
		c1 := wctRequireOK(t, resp, body).Commit.CommitSHA
		resp, body = pack(t, "main", "c2", "b.txt", map[string]any{"expected_target_sha": c1})
		c2 := wctRequireOK(t, resp, body).Commit.CommitSHA

		t.Run("stale expected_target_sha is 412", func(t *testing.T) {
			resp, body := pack(t, "main", "stale", "x.txt", map[string]any{"expected_target_sha": c1})
			wctRequireFail(t, resp, body, http.StatusPreconditionFailed, "precondition_failed")
		})

		t.Run("expected_target_sha wins over expected_head_sha", func(t *testing.T) {
			// The legacy guard matches the tip; only the current name is stale.
			tip := wctBranchTip(t, e, readTok, "main", false)
			resp, body := pack(t, "main", "stale-current", "y.txt",
				map[string]any{"expected_target_sha": c1, "expected_head_sha": tip})
			wctRequireFail(t, resp, body, http.StatusPreconditionFailed, "precondition_failed")
		})

		t.Run("target_is_ephemeral writes the ephemeral branch", func(t *testing.T) {
			resp, body := pack(t, "preview", "eph", "e.txt",
				map[string]any{"target_is_ephemeral": true, "base_branch": "main"})
			tip := wctRequireOK(t, resp, body).Commit.CommitSHA
			if got := wctBranchTip(t, e, readTok, "preview", true); got != tip {
				t.Fatalf("ephemeral preview tip = %q, want %q", got, tip)
			}
			if got := wctBranchTip(t, e, readTok, "preview", false); got != "" {
				t.Fatalf("persistent preview exists at %q", got)
			}
		})

		t.Run("explicit target_is_ephemeral false beats legacy ephemeral true", func(t *testing.T) {
			resp, body := pack(t, "persist", "p", "p.txt",
				map[string]any{"target_is_ephemeral": false, "ephemeral": true, "base_branch": "main"})
			tip := wctRequireOK(t, resp, body).Commit.CommitSHA
			if got := wctBranchTip(t, e, readTok, "persist", false); got != tip {
				t.Fatalf("persistent persist tip = %q, want %q", got, tip)
			}
		})

		t.Run("base_is_ephemeral branches from the ephemeral base", func(t *testing.T) {
			ephTip := wctBranchTip(t, e, readTok, "preview", true)
			resp, body := pack(t, "from-eph", "fe", "fe.txt",
				map[string]any{"base_branch": "preview", "base_is_ephemeral": true})
			env := wctRequireOK(t, resp, body)
			var commits listCommitsResponse
			wctReadJSON(t, e, "/api/v1/repos/commits?branch=from-eph&limit=1", readTok, &commits)
			if len(commits.Commits) != 1 || commits.Commits[0].SHA != env.Commit.CommitSHA ||
				commits.Commits[0].ParentSHAs[0] != ephTip {
				t.Fatalf("from-eph history = %+v, want parent %s", commits.Commits, ephTip)
			}
		})

		restore := func(t *testing.T, md map[string]any) (*http.Response, []byte) {
			t.Helper()
			md["author"] = wctAuthor
			raw, err := json.Marshal(map[string]any{"metadata": md})
			if err != nil {
				t.Fatal(err)
			}
			return wctPost(t, srv, "/api/v1/repos/restore-commit", writeTok, bytes.NewReader(raw))
		}

		var c1Tree string
		t.Run("restore base_ref sha with expected_target_sha", func(t *testing.T) {
			tip := wctBranchTip(t, e, readTok, "main", false)
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": c1, "expected_target_sha": tip})
			env := wctRequireOK(t, resp, body)
			c1Tree = env.Commit.TreeSHA
			if env.Result.OldSHA != tip {
				t.Fatalf("old_sha = %q, want %q", env.Result.OldSHA, tip)
			}
		})

		t.Run("restore base_ref branch name resolves to its tip", func(t *testing.T) {
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": "persist"})
			env := wctRequireOK(t, resp, body)
			if env.Commit.TreeSHA == c1Tree {
				t.Fatalf("restore to persist kept c1 tree %s", c1Tree)
			}
		})

		t.Run("restore base_ref wins over target_commit_sha", func(t *testing.T) {
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": c1, "target_commit_sha": c2})
			if env := wctRequireOK(t, resp, body); env.Commit.TreeSHA != c1Tree {
				t.Fatalf("tree = %s, want c1 tree %s", env.Commit.TreeSHA, c1Tree)
			}
		})

		t.Run("restore base_ref resolves a 40-hex branch name as a branch", func(t *testing.T) {
			// A branch whose name is the hex of c2 but whose tip is c1:
			// base_ref must pick the branch (c1 tree), not object c2.
			resp, body := pack(t, c2, "hexname", "h.txt", map[string]any{"base_branch": "main"})
			hexTip := wctRequireOK(t, resp, body).Commit.CommitSHA
			resp, body = restore(t, map[string]any{"target_branch": "main", "base_ref": c2})
			got := wctRequireOK(t, resp, body).Commit.TreeSHA
			resp, body = restore(t, map[string]any{"target_branch": "main", "base_ref": hexTip})
			want := wctRequireOK(t, resp, body).Commit.TreeSHA
			if got != want {
				t.Fatalf("base_ref %s restored tree %s, want the branch tip tree %s", c2, got, want)
			}
		})

		t.Run("restore unknown base_ref is 404", func(t *testing.T) {
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": "no-such-branch"})
			wctRequireFail(t, resp, body, http.StatusNotFound, "not_found")
		})

		t.Run("restore without a target is 400", func(t *testing.T) {
			resp, body := restore(t, map[string]any{"target_branch": "main"})
			wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid")
		})
	})
}

func TestDeleteBranchAndNoteQueryParams(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newWrefServer(t, e)
		fx := wrefSeedFixture(t, e)

		del := func(t *testing.T, path string, q url.Values, body any) (*http.Response, map[string]any) {
			t.Helper()
			return wrefDo(t, srv, "DELETE", path+"?"+q.Encode(), fx.token, body)
		}

		t.Run("branch by query, no body", func(t *testing.T) {
			resp, body := del(t, "/api/v1/repos/branches", url.Values{"target_branch": {"feature"}}, nil)
			if resp.StatusCode != http.StatusOK || body["target_branch"] != "feature" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)["refs/heads/feature"]; exists {
				t.Fatal("refs/heads/feature still present")
			}
		})

		t.Run("ephemeral branch by query", func(t *testing.T) {
			resp, body := del(t, "/api/v1/repos/branches",
				url.Values{"target_branch": {"scratch"}, "ephemeral": {"true"}}, nil)
			if resp.StatusCode != http.StatusOK || body["ephemeral"] != true {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)[auth.EphemeralRefName("scratch")]; exists {
				t.Fatal("ephemeral scratch still present")
			}
		})

		t.Run("query wins over body", func(t *testing.T) {
			if won, err := e.refStore.CASRef(t.Context(), fx.repoID, "refs/heads/keep", ref.ZeroSHA, fx.c1); err != nil || !won {
				t.Fatalf("seed keep: won=%v err=%v", won, err)
			}
			if won, err := e.refStore.CASRef(t.Context(), fx.repoID, "refs/heads/drop", ref.ZeroSHA, fx.c1); err != nil || !won {
				t.Fatalf("seed drop: won=%v err=%v", won, err)
			}
			resp, body := del(t, "/api/v1/repos/branches", url.Values{"target_branch": {"drop"}},
				map[string]any{"target_branch": "keep"})
			if resp.StatusCode != http.StatusOK || body["target_branch"] != "drop" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			refs := wrefRefs(t, e, fx.repoID)
			if _, exists := refs["refs/heads/drop"]; exists {
				t.Fatal("drop still present")
			}
			if refs["refs/heads/keep"] != fx.c1 {
				t.Fatal("body-named keep was deleted")
			}
			// An explicit ephemeral=false query beats a body ephemeral:true.
			resp, body = del(t, "/api/v1/repos/branches", url.Values{"target_branch": {"keep"}, "ephemeral": {"false"}},
				map[string]any{"ephemeral": true})
			if resp.StatusCode != http.StatusOK || body["ephemeral"] != false {
				t.Fatalf("ephemeral=false override: status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("present empty query does not fall back to body", func(t *testing.T) {
			resp, body := del(t, "/api/v1/repos/branches", url.Values{"target_branch": {""}},
				map[string]any{"target_branch": "main-copy"})
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %d body %v, want 400", resp.StatusCode, body)
			}
		})

		t.Run("bad ephemeral and missing name are 400", func(t *testing.T) {
			for name, q := range map[string]url.Values{
				"bad ephemeral": {"target_branch": {"x"}, "ephemeral": {"yes"}},
				"missing name":  {},
			} {
				if resp, body := del(t, "/api/v1/repos/branches", q, nil); resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status %d body %v, want 400", name, resp.StatusCode, body)
				}
			}
		})

		t.Run("default branch by query is 409, body form keeps 403", func(t *testing.T) {
			resp, _ := del(t, "/api/v1/repos/branches", url.Values{"target_branch": {"main"}}, nil)
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("query: status %d, want 409", resp.StatusCode)
			}
			resp, _ = wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", fx.token, map[string]any{"target_branch": "main"})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("body: status %d, want 403", resp.StatusCode)
			}
			if wrefRefs(t, e, fx.repoID)["refs/heads/main"] != fx.c2 {
				t.Fatal("default branch moved")
			}
		})

		addNote := func(t *testing.T, sha, notesRef string) string {
			t.Helper()
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"object_ref": sha, "note": "n", "notes_ref": notesRef})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("add note: status %d body %v", resp.StatusCode, body)
			}
			return body["new_ref_sha"].(string)
		}

		t.Run("note by query, no body", func(t *testing.T) {
			addNote(t, fx.c1, "qnotes")
			resp, body := del(t, "/api/v1/repos/notes", url.Values{"object_ref": {fx.c1}, "notes_ref": {"qnotes"}}, nil)
			if resp.StatusCode != http.StatusOK || wrefNoteResultOf(t, body)["success"] != true ||
				body["notes_ref"] != "refs/notes/qnotes" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("stale query guard is 409, body guard stays 412", func(t *testing.T) {
			tip := addNote(t, fx.c2, "")
			resp, body := del(t, "/api/v1/repos/notes",
				url.Values{"object_ref": {fx.c2}, "expected_notes_ref_sha": {ref.ZeroSHA}}, nil)
			if resp.StatusCode != http.StatusConflict || wrefNoteResultOf(t, body)["status"] != "conflict" {
				t.Fatalf("query guard: status %d body %v, want 409 conflict", resp.StatusCode, body)
			}
			resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c2, "expected_ref_sha": ref.ZeroSHA})
			if resp.StatusCode != http.StatusPreconditionFailed {
				t.Fatalf("body guard: status %d body %v, want 412", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]; got != tip {
				t.Fatalf("notes ref moved to %q after failed guards", got)
			}
			// Query wins over the body: a matching query guard deletes
			// despite a stale body guard.
			resp, body = del(t, "/api/v1/repos/notes",
				url.Values{"object_ref": {fx.c2}, "expected_notes_ref_sha": {tip}},
				map[string]any{"expected_notes_ref_sha": ref.ZeroSHA, "object_ref": fx.c1})
			if resp.StatusCode != http.StatusOK || body["sha"] != fx.c2 {
				t.Fatalf("matching query guard: status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("missing object_ref is 400 and malformed body still 400", func(t *testing.T) {
			if resp, body := del(t, "/api/v1/repos/notes", url.Values{}, nil); resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("no object: status %d body %v", resp.StatusCode, body)
			}
			req, err := http.NewRequest("DELETE", srv.URL+"/api/v1/repos/notes?object_ref="+fx.c1, strings.NewReader("{"))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+fx.token)
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("malformed body: status %d, want 400", resp.StatusCode)
			}
		})
	})
}
