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
		repo, writeTok, readTok := wctRepo(t, e)

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
		c2Env := wctRequireOK(t, resp, body)
		c2, c2Tree := c2Env.Commit.CommitSHA, c2Env.Commit.TreeSHA

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

		t.Run("present empty expected_target_sha drops the legacy guard", func(t *testing.T) {
			resp, body := pack(t, "main", "empty-current", "z.txt",
				map[string]any{"expected_target_sha": "", "expected_head_sha": c1})
			wctRequireOK(t, resp, body)
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

		// setRef points a branch at a commit directly in the ref authority.
		setRef := func(t *testing.T, branch, sha string) {
			t.Helper()
			if won, err := e.refStore.CASRef(t.Context(), repo.RepoID, "refs/heads/"+branch, ref.ZeroSHA, sha); err != nil || !won {
				t.Fatalf("seed %s: won=%v err=%v", branch, won, err)
			}
		}
		mainTip := func(t *testing.T) string { t.Helper(); return wctBranchTip(t, e, readTok, "main", false) }
		requireTip := func(t *testing.T, want string) {
			t.Helper()
			if got := mainTip(t); got != want {
				t.Fatalf("main moved to %s, want %s", got, want)
			}
		}
		var c1Tree string
		t.Run("restore base_ref sha with expected_target_sha", func(t *testing.T) {
			tip := mainTip(t)
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": c1, "expected_target_sha": tip})
			env := wctRequireOK(t, resp, body)
			c1Tree = env.Commit.TreeSHA
			if env.Result.OldSHA != tip {
				t.Fatalf("old_sha = %q, want %q", env.Result.OldSHA, tip)
			}
		})

		t.Run("restore base_ref equal to the tip is 412", func(t *testing.T) {
			tip := mainTip(t)
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": tip})
			wctRequireFail(t, resp, body, http.StatusPreconditionFailed, "precondition_failed")
			requireTip(t, tip)
		})

		t.Run("restore base_ref with no tree change is 412", func(t *testing.T) {
			tip := mainTip(t)
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": c1})
			wctRequireFail(t, resp, body, http.StatusPreconditionFailed, "precondition_failed")
			requireTip(t, tip)
		})

		t.Run("restore base_ref that is not an ancestor is 412", func(t *testing.T) {
			tip := mainTip(t)
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": "persist"})
			wctRequireFail(t, resp, body, http.StatusPreconditionFailed, "precondition_failed")
			requireTip(t, tip)
		})

		t.Run("restore base_ref resolves a 40-hex branch name as a branch", func(t *testing.T) {
			// Branch named like c1's hex but pointing at c2. Read as a SHA,
			// c1 would be a no-op (412); read as a branch it restores c2.
			setRef(t, c1, c2)
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": c1})
			if got := wctRequireOK(t, resp, body).Commit.TreeSHA; got != c2Tree {
				t.Fatalf("restored tree %s, want c2 tree", got)
			}
		})

		t.Run("restore base_ref branch name resolves to its tip", func(t *testing.T) {
			setRef(t, "at-c1", c1)
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": "at-c1"})
			if got := wctRequireOK(t, resp, body).Commit.TreeSHA; got != c1Tree {
				t.Fatalf("restored tree %s, want c1 tree %s", got, c1Tree)
			}
		})

		t.Run("restore base_ref wins over target_commit_sha", func(t *testing.T) {
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": c2, "target_commit_sha": c1})
			if got := wctRequireOK(t, resp, body).Commit.TreeSHA; got != c2Tree {
				t.Fatalf("tree = %s, want c2 tree", got)
			}
		})

		t.Run("legacy target_commit_sha keeps its unchecked behavior", func(t *testing.T) {
			// Recorded deviation: the SDK form restores even a no-op.
			tip := mainTip(t)
			resp, body := restore(t, map[string]any{"target_branch": "main", "target_commit_sha": tip})
			wctRequireOK(t, resp, body)
		})

		t.Run("restore unknown base_ref is 404", func(t *testing.T) {
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": "no-such-branch"})
			wctRequireFail(t, resp, body, http.StatusNotFound, "not_found")
		})

		t.Run("restore with present empty base_ref is 400", func(t *testing.T) {
			resp, body := restore(t, map[string]any{"target_branch": "main", "base_ref": "", "target_commit_sha": c1})
			wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid")
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

		t.Run("stale query guard is 409 ref_guard_failed, body guard stays 412", func(t *testing.T) {
			tip := addNote(t, fx.c2, "")
			resp, body := del(t, "/api/v1/repos/notes",
				url.Values{"object_ref": {fx.c2}, "expected_notes_ref_sha": {ref.ZeroSHA}}, nil)
			assertNotesRefGuard(t, resp.StatusCode, body, fx.c2, "refs/notes/commits", tip)
			resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c2, "expected_ref_sha": ref.ZeroSHA})
			if resp.StatusCode != http.StatusPreconditionFailed {
				t.Fatalf("body guard: status %d body %v, want 412", resp.StatusCode, body)
			}
			if _, has := body["code"]; has {
				t.Fatalf("body guard carries problem members: %v", body)
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

// TestRestoreCommitTagBaseRef covers issue #381: a restore-commit base_ref
// naming a lightweight tag, an annotated tag (peeled to its commit), a
// fully qualified refs/tags/ name, or an uppercase full SHA.
func TestRestoreCommitTagBaseRef(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, readTok := wctRepo(t, e)

		// c1 (root): a.txt v1   <- lightweight tag "light"
		// c2: a.txt v2          <- main, annotated tag "annot"
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", "v1\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "c1")
		c1 := coreRevParse(t, dir, "HEAD")
		coreWriteFile(t, dir, "a.txt", "v2\n")
		coreGit(t, dir, "commit", "-q", "-am", "c2")
		c2 := coreRevParse(t, dir, "HEAD")
		coreGit(t, dir, "tag", "-a", "-m", "annotated", "annot", c2)
		tagObj := coreRevParse(t, dir, "refs/tags/annot")
		if tagObj == c2 {
			t.Fatal("annotated tag object sha unexpectedly equals commit sha")
		}
		tree1, tree2 := coreRevParse(t, dir, c1+"^{tree}"), coreRevParse(t, dir, c2+"^{tree}")
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{
			"refs/heads/main":                c2,
			"refs/tags/light":                c1,
			"refs/tags/annot":                tagObj,
			auth.EphemeralRefName("scratch"): c2,
		})

		// Each restore parents on the current tip and moves main, so every
		// step restores a tree that differs from the previous step's.
		steps := []struct{ baseRef, wantTree string }{
			{"light", tree1},
			{"annot", tree2},
			{strings.ToUpper(c1), tree1},
			{"refs/tags/annot", tree2},
		}
		for _, step := range steps {
			tip := wctBranchTip(t, e, readTok, "main", false)
			raw, err := json.Marshal(map[string]any{"metadata": map[string]any{
				"target_branch": "main", "base_ref": step.baseRef, "author": wctAuthor,
				"expected_target_sha": tip}})
			if err != nil {
				t.Fatal(err)
			}
			resp, body := wctPost(t, srv, "/api/v1/repos/restore-commit", writeTok, bytes.NewReader(raw))
			env := wctRequireOK(t, resp, body)
			if env.Commit.TreeSHA != step.wantTree || env.Result.OldSHA != tip {
				t.Fatalf("base_ref %q: tree %s old_sha %s, want tree %s old_sha %s",
					step.baseRef, env.Commit.TreeSHA, env.Result.OldSHA, step.wantTree, tip)
			}
			if got := wctBranchTip(t, e, readTok, "main", false); got != env.Commit.CommitSHA {
				t.Fatalf("base_ref %q: main = %s, want %s", step.baseRef, got, env.Commit.CommitSHA)
			}
		}

		// target_is_ephemeral restores the ephemeral branch. base_is_ephemeral
		// is accepted and ignored: "light" exists only outside the ephemeral
		// namespace, so honoring the flag would answer 404.
		mainBefore := wctBranchTip(t, e, readTok, "main", false)
		raw, err := json.Marshal(map[string]any{"metadata": map[string]any{
			"target_branch": "scratch", "target_is_ephemeral": true, "base_is_ephemeral": true,
			"base_ref": "light", "author": wctAuthor}})
		if err != nil {
			t.Fatal(err)
		}
		resp, body := wctPost(t, srv, "/api/v1/repos/restore-commit", writeTok, bytes.NewReader(raw))
		env := wctRequireOK(t, resp, body)
		if env.Commit.TreeSHA != tree1 || env.Result.OldSHA != c2 {
			t.Fatalf("ephemeral restore: tree %s old_sha %s, want tree %s old_sha %s",
				env.Commit.TreeSHA, env.Result.OldSHA, tree1, c2)
		}
		if got := wctBranchTip(t, e, readTok, "scratch", true); got != env.Commit.CommitSHA {
			t.Fatalf("ephemeral scratch = %s, want %s", got, env.Commit.CommitSHA)
		}
		if got := wctBranchTip(t, e, readTok, "main", false); got != mainBefore {
			t.Fatalf("main moved %s -> %s", mainBefore, got)
		}
	})
}

// TestGuardSHAUppercase covers issue #385: every full-SHA guard field
// accepts an uppercase 40-hex value. A stale uppercase guard still fails
// the guard (412, or 409 on merge per #532), not validation (400), and a
// current one passes.
func TestGuardSHAUppercase(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, readTok := wctRepo(t, e)
		post := func(t *testing.T, path string, frames ...any) (int, []byte) {
			t.Helper()
			resp, body := wctPost(t, srv, path, writeTok, bytes.NewReader(wctLines(t, frames...)))
			return resp.StatusCode, body
		}
		pack := func(t *testing.T, msg string, extra map[string]any) (int, []byte) {
			t.Helper()
			files := []map[string]any{{"path": msg + ".txt", "content_id": "id", "operation": "upsert"}}
			return post(t, "/api/v1/repos/commit-pack", wctMeta("main", msg, files, extra), wctChunk("id", []byte(msg), true))
		}
		mainTip := func(t *testing.T) string { t.Helper(); return wctBranchTip(t, e, readTok, "main", false) }
		code, body := pack(t, "root", nil)
		if code != http.StatusOK {
			t.Fatalf("seed: %d %s", code, body)
		}
		root, rootTree := wctDecode(t, body).Commit.CommitSHA, wctDecode(t, body).Commit.TreeSHA
		// advance commits a new file unguarded and returns the current
		// and the now stale tip, so each restore changes the tree.
		advance := func(t *testing.T) (current, stale string) {
			t.Helper()
			stale = mainTip(t)
			if code, body := pack(t, "adv-"+stale[:8], nil); code != http.StatusOK {
				t.Fatalf("advance: %d %s", code, body)
			}
			return mainTip(t), stale
		}
		diff := func(t *testing.T, extra map[string]any) (int, []byte) {
			return post(t, "/api/v1/repos/diff-commit", wctMeta("main", "diff", nil, extra), wctDiffChunk(nil, true))
		}
		restore := func(t *testing.T, extra map[string]any) (int, []byte) {
			md := map[string]any{"target_branch": "main", "author": wctAuthor, "target_commit_sha": root}
			for k, v := range extra {
				md[k] = v
			}
			return post(t, "/api/v1/repos/restore-commit", map[string]any{"metadata": md})
		}

		msrv := wmServer(t, e)
		mfx := wmSeedFixture(t, e)
		merge := func(t *testing.T, extra map[string]any) (int, []byte) {
			extra["author"] = wmTestAuthor
			resp, body := wmDo(t, msrv, "POST", "/api/v1/repos/merge", mfx.token, wmMergeBody("feature", "t-clean", "merge", extra))
			return resp.StatusCode, body
		}

		nsrv := newWrefServer(t, e)
		nfx := wrefSeedFixture(t, e)
		note := func(t *testing.T, extra map[string]any) (int, []byte) {
			t.Helper()
			req := map[string]any{"object_ref": nfx.c2, "action": "append", "note": "n", "notes_ref": "case"}
			for k, v := range extra {
				req[k] = v
			}
			resp, body := wrefDo(t, nsrv, "POST", "/api/v1/repos/notes", nfx.token, req)
			raw, _ := json.Marshal(body)
			return resp.StatusCode, raw
		}
		noteTips := func(t *testing.T) (current, stale string) {
			t.Helper()
			if code, body := note(t, nil); code != http.StatusOK {
				t.Fatalf("seed note: %d %s", code, body)
			}
			return wrefRefs(t, e, nfx.repoID)["refs/notes/case"], nfx.c1
		}

		for _, tc := range []struct {
			name, field string
			tips        func(t *testing.T) (current, stale string)
			send        func(t *testing.T, extra map[string]any) (int, []byte)
		}{
			{"commit-pack", "expected_target_sha", advance, func(t *testing.T, x map[string]any) (int, []byte) { return pack(t, "cp-target", x) }},
			{"commit-pack", "expected_head_sha", advance, func(t *testing.T, x map[string]any) (int, []byte) { return pack(t, "cp-head", x) }},
			{"diff-commit", "expected_target_sha", advance, diff},
			{"diff-commit", "expected_head_sha", advance, diff},
			{"restore-commit", "expected_target_sha", advance, restore},
			{"restore-commit", "expected_head_sha", advance, restore},
			{"merge", "expected_target_sha", func(*testing.T) (string, string) { return mfx.c2, mfx.c1 }, merge},
			{"notes", "expected_notes_ref_sha", noteTips, note},
		} {
			t.Run(tc.name+" "+tc.field, func(t *testing.T) {
				current, stale := tc.tips(t)
				want := http.StatusPreconditionFailed
				if tc.name == "merge" {
					want = http.StatusConflict // #532: upstream's merge guard status
				}
				if code, body := tc.send(t, map[string]any{tc.field: strings.ToUpper(stale)}); code != want {
					t.Fatalf("stale uppercase guard: %d %s, want %d", code, body, want)
				}
				if code, body := tc.send(t, map[string]any{tc.field: strings.ToUpper(current)}); code != http.StatusOK {
					t.Fatalf("current uppercase guard: %d %s, want 200", code, body)
				}
			})
		}

		t.Run("restore-commit target_commit_sha", func(t *testing.T) {
			advance(t)
			code, body := restore(t, map[string]any{"target_commit_sha": strings.ToUpper(root)})
			if code != http.StatusOK {
				t.Fatalf("uppercase target_commit_sha: %d %s, want 200", code, body)
			}
			if got := wctDecode(t, body).Commit.TreeSHA; got != rootTree {
				t.Fatalf("restored tree %s, want %s", got, rootTree)
			}
		})
	})
}
