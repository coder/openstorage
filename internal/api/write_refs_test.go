package api

// Contract integration tests for the W3 write endpoints (docs/07 p4):
// POST /repos/branches/create, DELETE /repos/branches, POST/DELETE
// /repos/tags, POST/DELETE /repos/notes — fanned out over the pg and
// sqlite lanes via runBackends.
//
// api.New (api.go, owned by a sibling slice) does not register these
// routes yet, so each lane builds its own httptest server through
// registerWriteRefRoutes on top of the SAME lane stores — reads through
// the lane's regular server (e.srv) therefore double as cross-surface
// assertions. Fixtures seed durable state exactly like the read tests
// (coreGit + coreSeedStores). All helpers carry a wref prefix.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"codestorage/internal/auth"
	"codestorage/internal/gitread"
	"codestorage/internal/ref"
)

// newWrefServer builds an httptest server exposing only the W3 write routes
// with the production auth middleware, backed by the lane's stores.
func newWrefServer(t *testing.T, e *testEnv) *httptest.Server {
	t.Helper()
	s := &server{
		store:       e.store,
		httpBaseURL: testHTTPBaseURL,
		logger:      discardLogger(),
		refStore:    e.refStore,
		blobStore:   e.blobStore,
		cacheDir:    e.cacheDir,
	}
	mux := http.NewServeMux()
	s.registerWriteRefRoutes(mux, auth.NewVerifier(e.store))
	srv := httptest.NewServer(recoverPanics(s.logger)(mux))
	t.Cleanup(srv.Close)
	return srv
}

// wrefDo sends one JSON request to the write server and returns the decoded
// generic body plus the response.
func wrefDo(t *testing.T, srv *httptest.Server, method, path, token string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	decoded := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("%s %s: decode body: %v", method, path, err)
	}
	return resp, decoded
}

// wrefFixture is one seeded repo for the write tests.
type wrefFixture struct {
	repoID string
	url    string
	org    string
	token  string // git:read+git:write JWT

	c1, c2 string // main history, oldest first
	tagObj string // annotated tag object sha (refs/tags/annot)
}

// wrefSeedFixture creates a repo (default branch main) with:
//
//	c1 (root): a.txt          <- also branch "feature", ephemeral "scratch"
//	c2: a.txt v2, b.txt       <- main, annotated tag "annot"
func wrefSeedFixture(t *testing.T, e *testEnv) wrefFixture {
	t.Helper()
	ctx := context.Background()
	org := e.newOrg(t)
	url := "https://example.com/" + org + "/wref-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(ctx, org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	coreWriteFile(t, dir, "a.txt", "v1\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c1")
	c1 := coreRevParse(t, dir, "HEAD")
	coreWriteFile(t, dir, "a.txt", "v2\n")
	coreWriteFile(t, dir, "b.txt", "b\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c2")
	c2 := coreRevParse(t, dir, "HEAD")
	coreGit(t, dir, "tag", "-a", "-m", "annotated", "annot", c2)
	tagObj := coreRevParse(t, dir, "refs/tags/annot")
	if tagObj == c2 {
		t.Fatal("annotated tag object sha unexpectedly equals commit sha")
	}

	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{
		"refs/heads/main":                c2,
		"refs/heads/feature":             c1,
		"refs/tags/annot":                tagObj,
		auth.EphemeralRefName("scratch"): c1,
	})

	token := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
	return wrefFixture{repoID: repo.RepoID, url: url, org: org, token: token, c1: c1, c2: c2, tagObj: tagObj}
}

// wrefRefs snapshots the ref authority for the fixture repo.
func wrefRefs(t *testing.T, e *testEnv, repoID string) map[string]string {
	t.Helper()
	refs, err := e.refStore.ListRefs(context.Background(), repoID)
	if err != nil {
		t.Fatalf("list refs: %v", err)
	}
	return refs
}

func TestWriteRefsBranches(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newWrefServer(t, e)
		fx := wrefSeedFixture(t, e)

		t.Run("create from base_branch", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "topic", "base_branch": "main"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %v", resp.StatusCode, body)
			}
			if body["commit_sha"] != fx.c2 || body["target_branch"] != "topic" || body["target_is_ephemeral"] != false {
				t.Fatalf("body = %v", body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/heads/topic"]; got != fx.c2 {
				t.Fatalf("refs/heads/topic = %q, want %s", got, fx.c2)
			}
			// Cross-surface: the read API (regular server, same stores)
			// must list the API-created branch.
			readTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitRead}})
			rresp, rbody := e.doReq(t, "GET", "/api/v1/repos/branches", readTok, nil, "")
			if rresp.StatusCode != http.StatusOK || !strings.Contains(string(rbody), `"topic"`) {
				t.Fatalf("read surface: status %d body %s", rresp.StatusCode, rbody)
			}
		})

		t.Run("create from base_ref sha and annotated tag peels", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "from-sha", "base_ref": fx.c1})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c1 {
				t.Fatalf("from sha: status %d body %v", resp.StatusCode, body)
			}
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "from-tag", "base_ref": "annot"})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c2 {
				t.Fatalf("from annotated tag (want peeled %s): status %d body %v", fx.c2, resp.StatusCode, body)
			}
		})

		t.Run("create existing branch conflicts", func(t *testing.T) {
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "feature", "base_branch": "main"})
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("status = %d, want 409", resp.StatusCode)
			}
		})

		t.Run("create invalid and missing input", func(t *testing.T) {
			for name, req := range map[string]map[string]any{
				"refs prefix":  {"target_branch": "refs/heads/x", "base_branch": "main"},
				"dotdot":       {"target_branch": "a..b", "base_branch": "main"},
				"empty target": {"base_branch": "main"},
				"missing base": {"target_branch": "ok"},
			} {
				resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token, req)
				if resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status = %d, want 400", name, resp.StatusCode)
				}
			}
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "ok", "base_branch": "no-such-branch"})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("unknown base: status = %d, want 404", resp.StatusCode)
			}
		})

		t.Run("create ephemeral target", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "preview", "base_branch": "main", "target_is_ephemeral": true})
			if resp.StatusCode != http.StatusOK || body["target_is_ephemeral"] != true {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)[auth.EphemeralRefName("preview")]; got != fx.c2 {
				t.Fatalf("ephemeral preview = %q, want %s", got, fx.c2)
			}
		})

		t.Run("create from ephemeral base", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "from-eph", "base_branch": "scratch", "base_is_ephemeral": true})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c1 {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("delete default branch refused, ephemeral exempt", func(t *testing.T) {
			// Invariant 4: persistent default branch is protected.
			resp, body := wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", fx.token,
				map[string]any{"name": "main"})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("delete default: status = %d body %v, want 403", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/heads/main"]; got != fx.c2 {
				t.Fatalf("refs/heads/main = %q after refused delete", got)
			}
			// Invariant 7: the ephemeral namespace is exempt — seed an
			// ephemeral "main" and delete it.
			if won, err := e.refStore.CASRef(context.Background(), fx.repoID, auth.EphemeralRefName("main"), ref.ZeroSHA, fx.c1); err != nil || !won {
				t.Fatalf("seed ephemeral main: won=%v err=%v", won, err)
			}
			resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", fx.token,
				map[string]any{"name": "main", "ephemeral": true})
			if resp.StatusCode != http.StatusOK || body["ephemeral"] != true {
				t.Fatalf("delete ephemeral main: status %d body %v", resp.StatusCode, body)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)[auth.EphemeralRefName("main")]; exists {
				t.Fatal("ephemeral main still present after delete")
			}
		})

		t.Run("delete branch", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", fx.token,
				map[string]any{"name": "feature"})
			if resp.StatusCode != http.StatusOK || body["name"] != "feature" || body["ephemeral"] != false {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)["refs/heads/feature"]; exists {
				t.Fatal("refs/heads/feature still present after delete")
			}
			resp, _ = wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", fx.token,
				map[string]any{"name": "feature"})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("second delete: status = %d, want 404", resp.StatusCode)
			}
		})
	})
}

func TestWriteRefsTags(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newWrefServer(t, e)
		fx := wrefSeedFixture(t, e)

		t.Run("create lightweight tag from sha", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "v1.0.0", "target": fx.c1})
			if resp.StatusCode != http.StatusOK || body["sha"] != fx.c1 || body["name"] != "v1.0.0" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/tags/v1.0.0"]; got != fx.c1 {
				t.Fatalf("refs/tags/v1.0.0 = %q, want %s", got, fx.c1)
			}
		})

		t.Run("target peels annotated tag to commit", func(t *testing.T) {
			// Invariant 10: the stored value is the peeled COMMIT sha even
			// when the target names an annotated tag (object sha tagObj).
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "v1.0.1", "target": "annot"})
			if resp.StatusCode != http.StatusOK || body["sha"] != fx.c2 {
				t.Fatalf("status %d body %v, want peeled sha %s", resp.StatusCode, body, fx.c2)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/tags/v1.0.1"]; got != fx.c2 {
				t.Fatalf("refs/tags/v1.0.1 = %q, want peeled %s", got, fx.c2)
			}
		})

		t.Run("target must resolve to a commit", func(t *testing.T) {
			// A blob sha never peels to a commit.
			repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, fx.repoID)
			if err != nil {
				t.Fatal(err)
			}
			blobSHA := strings.TrimSpace(coreGit(t, repoPath, "rev-parse", fx.c2+":a.txt"))
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "bad", "target": blobSHA})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("blob target: status = %d, want 404", resp.StatusCode)
			}
		})

		t.Run("duplicate and invalid names", func(t *testing.T) {
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "annot", "target": fx.c1})
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("duplicate: status = %d, want 409", resp.StatusCode)
			}
			resp, _ = wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "refs/tags/x", "target": fx.c1})
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("refs/ name: status = %d, want 400", resp.StatusCode)
			}
		})

		t.Run("delete annotated tag by stored ref", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "DELETE", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "annot"})
			if resp.StatusCode != http.StatusOK || body["name"] != "annot" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)["refs/tags/annot"]; exists {
				t.Fatal("refs/tags/annot still present after delete")
			}
			resp, _ = wrefDo(t, srv, "DELETE", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "annot"})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("second delete: status = %d, want 404", resp.StatusCode)
			}
		})

		t.Run("delete requires both scopes", func(t *testing.T) {
			// docs/03: DELETE /repos/tags demands git:read AND git:write.
			for _, scopes := range [][]string{{auth.ScopeGitWrite}, {auth.ScopeGitRead}} {
				tok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: scopes})
				resp, _ := wrefDo(t, srv, "DELETE", "/api/v1/repos/tags", tok,
					map[string]any{"name": "v1.0.0"})
				if resp.StatusCode != http.StatusForbidden {
					t.Errorf("scopes %v: status = %d, want 403", scopes, resp.StatusCode)
				}
			}
		})
	})
}

// wrefNoteBody asserts the note-write envelope shape and returns result.
func wrefNoteResultOf(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	result, ok := body["result"].(map[string]any)
	if !ok {
		t.Fatalf("body %v has no result object", body)
	}
	if _, ok := body["new_ref_sha"].(string); !ok {
		t.Fatalf("body %v has no new_ref_sha string", body)
	}
	return result
}

func TestWriteRefsNotes(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newWrefServer(t, e)
		fx := wrefSeedFixture(t, e)
		ctx := context.Background()

		// showNote re-materializes the cache (hydrating API-published
		// packs) and reads the note back with plain git — end-to-end
		// durability, not just a ref flip.
		showNote := func(t *testing.T, notesRef, sha string) (string, error) {
			t.Helper()
			repoPath, err := gitread.Materialize(ctx, e.refStore, e.blobStore, e.cacheDir, fx.repoID)
			if err != nil {
				t.Fatal(err)
			}
			out, err := wrefGit(ctx, repoPath, nil, nil, "notes", "--ref", notesRef, "show", sha)
			return out, err
		}

		t.Run("add creates the default notes ref", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1, "action": "add", "note": "reviewed"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d body %v", resp.StatusCode, body)
			}
			result := wrefNoteResultOf(t, body)
			if result["success"] != true || result["status"] != "ok" {
				t.Fatalf("result = %v", result)
			}
			if body["target_ref"] != "refs/notes/commits" {
				t.Fatalf("target_ref = %v", body["target_ref"])
			}
			if _, hasBase := body["base_commit"]; hasBase {
				t.Fatalf("base_commit present on unborn notes ref: %v", body)
			}
			newRef := body["new_ref_sha"].(string)
			if got := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]; got != newRef {
				t.Fatalf("refs/notes/commits = %q, want %s", got, newRef)
			}
			out, err := showNote(t, "refs/notes/commits", fx.c1)
			if err != nil || !strings.Contains(out, "reviewed") {
				t.Fatalf("note show = %q err %v", out, err)
			}
		})

		t.Run("add on existing note conflicts", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1, "action": "add", "note": "again"})
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("status = %d body %v, want 409", resp.StatusCode, body)
			}
			if result := wrefNoteResultOf(t, body); result["status"] != "conflict" || result["success"] != false {
				t.Fatalf("result = %v", result)
			}
		})

		t.Run("append appends and add works for another object", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1, "action": "append", "note": "second line"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("append: status = %d body %v", resp.StatusCode, body)
			}
			if body["base_commit"] == nil || body["base_commit"] == "" {
				t.Fatalf("append on existing ref must report base_commit: %v", body)
			}
			out, err := showNote(t, "refs/notes/commits", fx.c1)
			if err != nil || !strings.Contains(out, "reviewed") || !strings.Contains(out, "second line") {
				t.Fatalf("note show = %q err %v", out, err)
			}
			// Notes for other objects on the same ref must be preserved.
			resp, _ = wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c2, "note": "on c2"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("add c2: status = %d", resp.StatusCode)
			}
			if out, err := showNote(t, "refs/notes/commits", fx.c1); err != nil || !strings.Contains(out, "reviewed") {
				t.Fatalf("c1 note lost after writing c2 note: %q err %v", out, err)
			}
		})

		t.Run("append creates on a fresh ref", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1, "action": "append", "note": "created by append", "ref": "reviews"})
			if resp.StatusCode != http.StatusOK || body["target_ref"] != "refs/notes/reviews" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			out, err := showNote(t, "refs/notes/reviews", fx.c1)
			if err != nil || !strings.Contains(out, "created by append") {
				t.Fatalf("note show = %q err %v", out, err)
			}
		})

		t.Run("expected_ref_sha guards", func(t *testing.T) {
			cur := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]
			if cur == "" {
				t.Fatal("missing notes ref")
			}
			// Mismatch -> 412 precondition_failed.
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c2, "action": "append", "note": "x", "expected_ref_sha": fx.c1})
			if resp.StatusCode != http.StatusPreconditionFailed {
				t.Fatalf("status = %d body %v, want 412", resp.StatusCode, body)
			}
			if result := wrefNoteResultOf(t, body); result["status"] != "precondition_failed" {
				t.Fatalf("result = %v", result)
			}
			if body["new_ref_sha"] != cur {
				t.Fatalf("412 new_ref_sha = %v, want current tip %s", body["new_ref_sha"], cur)
			}
			// Matching guard succeeds.
			resp, _ = wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c2, "action": "append", "note": "guarded", "expected_ref_sha": cur})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("guarded append: status = %d", resp.StatusCode)
			}
			// Zero-sha guard asserts an unborn ref.
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1, "note": "x", "ref": "fresh", "expected_ref_sha": ref.ZeroSHA})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("zero guard on unborn ref: status = %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("delete note and delete missing", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "DELETE", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("delete: status = %d body %v", resp.StatusCode, body)
			}
			if out, err := showNote(t, "refs/notes/commits", fx.c1); err == nil {
				t.Fatalf("note still present after delete: %q", out)
			}
			// The other object's note survives the removal commit.
			if out, err := showNote(t, "refs/notes/commits", fx.c2); err != nil || !strings.Contains(out, "on c2") {
				t.Fatalf("c2 note lost by c1 delete: %q err %v", out, err)
			}
			resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("second delete: status = %d body %v, want 404", resp.StatusCode, body)
			}
			if result := wrefNoteResultOf(t, body); result["status"] != "not_found" {
				t.Fatalf("result = %v", result)
			}
		})

		t.Run("invalid input", func(t *testing.T) {
			for name, req := range map[string]map[string]any{
				"missing sha":        {"note": "x"},
				"short sha":          {"sha": "abcd", "note": "x"},
				"bad action":         {"sha": fx.c1, "action": "replace", "note": "x"},
				"missing note":       {"sha": fx.c1, "action": "add"},
				"non-notes full ref": {"sha": fx.c1, "note": "x", "ref": "refs/heads/main"},
				"bad expected sha":   {"sha": fx.c1, "note": "x", "expected_ref_sha": "xyz"},
				"author sans email":  {"sha": fx.c1, "note": "x", "author": map[string]any{"name": "n"}},
			} {
				resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token, req)
				if resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status = %d body %v, want 400", name, resp.StatusCode, body)
					continue
				}
				if result := wrefNoteResultOf(t, body); result["status"] != "invalid" {
					t.Errorf("%s: result = %v", name, result)
				}
			}
			// Unknown target object -> 404 not_found.
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": strings.Repeat("ab", 20), "note": "x"})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("unknown object: status = %d body %v, want 404", resp.StatusCode, body)
			}
		})

		t.Run("author is recorded", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c2, "note": "authored", "ref": "authored",
					"author": map[string]any{"name": "Review Bot", "email": "review@example.com"}})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d body %v", resp.StatusCode, body)
			}
			repoPath, err := gitread.Materialize(ctx, e.refStore, e.blobStore, e.cacheDir, fx.repoID)
			if err != nil {
				t.Fatal(err)
			}
			out, err := wrefGit(ctx, repoPath, nil, nil, "log", "-1", "--format=%an <%ae>", "refs/notes/authored")
			if err != nil || strings.TrimSpace(out) != "Review Bot <review@example.com>" {
				t.Fatalf("notes commit author = %q err %v", out, err)
			}
		})
	})
}

func TestWriteRefsPolicies(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newWrefServer(t, e)
		fx := wrefSeedFixture(t, e)

		mintWith := func(refsJSON string, ops []string) string {
			return e.mint(t, tokenSpec{iss: fx.org, repo: fx.url,
				scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}, refs: refsJSON, ops: ops})
		}

		t.Run("no-push denies tag create", func(t *testing.T) {
			tok := mintWith(`[["refs/tags/*", ["no-push"]]]`, nil)
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/tags", tok,
				map[string]any{"name": "blocked", "target": fx.c1})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", resp.StatusCode)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)["refs/tags/blocked"]; exists {
				t.Fatal("denied tag was created")
			}
		})

		t.Run("no-push denies notes write with envelope", func(t *testing.T) {
			tok := mintWith(`[["refs/notes/*", ["no-push"]]]`, nil)
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", tok,
				map[string]any{"sha": fx.c1, "note": "blocked"})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d body %v, want 403", resp.StatusCode, body)
			}
			if result := wrefNoteResultOf(t, body); result["status"] != "forbidden" || result["success"] != false {
				t.Fatalf("result = %v", result)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]; exists {
				t.Fatal("denied notes ref was created")
			}
		})

		t.Run("no-force-push denies branch delete but allows create", func(t *testing.T) {
			tok := mintWith(`[["refs/heads/*", ["no-force-push"]]]`, nil)
			resp, _ := wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", tok,
				map[string]any{"name": "feature"})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("delete: status = %d, want 403", resp.StatusCode)
			}
			resp, _ = wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", tok,
				map[string]any{"target_branch": "nfp-ok", "base_branch": "main"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("create: status = %d, want 200 (creation is fast-forward)", resp.StatusCode)
			}
		})

		t.Run("deprecated ops claim folds in", func(t *testing.T) {
			tok := mintWith("", []string{"no-push"})
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", tok,
				map[string]any{"target_branch": "ops-denied", "base_branch": "main"})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", resp.StatusCode)
			}
		})

		t.Run("ephemeral namespace quirk", func(t *testing.T) {
			// A refs/heads/* rule does not police the ephemeral namespace
			// (hook parity: match against the namespaced storage name).
			tok := mintWith(`[["refs/heads/*", ["no-push"]]]`, nil)
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", tok,
				map[string]any{"target_branch": "eph-free", "base_branch": "main", "target_is_ephemeral": true})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("ephemeral create: status = %d, want 200", resp.StatusCode)
			}
			resp, _ = wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", tok,
				map[string]any{"target_branch": "pers-denied", "base_branch": "main"})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("persistent create: status = %d, want 403", resp.StatusCode)
			}
		})

		t.Run("verify-sig fails closed on server-created notes", func(t *testing.T) {
			tok := mintWith(`[["refs/notes/*", ["verify-sig"]]]`, nil)
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", tok,
				map[string]any{"sha": fx.c1, "note": "unsigned"})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d body %v, want 403", resp.StatusCode, body)
			}
		})

		t.Run("malformed refs claim fails closed", func(t *testing.T) {
			tok := mintWith(`[["refs/heads/*"]]`, nil) // tuple arity error
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", tok,
				map[string]any{"target_branch": "bad-claim", "base_branch": "main"})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", resp.StatusCode)
			}
		})
	})
}

// TestWriteRefsAuth exercises the middleware wiring of the write routes:
// missing token 401, missing scope 403, unknown repo 404.
func TestWriteRefsAuth(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newWrefServer(t, e)
		fx := wrefSeedFixture(t, e)

		req, err := http.NewRequest("POST", srv.URL+"/api/v1/repos/tags", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("no token: status = %d, want 401", resp.StatusCode)
		}

		readOnly := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitRead}})
		for _, rt := range []struct{ method, path string }{
			{"POST", "/api/v1/repos/branches/create"},
			{"DELETE", "/api/v1/repos/branches"},
			{"POST", "/api/v1/repos/tags"},
			{"POST", "/api/v1/repos/notes"},
			{"DELETE", "/api/v1/repos/notes"},
		} {
			resp, _ := wrefDo(t, srv, rt.method, rt.path, readOnly, map[string]any{})
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s %s with git:read only: status = %d, want 403", rt.method, rt.path, resp.StatusCode)
			}
		}

		ghost := e.mint(t, tokenSpec{iss: fx.org, repo: "https://example.com/" + fx.org + "/ghost",
			scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
		resp2, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", ghost,
			map[string]any{"target_branch": "x", "base_branch": "main"})
		if resp2.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown repo: status = %d, want 404", resp2.StatusCode)
		}
	})
}
