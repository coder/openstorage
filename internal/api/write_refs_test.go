package api

// Contract integration tests for the W3 write endpoints (docs/archive/phases/07-implementation-roadmap.md p4):
// POST /repos/branches/create, DELETE /repos/branches, POST/DELETE
// /repos/tags, POST/DELETE /repos/notes — fanned out over the pg and
// sqlite lanes via runBackends.
//
// api.New registers these routes (api.go); each lane still builds a
// dedicated httptest server through registerWriteRefRoutes on top of the
// SAME lane stores so the write surface is exercised in isolation — reads
// through the lane's regular server (e.srv) therefore double as
// cross-surface assertions. Fixtures seed durable state exactly like the read tests
// (coreGit + coreSeedStores). All helpers carry a wref prefix.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/gitread"
	"github.com/coder/code-storage/internal/ref"
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
	s.registerWriteRefRoutes(mux, auth.NewVerifier(e.store), "", nil)
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

		// Issue #17: target_branch is the current delete-branch field with
		// the legacy name kept as a deprecated alias; the response mirrors
		// the deleted name in both spellings.
		t.Run("delete branch body aliases", func(t *testing.T) {
			for _, b := range []string{"alias-a", "alias-b"} {
				resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
					map[string]any{"target_branch": b, "base_branch": "main"})
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("seed %s: status %d body %v", b, resp.StatusCode, body)
				}
			}
			// Current-name-only body.
			resp, body := wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", fx.token,
				map[string]any{"target_branch": "alias-a"})
			if resp.StatusCode != http.StatusOK || body["target_branch"] != "alias-a" || body["name"] != "alias-a" {
				t.Fatalf("target_branch-only delete: status %d body %v", resp.StatusCode, body)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)["refs/heads/alias-a"]; exists {
				t.Fatal("refs/heads/alias-a still present after alias delete")
			}
			// Both set: the current name wins, so the live branch named by
			// the deprecated alias must survive the 404.
			resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", fx.token,
				map[string]any{"target_branch": "no-such-branch", "name": "alias-b"})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("precedence delete: status %d body %v, want 404", resp.StatusCode, body)
			}
			if _, exists := wrefRefs(t, e, fx.repoID)["refs/heads/alias-b"]; !exists {
				t.Fatal("deprecated-name branch deleted although target_branch must win")
			}
			// Legacy-only body still works and the response now carries
			// target_branch.
			resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/branches", fx.token,
				map[string]any{"name": "alias-b"})
			if resp.StatusCode != http.StatusOK || body["target_branch"] != "alias-b" || body["name"] != "alias-b" {
				t.Fatalf("legacy delete: status %d body %v", resp.StatusCode, body)
			}
		})
	})
}

// TestWriteRefsBranchForcePreferredBase covers the phase-5 carry-overs on
// POST /repos/branches/create: force CAS-recreation of an existing branch
// (with the no-force-push interaction backed by a real ancestry check) and
// the preferred_base fallback.
func TestWriteRefsBranchForcePreferredBase(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newWrefServer(t, e)
		fx := wrefSeedFixture(t, e)

		t.Run("force recreates an existing branch", func(t *testing.T) {
			// Without force, an existing target conflicts.
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "feature", "base_branch": "main"})
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("no-force recreate: status = %d, want 409", resp.StatusCode)
			}
			// force moves feature (c1) to the main tip (c2): fast-forward.
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "feature", "base_branch": "main", "force": true})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c2 || body["message"] != "branch updated" {
				t.Fatalf("force ff: status %d body %v", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/heads/feature"]; got != fx.c2 {
				t.Fatalf("refs/heads/feature = %q, want %s", got, fx.c2)
			}
			// force moves it back to c1: non-fast-forward, allowed without a
			// no-force-push policy.
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "feature", "base_ref": fx.c1, "force": true})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c1 {
				t.Fatalf("force non-ff: status %d body %v", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/heads/feature"]; got != fx.c1 {
				t.Fatalf("refs/heads/feature = %q, want %s", got, fx.c1)
			}
		})

		t.Run("force create of a missing branch behaves like create", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "force-new", "base_branch": "main", "force": true})
			if resp.StatusCode != http.StatusOK || body["message"] != "branch created" || body["commit_sha"] != fx.c2 {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("no-force-push denies non-fast-forward force move", func(t *testing.T) {
			tok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url,
				scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite},
				refs:   `[["refs/heads/guarded", ["no-force-push"]]]`})
			// Creation is always fast-forward: allowed under the policy.
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", tok,
				map[string]any{"target_branch": "guarded", "base_branch": "main"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("guarded create: status %d body %v", resp.StatusCode, body)
			}
			// Non-fast-forward force move (c2 -> c1): denied 403.
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", tok,
				map[string]any{"target_branch": "guarded", "base_ref": fx.c1, "force": true})
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("guarded non-ff force: status %d body %v, want 403", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/heads/guarded"]; got != fx.c2 {
				t.Fatalf("refs/heads/guarded = %q after denied force, want %s", got, fx.c2)
			}
			// Fast-forward force moves stay allowed: rewind with the
			// unpoliced token, then force-forward under the policy.
			resp, _ = wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "guarded", "base_ref": fx.c1, "force": true})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("rewind guarded: status %d", resp.StatusCode)
			}
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", tok,
				map[string]any{"target_branch": "guarded", "base_ref": fx.c2, "force": true})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c2 {
				t.Fatalf("guarded ff force: status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("preferred_base backs an unresolvable primary base", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "pb-fallback", "base_branch": "no-such-branch", "preferred_base": "main"})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c2 {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/heads/pb-fallback"]; got != fx.c2 {
				t.Fatalf("refs/heads/pb-fallback = %q, want %s", got, fx.c2)
			}
		})

		t.Run("preferred_base alone is a valid base", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "pb-only", "preferred_base": fx.c1})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c1 {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("primary base wins over preferred_base", func(t *testing.T) {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "pb-loses", "base_ref": fx.c1, "preferred_base": "main"})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c1 {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("unresolvable primary and preferred base is 404", func(t *testing.T) {
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "pb-none", "base_branch": "nope", "preferred_base": "also-nope"})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", resp.StatusCode)
			}
		})

		t.Run("missing base and preferred_base is 400", func(t *testing.T) {
			resp, _ := wrefDo(t, srv, "POST", "/api/v1/repos/branches/create", fx.token,
				map[string]any{"target_branch": "pb-missing"})
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
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
			repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
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
			// docs/reference/http-api.md: DELETE /repos/tags demands git:read AND git:write.
			for _, scopes := range [][]string{{auth.ScopeGitWrite}, {auth.ScopeGitRead}} {
				tok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: scopes})
				resp, _ := wrefDo(t, srv, "DELETE", "/api/v1/repos/tags", tok,
					map[string]any{"name": "v1.0.0"})
				if resp.StatusCode != http.StatusForbidden {
					t.Errorf("scopes %v: status = %d, want 403", scopes, resp.StatusCode)
				}
			}
		})

		// Issue #17: ref is the current create-tag field with target kept
		// as the deprecated alias, the current name winning on conflict.
		t.Run("create tag body aliases", func(t *testing.T) {
			// Current-name-only body.
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "t17-ref", "ref": fx.c1})
			if resp.StatusCode != http.StatusOK || body["sha"] != fx.c1 {
				t.Fatalf("ref-only: status %d body %v", resp.StatusCode, body)
			}
			// ref keeps resolving branch names (openapi: branch, tag, or
			// commit), peeled to the branch tip.
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "t17-branch", "ref": "main"})
			if resp.StatusCode != http.StatusOK || body["sha"] != fx.c2 {
				t.Fatalf("branch ref: status %d body %v, want main tip %s", resp.StatusCode, body, fx.c2)
			}
			// Both set: ref (c1) wins over the deprecated target (c2).
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "t17-both", "ref": fx.c1, "target": fx.c2})
			if resp.StatusCode != http.StatusOK || body["sha"] != fx.c1 {
				t.Fatalf("precedence: status %d body %v, want ref to win with %s", resp.StatusCode, body, fx.c1)
			}
			// Neither spelling → 400.
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/tags", fx.token,
				map[string]any{"name": "t17-none"})
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("missing ref: status %d body %v, want 400", resp.StatusCode, body)
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
			repoPath, err := gitread.Materialize(ctx, e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
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
			repoPath, err := gitread.Materialize(ctx, e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
			if err != nil {
				t.Fatal(err)
			}
			out, err := wrefGit(ctx, repoPath, nil, nil, "log", "-1", "--format=%an <%ae>", "refs/notes/authored")
			if err != nil || strings.TrimSpace(out) != "Review Bot <review@example.com>" {
				t.Fatalf("notes commit author = %q err %v", out, err)
			}
		})

		// Issue #17: object_ref/notes_ref/expected_notes_ref_sha are the
		// current fields, aliasing the deprecated sha/ref/expected_ref_sha
		// (current name wins on conflict); responses now carry notes_ref
		// alongside the deprecated target_ref mirror.
		t.Run("notes body aliases", func(t *testing.T) {
			// Current-name-only body.
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"object_ref": fx.c2, "note": "alias add", "notes_ref": "alias-notes"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("current-name add: status %d body %v", resp.StatusCode, body)
			}
			if body["notes_ref"] != "refs/notes/alias-notes" || body["target_ref"] != "refs/notes/alias-notes" {
				t.Fatalf("response refs = %v/%v, want both refs/notes/alias-notes", body["notes_ref"], body["target_ref"])
			}
			if body["sha"] != fx.c2 {
				t.Fatalf("sha = %v, want %s", body["sha"], fx.c2)
			}
			out, err := showNote(t, "refs/notes/alias-notes", fx.c2)
			if err != nil || !strings.Contains(out, "alias add") {
				t.Fatalf("note show = %q err %v", out, err)
			}

			// expected_notes_ref_sha guards, winning over a stale value in
			// the deprecated expected_ref_sha spelling.
			tip := wrefRefs(t, e, fx.repoID)["refs/notes/alias-notes"]
			if tip == "" {
				t.Fatal("missing alias-notes ref")
			}
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"object_ref": fx.c2, "action": "append", "note": "guarded",
					"notes_ref": "alias-notes", "expected_notes_ref_sha": tip, "expected_ref_sha": fx.c1})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("guarded append: status %d body %v, want expected_notes_ref_sha to win", resp.StatusCode, body)
			}

			// DELETE precedence: current names (c2 note on alias-notes)
			// must win over deprecated values naming a noteless object on
			// another live ref.
			commitsTip := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]
			resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/notes", fx.token,
				map[string]any{"object_ref": fx.c2, "sha": fx.c1, "notes_ref": "alias-notes", "ref": "commits"})
			if resp.StatusCode != http.StatusOK || body["notes_ref"] != "refs/notes/alias-notes" || body["sha"] != fx.c2 {
				t.Fatalf("precedence delete: status %d body %v", resp.StatusCode, body)
			}
			if out, err := showNote(t, "refs/notes/alias-notes", fx.c2); err == nil {
				t.Fatalf("alias-notes note still present after delete: %q", out)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]; got != commitsTip {
				t.Fatalf("refs/notes/commits moved to %q although notes_ref must win", got)
			}

			// Legacy-only body still works; its response carries notes_ref.
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1, "note": "legacy body", "ref": "alias-notes"})
			if resp.StatusCode != http.StatusOK || body["notes_ref"] != "refs/notes/alias-notes" || body["sha"] != fx.c1 {
				t.Fatalf("legacy add: status %d body %v", resp.StatusCode, body)
			}

			// Neither object spelling → 400 (result envelope).
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"note": "x", "notes_ref": "alias-notes"})
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("missing object_ref: status %d body %v, want 400", resp.StatusCode, body)
			}
			if result := wrefNoteResultOf(t, body); result["status"] != "invalid" {
				t.Fatalf("result = %v", result)
			}
		})

		// Issue #161: object_ref accepts revisions — a branch, tag, short
		// SHA, or full ref resolving to a commit — alongside the unchanged
		// full-40-hex fast path (which still notes any object kind).
		// Responses echo the resolved id in `sha`.
		t.Run("object_ref revisions", func(t *testing.T) {
			for _, c := range []struct {
				name, objectRef, wantSHA string
			}{
				{"branch name", "feature", fx.c1},
				{"annotated tag peels to its commit", "annot", fx.c2},
				{"short sha", fx.c1[:8], fx.c1},
				{"full ref", "refs/heads/feature", fx.c1},
			} {
				t.Run(c.name, func(t *testing.T) {
					resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
						map[string]any{"object_ref": c.objectRef, "note": "via " + c.name, "notes_ref": "rev-notes"})
					if resp.StatusCode != http.StatusOK {
						t.Fatalf("add: status = %d body %v", resp.StatusCode, body)
					}
					if body["sha"] != c.wantSHA {
						t.Fatalf("add sha = %v, want resolved %s", body["sha"], c.wantSHA)
					}
					if out, err := showNote(t, "refs/notes/rev-notes", c.wantSHA); err != nil || !strings.Contains(out, "via "+c.name) {
						t.Fatalf("note show = %q err %v", out, err)
					}
					resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/notes", fx.token,
						map[string]any{"object_ref": c.objectRef, "notes_ref": "rev-notes"})
					if resp.StatusCode != http.StatusOK || body["sha"] != c.wantSHA {
						t.Fatalf("delete: status = %d body %v, want 200 with sha %s", resp.StatusCode, body, c.wantSHA)
					}
					if out, err := showNote(t, "refs/notes/rev-notes", c.wantSHA); err == nil {
						t.Fatalf("note still present after delete: %q", out)
					}
				})
			}

			// A non-resolving revision is a clear 404 in the note-write
			// envelope, for POST and DELETE alike.
			for _, method := range []string{"POST", "DELETE"} {
				req := map[string]any{"object_ref": "no-such-rev", "notes_ref": "rev-notes"}
				if method == "POST" {
					req["note"] = "x"
				}
				resp, body := wrefDo(t, srv, method, "/api/v1/repos/notes", fx.token, req)
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("%s no-such-rev: status = %d body %v, want 404", method, resp.StatusCode, body)
				}
				if result := wrefNoteResultOf(t, body); result["status"] != "not_found" {
					t.Fatalf("%s no-such-rev: result = %v", method, result)
				}
			}

			// Unchanged 40-hex fast path: the annotated tag OBJECT takes a
			// note that the peeled tag revision above never touched.
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"object_ref": fx.tagObj, "note": "on tag object", "notes_ref": "rev-notes"})
			if resp.StatusCode != http.StatusOK || body["sha"] != fx.tagObj {
				t.Fatalf("tag object add: status = %d body %v", resp.StatusCode, body)
			}
			if out, err := showNote(t, "refs/notes/rev-notes", fx.tagObj); err != nil || !strings.Contains(out, "on tag object") {
				t.Fatalf("tag object note show = %q err %v", out, err)
			}

			// PR #163 review (P2): resolution precedes the CAS guard — a
			// mismatched guard 412s with the RESOLVED id in the envelope,
			// and an unresolvable revision 404s despite a stale guard.
			resp, body = wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"object_ref": "feature", "note": "x", "notes_ref": "rev-notes", "expected_notes_ref_sha": fx.c2})
			if resp.StatusCode != http.StatusPreconditionFailed || body["sha"] != fx.c1 {
				t.Fatalf("guarded revision: status = %d sha %v, want 412 with resolved %s", resp.StatusCode, body["sha"], fx.c1)
			}
			resp, body = wrefDo(t, srv, "DELETE", "/api/v1/repos/notes", fx.token,
				map[string]any{"object_ref": "no-such-rev", "notes_ref": "rev-notes", "expected_notes_ref_sha": fx.c2})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("unresolvable with stale guard: status = %d body %v, want 404", resp.StatusCode, body)
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

		// Exercises the real wrefIsAncestor fast-forward evaluation on
		// notes refs: every notes op (add/append/remove) advances
		// refs/notes/commits by a child commit, so all must pass the
		// no-force-push ancestry check. Non-fast-forward transitions are
		// unreachable through this API by construction; that denial branch
		// is covered by TestWrefPolicyCheckNotesNonFastForward.
		t.Run("no-force-push allows fast-forward note ops", func(t *testing.T) {
			tok := mintWith(`[["refs/notes/*", ["no-force-push"]]]`, nil)
			for _, step := range []map[string]any{
				{"sha": fx.c2, "action": "add", "note": "nfp v1"},
				{"sha": fx.c2, "action": "append", "note": "nfp v2"},
			} {
				resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", tok, step)
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("%v: status = %d body %v", step["action"], resp.StatusCode, body)
				}
			}
			// remove also appends a notes commit: fast-forward, allowed.
			resp, body := wrefDo(t, srv, "DELETE", "/api/v1/repos/notes", tok,
				map[string]any{"sha": fx.c2})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("remove: status = %d body %v", resp.StatusCode, body)
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

// TestWrefPolicyCheckNotesNonFastForward unit-tests the no-force-push
// denial branch for notes refs. Non-fast-forward notes transitions are
// unreachable through the HTTP API (every notes op appends a commit), so
// the deny path is exercised directly with a stubbed ancestry check —
// it still guards hypothetical future callers (e.g. a raw ref-set API).
func TestWrefPolicyCheckNotesNonFastForward(t *testing.T) {
	claims := &auth.Claims{Refs: json.RawMessage(`[["refs/notes/*", ["no-force-push"]]]`)}
	oldSHA := strings.Repeat("a", 40)
	newSHA := strings.Repeat("b", 40)

	deny, err := wrefPolicyCheck(claims, "refs/notes/commits", oldSHA, newSHA,
		func(_, _ string) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(deny, "no-force-push denies non-fast-forward") {
		t.Fatalf("deny = %q, want non-fast-forward denial", deny)
	}

	deny, err = wrefPolicyCheck(claims, "refs/notes/commits", oldSHA, newSHA,
		func(_, _ string) (bool, error) { return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	if deny != "" {
		t.Fatalf("deny = %q, want allowed fast-forward", deny)
	}
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

// TestNotesQuota (issue #21, Codex round 12): POST /repos/notes publishes
// packs through the shared publishPack step, so the per-repo quota gates it
// exactly like commit-pack/merge/pre-receive — without this, repeated note
// appends grow packs/ without bound after every other writer is denied.
func TestNotesQuota(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		quotaSrv := func(quota int64) *httptest.Server {
			s := &server{
				store:          e.store,
				httpBaseURL:    testHTTPBaseURL,
				logger:         discardLogger(),
				refStore:       e.refStore,
				blobStore:      e.blobStore,
				cacheDir:       e.cacheDir,
				packQuotaBytes: quota,
			}
			mux := http.NewServeMux()
			s.registerWriteRefRoutes(mux, auth.NewVerifier(e.store), "", nil)
			srv := httptest.NewServer(recoverPanics(s.logger)(mux))
			t.Cleanup(srv.Close)
			return srv
		}

		// Generous quota: the note write passes.
		resp, body := wrefDo(t, quotaSrv(1<<20), "POST", "/api/v1/repos/notes", fx.token,
			map[string]any{"sha": fx.c1, "action": "add", "note": "under quota"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("under-quota note = %d body %v", resp.StatusCode, body)
		}
		usage, err := blob.StorageUsageBytes(context.Background(), e.blobStore, fx.repoID)
		if err != nil || usage == 0 {
			t.Fatalf("usage after note = (%d, %v), want > 0", usage, err)
		}

		// 1-byte quota: denied with the note envelope's 403, nothing stored.
		resp, body = wrefDo(t, quotaSrv(1), "POST", "/api/v1/repos/notes", fx.token,
			map[string]any{"sha": fx.c2, "action": "add", "note": "over quota"})
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("over-quota note = %d body %v", resp.StatusCode, body)
		}
		result := wrefNoteResultOf(t, body)
		if result["success"] != false || !strings.Contains(result["message"].(string), "quota") {
			t.Fatalf("result = %v, want quota failure", result)
		}
		after, err := blob.StorageUsageBytes(context.Background(), e.blobStore, fx.repoID)
		if err != nil || after != usage {
			t.Fatalf("denied note changed usage: %d -> %d (err %v)", usage, after, err)
		}
	})
}
