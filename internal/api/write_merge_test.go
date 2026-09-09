package api

// Contract integration tests for the merge write endpoints (docs/archive/phases/07-implementation-roadmap.md p4):
// POST /repos/merge and GET /repos/merge/preview — fanned out over the pg
// and sqlite lanes via runBackends.
//
// api.New registers these routes (api.go); each lane still builds a
// dedicated httptest server through registerWriteMergeRoutes with the
// PRODUCTION requireAuth middleware, on top of the SAME lane stores so the
// merge surface is exercised in isolation — reads through the lane's regular server
// (e.srv) therefore double as cross-surface assertions. Fixtures seed
// durable state exactly like the read tests (coreGit + coreSeedStores).
// All helpers carry a wm prefix.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/ref"
)

// wmServer mounts ONLY the merge routes with the production auth
// middleware, sharing the lane's stores and cache dir with e.srv.
func wmServer(t *testing.T, e *testEnv) *httptest.Server {
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
	s.registerWriteMergeRoutes(mux, auth.NewVerifier(e.store), "", nil)
	srv := httptest.NewServer(recoverPanics(s.logger)(mux))
	t.Cleanup(srv.Close)
	return srv
}

// wmDo sends one request to the merge server and returns the response and
// raw body. A nil body sends no payload.
func wmDo(t *testing.T, srv *httptest.Server, method, path, token string, body any) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
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
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	return resp, respBody
}

// wmDecode unmarshals a response body into a generic map for contract-
// shape assertions (key presence, JSON number types).
func wmDecode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	decoded := map[string]any{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode body %q: %v", body, err)
	}
	return decoded
}

// wmResult extracts the failure envelope's result object.
func wmResult(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	res, ok := body["result"].(map[string]any)
	if !ok {
		t.Fatalf("body has no result object: %v", body)
	}
	return res
}

// wmRefsSnapshot snapshots the ref authority for the fixture repo.
func wmRefsSnapshot(t *testing.T, e *testEnv, repoID string) map[string]string {
	t.Helper()
	refs, err := e.refStore.ListRefs(context.Background(), repoID)
	if err != nil {
		t.Fatalf("list refs: %v", err)
	}
	return refs
}

// wmMergeBody builds a merge request body; extra overrides/augments.
func wmMergeBody(source, target, strategy string, extra map[string]any) map[string]any {
	body := map[string]any{
		"source_branch": source,
		"target_branch": target,
		"strategy":      strategy,
	}
	maps.Copy(body, extra)
	return body
}

// wmTestAuthor is the canonical merge-commit ident for these tests.
var wmTestAuthor = map[string]any{"name": "wm", "email": "wm@test.invalid"}

// wmParents returns the parent shas of a commit from the (re-)materialized
// cache repo — proving the published pack is durably registered (the
// commit is readable back through Materialize, not just the request
// scratch).
func wmParents(t *testing.T, e *testEnv, repoID, commitSHA string) []string {
	t.Helper()
	repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, repoID, "main")
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	out := strings.TrimSpace(coreGit(t, repoPath, "log", "-1", "--format=%P", commitSHA))
	if out == "" {
		return nil
	}
	return strings.Fields(out)
}

// wmTargets are the pre-seeded target branches (all at the main tip c2) so
// mutation tests stay independent of each other.
var wmTargets = []string{
	"t-clean", "t-ffonly", "t-ffprefer-ff", "t-ffprefer-merge", "t-noff",
	"t-squash", "t-eph-ff", "t-eph-merge", "t-unrel",
	"t-pol-nopush", "t-pol-nff", "t-pol-vs-ff", "t-pol-vs-merge",
	"t-alias-ref", "t-alias-refsheads", "t-alias-both",
}

// wmFixture is one seeded repo for the merge tests.
type wmFixture struct {
	repoID string
	url    string
	org    string
	token  string // git:read+git:write JWT
	work   string // fixture work-tree path

	c1, c2      string // main history: c1 root, c2 = main tip
	featTip     string // feature: c1 + feat.txt (clean 3-way vs main)
	conflictTip string // conflict: c1 + conflicting a.txt
	ffMid       string // ffsrc first commit (c2 + ff1.txt)
	ffTip       string // ffsrc tip (ffMid + ff2.txt) — fast-forwardable from c2
	multiTip    string // multi: c1 + 2 commits (squash source)
	scratchTip  string // ephemeral "scratch": c2 + 2 commits
	scratch2Tip string // ephemeral "scratch2": c1 + 1 commit
	orphanTip   string // unrelated root commit
	binMainTip  string // binmain: c1 + binary bin.dat change
	binSideTip  string // binside: c1 + different binary bin.dat change
	bigMainTip  string // bigmain: c1 + small big.txt change
	bigSideTip  string // bigside: c1 + >1MiB big.txt change
	subMainTip  string // submain: c1 + gitlink vendor/dep -> 2222…
	subSideTip  string // subside: c1 + gitlink vendor/dep -> 3333…
}

// wmSeedFixture builds the merge test zoo. Every t-* target branch points
// at c2 (the main tip) so each mutating subtest gets its own target.
func wmSeedFixture(t *testing.T, e *testEnv) wmFixture {
	t.Helper()
	ctx := context.Background()
	org := e.newOrg(t)
	url := "https://example.com/" + org + "/wm-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(ctx, org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	coreWriteFile(t, dir, "a.txt", "base\n")
	coreWriteFile(t, dir, "bin.dat", "\x00\x01bin base\n")
	coreWriteFile(t, dir, "big.txt", "big base\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c1")
	c1 := coreRevParse(t, dir, "HEAD")

	coreWriteFile(t, dir, "a.txt", "main\n")
	coreWriteFile(t, dir, "m.txt", "m\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c2")
	c2 := coreRevParse(t, dir, "HEAD")

	commitOn := func(branch, from, file, content, msg string) string {
		coreGit(t, dir, "checkout", "-qb", branch, from)
		coreWriteFile(t, dir, file, content)
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", msg)
		return coreRevParse(t, dir, "HEAD")
	}
	featTip := commitOn("feature", c1, "feat.txt", "feat\n", "feat")
	conflictTip := commitOn("conflict", c1, "a.txt", "other\n", "conflicting a.txt")
	ffMid := commitOn("ffsrc", c2, "ff1.txt", "ff1\n", "ff1")
	coreWriteFile(t, dir, "ff2.txt", "ff2\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "ff2")
	ffTip := coreRevParse(t, dir, "HEAD")
	commitOn("multi", c1, "sq1.txt", "sq1\n", "sq1")
	coreWriteFile(t, dir, "sq2.txt", "sq2\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "sq2")
	multiTip := coreRevParse(t, dir, "HEAD")
	commitOn("scratch", c2, "s1.txt", "s1\n", "s1")
	coreWriteFile(t, dir, "s2.txt", "s2\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "s2")
	scratchTip := coreRevParse(t, dir, "HEAD")
	scratch2Tip := commitOn("scratch2", c1, "u1.txt", "u1\n", "u1")
	binMainTip := commitOn("binmain", c1, "bin.dat", "\x00\x02bin main\n", "bin main")
	binSideTip := commitOn("binside", c1, "bin.dat", "\x00\x03bin side\n", "bin side")
	bigMainTip := commitOn("bigmain", c1, "big.txt", "big main\n", "big main")
	bigSideTip := commitOn("bigside", c1, "big.txt", strings.Repeat("x", (1<<20)+64)+"\n", "big side")

	// Gitlink (submodule) add/add conflict: both sides add vendor/dep with
	// different commit oids that exist in no odb — the vendor scenario.
	gitlinkOn := func(branch, sha string) string {
		coreGit(t, dir, "checkout", "-qb", branch, c1)
		coreGit(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+sha+",vendor/dep")
		coreGit(t, dir, "commit", "-q", "-m", branch)
		return coreRevParse(t, dir, "HEAD")
	}
	subMainTip := gitlinkOn("submain", strings.Repeat("2", 40))
	subSideTip := gitlinkOn("subside", strings.Repeat("3", 40))

	refs := map[string]string{
		"refs/heads/main":                 c2,
		"refs/heads/feature":              featTip,
		"refs/heads/conflict":             conflictTip,
		"refs/heads/ffsrc":                ffTip,
		"refs/heads/multi":                multiTip,
		"refs/heads/binmain":              binMainTip,
		"refs/heads/binside":              binSideTip,
		"refs/heads/bigmain":              bigMainTip,
		"refs/heads/bigside":              bigSideTip,
		"refs/heads/submain":              subMainTip,
		"refs/heads/subside":              subSideTip,
		"refs/heads/t-noop":               ffTip,
		auth.EphemeralRefName("scratch"):  scratchTip,
		auth.EphemeralRefName("scratch2"): scratch2Tip,
		auth.EphemeralRefName("et"):       c2,
	}
	for _, target := range wmTargets {
		refs["refs/heads/"+target] = c2
	}
	coreSeedStores(t, e, repo.RepoID, dir, refs)

	// The unrelated history is seeded from a second work repo: its pack
	// registers under the same repo id, its root commit shares nothing
	// with c1's history.
	orphanDir := t.TempDir()
	coreGit(t, orphanDir, "init", "-q", "-b", "main", ".")
	coreWriteFile(t, orphanDir, "o.txt", "orphan\n")
	coreGit(t, orphanDir, "add", ".")
	coreGit(t, orphanDir, "commit", "-q", "-m", "orphan root")
	orphanTip := coreRevParse(t, orphanDir, "HEAD")
	coreSeedStores(t, e, repo.RepoID, orphanDir, map[string]string{"refs/heads/orphan": orphanTip})

	token := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
	return wmFixture{
		repoID: repo.RepoID, url: url, org: org, token: token, work: dir,
		c1: c1, c2: c2, featTip: featTip, conflictTip: conflictTip,
		ffMid: ffMid, ffTip: ffTip, multiTip: multiTip,
		scratchTip: scratchTip, scratch2Tip: scratch2Tip, orphanTip: orphanTip,
		binMainTip: binMainTip, binSideTip: binSideTip,
		bigMainTip: bigMainTip, bigSideTip: bigSideTip,
		subMainTip: subMainTip, subSideTip: subSideTip,
	}
}

// ---------------------------------------------------------------------------

func TestMergeStrategies(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)

		t.Run("clean three-way merge with durable pack", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("feature", "t-clean", "merge", map[string]any{
					"author": wmTestAuthor, "commit_message": "custom merge msg",
				}))
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			body := wmDecode(t, raw)
			if body["result"] != "merge_commit" {
				t.Fatalf("result = %v", body["result"])
			}
			commit, _ := body["commit_sha"].(string)
			if !ref.ValidSHA(commit) {
				t.Fatalf("commit_sha = %v", body["commit_sha"])
			}
			if body["merge_base_sha"] != fx.c1 {
				t.Errorf("merge_base_sha = %v, want %s", body["merge_base_sha"], fx.c1)
			}
			if body["promoted_commits"] != float64(0) {
				t.Errorf("promoted_commits = %v, want 0", body["promoted_commits"])
			}
			source := body["source"].(map[string]any)
			if source["branch"] != "feature" || source["ephemeral"] != false || source["sha"] != fx.featTip {
				t.Errorf("source = %v", source)
			}
			target := body["target"].(map[string]any)
			if target["branch"] != "t-clean" || target["ephemeral"] != false ||
				target["old_sha"] != fx.c2 || target["new_sha"] != commit {
				t.Errorf("target = %v", target)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-clean"]; got != commit {
				t.Fatalf("t-clean tip = %q, want %s", got, commit)
			}
			// Durable-pack proof: re-materialize the cache from the stores
			// and read the merge commit back through real git.
			if parents := wmParents(t, e, fx.repoID, commit); !reflect.DeepEqual(parents, []string{fx.c2, fx.featTip}) {
				t.Fatalf("merge commit parents = %v, want [%s %s]", parents, fx.c2, fx.featTip)
			}
			repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
			if err != nil {
				t.Fatal(err)
			}
			if msg := strings.TrimSpace(coreGit(t, repoPath, "log", "-1", "--format=%s", commit)); msg != "custom merge msg" {
				t.Errorf("commit message = %q", msg)
			}
			if tree := coreRevParse(t, repoPath, commit+"^{tree}"); tree != body["tree_sha"] {
				t.Errorf("tree_sha = %v, want %s", body["tree_sha"], tree)
			}
			// Cross-surface (docs/explanation/git-protocol-and-namespaces.md interop): the read API sees the merged
			// tree immediately.
			rresp, rbody := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=feat.txt&ref=t-clean", fx.token, nil, "")
			if rresp.StatusCode != http.StatusOK || string(rbody) != "feat\n" {
				t.Errorf("read feat.txt = %d %q", rresp.StatusCode, rbody)
			}
			rresp, rbody = e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=a.txt&ref=t-clean", fx.token, nil, "")
			if rresp.StatusCode != http.StatusOK || string(rbody) != "main\n" {
				t.Errorf("read a.txt = %d %q, want main version", rresp.StatusCode, rbody)
			}
		})

		t.Run("ff_only fast-forwards with matching guard", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("ffsrc", "t-ffonly", "ff_only", map[string]any{
					"expected_target_sha": fx.c2,
				}))
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			body := wmDecode(t, raw)
			if body["result"] != "fast_forward" || body["commit_sha"] != fx.ffTip {
				t.Fatalf("body = %v", body)
			}
			if body["merge_base_sha"] != fx.c2 || body["promoted_commits"] != float64(0) {
				t.Errorf("merge_base/promoted = %v/%v", body["merge_base_sha"], body["promoted_commits"])
			}
			if wantTree := coreRevParse(t, fx.work, fx.ffTip+"^{tree}"); body["tree_sha"] != wantTree {
				t.Errorf("tree_sha = %v, want %s", body["tree_sha"], wantTree)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-ffonly"]; got != fx.ffTip {
				t.Fatalf("t-ffonly tip = %q, want %s", got, fx.ffTip)
			}
		})

		t.Run("ff_only rejects non-fast-forward", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("feature", "main", "ff_only", nil))
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			res := wmResult(t, wmDecode(t, raw))
			if res["status"] != "conflict" || res["success"] != false {
				t.Fatalf("result = %v", res)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/main"]; got != fx.c2 {
				t.Fatalf("main moved to %q after refused ff_only", got)
			}
		})

		t.Run("ff_prefer fast-forwards when possible", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("ffsrc", "t-ffprefer-ff", "ff_prefer", nil))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "fast_forward" || body["commit_sha"] != fx.ffTip {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
		})

		t.Run("ff_prefer falls back to a merge commit", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("feature", "t-ffprefer-merge", "ff_prefer", map[string]any{"author": wmTestAuthor}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "merge_commit" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			commit := body["commit_sha"].(string)
			if parents := wmParents(t, e, fx.repoID, commit); !reflect.DeepEqual(parents, []string{fx.c2, fx.featTip}) {
				t.Fatalf("parents = %v", parents)
			}
		})

		t.Run("merge strategy never fast-forwards", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("ffsrc", "t-noff", "merge", map[string]any{"author": wmTestAuthor}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "merge_commit" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			commit := body["commit_sha"].(string)
			if commit == fx.ffTip {
				t.Fatal("merge strategy fast-forwarded instead of creating a merge commit")
			}
			if parents := wmParents(t, e, fx.repoID, commit); !reflect.DeepEqual(parents, []string{fx.c2, fx.ffTip}) {
				t.Fatalf("parents = %v, want [%s %s]", parents, fx.c2, fx.ffTip)
			}
		})

		t.Run("no_op when target already contains source", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("main", "t-noop", "merge", nil))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "no_op" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			if body["commit_sha"] != fx.ffTip {
				t.Errorf("commit_sha = %v, want target tip %s", body["commit_sha"], fx.ffTip)
			}
			target := body["target"].(map[string]any)
			if target["old_sha"] != fx.ffTip || target["new_sha"] != fx.ffTip {
				t.Errorf("target = %v, want unchanged tips", target)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-noop"]; got != fx.ffTip {
				t.Fatalf("t-noop moved to %q on no_op", got)
			}
		})

		t.Run("squash collapses to a single-parent commit", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("multi", "t-squash", "merge", map[string]any{
					"squash": true, "author": wmTestAuthor,
				}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "squash" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			commit := body["commit_sha"].(string)
			// Single parent = the pre-merge target tip; the two source
			// commits stay out of the target history.
			if parents := wmParents(t, e, fx.repoID, commit); !reflect.DeepEqual(parents, []string{fx.c2}) {
				t.Fatalf("squash parents = %v, want [%s]", parents, fx.c2)
			}
			for _, f := range []string{"sq1.txt", "sq2.txt"} {
				rresp, rbody := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path="+f+"&ref=t-squash", fx.token, nil, "")
				if rresp.StatusCode != http.StatusOK {
					t.Errorf("read %s = %d %q", f, rresp.StatusCode, rbody)
				}
			}
		})
	})
}

func TestMergeConflictsAndGuards(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)
		ctx := context.Background()

		t.Run("conflict is 409 with paths and mutates nothing", func(t *testing.T) {
			refsBefore := wmRefsSnapshot(t, e, fx.repoID)
			packsBefore, err := e.refStore.ListPacks(ctx, fx.repoID)
			if err != nil {
				t.Fatal(err)
			}
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("conflict", "main", "merge", map[string]any{"author": wmTestAuthor}))
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			body := wmDecode(t, raw)
			res := wmResult(t, body)
			if res["status"] != "conflict" || res["old_sha"] != fx.c2 {
				t.Fatalf("result = %v", res)
			}
			if !reflect.DeepEqual(body["conflict_paths"], []any{"a.txt"}) {
				t.Fatalf("conflict_paths = %v, want [a.txt]", body["conflict_paths"])
			}
			if body["merge_base_sha"] != fx.c1 {
				t.Fatalf("merge_base_sha = %v, want %s", body["merge_base_sha"], fx.c1)
			}
			if refsAfter := wmRefsSnapshot(t, e, fx.repoID); !reflect.DeepEqual(refsBefore, refsAfter) {
				t.Fatal("refs changed on a conflicting merge")
			}
			packsAfter, err := e.refStore.ListPacks(ctx, fx.repoID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(packsBefore, packsAfter) {
				t.Fatal("packs registered on a conflicting merge")
			}
		})

		t.Run("unrelated histories rejected without flag", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("orphan", "t-unrel", "merge", map[string]any{"author": wmTestAuthor}))
			if resp.StatusCode != http.StatusConflict {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			res := wmResult(t, wmDecode(t, raw))
			if !strings.Contains(res["message"].(string), "unrelated") {
				t.Fatalf("message = %v", res["message"])
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-unrel"]; got != fx.c2 {
				t.Fatalf("t-unrel moved to %q", got)
			}
		})

		t.Run("unrelated histories merge with flag", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("orphan", "t-unrel", "merge", map[string]any{
					"author": wmTestAuthor, "allow_unrelated_histories": true,
				}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "merge_commit" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			if _, has := body["merge_base_sha"]; has {
				t.Fatalf("merge_base_sha present for unrelated histories: %v", body["merge_base_sha"])
			}
			commit := body["commit_sha"].(string)
			if parents := wmParents(t, e, fx.repoID, commit); !reflect.DeepEqual(parents, []string{fx.c2, fx.orphanTip}) {
				t.Fatalf("parents = %v", parents)
			}
			rresp, rbody := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=o.txt&ref=t-unrel", fx.token, nil, "")
			if rresp.StatusCode != http.StatusOK || string(rbody) != "orphan\n" {
				t.Errorf("read o.txt = %d %q", rresp.StatusCode, rbody)
			}
		})

		t.Run("stale expected_target_sha is 412 and leaves state", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("feature", "main", "merge", map[string]any{
					"author": wmTestAuthor, "expected_target_sha": fx.c1, // stale
				}))
			if resp.StatusCode != http.StatusPreconditionFailed {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			res := wmResult(t, wmDecode(t, raw))
			if res["status"] != "precondition_failed" || res["old_sha"] != fx.c2 {
				t.Fatalf("result = %v", res)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/main"]; got != fx.c2 {
				t.Fatalf("main moved to %q after 412", got)
			}
		})
	})
}

func TestMergeEphemeral(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)

		t.Run("fast-forward promotion counts every source commit", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("scratch", "t-eph-ff", "ff_prefer", map[string]any{
					"source_is_ephemeral": true,
				}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "fast_forward" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			if body["commit_sha"] != fx.scratchTip {
				t.Errorf("commit_sha = %v, want %s", body["commit_sha"], fx.scratchTip)
			}
			if body["promoted_commits"] != float64(2) {
				t.Fatalf("promoted_commits = %v, want 2", body["promoted_commits"])
			}
			if body["source"].(map[string]any)["ephemeral"] != true {
				t.Errorf("source.ephemeral = %v", body["source"])
			}
			// The promoted commits are readable through the persistent
			// namespace now (durable-pack + ref proof).
			if parents := wmParents(t, e, fx.repoID, fx.scratchTip); len(parents) != 1 {
				t.Fatalf("scratch tip parents = %v", parents)
			}
		})

		t.Run("merge-commit promotion excludes the created commit", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("scratch2", "t-eph-merge", "merge", map[string]any{
					"source_is_ephemeral": true, "author": wmTestAuthor,
				}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "merge_commit" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			if body["promoted_commits"] != float64(1) {
				t.Fatalf("promoted_commits = %v, want 1 (u1 only, not the merge commit)", body["promoted_commits"])
			}
		})

		t.Run("ephemeral target stays namespaced", func(t *testing.T) {
			before := wmRefsSnapshot(t, e, fx.repoID)
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("feature", "et", "merge", map[string]any{
					"target_is_ephemeral": true, "author": wmTestAuthor,
				}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "merge_commit" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			if body["target"].(map[string]any)["ephemeral"] != true {
				t.Errorf("target.ephemeral = %v", body["target"])
			}
			commit := body["commit_sha"].(string)
			after := wmRefsSnapshot(t, e, fx.repoID)
			ephRef := auth.EphemeralRefName("et")
			if after[ephRef] != commit {
				t.Fatalf("%s = %q, want %s", ephRef, after[ephRef], commit)
			}
			if _, leaked := after["refs/heads/et"]; leaked {
				t.Fatal("ephemeral merge leaked a persistent refs/heads/et")
			}
			// Isolation: nothing outside the namespaced target moved.
			delete(before, ephRef)
			delete(after, ephRef)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("other refs changed: before %v after %v", before, after)
			}
		})
	})
}

func TestMergeRefPolicies(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)
		mintWith := func(refsJSON string) string {
			return e.mint(t, tokenSpec{iss: fx.org, repo: fx.url,
				scopes: []string{auth.ScopeGitWrite}, refs: refsJSON})
		}

		t.Run("no-push denies", func(t *testing.T) {
			tok := mintWith(`[["refs/heads/t-pol-nopush", ["no-push"]]]`)
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", tok,
				wmMergeBody("feature", "t-pol-nopush", "merge", map[string]any{"author": wmTestAuthor}))
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			res := wmResult(t, wmDecode(t, raw))
			if res["status"] != "forbidden" || !strings.Contains(res["message"].(string), "no-push") {
				t.Fatalf("result = %v", res)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-pol-nopush"]; got != fx.c2 {
				t.Fatalf("t-pol-nopush moved to %q", got)
			}
		})

		t.Run("no-force-push allows merge commits", func(t *testing.T) {
			// Merge outcomes are fast-forward by construction: the old tip
			// is a parent of the new commit.
			tok := mintWith(`[["refs/heads/t-pol-nff", ["no-force-push"]]]`)
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", tok,
				wmMergeBody("feature", "t-pol-nff", "merge", map[string]any{"author": wmTestAuthor}))
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
		})

		t.Run("verify-sig passes fast-forwards", func(t *testing.T) {
			// Hook parity: a fast-forward introduces no server-created
			// commits — the promoted commits were verified at push time.
			tok := mintWith(`[["refs/heads/t-pol-vs-ff", ["verify-sig"]]]`)
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", tok,
				wmMergeBody("ffsrc", "t-pol-vs-ff", "ff_only", nil))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "fast_forward" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
		})

		t.Run("verify-sig denies server-created merge commits", func(t *testing.T) {
			tok := mintWith(`[["refs/heads/t-pol-vs-merge", ["verify-sig"]]]`)
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", tok,
				wmMergeBody("feature", "t-pol-vs-merge", "merge", map[string]any{"author": wmTestAuthor}))
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			res := wmResult(t, wmDecode(t, raw))
			if !strings.Contains(res["message"].(string), "verify-sig") {
				t.Fatalf("result = %v", res)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-pol-vs-merge"]; got != fx.c2 {
				t.Fatalf("t-pol-vs-merge moved to %q", got)
			}
		})
	})
}

func TestMergeValidationAndAuth(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)

		t.Run("invalid requests are 400", func(t *testing.T) {
			cases := map[string]map[string]any{
				"missing source_branch": wmMergeBody("", "main", "merge", nil),
				"missing target_branch": wmMergeBody("feature", "", "merge", nil),
				"missing strategy":      wmMergeBody("feature", "main", "", nil),
				"invalid strategy":      wmMergeBody("feature", "main", "rebase", nil),
				"squash with ff_only":   wmMergeBody("ffsrc", "main", "ff_only", map[string]any{"squash": true}),
				"malformed guard":       wmMergeBody("feature", "main", "merge", map[string]any{"expected_target_sha": "nothex"}),
				"refs-prefixed target":  wmMergeBody("feature", "refs/heads/main", "merge", nil),
				"author without email":  wmMergeBody("feature", "main", "merge", map[string]any{"author": map[string]any{"name": "x"}}),
			}
			for name, body := range cases {
				resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token, body)
				if resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status = %d, body %s", name, resp.StatusCode, raw)
					continue
				}
				if res := wmResult(t, wmDecode(t, raw)); res["status"] != "invalid" {
					t.Errorf("%s: result = %v", name, res)
				}
			}
		})

		t.Run("author required when a commit is created", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("feature", "main", "merge", nil))
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			res := wmResult(t, wmDecode(t, raw))
			if !strings.Contains(res["message"].(string), "author") {
				t.Fatalf("result = %v", res)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/main"]; got != fx.c2 {
				t.Fatalf("main moved to %q", got)
			}
		})

		t.Run("malformed body is 400", func(t *testing.T) {
			req, err := http.NewRequest("POST", srv.URL+"/api/v1/repos/merge", strings.NewReader("{"))
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
				t.Fatalf("status = %d", resp.StatusCode)
			}
		})

		t.Run("missing token is 401", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", "",
				wmMergeBody("feature", "main", "merge", nil))
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
			}
			if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q", got)
			}
		})

		t.Run("scope enforcement is 403", func(t *testing.T) {
			readOnly := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitRead}})
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", readOnly,
				wmMergeBody("feature", "main", "merge", nil))
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("merge with git:read only: status = %d, body %s", resp.StatusCode, raw)
			}
			writeOnly := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitWrite}})
			resp, raw = wmDo(t, srv, "GET",
				"/api/v1/repos/merge/preview?source_branch=feature&target_branch=main", writeOnly, nil)
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("preview with git:write only: status = %d, body %s", resp.StatusCode, raw)
			}
		})

		t.Run("unknown branches and repos are 404", func(t *testing.T) {
			for name, body := range map[string]map[string]any{
				"unknown source":                wmMergeBody("ghost", "main", "merge", nil),
				"unknown target":                wmMergeBody("feature", "ghost", "merge", nil),
				"ephemeral source without flag": wmMergeBody("scratch", "main", "merge", nil),
			} {
				resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token, body)
				if resp.StatusCode != http.StatusNotFound {
					t.Errorf("%s: status = %d, body %s", name, resp.StatusCode, raw)
				}
			}
			ghostTok := e.mint(t, tokenSpec{iss: fx.org, repo: "https://example.com/" + fx.org + "/ghost",
				scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", ghostTok,
				wmMergeBody("feature", "main", "merge", nil))
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("unknown repo: status = %d, body %s", resp.StatusCode, raw)
			}
		})
	})
}

// TestMergeSourceRefAlias covers issue #17: source_ref is the current
// upstream merge-source field — accepting a bare branch name or its fully
// qualified refs/heads form — with source_branch kept as the deprecated
// bare-name alias, the current name winning on conflict. The 200 response
// source object now carries ref alongside the deprecated branch mirror.
func TestMergeSourceRefAlias(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)

		merge := func(t *testing.T, body map[string]any) (*http.Response, map[string]any) {
			t.Helper()
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token, body)
			return resp, wmDecode(t, raw)
		}

		t.Run("source_ref-only fast-forwards", func(t *testing.T) {
			resp, body := merge(t, map[string]any{
				"source_ref": "ffsrc", "target_branch": "t-alias-ref", "strategy": "ff_only",
			})
			if resp.StatusCode != http.StatusOK || body["result"] != "fast_forward" || body["commit_sha"] != fx.ffTip {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			source := body["source"].(map[string]any)
			if source["ref"] != "ffsrc" || source["branch"] != "ffsrc" || source["sha"] != fx.ffTip {
				t.Fatalf("source = %v, want ref and branch both ffsrc", source)
			}
		})

		t.Run("refs/heads form resolves to the bare branch", func(t *testing.T) {
			resp, body := merge(t, map[string]any{
				"source_ref": "refs/heads/ffsrc", "target_branch": "t-alias-refsheads", "strategy": "ff_only",
			})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.ffTip {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			source := body["source"].(map[string]any)
			if source["ref"] != "ffsrc" || source["branch"] != "ffsrc" {
				t.Fatalf("source = %v, want the bare name echoed", source)
			}
		})

		t.Run("current name wins over the deprecated alias", func(t *testing.T) {
			// A losing source_ref must not fall back to source_branch.
			resp, body := merge(t, map[string]any{
				"source_ref": "no-such-branch", "source_branch": "ffsrc",
				"target_branch": "t-alias-both", "strategy": "ff_only",
			})
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status %d body %v, want 404 (source_ref wins)", resp.StatusCode, body)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-alias-both"]; got != fx.c2 {
				t.Fatalf("t-alias-both moved to %q on refused merge", got)
			}
			resp, body = merge(t, map[string]any{
				"source_ref": "ffsrc", "source_branch": "no-such-branch",
				"target_branch": "t-alias-both", "strategy": "ff_only",
			})
			if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.ffTip {
				t.Fatalf("status %d body %v, want source_ref to win", resp.StatusCode, body)
			}
		})

		t.Run("legacy-only body still works with enriched response", func(t *testing.T) {
			// Repeatable no_op merge: t-noop already contains main.
			resp, body := merge(t, wmMergeBody("main", "t-noop", "merge", nil))
			if resp.StatusCode != http.StatusOK || body["result"] != "no_op" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			source := body["source"].(map[string]any)
			if source["ref"] != "main" || source["branch"] != "main" {
				t.Fatalf("source = %v, want ref mirroring branch", source)
			}
		})

		t.Run("non-refs/heads source_ref is 400", func(t *testing.T) {
			resp, body := merge(t, map[string]any{
				"source_ref": "refs/tags/v1", "target_branch": "t-alias-both", "strategy": "merge",
			})
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %d body %v, want 400", resp.StatusCode, body)
			}
		})
	})
}

func TestMergePreview(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)
		ctx := context.Background()
		preview := func(t *testing.T, query string) (*http.Response, map[string]any) {
			t.Helper()
			resp, raw := wmDo(t, srv, "GET", "/api/v1/repos/merge/preview?"+query, fx.token, nil)
			return resp, wmDecode(t, raw)
		}

		t.Run("clean three-way", func(t *testing.T) {
			resp, body := preview(t, "source_branch=feature&target_branch=main")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %v", resp.StatusCode, body)
			}
			if body["status"] != "clean" || body["result"] != "merge_commit" {
				t.Fatalf("body = %v", body)
			}
			if body["source_tip_sha"] != fx.featTip || body["target_tip_sha"] != fx.c2 || body["merge_base_sha"] != fx.c1 {
				t.Fatalf("tips = %v/%v/%v", body["source_tip_sha"], body["target_tip_sha"], body["merge_base_sha"])
			}
			for _, key := range []string{"conflict_paths", "conflicts", "filtered_conflicts"} {
				if arr, ok := body[key].([]any); !ok || len(arr) != 0 {
					t.Errorf("%s = %v, want []", key, body[key])
				}
			}
		})

		t.Run("fast-forward", func(t *testing.T) {
			resp, body := preview(t, "source_branch=ffsrc&target_branch=main")
			if resp.StatusCode != http.StatusOK || body["status"] != "clean" || body["result"] != "fast_forward" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
		})

		t.Run("no_op", func(t *testing.T) {
			resp, body := preview(t, "source_branch=main&target_branch=ffsrc")
			if resp.StatusCode != http.StatusOK || body["status"] != "clean" || body["result"] != "no_op" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
		})

		// Regression: preview previously hardcoded refs/heads/ lookups, so
		// ephemeral branches 404'd even though POST /repos/merge resolves
		// them (source_is_ephemeral/target_is_ephemeral).
		t.Run("ephemeral source resolves", func(t *testing.T) {
			resp, body := preview(t, "source_branch=scratch&target_branch=main&source_ephemeral=true")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %v", resp.StatusCode, body)
			}
			if body["source_tip_sha"] != fx.scratchTip {
				t.Fatalf("source_tip_sha = %v, want %v", body["source_tip_sha"], fx.scratchTip)
			}
		})
		t.Run("ephemeral source without flag still 404s", func(t *testing.T) {
			resp, _ := preview(t, "source_branch=scratch&target_branch=main")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", resp.StatusCode)
			}
		})
		t.Run("ephemeral target resolves", func(t *testing.T) {
			resp, body := preview(t, "source_branch=feature&target_branch=et&target_ephemeral=true")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %v", resp.StatusCode, body)
			}
			if body["target_tip_sha"] != fx.c2 {
				t.Fatalf("target_tip_sha = %v, want %v", body["target_tip_sha"], fx.c2)
			}
		})

		// Issue #15: source_is_ephemeral/target_is_ephemeral are the
		// official upstream spellings; the historical source_ephemeral/
		// target_ephemeral stay as deprecated aliases.
		t.Run("official *_is_ephemeral spellings resolve", func(t *testing.T) {
			resp, body := preview(t, "source_branch=scratch&target_branch=main&source_is_ephemeral=true")
			if resp.StatusCode != http.StatusOK || body["source_tip_sha"] != fx.scratchTip {
				t.Fatalf("status = %d, source_tip_sha = %v, want 200/%v", resp.StatusCode, body["source_tip_sha"], fx.scratchTip)
			}
			resp, body = preview(t, "source_branch=feature&target_branch=et&target_is_ephemeral=true")
			if resp.StatusCode != http.StatusOK || body["target_tip_sha"] != fx.c2 {
				t.Fatalf("status = %d, target_tip_sha = %v, want 200/%v", resp.StatusCode, body["target_tip_sha"], fx.c2)
			}
		})
		t.Run("official spelling wins over the deprecated alias", func(t *testing.T) {
			resp, body := preview(t, "source_branch=scratch&target_branch=main&source_is_ephemeral=true&source_ephemeral=false")
			if resp.StatusCode != http.StatusOK || body["source_tip_sha"] != fx.scratchTip {
				t.Fatalf("status = %d, body %v, want the official spelling to win", resp.StatusCode, body)
			}
			resp, _ = preview(t, "source_branch=scratch&target_branch=main&source_is_ephemeral=false&source_ephemeral=true")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (explicit official false beats deprecated true)", resp.StatusCode)
			}
		})
		t.Run("both ephemeral spellings stay shape-validated", func(t *testing.T) {
			for _, q := range []string{
				"source_is_ephemeral=bogus", "target_is_ephemeral=bogus",
				"source_ephemeral=bogus", "target_ephemeral=bogus",
				"source_is_ephemeral=true&source_ephemeral=bogus",
			} {
				resp, body := preview(t, "source_branch=feature&target_branch=main&"+q)
				if resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status = %d, body %v, want 400", q, resp.StatusCode, body)
				}
			}
		})
		t.Run("absent ephemeral flags preserve vendor-SDK defaults", func(t *testing.T) {
			// ADR: explicit flags must preserve default semantics when
			// absent — a bare preview and an explicit-false preview must
			// answer identically.
			respBare, bare := preview(t, "source_branch=feature&target_branch=main")
			respExplicit, explicit := preview(t, "source_branch=feature&target_branch=main&source_is_ephemeral=false&target_is_ephemeral=false")
			if respBare.StatusCode != http.StatusOK || respExplicit.StatusCode != http.StatusOK {
				t.Fatalf("statuses = %d/%d, want 200/200", respBare.StatusCode, respExplicit.StatusCode)
			}
			if !reflect.DeepEqual(bare, explicit) {
				t.Fatalf("bare = %v, explicit false = %v, want identical", bare, explicit)
			}
		})

		t.Run("conflicted without content", func(t *testing.T) {
			resp, body := preview(t, "source_branch=conflict&target_branch=main")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %v", resp.StatusCode, body)
			}
			if body["status"] != "conflicted" || body["result"] != "merge_commit" {
				t.Fatalf("body = %v", body)
			}
			if !reflect.DeepEqual(body["conflict_paths"], []any{"a.txt"}) {
				t.Fatalf("conflict_paths = %v", body["conflict_paths"])
			}
			if arr := body["conflicts"].([]any); len(arr) != 0 {
				t.Fatalf("conflicts inlined without include_content: %v", arr)
			}
		})

		t.Run("conflicted with content", func(t *testing.T) {
			resp, body := preview(t, "source_branch=conflict&target_branch=main&include_content=true")
			if resp.StatusCode != http.StatusOK || body["status"] != "conflicted" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			conflicts := body["conflicts"].([]any)
			if len(conflicts) != 1 {
				t.Fatalf("conflicts = %v", conflicts)
			}
			c := conflicts[0].(map[string]any)
			if c["path"] != "a.txt" {
				t.Fatalf("conflict path = %v", c["path"])
			}
			wantSides := map[string]struct{ oid, content string }{
				"base":   {coreRevParse(t, fx.work, fx.c1+":a.txt"), "base\n"},
				"ours":   {coreRevParse(t, fx.work, fx.c2+":a.txt"), "main\n"},
				"theirs": {coreRevParse(t, fx.work, fx.conflictTip+":a.txt"), "other\n"},
			}
			for side, want := range wantSides {
				got := c[side].(map[string]any)
				if got["oid"] != want.oid || got["content"] != want.content ||
					got["binary"] != false || got["truncated"] != false {
					t.Errorf("%s = %v, want oid %s content %q", side, got, want.oid, want.content)
				}
			}
			result := c["result"].(map[string]any)
			content, _ := result["content"].(string)
			if !strings.Contains(content, "<<<<<<<") || !strings.Contains(content, "main") || !strings.Contains(content, "other") {
				t.Errorf("result content = %q, want conflict markers with both sides", content)
			}
		})

		t.Run("gitlink conflict is 200 and never dereferenced", func(t *testing.T) {
			// Submodule (160000) conflict sides name commits that exist in
			// no odb; rendering them used to cat-file the oid and 500.
			resp, body := preview(t, "source_branch=subside&target_branch=submain&include_content=true")
			if resp.StatusCode != http.StatusOK || body["status"] != "conflicted" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if !reflect.DeepEqual(body["conflict_paths"], []any{"vendor/dep"}) {
				t.Fatalf("conflict_paths = %v", body["conflict_paths"])
			}
			conflicts := body["conflicts"].([]any)
			if len(conflicts) != 1 {
				t.Fatalf("conflicts = %v", conflicts)
			}
			c := conflicts[0].(map[string]any)
			wantSides := map[string]string{
				"ours":   strings.Repeat("2", 40),
				"theirs": strings.Repeat("3", 40),
			}
			for side, wantOID := range wantSides {
				got := c[side].(map[string]any)
				if got["oid"] != wantOID || got["binary"] != false || got["truncated"] != false {
					t.Errorf("%s = %v, want oid %s without content", side, got, wantOID)
				}
				if _, hasContent := got["content"]; hasContent {
					t.Errorf("%s carries content %v for a gitlink", side, got["content"])
				}
			}
			base := c["base"].(map[string]any)
			if _, hasOID := base["oid"]; hasOID {
				t.Errorf("base = %v, want absent stage (add/add conflict)", base)
			}
		})

		t.Run("preview never mutates", func(t *testing.T) {
			refsBefore := wmRefsSnapshot(t, e, fx.repoID)
			packsBefore, err := e.refStore.ListPacks(ctx, fx.repoID)
			if err != nil {
				t.Fatal(err)
			}
			blobsBefore, err := e.blobStore.List(ctx, blob.PackPrefix(fx.repoID))
			if err != nil {
				t.Fatal(err)
			}
			resp, body := preview(t, "source_branch=conflict&target_branch=main&include_content=true")
			if resp.StatusCode != http.StatusOK || body["status"] != "conflicted" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			refsAfter := wmRefsSnapshot(t, e, fx.repoID)
			packsAfter, err := e.refStore.ListPacks(ctx, fx.repoID)
			if err != nil {
				t.Fatal(err)
			}
			blobsAfter, err := e.blobStore.List(ctx, blob.PackPrefix(fx.repoID))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(refsBefore, refsAfter) {
				t.Fatal("preview changed refs")
			}
			if !reflect.DeepEqual(packsBefore, packsAfter) {
				t.Fatal("preview registered packs")
			}
			if !reflect.DeepEqual(blobsBefore, blobsAfter) {
				t.Fatal("preview uploaded blobs")
			}
		})

		t.Run("binary conflict sides omit content", func(t *testing.T) {
			resp, body := preview(t, "source_branch=binside&target_branch=binmain&include_content=true")
			if resp.StatusCode != http.StatusOK || body["status"] != "conflicted" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			conflicts := body["conflicts"].([]any)
			if len(conflicts) != 1 {
				t.Fatalf("conflicts = %v", conflicts)
			}
			c := conflicts[0].(map[string]any)
			if c["path"] != "bin.dat" {
				t.Fatalf("path = %v", c["path"])
			}
			for _, side := range []string{"base", "ours", "theirs"} {
				got := c[side].(map[string]any)
				if got["binary"] != true {
					t.Errorf("%s.binary = %v, want true", side, got["binary"])
				}
				if _, has := got["content"]; has {
					t.Errorf("%s carries binary content: %v", side, got)
				}
				if oid, _ := got["oid"].(string); !ref.ValidSHA(oid) {
					t.Errorf("%s.oid = %v", side, got["oid"])
				}
			}
		})

		t.Run("oversized conflict content is filtered", func(t *testing.T) {
			resp, body := preview(t, "source_branch=bigside&target_branch=bigmain&include_content=true")
			if resp.StatusCode != http.StatusOK || body["status"] != "conflicted" {
				t.Fatalf("status %d body %v", resp.StatusCode, body)
			}
			if !reflect.DeepEqual(body["conflict_paths"], []any{"big.txt"}) {
				t.Fatalf("conflict_paths = %v", body["conflict_paths"])
			}
			if arr := body["conflicts"].([]any); len(arr) != 0 {
				t.Fatalf("oversized conflict inlined: %v", arr)
			}
			filtered := body["filtered_conflicts"].([]any)
			if len(filtered) != 1 {
				t.Fatalf("filtered_conflicts = %v", filtered)
			}
			f := filtered[0].(map[string]any)
			if f["path"] != "big.txt" || f["reason"] == "" {
				t.Fatalf("filtered = %v", f)
			}
		})

		t.Run("unrelated histories preview as if allowed", func(t *testing.T) {
			resp, body := preview(t, "source_branch=orphan&target_branch=main")
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body %v", resp.StatusCode, body)
			}
			if body["status"] != "clean" || body["result"] != "merge_commit" {
				t.Fatalf("body = %v", body)
			}
			if _, has := body["merge_base_sha"]; has {
				t.Fatalf("merge_base_sha present for unrelated preview: %v", body["merge_base_sha"])
			}
		})

		t.Run("validation and missing branches", func(t *testing.T) {
			for name, query := range map[string]string{
				"missing source_branch": "target_branch=main",
				"missing target_branch": "source_branch=feature",
				"invalid include":       "source_branch=feature&target_branch=main&include_content=banana",
				"refs-prefixed source":  "source_branch=refs/heads/feature&target_branch=main",
			} {
				resp, raw := wmDo(t, srv, "GET", "/api/v1/repos/merge/preview?"+query, fx.token, nil)
				if resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status = %d, body %s", name, resp.StatusCode, raw)
					continue
				}
				requireEnvelope(t, resp, raw)
			}
			resp, raw := wmDo(t, srv, "GET",
				"/api/v1/repos/merge/preview?source_branch=ghost&target_branch=main", fx.token, nil)
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("unknown source: status = %d, body %s", resp.StatusCode, raw)
			}
			// Without the opt-in source_ephemeral flag, ephemeral refs
			// stay unaddressable (vendor-SDK default behavior preserved).
			resp, raw = wmDo(t, srv, "GET",
				"/api/v1/repos/merge/preview?source_branch=scratch&target_branch=main", fx.token, nil)
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("ephemeral source: status = %d, body %s", resp.StatusCode, raw)
			}
		})
	})
}

// TestMergeGitVersionProbe pins the ADR-23 version gate: merge-tree
// --write-tree needs git >= 2.38 and older versions get a clear error.
func TestMergeGitVersionProbe(t *testing.T) {
	for banner, wantErr := range map[string]bool{
		"git version 2.55.0":            false,
		"git version 2.38.0":            false,
		"git version 3.0.0":             false,
		"git version 2.37.7":            true,
		"git version 1.9.1":             true,
		"not a git banner":              true,
		"git version weird":             true,
		"git version 2.39.2 (Apple Git": false,
	} {
		err := wmCheckGitVersion(banner)
		if wantErr && err == nil {
			t.Errorf("%q: expected an error", banner)
		}
		if !wantErr && err != nil {
			t.Errorf("%q: unexpected error %v", banner, err)
		}
	}
}
