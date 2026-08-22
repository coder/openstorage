package api

// Contract integration tests for the streaming write endpoints (docs/07 p4
// W2): POST /repos/commit-pack, /repos/diff-commit, /repos/restore-commit.
// Wired through runBackends so the pg and sqlite lanes assert identical
// behavior.
//
// The write routes are registered in api.New (api.go) and additionally
// mounted on a dedicated test server via registerWriteCommitRoutes so the
// streaming surface is exercised in isolation; cross-surface visibility is asserted against the
// environment's api.New server (e.srv) — the docs/05 interop invariant:
// API-written commits must be immediately readable through the git:read
// endpoints, since both surfaces share one ref/blob store and cache.
//
// All helpers carry a wct prefix to stay collision-free with sibling slice
// test files in this package.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

// wctServer mounts ONLY the write-commit routes with the production auth
// middleware, sharing the lane's stores and cache dir with e.srv.
func wctServer(t *testing.T, e *testEnv) *httptest.Server {
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
	registerWriteCommitRoutes(mux, s, auth.NewVerifier(e.store), nil)
	srv := httptest.NewServer(recoverPanics(discardLogger())(mux))
	t.Cleanup(srv.Close)
	return srv
}

// wctRepo creates a fresh repo and mints write + read tokens for it.
func wctRepo(t *testing.T, e *testEnv) (repo meta.Repo, writeTok, readTok string) {
	t.Helper()
	org := e.newOrg(t)
	url := "https://example.com/" + org + "/wct-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	writeTok = e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitWrite}})
	readTok = e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
	return repo, writeTok, readTok
}

// wctPost performs one write-endpoint request with an NDJSON (or JSON)
// body. A nil body sends no payload.
func wctPost(t *testing.T, srv *httptest.Server, path, token string, body io.Reader) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp, respBody
}

// wctLines serializes frames (any JSON-marshalable values) as NDJSON.
func wctLines(t *testing.T, frames ...any) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, f := range frames {
		raw, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(raw)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// wctChunk builds one blob_chunk frame from raw bytes.
func wctChunk(contentID string, data []byte, eof bool) map[string]any {
	return map[string]any{"blob_chunk": map[string]any{
		"content_id": contentID,
		"data":       base64.StdEncoding.EncodeToString(data),
		"eof":        eof,
	}}
}

// wctDiffChunk builds one diff_chunk frame from raw bytes.
func wctDiffChunk(data []byte, eof bool) map[string]any {
	return map[string]any{"diff_chunk": map[string]any{
		"data": base64.StdEncoding.EncodeToString(data),
		"eof":  eof,
	}}
}

// wctAuthor is the canonical test ident.
var wctAuthor = map[string]any{"name": "wct", "email": "wct@test.invalid"}

// wctMeta builds a metadata frame; extra overrides/augments the defaults.
func wctMeta(target, message string, files []map[string]any, extra map[string]any) map[string]any {
	md := map[string]any{
		"target_branch":  target,
		"commit_message": message,
		"author":         wctAuthor,
	}
	if files != nil {
		md["files"] = files
	}
	for k, v := range extra {
		md[k] = v
	}
	return map[string]any{"metadata": md}
}

func wctDecode(t *testing.T, body []byte) wcEnvelope {
	t.Helper()
	var env wcEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode envelope %q: %v", body, err)
	}
	return env
}

// wctRequireOK asserts a 200 success envelope and returns it.
func wctRequireOK(t *testing.T, resp *http.Response, body []byte) wcEnvelope {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, body)
	}
	env := wctDecode(t, body)
	if env.Commit == nil || !env.Result.Success || env.Result.Status != "ok" {
		t.Fatalf("success envelope = %s", body)
	}
	if !ref.ValidSHA(env.Commit.CommitSHA) || !ref.ValidSHA(env.Commit.TreeSHA) {
		t.Fatalf("malformed shas in envelope %s", body)
	}
	if env.Result.NewSHA != env.Commit.CommitSHA {
		t.Fatalf("result.new_sha %q != commit.commit_sha %q", env.Result.NewSHA, env.Commit.CommitSHA)
	}
	if env.Commit.PackBytes <= 0 {
		t.Fatalf("pack_bytes = %d, want > 0", env.Commit.PackBytes)
	}
	return env
}

// wctRequireFail asserts the docs/04 failure envelope with the given
// HTTP/status pair and returns it.
func wctRequireFail(t *testing.T, resp *http.Response, body []byte, wantCode int, wantStatus string) wcEnvelope {
	t.Helper()
	if resp.StatusCode != wantCode {
		t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, wantCode, body)
	}
	env := wctDecode(t, body)
	if env.Result.Success || env.Result.Status != wantStatus {
		t.Fatalf("failure envelope = %s, want status %q", body, wantStatus)
	}
	if env.Result.Message == "" {
		t.Fatalf("failure envelope missing message: %s", body)
	}
	return env
}

// wctReadJSON asserts a 200 read-API response (against e.srv) and decodes it.
func wctReadJSON(t *testing.T, e *testEnv, path, token string, out any) {
	t.Helper()
	resp, body := e.doReq(t, http.MethodGet, path, token, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d (body %s)", path, resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

// wctBranchTip fetches one branch tip via the read API ("" when absent).
func wctBranchTip(t *testing.T, e *testEnv, token, branch string, ephemeral bool) string {
	t.Helper()
	path := "/api/v1/repos/branches?limit=100"
	if ephemeral {
		path += "&ephemeral=true"
	}
	var out listBranchesResponse
	wctReadJSON(t, e, path, token, &out)
	for _, b := range out.Branches {
		if b.Name == branch {
			return b.HeadSHA
		}
	}
	return ""
}

// ---------------------------------------------------------------------------

func TestCommitPack(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, readTok := wctRepo(t, e)
		_ = repo

		var tip1, tip2 string

		t.Run("root commit with streamed blobs", func(t *testing.T) {
			body := wctLines(t,
				wctMeta("main", "c1: root", []map[string]any{
					{"path": "README.md", "content_id": "id-readme", "operation": "upsert", "mode": "100644"},
					{"path": "empty.txt", "content_id": "id-empty", "operation": "upsert", "mode": "100644"},
					{"path": "script.sh", "content_id": "id-script", "operation": "upsert", "mode": "100755"},
					{"path": "link.txt", "content_id": "id-link", "operation": "upsert", "mode": "120000"},
				}, nil),
				wctChunk("id-readme", []byte("hello "), false),
				wctChunk("id-readme", []byte("world\n"), true),
				wctChunk("id-empty", nil, true), // empty file: single {"data":"","eof":true}
				wctChunk("id-script", []byte("#!/bin/sh\necho hi\n"), true),
				wctChunk("id-link", []byte("README.md"), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			env := wctRequireOK(t, resp, respBody)
			if env.Result.OldSHA != ref.ZeroSHA {
				t.Errorf("root commit old_sha = %q, want 40 zeros", env.Result.OldSHA)
			}
			if env.Commit.BlobCount == nil || *env.Commit.BlobCount != 4 {
				t.Errorf("blob_count = %v, want 4", env.Commit.BlobCount)
			}
			if env.Commit.TargetBranch != "main" || env.Result.Branch != "main" {
				t.Errorf("branch fields = %q/%q, want main", env.Commit.TargetBranch, env.Result.Branch)
			}
			tip1 = env.Commit.CommitSHA

			// Cross-surface (docs/05 interop invariant): the read API sees
			// the commit immediately.
			if got := wctBranchTip(t, e, readTok, "main", false); got != tip1 {
				t.Errorf("read API main tip = %q, want %q", got, tip1)
			}
			resp2, fileBody := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=README.md&ref=main", readTok, nil, "")
			if resp2.StatusCode != http.StatusOK || string(fileBody) != "hello world\n" {
				t.Errorf("read file = %d %q, want 200 %q", resp2.StatusCode, fileBody, "hello world\n")
			}
			var files listFilesResponse
			wctReadJSON(t, e, "/api/v1/repos/files?ref=main", readTok, &files)
			modes := map[string]string{}
			for _, entry := range files.Entries {
				modes[entry.Path] = entry.Mode
			}
			if modes["script.sh"] != "100755" || modes["link.txt"] != "120000" {
				t.Errorf("modes = %v, want script.sh=100755 link.txt=120000", modes)
			}
		})
		if t.Failed() {
			return
		}

		t.Run("second commit with guard, delete, gitlink", func(t *testing.T) {
			body := wctLines(t,
				wctMeta("main", "c2: update", []map[string]any{
					{"path": "README.md", "content_id": "id-r2", "operation": "upsert", "mode": "100644"},
					{"path": "empty.txt", "content_id": "id-del", "operation": "delete"},
					{"path": "vendor/dep", "content_id": "id-mod", "operation": "upsert", "mode": "160000"},
				}, map[string]any{"expected_head_sha": tip1}),
				wctChunk("id-r2", []byte("v2\n"), true),
				wctChunk("id-mod", []byte(tip1), true), // gitlink payload: a commit sha
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			env := wctRequireOK(t, resp, respBody)
			if env.Result.OldSHA != tip1 {
				t.Errorf("old_sha = %q, want %q", env.Result.OldSHA, tip1)
			}
			tip2 = env.Commit.CommitSHA

			resp2, _ := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=empty.txt&ref=main", readTok, nil, "")
			if resp2.StatusCode != http.StatusNotFound {
				t.Errorf("deleted file read = %d, want 404", resp2.StatusCode)
			}
			var files listFilesResponse
			wctReadJSON(t, e, "/api/v1/repos/files?ref=main&recursive=true", readTok, &files)
			modes := map[string]string{}
			for _, entry := range files.Entries {
				modes[entry.Path] = entry.Mode
			}
			if modes["vendor/dep"] != "160000" {
				t.Errorf("gitlink mode = %q, want 160000", modes["vendor/dep"])
			}
		})
		if t.Failed() {
			return
		}

		t.Run("stale guard is 412", func(t *testing.T) {
			body := wctLines(t,
				wctMeta("main", "stale", []map[string]any{
					{"path": "x.txt", "content_id": "id-x", "operation": "upsert"},
				}, map[string]any{"expected_head_sha": tip1}),
				wctChunk("id-x", []byte("x"), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			env := wctRequireFail(t, resp, respBody, http.StatusPreconditionFailed, "precondition_failed")
			if env.Result.OldSHA != tip2 {
				t.Errorf("412 old_sha = %q, want current tip %q", env.Result.OldSHA, tip2)
			}
		})

		t.Run("branch from base_branch", func(t *testing.T) {
			body := wctLines(t,
				wctMeta("feature", "f1", []map[string]any{
					{"path": "feature.txt", "content_id": "id-f", "operation": "upsert"},
				}, map[string]any{"base_branch": "main"}),
				wctChunk("id-f", []byte("feature\n"), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			env := wctRequireOK(t, resp, respBody)
			if env.Result.OldSHA != ref.ZeroSHA {
				t.Errorf("branch creation old_sha = %q, want 40 zeros", env.Result.OldSHA)
			}
			var commits listCommitsResponse
			wctReadJSON(t, e, "/api/v1/repos/commits?branch=feature&limit=10", readTok, &commits)
			if len(commits.Commits) != 3 { // c1, c2, f1
				t.Fatalf("feature history length = %d, want 3", len(commits.Commits))
			}
			if commits.Commits[0].SHA != env.Commit.CommitSHA || commits.Commits[0].ParentSHAs[0] != tip2 {
				t.Errorf("feature tip = %+v, want new commit parented on %q", commits.Commits[0], tip2)
			}
		})

		t.Run("guarded branch creation checks base tip", func(t *testing.T) {
			// The vendor SDK full-workflow drive creates a feature branch
			// with expected_head_sha = the base branch head; the guard must
			// compare against the base tip, not the unborn target (zero).
			tip := wctBranchTip(t, e, readTok, "feature", false)
			body := wctLines(t,
				wctMeta("feature2", "f2", []map[string]any{
					{"path": "feature2.txt", "content_id": "id-f2", "operation": "upsert"},
				}, map[string]any{"base_branch": "feature", "expected_head_sha": tip}),
				wctChunk("id-f2", []byte("feature2\n"), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			env := wctRequireOK(t, resp, respBody)
			if env.Result.OldSHA != ref.ZeroSHA {
				t.Errorf("branch creation old_sha = %q, want 40 zeros", env.Result.OldSHA)
			}

			// A stale guard against the base tip is still a 412.
			body = wctLines(t,
				wctMeta("feature3", "f3", []map[string]any{
					{"path": "feature3.txt", "content_id": "id-f3", "operation": "upsert"},
				}, map[string]any{"base_branch": "feature", "expected_head_sha": tip1}),
				wctChunk("id-f3", []byte("feature3\n"), true),
			)
			resp, respBody = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			wctRequireFail(t, resp, respBody, http.StatusPreconditionFailed, "precondition_failed")
		})

		t.Run("ephemeral namespace write", func(t *testing.T) {
			body := wctLines(t,
				wctMeta("scratch", "eph1", []map[string]any{
					{"path": "scratch.txt", "content_id": "id-s", "operation": "upsert"},
				}, map[string]any{"ephemeral": true, "base_branch": "main"}),
				wctChunk("id-s", []byte("scratch\n"), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			env := wctRequireOK(t, resp, respBody)
			if got := wctBranchTip(t, e, readTok, "scratch", true); got != env.Commit.CommitSHA {
				t.Errorf("ephemeral read tip = %q, want %q", got, env.Commit.CommitSHA)
			}
			if got := wctBranchTip(t, e, readTok, "scratch", false); got != "" {
				t.Errorf("persistent listing leaks ephemeral branch (tip %q)", got)
			}
		})

		t.Run("chunked body without content-length", func(t *testing.T) {
			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				frames := wctLines(t,
					wctMeta("chunked", "streamed", []map[string]any{
						{"path": "big.txt", "content_id": "id-big", "operation": "upsert"},
					}, map[string]any{"base_branch": "main"}),
				)
				_, _ = pw.Write(frames)
				// Two chunks written with a pause: the server must parse
				// incrementally off the wire.
				_, _ = pw.Write(wctLines(t, wctChunk("id-big", bytes.Repeat([]byte("a"), 1<<16), false)))
				time.Sleep(50 * time.Millisecond)
				_, _ = pw.Write(wctLines(t, wctChunk("id-big", []byte("end\n"), true)))
			}()
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/repos/commit-pack", pr)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+writeTok)
			req.Header.Set("Content-Type", "application/x-ndjson")
			// io.Pipe has unknown length -> Go sends Transfer-Encoding:
			// chunked with no Content-Length.
			if req.ContentLength != 0 {
				t.Fatalf("test bug: ContentLength = %d", req.ContentLength)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			respBody, _ := io.ReadAll(resp.Body)
			wctRequireOK(t, resp, respBody)
		})
	})
}

func TestCommitPackValidation(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, _ := wctRepo(t, e)

		upsert := func(path, id string) map[string]any {
			return map[string]any{"path": path, "content_id": id, "operation": "upsert"}
		}
		cases := []struct {
			name     string
			frames   []any
			wantCode int
			wantMsg  string
		}{
			{"refs-prefixed target branch",
				[]any{wctMeta("refs/heads/main", "m", nil, nil)},
				http.StatusBadRequest, "refs/ prefix"},
			{"missing commit message",
				[]any{wctMeta("main", "", nil, nil)},
				http.StatusBadRequest, "commit_message"},
			{"missing author",
				[]any{map[string]any{"metadata": map[string]any{"target_branch": "main", "commit_message": "m"}}},
				http.StatusBadRequest, "author"},
			{"malformed expected_head_sha",
				[]any{wctMeta("main", "m", nil, map[string]any{"expected_head_sha": "nothex"})},
				http.StatusBadRequest, "expected_head_sha"},
			{"path traversal",
				[]any{wctMeta("main", "m", []map[string]any{upsert("../evil", "id1")}, nil)},
				http.StatusBadRequest, "path"},
			{"absolute path",
				[]any{wctMeta("main", "m", []map[string]any{upsert("/etc/passwd", "id1")}, nil)},
				http.StatusBadRequest, "absolute"},
			{".git component",
				[]any{wctMeta("main", "m", []map[string]any{upsert(".git/config", "id1")}, nil)},
				http.StatusBadRequest, ".git"},
			{"invalid mode",
				[]any{wctMeta("main", "m", []map[string]any{
					{"path": "a", "content_id": "id1", "operation": "upsert", "mode": "100600"},
				}, nil)},
				http.StatusBadRequest, "mode"},
			{"invalid operation",
				[]any{wctMeta("main", "m", []map[string]any{
					{"path": "a", "content_id": "id1", "operation": "rename"},
				}, nil)},
				http.StatusBadRequest, "operation"},
			{"duplicate content_id",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1"), upsert("b", "id1")}, nil)},
				http.StatusBadRequest, "duplicate content_id"},
			{"duplicate path",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1"), upsert("a", "id2")}, nil)},
				http.StatusBadRequest, "duplicate path"},
			{"first frame not metadata",
				[]any{wctChunk("id1", []byte("x"), true)},
				http.StatusBadRequest, "metadata"},
			{"unknown content_id chunk",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil),
					wctChunk("bogus", []byte("x"), true)},
				http.StatusBadRequest, "unknown"},
			{"chunk for delete op",
				[]any{wctMeta("main", "m", []map[string]any{
					{"path": "a", "content_id": "id1", "operation": "delete"},
				}, nil),
					wctChunk("id1", []byte("x"), true)},
				http.StatusBadRequest, "unknown or non-upsert"},
			{"interleaved chunks",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1"), upsert("b", "id2")}, nil),
					wctChunk("id1", []byte("x"), false),
					wctChunk("id2", []byte("y"), true),
				},
				http.StatusBadRequest, "interleaved"},
			{"chunk after eof",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil),
					wctChunk("id1", []byte("x"), true),
					wctChunk("id1", []byte("y"), true)},
				http.StatusBadRequest, "after eof"},
			{"missing eof",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil),
					wctChunk("id1", []byte("x"), false)},
				http.StatusBadRequest, "before eof"},
			{"upsert without chunks",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil)},
				http.StatusBadRequest, "no blob chunks"},
			{"malformed base64",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil),
					map[string]any{"blob_chunk": map[string]any{"content_id": "id1", "data": "!!!", "eof": true}}},
				http.StatusBadRequest, "base64"},
			{"diff_chunk on commit-pack",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil),
					wctDiffChunk([]byte("x"), true)},
				http.StatusBadRequest, "diff_chunk"},
			{"unborn base_branch",
				[]any{wctMeta("main", "m", nil, map[string]any{"base_branch": "nope"})},
				http.StatusNotFound, "base branch"},
			{"gitlink payload not a sha",
				[]any{wctMeta("main", "m", []map[string]any{
					{"path": "dep", "content_id": "id1", "operation": "upsert", "mode": "160000"},
				}, nil),
					wctChunk("id1", []byte("not a sha"), true)},
				http.StatusBadRequest, "gitlink"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t, tc.frames...)))
				wantStatus := "invalid"
				if tc.wantCode == http.StatusNotFound {
					wantStatus = "not_found"
				}
				env := wctRequireFail(t, resp, body, tc.wantCode, wantStatus)
				if !strings.Contains(env.Result.Message, tc.wantMsg) {
					t.Errorf("message %q does not mention %q", env.Result.Message, tc.wantMsg)
				}
			})
		}

		t.Run("oversize decoded chunk", func(t *testing.T) {
			frames := wctLines(t,
				wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil),
				wctChunk("id1", bytes.Repeat([]byte("a"), wcMaxChunkBytes+1), true),
			)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(frames))
			wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid")
		})

		t.Run("garbage line", func(t *testing.T) {
			body := append(wctLines(t, wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil)), []byte("not json\n")...)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			wctRequireFail(t, resp, respBody, http.StatusBadRequest, "invalid")
		})

		// ADR-26: auth failures on write routes carry the docs/04 result
		// envelope (not the read-route {"error"} body).
		t.Run("missing token is 401 with result envelope", func(t *testing.T) {
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", "", bytes.NewReader(wctLines(t, wctMeta("main", "m", nil, nil))))
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
			}
			requireAuthResultEnvelope(t, body, "unauthorized")
		})

		t.Run("git:read scope is 403 with result envelope", func(t *testing.T) {
			_, _, readTok := wctRepo(t, e)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", readTok, bytes.NewReader(wctLines(t, wctMeta("main", "m", nil, nil))))
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", resp.StatusCode)
			}
			requireAuthResultEnvelope(t, body, "forbidden")
		})

		t.Run("unknown repo is enveloped 404", func(t *testing.T) {
			org := e.newOrg(t)
			tok := e.mint(t, tokenSpec{iss: org, repo: "https://example.com/" + org + "/ghost", scopes: []string{auth.ScopeGitWrite}})
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok, bytes.NewReader(wctLines(t, wctMeta("main", "m", nil, nil))))
			wctRequireFail(t, resp, body, http.StatusNotFound, "not_found")
		})
	})
}

// TestCommitPackCASRace loses the publish-time CAS deterministically: the
// handler snapshots the ref BEFORE materializing the cache (fresh repo -> no
// cache dir yet), so once the cache dir appears the snapshot is taken and
// the test moves the ref out from under it before releasing the blob frames.
func TestCommitPackCASRace(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, _ := wctRepo(t, e)

		// Seed main=c1 with c2 already registered in the pack store (the
		// concurrent "winner" the test will move the ref to) — without
		// materializing the cache.
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", "one\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "c1")
		c1 := coreRevParse(t, dir, "HEAD")
		coreWriteFile(t, dir, "a.txt", "two\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "c2")
		c2 := coreRevParse(t, dir, "HEAD")
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": c1})

		cacheMarker := filepath.Join(e.cacheDir, repo.RepoID+".git", "HEAD")
		if _, err := os.Stat(cacheMarker); err == nil {
			t.Fatal("test bug: cache repo already materialized")
		}

		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			_, _ = pw.Write(wctLines(t, wctMeta("main", "raced", []map[string]any{
				{"path": "b.txt", "content_id": "id-b", "operation": "upsert"},
			}, nil)))
			// The handler lists refs (snapshotting old=c1) strictly before
			// materializing; wait for the cache repo to appear, then move
			// the ref to c2 before releasing the blob frame.
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(cacheMarker); err == nil {
					break
				}
				if time.Now().After(deadline) {
					_ = pw.CloseWithError(fmt.Errorf("cache repo never materialized"))
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			won, err := e.refStore.CASRef(context.Background(), repo.RepoID, "refs/heads/main", c1, c2)
			if err != nil || !won {
				_ = pw.CloseWithError(fmt.Errorf("concurrent move failed: won=%v err=%v", won, err))
				return
			}
			_, _ = pw.Write(wctLines(t, wctChunk("id-b", []byte("b\n"), true)))
		}()

		req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/repos/commit-pack", pr)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+writeTok)
		req.Header.Set("Content-Type", "application/x-ndjson")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		env := wctRequireFail(t, resp, body, http.StatusConflict, "conflict")
		if env.Result.OldSHA != c1 {
			t.Errorf("conflict old_sha = %q, want the stale snapshot %q", env.Result.OldSHA, c1)
		}

		// The loser's ref must be untouched and its pack merely
		// unreferenced: main still points at the winner.
		tips, err := e.refStore.ListRefs(context.Background(), repo.RepoID)
		if err != nil || tips["refs/heads/main"] != c2 {
			t.Errorf("main = %q (err %v), want winner %q", tips["refs/heads/main"], err, c2)
		}
	})
}

func TestDiffCommit(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, readTok := wctRepo(t, e)

		baseText := "line1\nline2\nline3\n"
		baseBin := []byte{0x00, 0x01, 0x02, 0x03}

		// Base commit via commit-pack.
		body := wctLines(t,
			wctMeta("main", "base", []map[string]any{
				{"path": "a.txt", "content_id": "id-a", "operation": "upsert"},
				{"path": "bin.dat", "content_id": "id-bin", "operation": "upsert"},
			}, nil),
			wctChunk("id-a", []byte(baseText), true),
			wctChunk("id-bin", baseBin, true),
		)
		resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
		base := wctRequireOK(t, resp, respBody)

		// Build a real git patch (text edit + binary edit + new file) in a
		// local throwaway repo replicating the base state.
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", baseText)
		if err := os.WriteFile(filepath.Join(dir, "bin.dat"), baseBin, 0o644); err != nil {
			t.Fatal(err)
		}
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "base")
		coreWriteFile(t, dir, "a.txt", "line1\nLINE2\nline3\n")
		if err := os.WriteFile(filepath.Join(dir, "bin.dat"), []byte{0x09, 0x08, 0x07}, 0o644); err != nil {
			t.Fatal(err)
		}
		coreWriteFile(t, dir, "new.txt", "brand new\n")
		coreGit(t, dir, "add", "-A")
		patch := coreGit(t, dir, "diff", "--binary", "--cached")
		if !strings.Contains(patch, "GIT binary patch") {
			t.Fatalf("test bug: patch has no binary hunk:\n%s", patch)
		}

		var diffTip string
		t.Run("apply patch", func(t *testing.T) {
			frames := wctLines(t,
				wctMeta("main", "patched", nil, map[string]any{"expected_head_sha": base.Commit.CommitSHA}),
				// Split the patch across two chunks to exercise reassembly.
				wctDiffChunk([]byte(patch[:len(patch)/2]), false),
				wctDiffChunk([]byte(patch[len(patch)/2:]), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(frames))
			env := wctRequireOK(t, resp, respBody)
			if env.Commit.BlobCount == nil || *env.Commit.BlobCount != 3 {
				t.Errorf("blob_count = %v, want 3", env.Commit.BlobCount)
			}
			diffTip = env.Commit.CommitSHA

			resp2, fileBody := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=a.txt&ref=main", readTok, nil, "")
			if resp2.StatusCode != http.StatusOK || string(fileBody) != "line1\nLINE2\nline3\n" {
				t.Errorf("patched a.txt = %d %q", resp2.StatusCode, fileBody)
			}
			resp3, binBody := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=bin.dat&ref=main", readTok, nil, "")
			if resp3.StatusCode != http.StatusOK || !bytes.Equal(binBody, []byte{0x09, 0x08, 0x07}) {
				t.Errorf("patched bin.dat = %d %v", resp3.StatusCode, binBody)
			}
			resp4, newBody := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path=new.txt&ref=main", readTok, nil, "")
			if resp4.StatusCode != http.StatusOK || string(newBody) != "brand new\n" {
				t.Errorf("new.txt = %d %q", resp4.StatusCode, newBody)
			}
		})
		if t.Failed() {
			return
		}

		t.Run("empty diff is an empty commit", func(t *testing.T) {
			frames := wctLines(t,
				wctMeta("main", "empty", nil, nil),
				wctDiffChunk(nil, true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(frames))
			env := wctRequireOK(t, resp, respBody)
			if env.Result.OldSHA != diffTip {
				t.Errorf("old_sha = %q, want %q", env.Result.OldSHA, diffTip)
			}
			if env.Commit.BlobCount == nil || *env.Commit.BlobCount != 0 {
				t.Errorf("blob_count = %v, want 0", env.Commit.BlobCount)
			}
			// Same tree as the parent: an empty commit.
			var commits listCommitsResponse
			wctReadJSON(t, e, "/api/v1/repos/commits?branch=main&limit=1", readTok, &commits)
			if len(commits.Commits) != 1 || commits.Commits[0].SHA != env.Commit.CommitSHA {
				t.Errorf("tip = %+v, want %q", commits.Commits, env.Commit.CommitSHA)
			}
		})

		t.Run("non-applying patch is 422", func(t *testing.T) {
			// The same patch again: a.txt no longer has "line2" context.
			frames := wctLines(t,
				wctMeta("main", "conflict", nil, nil),
				wctDiffChunk([]byte(patch), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(frames))
			wctRequireFail(t, resp, respBody, http.StatusUnprocessableEntity, "invalid")
		})

		t.Run("files in metadata rejected", func(t *testing.T) {
			frames := wctLines(t,
				wctMeta("main", "m", []map[string]any{
					{"path": "a", "content_id": "id1", "operation": "upsert"},
				}, nil),
				wctDiffChunk(nil, true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(frames))
			wctRequireFail(t, resp, respBody, http.StatusBadRequest, "invalid")
		})

		t.Run("malformed base64 diff chunk is 400", func(t *testing.T) {
			frames := wctLines(t,
				wctMeta("main", "m", nil, nil),
				map[string]any{"diff_chunk": map[string]any{"data": "!!!", "eof": true}},
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(frames))
			env := wctRequireFail(t, resp, respBody, http.StatusBadRequest, "invalid")
			if !strings.Contains(env.Result.Message, "base64") {
				t.Errorf("message %q does not mention base64", env.Result.Message)
			}
		})
	})
}

func TestRestoreCommit(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, readTok := wctRepo(t, e)

		commitFile := func(msg, path, content, expected string) wcEnvelope {
			t.Helper()
			extra := map[string]any{}
			if expected != "" {
				extra["expected_head_sha"] = expected
			}
			body := wctLines(t,
				wctMeta("main", msg, []map[string]any{
					{"path": path, "content_id": "id-" + path, "operation": "upsert"},
				}, extra),
				wctChunk("id-"+path, []byte(content), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			return wctRequireOK(t, resp, respBody)
		}
		c1 := commitFile("c1", "f1.txt", "one\n", "")
		c2 := commitFile("c2", "f2.txt", "two\n", c1.Commit.CommitSHA)
		c3 := commitFile("c3", "f3.txt", "three\n", c2.Commit.CommitSHA)

		restore := func(md map[string]any) (*http.Response, []byte) {
			t.Helper()
			raw, err := json.Marshal(map[string]any{"metadata": md})
			if err != nil {
				t.Fatal(err)
			}
			return wctPost(t, srv, "/api/v1/repos/restore-commit", writeTok, bytes.NewReader(raw))
		}

		t.Run("restore drops later files, preserves history", func(t *testing.T) {
			resp, body := restore(map[string]any{
				"target_branch":     "main",
				"target_commit_sha": c1.Commit.CommitSHA,
				"author":            wctAuthor,
				"expected_head_sha": c3.Commit.CommitSHA,
			})
			env := wctRequireOK(t, resp, body)
			if env.Commit.TreeSHA != c1.Commit.TreeSHA {
				t.Errorf("restored tree = %q, want c1 tree %q", env.Commit.TreeSHA, c1.Commit.TreeSHA)
			}
			if env.Result.OldSHA != c3.Commit.CommitSHA {
				t.Errorf("old_sha = %q, want c3 %q", env.Result.OldSHA, c3.Commit.CommitSHA)
			}
			if env.Commit.BlobCount != nil {
				t.Errorf("restore-commit must omit blob_count, got %d", *env.Commit.BlobCount)
			}

			var files listFilesResponse
			wctReadJSON(t, e, "/api/v1/repos/files?ref=main", readTok, &files)
			if len(files.Paths) != 1 || files.Paths[0] != "f1.txt" {
				t.Errorf("restored files = %v, want [f1.txt]", files.Paths)
			}
			var commits listCommitsResponse
			wctReadJSON(t, e, "/api/v1/repos/commits?branch=main&limit=10", readTok, &commits)
			if len(commits.Commits) != 4 { // history preserved: c1..c3 + restore
				t.Fatalf("history length = %d, want 4", len(commits.Commits))
			}
			if commits.Commits[0].ParentSHAs[0] != c3.Commit.CommitSHA {
				t.Errorf("restore parent = %v, want c3", commits.Commits[0].ParentSHAs)
			}
		})

		t.Run("stale guard is 412", func(t *testing.T) {
			resp, body := restore(map[string]any{
				"target_branch":     "main",
				"target_commit_sha": c1.Commit.CommitSHA,
				"author":            wctAuthor,
				"expected_head_sha": c1.Commit.CommitSHA, // stale
			})
			wctRequireFail(t, resp, body, http.StatusPreconditionFailed, "precondition_failed")
		})

		t.Run("unknown target commit is 404", func(t *testing.T) {
			resp, body := restore(map[string]any{
				"target_branch":     "main",
				"target_commit_sha": strings.Repeat("ab", 20),
				"author":            wctAuthor,
			})
			wctRequireFail(t, resp, body, http.StatusNotFound, "not_found")
		})

		t.Run("unborn branch is 404", func(t *testing.T) {
			resp, body := restore(map[string]any{
				"target_branch":     "ghost",
				"target_commit_sha": c1.Commit.CommitSHA,
				"author":            wctAuthor,
			})
			wctRequireFail(t, resp, body, http.StatusNotFound, "not_found")
		})

		t.Run("malformed body is 400", func(t *testing.T) {
			resp, body := wctPost(t, srv, "/api/v1/repos/restore-commit", writeTok, strings.NewReader("{"))
			wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid")
		})
	})
}

func TestWriteCommitRefPolicies(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		org := e.newOrg(t)
		url := "https://example.com/" + org + "/wct-pol-" + randomHex(t, 4)
		if _, err := e.store.CreateRepo(context.Background(), org, url, "main"); err != nil {
			t.Fatal(err)
		}
		mint := func(refs string, ops []string) string {
			return e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitWrite}, refs: refs, ops: ops})
		}
		commitBody := func(branch string, extra map[string]any) []byte {
			return wctLines(t,
				wctMeta(branch, "m", []map[string]any{
					{"path": "p-" + randomHex(t, 3) + ".txt", "content_id": "id1", "operation": "upsert"},
				}, extra),
				wctChunk("id1", []byte("x\n"), true),
			)
		}

		t.Run("no-push denies", func(t *testing.T) {
			tok := mint(`[["refs/heads/main", ["no-push"]]]`, nil)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok, bytes.NewReader(commitBody("main", nil)))
			env := wctRequireFail(t, resp, body, http.StatusForbidden, "forbidden")
			if !strings.Contains(env.Result.Message, "no-push") {
				t.Errorf("message = %q", env.Result.Message)
			}
		})

		t.Run("verify-sig denies unsigned server commits", func(t *testing.T) {
			tok := mint(`[["*", ["verify-sig"]]]`, nil)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok, bytes.NewReader(commitBody("main", nil)))
			wctRequireFail(t, resp, body, http.StatusForbidden, "forbidden")
		})

		t.Run("no-force-push allows fast-forward writes", func(t *testing.T) {
			tok := mint(`[["refs/heads/main", ["no-force-push"]]]`, nil)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok, bytes.NewReader(commitBody("main", nil)))
			wctRequireOK(t, resp, body)
		})

		t.Run("persistent rule does not police ephemeral namespace", func(t *testing.T) {
			// Hook parity: the rule targets refs/heads/scratch, but the
			// ephemeral write matches against the namespaced storage name.
			tok := mint(`[["refs/heads/scratch", ["no-push"]]]`, nil)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok,
				bytes.NewReader(commitBody("scratch", map[string]any{"ephemeral": true})))
			wctRequireOK(t, resp, body)
		})

		t.Run("star rule polices ephemeral namespace", func(t *testing.T) {
			tok := mint(`[["*", ["no-push"]]]`, nil)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok,
				bytes.NewReader(commitBody("other", map[string]any{"ephemeral": true})))
			wctRequireFail(t, resp, body, http.StatusForbidden, "forbidden")
		})

		t.Run("deprecated ops claim folds in", func(t *testing.T) {
			tok := mint("", []string{"no-push"})
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok, bytes.NewReader(commitBody("main", nil)))
			wctRequireFail(t, resp, body, http.StatusForbidden, "forbidden")
		})

		t.Run("malformed refs claim fails closed", func(t *testing.T) {
			tok := mint(`["not-a-tuple"]`, nil)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok, bytes.NewReader(commitBody("main", nil)))
			wctRequireFail(t, resp, body, http.StatusForbidden, "forbidden")
		})

		t.Run("restore-commit honors policies", func(t *testing.T) {
			tok := mint(`[["refs/heads/main", ["no-push"]]]`, nil)
			raw, err := json.Marshal(map[string]any{"metadata": map[string]any{
				"target_branch":     "main",
				"target_commit_sha": strings.Repeat("ab", 20),
				"author":            wctAuthor,
			}})
			if err != nil {
				t.Fatal(err)
			}
			resp, body := wctPost(t, srv, "/api/v1/repos/restore-commit", tok, bytes.NewReader(raw))
			wctRequireFail(t, resp, body, http.StatusForbidden, "forbidden")
		})
	})
}

// TestCommitPackQuota (issue #21, Codex round 11): the REST pack writers
// enforce the same per-repo pack-bytes quota as the git surface — without
// this, POST /repos/commit-pack bypasses the quota entirely and grows the
// same packs/ prefix without bound. Breaches answer the ADR-26 write-route
// envelope (403 "forbidden"); identical re-publications dedup instead of
// re-uploading.
func TestCommitPackQuota(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
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
			registerWriteCommitRoutes(mux, s, auth.NewVerifier(e.store), nil)
			srv := httptest.NewServer(recoverPanics(discardLogger())(mux))
			t.Cleanup(srv.Close)
			return srv
		}
		repo, writeTok, _ := wctRepo(t, e)

		smallBody := func(msg, path string) []byte {
			return wctLines(t,
				wctMeta("main", msg, []map[string]any{
					{"path": path, "content_id": "id-f", "operation": "upsert", "mode": "100644"},
				}, nil),
				wctChunk("id-f", []byte("tiny\n"), true),
			)
		}

		// Under a generous quota the write passes.
		generous := quotaSrv(1 << 20)
		resp, respBody := wctPost(t, generous, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(smallBody("c1", "a.txt")))
		wctRequireOK(t, resp, respBody)

		usage, err := blob.PackUsageBytes(context.Background(), e.blobStore, repo.RepoID)
		if err != nil || usage == 0 {
			t.Fatalf("usage after first commit = (%d, %v), want > 0", usage, err)
		}

		// A 1-byte quota refuses new bytes with the 403 forbidden envelope
		// and stores nothing new.
		tiny := quotaSrv(1)
		resp, respBody = wctPost(t, tiny, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(smallBody("c2", "b.txt")))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("over-quota commit-pack = %d, want 403; body: %s", resp.StatusCode, respBody)
		}
		var env wcEnvelope
		if err := json.Unmarshal(respBody, &env); err != nil {
			t.Fatalf("non-JSON quota body %q: %v", respBody, err)
		}
		if env.Result.Success || env.Result.Status != "forbidden" || !strings.Contains(env.Result.Message, "quota") {
			t.Fatalf("unexpected result envelope: %+v", env.Result)
		}
		after, err := blob.PackUsageBytes(context.Background(), e.blobStore, repo.RepoID)
		if err != nil || after != usage {
			t.Fatalf("denied write changed usage: %d -> %d (err %v)", usage, after, err)
		}
	})
}
