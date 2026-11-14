package api

// Contract integration tests for the streaming write endpoints (docs/archive/phases/07-implementation-roadmap.md p4
// W2): POST /repos/commit-pack, /repos/diff-commit, /repos/restore-commit.
// Wired through runBackends so the pg and sqlite lanes assert identical
// behavior.
//
// The write routes are registered in api.New (api.go) and additionally
// mounted on a dedicated test server via registerWriteCommitRoutes so the
// streaming surface is exercised in isolation; cross-surface visibility is asserted against the
// environment's api.New server (e.srv) — the docs/explanation/git-protocol-and-namespaces.md interop invariant:
// API-written commits must be immediately readable through the git:read
// endpoints, since both surfaces share one ref/blob store and cache.
//
// All helpers carry a wct prefix to stay collision-free with sibling slice
// test files in this package.

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
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
	registerWriteCommitRoutes(mux, s, auth.NewVerifier(e.store), "", nil)
	srv := httptest.NewServer(withProblemInstance(recoverPanics(discardLogger())(mux)))
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
	maps.Copy(md, extra)
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

// wctRequireRefUpdate asserts the raw members of a commit-write envelope
// (#665): `commit` is present (null on a failure other than a lost CAS),
// and `result` has every member RefUpdateResult requires, no member it
// lacks, and a target_branch equal to the deprecated branch.
func wctRequireRefUpdate(t *testing.T, body []byte) {
	t.Helper()
	var env map[string]json.RawMessage
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode envelope %q: %v", body, err)
	}
	commit, hasCommit := env["commit"]
	var res map[string]any
	if err := json.Unmarshal(env["result"], &res); err != nil {
		t.Fatalf("decode result of %s: %v", body, err)
	}
	if !hasCommit || (res["status"] != "ok" && res["status"] != "failed") != (string(commit) == "null") {
		t.Fatalf("commit = %s (present %v), want an object on success and on a lost CAS, else null (body %s)", commit, hasCommit, body)
	}
	keys := slices.DeleteFunc(slices.Sorted(maps.Keys(res)), func(k string) bool { return k == "message" })
	if !slices.Equal(keys, refUpdateRequired) {
		t.Fatalf("result members = %v (and message), want RefUpdateResult's %v (body %s)", keys, refUpdateRequired, body)
	}
	if res["target_branch"] != res["branch"] {
		t.Fatalf("target_branch %v != branch %v (body %s)", res["target_branch"], res["branch"], body)
	}
}

// wctRequireOK asserts a 200 success envelope and returns it.
func wctRequireOK(t *testing.T, resp *http.Response, body []byte) wcEnvelope {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, body)
	}
	wctRequireRefUpdate(t, body)
	env := wctDecode(t, body)
	if env.Commit == nil || !env.Result.Success || env.Result.Status != "ok" {
		t.Fatalf("success envelope = %s", body)
	}
	if env.Result.TargetBranch == "" || env.Result.TargetBranch != env.Commit.TargetBranch {
		t.Fatalf("result.target_branch %q, want commit.target_branch %q", env.Result.TargetBranch, env.Commit.TargetBranch)
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

// wctRequireFail asserts a failed commit write with the given HTTP/status
// pair. Where upstream documents the docs/explanation/streaming-commit-protocols.md failure envelope for the
// route and status (upstreamEnvelopeStatuses), it asserts that envelope,
// with every RefUpdateResult member (wctRequireRefUpdate), and returns it.
// Otherwise it asserts the plain problem body with no `result`
// and returns the zero envelope.
func wctRequireFail(t *testing.T, resp *http.Response, body []byte, wantCode int, wantStatus string) wcEnvelope {
	t.Helper()
	if resp.StatusCode != wantCode {
		t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, wantCode, body)
	}
	path := resp.Request.URL.Path
	if strings.HasPrefix(path, "/api/repos/") { // alias: its last segment names the v1 route
		path = "/api/v1/repos/" + path[strings.LastIndex(path, "/")+1:]
	}
	if !slices.Contains(upstreamEnvelopeStatuses(t, path), wantCode) {
		requireEnvelope(t, resp, body)
		if bytes.Contains(body, []byte(`"result"`)) {
			t.Fatalf("problem body carries a result envelope: %s", body)
		}
		return wcEnvelope{}
	}
	wctRequireRefUpdate(t, body)
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

			// Cross-surface (docs/explanation/git-protocol-and-namespaces.md interop invariant): the read API sees
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

		t.Run("stale guard is 409 ref_guard_failed", func(t *testing.T) {
			body := wctLines(t,
				wctMeta("main", "stale", []map[string]any{
					{"path": "x.txt", "content_id": "id-x", "operation": "upsert"},
				}, map[string]any{"expected_head_sha": tip1}),
				wctChunk("id-x", []byte("x"), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			assertCommitRefGuard(t, resp.StatusCode, respBody, "main", tip2, tip2)
			if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("Content-Type = %q, want application/problem+json", ct)
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

			// A stale guard against the base tip fails too: current_sha is
			// the base tip, old_sha the unborn target's 40 zeros.
			body = wctLines(t,
				wctMeta("feature3", "f3", []map[string]any{
					{"path": "feature3.txt", "content_id": "id-f3", "operation": "upsert"},
				}, map[string]any{"base_branch": "feature", "expected_head_sha": tip1}),
				wctChunk("id-f3", []byte("feature3\n"), true),
			)
			resp, respBody = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			assertCommitRefGuard(t, resp.StatusCode, respBody, "feature3", ref.ZeroSHA, tip)
			if got := wctBranchTip(t, e, readTok, "feature3", false); got != "" {
				t.Fatalf("feature3 = %q after a failed guard, want no branch", got)
			}
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

// TestCommitPackModeAliases pins #806 (spec 5d8e1801): commit-pack stores
// the permission-style modes that upstream normalizes as a regular file or
// an executable, and still refuses every other mode.
func TestCommitPackModeAliases(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, readTok := wctRepo(t, e)

		stored := map[string]string{
			"644": "100644", "100664": "100644", "100644": "100644",
			"755": "100755", "100775": "100755", "100755": "100755",
		}
		var files []map[string]any
		var chunks []any
		for mode := range stored {
			files = append(files, map[string]any{"path": "m" + mode, "content_id": "id-" + mode, "operation": "upsert", "mode": mode})
			chunks = append(chunks, wctChunk("id-"+mode, []byte(mode+"\n"), true))
		}
		resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok,
			bytes.NewReader(wctLines(t, append([]any{wctMeta("main", "modes", files, nil)}, chunks...)...)))
		wctRequireOK(t, resp, body)
		var listing listFilesResponse
		wctReadJSON(t, e, "/api/v1/repos/files?ref=main", readTok, &listing)
		got := map[string]string{}
		for _, entry := range listing.Entries {
			got[strings.TrimPrefix(entry.Path, "m")] = entry.Mode
		}
		if !maps.Equal(got, stored) {
			t.Fatalf("stored modes = %v, want %v", got, stored)
		}

		for _, mode := range []string{"600", "664", "775", "777", "100600", "100700", "0644", "0100644", "1006440", "64", "100", "040000", "abc", " 644", "644 "} {
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
				wctMeta("main", "bad mode", []map[string]any{{"path": "bad", "content_id": "id1", "operation": "upsert", "mode": mode}}, nil),
				wctChunk("id1", []byte("x\n"), true))))
			if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `invalid mode \"`+mode+`\" for \"bad\"`) {
				t.Errorf("mode %q: status %d body %s, want 400 invalid mode", mode, resp.StatusCode, body)
			}
		}

		// A delete ignores its mode, as before.
		resp, body = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "delete with a mode", []map[string]any{{"path": "m644", "content_id": "id-del", "operation": "delete", "mode": "600"}}, nil))))
		wctRequireOK(t, resp, body)
	})
}

// TestCommitPackFileDefaults pins FileOperation rules of the spec: an entry
// without operation is an upsert, a delete needs no content_id, and a
// delete may send one empty final chunk, as the spec's request example does.
func TestCommitPackFileDefaults(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, readTok := wctRepo(t, e)
		ls := func(t *testing.T) map[string]string {
			t.Helper()
			var listing listFilesResponse
			wctReadJSON(t, e, "/api/v1/repos/files?ref=main", readTok, &listing)
			got := map[string]string{}
			for _, entry := range listing.Entries {
				got[entry.Path] = entry.Mode
			}
			return got
		}

		resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "no operation", []map[string]any{
				{"path": "a.txt", "content_id": "id-a"},
				{"path": "b.txt", "content_id": "id-b", "operation": ""},
			}, nil),
			wctChunk("id-a", []byte("a\n"), true),
			wctChunk("id-b", []byte("b\n"), true))))
		wctRequireOK(t, resp, body)
		if got := ls(t); !maps.Equal(got, map[string]string{"a.txt": "100644", "b.txt": "100644"}) {
			t.Fatalf("files = %v, want a.txt and b.txt as 100644", got)
		}

		resp, body = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "delete without content_id", []map[string]any{
				{"path": "a.txt", "operation": "delete"},
				{"path": "b.txt", "content_id": "", "operation": "delete"},
			}, nil))))
		wctRequireOK(t, resp, body)
		if got := ls(t); len(got) != 0 {
			t.Fatalf("files = %v, want none after the deletes", got)
		}

		for name, files := range map[string][]map[string]any{
			"upsert without content_id":  {{"path": "c.txt"}},
			"explicit upsert without id": {{"path": "c.txt", "operation": "upsert"}},
		} {
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t, wctMeta("main", "m", files, nil))))
			if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `missing content_id for \"c.txt\"`) {
				t.Errorf("%s: status %d body %s, want 400 missing content_id", name, resp.StatusCode, body)
			}
		}
		// The spec's request example: one empty final chunk for a delete.
		resp, body = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "spec example", []map[string]any{
				{"path": "f.txt", "content_id": "id-f", "operation": "upsert"},
			}, nil),
			wctChunk("id-f", []byte("f\n"), true))))
		wctRequireOK(t, resp, body)
		resp, body = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "spec example", []map[string]any{
				{"path": "g.txt", "content_id": "blob-1", "operation": "upsert", "mode": "100644"},
				{"path": "f.txt", "content_id": "blob-2", "operation": "delete"},
			}, nil),
			wctChunk("blob-1", []byte("g\n"), true),
			wctChunk("blob-2", nil, true))))
		wctRequireOK(t, resp, body)
		if got := ls(t); !maps.Equal(got, map[string]string{"g.txt": "100644"}) {
			t.Fatalf("files = %v, want only g.txt", got)
		}
		for name, chunks := range map[string][]any{
			"two empty chunks":    {wctChunk("del", nil, true), wctChunk("del", nil, true)},
			"empty chunk, no eof": {wctChunk("del", nil, false)},
			"chunk with data":     {wctChunk("del", []byte("x"), true)},
		} {
			frames := append([]any{wctMeta("main", "m", []map[string]any{{"path": "g.txt", "content_id": "del", "operation": "delete"}}, nil)}, chunks...)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t, frames...)))
			if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "unknown or non-upsert content_id del") {
				t.Errorf("%s: status %d body %s, want 400 unknown or non-upsert", name, resp.StatusCode, body)
			}
		}
		// The empty chunk must not cut into an upsert's stream, and a delete
		// without content_id gets no chunk at all.
		for name, frames := range map[string][]any{
			"empty delete chunk mid-upsert": {
				wctMeta("main", "m", []map[string]any{
					{"path": "h.txt", "content_id": "up", "operation": "upsert"},
					{"path": "g.txt", "content_id": "del", "operation": "delete"},
				}, nil),
				wctChunk("up", []byte("h"), false), wctChunk("del", nil, true), wctChunk("up", []byte("\n"), true),
			},
			"empty chunk for a delete without content_id": {
				wctMeta("main", "m", []map[string]any{{"path": "g.txt", "operation": "delete"}}, nil),
				wctChunk("", nil, true),
			},
		} {
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t, frames...)))
			if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "unknown or non-upsert content_id") {
				t.Errorf("%s: status %d body %s, want 400 unknown or non-upsert", name, resp.StatusCode, body)
			}
		}
		// An unknown operation is reported as such, also without content_id.
		resp, body = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "m", []map[string]any{{"path": "g.txt", "operation": "remove"}}, nil))))
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `invalid operation \"remove\" for \"g.txt\"`) {
			t.Errorf("unknown operation: status %d body %s, want 400 invalid operation", resp.StatusCode, body)
		}

		resp, body = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "m", []map[string]any{
				{"path": "d.txt", "content_id": "same", "operation": "upsert"},
				{"path": "e.txt", "content_id": "same", "operation": "delete"},
			}, nil),
			wctChunk("same", []byte("d\n"), true))))
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `duplicate content_id \"same\"`) {
			t.Errorf("shared content_id: status %d body %s, want 400 duplicate content_id", resp.StatusCode, body)
		}
		resp, body = wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "m", []map[string]any{
				{"path": "g.txt", "content_id": "twice", "operation": "delete"},
				{"path": "h.txt", "content_id": "twice", "operation": "delete"},
			}, nil))))
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `duplicate content_id \"twice\"`) {
			t.Errorf("content_id shared by two deletes: status %d body %s, want 400 duplicate content_id", resp.StatusCode, body)
		}
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
			{"ephemeral HEAD target branch",
				[]any{wctMeta("HEAD", "m", nil, map[string]any{"target_is_ephemeral": true})},
				http.StatusBadRequest, "invalid target_branch"},
			{"ephemeral HEAD target branch, deprecated flag",
				[]any{wctMeta("HEAD", "m", nil, map[string]any{"ephemeral": true})},
				http.StatusBadRequest, "invalid target_branch"},
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
				http.StatusPreconditionFailed, "before eof"},
			{"upsert without chunks",
				[]any{wctMeta("main", "m", []map[string]any{upsert("a", "id1")}, nil)},
				http.StatusPreconditionFailed, "no blob chunks"},
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
				wctRequireFail(t, resp, body, tc.wantCode, statusForHTTP(tc.wantCode))
				if !bytes.Contains(body, []byte(tc.wantMsg)) {
					t.Errorf("body %s does not mention %q", body, tc.wantMsg)
				}
			})
		}

		t.Run("persistent HEAD target allowed", func(t *testing.T) {
			// Only an ephemeral target named HEAD is refused.
			frames := wctLines(t,
				wctMeta("HEAD", "m", []map[string]any{upsert("a", "id1")}, nil),
				wctChunk("id1", []byte("x"), true))
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(frames))
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d body %s, want 200", resp.StatusCode, body)
			}
		})

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

		// ADR-26: auth failures on write routes carry the docs/explanation/streaming-commit-protocols.md result
		// envelope (not the read-route {"error"} body).
		t.Run("missing token is 401 with a problem body", func(t *testing.T) {
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", "", bytes.NewReader(wctLines(t, wctMeta("main", "m", nil, nil))))
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
			if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
			}
			requireEnvelope(t, resp, body)
		})

		t.Run("git:read scope is 403 with a problem body", func(t *testing.T) {
			_, _, readTok := wctRepo(t, e)
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", readTok, bytes.NewReader(wctLines(t, wctMeta("main", "m", nil, nil))))
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", resp.StatusCode)
			}
			requireEnvelope(t, resp, body)
		})

		t.Run("unknown repo is enveloped 404", func(t *testing.T) {
			org := e.newOrg(t)
			tok := e.mint(t, tokenSpec{iss: org, repo: "https://example.com/" + org + "/ghost", scopes: []string{auth.ScopeGitWrite}})
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", tok, bytes.NewReader(wctLines(t, wctMeta("main", "m", nil, nil))))
			wctRequireFail(t, resp, body, http.StatusNotFound, "not_found")
		})
	})
}

// TestCommitWriteRefusesANULInTextFields: git takes the commit message as an
// argument and each ident as an environment value, and neither can hold a
// NUL. A request with a NUL in one of them passed the validation and
// answered 500 when git ran, after the files or the patch were applied.
// Each route that writes a commit answers 400 and moves no ref.
func TestCommitWriteRefusesANULInTextFields(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, readTok := wctRepo(t, e)

		commitFile := func(msg, path, expected string) wcEnvelope {
			t.Helper()
			extra := map[string]any{}
			if expected != "" {
				extra["expected_head_sha"] = expected
			}
			frames := wctLines(t,
				wctMeta("main", msg, []map[string]any{{"path": path, "content_id": "id-" + path, "operation": "upsert"}}, extra),
				wctChunk("id-"+path, []byte(path+"\n"), true))
			resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(frames))
			return wctRequireOK(t, resp, body)
		}
		c1 := commitFile("c1", "f1.txt", "")
		c2 := commitFile("c2", "f2.txt", c1.Commit.CommitSHA)
		const patch = "diff --git a/new.txt b/new.txt\nnew file mode 100644\n--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1 @@\n+x\n"

		// metadata builds the metadata object of a request.
		metadata := func(message string, extra map[string]any) map[string]any {
			return wctMeta("main", message, nil, extra)["metadata"].(map[string]any)
		}
		routes := []struct {
			name string
			body func(md map[string]any) []byte
		}{
			{"commit-pack", func(md map[string]any) []byte {
				md["files"] = []map[string]any{{"path": "new.txt", "content_id": "id1", "operation": "upsert"}}
				return wctLines(t, map[string]any{"metadata": md}, wctChunk("id1", []byte("x\n"), true))
			}},
			{"diff-commit", func(md map[string]any) []byte {
				return wctLines(t, map[string]any{"metadata": md}, wctDiffChunk([]byte(patch), true))
			}},
			{"restore-commit", func(md map[string]any) []byte {
				md["target_commit_sha"] = c1.Commit.CommitSHA
				raw, err := json.Marshal(map[string]any{"metadata": md})
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}},
		}
		ident := func(name, email string) map[string]any { return map[string]any{"name": name, "email": email} }
		fields := []struct {
			name, message string
			extra         map[string]any
		}{
			{"commit_message", "m\x00x", nil},
			{"author.name", "m", map[string]any{"author": ident("a\x00b", "a@x.invalid")}},
			{"author.email", "m", map[string]any{"author": ident("a", "a\x00@x.invalid")}},
			{"committer.name", "m", map[string]any{"committer": ident("c\x00", "c@x.invalid")}},
			{"committer.email", "m", map[string]any{"committer": ident("c", "c@x.invalid\x00")}},
		}
		for _, rt := range routes {
			for _, f := range fields {
				t.Run(rt.name+"/"+f.name, func(t *testing.T) {
					body := rt.body(metadata(f.message, f.extra))
					resp, respBody := wctPost(t, srv, "/api/v1/repos/"+rt.name, writeTok, bytes.NewReader(body))
					wctRequireFail(t, resp, respBody, http.StatusBadRequest, "invalid")
					if want := invalidTextMsg(f.name); !bytes.Contains(respBody, []byte(want)) {
						t.Errorf("body %s does not say %q", respBody, want)
					}
					if tip := wctBranchTip(t, e, readTok, "main", false); tip != c2.Commit.CommitSHA {
						t.Errorf("tip = %q after a refused request, want %q", tip, c2.Commit.CommitSHA)
					}
				})
			}
		}

		// The default message of a restore names the target. A NUL in the
		// target is a problem of the target, which is checked after the
		// metadata, and not of a commit_message the request never sent.
		restoreTarget := func(t *testing.T, target map[string]any) (*http.Response, []byte) {
			t.Helper()
			md := map[string]any{"target_branch": "main", "author": wctAuthor}
			maps.Copy(md, target)
			raw, err := json.Marshal(map[string]any{"metadata": md})
			if err != nil {
				t.Fatal(err)
			}
			return wctPost(t, srv, "/api/v1/repos/restore-commit", writeTok, bytes.NewReader(raw))
		}
		t.Run("restore-commit/a NUL in base_ref without a message", func(t *testing.T) {
			resp, body := restoreTarget(t, map[string]any{"base_ref": "x\x00"})
			if resp.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte(`"code":"ref_not_found"`)) {
				t.Errorf("status = %d, body %s, want 404 ref_not_found", resp.StatusCode, body)
			}
		})
		t.Run("restore-commit/a valid base_ref without a message names the target", func(t *testing.T) {
			resp, body := restoreTarget(t, map[string]any{"base_ref": c1.Commit.CommitSHA})
			wctRequireOK(t, resp, body)
			var commits listCommitsResponse
			wctReadJSON(t, e, "/api/v1/repos/commits?branch=main&limit=1", readTok, &commits)
			if got, want := commits.Commits[0].Message, "Restore to "+c1.Commit.CommitSHA; got != want {
				t.Errorf("restore message = %q, want %q", got, want)
			}
		})
		t.Run("restore-commit/a NUL in target_commit_sha without a message", func(t *testing.T) {
			resp, body := restoreTarget(t, map[string]any{"target_commit_sha": c1.Commit.CommitSHA[:39] + "\x00"})
			wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid")
			if want := "base_ref is required (or the deprecated 40-hex target_commit_sha)"; !bytes.Contains(body, []byte(want)) {
				t.Errorf("body %s does not say %q", body, want)
			}
		})
	})
}

// TestCommitPackTotalSizeLimit (#597, spec 2026-10-28): commit-pack answers
// 413 payload_too_large once the decoded file contents of one commit pass
// 262144000 bytes, summed over every chunk of every file, and moves no ref.
// The test lowers the limit so it can cross it with a few bytes.
func TestCommitPackTotalSizeLimit(t *testing.T) {
	if wcMaxCommitBlobBytes != 262144000 {
		t.Fatalf("wcMaxCommitBlobBytes = %d, want the spec's 262144000", wcMaxCommitBlobBytes)
	}
	old := wcMaxCommitBlobBytes
	wcMaxCommitBlobBytes = 10
	t.Cleanup(func() { wcMaxCommitBlobBytes = old })
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, readTok := wctRepo(t, e)
		upsert := func(path, id string) map[string]any {
			return map[string]any{"path": path, "content_id": id, "operation": "upsert"}
		}
		post := func(frames ...any) (*http.Response, []byte) {
			t.Helper()
			return wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t, frames...)))
		}

		// Exactly at the limit: 3+2 bytes of a.txt and 5 of b.txt.
		resp, body := post(wctMeta("main", "at limit", []map[string]any{upsert("a.txt", "ida"), upsert("b.txt", "idb")}, nil),
			wctChunk("ida", []byte("aaa"), false), wctChunk("ida", []byte("aa"), true), wctChunk("idb", []byte("bbbbb"), true))
		c1 := wctRequireOK(t, resp, body)

		// One byte over, reached in the second file's second chunk.
		before := wmRefsSnapshot(t, e, repo.RepoID)
		resp, body = post(wctMeta("main", "over limit", []map[string]any{upsert("c.txt", "idc"), upsert("d.txt", "idd")}, nil),
			wctChunk("idc", []byte("ccccc"), true), wctChunk("idd", []byte("ddd"), false), wctChunk("idd", []byte("ddd"), true))
		env := wctRequireFail(t, resp, body, http.StatusRequestEntityTooLarge, "payload_too_large")
		if env.Result.Message != "commit payload too large: exceeds total blob limit (10 bytes)" || env.Commit != nil {
			t.Fatalf("413 body = %s, want the total blob limit message and no commit", body)
		}
		if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
			t.Fatalf("over-limit write moved refs: %v -> %v", before, after)
		}
		if tip := wctBranchTip(t, e, readTok, "main", false); tip != c1.Commit.CommitSHA {
			t.Fatalf("tip = %q, want %q", tip, c1.Commit.CommitSHA)
		}
	})
}

// TestFirstCommitZeroGuard pins spec 5d8e1801: 40 zeros as
// expected_target_sha creates the first commit of an empty repository
// without base_branch, on commit-pack and diff-commit. Once the branch
// exists, the same guard fails with its tip.
func TestFirstCommitZeroGuard(t *testing.T) {
	patch := "diff --git a/a.txt b/a.txt\nnew file mode 100644\n--- /dev/null\n+++ b/a.txt\n@@ -0,0 +1 @@\n+a\n"
	for _, tc := range []struct {
		route   string
		files   []map[string]any
		content any
	}{
		{"commit-pack", []map[string]any{{"path": "a.txt", "content_id": "id-a", "operation": "upsert"}}, wctChunk("id-a", []byte("a\n"), true)},
		{"diff-commit", nil, wctDiffChunk([]byte(patch), true)},
	} {
		t.Run(tc.route, func(t *testing.T) {
			runBackends(t, func(t *testing.T, e *testEnv) {
				srv := wctServer(t, e)
				_, writeTok, readTok := wctRepo(t, e)
				post := func() (*http.Response, []byte) {
					t.Helper()
					body := wctLines(t, wctMeta("main", "first", tc.files, map[string]any{"expected_target_sha": ref.ZeroSHA}), tc.content)
					return wctPost(t, srv, "/api/v1/repos/"+tc.route, writeTok, bytes.NewReader(body))
				}
				resp, raw := post()
				env := wctRequireOK(t, resp, raw)
				if env.Result.OldSHA != ref.ZeroSHA {
					t.Fatalf("old_sha = %q, want 40 zeros", env.Result.OldSHA)
				}
				var commits listCommitsResponse
				wctReadJSON(t, e, "/api/v1/repos/commits?branch=main&limit=10", readTok, &commits)
				if len(commits.Commits) != 1 || commits.Commits[0].SHA != env.Commit.CommitSHA || len(commits.Commits[0].ParentSHAs) != 0 {
					t.Fatalf("main history = %+v, want the one root commit %s", commits.Commits, env.Commit.CommitSHA)
				}
				resp, raw = post()
				assertCommitRefGuard(t, resp.StatusCode, raw, "main", env.Commit.CommitSHA, env.Commit.CommitSHA)
			})
		})
	}
}

// TestCommitPackCASRace loses the publish-time CAS deterministically: the
// handler snapshots the ref BEFORE materializing the cache (fresh repo -> no
// cache dir yet), so once the cache dir appears the snapshot is taken and
// the test moves the ref out from under it before releasing the content
// frames. commit-pack and diff-commit answer upstream's lost-CAS 409 (spec
// 2026-10-28, #596): result.status "failed", and `commit` plus
// result.new_sha name the commit the branch did not get.
func TestCommitPackCASRace(t *testing.T) {
	patch := "diff --git a/b.txt b/b.txt\nnew file mode 100644\n--- /dev/null\n+++ b/b.txt\n@@ -0,0 +1 @@\n+b\n"
	for _, tc := range []struct {
		route   string
		files   []map[string]any
		content any
	}{
		{"commit-pack", []map[string]any{{"path": "b.txt", "content_id": "id-b", "operation": "upsert"}}, wctChunk("id-b", []byte("b\n"), true)},
		{"diff-commit", nil, wctDiffChunk([]byte(patch), true)},
	} {
		t.Run(tc.route, func(t *testing.T) {
			runBackends(t, func(t *testing.T, e *testEnv) {
				srv := wctServer(t, e)
				repo, writeTok, readTok := wctRepo(t, e)

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
					_, _ = pw.Write(wctLines(t, wctMeta("main", "raced", tc.files, nil)))
					// The handler lists refs (snapshotting old=c1) strictly before
					// materializing; wait for the cache repo to appear, then move
					// the ref to c2 before releasing the content frame.
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
					_, _ = pw.Write(wctLines(t, tc.content))
				}()

				req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/repos/"+tc.route, pr)
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
				env := wctRequireFail(t, resp, body, http.StatusConflict, "failed")
				if env.Result.OldSHA != c1 || env.Result.Branch != "main" {
					t.Errorf("lost-CAS result = %+v, want branch main and the stale snapshot %q", env.Result, c1)
				}
				cm := env.Commit
				if cm == nil || !ref.ValidSHA(cm.CommitSHA) || cm.CommitSHA != env.Result.NewSHA || cm.CommitSHA == c2 ||
					!ref.ValidSHA(cm.TreeSHA) || cm.TargetBranch != "main" || cm.PackBytes <= 0 || cm.BlobCount == nil || *cm.BlobCount != 1 {
					t.Fatalf("lost-CAS commit = %s, want the written commit in commit and result.new_sha", body)
				}

				// The loser's ref must be untouched and its pack merely
				// unreferenced: main still points at the winner, and the
				// reported commit is the loser's (parent c1, with b.txt).
				tips, err := e.refStore.ListRefs(context.Background(), repo.RepoID)
				if err != nil || tips["refs/heads/main"] != c2 {
					t.Errorf("main = %q (err %v), want winner %q", tips["refs/heads/main"], err, c2)
				}
				var got struct {
					Commit commitInfo `json:"commit"`
				}
				wctReadJSON(t, e, "/api/v1/repos/commit?ref="+cm.CommitSHA, readTok, &got)
				if !slices.Equal(got.Commit.ParentSHAs, []string{c1}) || got.Commit.Message != "raced" {
					t.Errorf("reported commit %s = %+v, want message raced on parent %s", cm.CommitSHA, got.Commit, c1)
				}
			})
		})
	}
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

		t.Run("empty diff is 412 no changes", func(t *testing.T) {
			frames := wctLines(t,
				wctMeta("main", "empty", nil, nil),
				wctDiffChunk(nil, true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(frames))
			env := wctRequireFail(t, resp, respBody, http.StatusPreconditionFailed, "precondition_failed")
			if env.Result.Message != "no changes to commit" || env.Result.OldSHA != diffTip || env.Commit != nil {
				t.Errorf("empty diff result = %s, want no changes to commit on %s", respBody, diffTip)
			}
			if tip := wctBranchTip(t, e, readTok, "main", false); tip != diffTip {
				t.Errorf("tip = %q after an empty diff, want %q", tip, diffTip)
			}
		})

		t.Run("non-applying patch is 409 conflict", func(t *testing.T) {
			// The same patch again: a.txt no longer has "line2" context.
			frames := wctLines(t,
				wctMeta("main", "conflict", nil, nil),
				wctDiffChunk([]byte(patch), true),
			)
			resp, respBody := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(frames))
			wctRequireFail(t, resp, respBody, http.StatusConflict, "conflict")
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

// TestDiffCommitApplyFailures (#608; spec 409: "When the diff does not
// apply, the body is application/json and result.status is conflict"):
// a parseable patch that does not apply to the base tree answers 409
// conflict, and a payload that is not a patch or names a path no tree can
// hold answers 400 invalid (spec 400: "the diff payload was invalid").
// Neither moves a ref.
func TestDiffCommitApplyFailures(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, _ := wctRepo(t, e)
		upsert := func(path, mode string) map[string]any {
			return map[string]any{"path": path, "content_id": "id-" + path, "operation": "upsert", "mode": mode}
		}
		binBase := []byte{0x00, 0x01, 'b', 'i', 'n'}
		// A path whose name pushes git's second clash line past the first
		// wcMaxApplyMsgBytes of stderr (#613).
		longFile := strings.Repeat(strings.Repeat("a", 200)+"/", 24) + "f"
		resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "base", []map[string]any{
				upsert("a.txt", "100644"), upsert("link", "120000"), upsert("d/f", "100644"), upsert("bin.dat", "100644"), upsert(longFile, "100644"),
			}, nil),
			wctChunk("id-a.txt", []byte("line1\nline2\nline3\n"), true),
			wctChunk("id-link", []byte("a.txt"), true),
			wctChunk("id-d/f", []byte("x\n"), true),
			wctChunk("id-bin.dat", binBase, true),
			wctChunk("id-"+longFile, []byte("x\n"), true),
		)))
		wctRequireOK(t, resp, body)

		// A real binary patch whose preimage is not the server's bin.dat.
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		if err := os.WriteFile(filepath.Join(dir, "bin.dat"), []byte{0x00, 0x02, 'o', 't', 'h'}, 0o644); err != nil {
			t.Fatal(err)
		}
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "other")
		if err := os.WriteFile(filepath.Join(dir, "bin.dat"), []byte{0x00, 0x03, 'n', 'e', 'w'}, 0o644); err != nil {
			t.Fatal(err)
		}
		binPatch := coreGit(t, dir, "diff", "--binary")
		if !strings.Contains(binPatch, "GIT binary patch") {
			t.Fatalf("test bug: no binary hunk:\n%s", binPatch)
		}

		newFile := func(path string) string {
			return "diff --git a/" + path + " b/" + path + "\nnew file mode 100644\n--- /dev/null\n+++ b/" + path + "\n@@ -0,0 +1 @@\n+x\n"
		}
		newDir := func(path string) string {
			return "diff --git a/" + path + " b/" + path + "\nnew file mode 040000\n--- /dev/null\n+++ b/" + path + "\n@@ -0,0 +1 @@\n+x\n"
		}
		for _, tc := range []struct {
			name, patch string
			wantCode    int
			wantMsg     string
		}{
			{"context mismatch", "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1,3 +1,3 @@\n line1\n-XXXX\n+LINE2\n line3\n",
				http.StatusConflict, "patch does not apply"},
			{"modify a missing file", "diff --git a/nope.txt b/nope.txt\n--- a/nope.txt\n+++ b/nope.txt\n@@ -1 +1 @@\n-a\n+b\n",
				http.StatusConflict, "does not exist in index"},
			{"create an existing file", newFile("a.txt"), http.StatusConflict, "already exists in index"},
			{"delete a missing file", "diff --git a/nope.txt b/nope.txt\ndeleted file mode 100644\n--- a/nope.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-a\n",
				http.StatusConflict, "does not exist in index"},
			{"rename a missing file", "diff --git a/nope.txt b/new.txt\nsimilarity index 100%\nrename from nope.txt\nrename to new.txt\n",
				http.StatusConflict, "does not exist in index"},
			{"mode change on a missing file", "diff --git a/nope.txt b/nope.txt\nold mode 100644\nnew mode 100755\n",
				http.StatusConflict, "does not exist in index"},
			{"path beyond a symlink", newFile("link/x"), http.StatusConflict, "beyond a symbolic link"},
			{"file where a directory is", newFile("d"), http.StatusConflict, "both a file and as a directory"},
			// git's clash line starts past the first 4096 bytes of stderr.
			{"directory where a long-named file is", newFile(longFile + "/x"), http.StatusConflict, "patch does not apply"},
			{"binary preimage mismatch", binPatch, http.StatusConflict, "does not match the current contents"},
			{"not a patch", "hello world\nnot a patch\n", http.StatusBadRequest, "diff payload is not a valid patch: error: No valid patches"},
			{"corrupt hunk", "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1,3 +1,3 @@\n line1\n",
				http.StatusBadRequest, "diff payload is not a valid patch: error: corrupt patch"},
			{"binary diff without data", "diff --git a/bin.dat b/bin.dat\nBinary files a/bin.dat and b/bin.dat differ\n",
				http.StatusBadRequest, "diff payload is not a valid patch: error: No valid patches"},
			{".git path", newFile(".git/config"), http.StatusBadRequest, "invalid patch: .git component"},
			{"parent path", newFile("../x"), http.StatusBadRequest, "invalid patch: invalid path segment"},
			{"absolute path", "diff --git a//etc/x b//etc/x\nnew file mode 100644\n--- /dev/null\n+++ b//etc/x\n@@ -0,0 +1 @@\n+x\n",
				http.StatusBadRequest, "invalid patch: absolute path"},
			{"rename into .git", "diff --git a/a.txt b/.git/x\nsimilarity index 100%\nrename from a.txt\nrename to .git/x\n",
				http.StatusBadRequest, "invalid patch: .git component"},
			// git apply names only the new path of a rename; an old path no
			// tree may hold is never in the base, so the patch does not apply.
			{"rename from a .git path", "diff --git a/.git/x b/c.txt\nsimilarity index 100%\nrename from .git/x\nrename to c.txt\n",
				http.StatusConflict, "does not exist in index"},
			// Only git refuses these after --numstat parsed them (#613): its
			// own path rules (core.protectNTFS is on by default) and a mode
			// line that changes nothing git can record. No base tree helps.
			{".git. path", newFile(".git./x"), http.StatusBadRequest, "diff payload is not a valid patch: error: invalid path '.git./x'"},
			{"git~1 path", newFile("git~1/x"), http.StatusBadRequest, "diff payload is not a valid patch: error: invalid path 'git~1/x'"},
			{"nested .git. path", newFile("d/.git./x"), http.StatusBadRequest, "diff payload is not a valid patch: error: invalid path 'd/.git./x'"},
			{".gitmodules symlink", "diff --git a/.gitmodules b/.gitmodules\nnew file mode 120000\n--- /dev/null\n+++ b/.gitmodules\n@@ -0,0 +1 @@\n+a.txt\n\\ No newline at end of file\n",
				http.StatusBadRequest, "diff payload is not a valid patch: error: invalid path '.gitmodules'"},
			{"unusable new mode", "diff --git a/a.txt b/a.txt\nold mode 100644\nnew mode 100664\n", http.StatusBadRequest, "diff payload is not a valid patch: error: patch with only garbage"},
			{"unusable new mode alone", "diff --git a/a.txt b/a.txt\nnew mode 100664\n", http.StatusBadRequest, "diff payload is not a valid patch: error: patch with only garbage"},
			{"submodule without a commit", "diff --git a/sub b/sub\nnew file mode 160000\n--- /dev/null\n+++ b/sub\n@@ -0,0 +1 @@\n+Subproject commit zzz\n",
				http.StatusBadRequest, "diff payload is not a valid patch: error: corrupt patch for submodule sub"},
			{"submodule with a null commit", "diff --git a/sub b/sub\nnew file mode 160000\n--- /dev/null\n+++ b/sub\n@@ -0,0 +1 @@\n+Subproject commit 0000000000000000000000000000000000000000\n",
				http.StatusBadRequest, "diff payload is not a valid patch: error: cache entry has null sha1"},
			// `--numstat` prints a record with an empty path for a header
			// without names. No tree holds an empty path (#613).
			{"header without a name", "diff --git a/ b/\nold mode 100644\nnew mode 100755\n", http.StatusBadRequest, "invalid patch: empty path"},
			// git 2.55 aborts on an assertion when a header names a new file
			// twice, after --numstat parsed the patch. It is the patch's own
			// fault, so a 400 and never a 500 (#613).
			// The assertion text depends on the libc (glibc, musl, macOS), so
			// the rows pin the status and the wording in front of it.
			{"repeated header and a new file", "diff --git a/e b/e\ndiff --git a/e b/e\nnew file mode 100644\nindex 0000000..e69de29\n",
				http.StatusBadRequest, "diff payload is not a valid patch: "},
			{"/dev/null and a new file mode", "diff --git a/n b/n\n--- /dev/null\nindex 0000000..ce01362\nnew file mode 100644\n+++ b/n\n@@ -0,0 +1 @@\n+x\n",
				http.StatusBadRequest, "diff payload is not a valid patch: "},
			// git apply --cached takes a directory mode, and write-tree then
			// leaves the entry out of the tree (see TestDiffCommitNeverDropsAFile).
			{"new file with a directory mode", newDir("dd"), http.StatusBadRequest, "invalid patch: invalid mode \"040000\" for \"dd\""},
			{"mode change to a directory mode", "diff --git a/a.txt b/a.txt\nold mode 100644\nnew mode 040000\n", http.StatusBadRequest,
				"invalid patch: invalid mode \"040000\" for \"a.txt\""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				before := wmRefsSnapshot(t, e, repo.RepoID)
				resp, body := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok,
					bytes.NewReader(wctLines(t, wctMeta("main", "d", nil, nil), wctDiffChunk([]byte(tc.patch), true))))
				env := wctRequireFail(t, resp, body, tc.wantCode, statusForHTTP(tc.wantCode))
				if !strings.Contains(env.Result.Message, tc.wantMsg) || env.Commit != nil {
					t.Fatalf("result = %s, want message containing %q and no commit", body, tc.wantMsg)
				}
				// A 409 says "patch does not apply" and then git's message,
				// with no warning before it. Each 400 row names its own wording.
				if tc.wantCode == http.StatusConflict && !strings.HasPrefix(env.Result.Message, "patch does not apply: error: ") {
					t.Fatalf("message %q does not start with %q", env.Result.Message, "patch does not apply: error: ")
				}
				// git reads the patch from stdin: no server path or command line.
				if strings.Contains(env.Result.Message, "patch.diff") || strings.Contains(env.Result.Message, "--cached") {
					t.Fatalf("message %q leaks the server's scratch path or git command", env.Result.Message)
				}
				if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
					t.Fatalf("refs moved: %v -> %v", before, after)
				}
			})
		}

		// Control: a rename that fits the base commits, as one file.
		resp, body = wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(wctLines(t, wctMeta("main", "mv", nil, nil),
			wctDiffChunk([]byte("diff --git a/a.txt b/b.txt\nsimilarity index 100%\nrename from a.txt\nrename to b.txt\n"), true))))
		if env := wctRequireOK(t, resp, body); env.Commit.BlobCount == nil || *env.Commit.BlobCount != 1 {
			t.Fatalf("rename blob_count = %v, want 1", env.Commit.BlobCount)
		}

		// Controls for #613: git takes a regular .gitmodules and a mode line
		// that changes the mode, so neither is a blanket 400.
		for name, patch := range map[string]string{
			".gitmodules file": newFile(".gitmodules"),
			"mode change":      "diff --git a/b.txt b/b.txt\nold mode 100644\nnew mode 100755\n",
			// git stores a loose file mode as 100644, a symlink and a
			// submodule as they are: none is a directory mode.
			"loose file mode": "diff --git a/loose b/loose\nnew file mode 100664\n--- /dev/null\n+++ b/loose\n@@ -0,0 +1 @@\n+x\n",
			"symlink":         "diff --git a/ln b/ln\nnew file mode 120000\n--- /dev/null\n+++ b/ln\n@@ -0,0 +1 @@\n+a.txt\n\\ No newline at end of file\n",
			"submodule":       "diff --git a/sub b/sub\nnew file mode 160000\n--- /dev/null\n+++ b/sub\n@@ -0,0 +1 @@\n+Subproject commit 1111111111111111111111111111111111111111\n",
		} {
			resp, body = wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(wctLines(t, wctMeta("main", name, nil, nil), wctDiffChunk([]byte(patch), true))))
			wctRequireOK(t, resp, body)
		}
	})
}

// TestDiffCommitGitFaultIs500 (#613): git apply exits 128 for a failure of
// its own just as it does for a patch it refuses on sight, so only its message
// tells them apart, and a file name that holds a newline can forge any message
// line. A failure of the server's git is a 500 with the generic body and no
// ref moves. The body never shows git's stderr when it names the server's
// scratch directory, also when the patch's file names forge the lines that
// the classifier reads for a 400 or a 409. A patch can forge a fatal line
// without a fault, and then its own 400 answers 500.
func TestDiffCommitGitFaultIs500(t *testing.T) {
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	// The wrapper makes git's own work fail just before `git apply --cached`
	// runs. Mode "lock" holds the scratch index lock. Mode "objects" makes the
	// scratch object directory unwritable for that one run. Mode "config"
	// breaks the config that `git apply --numstat` reads, and mode
	// "config-summary" the one that `git apply --summary` reads.
	bin := t.TempDir()
	modeFile := filepath.Join(bin, "mode")
	wrapper := fmt.Sprintf(`#!/bin/sh
cached=
if [ "$1" = apply ]; then for a in "$@"; do [ "$a" = --cached ] && cached=1; done; fi
mode=$(cat %[1]q 2>/dev/null)
if [ -n "$cached" ] && [ "$mode" = lock ]; then : > "$GIT_INDEX_FILE.lock"; fi
if [ "$mode" = config ]; then for a in "$@"; do [ "$a" = --numstat ] && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.bare GIT_CONFIG_VALUE_0=maybe exec %[2]q "$@"; done; fi
if [ "$mode" = config-summary ]; then for a in "$@"; do [ "$a" = --summary ] && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.bare GIT_CONFIG_VALUE_0=maybe exec %[2]q "$@"; done; fi
if [ -n "$cached" ] && [ "$mode" = objects ]; then
	chmod 500 "$GIT_OBJECT_DIRECTORY"
	%[2]q "$@"; rc=$?
	chmod 700 "$GIT_OBJECT_DIRECTORY"
	exit $rc
fi
exec %[2]q "$@"
`, modeFile, realGit)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	newFile := func(quotedName string) string {
		return "diff --git \"a/" + quotedName + "\" \"b/" + quotedName + "\"\nnew file mode 100644\n--- /dev/null\n+++ \"b/" + quotedName + "\"\n@@ -0,0 +1 @@\n+hi\n"
	}
	for _, tc := range []struct {
		name, mode, patch string
	}{
		{"held index lock", "lock", "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-one\n+two\n"},
		{"broken config in the parse step", "config", "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-one\n+two\n"},
		{"broken config in the mode step", "config-summary", "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-one\n+two\n"},
		{"unwritable object directory", "objects", newFile("n.txt")},
		// git prints a name as it is, so these names put a line of a payload
		// error and of a clash into the stderr of a server fault.
		{"unwritable object directory, name forges an invalid path", "objects", newFile(`n\nerror: invalid path 'z`)},
		{"unwritable object directory, name forges a clash", "objects", newFile(`m\nerror: unable to add cache entry for q`)},
		// No fault at all: git repeats the rest of the patch after an invalid
		// mode, so the patch writes a fatal line into its own 400.
		{"a patch forges a fatal line after an invalid mode", "none", "diff --git a/a.txt b/a.txt\nold mode 100644\nnew mode 10x644\nfatal: boom\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.mode == "objects" && os.Geteuid() == 0 {
				t.Skip("root ignores directory permissions")
			}
			if err := os.WriteFile(modeFile, []byte(tc.mode), 0o644); err != nil {
				t.Fatal(err)
			}
			runBackends(t, func(t *testing.T, e *testEnv) {
				srv := wctServer(t, e)
				repo, writeTok, _ := wctRepo(t, e)
				resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
					wctMeta("main", "base", []map[string]any{{"path": "a.txt", "content_id": "id-a", "operation": "upsert"}}, nil),
					wctChunk("id-a", []byte("one\n"), true),
				)))
				wctRequireOK(t, resp, body)

				before := wmRefsSnapshot(t, e, repo.RepoID)
				resp, body = wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok,
					bytes.NewReader(wctLines(t, wctMeta("main", "d", nil, nil), wctDiffChunk([]byte(tc.patch), true))))
				wctRequireFail(t, resp, body, http.StatusInternalServerError, statusForHTTP(http.StatusInternalServerError))
				if !bytes.Contains(body, []byte(internalErrorMsg)) {
					t.Fatalf("body %s does not carry the generic internal error", body)
				}
				for _, leak := range []string{"index.lock", "openstorage-write-", os.TempDir(), "git apply", "fatal", "insufficient permission", "objects", "config", "core.bare"} {
					if bytes.Contains(body, []byte(leak)) {
						t.Fatalf("body %s leaks %q from git's stderr", body, leak)
					}
				}
				if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
					t.Fatalf("refs moved: %v -> %v", before, after)
				}
			})
		})
	}
}

// TestDiffCommitNeverDropsAFile: `git apply --cached` takes a patch that
// gives a file a directory mode (040000) with exit 0, and write-tree leaves
// that entry out of the tree. Such a commit answered 200 without the file.
// A patch either lands every file it names or fails with a 400 that moves no
// ref.
func TestDiffCommitNeverDropsAFile(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, readTok := wctRepo(t, e)
		resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "base", []map[string]any{{"path": "a.txt", "content_id": "id-a", "operation": "upsert"}}, nil),
			wctChunk("id-a", []byte("one\n"), true),
		)))
		wctRequireOK(t, resp, body)

		file := func(mode, path string) string {
			return "diff --git a/" + path + " b/" + path + "\nnew file mode " + mode + "\n--- /dev/null\n+++ b/" + path + "\n@@ -0,0 +1 @@\n+x\n"
		}
		for _, tc := range []struct {
			name, patch string
			paths       []string
		}{
			{"a directory mode and a file", file("100644", "ok1.txt") + file("040000", "dd1"), []string{"ok1.txt", "dd1"}},
			{"a directory mode and a file, in the other order", file("040000", "dd2") + file("100644", "ok2.txt"), []string{"dd2", "ok2.txt"}},
			{"a directory mode only", file("040000", "dd3"), []string{"dd3"}},
			{"a directory mode under a directory", file("040000", "x/y/dd4"), []string{"x/y/dd4"}},
			{"a mode change to a directory mode and a file", file("100644", "ok5.txt") + "diff --git a/a.txt b/a.txt\nold mode 100644\nnew mode 040000\n", []string{"ok5.txt"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				before := wmRefsSnapshot(t, e, repo.RepoID)
				resp, body := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok,
					bytes.NewReader(wctLines(t, wctMeta("main", "d", nil, nil), wctDiffChunk([]byte(tc.patch), true))))
				if resp.StatusCode == http.StatusOK {
					for _, path := range tc.paths {
						resp, _ := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path="+path+"&ref=main", readTok, nil, "")
						if resp.StatusCode != http.StatusOK {
							t.Errorf("commit answered 200 and %s is not in it (file read answered %d): %s", path, resp.StatusCode, body)
						}
					}
					return
				}
				wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid")
				if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
					t.Fatalf("refs moved: %v -> %v", before, after)
				}
			})
		}
	})
}

// TestDiffCommitAcceptsSummaryModesAsWritten: an older git prints the mode
// of a new file as the patch wrote it (100664), and git apply --cached takes
// that patch. A stand-in git rewrites the summary like that, and the commit
// must still land.
func TestDiffCommitAcceptsSummaryModesAsWritten(t *testing.T) {
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	wrapper := fmt.Sprintf(`#!/bin/sh
for a in "$@"; do
	if [ "$a" = --summary ]; then
		%[1]q "$@" | sed 's/ create mode 100644 / create mode 100664 /; s/ create mode 100755 / create mode 100775 /'
		exit 0
	fi
done
exec %[1]q "$@"
`, realGit)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, readTok := wctRepo(t, e)
		resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "base", []map[string]any{{"path": "a.txt", "content_id": "id-a", "operation": "upsert"}}, nil),
			wctChunk("id-a", []byte("one\n"), true),
		)))
		wctRequireOK(t, resp, body)
		patch := "diff --git a/loose.txt b/loose.txt\nnew file mode 100644\n--- /dev/null\n+++ b/loose.txt\n@@ -0,0 +1 @@\n+x\n" +
			"diff --git a/run.sh b/run.sh\nnew file mode 100755\n--- /dev/null\n+++ b/run.sh\n@@ -0,0 +1 @@\n+x\n"
		resp, body = wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok,
			bytes.NewReader(wctLines(t, wctMeta("main", "d", nil, nil), wctDiffChunk([]byte(patch), true))))
		wctRequireOK(t, resp, body)
		for _, path := range []string{"loose.txt", "run.sh"} {
			if resp, _ := e.doReq(t, http.MethodGet, "/api/v1/repos/file?path="+path+"&ref=main", readTok, nil, ""); resp.StatusCode != http.StatusOK {
				t.Errorf("%s is not in the commit (file read answered %d)", path, resp.StatusCode)
			}
		}
	})
}

// TestPatchModeError: the modes `git apply --summary` reports that no commit
// can hold. git canonicalizes every mode, so only 040000 is outside
// wcValidModes.
func TestPatchModeError(t *testing.T) {
	for _, tc := range []struct {
		name, summary string
		want          string // the error, or "" for none
	}{
		{"nothing", "", ""},
		{"a file", " create mode 100644 a.txt\n", ""},
		{"an executable, a symlink and a submodule", " create mode 100755 a\n create mode 120000 b\n create mode 160000 c\n", ""},
		// An older git prints the mode as the patch wrote it.
		{"a loose file mode", " create mode 100664 a\n create mode 100777 b\n mode change 100644 => 100666 c\n", ""},
		{"a mode with a leading zero", " create mode 0100644 a\n", ""},
		{"a mode change", " mode change 100644 => 100755 a.txt\n", ""},
		{"a rename, a delete and a copy", " rename a => b (100%)\n delete mode 040000 c\n copy d => e (90%)\n", ""},
		{"a directory", " create mode 040000 dd\n", `invalid mode "040000" for "dd"`},
		{"a directory after a file", " create mode 100644 ok\n create mode 040000 dd\n", `invalid mode "040000" for "dd"`},
		{"a mode change to a directory", " mode change 100644 => 040000 a.txt\n", `invalid mode "040000" for "a.txt"`},
		{"a mode change that git printed without a new mode", " mode change 100644\n", `invalid mode "" for ""`},
		{"a directory with permission bits", " create mode 040755 dd\n", `invalid mode "040755" for "dd"`},
		{"a fifo", " create mode 010644 a\n", `invalid mode "010644" for "a"`},
		{"a socket", " mode change 100644 => 140755 a\n", `invalid mode "140755" for "a"`},
		{"a mode that is not octal", " create mode 10x644 a\n", `invalid mode "10x644" for "a"`},
		{"a mode of zero", " create mode 0 a\n", `invalid mode "0" for "a"`},
		{"a directory name with a space", " create mode 040000 a b\n", `invalid mode "040000" for "a b"`},
		{"no final newline", " create mode 040000 dd", `invalid mode "040000" for "dd"`},
		// The summary does not quote a newline in a name, so a name can write
		// the line itself. That makes its own patch a 400.
		{"a name that forges a line", " create mode 100644 x\n create mode 040000 y\n", `invalid mode "040000" for "y"`},
		{"a line that does not start the line", "x create mode 040000 y\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := wcPatchModeError(tc.summary)
			if got := fmt.Sprint(err); (err == nil) != (tc.want == "") || (err != nil && got != tc.want) {
				t.Fatalf("wcPatchModeError(%q) = %v, want %q", tc.summary, err, tc.want)
			}
		})
	}
}

// wctFakeGit puts a `git` first on PATH that prints a stderr and ends as
// told, so a test can give applyPatch any message and status. It returns a
// scratch over repoPath, a patch file in it, and set, which chooses the next
// run: exit is a number, or "abort" or "term" for a git that dies of that
// signal.
func wctFakeGit(t *testing.T, repoPath string) (sc *wcScratch, patch string, set func(t *testing.T, exit, stderr string)) {
	t.Helper()
	bin := t.TempDir()
	stderrFile, exitFile := filepath.Join(bin, "stderr"), filepath.Join(bin, "exit")
	wrapper := fmt.Sprintf("#!/bin/sh\nulimit -c 0\ncat %q >&2\ncode=$(cat %q)\ncase $code in abort) kill -ABRT $$;; term) kill -TERM $$;; esac\nexit $code\n", stderrFile, exitFile)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	sc, err := wcNewScratch(repoPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	patch = filepath.Join(sc.dir, "patch.diff")
	if err := os.WriteFile(patch, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return sc, patch, func(t *testing.T, exit, stderr string) {
		t.Helper()
		if err := os.WriteFile(stderrFile, []byte(stderr), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(exitFile, []byte(exit), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// gitFailsOnArgStderr is what gitFailsOnArg's git prints: a server-side
// read error, which no response may echo.
const gitFailsOnArgStderr = "fatal: unable to read 1111111111111111111111111111111111111111"

// gitFailsOnArg puts a git first on PATH that runs the real git, except
// that a call with the argument arg prints gitFailsOnArgStderr and fails:
// how is an exit status, or "kill" for a git that dies of SIGKILL.
func gitFailsOnArg(t *testing.T, arg, how string) {
	t.Helper()
	gitFailsOnArgWith(t, arg, how, gitFailsOnArgStderr)
}

// gitFailsOnArgWith is gitFailsOnArg with stderr as git's message.
func gitFailsOnArgWith(t *testing.T, arg, how, stderr string) {
	t.Helper()
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	argFile := filepath.Join(bin, "arg")
	if err := os.WriteFile(argFile, []byte(arg+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fail := "exit " + how
	if how == "kill" {
		fail = "kill -KILL $$"
	}
	// read, [ and kill are shell builtins: the server runs some git
	// children with an environment that has no PATH.
	fail = "echo '" + strings.ReplaceAll(stderr, "'", `'\''`) + "' >&2; " + fail
	script := fmt.Sprintf("#!/bin/sh\nread -r fail < %q\nfor a in \"$@\"; do [ \"$a\" = \"$fail\" ] && { %s; }; done\nexec %q \"$@\"\n", argFile, fail, real)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestApplyPatchServerFailure: git's stderr that names the server's scratch
// directory or the cached repository, or that has a fatal line, is a failure
// of the server's git whatever other lines it holds, and the same lines
// without that keep their status.
func TestApplyPatchServerFailure(t *testing.T) {
	repoPath := t.TempDir()
	sc, patch, set := wctFakeGit(t, repoPath)
	for _, tc := range []struct {
		name, stderr string
		exit         string
		want         int
		server       bool
	}{
		{"payload line", "error: invalid path 'z'", "128", http.StatusBadRequest, false},
		{"payload line after a name that holds the scratch path", "error: unable to create backing store for newly created file n\nerror: invalid path '" + sc.dir + "/objects'", "128", http.StatusInternalServerError, true},
		{"clash line and the cached repository", "error: unable to add cache entry for " + repoPath + "/objects\n", "128", http.StatusInternalServerError, true},
		{"does not apply and the scratch path", "error: patch failed: " + sc.dir + ":1", "1", http.StatusInternalServerError, true},
		{"does not apply", "error: patch failed: f:1", "1", http.StatusConflict, false},
		{"fatal line", "fatal: bad config line 5 in file ./config", "128", http.StatusInternalServerError, true},
		{"fatal line after a payload line", "error: invalid path 'z'\nfatal: unable to read blob object 0000", "128", http.StatusInternalServerError, true},
		// git's own prefix is "fatal: ": a name that starts a line with
		// "fatal:" and no space is the client's text.
		{"fatal without a space", "error: patch failed: f:1\nfatal:x", "1", http.StatusConflict, false},
		{"fatal inside a line", "error: patch failed: fatal: x:1", "1", http.StatusConflict, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set(t, tc.exit, tc.stderr)
			_, err := sc.applyPatch(context.Background(), patch, "--cached")
			var rej *wcApplyRejection
			if !errors.As(err, &rej) || rej.status != tc.want || rej.server != tc.server {
				t.Fatalf("err = %v, want a rejection with status %d and server %v", err, tc.want, tc.server)
			}
		})
	}
}

// TestApplyPatchServerPathBehindSymlinks: git prints a directory by its real
// path when the server reached it through a symlink (macOS TMPDIR, a cache
// dir on a linked volume), so the guard reads both spellings.
func TestApplyPatchServerPathBehindSymlinks(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	realRepo, realTmp := filepath.Join(root, "repo"), filepath.Join(root, "tmp")
	for _, d := range []string{realRepo, realTmp} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	linkRepo, linkTmp := filepath.Join(root, "repo-link"), filepath.Join(root, "tmp-link")
	for link, target := range map[string]string{linkRepo: realRepo, linkTmp: realTmp} {
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("TMPDIR", linkTmp)
	sc, patch, set := wctFakeGit(t, linkRepo)
	realScratch, err := filepath.EvalSymlinks(sc.dir)
	if err != nil || realScratch == sc.dir {
		t.Fatalf("test bug: scratch %q is not behind a symlink (%q, %v)", sc.dir, realScratch, err)
	}
	for _, tc := range []struct{ name, stderr string }{
		{"cached repository", "error: unable to add cache entry for " + realRepo + "/objects"},
		{"scratch directory", "error: unable to add cache entry for " + realScratch + "/index"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set(t, "128", tc.stderr)
			_, err := sc.applyPatch(context.Background(), patch, "--cached")
			var rej *wcApplyRejection
			if !errors.As(err, &rej) || !rej.server || rej.status != http.StatusInternalServerError {
				t.Fatalf("err = %v, want a server failure with status 500", err)
			}
		})
	}
}

// TestApplyPatchRejectionMessage: what the client sees of git's stderr, and
// what ends in a rejection at all. A git that dies of SIGABRT (an assertion
// over the patch) is a 400, any other signal is the server's own failure.
func TestApplyPatchRejectionMessage(t *testing.T) {
	sc, patch, set := wctFakeGit(t, t.TempDir())
	assertion := "git: apply.c:3856: check_preimage: Assertion `patch->is_new <= 0' failed."
	for _, tc := range []struct {
		name, exit, stderr string
		want               int
		wantMsg            func(string) bool
	}{
		{"long message is cut", "1", "error: " + strings.Repeat("é", 3000), http.StatusConflict, func(m string) bool {
			return strings.HasPrefix(m, "error: é") && strings.HasSuffix(m, " …") && len(m) <= wcMaxApplyMsgBytes+len(" …") && utf8.ValidString(m)
		}},
		{"no message", "1", "", http.StatusConflict, func(m string) bool { return m == "git apply exited with status 1" }},
		{"no message and a server status", "128", "", http.StatusInternalServerError, func(m string) bool { return m == "git apply exited with status 128" }},
		{"abort", "abort", assertion, http.StatusBadRequest, func(m string) bool { return m == assertion }},
		{"abort without a message", "abort", "", http.StatusBadRequest, func(m string) bool { return m == "git apply aborted" }},
		{"abort that names the scratch directory", "abort", "error: unable to read " + sc.dir + "/index\n" + assertion, http.StatusInternalServerError, func(string) bool { return true }},
		{"abort after a fatal line", "abort", "fatal: unable to read blob object 0000\n" + assertion, http.StatusInternalServerError, func(string) bool { return true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set(t, tc.exit, tc.stderr)
			_, err := sc.applyPatch(context.Background(), patch, "--cached")
			var rej *wcApplyRejection
			if !errors.As(err, &rej) {
				t.Fatalf("err = %v, want a rejection", err)
			}
			if rej.status != tc.want || !tc.wantMsg(rej.msg) {
				t.Fatalf("status %d (want %d), message of %d bytes %.200q", rej.status, tc.want, len(rej.msg), rej.msg)
			}
		})
	}
	t.Run("another signal", func(t *testing.T) {
		set(t, "term", "")
		_, err := sc.applyPatch(context.Background(), patch, "--cached")
		var rej *wcApplyRejection
		if err == nil || errors.As(err, &rej) {
			t.Fatalf("err = %v, want a plain error for a git killed by SIGTERM", err)
		}
	})
}

// TestWcNewScratchKeepsGitInsideScratch: git warns "unable to access
// '<path>'" for a file it cannot read, and a 400 or 409 shows that warning.
// Every file git reads outside the repository must therefore lie under the
// scratch directory, whatever HOME the server runs with.
func TestWcNewScratchKeepsGitInsideScratch(t *testing.T) {
	t.Setenv("HOME", "/home/server")
	t.Setenv("XDG_CONFIG_HOME", "/home/server/.config")
	t.Setenv("GIT_ATTR_NOSYSTEM", "0")
	sc, err := wcNewScratch(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sc.close()
	got := map[string]string{} // the last value wins, as in exec.Cmd
	for _, kv := range sc.baseEnv {
		k, v, _ := strings.Cut(kv, "=")
		got[k] = v
	}
	for _, k := range []string{"HOME", "XDG_CONFIG_HOME"} {
		if v := got[k]; v != sc.dir && !strings.HasPrefix(v, sc.dir+string(os.PathSeparator)) {
			t.Errorf("%s = %q, want a path under the scratch dir %q", k, v, sc.dir)
		}
	}
	if got["GIT_ATTR_NOSYSTEM"] != "1" {
		t.Errorf("GIT_ATTR_NOSYSTEM = %q, want 1", got["GIT_ATTR_NOSYSTEM"])
	}
}

// TestDiffCommitNeverShowsServersHome: git prints "warning: unable to access
// '<HOME>/.config/git/attributes'" on every `apply --cached` when the server
// user cannot read that file. The warning must not reach a 400 or 409 body.
func TestDiffCommitNeverShowsServersHome(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every file")
	}
	for _, tc := range []struct {
		name string
		xdg  bool
	}{{"HOME", false}, {"XDG_CONFIG_HOME", true}} {
		t.Run(tc.name, func(t *testing.T) {
			runBackends(t, func(t *testing.T, e *testEnv) {
				srv := wctServer(t, e)
				repo, writeTok, _ := wctRepo(t, e)
				resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
					wctMeta("main", "base", []map[string]any{{"path": "a.txt", "content_id": "id-a", "operation": "upsert"}}, nil),
					wctChunk("id-a", []byte("one\n"), true),
				)))
				wctRequireOK(t, resp, body)

				home := t.TempDir()
				attrs := filepath.Join(home, ".config", "git", "attributes")
				t.Setenv("XDG_CONFIG_HOME", "")
				if tc.xdg {
					t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
					attrs = filepath.Join(home, "xdg", "git", "attributes")
				}
				t.Setenv("HOME", home)
				if err := os.MkdirAll(filepath.Dir(attrs), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(attrs, []byte("* text\n"), 0o000); err != nil {
					t.Fatal(err)
				}
				if f, err := os.Open(attrs); err == nil {
					f.Close()
					t.Skip("the attributes file is readable here")
				}

				before := wmRefsSnapshot(t, e, repo.RepoID)
				for _, p := range []struct {
					name, patch string
					code        int
				}{
					{"does not apply", "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-zzz\n+two\n", http.StatusConflict},
					{"git refuses", "diff --git a/a.txt b/a.txt\nold mode 100644\nnew mode 100664\n", http.StatusBadRequest},
				} {
					resp, body := wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok,
						bytes.NewReader(wctLines(t, wctMeta("main", "d", nil, nil), wctDiffChunk([]byte(p.patch), true))))
					env := wctRequireFail(t, resp, body, p.code, statusForHTTP(p.code))
					for _, leak := range []string{home, "unable to access", "attributes", "warning:"} {
						if strings.Contains(env.Result.Message, leak) {
							t.Fatalf("%s: message %q shows %q from git's warning", p.name, env.Result.Message, leak)
						}
					}
				}
				if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
					t.Fatalf("refs moved: %v -> %v", before, after)
				}

				// A patch that fits still commits: git needs no HOME of its own.
				resp, body = wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(wctLines(t, wctMeta("main", "ok", nil, nil),
					wctDiffChunk([]byte("diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-one\n+two\n"), true))))
				wctRequireOK(t, resp, body)
			})
		})
	}
}

// TestApplyPatchRunsGitInEnglish: the 400 and 500 split reads git's English
// messages, so git runs under LC_ALL=C whatever locale the server has.
func TestApplyPatchRunsGitInEnglish(t *testing.T) {
	bin := t.TempDir()
	wrapper := "#!/bin/sh\necho \"locale=$LC_ALL\" >&2\nexit 128\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("LC_ALL", "de_DE.UTF-8")
	sc, err := wcNewScratch(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sc.close()
	patch := filepath.Join(sc.dir, "patch.diff")
	if err := os.WriteFile(patch, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = sc.applyPatch(context.Background(), patch, "--cached")
	var rej *wcApplyRejection
	if !errors.As(err, &rej) || rej.exit != 128 || rej.status != http.StatusInternalServerError || rej.msg != "locale=C" {
		t.Fatalf("err = %v, want exit 128 with message %q", err, "locale=C")
	}
}

// TestApplyRejectionStatus: which statuses git apply's exit codes and
// messages map to. The messages are git 2.55's; TestDiffCommitApplyFailures
// pins them against the git that runs the tests.
func TestApplyRejectionStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit int
		msg  string
		want int
	}{
		{"unsafe name", 128, "error: invalid path '.git./x'", http.StatusBadRequest},
		{"unusable mode", 128, "error: patch with only garbage at <stdin>:4", http.StatusBadRequest},
		{"submodule without a commit", 128, "error: corrupt patch for submodule sub", http.StatusBadRequest},
		{"submodule with a null commit", 128, "error: cache entry has null sha1: sub\nerror: Unable to write new index file", http.StatusBadRequest},
		{"after another line", 128, "warning: noise\nerror: invalid path 'git~1/x'", http.StatusBadRequest},
		// A name can put a clash line in front of git's own payload line:
		// the payload line decides, whatever comes first.
		{"clash line before a payload line", 128, "error: unable to add cache entry for x\nerror: invalid path 'y'", http.StatusBadRequest},
		{"file and directory", 128, "error: 'd' appears as both a file and as a directory\nerror: unable to add cache entry for d", http.StatusConflict},
		{"file and directory under a long path", 128, "error: '" + strings.Repeat("a", 3*wcMaxApplyMsgBytes) + "' appears as both a file and as a directory\nerror: unable to add cache entry for x", http.StatusConflict},
		{"does not apply", 1, "error: patch failed: f:1\nerror: f: patch does not apply", http.StatusConflict},
		{"status 1 is never a payload error", 1, "error: invalid path 'a'", http.StatusConflict},
		{"locked index", 128, "fatal: Unable to create '/tmp/openstorage-write-1/index.lock': File exists.", http.StatusInternalServerError},
		{"unreadable index", 128, "error: unable to read index file", http.StatusInternalServerError},
		{"unwritable index", 128, "error: Unable to write new index file", http.StatusInternalServerError},
		{"no room for the blob", 128, "error: unable to create backing store for newly created file a", http.StatusInternalServerError},
		{"no message", 128, "", http.StatusInternalServerError},
		{"not at a line start", 128, "fatal: x error: invalid path 'a'", http.StatusInternalServerError},
		{"a line that quotes the message", 128, "fatal: Unable to create '/tmp/x'\nhint: error: invalid path", http.StatusInternalServerError},
		{"other status", 129, "error: invalid path 'a'", http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := wcApplyStatus(tc.exit, tc.msg); got != tc.want {
				t.Fatalf("wcApplyStatus(%d, %q) = %d, want %d", tc.exit, tc.msg, got, tc.want)
			}
		})
	}
}

// TestCommitWriteNoChanges (#595, spec 2026-10-28): commit-pack and
// diff-commit answer 412 precondition_failed "no changes to commit" when the
// new tree equals the base tree (or the empty tree on an unborn branch), and
// a stream whose last file or diff has no eof chunk answers 412 too. Nothing
// is published and no ref moves.
func TestCommitWriteNoChanges(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, readTok := wctRepo(t, e)
		upsert := func(path, id string) map[string]any {
			return map[string]any{"path": path, "content_id": id, "operation": "upsert"}
		}
		post := func(route string, frames ...any) (*http.Response, []byte) {
			t.Helper()
			return wctPost(t, srv, "/api/v1/repos/"+route, writeTok, bytes.NewReader(wctLines(t, frames...)))
		}
		// requireNoMove asserts the failure and that every ref is unchanged.
		requireNoMove := func(t *testing.T, wantCode int, wantMsg string, route string, frames ...any) {
			t.Helper()
			before := wmRefsSnapshot(t, e, repo.RepoID)
			resp, body := post(route, frames...)
			env := wctRequireFail(t, resp, body, wantCode, statusForHTTP(wantCode))
			if !strings.Contains(env.Result.Message, wantMsg) || env.Commit != nil {
				t.Fatalf("result = %s, want message %q and no commit", body, wantMsg)
			}
			if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
				t.Fatalf("refs moved: %v -> %v", before, after)
			}
		}

		t.Run("unborn branch without file operations", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no changes to commit", "commit-pack", wctMeta("main", "m", nil, nil))
		})
		t.Run("unborn branch deleting a missing path", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no changes to commit", "commit-pack",
				wctMeta("main", "m", []map[string]any{{"path": "nope.txt", "content_id": "d", "operation": "delete"}}, nil))
		})
		t.Run("unborn branch empty diff", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no changes to commit", "diff-commit",
				wctMeta("main", "m", nil, nil), wctDiffChunk(nil, true))
		})
		if t.Failed() {
			return
		}

		resp, body := post("commit-pack", wctMeta("main", "c1", []map[string]any{upsert("a.txt", "ida")}, nil),
			wctChunk("ida", []byte("a\n"), true))
		c1 := wctRequireOK(t, resp, body)

		t.Run("same content as the tip", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no changes to commit", "commit-pack",
				wctMeta("main", "m", []map[string]any{upsert("a.txt", "ida")}, nil), wctChunk("ida", []byte("a\n"), true))
		})
		t.Run("no file operations", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no changes to commit", "commit-pack", wctMeta("main", "m", nil, nil))
		})
		t.Run("deletes only missing paths", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no changes to commit", "commit-pack",
				wctMeta("main", "m", []map[string]any{{"path": "nope.txt", "content_id": "d", "operation": "delete"}}, nil))
		})
		t.Run("new branch from base_branch without changes", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no changes to commit", "commit-pack",
				wctMeta("side", "m", nil, map[string]any{"base_branch": "main"}))
		})
		t.Run("empty diff", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no changes to commit", "diff-commit",
				wctMeta("main", "m", nil, nil), wctDiffChunk(nil, true))
		})
		t.Run("file without its eof chunk", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "before eof", "commit-pack",
				wctMeta("main", "m", []map[string]any{upsert("b.txt", "idb")}, nil), wctChunk("idb", []byte("b"), false))
		})
		t.Run("file without chunks", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "no blob chunks", "commit-pack",
				wctMeta("main", "m", []map[string]any{upsert("b.txt", "idb")}, nil))
		})
		t.Run("diff without its eof chunk", func(t *testing.T) {
			requireNoMove(t, http.StatusPreconditionFailed, "before diff_chunk eof", "diff-commit",
				wctMeta("main", "m", nil, nil), wctDiffChunk([]byte("diff --git a/x b/x\n"), false))
		})
		// The NDJSON syntax and validation errors keep 400 (spec: "The NDJSON
		// stream was malformed or metadata validation failed").
		t.Run("diff-commit without diff chunks stays 400", func(t *testing.T) {
			requireNoMove(t, http.StatusBadRequest, "diff content missing", "diff-commit", wctMeta("main", "m", nil, nil))
		})

		// Real changes still commit: an upsert that changes content, and a
		// delete of an existing path.
		resp, body = post("commit-pack", wctMeta("main", "c2", []map[string]any{upsert("a.txt", "ida")}, nil),
			wctChunk("ida", []byte("changed\n"), true))
		c2 := wctRequireOK(t, resp, body)
		if c2.Result.OldSHA != c1.Commit.CommitSHA {
			t.Fatalf("c2 old_sha = %q, want %q", c2.Result.OldSHA, c1.Commit.CommitSHA)
		}
		resp, body = post("commit-pack",
			wctMeta("main", "c3", []map[string]any{{"path": "a.txt", "content_id": "d", "operation": "delete"}}, nil))
		c3 := wctRequireOK(t, resp, body)
		if tip := wctBranchTip(t, e, readTok, "main", false); tip != c3.Commit.CommitSHA {
			t.Fatalf("tip = %q, want %q", tip, c3.Commit.CommitSHA)
		}
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
			// No commit_message: the message names the target.
			if got, want := commits.Commits[0].Message, "Restore to "+c1.Commit.CommitSHA; got != want {
				t.Errorf("restore message = %q, want %q", got, want)
			}
		})

		t.Run("stale guard is 409 ref_guard_failed", func(t *testing.T) {
			tip := wctBranchTip(t, e, readTok, "main", false)
			resp, body := restore(map[string]any{
				"target_branch":     "main",
				"target_commit_sha": c1.Commit.CommitSHA,
				"author":            wctAuthor,
				"expected_head_sha": c1.Commit.CommitSHA, // stale
			})
			assertCommitRefGuard(t, resp.StatusCode, body, "main", tip, tip)
			if got := wctBranchTip(t, e, readTok, "main", false); got != tip {
				t.Fatalf("main moved to %q after a failed guard, want %q", got, tip)
			}
		})

		t.Run("unknown target commit is 404", func(t *testing.T) {
			resp, body := restore(map[string]any{
				"target_branch":     "main",
				"target_commit_sha": strings.Repeat("ab", 20),
				"author":            wctAuthor,
			})
			wctRequireFail(t, resp, body, http.StatusNotFound, "not_found")
		})

		// Only git's exit 1 says that the target names no commit, and
		// only when the target is no commit: for c1, which exists, a
		// failed peel is the repository's failure.
		for _, how := range []string{"1", "128", "kill"} {
			t.Run("git "+how+" on the target is 500, not 404", func(t *testing.T) {
				gitFailsOnArg(t, c1.Commit.CommitSHA+"^{commit}^{tree}", how)
				resp, body := restore(map[string]any{
					"target_branch":     "main",
					"target_commit_sha": c1.Commit.CommitSHA,
					"author":            wctAuthor,
				})
				if resp.StatusCode != http.StatusInternalServerError {
					t.Fatalf("status = %d, want 500 (body %s)", resp.StatusCode, body)
				}
			})
		}

		// target_commit_sha names a commit: a tree or blob id that git
		// can peel to a tree must not be restored.
		t.Run("a tree or blob as target is 404", func(t *testing.T) {
			tip := wctBranchTip(t, e, readTok, "main", false)
			for _, id := range []string{c1.Commit.TreeSHA, fmt.Sprintf("%x", sha1.Sum([]byte("blob 4\x00one\n")))} {
				resp, body := restore(map[string]any{
					"target_branch":     "main",
					"target_commit_sha": id,
					"author":            wctAuthor,
				})
				wctRequireFail(t, resp, body, http.StatusNotFound, "not_found")
			}
			if got := wctBranchTip(t, e, readTok, "main", false); got != tip {
				t.Fatalf("main moved to %q, want %q", got, tip)
			}
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
			if msg := requireEnvelope(t, resp, body); resp.StatusCode != http.StatusForbidden || !strings.Contains(msg, "no-push") {
				t.Errorf("status %d, message = %q", resp.StatusCode, msg)
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
			// Issue #841: the answer is the fixed text, never the decoder's.
			tcdRequireFixed(t, "commit-pack", body, "malformed refs claim")
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
			registerWriteCommitRoutes(mux, s, auth.NewVerifier(e.store), "", nil)
			srv := httptest.NewServer(withProblemInstance(recoverPanics(discardLogger())(mux)))
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

		usage, err := blob.StorageUsageBytes(context.Background(), e.blobStore, repo.RepoID)
		if err != nil || usage == 0 {
			t.Fatalf("usage after first commit = (%d, %v), want > 0", usage, err)
		}

		// A 1-byte quota refuses new bytes with a 403 problem body and
		// stores nothing new.
		tiny := quotaSrv(1)
		resp, respBody = wctPost(t, tiny, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(smallBody("c2", "b.txt")))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("over-quota commit-pack = %d, want 403; body: %s", resp.StatusCode, respBody)
		}
		if msg := requireEnvelope(t, resp, respBody); !strings.Contains(msg, "quota") {
			t.Fatalf("over-quota error = %q, want a quota message", msg)
		}
		after, err := blob.StorageUsageBytes(context.Background(), e.blobStore, repo.RepoID)
		if err != nil || after != usage {
			t.Fatalf("denied write changed usage: %d -> %d (err %v)", usage, after, err)
		}
	})
}

// TestWriteReservationCapAnswers429 (#594): when the repository's active
// pack-reservation rows are at the ledger's cap, commit-pack, diff-commit,
// restore-commit and merge answer the retryable 429 with the upstream
// RefUpdateResult status "unavailable", Retry-After and RateLimit, and
// leave every ref alone. These handlers used to send status "busy", which
// statusForHTTP(429) rejects, so the assert panicked into a 500.
func TestWriteReservationCapAnswers429(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		ctx := context.Background()
		plain := wctServer(t, e)
		repo, writeTok, _ := wctRepo(t, e)
		commit := func(branch, msg, path string, extra map[string]any) wcEnvelope {
			t.Helper()
			body := wctLines(t,
				wctMeta(branch, msg, []map[string]any{
					{"path": path, "content_id": "id-" + path, "operation": "upsert"},
				}, extra),
				wctChunk("id-"+path, []byte(msg+"\n"), true),
			)
			resp, respBody := wctPost(t, plain, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			return wctRequireOK(t, resp, respBody)
		}
		c1 := commit("main", "c1", "a.txt", nil)
		commit("feature", "feat", "f.txt", map[string]any{"base_branch": "main"})
		commit("main", "c2", "m.txt", map[string]any{"expected_head_sha": c1.Commit.CommitSHA})

		// Fill the active-row cap (ref.maxActiveReservations, 10,000) with
		// one admitted batch, then prove it is full: a denied probe
		// inserts no row.
		group := blob.QuotaRepoGroup(repo.RepoID)
		fill := make([]ref.Pack, 10_000)
		for i := range fill {
			fill[i] = ref.Pack{Key: blob.PackKey(repo.RepoID, fmt.Sprintf("fill%06d", i)), Size: 1}
		}
		dec, err := e.refStore.ReservePackBytes(ctx, group, fill, ref.NewReservationOwner(), 0, ref.ReserveNoFloorGen, math.MaxInt64, time.Hour)
		if err != nil || !dec.OK {
			t.Fatalf("fill ReservePackBytes = (%+v, %v), want admitted", dec, err)
		}
		probe := []ref.Pack{{Key: blob.PackKey(repo.RepoID, "probe"), Size: 1}}
		dec, err = e.refStore.ReservePackBytes(ctx, group, probe, ref.NewReservationOwner(), 0, ref.ReserveNoFloorGen, math.MaxInt64, time.Hour)
		if err != nil || !dec.KeyCapExceeded {
			t.Fatalf("probe ReservePackBytes = (%+v, %v), want KeyCapExceeded", dec, err)
		}

		// A quota-enforcing server: only the quota path reserves rows.
		s := &server{
			store:              e.store,
			httpBaseURL:        testHTTPBaseURL,
			logger:             discardLogger(),
			refStore:           e.refStore,
			blobStore:          e.blobStore,
			cacheDir:           e.cacheDir,
			packQuotaBytes:     1 << 40,
			packReservationTTL: time.Hour,
		}
		mux := http.NewServeMux()
		verifier := auth.NewVerifier(e.store)
		registerWriteCommitRoutes(mux, s, verifier, "", nil)
		s.registerWriteMergeRoutes(mux, verifier, "", nil)
		srv := httptest.NewServer(withProblemInstance(recoverPanics(discardLogger())(mux)))
		t.Cleanup(srv.Close)

		patch := "diff --git a/n.txt b/n.txt\nnew file mode 100644\n--- /dev/null\n+++ b/n.txt\n@@ -0,0 +1 @@\n+n\n"
		restore, err := json.Marshal(map[string]any{"metadata": map[string]any{
			"target_branch": "main", "target_commit_sha": c1.Commit.CommitSHA, "author": wctAuthor,
		}})
		if err != nil {
			t.Fatal(err)
		}
		cases := []struct {
			name string
			do   func() (*http.Response, []byte)
		}{
			{"commit-pack", func() (*http.Response, []byte) {
				body := wctLines(t,
					wctMeta("main", "c3", []map[string]any{
						{"path": "b.txt", "content_id": "id-b", "operation": "upsert"},
					}, nil),
					wctChunk("id-b", []byte("b\n"), true),
				)
				return wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			}},
			{"diff-commit", func() (*http.Response, []byte) {
				body := wctLines(t, wctMeta("main", "d", nil, nil), wctDiffChunk([]byte(patch), true))
				return wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(body))
			}},
			{"restore-commit", func() (*http.Response, []byte) {
				return wctPost(t, srv, "/api/v1/repos/restore-commit", writeTok, bytes.NewReader(restore))
			}},
			{"merge", func() (*http.Response, []byte) {
				return wmDo(t, srv, http.MethodPost, "/api/v1/repos/merge", writeTok,
					wmMergeBody("feature", "main", "merge", map[string]any{"author": wctAuthor}))
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				before := wmRefsSnapshot(t, e, repo.RepoID)
				resp, body := tc.do()
				wctRequireFail(t, resp, body, http.StatusTooManyRequests, "unavailable")
				if msg := requireEnvelope(t, resp, body); !strings.Contains(msg, "retry later") {
					t.Fatalf("429 error = %q, want a retry message", msg)
				}
				if got := resp.Header.Get("Retry-After"); got != "10" {
					t.Fatalf("Retry-After = %q, want 10", got)
				}
				if got := resp.Header.Get("RateLimit"); got != `"default";r=0;t=10` {
					t.Fatalf("RateLimit = %q, want the default service limit with t=10", got)
				}
				if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
					t.Fatalf("denied write moved refs: %v -> %v", before, after)
				}
			})
		}
	})
}
