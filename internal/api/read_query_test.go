package api

// Contract integration tests for the p3 "query" read endpoints (docs/archive/phases/07-implementation-roadmap.md
// phases 2c/2d): GET /repos/diff, GET /repos/branches/diff, GET
// /repos/blame, POST /repos/grep, POST /repos/archive. All tests fan out
// over both storage backend lanes via runBackends and assert the docs/reference/http-api.md +
// openapi status-code and envelope contract against a repo seeded with a
// real multi-commit git history (rename, executable bit, symlink, binary
// and >1MiB files, ephemeral ref, annotated tag).

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

// --- fixture ---------------------------------------------------------------

// queryFixture is one seeded repo shared by every query-endpoint test of a
// backend lane (tests are read-only against it). workDir outlives the test
// that built it (os.MkdirTemp + envCleanup), because later tests replay
// `git apply --check` against it.
type queryFixture struct {
	org     string
	repoURL string
	repoID  string
	workDir string

	c1, c2, c3, c4 string // c1..c2: main; c3: feature (from c2); c4: main tip
}

var (
	queryFixtureMu sync.Mutex
	queryFixtures  = map[*testEnv]*queryFixture{}
)

func queryFixtureFor(t *testing.T, e *testEnv) *queryFixture {
	t.Helper()
	queryFixtureMu.Lock()
	defer queryFixtureMu.Unlock()
	if fx, ok := queryFixtures[e]; ok {
		return fx
	}
	fx := buildQueryFixture(t, e)
	queryFixtures[e] = fx
	return fx
}

// buildQueryFixture creates the repo through the API, builds the git
// history locally, and seeds the backend stores the way a push would:
// one pack with every object (blob store + pack registry) plus one DB ref
// row per fixture ref.
func buildQueryFixture(t *testing.T, e *testEnv) *queryFixture {
	t.Helper()
	ctx := context.Background()

	fx := &queryFixture{
		org:     e.newOrg(t),
		repoURL: "https://example.com/org/query-" + randomHex(t, 4),
	}
	fx.repoID = createRepo(t, e, fx.org, fx.repoURL, nil, http.StatusOK).RepoID

	dir, err := os.MkdirTemp("", "codestorage-query-fixture-*")
	if err != nil {
		t.Fatal(err)
	}
	envCleanup = append(envCleanup, func() { _ = os.RemoveAll(dir) })
	fx.workDir = dir

	rqGit(t, dir, "init", "-b", "main", ".")
	rqGit(t, dir, "config", "user.name", "Test Author")
	rqGit(t, dir, "config", "user.email", "author@example.com")

	// c1 (main): baseline tree with every entry kind the endpoints care
	// about — no-trailing-newline file, >1MiB text, binary, exec, symlink.
	rqWriteFile(t, dir, "docs/readme.md", "# readme\n\nhello TODO one\nworld\n")
	rqWriteFile(t, dir, "src/main.go", "package main\n\n// TODO first\nfunc main() {\n\tprintln(\"todo lower\")\n}\n")
	rqWriteFile(t, dir, "noeol.txt", "tail without newline")
	rqWriteFile(t, dir, "large.txt", strings.Repeat("filler line for the large text fixture\n", 27000)) // > 1 MiB
	rqWriteFileBytes(t, dir, "big.bin", append([]byte{0x00, 0x01, 0x02, 0xff}, bytes.Repeat([]byte{0x00, 0x42}, 512)...))
	rqWriteFileMode(t, dir, "tools/run.sh", "#!/bin/sh\necho run\n", 0o755)
	if err := os.Symlink("docs/readme.md", filepath.Join(dir, "link.md")); err != nil {
		t.Fatal(err)
	}
	rqGit(t, dir, "add", "-A")
	rqGit(t, dir, "commit", "-m", "c1: initial tree")
	fx.c1 = rqRev(t, dir, "HEAD")

	// c2 (main): rename, modify, add — the R/M/A diff triple.
	rqGit(t, dir, "mv", "docs/readme.md", "docs/intro.md")
	rqAppendFile(t, dir, "src/main.go", "\n// TODO second\n")
	rqWriteFile(t, dir, "notes.txt", "alpha\nbeta\ngamma\n")
	rqGit(t, dir, "add", "-A")
	rqGit(t, dir, "commit", "-m", "c2: rename, modify, add")
	fx.c2 = rqRev(t, dir, "HEAD")

	// c3 (feature, branched at c2): blame/diff material.
	rqGit(t, dir, "checkout", "-b", "feature")
	rqWriteFile(t, dir, "notes.txt", "alpha\nBETA\ngamma\ndelta\n")
	rqWriteFile(t, dir, "feature.md", "feature TODO\n")
	rqGit(t, dir, "add", "-A")
	rqGit(t, dir, "commit", "-m", "c3: feature edits")
	fx.c3 = rqRev(t, dir, "HEAD")

	// c4 (main advances past the branch point): add + delete.
	rqGit(t, dir, "checkout", "main")
	rqWriteFile(t, dir, "main-only.txt", "mainline\n")
	rqGit(t, dir, "rm", "-q", "noeol.txt")
	rqGit(t, dir, "add", "-A")
	rqGit(t, dir, "commit", "-m", "c4: main advance")
	fx.c4 = rqRev(t, dir, "HEAD")

	rqGit(t, dir, "tag", "-a", "v1", "-m", "v1", fx.c1)
	rqGit(t, dir, "update-ref", auth.EphemeralRefName("eph"), fx.c3)

	// Seed the stores: one pack holding every object, then the ref rows.
	packFile, err := os.CreateTemp("", "codestorage-query-seed-*.pack")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(packFile.Name())
	packCmd := exec.Command("git", "pack-objects", "--all", "--stdout")
	packCmd.Dir = dir
	packCmd.Stdout = packFile
	var stderr bytes.Buffer
	packCmd.Stderr = &stderr
	if err := packCmd.Run(); err != nil {
		t.Fatalf("git pack-objects: %v\n%s", err, stderr.String())
	}
	if err := packFile.Close(); err != nil {
		t.Fatal(err)
	}
	packID := randomHex(t, 20)
	key := blob.PackKey(fx.repoID, packID)
	if err := e.blobStore.UploadFile(ctx, key, packFile.Name()); err != nil {
		t.Fatalf("upload seed pack: %v", err)
	}
	if err := e.refStore.RegisterPacks(ctx, fx.repoID, []ref.Pack{{ID: packID, Key: key}}); err != nil {
		t.Fatalf("register seed pack: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(rqGit(t, dir, "for-each-ref", "--format=%(refname) %(objectname)")), "\n") {
		name, sha, found := strings.Cut(line, " ")
		if !found {
			t.Fatalf("for-each-ref emitted %q", line)
		}
		won, err := e.refStore.CASRef(ctx, fx.repoID, name, ref.ZeroSHA, sha)
		if err != nil || !won {
			t.Fatalf("seed ref %s = %s: won=%v err=%v", name, sha, won, err)
		}
	}
	return fx
}

// readToken mints a git:read JWT for the fixture repo.
func (fx *queryFixture) readToken(t *testing.T, e *testEnv) string {
	t.Helper()
	return e.mint(t, tokenSpec{iss: fx.org, repo: fx.repoURL, scopes: []string{auth.ScopeGitRead}})
}

// --- small helpers ----------------------------------------------------------

func rqGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	// Deterministic identity: GIT_AUTHOR_NAME/GIT_COMMITTER_NAME from the
	// host environment would override repo-local user.* config.
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test Author",
		"GIT_AUTHOR_EMAIL=author@example.com",
		"GIT_COMMITTER_NAME=Test Author",
		"GIT_COMMITTER_EMAIL=author@example.com",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func rqRev(t *testing.T, dir, rev string) string {
	t.Helper()
	return strings.TrimSpace(rqGit(t, dir, "rev-parse", rev))
}

func rqWriteFileBytes(t *testing.T, dir, name string, content []byte) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func rqWriteFile(t *testing.T, dir, name, content string) {
	t.Helper()
	rqWriteFileBytes(t, dir, name, []byte(content))
}

func rqWriteFileMode(t *testing.T, dir, name, content string, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func rqAppendFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(old, content...), 0o644); err != nil {
		t.Fatal(err)
	}
}

// rqDecode unmarshals a 200 response body into out, failing loudly on
// unexpected statuses.
func rqDecode(t *testing.T, resp *http.Response, body []byte, out any) {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
}

// rqExpectErr asserts an error status + envelope and returns the message.
func rqExpectErr(t *testing.T, resp *http.Response, body []byte, wantStatus int) string {
	t.Helper()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, wantStatus, body)
	}
	return requireEnvelope(t, resp, body)
}

// rqFileByPath finds one diff entry by path.
func rqFileByPath(t *testing.T, files []fileDiffEntry, path string) fileDiffEntry {
	t.Helper()
	for _, f := range files {
		if f.Path == path {
			return f
		}
	}
	t.Fatalf("no diff entry for %q in %+v", path, files)
	return fileDiffEntry{}
}

func rqFilteredByPath(t *testing.T, files []filteredFileEntry, path string) filteredFileEntry {
	t.Helper()
	for _, f := range files {
		if f.Path == path {
			return f
		}
	}
	t.Fatalf("no filtered entry for %q in %+v", path, files)
	return filteredFileEntry{}
}

func rqGrepBody(t *testing.T, v map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// --- GET /repos/diff ---------------------------------------------------------

func TestQueryCommitDiff(t *testing.T) { runBackends(t, testQueryCommitDiff) }

func testQueryCommitDiff(t *testing.T, e *testEnv) {
	fx := queryFixtureFor(t, e)
	tok := fx.readToken(t, e)

	t.Run("parent default", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c2, tok, nil, "")
		var out commitDiffResponse
		rqDecode(t, resp, body, &out)
		if out.SHA != fx.c2 || out.MergeBaseSHA != "" {
			t.Errorf("sha/merge_base = %q/%q, want %q/empty", out.SHA, out.MergeBaseSHA, fx.c2)
		}
		if out.Stats.Files != 3 || len(out.Files) != 3 || len(out.FilteredFiles) != 0 {
			t.Fatalf("stats.files=%d files=%d filtered=%d, want 3/3/0 (%s)", out.Stats.Files, len(out.Files), len(out.FilteredFiles), body)
		}
		rename := rqFileByPath(t, out.Files, "docs/intro.md")
		if rename.State != "R" || rename.OldPath != "docs/readme.md" {
			t.Errorf("rename entry = %+v, want R docs/readme.md -> docs/intro.md", rename)
		}
		modified := rqFileByPath(t, out.Files, "src/main.go")
		if modified.State != "M" || modified.Additions != 2 || modified.Deletions != 0 {
			t.Errorf("modified entry = %+v, want M +2 -0", modified)
		}
		if !strings.HasPrefix(modified.Raw, "diff --git ") {
			t.Errorf("raw does not start with a diff --git header: %.60q", modified.Raw)
		}
		added := rqFileByPath(t, out.Files, "notes.txt")
		if added.State != "A" || added.Additions != 3 || !added.IsEOF || added.Bytes != int64(len("alpha\nbeta\ngamma\n")) {
			t.Errorf("added entry = %+v, want A +3, is_eof, %d bytes", added, len("alpha\nbeta\ngamma\n"))
		}
		if out.Stats.Additions != 5 || out.Stats.Deletions != 0 || out.Stats.Changes != 5 {
			t.Errorf("stats = %+v, want +5 -0 c5", out.Stats)
		}
	})

	t.Run("deletion stats from pre-image", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c4, tok, nil, "")
		var out commitDiffResponse
		rqDecode(t, resp, body, &out)
		deleted := rqFileByPath(t, out.Files, "noeol.txt")
		if deleted.State != "D" || deleted.Bytes != int64(len("tail without newline")) || deleted.IsEOF {
			t.Errorf("deleted entry = %+v, want D with pre-image size and no eof", deleted)
		}
		if added := rqFileByPath(t, out.Files, "main-only.txt"); added.State != "A" {
			t.Errorf("main-only.txt state = %q, want A", added.State)
		}
	})

	t.Run("root commit against empty tree", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c1, tok, nil, "")
		var out commitDiffResponse
		rqDecode(t, resp, body, &out)
		if out.MergeBaseSHA != "" {
			t.Errorf("merge_base_sha = %q, want empty on a root diff", out.MergeBaseSHA)
		}
		if out.Stats.Files != 7 {
			t.Errorf("stats.files = %d, want 7", out.Stats.Files)
		}
		for _, f := range out.Files {
			if f.State != "A" {
				t.Errorf("root diff state for %s = %q, want A", f.Path, f.State)
			}
		}
		// Binary and >1MiB files are filtered; text/symlink entries inline.
		if bin := rqFilteredByPath(t, out.FilteredFiles, "big.bin"); bin.State != "A" || bin.Bytes != 1028 {
			t.Errorf("big.bin filtered entry = %+v, want A with 1028 bytes", bin)
		}
		if large := rqFilteredByPath(t, out.FilteredFiles, "large.txt"); !large.IsEOF || large.Bytes <= 1<<20 {
			t.Errorf("large.txt filtered entry = %+v, want eof and > 1MiB", large)
		}
		if noeol := rqFileByPath(t, out.Files, "noeol.txt"); noeol.IsEOF {
			t.Errorf("noeol.txt is_eof = true, want false")
		}
		if readme := rqFileByPath(t, out.Files, "docs/readme.md"); !readme.IsEOF {
			t.Errorf("docs/readme.md is_eof = false, want true")
		}
		link := rqFileByPath(t, out.Files, "link.md")
		if link.Bytes != int64(len("docs/readme.md")) || link.IsEOF {
			t.Errorf("symlink entry = %+v, want %d bytes and no eof", link, len("docs/readme.md"))
		}
	})

	t.Run("explicit baseSha is a three-dot diff", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c3+"&baseSha="+fx.c4, tok, nil, "")
		var out commitDiffResponse
		rqDecode(t, resp, body, &out)
		if out.MergeBaseSHA != fx.c2 {
			t.Errorf("merge_base_sha = %q, want %q", out.MergeBaseSHA, fx.c2)
		}
		if len(out.Files) != 2 {
			t.Fatalf("files = %d, want 2 (three-dot must not surface main-side changes): %s", len(out.Files), body)
		}
		rqFileByPath(t, out.Files, "notes.txt")
		rqFileByPath(t, out.Files, "feature.md")
	})

	t.Run("snake_case base_sha is not accepted", func(t *testing.T) {
		// camelCase baseSha=c1 changes the base (merge base c1, 4 files);
		// the snake_case spelling must be ignored (parent diff, 2 files).
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c3+"&baseSha="+fx.c1, tok, nil, "")
		var camel commitDiffResponse
		rqDecode(t, resp, body, &camel)
		if camel.MergeBaseSHA != fx.c1 || len(camel.Files) != 4 {
			t.Errorf("camelCase baseSha: merge_base=%q files=%d, want %q/4", camel.MergeBaseSHA, len(camel.Files), fx.c1)
		}
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c3+"&base_sha="+fx.c1, tok, nil, "")
		var snake commitDiffResponse
		rqDecode(t, resp, body, &snake)
		if snake.MergeBaseSHA != "" || len(snake.Files) != 2 {
			t.Errorf("snake_case base_sha: merge_base=%q files=%d, want empty/2 (ignored)", snake.MergeBaseSHA, len(snake.Files))
		}
	})

	// Issue #15: upstream deprecated the camelCase params in favor of
	// ref/base_ref/git_apply_compatible and added the *_is_ephemeral flags.
	t.Run("ref and base_ref alias the deprecated camelCase params", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref="+fx.c3+"&base_ref="+fx.c1, tok, nil, "")
		var out commitDiffResponse
		rqDecode(t, resp, body, &out)
		if out.SHA != fx.c3 || out.MergeBaseSHA != fx.c1 || len(out.Files) != 4 {
			t.Errorf("ref/base_ref: sha=%q merge_base=%q files=%d, want %q/%q/4", out.SHA, out.MergeBaseSHA, len(out.Files), fx.c3, fx.c1)
		}
		// Both styles at once: the current names win (sha=c1 would be a
		// root diff, baseSha=c4 would move the merge base to c2).
		resp, body = e.doReq(t, http.MethodGet,
			"/api/v1/repos/diff?ref="+fx.c3+"&sha="+fx.c1+"&base_ref="+fx.c1+"&baseSha="+fx.c4, tok, nil, "")
		rqDecode(t, resp, body, &out)
		if out.SHA != fx.c3 || out.MergeBaseSHA != fx.c1 || len(out.Files) != 4 {
			t.Errorf("alias conflict: sha=%q merge_base=%q files=%d, want current names to win (%q/%q/4)", out.SHA, out.MergeBaseSHA, len(out.Files), fx.c3, fx.c1)
		}
		// snake_case git_apply_compatible is accepted...
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref="+fx.c2+"&git_apply_compatible=true", tok, nil, "")
		rqDecode(t, resp, body, &out)
		if out.SHA != fx.c2 {
			t.Errorf("git_apply_compatible: sha = %q, want %q", out.SHA, fx.c2)
		}
		// ...and both spellings stay shape-validated even when the winning
		// one is well-formed (pre-alias requests that 400'd must keep 400ing).
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref="+fx.c2+"&git_apply_compatible=bogus", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref="+fx.c2+"&git_apply_compatible=true&gitApplyCompatible=bogus", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
	})

	t.Run("ref_is_ephemeral and base_is_ephemeral namespace flags", func(t *testing.T) {
		// The fixture's ephemeral "eph" points at c3: with the flag the
		// name resolves, without it the persistent namespace 404s.
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref=eph&ref_is_ephemeral=true", tok, nil, "")
		var out commitDiffResponse
		rqDecode(t, resp, body, &out)
		if out.SHA != fx.c3 || len(out.Files) != 2 {
			t.Errorf("ephemeral ref: sha=%q files=%d, want %q/2 (parent diff of c3)", out.SHA, len(out.Files), fx.c3)
		}
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref=eph", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
		// base_is_ephemeral resolves the base side: base eph(=c3) against
		// c4 is the same three-dot diff as baseSha=c3 (merge base c2).
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref="+fx.c4+"&base_ref=eph&base_is_ephemeral=true", tok, nil, "")
		rqDecode(t, resp, body, &out)
		if out.SHA != fx.c4 || out.MergeBaseSHA != fx.c2 {
			t.Errorf("ephemeral base: sha=%q merge_base=%q, want %q/%q", out.SHA, out.MergeBaseSHA, fx.c4, fx.c2)
		}
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref="+fx.c2+"&ref_is_ephemeral=bogus", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?ref="+fx.c2+"&base_is_ephemeral=bogus", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
	})

	// Issue #160: the 200 payload gained base_sha — the commit base_ref
	// resolved to — staying empty on single-commit (and root) diffs.
	t.Run("base_sha reports the resolved base", func(t *testing.T) {
		for _, c := range []struct {
			name, query, want string
		}{
			{"base_ref sha", "ref=" + fx.c3 + "&base_ref=" + fx.c4, fx.c4},
			{"base_ref branch name", "ref=" + fx.c3 + "&base_ref=main", fx.c4},
			{"base_ref tag", "ref=" + fx.c3 + "&base_ref=v1", fx.c1},
			{"deprecated baseSha alias", "ref=" + fx.c3 + "&baseSha=" + fx.c1, fx.c1},
			{"single-commit diff", "ref=" + fx.c2, ""},
			{"root commit", "ref=" + fx.c1, ""},
		} {
			t.Run(c.name, func(t *testing.T) {
				resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?"+c.query, tok, nil, "")
				var out commitDiffResponse
				rqDecode(t, resp, body, &out)
				if out.BaseSHA != c.want {
					t.Errorf("base_sha = %q, want %q", out.BaseSHA, c.want)
				}
			})
		}
	})

	t.Run("gitApplyCompatible concatenation passes git apply", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c2+"&gitApplyCompatible=true", tok, nil, "")
		var out commitDiffResponse
		rqDecode(t, resp, body, &out)
		var patch strings.Builder
		for _, f := range out.Files {
			patch.WriteString(f.Raw)
		}
		clone := t.TempDir()
		rqGit(t, clone, "clone", "--quiet", "--no-checkout", fx.workDir, ".")
		rqGit(t, clone, "checkout", "--quiet", fx.c1)
		patchPath := filepath.Join(clone, ".git", "concat.patch")
		if err := os.WriteFile(patchPath, []byte(patch.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		rqGit(t, clone, "apply", "--check", patchPath)
	})

	t.Run("path filter narrows and bypasses size filtering", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c1+"&path=large.txt", tok, nil, "")
		var out commitDiffResponse
		rqDecode(t, resp, body, &out)
		if len(out.Files) != 1 || len(out.FilteredFiles) != 0 {
			t.Fatalf("files=%d filtered=%d, want 1/0", len(out.Files), len(out.FilteredFiles))
		}
		large := rqFileByPath(t, out.Files, "large.txt")
		if large.Raw == "" || large.Additions != 27000 {
			t.Errorf("explicitly requested large file entry = additions %d, raw %d bytes; want inline +27000", large.Additions, len(large.Raw))
		}
	})

	t.Run("errors", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/diff", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c2+"&gitApplyCompatible=bogus", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+strings.Repeat("deadbeef", 5), tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c2+"&baseSha="+strings.Repeat("deadbeef", 5), tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
		missingRepoTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.repoURL + "-missing", scopes: []string{auth.ScopeGitRead}})
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/diff?sha="+fx.c2, missingRepoTok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
	})
}

// --- GET /repos/branches/diff -------------------------------------------------

func TestQueryBranchesDiff(t *testing.T) { runBackends(t, testQueryBranchesDiff) }

func testQueryBranchesDiff(t *testing.T, e *testEnv) {
	fx := queryFixtureFor(t, e)
	tok := fx.readToken(t, e)

	t.Run("default base is the default branch", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=feature", tok, nil, "")
		var out branchDiffResponse
		rqDecode(t, resp, body, &out)
		if out.Branch != "feature" || out.Base != "main" {
			t.Errorf("branch/base = %q/%q, want feature/main", out.Branch, out.Base)
		}
		if out.MergeBaseSHA != fx.c2 {
			t.Errorf("merge_base_sha = %q, want %q", out.MergeBaseSHA, fx.c2)
		}
		if len(out.Files) != 2 {
			t.Fatalf("files = %d, want 2 (three-dot: no main-side changes): %s", len(out.Files), body)
		}
		notes := rqFileByPath(t, out.Files, "notes.txt")
		if notes.State != "M" || notes.Additions != 2 || notes.Deletions != 1 {
			t.Errorf("notes.txt entry = %+v, want M +2 -1", notes)
		}
		rqFileByPath(t, out.Files, "feature.md")
	})

	t.Run("explicit base ref", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=feature&base="+fx.c1, tok, nil, "")
		var out branchDiffResponse
		rqDecode(t, resp, body, &out)
		if out.Base != fx.c1 || out.MergeBaseSHA != fx.c1 {
			t.Errorf("base/merge_base = %q/%q, want both %q", out.Base, out.MergeBaseSHA, fx.c1)
		}
		if len(out.Files) != 4 { // c2's rename+modify+add plus c3's feature.md
			t.Errorf("files = %d, want 4 (%s)", len(out.Files), body)
		}
	})

	t.Run("ephemeral namespace resolution", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=eph&ephemeral=true", tok, nil, "")
		var out branchDiffResponse
		rqDecode(t, resp, body, &out)
		if out.MergeBaseSHA != fx.c2 || len(out.Files) != 2 {
			t.Errorf("ephemeral diff merge_base=%q files=%d, want %q/2", out.MergeBaseSHA, len(out.Files), fx.c2)
		}
		// The ephemeral name is invisible outside its namespace, and
		// persistent branches are invisible inside it.
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=eph", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=feature&ephemeral=true", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
	})

	t.Run("path filter", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=feature&path=notes.txt", tok, nil, "")
		var out branchDiffResponse
		rqDecode(t, resp, body, &out)
		if len(out.Files) != 1 || out.Files[0].Path != "notes.txt" {
			t.Errorf("path-filtered files = %+v, want just notes.txt", out.Files)
		}
	})

	t.Run("errors", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=feature&ephemeral=bogus", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=no-such-branch", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/branches/diff?branch=feature&base=no-such-base", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
	})
}

// --- GET /repos/blame ---------------------------------------------------------

func TestQueryBlame(t *testing.T) { runBackends(t, testQueryBlame) }

func testQueryBlame(t *testing.T, e *testEnv) {
	fx := queryFixtureFor(t, e)
	tok := fx.readToken(t, e)

	t.Run("default ref is the default branch", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=notes.txt", tok, nil, "")
		var out blameResponse
		rqDecode(t, resp, body, &out)
		if out.Ref != "main" || out.Path != "notes.txt" || out.CommitSHA != fx.c4 {
			t.Errorf("ref/path/commit = %q/%q/%q, want main/notes.txt/%q", out.Ref, out.Path, out.CommitSHA, fx.c4)
		}
		if len(out.Lines) != 3 {
			t.Fatalf("lines = %d, want 3 (%s)", len(out.Lines), body)
		}
		for i, line := range out.Lines {
			if line.LineNumber != i+1 || line.CommitSHA != fx.c2 || line.OriginalPath != "notes.txt" {
				t.Errorf("line %d = %+v, want line %d from %s", i+1, line, i+1, fx.c2)
			}
			if line.PreviousCommitSHA != "" {
				t.Errorf("line %d previous_commit_sha = %q, want omitted for the creating commit", i+1, line.PreviousCommitSHA)
			}
			if line.AuthorName != "Test Author" || line.AuthorEmail != "author@example.com" {
				t.Errorf("line %d author = %q <%q>", i+1, line.AuthorName, line.AuthorEmail)
			}
			if _, err := time.Parse(time.RFC3339, line.AuthorTime); err != nil {
				t.Errorf("line %d author_time %q: %v", i+1, line.AuthorTime, err)
			}
			if _, err := time.Parse(time.RFC3339, line.CommitterTime); err != nil {
				t.Errorf("line %d committer_time %q: %v", i+1, line.CommitterTime, err)
			}
		}
	})

	t.Run("feature attribution and previous commit", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=notes.txt&ref=feature", tok, nil, "")
		var out blameResponse
		rqDecode(t, resp, body, &out)
		if out.CommitSHA != fx.c3 || len(out.Lines) != 4 {
			t.Fatalf("commit/lines = %q/%d, want %q/4 (%s)", out.CommitSHA, len(out.Lines), fx.c3, body)
		}
		wantCommits := []string{fx.c2, fx.c3, fx.c2, fx.c3}
		for i, line := range out.Lines {
			if line.CommitSHA != wantCommits[i] {
				t.Errorf("line %d commit = %q, want %q", i+1, line.CommitSHA, wantCommits[i])
			}
		}
		if out.Lines[1].PreviousCommitSHA != fx.c2 {
			t.Errorf("changed line previous_commit_sha = %q, want %q", out.Lines[1].PreviousCommitSHA, fx.c2)
		}
		if out.Lines[1].Summary != "c3: feature edits" {
			t.Errorf("changed line summary = %q", out.Lines[1].Summary)
		}
	})

	t.Run("rename attribution keeps the original path", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=docs/intro.md&ref=main", tok, nil, "")
		var out blameResponse
		rqDecode(t, resp, body, &out)
		if len(out.Lines) == 0 {
			t.Fatalf("no blame lines (%s)", body)
		}
		if out.Lines[0].CommitSHA != fx.c1 || out.Lines[0].OriginalPath != "docs/readme.md" {
			t.Errorf("line 1 = %+v, want attributed to %s at docs/readme.md", out.Lines[0], fx.c1)
		}
	})

	t.Run("ranges", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=notes.txt&ref=feature&range=2,3", tok, nil, "")
		var out blameResponse
		rqDecode(t, resp, body, &out)
		if len(out.Lines) != 2 || out.Lines[0].LineNumber != 2 || out.Lines[1].LineNumber != 3 {
			t.Errorf("range 2,3 lines = %+v, want lines 2 and 3", out.Lines)
		}
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=notes.txt&ref=feature&range=1,1&range=4,4", tok, nil, "")
		rqDecode(t, resp, body, &out)
		if len(out.Lines) != 2 || out.Lines[0].LineNumber != 1 || out.Lines[1].LineNumber != 4 {
			t.Errorf("repeated ranges lines = %+v, want lines 1 and 4", out.Lines)
		}
	})

	t.Run("detect_moves accepted", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=notes.txt&ref=feature&detect_moves=true", tok, nil, "")
		var out blameResponse
		rqDecode(t, resp, body, &out)
		if len(out.Lines) != 4 {
			t.Errorf("detect_moves lines = %d, want 4", len(out.Lines))
		}
	})

	t.Run("errors", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/blame", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=no/such/file.txt", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=notes.txt&ref=no-such-ref", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
		resp, body = e.doReq(t, http.MethodGet, "/api/v1/repos/blame?path=notes.txt&range=zz,qq", tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		tooMany := "/api/v1/repos/blame?path=notes.txt" + strings.Repeat("&range=1,1", 17)
		resp, body = e.doReq(t, http.MethodGet, tooMany, tok, nil, "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
	})
}

// --- POST /repos/grep ---------------------------------------------------------

func TestQueryGrep(t *testing.T) { runBackends(t, testQueryGrep) }

func testQueryGrep(t *testing.T, e *testEnv) {
	fx := queryFixtureFor(t, e)
	tok := fx.readToken(t, e)

	grep := func(t *testing.T, body map[string]any) grepResponse {
		t.Helper()
		resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos/grep", tok, rqGrepBody(t, body), "")
		var out grepResponse
		rqDecode(t, resp, respBody, &out)
		return out
	}

	t.Run("basic match grouping", func(t *testing.T) {
		out := grep(t, map[string]any{"query": map[string]any{"pattern": "TODO"}})
		if out.Repo.Ref != "main" || out.Repo.Commit != fx.c4 {
			t.Errorf("repo = %+v, want main@%s", out.Repo, fx.c4)
		}
		if out.Query.Pattern != "TODO" || !out.Query.CaseSensitive {
			t.Errorf("query echo = %+v, want TODO/case-sensitive", out.Query)
		}
		if out.HasMore || out.NextCursor != "" || out.IncompleteResults {
			t.Errorf("pagination/truncation flags = %+v, want all clear", out)
		}
		if len(out.Matches) != 2 || out.Matches[0].Path != "docs/intro.md" || out.Matches[1].Path != "src/main.go" {
			t.Fatalf("matches = %+v, want docs/intro.md then src/main.go", out.Matches)
		}
		if lines := out.Matches[0].Lines; len(lines) != 1 || lines[0].LineNumber != 3 || lines[0].Type != "match" {
			t.Errorf("intro.md lines = %+v, want one match at line 3", lines)
		}
		if lines := out.Matches[1].Lines; len(lines) != 2 || lines[0].LineNumber != 3 || lines[1].LineNumber != 8 {
			t.Errorf("main.go lines = %+v, want matches at 3 and 8", lines)
		}
	})

	t.Run("ref and legacy rev select the commit", func(t *testing.T) {
		want := grep(t, map[string]any{"query": map[string]any{"pattern": "TODO"}, "ref": "feature"})
		if want.Repo.Commit != fx.c3 {
			t.Errorf("ref=feature commit = %q, want %q", want.Repo.Commit, fx.c3)
		}
		found := false
		for _, m := range want.Matches {
			if m.Path == "feature.md" {
				found = true
			}
		}
		if !found {
			t.Errorf("ref=feature matches = %+v, want feature.md included", want.Matches)
		}
		legacy := grep(t, map[string]any{"query": map[string]any{"pattern": "TODO"}, "rev": "feature"})
		if legacy.Repo.Commit != fx.c3 {
			t.Errorf("rev=feature commit = %q, want %q", legacy.Repo.Commit, fx.c3)
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		out := grep(t, map[string]any{"query": map[string]any{"pattern": "TODO", "case_sensitive": false}})
		if out.Query.CaseSensitive {
			t.Errorf("query echo case_sensitive = true, want false")
		}
		mainGo := out.Matches[len(out.Matches)-1]
		if mainGo.Path != "src/main.go" || len(mainGo.Lines) != 3 {
			t.Errorf("case-insensitive main.go lines = %+v, want 3 (adds the lowercase line 5)", mainGo.Lines)
		}
	})

	t.Run("context lines are typed", func(t *testing.T) {
		out := grep(t, map[string]any{
			"query":   map[string]any{"pattern": "BETA"},
			"ref":     "feature",
			"context": map[string]any{"before": 1, "after": 1},
		})
		if len(out.Matches) != 1 || out.Matches[0].Path != "notes.txt" {
			t.Fatalf("matches = %+v, want notes.txt only", out.Matches)
		}
		lines := out.Matches[0].Lines
		wantTypes := []string{"context", "match", "context"}
		if len(lines) != 3 {
			t.Fatalf("lines = %+v, want 3", lines)
		}
		for i, l := range lines {
			if l.LineNumber != i+1 || l.Type != wantTypes[i] {
				t.Errorf("line %d = %+v, want line %d type %s", i, l, i+1, wantTypes[i])
			}
		}
		if lines[1].Text != "BETA" {
			t.Errorf("match text = %q, want BETA", lines[1].Text)
		}
	})

	t.Run("path prefixes and file filters", func(t *testing.T) {
		pattern := map[string]any{"pattern": "TODO"}
		out := grep(t, map[string]any{"query": pattern, "paths": []string{"docs"}})
		if len(out.Matches) != 1 || out.Matches[0].Path != "docs/intro.md" {
			t.Errorf("paths=docs matches = %+v", out.Matches)
		}
		// Trailing-slash prefixes and literal wildcard characters: paths
		// are plain prefixes (openapi), never a glob dialect — the git-side
		// pathspec narrowing must not change that.
		out = grep(t, map[string]any{"query": pattern, "paths": []string{"docs/"}})
		if len(out.Matches) != 1 || out.Matches[0].Path != "docs/intro.md" {
			t.Errorf("paths=docs/ matches = %+v", out.Matches)
		}
		out = grep(t, map[string]any{"query": pattern, "paths": []string{"src/*"}})
		if len(out.Matches) != 0 {
			t.Errorf("paths=src/* matches = %+v, want none (literal prefix semantics)", out.Matches)
		}
		out = grep(t, map[string]any{"query": pattern, "paths": []string{"src/main.go"}})
		if len(out.Matches) != 1 || out.Matches[0].Path != "src/main.go" {
			t.Errorf("paths=src/main.go matches = %+v", out.Matches)
		}
		out = grep(t, map[string]any{"query": pattern, "file_filters": map[string]any{"include_globs": []string{"src/**"}}})
		if len(out.Matches) != 1 || out.Matches[0].Path != "src/main.go" {
			t.Errorf("include_globs matches = %+v", out.Matches)
		}
		out = grep(t, map[string]any{"query": pattern, "file_filters": map[string]any{"exclude_globs": []string{"*.go"}}})
		if len(out.Matches) != 1 || out.Matches[0].Path != "docs/intro.md" {
			t.Errorf("exclude_globs matches = %+v", out.Matches)
		}
		out = grep(t, map[string]any{"query": pattern, "file_filters": map[string]any{"extension_filters": []string{".md"}}})
		if len(out.Matches) != 1 || out.Matches[0].Path != "docs/intro.md" {
			t.Errorf("extension_filters matches = %+v", out.Matches)
		}
	})

	t.Run("limits truncate and flag incomplete results", func(t *testing.T) {
		out := grep(t, map[string]any{
			"query":  map[string]any{"pattern": "TODO"},
			"limits": map[string]any{"max_matches_per_file": 1},
		})
		if !out.IncompleteResults {
			t.Errorf("incomplete_results = false, want true when a per-file cap hits")
		}
		for _, m := range out.Matches {
			if len(m.Lines) != 1 {
				t.Errorf("%s lines = %+v, want 1 after per-file cap", m.Path, m.Lines)
			}
		}
		out = grep(t, map[string]any{
			"query":  map[string]any{"pattern": "TODO"},
			"limits": map[string]any{"max_lines": 2},
		})
		total := 0
		for _, m := range out.Matches {
			total += len(m.Lines)
		}
		if total != 2 || !out.IncompleteResults {
			t.Errorf("max_lines=2 total=%d incomplete=%v, want 2/true", total, out.IncompleteResults)
		}
	})

	t.Run("pagination walks to exhaustion", func(t *testing.T) {
		full := grep(t, map[string]any{"query": map[string]any{"pattern": "TODO"}})
		var fullFlat []string
		for _, m := range full.Matches {
			for _, l := range m.Lines {
				fullFlat = append(fullFlat, fmt.Sprintf("%s:%d:%s", m.Path, l.LineNumber, l.Type))
			}
		}

		var walked []string
		cursor := ""
		for page := 0; ; page++ {
			if page > 10 {
				t.Fatal("pagination did not terminate")
			}
			body := map[string]any{
				"query":      map[string]any{"pattern": "TODO"},
				"pagination": map[string]any{"limit": 1},
			}
			if cursor != "" {
				body["pagination"] = map[string]any{"limit": 1, "cursor": cursor}
			}
			out := grep(t, body)
			if out.Repo.Commit != fx.c4 {
				t.Errorf("page %d commit = %q, want anchored to %q", page, out.Repo.Commit, fx.c4)
			}
			for _, m := range out.Matches {
				for _, l := range m.Lines {
					walked = append(walked, fmt.Sprintf("%s:%d:%s", m.Path, l.LineNumber, l.Type))
				}
			}
			if !out.HasMore {
				if out.NextCursor != "" {
					t.Errorf("exhausted page has next_cursor %q, want empty", out.NextCursor)
				}
				break
			}
			if out.NextCursor == "" {
				t.Fatal("has_more with empty next_cursor")
			}
			cursor = out.NextCursor
		}
		if strings.Join(walked, "|") != strings.Join(fullFlat, "|") {
			t.Errorf("paginated walk = %v, want %v", walked, fullFlat)
		}
	})

	t.Run("errors", func(t *testing.T) {
		post := func(body []byte) (*http.Response, []byte) {
			return e.doReq(t, http.MethodPost, "/api/v1/repos/grep", tok, body, "")
		}
		resp, body := post([]byte(`{`))
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = post(rqGrepBody(t, map[string]any{"query": map[string]any{}}))
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = post(rqGrepBody(t, map[string]any{"query": map[string]any{"pattern": "x"}, "context": map[string]any{"before": -1}}))
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = post(rqGrepBody(t, map[string]any{"query": map[string]any{"pattern": "x"}, "limits": map[string]any{"max_lines": 5000}}))
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = post(rqGrepBody(t, map[string]any{"query": map[string]any{"pattern": "x"}, "pagination": map[string]any{"limit": 0}}))
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = post(rqGrepBody(t, map[string]any{"query": map[string]any{"pattern": "x"}, "pagination": map[string]any{"cursor": "!!not-base64!!"}}))
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		resp, body = post(rqGrepBody(t, map[string]any{"query": map[string]any{"pattern": "x"}, "ref": "no-such-ref"}))
		rqExpectErr(t, resp, body, http.StatusNotFound)
		// An invalid regex is client input: git's exit-128 fatal maps to a
		// 400, never a 500.
		resp, body = post(rqGrepBody(t, map[string]any{"query": map[string]any{"pattern": "["}}))
		rqExpectErr(t, resp, body, http.StatusBadRequest)
	})

	t.Run("malformed cursors are 400s", func(t *testing.T) {
		post := func(cursor string) (*http.Response, []byte) {
			return e.doReq(t, http.MethodPost, "/api/v1/repos/grep", tok, rqGrepBody(t, map[string]any{
				"query":      map[string]any{"pattern": "TODO"},
				"pagination": map[string]any{"cursor": cursor},
			}), "")
		}
		encode := func(raw string) string { return base64.RawURLEncoding.EncodeToString([]byte(raw)) }
		cases := map[string]string{
			"wrong field count":  encode("just-one-field"),
			"non-hex commit":     encode("zzzz\x00path\x003"),
			"empty path":         encode(fx.c4 + "\x00\x003"),
			"line zero":          encode(fx.c4 + "\x00docs/intro.md\x000"),
			"non-numeric line":   encode(fx.c4 + "\x00docs/intro.md\x00abc"),
			"unknown commit":     encodeGrepCursor("00000000000000000000000000000000000000ff", "docs/intro.md", 3),
			"position not found": encodeGrepCursor(fx.c4, "no/such/file.txt", 1),
		}
		for name, cursor := range cases {
			t.Run(name, func(t *testing.T) {
				resp, body := post(cursor)
				rqExpectErr(t, resp, body, http.StatusBadRequest)
			})
		}
	})
}

// --- POST /repos/archive --------------------------------------------------------

// rqArchiveEntry is one extracted tar entry.
type rqArchiveEntry struct {
	typeflag byte
	mode     int64
	linkname string
	content  string
}

// rqReadArchive gunzips and untars an archive response body.
func rqReadArchive(t *testing.T, body []byte) map[string]rqArchiveEntry {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	defer gz.Close()
	entries := map[string]rqArchiveEntry{}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("tar entry %s: %v", hdr.Name, err)
		}
		entries[hdr.Name] = rqArchiveEntry{
			typeflag: hdr.Typeflag,
			mode:     hdr.Mode,
			linkname: hdr.Linkname,
			content:  string(content),
		}
	}
	return entries
}

func TestQueryArchive(t *testing.T) { runBackends(t, testQueryArchive) }

func testQueryArchive(t *testing.T, e *testEnv) {
	fx := queryFixtureFor(t, e)
	tok := fx.readToken(t, e)

	t.Run("feature snapshot with prefix", func(t *testing.T) {
		body := []byte(`{"ref": "feature", "archive": {"prefix": "snap/"}}`)
		resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, body, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, respBody)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
			t.Errorf("Content-Type = %q, want application/gzip", ct)
		}
		if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, `attachment; filename="`) || !strings.Contains(cd, "feature.tar.gz") {
			t.Errorf("Content-Disposition = %q", cd)
		}
		if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}
		entries := rqReadArchive(t, respBody)
		want := []string{
			"snap/big.bin", "snap/docs/intro.md", "snap/feature.md", "snap/large.txt",
			"snap/link.md", "snap/noeol.txt", "snap/notes.txt", "snap/src/main.go", "snap/tools/run.sh",
		}
		if len(entries) != len(want) {
			t.Errorf("archive has %d entries, want %d: %v", len(entries), len(want), entries)
		}
		for _, name := range want {
			if _, ok := entries[name]; !ok {
				t.Errorf("archive is missing %s", name)
			}
		}
		if _, ok := entries["snap/main-only.txt"]; ok {
			t.Errorf("feature archive contains the main-only file")
		}
		if notes := entries["snap/notes.txt"]; notes.content != "alpha\nBETA\ngamma\ndelta\n" {
			t.Errorf("notes.txt content = %q, want the feature version", notes.content)
		}
		if run := entries["snap/tools/run.sh"]; run.mode != 0o755 || run.typeflag != tar.TypeReg {
			t.Errorf("run.sh entry = %+v, want regular file mode 0755", run)
		}
		if link := entries["snap/link.md"]; link.typeflag != tar.TypeSymlink || link.linkname != "docs/readme.md" {
			t.Errorf("link.md entry = %+v, want symlink to docs/readme.md", link)
		}
	})

	t.Run("prefix without trailing slash is normalized", func(t *testing.T) {
		body := []byte(`{"ref": "feature", "archive": {"prefix": "snap"}}`)
		resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, body, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, respBody)
		}
		entries := rqReadArchive(t, respBody)
		if _, ok := entries["snap/notes.txt"]; !ok {
			t.Errorf(`prefix "snap" entries = %v, want snap/ folder entries`, entries)
		}
		if _, ok := entries["snapnotes.txt"]; ok {
			t.Errorf(`prefix "snap" glued onto entry names: %v`, entries)
		}
	})

	t.Run("empty body archives the default branch", func(t *testing.T) {
		resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, respBody)
		}
		entries := rqReadArchive(t, respBody)
		if _, ok := entries["main-only.txt"]; !ok {
			t.Errorf("default-branch archive is missing main-only.txt: %v", entries)
		}
		if _, ok := entries["feature.md"]; ok {
			t.Errorf("default-branch archive contains feature.md")
		}
	})

	t.Run("globs and max_blob_size shape the archive", func(t *testing.T) {
		body := []byte(`{"ref": "main", "include_globs": ["docs/**"]}`)
		resp, respBody := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, body, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		entries := rqReadArchive(t, respBody)
		if len(entries) != 1 {
			t.Errorf("include_globs entries = %v, want just docs/intro.md", entries)
		}
		if _, ok := entries["docs/intro.md"]; !ok {
			t.Errorf("include_globs entries = %v, want docs/intro.md", entries)
		}

		body = []byte(`{"ref": "main", "max_blob_size": 1000}`)
		resp, respBody = e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, body, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		entries = rqReadArchive(t, respBody)
		if _, ok := entries["large.txt"]; ok {
			t.Errorf("max_blob_size archive still contains large.txt")
		}
		if _, ok := entries["big.bin"]; ok {
			t.Errorf("max_blob_size archive still contains big.bin (1028 bytes)")
		}
		if _, ok := entries["notes.txt"]; !ok {
			t.Errorf("max_blob_size archive is missing small notes.txt: %v", entries)
		}
	})

	t.Run("errors", func(t *testing.T) {
		resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{"ref": "no-such-ref"}`), "")
		rqExpectErr(t, resp, body, http.StatusNotFound)
		resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{"max_blob_size": -1}`), "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
		// Tar-slip prefixes must be rejected before any archive bytes flow.
		for _, prefix := range []string{"../", "/abs/", "a/../b/", "..\\evil\\"} {
			payload, err := json.Marshal(map[string]any{"archive": map[string]string{"prefix": prefix}})
			if err != nil {
				t.Fatal(err)
			}
			resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, payload, "")
			rqExpectErr(t, resp, body, http.StatusBadRequest)
		}
		resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{`), "")
		rqExpectErr(t, resp, body, http.StatusBadRequest)
	})
}

// --- unborn/unknown-repo 404s ---------------------------------------------------

// TestQueryUnborn404s covers docs/explanation/domain-model.md invariant 3 for the query group: a
// fresh repo has an unborn default branch, so every default-ref read maps
// to 404 (ref absent in the DB).
func TestQueryUnborn404s(t *testing.T) { runBackends(t, testQueryUnborn404s) }

func testQueryUnborn404s(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	repoURL := "https://example.com/org/unborn-" + randomHex(t, 4)
	createRepo(t, e, org, repoURL, nil, http.StatusOK)
	tok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

	requests := []struct {
		name, method, path string
		body               []byte
	}{
		{"diff", http.MethodGet, "/api/v1/repos/diff?sha=main", nil},
		{"branches diff", http.MethodGet, "/api/v1/repos/branches/diff?branch=main", nil},
		{"blame", http.MethodGet, "/api/v1/repos/blame?path=file.txt", nil},
		{"grep", http.MethodPost, "/api/v1/repos/grep", []byte(`{"query": {"pattern": "x"}}`)},
		{"archive", http.MethodPost, "/api/v1/repos/archive", []byte(`{}`)},
	}
	for _, req := range requests {
		t.Run(req.name, func(t *testing.T) {
			resp, body := e.doReq(t, req.method, req.path, tok, req.body, "")
			rqExpectErr(t, resp, body, http.StatusNotFound)
		})
	}
}
