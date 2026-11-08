package api

// A commit message and a grep pattern reach git as request values. exec
// refuses an argument past 128 KiB (E2BIG) and an argument with a NUL
// byte, so before these tests each such value got a 500.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// commitObjectSize reads a published commit's object size back from a
// re-materialized cache repo.
func commitObjectSize(t *testing.T, e *testEnv, repoID, sha string) int {
	t.Helper()
	repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, repoID, "main")
	if err != nil {
		t.Fatal(err)
	}
	out, err := wrefGit(context.Background(), repoPath, nil, nil, "cat-file", "-s", sha)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestLongCommitMessage(t *testing.T) {
	long := strings.Repeat("m", 200<<10) // past 128 KiB
	runBackends(t, func(t *testing.T, e *testEnv) {
		t.Run("commit-pack", func(t *testing.T) {
			srv := wctServer(t, e)
			repo, writeTok, _ := wctRepo(t, e)
			calls := 0
			postAs := func(msg string, extra map[string]any) (*http.Response, []byte) {
				t.Helper()
				calls++ // new content each time: a commit that changes nothing is a 412
				body := wctLines(t,
					wctMeta("main", msg, []map[string]any{{"path": "f.txt", "content_id": "id-f", "operation": "upsert"}}, extra),
					wctChunk("id-f", []byte(strconv.Itoa(calls)+"\n"), true))
				return wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
			}
			resp, body := postAs(long, nil)
			env := wctRequireOK(t, resp, body)
			if n := commitObjectSize(t, e, repo.RepoID, env.Commit.CommitSHA); n < len(long) {
				t.Fatalf("commit object is %d bytes, want the whole %d-byte message in it", n, len(long))
			}
			// Each request below names the tip, so the head guard passes and
			// only the value can stop it.
			tip := map[string]any{"expected_head_sha": env.Commit.CommitSHA}
			if resp, body := postAs("a\x00b", tip); resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("message with a NUL byte: status = %d, want 400 (body %s)", resp.StatusCode, body)
			}
			// git gets the identities through its environment, and exec
			// refuses a NUL byte there, and on Linux an entry over 128 KiB.
			over := wcMaxIdentityBytes + 1
			for name, extra := range map[string]map[string]any{
				"author name with a NUL byte":     {"author": map[string]any{"name": "A\x00B", "email": "a@x"}},
				"committer email with a NUL byte": {"committer": map[string]any{"name": "C", "email": "c\x00@x"}},
				"author name over the limit":      {"author": map[string]any{"name": strings.Repeat("n", over), "email": "a@x"}},
				"committer email over the limit":  {"committer": map[string]any{"name": "C", "email": strings.Repeat("e", over)}},
			} {
				maps.Copy(extra, tip)
				if resp, body := postAs("m", extra); resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status = %d, want 400 (body %.300s)", name, resp.StatusCode, body)
				}
			}
			// At the limit, GIT_COMMITTER_EMAIL=<email> is exactly the
			// 128 KiB that exec takes, and the commit lands.
			atLimit := map[string]any{"committer": map[string]any{"name": "C", "email": strings.Repeat("e", wcMaxIdentityBytes)}}
			maps.Copy(atLimit, tip)
			resp, body = postAs("m", atLimit)
			wctRequireOK(t, resp, body)
		})
		t.Run("merge", func(t *testing.T) {
			srv := wmServer(t, e)
			fx := wmSeedFixture(t, e)
			merge := func(msg string) (*http.Response, []byte) {
				t.Helper()
				return wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
					wmMergeBody("feature", "t-clean", "merge", map[string]any{"author": wmTestAuthor, "commit_message": msg}))
			}
			if resp, raw := merge("a\x00b"); resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("message with a NUL byte: status = %d, want 400 (body %s)", resp.StatusCode, raw)
			}
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("feature", "t-clean", "merge", map[string]any{"author": map[string]any{"name": "W\x00M", "email": "wm@test.invalid"}}))
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("author name with a NUL byte: status = %d, want 400 (body %s)", resp.StatusCode, raw)
			}
			for name, committer := range map[string]map[string]any{
				"committer name with a NUL byte": {"name": "C\x00M", "email": "c@test.invalid"},
				"committer email over the limit": {"name": "C", "email": strings.Repeat("e", wcMaxIdentityBytes+1)},
				"committer name over the limit":  {"name": strings.Repeat("n", wcMaxIdentityBytes+1), "email": "c@test.invalid"},
			} {
				resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
					wmMergeBody("feature", "t-clean", "merge", map[string]any{"author": wmTestAuthor, "committer": committer}))
				if resp.StatusCode != http.StatusBadRequest {
					t.Errorf("%s: status = %d, want 400 (body %.300s)", name, resp.StatusCode, raw)
				}
			}
			resp, raw = merge(long)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body %.300s)", resp.StatusCode, raw)
			}
			commit, _ := wmDecode(t, raw)["commit_sha"].(string)
			if n := commitObjectSize(t, e, fx.repoID, commit); n < len(long) {
				t.Fatalf("merge commit object is %d bytes, want the whole %d-byte message in it", n, len(long))
			}
		})
	})
}

// commit-tree -F - takes the message as is, and -m adds a line feed to a
// message without one: wcMessageInput must make the commit that -m made.
func TestCommitTreeMessageMatchesDashM(t *testing.T) {
	dir := t.TempDir()
	env := []string{
		"HOME=" + t.TempDir(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_DATE=1700000000 +0000", "GIT_COMMITTER_DATE=1700000000 +0000",
	}
	git := func(stdin string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir, cmd.Env, cmd.Stdin = dir, append(env, "GIT_AUTHOR_NAME=A", "GIT_AUTHOR_EMAIL=a@x", "GIT_COMMITTER_NAME=A", "GIT_COMMITTER_EMAIL=a@x"), strings.NewReader(stdin)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %v: %v", args[0], err)
		}
		return strings.TrimSpace(string(out))
	}
	git("", "init", "-q", "--bare")
	tree := git("", "hash-object", "-t", "tree", "-w", "--stdin")
	sc := &wcScratch{repoPath: dir, baseEnv: env}
	for _, msg := range []string{"a", "a\n", "a\n\n", "\n", "subject\n\nbody", "  spaced  ", "x\r\n", "x\r", "# hash", "é\n"} {
		got, err := sc.commitTree(context.Background(), tree, "", &wcMetadata{CommitMessage: msg, Author: wcAuthor{Name: "A", Email: "a@x"}})
		if err != nil {
			t.Fatalf("commitTree(%q): %v", msg, err)
		}
		if want := git("", "commit-tree", "-m", msg, tree); got != want {
			t.Errorf("commitTree(%q) = %s, want %s (the -m commit)", msg, got, want)
		}
	}
}

// gitValuesRepo seeds a one-file repository and returns a git:read token.
func gitValuesRepo(t *testing.T, e *testEnv) string {
	t.Helper()
	org := e.newOrg(t)
	repoURL := "https://example.com/" + org + "/git-values-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	coreWriteFile(t, dir, "a.txt", "needle\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c")
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": coreRevParse(t, dir, "HEAD")})
	return e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
}

// capStack lowers the soft stack limit to max for the rest of t when it
// is higher. Linux sets ARG_MAX, the total size of exec's arguments, to a
// quarter of it (at most 6 MiB), so a host with a larger stack limit
// takes arguments that the usual 8 MiB limit refuses. A grep body (at most
// 1 MiB) cannot reach 6 MiB of arguments.
func capStack(t *testing.T, max uint64) {
	t.Helper()
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_STACK, &lim); err != nil {
		t.Fatal(err)
	}
	if lim.Cur <= max {
		return
	}
	old := lim
	lim.Cur = max
	if err := syscall.Setrlimit(syscall.RLIMIT_STACK, &lim); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_STACK, &old); err != nil {
			t.Errorf("restore the stack limit: %v", err)
		}
	})
}

// A request value that exec refuses as an argument is the client's
// error: 400, not 500.
func TestGitValuesExecRefusesAre400(t *testing.T) {
	long := strings.Repeat("p", 200<<10) // past 128 KiB
	// exec's 128 KiB limit for one argument is Linux's. macOS limits only
	// the total, to 1 MiB, which the 200,000 paths pass.
	linux := runtime.GOOS == "linux"
	many := make([]string, 200000) // about 4 MB of arguments: past ARG_MAX at an 8 MiB stack limit
	for i := range many {
		many[i] = "a"
	}
	runBackends(t, func(t *testing.T, e *testEnv) {
		token := gitValuesRepo(t, e)
		check := func(t *testing.T, what string, resp *http.Response, body []byte) {
			t.Helper()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s: status = %d, want 400 (body %.200s)", what, resp.StatusCode, body)
			}
		}
		t.Run("grep", func(t *testing.T) {
			grep := func(query map[string]any, paths []string) (*http.Response, []byte) {
				t.Helper()
				body := map[string]any{"query": query}
				if paths != nil {
					body["paths"] = paths
				}
				return e.doReq(t, http.MethodPost, "/api/v1/repos/grep", token, rqGrepBody(t, body), "")
			}
			if resp, body := grep(map[string]any{"pattern": "needle"}, []string{"a.txt"}); resp.StatusCode != http.StatusOK {
				t.Fatalf("plain grep: status = %d (body %.200s)", resp.StatusCode, body)
			}
			// Each line of a pattern is a regex of its own; git refuses
			// one regex of 32 KiB with exit 128 (a 400 already).
			if linux {
				lines := strings.Repeat("needle\n", 200<<10/7)
				resp, body := grep(map[string]any{"pattern": lines}, nil)
				check(t, "200 KiB pattern", resp, body)
				resp, body = grep(map[string]any{"pattern": "needle"}, []string{long})
				check(t, "200 KiB path", resp, body)
			}
			resp, body := grep(map[string]any{"pattern": "nee\x00dle"}, nil)
			check(t, "pattern with a NUL byte", resp, body)
			resp, body = grep(map[string]any{"pattern": "needle"}, []string{"a\x00b"})
			check(t, "path with a NUL byte", resp, body)
			capStack(t, 8<<20)
			resp, body = grep(map[string]any{"pattern": "needle"}, many)
			check(t, "200,000 paths", resp, body)
		})
		for _, route := range []string{"file", "files", "files/metadata", "commits", "blame"} {
			t.Run(route, func(t *testing.T) {
				cases := []struct{ name, path string }{{"path with a NUL byte", "a\x00b"}}
				if linux {
					cases = append(cases, struct{ name, path string }{"200 KiB path", long})
				}
				for _, tc := range cases {
					resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/"+route+"?ref=main&path="+url.QueryEscape(tc.path), token, nil, "")
					check(t, tc.name, resp, body)
				}
			})
		}
	})
}

// The notes endpoint gives its author to git through the environment too.
func TestNoteAuthorExecRefusesAre400(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		srv := newWrefServer(t, e)
		for name, author := range map[string]map[string]any{
			"author name with a NUL byte": {"name": "A\x00B", "email": "a@x"},
			"author email over the limit": {"name": "A", "email": strings.Repeat("e", wcMaxIdentityBytes+1)},
		} {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": fx.c1, "action": "add", "note": "n", "author": author})
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s: status = %d, want 400 (body %.300v)", name, resp.StatusCode, body)
			}
		}
	})
}

// A repository path can be longer than exec takes as one argument: fsck
// limits each tree entry name to 4096 bytes, not the path. Here 40 nested
// directories make a 160,125-byte path. The diff and files/metadata read
// it from git and must not hand it back to git as an argument, which would
// fail as a client error (400) on a good repository.
func TestRepositoryPathOverExecLimit(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/deep-path-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", "one\n")
		coreGit(t, dir, "add", ".")
		coreGit(t, dir, "commit", "-q", "-m", "c1")
		root := coreRevParse(t, dir, "HEAD")
		stdinGit := func(stdin string, args ...string) string {
			t.Helper()
			cmd := exec.Command("git", args...)
			cmd.Dir, cmd.Stdin = dir, strings.NewReader(stdin)
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("git %s: %v", args[0], err)
			}
			return strings.TrimSpace(string(out))
		}
		blob := stdinGit("x\n", "hash-object", "-w", "--stdin")
		tree := stdinGit("100644 blob "+blob+"\tf.txt\n", "mktree")
		segment := strings.Repeat("d", 4000)
		path := "f.txt"
		for i := range 40 {
			name := fmt.Sprintf("%s%02d", segment, 40-i)
			tree = stdinGit("040000 tree "+tree+"\t"+name+"\n", "mktree")
			path = name + "/" + path
		}
		tip := strings.TrimSpace(coreGit(t, dir, "commit-tree", "-p", root, "-m", "deep", tree))
		coreGit(t, dir, "update-ref", "refs/heads/main", tip)
		if len(path) <= 128<<10 {
			t.Fatalf("path is %d bytes, want more than 128 KiB", len(path))
		}
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": tip, "refs/heads/base": root})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

		for _, route := range []string{
			"/api/v1/repos/diff?ref=" + tip + "&base_ref=" + root,
			"/api/v1/repos/diff?ref=main&base_ref=base",
			"/api/v1/repos/branches/diff?branch=main&base=base",
		} {
			resp, body := e.doReq(t, http.MethodGet, route, token, nil, "")
			if resp.StatusCode != http.StatusOK {
				t.Errorf("%s: status = %d, want 200 (body %.300s)", route, resp.StatusCode, body)
				continue
			}
			var diff struct {
				Files []struct {
					Path  string `json:"path"`
					State string `json:"state"`
					Bytes int64  `json:"bytes"`
					IsEOF bool   `json:"is_eof"`
				} `json:"files"`
			}
			if err := json.Unmarshal(body, &diff); err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for _, f := range diff.Files {
				got[f.Path] = fmt.Sprintf("%s %d %v", f.State, f.Bytes, f.IsEOF)
			}
			if want := map[string]string{"a.txt": "D 4 true", path: "A 2 true"}; !maps.Equal(got, want) {
				t.Errorf("%s files: %d entries, a.txt %q, the deep path %q; want D 4 true and A 2 true", route, len(got), got["a.txt"], got[path])
			}
		}
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/files/metadata?ref=main&recursive=true", token, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("files/metadata: status = %d, want 200 (body %.300s)", resp.StatusCode, body)
		}
	})
}
