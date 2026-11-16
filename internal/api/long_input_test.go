package api

// Request values that git takes as one argument or one environment entry
// must fit gitlimit.MaxArgBytes, the most that Linux takes (128 KiB with
// the terminating NUL). Each one is refused by its size with 400 before git
// starts, and the longest value that fits reaches git. Before these tests
// each such value of 128 KiB or more got a 500. A commit message goes to
// git on stdin and takes any length the request takes.

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/gitread"
)

// TestGitArgLimitsAreTheDocumentedNumbers pins the limits that
// docs/reference/http-api.md states. The other tests take them from the
// constants, so a limit made too strict would pass them.
func TestGitArgLimitsAreTheDocumentedNumbers(t *testing.T) {
	for _, c := range []struct {
		name      string
		got, want int
	}{
		{"gitlimit.MaxArgBytes", gitlimit.MaxArgBytes, 131071},
		{"maxGitPathBytes", maxGitPathBytes, 131060},
		{"maxGitIdentBytes", maxGitIdentBytes, 131051},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

// gitArgSizes are the lengths a value is sent at, for a field whose longest
// value is max: max reaches git, and each longer one is a 400.
func gitArgSizes(max int) []int { return []int{max, max + 1, 128 << 10, 200_000} }

// routeGroups are the two path styles of every repo route: v1 with the
// repo in the token, and the path-style alias with the repo in the path.
func routeGroups(repoURL string) map[string]string {
	return map[string]string{"v1": "/api/v1/repos", "alias": "/api/repos/" + url.PathEscape(repoURL)}
}

// requireTooLong asserts the 400 of a value over max bytes: the message
// names the field and the limit, and a body field is the one item of
// invalid_params.
func requireTooLong(t *testing.T, resp *http.Response, body []byte, field string, max int, bodyField bool) {
	t.Helper()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %.300s)", resp.StatusCode, body)
	}
	if want := tooLongMsg(field, max); !bytes.Contains(body, []byte(want)) {
		t.Fatalf("body %.300s, want the message %q", body, want)
	}
	if !bodyField {
		return
	}
	var p problemBody
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("must be at most %d bytes", max)
	if len(p.InvalidParams) != 1 || p.InvalidParams[0].Name != field || p.InvalidParams[0].Reason != want {
		t.Fatalf("invalid_params = %+v, want [{%s %s}]", p.InvalidParams, field, want)
	}
}

// commitMessage reads a published commit's message back from a
// re-materialized cache repo, byte for byte.
func commitMessage(t *testing.T, e *testEnv, repoID, sha string) string {
	t.Helper()
	repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, repoID, "main")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := wrefGit(context.Background(), repoPath, nil, nil, "cat-file", "commit", sha)
	if err != nil {
		t.Fatal(err)
	}
	_, msg, ok := strings.Cut(raw, "\n\n")
	if !ok {
		t.Fatalf("commit %s has no message", sha)
	}
	return msg
}

// TestLongCommitMessage sends a commit message of 128 KiB less one byte
// (the longest argument), of 128 KiB and of 200,000 bytes on every route
// that writes a commit, in both route groups. git reads it on stdin, so
// each commit lands with the message byte for byte.
func TestLongCommitMessage(t *testing.T) {
	messages := []string{
		strings.Repeat("m", gitlimit.MaxArgBytes),
		strings.Repeat("m", 128<<10),
		strings.Repeat("subject\n\nbody line\n", 200_000/19) + "tail\n\n",
	}
	runBackends(t, func(t *testing.T, e *testEnv) {
		for group := range routeGroups("") {
			t.Run(group, func(t *testing.T) {
				fx := wrefSeedFixture(t, e)
				w := newIdentWriter(t, e, fx, routeGroups(fx.url)[group])
				for _, route := range identRoutes {
					for _, msg := range messages {
						t.Run(fmt.Sprintf("%s/%d", route, len(msg)), func(t *testing.T) {
							sha := w.write(t, route, msg, nil)
							// As with -m, a message gets a final line feed.
							want := msg
							if !strings.HasSuffix(want, "\n") {
								want += "\n"
							}
							if got := commitMessage(t, e, fx.repoID, sha); got != want {
								t.Fatalf("a %d-byte message came back as %d bytes", len(msg), len(got))
							}
						})
					}
				}
			})
		}
	})
}

// identRoutes are the routes that give an author and a committer to git in
// its environment. notes has only an author, which git gets as both.
var identRoutes = []string{"commit-pack", "diff-commit", "restore-commit", "merge"}

// identWriter writes commits through one route group of the full server,
// each one a change, so that git always makes a commit.
type identWriter struct {
	e      *testEnv
	fx     wrefFixture
	prefix string
	n      int
	// restoreTo alternates between the fixture's two trees: a restore to
	// the tree the branch already has changes nothing (412).
	restoreTo string
}

func newIdentWriter(t *testing.T, e *testEnv, fx wrefFixture, prefix string) *identWriter {
	t.Helper()
	return &identWriter{e: e, fx: fx, prefix: prefix, restoreTo: fx.c1}
}

// post sends one request of route with message and the extra metadata
// (author, committer) and returns the answer.
func (w *identWriter) post(t *testing.T, route, msg string, extra map[string]any) (*http.Response, []byte) {
	t.Helper()
	w.n++
	n := strconv.Itoa(w.n)
	switch route {
	case "commit-pack":
		body := wctLines(t,
			wctMeta("main", msg, []map[string]any{{"path": "cp" + n + ".txt", "content_id": "f"}}, extra),
			wctChunk("f", []byte(n+"\n"), true))
		return w.e.doReq(t, http.MethodPost, w.prefix+"/commit-pack", w.fx.token, body, "")
	case "diff-commit":
		patch := "diff --git a/dc" + n + ".txt b/dc" + n + ".txt\nnew file mode 100644\n--- /dev/null\n+++ b/dc" + n + ".txt\n@@ -0,0 +1 @@\n+" + n + "\n"
		body := wctLines(t, wctMeta("main", msg, nil, extra), wctDiffChunk([]byte(patch), true))
		return w.e.doReq(t, http.MethodPost, w.prefix+"/diff-commit", w.fx.token, body, "")
	case "restore-commit":
		md := map[string]any{"target_branch": "main", "base_ref": w.restoreTo, "commit_message": msg, "author": wctAuthor}
		maps.Copy(md, extra)
		raw, err := json.Marshal(map[string]any{"metadata": md})
		if err != nil {
			t.Fatal(err)
		}
		resp, body := w.e.doReq(t, http.MethodPost, w.prefix+"/restore-commit", w.fx.token, raw, "")
		if resp.StatusCode == http.StatusOK {
			w.restoreTo = map[string]string{w.fx.c1: w.fx.c2, w.fx.c2: w.fx.c1}[w.restoreTo]
		}
		return resp, body
	case "merge":
		// A new source branch one commit ahead of main, so that the merge
		// makes a merge commit.
		src := "src" + n
		resp, body := w.e.doReq(t, http.MethodPost, w.prefix+"/commit-pack", w.fx.token, wctLines(t,
			wctMeta(src, "source", []map[string]any{{"path": src + ".txt", "content_id": "f"}}, map[string]any{"base_branch": "main"}),
			wctChunk("f", []byte(n+"\n"), true)), "")
		wctRequireOK(t, resp, body)
		req := wmMergeBody(src, "main", "merge", map[string]any{"author": wmTestAuthor, "commit_message": msg})
		maps.Copy(req, extra)
		raw, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		return w.e.doReq(t, http.MethodPost, w.prefix+"/merge", w.fx.token, raw, "")
	}
	t.Fatalf("no route %q", route)
	return nil, nil
}

// write is post for a request that must make a commit; it returns the
// commit's sha.
func (w *identWriter) write(t *testing.T, route, msg string, extra map[string]any) string {
	t.Helper()
	resp, body := w.post(t, route, msg, extra)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: status = %d, want 200 (body %.300s)", route, resp.StatusCode, body)
	}
	var out struct {
		CommitSHA string `json:"commit_sha"`
		Commit    struct {
			CommitSHA string `json:"commit_sha"`
		} `json:"commit"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	sha := cmp.Or(out.Commit.CommitSHA, out.CommitSHA)
	if sha == "" {
		t.Fatalf("%s: no commit in %.300s", route, body)
	}
	return sha
}

// TestIdentsPastGitArgLimit sends each name and email of an author and a
// committer at maxGitIdentBytes and past it, on every route that gives
// them to git, in both route groups. git gets each one in an environment
// entry such as GIT_COMMITTER_EMAIL=<email>.
func TestIdentsPastGitArgLimit(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		for group := range routeGroups("") {
			t.Run(group, func(t *testing.T) {
				fx := wrefSeedFixture(t, e)
				w := newIdentWriter(t, e, fx, routeGroups(fx.url)[group])
				for _, route := range identRoutes {
					for _, field := range []string{"author.name", "author.email", "committer.name", "committer.email"} {
						for _, size := range gitArgSizes(maxGitIdentBytes) {
							t.Run(fmt.Sprintf("%s/%s/%d", route, field, size), func(t *testing.T) {
								who, part, _ := strings.Cut(field, ".")
								ident := map[string]any{"name": "n", "email": "e@test.invalid"}
								ident[part] = strings.Repeat("x", size)
								extra := map[string]any{who: ident}
								if size <= maxGitIdentBytes {
									w.write(t, route, "m", extra)
									return
								}
								resp, body := w.post(t, route, "m", extra)
								requireTooLong(t, resp, body, field, maxGitIdentBytes, route == "merge")
								if route == "commit-pack" || route == "diff-commit" {
									wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid")
								}
							})
						}
					}
				}
				for _, field := range []string{"author.name", "author.email"} {
					for _, size := range gitArgSizes(maxGitIdentBytes) {
						t.Run(fmt.Sprintf("notes/%s/%d", field, size), func(t *testing.T) {
							author := map[string]any{"name": "n", "email": "e@test.invalid"}
							author[strings.TrimPrefix(field, "author.")] = strings.Repeat("x", size)
							raw, err := json.Marshal(map[string]any{"sha": fx.c1, "action": "append", "note": "n", "author": author})
							if err != nil {
								t.Fatal(err)
							}
							resp, body := e.doReq(t, http.MethodPost, routeGroups(fx.url)[group]+"/notes", fx.token, raw, "")
							if size > maxGitIdentBytes {
								requireTooLong(t, resp, body, field, maxGitIdentBytes, true)
							} else if resp.StatusCode != http.StatusOK {
								t.Fatalf("status = %d, want 200 (body %.300s)", resp.StatusCode, body)
							}
						})
					}
				}
			})
		}
	})
}

// TestValuesPastGitArgLimitAreNotEchoed sends values over their limit that
// also hold a NUL. For a path, a range or a paths entry the size answers
// first, so the 400 never quotes the value. For an ident the NUL answers
// (neither problem quotes it), each field is named once, and the items of
// invalid_params keep the order of the body's fields.
func TestValuesPastGitArgLimitAreNotEchoed(t *testing.T) {
	long := strings.Repeat("v", 200_000) + "\x00"
	runBackends(t, func(t *testing.T, e *testEnv) {
		_, token := gitValuesRepo(t, e)
		grepBody, err := json.Marshal(map[string]any{"query": map[string]any{"pattern": "needle"}, "paths": []string{long}})
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name, method, path, field string
			max                       int
			body                      []byte
		}{
			{"diff path", http.MethodGet, "/api/v1/repos/diff?ref=main&path=" + url.QueryEscape(long), "path", maxGitPathBytes, nil},
			{"blame range", http.MethodGet, "/api/v1/repos/blame?ref=main&path=a.txt&range=" + url.QueryEscape(long), "range", gitlimit.MaxArgBytes, nil},
			{"grep paths", http.MethodPost, "/api/v1/repos/grep", "paths.0", maxGitPathBytes, grepBody},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := e.doReq(t, tc.method, tc.path, token, tc.body, "")
				requireTooLong(t, resp, body, tc.field, tc.max, tc.body != nil)
				if len(body) > 1024 {
					t.Fatalf("the 400 is %d bytes: it quotes the value", len(body))
				}
			})
		}

		fx := wrefSeedFixture(t, e)
		raw, err := json.Marshal(wmMergeBody("feature", "main", "merge", map[string]any{
			"author": map[string]any{"name": strings.Repeat("n", maxGitIdentBytes+1), "email": "a\x00b"}}))
		if err != nil {
			t.Fatal(err)
		}
		resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/merge", fx.token, raw, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("merge: status = %d, want 400 (body %.300s)", resp.StatusCode, body)
		}
		var p problemBody
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, ip := range p.InvalidParams {
			names = append(names, ip.Name)
		}
		if want := []string{"author.name", "author.email"}; !slices.Equal(names, want) {
			t.Fatalf("invalid_params names %v, want %v (body %.300s)", names, want, body)
		}

		// A name over the limit that also holds a NUL is one problem.
		raw, err = json.Marshal(wmMergeBody("feature", "main", "merge", map[string]any{
			"author": map[string]any{"name": strings.Repeat("n", maxGitIdentBytes+1) + "\x00", "email": "a@b"}}))
		if err != nil {
			t.Fatal(err)
		}
		resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos/merge", fx.token, raw, "")
		p = problemBody{}
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusBadRequest || len(p.InvalidParams) != 1 ||
			p.InvalidParams[0].Name != "author.name" || p.InvalidParams[0].Reason != ipcNulReason {
			t.Fatalf("merge: status %d, invalid_params %+v; want 400 naming author.name once, for its NUL", resp.StatusCode, p.InvalidParams)
		}
	})
}

// commit-tree -F - takes the message as is, and -m adds a line feed to a
// message without one: writeCommit must make the commit that -m made.
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

// gitValuesRepo seeds a one-file repository and returns its URL and a
// git:read token.
func gitValuesRepo(t *testing.T, e *testEnv) (string, string) {
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
	return repoURL, e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
}

// grepPattern is a pattern of n bytes that git can compile: git takes each
// line as a regex of its own and refuses one regex of 32 KiB.
func grepPattern(n int) string {
	p := strings.Repeat("needle\n", n/7)
	return p + strings.Repeat("n", n-len(p))
}

// TestReadValuesPastGitArgLimit sends each request value of the read
// routes that git takes as an argument at its limit and past it, in both
// route groups. The longest value reaches git and answers as any value
// that names nothing (404, or 200 with nothing found); each longer one is
// a 400 that names the field.
func TestReadValuesPastGitArgLimit(t *testing.T) {
	path := func(n int) string { return strings.Repeat("p", n) }
	query := func(route string) func(v string) (string, []byte) {
		return func(v string) (string, []byte) { return route + url.QueryEscape(v), nil }
	}
	grep := func(body func(v string) map[string]any) func(v string) (string, []byte) {
		return func(v string) (string, []byte) {
			raw, err := json.Marshal(body(v))
			if err != nil {
				panic(err)
			}
			return "/grep", raw
		}
	}
	cases := []struct {
		name  string
		field string
		max   int
		value func(n int) string
		req   func(v string) (string, []byte) // route suffix and body (POST when not nil)
		// atMax is the status of the longest value.
		atMax int
	}{
		{"file", "path", maxGitPathBytes, path, query("/file?ref=main&path="), http.StatusNotFound},
		{"files", "path", maxGitPathBytes, path, query("/files?ref=main&path="), http.StatusNotFound},
		{"files/metadata", "path", maxGitPathBytes, path, query("/files/metadata?ref=main&path="), http.StatusNotFound},
		{"commits", "path", maxGitPathBytes, path, query("/commits?ref=main&path="), http.StatusOK},
		{"diff", "path", maxGitPathBytes, path, query("/diff?ref=main&path="), http.StatusOK},
		{"branches/diff", "path", maxGitPathBytes, path, query("/branches/diff?branch=main&path="), http.StatusOK},
		{"blame path", "path", maxGitPathBytes, path, query("/blame?ref=main&path="), http.StatusNotFound},
		// git reads 1,000...01 as line 1.
		{"blame range", "range", gitlimit.MaxArgBytes, func(n int) string { return "1," + strings.Repeat("0", n-3) + "1" },
			query("/blame?ref=main&path=a.txt&range="), http.StatusOK},
		{"grep pattern", "query.pattern", gitlimit.MaxArgBytes, grepPattern,
			grep(func(v string) map[string]any { return map[string]any{"query": map[string]any{"pattern": v}} }), http.StatusOK},
		{"grep paths", "paths.0", maxGitPathBytes, path,
			grep(func(v string) map[string]any {
				return map[string]any{"query": map[string]any{"pattern": "needle"}, "paths": []string{v}}
			}), http.StatusOK},
	}
	runBackends(t, func(t *testing.T, e *testEnv) {
		repoURL, token := gitValuesRepo(t, e)
		for group, prefix := range routeGroups(repoURL) {
			for _, tc := range cases {
				t.Run(group+"/"+tc.name, func(t *testing.T) {
					for _, size := range gitArgSizes(tc.max) {
						t.Run(strconv.Itoa(size), func(t *testing.T) {
							v := tc.value(size)
							if len(v) != size {
								t.Fatalf("value is %d bytes, want %d", len(v), size)
							}
							suffix, body := tc.req(v)
							method := http.MethodGet
							if body != nil {
								method = http.MethodPost
							}
							resp, out := e.doReq(t, method, prefix+suffix, token, body, "")
							if size > tc.max {
								requireTooLong(t, resp, out, tc.field, tc.max, body != nil)
							} else if resp.StatusCode != tc.atMax {
								t.Fatalf("status = %d, want %d (body %.300s)", resp.StatusCode, tc.atMax, out)
							}
						})
					}
				})
			}
		}
	})
}

// A repository path can be longer than exec takes as one argument: fsck
// limits each tree entry name to 4096 bytes, not the path. Here 40 nested
// directories make a 160,125-byte path. The diff and files/metadata read
// it from git and must not hand it back to git as an argument, which would
// fail (500) on a good repository.
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
