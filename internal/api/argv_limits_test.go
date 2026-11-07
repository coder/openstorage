package api

// Request values that cannot be passed to git as they are: one argument or
// environment variable of 128 KiB or more (E2BIG), and a NUL byte. Every
// client string that reaches a git command line or environment answers with
// a 4xx, never a 500, and a commit message of any size is kept whole. All
// helpers carry an al prefix.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// alRequireArgLimit skips a test that needs the 128 KiB limit of one argument:
// Linux allows 32 pages per argument (MAX_ARG_STRLEN), which is 128 KiB with
// 4 KiB pages. Other systems have another limit, and 200,000 bytes may fit.
func alRequireArgLimit(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" || os.Getpagesize() != 4096 {
		t.Skipf("the limit of one argument is 128 KiB only on Linux with 4 KiB pages (page size %d)", os.Getpagesize())
	}
}

// alKinds names the hostile values of alValues in the order of alWant.
var alKinds = [3]string{"long", "long path", "NUL"}

// alWant is the status of each field for the values of alKinds, in order:
// long, long path, NUL. A value that git cannot take as one argument answers
// 422 (too long) or 400 (NUL). A ref, which no repository holds at that
// size, is a 404, and a field that validation or the code does not hand to
// git as an argument answers as it did before.
var alWant = map[string][3]int{
	"archive prefix":              {200, 200, 400},
	"archive ref":                 {404, 404, 404},
	"blame path":                  {422, 422, 400},
	"blame range":                 {422, 422, 400},
	"blame ref":                   {404, 404, 404},
	"branch name":                 {404, 404, 404},
	"branches/create base_ref":    {404, 404, 404},
	"branches/create name":        {400, 400, 400},
	"branches/diff base":          {404, 404, 404},
	"branches/diff branch":        {404, 404, 404},
	"commit ref":                  {404, 404, 404},
	"commit-pack author.email":    {422, 422, 400},
	"commit-pack author.name":     {422, 422, 400},
	"commit-pack base_branch":     {400, 400, 400},
	"commit-pack commit_message":  {200, 200, 400},
	"commit-pack committer.email": {422, 422, 400},
	"commit-pack committer.name":  {422, 422, 400},
	"commit-pack files.path":      {200, 200, 400},
	"commit-pack target_branch":   {400, 400, 400},
	"commits branch":              {404, 404, 404},
	"commits cursor":              {400, 400, 400},
	"commits path":                {422, 422, 400},
	"commits ref":                 {404, 404, 404},
	"diff base_ref":               {404, 404, 404},
	"diff path":                   {422, 422, 400},
	"diff ref":                    {404, 404, 404},
	"file path":                   {422, 422, 400},
	"file ref":                    {404, 404, 404},
	"files cursor":                {400, 400, 400},
	"files path":                  {422, 422, 400},
	"files ref":                   {404, 404, 404},
	"files/metadata path":         {422, 422, 400},
	"files/metadata ref":          {404, 404, 404},
	"grep extension_filters":      {200, 200, 200},
	"grep include_globs":          {200, 200, 200},
	"grep paths":                  {422, 422, 400},
	"grep pattern":                {422, 422, 400},
	"grep ref":                    {404, 404, 404},
	"merge author.email":          {422, 422, 400},
	"merge author.name":           {422, 422, 400},
	"merge commit_message":        {200, 200, 400},
	"merge committer.email":       {422, 422, 400},
	"merge committer.name":        {422, 422, 400},
	"notes author email":          {422, 422, 400},
	"notes author name":           {422, 422, 400},
	"notes note":                  {200, 200, 200},
	"notes notes_ref":             {400, 400, 400},
	"notes object_ref":            {404, 404, 404},
	"notes write notes_ref":       {400, 400, 400},
	"tag name":                    {404, 404, 404},
	"tags create message":         {400, 400, 400},
	"tags create name":            {400, 400, 400},
}

// alValues are hostile values for one string: far over the limit of one
// argument, a long path of short components, and an embedded NUL.
func alValues() map[string]string {
	return map[string]string{
		"long":      strings.Repeat("x", 200_000),
		"long path": strings.TrimSuffix(strings.Repeat(strings.Repeat("d", 999)+"/", 200), "/"),
		"NUL":       "a\x00b",
	}
}

// alJSON sends a JSON body to the env server.
func alJSON(t *testing.T, e *testEnv, method, path, token string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, out := e.doReq(t, method, path, "", raw, "Bearer "+token)
	return resp.StatusCode, out
}

// TestRequestValuesGitCannotTake sends each hostile value in each field
// that reaches git, on the read routes and the ref routes.
func TestRequestValuesGitCannotTake(t *testing.T) {
	alRequireArgLimit(t)
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)
		rw := e.mint(t, tokenSpec{iss: org, repo: fx.repo.URL, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})

		get := func(tmpl string) func(v string) (int, []byte) {
			return func(v string) (int, []byte) {
				resp, body := coreGet(t, e, http.MethodGet, strings.ReplaceAll(tmpl, "{v}", url.QueryEscape(v)), fx.token, nil)
				return resp.StatusCode, body
			}
		}
		post := func(path string, body func(v string) any) func(v string) (int, []byte) {
			return func(v string) (int, []byte) { return alJSON(t, e, http.MethodPost, path, rw, body(v)) }
		}
		author := func(name, email string) map[string]any { return map[string]any{"name": name, "email": email} }
		cases := map[string]func(v string) (int, []byte){
			"file ref":             get("/api/v1/repos/file?path=README.md&ref={v}"),
			"file path":            get("/api/v1/repos/file?ref=main&path={v}"),
			"files ref":            get("/api/v1/repos/files?ref={v}"),
			"files path":           get("/api/v1/repos/files?path={v}"),
			"files cursor":         get("/api/v1/repos/files?cursor={v}"),
			"files/metadata ref":   get("/api/v1/repos/files/metadata?ref={v}"),
			"files/metadata path":  get("/api/v1/repos/files/metadata?path={v}"),
			"branches/diff branch": get("/api/v1/repos/branches/diff?branch={v}&base=main"),
			"branches/diff base":   get("/api/v1/repos/branches/diff?branch=feature&base={v}"),
			"commits ref":          get("/api/v1/repos/commits?ref={v}"),
			"commits branch":       get("/api/v1/repos/commits?branch={v}"),
			"commits path":         get("/api/v1/repos/commits?path={v}"),
			"commits cursor":       get("/api/v1/repos/commits?cursor={v}"),
			"commit ref":           get("/api/v1/repos/commit?ref={v}"),
			"diff ref":             get("/api/v1/repos/diff?ref={v}"),
			"diff base_ref":        get("/api/v1/repos/diff?ref=feature&base_ref={v}"),
			"diff path":            get("/api/v1/repos/diff?ref=feature&path={v}"),
			"blame ref":            get("/api/v1/repos/blame?path=README.md&ref={v}"),
			"blame path":           get("/api/v1/repos/blame?path={v}"),
			"blame range":          get("/api/v1/repos/blame?path=README.md&range={v}"),
			"tag name":             get("/api/v1/repos/tag?name={v}"),
			"branch name":          get("/api/v1/repos/branch?name={v}"),
			"notes object_ref":     get("/api/v1/repos/notes?object_ref={v}"),
			"notes notes_ref":      get("/api/v1/repos/notes?object_ref=main&notes_ref={v}"),
			"grep pattern": post("/api/v1/repos/grep", func(v string) any {
				return map[string]any{"query": map[string]any{"pattern": v}, "ref": "main"}
			}),
			"grep paths": post("/api/v1/repos/grep", func(v string) any {
				return map[string]any{"query": map[string]any{"pattern": "hello"}, "ref": "main", "paths": []string{v}}
			}),
			"grep ref": post("/api/v1/repos/grep", func(v string) any {
				return map[string]any{"query": map[string]any{"pattern": "hello"}, "ref": v}
			}),
			"grep include_globs": post("/api/v1/repos/grep", func(v string) any {
				return map[string]any{"query": map[string]any{"pattern": "hello"}, "ref": "main",
					"file_filters": map[string]any{"include_globs": []string{v}}}
			}),
			"grep extension_filters": post("/api/v1/repos/grep", func(v string) any {
				return map[string]any{"query": map[string]any{"pattern": "hello"}, "ref": "main",
					"file_filters": map[string]any{"extension_filters": []string{v}}}
			}),
			"archive ref": post("/api/v1/repos/archive", func(v string) any { return map[string]any{"ref": v} }),
			"archive prefix": post("/api/v1/repos/archive", func(v string) any {
				return map[string]any{"ref": "main", "archive": map[string]any{"prefix": v}}
			}),
			"branches/create name": post("/api/v1/repos/branches/create", func(v string) any {
				return map[string]any{"target_branch": v, "base_ref": "main"}
			}),
			"branches/create base_ref": post("/api/v1/repos/branches/create", func(v string) any {
				return map[string]any{"target_branch": "al-new", "base_ref": v}
			}),
			"tags create name": post("/api/v1/repos/tags", func(v string) any { return map[string]any{"name": v, "sha": fx.c1} }),
			"tags create message": post("/api/v1/repos/tags", func(v string) any {
				return map[string]any{"name": "al-tag", "sha": fx.c1, "message": v}
			}),
			"notes note": post("/api/v1/repos/notes", func(v string) any {
				return map[string]any{"object_ref": fx.c1, "note": v, "action": "append"}
			}),
			"notes author name": post("/api/v1/repos/notes", func(v string) any {
				return map[string]any{"object_ref": fx.c2, "note": "n", "author": author(v, "a@b.invalid")}
			}),
			"notes author email": post("/api/v1/repos/notes", func(v string) any {
				return map[string]any{"object_ref": fx.c3, "note": "n", "author": author("n", v)}
			}),
			"notes write notes_ref": post("/api/v1/repos/notes", func(v string) any {
				return map[string]any{"object_ref": fx.c1, "note": "n", "notes_ref": v}
			}),
		}
		for name, run := range cases {
			for i, kind := range alKinds {
				t.Run(name+"/"+kind, func(t *testing.T) {
					status, body := run(alValues()[kind])
					if want := alWant[name][i]; status != want {
						t.Fatalf("status %d, want %d: %.300s", status, want, body)
					}
				})
			}
		}
	})
}

// TestMergeValuesGitCannotTake covers the merge route: the commit message
// and the identities of the merge commit.
func TestMergeValuesGitCannotTake(t *testing.T) {
	alRequireArgLimit(t)
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)
		next := 0
		for _, field := range []string{"commit_message", "author.name", "author.email", "committer.name", "committer.email"} {
			for i, kind := range alKinds {
				t.Run(field+"/"+kind, func(t *testing.T) {
					v := alValues()[kind]
					extra := map[string]any{"author": wmTestAuthor, "commit_message": "merge"}
					switch field {
					case "commit_message":
						extra["commit_message"] = v
					case "author.name":
						extra["author"] = map[string]any{"name": v, "email": "wm@test.invalid"}
					case "author.email":
						extra["author"] = map[string]any{"name": "wm", "email": v}
					case "committer.name":
						extra["committer"] = map[string]any{"name": v, "email": "wm@test.invalid"}
					case "committer.email":
						extra["committer"] = map[string]any{"name": "wm", "email": v}
					}
					target := wmTargets[next]
					next++
					resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token, wmMergeBody("feature", target, "merge", extra))
					if want := alWant["merge "+field][i]; resp.StatusCode != want {
						t.Fatalf("status %d, want %d: %.300s", resp.StatusCode, want, raw)
					}
				})
			}
		}
	})
}

// TestCommitPackValuesGitCannotTake covers the commit-pack route: the commit
// message, the identities, and a file path.
func TestCommitPackValuesGitCannotTake(t *testing.T) {
	alRequireArgLimit(t)
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, _ := wctRepo(t, e)
		next := 0
		files := func(path string) []map[string]any {
			return []map[string]any{{"path": path, "content_id": "id1", "operation": "upsert", "mode": "100644"}}
		}
		for _, field := range []string{"commit_message", "author.name", "author.email", "committer.name", "committer.email", "files.path", "target_branch", "base_branch"} {
			for i, kind := range alKinds {
				t.Run(field+"/"+kind, func(t *testing.T) {
					v := alValues()[kind]
					next++
					target := "al-" + strings.ReplaceAll(strings.ToLower(kind), " ", "-") + "-" + string(rune('a'+next))
					extra := map[string]any{}
					path, message := "f.txt", "c"
					switch field {
					case "commit_message":
						message = v
					case "author.name":
						extra["author"] = map[string]any{"name": v, "email": "wct@test.invalid"}
					case "author.email":
						extra["author"] = map[string]any{"name": "wct", "email": v}
					case "committer.name":
						extra["committer"] = map[string]any{"name": v, "email": "wct@test.invalid"}
					case "committer.email":
						extra["committer"] = map[string]any{"name": "wct", "email": v}
					case "files.path":
						path = v
					case "target_branch":
						target = v
					case "base_branch":
						extra["base_branch"] = v
					}
					body := wctLines(t, wctMeta(target, message, files(path), extra), wctChunk("id1", []byte("x\n"), true))
					resp, raw := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(body))
					if want := alWant["commit-pack "+field][i]; resp.StatusCode != want {
						t.Fatalf("status %d, want %d: %.300s", resp.StatusCode, want, raw)
					}
					if resp.StatusCode == http.StatusBadRequest {
						// A 400 of this route is a result envelope, whichever value caused it.
						wctRequireFail(t, resp, raw, http.StatusBadRequest, "invalid")
					}
				})
			}
		}
	})
}

// alScratch returns a scratch odb over a fresh bare-enough repo, and the
// repo's directory. Its empty tree is the one commits point at.
func alScratch(t *testing.T) (*wcScratch, string, string) {
	t.Helper()
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	tree := strings.TrimSpace(coreGit(t, dir, "write-tree"))
	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	return sc, dir, tree
}

// alMessage returns the message of a commit object read from git's output
// of `cat-file commit`: the bytes after the blank line of its header.
func alMessage(t *testing.T, raw string) string {
	t.Helper()
	_, msg, found := strings.Cut(raw, "\n\n")
	if !found {
		t.Fatalf("commit has no message separator: %.200q", raw)
	}
	return msg
}

// TestWriteCommitMessage keeps a commit message whole, whatever its size,
// and ends it as `git commit-tree -m` does: with one newline when it lacks
// one, and as it is when it has one.
func TestWriteCommitMessage(t *testing.T) {
	sc, dir, tree := alScratch(t)
	author := wcAuthor{Name: "al", Email: "al@test.invalid"}
	for name, message := range map[string]string{
		"no newline":         "subject",
		"one newline":        "subject\n",
		"two newlines":       "subject\n\n",
		"body":               "subject\n\nbody line\nsecond line",
		"white space":        "  subject \t",
		"non-UTF-8":          "caf\xe9",
		"over one argument":  strings.Repeat("m", 200_000),
		"just over, newline": strings.Repeat("m", 131_072) + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := sc.writeCommit(t.Context(), tree, nil, message, author, author)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := sc.git(t.Context(), nil, nil, "cat-file", "commit", got)
			if err != nil {
				t.Fatal(err)
			}
			msg := alMessage(t, raw)
			want := message
			if !strings.HasSuffix(want, "\n") {
				want += "\n"
			}
			if len(message) < 100_000 {
				// git's own -m is the oracle where its argument fits. git
				// re-encodes a message that is not UTF-8, as it does for -F.
				oracle := exec.Command("git", "commit-tree", "-m", message, tree)
				oracle.Dir = dir
				oracle.Env = append(os.Environ(), "GIT_AUTHOR_NAME=al", "GIT_AUTHOR_EMAIL=al@test.invalid", "GIT_COMMITTER_NAME=al", "GIT_COMMITTER_EMAIL=al@test.invalid")
				out, err := oracle.Output()
				if err != nil {
					t.Fatalf("git commit-tree -m: %v", err)
				}
				want = alMessage(t, coreGit(t, dir, "cat-file", "commit", strings.TrimSpace(string(out))))
			}
			if msg != want {
				t.Fatalf("message has %d bytes, want %d (%.40q... want %.40q...)", len(msg), len(want), msg, want)
			}
		})
	}
}

// TestLongCommitMessageRoundTrips merges with a message over the limit of one
// argument and reads the commit back whole.
func TestLongCommitMessageRoundTrips(t *testing.T) {
	alRequireArgLimit(t)
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)
		message := strings.Repeat("long message line\n", 12_000) // 216,000 bytes
		resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
			wmMergeBody("feature", "t-clean", "merge", map[string]any{"author": wmTestAuthor, "commit_message": message}))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %.300s", resp.StatusCode, raw)
		}
		sha, _ := wmDecode(t, raw)["commit_sha"].(string)
		repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
		if err != nil {
			t.Fatal(err)
		}
		if msg := alMessage(t, coreGit(t, repoPath, "cat-file", "commit", sha)); msg != message {
			t.Fatalf("stored message has %d bytes, want %d", len(msg), len(message))
		}
	})
}

// alRecordGit puts a `git` first on PATH that appends the argument count and
// the first argument of each call to a log, then runs the real git. It returns
// the log's path.
func alRecordGit(t *testing.T) string {
	t.Helper()
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	script := "#!/bin/sh\necho \"$# $*\" | cut -c1-80 >> '" + log + "'\nexec '" + real + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return log
}

// TestPromotedCommitsTakeTheTipsOnStdin counts the commits that a merge
// promotes with every other branch tip excluded. A repository can hold more
// tips than one command line takes, so the tips are not arguments.
func TestPromotedCommitsTakeTheTipsOnStdin(t *testing.T) {
	log := alRecordGit(t)
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	const n = 400
	var stream strings.Builder
	for i := 1; i <= n; i++ {
		stream.WriteString("commit refs/heads/main\nmark :" + strconv.Itoa(i) + "\ncommitter al <al@test.invalid> " + strconv.Itoa(1_700_000_000+i) + " +0000\ndata 2\nc\n")
		if i > 1 {
			stream.WriteString("from :" + strconv.Itoa(i-1) + "\n")
		}
		stream.WriteString("\n")
	}
	stream.WriteString("done\n")
	fi := exec.Command("git", "fast-import", "--quiet", "--done")
	fi.Dir, fi.Stdin = dir, strings.NewReader(stream.String())
	if out, err := fi.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	shas := strings.Fields(coreGit(t, dir, "rev-list", "--reverse", "main"))
	if len(shas) != n {
		t.Fatalf("%d commits, want %d", len(shas), n)
	}
	tips := map[string]string{}
	for i, sha := range shas[:n-1] {
		tips["refs/heads/b"+strconv.Itoa(i)] = sha
	}
	tips[auth.EphemeralRefName("scratch")] = shas[n-1] // ephemeral tips never exclude
	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)

	got, err := wmPromotedCommits(t.Context(), sc, tips, shas[n-1], false, true)
	if err != nil || got != 1 {
		t.Fatalf("promoted = %d, %v, want 1", got, err)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(calls), " rev-list ") {
		t.Fatalf("the recording git did not run the count: %q", calls)
	}
	for line := range strings.Lines(string(calls)) {
		if strings.Contains(line, "rev-list") && !strings.HasPrefix(line, "3 rev-list --count --stdin") {
			t.Errorf("the count ran as %q, want 3 arguments", strings.TrimSpace(line))
		}
	}
}
