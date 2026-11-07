package api

// Conflict paths too long for one git argument. Linux caps a single argv
// string at 128 KiB (MAX_ARG_STRLEN), so a path-limited `git ls-tree` cannot
// name such a path, yet git itself stores and merges it. These tests build the
// repositories with `git mktree -z` and friends, which take the path on stdin.
// All helpers carry an lp prefix.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// lpGit runs git in dir with the hermetic fixture environment and feeds it
// stdin. coreGit has no stdin, and the paths under test cannot go in argv.
func lpGit(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append(append([]string{}, coreGitIdentity...), args...)...)
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stdin)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %.200s: %v\n%.500s", strings.Join(args, " "), err, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// lpMktree writes a tree of the given `<mode> <type> <oid>\t<name>` records
// and returns its oid. A gitlink oid need not exist.
func lpMktree(t *testing.T, dir string, records ...string) string {
	t.Helper()
	return lpGit(t, dir, strings.Join(records, "\x00")+"\x00", "mktree", "-z", "--missing")
}

// lpTreeAt writes the tree that holds the entry (mode, oid) at each of paths,
// slash-separated paths whose components may be any bytes but NUL, and
// returns the root tree oid. It builds the trees bottom-up with mktree.
func lpTreeAt(t *testing.T, dir, mode, oid string, paths ...string) string {
	t.Helper()
	typ := "blob"
	if mode == "160000" {
		typ = "commit"
	}
	var records, subdirs []string
	below := map[string][]string{}
	for _, path := range paths {
		head, rest, nested := strings.Cut(path, "/")
		if !nested {
			records = append(records, mode+" "+typ+" "+oid+"\t"+head)
			continue
		}
		if below[head] == nil {
			subdirs = append(subdirs, head)
		}
		below[head] = append(below[head], rest)
	}
	for _, name := range subdirs {
		records = append(records, "040000 tree "+lpTreeAt(t, dir, mode, oid, below[name]...)+"\t"+name)
	}
	return lpMktree(t, dir, records...)
}

// lpName returns n bytes of the letter c, for a path component of a given size.
func lpName(n int, c byte) string { return strings.Repeat(string(c), n) }

// lpConflictRepo seeds a repo whose branches main and conflict both change
// the files at paths from the same base, so merging conflict into main
// conflicts at exactly paths. It returns a git token for the repo.
func lpConflictRepo(t *testing.T, e *testEnv, paths ...string) string {
	t.Helper()
	org := e.newOrg(t)
	url := "https://example.com/" + org + "/lp-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	lpGit(t, dir, "", "init", "-q", "-b", "main", ".")
	commit := func(content string, parents ...string) string {
		blob := lpGit(t, dir, content, "hash-object", "-w", "--stdin")
		args := []string{"commit-tree", lpTreeAt(t, dir, "100644", blob, paths...), "-m", "c"}
		for _, p := range parents {
			args = append(args, "-p", p)
		}
		return lpGit(t, dir, "", args...)
	}
	base := commit("base\n")
	ours := commit("main\n", base)
	theirs := commit("other\n", base)
	lpGit(t, dir, "", "update-ref", "refs/heads/main", ours)
	lpGit(t, dir, "", "update-ref", "refs/heads/conflict", theirs)
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{
		"refs/heads/main":     ours,
		"refs/heads/conflict": theirs,
	})
	return e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
}

// lpLongPaths are conflict paths over the 128 KiB argument limit, each in
// a different shape: many chunks, one component over the limit, and an
// oversized component between short ones.
var lpLongPaths = map[string]string{
	"many components":                   strings.TrimSuffix(strings.Repeat(lpName(1999, 'd')+"/", 100), "/"),
	"one component over the limit":      lpName(200000, 'f'),
	"oversized component in the middle": "src/" + lpName(150000, 'm') + "/f.txt",
}

// TestMergePreviewLongConflictPath proves that a conflict path above the
// per-argument limit previews with its content, and that the merge and the
// preview without content still report it.
func TestMergePreviewLongConflictPath(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		for name, path := range lpLongPaths {
			t.Run(name, func(t *testing.T) {
				token := lpConflictRepo(t, e, path)
				preview := func(query string) map[string]any {
					t.Helper()
					resp, raw := wmDo(t, srv, "GET", "/api/v1/repos/merge/preview?source_branch=conflict&target_branch=main"+query, token, nil)
					if resp.StatusCode != http.StatusOK {
						t.Fatalf("preview%s: status = %d, body %.300s", query, resp.StatusCode, raw)
					}
					return wmDecode(t, raw)
				}

				body := preview("&include_content=true")
				conflicts, _ := body["conflicts"].([]any)
				if body["status"] != "conflicted" || len(conflicts) != 1 {
					t.Fatalf("status %v with %d conflicts, want conflicted with 1", body["status"], len(conflicts))
				}
				c := conflicts[0].(map[string]any)
				if c["path"] != path {
					t.Fatalf("conflict path has %d bytes, want the %d-byte path", len(c["path"].(string)), len(path))
				}
				for side, want := range map[string]string{"base": "base\n", "ours": "main\n", "theirs": "other\n"} {
					if got := c[side].(map[string]any)["content"]; got != want {
						t.Errorf("%s content = %q, want %q", side, got, want)
					}
				}
				result := c["result"].(map[string]any)
				if content, _ := result["content"].(string); !strings.Contains(content, "<<<<<<<") {
					t.Errorf("result content = %q, want conflict markers", content)
				}

				if paths, _ := preview("")["conflict_paths"].([]any); len(paths) != 1 || paths[0] != path {
					t.Errorf("preview without content lists %d paths, want the one long path", len(paths))
				}
				resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", token,
					wmMergeBody("conflict", "main", "merge", map[string]any{"author": wmTestAuthor}))
				if paths, _ := wmDecode(t, raw)["conflict_paths"].([]any); resp.StatusCode != http.StatusConflict || len(paths) != 1 || paths[0] != path {
					t.Errorf("merge: status = %d with %d paths, want 409 with the one long path (body %.300s)", resp.StatusCode, len(paths), raw)
				}
			})
		}
	})
}

// TestTreePathEntry resolves tree entries whose paths span several pathspec
// chunks and oversized components, and finds no entry for a path that
// differs from one that exists.
func TestTreePathEntry(t *testing.T) {
	dir := t.TempDir()
	lpGit(t, dir, "", "init", "-q", "-b", "main", ".")
	blob := lpGit(t, dir, "x\n", "hash-object", "-w", "--stdin")
	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	manyChunks := strings.TrimSuffix(strings.Repeat(lpName(3999, 'c')+"/", 40), "/")
	over := wmMaxPathspecBytes + 1 // the shortest component that is listed, not named in argv
	cases := []struct{ name, path, mode, oid string }{
		{"short path", "a/b/c", "100644", blob},
		{"newline and tab in names", "d\ne/f\tg", "100755", blob},
		{"gitlink whose commit is absent", "vendor/dep", "160000", strings.Repeat("2", 40)},
		{"many chunks", manyChunks, "100644", blob},
		{"component at the limit", lpName(wmMaxPathspecBytes, 'j'), "100644", blob},
		{"component over the limit", lpName(over, 'k'), "100644", blob},
		{"component over the argument limit of the system", lpName(200000, 'f'), "100644", blob},
		{"short directories then an oversized file", "a/b/" + lpName(150000, 'g'), "120000", blob},
		{"oversized directory then more path", lpName(over, 'h') + "/x/y", "100644", blob},
		{"chunks, an oversized component, then a chunk", manyChunks + "/" + lpName(over+1000, 'i') + "/q/r", "100644", blob},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := lpTreeAt(t, dir, tc.mode, tc.oid, tc.path)
			got, err := wmTreePathEntry(t.Context(), sc, wmTreeListings{}, tree, tc.path)
			if want := (wmStageEntry{mode: tc.mode, oid: tc.oid}); err != nil || got != want {
				t.Fatalf("entry = %+v, %v, want %+v", got, err, want)
			}
			for name, absent := range map[string]string{
				"last byte changed":  tc.path[:len(tc.path)-1] + "z",
				"extended by a byte": tc.path + "z",
				"first byte changed": "z" + tc.path[1:],
				"child of the entry": tc.path + "/child",
			} {
				if got, err := wmTreePathEntry(t.Context(), sc, wmTreeListings{}, tree, absent); err != nil || got != (wmStageEntry{}) {
					t.Errorf("%s: entry = %+v, %v, want none", name, got, err)
				}
			}
		})
	}

	t.Run("an oversized name is matched exactly among its siblings", func(t *testing.T) {
		other := lpGit(t, dir, "y\n", "hash-object", "-w", "--stdin")
		name := lpName(over+10, 'x')
		tree := lpMktree(t, dir,
			"100644 blob "+blob+"\t"+name,
			"100644 blob "+other+"\t"+name+"y",
			"100644 blob "+blob+"\ta")
		for path, want := range map[string]wmStageEntry{
			name:           {mode: "100644", oid: blob},
			name + "y":     {mode: "100644", oid: other},
			name[1:]:       {},
			name + "z":     {},
			name[:50000]:   {},
			name[:over-1]:  {},
			name[:over+5]:  {}, // a prefix of an oversized name, itself oversized
			" " + name:     {}, // the same name with white space around it
			name + " ":     {},
			name + "y/sub": {},
		} {
			if got, err := wmTreePathEntry(t.Context(), sc, wmTreeListings{}, tree, path); err != nil || got != want {
				t.Errorf("%d-byte path ending %.5q: entry = %+v, %v, want %+v", len(path), path[len(path)-5:], got, err, want)
			}
		}
	})
}

// TestPathChunk cuts paths at component boundaries within the limit.
func TestPathChunk(t *testing.T) {
	for _, tc := range []struct {
		path  string
		limit int
		want  string
	}{
		{"a/b/c", 5, "a/b/c"},
		{"a/b/c", 4, "a/b"},
		{"a/b/c", 3, "a/b"},
		{"a/b/c", 2, "a"},
		{"abc/d", 3, "abc"},
		{"abc/d", 2, ""},
		{"abcd", 3, ""},
		{"/a", 1, ""},
	} {
		if got := wmPathChunk(tc.path, tc.limit); got != tc.want {
			t.Errorf("wmPathChunk(%q, %d) = %q, want %q", tc.path, tc.limit, got, tc.want)
		}
	}
}

// lpLongestLookup is the longest path that one ls-tree argument can hold on
// Linux: 128 KiB per argument, less the terminating NUL and the ten bytes of
// ":(literal)". A name of this size is looked up by pathspec on main.
const lpLongestLookup = 128<<10 - 1 - len(":(literal)")

// lpBudgetCtx returns a context carrying a git read budget of outputBytes of
// buffered git output, like a merge preview request.
func lpBudgetCtx(t *testing.T, outputBytes int64) context.Context {
	t.Helper()
	b := gitread.NewBudget(gitread.NewSlots(1, 1, 0), "lp", time.Minute, outputBytes)
	t.Cleanup(b.Close)
	return gitread.WithBudget(t.Context(), b)
}

// lpScratch returns a scratch odb layered over a fresh repo, and the repo's
// directory.
func lpScratch(t *testing.T) (*wcScratch, string) {
	t.Helper()
	dir := t.TempDir()
	lpGit(t, dir, "", "init", "-q", "-b", "main", ".")
	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	return sc, dir
}

// lpSiblings writes a tree of n blobs named by lpName(size, a letter) plus a
// two-digit number, and returns the tree oid and the names.
func lpSiblings(t *testing.T, dir string, n, size int) (string, []string) {
	t.Helper()
	blob := lpGit(t, dir, "x\n", "hash-object", "-w", "--stdin")
	var records, names []string
	for i := range n {
		name := lpName(size, 's') + fmt.Sprintf("%02d", i)
		names = append(names, name)
		records = append(records, "100644 blob "+blob+"\t"+name)
	}
	return lpMktree(t, dir, records...), names
}

// TestTreePathEntryOutputBudget holds the output a lookup costs, which the
// merge preview counts against one buffered-output allowance per request.
func TestTreePathEntryOutputBudget(t *testing.T) {
	sc, dir := lpScratch(t)

	t.Run("a directory is listed once for all its oversized names", func(t *testing.T) {
		// Each listing is about 500 KB. The allowance holds one, not four.
		tree, names := lpSiblings(t, dir, 4, wmMaxPathspecBytes+1000)
		ctx := lpBudgetCtx(t, 600<<10)
		listings := wmTreeListings{}
		for _, name := range names {
			if got, err := wmTreePathEntry(ctx, sc, listings, tree, name); err != nil || got.oid == "" {
				t.Fatalf("lookup of a %d-byte name: %+v, %v", len(name), got, err)
			}
		}
	})

	t.Run("a name that fits an argument is looked up without a listing", func(t *testing.T) {
		// The listing is about 790 KB and the allowance 250 KiB: only the
		// one-entry output of a pathspec lookup (about 131 KB) fits.
		tree, names := lpSiblings(t, dir, 6, lpLongestLookup-2)
		got, err := wmTreePathEntry(lpBudgetCtx(t, 250<<10), sc, wmTreeListings{}, tree, names[0])
		if err != nil || got.oid == "" {
			t.Fatalf("lookup of a %d-byte name: %+v, %v", len(names[0]), got, err)
		}
	})
}

// TestRenderConflictsListsDirectoryOnce renders conflicts whose names are too
// long for an argument and sit in one directory. Each lookup needs the
// listing of that directory, which emits all of it, and the preview buffers
// all git output under one allowance: the allowance here holds one listing
// and not two.
func TestRenderConflictsListsDirectoryOnce(t *testing.T) {
	sc, dir := lpScratch(t)
	const n = 4
	_, names := lpSiblings(t, dir, n, wmMaxPathspecBytes+100)
	blob := func(content string) string { return lpGit(t, dir, content, "hash-object", "-w", "--stdin") }
	base, ours, theirs, merged := blob("base\n"), blob("main\n"), blob("other\n"), blob("<<<<<<<\n")
	// The merged tree holds the merged result at each name. The stages hold
	// the three sides.
	var records []string
	mt := &wmMergeTreeResult{stages: map[string]map[int]wmStageEntry{}}
	for _, name := range names {
		records = append(records, "100644 blob "+merged+"\t"+name)
		path := "d/" + name
		mt.paths = append(mt.paths, path)
		mt.stages[path] = map[int]wmStageEntry{
			1: {mode: "100644", oid: base}, 2: {mode: "100644", oid: ours}, 3: {mode: "100644", oid: theirs},
		}
	}
	mt.tree = lpMktree(t, dir, "040000 tree "+lpMktree(t, dir, records...)+"\td")

	listing := n * (wmMaxPathspecBytes + 100)
	ctx := lpBudgetCtx(t, int64(listing+listing/2)) // one listing and a half
	conflicts, filtered, err := wmRenderConflicts(ctx, sc, mt)
	if err != nil {
		t.Fatalf("wmRenderConflicts: %v", err)
	}
	if len(conflicts) != n || len(filtered) != 0 {
		t.Fatalf("%d conflicts and %d filtered, want %d and 0", len(conflicts), len(filtered), n)
	}
	for _, c := range conflicts {
		if c.Result.OID != merged || c.Ours.Content != "main\n" || c.Theirs.Content != "other\n" {
			t.Errorf("conflict %.20q: result %+v, ours %q, theirs %q", c.Path, c.Result, c.Ours.Content, c.Theirs.Content)
		}
	}
}

// TestTreePathEntryGitFailures checks that a failing git call reaches the
// caller, whichever lookup made it, and is not read as an absent path.
func TestTreePathEntryGitFailures(t *testing.T) {
	sc, dir := lpScratch(t)
	missing := strings.Repeat("a", 40) // a valid oid that no object has
	for name, path := range map[string]string{
		"a short path":             "a/b/c",
		"a chunk of a long path":   lpName(100000, 'c') + "/" + lpName(100000, 'd'),
		"a name over the limit":    lpName(wmMaxPathspecBytes+1, 'e'),
		"a name after a long path": lpName(100000, 'c') + "/" + lpName(wmMaxPathspecBytes+1, 'e'),
	} {
		t.Run(name+" in a missing tree", func(t *testing.T) {
			got, err := wmTreePathEntry(t.Context(), sc, wmTreeListings{}, missing, path)
			if err == nil {
				t.Fatalf("entry = %+v with no error, want git's error", got)
			}
		})
	}

	t.Run("a listing over the output allowance", func(t *testing.T) {
		tree, names := lpSiblings(t, dir, 4, wmMaxPathspecBytes+100)
		_, err := wmTreePathEntry(lpBudgetCtx(t, 250<<10), sc, wmTreeListings{}, tree, names[0])
		if !errors.Is(err, gitread.ErrOutputLimit) {
			t.Fatalf("error = %v, want %v", err, gitread.ErrOutputLimit)
		}
	})
}

// TestParseListing keeps the entries whose names are too long for an
// argument, and nothing else.
func TestParseListing(t *testing.T) {
	oid := strings.Repeat("1", 40)
	long := lpName(wmMaxPathspecBytes+1, 'l')
	record := func(mode, typ, name string) string { return mode + " " + typ + " " + oid + "\t" + name + "\x00" }
	out := record("100644", "blob", "short") +
		record("040000", "tree", long+"\tt a b") + // tabs and spaces in a name
		record("100644", "blob", lpName(wmMaxPathspecBytes, 'k')) + // the longest name that fits an argument
		record("160000", "commit", long+"2")
	got, err := wmParseListing(out)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]wmStageEntry{
		long + "\tt a b": {mode: "040000", oid: oid},
		long + "2":       {mode: "160000", oid: oid},
	}
	if !maps.Equal(got, want) {
		names := make([]string, 0, len(got))
		for name := range got {
			names = append(names, fmt.Sprintf("%d bytes", len(name)))
		}
		t.Fatalf("kept %v, want the two oversized names", names)
	}
	// What is kept is a copy. A name, a mode or an oid that points into the
	// listing holds all of it (a listing of 500,000 entries is 35 MB) for as
	// long as the entry lives.
	lo := uintptr(unsafe.Pointer(unsafe.StringData(out)))
	inListing := func(s string) bool {
		p := uintptr(unsafe.Pointer(unsafe.StringData(s)))
		return s != "" && p >= lo && p < lo+uintptr(len(out))
	}
	for name, entry := range got {
		if inListing(name) || inListing(entry.mode) || inListing(entry.oid) {
			t.Errorf("a kept entry of %d bytes points into the listing", len(name))
		}
	}
	for name, bad := range map[string]string{
		"a record with no tab":                "no tab here\x00",
		"an oversized name with a bad object": "100644 blob nothex\t" + long + "\x00",
	} {
		if _, err := wmParseListing(bad); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if got, err := wmParseListing(""); err != nil || len(got) != 0 {
		t.Errorf("an empty listing: %v, %v", got, err)
	}
}
