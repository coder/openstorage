package gitread

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBlame(t *testing.T) {
	f := newFixture(t)
	lines, err := Blame(context.Background(), f.dir, f.c2, "README.md", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 4 {
		t.Fatalf("got %d blame lines, want 4: %+v", len(lines), lines)
	}
	// Lines 1-3 came from c1, line 4 from c2.
	for i, want := range []string{f.c1, f.c1, f.c1, f.c2} {
		l := lines[i]
		if l.LineNumber != i+1 {
			t.Errorf("line %d numbered %d", i+1, l.LineNumber)
		}
		if l.CommitSHA != want {
			t.Errorf("line %d attributed to %s, want %s", i+1, l.CommitSHA, want)
		}
		if l.OriginalPath != "README.md" {
			t.Errorf("line %d original path %q", i+1, l.OriginalPath)
		}
		if l.AuthorName != "Alice Author" || l.AuthorEmail != "alice@example.com" {
			t.Errorf("line %d author %q <%q>", i+1, l.AuthorName, l.AuthorEmail)
		}
		if l.CommitterName != "Bob Committer" {
			t.Errorf("line %d committer %q", i+1, l.CommitterName)
		}
	}
	// c1 introduced its lines at the same numbers.
	if lines[1].OriginalLineNumber != 2 {
		t.Errorf("line 2 original line = %d", lines[1].OriginalLineNumber)
	}
	// The c2 group carries previous = c1; the root c1 group has none.
	if lines[3].PreviousCommitSHA != f.c1 {
		t.Errorf("line 4 previous = %q, want %s", lines[3].PreviousCommitSHA, f.c1)
	}
	if lines[0].PreviousCommitSHA != "" {
		t.Errorf("root line previous = %q, want empty", lines[0].PreviousCommitSHA)
	}
	if lines[0].Summary != "root commit" || lines[3].Summary != "second commit" {
		t.Errorf("summaries = %q / %q", lines[0].Summary, lines[3].Summary)
	}
	// Timestamps carry the fixture's +0200 author time.
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("+0200", 2*3600))
	if !lines[0].AuthorTime.Equal(want) {
		t.Errorf("author time = %v, want %v", lines[0].AuthorTime, want)
	}
	wantCommit := time.Date(2024, 1, 2, 4, 5, 6, 0, time.FixedZone("+0200", 2*3600))
	if !lines[0].CommitterTime.Equal(wantCommit) {
		t.Errorf("committer time = %v, want %v", lines[0].CommitterTime, wantCommit)
	}
}

func TestBlameRanges(t *testing.T) {
	f := newFixture(t)
	lines, err := Blame(context.Background(), f.dir, f.c2, "README.md", []string{"2,3"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 || lines[0].LineNumber != 2 || lines[1].LineNumber != 3 {
		t.Fatalf("ranged blame = %+v", lines)
	}
}

func TestBlameRenamedFileTracksOriginalPath(t *testing.T) {
	f := newFixture(t)
	lines, err := Blame(context.Background(), f.dir, f.c2, "dir/renamed.txt", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 {
		t.Fatalf("blame lines = %+v", lines)
	}
	// Content is unchanged since c1, where the file was dir/data.txt.
	if lines[0].CommitSHA != f.c1 || lines[0].OriginalPath != "dir/data.txt" {
		t.Errorf("renamed blame = %+v", lines[0])
	}
}

func TestBlameMissingPath(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := Blame(ctx, f.dir, f.c1, "does-not-exist.txt", nil, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing path err = %v, want ErrNotFound", err)
	}
	if _, err := Blame(ctx, f.dir, f.c1, "", nil, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("empty path err = %v, want ErrNotFound", err)
	}
	// A range STARTING past EOF is a git usage error, not a not-found
	// (modern git clamps an end past EOF, so "1,999" succeeds).
	if _, err := Blame(ctx, f.dir, f.c1, "README.md", []string{"999,1000"}, false); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("range past EOF err = %v, want non-NotFound error", err)
	}
}

// blame needs a file: a path that names an empty folder (ls-tree lists
// the folder and nothing in it) is not found, not a git failure.
func TestBlameOfAnEmptyFolderIsNotFound(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare")
	empty := strings.TrimSpace(mustGit(t, dir, "hash-object", "-t", "tree", "-w", "/dev/null"))
	mk := exec.Command("git", "mktree")
	mk.Dir = dir
	mk.Env = append(os.Environ(), gitEnv...)
	mk.Stdin = strings.NewReader("040000 tree " + empty + "\te\n")
	tree, err := mk.Output()
	if err != nil {
		t.Fatalf("git mktree: %v", err)
	}
	commit := strings.TrimSpace(mustGit(t, dir, "commit-tree", strings.TrimSpace(string(tree)), "-m", "empty folder"))
	if _, err := Blame(context.Background(), dir, commit, "e", nil, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("blame of an empty folder: err = %v, want ErrNotFound", err)
	}
}

// A path that exists but whose blob or folder git cannot read is a
// failure of the repository, not a missing path: blame returns git's
// error, not ErrNotFound.
func TestBlameOfAnUnreadableObjectIsNotNotFound(t *testing.T) {
	for _, tc := range []struct{ name, path, object string }{
		{"blob", "d/f.txt", "HEAD:d/f.txt"},
		{"folder", "d/f.txt", "HEAD:d"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			mustGit(t, dir, "init", "-q", "--initial-branch=main")
			writeFile(t, dir, "d/f.txt", "one\n", 0o644)
			mustGit(t, dir, "add", "-A")
			mustGit(t, dir, "commit", "-q", "-m", "c")
			commit := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
			oid := strings.TrimSpace(mustGit(t, dir, "rev-parse", tc.object))
			if err := os.Remove(filepath.Join(dir, ".git", "objects", oid[:2], oid[2:])); err != nil {
				t.Fatal(err)
			}
			_, err := Blame(context.Background(), dir, commit, tc.path, nil, false)
			if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrBadRange) {
				t.Fatalf("blame with the %s object gone: err = %v, want git's failure", tc.name, err)
			}
		})
	}
}

// blamePathNames are tree entry names that git's blame porcelain prints in
// C-quoted form (#590), plus two it prints verbatim.
var blamePathNames = []string{
	"plain.txt",
	" lead space",
	"caf\u00e9-utf8.txt",
	`q"t`,
	`back\slash`,
	`"lead-quote`,
	"ctl\a\b\t\n\v\f\r\x01\x1f\x7f.txt",
	"a\xffb",      // not valid UTF-8
	"caf\xe9.txt", // Latin-1
}

// blamePathOldName is renamed to "renamed.txt" in the second commit, so
// its blame reports the old name as original_path.
const blamePathOldName = "r\u00e9sum\u00e9 \"old\".txt"

// newBlamePathFixture commits every blamePathNames entry plus
// blamePathOldName, then renames blamePathOldName to renamed.txt.
func newBlamePathFixture(t *testing.T) (dir, c1, c2 string) {
	t.Helper()
	dir = t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	for i, name := range append([]string{blamePathOldName}, blamePathNames...) {
		writeFile(t, dir, name, fmt.Sprintf("line %d\n", i), 0o644)
	}
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "names")
	c1 = strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	mustGit(t, dir, "mv", "--", blamePathOldName, "renamed.txt")
	mustGit(t, dir, "commit", "-q", "-m", "rename")
	c2 = strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	return dir, c1, c2
}

func TestBlameOriginalPathIsTheTreeName(t *testing.T) {
	dir, c1, c2 := newBlamePathFixture(t)
	for _, quotePath := range []string{"true", "false"} {
		t.Run("core.quotePath="+quotePath, func(t *testing.T) {
			// Blame's git child inherits the test's environment.
			t.Setenv("GIT_CONFIG_COUNT", "1")
			t.Setenv("GIT_CONFIG_KEY_0", "core.quotePath")
			t.Setenv("GIT_CONFIG_VALUE_0", quotePath)
			check := func(path, want string) {
				t.Helper()
				lines, err := Blame(context.Background(), dir, c2, path, nil, false)
				if err != nil {
					t.Fatalf("blame %q: %v", path, err)
				}
				if len(lines) != 1 || lines[0].CommitSHA != c1 {
					t.Fatalf("blame %q = %+v, want one line from %s", path, lines, c1)
				}
				if got := lines[0].OriginalPath; got != want {
					t.Errorf("blame %q: original path %q, want %q", path, got, want)
				}
			}
			for _, name := range blamePathNames {
				check(name, name)
			}
			check("renamed.txt", blamePathOldName)
		})
	}
}

func TestUnquoteGitPathRejectsMalformedQuoting(t *testing.T) {
	for _, s := range []string{
		`"`,
		`"abc`,
		`"abc\"`,
		`"a"b`,
		`"a\q"`,
		`"a\x41"`,
		`"a\4000"`,
		`"a\389"`,
		`"a\38"`,
		`"a\3"`,
		`"a\37`,
		`"a\`,
	} {
		if got, err := unquoteGitPath(s); err == nil {
			t.Errorf("unquoteGitPath(%q) = %q, want an error", s, got)
		}
	}
}

// blameCommitTree commits a tree of the given "<mode> <type> <oid>" entries
// (mktree -z, so a name may hold a newline) and returns the commit.
func blameCommitTree(t *testing.T, dir string, entries map[string]string) string {
	t.Helper()
	var in strings.Builder
	for name, entry := range entries {
		in.WriteString(entry + "\t" + name + "\x00")
	}
	mk := exec.Command("git", "mktree", "-z")
	mk.Dir = dir
	mk.Env = append(os.Environ(), gitEnv...)
	mk.Stdin = strings.NewReader(in.String())
	tree, err := mk.Output()
	if err != nil {
		t.Fatalf("git mktree: %v", err)
	}
	return strings.TrimSpace(mustGit(t, dir, "commit-tree", strings.TrimSpace(string(tree)), "-m", "c"))
}

// git names a missing or unreadable file "fatal: no such path <path> in
// <commit>", so a path that holds a newline and "fatal: -L " puts a line
// that starts "fatal: -L " in that message. It is not a bad range: the
// missing path is not found, and the unreadable one is git's failure.
// The same name with a range past its end is a bad range.
func TestBlameRangeTextInAPathIsNotARangeError(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare")
	src := filepath.Join(t.TempDir(), "f")
	writeFile(t, filepath.Dir(src), "f", "l1\nl2\nl3\n", 0o644)
	blob := strings.TrimSpace(mustGit(t, dir, "hash-object", "-w", src))
	name := "x\nfatal: -L y"
	commit := blameCommitTree(t, dir, map[string]string{name: "100644 blob " + blob})
	ctx := context.Background()
	if _, err := Blame(ctx, dir, commit, "x\nfatal: -L y2", []string{"1,2"}, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing path with forged -L text: err = %v, want ErrNotFound", err)
	}
	if _, err := Blame(ctx, dir, commit, name, []string{"9,10"}, false); !errors.Is(err, ErrBadRange) {
		t.Errorf("range past the end: err = %v, want ErrBadRange", err)
	}
	if err := os.Remove(filepath.Join(dir, "objects", blob[:2], blob[2:])); err != nil {
		t.Fatal(err)
	}
	_, err := Blame(ctx, dir, commit, name, []string{"1,2"}, false)
	if err == nil || errors.Is(err, ErrBadRange) || errors.Is(err, ErrNotFound) {
		t.Errorf("unreadable file with forged -L text: err = %v, want git's failure", err)
	}
}

// failingBlameGit puts a git first on PATH that runs the real git, except
// that a call with any of the given arguments prints stderr and exits
// with code.
func failingBlameGit(t *testing.T, code int, stderr string, args ...string) {
	t.Helper()
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
	var match []string
	for _, a := range args {
		match = append(match, quote(a))
	}
	// printf and [ are builtins: some git children run without PATH.
	fakeGit(t, fmt.Sprintf("for a in \"$@\"; do for m in %s; do [ \"$a\" = \"$m\" ] && { printf '%%s' %s >&2; exit %d; }; done; done\nexec %s \"$@\"",
		strings.Join(match, " "), quote(stderr), code, quote(real)))
}

// Only git's exit 129, or exit 128 with a fixed head at the start of
// stderr, is a bad range; anything else ls-tree decides, and a.txt is a
// file there, so the failure is git's.
func TestBlameRangeFailureReadsOnlyGitsHead(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		name, stderr string
		code         int
		bad          bool
	}{
		{"-L head", "fatal: -L invalid line number: 0", 128, true},
		{"-L head, range text with a newline", "fatal: -L parameter 'a\nb' starting at line 1: No match", 128, true},
		{"usage", "usage: git blame", 129, true},
		{"short file", "fatal: file README.md has only 1 line", 128, true},
		{"short file, path with a newline", "fatal: file x\ny has only 2 lines", 128, true},
		{"-L line after another head", "fatal: no such path x\nfatal: -L y in 1111", 128, false},
		{"-L head with exit 130", "fatal: -L invalid line number: 0", 130, false},
		{"short file with exit 130", "fatal: file README.md has only 1 line", 130, false},
		{"short file without a number", "fatal: file README.md has only  line", 128, false},
		{"short-file tail after another head", "fatal: unable to read x has only 1 line", 128, false},
		{"short-file line after another line", "fatal: unable to read x\nfatal: file y has only 1 line", 128, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failingBlameGit(t, tc.code, tc.stderr, "blame")
			_, err := Blame(context.Background(), f.dir, f.c1, "README.md", []string{"1,2"}, false)
			if got := errors.Is(err, ErrBadRange); got != tc.bad || err == nil || errors.Is(err, ErrNotFound) {
				t.Errorf("err = %v, ErrBadRange = %v, want %v", err, got, tc.bad)
			}
		})
	}
}

// After a failed blame, ls-tree decides "not found": a symlink is a file,
// a submodule is not, and a failed ls-tree is git's failure, not a
// missing path.
func TestBlameNotFoundIsDecidedByLsTree(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "a.txt", "one\n", 0o644)
	if err := os.Symlink("a.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "c")
	head := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	mustGit(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+head+",sub")
	mustGit(t, dir, "commit", "-q", "-m", "submodule")
	commit := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	ctx := context.Background()

	if _, err := Blame(ctx, dir, commit, "sub", nil, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("blame of a submodule: err = %v, want ErrNotFound", err)
	}
	t.Run("symlink", func(t *testing.T) {
		failingBlameGit(t, 128, "fatal: unable to read 1111", "blame")
		if _, err := Blame(ctx, dir, commit, "link", nil, false); err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("failed blame of a symlink: err = %v, want git's failure", err)
		}
	})
	t.Run("second ls-tree fails", func(t *testing.T) {
		failingBlameGit(t, 128, "fatal: unable to read 1111", "blame", ":(literal)a.txt/")
		if _, err := Blame(ctx, dir, commit, "a.txt", nil, false); err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("failed blame and ls-tree of a.txt/: err = %v, want git's failure", err)
		}
	})
}
