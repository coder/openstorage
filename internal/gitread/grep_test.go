package gitread

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func grepByPath(files []GrepFile) map[string]GrepFile {
	m := make(map[string]GrepFile, len(files))
	for _, f := range files {
		m[f.Path] = f
	}
	return m
}

func TestGrepBasic(t *testing.T) {
	f := newFixture(t)
	files, err := Grep(context.Background(), f.dir, f.c2, GrepOptions{Pattern: "line two"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "README.md" {
		t.Fatalf("grep files = %+v", files)
	}
	lines := files[0].Lines
	if len(lines) != 1 || lines[0].LineNumber != 2 || lines[0].Type != GrepLineMatch || lines[0].Text != "line two" {
		t.Fatalf("grep lines = %+v", lines)
	}
}

func TestGrepContextTyping(t *testing.T) {
	f := newFixture(t)
	files, err := Grep(context.Background(), f.dir, f.c2, GrepOptions{
		Pattern:       "line three",
		BeforeContext: 1,
		AfterContext:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	m := grepByPath(files)
	lines := m["README.md"].Lines
	if len(lines) != 3 {
		t.Fatalf("grep context lines = %+v", lines)
	}
	wantTypes := []struct {
		n   int
		typ string
	}{{2, GrepLineContext}, {3, GrepLineMatch}, {4, GrepLineContext}}
	for i, w := range wantTypes {
		if lines[i].LineNumber != w.n || lines[i].Type != w.typ {
			t.Errorf("line[%d] = %+v, want number %d type %s", i, lines[i], w.n, w.typ)
		}
	}
}

func TestGrepCaseAndPathspecs(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// Case-sensitive by default: "LINE TWO" misses.
	files, err := Grep(ctx, f.dir, f.c2, GrepOptions{Pattern: "LINE TWO"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("case-sensitive grep matched %+v", files)
	}
	// -i matches.
	files, err = Grep(ctx, f.dir, f.c2, GrepOptions{Pattern: "LINE TWO", CaseInsensitive: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("case-insensitive grep = %+v", files)
	}

	// "data" appears only in dir/renamed.txt at c2; exclude pathspec hides it.
	files, err = Grep(ctx, f.dir, f.c2, GrepOptions{
		Pattern:   "data",
		Pathspecs: []string{":(glob)dir/**"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "dir/renamed.txt" {
		t.Fatalf("pathspec grep = %+v", files)
	}
	files, err = Grep(ctx, f.dir, f.c2, GrepOptions{
		Pattern:   "data",
		Pathspecs: []string{":(exclude)dir/"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("exclude pathspec grep = %+v", files)
	}

	// :(literal) pathspecs (the /repos/grep `paths` mapping): directory
	// prefixes narrow the search, wildcards stay literal characters, and a
	// pathspec matching nothing is an empty result — not a git fatal.
	files, err = Grep(ctx, f.dir, f.c2, GrepOptions{
		Pattern:   "data",
		Pathspecs: []string{":(literal)dir"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "dir/renamed.txt" {
		t.Fatalf("literal dir pathspec grep = %+v", files)
	}
	for _, spec := range []string{":(literal)dir/*", ":(literal)no-such-path"} {
		files, err = Grep(ctx, f.dir, f.c2, GrepOptions{Pattern: "data", Pathspecs: []string{spec}})
		if err != nil {
			t.Fatalf("pathspec %q: %v", spec, err)
		}
		if len(files) != 0 {
			t.Fatalf("pathspec %q grep = %+v, want empty", spec, files)
		}
	}
}

// TestGrepBadPattern pins the exit-128 contract the handler maps to a 400:
// an invalid regex surfaces as an *exec.ExitError with code 128.
func TestGrepBadPattern(t *testing.T) {
	f := newFixture(t)
	_, err := Grep(context.Background(), f.dir, f.c2, GrepOptions{Pattern: "["})
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 128 {
		t.Fatalf("bad-pattern grep err = %v, want exit status 128", err)
	}
}

func TestGrepNoMatchesAndBinarySkipped(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	files, err := Grep(ctx, f.dir, f.c2, GrepOptions{Pattern: "zzz-no-such-string"})
	if err != nil {
		t.Fatal(err)
	}
	if files == nil || len(files) != 0 {
		t.Fatalf("no-match grep = %#v, want empty non-nil", files)
	}

	// "binary" occurs only inside bin.dat, which -I skips.
	files, err = Grep(ctx, f.dir, f.c2, GrepOptions{Pattern: "binary"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("binary grep = %+v, want skipped", files)
	}
}

// #339 pins only git's messages to the C locale: a UTF-8 server locale
// must still reach git grep's regex matching (LC_ALL=C would break -i on
// non-ASCII text and "." over multibyte characters).
func TestGrepKeepsServerCharacterLocale(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "f.txt", "CAFÉ au lait\nnaïve\n", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "utf-8")
	commit := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	t.Setenv("LC_ALL", "C.UTF-8")
	probe := exec.Command("git", "grep", "-q", "-i", "café", commit)
	probe.Dir = dir
	if probe.Run() != nil {
		t.Skip("this git has no C.UTF-8 regex locale")
	}
	for _, opts := range []GrepOptions{{Pattern: "café", CaseInsensitive: true}, {Pattern: "na.ve"}} {
		files, err := Grep(context.Background(), dir, commit, opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 1 || len(files[0].Lines) != 1 {
			t.Errorf("grep %+v under LC_ALL=C.UTF-8 = %+v, want one match", opts, files)
		}
	}
}
