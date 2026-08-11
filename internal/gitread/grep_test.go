package gitread

import (
	"context"
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
