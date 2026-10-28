package gitread

import (
	"fmt"
	"strings"
	"testing"
)

// TestGrepReadsPathsWithNewlines pins #583: a tree entry name may hold a
// newline (`git fsck --strict` accepts it), and git grep --null prints it
// raw. A line's text may hold a NUL byte when git reads the file as text
// (git's binary check reads only the first 8000 bytes).
func TestGrepReadsPathsWithNewlines(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "a\nb", "needle one\n", 0o644)
	writeFile(t, dir, "--\nz", "x\nneedle two\ny", 0o644) // no final newline
	writeFile(t, dir, "c.txt", "needle three\n", 0o644)
	writeFile(t, dir, "e\n", "needle four\n", 0o644)
	writeFile(t, dir, "nul.txt", strings.Repeat("a", 9000)+"\nneedle\x00five\n", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "newline paths")
	commit := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))

	type line struct {
		n          int
		text, kind string
	}
	for _, tc := range []struct {
		name string
		opts GrepOptions
		want map[string][]line
	}{
		{"matches", GrepOptions{Pattern: "needle"}, map[string][]line{
			"--\nz":   {{2, "needle two", GrepLineMatch}},
			"a\nb":    {{1, "needle one", GrepLineMatch}},
			"c.txt":   {{1, "needle three", GrepLineMatch}},
			"e\n":     {{1, "needle four", GrepLineMatch}},
			"nul.txt": {{2, "needle\x00five", GrepLineMatch}},
		}},
		{"context", GrepOptions{Pattern: "two", BeforeContext: 1, AfterContext: 1}, map[string][]line{
			"--\nz": {{1, "x", GrepLineContext}, {2, "needle two", GrepLineMatch}, {3, "y", GrepLineContext}},
		}},
	} {
		files, err := Grep(t.Context(), dir, commit, tc.opts)
		if err != nil {
			t.Fatalf("%s: Grep: %v", tc.name, err)
		}
		got := map[string][]line{}
		for _, f := range files {
			for _, l := range f.Lines {
				got[f.Path] = append(got[f.Path], line{l.LineNumber, l.Text, l.Type})
			}
		}
		if fmt.Sprintf("%#v", got) != fmt.Sprintf("%#v", tc.want) {
			t.Errorf("%s:\n got %#v\nwant %#v", tc.name, got, tc.want)
		}
	}
}

// TestParseGrepOutputRejectsMisframedOutput checks that a record cut
// short fails instead of being read as another file or line.
func TestParseGrepOutputRejectsMisframedOutput(t *testing.T) {
	c := strings.Repeat("c", 40)
	rec := c + ":p\x001\x00text\n"
	for _, tc := range []struct {
		name, out string
		lines     int // -1: error
	}{
		{"empty", "", 0},
		{"one", rec, 1},
		{"separator between", rec + "--\n" + rec, 2},
		{"newline in path", c + ":a\nb\x001\x00t\n", 1},
		{"no final newline", strings.TrimSuffix(rec, "\n"), -1},
		{"no line number NUL", c + ":p\x001\n", -1},
		{"no path NUL", c + ":p\n", -1},
		{"line number not a number", c + ":p\x00x\x00t\n", -1},
		{"line number 0", c + ":p\x000\x00t\n", -1},
		{"other tree", strings.Repeat("d", 40) + ":p\x001\x00t\n", -1},
		{"empty path", c + ":\x001\x00t\n", -1},
		{"blank line", rec + "\n" + rec, -1},
	} {
		files, err := parseGrepOutput(tc.out, c)
		n := 0
		for _, f := range files {
			n += len(f.Lines)
		}
		switch {
		case tc.lines < 0 && err == nil:
			t.Errorf("%s: %d lines, want an error", tc.name, n)
		case tc.lines >= 0 && (err != nil || n != tc.lines):
			t.Errorf("%s: %d lines, %v; want %d", tc.name, n, err, tc.lines)
		}
	}
}
