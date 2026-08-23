package gitread

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/coder/code-storage/internal/ref"
)

// Grep line types (docs/reference/http-api.md POST /repos/grep lines[].type).
const (
	GrepLineMatch   = "match"
	GrepLineContext = "context"
)

// GrepLine is one output line: a pattern match or surrounding context.
type GrepLine struct {
	LineNumber int
	Text       string
	Type       string // GrepLineMatch | GrepLineContext
}

// GrepFile groups all matched/context lines of one file, ascending by line
// number (docs/reference/http-api.md matches[] entries).
type GrepFile struct {
	Path  string
	Lines []GrepLine
}

// GrepOptions tune one Grep invocation. Pattern is required (POSIX basic
// regex, git grep's default dialect).
type GrepOptions struct {
	Pattern         string
	CaseInsensitive bool     // -i
	Pathspecs       []string // git pathspecs incl. :(glob)/:(exclude) magic; empty = whole tree
	BeforeContext   int      // -B
	AfterContext    int      // -A
}

// Grep searches the tree at commit (a resolved full SHA). Binary blobs are
// skipped (-I). No matches is a successful empty result, not an error.
// Result limiting/pagination (docs/reference/http-api.md limits, incomplete_results) is the
// endpoint slice's job on top of this full result.
//
// Match/context typing: `--null` (needed for unambiguous parsing of paths
// containing ':' or '-') erases git's ':' vs '-' delimiter distinction, so
// with context enabled Grep runs a second match-only pass and classifies
// every line by (path, line) membership in that match set.
func Grep(ctx context.Context, repoPath, commit string, opts GrepOptions) ([]GrepFile, error) {
	assertf(ref.ValidSHA(commit), "Grep: unresolved commit %q", commit)
	if opts.Pattern == "" {
		return nil, errors.New("grep: empty pattern")
	}
	assertf(opts.BeforeContext >= 0 && opts.AfterContext >= 0, "Grep: negative context %+v", opts)

	contextArgs := []string{}
	if opts.BeforeContext > 0 {
		contextArgs = append(contextArgs, "-B", strconv.Itoa(opts.BeforeContext))
	}
	if opts.AfterContext > 0 {
		contextArgs = append(contextArgs, "-A", strconv.Itoa(opts.AfterContext))
	}

	files, err := runGrep(ctx, repoPath, commit, opts, contextArgs)
	if err != nil {
		return nil, err
	}
	if len(contextArgs) == 0 || len(files) == 0 {
		return files, nil // every line is a match
	}

	matchFiles, err := runGrep(ctx, repoPath, commit, opts, nil)
	if err != nil {
		return nil, err
	}
	matched := map[string]map[int]bool{}
	for _, mf := range matchFiles {
		set := map[int]bool{}
		for _, l := range mf.Lines {
			set[l.LineNumber] = true
		}
		matched[mf.Path] = set
	}
	for fi := range files {
		set := matched[files[fi].Path]
		for li := range files[fi].Lines {
			if !set[files[fi].Lines[li].LineNumber] {
				files[fi].Lines[li].Type = GrepLineContext
			}
		}
	}
	return files, nil
}

// runGrep executes one git grep pass and parses it; every line is typed as
// a match (the caller downgrades context lines).
func runGrep(ctx context.Context, repoPath, commit string, opts GrepOptions, extraArgs []string) ([]GrepFile, error) {
	args := []string{"grep", "-I", "-n", "--null"}
	if opts.CaseInsensitive {
		args = append(args, "-i")
	}
	args = append(args, extraArgs...)
	args = append(args, "-e", opts.Pattern, commit)
	if len(opts.Pathspecs) > 0 {
		args = append(append(args, "--"), opts.Pathspecs...)
	}
	out, err := runGit(ctx, repoPath, args...)
	if err != nil {
		// Exit status 1 with empty output is git grep's "no matches".
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && strings.TrimSpace(out) == "" {
			return []GrepFile{}, nil
		}
		return nil, err
	}
	return parseGrepOutput(out, commit)
}

// parseGrepOutput parses `git grep -n --null <tree>` lines:
//
//	<tree>:<path> \0 <lineno> \0 <text>
//	--                                     (hunk separator)
func parseGrepOutput(out, commit string) ([]GrepFile, error) {
	files := []GrepFile{}
	index := map[string]int{} // path -> files idx
	for _, line := range strings.Split(out, "\n") {
		if line == "" || line == "--" {
			continue
		}
		name, rest, found := strings.Cut(line, "\x00")
		if !found {
			return nil, fmt.Errorf("grep emitted %q", line)
		}
		path, ok := strings.CutPrefix(name, commit+":")
		if !ok || path == "" {
			return nil, fmt.Errorf("grep emitted file name %q", name)
		}
		lineno, text, found := strings.Cut(rest, "\x00")
		if !found {
			return nil, fmt.Errorf("grep emitted %q", line)
		}
		n, err := strconv.Atoi(lineno)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("grep emitted line number %q", lineno)
		}
		idx, seen := index[path]
		if !seen {
			idx = len(files)
			index[path] = idx
			files = append(files, GrepFile{Path: path})
		}
		files[idx].Lines = append(files[idx].Lines, GrepLine{LineNumber: n, Text: text, Type: GrepLineMatch})
	}
	return files, nil
}
