package gitread

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coder/openstorage/internal/ref"
)

// BlameLine is one attributed line (docs/reference/http-api.md GET /repos/blame line objects).
type BlameLine struct {
	LineNumber         int    // 1-based line number in the file at the blamed commit
	CommitSHA          string // commit that introduced the line
	OriginalLineNumber int    // line number in CommitSHA's version of the file
	OriginalPath       string // path in CommitSHA (differs across renames)
	PreviousCommitSHA  string // CommitSHA's parent per blame ("" for root/boundary)
	AuthorName         string
	AuthorEmail        string
	AuthorTime         time.Time
	CommitterName      string
	CommitterEmail     string
	CommitterTime      time.Time
	Summary            string // commit subject line
}

// Blame attributes every line of path at commit (a resolved full SHA).
// ranges are raw `git blame -L` specs (docs/reference/http-api.md: repeatable `range` params
// like "1,10" — the handler validates their shape); detectMoves adds -M.
// A path absent at commit wraps ErrNotFound.
func Blame(ctx context.Context, repoPath, commit, path string, ranges []string, detectMoves bool) ([]BlameLine, error) {
	assertf(ref.ValidSHA(commit), "Blame: unresolved commit %q", commit)
	if path == "" {
		return nil, fmt.Errorf("blame empty path: %w", ErrNotFound)
	}
	args := []string{"blame", "--porcelain"}
	for _, r := range ranges {
		args = append(args, "-L", r)
	}
	if detectMoves {
		args = append(args, "-M")
	}
	args = append(args, commit, "--", path)
	out, err := runGit(ctx, Heavy, repoPath, args...)
	if err == nil {
		return parseBlamePorcelain(out)
	}
	if !errors.As(err, new(*GitError)) {
		return nil, err // a read limit or a cancelled request
	}
	// git's text names the path as git rewrote it (./x, a//b, control
	// bytes), so it never decides "not found": the range check reads only
	// the fixed parts of git's line, and ls-tree, which reads the path the
	// way blame does, decides the rest.
	if len(ranges) > 0 && blameRangeFailed(err) {
		return nil, fmt.Errorf("blame %q at %s: %w: %w", path, commit, ErrBadRange, err)
	}
	missing, lerr := blamePathMissing(ctx, repoPath, commit, path)
	if lerr != nil {
		return nil, fmt.Errorf("%w (and the path check failed: %w)", err, lerr)
	}
	if missing {
		return nil, fmt.Errorf("blame %q at %s: %w", path, commit, ErrNotFound)
	}
	return nil, err
}

// ErrBadRange: git blame refused a -L range of the request (bad syntax,
// past the end of the file, no match).
var ErrBadRange = errors.New("blame range does not fit the file")

// blameShortFile is the tail of git's "fatal: file <path> has only N
// lines" (or "1 line"), after the path.
var blameShortFile = regexp.MustCompile(` has only [0-9]+ lines?\z`)

// blameRangeFailed reports whether git blame failed on its -L ranges:
// git prints its usage (exit 129) for a range it cannot parse, and dies
// (exit 128) with "fatal: -L ..." or "fatal: file <path> has only N
// lines". That message is all git prints, and its fixed head comes
// before the path, so only a head at the start of stderr counts: a path
// that holds a newline and a forged head cannot make a missing or
// unreadable file ("fatal: no such path <path> in <commit>") read as a
// bad range.
func blameRangeFailed(err error) bool {
	if _, ok := gitStderr(err, 129); ok {
		return true
	}
	stderr, ok := gitStderr(err, 128)
	if !ok {
		return false
	}
	if strings.HasPrefix(stderr, "fatal: -L ") {
		return true
	}
	return strings.HasPrefix(stderr, "fatal: file ") && blameShortFile.MatchString(stderr)
}

// blamePathMissing reports whether path names no file at commit, read
// the way git blame reads it: ls-tree lists exactly one blob or symlink
// for a file, and nothing under path + "/" (a "dir/" query lists the
// folder's entries instead).
func blamePathMissing(ctx context.Context, repoPath, commit, path string) (bool, error) {
	entries, err := LsTree(ctx, repoPath, commit, path, false)
	if err != nil {
		return false, err
	}
	if len(entries) != 1 || (entries[0].Type != EntryBlob && entries[0].Type != EntrySymlink) {
		return true, nil
	}
	below, err := LsTree(ctx, repoPath, commit, path+"/", false)
	if err != nil {
		return false, err
	}
	return len(below) > 0, nil
}

// blameMeta is the per-commit metadata block of the porcelain stream, which
// git emits only on a commit's first appearance.
type blameMeta struct {
	authorName, authorEmail       string
	authorTime                    time.Time
	committerName, committerEmail string
	committerTime                 time.Time
	summary                       string
	previous                      string
	filename                      string
}

// parseBlamePorcelain parses `git blame --porcelain` output: for each line a
// header `<sha> <orig-line> <final-line> [<group-size>]`, then (first time
// per commit) metadata lines, then the TAB-prefixed content line.
func parseBlamePorcelain(out string) ([]BlameLine, error) {
	metas := map[string]*blameMeta{}
	lines := []BlameLine{}
	raw := strings.Split(out, "\n")
	for i := 0; i < len(raw); i++ {
		line := raw[i]
		if line == "" {
			continue // trailing newline
		}
		sha, origLine, finalLine, err := parseBlameHeader(line)
		if err != nil {
			return nil, err
		}
		meta := metas[sha]
		if meta == nil {
			meta = &blameMeta{}
			metas[sha] = meta
		}
		// Metadata (or none, on repeats) until the TAB-prefixed content line.
		for i++; i < len(raw); i++ {
			l := raw[i]
			if strings.HasPrefix(l, "\t") {
				break // content line: consumed, not surfaced (docs/reference/http-api.md has no text field)
			}
			key, value, _ := strings.Cut(l, " ")
			switch key {
			case "author":
				meta.authorName = value
			case "author-mail":
				meta.authorEmail = strings.Trim(value, "<>")
			case "author-time":
				if meta.authorTime, err = parseBlameTime(value, blameTZ(raw, i)); err != nil {
					return nil, err
				}
			case "committer":
				meta.committerName = value
			case "committer-mail":
				meta.committerEmail = strings.Trim(value, "<>")
			case "committer-time":
				if meta.committerTime, err = parseBlameTime(value, blameTZ(raw, i)); err != nil {
					return nil, err
				}
			case "summary":
				meta.summary = value
			case "previous":
				// "previous <sha> <path>"
				prevSHA, _, _ := strings.Cut(value, " ")
				if !ref.ValidSHA(prevSHA) {
					return nil, fmt.Errorf("blame emitted previous %q", value)
				}
				meta.previous = prevSHA
			case "filename":
				if meta.filename, err = unquoteGitPath(value); err != nil {
					return nil, err
				}
			default:
				// author-tz/committer-tz (read via blameTZ), boundary, and
				// any future keys are intentionally skipped.
			}
		}
		if meta.filename == "" {
			return nil, fmt.Errorf("blame group for %s has no filename", sha)
		}
		lines = append(lines, BlameLine{
			LineNumber:         finalLine,
			CommitSHA:          sha,
			OriginalLineNumber: origLine,
			OriginalPath:       meta.filename,
			PreviousCommitSHA:  meta.previous,
			AuthorName:         meta.authorName,
			AuthorEmail:        meta.authorEmail,
			AuthorTime:         meta.authorTime,
			CommitterName:      meta.committerName,
			CommitterEmail:     meta.committerEmail,
			CommitterTime:      meta.committerTime,
			Summary:            meta.summary,
		})
	}
	return lines, nil
}

// unquoteGitPath reverses git's C-style path quoting (quote_c_style in
// git's quote.c). git prints a name verbatim unless it holds a control
// character, `"` or `\`, or (with core.quotePath, the default) a byte of
// 0x80 or above. Then git prints it in double quotes, with \a \b \t \n \v
// \f \r \" \\ and three-digit octal escapes. Raw bytes inside the quotes
// are kept, so the result is the tree entry name with either
// core.quotePath setting (#590).
func unquoteGitPath(s string) (string, error) {
	if !strings.HasPrefix(s, `"`) {
		return s, nil
	}
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			if i != len(s)-1 {
				return "", fmt.Errorf("git quoted path %q has bytes after the closing quote", s)
			}
			return b.String(), nil
		}
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		if i++; i == len(s) {
			break
		}
		switch e := s[i]; e {
		case 'a':
			b.WriteByte('\a')
		case 'b':
			b.WriteByte('\b')
		case 't':
			b.WriteByte('\t')
		case 'n':
			b.WriteByte('\n')
		case 'v':
			b.WriteByte('\v')
		case 'f':
			b.WriteByte('\f')
		case 'r':
			b.WriteByte('\r')
		case '"', '\\':
			b.WriteByte(e)
		case '0', '1', '2', '3':
			if i+2 >= len(s) || !isOctal(s[i+1]) || !isOctal(s[i+2]) {
				return "", fmt.Errorf("git quoted path %q has a bad octal escape", s)
			}
			b.WriteByte((e-'0')<<6 | (s[i+1]-'0')<<3 | (s[i+2] - '0'))
			i += 2
		default:
			return "", fmt.Errorf("git quoted path %q has escape \\%c", s, e)
		}
	}
	return "", fmt.Errorf("git quoted path %q has no closing quote", s)
}

func isOctal(c byte) bool { return '0' <= c && c <= '7' }

// parseBlameHeader parses `<sha> <orig-line> <final-line> [<group-size>]`.
func parseBlameHeader(line string) (sha string, origLine, finalLine int, err error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || len(fields) > 4 || !ref.ValidSHA(fields[0]) {
		return "", 0, 0, fmt.Errorf("blame emitted header %q", line)
	}
	orig, err1 := strconv.Atoi(fields[1])
	final, err2 := strconv.Atoi(fields[2])
	if err1 != nil || err2 != nil || orig < 1 || final < 1 {
		return "", 0, 0, fmt.Errorf("blame emitted header %q", line)
	}
	return fields[0], orig, final, nil
}

// blameTZ looks ahead one line for the matching author-tz/committer-tz
// offset ("+0100"); porcelain always emits it directly after the time.
func blameTZ(raw []string, timeIdx int) string {
	if timeIdx+1 < len(raw) {
		key, value, _ := strings.Cut(raw[timeIdx+1], " ")
		if key == "author-tz" || key == "committer-tz" {
			return value
		}
	}
	return "+0000"
}

// parseBlameTime combines porcelain unix seconds with a ±HHMM offset. An
// offset or local time that RFC 3339 cannot write (+9999, year 10000)
// reads as UTC (#581).
func parseBlameTime(sec, tz string) (time.Time, error) {
	t, err := identTime(sec)
	if err != nil {
		return time.Time{}, fmt.Errorf("blame time: %w", err)
	}
	loc := time.UTC
	if len(tz) == 5 && (tz[0] == '+' || tz[0] == '-') {
		hh, errH := strconv.Atoi(tz[1:3])
		mm, errM := strconv.Atoi(tz[3:5])
		if errH == nil && errM == nil && hh < 24 && mm < 60 {
			offset := hh*3600 + mm*60
			if tz[0] == '-' {
				offset = -offset
			}
			loc = time.FixedZone(tz, offset)
		}
	}
	if t.In(loc).Year() > 9999 {
		loc = time.UTC
	}
	return t.In(loc), nil
}
