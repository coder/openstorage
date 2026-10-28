package gitread

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coder/openstorage/internal/ref"
)

// Commit is one parsed commit — the docs/reference/http-api.md CommitInfo source data
// ({sha, parent_shas[], message, author_*, committer_*, date}).
type Commit struct {
	SHA            string
	TreeSHA        string
	ParentSHAs     []string // empty (never nil) for root commits — invariant 2
	AuthorName     string
	AuthorEmail    string
	AuthorDate     time.Time
	CommitterName  string
	CommitterEmail string
	CommitterDate  time.Time
	Message        string // full raw message (%B): subject + body, usually newline-terminated; "" under LogOptions.SubjectOnly
	Subject        string // subject line (%s), set only under LogOptions.SubjectOnly
}

// LogOptions tune one Log invocation. The zero value lists the full history.
type LogOptions struct {
	Path     string // non-empty limits to commits touching Path (`-- :(literal)<path>`, no --follow)
	Skip     int    // --skip offset (tip-anchored cursor pagination)
	MaxCount int    // >0 caps the returned commits (-n); 0 = unlimited
	// SubjectOnly renders the subject line (%s: the first paragraph as one
	// line) into Commit.Subject instead of the full message, so git prints
	// no body.
	SubjectOnly bool
}

// logFormat renders one record as logFields NUL-terminated fields: the
// fields are NUL-separated, and -z ends each record with a NUL. git prints
// no NUL inside a field (it stops a message at its first NUL byte), while
// names, emails and messages may hold any other byte, 0x1f included (#580).
// %aI/%cI are strict ISO 8601 (RFC 3339). Log swaps the final %B for %s
// under LogOptions.SubjectOnly.
const logFormat = "%H%x00%T%x00%P%x00%an%x00%ae%x00%aI%x00%cn%x00%ce%x00%cI%x00%B"

// logFields is the number of fields in one logFormat record.
const logFields = 10

// logSubjectFormat is logFormat with %s (the subject line) in place of the
// final %B.
var logSubjectFormat = strings.TrimSuffix(logFormat, "%B") + "%s"

// Log lists commits reachable from rev (a resolved full SHA), newest first
// in commit-graph order (git log default — invariant 9).
func Log(ctx context.Context, repoPath, rev string, opts LogOptions) ([]Commit, error) {
	assertf(ref.ValidSHA(rev), "Log: unresolved rev %q", rev)
	assertf(opts.Skip >= 0 && opts.MaxCount >= 0, "Log: negative Skip/MaxCount %+v", opts)
	format, parse := logFormat, parseLogRecord
	if opts.SubjectOnly {
		format, parse = logSubjectFormat, parseSubjectRecord
	}
	args := []string{"log", "-z", "--format=" + format}
	if opts.Skip > 0 {
		args = append(args, "--skip="+strconv.Itoa(opts.Skip))
	}
	if opts.MaxCount > 0 {
		args = append(args, "-n", strconv.Itoa(opts.MaxCount))
	}
	args = append(args, rev)
	if opts.Path != "" {
		args = append(args, "--", ":(literal)"+opts.Path)
	}
	// One commit with no path filter or skip is a single-object read;
	// anything else walks history.
	class := Heavy
	if opts.Path == "" && opts.Skip == 0 && opts.MaxCount == 1 {
		class = Light
	}
	out, err := runGit(ctx, class, repoPath, args...)
	if err != nil {
		return nil, err
	}
	recs, err := logRecords(out)
	if err != nil {
		return nil, err
	}
	commits := make([]Commit, 0, len(recs))
	for _, rec := range recs {
		c, err := parse(rec)
		if err != nil {
			return nil, err
		}
		commits = append(commits, c)
	}
	return commits, nil
}

// logRecords splits `git log -z` output of logFormat or logSubjectFormat
// into records of logFields fields each.
func logRecords(out string) ([][]string, error) {
	if out == "" {
		return nil, nil
	}
	body, ok := strings.CutSuffix(out, "\x00")
	if !ok {
		return nil, fmt.Errorf("log output does not end with a NUL: %.80q", out[max(0, len(out)-80):])
	}
	fields := strings.Split(body, "\x00")
	if len(fields)%logFields != 0 {
		return nil, fmt.Errorf("log emitted %d fields, not a multiple of %d", len(fields), logFields)
	}
	return slices.Collect(slices.Chunk(fields, logFields)), nil
}

// CommitsBySHA loads the given commits (resolved full SHAs, duplicates
// allowed) with one `git log --no-walk --stdin` child, keyed by SHA, with
// Subject set and Message empty (as LogOptions.SubjectOnly). It is Heavy:
// the output grows with the number of commits.
func CommitsBySHA(ctx context.Context, repoPath string, shas []string) (map[string]Commit, error) {
	out := make(map[string]Commit, len(shas))
	if len(shas) == 0 {
		return out, nil
	}
	var in strings.Builder
	for _, sha := range shas {
		assertf(ref.ValidSHA(sha), "CommitsBySHA: unresolved sha %q", sha)
		in.WriteString(sha + "\n")
	}
	stdout, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdin: strings.NewReader(in.String()),
		Args: []string{"log", "--no-walk", "--stdin", "-z", "--format=" + logSubjectFormat}})
	if err != nil {
		return nil, err
	}
	recs, err := logRecords(stdout)
	if err != nil {
		return nil, err
	}
	for _, rec := range recs {
		c, err := parseSubjectRecord(rec)
		if err != nil {
			return nil, err
		}
		out[c.SHA] = c
	}
	for _, sha := range shas {
		if _, ok := out[sha]; !ok {
			return nil, fmt.Errorf("log --no-walk did not print commit %s", sha)
		}
	}
	return out, nil
}

// parseSubjectRecord parses one logSubjectFormat record: the last field is
// the subject line, so it fills Subject and leaves Message empty.
func parseSubjectRecord(rec []string) (Commit, error) {
	c, err := parseLogRecord(rec)
	if err != nil {
		return Commit{}, err
	}
	if strings.Contains(c.Message, "\n") {
		return Commit{}, fmt.Errorf("log emitted multi-line subject for %s: %.80q", c.SHA, c.Message)
	}
	c.Subject, c.Message = c.Message, ""
	return c, nil
}

// parseLogRecord parses the fields of one logFormat record.
func parseLogRecord(fields []string) (Commit, error) {
	assertf(len(fields) == logFields, "parseLogRecord: %d fields", len(fields))
	c := Commit{
		SHA:            fields[0],
		TreeSHA:        fields[1],
		ParentSHAs:     []string{},
		AuthorName:     fields[3],
		AuthorEmail:    fields[4],
		CommitterName:  fields[6],
		CommitterEmail: fields[7],
		Message:        fields[9],
	}
	if !ref.ValidSHA(c.SHA) || !ref.ValidSHA(c.TreeSHA) {
		return Commit{}, fmt.Errorf("log emitted malformed sha in %.80q", fields[:2])
	}
	for p := range strings.FieldsSeq(fields[2]) {
		if !ref.ValidSHA(p) {
			return Commit{}, fmt.Errorf("log emitted malformed parent %q", p)
		}
		c.ParentSHAs = append(c.ParentSHAs, p)
	}
	var err error
	if c.AuthorDate, err = time.Parse(time.RFC3339, fields[5]); err != nil {
		return Commit{}, fmt.Errorf("log emitted author date %q: %w", fields[5], err)
	}
	if c.CommitterDate, err = time.Parse(time.RFC3339, fields[8]); err != nil {
		return Commit{}, fmt.Errorf("log emitted committer date %q: %w", fields[8], err)
	}
	return c, nil
}

// CommitDetail is GET /repos/commit's payload: the commit plus, for signed
// commits only, the detached signature and the exact payload bytes it signs.
type CommitDetail struct {
	Commit
	Signature string // armored PGP/SSH signature; "" when unsigned
	Payload   string // exact signed bytes (raw commit minus the gpgsig header); "" when unsigned
}

// GetCommit loads one commit (a resolved full SHA) with signature extraction.
func GetCommit(ctx context.Context, repoPath, sha string) (CommitDetail, error) {
	assertf(ref.ValidSHA(sha), "GetCommit: unresolved sha %q", sha)
	commits, err := Log(ctx, repoPath, sha, LogOptions{MaxCount: 1})
	if err != nil {
		return CommitDetail{}, err
	}
	if len(commits) != 1 {
		return CommitDetail{}, fmt.Errorf("log -n 1 %s returned %d commits", sha, len(commits))
	}
	detail := CommitDetail{Commit: commits[0]}
	raw, err := runGit(ctx, Light, repoPath, "cat-file", "commit", sha)
	if err != nil {
		return CommitDetail{}, err
	}
	detail.Signature, detail.Payload = splitCommitSignature(raw)
	return detail, nil
}

// splitCommitSignature extracts the gpgsig (or gpgsig-sha256) header from a
// raw commit object. The signed payload is the object with that header block
// removed — byte-exact, so `gpg/ssh-keygen --verify` accepts the pair.
// Unsigned commits return ("", "").
func splitCommitSignature(raw string) (signature, payload string) {
	headers, body, found := strings.Cut(raw, "\n\n")
	if !found {
		return "", ""
	}
	var sigLines, keptLines []string
	inSig := false
	for line := range strings.SplitSeq(headers, "\n") {
		switch {
		case strings.HasPrefix(line, "gpgsig ") || strings.HasPrefix(line, "gpgsig-sha256 "):
			inSig = true
			_, v, _ := strings.Cut(line, " ")
			sigLines = append(sigLines, v)
		case inSig && strings.HasPrefix(line, " "):
			// Header continuation: one leading space per line.
			sigLines = append(sigLines, line[1:])
		default:
			inSig = false
			keptLines = append(keptLines, line)
		}
	}
	if len(sigLines) == 0 {
		return "", ""
	}
	return strings.Join(sigLines, "\n"), strings.Join(keptLines, "\n") + "\n\n" + body
}
