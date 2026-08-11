package gitread

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"codestorage/internal/ref"
)

// Commit is one parsed commit — the docs/03 CommitInfo source data
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
	Message        string // full raw message (%B): subject + body, usually newline-terminated
}

// LogOptions tune one Log invocation. The zero value lists the full history.
type LogOptions struct {
	Path     string // non-empty limits to commits touching Path (`-- <path>`, no --follow)
	Skip     int    // --skip offset (tip-anchored cursor pagination)
	MaxCount int    // >0 caps the returned commits (-n); 0 = unlimited
}

// logFormat renders one record as 10 \x1f-separated fields; records are
// \x00-separated via -z. %aI/%cI are strict ISO 8601 (RFC 3339).
const logFormat = "%H%x1f%T%x1f%P%x1f%an%x1f%ae%x1f%aI%x1f%cn%x1f%ce%x1f%cI%x1f%B"

// Log lists commits reachable from rev (a resolved full SHA), newest first
// in commit-graph order (git log default — invariant 9).
func Log(ctx context.Context, repoPath, rev string, opts LogOptions) ([]Commit, error) {
	assertf(ref.ValidSHA(rev), "Log: unresolved rev %q", rev)
	assertf(opts.Skip >= 0 && opts.MaxCount >= 0, "Log: negative Skip/MaxCount %+v", opts)
	args := []string{"log", "-z", "--format=" + logFormat}
	if opts.Skip > 0 {
		args = append(args, "--skip="+strconv.Itoa(opts.Skip))
	}
	if opts.MaxCount > 0 {
		args = append(args, "-n", strconv.Itoa(opts.MaxCount))
	}
	args = append(args, rev)
	if opts.Path != "" {
		args = append(args, "--", opts.Path)
	}
	out, err := runGit(ctx, repoPath, args...)
	if err != nil {
		return nil, err
	}
	commits := []Commit{}
	for _, rec := range strings.Split(out, "\x00") {
		if rec == "" {
			continue // trailing record separator
		}
		c, err := parseLogRecord(rec)
		if err != nil {
			return nil, err
		}
		commits = append(commits, c)
	}
	return commits, nil
}

// parseLogRecord parses one \x1f-separated logFormat record.
func parseLogRecord(rec string) (Commit, error) {
	fields := strings.Split(rec, "\x1f")
	if len(fields) != 10 {
		return Commit{}, fmt.Errorf("log emitted %d fields in %.80q", len(fields), rec)
	}
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
		return Commit{}, fmt.Errorf("log emitted malformed sha in %.80q", rec)
	}
	for _, p := range strings.Fields(fields[2]) {
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
	raw, err := runGit(ctx, repoPath, "cat-file", "commit", sha)
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
	for _, line := range strings.Split(headers, "\n") {
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
