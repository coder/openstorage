package gitread

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/coder/openstorage/internal/ref"
)

// maxParentsLine bounds one `rev-list --parents` line, which LastCommits
// reads twice: about 25,000 parents. A commit with more (git allows any
// number, also of duplicates) fails with ErrOutputLimit instead of costing
// the server memory per parent.
const maxParentsLine = 1 << 20

// maxSpoolBytes bounds the spooled rev-list (about 3 million single-parent
// commits); a longer history fails with ErrOutputLimit.
const maxSpoolBytes = 256 << 20

// spoolWriter writes rev-list output to the spool. On a line longer than
// maxParentsLine or a spool larger than maxSpoolBytes it stops git (ErrStop)
// and keeps ErrOutputLimit in err.
type spoolWriter struct {
	f    io.Writer
	n    int64
	line int // bytes of the current, unterminated line
	err  error
}

func (s *spoolWriter) Write(p []byte) (int, error) {
	s.n += int64(len(p))
	for rest := p; ; {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			s.line += len(rest)
			break
		}
		s.line += i
		if s.line > maxParentsLine {
			break
		}
		s.line, rest = 0, rest[i+1:]
	}
	switch {
	case s.line > maxParentsLine:
		s.err = fmt.Errorf("rev-list line over %d bytes: %w", maxParentsLine, ErrOutputLimit)
	case s.n > maxSpoolBytes:
		s.err = fmt.Errorf("rev-list output over %d bytes: %w", maxSpoolBytes, ErrOutputLimit)
	}
	if s.err != nil {
		return 0, ErrStop
	}
	return s.f.Write(p)
}

// readRevListLine reads one spooled rev-list line of at most
// maxParentsLine bytes plus its newline from br, which must buffer that
// many.
func readRevListLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", fmt.Errorf("rev-list line over %d bytes: %w", maxParentsLine, ErrOutputLimit)
	}
	return string(line), err
}

// ErrTooManyLastCommits reports that the files passed to LastCommits have
// more distinct last commits than its limit. It is not a budget error: the
// same request fails the same way every time.
var ErrTooManyLastCommits = errors.New("too many distinct last commits")

// LastCommits returns, for every path in paths (entries of rev's tree), the
// commit that `git log -1 rev -- <path>` names, with literal pathspecs, in
// the order of paths (a slice, not a map: a listing can hold hundreds of
// thousands of paths). One
// difference: a submodule's `ignore` setting in .gitmodules never hides a
// gitlink change. It fails with ErrTooManyLastCommits as soon as more than
// maxDistinct distinct commits are found.
//
// One walk replaces a `git log -1` per path. `rev-list --topo-order
// --parents rev` is spooled to a temporary file. Then `diff-tree --stdin`
// diffs each commit against each parent, in that order. Each path waits at
// one commit, starting at rev, and moves the way git's default history
// simplification moves for that path alone. A root commit, or a commit
// whose diff against every parent changes the path, is the answer. A
// commit whose diff against a parent leaves the path unchanged hands it to
// the first such parent. Topological order puts every commit before its
// parents, so a commit has collected every path waiting on it before its
// diffs arrive. The walk stops once every path has its commit.
//
// Both children are Heavy and run one after the other under the request's
// budget (see Run).
func LastCommits(ctx context.Context, repoPath, rev string, paths []string, maxDistinct int) ([]string, error) {
	assertf(ref.ValidSHA(rev), "LastCommits: unresolved rev %q", rev)
	assertf(maxDistinct > 0, "LastCommits: maxDistinct %d", maxDistinct)
	if len(paths) == 0 {
		return []string{}, nil
	}
	assertf(repoPath != "", "LastCommits: empty repoPath")
	w := newLastCommitWalk(rev, paths, maxDistinct)

	spool, err := os.CreateTemp("", "openstorage-lastcommits-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spool.Name())
	}()
	sw := &spoolWriter{f: spool}
	if _, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdout: sw, Args: []string{"rev-list", "--topo-order", "--parents", rev}}); err != nil {
		return nil, err
	}
	if sw.err != nil {
		return nil, sw.err
	}
	expect, err := os.Open(spool.Name())
	if err != nil {
		return nil, err
	}
	defer func() { _ = expect.Close() }()
	w.expect = bufio.NewReaderSize(expect, maxParentsLine+1)
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// diff-tree reads one "<commit> <parent>" line per pair (a root: the
	// commit alone, diffed against the empty tree by --root) and, with
	// --always, prints one header per line even when the diff is empty.
	// The feeder and the walk read the same spool, so the walk knows which
	// pair each header belongs to.
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	fed := make(chan error, 1)
	go func() {
		fed <- feedPairs(stdinW, spool)
		_ = stdinW.Close()
	}()
	args := []string{"diff-tree", "--stdin", "--root", "--always", "-r", "--raw", "-z", "--no-renames", "--ignore-submodules=none"}
	if spec := commonPathspec(paths); spec != "" {
		args = append(args, "--", ":(literal)"+spec)
	}
	_, err = Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdin: stdinR, Stdout: w, Args: args})
	// git is reaped: closing the read end fails the feeder's next write.
	_ = stdinR.Close()
	feedErr := <-fed
	switch {
	case w.err != nil:
		return nil, w.err
	case err != nil:
		return nil, err
	case w.left > 0:
		if feedErr != nil {
			return nil, feedErr
		}
		// git ended on its own: the last commit's diffs are complete.
		if err := w.finish(); err != nil && !errors.Is(err, ErrStop) {
			return nil, err
		}
	}
	assertf(w.left == 0, "LastCommits: %d paths unresolved after the walk", w.left)
	return w.last, nil
}

// feedPairs writes diff-tree's input lines for every rev-list line.
func feedPairs(dst io.Writer, spool io.Reader) error {
	bw := bufio.NewWriter(dst)
	br := bufio.NewReaderSize(spool, maxParentsLine+1)
	for {
		line, err := readRevListLine(br)
		if err == io.EOF && line == "" {
			break
		} else if err != nil && err != io.EOF {
			return err
		}
		f := strings.Fields(line)
		if len(f) == 1 {
			if _, err := bw.WriteString(f[0] + "\n"); err != nil {
				return err
			}
		}
		for _, p := range f[1:] {
			if _, err := bw.WriteString(f[0] + " " + p + "\n"); err != nil {
				return err
			}
		}
	}
	return bw.Flush()
}

// commonPathspec returns the deepest directory that contains every path
// (the path itself when there is one), or "" for the whole tree. It only
// narrows diff-tree's work: the walk still matches paths exactly.
func commonPathspec(paths []string) string {
	lo, hi := paths[0], paths[0]
	for _, p := range paths[1:] {
		lo, hi = min(lo, p), max(hi, p)
	}
	if lo == hi {
		return lo
	}
	// Every path shares the common prefix of the smallest and largest.
	n := 0
	for n < len(lo) && n < len(hi) && lo[n] == hi[n] {
		n++
	}
	i := strings.LastIndexByte(lo[:n], '/')
	if i < 0 {
		return ""
	}
	return lo[:i]
}

// lastCommitWalk consumes diff-tree's -z raw output (an io.Writer for Run).
type lastCommitWalk struct {
	rev     string
	paths   []string
	index   map[string]int32 // path -> index into paths
	last    []string         // the answer per path; "" while unresolved
	waiting map[string]pathSet
	left    int
	commits map[string]struct{} // distinct answers
	max     int
	expect  *bufio.Reader // the spooled rev-list output

	// The commit whose diffs are arriving, one section per parent.
	cur      string
	curSet   pathSet // paths waiting on cur that every diff so far changed
	parents  []string
	sections int     // headers seen for cur
	changed  pathSet // curSet paths the current section's diff changes
	present  pathSet // root: waiting paths in its tree
	partial  []byte  // an unterminated field
	pathNext bool    // the next field is a raw record's path
	err      error
}

type pathSet map[int32]struct{}

func newLastCommitWalk(rev string, paths []string, maxDistinct int) *lastCommitWalk {
	w := &lastCommitWalk{
		rev:     rev,
		paths:   paths,
		index:   make(map[string]int32, len(paths)),
		last:    make([]string, len(paths)),
		waiting: map[string]pathSet{},
		left:    len(paths),
		commits: map[string]struct{}{},
		max:     maxDistinct,
	}
	all := make(pathSet, len(paths))
	for i, p := range paths {
		_, dup := w.index[p]
		assertf(!dup && p != "", "LastCommits: bad or duplicate path %q", p)
		w.index[p] = int32(i)
		all[int32(i)] = struct{}{}
	}
	w.waiting[rev] = all
	return w
}

// hand moves set to the paths waiting on commit c. Merging the smaller set
// into the larger keeps the total work O(paths * log paths).
func (w *lastCommitWalk) hand(c string, set pathSet) {
	have := w.waiting[c]
	if len(have) < len(set) {
		have, set = set, have
	}
	for i := range set {
		have[i] = struct{}{}
	}
	w.waiting[c] = have
}

func (w *lastCommitWalk) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		i := bytes.IndexByte(p, 0)
		if i < 0 {
			// A path from any commit of the walk, not only from rev's
			// tree, so the ls-tree output allowance does not bound it.
			if w.partial = append(w.partial, p...); len(w.partial) > maxParentsLine {
				w.err = fmt.Errorf("diff-tree field over %d bytes: %w", maxParentsLine, ErrOutputLimit)
				return 0, ErrStop
			}
			break
		}
		field := p[:i]
		if len(w.partial) > 0 {
			field = append(w.partial, field...)
			w.partial = nil
		}
		p = p[i+1:]
		if err := w.field(field); err != nil {
			if !errors.Is(err, ErrStop) {
				w.err = err
			}
			return 0, ErrStop
		}
	}
	return n, nil
}

// field handles one NUL-terminated field: a header (a commit SHA), a raw
// record (":<modes> <oids> <status>"), or the path that follows a record.
func (w *lastCommitWalk) field(f []byte) error {
	switch {
	case w.pathNext:
		w.pathNext = false
		i, ok := w.index[string(f)]
		if _, waits := w.curSet[i]; !ok || !waits {
			return nil
		}
		if len(w.parents) == 0 {
			w.present[i] = struct{}{}
		} else {
			w.changed[i] = struct{}{}
		}
		return nil
	case len(f) > 0 && f[0] == ':':
		if w.cur == "" {
			return fmt.Errorf("diff-tree: record %q before any header", f)
		}
		// --no-renames: one path per record, never a rename or copy.
		if parts := bytes.Split(f, []byte(" ")); len(parts) != 5 || len(parts[4]) != 1 || bytes.ContainsAny(parts[4], "RC") {
			return fmt.Errorf("diff-tree: unexpected raw record %q", f)
		}
		w.pathNext = true
		return nil
	}
	sha := string(f)
	if !ref.ValidSHA(sha) {
		return fmt.Errorf("diff-tree: unexpected field %.80q", f)
	}
	if w.cur != "" && w.sections < max(1, len(w.parents)) {
		// Another diff of the same commit, against its next parent.
		if sha != w.cur {
			return fmt.Errorf("diff-tree: header %s, want %s", sha, w.cur)
		}
		w.endSection()
		w.sections++
		return nil
	}
	if err := w.finish(); err != nil {
		return err
	}
	line, err := readRevListLine(w.expect)
	if err != nil {
		return fmt.Errorf("rev-list spool: %w", err)
	}
	fields := strings.Fields(line)
	if len(fields) == 0 || fields[0] != sha {
		return fmt.Errorf("diff-tree: header %s, want rev-list line %q", sha, strings.TrimSpace(line))
	}
	w.cur, w.curSet, w.parents, w.sections = sha, w.waiting[sha], fields[1:], 1
	delete(w.waiting, sha) // no later commit hands paths to cur
	w.changed, w.present = pathSet{}, pathSet{}
	return nil
}

// endSection ends the diff against parent k = sections-1. The waiting
// paths it leaves unchanged go to that parent: git follows the first
// TREESAME parent. The paths it changed stay for the next parent's diff.
// So the state per commit is one set of paths, not one per parent.
func (w *lastCommitWalk) endSection() {
	for i := range w.changed {
		delete(w.curSet, i)
	}
	if len(w.curSet) > 0 {
		w.hand(w.parents[w.sections-1], w.curSet)
	}
	w.curSet, w.changed = w.changed, pathSet{}
}

// finish moves or resolves the paths waiting on the current commit, once
// all of its diffs have arrived. It returns ErrStop when the walk is done.
func (w *lastCommitWalk) finish() error {
	if w.cur == "" {
		return nil
	}
	if w.pathNext || len(w.partial) > 0 || w.sections != max(1, len(w.parents)) {
		return fmt.Errorf("diff-tree: output for %s ended early", w.cur)
	}
	if len(w.parents) == 0 {
		for i := range w.curSet {
			if _, ok := w.present[i]; !ok {
				return fmt.Errorf("path %q is not in the tree of %s", w.paths[i], w.rev)
			}
			if err := w.resolve(i); err != nil {
				return err
			}
		}
	} else {
		w.endSection()
		// Changed against every parent: cur is the answer.
		for i := range w.curSet {
			if err := w.resolve(i); err != nil {
				return err
			}
		}
	}
	w.curSet = nil
	w.cur = ""
	if w.left == 0 {
		return ErrStop
	}
	return nil
}

// resolve records the current commit as the answer for path i.
func (w *lastCommitWalk) resolve(i int32) error {
	w.last[i] = w.cur
	w.left--
	w.commits[w.cur] = struct{}{}
	if len(w.commits) > w.max {
		return fmt.Errorf("%w: more than %d", ErrTooManyLastCommits, w.max)
	}
	return nil
}
