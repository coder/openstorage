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

// maxParentsLine bounds one `rev-list --parents` line and one diff-tree
// field: about 25,000 parents. A commit with more (git allows any
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
// thousands of paths). One difference: a submodule's `ignore` setting in
// .gitmodules never hides a gitlink change. It also returns the rank of
// each of those commits: its 0-based position in `git rev-list
// --topo-order rev` (files/metadata `order`). It fails with
// ErrTooManyLastCommits as soon as more than maxDistinct distinct commits
// are found.
//
// One walk replaces a `git log -1` per path. `rev-list --topo-order
// --parents rev` is spooled to a temporary file and read one line, so one
// commit, at a time; the line number is the rank (--parents does not change
// the order). Each path waits at one commit, starting at rev, and moves the
// way git's default history simplification moves for that path alone.
// Topological order puts every commit before its parents, so a commit has
// collected every path waiting on it when its line is read. The walk asks
// one `diff-tree --stdin` child for the diff of such a commit against each
// parent in turn (first diffs are sent ahead within a window, see walk).
// The paths a diff leaves
// unchanged go to that parent (git follows the first TREESAME parent); the
// paths it changes stay for the next parent's diff. A root commit, or a
// commit whose diff against every parent changes the path, is the answer.
// So a commit that no path waits on is diffed only when the walk sent it
// ahead, against its first parent, and a merge's later parents are not
// diffed once no path is left: on a history with many
// merges, the diffs against second parents are most of the work (#576).
// The walk stops once every path has its commit.
//
// Both children are Heavy and run under the request's budget (see Run).
func LastCommits(ctx context.Context, repoPath, rev string, paths []string, maxDistinct int) ([]string, map[string]int, error) {
	assertf(ref.ValidSHA(rev), "LastCommits: unresolved rev %q", rev)
	assertf(maxDistinct > 0, "LastCommits: maxDistinct %d", maxDistinct)
	if len(paths) == 0 {
		return []string{}, map[string]int{}, nil
	}
	assertf(repoPath != "", "LastCommits: empty repoPath")
	w := newLastCommitWalk(rev, paths, maxDistinct)

	spool, err := os.CreateTemp("", "openstorage-lastcommits-*")
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spool.Name())
	}()
	sw := &spoolWriter{f: spool}
	if _, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdout: sw, Args: []string{"rev-list", "--topo-order", "--parents", rev}}); err != nil {
		return nil, nil, err
	}
	if sw.err != nil {
		return nil, nil, sw.err
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	// Room for every answer in flight: the walk sends at most
	// lastCommitWindow requests ahead, plus the one it waits for.
	d := &diffReader{index: w.index, sections: make(chan diffSection, lastCommitWindow+1)}
	args := []string{"diff-tree", "--stdin", "--root", "-r", "--raw", "-z", "--no-renames", "--ignore-submodules=none"}
	if spec := commonPathspec(paths); spec != "" {
		args = append(args, "--", ":(literal)"+spec)
	}
	ran := make(chan error, 1)
	go func() {
		_, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdin: stdinR, Stdout: d, Args: args})
		// git is reaped: closing the read end fails the walk's next write.
		_ = stdinR.Close()
		ran <- err
	}()
	pd := &pipeDiffer{stdin: stdinW, sections: d.sections, ran: ran, running: true}
	walkErr := w.walk(bufio.NewReaderSize(spool, maxParentsLine+1), pd)
	// EOF on stdin ends git normally once the walk is done or failed.
	_ = stdinW.Close()
	if pd.running {
		pd.runErr = <-ran
	}
	// git's own failure comes before the walk's: a budget kill between two
	// answers makes the walk's next write fail with EPIPE, but the request
	// must get the budget error (503 or 422), not that write error.
	switch {
	case d.err != nil: // read after Run returned, which reaped git
		return nil, nil, d.err
	case pd.runErr != nil:
		return nil, nil, pd.runErr
	case walkErr != nil:
		return nil, nil, walkErr
	}
	assertf(w.left == 0, "LastCommits: %d paths unresolved after the walk", w.left)
	return w.last, w.order, nil
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

// lastCommitWalk holds the paths waiting at each commit.
type lastCommitWalk struct {
	rev     string
	paths   []string
	index   map[string]int32 // path -> index into paths
	last    []string         // the answer per path; "" while unresolved
	waiting map[string]pathSet
	left    int
	order   map[string]int // distinct answers -> rank
	max     int
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
		order:   map[string]int{},
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

// lastCommitWindow bounds the diff-tree requests that the walk sends ahead
// of the line it works on (#588). One request per commit costs a round
// trip each, which on a 400,000-commit linear history took most of the
// time. Answers come back in request order, and each buffered answer holds
// at most one entry per path.
const lastCommitWindow = 16

// maxExpected bounds the expected first parents that the walk has not
// read yet (one per line sent ahead whose parent comes later in the
// spool, such as a mainline commit after a merge's side branch). Past it,
// the walk only sends fewer lines ahead; the answers do not change.
const maxExpected = 4096

// differ sends diff-tree input lines and receives their answers in the
// same order.
type differ interface {
	send(input string) error
	recv() (diffSection, error)
}

// pipeDiffer is the differ of one diff-tree child.
type pipeDiffer struct {
	stdin    io.Writer
	sections <-chan diffSection
	ran      <-chan error
	running  bool  // false once ran delivered
	runErr   error // git's outcome, once ran delivered
}

func (p *pipeDiffer) send(input string) error {
	// An input line, then an empty line, which diff-tree echoes after the
	// diff: it marks where the answer ends.
	if _, err := io.WriteString(p.stdin, input+"\n\n"); err != nil {
		return fmt.Errorf("diff-tree input: %w", err)
	}
	return nil
}

func (p *pipeDiffer) recv() (diffSection, error) {
	select {
	case s := <-p.sections:
		return s, s.err
	case p.runErr = <-p.ran:
		p.running = false
		if p.runErr == nil {
			p.runErr = errors.New("diff-tree ended before its answer")
		}
		return diffSection{}, p.runErr
	}
}

// walkLine is one spooled rev-list line in the walk's window.
type walkLine struct {
	c        string
	parents  []string
	rank     int
	expected bool     // the first parent of a line whose diff was sent
	first    *diffReq // its first diff, sent ahead; nil if not sent
}

// input is the line's first diff-tree input: against its first parent, or
// against the empty tree for a root.
func (l *walkLine) input() string {
	if len(l.parents) == 0 {
		return l.c
	}
	return l.c + " " + l.parents[0]
}

// diffReq is one request in flight, answered in send order.
type diffReq struct {
	ans  diffSection
	done bool
}

// walk reads the spooled rev-list one commit at a time and moves the paths
// waiting on it (see LastCommits). A diff-tree input line is "<commit>
// <parent>", or "<commit>" for a root (diffed against the empty tree, so it
// lists the paths in the root's tree).
//
// It keeps a window of up to lastCommitWindow lines ahead and sends their
// first diffs before it needs them: for every line that a path already
// waits on, and for the first parent of every line whose diff it sent,
// since paths mostly move to first parents. That keeps a linear history to
// one request per commit without a round trip per commit. A line sent
// ahead that no path reaches costs one diff whose answer is dropped. Later
// parents of a merge are sent only when needed, and the walk then waits
// for them.
func (w *lastCommitWalk) walk(spool *bufio.Reader, d differ) error {
	var window []*walkLine
	var flight []*diffReq           // sent, not yet received, in send order
	expect := map[string]struct{}{} // expected commits not yet read
	eof := false
	send := func(input string) (*diffReq, error) {
		if err := d.send(input); err != nil {
			return nil, err
		}
		r := &diffReq{}
		flight = append(flight, r)
		return r, nil
	}
	await := func(r *diffReq, c, input string) (diffSection, error) {
		for !r.done {
			s, err := d.recv()
			if err != nil {
				return diffSection{}, err
			}
			flight[0].ans, flight[0].done = s, true
			flight = flight[1:]
		}
		if r.ans.header != "" && r.ans.header != c {
			return diffSection{}, fmt.Errorf("diff-tree: header %s for input %q", r.ans.header, input)
		}
		return r.ans, nil
	}
	expectParent := func(l *walkLine) {
		if len(l.parents) == 0 {
			return
		}
		for _, o := range window {
			if o.c == l.parents[0] {
				o.expected = true
				return
			}
		}
		if len(expect) < maxExpected {
			expect[l.parents[0]] = struct{}{}
		}
	}
	for rank := 0; w.left > 0; {
		for !eof && len(window) < lastCommitWindow {
			line, err := readRevListLine(spool)
			if err == io.EOF && line == "" {
				eof = true
				break
			} else if err != nil && err != io.EOF {
				return err
			}
			fields := strings.Fields(line)
			if len(fields) == 0 || !ref.ValidSHA(fields[0]) {
				return fmt.Errorf("rev-list emitted %.80q", line)
			}
			l := &walkLine{c: fields[0], parents: fields[1:], rank: rank}
			rank++
			if _, ok := expect[l.c]; ok {
				l.expected = true
				delete(expect, l.c)
			}
			window = append(window, l)
		}
		if len(window) == 0 {
			return fmt.Errorf("rev-list of %s ended with %d paths unresolved", w.rev, w.left)
		}
		// Send ahead, in window order, so an expected first parent later
		// in the window is sent in the same pass.
		for _, l := range window {
			if len(flight) >= lastCommitWindow {
				break
			}
			if l.first != nil || (!l.expected && len(w.waiting[l.c]) == 0) {
				continue
			}
			r, err := send(l.input())
			if err != nil {
				return err
			}
			l.first = r
			expectParent(l)
		}

		l := window[0]
		window = window[1:]
		set := w.waiting[l.c]
		delete(w.waiting, l.c) // no later commit hands paths to l.c
		if len(set) == 0 {
			continue // an answer sent ahead for l is received and dropped later
		}
		// diff answers l's diff against parent k (k 0 of a root: against
		// the empty tree), sending it now unless it was sent ahead.
		diff := func(k int) (diffSection, error) {
			input := l.input()
			if k > 0 {
				input = l.c + " " + l.parents[k]
			}
			r := l.first
			if k > 0 || r == nil {
				var err error
				if r, err = send(input); err != nil {
					return diffSection{}, err
				}
			}
			return await(r, l.c, input)
		}
		if len(l.parents) == 0 {
			s, err := diff(0)
			if err != nil {
				return err
			}
			present := make(pathSet, len(set))
			for _, i := range s.changed {
				present[i] = struct{}{}
			}
			for i := range set {
				if _, ok := present[i]; !ok {
					return fmt.Errorf("path %q is not in the tree of %s", w.paths[i], w.rev)
				}
			}
		}
		for k, p := range l.parents {
			s, err := diff(k)
			if err != nil {
				return err
			}
			changed := pathSet{}
			for _, i := range s.changed {
				if _, ok := set[i]; ok {
					changed[i] = struct{}{}
					delete(set, i)
				}
			}
			if len(set) > 0 {
				w.hand(p, set)
			}
			if set = changed; len(set) == 0 {
				break // later parents cannot change an answer
			}
		}
		// A root, or changed against every parent: l.c is the answer.
		for i := range set {
			if err := w.resolve(i, l.c, l.rank); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolve records commit c, of rank rank, as the answer for path i.
func (w *lastCommitWalk) resolve(i int32, c string, rank int) error {
	w.last[i] = c
	w.left--
	w.order[c] = rank
	if len(w.order) > w.max {
		return fmt.Errorf("%w: more than %d", ErrTooManyLastCommits, w.max)
	}
	return nil
}

// diffSection is diff-tree's answer to one input line: the header (empty
// for an empty diff) and the listed paths that are in the walk's index.
type diffSection struct {
	header  string
	changed []int32
	err     error
}

// diffReader parses diff-tree's -z raw output (an io.Writer for Run) into
// one diffSection per input line. Each answer ends with the echoed empty
// input line: a "\n" where a header or a record would start, which neither
// can (a path can, so it is recognized only between records).
type diffReader struct {
	index    map[string]int32
	sections chan diffSection // capacity 1: the walk writes the next line only after reading an answer

	cur      diffSection
	partial  []byte // an unterminated field
	pathNext bool   // the next field is a raw record's path
	err      error  // read by LastCommits only after Run returned
}

func (d *diffReader) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		if len(d.partial) == 0 && !d.pathNext && p[0] == '\n' {
			p = p[1:]
			select {
			case d.sections <- d.cur:
				d.cur = diffSection{}
				continue
			default:
				return 0, d.fail(errors.New("diff-tree answered more lines than it was given"))
			}
		}
		i := bytes.IndexByte(p, 0)
		if i < 0 {
			// A path from any commit of the walk, not only from rev's
			// tree, so the ls-tree output allowance does not bound it.
			if d.partial = append(d.partial, p...); len(d.partial) > maxParentsLine {
				return 0, d.fail(fmt.Errorf("diff-tree field over %d bytes: %w", maxParentsLine, ErrOutputLimit))
			}
			break
		}
		field := p[:i]
		if len(d.partial) > 0 {
			field = append(d.partial, field...)
			d.partial = nil
		}
		p = p[i+1:]
		if err := d.field(field); err != nil {
			return 0, d.fail(err)
		}
	}
	return n, nil
}

// fail keeps err, wakes a walk that waits for an answer, and stops git.
func (d *diffReader) fail(err error) error {
	d.err = err
	select {
	case d.sections <- diffSection{err: err}:
	default:
	}
	return ErrStop
}

// field handles one NUL-terminated field: a header (a commit SHA), a raw
// record (":<modes> <oids> <status>"), or the path that follows a record.
func (d *diffReader) field(f []byte) error {
	switch {
	case d.pathNext:
		d.pathNext = false
		if i, ok := d.index[string(f)]; ok {
			if len(d.cur.changed) == len(d.index) {
				return fmt.Errorf("diff-tree listed more than %d paths for one input line", len(d.index))
			}
			d.cur.changed = append(d.cur.changed, i)
		}
		return nil
	case len(f) > 0 && f[0] == ':':
		// --no-renames: one path per record, never a rename or copy.
		if parts := bytes.Split(f, []byte(" ")); len(parts) != 5 || len(parts[4]) != 1 || bytes.ContainsAny(parts[4], "RC") {
			return fmt.Errorf("diff-tree: unexpected raw record %q", f)
		}
		d.pathNext = true
		return nil
	case d.cur.header == "" && ref.ValidSHA(string(f)):
		d.cur.header = string(f)
		return nil
	}
	return fmt.Errorf("diff-tree: unexpected field %.80q", f)
}
