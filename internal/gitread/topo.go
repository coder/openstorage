package gitread

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/coder/openstorage/internal/ref"
)

// TopoRanks returns the history-order rank of every commit in want relative
// to rev (a resolved full SHA): the commit's 0-based position in
// `git rev-list --topo-order rev`. rev itself ranks 0 and every commit ranks
// higher than all of its descendants.
//
// Ties (commits that ancestry does not order) follow git's topo-order walk,
// which ignores author and committer dates: a commit becomes eligible once
// all of its children reachable from rev are ranked, and the most recently
// eligible commit is ranked next. When ranking a merge makes several of its
// parents eligible at once, they become eligible in parent order, so the
// line of history behind the last of them is ranked first. The same history
// therefore always yields the same ranks.
//
// Every entry of want must be reachable from rev; an unreachable commit is
// an error, not a missing entry. Duplicates in want are allowed. The walk
// stops once every wanted commit is ranked; without a commit-graph, git
// still reads the whole history before it prints the first commit. The walk
// is a Heavy child of the request's read budget (see Run): it fails with
// ErrBusy, ErrTimeBudget or ErrMemoryLimit rather than exceed it.
func TopoRanks(ctx context.Context, repoPath, rev string, want []string) (map[string]int, error) {
	assertf(ref.ValidSHA(rev), "TopoRanks: unresolved rev %q", rev)
	pending := make(map[string]struct{}, len(want))
	for _, sha := range want {
		assertf(ref.ValidSHA(sha), "TopoRanks: malformed wanted sha %q", sha)
		pending[sha] = struct{}{}
	}
	ranks := make(map[string]int, len(pending))
	if len(pending) == 0 {
		return ranks, nil
	}
	assertf(repoPath != "", "TopoRanks: empty repoPath")

	// One Heavy streaming child through Run (#313): the request's budget
	// applies (admission before git starts, the Heavy heap cap on Linux,
	// the time budget), and limit outcomes come back as ErrBusy,
	// ErrTimeBudget or ErrMemoryLimit. The writer ends git with ErrStop as
	// soon as the last wanted commit is ranked; Run then reaps git.
	rw := &rankWriter{pending: pending, ranks: ranks}
	_, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdout: rw, Args: []string{"rev-list", "--topo-order", rev}})
	if rw.err != nil {
		return nil, rw.err
	}
	if err != nil {
		return nil, err
	}
	// git exited on its own (or was stopped): account for a final line
	// without a newline, then every wanted commit must have been seen.
	if len(rw.partial) > 0 {
		rw.line(string(rw.partial))
		if rw.err != nil {
			return nil, rw.err
		}
	}
	if len(pending) > 0 {
		missing := slices.Sorted(maps.Keys(pending))
		return nil, fmt.Errorf("commit %s is not reachable from %s", missing[0], rev)
	}
	assertf(len(ranks) > 0, "TopoRanks: finished with no ranks")
	return ranks, nil
}

// maxRevListLine bounds one line of `rev-list` output (an object ID plus
// newline); anything longer is not rev-list output.
const maxRevListLine = 128

// rankWriter consumes `git rev-list --topo-order` output as Run streams
// it: the n-th line is the commit of rank n. It returns ErrStop once every
// pending commit is ranked, and records (in err) any output that is not a
// list of object IDs or a rank that would overflow the API's int32.
type rankWriter struct {
	pending map[string]struct{}
	ranks   map[string]int
	partial []byte
	rank    int
	err     error
}

func (w *rankWriter) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 && w.err == nil && len(w.pending) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			w.partial = append(w.partial, p...)
			if len(w.partial) > maxRevListLine {
				w.err = fmt.Errorf("git rev-list: output line longer than %d bytes", maxRevListLine)
			}
			break
		}
		w.partial = append(w.partial, p[:i]...)
		p = p[i+1:]
		w.line(string(w.partial))
		w.partial = w.partial[:0]
	}
	if w.err != nil {
		return 0, w.err
	}
	if len(w.pending) == 0 {
		return n, ErrStop
	}
	return n, nil
}

// line ranks one output line.
func (w *rankWriter) line(sha string) {
	if !ref.ValidSHA(sha) {
		w.err = fmt.Errorf("git rev-list: unexpected output line %q", sha)
		return
	}
	// The API documents order as int32.
	if w.rank > math.MaxInt32 {
		w.err = fmt.Errorf("topo rank %d overflows int32", w.rank)
		return
	}
	if _, ok := w.pending[sha]; ok {
		w.ranks[sha] = w.rank
		delete(w.pending, sha)
	}
	w.rank++
}
