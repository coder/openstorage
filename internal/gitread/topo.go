package gitread

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/coder/openstorage/internal/ref"
)

// ErrMemoryLimit marks a TopoRanks walk that needed more heap than its
// maxHeapBytes cap allowed; git was stopped by its own allocation failure.
var ErrMemoryLimit = errors.New("git exceeded its memory limit")

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
// still reads the whole history before it prints the first commit, and its
// heap grows with that history.
//
// maxHeapBytes > 0 caps git's heap: on Linux, RLIMIT_DATA (heap and other
// private writable mappings; read-only pack mmaps are not counted) is set
// by a /bin/sh wrapper before git is exec'd, and a walk that needs more
// fails with ErrMemoryLimit. Other platforms do not enforce the cap. 0
// means no cap.
func TopoRanks(ctx context.Context, repoPath, rev string, want []string, maxHeapBytes int64) (map[string]int, error) {
	assertf(ref.ValidSHA(rev), "TopoRanks: unresolved rev %q", rev)
	assertf(maxHeapBytes >= 0, "TopoRanks: negative maxHeapBytes %d", maxHeapBytes)
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

	// A child context lets us stop git as soon as the last wanted commit
	// is ranked instead of draining the rest of the history.
	walkCtx, stop := context.WithCancel(ctx)
	defer stop()
	args := []string{"rev-list", "--topo-order", rev}
	cmd := heapLimitedGit(walkCtx, maxHeapBytes, args...)
	cmd.Dir = repoPath
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	sc := bufio.NewScanner(stdout)
	rank := 0
	for len(pending) > 0 && sc.Scan() {
		sha := sc.Text()
		if _, ok := pending[sha]; ok {
			ranks[sha] = rank
			delete(pending, sha)
		}
		rank++
	}
	scanErr := sc.Err()
	if len(pending) == 0 {
		// Done: stop git early. Its exit status is irrelevant now,
		// since it may have been killed mid-write.
		stop()
		_ = cmd.Wait()
		// The API documents order as int32.
		if rank-1 > math.MaxInt32 {
			return nil, fmt.Errorf("topo rank %d overflows int32", rank-1)
		}
		assertf(len(ranks) > 0, "TopoRanks: finished with no ranks")
		return ranks, nil
	}
	waitErr := cmd.Wait()
	if scanErr != nil {
		return nil, fmt.Errorf("git %s: read: %w", strings.Join(args, " "), scanErr)
	}
	if waitErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if maxHeapBytes > 0 && heapLimitFailure(waitErr, msg) {
			return nil, fmt.Errorf("git %s: %w (%d bytes): %s", strings.Join(args, " "), ErrMemoryLimit, maxHeapBytes, msg)
		}
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), waitErr, msg)
	}
	missing := slices.Sorted(maps.Keys(pending))
	return nil, fmt.Errorf("commit %s is not reachable from %s", missing[0], rev)
}
