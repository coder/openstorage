package gitread

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// lcFile is one tree entry of a generated commit.
type lcFile struct{ mode, data string }

// lcHistory writes a random history with merges, octopus merges, extra
// roots, evil merges, deletions, and contents that recur on several
// branches (so a merge is often TREESAME to a later parent only). It
// returns the repo and the SHA of every commit, in creation order.
func lcHistory(t *testing.T, seed uint64, n int) (dir string, shas []string) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 569))
	paths := []string{"a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt", "g*.txt", "gx.txt", "g?.txt", "[x].txt", "x.txt", "[d]/e.txt", "[d]/f.txt", ":(attr:x)p/q.txt", "df", "df/in", "link", "mod", "\nnl.txt"}
	value := func(p string) lcFile {
		v := []string{"one", "two", "three"}[rng.IntN(3)]
		switch p {
		case "link":
			return lcFile{"120000", v}
		case "mod":
			return lcFile{"160000", strings.Repeat(fmt.Sprint(1+rng.IntN(3)), 40)}
		}
		return lcFile{"100644", v + "\n"}
	}
	var states []map[string]lcFile
	var in strings.Builder
	for k := range n {
		var parents []int
		switch r := rng.IntN(10); {
		case k == 0 || (k > 3 && r == 0): // a root
		case k > 2 && r < 4: // a merge, sometimes an octopus
			parents = rng.Perm(k)[:min(k, 2+rng.IntN(2))]
		default:
			parents = []int{max(0, k-1-rng.IntN(3))}
		}
		st := map[string]lcFile{}
		for _, p := range paths {
			switch {
			case len(parents) == 0:
				if rng.IntN(2) == 0 {
					st[p] = value(p)
				}
			case len(parents) > 1 && rng.IntN(8) == 0: // evil merge content
				st[p] = value(p)
			default:
				if f, ok := states[parents[rng.IntN(len(parents))]][p]; ok {
					st[p] = f
				}
			}
		}
		for range rng.IntN(4) {
			p := paths[rng.IntN(len(paths))]
			if rng.IntN(4) == 0 {
				delete(st, p)
			} else {
				st[p] = value(p)
			}
		}
		if _, ok := st["df"]; ok { // "df" is a file or a directory, not both
			delete(st, "df/in")
		}
		states = append(states, st)

		// Committer dates disagree with the topology on purpose.
		// One ref per commit: a commit without "from" on a fresh ref is a root.
		fmt.Fprintf(&in, "commit refs/heads/c%d\nmark :%d\ncommitter T <t@example.com> %d +0000\ndata 0\n", k, k+1, 1700000000+rng.IntN(100000))
		for i, p := range parents {
			fmt.Fprintf(&in, "%s :%d\n", map[bool]string{true: "from", false: "merge"}[i == 0], p+1)
		}
		in.WriteString("deleteall\n")
		names := slices.Sorted(func(yield func(string) bool) {
			for p := range st {
				if !yield(p) {
					return
				}
			}
		})
		for _, p := range names {
			f := st[p]
			if f.mode == "160000" {
				fmt.Fprintf(&in, "M 160000 %s %s\n", f.data, p)
				continue
			}
			fmt.Fprintf(&in, "M %s inline %s\ndata %d\n%s\n", f.mode, lcQuote(p), len(f.data), f.data)
		}
		in.WriteString("\n")
	}
	dir = t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	marks := filepath.Join(t.TempDir(), "marks")
	cmd := exec.Command("git", "fast-import", "--quiet", "--force", "--export-marks="+marks)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(marks)
	if err != nil {
		t.Fatal(err)
	}
	shas = make([]string, n)
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		var mark int
		var sha string
		if _, err := fmt.Sscanf(line, ":%d %s", &mark, &sha); err != nil {
			t.Fatalf("marks line %q: %v", line, err)
		}
		shas[mark-1] = sha
	}
	return dir, shas
}

// lcQuote quotes a fast-import path that holds a newline (a diff-tree
// answer ends with an echoed newline, so such names must stay paths).
func lcQuote(p string) string {
	if strings.Contains(p, "\n") {
		return strconv.Quote(p)
	}
	return p
}

// lcOracle returns what `git log -1 rev -- <path>` names for each path,
// with literal pathspecs (the glob-like names above match only themselves).
func lcOracle(t *testing.T, dir, rev string, paths []string) map[string]string {
	t.Helper()
	want := make(map[string]string, len(paths))
	for _, p := range paths {
		cmd := exec.Command("git", "--literal-pathspecs", "log", "-1", "--format=%H", rev, "--", p)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), gitEnv...)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git log -1 %s -- %s: %v", rev, p, err)
		}
		want[p] = strings.TrimSpace(string(out))
	}
	return want
}

// lastCommitsByPath is LastCommits keyed by path. It also checks that the
// ranks cover exactly the answers.
func lastCommitsByPath(ctx context.Context, dir, rev string, paths []string, maxDistinct int) (map[string]string, map[string]int, error) {
	last, ranks, err := LastCommits(ctx, dir, rev, paths, maxDistinct)
	if err != nil {
		return nil, nil, err
	}
	if len(last) != len(paths) {
		return nil, nil, fmt.Errorf("%d answers for %d paths", len(last), len(paths))
	}
	m := make(map[string]string, len(paths))
	for i, p := range paths {
		m[p] = last[i]
		if _, ok := ranks[last[i]]; !ok {
			return nil, nil, fmt.Errorf("no rank for %s", last[i])
		}
	}
	if distinct := len(slices.Compact(slices.Sorted(maps.Values(m)))); len(ranks) != distinct {
		return nil, nil, fmt.Errorf("%d ranks for %d distinct answers", len(ranks), distinct)
	}
	return m, ranks, nil
}

// lcRanks returns every commit's 0-based position in `git rev-list
// --topo-order rev`, the documented files/metadata `order`.
func lcRanks(t *testing.T, dir, rev string) map[string]int {
	t.Helper()
	ranks := map[string]int{}
	for i, sha := range strings.Fields(mustGit(t, dir, "rev-list", "--topo-order", rev)) {
		ranks[sha] = i
	}
	return ranks
}

// TestLastCommitsMatchesGitLog checks the one-walk answer against one
// `git log -1 -- <path>` per file, on random histories, at several revs,
// for the whole tree and for one subtree.
func TestLastCommitsMatchesGitLog(t *testing.T) {
	ctx := context.Background()
	subtrees := map[string]int{}
	for seed := range uint64(30) {
		dir, shas := lcHistory(t, seed, 25)
		revs := []string{shas[len(shas)-1], shas[len(shas)/2], shas[len(shas)/3]}
		for _, rev := range revs {
			entries, err := LsTree(ctx, dir, rev, "", true)
			if err != nil {
				t.Fatal(err)
			}
			// Subtrees whose names are pathspec magic or globs, unless
			// passed literally, would make diff-tree skip their files.
			for _, sub := range []string{"", "dir/", "[d]/", ":(attr:x)p/"} {
				var paths []string
				for _, e := range entries {
					if strings.HasPrefix(e.Path, sub) {
						paths = append(paths, e.Path)
					}
				}
				if len(paths) == 0 {
					continue
				}
				subtrees[sub]++
				got, ranks, err := lastCommitsByPath(ctx, dir, rev, paths, 1000)
				if err != nil {
					t.Fatalf("seed %d rev %s sub %q: %v", seed, rev, sub, err)
				}
				if want := lcOracle(t, dir, rev, paths); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("seed %d rev %s sub %q:\n got %v\nwant %v", seed, rev, sub, got, want)
				}
				all := lcRanks(t, dir, rev)
				for sha, rank := range ranks {
					if rank != all[sha] {
						t.Fatalf("seed %d rev %s: rank of %s = %d, want %d (rev-list --topo-order)", seed, rev, sha, rank, all[sha])
					}
				}
			}
		}
	}
	// Every subtree was checked at least once.
	if len(subtrees) != 4 {
		t.Fatalf("checked subtrees %v, want all four", subtrees)
	}
}

func TestLastCommitsDistinctLimit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	entries, err := LsTree(ctx, f.dir, f.c2, "", true)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, e := range entries {
		paths = append(paths, e.Path)
	}
	// c2 changed some files; the rest still date from c1.
	got, _, err := lastCommitsByPath(ctx, f.dir, f.c2, paths, 2)
	if err != nil {
		t.Fatalf("2 distinct commits with limit 2: %v", err)
	}
	var commits []string
	for _, sha := range got {
		commits = append(commits, sha)
	}
	sort.Strings(commits)
	if commits = slices.Compact(commits); len(commits) != 2 {
		t.Fatalf("distinct last commits = %v, want c1 and c2", commits)
	}
	if _, _, err := LastCommits(ctx, f.dir, f.c2, paths, 1); !errors.Is(err, ErrTooManyLastCommits) {
		t.Fatalf("2 distinct commits with limit 1: err = %v, want ErrTooManyLastCommits", err)
	}
}

func TestLastCommitsRejectsPathNotInRev(t *testing.T) {
	f := newFixture(t)
	// feature.txt exists only on the feature branch (c3), not at c2.
	_, _, err := LastCommits(context.Background(), f.dir, f.c2, []string{"README.md", "feature.txt"}, 10)
	if err == nil || !strings.Contains(err.Error(), "feature.txt") {
		t.Fatalf("err = %v, want an error naming feature.txt", err)
	}
}

func TestLastCommitsNoPathsRunsNoGit(t *testing.T) {
	got, ranks, err := LastCommits(context.Background(), "/nonexistent/repo", strings.Repeat("a", 40), nil, 1)
	if err != nil || got == nil || len(got) != 0 || ranks == nil || len(ranks) != 0 {
		t.Fatalf("got %#v %#v, err %v; want empty non-nil results", got, ranks, err)
	}
}

func TestCommitsBySHA(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	got, err := CommitsBySHA(ctx, f.dir, []string{f.c2, f.c1})
	if err != nil {
		t.Fatal(err)
	}
	for _, sha := range []string{f.c1, f.c2} {
		want, err := Log(ctx, f.dir, sha, LogOptions{MaxCount: 1, SubjectOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(got[sha]) != fmt.Sprint(want[0]) {
			t.Errorf("commit %s = %+v, want %+v", sha, got[sha], want[0])
		}
	}
	if len(got) != 2 {
		t.Fatalf("got %d commits, want 2", len(got))
	}
}

// TestLastCommitsDiffsOnlyWhatCanChangeAnAnswer pins #576: a side branch
// that no waiting path reaches is never diffed, and a merge is not diffed
// against its second parent once its first parent took every path. A git
// wrapper on PATH logs the lines diff-tree reads.
func TestLastCommitsDiffsOnlyWhatCanChangeAnAnswer(t *testing.T) {
	commit := func(ref string, mark, from int, merge int, file, data string) string {
		s := fmt.Sprintf("commit refs/heads/%s\nmark :%d\ncommitter T <t@example.com> %d +0000\ndata 0\n", ref, mark, 1700000000+mark)
		if from > 0 {
			s += fmt.Sprintf("from :%d\n", from)
		}
		if merge > 0 {
			s += fmt.Sprintf("merge :%d\n", merge)
		}
		return s + fmt.Sprintf("M 100644 inline %s\ndata %d\n%s\n", file, len(data), data)
	}
	stream := commit("main", 1, 0, 0, "a.txt", "a\n") + "M 100644 inline b.txt\ndata 2\nb\n\n" +
		commit("side", 2, 1, 0, "b.txt", "s1\n") + "\n" +
		commit("side", 3, 2, 0, "b.txt", "s2\n") + "\n" +
		commit("main", 4, 1, 0, "c.txt", "c\n") + "\n" +
		commit("main", 5, 4, 3, "b.txt", "s2\n") + "\n"
	dir := lcFastImport(t, stream)
	sha := func(rev string) string { return strings.TrimSpace(mustGit(t, dir, "rev-parse", rev)) }
	root, m4, tip := sha("main~2"), sha("main~1"), sha("main")

	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	log := filepath.Join(bin, "diff-tree-input")
	wrapper := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = diff-tree ]; then tee -a %q | %q \"$@\"; exit $?; fi\nexec %q \"$@\"\n", log, realGit, realGit)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	got, _, err := lastCommitsByPath(context.Background(), dir, tip, []string{"a.txt", "c.txt"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got["a.txt"] != root || got["c.txt"] != m4 {
		t.Fatalf("got %v, want a.txt at the root %s and c.txt at %s", got, root, m4)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for line := range strings.SplitSeq(string(raw), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	if want := []string{tip + " " + m4, m4 + " " + root, root}; !slices.Equal(lines, want) {
		t.Fatalf("diff-tree read %q, want %q", lines, want)
	}
}

// TestLastCommitsKeepsBudgetErrorsOfDiffTree: when the heap cap kills
// diff-tree in the middle of the walk, the call fails with ErrMemoryLimit
// (the documented 422), whether the walk was waiting for an answer or
// writing its next line. A git wrapper on PATH answers the first line,
// then dies the way git dies under the cap.
func TestLastCommitsKeepsBudgetErrorsOfDiffTree(t *testing.T) {
	if !HeapLimitSupported() {
		t.Skip("heap cap is Linux-only")
	}
	dir, tip, _ := deepHistory(t, 50)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	wrapper := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = diff-tree ]; then head -n 2 | %q \"$@\"; kill -SEGV $$; fi\nexec %q \"$@\"\n", realGit, realGit)
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes)
	defer b.Close()
	if _, _, err := LastCommits(WithBudget(context.Background(), b), dir, tip, []string{"cold.txt"}, 10); !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("err = %v, want ErrMemoryLimit", err)
	}
	assertNoChildren(t)
}

// execDiffer answers each walk request with its own `git diff-tree` run,
// parsed by a diffReader, and counts the requests in flight.
type execDiffer struct {
	t        *testing.T
	dir      string
	index    map[string]int32
	queue    []diffSection
	inputs   []string
	inFlight int // most requests sent and not yet received
}

func (e *execDiffer) send(input string) error {
	args := append([]string{"diff-tree", "--root", "-r", "--raw", "-z", "--no-renames", "--ignore-submodules=none"}, strings.Fields(input)...)
	out := mustGit(e.t, e.dir, args...)
	d := &diffReader{index: e.index, sections: make(chan diffSection, 1)}
	if _, err := d.Write([]byte(out + "\n")); err != nil {
		return d.err
	}
	e.queue = append(e.queue, <-d.sections)
	e.inputs = append(e.inputs, input)
	e.inFlight = max(e.inFlight, len(e.queue))
	return nil
}

func (e *execDiffer) recv() (diffSection, error) {
	if len(e.queue) == 0 {
		return diffSection{}, errors.New("recv with no request in flight")
	}
	s := e.queue[0]
	e.queue = e.queue[1:]
	return s, nil
}

// TestLastCommitWalkSendsAheadWithinWindow pins #588: on a linear history
// the walk keeps lastCommitWindow requests in flight instead of one, never
// more than one past the window, and still finds every answer and rank.
func TestLastCommitWalkSendsAheadWithinWindow(t *testing.T) {
	const n = 60
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\nmark :%d\ncommitter T <t@example.com> %d +0000\ndata 0\n", i+1, 1700000000+i)
		if i == 0 {
			in.WriteString("M 100644 inline cold.txt\ndata 5\ncold\n")
		} else {
			fmt.Fprintf(&in, "from :%d\n", i)
		}
		if i == n/2 {
			in.WriteString("M 100644 inline mid.txt\ndata 4\nmid\n")
		}
		fmt.Fprintf(&in, "M 100644 inline hot.txt\ndata %d\n%d\n\n", len(fmt.Sprint(i))+1, i)
	}
	dir := lcFastImport(t, in.String())
	tip := strings.TrimSpace(mustGit(t, dir, "rev-parse", "main"))
	paths := []string{"cold.txt", "hot.txt", "mid.txt"}
	w := newLastCommitWalk(tip, paths, 10)
	e := &execDiffer{t: t, dir: dir, index: w.index}
	spool := bufio.NewReaderSize(strings.NewReader(mustGit(t, dir, "rev-list", "--topo-order", "--parents", tip)), maxParentsLine+1)
	if err := w.walk(spool, e); err != nil {
		t.Fatal(err)
	}
	want := lcOracle(t, dir, tip, paths)
	ranks := lcRanks(t, dir, tip)
	for i, p := range paths {
		if w.last[i] != want[p] || w.order[w.last[i]] != ranks[w.last[i]] {
			t.Errorf("%s = %s rank %d, want %s rank %d", p, w.last[i], w.order[w.last[i]], want[p], ranks[want[p]])
		}
	}
	if e.inFlight != lastCommitWindow {
		t.Errorf("at most %d requests in flight, want %d", e.inFlight, lastCommitWindow)
	}
	// Every commit holds a waiting path, so each is diffed exactly once.
	if len(e.inputs) != n {
		t.Errorf("%d requests for %d commits: %q", len(e.inputs), n, e.inputs)
	}
}

// TestLastCommitWalkBoundsRequestsInFlight: the tip is a merge whose path
// comes from the second parent, while the first-parent chain rewrites it.
// The walk must not send that chain ahead (a merge's paths may all leave by
// a later parent, so nothing is sent ahead past a merge), and must keep the
// requests in flight within the window.
func TestLastCommitWalkBoundsRequestsInFlight(t *testing.T) {
	const n = 3 * lastCommitWindow
	var in strings.Builder
	mark := 1
	commit := func(from int, file, data string) int {
		fmt.Fprintf(&in, "commit refs/heads/b%d\nmark :%d\ncommitter T <t@example.com> %d +0000\ndata 0\n", mark, mark, 1700000000+mark)
		if from > 0 {
			fmt.Fprintf(&in, "from :%d\n", from)
		}
		fmt.Fprintf(&in, "M 100644 inline %s\ndata %d\n%s\n\n", file, len(data), data)
		mark++
		return mark - 1
	}
	root := commit(0, "p.txt", "p\n")
	side, mainline := root, commit(root, "p.txt", "changed on the first-parent side\n")
	for i := range n {
		side = commit(side, "b.txt", fmt.Sprint(i))
		mainline = commit(mainline, "a.txt", fmt.Sprint(i))
	}
	// The tip takes p.txt from the second parent's side.
	fmt.Fprintf(&in, "commit refs/heads/main\ncommitter T <t@example.com> 1800000000 +0000\ndata 0\nfrom :%d\nmerge :%d\nM 100644 inline p.txt\ndata 2\np\n\n", mainline, side)
	dir := lcFastImport(t, in.String())
	tip := strings.TrimSpace(mustGit(t, dir, "rev-parse", "main"))
	w := newLastCommitWalk(tip, []string{"p.txt"}, 10)
	e := &execDiffer{t: t, dir: dir, index: w.index}
	spool := bufio.NewReaderSize(strings.NewReader(mustGit(t, dir, "rev-list", "--topo-order", "--parents", tip)), maxParentsLine+1)
	if err := w.walk(spool, e); err != nil {
		t.Fatal(err)
	}
	if want := lcOracle(t, dir, tip, []string{"p.txt"}); w.last[0] != want["p.txt"] {
		t.Fatalf("p.txt = %s, want %s", w.last[0], want["p.txt"])
	}
	if e.inFlight > lastCommitWindow+1 {
		t.Fatalf("%d requests in flight, want at most %d", e.inFlight, lastCommitWindow+1)
	}
	// The chain below the tip's first parent, without the shared root.
	rootSHA := strings.TrimSpace(mustGit(t, dir, "rev-list", "--max-parents=0", "main"))
	firstParent := strings.Fields(mustGit(t, dir, "rev-list", "--first-parent", "main~1", "^"+rootSHA))
	for _, input := range e.inputs {
		if slices.Contains(firstParent, strings.Fields(input)[0]) {
			t.Fatalf("the walk diffed first-parent commit %s, which no path reaches (inputs %q)", input, e.inputs)
		}
	}
}

// tableDiffer answers walk requests from a table of changed paths per
// diff-tree input ("" lists nothing), for spools that git would not print
// but that the walk must handle: any order with children before parents.
type tableDiffer struct {
	changed map[string][]int32
	queue   []diffSection
	inputs  []string
}

func (d *tableDiffer) send(input string) error {
	d.queue = append(d.queue, diffSection{changed: d.changed[input]})
	d.inputs = append(d.inputs, input)
	return nil
}

func (d *tableDiffer) recv() (diffSection, error) {
	if len(d.queue) == 0 {
		return diffSection{}, errors.New("recv with no request in flight")
	}
	s := d.queue[0]
	d.queue = d.queue[1:]
	return s, nil
}

// TestLastCommitWalkCancelsDeadChains: the tip T merges chains D and E,
// whose lines the spool interleaves. p takes D and resolves at D1, so the
// rest of D is dead, while q walks all of E, and each E line makes the walk
// wait. The walk must stop sending D ahead once D1 resolves: at most one
// window of D's diffs is wasted, not the whole chain.
func TestLastCommitWalkCancelsDeadChains(t *testing.T) {
	const n = 4 * lastCommitWindow
	id := func(name string) string { return fmt.Sprintf("%x", sha1.Sum([]byte(name))) }
	p, q := int32(0), int32(1)
	tip, root := id("T"), id("R")
	changed := map[string][]int32{tip + " " + id("D1"): {q}, id("D1") + " " + id("D2"): {p}, root: {p, q}}
	var spool strings.Builder
	fmt.Fprintf(&spool, "%s %s %s\n", tip, id("D1"), id("E1"))
	for i := 1; i <= n; i++ {
		next := func(chain string) string {
			if i == n {
				return root
			}
			return id(fmt.Sprintf("%s%d", chain, i+1))
		}
		fmt.Fprintf(&spool, "%s %s\n%s %s\n", id(fmt.Sprintf("D%d", i)), next("D"), id(fmt.Sprintf("E%d", i)), next("E"))
	}
	spool.WriteString(root + "\n")

	w := newLastCommitWalk(tip, []string{"p", "q"}, 10)
	d := &tableDiffer{changed: changed}
	if err := w.walk(bufio.NewReader(strings.NewReader(spool.String())), d); err != nil {
		t.Fatal(err)
	}
	if w.last[p] != id("D1") || w.last[q] != root {
		t.Fatalf("answers %v, want p at D1 and q at the root", w.last)
	}
	wasted := 0
	for _, input := range d.inputs {
		for i := 2; i <= n; i++ {
			if strings.HasPrefix(input, id(fmt.Sprintf("D%d", i))) {
				wasted++
			}
		}
	}
	if wasted > lastCommitWindow {
		t.Fatalf("%d of the %d dead D commits were diffed, want at most %d", wasted, n-1, lastCommitWindow)
	}
}

// TestDiffReaderFieldsSplitAcrossWrites feeds real diff-tree answers to
// the reader one byte per Write, since pipes split fields anywhere, and
// compares them with the answers from one Write.
func TestDiffReaderFieldsSplitAcrossWrites(t *testing.T) {
	dir, shas := lcHistory(t, 7, 25)
	rev := shas[len(shas)-1]
	entries, err := LsTree(context.Background(), dir, rev, "", true)
	if err != nil {
		t.Fatal(err)
	}
	index := map[string]int32{}
	for i, e := range entries {
		index[e.Path] = int32(i)
	}
	// Every input line with its empty end-of-answer line.
	var in strings.Builder
	n := 0
	for line := range strings.SplitSeq(strings.TrimSpace(mustGit(t, dir, "rev-list", "--topo-order", "--parents", rev)), "\n") {
		f := strings.Fields(line)
		if len(f) == 1 {
			in.WriteString(f[0] + "\n\n")
			n++
		}
		for _, p := range f[1:] {
			in.WriteString(f[0] + " " + p + "\n\n")
			n++
		}
	}
	cmd := exec.Command("git", "diff-tree", "--stdin", "--root", "-r", "--raw", "-z", "--no-renames", "--ignore-submodules=none")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(in.String())
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	read := func(chunk int) []diffSection {
		// Room for every answer: here git had all input lines at once.
		d := &diffReader{index: index, sections: make(chan diffSection, n)}
		for i := 0; i < len(out); i += chunk {
			if _, err := d.Write(out[i:min(i+chunk, len(out))]); err != nil {
				t.Fatalf("chunk %d at %d: %v", chunk, i, d.err)
			}
		}
		close(d.sections)
		var got []diffSection
		for s := range d.sections {
			got = append(got, s)
		}
		return got
	}
	whole, bytewise := read(len(out)), read(1)
	if len(bytewise) != n {
		t.Fatalf("%d answers for %d input lines", len(bytewise), n)
	}
	if fmt.Sprint(whole) != fmt.Sprint(bytewise) {
		t.Fatalf("answers differ:\n whole %v\n bytes %v", whole, bytewise)
	}
}

// lcFastImport builds a repo from a fast-import stream on refs/heads/main.
func lcFastImport(t *testing.T, stream string) string {
	t.Helper()
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(stream)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	return dir
}

// TestLastCommitsSeesIgnoredSubmodules: an `ignore = all` submodule in
// .gitmodules must not hide the gitlink change that is its last commit.
func TestLastCommitsSeesIgnoredSubmodules(t *testing.T) {
	gm := "[submodule \"mod\"]\n\tpath = mod\n\turl = https://example.com/mod\n\tignore = all\n"
	dir := lcFastImport(t, fmt.Sprintf("commit refs/heads/main\nmark :1\ncommitter T <t@example.com> 1700000000 +0000\ndata 0\n"+
		"M 100644 inline .gitmodules\ndata %d\n%s\nM 160000 %s mod\n\n"+
		"commit refs/heads/main\nmark :2\ncommitter T <t@example.com> 1700000001 +0000\ndata 0\nfrom :1\nM 160000 %s mod\n\n",
		len(gm), gm, strings.Repeat("1", 40), strings.Repeat("2", 40)))
	tip := strings.TrimSpace(mustGit(t, dir, "rev-parse", "main"))
	got, _, err := lastCommitsByPath(context.Background(), dir, tip, []string{".gitmodules", "mod"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if root := strings.TrimSpace(mustGit(t, dir, "rev-parse", "main^")); got["mod"] != tip || got[".gitmodules"] != root {
		t.Fatalf("got %v, want mod at the tip %s and .gitmodules at the root %s", got, tip, root)
	}
}

// TestLastCommitsStopsEarlyWithoutLeaks: every path resolves at the tip of
// a long history, so the walk stops with most of the rev-list unread. The
// call must return, end its goroutine, and remove its spool.
func TestLastCommitsStopsEarlyWithoutLeaks(t *testing.T) {
	const n = 20000 // about 1.6 MB of pairs, far more than a pipe holds
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\nmark :%d\ncommitter T <t@example.com> %d +0000\ndata 0\n", i+1, 1700000000+i)
		if i > 0 {
			fmt.Fprintf(&in, "from :%d\n", i)
		}
		fmt.Fprintf(&in, "M 100644 inline hot.txt\ndata %d\n%d\n\n", len(fmt.Sprint(i))+1, i)
	}
	dir := lcFastImport(t, in.String())
	tip := strings.TrimSpace(mustGit(t, dir, "rev-parse", "main"))
	spools := t.TempDir()
	t.Setenv("TMPDIR", spools)
	before := runtime.NumGoroutine()
	got, _, err := lastCommitsByPath(context.Background(), dir, tip, []string{"hot.txt"}, 1)
	if err != nil || got["hot.txt"] != tip {
		t.Fatalf("got %v, err %v; want hot.txt at the tip", got, err)
	}
	if left, err := os.ReadDir(spools); err != nil || len(left) != 0 {
		t.Fatalf("temp dir holds %v (err %v), want no spool", left, err)
	}
	// The goroutine that runs diff-tree ends after git; give the scheduler a moment.
	for range 100 {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines %d after the call, %d before", runtime.NumGoroutine(), before)
}

// TestLastCommitsOctopusParentLimit: an octopus merge with thousands of
// parents is walked; one whose rev-list line passes maxParentsLine (30,000
// parents: 1.2 MB) fails with the documented output limit, not a 500.
func TestLastCommitsOctopusParentLimit(t *testing.T) {
	for _, tc := range []struct {
		roots   int
		limited bool
	}{{2000, false}, {30000, true}} {
		var in strings.Builder
		in.WriteString("commit refs/heads/r0\nmark :1\ncommitter T <t@example.com> 1700000000 +0000\ndata 0\nM 100644 inline a.txt\ndata 2\na\n\n")
		for i := 1; i < tc.roots; i++ { // empty root commits; reset makes each a root
			fmt.Fprintf(&in, "reset refs/heads/r\ncommit refs/heads/r\nmark :%d\ncommitter T <t@example.com> 1700000000 +0000\ndata 0\n\n", i+1)
		}
		in.WriteString("commit refs/heads/main\ncommitter T <t@example.com> 1700000001 +0000\ndata 0\nfrom :1\n")
		for i := 2; i <= tc.roots; i++ {
			fmt.Fprintf(&in, "merge :%d\n", i)
		}
		in.WriteString("M 100644 inline b.txt\ndata 2\nb\n\n")
		dir := lcFastImport(t, in.String())
		tip := strings.TrimSpace(mustGit(t, dir, "rev-parse", "main"))
		paths := []string{"a.txt", "b.txt"}
		got, _, err := lastCommitsByPath(context.Background(), dir, tip, paths, 10)
		if tc.limited {
			if !errors.Is(err, ErrOutputLimit) || !IsBudgetError(err) {
				t.Fatalf("%d parents: err = %v, want ErrOutputLimit", tc.roots, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if want := lcOracle(t, dir, tip, paths); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%d parents: got %v, want %v", tc.roots, got, want)
		}
	}
}

// TestDiffReaderCapsFields: a path in an old commit is not bounded by
// rev's tree, so the reader caps an unterminated diff-tree field.
func TestDiffReaderCapsFields(t *testing.T) {
	d := &diffReader{index: map[string]int32{"x": 0}, sections: make(chan diffSection, 1)}
	piece := bytes.Repeat([]byte("p"), 64<<10)
	var err error
	for i := 0; err == nil && i <= maxParentsLine/len(piece); i++ {
		_, err = d.Write(piece)
	}
	if !errors.Is(err, ErrStop) || !errors.Is(d.err, ErrOutputLimit) {
		t.Fatalf("long field: err %v / %v, want ErrStop and ErrOutputLimit", err, d.err)
	}
}

// TestDiffReaderStopsGitOnceTheWalkIsDone: after the walk ends, output
// for requests still in flight stops git and is not parsed.
func TestDiffReaderStopsGitOnceTheWalkIsDone(t *testing.T) {
	d := &diffReader{index: map[string]int32{"x": 0}, sections: make(chan diffSection, 1)}
	d.done.Store(true)
	if n, err := d.Write([]byte(strings.Repeat("a", 40) + "\x00\n")); n != 0 || !errors.Is(err, ErrStop) || len(d.sections) != 0 {
		t.Fatalf("Write after done = %d, %v, %d answers; want 0, ErrStop, none", n, err, len(d.sections))
	}
}

// TestDiffReaderRejectsAnswersAheadOfInput: an answer the walk did not ask
// for yet is an error, not the next line's answer.
func TestDiffReaderRejectsAnswersAheadOfInput(t *testing.T) {
	d := &diffReader{index: map[string]int32{"x": 0}, sections: make(chan diffSection, 1)}
	if _, err := d.Write([]byte("\n\n")); !errors.Is(err, ErrStop) || d.err == nil {
		t.Fatalf("two answers at once: err %v / %v, want ErrStop and an error", err, d.err)
	}
}

// TestSpoolWriterLimits: the rev-list spool stops git at a line longer
// than maxParentsLine, also when the line arrives in pieces, and past
// maxSpoolBytes in total.
func TestSpoolWriterLimits(t *testing.T) {
	line := []byte(strings.Repeat("a", 40) + " " + strings.Repeat("b", 40) + "\n")
	var out strings.Builder
	s := &spoolWriter{f: &out}
	if _, err := s.Write(append(line, line...)); err != nil || s.err != nil || out.Len() != 2*len(line) {
		t.Fatalf("short lines: err %v / %v, wrote %d", err, s.err, out.Len())
	}

	s = &spoolWriter{f: io.Discard}
	piece := bytes.Repeat([]byte("p"), 64<<10)
	var err error
	for i := 0; err == nil && i <= maxParentsLine/len(piece); i++ {
		_, err = s.Write(piece)
	}
	if !errors.Is(err, ErrStop) || !errors.Is(s.err, ErrOutputLimit) {
		t.Fatalf("long line in pieces: err %v / %v, want ErrStop and ErrOutputLimit", err, s.err)
	}

	s = &spoolWriter{f: io.Discard}
	chunk := bytes.Repeat(line, (64<<10)/len(line))
	err = nil
	for written := 0; err == nil && written <= maxSpoolBytes; written += len(chunk) {
		_, err = s.Write(chunk)
	}
	if !errors.Is(err, ErrStop) || !errors.Is(s.err, ErrOutputLimit) {
		t.Fatalf("large spool: err %v / %v, want ErrStop and ErrOutputLimit", err, s.err)
	}
}
