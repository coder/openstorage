package gitread

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
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
	paths := []string{"a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt", "g*.txt", "gx.txt", "g?.txt", "[x].txt", "x.txt", "[d]/e.txt", "[d]/f.txt", ":(attr:x)p/q.txt", "df", "df/in", "link", "mod"}
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
			fmt.Fprintf(&in, "M %s inline %s\ndata %d\n%s\n", f.mode, p, len(f.data), f.data)
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

// lastCommitsByPath is LastCommits keyed by path.
func lastCommitsByPath(ctx context.Context, dir, rev string, paths []string, maxDistinct int) (map[string]string, error) {
	last, err := LastCommits(ctx, dir, rev, paths, maxDistinct)
	if err != nil {
		return nil, err
	}
	if len(last) != len(paths) {
		return nil, fmt.Errorf("%d answers for %d paths", len(last), len(paths))
	}
	m := make(map[string]string, len(paths))
	for i, p := range paths {
		m[p] = last[i]
	}
	return m, nil
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
				got, err := lastCommitsByPath(ctx, dir, rev, paths, 1000)
				if err != nil {
					t.Fatalf("seed %d rev %s sub %q: %v", seed, rev, sub, err)
				}
				if want := lcOracle(t, dir, rev, paths); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("seed %d rev %s sub %q:\n got %v\nwant %v", seed, rev, sub, got, want)
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
	got, err := lastCommitsByPath(ctx, f.dir, f.c2, paths, 2)
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
	if _, err := LastCommits(ctx, f.dir, f.c2, paths, 1); !errors.Is(err, ErrTooManyLastCommits) {
		t.Fatalf("2 distinct commits with limit 1: err = %v, want ErrTooManyLastCommits", err)
	}
}

func TestLastCommitsRejectsPathNotInRev(t *testing.T) {
	f := newFixture(t)
	// feature.txt exists only on the feature branch (c3), not at c2.
	_, err := LastCommits(context.Background(), f.dir, f.c2, []string{"README.md", "feature.txt"}, 10)
	if err == nil || !strings.Contains(err.Error(), "feature.txt") {
		t.Fatalf("err = %v, want an error naming feature.txt", err)
	}
}

func TestLastCommitsNoPathsRunsNoGit(t *testing.T) {
	got, err := LastCommits(context.Background(), "/nonexistent/repo", strings.Repeat("a", 40), nil, 1)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("got %#v, err %v; want an empty non-nil map", got, err)
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
		want, err := Log(ctx, f.dir, sha, LogOptions{MaxCount: 1})
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

// TestLastCommitWalkFieldsSplitAcrossWrites feeds real diff-tree output to
// the walk one byte per Write: pipes split fields anywhere.
func TestLastCommitWalkFieldsSplitAcrossWrites(t *testing.T) {
	ctx := context.Background()
	dir, shas := lcHistory(t, 7, 25)
	rev := shas[len(shas)-1]
	entries, err := LsTree(ctx, dir, rev, "", true)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, e := range entries {
		paths = append(paths, e.Path)
	}
	want, err := lastCommitsByPath(ctx, dir, rev, paths, 1000)
	if err != nil {
		t.Fatal(err)
	}
	revList := mustGit(t, dir, "rev-list", "--topo-order", "--parents", rev)
	var pairs strings.Builder
	if err := feedPairs(&pairs, strings.NewReader(revList)); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("git", "diff-tree", "--stdin", "--root", "--always", "-r", "--raw", "-z", "--no-renames", "--ignore-submodules=none")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(pairs.String())
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	w := newLastCommitWalk(rev, paths, 1000)
	w.expect = bufio.NewReader(strings.NewReader(revList))
	for i := range out {
		if _, err := w.Write(out[i : i+1]); err != nil {
			break // ErrStop: done, or w.err
		}
	}
	if w.err == nil && w.left > 0 {
		if err := w.finish(); err != nil && !errors.Is(err, ErrStop) {
			t.Fatal(err)
		}
	}
	if w.err != nil || w.left != 0 {
		t.Fatalf("walk err %v, %d paths unresolved", w.err, w.left)
	}
	for i, p := range paths {
		if w.last[i] != want[p] {
			t.Errorf("%s = %s, want %s", p, w.last[i], want[p])
		}
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
	got, err := lastCommitsByPath(context.Background(), dir, tip, []string{".gitmodules", "mod"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if root := strings.TrimSpace(mustGit(t, dir, "rev-parse", "main^")); got["mod"] != tip || got[".gitmodules"] != root {
		t.Fatalf("got %v, want mod at the tip %s and .gitmodules at the root %s", got, tip, root)
	}
}

// TestLastCommitsStopsEarlyWithoutLeaks: every path resolves at the tip of
// a long history, so the walk stops while the feeder still has most of
// the rev-list to write. The call must return, end its goroutine, and
// remove its spool.
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
	got, err := lastCommitsByPath(context.Background(), dir, tip, []string{"hot.txt"}, 1)
	if err != nil || got["hot.txt"] != tip {
		t.Fatalf("got %v, err %v; want hot.txt at the tip", got, err)
	}
	if left, err := os.ReadDir(spools); err != nil || len(left) != 0 {
		t.Fatalf("temp dir holds %v (err %v), want no spool", left, err)
	}
	// The feeder ends once its write fails; give the scheduler a moment.
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
		got, err := lastCommitsByPath(context.Background(), dir, tip, paths, 10)
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
