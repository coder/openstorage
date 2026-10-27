package gitread

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
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
				got, err := LastCommits(ctx, dir, rev, paths, 1000)
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
		t.Fatalf("checked subtrees %v, want all three", subtrees)
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
	got, err := LastCommits(ctx, f.dir, f.c2, paths, 2)
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
	want, err := LastCommits(ctx, dir, rev, paths, 1000)
	if err != nil {
		t.Fatal(err)
	}
	revList := mustGit(t, dir, "rev-list", "--topo-order", "--parents", rev)
	var pairs strings.Builder
	if err := feedPairs(&pairs, strings.NewReader(revList)); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("git", "diff-tree", "--stdin", "--root", "--always", "-r", "--raw", "-z", "--no-renames")
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
