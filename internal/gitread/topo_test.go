package gitread

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// gitAt runs git in dir with the pinned gitEnv plus per-call author and
// committer dates, so one fixture can disagree with its own topology.
func gitAt(t testing.TB, dir, authorDate, committerDate string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(append(os.Environ(), gitEnv...),
		"GIT_AUTHOR_DATE="+authorDate, "GIT_COMMITTER_DATE="+committerDate)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func TestTopoRanksLinear(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	ranks, err := TopoRanks(ctx, f.dir, f.c2, []string{f.c1, f.c2, f.c1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{f.c2: 0, f.c1: 1}
	if fmt.Sprint(ranks) != fmt.Sprint(want) {
		t.Fatalf("ranks = %v, want %v", ranks, want)
	}

	// Ranks are relative to rev: from the feature tip, c1 is one step back.
	ranks, err = TopoRanks(ctx, f.dir, f.c3, []string{f.c1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranks) != 1 || ranks[f.c1] != 1 {
		t.Fatalf("ranks from feature = %v, want c1:1", ranks)
	}
}

func TestTopoRanksUnreachableCommitErrors(t *testing.T) {
	f := newFixture(t)
	// c3 (feature) is not an ancestor of c2 (main).
	if _, err := TopoRanks(context.Background(), f.dir, f.c2, []string{f.c1, f.c3}, 0); err == nil ||
		!strings.Contains(err.Error(), f.c3) {
		t.Fatalf("err = %v, want unreachable %s", err, f.c3)
	}
}

func TestTopoRanksEmptyWantRunsNoGit(t *testing.T) {
	// A nonexistent repo path proves no git process runs.
	ranks, err := TopoRanks(context.Background(), "/nonexistent/repo", strings.Repeat("a", 40), nil, 0)
	if err != nil || ranks == nil || len(ranks) != 0 {
		t.Fatalf("ranks = %#v, err = %v, want empty non-nil map", ranks, err)
	}
}

// TestTopoRanksMergeIgnoresDates pins the documented tie-break on a merge
// whose author AND committer dates both disagree with the topology:
//
//	base -- m1 ----------- M (main)
//	    \-- s1 -- s2 (side) /
//
// Author dates: s1 2030 > s2 2029 > M 2025 > base 2020 > m1 2001.
// Committer dates: m1 2031 > M 2025 > base 2020 > s2 2002 > s1 2001.
// Expected: M first, then the merge's last parent's line (s2, s1), then the
// first parent's line (m1), then base once all its children are ranked.
// The ranks must not change when a commit-graph lets git walk
// incrementally.
func TestTopoRanksMergeIgnoresDates(t *testing.T) {
	dir := t.TempDir()
	commit := func(msg, authorDate, committerDate string) string {
		gitAt(t, dir, authorDate, committerDate, "commit", "-q", "--allow-empty", "-m", msg)
		return strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	}
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	base := commit("base", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
	mustGit(t, dir, "checkout", "-q", "-b", "side")
	s1 := commit("s1", "2030-01-01T00:00:00Z", "2001-01-01T00:00:00Z")
	s2 := commit("s2", "2029-01-01T00:00:00Z", "2002-01-01T00:00:00Z")
	mustGit(t, dir, "checkout", "-q", "main")
	m1 := commit("m1", "2001-01-01T00:00:00Z", "2031-01-01T00:00:00Z")
	gitAt(t, dir, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z", "merge", "-q", "--no-ff", "-m", "M", "side")
	merge := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))

	want := map[string]int{merge: 0, s2: 1, s1: 2, m1: 3, base: 4}
	all := []string{base, m1, s1, s2, merge}
	check := func(label string) {
		t.Helper()
		ranks, err := TopoRanks(context.Background(), dir, merge, all, 0)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(ranks) != fmt.Sprint(want) {
			t.Fatalf("%s: ranks = %v, want %v", label, ranks, want)
		}
	}
	check("no commit-graph")
	mustGit(t, dir, "commit-graph", "write", "--reachable")
	check("with commit-graph")

	// Ranks count every commit ahead in the walk, including ones not
	// asked for: base alone still ranks 4.
	ranks, err := TopoRanks(context.Background(), dir, merge, []string{base}, 0)
	if err != nil || ranks[base] != 4 {
		t.Fatalf("base alone: ranks = %v, err = %v, want base:4", ranks, err)
	}
}

// benchHistory builds a linear history of n commits with fast-import. Every
// commit rewrites hot.txt; cold.txt exists only in the root commit.
func benchHistory(b *testing.B, n int) (dir, tip, root string) {
	b.Helper()
	dir = b.TempDir()
	mustGit(b, dir, "init", "-q", "--bare")
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\nmark :%d\ncommitter B <b@example.com> %d +0000\ndata 2\nc\n", i+1, 1700000000+i)
		if i > 0 {
			fmt.Fprintf(&in, "from :%d\n", i)
		} else {
			in.WriteString("M 100644 inline cold.txt\ndata 5\ncold\n")
		}
		fmt.Fprintf(&in, "M 100644 inline hot.txt\ndata %d\n%d\n", len(fmt.Sprint(i))+1, i)
	}
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("fast-import: %v\n%s", err, out)
	}
	tip = strings.TrimSpace(mustGit(b, dir, "rev-parse", "main"))
	root = strings.TrimSpace(mustGit(b, dir, "rev-list", "--max-parents=0", "main"))
	return dir, tip, root
}

// BenchmarkTopoRanks measures the per-page order walk that files/metadata
// adds, next to the per-file `git log -1 -- <path>` it already runs for
// every page entry. Without a commit-graph (open.storage writes none) git must
// read the whole history before it prints the first rank, so "tip" and
// "root" cost about the same; with a commit-graph the walk stops at the
// deepest requested commit.
func BenchmarkTopoRanks(b *testing.B) {
	const n = 20000
	dir, tip, root := benchHistory(b, n)
	ctx := context.Background()
	run := func(name, want string, wantRank int) {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				ranks, err := TopoRanks(ctx, dir, tip, []string{want}, 0)
				if err != nil || ranks[want] != wantRank {
					b.Fatalf("ranks = %v, err = %v", ranks, err)
				}
			}
		})
	}
	logPath := func(name, path, wantSHA string) {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				c, err := Log(ctx, dir, tip, LogOptions{Path: path, MaxCount: 1})
				if err != nil || len(c) != 1 || c[0].SHA != wantSHA {
					b.Fatalf("log = %v, err = %v", c, err)
				}
			}
		})
	}
	run("no-graph/tip", tip, 0)
	run("no-graph/root", root, n-1)
	logPath("no-graph/log-1-hot-path", "hot.txt", tip)
	logPath("no-graph/log-1-cold-path", "cold.txt", root)
	mustGit(b, dir, "commit-graph", "write", "--reachable")
	run("graph/tip", tip, 0)
	run("graph/root", root, n-1)
}
