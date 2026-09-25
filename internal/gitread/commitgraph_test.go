package gitread

// Commit-graph equivalence (issue #307, ADR-47): cache repos now carry a
// split commit-graph with changed-path Bloom filters. The graph must change
// only speed, never answers, so every read below is compared with Git's
// no-graph answer over the same repo state: a fresh graph, a stale graph
// that misses new commits, mixed Bloom/plain layers, and a compacted chain.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// cgGit runs git in dir with the package's pinned identity.
func cgGit(tb testing.TB, dir string, args ...string) string {
	tb.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		tb.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

const nonASCIIPath = "dír/ünïcödé.txt"

// hotContent is hot.txt at step i: ten lines, line j naming the last step
// (<= i) that rewrote it, so blame has real per-line history.
func hotContent(i int) string {
	var b strings.Builder
	for j := range 10 {
		k := i - ((i-j)%10+10)%10
		if k < 0 {
			fmt.Fprintf(&b, "line %d: init\n", j)
		} else {
			fmt.Fprintf(&b, "line %d: step %d\n", j, k)
		}
	}
	return b.String()
}

// appendGraphHistory writes steps [from, to) with fast-import: commits on
// main and side, a merge of side into main every sixth step, committer
// dates that jump backwards every eleventh step (clock skew), a non-ASCII
// path, a directory, a file deleted and re-added, and a root-only file.
func appendGraphHistory(tb testing.TB, dir string, from, to int) {
	tb.Helper()
	var in strings.Builder
	data := func(path, content string) {
		fmt.Fprintf(&in, "M 100644 inline %s\ndata %d\n%s", path, len(content), content)
	}
	if from > 0 {
		in.WriteString("reset refs/heads/main\nfrom refs/heads/main^0\n\nreset refs/heads/side\nfrom refs/heads/side^0\n\n")
	}
	for i := from; i < to; i++ {
		branch := "main"
		if i%3 == 1 {
			branch = "side"
		}
		date := 1700000000 + i*100
		if i%11 == 0 {
			date -= 5 * 86400
		}
		msg := fmt.Sprintf("step %d\n", i)
		fmt.Fprintf(&in, "commit refs/heads/%s\ncommitter C <c@example.com> %d +0000\ndata %d\n%s", branch, date, len(msg), msg)
		if branch == "main" && i%6 == 5 {
			in.WriteString("merge refs/heads/side\n")
		}
		data("hot.txt", hotContent(i))
		data(fmt.Sprintf("file_%d.txt", i%17), msg)
		if i%7 == 0 {
			data(nonASCIIPath, msg)
		}
		if i%13 == 0 {
			data("a/b/deep.txt", msg)
		}
		switch i {
		case 0:
			data("root-only.txt", "root\n")
			data("gone.txt", "first\n")
		case 150:
			in.WriteString("D gone.txt\n")
		case 250:
			data("gone.txt", "back\n")
		}
		if i == 0 {
			in.WriteString("\nreset refs/heads/side\nfrom refs/heads/main\n")
		}
		in.WriteString("\n")
	}
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("fast-import: %v\n%s", err, out)
	}
}

// graphReads renders every read whose answer a commit-graph could affect.
func graphReads(t *testing.T, dir string) string {
	t.Helper()
	ctx := context.Background()
	mainTip := strings.TrimSpace(cgGit(t, dir, "rev-parse", "main"))
	sideTip := strings.TrimSpace(cgGit(t, dir, "rev-parse", "side"))
	var b strings.Builder
	logs := func(label, rev string, opts LogOptions) {
		c, err := Log(ctx, dir, rev, opts)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		fmt.Fprintf(&b, "%s:", label)
		for _, x := range c {
			fmt.Fprintf(&b, " %s%v", x.SHA, x.ParentSHAs)
		}
		b.WriteString("\n")
	}
	logs("log", mainTip, LogOptions{})
	logs("log-page", mainTip, LogOptions{Skip: 37, MaxCount: 20})
	for _, p := range []string{"hot.txt", nonASCIIPath, "a/b/deep.txt", "a", "gone.txt", "root-only.txt", "file_3.txt", "missing.txt"} {
		logs("log-1 "+p, mainTip, LogOptions{Path: p, MaxCount: 1})
		logs("log "+p, mainTip, LogOptions{Path: p})
	}
	logs("side log hot.txt", sideTip, LogOptions{Path: "hot.txt"})
	for _, p := range []string{"hot.txt", nonASCIIPath} {
		lines, err := Blame(ctx, dir, mainTip, p, nil, false)
		if err != nil {
			t.Fatalf("blame %s: %v", p, err)
		}
		fmt.Fprintf(&b, "blame %s:", p)
		for _, l := range lines {
			fmt.Fprintf(&b, " %d:%s:%d:%s", l.LineNumber, l.CommitSHA, l.OriginalLineNumber, l.PreviousCommitSHA)
		}
		b.WriteString("\n")
	}
	for _, args := range [][]string{
		{"rev-list", "--topo-order", mainTip},
		{"rev-list", "--date-order", mainTip},
		{"rev-list", "--count", mainTip},
		{"merge-base", "--all", mainTip, sideTip},
		{"rev-list", "--topo-order", mainTip, "--", "hot.txt"},
	} {
		out, err := runGit(ctx, Heavy, dir, args...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		fmt.Fprintf(&b, "%s: %s\n", strings.Join(args[:2], " "), strings.Join(strings.Fields(out), " "))
	}
	return b.String()
}

// assertGraphEquivalent compares the reads with and without the graph
// (core.commitGraph=false through the environment, which every gitread
// subprocess inherits).
func assertGraphEquivalent(t *testing.T, dir, state string) {
	t.Helper()
	withGraph := graphReads(t, dir)
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.commitGraph")
	t.Setenv("GIT_CONFIG_VALUE_0", "false")
	noGraph := graphReads(t, dir)
	// Not vacuous: under the override a path walk must not touch the graph.
	if bloomStatistics(t, dir) {
		t.Fatalf("%s: core.commitGraph=false override did not disable the graph", state)
	}
	os.Unsetenv("GIT_CONFIG_COUNT")
	os.Unsetenv("GIT_CONFIG_KEY_0")
	os.Unsetenv("GIT_CONFIG_VALUE_0")
	if withGraph != noGraph {
		t.Fatalf("%s: reads differ with the commit-graph\n--- with graph\n%s\n--- without\n%s", state, withGraph, noGraph)
	}
}

// bloomStatistics reports whether a traced path walk (under the current
// environment) ruled commits out with changed-path Bloom filters.
func bloomStatistics(t *testing.T, dir string) bool {
	t.Helper()
	trace := filepath.Join(t.TempDir(), "trace.json")
	cmd := exec.Command("git", "log", "-1", "--format=%H", "main", "--", "root-only.txt")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TRACE2_EVENT="+trace)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("traced log: %v\n%s", err, out)
	}
	ev, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Contains(string(ev), `"definitely_not":`) && !strings.Contains(string(ev), `"definitely_not":0,`)
}

func TestReadsIdenticalWithCommitGraph(t *testing.T) {
	dir := t.TempDir()
	cgGit(t, dir, "init", "-q", "--bare")
	appendGraphHistory(t, dir, 0, 300)
	write := func(args ...string) {
		cgGit(t, dir, append([]string{"commit-graph", "write", "--reachable", "--no-progress"}, args...)...)
	}

	write("--split", "--changed-paths")
	assertGraphEquivalent(t, dir, "fresh Bloom graph")
	// Not vacuous: the path walk actually consulted Bloom filters.
	if !bloomStatistics(t, dir) {
		t.Fatal("path walk did not use Bloom filters")
	}

	appendGraphHistory(t, dir, 300, 340)
	assertGraphEquivalent(t, dir, "stale graph missing 40 steps")

	write("--split", "--no-changed-paths")
	assertGraphEquivalent(t, dir, "mixed Bloom and plain layers")

	appendGraphHistory(t, dir, 340, 350)
	write("--split=replace")
	assertGraphEquivalent(t, dir, "compacted chain")
}

// BenchmarkLogPathCommitGraph measures the per-file `git log -1 -- <path>`
// that files/metadata runs for every listed entry, without a graph, with a
// plain graph, and with the Bloom-filter graph a fresh cache repo gets
// (ADR-47). 20,000 commits touch 3 of ~1,000 files under 50 directories;
// cold.txt exists only in the root commit, so its walk is the worst case.
func BenchmarkLogPathCommitGraph(b *testing.B) {
	const n = 20000
	dir := b.TempDir()
	cgGit(b, dir, "init", "-q", "--bare")
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\ncommitter B <b@example.com> %d +0000\ndata 2\nc\n", 1700000000+i)
		if i == 0 {
			in.WriteString("M 100644 inline cold.txt\ndata 5\ncold\n")
		}
		for k := range 3 {
			content := fmt.Sprintf("%d %d\n", i, k)
			fmt.Fprintf(&in, "M 100644 inline d%d/f%d.txt\ndata %d\n%s", (i+k)%50, (i*3+k)%997, len(content), content)
		}
		content := fmt.Sprintf("%d\n", i)
		fmt.Fprintf(&in, "M 100644 inline hot.txt\ndata %d\n%s", len(content), content)
	}
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("fast-import: %v\n%s", err, out)
	}
	tip := strings.TrimSpace(cgGit(b, dir, "rev-parse", "main"))
	root := strings.TrimSpace(cgGit(b, dir, "rev-list", "--max-parents=0", "main"))
	ctx := context.Background()
	logPath := func(name, path, want string) {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				c, err := Log(ctx, dir, tip, LogOptions{Path: path, MaxCount: 1})
				if err != nil || len(c) != 1 || c[0].SHA != want {
					b.Fatalf("log = %v, err = %v", c, err)
				}
			}
		})
	}
	graphs := filepath.Join(dir, "objects", "info", "commit-graphs")
	logPath("no-graph/hot", "hot.txt", tip)
	logPath("no-graph/cold", "cold.txt", root)
	cgGit(b, dir, "commit-graph", "write", "--reachable", "--split", "--no-changed-paths", "--no-progress")
	logPath("plain-graph/hot", "hot.txt", tip)
	logPath("plain-graph/cold", "cold.txt", root)
	if err := os.RemoveAll(graphs); err != nil {
		b.Fatal(err)
	}
	cgGit(b, dir, "commit-graph", "write", "--reachable", "--split", "--changed-paths", "--no-progress")
	logPath("bloom-graph/hot", "hot.txt", tip)
	logPath("bloom-graph/cold", "cold.txt", root)
}
