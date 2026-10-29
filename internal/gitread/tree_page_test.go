package gitread

// LsTreePage (#612) must page a recursive listing exactly as sorting the
// whole LsTree output and taking a window of it does, while git stops
// after the page and only the page counts against the output allowance.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// gitIn runs git in dir with stdin and returns its trimmed stdout.
func gitIn(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}

// pageRepo imports a fast-import stream into a new bare repo and returns
// it with the commit of refs/heads/main.
func pageRepo(t *testing.T, stream string) (dir, commit string) {
	t.Helper()
	dir = t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	gitIn(t, dir, stream, "fast-import", "--quiet")
	return dir, gitIn(t, dir, "", "rev-parse", "refs/heads/main")
}

// windowOf is the paging LsTreePage replaces: a window of the sorted tree.
func windowOf(sorted []TreeEntry, after string, limit int) ([]TreeEntry, bool) {
	start := sort.Search(len(sorted), func(i int) bool { return sorted[i].Path > after })
	end := min(start+limit, len(sorted))
	return sorted[start:end], end < len(sorted)
}

func TestLsTreePageMatchesSortedLsTree(t *testing.T) {
	// Names around '/' in byte order, a gitlink, a symlink, a space and
	// non-ASCII and non-UTF-8 bytes.
	names := []string{"a!", "a-b", "a.txt", "a/x", "a/y/z", "a0", "b", "s-1", "z z", "\xc3\xa9", "\xff",
		"dir/a-b", "dir/a/c", "dir/a.c"}
	var in strings.Builder
	in.WriteString("commit refs/heads/main\ncommitter C <c@example.com> 1700000000 +0000\ndata 0\n")
	for i, n := range names {
		c := fmt.Sprint(i)
		fmt.Fprintf(&in, "M 100644 inline %s\ndata %d\n%s\n", n, len(c), c)
	}
	in.WriteString("M 120000 inline l\ndata 1\nb\n")
	in.WriteString("M 160000 " + strings.Repeat("1", 40) + " s\n\n")
	dir, commit := pageRepo(t, in.String())
	ctx := context.Background()

	for _, path := range []string{"", "a/", "dir/"} {
		all, err := LsTree(ctx, dir, commit, path, true)
		if err != nil {
			t.Fatal(err)
		}
		sorted := slices.Clone(all)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
		if len(all) < 2 || !slices.Equal(all, sorted) {
			t.Fatalf("path %q: git listed %d entries out of byte order: %v", path, len(all), all)
		}
		afters := []string{"", "\x00", "a", "a/", "dir/a", "zzzz"}
		for _, e := range all {
			afters = append(afters, e.Path)
		}
		for _, after := range afters {
			for limit := 1; limit <= len(all)+1; limit++ {
				page, more, found, err := LsTreePage(ctx, dir, commit, path, after, limit)
				want, wantMore := windowOf(sorted, after, limit)
				if err != nil || !found || !slices.Equal(page, want) || more != wantMore {
					t.Fatalf("LsTreePage(%q, after %q, limit %d) = %v, more %v, found %v, err %v; want %v, more %v",
						path, after, limit, page, more, found, err, want, wantMore)
				}
			}
		}
	}

	page, more, found, err := LsTreePage(ctx, dir, commit, "nope/", "", 5)
	if err != nil || found || more || len(page) != 0 {
		t.Fatalf("missing path: %v, more %v, found %v, err %v; want nothing found", page, more, found, err)
	}
}

// git lists a tree's entries in their stored order. A tree stored out of
// order (fsck's treeNotSorted, which serve's receive-pack does not check)
// cannot be paged by stopping early, so LsTreePage refuses it.
func TestLsTreePageRefusesAnUnsortedTree(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	raw, err := hex.DecodeString(gitIn(t, dir, "x", "hash-object", "-w", "--stdin"))
	if err != nil {
		t.Fatal(err)
	}
	tree := gitIn(t, dir, "100644 b\x00"+string(raw)+"100644 a\x00"+string(raw),
		"hash-object", "-t", "tree", "--literally", "-w", "--stdin")
	commit := gitIn(t, dir, "", "commit-tree", tree, "-m", "unsorted")
	all, err := LsTree(context.Background(), dir, commit, "", true)
	if err != nil || len(all) != 2 || all[0].Path != "b" {
		t.Fatalf("control: LsTree = %v, %v; want b before a", all, err)
	}
	// The refused page charges nothing, so the caller's whole-tree
	// fallback gets the full allowance.
	for _, limit := range []int{1, 5} {
		ctx, b := budgeted(t, 1<<20)
		if _, _, _, err := LsTreePage(ctx, dir, commit, "", "", limit); !errors.Is(err, ErrUnsortedTree) {
			t.Fatalf("limit %d: err = %v, want ErrUnsortedTree", limit, err)
		}
		if left := b.remaining(); left != 1<<20 {
			t.Fatalf("limit %d: %d bytes of allowance left, want all %d", limit, left, 1<<20)
		}
	}
}

// budgeted returns a context whose Budget allows output bytes of
// buffered output.
func budgeted(t *testing.T, output int64) (context.Context, *Budget) {
	t.Helper()
	b := NewBudget(NewSlots(1, 1, 0), "repo", time.Minute, output)
	t.Cleanup(b.Close)
	return WithBudget(context.Background(), b), b
}

func TestLsTreePageCountsOnlyThePage(t *testing.T) {
	const n = 1002
	var in strings.Builder
	in.WriteString("commit refs/heads/main\ncommitter C <c@example.com> 1700000000 +0000\ndata 0\n")
	for i := range n {
		fmt.Fprintf(&in, "M 100644 inline d%02d/f%04d.txt\ndata 2\nx\n", i%10, i)
	}
	dir, commit := pageRepo(t, in.String()+"\n")
	all, err := LsTree(context.Background(), dir, commit, "", true)
	if err != nil || len(all) != n {
		t.Fatalf("LsTree: %d entries, %v", len(all), err)
	}
	recLen := map[string]int{}
	for rec := range strings.SplitSeq(gitIn(t, dir, "", "ls-tree", "--long", "-z", "-r", commit), "\x00") {
		_, path, _ := strings.Cut(rec, "\t")
		recLen[path] = len(rec)
	}
	// 4 KiB holds a page of 10 records (about 80 bytes each), not the tree.
	const allowance = 4 << 10
	ctx, _ := budgeted(t, allowance)
	if _, err := LsTree(ctx, dir, commit, "", true); !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("control: whole listing err = %v, want ErrOutputLimit", err)
	}
	// A deep cursor walks the whole prefix, whose records git splits
	// across writes, but charges only the page.
	ctx, b := budgeted(t, allowance)
	page, more, _, err := LsTreePage(ctx, dir, commit, "", all[n-3].Path, 10)
	if err != nil || !slices.Equal(page, all[n-2:]) || more {
		t.Fatalf("deep cursor: %v, more %v, err %v; want the last 2 entries", page, more, err)
	}
	if used, want := allowance-b.remaining(), int64(recLen[all[n-2].Path]+recLen[all[n-1].Path]); used != want {
		t.Fatalf("deep cursor charged %d bytes, want the page's %d", used, want)
	}
	ctx, b = budgeted(t, allowance)
	if _, _, _, err := LsTreePage(ctx, dir, commit, "", "", 1000); !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("1000-entry page err = %v, want ErrOutputLimit: the page itself is counted", err)
	}
	if left := b.remaining(); left != allowance {
		t.Fatalf("refused page charged %d bytes, want none", allowance-left)
	}
}

// The pager reads one record past the page (has_more) and then stops git,
// also when records arrive split across writes.
func TestLsTreePagerStopsAfterThePage(t *testing.T) {
	rec := func(p string) string { return "100644 blob " + strings.Repeat("a", 40) + "       1\t" + p + "\x00" }
	head := rec("a") + rec("b") + rec("c") + rec("d")
	stream := head + rec("e")
	// Records before the cursor, split byte by byte, charge nothing.
	_, b := budgeted(t, 1<<10)
	p := &lsTreePager{after: "a", limit: 2, budget: b}
	stopped := -1
	for i := range len(stream) {
		if _, err := p.Write([]byte{stream[i]}); errors.Is(err, ErrStop) {
			stopped = i
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if stopped != len(head)-1 || p.err != nil || !p.hasMore || len(p.page) != 2 || p.page[0].Path != "b" || p.page[1].Path != "c" {
		t.Fatalf("stopped at byte %d (want %d), err %v, more %v, page %v", stopped, len(head)-1, p.err, p.hasMore, p.page)
	}
	if left := b.remaining(); left != 1<<10 || p.kept != 2*(len(rec("b"))-1) {
		t.Fatalf("pager charged %d bytes and kept %d; want none charged and the 2 page records kept", 1<<10-left, p.kept)
	}
}
