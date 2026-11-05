package gitread

// LsTreePage with a cursor (#615) against git: the same pages as sorting
// the whole `ls-tree -r` listing and taking a window, and as the streaming
// listing it replaces (streamTreePage), for every cursor of a tree with
// nested folders and awkward names.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// fiFile is a file of a fast-import stream: mode 100644, 100755 or 120000
// with data, or 160000 with a commit id as data.
type fiFile struct{ mode, path, data string }

// fiQuote quotes a path the way fast-import reads it: C-style.
func fiQuote(p string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for i := range len(p) {
		switch c := p[i]; {
		case c == '"' || c == '\\':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case c < 0x20 || c >= 0x7f:
			fmt.Fprintf(&sb, "\\%03o", c)
		default:
			sb.WriteByte(c)
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

func fiRepo(t testing.TB, files []fiFile) (dir, commit string) {
	t.Helper()
	var in strings.Builder
	in.WriteString("commit refs/heads/main\ncommitter C <c@example.com> 1700000000 +0000\ndata 0\n")
	for _, f := range files {
		if f.mode == "160000" {
			fmt.Fprintf(&in, "M 160000 %s %s\n", f.data, fiQuote(f.path))
			continue
		}
		fmt.Fprintf(&in, "M %s inline %s\ndata %d\n%s\n", f.mode, fiQuote(f.path), len(f.data), f.data)
	}
	in.WriteString("\n")
	return pageRepo(t, in.String())
}

// nestedFiles is a tree with folders inside folders, names on both sides of
// '/' in byte order, a newline, non-UTF-8 and multi-byte names, a symlink, a
// gitlink and an executable.
func nestedFiles() []fiFile {
	var files []fiFile
	for i, p := range []string{
		"a!", "a-b", "a.txt", "a/x", "a/y/z", "a0", "b", "s-1", "z z", "\xc3\xa9", "\xff", "new\nline", "empty",
		"dir/a-b", "dir/a/c", "dir/a.c", "dir/b/c/d/e", "dir/b/c/d.txt", "dir/b/c-d", "dir/b0",
		"dir/new\nline/x", "dir/new\nline-y", "dir/\xff/q", "dir/\xfe",
		"deep/1/2/3/4/f", "deep/1/2/3/4/g", "deep/1/2/h", "deep/1/z",
	} {
		data := strings.Repeat("x", i%5)
		if p == "empty" {
			data = ""
		}
		files = append(files, fiFile{"100644", p, data})
	}
	return append(files,
		fiFile{"100755", "e.sh", "#!/bin/sh\n"},
		fiFile{"120000", "l", "b"},
		fiFile{"120000", "dir/lnk", "a-b"},
		fiFile{"160000", "sub", strings.Repeat("1", 40)},
		fiFile{"160000", "dir/sub2", strings.Repeat("2", 40)},
	)
}

// pageOracle is LsTreePage as it was before the walk: the stream.
func pageOracle(ctx context.Context, dir, commit, path, after string, limit int) ([]TreeEntry, bool, bool, error) {
	return streamTreePage(ctx, dir, commit, path, after, limit, BudgetFrom(ctx))
}

func sortedListing(t testing.TB, dir, commit, path string) []TreeEntry {
	t.Helper()
	all, err := LsTree(context.Background(), dir, commit, path, true)
	if err != nil {
		t.Fatal(err)
	}
	sorted := slices.Clone(all)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	if !slices.Equal(all, sorted) {
		t.Fatalf("path %q: git listed %d entries out of byte order", path, len(all))
	}
	return sorted
}

func TestResumeTreePageMatchesGit(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	ctx := context.Background()
	var calls int
	for _, path := range []string{"", "a/", "dir/", "dir/a/", "dir/b/c/", "dir/b/c/d/", "dir/new\nline/", "dir/\xff/", "deep/1/2/", "deep/1/2/3/4/"} {
		sorted := sortedListing(t, dir, commit, path)
		if len(sorted) == 0 {
			t.Fatalf("path %q lists nothing", path)
		}
		afters := []string{"\x00", "a", "a/", "a/y", "dir", "dir/", "dir/b", "dir/b/c", "dir/b/c/d", "dir/b/c/d/", "dir/b/c/d/e\x00", "deep/1/2/3", "zzzz", "\xff\xff", path, strings.TrimSuffix(path, "/")}
		for _, e := range sorted {
			afters = append(afters, e.Path)
		}
		for _, after := range afters {
			if after == "" {
				continue
			}
			for _, limit := range []int{1, 3, len(sorted) + 1} {
				want, wantMore := windowOf(sorted, after, limit)
				// The walk handles every cursor here, a cursor that no
				// entry has too.
				page, more, found, err := resumeTreePage(ctx, dir, commit, path, after, limit)
				if err != nil || !found && len(want) > 0 || !slices.Equal(page, want) || more != wantMore {
					t.Fatalf("resumeTreePage(%q, after %q, limit %d) = %v, more %v, found %v, err %v\nwant %v, more %v",
						path, after, limit, paths(page), more, found, err, paths(want), wantMore)
				}
				if len(want) == 0 && !found {
					t.Fatalf("resumeTreePage(%q, after %q, limit %d): path matched nothing", path, after, limit)
				}
				calls++
			}
		}
	}
	t.Logf("%d pages", calls)
}

// Whichever way a page is read, the pages of a walk chain into the listing.
func TestLsTreePageCursorWalk(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	ctx := context.Background()
	for _, path := range []string{"", "dir/"} {
		sorted := sortedListing(t, dir, commit, path)
		for _, limit := range []int{1, 2, 3, 7, 1000} {
			var walked []TreeEntry
			after := ""
			for pages := 0; ; pages++ {
				if pages > len(sorted)+1 {
					t.Fatalf("path %q limit %d: never ended", path, limit)
				}
				page, more, found, err := LsTreePage(ctx, dir, commit, path, after, limit)
				if err != nil || !found {
					t.Fatalf("path %q limit %d after %q: %v, found %v", path, limit, after, err, found)
				}
				want, wantMore := windowOf(sorted, after, limit)
				if !slices.Equal(page, want) || more != wantMore {
					t.Fatalf("path %q limit %d after %q: %v, more %v; want %v, more %v", path, limit, after, paths(page), more, paths(want), wantMore)
				}
				walked = append(walked, page...)
				if !more {
					break
				}
				after = page[len(page)-1].Path
			}
			if !slices.Equal(walked, sorted) {
				t.Fatalf("path %q limit %d: pages hold %d entries, the listing %d", path, limit, len(walked), len(sorted))
			}
		}
	}
}

// What the walk leaves to the stream gives the stream's answer: paths git
// reads its own way, a folder that is not one, trees git does not write.
func TestLsTreePageCursorFallsBack(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	ctx := context.Background()
	for _, path := range []string{"dir", "dir//", "./dir/", "dir/../dir/", "nope/", "b/", "l/", "sub/", "dir/sub2/", "a/x/", "dir/./a/", "../dir/", "a\x00/"} {
		for _, after := range []string{"a", "dir/a", "dir/b/c/d.txt", "zzz", path} {
			for _, limit := range []int{2, 100} {
				page, more, found, err := LsTreePage(ctx, dir, commit, path, after, limit)
				wp, wm, wf, werr := pageOracle(ctx, dir, commit, path, after, limit)
				if !slices.Equal(page, wp) || more != wm || found != wf || (err == nil) != (werr == nil) {
					t.Fatalf("LsTreePage(%q, after %q, limit %d) = %v, %v, %v, %v\nstream: %v, %v, %v, %v", path, after, limit,
						paths(page), more, found, err, paths(wp), wm, wf, werr)
				}
			}
		}
	}
}

// rawTree stores a tree object with exactly these entries, in this order,
// and returns its id. A "40000" entry names a tree id; any other names a blob.
func rawTree(t testing.TB, dir string, entries ...[2]string) string {
	t.Helper()
	var raw strings.Builder
	for _, e := range entries {
		mode, name, ok := strings.Cut(e[0], " ")
		if !ok {
			t.Fatalf("bad entry %q", e[0])
		}
		id := e[1]
		if mode != "40000" && mode != "160000" {
			id = gitIn(t, dir, e[1], "hash-object", "-w", "--stdin")
		}
		b := make([]byte, 20)
		for i := range b {
			v, err := strconv.ParseUint(id[2*i:2*i+2], 16, 8)
			if err != nil {
				t.Fatal(err)
			}
			b[i] = byte(v)
		}
		raw.WriteString(mode + " " + name + "\x00" + string(b))
	}
	return gitIn(t, dir, raw.String(), "hash-object", "-t", "tree", "--literally", "-w", "--stdin")
}

func commitOf(t testing.TB, dir, tree string) string {
	t.Helper()
	return gitIn(t, dir, "", "commit-tree", tree, "-m", "t")
}

// Trees that git accepts and fsck rejects, or writes in an old way: the
// answers stay the stream's, errors included.
func TestLsTreePageCursorOnOddTrees(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	ctx := context.Background()
	empty := gitIn(t, dir, "", "hash-object", "-t", "tree", "-w", "--stdin")
	sortedSub := rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 b", "2"})
	unsortedSub := rawTree(t, dir, [2]string{"100644 y", "1"}, [2]string{"100644 x", "2"})
	longName := strings.Repeat("n", treeReadBuffer+10)
	trees := map[string]string{
		"unsorted root":        rawTree(t, dir, [2]string{"100644 c", "1"}, [2]string{"100644 b", "2"}, [2]string{"100644 a", "3"}),
		"unsorted nested":      rawTree(t, dir, [2]string{"40000 d", sortedSub}, [2]string{"40000 e", unsortedSub}, [2]string{"100644 f", "4"}),
		"folder before a-b":    rawTree(t, dir, [2]string{"40000 a", sortedSub}, [2]string{"100644 a-b", "1"}),
		"duplicate names":      rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 a", "2"}),
		"blob and folder of a": rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"40000 a", sortedSub}),
		"mode 100664":          rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100664 b", "2"}),
		"padded folder mode":   rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"040000 b", sortedSub}),
		"empty folders":        rawTree(t, dir, [2]string{"40000 a", empty}, [2]string{"40000 b", sortedSub}, [2]string{"40000 c", empty}, [2]string{"100644 d", "5"}),
		"only empty folders":   rawTree(t, dir, [2]string{"40000 a", empty}, [2]string{"40000 b", empty}),
		"name past the buffer": rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 " + longName, "2"}, [2]string{"100644 z", "3"}),
	}
	for name, tree := range trees {
		commit := commitOf(t, dir, tree)
		for _, path := range []string{"", "d/"} {
			all, lerr := LsTree(ctx, dir, commit, path, true)
			truth := slices.Clone(all)
			sort.Slice(truth, func(i, j int) bool { return truth[i].Path < truth[j].Path })
			for _, after := range []string{"a", "b/a", "d/a", longName, "z"} {
				for _, limit := range []int{1, 10} {
					page, more, found, err := LsTreePage(ctx, dir, commit, path, after, limit)
					wp, wm, wf, werr := pageOracle(ctx, dir, commit, path, after, limit)
					tp, tm := windowOf(truth, after, limit)
					// The answer is the stream's, or the listing sorted when
					// the stream sees a tree out of order and the walk does not
					// read it, or a refusal of an unsorted tree.
					same := (err == nil) == (werr == nil) && errors.Is(err, ErrUnsortedTree) == errors.Is(werr, ErrUnsortedTree) &&
						(err != nil || slices.Equal(page, wp) && more == wm && found == wf)
					sorted := err == nil && lerr == nil && slices.Equal(page, tp) && more == tm && found == (len(all) > 0)
					if !same && !sorted && !errors.Is(err, ErrUnsortedTree) {
						t.Fatalf("%s: LsTreePage(%q, after %.12q, limit %d) = %v, %v, %v, %v\nstream: %v, %v, %v, %v\nsorted: %v, %v", name, path, after, limit,
							paths(page), more, found, err, paths(wp), wm, wf, werr, paths(tp), tm)
					}
				}
			}
		}
	}
	// The walk declines the trees it cannot read, and reads the rest.
	for name, wantDecline := range map[string]bool{
		"unsorted root": true, "unsorted nested": true, "folder before a-b": true, "duplicate names": true,
		"mode 100664": true, "padded folder mode": true, "name past the buffer": true,
		"blob and folder of a": false, "empty folders": false,
	} {
		commit := commitOf(t, dir, trees[name])
		_, _, _, err := resumeTreePage(ctx, dir, commit, "", "a", 10)
		if errors.Is(err, errDeclined) != wantDecline {
			t.Errorf("%s: resumeTreePage err = %v, want declined %v", name, err, wantDecline)
		}
	}
}

// git reads a replaced object through its replacement, the stream and the
// walk both: the pages and the sizes are the ones ls-tree prints.
func TestLsTreePageCursorOnReplacedObjects(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	ctx := context.Background()
	blob := gitIn(t, dir, "", "rev-parse", commit+":dir/b0")
	repl := gitIn(t, dir, "a much longer replacement of one blob", "hash-object", "-w", "--stdin")
	gitIn(t, dir, "", "update-ref", "refs/replace/"+blob, repl)
	sub := gitIn(t, dir, "", "rev-parse", commit+":dir/a")
	subRepl := rawTree(t, dir, [2]string{"100644 c", "zz"}, [2]string{"100644 d", "yy"})
	gitIn(t, dir, "", "update-ref", "refs/replace/"+sub, subRepl)
	sorted := sortedListing(t, dir, commit, "")
	if !slices.ContainsFunc(sorted, func(e TreeEntry) bool { return e.Path == "dir/a/d" }) {
		t.Fatalf("control: git does not read the replaced folder: %v", paths(sorted))
	}
	for _, e := range sorted {
		want, wantMore := windowOf(sorted, e.Path, 4)
		page, more, _, err := LsTreePage(ctx, dir, commit, "", e.Path, 4)
		if err != nil || !slices.Equal(page, want) || more != wantMore {
			t.Fatalf("after %q: %v, more %v, err %v; want %v, more %v", e.Path, page, more, err, want, wantMore)
		}
	}
}

// The page is charged by the bytes of its ls-tree records, as the stream
// does: it fits an allowance of that size and not one byte less, and a
// page refused charges nothing.
func TestLsTreePageCursorCharge(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	sorted := sortedListing(t, dir, commit, "")
	recLen := map[string]int64{}
	for rec := range strings.SplitSeq(gitIn(t, dir, "", "ls-tree", "--long", "-z", "-r", commit), "\x00") {
		_, p, _ := strings.Cut(rec, "\t")
		recLen[p] = int64(len(rec))
	}
	for _, e := range sorted {
		if got := recordLen(e); got != recLen[e.Path] {
			t.Fatalf("recordLen(%v) = %d, ls-tree prints %d", e, got, recLen[e.Path])
		}
	}
	if got, want := recordLen(TreeEntry{Path: "p", Type: EntryBlob, Size: 12345678}), int64(6+1+4+1+40+1+8+1+1); got != want {
		t.Fatalf("recordLen of a blob of 12345678 bytes = %d, want %d", got, want)
	}
	for _, after := range []string{"a0", "dir/a/c", "dir/b/c/d/e", ""} {
		const limit = 5
		page, _ := windowOf(sorted, after, limit)
		var want int64
		for _, e := range page {
			want += recLen[e.Path]
		}
		for _, allowance := range []int64{want - 1, want, want + 1} {
			ctx, b := budgeted(t, allowance)
			got, _, _, err := LsTreePage(ctx, dir, commit, "", after, limit)
			octx, ob := budgeted(t, allowance)
			_, _, _, oerr := pageOracle(octx, dir, commit, "", after, limit)
			if (err == nil) != (oerr == nil) || errors.Is(err, ErrOutputLimit) != errors.Is(oerr, ErrOutputLimit) {
				t.Fatalf("after %q, allowance %d: err %v, stream err %v", after, allowance, err, oerr)
			}
			if b.remaining() != ob.remaining() {
				t.Fatalf("after %q, allowance %d: %d left, the stream leaves %d", after, allowance, b.remaining(), ob.remaining())
			}
			if allowance >= want && (err != nil || len(got) != len(page) || b.remaining() != allowance-want) {
				t.Fatalf("after %q, allowance %d: %v, %d entries, %d left; want the page charged %d", after, allowance, err, len(got), b.remaining(), want)
			}
			if allowance < want && (!errors.Is(err, ErrOutputLimit) || b.remaining() != allowance) {
				t.Fatalf("after %q, allowance %d: err %v, %d left; want ErrOutputLimit and nothing charged", after, allowance, err, b.remaining())
			}
		}
	}
}

// A page past the end is empty and charges nothing. It still says whether
// the folder held anything.
func TestLsTreePageCursorPastTheEnd(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	for _, c := range []struct {
		path, after string
		empty       bool
		found       bool
	}{
		{"", "\xff\xff", true, true},
		{"dir/", "dir/\xff\xff", true, true},
		{"dir/", "zzzz", true, true},
		{"dir/", "a", false, true},
		{"nope/", "a", true, false},
	} {
		ctx, b := budgeted(t, 1<<20)
		page, more, found, err := LsTreePage(ctx, dir, commit, c.path, c.after, 10)
		if err != nil || found != c.found || (len(page) == 0) != c.empty || (c.empty && more) {
			t.Fatalf("%+v: page %v, more %v, found %v, err %v", c, paths(page), more, found, err)
		}
		if c.empty && b.remaining() != 1<<20 {
			t.Fatalf("%+v: an empty page charged %d bytes", c, 1<<20-b.remaining())
		}
	}
}

// catFileChildren lists the processes this test process started that run
// git cat-file.
func catFileChildren(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Skip("no /proc")
	}
	var out []string
	me := strconv.Itoa(os.Getpid())
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue
		}
		// pid (comm) state ppid ...
		rest := string(stat[strings.LastIndexByte(string(stat), ')')+2:])
		fields := strings.Fields(rest)
		if len(fields) < 2 || fields[1] != me {
			continue
		}
		cmdline, _ := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if strings.Contains(string(cmdline), "cat-file") {
			out = append(out, strings.ReplaceAll(string(cmdline), "\x00", " "))
		}
	}
	return out
}

// Every walk reaps its child: after a page, a decline and an error.
func TestLsTreePageCursorReapsItsChild(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	unsorted := commitOf(t, dir, rawTree(t, dir, [2]string{"100644 b", "1"}, [2]string{"100644 a", "2"}))
	for range 5 {
		if _, _, _, err := LsTreePage(context.Background(), dir, commit, "", "a/x", 3); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := LsTreePage(context.Background(), dir, unsorted, "", "a", 3); !errors.Is(err, ErrUnsortedTree) {
			t.Fatalf("unsorted tree: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, _, err := LsTreePage(ctx, dir, commit, "", "a/x", 3); err == nil {
			t.Fatal("a canceled context paged")
		}
		ctx, _ = budgeted(t, 100) // a page that does not fit
		if _, _, _, err := LsTreePage(ctx, dir, commit, "", "a/x", 30); !errors.Is(err, ErrOutputLimit) {
			t.Fatalf("a page over the allowance: %v", err)
		}
	}
	if kids := catFileChildren(t); len(kids) != 0 {
		t.Fatalf("cat-file children left: %q", kids)
	}
}

// A git that stops answering is killed at the request's time budget, and
// the request gets the budget error, not a decline.
func TestLsTreePageCursorTimeBudget(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	wrapper := "#!/bin/sh\nfor a in \"$@\"; do case \"$a\" in --batch) sleep 30 ;; esac; done\nexec " + real + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(wrapper), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	b := NewBudget(NewSlots(1, 1, 0), "repo", 500*time.Millisecond, 1<<20)
	t.Cleanup(b.Close)
	start := time.Now()
	_, _, _, err = LsTreePage(WithBudget(context.Background(), b), dir, commit, "", "a/x", 3)
	if !errors.Is(err, ErrTimeBudget) {
		t.Fatalf("err = %v, want ErrTimeBudget", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("took %v", d)
	}
	if kids := catFileChildren(t); len(kids) != 0 {
		t.Fatalf("cat-file children left: %q", kids)
	}
}

func FuzzLsTreePageCursor(f *testing.F) {
	dir, commit := fiRepo(f, nestedFiles())
	folders := []string{"", "a/", "dir/", "dir/a/", "dir/b/c/", "deep/1/2/"}
	sorted := map[string][]TreeEntry{}
	for _, p := range folders {
		sorted[p] = sortedListing(f, dir, commit, p)
	}
	f.Add("a/y/z", uint8(0), uint8(3))
	f.Add("dir/b/c/d", uint8(2), uint8(1))
	f.Add("dir/new\nline/", uint8(1), uint8(200))
	f.Add("\xff", uint8(3), uint8(2))
	f.Add("deep/1/2/3/4/f\x00", uint8(4), uint8(5))
	f.Fuzz(func(t *testing.T, after string, folder, limit uint8) {
		if after == "" {
			return
		}
		path := folders[int(folder)%len(folders)]
		l := 1 + int(limit)%(len(sorted[path])+2)
		want, wantMore := windowOf(sorted[path], after, l)
		page, more, found, err := LsTreePage(context.Background(), dir, commit, path, after, l)
		if err != nil || !found || !slices.Equal(page, want) || more != wantMore {
			t.Fatalf("LsTreePage(%q, after %q, limit %d) = %v, more %v, found %v, err %v\nwant %v, more %v", path, after, l, paths(page), more, found, err, paths(want), wantMore)
		}
	})
}
