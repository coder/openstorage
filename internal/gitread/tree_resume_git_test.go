package gitread

// LsTreePage with a cursor (#615) against git: the same pages as sorting
// the whole `ls-tree -r` listing and taking a window, and as the streaming
// listing it replaces (streamTreePage), for every cursor of a tree with
// nested folders and awkward names.

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
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
// and returns its id. An entry's second element is a tree or commit id for
// a "40000" or "160000" entry, "id:<hex>" for an object that need not
// exist, and the content of a new blob otherwise.
func rawTree(t testing.TB, dir string, entries ...[2]string) string {
	t.Helper()
	var raw strings.Builder
	for _, e := range entries {
		mode, name, ok := strings.Cut(e[0], " ")
		if !ok {
			t.Fatalf("bad entry %q", e[0])
		}
		id := e[1]
		if hexID, ok := strings.CutPrefix(id, "id:"); ok {
			id = hexID // an object that may not exist
		} else if mode != "40000" && mode != "160000" {
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

// The walk holds more than the page it charges, so an allowance can be short
// for the walk and enough for the page, or enough for both: the answer and
// the charge are the stream's either way. The names are long: the walk holds
// less than twice the page, so an allowance between the two has less left
// after the charge than the page took.
func TestLsTreePageCursorChargeAroundWhatTheWalkHolds(t *testing.T) {
	var files []fiFile
	for i := range 12 {
		files = append(files, fiFile{"100644", fmt.Sprintf("d/%s%02d", strings.Repeat("n", 40), i), "x"})
	}
	dir, commit := fiRepo(t, files)
	const limit = 5
	page, _ := windowOf(sortedListing(t, dir, commit, ""), "\x00", limit)
	var want int64
	for _, e := range page {
		want += recordLen(e)
	}
	var viaWalk int
	for allowance := want - 1; allowance <= 2*want+50; allowance += 11 {
		ctx, b := budgeted(t, allowance)
		got, more, found, err := LsTreePage(ctx, dir, commit, "", "\x00", limit)
		octx, ob := budgeted(t, allowance)
		wp, wm, wf, werr := pageOracle(octx, dir, commit, "", "\x00", limit)
		if !slices.Equal(got, wp) || more != wm || found != wf || fmt.Sprint(err) != fmt.Sprint(werr) || b.remaining() != ob.remaining() {
			t.Fatalf("allowance %d: %v, %v, %v, %v, %d left; stream %v, %v, %v, %v, %d left", allowance, paths(got), more, found, err, b.remaining(), paths(wp), wm, wf, werr, ob.remaining())
		}
		rctx, _ := budgeted(t, allowance)
		if _, _, _, err := resumeTreePage(rctx, dir, commit, "", "\x00", limit); err == nil {
			viaWalk++
		}
	}
	if viaWalk == 0 {
		t.Fatal("the walk never gave the page")
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

// Whatever the old stream reads after the cursor, the page reads too: an
// object that is missing after it fails both, and a page past the end is the
// stream's own answer.
func TestLsTreePageCursorOnMissingObjects(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	ctx := context.Background()
	missing := "id:" + strings.Repeat("ab", 20)
	sub := rawTree(t, dir, [2]string{"100644 x", "1"})
	for name, c := range map[string]struct {
		tree        string
		after       string
		limit       int
		wantFailure bool
	}{
		// The entry past the page needs its size, as ls-tree --long reads it.
		"lookahead blob": {rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 b", missing}), "\x00", 1, true},
		"page blob":      {rawTree(t, dir, [2]string{"100644 a", missing}, [2]string{"100644 b", "2"}), "\x00", 1, true},
		"after the page": {rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 b", "2"}, [2]string{"100644 c", missing}), "\x00", 1, false},
		// A page past the end reads the whole listing, as the stream does.
		"empty page":   {rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 b", "2"}, [2]string{"40000 c", missing}), "z", 1, true},
		"empty folder": {rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"40000 b", sub}), "z", 1, false},
	} {
		commit := commitOf(t, dir, c.tree)
		page, more, found, err := LsTreePage(ctx, dir, commit, "", c.after, c.limit)
		wp, wm, wf, werr := pageOracle(ctx, dir, commit, "", c.after, c.limit)
		if (err != nil) != c.wantFailure || (werr != nil) != c.wantFailure || err == nil && (!slices.Equal(page, wp) || more != wm || found != wf) || fmt.Sprint(err) != fmt.Sprint(werr) {
			t.Errorf("%s: LsTreePage = %v, %v, %v, %v; stream = %v, %v, %v, %v; want failure %v", name, paths(page), more, found, err, paths(wp), wm, wf, werr, c.wantFailure)
		}
	}
}

// The entry past the page, long or short, against a short allowance: when the
// allowance cannot hold what the walk keeps, the stream answers, so the page,
// the error and the charge are the stream's.
func TestLsTreePageCursorLookaheadAgainstTheAllowance(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	for _, c := range []struct {
		nameLen   int
		allowance int64
	}{{40000, 100}, {40000, 1 << 20}, {100, 70}, {100, 1 << 20}, {100, 62}, {100, 61}} {
		commit := commitOf(t, dir, rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 " + strings.Repeat("z", c.nameLen), "2"}))
		bctx, b := budgeted(t, c.allowance)
		page, more, found, err := LsTreePage(bctx, dir, commit, "", "\x00", 1)
		octx, ob := budgeted(t, c.allowance)
		wp, wm, wf, werr := pageOracle(octx, dir, commit, "", "\x00", 1)
		if !slices.Equal(page, wp) || more != wm || found != wf || errors.Is(err, ErrOutputLimit) != errors.Is(werr, ErrOutputLimit) || (err == nil) != (werr == nil) || b.remaining() != ob.remaining() {
			t.Errorf("%+v: page %v, more %v, found %v, err %v, %d left\nstream: %v, %v, %v, %v, %d left", c, paths(page), more, found, err, b.remaining(), paths(wp), wm, wf, werr, ob.remaining())
		}
	}
}

// A tree nested past git's own limit is an error for the stream, so it is
// for a page whose path holds the nesting.
func TestLsTreePageCursorOnADeepPath(t *testing.T) {
	deep := strings.Repeat("a/", 2100) + "f"
	dir, commit := fiRepo(t, []fiFile{{"100644", deep, "x"}})
	ctx := context.Background()
	for _, path := range []string{strings.Repeat("a/", 1500), strings.Repeat("a/", 100), ""} {
		page, more, found, err := LsTreePage(ctx, dir, commit, path, "\x00", 1)
		wp, wm, wf, werr := pageOracle(ctx, dir, commit, path, "\x00", 1)
		if (err == nil) != (werr == nil) || err == nil && (!slices.Equal(page, wp) || more != wm || found != wf) {
			t.Errorf("path of %d bytes: LsTreePage = %d entries, %v, %v, %v; stream = %d entries, %v, %v, %v", len(path), len(page), more, found, err, len(wp), wm, wf, werr)
		}
	}
}

// nestedTrees stores n trees with one git call: the first is empty, and each
// holds the one before it as the folder d. It returns their ids.
func nestedTrees(t testing.TB, dir string, n int) []string {
	t.Helper()
	files := t.TempDir()
	ids := []string{EmptyTreeSHA}
	var list strings.Builder
	for i := 1; i < n; i++ {
		prev, err := hex.DecodeString(ids[i-1])
		if err != nil {
			t.Fatal(err)
		}
		raw := "40000 d\x00" + string(prev)
		sum := sha1.Sum([]byte("tree " + strconv.Itoa(len(raw)) + "\x00" + raw))
		ids = append(ids, hex.EncodeToString(sum[:]))
		file := filepath.Join(files, strconv.Itoa(i))
		if err := os.WriteFile(file, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		list.WriteString(file + "\n")
	}
	stored := strings.Fields(gitIn(t, dir, list.String(), "hash-object", "-t", "tree", "--literally", "-w", "--stdin-paths"))
	if !slices.Equal(stored, ids[1:]) {
		t.Fatalf("git stored %d trees, not the ones expected", len(stored))
	}
	return ids
}

// A folder without files that the walk met before is skipped, and the folders
// nested in it still count: a chain of such folders past git's own limit is
// an error for the stream, so it is for the page. Each of the three chains
// holds the one before it below its first 1023 folders, so the walk reads
// none of them deeper than its own limit. With a limit of 1 the stream stops
// before it reads that far, and so does the page. The limit of 1000 gives the
// walk enough reads to get there.
func TestLsTreePageCursorOnNestedEmptyFolders(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	ids := nestedTrees(t, dir, 3*(maxWalkDepth-1)+1)
	var entries [][2]string
	for i := 1; i <= 3; i++ {
		entries = append(entries, [2]string{fmt.Sprintf("40000 e%d", i), ids[i*(maxWalkDepth-1)]})
	}
	entries = append(entries, [2]string{"100644 z", "1"}, [2]string{"100644 zz", "2"})
	commit := commitOf(t, dir, rawTree(t, dir, entries...))
	ctx := context.Background()
	var errs int
	for _, limit := range []int{1, 2, 1000} {
		page, more, found, err := LsTreePage(ctx, dir, commit, "", "\x00", limit)
		wp, wm, wf, werr := pageOracle(ctx, dir, commit, "", "\x00", limit)
		if !slices.Equal(page, wp) || more != wm || found != wf || fmt.Sprint(err) != fmt.Sprint(werr) {
			t.Errorf("limit %d: LsTreePage = %v, %v, %v, %v; stream %v, %v, %v, %v", limit, paths(page), more, found, err, paths(wp), wm, wf, werr)
		}
		if werr != nil {
			errs++
		}
	}
	if errs == 0 {
		t.Fatal("git lists the chain: it is not past its depth limit")
	}
}

// Identical folders are one tree object in git, listed at each path.
func TestLsTreePageCursorOnIdenticalFolders(t *testing.T) {
	files := []fiFile{
		{"100644", "a/f", "same"}, {"100644", "a/g/h", "deep"},
		{"100644", "b/f", "same"}, {"100644", "b/g/h", "deep"},
		{"100644", "c/f", "same"}, {"100644", "c/g/h", "deep"},
		{"100644", "d", "x"},
	}
	dir, commit := fiRepo(t, files)
	ctx := context.Background()
	for _, path := range []string{"", "a/", "b/g/"} {
		sorted := sortedListing(t, dir, commit, path)
		for _, after := range []string{"\x00", "a", "a/f", "a/g/h", "b", "b/f", "b/g", "b/g/h", "c/f", "c/g/h", "d"} {
			for _, limit := range []int{1, 2, 3, 100} {
				want, wantMore := windowOf(sorted, after, limit)
				page, more, found, err := LsTreePage(ctx, dir, commit, path, after, limit)
				if err != nil || (!found && len(want) > 0) || !slices.Equal(page, want) || more != wantMore {
					t.Fatalf("LsTreePage(%q, after %q, limit %d) = %v, %v, %v, %v; want %v, %v", path, after, limit, paths(page), more, found, err, paths(want), wantMore)
				}
			}
		}
	}
}

// A query path longer than the walk's bound is the stream's, and git reads
// it as before.
func TestLsTreePageCursorOnALongQueryPath(t *testing.T) {
	name := strings.Repeat("n", maxWalkPath/2)
	dir, commit := fiRepo(t, []fiFile{{"100644", name + "/" + name + "/f", "x"}, {"100644", name + "/" + name + "/g", "y"}})
	ctx := context.Background()
	path := name + "/" + name + "/"
	if _, _, _, err := resumeTreePage(ctx, dir, commit, path, path+"f", 5); !errors.Is(err, errDeclined) {
		t.Fatalf("resumeTreePage err = %v, want errDeclined", err)
	}
	page, more, found, err := LsTreePage(ctx, dir, commit, path, path+"f", 5)
	wp, wm, wf, werr := pageOracle(ctx, dir, commit, path, path+"f", 5)
	if (err == nil) != (werr == nil) || err == nil && (!slices.Equal(page, wp) || more != wm || found != wf) || err == nil && len(page) != 1 {
		t.Fatalf("LsTreePage = %d entries, %v, %v, %v; stream = %d entries, %v, %v, %v", len(page), more, found, err, len(wp), wm, wf, werr)
	}
}

// Records wider than the minimum (a folder commit's type is longer than a
// blob's) fill the allowance before a blob that git cannot read.
func TestLsTreePageCursorOverflowBeforeAMissingBlob(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	var entries [][2]string
	for i := range 33 {
		entries = append(entries, [2]string{fmt.Sprintf("160000 f%03d", i), strings.Repeat("1", 40)})
	}
	entries = append(entries, [2]string{"100644 zzzz", "id:" + strings.Repeat("ab", 20)})
	commit := commitOf(t, dir, rawTree(t, dir, entries...))
	readable := commitOf(t, dir, rawTree(t, dir, entries[:33]...))
	for _, allowance := range []int64{2210, 2211, 1 << 20} {
		// The page of submodules alone: its records are wider than the floor,
		// and a refused page charges nothing.
		ctx, b := budgeted(t, allowance)
		_, _, _, err := LsTreePage(ctx, dir, readable, "", "\x00", 100)
		octx, ob := budgeted(t, allowance)
		_, _, _, oerr := pageOracle(octx, dir, readable, "", "\x00", 100)
		if (err == nil) != (oerr == nil) || errors.Is(err, ErrOutputLimit) != errors.Is(oerr, ErrOutputLimit) || b.remaining() != ob.remaining() {
			t.Errorf("submodules, allowance %d: err %v, %d left; stream: err %v, %d left", allowance, err, b.remaining(), oerr, ob.remaining())
		}
		ctx, b = budgeted(t, allowance)
		_, _, _, err = LsTreePage(ctx, dir, commit, "", "\x00", 100)
		octx, ob = budgeted(t, allowance)
		_, _, _, oerr = pageOracle(octx, dir, commit, "", "\x00", 100)
		if (err == nil) != (oerr == nil) || errors.Is(err, ErrOutputLimit) != errors.Is(oerr, ErrOutputLimit) || b.remaining() != ob.remaining() {
			t.Errorf("allowance %d: err %v, %d left; stream: err %v, %d left", allowance, err, b.remaining(), oerr, ob.remaining())
		}
	}
}

// A tree of a thousand folders that are one tree of a thousand empty folders
// is two small objects. Reading each folder in a request would take half a
// minute; the walk reads each tree once.
func TestLsTreePageCursorOnFoldersThatAreOneTree(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	empty := gitIn(t, dir, "", "hash-object", "-t", "tree", "-w", "--stdin")
	var inner, outer [][2]string
	for i := range 1000 {
		inner = append(inner, [2]string{fmt.Sprintf("40000 e%04d", i), empty})
	}
	x := rawTree(t, dir, inner...)
	for i := range 1000 {
		outer = append(outer, [2]string{fmt.Sprintf("40000 t%04d", i), x})
	}
	outer = append(outer, [2]string{"100644 z", "1"})
	commit := commitOf(t, dir, rawTree(t, dir, outer...))
	b := NewBudget(NewSlots(1, 1, 0), "repo", 30*time.Second, 64<<20)
	t.Cleanup(b.Close)
	start := time.Now()
	page, more, found, err := LsTreePage(WithBudget(context.Background(), b), dir, commit, "", "\x00", 1)
	if err != nil || !found || more || len(page) != 1 || page[0].Path != "z" {
		t.Fatalf("page %v, more %v, found %v, err %v; want [z]", paths(page), more, found, err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("took %v", d)
	}
}

// A folder and a submodule of one name: git's pathspec for the folder reads
// both, so the page is the stream's.
func TestLsTreePageCursorOnAFolderAndASubmoduleOfOneName(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	ctx := context.Background()
	d := rawTree(t, dir, [2]string{"100644 x", "1"}, [2]string{"100644 y", "2"})
	p := rawTree(t, dir, [2]string{"160000 d", strings.Repeat("1", 40)}, [2]string{"40000 d", d}, [2]string{"100644 e", "3"})
	for name, tree := range map[string]string{
		"at the root": p,
		"below":       rawTree(t, dir, [2]string{"40000 p", p}),
	} {
		commit := commitOf(t, dir, tree)
		path := "d/"
		if name == "below" {
			path = "p/d/"
		}
		for _, after := range []string{"\x00", "a", "c", "d", "p", "p/c", "p/d", "d/x", "p/d/x"} {
			for _, limit := range []int{1, 2, 5} {
				page, more, found, err := LsTreePage(ctx, dir, commit, path, after, limit)
				wp, wm, wf, werr := pageOracle(ctx, dir, commit, path, after, limit)
				if !slices.Equal(page, wp) || more != wm || found != wf || (err == nil) != (werr == nil) {
					t.Fatalf("%s: LsTreePage(%q, after %q, limit %d) = %v, %v, %v, %v; stream %v, %v, %v, %v", name, path, after, limit, paths(page), more, found, err, paths(wp), wm, wf, werr)
				}
			}
		}
	}
}

// A folder entry that names something other than a tree is git's error.
func TestLsTreePageCursorOnAFolderThatIsABlob(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	// A blob whose bytes read as a tree with one file, which exists.
	file, err := hex.DecodeString(gitIn(t, dir, "x", "hash-object", "-w", "--stdin"))
	if err != nil {
		t.Fatal(err)
	}
	blob := gitIn(t, dir, "100644 x\x00"+string(file), "hash-object", "-w", "--stdin")
	commit := commitOf(t, dir, rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"40000 d", blob}, [2]string{"100644 e", "2"}))
	for _, after := range []string{"\x00", "a", "d"} {
		page, more, found, err := LsTreePage(context.Background(), dir, commit, "", after, 5)
		wp, wm, wf, werr := pageOracle(context.Background(), dir, commit, "", after, 5)
		if !slices.Equal(page, wp) || more != wm || found != wf || (err == nil) != (werr == nil) {
			t.Errorf("after %q: %v, %v, %v, %v; stream %v, %v, %v, %v", after, paths(page), more, found, err, paths(wp), wm, wf, werr)
		}
	}
}

// A page that does not fit the allowance and holds a blob git cannot read
// has the error the stream reaches first.
func TestLsTreePageCursorErrorOrder(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	missing := "id:" + strings.Repeat("ab", 20)
	for name, entries := range map[string][][2]string{
		"missing, then overflow": {{"100644 a", missing}, {"100644 b", "2"}},
		"overflow, then missing": {{"100644 a", "1"}, {"100644 b", missing}},
		"overflow":               {{"100644 a", "1"}, {"100644 b", "2"}},
	} {
		commit := commitOf(t, dir, rawTree(t, dir, entries...))
		for _, allowance := range []int64{61, 70, 100, 1 << 20} {
			ctx, b := budgeted(t, allowance)
			_, _, _, err := LsTreePage(ctx, dir, commit, "", "\x00", 10)
			octx, ob := budgeted(t, allowance)
			_, _, _, oerr := pageOracle(octx, dir, commit, "", "\x00", 10)
			if (err == nil) != (oerr == nil) || errors.Is(err, ErrOutputLimit) != errors.Is(oerr, ErrOutputLimit) || b.remaining() != ob.remaining() {
				t.Errorf("%s, allowance %d: err %v, %d left; stream: err %v, %d left", name, allowance, err, b.remaining(), oerr, ob.remaining())
			}
		}
	}
}

// An ancestor of the folder that git reads whole is read whole: two folders
// of one name list both, and a tree that ends inside an entry is an error,
// as for the stream.
func TestLsTreePageCursorOnTheAncestorsOfAFolder(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	ctx := context.Background()
	da := rawTree(t, dir, [2]string{"100644 a", "1"})
	db := rawTree(t, dir, [2]string{"100644 b", "2"})
	oid, err := hex.DecodeString(da)
	if err != nil {
		t.Fatal(err)
	}
	for name, tree := range map[string]string{
		"two folders of one name": rawTree(t, dir, [2]string{"40000 d", da}, [2]string{"40000 d", db}),
		"cut after the folder":    gitIn(t, dir, "40000 d\x00"+string(oid)+"100644 z", "hash-object", "-t", "tree", "--literally", "-w", "--stdin"),
		"a file of that name":     rawTree(t, dir, [2]string{"100644 d", "1"}),
		"intact":                  rawTree(t, dir, [2]string{"40000 d", da}, [2]string{"100644 z", "3"}),
	} {
		commit := commitOf(t, dir, tree)
		page, more, found, err := LsTreePage(ctx, dir, commit, "d/", "\x00", 1)
		wp, wm, wf, werr := pageOracle(ctx, dir, commit, "d/", "\x00", 1)
		if (err == nil) != (werr == nil) || err == nil && (!slices.Equal(page, wp) || more != wm || found != wf) {
			t.Errorf("%s: LsTreePage = %v, %v, %v, %v; stream = %v, %v, %v, %v", name, paths(page), more, found, err, paths(wp), wm, wf, werr)
		}
	}
}

// A folder with thousands of empty folders in front of the first file is
// read in a few passes, not once for each empty folder.
func TestLsTreePageCursorOnManyEmptyFolders(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	empty := gitIn(t, dir, "", "hash-object", "-t", "tree", "-w", "--stdin")
	var entries [][2]string
	for i := range 20000 {
		entries = append(entries, [2]string{fmt.Sprintf("40000 e%06d", i), empty})
	}
	entries = append(entries, [2]string{"100644 z", "1"})
	commit := commitOf(t, dir, rawTree(t, dir, entries...))
	b := NewBudget(NewSlots(1, 1, 0), "repo", 60*time.Second, 1<<20)
	t.Cleanup(b.Close)
	start := time.Now()
	page, more, found, err := LsTreePage(WithBudget(context.Background(), b), dir, commit, "", "\x00", 1)
	if err != nil || !found || more || len(page) != 1 || page[0].Path != "z" {
		t.Fatalf("page %v, more %v, found %v, err %v; want [z]", paths(page), more, found, err)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Fatalf("took %v", d)
	}
	// With an allowance for the page and not for what the walk keeps, the
	// level goes to the stream at once.
	ctx, b2 := budgeted(t, 70)
	start = time.Now()
	page, more, found, err = LsTreePage(ctx, dir, commit, "", "\x00", 1)
	octx, ob := budgeted(t, 70)
	wp, wm, wf, werr := pageOracle(octx, dir, commit, "", "\x00", 1)
	if !slices.Equal(page, wp) || more != wm || found != wf || (err == nil) != (werr == nil) || b2.remaining() != ob.remaining() {
		t.Fatalf("short allowance: %v, %v, %v, %v, %d left; stream %v, %v, %v, %v, %d left", paths(page), more, found, err, b2.remaining(), paths(wp), wm, wf, werr, ob.remaining())
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Fatalf("short allowance took %v", d)
	}
}

// git sends a tree object that ends inside an entry as it is and waits for
// the next request, so the walk must not read past the object for the rest
// of the entry: it would wait for the request's whole time budget.
func TestLsTreePageCursorOnTruncatedTrees(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	oid := strings.Repeat("\x01", 20)
	for name, raw := range map[string]string{
		"mode only":        "100644",
		"no name end":      "100644 abc",
		"short object id":  "100644 a\x00" + oid[:5],
		"second entry cut": "100644 a\x00" + oid + "100644 b",
	} {
		commit := commitOf(t, dir, gitIn(t, dir, raw, "hash-object", "-t", "tree", "--literally", "-w", "--stdin"))
		b := NewBudget(NewSlots(1, 1, 0), "repo", 20*time.Second, 1<<20)
		start := time.Now()
		_, _, _, err := LsTreePage(WithBudget(context.Background(), b), dir, commit, "", "a", 3)
		b.Close()
		if err == nil || IsBudgetError(err) || time.Since(start) > 8*time.Second {
			t.Errorf("%s: err = %v after %v, want git's own error at once", name, err, time.Since(start))
		}
	}
	if kids := catFileChildren(t); len(kids) != 0 {
		t.Fatalf("cat-file children left: %q", kids)
	}
}

// A walk that ends while git still has a large tree to write kills git: it
// does not wait for a pipe that nobody reads.
func TestLsTreePageCursorAbandonsALargeTree(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	blob := gitIn(t, dir, "x", "hash-object", "-w", "--stdin")
	var names []string
	for i := range 60000 {
		names = append(names, fmt.Sprintf("f%07d", i))
	}
	names[1], names[2] = names[2], names[1] // out of order, early on
	commit := commitOf(t, dir, rawTreeBlob(t, dir, blob, names))
	b := NewBudget(NewSlots(1, 1, 0), "repo", 20*time.Second, 1<<20)
	t.Cleanup(b.Close)
	start := time.Now()
	_, _, _, err := LsTreePage(WithBudget(context.Background(), b), dir, commit, "", "f0000000", 3)
	if !errors.Is(err, ErrUnsortedTree) {
		t.Fatalf("err = %v, want ErrUnsortedTree", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("took %v: git was not killed", d)
	}
	if kids := catFileChildren(t); len(kids) != 0 {
		t.Fatalf("cat-file children left: %q", kids)
	}
}

// rawTreeBlob stores a tree of files that share one blob, in this order.
func rawTreeBlob(t testing.TB, dir, blob string, names []string) string {
	t.Helper()
	id := make([]byte, 20)
	for i := range id {
		v, err := strconv.ParseUint(blob[2*i:2*i+2], 16, 8)
		if err != nil {
			t.Fatal(err)
		}
		id[i] = byte(v)
	}
	var raw strings.Builder
	for _, name := range names {
		raw.WriteString("100644 " + name + "\x00" + string(id))
	}
	return gitIn(t, dir, raw.String(), "hash-object", "-t", "tree", "--literally", "-w", "--stdin")
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
