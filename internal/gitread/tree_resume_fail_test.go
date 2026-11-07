package gitread

// What the walk's git can do that the stream's cannot (#615): fail on its
// own, need more heap, or read a tree that the stream reads in full. In
// every case the answer is the stream's, which is the one LsTreePage gave
// before the walk. The exception is
// TestLsTreePageCursorOnABrokenFolderBeforeTheCursor: a broken folder before
// the cursor fails a page that the stream answers, and not one that the walk
// answers.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// sameAsStream checks that LsTreePage answers as the stream does, an error
// included.
func sameAsStream(t *testing.T, ctx context.Context, dir, commit, path, after string, limit int) {
	t.Helper()
	page, more, found, err := LsTreePage(ctx, dir, commit, path, after, limit)
	wp, wm, wf, werr := pageOracle(ctx, dir, commit, path, after, limit)
	if !slices.Equal(page, wp) || more != wm || found != wf || fmt.Sprint(err) != fmt.Sprint(werr) {
		t.Fatalf("LsTreePage(%q, after %q, limit %d) = %v, more %v, found %v, err %v\nstream: %v, more %v, found %v, err %v",
			path, after, limit, paths(page), more, found, err, paths(wp), wm, wf, werr)
	}
}

// A git that fails or dies when the walk starts its cat-file child leaves the
// page to the stream: a heap cap that the stream's ls-tree fits under (the
// walk's child reads a tree object into memory, as ls-tree does), a crash, an
// exit status. The request's own outcomes stay the request's: see
// TestLsTreePageCursorTimeBudget and the canceled context in
// TestLsTreePageCursorReapsItsChild.
func TestLsTreePageCursorWhenTheWalksGitFails(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		script   string
		budgeted bool
		walkErr  func(error) bool // what the walk alone returns
	}{
		"heap cap":   {`echo "fatal: Out of memory, malloc failed (tried to allocate 1 bytes)" >&2; exit 128`, true, func(err error) bool { return errors.Is(err, ErrMemoryLimit) }},
		"exit":       {`echo "fatal: boom" >&2; exit 3`, false, func(err error) bool { return err != nil && !errors.Is(err, errDeclined) }},
		"crash":      {`kill -SEGV $$`, false, func(err error) bool { return err != nil && !errors.Is(err, errDeclined) }},
		"silent end": {`exit 0`, false, func(err error) bool { return errors.Is(err, errDeclined) }},
	} {
		t.Run(name, func(t *testing.T) {
			if c.budgeted && !HeapLimitSupported() {
				t.Skip("heap cap is Linux-only")
			}
			fakeGit(t, "for a in \"$@\"; do case \"$a\" in --batch) "+c.script+" ;; esac; done\nexec "+real+" \"$@\"")
			ctx := context.Background()
			if c.budgeted {
				ctx, _ = budgeted(t, 1<<20)
			}
			if _, _, _, err := resumeTreePage(ctx, dir, commit, "", "a/x", 3); !c.walkErr(err) {
				t.Fatalf("resumeTreePage = %v: the stand-in does not fail the walk as intended", err)
			}
			for _, after := range []string{"a/x", "dir/b/c/d.txt", "\x00"} {
				sameAsStream(t, ctx, dir, commit, "", after, 3)
				sameAsStream(t, ctx, dir, commit, "dir/", after, 2)
			}
			if kids := catFileChildren(t); len(kids) != 0 {
				t.Fatalf("cat-file children left: %q", kids)
			}
		})
	}
}

// A git config that fails leaves the page to the stream: the walk does not
// know the depth limit, so it does not answer.
func TestLsTreePageCursorWhenTheDepthLimitCannotBeRead(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	fakeGit(t, "for a in \"$@\"; do case \"$a\" in config) echo \"fatal: boom\" >&2; exit 3 ;; esac; done\nexec "+real+" \"$@\"")
	ctx := context.Background()
	if _, _, _, err := resumeTreePage(ctx, dir, commit, "", "a0", 100); err == nil || errors.Is(err, errDeclined) {
		t.Fatalf("the walk answered or declined, want git's error: %v", err)
	}
	sameAsStream(t, ctx, dir, commit, "", "a0", 100)
}

// A root folder of more than half the heap cap: the walk asks cat-file for
// the tree by its id, which costs the tree once, as it costs ls-tree. By the
// name commit^{tree} git reads the tree to resolve the name and cat-file reads
// it again, so the walk would fail under a cap that ls-tree meets.
func TestLsTreePageCursorOnARootFolderOverHalfTheHeapCap(t *testing.T) {
	if !HeapLimitSupported() {
		t.Skip("heap cap is Linux-only")
	}
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	blob := gitIn(t, dir, "x", "hash-object", "-w", "--stdin")
	names := make([]string, 200_000) // a tree object of 7.2 MB
	for i := range names {
		names[i] = fmt.Sprintf("f%07d", i)
	}
	tree := rawTreeBlob(t, dir, blob, names)
	commit := commitOf(t, dir, tree)

	const heapCap = 12 << 20
	b := NewBudget(NewSlots(1, 1, 0), "repo", time.Minute, DefaultOutputBytes).
		WithLimits(Limits{HeavyHeap: heapCap, LightHeap: LightHeapBytes, MaxBlob: MaxBlobBytes})
	t.Cleanup(b.Close)
	ctx := WithBudget(context.Background(), b)

	catFile := func(req string) error {
		_, err := Run(ctx, Cmd{Class: Heavy, Dir: dir, Stdin: strings.NewReader(req + "\n"), Stdout: io.Discard, Args: []string{"cat-file", "--batch"}})
		return err
	}
	if err := catFile(commit + "^{tree}"); !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("control: cat-file --batch of %s^{tree} under %d bytes = %v, want ErrMemoryLimit", commit, heapCap, err)
	}
	if err := catFile(tree); err != nil {
		t.Fatalf("control: cat-file --batch of the tree id under %d bytes: %v", heapCap, err)
	}
	const limit = 3
	after := names[100_000]
	wp, wm, wf, werr := pageOracle(ctx, dir, commit, "", after, limit)
	if werr != nil || len(wp) != limit || !wm || !wf {
		t.Fatalf("control: the stream under the cap = %v, %v, %v, %v", paths(wp), wm, wf, werr)
	}
	page, more, found, err := resumeTreePage(ctx, dir, commit, "", after, limit)
	if err != nil || !slices.Equal(page, wp) || more != wm || found != wf {
		t.Fatalf("resumeTreePage = %v, %v, %v, %v; want %v, %v", paths(page), more, found, err, paths(wp), wm)
	}
	sameAsStream(t, ctx, dir, commit, "", after, limit)
}

// The walk takes the root tree from the first line of the commit, and leaves
// to the stream what is not a commit that says so: a tag, a tree, a commit of
// another shape.
func TestLsTreePageCursorOnOtherRoots(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	ctx := context.Background()
	tree := gitIn(t, dir, "", "rev-parse", commit+"^{tree}")

	sameAsStream(t, ctx, dir, commit, "", "a/x", 3) // the plain commit
	// A message of megabytes after the tree line.
	bigMsg := gitIn(t, dir, strings.Repeat("a long message\n", 200_000), "commit-tree", tree, "-F", "-")
	sameAsStream(t, ctx, dir, bigMsg, "", "a/x", 3)
	// A commit with a different tree, in place of the first by a replace ref.
	other := rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 b", "2"}, [2]string{"100644 c", "3"})
	repl := commitOf(t, dir, other)
	gitIn(t, dir, "", "update-ref", "refs/replace/"+commit, repl)
	sameAsStream(t, ctx, dir, commit, "", "a", 3)
	gitIn(t, dir, "", "update-ref", "-d", "refs/replace/"+commit)
	// What is not a commit: an annotated tag, a tree id, the empty tree, an
	// object that is not there, and commits that do not begin with the tree.
	gitIn(t, dir, "", "tag", "-a", "-m", "t", "v1", commit)
	tag := gitIn(t, dir, "", "rev-parse", "refs/tags/v1")
	sameAsStream(t, ctx, dir, tag, "", "a/x", 3)
	sameAsStream(t, ctx, dir, tree, "", "a/x", 3)
	sameAsStream(t, ctx, dir, EmptyTreeSHA, "", "a", 3)
	sameAsStream(t, ctx, dir, strings.Repeat("ab", 20), "", "a", 3)
	for name, raw := range map[string]string{
		"parent first":    "parent " + commit + "\ntree " + tree + "\n\nm\n",
		"no line feed":    "tree " + tree,
		"short":           "tree " + tree[:30] + "\n",
		"not hex":         "tree " + strings.Repeat("g", 40) + "\n\nm\n",
		"tree of nothing": "tree " + strings.Repeat("0", 40) + "\n\nm\n",
	} {
		c := gitIn(t, dir, raw, "hash-object", "-t", "commit", "--literally", "-w", "--stdin")
		t.Run(name, func(t *testing.T) { sameAsStream(t, ctx, dir, c, "", "a/x", 3) })
	}
	if kids := catFileChildren(t); len(kids) != 0 {
		t.Fatalf("cat-file children left: %q", kids)
	}
}

// git reads a whole tree before it prints the first record of it: a tree
// that ends inside an entry is an error there, however far the page reads
// into it. The walk read such a tree only as far as the page needs, and it
// answered the page. It reads the rest of each tree it opens now, so the
// answer is the stream's, an error included.
func TestLsTreePageCursorOnADefectAfterThePage(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	ctx := context.Background()
	blob := gitIn(t, dir, "x", "hash-object", "-w", "--stdin")
	oid := rawID(t, blob)
	tree := func(raw string) string {
		return gitIn(t, dir, raw, "hash-object", "-t", "tree", "--literally", "-w", "--stdin")
	}
	ent := func(mode, name string) string { return mode + " " + name + "\x00" + oid }
	filesOf := func(n int) string {
		var sb strings.Builder
		for i := range n {
			sb.WriteString(ent("100644", fmt.Sprintf("f%03d", i)))
		}
		return sb.String()
	}
	for name, defect := range map[string]string{
		"entry cut short":        "100644 z",
		"bytes after the last":   "\x00\x00\x00",
		"oid cut short":          ent("100644", "z")[:12],
		"mode git rejects":       ent("99", "z"),
		"entry out of order":     ent("100644", "a"),
		"name with a slash":      ent("100644", "z/z"),
		"entry of an empty name": ent("100644", ""),
	} {
		for where, c := range map[string]struct {
			root  func() string
			after string
			limit int
		}{
			// The page ends with a folder; the entry past it is the first of
			// the next, whose end is cut.
			"lookahead folder": {func() string {
				return tree("40000 a\x00" + rawID(t, tree(filesOf(3))) + "40000 b\x00" + rawID(t, tree(ent("100644", "y")+ent("100644", "y2")+defect)))
			}, "\x00", 3},
			// The page ends inside a folder whose end is cut.
			"page folder": {func() string {
				return tree("40000 a\x00" + rawID(t, tree(filesOf(5)+defect)))
			}, "\x00", 2},
			// The cursor is in a folder whose end is cut.
			"cursor folder": {func() string {
				return tree("40000 c\x00" + rawID(t, tree(filesOf(5)+defect)))
			}, "c/f000", 1},
		} {
			t.Run(name+"/"+where, func(t *testing.T) {
				sameAsStream(t, ctx, dir, commitOf(t, dir, c.root()), "", c.after, c.limit)
			})
		}
	}
	if kids := catFileChildren(t); len(kids) != 0 {
		t.Fatalf("cat-file children left: %q", kids)
	}
}

func rawID(t testing.TB, id string) string {
	t.Helper()
	b, err := hex.DecodeString(id)
	if err != nil || len(b) != 20 {
		t.Fatalf("object id %q: %v", id, err)
	}
	return string(b)
}

// Which engine answers a page: the walk reads each folder of the page with a
// round trip, which costs about as much as git needs for 20 entries of the
// listing. A page of a thousand folders of one file near the start of the
// listing is the stream's, a short one is not, and one far into the listing
// is the walk's, where the stream would read far more; a page of larger
// folders is the walk's, and so is a cursor path of a hundred folders and not
// one of a hundred and fifty (maxWalkDepth is 128). The answer is the same.
func TestLsTreePageCursorChoosesTheEngineByTheFoldersItReads(t *testing.T) {
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	many := func(n, files int) []fiFile {
		var out []fiFile
		for i := range n {
			for j := range files {
				out = append(out, fiFile{"100644", fmt.Sprintf("d%05d/f%d", i, j), "x"})
			}
		}
		return out
	}
	// Chains of folders, each level with three files in front of its folder.
	chain := func(depth int) []fiFile {
		var out []fiFile
		for i := range 12 {
			for l := 0; l <= depth; l++ {
				for _, f := range []string{"a", "b", "c"} {
					out = append(out, fiFile{"100644", fmt.Sprintf("c%02d/%s%s", i, strings.Repeat("x/", l), f), "x"})
				}
			}
		}
		return out
	}
	for _, c := range []struct {
		name     string
		files    []fiFile
		after    string
		limit    int
		streamed bool
	}{
		{"folders of one file, near the start", many(1500, 1), "d00010/f0", 1000, true},
		{"folders of one file, a short page", many(1500, 1), "d00010/f0", 200, false},
		{"folders of one file, far in", many(21000, 1), "d19500/f0", 1000, false},
		{"folders of ten files", many(300, 10), "d00010/f0", 1000, false},
		{"a cursor path of 150 folders", chain(150), "c02/" + strings.Repeat("x/", 150) + "c", 100, true},
		{"a cursor path of 100 folders", chain(100), "c02/" + strings.Repeat("x/", 100) + "c", 100, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir, commit := fiRepo(t, c.files)
			log := filepath.Join(t.TempDir(), "git.log")
			fakeGit(t, "echo \"$*\" >> "+log+"\nexec "+real+" \"$@\"")
			page, more, found, err := LsTreePage(context.Background(), dir, commit, "", c.after, c.limit)
			out, rerr := os.ReadFile(log)
			if rerr != nil {
				t.Fatal(rerr)
			}
			if streamed := strings.Contains(string(out), "ls-tree"); streamed != c.streamed || !strings.Contains(string(out), "cat-file --batch") {
				t.Errorf("git ran:\n%s\nwant the stream %v after the walk", out, c.streamed)
			}
			wp, wm, wf, werr := pageOracle(context.Background(), dir, commit, "", c.after, c.limit)
			if err != nil || werr != nil || !slices.Equal(page, wp) || more != wm || found != wf || len(page) != c.limit {
				t.Fatalf("LsTreePage = %d entries, %v, %v, %v; stream %d entries, %v, %v, %v", len(page), more, found, err, len(wp), wm, wf, werr)
			}
		})
	}
}

// A page that comes out empty is the stream's, which reads the whole listing
// to prove it, and its error is the answer: LsTreePage does not run the
// stream a second time for it.
func TestLsTreePageCursorRunsTheStreamOnceForAnEmptyPage(t *testing.T) {
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	missing := "id:" + strings.Repeat("ab", 20)
	commit := commitOf(t, dir, rawTree(t, dir, [2]string{"100644 a", "1"}, [2]string{"100644 b", "2"}, [2]string{"40000 c", missing}))
	log := filepath.Join(t.TempDir(), "git.log")
	fakeGit(t, "echo \"$*\" >> "+log+"\nexec "+real+" \"$@\"")
	_, _, _, err = LsTreePage(context.Background(), dir, commit, "", "z", 1)
	out, rerr := os.ReadFile(log)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if err == nil || strings.Count(string(out), "ls-tree") != 1 {
		t.Fatalf("err = %v after:\n%s\nwant the error of one stream", err, out)
	}
	if errors.As(err, new(streamError)) {
		t.Fatalf("err = %T: the marker of the stream's answer reached the caller", err)
	}
}

// A request that finds no admission slot fails once, after the wait for one:
// the stream, which would wait again, does not run for it.
func TestLsTreePageCursorOnABusyPool(t *testing.T) {
	dir, commit := fiRepo(t, nestedFiles())
	const wait = time.Second
	slots := NewSlots(1, 1, wait)
	holder := NewBudget(slots, "repo", time.Minute, DefaultOutputBytes)
	t.Cleanup(holder.Close)
	if _, err := Run(WithBudget(context.Background(), holder), Cmd{Class: Heavy, Dir: dir, Args: []string{"rev-parse", "HEAD"}}); err != nil {
		t.Fatal(err)
	}
	other := NewBudget(slots, "repo", time.Minute, DefaultOutputBytes)
	t.Cleanup(other.Close)
	start := time.Now()
	_, _, _, err := LsTreePage(WithBudget(context.Background(), other), dir, commit, "", "a/x", 3)
	if d := time.Since(start); !errors.Is(err, ErrBusy) || d > wait*17/10 {
		t.Fatalf("err = %v after %v, want ErrBusy after one wait of %v", err, d, wait)
	}
}

// A cursor near the start of a folder of 200,000 files is the stream's: the walk
// would read the whole folder, 7 MB, for a page the stream finds in its first
// entries. A cursor far into it is the walk's.
func TestLsTreePageCursorChoosesTheEngineByTheSizeOfAFolder(t *testing.T) {
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	blob := gitIn(t, dir, "x", "hash-object", "-w", "--stdin")
	names := make([]string, 200_000)
	for i := range names {
		names[i] = fmt.Sprintf("f%06d", i)
	}
	commit := commitOf(t, dir, rawTreeBlob(t, dir, blob, names))
	for _, c := range []struct {
		name     string
		after    string
		streamed bool
	}{
		{"near the start", "f000100", true},
		{"far in", "f100000", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "git.log")
			fakeGit(t, "echo \"$*\" >> "+log+"\nexec "+real+" \"$@\"")
			page, more, found, err := LsTreePage(context.Background(), dir, commit, "", c.after, 1000)
			out, rerr := os.ReadFile(log)
			if rerr != nil {
				t.Fatal(rerr)
			}
			if streamed := strings.Contains(string(out), "ls-tree"); streamed != c.streamed || !strings.Contains(string(out), "cat-file --batch") {
				t.Errorf("git ran:\n%s\nwant the stream %v after the walk", out, c.streamed)
			}
			wp, wm, wf, werr := pageOracle(context.Background(), dir, commit, "", c.after, 1000)
			if err != nil || werr != nil || !slices.Equal(page, wp) || more != wm || found != wf || len(page) != 1000 {
				t.Fatalf("LsTreePage = %d entries, %v, %v, %v; stream %d entries, %v, %v, %v", len(page), more, found, err, len(wp), wm, wf, werr)
			}
		})
	}
}

// A folder before the cursor that git cannot read fails a page that the stream
// answers, as it does for a first page, and not a page that the walk answers
// (the files row of docs/reference/http-api.md). The tree has 1,500 folders of
// one file and the cursor is d00010/f0. A page of 1000 entries is the stream's
// and a page of 200 is the walk's
// (TestLsTreePageCursorChoosesTheEngineByTheFoldersItReads). The folder d00005
// is missing, or an object that does not inflate, or the top of a chain of 10
// folders under a core.maxTreeDepth of 3.
func TestLsTreePageCursorOnABrokenFolderBeforeTheCursor(t *testing.T) {
	const cursor = "d00010/f0"
	ctx := context.Background()
	oneFile := func(chain int) []fiFile {
		var files []fiFile
		for i := range 1500 {
			path := fmt.Sprintf("d%05d/f0", i)
			if i == 5 && chain > 0 {
				path = fmt.Sprintf("d%05d/%sf0", i, strings.Repeat("c/", chain))
			}
			files = append(files, fiFile{"100644", path, "x"})
		}
		return files
	}
	// folder5 returns a commit of commit's tree with d00005 set to id, which
	// need not exist.
	folder5 := func(t *testing.T, dir, commit, id string) string {
		var listing strings.Builder
		for _, line := range strings.Split(gitIn(t, dir, "", "ls-tree", commit), "\n") {
			if fields := strings.Fields(line); strings.HasSuffix(line, "\td00005") {
				line = strings.Replace(line, fields[2], id, 1)
			}
			listing.WriteString(line + "\n")
		}
		return commitOf(t, dir, gitIn(t, dir, listing.String(), "mktree", "--missing"))
	}
	for _, c := range []struct {
		name   string
		files  []fiFile
		break_ func(t *testing.T, dir, intact string) string
	}{
		{"missing", oneFile(0), func(t *testing.T, dir, intact string) string {
			return folder5(t, dir, intact, strings.Repeat("a", 40))
		}},
		{"unreadable", oneFile(0), func(t *testing.T, dir, intact string) string {
			id := strings.Repeat("b", 40)
			file := filepath.Join(dir, "objects", id[:2], id[2:])
			if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, []byte("not a zlib stream"), 0o444); err != nil {
				t.Fatal(err)
			}
			return folder5(t, dir, intact, id)
		}},
		{"too deep", oneFile(10), func(t *testing.T, dir, intact string) string {
			gitIn(t, dir, "", "config", "core.maxTreeDepth", "3")
			return intact
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir, intact := fiRepo(t, c.files)
			want, _, _, err := LsTreePage(ctx, dir, intact, "", cursor, 200)
			if err != nil || len(want) != 200 {
				t.Fatalf("the intact tree: %d entries, %v", len(want), err)
			}
			broken := c.break_(t, dir, intact)

			_, _, _, err = LsTreePage(ctx, dir, broken, "", cursor, 1000)
			_, _, _, streamErr := pageOracle(ctx, dir, broken, "", cursor, 1000)
			if err == nil || streamErr == nil || err.Error() != streamErr.Error() {
				t.Fatalf("limit 1000: err %v, the stream's %v; want the same error", err, streamErr)
			}

			page, _, _, err := LsTreePage(ctx, dir, broken, "", cursor, 200)
			if err != nil || !slices.Equal(page, want) {
				t.Fatalf("limit 200: %d entries, err %v; want the %d entries of the intact tree", len(page), err, len(want))
			}
			if _, _, _, err := pageOracle(ctx, dir, broken, "", cursor, 200); err == nil {
				t.Fatal("limit 200: the stream answers, so the tree is not broken before the cursor")
			}
		})
	}
}
