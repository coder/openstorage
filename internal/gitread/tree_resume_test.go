package gitread

// The resumed listing (#615) must return what sorting the whole listing and
// taking a window of it returns, for any cursor string. These tests run the
// walk over in-memory trees, where a model gives the listing; the tests in
// tree_resume_git_test.go compare it with git's own output.

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"testing"
)

// memEntry is an entry of the model: a folder (sub != nil, perhaps empty)
// or a file with a mode.
type memEntry struct {
	mode  uint32
	sub   map[string]*memEntry
	built *[20]byte // the object id once built: an entry under two names is one object
}

// memTrees is a tree store in memory: raw tree objects by object name, and
// the model's listing in byte order.
type memTrees struct {
	objects map[string][]byte
	leaves  []TreeEntry
	next    uint64
}

// deepCursor is a count of entries before the cursor so large that the walk
// is cheaper than the stream whatever the page: the tests of what the walk
// handles start from it, so that repaysTheFolders does not decide for them.
const deepCursor = 1 << 30

// memRoot names the root tree in memTrees, where the walk is given a tree name
// and not a commit.
const memRoot = "root"

func (m *memTrees) open(rev string) (*treeBody, error) {
	raw, ok := m.objects[rev]
	if !ok {
		return nil, errDeclined
	}
	obj := append(bytes.Clone(raw), '\n')
	return newTreeBody(bufio.NewReaderSize(bytes.NewReader(obj), treeReadBuffer), int64(len(raw)), nil), nil
}

var rawModes = map[uint32]string{modeTree: "40000", modeBlob: "100644", modeExec: "100755", modeSymlink: "120000", modeGitlink: "160000"}

// build stores the tree dir and returns its object id. A folder that two
// names share is stored once, as git does, and so are its files' ids.
func (m *memTrees) build(dir map[string]*memEntry) [20]byte {
	type item struct {
		name string
		e    *memEntry
		oid  [20]byte
	}
	var items []item
	for name, e := range dir {
		if e.built == nil {
			var id [20]byte
			if e.mode == modeTree {
				id = m.build(e.sub)
			} else {
				m.next++
				id[0], id[12] = 0xb0, byte(m.next)
				id[13], id[14] = byte(m.next>>8), byte(m.next>>16)
			}
			e.built = &id
		}
		items = append(items, item{name: name, e: e, oid: *e.built})
	}
	slices.SortFunc(items, func(a, b item) int {
		return entryOrder([]byte(a.name), a.e.mode == modeTree, []byte(b.name), b.e.mode == modeTree)
	})
	var raw []byte
	for _, it := range items {
		raw = append(raw, rawModes[it.e.mode]+" "+it.name+"\x00"...)
		raw = append(raw, it.oid[:]...)
	}
	var id [20]byte
	if len(dir) > 0 {
		m.next++
		id[0], id[12], id[13], id[14] = 0xa0, byte(m.next), byte(m.next>>8), byte(m.next>>16)
	} // else the one empty tree, as in git
	m.objects[hex.EncodeToString(id[:])] = raw
	return id
}

// collect lists the files below dir, a shared folder once for each name.
func (m *memTrees) collect(dir map[string]*memEntry, prefix string) {
	for name, e := range dir {
		if e.mode == modeTree {
			m.collect(e.sub, prefix+name+"/")
			continue
		}
		m.leaves = append(m.leaves, TreeEntry{
			Path: prefix + name, Mode: modeText(e.mode), SHA: hex.EncodeToString(e.built[:]), Size: -1,
			Type: map[uint32]string{modeBlob: EntryBlob, modeExec: EntryBlob, modeSymlink: EntrySymlink, modeGitlink: EntrySubmodule}[e.mode],
		})
	}
}

func newMemTrees(root map[string]*memEntry) *memTrees {
	m := &memTrees{objects: map[string][]byte{}}
	id := m.build(root)
	m.collect(root, "")
	m.objects[memRoot] = m.objects[hex.EncodeToString(id[:])]
	sort.Slice(m.leaves, func(i, j int) bool { return m.leaves[i].Path < m.leaves[j].Path })
	return m
}

// nameParts are the pieces of generated names. They put names on both
// sides of '/' in byte order ('-', '.', '0'), and add a newline, a space,
// non-UTF-8 and multi-byte text.
var nameParts = []string{"a", "b", "ab", "-", ".", "0", "1", "z", "\xff", "\n", " ", "\xc3\xa9", "a-", "a."}

func genName(r *rand.Rand) string {
	var sb strings.Builder
	for range 1 + r.IntN(3) {
		sb.WriteString(nameParts[r.IntN(len(nameParts))])
	}
	return sb.String()
}

func genDir(r *rand.Rand, depth int, empties bool) map[string]*memEntry {
	dir := map[string]*memEntry{}
	for range r.IntN(7) {
		name := genName(r)
		switch k := r.IntN(20); {
		case dir[name] != nil:
		case depth < 4 && k < 7:
			dir[name] = &memEntry{mode: modeTree, sub: genDir(r, depth+1, empties)}
		case k < 8 && empties: // an empty folder: git can store one, ls-tree lists nothing in it
			dir[name] = &memEntry{mode: modeTree, sub: map[string]*memEntry{}}
		default:
			dir[name] = &memEntry{mode: []uint32{modeBlob, modeBlob, modeBlob, modeExec, modeSymlink, modeGitlink}[r.IntN(6)]}
		}
	}
	if !empties && depth > 0 && len(dir) == 0 {
		dir[genName(r)] = &memEntry{mode: modeBlob} // every folder holds a file
	}
	return dir
}

// shareFolders puts a few folders that hold files only under a second name,
// as git does for identical folders: one object, listed at each path.
func shareFolders(r *rand.Rand, root map[string]*memEntry) {
	var sources []*memEntry // folders of files: none holds the folder that gets it
	targets := []map[string]*memEntry{root}
	var visit func(d map[string]*memEntry)
	visit = func(d map[string]*memEntry) {
		for _, e := range d {
			if e.mode != modeTree {
				continue
			}
			if folders := slices.ContainsFunc(slices.Collect(maps.Values(e.sub)), func(f *memEntry) bool { return f.mode == modeTree }); folders {
				targets = append(targets, e.sub)
			} else if len(e.sub) > 0 {
				sources = append(sources, e)
			}
			visit(e.sub)
		}
	}
	visit(root)
	if len(sources) == 0 {
		return
	}
	for range 1 + r.IntN(3) {
		d := targets[r.IntN(len(targets))]
		if name := genName(r); d[name] == nil && len(d) < 8 {
			d[name] = sources[r.IntN(len(sources))]
		}
	}
}

// genTree generates a model.
func genTree(r *rand.Rand, empties bool) map[string]*memEntry {
	root := genDir(r, 0, empties)
	shareFolders(r, root)
	return root
}

// folders returns the paths of the model's folders, "" first.
func folders(dir map[string]*memEntry, prefix string, out []string) []string {
	out = append(out, prefix)
	for name, e := range dir {
		if e.mode == modeTree && name != "." && name != ".." { // no query path names such a folder
			out = folders(e.sub, prefix+name+"/", out)
		}
	}
	return out
}

// genCursor returns a cursor near the model's paths: one of them, a cut or
// extended copy of one, or a string over the name pieces and '/'.
func genCursor(r *rand.Rand, m *memTrees) string {
	if len(m.leaves) > 0 && r.IntN(4) > 0 {
		p := m.leaves[r.IntN(len(m.leaves))].Path
		switch r.IntN(5) {
		case 0:
			return p
		case 1:
			return p[:r.IntN(len(p)+1)]
		case 2:
			return p + nameParts[r.IntN(len(nameParts))]
		case 3:
			return p + "/"
		default:
			i := strings.LastIndexByte(p, '/')
			return p[:i+1] + nameParts[r.IntN(len(nameParts))]
		}
	}
	var sb strings.Builder
	for range r.IntN(5) {
		if r.IntN(3) == 0 {
			sb.WriteByte('/')
		} else {
			sb.WriteString(nameParts[r.IntN(len(nameParts))])
		}
	}
	return sb.String()
}

// windowAfter is the page LsTreePage promises: up to limit+1 of the
// listing's entries under path that sort after the cursor.
func (m *memTrees) windowAfter(path, after string, limit int) []TreeEntry {
	var out []TreeEntry
	for _, e := range m.leaves {
		if strings.HasPrefix(e.Path, path) && e.Path > after {
			if out = append(out, e); len(out) == limit+1 {
				break
			}
		}
	}
	return out
}

// walkMem runs the walk over m and returns the entries it collected.
func walkMem(m *memTrees, path, after string, limit int, room int64) ([]TreeEntry, error) {
	comps, ok := folderComponents(path)
	if !ok {
		return nil, errors.New("not a folder path")
	}
	rest, any := cursorWithin(path, after)
	if !any {
		return nil, nil
	}
	w := &treeWalk{before: deepCursor, src: m, limit: limit, room: room}
	if err := w.run(memRoot, path, comps, rest); err != nil {
		return nil, err
	}
	if w.held != 0 && len(w.page) == 0 {
		return nil, fmt.Errorf("held %d bytes after the walk", w.held)
	}
	return w.page, nil
}

func checkWalk(t testing.TB, m *memTrees, path, after string, limit int, room int64) {
	t.Helper()
	got, err := walkMem(m, path, after, limit, room)
	if err != nil {
		t.Fatalf("walk(%q, after %q, limit %d, room %d): %v", path, after, limit, room, err)
	}
	want := m.windowAfter(path, after, limit)
	if !slices.Equal(got, want) {
		t.Fatalf("walk(%q, after %q, limit %d, room %d)\n got  %v\n want %v", path, after, limit, room, paths(got), paths(want))
	}
}

func paths(es []TreeEntry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Path
	}
	return out
}

func TestTreeWalkMatchesTheSortedListing(t *testing.T) {
	for seed := range uint64(400) {
		r := rand.New(rand.NewPCG(seed, 615))
		root := genTree(r, true)
		m := newMemTrees(root)
		dirs := folders(root, "", nil)
		for range 12 {
			path := dirs[r.IntN(len(dirs))]
			checkWalk(t, m, path, genCursor(r, m), 1+r.IntN(len(m.leaves)+2), math.MaxInt64)
		}
		// Every cursor of the listing, with a page that ends inside a
		// folder or after it.
		for _, e := range m.leaves {
			checkWalk(t, m, "", e.Path, 1+r.IntN(4), math.MaxInt64)
		}
	}
}

// countingTrees counts the reads of each tree.
type countingTrees struct {
	*memTrees
	opens map[string]int
}

func (c *countingTrees) open(rev string) (*treeBody, error) {
	c.opens[rev]++
	return c.memTrees.open(rev)
}

// A page that the trees it reads can fill opens each tree once: the walk keeps
// the entry past the page with the others, and does not read a level
// again to find it.
func TestTreeWalkReadsEachTreeOnce(t *testing.T) {
	for seed := range uint64(300) {
		r := rand.New(rand.NewPCG(seed, 2615))
		root := genDir(r, 0, false) // a shared folder is read at each path
		m := newMemTrees(root)
		for range 8 {
			c := &countingTrees{memTrees: m, opens: map[string]int{}}
			after, limit := genCursor(r, m), 1+r.IntN(len(m.leaves)+2)
			rest, _ := cursorWithin("", after)
			w := &treeWalk{before: deepCursor, src: c, limit: limit, room: math.MaxInt64}
			if err := w.run(memRoot, "", nil, rest); err != nil {
				t.Fatal(err)
			}
			for rev, n := range c.opens {
				if n > 1 {
					t.Fatalf("seed %d: tree %s read %d times for after %q, limit %d", seed, rev, n, after, limit)
				}
			}
		}
	}
}

// Identical folders are one tree object, and a folder with files listed under
// two names lists them at both paths: only a folder without files is skipped
// when it comes again.
func TestTreeWalkListsFoldersThatAreOneTree(t *testing.T) {
	x := &memEntry{mode: modeTree, sub: map[string]*memEntry{"f": {mode: modeBlob}}}
	m := newMemTrees(map[string]*memEntry{"a": x, "b": x, "c": {mode: modeBlob}})
	if got, want := paths(m.leaves), []string{"a/f", "b/f", "c"}; !slices.Equal(got, want) || m.leaves[0].SHA != m.leaves[1].SHA {
		t.Fatalf("model lists %v, want %v with one id for the shared file", got, want)
	}
	for _, after := range []string{"\x00", "a", "a/f", "b/f"} {
		checkWalk(t, m, "", after, 5, math.MaxInt64)
	}
}

// Folders without files in front of the first file are read in a few passes,
// not one pass each: every pass reads eight times the entries of the last,
// and a level that takes more than maxWalkPasses is left to the stream. The
// folders are one tree, as git stores them: the walk reads one of them.
func TestTreeWalkPassesOverEmptyFolders(t *testing.T) {
	empty := &memEntry{mode: modeTree, sub: map[string]*memEntry{}}
	for _, c := range []struct {
		empties int
		decline bool
	}{{1000, false}, {3000, true}} {
		root := map[string]*memEntry{"z": {mode: modeBlob}}
		for i := range c.empties {
			root[fmt.Sprintf("e%05d", i)] = empty
		}
		m := newMemTrees(root)
		cnt := &countingTrees{memTrees: m, opens: map[string]int{}}
		w := &treeWalk{before: deepCursor, src: cnt, limit: 1, room: math.MaxInt64}
		err := w.run(memRoot, "", nil, "")
		if c.decline != errors.Is(err, errDeclined) || !c.decline && err != nil {
			t.Fatalf("%d folders without files: err = %v, declined wanted %v", c.empties, err, c.decline)
		}
		if got, want := paths(w.page), []string{"z"}; !c.decline && !slices.Equal(got, want) {
			t.Fatalf("%d folders without files: page %v, want %v", c.empties, got, want)
		}
		if n := cnt.opens[memRoot]; n > maxWalkPasses {
			t.Fatalf("%d folders without files: the root was read %d times", c.empties, n)
		}
	}
}

// The folders of the query path count towards the depth limit: only 50
// folders are left to walk below this path, and the walk still declines.
func TestTreeWalkCountsThePathInTheDepth(t *testing.T) {
	chain := func(depth int) *memTrees {
		root := map[string]*memEntry{}
		dir := root
		for range depth {
			next := map[string]*memEntry{}
			dir["a"] = &memEntry{mode: modeTree, sub: next}
			dir = next
		}
		dir["f"] = &memEntry{mode: modeBlob}
		return newMemTrees(root)
	}
	if _, err := walkMem(chain(maxWalkDepth-24), strings.Repeat("a/", maxWalkDepth-74), "\x00", 1, math.MaxInt64); err != nil {
		t.Fatalf("a tree %d folders deep: %v", maxWalkDepth-24, err)
	}
	if _, err := walkMem(chain(maxWalkDepth+50), strings.Repeat("a/", maxWalkDepth), "\x00", 1, math.MaxInt64); !errors.Is(err, errDeclined) {
		t.Fatalf("a path %d folders deep in a tree %d deep: err = %v, want errDeclined", maxWalkDepth, maxWalkDepth+50, err)
	}
	// A path deeper than the limit is refused before any tree is read.
	c := &countingTrees{memTrees: chain(maxWalkDepth + 50), opens: map[string]int{}}
	w := &treeWalk{before: deepCursor, src: c, limit: 1, room: math.MaxInt64}
	if err := w.run(memRoot, strings.Repeat("a/", maxWalkDepth+1), slices.Repeat([]string{"a"}, maxWalkDepth+1), "\x00"); !errors.Is(err, errDeclined) || len(c.opens) != 0 {
		t.Fatalf("a path of %d folders: err = %v after reading %d trees, want errDeclined after none", maxWalkDepth+1, err, len(c.opens))
	}
}

func TestFolderComponents(t *testing.T) {
	for path, want := range map[string]bool{
		"": true, "a/": true, "a/b/": true, "a": false, "/": false, "a//": false, "./a/": false, "a/../": false, "a\x00/": false,
		strings.Repeat("n", maxWalkPath-1) + "/": true,
		strings.Repeat("n", maxWalkPath) + "/":   false,
	} {
		if _, ok := folderComponents(path); ok != want {
			t.Errorf("folderComponents(%.20q, %d bytes) ok = %v, want %v", path, len(path), ok, want)
		}
	}
}

// Folders without files that are one tree, as git stores them, cost one read.
// Distinct ones past the bound on reads leave the page to the stream.
func TestTreeWalkBoundsTheReadsOfEmptyFolders(t *testing.T) {
	shared := &memEntry{mode: modeTree, sub: map[string]*memEntry{}}
	for i := range 1000 {
		shared.sub[fmt.Sprintf("e%04d", i)] = &memEntry{mode: modeTree, sub: map[string]*memEntry{}}
	}
	root := map[string]*memEntry{"z": {mode: modeBlob}}
	for i := range 1000 {
		root[fmt.Sprintf("t%04d", i)] = shared
	}
	c := &countingTrees{memTrees: newMemTrees(root), opens: map[string]int{}}
	w := &treeWalk{before: deepCursor, src: c, limit: 1, room: math.MaxInt64}
	if err := w.run(memRoot, "", nil, ""); err != nil || len(w.page) != 1 || w.page[0].Path != "z" {
		t.Fatalf("page %v, err %v; want [z]", paths(w.page), err)
	}
	total := 0
	for _, n := range c.opens {
		total += n
	}
	if total > 50 {
		t.Fatalf("%d reads for a million folders that are one tree", total)
	}

	distinct := map[string]*memEntry{"z": {mode: modeBlob}}
	for i := range 3000 {
		distinct[fmt.Sprintf("t%04d", i)] = &memEntry{mode: modeTree, sub: map[string]*memEntry{"x": {mode: modeTree, sub: map[string]*memEntry{}}}}
	}
	c = &countingTrees{memTrees: newMemTrees(distinct), opens: map[string]int{}}
	w = &treeWalk{before: deepCursor, src: c, limit: 100, room: math.MaxInt64}
	if err := w.run(memRoot, "", nil, ""); !errors.Is(err, errDeclined) {
		t.Fatalf("err = %v, want errDeclined", err)
	}
	total = 0
	for _, n := range c.opens {
		total += n
	}
	if total > w.openLimit()+1 {
		t.Fatalf("%d reads, the bound is %d", total, w.openLimit())
	}
}

// The folders that hold the cursor count in the path bound too.
func TestTreeWalkDeclinesALongCursorPath(t *testing.T) {
	name := strings.Repeat("n", 255)
	chain := func(depth int) (*memTrees, string) {
		root := map[string]*memEntry{}
		dir := root
		var cursor strings.Builder
		for range depth {
			next := map[string]*memEntry{"g": {mode: modeBlob}}
			dir[name] = &memEntry{mode: modeTree, sub: next}
			dir["a"] = &memEntry{mode: modeBlob}
			cursor.WriteString(name + "/")
			dir = next
		}
		dir["f"] = &memEntry{mode: modeBlob}
		return newMemTrees(root), cursor.String()
	}
	short, cursor := chain(maxWalkPath/256 - 2)
	checkWalk(t, short, "", cursor+"g", 5, math.MaxInt64)
	long, cursor := chain(maxWalkPath/256 + 2)
	if _, err := walkMem(long, "", cursor+"g", 5, math.MaxInt64); !errors.Is(err, errDeclined) {
		t.Fatalf("err = %v, want errDeclined", err)
	}
}

// A path longer than maxWalkPath is left to the stream.
func TestTreeWalkDeclinesALongPath(t *testing.T) {
	chain := func(depth int) map[string]*memEntry {
		root := map[string]*memEntry{}
		dir := root
		name := strings.Repeat("n", 255)
		for range depth {
			next := map[string]*memEntry{}
			dir[name] = &memEntry{mode: modeTree, sub: next}
			dir = next
		}
		dir["f"] = &memEntry{mode: modeBlob}
		return root
	}
	short := newMemTrees(chain(maxWalkPath/256 - 2))
	checkWalk(t, short, "", "\x00", 5, math.MaxInt64)
	long := newMemTrees(chain(maxWalkPath/256 + 2))
	if _, err := walkMem(long, "", "\x00", 5, math.MaxInt64); !errors.Is(err, errDeclined) {
		t.Fatalf("walk of paths past %d bytes: err = %v, want errDeclined", maxWalkPath, err)
	}
}

// An allowance too short for what a level keeps leaves the level to the
// stream: the walk gives the exact page or declines, never another page.
func TestTreeWalkUnderAShortAllowance(t *testing.T) {
	var walked, declined int
	for seed := range uint64(300) {
		r := rand.New(rand.NewPCG(seed, 1615))
		root := genTree(r, true)
		m := newMemTrees(root)
		for range 10 {
			after := genCursor(r, m)
			limit := 1 + r.IntN(len(m.leaves)+2)
			room := int64(entryFloor+heldOverhead+4) * int64(1+r.IntN(3*limit+3))
			got, err := walkMem(m, "", after, limit, room)
			switch {
			case errors.Is(err, errDeclined):
				declined++
			case errors.Is(err, ErrOutputLimit):
				// The page itself does not fit.
			case err != nil:
				t.Fatal(err)
			default:
				if want := m.windowAfter("", after, limit); !slices.Equal(got, want) {
					t.Fatalf("seed %d: walk(after %q, limit %d, room %d)\n got  %v\n want %v", seed, after, limit, room, paths(got), paths(want))
				}
				walked++
			}
		}
	}
	if walked < 300 || declined < 100 {
		t.Fatalf("%d walks gave the page and %d declined under a short allowance", walked, declined)
	}
}

// With an allowance that holds one entry of a level, folders without files
// are not read one pass each: the level is left to the stream at once.
func TestTreeWalkDeclinesAShortAllowanceOnEmptyFolders(t *testing.T) {
	root := map[string]*memEntry{"z": {mode: modeBlob}}
	for i := range 3000 {
		root[fmt.Sprintf("e%05d", i)] = &memEntry{mode: modeTree, sub: map[string]*memEntry{}}
	}
	c := &countingTrees{memTrees: newMemTrees(root), opens: map[string]int{}}
	w := &treeWalk{before: deepCursor, src: c, limit: 1, room: entryFloor + heldOverhead + 10}
	if err := w.run(memRoot, "", nil, ""); !errors.Is(err, errDeclined) {
		t.Fatalf("err = %v, want errDeclined", err)
	}
	if n := c.opens[memRoot]; n != 1 {
		t.Fatalf("the root was read %d times", n)
	}
}

// A folder after the cursor costs the walk a round trip to git. After a
// sample of 32 of them the walk projects the folders the rest of the page
// needs, from the entries the sample gave. It goes on when that is no more
// than 750 reads, and otherwise when the reads, priced at 20 entries of the
// stream each, cost no more than the stream reads before the cursor and for the
// page. The numbers are literal: they are the policy.
func TestTreeWalkDeclinesFoldersThatDoNotRepayTheirReads(t *testing.T) {
	tiny := func(folders int) *memTrees {
		root := map[string]*memEntry{}
		for i := range folders {
			root[fmt.Sprintf("d%05d", i)] = &memEntry{mode: modeTree, sub: map[string]*memEntry{"f": {mode: modeBlob}}}
		}
		return newMemTrees(root)
	}
	many := func(folders, files int) *memTrees {
		root := map[string]*memEntry{}
		for i := range folders {
			folder := map[string]*memEntry{}
			for j := range files {
				folder[fmt.Sprintf("f%d", j)] = &memEntry{mode: modeBlob}
			}
			root[fmt.Sprintf("d%05d", i)] = &memEntry{mode: modeTree, sub: folder}
		}
		return newMemTrees(root)
	}
	// 30,000 files, then 100 folders without a file (each holds a folder of
	// its own, so that they are 200 trees), then one file.
	empties := func() *memTrees {
		root := map[string]*memEntry{"z": {mode: modeBlob}}
		for i := range 30_000 {
			root[fmt.Sprintf("a%05d", i)] = &memEntry{mode: modeBlob}
		}
		for i := range 100 {
			inner := map[string]*memEntry{fmt.Sprintf("s%03d", i): {mode: modeTree, sub: map[string]*memEntry{}}}
			root[fmt.Sprintf("e%03d", i)] = &memEntry{mode: modeTree, sub: inner}
		}
		return newMemTrees(root)
	}
	for _, c := range []struct {
		name    string
		m       *memTrees
		after   string
		limit   int
		decline bool
	}{
		// After the sample of 32 (32 entries) a page of 1,001 needs 969
		// reads, 19,380 entries of the stream. Before the cursor d18376/f are
		// 18,376 folders, its folder and its file: 18,378 entries, and with
		// the 1,001 of the page it falls short by one.
		{"folders of one file, near the start", tiny(1500), "d00010/f", 1000, true},
		{"folders of one file, at the start", tiny(1500), "\x00", 1000, true},
		{"folders of one file, 18,378 entries in", tiny(20000), "d18376/f", 1000, true},
		{"folders of one file, 18,379 entries in", tiny(20000), "d18377/f", 1000, false},
		{"folders of one file, far in", tiny(30000), "d25000/f", 1000, false},
		// A page of 3,000 asks for three times the reads: 56,379 entries.
		{"folders of one file, limit 3000, 56,378 entries in", tiny(60000), "d56376/f", 3000, true},
		{"folders of one file, limit 3000, 56,379 entries in", tiny(60000), "d56377/f", 3000, false},
		// The reads that no stream proof is needed for: 750 of them.
		{"folders of one file, 569 reads", tiny(1500), "d00010/f", 600, false},
		{"folders of one file, 749 reads", tiny(1500), "d00010/f", 780, false},
		{"folders of one file, 751 reads", tiny(1500), "d00010/f", 782, true},
		{"folders of three files, near the start", many(1500, 3), "d00010/f2", 3000, true},
		{"folders of three files, far in", many(20000, 3), "d17000/f2", 3000, false},
		// A folder without files counts for a fourth of an entry: 4,004
		// reads for 1,001 entries, 80,080 entries of the stream against
		// 31,001; a folder of one file would need 20,020.
		{"folders without files", empties(), "a29999", 1000, true},
		// The page fills inside the sample: nothing to project.
		{"folders of a hundred files", many(400, 100), "d00010/f99", 3000, false},
	} {
		cnt := &countingTrees{memTrees: c.m, opens: map[string]int{}}
		w := &treeWalk{src: cnt, limit: c.limit, room: math.MaxInt64}
		err := w.run(memRoot, "", nil, c.after)
		reads := 0
		for _, n := range cnt.opens {
			reads += n
		}
		if c.decline {
			if !errors.Is(err, errDeclined) || reads > 1+walkSample+2 {
				t.Errorf("%s: err = %v after %d reads, want errDeclined after the sample of %d", c.name, err, reads, walkSample)
			}
			continue
		}
		if want := c.m.windowAfter("", c.after, c.limit); err != nil || !slices.Equal(w.page, want) {
			t.Errorf("%s: err = %v, page of %d entries, want %d", c.name, err, len(w.page), len(want))
		}
	}
}

// The walk reads each tree it opens to its end. When the cursor is near the
// start of a large tree, that costs it more than the stream, which stops at the
// page: the walk gives up the tree before it reads the rest, once the rest
// costs more than 15,000 entries of the stream (250 bytes each) and more than
// the stream reads before the cursor and for the page. The numbers are literal:
// they are the policy.
func TestTreeWalkDeclinesTheRestOfALargeTree(t *testing.T) {
	root := map[string]*memEntry{}
	for i := range 200_000 {
		root[fmt.Sprintf("f%06d", i)] = &memEntry{mode: modeBlob}
	}
	large := newMemTrees(root)
	small := map[string]*memEntry{}
	for i := range 10_000 {
		small[fmt.Sprintf("f%06d", i)] = &memEntry{mode: modeBlob}
	}
	for _, c := range []struct {
		name    string
		m       *memTrees
		after   string
		limit   int
		decline bool
	}{
		// A tree of 35 bytes an entry: the line is about 24,500 entries before
		// the cursor for a page of 10, and 23,700 for a page of 1,000.
		{"at the start", large, "\x00", 10, true},
		{"near the start", large, "f000100", 10, true},
		{"24,001 entries in", large, "f024000", 10, true},
		{"25,101 entries in", large, "f025100", 10, false},
		{"a page of 1,000, 23,001 entries in", large, "f023000", 1000, true},
		{"a page of 1,000, 24,301 entries in", large, "f024300", 1000, false},
		{"far in", large, "f150000", 10, false},
		{"no rest to read", large, "f199990", 20, false},
		{"a tree of 350 KB", newMemTrees(small), "\x00", 10, false},
	} {
		cnt := &countingTrees{memTrees: c.m, opens: map[string]int{}}
		w := &treeWalk{src: cnt, limit: c.limit, room: math.MaxInt64}
		err := w.run(memRoot, "", nil, c.after)
		if c.decline {
			if !errors.Is(err, errDeclined) || len(w.page) != 0 || w.held != 0 {
				t.Errorf("%s: err = %v, page of %d entries, %d bytes held, want errDeclined with nothing kept", c.name, err, len(w.page), w.held)
			}
			continue
		}
		if want := c.m.windowAfter("", c.after, c.limit); err != nil || !slices.Equal(w.page, want) {
			t.Errorf("%s: err = %v, page of %d entries, want %d", c.name, err, len(w.page), len(want))
		}
	}
}

// A folder without files that the walk met before is skipped, and its
// nesting still counts towards the depth limit: git refuses a tree nested
// past its own limit, so the stream errors for such a listing and the walk
// leaves it to the stream. Here e0000 is an empty folder, e0001 holds it,
// e0002 holds e0001's folder, and so on.
func TestTreeWalkCountsTheDepthOfSkippedFolders(t *testing.T) {
	chains := func(n int) *memTrees {
		chain := &memEntry{mode: modeTree, sub: map[string]*memEntry{}}
		root := map[string]*memEntry{"z": {mode: modeBlob}, "zz": {mode: modeBlob}}
		// Files before the folders, so that the page repays the folders read.
		for i := range 3 * n {
			root[fmt.Sprintf("a%05d", i)] = &memEntry{mode: modeBlob}
		}
		for i := range n {
			root[fmt.Sprintf("e%04d", i)] = chain
			chain = &memEntry{mode: modeTree, sub: map[string]*memEntry{"d": chain}}
		}
		return newMemTrees(root)
	}
	// The last folder holds maxWalkDepth-1 folders in a row: its deepest
	// folder is the maxWalkDepth-th.
	checkWalk(t, chains(maxWalkDepth), "", "\x00", 10_000, math.MaxInt64)
	if _, err := walkMem(chains(maxWalkDepth+1), "", "\x00", 10_000, math.MaxInt64); !errors.Is(err, errDeclined) {
		t.Fatalf("a folder %d deep below an empty folder met before: err = %v, want errDeclined", maxWalkDepth+1, err)
	}
}

// What the walk holds is bounded by maxWalkHeld whatever the allowance is:
// a page of 5000 entries that must read past 200,000 folders without files
// is left to the stream.
func TestTreeWalkBoundsWhatItHolds(t *testing.T) {
	root := map[string]*memEntry{"z": {mode: modeBlob}}
	for i := range 200000 {
		root[fmt.Sprintf("e%06d", i)] = &memEntry{mode: modeTree, sub: map[string]*memEntry{}}
	}
	w := &treeWalk{before: deepCursor, src: newMemTrees(root), limit: 5000, room: math.MaxInt64}
	if err := w.run(memRoot, "", nil, ""); !errors.Is(err, errDeclined) {
		t.Fatalf("err = %v, want errDeclined", err)
	}
	if w.held != 0 {
		t.Fatalf("%d bytes still held after the walk", w.held)
	}
}

// A tree nested deeper than maxWalkDepth is left to the stream.
func TestTreeWalkDeclinesPastTheDepthLimit(t *testing.T) {
	// Three files in front of each folder repay the reads of the folders.
	chain := func(depth int) map[string]*memEntry {
		root := map[string]*memEntry{}
		dir := root
		for range depth {
			next := map[string]*memEntry{}
			dir["a"], dir["b"], dir["c"] = &memEntry{mode: modeBlob}, &memEntry{mode: modeBlob}, &memEntry{mode: modeBlob}
			dir["d"] = &memEntry{mode: modeTree, sub: next}
			dir["f"] = &memEntry{mode: modeBlob}
			dir = next
		}
		dir["f"] = &memEntry{mode: modeBlob}
		return root
	}
	m := newMemTrees(chain(maxWalkDepth - 1))
	checkWalk(t, m, "", "d/", 10_000, math.MaxInt64)
	checkWalk(t, m, "", "d/d/d/f", 10_000, math.MaxInt64)
	deep := newMemTrees(chain(maxWalkDepth + 2))
	if _, err := walkMem(deep, "", "d/", 10_000, math.MaxInt64); !errors.Is(err, errDeclined) {
		t.Fatalf("walk of a tree %d deep: err = %v, want errDeclined", maxWalkDepth+2, err)
	}
}

func FuzzTreeWalk(f *testing.F) {
	f.Add(uint64(1), uint64(2), "", uint8(3), uint8(255))
	f.Add(uint64(7), uint64(9), "a/b", uint8(1), uint8(255))
	f.Add(uint64(3), uint64(3), "a-b/\xff", uint8(5), uint8(8))
	f.Add(uint64(11), uint64(5), "a/", uint8(2), uint8(255))
	f.Fuzz(func(t *testing.T, s1, s2 uint64, cursor string, limit, roomK uint8) {
		r := rand.New(rand.NewPCG(s1, s2))
		root := genTree(r, true)
		m := newMemTrees(root)
		dirs := folders(root, "", nil)
		path := dirs[r.IntN(len(dirs))]
		l := 1 + int(limit)%(len(m.leaves)+2)
		room := int64(math.MaxInt64)
		if roomK < 128 {
			room = int64(entryFloor+4) * (1 + int64(roomK))
		}
		got, err := walkMem(m, path, cursor, l, room)
		if (errors.Is(err, ErrOutputLimit) || errors.Is(err, errDeclined)) && room != math.MaxInt64 {
			return
		}
		if err != nil {
			t.Fatalf("walk(%q, after %q, limit %d, room %d): %v", path, cursor, l, room, err)
		}
		if want := m.windowAfter(path, cursor, l); !slices.Equal(got, want) {
			t.Fatalf("walk(%q, after %q, limit %d, room %d)\n got  %v\n want %v", path, cursor, l, room, paths(got), paths(want))
		}
	})
}

// classify, compareSlashed and entryOrder against their definitions.
func TestTreeOrderHelpers(t *testing.T) {
	alphabet := []string{"a", "b", "-", ".", "0", "\xff", "/"}
	gen := func(r *rand.Rand, slash bool) string {
		var sb strings.Builder
		for range 1 + r.IntN(4) {
			p := alphabet[r.IntN(len(alphabet))]
			if p == "/" && !slash {
				p = "a"
			}
			sb.WriteString(p)
		}
		return sb.String()
	}
	r := rand.New(rand.NewPCG(615, 1))
	for range 20000 {
		a, b, rest := gen(r, false), gen(r, false), gen(r, true)
		aTree, bTree := r.IntN(2) == 0, r.IntN(2) == 0
		key := func(n string, tree bool) string {
			if tree {
				return n + "/"
			}
			return n + "\x00"
		}
		want := strings.Compare(key(a, aTree), key(b, bTree))
		if got := entryOrder([]byte(a), aTree, []byte(b), bTree); (got < 0) != (want < 0) || (got > 0) != (want > 0) {
			t.Fatalf("entryOrder(%q tree %v, %q tree %v) = %d, want sign of %d", a, aTree, b, bTree, got, want)
		}
		if got, want := compareSlashed([]byte(a), rest), strings.Compare(a+"/", rest); got != want {
			t.Fatalf("compareSlashed(%q, %q) = %d, want %d", a, rest, got, want)
		}
		// The three classes partition the entry's paths: a file is after
		// when its path is, a folder when all its paths are, and a folder
		// holds the cursor when its paths straddle it.
		leaf := "x"
		class := classify([]byte(a), modeTree, rest)
		below, above := false, false
		for _, p := range []string{a + "/" + leaf, a + "/" + leaf + "~", a + "/\xff\xff\xff", a + "/\x01"} {
			below = below || p <= rest
			above = above || p > rest
		}
		holds := strings.HasPrefix(rest, a+"/")
		if (class == entryCursor) != holds {
			t.Fatalf("classify(tree %q, %q) = %d, but the prefix test says %v", a, rest, class, holds)
		}
		if !holds && ((class == entryAfter) != (above && !below) || (class == entryBefore) != (below && !above)) {
			t.Fatalf("classify(tree %q, %q) = %d, paths below %v above %v", a, rest, class, below, above)
		}
		if got, want := classify([]byte(a), modeBlob, rest) == entryAfter, a > rest; got != want {
			t.Fatalf("classify(file %q, %q) after = %v, want %v", a, rest, got, want)
		}
	}
}

func rawBody(raw string) *treeBody {
	return newTreeBody(bufio.NewReaderSize(strings.NewReader(raw+"\n"), treeReadBuffer), int64(len(raw)), nil)
}

func rawEnt(mode, name string) string { return mode + " " + name + "\x00" + strings.Repeat("\x01", 20) }

func TestTreeBodyParsing(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string // names, or "declined"
	}{
		{"modes", rawEnt("100644", "a") + rawEnt("100755", "b") + rawEnt("120000", "c") + rawEnt("160000", "d") + rawEnt("40000", "e"), []string{"a", "b", "c", "d", "e"}},
		{"empty", "", nil},
		{"blob and folder of one name", rawEnt("100644", "a") + rawEnt("40000", "a"), []string{"a", "a"}},
		{"padded folder mode", rawEnt("040000", "a"), []string{"declined"}},
		{"mode git does not write", rawEnt("100664", "a"), []string{"declined"}},
		{"group-writable mode", rawEnt("100660", "a"), []string{"declined"}},
		{"slash in a name", rawEnt("100644", "a/b"), []string{"declined"}},
		{"empty name", rawEnt("100644", ""), []string{"declined"}},
		{"out of order", rawEnt("100644", "b") + rawEnt("100644", "a"), []string{"b", "declined"}},
		{"duplicate", rawEnt("100644", "a") + rawEnt("100644", "a"), []string{"a", "declined"}},
		{"folder sorts as name/", rawEnt("100644", "a-b") + rawEnt("40000", "a") + rawEnt("100644", "a0"), []string{"a-b", "a", "a0"}},
		{"folder before a smaller name", rawEnt("40000", "a") + rawEnt("100644", "a-b"), []string{"a", "declined"}},
		{"truncated oid", rawEnt("100644", "a")[:20], []string{"declined"}},
		{"no space", "100644", []string{"declined"}},
		{"no NUL", "100644 abc", []string{"declined"}},
		{"name longer than the buffer", rawEnt("100644", strings.Repeat("x", treeReadBuffer)), []string{"declined"}},
		{"name that fills the buffer", rawEnt("100644", strings.Repeat("x", treeReadBuffer/2)), []string{strings.Repeat("x", treeReadBuffer/2)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := rawBody(tt.raw)
			var got []string
			for {
				name, _, _, ok, err := b.next()
				if errors.Is(err, errDeclined) {
					got = append(got, "declined")
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					if err := b.finish(); err != nil {
						t.Fatalf("finish: %v", err)
					}
					break
				}
				got = append(got, string(name))
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// finish reads the entries the walk did not need as well: a tree git would
// refuse or list differently ends the walk (git checks a whole tree before it
// prints the first of its records).
func TestTreeBodyFinishParsesTheRest(t *testing.T) {
	good := rawEnt("100644", "a") + rawEnt("100644", "b") + rawEnt("100644", "c")
	for name, tt := range map[string]struct {
		raw     string
		decline bool
	}{
		"whole":                {good, false},
		"truncated last entry": {good + rawEnt("100644", "d")[:12], true},
		"cut oid":              {good + rawEnt("100644", "d")[:20], true},
		"bytes after":          {good + "\x00\x00", true},
		"mode git rejects":     {good + rawEnt("100664", "d"), true},
		"out of order":         {good + rawEnt("100644", "a"), true},
		"slash in a name":      {good + rawEnt("100644", "d/e"), true},
		"empty name":           {good + rawEnt("100644", ""), true},
	} {
		b := rawBody(tt.raw)
		if _, _, _, ok, err := b.next(); err != nil || !ok {
			t.Fatalf("%s: first entry: ok %v, err %v", name, ok, err)
		}
		if err := b.finish(); tt.decline && !errors.Is(err, errDeclined) || !tt.decline && err != nil {
			t.Errorf("%s: finish = %v, want decline %v", name, err, tt.decline)
		}
	}
}

// FuzzTreeBody reads arbitrary bytes as a tree object: the walk must end,
// give entries in order or decline, and never panic.
func FuzzTreeBody(f *testing.F) {
	f.Add(rawEnt("100644", "a") + rawEnt("40000", "b"))
	f.Add(rawEnt("100644", "b") + rawEnt("100644", "a"))
	f.Add("100644 a\x00")
	f.Add("40000 \x00" + strings.Repeat("\x00", 20))
	f.Add(rawEnt("160000", "x/y"))
	f.Fuzz(func(t *testing.T, raw string) {
		b := rawBody(raw)
		var prev []byte
		prevTree := false
		for n := 0; ; n++ {
			if n > len(raw) {
				t.Fatalf("read %d entries from %d bytes", n, len(raw))
			}
			name, mode, _, ok, err := b.next()
			if err != nil {
				if !errors.Is(err, errDeclined) {
					t.Fatalf("error %v, want errDeclined", err)
				}
				return
			}
			if !ok {
				break
			}
			if len(name) == 0 || bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
				t.Fatalf("accepted name %q", name)
			}
			if _, known := rawModes[mode]; !known {
				t.Fatalf("accepted mode %o", mode)
			}
			if n > 0 && entryOrder(prev, prevTree, name, mode == modeTree) >= 0 {
				t.Fatalf("accepted %q after %q", name, prev)
			}
			prev, prevTree = bytes.Clone(name), mode == modeTree
		}
		if err := b.finish(); err != nil && !errors.Is(err, errDeclined) {
			t.Fatalf("finish: %v", err)
		}
		if _, err := b.resp.ReadByte(); !errors.Is(err, io.EOF) {
			t.Fatalf("bytes after the object: %v", err)
		}
	})
}
