package gitread

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/coder/openstorage/internal/ref"
)

// Resuming a recursive listing at its cursor (#615).
//
// git prints a recursive listing in byte order of the full paths, so the
// entries after a cursor are, folder by folder along the cursor's path, the
// later siblings of each path component, deepest folder first. resumeTreePage
// reads just those trees through one `git cat-file --batch` child, by object
// id, and parses them itself: the trees before the cursor are never opened,
// and the trees that hold the cursor are read once per pass, not once per
// entry before it. A second child, `git cat-file --batch-check`, sizes the
// files of the page.
//
// Within one tree (named n for a folder, relative cursor rest):
//
//   - a file n is listed when n > rest
//   - a folder n is listed whole when n+"/" > rest
//   - the folder n with n+"/" a prefix of rest holds the cursor: its listing
//     resumes at rest[len(n)+1:], and comes before the later siblings
//   - any other entry sorts before the cursor
//
// This is LsTreePage's contract for any cursor string, an existing path or
// not. The walk opens no tree before the cursor, so an object that is
// missing or unreadable there no longer fails a page that the walk answers.
// A page left to the stream fails on it as before.
// Anything else it does not handle returns errDeclined, and LsTreePage then
// pages by streaming `ls-tree -r`, which reads every entry the walk would
// skip and gives the same answer. A page past the end is the stream's. So is
// the page when git fails for the walk (leftToStream), whatever the cause.

// errDeclined: the walk met something it leaves to the streaming listing,
// which then gives the answer, an error included:
//
//   - a path that is not a plain folder or is too long, two entries of one
//     name on the path, or a submodule with a folder's name
//   - an object that is missing, not a tree or cut short, a mode git does
//     not write, a name with a slash or longer than the read buffer, or
//     entries out of order
//   - a tree nested deeper than maxWalkDepth, or than core.maxTreeDepth
//   - more reads than openLimit, work that the stream would do for less
//     (repays), a level that takes more than maxWalkPasses, or more than
//     maxWalkHeld to keep
//   - a blob that git cannot size
//   - an output allowance too short for what the walk keeps or for the page
//     (the stream then says which error comes first)
var errDeclined = errors.New("tree walk declined")

// Tree entry modes git writes. Any other mode declines.
const (
	modeTree    = 0o040000
	modeBlob    = 0o100644
	modeExec    = 0o100755
	modeSymlink = 0o120000
	modeGitlink = 0o160000
)

const (
	// treeReadBuffer bounds one entry's mode and name: the walk reads
	// them whole with bufio's ReadSlice.
	treeReadBuffer = 64 << 10
	// maxWalkDepth bounds the recursion: a tree nested deeper is left to
	// the stream. It is far below git's default limit (core.maxTreeDepth,
	// 2048), and treeDepthLimit leaves a page to the stream when a lower
	// limit is set. Each folder on the cursor's path is read twice and costs
	// a round trip to git, about 50 to 300 microseconds, which the stream
	// does not pay: past this depth the path alone costs more than a page of
	// the stream.
	maxWalkDepth = 128
	// maxWalkPath bounds the path of a folder the walk enters: every level of
	// the walk holds its own path, so memory grows with depth times path.
	maxWalkPath = 16 << 10
	// entryFloor is the size of an `ls-tree --long` record less its path:
	// mode, type, object id, a size field of its minimum width and the
	// separators. The page is charged by the exact length (recordLen).
	entryFloor = 6 + 1 + 4 + 1 + 40 + 1 + 7 + 1
	// heldOverhead is what the walk adds to entryFloor for an entry it holds
	// while it waits: the Go values (rawEntry, its name) take more than the
	// record would.
	heldOverhead = 64
	// maxWalkHeld bounds what the walk holds: the floor sizes and overheads
	// of the entries buffered for the page and of the page. A level that does
	// not fit is left to the stream. The output allowance bounds it too.
	maxWalkHeld = 16 << 20
	// The walk spends work that the stream would not, and goes on only when
	// the stream costs more. The unit is an entry that the stream lists. The
	// stream reads at least the entries before the cursor and the page, and
	// the walk may spend walkFree (about 25 milliseconds) with no proof that
	// the stream costs more.
	//
	//   - A folder that comes after the cursor costs a round trip to git,
	//     walkFolderCost entries (33 microseconds against the 2 that git
	//     needs for a folder of one file inside ls-tree). The walk takes a
	//     sample of walkSample folders and projects from the entries they gave
	//     the folders that the page still needs.
	//   - The rest of a tree that the walk read the start of costs one entry
	//     for every walkTailBytes bytes (7 nanoseconds a byte). A cursor near
	//     the start of a folder of a million entries is cheaper for the stream.
	walkSample     = 32
	walkFolderCost = 20
	walkTailBytes  = 250
	walkFree       = 15_000
	// maxWalkPasses bounds the reads of one tree: the walk reads a level
	// again for the entries of folders without files, and each read sends
	// the whole tree. A level that is still short after them is left to the
	// stream, which reads each tree once.
	maxWalkPasses = 4
)

// resumeTreePage is LsTreePage for a cursor, or errDeclined.
func resumeTreePage(ctx context.Context, repoPath, commit, path, after string, limit int) (page []TreeEntry, hasMore, found bool, err error) {
	comps, ok := folderComponents(path)
	if !ok {
		return nil, false, false, errDeclined
	}
	budget := BudgetFrom(ctx)
	room := int64(math.MaxInt64)
	if budget != nil {
		room = budget.remaining()
	}
	rest, anyAfter := cursorWithin(path, after)

	var w *treeWalk
	if anyAfter {
		src, err := startTreeSource(ctx, repoPath)
		if err != nil {
			return nil, false, false, err
		}
		closed := false
		defer func() { // a panic in the walk still reaps git
			if !closed {
				_ = src.close(true)
			}
		}()
		w = &treeWalk{src: src, limit: limit, room: room}
		var root string
		if root, err = src.rootTree(commit); err == nil {
			err = w.run(root, path, comps, rest)
		}
		closed = true
		if cerr := src.close(err != nil); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, false, false, err
		}
	} else {
		w = &treeWalk{}
	}

	page = w.page
	if len(page) == 0 {
		// Nothing sorts after the cursor, which the stream proves by reading
		// the whole listing (a stale cursor: it is not on a page boundary
		// of a listing that goes on). Its answer includes every error the
		// listing has after the cursor, so that is the answer.
		page, hasMore, found, err = streamTreePage(ctx, repoPath, commit, path, after, limit, budget)
		if err != nil {
			err = streamError{err}
		}
		return page, hasMore, found, err
	}
	// git refuses a tree nested past core.maxTreeDepth, and so does the
	// stream for each folder that the walk read. Without the setting, git's
	// default limit is far above maxWalkDepth.
	if depth, set, err := treeDepthLimit(ctx, repoPath); err != nil {
		return nil, false, false, err
	} else if set && int64(w.deepest) > depth {
		return nil, false, false, errDeclined
	}
	// The entry past the page needs its size too: ls-tree --long fails on
	// a blob it cannot read, whether or not the entry is on the page.
	if err := fillSizes(ctx, repoPath, page); err != nil {
		return nil, false, false, err
	}
	if hasMore = len(page) > limit; hasMore {
		page = page[:limit]
	}
	var total int64
	for _, e := range page {
		total += recordLen(e)
	}
	if budget != nil && (total > budget.remaining() || !budget.take(total)) {
		return nil, false, false, errDeclined // the stream refuses the page
	}
	return page, hasMore, true, nil
}

// streamError is an error of the stream that resumeTreePage ran itself, for a
// page that came out empty: the stream has answered, and nothing runs it again.
type streamError struct{ error }

func (e streamError) Unwrap() error { return e.error }

// leftToStream reports whether the stream must answer, given the walk's
// outcome err: the walk declined, or its own git failed, which includes a
// heap cap (ErrMemoryLimit). The stream gives the answer that LsTreePage gave
// before the walk, and it fails the same way when git cannot read the tree.
// Outcomes of the request itself end it: the caller gave up, the time budget
// ended, or no admission slot came free. The stream would meet them too.
func leftToStream(ctx context.Context, err error) bool {
	var answered streamError
	switch {
	case err == nil, errors.As(err, &answered):
		return false
	case errors.Is(err, errDeclined):
		return true
	case ctx.Err() != nil, errors.Is(err, ErrTimeBudget), errors.Is(err, ErrBusy):
		return false
	}
	return true
}

// folderComponents splits path, "" or a folder with a trailing slash, into
// its names. Any other spelling (a path git's pathspec rules would rewrite
// or refuse) and a path past maxWalkPath is not ok.
func folderComponents(path string) (comps []string, ok bool) {
	if path == "" {
		return nil, true
	}
	if !strings.HasSuffix(path, "/") || strings.IndexByte(path, 0) >= 0 || len(path) > maxWalkPath {
		return nil, false
	}
	comps = strings.Split(path[:len(path)-1], "/")
	for _, c := range comps {
		if c == "" || c == "." || c == ".." {
			return nil, false
		}
	}
	return comps, true
}

// cursorWithin returns the cursor relative to the folder path, and false
// when every path in the folder sorts before it.
func cursorWithin(path, after string) (rest string, ok bool) {
	switch {
	case strings.HasPrefix(after, path):
		return after[len(path):], true
	case after < path:
		return "", true
	}
	return "", false
}

// recordLen is the length of e's `ls-tree --long -z` record, less its NUL.
func recordLen(e TreeEntry) int64 {
	objType, size := "blob", "-"
	if e.Type == EntrySubmodule {
		objType = "commit"
	}
	if e.Size >= 0 {
		size = strconv.FormatInt(e.Size, 10)
	}
	return int64(6 + 1 + len(objType) + 1 + 40 + 1 + max(7, len(size)) + 1 + len(e.Path))
}

// fillSizes sets the size of the page's blobs, as `ls-tree --long` shows it.
// A blob it cannot size declines: the stream's error for it depends on the
// records before it.
func fillSizes(ctx context.Context, repoPath string, page []TreeEntry) error {
	var in strings.Builder
	var blobs []int
	for i, e := range page {
		if e.Type == EntryBlob || e.Type == EntrySymlink {
			in.WriteString(e.SHA + "\n")
			blobs = append(blobs, i)
		}
	}
	if len(blobs) == 0 {
		return nil
	}
	var out bytes.Buffer
	args := []string{"cat-file", "--batch-check=%(objectsize)"}
	if _, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdin: strings.NewReader(in.String()), Stdout: &out, Args: args}); err != nil {
		return err
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != len(blobs) {
		return errDeclined
	}
	for i, line := range lines {
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil || n < 0 {
			return errDeclined // a blob git cannot read: the stream says what that costs
		}
		page[blobs[i]].Size = n
	}
	return nil
}

// treeDepthLimit returns the core.maxTreeDepth that git runs with in the
// repository, the deepest folder nesting it reads, and whether one is set.
// git is the one that knows it: the setting comes from the repository, the
// user, the system and the environment.
func treeDepthLimit(ctx context.Context, repoPath string) (depth int64, set bool, err error) {
	var out bytes.Buffer
	args := []string{"config", "--type=int", "--get", "core.maxTreeDepth"}
	_, err = Run(ctx, Cmd{Class: Light, Dir: repoPath, Stdout: &out, Args: args})
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit) && exit.ExitCode() == 1:
		return 0, false, nil // not set
	case err != nil:
		return 0, false, err
	}
	depth, err = strconv.ParseInt(strings.TrimSpace(out.String()), 10, 64)
	if err != nil {
		return 0, false, errDeclined
	}
	return depth, true, nil
}

// treeReader opens tree objects by object name.
type treeReader interface {
	// open returns the tree that rev names, an object id.
	open(rev string) (*treeBody, error)
}

// treeWalk collects the entries after a cursor, up to limit+1 of them:
// the extra one tells whether more follow.
type treeWalk struct {
	src   treeReader
	limit int
	room  int64 // output allowance left when the walk began
	held  int64 // floor size and overhead of the entries buffered for the page
	bytes int64 // floor size of the page
	page  []TreeEntry
	opens int // trees read so far
	// deepest is the deepest folder the walk reached, one it skipped
	// included.
	deepest int
	// empty holds the folders found to hold no file, by how many folders
	// nest below each.
	empty map[[20]byte]int
	// folders counts the folders after the cursor that the walk read, the
	// ones it found empty included.
	folders int
	// before counts the entries that sort before the cursor in the trees
	// the walk scanned: the least entries the stream reads before the page.
	before int
}

// openLimit bounds the trees a walk reads: the folders on the cursor's path,
// and about one tree for each entry of the page. The rest is folders without
// files. Past the bound the stream pages, which reads such folders inside
// git, much faster than a request for each.
func (w *treeWalk) openLimit() int { return 4*(w.limit+1) + 2*maxWalkDepth }

func (w *treeWalk) full() bool { return len(w.page) > w.limit }

// run lists the folder named by comps under the tree root after rest.
func (w *treeWalk) run(root, path string, comps []string, rest string) error {
	if len(comps) > maxWalkDepth {
		return errDeclined
	}
	rev := root
	for _, name := range comps {
		var err error
		if rev, err = childTree(w.src, rev, name); err != nil {
			return err
		}
	}
	// The folders of path count: git refuses a tree nested past its own
	// limit, and so does the stream, which a deep path declines to.
	return w.walk(rev, path, rest, len(comps))
}

// walk lists the entries of tree rev, whose path is prefix, that sort after
// rest, until the page is full. rev is an object name git reads as a tree.
func (w *treeWalk) walk(rev, prefix, rest string, depth int) error {
	if depth > maxWalkDepth {
		return errDeclined
	}
	w.deepest = max(w.deepest, depth)
	// A scan keeps the entries the page can still take, fewer when the
	// allowance is short. When the page is short after them (a folder in
	// them held no file) or the scan stopped early, read the level again
	// and skip the entries used. Each pass keeps eight times as many, so a
	// level of folders without files takes a few passes, not one for each,
	// and one that takes more than maxWalkPasses is left to the stream.
	for pass, skip := 0, 0; !w.full(); pass++ {
		if pass == maxWalkPasses {
			return errDeclined
		}
		need := w.limit + 1 - len(w.page)
		sc, err := w.scan(rev, prefix, rest, skip, int(min(int64(need)<<(3*pass), math.MaxInt32)))
		if err != nil {
			return err
		}
		if err := w.use(prefix, rest, sc, pass == 0, depth); err != nil {
			return err
		}
		if !sc.more {
			return nil
		}
		skip += len(sc.later)
	}
	return nil
}

// use adds the entries of one scan to the page: the folder with the cursor
// first, then the later entries, a folder as its whole listing.
func (w *treeWalk) use(prefix, rest string, sc treeScan, first bool, depth int) error {
	left := sc.held
	defer func() { w.held -= left }()
	if first && sc.hasCand {
		c := sc.cand
		if len(prefix)+len(c.name)+1 > maxWalkPath {
			return errDeclined
		}
		if err := w.walk(hex.EncodeToString(c.oid[:]), prefix+c.name+"/", rest[len(c.name)+1:], depth+1); err != nil {
			return err
		}
	}
	for _, e := range sc.later {
		if w.full() {
			return nil
		}
		w.held -= e.cost
		left -= e.cost
		if e.mode == modeTree {
			if below, none := w.empty[e.oid]; none {
				// Skipped, but its folders still count towards the depth.
				if w.deepest = max(w.deepest, depth+1+below); depth+1+below > maxWalkDepth {
					return errDeclined
				}
				continue
			}
			if len(prefix)+len(e.name)+1 > maxWalkPath {
				return errDeclined
			}
			if w.folders == walkSample && !w.repaysTheFolders() {
				return errDeclined
			}
			w.folders++
			before, outer := len(w.page), w.deepest
			w.deepest = depth + 1
			if err := w.walk(hex.EncodeToString(e.oid[:]), prefix+e.name+"/", "", depth+1); err != nil {
				return err
			}
			if len(w.page) == before && len(w.empty) < 1<<16 {
				if w.empty == nil {
					w.empty = map[[20]byte]int{}
				}
				w.empty[e.oid] = w.deepest - (depth + 1) // the same folder again lists nothing again
			}
			w.deepest = max(outer, w.deepest)
			continue
		}
		w.add(prefix, e)
	}
	return nil
}

// repays reports whether work of cost stream entries is worth the walk's
// while: it is free up to walkFree, and then it must cost no more than the
// stream reads before the cursor and for the page.
func (w *treeWalk) repays(cost float64) bool {
	return cost <= max(walkFree, float64(w.before+w.limit+1))
}

// repaysTheFolders projects, from the folders read so far, the folders the
// rest of the page needs. A folder with no file counts for a fourth of an
// entry.
func (w *treeWalk) repaysTheFolders() bool {
	perFolder := max(float64(len(w.page))/float64(w.folders), 0.25)
	return w.repays(float64(w.limit+1-len(w.page)) / perFolder * walkFolderCost)
}

// add appends a file to the page. The entry past the limit is not charged:
// it only tells that the listing goes on.
func (w *treeWalk) add(prefix string, e rawEntry) {
	te := TreeEntry{Path: prefix + e.name, Mode: modeText(e.mode), Type: EntryBlob, SHA: hex.EncodeToString(e.oid[:]), Size: -1}
	switch e.mode {
	case modeSymlink:
		te.Type = EntrySymlink
	case modeGitlink:
		te.Type = EntrySubmodule
	}
	if len(w.page) < w.limit {
		// scan counted this entry in held, with an overhead on top, and
		// checked it against the allowance together with bytes.
		cost := entryFloor + int64(len(te.Path))
		assertf(w.bytes+cost <= w.room, "add: %d bytes of page and %d more, allowance %d", w.bytes, cost, w.room)
		w.bytes += cost
	}
	w.page = append(w.page, te)
}

func modeText(mode uint32) string {
	switch mode {
	case modeTree:
		return "040000"
	case modeBlob:
		return "100644"
	case modeExec:
		return "100755"
	case modeSymlink:
		return "120000"
	}
	assertf(mode == modeGitlink, "modeText: mode %o", mode)
	return "160000"
}

// rawEntry is one entry of a tree object.
type rawEntry struct {
	name string
	mode uint32
	oid  [20]byte
	cost int64 // what the walk holds for it while it waits (treeWalk.held)
}

// treeScan is what a walk keeps of one tree.
type treeScan struct {
	cand    rawEntry // the folder that holds the cursor, when hasCand
	hasCand bool
	later   []rawEntry // entries after the cursor, in order
	more    bool       // another later entry follows
	held    int64      // floor size of later, counted in treeWalk.held
}

// scan reads tree rev and keeps its entries after rest (see the top of this
// file): the folder that holds the cursor, and up to max entries after it,
// less the first skip of them. A scan stops at the first entry past max.
func (w *treeWalk) scan(rev, prefix, rest string, skip, max int) (sc treeScan, err error) {
	assertf(max > 0, "scan: max %d", max)
	if w.opens++; w.opens > w.openLimit() {
		return sc, errDeclined
	}
	body, err := w.src.open(rev)
	if err != nil {
		return sc, err
	}
	seen := 0
	for {
		name, mode, oid, ok, err := body.next()
		if err != nil {
			return sc, err
		}
		if !ok {
			break
		}
		switch classify(name, mode, rest) {
		case entryCursor:
			sc.cand, sc.hasCand = rawEntry{name: string(name), mode: mode, oid: oid}, true
			if skip == 0 {
				w.before++
			}
			continue
		case entryBefore:
			if skip == 0 { // not again in a later pass over the level
				w.before++
			}
			continue
		}
		if seen++; seen <= skip {
			continue
		}
		if len(sc.later) == max {
			sc.more = true
			break
		}
		// What the walk holds stays within the allowance. A level that does
		// not fit is left to the stream: reading it again for the entries
		// that do not fit would cost a pass for each of them.
		cost := entryFloor + heldOverhead + int64(len(prefix)+len(name))
		if w.bytes+w.held+cost > min(w.room, maxWalkHeld) {
			w.held -= sc.held
			return sc, errDeclined
		}
		w.held += cost
		sc.held += cost
		sc.later = append(sc.later, rawEntry{string(name), mode, oid, cost})
	}
	// The rest of the tree is read to its end (finish) when the walk goes on.
	if sc.more && !w.repays(float64(body.remaining())/walkTailBytes) {
		w.held -= sc.held
		return sc, errDeclined
	}
	if err := body.finish(); err != nil {
		w.held -= sc.held
		return sc, err
	}
	return sc, nil
}

type entryClass int

const (
	entryBefore entryClass = iota // sorts before the cursor
	entryCursor                   // a folder that holds the cursor
	entryAfter                    // sorts after the cursor
)

// classify places an entry of a tree against the cursor rest.
func classify(name []byte, mode uint32, rest string) entryClass {
	if mode != modeTree {
		if string(name) > rest {
			return entryAfter
		}
		return entryBefore
	}
	// The folder's paths all start with name+"/".
	if len(rest) > len(name) && rest[len(name)] == '/' && rest[:len(name)] == string(name) {
		return entryCursor
	}
	if compareSlashed(name, rest) > 0 {
		return entryAfter
	}
	return entryBefore
}

// compareSlashed compares name+"/" with s.
func compareSlashed(name []byte, s string) int {
	n := min(len(name), len(s))
	if c := strings.Compare(string(name[:n]), s[:n]); c != 0 {
		return c
	}
	if len(s) <= len(name) {
		return 1 // s is name, or a prefix of it: name+"/" is longer
	}
	if c := strings.Compare("/", s[len(name):len(name)+1]); c != 0 || len(s) == len(name)+1 {
		return c
	}
	return -1 // name+"/" is a proper prefix of s
}

// entryOrder compares two entries in git's tree order: by name, a folder as
// if its name ended in a slash.
func entryOrder(a []byte, aTree bool, b []byte, bTree bool) int {
	n := min(len(a), len(b))
	if c := bytes.Compare(a[:n], b[:n]); c != 0 {
		return c
	}
	at := func(name []byte, tree bool) int {
		switch {
		case len(name) > n:
			return int(name[n])
		case tree:
			return '/'
		}
		return 0
	}
	return at(a, aTree) - at(b, bTree)
}

// treeBody reads the entries of one tree object from a reply stream. It
// reads the object's bytes and no others: git sends no more until the next
// request, so a read for the rest of a truncated entry would wait for it.
type treeBody struct {
	obj      *bufio.Reader // the object's bytes, in front of lim
	lim      *io.LimitedReader
	resp     *bufio.Reader // the reply stream: the object, then a line feed
	cur      []byte
	prev     []byte
	prevTree bool
	started  bool
}

// newTreeBody reads the next size bytes of resp. A non-nil buf is a reader
// kept for reuse, which the body takes over.
func newTreeBody(resp *bufio.Reader, size int64, buf *bufio.Reader) *treeBody {
	lim := &io.LimitedReader{R: resp, N: size}
	if buf == nil {
		buf = bufio.NewReaderSize(lim, treeReadBuffer)
	} else {
		buf.Reset(lim)
	}
	return &treeBody{obj: buf, lim: lim, resp: resp}
}

// next reads the next entry: its name is valid until the next call. ok is
// false at the end of the tree. Entries must come in git's tree order.
func (b *treeBody) next() (name []byte, mode uint32, oid [20]byte, ok bool, err error) {
	m, err := b.obj.ReadSlice(' ')
	if err != nil {
		if errors.Is(err, io.EOF) && len(m) == 0 && b.lim.N == 0 {
			return nil, 0, oid, false, nil
		}
		return nil, 0, oid, false, readErr(err)
	}
	switch string(m[:len(m)-1]) {
	case "40000":
		mode = modeTree
	case "100644":
		mode = modeBlob
	case "100755":
		mode = modeExec
	case "120000":
		mode = modeSymlink
	case "160000":
		mode = modeGitlink
	default:
		return nil, 0, oid, false, errDeclined
	}
	raw, err := b.obj.ReadSlice(0)
	if err != nil {
		return nil, 0, oid, false, readErr(err)
	}
	raw = raw[:len(raw)-1]
	if len(raw) == 0 || bytes.IndexByte(raw, '/') >= 0 {
		return nil, 0, oid, false, errDeclined
	}
	// The next read can move the bytes raw points at.
	b.cur = append(b.cur[:0], raw...)
	if _, err := io.ReadFull(b.obj, oid[:]); err != nil {
		return nil, 0, oid, false, readErr(err)
	}
	isTree := mode == modeTree
	if b.started && entryOrder(b.prev, b.prevTree, b.cur, isTree) >= 0 {
		return nil, 0, oid, false, errDeclined
	}
	b.started, b.prevTree = true, isTree
	b.prev, b.cur = b.cur, b.prev
	return b.prev, mode, oid, true, nil
}

// remaining is the bytes of the object not yet returned by next.
func (b *treeBody) remaining() int64 { return b.lim.N + int64(b.obj.Buffered()) }

// finish reads what is left of the object, entry by entry, and the line feed
// after it, so the next reply starts the stream. git reads a whole tree before
// it prints the first of its records, and a tree that ends inside an entry is
// an error there before any record of it. A tree that the walk read only as
// far as the page needs is the same tree: it declines on what it does not
// handle in the rest of the tree too.
func (b *treeBody) finish() error {
	for {
		_, _, _, ok, err := b.next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}
	c, err := b.resp.ReadByte()
	if err != nil {
		return readErr(err)
	}
	if c != '\n' {
		return errDeclined
	}
	return nil
}

// readErr turns a read failure into the walk's outcomes: git's own error
// (a limit, a kill) passes through, and an output that ends or does not fit
// the buffer is not a tree the walk reads.
func readErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, bufio.ErrBufferFull) {
		return errDeclined
	}
	return err
}

// treeSource reads tree objects from one `git cat-file --batch` child, one
// request at a time. Run owns the child and its limits.
type treeSource struct {
	req    *os.File
	resp   *bufio.Reader
	obj    *bufio.Reader // reused by each tree body
	respR  *io.PipeReader
	cancel context.CancelFunc
	done   chan error
	waited bool
	runErr error
}

var errTreeSourceClosed = errors.New("tree source closed")

func startTreeSource(ctx context.Context, repoPath string) (*treeSource, error) {
	reqR, reqW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	respR, respW := io.Pipe()
	runCtx, cancel := context.WithCancel(ctx)
	s := &treeSource{req: reqW, resp: bufio.NewReaderSize(respR, treeReadBuffer), obj: bufio.NewReaderSize(nil, treeReadBuffer), respR: respR, cancel: cancel, done: make(chan error, 1)}
	go func() {
		_, err := Run(runCtx, Cmd{Class: Heavy, Dir: repoPath, Stdin: reqR, Stdout: respW, Args: []string{"cat-file", "--batch"}})
		// git is reaped: a later request fails, and a reader sees git's
		// error, or the end of the output.
		_ = reqR.Close()
		_ = respW.CloseWithError(err)
		s.done <- err
	}()
	return s, nil
}

// wait returns Run's result once git is gone.
func (s *treeSource) wait() error {
	if !s.waited {
		s.runErr, s.waited = <-s.done, true
	}
	return s.runErr
}

// close ends the child. When abort is set the walk is over already (an
// error, or a decline): git is killed and its result does not matter.
// Otherwise git sees the end of its input, and its result is returned.
func (s *treeSource) close(abort bool) error {
	_ = s.req.Close()
	if abort {
		s.cancel()
		_ = s.respR.CloseWithError(errTreeSourceClosed)
	}
	err := s.wait()
	s.cancel()
	if abort {
		return nil
	}
	return err
}

// request asks for object rev and reads the header of the reply, which
// leaves the reader at the object's first byte: its type and size.
func (s *treeSource) request(rev string) (objType string, size int64, err error) {
	if _, err := io.WriteString(s.req, rev+"\n"); err != nil {
		if runErr := s.wait(); runErr != nil {
			return "", 0, runErr
		}
		return "", 0, err
	}
	line, err := s.resp.ReadSlice('\n')
	if err != nil {
		return "", 0, readErr(err)
	}
	// <object id> SP <type> SP <size> LF
	f := bytes.Split(bytes.TrimSuffix(line, []byte("\n")), []byte(" "))
	if len(f) != 3 {
		return "", 0, errDeclined // "<name> missing"
	}
	size, err = strconv.ParseInt(string(f[2]), 10, 64)
	if err != nil || size < 0 {
		return "", 0, errDeclined
	}
	return string(f[1]), size, nil
}

// open requests object rev and returns it, which must be a tree.
func (s *treeSource) open(rev string) (*treeBody, error) {
	objType, size, err := s.request(rev)
	if err != nil {
		return nil, err
	}
	if objType != "tree" {
		return nil, errDeclined
	}
	return newTreeBody(s.resp, size, s.obj), nil
}

// rootTree returns the object id of commit's tree. It reads the commit and
// takes the id from its first line. Asking cat-file for commit^{tree} would
// cost twice the tree in heap: git resolves that name by reading the tree
// into memory, and cat-file then reads it again. A root folder of more than
// half of HeavyHeapBytes would fail where ls-tree, which reads it once, does
// not. By id the walk reads each tree once, as ls-tree does.
func (s *treeSource) rootTree(commit string) (string, error) {
	if commit == EmptyTreeSHA {
		return commit, nil
	}
	objType, size, err := s.request(commit)
	if err != nil {
		return "", err
	}
	// The commit starts with its tree: "tree" SP <object id> LF.
	var head [len("tree ") + 40 + 1]byte
	if objType != "commit" || size < int64(len(head)) {
		return "", errDeclined
	}
	if _, err := io.ReadFull(s.resp, head[:]); err != nil {
		return "", readErr(err)
	}
	id := string(head[len("tree ") : len(head)-1])
	if string(head[:len("tree ")]) != "tree " || head[len(head)-1] != '\n' || !ref.ValidSHA(id) {
		return "", errDeclined
	}
	// The rest of the commit and the line feed after it.
	if _, err := io.CopyN(io.Discard, s.resp, size-int64(len(head))+1); err != nil {
		return "", readErr(err)
	}
	return id, nil
}

// childTree returns the object id of the folder name in tree rev. A tree
// without one, or with an entry of that name that is not a folder, declines:
// the stream lists it as git's pathspec rules say. The whole tree is read,
// so a tree that git would refuse or list twice (two folders of one name)
// declines too.
func childTree(src treeReader, rev, name string) (string, error) {
	body, err := src.open(rev)
	if err != nil {
		return "", err
	}
	id := ""
	for {
		n, mode, oid, ok, err := body.next()
		if err != nil {
			return "", err
		}
		if !ok {
			break
		}
		if string(n) != name {
			continue
		}
		if mode != modeTree {
			return "", errDeclined // git's pathspec reads a submodule of that name too
		}
		id = hex.EncodeToString(oid[:])
	}
	if id == "" {
		return "", errDeclined
	}
	if err := body.finish(); err != nil {
		return "", err
	}
	return id, nil
}
