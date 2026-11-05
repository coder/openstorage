package gitread

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

// Resuming a recursive listing at its cursor (#615).
//
// git prints a recursive listing in byte order of the full paths, so the
// entries after a cursor are, folder by folder along the cursor's path, the
// later siblings of each path component, deepest folder first. resumeTreePage
// reads just those trees through one `git cat-file --batch` child and
// parses them itself: the trees before the cursor are never opened, and the
// tree that holds the cursor is read once, not once per entry before it.
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
// not. Anything the walk does not handle returns errDeclined, and
// LsTreePage then pages by streaming `ls-tree -r`, which reads every entry
// the walk would skip, so the answer is the same.

// errDeclined: the walk met a tree or a path it leaves to the streaming
// listing: a mode git does not write, an entry name with a slash, an
// entry out of order, a name longer than the read buffer, a path that is
// not a plain folder, or a tree nested deeper than maxWalkDepth.
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
	// the stream.
	maxWalkDepth = 1024
	// maxWalkPath bounds the path of a folder the walk enters: every level of
	// the walk holds its own path, so memory grows with depth times path.
	maxWalkPath = 16 << 10
	// entryFloor is the size of an `ls-tree --long` record less its path:
	// mode, type, object id, a size field of its minimum width and the
	// separators. The walk bounds what it holds by it, the page is charged
	// by the exact length (recordLen).
	entryFloor = 6 + 1 + 4 + 1 + 40 + 1 + 7 + 1
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
		err = w.run(commit, path, comps, rest)
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
		return streamTreePage(ctx, repoPath, commit, path, after, limit, budget)
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
		return nil, false, false, fmt.Errorf("git ls-tree: %w", ErrOutputLimit)
	}
	return page, hasMore, true, nil
}

// folderComponents splits path, "" or a folder with a trailing slash, into
// its names. Any other spelling (a path git's pathspec rules would rewrite
// or refuse) is not ok.
func folderComponents(path string) (comps []string, ok bool) {
	if path == "" {
		return nil, true
	}
	if !strings.HasSuffix(path, "/") || strings.IndexByte(path, 0) >= 0 {
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
		return fmt.Errorf("cat-file --batch-check answered %d lines for %d blobs", len(lines), len(blobs))
	}
	for i, line := range lines {
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil || n < 0 {
			return fmt.Errorf("cat-file --batch-check answered %q for blob %s", line, page[blobs[i]].SHA)
		}
		page[blobs[i]].Size = n
	}
	return nil
}

// treeReader opens tree objects by object name.
type treeReader interface {
	// open returns the tree rev names: an object id, or commit^{tree}.
	open(rev string) (*treeBody, error)
}

// treeWalk collects the entries after a cursor, up to limit+1 of them:
// the extra one tells whether more follow.
type treeWalk struct {
	src   treeReader
	limit int
	room  int64 // output allowance left when the walk began
	held  int64 // floor size of the entries buffered for the page
	bytes int64 // floor size of the page
	page  []TreeEntry
}

func (w *treeWalk) full() bool { return len(w.page) > w.limit }

// run lists the folder named by comps under commit's root tree after rest.
func (w *treeWalk) run(commit, path string, comps []string, rest string) error {
	rev := commit + "^{tree}"
	for _, name := range comps {
		var err error
		if rev, err = childTree(w.src, rev, name); err != nil {
			return err
		}
	}
	return w.walk(rev, path, rest, 0)
}

// walk lists the entries of tree rev, whose path is prefix, that sort after
// rest, until the page is full. rev is an object name git reads as a tree.
func (w *treeWalk) walk(rev, prefix, rest string, depth int) error {
	if depth > maxWalkDepth {
		return errDeclined
	}
	// A scan keeps the entries the page can still take, fewer when the
	// allowance is short. When the page is short after them (a folder in
	// them held no file) or the scan stopped early, read the level again
	// and skip the entries used. Each pass keeps twice as many, so a
	// level of folders without files takes a few passes, not one for each.
	for pass, skip := 0, 0; !w.full(); pass++ {
		need := w.limit + 1 - len(w.page)
		sc, err := w.scan(rev, prefix, rest, skip, int(min(int64(need)<<min(pass, 24), math.MaxInt32)))
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
			if len(prefix)+len(e.name)+1 > maxWalkPath {
				return errDeclined
			}
			if err := w.walk(hex.EncodeToString(e.oid[:]), prefix+e.name+"/", "", depth+1); err != nil {
				return err
			}
			continue
		}
		if err := w.add(prefix, e); err != nil {
			return err
		}
	}
	return nil
}

// add appends a file to the page. The entry past the limit is not
// charged against the page: it only tells that the listing goes on.
func (w *treeWalk) add(prefix string, e rawEntry) error {
	te := TreeEntry{Path: prefix + e.name, Mode: modeText(e.mode), Type: EntryBlob, SHA: hex.EncodeToString(e.oid[:]), Size: -1}
	switch e.mode {
	case modeSymlink:
		te.Type = EntrySymlink
	case modeGitlink:
		te.Type = EntrySubmodule
	}
	cost := entryFloor + int64(len(te.Path))
	if len(w.page) < w.limit {
		if w.bytes+cost > w.room {
			return fmt.Errorf("git ls-tree: %w", ErrOutputLimit)
		}
		w.bytes += cost
	} else if cost > w.room {
		// The entry past the page is not charged, but it must fit the
		// allowance on its own, like any record the stream reads.
		return fmt.Errorf("git ls-tree: %w", ErrOutputLimit)
	}
	w.page = append(w.page, te)
	return nil
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
			continue
		case entryBefore:
			continue
		}
		if seen++; seen <= skip {
			continue
		}
		// A scan always keeps its first entry, so every level goes on, and
		// the entries kept at once stay within the allowance otherwise.
		cost := entryFloor + int64(len(prefix)+len(name))
		if len(sc.later) == max || len(sc.later) > 0 && w.bytes+w.held+cost > w.room {
			sc.more = true
			break
		}
		w.held += cost
		sc.held += cost
		sc.later = append(sc.later, rawEntry{string(name), mode, oid, cost})
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

// finish reads what is left of the object and the line feed after it, so
// the next reply starts the stream.
func (b *treeBody) finish() error {
	if _, err := io.Copy(io.Discard, b.lim); err != nil {
		return readErr(err)
	}
	if b.lim.N != 0 {
		return errDeclined // the stream ended inside the object
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

// open requests object rev and returns it, which must be a tree.
func (s *treeSource) open(rev string) (*treeBody, error) {
	if _, err := io.WriteString(s.req, rev+"\n"); err != nil {
		if runErr := s.wait(); runErr != nil {
			return nil, runErr
		}
		return nil, err
	}
	line, err := s.resp.ReadSlice('\n')
	if err != nil {
		return nil, readErr(err)
	}
	// <object id> SP tree SP <size> LF
	f := bytes.Split(bytes.TrimSuffix(line, []byte("\n")), []byte(" "))
	if len(f) != 3 || string(f[1]) != "tree" {
		return nil, errDeclined
	}
	size, err := strconv.ParseInt(string(f[2]), 10, 64)
	if err != nil || size < 0 {
		return nil, errDeclined
	}
	return newTreeBody(s.resp, size, s.obj), nil
}

// childTree returns the object id of the folder name in tree rev. A tree
// without one, or a file of that name only, declines: the stream lists it
// as git's pathspec rules say.
func childTree(src treeReader, rev, name string) (string, error) {
	body, err := src.open(rev)
	if err != nil {
		return "", err
	}
	want := []byte(name)
	id := ""
	for id == "" {
		n, mode, oid, ok, err := body.next()
		if err != nil {
			return "", err
		}
		if !ok || entryOrder(n, mode == modeTree, want, true) > 0 {
			break
		}
		if mode == modeTree && string(n) == name {
			id = hex.EncodeToString(oid[:])
		}
	}
	if id == "" {
		return "", errDeclined
	}
	if err := body.finish(); err != nil {
		return "", err
	}
	return id, nil
}
