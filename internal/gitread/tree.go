package gitread

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/coder/openstorage/internal/ref"
)

// TreeEntry types (docs/reference/http-api.md GET /repos/files entry `type`): symlinks and
// submodules are surfaced distinctly from plain blobs/trees.
const (
	EntryBlob      = "blob"
	EntryTree      = "tree"
	EntrySymlink   = "symlink"
	EntrySubmodule = "submodule"
)

// TreeEntry is one ls-tree row.
type TreeEntry struct {
	Path string // full slash-separated path relative to the repo root
	Mode string // git octal mode string, e.g. "100644", "100755", "120000"
	Type string // EntryBlob | EntryTree | EntrySymlink | EntrySubmodule
	SHA  string // object id (blob sha for files — the X-Blob-Sha source)
	Size int64  // blob size in bytes; -1 for trees and submodules
}

// LsTree lists the tree at commit (a resolved full SHA). path filters with
// git's `ls-tree <commit> -- <path>` semantics: "" lists the root, "dir/"
// lists a directory's contents, "dir" lists the entry itself. The path is
// literal: pathspec magic and wildcards match only their own name (#577).
// recursive descends into subtrees (blobs only, git -r semantics). A path
// that matches nothing yields an empty slice, NOT an error — the handler
// decides whether that is a 404.
func LsTree(ctx context.Context, repoPath, commit, path string, recursive bool) ([]TreeEntry, error) {
	assertf(ref.ValidSHA(commit) || commit == EmptyTreeSHA, "LsTree: unresolved commit %q", commit)
	args := []string{"ls-tree", "--long", "-z"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, commit)
	if path != "" {
		args = append(args, "--", ":(literal)"+path)
	}
	out, err := runGit(ctx, Heavy, repoPath, args...)
	if err != nil {
		return nil, err
	}
	entries := []TreeEntry{}
	for rec := range strings.SplitSeq(out, "\x00") {
		if rec == "" {
			continue
		}
		e, err := parseLsTreeRecord(rec)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// ErrUnsortedTree: git listed a tree's entries out of byte order (a tree
// that fsck rejects as treeNotSorted), so LsTreePage cannot page it.
var ErrUnsortedTree = errors.New("ls-tree listed entries out of order")

// LsTreePage returns one page of LsTree(ctx, repoPath, commit, path, true)
// sorted by Path: up to limit entries whose Path sorts after `after`, and
// whether more follow (#612). git prints a recursive listing of a
// well-formed tree in byte order of the full paths, so LsTreePage streams
// `ls-tree -r` and stops git one entry past the page: its cost grows with
// the entries up to the page, not with the tree. Only the page counts
// against the request's output allowance. found reports whether path
// matched any entry. A listing out of order returns ErrUnsortedTree.
func LsTreePage(ctx context.Context, repoPath, commit, path, after string, limit int) (page []TreeEntry, hasMore, found bool, err error) {
	assertf(ref.ValidSHA(commit) || commit == EmptyTreeSHA, "LsTreePage: unresolved commit %q", commit)
	assertf(limit > 0, "LsTreePage: limit %d", limit)
	args := []string{"ls-tree", "--long", "-z", "-r", commit}
	if path != "" {
		args = append(args, "--", ":(literal)"+path)
	}
	p := &lsTreePager{after: after, limit: limit, budget: BudgetFrom(ctx)}
	if _, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdout: p, Args: args}); err != nil {
		return nil, false, false, err
	}
	if p.err == nil && len(p.pending) > 0 && !p.hasMore {
		p.err = fmt.Errorf("ls-tree output ended inside a record: %q", p.pending)
	}
	if p.err != nil {
		return nil, false, false, p.err
	}
	return p.page, p.hasMore, p.found, nil
}

// lsTreePager is LsTreePage's streaming consumer of `ls-tree --long -z`.
// It stops git (ErrStop) at the first record past the page or the first
// error, which it keeps in err.
type lsTreePager struct {
	after   string
	limit   int
	budget  *Budget // nil: no output allowance
	pending []byte  // the start of a record split across writes
	prev    string  // the last path read, for the order check
	found   bool
	page    []TreeEntry
	hasMore bool
	err     error
}

func (p *lsTreePager) Write(b []byte) (int, error) {
	n := len(b)
	for len(b) > 0 {
		i := bytes.IndexByte(b, 0)
		if i < 0 {
			p.keep(len(b))
			p.pending = append(p.pending, b...)
			break
		}
		rec := b[:i]
		if len(p.pending) > 0 {
			rec = append(p.pending, rec...)
		}
		b = b[i+1:]
		p.record(string(rec))
		p.pending = p.pending[:0]
		if p.err != nil || p.hasMore {
			return n, ErrStop
		}
	}
	if p.err != nil {
		return n, ErrStop
	}
	return n, nil
}

// keep charges n retained bytes to the output allowance.
func (p *lsTreePager) keep(n int) {
	if p.err == nil && p.budget != nil && !p.budget.take(int64(n)) {
		p.err = fmt.Errorf("git ls-tree: %w", ErrOutputLimit)
	}
}

func (p *lsTreePager) record(rec string) {
	e, err := parseLsTreeRecord(rec)
	if err != nil {
		p.err = err
		return
	}
	if p.found && e.Path <= p.prev {
		p.err = fmt.Errorf("%w: %q after %q", ErrUnsortedTree, e.Path, p.prev)
		return
	}
	p.found, p.prev = true, e.Path
	switch {
	case e.Path <= p.after:
	case len(p.page) == p.limit:
		p.hasMore = true
	default:
		p.keep(len(rec))
		p.page = append(p.page, e)
	}
}

// parseLsTreeRecord parses one `ls-tree --long -z` record:
//
//	<mode> SP <type> SP <object> SP+ <size|-> TAB <path>
func parseLsTreeRecord(rec string) (TreeEntry, error) {
	meta, path, ok := strings.Cut(rec, "\t")
	if !ok || path == "" {
		return TreeEntry{}, fmt.Errorf("ls-tree emitted %q", rec)
	}
	fields := strings.Fields(meta)
	if len(fields) != 4 {
		return TreeEntry{}, fmt.Errorf("ls-tree emitted %q", rec)
	}
	mode, objType, sha, sizeStr := fields[0], fields[1], fields[2], fields[3]
	if !ref.ValidSHA(sha) {
		return TreeEntry{}, fmt.Errorf("ls-tree emitted malformed sha in %q", rec)
	}
	e := TreeEntry{Path: path, Mode: mode, SHA: sha, Size: -1}
	if sizeStr != "-" {
		n, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			return TreeEntry{}, fmt.Errorf("ls-tree emitted size %q in %q", sizeStr, rec)
		}
		e.Size = n
	}
	switch {
	case mode == "120000":
		e.Type = EntrySymlink
	case objType == "commit":
		e.Type = EntrySubmodule
	case objType == "blob":
		e.Type = EntryBlob
	case objType == "tree":
		e.Type = EntryTree
	default:
		return TreeEntry{}, fmt.Errorf("ls-tree emitted object type %q in %q", objType, rec)
	}
	return e, nil
}

// BlobInfo locates one blob at commit:path plus the last commit that touched
// the path — the /repos/file streaming metadata (X-Blob-Sha,
// X-Last-Commit-Sha, ETag, Last-Modified sources).
type BlobInfo struct {
	SHA        string // blob object id
	Size       int64  // bytes
	Mode       string // git octal mode string
	Symlink    bool   // mode 120000: content is the link target
	LastCommit Commit // most recent commit touching the path, reachable from commit
}

// StatBlob resolves commit:path to its blob. A missing path (or a path that
// names a tree/submodule) wraps ErrNotFound.
func StatBlob(ctx context.Context, repoPath, commit, path string) (BlobInfo, error) {
	assertf(ref.ValidSHA(commit), "StatBlob: unresolved commit %q", commit)
	if path == "" {
		return BlobInfo{}, fmt.Errorf("stat empty path: %w", ErrNotFound)
	}
	entries, err := LsTree(ctx, repoPath, commit, path, false)
	if err != nil {
		return BlobInfo{}, err
	}
	var entry *TreeEntry
	for i := range entries {
		if entries[i].Path == path {
			entry = &entries[i]
			break
		}
	}
	if entry == nil || (entry.Type != EntryBlob && entry.Type != EntrySymlink) {
		return BlobInfo{}, fmt.Errorf("path %q at %s: %w", path, commit, ErrNotFound)
	}
	commits, err := Log(ctx, repoPath, commit, LogOptions{Path: path, MaxCount: 1})
	if err != nil {
		return BlobInfo{}, err
	}
	if len(commits) == 0 {
		return BlobInfo{}, fmt.Errorf("no commit touches existing path %q at %s", path, commit)
	}
	return BlobInfo{
		SHA:        entry.SHA,
		Size:       entry.Size,
		Mode:       entry.Mode,
		Symlink:    entry.Type == EntrySymlink,
		LastCommit: commits[0],
	}, nil
}

// CatBlobTo streams the raw bytes of blob sha into w without buffering the
// whole blob in memory (file/archive streaming path). git itself runs as a
// Heavy child: a blob it must inflate or rebuild from deltas in its heap
// can hit HeavyHeapBytes, so callers check CheckBlobSize first.
func CatBlobTo(ctx context.Context, repoPath, sha string, w io.Writer) error {
	assertf(ref.ValidSHA(sha), "CatBlobTo: malformed sha %q", sha)
	assertf(w != nil, "CatBlobTo: nil writer")
	_, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Stdout: w, Args: []string{"cat-file", "blob", sha}})
	return err
}

// ReadBlob returns the full contents of blob sha. Use only for small blobs
// (symlink targets); file streaming goes through CatBlobTo. The bytes count
// against the output allowance.
func ReadBlob(ctx context.Context, repoPath, sha string) ([]byte, error) {
	assertf(ref.ValidSHA(sha), "ReadBlob: malformed sha %q", sha)
	out, err := Run(ctx, Cmd{Class: Heavy, Dir: repoPath, Args: []string{"cat-file", "blob", sha}})
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}
