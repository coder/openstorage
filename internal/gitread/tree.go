package gitread

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"github.com/coder/open-storage/internal/ref"
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
// lists a directory's contents, "dir" lists the entry itself. recursive
// descends into subtrees (blobs only, git -r semantics). A path that
// matches nothing yields an empty slice, NOT an error — the handler decides
// whether that is a 404.
func LsTree(ctx context.Context, repoPath, commit, path string, recursive bool) ([]TreeEntry, error) {
	assertf(ref.ValidSHA(commit) || commit == EmptyTreeSHA, "LsTree: unresolved commit %q", commit)
	args := []string{"ls-tree", "--long", "-z"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, commit)
	if path != "" {
		args = append(args, "--", path)
	}
	out, err := runGit(ctx, repoPath, args...)
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
// whole blob in memory (file/archive streaming path).
func CatBlobTo(ctx context.Context, repoPath, sha string, w io.Writer) error {
	assertf(ref.ValidSHA(sha), "CatBlobTo: malformed sha %q", sha)
	assertf(w != nil, "CatBlobTo: nil writer")
	cmd := exec.CommandContext(ctx, "git", "cat-file", "blob", sha)
	cmd.Dir = repoPath
	var stderr bytes.Buffer
	cmd.Stdout = w
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git cat-file blob %s: %w: %s", sha, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// ReadBlob returns the full contents of blob sha. Use only for small blobs
// (symlink targets); file streaming goes through CatBlobTo.
func ReadBlob(ctx context.Context, repoPath, sha string) ([]byte, error) {
	var buf bytes.Buffer
	if err := CatBlobTo(ctx, repoPath, sha, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
