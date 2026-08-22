package gitread

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/ref"
)

// ArchiveOptions tune one Archive stream. The zero value archives the whole
// tree with no prefix.
type ArchiveOptions struct {
	// Prefix is prepended verbatim to every entry name (callers include the
	// trailing "/", e.g. "repo-main/"). It must satisfy ValidArchivePrefix;
	// callers accepting untrusted input must validate it first.
	Prefix string
	// IncludeGlobs whitelists paths when non-empty; ExcludeGlobs always
	// wins. See MatchGlob for the dialect.
	IncludeGlobs []string
	ExcludeGlobs []string
	// MaxBlobSize (>0) skips file blobs larger than this many bytes —
	// the reason this is a custom walk instead of `git archive`.
	MaxBlobSize int64
}

// PreparedArchive is a resolved, filtered archive plan. Everything that can
// fail without touching the output stream (commit lookup, tree walk, entry
// filtering) happens in PrepareArchive, so HTTP callers can commit response
// headers only after resolution succeeded and mid-stream failures are
// limited to blob streaming.
type PreparedArchive struct {
	repoPath string
	opts     ArchiveOptions
	mtime    time.Time
	entries  []TreeEntry // filtered blob/symlink entries, tree order
}

// PrepareArchive resolves the tree at commit (a resolved full SHA) and
// applies the include/exclude globs and MaxBlobSize filters up front.
func PrepareArchive(ctx context.Context, repoPath, commit string, opts ArchiveOptions) (*PreparedArchive, error) {
	assertf(ref.ValidSHA(commit), "PrepareArchive: unresolved commit %q", commit)
	assertf(ValidArchivePrefix(opts.Prefix), "PrepareArchive: unsafe prefix %q", opts.Prefix)

	commits, err := Log(ctx, repoPath, commit, LogOptions{MaxCount: 1})
	if err != nil {
		return nil, err
	}
	if len(commits) != 1 {
		return nil, fmt.Errorf("archive: commit %s not found", commit)
	}

	entries, err := LsTree(ctx, repoPath, commit, "", true)
	if err != nil {
		return nil, err
	}
	kept := []TreeEntry{}
	for _, e := range entries {
		switch e.Type {
		case EntrySubmodule, EntryTree:
			continue
		case EntrySymlink, EntryBlob:
			// archivable
		default:
			return nil, fmt.Errorf("archive: unexpected entry type %q for %s", e.Type, e.Path)
		}
		if !archiveWants(e.Path, opts) {
			continue
		}
		if opts.MaxBlobSize > 0 && e.Type == EntryBlob && e.Size > opts.MaxBlobSize {
			continue
		}
		kept = append(kept, e)
	}
	return &PreparedArchive{
		repoPath: repoPath,
		opts:     opts,
		mtime:    commits[0].CommitterDate,
		entries:  kept,
	}, nil
}

// Stream writes the prepared entries as a tar.gz into w: regular blobs
// (mode 0644, or 0755 for git mode 100755) and symlinks (target = blob
// content); entry mtimes are the commit's committer time. Errors here can
// only truncate the stream — HTTP callers have already sent headers.
func (p *PreparedArchive) Stream(ctx context.Context, w io.Writer) error {
	assertf(p != nil && p.repoPath != "", "Stream: unprepared archive")
	assertf(w != nil, "Stream: nil writer")

	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	for _, e := range p.entries {
		hdr := &tar.Header{
			Name:    p.opts.Prefix + e.Path,
			ModTime: p.mtime,
		}
		switch e.Type {
		case EntrySymlink:
			target, err := ReadBlob(ctx, p.repoPath, e.SHA)
			if err != nil {
				return err
			}
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = string(target)
			hdr.Mode = 0o777
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
		case EntryBlob:
			hdr.Typeflag = tar.TypeReg
			hdr.Size = e.Size
			hdr.Mode = 0o644
			if e.Mode == "100755" {
				hdr.Mode = 0o755
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			// Stream blob bytes straight into the tar entry; a size
			// mismatch surfaces as a tar write error.
			if err := CatBlobTo(ctx, p.repoPath, e.SHA, tw); err != nil {
				return err
			}
		default:
			// PrepareArchive admits only blobs and symlinks.
			assertf(false, "Stream: unexpected entry type %q for %s", e.Type, e.Path)
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// Archive is PrepareArchive + Stream in one call, for callers that do not
// need the resolve-before-stream split.
func Archive(ctx context.Context, repoPath, commit string, opts ArchiveOptions, w io.Writer) error {
	assertf(w != nil, "Archive: nil writer")
	prepared, err := PrepareArchive(ctx, repoPath, commit, opts)
	if err != nil {
		return err
	}
	return prepared.Stream(ctx, w)
}

// ValidArchivePrefix reports whether prefix is safe to prepend to tar entry
// names: no NUL bytes, no backslashes (Windows extractors treat them as
// separators), not absolute, and no "." or ".." segments — so extracted
// entries cannot escape the destination root (tar slip).
func ValidArchivePrefix(prefix string) bool {
	if strings.ContainsAny(prefix, "\x00\\") || strings.HasPrefix(prefix, "/") {
		return false
	}
	for _, seg := range strings.Split(prefix, "/") {
		if seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// archiveWants applies include (any-of, when present) then exclude (always
// wins) globs to one path.
func archiveWants(p string, opts ArchiveOptions) bool {
	for _, glob := range opts.ExcludeGlobs {
		if MatchGlob(glob, p) {
			return false
		}
	}
	if len(opts.IncludeGlobs) == 0 {
		return true
	}
	for _, glob := range opts.IncludeGlobs {
		if MatchGlob(glob, p) {
			return true
		}
	}
	return false
}

// MatchGlob matches a slash-separated path against one glob pattern:
//
//   - a pattern without "/" matches against the path's base name
//     ("*.md" matches docs/x.md), gitignore-style;
//   - a pattern with "/" matches against the full path, segment by
//     segment with path.Match semantics, where a "**" segment matches any
//     number of segments (including zero).
//
// Malformed patterns match nothing.
func MatchGlob(pattern, p string) bool {
	if pattern == "" {
		return false
	}
	if !strings.Contains(pattern, "/") && pattern != "**" {
		ok, err := path.Match(pattern, path.Base(p))
		return err == nil && ok
	}
	return matchSegments(strings.Split(pattern, "/"), strings.Split(p, "/"))
}

func matchSegments(pat, segs []string) bool {
	if len(pat) == 0 {
		return len(segs) == 0
	}
	if pat[0] == "**" {
		// "**" absorbs zero or more leading segments.
		for skip := 0; skip <= len(segs); skip++ {
			if matchSegments(pat[1:], segs[skip:]) {
				return true
			}
		}
		return false
	}
	if len(segs) == 0 {
		return false
	}
	ok, err := path.Match(pat[0], segs[0])
	if err != nil || !ok {
		return false
	}
	return matchSegments(pat[1:], segs[1:])
}
