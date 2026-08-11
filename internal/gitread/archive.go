package gitread

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"codestorage/internal/ref"
)

// ArchiveOptions tune one Archive stream. The zero value archives the whole
// tree with no prefix.
type ArchiveOptions struct {
	// Prefix is prepended verbatim to every entry name (callers include the
	// trailing "/", e.g. "repo-main/").
	Prefix string
	// IncludeGlobs whitelists paths when non-empty; ExcludeGlobs always
	// wins. See MatchGlob for the dialect.
	IncludeGlobs []string
	ExcludeGlobs []string
	// MaxBlobSize (>0) skips file blobs larger than this many bytes —
	// the reason this is a custom walk instead of `git archive`.
	MaxBlobSize int64
}

// Archive streams a tar.gz of the tree at commit (a resolved full SHA) into
// w: regular blobs (mode 0644, or 0755 for git mode 100755) and symlinks
// (target = blob content); submodules are skipped. Entry mtimes are the
// commit's committer time. Errors after bytes were written can only
// truncate the stream — HTTP callers have already sent headers.
func Archive(ctx context.Context, repoPath, commit string, opts ArchiveOptions, w io.Writer) error {
	assertf(ref.ValidSHA(commit), "Archive: unresolved commit %q", commit)
	assertf(w != nil, "Archive: nil writer")

	commits, err := Log(ctx, repoPath, commit, LogOptions{MaxCount: 1})
	if err != nil {
		return err
	}
	if len(commits) != 1 {
		return fmt.Errorf("archive: commit %s not found", commit)
	}
	mtime := commits[0].CommitterDate

	entries, err := LsTree(ctx, repoPath, commit, "", true)
	if err != nil {
		return err
	}

	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		if e.Type == EntrySubmodule || e.Type == EntryTree {
			continue
		}
		if !archiveWants(e.Path, opts) {
			continue
		}
		if opts.MaxBlobSize > 0 && e.Type == EntryBlob && e.Size > opts.MaxBlobSize {
			continue
		}
		hdr := &tar.Header{
			Name:    opts.Prefix + e.Path,
			ModTime: mtime,
		}
		switch e.Type {
		case EntrySymlink:
			target, err := ReadBlob(ctx, repoPath, e.SHA)
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
			if err := CatBlobTo(ctx, repoPath, e.SHA, tw); err != nil {
				return err
			}
		default:
			return fmt.Errorf("archive: unexpected entry type %q for %s", e.Type, e.Path)
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
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
