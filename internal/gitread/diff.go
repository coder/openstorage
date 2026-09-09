package gitread

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/coder/openstorage/internal/ref"
)

// FileDiff is one changed file between two trees. Status is git's raw
// status letter (A/M/D/R/C/T — docs/reference/http-api.md "state (raw letter)"); rename/copy
// similarity scores are stripped.
type FileDiff struct {
	Path      string // post-image path (the old path for deletions)
	OldPath   string // pre-image path for renames/copies; "" otherwise
	Status    string // "A", "M", "D", "R", "C", or "T"
	Additions int    // added lines; -1 for binary files (numstat "-")
	Deletions int    // deleted lines; -1 for binary files
	Binary    bool
	// Patch is this file's slice of `git diff --patch` output, starting at
	// its `diff --git` header line. Endpoint slices own the final `raw`
	// formatting (hunks-only vs git-apply-compatible, docs/explanation/domain-model.md:93-95).
	Patch string
}

// DiffStats aggregates a diff (docs/reference/http-api.md stats envelope). Binary files count
// toward Files but contribute 0 additions/deletions.
type DiffStats struct {
	Files     int
	Additions int
	Deletions int
	Changes   int // Additions + Deletions
}

// DiffOptions tune one Diff invocation. The zero value diffs the full trees.
type DiffOptions struct {
	Paths []string // optional pathspec filter (`-- <path>...`, repeatable `path` params)
}

// Diff computes the tree diff base..target (both resolved full SHAs; pass
// EmptyTreeSHA as base for root commits). Rename/copy detection is always
// on (-M) so renames surface as one R entry with OldPath, matching the
// docs/reference/http-api.md FileDiff shape.
func Diff(ctx context.Context, repoPath, base, target string, opts DiffOptions) ([]FileDiff, DiffStats, error) {
	assertf(ref.ValidSHA(base) || base == EmptyTreeSHA, "Diff: unresolved base %q", base)
	assertf(ref.ValidSHA(target) || target == EmptyTreeSHA, "Diff: unresolved target %q", target)

	common := []string{"-M", base, target}
	if len(opts.Paths) > 0 {
		common = append(append(common, "--"), opts.Paths...)
	}

	nameStatus, err := runGit(ctx, repoPath, append([]string{"diff", "--no-color", "--name-status", "-z"}, common...)...)
	if err != nil {
		return nil, DiffStats{}, err
	}
	files, err := parseNameStatus(nameStatus)
	if err != nil {
		return nil, DiffStats{}, err
	}

	numstat, err := runGit(ctx, repoPath, append([]string{"diff", "--no-color", "--numstat", "-z"}, common...)...)
	if err != nil {
		return nil, DiffStats{}, err
	}
	if err := mergeNumstat(files, numstat); err != nil {
		return nil, DiffStats{}, err
	}

	patch, err := runGit(ctx, repoPath, append([]string{"diff", "--no-color", "--patch"}, common...)...)
	if err != nil {
		return nil, DiffStats{}, err
	}
	if err := mergePatches(files, patch); err != nil {
		return nil, DiffStats{}, err
	}

	stats := DiffStats{Files: len(files)}
	for i := range files {
		if !files[i].Binary {
			stats.Additions += files[i].Additions
			stats.Deletions += files[i].Deletions
		}
	}
	stats.Changes = stats.Additions + stats.Deletions
	return files, stats, nil
}

// parseNameStatus parses `diff --name-status -z` records:
// STATUS \0 path \0 — or STATUS \0 old \0 new \0 for R/C (score stripped).
func parseNameStatus(out string) ([]FileDiff, error) {
	fields := strings.Split(out, "\x00")
	// Trailing NUL yields one empty tail field.
	if n := len(fields); n > 0 && fields[n-1] == "" {
		fields = fields[:n-1]
	}
	files := []FileDiff{}
	for i := 0; i < len(fields); {
		status := fields[i]
		if status == "" {
			return nil, fmt.Errorf("diff --name-status emitted empty status at %d", i)
		}
		letter := status[:1]
		switch letter {
		case "R", "C":
			if i+2 >= len(fields) {
				return nil, fmt.Errorf("diff --name-status truncated %q record", status)
			}
			files = append(files, FileDiff{Status: letter, OldPath: fields[i+1], Path: fields[i+2]})
			i += 3
		case "A", "M", "D", "T":
			if i+1 >= len(fields) {
				return nil, fmt.Errorf("diff --name-status truncated %q record", status)
			}
			files = append(files, FileDiff{Status: letter, Path: fields[i+1]})
			i += 2
		default:
			// U (unmerged) and X cannot appear in a two-tree diff.
			return nil, fmt.Errorf("diff --name-status emitted status %q", status)
		}
	}
	return files, nil
}

// mergeNumstat fills Additions/Deletions/Binary from `diff --numstat -z`,
// which emits records in the same file order as --name-status:
// added TAB deleted TAB path \0 — or added TAB deleted TAB \0 old \0 new \0
// for renames/copies. Binary files carry "-" counters.
func mergeNumstat(files []FileDiff, out string) error {
	recs := strings.Split(out, "\x00")
	idx := 0
	for i := 0; i < len(recs); i++ {
		rec := recs[i]
		if rec == "" {
			continue
		}
		parts := strings.SplitN(rec, "\t", 3)
		if len(parts) != 3 {
			return fmt.Errorf("diff --numstat emitted %q", rec)
		}
		added, deleted, path := parts[0], parts[1], parts[2]
		if path == "" {
			// Rename/copy: the two path fields follow as separate records.
			if i+2 >= len(recs) {
				return fmt.Errorf("diff --numstat truncated rename record %q", rec)
			}
			path = recs[i+2] // post-image path
			i += 2
		}
		if idx >= len(files) {
			return fmt.Errorf("diff --numstat has more records than --name-status (at %q)", path)
		}
		f := &files[idx]
		if f.Path != path {
			return fmt.Errorf("diff order mismatch: numstat %q vs name-status %q", path, f.Path)
		}
		if added == "-" || deleted == "-" {
			f.Binary, f.Additions, f.Deletions = true, -1, -1
		} else {
			a, errA := strconv.Atoi(added)
			d, errD := strconv.Atoi(deleted)
			if errA != nil || errD != nil {
				return fmt.Errorf("diff --numstat emitted counters %q/%q", added, deleted)
			}
			f.Additions, f.Deletions = a, d
		}
		idx++
	}
	if idx != len(files) {
		return fmt.Errorf("diff --numstat covered %d of %d files", idx, len(files))
	}
	return nil
}

// mergePatches splits one `diff --patch` stream on its `diff --git` header
// lines and assigns the slices to files in order (git emits --patch,
// --numstat, and --name-status in the same file order). Content lines can
// never start with "diff --git " at column 0: they carry a ' ', '+', or '-'
// prefix.
func mergePatches(files []FileDiff, patch string) error {
	if len(files) == 0 {
		if strings.TrimSpace(patch) != "" {
			return fmt.Errorf("diff --patch emitted output for an empty file list")
		}
		return nil
	}
	var starts []int
	for pos := 0; pos < len(patch); {
		line := patch[pos:]
		if strings.HasPrefix(line, "diff --git ") {
			starts = append(starts, pos)
		}
		next := strings.IndexByte(line, '\n')
		if next < 0 {
			break
		}
		pos += next + 1
	}
	if len(starts) != len(files) {
		return fmt.Errorf("diff --patch has %d file headers, want %d", len(starts), len(files))
	}
	for i := range files {
		end := len(patch)
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		files[i].Patch = patch[starts[i]:end]
	}
	return nil
}
