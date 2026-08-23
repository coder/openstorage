// Read-endpoint handlers, query group (docs/archive/phases/07-implementation-roadmap.md p3 phases 2c/2d): commit and
// branch diffs (GET /repos/diff, GET /repos/branches/diff), blame, grep, and
// tar.gz archive streaming. Contracts: docs/reference/http-api.md, docs/explanation/domain-model.md (invariants + the
// "consumers concatenate files[].raw" rule), docs/upstream/openapi.yaml.
//
// All git plumbing goes through internal/gitread over a cache repo
// materialized per request; handlers own parameter validation, filtering,
// cursoring, and wire envelopes.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/gitread"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
)

// maxInlineDiffBytes is the large-file filtering threshold for the diff
// endpoints: files whose relevant blob exceeds it are reported in
// `filtered_files` (no raw/counts) unless the request narrowed the diff to
// explicit `path` params, which bypasses size filtering (openapi: "Repeated
// path filters ... bypass the usual large-file filtering for those requested
// paths"). Neither docs/reference/http-api.md nor openapi pin an exact number; 1 MiB matches
// the "review-oriented" intent of the filtered example (50 KB lockfile).
const maxInlineDiffBytes = 1 << 20

// maxBlameRanges caps repeatable `range` params (openapi: "may be repeated
// to blame multiple regions (max 16)").
const maxBlameRanges = 16

// Grep limits (openapi POST /repos/grep): max_lines "default 2000, max
// 2000", max_matches_per_file "default 200", pagination.limit "default 200".
const (
	grepMaxLinesCap       = 2000
	grepDefaultMaxLines   = 2000
	grepDefaultMatchesPer = 200
	grepDefaultPageLimit  = 200
)

// resolveQueryRepo maps the verified JWT repo claim onto its repo row (404
// when unknown or soft-deleted) and materializes the cache repo for read
// plumbing. ok=false means the response has been written.
func (s *server) resolveQueryRepo(w http.ResponseWriter, r *http.Request) (meta.Repo, string, bool) {
	claims := mustClaims(r)
	repo, err := s.store.GetRepoByURL(r.Context(), claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return meta.Repo{}, "", false
	case err != nil:
		s.internalErr(w, r, "resolve repository", err)
		return meta.Repo{}, "", false
	}
	assertf(repo.RepoID != "", "store returned live repo with empty RepoID")
	repoPath, err := gitread.Materialize(r.Context(), s.refStore, s.blobStore, s.cacheDir, repo.RepoID, repo.DefaultBranch)
	if err != nil {
		s.internalErr(w, r, "materialize repository", err)
		return meta.Repo{}, "", false
	}
	return repo, repoPath, true
}

// Boolean query params are parsed via coreBoolParam (handlers_read_core.go)
// — one literal "true"/"false" dialect for the whole read surface.

// cleanPathParams validates repeatable `path` filters: no empties, no NULs.
func cleanPathParams(paths []string) ([]string, error) {
	for _, p := range paths {
		if strings.TrimSpace(p) == "" || strings.ContainsRune(p, 0) {
			return nil, fmt.Errorf("invalid path filter %q", p)
		}
	}
	return paths, nil
}

// mergeBaseSHA returns the merge base of a and b in the repo at repoPath;
// ok=false (no error) when the histories are unrelated.
func mergeBaseSHA(ctx context.Context, repoPath, a, b string) (string, bool, error) {
	// Callers pass ResolveRef outputs, so non-SHA inputs are a programming
	// error — but returning it (500) beats a panic that kills the process.
	if !ref.ValidSHA(a) || !ref.ValidSHA(b) {
		return "", false, fmt.Errorf("mergeBaseSHA: unresolved inputs %q/%q", a, b)
	}
	cmd := exec.CommandContext(ctx, "git", "merge-base", a, b)
	cmd.Dir = repoPath
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return "", false, nil // no common ancestor
		}
		return "", false, fmt.Errorf("git merge-base %s %s: %w", a, b, err)
	}
	sha := strings.TrimSpace(string(out))
	if !ref.ValidSHA(sha) {
		return "", false, fmt.Errorf("git merge-base emitted %q", sha)
	}
	return sha, true, nil
}

// --- diff payload building ---------------------------------------------

type diffStatsPayload struct {
	Files     int `json:"files"`
	Additions int `json:"additions"`
	Deletions int `json:"deletions"`
	Changes   int `json:"changes"`
}

// fileDiffEntry is the docs/reference/http-api.md FileDiff wire shape. Raw always carries the
// full per-file `diff --git` patch (docs/explanation/domain-model.md:93-95: consumers concatenate
// raws directly, and gitApplyCompatible=true concatenations must pass
// `git apply`) — byte-exact `git diff` output satisfies both modes, so the
// gitApplyCompatible flag is validated but does not alter formatting.
type fileDiffEntry struct {
	Path      string `json:"path"`
	State     string `json:"state"`
	OldPath   string `json:"old_path,omitempty"`
	Raw       string `json:"raw"`
	Bytes     int64  `json:"bytes"`
	IsEOF     bool   `json:"is_eof"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

// filteredFileEntry is a diff entry excluded from inline output (binary
// content, or size above maxInlineDiffBytes): no raw, no counts.
type filteredFileEntry struct {
	Path    string `json:"path"`
	State   string `json:"state"`
	OldPath string `json:"old_path,omitempty"`
	Bytes   int64  `json:"bytes"`
	IsEOF   bool   `json:"is_eof"`
}

type diffPayload struct {
	Stats    diffStatsPayload
	Files    []fileDiffEntry
	Filtered []filteredFileEntry
}

// lastByteWriter tracks the final byte of a stream (trailing-newline probe
// for is_eof) without buffering blob contents.
type lastByteWriter struct{ last byte }

func (w *lastByteWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.last = p[len(p)-1]
	}
	return len(p), nil
}

// statDiffBlob resolves size and trailing-newline state (`bytes`/`is_eof`)
// of commit:path. Submodule entries have no blob content: size 0, no eof.
func statDiffBlob(ctx context.Context, repoPath, commit, path string) (int64, bool, error) {
	entries, err := gitread.LsTree(ctx, repoPath, commit, path, false)
	if err != nil {
		return 0, false, err
	}
	var entry *gitread.TreeEntry
	for i := range entries {
		if entries[i].Path == path {
			entry = &entries[i]
			break
		}
	}
	if entry == nil {
		return 0, false, fmt.Errorf("diff path %q missing at %s", path, commit)
	}
	if entry.Size <= 0 {
		return 0, false, nil // empty blob or submodule pointer: no newline
	}
	var last lastByteWriter
	if err := gitread.CatBlobTo(ctx, repoPath, entry.SHA, &last); err != nil {
		return 0, false, err
	}
	return entry.Size, last.last == '\n', nil
}

// buildDiffPayload computes the base..target tree diff and splits it into
// inline files and filtered_files. Explicit path filters bypass large-file
// filtering (the whole diff is then "requested"); binary files are always
// filtered — their patch is a non-applyable "Binary files differ" stub.
func buildDiffPayload(ctx context.Context, repoPath, base, target string, paths []string) (diffPayload, error) {
	fileDiffs, stats, err := gitread.Diff(ctx, repoPath, base, target, gitread.DiffOptions{Paths: paths})
	if err != nil {
		return diffPayload{}, err
	}
	assertf(stats.Files == len(fileDiffs), "diff stats/files mismatch: %d vs %d", stats.Files, len(fileDiffs))
	bypassSize := len(paths) > 0
	payload := diffPayload{
		Stats: diffStatsPayload(stats),
		// Empty slices, not nil: files/filtered_files are required arrays.
		Files:    []fileDiffEntry{},
		Filtered: []filteredFileEntry{},
	}
	for _, f := range fileDiffs {
		statCommit := target
		if f.Status == "D" {
			// Deletions stat the pre-image side. A root diff (base ==
			// EmptyTreeSHA) has no pre-image; statDiffBlob then reports the
			// missing path as an error (500), never a process-killing panic.
			statCommit = base
		}
		size, isEOF, err := statDiffBlob(ctx, repoPath, statCommit, f.Path)
		if err != nil {
			return diffPayload{}, err
		}
		if f.Binary || (!bypassSize && size > maxInlineDiffBytes) {
			payload.Filtered = append(payload.Filtered, filteredFileEntry{
				Path:    f.Path,
				State:   f.Status,
				OldPath: f.OldPath,
				Bytes:   size,
				IsEOF:   isEOF,
			})
			continue
		}
		payload.Files = append(payload.Files, fileDiffEntry{
			Path:      f.Path,
			State:     f.Status,
			OldPath:   f.OldPath,
			Raw:       f.Patch,
			Bytes:     size,
			IsEOF:     isEOF,
			Additions: f.Additions,
			Deletions: f.Deletions,
		})
	}
	return payload, nil
}

// --- GET /api/v1/repos/diff (2c) -----------------------------------------

type commitDiffResponse struct {
	SHA           string              `json:"sha"`
	MergeBaseSHA  string              `json:"merge_base_sha"`
	Stats         diffStatsPayload    `json:"stats"`
	Files         []fileDiffEntry     `json:"files"`
	FilteredFiles []filteredFileEntry `json:"filtered_files"`
}

// handleDiff implements GET /api/v1/repos/diff: `ref` and `base_ref` accept
// any revision (deprecated camelCase aliases `sha`/`baseSha` — the
// endpoint's historical quirk; note `base_sha` was never a param and stays
// an ignored unknown). `ref_is_ephemeral`/`base_is_ephemeral` resolve either
// side in the ephemeral namespace. Explicit base_ref selects a three-dot
// comparison: the diff runs from merge-base(base_ref, ref) to ref with
// merge_base_sha set; without base_ref the diff is against the first parent
// (root commits: empty tree) and merge_base_sha is empty (openapi: "Empty
// for single-commit diffs").
func (s *server) handleDiff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	refParam := strings.TrimSpace(coreAliasParam(q, "ref", "sha"))
	if refParam == "" {
		writeErr(w, http.StatusBadRequest, "ref is required")
		return
	}
	refEphemeral, err := coreBoolParam(q, "ref_is_ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	baseEphemeral, err := coreBoolParam(q, "base_is_ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// Validated for shape only: raw output is byte-exact `git diff` patches
	// in both modes, which already satisfies the apply-compatibility
	// invariant (see fileDiffEntry).
	if _, err := coreAliasBoolParam(q, "git_apply_compatible", "gitApplyCompatible"); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	paths, err := cleanPathParams(q["path"])
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	_, repoPath, ok := s.resolveQueryRepo(w, r)
	if !ok {
		return
	}
	target, err := gitread.ResolveRef(r.Context(), repoPath, refParam, refEphemeral)
	if errors.Is(err, gitread.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "commit not found")
		return
	} else if err != nil {
		s.internalErr(w, r, "resolve diff ref", err)
		return
	}

	base := gitread.EmptyTreeSHA
	mergeBase := ""
	if baseParam := strings.TrimSpace(coreAliasParam(q, "base_ref", "baseSha")); baseParam != "" {
		baseCommit, err := gitread.ResolveRef(r.Context(), repoPath, baseParam, baseEphemeral)
		if errors.Is(err, gitread.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "base commit not found")
			return
		} else if err != nil {
			s.internalErr(w, r, "resolve diff base_ref", err)
			return
		}
		mb, found, err := mergeBaseSHA(r.Context(), repoPath, baseCommit, target)
		if err != nil {
			s.internalErr(w, r, "compute merge base", err)
			return
		}
		if found {
			base, mergeBase = mb, mb
		} else {
			base = baseCommit // unrelated histories: direct two-dot diff
		}
	} else {
		commits, err := gitread.Log(r.Context(), repoPath, target, gitread.LogOptions{MaxCount: 1})
		if err != nil {
			s.internalErr(w, r, "load diff commit", err)
			return
		}
		assertf(len(commits) == 1, "log -n1 %s returned %d commits", target, len(commits))
		if parents := commits[0].ParentSHAs; len(parents) > 0 {
			base = parents[0]
		}
	}

	payload, err := buildDiffPayload(r.Context(), repoPath, base, target, paths)
	if err != nil {
		s.internalErr(w, r, "compute diff", err)
		return
	}
	writeJSON(w, http.StatusOK, commitDiffResponse{
		SHA:           target,
		MergeBaseSHA:  mergeBase,
		Stats:         payload.Stats,
		Files:         payload.Files,
		FilteredFiles: payload.Filtered,
	})
}

// --- GET /api/v1/repos/branches/diff (2c) --------------------------------

type branchDiffResponse struct {
	Branch        string              `json:"branch"`
	Base          string              `json:"base"`
	MergeBaseSHA  string              `json:"merge_base_sha"`
	Stats         diffStatsPayload    `json:"stats"`
	Files         []fileDiffEntry     `json:"files"`
	FilteredFiles []filteredFileEntry `json:"filtered_files"`
}

// handleBranchesDiff implements GET /api/v1/repos/branches/diff: the
// three-dot review diff merge-base(base, branch)..branch. `base` defaults
// to the repo default branch; `ephemeral`/`ephemeral_base` resolve either
// side in the ephemeral namespace. Unrelated histories degrade to a direct
// base..branch diff with an empty merge_base_sha.
func (s *server) handleBranchesDiff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	branchParam := strings.TrimSpace(q.Get("branch"))
	if branchParam == "" {
		writeErr(w, http.StatusBadRequest, "branch is required")
		return
	}
	ephemeral, err := coreBoolParam(q, "ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ephemeralBase, err := coreBoolParam(q, "ephemeral_base")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	paths, err := cleanPathParams(q["path"])
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	repo, repoPath, ok := s.resolveQueryRepo(w, r)
	if !ok {
		return
	}
	branchSHA, err := gitread.ResolveRef(r.Context(), repoPath, branchParam, ephemeral)
	if errors.Is(err, gitread.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "branch not found")
		return
	} else if err != nil {
		s.internalErr(w, r, "resolve diff branch", err)
		return
	}
	baseName := strings.TrimSpace(q.Get("base"))
	if baseName == "" {
		baseName = repo.DefaultBranch
	}
	baseSHA, err := gitread.ResolveRef(r.Context(), repoPath, baseName, ephemeralBase)
	if errors.Is(err, gitread.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "base not found")
		return
	} else if err != nil {
		s.internalErr(w, r, "resolve diff base", err)
		return
	}

	diffBase, mergeBase := baseSHA, ""
	if mb, found, err := mergeBaseSHA(r.Context(), repoPath, baseSHA, branchSHA); err != nil {
		s.internalErr(w, r, "compute merge base", err)
		return
	} else if found {
		diffBase, mergeBase = mb, mb
	}

	payload, err := buildDiffPayload(r.Context(), repoPath, diffBase, branchSHA, paths)
	if err != nil {
		s.internalErr(w, r, "compute branch diff", err)
		return
	}
	writeJSON(w, http.StatusOK, branchDiffResponse{
		Branch:        branchParam,
		Base:          baseName,
		MergeBaseSHA:  mergeBase,
		Stats:         payload.Stats,
		Files:         payload.Files,
		FilteredFiles: payload.Filtered,
	})
}

// --- GET /api/v1/repos/blame (2d) ----------------------------------------

type blameLineEntry struct {
	LineNumber         int    `json:"line_number"`
	CommitSHA          string `json:"commit_sha"`
	OriginalLineNumber int    `json:"original_line_number"`
	OriginalPath       string `json:"original_path"`
	PreviousCommitSHA  string `json:"previous_commit_sha,omitempty"`
	AuthorName         string `json:"author_name"`
	AuthorEmail        string `json:"author_email"`
	AuthorTime         string `json:"author_time"`
	CommitterName      string `json:"committer_name"`
	CommitterEmail     string `json:"committer_email"`
	CommitterTime      string `json:"committer_time"`
	Summary            string `json:"summary"`
}

type blameResponse struct {
	Ref       string           `json:"ref"`
	Path      string           `json:"path"`
	CommitSHA string           `json:"commit_sha"`
	Lines     []blameLineEntry `json:"lines"`
}

// handleBlame implements GET /api/v1/repos/blame: per-line attribution of
// `path` at `ref` (default branch when omitted), with repeatable `git blame
// -L`-style `range` specs (max 16) and optional move detection.
func (s *server) handleBlame(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	path := q.Get("path")
	if strings.TrimSpace(path) == "" {
		writeErr(w, http.StatusBadRequest, "path is required")
		return
	}
	ephemeral, err := coreBoolParam(q, "ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	detectMoves, err := coreBoolParam(q, "detect_moves")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ranges := q["range"]
	if len(ranges) > maxBlameRanges {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("too many range params (max %d)", maxBlameRanges))
		return
	}
	for _, spec := range ranges {
		if strings.TrimSpace(spec) == "" || strings.ContainsRune(spec, 0) {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid range %q", spec))
			return
		}
	}

	repo, repoPath, ok := s.resolveQueryRepo(w, r)
	if !ok {
		return
	}
	refName := strings.TrimSpace(q.Get("ref"))
	if refName == "" {
		refName = repo.DefaultBranch
	}
	commit, err := gitread.ResolveRef(r.Context(), repoPath, refName, ephemeral)
	if errors.Is(err, gitread.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "ref not found")
		return
	} else if err != nil {
		s.internalErr(w, r, "resolve blame ref", err)
		return
	}

	lines, err := gitread.Blame(r.Context(), repoPath, commit, path, ranges, detectMoves)
	var exitErr *exec.ExitError
	switch {
	case errors.Is(err, gitread.ErrNotFound):
		writeErr(w, http.StatusNotFound, "path not found")
		return
	case errors.As(err, &exitErr):
		// Ref and path shape were validated above; a git failure here means
		// the -L range specs did not fit the file (bad syntax, out of
		// bounds, unmatched /regex/) — client input, not a server fault.
		writeErr(w, http.StatusBadRequest, "invalid range")
		return
	case err != nil:
		s.internalErr(w, r, "compute blame", err)
		return
	}

	entries := make([]blameLineEntry, 0, len(lines))
	for _, l := range lines {
		entries = append(entries, blameLineEntry{
			LineNumber:         l.LineNumber,
			CommitSHA:          l.CommitSHA,
			OriginalLineNumber: l.OriginalLineNumber,
			OriginalPath:       l.OriginalPath,
			PreviousCommitSHA:  l.PreviousCommitSHA,
			AuthorName:         l.AuthorName,
			AuthorEmail:        l.AuthorEmail,
			AuthorTime:         l.AuthorTime.Format(time.RFC3339),
			CommitterName:      l.CommitterName,
			CommitterEmail:     l.CommitterEmail,
			CommitterTime:      l.CommitterTime.Format(time.RFC3339),
			Summary:            l.Summary,
		})
	}
	writeJSON(w, http.StatusOK, blameResponse{
		Ref:       refName,
		Path:      path,
		CommitSHA: commit,
		Lines:     entries,
	})
}

// --- POST /api/v1/repos/grep (2d) ----------------------------------------

type grepRequestBody struct {
	Query struct {
		Pattern       string `json:"pattern"`
		CaseSensitive *bool  `json:"case_sensitive"`
	} `json:"query"`
	Ref       string   `json:"ref"`
	Rev       string   `json:"rev"` // legacy alias (openapi); `ref` wins
	Ephemeral bool     `json:"ephemeral"`
	Paths     []string `json:"paths"`

	FileFilters struct {
		IncludeGlobs     []string `json:"include_globs"`
		ExcludeGlobs     []string `json:"exclude_globs"`
		ExtensionFilters []string `json:"extension_filters"`
	} `json:"file_filters"`
	Context struct {
		Before *int `json:"before"`
		After  *int `json:"after"`
	} `json:"context"`
	Limits struct {
		MaxLines          *int `json:"max_lines"`
		MaxMatchesPerFile *int `json:"max_matches_per_file"`
	} `json:"limits"`
	Pagination struct {
		Cursor string `json:"cursor"`
		Limit  *int   `json:"limit"`
	} `json:"pagination"`
}

type grepLineEntry struct {
	LineNumber int    `json:"line_number"`
	Text       string `json:"text"`
	Type       string `json:"type"`
}

type grepFileMatch struct {
	Path  string          `json:"path"`
	Lines []grepLineEntry `json:"lines"`
}

type grepResponse struct {
	Query struct {
		Pattern       string `json:"pattern"`
		CaseSensitive bool   `json:"case_sensitive"`
	} `json:"query"`
	Repo struct {
		Ref    string `json:"ref"`
		Commit string `json:"commit"`
	} `json:"repo"`
	Matches           []grepFileMatch `json:"matches"`
	NextCursor        string          `json:"next_cursor"`
	HasMore           bool            `json:"has_more"`
	IncompleteResults bool            `json:"incomplete_results"`
}

// encodeGrepCursor renders the keyset position of the last delivered line:
// base64url (unpadded, ADR-6 style) of "commit \x00 path \x00 line". Pages
// stay anchored to one commit so concurrent pushes cannot shift results.
func encodeGrepCursor(commit, path string, line int) string {
	assertf(ref.ValidSHA(commit), "encodeGrepCursor: unresolved commit %q", commit)
	assertf(path != "" && line > 0, "encodeGrepCursor: bad position %q:%d", path, line)
	raw := commit + "\x00" + path + "\x00" + strconv.Itoa(line)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeGrepCursor parses an opaque grep cursor; any malformed input maps
// to a 400 at the callsite.
func decodeGrepCursor(cursor string) (commit, path string, line int, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid cursor")
	}
	parts := strings.SplitN(string(raw), "\x00", 3)
	if len(parts) != 3 || !ref.ValidSHA(parts[0]) || parts[1] == "" {
		return "", "", 0, fmt.Errorf("invalid cursor")
	}
	n, convErr := strconv.Atoi(parts[2])
	if convErr != nil || n < 1 {
		return "", "", 0, fmt.Errorf("invalid cursor")
	}
	return parts[0], parts[1], n, nil
}

// grepFileWanted applies the request's path prefixes and file filters to
// one candidate path. Filtering runs server-side over the full grep result
// (one uniform glob dialect — gitread.MatchGlob — shared with archive).
func grepFileWanted(path string, body *grepRequestBody) bool {
	if len(body.Paths) > 0 {
		matched := false
		for _, prefix := range body.Paths {
			trimmed := strings.TrimSuffix(prefix, "/")
			if path == trimmed || strings.HasPrefix(path, trimmed+"/") {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, glob := range body.FileFilters.ExcludeGlobs {
		if gitread.MatchGlob(glob, path) {
			return false
		}
	}
	includes := len(body.FileFilters.IncludeGlobs) > 0 || len(body.FileFilters.ExtensionFilters) > 0
	if !includes {
		return true
	}
	for _, glob := range body.FileFilters.IncludeGlobs {
		if gitread.MatchGlob(glob, path) {
			return true
		}
	}
	for _, ext := range body.FileFilters.ExtensionFilters {
		if ext != "" && strings.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}

// grepPosition is one flattened output line (pagination unit).
type grepPosition struct {
	path string
	line grepLineEntry
}

// handleGrep implements POST /api/v1/repos/grep: synchronous git grep at
// the resolved ref (docs/explanation/domain-model.md allows strictly-fresher-than-index synchronous
// results). Limits truncate the collected result (incomplete_results);
// pagination pages through the truncated set with a commit-anchored keyset
// cursor, so every page reflects one commit.
func (s *server) handleGrep(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	var body grepRequestBody
	if err := json.Unmarshal(raw, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	pattern := body.Query.Pattern
	if strings.TrimSpace(pattern) == "" {
		writeErr(w, http.StatusBadRequest, "query.pattern is required")
		return
	}
	caseSensitive := true
	if body.Query.CaseSensitive != nil {
		caseSensitive = *body.Query.CaseSensitive
	}
	before, after := 0, 0
	if body.Context.Before != nil {
		before = *body.Context.Before
	}
	if body.Context.After != nil {
		after = *body.Context.After
	}
	if before < 0 || after < 0 {
		writeErr(w, http.StatusBadRequest, "context lines must be non-negative")
		return
	}
	maxLines := grepDefaultMaxLines
	if body.Limits.MaxLines != nil {
		maxLines = *body.Limits.MaxLines
		if maxLines < 1 || maxLines > grepMaxLinesCap {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid limits.max_lines (want 1..%d)", grepMaxLinesCap))
			return
		}
	}
	maxPerFile := grepDefaultMatchesPer
	if body.Limits.MaxMatchesPerFile != nil {
		maxPerFile = *body.Limits.MaxMatchesPerFile
		if maxPerFile < 1 {
			writeErr(w, http.StatusBadRequest, "invalid limits.max_matches_per_file (want >= 1)")
			return
		}
	}
	pageLimit := grepDefaultPageLimit
	if body.Pagination.Limit != nil {
		pageLimit = *body.Pagination.Limit
		if pageLimit < 1 {
			writeErr(w, http.StatusBadRequest, "invalid pagination.limit (want >= 1)")
			return
		}
	}
	for _, p := range body.Paths {
		if strings.TrimSpace(p) == "" || strings.ContainsRune(p, 0) {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid paths entry %q", p))
			return
		}
	}
	// Narrow the git-side search to the requested path prefixes (openapi:
	// "path prefixes used to narrow the search before pattern matching").
	// `:(literal)` magic keeps the documented plain-prefix semantics (no
	// glob dialect) and stops request input from smuggling other pathspec
	// magic. grepFileWanted below stays as the authoritative output filter.
	pathspecs := make([]string, 0, len(body.Paths))
	for _, p := range body.Paths {
		if trimmed := strings.TrimSuffix(p, "/"); trimmed != "" {
			pathspecs = append(pathspecs, ":(literal)"+trimmed)
		}
	}

	repo, repoPath, ok := s.resolveQueryRepo(w, r)
	if !ok {
		return
	}
	refName := strings.TrimSpace(body.Ref)
	if refName == "" {
		refName = strings.TrimSpace(body.Rev)
	}
	if refName == "" {
		refName = repo.DefaultBranch
	}

	var commit string
	var cursorPath string
	var cursorLine int
	if body.Pagination.Cursor != "" {
		commit, cursorPath, cursorLine, err = decodeGrepCursor(body.Pagination.Cursor)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		// The cursor's anchor commit must still exist in this repo.
		if _, resolveErr := gitread.ResolveRef(r.Context(), repoPath, commit, false); resolveErr != nil {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		}
	} else {
		commit, err = gitread.ResolveRef(r.Context(), repoPath, refName, body.Ephemeral)
		if errors.Is(err, gitread.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "ref not found")
			return
		} else if err != nil {
			s.internalErr(w, r, "resolve grep ref", err)
			return
		}
	}

	files, err := gitread.Grep(r.Context(), repoPath, commit, gitread.GrepOptions{
		Pattern:         pattern,
		CaseInsensitive: !caseSensitive,
		Pathspecs:       pathspecs,
		BeforeContext:   before,
		AfterContext:    after,
	})
	var grepExitErr *exec.ExitError
	switch {
	case errors.As(err, &grepExitErr) && grepExitErr.ExitCode() == 128:
		// The commit is a resolved SHA and pathspecs carry only :(literal)
		// magic, so a git usage failure (exit 128) means the request's
		// pattern is not a valid regex — client input, not a server fault.
		writeErr(w, http.StatusBadRequest, "invalid query.pattern")
		return
	case err != nil:
		s.internalErr(w, r, "run grep", err)
		return
	}

	// Filter files, truncate per-file matches then total lines, flatten.
	incomplete := false
	var flat []grepPosition
	totalLines := 0
truncated:
	for _, f := range files {
		if !grepFileWanted(f.Path, &body) {
			continue
		}
		matchCount := 0
		for _, l := range f.Lines {
			if l.Type == gitread.GrepLineMatch {
				matchCount++
				if matchCount > maxPerFile {
					incomplete = true
					break // drop the rest of this file's lines
				}
			}
			if totalLines >= maxLines {
				incomplete = true
				break truncated
			}
			flat = append(flat, grepPosition{path: f.Path, line: grepLineEntry(l)})
			totalLines++
		}
	}

	start := 0
	if body.Pagination.Cursor != "" {
		start = -1
		for i, pos := range flat {
			if pos.path == cursorPath && pos.line.LineNumber == cursorLine {
				start = i + 1
				break
			}
		}
		if start < 0 {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		}
	}
	end := min(start+pageLimit, len(flat))
	page := flat[start:end]

	matches := []grepFileMatch{}
	for _, pos := range page {
		if n := len(matches); n == 0 || matches[n-1].Path != pos.path {
			matches = append(matches, grepFileMatch{Path: pos.path, Lines: []grepLineEntry{}})
		}
		last := &matches[len(matches)-1]
		last.Lines = append(last.Lines, pos.line)
	}

	resp := grepResponse{
		Matches:           matches,
		HasMore:           end < len(flat),
		IncompleteResults: incomplete,
	}
	resp.Query.Pattern = pattern
	resp.Query.CaseSensitive = caseSensitive
	resp.Repo.Ref = refName
	resp.Repo.Commit = commit
	if resp.HasMore {
		lastPos := page[len(page)-1]
		resp.NextCursor = encodeGrepCursor(commit, lastPos.path, lastPos.line.LineNumber)
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- POST /api/v1/repos/archive (2d) --------------------------------------

type archiveRequestBody struct {
	Ref          string   `json:"ref"`
	IncludeGlobs []string `json:"include_globs"`
	ExcludeGlobs []string `json:"exclude_globs"`
	MaxBlobSize  *int64   `json:"max_blob_size"`
	Archive      struct {
		Prefix string `json:"prefix"`
	} `json:"archive"`
}

// archiveDispositionRe strips everything that could break the quoted
// Content-Disposition filename token down to a conservative charset.
func archiveFilenamePart(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// handleArchive implements POST /api/v1/repos/archive: a gz-compressed
// tarball of the tree at `ref` (default branch when omitted), shaped by
// include/exclude globs, max_blob_size, and an optional entry-name prefix.
// The body is optional entirely (the SDK omits it for all-default requests).
func (s *server) handleArchive(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	var body archiveRequestBody
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
	}
	maxBlobSize := int64(0)
	if body.MaxBlobSize != nil {
		maxBlobSize = *body.MaxBlobSize
		if maxBlobSize < 0 {
			writeErr(w, http.StatusBadRequest, "invalid max_blob_size (want >= 0)")
			return
		}
	}
	for _, glob := range append(append([]string{}, body.IncludeGlobs...), body.ExcludeGlobs...) {
		if strings.TrimSpace(glob) == "" || strings.ContainsRune(glob, 0) {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid glob %q", glob))
			return
		}
	}
	// Reject prefixes that could make extracted entries escape the
	// destination root (tar slip): absolute paths, "."/".." segments,
	// NUL bytes, backslashes.
	if !gitread.ValidArchivePrefix(body.Archive.Prefix) {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid archive.prefix %q", body.Archive.Prefix))
		return
	}
	// The prefix is a folder name (openapi example "sdk-docs/"); normalize
	// a missing trailing slash so "snap" cannot glue onto entry names.
	prefix := body.Archive.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	repo, repoPath, ok := s.resolveQueryRepo(w, r)
	if !ok {
		return
	}
	refName := strings.TrimSpace(body.Ref)
	if refName == "" {
		refName = repo.DefaultBranch
	}
	commit, err := gitread.ResolveRef(r.Context(), repoPath, refName, false)
	if errors.Is(err, gitread.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "ref not found")
		return
	} else if err != nil {
		s.internalErr(w, r, "resolve archive ref", err)
		return
	}

	// Filename from the repository URL's last path element + the resolved
	// revision (openapi: "generated from the repository URL and resolved
	// revision").
	repoBase := repo.URL
	if idx := strings.LastIndexByte(repoBase, '/'); idx >= 0 {
		repoBase = repoBase[idx+1:]
	}
	repoBase = strings.TrimSuffix(repoBase, ".git")
	if repoBase == "" {
		repoBase = repo.RepoID
	}
	filename := archiveFilenamePart(repoBase) + "-" + archiveFilenamePart(refName) + ".tar.gz"

	// Resolve the commit and walk the tree BEFORE committing a 200:
	// resolution failures still get a clean error response instead of a
	// mid-stream abort.
	prepared, err := gitread.PrepareArchive(r.Context(), repoPath, commit, gitread.ArchiveOptions{
		Prefix:       prefix,
		IncludeGlobs: body.IncludeGlobs,
		ExcludeGlobs: body.ExcludeGlobs,
		MaxBlobSize:  maxBlobSize,
	})
	if err != nil {
		s.internalErr(w, r, "prepare archive", err)
		return
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	if err := prepared.Stream(r.Context(), w); err != nil {
		// Headers are gone; a clean error response is impossible. Abort the
		// connection so the client sees a hard failure instead of silently
		// truncated (checksum-broken) gzip output.
		s.logger.Error("archive stream aborted", "repo", repo.RepoID, "commit", commit, "err", err)
		panic(http.ErrAbortHandler)
	}
}
