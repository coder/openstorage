// Core git:read endpoints (docs/07 p3 phases 2a/2b): refs, commits, files,
// and conditional/range file streaming per docs/03 + docs/01 invariants
// 2 (root commits), 3 (unborn branch 404), 9 (commits newest-first), and
// 10 (annotated tags listed with the peeled commit sha).
package api

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/gitread"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

// ---------------------------------------------------------------------------
// Shared read plumbing: repo resolution, cache materialization, params,
// cursors. Helpers deliberately carry a "core" prefix: sibling endpoint
// files add their own helpers and must not collide in this package.

// resolveReadRepo maps the JWT repo claim to its live repo row; a repo that
// never existed (or is soft-deleted) is a 404 for every read endpoint.
// Writes the error response and returns ok=false on failure.
func (s *server) resolveReadRepo(w http.ResponseWriter, r *http.Request) (meta.Repo, bool) {
	claims := mustClaims(r)
	// requireAuth(orgLevel=false) rejects empty/"org" repo claims with 403.
	assertf(claims.Repo != "" && claims.Repo != auth.RepoClaimOrg, "read: repo claim %q passed requireAuth", claims.Repo)
	repo, err := s.store.GetRepoByURL(r.Context(), claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "repository not found")
		return meta.Repo{}, false
	case err != nil:
		s.internalErr(w, r, "resolve repository", err)
		return meta.Repo{}, false
	}
	assertf(repo.RepoID != "", "store returned repo with empty RepoID")
	return repo, true
}

// materializeReadRepo rebuilds the repo's bare cache from the ref/blob
// stores (DB is the ref authority) and returns its path. Writes a 500 and
// returns ok=false on failure.
func (s *server) materializeReadRepo(w http.ResponseWriter, r *http.Request, repoID string) (string, bool) {
	repoPath, err := gitread.Materialize(r.Context(), s.refStore, s.blobStore, s.cacheDir, repoID)
	if err != nil {
		s.internalErr(w, r, "materialize repository", err)
		return "", false
	}
	return repoPath, true
}

// coreBoolParam parses a literal "true"/"false" query boolean (docs/03);
// absent means false. Anything else is a 400.
func coreBoolParam(q url.Values, name string) (bool, error) {
	switch q.Get(name) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf("invalid %s (want true|false)", name)
	}
}

// coreLimitParam parses the limit query param: absent/0 selects def
// (openapi: minimum 0, "defaults to N"); negative, non-integer, or above
// max is a 400.
func coreLimitParam(q url.Values, def, max int) (int, error) {
	assertf(def > 0 && def <= max, "coreLimitParam: bad defaults %d/%d", def, max)
	raw := q.Get("limit")
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > max {
		return 0, fmt.Errorf("invalid limit (want 0..%d)", max)
	}
	if n == 0 {
		return def, nil
	}
	return n, nil
}

// Cursor kinds namespace the opaque tokens per endpoint family so a cursor
// pasted across endpoints fails closed with a 400 instead of misbehaving.
const (
	cursorKindBranches = "branches"
	cursorKindTags     = "tags"
	cursorKindCommits  = "commits"
	cursorKindFiles    = "files"
	cursorKindFileMeta = "filemeta"
)

// coreCursorEncode renders an opaque keyset cursor: unpadded base64url of
// "<kind>|<field>|..." (the p1 ADR-6 style). The LAST field may contain
// '|' (paths); decode splits accordingly.
func coreCursorEncode(kind string, fields ...string) string {
	assertf(kind != "" && len(fields) > 0, "coreCursorEncode: empty kind/fields")
	raw := kind + "|" + strings.Join(fields, "|")
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// coreCursorDecode parses a cursor of the given kind back into its nFields
// fields. Any malformed input (bad base64, wrong kind, missing fields) is
// reported as an error the handlers map to 400.
func coreCursorDecode(kind, cursor string, nFields int) ([]string, error) {
	assertf(kind != "" && nFields > 0, "coreCursorDecode: empty kind/nFields")
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	parts := strings.SplitN(string(raw), "|", nFields+1)
	if len(parts) != nFields+1 || parts[0] != kind {
		return nil, fmt.Errorf("invalid cursor")
	}
	for _, p := range parts[1:] {
		if p == "" {
			return nil, fmt.Errorf("invalid cursor")
		}
	}
	return parts[1:], nil
}

// ---------------------------------------------------------------------------
// 2a: GET /repos/branches

type branchEntry struct {
	Cursor    string `json:"cursor"`
	Name      string `json:"name"`
	HeadSHA   string `json:"head_sha"`
	CreatedAt string `json:"created_at"`
}

type listBranchesResponse struct {
	Branches   []branchEntry `json:"branches"`
	NextCursor string        `json:"next_cursor"`
	HasMore    bool          `json:"has_more"`
}

// handleListBranches implements GET /api/v1/repos/branches: branch rows come
// straight from the ref authority (name, head sha, created_at), lexically
// ordered, keyset-paginated by name. `ephemeral=true` lists the ephemeral
// namespace instead (docs/02 p2 namespace).
func (s *server) handleListBranches(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, err := coreLimitParam(q, defaultListLimit, maxListLimit)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ephemeral, err := coreBoolParam(q, "ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	after := ""
	if cursor := q.Get("cursor"); cursor != "" {
		fields, err := coreCursorDecode(cursorKindBranches, cursor, 1)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		after = fields[0]
	}

	refs, err := s.refStore.ListRefsWithMeta(r.Context(), repo.RepoID)
	if err != nil {
		s.internalErr(w, r, "list refs", err)
		return
	}
	prefix := "refs/heads/"
	if ephemeral {
		prefix = auth.EphemeralNamespacePrefix + "refs/heads/"
	}
	// ListRefsWithMeta is sorted by full ref name; with a constant prefix the
	// stripped branch names stay in the same lexical order.
	entries := []branchEntry{}
	hasMore := false
	for _, ri := range refs {
		if !strings.HasPrefix(ri.Name, prefix) {
			continue
		}
		name := strings.TrimPrefix(ri.Name, prefix)
		if after != "" && name <= after {
			continue
		}
		if len(entries) == limit {
			hasMore = true
			break
		}
		entries = append(entries, branchEntry{
			Cursor:    coreCursorEncode(cursorKindBranches, name),
			Name:      name,
			HeadSHA:   ri.SHA,
			CreatedAt: ri.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	next := ""
	if hasMore {
		next = entries[len(entries)-1].Cursor
	}
	writeJSON(w, http.StatusOK, listBranchesResponse{Branches: entries, NextCursor: next, HasMore: hasMore})
}

// ---------------------------------------------------------------------------
// 2a: GET /repos/tags

type tagEntry struct {
	Cursor string `json:"cursor"`
	Name   string `json:"name"`
	SHA    string `json:"sha"`
}

type listTagsResponse struct {
	Tags       []tagEntry `json:"tags"`
	NextCursor string     `json:"next_cursor"`
	HasMore    bool       `json:"has_more"`
}

// handleListTags implements GET /api/v1/repos/tags: lexical order, keyset
// cursor = tag name, and `sha` is the peeled commit for annotated tags
// (invariant 10) — peeling needs the objects, hence the materialize.
func (s *server) handleListTags(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, err := coreLimitParam(q, defaultListLimit, maxListLimit)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	after := ""
	if cursor := q.Get("cursor"); cursor != "" {
		fields, err := coreCursorDecode(cursorKindTags, cursor, 1)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		after = fields[0]
	}
	repoPath, ok := s.materializeReadRepo(w, r, repo.RepoID)
	if !ok {
		return
	}
	tags, err := gitread.PeeledTags(r.Context(), repoPath)
	if err != nil {
		s.internalErr(w, r, "list tags", err)
		return
	}
	entries := []tagEntry{}
	hasMore := false
	for _, t := range tags {
		if after != "" && t.Name <= after {
			continue
		}
		if len(entries) == limit {
			hasMore = true
			break
		}
		entries = append(entries, tagEntry{
			Cursor: coreCursorEncode(cursorKindTags, t.Name),
			Name:   t.Name,
			SHA:    t.SHA,
		})
	}
	next := ""
	if hasMore {
		next = entries[len(entries)-1].Cursor
	}
	writeJSON(w, http.StatusOK, listTagsResponse{Tags: entries, NextCursor: next, HasMore: hasMore})
}

// ---------------------------------------------------------------------------
// 2a: GET /repos/commits + GET /repos/commit

// commitInfo is the docs/03 CommitInfo wire shape; `date` is the committer
// date (invariant 9 orders history by commit date).
type commitInfo struct {
	SHA            string   `json:"sha"`
	ParentSHAs     []string `json:"parent_shas"`
	Message        string   `json:"message"`
	AuthorName     string   `json:"author_name"`
	AuthorEmail    string   `json:"author_email"`
	CommitterName  string   `json:"committer_name"`
	CommitterEmail string   `json:"committer_email"`
	Date           string   `json:"date"`
}

func toCommitInfo(c gitread.Commit) commitInfo {
	assertf(c.ParentSHAs != nil, "gitread.Commit with nil ParentSHAs")
	return commitInfo{
		SHA:            c.SHA,
		ParentSHAs:     c.ParentSHAs,
		Message:        strings.TrimRight(c.Message, "\n"),
		AuthorName:     c.AuthorName,
		AuthorEmail:    c.AuthorEmail,
		CommitterName:  c.CommitterName,
		CommitterEmail: c.CommitterEmail,
		Date:           c.CommitterDate.UTC().Format(time.RFC3339),
	}
}

type listCommitsResponse struct {
	Commits    []commitInfo `json:"commits"`
	NextCursor string       `json:"next_cursor"`
	HasMore    bool         `json:"has_more"`
}

// handleListCommits implements GET /api/v1/repos/commits: newest-first from
// the resolved branch tip (default branch when omitted). Pagination is
// anchored: page one resolves the branch to an anchor sha, the cursor
// encodes (anchor, offset), and later pages walk `git log <anchor>` — so a
// concurrent push never shifts a walk in progress. Unknown branch and
// unborn default branch are 404s (invariant 3).
func (s *server) handleListCommits(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, err := coreLimitParam(q, defaultListLimit, maxListLimit)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ephemeral, err := coreBoolParam(q, "ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	repoPath, ok := s.materializeReadRepo(w, r, repo.RepoID)
	if !ok {
		return
	}

	anchor, offset := "", 0
	if cursor := q.Get("cursor"); cursor != "" {
		fields, err := coreCursorDecode(cursorKindCommits, cursor, 2)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if !ref.ValidSHA(fields[0]) {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		offset, err = strconv.Atoi(fields[1])
		if err != nil || offset < 0 {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		// The anchor came from an earlier response for THIS repo; a sha the
		// repo does not know marks a foreign/stale cursor, not a 404.
		anchor, err = gitread.ResolveRef(r.Context(), repoPath, fields[0], false)
		if errors.Is(err, gitread.ErrNotFound) {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		} else if err != nil {
			s.internalErr(w, r, "resolve cursor anchor", err)
			return
		}
	} else {
		branch := q.Get("branch")
		if branch == "" {
			branch = repo.DefaultBranch
		}
		anchor, err = gitread.ResolveRef(r.Context(), repoPath, branch, ephemeral)
		if errors.Is(err, gitread.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "branch not found")
			return
		} else if err != nil {
			s.internalErr(w, r, "resolve branch", err)
			return
		}
	}

	commits, err := gitread.Log(r.Context(), repoPath, anchor, gitread.LogOptions{
		Path:     q.Get("path"),
		Skip:     offset,
		MaxCount: limit + 1,
	})
	if err != nil {
		s.internalErr(w, r, "list commits", err)
		return
	}
	hasMore := len(commits) > limit
	if hasMore {
		commits = commits[:limit]
	}
	entries := make([]commitInfo, 0, len(commits))
	for _, c := range commits {
		entries = append(entries, toCommitInfo(c))
	}
	next := ""
	if hasMore {
		next = coreCursorEncode(cursorKindCommits, anchor, strconv.Itoa(offset+limit))
	}
	writeJSON(w, http.StatusOK, listCommitsResponse{Commits: entries, NextCursor: next, HasMore: hasMore})
}

type commitDetailEntry struct {
	commitInfo
	Signature string `json:"signature,omitempty"`
	Payload   string `json:"payload,omitempty"`
}

type getCommitResponse struct {
	Commit commitDetailEntry `json:"commit"`
}

// handleGetCommit implements GET /api/v1/repos/commit: `sha` accepts a
// branch name, tag, short sha, or full sha; signed commits additionally
// carry the armored signature and the exact signed payload bytes.
func (s *server) handleGetCommit(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	spec := r.URL.Query().Get("sha")
	if spec == "" {
		writeErr(w, http.StatusBadRequest, "missing sha")
		return
	}
	repoPath, ok := s.materializeReadRepo(w, r, repo.RepoID)
	if !ok {
		return
	}
	sha, err := gitread.ResolveRef(r.Context(), repoPath, spec, false)
	if errors.Is(err, gitread.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "commit not found")
		return
	} else if err != nil {
		s.internalErr(w, r, "resolve commit", err)
		return
	}
	detail, err := gitread.GetCommit(r.Context(), repoPath, sha)
	if err != nil {
		s.internalErr(w, r, "load commit", err)
		return
	}
	writeJSON(w, http.StatusOK, getCommitResponse{Commit: commitDetailEntry{
		commitInfo: toCommitInfo(detail.Commit),
		Signature:  detail.Signature,
		Payload:    detail.Payload,
	}})
}

// ---------------------------------------------------------------------------
// 2b: GET /repos/files + GET /repos/files/metadata

// files/files-metadata limits (openapi list_files/list_files_metadata).
const (
	defaultFilesLimit     = 1000
	maxFilesLimit         = 5000
	defaultFilesMetaLimit = 200
	maxFilesMetaLimit     = 1000
)

// resolveFilesRequest validates the params shared by the files listings and
// resolves ref -> commit. Returns ok=false with the response written.
func (s *server) resolveFilesRequest(w http.ResponseWriter, r *http.Request, repo meta.Repo) (repoPath, commit, refLabel string, ok bool) {
	q := r.URL.Query()
	ephemeral, err := coreBoolParam(q, "ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return "", "", "", false
	}
	refLabel = q.Get("ref")
	if refLabel == "" {
		refLabel = repo.DefaultBranch
	}
	repoPath, ok = s.materializeReadRepo(w, r, repo.RepoID)
	if !ok {
		return "", "", "", false
	}
	commit, err = gitread.ResolveRef(r.Context(), repoPath, refLabel, ephemeral)
	if errors.Is(err, gitread.ErrNotFound) {
		// Unknown ref and the unborn-branch case (fresh repo: the ref
		// authority knows no such branch yet) — invariant 3: 404.
		writeErr(w, http.StatusNotFound, "ref not found")
		return "", "", "", false
	} else if err != nil {
		s.internalErr(w, r, "resolve ref", err)
		return "", "", "", false
	}
	return repoPath, commit, refLabel, true
}

// listTreePage lists the (optionally recursive) tree at commit under the
// `path` query param and returns the lexically ordered entries. A non-empty
// path that names a blob/symlink/submodule yields that single entry; a path
// that matches nothing is a 404. Returns ok=false with the response written.
func (s *server) listTreePage(w http.ResponseWriter, r *http.Request, repoPath, commit string, recursive bool) ([]gitread.TreeEntry, bool) {
	subtree := strings.TrimSuffix(r.URL.Query().Get("path"), "/")
	listPath := ""
	if subtree != "" {
		listPath = subtree + "/"
	}
	entries, err := gitread.LsTree(r.Context(), repoPath, commit, listPath, recursive)
	if err != nil {
		s.internalErr(w, r, "list tree", err)
		return nil, false
	}
	if subtree != "" && len(entries) == 0 {
		// "dir/" matched nothing: either the path is a non-tree entry
		// (list it verbatim, git ls-tree semantics) or it does not exist.
		entries, err = gitread.LsTree(r.Context(), repoPath, commit, subtree, false)
		if err != nil {
			s.internalErr(w, r, "list tree", err)
			return nil, false
		}
		if len(entries) == 0 {
			writeErr(w, http.StatusNotFound, "path not found")
			return nil, false
		}
	}
	// Keyset pagination needs one total order. Git's tree order sorts
	// subtrees as "name/", which diverges from plain byte order on names
	// like "a-b" vs tree "a" — sort lexically by path so cursors resume
	// against the same order they were minted in.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, true
}

// pageTreeEntries applies the (cursorKind, cursor, limit) keyset window.
func pageTreeEntries(entries []gitread.TreeEntry, after string, limit int) (page []gitread.TreeEntry, hasMore bool) {
	assertf(limit > 0, "pageTreeEntries: limit %d", limit)
	page = []gitread.TreeEntry{}
	for _, e := range entries {
		if after != "" && e.Path <= after {
			continue
		}
		if len(page) == limit {
			return page, true
		}
		page = append(page, e)
	}
	return page, false
}

type fileEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
	Mode string `json:"mode"`
}

type listFilesResponse struct {
	Paths      []string    `json:"paths"`
	Ref        string      `json:"ref"`
	Entries    []fileEntry `json:"entries"`
	NextCursor string      `json:"next_cursor"`
	HasMore    bool        `json:"has_more"`
}

// handleListFiles implements GET /api/v1/repos/files: ls-tree at the
// resolved ref (recursive flag honored), keyset cursor = last path.
// `paths` mirrors the page's plain blob entries (openapi list_files).
func (s *server) handleListFiles(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, err := coreLimitParam(q, defaultFilesLimit, maxFilesLimit)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	recursive, err := coreBoolParam(q, "recursive")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	after := ""
	if cursor := q.Get("cursor"); cursor != "" {
		fields, err := coreCursorDecode(cursorKindFiles, cursor, 1)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		after = fields[0]
	}
	repoPath, commit, refLabel, ok := s.resolveFilesRequest(w, r, repo)
	if !ok {
		return
	}
	entries, ok := s.listTreePage(w, r, repoPath, commit, recursive)
	if !ok {
		return
	}
	page, hasMore := pageTreeEntries(entries, after, limit)

	respEntries := make([]fileEntry, 0, len(page))
	paths := []string{}
	for _, e := range page {
		respEntries = append(respEntries, fileEntry{Path: e.Path, Type: e.Type, Mode: e.Mode})
		if e.Type == gitread.EntryBlob {
			paths = append(paths, e.Path)
		}
	}
	next := ""
	if hasMore {
		next = coreCursorEncode(cursorKindFiles, page[len(page)-1].Path)
	}
	writeJSON(w, http.StatusOK, listFilesResponse{
		Paths:      paths,
		Ref:        refLabel,
		Entries:    respEntries,
		NextCursor: next,
		HasMore:    hasMore,
	})
}

type fileMetaEntry struct {
	Path          string `json:"path"`
	Mode          string `json:"mode"`
	Size          int64  `json:"size"`
	LastCommitSHA string `json:"last_commit_sha"`
	Type          string `json:"type"`
}

type fileMetaCommit struct {
	Author  string `json:"author"`
	Date    string `json:"date"`
	Message string `json:"message"`
}

type listFilesMetadataResponse struct {
	Files      []fileMetaEntry           `json:"files"`
	Commits    map[string]fileMetaCommit `json:"commits"`
	Ref        string                    `json:"ref"`
	NextCursor string                    `json:"next_cursor"`
	HasMore    bool                      `json:"has_more"`
}

// handleFilesMetadata implements GET /api/v1/repos/files/metadata: always
// recursive (the `recursive` flag is accepted for symmetry, openapi), each
// page entry annotated with the last commit touching it, commit details
// deduplicated into the `commits` map. Unborn branch -> 404 (invariant 3).
// The per-path `git log -1` is O(page) subprocesses — accepted for p3.
func (s *server) handleFilesMetadata(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, err := coreLimitParam(q, defaultFilesMetaLimit, maxFilesMetaLimit)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// Accepted for API symmetry; metadata listings are recursive regardless.
	if _, err := coreBoolParam(q, "recursive"); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	after := ""
	if cursor := q.Get("cursor"); cursor != "" {
		fields, err := coreCursorDecode(cursorKindFileMeta, cursor, 1)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		after = fields[0]
	}
	repoPath, commit, refLabel, ok := s.resolveFilesRequest(w, r, repo)
	if !ok {
		return
	}
	entries, ok := s.listTreePage(w, r, repoPath, commit, true)
	if !ok {
		return
	}
	page, hasMore := pageTreeEntries(entries, after, limit)

	files := make([]fileMetaEntry, 0, len(page))
	commitsMap := map[string]fileMetaCommit{}
	for _, e := range page {
		last, err := gitread.Log(r.Context(), repoPath, commit, gitread.LogOptions{Path: e.Path, MaxCount: 1})
		if err != nil {
			s.internalErr(w, r, "last commit for path", err)
			return
		}
		if len(last) == 0 {
			// ls-tree listed the path at this commit, so some reachable
			// commit must have introduced it.
			s.internalErr(w, r, "last commit for path", fmt.Errorf("no commit touches %q at %s", e.Path, commit))
			return
		}
		size := e.Size
		if size < 0 {
			size = 0 // submodules: ls-tree reports no blob size
		}
		files = append(files, fileMetaEntry{
			Path:          e.Path,
			Mode:          e.Mode,
			Size:          size,
			LastCommitSHA: last[0].SHA,
			Type:          e.Type,
		})
		commitsMap[last[0].SHA] = fileMetaCommit{
			Author:  last[0].AuthorName,
			Date:    last[0].CommitterDate.UTC().Format(time.RFC3339),
			Message: strings.TrimRight(last[0].Message, "\n"),
		}
	}
	next := ""
	if hasMore {
		next = coreCursorEncode(cursorKindFileMeta, page[len(page)-1].Path)
	}
	writeJSON(w, http.StatusOK, listFilesMetadataResponse{
		Files:      files,
		Commits:    commitsMap,
		Ref:        refLabel,
		NextCursor: next,
		HasMore:    hasMore,
	})
}

// ---------------------------------------------------------------------------
// 2b: GET+HEAD /repos/file

// handleGetFile implements GET+HEAD /api/v1/repos/file: conditional and
// range blob streaming. The blob is staged to a request-scoped temp file
// and served via http.ServeContent, which owns the RFC 9110 precondition
// and range matrix (200/206/304/412/416, If-Range, HEAD body suppression)
// against the ETag (quoted blob sha, strong) and Last-Modified (last commit
// touching the path) we set first.
func (s *server) handleGetFile(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	filePath := q.Get("path")
	if filePath == "" {
		writeErr(w, http.StatusBadRequest, "missing path")
		return
	}
	// docs/03 lists ephemeral_base on this endpoint; with a single ref to
	// resolve it has no distinct meaning here, so it is validated and
	// otherwise ignored (SDKs send it for API symmetry).
	if _, err := coreBoolParam(q, "ephemeral_base"); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	repoPath, commit, _, ok := s.resolveFilesRequest(w, r, repo)
	if !ok {
		return
	}
	info, err := gitread.StatBlob(r.Context(), repoPath, commit, strings.TrimSuffix(filePath, "/"))
	if errors.Is(err, gitread.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "file not found")
		return
	} else if err != nil {
		s.internalErr(w, r, "stat blob", err)
		return
	}

	tmp, err := os.CreateTemp("", "codestorage-file-*")
	if err != nil {
		s.internalErr(w, r, "stage blob", err)
		return
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if err := gitread.CatBlobTo(r.Context(), repoPath, info.SHA, tmp); err != nil {
		s.internalErr(w, r, "stream blob", err)
		return
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		s.internalErr(w, r, "rewind blob", err)
		return
	}
	// Defensive: the bytes we serve must be exactly the blob ls-tree sized.
	fi, err := tmp.Stat()
	if err != nil {
		s.internalErr(w, r, "stage blob", err)
		return
	}
	if fi.Size() != info.Size {
		s.internalErr(w, r, "stage blob", fmt.Errorf("staged %d bytes, want %d", fi.Size(), info.Size))
		return
	}

	h := w.Header()
	h.Set("X-Blob-Sha", info.SHA)
	h.Set("X-Last-Commit-Sha", info.LastCommit.SHA)
	h.Set("ETag", `"`+info.SHA+`"`)
	// ServeContent fills Last-Modified/Accept-Ranges/Content-* and evaluates
	// the conditional headers; the path base gives it the extension for
	// Content-Type detection (content sniffing is the fallback).
	http.ServeContent(w, r, path.Base(filePath), info.LastCommit.CommitterDate, tmp)
}
