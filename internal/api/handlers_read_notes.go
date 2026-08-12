// Notes read endpoints (docs/03 "Notes"): GET /repos/notes returns the note
// attached to one object, GET /repos/notes/refs enumerates notes refs from
// the ref authority. Custom notes refs are always enabled on this server
// (the write surface accepts them), so the upstream "custom notes refs
// disabled" 400 has no equivalent here. Helpers carry an "rnotes" prefix
// (package convention: sibling endpoint files must not collide).
package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"codestorage/internal/gitread"
	"codestorage/internal/ref"
)

// ---------------------------------------------------------------------------
// GET /repos/notes

type rnotesGetResponse struct {
	SHA    string `json:"sha"`
	Note   string `json:"note"`
	RefSHA string `json:"ref_sha"`
}

// handleGetNote implements GET /api/v1/repos/notes: `sha` (required, full
// object id — same validation as the notes write surface) plus optional
// `ref` with the write path's resolution (default refs/notes/commits, bare
// names under refs/notes/). ref_sha is the notes ref tip from the ref
// authority; an unborn notes ref or a sha without a note is a 404.
func (s *server) handleGetNote(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	sha := q.Get("sha")
	if !ref.ValidSHA(sha) {
		writeErr(w, http.StatusBadRequest, "sha must be a full 40-hex object id")
		return
	}
	targetRef, err := wrefNotesRefName(q.Get("ref"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	refs, err := s.refStore.ListRefs(r.Context(), repo.RepoID)
	if err != nil {
		s.internalErr(w, r, "list refs", err)
		return
	}
	refSHA, exists := refs[targetRef]
	if !exists {
		writeErr(w, http.StatusNotFound, "no note found for "+sha)
		return
	}
	repoPath, ok := s.materializeReadRepo(w, r, repo.RepoID)
	if !ok {
		return
	}
	note, err := gitread.Note(r.Context(), repoPath, targetRef, sha)
	switch {
	case errors.Is(err, gitread.ErrNotFound):
		writeErr(w, http.StatusNotFound, "no note found for "+sha)
		return
	case err != nil:
		s.internalErr(w, r, "read note", err)
		return
	}
	// git notes add/append run stripspace, so stored notes end in exactly
	// one newline; trim it to echo back what was posted (docs/03 shape).
	writeJSON(w, http.StatusOK, rnotesGetResponse{SHA: sha, Note: strings.TrimSuffix(note, "\n"), RefSHA: refSHA})
}

// ---------------------------------------------------------------------------
// GET /repos/notes/refs

// cursorKindNotesRefs namespaces this endpoint's cursors (see the cursor
// kind registry in handlers_read_core.go).
const cursorKindNotesRefs = "notesrefs"

type rnotesRefEntry struct {
	Cursor string `json:"cursor"`
	Ref    string `json:"ref"`
	SHA    string `json:"sha"`
}

type rnotesListRefsResponse struct {
	Refs       []rnotesRefEntry `json:"refs"`
	Prefix     string           `json:"prefix"`
	NextCursor string           `json:"next_cursor"`
	HasMore    bool             `json:"has_more"`
}

// rnotesPrefix normalizes the `prefix` query param (docs/03): empty selects
// refs/notes/, a bare prefix goes under refs/notes/, and a fully qualified
// prefix must already live there.
func rnotesPrefix(raw string) (string, error) {
	switch {
	case raw == "":
		return "refs/notes/", nil
	case strings.HasPrefix(raw, "refs/"):
		if !strings.HasPrefix(raw, "refs/notes/") {
			return "", fmt.Errorf("prefix %q is not under refs/notes/", raw)
		}
		return raw, nil
	default:
		return "refs/notes/" + raw, nil
	}
}

// handleListNotesRefs implements GET /api/v1/repos/notes/refs: notes refs
// come straight from the ref authority in lexical order, keyset-paginated
// by full ref name (same shape as GET /repos/branches).
func (s *server) handleListNotesRefs(w http.ResponseWriter, r *http.Request) {
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
	prefix, err := rnotesPrefix(q.Get("prefix"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	after := ""
	if cursor := q.Get("cursor"); cursor != "" {
		fields, err := coreCursorDecode(cursorKindNotesRefs, cursor, 1)
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
	// ListRefsWithMeta is sorted by full ref name, so the filtered notes
	// refs stay in lexical order.
	entries := []rnotesRefEntry{}
	hasMore := false
	for _, ri := range refs {
		if !strings.HasPrefix(ri.Name, prefix) {
			continue
		}
		if after != "" && ri.Name <= after {
			continue
		}
		if len(entries) == limit {
			hasMore = true
			break
		}
		entries = append(entries, rnotesRefEntry{
			Cursor: coreCursorEncode(cursorKindNotesRefs, ri.Name),
			Ref:    ri.Name,
			SHA:    ri.SHA,
		})
	}
	next := ""
	if hasMore {
		next = entries[len(entries)-1].Cursor
	}
	writeJSON(w, http.StatusOK, rnotesListRefsResponse{Refs: entries, Prefix: prefix, NextCursor: next, HasMore: hasMore})
}
