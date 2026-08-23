// Branch, tag, and notes write endpoints (docs/archive/phases/07-implementation-roadmap.md p4 W3, docs/reference/http-api.md):
// POST /repos/branches/create, DELETE /repos/branches, POST/DELETE
// /repos/tags, POST/DELETE /repos/notes — with default-branch delete
// protection (docs/explanation/domain-model.md invariant 4, ephemeral exemption per invariant 7),
// lightweight-tag peeling (invariant 10), notes add/append CAS via
// expected_notes_ref_sha (invariant 1), and ref-policy enforcement on
// every write (invariant 5). Aliased request-body fields (issue #17)
// accept both the current upstream spelling and the deprecated one, the
// current name winning on conflict.
//
// Routes are registered by api.New via registerWriteRefRoutes; the tests in
// write_refs_test.go build their own mux through the same helper.
//
// Ephemeral namespace: consistent with the frozen read surface
// (handleListBranches, gitread.ResolveRef), ephemeral refs are stored under
// the base RepoID storage key with the auth.EphemeralRefName prefix
// (refs/namespaces/ephemeral/refs/heads/...). Ref policies match the stored
// ref name verbatim, which reproduces the hook's ephemeral-namespace quirk.
//
// All helpers carry a "wref" prefix to stay collision-free with sibling
// write-endpoint slices in this package (same convention as the core*
// read helpers).
package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/githttp"
	"github.com/coder/code-storage/internal/gitread"
	"github.com/coder/code-storage/internal/ref"
)

// registerWriteRefRoutes registers the W3 write endpoints on mux with the
// same per-route auth middleware api.New uses. DELETE /repos/tags demands
// BOTH git:read and git:write (docs/reference/http-api.md, docs/archive/phases/12-phase3-implementation-notes.md).
func (s *server) registerWriteRefRoutes(mux *http.ServeMux, verifier auth.Verifier, org string, rl *rateLimiter) {
	assertf(mux != nil, "registerWriteRefRoutes: nil mux")
	assertf(verifier != nil, "registerWriteRefRoutes: nil verifier")
	route := func(pattern string, h http.HandlerFunc, scopes ...string) {
		mux.Handle(pattern, rl.limit(routeWrite)(requireAuthWrite(verifier, org, false, scopes...)(h)))
	}
	route("POST /api/v1/repos/branches/create", s.handleCreateBranch, auth.ScopeGitWrite)
	route("DELETE /api/v1/repos/branches", s.handleDeleteBranch, auth.ScopeGitWrite)
	route("POST /api/v1/repos/tags", s.handleCreateTag, auth.ScopeGitWrite)
	route("DELETE /api/v1/repos/tags", s.handleDeleteTag, auth.ScopeGitRead, auth.ScopeGitWrite)
	route("POST /api/v1/repos/notes", s.handleWriteNote, auth.ScopeGitWrite)
	route("DELETE /api/v1/repos/notes", s.handleDeleteNote, auth.ScopeGitWrite)
}

// ---------------------------------------------------------------------------
// Shared write plumbing

// wrefDecodeBody reads and unmarshals a JSON request body into dst. Writes
// a 400 {"error"} envelope and returns false on failure. An empty body is
// rejected: every W3 endpoint has required fields.
func wrefDecodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return false
	}
	if len(body) == 0 {
		writeErr(w, http.StatusBadRequest, "missing request body")
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// wrefAliasField resolves a request-body alias pair: the current upstream
// field wins when both are set (the live spec defines no precedence, so
// this mirrors the coreAliasParam query convention from issue #15).
func wrefAliasField(current, deprecated string) string {
	if current != "" {
		return current
	}
	return deprecated
}

// wrefRefName maps a bare branch name to its stored ref name: the plain
// refs/heads/ name, or the ephemeral-namespace name under the same storage
// key (see package comment).
func wrefRefName(branch string, ephemeral bool) string {
	assertf(branch != "" && !strings.HasPrefix(branch, "refs/"), "wrefRefName: unvalidated branch %q", branch)
	if ephemeral {
		return auth.EphemeralRefName(branch)
	}
	return "refs/heads/" + branch
}

// wrefValidBranchInput validates a request branch/tag name (docs/reference/http-api.md: bare
// name, never refs/-prefixed) with the same conservative rules as the
// default-branch validator.
func wrefValidBranchInput(name string) bool {
	return !strings.HasPrefix(name, "refs/") && validBranchName(name)
}

// wrefPolicyCheck evaluates the token's ref policies (docs/reference/authentication.md, invariant 5)
// for one CAS update against the STORED ref name, mirroring the git update
// hook: no-push denies everything; no-force-push denies deletions and
// non-fast-forward updates of existing refs; verify-sig lets deletions pass
// (they introduce no commits) and fails closed for creations/updates —
// this API surface can neither produce signed commits nor verify third-party
// signatures, so allowing the write would bypass the policy. Unknown ops
// fail closed. Malformed policy claims fail closed too.
//
// isAncestor is only consulted for no-force-push on existing refs; callers
// whose old SHA is zero may pass nil.
//
// Returns deny != "" when a policy denies the update (403), err != nil for
// internal failures (500).
func wrefPolicyCheck(claims *auth.Claims, refName, old, new string, isAncestor func(old, new string) (bool, error)) (deny string, err error) {
	assertf(refName != "", "wrefPolicyCheck: empty ref name")
	assertf(ref.ValidSHA(old) && ref.ValidSHA(new), "wrefPolicyCheck: malformed shas %q %q", old, new)
	policies, perr := refPolicies(claims)
	if perr != nil {
		return perr.Error(), nil
	}
	rule, ok := auth.MatchPolicy(policies, refName)
	if !ok {
		return "", nil // unpoliced ref: allowed
	}
	for _, op := range rule.Ops {
		switch op {
		case auth.OpNoPush:
			return fmt.Sprintf("policy no-push denies update to %s", refName), nil
		case auth.OpNoForcePush:
			if old == ref.ZeroSHA {
				continue // creation is always fast-forward
			}
			if new == ref.ZeroSHA {
				return fmt.Sprintf("policy no-force-push denies deletion of %s", refName), nil
			}
			assertf(isAncestor != nil, "wrefPolicyCheck: nil isAncestor for existing-ref update")
			ff, err := isAncestor(old, new)
			if err != nil {
				return "", err
			}
			if !ff {
				return fmt.Sprintf("policy no-force-push denies non-fast-forward update to %s", refName), nil
			}
		case auth.OpVerifySig:
			if new == ref.ZeroSHA {
				continue // deletion introduces no commits
			}
			return fmt.Sprintf("policy verify-sig cannot be satisfied by this endpoint for %s", refName), nil
		default:
			return fmt.Sprintf("unknown policy op %q for %s", op, refName), nil
		}
	}
	return "", nil
}

// wrefGit runs git -C repoPath with a hermetic config environment (host
// global/system config and GIT_* identity vars must never leak into
// server-created objects) plus extraEnv, feeding stdin when non-nil.
// Returns stdout; stderr is captured into the error.
func wrefGit(ctx context.Context, repoPath string, extraEnv []string, stdin io.Reader, args ...string) (string, error) {
	assertf(repoPath != "", "wrefGit: empty repoPath")
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = repoPath
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "GIT_") {
			continue
		}
		cmd.Env = append(cmd.Env, kv)
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stdin = stdin
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// wrefListRefs fetches the ref-authority view of the repo (storage key =
// RepoID; ephemeral refs live under the namespace prefix in the same key).
func (s *server) wrefListRefs(w http.ResponseWriter, r *http.Request, repoID string) (map[string]string, bool) {
	refs, err := s.refStore.ListRefs(r.Context(), repoID)
	if err != nil {
		s.internalErr(w, r, "list refs", err)
		return nil, false
	}
	return refs, true
}

// ---------------------------------------------------------------------------
// POST /api/v1/repos/branches/create (git:write)

type wrefCreateBranchRequest struct {
	TargetBranch      string `json:"target_branch"`
	BaseRef           string `json:"base_ref"`
	BaseBranch        string `json:"base_branch"` // deprecated; base_ref wins
	BaseIsEphemeral   bool   `json:"base_is_ephemeral"`
	TargetIsEphemeral bool   `json:"target_is_ephemeral"`
	// Force CAS-recreates an existing target branch. Non-fast-forward moves
	// stay subject to the no-force-push ref-policy op (real ancestry check).
	Force bool `json:"force"`
	// PreferredBase is the fallback base: used when no base_ref/base_branch
	// is given, or when the primary base does not resolve.
	PreferredBase string `json:"preferred_base"`
}

type wrefCreateBranchResponse struct {
	Message           string `json:"message"`
	TargetBranch      string `json:"target_branch"`
	TargetIsEphemeral bool   `json:"target_is_ephemeral"`
	CommitSHA         string `json:"commit_sha"`
}

// handleCreateBranch resolves the base to a commit (branch, tag — peeled —
// or hex sha via gitread.ResolveRef; preferred_base is the fallback when
// the primary base is absent or unresolvable), policy-checks the new ref,
// and CAS creates it. With force, an existing target is CAS-recreated at
// the base — no-force-push policies still deny non-fast-forward moves via a
// real ancestry check. No new objects are created, so there is nothing to
// publish.
func (s *server) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	var req wrefCreateBranchRequest
	if !wrefDecodeBody(w, r, &req) {
		return
	}
	if !wrefValidBranchInput(req.TargetBranch) {
		writeErr(w, http.StatusBadRequest, "invalid target_branch")
		return
	}
	base := req.BaseRef
	if base == "" {
		base = req.BaseBranch
	}
	if base == "" && req.PreferredBase == "" {
		writeErr(w, http.StatusBadRequest, "base_ref, base_branch, or preferred_base is required")
		return
	}
	repoPath, ok := s.materializeReadRepo(w, r, repo)
	if !ok {
		return
	}
	ctx := r.Context()
	baseSHA := ""
	if base != "" {
		sha, err := gitread.ResolveRef(ctx, repoPath, base, req.BaseIsEphemeral)
		switch {
		case errors.Is(err, gitread.ErrNotFound):
			// Fall through to preferred_base below (when given).
		case err != nil:
			s.internalErr(w, r, "resolve base ref", err)
			return
		default:
			baseSHA = sha
		}
	}
	if baseSHA == "" && req.PreferredBase != "" {
		sha, err := gitread.ResolveRef(ctx, repoPath, req.PreferredBase, req.BaseIsEphemeral)
		switch {
		case errors.Is(err, gitread.ErrNotFound):
			// Report below with the primary-base message.
		case err != nil:
			s.internalErr(w, r, "resolve preferred base ref", err)
			return
		default:
			baseSHA = sha
		}
	}
	if baseSHA == "" {
		writeErr(w, http.StatusNotFound, "base ref not found")
		return
	}
	targetRef := wrefRefName(req.TargetBranch, req.TargetIsEphemeral)
	// The CAS precondition: zero for plain create, the current tip when
	// force re-creates an existing branch.
	oldSHA := ref.ZeroSHA
	if req.Force {
		refs, ok := s.wrefListRefs(w, r, repo.RepoID)
		if !ok {
			return
		}
		if cur, exists := refs[targetRef]; exists {
			oldSHA = cur
		}
	}
	deny, err := wrefPolicyCheck(mustClaims(r), targetRef, oldSHA, baseSHA, func(a, b string) (bool, error) {
		return wrefIsAncestor(ctx, repoPath, nil, a, b)
	})
	if err != nil {
		s.internalErr(w, r, "evaluate ref policy", err)
		return
	}
	if deny != "" {
		writeErr(w, http.StatusForbidden, deny)
		return
	}
	won, err := s.refStore.CASRef(ctx, repo.RepoID, targetRef, oldSHA, baseSHA)
	if err != nil {
		s.internalErr(w, r, "create branch ref", err)
		return
	}
	if !won {
		if req.Force {
			writeErr(w, http.StatusConflict, "branch moved concurrently")
			return
		}
		writeErr(w, http.StatusConflict, "branch already exists")
		return
	}
	message := "branch created"
	if oldSHA != ref.ZeroSHA {
		message = "branch updated"
	}
	writeJSON(w, http.StatusOK, wrefCreateBranchResponse{
		Message:           message,
		TargetBranch:      req.TargetBranch,
		TargetIsEphemeral: req.TargetIsEphemeral,
		CommitSHA:         baseSHA,
	})
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/repos/branches (git:write)

type wrefDeleteBranchRequest struct {
	TargetBranch string `json:"target_branch"`
	Name         string `json:"name"` // deprecated alias of target_branch
	Ephemeral    bool   `json:"ephemeral"`
}

type wrefDeleteBranchResponse struct {
	TargetBranch string `json:"target_branch"`
	Name         string `json:"name"` // deprecated: mirrors target_branch
	Ephemeral    bool   `json:"ephemeral"`
	Message      string `json:"message"`
}

// handleDeleteBranch CAS-deletes a branch ref. The repo's default branch is
// protected in the persistent namespace (invariant 4) — even while unborn —
// and exempt in the ephemeral namespace (invariant 7).
func (s *server) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	var req wrefDeleteBranchRequest
	if !wrefDecodeBody(w, r, &req) {
		return
	}
	// openapi (issue #17): target_branch is the current field, name the
	// deprecated alias.
	name := wrefAliasField(req.TargetBranch, req.Name)
	if !wrefValidBranchInput(name) {
		writeErr(w, http.StatusBadRequest, "invalid branch name")
		return
	}
	assertf(repo.DefaultBranch != "", "repo %s has empty default branch", repo.RepoID)
	refName := wrefRefName(name, req.Ephemeral)
	// Compare STORED ref names: the default branch may be recorded in
	// refs/-qualified form (PATCH /repo stores such values verbatim), and
	// the short request name must still hit the protection.
	if !req.Ephemeral && refName == githttp.HeadTarget(repo.DefaultBranch) {
		writeErr(w, http.StatusForbidden, "refusing to delete the default branch")
		return
	}
	refs, ok := s.wrefListRefs(w, r, repo.RepoID)
	if !ok {
		return
	}
	cur, exists := refs[refName]
	if !exists {
		writeErr(w, http.StatusNotFound, "branch not found")
		return
	}
	deny, err := wrefPolicyCheck(mustClaims(r), refName, cur, ref.ZeroSHA, nil)
	if err != nil {
		s.internalErr(w, r, "evaluate ref policy", err)
		return
	}
	if deny != "" {
		writeErr(w, http.StatusForbidden, deny)
		return
	}
	won, err := s.refStore.CASRef(r.Context(), repo.RepoID, refName, cur, ref.ZeroSHA)
	if err != nil {
		s.internalErr(w, r, "delete branch ref", err)
		return
	}
	if !won {
		writeErr(w, http.StatusConflict, "branch moved concurrently")
		return
	}
	writeJSON(w, http.StatusOK, wrefDeleteBranchResponse{
		TargetBranch: name,
		Name:         name,
		Ephemeral:    req.Ephemeral,
		Message:      "branch deleted",
	})
}

// ---------------------------------------------------------------------------
// POST /api/v1/repos/tags (git:write)

type wrefCreateTagRequest struct {
	Name   string `json:"name"`
	Ref    string `json:"ref"`
	Target string `json:"target"` // deprecated alias of ref
}

type wrefCreateTagResponse struct {
	Name    string `json:"name"`
	SHA     string `json:"sha"`
	Message string `json:"message"`
}

// handleCreateTag creates a lightweight tag (invariant 10): target must
// resolve — and peel, for annotated-tag or branch targets — to a commit
// (gitread.ResolveRef applies ^{commit}); the stored ref value is that
// peeled commit sha.
func (s *server) handleCreateTag(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	var req wrefCreateTagRequest
	if !wrefDecodeBody(w, r, &req) {
		return
	}
	if !wrefValidBranchInput(req.Name) {
		writeErr(w, http.StatusBadRequest, "invalid tag name")
		return
	}
	// openapi (issue #17): ref is the current field, target the deprecated
	// alias; both have always accepted any revision (branch, tag, or
	// commit) — ResolveRef peels to a commit either way.
	target := wrefAliasField(req.Ref, req.Target)
	if target == "" {
		writeErr(w, http.StatusBadRequest, "ref is required")
		return
	}
	repoPath, ok := s.materializeReadRepo(w, r, repo)
	if !ok {
		return
	}
	sha, err := gitread.ResolveRef(r.Context(), repoPath, target, false)
	switch {
	case errors.Is(err, gitread.ErrNotFound):
		writeErr(w, http.StatusNotFound, "ref does not resolve to a commit")
		return
	case err != nil:
		s.internalErr(w, r, "resolve tag target", err)
		return
	}
	refName := "refs/tags/" + req.Name
	deny, err := wrefPolicyCheck(mustClaims(r), refName, ref.ZeroSHA, sha, nil)
	if err != nil {
		s.internalErr(w, r, "evaluate ref policy", err)
		return
	}
	if deny != "" {
		writeErr(w, http.StatusForbidden, deny)
		return
	}
	won, err := s.refStore.CASRef(r.Context(), repo.RepoID, refName, ref.ZeroSHA, sha)
	if err != nil {
		s.internalErr(w, r, "create tag ref", err)
		return
	}
	if !won {
		writeErr(w, http.StatusConflict, "tag already exists")
		return
	}
	writeJSON(w, http.StatusOK, wrefCreateTagResponse{Name: req.Name, SHA: sha, Message: "tag created"})
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/repos/tags (git:read + git:write, enforced by the route)

type wrefDeleteTagRequest struct {
	Name string `json:"name"`
}

type wrefDeleteTagResponse struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

// handleDeleteTag CAS-deletes a tag by its STORED ref value — for annotated
// tags that is the tag object sha, so both lightweight and annotated tags
// delete correctly without peeling (docs/reference/http-api.md).
func (s *server) handleDeleteTag(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	var req wrefDeleteTagRequest
	if !wrefDecodeBody(w, r, &req) {
		return
	}
	if !wrefValidBranchInput(req.Name) {
		writeErr(w, http.StatusBadRequest, "invalid tag name")
		return
	}
	refName := "refs/tags/" + req.Name
	refs, ok := s.wrefListRefs(w, r, repo.RepoID)
	if !ok {
		return
	}
	cur, exists := refs[refName]
	if !exists {
		writeErr(w, http.StatusNotFound, "tag not found")
		return
	}
	deny, err := wrefPolicyCheck(mustClaims(r), refName, cur, ref.ZeroSHA, nil)
	if err != nil {
		s.internalErr(w, r, "evaluate ref policy", err)
		return
	}
	if deny != "" {
		writeErr(w, http.StatusForbidden, deny)
		return
	}
	won, err := s.refStore.CASRef(r.Context(), repo.RepoID, refName, cur, ref.ZeroSHA)
	if err != nil {
		s.internalErr(w, r, "delete tag ref", err)
		return
	}
	if !won {
		writeErr(w, http.StatusConflict, "tag moved concurrently")
		return
	}
	writeJSON(w, http.StatusOK, wrefDeleteTagResponse{Name: req.Name, Message: "tag deleted"})
}

// ---------------------------------------------------------------------------
// POST/DELETE /api/v1/repos/notes (git:write)

type wrefNoteAuthor struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type wrefNoteRequest struct {
	ObjectRef           string          `json:"object_ref"`
	SHA                 string          `json:"sha"`    // deprecated alias of object_ref
	Action              string          `json:"action"` // add|append; POST only, default add
	Note                string          `json:"note"`
	NotesRef            string          `json:"notes_ref"`
	Ref                 string          `json:"ref"` // deprecated alias of notes_ref
	ExpectedNotesRefSHA string          `json:"expected_notes_ref_sha"`
	ExpectedRefSHA      string          `json:"expected_ref_sha"` // deprecated alias of expected_notes_ref_sha
	Author              *wrefNoteAuthor `json:"author"`
}

type wrefNoteResult struct {
	Success bool   `json:"success"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type wrefNoteResponse struct {
	SHA        string         `json:"sha"`
	NotesRef   string         `json:"notes_ref"`
	TargetRef  string         `json:"target_ref"` // deprecated: mirrors notes_ref
	BaseCommit string         `json:"base_commit,omitempty"`
	NewRefSHA  string         `json:"new_ref_sha"`
	Result     wrefNoteResult `json:"result"`
}

// wrefNoteFail writes the docs/reference/http-api.md note-write envelope with a failure result
// (SDKs parse this body on every 4xx/5xx and raise typed RefUpdateErrors).
// newRefSHA is the notes ref's current tip, or the zero sha when unborn.
func wrefNoteFail(w http.ResponseWriter, httpStatus int, msg, sha, targetRef, baseCommit, newRefSHA string) {
	assertf(httpStatus >= 400 && httpStatus < 600, "wrefNoteFail: non-error status %d", httpStatus)
	assertf(msg != "", "wrefNoteFail: empty message")
	writeJSON(w, httpStatus, wrefNoteResponse{
		SHA:        sha,
		NotesRef:   targetRef,
		TargetRef:  targetRef,
		BaseCommit: baseCommit,
		NewRefSHA:  newRefSHA,
		Result:     wrefNoteResult{Success: false, Status: statusForHTTP(httpStatus), Message: msg},
	})
}

// wrefNotesRefName resolves the request `notes_ref` field (deprecated
// alias `ref`) to a full notes ref: empty picks the default
// refs/notes/commits, a bare name goes under refs/notes/, and a fully
// qualified name must already live there (docs/reference/http-api.md).
func wrefNotesRefName(raw string) (string, error) {
	if raw == "" {
		return "refs/notes/commits", nil
	}
	if strings.HasPrefix(raw, "refs/") {
		name, found := strings.CutPrefix(raw, "refs/notes/")
		if !found || !validBranchName(name) {
			return "", fmt.Errorf("ref %q is not a valid refs/notes/ ref", raw)
		}
		return raw, nil
	}
	if !validBranchName(raw) {
		return "", fmt.Errorf("invalid notes ref name %q", raw)
	}
	return "refs/notes/" + raw, nil
}

func (s *server) handleWriteNote(w http.ResponseWriter, r *http.Request) {
	s.wrefHandleNote(w, r, false)
}

func (s *server) handleDeleteNote(w http.ResponseWriter, r *http.Request) {
	s.wrefHandleNote(w, r, true)
}

// wrefHandleNote implements the shared POST/DELETE notes flow: validate,
// CAS-guard on expected_notes_ref_sha (412), run the notes operation on a request-
// scoped temp ref with a scratch object directory, policy-check, publish the
// new objects (pack → upload → register), and CAS the notes ref (409 on a
// lost race). Domain failures answer with the note-write envelope.
func (s *server) wrefHandleNote(w http.ResponseWriter, r *http.Request, deleteNote bool) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	var req wrefNoteRequest
	if !wrefDecodeBody(w, r, &req) {
		return
	}
	// openapi (issue #17): object_ref/notes_ref/expected_notes_ref_sha are
	// the current fields; sha/ref/expected_ref_sha stay as deprecated
	// aliases, the current name winning on conflict.
	objectSHA := wrefAliasField(req.ObjectRef, req.SHA)
	notesRefRaw := wrefAliasField(req.NotesRef, req.Ref)
	expectedRefSHA := wrefAliasField(req.ExpectedNotesRefSHA, req.ExpectedRefSHA)
	targetRef, err := wrefNotesRefName(notesRefRaw)
	if err != nil {
		wrefNoteFail(w, http.StatusBadRequest, err.Error(), objectSHA, notesRefRaw, "", ref.ZeroSHA)
		return
	}
	if !ref.ValidSHA(objectSHA) {
		wrefNoteFail(w, http.StatusBadRequest, "object_ref must be a full 40-hex object id", objectSHA, targetRef, "", ref.ZeroSHA)
		return
	}
	action := "remove"
	if !deleteNote {
		// docs/reference/http-api.md + openapi: action defaults to add; note text is required.
		action = req.Action
		if action == "" {
			action = "add"
		}
		if action != "add" && action != "append" {
			wrefNoteFail(w, http.StatusBadRequest, "action must be add or append", objectSHA, targetRef, "", ref.ZeroSHA)
			return
		}
		if req.Note == "" {
			wrefNoteFail(w, http.StatusBadRequest, "note is required", objectSHA, targetRef, "", ref.ZeroSHA)
			return
		}
	}
	if expectedRefSHA != "" && !ref.ValidSHA(expectedRefSHA) {
		wrefNoteFail(w, http.StatusBadRequest, "expected_notes_ref_sha must be a full 40-hex object id", objectSHA, targetRef, "", ref.ZeroSHA)
		return
	}
	author := wrefNoteAuthor{Name: "codestorage", Email: "notes@codestorage.invalid"}
	if req.Author != nil {
		if req.Author.Name == "" || req.Author.Email == "" {
			wrefNoteFail(w, http.StatusBadRequest, "author name and email are required when author is provided", objectSHA, targetRef, "", ref.ZeroSHA)
			return
		}
		author = *req.Author
	}

	repoPath, ok := s.materializeReadRepo(w, r, repo)
	if !ok {
		return
	}
	refs, ok := s.wrefListRefs(w, r, repo.RepoID)
	if !ok {
		return
	}
	old := refs[targetRef] // "" when the notes ref is unborn
	oldOrZero := old
	if oldOrZero == "" {
		oldOrZero = ref.ZeroSHA
	}

	// Invariant 1: expected_notes_ref_sha is the notes-ref CAS guard; the
	// zero sha asserts the ref is unborn.
	if expectedRefSHA != "" && expectedRefSHA != oldOrZero {
		wrefNoteFail(w, http.StatusPreconditionFailed,
			fmt.Sprintf("expected_notes_ref_sha %.8s does not match %s tip %.8s", expectedRefSHA, targetRef, oldOrZero),
			objectSHA, targetRef, old, oldOrZero)
		return
	}

	ctx := r.Context()
	// The note target must exist (any object kind, per git notes).
	if _, err := wrefGit(ctx, repoPath, nil, nil,
		"rev-parse", "--verify", "--quiet", "--end-of-options", objectSHA+"^{object}"); err != nil {
		wrefNoteFail(w, http.StatusNotFound, "object not found: "+objectSHA, objectSHA, targetRef, old, oldOrZero)
		return
	}
	// Existence check drives add-vs-existing (409) and delete-missing (404).
	// The cache repo's notes ref was just synced from the ref authority.
	noteExists := false
	if old != "" {
		if _, err := wrefGit(ctx, repoPath, nil, nil, "notes", "--ref", targetRef, "show", objectSHA); err == nil {
			noteExists = true
		}
	}
	if action == "add" && noteExists {
		wrefNoteFail(w, http.StatusConflict, "note already exists for "+objectSHA, objectSHA, targetRef, old, oldOrZero)
		return
	}
	if deleteNote && !noteExists {
		wrefNoteFail(w, http.StatusNotFound, "no note found for "+objectSHA, objectSHA, targetRef, old, oldOrZero)
		return
	}

	// The scratch env stays alive (via the deferred cleanup) through the
	// ancestry check and publish below: the new notes objects are visible
	// only through it until the published pack is hydrated back.
	newSHA, scratchEnv, cleanup, err := s.wrefRunNotesOp(ctx, repoPath, old, action, objectSHA, req.Note, author)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		s.internalErr(w, r, "run notes operation", err)
		return
	}

	deny, err := wrefPolicyCheck(mustClaims(r), targetRef, oldOrZero, newSHA, func(a, b string) (bool, error) {
		return wrefIsAncestor(ctx, repoPath, scratchEnv, a, b)
	})
	if err != nil {
		s.internalErr(w, r, "evaluate ref policy", err)
		return
	}
	if deny != "" {
		wrefNoteFail(w, http.StatusForbidden, deny, objectSHA, targetRef, old, oldOrZero)
		return
	}

	if err := s.wrefPublish(ctx, repoPath, repo.RepoID, scratchEnv, oldOrZero, newSHA); err != nil {
		if errors.Is(err, errPackQuotaExceeded) {
			// Same 403 quota denial as commit-pack/merge/pre-receive
			// (issue #21), in this route's note envelope.
			wrefNoteFail(w, http.StatusForbidden, err.Error(), objectSHA, targetRef, old, oldOrZero)
			return
		}
		s.internalErr(w, r, "publish notes objects", err)
		return
	}
	won, err := s.refStore.CASRef(ctx, repo.RepoID, targetRef, oldOrZero, newSHA)
	if err != nil {
		s.internalErr(w, r, "update notes ref", err)
		return
	}
	if !won {
		// Lost CAS race: the published pack stays registered but
		// unreferenced (immutable, same as the push-hook pipeline).
		wrefNoteFail(w, http.StatusConflict, targetRef+" moved concurrently", objectSHA, targetRef, old, oldOrZero)
		return
	}
	writeJSON(w, http.StatusOK, wrefNoteResponse{
		SHA:        objectSHA,
		NotesRef:   targetRef,
		TargetRef:  targetRef,
		BaseCommit: old,
		NewRefSHA:  newSHA,
		Result:     wrefNoteResult{Success: true, Status: "ok"},
	})
}

// wrefScratchEnv builds the request-scoped object environment: new objects
// land in the scratch directory while the cache repo's objects stay
// readable (and untouched) via the alternates mechanism.
func wrefScratchEnv(repoPath, scratch string) []string {
	assertf(repoPath != "" && scratch != "", "wrefScratchEnv: empty paths")
	return []string{
		"GIT_OBJECT_DIRECTORY=" + scratch,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES=" + filepath.Join(repoPath, "objects"),
	}
}

// wrefRunNotesOp performs one git-notes mutation (add/append/remove) on a
// request-scoped temp ref seeded from the current notes tip — preserving
// notes for other objects — with all new objects written to a scratch
// object directory. Returns the resulting notes commit sha, the scratch
// object env (callers need it to see the new objects until publish), and a
// cleanup func (non-nil once resources exist) removing the scratch dir and
// temp ref.
func (s *server) wrefRunNotesOp(ctx context.Context, repoPath, old, action, sha, note string, author wrefNoteAuthor) (string, []string, func(), error) {
	assertf(action == "add" || action == "append" || action == "remove", "wrefRunNotesOp: bad action %q", action)
	// sha was validated with ref.ValidSHA upstream: it can never be parsed
	// as a git option or rev expression below.
	assertf(ref.ValidSHA(sha), "wrefRunNotesOp: unvalidated sha %q", sha)
	scratch, err := os.MkdirTemp("", "codestorage-notes-*")
	if err != nil {
		return "", nil, nil, err
	}
	cleanup := func() { _ = os.RemoveAll(scratch) }
	scratchEnv := wrefScratchEnv(repoPath, scratch)
	env := append(append([]string{}, scratchEnv...),
		"GIT_AUTHOR_NAME="+author.Name,
		"GIT_AUTHOR_EMAIL="+author.Email,
		"GIT_COMMITTER_NAME="+author.Name,
		"GIT_COMMITTER_EMAIL="+author.Email,
	)

	// git notes only mutates refs under refs/notes/, so the temp ref must
	// live there; the random suffix keeps concurrent requests apart.
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return "", nil, cleanup, err
	}
	tmpRef := "refs/notes/wref-tmp-" + hex.EncodeToString(suffix)
	if old != "" {
		if _, err := wrefGit(ctx, repoPath, env, nil, "update-ref", tmpRef, old); err != nil {
			return "", nil, cleanup, err
		}
	}
	fullCleanup := func() {
		_, _ = wrefGit(context.Background(), repoPath, nil, nil, "update-ref", "-d", tmpRef)
		cleanup()
	}

	var stdin io.Reader
	args := []string{"notes", "--ref", tmpRef, action}
	if action != "remove" {
		stdin = strings.NewReader(note)
		args = append(args, "-F", "-")
	}
	args = append(args, sha)
	if _, err := wrefGit(ctx, repoPath, env, stdin, args...); err != nil {
		return "", nil, fullCleanup, err
	}
	out, err := wrefGit(ctx, repoPath, env, nil, "rev-parse", "--verify", tmpRef)
	if err != nil {
		return "", nil, fullCleanup, err
	}
	newSHA := strings.TrimSpace(out)
	if !ref.ValidSHA(newSHA) {
		return "", nil, fullCleanup, fmt.Errorf("rev-parse %s emitted %q", tmpRef, newSHA)
	}
	assertf(newSHA != old, "notes op %s produced no new commit", action)
	return newSHA, scratchEnv, fullCleanup, nil
}

// wrefIsAncestor reports whether a is an ancestor of b (the fast-forward
// test for no-force-push). scratchEnv makes request-created objects (b)
// visible; merge-base exit code 1 means "no", anything else is an error.
func wrefIsAncestor(ctx context.Context, repoPath string, scratchEnv []string, a, b string) (bool, error) {
	assertf(ref.ValidSHA(a) && ref.ValidSHA(b), "wrefIsAncestor: malformed shas %q %q", a, b)
	_, err := wrefGit(ctx, repoPath, scratchEnv, nil, "merge-base", "--is-ancestor", a, b)
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

// wrefPublish packs the objects reachable from newSHA but not from oldSHA
// (the scratch-created notes objects), uploads the pack, and registers it —
// strictly BEFORE the caller's CASRef, exactly like the push-hook pipeline,
// so a lost race leaves only an unreferenced immutable pack.
func (s *server) wrefPublish(ctx context.Context, repoPath, repoID string, scratchEnv []string, oldSHA, newSHA string) error {
	assertf(ref.ValidSHA(oldSHA) && ref.ValidSHA(newSHA), "wrefPublish: malformed shas")
	assertf(newSHA != ref.ZeroSHA, "wrefPublish: zero new sha")
	assertf(len(scratchEnv) > 0, "wrefPublish: missing scratch env")
	packDir, err := os.MkdirTemp("", "codestorage-notes-pack-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(packDir)

	revs := newSHA + "\n"
	if oldSHA != ref.ZeroSHA {
		revs += "^" + oldSHA + "\n"
	}
	// The new notes objects live in the request's scratch odb (scratchEnv);
	// the caller keeps that directory alive until this publish completes.
	out, err := wrefGit(ctx, repoPath, scratchEnv, strings.NewReader(revs),
		"pack-objects", "--revs", "-q", filepath.Join(packDir, "pack"))
	if err != nil {
		return err
	}
	id := strings.TrimSpace(out)
	if id == "" {
		return errors.New("pack-objects emitted no pack id")
	}
	packPath := filepath.Join(packDir, "pack-"+id+".pack")
	if fi, err := os.Stat(packPath); err != nil || fi.Size() == 0 {
		return fmt.Errorf("pack-objects produced no pack at %s (err %v)", packPath, err)
	}
	// Dedup + quota + upload + register + verify + repack trigger via the
	// shared REST publication step (publish.go); quota denials store
	// nothing and surface as errPackQuotaExceeded.
	return s.publishPack(ctx, repoID, id, packPath)
}
