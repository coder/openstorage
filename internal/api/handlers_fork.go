// Synchronous repo forking (docs/explanation/domain-model.md invariant 12, docs/reference/http-api.md POST /repos,
// phase-5 workstream E):
//
//	POST /api/v1/repos  repo:write  base_repo {provider: "code"|"code.storage",
//	                                operation: "fork", auth: {token}} -> one-shot fork
//	POST /api/v1/repos  repo:write  base_repo {name, ref?, auth: {token}}
//	                                (no provider/operation: upstream's copy form) -> the same fork
//
// A fork is a deterministic, independent clone performed inside the request
// (ADR-31): the fork-point closure is packed from the source's materialized
// cache, published under the NEW RepoID's storage keys (ADR-21: RepoID is
// the only storage key), and the fork's default branch is CAS-created. No
// repo_upstreams row is written — a fork is one-shot and fully independent
// of its source.
//
// api.New (api.go) wires this dispatcher on POST /api/v1/repos with the
// base_repo-sync-aware creator from handlers_sync.go
// ((*syncServer).handleCreateRepo) as the fallback; fork_test.go builds the
// same stack with the frozen (*server).handleCreateRepo as the fallback.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// forkAwareCreateRepo dispatches POST /api/v1/repos: bodies carrying a
// base_repo fork spec for the code/code.storage provider are handled by
// handleForkRepo; everything else (no base_repo, sync operations, non-code
// fork providers, unparseable bodies) replays the body verbatim to
// fallback, which stays the single source of truth for its own validation
// and error mapping.
func (s *server) forkAwareCreateRepo(verifier auth.Verifier, fallback http.HandlerFunc) http.HandlerFunc {
	assertf(s != nil && s.store != nil && s.refStore != nil && s.blobStore != nil && s.logger != nil,
		"forkAwareCreateRepo: nil server deps")
	assertf(verifier != nil, "forkAwareCreateRepo: nil verifier")
	assertf(fallback != nil, "forkAwareCreateRepo: nil fallback")
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			// The body is consumed and cannot be replayed; answer with the
			// same envelope every sibling creator uses.
			writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
			return
		}
		replay := func() {
			r.Body = io.NopCloser(bytes.NewReader(body))
			fallback(w, r)
		}
		// Probe only — full validation belongs to the chosen handler.
		var req createRepoRequest
		if len(body) == 0 || json.Unmarshal(body, &req) != nil {
			replay()
			return
		}
		// A body with `upstream` always goes to the fallback, which owns
		// the Git Sync form and rejects it next to base_repo (issue #359).
		if !jsonPresent(req.BaseRepo) || jsonPresent(req.Upstream) {
			replay()
			return
		}
		var spec usyncBaseRepo
		if json.Unmarshal(req.BaseRepo, &spec) != nil {
			replay()
			return
		}
		if !spec.isCopy() && (spec.Operation != "fork" || (spec.Provider != "code" && spec.Provider != "code.storage")) {
			replay()
			return
		}
		if spec.isCopy() {
			// The copy form is new (issue #359) and upstream's BaseRepo
			// sets additionalProperties: false: a misspelled field (say,
			// "reff") must not silently copy another ref.
			dec := json.NewDecoder(bytes.NewReader(req.BaseRepo))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&usyncBaseRepo{}); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid base_repo: "+err.Error())
				return
			}
		}
		s.handleForkRepo(w, r, verifier, &req, &spec)
	}
}

// isCopy reports upstream's one-time-copy form of base_repo ({name, ref?,
// auth.token}, no provider and no operation; issue #359). A copy is a fork
// of a Code Storage repository under the current field names.
func (spec *usyncBaseRepo) isCopy() bool {
	return spec.Provider == "" && spec.Operation == ""
}

// copySyncFieldsMsg is the 400 body for a copy that carries fields of the
// deprecated sync form, which a copy would otherwise ignore.
const copySyncFieldsMsg = "base_repo without provider is a copy: owner, upstream_host, forward_pushes, and auth.auth_type need the deprecated sync form"

// copyNameMismatchMsg is the 403 body when base_repo.name does not name
// the repository base_repo.auth.token grants.
const copyNameMismatchMsg = "base_repo.name is not the repository base_repo.auth.token grants"

// handleForkRepo performs one synchronous fork (or copy). Flow: verify the embedded
// fork-source JWT (repo-scoped git:read on the source), resolve the source
// row and the fork point in its materialized cache, policy-check the fork's
// initial ref, pack the fork-point closure, create the repo row, publish the
// pack under the new RepoID, and CAS-create the default branch. Failures
// after the repo row exists soft-delete it (forkAbandon) so no orphaned
// live repo remains.
func (s *server) handleForkRepo(w http.ResponseWriter, r *http.Request, verifier auth.Verifier, req *createRepoRequest, spec *usyncBaseRepo) {
	assertf(s != nil && s.store != nil && s.refStore != nil && s.blobStore != nil && s.cacheDir != "",
		"handleForkRepo: nil server deps")
	assertf(verifier != nil && req != nil && spec != nil, "handleForkRepo: nil inputs")
	assertf(spec.isCopy() || (spec.Operation == "fork" && (spec.Provider == "code" || spec.Provider == "code.storage")),
		"handleForkRepo: mis-dispatched base_repo provider %q operation %q", spec.Provider, spec.Operation)

	claims := mustClaims(r)
	url := claims.Repo
	// requireAuth(orgLevel=false) rejects empty/"org" repo claims with 403.
	assertf(url != "" && url != auth.RepoClaimOrg, "fork: repo claim %q passed requireAuth", url)

	// docs/reference/authentication.md: ref policies apply to every HTTP write endpoint; malformed
	// policy claims fail closed exactly like the sibling creators.
	if _, err := refPolicies(claims); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	// Before the source token is even verified: a body repo_name can only
	// confirm the destination claim, never redirect the fork.
	if !req.repoNameMatches(url) {
		writeErr(w, http.StatusBadRequest, repoNameMismatchMsg)
		return
	}

	// The copy form names its source repository (checked below) and
	// carries none of the deprecated sync form's fields.
	if spec.isCopy() && spec.Name == "" {
		writeErr(w, http.StatusBadRequest, "base_repo.name is required for a copy")
		return
	}
	if spec.isCopy() && (spec.Owner != "" || spec.UpstreamHost != "" || spec.ForwardPushes || (spec.Auth != nil && spec.Auth.AuthType != "")) {
		writeErr(w, http.StatusBadRequest, copySyncFieldsMsg)
		return
	}

	// The fork-source JWT (docs/explanation/domain-model.md invariant 12) authorizes reading the
	// source repo. Absent token: malformed request (400). A token that does
	// not verify, is not repo-scoped, or lacks git:read: authorization
	// failure on the source (403).
	if spec.Auth == nil || spec.Auth.Token == "" {
		if spec.isCopy() {
			writeErr(w, http.StatusBadRequest, "base_repo.auth.token is required for a copy")
			return
		}
		writeErr(w, http.StatusBadRequest, "base_repo.auth.token is required for provider "+strconv.Quote(spec.Provider))
		return
	}
	srcClaims, err := verifier.Verify(r.Context(), spec.Auth.Token)
	if err != nil {
		writeErr(w, http.StatusForbidden, "base_repo.auth.token: invalid token")
		return
	}
	assertf(srcClaims != nil, "Verifier returned nil claims without error")
	if srcClaims.Repo == "" || srcClaims.Repo == auth.RepoClaimOrg {
		writeErr(w, http.StatusForbidden, "base_repo.auth.token is not repo-scoped")
		return
	}
	if !srcClaims.HasScope(auth.ScopeGitRead) {
		writeErr(w, http.StatusForbidden, "base_repo.auth.token is missing required scope "+auth.ScopeGitRead)
		return
	}

	ctx := r.Context()
	// The source org comes from the source token's iss claim, so cross-org
	// forks work whenever the caller holds a valid source-read JWT.
	srcRepo, err := meta.ResolveRepoKey(ctx, s.store, srcClaims.Iss, srcClaims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		writeErr(w, http.StatusNotFound, "fork source repository not found")
		return
	case err != nil:
		s.internalErr(w, r, "resolve fork source", err)
		return
	}
	assertf(srcRepo.RepoID != "", "store returned repo with empty RepoID")
	if spec.isCopy() {
		// The copy form names its source; the token must grant exactly
		// that repository, or the request would copy a repo other than the
		// one named. Resolved in the token's org, after the token verified,
		// so an unknown name and a different repo answer alike.
		named, err := meta.ResolveRepoKey(ctx, s.store, srcClaims.Iss, spec.Name)
		switch {
		case errors.Is(err, meta.ErrNotFound) || (err == nil && named.RepoID != srcRepo.RepoID):
			writeErr(w, http.StatusForbidden, copyNameMismatchMsg)
			return
		case err != nil:
			s.internalErr(w, r, "resolve copy source name", err)
			return
		}
	}

	srcPath, err := gitread.Materialize(ctx, s.refStore, s.blobStore, s.cacheDir, srcRepo.RepoID, srcRepo.DefaultBranch)
	if err != nil {
		s.internalErr(w, r, "materialize fork source", err)
		return
	}

	// Fork point (docs/reference/http-api.md): sha overrides ref; default = the source's
	// default branch. Everything unresolvable is invalid input (400).
	point := spec.SHA
	if point == "" {
		point = spec.Ref
	}
	if point == "" {
		assertf(srcRepo.DefaultBranch != "", "repo %s has empty default branch", srcRepo.RepoID)
		point = srcRepo.DefaultBranch
	}
	forkSHA, err := gitread.ResolveRef(ctx, srcPath, point, false)
	switch {
	case errors.Is(err, gitread.ErrNotFound):
		writeErr(w, http.StatusBadRequest, "base_repo fork point "+strconv.Quote(point)+" not found in source repository")
		return
	case err != nil:
		s.internalErr(w, r, "resolve fork point", err)
		return
	}
	assertf(ref.ValidSHA(forkSHA) && forkSHA != ref.ZeroSHA, "ResolveRef returned malformed sha %q", forkSHA)

	// The fork's default branch name: request body wins, then the
	// base_repo.default_branch spelling, then the query fallback, then the
	// source's own default branch name.
	branch := req.DefaultBranch
	if branch == "" {
		branch = spec.DefaultBranch
	}
	if branch == "" {
		branch = r.URL.Query().Get("default_branch")
	}
	if branch == "" {
		// The source's stored default may be refs/heads/-qualified (PATCH
		// /repo stores that spelling verbatim; both create and PATCH
		// reject other ref namespaces); the fork inherits the branch
		// NAME — forkRef below re-qualifies it.
		branch = strings.TrimPrefix(srcRepo.DefaultBranch, "refs/heads/")
	}
	if !validBranchName(branch) {
		writeErr(w, http.StatusBadRequest, "invalid default_branch")
		return
	}

	// The fork's initial ref is a ref creation by the CALLER: their ref
	// policies apply (docs/reference/authentication.md, invariant 5). Creation is fast-forward by
	// definition, so no ancestry callback is needed.
	forkRef := "refs/heads/" + branch
	deny, err := wrefPolicyCheck(claims, forkRef, ref.ZeroSHA, forkSHA, nil)
	if err != nil {
		s.internalErr(w, r, "evaluate ref policy", err)
		return
	}
	if deny != "" {
		writeErr(w, http.StatusForbidden, deny)
		return
	}

	// Pack the fork-point closure BEFORE creating the repo row: a packing
	// failure must not leave a shell repo behind. The full reachability
	// closure of forkSHA is the fork's entire object set — no negatives —
	// so the fork shares nothing with the source's storage keys.
	packDir, err := os.MkdirTemp("", "openstorage-fork-pack-*")
	if err != nil {
		s.internalErr(w, r, "create fork pack dir", err)
		return
	}
	defer os.RemoveAll(packDir)
	// Everything below (git's env and argv, the server's own stat) must
	// agree on this dir: git resolves relative paths against srcPath.
	if packDir, err = filepath.Abs(packDir); err != nil {
		s.internalErr(w, r, "resolve fork pack dir", err)
		return
	}
	// packDir doubles as git's object directory, with the source cache odb
	// as a read-only alternate (the write pipelines' scratch layout):
	// pack-objects stages its temporary pack under the object directory
	// and renames it to the destination, so both must share a filesystem.
	// With the cache odb as object directory, a TMPDIR on another
	// filesystem than the data dir failed that rename with EXDEV (#360).
	// The alternate must be absolute too: srcPath is relative under a
	// relative --cache-dir, and git runs with it as working directory.
	absSrc, err := filepath.Abs(srcPath)
	if err != nil {
		s.internalErr(w, r, "resolve fork source path", err)
		return
	}
	out, err := wrefGit(ctx, srcPath, wrefScratchEnv(absSrc, packDir), strings.NewReader(forkSHA+"\n"),
		"pack-objects", "--revs", "-q", filepath.Join(packDir, "pack"))
	if err != nil {
		s.internalErr(w, r, "pack fork closure", err)
		return
	}
	packID := strings.TrimSpace(out)
	if packID == "" {
		s.internalErr(w, r, "pack fork closure", errors.New("pack-objects emitted no pack id"))
		return
	}
	packPath := filepath.Join(packDir, "pack-"+packID+".pack")
	fi, err := os.Stat(packPath)
	if err != nil || fi.Size() == 0 {
		s.internalErr(w, r, "pack fork closure", errors.New("pack-objects produced no pack at "+packPath))
		return
	}
	// Quota pre-check BEFORE CreateRepo (issue #21, Codex round 13): the
	// fork targets a fresh repo whose pack usage is definitionally zero,
	// so the whole quota question is "does the closure fit" — answerable
	// now. Deciding it after CreateRepo would burn the requested name on
	// denial: forkAbandon soft-deletes the row, and soft-deleted URLs stay
	// reserved (recreate → 409) by design. publishPack re-checks under the
	// new RepoID (usage 0), which by construction agrees.
	if s.packQuotaBytes > 0 && fi.Size() > s.packQuotaBytes {
		writeErr(w, http.StatusForbidden, fmt.Sprintf("%v: fork closure is %d bytes, quota is %d bytes", errPackQuotaExceeded, fi.Size(), s.packQuotaBytes))
		return
	}

	repo, err := s.store.CreateRepo(ctx, claims.Iss, url, branch)
	switch {
	case errors.Is(err, meta.ErrAlreadyExists):
		answerTakenName(w, r, s.store, s.internalErr, claims.Iss, url)
		return
	case errors.Is(err, meta.ErrNotFound):
		// Unreachable after successful auth (the org must exist to have
		// verification keys), but map it defensively.
		writeErr(w, http.StatusNotFound, "organization not found")
		return
	case err != nil:
		s.internalErr(w, r, "create repository", err)
		return
	}
	assertf(repo.RepoID != "", "store returned repo with empty RepoID")

	// Publish under the NEW RepoID's keys (ADR-21), strictly before the
	// CASRef — same order as every write pipeline, via the shared REST
	// publication step (publish.go: dedup, quota, upload, register,
	// verify). From here on a failure abandons the shell repo; an
	// already-uploaded pack stays behind as an unreferenced immutable
	// object (same as a lost CAS race). A quota denial (fork closure
	// larger than the per-repo quota, issue #21) stores nothing and maps
	// to this route's plain 403 envelope.
	if err := s.publishPack(ctx, repo.RepoID, packID, packPath); err != nil {
		s.forkAbandon(ctx, claims.Iss, url)
		if errors.Is(err, errPackQuotaExceeded) {
			writeErr(w, http.StatusForbidden, err.Error())
			return
		}
		if errors.Is(err, errPackReservationsBusy) {
			// Transient row-cap denial (Codex round 43): retryable.
			rateLimited(w, 10)
			writeErr(w, http.StatusTooManyRequests, err.Error())
			return
		}
		s.internalErr(w, r, "publish fork pack", err)
		return
	}
	won, err := s.refStore.CASRef(ctx, repo.RepoID, forkRef, ref.ZeroSHA, forkSHA)
	if err != nil {
		s.forkAbandon(ctx, claims.Iss, url)
		s.internalErr(w, r, "create fork default branch", err)
		return
	}
	if !won {
		// A fresh RepoID with a pre-existing ref means a duplicate-create
		// race on the same URL: surface as the create conflict.
		s.forkAbandon(ctx, claims.Iss, url)
		writeErr(w, http.StatusConflict, "repository already exists")
		return
	}

	writeJSON(w, http.StatusCreated, createRepoResponse{
		RepoID:   repo.RepoID,
		RepoName: repo.URL,
		HTTPURL:  s.httpBaseURL + "/" + repo.RepoID + ".git",
		Message:  repoCreatedMsg(repo.URL),
	})
}

// forkAbandon soft-deletes the shell repo row after a post-create failure
// so a failed fork never leaves an orphaned live repo. Best effort: the
// original failure is what the client sees; a second fault here is only
// logged.
func (s *server) forkAbandon(ctx context.Context, org, url string) {
	assertf(s != nil && s.store != nil && s.logger != nil, "forkAbandon: nil server deps")
	assertf(org != "" && url != "", "forkAbandon: empty org/url")
	// The post-create failure may BE the request context's cancellation or
	// timeout; detach from it (bounded) so the soft-delete still executes
	// instead of failing with context.Canceled and orphaning the repo row.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if _, err := s.store.SoftDeleteRepo(ctx, org, url); err != nil {
		s.logger.Error("abandon failed fork", "org", org, "url", url, "err", err)
	}
}
