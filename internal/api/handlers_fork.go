// Synchronous repo forking (docs/explanation/domain-model.md invariant 12, docs/reference/http-api.md POST /repos,
// phase-5 workstream E):
//
//	POST /api/v1/repos  repo:write  base_repo {provider: "code"|"code.storage",
//	                                operation: "fork", auth: {token}} -> one-shot fork
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

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/gitread"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
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
		if len(req.BaseRepo) == 0 || string(req.BaseRepo) == "null" {
			replay()
			return
		}
		var spec usyncBaseRepo
		if json.Unmarshal(req.BaseRepo, &spec) != nil {
			replay()
			return
		}
		if spec.Operation != "fork" || (spec.Provider != "code" && spec.Provider != "code.storage") {
			replay()
			return
		}
		s.handleForkRepo(w, r, verifier, &req, &spec)
	}
}

// handleForkRepo performs one synchronous fork. Flow: verify the embedded
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
	assertf(spec.Operation == "fork" && (spec.Provider == "code" || spec.Provider == "code.storage"),
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

	// The fork-source JWT (docs/explanation/domain-model.md invariant 12) authorizes reading the
	// source repo. Absent token: malformed request (400). A token that does
	// not verify, is not repo-scoped, or lacks git:read: authorization
	// failure on the source (403).
	if spec.Auth == nil || spec.Auth.Token == "" {
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
	packDir, err := os.MkdirTemp("", "codestorage-fork-pack-*")
	if err != nil {
		s.internalErr(w, r, "create fork pack dir", err)
		return
	}
	defer os.RemoveAll(packDir)
	out, err := wrefGit(ctx, srcPath, nil, strings.NewReader(forkSHA+"\n"),
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
		writeErr(w, http.StatusConflict, "repository already exists")
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

	writeJSON(w, http.StatusOK, createRepoResponse{
		RepoID:   repo.RepoID,
		RepoName: repo.URL,
		HTTPURL:  s.httpBaseURL + "/" + repo.RepoID + ".git",
		Message:  "repository created",
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
