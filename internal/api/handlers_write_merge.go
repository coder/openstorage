// Merge write endpoints (docs/archive/phases/07-implementation-roadmap.md p4; contract docs/reference/http-api.md + openapi):
//
//	POST /api/v1/repos/merge          — merge source into target (git:write)
//	GET  /api/v1/repos/merge/preview  — read-only merge preview (git:read)
//
// Merge engine (docs/archive/phases/13-phase4-implementation-notes.md ADR-23): `git merge-base` + `git merge-tree
// --write-tree -z` (requires git >= 2.38, probed at first use) running in
// the per-request scratch odb over the materialized cache repo, so every
// object the merge creates stays request-local until publish. The merge
// endpoint publishes in the hook order via the landed wcPublish pipeline
// (pack-objects over the new-tip reachability frontier -> blob upload ->
// RegisterPacks -> CASRef); a lost CAS race strands only an unreferenced
// immutable pack. The preview endpoint never publishes anything — the
// scratch odb is deleted with the request (docs/explanation/domain-model.md invariant 6).
//
// Status mapping (ADR-22, amended by #532 for merge): a stale
// expected_target_sha or expected_source_sha is a 409 problem body with
// code `conflict`, as upstream documents; merge conflicts, ff_only non-fast-forwards,
// unrelated histories without allow_unrelated_histories, and CAS races
// lost at publish time are 409 `conflict` (merge conflicts additionally
// carry conflict_paths + merge_base_sha, docs/reference/http-api.md).
//
// Fast-forwards move the ref without publishing: refs only ever name
// objects already durable in registered packs (the source tip was a ref),
// so there is nothing new to upload. Reachability-based packing publishes
// ephemeral->persistent promotions for free; `promoted_commits` reports
// the number of source commits newly reachable from the persistent
// namespace when the source is ephemeral (fast-forward counts them all,
// merge-commit/squash outcomes exclude the server-created commit itself).
//
// Ref policies mirror the git update hook against the STORED target ref
// name (ephemeral targets match their refs/namespaces/ephemeral/... name):
// no-push denies every update; no-force-push denies non-fast-forwards via
// merge-base --is-ancestor (merge outcomes are fast-forward by
// construction — the old tip is a parent); verify-sig fails closed for the
// unsigned server-created merge/squash commits but passes fast-forwards,
// whose commits were already verified when pushed; unknown ops fail
// closed.
//
// All helpers carry a wm ("write merge") prefix to stay collision-free
// with sibling slice files; shared plumbing (wcResolveRepo, wcNewScratch,
// wcPublish, wcRefName, validation helpers, mergeBaseSHA) is reused from
// the landed slices. Route wiring: api.New (owned by the foundation slice)
// must call registerWriteMergeRoutes — tests mount it on their own mux
// until that one-line hookup lands.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/ref"
)

// wmMaxConflictSideBytes caps one rendered conflict side (result, base,
// ours, theirs); any larger side moves the whole path to
// filtered_conflicts instead of inlining content.
const wmMaxConflictSideBytes = 1 << 20

// wmFilteredReasonTooLarge is the filtered_conflicts reason for paths with
// a conflict side above wmMaxConflictSideBytes.
const wmFilteredReasonTooLarge = "max_content_bytes_exceeded"

// registerWriteMergeRoutes mounts the merge endpoints (called from api.New)
// with the per-route auth middleware (scope table docs/reference/authentication.md: merge needs
// git:write, the read-only preview needs git:read).
func (s *server) registerWriteMergeRoutes(mux *http.ServeMux, verifier auth.Verifier, org string, rl *rateLimiter) {
	assertf(mux != nil && s != nil && verifier != nil, "registerWriteMergeRoutes: nil dependency")
	mux.Handle("POST /api/v1/repos/merge",
		rl.limit()(requireAuth(verifier, org, false, auth.ScopeGitWrite)(http.HandlerFunc(s.handleMerge))))
	// Preview is a read (docs/reference/http-api.md: git:read) and keeps the {"error"} body.
	// Its git children run under the read budget (#313); the scratch git
	// runner (wcScratch.git) switches to gitread.Run when one is present.
	mux.Handle("GET /api/v1/repos/merge/preview",
		rl.limit()(requireAuth(verifier, org, false, auth.ScopeGitRead)(s.withGitReadBudget(s.handleMergePreview))))
}

// ---------------------------------------------------------------------------
// Wire shapes (SDK mergeResponseSchema / previewMergeResponseSchema)

type wmMergeSource struct {
	Ref       string `json:"ref"`
	Branch    string `json:"branch"` // deprecated: mirrors ref
	Ephemeral bool   `json:"ephemeral"`
	SHA       string `json:"sha"`
}

type wmMergeTarget struct {
	Branch    string `json:"branch"`
	Ephemeral bool   `json:"ephemeral"`
	OldSHA    string `json:"old_sha"`
	NewSHA    string `json:"new_sha"`
}

// wmMergeResponse is the 200 body, field-by-field the SDK's
// mergeResponseSchema (merge_base_sha optional, promoted_commits a COUNT).
type wmMergeResponse struct {
	Result          string        `json:"result"`
	CommitSHA       string        `json:"commit_sha"`
	TreeSHA         string        `json:"tree_sha"`
	Source          wmMergeSource `json:"source"`
	Target          wmMergeTarget `json:"target"`
	MergeBaseSHA    string        `json:"merge_base_sha,omitempty"`
	PromotedCommits int           `json:"promoted_commits"`
}

// wmConflictBody is upstream's MergeConflict, the 409 application/json body
// for file conflicts (conflict_type merge_conflict). Upstream also sends it
// when an automatic stale-target retry fails (target_moved, repo_changed).
// open.storage has no such retry, so a target that moved is "any other
// conflict": a plain problem body, like every other merge failure (#352
// item 3).
type wmConflictBody struct {
	Error         string   `json:"error"`
	ConflictType  string   `json:"conflict_type"`
	ConflictPaths []string `json:"conflict_paths,omitempty"`
	MergeBaseSHA  string   `json:"merge_base_sha,omitempty"`
}

// wmFailConflict writes the 409 MergeConflict body for file conflicts.
func wmFailConflict(w http.ResponseWriter, msg string, conflictPaths []string, mergeBase string) {
	assertf(msg != "" && len(conflictPaths) > 0, "wmFailConflict: message %q with %d paths", msg, len(conflictPaths))
	writeJSON(w, http.StatusConflict, wmConflictBody{
		Error:         msg,
		ConflictType:  "merge_conflict",
		ConflictPaths: conflictPaths,
		MergeBaseSHA:  mergeBase,
	})
}

// wmInternal logs the real error and writes the generic 500 problem body
// (docs/reference/http-api.md: raw error strings never leak onto the wire).
func (s *server) wmInternal(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(err != nil && op != "", "wmInternal: nil error or empty op")
	if s.writeRefHeldErr(w, r, op, err) {
		return
	}
	s.logger.Error("internal error", "op", op, "method", logMethod(r), "path", logPath(r), "err", err)
	writeErr(w, http.StatusInternalServerError, internalErrorMsg)
}

// ---------------------------------------------------------------------------
// git version probe

// wmGitProbe caches the once-per-process merge-tree support probe
// (docs/archive/phases/13-phase4-implementation-notes.md ADR-23: git >= 2.38, asserted at first use).
var wmGitProbe struct {
	once sync.Once
	err  error
}

// wmEnsureMergeTreeGit verifies the git binary supports `merge-tree
// --write-tree` and returns a clear error for older versions.
func wmEnsureMergeTreeGit(ctx context.Context) error {
	wmGitProbe.once.Do(func() {
		out, err := gitlimit.Output(exec.CommandContext(ctx, "git", "version"))
		if err != nil {
			wmGitProbe.err = fmt.Errorf("probe git version: %w", err)
			return
		}
		wmGitProbe.err = wmCheckGitVersion(strings.TrimSpace(string(out)))
	})
	return wmGitProbe.err
}

// wmCheckGitVersion parses a `git version X.Y[.Z...]` banner and rejects
// versions below 2.38 (the first release with merge-tree --write-tree).
func wmCheckGitVersion(banner string) error {
	fields := strings.Fields(banner)
	if len(fields) < 3 || fields[0] != "git" || fields[1] != "version" {
		return fmt.Errorf("unrecognized git version banner %q", banner)
	}
	parts := strings.Split(fields[2], ".")
	if len(parts) < 2 {
		return fmt.Errorf("unrecognized git version %q", fields[2])
	}
	major, majErr := strconv.Atoi(parts[0])
	minor, minErr := strconv.Atoi(parts[1])
	if majErr != nil || minErr != nil {
		return fmt.Errorf("unrecognized git version %q", fields[2])
	}
	if major > 2 || (major == 2 && minor >= 38) {
		return nil
	}
	return fmt.Errorf("git %s does not support merge-tree --write-tree; the merge endpoints require git >= 2.38", fields[2])
}

// ---------------------------------------------------------------------------
// Merge-tree engine

// wmStageEntry is one conflict side from the merge-tree records: the tree
// entry's mode and oid. Gitlink (160000) oids name commits of another
// repository and are usually absent from the odb, so renderers must never
// dereference them.
type wmStageEntry struct {
	mode string
	oid  string
}

// wmMergeTreeResult is the parsed `git merge-tree --write-tree -z` output.
type wmMergeTreeResult struct {
	tree  string
	clean bool
	// paths lists conflicted paths in merge-tree order (deduped); stages
	// maps path -> index stage (1 base, 2 ours/target, 3 theirs/source)
	// -> mode+oid, exactly the `ls-files -u` shape of the output records.
	paths  []string
	stages map[string]map[int]wmStageEntry
}

// wmRunMergeTree three-way-merges source into target inside the scratch
// odb (every object it creates lands there, nothing touches the cache).
// Exit 0 is a clean merge, exit 1 carries conflicts; anything else is an
// internal error. Spiked against git 2.55: the -z output is NUL-separated
// tokens — the result tree oid, then (conflicts only) `ls-files -u`-shaped
// records `<mode> <oid> <stage>\t<path>`, then an EMPTY token before the
// informational-message section, which this parser ignores.
func wmRunMergeTree(ctx context.Context, sc *wcScratch, targetSHA, sourceSHA string, allowUnrelated bool) (*wmMergeTreeResult, error) {
	assertf(ref.ValidSHA(targetSHA) && ref.ValidSHA(sourceSHA), "wmRunMergeTree: malformed shas %q %q", targetSHA, sourceSHA)
	args := []string{"merge-tree", "--write-tree", "-z"}
	if allowUnrelated {
		args = append(args, "--allow-unrelated-histories")
	}
	args = append(args, targetSHA, sourceSHA)
	// Exit 1 means conflicts; its stdout is still the result.
	out, err := sc.git(ctx, nil, nil, args...)
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			return nil, err
		}
	}
	return wmParseMergeTreeZ(out, err == nil)
}

// wmParseMergeTreeZ parses the NUL-separated merge-tree output described
// on wmRunMergeTree.
func wmParseMergeTreeZ(raw string, clean bool) (*wmMergeTreeResult, error) {
	tokens := strings.Split(raw, "\x00")
	if len(tokens) == 0 || !ref.ValidSHA(tokens[0]) {
		return nil, fmt.Errorf("merge-tree emitted no result tree (output %q)", raw)
	}
	res := &wmMergeTreeResult{
		tree:   tokens[0],
		clean:  clean,
		paths:  []string{},
		stages: map[string]map[int]wmStageEntry{},
	}
	if clean {
		return res, nil
	}
	for _, tok := range tokens[1:] {
		if tok == "" {
			break // separator: informational messages follow
		}
		head, path, found := strings.Cut(tok, "\t")
		if !found || path == "" {
			return nil, fmt.Errorf("malformed merge-tree conflict record %q", tok)
		}
		fields := strings.Fields(head)
		if len(fields) != 3 || !ref.ValidSHA(fields[1]) {
			return nil, fmt.Errorf("malformed merge-tree conflict record %q", tok)
		}
		stage, err := strconv.Atoi(fields[2])
		if err != nil || stage < 1 || stage > 3 {
			return nil, fmt.Errorf("malformed merge-tree conflict stage in %q", tok)
		}
		if _, seen := res.stages[path]; !seen {
			res.paths = append(res.paths, path)
			res.stages[path] = map[int]wmStageEntry{}
		}
		res.stages[path][stage] = wmStageEntry{mode: fields[0], oid: fields[1]}
	}
	return res, nil
}

// wmIsAncestor reports whether a is an ancestor of (or equal to) b, with
// scratch-created objects visible; merge-base exit 1 means "no".
func wmIsAncestor(ctx context.Context, sc *wcScratch, a, b string) (bool, error) {
	assertf(ref.ValidSHA(a) && ref.ValidSHA(b), "wmIsAncestor: malformed shas %q %q", a, b)
	_, err := sc.git(ctx, nil, nil, "merge-base", "--is-ancestor", a, b)
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

// wmSourceContains reports whether pin is a commit reachable from (or
// equal to) sourceTip. An id the odb lacks, or one naming a tree, blob or
// tag object, is not contained: git merge-base would fail on it.
func wmSourceContains(ctx context.Context, sc *wcScratch, pin, sourceTip string) (bool, error) {
	assertf(ref.ValidSHA(pin) && ref.ValidSHA(sourceTip), "wmSourceContains: malformed shas %q %q", pin, sourceTip)
	out, err := sc.git(ctx, strings.NewReader(pin+"\n"), nil, "cat-file", "--batch-check=%(objecttype)")
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(out) != "commit" { // "<pin> missing" or another type
		return false, nil
	}
	return wmIsAncestor(ctx, sc, pin, sourceTip)
}

// wmCommitTree creates the merge/squash commit in the scratch odb.
// Committer defaults are the caller's concern; parents order is
// [target, source] for merge commits and [target] for squash.
func wmCommitTree(ctx context.Context, sc *wcScratch, tree string, parents []string, message string, author, committer wcAuthor) (string, error) {
	assertf(ref.ValidSHA(tree), "wmCommitTree: malformed tree %q", tree)
	assertf(len(parents) >= 1, "wmCommitTree: no parents")
	assertf(message != "", "wmCommitTree: empty message")
	env := []string{
		"GIT_AUTHOR_NAME=" + author.Name,
		"GIT_AUTHOR_EMAIL=" + author.Email,
		"GIT_COMMITTER_NAME=" + committer.Name,
		"GIT_COMMITTER_EMAIL=" + committer.Email,
	}
	args := []string{"commit-tree", "-F", "-"}
	for _, p := range parents {
		assertf(ref.ValidSHA(p), "wmCommitTree: malformed parent %q", p)
		args = append(args, "-p", p)
	}
	args = append(args, tree)
	out, err := sc.git(ctx, wcMessageInput(message), env, args...)
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(out)
	if !ref.ValidSHA(sha) {
		return "", fmt.Errorf("commit-tree emitted %q", sha)
	}
	return sha, nil
}

// wmPromotedCommits counts the commits newly reachable from the persistent
// namespace after the update: `git rev-list --count <new> --not <every
// pre-update persistent tip>`, minus the server-created commit when the
// outcome created one. Only ephemeral sources promote; persistent sources
// were already reachable and report 0.
func wmPromotedCommits(ctx context.Context, sc *wcScratch, tips map[string]string, newSHA string, createdCommit, sourceEphemeral bool) (int, error) {
	if !sourceEphemeral {
		return 0, nil
	}
	assertf(ref.ValidSHA(newSHA), "wmPromotedCommits: malformed sha %q", newSHA)
	args := []string{"rev-list", "--count", newSHA, "--not"}
	seen := map[string]bool{}
	for name, tip := range tips {
		if strings.HasPrefix(name, auth.EphemeralNamespacePrefix) {
			continue
		}
		if !seen[tip] {
			seen[tip] = true
			args = append(args, tip)
		}
	}
	out, err := sc.git(ctx, nil, nil, args...)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0, fmt.Errorf("rev-list --count emitted %q", out)
	}
	if createdCommit {
		// The created merge/squash commit is never reachable from a
		// pre-update tip, so it is always in the count — exclude it.
		n--
	}
	assertf(n >= 0, "wmPromotedCommits: negative count %d", n)
	return n, nil
}

// wmCheckRefPolicy enforces the token ref policies (docs/reference/authentication.md, invariant 5)
// for the merge-driven target update, mirroring the git update hook (see
// package comment for the per-op semantics). Writes the 403 problem body and
// returns false on denial.
func (s *server) wmCheckRefPolicy(w http.ResponseWriter, r *http.Request, sc *wcScratch, refName, old, new string, serverCreatedCommit bool, targetBranch string) bool {
	assertf(ref.ValidSHA(old) && ref.ValidSHA(new), "wmCheckRefPolicy: malformed shas %q %q", old, new)
	deny := func(msg string) bool {
		writeErr(w, http.StatusForbidden, msg)
		return false
	}
	policies, perr := refPolicies(mustClaims(r))
	if perr != nil {
		return deny(perr.Error())
	}
	rule, ok := auth.MatchPolicy(policies, refName)
	if !ok {
		return true // unpoliced ref: allowed
	}
	for _, op := range rule.Ops {
		switch op {
		case auth.OpNoPush:
			return deny("policy no-push denies update to " + refName)
		case auth.OpNoForcePush:
			// Merge outcomes are fast-forward by construction (the old tip
			// is the new commit's parent, or its ancestor on ff) — but the
			// hook decides via merge-base, so mirror it exactly.
			ff, err := wmIsAncestor(r.Context(), sc, old, new)
			if err != nil {
				s.wmInternal(w, r, "evaluate ref policy", err)
				return false
			}
			if !ff {
				return deny("policy no-force-push denies non-fast-forward update to " + refName)
			}
		case auth.OpVerifySig:
			if serverCreatedCommit {
				return deny("policy verify-sig denies unsigned server-created commits on " + refName)
			}
			// Fast-forwards introduce no server-created commits: the
			// promoted commits already passed hook verification at push
			// time, so verify-sig lets them through (hook parity).
		default:
			return deny("unknown policy op " + op + " for " + refName)
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// POST /api/v1/repos/merge

type wmMergeRequest struct {
	SourceRef               string    `json:"source_ref"`
	SourceBranch            string    `json:"source_branch"` // deprecated alias of source_ref
	TargetBranch            string    `json:"target_branch"`
	Strategy                string    `json:"strategy"` // merge|ff_only|ff_prefer
	SourceIsEphemeral       bool      `json:"source_is_ephemeral"`
	TargetIsEphemeral       bool      `json:"target_is_ephemeral"`
	ExpectedTargetSHA       string    `json:"expected_target_sha"`
	CommitMessage           string    `json:"commit_message"`
	Author                  *wcAuthor `json:"author"`
	Committer               *wcAuthor `json:"committer"`
	AllowUnrelatedHistories bool      `json:"allow_unrelated_histories"`
	Squash                  bool      `json:"squash"`

	// ExpectedSourceSHA pins the source commit (spec 2026-10-16, #531).
	// It stays raw so an explicit null or "" is a 400, not an absent
	// guard. wmValidateMergeRequest stores the checked id in sourcePin.
	ExpectedSourceSHA json.RawMessage `json:"expected_source_sha"`
	sourcePin         string
}

// wmValidateMergeRequest checks the request fields shared by every merge
// outcome; author presence is checked later, only when the outcome
// actually creates a commit (openapi: required for merge-commit
// strategies).
func wmValidateMergeRequest(req *wmMergeRequest) error {
	assertf(req != nil, "wmValidateMergeRequest: nil request")
	// openapi (issue #17): source_ref — a branch name or its fully
	// qualified refs/heads form — is the current field; source_branch
	// stays as the deprecated bare-name alias, the current name winning
	// on conflict. Canonicalize into SourceBranch so the merge flow and
	// the response echo one bare branch name.
	sourceField := "source_ref"
	if req.SourceRef != "" {
		req.SourceBranch = strings.TrimPrefix(req.SourceRef, "refs/heads/")
	} else if req.SourceBranch != "" {
		sourceField = "source_branch"
	}
	if err := wcValidateBranchParam(sourceField, req.SourceBranch); err != nil {
		return err
	}
	if err := wcValidateBranchParam("target_branch", req.TargetBranch); err != nil {
		return err
	}
	switch req.Strategy {
	case "merge", "ff_only", "ff_prefer":
	case "":
		return newFieldError("strategy", "is required", "missing strategy")
	default:
		return newFieldError("strategy", "must be merge, ff_only or ff_prefer",
			fmt.Sprintf("invalid strategy %q (want merge|ff_only|ff_prefer)", req.Strategy))
	}
	if req.Squash && req.Strategy == "ff_only" {
		return errors.New("squash is incompatible with the ff_only strategy")
	}
	req.ExpectedTargetSHA = lowerFullSHA(req.ExpectedTargetSHA)
	if req.ExpectedTargetSHA != "" && !ref.ValidSHA(req.ExpectedTargetSHA) {
		return newFieldError("expected_target_sha", "must be a 40-hex sha", "expected_target_sha must be a 40-hex sha")
	}
	if req.ExpectedSourceSHA != nil {
		var pin string
		if err := json.Unmarshal(req.ExpectedSourceSHA, &pin); err != nil || !ref.ValidSHA(lowerFullSHA(pin)) {
			return newFieldError("expected_source_sha", "must be a 40-hex sha", "expected_source_sha must be a 40-hex sha")
		}
		req.sourcePin = lowerFullSHA(pin)
	}
	if err := wcValidateCommitMessage(req.CommitMessage); err != nil {
		return err
	}
	if req.Author != nil {
		if err := wcValidateAuthor("author", *req.Author); err != nil {
			return err
		}
	}
	if req.Committer != nil {
		if err := wcValidateAuthor("committer", *req.Committer); err != nil {
			return err
		}
	}
	return nil
}

// handleMerge implements POST /repos/merge (docs/reference/http-api.md): resolve both branch
// tips from the ref authority, guard on expected_target_sha and
// expected_source_sha (409 ref_guard_failed problem bodies), decide
// the outcome (no_op / fast_forward / merge_commit / squash), run
// merge-tree in the scratch odb, enforce ref policies, and publish under
// CAS.
func (s *server) handleMerge(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.wcResolveRepo(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read request body: "+err.Error())
		return
	}
	var req wmMergeRequest
	if len(body) == 0 {
		writeErr(w, http.StatusBadRequest, "missing request body")
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeInvalid(w, "invalid JSON body: "+err.Error(), err)
		return
	}
	if err := wmValidateMergeRequest(&req); err != nil {
		writeInvalid(w, err.Error(), err)
		return
	}

	ctx := r.Context()
	tips, err := s.refStore.ListRefs(ctx, repo.RepoID)
	if err != nil {
		s.wmInternal(w, r, "list refs", err)
		return
	}
	sourceRef := wcRefName(req.SourceBranch, req.SourceIsEphemeral)
	targetRef := wcRefName(req.TargetBranch, req.TargetIsEphemeral)
	// With expected_source_sha, a missing source branch fails the source
	// guard below (upstream: current_sha is 40 zeros), after the target
	// checks.
	sourceSHA, sourceExists := tips[sourceRef]
	if !sourceExists && req.sourcePin == "" {
		writeProblem(w, http.StatusNotFound, codeBranchNotFound, "source branch "+req.SourceBranch+" not found")
		return
	}
	targetSHA, ok := tips[targetRef]
	if !ok {
		writeProblem(w, http.StatusNotFound, codeBranchNotFound, "target branch "+req.TargetBranch+" not found")
		return
	}
	// A stale guard is a 409 ref_guard_failed problem body before any
	// merge work happens (ADR-22 as amended by #532 and #568: upstream's
	// merge contract).
	if req.ExpectedTargetSHA != "" && strings.ToLower(req.ExpectedTargetSHA) != targetSHA {
		writeRefGuardFailed(w, "expected_target_sha", targetSHA,
			fmt.Sprintf("expected_target_sha %s does not match current tip %s", req.ExpectedTargetSHA, targetSHA))
		return
	}
	sourceMovedMsg := "source branch moved: the source branch does not contain commit " + req.sourcePin
	if !sourceExists {
		writeRefGuardFailed(w, "expected_source_sha", ref.ZeroSHA, sourceMovedMsg)
		return
	}

	if err := wmEnsureMergeTreeGit(ctx); err != nil {
		s.wmInternal(w, r, "probe merge-tree support", err)
		return
	}
	repoPath, err := gitread.Materialize(ctx, s.refStore, s.blobStore, s.cacheDir, repo.RepoID, repo.DefaultBranch)
	if err != nil {
		s.wmInternal(w, r, "materialize repository", err)
		return
	}
	sc, err := wcNewScratch(repoPath)
	if err != nil {
		s.wmInternal(w, r, "create scratch", err)
		return
	}
	defer sc.close()

	// expected_source_sha: merge the pinned commit instead of the source
	// tip, but only if the source branch still contains it. Like the target
	// guard, this runs before any merge work.
	if req.sourcePin != "" {
		contained, err := wmSourceContains(ctx, sc, req.sourcePin, sourceSHA)
		if err != nil {
			s.wmInternal(w, r, "check expected_source_sha ancestry", err)
			return
		}
		if !contained {
			writeRefGuardFailed(w, "expected_source_sha", sourceSHA, sourceMovedMsg)
			return
		}
		sourceSHA = req.sourcePin
	}

	base, hasBase, err := mergeBaseSHA(ctx, repoPath, targetSHA, sourceSHA)
	if err != nil {
		s.wmInternal(w, r, "compute merge base", err)
		return
	}
	respond := func(outcome, commit, tree, newSHA string, promoted int) {
		mergeBase := ""
		if hasBase {
			mergeBase = base
		}
		writeJSON(w, http.StatusOK, wmMergeResponse{
			Result:    outcome,
			CommitSHA: commit,
			TreeSHA:   tree,
			Source:    wmMergeSource{Ref: req.SourceBranch, Branch: req.SourceBranch, Ephemeral: req.SourceIsEphemeral, SHA: sourceSHA},
			Target: wmMergeTarget{
				Branch: req.TargetBranch, Ephemeral: req.TargetIsEphemeral,
				OldSHA: targetSHA, NewSHA: newSHA,
			},
			MergeBaseSHA:    mergeBase,
			PromotedCommits: promoted,
		})
	}

	// no_op: the target already contains the source — no ref update, no
	// policy question, nothing to publish.
	noOp, err := wmIsAncestor(ctx, sc, sourceSHA, targetSHA)
	if err != nil {
		s.wmInternal(w, r, "check no-op ancestry", err)
		return
	}
	if noOp {
		tree, err := sc.resolveTree(ctx, targetSHA)
		if err != nil {
			s.wmInternal(w, r, "resolve target tree", err)
			return
		}
		respond("no_op", targetSHA, tree, targetSHA, 0)
		return
	}

	ffPossible := false
	if hasBase {
		ffPossible, err = wmIsAncestor(ctx, sc, targetSHA, sourceSHA)
		if err != nil {
			s.wmInternal(w, r, "check fast-forward ancestry", err)
			return
		}
	}

	// Fast-forward: ff_only and ff_prefer take it when possible (squash
	// always collapses instead); the plain merge strategy never
	// fast-forwards — it records a merge commit (--no-ff semantics).
	if !req.Squash && ffPossible && req.Strategy != "merge" {
		if !s.wmCheckRefPolicy(w, r, sc, targetRef, targetSHA, sourceSHA, false, req.TargetBranch) {
			return
		}
		promoted, err := wmPromotedCommits(ctx, sc, tips, sourceSHA, false, req.SourceIsEphemeral)
		if err != nil {
			s.wmInternal(w, r, "count promoted commits", err)
			return
		}
		tree, err := sc.resolveTree(ctx, sourceSHA)
		if err != nil {
			s.wmInternal(w, r, "resolve source tree", err)
			return
		}
		// Nothing to publish: every object reachable from the source tip
		// is already durable in registered packs (it was a ref), so the
		// fast-forward is a pure CAS ref move.
		won, err := s.refStore.CASRef(ctx, repo.RepoID, targetRef, targetSHA, sourceSHA)
		if err != nil {
			s.wmInternal(w, r, "fast-forward ref", err)
			return
		}
		if !won {
			writeErr(w, http.StatusConflict, "ref "+targetRef+" moved concurrently")
			return
		}
		respond("fast_forward", sourceSHA, tree, sourceSHA, promoted)
		return
	}
	// Upstream's merge 412 names both refusals: "a merge with no common
	// ancestor" and a target "that moved off a fast-forward under the
	// ff_only strategy".
	if req.Strategy == "ff_only" {
		writeErr(w, http.StatusPreconditionFailed, "merge of "+req.SourceBranch+" into "+req.TargetBranch+" is not a fast-forward")
		return
	}
	if !hasBase && !req.AllowUnrelatedHistories {
		writeErr(w, http.StatusPreconditionFailed, "refusing to merge unrelated histories (set allow_unrelated_histories)")
		return
	}

	mt, err := wmRunMergeTree(ctx, sc, targetSHA, sourceSHA, !hasBase)
	if err != nil {
		s.wmInternal(w, r, "run merge-tree", err)
		return
	}
	if !mt.clean {
		mergeBase := ""
		if hasBase {
			mergeBase = base
		}
		wmFailConflict(w,
			fmt.Sprintf("merge of %s into %s has %d conflicting paths", req.SourceBranch, req.TargetBranch, len(mt.paths)),
			mt.paths, mergeBase)
		return
	}
	if req.Author == nil {
		writeInvalidField(w, "author", "is required", "author is required when the merge creates a commit")
		return
	}
	committer := *req.Author
	if req.Committer != nil {
		committer = *req.Committer
	}
	message := req.CommitMessage
	if message == "" {
		message = "Merge " + req.SourceBranch + " into " + req.TargetBranch
	}
	outcome := "merge_commit"
	parents := []string{targetSHA}
	if req.Squash {
		// Squash: one new commit, parent = current target tip, tree = the
		// merged tree; the source history stays unreferenced.
		outcome = "squash"
	} else {
		parents = append(parents, sourceSHA)
	}
	commit, err := wmCommitTree(ctx, sc, mt.tree, parents, message, *req.Author, committer)
	if err != nil {
		s.wmInternal(w, r, "create merge commit", err)
		return
	}
	if !s.wmCheckRefPolicy(w, r, sc, targetRef, targetSHA, commit, true, req.TargetBranch) {
		return
	}
	promoted, err := wmPromotedCommits(ctx, sc, tips, commit, true, req.SourceIsEphemeral)
	if err != nil {
		s.wmInternal(w, r, "count promoted commits", err)
		return
	}
	// Publish in hook order via the landed pipeline (pack over the
	// reachability frontier -> upload -> register -> CAS); a lost race
	// strands only an unreferenced immutable pack (invariant 1).
	_, won, err := s.wcPublish(ctx, sc, repo.RepoID, &wcPlan{refName: targetRef, oldSHA: targetSHA, tips: tips}, commit)
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
	if err != nil {
		s.wmInternal(w, r, "publish merge", err)
		return
	}
	if !won {
		writeErr(w, http.StatusConflict, "ref "+targetRef+" moved concurrently")
		return
	}
	respond(outcome, commit, mt.tree, commit, promoted)
}

// ---------------------------------------------------------------------------
// GET /api/v1/repos/merge/preview

// wmPreviewBlob is one conflict side (SDK previewMergeBlobSchema): oid and
// content are optional (absent stage, or binary content), truncated/binary
// are mandatory. Content is never truncated — oversized sides move the
// whole path to filtered_conflicts instead.
type wmPreviewBlob struct {
	OID       string `json:"oid,omitempty"`
	Content   string `json:"content,omitempty"`
	Truncated bool   `json:"truncated"`
	Binary    bool   `json:"binary"`
}

type wmPreviewConflict struct {
	Path   string        `json:"path"`
	Result wmPreviewBlob `json:"result"`
	Base   wmPreviewBlob `json:"base"`
	Ours   wmPreviewBlob `json:"ours"`
	Theirs wmPreviewBlob `json:"theirs"`
}

type wmPreviewFiltered struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type wmPreviewResponse struct {
	Status            string              `json:"status"` // clean|conflicted
	Result            string              `json:"result"` // merge_commit|fast_forward|no_op
	SourceBranch      string              `json:"source_branch"`
	TargetBranch      string              `json:"target_branch"`
	SourceTipSHA      string              `json:"source_tip_sha"`
	TargetTipSHA      string              `json:"target_tip_sha"`
	MergeBaseSHA      string              `json:"merge_base_sha,omitempty"`
	ConflictPaths     []string            `json:"conflict_paths"`
	Conflicts         []wmPreviewConflict `json:"conflicts"`
	FilteredConflicts []wmPreviewFiltered `json:"filtered_conflicts"`
}

// handleMergePreview implements GET /repos/merge/preview (docs/reference/http-api.md,
// invariant 6): a read-only dry run answering 200 for clean AND conflicted
// outcomes. Everything merge-tree creates lands in the request's scratch
// odb, which is deleted on return — no ref store, blob store, or cache
// mutation is possible. The contract has no allow_unrelated_histories
// parameter, so unrelated histories preview as if it were allowed.
func (s *server) handleMergePreview(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.resolveReadRepo(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	source := q.Get("source_branch")
	target := q.Get("target_branch")
	if err := wcValidateBranchParam("source_branch", source); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := wcValidateBranchParam("target_branch", target); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	includeContent, err := coreBoolParam(q, "include_content")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// source_is_ephemeral/target_is_ephemeral are the official upstream
	// query params (mirroring POST /repos/merge's body flags); this server
	// historically spelled them source_ephemeral/target_ephemeral, kept as
	// deprecated aliases. The vendor SDK never sends either spelling, so
	// absent flags preserve its default (persistent-namespace) behavior.
	sourceEphemeral, err := coreAliasBoolParam(q, "source_is_ephemeral", "source_ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	targetEphemeral, err := coreAliasBoolParam(q, "target_is_ephemeral", "target_ephemeral")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	tips, err := s.refStore.ListRefs(ctx, repo.RepoID)
	if err != nil {
		s.internalErr(w, r, "list refs", err)
		return
	}
	sourceSHA, ok := tips[wcRefName(source, sourceEphemeral)]
	if !ok {
		writeProblem(w, http.StatusNotFound, codeBranchNotFound, "source branch "+source+" not found")
		return
	}
	targetSHA, ok := tips[wcRefName(target, targetEphemeral)]
	if !ok {
		writeProblem(w, http.StatusNotFound, codeBranchNotFound, "target branch "+target+" not found")
		return
	}

	if err := wmEnsureMergeTreeGit(ctx); err != nil {
		s.internalErr(w, r, "probe merge-tree support", err)
		return
	}
	repoPath, ok := s.materializeReadRepo(w, r, repo)
	if !ok {
		return
	}
	sc, err := wcNewScratch(repoPath)
	if err != nil {
		s.internalErr(w, r, "create scratch", err)
		return
	}
	defer sc.close()

	base, hasBase, err := mergeBaseSHA(ctx, repoPath, targetSHA, sourceSHA)
	if err != nil {
		s.internalErr(w, r, "compute merge base", err)
		return
	}
	resp := wmPreviewResponse{
		Status:            "clean",
		SourceBranch:      source,
		TargetBranch:      target,
		SourceTipSHA:      sourceSHA,
		TargetTipSHA:      targetSHA,
		ConflictPaths:     []string{},
		Conflicts:         []wmPreviewConflict{},
		FilteredConflicts: []wmPreviewFiltered{},
	}
	if hasBase {
		resp.MergeBaseSHA = base
	}

	noOp, err := wmIsAncestor(ctx, sc, sourceSHA, targetSHA)
	if err != nil {
		s.internalErr(w, r, "check no-op ancestry", err)
		return
	}
	if noOp {
		resp.Result = "no_op"
		writeJSON(w, http.StatusOK, resp)
		return
	}
	if hasBase {
		ff, err := wmIsAncestor(ctx, sc, targetSHA, sourceSHA)
		if err != nil {
			s.internalErr(w, r, "check fast-forward ancestry", err)
			return
		}
		if ff {
			resp.Result = "fast_forward"
			writeJSON(w, http.StatusOK, resp)
			return
		}
	}

	mt, err := wmRunMergeTree(ctx, sc, targetSHA, sourceSHA, !hasBase)
	if err != nil {
		s.internalErr(w, r, "run merge-tree", err)
		return
	}
	resp.Result = "merge_commit"
	if mt.clean {
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.Status = "conflicted"
	resp.ConflictPaths = mt.paths
	if includeContent {
		conflicts, filtered, err := wmRenderConflicts(ctx, sc, mt)
		if err != nil {
			s.internalErr(w, r, "render conflict content", err)
			return
		}
		resp.Conflicts = conflicts
		resp.FilteredConflicts = filtered
	}
	writeJSON(w, http.StatusOK, resp)
}

// wmRenderConflicts renders inline conflict content (include_content=true)
// for every conflicted path: the index stages from the merge-tree records
// (1=base, 2=ours/target, 3=theirs/source) plus the "result" blob taken
// from the merged tree (conflict markers for text conflicts). Binary
// detection is NUL-byte/UTF-8 based; any side above wmMaxConflictSideBytes
// moves the whole path to filtered_conflicts.
func wmRenderConflicts(ctx context.Context, sc *wcScratch, mt *wmMergeTreeResult) ([]wmPreviewConflict, []wmPreviewFiltered, error) {
	assertf(!mt.clean, "wmRenderConflicts: clean merge result")
	conflicts := []wmPreviewConflict{}
	filtered := []wmPreviewFiltered{}
	listings := wmTreeListings{}
	for _, path := range mt.paths {
		stages := mt.stages[path]
		resultEntry, err := wmTreePathEntry(ctx, sc, listings, mt.tree, path)
		if err != nil {
			return nil, nil, err
		}
		// Slot order matches the response shape: result, base, ours, theirs.
		entries := [4]wmStageEntry{resultEntry, stages[1], stages[2], stages[3]}
		tooLarge := false
		for _, entry := range entries {
			if !wmBlobMode(entry.mode) {
				continue
			}
			out, err := sc.git(ctx, nil, nil, "cat-file", "-s", entry.oid)
			if err != nil {
				return nil, nil, err
			}
			size, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("cat-file -s %s emitted %q", entry.oid, out)
			}
			if size > wmMaxConflictSideBytes {
				tooLarge = true
				break
			}
		}
		if tooLarge {
			filtered = append(filtered, wmPreviewFiltered{Path: path, Reason: wmFilteredReasonTooLarge})
			continue
		}
		var blobs [4]wmPreviewBlob
		for i, entry := range entries {
			if entry.oid == "" {
				continue // absent stage: {truncated:false, binary:false}
			}
			if entry.mode == "160000" {
				// Gitlink side: the oid names a commit that is usually not
				// in this repository's odb at all, so report it without
				// ever reading it.
				blobs[i] = wmPreviewBlob{OID: entry.oid}
				continue
			}
			if !wmBlobMode(entry.mode) {
				continue // non-blob result entry (e.g. a D/F-conflict tree)
			}
			content, err := sc.git(ctx, nil, nil, "cat-file", "blob", entry.oid)
			if err != nil {
				return nil, nil, err
			}
			binary := strings.ContainsRune(content, 0) || !utf8.ValidString(content)
			blobs[i] = wmPreviewBlob{OID: entry.oid, Binary: binary}
			if !binary {
				blobs[i].Content = content
			}
		}
		conflicts = append(conflicts, wmPreviewConflict{
			Path: path, Result: blobs[0], Base: blobs[1], Ours: blobs[2], Theirs: blobs[3],
		})
	}
	return conflicts, filtered, nil
}

// wmBlobMode reports whether a tree-entry mode names a readable blob
// (regular file, executable, or symlink). Gitlinks (160000) and trees
// (040000) must never be cat-file'd: gitlink targets usually do not exist
// in the odb (previously a 500 on submodule-conflict previews).
func wmBlobMode(mode string) bool {
	switch mode {
	case "100644", "100755", "120000":
		return true
	}
	return false
}

// wmLiteralPathspec makes an ls-tree pathspec match its path byte for byte.
const wmLiteralPathspec = ":(literal)"

// wmMaxPathspecBytes bounds the path bytes of one ls-tree pathspec argument.
// Linux refuses to exec a program with a single argument of 128 KiB or more,
// counting its terminating NUL (MAX_ARG_STRLEN: 32 pages of at least 4 KiB,
// error E2BIG). git stores and merges paths of that size, so a longer path is
// resolved one chunk at a time. The limit is the longest path that still fits
// behind wmLiteralPathspec, so that every path which one ls-tree can name
// still takes one ls-tree and lists nothing.
const wmMaxPathspecBytes = 128<<10 - len(wmLiteralPathspec) - 1

// wmTreeListings holds, for each tree that wmTreePathEntry had to list, its
// entries with names over wmMaxPathspecBytes, by tree oid. A tree is listed
// once per request however many conflicts sit in it: a listing emits the
// whole tree, and the preview buffers all git output under one budget.
type wmTreeListings map[string]map[string]wmStageEntry

// wmTreePathEntry resolves the merged tree's entry at path via ls-tree —
// mode+oid straight from the tree, never dereferencing the object, so
// gitlink entries whose commits are absent from the odb resolve fine
// (previously cat-file'd and 500'd). Zero value when the path is absent
// (e.g. one side of a delete/modify conflict).
//
// A path of at most wmMaxPathspecBytes takes one ls-tree. A longer path is
// cut at slashes into chunks of that size, and each chunk is looked up in
// the tree that the previous chunk named. A single component longer than the
// limit is not named in argv: it is found in the listing of its directory,
// which listings keeps for the next such component.
func wmTreePathEntry(ctx context.Context, sc *wcScratch, listings wmTreeListings, tree, path string) (wmStageEntry, error) {
	assertf(ref.ValidSHA(tree) && path != "", "wmTreePathEntry: bad inputs %q %q", tree, path)
	dir, rest := tree, path
	for {
		var chunk string
		var entry wmStageEntry
		var err error
		if chunk = wmPathChunk(rest, wmMaxPathspecBytes); chunk != "" {
			entry, err = wmLsTreePathspec(ctx, sc, dir, chunk)
		} else {
			chunk, _, _ = strings.Cut(rest, "/")
			entry, err = wmLsTreeListed(ctx, sc, listings, dir, chunk)
		}
		if err != nil || entry.oid == "" {
			return wmStageEntry{}, err // failed, or the path is absent
		}
		if len(chunk) == len(rest) {
			return entry, nil
		}
		if entry.mode != "040000" {
			return wmStageEntry{}, nil // a file or gitlink has no children
		}
		dir, rest = entry.oid, rest[len(chunk)+1:]
	}
}

// wmPathChunk returns the longest prefix of path that is at most limit bytes
// and ends at a component boundary, or "" when path starts with a component
// longer than limit. A path within limit is its own chunk.
func wmPathChunk(path string, limit int) string {
	if len(path) <= limit {
		return path
	}
	if i := strings.LastIndexByte(path[:limit+1], '/'); i > 0 {
		return path[:i]
	}
	return ""
}

// wmLsTreePathspec looks up one path (at most wmMaxPathspecBytes) in tree
// with a literal pathspec. The entry is zero when the path is absent.
func wmLsTreePathspec(ctx context.Context, sc *wcScratch, tree, path string) (wmStageEntry, error) {
	out, err := sc.git(ctx, nil, nil, "ls-tree", "-z", tree, "--", wmLiteralPathspec+path)
	if err != nil {
		return wmStageEntry{}, err
	}
	record := strings.TrimSuffix(out, "\x00")
	if record == "" {
		return wmStageEntry{}, nil // path absent from the tree
	}
	// One entry: path is a full path, not a prefix, and ls-tree is not
	// recursive.
	_, entry, ok := wmParseLsTreeRecord(record)
	if !ok || strings.Contains(record, "\x00") {
		return wmStageEntry{}, fmt.Errorf("ls-tree %s -- %.200q emitted %.200q", tree, path, out)
	}
	return entry, nil
}

// wmLsTreeListed finds the entry called name in the listing of the whole of
// tree, which names no path in argv. The entry is zero when tree has none.
// name is longer than wmMaxPathspecBytes: shorter names are looked up by
// pathspec, and wmParseListing keeps only the entries of longer ones.
func wmLsTreeListed(ctx context.Context, sc *wcScratch, listings wmTreeListings, tree, name string) (wmStageEntry, error) {
	if len(name) <= wmMaxPathspecBytes {
		return wmStageEntry{}, fmt.Errorf("ls-tree %s: a %d-byte name fits one argument and is not in a listing", tree, len(name))
	}
	entries, ok := listings[tree]
	if !ok {
		out, err := sc.git(ctx, nil, nil, "ls-tree", "-z", tree)
		if err != nil {
			return wmStageEntry{}, err
		}
		if entries, err = wmParseListing(out); err != nil {
			return wmStageEntry{}, fmt.Errorf("ls-tree %s: %w", tree, err)
		}
		listings[tree] = entries
	}
	return entries[name], nil
}

// wmParseListing scans the output of `ls-tree -z <tree>` and returns the
// entries whose names are longer than wmMaxPathspecBytes, by name. It keeps
// no entry with a shorter name, and copies what it keeps, so that a directory
// of 500,000 ordinary names costs the request its listing and then nothing
// (parsing every record into a map held about three times the listing).
func wmParseListing(out string) (map[string]wmStageEntry, error) {
	entries := map[string]wmStageEntry{}
	for rest := out; rest != ""; {
		var record string
		record, rest, _ = strings.Cut(rest, "\x00")
		head, name, found := strings.Cut(record, "\t")
		if !found {
			return nil, fmt.Errorf("emitted %.200q", record)
		}
		if len(name) <= wmMaxPathspecBytes {
			continue
		}
		_, entry, valid := wmParseLsTreeRecord(record)
		if !valid {
			return nil, fmt.Errorf("emitted %.200q", head)
		}
		entries[strings.Clone(name)] = wmStageEntry{mode: strings.Clone(entry.mode), oid: strings.Clone(entry.oid)}
	}
	return entries, nil
}

// wmParseLsTreeRecord splits one `ls-tree -z` record,
// `<mode> <type> <oid>\t<name>`, into its name and mode+oid.
func wmParseLsTreeRecord(record string) (string, wmStageEntry, bool) {
	head, name, found := strings.Cut(record, "\t")
	fields := strings.Fields(head)
	if !found || len(fields) != 3 || !ref.ValidSHA(fields[2]) {
		return "", wmStageEntry{}, false
	}
	return name, wmStageEntry{mode: fields[0], oid: fields[2]}, true
}
