// Streaming write endpoints (docs/archive/phases/07-implementation-roadmap.md p4 W2; contract docs/explanation/streaming-commit-protocols.md + docs/reference/http-api.md):
//
//	POST /api/v1/repos/commit-pack    — NDJSON: metadata + blob_chunk frames
//	POST /api/v1/repos/diff-commit    — NDJSON: metadata + diff_chunk frames
//	POST /api/v1/repos/restore-commit — plain JSON
//
// NDJSON bodies are parsed INCREMENTALLY (docs/explanation/streaming-commit-protocols.md: chunked bodies without
// Content-Length, uploads can exceed memory): one bounded line at a time,
// with blob bytes streamed straight into `git hash-object -w --stdin`.
//
// Write pipeline (docs plan decision #5): materialize the cache repo
// (read-only through alternates), create objects in a per-request scratch
// odb, then publish in hook order — pack-objects, blob upload, RegisterPacks,
// CASRef — so a lost CAS race leaves only an unreferenced immutable pack.
//
// Status mapping (plan decision #2): expected_head_sha mismatch -> 412
// precondition_failed; CAS race lost at publish time -> 409 conflict; root
// commits report old_sha = 40 zeros (docs/explanation/domain-model.md invariant 2). Success is 200
// (decision #3). Failures carry the docs/explanation/streaming-commit-protocols.md result envelope; the auth
// middleware's plain {"error"} bodies on 401/403 are SDK-compatible (the
// SDK falls back to the error envelope + numeric status mapping).
//
// Ephemeral handling: `ephemeral`/`ephemeral_base` address refs under
// auth.EphemeralRefName (refs/namespaces/ephemeral/...) in the BASE repo
// storage key — the convention the frozen p3 read surface already uses
// (GET /repos/branches?ephemeral=true, gitread.ResolveRef), so API-written
// ephemeral commits are immediately visible to reads and merge previews.
// Ref policies match against that namespaced name, mirroring the git hook's
// `+ephemeral` quirk.
//
// All helpers carry a wc ("write commit") prefix to stay collision-free
// with sibling slice files in this package. Route wiring: api.New (owned by
// the foundation slice) must call registerWriteCommitRoutes — tests mount
// it on their own mux until that one-line hookup lands.
package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

const (
	// wcMaxChunkBytes is the docs/explanation/streaming-commit-protocols.md chunk cap: 4 MiB of raw bytes before
	// base64.
	wcMaxChunkBytes = 4 << 20
	// wcMaxLineBytes bounds one NDJSON line: 4 MiB raw -> +33% base64 ->
	// ~5.6 MiB plus JSON framing; 8 MiB gives comfortable headroom while
	// keeping memory bounded per request.
	wcMaxLineBytes = 8 << 20
	// wcMaxRestoreBody caps the plain-JSON restore-commit body.
	wcMaxRestoreBody = 1 << 20
	// wcMaxGitlinkBytes caps a 160000 entry payload (a 40-hex commit sha,
	// possibly with trailing whitespace).
	wcMaxGitlinkBytes = 64
)

// registerWriteCommitRoutes mounts the three write-commit endpoints (called
// from api.New). All require a repo-scoped token with git:write (docs/reference/authentication.md
// scope table).
func registerWriteCommitRoutes(mux *http.ServeMux, s *server, verifier auth.Verifier, org string, rl *rateLimiter) {
	assertf(mux != nil && s != nil && verifier != nil, "registerWriteCommitRoutes: nil dependency")
	route := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, rl.limit(routeWrite)(requireAuthWrite(verifier, org, false, auth.ScopeGitWrite)(h)))
	}
	route("POST /api/v1/repos/commit-pack", s.handleCommitPack)
	route("POST /api/v1/repos/diff-commit", s.handleDiffCommit)
	route("POST /api/v1/repos/restore-commit", s.handleRestoreCommit)
}

// ---------------------------------------------------------------------------
// Result envelope (docs/explanation/streaming-commit-protocols.md §3)

// wcCommitInfo is the success-side `commit` object. BlobCount is a pointer:
// commit-pack and diff-commit must emit it (the SDK ack schema requires it
// on both), restore-commit omits it (docs/explanation/streaming-commit-protocols.md §4).
type wcCommitInfo struct {
	CommitSHA    string `json:"commit_sha"`
	TreeSHA      string `json:"tree_sha"`
	TargetBranch string `json:"target_branch"`
	PackBytes    int64  `json:"pack_bytes"`
	BlobCount    *int   `json:"blob_count,omitempty"`
}

// wcResult is the shared `result` object; on failure only Status is
// mandatory (SDK refUpdateResultWithOptionalsSchema), known fields are
// included for context.
type wcResult struct {
	Branch  string `json:"branch,omitempty"`
	OldSHA  string `json:"old_sha,omitempty"`
	NewSHA  string `json:"new_sha,omitempty"`
	Success bool   `json:"success"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type wcEnvelope struct {
	Commit *wcCommitInfo `json:"commit,omitempty"`
	Result wcResult      `json:"result"`
}

// wcFail writes the failure envelope. The status string must agree with the
// docs/explanation/streaming-commit-protocols.md HTTP mapping table (statusForHTTP, errors.go) — enforced here so a
// drifting call site fails fast in tests instead of shipping a mismatched
// pair.
func wcFail(w http.ResponseWriter, httpCode int, status, msg, branch, oldSHA string) {
	assertf(statusForHTTP(httpCode) == status, "wcFail: status %q does not map to HTTP %d", status, httpCode)
	assertf(msg != "", "wcFail: empty message")
	writeJSON(w, httpCode, wcEnvelope{Result: wcResult{
		Branch: branch, OldSHA: oldSHA, Success: false, Status: status, Message: msg,
	}})
}

// wcInternal is internalErr's envelope-flavored twin: log the real error,
// leak nothing (docs/reference/http-api.md: clients branch on codes, bodies stay generic).
func (s *server) wcInternal(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(err != nil && op != "", "wcInternal: nil error or empty op")
	s.logger.Error("internal error", "op", op, "method", r.Method, "path", r.URL.Path, "err", err)
	wcFail(w, http.StatusInternalServerError, "internal", internalErrorMsg, "", "")
}

// ---------------------------------------------------------------------------
// Repo resolution + ref policy

// wcResolveRepo maps the JWT repo claim to its live repo row with envelope
// errors. The claim may carry the repo url or the server-generated repo_id
// (docs/reference/authentication.md: auth accepts both), resolved through the shared
// meta.ResolveRepoKey (ADR-21: repo_id first, then url).
func (s *server) wcResolveRepo(w http.ResponseWriter, r *http.Request) (meta.Repo, bool) {
	claims := mustClaims(r)
	// requireAuth(orgLevel=false) rejects empty/"org" repo claims with 403.
	assertf(claims.Repo != "" && claims.Repo != auth.RepoClaimOrg, "write: repo claim %q passed requireAuth", claims.Repo)
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		wcFail(w, http.StatusNotFound, "not_found", "repository not found", "", "")
		return meta.Repo{}, false
	case err != nil:
		s.wcInternal(w, r, "resolve repository", err)
		return meta.Repo{}, false
	}
	assertf(repo.RepoID != "", "store returned repo with empty RepoID")
	return repo, true
}

// wcRefName maps a branch name to its storage ref name; ephemeral targets
// live under the namespaced prefix in the base repo key (see package doc).
func wcRefName(branch string, ephemeral bool) string {
	assertf(branch != "", "wcRefName: empty branch")
	if ephemeral {
		return auth.EphemeralRefName(branch)
	}
	return "refs/heads/" + branch
}

// wcCheckRefPolicy enforces the token ref policies (docs/reference/authentication.md, invariant 5)
// for one server-created ref update, mirroring the git update hook:
//   - no-push denies every update;
//   - verify-sig denies (server-created commits are unsigned — hook parity);
//   - no-force-push permits our writes: they are fast-forward by
//     construction (the new commit's parent IS the old tip) and never
//     deletes;
//   - unknown ops fail closed.
//
// refName is the storage ref name, already namespaced for ephemeral targets
// (the hook matches auth.EphemeralRefName for `+ephemeral` pushes).
// Writes the 403 envelope and returns false on denial.
func wcCheckRefPolicy(w http.ResponseWriter, claims *auth.Claims, refName string) bool {
	policies, err := refPolicies(claims)
	if err != nil {
		wcFail(w, http.StatusForbidden, "forbidden", err.Error(), "", "")
		return false
	}
	rule, ok := auth.MatchPolicy(policies, refName)
	if !ok {
		return true // unpoliced ref: allowed
	}
	for _, op := range rule.Ops {
		switch op {
		case auth.OpNoPush:
			wcFail(w, http.StatusForbidden, "forbidden", "policy no-push denies update to "+refName, "", "")
			return false
		case auth.OpNoForcePush:
			// Fast-forward by construction (see doc comment): allowed.
		case auth.OpVerifySig:
			wcFail(w, http.StatusForbidden, "forbidden", "policy verify-sig denies unsigned server-created commits on "+refName, "", "")
			return false
		default:
			wcFail(w, http.StatusForbidden, "forbidden", "unknown policy op "+op+" for "+refName, "", "")
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// NDJSON stream reader

// wcLineReader yields one bounded NDJSON line at a time without buffering
// the request body (docs/explanation/streaming-commit-protocols.md: streaming parse). next returns io.EOF at end.
type wcLineReader struct {
	br *bufio.Reader
}

func newWcLineReader(r io.Reader) *wcLineReader {
	assertf(r != nil, "newWcLineReader: nil reader")
	return &wcLineReader{br: bufio.NewReaderSize(r, 64<<10)}
}

// next reads the next line (without the trailing '\n'). Lines above
// wcMaxLineBytes fail instead of buffering unboundedly.
func (lr *wcLineReader) next() ([]byte, error) {
	var line []byte
	for {
		frag, err := lr.br.ReadSlice('\n')
		line = append(line, frag...)
		if len(line) > wcMaxLineBytes {
			return nil, fmt.Errorf("ndjson line exceeds %d bytes", wcMaxLineBytes)
		}
		switch {
		case err == nil:
			return line[:len(line)-1], nil // strip '\n'
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(line) == 0 {
				return nil, io.EOF
			}
			return line, nil // final unterminated line
		default:
			return nil, err
		}
	}
}

// Frame shapes (docs/explanation/streaming-commit-protocols.md §1–2). Exactly one member per line.
type wcAuthor struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type wcFileOp struct {
	Path      string `json:"path"`
	ContentID string `json:"content_id"`
	Operation string `json:"operation"`
	Mode      string `json:"mode"`
}

type wcMetadata struct {
	TargetBranch    string     `json:"target_branch"`
	CommitMessage   string     `json:"commit_message"`
	Author          wcAuthor   `json:"author"`
	Committer       *wcAuthor  `json:"committer"`
	ExpectedHeadSHA string     `json:"expected_head_sha"`
	BaseBranch      string     `json:"base_branch"`
	Ephemeral       bool       `json:"ephemeral"`
	EphemeralBase   bool       `json:"ephemeral_base"`
	Files           []wcFileOp `json:"files"`
	// restore-commit only (docs/explanation/streaming-commit-protocols.md §4).
	TargetCommitSHA string `json:"target_commit_sha"`

	// Current upstream names (ReposCommitsCreateRequest and siblings,
	// issue #378). The SDKs still send the deprecated names above;
	// applyCurrentNames folds these into them, the current name winning.
	ExpectedTargetSHA *string `json:"expected_target_sha"`
	TargetIsEphemeral *bool   `json:"target_is_ephemeral"`
	BaseIsEphemeral   *bool   `json:"base_is_ephemeral"`
	BaseRef           *string `json:"base_ref"` // restore-commit only
}

// applyCurrentNames folds the current upstream field names into the
// deprecated fields the handlers read. A current name wins whenever it is
// present, even when empty or false: "expected_target_sha": "" drops a
// legacy guard, and target_is_ephemeral: false overrides ephemeral: true.
func (md *wcMetadata) applyCurrentNames() {
	assertf(md != nil, "applyCurrentNames: nil metadata")
	if md.ExpectedTargetSHA != nil {
		md.ExpectedHeadSHA = *md.ExpectedTargetSHA
	}
	if md.TargetIsEphemeral != nil {
		md.Ephemeral = *md.TargetIsEphemeral
	}
	if md.BaseIsEphemeral != nil {
		md.EphemeralBase = *md.BaseIsEphemeral
	}
	// Full-SHA guards are case-insensitive (issue #385). base_ref is a
	// revision and stays verbatim: a branch may have a 40-hex name.
	md.ExpectedHeadSHA = lowerFullSHA(md.ExpectedHeadSHA)
	if md.BaseRef != nil {
		md.TargetCommitSHA = *md.BaseRef
	} else {
		md.TargetCommitSHA = lowerFullSHA(md.TargetCommitSHA)
	}
}

type wcBlobChunk struct {
	ContentID string `json:"content_id"`
	Data      string `json:"data"`
	EOF       bool   `json:"eof"`
}

type wcDiffChunk struct {
	Data string `json:"data"`
	EOF  bool   `json:"eof"`
}

type wcFrame struct {
	Metadata  *wcMetadata  `json:"metadata"`
	BlobChunk *wcBlobChunk `json:"blob_chunk"`
	DiffChunk *wcDiffChunk `json:"diff_chunk"`
}

// wcReadMetadata reads and parses the mandatory first frame. Writes the 400
// envelope and returns ok=false on any framing violation.
func wcReadMetadata(w http.ResponseWriter, lr *wcLineReader) (*wcMetadata, bool) {
	line, err := lr.next()
	if err != nil {
		wcFail(w, http.StatusBadRequest, "invalid", "missing metadata frame: "+err.Error(), "", "")
		return nil, false
	}
	var frame wcFrame
	if err := json.Unmarshal(line, &frame); err != nil {
		wcFail(w, http.StatusBadRequest, "invalid", "malformed NDJSON frame: "+err.Error(), "", "")
		return nil, false
	}
	if frame.Metadata == nil || frame.BlobChunk != nil || frame.DiffChunk != nil {
		wcFail(w, http.StatusBadRequest, "invalid", "first frame must be a metadata frame", "", "")
		return nil, false
	}
	frame.Metadata.applyCurrentNames()
	return frame.Metadata, true
}

// wcValidPath rejects the traversal/mangling vectors of docs/explanation/streaming-commit-protocols.md (absolute
// paths, `..`), plus the inputs that would corrupt the update-index stream
// or the repository itself (control bytes, `.git` components, `.` segments,
// empty segments/trailing slash).
func wcValidPath(p string) error {
	if p == "" {
		return errors.New("empty path")
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("absolute path %q", p)
	}
	for _, r := range p {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("control character in path %q", p)
		}
	}
	for seg := range strings.SplitSeq(p, "/") {
		switch {
		case seg == "" || seg == "." || seg == "..":
			return fmt.Errorf("invalid path segment in %q", p)
		case strings.EqualFold(seg, ".git"):
			return fmt.Errorf(".git component in path %q", p)
		}
	}
	return nil
}

var wcValidModes = map[string]bool{"100644": true, "100755": true, "120000": true, "160000": true}

// wcValidateBranchParam validates one branch-name request field (docs/explanation/streaming-commit-protocols.md:
// no refs/ prefix; conservative component rule shared with repo creation).
func wcValidateBranchParam(field, name string) error {
	if name == "" {
		return fmt.Errorf("missing %s", field)
	}
	if strings.HasPrefix(name, "refs/") {
		return fmt.Errorf("%s must not include the refs/ prefix", field)
	}
	if !validBranchName(name) {
		return fmt.Errorf("invalid %s", field)
	}
	return nil
}

// wcValidateAuthor validates one ident. Newlines would corrupt the
// GIT_AUTHOR_*/GIT_COMMITTER_* env-based ident encoding.
func wcValidateAuthor(field string, a wcAuthor) error {
	if a.Name == "" || a.Email == "" {
		return fmt.Errorf("%s name and email are required", field)
	}
	if strings.ContainsAny(a.Name+a.Email, "\n\r") {
		return fmt.Errorf("%s contains newlines", field)
	}
	return nil
}

// wcValidateMetadata checks the fields shared by commit-pack and
// diff-commit (docs/explanation/streaming-commit-protocols.md §1–2). File-op validation is commit-pack-specific
// and handled by the caller.
func wcValidateMetadata(md *wcMetadata) error {
	assertf(md != nil, "wcValidateMetadata: nil metadata")
	if err := wcValidateBranchParam("target_branch", md.TargetBranch); err != nil {
		return err
	}
	if md.CommitMessage == "" {
		return errors.New("missing commit_message")
	}
	if err := wcValidateAuthor("author", md.Author); err != nil {
		return err
	}
	if md.Committer != nil {
		if err := wcValidateAuthor("committer", *md.Committer); err != nil {
			return err
		}
	}
	if md.ExpectedHeadSHA != "" && !ref.ValidSHA(md.ExpectedHeadSHA) {
		return errors.New("expected_target_sha (or expected_head_sha) must be a 40-hex sha")
	}
	if md.BaseBranch != "" {
		if err := wcValidateBranchParam("base_branch", md.BaseBranch); err != nil {
			return err
		}
	}
	return nil
}

// wcValidateFiles checks the commit-pack file-operation table: unique
// paths + content ids, safe paths, known modes/operations.
func wcValidateFiles(files []wcFileOp) error {
	paths := make(map[string]bool, len(files))
	ids := make(map[string]bool, len(files))
	for i := range files {
		f := &files[i]
		if err := wcValidPath(f.Path); err != nil {
			return err
		}
		if paths[f.Path] {
			return fmt.Errorf("duplicate path %q", f.Path)
		}
		paths[f.Path] = true
		if f.ContentID == "" {
			return fmt.Errorf("missing content_id for %q", f.Path)
		}
		if ids[f.ContentID] {
			return fmt.Errorf("duplicate content_id %q", f.ContentID)
		}
		ids[f.ContentID] = true
		switch f.Operation {
		case "upsert":
			if f.Mode == "" {
				f.Mode = "100644" // SDK default when the caller sets none
			}
			if !wcValidModes[f.Mode] {
				return fmt.Errorf("invalid mode %q for %q", f.Mode, f.Path)
			}
		case "delete":
			// Mode ignored; no blob chunks may follow (checked in the
			// stream loop).
		default:
			return fmt.Errorf("invalid operation %q for %q", f.Operation, f.Path)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Commit planning (base/CAS/policy resolution)

// wcPlan is the resolved write: target storage ref, CAS old value, parent
// commit ("" for a root commit), and the ref snapshot used to scope
// pack-objects reachability at publish time.
type wcPlan struct {
	refName string
	oldSHA  string            // current tip, or ref.ZeroSHA for creation
	parent  string            // parent commit sha; "" -> root commit
	tips    map[string]string // all refs at resolve time
}

// wcPlanCommit resolves the docs/explanation/streaming-commit-protocols.md base rules against the ref authority
// and enforces guard + policy, writing the mapped envelope on failure:
//  1. target branch exists -> old = parent = tip;
//  2. unborn + base_branch -> branch from that tip (404 when missing);
//  3. unborn -> root commit (old_sha reported as 40 zeros, invariant 2).
func (s *server) wcPlanCommit(w http.ResponseWriter, r *http.Request, repoID string, md *wcMetadata) (*wcPlan, bool) {
	assertf(repoID != "", "wcPlanCommit: empty repoID")
	tips, err := s.refStore.ListRefs(r.Context(), repoID)
	if err != nil {
		s.wcInternal(w, r, "list refs", err)
		return nil, false
	}
	plan := &wcPlan{refName: wcRefName(md.TargetBranch, md.Ephemeral), tips: tips}
	plan.oldSHA = tips[plan.refName]
	if plan.oldSHA == "" {
		plan.oldSHA = ref.ZeroSHA
	}
	// casTip is what expected_head_sha guards against: the target tip, or —
	// when branching an unborn target from base_branch — the base tip. The
	// vendor SDK's full-workflow conformance drive sends the base branch's
	// head as expected_head_sha when creating a feature branch, so guarding
	// against the (zero) unborn target would 412 every guarded branch
	// creation.
	casTip := plan.oldSHA
	if plan.oldSHA != ref.ZeroSHA {
		plan.parent = plan.oldSHA
	} else if md.BaseBranch != "" {
		baseTip, ok := tips[wcRefName(md.BaseBranch, md.EphemeralBase)]
		if !ok {
			wcFail(w, http.StatusNotFound, "not_found", "base branch "+md.BaseBranch+" not found",
				md.TargetBranch, ref.ZeroSHA)
			return nil, false
		}
		plan.parent = baseTip
		casTip = baseTip
	}
	// CAS guard (decision #2): a stale expected_target_sha (deprecated
	// expected_head_sha) is a 412.
	if md.ExpectedHeadSHA != "" && strings.ToLower(md.ExpectedHeadSHA) != casTip {
		wcFail(w, http.StatusPreconditionFailed, "precondition_failed",
			fmt.Sprintf("expected target sha %s does not match current tip %s", md.ExpectedHeadSHA, casTip),
			md.TargetBranch, plan.oldSHA)
		return nil, false
	}
	if !wcCheckRefPolicy(w, mustClaims(r), plan.refName) {
		return nil, false
	}
	return plan, true
}

// ---------------------------------------------------------------------------
// Scratch odb + git plumbing

// wcScratch is the per-request scratch object directory layered over the
// materialized cache repo (decision #5): git runs with
// GIT_OBJECT_DIRECTORY=<scratch> and the cache odb as a read-only
// alternate, so nothing is persisted unless publish uploads it.
type wcScratch struct {
	repoPath string // materialized bare cache repo
	dir      string // request-scoped scratch (deleted by close)
	baseEnv  []string
}

func wcNewScratch(repoPath string) (*wcScratch, error) {
	assertf(repoPath != "", "wcNewScratch: empty repoPath")
	dir, err := os.MkdirTemp("", "openstorage-write-*")
	if err != nil {
		return nil, err
	}
	objDir := filepath.Join(dir, "objects")
	if err := os.MkdirAll(objDir, 0o755); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	// Hermetic env: the host's GIT_* (identity overrides, config, index)
	// must never leak into server-created commits.
	env := make([]string, 0, 32)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "GIT_") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"GIT_OBJECT_DIRECTORY="+objDir,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES="+filepath.Join(repoPath, "objects"),
		"GIT_INDEX_FILE="+filepath.Join(dir, "index"),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
	)
	return &wcScratch{repoPath: repoPath, dir: dir, baseEnv: env}, nil
}

func (sc *wcScratch) close() { _ = os.RemoveAll(sc.dir) }

// gitCmd builds a git command running in the cache repo with the scratch
// env (plus extra vars), without starting it.
func (sc *wcScratch) gitCmd(ctx context.Context, extraEnv []string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = sc.repoPath
	cmd.Env = append(append([]string{}, sc.baseEnv...), extraEnv...)
	return cmd
}

// git runs one git command to completion, returning stdout; stderr is
// captured separately so warnings never contaminate parsed output.
func (sc *wcScratch) git(ctx context.Context, stdin io.Reader, extraEnv []string, args ...string) (string, error) {
	if gitread.BudgetFrom(ctx) != nil {
		// A git:read request (merge preview): run under its read budget
		// (#313). Write requests carry no budget and keep the plain path.
		return gitread.Run(ctx, gitread.Cmd{
			Class: gitread.Heavy, Dir: sc.repoPath, Env: append(append([]string{}, sc.baseEnv...), extraEnv...),
			Stdin: stdin, Args: args,
		})
	}
	cmd := sc.gitCmd(ctx, extraEnv, args...)
	cmd.Stdin = stdin
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := gitlimit.Output(cmd)
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// resolveTree resolves a commit sha to its tree sha.
func (sc *wcScratch) resolveTree(ctx context.Context, commitSHA string) (string, error) {
	assertf(ref.ValidSHA(commitSHA), "resolveTree: malformed sha %q", commitSHA)
	out, err := sc.git(ctx, nil, nil, "rev-parse", "--verify", "--quiet", "--end-of-options", commitSHA+"^{tree}")
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(out)
	if !ref.ValidSHA(sha) {
		return "", fmt.Errorf("rev-parse emitted %q", sha)
	}
	return sha, nil
}

// wcBlobSink streams one file's decoded bytes into `git hash-object -w
// --stdin` (blob created in the scratch odb) without buffering the content.
type wcBlobSink struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func (sc *wcScratch) startBlobSink(ctx context.Context) (*wcBlobSink, error) {
	sink := &wcBlobSink{cmd: sc.gitCmd(ctx, nil, "hash-object", "-w", "--stdin")}
	sink.cmd.Stdout = &sink.stdout
	sink.cmd.Stderr = &sink.stderr
	stdin, err := sink.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	sink.stdin = stdin
	if err := gitlimit.Start(sink.cmd); err != nil {
		return nil, err
	}
	return sink, nil
}

func (b *wcBlobSink) write(p []byte) error {
	_, err := b.stdin.Write(p)
	return err
}

// finish closes stdin and returns the written blob's sha.
func (b *wcBlobSink) finish() (string, error) {
	if err := b.stdin.Close(); err != nil {
		return "", err
	}
	if err := gitlimit.Wait(b.cmd); err != nil {
		return "", fmt.Errorf("hash-object: %w: %s", err, strings.TrimSpace(b.stderr.String()))
	}
	sha := strings.TrimSpace(b.stdout.String())
	if !ref.ValidSHA(sha) {
		return "", fmt.Errorf("hash-object emitted %q", sha)
	}
	return sha, nil
}

// abort tears down an unfinished sink (early handler exit).
func (b *wcBlobSink) abort() {
	_ = b.stdin.Close()
	if b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	_ = gitlimit.Wait(b.cmd)
}

// wcIndexEntry is one tree operation: Delete when SHA is empty.
type wcIndexEntry struct {
	Path string
	Mode string // "" for deletes
	SHA  string // "" for deletes
}

// buildTree loads baseTree ("" = empty tree) into the request-scoped temp
// index, applies the entries via `update-index -z --index-info`, and
// returns the new tree sha. Deletions of paths absent from the base are
// no-ops (git semantics).
func (sc *wcScratch) buildTree(ctx context.Context, baseTree string, entries []wcIndexEntry) (string, error) {
	if baseTree == "" {
		if _, err := sc.git(ctx, nil, nil, "read-tree", "--empty"); err != nil {
			return "", err
		}
	} else {
		if _, err := sc.git(ctx, nil, nil, "read-tree", baseTree); err != nil {
			return "", err
		}
	}
	if len(entries) > 0 {
		var b strings.Builder
		for _, e := range entries {
			if e.SHA == "" {
				fmt.Fprintf(&b, "0 %s\t%s\x00", ref.ZeroSHA, e.Path)
				continue
			}
			assertf(wcValidModes[e.Mode], "buildTree: invalid mode %q", e.Mode)
			assertf(ref.ValidSHA(e.SHA), "buildTree: malformed sha %q", e.SHA)
			fmt.Fprintf(&b, "%s %s\t%s\x00", e.Mode, e.SHA, e.Path)
		}
		if _, err := sc.git(ctx, strings.NewReader(b.String()), nil, "update-index", "-z", "--index-info"); err != nil {
			return "", err
		}
	}
	out, err := sc.git(ctx, nil, nil, "write-tree")
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(out)
	if !ref.ValidSHA(sha) {
		return "", fmt.Errorf("write-tree emitted %q", sha)
	}
	return sha, nil
}

// commitTree creates the commit object in the scratch odb. parent "" makes
// a root commit. Committer defaults to the author (docs/explanation/streaming-commit-protocols.md).
func (sc *wcScratch) commitTree(ctx context.Context, tree, parent string, md *wcMetadata) (string, error) {
	assertf(ref.ValidSHA(tree), "commitTree: malformed tree %q", tree)
	committer := md.Author
	if md.Committer != nil {
		committer = *md.Committer
	}
	env := []string{
		"GIT_AUTHOR_NAME=" + md.Author.Name,
		"GIT_AUTHOR_EMAIL=" + md.Author.Email,
		"GIT_COMMITTER_NAME=" + committer.Name,
		"GIT_COMMITTER_EMAIL=" + committer.Email,
	}
	args := []string{"commit-tree", "-m", md.CommitMessage}
	if parent != "" {
		assertf(ref.ValidSHA(parent), "commitTree: malformed parent %q", parent)
		args = append(args, "-p", parent)
	}
	args = append(args, tree)
	out, err := sc.git(ctx, nil, env, args...)
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(out)
	if !ref.ValidSHA(sha) {
		return "", fmt.Errorf("commit-tree emitted %q", sha)
	}
	return sha, nil
}

// ---------------------------------------------------------------------------
// Publish: pack + upload + register + CAS (hook crash-ordering)

// wcPublish packs everything reachable from newSHA but not from any
// pre-existing tip, uploads it, registers it, and CASes the ref. Returns
// won=false when the ref moved concurrently (the caller maps it to 409);
// the uploaded pack is then merely unreferenced — same crash window as the
// push hooks, safe by construction.
func (s *server) wcPublish(ctx context.Context, sc *wcScratch, repoID string, plan *wcPlan, newSHA string) (packBytes int64, won bool, err error) {
	assertf(ref.ValidSHA(newSHA), "wcPublish: malformed new sha %q", newSHA)
	var revs strings.Builder
	revs.WriteString(newSHA + "\n")
	seen := map[string]bool{}
	for _, tip := range plan.tips {
		if !seen[tip] {
			seen[tip] = true
			revs.WriteString("^" + tip + "\n")
		}
	}
	base := filepath.Join(sc.dir, "pub")
	out, err := sc.git(ctx, strings.NewReader(revs.String()), nil, "pack-objects", "--revs", "-q", base)
	if err != nil {
		return 0, false, fmt.Errorf("pack-objects: %w", err)
	}
	packID := strings.TrimSpace(out)
	// A fresh commit object is always in the pack, so exactly one
	// non-empty pack id must come back.
	if packID == "" || strings.ContainsAny(packID, "\n/") {
		return 0, false, fmt.Errorf("pack-objects emitted %q", out)
	}
	packPath := base + "-" + packID + ".pack"
	info, err := os.Stat(packPath)
	if err != nil {
		return 0, false, fmt.Errorf("stat pack: %w", err)
	}
	// Dedup + quota + upload + register + verify + repack trigger via the
	// shared REST publication step (publish.go); quota denials store
	// nothing and surface as errPackQuotaExceeded.
	if err := s.publishPack(ctx, repoID, packID, packPath); err != nil {
		return 0, false, err
	}
	won, err = s.refStore.CASRef(ctx, repoID, plan.refName, plan.oldSHA, newSHA)
	if err != nil {
		return 0, false, fmt.Errorf("cas %s: %w", plan.refName, err)
	}
	return info.Size(), won, nil
}

// wcFinishCommit runs the shared tail of all three handlers: publish, map
// the CAS outcome, and write the 200 envelope.
func (s *server) wcFinishCommit(w http.ResponseWriter, r *http.Request, sc *wcScratch, repoID string, plan *wcPlan, md *wcMetadata, tree, commit string, blobCount *int) {
	packBytes, won, err := s.wcPublish(r.Context(), sc, repoID, plan, commit)
	if errors.Is(err, errPackQuotaExceeded) {
		// md.TargetBranch is the request's bare branch name — the failure
		// envelope must match the success path's branch identity, not the
		// internal (possibly namespaced) ref name (Codex round 12).
		wcFail(w, http.StatusForbidden, "forbidden", err.Error(), md.TargetBranch, plan.oldSHA)
		return
	}
	if errors.Is(err, errPackReservationsBusy) {
		// Transient row-cap denial (Codex round 43): retryable.
		rateLimited(w, 10)
		wcFail(w, http.StatusTooManyRequests, "unavailable", err.Error(), md.TargetBranch, plan.oldSHA)
		return
	}
	if err != nil {
		s.wcInternal(w, r, "publish commit", err)
		return
	}
	if !won {
		// Guard already validated (or absent): losing the CAS here means a
		// concurrent writer moved the ref in the race window -> 409
		// (decision #2).
		wcFail(w, http.StatusConflict, "conflict",
			"ref "+plan.refName+" moved concurrently", md.TargetBranch, plan.oldSHA)
		return
	}
	writeJSON(w, http.StatusOK, wcEnvelope{
		Commit: &wcCommitInfo{
			CommitSHA:    commit,
			TreeSHA:      tree,
			TargetBranch: md.TargetBranch,
			PackBytes:    packBytes,
			BlobCount:    blobCount,
		},
		Result: wcResult{
			Branch:  md.TargetBranch,
			OldSHA:  plan.oldSHA,
			NewSHA:  commit,
			Success: true,
			Status:  "ok",
			Message: "commit created",
		},
	})
}

// ---------------------------------------------------------------------------
// POST /api/v1/repos/commit-pack

// handleCommitPack implements the docs/explanation/streaming-commit-protocols.md §1 NDJSON commit: metadata frame,
// then contiguous base64 blob_chunk frames per upsert file terminated by
// eof:true, streamed into scratch blobs, composed into a tree over the base
// and published under CAS.
func (s *server) handleCommitPack(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.wcResolveRepo(w, r)
	if !ok {
		return
	}
	lr := newWcLineReader(r.Body)
	md, ok := wcReadMetadata(w, lr)
	if !ok {
		return
	}
	if err := wcValidateMetadata(md); err != nil {
		wcFail(w, http.StatusBadRequest, "invalid", err.Error(), "", "")
		return
	}
	if err := wcValidateFiles(md.Files); err != nil {
		wcFail(w, http.StatusBadRequest, "invalid", err.Error(), "", "")
		return
	}
	plan, ok := s.wcPlanCommit(w, r, repo.RepoID, md)
	if !ok {
		return
	}
	repoPath, err := gitread.Materialize(r.Context(), s.refStore, s.blobStore, s.cacheDir, repo.RepoID, repo.DefaultBranch)
	if err != nil {
		s.wcInternal(w, r, "materialize repository", err)
		return
	}
	sc, err := wcNewScratch(repoPath)
	if err != nil {
		s.wcInternal(w, r, "create scratch", err)
		return
	}
	defer sc.close()

	blobSHAs, ok := s.wcStreamBlobs(w, r, lr, sc, md.Files)
	if !ok {
		return
	}

	baseTree := ""
	if plan.parent != "" {
		baseTree, err = sc.resolveTree(r.Context(), plan.parent)
		if err != nil {
			s.wcInternal(w, r, "resolve base tree", err)
			return
		}
	}
	entries := make([]wcIndexEntry, 0, len(md.Files))
	blobCount := 0
	for _, f := range md.Files {
		if f.Operation == "delete" {
			entries = append(entries, wcIndexEntry{Path: f.Path})
			continue
		}
		sha, ok := blobSHAs[f.ContentID]
		assertf(ok, "streamed upsert %q missing blob sha", f.ContentID)
		entries = append(entries, wcIndexEntry{Path: f.Path, Mode: f.Mode, SHA: sha})
		blobCount++
	}
	tree, err := sc.buildTree(r.Context(), baseTree, entries)
	if err != nil {
		s.wcInternal(w, r, "build tree", err)
		return
	}
	commit, err := sc.commitTree(r.Context(), tree, plan.parent, md)
	if err != nil {
		s.wcInternal(w, r, "create commit", err)
		return
	}
	s.wcFinishCommit(w, r, sc, repo.RepoID, plan, md, tree, commit, &blobCount)
}

// wcStreamBlobs drives the blob_chunk state machine (docs/explanation/streaming-commit-protocols.md §1): chunks
// per file are contiguous and eof-terminated; every upsert must be fed;
// deletes must not be; decoded chunks are capped at 4 MiB. Content bytes
// stream straight into hash-object — except 160000 gitlinks, whose payload
// is the referenced commit sha in hex (they have no blob). Returns
// content_id -> object sha. On failure the 4xx envelope is written.
func (s *server) wcStreamBlobs(w http.ResponseWriter, r *http.Request, lr *wcLineReader, sc *wcScratch, files []wcFileOp) (map[string]string, bool) {
	type blobState struct {
		op      *wcFileOp
		sink    *wcBlobSink // nil for gitlinks
		gitlink []byte      // 160000 payload accumulator
		started bool
		done    bool
		sha     string
	}
	states := make(map[string]*blobState)
	for i := range files {
		if files[i].Operation == "upsert" {
			states[files[i].ContentID] = &blobState{op: &files[i]}
		}
	}
	var open *blobState // the file currently receiving chunks
	fail := func(code int, msg string) (map[string]string, bool) {
		if open != nil && open.sink != nil {
			open.sink.abort()
		}
		wcFail(w, code, "invalid", msg, "", "")
		return nil, false
	}

	for {
		line, err := lr.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fail(http.StatusBadRequest, err.Error())
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame wcFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			return fail(http.StatusBadRequest, "malformed NDJSON frame: "+err.Error())
		}
		switch {
		case frame.BlobChunk != nil:
			ch := frame.BlobChunk
			st, ok := states[ch.ContentID]
			if !ok {
				return fail(http.StatusBadRequest, "blob_chunk for unknown or non-upsert content_id "+ch.ContentID)
			}
			if st.done {
				return fail(http.StatusBadRequest, "blob_chunk after eof for content_id "+ch.ContentID)
			}
			if open != nil && open != st {
				return fail(http.StatusBadRequest, "interleaved blob_chunk for content_id "+ch.ContentID)
			}
			data, err := base64.StdEncoding.DecodeString(ch.Data)
			if err != nil {
				return fail(http.StatusBadRequest, "malformed base64 for content_id "+ch.ContentID)
			}
			if len(data) > wcMaxChunkBytes {
				return fail(http.StatusBadRequest, fmt.Sprintf("chunk exceeds %d bytes for content_id %s", wcMaxChunkBytes, ch.ContentID))
			}
			if !st.started {
				st.started = true
				open = st
				if st.op.Mode != "160000" {
					sink, err := sc.startBlobSink(r.Context())
					if err != nil {
						s.wcInternal(w, r, "start blob sink", err)
						return nil, false
					}
					st.sink = sink
				}
			}
			if st.op.Mode == "160000" {
				st.gitlink = append(st.gitlink, data...)
				if len(st.gitlink) > wcMaxGitlinkBytes {
					return fail(http.StatusBadRequest, "gitlink payload for "+st.op.Path+" exceeds a commit sha")
				}
			} else if err := st.sink.write(data); err != nil {
				st.sink.abort()
				open = nil
				return fail(http.StatusBadRequest, "write blob: "+err.Error())
			}
			if ch.EOF {
				if st.op.Mode == "160000" {
					sha := strings.TrimSpace(string(st.gitlink))
					if !ref.ValidSHA(sha) {
						return fail(http.StatusBadRequest, "gitlink payload for "+st.op.Path+" must be a 40-hex commit sha")
					}
					st.sha = sha
				} else {
					sha, err := st.sink.finish()
					st.sink = nil
					if err != nil {
						open = nil
						return fail(http.StatusBadRequest, "hash blob: "+err.Error())
					}
					st.sha = sha
				}
				st.done = true
				open = nil
			}
		case frame.Metadata != nil:
			return fail(http.StatusBadRequest, "duplicate metadata frame")
		case frame.DiffChunk != nil:
			return fail(http.StatusBadRequest, "diff_chunk frame not allowed on commit-pack")
		default:
			return fail(http.StatusBadRequest, "unrecognized NDJSON frame")
		}
	}
	if open != nil {
		return fail(http.StatusBadRequest, "stream ended before eof for content_id "+open.op.ContentID)
	}
	shas := make(map[string]string, len(states))
	for id, st := range states {
		if !st.done {
			return fail(http.StatusBadRequest, "no blob chunks received for content_id "+id)
		}
		shas[id] = st.sha
	}
	return shas, true
}

// ---------------------------------------------------------------------------
// POST /api/v1/repos/diff-commit

// handleDiffCommit implements docs/explanation/streaming-commit-protocols.md §2: same metadata frame (no files),
// diff_chunk frames spooled to a scratch file and applied to the base tree
// with `git apply --cached`. An empty diff yields an empty commit; a patch
// that does not apply is `invalid` (422).
func (s *server) handleDiffCommit(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.wcResolveRepo(w, r)
	if !ok {
		return
	}
	lr := newWcLineReader(r.Body)
	md, ok := wcReadMetadata(w, lr)
	if !ok {
		return
	}
	if err := wcValidateMetadata(md); err != nil {
		wcFail(w, http.StatusBadRequest, "invalid", err.Error(), "", "")
		return
	}
	if len(md.Files) > 0 {
		wcFail(w, http.StatusBadRequest, "invalid", "diff-commit metadata must not carry files", "", "")
		return
	}
	plan, ok := s.wcPlanCommit(w, r, repo.RepoID, md)
	if !ok {
		return
	}
	repoPath, err := gitread.Materialize(r.Context(), s.refStore, s.blobStore, s.cacheDir, repo.RepoID, repo.DefaultBranch)
	if err != nil {
		s.wcInternal(w, r, "materialize repository", err)
		return
	}
	sc, err := wcNewScratch(repoPath)
	if err != nil {
		s.wcInternal(w, r, "create scratch", err)
		return
	}
	defer sc.close()

	patchPath, patchBytes, ok := s.wcSpoolDiff(w, lr, sc)
	if !ok {
		return
	}

	baseTree := ""
	if plan.parent != "" {
		baseTree, err = sc.resolveTree(r.Context(), plan.parent)
		if err != nil {
			s.wcInternal(w, r, "resolve base tree", err)
			return
		}
	}
	// Load the base into the temp index, then let git apply the patch
	// against it (blob content resolved through the alternate odb —
	// binary deltas included).
	if _, err := sc.buildTree(r.Context(), baseTree, nil); err != nil {
		s.wcInternal(w, r, "load base tree", err)
		return
	}
	blobCount := 0
	if patchBytes > 0 {
		if _, err := sc.git(r.Context(), nil, nil, "apply", "--cached", "--binary", "--whitespace=nowarn", patchPath); err != nil {
			// The patch is client input: rejection is an envelope, never
			// a 500 (docs/explanation/streaming-commit-protocols.md maps 422 -> invalid).
			wcFail(w, http.StatusUnprocessableEntity, "invalid", "patch does not apply: "+err.Error(),
				md.TargetBranch, plan.oldSHA)
			return
		}
		// blob_count: the SDK ack schema requires a number on diff-commit
		// too; report the count of files the patch touches (one numstat
		// line per file).
		numstat, err := sc.git(r.Context(), nil, nil, "apply", "--numstat", patchPath)
		if err != nil {
			s.wcInternal(w, r, "numstat patch", err)
			return
		}
		for ln := range strings.SplitSeq(numstat, "\n") {
			if strings.TrimSpace(ln) != "" {
				blobCount++
			}
		}
	}
	tree, err := sc.git(r.Context(), nil, nil, "write-tree")
	if err != nil {
		s.wcInternal(w, r, "write tree", err)
		return
	}
	treeSHA := strings.TrimSpace(tree)
	if !ref.ValidSHA(treeSHA) {
		s.wcInternal(w, r, "write tree", fmt.Errorf("write-tree emitted %q", tree))
		return
	}
	commit, err := sc.commitTree(r.Context(), treeSHA, plan.parent, md)
	if err != nil {
		s.wcInternal(w, r, "create commit", err)
		return
	}
	s.wcFinishCommit(w, r, sc, repo.RepoID, plan, md, treeSHA, commit, &blobCount)
}

// wcSpoolDiff reads diff_chunk frames until eof:true, appending decoded
// bytes to a scratch file (patches can be large; never buffered in memory).
// Returns the spooled path and byte count; on failure the 400 envelope is
// written.
func (s *server) wcSpoolDiff(w http.ResponseWriter, lr *wcLineReader, sc *wcScratch) (string, int64, bool) {
	path := filepath.Join(sc.dir, "patch.diff")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		wcFail(w, http.StatusInternalServerError, "internal", internalErrorMsg, "", "")
		return "", 0, false
	}
	defer f.Close()
	var total int64
	sawEOF := false
	for !sawEOF {
		line, err := lr.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			wcFail(w, http.StatusBadRequest, "invalid", err.Error(), "", "")
			return "", 0, false
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame wcFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			wcFail(w, http.StatusBadRequest, "invalid", "malformed NDJSON frame: "+err.Error(), "", "")
			return "", 0, false
		}
		if frame.DiffChunk == nil || frame.Metadata != nil || frame.BlobChunk != nil {
			wcFail(w, http.StatusBadRequest, "invalid", "diff-commit accepts only diff_chunk frames after metadata", "", "")
			return "", 0, false
		}
		data, err := base64.StdEncoding.DecodeString(frame.DiffChunk.Data)
		if err != nil {
			wcFail(w, http.StatusBadRequest, "invalid", "malformed base64 diff chunk", "", "")
			return "", 0, false
		}
		if len(data) > wcMaxChunkBytes {
			wcFail(w, http.StatusBadRequest, "invalid", fmt.Sprintf("diff chunk exceeds %d bytes", wcMaxChunkBytes), "", "")
			return "", 0, false
		}
		if _, err := f.Write(data); err != nil {
			wcFail(w, http.StatusInternalServerError, "internal", internalErrorMsg, "", "")
			return "", 0, false
		}
		total += int64(len(data))
		sawEOF = frame.DiffChunk.EOF
	}
	if !sawEOF {
		wcFail(w, http.StatusBadRequest, "invalid", "stream ended before diff_chunk eof", "", "")
		return "", 0, false
	}
	if err := f.Close(); err != nil {
		wcFail(w, http.StatusInternalServerError, "internal", internalErrorMsg, "", "")
		return "", 0, false
	}
	return path, total, true
}

// ---------------------------------------------------------------------------
// POST /api/v1/repos/restore-commit

// wcRestoreBody is the plain-JSON restore request (docs/explanation/streaming-commit-protocols.md §4; the openapi
// NDJSON/`reset-commits` variant is a recorded deviation, docs/archive/phases/12-phase3-implementation-notes.md).
type wcRestoreBody struct {
	Metadata *wcMetadata `json:"metadata"`
}

// handleRestoreCommit creates a NEW commit whose tree equals
// target_commit_sha's tree, parented on the current tip — history is
// preserved, never reset. The target branch must exist (a restore on an
// unborn branch has no tip to parent on -> 404).
func (s *server) handleRestoreCommit(w http.ResponseWriter, r *http.Request) {
	repo, ok := s.wcResolveRepo(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, wcMaxRestoreBody))
	if err != nil {
		wcFail(w, http.StatusBadRequest, "invalid", "read request body: "+err.Error(), "", "")
		return
	}
	var req wcRestoreBody
	if err := json.Unmarshal(body, &req); err != nil || req.Metadata == nil {
		wcFail(w, http.StatusBadRequest, "invalid", "body must be {\"metadata\": {...}}", "", "")
		return
	}
	md := req.Metadata
	// base_ref (the current name, issue #378) names a branch, tag, or
	// commit; the deprecated target_commit_sha stays a 40-hex sha.
	revision := md.BaseRef != nil
	md.applyCurrentNames()
	if md.CommitMessage == "" {
		md.CommitMessage = "Restore to " + md.TargetCommitSHA
	}
	if err := wcValidateMetadata(md); err != nil {
		wcFail(w, http.StatusBadRequest, "invalid", err.Error(), "", "")
		return
	}
	if revision && md.TargetCommitSHA == "" {
		wcFail(w, http.StatusBadRequest, "invalid", "base_ref must not be empty", "", "")
		return
	}
	if !revision && !ref.ValidSHA(md.TargetCommitSHA) {
		wcFail(w, http.StatusBadRequest, "invalid", "base_ref is required (or the deprecated 40-hex target_commit_sha)", "", "")
		return
	}
	plan, ok := s.wcPlanCommit(w, r, repo.RepoID, md)
	if !ok {
		return
	}
	if plan.oldSHA == ref.ZeroSHA {
		wcFail(w, http.StatusNotFound, "not_found", "target branch "+md.TargetBranch+" not found",
			md.TargetBranch, ref.ZeroSHA)
		return
	}
	repoPath, err := gitread.Materialize(r.Context(), s.refStore, s.blobStore, s.cacheDir, repo.RepoID, repo.DefaultBranch)
	if err != nil {
		s.wcInternal(w, r, "materialize repository", err)
		return
	}
	sc, err := wcNewScratch(repoPath)
	if err != nil {
		s.wcInternal(w, r, "create scratch", err)
		return
	}
	defer sc.close()

	target := md.TargetCommitSHA
	if revision {
		// A base_ref revision resolves like other revision inputs
		// (gitread.ResolveRef: branch, tag — peeled — then hex), even when
		// it is 40 hex characters: a branch may have such a name.
		resolved, err := gitread.ResolveRef(r.Context(), repoPath, target, false)
		if errors.Is(err, gitread.ErrNotFound) {
			wcFail(w, http.StatusNotFound, "not_found", "base_ref "+target+" does not resolve to a commit",
				md.TargetBranch, plan.oldSHA)
			return
		} else if err != nil {
			s.wcInternal(w, r, "resolve restore base_ref", err)
			return
		}
		target = resolved
	}
	// The restore target must exist and be a commit; its tree becomes the
	// new commit's tree verbatim.
	tree, err := sc.resolveTree(r.Context(), target)
	if err != nil {
		wcFail(w, http.StatusNotFound, "not_found", "restore target "+target+" not found",
			md.TargetBranch, plan.oldSHA)
		return
	}
	if revision && !s.wcRestoreMoves(w, r, sc, md, plan, target, tree) {
		return
	}
	commit, err := sc.commitTree(r.Context(), tree, plan.parent, md)
	if err != nil {
		s.wcInternal(w, r, "create commit", err)
		return
	}
	s.wcFinishCommit(w, r, sc, repo.RepoID, plan, md, tree, commit, nil)
}

// wcRestoreMoves enforces the spec's restore preconditions for the base_ref
// form (412 precondition_failed): the branch must not already point at
// base_ref, the restore must change the tree, and base_ref must be an
// ancestor of the current tip. The deprecated target_commit_sha form keeps
// its unchecked behavior, which the SDKs rely on (recorded deviation,
// issue #378). Writes the failure envelope and returns false on rejection.
func (s *server) wcRestoreMoves(w http.ResponseWriter, r *http.Request, sc *wcScratch, md *wcMetadata, plan *wcPlan, target, tree string) bool {
	assertf(ref.ValidSHA(target) && plan.oldSHA != ref.ZeroSHA, "wcRestoreMoves: target %q tip %q", target, plan.oldSHA)
	fail := func(msg string) bool {
		wcFail(w, http.StatusPreconditionFailed, "precondition_failed", msg, md.TargetBranch, plan.oldSHA)
		return false
	}
	if target == plan.oldSHA {
		return fail("branch " + md.TargetBranch + " already points at base_ref")
	}
	tipTree, err := sc.resolveTree(r.Context(), plan.oldSHA)
	if err != nil {
		s.wcInternal(w, r, "resolve restore tip tree", err)
		return false
	}
	if tipTree == tree {
		return fail("restore to base_ref would produce no change")
	}
	ok, err := wmIsAncestor(r.Context(), sc, target, plan.oldSHA)
	if err != nil {
		s.wcInternal(w, r, "check restore ancestry", err)
		return false
	}
	if !ok {
		return fail("base_ref is not an ancestor of " + md.TargetBranch)
	}
	return true
}
