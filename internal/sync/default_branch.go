package sync

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// DefaultBranchAdopter is an UpstreamStore that records the upstream
// default branch of a repo whose create named none (issue #599): it sets
// the branch and clears Upstream.DefaultBranchOmitted in one write, only
// while the flag is set. ErrNotFound when the repo is deleted. The meta
// adapter implements it. MemStore never stores the flag.
type DefaultBranchAdopter interface {
	AdoptDefaultBranch(ctx context.Context, repoID, branch string) (adopted bool, err error)
}

// DefaultBranchEvidence is an UpstreamStore that keeps, per flagged repo,
// the success count at which a worker with the adoption rule last handled
// it (issue #609). The reconcile job re-requests a sync only for a repo
// whose success count passed that evidence, which only a worker without the
// rule can cause. The meta adapter implements it, and only PostgreSQL
// stores the evidence: SQLite and S3WAL run one process, so no older worker
// can sync beside it. MemStore does not implement it.
type DefaultBranchEvidence interface {
	RecordDefaultBranchChecked(ctx context.Context, repoID string, successCount int) error
}

// lsRemoteOutputCap bounds the captured `ls-remote --symref <url> HEAD`
// output. The HEAD pattern also matches every upstream ref ending in
// /HEAD, so the upstream controls the line count. git prints the HEAD
// symref line first, so the capped prefix keeps it.
const lsRemoteOutputCap = 64 << 10

// headLookupTimeout bounds the upstream HEAD lookup; it gets at most a
// quarter of the time left in the run.
const headLookupTimeout = 30 * time.Second

// cappedBuffer keeps the first n bytes written and discards the rest,
// so the writer never blocks or fails the child.
type cappedBuffer struct {
	buf []byte
	n   int
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if room := c.n - len(c.buf); room > 0 {
		c.buf = append(c.buf, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

// upstreamHead resolves the upstream HEAD branch for a repo whose create
// named no default branch. It returns "" and logs a WARN line when the
// HEAD cannot be used: the repo keeps its branch, the flag stays set, and
// a later sync retries.
func (w *Worker) upstreamHead(ctx context.Context, job Job, mirror string, env []string, url string) string {
	// The lookup is optional: its own deadline keeps a stalled upstream
	// from spending the run budget that the push still needs.
	budget := headLookupTimeout
	if dl, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(dl)/4)
	}
	lctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	out := &cappedBuffer{n: lsRemoteOutputCap}
	stderr, err := runGitTo(lctx, mirror, env, out, "ls-remote", "--symref", url, "HEAD")
	var branch, reason string
	if err != nil {
		reason = fmt.Sprintf("git ls-remote: %v: %s", err, strings.TrimSpace(stderr))
	} else {
		branch, reason = parseHeadSymref(string(out.buf))
	}
	if reason == "" {
		// The adopted branch must exist: the loopback push mirrors every
		// fetched head, so a fetched branch is a local branch.
		if _, err := runGit(ctx, mirror, hermeticGitEnv(w.allowProtocol()),
			"rev-parse", "--verify", "--quiet", "refs/heads/"+branch+"^{commit}"); err != nil {
			reason = "the upstream HEAD names branch " + strconv.Quote(branch) + ", which the fetch did not return"
		}
	}
	if reason != "" {
		w.Logger.Warn("upstream default branch not resolved; the repo keeps its default branch until a later sync resolves it",
			"org", job.Org, "repo", job.RepoID, "reason", reason)
		return ""
	}
	return branch
}

// parseHeadSymref reads the HEAD symref line of `git ls-remote --symref`
// output ("ref: refs/heads/<branch>\tHEAD"). A non-empty reason says why
// no default branch can come from it.
func parseHeadSymref(out string) (branch, reason string) {
	for line := range strings.SplitSeq(out, "\n") {
		rest, ok := strings.CutPrefix(line, "ref: ")
		if !ok {
			continue
		}
		target, name, ok := strings.Cut(rest, "\t")
		if !ok || name != "HEAD" {
			continue
		}
		b, ok := strings.CutPrefix(target, "refs/heads/")
		if !ok {
			return "", "the upstream HEAD names " + strconv.Quote(target) + ", not a branch"
		}
		if !ValidBranchName(b) {
			return "", "the upstream HEAD names branch " + strconv.Quote(b) + ", which is not a valid default branch"
		}
		return b, ""
	}
	return "", "the upstream advertises no HEAD branch"
}

// adoptHead records branch (non-empty) as the repo's default branch. It
// runs before the run is finished, so a repo.sync.succeeded consumer reads
// the adopted branch. A failed write keeps the flag for a later sync.
func (w *Worker) adoptHead(ctx context.Context, job Job, branch string) {
	if branch == "" {
		return
	}
	a, ok := w.Upstreams.(DefaultBranchAdopter)
	assertf(ok, "Worker.Upstreams %T returned a DefaultBranchOmitted row but cannot adopt", w.Upstreams)
	adopted, err := a.AdoptDefaultBranch(ctx, job.RepoID, branch)
	switch {
	case errors.Is(err, ErrNotFound): // deleted mid-run: nothing to record
	case err != nil:
		w.Logger.Warn("record upstream default branch failed; a later sync retries",
			"org", job.Org, "repo", job.RepoID, "branch", branch, "err", err)
	case adopted:
		w.Logger.Info("adopted the upstream default branch", "org", job.Org, "repo", job.RepoID, "branch", branch)
	}
}

// recordHeadChecked notes that this worker handled a flagged repo at the
// given success count, whether or not the run succeeded, so the reconcile
// job asks for one more sync only after a worker without the rule runs one.
// A repo that this worker could not resolve keeps the documented rule: the
// next successful sync tries again. A failed write is logged, and the cost
// is one extra sync.
func (w *Worker) recordHeadChecked(ctx context.Context, job Job, successCount int) {
	e, ok := w.Upstreams.(DefaultBranchEvidence)
	if !ok {
		return
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
	defer cancel()
	if err := e.RecordDefaultBranchChecked(rctx, job.RepoID, successCount); err != nil && !errors.Is(err, ErrNotFound) {
		w.Logger.Warn("record default branch check failed; the reconcile job may request one more sync",
			"org", job.Org, "repo", job.RepoID, "err", err)
	}
}

// branchComponentRe and ValidBranchName mirror the API's validBranchName
// (internal/api/handlers.go, which this package cannot import): a stored
// default branch drives `symbolic-ref HEAD` on every hydrate, so the
// adopted branch must pass the same check as a requested one.
// TestValidBranchNameMatchesAPI pins the two together.
var branchComponentRe = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]*\z`)

// ValidBranchName reports whether name is an acceptable default branch.
func ValidBranchName(name string) bool {
	if name == "" || len(name) > 255 || strings.Contains(name, "..") {
		return false
	}
	for comp := range strings.SplitSeq(name, "/") {
		if !branchComponentRe.MatchString(comp) ||
			strings.HasSuffix(comp, ".lock") || strings.HasSuffix(comp, ".") {
			return false
		}
	}
	return true
}
