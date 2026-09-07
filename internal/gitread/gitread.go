// Package gitread is the read-side git plumbing layer for the /api/v1
// git:read endpoints (docs/reference/http-api.md): ref resolution, ls-tree, log, diff, blame,
// grep, and tar.gz archive streaming over a hydrated bare cache repo.
//
// FROZEN CONTRACT: the exported types and function signatures are the shared
// surface the phase-3 endpoint slices build on. gitread stays HTTP-agnostic
// (no status codes, no wire envelopes): handlers own parameter validation,
// cursoring, and response shaping. All plumbing is `os/exec git` over a
// repoPath produced by Materialize — consistent with the rest of the repo,
// no go-git.
//
// Error mapping: ErrNotFound (wrapped) marks "the requested ref/object/path
// does not exist" outcomes that handlers turn into 404; every other error is
// internal (500).
package gitread

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/coder/open-storage/internal/auth"
	"github.com/coder/open-storage/internal/blob"
	"github.com/coder/open-storage/internal/githttp"
	"github.com/coder/open-storage/internal/ref"
)

// ErrNotFound marks a missing ref, object, or path (handlers map it to 404
// per docs/reference/http-api.md, e.g. unknown branch and unborn-branch reads).
var ErrNotFound = errors.New("not found")

// EmptyTreeSHA is git's well-known empty tree object id (SHA-1 repos): the
// diff base for root commits (`git hash-object -t tree /dev/null`).
const EmptyTreeSHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

// assertf is a fail-fast invariant check for programming errors only —
// request-dependent failures must return errors instead.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("gitread assertion failed: "+format, args...))
	}
}

// runGit executes git -C repoPath with args, returning stdout only. Stderr
// is captured separately and surfaced in the error text so git warnings
// never contaminate parsed stdout.
func runGit(ctx context.Context, repoPath string, args ...string) (string, error) {
	assertf(repoPath != "", "runGit: empty repoPath")
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = repoPath
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// Materialize rebuilds the cache repo for repo from the ref/blob stores
// (githttp.Hydrate: ensure repo, hydrate packs, sync refs from the DB
// authority, point HEAD at defaultBranch) and returns its bare-repo path.
// Unlike the receive path, a ref-sync failure is fatal here: read plumbing
// over stale refs would serve wrong answers.
func Materialize(ctx context.Context, st ref.RefStore, bl blob.BlobStore, cacheDir, repo, defaultBranch string) (string, error) {
	assertf(st != nil && bl != nil && cacheDir != "" && repo != "", "Materialize: missing store/blob/cacheDir/repo")
	repoPath, refsErr, err := githttp.Hydrate(ctx, st, bl, cacheDir, repo, defaultBranch)
	if err != nil {
		return "", err
	}
	if refsErr != nil {
		return "", refsErr
	}
	return repoPath, nil
}

// hexRevRe matches a short or full hex object name (git's minimum
// abbreviation is 4). Restricting the raw-revision fallback to hex prevents
// request input from smuggling arbitrary rev-parse expressions (HEAD~3,
// :/text, ...) into the API surface.
var hexRevRe = regexp.MustCompile(`\A[0-9a-f]{4,40}\z`)

// ResolveRef resolves a request ref to a full commit SHA in the repo at
// repoPath. Resolution order (docs/reference/http-api.md "branch name, short or full SHA"):
//
//  1. branch:   refs/heads/<spec> — or the ephemeral namespace name
//     (auth.EphemeralRefName) when ephemeral is true
//  2. tag:      refs/tags/<spec> (annotated tags peel to their commit)
//  3. full ref: <spec> verbatim when it already starts with "refs/"
//  4. hex:      short or full SHA (lowercase hex only, no rev expressions)
//
// Everything unresolvable wraps ErrNotFound — including the empty spec and
// the unborn-branch case, where the DB knows no such ref yet (invariant 3:
// handlers map it to 404).
func ResolveRef(ctx context.Context, repoPath, spec string, ephemeral bool) (string, error) {
	assertf(repoPath != "", "ResolveRef: empty repoPath")
	if spec == "" {
		return "", fmt.Errorf("resolve empty ref: %w", ErrNotFound)
	}
	// Ref candidates are built only from well-formed ref names
	// (check-ref-format): git forbids the rev-expression metacharacters
	// (~ ^ : @{ ...) in refnames, so a validated candidate cannot smuggle
	// an expression like "main~1" through the rev-parse below.
	validRefName := func(name string) bool {
		_, err := runGit(ctx, repoPath, "check-ref-format", name)
		return err == nil
	}
	var candidates []string
	if ephemeral {
		// Branch names resolve inside the ephemeral namespace only; SHAs
		// (below) address objects, which namespaces share anyway.
		if name := auth.EphemeralRefName(spec); validRefName(name) {
			candidates = append(candidates, name)
		}
	} else if !strings.HasPrefix(spec, "refs/") {
		if validRefName("refs/heads/" + spec) {
			candidates = append(candidates, "refs/heads/"+spec, "refs/tags/"+spec)
		}
	} else if validRefName(spec) {
		candidates = append(candidates, spec)
	}
	if hexRevRe.MatchString(spec) {
		candidates = append(candidates, spec)
	}
	for _, cand := range candidates {
		// --end-of-options stops a hostile spec from being parsed as an
		// option; ^{commit} peels annotated tags and rejects non-commits.
		out, err := runGit(ctx, repoPath, "rev-parse", "--verify", "--quiet", "--end-of-options", cand+"^{commit}")
		if err != nil {
			continue
		}
		sha := strings.TrimSpace(out)
		if !ref.ValidSHA(sha) {
			return "", fmt.Errorf("rev-parse %q emitted %q", cand, sha)
		}
		return sha, nil
	}
	return "", fmt.Errorf("resolve ref %q: %w", spec, ErrNotFound)
}
