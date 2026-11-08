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
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/ref"
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

// runGit runs `git args...` in repoPath as one child of class (see Run),
// returning stdout only. Stderr is kept separately and surfaced in the
// error text so git warnings never contaminate parsed stdout.
func runGit(ctx context.Context, class Class, repoPath string, args ...string) (string, error) {
	assertf(repoPath != "", "runGit: empty repoPath")
	return Run(ctx, Cmd{Class: class, Dir: repoPath, Args: args})
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

// fullHexRe matches a full object name in either case. Git object names
// are case-insensitive hex, so ResolveRef lowercases a full one before the
// hex fallback (issue #381). Abbreviations stay lowercase-only.
var fullHexRe = regexp.MustCompile(`\A[0-9a-fA-F]{40}\z`)

// ResolveRef resolves a request ref to a full commit SHA in the repo at
// repoPath. Resolution order (docs/reference/http-api.md "branch name, short or full SHA"):
//
//  1. branch:   refs/heads/<spec> — or the ephemeral namespace name
//     (auth.EphemeralRefName) when ephemeral is true
//  2. tag:      refs/tags/<spec> (annotated tags peel to their commit)
//  3. full ref: <spec> verbatim when it already starts with "refs/"
//  4. hex:      short or full SHA (no rev expressions; a full SHA may be
//     uppercase, an abbreviation must be lowercase)
//
// Everything unresolvable wraps ErrNotFound — including the empty spec and
// the unborn-branch case, where the DB knows no such ref yet (invariant 3:
// handlers map it to 404). Only git's own negative answer counts as
// unresolvable (gitSaidNo); any other git failure is returned as is.
func ResolveRef(ctx context.Context, repoPath, spec string, ephemeral bool) (string, error) {
	assertf(repoPath != "", "ResolveRef: empty repoPath")
	if spec == "" {
		return "", fmt.Errorf("resolve empty ref: %w", ErrNotFound)
	}
	if len(spec) > maxRefSpecBytes {
		// The spec stays out of the error: it can be request-sized.
		return "", fmt.Errorf("resolve a %d-byte ref spec: %w", len(spec), ErrNotFound)
	}
	if strings.IndexByte(spec, 0) >= 0 {
		// exec refuses an argument with a NUL byte before git starts, so
		// git cannot answer: such a spec names no ref, like any other
		// invalid name.
		return "", fmt.Errorf("resolve ref %q: %w", spec, ErrNotFound)
	}
	// failErr records a git failure that is not git's answer (a resource
	// limit, a cancellation, a kill, an exec.ErrWaitDelay): those must
	// surface as errors, never as "not found".
	var failErr error
	// Ref candidates are built only from well-formed ref names
	// (check-ref-format): git forbids the rev-expression metacharacters
	// (~ ^ : @{ ...) in refnames, so a validated candidate cannot smuggle
	// an expression like "main~1" through the rev-parse below.
	validRefName := func(name string) bool {
		_, err := runGit(ctx, Light, repoPath, "check-ref-format", name)
		if err != nil && failErr == nil && !gitSaidNo(err) {
			failErr = err
		}
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
	hex := spec
	if fullHexRe.MatchString(spec) {
		hex = strings.ToLower(spec)
	}
	if hexRevRe.MatchString(hex) {
		candidates = append(candidates, hex)
	}
	if failErr != nil {
		return "", failErr
	}
	for _, cand := range candidates {
		// --end-of-options stops a hostile spec from being parsed as an
		// option; ^{commit} peels annotated tags and rejects non-commits.
		out, err := runGit(ctx, Light, repoPath, "rev-parse", "--verify", "--quiet", "--end-of-options", cand+"^{commit}")
		if err != nil {
			if !gitSaidNo(err) {
				return "", err
			}
			// rev-parse also exits 1 for a ref or object that exists
			// when git cannot read the commit it leads to: that is the
			// repository's failure, not an unknown ref.
			if berr := commitUnreadable(ctx, repoPath, cand); berr != nil {
				return "", fmt.Errorf("resolve ref %q: %w", spec, berr)
			}
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

// maxRefSpecBytes bounds the spec that ResolveRef hands to git. No ref is
// longer: githttp refuses a pushed ref name past 900 bytes
// (maxPushRefNameBytes), the API refuses a branch name past 255, and git
// keeps every ref in the cache as a loose file, which PATH_MAX (4096 on
// Linux) bounds. A spec past 128 KiB would fail exec with E2BIG, and that
// error would carry the whole spec into the server's log.
const maxRefSpecBytes = 4096

// gitSaidNo reports whether err is git's own negative answer: the child
// ran to the end and exited 1. check-ref-format exits 1 for a name that is
// not a valid ref name, and rev-parse --verify --quiet for one that names
// no commit (missing, ambiguous, or not a commit). Any other failure says
// nothing about the ref: a resource limit or cancellation (gitread limit
// errors never wrap *exec.ExitError), a kill, exec.ErrWaitDelay after a
// long stall, or a fatal exit 128.
func gitSaidNo(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 1
}

// commitUnreadable checks a ResolveRef candidate whose ^{commit} peel git
// answered with exit 1. git gives that exit for a ref or object that does
// not exist, for one that leads to no commit (a tree, a blob, or a tag of
// one), and for one whose way to a commit git cannot read. It returns nil
// for the first two, and an error for the third: an existing ref whose
// object is missing, or an object whose tag chain or commit git cannot
// read.
func commitUnreadable(ctx context.Context, repoPath, cand string) error {
	id := cand
	if strings.HasPrefix(cand, "refs/") {
		// Without a peel, rev-parse reads the ref's value: it exits 1
		// only when the ref does not exist.
		out, err := runGit(ctx, Light, repoPath, "rev-parse", "--verify", "--quiet", "--end-of-options", cand)
		if gitSaidNo(err) {
			return nil
		} else if err != nil {
			return err
		}
		if id = strings.TrimSpace(out); !ref.ValidSHA(id) {
			return fmt.Errorf("rev-parse %q emitted %q", cand, id)
		}
	} else if !ref.ValidSHA(cand) {
		return nil // git resolves an abbreviation only to an object it has
	}
	if _, err := runGit(ctx, Light, repoPath, "cat-file", "-e", id); gitSaidNo(err) {
		if id == cand {
			return nil // a full id that names no object
		}
		return fmt.Errorf("%s names object %s, which is missing", cand, id)
	} else if err != nil {
		return err
	}
	// The object exists. git peels its tag chain to the end (<id>^{}) and
	// fails only when an object on the way is missing or a tag cannot be
	// parsed. Nothing here reads a tag's message.
	out, err := runGit(ctx, Light, repoPath, "rev-parse", "--verify", "--quiet", "--end-of-options", id+"^{}")
	if err != nil {
		return fmt.Errorf("git cannot peel %s: %w", id, err)
	}
	end := strings.TrimSpace(out)
	if !ref.ValidSHA(end) {
		return fmt.Errorf("rev-parse %s^{} emitted %q", id, end)
	}
	out, err = runGit(ctx, Light, repoPath, "cat-file", "-t", end)
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) == "commit" {
		return fmt.Errorf("git cannot read commit %s", end)
	}
	return nil // a tree or a blob, or a tag of one
}
