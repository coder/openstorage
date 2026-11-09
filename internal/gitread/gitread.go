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
// unresolvable (gitSaidNo); any other git failure is returned as is. An
// existing ref, or an object, whose way to a commit git cannot read (a
// missing object, a broken tag) is an error too (resolveCommitObject).
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
	// rev-parse reads each candidate as a ref name, including its DWIM
	// rules ("refs/r" can name refs/heads/refs/r). It rejects malformed
	// names itself, but also accepts revision expressions: refuse every
	// revision operator and control byte before any spec reaches it.
	// Those bytes cannot be in a ref's name either. Only the hex fallback
	// below is an object revision.
	if strings.ContainsAny(spec, "~^:?*[\\") || strings.Contains(spec, "..") || strings.Contains(spec, "@{") {
		return "", fmt.Errorf("resolve ref %q: %w", spec, ErrNotFound)
	}
	for _, c := range []byte(spec) {
		if c <= ' ' || c == 0x7f {
			return "", fmt.Errorf("resolve ref %q: %w", spec, ErrNotFound)
		}
	}
	var candidates []string
	if ephemeral {
		candidates = append(candidates, auth.EphemeralRefName(spec))
	} else if strings.HasPrefix(spec, "refs/") {
		candidates = append(candidates, spec)
	} else {
		candidates = append(candidates, "refs/heads/"+spec, "refs/tags/"+spec)
	}
	hex := spec
	if fullHexRe.MatchString(spec) {
		hex = strings.ToLower(spec)
	}
	if hexRevRe.MatchString(hex) {
		candidates = append(candidates, hex)
	}
	for _, cand := range candidates {
		if hexRevRe.MatchString(cand) && !ref.ValidSHA(cand) {
			// An abbreviation peels first, as on main: when a commit
			// shares it with a blob or a tree, only <abbrev>^{commit}
			// picks the commit, and the plain name is ambiguous. On exit
			// 1, the plain lookup below still reads a tag whose commit
			// git cannot read.
			out, err := runGit(ctx, Light, repoPath, "rev-parse", "--verify", "--quiet", "--end-of-options", cand+"^{commit}")
			if err == nil {
				sha := strings.TrimSpace(out)
				if !ref.ValidSHA(sha) {
					return "", fmt.Errorf("rev-parse %q emitted %q", cand+"^{commit}", out)
				}
				return sha, nil
			} else if !gitSaidNo(err) {
				return "", err
			}
		}
		// No peel: rev-parse reads a ref's value even when its object is
		// missing, and exits 1 only for a name that does not resolve.
		// Each lookup reads one name, never a ref directory listing.
		out, err := runGit(ctx, Light, repoPath, "rev-parse", "--verify", "--quiet", "--end-of-options", cand)
		if gitSaidNo(err) {
			continue
		} else if err != nil {
			return "", err
		}
		id := strings.TrimSpace(out)
		if !ref.ValidSHA(id) {
			return "", fmt.Errorf("rev-parse %q emitted %q", cand, out)
		}
		sha, err := resolveCommitObject(ctx, repoPath, cand, id)
		if err != nil {
			return "", fmt.Errorf("resolve ref %q: %w", spec, err)
		}
		if sha != "" {
			return sha, nil
		}
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
// ran to the end and exited 1. rev-parse --verify --quiet exits 1 for a
// name that names no commit (missing, ambiguous, or not a commit). Any other failure says
// nothing about the ref: a resource limit or cancellation (gitread limit
// errors never wrap *exec.ExitError), a kill, exec.ErrWaitDelay after a
// long stall, or a fatal exit 128.
func gitSaidNo(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 1
}

// resolveCommitObject resolves a candidate's full object id to a commit.
// One batch-check reads the object's type and its peel to a commit. git
// answers a ref whose object is missing as "<id> missing", and a tag
// whose commit is missing as "<id>^{commit} missing". An id with no
// object, and an object that is not a commit (or a tag of one), answer
// with no sha and no error. A ref whose object is missing, a broken tag
// chain, and a commit git cannot read are errors.
func resolveCommitObject(ctx context.Context, repoPath, cand, id string) (string, error) {
	out, err := Run(ctx, Cmd{Class: Light, Dir: repoPath,
		Stdin: strings.NewReader(id + "\n" + id + "^{commit}\n" + id + "^{}\n"),
		Args:  []string{"cat-file", "--batch-check=%(objectname) %(objecttype)"}})
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 {
		return "", fmt.Errorf("cat-file --batch-check for %s emitted %q", id, out)
	}
	if lines[0] == id+" missing" {
		if cand == id {
			return "", nil // an unknown full id
		}
		return "", fmt.Errorf("%s names object %s, which is missing", cand, id)
	}
	oid, typ, _ := strings.Cut(lines[0], " ")
	if oid != id || (typ != "commit" && typ != "tag" && typ != "tree" && typ != "blob") {
		return "", fmt.Errorf("cat-file --batch-check for %s emitted %q", id, out)
	}
	if sha, kind, ok := strings.Cut(lines[1], " "); ok && ref.ValidSHA(sha) && kind == "commit" {
		return sha, nil
	}
	if lines[1] != id+"^{commit} missing" {
		return "", fmt.Errorf("cat-file --batch-check for %s emitted %q", id, out)
	}
	if typ == "tree" || typ == "blob" {
		return "", nil
	}
	// A tag of a tree or a blob is not a commit. A failed ^{commit}
	// peel, and anything else at the chain's end, is git's failure to
	// read a commit or to peel the tag chain, not an unknown candidate.
	if end, kind, ok := strings.Cut(lines[2], " "); ok && ref.ValidSHA(end) && (kind == "tree" || kind == "blob") {
		return "", nil
	}
	return "", fmt.Errorf("git cannot read commit or tag chain %s", id)
}
