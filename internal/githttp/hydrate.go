package githttp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/ref"
)

// A repo URL element is a base repo name optionally followed by one
// namespace suffix, `+ephemeral` or `+import` (docs/explanation/git-protocol-and-namespaces.md). Namespaced repos
// use fully separate ref stores, pack spaces, and cache dirs keyed by the
// full name — docs/explanation/git-protocol-and-namespaces.md allows "git namespaces or separate ref stores", and
// separate stores give the required isolation for free.
var repoNameRe = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]*(\+(ephemeral|import))?\z`)

// ValidRepoName guards against path traversal via the URL repo element.
func ValidRepoName(repo string) bool {
	return repoNameRe.MatchString(repo) && !strings.Contains(repo, "..")
}

// BaseRepo strips the optional namespace suffix: JWTs carry the base repo id
// in the `repo` claim and are valid for its namespaces (docs/explanation/git-protocol-and-namespaces.md URL scheme).
func BaseRepo(repo string) string {
	base, _, _ := strings.Cut(repo, "+")
	return base
}

// RepoPath maps a repo name to its bare cache repo directory.
func RepoPath(cacheDir, repo string) string {
	assertf(ValidRepoName(repo), "RepoPath: invalid repo name %q", repo)
	return filepath.Join(cacheDir, repo+".git")
}

// gitSpawnObserver, when set, observes every git subprocess spawned through
// runGitCtx, updateRefStdin, and runCommitGraphAttempt — the only exec
// sites reachable from Hydrate. Test seam for issue #93's guarantee that
// the generation-match fast path forks no subprocess; an atomic pointer so
// installing the observer never races a concurrent hydrator.
var gitSpawnObserver atomic.Pointer[func(dir string, args []string)]

func observeGitSpawn(dir string, args []string) {
	if obs := gitSpawnObserver.Load(); obs != nil {
		(*obs)(dir, args)
	}
}

// hookVerifyBarrier, when set, runs inside ensureHook's verification branch
// between the content read and the trailing descriptor stat. Test seam for
// injecting an in-place rewrite into that window deterministically (PR #98
// round-9 review); same atomic-pointer pattern as gitSpawnObserver.
var hookVerifyBarrier atomic.Pointer[func(path string)]

// runGit executes git with args, returning stdout only. Stderr is captured
// separately and surfaced in the error text: git warnings on stderr must
// never contaminate parsed stdout (e.g. for-each-ref in syncRefsStreamed).
func runGit(dir string, args ...string) (string, error) {
	return runGitCtx(context.Background(), dir, args...)
}

// runGitCtx is runGit with cancellation: the subprocess is killed when ctx
// expires. Pack materialization runs under callers' deadlines (the repack
// fold's wall-clock budget, request contexts), and index-pack over a large
// pack must not outlive them. A kill caused by ctx folds the context's own
// error into the chain — the raw *exec.ExitError ("signal: killed") does
// not match errors.Is(err, context.DeadlineExceeded), and the repack
// worker classifies deterministic budget exhaustion by that identity.
func runGitCtx(ctx context.Context, dir string, args ...string) (string, error) {
	return runGitLeased(ctx, dir, dir, args...)
}

// runGitLeased is runGitCtx for a child that holds the lease of the cache
// repository at leaseDir (gitlimit.LeaseFile). dir is the child's working
// directory. It is empty for `git init --bare <leaseDir>`, which runs where
// serve runs.
func runGitLeased(ctx context.Context, dir, leaseDir string, args ...string) (string, error) {
	observeGitSpawn(dir, args)
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := gitlimit.OutputIn(ctx, leaseDir, cmd)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			err = fmt.Errorf("%w: %w", cerr, err)
		}
		return string(out), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

const hookScript = `#!/bin/sh
exec %q hook %s "$@"
`

// EnsureCacheRepo idempotently creates the bare cache repo with the server
// config and hooks that re-exec this binary (no shell/psql deps in hooks).
//
// Config and hooks are verified on every call, not only at init time: a
// crash between `git init` and hook installation (or a deleted/tampered
// hook file) must never leave a repo whose receive-pack silently runs
// without the hooks that own S3 upload, PG pack/ref registration, and
// policy enforcement. The hook script also embeds this binary's path,
// which changes across deployments.
//
// The steady-state verification is memoized (issue #93): the full pass —
// two `git config --get` forks plus per-hook ReadFile/Stat — used to run
// on every request and dominated the hydration fast path (~93% of it, PR
// #97 baseline). Every mutator of the verified state is our own code
// (`git init` and the `git config` writes below rewrite config via
// lockfile + rename; ensureHook rewrites hooks via temp + rename; nothing
// in this codebase deletes cache repos), so a fingerprint of the exact
// file states the last full pass in THIS process verified proves them
// unchanged without forking — see cacheRepoMemo.
func EnsureCacheRepo(cacheDir, repo string) (string, error) {
	return EnsureCacheRepoCtx(context.Background(), cacheDir, repo)
}

// cacheRepoMemo fingerprints (os.FileInfo: dev+inode, mtime, size, mode)
// the config and hook files a full EnsureCacheRepo pass verified. The
// FileInfos come from the verification itself (ensureHook returns the stat
// of the content it read-verified or the temp file it installed — rename
// preserves inode, mtime, and mode), so a memo can only ever vouch for a
// state some full pass actually checked. Every mutation invalidates it:
// rewrites through lockfile/temp + rename change the inode (os.SameFile),
// in-place writes bump mtime or size, and a bare chmod — which changes
// neither — flips the mode, compared precisely because git silently skips
// non-executable hooks. Re-executed hook processes and restarted servers
// start with an empty memo and always take the full pass.
type cacheRepoMemo struct {
	config      os.FileInfo
	preReceive  os.FileInfo
	update      os.FileInfo
	postReceive os.FileInfo
	refTx       os.FileInfo
}

// cacheRepoMemos maps cache repo paths to their verified fingerprints, one
// small entry per repo this process serves. Full passes serialize on the
// repository's hydrate lock; memo hits remain lock-free.
var cacheRepoMemos sync.Map // map[string]cacheRepoMemo

// memoFiles pairs each fingerprint with the path it must still match.
func (m cacheRepoMemo) memoFiles(repoPath string) [5]struct {
	fi   os.FileInfo
	path string
} {
	return [5]struct {
		fi   os.FileInfo
		path string
	}{
		{m.config, filepath.Join(repoPath, "config")},
		{m.preReceive, filepath.Join(repoPath, "hooks", "pre-receive")},
		{m.update, filepath.Join(repoPath, "hooks", "update")},
		{m.postReceive, filepath.Join(repoPath, "hooks", "post-receive")},
		// The reference-transaction hook joins the memo (PR #195 round-11
		// P2): excluded, a memo hit skipped its exec-bit and staleness
		// repairs for the process's whole lifetime — the mode flip alone
		// invalidates this fingerprint and forces the full pass.
		{m.refTx, filepath.Join(repoPath, "hooks", "reference-transaction")},
	}
}

// cacheRepoConfig lists the values every cache repo must pin in its LOCAL
// config file. Every push must yield a pack file (>= 0 received objects
// keeps the pack).
var cacheRepoConfig = [][2]string{
	{"receive.unpackLimit", "0"},
	{"gc.auto", "0"},
}

// fileUnchanged reports whether two stats identify the same unchanged file:
// same inode (os.SameFile — rewrites via lockfile/temp + rename always
// change it), mtime, size, mode (a bare chmod changes nothing else, and
// git silently skips non-executable hooks), and ctime (an in-place rewrite
// can preserve all of the former with a same-length write plus Chtimes,
// but never ctime — see fileChangeTime).
func fileUnchanged(a, b os.FileInfo) bool {
	assertf(a != nil && b != nil, "fileUnchanged: nil FileInfo")
	return os.SameFile(a, b) && a.ModTime().Equal(b.ModTime()) && a.Size() == b.Size() &&
		a.Mode() == b.Mode() && fileChangeTime(a).Equal(fileChangeTime(b))
}

// errConfigHasIncludes marks a local config containing [include]/
// [includeIf] directives: the included files control effective values but
// live outside the memo's fingerprint, so such a config is verified but
// NEVER memoized — every call re-checks (PR #98 round-5 review).
var errConfigHasIncludes = errors.New("local config contains include directives; memoization disabled")

// errConfigValueMismatch marks a pinned key whose effective local-scope
// value (includes resolved) is wrong or unset. Surviving the caller's own
// direct repair write, it can only mean an include override or a hostile
// racer — corruption-grade.
var errConfigValueMismatch = errors.New("local config value mismatch")

// errConfigChangedUnderfoot marks a config file that changed between the
// bracketing stats of one verification pass: a concurrent full pass is
// racing. Benign for serving — the racer writes the same values — but the
// pass proved nothing, so it must not memoize.
var errConfigChangedUnderfoot = errors.New("config file changed underneath the verification reads")

// verifyLocalConfig proves every cacheRepoConfig value is effective in the
// repo's LOCAL scope and returns a fingerprint of the exact file state the
// reads covered: stat, read each value, stat again, and fail unless both
// stats identify the same unchanged file — a rewrite racing the reads
// could otherwise pair one file's values with another's fingerprint (PR
// #98 round-3 review).
//
// Scoped --local: an unscoped get would be satisfied by global/system
// scopes, skip the local pin, and the memo (which fingerprints only the
// local file) would freeze that decision against later drift of the
// external source (PR #98 round-2 review). The local pin also outranks
// those scopes at receive-pack time, so drift cannot re-enable unpacking
// pushes into loose objects that the hooks never upload.
//
// WITH --includes and --get-all (PR #98 round-5 review): receive-pack
// follows local [include] directives and takes the LAST definition, so a
// local include appended after the pins could override them while an
// include-blind single-value read still reported the pin. Resolving
// includes and checking the LAST value listed — git's last-definition-wins,
// which is exactly what receive-pack loads — makes any effective override
// fail verification, while an earlier stale duplicate followed by the safe
// pin stays accepted, as the pre-memo code accepted it (PR #98 round-6
// review). A config that merely CONTAINS include directives additionally
// fails with errConfigHasIncludes after the value checks, because the
// included files could drift later without touching the fingerprinted
// file.
func verifyLocalConfig(ctx context.Context, repoPath, configPath string) (os.FileInfo, error) {
	pre, err := os.Stat(configPath)
	if err != nil {
		return nil, err
	}
	for _, kv := range cacheRepoConfig {
		cur, err := runGitCtx(ctx, repoPath, "config", "--local", "--includes", "--get-all", kv[0])
		if err != nil {
			return nil, err
		}
		vals := strings.Split(strings.TrimSpace(cur), "\n")
		if len(vals) == 0 || vals[0] == "" {
			return nil, fmt.Errorf("%w: %s unset, want %q", errConfigValueMismatch, kv[0], kv[1])
		}
		if got := vals[len(vals)-1]; got != kv[1] {
			return nil, fmt.Errorf("%w: %s = %q effective, want %q", errConfigValueMismatch, kv[0], got, kv[1])
		}
	}
	// Include scan AFTER the value checks so callers repair values first;
	// --no-includes lists the raw directives themselves.
	names, err := runGitCtx(ctx, repoPath, "config", "--local", "--no-includes", "--name-only", "--list")
	if err != nil {
		return nil, err
	}
	hasIncludes := false
	for name := range strings.SplitSeq(names, "\n") {
		key := strings.ToLower(strings.TrimSpace(name))
		if strings.HasPrefix(key, "include.") || strings.HasPrefix(key, "includeif.") {
			hasIncludes = true
			break
		}
	}
	post, err := os.Stat(configPath)
	if err != nil {
		return nil, err
	}
	if !fileUnchanged(pre, post) {
		return nil, errConfigChangedUnderfoot
	}
	if hasIncludes {
		return nil, errConfigHasIncludes
	}
	return pre, nil
}

// cacheRepoUnchanged reports whether repoPath still matches the memoized
// verified state, using only stats (zero forks, zero reads).
func cacheRepoUnchanged(repoPath string, m cacheRepoMemo) bool {
	// Defensive (issue #93): a memo must never vouch for a missing or
	// recreated repo dir — fall through to the full pass, never crash. HEAD
	// is the same liveness probe the full pass uses.
	if _, err := os.Stat(filepath.Join(repoPath, "HEAD")); err != nil {
		return false
	}
	for _, f := range m.memoFiles(repoPath) {
		assertf(f.fi != nil, "cacheRepoUnchanged: memo without fingerprint for %s", f.path)
		fi, err := os.Stat(f.path)
		if err != nil || !fileUnchanged(f.fi, fi) {
			return false
		}
	}
	return true
}

// EnsureCacheRepoCtx is EnsureCacheRepo honoring ctx in its lock wait and
// git subprocesses (init, config): the repack fold's materialization runs the
// setup under the fold's wall-clock budget, and a stalled subprocess or
// filesystem must not outlive it.
func EnsureCacheRepoCtx(ctx context.Context, cacheDir, repo string) (repoPath string, retErr error) {
	// Before memoization every call reached runGitCtx and propagated an
	// expired context via its first subprocess; the memo hit must honor
	// the same contract — the repack materializer runs this under its
	// fold's wall-clock budget (PR #98 round-9 review).
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path := RepoPath(cacheDir, repo)
	if m, ok := cacheRepoMemos.Load(path); ok && cacheRepoUnchanged(path, m.(cacheRepoMemo)) {
		return path, nil
	}
	// A repository that this call creates gets the lease epoch
	// (gitlimit.EpochFile) before git runs in it. In a directory that already
	// existed, an older release can have a git alive that holds no lease, so
	// the stale ref lock cleanup keeps every lock there (staleref.go).
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	switch err := os.Mkdir(path, 0o755); {
	case err == nil:
		if err := gitlimit.CreateEpoch(path); err != nil {
			return "", err
		}
	case !errors.Is(err, os.ErrExist):
		return "", err
	}
	// Cold initialization and repairs write config.lock. Serialize the full
	// pass across processes sharing this cache, using the existing hydrate
	// lock inode (never unlink it). Release before Hydrate reads authority or
	// takes this lock for pack/ref synchronization; no locks are nested here.
	unlock, err := flockExclusiveCtx(ctx, filepath.Join(path, "codestorage.hydrate.lock"))
	if err != nil {
		return "", fmt.Errorf("lock cache initialization %s: %w", repo, err)
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// A previous initializer may have finished while we waited. Keep both
	// the warm fast path above and same-process waiters fork-free.
	if m, ok := cacheRepoMemos.Load(path); ok {
		if cacheRepoUnchanged(path, m.(cacheRepoMemo)) {
			return path, nil
		}
		// Stale memo: the repo was recreated, repaired by another process,
		// or a verified file changed underneath us. Drop it and take the
		// full verify/repair pass below, which re-memoizes on success.
		cacheRepoMemos.Delete(path)
	}
	// A prior interrupted init may have left HEAD but not the directories
	// Git requires. Re-init that partial tree without discarding its contents.
	needsInit := false
	for _, name := range []string{"HEAD", "objects", "refs"} {
		fi, err := os.Stat(filepath.Join(path, name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if err != nil || (name != "HEAD" && !fi.IsDir()) {
			needsInit = true
		}
	}
	// A canceled Git child can leave HEAD before init finishes, or leave
	// HEAD.lock/config.lock. After runGitCtx reaps it, remove only our new
	// regular files under the hydrate lock. Never remove preexisting state.
	initialized := false
	var cleanup [3]string
	for i, name := range []string{"HEAD", "HEAD.lock", "config.lock"} {
		if name != "config.lock" && !needsInit {
			continue
		}
		file := filepath.Join(path, name)
		if _, err := os.Lstat(file); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		cleanup[i] = file
	}
	defer func() {
		if retErr == nil {
			return
		}
		for _, file := range cleanup {
			if file == "" || (initialized && filepath.Base(file) == "HEAD") {
				continue
			}
			fi, err := os.Lstat(file)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err == nil {
				if fi.Mode().IsRegular() {
					err = os.Remove(file)
				} else {
					err = errors.New("not a regular file")
				}
			}
			if err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("clean interrupted cache initialization %s: %w", file, err))
			}
		}
	}()
	if needsInit {
		if _, err := runGitLeased(ctx, "", path, "init", "--bare", "--initial-branch=main", path); err != nil {
			return "", err
		}
	}
	initialized = true
	// Read-check first: config writes take config.lock, and concurrent
	// hydrates of the same repo must not fail on lock contention in the
	// steady-state already-configured case. On any verification failure —
	// values missing (fresh init), drifted, or the file changing under the
	// reads — write the values and verify once more, so a fresh or
	// repaired repo converges to the memoized fork-free steady state in
	// this same call. A second failure means a concurrent writer is still
	// racing: proceed (our values are written; a racer writes the same
	// ones) but skip memoization below — fail closed, the next call
	// re-verifies.
	configPath := filepath.Join(path, "config")
	configFI, configErr := verifyLocalConfig(ctx, path, configPath)
	if configErr != nil && !errors.Is(configErr, errConfigHasIncludes) {
		for _, kv := range cacheRepoConfig {
			// --replace-all: a hostile trailing duplicate must collapse to
			// one pinned definition — a single-value write would fail with
			// "cannot overwrite multiple values" (PR #98 round-6 review).
			if _, err := runGitCtx(ctx, path, "config", "--replace-all", kv[0], kv[1]); err != nil {
				return "", err
			}
		}
		configFI, configErr = verifyLocalConfig(ctx, path, configPath)
		// A wrong effective value surviving our own direct write can only
		// be a local include override or a hostile racer: fail loudly
		// rather than serve pushes that unpack loose objects the hooks
		// never persist (PR #98 round-5 review).
		if errors.Is(configErr, errConfigValueMismatch) {
			return "", fmt.Errorf("cache repo %s: pinned config overridden after repair: %w", repo, configErr)
		}
		// Only the two documented outcomes are benign residuals: a racing
		// full pass changing the file underfoot (serve, skip memoization)
		// or include directives (same). Anything else — context expiry,
		// git or stat failures — must propagate: the repack materializer
		// runs this under its wall-clock budget and swallowing its
		// cancellation would fake success (PR #98 round-8 review).
		if configErr != nil && !errors.Is(configErr, errConfigHasIncludes) &&
			!errors.Is(configErr, errConfigChangedUnderfoot) {
			return "", configErr
		}
	}
	self, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve own executable for hooks: %w", err)
	}
	hooksDir := filepath.Join(path, "hooks")
	if err := os.MkdirAll(hooksDir, 0o755); err != nil {
		return "", err
	}
	var memo cacheRepoMemo
	for _, hook := range []struct {
		name string
		fi   *os.FileInfo
	}{
		{"pre-receive", &memo.preReceive},
		{"update", &memo.update},
		{"post-receive", &memo.postReceive},
	} {
		script := fmt.Sprintf(hookScript, self, hook.name)
		fi, err := ensureHook(filepath.Join(hooksDir, hook.name), script)
		if err != nil {
			return "", err
		}
		assertf(fi != nil, "ensureHook returned no FileInfo for %s", hook.name)
		*hook.fi = fi
	}
	// The reference-transaction hook is installed IF ABSENT only, and
	// deliberately outside the verified memo (issue #186 finding 1): it
	// is a best-effort release optimization, not an enforcement hook — a
	// missing or foreign script only defers hold release to post-receive,
	// the pre-existing bound — and tests (or operators) may legitimately
	// wrap it, which content verification would clobber on the next pass.
	// Failure here is NONFATAL (PR #195 round-11 P2): this hook is a
	// best-effort release optimization whose absence falls back to
	// post-receive — an unwritable hooks directory in an otherwise
	// serviceable repo must not take it offline while the enforcement
	// hooks above verified fine. An unverified state is simply never
	// memoized, so every call retries the install until it heals.
	refTxFI, refTxErr := ensureRefTxHook(filepath.Join(hooksDir, "reference-transaction"),
		fmt.Sprintf(refTxHookScript, self))
	// Memoize only a config state some verifyLocalConfig pass proved: the
	// fingerprint identifies the exact unchanged file the reads covered
	// (PR #98 round-3 review). A raced rewrite leaves configErr set and
	// skips memoization entirely — hooks included, so the whole memo only
	// ever vouches for verified state.
	if configErr == nil && refTxErr == nil && refTxFI != nil {
		assertf(configFI != nil, "verifyLocalConfig returned no FileInfo for %s", configPath)
		memo.config = configFI
		memo.refTx = refTxFI
		cacheRepoMemos.Store(path, memo)
	}
	return path, nil
}

// refTxHookScript gates on the hold env INSIDE the script, unlike the
// enforcement hooks: reference-transaction fires on EVERY ref transaction
// in the cache repo — hydration's update-ref batches, maintenance,
// read-path fixture setup — and only receive-pack pushes export the hold
// env. Non-push transactions must cost one /bin/sh exit, not a
// server-binary exec per transaction (and, in packages whose test
// binaries do not dispatch hook args, not a recursive test run — the
// enforcement hooks never fire outside receive-pack, so only this hook
// needs the script-level gate).
//
// The binary runs WITHOUT exec, guarded by `|| exit 0` (PR #195 round-1
// P1): a failed exec exits a non-interactive POSIX shell with the exec's
// own status, and a nonzero exit at the "prepared" state ABORTS git's
// ref transaction — a vanished binary (mid-rolling-deploy) would fail a
// valid push whose authority CASes already committed, the exact
// wrong-way failure this machinery exists to prevent. The hook is
// best-effort by contract; every failure mode must degrade to
// "release deferred to post-receive", never to an aborted transaction.
const refTxHookScript = refTxHookGate + `%q hook reference-transaction "$@" || exit 0
`

// refTxHookGate is the script's managed-identity prefix, shared by the
// generator above and the refresh detector below. The committed-state
// check lives in the SHELL too (PR #195 round-6 security P2): git
// versions with per-ref reference transactions invoke the hook at both
// prepared and committed for every ref, and a Go-side gate alone would
// exec the full binary twice per ref on an allowed high-fanout push —
// node-wide process amplification under concurrent pushes. Every
// non-committed callback costs one /bin/sh exit; per-ref gits then pay
// one binary exec per committed ref, the same order as their own
// per-command update hooks, and batched gits pay one per push.
const refTxHookGate = `#!/bin/sh
[ -n "$OPENSTORAGE_GIT_PUSH_HOLD" ] || exit 0
[ "$1" = committed ] || exit 0
`

// refTxHookGateV015 is the 0.15.x and 0.16.x gate, which fell back to the
// CODESTORAGE_* name when the canonical one was unset. Recognizing it lets the
// refresh pass replace it with the canonical-only gate above.
const refTxHookGateV015 = `#!/bin/sh
[ -n "${OPENSTORAGE_GIT_PUSH_HOLD-${CODESTORAGE_GIT_PUSH_HOLD}}" ] || exit 0
[ "$1" = committed ] || exit 0
`

// Keep the exact historical CODESTORAGE gates: cache repos survive upgrades,
// including old binary paths.
const refTxHookGateLegacy = refTxHookGateV1 + `[ "$1" = committed ] || exit 0
`

// refTxHookGateV1 is the first managed generation's gate (no committed line).
const refTxHookGateV1 = `#!/bin/sh
[ -n "$CODESTORAGE_GIT_PUSH_HOLD" ] || exit 0
`

// isManagedRefTxHook reports whether content is one of OUR generated
// reference-transaction scripts — the COMPLETE template shape (the gate
// plus exactly one delegated-invocation line, any embedded binary path,
// exec-style historical generations included), nothing more (PR #195
// round-3 P2): an operator wrapper that keeps the gate and the delegated
// invocation but ADDS logic must classify as foreign, or the refresh
// pass would silently strip the additions.
func isManagedRefTxHook(content []byte) bool {
	// Every generation this PR's iterations produced is recognized (PR
	// #195 round-8 P1): the committed-state gate line was added late, and
	// matching only the current gate would orphan scripts installed by
	// earlier snapshots — unrefreshable stale binary paths whose failing
	// prepared callbacks abort valid pushes. Longest gate first: the
	// older gate is a prefix of the newer.
	rest, ok := strings.CutPrefix(string(content), refTxHookGate)
	if !ok {
		rest, ok = strings.CutPrefix(string(content), refTxHookGateV015)
	}
	if !ok {
		rest, ok = strings.CutPrefix(string(content), refTxHookGateLegacy)
	}
	if !ok {
		rest, ok = strings.CutPrefix(string(content), refTxHookGateV1)
	}
	if !ok {
		// The original, ungated generation (the plain enforcement-hook
		// template): shebang plus one delegated-invocation line, nothing
		// else (PR #195 round-13 P1). An operator script of this exact
		// shape is indistinguishable by construction — and refreshing it
		// preserves its whole observable behavior (it already delegated
		// everything to the binary) while adding the cost gates.
		rest, ok = strings.CutPrefix(string(content), "#!/bin/sh\n")
	}
	if !ok {
		return false
	}
	line, rest, ok := strings.Cut(rest, "\n")
	if !ok || rest != "" {
		return false
	}
	// The invocation line must consist SOLELY of the optional historical
	// `exec`, one quoted executable, and the delegation (PR #195 round-4
	// P2): a suffix-only check classified same-line wrapper additions
	// (`logger ...; "/usr/bin/openstorage" hook ...`) as managed and
	// clobbered them.
	line, cut := strings.CutSuffix(line, ` hook reference-transaction "$@" || exit 0`)
	if !cut {
		if line, cut = strings.CutSuffix(line, ` hook reference-transaction "$@"`); !cut {
			return false
		}
	}
	line = strings.TrimPrefix(line, "exec ")
	if len(line) < 2 || line[0] != '"' {
		return false
	}
	path, err := strconv.Unquote(line) // errors on any trailing bytes
	if err != nil || !filepath.IsAbs(path) {
		return false
	}
	// Literal executables only (PR #195 round-9 P2): $, backquote, and
	// backslash keep their meaning inside shell double quotes, so a
	// quoted `"$OPENSTORAGE_BIN"` passes Unquote while being an
	// operator's dynamic selection — the generator only ever emits
	// literal absolute paths, and anything else must classify as foreign
	// and be preserved.
	return !strings.ContainsAny(path, "$`\\")
}

// ensureRefTxHook installs script at path when nothing exists there, and
// REFRESHES it when the existing content is a stale managed script (PR
// #195 round-1 P1): cache repos survive deployments while the script
// embeds a version-specific binary path — install-if-absent alone would
// preserve a dead path forever, and its failure at "prepared" aborts
// valid pushes after an upgrade. Foreign/operator content is preserved:
// this hook is a best-effort release optimization whose replacement only
// defers release to post-receive, and tests legitimately wrap it. Writes
// go through temp + rename (a concurrent receive-pack never execs a
// partial script; concurrent installers write identical content and the
// last rename wins).
// It returns the FileInfo of the exact state it verified, installed, or
// preserved — memoized like the enforcement hooks' (PR #195 round-11 P2:
// a memo excluding this hook skipped its repairs for the process's whole
// lifetime) — or nil when no verified state exists (a raced install
// aborted; the next full pass re-decides).
func ensureRefTxHook(path, script string) (os.FileInfo, error) {
	// Content, mode, and fingerprint come from ONE descriptor (PR #195
	// round-14 P2, mirroring ensureHook): a re-read or path-stat could
	// pair one inode's bytes with a concurrent replacement's metadata —
	// memoizing a fingerprint the content check never covered. The final
	// path re-stat confirms the verified inode still names the path.
	var existing []byte
	f, err := os.Open(path)
	switch {
	case err == nil:
		// Stat–read–stat: the fingerprint is taken BEFORE the bytes and
		// re-verified unchanged after them, so it can only ever describe
		// the exact content the checks below examined — an in-place
		// rewrite mid-read (which keeps the inode, defeating SameFile
		// alone) changes mtime/ctime and refuses the pass.
		fi, err := f.Stat()
		if err == nil {
			existing, err = io.ReadAll(f)
		}
		if testHookRefTxMidVerify != nil {
			testHookRefTxMidVerify()
		}
		var fi2 os.FileInfo
		if err == nil {
			fi2, err = f.Stat()
		}
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if !fileUnchanged(fi, fi2) {
			_ = f.Close()
			return nil, nil // rewritten underfoot; next pass re-decides
		}
		if string(existing) == script {
			// Verify the mode too (PR #195 round-2 P2): a restored cache
			// volume can strip the execute bit while the bytes stay
			// current, and git SILENTLY skips non-executable hooks. The
			// OWNER bit specifically (round-8 P2): the serving process is
			// the file's owner, and POSIX resolves exactly one permission
			// class — group/other execute bits do nothing for it. The
			// descriptor chmod is atomic on the VERIFIED inode.
			if fi.Mode()&0o100 == 0 {
				if err := f.Chmod(0o755); err != nil {
					_ = f.Close()
					return nil, err
				}
				if fi, err = f.Stat(); err != nil {
					_ = f.Close()
					return nil, err
				}
			}
			_ = f.Close()
			psi, err := os.Stat(path)
			if err != nil || !os.SameFile(fi, psi) {
				return nil, nil // replaced underfoot; next pass re-decides
			}
			return fi, nil
		}
		_ = f.Close()
		if !isManagedRefTxHook(existing) {
			// Foreign content is preserved — fingerprinted only while the
			// verified inode still names the path.
			psi, err := os.Stat(path)
			if err != nil || !os.SameFile(fi, psi) {
				return nil, nil
			}
			return psi, nil
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op after a successful rename
	if _, err := tmp.WriteString(script); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Chmod(0o755); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	fi, err := tmp.Stat()
	if err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if testHookRefTxPreInstall != nil {
		testHookRefTxPreInstall()
	}
	// No-clobber installation (PR #195 rounds 9 and 12): link(2) fails
	// EEXIST instead of displacing whatever occupies the path, so a
	// concurrent install always survives — there is no read-to-rename
	// window at all. The install links the temp's inode into place and
	// re-stats the DESTINATION for the memo fingerprint (round-12 P3:
	// link updates ctime, which fileUnchanged compares — a pre-install
	// fingerprint would reject every memo immediately and re-run the
	// full verification per request).
	install := func() (os.FileInfo, error) {
		if err := os.Link(tmp.Name(), path); err != nil {
			if errors.Is(err, os.ErrExist) {
				return nil, nil // a concurrent install won; next pass re-decides
			}
			return nil, err
		}
		// Drop the temp NAME before fingerprinting: unlinking it later
		// updates the shared inode's ctime (link count is inode
		// metadata), which fileUnchanged compares — a fingerprint taken
		// with the temp name still present is invalidated the moment the
		// deferred cleanup runs, rejecting the memo on every subsequent
		// call (the second test-lane CI red on this PR).
		_ = os.Remove(tmp.Name())
		dfi, err := os.Stat(path)
		if err != nil || !os.SameFile(fi, dfi) {
			return nil, nil
		}
		return dfi, nil
	}
	if existing == nil {
		return install()
	}
	// The replace path takes the target ASIDE atomically and inspects the
	// inode it actually displaced: only content byte-equal to the stale
	// managed script the decision described is discarded; anything else —
	// a foreign hook landing at any point since the decision read — is
	// restored, again without clobbering an even newer install. The hook
	// may be briefly absent mid-replace; it is optional by contract
	// (release falls back to post-receive), unlike the enforcement hooks.
	quarantine := tmp.Name() + ".displaced"
	if err := os.Rename(path, quarantine); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return install() // vanished underfoot; a plain no-clobber create
		}
		return nil, err
	}
	displaced, derr := os.ReadFile(quarantine)
	if derr == nil && bytes.Equal(displaced, existing) {
		dfi, ierr := install()
		_ = os.Remove(quarantine)
		return dfi, ierr
	}
	// Foreign (or unverifiable) content was displaced: restore it. EEXIST
	// means an even newer install took the slot — the operator's latest
	// wins over the mid-window one, exactly as consecutive operator
	// writes always resolve. On any OTHER restore failure (ENOSPC,
	// EMLINK, no-hardlink fs) the quarantined inode is RETAINED at its
	// .displaced name for operator recovery instead of silently deleted
	// (PR #195 round-14 P2) — the promise is preservation, and a leaked
	// recovery file beats a lost hook.
	if lerr := linkFile(quarantine, path); lerr == nil || errors.Is(lerr, os.ErrExist) {
		_ = os.Remove(quarantine)
	}
	return nil, nil
}

// ensureHook installs the hook script unless it is already current and
// executable (git silently skips non-executable hooks, which would bypass
// enforcement). Writes go through a temp file + rename so a concurrent
// receive-pack never execs a partially written script.
//
// It returns the FileInfo of the exact state it verified: the stat of the
// content-matched file, or of the fully written temp file (rename preserves
// inode, mtime, and mode). The memo fingerprints these so it can only ever
// vouch for a state this function actually checked or wrote.
//
// Content, mode, and fingerprint all come from ONE opened descriptor: a
// re-read by path could pair the content of one inode with the stat of a
// concurrent replacement renamed over it — during a rolling deployment the
// memo would then vouch for the other binary's hook as if verified, and
// stop repairing it (PR #98 round-1 review). The descriptor is stat'd
// BEFORE and AFTER the content read and both must match: an in-place
// writer on the same inode landing inside the read window would otherwise
// pair the pre-write content with the post-write fingerprint (PR #98
// round-9 review). Finally the path must still name that same unchanged
// inode for the fingerprint to be memoizable; any failure falls through
// and installs our own script, exactly as if the content had not matched.
func ensureHook(path, script string) (os.FileInfo, error) {
	if f, err := os.Open(path); err == nil {
		pre, perr := f.Stat()
		cur, rerr := io.ReadAll(f)
		if barrier := hookVerifyBarrier.Load(); barrier != nil {
			(*barrier)(path)
		}
		post, serr := f.Stat()
		_ = f.Close()
		if perr == nil && rerr == nil && serr == nil && string(cur) == script &&
			post.Mode().Perm() == 0o755 && fileUnchanged(pre, post) {
			if pathFI, err := os.Stat(path); err == nil && fileUnchanged(post, pathFI) {
				return post, nil
			}
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(script); err != nil {
		tmp.Close()
		return nil, err
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		return nil, err
	}
	// Fingerprint before close/rename: no writes follow, so this is the
	// final state the rename publishes.
	fi, err := tmp.Stat()
	if err != nil {
		tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return nil, err
	}
	// Rename preserves content, mtime, size, and mode but bumps ctime, so
	// re-stat the path for the settled fingerprint — only if it still
	// names the inode we wrote (SameFile). If a concurrent installer won
	// the race, return the pre-rename stat: its stale ctime simply fails
	// the next memo comparison, forcing a re-verify (fail closed).
	if pathFI, err := os.Stat(path); err == nil && os.SameFile(fi, pathFI) {
		return pathFI, nil
	}
	return fi, nil
}

// maxHydrateRaces bounds the "missing pack blob → re-ListPacks → retry"
// loop: a listed pack can legitimately vanish from blob storage between
// ListPacks and download (folded away by a repack), and the fresh listing
// then carries its replacement. A blob still missing after the bound is
// genuinely gone, not racing.
const maxHydrateRaces = 8

// ErrPackMissing is returned (wrapped, match with errors.Is) when a
// registered pack's blob stays missing after maxHydrateRaces listing passes:
// the ref store references bytes that do not exist, which is corruption
// grade — refs may point at unreachable objects.
var ErrPackMissing = errors.New("githttp: registered pack missing from blob store")

// HydratePacks downloads and indexes every registered pack that is not
// already present in the cache repo. Packs are self-contained (index-pack
// --fix-thin ran on the receiving node before upload), so each is
// independently indexable.
//
// Concurrent hydrators (serve requests, re-exec'd hooks) race on the same
// cache dir, so each pack is materialized under a per-pack flock: download
// to a uniquely-named temp file, index the temp, then rename pack+idx into
// place. Only our own temp names are ever removed, and "idx appeared while
// waiting for the lock" counts as success.
//
// A download hitting blob.ErrNotFound restarts the pass on a fresh ListPacks
// (bounded by maxHydrateRaces): the pack was folded away by a concurrent
// repack and the new listing carries its replacement. Restarts are cheap and
// idempotent — hydrated packs are skipped by the .idx probe. Transport
// errors keep failing fast; on bound exhaustion the hydrate fails with
// ErrPackMissing rather than looping.
func HydratePacks(ctx context.Context, st ref.RefStore, bl blob.BlobStore, repoPath, repo string) error {
	return hydratePacksBy(ctx, bl, repoPath, repo, func(ctx context.Context) ([]ref.Pack, error) {
		return st.ListPacks(ctx, repo)
	})
}

// hydratePacksBy is HydratePacks over an injectable pack-listing source:
// the portable path re-lists via ListPacks, the snapshot path (issue #182)
// re-reads a whole hydrate snapshot so the retry stays bound to one
// coherent state.
func hydratePacksBy(ctx context.Context, bl blob.BlobStore, repoPath, repo string, list func(ctx context.Context) ([]ref.Pack, error)) error {
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		return err
	}
	var lastMissing error
	for range maxHydrateRaces {
		packs, err := list(ctx)
		if err != nil {
			return err
		}
		lastMissing = nil
		for _, p := range packs {
			base := filepath.Join(packDir, "pack-"+p.ID)
			if _, err := os.Stat(base + ".idx"); err == nil {
				continue // already hydrated
			}
			if err := hydrateOnePack(ctx, bl, repoPath, base, p); err != nil {
				if !errors.Is(err, blob.ErrNotFound) {
					return err
				}
				lastMissing = fmt.Errorf("pack %s: %w", p.ID, err)
				break
			}
		}
		if lastMissing == nil {
			return nil
		}
	}
	return fmt.Errorf("hydrate %s: %w after %d attempts: %v", repo, ErrPackMissing, maxHydrateRaces, lastMissing)
}

// hydrateOnePack materializes one registered pack (pack + idx) at base under
// an exclusive flock on <base>.pack.lock. The lock file is left in place:
// removing it would race a concurrent locker onto a deleted inode. Both the
// lock wait and the index-pack subprocess honor ctx: hydration runs under
// request contexts and the repack fold's wall-clock budget, and neither a
// peer's slow download nor indexing a large pack may outlive them.
func hydrateOnePack(ctx context.Context, bl blob.BlobStore, repoPath, base string, p ref.Pack) error {
	unlock, err := flockExclusiveCtx(ctx, base+".pack.lock")
	if err != nil {
		return fmt.Errorf("lock pack %s: %w", p.ID, err)
	}
	defer unlock()
	// Re-check under the lock: a concurrent hydrator may have finished the
	// job while we were waiting.
	if _, err := os.Stat(base + ".idx"); err == nil {
		return nil
	}
	// index-pack derives the idx name by replacing the .pack suffix, so the
	// temp file must end in .pack.
	tmp, err := os.CreateTemp(filepath.Dir(base), filepath.Base(base)+".tmp-*.pack")
	if err != nil {
		return err
	}
	tmpPack := tmp.Name()
	tmpBase := strings.TrimSuffix(tmpPack, ".pack")
	tmpIdx := tmpBase + ".idx"
	tmpRev := tmpBase + ".rev" // reverse index, emitted by modern git
	_ = tmp.Close()            // DownloadFile writes by path (tmp + rename)
	defer func() {
		// Best-effort cleanup of OUR temp names only; no-ops after the
		// renames below succeeded.
		_ = os.Remove(tmpPack)
		_ = os.Remove(tmpIdx)
		_ = os.Remove(tmpRev)
	}()
	if err := bl.DownloadFile(ctx, p.Key, tmpPack); err != nil {
		return err
	}
	if out, err := runGitCtx(ctx, repoPath, "index-pack", tmpPack); err != nil {
		// Corrupt/partial download: the deferred cleanup removes the temp
		// files, so a retry re-downloads fresh bytes.
		return fmt.Errorf("index-pack %s: %w (%s)", p.ID, err, strings.TrimSpace(out))
	}
	// Pack first, idx LAST: the "already hydrated" probe keys on the idx,
	// so a visible idx implies a complete pack. A crash in between leaves
	// partial files the next hydrator overwrites under the lock.
	if err := os.Rename(tmpPack, base+".pack"); err != nil {
		return err
	}
	// The reverse index is optional (config/version dependent); move it
	// along when present.
	if _, err := os.Stat(tmpRev); err == nil {
		if err := os.Rename(tmpRev, base+".rev"); err != nil {
			return err
		}
	}
	if err := os.Rename(tmpIdx, base+".idx"); err != nil {
		return err
	}
	return nil
}

// MaterializePack materializes ONE registered pack into repoPath — the
// bounded, cancelable alternative to HydratePacks for callers that must
// not touch any other pack (the issue-#1 repack fold selects its input
// under byte/object budgets BEFORE downloading; hydrating the whole repo
// first would defeat them). Safe against concurrent hydrators of the same
// cache repo: identical per-pack lock, temp naming, and rename ordering. A
// download hitting blob.ErrNotFound propagates (wrapped, match with
// errors.Is): for a fold it means a concurrent repack already replaced the
// snapshotted rows.
func MaterializePack(ctx context.Context, bl blob.BlobStore, repoPath string, p ref.Pack) error {
	assertf(p.ID != "" && p.Key != "", "MaterializePack: empty pack id/key %+v", p)
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		return err
	}
	base := filepath.Join(packDir, "pack-"+p.ID)
	if _, err := os.Stat(base + ".idx"); err == nil {
		return nil // already materialized
	}
	return hydrateOnePack(ctx, bl, repoPath, base, p)
}

// flockExclusive takes a blocking exclusive flock on path (creating it if
// needed) and returns the release func.
func flockExclusive(path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		// Close releases the flock with the descriptor.
		_ = f.Close()
	}, nil
}

// flockExclusiveCtx is flockExclusive with a cancelable wait: LOCK_NB
// attempts polled against ctx. The repo-wide hydrate lock can be held
// across slow blob downloads, and a blocking LOCK_EX would pin every
// goroutine whose client already hung up behind it until the holder
// finishes (PR #14 review). The per-pack locks stay blocking: their
// critical sections are one pack's download, and their waiters overlap
// that same work.
func flockExclusiveCtx(ctx context.Context, path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = f.Close() }, nil
		}
		// EWOULDBLOCK (== EAGAIN): held by someone else. EINTR: attempt
		// interrupted by a signal. Both keep polling; anything else is a
		// real failure.
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN && err != syscall.EINTR {
			_ = f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// HeadTarget qualifies a repo's recorded default branch into the HEAD
// symref target its cache repo advertises (openapi.yaml PATCH repo: `main`
// becomes `refs/heads/main`; a value that already starts with `refs/` is
// used verbatim).
func HeadTarget(defaultBranch string) string {
	assertf(defaultBranch != "", "HeadTarget: empty default branch")
	assertf(!strings.ContainsAny(defaultBranch, " \t\n"), "HeadTarget: default branch %q contains whitespace", defaultBranch)
	if strings.HasPrefix(defaultBranch, "refs/") {
		return defaultBranch
	}
	return "refs/heads/" + defaultBranch
}

// syncRefsBatch bounds ref synchronization's working set (issue #119): the
// store enumeration page size, the update-ref transaction size, and the
// in-memory update spool are all capped at this many refs, so a full sync
// over attacker-scaled ref fanout holds O(batch) heap instead of
// materializing the repo's whole ref set. Compiled-in like the repack
// budgets: it prices a structural bound, not a tunable policy.
const syncRefsBatch = 10_000

// refSeq is a pull iterator over (name, sha) rows in strictly ascending
// byte order of name — the merge-join order shared by ListRefsAfter pages
// and for-each-ref output. next returns ok=false at end of stream.
type refSeq interface {
	next() (name, sha string, ok bool, err error)
}

// storeRefSeq streams a repo's refs from the store in syncRefsBatch-sized
// keyset pages. Each page is one consistent snapshot; cross-page tearing
// under concurrent mutation is healed by the generation marker (any
// mutation bumps the generation past the value this sync publishes, so the
// next hydrate takes the full path again — see ref.RefStore.ListRefsAfter).
type storeRefSeq struct {
	ctx      context.Context
	st       ref.RefStore
	repo     string
	pageSize int
	page     []ref.RefEntry
	i        int
	last     string // keyset cursor: last name yielded
	done     bool
}

// refScanError marks a store-side enumeration failure inside the streaming
// sync. Hydrate surfaces it as an infrastructure error (err), never as a
// ref-application failure (refsErr): the receive path tolerates refsErr as
// cache staleness git re-validates, and a store outage is not staleness —
// exactly the split the pre-#119 code had between ListRefs and SyncRefs
// errors.
type refScanError struct{ err error }

func (e *refScanError) Error() string { return e.err.Error() }
func (e *refScanError) Unwrap() error { return e.err }

// entryRefSeq yields a hydrate snapshot's materialized ref entries —
// already in the ascending byte order storeRefSeq pages in (the
// ref.HydrateSnapshot contract).
type entryRefSeq struct {
	refs []ref.RefEntry
	i    int
}

func (s *entryRefSeq) next() (string, string, bool, error) {
	if s.i >= len(s.refs) {
		return "", "", false, nil
	}
	e := s.refs[s.i]
	s.i++
	return e.Name, e.SHA, true, nil
}

func (s *storeRefSeq) next() (string, string, bool, error) {
	if s.i >= len(s.page) {
		if s.done {
			return "", "", false, nil
		}
		assertf(s.pageSize > 0, "storeRefSeq: non-positive page size %d", s.pageSize)
		page, err := s.st.ListRefsAfter(s.ctx, s.repo, s.last, s.pageSize)
		if err != nil {
			return "", "", false, &refScanError{err: err}
		}
		s.page, s.i = page, 0
		s.done = len(page) < s.pageSize
		if len(page) == 0 {
			return "", "", false, nil
		}
	}
	e := s.page[s.i]
	s.i++
	// Cross-page monotonicity is this layer's stitching invariant: a
	// page regressing below the cursor could re-yield or skip rows.
	assertf(e.Name > s.last, "ListRefsAfter yielded %q not above cursor %q", e.Name, s.last)
	s.last = e.Name
	return e.Name, e.SHA, true, nil
}

// sliceRefSeq yields a pre-sorted in-memory ref set (tests).
type sliceRefSeq struct {
	names []string
	shas  map[string]string
	i     int
}

func (s *sliceRefSeq) next() (string, string, bool, error) {
	if s.i >= len(s.names) {
		return "", "", false, nil
	}
	name := s.names[s.i]
	s.i++
	return name, s.shas[name], true, nil
}

// localRefSeq streams `git for-each-ref` output without materializing it:
// the local cache repo's ref count equals the store's fanout, so buffering
// the whole listing would reintroduce the unbounded materialization this
// sync exists to avoid (issue #119). for-each-ref's default refname sort is
// strcmp order — the same byte order storeRefSeq pages in — asserted row by
// row. Stderr is captured separately so git warnings never contaminate the
// parsed stream.
type localRefSeq struct {
	cmd    *exec.Cmd
	sc     *bufio.Scanner
	stderr bytes.Buffer
	prev   string
	waited bool
}

func startLocalRefSeq(ctx context.Context, repoPath string) (*localRefSeq, error) {
	l := &localRefSeq{}
	args := []string{"for-each-ref", "--format=%(refname) %(objectname)"}
	observeGitSpawn(repoPath, args)
	l.cmd = exec.CommandContext(ctx, "git", args...)
	l.cmd.Dir = repoPath
	l.cmd.Stderr = &l.stderr
	stdout, err := l.cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := gitlimit.StartIn(ctx, repoPath, l.cmd); err != nil {
		return nil, fmt.Errorf("git for-each-ref: %w", err)
	}
	l.sc = bufio.NewScanner(stdout)
	// Refnames are practically short, but the scanner's default 64KiB max
	// token would fail loudly rather than mis-parse; give it headroom.
	l.sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	return l, nil
}

func (l *localRefSeq) next() (string, string, bool, error) {
	if l.sc.Scan() {
		line := l.sc.Text()
		name, sha, ok := strings.Cut(line, " ")
		assertf(ok && ref.ValidSHA(sha), "for-each-ref emitted %q", line)
		assertf(name > l.prev, "for-each-ref order regressed: %q after %q", name, l.prev)
		l.prev = name
		return name, sha, true, nil
	}
	if err := l.sc.Err(); err != nil {
		return "", "", false, fmt.Errorf("git for-each-ref: %w", err)
	}
	l.waited = true
	if err := gitlimit.Wait(l.cmd); err != nil {
		return "", "", false, fmt.Errorf("git for-each-ref: %w: %s", err, strings.TrimSpace(l.stderr.String()))
	}
	return "", "", false, nil
}

// close reaps the subprocess after an early exit; a no-op once next hit
// EOF and waited.
func (l *localRefSeq) close() {
	if l.waited {
		return
	}
	l.waited = true
	_ = l.cmd.Process.Kill()
	_ = gitlimit.Wait(l.cmd)
}

// refCmd is one update-ref --stdin command plus the ref name it targets,
// kept structured so flush can filter push-held refs (issue #170).
type refCmd struct{ name, line string }

// refCmdBatch accumulates update-ref --stdin commands and flushes them in
// syncRefsBatch-sized transactions. Each flush takes ONE fresh snapshot of
// the repo's live push holds and drops commands whose ref a hold covers,
// recording skipped: every command in the batch was produced by a store
// read strictly before the flush, so a command carrying a push's committed
// value has that push's hold — appended before its CAS — visible in the
// snapshot unless the push already finished (then re-applying is a no-op).
// One snapshot per flush keeps the hold cost O(flushes), not O(changed
// refs) — a cold million-ref hydration must not rescan the hold directory
// per ref (Codex P2 on #173).
type refCmdBatch struct {
	repoPath string
	cap      int
	cmds     []refCmd
	skipped  bool
	holds    *pushHoldDirCache // lazily built by flush, reused across flushes
}

func (b *refCmdBatch) add(name, line string) error {
	assertf(b.cap > 0, "refCmdBatch: non-positive cap %d", b.cap)
	b.cmds = append(b.cmds, refCmd{name: name, line: line})
	if len(b.cmds) < b.cap {
		return nil
	}
	return b.flush()
}

func (b *refCmdBatch) flush() error {
	if len(b.cmds) == 0 {
		return nil
	}
	// One parse cache per batch lifetime (Codex round-11 P2): a large full
	// sync flushes every syncRefsBatch commands, and loading holds fresh
	// per flush multiplies held-ref bytes by the flush count — a
	// repo-scale hold overlapping a repo-scale sync re-parses gigabytes.
	// The cache re-reads a hold file only when its (size, mtime) changes;
	// hydration stays FAIL-OPEN on read errors (reservation fails closed).
	if b.holds == nil {
		b.holds = &pushHoldDirCache{dir: filepath.Join(b.repoPath, pushHoldDir)}
	}
	holds, _ := b.holds.load()
	var sb strings.Builder
	var kept []string
	for _, c := range b.cmds {
		if holds.covers(c.name) {
			b.skipped = true
			continue
		}
		sb.WriteString(c.line)
		kept = append(kept, c.name)
	}
	b.cmds = b.cmds[:0]
	if len(kept) == 0 {
		return nil
	}
	if err := updateRefStdin(b.repoPath, sb.String()); err != nil {
		// A git killed inside its ref transaction leaves a lock that fails
		// every later batch for that ref (issue #464). The for-each-ref
		// child of this pass is alive and holds the lease, so the lock
		// cannot be removed here: hydrateRefs removes it once the pass has
		// returned and its children are reaped.
		return &refBatchError{names: kept, err: err}
	}
	return nil
}

// refBatchError marks a failed update-ref batch. names are the refs of the
// batch, from which hydrateRefs finds the ref locks that can have failed it.
type refBatchError struct {
	names []string
	err   error
}

func (e *refBatchError) Error() string { return e.err.Error() }
func (e *refBatchError) Unwrap() error { return e.err }

// syncRefsWithCleanup runs syncRefsStreamed up to three times, because a
// racing writer can fail a pass. A store-side scan failure is an
// infrastructure error, not local ref staleness: it is returned as scanErr at
// once (no marker, no retries: the store is down, not the cache racing).
//
// A ref lock that a killed git left fails every batch for its ref (issue
// #490), and one killed transaction can leave locks in several batches. When
// a pass fails on a batch, both of its sync passes have returned and their
// children are reaped, and the caller holds the repository's hydrate lock, so
// a cleanup that wins the lease exclusively can remove the locks of that
// batch. The cleanup runs after every failed attempt, and the three attempts
// bound the work: a hydration cleans the failed batch of each attempt, and a
// repository with stale locks in more batches heals over the next requests.
// Nothing here remembers a batch, and no cleanup earns an attempt.
func syncRefsWithCleanup(ctx context.Context, repoPath, headTarget string, newStore func() refSeq, batch int) (skipped bool, refsErr, scanErr error) {
	for attempt := 0; attempt < 3; attempt++ {
		if skipped, refsErr = syncRefsStreamed(ctx, repoPath, headTarget, newStore, batch); refsErr == nil {
			return skipped, nil, nil
		}
		if scan, ok := errors.AsType[*refScanError](refsErr); ok {
			return skipped, refsErr, scan.Unwrap()
		}
		if failed, ok := errors.AsType[*refBatchError](refsErr); ok {
			// The cleanup logs and counts its own result, an error included.
			_, _ = cleanStaleRefLocks(repoPath, failed.names, time.Now())
		}
		time.Sleep(50 * time.Millisecond)
	}
	return skipped, refsErr, nil
}

// syncRefsStreamed makes the local ref store match the given stream exactly
// (the store wins): updates changed refs, deletes locals the store does not
// know, and points HEAD at headTarget — in O(syncRefsBatch) memory.
//
// One merge-join pass over the two byte-ordered streams detects both
// deletions and updates. Deletions flush in batches DURING the pass;
// updates spool and flush strictly AFTER it, because all deletions must
// precede all updates: git's files backend cannot delete refs/heads/foo and
// create refs/heads/foo/bar in one transaction (the doomed loose ref still
// blocks the new ref's lock file), so a D/F rename in the store would wedge
// local sync forever otherwise. When the update spool overflows one batch
// (mass change: a cold cache's first hydration), the pass completes
// deletions only and a SECOND pass over fresh streams applies updates in
// batches — still strictly after every deletion. Batches are separate
// update-ref transactions; partial application is fine — the sync is
// idempotent over a rebuildable cache and callers retry.
//
// Commands covered by a push hold (issue #170, see refCmdBatch.flush) are
// skipped in both passes and reported as skipped=true: a receive-pack on
// this node is mid-way between its committed authority CAS and its own
// local update of that ref, and a sync touching it (or a D/F relative of
// it) would make git reject the push. Callers must not let any success
// marker vouch for a sync that skipped a ref.
func syncRefsStreamed(ctx context.Context, repoPath, headTarget string, newStore func() refSeq, batch int) (skipped bool, err error) {
	assertf(strings.HasPrefix(headTarget, "refs/"), "syncRefsStreamed: unqualified HEAD target %q", headTarget)
	assertf(batch > 0, "syncRefsStreamed: non-positive batch %d", batch)
	overflow, skipped, err := syncRefsPassOne(ctx, repoPath, newStore(), batch)
	if err != nil {
		return skipped, err
	}
	if overflow {
		s2, err := syncRefsPassUpdates(ctx, repoPath, newStore(), batch)
		skipped = skipped || s2
		if err != nil {
			return skipped, err
		}
	}
	// HEAD follows the meta row's default branch (issue #18). git tolerates
	// a dangling target: an unborn default branch advertises no HEAD.
	if _, err := runGitCtx(ctx, repoPath, "symbolic-ref", "HEAD", headTarget); err != nil {
		return skipped, err
	}
	return skipped, nil
}

// syncRefsPassOne merge-joins the store stream against the local listing:
// deletions flush in batches during the pass; updates spool up to one batch
// and flush after every deletion has been applied. Returns overflow=true
// when the spool capacity was exceeded — updates were then discarded and
// the caller owns running syncRefsPassUpdates. Push-hold filtering happens
// at flush time (refCmdBatch.flush), which is strictly after every store
// read that produced a flushed command — the ordering the push-hold proof
// requires.
func syncRefsPassOne(ctx context.Context, repoPath string, store refSeq, batch int) (overflow, skipped bool, err error) {
	local, err := startLocalRefSeq(ctx, repoPath)
	if err != nil {
		return false, false, err
	}
	defer local.close()
	dels := &refCmdBatch{repoPath: repoPath, cap: batch}
	var upds []refCmd
	spool := func(name, sha string) {
		if overflow {
			return
		}
		if len(upds) == batch {
			overflow, upds = true, nil
			return
		}
		upds = append(upds, refCmd{name: name, line: fmt.Sprintf("update %s %s\n", name, sha)})
	}
	var ln, ls string
	var lok bool
	var lerr error
	advance := func() bool {
		ln, ls, lok, lerr = local.next()
		return lerr == nil
	}
	if !advance() {
		return false, false, lerr
	}
	for {
		sn, ss, sok, serr := store.next()
		if serr != nil {
			return false, dels.skipped, serr
		}
		if !sok {
			break
		}
		for lok && ln < sn {
			if err := dels.add(ln, fmt.Sprintf("delete %s\n", ln)); err != nil {
				return false, dels.skipped, err
			}
			if !advance() {
				return false, dels.skipped, lerr
			}
		}
		if lok && ln == sn {
			if ls != ss {
				spool(sn, ss)
			}
			if !advance() {
				return false, dels.skipped, lerr
			}
		} else {
			spool(sn, ss)
		}
	}
	for lok {
		if err := dels.add(ln, fmt.Sprintf("delete %s\n", ln)); err != nil {
			return false, dels.skipped, err
		}
		if !advance() {
			return false, dels.skipped, lerr
		}
	}
	if err := dels.flush(); err != nil {
		return false, dels.skipped, err
	}
	if overflow {
		return true, dels.skipped, nil
	}
	for _, cmd := range upds {
		// Under the spool cap by construction: one flush at the end.
		if err := dels.add(cmd.name, cmd.line); err != nil {
			return false, dels.skipped, err
		}
	}
	err = dels.flush()
	return false, dels.skipped, err
}

// syncRefsPassUpdates applies store rows that are missing or changed
// locally, in batches, over fresh streams (the local listing reflects pass
// one's deletions). Local-only rows are NOT deleted here: interleaving
// delete and update batches would break the all-deletions-first ordering,
// and a row deleted from the store between passes is healed by the next
// full sync (its deletion bumped the generation past this sync's marker).
// Push-held refs are dropped at flush time like in pass one (skipped=true).
func syncRefsPassUpdates(ctx context.Context, repoPath string, store refSeq, batch int) (skipped bool, err error) {
	local, err := startLocalRefSeq(ctx, repoPath)
	if err != nil {
		return false, err
	}
	defer local.close()
	upds := &refCmdBatch{repoPath: repoPath, cap: batch}
	add := func(name, sha string) error {
		return upds.add(name, fmt.Sprintf("update %s %s\n", name, sha))
	}
	ln, ls, lok, lerr := local.next()
	if lerr != nil {
		return false, lerr
	}
	for {
		sn, ss, sok, serr := store.next()
		if serr != nil {
			return upds.skipped, serr
		}
		if !sok {
			break
		}
		for lok && ln < sn {
			if ln, ls, lok, lerr = local.next(); lerr != nil {
				return upds.skipped, lerr
			}
		}
		if lok && ln == sn {
			if ls != ss {
				if err := add(sn, ss); err != nil {
					return upds.skipped, err
				}
			}
			if ln, ls, lok, lerr = local.next(); lerr != nil {
				return upds.skipped, lerr
			}
		} else if err := add(sn, ss); err != nil {
			return upds.skipped, err
		}
	}
	err = upds.flush()
	return upds.skipped, err
}

// updateRefStdin feeds commands to one `git update-ref --stdin` transaction.
func updateRefStdin(repoPath, commands string) error {
	observeGitSpawn(repoPath, []string{"update-ref", "--stdin"})
	cmd := exec.Command("git", "update-ref", "--stdin")
	cmd.Dir = repoPath
	cmd.Stdin = strings.NewReader(commands)
	if out, err := gitlimit.CombinedOutputIn(context.Background(), repoPath, cmd); err != nil {
		return fmt.Errorf("update-ref --stdin: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Push holds (issue #170): receive-pack performs its own local ref updates
// in the shared cache repo only AFTER its update hook has committed the
// authority CAS, so a concurrent request's full sync can materialize the
// just-committed value first — and git then rejects the push ("cannot lock
// ref ... reference already exists" on creation, a failed old-value check
// otherwise) even though the authority accepted it. The bridge across that
// gap is a per-push hold file under <repoPath>/codestorage.push-holds/:
// the server mints the path (EnvPushHold), pre-receive reserves EVERY ref
// of the push in it before any update-hook CAS can run
// (reservePushHolds), and the server removes the file once receive-pack
// exits. Full syncs skip commands naming a held ref and suppress their
// generation marker, so no marker can vouch for a sync that skipped a ref
// (the next full sync heals it).
//
// Safety: the holds are durably in the file before any CAS commits, so any
// store page that can reveal a pushed value is read after the hold
// exists — and every sync command is hold-checked at batch flush, after
// the page read that produced it. A hold observed missing therefore means
// either the value predates the push (safe to apply) or receive-pack
// already exited (its local update landed; re-applying the store value is
// a no-op). Hold windows are server-controlled — hooks only run after the
// client's transfer completed — and the TTL bounds crash leaks: readers
// ignore expired files, minting sweeps them, and every live hook refreshes
// the file so a live push never expires. Tradeoff: while a ref is held,
// same-node advertisements omit its in-flight value, so a racing push of
// the SAME ref sees a slightly stale base and fails with the ordinary CAS
// conflict instead of fast-forwarding — the normal concurrent-writer path,
// clean and retryable, unlike the spurious rejection this prevents.
const (
	// Shared-cache protocol name, not branding: rolling deployments must
	// observe the same holds rather than hydrate over another binary's push.
	pushHoldDir = "codestorage.push-holds"
	// pushHoldUnreservedSuffix marks a push's sidecar DIRECTORY holding
	// one empty marker file per command pre-receive did NOT bulk-reserve
	// (unupdatable names, policy-denied refs, authority-doomed creations),
	// keyed by the ref name's SHA-256 so an update hook's membership check
	// is one O(1) stat — a listing file would be re-read once per allowed
	// hook, O(allowed × denied) across a crafted mixed push (Codex round-7
	// security P2 on #173). Hold READERS must skip it — its names are
	// exactly the ones that must not be treated as held.
	pushHoldUnreservedSuffix = ".unreserved"
	// pushHoldVerifiedSuffix marks a push's sidecar FILE of sorted
	// SHA-256 digests, one per command whose full pre-receive evaluation
	// PASSED, each keyed over (ref, old, new) — the update hook consumes
	// the verdict with an O(log n) binary search instead of charging the
	// shared verify-sig walk budget a second per-evaluation fee (Codex
	// round-16 P2 on #173). One fixed-width file, not one marker inode
	// per verdict (round-19 P2: the per-inode form needed a cap, and
	// verdicts dropped at the cap re-charged exactly the pushes big
	// enough to need them). Hold READERS must skip it.
	pushHoldVerifiedSuffix = ".verified"
	// pushHoldScanSuffix names the sidecar accruing the push's cumulative
	// peer-hold scan bytes (see maxPushPeerScanBytes).
	pushHoldScanSuffix = ".scanwork"
	// pushHoldReapSuffix marks a hold file OBSERVED past its wall-clock
	// TTL: expiry needs a second, later observation (see pushHoldLive),
	// and the marker's own mtime is the clock that separates the two.
	pushHoldReapSuffix = ".reap"
	// pushHoldReleasedSuffix names a hold's tombstone sidecar (issue #186
	// findings 1+6): an append-only list of exact ref names this push no
	// longer reserves — committed non-atomic refs (released by the
	// reference-transaction hook the moment git's per-command local
	// transaction commits) and denied refs (released by their own update
	// hook). Readers subtract these names from the hold's coverage;
	// appends are O(1), replacing the whole-file rewrite that made an
	// all-denied fanout O(N²) in disk I/O.
	pushHoldReleasedSuffix = ".released"
	// pushHoldGensSuffix names a hold's claimed-generation sidecar (issue
	// #186 finding 5): an append-only list of the exact store generations
	// this push's won authority CASes produced (CASRefGen attribution).
	// heldMarkerValid's range rule consults the LIVE holds' claims to let
	// a held marker also vouch for later generations fully attributed to
	// still-pending pushes; any unclaimed generation — a REST writer,
	// another replica, a completed push — forces the healing rescan.
	// Appends are best-effort O(1): a lost claim only costs coalescing.
	pushHoldGensSuffix = ".gens"
)

// maxPushHoldGensBytes hard-caps one hold's claimed-generation sidecar
// read during held-marker validation (issue #186 finding 5): ~50k claims
// per hold at 1 MiB. Over the cap the hold's range reads as unclaimed —
// bounded cost, and the failure direction is a healing rescan, never a
// stale skip.
const maxPushHoldGensBytes = 1 << 20

// isPushHoldSidecar reports whether a hold-directory entry name is one of
// a hold's sidecar artifacts rather than a hold file itself.
func isPushHoldSidecar(name string) bool {
	return strings.HasSuffix(name, pushHoldUnreservedSuffix) ||
		strings.HasSuffix(name, pushHoldVerifiedSuffix) ||
		strings.HasSuffix(name, pushHoldScanSuffix) ||
		strings.HasSuffix(name, pushHoldReapSuffix) ||
		strings.HasSuffix(name, pushHoldReleasedSuffix) ||
		strings.HasSuffix(name, pushHoldGensSuffix)
}

// removePushHoldFiles releases a push's hold file together with every
// sidecar artifact — the ONE list every release site shares (post-receive,
// the handler fallback, the mint sweep's expired branch, and an atomic
// denial), so a newly added sidecar cannot silently leak from one of them.
// Best-effort like every caller: a failed removal leaves the TTL machinery
// the bound.
func removePushHoldFiles(holdPath string) {
	// Serialize with the reaper's quarantine window (Codex round-36
	// security P2): a release racing it sees the hold path ABSENT,
	// removes only sidecars, and the reaper's restore then resurrects a
	// hold post-receive already released — kept alive indefinitely by
	// the handler refresher while a non-reading client stalls the
	// response copy. Under the reservation flock the release runs
	// strictly before the window (normal removal) or strictly after it
	// (removing the restored hold, or a no-op if the reaper unlinked).
	// Best-effort like the removals below: without the lock the TTL
	// machinery remains the bound.
	if lock, err := flockExclusive(filepath.Dir(holdPath) + ".reserve.lock"); err == nil {
		defer lock()
	}
	if _, err := os.Lstat(holdPath); err == nil {
		// Bump only when a hold FILE is actually being removed (Codex
		// round-33 security P2): every receive-pack mints a hold PATH,
		// but rejected or malformed RPCs may never create the file, and
		// this release still runs for them — an unconditional bump would
		// let each tiny denied push invalidate the held-generation fast
		// path and re-buy the full pack listing and ref paging under the
		// hydrate lock for a live hold set that never changed. A
		// never-created hold was never in any fingerprint listing, and
		// sidecars are excluded from it.
		//
		// And bump BEFORE removing (Codex round-34 P2): a hold whose
		// fail-closed create bump failed is visible with an UNCHANGED
		// epoch, so letting this cleanup remove it with its own bump
		// failing for the same cause would complete a transient lifecycle
		// both fingerprint components are blind to — a hydration spanning
		// it could publish a held marker for a set that changed mid-scan.
		// A hold file therefore never transitions visible→absent without
		// a successful epoch bump: on failure everything stays (the file
		// keeps marking the fingerprint), and the TTL reap — which
		// enforces the same bump-first rule — retires it once the epoch
		// is writable again.
		if bumpPushHoldEpoch(filepath.Dir(holdPath)) != nil {
			return
		}
		_ = os.Remove(holdPath)
	}
	_ = os.RemoveAll(holdPath + pushHoldUnreservedSuffix)
	_ = os.RemoveAll(holdPath + pushHoldVerifiedSuffix)
	_ = os.Remove(holdPath + pushHoldScanSuffix)
	_ = os.Remove(holdPath + pushHoldReapSuffix)
	_ = os.Remove(holdPath + pushHoldReleasedSuffix)
	_ = os.Remove(holdPath + pushHoldGensSuffix)
}

// pushHoldLive reports whether one hold-file observation counts as live,
// deciding expiry across TWO observations instead of one wall-clock read
// (Codex round-25 P2 on #173): after a FORWARD clock step or a host
// suspend/resume, a LIVE push's hold looks ancient the moment the clock
// lands — readers would expose its in-flight refs to hydration and the
// next mint sweep would delete a file its refresher can only Chtimes,
// never recreate — even though no real time passed for the push. A
// wall-expired hold therefore first gains a reap marker stamped with the
// CURRENT observation time and stays fully live; it expires only when a
// later observation finds the marker itself older than TTL/2 with no
// owner touch after it. The marker's clock starts at the anomaly's
// discovery, so the grace measures real observed time: a suspended live
// push is revived by its refresher (TTL/4 cadence, well inside the
// grace), which advances the hold past the marker and re-arms suspicion
// afresh, while a genuine crash leak — whose refresher is gone — ages out
// within ~1.5 TTLs of its first observation, the same bound the
// backward-step clamp below already accepts. Far-FUTURE mtimes (backward
// steps, round-21/22) are still clamped to now and kept live.
func pushHoldLive(path string, mtime time.Time) bool {
	now := time.Now()
	age := now.Sub(mtime)
	if age < -pushHoldTTL {
		_ = os.Chtimes(path, now, now)
		return true
	}
	if age <= pushHoldTTL {
		return true
	}
	reap := path + pushHoldReapSuffix
	ri, err := os.Stat(reap)
	if err != nil || !ri.ModTime().After(mtime) {
		// No usable marker (none yet, or the owner touched the hold
		// after the last suspicion): (re)arm one and keep the hold
		// live. Concurrent observers racing here all stamp ~now.
		if f, cerr := os.OpenFile(reap, os.O_CREATE|os.O_WRONLY, 0o600); cerr == nil {
			_ = f.Close()
		}
		_ = os.Chtimes(reap, now, now)
		return true
	}
	if ri.ModTime().After(now) {
		// Future-dated marker (the wall clock stepped backward after the
		// marker was armed — e.g. a VM snapshot restore): clamp it to now
		// and keep the hold live, the exact mirror of the hold-file clamp
		// above (Codex round-27 P2 on #173: the subtraction below goes
		// NEGATIVE for a future marker, so a crash leak would stay
		// "within grace" — live, skipping hydration, timing out same-name
		// pushes — until wall time caught up with the marker). The clamp
		// restarts the grace at the anomaly's discovery, so the leak
		// still ages out within TTL/2 of this observation.
		_ = os.Chtimes(reap, now, now)
		return true
	}
	if now.Sub(ri.ModTime()) <= pushHoldTTL/2 {
		return true
	}
	// Final re-stat before the expiry verdict (Codex round-30 P2 on
	// #173): mtime came from the caller's earlier observation — a delayed
	// refresher (a second suspend, a long scheduler pause) may have
	// touched the hold between that stat and this verdict, and expiring a
	// NOW-fresh hold would let hydration touch its in-flight refs and the
	// mint sweep delete a file the refresher can only Chtimes, never
	// recreate. Expire only when a fresh stat still shows no owner touch;
	// a hold that vanished meanwhile was already released and stays
	// expired.
	if hi, err := os.Stat(path); err == nil && !hi.ModTime().Equal(mtime) {
		return true
	}
	return false
}

// pushHoldTTL bounds a crash-leaked hold's life. A variable only so tests
// can shrink it to observe the refresh machinery on test timescales;
// production never mutates it.
var pushHoldTTL = 5 * time.Minute

// pushHoldUnreservedMarker is the sidecar marker path recording that name
// was not bulk-reserved by pre-receive.
func pushHoldUnreservedMarker(holdPath, name string) string {
	sum := sha256.Sum256([]byte(name))
	return holdPath + pushHoldUnreservedSuffix + "/" + hex.EncodeToString(sum[:])
}

// pushVerifiedDigest keys one verified verdict: SHA-256 over the exact
// command (name, old, new), so a hook can only consume the verdict for
// the identical update.
func pushVerifiedDigest(name, old, new string) [sha256.Size]byte {
	return sha256.Sum256([]byte(name + "\x00" + old + "\x00" + new))
}

// writePushVerifiedVerdicts persists a push's passed evaluations as ONE
// sorted fixed-width digest file. EVERY pass is kept (Codex round-19 P2
// on #173: the former one-inode-per-verdict directory needed a marker
// cap, and commands past the cap silently lost their verdicts — their
// update hooks then re-charged the very walk allowance the verdicts exist
// to protect, rejecting exactly the large compliant fanouts that need
// them). Size is bounded by construction: 32 bytes per command, strictly
// smaller than the wire request that carried the commands. Written once
// by pre-receive, strictly before git runs any update hook, so readers
// never race the write.
func writePushVerifiedVerdicts(holdPath string, digests [][sha256.Size]byte) error {
	if len(digests) == 0 {
		return nil
	}
	// Anchor the sidecar to a live base hold (Codex round-35 P2): an
	// all-JIT push reserves no names, so no hold file exists yet — and a
	// PARENTLESS sidecar ages out on a single wall observation, letting a
	// suspend/clock step plus any concurrent mint sweep delete the
	// verdicts before the first hook consumed one. The empty hold covers
	// no names (no peer conflicts, no skip verdicts — no epoch bump
	// needed; its listing presence alone can only force conservative
	// revalidation) but it lives on the refresher's clock with the
	// two-observation expiry, and every release path already removes it
	// together with its sidecars. O_CREATE without O_TRUNC: a reserved
	// hold passes through untouched.
	f, err := os.OpenFile(holdPath, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	sort.Slice(digests, func(i, j int) bool { return bytes.Compare(digests[i][:], digests[j][:]) < 0 })
	buf := make([]byte, 0, len(digests)*sha256.Size)
	for _, d := range digests {
		buf = append(buf, d[:]...)
	}
	return os.WriteFile(holdPath+pushHoldVerifiedSuffix, buf, 0o600)
}

// pushVerifiedVerdict reports whether the exact command passed
// pre-receive's full policy evaluation, by binary search over the sorted
// digest file. Any trouble (no sidecar, torn size, read error) falls back
// closed to false — the hook then just re-evaluates for real.
func pushVerifiedVerdict(holdPath, name, old, new string) bool {
	f, err := os.Open(holdPath + pushHoldVerifiedSuffix)
	if err != nil {
		return false
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.Size()%sha256.Size != 0 {
		return false
	}
	want := pushVerifiedDigest(name, old, new)
	var rec [sha256.Size]byte
	lo, hi := int64(0), fi.Size()/sha256.Size
	for lo < hi {
		mid := (lo + hi) / 2
		if _, err := f.ReadAt(rec[:], mid*sha256.Size); err != nil {
			return false
		}
		switch bytes.Compare(want[:], rec[:]) {
		case 0:
			return true
		case -1:
			hi = mid
		default:
			lo = mid + 1
		}
	}
	return false
}

// ValidateCacheDir rejects cache locations too deep for the push-hold
// bookkeeping this package materializes beside each repository (Codex
// round-25 P2): the deepest auxiliary path — a hold's unreserved-marker
// sidecar entry — needs more room than git's own files, so a cache root
// that leaves hydration and git ref locking working can still fail every
// push with an opaque mid-push error. Validated once, where the operator's
// choice is accepted (serve boot), against the target OS PATH_MAX with a
// generous repo-component budget (repo directory names are
// server-generated and short); the per-request mint stays the defensive
// backstop for anything that slips through.
func ValidateCacheDir(cacheDir string) error {
	abs, err := filepath.Abs(cacheDir)
	if err != nil {
		return fmt.Errorf("cache dir %q: %w", cacheDir, err)
	}
	// <cacheDir>/<repo>.git/<hold dir>/push-<32 hex><.unreserved>/<64 hex> + NUL
	const repoComponentBudget = 128 // server-generated IDs plus suffixes are ~30 bytes
	deepest := len(abs) + 1 + repoComponentBudget + len(".git") +
		1 + len(pushHoldDir) +
		1 + len("push-") + 32 + len(pushHoldUnreservedSuffix) +
		1 + 2*sha256.Size +
		1 // NUL
	if deepest > pushRefPathMax {
		return fmt.Errorf("cache dir %q is too deep: push-hold bookkeeping paths would exceed the OS PATH_MAX budget (%d bytes); configure a shorter cache directory", abs, pushRefPathMax)
	}
	return nil
}

// mintPushHold returns the absolute path a receive-pack request's update
// hooks record their in-flight refs in, creating the hold directory and
// sweeping crash-leaked expired files. The file itself is created by the
// first update hook's append: minting one up front would start the TTL
// clock during the client-controlled pack transfer.
func mintPushHold(repoPath string) (string, error) {
	dir := filepath.Join(repoPath, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	sweepExpiredPushHolds(dir)
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	// Absolute: the hook subprocess resolves the path from a different
	// working directory than the serve process.
	return filepath.Abs(filepath.Join(dir, "push-"+hex.EncodeToString(buf[:])))
}

// sweepExpiredPushHolds removes hold files past the TTL — leaks from a
// crashed serve process (the normal cleanup is post-receive plus the
// request's release). Best-effort: readers already ignore expired files;
// this only bounds directory growth. Far-FUTURE mtimes (the wall clock
// stepped backward beneath the file — under a LIVE push, or a crash leak
// restored from a VM snapshot) are HEALED to now, never deleted (Codex
// round-21 P2, round-22 P1): a live push's file must not be yanked out
// from under its refresher, and from the clamp a leak ages out within
// one TTL of first observation instead of waiting for wall time to
// catch up.
func sweepExpiredPushHolds(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if !isPushHoldSidecar(e.Name()) && !e.IsDir() {
			// A hold file: reap only on the two-observation verdict
			// (pushHoldLive — a single wall read misfires after a
			// forward clock step, round-25), and take its sidecars
			// with it so nothing orphans.
			if !pushHoldLive(path, info.ModTime()) {
				reapExpiredPushHold(path, info.ModTime())
			}
			continue
		}
		// Sidecars live and die with their hold: while the hold FILE
		// exists, its own verdict above owns their cleanup (sweeping a
		// suspended live push's sidecar by wall age alone would strip
		// its unreserved markers and let sidecar hooks skip their
		// self-reservation). Only ORPHANS — crash windows between the
		// hold's removal and its sidecars' — age out here, on the same
		// clamped clock as before.
		if base, ok := pushHoldSidecarBase(e.Name()); ok {
			if _, err := os.Stat(filepath.Join(dir, base)); err == nil {
				continue
			}
		}
		switch age := time.Since(info.ModTime()); {
		case age > pushHoldTTL:
			// Orphan deletion runs under the reservation flock with the
			// parent re-checked at BOTH names (PR #195 round-14 P2): a
			// concurrent sweep's reap window renames the hold to
			// .reaping, and an unserialized age check would delete a
			// LIVE hold's tombstone sidecar as an orphan — a restore
			// then resurrects every released name until the push exits.
			// Reaps and restores hold this same lock, so under it the
			// parent's two names are stable; busy means a reap window is
			// active — skip, the next sweep retries.
			if base, ok := pushHoldSidecarBase(e.Name()); ok {
				lock, err := os.OpenFile(dir+".reserve.lock", os.O_CREATE|os.O_RDWR, 0o644)
				if err != nil {
					continue
				}
				if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
					_ = lock.Close()
					continue
				}
				// Confirmed ABSENCE only (PR #195 round-15 P2): a
				// transient stat error (EIO) is not proof the parent is
				// gone, and deleting a live hold's sidecar resurrects its
				// released names. Any non-ENOENT outcome preserves the
				// sidecar for the next sweep.
				_, baseErr := statFile(filepath.Join(dir, base))
				_, reapErr := statFile(filepath.Join(dir, base) + ".reaping")
				if errors.Is(baseErr, os.ErrNotExist) && errors.Is(reapErr, os.ErrNotExist) {
					// RemoveAll: unreserved sidecars are directories.
					_ = os.RemoveAll(path)
				}
				_ = lock.Close()
				continue
			}
			_ = os.RemoveAll(path)
		case age < -pushHoldTTL:
			now := time.Now()
			_ = os.Chtimes(path, now, now)
		}
	}
}

// Test seams: called (when non-nil) at interleaving points concurrency
// tests cannot otherwise reach deterministically. Nil in production.
var (
	testHookReapAfterQuarantine   func()
	testHookAppendReleasedMidGate func()
	testHookHoldsKeyMidSnapshot   func()
	testHookHoldsKeyPreClose      func()
	testHookHoldsKeyPostVerify    func()
	testHookRemoveHoldsPreRename  func()
	testHookRefreshBeforeLock     func()
	testHookRefTxPreInstall       func()
	testHookRefTxMidVerify        func()
	testHookPeerScanPostStat      func()
	testHookDirCachePreParse      func()
)

// reapExpiredPushHold removes an expiry-verdict hold with the verdict and
// the removal LINEARIZED at one rename (Codex round-31 P2 on #173): the
// two-observation verdict and the unlink used to be separate filesystem
// operations, so a refresher resuming between them could touch the hold
// and still lose it to the sweep — and refreshers can only Chtimes, never
// recreate a reservation. The hold is renamed (atomically; mtimes travel
// with the inode) to a quarantine name that readers still treat as a HOLD
// file, so its names stay covered throughout; the verdict is then
// revalidated on the quarantined inode itself. Any owner touch that
// preceded the rename is visible there — refreshers touch through an open
// fd, following the inode into quarantine — and restores the hold
// untouched. Only a verifiably untouched leak is unlinked, together with
// its sidecars. Crash leftovers behave as ordinary leaked holds and age
// out through the same two-observation expiry.
func reapExpiredPushHold(path string, observed time.Time) {
	// The whole quarantine→verdict→restore/unlink window runs under the
	// reservation flock that appendPushHolds' callers serialize on (Codex
	// round-33 P1 on #173/#186): the quarantine rename removes the hold
	// PATH, so a JIT or sidecar hook's reservePushHolds could otherwise
	// recreate it with a NEW reservation that the restore rename below
	// silently replaces — the hook then commits its CAS believing the ref
	// is held, reopening the exact race this machinery closes. Try-lock
	// only: a busy lock means reservations are in flight, so skip — the
	// reap is best-effort and the next mint sweep retries.
	lock, err := os.OpenFile(filepath.Dir(path)+".reserve.lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return
	}
	defer func() { _ = lock.Close() }() // close releases the flock
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return
	}
	quarantine := path + ".reaping"
	if err := os.Rename(path, quarantine); err != nil {
		return // released or already quarantined by a concurrent sweep
	}
	if testHookReapAfterQuarantine != nil {
		testHookReapAfterQuarantine()
	}
	restore := func() { _ = os.Rename(quarantine, path) }
	q, err := os.OpenFile(quarantine, os.O_WRONLY, 0)
	if err != nil {
		restore()
		return
	}
	defer func() { _ = q.Close() }()
	// Owner-liveness gate (Codex round-32 P2): every live refresher holds
	// a SHARED flock on its hold's inode, so winning an exclusive
	// non-blocking flock here proves no fd-owning refresher exists at
	// this instant — kernel-arbitrated, closing the residual stat-to-
	// unlink interval in which a resumed refresher's fd touch could
	// still be discarded. Failing the lock means the owner is alive:
	// restore, fail LIVE.
	if err := syscall.Flock(int(q.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		restore()
		return
	}
	// With the exclusive lock held, revalidate on the locked inode: any
	// owner touch that landed before the lock left a newer mtime (fd
	// touches follow the inode through the rename; path touches can only
	// precede it) and restores the hold.
	if qi, err := q.Stat(); err != nil || !qi.ModTime().Equal(observed) {
		restore()
		return
	}
	// Bump BEFORE the unlink (Codex round-34 P2, mirroring
	// removePushHoldFiles): a hold file never transitions visible→absent
	// without a successful epoch bump, or a transient lifecycle whose
	// create bump also failed becomes invisible to the fingerprint. When
	// the epoch cannot record the transition, restore the hold — it keeps
	// marking the fingerprint by presence, and a later sweep retries.
	if bumpPushHoldEpoch(filepath.Dir(path)) != nil {
		restore()
		return
	}
	_ = os.Remove(quarantine)
	_ = os.RemoveAll(path + pushHoldUnreservedSuffix)
	_ = os.RemoveAll(path + pushHoldVerifiedSuffix)
	_ = os.Remove(path + pushHoldScanSuffix)
	_ = os.Remove(path + pushHoldReapSuffix)
	_ = os.Remove(path + pushHoldReleasedSuffix)
	_ = os.Remove(path + pushHoldGensSuffix)
}

// pushHoldSidecarBase maps a sidecar entry name to its hold file's name.
func pushHoldSidecarBase(name string) (string, bool) {
	for _, suffix := range []string{pushHoldUnreservedSuffix, pushHoldVerifiedSuffix, pushHoldScanSuffix, pushHoldReapSuffix, pushHoldReleasedSuffix, pushHoldGensSuffix} {
		if base, ok := strings.CutSuffix(name, suffix); ok {
			return base, true
		}
	}
	return "", false
}

// pushHoldEpochSuffix names the hold-lifecycle epoch file, a SIBLING of
// the hold directory (like the reserve lock) so hold readers never see it
// as an entry. Every mutation of the live hold set rewrites it with fresh
// randomness; livePushHoldsKey folds its content into the fingerprint so
// equal bracketing snapshots prove no transient hold appeared and
// vanished in between (Codex round-32 P2 — a directory mtime is blind to
// lifecycles inside one coarse clock tick).
const pushHoldEpochSuffix = ".epoch"

// writeEpochFile is os.WriteFile behind a test seam: fault injection for
// mid-write failures (round-35 P2) needs to be deterministic.
var writeEpochFile = os.WriteFile

// bumpPushHoldEpoch atomically rewrites dir's epoch file with fresh
// randomness (temp + rename; concurrent bumps each land whole, last one
// wins — only inequality with the previous value matters). Reservations
// FAIL CLOSED on a failed bump: an unbumped create is exactly the
// transition that would let a marker vouch for a set that was never
// stable. Release-side callers stay best-effort — a net-zero lifecycle
// always contains the fail-closed create bump, so a missed release bump
// can only cause a conservative extra revalidation.
func bumpPushHoldEpoch(dir string) error {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return err
	}
	tmp := dir + pushHoldEpochSuffix + ".tmp-" + hex.EncodeToString(buf[:8])
	if err := writeEpochFile(tmp, []byte(hex.EncodeToString(buf[:])), 0o600); err != nil {
		// Remove the temp on write failure too (Codex round-35 P2): these
		// are SIBLINGS of the hold directory — no sweep ever visits them —
		// and every retry picks a fresh random name, so a temp stranded by
		// ENOSPC/EIO would accumulate forever, worsening the very storage
		// condition that stranded it. Best-effort: an open() failure left
		// nothing behind, and removal under the same fault at worst leaves
		// what it found.
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dir+pushHoldEpochSuffix); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// appendPushHolds records names in the push's hold file with one append
// write. reservePushHolds calls this BEFORE any of the push's authority
// CASes can run — the ordering the skip proof above depends on — and the
// push is failed when the hold cannot be recorded, because committing
// without it reopens the race.
func appendPushHolds(holdPath string, names []string) error {
	var sb strings.Builder
	for _, name := range names {
		sb.WriteString(name)
		sb.WriteByte('\n')
	}
	f, err := os.OpenFile(holdPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(sb.String()); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// Fail CLOSED on the epoch bump (round-32 P2): committing against an
	// unbumped reservation is the one transition the ABA fingerprint
	// cannot see.
	return bumpPushHoldEpoch(filepath.Dir(holdPath))
}

// touchPushHold refreshes the hold file's TTL clock. Every hook of the
// push — pre-receive through its publication work, each update hook across
// its FULL runtime — keeps the shared hold file fresh while it lives
// (startPushHoldRefresh): pack uploads, policy walks, and post-CAS
// decorator work (webhook and sync-poke enqueues) all have no fixed bound,
// and a hold must expire only when its owning receive-pack is actually
// dead, or the race this machinery closes reopens after one TTL and the
// mint sweep could reap a live hold (Codex P2s, rounds 1-2 on #173).
func touchPushHold(holdPath string) {
	now := time.Now()
	_ = os.Chtimes(holdPath, now, now)
	// The sidecars age on the same clock: a live push longer than one TTL
	// must not have another push's mint sweep reap its markers (Chtimes
	// is a no-op ENOENT when no sidecar exists).
	_ = os.Chtimes(holdPath+pushHoldUnreservedSuffix, now, now)
	_ = os.Chtimes(holdPath+pushHoldVerifiedSuffix, now, now)
	_ = os.Chtimes(holdPath+pushHoldScanSuffix, now, now)
	_ = os.Chtimes(holdPath+pushHoldReleasedSuffix, now, now)
	_ = os.Chtimes(holdPath+pushHoldGensSuffix, now, now)
}

// startPushHoldRefresh keeps holdPath fresh on a TTL/4 cadence until the
// returned stop function runs (or the hook process dies — exactly the
// condition the TTL is meant to detect). The first touch is synchronous
// (Codex round-5 P2 on #173): a sequence of hooks each shorter than one
// ticker period would otherwise never touch the file at all, and a push
// whose hook phase outlives the TTL would lose its hold mid-flight.
func startPushHoldRefresh(holdPath string) (stop func()) {
	// Touches land on the INODE, not just the path (Codex round-31 P2 on
	// #173): the expiry reaper quarantines an expired-verdict hold by
	// RENAME before revalidating and unlinking it, and a path-based
	// Chtimes from a refresher that resumed exactly then would hit ENOENT
	// and be lost — the reaper would see an untouched inode and delete a
	// live push's hold, which Chtimes can never recreate. An fd-based
	// touch follows the inode through the rename, so the reaper's
	// post-rename revalidation observes every touch that preceded its
	// unlink and restores the hold. The fd is opened lazily: the server's
	// refresher starts before the reservation creates the file.
	var f *os.File
	touch := func() {
		if f != nil {
			// A removePushHolds rewrite replaces the inode (temp +
			// rename): detect the orphan and re-point at the current
			// file so both the touches and the liveness lock (below)
			// keep covering the visible hold.
			if fi, err := f.Stat(); err != nil || fi.Sys().(*syscall.Stat_t).Nlink == 0 {
				_ = f.Close()
				f = nil
			}
		}
		if f == nil {
			// Establish the fd and its shared lock ATOMICALLY with
			// respect to reapers, under the reservation flock (Codex
			// round-36 P2): merely retrying a lost LOCK_SH (round 34)
			// still left the CURRENT reap unopposed — a reaper could
			// quarantine and win its exclusive lock between this open
			// and the LOCK_SH below, then unlink a live reservation
			// while the only fallback was an ENOENT path touch. The reap
			// window holds the same lock, so an open that succeeds here
			// is always followed by a LOCK_SH no reaper can interleave.
			// Try-lock only: a busy lock (a peer's reservation scan, a
			// reap on another hold) skips to the next tick — the path
			// touches below keep the mtime fresh, which alone keeps
			// expiry ineligible on a sane clock; fd-less owners keep the
			// mtime revalidation as their bound.
			if lock, lerr := os.OpenFile(filepath.Dir(holdPath)+".reserve.lock", os.O_CREATE|os.O_RDWR, 0o644); lerr == nil {
				if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
					f, _ = os.OpenFile(holdPath, os.O_WRONLY, 0)
					if testHookRefreshBeforeLock != nil {
						testHookRefreshBeforeLock()
					}
					if f != nil {
						// The shared flock is the owner-liveness beacon
						// (Codex round-32 P2): the expiry reaper unlinks
						// a quarantined hold only after WINNING an
						// exclusive flock on its inode, so as long as
						// this fd lives, no interleaving of stats,
						// touches, and renames can discard the hold —
						// kernel-arbitrated, no timing anywhere. Losing
						// it here is impossible for a reaper (it needs
						// the reservation lock this tick holds); close
						// and retry against anything more exotic (Codex
						// round-34 P2: an unlocked fd must never
						// persist).
						if syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB) != nil {
							_ = f.Close()
							f = nil
						}
					}
				}
				_ = lock.Close() // close releases the flock when held
			}
		}
		if f != nil {
			now := syscall.NsecToTimeval(time.Now().UnixNano())
			_ = syscall.Futimes(int(f.Fd()), []syscall.Timeval{now, now})
		}
		// Path-based touches still cover the sidecars (and the hold when
		// the fd has not opened yet); after release they are ENOENT
		// no-ops, exactly as before.
		touchPushHold(holdPath)
	}
	touch()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(pushHoldTTL / 4)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				touch()
			}
		}
	})
	return func() {
		close(done)
		wg.Wait()
		if f != nil {
			_ = f.Close()
		}
	}
}

// pushHoldSet is one snapshot of live held ref names, sorted for O(log n)
// D/F-aware lookups: matching must not go quadratic when two high-fanout
// pushes overlap (Codex security P2 on #173).
type pushHoldSet struct{ names []string } // sorted ascending

func newPushHoldSet(names []string) pushHoldSet {
	sort.Strings(names)
	return pushHoldSet{names: names}
}

// linkFile is os.Link behind a test seam (PR #195 round-14 P2: restore-
// failure handling needs deterministic fault injection).
var linkFile = os.Link

// statFile is os.Stat behind a test seam (PR #195 round-15 P2: the
// orphan sweep's confirmed-absence rule needs deterministic EIO).
var statFile = os.Stat

// holdSidecarOwner maps a hold FILE path to the path its sidecars are
// named under: a quarantined hold (`push-X.reaping`) keeps its sidecars
// at the BASE name — the reap renames only the hold file (PR #195
// round-16 security P2: readers that sought `push-X.reaping.released`
// lost the tombstone association for the quarantine window, letting a
// scan over-hold released coverage while the bracketing fingerprints —
// blind to the net-zero quarantine/restore renames — still matched and
// certified its skips until the TTL).
func holdSidecarOwner(path string) string {
	if base, ok := strings.CutSuffix(path, ".reaping"); ok {
		return base
	}
	return path
}

// errPushScanBudget marks a peer scan denied MID-SCAN because reading the
// next file would exceed the push's remaining allowance (PR #195 round-12
// P2): the precheck's size snapshot cannot bind flock-less tombstone
// growth, so the budget is enforced against each file's CURRENT size
// immediately before its bytes are read — zero overshoot by construction,
// independent of when the growth lands.
var errPushScanBudget = Refuse(errors.New("push hold: push exceeded its peer-hold scan allowance against unusually large concurrent push holds; retry shortly or split the push"))

// readFileLimit reads at most limit bytes of path — os.ReadFile with a
// hard cap for files that can grow under the reader (PR #195 round-11
// P2: the scan-work allowance charges sampled sizes, so reads must not
// exceed them).
func readFileLimit(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(io.LimitReader(f, limit))
}

// readPushHolds snapshots the live (unexpired) holds in dir, skipping
// exceptPath (a push's own file — its reservation must not conflict with
// its own earlier holds). ENOENT anywhere is benign: the directory may not
// exist yet, and a peer's file vanishes exactly when its push completes.
// Any other error is reported alongside the partial set, because an
// unreadable peer hold is NOT proof of no conflict — reservation must fail
// closed on it (Codex round-3 P1 on #173), while hydration deliberately
// proceeds fail-open.
func readPushHolds(dir, exceptPath string) (pushHoldSet, error) {
	return (&pushHoldDirCache{dir: dir, except: exceptPath}).load()
}

// pushHoldDirCache is readPushHolds with a per-file parse cache for
// callers that re-read the same directory in a tight loop —
// reservePushHolds' 25ms conflict poll (Codex round-10 P2: without it a
// blocked reservation re-reads, re-splits, and re-sorts every peer hold on
// every poll, hundreds of times per waiter). A hold file is re-parsed only
// when its (size, mtime) changes — appends change the size, and the
// refresher's touch only bumps the mtime a few times a minute — and the
// combined sorted set is rebuilt only when the surviving file set or one
// of its parses changed. Not goroutine-safe; one loop owns one cache.
type pushHoldDirCache struct {
	dir, except string

	// remaining, when positive, bounds the bytes a single load may read:
	// each stale entry's CURRENT listed sizes are checked against it
	// before any read (round-12 P2 — the reservation path's precheck
	// snapshot cannot bind flock-less sidecar growth), failing closed
	// with errPushScanBudget. Zero means unlimited (hydration's fail-open
	// reader charges no allowance).
	remaining int64

	files  map[string]holdFileParse
	set    pushHoldSet
	setKey string // "path\x00size\x00mtime\x01..." fingerprint the set was built from

	parses      int   // test seam: number of ReadFile+split parses performed
	parsedBytes int64 // bytes actually re-read across loads (unchanged files cost a stat)
}

type holdFileParse struct {
	size  int64
	mtime int64 // UnixNano
	// The `.released` tombstone sidecar's fingerprint (issue #186): -1
	// when absent, -2 after a failed read so the next poll re-probes.
	// Tombstoned names are subtracted from names at parse time, so a
	// tombstone append re-parses this peer on the next load.
	relSize  int64
	relMtime int64
	names    []string // held minus tombstoned
}

func (c *pushHoldDirCache) load() (pushHoldSet, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return pushHoldSet{}, nil
		}
		return pushHoldSet{}, err
	}
	if c.files == nil {
		c.files = make(map[string]holdFileParse)
	}
	// Pre-pass: tombstone sidecar stats, keyed by their hold file's path
	// (issue #186). Same ReadDir snapshot as the holds below; a tombstone
	// landing after the listing is seen as a subset, which over-holds —
	// the safe direction.
	released := make(map[string]os.FileInfo)
	for _, e := range entries {
		if base, ok := strings.CutSuffix(e.Name(), pushHoldReleasedSuffix); ok && !e.IsDir() {
			if info, err := e.Info(); err == nil {
				released[filepath.Join(c.dir, base)] = info
			}
		}
	}
	var firstErr error
	live := make(map[string]holdFileParse, len(entries))
	var key strings.Builder
	for _, e := range entries {
		// Sidecar artifacts mark a push's UNRESERVED names, verified
		// verdicts, or reap suspicion — never holds (the IsDir check is
		// defensive: nothing else creates directories here).
		if isPushHoldSidecar(e.Name()) || e.IsDir() {
			continue
		}
		path := filepath.Join(c.dir, e.Name())
		if c.except != "" && path == c.except {
			continue
		}
		info, err := e.Info()
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) && firstErr == nil {
				firstErr = err
			}
			continue
		}
		// The liveness check runs on every load — expiry is time-driven,
		// not change-driven — so an expired hold drops out of the set on
		// the next poll even though its file never changed. Clock
		// anomalies in EITHER direction must not drop a live push's
		// reservation (that reopens the D/F pair race until its next
		// refresher tick): pushHoldLive clamps far-future mtimes
		// (backward steps, round-21/22) and requires a second aged
		// observation for far-past ones (forward steps, round-25).
		if !pushHoldLive(path, info.ModTime()) {
			continue
		}
		relSize, relMtime := int64(-1), int64(-1)
		if ri, ok := released[holdSidecarOwner(path)]; ok {
			relSize, relMtime = ri.Size(), ri.ModTime().UnixNano()
		}
		parse, ok := c.files[path]
		if !ok || parse.size != info.Size() || parse.mtime != info.ModTime().UnixNano() ||
			parse.relSize != relSize || parse.relMtime != relMtime {
			if testHookDirCachePreParse != nil {
				testHookDirCachePreParse()
			}
			if c.remaining > 0 && c.parsedBytes+info.Size()+max(relSize, 0) > c.remaining {
				if firstErr == nil {
					firstErr = errPushScanBudget
				}
				continue
			}
			b, err := os.ReadFile(path)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) && firstErr == nil {
					firstErr = err
				}
				continue
			}
			c.parses++
			c.parsedBytes += int64(len(b))
			parse = holdFileParse{size: info.Size(), mtime: info.ModTime().UnixNano(), relSize: relSize, relMtime: relMtime}
			// Subtract the peer's tombstones (issue #186): exact released
			// names drop out of coverage. An unreadable sidecar subtracts
			// NOTHING (over-holding, the safe direction — unlike a hold
			// file, whose unreadability must fail reservations closed)
			// and re-probes on the next load via the -2 sentinel. The
			// read is BOUNDED to the sampled size (round-11 P2, the
			// multi-name twin of the single-name LimitReader): flock-less
			// appends can grow the sidecar between the listing's stat and
			// this read, and unbounded reading would parse bytes the
			// pre-read allowance check never charged. A boundary-cut
			// record is an unterminated tail, dropped below; the growth
			// re-parses on the next load via the fresh stat.
			var tombstoned map[string]bool
			if relSize >= 0 {
				if rb, err := readFileLimit(holdSidecarOwner(path)+pushHoldReleasedSuffix, relSize); err == nil {
					c.parsedBytes += int64(len(rb))
					// Trust only newline-TERMINATED records (PR #195
					// round-1 P2): a short append (ENOSPC, EIO) strands a
					// PREFIX of the released name as the last record, and
					// that prefix can itself be a different still-reserved
					// name — releasing refs/heads/foobar must never free
					// refs/heads/foo. An unterminated tail is dropped
					// (over-holding, the safe direction).
					if i := bytes.LastIndexByte(rb, '\n'); i >= 0 {
						rb = rb[:i+1]
					} else {
						rb = nil
					}
					tombstoned = make(map[string]bool)
					for name := range strings.SplitSeq(string(rb), "\n") {
						if name != "" {
							tombstoned[name] = true
						}
					}
				} else {
					parse.relSize, parse.relMtime = -2, -2
				}
			}
			for held := range strings.SplitSeq(string(b), "\n") {
				if held != "" && !tombstoned[held] {
					parse.names = append(parse.names, held)
				}
			}
		}
		live[path] = parse
		fmt.Fprintf(&key, "%s\x00%d\x00%d\x00%d\x00%d\x01", path, parse.size, parse.mtime, parse.relSize, parse.relMtime)
	}
	c.files = live
	if key.String() != c.setKey {
		var names []string
		for _, parse := range live {
			names = append(names, parse.names...)
		}
		c.set = newPushHoldSet(names)
		c.setKey = key.String()
	}
	return c.set, firstErr
}

func (s pushHoldSet) contains(name string) bool {
	i := sort.SearchStrings(s.names, name)
	return i < len(s.names) && s.names[i] == name
}

// holdsDescendantOf reports whether any held name lives strictly under
// name's directory (name + "/...").
func (s pushHoldSet) holdsDescendantOf(name string) bool {
	prefix := name + "/"
	i := sort.SearchStrings(s.names, prefix)
	return i < len(s.names) && strings.HasPrefix(s.names[i], prefix)
}

// holdsAncestorOf reports whether any held name is a strict directory
// ancestor of name. O(len(name) + segments·log n).
func (s pushHoldSet) holdsAncestorOf(name string) bool {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' && s.contains(name[:i]) {
			return true
		}
	}
	return false
}

// covers reports whether name is held — exactly, or as a D/F relative of a
// held name (Codex P1 on #173): git's files backend cannot hold refs/heads/a
// and refs/heads/a/b simultaneously, so while a push-in-flight owns one
// side (say a committed deletion of `a` whose local deletion is pending), a
// sync must not touch the other side either — applying `a/b` would fail the
// whole update-ref transaction against the held survivor.
func (s pushHoldSet) covers(name string) bool {
	return s.contains(name) || s.holdsAncestorOf(name) || s.holdsDescendantOf(name)
}

// conflicts reports whether a STRICT D/F relative of name is held, equality
// excluded. Its remaining caller is pre-receive's INTRA-push creation check,
// where the probed set always contains the probed name itself; RESERVATION
// against peer holds uses covers instead (Codex round-14 P1 on #173): a
// held name's authority value may already differ from the git cache — the
// hold exists precisely to keep hydration off it — so a same-name peer
// reserving through would base its push on a stale value, and for an
// ATOMIC push the resulting mid-sequence CAS loss strands earlier
// commands' committed authority updates. Same-name races now serialize on
// the hold; the loser then fails cleanly against the healed cache or the
// atomic base check instead of half-committing.
func (s pushHoldSet) conflicts(name string) bool {
	return s.holdsAncestorOf(name) || s.holdsDescendantOf(name)
}

// holdLineCovers reports whether scanned held line and candidate name
// collide as exact names or strict directory/file relatives — the
// allocation-free scalar twin of pushHoldSet.covers ([]byte-to-string
// comparisons compile without conversions).
func holdLineCovers(held []byte, name string) bool {
	switch {
	case len(held) == len(name):
		return string(held) == name
	case len(held) < len(name):
		return len(held) > 0 && name[len(held)] == '/' && string(held) == name[:len(held)]
	default:
		return held[len(name)] == '/' && string(held[:len(name)]) == name
	}
}

// peerHoldCovers reports whether any LIVE peer hold covers name,
// streaming each peer file line by line instead of building the combined
// sorted set (Codex round-25 security P2 on #173): every sidecar/JIT
// update hook runs in its own PROCESS, so reservePushHolds' cross-poll
// parse cache cannot amortize across them — a push carrying the full
// 1,024-command sidecar bundled against a concurrent 100k-name hold
// would otherwise pay ~1,000 repository-scale split+merge+sort parses
// under the reservation lock. The scan allocates one fixed scanner
// buffer, short-circuits on the first conflict, and costs one buffered
// read of each live peer file. Fail CLOSED on read trouble, exactly like
// the set-based path. The second return is the scan's work in bytes —
// the size of every peer file it opened (a deliberate overcount on a
// short-circuit) — which the caller charges against the push's
// maxPushPeerScanBytes allowance.
// remaining is the push's unspent scan allowance, enforced against each
// file's CURRENT size before its bytes are read (round-12 P2 — the
// caller's precheck snapshot cannot bind flock-less sidecar growth);
// exceeding it returns errPushScanBudget with zero overshoot.
func peerHoldCovers(dir, exceptPath, name string, remaining int64) (bool, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, err
	}
	var scanned int64
	for _, e := range entries {
		if isPushHoldSidecar(e.Name()) || e.IsDir() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if path == exceptPath {
			continue
		}
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // released between the listing and the stat
			}
			return false, scanned, err
		}
		if !pushHoldLive(path, info.ModTime()) {
			continue
		}
		if scanned+info.Size() > remaining {
			return false, scanned, errPushScanBudget
		}
		f, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return false, scanned, err
		}
		scanned += info.Size()
		// Collect the covering held lines (normally none, at most a
		// handful — exact name plus D/F relatives), then subtract the
		// peer's tombstones before declaring a conflict (issue #186).
		var covering map[string]bool
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if holdLineCovers(sc.Bytes(), name) {
				if covering == nil {
					covering = make(map[string]bool)
				}
				covering[string(sc.Bytes())] = true
			}
		}
		err = sc.Err()
		_ = f.Close()
		if err != nil {
			return false, scanned, err
		}
		if len(covering) == 0 {
			continue
		}
		// Stream the tombstones, deleting exact matches. Absent or
		// unreadable sidecars subtract nothing — over-holding, the safe
		// direction (a hold file's unreadability above stays fail-closed,
		// because there a missed line would UNDER-hold). Bytes charged
		// like the hold's own.
		if rf, err := os.Open(holdSidecarOwner(path) + pushHoldReleasedSuffix); err == nil {
			var sampled int64
			if ri, err := rf.Stat(); err == nil {
				sampled = ri.Size()
			}
			if scanned+sampled > remaining {
				_ = rf.Close()
				return false, scanned, errPushScanBudget
			}
			scanned += sampled
			if testHookPeerScanPostStat != nil {
				testHookPeerScanPostStat()
			}
			// Trust only newline-TERMINATED records (PR #195 round-1 P2):
			// ReadString returns the unterminated tail WITH an error, so a
			// torn append's stranded prefix — possibly a different live
			// name — is never applied. The read is LIMITED to the sampled,
			// charged size (round-10 P2): appends take no flock, so the
			// sidecar can grow under this stream, and unbounded reading
			// would bypass the scan-work allowance byte-for-byte; a record
			// cut at the boundary is an unterminated tail, dropped like
			// any other (unread tombstones over-hold, the safe direction).
			rr := bufio.NewReader(io.LimitReader(rf, sampled))
			for len(covering) > 0 {
				line, err := rr.ReadString('\n')
				if err != nil {
					break
				}
				line = line[:len(line)-1]
				if covering[line] {
					delete(covering, line)
				}
			}
			_ = rf.Close()
		}
		if len(covering) > 0 {
			return true, scanned, nil
		}
	}
	return false, scanned, nil
}

// maxPushPeerScanBytes bounds ONE push's aggregate peer-hold scan work
// (Codex round-27 security P2 on #173): each sidecar or JIT update hook is
// a fresh process whose single-name self-reservation streams every live
// peer hold, so two overlapping crafted pushes — thousands of no-force
// updates racing a 100k-name reservation — could otherwise reread
// repository-scale bytes once per hook for the push's whole duration,
// unbounded. Every scan's opened bytes accrue in the push's counter
// sidecar; once the total exceeds this allowance the push's remaining
// self-reserving hooks are denied with a retryable message BEFORE
// scanning again, so aggregate work is capped at one allowance plus one
// trailing scan. Generous by design: a full 1,024-command sidecar each
// streaming a 100k-name (≈6.4MB) peer set totals ≈6.6GB and stays
// admitted, while the round-27 attack figure (≈200GB) dies two orders of
// magnitude earlier. A var so tests can shrink it.
var maxPushPeerScanBytes = int64(8) << 30

// readPushScanWork and writePushScanWork maintain the counter. Both run
// only under the reservation flock, so plain read/overwrite is race-free;
// absent means zero. Callers fail CLOSED on counter trouble — a bound
// that cannot be maintained is not a bound.
func readPushScanWork(holdPath string) (int64, error) {
	b, err := os.ReadFile(holdPath + pushHoldScanSuffix)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed scan-work counter: %w", err)
	}
	return n, nil
}

func writePushScanWork(holdPath string, n int64) error {
	return os.WriteFile(holdPath+pushHoldScanSuffix, []byte(strconv.FormatInt(n, 10)), 0o600)
}

// pushHoldPendingBytes reports, STAT-ONLY, the peer-hold bytes a scan
// would read right now: every non-sidecar file except exceptPath whose
// (size, mtime) is not already covered by cached (nil for the uncached
// single-name scans). The allowance check runs on used+pending BEFORE any
// parse (Codex round-31 P2 on #173): checking only previously charged
// work let one scan overshoot the limit by an entire peer set — read,
// split, and sorted under the reservation flock before the budget had any
// effect. Expired files inflate the estimate slightly (no liveness
// probing here — the estimate must stay pure stats); the mint sweep
// bounds that skew.
func pushHoldPendingBytes(dir, exceptPath string, cached map[string]holdFileParse) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	// Tombstone sidecars are read alongside their holds (issue #186), so
	// their bytes join the estimate under the same staleness test — a
	// tombstone append alone re-parses its peer, and skipping its size
	// here would reopen the overshoot this function closes.
	released := make(map[string]os.FileInfo)
	for _, e := range entries {
		if base, ok := strings.CutSuffix(e.Name(), pushHoldReleasedSuffix); ok && !e.IsDir() {
			if info, err := e.Info(); err == nil {
				released[filepath.Join(dir, base)] = info
			}
		}
	}
	var pending int64
	for _, e := range entries {
		if isPushHoldSidecar(e.Name()) || e.IsDir() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if exceptPath != "" && path == exceptPath {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // released between the listing and the stat
		}
		relSize, relMtime := int64(-1), int64(-1)
		if ri, ok := released[holdSidecarOwner(path)]; ok {
			relSize, relMtime = ri.Size(), ri.ModTime().UnixNano()
		}
		if parse, ok := cached[path]; ok && parse.size == info.Size() && parse.mtime == info.ModTime().UnixNano() &&
			parse.relSize == relSize && parse.relMtime == relMtime {
			continue
		}
		pending += info.Size()
		if relSize > 0 {
			pending += relSize
		}
	}
	return pending, nil
}

// pushHoldWaitMax bounds reservePushHolds' wait for D/F-conflicting holds
// from other pushes; a var so tests can exercise the denial path quickly.
// Hold windows are server-controlled and normally millisecond-scale.
var pushHoldWaitMax = 10 * time.Second

// ErrRefHeld marks a push-hold reservation that gave up after
// pushHoldWaitMax because a concurrent push or ref write still holds a
// conflicting ref, or the reservation lock. It is transient: the API
// answers a ref write that fails with it with 503 and Retry-After.
var ErrRefHeld = errors.New("a concurrent push or ref write holds the ref")

// refHeldError is such a give-up. It keeps the reservation's own message,
// which a pushing git client shows, and matches ErrRefHeld.
type refHeldError struct{ err error }

func (e refHeldError) Error() string   { return e.err.Error() }
func (e refHeldError) Unwrap() []error { return []error{e.err, ErrRefHeld} }

// heldUnlessCanceled marks a give-up err as ErrRefHeld, unless the
// caller's own context ended: then nothing waits for an answer.
func heldUnlessCanceled(parent context.Context, err error) error {
	if parent.Err() != nil {
		return err
	}
	return refHeldError{err}
}

// reservePushHolds atomically verifies that no OTHER live push holds any
// of names or a strict D/F relative of one (covers semantics — see
// conflicts for why equality is a conflict here), then appends all of
// names to this push's hold file — ONE check+reserve per push, serialized against
// concurrent reservers by an exclusive lock file (Codex round-2 P1 on
// #173: a non-atomic check-then-append lets two pushes creating
// refs/heads/a and refs/heads/a/b both pass, and the authority CAS — keyed
// by exact name — would accept both while the git cache can materialize
// only one). pre-receive calls this before any publication or update-hook
// CAS. Live conflicts are waited out briefly; on timeout the push FAILS
// CLOSED (round-2 P1: proceeding would commit an authority update whose
// local ref transaction is doomed while the conflicting neighbor
// survives) — a clean, pre-publication, retryable denial.
func reservePushHolds(ctx context.Context, holdPath string, names []string) error {
	return reservePushHoldsCovering(ctx, holdPath, names, nil)
}

// reservePushHoldsCovering is reservePushHolds reporting COVERAGE to the
// caller: *covered flips exactly at the append boundary — after every
// pre-append exit (scan-allowance denials, lock deadlines, peer-conflict
// timeouts, where the name provably never touched the hold file) and
// strictly before appendPushHolds can first write it (PR #195 round-18
// P2, refining round 9's cover-the-attempt: the append can fail AFTER its
// bytes landed, so waiting for success would leak a physically held name,
// while flipping before the reservation call tombstoned never-covered
// names — each such release bumping the repository epoch and invalidating
// the allowed siblings' held markers once per doomed command).
func reservePushHoldsCovering(ctx context.Context, holdPath string, names []string, covered *bool) error {
	if !filepath.IsAbs(holdPath) {
		return fmt.Errorf("push hold path %q is not absolute", holdPath)
	}
	for _, name := range names {
		// Git's check-ref-format rejects control characters long before
		// hooks run; refuse rather than mint line-split holds.
		if name == "" || strings.Contains(name, "\n") {
			return fmt.Errorf("push hold: malformed ref name %q", name)
		}
	}
	if len(names) == 0 {
		// Nothing to reserve — every command was denied, sidecar-diverted,
		// or JIT-deferred (Codex round-28 security P2 on #173): a zero-name
		// reservation can conflict with nothing, so skip the lock and the
		// peer scan entirely. The multi-name path below parses EVERY live
		// peer hold under the reservation flock before consulting names, so
		// repeated compact doomed pushes against a large concurrent hold
		// would otherwise buy repository-scale parses per push — work the
		// single-name scan allowance never sees.
		return nil
	}
	dir := filepath.Dir(holdPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// The reserve lock lives NEXT TO the hold directory, not inside it:
	// readers treat every directory entry as a hold file, and the mint
	// sweep may unlink stale entries — an unlinked lock inode would let
	// two reservers "hold" different inodes of the same path.
	//
	// One deadline covers BOTH the lock acquisition and the conflict
	// polling (Codex round-4 P2 on #173): a reserver stalled while
	// holding the lock must produce the documented retryable denial for
	// the pushes queued behind it, not an unbounded hang.
	lockPath := dir + ".reserve.lock"
	deadline := time.Now().Add(pushHoldWaitMax)
	parent := ctx
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	// One parse cache across the poll iterations (Codex round-10 P2): a
	// large peer hold otherwise gets re-read, re-split, and re-sorted every
	// 25ms for up to the full deadline — repo-scale bytes times hundreds of
	// polls, multiplied by concurrent waiters. Unchanged files cost a stat.
	cache := &pushHoldDirCache{dir: dir, except: holdPath}
	for {
		unlock, err := flockExclusiveCtx(ctx, lockPath)
		if err != nil {
			if ctx.Err() != nil {
				// The refusal is outermost: the pusher sees the retry
				// guidance, and errors.Is still finds ErrRefHeld for the
				// REST 503.
				return Refuse(heldUnlessCanceled(parent, fmt.Errorf("timed out waiting for a concurrent push's hold reservation; retry shortly: %w", ctx.Err())))
			}
			return err
		}
		conflict := ""
		if len(names) == 1 {
			// The single-name case is every sidecar/JIT hook's
			// self-reservation — a fresh process each, so the parse
			// cache below cannot amortize for it (Codex round-25
			// security P2): stream the peers instead of building the
			// combined sorted set. The push-wide scan-work allowance
			// (Codex round-27 security P2) bounds what those fresh
			// processes can spend in aggregate: over-allowance hooks are
			// denied before scanning again, and conflict-poll rescans
			// charge the same counter.
			used, err := readPushScanWork(holdPath)
			if err != nil {
				unlock()
				return fmt.Errorf("push hold: scan-work counter: %w", err)
			}
			pending, err := pushHoldPendingBytes(dir, holdPath, nil)
			if err != nil {
				unlock()
				return fmt.Errorf("push hold: reading peer holds: %w", err)
			}
			if used+pending > maxPushPeerScanBytes {
				unlock()
				return refusef("push hold: push exceeded its peer-hold scan allowance against unusually large concurrent push holds; retry shortly or split the push")
			}
			covered, n, err := peerHoldCovers(dir, holdPath, names[0], maxPushPeerScanBytes-used)
			if err != nil {
				unlock()
				if errors.Is(err, errPushScanBudget) {
					// Mid-scan denial (round-12 P2): sidecar growth after
					// the precheck busted the allowance — same retryable
					// verdict, zero overshoot.
					return err
				}
				// Fail CLOSED: an unreadable peer hold is not proof of
				// no conflict (Codex round-3 P1 on #173).
				return fmt.Errorf("push hold: reading peer holds: %w", err)
			}
			if n > 0 {
				if err := writePushScanWork(holdPath, used+n); err != nil {
					unlock()
					return fmt.Errorf("push hold: scan-work counter: %w", err)
				}
			}
			if covered {
				conflict = names[0]
			}
		} else {
			// The multi-name path charges the same push-wide allowance as
			// the single-name one (Codex round-29 security P2): the parse
			// cache amortizes only WITHIN this call's poll iterations —
			// every compact doomed push is a fresh pre-receive process
			// whose first load parses the entire peer set under the
			// reservation flock. Charged by bytes actually re-read
			// (unchanged files cost a stat), so one waiting reserver's
			// polls do not re-bill an unchanged repository-scale peer.
			used, err := readPushScanWork(holdPath)
			if err != nil {
				unlock()
				return fmt.Errorf("push hold: scan-work counter: %w", err)
			}
			pending, err := pushHoldPendingBytes(dir, holdPath, cache.files)
			if err != nil {
				unlock()
				return fmt.Errorf("push hold: reading peer holds: %w", err)
			}
			if used+pending > maxPushPeerScanBytes {
				unlock()
				return refusef("push hold: push exceeded its peer-hold scan allowance against unusually large concurrent push holds; retry shortly or split the push")
			}
			before := cache.parsedBytes
			// The in-load budget (round-12 P2): allowance still unspent
			// plus what this cache already read (parsedBytes is cumulative
			// across polls while `used` already charges completed polls).
			cache.remaining = maxPushPeerScanBytes - used + before
			peers, err := cache.load()
			if err != nil {
				unlock()
				if errors.Is(err, errPushScanBudget) {
					return err
				}
				// Fail CLOSED (round-3 P1), as above.
				return fmt.Errorf("push hold: reading peer holds: %w", err)
			}
			if n := cache.parsedBytes - before; n > 0 {
				if err := writePushScanWork(holdPath, used+n); err != nil {
					unlock()
					return fmt.Errorf("push hold: scan-work counter: %w", err)
				}
			}
			for _, name := range names {
				if peers.covers(name) {
					conflict = name
					break
				}
			}
		}
		if conflict == "" {
			if covered != nil {
				*covered = true
			}
			err := appendPushHolds(holdPath, names)
			unlock()
			return err
		}
		unlock()
		if time.Now().After(deadline) || ctx.Err() != nil {
			return Refuse(heldUnlessCanceled(parent, fmt.Errorf("a concurrent push holds a ref conflicting with %s (same ref or directory/file relative); retry shortly", conflict)))
		}
		select {
		case <-ctx.Done():
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// removePushHolds drops names from a push's OWN hold file, releasing
// reservations pre-receive decided not to keep (authority-doomed creations
// — see the post-reservation probe): leaving them held would squat on
// their existing D/F relatives' namespace for the whole push window. The
// rewrite is atomic (temp file + rename) so concurrent readers see the old
// or the new set, never a truncated one; the temp lives in the hold
// directory under a hold-like name, so a reader that catches it merely
// double-counts our remaining names for one poll (over-holding, the safe
// direction) and the TTL sweep collects it if we crash mid-rename. Only
// this push's processes write this file (git runs pre-receive strictly
// before the update hooks that append to it), but the rewrite runs under
// the reservation flock anyway (Codex round-36 P2): the expiry reaper's
// quarantine window holds that lock, and an unserialized read → rename
// straddling the window would let the reaper's restore rename the OLD
// full file over the reduced replacement, silently resurrecting every
// released name for the rest of the push.
func removePushHolds(holdPath string, names []string) error {
	drop := make(map[string]bool, len(names))
	for _, name := range names {
		drop[name] = true
	}
	unlock, err := flockExclusive(filepath.Dir(holdPath) + ".reserve.lock")
	if err != nil {
		return err
	}
	defer unlock()
	b, err := os.ReadFile(holdPath)
	if err != nil {
		return err
	}
	var keep strings.Builder
	for held := range strings.SplitSeq(string(b), "\n") {
		if held != "" && !drop[held] {
			keep.WriteString(held)
			keep.WriteByte('\n')
		}
	}
	tmp := holdPath + ".release"
	if err := os.WriteFile(tmp, []byte(keep.String()), 0o644); err != nil {
		return err
	}
	if testHookRemoveHoldsPreRename != nil {
		testHookRemoveHoldsPreRename()
	}
	// Bump BEFORE the replacement lands (Codex round-36 P2, mirroring the
	// removal invariant of removePushHoldFiles): the partial rewrite is a
	// hold-set mutation like any other, and replacing the contents with
	// the bump lost lets bracketing snapshots fingerprint identically
	// under coarse timestamp granularity. When the epoch cannot record
	// the transition, retain the old contents — over-holding, the safe
	// direction, bounded by the TTL machinery.
	if err := bumpPushHoldEpoch(filepath.Dir(holdPath)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, holdPath); err != nil {
		return err
	}
	return nil
}

// appendPushReleased tombstones names in the push's `.released` sidecar
// with ONE append write (issue #186 findings 1+6): readers subtract these
// exact names from the hold's coverage, so a release costs O(names)
// instead of the whole-file rewrite that made an all-denied fanout O(N²).
//
// Deliberately NOT under the reservation flock, validated against the
// #189 invariants: only the owning push's git-serialized processes write
// this file (update hooks and reference-transaction callbacks run one at
// a time, strictly before post-receive's release); the reaper's
// quarantine window renames only the HOLD file, never the sidecar, so a
// racing append lands in the same sidecar either way, and an append after
// an unlink merely recreates an orphan the sweep ages out. Readers under
// the flock may observe a mid-append subset — tombstones only ever grow,
// so a missed suffix over-holds, the safe direction. The single write
// keeps torn tails line-shaped at worst; a torn line matches no exact
// name and over-holds.
//
// A release for a push that never created its hold file is a FULL no-op
// (PR #195 round-5 P2): a no-pack policy-denied push reserves nothing,
// and letting its cheap denial mint orphan sidecars or invalidate live
// peers' held markers per request is the amplification the no-hold
// cleanup rule (round-33 security P2 on #189) exists to prevent.
//
// The epoch bump comes BEFORE the append becomes visible and fails
// CLOSED (PR #195 rounds 1, 4, 5 — the blindness plan in the original
// design comment proved unsound twice): a tombstone is a coverage
// mutation like any other and must sit under the fingerprint seqlock,
// or an append racing a hydrate's bracketed snapshots leaves both keys
// equal while the scan skipped the released ref. On failure the names
// simply stay held; post-receive and the TTL remain the bound.
func appendPushReleased(holdPath string, names []string) error {
	// A hold riding at its quarantine name is still an existing
	// reservation (PR #195 round-17 P2): the reap's restore path can
	// bring it back, and a dropped tombstone would leave an all-denied
	// push's names re-reserved with no post-receive to release them — a
	// stalled report tail then blocks conflicting pushes until the TTL.
	// The sidecar stays at the BASE name either way (readers map the
	// quarantine name to it).
	//
	// The two-name observation runs under the reservation flock (round-18
	// P2): the reaper holds that lock across its whole quarantine →
	// verdict → restore/unlink window, so an unserialized gate could stat
	// the base name before the restore rename and the quarantine name
	// after it — both absent, the release silently dropped while the hold
	// lives on at its base name. Serialized, the gate sees the hold
	// strictly before the window or strictly after its outcome; a
	// crash-stranded quarantine (its flock died with its process) still
	// satisfies the second stat.
	unlock, err := flockExclusive(filepath.Dir(holdPath) + ".reserve.lock")
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := os.Lstat(holdPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if testHookAppendReleasedMidGate != nil {
			testHookAppendReleasedMidGate()
		}
		if _, rerr := os.Lstat(holdPath + ".reaping"); rerr != nil {
			if errors.Is(rerr, os.ErrNotExist) {
				return nil
			}
			return rerr
		}
	}
	if err := bumpPushHoldEpoch(filepath.Dir(holdPath)); err != nil {
		return err
	}
	var sb strings.Builder
	for _, name := range names {
		sb.WriteString(name)
		sb.WriteByte('\n')
	}
	f, err := os.OpenFile(holdPath+pushHoldReleasedSuffix, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	// Fail CLOSED on an unterminated existing tail (PR #195 round-2 P2):
	// readers drop a torn final record, but a further O_APPEND would fuse
	// its first name onto that prefix plus a newline — one complete
	// record whose combined bytes can equal a DIFFERENT still-held ref,
	// releasing it prematurely. Only this push's serialized processes
	// write here, so the check cannot race another appender; failing
	// merely keeps the names held until post-receive.
	if fi, err := f.Stat(); err != nil {
		_ = f.Close()
		return err
	} else if fi.Size() > 0 {
		var tail [1]byte
		if _, err := f.ReadAt(tail[:], fi.Size()-1); err != nil {
			_ = f.Close()
			return err
		}
		if tail[0] != '\n' {
			_ = f.Close()
			return fmt.Errorf("push hold: released sidecar ends in an unterminated record; retaining reservations")
		}
	}
	if _, err := f.WriteString(sb.String()); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// appendPushHoldGen records one won CAS's generation attribution under
// the push's hold (issue #186 finding 5). Best-effort O(1): claims only
// ever widen what a held marker may coalesce, so a lost append costs a
// rescan, never correctness — the caller ignores the error. Lock-free
// like the tombstone append: only this push's git-serialized hook
// processes write it, and the reaper renames only the hold file, never
// sidecars. The unterminated-tail check mirrors appendPushReleased
// (PR #195 round-2 P2): readers drop a torn final record, but a further
// O_APPEND would fuse this claim's digits onto the torn prefix — one
// complete record claiming a generation NOBODY attributed, exactly what
// the range rule must never accept; skipping the claim merely costs
// coalescing.
func appendPushHoldGen(holdPath string, gen int64) error {
	assertf(gen > 0, "appendPushHoldGen: non-positive generation %d", gen)
	f, err := os.OpenFile(holdPath+pushHoldGensSuffix, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if fi, err := f.Stat(); err != nil {
		_ = f.Close()
		return err
	} else if fi.Size() > 0 {
		var tail [1]byte
		if _, err := f.ReadAt(tail[:], fi.Size()-1); err != nil {
			_ = f.Close()
			return err
		}
		if tail[0] != '\n' {
			_ = f.Close()
			return fmt.Errorf("push hold: claims sidecar ends in an unterminated record; skipping claim")
		}
	}
	if _, err := fmt.Fprintf(f, "%d\n", gen); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// releaseDeniedPushHold releases ONE ref from a push's hold on a
// non-atomic update-hook denial — an O(name) tombstone append (issue
// #186 finding 6): the former per-denial removePushHolds rewrite read and
// rewrote the entire remaining hold, O(N²) I/O across an all-denied
// fanout whose bases a concurrent writer moved. The bump-before-
// visibility invariant (PR #195 round-1 P2 — a denial can precede the
// CAS, so no competing writer necessarily invalidates held markers) and
// the never-reserved no-op rule (round-5 P2) live inside the append,
// shared with the committed-release path.
func releaseDeniedPushHold(holdPath, refName string) error {
	return appendPushReleased(holdPath, []string{refName})
}

// Generation marker (issue #4): the store's per-repo mutation generation
// the last fully successful hydrate ran at, persisted as a file inside the
// bare cache repo. Every mutation bumps the generation in the same atomic
// unit (ref.RefStore.GetGeneration), so an unchanged value proves packs and
// refs are unchanged and Hydrate can skip pack listing and ref sync
// entirely. The TTL forces a periodic full sync regardless, bounding the
// staleness of any missed bump or lost marker-ordering race to one window.
//
// The marker's second field is the HEAD symref target that hydrate synced
// (issue #18): the default branch lives in the meta row, whose changes bump
// no ref-store generation, so the short-circuit must additionally prove
// HEAD is unchanged. A PATCH /repo therefore defeats the short-circuit on
// every node the moment its next request compares the freshly resolved
// default branch against the marker. Markers written by binaries that
// predate the field fail parsing (ok=false) and cost exactly one full sync.
const (
	// Keep marker names across product renames: all shared-cache users
	// must invalidate and read the same generation evidence.
	generationMarker    = "codestorage.generation"
	generationMarkerTTL = 5 * time.Minute
)

// readGenerationMarker returns the marker's recorded generation and HEAD
// target, and whether its mtime — refreshed only by full successful
// hydrates, never by short-circuited ones — is still within the TTL window.
// ok=false means no usable marker (missing, malformed, or unreadable):
// callers full-hydrate, and the next success rewrites it.
func readGenerationMarker(repoPath string) (gen int64, head string, fresh bool, ok bool) {
	path := filepath.Join(repoPath, generationMarker)
	fi, err := os.Stat(path)
	if err != nil {
		return 0, "", false, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, "", false, false
	}
	genField, head, found := strings.Cut(strings.TrimSpace(string(b)), " ")
	gen, perr := strconv.ParseInt(genField, 10, 64)
	if !found || perr != nil || gen < 0 || !strings.HasPrefix(head, "refs/") {
		return 0, "", false, false
	}
	return gen, head, time.Since(fi.ModTime()) < generationMarkerTTL, true
}

// heldGenerationMarker is the held-skip variant of the generation marker
// (Codex round-22 security P2 on #173): a sync that skipped push-held
// refs must not publish the NORMAL marker (its held refs were never
// applied), but suppressing every marker made each subsequent
// authenticated advertisement repeat the full paged authority scan for
// the whole life of the push — one long hook-phase no-pack push plus
// advertisement spam sustained repository-scale store reads. The held
// marker records the scan's generation, HEAD target, and a fingerprint
// of the exact live holds it skipped under; while all three are
// unchanged — or the generation moved only through claimed, still-held
// CASes (the range rule, issue #186 finding 5) — a re-scan could only
// reproduce the same verdicts, so hydrate serves the cache as-is. Any
// hold release, append, rewrite, or expiry changes the fingerprint and
// forces the healing re-scan the suppression exists for. Like generationMarker,
// its physical name is shared-cache protocol, not product branding.
const heldGenerationMarker = "codestorage.generation.held"

// holdFileID is one fingerprint-folded file observation: size plus the
// (dev, inode) identity. Deliberately mtime-free — see the fold comment
// in livePushHoldsKey (issue #186 finding 5).
type holdFileID struct {
	size     int64
	dev, ino uint64
}

// absentHoldFile is the folded observation of a path with no file.
var absentHoldFile = holdFileID{size: -1}

// holdFileIdentity extracts the folded observation from a stat.
func holdFileIdentity(fi os.FileInfo) holdFileID {
	st, ok := fi.Sys().(*syscall.Stat_t)
	assertf(ok, "holdFileIdentity: non-syscall stat %T", fi.Sys())
	// Explicit conversions: Stat_t.Dev/Ino widths differ across platforms
	// (e.g. Dev is int32 on darwin, uint64 on linux). The fold only needs a
	// stable identity, not the numeric value.
	//nolint:unconvert // identity on linux, required on darwin (v0.14.4 incident)
	return holdFileID{size: fi.Size(), dev: uint64(st.Dev), ino: uint64(st.Ino)}
}

// livePushHoldsKey fingerprints the repo's live push holds (path, size,
// dev+inode identity of every unexpired hold file and its tombstone
// sidecar, hashed). ok=false on a read error — callers must then neither
// publish nor honor a held marker.
//
// Stat-only, never reading hold CONTENTS (Codex round-23 security P2):
// this runs once per authenticated advertisement while a held marker is
// being validated, and the fingerprint the marker semantics need is
// exactly the (path, size, identity) set — every release, append,
// rewrite, or expiry changes it, and no coverage change can happen
// without one of those (refresher touches deliberately do NOT change it;
// see the fold comment below, issue #186 finding 5). Parsing would
// re-read and re-split a 100k-name hold file on EVERY advertisement
// against a paused push, per request, for work whose answer the stats
// already carry. TTL handling mirrors the hold readers: expired files
// drop out, far-future mtimes are clamped live.
func livePushHoldsKey(repoPath string) (string, bool) {
	dir := filepath.Join(repoPath, pushHoldDir)
	var key strings.Builder
	// The lifecycle epoch makes equal endpoint snapshots proof of a
	// STABLE hold set (Codex round-32 P2): per-entry stats alone are
	// ABA-blind — a hold created and released entirely between the
	// bracketing scans (a transient stale-base push) would let a sync
	// that skipped its ref publish a held marker vouching for a hold set
	// that was never stable. Every hold mutator rewrites the epoch file
	// with fresh randomness (bumpPushHoldEpoch), so any lifecycle
	// transition inside the bracket changes the key — clock-granularity
	// independent, unlike a directory mtime. The tiny read stays within
	// the stat-only cost contract.
	//
	// Read BEFORE the directory listing (Codex round-33 P2) and AGAIN
	// after it (round-35 P2), seqlock-style: with a single read on either
	// side of the listing, a transient lifecycle straddling the gap can
	// hand this snapshot a mixed generation — an epoch from before the
	// lifecycle with a listing from after it (or vice versa) reproduces a
	// bracketing key despite the hold set having changed mid-scan. Equal
	// bracketing reads prove the epoch was stable across the whole
	// snapshot; inequality refuses it (ok=false), which every caller
	// treats as "neither publish nor honor a held marker" — the
	// conservative direction, costing one extra revalidation.
	epoch, err := os.ReadFile(dir + pushHoldEpochSuffix)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", false
		}
		epoch = nil
	}
	key.Write(epoch)
	key.WriteByte('\x01')
	if testHookHoldsKeyMidSnapshot != nil {
		testHookHoldsKeyMidSnapshot()
	}
	entries, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", false
	}
	// Tombstone sidecar stats fold into the key (PR #195 round-4 P2 ×2):
	// a tombstone append IS a coverage change — a denial released between
	// the two scans of one hydrate, or a committed DELETION whose hold
	// was suppressing a newly authoritative D/F child — and a marker that
	// cannot see it would keep vouching for skip verdicts the release
	// invalidated. Same stat-only cost contract, same single listing as
	// the holds themselves.
	//
	// The per-file fold is (size, dev, inode) IDENTITY, deliberately not
	// mtime (issue #186 finding 5): every hook of a fanout synchronously
	// touches its hold's mtime (refresher liveness, round-5 on #173), so
	// an mtime-folding key changed once per hook and no held marker could
	// survive the very window range coalescing exists for. Touches are
	// liveness maintenance, never coverage changes; every actual coverage
	// mutation is an append (size grows), a temp+rename rewrite (inode
	// changes), or an unlink (absence — and a lifecycle epoch bump), so
	// identity+size captures them all while staying touch-blind. mtime
	// still gates LIVENESS (pushHoldLive above): an expiring hold leaves
	// the key by omission, not by timestamp.
	released := make(map[string]os.FileInfo)
	for _, e := range entries {
		if base, ok := strings.CutSuffix(e.Name(), pushHoldReleasedSuffix); ok && !e.IsDir() {
			if info, err := e.Info(); err == nil {
				released[filepath.Join(dir, base)] = info
			}
		}
	}
	reverify := make(map[string]holdFileID)
	for _, e := range entries {
		if isPushHoldSidecar(e.Name()) || e.IsDir() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // released between the listing and the stat
			}
			return "", false
		}
		if !pushHoldLive(path, info.ModTime()) {
			continue
		}
		// The sidecar association follows the OWNER path (PR #195
		// round-18 P2): a quarantined hold rides at `push-X.reaping`
		// while its tombstones stay at the base name — every writer and
		// coverage reader uses holdSidecarOwner, and a fingerprint keyed
		// on `push-X.reaping.released` (a path nothing writes) would stay
		// blind to releases landing during the reap window, letting
		// bracketing keys certify a scan whose coverage changed mid-way.
		owner := holdSidecarOwner(path)
		rel := absentHoldFile
		if ri, ok := released[owner]; ok {
			rel = holdFileIdentity(ri)
		}
		id := holdFileIdentity(info)
		reverify[path] = id
		reverify[owner+pushHoldReleasedSuffix] = rel
		fmt.Fprintf(&key, "%s\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x01", path, id.size, id.dev, id.ino, rel.size, rel.dev, rel.ino)
	}
	if testHookHoldsKeyPreClose != nil {
		testHookHoldsKeyPreClose()
	}
	// The closing seqlock read (round-35 P2, see above): any epoch bump
	// that landed between the two reads means the listing may belong to
	// neither generation — refuse the snapshot.
	epoch2, err := os.ReadFile(dir + pushHoldEpochSuffix)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", false
		}
		epoch2 = nil
	}
	if !bytes.Equal(epoch, epoch2) {
		return "", false
	}
	if testHookHoldsKeyPostVerify != nil {
		testHookHoldsKeyPostVerify()
	}
	// The FINAL observation: re-stat every folded path — hold files AND
	// tombstone sidecars — and refuse the snapshot on any change (PR #195
	// rounds 7–8, both loops). The epoch seqlock alone cannot order a
	// lock-free reader against multi-operation writers (a cross-push
	// bump can mask a mutation's own), and a re-stat placed BEFORE the
	// closing epoch read left the gap between them. Placed last, every
	// mutation visible before the snapshot completes refuses it; one
	// landing after post-dates the snapshot entirely — the key is true
	// at publication, and the very next validation recomputes over the
	// changed (size, dev, inode) identity, so a published marker cannot
	// outlive the mutation it missed.
	for spath, want := range reverify {
		got := absentHoldFile
		if fi, err := os.Lstat(spath); err == nil {
			got = holdFileIdentity(fi)
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", false
		}
		if got != want {
			return "", false
		}
	}
	sum := sha256.Sum256([]byte(key.String()))
	return hex.EncodeToString(sum[:]), true
}

// readHeldGenerationMarker mirrors readGenerationMarker for the held
// variant, additionally returning the recorded hold fingerprint.
func readHeldGenerationMarker(repoPath string) (gen int64, head, holdsKey string, fresh bool, ok bool) {
	path := filepath.Join(repoPath, heldGenerationMarker)
	fi, err := os.Stat(path)
	if err != nil {
		return 0, "", "", false, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, "", "", false, false
	}
	fields := strings.Fields(strings.TrimSpace(string(b)))
	if len(fields) != 3 {
		return 0, "", "", false, false
	}
	gen, perr := strconv.ParseInt(fields[0], 10, 64)
	head, holdsKey = fields[1], fields[2]
	if perr != nil || gen < 0 || !strings.HasPrefix(head, "refs/") || holdsKey == "" {
		return 0, "", "", false, false
	}
	return gen, head, holdsKey, time.Since(fi.ModTime()) < generationMarkerTTL, true
}

// heldMarkerValid reports whether a held-skip verdict covers the current
// state: same HEAD target, fresh, the live hold fingerprint is exactly
// the one the recorded scan ran under, and the generation either matches
// exactly or every later one is claimed (issue #186 finding 5) — then
// every skip verdict still stands and a re-scan could only reproduce it.
//
// The range rule: a marker at hgen also covers gen > hgen iff every
// generation in (hgen, gen] is claimed by some LIVE hold's .gens sidecar.
// Each claim is a CASRefGen attribution — that exact generation was
// produced by a won hook CAS on a ref its (still live, fingerprint-
// pinned) hold covers, so the marked scan skipped that ref then and a
// rescan would skip it now: the advertisement cannot have changed. Any
// unclaimed generation — a REST writer, another replica, a completed
// push, a pack registration — forces the healing rescan exactly as
// before. The claims read runs strictly AFTER the fingerprint check:
// claims only ever accrue under a live hold (append-only, removed with
// the hold in lifecycle transitions the epoch fingerprint detects), and
// any hold reserved after the marker was published is visible to the key
// comparison — its reservation strictly precedes its first CAS, so a
// claim inside (hgen, gen] can only belong to a hold both snapshots
// already pinned. A concurrent removal between the two reads costs a
// spurious rescan, never a stale skip.
func heldMarkerValid(repoPath string, gen int64, headTarget string) bool {
	hgen, hhead, hkey, fresh, ok := readHeldGenerationMarker(repoPath)
	if !ok || !fresh || hgen > gen || hhead != headTarget {
		return false
	}
	key, kok := livePushHoldsKey(repoPath)
	if !kok || key != hkey {
		return false
	}
	if hgen == gen {
		return true
	}
	if !liveHoldsClaimRange(repoPath, hgen, gen) {
		return false
	}
	// Best-effort forward rewrite: later validations then verify only the
	// suffix past gen instead of re-walking this whole range. Sound
	// because this validation IS a fresh full observation (fingerprint +
	// claims) — semantically a re-publication of the same scan verdict at
	// the newer generation; a racing hydrate's own marker write simply
	// wins or loses the rename, and the next validation re-derives.
	writeHeldGenerationMarker(repoPath, gen, headTarget, hkey)
	return true
}

// liveHoldsClaimRange reports whether every generation in (hgen, gen] is
// claimed by some LIVE hold's claimed-generation sidecar. Fail-safe by
// construction: unreadable, over-cap, or torn claims read as unclaimed;
// non-live holds' claims are ignored (their pushes are gone and nothing
// vouches for their refs anymore); the sidecar is read at the OWNER path
// so a quarantined hold's claims stay visible through the reap window,
// like every other sidecar consumer (holdSidecarOwner).
func liveHoldsClaimRange(repoPath string, hgen, gen int64) bool {
	assertf(hgen < gen, "liveHoldsClaimRange: empty range (%d, %d]", hgen, gen)
	dir := filepath.Join(repoPath, pushHoldDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	span := gen - hgen
	claimed := make(map[int64]bool, min(span, 4096))
	for _, e := range entries {
		if isPushHoldSidecar(e.Name()) || e.IsDir() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		info, err := e.Info()
		if err != nil || !pushHoldLive(path, info.ModTime()) {
			// Vanished or expired between listing and stat: its claims
			// no longer vouch — skipping only shrinks the claimed set,
			// the rescan direction.
			continue
		}
		// One extra byte past the cap distinguishes at-cap from over-cap
		// (readFileLimit truncates silently); a truncated final record is
		// dropped by the newline framing either way.
		b, rerr := readFileLimit(holdSidecarOwner(path)+pushHoldGensSuffix, maxPushHoldGensBytes+1)
		if rerr != nil {
			if errors.Is(rerr, os.ErrNotExist) {
				continue // a hold with no won CAS yet claims nothing
			}
			return false
		}
		if int64(len(b)) > maxPushHoldGensBytes {
			return false // over the cap: unclaimed, bounded cost
		}
		for len(b) > 0 {
			nl := bytes.IndexByte(b, '\n')
			if nl < 0 {
				break // torn final record: not a claim
			}
			g, perr := strconv.ParseInt(string(b[:nl]), 10, 64)
			b = b[nl+1:]
			if perr != nil || g <= 0 {
				return false // corrupt sidecar: fail safe to a rescan
			}
			if g > hgen && g <= gen {
				claimed[g] = true
			}
		}
	}
	if int64(len(claimed)) < span {
		return false
	}
	for g := hgen + 1; g <= gen; g++ {
		if !claimed[g] {
			return false
		}
	}
	return true
}

// writeHeldGenerationMarker persists the held-skip verdict via temp file
// + rename, like writeGenerationMarker. Best-effort: a lost write only
// costs the next hydrate a re-scan.
func writeHeldGenerationMarker(repoPath string, gen int64, head, holdsKey string) {
	assertf(gen >= 0, "writeHeldGenerationMarker: negative generation %d", gen)
	assertf(strings.HasPrefix(head, "refs/"), "writeHeldGenerationMarker: unqualified HEAD target %q", head)
	assertf(holdsKey != "", "writeHeldGenerationMarker: empty holds key")
	tmp, err := os.CreateTemp(repoPath, heldGenerationMarker+".tmp-*")
	if err != nil {
		return
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op after a successful rename
	if _, err := fmt.Fprintf(tmp, "%d %s %s\n", gen, head, holdsKey); err != nil {
		_ = tmp.Close()
		return
	}
	if err := tmp.Close(); err != nil {
		return
	}
	_ = os.Rename(tmp.Name(), filepath.Join(repoPath, heldGenerationMarker))
}

// writeGenerationMarker persists gen via temp file + rename: concurrent
// hydrators and re-exec'd hook processes share the cache dir, and a reader
// must never see a torn write (a crash mid-write leaves only a temp file).
// Best-effort by design — on failure the marker is simply absent or stale
// and the next hydrate does the full sync; a lost write can cost work but
// never staleness.
func writeGenerationMarker(repoPath string, gen int64, head string) {
	assertf(gen >= 0, "writeGenerationMarker: negative generation %d", gen)
	assertf(strings.HasPrefix(head, "refs/"), "writeGenerationMarker: unqualified HEAD target %q", head)
	tmp, err := os.CreateTemp(repoPath, generationMarker+".tmp-*")
	if err != nil {
		return
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op after a successful rename
	if _, err := fmt.Fprintf(tmp, "%d %s\n", gen, head); err != nil {
		_ = tmp.Close()
		return
	}
	if err := tmp.Close(); err != nil {
		return
	}
	if err := os.Rename(tmp.Name(), filepath.Join(repoPath, generationMarker)); err == nil {
		// A full success supersedes any held-skip verdict.
		_ = os.Remove(filepath.Join(repoPath, heldGenerationMarker))
	}
}

// Hydrate rebuilds the cache repo state for repo from PG + S3 (the stateless
// node property): ensure repo, hydrate packs, then sync refs.
// On the receive path a concurrent receive-pack may hold ref locks while we
// sync; retry briefly, and let the caller decide whether a residual failure
// is fatal (upload path) or tolerable staleness (receive path, where git
// re-validates old SHAs itself).
//
// Fast path (issue #4): when the store generation AND the HEAD symref
// target derived from defaultBranch (issue #18) match the local marker
// written by the last fully successful hydrate — and the marker is within
// its TTL — the packs, refs, and HEAD are provably unchanged and Hydrate
// returns after one point-read, skipping ListPacks, per-pack stats,
// the paged ListRefsAfter scan, and the for-each-ref/update-ref/
// symbolic-ref subprocesses.
// EnsureCacheRepo still runs unconditionally: hook and config repair must
// stay verified on every request (the hook script embeds this binary's
// path, which changes across deployments), and it touches no DB state.
// After the first full pass its verification is memoized fingerprint stats
// (issue #93), so the fast path as a whole forks zero subprocesses.
//
// defaultBranch is the repo's default branch as resolved from the meta row
// for THIS request. A PATCH /repo landing after that read can transiently
// lose to this hydrate's older value (each full sync records the HEAD it
// actually set, so the marker never lies), and the next request — which
// resolves the row fresh — repairs HEAD with one full sync.
//
// Full syncs serialize per repo on <repoPath>/codestorage.hydrate.lock:
// two overlapping full hydrates spanning a mutation could otherwise
// interleave so the OLDER ref sync lands after the newer one while the
// newer marker survives — and the marker would then short-circuit reads
// onto stale refs for up to a TTL window. Under the lock, each full sync
// re-reads the generation before listing refs and publishes its marker
// before releasing, so marker and refs advance monotonically together. The
// marker read stays lock-free (temp+rename keeps it atomic): the idle fast
// path must not serialize on an exclusive lock.
func Hydrate(ctx context.Context, st ref.RefStore, bl blob.BlobStore, cacheDir, repo, defaultBranch string) (repoPath string, refsErr error, err error) {
	return hydrateRefs(ctx, st, bl, cacheDir, repo, defaultBranch, false)
}

// ErrHeldRefStale is the refsErr of a strict hydrate whose ref sync
// skipped a push-held ref: the cache keeps a value the authority no
// longer has (a sync only touches refs whose values differ), and the
// hold may belong to a push whose CAS fails, so nothing would heal it
// before the caller acts on the stale value.
var ErrHeldRefStale = errors.New("a push holds a ref whose cached value is older than the authority")

// hydrateRefs is Hydrate. With strictHeld (a push run's loopback fetch,
// Server.hydrate), a sync that skips a held ref returns
// ErrHeldRefStale, and a held-skip verdict never short-cuts the sync.
// Hold and marker handling is the same in both modes.
func hydrateRefs(ctx context.Context, st ref.RefStore, bl blob.BlobStore, cacheDir, repo, defaultBranch string, strictHeld bool) (repoPath string, refsErr error, err error) {
	headTarget := HeadTarget(defaultBranch)
	repoPath, err = EnsureCacheRepoCtx(ctx, cacheDir, repo)
	if err != nil {
		return "", nil, err
	}
	// Snapshot-capable stores (issue #182) serve generation, packs, and
	// refs from ONE read: on s3wal each portable call below re-fetched
	// the whole base manifest — one WAN round trip per call on real S3.
	// The freshness discipline is unchanged: both marker checks read
	// live state, and the marker publishes exactly the state the sync
	// consumed. The PRE-lock marker check stays on GetGeneration for
	// every backend: it is the cache-hit hot path, needs only the
	// generation, and materializing a snapshot there would sort and copy
	// the full ref/pack sets per request without saving a round trip.
	snapper, _ := st.(ref.HydrateSnapshotter)
	readState := func() (int64, *ref.HydrateSnapshot, error) {
		if snapper == nil {
			gen, err := st.GetGeneration(ctx, repo)
			return gen, nil, err
		}
		snap, err := snapper.HydrateSnapshot(ctx, repo)
		if err != nil {
			return 0, nil, err
		}
		return snap.Generation, &snap, nil
	}
	gen, err := st.GetGeneration(ctx, repo)
	if err != nil {
		return "", nil, err
	}
	if markerGen, markerHead, fresh, ok := readGenerationMarker(repoPath); ok && fresh && markerGen == gen && markerHead == headTarget {
		return repoPath, nil, nil
	}
	if !strictHeld && heldMarkerValid(repoPath, gen, headTarget) {
		return repoPath, nil, nil
	}
	// Full-sync critical section. The lock file is left in place, like the
	// per-pack locks: removing it would race a concurrent locker onto a
	// deleted inode. Lock order is repo hydrate lock -> per-pack locks;
	// nothing acquires them in the opposite order. The wait is cancelable:
	// a request whose client hung up must not stay parked behind a slow
	// hydrate holding the repo-wide lock. Keep the physical name across
	// product renames so old and new binaries still serialize together.
	unlock, err := flockExclusiveCtx(ctx, filepath.Join(repoPath, "codestorage.hydrate.lock"))
	if err != nil {
		return "", nil, fmt.Errorf("lock hydrate %s: %w", repo, err)
	}
	defer unlock()
	// Re-check under the lock: the hydrator we waited on may have already
	// synced this generation (or newer — re-read, don't reuse the pre-lock
	// value). Generation read strictly BEFORE the pack/ref reads below:
	// the marker this hydrate publishes must never claim newer state than
	// it actually synced. Mutations landing after this read leave
	// marker < generation, which just forces the next hydrate down the
	// full path again.
	var snap *ref.HydrateSnapshot
	gen, snap, err = readState()
	if err != nil {
		return "", nil, err
	}
	if markerGen, markerHead, fresh, ok := readGenerationMarker(repoPath); ok && fresh && markerGen == gen && markerHead == headTarget {
		return repoPath, nil, nil
	}
	if !strictHeld && heldMarkerValid(repoPath, gen, headTarget) {
		return repoPath, nil, nil
	}
	if snap != nil {
		// One coherent state feeds packs, refs, AND the marker: the
		// missing-blob retry re-snapshots wholesale (a concurrent fold
		// replaced rows), so gen/refs advance in lockstep with the pack
		// list actually materialized.
		first := true
		if err := hydratePacksBy(ctx, bl, repoPath, repo, func(ctx context.Context) ([]ref.Pack, error) {
			if !first {
				fresh, err := snapper.HydrateSnapshot(ctx, repo)
				if err != nil {
					return nil, err
				}
				*snap, gen = fresh, fresh.Generation
			}
			first = false
			return snap.Packs, nil
		}); err != nil {
			return "", nil, err
		}
	} else if err := HydratePacks(ctx, st, bl, repoPath, repo); err != nil {
		return "", nil, err
	}
	// Refs stream straight from the store in bounded pages (issue #119):
	// ref fanout is attacker-scaled, so the serving path must never
	// materialize a repo's whole ref set — except through the snapshot
	// capability, whose ref sets are bounded by construction (see
	// ref.HydrateSnapshotter). The generation read above binds the paged
	// scan: any ref minted or moved while pages stream bumps the
	// generation past the marker this sync publishes, so the next hydrate
	// takes the full path again and heals whatever the torn scan missed —
	// the same staleness window a point-snapshot ListRefs already had
	// between its read and the marker write.
	newStore := func() refSeq { return &storeRefSeq{ctx: ctx, st: st, repo: repo, pageSize: syncRefsBatch} }
	var skippedHeld bool
	if snap != nil {
		newStore = func() refSeq { return &entryRefSeq{refs: snap.Refs} }
	}
	// Fingerprint the live holds BEFORE the scan: a held-skip verdict is
	// reusable only if the holds were stable across the WHOLE scan (an
	// early flush's skip under a hold released mid-scan would otherwise
	// be vouched for by a fingerprint that still matches).
	preHoldsKey, preHoldsOK := livePushHoldsKey(repoPath)
	// Collect the distinct SHAs the sync streams (issue #307): every pass
	// drains the whole store sequence, so after a successful sync tips holds
	// exactly the tips it applied (or is over the cap). The commit-graph
	// refresh below walks only these, never the live ref namespace.
	var tips *refTips
	uncollected := newStore
	collectingStore := func() refSeq {
		tips = newRefTips()
		return &tipCollectingRefSeq{inner: uncollected(), tips: tips}
	}
	skippedHeld, refsErr, scanErr := syncRefsWithCleanup(ctx, repoPath, headTarget, collectingStore, syncRefsBatch)
	if scanErr != nil {
		return "", nil, scanErr
	}
	if refsErr == nil && !skippedHeld {
		// Strictly after full success — a failed hydrate must never advance
		// the marker — and strictly before the lock releases, so the next
		// locker's recheck sees exactly the state this sync produced. The
		// rename also refreshes the mtime, restarting the TTL window.
		//
		// A sync that skipped a push-held ref (issue #170) is NOT full
		// success: its marker could vouch for a generation whose held ref
		// it never applied (the CAS may have committed before the
		// generation read above), and if that push then dies before its own
		// local ref update, the cache would serve the stale ref under a
		// fresh marker for a whole TTL. No marker — the next hydrate
		// full-syncs again and heals the ref once the hold clears.
		writeGenerationMarker(repoPath, gen, headTarget)
		// Refresh the commit-graph (issue #307, ADR-47) after the marker,
		// so lock-free fast-path readers are not held up by it, and still
		// under the hydrate lock, which serializes every graph writer for
		// this repo. Best-effort: failures are logged, never returned.
		refreshCommitGraph(ctx, repoPath, gitlimit.RepoKey(BaseRepo(repo)), tips)
	} else if refsErr == nil && skippedHeld && preHoldsOK {
		// Held-skip verdict (Codex round-22 security P2, see
		// heldGenerationMarker): reusable by later hydrates while the
		// generation, HEAD target, and the exact hold fingerprint are
		// unchanged — but only when the holds were provably stable
		// across the scan (pre == post), else fall back to the
		// pre-existing no-marker behavior.
		if postHoldsKey, ok := livePushHoldsKey(repoPath); ok && postHoldsKey == preHoldsKey {
			writeHeldGenerationMarker(repoPath, gen, headTarget, postHoldsKey)
		}
	}
	if refsErr == nil && skippedHeld && strictHeld {
		refsErr = ErrHeldRefStale
	}
	return repoPath, refsErr, nil
}
