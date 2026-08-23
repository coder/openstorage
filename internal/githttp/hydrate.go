package githttp

import (
	"bytes"
	"context"
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

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
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
// runGitCtx and updateRefStdin — the only two exec sites reachable from
// Hydrate. Test seam for issue #93's guarantee that the generation-match
// fast path forks no subprocess; an atomic pointer so installing the
// observer never races a concurrent hydrator.
var gitSpawnObserver atomic.Pointer[func(dir string, args []string)]

func observeGitSpawn(dir string, args []string) {
	if obs := gitSpawnObserver.Load(); obs != nil {
		(*obs)(dir, args)
	}
}

// runGit executes git with args, returning stdout only. Stderr is captured
// separately and surfaced in the error text: git warnings on stderr must
// never contaminate parsed stdout (e.g. for-each-ref in SyncRefs).
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
	observeGitSpawn(dir, args)
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
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
	config     os.FileInfo
	preReceive os.FileInfo
	update     os.FileInfo
}

// cacheRepoMemos maps cache repo paths to their verified fingerprints, one
// small entry per repo this process serves. Concurrent full passes may
// both store; either snapshot is a state a full pass verified, and the
// same-value config writes make the passes idempotent.
var cacheRepoMemos sync.Map // map[string]cacheRepoMemo

// memoFiles pairs each fingerprint with the path it must still match.
func (m cacheRepoMemo) memoFiles(repoPath string) [3]struct {
	fi   os.FileInfo
	path string
} {
	return [3]struct {
		fi   os.FileInfo
		path string
	}{
		{m.config, filepath.Join(repoPath, "config")},
		{m.preReceive, filepath.Join(repoPath, "hooks", "pre-receive")},
		{m.update, filepath.Join(repoPath, "hooks", "update")},
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
	for _, name := range strings.Split(names, "\n") {
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
		return nil, errors.New("config file changed underneath the verification reads")
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

// EnsureCacheRepoCtx is EnsureCacheRepo honoring ctx in its git
// subprocesses (init, config): the repack fold's materialization runs the
// setup under the fold's wall-clock budget, and a stalled subprocess or
// filesystem must not outlive it.
func EnsureCacheRepoCtx(ctx context.Context, cacheDir, repo string) (string, error) {
	path := RepoPath(cacheDir, repo)
	if m, ok := cacheRepoMemos.Load(path); ok {
		if cacheRepoUnchanged(path, m.(cacheRepoMemo)) {
			return path, nil
		}
		// Stale memo: the repo was recreated, repaired by another process,
		// or a verified file changed underneath us. Drop it and take the
		// full verify/repair pass below, which re-memoizes on success.
		cacheRepoMemos.Delete(path)
	}
	if _, err := os.Stat(filepath.Join(path, "HEAD")); err != nil {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return "", err
		}
		if _, err := runGitCtx(ctx, "", "init", "--bare", "--initial-branch=main", path); err != nil {
			return "", err
		}
	}
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
		// never persist (PR #98 round-5 review). Other residual errors —
		// the file changing under a racing full pass — stay non-fatal:
		// serve, skip memoization, and the next call re-verifies.
		if errors.Is(configErr, errConfigValueMismatch) {
			return "", fmt.Errorf("cache repo %s: pinned config overridden after repair: %w", repo, configErr)
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
	} {
		script := fmt.Sprintf(hookScript, self, hook.name)
		fi, err := ensureHook(filepath.Join(hooksDir, hook.name), script)
		if err != nil {
			return "", err
		}
		assertf(fi != nil, "ensureHook returned no FileInfo for %s", hook.name)
		*hook.fi = fi
	}
	// Memoize only a config state some verifyLocalConfig pass proved: the
	// fingerprint identifies the exact unchanged file the reads covered
	// (PR #98 round-3 review). A raced rewrite leaves configErr set and
	// skips memoization entirely — hooks included, so the whole memo only
	// ever vouches for verified state.
	if configErr == nil {
		assertf(configFI != nil, "verifyLocalConfig returned no FileInfo for %s", configPath)
		memo.config = configFI
		cacheRepoMemos.Store(path, memo)
	}
	return path, nil
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
// stop repairing it (PR #98 review, P1). The path is then confirmed to
// still name that same inode before the fingerprint may be memoized;
// otherwise fall through and install our own script, exactly as if the
// content had not matched.
func ensureHook(path, script string) (os.FileInfo, error) {
	if f, err := os.Open(path); err == nil {
		cur, rerr := io.ReadAll(f)
		fi, serr := f.Stat()
		_ = f.Close()
		if rerr == nil && serr == nil && string(cur) == script && fi.Mode().Perm() == 0o755 {
			if pathFI, err := os.Stat(path); err == nil && os.SameFile(fi, pathFI) {
				return fi, nil
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
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		return err
	}
	var lastMissing error
	for attempt := 0; attempt < maxHydrateRaces; attempt++ {
		packs, err := st.ListPacks(ctx, repo)
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

// SyncRefs makes the local ref store match PG exactly (PG wins): updates
// changed refs, deletes locals that PG does not know, and points HEAD at
// headTarget (the repo's default branch per the meta row, qualified by
// HeadTarget). Must run after HydratePacks so targets exist locally.
func SyncRefs(repoPath string, want map[string]string, headTarget string) error {
	assertf(strings.HasPrefix(headTarget, "refs/"), "SyncRefs: unqualified HEAD target %q", headTarget)
	out, err := runGit(repoPath, "for-each-ref", "--format=%(refname) %(objectname)")
	if err != nil {
		return err
	}
	have := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		name, sha, ok := strings.Cut(line, " ")
		assertf(ok && ref.ValidSHA(sha), "for-each-ref emitted %q", line)
		have[name] = sha
	}
	// Deletions run first, in their own update-ref transaction: git's files
	// backend cannot delete refs/heads/foo and create refs/heads/foo/bar in
	// one transaction (the doomed loose ref still blocks the new ref's lock
	// file), so a D/F rename in PG would wedge local sync forever otherwise.
	// Partial application is fine — SyncRefs is idempotent over a
	// rebuildable cache and callers retry. Sorted for deterministic order.
	var dels []string
	for name := range have {
		if _, ok := want[name]; !ok {
			dels = append(dels, name)
		}
	}
	sort.Strings(dels)
	if len(dels) > 0 {
		var b strings.Builder
		for _, name := range dels {
			fmt.Fprintf(&b, "delete %s\n", name)
		}
		if err := updateRefStdin(repoPath, b.String()); err != nil {
			return err
		}
	}
	var b strings.Builder
	// Deterministic order for debuggability.
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if have[name] != want[name] {
			fmt.Fprintf(&b, "update %s %s\n", name, want[name])
		}
	}
	if b.Len() > 0 {
		if err := updateRefStdin(repoPath, b.String()); err != nil {
			return err
		}
	}
	// HEAD follows the meta row's default branch (issue #18). git tolerates
	// a dangling target: an unborn default branch advertises no HEAD.
	if _, err := runGit(repoPath, "symbolic-ref", "HEAD", headTarget); err != nil {
		return err
	}
	return nil
}

// updateRefStdin feeds commands to one `git update-ref --stdin` transaction.
func updateRefStdin(repoPath, commands string) error {
	observeGitSpawn(repoPath, []string{"update-ref", "--stdin"})
	cmd := exec.Command("git", "update-ref", "--stdin")
	cmd.Dir = repoPath
	cmd.Stdin = strings.NewReader(commands)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("update-ref --stdin: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
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
	_ = os.Rename(tmp.Name(), filepath.Join(repoPath, generationMarker))
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
// ListRefs, and the for-each-ref/update-ref/symbolic-ref subprocesses.
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
	headTarget := HeadTarget(defaultBranch)
	repoPath, err = EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		return "", nil, err
	}
	gen, err := st.GetGeneration(ctx, repo)
	if err != nil {
		return "", nil, err
	}
	if markerGen, markerHead, fresh, ok := readGenerationMarker(repoPath); ok && fresh && markerGen == gen && markerHead == headTarget {
		return repoPath, nil, nil
	}
	// Full-sync critical section. The lock file is left in place, like the
	// per-pack locks: removing it would race a concurrent locker onto a
	// deleted inode. Lock order is repo hydrate lock -> per-pack locks;
	// nothing acquires them in the opposite order. The wait is cancelable:
	// a request whose client hung up must not stay parked behind a slow
	// hydrate holding the repo-wide lock.
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
	gen, err = st.GetGeneration(ctx, repo)
	if err != nil {
		return "", nil, err
	}
	if markerGen, markerHead, fresh, ok := readGenerationMarker(repoPath); ok && fresh && markerGen == gen && markerHead == headTarget {
		return repoPath, nil, nil
	}
	if err := HydratePacks(ctx, st, bl, repoPath, repo); err != nil {
		return "", nil, err
	}
	refs, err := st.ListRefs(ctx, repo)
	if err != nil {
		return "", nil, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		if refsErr = SyncRefs(repoPath, refs, headTarget); refsErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if refsErr == nil {
		// Strictly after full success — a failed hydrate must never advance
		// the marker — and strictly before the lock releases, so the next
		// locker's recheck sees exactly the state this sync produced. The
		// rename also refreshes the mtime, restarting the TTL window.
		writeGenerationMarker(repoPath, gen, headTarget)
	}
	return repoPath, refsErr, nil
}
