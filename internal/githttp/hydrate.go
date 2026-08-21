package githttp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// A repo URL element is a base repo name optionally followed by one
// namespace suffix, `+ephemeral` or `+import` (docs/05). Namespaced repos
// use fully separate ref stores, pack spaces, and cache dirs keyed by the
// full name — docs/05 allows "git namespaces or separate ref stores", and
// separate stores give the required isolation for free.
var repoNameRe = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]*(\+(ephemeral|import))?\z`)

// ValidRepoName guards against path traversal via the URL repo element.
func ValidRepoName(repo string) bool {
	return repoNameRe.MatchString(repo) && !strings.Contains(repo, "..")
}

// BaseRepo strips the optional namespace suffix: JWTs carry the base repo id
// in the `repo` claim and are valid for its namespaces (docs/05 URL scheme).
func BaseRepo(repo string) string {
	base, _, _ := strings.Cut(repo, "+")
	return base
}

// RepoPath maps a repo name to its bare cache repo directory.
func RepoPath(cacheDir, repo string) string {
	assertf(ValidRepoName(repo), "RepoPath: invalid repo name %q", repo)
	return filepath.Join(cacheDir, repo+".git")
}

// runGit executes git with args, returning stdout only. Stderr is captured
// separately and surfaced in the error text: git warnings on stderr must
// never contaminate parsed stdout (e.g. for-each-ref in SyncRefs).
func runGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
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
func EnsureCacheRepo(cacheDir, repo string) (string, error) {
	path := RepoPath(cacheDir, repo)
	if _, err := os.Stat(filepath.Join(path, "HEAD")); err != nil {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return "", err
		}
		if _, err := runGit("", "init", "--bare", "--initial-branch=main", path); err != nil {
			return "", err
		}
	}
	// Every push must yield a pack file (>= 0 received objects keeps the pack).
	for _, kv := range [][2]string{
		{"receive.unpackLimit", "0"},
		{"gc.auto", "0"},
	} {
		// Read-check first: config writes take config.lock, and concurrent
		// hydrates of the same repo must not fail on lock contention in the
		// steady-state already-configured case.
		if cur, err := runGit(path, "config", "--get", kv[0]); err == nil && strings.TrimSpace(cur) == kv[1] {
			continue
		}
		if _, err := runGit(path, "config", kv[0], kv[1]); err != nil {
			return "", err
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
	for _, name := range []string{"pre-receive", "update"} {
		script := fmt.Sprintf(hookScript, self, name)
		if err := ensureHook(filepath.Join(hooksDir, name), script); err != nil {
			return "", err
		}
	}
	return path, nil
}

// ensureHook installs the hook script unless it is already current and
// executable (git silently skips non-executable hooks, which would bypass
// enforcement). Writes go through a temp file + rename so a concurrent
// receive-pack never execs a partially written script.
func ensureHook(path, script string) error {
	if cur, err := os.ReadFile(path); err == nil && string(cur) == script {
		if fi, err := os.Stat(path); err == nil && fi.Mode().Perm() == 0o755 {
			return nil
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(script); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

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
func HydratePacks(ctx context.Context, st ref.RefStore, bl blob.BlobStore, repoPath, repo string) error {
	packs, err := st.ListPacks(ctx, repo)
	if err != nil {
		return err
	}
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		return err
	}
	for _, p := range packs {
		base := filepath.Join(packDir, "pack-"+p.ID)
		if _, err := os.Stat(base + ".idx"); err == nil {
			continue // already hydrated
		}
		if err := hydrateOnePack(ctx, bl, repoPath, base, p); err != nil {
			return err
		}
	}
	return nil
}

// hydrateOnePack materializes one registered pack (pack + idx) at base under
// an exclusive flock on <base>.pack.lock. The lock file is left in place:
// removing it would race a concurrent locker onto a deleted inode.
func hydrateOnePack(ctx context.Context, bl blob.BlobStore, repoPath, base string, p ref.Pack) error {
	unlock, err := flockExclusive(base + ".pack.lock")
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
	if out, err := runGit(repoPath, "index-pack", tmpPack); err != nil {
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

// SyncRefs makes the local ref store match PG exactly (PG wins): updates
// changed refs, deletes locals that PG does not know, and points HEAD at
// refs/heads/main. Must run after HydratePacks so targets exist locally.
func SyncRefs(repoPath string, want map[string]string) error {
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
	// HEAD always points at main (default branch handling is a later phase).
	if _, err := runGit(repoPath, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		return err
	}
	return nil
}

// updateRefStdin feeds commands to one `git update-ref --stdin` transaction.
func updateRefStdin(repoPath, commands string) error {
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
const (
	generationMarker    = "codestorage.generation"
	generationMarkerTTL = 5 * time.Minute
)

// readGenerationMarker returns the marker's recorded generation and whether
// its mtime — refreshed only by full successful hydrates, never by
// short-circuited ones — is still within the TTL window. ok=false means no
// usable marker (missing, malformed, or unreadable): callers full-hydrate,
// and the next success rewrites it.
func readGenerationMarker(repoPath string) (gen int64, fresh bool, ok bool) {
	path := filepath.Join(repoPath, generationMarker)
	fi, err := os.Stat(path)
	if err != nil {
		return 0, false, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false, false
	}
	gen, perr := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if perr != nil || gen < 0 {
		return 0, false, false
	}
	return gen, time.Since(fi.ModTime()) < generationMarkerTTL, true
}

// writeGenerationMarker persists gen via temp file + rename: concurrent
// hydrators and re-exec'd hook processes share the cache dir, and a reader
// must never see a torn write (a crash mid-write leaves only a temp file).
// Best-effort by design — on failure the marker is simply absent or stale
// and the next hydrate does the full sync; a lost write can cost work but
// never staleness.
func writeGenerationMarker(repoPath string, gen int64) {
	assertf(gen >= 0, "writeGenerationMarker: negative generation %d", gen)
	tmp, err := os.CreateTemp(repoPath, generationMarker+".tmp-*")
	if err != nil {
		return
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op after a successful rename
	if _, err := fmt.Fprintf(tmp, "%d\n", gen); err != nil {
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
// Fast path (issue #4): when the store generation matches the local marker
// written by the last fully successful hydrate — and the marker is within
// its TTL — the packs and refs are provably unchanged and Hydrate returns
// after one point-read, skipping ListPacks, per-pack stats, ListRefs, and
// the for-each-ref/update-ref/symbolic-ref subprocesses. EnsureCacheRepo
// still runs unconditionally: hook and config repair must stay verified on
// every request (the hook script embeds this binary's path, which changes
// across deployments), and it touches no DB state.
func Hydrate(ctx context.Context, st ref.RefStore, bl blob.BlobStore, cacheDir, repo string) (repoPath string, refsErr error, err error) {
	repoPath, err = EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		return "", nil, err
	}
	// Generation read strictly BEFORE the pack/ref reads below: the marker
	// this hydrate may write must never claim newer state than it actually
	// synced. Mutations landing after this read leave marker < generation,
	// which just forces the next hydrate down the full path.
	gen, err := st.GetGeneration(ctx, repo)
	if err != nil {
		return "", nil, err
	}
	if markerGen, fresh, ok := readGenerationMarker(repoPath); ok && fresh && markerGen == gen {
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
		if refsErr = SyncRefs(repoPath, refs); refsErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if refsErr == nil {
		// Strictly after full success — a failed hydrate must never advance
		// the marker. The rename also refreshes the mtime, restarting the
		// TTL window. Residual race, TTL-bounded by construction: a
		// hydrator that dies between SyncRefs and this write can strand a
		// concurrent hydrator's newer marker over the older refs it just
		// synced; the marker is a heuristic, the TTL is the staleness bound.
		writeGenerationMarker(repoPath, gen)
	}
	return repoPath, refsErr, nil
}
