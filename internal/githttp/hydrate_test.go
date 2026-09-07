package githttp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/open-storage/internal/blob"
	"github.com/coder/open-storage/internal/ref"
)

// SyncRefs must delete local refs that PG does not know about (PG wins).
func TestSyncRefs_DeletesStaleLocalRef(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	mustGit(t, dir, "branch", "stale")

	if err := SyncRefs(dir, map[string]string{"refs/heads/main": head}, "refs/heads/main"); err != nil {
		t.Fatalf("SyncRefs: %v", err)
	}

	out := mustGit(t, dir, "for-each-ref", "--format=%(refname) %(objectname)")
	if strings.Contains(out, "refs/heads/stale") {
		t.Fatalf("stale local ref survived sync:\n%s", out)
	}
	if !strings.Contains(out, "refs/heads/main "+head) {
		t.Fatalf("refs/heads/main missing or moved:\n%s", out)
	}
}

// SyncRefs must point HEAD at the given target (issue #18: the default
// branch is per-repo state from the meta row, no longer hardcoded main).
func TestSyncRefs_PointsHeadAtTarget(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")

	if err := SyncRefs(dir, map[string]string{
		"refs/heads/main": head,
		"refs/heads/dev":  head,
	}, "refs/heads/dev"); err != nil {
		t.Fatalf("SyncRefs: %v", err)
	}

	out := mustGit(t, dir, "symbolic-ref", "HEAD")
	if got := strings.TrimSpace(out); got != "refs/heads/dev" {
		t.Fatalf("HEAD = %q, want refs/heads/dev", got)
	}
}

// HeadTarget qualifies short branch names and passes refs/-prefixed values
// through verbatim (openapi.yaml PATCH repo).
func TestHeadTarget(t *testing.T) {
	if got := HeadTarget("main"); got != "refs/heads/main" {
		t.Fatalf("HeadTarget(main) = %q, want refs/heads/main", got)
	}
	if got := HeadTarget("refs/heads/dev"); got != "refs/heads/dev" {
		t.Fatalf("HeadTarget(refs/heads/dev) = %q, want refs/heads/dev", got)
	}
}

// A ref replaced across a directory/file boundary (refs/heads/foo deleted,
// refs/heads/foo/bar created in PG) must sync: deletions have to run before
// updates and in their own update-ref transaction, because git's files
// backend rejects the delete+create pair in one transaction.
func TestSyncRefs_DirectoryFileConflict(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	mustGit(t, dir, "branch", "foo")

	if err := SyncRefs(dir, map[string]string{
		"refs/heads/main":    head,
		"refs/heads/foo/bar": head,
	}, "refs/heads/main"); err != nil {
		t.Fatalf("SyncRefs: %v", err)
	}

	out := mustGit(t, dir, "for-each-ref", "--format=%(refname) %(objectname)")
	if strings.Contains(out, "refs/heads/foo "+head) {
		t.Fatalf("refs/heads/foo survived sync:\n%s", out)
	}
	if !strings.Contains(out, "refs/heads/foo/bar "+head) {
		t.Fatalf("refs/heads/foo/bar missing:\n%s", out)
	}
}

// EnsureCacheRepo must repair hooks and config even when HEAD already
// exists: a crash between `git init` and hook installation (or a deleted
// hook file) must not yield a receive-pack that bypasses the S3 upload, PG
// registration, and policy hooks.
func TestEnsureCacheRepo_RepairsHooksAndConfig(t *testing.T) {
	cacheDir := t.TempDir()
	repoPath, err := EnsureCacheRepo(cacheDir, "t-ensure-repair")
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}

	// Simulate a crash right after `git init`: hooks gone, config reverted.
	for _, name := range []string{"pre-receive", "update"} {
		if err := os.Remove(filepath.Join(repoPath, "hooks", name)); err != nil {
			t.Fatal(err)
		}
	}
	mustGit(t, repoPath, "config", "--unset", "receive.unpackLimit")

	again, err := EnsureCacheRepo(cacheDir, "t-ensure-repair")
	if err != nil {
		t.Fatalf("EnsureCacheRepo (repair): %v", err)
	}
	if again != repoPath {
		t.Fatalf("repo path changed on repair: %q != %q", again, repoPath)
	}
	for _, name := range []string{"pre-receive", "update"} {
		fi, err := os.Stat(filepath.Join(repoPath, "hooks", name))
		if err != nil {
			t.Fatalf("hook %s not reinstalled: %v", name, err)
		}
		if fi.Mode().Perm() != 0o755 {
			t.Fatalf("hook %s not executable: %v", name, fi.Mode())
		}
	}
	if got := strings.TrimSpace(mustGit(t, repoPath, "config", "--get", "receive.unpackLimit")); got != "0" {
		t.Fatalf("receive.unpackLimit = %q, want 0", got)
	}
}

// A memoized "verified" cache repo whose directory vanishes underneath the
// process (operator wipe of the rebuildable cache — the stateless-node
// property) must fall through to the full pass and recreate everything,
// never crash and never vouch for the missing repo (issue #93).
func TestEnsureCacheRepo_MemoFallsThroughOnRepoRemoval(t *testing.T) {
	cacheDir := t.TempDir()
	const repo = "t-ensure-recreate"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	// Second call takes the memo fast path and must agree on the path.
	if again, err := EnsureCacheRepo(cacheDir, repo); err != nil || again != repoPath {
		t.Fatalf("memoized EnsureCacheRepo = (%q, %v), want (%q, nil)", again, err, repoPath)
	}

	if err := os.RemoveAll(repoPath); err != nil {
		t.Fatal(err)
	}

	again, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo (recreate): %v", err)
	}
	if again != repoPath {
		t.Fatalf("repo path changed on recreate: %q != %q", again, repoPath)
	}
	for _, name := range []string{"pre-receive", "update"} {
		fi, err := os.Stat(filepath.Join(repoPath, "hooks", name))
		if err != nil {
			t.Fatalf("hook %s not reinstalled after recreate: %v", name, err)
		}
		if fi.Mode().Perm() != 0o755 {
			t.Fatalf("hook %s not executable: %v", name, fi.Mode())
		}
	}
	if got := strings.TrimSpace(mustGit(t, repoPath, "config", "--get", "receive.unpackLimit")); got != "0" {
		t.Fatalf("receive.unpackLimit = %q, want 0", got)
	}
}

// An in-place hook rewrite landing between ensureHook's content read and
// its descriptor stat (same inode — no rename) must not be memoized as
// verified: the read returned the good script but the fingerprint captures
// the replacement, and the memo would then vouch for content nobody
// checked (PR #98 round-9 review). The hookVerifyBarrier seam injects the
// write into exactly that window.
func TestEnsureCacheRepo_HookRewriteDuringVerifyNotMemoized(t *testing.T) {
	cacheDir := t.TempDir()
	const repo = "t-ensure-hookrace"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	hook := filepath.Join(repoPath, "hooks", "pre-receive")
	good, err := os.ReadFile(hook)
	if err != nil {
		t.Fatal(err)
	}
	// Force the next call down the full verification path.
	cacheRepoMemos.Delete(repoPath)

	var raced atomic.Bool
	barrier := func(path string) {
		if path != hook || raced.Swap(true) {
			return
		}
		// In-place O_TRUNC rewrite: same inode, hostile content.
		if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Errorf("in-place hook rewrite: %v", err)
		}
	}
	hookVerifyBarrier.Store(&barrier)
	defer hookVerifyBarrier.Store(nil)

	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (raced): %v", err)
	}
	if !raced.Load() {
		t.Fatal("race injection never fired: hook verification read not observed")
	}
	hookVerifyBarrier.Store(nil)

	// Whether the raced call repaired inline or declined to memoize, the
	// hostile hook must not survive behind a memoized fast path.
	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (after race): %v", err)
	}
	cur, err := os.ReadFile(hook)
	if err != nil {
		t.Fatal(err)
	}
	if string(cur) != string(good) {
		t.Fatalf("hostile hook survived verification race:\n%s", cur)
	}
}

// A memo hit must still observe an expired context (PR #98 round-9
// review): before memoization even the steady-state call reached runGitCtx
// and propagated cancellation, and the repack materializer's fold budget
// relies on that contract.
func TestEnsureCacheRepoCtx_MemoHitObservesCancellation(t *testing.T) {
	cacheDir := t.TempDir()
	const repo = "t-ensure-memocancel"
	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (memoize): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := EnsureCacheRepoCtx(ctx, cacheDir, repo); !errors.Is(err, context.Canceled) {
		t.Fatalf("memo-hit EnsureCacheRepoCtx = %v, want context.Canceled", err)
	}
}

// Context expiry during the post-repair re-verification must propagate
// (PR #98 round-8 review): the repack materializer runs EnsureCacheRepoCtx
// under its wall-clock budget, and swallowing the ctx error as a "benign
// residual race" would return success from a call whose budget expired.
// The observer cancels the context right after the second repair write, so
// the re-verify's first subprocess fails with the ctx error in its chain.
func TestEnsureCacheRepoCtx_PropagatesCancellationDuringReverify(t *testing.T) {
	cacheDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The observer runs before each exec: count the two repair writes,
	// then cancel on the NEXT read spawn — the re-verify's first
	// subprocess — so both writes complete and the ctx error surfaces
	// inside verifyLocalConfig itself.
	var writes atomic.Int32
	obs := func(dir string, args []string) {
		if len(args) == 0 || args[0] != "config" {
			return
		}
		line := strings.Join(args, " ")
		if strings.Contains(line, "--replace-all") {
			writes.Add(1)
			return
		}
		if writes.Load() >= 2 && strings.Contains(line, "--get-all") {
			cancel() // budget expires as the re-verify starts
		}
	}
	gitSpawnObserver.Store(&obs)
	defer gitSpawnObserver.Store(nil)

	// Fresh repo: the first verification fails (values unset), forcing the
	// repair loop and the re-verify the cancellation lands on.
	_, err := EnsureCacheRepoCtx(ctx, cacheDir, "t-ensure-cancel")
	if err == nil {
		t.Fatal("EnsureCacheRepoCtx returned success although its context expired mid-verification")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureCacheRepoCtx = %v, want context.Canceled in the chain", err)
	}
}

// Duplicate definitions where the TRAILING value is the safe pin (git's
// last-definition-wins: `unpackLimit = 1` then `= 0` is effectively 0)
// must keep serving and memoizing — the pre-memo code accepted them, and
// rejecting would make every EnsureCacheRepo fail because a single-value
// repair write cannot overwrite multiple values (PR #98 round-6 review).
// A duplicate whose trailing value is HOSTILE must be repaired.
func TestEnsureCacheRepo_AcceptsTrailingSafeDuplicateConfig(t *testing.T) {
	cacheDir := t.TempDir()
	const repo = "t-ensure-dup"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}

	// Stale duplicate BEFORE the effective safe value.
	mustGit(t, repoPath, "config", "--add", "receive.unpackLimit", "1")
	mustGit(t, repoPath, "config", "--add", "receive.unpackLimit", "0")

	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (trailing-safe duplicates): %v", err)
	}
	if _, ok := cacheRepoMemos.Load(repoPath); !ok {
		t.Fatal("trailing-safe duplicate config was not memoized")
	}
	if got := strings.TrimSpace(mustGit(t, repoPath, "config", "--get", "receive.unpackLimit")); got != "0" {
		t.Fatalf("effective receive.unpackLimit = %q, want 0", got)
	}

	// Trailing HOSTILE duplicate: must be repaired, not served as-is.
	mustGit(t, repoPath, "config", "--add", "receive.unpackLimit", "1")
	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (trailing-hostile duplicate): %v", err)
	}
	if got := strings.TrimSpace(mustGit(t, repoPath, "config", "--get", "receive.unpackLimit")); got != "0" {
		t.Fatalf("effective receive.unpackLimit = %q after repair, want 0", got)
	}
}

// A local-scope [include] can override the pinned values for receive-pack
// (which follows includes) while `--local --get` without --includes still
// reports the direct pins — and the included file lives OUTSIDE the memo's
// fingerprint, so its later drift would be invisible (PR #98 round-5
// review). A config with include directives must therefore never be
// memoized, and verification must resolve includes so an override defeats
// it: every call re-checks, fail closed.
func TestEnsureCacheRepo_LocalIncludeDisablesMemoization(t *testing.T) {
	cacheDir := t.TempDir()
	const repo = "t-ensure-include"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}

	// Hostile include appended AFTER the pinned values: the included
	// definition wins for receive-pack.
	inc := filepath.Join(repoPath, "hostile.inc")
	if err := os.WriteFile(inc, []byte("[receive]\n\tunpackLimit = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(repoPath, "config"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("[include]\n\tpath = hostile.inc\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// The edit changed the config fingerprint, so this call re-verifies
	// with includes resolved. The override survives the direct repair
	// write (the include still wins), which is corruption-grade: fail
	// loudly rather than serve pushes that unpack loose objects the hooks
	// never persist.
	if _, err := EnsureCacheRepo(cacheDir, repo); err == nil {
		t.Fatal("EnsureCacheRepo served a repo whose pinned config an include overrides")
	} else if !errors.Is(err, errConfigValueMismatch) {
		t.Fatalf("EnsureCacheRepo (hostile include) = %v, want errConfigValueMismatch", err)
	}
	if _, ok := cacheRepoMemos.Load(repoPath); ok {
		t.Fatal("memoized a config whose effective values an included file controls")
	}

	// Benign include (no pinned keys): serves, but never memoized — the
	// included file could later grow an override without touching the
	// fingerprinted file.
	if err := os.WriteFile(inc, []byte("[user]\n\tname = ops\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (benign include): %v", err)
	}
	if _, ok := cacheRepoMemos.Load(repoPath); ok {
		t.Fatal("memoized a config containing include directives")
	}
}

// An in-place rewrite that preserves every classic stat attribute — same
// inode (no rename), same byte length, mtime restored via Chtimes (the
// `touch -r` pattern) — must still invalidate the memo (PR #98 round-4
// review): only ctime, which no unprivileged rewrite can restore, tells
// such a tampered file apart, and a fast path trusting it would leave a
// hostile receive.unpackLimit active for the process lifetime.
func TestEnsureCacheRepo_MemoInvalidatedByMetadataPreservingRewrite(t *testing.T) {
	cacheDir := t.TempDir()
	const repo = "t-ensure-ctime"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	// Prime the memo fast path.
	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (memoize): %v", err)
	}

	configPath := filepath.Join(repoPath, "config")
	before, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	// Same-length hostile edit, written in place (same inode), mtime
	// restored: every attribute but ctime matches the memo.
	tampered := strings.Replace(string(old), "unpackLimit = 0", "unpackLimit = 1", 1)
	if tampered == string(old) || len(tampered) != len(old) {
		t.Fatalf("fixture drift: cannot build a same-length tamper of %q", string(old))
	}
	f, err := os.OpenFile(configPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte(tampered), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(configPath, time.Time{}, before.ModTime()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("tamper was not metadata-preserving: %+v vs %+v", before, after)
	}

	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (after tamper): %v", err)
	}
	if got := strings.TrimSpace(mustGit(t, repoPath, "config", "--local", "--get", "receive.unpackLimit")); got != "0" {
		t.Fatalf("receive.unpackLimit = %q after metadata-preserving tamper, want 0 repinned", got)
	}
}

// A concurrent rewrite of <repo>/config landing between the verification
// reads and the fingerprint stat must never be memoized as verified (PR
// #98 round-3 review): the memo would then vouch indefinitely for values
// nobody checked — a hostile receive.unpackLimit would let pushes unpack
// into loose objects the hooks never upload. The gitSpawnObserver seam
// injects the race deterministically: after the first config value is read
// and verified, the file is replaced with one whose already-read value is
// hostile while the not-yet-read value still matches.
func TestEnsureCacheRepo_RacedConfigRewriteNotMemoized(t *testing.T) {
	cacheDir := t.TempDir()
	const repo = "t-ensure-raced-config"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	// Force the next call down the full verification path.
	cacheRepoMemos.Delete(repoPath)

	var raced atomic.Bool
	obs := func(dir string, args []string) {
		line := strings.Join(args, " ")
		// Fire once, on the gc.auto read — receive.unpackLimit was already
		// read and verified from the pre-race file by then.
		if dir != repoPath || !strings.Contains(line, "--get-all gc.auto") || raced.Swap(true) {
			return
		}
		tmp := filepath.Join(repoPath, "config.raced")
		if err := os.WriteFile(tmp, []byte("[receive]\n\tunpackLimit = 1\n[gc]\n\tauto = 0\n"), 0o644); err != nil {
			t.Errorf("write raced config: %v", err)
			return
		}
		if err := os.Rename(tmp, filepath.Join(repoPath, "config")); err != nil {
			t.Errorf("rename raced config: %v", err)
		}
	}
	gitSpawnObserver.Store(&obs)
	defer gitSpawnObserver.Store(nil)

	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (raced): %v", err)
	}
	if !raced.Load() {
		t.Fatal("race injection never fired: no gc.auto config read observed")
	}
	gitSpawnObserver.Store(nil)

	// Whether the raced call repaired inline or declined to memoize, the
	// next call must observe and repin the hostile value — it must NOT
	// fast-path over a fingerprint nobody verified.
	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (after race): %v", err)
	}
	if got := strings.TrimSpace(mustGit(t, repoPath, "config", "--local", "--get", "receive.unpackLimit")); got != "0" {
		t.Fatalf("receive.unpackLimit = %q after raced rewrite, want 0 repinned", got)
	}
}

// Config values supplied by global/system git config must still be written
// to the LOCAL config file: the memo fingerprints only <repo>/config, so a
// value satisfied from an external scope would freeze the decision against
// later drift of that scope (PR #98 round-2 review). Pinning locally also
// outranks global/system at receive-pack time, so drift cannot re-enable
// unpacking pushes into loose objects even on the full pass.
func TestEnsureCacheRepo_PinsConfigLocallyDespiteGlobalConfig(t *testing.T) {
	global := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(global, []byte("[receive]\n\tunpackLimit = 0\n[gc]\n\tauto = 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", global)

	cacheDir := t.TempDir()
	repoPath, err := EnsureCacheRepo(cacheDir, "t-ensure-pin-local")
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	for key, want := range map[string]string{"receive.unpackLimit": "0", "gc.auto": "0"} {
		got := strings.TrimSpace(mustGit(t, repoPath, "config", "--local", "--get", key))
		if got != want {
			t.Fatalf("local config %s = %q, want %q pinned despite global scope supplying it", key, got, want)
		}
	}
}

// A bare chmod changes neither the hook's inode nor its mtime/size, and git
// silently skips non-executable hooks — the memo compares the mode exactly
// so a memoized fast path can never vouch for a disarmed hook (issue #93).
func TestEnsureCacheRepo_MemoInvalidatedByHookChmod(t *testing.T) {
	cacheDir := t.TempDir()
	const repo = "t-ensure-chmod"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	// Prime the memo fast path.
	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (memoize): %v", err)
	}

	hook := filepath.Join(repoPath, "hooks", "pre-receive")
	if err := os.Chmod(hook, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := EnsureCacheRepo(cacheDir, repo); err != nil {
		t.Fatalf("EnsureCacheRepo (repair): %v", err)
	}
	fi, err := os.Stat(hook)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o755 {
		t.Fatalf("disarmed hook not repaired: mode %v, want 0755", fi.Mode())
	}
}

// Regression for the corrupt-pack race: a corrupt pack file left at the
// hydration target (crashed downloader, partial write) must never wedge
// hydration. The temp-file + rename scheme downloads fresh bytes beside it
// and atomically replaces it, so hydrate succeeds on the FIRST attempt.
func TestHydratePacks_ReplacesCorruptPack(t *testing.T) {
	ctx := context.Background()
	repo := registerRepo(t, "t-hydrate-corrupt")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	packs := pgPacks(t, repo)
	if len(packs) == 0 {
		t.Fatal("push registered no packs")
	}

	// Fresh cache repo with a corrupt pack pre-placed at the hydration target.
	repoPath, err := EnsureCacheRepo(t.TempDir(), repo)
	if err != nil {
		t.Fatalf("ensure cache repo: %v", err)
	}
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		t.Fatal(err)
	}
	corrupt := filepath.Join(packDir, "pack-"+packs[0].ID+".pack")
	if err := os.WriteFile(corrupt, []byte("not a packfile"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := HydratePacks(ctx, testStore, testBlob, repoPath, repo); err != nil {
		t.Fatalf("hydrate with corrupt pre-placed pack failed: %v", err)
	}
	for _, p := range packs {
		idx := filepath.Join(packDir, "pack-"+p.ID+".idx")
		if _, err := os.Stat(idx); err != nil {
			t.Fatalf("pack %s not indexed: %v", p.ID, err)
		}
	}
	// The corrupt bytes were replaced by the real pack.
	if out := mustGit(t, repoPath, "verify-pack", "-v", corrupt); out == "" {
		t.Fatal("verify-pack produced no output for replaced pack")
	}
	// No stray temp files survive a successful hydrate.
	leftovers, err := filepath.Glob(filepath.Join(packDir, "*.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}
}

// Concurrent hydrators of the same pack must serialize on the per-pack lock
// and both succeed (no partial-file os.Remove race, no double index-pack
// failure).
func TestHydratePacks_ConcurrentHydratorsSerialize(t *testing.T) {
	ctx := context.Background()
	repo := registerRepo(t, "t-hydrate-concurrent")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	repoPath, err := EnsureCacheRepo(t.TempDir(), repo)
	if err != nil {
		t.Fatalf("ensure cache repo: %v", err)
	}

	const n = 4
	errs := make(chan error, n)
	for range n {
		go func() {
			errs <- HydratePacks(ctx, testStore, testBlob, repoPath, repo)
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent hydrate failed: %v", err)
		}
	}
	packs := pgPacks(t, repo)
	if len(packs) == 0 {
		t.Fatal("push registered no packs")
	}
	for _, p := range packs {
		idx := filepath.Join(repoPath, "objects", "pack", "pack-"+p.ID+".idx")
		if _, err := os.Stat(idx); err != nil {
			t.Fatalf("pack %s not indexed: %v", p.ID, err)
		}
	}
}

// scriptedPacks is a RefStore stub whose ListPacks returns successive
// scripted listings (the last one repeats). Unused RefStore methods panic
// via the nil embedded interface.
type scriptedPacks struct {
	ref.RefStore
	lists [][]ref.Pack
	calls int
}

func (s *scriptedPacks) ListPacks(context.Context, string) ([]ref.Pack, error) {
	s.calls++
	if s.calls > len(s.lists) {
		return s.lists[len(s.lists)-1], nil
	}
	return s.lists[s.calls-1], nil
}

// failDownloads fails every DownloadFile with err. Nothing else is called on
// the nil embedded interface once the download path errors.
type failDownloads struct {
	blob.BlobStore
	err error
}

func (f *failDownloads) DownloadFile(context.Context, string, string) error { return f.err }

// buildTestPack repacks a fresh work repo into a single pack file and
// returns its path (real, index-able pack bytes for hydration tests).
func buildTestPack(t *testing.T) string {
	t.Helper()
	work := initWorkRepo(t)
	mustGit(t, work, "repack", "-a", "-d")
	packs, err := filepath.Glob(filepath.Join(work, ".git", "objects", "pack", "*.pack"))
	if err != nil || len(packs) != 1 {
		t.Fatalf("repack produced packs %v (err %v), want exactly one", packs, err)
	}
	return packs[0]
}

// A pack blob deleted between ListPacks and download (folded away by a
// repack) must not fail hydration: the bounded retry re-lists and hydrates
// the replacement pack (issue #6).
func TestHydratePacks_RetriesOnMissingPackBlob(t *testing.T) {
	ctx := context.Background()
	const repo = "t-hydrate-race"

	bl, err := blob.OpenFile("file://" + t.TempDir())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer bl.Close()
	replacement := ref.Pack{ID: "new", Key: blob.PackKey(repo, "new")}
	if err := bl.UploadFile(ctx, replacement.Key, buildTestPack(t)); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	// The first listing names a pack whose blob no longer exists; the
	// re-listing after the not-found download carries the replacement.
	folded := ref.Pack{ID: "old", Key: blob.PackKey(repo, "old")}
	st := &scriptedPacks{lists: [][]ref.Pack{{folded}, {replacement}}}

	repoPath, err := EnsureCacheRepo(t.TempDir(), repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	if err := HydratePacks(ctx, st, bl, repoPath, repo); err != nil {
		t.Fatalf("hydrate across a pack fold failed: %v", err)
	}
	if st.calls != 2 {
		t.Fatalf("ListPacks called %d times, want 2 (initial + one retry)", st.calls)
	}
	idx := filepath.Join(repoPath, "objects", "pack", "pack-"+replacement.ID+".idx")
	if _, err := os.Stat(idx); err != nil {
		t.Fatalf("replacement pack not indexed: %v", err)
	}
}

// A registered pack whose blob is genuinely gone (still listed on every
// retry) must exhaust the bound and fail with ErrPackMissing — never loop
// forever, never surface as a generic download failure.
func TestHydratePacks_MissingPackExhaustsBound(t *testing.T) {
	ctx := context.Background()
	const repo = "t-hydrate-gone"

	bl, err := blob.OpenFile("file://" + t.TempDir())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer bl.Close()
	gone := ref.Pack{ID: "gone", Key: blob.PackKey(repo, "gone")}
	st := &scriptedPacks{lists: [][]ref.Pack{{gone}}}

	repoPath, err := EnsureCacheRepo(t.TempDir(), repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	err = HydratePacks(ctx, st, bl, repoPath, repo)
	if !errors.Is(err, ErrPackMissing) {
		t.Fatalf("hydrate of permanently missing pack = %v, want ErrPackMissing", err)
	}
	if st.calls != maxHydrateRaces {
		t.Fatalf("ListPacks called %d times, want %d (the bound)", st.calls, maxHydrateRaces)
	}
	if !strings.Contains(err.Error(), gone.ID) {
		t.Fatalf("error must name the missing pack %q: %v", gone.ID, err)
	}
}

// Transport errors must keep failing fast: no re-list, no retry, and no
// misclassification as a missing pack.
func TestHydratePacks_TransportErrorFailsFast(t *testing.T) {
	ctx := context.Background()
	const repo = "t-hydrate-transport"

	transportErr := errors.New("connection reset by peer")
	st := &scriptedPacks{lists: [][]ref.Pack{{{ID: "x", Key: blob.PackKey(repo, "x")}}}}

	repoPath, err := EnsureCacheRepo(t.TempDir(), repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	err = HydratePacks(ctx, st, &failDownloads{err: transportErr}, repoPath, repo)
	if !errors.Is(err, transportErr) {
		t.Fatalf("hydrate with failing transport = %v, want the transport error", err)
	}
	if errors.Is(err, ErrPackMissing) {
		t.Fatalf("transport error misclassified as missing pack: %v", err)
	}
	if st.calls != 1 {
		t.Fatalf("ListPacks called %d times, want 1 (transport errors are not retried)", st.calls)
	}
}
