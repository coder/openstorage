package githttp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// SyncRefs must delete local refs that PG does not know about (PG wins).
func TestSyncRefs_DeletesStaleLocalRef(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	mustGit(t, dir, "branch", "stale")

	if err := SyncRefs(dir, map[string]string{"refs/heads/main": head}); err != nil {
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
	}); err != nil {
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
	for i := 0; i < n; i++ {
		go func() {
			errs <- HydratePacks(ctx, testStore, testBlob, repoPath, repo)
		}()
	}
	for i := 0; i < n; i++ {
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
