package spike

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var repoNameRe = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]*\z`)

// ValidRepoName guards against path traversal via the URL repo element.
func ValidRepoName(repo string) bool {
	return repoNameRe.MatchString(repo) && !strings.Contains(repo, "..")
}

// RepoPath maps a repo name to its bare cache repo directory.
func RepoPath(cacheDir, repo string) string {
	assertf(ValidRepoName(repo), "RepoPath: invalid repo name %q", repo)
	return filepath.Join(cacheDir, repo+".git")
}

// runGit executes git with args, returning combined output for diagnostics.
func runGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

const hookScript = `#!/bin/sh
exec %q hook %s "$@"
`

// EnsureCacheRepo idempotently creates the bare cache repo with spike config
// and hooks that re-exec this binary (no shell/psql deps in hooks).
func EnsureCacheRepo(cacheDir, repo string) (string, error) {
	path := RepoPath(cacheDir, repo)
	if _, err := os.Stat(filepath.Join(path, "HEAD")); err == nil {
		return path, nil
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", err
	}
	if _, err := runGit("", "init", "--bare", "--initial-branch=main", path); err != nil {
		return "", err
	}
	// Every push must yield a pack file (>= 0 received objects keeps the pack).
	for _, kv := range [][2]string{
		{"receive.unpackLimit", "0"},
		{"gc.auto", "0"},
	} {
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
		if err := os.WriteFile(filepath.Join(hooksDir, name), []byte(script), 0o755); err != nil {
			return "", err
		}
	}
	return path, nil
}

// HydratePacks downloads and indexes every registered pack that is not
// already present in the cache repo. Packs are self-contained (index-pack
// --fix-thin ran on the receiving node before upload), so each is
// independently indexable.
func HydratePacks(ctx context.Context, st *Store, bl *Blob, repoPath, repo string) error {
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
		packFile := base + ".pack"
		if _, err := os.Stat(packFile); err != nil {
			if err := bl.DownloadFile(ctx, p.S3Key, packFile); err != nil {
				return err
			}
		}
		if out, err := runGit(repoPath, "index-pack", packFile); err != nil {
			return fmt.Errorf("index-pack %s: %w (%s)", p.ID, err, strings.TrimSpace(out))
		}
	}
	return nil
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
		assertf(ok && shaRe.MatchString(sha), "for-each-ref emitted %q", line)
		have[name] = sha
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
	for name := range have {
		if _, ok := want[name]; !ok {
			fmt.Fprintf(&b, "delete %s\n", name)
		}
	}
	if b.Len() > 0 {
		cmd := exec.Command("git", "update-ref", "--stdin")
		cmd.Dir = repoPath
		cmd.Stdin = strings.NewReader(b.String())
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("update-ref --stdin: %w: %s", err, strings.TrimSpace(string(out)))
		}
	}
	// Spike-local: HEAD always points at main.
	if _, err := runGit(repoPath, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		return err
	}
	return nil
}

// Hydrate rebuilds the cache repo state for repo from PG + S3 (the stateless
// node property): ensure repo, hydrate packs, then sync refs.
// On the receive path a concurrent receive-pack may hold ref locks while we
// sync; retry briefly, and let the caller decide whether a residual failure
// is fatal (upload path) or tolerable staleness (receive path, where git
// re-validates old SHAs itself).
func Hydrate(ctx context.Context, st *Store, bl *Blob, cacheDir, repo string) (repoPath string, refsErr error, err error) {
	repoPath, err = EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		return "", nil, err
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
	return repoPath, refsErr, nil
}
