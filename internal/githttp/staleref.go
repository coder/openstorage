package githttp

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// staleRefLockAge is how old a ref lock in a cache repo must be before
// hydration reports it (issue #464). Git holds a ref lock for the
// milliseconds of one ref transaction, so a lock this old was most likely
// left by a killed git (SIGKILL inside the transaction, the OOM killer, a
// crash).
const staleRefLockAge = time.Minute

// reportOldRefLocks logs one line when locks older than staleRefLockAge
// can block a failed update-ref batch for names in the cache repo at
// repoPath (issue #464), so an operator can follow the stale-ref-lock
// runbook in docs/how-to/deploy-production.md. The line carries only a
// count: a batch can leave thousands of locks, and their paths come from
// ref names. It never removes a lock: it cannot prove that no live git
// owns one. It returns the number of old locks.
func reportOldRefLocks(repoPath string, names []string) int {
	n := 0
	for _, p := range refLockPaths(repoPath, names) {
		if fi, err := os.Lstat(p); err == nil && fi.Mode().IsRegular() && time.Since(fi.ModTime()) >= staleRefLockAge {
			n++
		}
	}
	if n > 0 {
		log.Printf("hydrate %s: %d ref lock(s) older than %v block ref updates; a killed git can leave them behind: see \"Stale ref locks\" in docs/how-to/deploy-production.md", repoPath, n, staleRefLockAge)
	}
	return n
}

// refLockPaths lists the lock files that can block an update-ref batch for
// names: each loose ref's lock, the locks below it (a stale
// `refs/heads/foo/bar.lock` keeps the directory that blocks creating
// `refs/heads/foo`), and packed-refs.lock (deletions rewrite packed-refs).
// Names that do not map to a path inside refs/ are skipped.
func refLockPaths(repoPath string, names []string) []string {
	paths := []string{filepath.Join(repoPath, "packed-refs.lock")}
	for _, name := range names {
		if !strings.HasPrefix(name, "refs/") || filepath.Clean(name) != name || strings.Contains(name, "..") {
			continue
		}
		p := filepath.Join(repoPath, filepath.FromSlash(name))
		paths = append(paths, p+".lock")
		_ = filepath.WalkDir(p, func(sub string, d fs.DirEntry, err error) error {
			if err == nil && d.Type().IsRegular() && strings.HasSuffix(sub, ".lock") {
				paths = append(paths, sub)
			}
			return nil // a missing or unreadable directory just has no locks to offer
		})
	}
	return paths
}
