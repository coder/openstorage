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
// hydration may remove it (issue #464). Git holds a ref lock for the
// milliseconds of one ref transaction, so a lock this old was left by a
// killed git (SIGKILL inside the transaction, the OOM killer, a crash).
const staleRefLockAge = time.Minute

// removeStaleRefLocks removes the stale locks that block an update-ref
// batch for names in the cache repo at repoPath (issue #464): a git killed
// inside its ref transaction leaves `<ref>.lock` (or `packed-refs.lock`)
// behind, and nothing else removes it, so every later hydration of the
// repository fails. The caller holds the repository's hydrate lock and
// filtered out push-held refs. A lock is removed only when it is older
// than staleRefLockAge and no process works in the repository (see
// processInRepo): every git that can own a cache-repo ref lock
// (receive-pack, its hooks, hydration, the REST writers) runs that way.
// Without that proof (no /proc, an unreadable process table) nothing is
// removed. exclude is a PID that cannot own a ref lock (hydration's own
// for-each-ref), or 0. It returns the number of locks removed.
func removeStaleRefLocks(repoPath string, names []string, exclude int) int {
	type staleLock struct {
		path string
		fi   os.FileInfo
	}
	var stale []staleLock
	for _, p := range refLockPaths(repoPath, names) {
		fi, err := os.Lstat(p)
		if err != nil || !fi.Mode().IsRegular() || time.Since(fi.ModTime()) < staleRefLockAge {
			continue
		}
		stale = append(stale, staleLock{p, fi})
	}
	if len(stale) == 0 {
		return 0
	}
	if pid, err := processInRepo(repoPath, exclude); err != nil {
		log.Printf("hydrate %s: keeping %d old ref lock(s): cannot check for processes in the repository: %v", repoPath, len(stale), err)
		return 0
	} else if pid != 0 {
		log.Printf("hydrate %s: keeping %d old ref lock(s): process %d works in the repository", repoPath, len(stale), pid)
		return 0
	}
	removed := 0
	for _, l := range stale {
		// The pathname must still name the inspected file: a lock that a
		// new writer created after the old one went away is live.
		if fi, err := os.Lstat(l.path); err != nil || !os.SameFile(fi, l.fi) || !fi.ModTime().Equal(l.fi.ModTime()) {
			continue
		}
		if err := os.Remove(l.path); err != nil {
			log.Printf("hydrate %s: remove stale ref lock: %v", repoPath, err)
			continue
		}
		log.Printf("hydrate %s: removed stale ref lock %s (older than %v, no process works in the repository)", repoPath, strings.TrimPrefix(l.path, repoPath+string(filepath.Separator)), staleRefLockAge)
		removed++
	}
	return removed
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
