package githttp

import (
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
// than staleRefLockAge and no process works in the repository (its
// working directory is inside it, or its command line names it): every
// git that can own a cache-repo ref lock (receive-pack, its hooks,
// hydration, the REST writers) runs that way. Without that proof (no
// /proc, an unreadable process table) nothing is removed. It returns the
// number of locks removed.
func removeStaleRefLocks(repoPath string, names []string) int {
	var stale []string
	for _, p := range refLockPaths(repoPath, names) {
		fi, err := os.Lstat(p)
		if err != nil || !fi.Mode().IsRegular() || time.Since(fi.ModTime()) < staleRefLockAge {
			continue
		}
		stale = append(stale, p)
	}
	if len(stale) == 0 {
		return 0
	}
	if pid, err := processInRepo(repoPath); err != nil || pid != 0 {
		log.Printf("hydrate %s: keeping %d old ref lock(s): process %d works in the repository (%v)", repoPath, len(stale), pid, err)
		return 0
	}
	removed := 0
	for _, p := range stale {
		if err := os.Remove(p); err != nil {
			log.Printf("hydrate %s: remove stale ref lock: %v", repoPath, err)
			continue
		}
		log.Printf("hydrate %s: removed stale ref lock %s (older than %v, no process works in the repository)", repoPath, strings.TrimPrefix(p, repoPath+string(filepath.Separator)), staleRefLockAge)
		removed++
	}
	return removed
}

// refLockPaths lists the lock files that can block an update-ref batch for
// names: each loose ref's lock, and packed-refs.lock (deletions rewrite
// packed-refs). Names that do not map to a path inside refs/ are skipped.
func refLockPaths(repoPath string, names []string) []string {
	paths := []string{filepath.Join(repoPath, "packed-refs.lock")}
	for _, name := range names {
		if !strings.HasPrefix(name, "refs/") || filepath.Clean(name) != name || strings.Contains(name, "..") {
			continue
		}
		paths = append(paths, filepath.Join(repoPath, filepath.FromSlash(name))+".lock")
	}
	return paths
}
