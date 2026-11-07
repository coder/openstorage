package githttp

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/metrics"
)

// staleRefLockAge is how old a ref lock in a cache repo must be before
// cleanStaleRefLocks considers it (issues #464 and #490). Git holds a ref
// lock for the milliseconds of one ref transaction, so a lock this old was
// most likely left by a killed git (SIGKILL inside the transaction, the OOM
// killer, a crash).
const staleRefLockAge = time.Minute

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

// staleLocksDir is the quarantine directory of a cache repository (issue
// #490). It is outside refs/, so `git for-each-ref` never lists a file in it.
const staleLocksDir = "codestorage-stale-locks"

// Test seams for the window between cleanup's look at a lock and its move
// (staleLockBeforeMove), and between the move and the identity check
// (staleLockAfterMove). Production never sets them.
var (
	staleLockBeforeMove atomic.Pointer[func(lock string)]
	staleLockAfterMove  atomic.Pointer[func(lock string)]
)

func staleLockSeam(seam *atomic.Pointer[func(string)], lock string) {
	if f := seam.Load(); f != nil {
		(*f)(lock)
	}
}

// staleRefCleanup is the result of one cleanStaleRefLocks run.
type staleRefCleanup struct {
	// Old is the number of ref locks older than staleRefLockAge that the run
	// found. A run that found none did nothing: it took no lease, logged no
	// line and counted no outcome, and Outcome means nothing.
	Old     int
	Removed int
	Outcome metrics.StaleRefLockOutcome
}

// cleanStaleRefLocks removes the ref locks that can block an update-ref batch
// for names in the cache repository at repoPath (issue #490) when no git that
// serve started can own them. The caller holds the repository's hydrate lock
// and has reaped its own git children.
//
// The proof is the lease (gitlimit.LeaseFile). Winning it exclusively means
// that no git that serve started is alive in the repository, and none starts
// until the lease is released. When the lease is busy the run keeps every
// lock (outcome busy). It also keeps a lock that is not newer than the lease
// epoch (gitlimit.EpochFile), because a git without a lease can own it.
//
// What the proof leaves open is a git that holds no lease and has a lock
// newer than the epoch: a git run by hand in the cache, or a git of an older
// binary that outlived its server and started a new transaction after the
// first lease-aware start. The age gate (staleRefLockAge) and the identity
// check below narrow it, and the operator rules in
// docs/how-to/deploy-production.md close it: stop the old binary and its git
// children before the first start of a binary with the lease.
//
// A lock is moved into the quarantine directory with a rename, and the moved
// file's device and inode are compared with the lock that was inspected.
// Only a match is deleted. A path that was inspected and then deleted by name
// can be a different file by then. A moved file that does not match is linked
// back to its path, which fails when a new lock appeared there, and is kept.
//
// A run that finds an old lock logs one line and counts one outcome, never
// one per lock (a failed batch can leave thousands of locks), also when it
// fails: an error counts as kept.
func cleanStaleRefLocks(repoPath string, names []string, now time.Time) (res staleRefCleanup, err error) {
	paths := refLockPaths(repoPath, names)
	old := oldRefLocks(paths, now)
	if len(old) == 0 {
		return res, nil
	}
	res = staleRefCleanup{Old: len(old), Outcome: metrics.StaleRefLockKept}
	replaced := 0
	defer func() {
		if res.Old == 0 {
			return
		}
		if res.Removed > 0 {
			res.Outcome = metrics.StaleRefLockRemoved
		}
		metrics.StaleRefLockCleanup(res.Outcome)
		switch {
		case err != nil:
			log.Printf("hydrate %s: removed %d of %d ref lock(s) older than %v, then stopped: %v", repoPath, res.Removed, res.Old, staleRefLockAge, err)
		case res.Outcome == metrics.StaleRefLockBusy:
			log.Printf("hydrate %s: %d ref lock(s) older than %v kept: a git that serve started is still running in the repository", repoPath, res.Old, staleRefLockAge)
		default:
			log.Printf("hydrate %s: removed %d of %d ref lock(s) older than %v (%d replaced while moved); the others stay: see \"Stale ref locks\" in docs/how-to/deploy-production.md", repoPath, res.Removed, res.Old, staleRefLockAge, replaced)
		}
	}()

	lease, err := gitlimit.TryExclusive(repoPath)
	if errors.Is(err, gitlimit.ErrLeaseBusy) {
		res.Outcome = metrics.StaleRefLockBusy
		return res, nil
	}
	if err != nil {
		return res, fmt.Errorf("take the git lease: %w", err)
	}
	defer lease.Close()

	// A lock can change between the first look and the lease: inspect again.
	old = oldRefLocks(paths, now)
	if len(old) == 0 {
		res = staleRefCleanup{}
		return res, nil
	}
	res.Old = len(old)
	epoch, err := gitlimit.LeaseEpoch(repoPath)
	if err != nil {
		return res, err
	}
	for _, lock := range old {
		if !lock.fi.ModTime().After(epoch) {
			continue
		}
		moved, err := quarantineRefLock(repoPath, lock.path, lock.fi)
		if err != nil {
			return res, fmt.Errorf("move a ref lock: %w", err)
		}
		switch moved {
		case lockDeleted:
			res.Removed++
		case lockReplaced:
			replaced++
		}
	}
	return res, nil
}

type refLock struct {
	path string
	fi   fs.FileInfo
}

// oldRefLocks returns the regular files among paths that are older than
// staleRefLockAge, as inspected now.
func oldRefLocks(paths []string, now time.Time) []refLock {
	var out []refLock
	for _, p := range paths {
		if fi, err := os.Lstat(p); err == nil && fi.Mode().IsRegular() && now.Sub(fi.ModTime()) >= staleRefLockAge {
			out = append(out, refLock{p, fi})
		}
	}
	return out
}

type quarantineResult int

const (
	lockGone     quarantineResult = iota // nothing at the path to move
	lockDeleted                          // the inspected file, moved and deleted
	lockReplaced                         // another file was moved, and it is back at its path or in quarantine
)

// quarantineRefLock moves the lock at path, which was inspected as fi, into
// the quarantine directory, and deletes it when it is still that file. It
// returns an error when the quarantine directory cannot be made, a move fails
// for a reason other than the lock being gone, or a replaced file cannot be
// put back although no new lock took its path.
func quarantineRefLock(repoPath, path string, fi fs.FileInfo) (quarantineResult, error) {
	dir := filepath.Join(repoPath, staleLocksDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return lockGone, err
	}
	q, err := os.CreateTemp(dir, "lock-*")
	if err != nil {
		return lockGone, err
	}
	quarantined := q.Name()
	if err := q.Close(); err != nil {
		return lockGone, err
	}
	staleLockSeam(&staleLockBeforeMove, path)
	if err := os.Rename(path, quarantined); err != nil {
		_ = os.Remove(quarantined)
		if errors.Is(err, fs.ErrNotExist) {
			return lockGone, nil
		}
		return lockGone, err
	}
	staleLockSeam(&staleLockAfterMove, path)
	if got, err := os.Lstat(quarantined); err == nil && os.SameFile(fi, got) {
		return lockDeleted, os.Remove(quarantined)
	}
	// Link, unlike rename, fails when a new lock took the path: that lock
	// stays, and the moved file stays in quarantine. Any other failure left
	// the path empty and the moved file, which may be a live writer's lock,
	// only in quarantine: report it.
	switch err := os.Link(quarantined, path); {
	case err == nil:
		_ = os.Remove(quarantined)
	case !errors.Is(err, fs.ErrExist):
		return lockReplaced, fmt.Errorf("put back %s: %w", path, err)
	}
	return lockReplaced, nil
}
