//go:build !linux

package githttp

import (
	"os"
	"time"
)

// fileChangeTime on non-Linux platforms (darwin dev/release-verify builds)
// returns the zero time for every file: ctime comparison degrades to a
// no-op and the memo falls back to inode/mtime/size/mode fingerprints.
// Production serves run on Linux (the release image), where the real ctime
// check in ctime_linux.go detects metadata-preserving in-place rewrites.
func fileChangeTime(os.FileInfo) time.Time {
	return time.Time{}
}
