//go:build !linux && !darwin

package githttp

import (
	"os"
	"time"
)

// fileChangeTime on platforms without a dedicated implementation returns
// the zero time for every file: ctime comparison degrades to a no-op and
// the memo falls back to inode/mtime/size/mode fingerprints. Both
// supported serving platforms get the real check — Linux (the release
// image, ctime_linux.go) and Darwin (published and natively verified
// binaries, ctime_darwin.go).
func fileChangeTime(os.FileInfo) time.Time {
	return time.Time{}
}
