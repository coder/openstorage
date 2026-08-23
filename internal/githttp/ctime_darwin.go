//go:build darwin

package githttp

import (
	"os"
	"syscall"
	"time"
)

// fileChangeTime returns the inode change time (ctime) on Darwin, which
// spells the Stat_t field Ctimespec. Darwin is a supported serving
// platform, not just a cross-compile target — release.yml publishes and
// natively verifies macOS binaries and the README documents bare-binary
// local serving — so the metadata-preserving-rewrite check must hold here
// too (PR #98 round-7 review). See ctime_linux.go for the invariant.
func fileChangeTime(fi os.FileInfo) time.Time {
	st, ok := fi.Sys().(*syscall.Stat_t)
	assertf(ok, "fileChangeTime: unexpected FileInfo.Sys type %T", fi.Sys())
	sec, nsec := st.Ctimespec.Unix()
	return time.Unix(sec, nsec)
}
