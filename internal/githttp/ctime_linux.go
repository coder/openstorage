//go:build linux

package githttp

import (
	"os"
	"syscall"
	"time"
)

// fileChangeTime returns the inode change time (ctime): an in-place rewrite
// can preserve size and restore mtime (the `touch -r` pattern), but no
// unprivileged write can restore ctime — the kernel bumps it on every
// content or metadata change, including Chtimes itself (PR #98 round-4
// review). Linux is the production platform (serve runs in the Linux
// image); the !linux fallback degrades gracefully.
func fileChangeTime(fi os.FileInfo) time.Time {
	st, ok := fi.Sys().(*syscall.Stat_t)
	assertf(ok, "fileChangeTime: unexpected FileInfo.Sys type %T", fi.Sys())
	sec, nsec := st.Ctim.Unix()
	return time.Unix(sec, nsec)
}
