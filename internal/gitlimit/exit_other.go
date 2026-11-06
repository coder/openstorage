//go:build !linux

package gitlimit

// awaitExit reports false, so the caller reaps first. macOS has no wait for
// an exit that leaves the child unreaped: Go's os package does not use
// waitid there either, because waitid also returns for a stopped child
// (golang/go#19314). FreeBSD and NetBSD have wait6, which is not used here
// because releases build only linux and darwin.
func awaitExit(int) bool { return false }
