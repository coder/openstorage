//go:build !linux

package gitlimit

// awaitExit reports false: there is no wait for an exit that leaves the
// child unreaped. Go's os package has none on macOS either, because waitid
// also returns for a stopped child (golang/go#19314). The caller reaps
// first.
func awaitExit(int) bool { return false }
