//go:build !linux

package gitlimit

// awaitExit reports false: Go has no portable wait for an exit that leaves
// the child unreaped here, so the caller reaps first.
func awaitExit(int) bool { return false }
