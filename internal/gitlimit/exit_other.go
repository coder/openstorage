//go:build !linux

package gitlimit

// awaitExit reports false: this platform has no wait for an exit that leaves
// the child unreaped (Go's os package has none on macOS either), so the
// caller reaps first and the PID can be reused before the caller removes
// the group from its registry.
func awaitExit(int) bool { return false }
