//go:build !linux && !darwin

package gitlimit

// leaderHoldSupported reports false: this platform has no wait for an exit
// that leaves the child unreaped.
func leaderHoldSupported() bool { return false }

// awaitExit reports false, so the caller reaps first and the PID can be
// reused before the caller removes the group from its registry.
func awaitExit(int) bool { return false }
