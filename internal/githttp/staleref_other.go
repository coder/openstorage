//go:build !linux

package githttp

import "errors"

// processInRepo cannot prove that no process works in a repository
// without /proc, so stale ref locks are never removed here (issue #464).
func processInRepo(string, int) (int, error) {
	return 0, errors.New("no process table on this platform")
}
