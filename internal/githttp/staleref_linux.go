//go:build linux

package githttp

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// processInRepo returns the PID of a process whose working directory is
// inside repoPath or whose command line names it, or 0 when there is none.
// It scans every process this one can read. Processes of other users are
// unreadable here, and they cannot write a cache repo, which is private to
// the server's user (only root could). It fails when /proc does not list
// this process, because then it cannot prove anything.
func processInRepo(repoPath string) (int, error) {
	abs, err := filepath.Abs(repoPath)
	if err != nil {
		return 0, err
	}
	roots := []string{abs}
	if real, err := filepath.EvalSymlinks(abs); err == nil && real != abs {
		roots = append(roots, real)
	}
	self := os.Getpid()
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", self)); err != nil {
		return 0, fmt.Errorf("process table unavailable: %w", err)
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	inside := func(p string) bool {
		for _, r := range roots {
			if p == r || strings.HasPrefix(p, r+"/") {
				return true
			}
		}
		return false
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		if cwd, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid)); err == nil && inside(cwd) {
			return pid, nil
		}
		cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			continue // exited, or not ours
		}
		for _, arg := range bytes.Split(cmdline, []byte{0}) {
			for _, r := range roots {
				if bytes.Contains(arg, []byte(r)) {
					return pid, nil
				}
			}
		}
	}
	return 0, nil
}
