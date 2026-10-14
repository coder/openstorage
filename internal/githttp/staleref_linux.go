//go:build linux

package githttp

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// processInRepo returns the PID of a process that works in the repository
// at repoPath, or 0 when there is none. A process works in it when its
// working directory is inside it, or when a command-line argument (also
// the value of `--opt=value`, and paths after `-C`) or its GIT_DIR, resolved
// against the process's working directory and symlinks, is inside it.
// Every git that can own a cache-repo ref lock does one of these. It scans
// every process this one can read. Processes of other users are unreadable
// here, and they cannot write a cache repo, which is private to the
// server's user (only root could). It fails when /proc does not list this
// process, because then it cannot prove anything.
func processInRepo(repoPath string) (int, error) {
	abs, err := filepath.Abs(repoPath)
	if err != nil {
		return 0, err
	}
	roots := []string{abs}
	if real, err := filepath.EvalSymlinks(abs); err == nil && real != abs {
		roots = append(roots, real)
	}
	inside := func(p string) bool {
		for _, q := range []string{p, evalOr(p)} {
			for _, r := range roots {
				if q == r || strings.HasPrefix(q, r+"/") {
					return true
				}
			}
		}
		return false
	}
	self := os.Getpid()
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", self)); err != nil {
		return 0, fmt.Errorf("process table unavailable: %w", err)
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		cwd, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid))
		if err != nil {
			continue // exited, or not ours (another user cannot write the cache)
		}
		if inside(cwd) {
			return pid, nil
		}
		cmdline, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		base := cwd
		args := strings.Split(string(cmdline), "\x00")
		for i, arg := range args {
			if arg == "-C" && i+1 < len(args) {
				if base = resolveAt(base, args[i+1]); inside(base) {
					return pid, nil
				}
			}
			if _, v, ok := strings.Cut(arg, "="); ok && strings.HasPrefix(arg, "-") {
				arg = v
			}
			if arg != "" && inside(resolveAt(base, arg)) {
				return pid, nil
			}
		}
		environ, _ := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
		for _, kv := range strings.Split(string(environ), "\x00") {
			if v, ok := strings.CutPrefix(kv, "GIT_DIR="); ok && v != "" && inside(resolveAt(cwd, v)) {
				return pid, nil
			}
		}
	}
	return 0, nil
}

// resolveAt resolves p against the directory base.
func resolveAt(base, p string) string {
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	return filepath.Join(base, p)
}

// evalOr returns p with symlinks resolved, or p when that fails.
func evalOr(p string) string {
	if real, err := filepath.EvalSymlinks(p); err == nil {
		return real
	}
	return p
}
