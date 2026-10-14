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
// server's user. Only root could: an unreadable process counts as working
// in the repository when it may be git (unreadableMayOwn). It fails when
// /proc does not list this process or hides other users' processes
// (hidepid), because then it cannot prove anything. The process exclude
// (0 for none) is never counted.
func processInRepo(repoPath string, exclude int) (int, error) {
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
	if mounts, err := os.ReadFile("/proc/self/mounts"); err != nil || procHidesPIDs(mounts) {
		return 0, fmt.Errorf("process table may hide processes (hidepid or unreadable mounts: %v)", err)
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self || pid == exclude {
			continue
		}
		cmdline, cmdErr := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		cwd, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid))
		if err != nil {
			if _, serr := os.Stat(fmt.Sprintf("/proc/%d", pid)); serr == nil && unreadableMayOwn(cmdline, cmdErr) {
				return pid, nil // another user's live git (root can write the cache)
			}
			continue // exited, or another user's non-git process
		}
		if inside(cwd) {
			return pid, nil
		}
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

// unreadableMayOwn reports whether a process whose working directory this
// process cannot read may still own a cache-repo ref lock: when its command
// line is unreadable, or when its program is git. Kernel threads (an empty
// command line) and other programs cannot.
func unreadableMayOwn(cmdline []byte, err error) bool {
	if err != nil {
		return true
	}
	argv0, _, _ := strings.Cut(string(cmdline), "\x00")
	return strings.HasPrefix(filepath.Base(argv0), "git")
}

// procHidesPIDs reports whether /proc is mounted with hidepid set to hide
// other users' processes (/proc/self/mounts content).
func procHidesPIDs(mounts []byte) bool {
	for _, line := range strings.Split(string(mounts), "\n") {
		f := strings.Fields(line)
		if len(f) < 4 || f[1] != "/proc" || f[2] != "proc" {
			continue
		}
		for _, opt := range strings.Split(f[3], ",") {
			if v, ok := strings.CutPrefix(opt, "hidepid="); ok && v != "0" && v != "off" {
				return true
			}
		}
	}
	return false
}
