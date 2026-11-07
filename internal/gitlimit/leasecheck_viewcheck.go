//go:build openstorage_viewcheck

package gitlimit

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// The openstorage_viewcheck build tag turns on the lease check in any binary,
// so the tests catch a git start that runs in a repository without the lease
// (issue #490): CI's in-process test runs and the e2e serve binary that
// internal/githttp builds. Release builds never set the tag, so a missed
// call site is caught here and not in production. A start that has no lease
// must run in no repository at all: its Dir is empty and none of its
// arguments is a repository. A start with a lease must run in that
// repository only.
func init() {
	checkUnleased = func(cmd *exec.Cmd) {
		if cmd.Dir != "" {
			panic(fmt.Sprintf("gitlimit: git %s starts in %q without a lease; use gitlimit.StartIn", Name(cmd.Args[1:]), cmd.Dir))
		}
		for _, p := range repoArgs(cmd) {
			panic(fmt.Sprintf("gitlimit: git %s starts in the repository %q without a lease; use gitlimit.StartIn", Name(cmd.Args[1:]), p))
		}
	}
	checkLeased = func(repoPath string, cmd *exec.Cmd) {
		dirs := repoArgs(cmd)
		if cmd.Dir != "" {
			dirs = append(dirs, cmd.Dir)
		}
		for _, p := range dirs {
			if !sameDir(p, repoPath) {
				panic(fmt.Sprintf("gitlimit: git %s runs in %q under the lease of %q", Name(cmd.Args[1:]), p, repoPath))
			}
		}
	}
}

// repoArgs returns the arguments of cmd that are the path of a git repository
// (a directory with HEAD and objects), such as upload-pack's.
func repoArgs(cmd *exec.Cmd) []string {
	var dirs []string
	for _, a := range cmd.Args {
		if !filepath.IsAbs(a) {
			continue
		}
		_, herr := os.Stat(filepath.Join(a, "HEAD"))
		_, oerr := os.Stat(filepath.Join(a, "objects"))
		if herr == nil && oerr == nil {
			dirs = append(dirs, a)
		}
	}
	return dirs
}

func sameDir(a, b string) bool {
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(fa, fb)
}
