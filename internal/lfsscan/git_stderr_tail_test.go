package lfsscan

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// noisyGitOn puts a git first on PATH that runs the real git, except that
// a call with an argument that matches the shell pattern arg prints 1 MiB
// and then a fatal line to stderr, and exits 1.
func noisyGitOn(t *testing.T, arg string) {
	t.Helper()
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	script := "#!/bin/sh\nfor a in \"$@\"; do case \"$a\" in " + arg + ")\n" +
		"PATH=/usr/bin:/bin; head -c 1048576 /dev/zero | tr '\\0' e >&2; echo 'fatal: the end' >&2; exit 1;; esac; done\n" +
		"exec '" + real + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// Each of the scan's three git children quotes only the tail of its
// stderr: the errors reach the log.
func TestReachableLFSOIDsErrorKeepsStderrTail(t *testing.T) {
	for _, arg := range []string{"rev-list", "--batch-check=*", "--batch"} {
		t.Run(arg, func(t *testing.T) {
			repo := t.TempDir()
			if out, err := exec.Command("git", "init", "-q", "--bare", repo).CombinedOutput(); err != nil {
				t.Fatalf("git init: %v: %s", err, out)
			}
			noisyGitOn(t, arg)
			_, _, err := ReachableLFSOIDs(context.Background(), repo, 1<<30)
			if err == nil {
				t.Fatal("nil error from a git that exits 1")
			}
			if n := len(err.Error()); n > 64<<10+1024 {
				t.Errorf("the error is %d bytes, want git's stderr cut to its last 64 KiB", n)
			}
			if !strings.Contains(err.Error(), "fatal: the end") {
				t.Errorf("the error lost the end of git's stderr: %.120q", err.Error())
			}
		})
	}
}
