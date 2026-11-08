package jobs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// noisyGit puts a git first on PATH that prints 1 MiB and then a fatal
// line to stderr, and exits 1.
func noisyGit(t *testing.T) {
	t.Helper()
	bin := t.TempDir()
	script := "#!/bin/sh\nPATH=/usr/bin:/bin\nhead -c 1048576 /dev/zero | tr '\\0' e >&2\necho 'fatal: the end' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// requireStderrTail checks that err quotes only the tail of git's stderr:
// the errors of git runners reach the log.
func requireStderrTail(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: nil error from a git that exits 1", name)
	}
	if n := len(err.Error()); n > 64<<10+1024 {
		t.Errorf("%s: the error is %d bytes, want git's stderr cut to its last 64 KiB", name, n)
	}
	if !strings.Contains(err.Error(), "fatal: the end") {
		t.Errorf("%s: the error lost the end of git's stderr", name)
	}
}

func TestRunGitStdinErrorKeepsStderrTail(t *testing.T) {
	dir := t.TempDir()
	noisyGit(t)
	_, err := runGitStdin(context.Background(), dir, nil, "status")
	requireStderrTail(t, "runGitStdin", err)
}

func TestStreamShowIndexErrorKeepsStderrTail(t *testing.T) {
	noisyGit(t)
	err := streamShowIndex(context.Background(), t.TempDir(), strings.NewReader(""), func(string) {})
	requireStderrTail(t, "streamShowIndex", err)
}
