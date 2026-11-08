package simtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// simGit's errors reach the test log: they keep only the tail of git's
// stderr, where git prints its fatal message.
func TestSimGitErrorKeepsStderrTail(t *testing.T) {
	bin := t.TempDir()
	script := "#!/bin/sh\nPATH=/usr/bin:/bin\nhead -c 1048576 /dev/zero | tr '\\0' e >&2\necho 'fatal: the end' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, err := simGit(t.TempDir(), nil, nil, "status")
	if err == nil {
		t.Fatal("simGit: nil error from a git that exits 1")
	}
	if n := len(err.Error()); n > 64<<10+1024 {
		t.Errorf("simGit: the error is %d bytes, want git's stderr cut to its last 64 KiB", n)
	}
	if !strings.HasSuffix(err.Error(), "fatal: the end") {
		t.Errorf("simGit: the error lost the end of git's stderr")
	}
}
