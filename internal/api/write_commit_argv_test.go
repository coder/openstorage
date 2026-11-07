package api

import (
	"context"
	"strings"
	"testing"
)

// A commit message reaches git as an argument (commit-tree -m), and the
// API logs wcScratch.git's errors, so an error must not quote a
// request-sized argv whole.
func TestWCScratchGitErrorBoundsTheArgv(t *testing.T) {
	sc := &wcScratch{repoPath: t.TempDir()}
	msg := strings.Repeat("a", 200<<10) // past 128 KiB, exec fails with E2BIG
	_, err := sc.git(context.Background(), nil, nil, "commit-tree", "-m", msg, "4b825dc642cb6eb9a060e54bf8d69288fbee4904")
	if err == nil {
		t.Fatal("commit-tree with a 200 KiB argument: nil error")
	}
	if n := len(err.Error()); n > 8<<10 {
		t.Fatalf("the error is %d bytes, want at most 8 KiB", n)
	}
	if !strings.HasPrefix(err.Error(), "git commit-tree -m aaa") {
		t.Fatalf("the error does not name the command: %.80q", err.Error())
	}
}
