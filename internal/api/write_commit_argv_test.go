package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The write path's git helpers quote their command in their errors, and
// the API logs them, so an error must not quote a request-sized argv
// whole (a commit message reaches git as commit-tree -m <message>).
func TestWriteGitErrorsBoundTheArgv(t *testing.T) {
	long := strings.Repeat("a", 200<<10) // past 128 KiB, exec fails with E2BIG
	sc := &wcScratch{repoPath: t.TempDir(), dir: t.TempDir()}
	patch := filepath.Join(sc.dir, "patch.diff")
	if err := os.WriteFile(patch, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, scErr := sc.git(context.Background(), nil, nil, "commit-tree", "-m", long, "4b825dc642cb6eb9a060e54bf8d69288fbee4904")
	_, applyErr := sc.applyPatch(context.Background(), patch, "--cached", long)
	_, wrefErr := wrefGit(context.Background(), t.TempDir(), nil, nil, "notes", "--ref", long)
	for _, tc := range []struct {
		name, prefix string
		err          error
	}{
		{"wcScratch.git", "git commit-tree -m aaa", scErr},
		{"applyPatch", "git apply --cached aaa", applyErr},
		{"wrefGit", "git notes --ref aaa", wrefErr},
	} {
		if tc.err == nil {
			t.Fatalf("%s with a 200 KiB argument: nil error", tc.name)
		}
		if n := len(tc.err.Error()); n > 8<<10 {
			t.Errorf("%s: the error is %d bytes, want at most 8 KiB", tc.name, n)
		}
		if !strings.HasPrefix(tc.err.Error(), tc.prefix) {
			t.Errorf("%s: the error does not name the command: %.80q", tc.name, tc.err.Error())
		}
	}
}
