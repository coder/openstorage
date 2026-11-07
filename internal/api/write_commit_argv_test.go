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
	// A git that prints nothing (a real git can echo the argument into
	// stderr): the command must be cut however git fails, by its exit
	// (64 KiB) or by E2BIG from exec (200 KiB, Linux). A git apply dies of
	// SIGTERM: applyPatch's plain error names the command, and its
	// rejection for an exit status does not.
	bin := t.TempDir()
	script := "#!/bin/sh\n[ \"$1\" = apply ] && kill -TERM $$\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	sc := &wcScratch{repoPath: t.TempDir(), dir: t.TempDir()}
	patch := filepath.Join(sc.dir, "patch.diff")
	if err := os.WriteFile(patch, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{64 << 10, 200 << 10} {
		long := strings.Repeat("a", n)
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
				t.Fatalf("%s with a %d-byte argument: nil error", tc.name, n)
			}
			if size := len(tc.err.Error()); size > 8<<10 {
				t.Errorf("%s with a %d-byte argument: the error is %d bytes, want at most 8 KiB", tc.name, n, size)
			}
			if !strings.HasPrefix(tc.err.Error(), tc.prefix) {
				t.Errorf("%s with a %d-byte argument: the error does not name the command: %.80q", tc.name, n, tc.err.Error())
			}
		}
	}
}
