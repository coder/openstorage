package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// applyPatch's plain error names its command, and the API logs it, so it
// must not quote a request-sized argument whole. (wcScratch.git and wrefGit:
// TestWriteGitErrorsNameLongArgumentsBriefly.)
func TestApplyPatchErrorBoundsTheArgv(t *testing.T) {
	// A git that prints nothing (a real git can echo the argument into
	// stderr) and dies of SIGTERM, so applyPatch returns its plain error,
	// which names the command. The command must be cut however git fails:
	// it runs (64 KiB), or exec fails with E2BIG (200 KiB, Linux).
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\nkill -TERM $$\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	sc := &wcScratch{repoPath: t.TempDir(), dir: t.TempDir()}
	patch := filepath.Join(sc.dir, "patch.diff")
	if err := os.WriteFile(patch, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{64 << 10, 200 << 10} {
		_, err := sc.applyPatch(context.Background(), patch, "--cached", strings.Repeat("a", n))
		if err == nil {
			t.Fatalf("applyPatch with a %d-byte argument: nil error", n)
		}
		if size := len(err.Error()); size > 1024 {
			t.Errorf("applyPatch with a %d-byte argument: the error is %d bytes, want at most 1 KiB", n, size)
		}
		if !strings.HasPrefix(err.Error(), "git apply --cached aaa") {
			t.Errorf("applyPatch with a %d-byte argument: the error does not name the command: %.80q", n, err.Error())
		}
	}
}
