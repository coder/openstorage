package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The write path's git errors quote git's stderr, and the API logs them.
// Like gitread.Run, wcScratch.git and wrefGit keep only its last 64 KiB:
// git prints its fatal message last.
func TestWriteGitErrorsKeepStderrTail(t *testing.T) {
	bin := t.TempDir()
	// sc.git runs git with only the scratch environment, so the fake sets
	// its own PATH.
	script := "#!/bin/sh\nPATH=/usr/bin:/bin\nhead -c 1048576 /dev/zero | tr '\\0' e >&2\necho 'fatal: the end' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	sc := &wcScratch{repoPath: t.TempDir()}
	_, scErr := sc.git(context.Background(), nil, nil, "status")
	_, wrefErr := wrefGit(context.Background(), t.TempDir(), nil, nil, "status")
	for name, err := range map[string]error{"wcScratch.git": scErr, "wrefGit": wrefErr} {
		if err == nil {
			t.Fatalf("%s: nil error from a git that exits 1", name)
		}
		if n := len(err.Error()); n > 64<<10+1024 {
			t.Errorf("%s: the error is %d bytes, want git's stderr cut to its last 64 KiB", name, n)
		}
		if !strings.HasSuffix(err.Error(), "fatal: the end") {
			t.Errorf("%s: the error lost the end of git's stderr: %.80q", name, err.Error()[max(0, len(err.Error())-80):])
		}
	}
}
